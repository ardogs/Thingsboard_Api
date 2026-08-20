import asyncio
import httpx
from typing import Optional, Dict, Any
from celery import Celery
from beanie import PydanticObjectId

from core.config import settings
from core.database import init_db
from core.models.tb_server import TBServer
from core.tb_client import ThingsBoardClient
from core.logger import logger
from core.services.telemetry_service import (
    run_download_orchestrator,
    publish_task_status,
    publish_task_status_sync,
    get_user_stream_channel,
    get_user_registry_key
)

celery_app = Celery("telemetry_tasks", broker=settings.REDIS_URL, backend=settings.REDIS_URL)

GLOBAL_REGISTRY_KEY = "tb_events:global_registry"
STREAM_CHANNEL_PREFIX = "tb_events:stream"


async def _execute_routed_telemetry_download(task_id: str, payload: dict):
    """
    Función interna asíncrona que resuelve el servidor en MongoDB, instancia el cliente dinámico y delega al servicio.
    """
    # 1. Asegurar inicialización de la conexión a MongoDB y Beanie
    await init_db()

    server_id = payload.get("server_id")
    user_id = str(payload.get("user_id") or "default_user")

    tb_client: Optional[ThingsBoardClient] = None

    # 2. Obtener documento del servidor desde MongoDB si se especificó server_id
    if server_id:
        try:
            try:
                obj_id = PydanticObjectId(server_id)
                server = await TBServer.get(obj_id)
            except Exception:
                server = await TBServer.get(server_id)

            if not server:
                raise ValueError(f"No se encontró el servidor ThingsBoard con ID: {server_id}")

            logger.info(f"[Celery Router] Servidor resuelto desde MongoDB: {server.name} ({server.base_url})")

            # 3. Instanciar cliente dinámico con los datos del documento de MongoDB
            tb_client = ThingsBoardClient(
                base_url=server.base_url,
                token=payload.get("token") or server.token,
                refresh_token=payload.get("refresh_token") or server.refresh_token,
                username=server.username,
                password=server.password
            )
        except Exception as e:
            logger.error(f"[Celery Router] Error recuperando servidor {server_id} de MongoDB: {e}")
            raise e
    else:
        # Modo compatibilidad si se proporciona server_url directamente
        server_url = payload.get("server_url")
        if not server_url:
            raise ValueError("Se requiere 'server_id' (MongoDB) o 'server_url' para enrutar la tarea")
        tb_client = ThingsBoardClient(
            base_url=server_url,
            token=payload.get("token"),
            refresh_token=payload.get("refresh_token")
        )

    # 4. Delegar la ejecución pesada al servicio de telemetría
    await run_download_orchestrator(
        task_id=task_id,
        tb=tb_client,
        user_id=user_id,
        payload=payload
    )


@celery_app.task(bind=True, max_retries=None)
def download_telemetry_task(self, payload: dict):
    """
    Enrutador ligero de Celery para tareas de descarga de telemetría.
    Resuelve el servidor en MongoDB y delega la ejecución al telemetry_service.
    """
    task_id = self.request.id
    tenant = payload.get("tenant_name") or payload.get("TENANT_NAME", "default")
    server_id = payload.get("server_id", "direct_url")
    logger.info(f"[Celery Router] Enrutando tarea {task_id} (Server: {server_id}, Tenant: {tenant})")

    try:
        asyncio.run(_execute_routed_telemetry_download(task_id, payload))
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.error(f"[Celery Router] Fallo de red en tarea {task_id}. Reintentando...")
        countdown = 2 ** self.request.retries
        raise self.retry(exc=exc, countdown=countdown)
    except Exception as exc:
        logger.error(f"[Celery Router] Error no recuperable en tarea {task_id}: {exc}")
        raise exc

