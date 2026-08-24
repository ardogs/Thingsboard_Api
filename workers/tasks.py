import asyncio
import httpx
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from celery import Celery
import redis.asyncio as redis
from beanie import PydanticObjectId

from core.config import settings
from core.database import init_db
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
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

# Configuración extrema para tareas masivas de larga duración (6 a 8 días)
celery_app.conf.update(
    broker_transport_options={"visibility_timeout": 864000},  # 10 días (864,000s) para evitar reentregas prematuras en Redis
    task_acks_late=True,                                      # Acknowledgment tardío tras completar la ejecución
    worker_prefetch_multiplier=1,                             # Prefetch de 1 tarea a la vez por worker
)

GLOBAL_REGISTRY_KEY = "tb_events:global_registry"
STREAM_CHANNEL_PREFIX = "tb_events:stream"


def get_server_lock_key(server_id: str) -> str:
    """Retorna la clave Redis para el candado distribuido por servidor ThingsBoard."""
    return f"tb_server_lock:{server_id}"


async def _heartbeat_server_lock(
    redis_conn: redis.Redis,
    lock_key: str,
    interval_seconds: int = 1800,  # 30 minutos
    ttl_seconds: int = 3600         # 1 hora
):
    """
    Heartbeat asíncrono en segundo plano:
    Mantiene vivo el candado distribuido en Redis durante descargas de larga duración (6 a 8 días)
    renovando el TTL cada 30 minutos a 1 hora.
    Si el pod/contenedor sufre una caída catastrófica, el candado expirará automáticamente en máximo 1 hora.
    """
    try:
        while True:
            await asyncio.sleep(interval_seconds)
            await redis_conn.expire(lock_key, ttl_seconds)
            logger.info(f"[Heartbeat] Candado '{lock_key}' renovado exitosamente (TTL: {ttl_seconds}s).")
    except asyncio.CancelledError:
        logger.debug(f"[Heartbeat] Tarea de latido para '{lock_key}' cancelada correctamente.")
        raise
    except Exception as e:
        logger.error(f"[Heartbeat] Error inesperado renovando candado '{lock_key}': {e}")


async def _execute_routed_telemetry_download(task_id: str, payload: dict):
    """
    Función interna asíncrona que resuelve el Tenant y Servidor en MongoDB,
    gestiona el arranque en frío (login inicial si no hay token),
    adquiere el Lock Distribuido con Heartbeat para ejecuciones multi-día (6-8 días),
    instancia el ThingsBoardClient dinámico y delega la ejecución al telemetry_service.
    """
    # 1. Asegurar inicialización de la conexión a MongoDB y Beanie ODM en el contexto del worker
    await init_db()

    tenant_id = payload.get("tenant_id")
    user_id = str(payload.get("user_id") or "default_user")

    if not tenant_id:
        raise ValueError("Se requiere 'tenant_id' en el payload para enrutar la tarea de telemetría")

    # 2. Consultar el documento TBTenant en MongoDB
    try:
        obj_id = PydanticObjectId(tenant_id)
        tenant = await TBTenant.get(obj_id)
    except Exception:
        tenant = await TBTenant.get(tenant_id)

    if not tenant:
        raise ValueError(f"No se encontró el Tenant de ThingsBoard con ID: {tenant_id}")

    server = await tenant.get_server()
    if not server:
        raise ValueError(f"No se encontró el servidor ThingsBoard asociado al Tenant '{tenant.name}'")

    server_id = str(server.id)
    logger.info(f"[Celery Router] Tenant resuelto: '{tenant.name}' en Servidor: '{server.name}' (ID: {server_id}, URL: {server.base_url})")

    # 3. Instanciar cliente dinámico ThingsBoardClient
    tb_client = ThingsBoardClient(
        base_url=server.base_url,
        token=tenant.token,
        refresh_token=tenant.refresh_token,
        username=tenant.username,
        password=tenant.password
    )

    # 4. REGLA DE ARRANQUE EN FRÍO:
    # Si tenant.token es None o está vacío, ejecutar login inicial con credenciales y persistir en MongoDB
    if not tenant.token or not str(tenant.token).strip():
        if not tenant.username or not tenant.password:
            raise ValueError(
                f"El Tenant '{tenant.name}' no tiene tokens iniciales ni credenciales (username/password) configuradas para el arranque en frío."
            )
        logger.info(f"[Celery Worker] Arranque en frío detectado para Tenant '{tenant.name}'. Ejecutando login inicial en {server.base_url}...")
        login_res = await tb_client.login(tenant.username, tenant.password)
        if not login_res or "token" not in login_res:
            raise ValueError(
                f"Fallo de autenticación en arranque en frío para Tenant '{tenant.name}' en {server.base_url}."
            )
        
        # Persistir tokens iniciales en MongoDB
        tenant.token = login_res["token"]
        tenant.refresh_token = login_res.get("refreshToken")
        tenant.updated_at = datetime.now(timezone.utc)
        await tenant.save()
        logger.info(f"[MongoDB] Tokens de arranque en frío persistidos exitosamente para TBTenant '{tenant.name}'.")

    payload["tenant_name"] = tenant.name
    payload["server_url"] = server.base_url
    payload["server_id"] = server_id

    # 5. Herencia del Candado Distribuido (creado por FastAPI) y Lanzamiento de Heartbeat
    lock_key = get_server_lock_key(server_id)
    redis_conn = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)

    # Extender el TTL del candado a 1 hora (3600s) ya que el worker tomó el relevo de la ejecución
    await redis_conn.expire(lock_key, 3600)
    logger.info(f"[Celery Worker] Candado heredado de FastAPI: '{lock_key}' para tarea {task_id} (TTL extendido a 3600s)")

    # Iniciar la tarea de Heartbeat en segundo plano (renueva cada 30 min por 1 hora más)
    heartbeat_task = asyncio.create_task(
        _heartbeat_server_lock(redis_conn=redis_conn, lock_key=lock_key, interval_seconds=1800, ttl_seconds=3600)
    )

    try:
        # 6. Delegar la ejecución pesada al servicio de telemetría
        await run_download_orchestrator(
            task_id=task_id,
            tb=tb_client,
            user_id=user_id,
            payload=payload
        )
    finally:
        # 7. Limpieza Garantizada: Cancelar Heartbeat y liberar el Lock en Redis
        logger.info(f"[Celery Worker] Finalizando tarea {task_id}. Cancelando Heartbeat y liberando lock '{lock_key}'...")
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"[Celery Worker] Advertencia al esperar cancelación de heartbeat: {e}")

        try:
            await redis_conn.delete(lock_key)
            logger.info(f"[Celery Worker] Candado distribuido '{lock_key}' eliminado exitosamente de Redis.")
        except Exception as e:
            logger.error(f"[Celery Worker] Error al eliminar candado '{lock_key}' en Redis: {e}")
        finally:
            await redis_conn.aclose()


def release_server_lock_sync(server_id: Optional[str]):
    """Libera el candado distribuido de un servidor en Redis de forma síncrona en caso de error fatal."""
    if not server_id:
        return
    try:
        import redis as sync_redis
        sr = sync_redis.from_url(settings.REDIS_URL, decode_responses=True)
        sr.delete(get_server_lock_key(str(server_id)))
        sr.close()
        logger.info(f"[Celery Router] Candado de emergencia liberado para servidor {server_id}.")
    except Exception as e:
        logger.warning(f"[Celery Router] Error al liberar candado de emergencia para server {server_id}: {e}")


@celery_app.task(bind=True, max_retries=5)
def download_telemetry_task(self, payload: dict):
    """
    Enrutador ligero de Celery para tareas de descarga de telemetría Multi-Tenant.
    Resuelve el tenant y servidor en MongoDB, maneja arranque en frío y delega la ejecución al telemetry_service.
    """
    task_id = self.request.id
    tenant_id = payload.get("tenant_id", "unknown_tenant")
    tenant_name = payload.get("tenant_name", "default")
    user_id = str(payload.get("user_id") or "default_user")
    server_id = payload.get("server_id")
    logger.info(f"[Celery Router] Enrutando tarea {task_id} (TenantID: {tenant_id}, Tenant: {tenant_name})")

    try:
        asyncio.run(_execute_routed_telemetry_download(task_id, payload))
    except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
        logger.error(f"[Celery Router] Fallo de conectividad/red en tarea {task_id}: {exc}. Reintentando...")
        countdown = 2 ** min(self.request.retries, 5)
        raise self.retry(exc=exc, countdown=countdown)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (500, 502, 503, 504):
            logger.error(f"[Celery Router] Error de servidor ThingsBoard ({exc.response.status_code}) en tarea {task_id}. Reintentando...")
            countdown = 2 ** min(self.request.retries, 5)
            raise self.retry(exc=exc, countdown=countdown)
        else:
            logger.error(f"[Celery Router] Error HTTP cliente ({exc.response.status_code}) no recuperable en tarea {task_id}: {exc}")
            release_server_lock_sync(server_id)
            publish_task_status_sync(
                user_id=user_id,
                task_id=task_id,
                status="ERROR",
                tenant_name=tenant_name,
                cleanup_on_terminal=True
            )
            raise exc
    except Exception as exc:
        logger.error(f"[Celery Router] Error no recuperable en tarea {task_id}: {exc}")
        release_server_lock_sync(server_id)
        publish_task_status_sync(
            user_id=user_id,
            task_id=task_id,
            status="ERROR",
            tenant_name=tenant_name,
            cleanup_on_terminal=True
        )
        raise exc

