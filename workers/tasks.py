import asyncio
import os
import shutil
import time
import httpx
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from celery import Celery
from celery.schedules import crontab
from zoneinfo import ZoneInfo
from croniter import croniter
import redis.asyncio as redis
from beanie import PydanticObjectId

from core.config import settings
from core.database import init_db
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.models.tb_scheduled_task import TBScheduledTask
from core.tb_client import ThingsBoardClient
from core.redis_client import redis_client, get_redis_client
from core.logger import logger
from core.services.telemetry_service import (
    run_download_orchestrator,
    publish_task_status,
    publish_task_status_sync,
    get_user_stream_channel,
    get_user_registry_key
)

celery_app = Celery("telemetry_tasks", broker=settings.REDIS_URL, backend=settings.REDIS_URL)

# Configuración de Timezone y Scheduler para Celery Beat
celery_app.conf.timezone = "UTC"

# Configuración extrema para tareas masivas de larga duración y Despachador Maestro Beat
celery_app.conf.update(
    broker_transport_options={"visibility_timeout": 864000},  # 10 días (864,000s) para evitar reentregas prematuras en Redis
    task_acks_late=True,                                      # Acknowledgment tardío tras completar la ejecución
    worker_prefetch_multiplier=1,                             # Prefetch de 1 tarea a la vez por worker
    beat_schedule={
        "master_dispatcher_task": {
            "task": "tasks.master_dispatcher",
            "schedule": crontab(minute="*"),                  # Estrictamente cada 1 minuto
            "args": (),
        }
    }
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

    # 3. Instanciar cliente dinámico ThingsBoardClient con credenciales descifradas en memoria RAM
    plain_token = tenant.get_token()
    plain_refresh_token = tenant.get_refresh_token()
    plain_password = tenant.get_password()

    tb_client = ThingsBoardClient(
        base_url=server.base_url,
        token=plain_token,
        refresh_token=plain_refresh_token,
        username=tenant.username,
        password=plain_password
    )

    # 4. REGLA DE ARRANQUE EN FRÍO:
    # Si no hay token inicial, ejecutar login inicial con credenciales y persistir en MongoDB cifrado
    if not plain_token or not str(plain_token).strip():
        if not tenant.username or not plain_password:
            raise ValueError(
                f"El Tenant '{tenant.name}' no tiene tokens iniciales ni credenciales (username/password) configuradas para el arranque en frío."
            )
        logger.info(f"[Celery Worker] Arranque en frío detectado para Tenant '{tenant.name}'. Ejecutando login inicial en {server.base_url}...")
        login_res = await tb_client.login(tenant.username, plain_password)
        if not login_res or "token" not in login_res:
            raise ValueError(
                f"Fallo de autenticación en arranque en frío para Tenant '{tenant.name}' en {server.base_url}."
            )
        
        # Persistir tokens iniciales cifrados en MongoDB
        tenant.set_tokens(login_res["token"], login_res.get("refreshToken"))
        tenant.updated_at = datetime.now(timezone.utc)
        await tenant.save()
        logger.info(f"[MongoDB] Tokens de arranque en frío cifrados y persistidos exitosamente para TBTenant '{tenant.name}'.")

    payload["tenant_name"] = tenant.name
    payload["server_url"] = server.base_url
    payload["server_id"] = server_id

    # 5. Herencia del Candado Distribuido (creado por FastAPI) y Lanzamiento de Heartbeat
    lock_key = get_server_lock_key(server_id)
    redis_conn = get_redis_client()

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


def _run_sync_in_worker(coro):
    """
    Ejecuta una corrutina asíncrona de forma síncrona en el contexto del worker de Celery.
    Si detecta un event loop activo (ej: ejecuciones dentro de pruebas asíncronas),
    delega la ejecución a un ThreadPoolExecutor para prevenir 'asyncio.run() cannot be called from a running event loop'.
    En el worker normal de Celery, invoca directamente asyncio.run().
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            return executor.submit(asyncio.run, coro).result()
    else:
        return asyncio.run(coro)


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
        _run_sync_in_worker(_execute_routed_telemetry_download(task_id, payload))
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


async def _execute_master_dispatcher():
    """
    Lógica asíncrona central del Despachador Maestro:
    1. Inicializa o verifica la conexión a MongoDB y Beanie ODM (resiliente a reinicios de loop).
    2. Captura el tiempo actual estricto en UTC: now_utc = datetime.now(timezone.utc).
    3. Consulta todas las tareas programadas activas cuya fecha de ejecución haya vencido (next_run_time <= now_utc).
    4. Itera de forma aislada y segura sobre cada tarea:
       a. Encola la tarea en Celery usando celery_app.send_task(task.task_name, kwargs=task.payload).
       b. Calcula la próxima fecha de ejecución interpretando la expresión cron en la zona horaria local de México y convirtiéndola a UTC puro.
       c. Actualiza task.next_run_time, task.last_run_status y persiste en MongoDB.
    """
    await init_db()

    now_utc = datetime.now(timezone.utc)
    logger.info(f"[Master Dispatcher] Ejecutando evaluación a las {now_utc.isoformat()} (UTC)...")

    due_tasks = await TBScheduledTask.find(
        TBScheduledTask.is_active == True,
        TBScheduledTask.next_run_time <= now_utc
    ).to_list()

    if not due_tasks:
        logger.debug("[Master Dispatcher] No hay tareas programadas pendientes de ejecución en este ciclo.")
        return

    logger.info(f"[Master Dispatcher] Se encontraron {len(due_tasks)} tareas programadas pendientes de despacho.")

    for task in due_tasks:
        task_id_str = str(task.id)
        try:
            payload = task.payload or {}
            logger.info(f"[Master Dispatcher] Despachando tarea '{task.name}' ({task.task_name}) [ID: {task_id_str}]...")

            # 1. Encolar la tarea en el broker de Celery
            async_result = celery_app.send_task(task.task_name, kwargs=payload)
            dispatched_task_id = async_result.id if async_result else "unknown"

            # 2. Calcular próxima fecha en zona horaria local y convertir a UTC puro
            next_utc = task.compute_next_run()

            # 3. Actualizar metadatos y persistir en MongoDB
            task.next_run_time = next_utc
            task.last_run_status = f"DISPATCHED (Celery Task ID: {dispatched_task_id})"
            task.last_run_at = now_utc
            task.updated_at = now_utc
            await task.save()

            logger.info(
                f"[Master Dispatcher] Tarea '{task.name}' despachada con éxito (Celery ID: {dispatched_task_id}). "
                f"Próxima ejecución calculada para {next_utc.isoformat()} (UTC)."
            )
        except Exception as exc:
            logger.error(
                f"[Master Dispatcher] Fallo al procesar tarea programada '{task.name}' [ID: {task_id_str}]: {exc}",
                exc_info=True
            )
            try:
                task.last_run_status = f"ERROR: {str(exc)}"
                task.updated_at = datetime.now(timezone.utc)
                await task.save()
            except Exception as save_err:
                logger.critical(
                    f"[Master Dispatcher] Error crítico al actualizar estado de fallo para tarea '{task.name}': {save_err}"
                )


@celery_app.task(name="tasks.master_dispatcher")
def master_dispatcher_task():
    """
    Despachador Maestro periódico de Celery Beat (cada 1 minuto).
    Ejecuta la resolución de tareas programadas en MongoDB y las despacha dinámicamente.
    """
    logger.info("[Celery Beat] Ciclo de despacho maestro iniciado.")
    try:
        _run_sync_in_worker(_execute_master_dispatcher())
    except Exception as exc:
        logger.error(f"[Celery Beat] Error fatal en ejecución de master_dispatcher_task: {exc}", exc_info=True)
        raise exc


async def _execute_cleanup_old_backups(days_to_keep: int = 30) -> dict:
    """
    Ejecución asíncrona de la política de retención de respaldos y limpieza del sistema de archivos:
    1. Conexión resiliente a MongoDB y Beanie ODM.
    2. Identificación de registros en TBBackup con created_at <= cutoff_date (30 días por defecto).
    3. Eliminación segura de los archivos ZIP físicos en disco y posterior borrado del registro en MongoDB.
    4. Barrido de carpetas temporales huérfanas 'tmp_*' con antigüedad mayor a 24 horas.
    5. Retorno de estadísticas completas de la operación.
    """
    await init_db()

    now_utc = datetime.now(timezone.utc)
    cutoff_date = now_utc - timedelta(days=days_to_keep)
    backup_dir = getattr(settings, "BACKUP_DIR", "backups")
    abs_backup_dir = os.path.abspath(backup_dir)

    logger.info(
        f"[Cleanup Task] Iniciando purga de respaldos (Retención: {days_to_keep} días). "
        f"Fecha de corte: {cutoff_date.isoformat()} (UTC). Directorio: {abs_backup_dir}"
    )

    stats = {
        "status": "SUCCESS",
        "days_to_keep": days_to_keep,
        "cutoff_date": cutoff_date.isoformat(),
        "deleted_db_records": 0,
        "deleted_files": 0,
        "missing_files": 0,
        "file_errors": 0,
        "zombie_dirs_deleted": 0,
        "zombie_dir_errors": 0,
        "executed_at": now_utc.isoformat()
    }

    # 1. Purgar catálogo TBBackup y archivos ZIP físicos
    try:
        old_backups = await TBBackup.find(TBBackup.created_at <= cutoff_date).to_list()
        logger.info(f"[Cleanup Task] Se encontraron {len(old_backups)} respaldos caducados en MongoDB.")

        for backup in old_backups:
            # Eliminar archivo físico
            if backup.file_name:
                file_path = os.path.join(abs_backup_dir, backup.file_name)
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        stats["deleted_files"] += 1
                        logger.debug(f"[Cleanup Task] Archivo físico eliminado: {file_path}")
                    else:
                        stats["missing_files"] += 1
                        logger.debug(f"[Cleanup Task] Archivo físico ya no existía en disco: {file_path}")
                except Exception as file_err:
                    stats["file_errors"] += 1
                    logger.warning(f"[Cleanup Task] Error al eliminar archivo físico '{file_path}': {file_err}")

            # Eliminar registro del catálogo en MongoDB
            try:
                await backup.delete()
                stats["deleted_db_records"] += 1
                logger.debug(f"[Cleanup Task] Documento TBBackup {backup.id} eliminado de MongoDB.")
            except Exception as db_err:
                logger.error(f"[Cleanup Task] Error al eliminar documento TBBackup {backup.id}: {db_err}")

    except Exception as query_err:
        logger.error(f"[Cleanup Task] Error consultando registros caducados en TBBackup: {query_err}", exc_info=True)
        stats["status"] = "PARTIAL_ERROR"

    # 2. Barrer carpetas temporales zombis / huérfanas (tmp_*)
    if os.path.exists(abs_backup_dir) and os.path.isdir(abs_backup_dir):
        logger.info(f"[Cleanup Task] Inspeccionando '{abs_backup_dir}' en busca de directorios temporales zombis...")
        current_time = time.time()
        one_day_seconds = 86400  # 24 horas

        try:
            with os.scandir(abs_backup_dir) as entries:
                for entry in entries:
                    if entry.is_dir() and entry.name.startswith("tmp_"):
                        try:
                            mtime = entry.stat().st_mtime
                            age_seconds = current_time - mtime
                            if age_seconds > one_day_seconds:
                                age_hours = round(age_seconds / 3600, 2)
                                logger.info(f"[Cleanup Task] Purgando directorio temporal zombi '{entry.name}' (Antigüedad: {age_hours}h)...")
                                shutil.rmtree(entry.path, ignore_errors=True)
                                stats["zombie_dirs_deleted"] += 1
                            else:
                                logger.debug(f"[Cleanup Task] Conservando temporal activo/reciente '{entry.name}' ({round(age_seconds / 60, 2)} min).")
                        except Exception as tmp_err:
                            stats["zombie_dir_errors"] += 1
                            logger.warning(f"[Cleanup Task] Error inspeccionando/eliminando '{entry.path}': {tmp_err}")
        except Exception as scan_err:
            logger.error(f"[Cleanup Task] Error escaneando directorio '{abs_backup_dir}': {scan_err}", exc_info=True)
            stats["status"] = "PARTIAL_ERROR"
    else:
        logger.debug(f"[Cleanup Task] El directorio de respaldos '{abs_backup_dir}' no existe o no es accesible.")

    logger.info(
        f"[Cleanup Task] Finalizada purga de respaldos. Resumen: "
        f"DB={stats['deleted_db_records']}, ZIPs={stats['deleted_files']}, "
        f"Zombis={stats['zombie_dirs_deleted']}, Errores={stats['file_errors'] + stats['zombie_dir_errors']}"
    )
    return stats


@celery_app.task(name="tasks.cleanup_old_backups")
def cleanup_old_backups_task(days_to_keep: int = 30) -> dict:
    """
    Tarea periódica y bajo demanda de Celery para purgar respaldos antiguos y limpiar temporales zombis.
    Orquestada por el Patrón Dispatcher o ejecutable manualmente.
    """
    logger.info(f"[Celery Worker] Iniciando tarea tasks.cleanup_old_backups con days_to_keep={days_to_keep}")
    try:
        return _run_sync_in_worker(_execute_cleanup_old_backups(days_to_keep=days_to_keep))
    except Exception as exc:
        logger.error(f"[Celery Worker] Error fatal en ejecución de cleanup_old_backups: {exc}", exc_info=True)
        raise exc


