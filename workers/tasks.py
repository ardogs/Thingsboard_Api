import asyncio
import os
import shutil
import time
import uuid
import httpx
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from arq import Retry
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
from core.io_limiter import async_rmtree, async_remove_file
from core.services.telemetry_service import (
    run_download_orchestrator,
    publish_task_status,
    get_user_stream_channel,
    get_user_registry_key
)
from core.services.excel_report_service import run_excel_report_orchestrator
from core.services.incremental_backup_service import (
    calculate_previous_month_boundaries,
    run_incremental_tenant_backup
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
    Heartbeat asíncrono en segundo plano (asyncio.create_task):
    Mantiene vivo el candado distribuido en Redis durante descargas de larga duración (6 a 8 días)
    renovando el TTL cada 30 minutos a 1 hora.
    Cede el control no bloqueante al loop principal mediante await asyncio.sleep().
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


async def download_telemetry_task(ctx: dict, payload: Optional[dict] = None, **kwargs) -> dict:
    """
    Enrutador ARQ puramente asíncrono para tareas de descarga de telemetría Multi-Tenant.
    Resuelve el tenant y servidor en MongoDB, maneja arranque en frío,
    lanza el heartbeat con asyncio.create_task() y delega la ejecución al telemetry_service.
    """
    actual_payload = payload if payload is not None else kwargs
    job_id = ctx.get("job_id") or actual_payload.get("task_id") or str(uuid.uuid4())
    job_try = ctx.get("job_try", 1)
    tenant_id = actual_payload.get("tenant_id", "unknown_tenant")
    tenant_name = actual_payload.get("tenant_name", "default")
    user_id = str(actual_payload.get("user_id") or "default_user")
    server_id = actual_payload.get("server_id")

    logger.info(f"[ARQ Router] Enrutando tarea {job_id} (TenantID: {tenant_id}, Tenant: {tenant_name}, Intento: {job_try})")

    # 1. Resolver Tenant en MongoDB
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
    logger.info(f"[ARQ Router] Tenant resuelto: '{tenant.name}' en Servidor: '{server.name}' (ID: {server_id}, URL: {server.base_url})")

    # 2. Instanciar cliente dinámico ThingsBoardClient con credenciales descifradas en memoria RAM
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

    # 3. Arranque en frío si no hay token inicial
    if not plain_token or not str(plain_token).strip():
        if not tenant.username or not plain_password:
            raise ValueError(
                f"El Tenant '{tenant.name}' no tiene tokens iniciales ni credenciales (username/password) configuradas para el arranque en frío."
            )
        logger.info(f"[ARQ Worker] Arranque en frío detectado para Tenant '{tenant.name}'. Ejecutando login inicial en {server.base_url}...")
        login_res = await tb_client.login(tenant.username, plain_password)
        if not login_res or "token" not in login_res:
            raise ValueError(
                f"Fallo de autenticación en arranque en frío para Tenant '{tenant.name}' en {server.base_url}."
            )

        tenant.set_tokens(login_res["token"], login_res.get("refreshToken"))
        tenant.updated_at = datetime.now(timezone.utc)
        await tenant.save()
        logger.info(f"[MongoDB] Tokens de arranque en frío cifrados y persistidos exitosamente para TBTenant '{tenant.name}'.")

    payload["tenant_name"] = tenant.name
    payload["server_url"] = server.base_url
    payload["server_id"] = server_id

    # 4. Herencia del Candado Distribuido y Lanzamiento Concurrente de Heartbeat
    lock_key = get_server_lock_key(server_id)
    redis_conn = ctx.get("redis") or get_redis_client()

    await redis_conn.expire(lock_key, 3600)
    logger.info(f"[ARQ Worker] Candado heredado: '{lock_key}' para tarea {job_id} (TTL extendido a 3600s)")

    # Iniciar la tarea de Heartbeat concurrente en segundo plano
    heartbeat_task = asyncio.create_task(
        _heartbeat_server_lock(redis_conn=redis_conn, lock_key=lock_key, interval_seconds=1800, ttl_seconds=3600)
    )

    try:
        # 5. Delegar la ejecución pesada al servicio de telemetría
        return await run_download_orchestrator(
            task_id=job_id,
            tb=tb_client,
            user_id=user_id,
            payload=payload,
            redis_client=redis_conn
        )
    except asyncio.CancelledError:
        logger.warning(f"[ARQ Router] Tarea {job_id} CANCELADA / ABORTADA por señal externa.")
        # Purgar carpeta temporal residual tmp_<job_id>
        tmp_dir = os.path.join("backups", f"tmp_{job_id}")
        if os.path.exists(tmp_dir):
            try:
                await async_rmtree(tmp_dir, ignore_errors=True)
                logger.info(f"[ARQ Router] Carpeta temporal residual '{tmp_dir}' purgada exitosamente tras cancelación.")
            except Exception as clean_err:
                logger.warning(f"[ARQ Router] Error purgando carpeta temporal '{tmp_dir}': {clean_err}")

        await publish_task_status(
            redis_client=redis_conn,
            user_id=user_id,
            task_id=job_id,
            status="CANCELLED",
            tenant_name=tenant_name,
            cleanup_on_terminal=True
        )
        raise
    except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
        if job_try <= 5:
            countdown = 2 ** min(job_try, 5)
            logger.error(f"[ARQ Router] Fallo de conectividad/red en tarea {job_id}: {exc}. Reintentando en {countdown}s...")
            raise Retry(defer=countdown)
        else:
            await publish_task_status(
                redis_client=redis_conn,
                user_id=user_id,
                task_id=job_id,
                status="ERROR",
                tenant_name=tenant_name,
                cleanup_on_terminal=True
            )
            raise exc
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (500, 502, 503, 504) and job_try <= 5:
            countdown = 2 ** min(job_try, 5)
            logger.error(f"[ARQ Router] Error de servidor ThingsBoard ({exc.response.status_code}) en tarea {job_id}. Reintentando en {countdown}s...")
            raise Retry(defer=countdown)
        else:
            logger.error(f"[ARQ Router] Error HTTP cliente ({exc.response.status_code}) no recuperable en tarea {job_id}: {exc}")
            await publish_task_status(
                redis_client=redis_conn,
                user_id=user_id,
                task_id=job_id,
                status="ERROR",
                tenant_name=tenant_name,
                cleanup_on_terminal=True
            )
            raise exc
    except Exception as exc:
        logger.error(f"[ARQ Router] Error no recuperable en tarea {job_id}: {exc}")
        await publish_task_status(
            redis_client=redis_conn,
            user_id=user_id,
            task_id=job_id,
            status="ERROR",
            tenant_name=tenant_name,
            cleanup_on_terminal=True
        )
        raise exc
    finally:
        # 6. Limpieza Garantizada: Cancelar Heartbeat, liberar el Lock en Redis y asegurar purga de temporales
        logger.info(f"[ARQ Worker] Finalizando tarea {job_id}. Cancelando Heartbeat y liberando lock '{lock_key}'...")
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"[ARQ Worker] Advertencia al esperar cancelación de heartbeat: {e}")

        try:
            await redis_conn.delete(lock_key)
            logger.info(f"[ARQ Worker] Candado distribuido '{lock_key}' eliminado exitosamente de Redis.")
        except Exception as e:
            logger.error(f"[ARQ Worker] Error al eliminar candado '{lock_key}' en Redis: {e}")

        # Limpieza defensiva de carpeta temporal si quedó residual
        tmp_dir = os.path.join("backups", f"tmp_{job_id}")
        if os.path.exists(tmp_dir):
            try:
                await async_rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                pass


async def generate_excel_report_task(ctx: dict, payload: Optional[dict] = None, **kwargs) -> dict:
    """
    Enrutador ARQ asíncrono para tareas de exportación de telemetría a Excel (.xlsx).
    Resuelve el tenant y servidor en MongoDB, recupera report_config,
    adquiere/renueva el distributed lock con heartbeat y delega la ejecución a excel_report_service.
    """
    actual_payload = payload if payload is not None else kwargs
    job_id = ctx.get("job_id") or actual_payload.get("task_id") or str(uuid.uuid4())
    job_try = ctx.get("job_try", 1)
    tenant_id = actual_payload.get("tenant_id", "unknown_tenant")
    tenant_name = actual_payload.get("tenant_name", "default")
    user_id = str(actual_payload.get("user_id") or "default_user")

    logger.info(f"[ARQ Router] Enrutando tarea de reporte Excel {job_id} (Tenant: {tenant_name}, ID: {tenant_id}, Intento: {job_try})")

    # 1. Resolver Tenant en MongoDB
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
    logger.info(f"[ARQ Router] Tenant resuelto: '{tenant.name}' en Servidor: '{server.name}' (ID: {server_id})")

    # 2. Instanciar ThingsBoardClient
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

    # 3. Arranque en frío si no hay token inicial
    if not plain_token or not str(plain_token).strip():
        if not tenant.username or not plain_password:
            raise ValueError(
                f"El Tenant '{tenant.name}' no tiene tokens iniciales ni credenciales para arranque en frío."
            )
        logger.info(f"[ARQ Worker] Arranque en frío detectado para Tenant '{tenant.name}'. Ejecutando login inicial...")
        login_res = await tb_client.login(tenant.username, plain_password)
        if not login_res or "token" not in login_res:
            raise ValueError(f"Fallo de autenticación en arranque en frío para Tenant '{tenant.name}'.")

        tenant.set_tokens(login_res["token"], login_res.get("refreshToken"))
        tenant.updated_at = datetime.now(timezone.utc)
        await tenant.save()

    # Inyectar report_config del documento si no viene en payload
    if "report_config" not in actual_payload or not actual_payload["report_config"]:
        actual_payload["report_config"] = tenant.custom_metadata.get("report_config", {}) if tenant.custom_metadata else {}

    actual_payload["tenant_name"] = tenant.name
    actual_payload["server_url"] = server.base_url
    actual_payload["server_id"] = server_id

    # 4. Candado distribuido y Heartbeat
    lock_key = get_server_lock_key(server_id)
    redis_conn = ctx.get("redis") or get_redis_client()

    await redis_conn.expire(lock_key, 3600)
    heartbeat_task = asyncio.create_task(
        _heartbeat_server_lock(redis_conn=redis_conn, lock_key=lock_key, interval_seconds=1800, ttl_seconds=3600)
    )

    try:
        return await run_excel_report_orchestrator(
            task_id=job_id,
            tb=tb_client,
            user_id=user_id,
            payload=actual_payload,
            redis_client=redis_conn
        )
    except asyncio.CancelledError:
        logger.warning(f"[ARQ Router] Tarea Excel {job_id} CANCELADA / ABORTADA por señal externa.")
        tmp_dir = os.path.join("backups", f"tmp_{job_id}")
        if os.path.exists(tmp_dir):
            try:
                await async_rmtree(tmp_dir, ignore_errors=True)
                logger.info(f"[ARQ Router] Carpeta temporal residual '{tmp_dir}' purgada exitosamente tras cancelación.")
            except Exception as clean_err:
                logger.warning(f"[ARQ Router] Error purgando carpeta temporal '{tmp_dir}': {clean_err}")

        await publish_task_status(
            redis_client=redis_conn,
            user_id=user_id,
            task_id=job_id,
            status="CANCELLED",
            tenant_name=tenant_name,
            cleanup_on_terminal=True
        )
        raise
    except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
        if job_try <= 5:
            countdown = 2 ** min(job_try, 5)
            logger.error(f"[ARQ Router] Fallo de conectividad en tarea Excel {job_id}: {exc}. Reintentando en {countdown}s...")
            raise Retry(defer=countdown)
        else:
            await publish_task_status(
                redis_client=redis_conn,
                user_id=user_id,
                task_id=job_id,
                status="ERROR",
                tenant_name=tenant_name,
                cleanup_on_terminal=True
            )
            raise exc
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (500, 502, 503, 504) and job_try <= 5:
            countdown = 2 ** min(job_try, 5)
            logger.error(f"[ARQ Router] Error de servidor ({exc.response.status_code}) en tarea Excel {job_id}. Reintentando en {countdown}s...")
            raise Retry(defer=countdown)
        else:
            await publish_task_status(
                redis_client=redis_conn,
                user_id=user_id,
                task_id=job_id,
                status="ERROR",
                tenant_name=tenant_name,
                cleanup_on_terminal=True
            )
            raise exc
    except Exception as exc:
        logger.error(f"[ARQ Router] Error no recuperable en tarea Excel {job_id}: {exc}")
        await publish_task_status(
            redis_client=redis_conn,
            user_id=user_id,
            task_id=job_id,
            status="ERROR",
            tenant_name=tenant_name,
            cleanup_on_terminal=True
        )
        raise exc
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

        try:
            await redis_conn.delete(lock_key)
            logger.info(f"[ARQ Worker] Candado distribuido '{lock_key}' eliminado exitosamente de Redis.")
        except Exception as e:
            logger.error(f"[ARQ Worker] Error al eliminar candado '{lock_key}' en Redis: {e}")

        # Limpieza defensiva de carpeta temporal si quedó residual
        tmp_dir = os.path.join("backups", f"tmp_{job_id}")
        if os.path.exists(tmp_dir):
            try:
                await async_rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                pass


async def master_dispatcher_task(ctx: dict) -> None:
    """
    Despachador Maestro Periódico de ARQ (ejecutado cada 1 minuto por el cron integrado de WorkerSettings).
    1. Captura el tiempo actual estricto en UTC: now_utc = datetime.now(timezone.utc).
    2. Consulta en MongoDB todas las tareas programadas activas cuya fecha de ejecución haya vencido (next_run_time <= now_utc).
    3. Itera de forma aislada sobre cada tarea:
       a. Encola la tarea en ARQ usando ctx['redis'].enqueue_job(task.task_name, payload=task.payload).
       b. Calcula la próxima fecha de ejecución interpretando la expresión cron en la zona horaria configurada y convirtiéndola a UTC.
       c. Actualiza task.next_run_time, task.last_run_status y persiste en MongoDB.
    """
    now_utc = datetime.now(timezone.utc)
    logger.info(f"[ARQ Master Dispatcher] Ejecutando ciclo de evaluación a las {now_utc.isoformat()} (UTC)...")

    due_tasks = await TBScheduledTask.find(
        TBScheduledTask.is_active == True,
        TBScheduledTask.next_run_time <= now_utc
    ).to_list()

    if not due_tasks:
        logger.debug("[ARQ Master Dispatcher] No hay tareas programadas pendientes de ejecución en este ciclo.")
        return

    logger.info(f"[ARQ Master Dispatcher] Se encontraron {len(due_tasks)} tareas programadas pendientes de despacho.")
    arq_redis = ctx.get("redis") or get_redis_client()

    for task in due_tasks:
        task_id_str = str(task.id)
        try:
            payload = dict(task.payload or {})
            if task.tenant_id is not None:
                t_id = None
                if hasattr(task.tenant_id, "id"):
                    t_id = str(task.tenant_id.id)
                elif hasattr(task.tenant_id, "ref") and task.tenant_id.ref:
                    t_id = str(task.tenant_id.ref.id)
                else:
                    t_id = str(task.tenant_id)
                if t_id:
                    payload["tenant_id"] = t_id

            logger.info(f"[ARQ Master Dispatcher] Despachando tarea '{task.name}' ({task.task_name}) [ID: {task_id_str}]...")

            is_incremental = task.task_name in (
                "tasks.execute_incremental_tenant_backup",
                "execute_incremental_tenant_backup_task",
                "execute_incremental_tenant_backup"
            )
            queue_name = "incremental_backups" if is_incremental else "default"

            # 1. Encolar la tarea en el broker de ARQ
            if hasattr(arq_redis, "enqueue_job"):
                job = await arq_redis.enqueue_job(task.task_name, payload=payload, _queue_name=queue_name)
                dispatched_job_id = job.job_id if job else "unknown"
            else:
                from core.arq_pool import get_arq_pool
                pool = await get_arq_pool()
                job = await pool.enqueue_job(task.task_name, payload=payload, _queue_name=queue_name)
                dispatched_job_id = job.job_id if job else "unknown"

            # 2. Calcular próxima fecha en zona horaria local y convertir a UTC puro
            next_utc = task.compute_next_run()

            # 3. Actualizar metadatos y persistir en MongoDB
            task.next_run_time = next_utc
            task.last_run_status = f"DISPATCHED (ARQ Job ID: {dispatched_job_id})"
            task.last_run_at = now_utc
            task.updated_at = now_utc
            await task.save()

            logger.info(
                f"[ARQ Master Dispatcher] Tarea '{task.name}' despachada con éxito (ARQ ID: {dispatched_job_id}). "
                f"Próxima ejecución calculada para {next_utc.isoformat()} (UTC)."
            )
        except Exception as exc:
            logger.error(
                f"[ARQ Master Dispatcher] Fallo al procesar tarea programada '{task.name}' [ID: {task_id_str}]: {exc}",
                exc_info=True
            )
            try:
                task.last_run_status = f"ERROR: {str(exc)}"
                task.updated_at = datetime.now(timezone.utc)
                await task.save()
            except Exception as save_err:
                logger.critical(
                    f"[ARQ Master Dispatcher] Error crítico al actualizar estado de fallo para tarea '{task.name}': {save_err}"
                )


def _get_directory_latest_mtime(dir_path: str) -> float:
    """Obtiene la marca de tiempo de modificación más reciente de cualquier archivo o subdirectorio dentro de un directorio."""
    latest = os.stat(dir_path).st_mtime
    try:
        for root, dirs, files in os.walk(dir_path):
            for name in files:
                try:
                    f_mtime = os.stat(os.path.join(root, name)).st_mtime
                    if f_mtime > latest:
                        latest = f_mtime
                except OSError:
                    pass
            for name in dirs:
                try:
                    d_mtime = os.stat(os.path.join(root, name)).st_mtime
                    if d_mtime > latest:
                        latest = d_mtime
                except OSError:
                    pass
    except OSError:
        pass
    return latest


async def _is_task_active_in_redis(task_id: str, redis_conn=None) -> bool:
    """Verifica si una tarea sigue registrada como activa en Redis (tb_events:user:*:registry)."""
    try:
        r = redis_conn or get_redis_client()
        async for key in r.scan_iter(match="tb_events:user:*:registry"):
            if await r.hexists(key, task_id):
                return True
    except Exception:
        pass
    return False


async def _execute_cleanup_old_backups(days_to_keep: int = 30, redis_conn=None) -> dict:
    """
    Ejecución asíncrona de la política de retención de respaldos y limpieza del sistema de archivos:
    1. Identificación de registros en TBBackup con created_at <= cutoff_date (30 días por defecto).
    2. Eliminación segura de los archivos ZIP físicos en disco y posterior borrado del registro en MongoDB.
    3. Barrido de carpetas temporales huérfanas 'tmp_*' garantizando que NO se borren respaldos activos de larga duración:
       - Solo se purgan si NO están registradas como activas en Redis Y no han tenido ninguna escritura en las últimas 24 horas.
    4. Retorno de estadísticas completas de la operación.
    """
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
            if backup.file_name:
                file_path = os.path.join(abs_backup_dir, backup.file_name)
                try:
                    if os.path.exists(file_path):
                        await async_remove_file(file_path, ignore_errors=False)
                        stats["deleted_files"] += 1
                        logger.debug(f"[Cleanup Task] Archivo físico eliminado: {file_path}")
                    else:
                        stats["missing_files"] += 1
                        logger.debug(f"[Cleanup Task] Archivo físico ya no existía en disco: {file_path}")
                except Exception as file_err:
                    stats["file_errors"] += 1
                    logger.warning(f"[Cleanup Task] Error al eliminar archivo físico '{file_path}': {file_err}")

            try:
                await backup.delete()
                stats["deleted_db_records"] += 1
                logger.debug(f"[Cleanup Task] Documento TBBackup {backup.id} eliminado de MongoDB.")
            except Exception as db_err:
                logger.error(f"[Cleanup Task] Error al eliminar documento TBBackup {backup.id}: {db_err}")

    except Exception as query_err:
        logger.error(f"[Cleanup Task] Error consultando registros caducados en TBBackup: {query_err}", exc_info=True)
        stats["status"] = "PARTIAL_ERROR"

    # 2. Barrer carpetas temporales zombis / huérfanas (tmp_*) con salvaguarda de inactividad y estado en Redis
    if os.path.exists(abs_backup_dir) and os.path.isdir(abs_backup_dir):
        logger.info(f"[Cleanup Task] Inspeccionando '{abs_backup_dir}' en busca de directorios temporales zombis...")
        current_time = time.time()
        one_day_seconds = 86400  # 24 horas de inactividad absoluta

        try:
            with os.scandir(abs_backup_dir) as entries:
                for entry in entries:
                    if entry.is_dir() and entry.name.startswith("tmp_"):
                        try:
                            task_id_candidate = entry.name[4:]
                            is_active_in_redis = await _is_task_active_in_redis(task_id_candidate, redis_conn)
                            latest_mtime = await asyncio.to_thread(_get_directory_latest_mtime, entry.path)
                            inactivity_seconds = current_time - latest_mtime

                            # Si la tarea está activa en Redis o ha tenido escrituras recientes (< 24h), conservarla
                            if is_active_in_redis or inactivity_seconds <= one_day_seconds:
                                logger.debug(
                                    f"[Cleanup Task] Conservando temporal activo/reciente '{entry.name}' "
                                    f"(Inactividad: {round(inactivity_seconds / 60, 2)} min, Activo en Redis: {is_active_in_redis})."
                                )
                            else:
                                age_hours = round(inactivity_seconds / 3600, 2)
                                logger.info(
                                    f"[Cleanup Task] Purgando directorio temporal zombi '{entry.name}' "
                                    f"(Sin escrituras desde hace {age_hours}h y sin registro activo en Redis)..."
                                )
                                await async_rmtree(entry.path, ignore_errors=True)
                                stats["zombie_dirs_deleted"] += 1
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


async def cleanup_old_backups_task(
    ctx: dict,
    days_to_keep: int = 30,
    payload: Optional[dict] = None,
    **kwargs
) -> dict:
    """
    Tarea asíncrona de ARQ para purgar respaldos antiguos y limpiar temporales zombis.
    Soporta invocación con days_to_keep posicional, vía dict payload={"days_to_keep": ...} o kwargs.
    """
    resolved_days = days_to_keep
    if payload and isinstance(payload, dict):
        if "days_to_keep" in payload:
            resolved_days = int(payload["days_to_keep"])
        elif "days" in payload:
            resolved_days = int(payload["days"])
    elif "days_to_keep" in kwargs:
        resolved_days = int(kwargs["days_to_keep"])

    redis_conn = ctx.get("redis")
    logger.info(f"[ARQ Worker] Iniciando tarea cleanup_old_backups_task con days_to_keep={resolved_days}")
    return await _execute_cleanup_old_backups(days_to_keep=resolved_days, redis_conn=redis_conn)


async def execute_incremental_tenant_backup_task(
    ctx: dict,
    payload: Optional[dict] = None,
    **kwargs
) -> dict:
    """
    Worker asíncrono de ARQ para la descarga incremental de telemetría por Tenant ("Mes Vencido").
    Enrutado a la cola 'incremental_backups'.
    """
    actual_payload = payload if payload is not None else kwargs
    job_id = ctx.get("job_id") or "incremental_" + str(uuid.uuid4())
    job_try = ctx.get("job_try", 1)
    tenant_id = actual_payload.get("tenant_id", "unknown_tenant")
    tenant_name = actual_payload.get("tenant_name", "default")
    logger.info(f"[ARQ Worker] Ejecutando respaldo incremental {job_id} para Tenant '{tenant_name}' ({tenant_id}) [Intento: {job_try}]")

    try:
        return await run_incremental_tenant_backup(actual_payload)
    except asyncio.CancelledError:
        logger.warning(f"[ARQ Worker] Tarea incremental {job_id} CANCELADA / ABORTADA por señal externa.")
        raise
    except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
        if job_try <= 5:
            countdown = 2 ** min(job_try, 5)
            logger.error(f"[ARQ Worker] Fallo transitorio de red en tarea incremental {job_id}: {exc}. Reintentando en {countdown}s...")
            raise Retry(defer=countdown)
        raise exc
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (429, 500, 502, 503, 504) and job_try <= 5:
            countdown = 2 ** min(job_try, 5)
            logger.error(f"[ARQ Worker] Error HTTP transitorio ({exc.response.status_code}) en tarea incremental {job_id}. Reintentando en {countdown}s...")
            raise Retry(defer=countdown)
        raise exc
    except Exception as exc:
        logger.error(f"[ARQ Worker] Error fatal en tarea incremental {job_id}: {exc}", exc_info=True)
        raise exc
