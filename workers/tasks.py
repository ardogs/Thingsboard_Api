import asyncio
import os
import gc
import json
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
from core.models.tb_email_config import TBEmailConfig
from core.tb_client import ThingsBoardClient
from core.redis_client import redis_client, get_redis_client
from core.logger import logger
from core.io_limiter import async_rmtree, async_remove_file
from core.services.telemetry_service import (
    run_download_orchestrator,
    publish_task_status,
    get_user_stream_channel,
    get_user_registry_key,
    sanitize_name,
    refresh_tenant_tokens_in_db
)
from core.services.excel_report_service import run_excel_report_orchestrator
from core.services.heatmap_report_service import generate_heatmap_report_pdf
from core.services.incremental_backup_service import (
    calculate_previous_month_boundaries,
    run_incremental_tenant_backup
)
import aiosmtplib
from core.services.email_service import send_email_async

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

    # Resolver fechas para reporte Excel si se especificaron year/month o si se omitieron (ejecución periódica de mes cerrado)
    if not actual_payload.get("start_date") or not actual_payload.get("end_date"):
        raw_year = actual_payload.get("year") or actual_payload.get("year_str")
        raw_month = actual_payload.get("month") or actual_payload.get("month_str")
        tz_str = actual_payload.get("time_zone") or settings.APP_TIMEZONE
        if raw_year and raw_month:
            import calendar
            from zoneinfo import ZoneInfo
            y = int(raw_year)
            m = int(raw_month)
            tz = ZoneInfo(tz_str)
            _, last_day = calendar.monthrange(y, m)
            s_dt = datetime(y, m, 1, 0, 0, 0, 0, tzinfo=tz)
            e_dt = datetime(y, m, last_day, 23, 59, 59, 999000, tzinfo=tz)
            actual_payload["start_date"] = s_dt.isoformat()
            actual_payload["end_date"] = e_dt.isoformat()
        else:
            s_dt, e_dt, _, _, _, _ = calculate_previous_month_boundaries(tz_name=tz_str)
            actual_payload["start_date"] = s_dt.isoformat()
            actual_payload["end_date"] = e_dt.isoformat()

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


async def generate_monthly_heatmap_task(
    ctx: dict,
    tenant_id: Optional[str] = None,
    payload: Optional[dict] = None,
    **kwargs
) -> dict:
    """
    Tarea de ARQ para la generación de reportes mensuales de Mapas de Calor (Heatmaps) en formato PDF.

    Flujo de ejecución:
    1. Resuelve el Tenant y el TBServer asociado en MongoDB con Beanie ODM.
    2. Adquiere candado distribuido e inicia heartbeat no bloqueante en background.
    3. Consulta los dispositivos del Tenant e itera consultando atributos de servidor buscando heatmap_active == True o 'true'.
    4. Lee custom_metadata.heatmap_config (lista blanca de llaves, reglas, periodo, etc.).
    5. Extrae la telemetría correspondiente vía API para los dispositivos activos.
    6. Delega el renderizado y consolidación PDF a heatmap_report_service (aislado en ProcessPoolExecutor).
    7. Guarda el PDF temporalmente en backups/heatmaps/{task_id}_{tenant_name}_heatmap.pdf.
    8. Registra el artefacto en TBBackup (MongoDB).
    9. Bloque finally: garantiza la liberación del candado distribuido y limpieza defensiva del entorno.
    """
    import calendar
    from zoneinfo import ZoneInfo
    import matplotlib.pyplot as plt

    actual_payload = payload if payload is not None else kwargs
    target_tenant_id = tenant_id or actual_payload.get("tenant_id")
    if not target_tenant_id:
        raise ValueError("Se requiere 'tenant_id' para ejecutar generate_monthly_heatmap_task")

    job_id = ctx.get("job_id") or actual_payload.get("task_id") or str(uuid.uuid4())
    job_try = ctx.get("job_try", 1)
    user_id = str(actual_payload.get("user_id") or "system")

    logger.info(f"[Heatmap Task] Iniciando tarea {job_id} para TenantID: {target_tenant_id} (Intento: {job_try})")

    # 1. Resolver Tenant en MongoDB
    try:
        obj_id = PydanticObjectId(target_tenant_id)
        tenant = await TBTenant.get(obj_id)
    except Exception:
        tenant = await TBTenant.get(target_tenant_id)

    if not tenant:
        raise ValueError(f"No se encontró el Tenant de ThingsBoard con ID: {target_tenant_id}")

    server = await tenant.get_server()
    if not server:
        raise ValueError(f"No se encontró el servidor ThingsBoard asociado al Tenant '{tenant.name}'")

    server_id = str(server.id)
    tenant_name = tenant.name
    safe_tenant_name = sanitize_name(tenant_name)

    # 2. Cliente ThingsBoard y credenciales
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

    token_ref = [plain_token or ""]
    if not token_ref[0]:
        if not tenant.username or not plain_password:
            raise ValueError(f"El Tenant '{tenant.name}' no posee credenciales para login inicial.")
        login_res = await tb_client.login(tenant.username, plain_password)
        if not login_res or "token" not in login_res:
            raise ValueError(f"Fallo de autenticación en ThingsBoard para el Tenant '{tenant.name}'.")
        token_ref[0] = login_res["token"]
        tenant.set_tokens(login_res["token"], login_res.get("refreshToken"))
        tenant.updated_at = datetime.now(timezone.utc)
        await tenant.save()

    # 3. Candado Distribuido y Heartbeat
    lock_key = get_server_lock_key(server_id)
    redis_conn = ctx.get("redis") or get_redis_client()
    await redis_conn.expire(lock_key, 3600)
    heartbeat_task = asyncio.create_task(
        _heartbeat_server_lock(redis_conn=redis_conn, lock_key=lock_key, interval_seconds=1800, ttl_seconds=3600)
    )

    heatmaps_dir = os.path.join("backups", "heatmaps")
    os.makedirs(heatmaps_dir, exist_ok=True)
    pdf_filename = f"{job_id}_{safe_tenant_name}_heatmap.pdf"
    output_pdf_path = os.path.join(heatmaps_dir, pdf_filename)

    try:
        # Notificar inicio de tarea
        await publish_task_status(
            redis_client=redis_conn,
            user_id=user_id,
            task_id=job_id,
            status="DOWNLOADING",
            tenant_name=tenant_name,
            progress_pct=10.0,
            total_records=0
        )

        # 4. Consultar dispositivos del Tenant
        devices = []
        page = 0
        while True:
            try:
                res = await tb_client.get_tenant_devices(token=token_ref[0], limit=100, page=page)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    await refresh_tenant_tokens_in_db(
                        tenant_id=str(tenant.id),
                        tb=tb_client,
                        token_ref=token_ref,
                        payload=actual_payload
                    )
                    res = await tb_client.get_tenant_devices(token=token_ref[0], limit=100, page=page)
                else:
                    raise

            dev_batch = res.get("data", [])
            if not dev_batch:
                break
            devices.extend(dev_batch)
            if not res.get("hasNext"):
                break
            page += 1

        logger.info(f"[Heatmap Task] Dispositivos totales recuperados para '{tenant_name}': {len(devices)}")

        # 5. Iterar consultando atributos del servidor buscando heatmap_active == True o 'true'
        active_devices: List[Tuple[str, str]] = []
        for dev in devices:
            d_id = dev["id"]["id"]
            d_name = dev.get("name") or d_id

            try:
                attrs = await tb_client.get_entity_attributes(
                    entity_id=d_id,
                    scope="SERVER_SCOPE",
                    keys="heatmap_active",
                    token=token_ref[0]
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    await refresh_tenant_tokens_in_db(
                        tenant_id=str(tenant.id),
                        tb=tb_client,
                        token_ref=token_ref,
                        payload=actual_payload
                    )
                    attrs = await tb_client.get_entity_attributes(
                        entity_id=d_id,
                        scope="SERVER_SCOPE",
                        keys="heatmap_active",
                        token=token_ref[0]
                    )
                else:
                    logger.warning(f"[Heatmap Task] No se pudieron consultar atributos para '{d_name}': {e}")
                    attrs = []
            except Exception as e:
                logger.warning(f"[Heatmap Task] Error consultando atributos de '{d_name}': {e}")
                attrs = []

            is_active = False
            for attr in attrs:
                if attr.get("key") == "heatmap_active":
                    val = attr.get("value")
                    if val is True or str(val).strip().lower() in ("true", "1", "yes"):
                        is_active = True
                        break
            if is_active:
                active_devices.append((d_id, d_name))

        logger.info(f"[Heatmap Task] Dispositivos con heatmap_active activo: {len(active_devices)} de {len(devices)}")

        # 6. Leer custom_metadata.heatmap_config (lista blanca de llaves y reglas)
        custom_metadata = tenant.custom_metadata or {}
        heatmap_config = custom_metadata.get("heatmap_config") or actual_payload.get("heatmap_config") or {}

        # Whitelist de llaves de telemetría y reglas (soporta formato lista o diccionario)
        if isinstance(heatmap_config, list):
            whitelist_keys = list(heatmap_config)
            rules = actual_payload.get("rules") or []
            time_zone_str = actual_payload.get("time_zone") or settings.APP_TIMEZONE
            agg_func = "AVG"
            raw_year = actual_payload.get("year")
            raw_month = actual_payload.get("month")
        elif isinstance(heatmap_config, dict):
            whitelist_keys = (
                heatmap_config.get("keys")
                or heatmap_config.get("whitelist")
                or heatmap_config.get("telemetry_keys")
                or actual_payload.get("keys")
                or []
            )
            rules = heatmap_config.get("rules") or actual_payload.get("rules") or []
            time_zone_str = (
                heatmap_config.get("time_zone")
                or actual_payload.get("time_zone")
                or settings.APP_TIMEZONE
            )
            agg_func = str(heatmap_config.get("aggregation", "AVG")).strip().upper()
            raw_year = actual_payload.get("year") or heatmap_config.get("year")
            raw_month = actual_payload.get("month") or heatmap_config.get("month")
        else:
            whitelist_keys = actual_payload.get("keys") or []
            rules = actual_payload.get("rules") or []
            time_zone_str = actual_payload.get("time_zone") or settings.APP_TIMEZONE
            agg_func = "AVG"
            raw_year = actual_payload.get("year")
            raw_month = actual_payload.get("month")

        if isinstance(whitelist_keys, str):
            whitelist_keys = [k.strip() for k in whitelist_keys.split(",") if k.strip()]

        tz = ZoneInfo(time_zone_str)

        if raw_year and raw_month:
            y = int(raw_year)
            m = int(raw_month)
            _, last_day = calendar.monthrange(y, m)
            start_dt = datetime(y, m, 1, 0, 0, 0, 0, tzinfo=tz)
            end_dt = datetime(y, m, last_day, 23, 59, 59, 999000, tzinfo=tz)
            period_label = f"{y}-{m:02d}"
        else:
            start_dt, end_dt, _, _, year_str, month_str = calculate_previous_month_boundaries(tz_name=time_zone_str)
            y = int(year_str)
            m = int(month_str)
            _, last_day = calendar.monthrange(y, m)
            period_label = f"{year_str}-{month_str}"

        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
        day_columns = [f"{d:02d}" for d in range(1, last_day + 1)]

        heatmap_sections: List[dict] = []
        total_datapoints = 0

        # Si no hay dispositivos activos con heatmap_active, generar reporte informativo
        if not active_devices:
            logger.warning(f"[Heatmap Task] No se encontraron dispositivos con heatmap_active=True para '{tenant_name}'")
            heatmap_sections.append({
                "device_name": "Sin Dispositivos",
                "metric_name": "Sin Datos",
                "data": [["NA"] * 24],
                "rules": rules,
                "title": f"Reporte mensual {y} | mapas de calor",
                "period": period_label,
                "tenant_name": tenant_name,
            })
        else:
            # Si no se configuró whitelist, consultar llaves del primer dispositivo activo
            if not whitelist_keys:
                first_d_id = active_devices[0][0]
                try:
                    all_keys = await tb_client.get_entity_timeseries_keys(entity_id=first_d_id, token=token_ref[0])
                    whitelist_keys = all_keys[:1] if all_keys else ["telemetry"]
                except Exception:
                    whitelist_keys = ["telemetry"]

            # 7. Extraer telemetría vía API para cada dispositivo activo y cada llave permitida
            for d_id, d_name in active_devices:
                for t_key in whitelist_keys:
                    try:
                        telem_res = await tb_client.get_entity_telemetry(
                            entity_id=d_id,
                            keys=t_key,
                            start_ts=start_ts,
                            end_ts=end_ts,
                            limit=10000,
                            token=token_ref[0]
                        )
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 401:
                            await refresh_tenant_tokens_in_db(
                                tenant_id=str(tenant.id),
                                tb=tb_client,
                                token_ref=token_ref,
                                payload=actual_payload
                            )
                            telem_res = await tb_client.get_entity_telemetry(
                                entity_id=d_id,
                                keys=t_key,
                                start_ts=start_ts,
                                end_ts=end_ts,
                                limit=10000,
                                token=token_ref[0]
                            )
                        else:
                            telem_res = {}
                    except Exception as e:
                        logger.warning(f"[Heatmap Task] Error descargando telemetría de '{d_name}' (llave '{t_key}'): {e}")
                        telem_res = {}

                    points = telem_res.get(t_key, [])
                    total_datapoints += len(points)

                    matrix_data = None
                    metric_rules = list(rules)

                    # Caso 1: Verificar si el punto de telemetría es un JSON precalculado {"data": [...], "rules": [...]}
                    for pt in points:
                        val = pt.get("value")
                        if isinstance(val, str) and "data" in val:
                            try:
                                parsed = json.loads(val)
                                if isinstance(parsed, dict) and "data" in parsed and isinstance(parsed["data"], list):
                                    matrix_data = parsed["data"]
                                    if "rules" in parsed and parsed["rules"]:
                                        metric_rules = parsed["rules"]
                                    break
                            except Exception:
                                pass

                    # Caso 2: Si no es una matriz precalculada, agregar puntos por día y hora (last_day x 24 horas)
                    if matrix_data is None:
                        hourly_buckets = [[[] for _ in range(24)] for _ in range(last_day)]
                        for pt in points:
                            ts = pt.get("ts")
                            val = pt.get("value")
                            if ts is not None and val is not None:
                                try:
                                    num_v = float(val)
                                    pt_dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).astimezone(tz)
                                    if 1 <= pt_dt.day <= last_day and 0 <= pt_dt.hour < 24:
                                        hourly_buckets[pt_dt.day - 1][pt_dt.hour].append(num_v)
                                except (ValueError, TypeError):
                                    pass

                        matrix_data = []
                        for d_idx in range(last_day):
                            row = []
                            for h_idx in range(24):
                                b = hourly_buckets[d_idx][h_idx]
                                if not b:
                                    row.append("NA")
                                elif agg_func == "MAX":
                                    row.append(round(max(b), 2))
                                elif agg_func == "MIN":
                                    row.append(round(min(b), 2))
                                elif agg_func == "SUM":
                                    row.append(round(sum(b), 2))
                                elif agg_func == "LAST":
                                    row.append(round(b[-1], 2))
                                else:  # Default: AVG
                                    row.append(round(sum(b) / len(b), 2))
                            matrix_data.append(row)

                    heatmap_sections.append({
                        "device_name": d_name,
                        "metric_name": t_key,
                        "data": matrix_data,
                        "rules": metric_rules,
                        "title": f"Reporte mensual {y} | mapas de calor",
                        "period": period_label,
                        "tenant_name": tenant_name,
                    })

        # 8. Delegar generación a heatmap_report_service
        await publish_task_status(
            redis_client=redis_conn,
            user_id=user_id,
            task_id=job_id,
            status="PACKAGING",
            tenant_name=tenant_name,
            progress_pct=80.0,
            total_records=total_datapoints
        )

        logger.info(f"[Heatmap Task] Delegando generación de {len(heatmap_sections)} secciones a heatmap_report_service: {output_pdf_path}")
        await generate_heatmap_report_pdf(
            matrix_data=heatmap_sections,
            rules=rules,
            output_pdf_path=output_pdf_path,
            title=f"Reporte mensual {y} | mapas de calor",
            subtitle=f"Tenant: <b>{tenant_name}</b> | Período: <b>{period_label}</b>",
            metadata={
                "tenant_name": tenant_name,
                "key": ", ".join(whitelist_keys) if whitelist_keys else "General",
                "period": period_label,
                "year": y
            }
        )

        file_size = os.path.getsize(output_pdf_path) if os.path.exists(output_pdf_path) else 0

        # 9. Registrar en TBBackup en MongoDB
        try:
            backup_record = TBBackup(
                tenant_id=tenant,
                task_id=job_id,
                requested_by=user_id,
                file_name=pdf_filename,
                start_date=start_dt,
                end_date=end_dt,
                file_size_bytes=file_size,
                created_at=datetime.now(timezone.utc)
            )
            await backup_record.insert()
            logger.info(f"[MongoDB] Reporte Heatmap registrado en TBBackup: {pdf_filename} ({file_size} bytes)")
        except Exception as bkp_err:
            logger.warning(f"[MongoDB] Advertencia registrando en TBBackup: {bkp_err}")

        # 10. Finalización exitosa
        await publish_task_status(
            redis_client=redis_conn,
            user_id=user_id,
            task_id=job_id,
            status="SUCCESS",
            tenant_name=tenant_name,
            progress_pct=100.0,
            total_records=total_datapoints,
            cleanup_on_terminal=True
        )

        return {
            "task_id": job_id,
            "status": "SUCCESS",
            "tenant_name": tenant_name,
            "file_name": pdf_filename,
            "file_path": output_pdf_path,
            "file_size_bytes": file_size,
            "active_devices_count": len(active_devices),
            "total_datapoints": total_datapoints,
            "period": period_label
        }

    except Exception as exc:
        logger.error(f"[Heatmap Task] Error en tarea {job_id}: {exc}", exc_info=True)
        try:
            await publish_task_status(
                redis_client=redis_conn,
                user_id=user_id,
                task_id=job_id,
                status="ERROR",
                tenant_name=tenant_name,
                progress_pct=0.0,
                total_records=0,
                cleanup_on_terminal=True
            )
        except Exception:
            pass
        raise exc

    finally:
        # Cancelar y aguardar tarea de heartbeat
        if "heartbeat_task" in locals() and heartbeat_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        # Liberar candado distribuido en Redis
        if "lock_key" in locals() and "redis_conn" in locals() and redis_conn:
            try:
                await redis_conn.delete(lock_key)
                logger.info(f"[Heatmap Task] Candado '{lock_key}' liberado exitosamente en Redis.")
            except Exception as e:
                logger.warning(f"[Heatmap Task] Error liberando candado '{lock_key}': {e}")

        # Limpieza defensiva del entorno de memoria y recursos
        plt.close("all")
        gc.collect()
        logger.info(f"[Heatmap Task] Bloque finally ejecutado. Entorno limpio para tarea {job_id}.")


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
            queue_name = "incremental_backups" if is_incremental else None

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


async def collect_servers_system_info_task(
    ctx: dict,
    payload: Optional[dict] = None,
    **kwargs
) -> dict:
    """
    Tarea asíncrona de ARQ para recolectar información del uso de CPU, RAM y Disco
    mediante GET /api/admin/systemInfo de cada servidor registrado (o servidor específico).
    Soporta programación periódica mediante TBScheduledTask o disparo manual bajo demanda.
    """
    actual_payload = payload if payload is not None else kwargs
    job_id = ctx.get("job_id") or "sysinfo_" + str(uuid.uuid4())
    job_try = ctx.get("job_try", 1)
    server_id = actual_payload.get("server_id") if actual_payload else None

    logger.info(f"[ARQ Worker] Ejecutando collect_servers_system_info_task {job_id} (server_id={server_id}, Intento: {job_try})")

    try:
        from core.services.system_info_service import collect_all_servers_system_info
        return await collect_all_servers_system_info(server_id=server_id)
    except asyncio.CancelledError:
        logger.warning(f"[ARQ Worker] Tarea systemInfo {job_id} CANCELADA por señal externa.")
        raise
    except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
        if job_try <= 5:
            countdown = 2 ** min(job_try, 5)
            logger.error(f"[ARQ Worker] Fallo transitorio de red en tarea systemInfo {job_id}: {exc}. Reintentando en {countdown}s...")
            raise Retry(defer=countdown)
        raise exc
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (429, 500, 502, 503, 504) and job_try <= 5:
            countdown = 2 ** min(job_try, 5)
            logger.error(f"[ARQ Worker] Error HTTP transitorio ({exc.response.status_code}) en tarea systemInfo {job_id}. Reintentando en {countdown}s...")
            raise Retry(defer=countdown)
        raise exc
    except Exception as exc:
        logger.error(f"[ARQ Worker] Error no recuperable en tarea systemInfo {job_id}: {exc}", exc_info=True)
        raise exc


async def send_email_task(
    ctx: dict,
    to_email: Optional[str] = None,
    subject: Optional[str] = None,
    html_body: Optional[str] = None,
    text_body: Optional[str] = None,
    attachment_paths: Optional[list[str]] = None,
    payload: Optional[dict] = None,
    **kwargs
) -> dict:
    """
    Tarea asíncrona de ARQ para envío de correos electrónicos con configuración dinámica en MongoDB.
    1. Resuelve el documento TBEmailConfig activo en MongoDB y descifra su contraseña en memoria RAM.
    2. Invoca send_email_async pasando las credenciales descifradas y rutas de archivos locales (attachment_paths).
       Evita la saturación de memoria en Redis al pasar únicamente referencias a rutas de disco local.
    3. Maneja fallos transitorios con arq.Retry y retroceso exponencial.
    4. Bloque finally: Purga de forma asíncrona no bloqueante (asyncio.to_thread(os.remove)) los archivos temporales
       del disco local una vez que el correo se haya enviado o haya fallado de forma definitiva.

    Parámetros:
        ctx: Contexto inyectado por el worker de ARQ (job_id, job_try, etc.).
        to_email: Correo electrónico del destinatario.
        subject: Asunto del mensaje.
        html_body: Cuerpo del mensaje formateado en HTML.
        text_body: Cuerpo opcional en texto plano para clientes sin soporte HTML.
        attachment_paths: Lista opcional de rutas a archivos temporales en disco.
        payload: Diccionario alternativo con argumentos.
    """
    actual_payload = payload if payload is not None else kwargs
    target_to_email = to_email or actual_payload.get("to_email")
    target_subject = subject or actual_payload.get("subject")
    target_html_body = html_body or actual_payload.get("html_body")
    target_text_body = text_body or actual_payload.get("text_body")
    target_attachment_paths = (
        attachment_paths
        if attachment_paths is not None
        else actual_payload.get("attachment_paths") or []
    )
    target_config_id = actual_payload.get("config_id")
    target_from_email = actual_payload.get("from_email")
    delete_attachments = actual_payload.get("delete_attachments", True)

    if not target_to_email:
        raise ValueError("El destinatario 'to_email' es obligatorio para la tarea send_email_task.")

    if not target_subject:
        target_subject = "Notificación del Sistema - ThingsBoard Super API Gateway"

    if not target_html_body and not target_text_body:
        target_html_body = f"<p>Notificación automática del sistema generada a las {datetime.now(timezone.utc).isoformat()} UTC.</p>"

    job_id = ctx.get("job_id") or "email_" + str(uuid.uuid4())
    job_try = ctx.get("job_try", 1)
    is_retrying = False

    logger.info(
        f"[ARQ Worker] Ejecutando send_email_task {job_id} hacia '{target_to_email}' "
        f"(Asunto: '{target_subject}', Intento: {job_try}, Adjuntos: {len(target_attachment_paths)})"
    )

    try:
        # 1. Consultar configuración SMTP en MongoDB (Beanie ODM)
        email_config: Optional[TBEmailConfig] = None
        if target_config_id:
            try:
                email_config = await TBEmailConfig.get(PydanticObjectId(target_config_id))
            except Exception:
                email_config = await TBEmailConfig.get(target_config_id)

        if not email_config:
            # Buscar configuración activa por defecto
            email_config = await TBEmailConfig.find_one(TBEmailConfig.is_active == True)

        if not email_config:
            # Fallback al primer registro en MongoDB
            email_config = await TBEmailConfig.find_one()

        if not email_config:
            raise ValueError(
                "No se encontró ninguna configuración de correo (TBEmailConfig) en MongoDB. "
                "Debe registrar un documento TBEmailConfig antes de enviar correos."
            )

        # 2. Descifrar contraseña en memoria RAM
        plain_password = await email_config.get_password()

        # 3. Determinar remitente (target_from_email > sender_email > username)
        sender = target_from_email or email_config.sender_email or email_config.username

        # 4. Invocar servicio dinámico no bloqueante
        result = await send_email_async(
            to_email=target_to_email,
            subject=target_subject,
            html_body=target_html_body,
            text_body=target_text_body,
            from_email=sender,
            attachment_paths=target_attachment_paths,
            host=email_config.host,
            port=email_config.port,
            username=email_config.username,
            password=plain_password,
            use_tls=email_config.use_tls,
        )
        logger.info(f"[ARQ Worker] Tarea send_email_task {job_id} completada exitosamente hacia '{target_to_email}'.")
        return result

    except asyncio.CancelledError:
        logger.warning(f"[ARQ Worker] Tarea send_email_task {job_id} CANCELADA por señal externa.")
        raise
    except (
        aiosmtplib.SMTPConnectTimeoutError,
        aiosmtplib.SMTPReadTimeoutError,
        aiosmtplib.SMTPTimeoutError,
        aiosmtplib.SMTPConnectError,
        aiosmtplib.SMTPServerDisconnected,
        TimeoutError,
        asyncio.TimeoutError,
        ConnectionRefusedError,
        ConnectionResetError,
        OSError,
    ) as exc:
        if job_try <= 5:
            is_retrying = True
            countdown = 2 ** min(job_try, 5)
            logger.warning(
                f"[ARQ Worker] Fallo transitorio de red/timeout SMTP en tarea {job_id}: {exc}. "
                f"Reintentando en {countdown}s (Intento {job_try}/5)..."
            )
            raise Retry(defer=countdown)
        logger.error(f"[ARQ Worker] Agotados los reintentos (5) para enviar correo en tarea {job_id}: {exc}")
        raise exc
    except aiosmtplib.SMTPResponseException as exc:
        # Respuestas 4xx o 503 del servidor SMTP (indisponibilidad temporal)
        if (exc.code in (421, 450, 451, 452, 503) or 400 <= exc.code < 500) and job_try <= 5:
            is_retrying = True
            countdown = 2 ** min(job_try, 5)
            logger.warning(
                f"[ARQ Worker] Error de respuesta SMTP transitorio ({exc.code}: {exc.message}) en tarea {job_id}. "
                f"Reintentando en {countdown}s (Intento {job_try}/5)..."
            )
            raise Retry(defer=countdown)
        logger.error(f"[ARQ Worker] Error SMTP no recuperable ({exc.code}: {exc.message}) en tarea {job_id}")
        raise exc
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (429, 500, 502, 503, 504) and job_try <= 5:
            is_retrying = True
            countdown = 2 ** min(job_try, 5)
            logger.warning(
                f"[ARQ Worker] Error HTTP transitorio ({exc.response.status_code}) en tarea {job_id}. "
                f"Reintentando en {countdown}s (Intento {job_try}/5)..."
            )
            raise Retry(defer=countdown)
        raise exc
    except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
        if job_try <= 5:
            is_retrying = True
            countdown = 2 ** min(job_try, 5)
            logger.warning(
                f"[ARQ Worker] Error de red HTTP transitorio ({exc}) en tarea {job_id}. "
                f"Reintentando en {countdown}s (Intento {job_try}/5)..."
            )
            raise Retry(defer=countdown)
        raise exc
    except Exception as exc:
        logger.error(f"[ARQ Worker] Error en send_email_task {job_id}: {exc}", exc_info=True)
        raise exc
    finally:
        # Limpieza asíncrona de archivos temporales del disco tras envío exitoso o fallo definitivo
        if not is_retrying and target_attachment_paths and delete_attachments:
            logger.info(
                f"[ARQ Worker] Limpiando {len(target_attachment_paths)} archivos adjuntos del disco en bloque finally..."
            )
            for file_path in target_attachment_paths:
                try:
                    exists = await asyncio.to_thread(os.path.exists, file_path)
                    if exists:
                        await asyncio.to_thread(os.remove, file_path)
                        logger.debug(f"[ARQ Worker] Archivo temporal eliminado del disco: {file_path}")
                except Exception as cleanup_err:
                    logger.warning(
                        f"[ARQ Worker] Advertencia al eliminar archivo temporal '{file_path}' en finally: {cleanup_err}"
                    )



