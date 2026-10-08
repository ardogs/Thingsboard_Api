from __future__ import annotations

import asyncio
import os
import gc
import json
import shutil
import time
import uuid
import httpx
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Union, Tuple
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
from core.logger import get_logger
from core.services.task_registry import publish_task_event
from core.io_limiter import async_rmtree, async_remove_file

logger = get_logger("arq_worker")
from core.services.telemetry_service import (
    run_download_orchestrator,
    publish_task_status,
    get_user_stream_channel,
    get_user_registry_key,
    sanitize_name,
    refresh_tenant_tokens_in_db
)
from core.services.excel_report_service import run_excel_report_orchestrator
from core.services.heatmap_report_service import (
    generate_heatmap_report_pdf,
    interpolate_heatmap_placeholders,
    extract_heatmap_whitelist_and_config,
    build_or_normalize_heatmap_matrix
)
from core.services.incremental_backup_service import (
    calculate_previous_month_boundaries,
    run_incremental_tenant_backup
)
import aiosmtplib
from core.services.email_service import send_email_async
from core.services.alert_dispatcher import dispatch_debounced_alert, send_telegram_alert
from core.services.hierarchical_suppression_service import check_parent_gateway_status

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
            cleanup_on_terminal=True,
            task_type="telemetry"
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
                cleanup_on_terminal=True,
                task_type="telemetry"
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
                cleanup_on_terminal=True,
                task_type="telemetry"
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
            cleanup_on_terminal=True,
            task_type="telemetry"
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
            cleanup_on_terminal=True,
            task_type="excel_report"
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
                cleanup_on_terminal=True,
                task_type="excel_report"
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
                cleanup_on_terminal=True,
                task_type="excel_report"
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
            cleanup_on_terminal=True,
            task_type="excel_report"
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
    import re
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
            total_records=0,
            task_type="heatmap"
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
        # Whitelist aislada: Se lee EXCLUSIVAMENTE de custom_metadata.heatmap_config
        heatmap_config = custom_metadata.get("heatmap_config") or {}
        (
            whitelist_keys,
            rules,
            time_zone_str,
            agg_func,
            raw_year,
            raw_month
        ) = extract_heatmap_whitelist_and_config(
            heatmap_config=heatmap_config,
            actual_payload=actual_payload,
            default_timezone=settings.APP_TIMEZONE
        )

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

        SPANISH_MONTHS = [
            "", "enero", "febrero", "marzo", "abril", "mayo", "junio",
            "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
        ]
        default_mes_nombre = SPANISH_MONTHS[m].capitalize() if 1 <= m <= 12 else str(m)

        heatmap_sections: List[dict] = []
        total_datapoints = 0
        telemetry_period_detected: Optional[Tuple[int, int]] = None

        # Si no hay dispositivos activos con heatmap_active, generar reporte informativo
        if not active_devices:
            logger.warning(f"[Heatmap Task] No se encontraron dispositivos con heatmap_active=True para '{tenant_name}'")
            heatmap_sections.append({
                "device_name": "Sin Dispositivos",
                "metric_name": "Sin Datos",
                "data": [["NA"] * 24],
                "rules": rules,
                "title": f"Reporte mensual mapas de calor {default_mes_nombre} {y}",
                "period": period_label,
                "tenant_name": tenant_name,
            })
        elif not whitelist_keys:
            logger.warning(
                f"[Heatmap Task] El tenant '{tenant_name}' no posee una lista blanca de variables configurada "
                f"en custom_metadata.heatmap_config. No se procesará ninguna variable ajena."
            )
            heatmap_sections.append({
                "device_name": active_devices[0][1] if active_devices else "Sin Dispositivos",
                "metric_name": "Sin Variables Admitidas",
                "data": [["NA"] * 24],
                "rules": rules,
                "title": f"Reporte mensual mapas de calor {default_mes_nombre} {y}",
                "period": period_label,
                "tenant_name": tenant_name,
            })
        else:
            # 7. Extraer telemetría vía API para cada dispositivo activo y cada llave permitida
            for d_id, d_name in active_devices:
                # Consultar llaves reales registradas en el dispositivo para resolución inteligente
                try:
                    dev_keys = await tb_client.get_entity_timeseries_keys(entity_id=d_id, token=token_ref[0])
                    if not isinstance(dev_keys, list):
                        dev_keys = []
                except Exception:
                    dev_keys = []

                for t_key in whitelist_keys:
                    # Resolver llave objetivo para el dispositivo según la lista blanca (soporta HM_ y case-insensitive)
                    target_key = t_key
                    if dev_keys:
                        alt_cand = t_key[3:] if t_key.startswith("HM_") else f"HM_{t_key}"
                        matched_key = None

                        if t_key in dev_keys:
                            matched_key = t_key
                        elif alt_cand in dev_keys:
                            matched_key = alt_cand
                        else:
                            ci_map = {k.lower(): k for k in dev_keys}
                            if t_key.lower() in ci_map:
                                matched_key = ci_map[t_key.lower()]
                            elif alt_cand.lower() in ci_map:
                                matched_key = ci_map[alt_cand.lower()]

                        if not matched_key:
                            logger.info(
                                f"[Heatmap Task] Variable '{t_key}' no presente en las llaves del dispositivo '{d_name}'. "
                                f"No se procesará ninguna variable ajena."
                            )
                            continue
                        target_key = matched_key

                    # Intento 1: Rango estricto del mes
                    try:
                        telem_res = await tb_client.get_entity_telemetry(
                            entity_id=d_id,
                            keys=target_key,
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
                                keys=target_key,
                                start_ts=start_ts,
                                end_ts=end_ts,
                                limit=10000,
                                token=token_ref[0]
                            )
                        else:
                            telem_res = {}
                    except Exception as e:
                        logger.warning(f"[Heatmap Task] Error descargando telemetría de '{d_name}' (llave '{target_key}'): {e}")
                        telem_res = {}

                    # Extracción estricta de puntos (únicamente target_key exacto o case-insensitive)
                    points = []
                    if telem_res:
                        if target_key in telem_res and isinstance(telem_res[target_key], list):
                            points = telem_res[target_key]
                        else:
                            for k, v in telem_res.items():
                                if k.lower() == target_key.lower() and isinstance(v, list):
                                    points = v
                                    break

                    # Fallback si no hay puntos en el rango estricto:
                    # Resúmenes mensuales precalculados suelen guardarse con la fecha de cálculo en el mes posterior
                    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                    if not points:
                        try:
                            res_ext = await tb_client.get_entity_telemetry(
                                entity_id=d_id,
                                keys=target_key,
                                start_ts=start_ts,
                                end_ts=now_ms + 86400000,
                                limit=100,
                                token=token_ref[0]
                            )
                            if res_ext:
                                if target_key in res_ext and isinstance(res_ext[target_key], list):
                                    points = res_ext[target_key]
                                else:
                                    for k, v in res_ext.items():
                                        if k.lower() == target_key.lower() and isinstance(v, list):
                                            points = v
                                            break
                        except Exception as e:
                            logger.debug(f"[Heatmap Task] Error en consulta extendida para '{d_name}' (llave '{target_key}'): {e}")

                    if not points:
                        try:
                            res_latest = await tb_client.get_entity_telemetry(
                                entity_id=d_id,
                                keys=target_key,
                                start_ts=0,
                                end_ts=now_ms + 86400000,
                                limit=10,
                                token=token_ref[0]
                            )
                            if res_latest:
                                if target_key in res_latest and isinstance(res_latest[target_key], list):
                                    points = res_latest[target_key]
                                else:
                                    for k, v in res_latest.items():
                                        if k.lower() == target_key.lower() and isinstance(v, list):
                                            points = v
                                            break
                        except Exception as e:
                            logger.debug(f"[Heatmap Task] Error en consulta histórica para '{d_name}' (llave '{target_key}'): {e}")

                    total_datapoints += len(points)

                    matrix_data = None
                    metric_rules = list(rules)

                    # Determinar mes y año del reporte según la fecha de la telemetría leída
                    # "Tomando como base que la fecha escrita en esa telemetria corresponde al mes anterior"
                    sec_year = y
                    sec_month = m

                    if points:
                        date_extracted = None
                        telem_ts = None
                        for pt in points:
                            if not isinstance(pt, dict):
                                continue
                            val = pt.get("value")
                            if isinstance(val, str) and "{" in val:
                                try:
                                    p_data = json.loads(val)
                                    if isinstance(p_data, dict):
                                        d_cand = p_data.get("date") or p_data.get("fecha") or p_data.get("period")
                                        if d_cand:
                                            m_d = re.search(r"(\d{4})[-/](\d{1,2})", str(d_cand))
                                            if m_d:
                                                date_extracted = (int(m_d.group(1)), int(m_d.group(2)))
                                                break
                                except Exception:
                                    pass
                            if pt.get("ts") and telem_ts is None:
                                telem_ts = pt.get("ts")

                        # La fecha del reporte está en función de la telemetría leída menos 1 mes
                        if date_extracted:
                            t_y, t_m = date_extracted
                            if t_m == 1:
                                sec_year = t_y - 1
                                sec_month = 12
                            else:
                                sec_year = t_y
                                sec_month = t_m - 1
                            if telemetry_period_detected is None:
                                telemetry_period_detected = (sec_year, sec_month)
                        elif telem_ts:
                            t_dt = datetime.fromtimestamp(telem_ts / 1000.0, tz=timezone.utc).astimezone(tz)
                            if t_dt.month == 1:
                                sec_year = t_dt.year - 1
                                sec_month = 12
                            else:
                                sec_year = t_dt.year
                                sec_month = t_dt.month - 1
                            if telemetry_period_detected is None:
                                telemetry_period_detected = (sec_year, sec_month)

                    sec_mes_nombre = SPANISH_MONTHS[sec_month].capitalize() if 1 <= sec_month <= 12 else str(sec_month)
                    sec_title = f"Reporte mensual mapas de calor {sec_mes_nombre} {sec_year}"

                    # Caso 1: Verificar si el punto de telemetría es un JSON precalculado {"data": [...], "rules": [...]}
                    for pt in points:
                        if not isinstance(pt, dict):
                            continue
                        val = pt.get("value")
                        if isinstance(val, dict):
                            if "data" in val and isinstance(val["data"], list):
                                matrix_data = val["data"]
                                if "rules" in val and val["rules"]:
                                    metric_rules = val["rules"]
                                break
                        elif isinstance(val, str) and ("data" in val or "{" in val):
                            try:
                                parsed = json.loads(val)
                                if isinstance(parsed, dict) and "data" in parsed and isinstance(parsed["data"], list):
                                    matrix_data = parsed["data"]
                                    if "rules" in parsed and parsed["rules"]:
                                        metric_rules = parsed["rules"]
                                    break
                            except Exception:
                                pass

                    # Normalizar o construir matrix_data (last_day x 24 horas)
                    matrix_data = build_or_normalize_heatmap_matrix(
                        points=points,
                        last_day=last_day,
                        agg_func=agg_func,
                        tz=tz,
                        existing_matrix=matrix_data
                    )

                    heatmap_sections.append({
                        "device_name": d_name,
                        "metric_name": target_key,
                        "data": matrix_data,
                        "rules": metric_rules,
                        "title": sec_title,
                        "period": f"{sec_year}-{sec_month:02d}",
                        "tenant_name": tenant_name,
                    })

            if not heatmap_sections:
                logger.warning(
                    f"[Heatmap Task] Ningún dispositivo activo posee las variables admitidas {whitelist_keys} para '{tenant_name}'"
                )
                heatmap_sections.append({
                    "device_name": active_devices[0][1] if active_devices else "Sin Dispositivos",
                    "metric_name": "Sin Datos",
                    "data": [["NA"] * 24],
                    "rules": rules,
                    "title": f"Reporte mensual mapas de calor {default_mes_nombre} {y}",
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
            total_records=total_datapoints,
            task_type="heatmap"
        )

        # Determinar mes y año efectivos del reporte en función de la telemetría leída menos 1 mes
        if telemetry_period_detected:
            effective_year, effective_month = telemetry_period_detected
            effective_mes_nombre = SPANISH_MONTHS[effective_month].capitalize() if 1 <= effective_month <= 12 else str(effective_month)
            effective_period = f"{effective_year}-{effective_month:02d}"
        else:
            effective_year = y
            effective_month = m
            effective_mes_nombre = default_mes_nombre
            effective_period = period_label

        final_report_title = (
            heatmap_sections[0]["title"]
            if heatmap_sections
            else f"Reporte mensual mapas de calor {effective_mes_nombre} {effective_year}"
        )
        logger.info(
            f"[Heatmap Task] Período efectivo del reporte (telemetría - 1 mes): {effective_period} "
            f"({effective_mes_nombre} {effective_year})"
        )
        logger.info(f"[Heatmap Task] Delegando generación de {len(heatmap_sections)} secciones a heatmap_report_service: {output_pdf_path}")
        await generate_heatmap_report_pdf(
            matrix_data=heatmap_sections,
            rules=rules,
            output_pdf_path=output_pdf_path,
            title=final_report_title,
            subtitle=f"Tenant: <b>{tenant_name}</b> | Período: <b>{effective_period}</b>",
            metadata={
                "tenant_name": tenant_name,
                "key": ", ".join(whitelist_keys) if whitelist_keys else "General",
                "period": effective_period,
                "year": effective_year
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
                backup_type="heatmap",
                start_date=start_dt,
                end_date=end_dt,
                file_size_bytes=file_size,
                created_at=datetime.now(timezone.utc)
            )
            await backup_record.insert()
            logger.info(f"[MongoDB] Reporte Heatmap registrado en TBBackup: {pdf_filename} ({file_size} bytes)")
        except Exception as bkp_err:
            logger.warning(f"[MongoDB] Advertencia registrando en TBBackup: {bkp_err}")

        # 10. Envío por correo electrónico del último archivo generado si la bandera está activa
        email_sent_status = False
        email_delivery_data = None

        email_opts = actual_payload.get("email_options") or {}
        if not isinstance(email_opts, dict):
            email_opts = {}

        send_email_flag = bool(
            actual_payload.get("send_email")
            or actual_payload.get("email_enabled")
            or email_opts.get("enabled")
        )

        if send_email_flag:
            logger.info(f"[Heatmap Task] Envío por correo electrónico activado para tarea {job_id}.")
            await publish_task_status(
                redis_client=redis_conn,
                user_id=user_id,
                task_id=job_id,
                status="SENDING_EMAIL",
                tenant_name=tenant_name,
                progress_pct=90.0,
                total_records=total_datapoints,
                task_type="heatmap"
            )

            try:
                # Cargar configuración SMTP dinámica de MongoDB (Singleton)
                email_config = await TBEmailConfig.get_singleton()
                if not email_config:
                    email_config = await TBEmailConfig.find_one({"is_active": True})

                if not email_config:
                    error_email_msg = (
                        "No se encontró ninguna configuración de correo activa (TBEmailConfig) en MongoDB. "
                        "Registre o active los parámetros SMTP antes de solicitar el envío de reportes por correo."
                    )
                    logger.error(f"[Heatmap Task] {error_email_msg}")
                    raise ValueError(error_email_msg)

                # 1. De* [opción obtenida de la configuración cargada o personalizada si se provee]
                configured_sender = email_config.sender_email or email_config.username
                custom_from = actual_payload.get("from_email") or email_opts.get("from_email")
                sender = custom_from or configured_sender

                configured_sender_name = email_config.sender_name
                custom_from_name = (
                    actual_payload.get("from_name")
                    or actual_payload.get("sender_name")
                    or email_opts.get("from_name")
                    or email_opts.get("sender_name")
                )
                sender_name = custom_from_name or configured_sender_name

                # 2. Para (destinatario)
                target_to = (
                    actual_payload.get("to_email")
                    or email_opts.get("to_email")
                    or actual_payload.get("recipient")
                    or email_opts.get("recipient")
                )
                if not target_to:
                    raise ValueError("El campo 'to_email' (destinatario) es obligatorio cuando el envío de correo está activo.")

                # 3. Asunto (evaluado con mes y año efectivos: telemetría leída - 1 mes)
                raw_subject = (
                    actual_payload.get("subject")
                    or actual_payload.get("email_subject")
                    or email_opts.get("subject")
                    or f"Reporte Mensual de Mapa de Calor - {tenant_name} ({effective_period})"
                )
                target_subject = interpolate_heatmap_placeholders(
                    text=raw_subject,
                    mes_nombre=effective_mes_nombre,
                    anio=effective_year,
                    period=effective_period,
                    tenant=tenant_name,
                )

                # 4. Con copia (CC)
                target_cc = (
                    actual_payload.get("cc")
                    or actual_payload.get("email_cc")
                    or email_opts.get("cc")
                )

                # 5. Con copia oculta (BCC)
                target_bcc = (
                    actual_payload.get("bcc")
                    or actual_payload.get("email_bcc")
                    or email_opts.get("bcc")
                )

                # 6. Cuerpo
                custom_body = (
                    actual_payload.get("body")
                    or actual_payload.get("email_body")
                    or actual_payload.get("html_body")
                    or email_opts.get("body")
                    or email_opts.get("html_body")
                )
                if custom_body:
                    email_body_html = interpolate_heatmap_placeholders(
                        text=custom_body,
                        mes_nombre=effective_mes_nombre,
                        anio=effective_year,
                        period=effective_period,
                        tenant=tenant_name,
                    )
                else:
                    email_body_html = f"""
                    <div style="font-family: Arial, sans-serif; color: #1e293b; max-width: 650px; margin: 0 auto; padding: 24px; border: 1px solid #e2e8f0; border-radius: 10px; background-color: #ffffff;">
                        <div style="background-color: #0f172a; padding: 16px; border-radius: 8px; margin-bottom: 20px;">
                            <h2 style="color: #ffffff; margin: 0; font-size: 20px;">TKmE CLOUD - Reporte de Mapas de Calor</h2>
                        </div>
                        <p style="font-size: 15px; line-height: 1.5;">Estimado usuario,</p>
                        <p style="font-size: 14px; line-height: 1.6; color: #334155;">
                            Se ha generado exitosamente el <strong>Reporte Mensual de Mapa de Calor</strong> para el tenant <strong>{tenant_name}</strong> correspondiente al período <strong>{effective_period}</strong> ({effective_mes_nombre} {effective_year}).
                        </p>
                        <div style="background-color: #f8fafc; border-left: 4px solid #0284c7; padding: 12px 16px; margin: 20px 0; border-radius: 4px;">
                            <ul style="margin: 0; padding-left: 20px; font-size: 13px; color: #475569; line-height: 1.8;">
                                <li><strong>Tenant:</strong> {tenant_name}</li>
                                <li><strong>Período evaluado:</strong> {effective_period} ({effective_mes_nombre} {effective_year})</li>
                                <li><strong>Dispositivos activos evaluados:</strong> {len(active_devices)}</li>
                                <li><strong>Total de puntos de telemetría:</strong> {total_datapoints}</li>
                                <li><strong>Archivo adjunto:</strong> {pdf_filename} ({file_size} bytes)</li>
                            </ul>
                        </div>
                        <p style="font-size: 14px; color: #334155;">
                            Adjunto a este correo encontrará el documento PDF generado con la visualización completa.
                        </p>
                        <hr style="border: 0; border-top: 1px solid #e2e8f0; margin: 24px 0 16px 0;" />
                        <p style="font-size: 12px; color: #94a3b8; margin: 0;">
                            Generado automáticamente por ThingsBoard Super API Gateway.
                        </p>
                    </div>
                    """

                # 7. Despachar correo adjuntando el último archivo creado (output_pdf_path)
                plain_smtp_password = await email_config.get_password()
                email_delivery_data = None
                max_email_attempts = 3
                last_email_err = None
                email_timeout = float(
                    email_opts.get("timeout")
                    or actual_payload.get("timeout")
                    or 120.0
                )

                for email_attempt in range(1, max_email_attempts + 1):
                    try:
                        logger.info(
                            f"[Heatmap Task] Enviando reporte PDF adjunto por correo (Intento {email_attempt}/{max_email_attempts}) "
                            f"hacia '{target_to}' (Timeout: {email_timeout:.1f}s)..."
                        )
                        email_delivery_data = await send_email_async(
                            to_email=target_to,
                            subject=target_subject,
                            html_body=email_body_html,
                            from_email=sender,
                            from_name=sender_name,
                            attachment_paths=[output_pdf_path],
                            cc=target_cc,
                            bcc=target_bcc,
                            host=email_config.host,
                            port=email_config.port,
                            username=email_config.username,
                            password=plain_smtp_password,
                            use_tls=email_config.use_tls,
                            timeout=email_timeout,
                        )
                        email_sent_status = True
                        logger.info(f"[Heatmap Task] Reporte PDF enviado exitosamente por correo hacia '{target_to}'.")
                        break
                    except (aiosmtplib.errors.SMTPReadTimeoutError, aiosmtplib.errors.SMTPServerDisconnected, TimeoutError) as net_err:
                        last_email_err = net_err
                        logger.warning(
                            f"[Heatmap Task] Fallo transitorio de red/timeout enviando correo (Intento {email_attempt}/{max_email_attempts}): {net_err}. "
                            f"Reintentando en {2 ** email_attempt}s..."
                        )
                        if email_attempt < max_email_attempts:
                            await asyncio.sleep(2 ** email_attempt)
                    except Exception as err:
                        last_email_err = err
                        break

                if not email_sent_status and last_email_err:
                    raise last_email_err
            except Exception as email_err:
                email_sent_status = False
                email_delivery_data = {
                    "status": "FAILED",
                    "error": str(email_err),
                    "error_type": type(email_err).__name__,
                    "target_to": target_to if "target_to" in locals() else None,
                }
                logger.error(
                    f"[Heatmap Task] Falló el envío de correo para tarea {job_id}: {email_err}. "
                    f"El reporte PDF fue generado y catalogado exitosamente ({pdf_filename}).",
                    exc_info=True
                )


        # 11. Finalización exitosa
        await publish_task_status(
            redis_client=redis_conn,
            user_id=user_id,
            task_id=job_id,
            status="SUCCESS",
            tenant_name=tenant_name,
            progress_pct=100.0,
            total_records=total_datapoints,
            cleanup_on_terminal=True,
            task_type="heatmap"
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
            "period": period_label,
            "email_sent": email_sent_status,
            "email_delivery": email_delivery_data
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
                cleanup_on_terminal=True,
                task_type="heatmap"
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
    to_email: Optional[Union[str, list[str]]] = None,
    subject: Optional[str] = None,
    html_body: Optional[str] = None,
    text_body: Optional[str] = None,
    body: Optional[str] = None,
    cc: Optional[Union[str, list[str]]] = None,
    bcc: Optional[Union[str, list[str]]] = None,
    attachment_paths: Optional[list[str]] = None,
    payload: Optional[dict] = None,
    **kwargs
) -> dict:
    """
    Tarea asíncrona de ARQ para envío de correos electrónicos con configuración dinámica en MongoDB.
    1. Resuelve el documento TBEmailConfig activo en MongoDB y descifra su contraseña en memoria RAM.
    2. Invoca send_email_async pasando las credenciales descifradas, CC, BCC, cuerpo y rutas locales (attachment_paths).
       Evita la saturación de memoria en Redis al pasar únicamente referencias a rutas de disco local.
    3. Maneja fallos transitorios con arq.Retry y retroceso exponencial.
    4. Bloque finally: Purga de forma asíncrona no bloqueante (asyncio.to_thread(os.remove)) los archivos temporales
       del disco local una vez que el correo se haya enviado o haya fallado de forma definitiva (si delete_attachments=True).
    """
    actual_payload = payload if payload is not None else kwargs
    target_to_email = to_email or actual_payload.get("to_email") or actual_payload.get("to")
    target_subject = subject or actual_payload.get("subject")
    target_body = body or actual_payload.get("body")
    target_html_body = html_body or actual_payload.get("html_body")
    target_text_body = text_body or actual_payload.get("text_body")
    target_cc = cc or actual_payload.get("cc")
    target_bcc = bcc or actual_payload.get("bcc")
    target_attachment_paths = (
        attachment_paths
        if attachment_paths is not None
        else actual_payload.get("attachment_paths") or []
    )
    target_config_id = actual_payload.get("config_id")
    target_from_email = actual_payload.get("from_email") or actual_payload.get("from")
    target_from_name = (
        actual_payload.get("from_name")
        or actual_payload.get("sender_name")
        or kwargs.get("from_name")
        or kwargs.get("sender_name")
    )
    target_user_id = actual_payload.get("user_id") or kwargs.get("user_id")
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

    async def _publish_email_event(evt_status: str, evt_pct: float, evt_msg: str, evt_details: dict, terminal: bool = False):
        if target_user_id:
            try:
                r_cli = ctx.get("redis") or redis_client
                await publish_task_event(
                    redis_client=r_cli,
                    user_id=str(target_user_id),
                    task_id=job_id,
                    status=evt_status,
                    task_type="email",
                    progress_pct=evt_pct,
                    message=evt_msg,
                    details=evt_details,
                    cleanup_on_terminal=terminal
                )
            except Exception as pe:
                logger.debug(f"[ARQ Worker] No se pudo publicar evento de correo {job_id}: {pe}")

    await _publish_email_event(
        evt_status="PROCESSING",
        evt_pct=10.0,
        evt_msg=f"Iniciando envío de correo hacia '{target_to_email}' (Intento {job_try}/5)...",
        evt_details={"to_email": target_to_email, "subject": target_subject, "attempt": job_try},
        terminal=False
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
            # Buscar configuración singleton activa en MongoDB
            email_config = await TBEmailConfig.get_singleton()

        if not email_config:
            # Fallback seguro con diccionario
            email_config = await TBEmailConfig.find_one({"is_active": True})

        if not email_config:
            raise ValueError(
                "No se encontró ninguna configuración de correo (TBEmailConfig) en MongoDB. "
                "Debe registrar un documento TBEmailConfig antes de enviar correos."
            )

        # 2. Descifrar contraseña en memoria RAM
        plain_password = await email_config.get_password()

        # 3. Determinar remitente (target_from_email > sender_email > username) y nombre (target_from_name > sender_name)
        sender = target_from_email or email_config.sender_email or email_config.username
        sender_name = target_from_name or email_config.sender_name

        # 4. Invocar servicio dinámico no bloqueante
        result = await send_email_async(
            to_email=target_to_email,
            subject=target_subject,
            html_body=target_html_body,
            text_body=target_text_body,
            body=target_body,
            from_email=sender,
            from_name=sender_name,
            attachment_paths=target_attachment_paths,
            cc=target_cc,
            bcc=target_bcc,
            host=email_config.host,
            port=email_config.port,
            username=email_config.username,
            password=plain_password,
            use_tls=email_config.use_tls,
        )
        logger.info(f"[ARQ Worker] Tarea send_email_task {job_id} completada exitosamente hacia '{target_to_email}'.")
        await _publish_email_event(
            evt_status="SUCCESS",
            evt_pct=100.0,
            evt_msg=f"Correo enviado exitosamente hacia '{target_to_email}'.",
            evt_details={"to_email": target_to_email, "subject": target_subject, "result": result},
            terminal=True
        )
        return result

    except asyncio.CancelledError:
        logger.warning(f"[ARQ Worker] Tarea send_email_task {job_id} CANCELADA por señal externa.")
        await _publish_email_event(
            evt_status="CANCELLED",
            evt_pct=0.0,
            evt_msg="Tarea de correo cancelada externamente.",
            evt_details={"to_email": target_to_email},
            terminal=True
        )
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
            await _publish_email_event(
                evt_status="RETRYING",
                evt_pct=round(min(90.0, job_try * 18.0), 1),
                evt_msg=f"Fallo transitorio de conexión SMTP: {exc}. Reintentando en {countdown}s (Intento {job_try}/5)...",
                evt_details={"to_email": target_to_email, "attempt": job_try, "error": str(exc), "retry_countdown_seconds": countdown},
                terminal=False
            )
            raise Retry(defer=countdown)
        logger.error(f"[ARQ Worker] Agotados los reintentos (5) para enviar correo en tarea {job_id}: {exc}")
        await _publish_email_event(
            evt_status="FAILURE",
            evt_pct=100.0,
            evt_msg=f"Agotados los reintentos (5) para enviar correo hacia '{target_to_email}': {exc}",
            evt_details={"to_email": target_to_email, "error": str(exc), "attempt": job_try},
            terminal=True
        )
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
            await _publish_email_event(
                evt_status="RETRYING",
                evt_pct=round(min(90.0, job_try * 18.0), 1),
                evt_msg=f"Respuesta SMTP transitoria ({exc.code}). Reintentando en {countdown}s (Intento {job_try}/5)...",
                evt_details={"to_email": target_to_email, "attempt": job_try, "error": f"{exc.code}: {exc.message}"},
                terminal=False
            )
            raise Retry(defer=countdown)
        logger.error(f"[ARQ Worker] Error SMTP no recuperable ({exc.code}: {exc.message}) en tarea {job_id}")
        await _publish_email_event(
            evt_status="FAILURE",
            evt_pct=100.0,
            evt_msg=f"Error SMTP no recuperable ({exc.code}: {exc.message}) al enviar hacia '{target_to_email}'",
            evt_details={"to_email": target_to_email, "error": f"{exc.code}: {exc.message}", "attempt": job_try},
            terminal=True
        )
        raise exc
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (429, 500, 502, 503, 504) and job_try <= 5:
            is_retrying = True
            countdown = 2 ** min(job_try, 5)
            logger.warning(
                f"[ARQ Worker] Error HTTP transitorio ({exc.response.status_code}) en tarea {job_id}. "
                f"Reintentando en {countdown}s (Intento {job_try}/5)..."
            )
            await _publish_email_event(
                evt_status="RETRYING",
                evt_pct=round(min(90.0, job_try * 18.0), 1),
                evt_msg=f"Error HTTP transitorio ({exc.response.status_code}). Reintentando en {countdown}s...",
                evt_details={"to_email": target_to_email, "attempt": job_try, "error": str(exc)},
                terminal=False
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
            await _publish_email_event(
                evt_status="RETRYING",
                evt_pct=round(min(90.0, job_try * 18.0), 1),
                evt_msg=f"Error de red transitorio ({exc}). Reintentando en {countdown}s...",
                evt_details={"to_email": target_to_email, "attempt": job_try, "error": str(exc)},
                terminal=False
            )
            raise Retry(defer=countdown)
        raise exc
    except Exception as exc:
        logger.error(f"[ARQ Worker] Error en send_email_task {job_id}: {exc}", exc_info=True)
        await _publish_email_event(
            evt_status="FAILURE",
            evt_pct=100.0,
            evt_msg=f"Error inesperado al enviar correo hacia '{target_to_email}': {exc}",
            evt_details={"to_email": target_to_email, "error": str(exc), "attempt": job_try},
            terminal=True
        )
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


async def send_telegram_alert_task(ctx: dict, payload: Optional[dict] = None, **kwargs) -> dict:
    """
    Tarea de fondo ARQ 100% asíncrona para el envío de alertas y notificaciones a Telegram Bot API.
    Aplica debouncing en Redis mediante cerraduras distribuidas (SET NX EX) para mitigar tormentas de alertas y spam.
    Maneja reintentos automáticos no bloqueantes con arq.Retry ante errores transitorios de red o límites de tasa (429, 5xx)
    respetando el debouncer.
    """
    actual_payload = {**(payload or {}), **kwargs}
    job_id = ctx.get("job_id") or actual_payload.get("task_id") or actual_payload.get("job_id") or str(uuid.uuid4())
    job_try = ctx.get("job_try", 1)

    message = actual_payload.get("message")
    if not message:
        logger.error(f"[ARQ Worker] Tarea {job_id} fallida: falta el parámetro obligatorio 'message'.")
        return {
            "sent": False,
            "reason": "missing_message",
            "error": "El parámetro 'message' es obligatorio para enviar una alerta a Telegram.",
            "job_id": job_id,
        }

    alert_key = actual_payload.get("alert_key")
    ttl_seconds = int(actual_payload.get("ttl_seconds", 300))
    bot_token = actual_payload.get("bot_token")
    chat_id = actual_payload.get("chat_id")
    parse_mode = actual_payload.get("parse_mode", "HTML")
    disable_web_page_preview = bool(actual_payload.get("disable_web_page_preview", True))
    skip_debounce = bool(actual_payload.get("skip_debounce", False))

    redis_conn = ctx.get("redis") or get_redis_client()
    http_client = ctx.get("http_client")

    logger.info(
        f"[ARQ Worker] Ejecutando send_telegram_alert_task (Job: {job_id}, Intento: {job_try}/5, TTL: {ttl_seconds}s)"
    )

    # --------------------------------------------------------------------------
    # Supresión Jerárquica: Capa 3 (IOTGateway) vs Capa 4 (Sensores Perimetrales)
    # --------------------------------------------------------------------------
    raw_layer = actual_payload.get("layer")
    device_name = actual_payload.get("device_name")
    tenant_id = actual_payload.get("tenant_id")

    is_layer_4 = False
    if raw_layer is not None:
        clean_layer = str(raw_layer).strip().lower()
        is_layer_4 = clean_layer in ("4", "capa 4", "capa4", "layer 4", "layer_4")

    if is_layer_4 and device_name and tenant_id:
        logger.info(
            f"[ARQ Worker] Verificando supresión jerárquica para sensor '{device_name}' (Capa 4, Tenant: '{tenant_id}')..."
        )
        is_parent_inactive, parent_gw_name, parent_meta = await check_parent_gateway_status(
            tenant_id=str(tenant_id),
            device_name=str(device_name),
            redis_conn=redis_conn,
            http_client=http_client,
        )
        if is_parent_inactive:
            logger.info(
                f"[ARQ Worker] Alerta de sensor '{device_name}' (Capa 4) suprimida: "
                f"IOTGateway padre '{parent_gw_name}' se encuentra inactivo."
            )
            return {
                "sent": False,
                "reason": "suppressed_by_parent_layer",
                "device_name": device_name,
                "layer": raw_layer,
                "parent_gateway": parent_gw_name,
                "parent_status": parent_meta,
                "job_id": job_id,
            }

    try:
        result = await dispatch_debounced_alert(
            message=message,
            alert_key=alert_key,
            ttl_seconds=ttl_seconds,
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            skip_debounce=skip_debounce,
            redis_conn=redis_conn,
            http_client=http_client,
            raise_on_error=True,
            release_lock_on_failure=True,
        )
        if result.get("sent"):
            logger.info(
                f"[ARQ Worker] Alerta de Telegram enviada exitosamente en tarea {job_id} "
                f"(MsgID: {result.get('message_id')}, Hash: {result.get('alert_hash')})."
            )
        elif result.get("reason") == "debounced":
            logger.info(
                f"[ARQ Worker] Alerta descartada por debouncer anti-spam en tarea {job_id} "
                f"(Hash: {result.get('alert_hash')})."
            )
        elif result.get("reason") == "missing_credentials":
            logger.warning(
                f"[ARQ Worker] Alerta de Telegram omitida en tarea {job_id}: credenciales no configuradas."
            )

        return result

    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status in (429, 500, 502, 503, 504) and job_try <= 5:
            # Respetar cabecera Retry-After de Telegram si está disponible
            retry_after = exc.response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                countdown = int(retry_after)
            else:
                countdown = 2 ** min(job_try, 5)

            logger.warning(
                f"[ARQ Worker] Error HTTP transitorio ({status}) al enviar alerta Telegram en tarea {job_id}. "
                f"Reintentando en {countdown}s (Intento {job_try}/5)..."
            )
            raise Retry(defer=countdown)

        logger.error(
            f"[ARQ Worker] Error HTTP no recuperable ({status}) en send_telegram_alert_task {job_id}: {exc}"
        )
        return {
            "sent": False,
            "reason": "http_error",
            "status_code": status,
            "error": str(exc),
            "job_id": job_id,
            "job_try": job_try,
        }

    except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
        if job_try <= 5:
            countdown = 2 ** min(job_try, 5)
            logger.warning(
                f"[ARQ Worker] Error de red transitorio ({type(exc).__name__}: {exc}) en send_telegram_alert_task {job_id}. "
                f"Reintentando en {countdown}s (Intento {job_try}/5)..."
            )
            raise Retry(defer=countdown)

        logger.error(f"[ARQ Worker] Error de red agotó los reintentos en tarea {job_id}: {exc}")
        return {
            "sent": False,
            "reason": "network_error",
            "error": str(exc),
            "job_id": job_id,
            "job_try": job_try,
        }

    except Exception as exc:
        logger.error(
            f"[ARQ Worker] Error inesperado en send_telegram_alert_task {job_id}: {exc}",
            exc_info=True,
        )
        return {
            "sent": False,
            "reason": "internal_error",
            "error": str(exc),
            "job_id": job_id,
            "job_try": job_try,
        }



