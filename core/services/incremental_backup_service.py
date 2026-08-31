import os
import re
import json
import calendar
import asyncio
import httpx
import aiofiles
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple

from beanie import PydanticObjectId
from tenacity import (
    retry,
    retry_if_exception,
    wait_exponential,
    stop_after_attempt
)

from core.config import settings
from core.database import init_db
from core.models.tb_tenant import TBTenant
from core.models.tb_server import TBServer
from core.tb_client import ThingsBoardClient
from core.logger import logger
from core.io_limiter import async_replace_file, async_remove_file
from core.services.telemetry_service import (
    sanitize_name,
    refresh_tenant_tokens_in_db
)


def calculate_previous_month_boundaries(
    reference_dt: Optional[datetime] = None,
    tz_name: Optional[str] = None
) -> Tuple[datetime, datetime, int, int, str, str]:
    """
    Calcula de manera estricta el primer día (00:00:00.000) y el último día (23:59:59.999)
    del mes calendario anterior al momento de la ejecución, basado en la zona horaria indicada
    (por defecto settings.APP_TIMEZONE, ej: America/Mexico_City).

    Retorna:
        Tuple: (start_dt, end_dt, start_ts_ms, end_ts_ms, year_str, month_str)
    """
    tz_str = tz_name or settings.APP_TIMEZONE
    tz = ZoneInfo(tz_str)

    if reference_dt is None:
        now_local = datetime.now(tz)
    else:
        if reference_dt.tzinfo is None:
            now_local = reference_dt.replace(tzinfo=tz)
        else:
            now_local = reference_dt.astimezone(tz)

    if now_local.month == 1:
        target_year = now_local.year - 1
        target_month = 12
    else:
        target_year = now_local.year
        target_month = now_local.month - 1

    _, last_day = calendar.monthrange(target_year, target_month)

    start_dt = datetime(target_year, target_month, 1, 0, 0, 0, 0, tzinfo=tz)
    end_dt = datetime(target_year, target_month, last_day, 23, 59, 59, 999000, tzinfo=tz)

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    year_str = f"{target_year:04d}"
    month_str = f"{target_month:02d}"

    return start_dt, end_dt, start_ts, end_ts, year_str, month_str


def is_retryable_http_exception(exc: BaseException) -> bool:
    """
    Determina si una excepción es transitoria y susceptible de reintento:
    - Problemas de conectividad y timeouts de HTTPX.
    - Respuestas de error HTTP del servidor o de limitación de tasa (429, 500, 502, 503, 504).
    """
    if isinstance(exc, (
        httpx.TimeoutException,
        httpx.NetworkError,
        httpx.ConnectError,
        httpx.ReadTimeout,
        httpx.ConnectTimeout,
        httpx.WriteTimeout,
        httpx.PoolTimeout,
        httpx.RemoteProtocolError
    )):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    err_str = (str(type(exc)) + " " + str(exc)).lower()
    if any(k in err_str for k in ("connecterror", "connection", "timeout", "reset", "closed", "ssl", "protocol", "httpcore", "broken pipe", "network")):
        return True
    return False


@retry(
    retry=retry_if_exception(is_retryable_http_exception),
    wait=wait_exponential(multiplier=1.5, min=2, max=30),
    stop=stop_after_attempt(10),
    reraise=True
)
async def fetch_telemetry_page_with_retry(
    tb: ThingsBoardClient,
    token: str,
    entity_id: str,
    keys: str,
    start_ts: int,
    end_ts: int,
    entity_type: str = "DEVICE",
    limit: int = 2000
) -> dict:
    """
    Función protegida con reintentos exponenciales (tenacity) para descargar una página de telemetría de ThingsBoard.
    Reintenta ante errores de red y códigos de estado HTTP 429, 500, 502, 503, 504.
    """
    return await tb.get_entity_telemetry(
        entity_id=entity_id,
        keys=keys,
        start_ts=start_ts,
        end_ts=end_ts,
        entity_type=entity_type,
        token=token,
        limit=limit
    )


async def download_incremental_key_telemetry(
    tb: ThingsBoardClient,
    tenant_id: Optional[str],
    tenant_name: str,
    entity_id: str,
    device_name: str,
    key: str,
    start_ts: int,
    end_ts: int,
    year_str: str,
    month_str: str,
    token_ref: list,
    sem: asyncio.Semaphore,
    payload: dict,
    base_storage_dir: str = "tenant_backups"
) -> dict:
    """
    Descarga la telemetría incremental de una llave específica para un dispositivo:
    1. Verifica si el archivo '<ENTITY_UUID>.<KEY>.<MM-AAAA>.completo.json' ya existe. Si existe, omite la descarga.
    2. Utiliza un semáforo asíncrono para limitar la concurrencia interna.
    3. Escribe en streaming hacia un archivo '.parcial.json' para evitar problemas de OOM (Out Of Memory).
    4. Estructura JSON: {"data": [...], "length": X}.
    5. Al finalizar la paginación garantizando que no hay más datos hasta end_ts, renombra atómicamente a '.completo.json'.
    """
    safe_tenant = sanitize_name(tenant_name)
    safe_device = sanitize_name(device_name)
    safe_key = sanitize_name(key)

    dir_path = os.path.join(base_storage_dir, safe_tenant, safe_device, year_str, month_str)
    os.makedirs(dir_path, exist_ok=True)

    final_file_name = f"{entity_id}.{safe_key}.{month_str}-{year_str}.completo.json"
    final_file_path = os.path.join(dir_path, final_file_name)

    # 1. Regla de Idempotencia: Si ya existe el archivo completo, omitir
    if os.path.exists(final_file_path):
        logger.info(f"[{month_str}-{year_str}] - Archivo completo ya existe para '{device_name}' ({key}): {final_file_name}. Omitiendo descarga.")
        return {
            "status": "SKIPPED",
            "file": final_file_path,
            "device": device_name,
            "key": key,
            "records": 0
        }

    partial_file_name = f"{entity_id}.{safe_key}.{month_str}-{year_str}.parcial.json"
    partial_file_path = os.path.join(dir_path, partial_file_name)

    page_limit = int(payload.get("page_limit") or 2000)
    entity_type = str(payload.get("entity_type") or "DEVICE")

    async with sem:
        logger.info(f"[{month_str}-{year_str}] [Slot Adquirido] Iniciando descarga incremental para '{device_name}' ({key}) en rango [{start_ts} -> {end_ts}]")
        current_ts = start_ts
        total_records_written = 0

        # Escritura en streaming asíncrono directo a disco con aiofiles para prevenir OOM
        try:
            async with aiofiles.open(partial_file_path, "w", encoding="utf-8") as f:
                await f.write('{\n  "data": [\n')
                first_record = True

                while current_ts < end_ts:
                    try:
                        data = await fetch_telemetry_page_with_retry(
                            tb=tb,
                            token=token_ref[0],
                            entity_id=entity_id,
                            keys=key,
                            start_ts=current_ts,
                            end_ts=end_ts,
                            entity_type=entity_type,
                            limit=page_limit
                        )
                    except httpx.HTTPStatusError as e:
                        # Manejo de token expirado en vuelo (HTTP 401)
                        if e.response.status_code == 401:
                            logger.warning(f"[Incremental Key] 401 detectado en '{key}'. Renovando token de acceso...")
                            await refresh_tenant_tokens_in_db(
                                tenant_id=tenant_id,
                                tb=tb,
                                token_ref=token_ref,
                                payload=payload
                            )
                            continue
                        else:
                            logger.error(f"[Incremental Key] Error HTTP {e.response.status_code} no recuperable en '{key}': {e}")
                            raise
                    except Exception as exc:
                        logger.error(f"[Incremental Key] Fallo al consultar telemetría para '{device_name}' ({key}): {exc}")
                        raise

                    records = []
                    if data:
                        if key in data and isinstance(data[key], list):
                            records = data[key]
                        else:
                            for k, v in data.items():
                                if k.lower() == key.lower() and isinstance(v, list):
                                    records = v
                                    break
                            if not records and len(data) == 1:
                                first_val = list(data.values())[0]
                                if isinstance(first_val, list):
                                    records = first_val

                    if not records:
                        logger.debug(f"[{month_str}-{year_str}] Sin más registros para '{device_name}' ({key}) desde ts={current_ts}")
                        break

                    # Ordenar registros ascendentemente por timestamp
                    records = sorted(records, key=lambda x: x.get("ts", 0))

                    for rec in records:
                        if not first_record:
                            await f.write(",\n")
                        await f.write("    " + json.dumps(rec, ensure_ascii=False))
                        first_record = False
                        total_records_written += 1

                    # Avanzar puntero de tiempo
                    last_record_ts = records[-1]["ts"]
                    next_ts = max(current_ts + 1, last_record_ts + 1)
                    current_ts = next_ts

                    if len(records) < page_limit:
                        break

                # Cierre de la estructura JSON inyectando el length
                await f.write(f'\n  ],\n  "length": {total_records_written}\n}}\n')

            # Renombrar atómicamente a .completo.json una vez garantizada la completitud
            if os.path.exists(partial_file_path):
                await async_replace_file(partial_file_path, final_file_path)
                logger.info(f"[{month_str}-{year_str}] - '{device_name}' ({key}): {total_records_written} registros completados en {final_file_name}")

        except Exception as exc:
            # Integridad Transaccional: Purgar archivo parcial corrupto en caso de error
            if os.path.exists(partial_file_path):
                try:
                    await async_remove_file(partial_file_path, ignore_errors=True)
                    logger.info(f"[Transactional Cleanup] Archivo parcial corrupto '{partial_file_path}' purgado tras error.")
                except Exception:
                    pass
            raise exc

        return {
            "status": "DOWNLOADED",
            "file": final_file_path,
            "device": device_name,
            "key": key,
            "records": total_records_written
        }


async def run_incremental_tenant_backup(payload: dict) -> dict:
    """
    Ejecuta el respaldo incremental del mes vencido para un Tenant específico:
    1. Resuelve el documento TBTenant y el TBServer padre en MongoDB.
    2. Descifra credenciales con Fernet en memoria RAM y ejecuta arranque en frío si no hay token.
    3. Descubre los dispositivos y llaves de telemetría del tenant.
    4. Procesa concurrentemente las descargas usando asyncio.Semaphore(4).
    5. Retorna resumen detallado de la ejecución.
    """
    await init_db()

    tenant_id = payload.get("tenant_id")
    if not tenant_id:
        raise ValueError("Se requiere 'tenant_id' en el payload de respaldo incremental")

    try:
        obj_id = PydanticObjectId(tenant_id)
        tenant = await TBTenant.get(obj_id)
    except Exception:
        tenant = await TBTenant.get(tenant_id)

    if not tenant:
        raise ValueError(f"No se encontró el Tenant con ID '{tenant_id}' en MongoDB")

    server = await tenant.get_server()
    if not server:
        raise ValueError(f"No se encontró el TBServer asociado al Tenant '{tenant.name}'")

    tenant_name = tenant.name
    logger.info(f"[Incremental Worker] Iniciando respaldo para Tenant: '{tenant_name}' en Servidor: '{server.name}'")

    # Descifrado en RAM
    plain_token = tenant.get_token()
    plain_refresh_token = tenant.get_refresh_token()
    plain_password = tenant.get_password()

    tb = ThingsBoardClient(
        base_url=server.base_url,
        token=plain_token,
        refresh_token=plain_refresh_token,
        username=tenant.username,
        password=plain_password
    )

    token_ref = [plain_token or ""]

    # Arranque en frío si no hay token
    if not token_ref[0]:
        await refresh_tenant_tokens_in_db(
            tenant_id=tenant_id,
            tb=tb,
            token_ref=token_ref,
            payload=payload
        )

    # Parámetros temporales
    start_ts = payload.get("start_ts")
    end_ts = payload.get("end_ts")
    year_str = str(payload.get("year_str") or "")
    month_str = str(payload.get("month_str") or "")

    if not start_ts or not end_ts or not year_str or not month_str:
        # Calcular automáticamente si no vienen en el payload
        _, _, s_ts, e_ts, y_str, m_str = calculate_previous_month_boundaries()
        start_ts = start_ts or s_ts
        end_ts = end_ts or e_ts
        year_str = year_str or y_str
        month_str = month_str or m_str

    concurrency_limit = int(payload.get("concurrency_limit") or 4)
    sem = asyncio.Semaphore(concurrency_limit)
    base_storage_dir = str(payload.get("base_storage_dir") or "tenant_backups")

    # Descubrimiento de dispositivos
    devices_info = []
    page = 0
    while True:
        try:
            res = await tb.get_tenant_devices(token=token_ref[0], limit=100, page=page)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                await refresh_tenant_tokens_in_db(tenant_id=tenant_id, tb=tb, token_ref=token_ref, payload=payload)
                res = await tb.get_tenant_devices(token=token_ref[0], limit=100, page=page)
            else:
                raise

        devices = res.get("data", [])
        if not devices:
            break
        for d in devices:
            d_id = d["id"]["id"]
            d_name = d.get("name") or d_id
            devices_info.append((d_id, sanitize_name(d_name)))
        if not res.get("hasNext"):
            break
        page += 1

    logger.info(f"[Incremental Discovery] Tenant '{tenant_name}' tiene {len(devices_info)} dispositivos registrados.")

    work_items = []
    entity_type = str(payload.get("entity_type") or "DEVICE")

    for eid, d_name in devices_info:
        try:
            keys = await tb.get_entity_timeseries_keys(token=token_ref[0], entity_type=entity_type, entity_id=eid)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                await refresh_tenant_tokens_in_db(tenant_id=tenant_id, tb=tb, token_ref=token_ref, payload=payload)
                keys = await tb.get_entity_timeseries_keys(token=token_ref[0], entity_type=entity_type, entity_id=eid)
            elif e.response.status_code in (400, 404):
                logger.warning(f"[Incremental Discovery] No se pudieron obtener llaves para '{d_name}' ({eid}): HTTP {e.response.status_code}")
                keys = []
            else:
                raise

        for key in keys:
            work_items.append((eid, d_name, key))

    logger.info(f"[Incremental Worker] Total de llaves a procesar para Tenant '{tenant_name}': {len(work_items)} (Concurrencia: {concurrency_limit})")

    tasks = [
        download_incremental_key_telemetry(
            tb=tb,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            entity_id=eid,
            device_name=d_name,
            key=key,
            start_ts=start_ts,
            end_ts=end_ts,
            year_str=year_str,
            month_str=month_str,
            token_ref=token_ref,
            sem=sem,
            payload=payload,
            base_storage_dir=base_storage_dir
        )
        for eid, d_name, key in work_items
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    summary = {
        "status": "SUCCESS",
        "tenant_id": tenant_id,
        "tenant_name": tenant_name,
        "year_str": year_str,
        "month_str": month_str,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "total_keys": len(work_items),
        "downloaded_keys": 0,
        "skipped_keys": 0,
        "failed_keys": 0,
        "total_records": 0,
        "errors": []
    }

    for res in results:
        if isinstance(res, Exception):
            summary["failed_keys"] += 1
            summary["errors"].append(str(res))
            logger.error(f"[Incremental Worker] Error en llave individual: {res}")
        elif isinstance(res, dict):
            if res.get("status") == "SKIPPED":
                summary["skipped_keys"] += 1
            elif res.get("status") == "DOWNLOADED":
                summary["downloaded_keys"] += 1
                summary["total_records"] += int(res.get("records", 0))

    if summary["failed_keys"] > 0:
        summary["status"] = "PARTIAL_ERROR"

    logger.info(
        f"[Incremental Worker] Respaldo finalizado para Tenant '{tenant_name}'. "
        f"Completados: {summary['downloaded_keys']}, Omitidos: {summary['skipped_keys']}, "
        f"Fallidos: {summary['failed_keys']}, Total Registros: {summary['total_records']}"
    )
    return summary
