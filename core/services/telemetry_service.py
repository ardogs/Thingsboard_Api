import os
import re
import json
import shutil
import asyncio
import calendar
import httpx
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple

import aiofiles
import ijson
import redis.asyncio as redis
from beanie import PydanticObjectId

from core.config import settings
from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.tb_client import ThingsBoardClient
from core.logger import logger
from tenacity import (
    retry,
    retry_if_exception,
    wait_exponential,
    stop_after_attempt
)
from core.io_limiter import (
    async_create_zip_archive,
    async_rmtree,
    async_remove_file,
    get_zip_semaphore
)


def sanitize_name(name: str) -> str:
    sanitized = re.sub(r'[\\/*?:"<>|]', '_', str(name)).strip()
    return sanitized if sanitized else "unknown_device"


from core.services.task_registry import (
    get_user_stream_channel,
    get_user_registry_key,
    publish_task_event,
)


async def publish_task_status(
    redis_client: redis.Redis,
    user_id: str,
    task_id: str,
    status: str,
    tenant_name: str,
    current_device: Optional[str] = None,
    current_key: Optional[str] = None,
    progress_pct: float = 0.0,
    total_records: int = 0,
    records_count: Optional[int] = None,
    cleanup_on_terminal: bool = True,
    task_type: str = "telemetry"
) -> dict:
    return await publish_task_event(
        redis_client=redis_client,
        user_id=user_id,
        task_id=task_id,
        status=status,
        task_type=task_type,
        progress_pct=progress_pct,
        cleanup_on_terminal=cleanup_on_terminal,
        tenant_name=tenant_name,
        current_device=current_device,
        current_key=current_key,
        total_records=int(total_records),
        records_count=records_count
    )


def publish_task_status_sync(
    user_id: str,
    task_id: str,
    status: str,
    tenant_name: str,
    current_device: Optional[str] = None,
    current_key: Optional[str] = None,
    progress_pct: float = 0.0,
    total_records: int = 0,
    records_count: Optional[int] = None,
    redis_url: str = settings.REDIS_URL,
    cleanup_on_terminal: bool = True,
    task_type: str = "telemetry"
) -> dict:
    import redis as sync_redis
    normalized_pct = max(0.0, min(100.0, round(float(progress_pct), 2)))
    payload = {
        "task_id": task_id,
        "user_id": user_id,
        "task_type": task_type,
        "status": status,
        "tenant_name": tenant_name,
        "current_device": current_device,
        "current_key": current_key,
        "progress_pct": normalized_pct,
        "total_records": int(total_records),
        "records_count": records_count
    }
    payload_json = json.dumps(payload)
    channel = get_user_stream_channel(user_id, task_id)
    registry_key = get_user_registry_key(user_id)

    try:
        r = sync_redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        r.publish(channel, payload_json)
        if status in ("SUCCESS", "ERROR", "FAILURE") and cleanup_on_terminal:
            r.hdel(registry_key, task_id)
        else:
            r.hset(registry_key, task_id, payload_json)
        r.close()
    except Exception as e:
        logger.error(f"[Redis Status Sync] Error publicando estado para tarea {task_id} (user {user_id}): {e}")

    return payload


def get_month_intervals(start_dt: datetime, end_dt: datetime, now_dt: datetime) -> list[dict]:
    intervals = []
    current_start = start_dt
    while current_start <= end_dt:
        year = current_start.year
        month = current_start.month
        _, last_day = calendar.monthrange(year, month)
        month_end = datetime(year, month, last_day, 23, 59, 59, 999000, tzinfo=current_start.tzinfo)
        period_end = min(month_end, end_dt)
        year_str = f"{year:04d}"
        month_str = f"{month:02d}"

        is_current_month = (year == now_dt.year and month == now_dt.month)
        if is_current_month or period_end < month_end:
            state = "parcial"
        else:
            state = "completo"

        intervals.append({
            "start_ts": int(current_start.timestamp() * 1000),
            "end_ts": int(period_end.timestamp() * 1000),
            "start_date_str": current_start.strftime("%Y%m%d"),
            "end_date_str": period_end.strftime("%Y%m%d"),
            "year_str": year_str,
            "month_str": month_str,
            "state": state
        })

        if month == 12:
            current_start = datetime(year + 1, 1, 1, 0, 0, 0, 0, tzinfo=current_start.tzinfo)
        else:
            current_start = datetime(year, month + 1, 1, 0, 0, 0, 0, tzinfo=current_start.tzinfo)

    return intervals


async def refresh_tenant_tokens_in_db(
    tenant_id: Optional[str],
    tb: ThingsBoardClient,
    token_ref: list,
    payload: dict
) -> Tuple[str, Optional[str]]:
    """
    Manejo resiliente de auto-renovación de tokens JWT ante errores HTTP 401 o arranque en frío:
    a) Intenta renovar el token usando el refresh_token almacenado en el cliente/documento.
    b) Si el refresh_token expiró o falla, realiza login completo usando username y password de TBTenant.
    c) Una vez obtenidos los nuevos token y refresh_token, ACTUALIZA asíncronamente el documento TBTenant en MongoDB.
    d) Actualiza la referencia en memoria para reintentar la petición HTTP sin abortar la tarea.
    """
    refresh_token = payload.get("refresh_token") or tb.refresh_token
    new_token: Optional[str] = None
    new_refresh_token: Optional[str] = None

    # a) Intentar renovar con refresh_token
    if refresh_token:
        logger.warning("[Telemetry Service] HTTP 401 interceptado. Intentando renovación mediante refresh_token...")
        try:
            new_tokens = await tb.refresh_jwt_token(refresh_token)
            new_token = new_tokens.get("token")
            new_refresh_token = new_tokens.get("refreshToken", refresh_token)
            logger.info("[Telemetry Service] Token renovado exitosamente con refresh_token.")
        except Exception as e:
            logger.warning(f"[Telemetry Service] Falló renovación con refresh_token ({e}). Procediendo con fallback de login...")

    # b) Si refresh_token falló o no existe, hacer login completo con credenciales de Tenant Admin
    if not new_token and tb.username and tb.password:
        logger.warning("[Telemetry Service] Ejecutando login completo con credenciales de Tenant Admin...")
        try:
            login_res = await tb.login(tb.username, tb.password)
            if login_res and "token" in login_res:
                new_token = login_res.get("token")
                new_refresh_token = login_res.get("refreshToken")
                logger.info("[Telemetry Service] Re-autenticación exitosa mediante login de credenciales.")
        except Exception as e:
            logger.error(f"[Telemetry Service] Falló re-autenticación con credenciales: {e}")

    if not new_token:
        raise ValueError(
            f"No se pudo autenticar ni renovar tokens para el Tenant (ID: {tenant_id}) en {tb.base_url}."
        )

    # Actualizar estado en memoria
    token_ref[0] = new_token
    tb.token = new_token
    if new_refresh_token:
        tb.refresh_token = new_refresh_token
        payload["refresh_token"] = new_refresh_token
    payload["token"] = new_token

    # c) CRÍTICO: Persistir tokens actualizados en MongoDB (TBTenant) de forma asíncrona
    if tenant_id:
        try:
            try:
                obj_id = PydanticObjectId(tenant_id)
                tenant_doc = await TBTenant.get(obj_id)
            except Exception:
                tenant_doc = await TBTenant.get(tenant_id)

            if tenant_doc:
                tenant_doc.set_tokens(new_token, new_refresh_token)
                tenant_doc.updated_at = datetime.now(timezone.utc)
                await tenant_doc.save()
                logger.info(f"[MongoDB] Documento TBTenant '{tenant_doc.name}' ({tenant_id}) sincronizado en MongoDB con nuevos tokens cifrados.")
        except Exception as e:
            logger.error(f"[MongoDB] Error actualizando tokens en TBTenant ({tenant_id}): {e}")

    return new_token, new_refresh_token


def _sync_get_highest_ts(file_path: str) -> Optional[int]:
    """
    Lee iterativamente un archivo JSON con ijson para encontrar el ts máximo sin cargar el archivo a RAM.
    """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return None
    max_ts = None
    try:
        with open(file_path, "rb") as f:
            for item in ijson.items(f, "data.item", use_float=True):
                if isinstance(item, dict):
                    ts = item.get("ts")
                    if ts is not None:
                        try:
                            parsed_ts = int(ts)
                            if max_ts is None or parsed_ts > max_ts:
                                max_ts = parsed_ts
                        except (ValueError, TypeError):
                            pass
    except Exception as e:
        logger.warning(f"[ijson] Error extrayendo highest_ts de '{file_path}': {e}")
    return max_ts


def _sync_read_records_in_range(file_path: str, start_ts: int, end_ts: int) -> List[dict]:
    """
    Lee iterativamente con ijson los registros dentro del rango [start_ts, end_ts].
    Descarta registros fuera del rango sin acumularlos en memoria.
    """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return []
    records = []
    try:
        with open(file_path, "rb") as f:
            for item in ijson.items(f, "data.item", use_float=True):
                if isinstance(item, dict):
                    ts = item.get("ts")
                    if ts is not None:
                        try:
                            parsed_ts = int(ts)
                            if start_ts <= parsed_ts <= end_ts:
                                records.append(item)
                            elif parsed_ts > end_ts:
                                # Los registros están en orden cronológico ascendente
                                break
                        except (ValueError, TypeError):
                            pass
    except Exception as e:
        logger.warning(f"[ijson] Error leyendo registros en rango [{start_ts} -> {end_ts}] de '{file_path}': {e}")
    return records


async def get_highest_ts_from_partial_file(file_path: str) -> Optional[int]:
    """
    Wrapper asíncrono no bloqueante para extraer el timestamp máximo de un archivo parcial.
    """
    return await asyncio.to_thread(_sync_get_highest_ts, file_path)


async def read_local_telemetry_stream(file_path: str, start_ts: int, end_ts: int) -> List[dict]:
    """
    Wrapper asíncrono no bloqueante para leer registros de telemetría locales en un sub-rango.
    """
    return await asyncio.to_thread(_sync_read_records_in_range, file_path, start_ts, end_ts)


async def calculate_telemetry_delta_plan(
    tenant_name: str,
    device_name: str,
    entity_id: str,
    key: str,
    interval: dict,
    base_storage_dir: str = "tenant_backups"
) -> dict:
    """
    Motor de Intersección (Delta Calculator):
    Compara el rango solicitado [interval['start_ts'], interval['end_ts']] con los archivos existentes en:
    tenant_backups/<TENANT>/<DEVICE>/<AÑO>/<MES>/

    Retorna un diccionario con el plan de ejecución:
    {
        "plan_type": "FULL_LOCAL" | "HYBRID" | "FULL_REMOTE",
        "local_file": Optional[str],
        "local_start_ts": Optional[int],
        "local_end_ts": Optional[int],
        "remote_start_ts": Optional[int],
        "remote_end_ts": Optional[int],
        "max_local_ts": Optional[int]
    }
    """
    safe_tenant = sanitize_name(tenant_name)
    safe_device = sanitize_name(device_name)
    safe_key = sanitize_name(key)
    year_str = interval["year_str"]
    month_str = interval["month_str"]
    req_start = interval["start_ts"]
    req_end = interval["end_ts"]

    dir_path = os.path.join(base_storage_dir, safe_tenant, safe_device, year_str, month_str)

    completo_candidates = [
        os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}.completo.json"),
        os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}_completo.json")
    ]
    completo_file = None
    for cand in completo_candidates:
        if os.path.exists(cand):
            completo_file = cand
            break

    # 1. Caso A: Archivo .completo.json existe
    if completo_file:
        return {
            "plan_type": "FULL_LOCAL",
            "local_file": completo_file,
            "local_start_ts": req_start,
            "local_end_ts": req_end,
            "remote_start_ts": None,
            "remote_end_ts": None,
            "max_local_ts": None
        }

    # 2. Caso B: Archivo .parcial.json existe
    parcial_candidates = [
        os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}.parcial.json"),
        os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}_parcial.json")
    ]
    parcial_file = None
    for cand in parcial_candidates:
        if os.path.exists(cand):
            parcial_file = cand
            break

    if parcial_file:
        max_ts = await get_highest_ts_from_partial_file(parcial_file)
        if max_ts is not None:
            # Si el archivo parcial cubre hasta o más allá del final solicitado:
            if max_ts >= req_end:
                return {
                    "plan_type": "FULL_LOCAL",
                    "local_file": parcial_file,
                    "local_start_ts": req_start,
                    "local_end_ts": req_end,
                    "remote_start_ts": None,
                    "remote_end_ts": None,
                    "max_local_ts": max_ts
                }
            # Si el archivo parcial cubre una porción intermedia [req_start, max_ts]:
            elif req_start <= max_ts < req_end:
                return {
                    "plan_type": "HYBRID",
                    "local_file": parcial_file,
                    "local_start_ts": req_start,
                    "local_end_ts": max_ts,
                    "remote_start_ts": max_ts + 1,
                    "remote_end_ts": req_end,
                    "max_local_ts": max_ts
                }
            # Si max_ts es menor que req_start, el archivo parcial termina antes del rango solicitado
            else:
                return {
                    "plan_type": "FULL_REMOTE",
                    "local_file": None,
                    "local_start_ts": None,
                    "local_end_ts": None,
                    "remote_start_ts": req_start,
                    "remote_end_ts": req_end,
                    "max_local_ts": max_ts
                }

    # 3. Caso C: No existe ningún archivo previo en disco
    return {
        "plan_type": "FULL_REMOTE",
        "local_file": None,
        "local_start_ts": None,
        "local_end_ts": None,
        "remote_start_ts": req_start,
        "remote_end_ts": req_end,
        "max_local_ts": None
    }


def is_retryable_http_exception(exc: BaseException) -> bool:
    """
    Determina si una excepción es transitoria y susceptible de reintento:
    - Problemas de conectividad, TLS, sockets y timeouts de HTTPX / httpcore (ConnectError, ReadTimeout, etc.).
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
async def _safe_fetch_telemetry_page(
    tb: ThingsBoardClient,
    token: str,
    entity_type: str,
    entity_id: str,
    keys: str,
    start_ts: int,
    end_ts: int,
    limit: int
) -> dict:
    return await tb.get_entity_telemetry(
        token=token,
        entity_type=entity_type,
        entity_id=entity_id,
        keys=keys,
        start_ts=start_ts,
        end_ts=end_ts,
        limit=limit
    )


async def fetch_remote_telemetry_range(
    tb: ThingsBoardClient,
    token_ref: list,
    entity_id: str,
    entity_type: str,
    key: str,
    start_ts: int,
    end_ts: int,
    page_limit: int,
    tenant_id: Optional[str],
    tenant_name: str,
    device_name: str,
    payload: dict,
    local_redis: redis.Redis,
    cache_key: str,
    force_reload: bool = False
) -> List[dict]:
    """
    Descarga registros de telemetría de ThingsBoard REST API para el sub-rango [start_ts, end_ts].
    Soporta auto-renovación de tokens en vuelo (HTTP 401), reintentos con tenacity ante fallos de conexión,
    paginación continua y checkpoints en Redis.
    """
    current_ts = start_ts
    if not force_reload:
        last_saved_ts = await local_redis.get(cache_key)
        if last_saved_ts:
            try:
                parsed_ts = int(last_saved_ts)
                if start_ts <= parsed_ts < end_ts:
                    current_ts = parsed_ts
            except (ValueError, TypeError):
                pass

    remote_records: List[dict] = []
    logger.info(f"[{device_name}] [REST API] Consultando '{key}' en rango [{current_ts} -> {end_ts}]")

    while current_ts < end_ts:
        try:
            data = await _safe_fetch_telemetry_page(
                tb=tb,
                token=token_ref[0],
                entity_type=entity_type,
                entity_id=entity_id,
                keys=key,
                start_ts=current_ts,
                end_ts=end_ts,
                limit=page_limit
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.warning(f"[Telemetry Key] 401 detectado al descargar '{key}'. Ejecutando auto-renovación en vuelo...")
                await refresh_tenant_tokens_in_db(
                    tenant_id=tenant_id,
                    tb=tb,
                    token_ref=token_ref,
                    payload=payload
                )
                continue
            else:
                logger.error(f"Error HTTP no recuperable ({e.response.status_code}) al consultar '{key}': {e}")
                raise
        except Exception as e:
            logger.error(f"Error persistente tras 5 reintentos al consultar '{key}': {e}")
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
            logger.info(f"[{device_name}] [REST API] Sin más registros remotos para '{key}' desde ts={current_ts}")
            break

        # Asegurar orden cronológico ascendente
        records = sorted(records, key=lambda x: x.get("ts", 0))
        remote_records.extend(records)

        # Actualizar checkpoint de la tarea con TTL de 24h
        last_record_ts = records[-1]["ts"]
        next_ts = max(current_ts + 1, last_record_ts + 1)
        current_ts = next_ts
        await local_redis.setex(cache_key, 86400, str(current_ts))

        if len(records) < page_limit:
            break

    return remote_records


async def download_telemetry_for_key(
    tb: ThingsBoardClient,
    task_id: str,
    user_id: str,
    tenant_id: Optional[str],
    entity_id: str,
    device_name: str,
    key: str,
    intervals: list[dict],
    tenant_name: str,
    payload: dict,
    token_ref: list,
    sem: asyncio.Semaphore,
    local_redis: redis.Redis,
    progress_tracker: dict,
    progress_lock: asyncio.Lock
):
    """
    Orquestador de descarga para una llave específica:
    1. Calcula el plan delta (Local Data Lake vs ThingsBoard REST API).
    2. Ejecuta lecturas locales con ijson (asyncio.to_thread) y descargas REST concurrentemente.
    3. Trata max_ts en archivos .parcial.json como el nuevo start_date para REST API.
    4. Consolida resultados unificados en backups/tmp_<TASK_ID>/ usando aiofiles.
    """
    async with sem:
        page_limit = payload.get("page_limit") or payload.get("config", {}).get("page_limit", 2000)
        entity_type = payload.get("entity_type") or payload.get("ENTITY_TYPE", "DEVICE")
        base_storage_dir = payload.get("base_storage_dir") or "tenant_backups"
        safe_key = sanitize_name(key)

        for interval in intervals:
            start_ts = interval["start_ts"]
            end_ts = interval["end_ts"]
            year_str = interval["year_str"]
            month_str = interval["month_str"]
            state = interval["state"]

            force_reload = payload.get("force_reload", False)
            cache_key = f"tb_checkpoint:{task_id}:{tenant_name}:{entity_id}:{safe_key}:{year_str}_{month_str}"

            async with progress_lock:
                current_pct = progress_tracker["current_pct"]
                total_recs = progress_tracker["total_records"]

            await publish_task_status(
                redis_client=local_redis,
                user_id=user_id,
                task_id=task_id,
                status="DOWNLOADING",
                tenant_name=tenant_name,
                current_device=device_name,
                current_key=key,
                progress_pct=current_pct,
                total_records=total_recs
            )

            # 1. Motor de Intersección: Calcular Plan de Extracción Híbrido
            if force_reload:
                delta_plan = {
                    "plan_type": "FULL_REMOTE",
                    "local_file": None,
                    "local_start_ts": None,
                    "local_end_ts": None,
                    "remote_start_ts": start_ts,
                    "remote_end_ts": end_ts,
                    "max_local_ts": None
                }
            else:
                delta_plan = await calculate_telemetry_delta_plan(
                    tenant_name=tenant_name,
                    device_name=device_name,
                    entity_id=entity_id,
                    key=key,
                    interval=interval,
                    base_storage_dir=base_storage_dir
                )

            logger.info(
                f"[{device_name}] ({key}) [{month_str}-{year_str}] Plan Delta: {delta_plan['plan_type']} "
                f"(Local: {delta_plan['local_start_ts']}..{delta_plan['local_end_ts']}, "
                f"REST: {delta_plan['remote_start_ts']}..{delta_plan['remote_end_ts']})"
            )

            month_records: List[dict] = []

            # 2. Ejecución Concurrente del Plan
            if delta_plan["plan_type"] == "FULL_LOCAL":
                logger.info(f"[{device_name}] ({key}) 100% de registros leídos desde Data Lake local: {delta_plan['local_file']}")
                month_records = await read_local_telemetry_stream(
                    file_path=delta_plan["local_file"],
                    start_ts=delta_plan["local_start_ts"],
                    end_ts=delta_plan["local_end_ts"]
                )

            elif delta_plan["plan_type"] == "FULL_REMOTE":
                month_records = await fetch_remote_telemetry_range(
                    tb=tb,
                    token_ref=token_ref,
                    entity_id=entity_id,
                    entity_type=entity_type,
                    key=key,
                    start_ts=delta_plan["remote_start_ts"],
                    end_ts=delta_plan["remote_end_ts"],
                    page_limit=page_limit,
                    tenant_id=tenant_id,
                    tenant_name=tenant_name,
                    device_name=device_name,
                    payload=payload,
                    local_redis=local_redis,
                    cache_key=cache_key,
                    force_reload=force_reload
                )

            elif delta_plan["plan_type"] == "HYBRID":
                logger.info(
                    f"[{device_name}] ({key}) Ejecutando extracción HÍBRIDA concurrente: "
                    f"Local [{delta_plan['local_start_ts']} -> {delta_plan['local_end_ts']}] & "
                    f"REST [{delta_plan['remote_start_ts']} -> {delta_plan['remote_end_ts']}]"
                )
                local_task = read_local_telemetry_stream(
                    file_path=delta_plan["local_file"],
                    start_ts=delta_plan["local_start_ts"],
                    end_ts=delta_plan["local_end_ts"]
                )
                remote_task = fetch_remote_telemetry_range(
                    tb=tb,
                    token_ref=token_ref,
                    entity_id=entity_id,
                    entity_type=entity_type,
                    key=key,
                    start_ts=delta_plan["remote_start_ts"],
                    end_ts=delta_plan["remote_end_ts"],
                    page_limit=page_limit,
                    tenant_id=tenant_id,
                    tenant_name=tenant_name,
                    device_name=device_name,
                    payload=payload,
                    local_redis=local_redis,
                    cache_key=cache_key,
                    force_reload=force_reload
                )

                local_records, remote_records = await asyncio.gather(local_task, remote_task)
                month_records = local_records + remote_records

            # 3. Consolidación y Ordenamiento Cronológico
            if month_records:
                month_records.sort(key=lambda x: x.get("ts", 0))

                dir_path = os.path.join("backups", f"tmp_{task_id}", tenant_name, device_name, year_str, month_str)
                os.makedirs(dir_path, exist_ok=True)

                if state == "completo":
                    old_candidates = [
                        os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}.parcial.json"),
                        os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}_parcial.json"),
                    ]
                    for old_file in old_candidates:
                        if os.path.exists(old_file):
                            await async_remove_file(old_file, ignore_errors=True)

                file_name = f"{entity_id}.{safe_key}.{month_str}-{year_str}.{state}.json"
                file_path = os.path.join(dir_path, file_name)

                records_in_file = len(month_records)
                output_content = {
                    "data": month_records,
                    "length": records_in_file
                }

                json_str = json.dumps(output_content, indent=4, ensure_ascii=False, default=str)
                async with aiofiles.open(file_path, mode="w", encoding="utf-8") as f:
                    await f.write(json_str)

                logger.info(
                    f"[{month_str}-{year_str}] - {device_name} ({key}): {records_in_file} registros guardados en {file_name} "
                    f"(Plan: {delta_plan['plan_type']})"
                )

                async with progress_lock:
                    progress_tracker["total_records"] += records_in_file
                    current_total = progress_tracker["total_records"]
                    current_pct = progress_tracker["current_pct"]

                await publish_task_status(
                    redis_client=local_redis,
                    user_id=user_id,
                    task_id=task_id,
                    status="DOWNLOADED",
                    tenant_name=tenant_name,
                    current_device=device_name,
                    current_key=key,
                    progress_pct=current_pct,
                    total_records=current_total,
                    records_count=records_in_file
                )
            else:
                logger.info(f"[{month_str}-{year_str}] - Sin registros para {device_name} ({key}). Llave omitida.")

            async with progress_lock:
                progress_tracker["completed_units"] += 1
                total = max(progress_tracker["total_units"], 1)
                computed_pct = 5.0 + (progress_tracker["completed_units"] / total) * 85.0
                progress_tracker["current_pct"] = round(min(computed_pct, 90.0), 2)


async def run_download_orchestrator(
    task_id: str,
    tb: ThingsBoardClient,
    user_id: str,
    payload: dict,
    redis_client: Optional[redis.Redis] = None
):
    """
    Orquestador asíncrono principal de descarga masiva de telemetría Multi-Tenant.
    Ejecuta el descubrimiento de dispositivos, particionado de fechas, paginación continua,
    auto-renovación de tokens con persistencia en MongoDB y compresión final en archivo ZIP.
    """
    tenant_id = payload.get("tenant_id")
    token = tb.token
    tenant_name = sanitize_name(payload.get("tenant_name") or "default")
    time_zone_str = payload.get("time_zone") or "UTC"
    start_date_str = payload.get("start_date")
    end_date_str = payload.get("end_date")
    entity_type = payload.get("entity_type") or "DEVICE"
    concurrency_limit = payload.get("concurrency_limit") or 3

    token_ref = [token or ""]

    # Validación de arranque en frío si la instancia no tiene token
    if not token_ref[0]:
        await refresh_tenant_tokens_in_db(
            tenant_id=tenant_id,
            tb=tb,
            token_ref=token_ref,
            payload=payload
        )

    tz = ZoneInfo(time_zone_str)
    now_dt = datetime.now(tz)

    def _parse_target_datetime(date_str: str, target_tz: ZoneInfo) -> datetime:
        clean = date_str.strip()
        if clean.endswith("Z"):
            clean = clean[:-1] + "+00:00"
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=target_tz)
        return dt.astimezone(target_tz)

    start_dt = _parse_target_datetime(start_date_str, tz)
    end_dt = _parse_target_datetime(end_date_str, tz)

    intervals = get_month_intervals(start_dt, end_dt, now_dt)
    created_local_redis = False
    if redis_client is not None:
        local_redis = redis_client
    else:
        local_redis = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
        created_local_redis = True

    devices_info = []

    try:
        # Estado inicial: PENDING
        await publish_task_status(
            redis_client=local_redis,
            user_id=user_id,
            task_id=task_id,
            status="PENDING",
            tenant_name=tenant_name,
            current_device=None,
            current_key=None,
            progress_pct=0.0,
            total_records=0
        )

        placeholder_values = {"string", "null", "none", "undefined", "", "{}", "[]"}
        raw_entity_id = payload.get("entity_id")
        entity_ids = []
        if isinstance(raw_entity_id, list):
            entity_ids = [
                str(x).strip() for x in raw_entity_id 
                if str(x).strip() and str(x).strip().lower() not in placeholder_values
            ]
        elif isinstance(raw_entity_id, str) and raw_entity_id.strip():
            if "," in raw_entity_id:
                entity_ids = [
                    x.strip() for x in raw_entity_id.split(",") 
                    if x.strip() and x.strip().lower() not in placeholder_values
                ]
            elif raw_entity_id.strip().lower() not in placeholder_values:
                entity_ids = [raw_entity_id.strip()]

        if entity_ids:
            for eid in entity_ids:
                dev_name = eid
                try:
                    dev_info = await tb.get_device_by_id(token=token_ref[0], device_id=eid)
                    if dev_info and "name" in dev_info:
                        dev_name = dev_info["name"]
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 401:
                        await refresh_tenant_tokens_in_db(
                            tenant_id=tenant_id,
                            tb=tb,
                            token_ref=token_ref,
                            payload=payload
                        )
                        dev_info = await tb.get_device_by_id(token=token_ref[0], device_id=eid)
                        if dev_info and "name" in dev_info:
                            dev_name = dev_info["name"]
                    elif e.response.status_code in (400, 404):
                        raise ValueError(
                            f"El dispositivo con ID '{eid}' no existe o el formato del UUID es inválido en ThingsBoard (HTTP {e.response.status_code})."
                        )
                    else:
                        raise
                except Exception as e:
                    if isinstance(e, ValueError):
                        raise
                    logger.warning(f"No se pudo consultar el nombre del dispositivo {eid}: {e}")
                devices_info.append((eid, sanitize_name(dev_name)))
        else:
            # Obtener todos los dispositivos del tenant con paginación y manejo de 401
            page = 0
            while True:
                try:
                    res = await tb.get_tenant_devices(token=token_ref[0], limit=100, page=page)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 401:
                        await refresh_tenant_tokens_in_db(
                            tenant_id=tenant_id,
                            tb=tb,
                            token_ref=token_ref,
                            payload=payload
                        )
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

        logger.info(f"[Discovery] Total de dispositivos identificados para Tenant '{tenant_name}': {len(devices_info)}")
        if not devices_info:
            logger.warning(f"[Discovery] No se encontraron dispositivos disponibles para el Tenant '{tenant_name}' en ThingsBoard.")

        sem = asyncio.Semaphore(concurrency_limit)
        work_items = []

        for eid, d_name in devices_info:
            try:
                keys = await tb.get_entity_timeseries_keys(token=token_ref[0], entity_type=entity_type, entity_id=eid)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    await refresh_tenant_tokens_in_db(
                        tenant_id=tenant_id,
                        tb=tb,
                        token_ref=token_ref,
                        payload=payload
                    )
                    keys = await tb.get_entity_timeseries_keys(token=token_ref[0], entity_type=entity_type, entity_id=eid)
                elif e.response.status_code in (400, 404):
                    logger.warning(f"[Discovery] No se pudieron obtener llaves para '{d_name}' ({eid}): HTTP {e.response.status_code}")
                    keys = []
                else:
                    raise

            logger.info(f"[Discovery] Dispositivo '{d_name}' ({eid}) reporta {len(keys)} llaves de telemetría: {keys}")
            for key in keys:
                work_items.append((eid, d_name, key))

        total_units = len(work_items) * len(intervals)
        logger.info(f"[Orquestador] Total de unidades de trabajo a procesar: {total_units} (Items: {len(work_items)}, Intervalos: {len(intervals)})")

        progress_tracker = {
            "total_units": max(total_units, 1),
            "completed_units": 0,
            "current_pct": 5.0,
            "total_records": 0
        }
        progress_lock = asyncio.Lock()

        # Notificar inicio de la fase de descarga
        await publish_task_status(
            redis_client=local_redis,
            user_id=user_id,
            task_id=task_id,
            status="DOWNLOADING",
            tenant_name=tenant_name,
            current_device=None,
            current_key=None,
            progress_pct=5.0,
            total_records=0
        )

        tasks = [
            download_telemetry_for_key(
                tb=tb,
                task_id=task_id,
                user_id=user_id,
                tenant_id=tenant_id,
                entity_id=eid,
                device_name=d_name,
                key=key,
                intervals=intervals,
                tenant_name=tenant_name,
                payload=payload,
                token_ref=token_ref,
                sem=sem,
                local_redis=local_redis,
                progress_tracker=progress_tracker,
                progress_lock=progress_lock
            )
            for eid, d_name, key in work_items
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Error en tarea de descarga de llave: {res}")
                raise res

        async with progress_lock:
            final_total_records = progress_tracker["total_records"]

        # Fase de empaquetado
        await publish_task_status(
            redis_client=local_redis,
            user_id=user_id,
            task_id=task_id,
            status="PACKAGING",
            tenant_name=tenant_name,
            current_device=None,
            current_key=None,
            progress_pct=95.0,
            total_records=final_total_records
        )

        start_fmt = start_dt.strftime("%Y%m%d")
        end_fmt = end_dt.strftime("%Y%m%d")
        zip_filename_only = f"{tenant_name}_{start_fmt}_to_{end_fmt}_{task_id}.zip"
        zip_base_name = f"{tenant_name}_{start_fmt}_to_{end_fmt}_{task_id}"
        zip_file_path = os.path.join("backups", zip_filename_only)

        os.makedirs("backups", exist_ok=True)
        tmp_task_dir = os.path.join("backups", f"tmp_{task_id}")
        tenant_dir_in_tmp = os.path.join(tmp_task_dir, tenant_name)

        if os.path.exists(tenant_dir_in_tmp):
            await async_create_zip_archive(
                base_name=os.path.join("backups", zip_base_name),
                root_dir=tmp_task_dir,
                base_dir=tenant_name,
                format="zip"
            )
            logger.info(f"Empaquetado exitoso: {zip_file_path}")
        elif os.path.exists(tmp_task_dir):
            await async_create_zip_archive(
                base_name=os.path.join("backups", zip_base_name),
                root_dir=tmp_task_dir,
                format="zip"
            )
            logger.info(f"Empaquetado exitoso: {zip_file_path}")
        else:
            logger.warning(f"No hay datos para empaquetar para el tenant {tenant_name} (tarea {task_id})")

        # Eliminar completamente la carpeta temporal tmp_{task_id} de forma asíncrona para liberar espacio
        if os.path.exists(tmp_task_dir):
            try:
                await async_rmtree(tmp_task_dir, ignore_errors=True)
                logger.info(f"Carpeta temporal de trabajo eliminada tras compresión: {tmp_task_dir}")
            except Exception as e:
                logger.warning(f"No se pudo eliminar la carpeta temporal {tmp_task_dir}: {e}")

        # Registrar el respaldo en el Catálogo de Respaldos de MongoDB (TBBackup)
        file_size = os.path.getsize(zip_file_path) if os.path.exists(zip_file_path) else 0
        tenant_doc = None
        if tenant_id:
            try:
                obj_id = PydanticObjectId(tenant_id)
                tenant_doc = await TBTenant.get(obj_id)
            except Exception:
                tenant_doc = await TBTenant.get(tenant_id)

        if tenant_doc:
            try:
                backup_record = TBBackup(
                    tenant_id=tenant_doc,
                    task_id=task_id,
                    requested_by=user_id,
                    file_name=zip_filename_only,
                    backup_type="telemetry",
                    start_date=start_dt,
                    end_date=end_dt,
                    file_size_bytes=file_size,
                    created_at=datetime.now(timezone.utc)
                )
                await backup_record.insert()
                logger.info(f"[MongoDB] Catálogo de respaldos: Documento TBBackup guardado exitosamente para tarea {task_id} (Archivo: {zip_filename_only}, Tamaño: {file_size} bytes)")
            except Exception as e:
                logger.error(f"[MongoDB] Error guardando documento TBBackup en base de datos para tarea {task_id}: {e}")
        else:
            logger.warning(f"[MongoDB] No se encontró el documento TBTenant para el ID '{tenant_id}'. TBBackup no registrado.")

        logger.info(f"Tarea {task_id} finalizada exitosamente para {tenant_name} (user {user_id}). Total de registros: {final_total_records}")

        # Finalización exitosa
        await publish_task_status(
            redis_client=local_redis,
            user_id=user_id,
            task_id=task_id,
            status="SUCCESS",
            tenant_name=tenant_name,
            current_device=None,
            current_key=None,
            progress_pct=100.0,
            total_records=final_total_records,
            cleanup_on_terminal=True
        )

    except Exception as exc:
        # Integridad Transaccional: Limpiar de inmediato cualquier directorio temporal huérfano o ZIP corrupto
        if "tmp_task_dir" in locals() and os.path.exists(tmp_task_dir):
            try:
                await async_rmtree(tmp_task_dir, ignore_errors=True)
                logger.info(f"[Transactional Cleanup] Directorio temporal corrupto '{tmp_task_dir}' purgado tras fallo.")
            except Exception:
                pass
        if "zip_file_path" in locals() and os.path.exists(zip_file_path):
            try:
                await async_remove_file(zip_file_path, ignore_errors=True)
                logger.info(f"[Transactional Cleanup] Archivo ZIP incompleto/corrupto '{zip_file_path}' purgado tras fallo.")
            except Exception:
                pass

        try:
            total_recs = progress_tracker.get("total_records", 0) if "progress_tracker" in locals() else 0
            await publish_task_status(
                redis_client=local_redis,
                user_id=user_id,
                task_id=task_id,
                status="ERROR",
                tenant_name=tenant_name,
                current_device=None,
                current_key=None,
                progress_pct=progress_tracker.get("current_pct", 0.0) if "progress_tracker" in locals() else 0.0,
                total_records=total_recs,
                cleanup_on_terminal=True
            )
        except Exception as e:
            logger.error(f"No se pudo publicar el estado de ERROR para {task_id}: {e}")
        raise exc

    finally:
        if created_local_redis and local_redis:
            await local_redis.aclose()
