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

import redis.asyncio as redis
from beanie import PydanticObjectId

from core.config import settings
from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.tb_client import ThingsBoardClient
from core.logger import logger


def sanitize_name(name: str) -> str:
    sanitized = re.sub(r'[\\/*?:"<>|]', '_', str(name)).strip()
    return sanitized if sanitized else "unknown_device"


def get_user_stream_channel(user_id: str, task_id: str) -> str:
    return f"user:{user_id}:stream:{task_id}"


def get_user_registry_key(user_id: str) -> str:
    return f"tb_events:user:{user_id}:registry"


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
    cleanup_on_terminal: bool = True
) -> dict:
    normalized_pct = max(0.0, min(100.0, round(float(progress_pct), 2)))
    payload = {
        "task_id": task_id,
        "user_id": user_id,
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
        await redis_client.publish(channel, payload_json)
        is_terminal = status in ("SUCCESS", "ERROR", "FAILURE")
        if is_terminal and cleanup_on_terminal:
            await redis_client.hdel(registry_key, task_id)
        else:
            await redis_client.hset(registry_key, task_id, payload_json)
    except Exception as e:
        logger.error(f"[Redis Status] Error publicando estado para tarea {task_id} (user {user_id}): {e}")

    return payload


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
    cleanup_on_terminal: bool = True
) -> dict:
    import redis as sync_redis
    normalized_pct = max(0.0, min(100.0, round(float(progress_pct), 2)))
    payload = {
        "task_id": task_id,
        "user_id": user_id,
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
                tenant_doc.token = new_token
                if new_refresh_token:
                    tenant_doc.refresh_token = new_refresh_token
                tenant_doc.updated_at = datetime.now(timezone.utc)
                await tenant_doc.save()
                logger.info(f"[MongoDB] Documento TBTenant '{tenant_doc.name}' ({tenant_id}) sincronizado en MongoDB con nuevos tokens.")
        except Exception as e:
            logger.error(f"[MongoDB] Error actualizando tokens en TBTenant ({tenant_id}): {e}")

    return new_token, new_refresh_token


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
    async with sem:
        page_limit = payload.get("page_limit") or payload.get("config", {}).get("page_limit", 2000)
        entity_type = payload.get("entity_type") or payload.get("ENTITY_TYPE", "DEVICE")
        safe_key = sanitize_name(key)

        for interval in intervals:
            start_ts = interval["start_ts"]
            end_ts = interval["end_ts"]
            year_str = interval["year_str"]
            month_str = interval["month_str"]
            state = interval["state"]

            force_reload = payload.get("force_reload", False)
            cache_key = f"tb_backup:{tenant_name}:{entity_id}:{key}:{year_str}_{month_str}:last_ts"

            # Checkpoint: Reanudar desde el último timestamp guardado si es válido y está dentro del rango solicitado
            if force_reload:
                current_ts = start_ts
            else:
                last_saved_ts = await local_redis.get(cache_key)
                if last_saved_ts:
                    try:
                        parsed_ts = int(last_saved_ts)
                        if start_ts <= parsed_ts < end_ts:
                            current_ts = parsed_ts
                        else:
                            current_ts = start_ts
                    except (ValueError, TypeError):
                        current_ts = start_ts
                else:
                    current_ts = start_ts

            month_records = []

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

            logger.info(f"[{device_name}] Consultando telemetría para '{key}' en rango [{current_ts} -> {end_ts}] ({month_str}-{year_str})")

            while current_ts < end_ts:
                try:
                    data = await tb.get_entity_telemetry(
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
                    elif e.response.status_code in (500, 502, 503, 504):
                        logger.error(f"Error del servidor ThingsBoard: {e.response.status_code}. Delegando a Celery Retry.")
                        raise
                    else:
                        raise
                except httpx.RequestError as e:
                    logger.error(f"Error de red: {str(e)}. Delegando a Celery Retry.")
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
                    logger.info(f"[{device_name}] Sin más registros para '{key}' a partir de ts={current_ts}")
                    break

                # Asegurar orden cronológico ascendente
                records = sorted(records, key=lambda x: x.get("ts", 0))
                month_records.extend(records)

                # Actualizar checkpoint
                last_record_ts = records[-1]["ts"]
                next_ts = max(current_ts + 1, last_record_ts + 1)
                current_ts = next_ts
                await local_redis.set(cache_key, str(current_ts))

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

                if len(records) < page_limit:
                    break

            if month_records:
                dir_path = os.path.join("backups", f"tmp_{task_id}", tenant_name, device_name, year_str, month_str)
                os.makedirs(dir_path, exist_ok=True)

                if state == "completo":
                    old_candidates = [
                        os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}.parcial.json"),
                        os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}_parcial.json"),
                    ]
                    for old_file in old_candidates:
                        if os.path.exists(old_file):
                            try:
                                os.remove(old_file)
                            except Exception:
                                pass

                file_name = f"{entity_id}.{safe_key}.{month_str}-{year_str}.{state}.json"
                file_path = os.path.join(dir_path, file_name)

                records_in_file = len(month_records)
                output_content = {
                    "data": month_records,
                    "length": records_in_file
                }

                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(output_content, f, indent=4)

                logger.info(f"[{month_str}-{year_str}] - {device_name} ({key}): {records_in_file} registros guardados en {file_name}")

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
    payload: dict
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

    start_dt = datetime.fromisoformat(start_date_str).replace(tzinfo=tz)
    end_dt = datetime.fromisoformat(end_date_str).replace(tzinfo=tz)

    intervals = get_month_intervals(start_dt, end_dt, now_dt)
    local_redis = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)

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
            shutil.make_archive(
                base_name=os.path.join("backups", zip_base_name),
                format="zip",
                root_dir=tmp_task_dir,
                base_dir=tenant_name
            )
            logger.info(f"Empaquetado exitoso: {zip_file_path}")
        elif os.path.exists(tmp_task_dir):
            shutil.make_archive(
                base_name=os.path.join("backups", zip_base_name),
                format="zip",
                root_dir=tmp_task_dir
            )
            logger.info(f"Empaquetado exitoso: {zip_file_path}")
        else:
            logger.warning(f"No hay datos para empaquetar para el tenant {tenant_name} (tarea {task_id})")

        # Eliminar completamente la carpeta temporal tmp_{task_id} para liberar espacio
        if os.path.exists(tmp_task_dir):
            try:
                shutil.rmtree(tmp_task_dir, ignore_errors=True)
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
        await local_redis.aclose()
