import os
import re
import json
import shutil
import asyncio
import calendar
import httpx
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any

import redis.asyncio as redis

from core.config import settings
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
            "year_str": year_str,
            "month_str": month_str,
            "state": state
        })

        if month == 12:
            current_start = datetime(year + 1, 1, 1, 0, 0, 0, 0, tzinfo=current_start.tzinfo)
        else:
            current_start = datetime(year, month + 1, 1, 0, 0, 0, 0, tzinfo=current_start.tzinfo)

    return intervals


async def download_telemetry_for_key(
    tb: ThingsBoardClient,
    task_id: str,
    user_id: str,
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

            # Checkpoint: Resume from last saved timestamp unless force_reload is True
            if force_reload:
                current_ts = start_ts
            else:
                last_saved_ts = await local_redis.get(cache_key)
                current_ts = int(last_saved_ts) if last_saved_ts else start_ts

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
                    refresh_token = payload.get("refresh_token") or payload.get("REFRESH_TOKEN") or tb.refresh_token
                    if e.response.status_code == 401 and refresh_token:
                        logger.warning("Token expirado (401). Renovando mediante refresh token...")
                        new_tokens = await tb.refresh_jwt_token(refresh_token)
                        token_ref[0] = new_tokens["token"]
                        if "refresh_token" in payload:
                            payload["refresh_token"] = new_tokens["refreshToken"]
                        continue
                    elif e.response.status_code in (500, 502, 503, 504):
                        logger.error(f"Error del servidor de ThingsBoard: {e.response.status_code}. Delegando a Celery Retry.")
                        raise
                    else:
                        raise
                except httpx.RequestError as e:
                    logger.error(f"Error de red: {str(e)}. Delegando a Celery Retry.")
                    raise

                if not data or key not in data or len(data[key]) == 0:
                    break

                records = data[key]
                month_records.extend(records)

                # Actualizar checkpoint
                last_record_ts = records[-1]["ts"]
                current_ts = last_record_ts + 1
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
                dir_path = f"backups/{tenant_name}/{device_name}/{year_str}/{month_str}"
                os.makedirs(dir_path, exist_ok=True)

                if state == "completo":
                    old_partial_1 = os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}_parcial.json")
                    old_partial_2 = os.path.join(dir_path, f"{entity_id}.{safe_key}.{month_str}-{year_str}.parcial.json")
                    if os.path.exists(old_partial_1):
                        os.remove(old_partial_1)
                    if os.path.exists(old_partial_2):
                        os.remove(old_partial_2)

                file_name = f"{entity_id}.{safe_key}.{month_str}-{year_str}_{state}.json"
                file_path = os.path.join(dir_path, file_name)

                records_in_file = len(month_records)
                output_content = {
                    "data": month_records,
                    "length": records_in_file,
                    "records_count": records_in_file
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
    Orquestador asíncrono principal de descarga masiva de telemetría.
    Ejecuta el descubrimiento de dispositivos, particionado de fechas, paginación por marcas de tiempo y empaquetado ZIP.
    """
    token = payload.get("token") or tb.token
    refresh_token = payload.get("refresh_token") or tb.refresh_token
    tenant_name = sanitize_name(payload.get("tenant_name") or payload.get("TENANT_NAME", "default"))
    time_zone_str = payload.get("time_zone") or payload.get("TIME_ZONE", "UTC")
    start_date_str = payload.get("start_date") or payload.get("START_DATE")
    end_date_str = payload.get("end_date") or payload.get("END_DATE")
    entity_type = payload.get("entity_type") or payload.get("ENTITY_TYPE", "DEVICE")
    concurrency_limit = payload.get("concurrency_limit") or payload.get("config", {}).get("concurrency_limit", 3)

    token_ref = [token]

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

        raw_entity_id = payload.get("entity_id")
        entity_ids = []
        if isinstance(raw_entity_id, list):
            entity_ids = [str(x).strip() for x in raw_entity_id if str(x).strip()]
        elif isinstance(raw_entity_id, str) and raw_entity_id.strip():
            if "," in raw_entity_id:
                entity_ids = [x.strip() for x in raw_entity_id.split(",") if x.strip()]
            else:
                entity_ids = [raw_entity_id.strip()]

        if entity_ids:
            for eid in entity_ids:
                dev_name = eid
                try:
                    dev_info = await tb.get_device_by_id(token=token_ref[0], device_id=eid)
                    if dev_info and "name" in dev_info:
                        dev_name = dev_info["name"]
                except Exception as e:
                    logger.warning(f"No se pudo consultar el nombre del dispositivo {eid}: {e}")
                devices_info.append((eid, sanitize_name(dev_name)))
        else:
            # Obtener todos los dispositivos del tenant
            page = 0
            while True:
                try:
                    res = await tb.get_tenant_devices(token=token_ref[0], limit=100, page=page)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 401 and refresh_token:
                        logger.warning("Token expirado (401). Renovando mediante refresh token...")
                        new_tokens = await tb.refresh_jwt_token(refresh_token)
                        token_ref[0] = new_tokens["token"]
                        continue
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

        sem = asyncio.Semaphore(concurrency_limit)
        work_items = []

        for eid, d_name in devices_info:
            try:
                keys = await tb.get_entity_timeseries_keys(token=token_ref[0], entity_type=entity_type, entity_id=eid)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401 and refresh_token:
                    logger.warning("Token expirado (401). Renovando mediante refresh token...")
                    new_tokens = await tb.refresh_jwt_token(refresh_token)
                    token_ref[0] = new_tokens["token"]
                    keys = await tb.get_entity_timeseries_keys(token=token_ref[0], entity_type=entity_type, entity_id=eid)
                else:
                    raise

            for key in keys:
                work_items.append((eid, d_name, key))

        total_units = len(work_items) * len(intervals)
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

        dir_to_zip = f"backups/{tenant_name}"
        zip_filename = f"backups/{task_id}_{tenant_name}.zip"

        if os.path.exists(dir_to_zip):
            shutil.make_archive(zip_filename.replace('.zip', ''), 'zip', dir_to_zip)
            logger.info(f"Empaquetado exitoso: {zip_filename}")
            try:
                shutil.rmtree(dir_to_zip)
                logger.info(f"Carpeta original eliminada tras compresión: {dir_to_zip}")
            except Exception as e:
                logger.warning(f"No se pudo eliminar la carpeta original {dir_to_zip}: {e}")
        else:
            logger.warning(f"No hay datos para empaquetar para el tenant {tenant_name}")

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
