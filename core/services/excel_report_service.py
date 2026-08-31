import os
import re
import gc
import json
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple

import pandas as pd
import aiofiles
import httpx
import redis.asyncio as redis
from beanie import PydanticObjectId
from openpyxl.utils import get_column_letter

from core.config import settings
from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.tb_client import ThingsBoardClient
from core.logger import logger
from core.io_limiter import (
    async_create_zip_archive,
    async_rmtree,
    async_remove_file,
    get_zip_semaphore
)
from core.services.telemetry_service import (
    sanitize_name,
    get_month_intervals,
    refresh_tenant_tokens_in_db,
    calculate_telemetry_delta_plan,
    read_local_telemetry_stream,
    fetch_remote_telemetry_range,
    publish_task_status
)


def _autofit_worksheet_columns(worksheet, min_width: int = 12, padding: int = 4, max_width: int = 60) -> None:
    """
    Ajusta dinámicamente el ancho de todas las columnas de una hoja de openpyxl
    al tamaño del contenido más largo presente en cada columna (incluyendo encabezados).
    """
    for col in worksheet.columns:
        if not col:
            continue
        first_cell = col[0]
        col_letter = first_cell.column_letter if hasattr(first_cell, "column_letter") else get_column_letter(first_cell.column)

        max_len = 0
        for cell in col:
            val = cell.value
            if val is not None:
                val_str = str(val)
                lines = val_str.split("\n")
                cell_len = max(len(l) for l in lines) if lines else len(val_str)
                if cell_len > max_len:
                    max_len = cell_len

        calculated_width = max(max_len + padding, min_width)
        worksheet.column_dimensions[col_letter].width = min(calculated_width, max_width)


def sanitize_sheet_name(name: str) -> str:
    """
    Sanitiza el nombre de una hoja de cálculo en Excel:
    - Remueve caracteres prohibidos por Excel: [ \\ / * ? : [ ] ]
    - Limita la longitud a 31 caracteres máximos.
    """
    sanitized = re.sub(r'[\\/*?:\[\]]', '_', str(name)).strip()
    return sanitized[:31] if sanitized else "Sheet1"


def _build_dataframe_from_telemetry(
    device_data: Dict[str, List[dict]],
    time_zone_str: str
) -> pd.DataFrame:
    """
    Construye un DataFrame de Pandas alineado temporalmente a partir del diccionario de telemetría:
    - Agrupa por timestamp (ts)
    - Formatea la marca temporal en la zona horaria indicada
    - Genera columnas por cada llave de telemetría
    """
    tz = ZoneInfo(time_zone_str)
    all_keys = list(device_data.keys())
    records_by_ts: Dict[int, Dict[str, Any]] = {}

    for key, records in device_data.items():
        for rec in records:
            ts = rec.get("ts")
            val = rec.get("value")
            if ts is not None:
                if ts not in records_by_ts:
                    records_by_ts[ts] = {}
                records_by_ts[ts][key] = val

    if not records_by_ts:
        return pd.DataFrame(columns=["timestamp"] + all_keys)

    rows = []
    for ts in sorted(records_by_ts.keys()):
        try:
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).astimezone(tz)
            row = {"timestamp": dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}
        except Exception:
            row = {"timestamp": str(ts)}

        row.update(records_by_ts[ts])
        rows.append(row)

    df = pd.DataFrame(rows)
    return df


def _sync_write_excel_single_file(
    file_path: str,
    device_data_map: Dict[str, Dict[str, List[dict]]],
    report_config: Dict[str, List[str]],
    time_zone_str: str
) -> str:
    """
    Función sincrónica ejecutada en asyncio.to_thread():
    Escribe un único archivo .xlsx con pd.ExcelWriter y una pestaña por dispositivo.
    Aplica el filtrado previo de report_config antes de construir cada DataFrame.
    """
    dir_name = os.path.dirname(file_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    used_sheet_names: set[str] = set()

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        if not device_data_map:
            empty_df = pd.DataFrame({"message": ["No data available"]})
            empty_df.to_excel(writer, sheet_name="Resumen", index=False)
            ws = writer.sheets["Resumen"]
            _autofit_worksheet_columns(ws)
            return file_path

        for dev_name, keys_data in device_data_map.items():
            # 1. Filtrar llaves permitidas con report_config
            allowed_keys = report_config.get(dev_name) or report_config.get("default")
            if allowed_keys is not None:
                filtered_keys_data = {
                    k: v for k, v in keys_data.items() if k in allowed_keys
                }
            else:
                filtered_keys_data = keys_data

            # 2. Construir DataFrame del dispositivo
            df = _build_dataframe_from_telemetry(filtered_keys_data, time_zone_str)

            # 3. Asignar nombre de hoja único respetando el límite de 31 caracteres
            base_sheet = sanitize_sheet_name(dev_name)
            sheet_name = base_sheet
            counter = 1
            while sheet_name in used_sheet_names:
                suffix = f"_{counter}"
                sheet_name = f"{base_sheet[:31-len(suffix)]}{suffix}"
                counter += 1
            used_sheet_names.add(sheet_name)

            df.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            _autofit_worksheet_columns(ws)

    return file_path


def _sync_write_excel_multiple_files(
    temp_dir: str,
    device_data_map: Dict[str, Dict[str, List[dict]]],
    report_config: Dict[str, List[str]],
    start_fmt: str,
    end_fmt: str,
    time_zone_str: str
) -> List[str]:
    """
    Función sincrónica ejecutada en asyncio.to_thread():
    Genera archivos .xlsx independientes por dispositivo con autoajuste de celdas al contenido,
    invocando gc.collect() tras crear cada uno para liberación agresiva de memoria RAM.
    """
    os.makedirs(temp_dir, exist_ok=True)
    generated_files = []

    for dev_name, keys_data in device_data_map.items():
        # 1. Filtrar llaves permitidas con report_config
        allowed_keys = report_config.get(dev_name) or report_config.get("default")
        if allowed_keys is not None:
            filtered_keys_data = {
                k: v for k, v in keys_data.items() if k in allowed_keys
            }
        else:
            filtered_keys_data = keys_data

        # 2. Construir DataFrame
        df = _build_dataframe_from_telemetry(filtered_keys_data, time_zone_str)

        safe_dev = sanitize_name(dev_name)
        dev_file_name = f"{safe_dev}_{start_fmt}_to_{end_fmt}.xlsx"
        dev_file_path = os.path.join(temp_dir, dev_file_name)
        sheet_name = sanitize_sheet_name(dev_name)

        # 3. Escribir a Excel individual y autoajustar ancho de columnas al contenido
        with pd.ExcelWriter(dev_file_path, engine="openpyxl") as dev_writer:
            df.to_excel(dev_writer, sheet_name=sheet_name, index=False)
            ws = dev_writer.sheets[sheet_name]
            _autofit_worksheet_columns(ws)

        generated_files.append(dev_file_path)

        # 4. Gestión Agresiva de Memoria RAM: Eliminar referencia y forzar Garbage Collection
        del df
        gc.collect()

    return generated_files


async def extract_hybrid_telemetry_for_key(
    tb: ThingsBoardClient,
    token_ref: list,
    tenant_name: str,
    device_name: str,
    entity_id: str,
    entity_type: str,
    key: str,
    intervals: list[dict],
    page_limit: int,
    tenant_id: Optional[str],
    payload: dict,
    local_redis: redis.Redis,
    task_id: str,
    sem: asyncio.Semaphore,
    force_reload: bool = False
) -> List[dict]:
    """
    Extrae la telemetría para una llave específica utilizando el motor híbrido (Local Data Lake + REST API).
    """
    async with sem:
        safe_key = sanitize_name(key)
        all_records: List[dict] = []
        base_storage_dir = payload.get("base_storage_dir") or "tenant_backups"

        for interval in intervals:
            start_ts = interval["start_ts"]
            end_ts = interval["end_ts"]
            year_str = interval["year_str"]
            month_str = interval["month_str"]
            cache_key = f"tb_checkpoint:{task_id}:{tenant_name}:{entity_id}:{safe_key}:{year_str}_{month_str}"

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

            month_records: List[dict] = []
            if delta_plan["plan_type"] == "FULL_LOCAL":
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

            all_records.extend(month_records)

        all_records.sort(key=lambda x: x.get("ts", 0))
        return all_records


async def run_excel_report_orchestrator(
    task_id: str,
    tb: ThingsBoardClient,
    user_id: str,
    payload: dict,
    redis_client: Optional[redis.Redis] = None
) -> dict:
    """
    Orquestador asíncrono para la generación de reportes Excel (.xlsx) Multi-Tenant:
    1. Resuelve configuración de fechas, zonas horarias y whitelist (report_config).
    2. Descubre dispositivos y filtra llaves con report_config previo a la consulta.
    3. Extrae telemetría mediante motor híbrido (Local Data Lake + REST API).
    4. Aplica filtrado estricto con report_config previo a la construcción de DataFrames.
    5. Aislamiento I/O con asyncio.to_thread():
       - combine_in_single_file == True: Un único archivo .xlsx multi-hoja con pd.ExcelWriter.
       - combine_in_single_file == False: Múltiples .xlsx con gc.collect() individual, compresión .zip y purga de residuales.
    6. Registra el respaldo resultante en MongoDB (TBBackup).
    """
    tenant_id = payload.get("tenant_id")
    tenant_name = sanitize_name(payload.get("tenant_name") or "default")
    time_zone_str = payload.get("time_zone") or settings.APP_TIMEZONE
    start_date_str = payload.get("start_date")
    end_date_str = payload.get("end_date")
    combine_in_single_file = payload.get("combine_in_single_file", True)
    entity_type = payload.get("entity_type") or "DEVICE"
    concurrency_limit = payload.get("concurrency_limit") or 3
    page_limit = payload.get("page_limit") or 2000
    force_reload = payload.get("force_reload", False)
    report_config: Dict[str, List[str]] = payload.get("report_config") or {}

    token_ref = [tb.token or ""]
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

    created_local_redis = False
    if redis_client is not None:
        local_redis = redis_client
    else:
        local_redis = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
        created_local_redis = True

    try:
        # 1. Estado inicial
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

        # 2. Descubrimiento de dispositivos
        devices_info = []
        raw_entity_id = payload.get("entity_id")
        placeholder_values = {"string", "null", "none", "undefined", "", "{}", "[]"}
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
                        raise ValueError(f"El dispositivo '{eid}' no existe en ThingsBoard (HTTP {e.response.status_code}).")
                    else:
                        raise
                except Exception as e:
                    if isinstance(e, ValueError):
                        raise
                    logger.warning(f"No se pudo consultar el nombre del dispositivo {eid}: {e}")
                devices_info.append((eid, sanitize_name(dev_name)))
        else:
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

        logger.info(f"[Excel Report] Dispositivos identificados: {len(devices_info)}")

        # 3. Descubrimiento de llaves y filtrado preliminar
        sem = asyncio.Semaphore(concurrency_limit)
        device_data_map: Dict[str, Dict[str, List[dict]]] = {}
        total_extracted_records = 0

        await publish_task_status(
            redis_client=local_redis,
            user_id=user_id,
            task_id=task_id,
            status="DOWNLOADING",
            tenant_name=tenant_name,
            progress_pct=10.0,
            total_records=0
        )

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
                    logger.warning(f"No se pudieron obtener llaves para '{d_name}': HTTP {e.response.status_code}")
                    keys = []
                else:
                    raise

            # Filtrar llaves usando report_config si está configurado
            allowed_keys = report_config.get(d_name) or report_config.get("default")
            if allowed_keys is not None:
                keys_to_fetch = [k for k in keys if k in allowed_keys]
            else:
                keys_to_fetch = keys

            logger.info(f"[Excel Report] Dispositivo '{d_name}': llaves a extraer={keys_to_fetch} (Total reportadas={len(keys)})")
            device_data_map[d_name] = {}

            key_tasks = [
                extract_hybrid_telemetry_for_key(
                    tb=tb,
                    token_ref=token_ref,
                    tenant_name=tenant_name,
                    device_name=d_name,
                    entity_id=eid,
                    entity_type=entity_type,
                    key=key,
                    intervals=intervals,
                    page_limit=page_limit,
                    tenant_id=tenant_id,
                    payload=payload,
                    local_redis=local_redis,
                    task_id=task_id,
                    sem=sem,
                    force_reload=force_reload
                )
                for key in keys_to_fetch
            ]

            results = await asyncio.gather(*key_tasks, return_exceptions=True)
            for key, res in zip(keys_to_fetch, results):
                if isinstance(res, Exception):
                    logger.error(f"[Excel Report] Error crítico tras 10 reintentos al extraer telemetría de '{d_name}' (llave '{key}'): {res}")
                    raise res
                device_data_map[d_name][key] = res
                total_extracted_records += len(res)

        # 4. Fase de Generación de Excel e I/O Asíncrono
        await publish_task_status(
            redis_client=local_redis,
            user_id=user_id,
            task_id=task_id,
            status="PACKAGING",
            tenant_name=tenant_name,
            progress_pct=85.0,
            total_records=total_extracted_records
        )

        start_fmt = start_dt.strftime("%Y%m%d")
        end_fmt = end_dt.strftime("%Y%m%d")
        os.makedirs("backups", exist_ok=True)

        if combine_in_single_file:
            # Caso 1: Un solo archivo .xlsx con una pestaña por dispositivo
            xlsx_filename = f"{tenant_name}_Report_{start_fmt}_to_{end_fmt}_{task_id}.xlsx"
            final_file_path = os.path.join("backups", xlsx_filename)

            logger.info(f"[Excel Report] Generando archivo único multi-hoja en hilo secundario: {final_file_path}")
            await asyncio.to_thread(
                _sync_write_excel_single_file,
                file_path=final_file_path,
                device_data_map=device_data_map,
                report_config=report_config,
                time_zone_str=time_zone_str
            )
            final_artifact_name = xlsx_filename
        else:
            # Caso 2: Múltiples archivos .xlsx individuales + gc.collect() + compresión .zip
            tmp_report_dir = os.path.join("backups", f"tmp_{task_id}")
            xlsx_temp_dir = os.path.join(tmp_report_dir, "xlsx_files")
            zip_base_name = f"{tenant_name}_Reports_{start_fmt}_to_{end_fmt}_{task_id}"
            zip_filename = f"{zip_base_name}.zip"
            final_file_path = os.path.join("backups", zip_filename)

            logger.info(f"[Excel Report] Generando múltiples archivos .xlsx con gc.collect() en {xlsx_temp_dir}...")
            await asyncio.to_thread(
                _sync_write_excel_multiple_files,
                temp_dir=xlsx_temp_dir,
                device_data_map=device_data_map,
                report_config=report_config,
                start_fmt=start_fmt,
                end_fmt=end_fmt,
                time_zone_str=time_zone_str
            )

            logger.info(f"[Excel Report] Comprimiendo archivos .xlsx en ZIP: {final_file_path}")
            await async_create_zip_archive(
                base_name=os.path.join("backups", zip_base_name),
                root_dir=tmp_report_dir,
                base_dir="xlsx_files",
                format="zip"
            )

            # Purga de archivos temporales residuales
            if os.path.exists(tmp_report_dir):
                await async_rmtree(tmp_report_dir, ignore_errors=True)
                logger.info(f"[Excel Report] Directorio temporal {tmp_report_dir} purgado exitosamente.")

            final_artifact_name = zip_filename

        # 5. Registro en el Catálogo TBBackup de MongoDB
        file_size = os.path.getsize(final_file_path) if os.path.exists(final_file_path) else 0
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
                    file_name=final_artifact_name,
                    start_date=start_dt,
                    end_date=end_dt,
                    file_size_bytes=file_size,
                    created_at=datetime.now(timezone.utc)
                )
                await backup_record.insert()
                logger.info(f"[MongoDB] Reporte registrado en TBBackup: {final_artifact_name} ({file_size} bytes)")
            except Exception as e:
                logger.error(f"[MongoDB] Error registrando reporte en TBBackup: {e}")

        # 6. Finalización exitosa
        await publish_task_status(
            redis_client=local_redis,
            user_id=user_id,
            task_id=task_id,
            status="SUCCESS",
            tenant_name=tenant_name,
            progress_pct=100.0,
            total_records=total_extracted_records,
            cleanup_on_terminal=True
        )

        return {
            "task_id": task_id,
            "status": "SUCCESS",
            "file_name": final_artifact_name,
            "file_size_bytes": file_size,
            "total_records": total_extracted_records
        }

    except Exception as exc:
        logger.error(f"[Excel Report] Error en tarea {task_id}: {exc}", exc_info=True)
        # Limpieza defensiva en caso de fallo
        if "tmp_report_dir" in locals() and os.path.exists(tmp_report_dir):
            try:
                await async_rmtree(tmp_report_dir, ignore_errors=True)
            except Exception:
                pass
        if "final_file_path" in locals() and os.path.exists(final_file_path):
            try:
                await async_remove_file(final_file_path, ignore_errors=True)
            except Exception:
                pass

        try:
            await publish_task_status(
                redis_client=local_redis,
                user_id=user_id,
                task_id=task_id,
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
        if created_local_redis and local_redis:
            await local_redis.aclose()
