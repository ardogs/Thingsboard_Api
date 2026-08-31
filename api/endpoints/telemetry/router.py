import os
import json
import asyncio
import calendar
import mimetypes
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional, List, Union, Any

from fastapi import APIRouter, HTTPException, Request, Depends, status
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator, model_validator
import redis.asyncio as redis
from beanie import PydanticObjectId

from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.config import settings
from core.logger import logger
from core.redis_client import redis_client
from core.arq_pool import get_arq_pool
from core.casbin_enforcer import get_casbin_enforcer
from api.deps import User, get_current_user
from workers.tasks import (
    download_telemetry_task,
    get_user_stream_channel,
    get_user_registry_key,
    get_server_lock_key
)

router = APIRouter()


class DownloadTelemetryRequest(BaseModel):
    tenant_id: str = Field(..., description="ID de MongoDB del documento TBTenant")
    start_date: str = Field(..., description="Fecha inicial ISO (ej: 2026-01-01T00:00:00)")
    end_date: str = Field(..., description="Fecha final ISO (ej: 2026-08-01T23:59:59)")
    entity_type: str = Field(default="DEVICE", description="Tipo de entidad en ThingsBoard (DEVICE, ASSET)")
    entity_id: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="UUID o lista de UUIDs de dispositivos específicos. Dejar en null para descargar todos los dispositivos del Tenant."
    )
    time_zone: str = Field(default="UTC", description="Zona horaria (ej: America/Mexico_City)")
    concurrency_limit: int = Field(default=3, description="Límite de concurrencia de descarga simultánea")
    page_limit: int = Field(default=2000, description="Tamaño de página por petición de telemetría")
    force_reload: bool = Field(default=False, description="Sobrescribir checkpoints previos e iniciar desde cero")

    @field_validator("entity_id", mode="before")
    @classmethod
    def sanitize_entity_id(cls, v: Any) -> Optional[Union[str, List[str]]]:
        if v is None:
            return None
        placeholder_values = {"string", "null", "none", "undefined", "", "{}", "[]"}
        if isinstance(v, str):
            clean_v = v.strip()
            if clean_v.lower() in placeholder_values:
                return None
            return clean_v
        if isinstance(v, list):
            cleaned_list = [
                str(item).strip() for item in v
                if str(item).strip() and str(item).strip().lower() not in placeholder_values
            ]
            return cleaned_list if cleaned_list else None
        return v


class ExcelReportRequest(BaseModel):
    tenant_id: str = Field(..., description="ID de MongoDB del documento TBTenant")
    combine_in_single_file: bool = Field(default=True, description="True para un solo XLSX multi-hoja; False para ZIP con múltiples XLSX independientes")
    start_date: Optional[str] = Field(default=None, description="Fecha inicial ISO (ej: 2026-01-01T00:00:00)")
    end_date: Optional[str] = Field(default=None, description="Fecha final ISO (ej: 2026-01-31T23:59:59)")
    year: Optional[int] = Field(default=None, description="Año numérico (ej: 2026)")
    month: Optional[int] = Field(default=None, description="Mes numérico (1-12)")
    entity_type: str = Field(default="DEVICE", description="Tipo de entidad en ThingsBoard (DEVICE, ASSET)")
    entity_id: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="UUID o lista de UUIDs de dispositivos específicos. Dejar en null para todos."
    )
    time_zone: Optional[str] = Field(default=None, description="Zona horaria (default: settings.APP_TIMEZONE)")
    concurrency_limit: int = Field(default=3, description="Límite de concurrencia de descarga simultánea")
    page_limit: int = Field(default=2000, description="Tamaño de página por petición de telemetría")
    force_reload: bool = Field(default=False, description="Sobrescribir checkpoints previos e iniciar desde cero")

    @field_validator("entity_id", mode="before")
    @classmethod
    def sanitize_entity_id(cls, v: Any) -> Optional[Union[str, List[str]]]:
        if v is None:
            return None
        placeholder_values = {"string", "null", "none", "undefined", "", "{}", "[]"}
        if isinstance(v, str):
            clean_v = v.strip()
            if clean_v.lower() in placeholder_values:
                return None
            return clean_v
        if isinstance(v, list):
            cleaned_list = [
                str(item).strip() for item in v
                if str(item).strip() and str(item).strip().lower() not in placeholder_values
            ]
            return cleaned_list if cleaned_list else None
        return v

    @model_validator(mode="after")
    def validate_and_compute_dates(self):
        has_exact = bool(self.start_date and self.end_date)
        has_month = bool(self.year is not None and self.month is not None)

        if has_exact == has_month:
            raise ValueError("Debe especificar O BIEN ('start_date' y 'end_date') O BIEN ('year' y 'month'), pero no ambos ni ninguno.")

        tz_name = self.time_zone or settings.APP_TIMEZONE
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo(settings.APP_TIMEZONE)

        if has_month:
            if not (1 <= self.month <= 12):
                raise ValueError("El mes debe estar comprendido entre 1 y 12.")
            if self.year < 1970 or self.year > 3000:
                raise ValueError("Año fuera del rango soportado.")

            _, last_day = calendar.monthrange(self.year, self.month)
            start_dt = datetime(self.year, self.month, 1, 0, 0, 0, 0, tzinfo=tz)
            end_dt = datetime(self.year, self.month, last_day, 23, 59, 59, 999000, tzinfo=tz)

            self.start_date = start_dt.isoformat()
            self.end_date = end_dt.isoformat()
        else:
            try:
                start_dt = datetime.fromisoformat(self.start_date)
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=tz)
                end_dt = datetime.fromisoformat(self.end_date)
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=tz)
            except Exception as e:
                raise ValueError(f"Formato ISO inválido para las fechas: {e}")

            if start_dt > end_dt:
                raise ValueError("start_date no puede ser posterior a end_date.")

            self.start_date = start_dt.isoformat()
            self.end_date = end_dt.isoformat()

        return self


class ActiveTaskResponse(BaseModel):
    task_id: str
    user_id: Optional[str] = None
    status: str
    tenant_name: str
    current_device: Optional[str] = None
    current_key: Optional[str] = None
    progress_pct: float = 0.0
    total_records: int = 0
    records_count: Optional[int] = None


class BackupResponse(BaseModel):
    id: str = Field(..., description="ID del documento de respaldo en MongoDB")
    tenant_id: str = Field(..., description="ID del Tenant respaldado")
    tenant_name: Optional[str] = Field(default=None, description="Nombre del Tenant")
    task_id: str = Field(..., description="ID del trabajo/tarea de respaldo en ARQ")
    requested_by: str = Field(..., description="ID del usuario que solicitó el respaldo")
    file_name: str = Field(..., description="Nombre del archivo ZIP generado")
    start_date: datetime = Field(..., description="Fecha de inicio del rango de telemetría")
    end_date: datetime = Field(..., description="Fecha de fin del rango de telemetría")
    file_size_bytes: int = Field(default=0, description="Tamaño del archivo ZIP en bytes")
    created_at: datetime = Field(..., description="Fecha y hora de creación del respaldo")
    download_url: str = Field(..., description="URL para la descarga directa del archivo ZIP")


@router.post("/download", status_code=status.HTTP_200_OK)
async def download_telemetry(
    request: DownloadTelemetryRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Inicia una tarea asíncrona de descarga masiva de telemetría para un Tenant registrado en MongoDB.
    Valida la existencia del Tenant y el aislamiento por usuario antes de encolar en Celery.
    Aplica Rechazo Temprano (Fail Fast / Distributed Lock) si el servidor ThingsBoard se encuentra ocupado.
    El cliente se comunica exclusivamente mediante el JWT del API Gateway, sin necesidad de enviar tokens de ThingsBoard.
    """
    # 1. Validar existencia del Tenant en MongoDB usando Beanie
    try:
        obj_id = PydanticObjectId(request.tenant_id)
        tenant_doc = await TBTenant.get(obj_id)
    except Exception:
        tenant_doc = await TBTenant.get(request.tenant_id)

    if not tenant_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant de ThingsBoard con ID '{request.tenant_id}' no encontrado en MongoDB"
        )

    # 2. Validar aislamiento y permisos de usuario (Casbin RBAC + Ownership + Superadmin)
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    is_owner = tenant_doc.user_id in [str(current_user.id), current_user.id]
    if not is_admin and not is_owner:
        try:
            enforcer = get_casbin_enforcer()
            user_id_str = str(current_user.id)
            tenant_domain = f"tenant:{tenant_doc.id}"
            is_allowed = enforcer.enforce(user_id_str, tenant_domain, "telemetry", "write")
            if not is_allowed and current_user.username:
                is_allowed = enforcer.enforce(current_user.username, tenant_domain, "telemetry", "write")
        except Exception:
            is_allowed = False

        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos para iniciar descargas en este Tenant"
            )

    # 3. Resolver servidor ThingsBoard padre
    server_doc = await tenant_doc.get_server()
    if not server_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"El servidor ThingsBoard asociado al Tenant '{tenant_doc.name}' no existe o fue eliminado"
        )

    # 4. Adquisición Atómica del Candado Distribuido (Fail Fast / Prevención TOCTOU)
    server_id = str(server_doc.id)
    lock_key = get_server_lock_key(server_id)
    lock_acquired = await redis_client.set(lock_key, "locked", nx=True, ex=300)
    if not lock_acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El servidor ThingsBoard se encuentra actualmente ocupado procesando otro respaldo. Por favor, intente más tarde."
        )

    # 5. Preparar payload desacoplado para el Celery Worker
    task_payload = {
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "entity_type": request.entity_type,
        "entity_id": request.entity_id,
        "time_zone": request.time_zone,
        "concurrency_limit": request.concurrency_limit,
        "page_limit": request.page_limit,
        "force_reload": request.force_reload,
        "user_id": str(current_user.id)
    }

    # 6. Encolar la tarea en ARQ de forma completamente asíncrona y no bloqueante
    try:
        arq_pool = await get_arq_pool()
        job = await arq_pool.enqueue_job("download_telemetry_task", payload=task_payload)
        job_id = job.job_id if job else "unknown_job"
    except Exception as e:
        await redis_client.delete(lock_key)
        raise e

    return {
        "task_id": job_id,
        "status": "Task enqueued",
        "user_id": str(current_user.id),
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url
    }


@router.post("/report/excel", status_code=status.HTTP_200_OK)
async def generate_excel_report(
    request: ExcelReportRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Inicia una tarea asíncrona de exportación de telemetría a formato Excel (.xlsx).
    Soporta fechas exactas o mes/año calculado matemáticamente, modo mono-archivo (.xlsx multi-hoja)
    o multi-archivo (.zip con .xlsx individuales y recolección de basura por dispositivo).
    Aplica Distributed Lock Fail Fast y encola en ARQ.
    """
    # 1. Validar existencia del Tenant en MongoDB usando Beanie
    try:
        obj_id = PydanticObjectId(request.tenant_id)
        tenant_doc = await TBTenant.get(obj_id)
    except Exception:
        tenant_doc = await TBTenant.get(request.tenant_id)

    if not tenant_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant de ThingsBoard con ID '{request.tenant_id}' no encontrado en MongoDB"
        )

    # 2. Validar aislamiento y permisos de usuario (Casbin RBAC + Ownership + Superadmin)
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    is_owner = tenant_doc.user_id in [str(current_user.id), current_user.id]
    if not is_admin and not is_owner:
        try:
            enforcer = get_casbin_enforcer()
            user_id_str = str(current_user.id)
            tenant_domain = f"tenant:{tenant_doc.id}"
            is_allowed = enforcer.enforce(user_id_str, tenant_domain, "telemetry", "write")
            if not is_allowed and current_user.username:
                is_allowed = enforcer.enforce(current_user.username, tenant_domain, "telemetry", "write")
        except Exception:
            is_allowed = False

        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos para generar reportes en este Tenant"
            )

    # 3. Resolver servidor ThingsBoard padre
    server_doc = await tenant_doc.get_server()
    if not server_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"El servidor ThingsBoard asociado al Tenant '{tenant_doc.name}' no existe o fue eliminado"
        )

    # 4. Adquisición Atómica del Candado Distribuido (Fail Fast)
    server_id = str(server_doc.id)
    lock_key = get_server_lock_key(server_id)
    lock_acquired = await redis_client.set(lock_key, "locked", nx=True, ex=300)
    if not lock_acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El servidor ThingsBoard se encuentra actualmente ocupado procesando otro respaldo o reporte. Por favor, intente más tarde."
        )

    # 5. Extraer report_config de custom_metadata
    report_config = tenant_doc.custom_metadata.get("report_config", {}) if tenant_doc.custom_metadata else {}

    # 6. Preparar payload desacoplado para el ARQ Worker
    task_payload = {
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "combine_in_single_file": request.combine_in_single_file,
        "entity_type": request.entity_type,
        "entity_id": request.entity_id,
        "time_zone": request.time_zone or settings.APP_TIMEZONE,
        "concurrency_limit": request.concurrency_limit,
        "page_limit": request.page_limit,
        "force_reload": request.force_reload,
        "report_config": report_config,
        "user_id": str(current_user.id)
    }

    # 7. Encolar la tarea en ARQ de forma asíncrona
    try:
        arq_pool = await get_arq_pool()
        job = await arq_pool.enqueue_job("generate_excel_report_task", payload=task_payload)
        job_id = job.job_id if job else "unknown_job"
    except Exception as e:
        await redis_client.delete(lock_key)
        raise e

    return {
        "task_id": job_id,
        "status": "Task enqueued",
        "user_id": str(current_user.id),
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url,
        "combine_in_single_file": request.combine_in_single_file,
        "start_date": request.start_date,
        "end_date": request.end_date
    }



@router.get("/tasks/active", response_model=List[ActiveTaskResponse])
async def get_active_tasks(current_user: User = Depends(get_current_user)):
    """
    Retorna la lista de tareas en ejecución actualmente pertenecientes al usuario autenticado.
    """
    user_registry_key = get_user_registry_key(str(current_user.id))
    raw_tasks = await redis_client.hgetall(user_registry_key)
    active_tasks: List[ActiveTaskResponse] = []

    for task_id, payload_str in raw_tasks.items():
        try:
            task_data = json.loads(payload_str)
            active_tasks.append(ActiveTaskResponse(**task_data))
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"[Active Tasks] Error al parsear estado de tarea {task_id}: {e}")

    return active_tasks


@router.get("/stream/{task_id}")
async def stream_task_progress(
    task_id: str,
    request: Request,
    current_user: User = Depends(get_current_user)
):
    """
    Endpoint Server-Sent Events (SSE) para transmitir el progreso de una tarea en tiempo real.
    """
    user_id = str(current_user.id)

    async def event_generator():
        pubsub = redis_client.pubsub()
        channel = get_user_stream_channel(user_id, task_id)
        registry_key = get_user_registry_key(user_id)

        try:
            await pubsub.subscribe(channel)

            # 1. Enviar estado inicial si está registrado en el Hash del usuario
            initial_state = await redis_client.hget(registry_key, task_id)
            if initial_state:
                yield f"data: {initial_state}\n\n"
                try:
                    parsed_initial = json.loads(initial_state)
                    if parsed_initial.get("status") in ("SUCCESS", "ERROR", "FAILURE"):
                        return
                except json.JSONDecodeError:
                    pass

            last_ping_time = asyncio.get_running_loop().time()

            while True:
                if await request.is_disconnected():
                    logger.info(f"[SSE] Cliente desconectado del stream para task_id {task_id} (user {user_id})")
                    break

                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                current_time = asyncio.get_running_loop().time()

                if message and message.get("type") == "message":
                    data_str = message.get("data")
                    yield f"data: {data_str}\n\n"
                    last_ping_time = current_time

                    try:
                        event_data = json.loads(data_str)
                        if event_data.get("status") in ("SUCCESS", "ERROR", "FAILURE"):
                            logger.info(f"[SSE] Tarea {task_id} finalizada ({event_data.get('status')}). Cerrando stream.")
                            break
                    except json.JSONDecodeError:
                        pass
                else:
                    if current_time - last_ping_time >= 15.0:
                        yield ": ping\n\n"
                        last_ping_time = current_time

                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info(f"[SSE] Petición cancelada por el cliente para task_id {task_id} (user {user_id})")
        except Exception as e:
            logger.error(f"[SSE] Error en el stream para task_id {task_id} (user {user_id}): {e}")
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()
            except Exception as e:
                logger.warning(f"[SSE] Error cerrando recursos de Redis para task_id {task_id}: {e}")

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
            "X-Accel-Buffering": "no",
        }
    )


@router.get("/backups", response_model=List[BackupResponse], status_code=status.HTTP_200_OK)
async def list_tenant_backups(
    tenant_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Lista el historial de respaldos de telemetría disponibles en MongoDB para un Tenant específico.
    Permite el acceso a administradores o usuarios con permisos sobre dicho Tenant.
    """
    # 1. Validar existencia del Tenant
    try:
        obj_id = PydanticObjectId(tenant_id)
        tenant_doc = await TBTenant.get(obj_id)
    except Exception:
        tenant_doc = await TBTenant.get(tenant_id)

    if not tenant_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant de ThingsBoard con ID '{tenant_id}' no encontrado en MongoDB"
        )

    # 2. Validar permisos de acceso al Tenant (Casbin RBAC + Ownership + Superadmin)
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    is_owner = tenant_doc.user_id in [str(current_user.id), current_user.id]
    if not is_admin and not is_owner:
        try:
            enforcer = get_casbin_enforcer()
            user_id_str = str(current_user.id)
            tenant_domain = f"tenant:{tenant_doc.id}"
            is_allowed = enforcer.enforce(user_id_str, tenant_domain, "telemetry", "read")
            if not is_allowed and current_user.username:
                is_allowed = enforcer.enforce(current_user.username, tenant_domain, "telemetry", "read")
        except Exception:
            is_allowed = False

        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos para consultar los respaldos de este Tenant"
            )

    # 3. Consultar documentos TBBackup asociados al Tenant
    ref = tenant_doc.to_ref()
    backups = await TBBackup.find(
        {"$or": [{"tenant_id": ref}, {"tenant_id.$id": tenant_doc.id}, {"tenant_id": tenant_doc.id}]}
    ).sort("-created_at").to_list()

    results: List[BackupResponse] = []
    for b in backups:
        results.append(
            BackupResponse(
                id=str(b.id),
                tenant_id=str(tenant_doc.id),
                tenant_name=tenant_doc.name,
                task_id=b.task_id,
                requested_by=b.requested_by,
                file_name=b.file_name,
                start_date=b.start_date,
                end_date=b.end_date,
                file_size_bytes=b.file_size_bytes,
                created_at=b.created_at,
                download_url=f"/api/v1/telemetry/download/file/{b.task_id}"
            )
        )

    return results


@router.get("/download/file/{task_id}")
async def download_file(
    task_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Permite descargar el archivo ZIP respaldado consultando el catálogo TBBackup en MongoDB.
    Valida permisos de acceso al Tenant asociado antes de servir el archivo.
    """
    backup_dir = "backups"
    if not os.path.exists(backup_dir):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="El directorio de backups no existe"
        )

    # 1. Consultar el documento TBBackup en MongoDB
    backup_doc = None
    try:
        backup_doc = await TBBackup.find_one({"task_id": task_id})
    except Exception as e:
        logger.warning(f"[Download File] No se pudo consultar TBBackup para task {task_id}: {e}")

    if backup_doc:
        tenant_doc = await backup_doc.get_tenant()
        is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
        is_requester = backup_doc.requested_by in [str(current_user.id), current_user.id]
        is_tenant_owner = tenant_doc and tenant_doc.user_id in [str(current_user.id), current_user.id]

        if not is_admin and not is_requester and not is_tenant_owner:
            # Validar con Casbin si tiene permisos de lectura para ese tenant
            tenant_id_str = f"tenant:{tenant_doc.id}" if tenant_doc else "*"
            try:
                enforcer = get_casbin_enforcer()
                is_allowed = enforcer.enforce(str(current_user.id), tenant_id_str, "telemetry", "read")
            except Exception:
                is_allowed = False

            if not is_allowed:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="No tienes permisos para descargar este respaldo"
                )

        file_path = os.path.join(backup_dir, backup_doc.file_name)
        if os.path.exists(file_path):
            if backup_doc.file_name.endswith(".xlsx"):
                media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif backup_doc.file_name.endswith(".zip"):
                media_type = "application/zip"
            else:
                guessed, _ = mimetypes.guess_type(backup_doc.file_name)
                media_type = guessed or "application/octet-stream"

            return FileResponse(
                path=file_path,
                media_type=media_type,
                filename=backup_doc.file_name
            )

    # 2. Fallback de compatibilidad si el archivo existe en disco
    for filename in os.listdir(backup_dir):
        if (filename.startswith(f"{task_id}_") or filename.endswith(f"_{task_id}.zip") or filename.endswith(f"_{task_id}.xlsx") or task_id in filename) and (filename.endswith(".zip") or filename.endswith(".xlsx")):
            file_path = os.path.join(backup_dir, filename)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if filename.endswith(".xlsx") else "application/zip"
            return FileResponse(
                path=file_path,
                media_type=media_type,
                filename=filename
            )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="El archivo de backup no existe o no ha terminado de procesarse"
    )


@router.get("/status/{task_id}")
async def get_task_status(
    task_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Consulta el estado de un trabajo en ARQ para usuarios autenticados.
    """
    from arq.jobs import Job
    arq_pool = await get_arq_pool()
    job = Job(task_id, arq_pool)
    try:
        raw_status = await job.status()
        status_str = raw_status.value if hasattr(raw_status, "value") else str(raw_status)
    except Exception:
        status_str = "unknown"

    response = {
        "task_id": task_id,
        "status": status_str,
    }

    if status_str in ("not_found", "unknown"):
        # Fallback de verificación en registro de usuario
        r = redis_client
        user_registry_key = get_user_registry_key(str(current_user.id))
        task_json = await r.hget(user_registry_key, task_id)
        if task_json:
            try:
                task_data = json.loads(task_json)
                response["status"] = task_data.get("status", status_str)
            except Exception:
                pass

    return response


@router.post("/tasks/{job_id}/cancel")
async def cancel_telemetry_task(
    job_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Botón de Pánico: Envía una señal de aborto/cancelación inmediata a un trabajo de ARQ en ejecución o encolado.
    - Obtiene el pool de ARQ (get_arq_pool).
    - Instancia el trabajo: job = Job(job_id, arq_pool).
    - Verifica el estado actual. Si no existe (404) o ya terminó (400), devuelve el error correspondiente.
    - Si está encolado o en progreso, ejecuta await job.abort().
    - Notifica a los canales SSE de Redis y devuelve HTTP 200 confirmando la emisión de la señal de terminación.
    """
    from arq.jobs import Job, JobStatus

    arq_pool = await get_arq_pool()
    job = Job(job_id, arq_pool)

    try:
        raw_status = await job.status()
    except Exception as e:
        logger.error(f"[Cancel Task] Error consultando estado del trabajo {job_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error consultando el estado del trabajo en ARQ: {str(e)}"
        )

    if raw_status == JobStatus.not_found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"El trabajo con ID '{job_id}' no fue encontrado en ARQ"
        )

    if raw_status == JobStatus.complete:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"El trabajo '{job_id}' ya ha finalizado y no puede ser cancelado"
        )

    logger.warning(
        f"[Cancel Task] Usuario '{current_user.username}' ({current_user.id}) solicitó cancelar "
        f"el trabajo '{job_id}' (Estado actual: {raw_status}). Enviando señal de aborto..."
    )

    # Enviar señal de cancelación a ARQ
    aborted = await job.abort(timeout=5.0)

    # Publicar estado CANCELLED en Redis Pub/Sub y purgar del registro activo
    r = redis_client
    user_id_str = str(current_user.id)
    stream_channel = get_user_stream_channel(user_id_str, job_id)
    user_registry_key = get_user_registry_key(user_id_str)

    cancel_event = {
        "task_id": job_id,
        "status": "CANCELLED",
        "progress_pct": 0.0,
        "message": "Tarea cancelada exitosamente a petición del usuario.",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    try:
        await r.publish(stream_channel, json.dumps(cancel_event))
        await r.hdel(user_registry_key, job_id)
    except Exception as pub_err:
        logger.warning(f"[Cancel Task] Advertencia notificando cancelación en Redis: {pub_err}")

    prev_status_str = raw_status.value if hasattr(raw_status, "value") else str(raw_status)
    return {
        "status": "cancelled",
        "job_id": job_id,
        "aborted": aborted,
        "previous_status": prev_status_str,
        "message": f"Señal de terminación enviada exitosamente para la tarea '{job_id}'."
    }
