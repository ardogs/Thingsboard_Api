import os
import json
import asyncio
from datetime import datetime
from typing import Optional, List, Union, Any

from fastapi import APIRouter, HTTPException, Request, Depends, status
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator
import redis.asyncio as redis
from beanie import PydanticObjectId

from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.config import settings
from core.logger import logger
from core.redis_client import redis_client
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
    task_id: str = Field(..., description="ID de la tarea de Celery")
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

    # 6. Encolar la tarea en Celery de forma completamente asíncrona y no bloqueante
    try:
        task = download_telemetry_task.delay(task_payload)
    except Exception as e:
        await redis_client.delete(lock_key)
        raise e

    return {
        "task_id": task.id,
        "status": "Task enqueued",
        "user_id": str(current_user.id),
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url
    }


@router.get("/tasks/active", response_model=List[ActiveTaskResponse])
async def get_active_tasks(current_user: User = Depends(get_current_user)):
    """
    Retorna la lista de tareas en ejecución actualmente pertenecientes al usuario autenticado.
    """
    r = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    try:
        user_registry_key = get_user_registry_key(str(current_user.id))
        raw_tasks = await r.hgetall(user_registry_key)
        active_tasks: List[ActiveTaskResponse] = []

        for task_id, payload_str in raw_tasks.items():
            try:
                task_data = json.loads(payload_str)
                active_tasks.append(ActiveTaskResponse(**task_data))
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"[Active Tasks] Error al parsear estado de tarea {task_id}: {e}")

        return active_tasks
    finally:
        await r.aclose()


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
        r = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
        pubsub = r.pubsub()
        channel = get_user_stream_channel(user_id, task_id)
        registry_key = get_user_registry_key(user_id)

        try:
            await pubsub.subscribe(channel)

            # 1. Enviar estado inicial si está registrado en el Hash del usuario
            initial_state = await r.hget(registry_key, task_id)
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
                await r.aclose()
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
            return FileResponse(
                path=file_path,
                media_type="application/zip",
                filename=backup_doc.file_name
            )

    # 2. Fallback de compatibilidad si el archivo existe en disco
    for filename in os.listdir(backup_dir):
        if (filename.startswith(f"{task_id}_") or filename.endswith(f"_{task_id}.zip")) and filename.endswith(".zip"):
            file_path = os.path.join(backup_dir, filename)
            return FileResponse(
                path=file_path,
                media_type="application/zip",
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
    Consulta el estado de una tarea en Celery para usuarios autenticados.
    """
    from workers.tasks import celery_app
    task_result = celery_app.AsyncResult(task_id)

    response = {
        "task_id": task_id,
        "status": task_result.status,
    }

    if task_result.status == "FAILURE":
        response["error"] = str(task_result.result)

    return response
