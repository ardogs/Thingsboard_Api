import os
import json
import asyncio
from datetime import datetime
from typing import Optional, List, Union

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
import redis.asyncio as redis
from beanie import PydanticObjectId

from core.models.tb_server import TBServer
from core.tb_client import ThingsBoardClient
from core.config import settings
from core.logger import logger
from api.deps import User, get_current_user
from workers.tasks import (
    download_telemetry_task,
    get_user_stream_channel,
    get_user_registry_key
)

router = APIRouter()


class DownloadTelemetryRequest(BaseModel):
    server_id: Optional[str] = Field(default=None, description="ID del servidor ThingsBoard registrado en MongoDB")
    server_url: Optional[str] = Field(default=None, description="URL directa (opcional si se usa server_id)")
    start_date: str = Field(..., description="Fecha inicial ISO (ej: 2026-01-01T00:00:00)")
    end_date: str = Field(..., description="Fecha final ISO (ej: 2026-08-01T23:59:59)")
    token: Optional[str] = Field(default=None, description="Token JWT de ThingsBoard (opcional si el servidor ya lo tiene)")
    tenant_name: str = "default"
    refresh_token: Optional[str] = None
    entity_type: str = "DEVICE"
    entity_id: Optional[Union[str, List[str]]] = None
    time_zone: str = "UTC"
    concurrency_limit: int = 3
    page_limit: int = 2000
    force_reload: bool = False


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


@router.post("/download")
async def download_telemetry(
    request: DownloadTelemetryRequest,
    current_user: User = Depends(get_current_user),
    authorization: Optional[str] = None,
    x_authorization: Optional[str] = None
):
    """
    Inicia una tarea asíncrona de descarga de telemetría sobre un servidor ThingsBoard registrado en MongoDB o URL directa.
    """
    target_base_url = request.server_url
    target_token = request.token

    if not target_token:
        auth_header = x_authorization or authorization
        if auth_header:
            target_token = auth_header.replace("Bearer ", "").strip()

    # Si se especificó server_id, resolver desde MongoDB
    if request.server_id:
        try:
            try:
                obj_id = PydanticObjectId(request.server_id)
                server = await TBServer.get(obj_id)
            except Exception:
                server = await TBServer.get(request.server_id)
        except Exception:
            server = None

        if not server:
            raise HTTPException(status_code=404, detail=f"Servidor ThingsBoard con ID '{request.server_id}' no encontrado en MongoDB")

        if current_user.role != "admin" and server.user_id != current_user.id:
            raise HTTPException(status_code=403, detail="No tienes permisos para usar este servidor ThingsBoard")

        target_base_url = server.base_url
        if not target_token:
            target_token = server.token

    if not target_base_url:
        raise HTTPException(
            status_code=400,
            detail="Debes proporcionar 'server_id' (MongoDB) o 'server_url'"
        )

    if not target_token:
        raise HTTPException(
            status_code=400,
            detail="Se requiere el token de ThingsBoard (en el servidor registrado, body 'token' o header 'Authorization')"
        )

    # Validar token contra el servidor ThingsBoard dinámico
    tb = ThingsBoardClient(base_url=target_base_url)
    is_valid = await tb.verify_token(target_token)

    if not is_valid:
        raise HTTPException(
            status_code=401,
            detail=f"El token de ThingsBoard ha expirado o es inválido en el servidor {target_base_url}"
        )

    task_payload = request.model_dump()
    task_payload["token"] = target_token
    task_payload["server_url"] = target_base_url
    task_payload["user_id"] = current_user.id

    # Encolar tarea en Celery a través del enrutador
    task = download_telemetry_task.delay(task_payload)

    return {
        "task_id": task.id,
        "status": "Task enqueued",
        "user_id": current_user.id,
        "server_id": request.server_id,
        "server_url": target_base_url
    }


@router.get("/tasks/active", response_model=List[ActiveTaskResponse])
async def get_active_tasks(current_user: User = Depends(get_current_user)):
    """
    Retorna la lista de tareas en ejecución actualmente pertenecientes al usuario autenticado.
    """
    r = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    try:
        user_registry_key = get_user_registry_key(current_user.id)
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
    user_id = current_user.id

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


@router.get("/download/file/{task_id}")
async def download_file(
    task_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Permite a cualquier usuario autenticado descargar el archivo ZIP respaldado en la carpeta común 'backups/'.
    """
    backup_dir = "backups"
    if not os.path.exists(backup_dir):
        raise HTTPException(status_code=404, detail="El directorio de backups no existe")

    for filename in os.listdir(backup_dir):
        if filename.startswith(f"{task_id}_") and filename.endswith(".zip"):
            file_path = os.path.join(backup_dir, filename)
            return FileResponse(
                path=file_path,
                media_type="application/zip",
                filename=filename
            )

    raise HTTPException(
        status_code=404,
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
