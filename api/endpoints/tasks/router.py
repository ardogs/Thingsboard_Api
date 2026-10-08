import json
import asyncio
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict

from fastapi import APIRouter, HTTPException, Request, Depends, status, Query
from fastapi.responses import StreamingResponse
from arq.jobs import Job, JobStatus

from core.logger import get_logger
from core.redis_client import redis_client
from core.arq_pool import get_arq_pool
from core.services.task_registry import (
    get_user_stream_channel,
    get_user_registry_key
)
from api.deps import User, get_current_user
from api.endpoints.tasks.schemas import (
    ActiveTaskResponse,
    TaskStatusResponse,
    CancelTaskResponse
)

logger = get_logger("tasks_router")

router = APIRouter()


@router.get("/active", response_model=List[ActiveTaskResponse], status_code=status.HTTP_200_OK)
async def get_active_tasks(
    task_type: Optional[str] = Query(default=None, description="Filtrar tareas activas por tipo de tarea (ej: telemetry, heatmap, excel_report, email)"),
    current_user: User = Depends(get_current_user)
):
    """
    Retorna la lista unificada de tareas en segundo plano en ejecución pertenecientes al usuario autenticado.
    Abarca tareas de cualquier dominio: telemetría, envíos de correo, reportes Excel, heatmaps y automatizaciones.
    """
    active_tasks: List[ActiveTaskResponse] = []

    if current_user.is_superuser:
        keys = await redis_client.keys("tb_events:user:*:registry")
        for key in keys:
            raw_tasks = await redis_client.hgetall(key)
            for task_id, payload_str in raw_tasks.items():
                try:
                    task_data = json.loads(payload_str)
                    active_tasks.append(ActiveTaskResponse(**task_data))
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"[Active Tasks] Error al parsear estado de tarea {task_id}: {e}")
        if task_type:
            allowed_types = {tt.strip().lower() for tt in task_type.split(",") if tt.strip()}
            active_tasks = [t for t in active_tasks if (t.task_type or "").lower() in allowed_types]
        return active_tasks

    user_registry_key = get_user_registry_key(str(current_user.id))
    raw_tasks = await redis_client.hgetall(user_registry_key)

    seen_task_ids = set()
    for task_id, payload_str in raw_tasks.items():
        try:
            task_data = json.loads(payload_str)
            active_tasks.append(ActiveTaskResponse(**task_data))
            seen_task_ids.add(task_id)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"[Active Tasks] Error al parsear estado de tarea {task_id}: {e}")

    # Consultar tareas de tenants donde el usuario posee permisos de monitoreo
    try:
        from core.casbin_enforcer import get_casbin_enforcer
        from core.models.tb_tenant import TBTenant
        enforcer = get_casbin_enforcer()
        all_tenants = await TBTenant.find_all().to_list()
        user_id_str = str(current_user.id)
        allowed_tenant_names = set()
        for t in all_tenants:
            t_dom = f"tenant:{t.id}"
            if (
                t.user_id in [user_id_str, current_user.id]
                or (
                    enforcer.enforce(user_id_str, t_dom, "telemetry", "write")
                    and enforcer.enforce(user_id_str, t_dom, "telemetry", "delete")
                )
            ):
                allowed_tenant_names.add(t.name.lower())

        if allowed_tenant_names:
            keys = await redis_client.keys("tb_events:user:*:registry")
            for k in keys:
                if k == user_registry_key:
                    continue
                other_tasks = await redis_client.hgetall(k)
                for tid, p_str in other_tasks.items():
                    if tid in seen_task_ids:
                        continue
                    try:
                        tdata = json.loads(p_str)
                        tname = str(tdata.get("tenant_name") or "").lower()
                        if tname and tname in allowed_tenant_names:
                            active_tasks.append(ActiveTaskResponse(**tdata))
                            seen_task_ids.add(tid)
                    except Exception:
                        pass
    except Exception as e:
        logger.warning(f"[Active Tasks] Error al verificar tareas por tenant: {e}")

    if task_type:
        allowed_types = {tt.strip().lower() for tt in task_type.split(",") if tt.strip()}
        active_tasks = [t for t in active_tasks if (t.task_type or "").lower() in allowed_types]

    return active_tasks


@router.get("/{task_id}", response_model=TaskStatusResponse, status_code=status.HTTP_200_OK)
@router.get("/status/{task_id}", response_model=TaskStatusResponse, status_code=status.HTTP_200_OK, include_in_schema=False)
async def get_task_status(
    task_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Consulta exhaustiva del estado y resultado de cualquier trabajo en ARQ.
    - Inspecciona el estado del Job en ARQ (queued, in_progress, complete, not_found, etc.).
    - Extrae el resultado retornado o el detalle de la excepción si la tarea falló.
    - Combina con los datos de progreso en tiempo real almacenados en Redis.
    """
    arq_pool = await get_arq_pool()
    job = Job(task_id, arq_pool)

    try:
        raw_status = await job.status()
        status_str = raw_status.value if hasattr(raw_status, "value") else str(raw_status)
    except Exception:
        status_str = "unknown"

    task_name: Optional[str] = None
    task_type: Optional[str] = None
    success: Optional[bool] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    enqueue_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    finish_time: Optional[datetime] = None
    progress_pct: Optional[float] = None
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

    # 1. Intentar obtener información completa de finalización desde ARQ
    try:
        result_info = await job.result_info()
        if result_info:
            task_name = result_info.function
            success = result_info.success
            enqueue_time = result_info.enqueue_time
            start_time = result_info.start_time
            finish_time = result_info.finish_time

            if result_info.success:
                result = result_info.result
                progress_pct = 100.0
                message = "Tarea completada exitosamente."
            else:
                error = str(result_info.result)
                message = f"Fallo en la ejecución de la tarea: {error}"
    except Exception as exc:
        logger.debug(f"[Task Status] Error consultando result_info para task {task_id}: {exc}")

    # 2. Consultar información de registro en Redis para progreso en vivo o fallbacks
    user_registry_key = get_user_registry_key(str(current_user.id))
    try:
        task_json = await redis_client.hget(user_registry_key, task_id)
        if task_json:
            reg_data = json.loads(task_json)
            if progress_pct is None:
                progress_pct = reg_data.get("progress_pct", 0.0)
            if message is None:
                message = reg_data.get("message")
            if details is None:
                details = reg_data.get("details")
            if not task_type:
                task_type = reg_data.get("task_type")
            if not task_name:
                task_name = reg_data.get("task_type")
            # Si en ARQ era not_found o unknown, usar el estado de Redis
            if status_str in ("not_found", "unknown"):
                status_str = reg_data.get("status", status_str)
    except Exception as exc:
        logger.debug(f"[Task Status] Error consultando registro de usuario en Redis para task {task_id}: {exc}")

    # 3. Fallback determinista para resolver task_type a partir del nombre de la función
    TASK_NAME_TO_TYPE = {
        "download_telemetry_task": "telemetry",
        "generate_excel_report_task": "excel_report",
        "generate_monthly_heatmap_task": "heatmap",
        "send_email_task": "email",
    }
    if not task_type and task_name in TASK_NAME_TO_TYPE:
        task_type = TASK_NAME_TO_TYPE[task_name]

    return TaskStatusResponse(
        task_id=task_id,
        task_type=task_type,
        status=status_str,
        task_name=task_name,
        success=success,
        progress_pct=progress_pct,
        message=message,
        result=result,
        error=error,
        enqueue_time=enqueue_time,
        start_time=start_time,
        finish_time=finish_time,
        details=details
    )


@router.get("/{task_id}/stream")
@router.get("/stream/{task_id}", include_in_schema=False)
async def stream_task_progress(
    task_id: str,
    request: Request,
    current_user: User = Depends(get_current_user)
):
    """
    Endpoint universal Server-Sent Events (SSE) para transmitir el progreso de cualquier tarea en tiempo real.
    Funciona para descargas de telemetría, envíos de correo, reportes y tareas del scheduler.
    """
    user_id = str(current_user.id)

    async def event_generator():
        pubsub = redis_client.pubsub()
        channel = get_user_stream_channel(user_id, task_id)
        registry_key = get_user_registry_key(user_id)

        try:
            await pubsub.subscribe(channel)

            # 1. Emitir estado inicial si existe en el Hash del usuario
            initial_state = await redis_client.hget(registry_key, task_id)
            if initial_state:
                yield f"data: {initial_state}\n\n"
                try:
                    parsed_initial = json.loads(initial_state)
                    if parsed_initial.get("status") in ("SUCCESS", "ERROR", "FAILURE", "CANCELLED"):
                        return
                except json.JSONDecodeError:
                    pass

            last_ping_time = asyncio.get_running_loop().time()

            while True:
                if await request.is_disconnected():
                    logger.info(f"[Tasks SSE] Cliente desconectado del stream para task_id {task_id} (user {user_id})")
                    break

                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                current_time = asyncio.get_running_loop().time()

                if message and message.get("type") == "message":
                    data_str = message.get("data")
                    yield f"data: {data_str}\n\n"
                    last_ping_time = current_time

                    try:
                        event_data = json.loads(data_str)
                        if event_data.get("status") in ("SUCCESS", "ERROR", "FAILURE", "CANCELLED"):
                            logger.info(f"[Tasks SSE] Tarea {task_id} finalizada ({event_data.get('status')}). Cerrando stream.")
                            break
                    except json.JSONDecodeError:
                        pass
                else:
                    # Keep-alive cada 15 segundos
                    if current_time - last_ping_time >= 15.0:
                        yield ": ping\n\n"
                        last_ping_time = current_time

                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info(f"[Tasks SSE] Petición cancelada por el cliente para task_id {task_id} (user {user_id})")
        except Exception as e:
            logger.error(f"[Tasks SSE] Error en el stream para task_id {task_id} (user {user_id}): {e}")
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()
            except Exception as e:
                logger.warning(f"[Tasks SSE] Error cerrando recursos de Redis para task_id {task_id}: {e}")

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


@router.post("/{job_id}/cancel", response_model=CancelTaskResponse, status_code=status.HTTP_200_OK)
@router.post("/cancel/{job_id}", response_model=CancelTaskResponse, status_code=status.HTTP_200_OK, include_in_schema=False)
async def cancel_task(
    job_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Botón de Pánico Universal: Envía una señal de aborto/cancelación inmediata a cualquier trabajo de ARQ
    en ejecución o en cola (telemetría, email, reportes, automatizaciones).
    """
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

    # Enviar señal de aborto a ARQ
    aborted = await job.abort(timeout=5.0)

    # Notificar estado CANCELLED vía Pub/Sub y purgar del registro activo
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
        await redis_client.publish(stream_channel, json.dumps(cancel_event))
        await redis_client.hdel(user_registry_key, job_id)
    except Exception as pub_err:
        logger.warning(f"[Cancel Task] Advertencia notificando cancelación en Redis: {pub_err}")

    prev_status_str = raw_status.value if hasattr(raw_status, "value") else str(raw_status)
    return CancelTaskResponse(
        status="cancelled",
        job_id=job_id,
        aborted=aborted,
        previous_status=prev_status_str,
        message=f"Señal de terminación enviada exitosamente para la tarea '{job_id}'."
    )
