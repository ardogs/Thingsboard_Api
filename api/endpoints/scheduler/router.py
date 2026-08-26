from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from beanie import PydanticObjectId

from core.models.tb_scheduled_task import TBScheduledTask
from core.models.user import User
from core.logger import logger
from api.deps import CasbinAuth
from workers.tasks import celery_app
from api.endpoints.scheduler.schemas import (
    ScheduledTaskCreate,
    ScheduledTaskUpdate,
    ScheduledTaskResponse,
    ScheduledTaskTriggerResponse
)

router = APIRouter()


def _to_response(task: TBScheduledTask) -> ScheduledTaskResponse:
    """Convierte un documento Beanie TBScheduledTask en su esquema de salida Pydantic."""
    return ScheduledTaskResponse(
        id=str(task.id),
        name=task.name,
        task_name=task.task_name,
        cron_expression=task.cron_expression,
        payload=task.payload or {},
        is_active=task.is_active,
        next_run_time=task.next_run_time,
        last_run_status=task.last_run_status,
        last_run_at=task.last_run_at,
        created_at=task.created_at,
        updated_at=task.updated_at
    )


async def _get_task_or_404(task_id: str) -> TBScheduledTask:
    """Resuelve un documento TBScheduledTask por ID o levanta HTTP 404 Not Found."""
    task = None
    try:
        obj_id = PydanticObjectId(task_id)
        task = await TBScheduledTask.get(obj_id)
    except Exception:
        pass

    if task is None:
        try:
            task = await TBScheduledTask.get(task_id)
        except Exception:
            pass

    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No se encontró la tarea programada con ID: '{task_id}'"
        )
    return task


@router.post(
    "",
    response_model=ScheduledTaskResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Crear nueva tarea programada",
    description="Registra una nueva automatización periódica en MongoDB y calcula su primer ciclo de ejecución en UTC según la zona horaria del sistema."
)
async def create_scheduled_task(
    payload: ScheduledTaskCreate,
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="write", domain_type="server"))
):
    # 1. Instanciar documento provisional
    task = TBScheduledTask(
        name=payload.name,
        task_name=payload.task_name,
        cron_expression=payload.cron_expression,
        payload=payload.payload,
        is_active=payload.is_active,
        next_run_time=datetime.now(timezone.utc)
    )

    # 2. Calcular primer ciclo de ejecución en UTC a partir de la expresión cron
    try:
        task.next_run_time = task.compute_next_run()
    except Exception as e:
        logger.error(f"[Scheduler Router] Error al calcular primer next_run_time para '{payload.name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"No se pudo calcular la fecha de ejecución para la expresión cron: {e}"
        )

    # 3. Guardar en MongoDB
    await task.insert()
    logger.info(
        f"[Scheduler Router] Tarea programada '{task.name}' ({task.task_name}) creada por usuario '{current_user.username}'. "
        f"Próxima ejecución: {task.next_run_time.isoformat()} (UTC)"
    )
    return _to_response(task)


@router.get(
    "",
    response_model=List[ScheduledTaskResponse],
    status_code=status.HTTP_200_OK,
    summary="Listar tareas programadas",
    description="Retorna la lista de todas las tareas periódicas registradas en MongoDB con soporte de filtros opcionales."
)
async def list_scheduled_tasks(
    is_active: Optional[bool] = Query(default=None, description="Filtrar por estado activo o inactivo"),
    task_name: Optional[str] = Query(default=None, description="Filtrar por nombre canónico de Celery"),
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="read", domain_type="server"))
):
    query: Dict[str, Any] = {}
    if is_active is not None:
        query["is_active"] = is_active
    if task_name:
        query["task_name"] = task_name

    tasks = await TBScheduledTask.find(query).sort("-created_at").to_list()
    return [_to_response(t) for t in tasks]


@router.get(
    "/{task_id}",
    response_model=ScheduledTaskResponse,
    status_code=status.HTTP_200_OK,
    summary="Obtener detalle de una tarea programada",
    description="Consulta la información detallada de una tarea específica por su ID de MongoDB."
)
async def get_scheduled_task(
    task_id: str,
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="read", domain_type="server"))
):
    task = await _get_task_or_404(task_id)
    return _to_response(task)


@router.put(
    "/{task_id}",
    response_model=ScheduledTaskResponse,
    status_code=status.HTTP_200_OK,
    summary="Actualizar tarea programada",
    description="Actualiza parcialmente los campos de una tarea. Si cambia la expresión cron, recalcula automáticamente la próxima fecha de ejecución."
)
async def update_scheduled_task(
    task_id: str,
    payload: ScheduledTaskUpdate,
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="write", domain_type="server"))
):
    task = await _get_task_or_404(task_id)

    if payload.name is not None:
        task.name = payload.name
    if payload.task_name is not None:
        task.task_name = payload.task_name
    if payload.payload is not None:
        task.payload = payload.payload
    if payload.is_active is not None:
        task.is_active = payload.is_active

    # Si cambia la expresión cron, recalcular el próximo ciclo de ejecución
    if payload.cron_expression is not None and payload.cron_expression != task.cron_expression:
        task.cron_expression = payload.cron_expression
        try:
            task.next_run_time = task.compute_next_run()
            logger.info(
                f"[Scheduler Router] Expresión cron actualizada para '{task.name}'. "
                f"Nuevo next_run_time: {task.next_run_time.isoformat()} (UTC)"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Expresión cron inválida o error en cálculo: {e}"
            )

    task.updated_at = datetime.now(timezone.utc)
    await task.save()
    logger.info(f"[Scheduler Router] Tarea programada '{task.name}' [ID: {task_id}] actualizada por '{current_user.username}'.")
    return _to_response(task)


@router.delete(
    "/{task_id}",
    status_code=status.HTTP_200_OK,
    summary="Eliminar tarea programada",
    description="Elimina de forma permanente el documento de la tarea programada en MongoDB."
)
async def delete_scheduled_task(
    task_id: str,
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="delete", domain_type="server"))
):
    task = await _get_task_or_404(task_id)
    task_name = task.name
    await task.delete()
    logger.info(f"[Scheduler Router] Tarea programada '{task_name}' [ID: {task_id}] eliminada por '{current_user.username}'.")
    return {
        "status": "success",
        "message": f"Tarea programada '{task_name}' eliminada exitosamente",
        "id": task_id
    }


@router.post(
    "/{task_id}/trigger",
    response_model=ScheduledTaskTriggerResponse,
    status_code=status.HTTP_200_OK,
    summary="Ejecutar tarea programada bajo demanda",
    description="Encola inmediatamente la tarea en el broker de Celery con sus kwargs configurados, sin alterar el ciclo de cron programado ni la próxima fecha de ejecución."
)
async def trigger_scheduled_task(
    task_id: str,
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="write", domain_type="server"))
):
    task = await _get_task_or_404(task_id)
    now_utc = datetime.now(timezone.utc)

    try:
        kwargs_payload = task.payload or {}
        logger.info(f"[Scheduler Router] Disparando manualmente tarea '{task.name}' ({task.task_name})...")
        
        async_result = celery_app.send_task(task.task_name, kwargs=kwargs_payload)
        celery_task_id = async_result.id if async_result else "unknown"

        # Registrar trazabilidad del disparo manual sin alterar next_run_time
        task.last_run_status = f"MANUALLY_TRIGGERED (Celery Task ID: {celery_task_id})"
        task.last_run_at = now_utc
        task.updated_at = now_utc
        await task.save()

        logger.info(
            f"[Scheduler Router] Tarea '{task.name}' despachada manualmente a Celery exitosamente (ID: {celery_task_id}) por '{current_user.username}'."
        )

        return ScheduledTaskTriggerResponse(
            task_id=str(task.id),
            celery_task_id=celery_task_id,
            status="DISPATCHED",
            message=f"Tarea '{task.name}' despachada exitosamente a Celery",
            dispatched_at=now_utc
        )
    except Exception as e:
        logger.error(f"[Scheduler Router] Error al disparar manualmente la tarea '{task.name}': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Fallo al encolar la tarea en Celery: {str(e)}"
        )
