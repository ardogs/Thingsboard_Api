from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from beanie import PydanticObjectId

from core.models.tb_scheduled_task import TBScheduledTask
from core.models.tb_tenant import TBTenant
from core.models.user import User
from core.logger import logger
from core.arq_pool import get_arq_pool
from api.deps import CasbinAuth
from api.endpoints.scheduler.schemas import (
    ScheduledTaskCreate,
    ScheduledTaskUpdate,
    ScheduledTaskResponse,
    ScheduledTaskTriggerResponse,
    INCREMENTAL_BACKUP_TASKS
)

router = APIRouter()


async def _extract_tenant_info(task: TBScheduledTask) -> tuple[Optional[str], Optional[str]]:
    """Extrae el tenant_id y el tenant_name de un documento TBScheduledTask."""
    t_id = None
    t_name = None
    if task.tenant_id is not None:
        if hasattr(task.tenant_id, "name") and task.tenant_id.name:
            t_name = task.tenant_id.name
            t_id = str(task.tenant_id.id)
        elif hasattr(task.tenant_id, "ref") and task.tenant_id.ref:
            t_id = str(task.tenant_id.ref.id)
            try:
                t_doc = await TBTenant.get(task.tenant_id.ref.id)
                if t_doc:
                    t_name = t_doc.name
            except Exception:
                pass
        elif hasattr(task.tenant_id, "id"):
            t_id = str(task.tenant_id.id)
            try:
                t_doc = await TBTenant.get(task.tenant_id.id)
                if t_doc:
                    t_name = t_doc.name
            except Exception:
                pass
        else:
            t_id = str(task.tenant_id)
            try:
                t_doc = await TBTenant.get(task.tenant_id)
                if t_doc:
                    t_name = t_doc.name
            except Exception:
                pass
    return t_id, t_name


async def _to_response_async(task: TBScheduledTask) -> ScheduledTaskResponse:
    """Convierte un documento Beanie TBScheduledTask en su esquema de salida Pydantic."""
    t_id, t_name = await _extract_tenant_info(task)
    return ScheduledTaskResponse(
        id=str(task.id),
        name=task.name,
        task_name=task.task_name,
        cron_expression=task.cron_expression,
        tenant_id=t_id,
        tenant_name=t_name,
        payload=task.payload or {},
        is_active=task.is_active,
        next_run_time=task.next_run_time,
        last_run_status=task.last_run_status,
        last_run_at=task.last_run_at,
        created_at=task.created_at,
        updated_at=task.updated_at
    )


async def _resolve_tenant_or_404(tenant_id: str) -> TBTenant:
    """Resuelve un documento TBTenant por ID o levanta HTTP 404 Not Found."""
    tenant = None
    try:
        obj_id = PydanticObjectId(tenant_id)
        tenant = await TBTenant.get(obj_id)
    except Exception:
        pass

    if tenant is None:
        try:
            tenant = await TBTenant.get(tenant_id)
        except Exception:
            pass

    if tenant is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No se encontró el Tenant especificado con ID: '{tenant_id}' en MongoDB"
        )
    return tenant


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
    description="Registra una nueva automatización periódica en MongoDB y calcula su primer ciclo de ejecución en UTC. Valida colisiones por Tenant (HTTP 409) para tareas de respaldo incremental."
)
async def create_scheduled_task(
    payload: ScheduledTaskCreate,
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="write", domain_type="server"))
):
    tenant = None
    if payload.tenant_id:
        tenant = await _resolve_tenant_or_404(payload.tenant_id)

    # 1. Validación de colisiones para tareas de respaldo incremental
    if payload.task_name in INCREMENTAL_BACKUP_TASKS and tenant:
        all_tasks = await TBScheduledTask.find().to_list()
        for existing_t in all_tasks:
            if existing_t.task_name in INCREMENTAL_BACKUP_TASKS:
                t_id, _ = await _extract_tenant_info(existing_t)
                if t_id == str(tenant.id):
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Ya existe una tarea de respaldo programada para este Tenant."
                    )

    # 2. Instanciar documento Beanie mapeando el tenant_id resuelto
    task = TBScheduledTask(
        name=payload.name,
        task_name=payload.task_name,
        cron_expression=payload.cron_expression,
        tenant_id=tenant,
        payload=payload.payload,
        is_active=payload.is_active,
        next_run_time=datetime.now(timezone.utc)
    )

    # 3. Calcular primer ciclo de ejecución en UTC a partir de la expresión cron
    try:
        task.next_run_time = task.compute_next_run()
    except Exception as e:
        logger.error(f"[Scheduler Router] Error al calcular primer next_run_time para '{payload.name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"No se pudo calcular la fecha de ejecución para la expresión cron: {e}"
        )

    # 4. Guardar en MongoDB
    await task.insert()
    logger.info(
        f"[Scheduler Router] Tarea programada '{task.name}' ({task.task_name}) creada por usuario '{current_user.username}'. "
        f"Próxima ejecución: {task.next_run_time.isoformat()} (UTC)"
    )
    return await _to_response_async(task)


@router.get(
    "",
    response_model=List[ScheduledTaskResponse],
    status_code=status.HTTP_200_OK,
    summary="Listar tareas programadas",
    description="Retorna la lista de todas las tareas periódicas registradas en MongoDB con soporte de filtros opcionales."
)
async def list_scheduled_tasks(
    is_active: Optional[bool] = Query(default=None, description="Filtrar por estado activo o inactivo"),
    task_name: Optional[str] = Query(default=None, description="Filtrar por nombre canónico de la tarea"),
    tenant_id: Optional[str] = Query(default=None, description="Filtrar por ID del Tenant asociado"),
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="read", domain_type="server"))
):
    query: Dict[str, Any] = {}
    if is_active is not None:
        query["is_active"] = is_active
    if task_name:
        query["task_name"] = task_name

    tasks = await TBScheduledTask.find(query).sort("-created_at").to_list()
    responses = []
    for t in tasks:
        t_id, t_name = await _extract_tenant_info(t)
        if tenant_id and t_id != str(tenant_id):
            continue
        responses.append(ScheduledTaskResponse(
            id=str(t.id),
            name=t.name,
            task_name=t.task_name,
            cron_expression=t.cron_expression,
            tenant_id=t_id,
            tenant_name=t_name,
            payload=t.payload or {},
            is_active=t.is_active,
            next_run_time=t.next_run_time,
            last_run_status=t.last_run_status,
            last_run_at=t.last_run_at,
            created_at=t.created_at,
            updated_at=t.updated_at
        ))
    return responses


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
    return await _to_response_async(task)


@router.put(
    "/{task_id}",
    response_model=ScheduledTaskResponse,
    status_code=status.HTTP_200_OK,
    summary="Actualizar tarea programada",
    description="Actualiza parcialmente los campos de una tarea. Valida colisiones por Tenant (HTTP 409) excluyendo la tarea actual."
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

    if payload.tenant_id is not None:
        if str(payload.tenant_id).strip():
            tenant = await _resolve_tenant_or_404(payload.tenant_id)
            task.tenant_id = tenant
        else:
            task.tenant_id = None

    if payload.is_active is not None:
        task.is_active = payload.is_active

    # Validación de colisiones para tareas de respaldo incremental (excluyendo el task_id actual)
    if task.task_name in INCREMENTAL_BACKUP_TASKS and task.tenant_id:
        target_t_id, _ = await _extract_tenant_info(task)
        if target_t_id:
            all_tasks = await TBScheduledTask.find().to_list()
            for other_t in all_tasks:
                if str(other_t.id) != str(task.id) and other_t.task_name in INCREMENTAL_BACKUP_TASKS:
                    t_id, _ = await _extract_tenant_info(other_t)
                    if t_id == target_t_id:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="Ya existe una tarea de respaldo programada para este Tenant."
                        )

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
    return await _to_response_async(task)


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
    description="Encola inmediatamente la tarea en el broker de ARQ con sus kwargs configurados, sin alterar el ciclo de cron programado ni la próxima fecha de ejecución."
)
async def trigger_scheduled_task(
    task_id: str,
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="write", domain_type="server"))
):
    task = await _get_task_or_404(task_id)
    now_utc = datetime.now(timezone.utc)

    try:
        kwargs_payload = dict(task.payload or {})
        t_id, _ = await _extract_tenant_info(task)
        if t_id:
            kwargs_payload["tenant_id"] = t_id

        logger.info(f"[Scheduler Router] Disparando manualmente tarea '{task.name}' ({task.task_name})...")
        
        arq_pool = await get_arq_pool()
        queue_name = "incremental_backups" if task.task_name in INCREMENTAL_BACKUP_TASKS else "default"
        job = await arq_pool.enqueue_job(
            task.task_name,
            payload=kwargs_payload,
            _queue_name=queue_name
        )
        job_id = job.job_id if job else "unknown"

        # Registrar trazabilidad del disparo manual sin alterar next_run_time
        task.last_run_status = f"MANUALLY_TRIGGERED (ARQ Job ID: {job_id})"
        task.last_run_at = now_utc
        task.updated_at = now_utc
        await task.save()

        logger.info(
            f"[Scheduler Router] Tarea '{task.name}' despachada manualmente a ARQ exitosamente (ID: {job_id}) por '{current_user.username}'."
        )

        return ScheduledTaskTriggerResponse(
            task_id=str(task.id),
            job_id=job_id,
            status="DISPATCHED",
            message=f"Tarea '{task.name}' despachada exitosamente a ARQ",
            dispatched_at=now_utc
        )
    except Exception as e:
        logger.error(f"[Scheduler Router] Error al disparar manualmente la tarea '{task.name}': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Fallo al encolar la tarea en ARQ: {str(e)}"
        )
