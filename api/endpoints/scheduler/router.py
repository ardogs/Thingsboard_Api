from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Body, status
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
    INCREMENTAL_BACKUP_TASKS,
    AvailableTaskResponse,
    get_available_tasks,
    get_available_task_names
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


SCHEDULER_OPENAPI_EXAMPLES = {
    "cleanup_old_backups": {
        "summary": "1. Purga de Respaldos (tasks.cleanup_old_backups)",
        "description": "Purga periódica de archivos ZIP caducados en disco según 'days_to_keep' y carpetas temporales huérfanas zombis (tmp_*).",
        "value": {
            "name": "Purga Diaria de Respaldos",
            "task_name": "tasks.cleanup_old_backups",
            "cron_expression": "0 3 * * *",
            "payload": {
                "days_to_keep": 30
            },
            "is_active": True
        }
    },
    "incremental_backup": {
        "summary": "2. Respaldo Incremental Tenant (tasks.execute_incremental_tenant_backup)",
        "description": "Sincronización mensual hacia Data Lake de telemetría de mes vencido (streaming JSON). Exige tenant_id.",
        "value": {
            "name": "Respaldo Incremental Mensual CFE",
            "task_name": "tasks.execute_incremental_tenant_backup",
            "cron_expression": "0 2 1 * *",
            "tenant_id": "64b1f2e3d4c5b6a789012345",
            "payload": {
                "concurrency_limit": 4,
                "page_limit": 2000,
                "year": 2026,
                "month": 8,
                "entity_type": "DEVICE"
            },
            "is_active": True
        }
    },
    "collect_system_info": {
        "summary": "3. Monitoreo de Recursos de Hardware (tasks.collect_servers_system_info)",
        "description": "Recolección periódica del estado de salud (CPU, RAM, Disco) desde /api/admin/systemInfo de ThingsBoard.",
        "value": {
            "name": "Monitoreo Periódico de Servidores",
            "task_name": "tasks.collect_servers_system_info",
            "cron_expression": "*/30 * * * *",
            "payload": {
                "server_id": None
            },
            "is_active": True
        }
    },
    "download_telemetry": {
        "summary": "4. Descarga Masiva de Telemetría (tasks.download_telemetry)",
        "description": "Descarga masiva para rango temporal y generación de ZIP descargable. Exige tenant_id.",
        "value": {
            "name": "Descarga Semanal de Telemetría CONAFOR",
            "task_name": "tasks.download_telemetry",
            "cron_expression": "0 4 * * 0",
            "tenant_id": "64b1f2e3d4c5b6a789012345",
            "payload": {
                "start_date": "2026-08-01T00:00:00",
                "end_date": "2026-08-31T23:59:59",
                "entity_type": "DEVICE",
                "entity_id": None,
                "time_zone": "America/Mexico_City",
                "concurrency_limit": 3,
                "page_limit": 2000,
                "force_reload": False
            },
            "is_active": True
        }
    },
    "generate_excel_report": {
        "summary": "5. Reporte de Telemetría en Excel (tasks.generate_excel_report)",
        "description": "Generación periódica de hojas de cálculo .xlsx estructuradas con filtrado por lista blanca. Exige tenant_id.",
        "value": {
            "name": "Reporte Mensual en Excel (.xlsx)",
            "task_name": "tasks.generate_excel_report",
            "cron_expression": "0 5 1 * *",
            "tenant_id": "64b1f2e3d4c5b6a789012345",
            "payload": {
                "combine_in_single_file": True,
                "year": 2026,
                "month": 8,
                "time_zone": "America/Mexico_City",
                "concurrency_limit": 3,
                "page_limit": 2000
            },
            "is_active": True
        }
    }
}

SCHEDULER_UPDATE_OPENAPI_EXAMPLES = {
    "update_cron": {
        "summary": "Modificar Expresión Cron",
        "value": {
            "cron_expression": "30 4 * * *"
        }
    },
    "update_payload_retention": {
        "summary": "Modificar Días de Retención (cleanup_old_backups)",
        "value": {
            "payload": {
                "days_to_keep": 15
            }
        }
    },
    "update_payload_incremental": {
        "summary": "Modificar Concurrencia (execute_incremental_tenant_backup)",
        "value": {
            "payload": {
                "concurrency_limit": 6,
                "page_limit": 2500
            }
        }
    },
    "update_payload_excel": {
        "summary": "Modificar Modo de Archivo (generate_excel_report)",
        "value": {
            "payload": {
                "combine_in_single_file": False,
                "concurrency_limit": 4
            }
        }
    }
}


@router.post(
    "",
    response_model=ScheduledTaskResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Crear nueva tarea programada",
    description=(
        "Registra una nueva automatización periódica en MongoDB y calcula su primer ciclo de ejecución en UTC.\n\n"
        "### 📚 Guía Exhaustiva de Parámetros Especiales en `payload` por Tarea:\n\n"
        "| Tarea (`task_name`) | Parámetro | Tipo | Default | Obligatorio | Descripción |\n"
        "| :--- | :--- | :--- | :--- | :--- | :--- |\n"
        "| **`tasks.cleanup_old_backups`** | `days_to_keep` | `integer` | `30` | No | Días de antigüedad máxima para conservar respaldos ZIP y registros `TBBackup`. Temporales huérfanos (`tmp_*`) con más de 24h sin escritura son purgados. |\n"
        "| **`tasks.execute_incremental_tenant_backup`** | `tenant_id` | `string` | — | **Sí** | ID de MongoDB del Tenant a respaldar. |\n"
        "| | `concurrency_limit` | `integer` | `4` | No | Concurrencia máxima de descarga de llaves de telemetría. |\n"
        "| | `page_limit` | `integer` | `2000` | No | Tamaño de lote por consulta REST a ThingsBoard. |\n"
        "| | `year` | `integer` | `null` | No | Año específico a respaldar. Si se omite junto con `month`, calcula de forma autónoma el mes cerrado anterior. |\n"
        "| | `month` | `integer` | `null` | No | Mes específico (1-12) a respaldar. |\n"
        "| | `entity_type` | `string` | `'DEVICE'` | No | Tipo de entidad ThingsBoard (`'DEVICE'` o `'ASSET'`). |\n"
        "| | `base_storage_dir` | `string` | `'tenant_backups'` | No | Carpeta raíz del Data Lake persistente. |\n"
        "| **`tasks.collect_servers_system_info`** | `server_id` | `string` | `null` | No | ID en MongoDB del TBServer específico a inspeccionar. `null` consulta todos los servidores registrados. |\n"
        "| **`tasks.download_telemetry`** | `start_date` | `string` | — | Opcional | Fecha inicial ISO-8601 (ej: `'2026-08-01T00:00:00'`). |\n"
        "| | `end_date` | `string` | — | Opcional | Fecha final ISO-8601 (ej: `'2026-08-31T23:59:59'`). |\n"
        "| | `time_zone` | `string` | `'America/Mexico_City'` | No | Zona horaria para delimitar marcas de tiempo. |\n"
        "| | `entity_type` | `string` | `'DEVICE'` | No | Tipo de entidad (`'DEVICE'` o `'ASSET'`). |\n"
        "| | `entity_id` | `string`/`list` | `null` | No | UUID o lista de UUIDs de dispositivos. `null` abarca todos. |\n"
        "| | `concurrency_limit` | `integer` | `3` | No | Concurrencia simultánea de descarga. |\n"
        "| | `page_limit` | `integer` | `2000` | No | Registros por página. |\n"
        "| | `force_reload` | `boolean` | `false` | No | Descartar checkpoints en Redis e iniciar extracción desde cero. |\n"
        "| **`tasks.generate_excel_report`** | `combine_in_single_file` | `boolean` | `true` | No | `true`: archivo `.xlsx` multi-hoja; `false`: empaquetado `.zip` con `.xlsx` individuales. |\n"
        "| | `year` | `integer` | `null` | No | Año del reporte mensual (omite para mes cerrado anterior). |\n"
        "| | `month` | `integer` | `null` | No | Mes del reporte (1-12) (omite para mes cerrado anterior). |\n"
        "| | `start_date` / `end_date` | `string` | `null` | No | Rango exacto ISO si no se usa año/mes. |\n"
        "| | `time_zone` | `string` | `'America/Mexico_City'` | No | Zona horaria para formato de celdas. |\n"
        "| | `concurrency_limit` | `integer` | `3` | No | Concurrencia de extracción de telemetría. |\n"
        "| | `page_limit` | `integer` | `2000` | No | Registros por página REST. |"
    )
)
async def create_scheduled_task(
    payload: ScheduledTaskCreate = Body(..., openapi_examples=SCHEDULER_OPENAPI_EXAMPLES),
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
    "/available",
    response_model=List[AvailableTaskResponse],
    status_code=status.HTTP_200_OK,
    summary="Listar tareas disponibles para programación",
    description="Retorna el catálogo completo de tareas registradas en los workers de ARQ listas para programar, incluyendo sus nombres canónicos (task_name), descripciones, requerimientos de Tenant, cron sugerido y parámetros de payload."
)
async def list_available_tasks(
    category: Optional[str] = Query(default=None, description="Filtrar por categoría (ej: 'Mantenimiento', 'Respaldos', 'Monitoreo', 'Telemetría', 'Reportes')"),
    requires_tenant: Optional[bool] = Query(default=None, description="Filtrar por requerimiento obligatorio de Tenant (True/False)"),
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="read", domain_type="server"))
):
    return get_available_tasks(category=category, requires_tenant=requires_tenant)


@router.get(
    "/available-tasks",
    response_model=List[AvailableTaskResponse],
    status_code=status.HTTP_200_OK,
    summary="Alias: Listar tareas disponibles para programación",
    description="Alias de /available para facilitar el consumo desde el front end.",
    include_in_schema=False
)
async def list_available_tasks_alias(
    category: Optional[str] = Query(default=None),
    requires_tenant: Optional[bool] = Query(default=None),
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="read", domain_type="server"))
):
    return get_available_tasks(category=category, requires_tenant=requires_tenant)


@router.get(
    "/task-names",
    response_model=List[str],
    status_code=status.HTTP_200_OK,
    summary="Listar nombres canónicos de tareas (task_name)",
    description="Retorna únicamente el listado simple de cadenas de texto con los task_name registrados para rellenar de forma inmediata selectores o dropdowns en el front end."
)
async def list_task_names(
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="read", domain_type="server"))
):
    return get_available_task_names()


@router.get(
    "/names",
    response_model=List[str],
    status_code=status.HTTP_200_OK,
    summary="Alias: Listar nombres canónicos de tareas",
    description="Alias de /task-names para el front end.",
    include_in_schema=False
)
async def list_task_names_alias(
    current_user: User = Depends(CasbinAuth(resource="scheduler", action="read", domain_type="server"))
):
    return get_available_task_names()


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
    description=(
        "Actualiza parcialmente los campos de una tarea programada existente en MongoDB.\n\n"
        "Permite modificar el nombre, la función en ARQ (`task_name`), la expresión cron de 5 campos (recalculando automáticamente `next_run_time`), "
        "el tenant asociado (`tenant_id`), el estado (`is_active`) y los argumentos personalizados del `payload`.\n\n"
        "Consulta la documentación de `POST /api/v1/scheduler/tasks` para conocer la tabla completa de parámetros especiales soportados por tarea."
    )
)
async def update_scheduled_task(
    task_id: str,
    payload: ScheduledTaskUpdate = Body(..., openapi_examples=SCHEDULER_UPDATE_OPENAPI_EXAMPLES),
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
        queue_name = "incremental_backups" if task.task_name in INCREMENTAL_BACKUP_TASKS else None
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
