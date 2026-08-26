from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
from croniter import croniter


def validate_cron_expression(v: Optional[str]) -> Optional[str]:
    """Valida que la expresión cron tenga un formato estándar válido de 5 campos interpretables por croniter."""
    if v is not None:
        clean_v = v.strip()
        if not croniter.is_valid(clean_v):
            raise ValueError(
                "Expresión cron inválida. Debe contener un formato estándar válido de 5 campos (ej: '0 8 * * *', '*/15 * * * *')."
            )
        return clean_v
    return v


class ScheduledTaskCreate(BaseModel):
    """Esquema de entrada para crear una nueva tarea programada."""
    name: str = Field(..., description="Nombre descriptivo de la tarea programada", min_length=3, max_length=100)
    task_name: str = Field(..., description="Nombre canónico registrado en Celery (ej: 'tasks.cleanup_old_backups')", min_length=3, max_length=100)
    cron_expression: str = Field(..., description="Expresión cron estándar de 5 campos (ej: '0 3 * * *')")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Diccionario de argumentos (kwargs) a pasar a la tarea de Celery")
    is_active: bool = Field(default=True, description="Estado activo o inactivo de la programación")

    @field_validator("cron_expression", mode="after")
    @classmethod
    def check_cron(cls, v: str) -> str:
        return validate_cron_expression(v)


class ScheduledTaskUpdate(BaseModel):
    """Esquema de entrada para actualizar una tarea programada existente (todos los campos opcionales)."""
    name: Optional[str] = Field(default=None, description="Nombre descriptivo de la tarea", min_length=3, max_length=100)
    task_name: Optional[str] = Field(default=None, description="Nombre canónico en Celery", min_length=3, max_length=100)
    cron_expression: Optional[str] = Field(default=None, description="Nueva expresión cron de 5 campos")
    payload: Optional[Dict[str, Any]] = Field(default=None, description="Nuevos argumentos (kwargs)")
    is_active: Optional[bool] = Field(default=None, description="Activar o desactivar la tarea")

    @field_validator("cron_expression", mode="after")
    @classmethod
    def check_cron(cls, v: Optional[str]) -> Optional[str]:
        return validate_cron_expression(v)


class ScheduledTaskResponse(BaseModel):
    """Esquema de salida con la información completa de la tarea programada."""
    id: str = Field(..., description="Identificador único del documento en MongoDB")
    name: str = Field(..., description="Nombre descriptivo de la tarea programada")
    task_name: str = Field(..., description="Nombre canónico registrado en Celery")
    cron_expression: str = Field(..., description="Expresión cron estándar")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Argumentos kwargs asociados")
    is_active: bool = Field(..., description="Flag de estado activo/inactivo")
    next_run_time: datetime = Field(..., description="Próxima fecha y hora de ejecución programada en UTC")
    last_run_status: Optional[str] = Field(default=None, description="Estado de la última ejecución despachada")
    last_run_at: Optional[datetime] = Field(default=None, description="Marca de tiempo UTC del último despacho")
    created_at: datetime = Field(..., description="Fecha de creación")
    updated_at: Optional[datetime] = Field(default=None, description="Fecha de última actualización")


class ScheduledTaskTriggerResponse(BaseModel):
    """Esquema de respuesta para disparos manuales inmediatos en Celery."""
    task_id: str = Field(..., description="ID de la tarea programada en MongoDB")
    celery_task_id: str = Field(..., description="UUID de la tarea encolada en Celery")
    status: str = Field(..., description="Estado del despacho manual")
    message: str = Field(..., description="Mensaje explicativo del despacho")
    dispatched_at: datetime = Field(..., description="Marca de tiempo del despacho en UTC")
