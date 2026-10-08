from datetime import datetime
from typing import Optional, Any, Dict
from pydantic import BaseModel, Field


class ActiveTaskResponse(BaseModel):
    """Esquema para una tarea en ejecución activa en Redis."""
    task_id: str = Field(..., description="Identificador único del trabajo en ARQ")
    user_id: Optional[str] = Field(default=None, description="ID del usuario que solicitó la tarea")
    task_type: Optional[str] = Field(default="general", description="Dominio/tipo de tarea (telemetry, email, excel_report, heatmap, etc.)")
    status: str = Field(..., description="Estado actual (ej: IN_PROGRESS, PROCESSING, RETRYING, SUCCESS, FAILURE)")
    tenant_name: Optional[str] = Field(default=None, description="Nombre del tenant asociado (si aplica)")
    current_device: Optional[str] = Field(default=None, description="Dispositivo actual en procesamiento (si aplica)")
    current_key: Optional[str] = Field(default=None, description="Clave de telemetría actual (si aplica)")
    progress_pct: float = Field(default=0.0, description="Porcentaje de avance normalizado de 0 a 100")
    total_records: int = Field(default=0, description="Total de registros calculados/procesados")
    records_count: Optional[int] = Field(default=None, description="Conteo de registros procesados")
    message: Optional[str] = Field(default=None, description="Mensaje explicativo o descripción del paso actual")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Detalles adicionales del contexto de la tarea")
    updated_at: Optional[str] = Field(default=None, description="Marca de tiempo ISO de la última actualización")


class TaskStatusResponse(BaseModel):
    """Respuesta exhaustiva del estado y resultado de una tarea en segundo plano."""
    task_id: str = Field(..., description="Identificador único de la tarea en ARQ")
    task_type: Optional[str] = Field(default=None, description="Tipo/dominio de tarea")
    status: str = Field(..., description="Estado devuelto por ARQ o registro (queued, in_progress, complete, not_found, etc.)")
    task_name: Optional[str] = Field(default=None, description="Nombre de la función ARQ ejecutada")
    success: Optional[bool] = Field(default=None, description="True si la tarea finalizó con éxito, False si falló")
    progress_pct: Optional[float] = Field(default=None, description="Porcentaje de avance (si está disponible)")
    message: Optional[str] = Field(default=None, description="Mensaje de estado o diagnóstico")
    result: Optional[Any] = Field(default=None, description="Valor retornado por la tarea si finalizó exitosamente")
    error: Optional[str] = Field(default=None, description="Detalle de la excepción o causa de fallo si ocurrió un error")
    retries: Optional[int] = Field(default=None, description="Número de reintentos realizados (si aplica)")
    enqueue_time: Optional[datetime] = Field(default=None, description="Fecha y hora de encolamiento")
    start_time: Optional[datetime] = Field(default=None, description="Fecha y hora de inicio de ejecución")
    finish_time: Optional[datetime] = Field(default=None, description="Fecha y hora de finalización")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Metadatos contextuales adicionales")


class CancelTaskResponse(BaseModel):
    """Respuesta tras solicitar la cancelación o aborto de una tarea."""
    status: str = Field(..., description="Estado tras la solicitud (ej: cancelled)")
    job_id: str = Field(..., description="Identificador de la tarea abortada")
    aborted: bool = Field(..., description="Indica si la señal de aborto fue procesada por ARQ")
    previous_status: str = Field(..., description="Estado que tenía la tarea antes de ser cancelada")
    message: str = Field(..., description="Descripción del resultado de la cancelación")
