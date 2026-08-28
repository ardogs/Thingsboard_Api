from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any
from beanie import Document, Link
from pydantic import Field, field_validator
from croniter import croniter

from core.config import settings
from core.models.tb_tenant import TBTenant


class TBScheduledTask(Document):
    """
    Modelo de Documento Beanie para la programación dinámica de tareas periódicas (ARQ Master Dispatcher).
    Representa el Despachador Maestro donde MongoDB es la única fuente de verdad para los horarios.
    Las expresiones cron se calculan sobre la zona horaria local de la aplicación (settings.APP_TIMEZONE)
    y se persisten estrictamente normalizadas en UTC.
    """
    name: str = Field(..., description="Nombre descriptivo de la tarea programada")
    task_name: str = Field(..., description="Nombre canónico y registrado de la tarea en ARQ")
    cron_expression: str = Field(..., description="Expresión cron estándar de 5 campos (ej: '0 8 * * *', '*/5 * * * *')")
    tenant_id: Optional[Link[TBTenant]] = Field(default=None, description="Enlace directo al Tenant objetivo para tareas granulares por cliente")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Diccionario de argumentos adicionales (kwargs) para la tarea de ARQ")
    next_run_time: datetime = Field(..., description="Próxima fecha y hora de ejecución estricta en UTC")
    is_active: bool = Field(default=True, description="Flag booleano que activa o desactiva la ejecución periódica")
    last_run_status: Optional[str] = Field(default=None, description="Estado de la última ejecución (ej: 'DISPATCHED', 'SUCCESS', 'ERROR: ...')")
    last_run_at: Optional[datetime] = Field(default=None, description="Marca de tiempo UTC de la última ejecución despachada")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Fecha y hora de creación de la tarea programada")
    updated_at: Optional[datetime] = Field(default=None, description="Fecha y hora de última actualización del documento")

    @field_validator("next_run_time", "last_run_at", "created_at", "updated_at", mode="after")
    @classmethod
    def ensure_tz_aware(cls, v: Optional[datetime]) -> Optional[datetime]:
        """Asegura que todas las marcas de tiempo sean offset-aware en UTC puro."""
        if v is not None and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    class Settings:
        name = "tb_scheduled_tasks"
        indexes = [
            [("is_active", 1), ("next_run_time", 1)],
            [("task_name", 1), ("tenant_id", 1), ("is_active", 1)],
            "name",
            "task_name",
        ]

    def compute_next_run(self, base_time: Optional[datetime] = None, tz_str: Optional[str] = None) -> datetime:
        """
        Calcula la próxima fecha de ejecución interpretando la expresión cron en la zona horaria local
        especificada (o settings.APP_TIMEZONE por defecto) y retorna el resultado normalizado en UTC puro.
        """
        target_tz_name = tz_str or settings.APP_TIMEZONE
        local_tz = ZoneInfo(target_tz_name)

        if base_time is None:
            base_local_dt = datetime.now(local_tz)
        else:
            if base_time.tzinfo is None:
                # Si viene naive, asumimos UTC y convertimos a la zona horaria local
                base_local_dt = base_time.replace(tzinfo=timezone.utc).astimezone(local_tz)
            else:
                base_local_dt = base_time.astimezone(local_tz)

        cron = croniter(self.cron_expression, base_local_dt)
        next_local_dt = cron.get_next(datetime)

        if next_local_dt.tzinfo is None:
            next_local_dt = next_local_dt.replace(tzinfo=local_tz)

        return next_local_dt.astimezone(timezone.utc)
