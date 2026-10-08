from datetime import datetime, timezone
from typing import Optional
from beanie import Document, Link, PydanticObjectId
from pydantic import Field, field_validator

from core.models.tb_tenant import TBTenant


class TBBackup(Document):
    """
    Modelo de Documento Beanie para el catálogo global de respaldos de telemetría.
    Registra los metadatos de los archivos ZIP generados por tareas de Celery,
    permitiendo la visibilidad y descarga compartida entre usuarios autorizados.
    """
    tenant_id: Link[TBTenant] = Field(..., description="Referencia/Link al Tenant de ThingsBoard respaldado")
    task_id: str = Field(..., description="Identificador único de la tarea de Celery")
    requested_by: str = Field(..., description="ID del usuario que solicitó el respaldo")
    file_name: str = Field(..., description="Nombre del archivo ZIP generado y almacenado en disco")
    backup_type: str = Field(default="telemetry", description="Tipo de respaldo o artefacto generado (telemetry, excel_report, heatmap, etc.)")
    start_date: datetime = Field(..., description="Fecha de inicio del rango de telemetría respaldado")
    end_date: datetime = Field(..., description="Fecha de fin del rango de telemetría respaldado")
    file_size_bytes: int = Field(default=0, description="Tamaño del archivo ZIP en bytes")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Fecha y hora de creación del respaldo")

    @field_validator("start_date", "end_date", "created_at", mode="after")
    @classmethod
    def ensure_tz_aware(cls, v: Optional[datetime]) -> Optional[datetime]:
        """Asegura que todas las marcas de tiempo sean offset-aware en UTC puro."""
        if v is not None and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    class Settings:
        name = "tb_backups"
        indexes = [
            "tenant_id",
            "task_id",
            "requested_by",
            "created_at",
            "backup_type",
        ]

    def get_backup_type(self) -> str:
        """
        Retorna el tipo de artefacto/respaldo con compatibilidad hacia atrás.
        Si backup_type está establecido explícitamente a un valor no por defecto o si el archivo
        posee una extensión reconocible (.pdf -> heatmap, .xlsx -> excel_report), resuelve el tipo adecuado;
        de lo contrario, recurre a 'telemetry'.
        """
        if hasattr(self, "backup_type") and self.backup_type and self.backup_type not in ("telemetry", None):
            return self.backup_type

        if self.file_name:
            fname = self.file_name.lower()
            if fname.endswith(".pdf"):
                return "heatmap"
            if fname.endswith(".xlsx"):
                return "excel_report"

        return getattr(self, "backup_type", "telemetry") or "telemetry"

    async def get_tenant(self) -> Optional[TBTenant]:
        """
        Resuelve y retorna el documento TBTenant padre asociado a este respaldo.
        """
        if isinstance(self.tenant_id, TBTenant):
            return self.tenant_id
        ref = self.tenant_id.to_ref() if hasattr(self.tenant_id, "to_ref") else self.tenant_id
        ref_id = ref.id if hasattr(ref, "id") else ref
        try:
            return await TBTenant.get(ref_id)
        except Exception:
            return None

    def get_tenant_id_str(self) -> str:
        """
        Retorna el ID del tenant en formato string.
        """
        if isinstance(self.tenant_id, TBTenant):
            return str(self.tenant_id.id)
        ref = self.tenant_id.to_ref() if hasattr(self.tenant_id, "to_ref") else self.tenant_id
        ref_id = ref.id if hasattr(ref, "id") else ref
        return str(ref_id)
