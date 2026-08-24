from datetime import datetime, timezone
from typing import Optional
from beanie import Document, Link, PydanticObjectId
from pydantic import Field

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
    start_date: datetime = Field(..., description="Fecha de inicio del rango de telemetría respaldado")
    end_date: datetime = Field(..., description="Fecha de fin del rango de telemetría respaldado")
    file_size_bytes: int = Field(default=0, description="Tamaño del archivo ZIP en bytes")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Fecha y hora de creación del respaldo")

    class Settings:
        name = "tb_backups"
        indexes = [
            "tenant_id",
            "task_id",
            "requested_by",
            "created_at",
        ]

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
