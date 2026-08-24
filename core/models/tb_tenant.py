from datetime import datetime, timezone
from typing import Optional, Dict, Any
from beanie import Document, Link
from pydantic import Field

from core.models.tb_server import TBServer


class TBTenant(Document):
    """
    Modelo de Documento Beanie para la persistencia de Tenants individuales dentro de un TBServer.
    Almacena las credenciales de Tenant Admin, tokens JWT de sesión y metadatos específicos por Tenant.
    """
    server_id: Link[TBServer] = Field(..., description="Referencia/Link al servidor ThingsBoard padre")
    name: str = Field(..., description="Nombre del tenant")
    
    # Credenciales del Tenant Admin en ThingsBoard
    username: Optional[str] = Field(default=None, description="Usuario / Email del Tenant Admin en ThingsBoard")
    password: Optional[str] = Field(default=None, description="Contraseña del Tenant Admin en ThingsBoard")
    
    # Tokens JWT de sesión con ThingsBoard
    token: Optional[str] = Field(default=None, description="Token JWT de acceso a ThingsBoard")
    refresh_token: Optional[str] = Field(default=None, description="Refresh Token de ThingsBoard")

    # Metadatos dinámicos específicos del Tenant
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables específicos del tenant")

    # Aislamiento por usuario en el API Gateway
    user_id: Optional[str] = Field(default=None, description="Identificador del usuario propietario en el API Gateway")
    is_active: bool = Field(default=True, description="Estado activo o inactivo del tenant")

    # Auditoría
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Settings:
        name = "tb_tenants"
        indexes = [
            "server_id",
            "user_id",
            "name"
        ]

    async def get_server(self) -> Optional[TBServer]:
        """
        Resuelve y retorna el documento TBServer padre asociado a este Tenant.
        """
        if isinstance(self.server_id, TBServer):
            return self.server_id
        ref = self.server_id.to_ref() if hasattr(self.server_id, "to_ref") else self.server_id
        ref_id = ref.id if hasattr(ref, "id") else ref
        try:
            return await TBServer.get(ref_id)
        except Exception:
            return None

    def get_server_id_str(self) -> str:
        """
        Retorna el ID del servidor padre en formato string.
        """
        if isinstance(self.server_id, TBServer):
            return str(self.server_id.id)
        ref = self.server_id.to_ref() if hasattr(self.server_id, "to_ref") else self.server_id
        ref_id = ref.id if hasattr(ref, "id") else ref
        return str(ref_id)
