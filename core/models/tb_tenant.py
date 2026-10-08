from datetime import datetime, timezone
from typing import Optional, Dict, Any
from beanie import Document, Link
from pydantic import Field, field_validator

from core.models.tb_server import TBServer
from core.crypto import encrypt_data, decrypt_data


class TBTenant(Document):
    """
    Modelo de Documento Beanie para la persistencia de Tenants individuales dentro de un TBServer.
    Almacena las credenciales de Tenant Admin, tokens JWT de sesión y metadatos específicos por Tenant.
    Todas las credenciales y tokens se almacenan cifrados con Fernet en reposo (MongoDB).
    """
    server_id: Link[TBServer] = Field(..., description="Referencia/Link al servidor ThingsBoard padre")
    name: str = Field(..., description="Nombre del tenant")
    
    # Credenciales del Tenant Admin en ThingsBoard (username en texto plano, contraseña cifrada)
    username: Optional[str] = Field(default=None, description="Usuario / Email del Tenant Admin en ThingsBoard")
    encrypted_password: Optional[str] = Field(default=None, description="Contraseña cifrada (Fernet) del Tenant Admin en ThingsBoard")
    
    # Tokens JWT de sesión con ThingsBoard cifrados
    encrypted_token: Optional[str] = Field(default=None, description="Token JWT de acceso cifrado (Fernet) a ThingsBoard")
    encrypted_refresh_token: Optional[str] = Field(default=None, description="Refresh Token cifrado (Fernet) de ThingsBoard")

    # Metadatos dinámicos específicos del Tenant
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables específicos del tenant")

    # Aislamiento por usuario en el API Gateway
    user_id: Optional[str] = Field(default=None, description="Identificador del usuario propietario en el API Gateway")
    is_active: bool = Field(default=True, description="Estado activo o inactivo del tenant")

    # Auditoría
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("created_at", "updated_at", mode="after")
    @classmethod
    def ensure_tz_aware(cls, v: Optional[datetime]) -> Optional[datetime]:
        """Asegura que todas las marcas de tiempo sean offset-aware en UTC puro."""
        if v is not None and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    class Settings:
        name = "tb_tenants"
        indexes = [
            "server_id",
            "user_id",
            "name"
        ]

    # ==========================================
    # Getters y Setters Seguros (Fernet RAM Decryption)
    # ==========================================

    def set_password(self, plain_password: Optional[str]) -> None:
        """
        Cifra y asigna la contraseña del Tenant Admin antes de persistir en MongoDB.
        """
        if plain_password:
            self.encrypted_password = encrypt_data(plain_password)
        else:
            self.encrypted_password = None

    def get_password(self) -> Optional[str]:
        """
        Descifra y retorna la contraseña del Tenant Admin en memoria RAM.
        """
        if self.encrypted_password:
            return decrypt_data(self.encrypted_password)
        return None

    def set_tokens(self, token: Optional[str], refresh_token: Optional[str] = None) -> None:
        """
        Cifra y asigna los tokens JWT de ThingsBoard antes de persistir en MongoDB.
        """
        if token is not None:
            self.encrypted_token = encrypt_data(token) if token else None
        if refresh_token is not None:
            self.encrypted_refresh_token = encrypt_data(refresh_token) if refresh_token else None

    def get_token(self) -> Optional[str]:
        """
        Descifra y retorna el token JWT de acceso a ThingsBoard en memoria RAM.
        """
        if self.encrypted_token:
            return decrypt_data(self.encrypted_token)
        return None

    def get_refresh_token(self) -> Optional[str]:
        """
        Descifra y retorna el refresh token de ThingsBoard en memoria RAM.
        """
        if self.encrypted_refresh_token:
            return decrypt_data(self.encrypted_refresh_token)
        return None

    # ==========================================
    # Métodos de Resolución Jerárquica
    # ==========================================

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
