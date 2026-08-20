from datetime import datetime, timezone
from typing import Optional, Dict, Any
from beanie import Document
from pydantic import Field


class TBServer(Document):
    """
    Modelo de Documento Beanie para la persistencia y gestión dinámica de servidores ThingsBoard en MongoDB.
    Permite almacenar metadatos flexibles específicos por instancia (rate limits, tokens, credenciales, proxies).
    """
    name: str = Field(..., description="Nombre identificativo del servidor ThingsBoard")
    base_url: str = Field(..., description="URL base de la instancia ThingsBoard (ej: https://tb.midominio.com)")
    description: Optional[str] = Field(default=None, description="Descripción o notas del servidor")
    
    # Credenciales y Autenticación
    api_key: Optional[str] = Field(default=None, description="API Key o Device Provisioning Key")
    token: Optional[str] = Field(default=None, description="Token JWT de acceso a ThingsBoard")
    refresh_token: Optional[str] = Field(default=None, description="Refresh Token de ThingsBoard")
    username: Optional[str] = Field(default=None, description="Usuario de ThingsBoard (Tenant / Customer)")
    password: Optional[str] = Field(default=None, description="Contraseña de ThingsBoard")

    # Metadatos dinámicos y control de consumo IoT
    rate_limit_rpm: Optional[int] = Field(default=60, description="Límite de peticiones por minuto para este servidor")
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables sin esquema rígido (proxies, headers, flags)")

    # Aislamiento Multi-Tenant
    user_id: Optional[str] = Field(default=None, description="Identificador del usuario propietario del servidor")
    is_active: bool = Field(default=True, description="Estado activo o inactivo del servidor")

    # Auditoría
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Settings:
        name = "tb_servers"
        indexes = [
            "name",
            "user_id",
            "base_url"
        ]
