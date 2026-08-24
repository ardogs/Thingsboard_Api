from datetime import datetime, timezone
from typing import Optional, Dict, Any
from beanie import Document
from pydantic import Field


class TBServer(Document):
    """
    Modelo de Documento Beanie para la persistencia y gestión dinámica de servidores ThingsBoard en MongoDB.
    Almacena la configuración de infraestructura de la instancia ThingsBoard (URL, rate limits, proxies).
    Las credenciales y tokens específicos de cada Tenant se administran en el documento TBTenant.
    """
    name: str = Field(..., description="Nombre identificativo del servidor ThingsBoard")
    base_url: str = Field(..., description="URL base de la instancia ThingsBoard (ej: https://tb.midominio.com)")
    description: Optional[str] = Field(default=None, description="Descripción o notas del servidor")
    
    # Metadatos dinámicos y control de consumo IoT a nivel de infraestructura
    rate_limit_rpm: Optional[int] = Field(default=60, description="Límite de peticiones por minuto para este servidor")
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables de infraestructura (proxies, headers, flags, puertos)")

    # Aislamiento por usuario en el API Gateway
    user_id: Optional[str] = Field(default=None, description="Identificador del usuario propietario del servidor en el Gateway")
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
