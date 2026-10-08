from datetime import datetime
from typing import Optional, List, Any, Union
from pydantic import BaseModel, Field, field_validator, model_validator


class EmailConfigCreateRequest(BaseModel):
    """Esquema de solicitud para registrar una nueva configuración de servidor SMTP."""
    host: str = Field(..., description="Host o FQDN del servidor SMTP (ej: smtp.gmail.com)", json_schema_extra={"example": "smtp.gmail.com"})
    port: int = Field(default=587, ge=1, le=65535, description="Puerto de conexión SMTP", json_schema_extra={"example": 587})
    username: str = Field(..., description="Usuario o cuenta de correo para autenticación SMTP", json_schema_extra={"example": "notificaciones@empresa.com"})
    password: str = Field(..., description="Contraseña en texto plano (se almacenará cifrada con Fernet en MongoDB)", json_schema_extra={"example": "SuperSecretAppPass2026!"})
    use_tls: bool = Field(default=True, description="Habilitar cifrado TLS / STARTTLS")
    sender_email: Optional[str] = Field(default=None, description="Dirección remitente por defecto (From)", json_schema_extra={"example": "notificaciones@empresa.com"})
    sender_name: Optional[str] = Field(default=None, description="Nombre descriptivo del remitente", json_schema_extra={"example": "ThingsBoard Gateway"})
    is_active: bool = Field(default=True, description="Si es True, se marca como la configuración activa principal")

    @field_validator("host", "username", "password")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        clean = v.strip()
        if not clean:
            raise ValueError("El campo no puede estar vacío ni contener solo espacios en blanco.")
        return clean


class EmailConfigUpdateRequest(BaseModel):
    """Esquema de solicitud para actualizar parcialmente una configuración SMTP existente."""
    host: Optional[str] = Field(default=None, description="Host o FQDN del servidor SMTP")
    port: Optional[int] = Field(default=None, ge=1, le=65535, description="Puerto de conexión SMTP")
    username: Optional[str] = Field(default=None, description="Usuario o correo de autenticación")
    password: Optional[str] = Field(default=None, description="Nueva contraseña (si se suministra, se cifrará con Fernet)")
    use_tls: Optional[bool] = Field(default=None, description="Habilitar cifrado TLS / STARTTLS")
    sender_email: Optional[str] = Field(default=None, description="Dirección remitente por defecto (From)")
    sender_name: Optional[str] = Field(default=None, description="Nombre descriptivo del remitente")
    is_active: Optional[bool] = Field(default=None, description="Estado activo de la configuración")


class EmailConfigResponse(BaseModel):
    """Esquema de respuesta seguro para configuración SMTP (la contraseña nunca se expone en texto plano)."""
    id: str = Field(..., description="ID de MongoDB del documento TBEmailConfig")
    host: str = Field(..., description="Host del servidor SMTP")
    port: int = Field(..., description="Puerto de conexión")
    username: str = Field(..., description="Usuario de autenticación")
    has_password: bool = Field(..., description="Indica si la configuración cuenta con contraseña cifrada almacenada")
    use_tls: bool = Field(..., description="Estado de cifrado TLS/STARTTLS")
    sender_email: Optional[str] = Field(default=None, description="Dirección de remitente por defecto")
    sender_name: Optional[str] = Field(default=None, description="Nombre descriptivo del remitente")
    is_active: bool = Field(..., description="Indica si es la configuración activa por defecto")
    created_at: datetime = Field(..., description="Fecha de creación UTC")
    updated_at: datetime = Field(..., description="Fecha de última modificación UTC")


class EmailConfigTestRequest(BaseModel):
    """Solicitud para enviar un correo de prueba utilizando una configuración SMTP específica."""
    to_email: str = Field(..., description="Dirección de correo destinatario", json_schema_extra={"example": "admin@empresa.com"})
    subject: Optional[str] = Field(default=None, description="Asunto personalizado opcional")
    html_body: Optional[str] = Field(default=None, description="Cuerpo HTML opcional")
    body: Optional[str] = Field(default=None, description="Cuerpo del mensaje (texto plano o HTML)")
    cc: Optional[Union[str, List[str]]] = Field(default=None, description="Con copia (CC)")
    bcc: Optional[Union[str, List[str]]] = Field(default=None, description="Con copia oculta (BCC)")
    from_email: Optional[str] = Field(default=None, description="De* (opcional para sobreescribir la dirección remitente de la configuración)")
    from_name: Optional[str] = Field(default=None, description="Nombre descriptivo del remitente (From Name) opcional", json_schema_extra={"example": "ThingsBoard Gateway"})
    sender_name: Optional[str] = Field(default=None, description="Alias para from_name (Nombre descriptivo del remitente)", json_schema_extra={"example": "ThingsBoard Gateway"})
    attachment_paths: Optional[List[str]] = Field(default=None, description="Rutas opcionales a archivos locales en disco para adjuntar")
    sync: bool = Field(
        default=False,
        description="Si es True, ejecuta el envío de forma síncrona con confirmación inmediata (HTTP 200/502). Si es False, encola en ARQ (HTTP 202)."
    )

    @model_validator(mode="after")
    def sync_sender_name(self):
        val = self.from_name or self.sender_name
        if val:
            self.from_name = val
            self.sender_name = val
        return self

    @field_validator("to_email")
    @classmethod
    def validate_email_format(cls, v: str) -> str:
        clean_v = v.strip().lower()
        if "@" not in clean_v or "." not in clean_v.split("@")[-1] or len(clean_v) < 5:
            raise ValueError(f"El correo electrónico '{v}' no tiene un formato válido.")
        return clean_v

    @field_validator("attachment_paths", mode="before")
    @classmethod
    def sanitize_attachment_paths(cls, v: Any) -> Optional[List[str]]:
        if not v:
            return None
        placeholder_values = {"string", "null", "none", "undefined", "", "{}", "[]"}
        if isinstance(v, list):
            cleaned = [
                str(item).strip() for item in v
                if str(item).strip() and str(item).strip().lower() not in placeholder_values
            ]
            return cleaned if cleaned else None
        if isinstance(v, str):
            clean_str = v.strip()
            if clean_str.lower() in placeholder_values or not clean_str:
                return None
            return [clean_str]
        return v


class TestEmailRequest(BaseModel):
    """Esquema para disparo general de correo de prueba vía worker de ARQ o modo síncrono."""
    to_email: str = Field(
        ...,
        description="Dirección de correo electrónico del destinatario",
        json_schema_extra={"example": "admin@empresa.com"}
    )

    @field_validator("to_email")
    @classmethod
    def validate_email_format(cls, v: str) -> str:
        clean_v = v.strip().lower()
        if "@" not in clean_v or "." not in clean_v.split("@")[-1] or len(clean_v) < 5:
            raise ValueError(f"El correo electrónico '{v}' no tiene un formato válido.")
        return clean_v

    subject: Optional[str] = Field(
        default="Prueba de Configuración SMTP - ThingsBoard Super API Gateway",
        description="Asunto del correo electrónico"
    )
    html_body: Optional[str] = Field(
        default=None,
        description="Cuerpo del mensaje en HTML. Si se omite, se generará una plantilla de diagnóstico."
    )
    text_body: Optional[str] = Field(
        default=None,
        description="Cuerpo del mensaje en texto plano (opcional)."
    )
    body: Optional[str] = Field(
        default=None,
        description="Cuerpo genérico del mensaje (texto plano o HTML)."
    )
    cc: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Con copia (CC): Dirección o lista de destinatarios en copia."
    )
    bcc: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Con copia oculta (BCC): Dirección o lista de destinatarios en copia oculta."
    )
    from_email: Optional[str] = Field(
        default=None,
        description="De*: Remitente opcional para sobreescribir la configuración cargada."
    )
    from_name: Optional[str] = Field(
        default=None,
        description="Nombre descriptivo del remitente (From Name) opcional.",
        json_schema_extra={"example": "ThingsBoard Gateway"}
    )
    sender_name: Optional[str] = Field(
        default=None,
        description="Alias para from_name (Nombre descriptivo del remitente).",
        json_schema_extra={"example": "ThingsBoard Gateway"}
    )
    attachment_paths: Optional[List[str]] = Field(
        default=None,
        description="Lista opcional de rutas a archivos locales en disco para ser adjuntados."
    )
    sync: bool = Field(
        default=False,
        description="Si es True, ejecuta el envío de forma síncrona con confirmación inmediata (HTTP 200/502). Si es False, encola en ARQ (HTTP 202)."
    )

    @model_validator(mode="after")
    def sync_sender_name(self):
        val = self.from_name or self.sender_name
        if val:
            self.from_name = val
            self.sender_name = val
        return self

    @field_validator("attachment_paths", mode="before")
    @classmethod
    def sanitize_attachment_paths(cls, v: Any) -> Optional[List[str]]:
        if not v:
            return None
        placeholder_values = {"string", "null", "none", "undefined", "", "{}", "[]"}
        if isinstance(v, list):
            cleaned = [
                str(item).strip() for item in v
                if str(item).strip() and str(item).strip().lower() not in placeholder_values
            ]
            return cleaned if cleaned else None
        if isinstance(v, str):
            clean_str = v.strip()
            if clean_str.lower() in placeholder_values or not clean_str:
                return None
            return [clean_str]
        return v


class TestEmailResponse(BaseModel):
    """Esquema de respuesta para envío de correos de prueba (síncrono o asíncrono en ARQ)."""
    status: str = Field(..., description="Estado del proceso (ej: ACCEPTED, SUCCESS, FAILED)")
    message: str = Field(..., description="Descripción detallada del resultado")
    task_id: Optional[str] = Field(None, description="Identificador del trabajo en ARQ (si fue asíncrono)")
    to_email: str = Field(..., description="Destinatario objetivo")
    subject: str = Field(..., description="Asunto enviado")
    status_url: Optional[str] = Field(None, description="URL para consultar el estado del trabajo en /api/v1/tasks/{task_id}")
    stream_url: Optional[str] = Field(None, description="URL de SSE para seguir el progreso en tiempo real")
    details: Optional[dict] = Field(None, description="Detalles adicionales del envío o diagnóstico")
