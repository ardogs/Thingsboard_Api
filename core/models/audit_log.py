from datetime import datetime, timezone
from typing import Optional, Dict, Any
from beanie import Document
from pydantic import Field


class AuditLog(Document):
    """
    Modelo de Documento Beanie para la persistencia del registro de auditoría y trazabilidad DevSecOps.
    Almacena métodos mutantes (POST, PUT, DELETE, PATCH) con payloads sanitizados para proteger credenciales.
    """
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Marca de tiempo UTC del evento")
    user_id: Optional[str] = Field(default=None, description="ID del usuario autenticado en la plataforma (o None si es anónimo/login)")
    ip_address: str = Field(default="127.0.0.1", description="Dirección IP de origen del cliente")
    method: str = Field(..., description="Método HTTP ejecutado (POST, PUT, DELETE, etc.)")
    endpoint: str = Field(..., description="Ruta o URI de la petición solicitada")
    status_code: int = Field(..., description="Código de estado HTTP de la respuesta")
    payload: Optional[Dict[str, Any]] = Field(default=None, description="Cuerpo o parámetros de la petición con campos sensibles enmascarados")

    class Settings:
        name = "audit_logs"
        indexes = [
            "timestamp",
            "user_id",
            "endpoint",
            "method",
            "status_code"
        ]

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "timestamp": self.timestamp.isoformat(),
            "user_id": self.user_id,
            "ip_address": self.ip_address,
            "method": self.method,
            "endpoint": self.endpoint,
            "status_code": self.status_code,
            "payload": self.payload
        }
