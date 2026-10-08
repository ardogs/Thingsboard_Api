import asyncio
from datetime import datetime, timezone
from typing import Optional
from pymongo import IndexModel, ASCENDING
from beanie import Document
from pydantic import Field, field_validator

from core.crypto import encrypt_data, decrypt_data


class TBEmailConfig(Document):
    """
    Modelo de Documento Beanie para la persistencia de configuraciones SMTP en MongoDB.
    Almacena host, port, username y encrypted_password con cifrado simétrico Fernet en reposo.
    Garantiza el patrón Singleton: solo puede existir una única configuración en todo el sistema.
    """
    singleton_key: str = Field(
        default="global_smtp_config",
        description="Identificador único para garantizar a nivel de base de datos que solo exista una configuración SMTP"
    )
    host: str = Field(..., description="Host o FQDN del servidor SMTP (ej: smtp.gmail.com)")
    port: int = Field(default=587, ge=1, le=65535, description="Puerto del servidor SMTP")
    username: str = Field(..., description="Usuario o cuenta de correo para autenticación SMTP")
    encrypted_password: Optional[str] = Field(default=None, description="Contraseña cifrada con Fernet")
    use_tls: bool = Field(default=True, description="Flag para habilitar TLS/STARTTLS en la conexión SMTP")
    sender_email: Optional[str] = Field(default=None, description="Dirección remitente por defecto (From)")
    sender_name: Optional[str] = Field(default=None, description="Nombre descriptivo del remitente")
    is_active: bool = Field(default=True, description="Indica si esta configuración se encuentra activa")

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
        name = "tb_email_configs"
        indexes = [
            IndexModel([("singleton_key", ASCENDING)], unique=True),
            "username",
            "is_active"
        ]

    @classmethod
    async def get_singleton(cls) -> Optional["TBEmailConfig"]:
        """Retorna la única configuración SMTP registrada en el sistema, o None si no existe."""
        return await cls.find_one()

    @classmethod
    async def exists_config(cls) -> bool:
        """Verifica si ya existe una configuración SMTP registrada en MongoDB."""
        return (await cls.count()) > 0


    # =========================================================================
    # Métodos Asíncronos de Cifrado y Descifrado (Fernet en RAM)
    # =========================================================================

    async def set_password(self, plain_password: Optional[str]) -> None:
        """
        Cifra y asigna la contraseña en encrypted_password de forma asíncrona.
        Delega el cómputo criptográfico a un hilo secundario para evitar bloqueos del Event Loop.
        """
        if plain_password:
            self.encrypted_password = await asyncio.to_thread(encrypt_data, plain_password)
        else:
            self.encrypted_password = None

    async def get_password(self) -> Optional[str]:
        """
        Descifra y retorna la contraseña en texto plano en memoria RAM de forma asíncrona.
        Delega el descifrado Fernet a un hilo secundario para pureza asíncrona.
        """
        if self.encrypted_password:
            return await asyncio.to_thread(decrypt_data, self.encrypted_password)
        return None

    # =========================================================================
    # Métodos Síncronos de Conveniencia
    # =========================================================================

    def set_password_sync(self, plain_password: Optional[str]) -> None:
        """Versión síncrona para asignación de contraseña."""
        if plain_password:
            self.encrypted_password = encrypt_data(plain_password)
        else:
            self.encrypted_password = None

    def get_password_sync(self) -> Optional[str]:
        """Versión síncrona para descifrado de contraseña en memoria RAM."""
        if self.encrypted_password:
            return decrypt_data(self.encrypted_password)
        return None
