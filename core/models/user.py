from datetime import datetime, timezone
from typing import Optional
from beanie import Document
from pydantic import Field, field_validator


class User(Document):
    """
    Modelo de Documento Beanie para la persistencia y gestión de usuarios reales en MongoDB.
    Almacena identidad, credenciales con hash seguro bcrypt, estado de activación y privilegios.
    """
    username: str = Field(..., description="Nombre de usuario único en el sistema")
    email: Optional[str] = Field(default=None, description="Correo electrónico del usuario")
    hashed_password: Optional[str] = Field(default=None, description="Hash bcrypt de la contraseña del usuario")
    role: str = Field(default="user", description="Rol descriptivo o categoría de usuario (ej: superadmin, admin, user, operator)")
    is_active: bool = Field(default=True, description="Indica si la cuenta de usuario se encuentra activa")
    is_superuser: bool = Field(default=False, description="Flag de superadministrador con acceso global irrestricto")
    must_change_password: bool = Field(default=False, description="Indica si el usuario debe cambiar obligatoriamente su contraseña temporal/de un solo uso en su siguiente inicio de sesión")

    # Auditoría y marcas de tiempo
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
        name = "users"
        indexes = [
            "username",
            "email",
            "is_active"
        ]

    def to_dict_safe(self) -> dict:
        """
        Retorna una representación serializable del usuario excluyendo información sensible como hashed_password.
        """
        return {
            "id": str(self.id),
            "username": self.username,
            "email": self.email,
            "role": self.role,
            "is_active": self.is_active,
            "is_superuser": self.is_superuser,
            "must_change_password": self.must_change_password,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }
