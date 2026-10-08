from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any
from beanie import Document
from pydantic import Field, field_validator

from core.crypto import encrypt_data, decrypt_data


class InstallationType(str, Enum):
    """
    Tipo de instalación/despliegue del servidor ThingsBoard.
    - standalone: Instancia única monolítica (nodo único Master/Standalone)
    - cluster: Arquitectura distribuida con múltiples nodos (Master + Workers/Databases independientes)
    """
    STANDALONE = "standalone"
    CLUSTER = "cluster"


class SSHAuthMethod(str, Enum):
    """
    Método explícito de autenticación SSH remota.
    - password: Autenticación clásica mediante contraseña de usuario
    - pem_key: Autenticación mediante llave privada PEM/RSA (con passphrase opcional)
    """
    PASSWORD = "password"
    PEM_KEY = "pem_key"


class TBServer(Document):
    """
    Modelo de Documento Beanie para la persistencia y gestión de servidores ThingsBoard en MongoDB.
    Representa el servidor físico/VM anfitrión principal (Master en topología de cluster o
    servidor monolítico único en Standalone).
    
    Almacena la configuración de infraestructura, credenciales de Sysadmin ThingsBoard,
    así como la administración directa por SSH del nodo anfitrión principal.
    Los nodos secundarios del cluster se gestionan de forma desacoplada y relacional mediante TBNode (Link[TBServer]).
    """
    name: str = Field(..., description="Nombre identificativo del servidor ThingsBoard")
    base_url: str = Field(..., description="URL base de la instancia ThingsBoard (ej: https://tb.midominio.com)")
    description: Optional[str] = Field(default=None, description="Descripción o notas del servidor")
    
    installation_type: InstallationType = Field(
        default=InstallationType.STANDALONE,
        description="Tipo de instalación de ThingsBoard: 'standalone' o 'cluster'"
    )
    
    # Credenciales del Sysadmin en ThingsBoard (username en texto plano, contraseña cifrada)
    username: Optional[str] = Field(default=None, description="Usuario / Email del Sysadmin en ThingsBoard")
    encrypted_password: Optional[str] = Field(default=None, description="Contraseña cifrada (Fernet) del Sysadmin en ThingsBoard")

    # Tokens JWT de sesión de Sysadmin con ThingsBoard cifrados
    encrypted_token: Optional[str] = Field(default=None, description="Token JWT de acceso cifrado (Fernet) del Sysadmin a ThingsBoard")
    encrypted_refresh_token: Optional[str] = Field(default=None, description="Refresh Token cifrado (Fernet) del Sysadmin de ThingsBoard")

    # ==========================================
    # Administración SSH del Servidor Anfitrión Principal (Master / Standalone)
    # ==========================================
    ssh_port: int = Field(default=22, description="Puerto SSH del servidor principal (default: 22)")
    ssh_username: Optional[str] = Field(default=None, description="Usuario del sistema operativo para conexión SSH")
    ssh_auth_method: Optional[SSHAuthMethod] = Field(
        default=None,
        description="Método de autenticación SSH del servidor principal: 'password' o 'pem_key'"
    )
    encrypted_ssh_password: Optional[bytes] = Field(
        default=None,
        description="Contraseña SSH cifrada (Fernet en bytes) del servidor principal"
    )
    encrypted_ssh_pem_file: Optional[bytes] = Field(
        default=None,
        description="Archivo PEM/RSA cifrado (Fernet en bytes) del servidor principal"
    )
    encrypted_ssh_passphrase: Optional[bytes] = Field(
        default=None,
        description="Passphrase opcional de la llave PEM cifrada (Fernet en bytes) del servidor principal"
    )

    # Metadatos dinámicos y control de consumo IoT a nivel de infraestructura
    rate_limit_rpm: Optional[int] = Field(default=60, description="Límite de peticiones por minuto para este servidor")
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables de infraestructura (proxies, headers, flags, puertos)")

    # Aislamiento por usuario en el API Gateway
    user_id: Optional[str] = Field(default=None, description="Identificador del usuario propietario del servidor en el Gateway")
    is_active: bool = Field(default=True, description="Estado activo o inactivo del servidor")

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
        name = "tb_servers"
        indexes = [
            "name",
            "user_id",
            "base_url",
            "installation_type"
        ]

    @property
    def ssh_host(self) -> str:
        """Deriva automáticamente el host o IP para SSH a partir de base_url."""
        from urllib.parse import urlparse
        parsed = urlparse(self.base_url)
        return parsed.hostname or self.base_url

    # ==========================================
    # Getters y Setters Seguros: Sysadmin ThingsBoard
    # ==========================================

    def set_password(self, plain_password: Optional[str]) -> None:
        """Cifra y asigna la contraseña del Sysadmin antes de persistir en MongoDB."""
        if plain_password:
            self.encrypted_password = encrypt_data(plain_password)
        else:
            self.encrypted_password = None

    def get_password(self) -> Optional[str]:
        """Descifra y retorna la contraseña del Sysadmin en memoria RAM."""
        if self.encrypted_password:
            return decrypt_data(self.encrypted_password)
        return None

    def set_tokens(self, token: Optional[str], refresh_token: Optional[str] = None) -> None:
        """Cifra y asigna los tokens JWT de ThingsBoard antes de persistir en MongoDB."""
        if token is not None:
            self.encrypted_token = encrypt_data(token) if token else None
        if refresh_token is not None:
            self.encrypted_refresh_token = encrypt_data(refresh_token) if refresh_token else None

    def get_token(self) -> Optional[str]:
        """Descifra y retorna el token JWT de acceso a ThingsBoard en memoria RAM."""
        if self.encrypted_token:
            return decrypt_data(self.encrypted_token)
        return None

    def get_refresh_token(self) -> Optional[str]:
        """Descifra y retorna el refresh token de ThingsBoard en memoria RAM."""
        if self.encrypted_refresh_token:
            return decrypt_data(self.encrypted_refresh_token)
        return None

    # ==========================================
    # Getters y Setters Seguros: SSH Servidor Principal (RAM Decryption)
    # ==========================================

    def set_ssh_password(self, plain_password: Optional[str]) -> None:
        """Cifra y asigna la contraseña SSH en formato bytes antes de persistir en MongoDB."""
        if plain_password:
            enc = encrypt_data(plain_password)
            self.encrypted_ssh_password = enc.encode("utf-8") if enc else None
        else:
            self.encrypted_ssh_password = None

    def get_ssh_password(self) -> Optional[str]:
        """Descifra y retorna la contraseña SSH del servidor principal en memoria RAM."""
        if self.encrypted_ssh_password:
            return decrypt_data(self.encrypted_ssh_password)
        return None

    def set_ssh_pem_file(self, plain_pem: Optional[str]) -> None:
        """Cifra y asigna el archivo PEM/RSA en formato bytes antes de persistir en MongoDB."""
        if plain_pem:
            enc = encrypt_data(plain_pem)
            self.encrypted_ssh_pem_file = enc.encode("utf-8") if enc else None
        else:
            self.encrypted_ssh_pem_file = None

    def get_ssh_pem_file(self) -> Optional[str]:
        """Descifra y retorna la llave PEM/RSA del servidor principal en memoria RAM."""
        if self.encrypted_ssh_pem_file:
            return decrypt_data(self.encrypted_ssh_pem_file)
        return None

    def set_ssh_passphrase(self, plain_passphrase: Optional[str]) -> None:
        """Cifra y asigna la passphrase de la llave PEM en formato bytes."""
        if plain_passphrase:
            enc = encrypt_data(plain_passphrase)
            self.encrypted_ssh_passphrase = enc.encode("utf-8") if enc else None
        else:
            self.encrypted_ssh_passphrase = None

    def get_ssh_passphrase(self) -> Optional[str]:
        """Descifra y retorna la passphrase del servidor principal en memoria RAM."""
        if self.encrypted_ssh_passphrase:
            return decrypt_data(self.encrypted_ssh_passphrase)
        return None

    def set_ssh_credentials(
        self,
        ssh_password: Optional[str] = None,
        ssh_pem_file: Optional[str] = None,
        ssh_passphrase: Optional[str] = None
    ) -> None:
        """
        Asigna de forma atómica y sanitizada las credenciales SSH del servidor principal,
        limpiando campos incompatibles según el método seleccionado.
        """
        if self.ssh_auth_method == SSHAuthMethod.PASSWORD:
            self.set_ssh_password(ssh_password)
            self.encrypted_ssh_pem_file = None
            self.encrypted_ssh_passphrase = None
        elif self.ssh_auth_method == SSHAuthMethod.PEM_KEY:
            self.set_ssh_pem_file(ssh_pem_file)
            self.set_ssh_passphrase(ssh_passphrase)
            self.encrypted_ssh_password = None
        else:
            if ssh_password:
                self.set_ssh_password(ssh_password)
            if ssh_pem_file:
                self.set_ssh_pem_file(ssh_pem_file)
            if ssh_passphrase:
                self.set_ssh_passphrase(ssh_passphrase)
