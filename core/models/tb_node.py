from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from beanie import Document, Link
from pydantic import Field

from core.models.tb_server import TBServer, SSHAuthMethod
from core.crypto import encrypt_data, decrypt_data


class TBNode(Document):
    """
    Modelo de Documento Beanie para la persistencia y gestión de nodos de infraestructura ThingsBoard
    secundarios vinculados a un TBServer padre (Link[TBServer]), análogo a la relación TBServer -> TBTenant.
    
    Almacena el host, puerto SSH, usuario SSH, método de autenticación estricta (password o pem_key),
    así como las credenciales cifradas con Fernet en formato bytes en reposo (MongoDB).
    """
    server_id: Link[TBServer] = Field(..., description="Referencia/Link al servidor ThingsBoard padre")
    name: Optional[str] = Field(default=None, description="Nombre o identificador descriptivo del nodo")
    node_role: str = Field(default="worker", description="Rol funcional del nodo: 'worker', 'database', 'transport', 'ui', etc.")
    
    ssh_host: str = Field(..., description="Dirección IP o FQDN del nodo para conexión SSH")
    ssh_port: int = Field(default=22, description="Puerto SSH para administración remota (1-65535)")
    ssh_username: str = Field(..., description="Usuario para autenticación SSH en el nodo")
    ssh_auth_method: SSHAuthMethod = Field(
        default=SSHAuthMethod.PASSWORD,
        description="Método de autenticación SSH: 'password' o 'pem_key'"
    )

    # Credenciales SSH cifradas en reposo con Fernet (bytes)
    encrypted_ssh_password: Optional[bytes] = Field(
        default=None,
        description="Contraseña SSH cifrada (Fernet en bytes) en MongoDB"
    )
    encrypted_ssh_pem_file: Optional[bytes] = Field(
        default=None,
        description="Contenido de llave privada PEM/RSA cifrada (Fernet en bytes) en MongoDB"
    )
    encrypted_ssh_passphrase: Optional[bytes] = Field(
        default=None,
        description="Passphrase opcional para llave PEM cifrada (Fernet en bytes) en MongoDB"
    )

    # Metadatos auxiliares de infraestructura
    description: Optional[str] = Field(default=None, description="Descripción o propósito del nodo")
    is_active: bool = Field(default=True, description="Estado operativo del nodo")

    # Auditoría
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Settings:
        name = "tb_nodes"
        indexes = [
            "server_id",
            "ssh_host",
            "node_role"
        ]

    # Propiedad de compatibilidad con ip_address
    @property
    def ip_address(self) -> str:
        return self.ssh_host

    @ip_address.setter
    def ip_address(self, value: str) -> None:
        self.ssh_host = value

    # ==========================================
    # Getters y Setters Seguros (Fernet RAM Decryption)
    # ==========================================

    def set_ssh_password(self, plain_password: Optional[str]) -> None:
        """Cifra y asigna la contraseña SSH en formato bytes antes de persistir en MongoDB."""
        if plain_password:
            enc = encrypt_data(plain_password)
            self.encrypted_ssh_password = enc.encode("utf-8") if enc else None
        else:
            self.encrypted_ssh_password = None

    def get_ssh_password(self) -> Optional[str]:
        """Descifra y retorna la contraseña SSH en memoria RAM."""
        if self.encrypted_ssh_password:
            return decrypt_data(self.encrypted_ssh_password)
        return None

    def set_ssh_pem_file(self, plain_pem: Optional[str]) -> None:
        """Cifra y asigna la llave privada PEM/RSA en formato bytes antes de persistir en MongoDB."""
        if plain_pem:
            enc = encrypt_data(plain_pem)
            self.encrypted_ssh_pem_file = enc.encode("utf-8") if enc else None
        else:
            self.encrypted_ssh_pem_file = None

    def get_ssh_pem_file(self) -> Optional[str]:
        """Descifra y retorna la llave privada PEM/RSA en memoria RAM."""
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
        """Descifra y retorna la passphrase en memoria RAM."""
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
        Asigna y sanitiza de forma coherente las credenciales según ssh_auth_method.
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

    # ==========================================
    # Métodos de Resolución Jerárquica
    # ==========================================

    async def get_server(self) -> Optional[TBServer]:
        """Resuelve y retorna el documento TBServer padre asociado a este Nodo."""
        if isinstance(self.server_id, TBServer):
            return self.server_id
        ref = self.server_id.to_ref() if hasattr(self.server_id, "to_ref") else self.server_id
        ref_id = ref.id if hasattr(ref, "id") else ref
        try:
            return await TBServer.get(ref_id)
        except Exception:
            return None

    def get_server_id_str(self) -> str:
        """Retorna el ID del servidor padre en formato string."""
        if isinstance(self.server_id, TBServer):
            return str(self.server_id.id)
        ref = self.server_id.to_ref() if hasattr(self.server_id, "to_ref") else self.server_id
        ref_id = ref.id if hasattr(ref, "id") else ref
        return str(ref_id)
