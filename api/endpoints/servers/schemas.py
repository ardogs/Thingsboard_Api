from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, model_validator

from core.models.tb_server import InstallationType, SSHAuthMethod


# ==========================================
# DTOs: Nodos de Infraestructura (TBNode / Colección tb_nodes)
# Relación desacoplada análoga a TBServer -> TBTenant
# ==========================================

class NodeCreateRequest(BaseModel):
    """
    Esquema de entrada para registrar un nodo de infraestructura secundario (worker, db, transport)
    vinculado relacionalmente al servidor ThingsBoard padre (Link[TBServer]).
    Aplica validación estricta de coherencia según el método de autenticación SSH seleccionado.
    """
    name: Optional[str] = Field(default=None, description="Nombre o identificador descriptivo del nodo")
    node_role: str = Field(default="worker", description="Rol funcional del nodo: 'worker', 'database', 'transport', 'ui', etc.")
    ssh_host: Optional[str] = Field(default=None, description="Dirección IP o hostname FQDN del nodo secundario")
    ip_address: Optional[str] = Field(default=None, description="Alias de compatibilidad para ssh_host")
    ssh_port: int = Field(
        default=22,
        ge=1,
        le=65535,
        description="Puerto del servicio SSH para administración remota (1-65535)"
    )
    ssh_username: str = Field(
        ...,
        description="Nombre de usuario del sistema operativo para la conexión SSH",
        min_length=1
    )
    ssh_auth_method: SSHAuthMethod = Field(
        default=SSHAuthMethod.PASSWORD,
        description="Método explícito de autenticación SSH: 'password' o 'pem_key'"
    )
    ssh_password: Optional[str] = Field(
        default=None,
        description="Contraseña SSH en texto plano (será cifrada en bytes con Fernet)"
    )
    ssh_pem_file: Optional[str] = Field(
        default=None,
        description="Contenido completo de la llave privada PEM/RSA (será cifrada en bytes con Fernet)"
    )
    ssh_passphrase: Optional[str] = Field(
        default=None,
        description="Passphrase opcional para la llave PEM (será cifrada en bytes con Fernet)"
    )
    description: Optional[str] = Field(
        default=None,
        description="Descripción o propósito específico del nodo dentro de la infraestructura"
    )
    is_active: bool = Field(
        default=True,
        description="Estado operativo del nodo (activo/inactivo)"
    )

    @model_validator(mode="after")
    def validate_node_ssh_auth(self) -> "NodeCreateRequest":
        # Sincronizar ssh_host e ip_address
        if not self.ssh_host and self.ip_address:
            self.ssh_host = self.ip_address
        elif not self.ssh_host and not self.ip_address:
            raise ValueError("El campo 'ssh_host' (o 'ip_address') es estrictamente obligatorio.")
        if not self.ip_address:
            self.ip_address = self.ssh_host

        # Validación estricta según el método de autenticación SSH
        if self.ssh_auth_method == SSHAuthMethod.PASSWORD:
            if not self.ssh_password or not self.ssh_password.strip():
                raise ValueError("Para el método de autenticación 'password', el campo 'ssh_password' es estrictamente obligatorio.")
            if self.ssh_pem_file or self.ssh_passphrase:
                raise ValueError("Incompatibilidad de autenticación: los campos 'ssh_pem_file' y 'ssh_passphrase' no están permitidos cuando el método es 'password'.")
        elif self.ssh_auth_method == SSHAuthMethod.PEM_KEY:
            if not self.ssh_pem_file or not self.ssh_pem_file.strip():
                raise ValueError("Para el método de autenticación 'pem_key', el archivo 'ssh_pem_file' es estrictamente obligatorio.")
            if self.ssh_password:
                raise ValueError("Incompatibilidad de autenticación: el campo 'ssh_password' no está permitido cuando el método es 'pem_key'.")

        return self


class NodeUpdateRequest(BaseModel):
    """
    Esquema de entrada para la actualización parcial de un nodo existente en tb_nodes.
    """
    name: Optional[str] = None
    node_role: Optional[str] = None
    ssh_host: Optional[str] = None
    ip_address: Optional[str] = None
    ssh_port: Optional[int] = Field(default=None, ge=1, le=65535)
    ssh_username: Optional[str] = None
    ssh_auth_method: Optional[SSHAuthMethod] = None
    ssh_password: Optional[str] = None
    ssh_pem_file: Optional[str] = None
    ssh_passphrase: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None

    @model_validator(mode="after")
    def validate_node_update_ssh_auth(self) -> "NodeUpdateRequest":
        if not self.ssh_host and self.ip_address:
            self.ssh_host = self.ip_address
        elif self.ssh_host and not self.ip_address:
            self.ip_address = self.ssh_host

        if self.ssh_auth_method == SSHAuthMethod.PASSWORD:
            if self.ssh_pem_file or self.ssh_passphrase:
                raise ValueError("Incompatibilidad de autenticación: los campos 'ssh_pem_file' y 'ssh_passphrase' no están permitidos cuando el método es 'password'.")
        elif self.ssh_auth_method == SSHAuthMethod.PEM_KEY:
            if self.ssh_password:
                raise ValueError("Incompatibilidad de autenticación: el campo 'ssh_password' no está permitido cuando el método es 'pem_key'.")
        elif self.ssh_auth_method is None:
            if self.ssh_password and (self.ssh_pem_file or self.ssh_passphrase):
                raise ValueError("Incompatibilidad de autenticación: no se pueden especificar 'ssh_password' y campos PEM ('ssh_pem_file' / 'ssh_passphrase') simultáneamente.")

        return self


class NodeResponse(BaseModel):
    """
    Esquema de salida seguro para un nodo secundario ThingsBoard.
    Protege los secretos en reposo sin filtrar contraseñas ni archivos PEM en texto plano.
    """
    id: str = Field(..., description="ID de MongoDB del nodo (ObjectId)")
    server_id: str = Field(..., description="ID del servidor ThingsBoard padre al que pertenece el nodo")
    name: Optional[str] = Field(default=None, description="Nombre descriptivo del nodo")
    node_role: str = Field(default="worker", description="Rol funcional del nodo ('worker', 'database', etc.)")
    ssh_host: str = Field(..., description="Dirección IP o hostname del nodo")
    ip_address: str = Field(..., description="Dirección IP o hostname del nodo (compatibilidad)")
    ssh_port: int = Field(..., description="Puerto SSH configurado")
    ssh_username: str = Field(..., description="Usuario SSH del nodo")
    ssh_auth_method: SSHAuthMethod = Field(default=SSHAuthMethod.PASSWORD, description="Método de autenticación SSH configurado")
    has_ssh_password: bool = Field(..., description="Indica si el nodo tiene una contraseña SSH configurada y cifrada")
    has_ssh_pem_file: bool = Field(..., description="Indica si el nodo tiene una llave privada SSH PEM/RSA configurada y cifrada")
    has_ssh_passphrase: bool = Field(..., description="Indica si la llave PEM tiene una passphrase configurada y cifrada")
    description: Optional[str] = Field(default=None, description="Descripción del nodo")
    is_active: bool = Field(..., description="Estado operativo del nodo")
    created_at: datetime = Field(..., description="Fecha de creación del registro en UTC")
    updated_at: datetime = Field(..., description="Fecha de última actualización en UTC")


# ==========================================
# DTOs: Servidores ThingsBoard (TBServer / Colección tb_servers)
# Servidor anfitrión principal (Master / Standalone)
# ==========================================

class ServerCreateRequest(BaseModel):
    """
    Esquema para registrar un servidor ThingsBoard anfitrión principal (Master/Standalone),
    con soporte para administración directa por SSH.
    """
    name: str = Field(..., description="Nombre identificativo del servidor ThingsBoard")
    base_url: str = Field(..., description="URL base (ej: https://thingsboard.cloud)")
    installation_type: InstallationType = Field(
        default=InstallationType.STANDALONE,
        description="Tipo de instalación de ThingsBoard: 'standalone' o 'cluster'"
    )

    # Credenciales Sysadmin ThingsBoard
    username: Optional[str] = Field(default=None, description="Usuario / Email del Sysadmin en ThingsBoard")
    password: Optional[str] = Field(default=None, description="Contraseña del Sysadmin en ThingsBoard")
    token: Optional[str] = Field(default=None, description="Token JWT de acceso a ThingsBoard (opcional)")
    refresh_token: Optional[str] = Field(default=None, description="Refresh Token de ThingsBoard (opcional)")

    # Administración SSH del Servidor Anfitrión Principal (Master / Standalone)
    ssh_port: int = Field(default=22, ge=1, le=65535, description="Puerto SSH del servidor principal (default: 22)")
    ssh_username: Optional[str] = Field(default=None, description="Usuario SSH del servidor principal")
    ssh_auth_method: Optional[SSHAuthMethod] = Field(
        default=None,
        description="Método de autenticación SSH del servidor principal: 'password' o 'pem_key'"
    )
    ssh_password: Optional[str] = Field(default=None, description="Contraseña SSH en texto plano (será cifrada en bytes con Fernet)")
    ssh_pem_file: Optional[str] = Field(default=None, description="Contenido de la llave PEM en texto plano (será cifrada en bytes con Fernet)")
    ssh_passphrase: Optional[str] = Field(default=None, description="Passphrase opcional para la llave PEM (será cifrada en bytes con Fernet)")

    description: Optional[str] = None
    rate_limit_rpm: Optional[int] = Field(default=60, description="Límite de peticiones por minuto")
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables (proxies, headers, flags)")

    @model_validator(mode="after")
    def validate_server_ssh_auth(self) -> "ServerCreateRequest":
        # Validación estricta de autenticación SSH del servidor principal
        if self.ssh_auth_method == SSHAuthMethod.PASSWORD:
            if not self.ssh_password or not self.ssh_password.strip():
                raise ValueError("Para el método de autenticación 'password' en el nodo principal, el campo 'ssh_password' es estrictamente obligatorio.")
            if self.ssh_pem_file or self.ssh_passphrase:
                raise ValueError("Incompatibilidad de autenticación en nodo principal: 'ssh_pem_file' y 'ssh_passphrase' no están permitidos cuando ssh_auth_method es 'password'.")
        elif self.ssh_auth_method == SSHAuthMethod.PEM_KEY:
            if not self.ssh_pem_file or not self.ssh_pem_file.strip():
                raise ValueError("Para el método de autenticación 'pem_key' en el nodo principal, el archivo 'ssh_pem_file' es estrictamente obligatorio.")
            if self.ssh_password:
                raise ValueError("Incompatibilidad de autenticación en nodo principal: 'ssh_password' no está permitido cuando ssh_auth_method es 'pem_key'.")
        elif self.ssh_auth_method is None:
            if self.ssh_password and (self.ssh_pem_file or self.ssh_passphrase):
                raise ValueError("Incompatibilidad de autenticación en nodo principal: no se pueden especificar 'ssh_password' y campos PEM simultáneamente sin definir un método.")

        return self


class ServerUpdateRequest(BaseModel):
    """
    Esquema de actualización parcial de un servidor ThingsBoard principal.
    """
    name: Optional[str] = None
    base_url: Optional[str] = None
    installation_type: Optional[InstallationType] = None
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    refresh_token: Optional[str] = None

    # Nodo Principal SSH
    ssh_port: Optional[int] = Field(default=None, ge=1, le=65535)
    ssh_username: Optional[str] = None
    ssh_auth_method: Optional[SSHAuthMethod] = None
    ssh_password: Optional[str] = None
    ssh_pem_file: Optional[str] = None
    ssh_passphrase: Optional[str] = None

    description: Optional[str] = None
    rate_limit_rpm: Optional[int] = None
    custom_metadata: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None

    @model_validator(mode="after")
    def validate_server_update_ssh_auth(self) -> "ServerUpdateRequest":
        if self.ssh_auth_method == SSHAuthMethod.PASSWORD:
            if self.ssh_pem_file or self.ssh_passphrase:
                raise ValueError("Incompatibilidad de autenticación en nodo principal: 'ssh_pem_file' y 'ssh_passphrase' no están permitidos cuando ssh_auth_method es 'password'.")
        elif self.ssh_auth_method == SSHAuthMethod.PEM_KEY:
            if self.ssh_password:
                raise ValueError("Incompatibilidad de autenticación en nodo principal: 'ssh_password' no está permitido cuando ssh_auth_method es 'pem_key'.")
        elif self.ssh_auth_method is None:
            if self.ssh_password and (self.ssh_pem_file or self.ssh_passphrase):
                raise ValueError("Incompatibilidad de autenticación en nodo principal: no se pueden actualizar 'ssh_password' y campos PEM simultáneamente.")

        return self


class ServerResponse(BaseModel):
    """
    Esquema de salida completo y seguro para un servidor ThingsBoard anfitrión.
    Protege los secretos en reposo sin filtrar contraseñas ni llaves en texto plano.
    """
    id: str
    name: str
    base_url: str
    installation_type: InstallationType = InstallationType.STANDALONE
    username: Optional[str] = None
    has_token: bool
    has_credentials: bool
    token: Optional[str] = None
    refresh_token: Optional[str] = None

    # Flags seguros del nodo principal SSH
    ssh_host: str
    ssh_port: int = 22
    ssh_username: Optional[str] = None
    ssh_auth_method: Optional[SSHAuthMethod] = None
    has_ssh_password: bool = False
    has_ssh_pem_file: bool = False
    has_ssh_passphrase: bool = False

    description: Optional[str] = None
    rate_limit_rpm: int
    custom_metadata: Dict[str, Any]
    user_id: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class ServerStatusResponse(BaseModel):
    server_id: str = Field(..., description="ID del servidor ThingsBoard")
    is_busy: bool = Field(..., description="True si el servidor está procesando un respaldo (lock activo), False si está disponible")


class SystemInfoMetricPoint(BaseModel):
    timestamp: str = Field(..., description="Marca de tiempo ISO de la medición")
    status: str = Field(default="HEALTHY", description="Estado: HEALTHY, ERROR")
    cpu_usage: Optional[float] = Field(default=None, description="Uso de CPU (%)")
    memory_usage: Optional[float] = Field(default=None, description="Uso de memoria RAM (%)")
    disc_usage: Optional[float] = Field(default=None, description="Uso de disco (%)")
    total_memory: Optional[int] = Field(default=None, description="Memoria total en bytes")
    free_memory: Optional[int] = Field(default=None, description="Memoria libre en bytes")
    total_disc: Optional[int] = Field(default=None, description="Disco total en bytes")
    free_disc: Optional[int] = Field(default=None, description="Disco libre en bytes")
    error: Optional[str] = Field(default=None, description="Mensaje de error si falló")


class ServerSystemInfoResponse(BaseModel):
    server_id: str = Field(..., description="ID del servidor ThingsBoard")
    server_name: str = Field(..., description="Nombre del servidor")
    base_url: str = Field(..., description="URL base de la instancia")
    status: str = Field(..., description="Estado de la recolección: HEALTHY, ERROR, UNCOLLECTED")
    collected_at: Optional[str] = Field(default=None, description="Marca de tiempo ISO de la última recolección")
    cpu_usage: Optional[float] = Field(default=None, description="Porcentaje de uso de CPU (%)")
    memory_usage: Optional[float] = Field(default=None, description="Porcentaje de uso de memoria RAM (%)")
    disc_usage: Optional[float] = Field(default=None, description="Porcentaje de uso de disco (%)")
    history_count: int = Field(default=0, description="Cantidad de registros acumulados en el buffer deslizante (hasta 60)")
    history: List[SystemInfoMetricPoint] = Field(default_factory=list, description="Últimos 60 registros cronológicos para gráficas de 1 hora")
    raw_data: Optional[Any] = Field(default=None, description="Información detallada completa devuelta por ThingsBoard (dict o list)")
    error: Optional[str] = Field(default=None, description="Mensaje de error si la recolección falló")


# ==========================================
# DTOs: Tenants de ThingsBoard
# ==========================================

class TenantCreateRequest(BaseModel):
    name: str = Field(..., description="Nombre del tenant (ej: CONAFOR, CFE, Bajio_Norte)")
    username: Optional[str] = Field(default=None, description="Usuario / Email del Tenant Admin en ThingsBoard")
    password: Optional[str] = Field(default=None, description="Contraseña del Tenant Admin en ThingsBoard")
    token: Optional[str] = Field(default=None, description="Token JWT de acceso a ThingsBoard (opcional)")
    refresh_token: Optional[str] = Field(default=None, description="Refresh Token de ThingsBoard (opcional)")
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables específicos del tenant")


class TenantUpdateRequest(BaseModel):
    name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    custom_metadata: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class ReportConfigRequest(BaseModel):
    report_config: Dict[str, List[str]] = Field(
        ...,
        description="Configuración de whitelist de telemetría por dispositivo o default",
        json_schema_extra={
            "example": {
                "default": ["temperature", "humidity"],
                "S1_TH_019": ["temperature"]
            }
        }
    )


class TenantResponse(BaseModel):
    id: str
    server_id: str
    name: str
    username: Optional[str] = None
    has_token: bool
    has_credentials: bool
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    custom_metadata: Dict[str, Any]
    user_id: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime


# ==========================================
# DTOs: Ejecución SSH Remota
# ==========================================

class SSHExecuteRequest(BaseModel):
    """
    Esquema de solicitud para ejecutar un comando SSH autorizado en el servidor ThingsBoard.
    El comando está restringido a una lista blanca estricta con filtrado de metacaracteres.
    """
    command: str = Field(
        ...,
        min_length=1,
        description="Comando SSH a ejecutar perteneciente a la lista blanca permitida"
    )
    timeout_seconds: int = Field(
        default=30,
        ge=1,
        le=300,
        description="Tiempo máximo de espera para la ejecución del comando en segundos"
    )


class SSHExecuteResponse(BaseModel):
    """
    Esquema de respuesta tras la ejecución remota de un comando SSH en el servidor.
    Contiene el código de salida, salidas estándar (stdout/stderr) y telemetría de ejecución.
    """
    server_id: str
    server_name: str
    host: str
    command: str
    exit_status: int
    stdout: str
    stderr: str
    executed_at: datetime
    duration_ms: float

