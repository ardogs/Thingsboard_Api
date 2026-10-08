from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuración centralizada de la aplicación con validación estricta de variables de entorno.
    Utiliza Pydantic Settings v2 para forzar la inyección de todas las variables desde el archivo .env
    o desde el entorno del sistema operativo, sin ningún valor por defecto en el código fuente.
    """
    # --------------------------------------------------------------------------
    # Parámetros Generales de la Aplicación
    # --------------------------------------------------------------------------
    PROJECT_NAME: str = Field(..., description="Nombre del proyecto y API Gateway")
    DEBUG: bool = Field(..., description="Flag de modo depuración")
    APP_TIMEZONE: str = Field(default="America/Mexico_City", description="Zona horaria de referencia para la aplicación y expresiones cron")
    BACKUP_DIR: str = Field(default="backups", description="Directorio base para almacenamiento de respaldos ZIP y temporales")
    MAX_CONCURRENT_ZIP_PACKAGING: int = Field(default=3, description="Límite máximo de empaquetados ZIP concurrentes para no saturar disco/CPU")
    MAX_CONCURRENT_IO_OPERATIONS: int = Field(default=10, description="Límite máximo de operaciones pesadas concurrentes de I/O en disco")
    CORS_ORIGINS: list[str] = Field(default=["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:8000"], description="Lista de orígenes permitidos por CORS")

    # --------------------------------------------------------------------------
    # Base de Datos MongoDB
    # --------------------------------------------------------------------------
    MONGO_URI: str = Field(..., description="URI de conexión a MongoDB")
    MONGO_DB_NAME: str = Field(..., description="Nombre de la base de datos de MongoDB")

    # --------------------------------------------------------------------------
    # Broker y Caché Redis
    # --------------------------------------------------------------------------
    REDIS_URL: str = Field(..., description="URL de conexión al servicio Redis")

    # --------------------------------------------------------------------------
    # Parámetros de Seguridad JWT y Criptografía
    # --------------------------------------------------------------------------
    SECRET_KEY: str = Field(..., description="Clave secreta criptográfica para la firma y verificación de tokens JWT")
    ALGORITHM: str = Field(..., description="Algoritmo de firma para tokens JWT (ej: HS256)")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(..., description="Tiempo de expiración del access token en minutos")
    ENCRYPTION_KEY: str = Field(..., description="Clave de cifrado para datos sensibles en reposo")
    COOKIE_SECURE: bool = Field(default=False, description="Flag Secure para cookies HttpOnly (HTTPS)")

    # --------------------------------------------------------------------------
    # Bootstrapping y Superadministrador Inicial
    # --------------------------------------------------------------------------
    FIRST_SUPERUSER_USERNAME: str = Field(..., description="Nombre de usuario del superadministrador inicial")
    FIRST_SUPERUSER_EMAIL: str = Field(..., description="Correo electrónico del superadministrador inicial")
    FIRST_SUPERUSER_PASSWORD: str = Field(..., description="Contraseña de arranque para el superadministrador inicial")

    # --------------------------------------------------------------------------
    # Configuración de PyCasbin (Rutas de configuración y colección)
    # --------------------------------------------------------------------------
    CASBIN_MODEL_PATH: str = Field(..., description="Ruta al modelo RBAC de Casbin")
    CASBIN_COLLECTION_NAME: str = Field(..., description="Nombre de la colección para las reglas de Casbin")


    # --------------------------------------------------------------------------
    # Notificaciones y Alertas por Telegram
    # --------------------------------------------------------------------------
    TG_BOT_TOKEN: Optional[str] = Field(default=None, description="Token del bot de Telegram para alertas")
    TG_CHAT_ID: Optional[str] = Field(default=None, description="Chat ID de Telegram para alertas")

    # Configuración del motor de Pydantic Settings v2
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()