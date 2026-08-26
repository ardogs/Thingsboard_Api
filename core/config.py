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

    # Configuración del motor de Pydantic Settings v2
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()