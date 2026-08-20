from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "ThingsBoard Super API Gateway"
    REDIS_URL: str = "redis://localhost:6379/0"

    # Base de Datos MongoDB
    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB_NAME: str = "tb_super_api"

    # Parámetros de Seguridad JWT
    SECRET_KEY: str = "super-secret-key-change-in-production-thingsboard-2026"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440

    class Config:
        env_file = ".env"

settings = Settings()