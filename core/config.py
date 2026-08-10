from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "ThingsBoard Super API"
    REDIS_URL: str = "redis://localhost:6379/0"
    TB_BASE_URL: str = "[https://tu-servidor-thingsboard.com](https://tu-servidor-thingsboard.com)"

    class Config:
        env_file = ".env"

settings = Settings()