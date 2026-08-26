import asyncio
from typing import Optional, Any
import redis.asyncio as redis
from core.config import settings
from core.logger import logger


class DynamicRedisClient:
    """
    Cliente Redis asíncrono dinámico y consciente del ciclo de vida del Event Loop.
    Detecta automáticamente si el event loop actual ha sido cerrado o recreado
    (ej: múltiples ejecuciones consecutivas de asyncio.run() en Celery Workers o reinicios)
    y reinicializa el cliente y su pool de conexiones de forma transparente para evitar errores de 'Event loop is closed'.
    """
    def __init__(self, url: str):
        self._url = url
        self._client: Optional[redis.Redis] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def get_client(self) -> redis.Redis:
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if (
            self._client is None
            or self._loop is None
            or self._loop.is_closed()
            or (current_loop is not None and self._loop is not current_loop)
        ):
            self._loop = current_loop
            self._client = redis.from_url(self._url, encoding="utf-8", decode_responses=True)

        return self._client

    def __getattr__(self, name: str) -> Any:
        client = self.get_client()
        return getattr(client, name)


redis_client = DynamicRedisClient(settings.REDIS_URL)


def get_redis_client() -> Any:
    """
    Retorna la instancia activa de redis.asyncio.Redis vinculada al event loop en ejecución.
    Si redis_client ha sido reemplazado o mockeado, retorna el objeto activo directamente.
    """
    if hasattr(redis_client, "get_client"):
        return redis_client.get_client()
    return redis_client