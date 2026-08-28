import redis.asyncio as redis
from core.config import settings

# Global async Redis client using connection pooling
redis_client: redis.Redis = redis.from_url(
    settings.REDIS_URL,
    encoding="utf-8",
    decode_responses=True
)


def get_redis_client() -> redis.Redis:
    """
    Retorna la instancia global del cliente Redis asíncrono con pool de conexiones nativo.
    """
    return redis_client