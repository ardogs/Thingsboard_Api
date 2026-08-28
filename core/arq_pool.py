from typing import Optional
from arq import create_pool
from arq.connections import ArqRedis, RedisSettings
from core.config import settings
from core.logger import logger

_arq_pool: Optional[ArqRedis] = None


async def get_arq_pool() -> ArqRedis:
    """
    Retorna el pool singleton de ArqRedis para encolar tareas asíncronas y consultar estados de trabajos.
    """
    global _arq_pool
    if _arq_pool is None:
        _arq_pool = await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))
        logger.info("[ARQ Pool] Pool de conexiones ArqRedis inicializado exitosamente.")
    return _arq_pool


async def close_arq_pool() -> None:
    """
    Cierra el pool de conexiones ArqRedis durante el ciclo de vida de shutdown de la aplicación.
    """
    global _arq_pool
    if _arq_pool is not None:
        await _arq_pool.close()
        _arq_pool = None
        logger.info("[ARQ Pool] Pool de conexiones ArqRedis cerrado exitosamente.")
