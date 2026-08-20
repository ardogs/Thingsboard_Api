from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie

from core.config import settings
from core.models.tb_server import TBServer
from core.logger import logger

_mongo_client: Optional[AsyncIOMotorClient] = None


async def init_db(custom_client: Optional[AsyncIOMotorClient] = None, database_name: Optional[str] = None):
    """
    Inicializa la conexión con MongoDB e inicializa Beanie ODM con los modelos de documentos registrados.
    Permite inyectar un cliente personalizado (ej. para pruebas con mongomock_motor).
    """
    global _mongo_client
    db_name = database_name or settings.MONGO_DB_NAME

    if custom_client is not None:
        _mongo_client = custom_client
    else:
        logger.info(f"[MongoDB] Conectando a {settings.MONGO_URI} (Base de datos: {db_name})...")
        _mongo_client = AsyncIOMotorClient(settings.MONGO_URI)

    database = _mongo_client[db_name]

    await init_beanie(
        database=database,
        document_models=[
            TBServer
        ]
    )
    logger.info("[MongoDB] Beanie ODM inicializado exitosamente con el modelo TBServer.")


async def close_db():
    """
    Cierra la conexión activa con MongoDB.
    """
    global _mongo_client
    if _mongo_client:
        _mongo_client.close()
        logger.info("[MongoDB] Conexión a MongoDB cerrada.")


def get_mongo_client() -> Optional[AsyncIOMotorClient]:
    """
    Retorna el cliente asíncrono de Motor activo.
    """
    return _mongo_client
