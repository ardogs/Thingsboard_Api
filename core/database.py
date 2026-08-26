import asyncio
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie

from core.config import settings
from core.models.user import User
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.models.audit_log import AuditLog
from core.models.tb_scheduled_task import TBScheduledTask
from core.logger import logger

_mongo_client: Optional[AsyncIOMotorClient] = None
_database_name: Optional[str] = None


async def init_db(custom_client: Optional[AsyncIOMotorClient] = None, database_name: Optional[str] = None):
    """
    Inicializa la conexión con MongoDB e inicializa Beanie ODM con los modelos de documentos registrados (User, TBServer, TBTenant, TBBackup, AuditLog, TBScheduledTask).
    Detecta automáticamente si el cliente actual pertenece a un event loop cerrado o diferente y lo recrea para evitar errores de 'Event loop is closed'.
    Permite inyectar un cliente personalizado (ej. para pruebas con mongomock_motor).
    """
    global _mongo_client, _database_name
    if database_name is not None:
        _database_name = database_name
    
    db_name = _database_name or settings.MONGO_DB_NAME
    current_loop = asyncio.get_running_loop()

    recreate = False
    if custom_client is not None:
        _mongo_client = custom_client
    elif _mongo_client is None:
        recreate = True
    else:
        # Verificar si el cliente existente está vinculado a un event loop cerrado o distinto
        try:
            client_loop = getattr(_mongo_client, "get_io_loop", lambda: None)()
            if client_loop is None or client_loop.is_closed() or client_loop is not current_loop:
                recreate = True
        except Exception:
            recreate = True

    if recreate:
        if _mongo_client is not None:
            try:
                _mongo_client.close()
            except Exception:
                pass
        logger.info(f"[MongoDB] Conectando a {settings.MONGO_URI} (Base de datos: {db_name})...")
        _mongo_client = AsyncIOMotorClient(settings.MONGO_URI)

    database = _mongo_client[db_name]

    await init_beanie(
        database=database,
        document_models=[
            User,
            TBServer,
            TBTenant,
            TBBackup,
            AuditLog,
            TBScheduledTask
        ]
    )
    logger.info("[MongoDB] Beanie ODM inicializado exitosamente con los modelos User, TBServer, TBTenant, TBBackup, AuditLog y TBScheduledTask.")


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
