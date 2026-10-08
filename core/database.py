import asyncio
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie

from core.config import settings
from core.models.user import User
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.tb_node import TBNode
from core.models.tb_backup import TBBackup
from core.models.audit_log import AuditLog
from core.models.tb_scheduled_task import TBScheduledTask
from core.models.tb_email_config import TBEmailConfig
from core.logger import logger

_mongo_client: Optional[AsyncIOMotorClient] = None
_database_name: Optional[str] = None


async def init_db(custom_client: Optional[AsyncIOMotorClient] = None, database_name: Optional[str] = None):
    """
    Inicializa la conexión con MongoDB e inicializa Beanie ODM con los modelos de documentos registrados (User, TBServer, TBTenant, TBNode, TBBackup, AuditLog, TBScheduledTask).
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
        logger.info(f"[MongoDB] Conectando a {settings.MONGO_URI} (Base de datos: {db_name}, tz_aware=True)...")
        _mongo_client = AsyncIOMotorClient(settings.MONGO_URI, tz_aware=True)

    database = _mongo_client[db_name]

    # Reconciliación defensiva de índices previos para evitar IndexKeySpecsConflict
    try:
        email_col = database["tb_email_configs"]
        idx_info = await email_col.index_information()
        if "singleton_key_1" in idx_info and not idx_info["singleton_key_1"].get("unique", False):
            logger.info("[MongoDB] Reconciliando índice 'singleton_key_1' en tb_email_configs (recreando como único)...")
            await email_col.drop_index("singleton_key_1")
    except Exception as e:
        logger.debug(f"[MongoDB] Reconciliación defensiva de índices: {e}")

    # Inicialización de Beanie con reintentos para mitigar condiciones de carrera en arranque concurrente
    max_init_retries = 3
    for attempt in range(1, max_init_retries + 1):
        try:
            await init_beanie(
                database=database,
                document_models=[
                    User,
                    TBServer,
                    TBTenant,
                    TBNode,
                    TBBackup,
                    AuditLog,
                    TBScheduledTask,
                    TBEmailConfig
                ],
                allow_index_dropping=True
            )
            break
        except Exception as e:
            if attempt < max_init_retries and ("Index" in str(e) or "code 86" in str(e) or "IndexKeySpecsConflict" in str(e)):
                logger.warning(f"[MongoDB] Reintentando inicialización de Beanie (intento {attempt}/{max_init_retries}) tras conflicto de índice: {e}")
                try:
                    await database["tb_email_configs"].drop_index("singleton_key_1")
                except Exception:
                    pass
                await asyncio.sleep(attempt * 0.5)
            else:
                raise

    logger.info("[MongoDB] Beanie ODM inicializado exitosamente con los modelos User, TBServer, TBTenant, TBNode, TBBackup, AuditLog, TBScheduledTask y TBEmailConfig.")


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
