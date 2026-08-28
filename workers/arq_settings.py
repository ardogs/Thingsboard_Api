import httpx
from arq.connections import RedisSettings
from arq.cron import cron
from arq.worker import func

from core.config import settings
from core.database import init_db, close_db
from core.logger import logger

from workers.tasks import (
    download_telemetry_task,
    master_dispatcher_task,
    cleanup_old_backups_task,
    execute_incremental_tenant_backup_task,
)


async def startup(ctx: dict):
    """
    Ciclo de vida de inicio del Worker ARQ (on_startup):
    1. Inicializa conexión a MongoDB con Beanie ODM una sola vez.
    2. Crea un pool compartido de conexiones HTTP con httpx.AsyncClient y lo inyecta en ctx['http_client'].
    """
    logger.info("[ARQ Worker] Inicializando worker y recursos compartidos...")
    await init_db()

    # Pool persistente HTTPX para reutilización de sockets TCP/TLS Keep-Alive
    ctx["http_client"] = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=60.0),
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)
    )
    logger.info("[ARQ Worker] Worker iniciado: MongoDB (Beanie) y HTTPX connection pool listos.")


async def shutdown(ctx: dict):
    """
    Ciclo de vida de apagado del Worker ARQ (on_shutdown):
    1. Cierra el pool compartido de HTTPX.
    2. Cierra las conexiones a MongoDB.
    """
    logger.info("[ARQ Worker] Cerrando worker y liberando recursos...")
    if "http_client" in ctx and ctx["http_client"]:
        try:
            await ctx["http_client"].aclose()
            logger.info("[ARQ Worker] HTTPX client pool cerrado exitosamente.")
        except Exception as e:
            logger.warning(f"[ARQ Worker] Advertencia cerrando HTTPX pool: {e}")

    await close_db()
    logger.info("[ARQ Worker] Worker detenido limpiamente.")


# Lista compartida de funciones registradas en ARQ
REGISTERED_FUNCTIONS = [
    download_telemetry_task,
    func(download_telemetry_task, name="tasks.download_telemetry"),
    master_dispatcher_task,
    func(master_dispatcher_task, name="tasks.master_dispatcher"),
    cleanup_old_backups_task,
    func(cleanup_old_backups_task, name="tasks.cleanup_old_backups"),
    execute_incremental_tenant_backup_task,
    func(execute_incremental_tenant_backup_task, name="tasks.execute_incremental_tenant_backup"),
]


class WorkerSettings:
    """
    Configuración principal del Worker de ARQ.
    Ejecución: arq workers.arq_settings.WorkerSettings
    """
    functions = REGISTERED_FUNCTIONS
    # Planificador integrado (reemplazo de Celery Beat): ejecuta master_dispatcher cada minuto en el segundo 0
    cron_jobs = [
        cron(master_dispatcher_task, second=0),
    ]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL)
    max_jobs = 10
    job_timeout = 864000  # 10 días (864,000s) para descargas masivas de larga duración
    max_tries = 5


class IncrementalWorkerSettings:
    """
    Worker dedicado para la cola de respaldos incrementales.
    Ejecución: arq workers.arq_settings.IncrementalWorkerSettings
    """
    functions = REGISTERED_FUNCTIONS
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL)
    queue_name = "incremental_backups"
    max_jobs = 2
    job_timeout = 864000
    max_tries = 5
