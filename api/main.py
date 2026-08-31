from contextlib import asynccontextmanager
from fastapi import FastAPI

from core.config import settings
from core.database import init_db, close_db
from core.casbin_enforcer import init_casbin_enforcer
from core.bootstrap import bootstrap_superadmin
from core.arq_pool import get_arq_pool, close_arq_pool
from api.endpoints.auth import router as auth_router
from api.endpoints.users import router as users_router
from api.endpoints.iam import router as iam_router
from api.endpoints.servers import router as servers_router
from api.endpoints.telemetry import router as telemetry_router
from api.endpoints.devices import router as devices_router
from api.endpoints.scheduler import router as scheduler_router
from api.middlewares.audit_log import AuditLogMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manejo del ciclo de vida de la aplicación:
    - Startup: Conectar a MongoDB (Beanie ODM), inicializar AsyncEnforcer de Casbin, bootstrapping y ARQ pool
    - Shutdown: Cerrar conexiones ARQ y MongoDB
    """
    await init_db()
    await init_casbin_enforcer()
    await bootstrap_superadmin()
    app.state.arq_pool = await get_arq_pool()
    yield
    await close_arq_pool()
    await close_db()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version="0.1.0-alfa",
    description="API Gateway de administración multi-propósito para ThingsBoard",
    lifespan=lifespan
)

# Middleware de Auditoría y Trazabilidad DevSecOps
app.add_middleware(AuditLogMiddleware)

# Routers de Dominio (API v1)
app.include_router(auth_router, prefix="/api/v1/auth", tags=["Autenticación"])
app.include_router(users_router, prefix="/api/v1/users", tags=["Gestión de Usuarios"])
app.include_router(iam_router, prefix="/api/v1/iam", tags=["IAM y Políticas Casbin"])
app.include_router(servers_router, prefix="/api/v1/servers", tags=["Servidores ThingsBoard"])
app.include_router(telemetry_router, prefix="/api/v1/telemetry", tags=["Telemetría"])
app.include_router(devices_router, prefix="/api/v1/tenants", tags=["Dispositivos y Aprovisionamiento"])
app.include_router(scheduler_router, prefix="/api/v1/scheduler/tasks", tags=["Scheduler y Tareas Programadas"])

# Compatibilidad con rutas v0 / legacy
app.include_router(auth_router, prefix="/api/auth", include_in_schema=False)
app.include_router(telemetry_router, prefix="/api/telemetry", include_in_schema=False)


@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": settings.PROJECT_NAME,
        "version": "2.0.0",
        "message": "ThingsBoard Super API Gateway activo con MongoDB y Beanie ODM"
    }