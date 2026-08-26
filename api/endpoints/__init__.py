from api.endpoints.auth import router as auth_router
from api.endpoints.users import router as users_router
from api.endpoints.iam import router as iam_router
from api.endpoints.servers import router as servers_router
from api.endpoints.telemetry import router as telemetry_router
from api.endpoints.devices import router as devices_router

__all__ = [
    "auth_router",
    "users_router",
    "iam_router",
    "servers_router",
    "telemetry_router",
    "devices_router"
]
