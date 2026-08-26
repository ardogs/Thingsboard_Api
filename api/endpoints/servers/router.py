from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from beanie import PydanticObjectId

from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.tb_client import ThingsBoardClient
from core.redis_client import redis_client
from api.deps import User, get_current_user
from workers.tasks import get_server_lock_key

router = APIRouter()


# ==========================================
# DTOs: Servidores ThingsBoard
# ==========================================

class ServerCreateRequest(BaseModel):
    name: str = Field(..., description="Nombre identificativo del servidor ThingsBoard")
    base_url: str = Field(..., description="URL base (ej: https://thingsboard.cloud)")
    description: Optional[str] = None
    rate_limit_rpm: Optional[int] = Field(default=60, description="Límite de peticiones por minuto")
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables (proxies, headers, flags)")


class ServerUpdateRequest(BaseModel):
    name: Optional[str] = None
    base_url: Optional[str] = None
    description: Optional[str] = None
    rate_limit_rpm: Optional[int] = None
    custom_metadata: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class ServerResponse(BaseModel):
    id: str
    name: str
    base_url: str
    description: Optional[str] = None
    rate_limit_rpm: int
    custom_metadata: Dict[str, Any]
    user_id: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class ServerStatusResponse(BaseModel):
    server_id: str = Field(..., description="ID del servidor ThingsBoard")
    is_busy: bool = Field(..., description="True si el servidor está procesando un respaldo (lock activo), False si está disponible")


def _to_server_response(server: TBServer) -> ServerResponse:
    return ServerResponse(
        id=str(server.id),
        name=server.name,
        base_url=server.base_url,
        description=server.description,
        rate_limit_rpm=server.rate_limit_rpm or 60,
        custom_metadata=server.custom_metadata or {},
        user_id=server.user_id,
        is_active=server.is_active,
        created_at=server.created_at,
        updated_at=server.updated_at
    )


# ==========================================
# DTOs: Tenants de ThingsBoard
# ==========================================

class TenantCreateRequest(BaseModel):
    name: str = Field(..., description="Nombre del tenant (ej: CONAFOR, CFE, Bajio_Norte)")
    username: Optional[str] = Field(default=None, description="Usuario / Email del Tenant Admin en ThingsBoard")
    password: Optional[str] = Field(default=None, description="Contraseña del Tenant Admin en ThingsBoard")
    token: Optional[str] = Field(default=None, description="Token JWT de acceso a ThingsBoard (opcional)")
    refresh_token: Optional[str] = Field(default=None, description="Refresh Token de ThingsBoard (opcional)")
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables específicos del tenant")


class TenantUpdateRequest(BaseModel):
    name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    custom_metadata: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class TenantResponse(BaseModel):
    id: str
    server_id: str
    name: str
    username: Optional[str] = None
    has_token: bool
    has_credentials: bool
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    custom_metadata: Dict[str, Any]
    user_id: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime


def _get_server_ref_id(tenant: TBTenant) -> str:
    if isinstance(tenant.server_id, TBServer):
        return str(tenant.server_id.id)
    ref = tenant.server_id.to_ref() if hasattr(tenant.server_id, "to_ref") else tenant.server_id
    ref_id = ref.id if hasattr(ref, "id") else ref
    return str(ref_id)


def _to_tenant_response(tenant: TBTenant) -> TenantResponse:
    return TenantResponse(
        id=str(tenant.id),
        server_id=_get_server_ref_id(tenant),
        name=tenant.name,
        username=tenant.username,
        has_token=bool(tenant.encrypted_token),
        has_credentials=bool(tenant.username and tenant.encrypted_password),
        token=tenant.get_token(),
        refresh_token=tenant.get_refresh_token(),
        custom_metadata=tenant.custom_metadata or {},
        user_id=tenant.user_id,
        is_active=tenant.is_active,
        created_at=tenant.created_at,
        updated_at=tenant.updated_at
    )


async def _resolve_server(server_id: str, current_user: User) -> TBServer:
    try:
        obj_id = PydanticObjectId(server_id)
        server = await TBServer.get(obj_id)
    except Exception:
        server = await TBServer.get(server_id)

    if not server:
        raise HTTPException(status_code=404, detail="Servidor ThingsBoard no encontrado")

    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    if not is_admin and server.user_id not in [str(current_user.id), current_user.id]:
        raise HTTPException(status_code=403, detail="No tienes permisos para acceder a este servidor")

    return server


async def _resolve_tenant(tenant_id: str, current_user: User) -> TBTenant:
    try:
        obj_id = PydanticObjectId(tenant_id)
        tenant = await TBTenant.get(obj_id)
    except Exception:
        tenant = await TBTenant.get(tenant_id)

    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant de ThingsBoard no encontrado")

    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    if not is_admin and tenant.user_id not in [str(current_user.id), current_user.id]:
        raise HTTPException(status_code=403, detail="No tienes permisos para acceder a este tenant")

    return tenant


# ==========================================
# Endpoints: Servidores ThingsBoard (/api/v1/servers)
# ==========================================

@router.post("", response_model=ServerResponse, status_code=status.HTTP_201_CREATED)
async def create_server(
    request: ServerCreateRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Registra una nueva instancia o servidor ThingsBoard en la base de datos MongoDB.
    """
    server = TBServer(
        name=request.name.strip(),
        base_url=request.base_url.strip().rstrip("/"),
        description=request.description,
        rate_limit_rpm=request.rate_limit_rpm or 60,
        custom_metadata=request.custom_metadata or {},
        user_id=str(current_user.id),
        is_active=True
    )
    await server.insert()
    return _to_server_response(server)


@router.get("", response_model=List[ServerResponse])
async def list_servers(
    current_user: User = Depends(get_current_user)
):
    """
    Lista todos los servidores ThingsBoard registrados pertenecientes al usuario autenticado.
    """
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    if is_admin:
        servers = await TBServer.find_all().to_list()
    else:
        servers = await TBServer.find(
            {"$or": [{"user_id": str(current_user.id)}, {"user_id": current_user.id}]}
        ).to_list()

    return [_to_server_response(s) for s in servers]


@router.get("/{server_id}", response_model=ServerResponse)
async def get_server(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Obtiene el detalle de un servidor ThingsBoard registrado.
    """
    server = await _resolve_server(server_id, current_user)
    return _to_server_response(server)


@router.put("/{server_id}", response_model=ServerResponse)
async def update_server(
    server_id: str,
    request: ServerUpdateRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Actualiza la configuración o metadatos de un servidor ThingsBoard en MongoDB.
    """
    server = await _resolve_server(server_id, current_user)

    update_data = request.model_dump(exclude_unset=True)
    if "base_url" in update_data and update_data["base_url"]:
        update_data["base_url"] = update_data["base_url"].strip().rstrip("/")
    if "name" in update_data and update_data["name"]:
        update_data["name"] = update_data["name"].strip()

    update_data["updated_at"] = datetime.now(timezone.utc)

    for field, val in update_data.items():
        setattr(server, field, val)

    await server.save()
    return _to_server_response(server)


@router.delete("/{server_id}")
async def delete_server(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Elimina un servidor ThingsBoard de MongoDB y sus tenants asociados.
    """
    server = await _resolve_server(server_id, current_user)
    
    # Eliminar tenants asociados
    await TBTenant.find(TBTenant.server_id == server.to_ref()).delete()
    await server.delete()
    return {"status": "ok", "message": f"Servidor '{server.name}' y sus tenants asociados eliminados exitosamente"}


@router.post("/{server_id}/test-connection")
async def test_server_connection(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Prueba la conectividad hacia la instancia ThingsBoard.
    """
    server = await _resolve_server(server_id, current_user)
    client = ThingsBoardClient(base_url=server.base_url)
    result = await client.test_connection()
    return result


@router.get("/{server_id}/status", response_model=ServerStatusResponse)
async def get_server_status(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Consulta el estado de concurrencia y bloqueo distribuido (Distributed Lock) de un servidor ThingsBoard.
    Permite al frontend verificar en tiempo real si el servidor está libre u ocupado procesando un respaldo.
    """
    server = await _resolve_server(server_id, current_user)
    lock_key = get_server_lock_key(str(server.id))
    is_busy = bool(await redis_client.exists(lock_key))
    return ServerStatusResponse(
        server_id=str(server.id),
        is_busy=is_busy
    )


@router.post("/{server_id}/unlock")
async def force_unlock_server(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Libera forzadamente el candado distribuido de un servidor ThingsBoard en Redis.
    Permite recuperar servidores bloqueados tras caídas forzadas o reinicios de servicios.
    """
    server = await _resolve_server(server_id, current_user)
    lock_key = get_server_lock_key(str(server.id))
    deleted_count = await redis_client.delete(lock_key)
    was_locked = bool(deleted_count > 0)
    return {
        "status": "ok",
        "server_id": str(server.id),
        "unlocked": was_locked,
        "message": f"Candado de '{server.name}' liberado exitosamente" if was_locked else f"El servidor '{server.name}' no tenía ningún candado activo"
    }


# ==========================================
# Endpoints: CRUD de Tenants (/api/v1/servers/{server_id}/tenants)
# ==========================================

@router.post("/{server_id}/tenants", response_model=TenantResponse, status_code=status.HTTP_201_CREATED)
async def create_tenant(
    server_id: str,
    request: TenantCreateRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Registra un nuevo Tenant bajo el servidor ThingsBoard especificado.
    Almacena las credenciales de Tenant Admin, tokens JWT cifrados y metadatos específicos.
    Si no se proporcionan tokens, el Celery Worker ejecutará el login de arranque en frío automáticamente.
    """
    server = await _resolve_server(server_id, current_user)

    tenant = TBTenant(
        server_id=server,
        name=request.name.strip(),
        username=request.username.strip() if request.username else None,
        custom_metadata=request.custom_metadata or {},
        user_id=str(current_user.id),
        is_active=True
    )
    if request.password:
        tenant.set_password(request.password)
    if request.token or request.refresh_token:
        tenant.set_tokens(request.token, request.refresh_token)

    await tenant.insert()
    return _to_tenant_response(tenant)


@router.get("/{server_id}/tenants", response_model=List[TenantResponse])
async def list_server_tenants(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Lista todos los tenants registrados bajo un servidor ThingsBoard específico.
    """
    server = await _resolve_server(server_id, current_user)

    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    if is_admin:
        tenants = await TBTenant.find(TBTenant.server_id == server.to_ref()).to_list()
    else:
        tenants = await TBTenant.find(
            TBTenant.server_id == server.to_ref(),
            {"$or": [{"user_id": str(current_user.id)}, {"user_id": current_user.id}]}
        ).to_list()

    return [_to_tenant_response(t) for t in tenants]


@router.get("/{server_id}/tenants/{tenant_id}", response_model=TenantResponse)
async def get_server_tenant(
    server_id: str,
    tenant_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Obtiene el detalle de un tenant específico bajo un servidor ThingsBoard.
    """
    await _resolve_server(server_id, current_user)
    tenant = await _resolve_tenant(tenant_id, current_user)
    
    if _get_server_ref_id(tenant) != server_id:
        raise HTTPException(status_code=400, detail="El tenant no pertenece al servidor especificado")

    return _to_tenant_response(tenant)


@router.put("/{server_id}/tenants/{tenant_id}", response_model=TenantResponse)
async def update_server_tenant(
    server_id: str,
    tenant_id: str,
    request: TenantUpdateRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Actualiza la configuración, credenciales cifradas o metadatos de un tenant.
    """
    await _resolve_server(server_id, current_user)
    tenant = await _resolve_tenant(tenant_id, current_user)

    if _get_server_ref_id(tenant) != server_id:
        raise HTTPException(status_code=400, detail="El tenant no pertenece al servidor especificado")

    update_data = request.model_dump(exclude_unset=True)
    if "name" in update_data and update_data["name"]:
        tenant.name = update_data["name"].strip()
    if "username" in update_data:
        tenant.username = update_data["username"].strip() if update_data["username"] else None
    if "password" in update_data:
        tenant.set_password(update_data["password"])
    if "token" in update_data or "refresh_token" in update_data:
        current_tok = tenant.get_token()
        current_ref = tenant.get_refresh_token()
        new_tok = update_data.get("token", current_tok)
        new_ref = update_data.get("refresh_token", current_ref)
        tenant.set_tokens(new_tok, new_ref)
    if "custom_metadata" in update_data:
        tenant.custom_metadata = update_data["custom_metadata"]
    if "is_active" in update_data and update_data["is_active"] is not None:
        tenant.is_active = update_data["is_active"]

    tenant.updated_at = datetime.now(timezone.utc)
    await tenant.save()
    return _to_tenant_response(tenant)


@router.delete("/{server_id}/tenants/{tenant_id}")
async def delete_server_tenant(
    server_id: str,
    tenant_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Elimina un tenant de ThingsBoard registrado en MongoDB.
    """
    await _resolve_server(server_id, current_user)
    tenant = await _resolve_tenant(tenant_id, current_user)

    if _get_server_ref_id(tenant) != server_id:
        raise HTTPException(status_code=400, detail="El tenant no pertenece al servidor especificado")

    await tenant.delete()
    return {"status": "ok", "message": f"Tenant '{tenant.name}' eliminado exitosamente"}


@router.post("/{server_id}/tenants/{tenant_id}/test-connection")
async def test_tenant_connection(
    server_id: str,
    tenant_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Prueba la conectividad y validación de autenticación de un tenant específico contra ThingsBoard.
    """
    server = await _resolve_server(server_id, current_user)
    tenant = await _resolve_tenant(tenant_id, current_user)

    if _get_server_ref_id(tenant) != server_id:
        raise HTTPException(status_code=400, detail="El tenant no pertenece al servidor especificado")

    client = ThingsBoardClient(
        base_url=server.base_url,
        token=tenant.get_token(),
        refresh_token=tenant.get_refresh_token(),
        username=tenant.username,
        password=tenant.get_password()
    )

    result = await client.test_connection()
    result["tenant_name"] = tenant.name
    result["tenant_id"] = str(tenant.id)
    return result
