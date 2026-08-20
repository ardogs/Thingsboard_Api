from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from beanie import PydanticObjectId

from core.models.tb_server import TBServer
from core.tb_client import ThingsBoardClient
from api.deps import User, get_current_user

router = APIRouter()


class ServerCreateRequest(BaseModel):
    name: str = Field(..., description="Nombre identificativo del servidor ThingsBoard")
    base_url: str = Field(..., description="URL base (ej: https://thingsboard.cloud)")
    description: Optional[str] = None
    api_key: Optional[str] = None
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    rate_limit_rpm: Optional[int] = Field(default=60, description="Límite de peticiones por minuto")
    custom_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadatos variables (proxies, headers, flags)")


class ServerUpdateRequest(BaseModel):
    name: Optional[str] = None
    base_url: Optional[str] = None
    description: Optional[str] = None
    api_key: Optional[str] = None
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    rate_limit_rpm: Optional[int] = None
    custom_metadata: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class ServerResponse(BaseModel):
    id: str
    name: str
    base_url: str
    description: Optional[str] = None
    has_token: bool
    has_credentials: bool
    rate_limit_rpm: int
    custom_metadata: Dict[str, Any]
    user_id: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime


def _to_server_response(server: TBServer) -> ServerResponse:
    return ServerResponse(
        id=str(server.id),
        name=server.name,
        base_url=server.base_url,
        description=server.description,
        has_token=bool(server.token),
        has_credentials=bool(server.username and server.password),
        rate_limit_rpm=server.rate_limit_rpm or 60,
        custom_metadata=server.custom_metadata or {},
        user_id=server.user_id,
        is_active=server.is_active,
        created_at=server.created_at,
        updated_at=server.updated_at
    )


@router.post("", response_model=ServerResponse, status_code=status.HTTP_201_CREATED)
async def create_server(
    request: ServerCreateRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Registra un nuevo servidor ThingsBoard en MongoDB asociado al usuario autenticado.
    """
    server = TBServer(
        name=request.name.strip(),
        base_url=request.base_url.strip().rstrip("/"),
        description=request.description,
        api_key=request.api_key,
        token=request.token,
        refresh_token=request.refresh_token,
        username=request.username,
        password=request.password,
        rate_limit_rpm=request.rate_limit_rpm or 60,
        custom_metadata=request.custom_metadata or {},
        user_id=current_user.id,
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
    if current_user.role == "admin":
        servers = await TBServer.find_all().to_list()
    else:
        servers = await TBServer.find(TBServer.user_id == current_user.id).to_list()

    return [_to_server_response(s) for s in servers]


@router.get("/{server_id}", response_model=ServerResponse)
async def get_server(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Obtiene el detalle de un servidor ThingsBoard registrado.
    """
    try:
        obj_id = PydanticObjectId(server_id)
        server = await TBServer.get(obj_id)
    except Exception:
        server = await TBServer.get(server_id)

    if not server:
        raise HTTPException(status_code=404, detail="Servidor ThingsBoard no encontrado")

    if current_user.role != "admin" and server.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="No tienes permisos para consultar este servidor")

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
    try:
        obj_id = PydanticObjectId(server_id)
        server = await TBServer.get(obj_id)
    except Exception:
        server = await TBServer.get(server_id)

    if not server:
        raise HTTPException(status_code=404, detail="Servidor ThingsBoard no encontrado")

    if current_user.role != "admin" and server.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="No tienes permisos para modificar este servidor")

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
    Elimina un servidor ThingsBoard de MongoDB.
    """
    try:
        obj_id = PydanticObjectId(server_id)
        server = await TBServer.get(obj_id)
    except Exception:
        server = await TBServer.get(server_id)

    if not server:
        raise HTTPException(status_code=404, detail="Servidor ThingsBoard no encontrado")

    if current_user.role != "admin" and server.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="No tienes permisos para eliminar este servidor")

    await server.delete()
    return {"status": "ok", "message": f"Servidor '{server.name}' eliminado exitosamente"}


@router.post("/{server_id}/test-connection")
async def test_server_connection(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Prueba la conectividad y validación de credenciales en tiempo real contra la instancia ThingsBoard.
    """
    try:
        obj_id = PydanticObjectId(server_id)
        server = await TBServer.get(obj_id)
    except Exception:
        server = await TBServer.get(server_id)

    if not server:
        raise HTTPException(status_code=404, detail="Servidor ThingsBoard no encontrado")

    if current_user.role != "admin" and server.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="No tienes permisos para este servidor")

    client = ThingsBoardClient(
        base_url=server.base_url,
        token=server.token,
        refresh_token=server.refresh_token,
        username=server.username,
        password=server.password
    )

    result = await client.test_connection()
    return result
