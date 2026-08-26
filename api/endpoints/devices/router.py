from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from beanie import PydanticObjectId

from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.tb_client import ThingsBoardClient
from api.deps import User, get_current_user

router = APIRouter()


class DeviceProvisionRequest(BaseModel):
    name: str = Field(..., description="Nombre del dispositivo")
    type: str = Field(default="default", description="Tipo o perfil del dispositivo")
    label: Optional[str] = None
    additional_info: Dict[str, Any] = Field(default_factory=dict, description="Atributos o metadatos iniciales")


class DeviceProvisionBatchRequest(BaseModel):
    devices: List[DeviceProvisionRequest]
    device_profile_id: Optional[str] = None
    tenant_id: Optional[str] = None


async def _resolve_server_and_tenant(
    server_id: str,
    tenant_id: Optional[str],
    current_user: User
) -> tuple[TBServer, TBTenant]:
    try:
        obj_id = PydanticObjectId(server_id)
        server = await TBServer.get(obj_id)
    except Exception:
        server = await TBServer.get(server_id)

    if not server:
        raise HTTPException(status_code=404, detail="Servidor ThingsBoard no encontrado")

    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    if not is_admin and server.user_id not in [str(current_user.id), current_user.id]:
        raise HTTPException(status_code=403, detail="No tienes permisos para este servidor")

    # Si se especificó tenant_id, buscar ese tenant específico
    tenant: Optional[TBTenant] = None
    if tenant_id:
        try:
            t_id = PydanticObjectId(tenant_id)
            tenant = await TBTenant.get(t_id)
        except Exception:
            tenant = await TBTenant.get(tenant_id)

        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant especificado no encontrado")
        if not is_admin and tenant.user_id not in [str(current_user.id), current_user.id]:
            raise HTTPException(status_code=403, detail="No tienes permisos para este tenant")
    else:
        # Obtener el primer tenant disponible del usuario bajo este servidor
        if is_admin:
            tenant = await TBTenant.find_one(TBTenant.server_id == server.to_ref())
        else:
            tenant = await TBTenant.find_one(
                TBTenant.server_id == server.to_ref(),
                {"$or": [{"user_id": str(current_user.id)}, {"user_id": current_user.id}]}
            )

    if not tenant:
        raise HTTPException(
            status_code=400,
            detail="No se encontró ningún Tenant registrado bajo este servidor. Registra un Tenant primero."
        )

    if not tenant.encrypted_token and not (tenant.username and tenant.encrypted_password):
        raise HTTPException(status_code=400, detail="El Tenant no tiene un token JWT ni credenciales configuradas")

    return server, tenant


@router.get("/{server_id}")
async def list_server_devices(
    server_id: str,
    tenant_id: Optional[str] = Query(default=None, description="ID opcional del Tenant específico"),
    limit: int = 100,
    page: int = 0,
    current_user: User = Depends(get_current_user)
):
    """
    Lista dispositivos desde una instancia ThingsBoard utilizando las credenciales del Tenant.
    """
    server, tenant = await _resolve_server_and_tenant(server_id, tenant_id, current_user)

    client = ThingsBoardClient(
        base_url=server.base_url,
        token=tenant.get_token(),
        refresh_token=tenant.get_refresh_token(),
        username=tenant.username,
        password=tenant.get_password()
    )

    token = tenant.get_token()
    password = tenant.get_password()
    if not token and tenant.username and password:
        login_res = await client.login(tenant.username, password)
        if login_res and "token" in login_res:
            token = login_res["token"]
            tenant.set_tokens(token, login_res.get("refreshToken"))
            tenant.updated_at = datetime.now(timezone.utc)
            await tenant.save()

    try:
        devices_data = await client.get_tenant_devices(token=token, limit=limit, page=page)
        return devices_data
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error consultando dispositivos en ThingsBoard: {str(e)}")


@router.get("/{server_id}/{device_id}")
async def get_device_details(
    server_id: str,
    device_id: str,
    tenant_id: Optional[str] = Query(default=None, description="ID opcional del Tenant específico"),
    current_user: User = Depends(get_current_user)
):
    """
    Obtiene los detalles de un dispositivo específico en el servidor ThingsBoard seleccionado.
    """
    server, tenant = await _resolve_server_and_tenant(server_id, tenant_id, current_user)

    client = ThingsBoardClient(
        base_url=server.base_url,
        token=tenant.get_token(),
        refresh_token=tenant.get_refresh_token(),
        username=tenant.username,
        password=tenant.get_password()
    )

    token = tenant.get_token()
    password = tenant.get_password()
    if not token and tenant.username and password:
        login_res = await client.login(tenant.username, password)
        if login_res and "token" in login_res:
            token = login_res["token"]
            tenant.set_tokens(token, login_res.get("refreshToken"))
            tenant.updated_at = datetime.now(timezone.utc)
            await tenant.save()

    device = await client.get_device_by_id(device_id=device_id, token=token)

    if not device:
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado en ThingsBoard")

    return device


@router.post("/{server_id}/provision")
async def provision_devices(
    server_id: str,
    request: DeviceProvisionBatchRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Plantilla base para aprovisionamiento masivo de dispositivos en ThingsBoard por Tenant.
    """
    server, tenant = await _resolve_server_and_tenant(server_id, request.tenant_id, current_user)

    return {
        "status": "ready_for_provisioning",
        "server_id": str(server.id),
        "server_name": server.name,
        "tenant_id": str(tenant.id),
        "tenant_name": tenant.name,
        "base_url": server.base_url,
        "count": len(request.devices),
        "message": "Plantilla de aprovisionamiento lista para integración con scripts de orquestación."
    }
