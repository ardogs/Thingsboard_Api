from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from beanie import PydanticObjectId

from core.models.tb_server import TBServer
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


@router.get("/{server_id}")
async def list_server_devices(
    server_id: str,
    limit: int = 100,
    page: int = 0,
    current_user: User = Depends(get_current_user)
):
    """
    Lista dispositivos desde una instancia ThingsBoard registrada en MongoDB.
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

    if not server.token:
        raise HTTPException(status_code=400, detail="El servidor no tiene un token JWT configurado")

    client = ThingsBoardClient(base_url=server.base_url, token=server.token, refresh_token=server.refresh_token)
    try:
        devices_data = await client.get_tenant_devices(token=server.token, limit=limit, page=page)
        return devices_data
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error consultando dispositivos en ThingsBoard: {str(e)}")


@router.get("/{server_id}/{device_id}")
async def get_device_details(
    server_id: str,
    device_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Obtiene los detalles de un dispositivo específico en el servidor ThingsBoard seleccionado.
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

    if not server.token:
        raise HTTPException(status_code=400, detail="El servidor no tiene un token JWT configurado")

    client = ThingsBoardClient(base_url=server.base_url, token=server.token, refresh_token=server.refresh_token)
    device = await client.get_device_by_id(device_id=device_id)

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
    Plantilla base para futuros scripts de aprovisionamiento masivo de dispositivos en ThingsBoard.
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

    return {
        "status": "ready_for_provisioning",
        "server_id": str(server.id),
        "server_name": server.name,
        "base_url": server.base_url,
        "count": len(request.devices),
        "message": "Plantilla de aprovisionamiento lista para integración con scripts de orquestación."
    }
