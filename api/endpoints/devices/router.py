import httpx
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from beanie import PydanticObjectId

from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.tb_client import ThingsBoardClient
from core.casbin_enforcer import get_casbin_enforcer
from core.logger import logger
from api.deps import User, get_current_user

router = APIRouter()


# ==========================================
# DTOs: Dispositivos y Aprovisionamiento
# ==========================================

class DeviceProvisionRequest(BaseModel):
    name: str = Field(..., description="Nombre del dispositivo")
    type: str = Field(default="default", description="Tipo o perfil del dispositivo")
    label: Optional[str] = None
    additional_info: Dict[str, Any] = Field(default_factory=dict, description="Atributos o metadatos iniciales")


class DeviceProvisionBatchRequest(BaseModel):
    devices: List[DeviceProvisionRequest] = Field(..., description="Lista de dispositivos a aprovisionar")
    device_profile_id: Optional[str] = Field(default=None, description="ID opcional del perfil de dispositivo")


class DeviceProvisionErrorDetail(BaseModel):
    name: str = Field(..., description="Nombre del dispositivo que falló")
    index: int = Field(..., description="Índice del dispositivo en el lote recibido (0-indexed)")
    status_code: Optional[int] = Field(default=None, description="Código de estado HTTP devuelto por ThingsBoard")
    error: str = Field(..., description="Descripción del error o excepción capturada")


class CreatedDeviceSummary(BaseModel):
    id: str = Field(..., description="ID del dispositivo creado en ThingsBoard")
    name: str = Field(..., description="Nombre del dispositivo")
    type: str = Field(default="default", description="Tipo o perfil del dispositivo")
    label: Optional[str] = Field(default=None, description="Etiqueta del dispositivo")


class DeviceProvisionBatchResponse(BaseModel):
    status: str = Field(..., description="Estado de la operación: success, partial_success, failed")
    tenant_id: str = Field(..., description="ID del Tenant")
    tenant_name: str = Field(..., description="Nombre del Tenant")
    server_id: str = Field(..., description="ID del servidor ThingsBoard")
    server_name: str = Field(..., description="Nombre del servidor")
    total_received: int = Field(..., description="Total de dispositivos recibidos en la solicitud")
    successfully_created: int = Field(..., description="Cantidad de dispositivos creados con éxito")
    failed: int = Field(..., description="Cantidad de dispositivos con error")
    created_devices: List[CreatedDeviceSummary] = Field(default_factory=list, description="Lista de dispositivos creados exitosamente")
    errors: List[DeviceProvisionErrorDetail] = Field(default_factory=list, description="Detalle de errores ocurridos durante el aprovisionamiento")
    message: str = Field(..., description="Resumen descriptivo del resultado del lote")


class DeviceSummary(BaseModel):
    id: str
    name: str
    type: str = "default"
    label: Optional[str] = None
    additional_info: Optional[Dict[str, Any]] = None


class SiteWithDevicesResponse(BaseModel):
    id: str
    name: str
    type: str = "ASSET"
    label: Optional[str] = None
    additional_info: Optional[Dict[str, Any]] = None
    devices: List[DeviceSummary] = Field(default_factory=list)


# ==========================================
# Resolución Inversa y Autenticación Resiliente
# ==========================================

async def _resolve_tenant_and_server(
    tenant_id: str,
    current_user: User
) -> tuple[TBTenant, TBServer]:
    """
    Resolución Inversa Confiable:
    1. Busca el documento TBTenant en MongoDB usando tenant_id (HTTP 404 si no existe).
    2. Valida permisos del usuario con aislamiento Zero-Trust (Superadmin, Admin o Propietario del tenant).
    3. Resuelve el servidor padre dinámicamente mediante await tenant.get_server().
    """
    try:
        obj_id = PydanticObjectId(tenant_id)
        tenant = await TBTenant.get(obj_id)
    except Exception:
        tenant = await TBTenant.get(tenant_id)

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant de ThingsBoard con ID '{tenant_id}' no encontrado"
        )

    # Validación de permisos y aislamiento de identidad
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    is_owner = tenant.user_id in [str(current_user.id), current_user.id]
    if not is_admin and not is_owner:
        try:
            enforcer = get_casbin_enforcer()
            tenant_domain = f"tenant:{tenant.id}"
            is_allowed = enforcer.enforce(str(current_user.id), tenant_domain, "devices", "read")
            if not is_allowed and current_user.username:
                is_allowed = enforcer.enforce(current_user.username, tenant_domain, "devices", "read")
        except Exception:
            is_allowed = False

        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos para acceder a los dispositivos de este Tenant"
            )

    server = await tenant.get_server()
    if not server:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"El servidor ThingsBoard asociado al Tenant '{tenant.name}' no existe o fue eliminado"
        )

    return tenant, server


async def _reauthenticate_tenant(
    tenant: TBTenant,
    client: ThingsBoardClient
) -> str:
    """
    Auto-renovación autónoma de tokens JWT contra ThingsBoard:
    1. Intenta renovar mediante refresh_token si existe.
    2. Si falla o no existe, ejecuta login con username y password descifrada en RAM.
    3. Actualiza y persiste los nuevos tokens cifrados en MongoDB (TBTenant) para futuras peticiones.
    """
    plain_refresh_token = tenant.get_refresh_token()
    plain_password = tenant.get_password()
    new_token: Optional[str] = None
    new_refresh_token: Optional[str] = None

    # a) Intentar renovar con refresh_token
    if plain_refresh_token:
        try:
            logger.warning(f"[Devices] Intentando renovación con refreshToken para Tenant '{tenant.name}'...")
            tokens_res = await client.refresh_jwt_token(plain_refresh_token)
            if tokens_res and "token" in tokens_res:
                new_token = tokens_res["token"]
                new_refresh_token = tokens_res.get("refreshToken", plain_refresh_token)
                logger.info(f"[Devices] Token JWT del Tenant '{tenant.name}' renovado con éxito mediante refreshToken.")
        except Exception as ref_err:
            logger.warning(f"[Devices] Falló renovación con refreshToken ({ref_err}). Procediendo con fallback de login...")

    # b) Si refresh_token falló o no existe, hacer login completo
    if not new_token and tenant.username and plain_password:
        try:
            logger.info(f"[Devices] Ejecutando login de credenciales para Tenant '{tenant.name}'...")
            login_res = await client.login(tenant.username, plain_password)
            if login_res and "token" in login_res:
                new_token = login_res["token"]
                new_refresh_token = login_res.get("refreshToken")
                logger.info(f"[Devices] Re-autenticación exitosa mediante login para Tenant '{tenant.name}'.")
        except Exception as login_err:
            logger.error(f"[Devices] Falló login con credenciales para Tenant '{tenant.name}': {login_err}")

    if not new_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"No se pudo autenticar contra ThingsBoard para el Tenant '{tenant.name}'. Verifica las credenciales configuradas."
        )

    # Actualizar estado en cliente y persistir en MongoDB cifrado con Fernet
    client.token = new_token
    if new_refresh_token:
        client.refresh_token = new_refresh_token
    tenant.set_tokens(new_token, new_refresh_token)
    tenant.updated_at = datetime.now(timezone.utc)
    await tenant.save()

    return new_token


async def _get_authenticated_client(
    tenant: TBTenant,
    server: TBServer
) -> tuple[ThingsBoardClient, str]:
    """
    Despliegue Dinámico de Credenciales:
    Descifra el JWT en RAM e inicializa la instancia ThingsBoardClient.
    Si el token está ausente en la base de datos, ejecuta login inicial.
    """
    plain_token = tenant.get_token()
    plain_refresh_token = tenant.get_refresh_token()
    plain_password = tenant.get_password()

    client = ThingsBoardClient(
        base_url=server.base_url,
        token=plain_token,
        refresh_token=plain_refresh_token,
        username=tenant.username,
        password=plain_password
    )

    token = plain_token
    if not token:
        token = await _reauthenticate_tenant(tenant, client)

    return client, token


# ==========================================
# Endpoints RESTful Canónicos (/api/v1/tenants/{tenant_id}/devices)
# ==========================================

@router.get("/{tenant_id}/devices")
async def list_tenant_devices(
    tenant_id: str,
    limit: int = 100,
    page: int = 0,
    current_user: User = Depends(get_current_user)
):
    """
    Lista dispositivos desde ThingsBoard pertenecientes al Tenant especificado.
    Aplica resolución inversa (TBTenant -> TBServer), auto-renovación ante 401 y despliegue de credenciales en RAM.
    """
    tenant, server = await _resolve_tenant_and_server(tenant_id, current_user)
    client, token = await _get_authenticated_client(tenant, server)

    try:
        devices_data = await client.get_tenant_devices(token=token, limit=limit, page=page)
        return devices_data
    except httpx.HTTPStatusError as http_err:
        if http_err.response.status_code == 401:
            logger.warning(f"[Devices] HTTP 401 al listar dispositivos para Tenant '{tenant.name}'. Renovando token y reintentando...")
            token = await _reauthenticate_tenant(tenant, client)
            try:
                return await client.get_tenant_devices(token=token, limit=limit, page=page)
            except Exception as retry_err:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Error consultando dispositivos en ThingsBoard tras re-autenticar: {str(retry_err)}"
                )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Error consultando dispositivos en ThingsBoard: {str(http_err)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Error consultando dispositivos en ThingsBoard: {str(e)}"
        )


@router.get("/{tenant_id}/devices/sites", response_model=List[SiteWithDevicesResponse])
async def list_sites_with_devices(
    tenant_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Recupera locaciones (Assets) y resuelve sus Devices relacionados en una sola llamada de red
    utilizando el motor de Entity Query de ThingsBoard (/api/entitiesQuery/find), con fallback
    automático a /api/tenant/assets, auto-renovación ante HTTP 401 y persistencia en MongoDB.
    """
    tenant, server = await _resolve_tenant_and_server(tenant_id, current_user)
    client, token = await _get_authenticated_client(tenant, server)

    entity_query = {
        "entityFilter": {
            "type": "entityType",
            "entityType": "ASSET"
        },
        "pageLink": {
            "pageSize": 1000,
            "page": 0,
            "sortOrder": {
                "key": {
                    "type": "ENTITY_FIELD",
                    "key": "name"
                },
                "direction": "ASC"
            }
        },
        "entityFields": [
            {"type": "ENTITY_FIELD", "key": "name"},
            {"type": "ENTITY_FIELD", "key": "type"},
            {"type": "ENTITY_FIELD", "key": "label"}
        ]
    }

    async def _fetch_sites(current_token: str) -> list[dict]:
        try:
            query_res = await client.find_entities_by_query(query=entity_query, token=current_token)
            if isinstance(query_res, dict) and "data" in query_res:
                return query_res.get("data", [])
        except httpx.HTTPStatusError as http_err:
            if http_err.response.status_code == 401:
                raise http_err
            logger.warning(f"[Sites] Entity Query retornó código {http_err.response.status_code}. Intentando fallback a /api/tenant/assets...")
        except Exception as e:
            logger.warning(f"[Sites] Entity Query falló ({e}). Intentando fallback a /api/tenant/assets...")

        # Fallback transparente a /api/tenant/assets
        assets_res = await client.get_tenant_assets(token=current_token, limit=1000, page=0)
        if isinstance(assets_res, dict):
            return assets_res.get("data", [])
        return []

    data_items = []
    try:
        data_items = await _fetch_sites(token)
    except httpx.HTTPStatusError as err:
        if err.response.status_code == 401:
            logger.warning(f"[Sites] HTTP 401 Unauthorized para Tenant '{tenant.name}'. Renovando token y reintentando...")
            token = await _reauthenticate_tenant(tenant, client)
            try:
                data_items = await _fetch_sites(token)
            except Exception as retry_err:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Error consultando sitios en ThingsBoard tras re-autenticar: {str(retry_err)}"
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error consultando sitios en ThingsBoard: {str(err)}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Error consultando sitios en ThingsBoard: {str(e)}"
        )

    sites_map: Dict[str, SiteWithDevicesResponse] = {}

    for item in data_items:
        # 1. Extracción robusta de ID (soporta entityId, id, dict o string)
        entity_id_info = item.get("entityId") or item.get("id") or {}
        if isinstance(entity_id_info, dict):
            entity_id = entity_id_info.get("id") or entity_id_info.get("entityId")
            entity_type = entity_id_info.get("entityType", "ASSET")
        elif isinstance(entity_id_info, str):
            entity_id = entity_id_info
            entity_type = "ASSET"
        else:
            entity_id = None
            entity_type = "ASSET"

        if not entity_id:
            entity_id = item.get("name") or str(id(item))

        # 2. Extracción robusta de campos (latest.ENTITY_FIELD o raíz)
        latest_fields = item.get("latest", {})
        entity_fields = latest_fields.get("ENTITY_FIELD", {}) if isinstance(latest_fields, dict) else {}

        name_obj = entity_fields.get("name") if isinstance(entity_fields, dict) else None
        name_val = name_obj.get("value") if isinstance(name_obj, dict) else name_obj
        name = name_val or item.get("name") or str(entity_id)

        type_obj = entity_fields.get("type") if isinstance(entity_fields, dict) else None
        type_val = type_obj.get("value") if isinstance(type_obj, dict) else type_obj
        asset_type = type_val or item.get("type") or entity_type or "ASSET"

        label_obj = entity_fields.get("label") if isinstance(entity_fields, dict) else None
        label_val = label_obj.get("value") if isinstance(label_obj, dict) else label_obj
        label = label_val or item.get("label")

        additional_info = item.get("additionalInfo") or item.get("additional_info")

        # 3. Extraer dispositivos embebidos o relacionados
        related_devices: List[DeviceSummary] = []
        raw_devices = item.get("devices") or item.get("related_devices") or []
        for dev in raw_devices:
            dev_id_info = dev.get("entityId") or dev.get("id") or {}
            if isinstance(dev_id_info, dict):
                dev_id = dev_id_info.get("id") or dev_id_info.get("entityId") or ""
            elif isinstance(dev_id_info, str):
                dev_id = dev_id_info
            else:
                dev_id = str(dev_id_info or "")

            if not dev_id:
                continue

            dev_name = dev.get("name") or dev_id
            dev_type = dev.get("type", "default")
            dev_label = dev.get("label")
            dev_info = dev.get("additionalInfo") or dev.get("additional_info")
            related_devices.append(
                DeviceSummary(
                    id=dev_id,
                    name=dev_name,
                    type=dev_type,
                    label=dev_label,
                    additional_info=dev_info
                )
            )

        site_obj = SiteWithDevicesResponse(
            id=str(entity_id),
            name=str(name),
            type=str(asset_type),
            label=label,
            additional_info=additional_info,
            devices=related_devices
        )
        sites_map[str(entity_id)] = site_obj

    return list(sites_map.values())


@router.get("/{tenant_id}/devices/{device_id}")
async def get_device_details(
    tenant_id: str,
    device_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Obtiene los detalles de un dispositivo específico perteneciente al Tenant.
    Aplica auto-renovación ante 401 si el token expiró.
    """
    tenant, server = await _resolve_tenant_and_server(tenant_id, current_user)
    client, token = await _get_authenticated_client(tenant, server)

    device = None
    try:
        device = await client.get_device_by_id(device_id=device_id, token=token)
    except httpx.HTTPStatusError as http_err:
        if http_err.response.status_code == 401:
            logger.warning(f"[Devices] HTTP 401 al consultar dispositivo '{device_id}'. Renovando token y reintentando...")
            token = await _reauthenticate_tenant(tenant, client)
            try:
                device = await client.get_device_by_id(device_id=device_id, token=token)
            except Exception as retry_err:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Error consultando dispositivo en ThingsBoard tras re-autenticar: {str(retry_err)}"
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error consultando dispositivo en ThingsBoard: {str(http_err)}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Error consultando dispositivo en ThingsBoard: {str(e)}"
        )

    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dispositivo no encontrado en ThingsBoard"
        )

    return device


@router.post("/{tenant_id}/devices/provision", response_model=DeviceProvisionBatchResponse)
async def provision_devices(
    tenant_id: str,
    request: DeviceProvisionBatchRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Aprovisionamiento masivo real de dispositivos en ThingsBoard por Tenant.
    - Resuelve de forma inversa TBTenant -> TBServer.
    - Despliega credenciales dinámicas en RAM (tenant.get_token()).
    - Itera sobre el lote enviando POST /api/device por cada dispositivo.
    - Auto-renueva tokens si intercepta HTTP 401.
    - Maneja fallos individuales sin interrumpir el lote completo.
    - Retorna un reporte detallado con total_received, successfully_created, failed y lista de errores.
    """
    tenant, server = await _resolve_tenant_and_server(tenant_id, current_user)
    client, token = await _get_authenticated_client(tenant, server)

    total_received = len(request.devices)
    created_devices: List[CreatedDeviceSummary] = []
    errors: List[DeviceProvisionErrorDetail] = []

    logger.info(
        f"[Provisioning] Iniciando lote de {total_received} dispositivos para Tenant '{tenant.name}' "
        f"en Servidor '{server.name}' ({server.base_url})..."
    )

    async with httpx.AsyncClient(timeout=client.timeout) as http_client:
        for idx, dev in enumerate(request.devices):
            payload: Dict[str, Any] = {
                "name": dev.name,
                "type": dev.type or "default",
            }
            if dev.label is not None:
                payload["label"] = dev.label
            if dev.additional_info:
                payload["additionalInfo"] = dev.additional_info
            if request.device_profile_id:
                payload["deviceProfileId"] = {
                    "id": request.device_profile_id,
                    "entityType": "DEVICE_PROFILE"
                }

            try:
                try:
                    created_data = await client.create_device(
                        device_payload=payload,
                        token=token,
                        client=http_client
                    )
                except httpx.HTTPStatusError as first_err:
                    if first_err.response.status_code == 401:
                        logger.warning(f"[Provisioning] HTTP 401 al crear '{dev.name}'. Renovando sesión de Tenant y reintentando...")
                        token = await _reauthenticate_tenant(tenant, client)
                        created_data = await client.create_device(
                            device_payload=payload,
                            token=token,
                            client=http_client
                        )
                    else:
                        raise first_err

                dev_id_info = created_data.get("id") or created_data.get("entityId") or {}
                if isinstance(dev_id_info, dict):
                    dev_id = dev_id_info.get("id") or dev_id_info.get("entityId") or ""
                elif isinstance(dev_id_info, str):
                    dev_id = dev_id_info
                else:
                    dev_id = str(dev_id_info or "")

                created_devices.append(
                    CreatedDeviceSummary(
                        id=dev_id or dev.name,
                        name=created_data.get("name") or dev.name,
                        type=created_data.get("type") or dev.type,
                        label=created_data.get("label") or dev.label
                    )
                )
                logger.info(f"[Provisioning] [{idx + 1}/{total_received}] Dispositivo '{dev.name}' creado exitosamente (ID: {dev_id}).")

            except httpx.HTTPStatusError as http_err:
                status_code = http_err.response.status_code
                error_body = http_err.response.text
                err_msg = f"HTTP {status_code}: {error_body}"
                logger.warning(f"[Provisioning] [{idx + 1}/{total_received}] Error creando dispositivo '{dev.name}': {err_msg}")
                errors.append(
                    DeviceProvisionErrorDetail(
                        name=dev.name,
                        index=idx,
                        status_code=status_code,
                        error=err_msg
                    )
                )
            except Exception as exc:
                err_msg = f"Error inesperado: {str(exc)}"
                logger.error(f"[Provisioning] [{idx + 1}/{total_received}] Excepción creando dispositivo '{dev.name}': {err_msg}")
                errors.append(
                    DeviceProvisionErrorDetail(
                        name=dev.name,
                        index=idx,
                        status_code=None,
                        error=err_msg
                    )
                )

    success_count = len(created_devices)
    failed_count = len(errors)

    if failed_count == 0:
        overall_status = "success"
        message = f"Se aprovisionaron exitosamente todos los {success_count} dispositivos."
    elif success_count > 0:
        overall_status = "partial_success"
        message = f"Aprovisionamiento parcial: {success_count} creados con éxito, {failed_count} con error."
    else:
        overall_status = "failed"
        message = f"Falló el aprovisionamiento de todos los {failed_count} dispositivos."

    logger.info(f"[Provisioning] Finalizado lote para Tenant '{tenant.name}': {message}")

    return DeviceProvisionBatchResponse(
        status=overall_status,
        tenant_id=str(tenant.id),
        tenant_name=tenant.name,
        server_id=str(server.id),
        server_name=server.name,
        total_received=total_received,
        successfully_created=success_count,
        failed=failed_count,
        created_devices=created_devices,
        errors=errors,
        message=message
    )
