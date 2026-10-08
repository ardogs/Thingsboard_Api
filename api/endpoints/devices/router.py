import asyncio
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
from api.deps import User, get_current_user, CasbinAuth

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


class DeviceRelationRequest(BaseModel):
    relation_type: Optional[str] = Field(default=None, min_length=1, description="Tipo de relación física/edge personalizada (ej: Edge_Link)")


class DeviceRelationResponse(BaseModel):
    status: str = Field(..., description="Estado de la operación: success")
    tenant_id: str = Field(..., description="ID del Tenant")
    parent_id: str = Field(..., description="ID del dispositivo padre (Edge / Gateway)")
    child_id: str = Field(..., description="ID del dispositivo hijo (Sensor / Subdispositivo)")
    relation_type: str = Field(..., description="Tipo de relación física/edge")
    message: str = Field(..., description="Descripción del resultado de la operación")


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
            user_id_str = str(current_user.id)
            is_allowed = (
                enforcer.enforce(user_id_str, tenant_domain, "devices", "read")
                or enforcer.enforce(user_id_str, tenant_domain, "telemetry", "read")
                or enforcer.enforce(user_id_str, tenant_domain, "telemetry", "write")
            )
            if not is_allowed and current_user.username:
                is_allowed = (
                    enforcer.enforce(current_user.username, tenant_domain, "devices", "read")
                    or enforcer.enforce(current_user.username, tenant_domain, "telemetry", "read")
                    or enforcer.enforce(current_user.username, tenant_domain, "telemetry", "write")
                )
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

    if not data_items:
        return []

    # Precargar dispositivos del Tenant para resolución rápida y enriquecimiento (name, type, label, additional_info)
    devices_lookup: Dict[str, dict] = {}
    try:
        try:
            devices_res = await client.get_tenant_devices(token=token, limit=1000)
        except httpx.HTTPStatusError as dev_http_err:
            if dev_http_err.response.status_code == 401:
                logger.warning(f"[Sites] HTTP 401 al precargar dispositivos de Tenant '{tenant.name}'. Re-autenticando...")
                token = await _reauthenticate_tenant(tenant, client)
                devices_res = await client.get_tenant_devices(token=token, limit=1000)
            else:
                raise dev_http_err

        if isinstance(devices_res, dict):
            for dev in devices_res.get("data", []):
                d_id_info = dev.get("id") or dev.get("entityId") or {}
                if isinstance(d_id_info, dict):
                    d_id = d_id_info.get("id") or d_id_info.get("entityId")
                elif isinstance(d_id_info, str):
                    d_id = d_id_info
                else:
                    d_id = None
                if d_id:
                    devices_lookup[str(d_id)] = dev
    except Exception as dev_err:
        logger.warning(f"[Sites] No se pudieron cargar los dispositivos del tenant para enriquecimiento ({dev_err}). Se continuará con datos de relaciones.")

    sem = asyncio.Semaphore(10)
    auth_lock = asyncio.Lock()

    def _clean_additional_info(info: Any) -> Optional[Dict[str, Any]]:
        return info if isinstance(info, dict) else None

    async def _safe_get_relations(
        http_client: httpx.AsyncClient,
        from_id: Optional[str] = None,
        from_type: Optional[str] = "ASSET",
        to_id: Optional[str] = None,
        to_type: Optional[str] = None
    ) -> list[dict]:
        nonlocal token
        try:
            return await client.get_entity_relations(
                from_id=from_id,
                from_type=from_type,
                to_id=to_id,
                to_type=to_type,
                token=token,
                client=http_client
            )
        except httpx.HTTPStatusError as http_err:
            if http_err.response.status_code == 401:
                failed_token = token
                async with auth_lock:
                    if token == failed_token:
                        logger.warning(f"[Sites] HTTP 401 al consultar relaciones en Tenant '{tenant.name}'. Renovando token...")
                        token = await _reauthenticate_tenant(tenant, client)
                try:
                    return await client.get_entity_relations(
                        from_id=from_id,
                        from_type=from_type,
                        to_id=to_id,
                        to_type=to_type,
                        token=token,
                        client=http_client
                    )
                except httpx.HTTPStatusError as retry_http_err:
                    if retry_http_err.response.status_code == 401:
                        logger.error(f"[Sites] Fallo 401 persistente contra ThingsBoard tras re-autenticar para Tenant '{tenant.name}'")
                        raise HTTPException(
                            status_code=status.HTTP_502_BAD_GATEWAY,
                            detail="Fallo de autenticación persistente contra ThingsBoard tras re-autenticar"
                        )
                    return []
                except HTTPException:
                    raise
                except Exception as retry_exc:
                    logger.warning(f"[Sites] Error en retry de relaciones ({retry_exc})")
                    return []
            return []
        except HTTPException:
            raise
        except Exception as exc:
            logger.warning(f"[Sites] Error consultando relaciones para {from_id or to_id}: {exc}")
            return []

    async def process_site(item: dict, http_client: httpx.AsyncClient) -> SiteWithDevicesResponse:
        # 1. Extracción robusta de ID (soporta entityId, id, dict, string o int)
        entity_id_info = item.get("entityId") if item.get("entityId") is not None else item.get("id")
        if isinstance(entity_id_info, dict):
            entity_id = entity_id_info.get("id") or entity_id_info.get("entityId")
            entity_type = entity_id_info.get("entityType", "ASSET")
        elif isinstance(entity_id_info, (str, int)):
            entity_id = str(entity_id_info)
            entity_type = "ASSET"
        else:
            entity_id = None
            entity_type = "ASSET"

        fallback_id = item.get("name") or str(id(item))
        site_id = str(entity_id) if entity_id is not None else str(fallback_id)

        # 2. Extracción robusta de campos (latest.ENTITY_FIELD o raíz)
        latest_fields = item.get("latest", {})
        entity_fields = latest_fields.get("ENTITY_FIELD", {}) if isinstance(latest_fields, dict) else {}

        name_obj = entity_fields.get("name") if isinstance(entity_fields, dict) else None
        name_val = name_obj.get("value") if isinstance(name_obj, dict) else name_obj
        name = name_val or item.get("name") or site_id

        type_obj = entity_fields.get("type") if isinstance(entity_fields, dict) else None
        type_val = type_obj.get("value") if isinstance(type_obj, dict) else type_obj
        asset_type = type_val or item.get("type") or entity_type or "ASSET"

        label_obj = entity_fields.get("label") if isinstance(entity_fields, dict) else None
        label_val = label_obj.get("value") if isinstance(label_obj, dict) else label_obj
        label = label_val or item.get("label")

        additional_info = _clean_additional_info(item.get("additionalInfo") or item.get("additional_info"))

        site_devices: List[DeviceSummary] = []
        seen_device_ids: set[str] = set()

        # 3. Consultar relaciones en ThingsBoard si tenemos ID real del activo
        if entity_id:
            async with sem:
                # Consulta principal: Relaciones salientes "Desde" (Asset -> Device)
                outward_relations = await _safe_get_relations(
                    http_client=http_client,
                    from_id=str(entity_id),
                    from_type="ASSET"
                )

                for rel in outward_relations:
                    if not isinstance(rel, dict):
                        continue
                    to_info = rel.get("to") if isinstance(rel.get("to"), dict) else {}
                    if to_info.get("entityType") == "DEVICE":
                        dev_id = to_info.get("id") or to_info.get("entityId")
                        if not dev_id:
                            continue
                        dev_id_str = str(dev_id)
                        if dev_id_str in seen_device_ids:
                            continue
                        seen_device_ids.add(dev_id_str)

                        if dev_id_str in devices_lookup:
                            dev_meta = devices_lookup[dev_id_str]
                            raw_dev_info = dev_meta.get("additionalInfo") or dev_meta.get("additional_info") or rel.get("additionalInfo")
                            site_devices.append(
                                DeviceSummary(
                                    id=dev_id_str,
                                    name=dev_meta.get("name") or rel.get("toName") or dev_id_str,
                                    type=dev_meta.get("type") or "default",
                                    label=dev_meta.get("label"),
                                    additional_info=_clean_additional_info(raw_dev_info)
                                )
                            )
                        else:
                            site_devices.append(
                                DeviceSummary(
                                    id=dev_id_str,
                                    name=rel.get("toName") or dev_id_str,
                                    type="default",
                                    label=None,
                                    additional_info=_clean_additional_info(rel.get("additionalInfo"))
                                )
                            )

                # Fallback secundario: si no hay relaciones salientes, verificar relaciones entrantes ("Hacia")
                if not outward_relations:
                    inward_relations = await _safe_get_relations(
                        http_client=http_client,
                        from_id=None,
                        from_type=None,
                        to_id=str(entity_id),
                        to_type="ASSET"
                    )
                    for rel in inward_relations:
                        if not isinstance(rel, dict):
                            continue
                        from_info = rel.get("from") if isinstance(rel.get("from"), dict) else {}
                        if from_info.get("entityType") == "DEVICE":
                            dev_id = from_info.get("id") or from_info.get("entityId")
                            if not dev_id:
                                continue
                            dev_id_str = str(dev_id)
                            if dev_id_str in seen_device_ids:
                                continue
                            seen_device_ids.add(dev_id_str)

                            if dev_id_str in devices_lookup:
                                dev_meta = devices_lookup[dev_id_str]
                                raw_dev_info = dev_meta.get("additionalInfo") or dev_meta.get("additional_info") or rel.get("additionalInfo")
                                site_devices.append(
                                    DeviceSummary(
                                        id=dev_id_str,
                                        name=dev_meta.get("name") or rel.get("fromName") or dev_id_str,
                                        type=dev_meta.get("type") or "default",
                                        label=dev_meta.get("label"),
                                        additional_info=_clean_additional_info(raw_dev_info)
                                    )
                                )
                            else:
                                site_devices.append(
                                    DeviceSummary(
                                        id=dev_id_str,
                                        name=rel.get("fromName") or dev_id_str,
                                        type="default",
                                        label=None,
                                        additional_info=_clean_additional_info(rel.get("additionalInfo"))
                                    )
                                )

        # 4. Compatibilidad retroactiva: mantener dispositivos embebidos en el payload (item.get("devices"))
        raw_devices = item.get("devices") or item.get("related_devices") or []
        for dev in raw_devices:
            if not isinstance(dev, dict):
                continue
            dev_id_info = dev.get("entityId") or dev.get("id") or {}
            if isinstance(dev_id_info, dict):
                dev_id = dev_id_info.get("id") or dev_id_info.get("entityId") or ""
            elif isinstance(dev_id_info, (str, int)):
                dev_id = str(dev_id_info)
            else:
                dev_id = str(dev_id_info or "")

            if not dev_id or str(dev_id) in seen_device_ids:
                continue
            dev_id_str = str(dev_id)
            seen_device_ids.add(dev_id_str)

            if dev_id_str in devices_lookup:
                dev_meta = devices_lookup[dev_id_str]
                raw_dev_info = dev_meta.get("additionalInfo") or dev_meta.get("additional_info") or dev.get("additionalInfo") or dev.get("additional_info")
                site_devices.append(
                    DeviceSummary(
                        id=dev_id_str,
                        name=dev_meta.get("name") or dev.get("name") or dev_id_str,
                        type=dev_meta.get("type") or dev.get("type", "default"),
                        label=dev_meta.get("label") or dev.get("label"),
                        additional_info=_clean_additional_info(raw_dev_info)
                    )
                )
            else:
                site_devices.append(
                    DeviceSummary(
                        id=dev_id_str,
                        name=dev.get("name") or dev_id_str,
                        type=dev.get("type", "default"),
                        label=dev.get("label"),
                        additional_info=_clean_additional_info(dev.get("additionalInfo") or dev.get("additional_info"))
                    )
                )

        return SiteWithDevicesResponse(
            id=site_id,
            name=str(name),
            type=str(asset_type),
            label=label,
            additional_info=additional_info,
            devices=site_devices
        )

    async with httpx.AsyncClient(timeout=client.timeout) as http_client:
        tasks = [process_site(item, http_client) for item in data_items]
        results = await asyncio.gather(*tasks)

    # Deduplicar sitios por id preservando el orden original
    seen_site_ids: set[str] = set()
    unique_sites: List[SiteWithDevicesResponse] = []
    for site in results:
        if site.id not in seen_site_ids:
            seen_site_ids.add(site.id)
            unique_sites.append(site)

    return unique_sites


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


# ==========================================
# Gestión de Relaciones Físicas (Topología Edge)
# ==========================================

@router.post(
    "/{tenant_id}/devices/{parent_id}/relations/{child_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=DeviceRelationResponse,
    summary="Crear relación física entre dispositivos (Topología Edge)"
)
async def create_device_physical_relation(
    tenant_id: str,
    parent_id: str,
    child_id: str,
    relation_type: Optional[str] = Query(None, description="Tipo de relación personalizada (ej: Edge_Link)"),
    request_data: Optional[DeviceRelationRequest] = None,
    current_user: User = Depends(CasbinAuth(obj="devices", act="write"))
) -> DeviceRelationResponse:
    """
    Crea una relación física/edge entre dos dispositivos en ThingsBoard.
    - parent_id: Dispositivo raíz/Gateway/Edge (from).
    - child_id: Dispositivo subordinado/Sensor (to).
    - relation_type: Obligatorio vía query string o payload JSON.
    """
    # 1. Extraer y validar relation_type obligatorio
    rel_type: Optional[str] = None
    if relation_type and relation_type.strip():
        rel_type = relation_type.strip()
    elif request_data and request_data.relation_type and request_data.relation_type.strip():
        rel_type = request_data.relation_type.strip()

    if not rel_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El parámetro 'relation_type' es obligatorio (por query string o payload JSON)."
        )

    # 2. Resolución Inversa de Tenant y Servidor
    tenant, server = await _resolve_tenant_and_server(tenant_id, current_user)
    client, token = await _get_authenticated_client(tenant, server)

    # 3. Construcción del Payload para ThingsBoard
    tb_payload = {
        "from": {
            "id": parent_id,
            "entityType": "DEVICE"
        },
        "to": {
            "id": child_id,
            "entityType": "DEVICE"
        },
        "type": rel_type,
        "typeGroup": "COMMON"
    }

    base_server_url = server.base_url.rstrip("/")
    primary_url = f"{base_server_url}/api/relations"
    fallback_url = f"{base_server_url}/api/relation"

    # 4. Despacho Asíncrono con HTTPX y Manejo de 401/404 Fallback
    async with httpx.AsyncClient(timeout=client.timeout) as http_client:
        async def _post_relation(target_url: str, auth_token: str) -> httpx.Response:
            headers = {
                "X-Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            }
            return await http_client.post(target_url, json=tb_payload, headers=headers)

        try:
            target_url = primary_url
            response = await _post_relation(target_url, token)

            # Si ThingsBoard responde 401, re-autenticar y reintentar una vez
            if response.status_code == 401:
                logger.warning(
                    f"[DeviceRelations] Token expirado (401) para Tenant '{tenant.name}'. "
                    "Re-autenticando y reintentando..."
                )
                token = await _reauthenticate_tenant(tenant, client)
                response = await _post_relation(target_url, token)

            # Fallback transparente si 404 en /api/relations -> intentar /api/relation
            if response.status_code == 404 and target_url == primary_url:
                logger.info(
                    f"[DeviceRelations] Endpoint {primary_url} retornó 404. "
                    f"Aplicando fallback a {fallback_url}..."
                )
                target_url = fallback_url
                response = await _post_relation(target_url, token)

                if response.status_code == 401:
                    logger.warning(
                        f"[DeviceRelations] Token expirado (401) en fallback para Tenant '{tenant.name}'. "
                        "Re-autenticando..."
                    )
                    token = await _reauthenticate_tenant(tenant, client)
                    response = await _post_relation(target_url, token)

            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            err_text = exc.response.text
            status_code = exc.response.status_code
            logger.error(
                f"[DeviceRelations] Error de ThingsBoard ({status_code}) al crear relación: {err_text}"
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error en ThingsBoard ({status_code}) al crear relación: {err_text}"
            )
        except Exception as exc:
            logger.error(f"[DeviceRelations] Error de conexión hacia ThingsBoard: {exc}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error de conexión hacia ThingsBoard: {str(exc)}"
            )

    logger.info(
        f"[DeviceRelations] Relación '{rel_type}' creada exitosamente entre '{parent_id}' y '{child_id}' (Tenant: '{tenant.name}')."
    )

    return DeviceRelationResponse(
        status="success",
        tenant_id=tenant_id,
        parent_id=parent_id,
        child_id=child_id,
        relation_type=rel_type,
        message=f"Relación '{rel_type}' creada exitosamente entre '{parent_id}' y '{child_id}'."
    )

