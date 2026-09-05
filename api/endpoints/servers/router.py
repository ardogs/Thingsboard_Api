from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query
from beanie import PydanticObjectId

from core.models.tb_server import TBServer, InstallationType, SSHAuthMethod
from core.models.tb_tenant import TBTenant
from core.models.tb_node import TBNode
from core.tb_client import ThingsBoardClient
from core.redis_client import redis_client
from core.logger import logger
from api.deps import User, get_current_user
from workers.tasks import get_server_lock_key
from api.endpoints.servers.schemas import (
    NodeCreateRequest,
    NodeUpdateRequest,
    NodeResponse,
    ServerCreateRequest,
    ServerUpdateRequest,
    ServerResponse,
    ServerStatusResponse,
    SystemInfoMetricPoint,
    ServerSystemInfoResponse,
    TenantCreateRequest,
    TenantUpdateRequest,
    ReportConfigRequest,
    TenantResponse,
)

router = APIRouter()


def _extract_metrics(info_data: Any) -> tuple[Optional[float], Optional[float], Optional[float]]:
    from core.services.system_info_service import _normalize_system_info
    norm = _normalize_system_info(info_data)
    return norm["cpu_usage"], norm["memory_usage"], norm["disc_usage"]


def _to_server_response(server: TBServer) -> ServerResponse:
    inst_type = server.installation_type or InstallationType.STANDALONE
    return ServerResponse(
        id=str(server.id),
        name=server.name,
        base_url=server.base_url,
        installation_type=inst_type,
        username=server.username,
        has_token=bool(server.encrypted_token),
        has_credentials=bool(server.username and server.encrypted_password),
        token=server.get_token(),
        refresh_token=server.get_refresh_token(),
        ssh_host=server.ssh_host,
        ssh_port=server.ssh_port or 22,
        ssh_username=server.ssh_username,
        ssh_auth_method=server.ssh_auth_method,
        has_ssh_password=bool(server.encrypted_ssh_password),
        has_ssh_pem_file=bool(server.encrypted_ssh_pem_file),
        has_ssh_passphrase=bool(server.encrypted_ssh_passphrase),
        description=server.description,
        rate_limit_rpm=server.rate_limit_rpm or 60,
        custom_metadata=server.custom_metadata or {},
        user_id=server.user_id,
        is_active=server.is_active,
        created_at=server.created_at,
        updated_at=server.updated_at
    )



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


def _get_node_server_ref_id(node: TBNode) -> str:
    if isinstance(node.server_id, TBServer):
        return str(node.server_id.id)
    ref = node.server_id.to_ref() if hasattr(node.server_id, "to_ref") else node.server_id
    ref_id = ref.id if hasattr(ref, "id") else ref
    return str(ref_id)


def _to_node_response(node: TBNode) -> NodeResponse:
    return NodeResponse(
        id=str(node.id),
        server_id=_get_node_server_ref_id(node),
        name=node.name,
        node_role=node.node_role,
        ssh_host=node.ssh_host,
        ip_address=node.ssh_host,
        ssh_port=node.ssh_port,
        ssh_username=node.ssh_username,
        ssh_auth_method=node.ssh_auth_method,
        has_ssh_password=bool(node.encrypted_ssh_password),
        has_ssh_pem_file=bool(node.encrypted_ssh_pem_file),
        has_ssh_passphrase=bool(node.encrypted_ssh_passphrase),
        description=node.description,
        is_active=node.is_active,
        created_at=node.created_at,
        updated_at=node.updated_at
    )


async def _resolve_node(node_id: str, server_id: str) -> TBNode:
    try:
        obj_id = PydanticObjectId(node_id)
        node = await TBNode.get(obj_id)
    except Exception:
        node = None

    if not node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Nodo con ID '{node_id}' no encontrado"
        )

    if _get_node_server_ref_id(node) != server_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"El nodo '{node_id}' no pertenece al servidor especificado '{server_id}'"
        )

    return node



async def _resolve_server(server_id: str, current_user: User) -> TBServer:
    try:
        obj_id = PydanticObjectId(server_id)
        server = await TBServer.get(obj_id)
    except Exception:
        server = None

    if not server:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Servidor ThingsBoard con ID '{server_id}' no encontrado"
        )

    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    if not is_admin and server.user_id not in [str(current_user.id), current_user.id]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tienes permisos para acceder a este servidor")

    return server


async def _resolve_tenant(tenant_id: str, current_user: User) -> TBTenant:
    try:
        obj_id = PydanticObjectId(tenant_id)
        tenant = await TBTenant.get(obj_id)
    except Exception:
        tenant = None

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant de ThingsBoard con ID '{tenant_id}' no encontrado"
        )

    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    if not is_admin and tenant.user_id not in [str(current_user.id), current_user.id]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tienes permisos para acceder a este tenant")

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
    Almacena credenciales de Sysadmin cifradas con Fernet y previene el arranque en frío.
    """
    clean_base_url = request.base_url.strip().rstrip("/")
    clean_username = request.username.strip() if request.username else None
    inst_type = request.installation_type
    server = TBServer(
        name=request.name.strip(),
        base_url=clean_base_url,
        installation_type=inst_type,
        username=clean_username,
        description=request.description,
        ssh_port=request.ssh_port or 22,
        ssh_username=request.ssh_username.strip() if request.ssh_username else None,
        ssh_auth_method=request.ssh_auth_method,
        rate_limit_rpm=request.rate_limit_rpm or 60,
        custom_metadata=request.custom_metadata or {},
        user_id=str(current_user.id),
        is_active=True
    )
    if request.password:
        server.set_password(request.password)

    if request.token or request.refresh_token:
        server.set_tokens(request.token, request.refresh_token)
    elif clean_username and request.password:
        # Prevenir arranque en frío: intentar autenticación inicial contra ThingsBoard
        try:
            tb_client = ThingsBoardClient(
                base_url=clean_base_url,
                username=clean_username,
                password=request.password
            )
            login_res = await tb_client.login()
            if login_res and "token" in login_res:
                server.set_tokens(login_res["token"], login_res.get("refreshToken"))
                logger.info(f"[Server Router] Arranque en frío prevenido para servidor '{server.name}': tokens de Sysadmin obtenidos y cifrados exitosamente.")
        except Exception as auth_exc:
            logger.warning(f"[Server Router] No se pudo obtener token inicial para '{server.name}' durante registro: {auth_exc}")

    # Asignar credenciales SSH del nodo principal cifradas
    if request.ssh_auth_method:
        server.set_ssh_credentials(
            ssh_password=request.ssh_password,
            ssh_pem_file=request.ssh_pem_file,
            ssh_passphrase=request.ssh_passphrase
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


@router.get("/overview", response_model=List[ServerSystemInfoResponse])
@router.get("/system-info/overview", response_model=List[ServerSystemInfoResponse])
async def get_all_servers_system_info_overview(
    live: bool = Query(default=False, description="Si es True, consulta en tiempo real cada servidor; si es False, retorna las últimas métricas guardadas"),
    current_user: User = Depends(get_current_user)
):
    """
    Retorna un resumen de métricas de uso de CPU, RAM y Disco para todos los servidores registrados.
    Permite al frontend alimentar tableros de control y gráficas de monitoreo en tiempo real o histórico reciente.
    """
    from core.services.system_info_service import collect_server_system_info

    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    if is_admin:
        servers = await TBServer.find({"is_active": {"$ne": False}}).to_list()
    else:
        servers = await TBServer.find(
            {"is_active": {"$ne": False}},
            {"$or": [{"user_id": str(current_user.id)}, {"user_id": current_user.id}]}
        ).to_list()


    responses = []
    for srv in servers:
        meta = srv.custom_metadata or {}
        raw_history = meta.get("system_info_history", [])
        history_pts = [SystemInfoMetricPoint(**h) for h in raw_history if isinstance(h, dict)]

        if live:
            res_live = await collect_server_system_info(srv)
            cpu, mem, disc = _extract_metrics(res_live.get("system_info"))
            # Recargar metadata tras collect_server_system_info
            updated_history = (srv.custom_metadata or {}).get("system_info_history", [])
            history_pts = [SystemInfoMetricPoint(**h) for h in updated_history if isinstance(h, dict)]
            responses.append(ServerSystemInfoResponse(
                server_id=str(srv.id),
                server_name=srv.name,
                base_url=srv.base_url,
                status=res_live.get("status", "HEALTHY"),
                collected_at=res_live.get("collected_at"),
                cpu_usage=cpu,
                memory_usage=mem,
                disc_usage=disc,
                history_count=len(history_pts),
                history=history_pts,
                raw_data=res_live.get("system_info"),
                error=res_live.get("error")
            ))
        else:
            saved_info = meta.get("last_system_info", {})
            raw_data = saved_info.get("data")
            cpu, mem, disc = _extract_metrics(raw_data)
            responses.append(ServerSystemInfoResponse(
                server_id=str(srv.id),
                server_name=srv.name,
                base_url=srv.base_url,
                status=saved_info.get("status", "UNCOLLECTED"),
                collected_at=saved_info.get("collected_at"),
                cpu_usage=cpu,
                memory_usage=mem,
                disc_usage=disc,
                history_count=len(history_pts),
                history=history_pts,
                raw_data=raw_data,
                error=saved_info.get("error")
            ))
    return responses


@router.get("/{server_id}/system-info", response_model=ServerSystemInfoResponse)
async def get_single_server_system_info(
    server_id: str,
    live: bool = Query(default=False, description="Si es True, consulta en tiempo real al servidor ThingsBoard; si es False, retorna la última métrica persistida"),
    current_user: User = Depends(get_current_user)
):
    """
    Retorna el estado de CPU, memoria RAM y almacenamiento en disco de un servidor ThingsBoard específico,
    incluyendo el buffer deslizante de los últimos 60 registros históricos (1 hora).
    """
    from core.services.system_info_service import collect_server_system_info

    server = await _resolve_server(server_id, current_user)
    if live:
        res_live = await collect_server_system_info(server)
        cpu, mem, disc = _extract_metrics(res_live.get("system_info"))
        updated_history = (server.custom_metadata or {}).get("system_info_history", [])
        history_pts = [SystemInfoMetricPoint(**h) for h in updated_history if isinstance(h, dict)]
        return ServerSystemInfoResponse(
            server_id=str(server.id),
            server_name=server.name,
            base_url=server.base_url,
            status=res_live.get("status", "HEALTHY"),
            collected_at=res_live.get("collected_at"),
            cpu_usage=cpu,
            memory_usage=mem,
            disc_usage=disc,
            history_count=len(history_pts),
            history=history_pts,
            raw_data=res_live.get("system_info"),
            error=res_live.get("error")
        )
    else:
        meta = server.custom_metadata or {}
        saved_info = meta.get("last_system_info", {})
        raw_data = saved_info.get("data")
        cpu, mem, disc = _extract_metrics(raw_data)
        raw_history = meta.get("system_info_history", [])
        history_pts = [SystemInfoMetricPoint(**h) for h in raw_history if isinstance(h, dict)]
        return ServerSystemInfoResponse(
            server_id=str(server.id),
            server_name=server.name,
            base_url=server.base_url,
            status=saved_info.get("status", "UNCOLLECTED"),
            collected_at=saved_info.get("collected_at"),
            cpu_usage=cpu,
            memory_usage=mem,
            disc_usage=disc,
            history_count=len(history_pts),
            history=history_pts,
            raw_data=raw_data,
            error=saved_info.get("error")
        )



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
    Actualiza la configuración, credenciales de Sysadmin o metadatos de un servidor ThingsBoard en MongoDB.
    """
    server = await _resolve_server(server_id, current_user)

    update_data = request.model_dump(exclude_unset=True)
    if "base_url" in update_data and update_data["base_url"]:
        update_data["base_url"] = update_data["base_url"].strip().rstrip("/")
    if "name" in update_data and update_data["name"]:
        update_data["name"] = update_data["name"].strip()
    if "username" in update_data:
        server.username = update_data["username"].strip() if update_data["username"] else None
    if "password" in update_data:
        server.set_password(update_data["password"])
    if "token" in update_data or "refresh_token" in update_data:
        current_tok = server.get_token()
        current_ref = server.get_refresh_token()
        new_tok = update_data.get("token", current_tok)
        new_ref = update_data.get("refresh_token", current_ref)
        server.set_tokens(new_tok, new_ref)

    for field in [
        "name", "base_url", "description", "installation_type",
        "ssh_port", "ssh_username", "ssh_auth_method",
        "rate_limit_rpm", "custom_metadata", "is_active"
    ]:
        if field in update_data and update_data[field] is not None:
            setattr(server, field, update_data[field])

    if "ssh_password" in update_data:
        server.set_ssh_password(update_data["ssh_password"])
    if "ssh_pem_file" in update_data:
        server.set_ssh_pem_file(update_data["ssh_pem_file"])
    if "ssh_passphrase" in update_data:
        server.set_ssh_passphrase(update_data["ssh_passphrase"])

    # Limpiar campos incompatibles si se actualizó el método de autenticación SSH
    if "ssh_auth_method" in update_data:
        if server.ssh_auth_method == SSHAuthMethod.PASSWORD:
            server.encrypted_ssh_pem_file = None
            server.encrypted_ssh_passphrase = None
        elif server.ssh_auth_method == SSHAuthMethod.PEM_KEY:
            server.encrypted_ssh_password = None

    server.updated_at = datetime.now(timezone.utc)
    await server.save()
    return _to_server_response(server)


@router.delete("/{server_id}")
async def delete_server(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Elimina un servidor ThingsBoard de MongoDB y sus tenants y nodos asociados.
    """
    server = await _resolve_server(server_id, current_user)
    
    # Eliminar tenants y nodos asociados en cascada
    await TBTenant.find(TBTenant.server_id == server.to_ref()).delete()
    await TBNode.find(TBNode.server_id == server.to_ref()).delete()
    await server.delete()
    return {"status": "ok", "message": f"Servidor '{server.name}', sus tenants y nodos asociados eliminados exitosamente"}


@router.post("/{server_id}/test-connection")
@router.get("/{server_id}/test-connection")
async def test_server_connection(
    server_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Prueba la conectividad y validación de autenticación de Sysadmin contra la instancia ThingsBoard.
    Si se obtienen nuevos tokens durante la prueba, se persisten cifrados en MongoDB.
    """
    server = await _resolve_server(server_id, current_user)
    client = ThingsBoardClient(
        base_url=server.base_url,
        token=server.get_token(),
        refresh_token=server.get_refresh_token(),
        username=server.username,
        password=server.get_password()
    )
    result = await client.test_connection()
    
    # Si se obtuvieron nuevos tokens durante la prueba de conexión, persistirlos cifrados
    if client.token and client.token != server.get_token():
        server.set_tokens(client.token, client.refresh_token)
        server.updated_at = datetime.now(timezone.utc)
        await server.save()

    result["server_name"] = server.name
    result["server_id"] = str(server.id)
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


@router.put("/{server_id}/tenants/{tenant_id}/report-config", response_model=TenantResponse)
async def update_tenant_report_config(
    server_id: str,
    tenant_id: str,
    request: ReportConfigRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Actualiza específicamente el diccionario report_config dentro del campo custom_metadata
    del documento TBTenant para definir las listas blancas de telemetría a exportar.
    """
    await _resolve_server(server_id, current_user)
    tenant = await _resolve_tenant(tenant_id, current_user)

    if _get_server_ref_id(tenant) != server_id:
        raise HTTPException(status_code=400, detail="El tenant no pertenece al servidor especificado")

    if tenant.custom_metadata is None:
        tenant.custom_metadata = {}

    tenant.custom_metadata["report_config"] = request.report_config
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


# ==========================================
# Endpoints: Nodos ThingsBoard (/api/v1/servers/{server_id}/nodes)
# Infraestructura SSH (Standalone / Cluster)
# ==========================================

@router.post("/{server_id}/nodes", response_model=NodeResponse, status_code=status.HTTP_201_CREATED)
async def create_server_node(
    server_id: str,
    request: NodeCreateRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Registra un nuevo nodo de infraestructura (SSH) secundario asociado a un servidor ThingsBoard padre.
    Aplica validación estricta de autenticación SSH (password o pem_key) y cifra las credenciales con Fernet en bytes.
    """
    server = await _resolve_server(server_id, current_user)

    target_host = request.ssh_host or request.ip_address
    node = TBNode(
        server_id=server.to_ref(),
        name=request.name,
        node_role=request.node_role,
        ssh_host=target_host.strip(),
        ssh_port=request.ssh_port,
        ssh_username=request.ssh_username.strip(),
        ssh_auth_method=request.ssh_auth_method,
        description=request.description,
        is_active=request.is_active
    )

    node.set_ssh_credentials(
        ssh_password=request.ssh_password,
        ssh_pem_file=request.ssh_pem_file,
        ssh_passphrase=request.ssh_passphrase
    )

    await node.insert()
    logger.info(f"[Node Router] Nodo '{node.ssh_host}' ({node.node_role}) registrado para el servidor '{server.name}'.")
    return _to_node_response(node)


@router.get("/{server_id}/nodes", response_model=List[NodeResponse])
async def list_server_nodes(
    server_id: str,
    node_role: Optional[str] = Query(default=None, description="Filtrar por rol funcional: 'worker', 'database', 'transport'"),
    is_active: Optional[bool] = Query(default=None, description="Filtrar por estado activo/inactivo"),
    current_user: User = Depends(get_current_user)
):
    """
    Lista todos los nodos de infraestructura subyacente registrados para un servidor ThingsBoard.
    Soporta filtros opcionales por rol funcional y estado operativo.
    """
    server = await _resolve_server(server_id, current_user)

    conditions = [TBNode.server_id == server.to_ref()]
    if node_role:
        conditions.append(TBNode.node_role == node_role)
    if is_active is not None:
        conditions.append(TBNode.is_active == is_active)

    nodes = await TBNode.find(*conditions).to_list()
    return [_to_node_response(n) for n in nodes]


@router.get("/{server_id}/nodes/{node_id}", response_model=NodeResponse)
async def get_server_node(
    server_id: str,
    node_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Obtiene el detalle seguro de un nodo específico perteneciente a un servidor ThingsBoard.
    No expone contraseñas ni llaves SSH en texto plano.
    """
    await _resolve_server(server_id, current_user)
    node = await _resolve_node(node_id, server_id)
    return _to_node_response(node)


@router.put("/{server_id}/nodes/{node_id}", response_model=NodeResponse)
async def update_server_node(
    server_id: str,
    node_id: str,
    request: NodeUpdateRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Actualiza la configuración, direccionamiento de red o credenciales SSH cifradas de un nodo de infraestructura.
    """
    await _resolve_server(server_id, current_user)
    node = await _resolve_node(node_id, server_id)

    update_data = request.model_dump(exclude_unset=True)
    if "name" in update_data:
        node.name = update_data["name"]
    if "node_role" in update_data and update_data["node_role"]:
        node.node_role = update_data["node_role"]
    if "ssh_host" in update_data and update_data["ssh_host"]:
        node.ssh_host = update_data["ssh_host"].strip()
    elif "ip_address" in update_data and update_data["ip_address"]:
        node.ssh_host = update_data["ip_address"].strip()
    if "ssh_port" in update_data and update_data["ssh_port"] is not None:
        node.ssh_port = update_data["ssh_port"]
    if "ssh_username" in update_data and update_data["ssh_username"]:
        node.ssh_username = update_data["ssh_username"].strip()
    if "ssh_auth_method" in update_data and update_data["ssh_auth_method"] is not None:
        node.ssh_auth_method = update_data["ssh_auth_method"]

    if "ssh_password" in update_data:
        node.set_ssh_password(update_data["ssh_password"])
    if "ssh_pem_file" in update_data:
        node.set_ssh_pem_file(update_data["ssh_pem_file"])
    if "ssh_passphrase" in update_data:
        node.set_ssh_passphrase(update_data["ssh_passphrase"])

    # Limpiar campos incompatibles si se actualizó el método
    if "ssh_auth_method" in update_data:
        if node.ssh_auth_method == SSHAuthMethod.PASSWORD:
            node.encrypted_ssh_pem_file = None
            node.encrypted_ssh_passphrase = None
        elif node.ssh_auth_method == SSHAuthMethod.PEM_KEY:
            node.encrypted_ssh_password = None

    if "description" in update_data:
        node.description = update_data["description"]
    if "is_active" in update_data and update_data["is_active"] is not None:
        node.is_active = update_data["is_active"]

    node.updated_at = datetime.now(timezone.utc)
    await node.save()
    logger.info(f"[Node Router] Nodo '{node.ssh_host}' ({node.id}) actualizado exitosamente.")
    return _to_node_response(node)


@router.delete("/{server_id}/nodes/{node_id}")
async def delete_server_node(
    server_id: str,
    node_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Elimina un nodo de infraestructura de ThingsBoard registrado en MongoDB.
    """
    await _resolve_server(server_id, current_user)
    node = await _resolve_node(node_id, server_id)

    await node.delete()
    logger.info(f"[Node Router] Nodo '{node.ssh_host}' ({node_id}) eliminado exitosamente.")
    return {
        "status": "ok",
        "message": f"Nodo '{node.ssh_host}' ({node.node_role}) eliminado exitosamente"
    }
