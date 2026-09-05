import asyncio
import httpx
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple, Union
from beanie import PydanticObjectId

from core.models.tb_server import TBServer
from core.tb_client import ThingsBoardClient
from core.logger import logger

# Límite máximo de registros históricos en la ventana deslizante (1 hora con recolección cada 1 minuto)
MAX_SYSTEM_INFO_HISTORY: int = 60


def _normalize_system_info(info_data: Any) -> dict:
    """
    Normaliza de forma robusta la respuesta de ThingsBoard GET /api/admin/systemInfo,
    la cual puede ser una lista de nodos (List[dict]) o un diccionario con/sin 'systemData'.
    """
    empty_result = {
        "cpu_usage": None,
        "memory_usage": None,
        "disc_usage": None,
        "total_memory": None,
        "free_memory": None,
        "total_disc": None,
        "free_disc": None,
    }
    if not info_data:
        return empty_result

    # 1. Extraer lista de nodos según el formato recibido
    nodes: List[dict] = []
    if isinstance(info_data, list):
        nodes = [item for item in info_data if isinstance(item, dict)]
    elif isinstance(info_data, dict):
        sys_data = info_data.get("systemData")
        if isinstance(sys_data, list):
            nodes = [item for item in sys_data if isinstance(item, dict)]
        elif isinstance(sys_data, dict):
            nodes = [sys_data]
        else:
            nodes = [info_data]

    if not nodes:
        return empty_result

    def _to_float(v) -> Optional[float]:
        try:
            return round(float(v), 2) if v is not None else None
        except Exception:
            return None

    def _to_int(v) -> Optional[int]:
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    # Caso 2: Un solo nodo (ThingsBoard Monolith / Standalone estándar)
    if len(nodes) == 1:
        node = nodes[0]
        cpu = node.get("cpuUsage") if node.get("cpuUsage") is not None else node.get("cpu_usage")
        mem = node.get("memoryUsage") if node.get("memoryUsage") is not None else node.get("memory_usage")
        disc = node.get("discUsage") if node.get("discUsage") is not None else (
            node.get("diskUsage") if node.get("diskUsage") is not None else (
                node.get("disc_usage") if node.get("disc_usage") is not None else node.get("disk_usage")
            )
        )
        return {
            "cpu_usage": _to_float(cpu),
            "memory_usage": _to_float(mem),
            "disc_usage": _to_float(disc),
            "total_memory": _to_int(node.get("totalMemory")),
            "free_memory": _to_int(node.get("freeMemory")),
            "total_disc": _to_int(node.get("totalDisc")),
            "free_disc": _to_int(node.get("freeDisc")),
        }

    # Caso 3: Múltiples nodos en cluster (ej: tb-core, tb-rule-engine)
    cpus, mems, discs = [], [], []
    total_mem = 0
    free_mem = 0
    total_disc = 0
    free_disc = 0
    has_mem = False
    has_disc = False

    for node in nodes:
        c = node.get("cpuUsage") if node.get("cpuUsage") is not None else node.get("cpu_usage")
        m = node.get("memoryUsage") if node.get("memoryUsage") is not None else node.get("memory_usage")
        d = node.get("discUsage") if node.get("discUsage") is not None else (
            node.get("diskUsage") if node.get("diskUsage") is not None else (
                node.get("disc_usage") if node.get("disc_usage") is not None else node.get("disk_usage")
            )
        )
        if c is not None:
            try: cpus.append(float(c))
            except Exception: pass
        if m is not None:
            try: mems.append(float(m))
            except Exception: pass
        if d is not None:
            try: discs.append(float(d))
            except Exception: pass

        if node.get("totalMemory") is not None:
            try:
                total_mem += int(node["totalMemory"])
                free_mem += int(node.get("freeMemory", 0))
                has_mem = True
            except Exception: pass

        if node.get("totalDisc") is not None:
            try:
                total_disc += int(node["totalDisc"])
                free_disc += int(node.get("freeDisc", 0))
                has_disc = True
            except Exception: pass

    avg_cpu = round(sum(cpus) / len(cpus), 2) if cpus else None
    avg_mem = round(sum(mems) / len(mems), 2) if mems else None
    avg_disc = round(sum(discs) / len(discs), 2) if discs else None

    return {
        "cpu_usage": avg_cpu,
        "memory_usage": avg_mem,
        "disc_usage": avg_disc,
        "total_memory": total_mem if has_mem else None,
        "free_memory": free_mem if has_mem else None,
        "total_disc": total_disc if has_disc else None,
        "free_disc": free_disc if has_disc else None,
    }


def _build_metric_point(
    raw_info: Any,
    status: str,
    collected_at: str,
    error: Optional[str] = None
) -> dict:
    """Construye un registro ligero de telemetría para el buffer histórico deslizante."""
    norm = _normalize_system_info(raw_info)
    return {
        "timestamp": collected_at,
        "status": status,
        "cpu_usage": norm["cpu_usage"],
        "memory_usage": norm["memory_usage"],
        "disc_usage": norm["disc_usage"],
        "total_memory": norm["total_memory"],
        "free_memory": norm["free_memory"],
        "total_disc": norm["total_disc"],
        "free_disc": norm["free_disc"],
        "error": error
    }


async def refresh_server_tokens_in_db(
    server_id: str,
    tb: ThingsBoardClient,
    token_ref: list,
    payload: Optional[dict] = None
) -> Tuple[str, Optional[str]]:
    """
    Manejo resiliente de auto-renovación de tokens JWT de Sysadmin ante errores HTTP 401 o arranque en frío:
    a) Intenta renovar el token usando el refresh_token almacenado en el cliente/documento TBServer.
    b) Si el refresh_token expiró o falla, realiza login completo usando username y password de Sysadmin en TBServer.
    c) Persiste asíncronamente los nuevos tokens cifrados con Fernet en el documento TBServer en MongoDB.
    d) Actualiza la referencia en memoria RAM para reintentar peticiones sin abortar la tarea.
    """
    refresh_token = (payload.get("refresh_token") if payload else None) or tb.refresh_token
    new_token: Optional[str] = None
    new_refresh_token: Optional[str] = None

    # a) Intentar renovar con refresh_token
    if refresh_token:
        logger.warning(f"[System Info Service] HTTP 401 interceptado en servidor '{tb.base_url}'. Intentando renovación mediante refresh_token...")
        try:
            new_tokens = await tb.refresh_jwt_token(refresh_token)
            new_token = new_tokens.get("token")
            new_refresh_token = new_tokens.get("refreshToken", refresh_token)
            logger.info(f"[System Info Service] Token de Sysadmin renovado exitosamente con refresh_token para servidor '{tb.base_url}'.")
        except Exception as e:
            logger.warning(f"[System Info Service] Falló renovación con refresh_token ({e}). Procediendo con fallback de login de Sysadmin...")

    # b) Si refresh_token falló o no existe, hacer login con credenciales de Sysadmin
    if not new_token and tb.username and tb.password:
        logger.warning(f"[System Info Service] Ejecutando login completo con credenciales de Sysadmin en {tb.base_url}...")
        try:
            login_res = await tb.login(tb.username, tb.password)
            if login_res and "token" in login_res:
                new_token = login_res.get("token")
                new_refresh_token = login_res.get("refreshToken")
                logger.info(f"[System Info Service] Re-autenticación exitosa mediante login de Sysadmin en {tb.base_url}.")
        except Exception as e:
            logger.error(f"[System Info Service] Falló re-autenticación de Sysadmin en {tb.base_url}: {e}")

    if not new_token:
        raise ValueError(
            f"No se pudo autenticar ni renovar tokens de Sysadmin para el servidor (ID: {server_id}) en {tb.base_url}."
        )

    # Actualizar estado en memoria
    token_ref[0] = new_token
    tb.token = new_token
    if new_refresh_token:
        tb.refresh_token = new_refresh_token
        if payload is not None:
            payload["refresh_token"] = new_refresh_token
    if payload is not None:
        payload["token"] = new_token

    # c) Persistir tokens actualizados en MongoDB (TBServer) cifrados con Fernet
    if server_id:
        try:
            try:
                obj_id = PydanticObjectId(server_id)
                server_doc = await TBServer.get(obj_id)
            except Exception:
                server_doc = await TBServer.get(server_id)

            if server_doc:
                server_doc.set_tokens(new_token, new_refresh_token)
                server_doc.updated_at = datetime.now(timezone.utc)
                await server_doc.save()
                logger.info(f"[MongoDB] Documento TBServer '{server_doc.name}' ({server_id}) actualizado en MongoDB con nuevos tokens cifrados.")
        except Exception as e:
            logger.error(f"[MongoDB] Error actualizando tokens en TBServer ({server_id}): {e}")

    return new_token, new_refresh_token


async def _persist_server_system_info(
    server: TBServer,
    info_payload: dict,
    metric_point: Optional[dict] = None
) -> None:
    """
    Persiste de forma atómica el payload de system_info y mantiene el buffer deslizante FIFO
    de los últimos 60 registros en custom_metadata.system_info_history.
    """
    try:
        meta = dict(server.custom_metadata or {})
        meta["last_system_info"] = info_payload

        # Gestión del buffer deslizante (FIFO) de 60 registros
        if metric_point is not None:
            history = list(meta.get("system_info_history") or [])
            history.append(metric_point)
            # Conservar estrictamente los últimos 60 registros (1 por delante, elimina el más viejo)
            if len(history) > MAX_SYSTEM_INFO_HISTORY:
                history = history[-MAX_SYSTEM_INFO_HISTORY:]
            meta["system_info_history"] = history

        server.custom_metadata = meta
        server.updated_at = datetime.now(timezone.utc)
        await server.save()
        logger.info(
            f"[System Info Service] Métricas persistidas para '{server.name}' "
            f"(Historial acumulado: {len(meta.get('system_info_history', []))}/{MAX_SYSTEM_INFO_HISTORY})."
        )
    except Exception as e:
        logger.error(f"[System Info Service] Error persistiendo system_info para '{server.name}': {e}")


async def collect_server_system_info(server: TBServer) -> dict:
    """
    Recolecta la información de telemetría de sistema (CPU, RAM, Disco) de un servidor ThingsBoard
    mediante GET /api/admin/systemInfo, gestionando arranque en frío y renovación de tokens.
    Persiste siempre el resultado en custom_metadata.last_system_info y en custom_metadata.system_info_history.
    """
    server_id = str(server.id)
    now_iso = datetime.now(timezone.utc).isoformat()
    plain_token = server.get_token()
    plain_refresh_token = server.get_refresh_token()
    plain_password = server.get_password()

    tb_client = ThingsBoardClient(
        base_url=server.base_url,
        token=plain_token,
        refresh_token=plain_refresh_token,
        username=server.username,
        password=plain_password
    )

    # 1. Arranque en frío si no hay token inicial
    if not plain_token or not str(plain_token).strip():
        if not server.username or not plain_password:
            error_msg = f"El servidor '{server.name}' no tiene tokens iniciales ni credenciales de Sysadmin (username/password) configuradas."
            logger.error(f"[System Info Service] {error_msg}")
            err_payload = {
                "status": "ERROR",
                "collected_at": now_iso,
                "error": error_msg
            }
            metric_pt = _build_metric_point(None, status="ERROR", collected_at=now_iso, error=error_msg)
            await _persist_server_system_info(server, err_payload, metric_pt)
            return {
                "server_id": server_id,
                "server_name": server.name,
                "base_url": server.base_url,
                "status": "ERROR",
                "error": error_msg,
                "collected_at": now_iso
            }

        logger.info(f"[System Info Service] Arranque en frío detectado para Sysadmin en '{server.name}'. Iniciando sesión en {server.base_url}...")
        try:
            login_res = await tb_client.login(server.username, plain_password)
            if not login_res or "token" not in login_res:
                raise ValueError(f"Credenciales de Sysadmin inválidas o servidor '{server.base_url}' no respondió con token.")
            plain_token = login_res["token"]
            plain_refresh_token = login_res.get("refreshToken")
            server.set_tokens(plain_token, plain_refresh_token)
            server.updated_at = datetime.now(timezone.utc)
            await server.save()
            logger.info(f"[MongoDB] Tokens de Sysadmin cifrados y persistidos para TBServer '{server.name}'.")
        except Exception as login_err:
            error_msg = f"Fallo al autenticar Sysadmin en arranque en frío para servidor '{server.name}': {login_err}"
            logger.error(f"[System Info Service] {error_msg}")
            err_payload = {
                "status": "ERROR",
                "collected_at": now_iso,
                "error": error_msg
            }
            metric_pt = _build_metric_point(None, status="ERROR", collected_at=now_iso, error=error_msg)
            await _persist_server_system_info(server, err_payload, metric_pt)
            return {
                "server_id": server_id,
                "server_name": server.name,
                "base_url": server.base_url,
                "status": "ERROR",
                "error": error_msg,
                "collected_at": now_iso
            }

    # 2. Consultar GET /api/admin/systemInfo con manejo de 401 y 403
    token_ref = [plain_token]
    raw_info = None

    try:
        raw_info = await tb_client.get_system_info(token=token_ref[0])
    except httpx.HTTPStatusError as http_err:
        if http_err.response.status_code == 401:
            logger.warning(f"[System Info Service] Recibido 401 al consultar systemInfo en '{server.name}'. Intentando renovación de tokens...")
            try:
                new_t, _ = await refresh_server_tokens_in_db(
                    server_id=server_id,
                    tb=tb_client,
                    token_ref=token_ref,
                    payload={}
                )
                raw_info = await tb_client.get_system_info(token=new_t)
            except Exception as ref_exc:
                error_msg = f"Error tras renovar tokens para servidor '{server.name}': {ref_exc}"
                logger.error(f"[System Info Service] {error_msg}")
                err_payload = {
                    "status": "ERROR",
                    "collected_at": now_iso,
                    "error": error_msg
                }
                metric_pt = _build_metric_point(None, status="ERROR", collected_at=now_iso, error=error_msg)
                await _persist_server_system_info(server, err_payload, metric_pt)
                return {
                    "server_id": server_id,
                    "server_name": server.name,
                    "base_url": server.base_url,
                    "status": "ERROR",
                    "error": error_msg,
                    "collected_at": now_iso
                }
        elif http_err.response.status_code == 403:
            error_msg = f"HTTP 403 Forbidden: La cuenta configurada para '{server.name}' no tiene rol Sysadmin en ThingsBoard (se requiere cuenta Sysadmin para /api/admin/systemInfo)."
            logger.error(f"[System Info Service] {error_msg}")
            err_payload = {
                "status": "ERROR",
                "collected_at": now_iso,
                "error": error_msg
            }
            metric_pt = _build_metric_point(None, status="ERROR", collected_at=now_iso, error=error_msg)
            await _persist_server_system_info(server, err_payload, metric_pt)
            return {
                "server_id": server_id,
                "server_name": server.name,
                "base_url": server.base_url,
                "status": "ERROR",
                "error": error_msg,
                "collected_at": now_iso
            }
        else:
            error_msg = f"Error HTTP ({http_err.response.status_code}) consultando systemInfo en '{server.name}': {http_err}"
            logger.error(f"[System Info Service] {error_msg}")
            err_payload = {
                "status": "ERROR",
                "collected_at": now_iso,
                "error": error_msg
            }
            metric_pt = _build_metric_point(None, status="ERROR", collected_at=now_iso, error=error_msg)
            await _persist_server_system_info(server, err_payload, metric_pt)
            return {
                "server_id": server_id,
                "server_name": server.name,
                "base_url": server.base_url,
                "status": "ERROR",
                "error": error_msg,
                "collected_at": now_iso
            }
    except Exception as exc:
        error_msg = f"Error de conectividad/red consultando systemInfo en '{server.name}': {exc}"
        logger.error(f"[System Info Service] {error_msg}")
        err_payload = {
            "status": "ERROR",
            "collected_at": now_iso,
            "error": error_msg
        }
        metric_pt = _build_metric_point(None, status="ERROR", collected_at=now_iso, error=error_msg)
        await _persist_server_system_info(server, err_payload, metric_pt)
        return {
            "server_id": server_id,
            "server_name": server.name,
            "base_url": server.base_url,
            "status": "ERROR",
            "error": error_msg,
            "collected_at": now_iso
        }

    # 3. Guardar system_info exitoso en custom_metadata y buffer deslizante
    success_payload = {
        "status": "HEALTHY",
        "collected_at": now_iso,
        "data": raw_info
    }
    metric_pt = _build_metric_point(raw_info, status="HEALTHY", collected_at=now_iso)
    await _persist_server_system_info(server, success_payload, metric_pt)

    return {
        "server_id": server_id,
        "server_name": server.name,
        "base_url": server.base_url,
        "status": "SUCCESS",
        "system_info": raw_info,
        "metric_point": metric_pt,
        "collected_at": now_iso
    }


async def collect_all_servers_system_info(server_id: Optional[str] = None) -> dict:
    """
    Orquesta la recolección de métricas de sistema (CPU, RAM, Disco) para:
    - Un servidor específico si se provee server_id.
    - Todos los servidores ThingsBoard registrados y activos si server_id es None o 'all'.
    """
    now_utc = datetime.now(timezone.utc)
    logger.info(f"[System Info Service] Iniciando recolección de systemInfo a las {now_utc.isoformat()} (server_id: {server_id})...")

    if server_id and str(server_id).strip() and str(server_id).lower() != "all":
        try:
            obj_id = PydanticObjectId(server_id)
            server = await TBServer.get(obj_id)
        except Exception:
            server = await TBServer.get(server_id)

        if not server:
            error_msg = f"No se encontró el servidor ThingsBoard con ID '{server_id}'"
            logger.error(f"[System Info Service] {error_msg}")
            return {
                "status": "ERROR",
                "error": error_msg,
                "executed_at": now_utc.isoformat(),
                "servers": []
            }
        servers = [server]
    else:
        servers = await TBServer.find({"is_active": {"$ne": False}}).to_list()

    if not servers:
        logger.warning("[System Info Service] No hay servidores ThingsBoard activos registrados en MongoDB.")
        return {
            "status": "SUCCESS",
            "total_servers": 0,
            "successful": 0,
            "failed": 0,
            "servers": [],
            "executed_at": now_utc.isoformat()
        }

    results = []
    successful = 0
    failed = 0

    for srv in servers:
        res = await collect_server_system_info(srv)
        results.append(res)
        if res.get("status") == "SUCCESS":
            successful += 1
        else:
            failed += 1

    logger.info(
        f"[System Info Service] Recolección completada: Total={len(servers)}, "
        f"Exitosos={successful}, Fallidos={failed}."
    )

    return {
        "status": "SUCCESS" if failed == 0 else ("PARTIAL_SUCCESS" if successful > 0 else "ERROR"),
        "total_servers": len(servers),
        "successful": successful,
        "failed": failed,
        "servers": results,
        "executed_at": now_utc.isoformat()
    }
