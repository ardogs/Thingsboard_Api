from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional, Tuple

import httpx
import redis.asyncio as redis
from beanie import PydanticObjectId

from core.logger import get_logger
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.redis_client import get_redis_client
from core.tb_client import ThingsBoardClient

logger = get_logger("hierarchical_suppression")

# Constantes de configuración y protección de concurrencia
RELATION_CACHE_TTL = 300       # TTL 5m para mapeo Sensor -> Gateway
GW_STATUS_CACHE_TTL = 45       # TTL 45s (30s-60s) para estado del Gateway
SINGLEFLIGHT_LOCK_TTL = 5      # TTL 5s para mutex de deduplicación de consultas
MAX_POLL_ITERATIONS = 30       # 30 iteraciones x 0.05s = 1.5s máximo de espera
POLL_INTERVAL_SECONDS = 0.05   # Intervalo de sondeo en SingleFlight

# Semáforo a nivel de módulo para proteger ThingsBoard de agotamiento de sockets
TB_HTTP_SEMAPHORE = asyncio.Semaphore(10)


async def check_parent_gateway_status(
    tenant_id: str,
    device_name: str,
    redis_conn: Optional[redis.Redis] = None,
    http_client: Optional[httpx.AsyncClient] = None,
) -> Tuple[bool, Optional[str], Optional[dict]]:
    """
    Evalúa de forma jerárquica y 100% asíncrona si el dispositivo sensor (Capa 4)
    pertenece a un IOTGateway padre (Capa 3) y si dicho gateway se encuentra inactivo.

    Mecanismos de protección Thundering Herd implementados:
    1. Sensor-to-Gateway Relation Cache: 'tb_parent_gw:{tenant_id}:{device_name}' (TTL 300s).
    2. Gateway Status Cache: 'tb_gw_status:{tenant_id}:{gateway_id}' (TTL 30s-60s).
    3. SingleFlight / Mutex Lock: 'tb_lock:check_gw:{tenant_id}:{target_key}' (SET NX EX 5)
       con sondeo no bloqueante (await asyncio.sleep(0.05) hasta 1.5s).
    4. HTTP Concurrency Semaphore: asyncio.Semaphore(10) para evitar saturación de sockets.
    5. Semántica Fail-Open: ante errores de red o ThingsBoard caído, retorna False para
       no suprimir alertas críticas indebidamente.

    Args:
        tenant_id: Identificador del Tenant (ObjectId o nombre).
        device_name: Nombre exacto del sensor reportado.
        redis_conn: Conexión Redis opcional reutilizable; fallback a get_redis_client().
        http_client: Cliente httpx.AsyncClient opcional reutilizable.

    Returns:
        Tuple[bool, Optional[str], Optional[dict]]:
        - is_parent_inactive: True si el IOTGateway padre existe y está caído/inactivo.
        - parent_gateway_name: Nombre del gateway padre si fue localizado, o None.
        - metadata: Diccionario con atributos y diagnóstico del gateway padre, o None.
    """
    effective_redis = redis_conn or get_redis_client()
    relation_cache_key = f"tb_parent_gw:{tenant_id}:{device_name}"

    gateway_id: Optional[str] = None
    gateway_name: Optional[str] = None
    lock_key: Optional[str] = None
    lock_acquired = False

    try:
        # ----------------------------------------------------------------------
        # PASO 1: Consulta de Caché de Relación (Sensor -> Gateway)
        # ----------------------------------------------------------------------
        cached_relation = await effective_redis.get(relation_cache_key)
        if isinstance(cached_relation, bytes):
            cached_relation = cached_relation.decode("utf-8")

        if cached_relation == "NONE":
            # Sensor independiente sin IOTGateway padre
            return False, None, None

        if cached_relation:
            try:
                rel_data = json.loads(cached_relation)
                gateway_id = rel_data.get("gateway_id")
                gateway_name = rel_data.get("gateway_name")
            except Exception as parse_err:
                logger.debug(f"[HierarchicalSuppression] Error parseando relación en caché: {parse_err}")

        # ----------------------------------------------------------------------
        # PASO 2: Consulta de Caché de Estado de Gateway (si ya conocemos el gateway_id)
        # ----------------------------------------------------------------------
        if gateway_id:
            status_cache_key = f"tb_gw_status:{tenant_id}:{gateway_id}"
            cached_status = await effective_redis.get(status_cache_key)
            if isinstance(cached_status, bytes):
                cached_status = cached_status.decode("utf-8")
            if cached_status:
                try:
                    st_data = json.loads(cached_status)
                    logger.debug(
                        f"[HierarchicalSuppression] Cache Hit estado de Gateway '{gateway_name}' ({gateway_id}): "
                        f"inactivo={st_data.get('is_inactive')}"
                    )
                    return (
                        bool(st_data.get("is_inactive")),
                        st_data.get("gateway_name", gateway_name),
                        st_data.get("metadata"),
                    )
                except Exception as parse_err:
                    logger.debug(f"[HierarchicalSuppression] Error parseando estado de gateway en caché: {parse_err}")

        # ----------------------------------------------------------------------
        # PASO 3: SingleFlight / Mutex Lock para evitar Thundering Herd
        # ----------------------------------------------------------------------
        target_key = gateway_id or device_name
        lock_key = f"tb_lock:check_gw:{tenant_id}:{target_key}"
        lock_acquired = bool(await effective_redis.set(lock_key, "1", nx=True, ex=SINGLEFLIGHT_LOCK_TTL))

        if not lock_acquired:
            # Otra corrutina/worker ya está consultando ThingsBoard. Sondeamos la caché.
            logger.debug(
                f"[HierarchicalSuppression] Mutex '{lock_key}' activo. Sondeando caché (SingleFlight)..."
            )
            for _ in range(MAX_POLL_ITERATIONS):
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                if not gateway_id:
                    polled_rel = await effective_redis.get(relation_cache_key)
                    if isinstance(polled_rel, bytes):
                        polled_rel = polled_rel.decode("utf-8")
                    if polled_rel == "NONE":
                        return False, None, None
                    elif polled_rel:
                        try:
                            p_data = json.loads(polled_rel)
                            gateway_id = p_data.get("gateway_id")
                            gateway_name = p_data.get("gateway_name")
                        except Exception:
                            pass

                if gateway_id:
                    polled_status = await effective_redis.get(f"tb_gw_status:{tenant_id}:{gateway_id}")
                    if isinstance(polled_status, bytes):
                        polled_status = polled_status.decode("utf-8")
                    if polled_status:
                        try:
                            s_data = json.loads(polled_status)
                            logger.debug(
                                f"[HierarchicalSuppression] Sondeo exitoso para gateway '{gateway_name}': "
                                f"inactivo={s_data.get('is_inactive')}"
                            )
                            return (
                                bool(s_data.get("is_inactive")),
                                s_data.get("gateway_name", gateway_name),
                                s_data.get("metadata"),
                            )
                        except Exception:
                            pass

            # Si el sondeo expira sin resultado, se continúa resolviendo bajo semáforo
            logger.debug(
                f"[HierarchicalSuppression] Sondeo agotado para '{device_name}'. Procediendo con resolución directa."
            )

        # ----------------------------------------------------------------------
        # PASO 4: Resolución en ThingsBoard bajo Semáforo de Concurrencia
        # ----------------------------------------------------------------------
        async with TB_HTTP_SEMAPHORE:
            # 4.1 Resolver Tenant y Servidor ThingsBoard en MongoDB
            tenant: Optional[TBTenant] = None
            try:
                obj_id = PydanticObjectId(tenant_id)
                tenant = await TBTenant.get(obj_id)
            except Exception:
                pass

            if not tenant:
                try:
                    tenant = await TBTenant.find_one({"name": tenant_id})
                except Exception:
                    pass
            if not tenant:
                try:
                    tenant = await TBTenant.find_one({"custom_metadata.tb_tenant_id": tenant_id})
                except Exception:
                    pass

            if not tenant:
                logger.warning(
                    f"[HierarchicalSuppression] No se encontró TBTenant para tenant_id='{tenant_id}'. Fail-open."
                )
                return False, None, {"fail_open": True, "reason": "tenant_not_found"}

            server = await tenant.get_server()
            if not server:
                logger.warning(
                    f"[HierarchicalSuppression] No se encontró TBServer para tenant '{tenant.name}'. Fail-open."
                )
                return False, None, {"fail_open": True, "reason": "server_not_found"}

            # 4.2 Inicializar cliente ThingsBoard
            tb_client = ThingsBoardClient(
                base_url=server.base_url,
                token=tenant.get_token(),
                username=tenant.username,
                password=tenant.get_password(),
            )
            token = tb_client.token
            if not token and tenant.username and tenant.get_password():
                login_data = await tb_client.login()
                if login_data and "token" in login_data:
                    token = login_data["token"]

            # 4.3 Si aún no tenemos gateway_id, buscar sensor y relaciones
            if not gateway_id:
                sensor_device = await tb_client.get_device_by_name(
                    device_name=device_name,
                    token=token,
                    client=http_client,
                )
                if not sensor_device:
                    logger.debug(
                        f"[HierarchicalSuppression] Dispositivo sensor '{device_name}' no encontrado en ThingsBoard."
                    )
                    await effective_redis.set(relation_cache_key, "NONE", ex=RELATION_CACHE_TTL)
                    return False, None, None

                sensor_id_raw = sensor_device.get("id")
                sensor_id = (
                    sensor_id_raw.get("id")
                    if isinstance(sensor_id_raw, dict)
                    else str(sensor_id_raw)
                )

                # Consultar relaciones entrantes (to_id=sensor_id, inward)
                inward_relations = await tb_client.get_entity_relations(
                    to_id=sensor_id,
                    to_type="DEVICE",
                    token=token,
                    client=http_client,
                )
                for rel in inward_relations:
                    from_ent = rel.get("from") or {}
                    if from_ent.get("entityType") == "DEVICE":
                        gateway_id = from_ent.get("id")
                        gateway_name = rel.get("fromName") or from_ent.get("name")
                        break

                # Si no se encontró en entrantes, consultar salientes (from_id=sensor_id, outward)
                if not gateway_id:
                    outward_relations = await tb_client.get_entity_relations(
                        from_id=sensor_id,
                        from_type="DEVICE",
                        token=token,
                        client=http_client,
                    )
                    for rel in outward_relations:
                        to_ent = rel.get("to") or {}
                        if to_ent.get("entityType") == "DEVICE":
                            gateway_id = to_ent.get("id")
                            gateway_name = rel.get("toName") or to_ent.get("name")
                            break

                # Si el sensor no tiene relaciones de tipo DEVICE padre
                if not gateway_id:
                    logger.debug(
                        f"[HierarchicalSuppression] Sensor '{device_name}' no posee IOTGateway padre. Almacenando NONE en caché."
                    )
                    await effective_redis.set(relation_cache_key, "NONE", ex=RELATION_CACHE_TTL)
                    return False, None, None

                # Si el nombre no vino en la relación, obtenerlo por id
                if not gateway_name:
                    gw_dev = await tb_client.get_device_by_id(
                        gateway_id,
                        token=token,
                        client=http_client,
                    )
                    gateway_name = gw_dev.get("name", gateway_id) if gw_dev else gateway_id

                # Guardar relación sensor -> gateway en Redis
                await effective_redis.set(
                    relation_cache_key,
                    json.dumps({"gateway_id": gateway_id, "gateway_name": gateway_name}),
                    ex=RELATION_CACHE_TTL,
                )

            # 4.3.1 Comprobar si el estado del Gateway ya fue resuelto y almacenado en caché por otra corrutina
            status_cache_key = f"tb_gw_status:{tenant_id}:{gateway_id}"
            cached_gw_st = await effective_redis.get(status_cache_key)
            if isinstance(cached_gw_st, bytes):
                cached_gw_st = cached_gw_st.decode("utf-8")
            if cached_gw_st:
                try:
                    s_data = json.loads(cached_gw_st)
                    return (
                        bool(s_data.get("is_inactive")),
                        s_data.get("gateway_name", gateway_name),
                        s_data.get("metadata"),
                    )
                except Exception:
                    pass

            # 4.4 Consultar atributos de estado del Gateway en ThingsBoard
            attributes = await tb_client.get_entity_attributes(
                entity_id=gateway_id,
                scope="SERVER_SCOPE",
                token=token,
                client=http_client,
            )

            attrs_dict: Dict[str, Any] = {}
            if isinstance(attributes, list):
                for item in attributes:
                    if isinstance(item, dict) and "key" in item:
                        attrs_dict[item["key"]] = item.get("value")
            elif isinstance(attributes, dict):
                attrs_dict = attributes

            active_attr = attrs_dict.get("active")
            status_attr = str(attrs_dict.get("status", "")).strip().upper()

            # Determinación de inactividad: active is False O status en ('OFFLINE', 'INACTIVE', 'DOWN', 'DEAD')
            is_parent_inactive = (active_attr is False) or (status_attr in ("OFFLINE", "INACTIVE", "DOWN", "DEAD"))

            metadata = {
                "gateway_id": gateway_id,
                "gateway_name": gateway_name,
                "active": active_attr,
                "status": status_attr,
                "attributes": attrs_dict,
            }

            # Guardar estado de Gateway en caché
            status_cache_key = f"tb_gw_status:{tenant_id}:{gateway_id}"
            await effective_redis.set(
                status_cache_key,
                json.dumps({
                    "is_inactive": is_parent_inactive,
                    "gateway_name": gateway_name,
                    "metadata": metadata,
                }),
                ex=GW_STATUS_CACHE_TTL,
            )

            logger.info(
                f"[HierarchicalSuppression] Evaluación de gateway '{gateway_name}' ({gateway_id}) para sensor '{device_name}': "
                f"inactivo={is_parent_inactive} [active={active_attr}, status='{status_attr}']"
            )

            return is_parent_inactive, gateway_name, metadata

    except Exception as exc:
        # Semántica Fail-Open: ante fallos de red o base de datos, no suprimimos la alerta
        logger.warning(
            f"[HierarchicalSuppression] Error evaluando estado de gateway padre para '{device_name}' (tenant: '{tenant_id}'): {exc}. "
            "Fail-open activado para garantizar entrega de alerta.",
            exc_info=False,
        )
        return False, None, {"fail_open": True, "error": str(exc)}

    finally:
        # Liberar mutex de SingleFlight si fue adquirido por esta instancia
        if lock_acquired and lock_key:
            try:
                await effective_redis.delete(lock_key)
            except Exception as del_err:
                logger.debug(f"[HierarchicalSuppression] Error liberando mutex '{lock_key}': {del_err}")
