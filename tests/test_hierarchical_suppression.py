import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from core.services.hierarchical_suppression_service import (
    check_parent_gateway_status,
    RELATION_CACHE_TTL,
    GW_STATUS_CACHE_TTL,
)
from workers.tasks import send_telegram_alert_task


class MockRedis:
    """In-memory async Redis mock supporting get, set, delete, and TTL tracking."""
    def __init__(self):
        self.store = {}
        self.ttls = {}

    async def set(self, key: str, value: str, nx: bool = False, ex: int = None):
        if nx and key in self.store:
            return None
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex
        return True

    async def get(self, key: str):
        return self.store.get(key)

    async def delete(self, key: str):
        existed = key in self.store
        self.store.pop(key, None)
        self.ttls.pop(key, None)
        return 1 if existed else 0


@pytest.mark.asyncio
async def test_check_parent_gateway_cached_none():
    """Valida que si la relación en caché es 'NONE', retorna False sin consultar ThingsBoard."""
    mock_redis = MockRedis()
    tenant_id = "tenant_123"
    device_name = "Sensor_Temp_01"

    # Pre-cargar en caché que el sensor no tiene padre
    await mock_redis.set(f"tb_parent_gw:{tenant_id}:{device_name}", "NONE", ex=300)

    is_inactive, parent_name, meta = await check_parent_gateway_status(
        tenant_id=tenant_id,
        device_name=device_name,
        redis_conn=mock_redis,
    )
    assert is_inactive is False
    assert parent_name is None
    assert meta is None


@pytest.mark.asyncio
async def test_check_parent_gateway_cache_hit_status():
    """Valida que si el estado del gateway ya está en caché, no se realiza ninguna petición de red."""
    mock_redis = MockRedis()
    tenant_id = "tenant_123"
    device_name = "Sensor_Temp_02"
    gw_id = "gw_uuid_999"
    gw_name = "IOTGateway_Central"

    # Pre-cargar relación sensor -> gateway
    await mock_redis.set(
        f"tb_parent_gw:{tenant_id}:{device_name}",
        json.dumps({"gateway_id": gw_id, "gateway_name": gw_name}),
        ex=300
    )

    # Pre-cargar estado del gateway (inactivo)
    status_data = {
        "is_inactive": True,
        "gateway_name": gw_name,
        "metadata": {"active": False, "status": "OFFLINE"},
    }
    await mock_redis.set(
        f"tb_gw_status:{tenant_id}:{gw_id}",
        json.dumps(status_data),
        ex=45
    )

    is_inactive, parent_name, meta = await check_parent_gateway_status(
        tenant_id=tenant_id,
        device_name=device_name,
        redis_conn=mock_redis,
    )
    assert is_inactive is True
    assert parent_name == gw_name
    assert meta["active"] is False


@pytest.mark.asyncio
async def test_check_parent_gateway_resolution_inactive_parent():
    """Valida la resolución completa en ThingsBoard detectando gateway padre inactivo."""
    mock_redis = MockRedis()
    tenant_id = "64b0f0000000000000000001"
    device_name = "Sensor_Presion_03"
    gw_id = "gateway_uuid_777"
    gw_name = "IOTGateway_SectorA"

    # Mock Tenant y Server
    mock_tenant = MagicMock()
    mock_tenant.name = "TenantTest"
    mock_tenant.username = "tenant_admin"
    mock_tenant.get_password.return_value = "secret"
    mock_tenant.get_token.return_value = "mock_jwt_token"

    mock_server = MagicMock()
    mock_server.base_url = "http://tb.example.com"
    mock_tenant.get_server = AsyncMock(return_value=mock_server)

    with patch("core.models.tb_tenant.TBTenant.get", AsyncMock(return_value=mock_tenant)), \
         patch("core.tb_client.ThingsBoardClient.get_device_by_name") as mock_get_dev, \
         patch("core.tb_client.ThingsBoardClient.get_entity_relations") as mock_get_rels, \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes") as mock_get_attrs:

        mock_get_dev.return_value = {"id": {"id": "sensor_uuid_123"}, "name": device_name}
        # Relación entrante hacia el sensor desde el gateway
        mock_get_rels.return_value = [
            {
                "from": {"entityType": "DEVICE", "id": gw_id},
                "to": {"entityType": "DEVICE", "id": "sensor_uuid_123"},
                "fromName": gw_name,
            }
        ]
        # Atributos del gateway indican que está inactivo
        mock_get_attrs.return_value = [
            {"key": "active", "value": False},
            {"key": "status", "value": "OFFLINE"},
        ]

        is_inactive, parent_name, meta = await check_parent_gateway_status(
            tenant_id=tenant_id,
            device_name=device_name,
            redis_conn=mock_redis,
        )

        assert is_inactive is True
        assert parent_name == gw_name
        assert meta["active"] is False
        assert meta["status"] == "OFFLINE"

        # Verificar que se pobló la relación y el estado en Redis
        cached_rel = await mock_redis.get(f"tb_parent_gw:{tenant_id}:{device_name}")
        assert cached_rel is not None
        rel_obj = json.loads(cached_rel)
        assert rel_obj["gateway_id"] == gw_id
        assert rel_obj["gateway_name"] == gw_name

        cached_st = await mock_redis.get(f"tb_gw_status:{tenant_id}:{gw_id}")
        assert cached_st is not None
        st_obj = json.loads(cached_st)
        assert st_obj["is_inactive"] is True


@pytest.mark.asyncio
async def test_check_parent_gateway_fail_open_on_network_error():
    """Valida que ante fallo de red o excepción inesperada, se active fail-open (retorna False)."""
    mock_redis = MockRedis()
    tenant_id = "tenant_err"
    device_name = "Sensor_FailOpen"

    with patch("core.models.tb_tenant.TBTenant.get", AsyncMock(side_effect=Exception("MongoDB Connection Timeout"))):
        is_inactive, parent_name, meta = await check_parent_gateway_status(
            tenant_id=tenant_id,
            device_name=device_name,
            redis_conn=mock_redis,
        )
        # Fail-open: no se suprime la alerta
        assert is_inactive is False
        assert parent_name is None
        assert meta.get("fail_open") is True


@pytest.mark.asyncio
async def test_send_telegram_alert_task_suppressed_for_layer_4_sensor():
    """
    Valida que send_telegram_alert_task suprima la alerta de un sensor Capa 4
    cuando el IOTGateway padre está inactivo, sin realizar llamadas HTTP a Telegram.
    """
    mock_redis = MockRedis()
    mock_http_client = AsyncMock(spec=httpx.AsyncClient)

    ctx = {
        "job_id": "job_suppress_1",
        "job_try": 1,
        "redis": mock_redis,
        "http_client": mock_http_client,
    }

    payload = {
        "message": "Alerta de sensor sin comunicación",
        "layer": 4,
        "device_name": "Sensor_Humedad_01",
        "tenant_id": "tenant_123",
        "status": "OFFLINE",
        "bot_token": "mock_token",
        "chat_id": "mock_chat",
    }

    # Simulamos que check_parent_gateway_status indica que el gateway padre está caído
    with patch(
        "workers.tasks.check_parent_gateway_status",
        AsyncMock(return_value=(True, "IOTGateway_Principal", {"active": False, "status": "OFFLINE"}))
    ) as mock_check, \
         patch("workers.tasks.dispatch_debounced_alert") as mock_send_tg:

        result = await send_telegram_alert_task(ctx, payload=payload)

        mock_check.assert_called_once_with(
            tenant_id="tenant_123",
            device_name="Sensor_Humedad_01",
            redis_conn=mock_redis,
            http_client=mock_http_client,
        )
        # Se debió suprimir y NO llamar a send_telegram_alert
        mock_send_tg.assert_not_called()
        assert result["sent"] is False
        assert result["reason"] == "suppressed_by_parent_layer"
        assert result["parent_gateway"] == "IOTGateway_Principal"
        assert result["device_name"] == "Sensor_Humedad_01"


@pytest.mark.asyncio
async def test_send_telegram_alert_task_active_parent_gateway():
    """
    Valida que send_telegram_alert_task proceda con el envío de la alerta si el IOTGateway
    padre está activo.
    """
    mock_redis = MockRedis()
    mock_http_client = AsyncMock(spec=httpx.AsyncClient)

    ctx = {
        "job_id": "job_active_parent_1",
        "job_try": 1,
        "redis": mock_redis,
        "http_client": mock_http_client,
    }

    payload = {
        "message": "Sensor reporta batería baja",
        "layer": "Capa 4",
        "device_name": "Sensor_Bateria_02",
        "tenant_id": "tenant_123",
        "status": "WARNING",
        "bot_token": "mock_token",
        "chat_id": "mock_chat",
    }

    with patch(
        "workers.tasks.check_parent_gateway_status",
        AsyncMock(return_value=(False, "IOTGateway_Activo", {"active": True, "status": "ONLINE"}))
    ), patch(
        "workers.tasks.dispatch_debounced_alert",
        AsyncMock(return_value={"sent": True, "message_id": 8888, "alert_hash": "hash_xyz"})
    ) as mock_send_tg:

        result = await send_telegram_alert_task(ctx, payload=payload)

        mock_send_tg.assert_called_once()
        assert result["sent"] is True
        assert result["message_id"] == 8888


@pytest.mark.asyncio
async def test_send_telegram_alert_task_non_layer_4_not_suppressed():
    """
    Valida que alertas que no sean de Capa 4 (ej: Capa 3 - IOTGateway) no consulten
    la supresión jerárquica y se envíen directamente.
    """
    mock_redis = MockRedis()
    mock_http_client = AsyncMock(spec=httpx.AsyncClient)

    ctx = {
        "job_id": "job_layer_3_1",
        "job_try": 1,
        "redis": mock_redis,
        "http_client": mock_http_client,
    }

    payload = {
        "message": "Gateway IOTGateway_Principal está OFFLINE",
        "layer": 3,
        "device_name": "IOTGateway_Principal",
        "tenant_id": "tenant_123",
        "status": "OFFLINE",
        "bot_token": "mock_token",
        "chat_id": "mock_chat",
    }

    with patch("workers.tasks.check_parent_gateway_status") as mock_check, \
         patch(
             "workers.tasks.dispatch_debounced_alert",
             AsyncMock(return_value={"sent": True, "message_id": 9999, "alert_hash": "hash_gw"})
         ) as mock_send_tg:

        result = await send_telegram_alert_task(ctx, payload=payload)

        # No se debe consultar supresión jerárquica para Capa 3
        mock_check.assert_not_called()
        mock_send_tg.assert_called_once()
        assert result["sent"] is True
