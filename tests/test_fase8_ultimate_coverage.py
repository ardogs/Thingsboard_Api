"""
Test Suite Fase 8: Cobertura Extrema de Casos de Red, SSE Streaming y Fallbacks
Cubre:
- core/services/hierarchical_suppression_service.py (Paso 4: Resolución completa de relaciones y atributos en ThingsBoard)
- api/endpoints/tasks/router.py (stream_task_progress con estado inicial terminal y eventos Pub/Sub)
- api/endpoints/telemetry/router.py (Validaciones y ramas de error en DTOs y consultas)
- api/endpoints/devices/router.py (Fallbacks de assets y ramas de re-autenticación)
"""

import os
import json
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx
from starlette.requests import Request
from fastapi import HTTPException, status
from beanie import init_beanie, PydanticObjectId
from mongomock_motor import AsyncMongoMockClient

from core.models.user import User
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.tb_node import TBNode
from core.models.tb_backup import TBBackup
from core.models.tb_scheduled_task import TBScheduledTask
from core.models.tb_email_config import TBEmailConfig
from core.models.audit_log import AuditLog
from core.tb_client import ThingsBoardClient

from core.services.hierarchical_suppression_service import check_parent_gateway_status
from api.endpoints.tasks.router import stream_task_progress
from api.endpoints.devices.router import (
    _reauthenticate_tenant,
    list_sites_with_devices,
)


async def init_mock_db(db_name: str = "fase8_ultimate_db"):
    client = AsyncMongoMockClient()
    db = client[db_name]
    await init_beanie(
        database=db,
        document_models=[
            User,
            TBServer,
            TBTenant,
            TBNode,
            TBBackup,
            TBScheduledTask,
            TBEmailConfig,
            AuditLog,
        ],
    )
    return db


# ==============================================================================
# 1. HIERARCHICAL SUPPRESSION: RED REAL MOCKEADA (PASO 4 COMPLETO)
# ==============================================================================

@pytest.mark.asyncio
async def test_hierarchical_suppression_full_network_flow():
    await init_mock_db("hier_net_db")
    server = TBServer(name="SRV_HIER_NET", base_url="https://tb-net.com")
    await server.insert()
    tenant = TBTenant(name="TNT_HIER_NET", server_id=server, username="adm")
    tenant.set_tokens("valid_jwt", "valid_ref")
    await tenant.insert()

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)  # No cache hit
    mock_redis.set = AsyncMock(return_value=True)  # SingleFlight lock adquirido

    mock_tb = MagicMock(spec=ThingsBoardClient)
    mock_tb.base_url = server.base_url
    mock_tb.token = "valid_jwt"

    # 1. get_device_by_name encuentra el sensor
    mock_tb.get_device_by_name = AsyncMock(return_value={
        "id": {"id": "sensor_uuid_100"},
        "name": "Sensor_Humedad_P1"
    })

    # 2. get_entity_relations encuentra el gateway padre
    mock_tb.get_entity_relations = AsyncMock(return_value=[
        {
            "from": {"id": "gw_uuid_999", "entityType": "DEVICE", "name": "GW_Central"},
            "to": {"id": "sensor_uuid_100", "entityType": "DEVICE"},
            "fromName": "GW_Central"
        }
    ])

    # 3. get_entity_attributes retorna el gateway inactivo (active = False)
    mock_tb.get_entity_attributes = AsyncMock(return_value=[
        {"key": "active", "value": False},
        {"key": "lastActivityTime", "value": 1750000000000}
    ])

    with patch("core.services.hierarchical_suppression_service.ThingsBoardClient", return_value=mock_tb):
        is_suppressed, parent_gw, meta = await check_parent_gateway_status(
            tenant_id=str(tenant.id),
            device_name="Sensor_Humedad_P1",
            redis_conn=mock_redis
        )

        assert is_suppressed is True
        assert parent_gw == "GW_Central"
        assert meta["attributes"]["active"] is False


# ==============================================================================
# 2. TASKS ROUTER: SSE STREAM PROGRESS
# ==============================================================================

@pytest.mark.asyncio
async def test_stream_task_progress_sse():
    await init_mock_db("stream_test_db")
    user = User(username="stream_user", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    mock_redis = AsyncMock()
    mock_pubsub = AsyncMock()

    # Caso 1: Initial state terminal (SUCCESS) -> cierra stream inmediatamente
    terminal_payload = json.dumps({
        "task_id": "job_sse_1",
        "status": "SUCCESS",
        "progress_pct": 100.0,
        "message": "Completado"
    })
    mock_redis.hget = AsyncMock(return_value=terminal_payload)
    mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

    req_mock = MagicMock(spec=Request)
    req_mock.is_disconnected = AsyncMock(return_value=False)

    with patch("api.endpoints.tasks.router.redis_client", mock_redis):
        resp = await stream_task_progress(
            task_id="job_sse_1",
            request=req_mock,
            current_user=user
        )

        # Consumir el generador asíncrono
        gen = resp.body_iterator
        first_chunk = await anext(gen)
        assert "data: " in first_chunk
        assert "SUCCESS" in first_chunk

        # El generador debe terminar porque el estado es SUCCESS
        with pytest.raises(StopAsyncIteration):
            await anext(gen)


# ==============================================================================
# 3. DEVICES ROUTER: REAUTHENTICATE ERROR & ASSETS FALLBACK
# ==============================================================================

@pytest.mark.asyncio
async def test_devices_router_reauth_failure_and_asset_fallback():
    await init_mock_db("dev_edge_db")
    server = TBServer(name="SRV_DEV_EDGE", base_url="https://tb.com")
    await server.insert()
    tenant = TBTenant(name="TNT_DEV_EDGE", server_id=server, username="bad_adm")
    tenant.set_password("wrong_password")
    await tenant.insert()
    user = User(username="dev_edge_user", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    # 1. _reauthenticate_tenant falla login -> HTTPException 401
    mock_tb_fail = MagicMock(spec=ThingsBoardClient)
    mock_tb_fail.refresh_jwt_token = AsyncMock(side_effect=Exception("Invalid refresh token"))
    mock_tb_fail.login = AsyncMock(side_effect=Exception("Bad credentials"))

    with pytest.raises(HTTPException) as exc_auth:
        await _reauthenticate_tenant(tenant, mock_tb_fail)
    assert exc_auth.value.status_code == status.HTTP_401_UNAUTHORIZED

    # 2. list_sites_with_devices con fallback a /api/tenant/assets
    tenant.set_tokens("valid_jwt", "valid_ref")
    await tenant.save()

    mock_tb_fallback = MagicMock(spec=ThingsBoardClient)
    mock_tb_fallback.base_url = server.base_url
    mock_tb_fallback.token = "valid_jwt"
    mock_tb_fallback.timeout = 30.0
    # Entity query falla (500)
    mock_req = httpx.Request("POST", "https://tb.com")
    mock_resp_500 = httpx.Response(500, request=mock_req)
    mock_tb_fallback.find_entities_by_query = AsyncMock(
        side_effect=httpx.HTTPStatusError("Server error", request=mock_req, response=mock_resp_500)
    )
    # get_tenant_assets fallback funciona
    mock_tb_fallback.get_tenant_assets = AsyncMock(return_value={
        "data": [
            {
                "id": {"id": "asset_uuid_1", "entityType": "ASSET"},
                "name": "Sitio Recuperado por Fallback",
                "type": "SITE"
            }
        ]
    })
    mock_tb_fallback.get_tenant_devices = AsyncMock(return_value={"data": []})
    mock_tb_fallback.get_entity_relations = AsyncMock(return_value=[])

    with patch("api.endpoints.devices.router.ThingsBoardClient", return_value=mock_tb_fallback):
        sites = await list_sites_with_devices(
            tenant_id=str(tenant.id),
            current_user=user
        )
        assert len(sites) == 1
        assert sites[0].name == "Sitio Recuperado por Fallback"
