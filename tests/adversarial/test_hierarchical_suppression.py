"""
ADVERSARIAL STRESS TEST SUITE: Hierarchical Alert Suppression (Layer 3 vs Layer 4)
==================================================================================
Phase 3: Adversarial Review and Stress Testing
Target: MICRO-HITO 3: La Supresión Jerárquica Capa 3 vs Capa 4
Task ID: task_20260929_hierarchical_suppression

Adversarial Objectives:
-----------------------
1. Thundering Herd Burst of 100 Sensors (The Primary Invariant):
   - 100 sensors in Capa 4 failing simultaneously, all reporting to the SAME parent IOTGateway.
   - Invariant 1 (Full Suppression): All 100 tasks return sent=False, reason="suppressed_by_parent_layer".
   - Invariant 2 (Zero Telegram Spam): Exactly 0 Telegram HTTP calls.
   - Invariant 3 (Zero ThingsBoard Bombardment): Parent status endpoint call count <= 2 (ideally 1).
2. Event Loop Latency & Zero GIL Starvation Probe:
   - High-frequency background ticker (5ms interval) running during 100 concurrent tasks.
   - Invariant: Maximum jitter between ticks remains < 200ms.
3. Multi-Gateway Partitioned Thundering Herd:
   - 100 sensors distributed across 5 gateways (20 sensors each).
   - Gateways 1, 2, 3 are INACTIVE; Gateways 4, 5 are ACTIVE.
   - Invariant: 60 sensors on inactive gateways are suppressed; 40 sensors on active gateways proceed.
   - Invariant: Total ThingsBoard calls per gateway <= 2.
4. Fail-Open Resilience on Infrastructure Outage:
   - ThingsBoard / MongoDB raising network exceptions (httpx.ConnectError / Timeout).
   - Invariant: Alerts fail-open to Telegram; worker does not crash.
5. Cache Expiry & Stampede Prevention:
   - 45s TTL expiry simulated; subsequent 20 concurrent requests re-acquire SingleFlight mutex cleanly.
6. Edge Cases & Layer Filtering:
   - Capa 3 (IOTGateway itself) must NEVER be suppressed.
   - Sensors without parent relation ("NONE") must NOT be suppressed.
   - Case-insensitive layer formatting ("4", "Capa 4", "CAPA4", "layer_4").
"""

import asyncio
import json
import time
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
import httpx
import fakeredis.aioredis
from beanie import PydanticObjectId

from core.services.hierarchical_suppression_service import (
    check_parent_gateway_status,
    RELATION_CACHE_TTL,
    GW_STATUS_CACHE_TTL,
)
from workers.tasks import send_telegram_alert_task


# ==============================================================================
# FIXTURES & HELPERS
# ==============================================================================

@pytest_asyncio.fixture
async def fake_redis():
    """Provides an isolated Async FakeRedis instance for deterministic lock/cache testing."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def setup_mock_tenant_and_server(tenant_id: str = "64b0f0000000000000000001"):
    """Creates mock TBTenant and TBServer objects with standard credentials."""
    mock_tenant = MagicMock()
    mock_tenant.id = PydanticObjectId(tenant_id)
    mock_tenant.name = "TenantProduction"
    mock_tenant.username = "tenant_admin"
    mock_tenant.get_password.return_value = "top_secret_pass"
    mock_tenant.get_token.return_value = "mock_valid_jwt_token"

    mock_server = MagicMock()
    mock_server.base_url = "http://tb.cloud.tkme.internal"
    mock_tenant.get_server = AsyncMock(return_value=mock_server)

    return mock_tenant, mock_server


# ==============================================================================
# VECTOR 1: THUNDERING HERD OF 100 SENSORS BURST (PRIMARY INVARIANT)
# ==============================================================================

@pytest.mark.asyncio
async def test_thundering_herd_100_sensors_burst_suppression(fake_redis):
    """
    ATTACK VECTOR 1:
    Simulate 100 sensors (sensor_000 to sensor_099) in Capa 4 failing simultaneously,
    all attached to the SAME parent IOTGateway ('gw_main_01').
    Launch all 100 worker tasks concurrently via asyncio.gather.

    ASSERTIONS:
    - Invariant 1 (Full Suppression): All 100 calls return reason='suppressed_by_parent_layer'.
    - Invariant 2 (Zero Telegram Spam): Exactly 0 calls to send_telegram_alert.
    - Invariant 3 (Zero ThingsBoard Bombardment): Parent gateway attributes called <= 2 times (ideally 1).
    """
    tenant_id = "64b0f0000000000000000001"
    gw_id = "gw_main_01"
    gw_name = "IOTGateway_Main_Core"
    sensor_count = 100

    # 1. Pre-seed relation cache for all 100 sensors pointing to the same parent gateway
    for i in range(sensor_count):
        sensor_name = f"sensor_{i:03d}"
        await fake_redis.set(
            f"tb_parent_gw:{tenant_id}:{sensor_name}",
            json.dumps({"gateway_id": gw_id, "gateway_name": gw_name}),
            ex=RELATION_CACHE_TTL
        )

    # 2. Setup mock ThingsBoard & Tenant infrastructure
    mock_tenant, _ = setup_mock_tenant_and_server(tenant_id)

    # ThingsBoard parent gateway attributes indicating gateway is INACTIVE (active=False, status=OFFLINE)
    mock_get_attrs = AsyncMock(return_value=[
        {"key": "active", "value": False},
        {"key": "status", "value": "OFFLINE"},
        {"key": "lastActivityTime", "value": 1727610000},
    ])
    mock_send_telegram = AsyncMock()

    with patch("core.models.tb_tenant.TBTenant.get", AsyncMock(return_value=mock_tenant)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", mock_get_attrs), \
         patch("workers.tasks.dispatch_debounced_alert", mock_send_telegram):

        tasks = [
            send_telegram_alert_task(
                ctx={"job_id": f"thundering_job_{i:03d}", "redis": fake_redis},
                payload={
                    "message": f"Sensor sensor_{i:03d} disconnected from field",
                    "layer": 4,
                    "device_name": f"sensor_{i:03d}",
                    "tenant_id": tenant_id,
                    "status": "OFFLINE",
                    "bot_token": "tg_token_123",
                    "chat_id": "tg_chat_123",
                }
            )
            for i in range(sensor_count)
        ]

        results = await asyncio.gather(*tasks)

    # Invariant 1: All 100 sensors are suppressed
    assert len(results) == sensor_count
    for idx, res in enumerate(results):
        assert res.get("sent") is False, f"Task #{idx} was not suppressed!"
        assert res.get("reason") == "suppressed_by_parent_layer"
        assert res.get("parent_gateway") == gw_name

    # Invariant 2: Zero Telegram alerts dispatched
    assert mock_send_telegram.call_count == 0, (
        f"VERDICT: FAIL - Telegram spammed! Call count: {mock_send_telegram.call_count}"
    )

    # Invariant 3: ThingsBoard parent attributes queried <= 2 times (SingleFlight deduplication)
    assert mock_get_attrs.call_count <= 2, (
        f"VERDICT: FAIL - ThingsBoard bombarded during 100-sensor burst! "
        f"Call count was {mock_get_attrs.call_count} (limit: <= 2)"
    )


# ==============================================================================
# VECTOR 2: EVENT LOOP LATENCY & ZERO GIL STARVATION PROBE
# ==============================================================================

@pytest.mark.asyncio
async def test_event_loop_latency_under_100_sensors_burst(fake_redis):
    """
    ATTACK VECTOR 2:
    Run a high-frequency background ticker (5ms interval) during the 100-sensor burst.
    Verifies that Redis mutex polling, JSON deserialization, and ARQ task dispatching
    execute with zero blocking I/O and zero GIL starvation.

    Invariant: Max jitter between ticks remains < 200ms.
    """
    tenant_id = "64b0f0000000000000000001"
    gw_id = "gw_latency_01"
    gw_name = "IOTGateway_Latency_Test"
    sensor_count = 100

    for i in range(sensor_count):
        await fake_redis.set(
            f"tb_parent_gw:{tenant_id}:sensor_lat_{i:03d}",
            json.dumps({"gateway_id": gw_id, "gateway_name": gw_name}),
            ex=300
        )

    mock_tenant, _ = setup_mock_tenant_and_server(tenant_id)
    mock_get_attrs = AsyncMock(return_value=[{"key": "active", "value": False}])

    tick_interval = 0.005  # 5ms tick
    jitter_samples: List[float] = []
    stop_event = asyncio.Event()

    async def event_loop_ticker():
        while not stop_event.is_set():
            t0 = time.perf_counter()
            await asyncio.sleep(tick_interval)
            t1 = time.perf_counter()
            jitter = (t1 - t0) - tick_interval
            jitter_samples.append(jitter * 1000.0)

    ticker_task = asyncio.create_task(event_loop_ticker())

    with patch("core.models.tb_tenant.TBTenant.get", AsyncMock(return_value=mock_tenant)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", mock_get_attrs), \
         patch("workers.tasks.dispatch_debounced_alert", AsyncMock()):

        tasks = [
            send_telegram_alert_task(
                ctx={"job_id": f"lat_job_{i:03d}", "redis": fake_redis},
                payload={
                    "message": f"Sensor lat_{i} dropped",
                    "layer": "Capa 4",
                    "device_name": f"sensor_lat_{i:03d}",
                    "tenant_id": tenant_id,
                    "status": "OFFLINE",
                }
            )
            for i in range(sensor_count)
        ]
        await asyncio.gather(*tasks)

    stop_event.set()
    await ticker_task

    assert len(jitter_samples) > 0
    max_jitter_ms = max(jitter_samples)
    assert max_jitter_ms < 200.0, (
        f"VERDICT: FAIL - Event loop stalled! Max jitter was {max_jitter_ms:.2f}ms (threshold: 200ms)."
    )


# ==============================================================================
# VECTOR 3: MULTI-GATEWAY PARTITIONED THUNDERING HERD
# ==============================================================================

@pytest.mark.asyncio
async def test_multi_gateway_partitioned_thundering_herd(fake_redis):
    """
    ATTACK VECTOR 3:
    100 sensors distributed evenly across 5 different IOTGateways (20 sensors each).
    - Gateways 1, 2, 3 are INACTIVE (active=False) -> Sensors MUST be suppressed (60 suppressed).
    - Gateways 4, 5 are ACTIVE (active=True) -> Sensors MUST NOT be suppressed (40 proceed to Telegram).
    - Invariant: Call count to ThingsBoard attributes endpoint for each gateway <= 2.
    """
    tenant_id = "64b0f0000000000000000001"
    mock_tenant, _ = setup_mock_tenant_and_server(tenant_id)

    gateway_status_map = {
        "gw_part_01": False,  # Inactive
        "gw_part_02": False,  # Inactive
        "gw_part_03": False,  # Inactive
        "gw_part_04": True,   # Active
        "gw_part_05": True,   # Active
    }

    # Pre-seed relations: 20 sensors per gateway
    tasks = []
    sensor_idx = 0
    for gw_id, is_active in gateway_status_map.items():
        gw_name = f"Gateway_{gw_id}"
        for _ in range(20):
            sensor_name = f"sensor_part_{sensor_idx:03d}"
            await fake_redis.set(
                f"tb_parent_gw:{tenant_id}:{sensor_name}",
                json.dumps({"gateway_id": gw_id, "gateway_name": gw_name}),
                ex=300
            )
            tasks.append({
                "job_id": f"part_job_{sensor_idx:03d}",
                "sensor_name": sensor_name,
                "gw_id": gw_id,
                "expected_suppression": not is_active,
            })
            sensor_idx += 1

    gateway_query_counts: Dict[str, int] = {gw_id: 0 for gw_id in gateway_status_map}

    async def dynamic_get_attributes(entity_id: str, scope: str, token: str, client=None):
        gateway_query_counts[entity_id] = gateway_query_counts.get(entity_id, 0) + 1
        is_active = gateway_status_map.get(entity_id, True)
        return [{"key": "active", "value": is_active}]

    mock_send_telegram = AsyncMock(return_value={"sent": True, "message_id": 12345})

    with patch("core.models.tb_tenant.TBTenant.get", AsyncMock(return_value=mock_tenant)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", side_effect=dynamic_get_attributes), \
         patch("workers.tasks.dispatch_debounced_alert", mock_send_telegram):

        worker_tasks = [
            send_telegram_alert_task(
                ctx={"job_id": t["job_id"], "redis": fake_redis},
                payload={
                    "message": f"Sensor {t['sensor_name']} alarm",
                    "layer": 4,
                    "device_name": t["sensor_name"],
                    "tenant_id": tenant_id,
                    "status": "WARNING",
                    "bot_token": "token123",
                    "chat_id": "chat123",
                }
            )
            for t in tasks
        ]

        results = await asyncio.gather(*worker_tasks)

    # 1. Assert exactly 60 suppressed and 40 sent
    suppressed_count = sum(1 for r in results if r.get("reason") == "suppressed_by_parent_layer")
    sent_count = sum(1 for r in results if r.get("sent") is True)

    assert suppressed_count == 60, f"Expected 60 suppressed, got {suppressed_count}"
    assert sent_count == 40, f"Expected 40 sent, got {sent_count}"
    assert mock_send_telegram.call_count == 40

    # 2. Assert SingleFlight query counts per gateway <= 2
    for gw_id, count in gateway_query_counts.items():
        assert count <= 2, f"Gateway {gw_id} queried {count} times (exceeds limit <= 2)"


# ==============================================================================
# VECTOR 4: FAIL-OPEN RESILIENCE ON INFRASTRUCTURE OUTAGE
# ==============================================================================

@pytest.mark.asyncio
async def test_fail_open_on_thingsboard_network_failure(fake_redis):
    """
    ATTACK VECTOR 4:
    Simulate ThingsBoard network disconnect (httpx.ConnectError) during parent status check.
    Invariant: System fails OPEN, meaning sensor alert is NOT suppressed, and proceeds
    to dispatch notification to Telegram without crashing the worker task.
    """
    tenant_id = "64b0f0000000000000000001"
    sensor_name = "sensor_failopen_01"
    gw_id = "gw_failopen_01"

    await fake_redis.set(
        f"tb_parent_gw:{tenant_id}:{sensor_name}",
        json.dumps({"gateway_id": gw_id, "gateway_name": "IOTGateway_Down"}),
        ex=300
    )

    mock_tenant, _ = setup_mock_tenant_and_server(tenant_id)
    mock_send_telegram = AsyncMock(return_value={"sent": True, "message_id": 9999})

    with patch("core.models.tb_tenant.TBTenant.get", AsyncMock(return_value=mock_tenant)), \
         patch(
             "core.tb_client.ThingsBoardClient.get_entity_attributes",
             AsyncMock(side_effect=httpx.ConnectError("Connection refused by ThingsBoard:8080"))
         ), \
         patch("workers.tasks.dispatch_debounced_alert", mock_send_telegram):

        result = await send_telegram_alert_task(
            ctx={"job_id": "job_failopen_1", "redis": fake_redis},
            payload={
                "message": "Critical water leak sensor alert",
                "layer": 4,
                "device_name": sensor_name,
                "tenant_id": tenant_id,
                "status": "CRITICAL",
                "bot_token": "token123",
                "chat_id": "chat123",
            }
        )

        # Invariant: Alert is NOT suppressed, dispatched to Telegram
        assert result.get("sent") is True
        assert mock_send_telegram.call_count == 1


# ==============================================================================
# VECTOR 5: CACHE EXPIRY & STAMPEDE PREVENTION
# ==============================================================================

@pytest.mark.asyncio
async def test_cache_expiry_and_subsequent_stampede_prevention(fake_redis):
    """
    ATTACK VECTOR 5:
    Test behavior across TTL boundaries:
    1. Pre-seed relation, and simulate status check for 10 concurrent requests (Status Cached with TTL=1s).
    2. Wait 1.05s for TTL expiry.
    3. Launch 20 concurrent requests simultaneously.
    4. Assert SingleFlight mutex re-locks cleanly, ThingsBoard queried only 1 additional time.
    """
    tenant_id = "64b0f0000000000000000001"
    gw_id = "gw_ttl_01"
    gw_name = "IOTGateway_TTL"

    for i in range(20):
        await fake_redis.set(
            f"tb_parent_gw:{tenant_id}:sensor_ttl_{i:02d}",
            json.dumps({"gateway_id": gw_id, "gateway_name": gw_name}),
            ex=300
        )

    mock_tenant, _ = setup_mock_tenant_and_server(tenant_id)
    mock_get_attrs = AsyncMock(return_value=[{"key": "active", "value": False}])

    with patch("core.models.tb_tenant.TBTenant.get", AsyncMock(return_value=mock_tenant)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", mock_get_attrs), \
         patch("core.services.hierarchical_suppression_service.GW_STATUS_CACHE_TTL", 1):

        # First burst: 10 sensors
        tasks_1 = [
            check_parent_gateway_status(
                tenant_id=tenant_id,
                device_name=f"sensor_ttl_{i:02d}",
                redis_conn=fake_redis,
            )
            for i in range(10)
        ]
        res_1 = await asyncio.gather(*tasks_1)
        assert all(r[0] is True for r in res_1)
        assert mock_get_attrs.call_count == 1

        # Wait for 1s TTL expiration
        await asyncio.sleep(1.05)
        # Ensure status cache expired
        assert await fake_redis.get(f"tb_gw_status:{tenant_id}:{gw_id}") is None

        # Second burst: 20 sensors stampede on expired cache
        tasks_2 = [
            check_parent_gateway_status(
                tenant_id=tenant_id,
                device_name=f"sensor_ttl_{i:02d}",
                redis_conn=fake_redis,
            )
            for i in range(20)
        ]
        res_2 = await asyncio.gather(*tasks_2)
        assert all(r[0] is True for r in res_2)

        # ThingsBoard attributes must only have been called once more (total <= 2)
        assert mock_get_attrs.call_count <= 2, (
            f"Expected <= 2 total queries, got {mock_get_attrs.call_count}"
        )


# ==============================================================================
# VECTOR 6: EDGE CASES & LAYER FILTERING
# ==============================================================================

@pytest.mark.asyncio
async def test_layer_3_gateway_itself_never_suppressed(fake_redis):
    """
    ATTACK VECTOR 6A:
    Capa 3 (IOTGateway itself) failing must NEVER be suppressed by hierarchical check,
    as it is the parent entity and must alert immediately.
    """
    tenant_id = "64b0f0000000000000000001"
    mock_send_telegram = AsyncMock(return_value={"sent": True, "message_id": 777})

    with patch("workers.tasks.dispatch_debounced_alert", mock_send_telegram):
        for layer_val in [3, "3", "Capa 3", "CAPA 3", "Layer 3", "layer_3"]:
            res = await send_telegram_alert_task(
                ctx={"job_id": f"gw_job_{layer_val}", "redis": fake_redis},
                payload={
                    "message": "Gateway power supply failure",
                    "layer": layer_val,
                    "device_name": "IOTGateway_Main_01",
                    "tenant_id": tenant_id,
                    "status": "CRITICAL",
                    "bot_token": "tok",
                    "chat_id": "cid",
                }
            )
            assert res.get("sent") is True
            assert res.get("reason") != "suppressed_by_parent_layer"


@pytest.mark.asyncio
async def test_sensor_without_parent_relation_not_suppressed(fake_redis):
    """
    ATTACK VECTOR 6B:
    Sensors without a parent relation in ThingsBoard (standalone sensors) must cache 'NONE'
    and NOT be suppressed.
    """
    tenant_id = "64b0f0000000000000000001"
    sensor_name = "Standalone_Sensor_No_Parent"

    # Pre-cache NONE relation
    await fake_redis.set(f"tb_parent_gw:{tenant_id}:{sensor_name}", "NONE", ex=300)

    mock_send_telegram = AsyncMock(return_value={"sent": True, "message_id": 888})

    with patch("workers.tasks.dispatch_debounced_alert", mock_send_telegram):
        res = await send_telegram_alert_task(
            ctx={"job_id": "standalone_job", "redis": fake_redis},
            payload={
                "message": "Standalone sensor battery low",
                "layer": 4,
                "device_name": sensor_name,
                "tenant_id": tenant_id,
                "status": "WARNING",
                "bot_token": "tok",
                "chat_id": "cid",
            }
        )

        assert res.get("sent") is True
        assert res.get("reason") != "suppressed_by_parent_layer"


@pytest.mark.asyncio
async def test_case_insensitive_layer_4_variants(fake_redis):
    """
    ATTACK VECTOR 6C:
    Verify that layer 4 is identified across all canonical string and integer variants:
    4, '4', 'Capa 4', 'CAPA4', 'Layer 4', 'layer_4'.
    """
    tenant_id = "64b0f0000000000000000001"
    gw_id = "gw_case_01"
    gw_name = "IOTGateway_Case"

    # Pre-cache relation and inactive status
    await fake_redis.set(
        f"tb_gw_status:{tenant_id}:{gw_id}",
        json.dumps({"is_inactive": True, "gateway_name": gw_name, "metadata": {"active": False}}),
        ex=45
    )

    variants = [4, "4", "Capa 4", "capa 4", "CAPA4", "Layer 4", "layer_4"]
    for idx, layer_var in enumerate(variants):
        sensor_name = f"sensor_var_{idx}"
        await fake_redis.set(
            f"tb_parent_gw:{tenant_id}:{sensor_name}",
            json.dumps({"gateway_id": gw_id, "gateway_name": gw_name}),
            ex=300
        )

        res = await send_telegram_alert_task(
            ctx={"job_id": f"var_job_{idx}", "redis": fake_redis},
            payload={
                "message": "Variant test alert",
                "layer": layer_var,
                "device_name": sensor_name,
                "tenant_id": tenant_id,
                "status": "OFFLINE",
            }
        )
        assert res.get("sent") is False, f"Variant '{layer_var}' was not recognized as Layer 4!"
        assert res.get("reason") == "suppressed_by_parent_layer"
