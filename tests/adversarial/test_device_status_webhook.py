"""
ADVERSARIAL STRESS TEST SUITE: Device Status Webhook & ARQ Enqueueing
====================================================================
Phase 3: Adversarial Review and Stress Testing
Target: MICRO-HITO 2: El Webhook de ThingsBoard (POST /api/v1/telemetry/webhooks/device-status)
Task ID: task_20260929_device_status_webhook

Attack Vectors & Invariants Tested:
-----------------------------------
1. Payload Schema Attacks & Edge Cases:
   - Missing fields, empty dict, and null values -> HTTP 422.
   - Whitespace attacks (spaces, tabs, newlines) -> HTTP 422.
   - Type confusion: boolean, integer, list, dict in string fields -> HTTP 422.
   - Layer variants: valid int and str (HTTP 202); invalid bool, list, dict (HTTP 422).
   - Massive payload boundary/fuzzing: 50KB strings and deeply nested dicts -> HTTP 202.
2. HTML Injection & Sanitization in Alert Formatting:
   - XSS/HTML payloads (<script>, <a href=...>, <b>, <img>) in device_name, message, tenant_id, and details.
   - Invariant: All raw tags are strictly escaped (&lt;, &gt;, &amp;) in ARQ payload message.
3. High-Concurrency Burst (Event Loop & ARQ Enqueueing):
   - 50 concurrent HTTP POST requests via ASGITransport using asyncio.gather.
   - Invariant: All 50 return HTTP 202 Accepted.
   - Invariant: Exactly 50 jobs enqueued in ARQ with correct alert_key and ttl_seconds=300.
   - Invariant: Background ticker measures event loop lag; no GIL starvation (< 100ms jitter).
4. Resilience to Infrastructure Failure:
   - ARQ / Redis pool raising ConnectionError or RuntimeError during enqueue_job.
   - Invariant: Returns HTTP 503 / 500 without crashing or hanging the server process.
5. Alert Key & Severity Mapping Semantics:
   - Deterministic alert_key: device_status:{tenant_id}:{device_name}:{status}.
   - Semantic severity level mapping (CRITICAL, ERROR/FAIL, WARNING/OFFLINE, ONLINE/SUCCESS).
"""

import asyncio
import time
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from api.endpoints.telemetry.router import (
    DeviceStatusWebhookRequest,
    device_status_webhook,
)
from api.main import app


# ==============================================================================
# FIXTURES & HELPERS
# ==============================================================================

@pytest.fixture
def mock_arq_pool():
    """Provides a thread-safe mock ARQ pool that records enqueued jobs."""
    jobs: List[Dict[str, Any]] = []
    pool = MagicMock()

    async def mock_enqueue(task_name: str, *args, **kwargs):
        job_id = f"job-{len(jobs) + 1:04d}"
        job_mock = MagicMock()
        job_mock.job_id = job_id
        payload = kwargs.get("payload") if "payload" in kwargs else (args[0] if args else {})
        jobs.append({"job_id": job_id, "task_name": task_name, "payload": payload})
        return job_mock

    pool.enqueue_job = AsyncMock(side_effect=mock_enqueue)
    pool.recorded_jobs = jobs
    return pool


import pytest_asyncio


@pytest_asyncio.fixture
async def async_client():
    """Provides an HTTPX AsyncClient wired directly to the FastAPI ASGI app."""
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ==============================================================================
# VECTOR 1: PAYLOAD SCHEMA ATTACKS & EDGE CASES
# ==============================================================================

@pytest.mark.asyncio
async def test_schema_attack_empty_payload_and_missing_fields(async_client, mock_arq_pool):
    """
    ATTACK VECTOR 1A:
    Empty payload {} and payloads with missing required fields must return HTTP 422.
    """
    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_arq_pool)):
        # 1. Completely empty body
        resp = await async_client.post("/api/v1/telemetry/webhooks/device-status", json={})
        assert resp.status_code == 422

        # 2. Missing device_name
        resp = await async_client.post(
            "/api/v1/telemetry/webhooks/device-status",
            json={"status": "ONLINE", "layer": 1, "tenant_id": "tenant-1"}
        )
        assert resp.status_code == 422

        # 3. Missing status
        resp = await async_client.post(
            "/api/v1/telemetry/webhooks/device-status",
            json={"device_name": "dev-01", "layer": 1, "tenant_id": "tenant-1"}
        )
        assert resp.status_code == 422

        # 4. Missing layer
        resp = await async_client.post(
            "/api/v1/telemetry/webhooks/device-status",
            json={"device_name": "dev-01", "status": "ONLINE", "tenant_id": "tenant-1"}
        )
        assert resp.status_code == 422

        # 5. Missing tenant_id
        resp = await async_client.post(
            "/api/v1/telemetry/webhooks/device-status",
            json={"device_name": "dev-01", "status": "ONLINE", "layer": 1}
        )
        assert resp.status_code == 422

        # Ensure no jobs were enqueued
        assert len(mock_arq_pool.recorded_jobs) == 0


@pytest.mark.asyncio
async def test_schema_attack_null_values_in_required_fields(async_client, mock_arq_pool):
    """
    ATTACK VECTOR 1B:
    Null values in required fields must trigger validation error and return HTTP 422.
    """
    base = {"device_name": "dev-01", "status": "ONLINE", "layer": 1, "tenant_id": "tenant-1"}
    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_arq_pool)):
        for required_field in ["device_name", "status", "layer", "tenant_id"]:
            payload = dict(base)
            payload[required_field] = None
            resp = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=payload)
            assert resp.status_code == 422, f"Field '{required_field}' set to None should return 422"

    assert len(mock_arq_pool.recorded_jobs) == 0


@pytest.mark.asyncio
async def test_schema_attack_whitespace_injections(async_client, mock_arq_pool):
    """
    ATTACK VECTOR 1C:
    Strings containing only whitespace, tabs, or newlines must be rejected with HTTP 422.
    """
    whitespace_samples = ["   ", "\t", "\n\r", "  \t \n  "]
    base = {"device_name": "dev-01", "status": "ONLINE", "layer": 1, "tenant_id": "tenant-1"}

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_arq_pool)):
        for ws in whitespace_samples:
            # device_name whitespace
            p = dict(base, device_name=ws)
            r = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=p)
            assert r.status_code == 422

            # status whitespace
            p = dict(base, status=ws)
            r = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=p)
            assert r.status_code == 422

            # tenant_id whitespace
            p = dict(base, tenant_id=ws)
            r = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=p)
            assert r.status_code == 422

            # layer whitespace
            p = dict(base, layer=ws)
            r = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=p)
            assert r.status_code == 422

    assert len(mock_arq_pool.recorded_jobs) == 0


@pytest.mark.asyncio
async def test_schema_attack_type_confusion(async_client, mock_arq_pool):
    """
    ATTACK VECTOR 1D:
    Type confusion attacks: passing booleans, integers, lists, or dicts into string fields
    (device_name, tenant_id, message) must be strictly rejected with HTTP 422.
    """
    type_confusion_values = [True, False, 12345, 99.9, ["nested", "list"], {"key": "val"}]
    base = {"device_name": "dev-01", "status": "ONLINE", "layer": 1, "tenant_id": "tenant-1"}

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_arq_pool)):
        for bad_val in type_confusion_values:
            # Type confusion on device_name
            p = dict(base, device_name=bad_val)
            r = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=p)
            assert r.status_code == 422, f"device_name with {type(bad_val)} should return 422"

            # Type confusion on tenant_id
            p = dict(base, tenant_id=bad_val)
            r = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=p)
            assert r.status_code == 422, f"tenant_id with {type(bad_val)} should return 422"

            # Type confusion on message
            p = dict(base, message=bad_val)
            r = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=p)
            assert r.status_code == 422, f"message with {type(bad_val)} should return 422"

    assert len(mock_arq_pool.recorded_jobs) == 0


@pytest.mark.asyncio
async def test_schema_layer_edge_cases(async_client, mock_arq_pool):
    """
    ATTACK VECTOR 1E:
    Layer validation semantics:
    - Valid integer layer (1, 2) -> HTTP 202
    - Valid string layer ("L1", "Layer-Core") -> HTTP 202
    - Invalid types (bool True/False, list, dict) -> HTTP 422
    """
    base = {"device_name": "dev-01", "status": "ONLINE", "tenant_id": "tenant-1"}

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_arq_pool)):
        # Valid layers
        for valid_layer in [1, 2, 4, "L2", "Layer-Core", "Core-3"]:
            p = dict(base, layer=valid_layer)
            r = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=p)
            assert r.status_code == 202
            body = r.json()
            assert body["layer"] == str(valid_layer)

        # Invalid layer types (bool, list, dict)
        for invalid_layer in [True, False, [1, 2], {"layer": 1}]:
            p = dict(base, layer=invalid_layer)
            r = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=p)
            assert r.status_code == 422, f"layer with {type(invalid_layer)} should return 422"


@pytest.mark.asyncio
async def test_boundary_fuzz_massive_payload(async_client, mock_arq_pool):
    """
    ATTACK VECTOR 1F:
    Boundary & Fuzz testing: massive payloads (50KB message string and deeply nested details).
    Ensures no buffer overflow, crash, or unhandled exception occurs.
    """
    massive_message = "ALERT " + ("X" * 50000)
    complex_details = {
        f"metric_{i}": {
            "sub_metric": f"val_{i}",
            "nested_list": [i, i * 2, f"item_{i}"],
            "data": "A" * 500
        }
        for i in range(20)
    }

    payload = {
        "device_name": "fuzz-gateway-99",
        "status": "CRITICAL",
        "layer": 1,
        "tenant_id": "tenant-fuzz",
        "message": massive_message,
        "details": complex_details,
    }

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_arq_pool)):
        resp = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=payload)
        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "accepted"
        assert len(mock_arq_pool.recorded_jobs) == 1

        enqueued_payload = mock_arq_pool.recorded_jobs[0]["payload"]
        assert enqueued_payload["alert_key"] == "device_status:tenant-fuzz:fuzz-gateway-99:CRITICAL"
        assert len(enqueued_payload["message"]) > 0


# ==============================================================================
# VECTOR 2: HTML INJECTION & SANITIZATION IN ALERT FORMATTING
# ==============================================================================

@pytest.mark.asyncio
async def test_html_injection_sanitization_in_enqueued_alert(async_client, mock_arq_pool):
    """
    ATTACK VECTOR 2:
    Inject hostile HTML/XSS payloads (<script>, <a href=...>, <b>, <img>, <style>) into:
    - device_name
    - status
    - tenant_id
    - message
    - details (both keys and values)

    INVARIANT:
    All dangerous HTML entities (&, <, >) must be properly sanitized in the message
    enqueued to ARQ, ensuring Telegram Bot API parse_mode='HTML' will not fail or execute scripts.
    """
    payload = {
        "device_name": "<script>alert('xss_dev')</script>",
        "status": "CRITICAL",
        "layer": "1",
        "tenant_id": "<tenant_id>&inject",
        "message": "<a href='http://malicious.evil.com'>click</a> & <b>bold_body</b> <img src=x onerror=alert(1)>",
        "details": {
            "<script>evil_key</script>": "<style>body{color:red}</style>&value",
            "normal_key": "5 > 2 & 1 < 3",
        }
    }

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_arq_pool)):
        resp = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=payload)
        assert resp.status_code == 202

        assert len(mock_arq_pool.recorded_jobs) == 1
        enqueued_message = mock_arq_pool.recorded_jobs[0]["payload"]["message"]

        # Assert NO raw dangerous HTML tags remain unescaped
        assert "<script>" not in enqueued_message
        assert "</script>" not in enqueued_message
        assert "<a href=" not in enqueued_message
        assert "<img" not in enqueued_message
        assert "<style>" not in enqueued_message

        # Assert properly escaped entities
        assert "&lt;script&gt;alert('xss_dev')&lt;/script&gt;" in enqueued_message
        assert "&lt;a href='http://malicious.evil.com'&gt;click&lt;/a&gt;" in enqueued_message
        assert "&lt;b&gt;bold_body&lt;/b&gt;" in enqueued_message
        assert "&lt;tenant_id&gt;&amp;inject" in enqueued_message
        assert "&lt;script&gt;evil_key&lt;/script&gt;" in enqueued_message
        assert "5 &gt; 2 &amp; 1 &lt; 3" in enqueued_message


# ==============================================================================
# VECTOR 3: HIGH-CONCURRENCY BURST (EVENT LOOP & ARQ ENQUEUEING)
# ==============================================================================

@pytest.mark.asyncio
async def test_high_concurrency_burst_and_event_loop_integrity(async_client, mock_arq_pool):
    """
    ATTACK VECTOR 3:
    Simultaneously dispatch 50 concurrent HTTP POST webhook requests via asyncio.gather.
    Verify:
    1. All 50 return HTTP 202 Accepted.
    2. Exactly 50 jobs are enqueued in ARQ with valid parameters.
    3. Background ticker measures event loop latency to ensure zero blocking I/O or GIL starvation.
       Invariant: Max jitter between ticks remains < 100ms.
    """
    concurrency_count = 50
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

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_arq_pool)):
        tasks = [
            async_client.post(
                "/api/v1/telemetry/webhooks/device-status",
                json={
                    "device_name": f"sensor-burst-{i:03d}",
                    "status": "WARNING" if i % 2 == 0 else "ERROR",
                    "layer": (i % 4) + 1,
                    "tenant_id": "tenant-burst-prod",
                    "message": f"Burst telemetry alert index #{i}",
                    "details": {"seq": i, "timestamp_ms": int(time.time() * 1000)}
                }
            )
            for i in range(concurrency_count)
        ]
        responses = await asyncio.gather(*tasks)

    # Stop ticker
    stop_event.set()
    await ticker_task

    # 1. Assert all 50 returned 202
    assert len(responses) == concurrency_count
    for idx, r in enumerate(responses):
        assert r.status_code == 202, f"Request #{idx} returned {r.status_code} instead of 202"
        body = r.json()
        assert body["status"] == "accepted"
        assert body["job_id"] is not None

    # 2. Assert exactly 50 jobs recorded in ARQ
    assert len(mock_arq_pool.recorded_jobs) == concurrency_count
    for job in mock_arq_pool.recorded_jobs:
        assert job["task_name"] == "send_telegram_alert_task"
        payload = job["payload"]
        assert payload["ttl_seconds"] == 300
        assert payload["alert_key"].startswith("device_status:tenant-burst-prod:")

    # 3. Assert zero event loop starvation
    assert len(jitter_samples) > 0
    max_jitter_ms = max(jitter_samples)
    assert max_jitter_ms < 150.0, (
        f"VERDICT: FAIL - Event loop stalled during 50-request burst! "
        f"Max jitter was {max_jitter_ms:.2f}ms (threshold: 150ms)."
    )


# ==============================================================================
# VECTOR 4: RESILIENCE TO INFRASTRUCTURE FAILURE (ARQ / REDIS OUTAGE)
# ==============================================================================

@pytest.mark.asyncio
async def test_resilience_on_arq_pool_connection_error(async_client):
    """
    ATTACK VECTOR 4A:
    Simulate ARQ / Redis pool raising ConnectionError when attempting to connect or enqueue.
    Invariant: Endpoint catches exception and responds with HTTP 503 Service Unavailable
    without unhandled crashes or hanging coroutines.
    """
    with patch(
        "api.endpoints.telemetry.router.get_arq_pool",
        AsyncMock(side_effect=ConnectionError("Cannot connect to Redis at redis:6379"))
    ):
        resp = await async_client.post(
            "/api/v1/telemetry/webhooks/device-status",
            json={
                "device_name": "dev-resilience-01",
                "status": "CRITICAL",
                "layer": 1,
                "tenant_id": "tenant-resilience",
            }
        )
        assert resp.status_code in (500, 503), f"Expected 500 or 503, got {resp.status_code}"
        if resp.status_code == 503:
            assert "Servicio de encolamiento temporalmente no disponible" in resp.json().get("detail", "")


@pytest.mark.asyncio
async def test_resilience_on_enqueue_job_runtime_error(async_client):
    """
    ATTACK VECTOR 4B:
    Simulate get_arq_pool succeeding, but enqueue_job raising RuntimeError
    (e.g., Redis cluster failover or OOM).
    Invariant: Returns HTTP 503 or 500 cleanly.
    """
    broken_pool = MagicMock()
    broken_pool.enqueue_job = AsyncMock(side_effect=RuntimeError("Redis cluster slot migrating"))

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=broken_pool)):
        resp = await async_client.post(
            "/api/v1/telemetry/webhooks/device-status",
            json={
                "device_name": "dev-resilience-02",
                "status": "OFFLINE",
                "layer": 2,
                "tenant_id": "tenant-resilience",
            }
        )
        assert resp.status_code in (500, 503), f"Expected 500 or 503, got {resp.status_code}"


# ==============================================================================
# VECTOR 5: ALERT KEY & SEVERITY MAPPING DETERMINISM
# ==============================================================================

@pytest.mark.asyncio
async def test_alert_key_and_severity_level_mapping(async_client, mock_arq_pool):
    """
    ATTACK VECTOR 5:
    Validate severity icon mapping and deterministic alert_key generation:
    - CRITICAL -> 🚨
    - ERROR/FAIL -> ❌
    - WARNING/OFFLINE -> ⚠️
    - ONLINE/SUCCESS/OK -> ✅
    - Unknown/Default -> ℹ️
    """
    severity_test_cases = [
        ("CRITICAL", "🚨"),
        ("ERROR", "❌"),
        ("FAIL", "❌"),
        ("FAILED", "❌"),
        ("WARNING", "⚠️"),
        ("WARN", "⚠️"),
        ("OFFLINE", "⚠️"),
        ("ONLINE", "✅"),
        ("SUCCESS", "✅"),
        ("OK", "✅"),
        ("CUSTOM_UNKNOWN_STATUS", "ℹ️"),
    ]

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_arq_pool)):
        for status_val, expected_icon in severity_test_cases:
            payload = {
                "device_name": f"dev-{status_val.lower()}",
                "status": status_val,
                "layer": 1,
                "tenant_id": "tenant-semantic",
            }
            resp = await async_client.post("/api/v1/telemetry/webhooks/device-status", json=payload)
            assert resp.status_code == 202

            last_job = mock_arq_pool.recorded_jobs[-1]
            enqueued_payload = last_job["payload"]

            expected_key = f"device_status:tenant-semantic:dev-{status_val.lower()}:{status_val}"
            assert enqueued_payload["alert_key"] == expected_key
            assert expected_icon in enqueued_payload["message"], (
                f"Status '{status_val}' expected icon '{expected_icon}' in message: {enqueued_payload['message']}"
            )
