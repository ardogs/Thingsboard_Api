"""
ADVERSARIAL STRESS TEST SUITE: Telegram Async Service & Redis Debouncer
=======================================================================
Phase 3: Adversarial Review and Stress Testing
Target: MICRO-HITO: Desacoplamiento Agnóstico del Servicio de Telegram
Task ID: task_20261001_telegram_decoupling

Adversarial Objectives:
-----------------------
Aggressively seek out latent concurrency bugs, race conditions, architectural coupling,
event loop stalls, cryptographic flaws, dangling locks, and state corruptions under extreme load.

Key Invariants Tested:
----------------------
A. Agnostic Telegram Service Invariant (CRITICAL):
   - core/services/telegram_service.py MUST be 100% agnostic, dumb, and pure:
     * Zero imports of redis or redis.asyncio.
     * Zero references to redis_client, ALERT_LOCK_PREFIX, or Redis lock keys.
   - Raw send_telegram_message(...) invoked concurrently 50 times MUST execute 50 HTTP POSTs
     with ZERO debouncing/rate-limiting at this layer.
   - MANDATORY VERDICT RULE: If telegram_service touches or interacts with Redis in any way,
     VERDICT is FAIL.

B. Alert Dispatcher Concurrency Burst & Debouncer:
   - 50 concurrent calls with identical message/hash to dispatch_debounced_alert or
     send_telegram_alert_task MUST trigger EXACTLY 1 HTTP POST to Telegram API.
   - Exactly 49 calls MUST be cut dead with {'sent': False, 'reason': 'debounced'}.
   - Multi-key concurrent contention (30 Alpha + 20 Beta) yields exactly 2 HTTP requests.
   - TTL expiration window behaves deterministically.
   - Self-healing: transient errors (429, 500, network error) purge Redis lock immediately.
   - Fail-open: Redis outages do NOT drop mission-critical alerts.
"""

import ast
import asyncio
import hashlib
import os
import pathlib
import sys
import time
import uuid
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx
import fakeredis.aioredis
import redis.asyncio as aioredis
from arq import Retry

# 1. Pure Agnostic Telegram Service (Dumb HTTP Client)
from core.services.telegram_service import (
    escape_html_text,
    format_alert_message,
    send_telegram_message,
)

# 2. Alert Dispatcher & Anti-Spam Debouncer (Domain & Distributed State Layer)
from core.services.alert_dispatcher import (
    ALERT_LOCK_PREFIX,
    calculate_alert_hash,
    get_alert_lock_key,
    dispatch_debounced_alert,
    send_telegram_alert,
)

# 3. Distributed ARQ Task
from workers.tasks import send_telegram_alert_task


# ==============================================================================
# FIXTURES & HELPERS
# ==============================================================================

@pytest.fixture
def fake_redis():
    """Provides an isolated in-memory Async FakeRedis instance for testing atomic locks."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def create_mock_telegram_response(message_id: int = 999001, text: str = "ok") -> MagicMock:
    """Builds a realistic successful Telegram Bot API response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = {
        "ok": True,
        "result": {
            "message_id": message_id,
            "date": 1727611200,
            "chat": {"id": -1001999999999, "type": "channel", "title": "TB Alerts"},
            "text": text,
        },
    }
    resp.raise_for_status = MagicMock()
    return resp


# ==============================================================================
# SECTION 1: AGNOSTIC TELEGRAM SERVICE INVARIANT (CRITICAL ARCHITECTURAL CHECK)
# ==============================================================================

class TestAgnosticTelegramServicePurity:
    """
    Validates that core/services/telegram_service.py has zero dependencies on Redis,
    zero knowledge of locks or debouncers, and functions purely as a dumb HTTP client.
    """

    def test_telegram_service_ast_pure_and_no_redis_references(self):
        """
        CRITICAL ARCHITECTURAL INVARIANT:
        Inspects the AST and source code of core/services/telegram_service.py.
        Asserts that:
        1. NO 'import redis', 'import redis.asyncio', or 'from redis import ...' exists.
        2. NO references to 'redis_client', 'ALERT_LOCK_PREFIX', or 'tb_alert_lock' exist.
        3. NO mention of Redis connection objects or lock keys.
        """
        service_path = pathlib.Path(__file__).resolve().parents[2] / "core" / "services" / "telegram_service.py"
        assert service_path.exists(), f"Source file not found at {service_path}"

        source_code = service_path.read_text(encoding="utf-8")
        parsed_ast = ast.parse(source_code, filename=str(service_path))

        disallowed_imports = {"redis", "fakeredis", "redis_client"}
        disallowed_identifiers = {"ALERT_LOCK_PREFIX", "tb_alert_lock", "redis_conn", "redis_client", "get_redis_client"}

        found_forbidden_imports = []
        found_forbidden_identifiers = []

        for node in ast.walk(parsed_ast):
            # Check imports
            if isinstance(node, ast.Import):
                for alias in node.names:
                    mod_root = alias.name.split(".")[0]
                    if mod_root in disallowed_imports:
                        found_forbidden_imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    mod_root = node.module.split(".")[0]
                    if mod_root in disallowed_imports:
                        found_forbidden_imports.append(node.module)

            # Check identifier names
            if isinstance(node, ast.Name):
                if node.id in disallowed_identifiers:
                    found_forbidden_identifiers.append(node.id)

        assert not found_forbidden_imports, (
            f"MANDATORY VERDICT: FAIL - telegram_service.py violates architectural decoupling! "
            f"Found forbidden imports: {found_forbidden_imports}"
        )

        assert not found_forbidden_identifiers, (
            f"MANDATORY VERDICT: FAIL - telegram_service.py violates architectural decoupling! "
            f"Found forbidden Redis-related identifiers: {found_forbidden_identifiers}"
        )

    @pytest.mark.asyncio
    async def test_raw_send_telegram_message_concurrent_burst_no_debouncing(self):
        """
        CRITICAL BEHAVIORAL INVARIANT:
        When calling raw send_telegram_message(...) concurrently 50 times with identical messages
        and without any Redis parameters, ALL 50 requests MUST be dispatched via HTTP.
        No debouncing, no rate limiting, and zero interaction with Redis must occur at this layer.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)

        async def quick_post(*args, **kwargs):
            await asyncio.sleep(0.005)
            return create_mock_telegram_response(message_id=123)

        mock_http_client.post.side_effect = quick_post

        concurrency_count = 50
        message = "Agnostic burst notification with zero debouncing"
        bot_token = "agnostic_test_token"
        chat_id = "-1009999999"

        tasks = [
            send_telegram_message(
                message=message,
                chat_id=chat_id,
                bot_token=bot_token,
                http_client=mock_http_client,
            )
            for _ in range(concurrency_count)
        ]

        results = await asyncio.gather(*tasks)

        # Invariant 1: Exactly 50 HTTP calls dispatched (NO DEBOUNCING)
        assert mock_http_client.post.call_count == concurrency_count, (
            f"MANDATORY VERDICT: FAIL - send_telegram_message must be agnostic and pure! "
            f"Expected {concurrency_count} HTTP calls, but got {mock_http_client.post.call_count}"
        )

        # Invariant 2: All 50 returned sent=True
        assert all(r.get("sent") is True for r in results), (
            "Expected all 50 calls to return sent=True in agnostic client."
        )

    @pytest.mark.asyncio
    async def test_raw_send_telegram_message_absorbs_legacy_kwargs_without_redis(self, fake_redis):
        """
        Verifies backward compatibility: passing legacy parameters (redis_conn, alert_key,
        ttl_seconds, skip_debounce) to send_telegram_message does not fail with TypeError
        and NEVER touches Redis.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_http_client.post.return_value = create_mock_telegram_response()

        res = await send_telegram_message(
            message="Legacy kwargs test message",
            chat_id="-1009999999",
            bot_token="test_token",
            http_client=mock_http_client,
            # Legacy kwargs that should be safely absorbed
            redis_conn=fake_redis,
            alert_key="legacy_alert_key",
            ttl_seconds=120,
            skip_debounce=False,
            release_lock_on_failure=True,
        )

        assert res.get("sent") is True
        assert mock_http_client.post.call_count == 1

        # Invariant: Redis was completely untouched
        keys = await fake_redis.keys("*")
        assert len(keys) == 0, f"Expected 0 keys in Redis, found: {keys}"


# ==============================================================================
# SECTION 2: ALERT DISPATCHER CONCURRENCY BURST & DEBOUNCER INVARIANTS
# ==============================================================================

class TestAlertDispatcherDebouncerAndConcurrency:
    """
    Stress tests verifying the anti-spam debouncing circuit in core/services/alert_dispatcher.py
    and its ARQ worker task integration.
    """

    @pytest.mark.asyncio
    async def test_burst_concurrency_race_condition_dispatch_debounced_alert(self, fake_redis):
        """
        ATTACK VECTOR 1A:
        Launch 50 identical concurrent coroutines against dispatch_debounced_alert using asyncio.gather.
        Inject simulated network latency (15ms) on the mock transport to simulate network inflight
        state while other coroutines contend for the Redis lock.

        ASSERTIONS:
        - mock_post.call_count MUST be exactly 1.
        - Exactly 1 coroutine returns {'sent': True, ...}.
        - Exactly 49 coroutines return {'sent': False, 'reason': 'debounced', ...}.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)

        async def delayed_post(*args, **kwargs):
            await asyncio.sleep(0.015)
            return create_mock_telegram_response(message_id=777)

        mock_http_client.post.side_effect = delayed_post

        message = "CRITICAL: Database connection pool exhausted on worker-04"
        bot_token = "adv_test_bot_token"
        chat_id = "-100123456789"
        concurrency_count = 50

        tasks = [
            dispatch_debounced_alert(
                message=message,
                bot_token=bot_token,
                chat_id=chat_id,
                ttl_seconds=300,
                redis_conn=fake_redis,
                http_client=mock_http_client,
            )
            for _ in range(concurrency_count)
        ]

        results = await asyncio.gather(*tasks)

        # 1. Assert exactly 1 HTTP call
        assert mock_http_client.post.call_count == 1, (
            f"VERDICT: FAIL - Concurrency race condition detected in dispatch_debounced_alert! "
            f"Expected exactly 1 HTTP request, but got {mock_http_client.post.call_count}"
        )

        # 2. Partition results
        sent_results = [r for r in results if r.get("sent") is True]
        debounced_results = [r for r in results if r.get("sent") is False and r.get("reason") == "debounced"]

        assert len(sent_results) == 1, f"Expected 1 sent alert, got {len(sent_results)}"
        assert len(debounced_results) == concurrency_count - 1, (
            f"Expected {concurrency_count - 1} debounced alerts, got {len(debounced_results)}"
        )

        # 3. Assert all returned the exact same alert hash
        expected_hash = calculate_alert_hash(message)
        for r in results:
            assert r.get("alert_hash") == expected_hash

        # 4. Verify lock exists in Redis
        lock_key = get_alert_lock_key(expected_hash)
        val = await fake_redis.get(lock_key)
        assert val is not None

    @pytest.mark.asyncio
    async def test_burst_concurrency_race_condition_arq_worker_task(self, fake_redis):
        """
        ATTACK VECTOR 1B:
        Launch 50 identical concurrent calls to send_telegram_alert_task (ARQ Worker layer).
        Ensures that worker wrapper doesn't inadvertently bypass debouncing or mismanage ctx.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)

        async def delayed_post(*args, **kwargs):
            await asyncio.sleep(0.01)
            return create_mock_telegram_response(message_id=888)

        mock_http_client.post.side_effect = delayed_post

        concurrency_count = 50
        alert_payload = {
            "message": "WARNING: Memory usage > 90% on TB-Server-01",
            "bot_token": "adv_test_bot_token",
            "chat_id": "-100123456789",
            "ttl_seconds": 120,
        }

        tasks = [
            send_telegram_alert_task(
                ctx={"job_id": f"burst_job_{i}", "redis": fake_redis, "http_client": mock_http_client},
                payload=alert_payload,
            )
            for i in range(concurrency_count)
        ]

        results = await asyncio.gather(*tasks)

        # Assert exactly 1 HTTP post
        assert mock_http_client.post.call_count == 1, (
            f"VERDICT: FAIL - ARQ task concurrency leak! HTTP calls: {mock_http_client.post.call_count}"
        )

        sent_count = sum(1 for r in results if r.get("sent") is True)
        debounced_count = sum(1 for r in results if r.get("reason") == "debounced")

        assert sent_count == 1
        assert debounced_count == concurrency_count - 1

    @pytest.mark.asyncio
    async def test_multi_key_partitioned_burst_concurrency(self, fake_redis):
        """
        ATTACK VECTOR 2:
        Contention stress across multiple distinct alert topics simultaneously:
        - 30 coroutines firing Alert-Alpha ("Disk full on /dev/sda1")
        - 20 coroutines firing Alert-Beta ("CPU spike > 98%")
        All 50 tasks launched simultaneously via asyncio.gather.

        ASSERTIONS:
        - Exactly 2 HTTP requests executed (1 for Alpha, 1 for Beta).
        - Exactly 29 Alpha alerts debounced.
        - Exactly 19 Beta alerts debounced.
        - Zero state bleed between distinct keys.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)

        async def delayed_post(*args, **kwargs):
            await asyncio.sleep(0.01)
            return create_mock_telegram_response()

        mock_http_client.post.side_effect = delayed_post

        tasks = []
        # 30 Alpha tasks
        for i in range(30):
            tasks.append(
                dispatch_debounced_alert(
                    message="Disk full on /dev/sda1",
                    alert_key="disk_full_sda1",
                    bot_token="token_multi",
                    chat_id="chat_multi",
                    redis_conn=fake_redis,
                    http_client=mock_http_client,
                )
            )
        # 20 Beta tasks
        for j in range(20):
            tasks.append(
                dispatch_debounced_alert(
                    message="CPU spike > 98%",
                    alert_key="cpu_spike_node2",
                    bot_token="token_multi",
                    chat_id="chat_multi",
                    redis_conn=fake_redis,
                    http_client=mock_http_client,
                )
            )

        results = await asyncio.gather(*tasks)

        # Invariant: exactly 2 HTTP requests
        assert mock_http_client.post.call_count == 2, (
            f"VERDICT: FAIL - Expected 2 HTTP requests (1 per distinct key), got {mock_http_client.post.call_count}"
        )

        alpha_results = results[:30]
        beta_results = results[30:]

        assert sum(1 for r in alpha_results if r.get("sent") is True) == 1
        assert sum(1 for r in alpha_results if r.get("reason") == "debounced") == 29

        assert sum(1 for r in beta_results if r.get("sent") is True) == 1
        assert sum(1 for r in beta_results if r.get("reason") == "debounced") == 19

    @pytest.mark.asyncio
    async def test_debouncer_ttl_expiration_and_subsequent_window(self, fake_redis):
        """
        ATTACK VECTOR 3:
        Verify that once the debounce TTL window expires, subsequent identical alerts
        are permitted and result in a new HTTP request.

        Steps:
        1. Send Alert 1 with ttl_seconds=1. (HTTP call #1)
        2. Immediate identical Alert 2 -> Debounced (HTTP calls stay 1).
        3. Sleep 1.05s to allow Redis TTL expiration.
        4. Send identical Alert 3 -> Admitted (HTTP call #2).
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_http_client.post.return_value = create_mock_telegram_response(message_id=101)

        message = "INFO: Scheduled backup started for Tenant A"
        bot_token = "token123"
        chat_id = "chat123"

        # Step 1: Initial alert
        r1 = await dispatch_debounced_alert(
            message=message,
            bot_token=bot_token,
            chat_id=chat_id,
            ttl_seconds=1,
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )
        assert r1["sent"] is True
        assert mock_http_client.post.call_count == 1

        # Step 2: Immediate duplicate within TTL window
        r2 = await dispatch_debounced_alert(
            message=message,
            bot_token=bot_token,
            chat_id=chat_id,
            ttl_seconds=1,
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )
        assert r2["sent"] is False
        assert r2["reason"] == "debounced"
        assert mock_http_client.post.call_count == 1

        # Step 3: Wait for Redis TTL expiration
        await asyncio.sleep(1.05)

        # Step 4: Subsequent window - should acquire lock and send new alert
        r3 = await dispatch_debounced_alert(
            message=message,
            bot_token=bot_token,
            chat_id=chat_id,
            ttl_seconds=1,
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )
        assert r3["sent"] is True
        assert mock_http_client.post.call_count == 2, (
            f"VERDICT: FAIL - Alert not sent after TTL expiration. Call count: {mock_http_client.post.call_count}"
        )

    @pytest.mark.asyncio
    async def test_skip_debounce_and_alert_isolation(self, fake_redis):
        """
        ATTACK VECTOR 4:
        1. Verify skip_debounce=True allows repetitive alerts without debouncing.
        2. Verify alerts with different messages have independent hashes and don't block each other.
        3. Verify alerts with the same message but different alert_keys don't block each other.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_http_client.post.return_value = create_mock_telegram_response()

        bot_token = "token123"
        chat_id = "chat123"
        msg = "Routine heartbeat message"

        # Part A: skip_debounce=True
        for _ in range(3):
            res = await dispatch_debounced_alert(
                message=msg,
                bot_token=bot_token,
                chat_id=chat_id,
                skip_debounce=True,
                redis_conn=fake_redis,
                http_client=mock_http_client,
            )
            assert res["sent"] is True
        assert mock_http_client.post.call_count == 3

        # Part B: Different messages isolation
        msg_a = "Alert Node A down"
        msg_b = "Alert Node B down"

        res_a = await dispatch_debounced_alert(
            message=msg_a,
            bot_token=bot_token,
            chat_id=chat_id,
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )
        res_b = await dispatch_debounced_alert(
            message=msg_b,
            bot_token=bot_token,
            chat_id=chat_id,
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )
        assert res_a["sent"] is True
        assert res_b["sent"] is True
        assert res_a["alert_hash"] != res_b["alert_hash"]
        assert mock_http_client.post.call_count == 5

        # Part C: Same message, different alert_key isolation
        common_msg = "Device disconnected"
        res_dev1 = await dispatch_debounced_alert(
            message=common_msg,
            alert_key="device:dev_001",
            bot_token=bot_token,
            chat_id=chat_id,
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )
        res_dev2 = await dispatch_debounced_alert(
            message=common_msg,
            alert_key="device:dev_002",
            bot_token=bot_token,
            chat_id=chat_id,
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )
        assert res_dev1["sent"] is True
        assert res_dev2["sent"] is True
        assert res_dev1["alert_hash"] != res_dev2["alert_hash"]
        assert mock_http_client.post.call_count == 7

    @pytest.mark.asyncio
    async def test_event_loop_latency_and_zero_blocking(self, fake_redis):
        """
        ATTACK VECTOR 5:
        Measure event loop lag using a background ticker coroutine during 50 concurrent calls.
        Verifies that hash calculation (SHA-256), HTML escaping, and Redis operations execute
        without blocking the asyncio event loop or causing GIL starvation.

        Invariant: Max jitter between expected tick and actual tick MUST remain < 75ms.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)

        async def quick_post(*args, **kwargs):
            await asyncio.sleep(0.005)
            return create_mock_telegram_response()

        mock_http_client.post.side_effect = quick_post

        tick_interval = 0.005  # 5ms tick
        jitter_samples: List[float] = []
        stop_event = asyncio.Event()

        async def event_loop_ticker():
            while not stop_event.is_set():
                t0 = time.perf_counter()
                await asyncio.sleep(tick_interval)
                t1 = time.perf_counter()
                jitter = (t1 - t0) - tick_interval
                jitter_samples.append(jitter)

        ticker_task = asyncio.create_task(event_loop_ticker())

        burst_msg = "CRITICAL: Event loop probe test message under burst"
        tasks = [
            dispatch_debounced_alert(
                message=burst_msg,
                bot_token="token123",
                chat_id="chat123",
                redis_conn=fake_redis,
                http_client=mock_http_client,
            )
            for _ in range(50)
        ]
        await asyncio.gather(*tasks)

        stop_event.set()
        await ticker_task

        assert len(jitter_samples) > 0
        max_jitter_ms = max(jitter_samples) * 1000.0

        assert max_jitter_ms < 75.0, (
            f"VERDICT: FAIL - Event loop stalled! Max jitter was {max_jitter_ms:.2f}ms (threshold: 75.0ms)."
        )

    @pytest.mark.asyncio
    async def test_transient_failure_lock_release_and_recovery(self, fake_redis):
        """
        ATTACK VECTOR 6A:
        Test Telegram API returning HTTP 429 (Rate Limit) and verify that:
        1. Lock is deleted from Redis when release_lock_on_failure=True.
        2. A subsequent call can immediately re-acquire the lock without waiting for TTL.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("POST", "https://api.telegram.org")
        resp_429 = httpx.Response(429, request=req, headers={"Retry-After": "15"}, text='{"ok":false,"error_code":429}')
        mock_http_client.post.side_effect = httpx.HTTPStatusError("429 Too Many Requests", request=req, response=resp_429)

        message = "CRITICAL: Out of memory on cluster node 3"
        bot_token = "token123"
        chat_id = "chat123"
        alert_hash = calculate_alert_hash(message)
        lock_key = get_alert_lock_key(alert_hash)

        # 1. First attempt fails with 429 (raise_on_error=False)
        res1 = await dispatch_debounced_alert(
            message=message,
            bot_token=bot_token,
            chat_id=chat_id,
            ttl_seconds=300,
            redis_conn=fake_redis,
            http_client=mock_http_client,
            raise_on_error=False,
            release_lock_on_failure=True,
        )
        assert res1["sent"] is False
        assert res1["reason"] == "http_error"
        assert res1["status_code"] == 429

        # 2. Invariant: Lock MUST have been purged from Redis
        lock_val = await fake_redis.get(lock_key)
        assert lock_val is None, "VERDICT: FAIL - Lock was NOT released after HTTP 429 error!"

        # 3. Simulate recovery on second attempt
        mock_http_client.post.side_effect = None
        mock_http_client.post.return_value = create_mock_telegram_response(message_id=555)

        res2 = await dispatch_debounced_alert(
            message=message,
            bot_token=bot_token,
            chat_id=chat_id,
            ttl_seconds=300,
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )
        assert res2["sent"] is True
        assert res2["message_id"] == 555
        assert await fake_redis.get(lock_key) is not None

    @pytest.mark.asyncio
    async def test_telegram_api_ok_false_releases_lock(self, fake_redis):
        """
        ATTACK VECTOR 6B:
        Telegram returns HTTP 200 but payload has {"ok": false, "description": "Bad Request: chat not found"}.
        Invariant: Lock MUST be released to prevent locking out future corrected attempts.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.json.return_value = {"ok": False, "description": "Bad Request: chat not found"}
        resp.raise_for_status = MagicMock()
        resp.request = httpx.Request("POST", "https://api.telegram.org")
        resp.text = '{"ok": false, "description": "Bad Request: chat not found"}'
        mock_http_client.post.return_value = resp

        message = "Chat ID verification alert"
        alert_hash = calculate_alert_hash(message)
        lock_key = get_alert_lock_key(alert_hash)

        res = await dispatch_debounced_alert(
            message=message,
            bot_token="token123",
            chat_id="invalid_chat",
            redis_conn=fake_redis,
            http_client=mock_http_client,
            raise_on_error=False,
            release_lock_on_failure=True,
        )

        assert res["sent"] is False
        assert res["reason"] == "http_error"
        # Invariant: Lock released
        assert await fake_redis.get(lock_key) is None

    @pytest.mark.asyncio
    async def test_transient_failure_contrast_with_release_lock_false(self, fake_redis):
        """
        ATTACK VECTOR 6C:
        Verify contrast: when release_lock_on_failure=False, the lock REMAINS held after failure,
        causing subsequent calls to be debounced.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("POST", "https://api.telegram.org")
        resp_500 = httpx.Response(500, request=req, text="Internal Server Error")
        mock_http_client.post.side_effect = httpx.HTTPStatusError("500 Server Error", request=req, response=resp_500)

        message = "CRITICAL: Database offline"
        bot_token = "token123"
        chat_id = "chat123"
        alert_hash = calculate_alert_hash(message)
        lock_key = get_alert_lock_key(alert_hash)

        # 1. Call with release_lock_on_failure=False
        res1 = await dispatch_debounced_alert(
            message=message,
            bot_token=bot_token,
            chat_id=chat_id,
            ttl_seconds=300,
            redis_conn=fake_redis,
            http_client=mock_http_client,
            raise_on_error=False,
            release_lock_on_failure=False,
        )
        assert res1["sent"] is False

        # Lock must remain held in Redis
        assert await fake_redis.get(lock_key) is not None

        # Subsequent call is debounced despite previous failure
        res2 = await dispatch_debounced_alert(
            message=message,
            bot_token=bot_token,
            chat_id=chat_id,
            ttl_seconds=300,
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )
        assert res2["sent"] is False
        assert res2["reason"] == "debounced"

    @pytest.mark.asyncio
    async def test_arq_task_transient_retry_and_lock_purging(self, fake_redis):
        """
        ATTACK VECTOR 6D:
        Verify ARQ worker task level behavior on transient network errors:
        1. httpx.ConnectError raises arq.Retry.
        2. Redis lock is liberated during the exception unwinding.
        3. The next worker execution (or retry) has an unblocked path to Redis.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_http_client.post.side_effect = httpx.ConnectError("Connection refused by gateway")

        message = "Connection refused test alert"
        alert_hash = calculate_alert_hash(message)
        lock_key = get_alert_lock_key(alert_hash)

        ctx = {
            "job_id": "job_retry_test_1",
            "job_try": 1,
            "redis": fake_redis,
            "http_client": mock_http_client,
        }
        payload = {
            "message": message,
            "bot_token": "token123",
            "chat_id": "chat123",
        }

        # First attempt raises Retry
        with pytest.raises(Retry) as exc_info:
            await send_telegram_alert_task(ctx, payload=payload)

        assert exc_info.value.defer_score >= 2

        # Invariant: Lock in Redis MUST be released!
        assert await fake_redis.get(lock_key) is None

        # Second attempt: Network recovers -> Task succeeds!
        mock_http_client.post.side_effect = None
        mock_http_client.post.return_value = create_mock_telegram_response(message_id=909)

        ctx["job_try"] = 2
        res_retry = await send_telegram_alert_task(ctx, payload=payload)
        assert res_retry["sent"] is True
        assert res_retry["message_id"] == 909

    @pytest.mark.asyncio
    async def test_fail_open_on_redis_catastrophic_failure(self):
        """
        ATTACK VECTOR 7:
        Simulate a complete Redis partition/outage where redis.set raises redis.RedisError.
        Design Invariant: The system MUST fail-open and deliver the critical alert rather than
        crashing or swallowing the notification silently.
        """
        broken_redis = AsyncMock()
        broken_redis.set.side_effect = aioredis.RedisError("Cluster connection unreachable: 6379")

        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_http_client.post.return_value = create_mock_telegram_response(message_id=1234)

        res = await dispatch_debounced_alert(
            message="EMERGENCY: Fire alarm triggered in Datacenter 2",
            bot_token="token123",
            chat_id="chat123",
            redis_conn=broken_redis,
            http_client=mock_http_client,
        )

        assert res["sent"] is True
        assert res["message_id"] == 1234
        assert mock_http_client.post.call_count == 1

    @pytest.mark.asyncio
    async def test_extreme_payload_truncation(self, fake_redis):
        """
        ATTACK VECTOR 8A:
        Telegram Bot API has an absolute limit of 4096 UTF-8 characters per message.
        Pass an oversized message (12,000 characters) and assert that:
        1. It does NOT raise an error.
        2. The message payload is truncated safely to <= 4096 chars (ending in '...').
        3. Hash calculation remains deterministic.
        """
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_http_client.post.return_value = create_mock_telegram_response()

        oversized_message = "A" * 12000
        res = await dispatch_debounced_alert(
            message=oversized_message,
            bot_token="token123",
            chat_id="chat123",
            redis_conn=fake_redis,
            http_client=mock_http_client,
        )

        assert res["sent"] is True
        sent_payload = mock_http_client.post.call_args.kwargs["json"]
        sent_text = sent_payload["text"]

        assert len(sent_text) <= 4096
        assert len(sent_text) == 4093  # 4090 + '...'
        assert sent_text.endswith("...")

    def test_html_injection_sanitization(self):
        """
        ATTACK VECTOR 8B:
        Adversarial inputs containing unclosed or malicious HTML tags must be sanitized
        to prevent Telegram parse errors (parse_mode='HTML').
        """
        raw_title = "<script>alert('XSS')</script> & Danger <root>"
        raw_body = "User <admin> injected code: 5 > 2 & 1 < 3"
        tags = ["<PROD>", "CRIT&ALERT"]
        details = {"key<1>": "value&2", "nested<tag>": "test"}

        formatted = format_alert_message(
            title=raw_title,
            body=raw_body,
            level="CRITICAL",
            tags=tags,
            details=details,
        )

        assert "<script>" not in formatted
        assert "&lt;script&gt;" in formatted
        assert "&amp;" in formatted
        assert "&lt;admin&gt;" in formatted
        assert "#&lt;PROD&gt;" in formatted
        assert "• <b>key&lt;1&gt;</b>: <code>value&amp;2</code>" in formatted

    @pytest.mark.asyncio
    async def test_live_redis_concurrency_burst(self):
        """
        ATTACK VECTOR 9:
        Executes a 50-task concurrent burst against the real local Redis instance (localhost:6379)
        if accessible. Validates that true Redis atomic SET NX EX performs identically to the mock.
        """
        real_redis = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
        try:
            await real_redis.ping()
        except Exception as e:
            pytest.skip(f"Live Redis server not accessible on localhost:6379 ({e}). Skipping live test.")

        test_uid = uuid.uuid4().hex[:8]
        test_key = f"adv_live_test_{test_uid}"
        message = f"Live Redis Concurrency Probe Test {test_uid}"
        expected_hash = calculate_alert_hash(message, alert_key=test_key)
        lock_key = get_alert_lock_key(expected_hash)

        mock_http_client = AsyncMock(spec=httpx.AsyncClient)

        async def delayed_post(*args, **kwargs):
            await asyncio.sleep(0.01)
            return create_mock_telegram_response()

        mock_http_client.post.side_effect = delayed_post

        try:
            tasks = [
                dispatch_debounced_alert(
                    message=message,
                    alert_key=test_key,
                    ttl_seconds=30,
                    bot_token="token_live",
                    chat_id="chat_live",
                    redis_conn=real_redis,
                    http_client=mock_http_client,
                )
                for _ in range(50)
            ]
            results = await asyncio.gather(*tasks)

            assert mock_http_client.post.call_count == 1, (
                f"VERDICT: FAIL - Live Redis permitted multiple locks! Count: {mock_http_client.post.call_count}"
            )

            sent_count = sum(1 for r in results if r.get("sent") is True)
            debounced_count = sum(1 for r in results if r.get("reason") == "debounced")

            assert sent_count == 1
            assert debounced_count == 49

        finally:
            await real_redis.delete(lock_key)
            await real_redis.aclose()
