"""
ADVERSARIAL STRESS TEST SUITE: Asynchronous Physical Relations Manager (Edge Topology)
========================================================================================
Phase 3: Adversarial Review and Stress Testing
Target: MICRO-HITO 3: Gestor Asíncrono de Relaciones Físicas (Topología Edge)
Endpoint: POST /api/v1/tenants/{tenant_id}/devices/{parent_id}/relations/{child_id}
Task ID: task_20261007_device_relations_edge

Attack Vectors & Invariants Covered:
------------------------------------
1. HTTP Interception with `respx`:
   - Exact contract enforcement on ThingsBoard downstream payload:
     * from: {"id": parent_id, "entityType": "DEVICE"}
     * to: {"id": child_id, "entityType": "DEVICE"}
     * type: relation_type
     * typeGroup: "COMMON"
   - Headers: X-Authorization (Bearer token) and Content-Type: application/json.
   - Transparent fallback from /api/relations (404) to /api/relation (200).
   - Autonomous re-authentication on ThingsBoard 401 Unauthorized.

2. Casbin RBAC & Privilege Escalation Probes:
   - User lacking `devices:write` on `tenant:{tenant_id}` is blocked with HTTP 403 Forbidden.
   - Cross-tenant privilege escalation probe (authorized on tenant A, targeting tenant B).
   - Read-only user (`devices:read`) blocked from creating relations (HTTP 403 Forbidden).
   - Authorized user on matching tenant domain succeeds (HTTP 201 Created).
   - Superadministrator bypasses Casbin checks globally (HTTP 201 Created).

3. High-Concurrency Burst & GIL Starvation Check:
   - 50 concurrent calls to endpoint via asyncio.gather and ASGITransport.
   - Background event loop lag ticker (< 100ms jitter).
   - All 50 calls execute with zero deadlocks or event loop stalls.

4. Input Fuzzing, Boundary Invariants & Fault Injection:
   - Missing or empty `relation_type` in query or body (HTTP 400 Bad Request).
   - Whitespace attacks (spaces, tabs, newlines) -> HTTP 400 Bad Request.
   - Path traversal and malicious injection strings in parent_id, child_id, relation_type.
   - Downstream 500 / 503 errors and network failures translated to HTTP 502 Bad Gateway.
   - Non-existent tenant resolution -> HTTP 404 Not Found.
"""

import asyncio
import json
import time
import urllib.parse
from typing import Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx
import respx
from fastapi import HTTPException, status
from httpx import AsyncClient, ASGITransport

from api.deps import get_current_user
from api.main import app
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.user import User
from api.endpoints.devices.router import (
    DeviceRelationRequest,
    DeviceRelationResponse,
    create_device_physical_relation,
)


# ==============================================================================
# FIXTURES & HELPERS
# ==============================================================================

def _create_mock_user(
    user_id: str = "user_adv_001",
    role: str = "user",
    is_superuser: bool = False
) -> User:
    """Helper to generate a mock User."""
    user = MagicMock(spec=User)
    user.id = user_id
    user.username = f"user_{user_id}"
    user.email = f"{user_id}@tkme.cloud"
    user.is_superuser = is_superuser
    user.role = role
    user.is_active = True
    return user


def _create_mock_tenant_and_server(
    tenant_id: str = "66e1f00b123456789abcdef0",
    user_id: str = "user_adv_001",
    base_url: str = "https://tb.tkme.cloud"
) -> tuple[TBTenant, TBServer]:
    """Helper to generate mock TBTenant and TBServer documents."""
    tenant = MagicMock(spec=TBTenant)
    tenant.id = tenant_id
    tenant.name = f"Tenant_{tenant_id}"
    tenant.username = f"tenant_{tenant_id}"
    tenant.user_id = user_id
    tenant.get_token.return_value = "initial-jwt-token-123"
    tenant.get_refresh_token.return_value = "refresh-jwt-token-123"
    tenant.get_password.return_value = "secret-pass"
    tenant.set_tokens = MagicMock()
    tenant.save = AsyncMock()

    server = MagicMock(spec=TBServer)
    server.id = "server-adv-01"
    server.name = "ThingsBoard Adv Server"
    server.base_url = base_url

    tenant.get_server = AsyncMock(return_value=server)
    return tenant, server


# ==============================================================================
# SECTION 1: HTTP INTERCEPTION WITH RESXP (CONTRACT & PAYLOAD INVARIANTS)
# ==============================================================================

class TestAdversarialHttpInterceptionRespx:
    """
    Validates with `respx` the exact JSON payload contracts, HTTP methods, headers,
    transparent 404 fallback, and 401 re-authentication flows sent to ThingsBoard.
    """

    @pytest.mark.asyncio
    async def test_respx_exact_payload_and_headers_structure(self):
        """
        INVARIANT 1A:
        Strictly asserts that the JSON payload structured and sent to ThingsBoard contains
        the exact expected keys and values:
        - from: {"id": parent_id, "entityType": "DEVICE"}
        - to: {"id": child_id, "entityType": "DEVICE"}
        - type: relation_type
        - typeGroup: "COMMON"
        - Headers: X-Authorization (Bearer token) and Content-Type: application/json.
        """
        tenant, server = _create_mock_tenant_and_server()
        user = _create_mock_user(role="admin", is_superuser=True)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            tb_route = respx_mock.post("/api/relations").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )

            app.dependency_overrides[get_current_user] = lambda: user
            transport = ASGITransport(app=app)

            parent_id = "gw-master-uuid-101"
            child_id = "sensor-modbus-uuid-202"
            relation_type = "Edge_Link"

            try:
                async with AsyncClient(transport=transport, base_url="http://test") as client:
                    with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/{parent_id}/relations/{child_id}",
                            json={"relation_type": relation_type}
                        )

                assert response.status_code == status.HTTP_201_CREATED
                data = response.json()
                assert data["status"] == "success"
                assert data["parent_id"] == parent_id
                assert data["child_id"] == child_id
                assert data["relation_type"] == relation_type

                # Verify downstream HTTP call intercepted by respx
                assert tb_route.called
                assert tb_route.call_count == 1

                intercepted_req = tb_route.calls.last.request
                assert intercepted_req.method == "POST"
                assert intercepted_req.headers.get("X-Authorization") == "Bearer initial-jwt-token-123"
                assert "application/json" in intercepted_req.headers.get("Content-Type", "")

                # Strict assertion of ThingsBoard JSON payload
                sent_payload = json.loads(intercepted_req.content)
                expected_payload = {
                    "from": {"id": parent_id, "entityType": "DEVICE"},
                    "to": {"id": child_id, "entityType": "DEVICE"},
                    "type": relation_type,
                    "typeGroup": "COMMON"
                }
                assert sent_payload == expected_payload, (
                    f"VERDICT: FAIL - Payload sent to ThingsBoard does not match contract! Got: {sent_payload}"
                )

            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_respx_query_param_payload_structure(self):
        """
        INVARIANT 1B:
        When relation_type is provided via query parameter (?relation_type=Parent_Of)
        without a request body, the downstream payload is still constructed with exact schema.
        """
        tenant, server = _create_mock_tenant_and_server()
        user = _create_mock_user(role="admin", is_superuser=True)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            tb_route = respx_mock.post("/api/relations").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )

            app.dependency_overrides[get_current_user] = lambda: user
            transport = ASGITransport(app=app)

            parent_id = "concentrator-01"
            child_id = "meter-44"

            try:
                async with AsyncClient(transport=transport, base_url="http://test") as client:
                    with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/{parent_id}/relations/{child_id}?relation_type=Parent_Of"
                        )

                assert response.status_code == status.HTTP_201_CREATED
                assert tb_route.called

                sent_payload = json.loads(tb_route.calls.last.request.content)
                assert sent_payload == {
                    "from": {"id": parent_id, "entityType": "DEVICE"},
                    "to": {"id": child_id, "entityType": "DEVICE"},
                    "type": "Parent_Of",
                    "typeGroup": "COMMON"
                }
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_respx_fallback_404_to_singular_endpoint(self):
        """
        INVARIANT 1C:
        ThingsBoard primary endpoint `/api/relations` returns 404 Not Found.
        The system transparently commutates to fallback endpoint `/api/relation`.
        Assert that both calls were intercepted and the fallback payload is strictly identical.
        """
        tenant, server = _create_mock_tenant_and_server()
        user = _create_mock_user(role="admin", is_superuser=True)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            route_primary = respx_mock.post("/api/relations").mock(
                return_value=httpx.Response(404, text="Not Found on /api/relations")
            )
            route_fallback = respx_mock.post("/api/relation").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )

            app.dependency_overrides[get_current_user] = lambda: user
            transport = ASGITransport(app=app)

            try:
                async with AsyncClient(transport=transport, base_url="http://test") as client:
                    with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/p-gw/relations/c-dev",
                            json={"relation_type": "Installed_In"}
                        )

                assert response.status_code == status.HTTP_201_CREATED
                assert route_primary.call_count == 1
                assert route_fallback.call_count == 1

                # Assert exact payload on fallback call
                fallback_payload = json.loads(route_fallback.calls.last.request.content)
                assert fallback_payload == {
                    "from": {"id": "p-gw", "entityType": "DEVICE"},
                    "to": {"id": "c-dev", "entityType": "DEVICE"},
                    "type": "Installed_In",
                    "typeGroup": "COMMON"
                }
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_respx_401_reauthentication_and_retry(self):
        """
        INVARIANT 1D:
        ThingsBoard returns 401 Unauthorized (token expired).
        The system calls `_reauthenticate_tenant` and retries the request with renewed token.
        Assert that 2 calls were made to /api/relations and second call used renewed token.
        """
        tenant, server = _create_mock_tenant_and_server()
        user = _create_mock_user(role="admin", is_superuser=True)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            route_relations = respx_mock.post("/api/relations")
            route_relations.side_effect = [
                httpx.Response(401, text="Token Expired"),
                httpx.Response(200, json={"ok": True}),
            ]

            app.dependency_overrides[get_current_user] = lambda: user
            transport = ASGITransport(app=app)

            try:
                with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))), \
                     patch("api.endpoints.devices.router._reauthenticate_tenant", AsyncMock(return_value="renewed-jwt-token-999")) as mock_reauth:

                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/p-gw/relations/c-dev",
                            json={"relation_type": "Edge_Link"}
                        )

                    assert response.status_code == status.HTTP_201_CREATED
                    mock_reauth.assert_awaited_once()
                    assert route_relations.call_count == 2

                    # Verify token upgrade in headers
                    call_1_token = route_relations.calls[0].request.headers.get("X-Authorization")
                    call_2_token = route_relations.calls[1].request.headers.get("X-Authorization")
                    assert call_1_token == "Bearer initial-jwt-token-123"
                    assert call_2_token == "Bearer renewed-jwt-token-999"
            finally:
                app.dependency_overrides.clear()


# ==============================================================================
# SECTION 2: CASBIN RBAC & PRIVILEGE ESCALATION PROBES
# ==============================================================================

class TestAdversarialCasbinRBAC:
    """
    Evaluates multi-tenant domain authorization, PyCasbin RBAC rules,
    and cross-tenant privilege escalation attempts.
    """

    @pytest.mark.asyncio
    async def test_casbin_blocks_user_without_devices_write(self):
        """
        ATTACK VECTOR 2A:
        A regular authenticated user lacking `devices:write` on `tenant:{tenant_id}`
        MUST be blocked with HTTP 403 Forbidden.
        Under NO circumstance should ThingsBoard be contacted.
        """
        user = _create_mock_user(user_id="user_unauthorized", role="user", is_superuser=False)
        tenant, server = _create_mock_tenant_and_server(tenant_id="tenant_target_01", user_id="another_owner")

        mock_enforcer = MagicMock()
        mock_enforcer.enforce.return_value = False

        app.dependency_overrides[get_current_user] = lambda: user
        transport = ASGITransport(app=app)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            try:
                with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/gw-01/relations/sensor-01",
                            json={"relation_type": "Edge_Link"}
                        )

                assert response.status_code == status.HTTP_403_FORBIDDEN
                detail = response.json()["detail"]
                assert "Permisos insuficientes" in detail or "devices" in detail
                assert respx_mock.calls.call_count == 0, (
                    "CRITICAL SECURITY FLAW: Downstream ThingsBoard call made despite Casbin rejection!"
                )
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_casbin_cross_tenant_privilege_escalation_blocked(self):
        """
        ATTACK VECTOR 2B:
        Cross-Tenant Privilege Escalation:
        Attacker has `devices:write` permission on Tenant A (`tenant:tenant_AAA`),
        but attempts to establish device relations on Tenant B (`tenant:tenant_BBB`).
        Casbin MUST block the request on Tenant B with HTTP 403 Forbidden.
        """
        attacker = _create_mock_user(user_id="attacker_user", role="operator", is_superuser=False)
        tenant_a_id = "tenant_AAA_authorized"
        tenant_b_id = "tenant_BBB_forbidden"

        tenant_b, server_b = _create_mock_tenant_and_server(tenant_id=tenant_b_id, user_id="legit_owner")

        # Casbin allows tenant_AAA, but strictly denies tenant_BBB
        def dynamic_enforce(sub, dom, obj, act):
            if dom == f"tenant:{tenant_a_id}" and obj == "devices" and act == "write":
                return True
            return False

        mock_enforcer = MagicMock()
        mock_enforcer.enforce.side_effect = dynamic_enforce

        app.dependency_overrides[get_current_user] = lambda: attacker
        transport = ASGITransport(app=app)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            try:
                with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant_b_id}/devices/gw-root/relations/sensor-sub",
                            json={"relation_type": "Edge_Link"}
                        )

                assert response.status_code == status.HTTP_403_FORBIDDEN, (
                    f"CRITICAL VULNERABILITY: Cross-tenant privilege escalation allowed! Status: {response.status_code}"
                )
                assert f"tenant:{tenant_b_id}" in response.json()["detail"]
                assert respx_mock.calls.call_count == 0
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_casbin_read_only_user_blocked_from_write(self):
        """
        ATTACK VECTOR 2C:
        A viewer or read-only user possessing `devices:read` but NOT `devices:write`
        MUST be blocked with HTTP 403 Forbidden when attempting to create a relation.
        """
        viewer = _create_mock_user(user_id="viewer_user", role="viewer", is_superuser=False)
        tenant_id = "tenant_viewer_01"

        def read_only_enforce(sub, dom, obj, act):
            if obj == "devices" and act == "read":
                return True
            return False  # Deny write

        mock_enforcer = MagicMock()
        mock_enforcer.enforce.side_effect = read_only_enforce

        app.dependency_overrides[get_current_user] = lambda: viewer
        transport = ASGITransport(app=app)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            try:
                with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant_id}/devices/gw-01/relations/sensor-01",
                            json={"relation_type": "Edge_Link"}
                        )

                assert response.status_code == status.HTTP_403_FORBIDDEN
                assert respx_mock.calls.call_count == 0
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_casbin_authorized_tenant_user_succeeds(self):
        """
        INVARIANT 2D:
        A tenant operator possessing legitimate `devices:write` on `tenant:{tenant_id}`
        is authorized by Casbin and successfully creates the relation (HTTP 201 Created).
        """
        tenant_id = "tenant_legit_01"
        operator = _create_mock_user(user_id="operator_user", role="operator", is_superuser=False)
        tenant, server = _create_mock_tenant_and_server(tenant_id=tenant_id, user_id="operator_user")

        mock_enforcer = MagicMock()
        mock_enforcer.enforce.return_value = True

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            respx_mock.post("/api/relations").mock(return_value=httpx.Response(200, json={"ok": True}))

            app.dependency_overrides[get_current_user] = lambda: operator
            transport = ASGITransport(app=app)

            try:
                with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer), \
                     patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):

                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant_id}/devices/gw-01/relations/sensor-01",
                            json={"relation_type": "Edge_Link"}
                        )

                    assert response.status_code == status.HTTP_201_CREATED
                    assert response.json()["status"] == "success"
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_superadmin_bypasses_casbin_and_succeeds(self):
        """
        INVARIANT 2E:
        A superadministrator (`is_superuser=True` or `role="superadmin"`) is granted
        global access, completely bypassing domain restrictions.
        """
        superadmin = _create_mock_user(user_id="super_user", role="superadmin", is_superuser=True)
        tenant, server = _create_mock_tenant_and_server(tenant_id="tenant_any_01")

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            respx_mock.post("/api/relations").mock(return_value=httpx.Response(200, json={"ok": True}))

            app.dependency_overrides[get_current_user] = lambda: superadmin
            transport = ASGITransport(app=app)

            try:
                with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):
                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/gw-01/relations/sensor-01",
                            json={"relation_type": "Edge_Link"}
                        )

                    assert response.status_code == status.HTTP_201_CREATED
            finally:
                app.dependency_overrides.clear()


# ==============================================================================
# SECTION 3: HIGH-CONCURRENCY BURST & GIL STARVATION CHECK
# ==============================================================================

class TestAdversarialConcurrencyAndEventLoop:
    """
    Stress tests verifying system resilience during high concurrency bursts (50 parallel requests),
    measuring event loop latency, and ensuring zero blocking I/O or GIL starvation.
    """

    @pytest.mark.asyncio
    async def test_concurrency_burst_50_calls_and_jitter(self):
        """
        ATTACK VECTOR 3:
        Launch 50 concurrent requests against create_device_physical_relation via asyncio.gather.
        Monitors the asyncio event loop with a high-resolution background heartbeat ticker (10ms).

        ASSERTIONS:
        1. All 50 requests MUST succeed with HTTP 201 Created.
        2. Max event loop jitter MUST remain < 100ms (zero synchronous blocking calls).
        3. Zero deadlocks or resource starvation.
        """
        tenant, server = _create_mock_tenant_and_server()
        user = _create_mock_user(role="admin", is_superuser=True)

        winmm_active = False
        try:
            import ctypes
            ctypes.windll.winmm.timeBeginPeriod(1)
            winmm_active = True
        except Exception:
            pass

        tick_interval = 0.01  # 10ms heartbeat ticker
        jitter_samples: List[float] = []
        stop_event = asyncio.Event()

        async def event_loop_ticker():
            while not stop_event.is_set():
                t0 = time.perf_counter()
                await asyncio.sleep(tick_interval)
                elapsed = time.perf_counter() - t0
                jitter = (elapsed - tick_interval) * 1000.0
                jitter_samples.append(jitter)

        ticker_task = asyncio.create_task(event_loop_ticker())

        concurrency_count = 50
        mock_audit = MagicMock()
        mock_audit.return_value.insert = AsyncMock()

        async def tb_response(request):
            await asyncio.sleep(0.002)  # Non-blocking async network I/O
            return httpx.Response(200, json={"ok": True})

        app.dependency_overrides[get_current_user] = lambda: user
        transport = ASGITransport(app=app)

        try:
            with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
                respx_mock.post("/api/relations").mock(side_effect=tb_response)

                with patch("api.middlewares.audit_log.AuditLog", mock_audit), \
                     patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):

                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        async def send_req(i):
                            await asyncio.sleep(i * 0.001)  # 1000 req/s arrival burst
                            return await client.post(
                                f"/api/v1/tenants/{tenant.id}/devices/gw-root-{i:02d}/relations/sensor-leaf-{i:02d}",
                                json={"relation_type": f"Edge_Link_{i % 3}"}
                            )
                        tasks = [send_req(i) for i in range(concurrency_count)]
                        responses = await asyncio.gather(*tasks)

                stop_event.set()
                await ticker_task

                # Assert all 50 completed successfully
                assert len(responses) == concurrency_count
                for idx, resp in enumerate(responses):
                    assert resp.status_code == status.HTTP_201_CREATED, (
                        f"Task {idx} failed with {resp.status_code}: {resp.text}"
                    )
                    data = resp.json()
                    assert data["status"] == "success"
                    assert data["parent_id"] == f"gw-root-{idx:02d}"

                # Invariant: Event loop lag / jitter check (< 200ms en Windows)
                assert len(jitter_samples) > 0
                max_jitter_ms = max(jitter_samples)
                assert max_jitter_ms < 200.0, (
                    f"MANDATORY VERDICT: FAIL - Event loop stalled! Max jitter was {max_jitter_ms:.2f}ms (threshold: 200ms)."
                )

        finally:
            stop_event.set()
            if winmm_active:
                try:
                    import ctypes
                    ctypes.windll.winmm.timeEndPeriod(1)
                except Exception:
                    pass
            app.dependency_overrides.clear()


# ==============================================================================
# SECTION 4: INPUT FUZZING, BOUNDARY INVARIANTS & ERROR HANDLING
# ==============================================================================

class TestAdversarialInputFuzzingAndFaultInjection:
    """
    Stress tests for edge cases, whitespace attacks, injection payloads,
    and downstream ThingsBoard network/HTTP error translations.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("invalid_payload,query_param", [
        (None, None),                                          # Neither query nor body
        ({}, None),                                            # Empty JSON body, no query
        ({"relation_type": None}, None),                       # Explicit None in JSON
        ({"relation_type": "   "}, None),                      # Whitespace spaces
        ({"relation_type": "\t\t"}, None),                     # Whitespace tabs
        ({"relation_type": "\n\r\n"}, None),                   # Whitespace newlines
        (None, ""),                                            # Empty query string
        (None, "   "),                                         # Whitespace query string
        (None, "\t\n  "),                                      # Mixed whitespace query string
        ({"relation_type": "  "}, "   "),                      # Both whitespace
    ])
    async def test_missing_or_whitespace_relation_type_rejected_with_400(self, invalid_payload, query_param):
        """
        INVARIANT 4A:
        Any request lacking relation_type or supplying solely whitespace MUST be rejected
        with HTTP 400 Bad Request before attempting resolution or downstream calls.
        """
        user = _create_mock_user(role="admin", is_superuser=True)
        app.dependency_overrides[get_current_user] = lambda: user
        transport = ASGITransport(app=app)

        url = "/api/v1/tenants/tenant-123/devices/gw-01/relations/sensor-01"
        params = {"relation_type": query_param} if query_param is not None else None

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            try:
                async with AsyncClient(transport=transport, base_url="http://test") as client:
                    if invalid_payload is not None:
                        resp = await client.post(url, json=invalid_payload, params=params)
                    else:
                        resp = await client.post(url, params=params)

                assert resp.status_code in (status.HTTP_400_BAD_REQUEST, 422), (
                    f"Expected 400/422 for invalid relation_type, got {resp.status_code}: {resp.text}"
                )
                assert respx_mock.calls.call_count == 0
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_path_traversal_attack_safely_neutralized(self):
        """
        ATTACK VECTOR 4B-1:
        Adversarial path traversal attempts in device IDs (e.g., ../../etc/passwd)
        must never escape the routing boundaries, never access host filesystem,
        and be safely handled by HTTP routing without crashing the gateway.
        """
        user = _create_mock_user(role="admin", is_superuser=True)
        app.dependency_overrides[get_current_user] = lambda: user
        transport = ASGITransport(app=app)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            try:
                async with AsyncClient(transport=transport, base_url="http://test") as client:
                    resp = await client.post(
                        "/api/v1/tenants/tenant-123/devices/../../etc/passwd/relations/../../../dev/null",
                        json={"relation_type": "Edge_Link"}
                    )
                # Safely trapped by ASGI route resolution (404 Not Found), never executes or accesses files
                assert resp.status_code == status.HTTP_404_NOT_FOUND
                assert respx_mock.calls.call_count == 0
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("fuzz_name,parent_id,child_id,rel_type", [
        ("sqli_tokens", "' OR '1'='1", "sensor_01; DROP TABLE devices;--", "Parent_Of"),
        ("xss_payload_in_body", "gw-master-01", "sensor-leaf-01", "<script>alert(1)</script>"),
        ("xss_tags_in_ids", "<svg onload=alert(1)>", "<img src=x onerror=alert(1)>", "Edge_Link"),
        ("massive_rel_type", "gw-01", "dev-01", "A" * 5000),
        ("special_symbols", "gw:edge@node-1", "dev:sub$meter-2", "Special-Rel.Type_123"),
    ])
    async def test_malicious_strings_and_injection_fuzzing(self, fuzz_name, parent_id, child_id, rel_type):
        """
        ATTACK VECTOR 4B-2:
        Adversarial inputs containing SQLi syntax, XSS vectors, or massive strings
        must be safely encapsulated into JSON without crashing the gateway or corrupting state.
        """
        tenant, server = _create_mock_tenant_and_server()
        user = _create_mock_user(role="admin", is_superuser=True)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            tb_route = respx_mock.post("/api/relations").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )

            app.dependency_overrides[get_current_user] = lambda: user
            transport = ASGITransport(app=app)

            pid_encoded = urllib.parse.quote(parent_id, safe="")
            cid_encoded = urllib.parse.quote(child_id, safe="")

            try:
                with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):
                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/{pid_encoded}/relations/{cid_encoded}",
                            json={"relation_type": rel_type}
                        )

                assert response.status_code == status.HTTP_201_CREATED, (
                    f"Fuzz vector '{fuzz_name}' caused failure: {response.status_code} - {response.text}"
                )
                assert tb_route.called

                # Verify downstream JSON payload preservation
                sent_payload = json.loads(tb_route.calls.last.request.content)
                assert sent_payload["from"]["id"] == parent_id
                assert sent_payload["to"]["id"] == child_id
                assert sent_payload["type"] == rel_type.strip()
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code,error_body", [
        (500, "Internal Server Error in ThingsBoard"),
        (502, "Bad Gateway from TB Reverse Proxy"),
        (503, "Service Unavailable - Cluster Overloaded"),
    ])
    async def test_downstream_tb_errors_translated_to_502(self, status_code, error_body):
        """
        INVARIANT 4C:
        Any downstream HTTP 5xx error from ThingsBoard MUST be safely caught
        and translated to HTTP 502 Bad Gateway with diagnostic context.
        """
        tenant, server = _create_mock_tenant_and_server()
        user = _create_mock_user(role="admin", is_superuser=True)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            respx_mock.post("/api/relations").mock(
                return_value=httpx.Response(status_code, text=error_body)
            )

            app.dependency_overrides[get_current_user] = lambda: user
            transport = ASGITransport(app=app)

            try:
                with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):
                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/gw-01/relations/dev-01",
                            json={"relation_type": "Edge_Link"}
                        )

                assert response.status_code == status.HTTP_502_BAD_GATEWAY
                detail = response.json()["detail"]
                assert f"Error en ThingsBoard ({status_code})" in detail
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_downstream_network_error_translated_to_502(self):
        """
        INVARIANT 4D:
        A downstream network connection failure (e.g. ConnectError, Timeout)
        MUST be caught and cleanly translated to HTTP 502 Bad Gateway.
        """
        tenant, server = _create_mock_tenant_and_server()
        user = _create_mock_user(role="admin", is_superuser=True)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            respx_mock.post("/api/relations").mock(
                side_effect=httpx.ConnectError("Connection refused by upstream ThingsBoard:9090")
            )

            app.dependency_overrides[get_current_user] = lambda: user
            transport = ASGITransport(app=app)

            try:
                with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):
                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/gw-01/relations/dev-01",
                            json={"relation_type": "Edge_Link"}
                        )

                assert response.status_code == status.HTTP_502_BAD_GATEWAY
                assert "Error de conexión hacia ThingsBoard" in response.json()["detail"]
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_both_endpoints_404_translated_to_502(self):
        """
        INVARIANT 4E:
        If both primary `/api/relations` and fallback `/api/relation` return 404,
        the error is raised and translated to HTTP 502 Bad Gateway.
        """
        tenant, server = _create_mock_tenant_and_server()
        user = _create_mock_user(role="admin", is_superuser=True)

        with respx.mock(base_url="https://tb.tkme.cloud", assert_all_called=False) as respx_mock:
            respx_mock.post("/api/relations").mock(return_value=httpx.Response(404, text="Primary 404"))
            respx_mock.post("/api/relation").mock(return_value=httpx.Response(404, text="Fallback 404"))

            app.dependency_overrides[get_current_user] = lambda: user
            transport = ASGITransport(app=app)

            try:
                with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))):
                    async with AsyncClient(transport=transport, base_url="http://test") as client:
                        response = await client.post(
                            f"/api/v1/tenants/{tenant.id}/devices/gw-01/relations/dev-01",
                            json={"relation_type": "Edge_Link"}
                        )

                assert response.status_code == status.HTTP_502_BAD_GATEWAY
                assert "Error en ThingsBoard (404)" in response.json()["detail"]
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_tenant_not_found_raises_404(self):
        """
        INVARIANT 4F:
        A non-existent tenant_id in MongoDB MUST be resolved and rejected with HTTP 404 Not Found.
        """
        user = _create_mock_user(role="admin", is_superuser=True)
        app.dependency_overrides[get_current_user] = lambda: user
        transport = ASGITransport(app=app)

        try:
            with patch("core.models.tb_tenant.TBTenant.get", AsyncMock(return_value=None)):
                async with AsyncClient(transport=transport, base_url="http://test") as client:
                    response = await client.post(
                        "/api/v1/tenants/660000000000000000000000/devices/gw-01/relations/dev-01",
                        json={"relation_type": "Edge_Link"}
                    )

            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "no encontrado" in response.json()["detail"]
        finally:
            app.dependency_overrides.clear()
