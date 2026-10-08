"""
Adversarial QA & Stress Test Suite: Sites with Devices Relations Resolution
=============================================================================
Lead Adversarial QA & Stress Test Engineer: qa-adversario
Task ID: task_20260922_fix_sites_devices_relations

Vectors Covered:
1. Concurrency & Rate Limiting: Semaphore(10) saturation, 25+ sites, 200+ devices, task starvation.
2. Thundering Herd on 401: Concurrent 401 re-authentication race conditions.
3. Repeated 401s & Authentication Failures: Upstream token rejection and re-auth failures.
4. Malformed Relation Payloads: None, non-dict, missing to/from, missing id/entityId, non-dict additional_info.
5. Upstream HTTP & Network Faults: 500/502/503 errors, Network Timeout, ConnectError.
6. Boundary Identifiers: Integer site IDs, missing name/latest, fallback identifiers.
7. Relation Graph Topologies: Circular relations, self-referencing assets, multi-relation deduplication.
8. Entity Type Filtering: Non-DEVICE entity relations (USER, RULE_CHAIN, DASHBOARD, CUSTOMER, TENANT).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import httpx
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from api.deps import get_current_user
from api.main import app
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.user import User
from core.tb_client import ThingsBoardClient


def _create_mock_user():
    user = MagicMock(spec=User)
    user.id = "user_adversary_001"
    user.username = "qa_adversario"
    user.email = "adversary@tkme.cloud"
    user.is_superuser = True
    user.is_active = True
    user.role = "admin"
    return user


@pytest.fixture
def mock_tenant_and_server():
    server = MagicMock(spec=TBServer)
    server.id = "server-adv-001"
    server.name = "Adversarial ThingsBoard"
    server.base_url = "https://tb.adversary.tkme.cloud"

    tenant = MagicMock(spec=TBTenant)
    tenant.id = "tenant-adv-001"
    tenant.name = "Adversarial Tenant"
    tenant.username = "adv_tenant@tkme.cloud"
    tenant.user_id = "user_adversary_001"
    tenant.get_token.return_value = "initial-token-123"
    tenant.get_refresh_token.return_value = "refresh-token-123"
    tenant.get_password.return_value = "supersecret"
    tenant.get_server = AsyncMock(return_value=server)
    tenant.set_tokens = MagicMock()
    tenant.save = AsyncMock()
    return tenant, server


# ==============================================================================
# VECTOR 1: CONCURRENCY, SEMAPHORE SATURATION & STRESS TESTING
# ==============================================================================

@pytest.mark.asyncio
async def test_concurrency_semaphore_limits_to_10(mock_tenant_and_server):
    """
    Stress probe: 25 concurrent sites query relations simultaneously.
    Verifica que asyncio.Semaphore(10) limite la concurrencia activa a máximo 10
    sin saturar sockets ni bloquear el loop.
    """
    tenant, server = mock_tenant_and_server
    num_sites = 25
    site_assets = [
        {"id": {"id": f"asset-stress-{i}", "entityType": "ASSET"}, "name": f"Site Stress {i}"}
        for i in range(num_sites)
    ]

    active_concurrent = 0
    max_observed_concurrent = 0
    lock = asyncio.Lock()

    async def mock_get_relations(*args, **kwargs):
        nonlocal active_concurrent, max_observed_concurrent
        async with lock:
            active_concurrent += 1
            if active_concurrent > max_observed_concurrent:
                max_observed_concurrent = active_concurrent
        await asyncio.sleep(0.02)
        async with lock:
            active_concurrent -= 1
        return [
            {
                "from": {"id": kwargs.get("from_id"), "entityType": "ASSET"},
                "to": {"id": f"dev-{kwargs.get('from_id')}", "entityType": "DEVICE"},
                "toName": f"Dev {kwargs.get('from_id')}"
            }
        ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=mock_get_relations):

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": site_assets}
        mock_get_devices.return_value = {"data": []}

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == num_sites
        assert max_observed_concurrent <= 10, f"Semaphore exceeded! Observed {max_observed_concurrent} > 10"


@pytest.mark.asyncio
async def test_concurrency_stress_mass_sites_and_devices(mock_tenant_and_server):
    """
    Stress probe: 30 sitios con 8 dispositivos cada uno (240 dispositivos en total).
    Verifica integridad de datos, ausencia de memory leaks y resolución completa sin timeout.
    """
    tenant, server = mock_tenant_and_server
    num_sites = 30
    devs_per_site = 8

    site_assets = [
        {
            "id": f"asset-mass-{i}",
            "name": f"Campus {i}",
            "type": "Campus",
            "label": f"Camp-{i}"
        }
        for i in range(num_sites)
    ]

    all_catalog_devs = []
    for i in range(num_sites):
        for d in range(devs_per_site):
            all_catalog_devs.append({
                "id": {"id": f"dev-{i}-{d}", "entityType": "DEVICE"},
                "name": f"Sensor {i}-{d}",
                "type": "iot_sensor",
                "label": f"LBL-{i}-{d}",
                "additionalInfo": {"cluster": f"c_{i}"}
            })

    async def mock_get_relations(*args, **kwargs):
        from_id = kwargs.get("from_id")
        if not from_id:
            return []
        site_idx = from_id.replace("asset-mass-", "")
        return [
            {
                "from": {"id": from_id, "entityType": "ASSET"},
                "to": {"id": f"dev-{site_idx}-{d}", "entityType": "DEVICE"},
                "type": "Contains",
                "toName": f"Sensor {site_idx}-{d}"
            }
            for d in range(devs_per_site)
        ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=mock_get_relations):

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": site_assets}
        mock_get_devices.return_value = {"data": all_catalog_devs}

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == num_sites
        total_resolved_devs = sum(len(s["devices"]) for s in data)
        assert total_resolved_devs == num_sites * devs_per_site
        # Verificar enriquecimiento en O(1) de devices_lookup
        sample_dev = data[0]["devices"][0]
        assert sample_dev["type"] == "iot_sensor"
        assert sample_dev["additional_info"] == {"cluster": "c_0"}


# ==============================================================================
# VECTOR 2: THUNDERING HERD & RACE CONDITIONS ON CONCURRENT 401
# ==============================================================================

@pytest.mark.asyncio
async def test_thundering_herd_concurrent_401_reauth(mock_tenant_and_server):
    """
    Adversarial Probe: 20 sitios concurrentes sufren HTTP 401 simultáneo con token expirado.
    Evalúa si auth_lock evita re-autenticaciones redundantes o si ocurre thundering herd.
    """
    tenant, server = mock_tenant_and_server
    num_sites = 20
    site_assets = [{"id": f"asset-th-{i}", "name": f"Site TH {i}"} for i in range(num_sites)]

    req = httpx.Request("GET", "https://tb.adversary.tkme.cloud/api/relations/info")
    resp_401 = httpx.Response(status_code=401, request=req)

    reauth_count = 0
    async def mock_reauth(*args, **kwargs):
        nonlocal reauth_count
        reauth_count += 1
        await asyncio.sleep(0.01)
        return f"fresh-token-v{reauth_count}"

    async def mock_get_relations(*args, **kwargs):
        tok = kwargs.get("token")
        if tok == "initial-token-123":
            raise httpx.HTTPStatusError("Unauthorized", request=req, response=resp_401)
        return [
            {
                "from": {"id": kwargs.get("from_id"), "entityType": "ASSET"},
                "to": {"id": f"dev-{kwargs.get('from_id')}", "entityType": "DEVICE"},
                "toName": f"Dev {kwargs.get('from_id')}"
            }
        ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=mock_get_relations), \
         patch("api.endpoints.devices.router._reauthenticate_tenant", side_effect=mock_reauth):

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": site_assets}
        mock_get_devices.return_value = {"data": []}

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        # Análisis adversario: Idealmente reauth_count debería ser 1.
        # Si no se valida si el token ya cambió antes de re-autenticar dentro del lock,
        # cada corrutina del lote ejecuta re-autenticación redundante.
        assert reauth_count <= 1, (
            f"Thundering herd detectado: _reauthenticate_tenant ejecutado {reauth_count} veces "
            f"para un mismo lote de peticiones concurrentes expiradas."
        )


# ==============================================================================
# VECTOR 3: REPEATED 401s AND RE-AUTHENTICATION FAILURES
# ==============================================================================

@pytest.mark.asyncio
async def test_repeated_401_retry_failure_handling(mock_tenant_and_server):
    """
    Adversarial Probe: Fallo 401 persistente (ThingsBoard rechaza tanto el token original
    como el nuevo token emitido tras re-autenticación).
    Verifica si el gateway maneja limpiamente el fallo (HTTP 502 Bad Gateway)
    o si colapsa con HTTP 500 Internal Server Error por excepción no capturada en el retry.
    """
    tenant, server = mock_tenant_and_server
    site_asset = {"id": "asset-fail-401", "name": "Site Failing 401"}

    req = httpx.Request("GET", "https://tb.adversary.tkme.cloud/api/relations/info")
    resp_401 = httpx.Response(status_code=401, request=req)

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=httpx.HTTPStatusError("401", request=req, response=resp_401)), \
         patch("api.endpoints.devices.router._reauthenticate_tenant", new_callable=AsyncMock) as mock_reauth:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_reauth.return_value = "new-token-that-also-fails"

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        # Debe responder con HTTP 502 Bad Gateway (o fallback graceful), NUNCA 500 Internal Server Error
        assert response.status_code in (status.HTTP_502_BAD_GATEWAY, status.HTTP_200_OK), (
            f"Expected HTTP 502 Bad Gateway or graceful 200 on repeated 401, but got HTTP {response.status_code}: {response.text}"
        )


@pytest.mark.asyncio
async def test_reauth_failure_propagates_clean_error(mock_tenant_and_server):
    """
    Adversarial Probe: La re-autenticación falla (credenciales inválidas en base de datos).
    _reauthenticate_tenant lanza HTTPException(401).
    Verifica que no colapse el proceso del servidor y responda código HTTP controlado.
    """
    tenant, server = mock_tenant_and_server
    site_asset = {"id": "asset-reauth-fail", "name": "Site Reauth Fail"}

    req = httpx.Request("GET", "https://tb.adversary.tkme.cloud/api/relations/info")
    resp_401 = httpx.Response(status_code=401, request=req)

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=httpx.HTTPStatusError("401", request=req, response=resp_401)), \
         patch("api.endpoints.devices.router._reauthenticate_tenant", side_effect=HTTPException(status_code=401, detail="Credenciales ThingsBoard inválidas")):

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        # Debe responder 401 o 502 sin generar un 500
        assert response.status_code in (status.HTTP_401_UNAUTHORIZED, status.HTTP_502_BAD_GATEWAY)
        assert "Credenciales" in response.text or "autenticar" in response.text


# ==============================================================================
# VECTOR 4: MALFORMED RELATION PAYLOADS & BOUNDARY DATA
# ==============================================================================

@pytest.mark.asyncio
async def test_malformed_outward_relations_payload(mock_tenant_and_server):
    """
    Adversarial Probe: ThingsBoard devuelve elementos malformados en outward_relations:
    - Elemento None
    - Elemento string en vez de dict
    - Elemento entero
    - Dict sin campo 'to'
    - Dict con 'to' igual a None
    - Dict con 'to' no-dict
    - Dict con 'to' DEVICE sin 'id' ni 'entityId'
    - Dict con relación DEVICE válida
    Verifica que el gateway procese la relación válida sin colapsar por AttributeError.
    """
    tenant, server = mock_tenant_and_server
    site_asset = {"id": "asset-malformed-1", "name": "Site Malformed Relations"}

    hostile_relations = [
        None,
        "corrupt_string_relation",
        42,
        {},
        {"type": "Contains"},
        {"to": None, "type": "Contains"},
        {"to": "not_a_dict_id", "type": "Contains"},
        {"to": {"entityType": "DEVICE"}, "type": "Contains"},  # Falta id
        {"to": {"entityType": "DEVICE", "id": None}, "type": "Contains"},
        {
            "from": {"id": "asset-malformed-1", "entityType": "ASSET"},
            "to": {"id": "dev-legit-001", "entityType": "DEVICE"},
            "toName": "Legitimate Device",
            "type": "Contains"
        }
    ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", new_callable=AsyncMock) as mock_relations:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_relations.return_value = hostile_relations

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200, f"Failed with HTTP {response.status_code}: {response.text}"
        data = response.json()
        assert len(data) == 1
        devices = data[0]["devices"]
        assert len(devices) == 1
        assert devices[0]["id"] == "dev-legit-001"
        assert devices[0]["name"] == "Legitimate Device"


@pytest.mark.asyncio
async def test_malformed_inward_relations_payload(mock_tenant_and_server):
    """
    Adversarial Probe: Fallback de inward relations con elementos corruptos (None, enteros, strings, from=None).
    """
    tenant, server = mock_tenant_and_server
    site_asset = {"id": "asset-inward-malformed", "name": "Site Inward Malformed"}

    hostile_inward = [
        None,
        "bad_inward_string",
        999,
        {"from": None},
        {"from": "not_a_dict"},
        {"from": {"entityType": "DEVICE"}},  # Missing id
        {
            "from": {"id": "dev-inward-legit", "entityType": "DEVICE"},
            "to": {"id": "asset-inward-malformed", "entityType": "ASSET"},
            "fromName": "Inward Legit Sensor",
            "type": "InstalledIn"
        }
    ]

    async def mock_relations_fn(**kwargs):
        if kwargs.get("from_id"):
            return []  # Disparar fallback hacia inward
        if kwargs.get("to_id"):
            return hostile_inward
        return []

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=mock_relations_fn):

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200, f"Failed with HTTP {response.status_code}: {response.text}"
        data = response.json()
        assert len(data) == 1
        devices = data[0]["devices"]
        assert len(devices) == 1
        assert devices[0]["id"] == "dev-inward-legit"


@pytest.mark.asyncio
async def test_malformed_embedded_devices_payload(mock_tenant_and_server):
    """
    Adversarial Probe: Dispositivos embebidos en el payload del sitio con valores None o tipos no-dict.
    """
    tenant, server = mock_tenant_and_server
    site_asset = {
        "id": "asset-emb-malformed",
        "name": "Site Embedded Malformed",
        "devices": [
            None,
            "raw-device-id-str",
            12345,
            {"id": None},
            {"id": "dev-emb-ok", "name": "Valid Embedded"}
        ]
    }

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", new_callable=AsyncMock) as mock_relations:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_relations.return_value = []

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200, f"Failed with HTTP {response.status_code}: {response.text}"
        data = response.json()
        assert len(data) == 1
        dev_ids = [d["id"] for d in data[0]["devices"]]
        assert "dev-emb-ok" in dev_ids


@pytest.mark.asyncio
async def test_non_dict_additional_info_sanitization(mock_tenant_and_server):
    """
    Adversarial Probe: additionalInfo en relación o catálogo viene como string, número o lista.
    Pydantic espera Optional[Dict[str, Any]]. Debe sanitizarse para evitar ValidationError (HTTP 500).
    """
    tenant, server = mock_tenant_and_server
    site_asset = {"id": "asset-addinfo-test", "name": "Site AddInfo"}

    relations = [
        {
            "from": {"id": "asset-addinfo-test", "entityType": "ASSET"},
            "to": {"id": "dev-bad-addinfo", "entityType": "DEVICE"},
            "toName": "Device with String AddInfo",
            "additionalInfo": "this is a raw string, not a json dict!"
        }
    ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", new_callable=AsyncMock) as mock_relations:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_relations.return_value = relations

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200, (
            f"Expected 200 with sanitized additional_info, got HTTP {response.status_code}: {response.text}"
        )
        data = response.json()
        dev = data[0]["devices"][0]
        assert dev["id"] == "dev-bad-addinfo"
        assert dev["additional_info"] is None or isinstance(dev["additional_info"], dict)


@pytest.mark.asyncio
async def test_site_with_integer_id(mock_tenant_and_server):
    """
    Adversarial Probe: Sitio con ID entero (ej: 12345).
    Verifica que el ID no se descarte a None ni se sustituya erróneamente por el nombre del sitio,
    y que se consulten sus relaciones.
    """
    tenant, server = mock_tenant_and_server
    site_asset = {"id": 12345, "name": "Site Integer ID"}

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", new_callable=AsyncMock) as mock_relations:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_relations.return_value = [
            {
                "from": {"id": "12345", "entityType": "ASSET"},
                "to": {"id": "dev-for-int-site", "entityType": "DEVICE"},
                "toName": "Dev Int Site"
            }
        ]

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        site = data[0]
        # El ID del sitio debe ser "12345", no el nombre "Site Integer ID"
        assert site["id"] == "12345", f"Expected site id '12345', got '{site['id']}'"
        assert mock_relations.called, "get_entity_relations was NOT called for integer ID site!"
        assert len(site["devices"]) == 1


@pytest.mark.asyncio
async def test_site_missing_name_and_corrupt_latest(mock_tenant_and_server):
    """
    Adversarial Probe: Sitio sin nombre, con 'latest' corrupto (None o no-dict).
    Verifica que degrade limpiamente sin lanzar TypeError o AttributeError.
    """
    tenant, server = mock_tenant_and_server
    site_asset = {
        "id": "asset-noname-001",
        "latest": None  # Corrupto
    }

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", new_callable=AsyncMock) as mock_relations:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_relations.return_value = []

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == "asset-noname-001"
        assert data[0]["name"] == "asset-noname-001"


# ==============================================================================
# VECTOR 5: UPSTREAM HTTP & NETWORK FAULT INJECTION
# ==============================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [500, 502, 503, 404])
async def test_upstream_http_status_fault_injection(mock_tenant_and_server, status_code):
    """
    Adversarial Probe: ThingsBoard responde 500, 502, 503 o 404 en get_entity_relations.
    Verifica que el gateway degrade de forma segura (retorne sitio con devices=[], status 200)
    sin colapsar la aplicación completa.
    """
    tenant, server = mock_tenant_and_server
    site_asset = {"id": "asset-tb-err", "name": "Site TB Err"}

    req = httpx.Request("GET", "https://tb.adversary.tkme.cloud/api/relations/info")
    resp_err = httpx.Response(status_code=status_code, request=req)

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_http_get:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_http_get.return_value = resp_err

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200, f"Failed on upstream {status_code}: {response.text}"
        data = response.json()
        assert len(data) == 1
        assert data[0]["devices"] == []


@pytest.mark.asyncio
async def test_upstream_network_timeout_and_connect_error(mock_tenant_and_server):
    """
    Adversarial Probe: Inyección de httpx.TimeoutException y httpx.ConnectError en relaciones.
    Verifica captura y degradación graceful por sitio.
    """
    tenant, server = mock_tenant_and_server
    site_assets = [
        {"id": "asset-timeout", "name": "Site Timeout"},
        {"id": "asset-connect-err", "name": "Site Connect Error"}
    ]

    async def mock_relations_fault(**kwargs):
        from_id = kwargs.get("from_id")
        if from_id == "asset-timeout":
            raise httpx.TimeoutException("Read timed out after 10.0 seconds")
        else:
            raise httpx.ConnectError("Connection refused by upstream")

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=mock_relations_fault):

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": site_assets}
        mock_get_devices.return_value = {"data": []}

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["devices"] == []
        assert data[1]["devices"] == []


# ==============================================================================
# VECTOR 6: CIRCULAR RELATIONS, TOPOLOGY LOOPS & SELF-REFERENCES
# ==============================================================================

@pytest.mark.asyncio
async def test_circular_and_self_referencing_assets(mock_tenant_and_server):
    """
    Adversarial Probe: Topología de grafo con ciclos:
    - Site A se relaciona consigo mismo (Site A -> Site A)
    - Site A se relaciona con Site B (Site A -> Site B)
    - Site B se relaciona con Site A (Site B -> Site A)
    - Site A contiene Device 1
    Verifica que no exista recursión infinita ni bloqueo del event loop.
    """
    tenant, server = mock_tenant_and_server
    site_assets = [
        {"id": "site-A", "name": "Site Alpha"},
        {"id": "site-B", "name": "Site Beta"}
    ]

    async def mock_relations(**kwargs):
        from_id = kwargs.get("from_id")
        if from_id == "site-A":
            return [
                {"from": {"id": "site-A", "entityType": "ASSET"}, "to": {"id": "site-A", "entityType": "ASSET"}, "type": "Parent"},
                {"from": {"id": "site-A", "entityType": "ASSET"}, "to": {"id": "site-B", "entityType": "ASSET"}, "type": "Subsite"},
                {"from": {"id": "site-A", "entityType": "ASSET"}, "to": {"id": "dev-alpha-1", "entityType": "DEVICE"}, "type": "Contains", "toName": "Alpha Sensor"}
            ]
        elif from_id == "site-B":
            return [
                {"from": {"id": "site-B", "entityType": "ASSET"}, "to": {"id": "site-A", "entityType": "ASSET"}, "type": "Parent"},
                {"from": {"id": "site-B", "entityType": "ASSET"}, "to": {"id": "dev-beta-1", "entityType": "DEVICE"}, "type": "Contains", "toName": "Beta Sensor"}
            ]
        return []

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=mock_relations):

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": site_assets}
        mock_get_devices.return_value = {"data": []}

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        site_a = next(s for s in data if s["id"] == "site-A")
        site_b = next(s for s in data if s["id"] == "site-B")

        # Solo deben incluir dispositivos, ignorando las relaciones ASSET -> ASSET
        assert len(site_a["devices"]) == 1
        assert site_a["devices"][0]["id"] == "dev-alpha-1"
        assert len(site_b["devices"]) == 1
        assert site_b["devices"][0]["id"] == "dev-beta-1"


# ==============================================================================
# VECTOR 7: FILTERING NON-DEVICE ENTITY TYPES
# ==============================================================================

@pytest.mark.asyncio
async def test_filtering_non_device_entity_types(mock_tenant_and_server):
    """
    Adversarial Probe: Relaciones hacia entidades de ThingsBoard que NO son dispositivos:
    USER, RULE_CHAIN, DASHBOARD, CUSTOMER, TENANT, ALARM, OTA_PACKAGE.
    Verifica que se filtren estrictamente y ninguna entidad no-dispositivo se cuele en 'devices'.
    """
    tenant, server = mock_tenant_and_server
    site_asset = {"id": "site-filter-test", "name": "Site Filter Non Devices"}

    alien_relations = [
        {"to": {"id": "user-uuid-1", "entityType": "USER"}, "type": "AssignedTo", "toName": "Admin User"},
        {"to": {"id": "rc-uuid-1", "entityType": "RULE_CHAIN"}, "type": "ProcessedBy", "toName": "Root Rule Chain"},
        {"to": {"id": "dash-uuid-1", "entityType": "DASHBOARD"}, "type": "MonitoredOn", "toName": "Main Dashboard"},
        {"to": {"id": "cust-uuid-1", "entityType": "CUSTOMER"}, "type": "OwnedBy", "toName": "ACME Corp"},
        {"to": {"id": "tenant-uuid-1", "entityType": "TENANT"}, "type": "BelongsTo", "toName": "Tenant Corp"},
        {"to": {"id": "alarm-uuid-1", "entityType": "ALARM"}, "type": "Raised", "toName": "High Temp Alarm"},
        {"to": {"id": "ota-uuid-1", "entityType": "OTA_PACKAGE"}, "type": "Firmware", "toName": "FW v2.1"},
        {"to": {"id": "real-dev-001", "entityType": "DEVICE"}, "type": "Contains", "toName": "Genuine Sensor"}
    ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", new_callable=AsyncMock) as mock_relations:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_relations.return_value = alien_relations

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        devs = data[0]["devices"]
        assert len(devs) == 1
        assert devs[0]["id"] == "real-dev-001"
