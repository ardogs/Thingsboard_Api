import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from core.tb_client import ThingsBoardClient
from core.models.user import User
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from api.main import app
from api.deps import get_current_user


def _create_mock_user():
    user = MagicMock(spec=User)
    user.id = "user_test_123"
    user.username = "test_user"
    user.email = "test@tkme.cloud"
    user.is_superuser = True
    user.is_active = True
    user.role = "admin"
    return user


# ==============================================================================
# 1. PRUEBAS UNITARIAS DE ThingsBoardClient.get_entity_relations
# ==============================================================================

@pytest.mark.asyncio
async def test_get_entity_relations_outward():
    """
    Verifica que get_entity_relations consulte relaciones salientes ("Desde")
    con los parámetros fromId y fromType correctos.
    """
    client = ThingsBoardClient(base_url="https://tb.tkme.cloud", token="mock-token")

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "from": {"id": "asset-1", "entityType": "ASSET"},
            "to": {"id": "dev-1", "entityType": "DEVICE"},
            "type": "Contains",
            "toName": "Sensor Temperatura 01"
        }
    ]

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response

        relations = await client.get_entity_relations(
            from_id="asset-1",
            from_type="ASSET"
        )

        assert len(relations) == 1
        assert relations[0]["to"]["id"] == "dev-1"
        assert relations[0]["toName"] == "Sensor Temperatura 01"

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args.kwargs
        assert call_kwargs["params"] == {"fromId": "asset-1", "fromType": "ASSET"}
        assert call_kwargs["headers"]["X-Authorization"] == "Bearer mock-token"


@pytest.mark.asyncio
async def test_get_entity_relations_inward():
    """
    Verifica que get_entity_relations consulte relaciones entrantes ("Hacia")
    con los parámetros toId y toType correctos.
    """
    client = ThingsBoardClient(base_url="https://tb.tkme.cloud", token="mock-token")

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "from": {"id": "dev-2", "entityType": "DEVICE"},
            "to": {"id": "asset-2", "entityType": "ASSET"},
            "type": "InstalledIn",
            "fromName": "Sensor Presion 02"
        }
    ]

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response

        relations = await client.get_entity_relations(
            to_id="asset-2",
            to_type="ASSET"
        )

        assert len(relations) == 1
        assert relations[0]["from"]["id"] == "dev-2"
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args.kwargs
        assert call_kwargs["params"] == {"toId": "asset-2", "toType": "ASSET"}


@pytest.mark.asyncio
async def test_get_entity_relations_with_shared_client():
    """
    Verifica la reutilización de cliente httpx.AsyncClient inyectado.
    """
    client = ThingsBoardClient(base_url="https://tb.tkme.cloud", token="mock-token")

    shared_http = MagicMock(spec=httpx.AsyncClient)
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = []
    shared_http.get = AsyncMock(return_value=mock_response)

    res = await client.get_entity_relations(
        from_id="asset-3",
        from_type="ASSET",
        client=shared_http
    )

    assert res == []
    shared_http.get.assert_called_once()


@pytest.mark.asyncio
async def test_get_entity_relations_raises_on_401():
    """
    Verifica que get_entity_relations invoque raise_for_status() cuando recibe un 401,
    permitiendo la intercepción y auto-reautenticación de tokens.
    """
    client = ThingsBoardClient(base_url="https://tb.tkme.cloud", token="expired-token")

    mock_request = httpx.Request("GET", "https://tb.tkme.cloud/api/relations/info")
    mock_response = httpx.Response(status_code=401, request=mock_request)

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await client.get_entity_relations(
                from_id="asset-1",
                from_type="ASSET"
            )

        assert exc_info.value.response.status_code == 401


# ==============================================================================
# 2. PRUEBAS DE INTEGRACIÓN / ENDPOINT GET /api/v1/tenants/{tenant_id}/devices/sites
# ==============================================================================

@pytest.fixture
def mock_tenant_and_server():
    server = MagicMock(spec=TBServer)
    server.id = "server-uuid-1"
    server.name = "Production ThingsBoard"
    server.base_url = "https://tb.tkme.cloud"

    tenant = MagicMock(spec=TBTenant)
    tenant.id = "tenant-uuid-1"
    tenant.name = "Tenant Alpha"
    tenant.username = "tenant_admin@tkme.cloud"
    tenant.user_id = "user_test_123"
    tenant.get_token.return_value = "valid-token"
    tenant.get_refresh_token.return_value = "refresh-token"
    tenant.get_password.return_value = "secret123"
    tenant.get_server = AsyncMock(return_value=server)
    tenant.set_tokens = MagicMock()
    tenant.save = AsyncMock()

    return tenant, server


@pytest.mark.asyncio
async def test_list_sites_with_devices_success(mock_tenant_and_server):
    """
    Prueba el flujo completo de resolución de sitios con dispositivos relacionados
    vía ThingsBoard 'Relaciones -> Desde' (fromId=site, fromType='ASSET').
    """
    tenant, server = mock_tenant_and_server

    # Datos simulados de Assets (Sitios)
    site_asset = {
        "id": {"id": "asset-uuid-100", "entityType": "ASSET"},
        "name": "Sede Principal Monterrey",
        "type": "Building",
        "label": "Edificio A",
        "additionalInfo": {"description": "Oficinas centrales"}
    }

    # Datos simulados del catálogo de dispositivos del tenant
    tenant_devices_catalog = {
        "data": [
            {
                "id": {"id": "dev-uuid-201", "entityType": "DEVICE"},
                "name": "Medidor Energia 01",
                "type": "power_meter",
                "label": "PM-01 Tablero Principal",
                "additionalInfo": {"gateway": "GW-01"}
            },
            {
                "id": {"id": "dev-uuid-202", "entityType": "DEVICE"},
                "name": "Termómetro Nevera",
                "type": "temperature",
                "label": "Temp-02",
                "additionalInfo": {}
            }
        ]
    }

    # Relaciones "Desde" (Asset -> Device)
    site_relations = [
        {
            "from": {"id": "asset-uuid-100", "entityType": "ASSET"},
            "to": {"id": "dev-uuid-201", "entityType": "DEVICE"},
            "type": "Contains",
            "toName": "Medidor Energia 01"
        }
    ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", new_callable=AsyncMock) as mock_relations:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = tenant_devices_catalog
        mock_relations.return_value = site_relations

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        site = data[0]
        assert site["id"] == "asset-uuid-100"
        assert site["name"] == "Sede Principal Monterrey"
        assert site["type"] == "Building"
        assert site["label"] == "Edificio A"
        assert len(site["devices"]) == 1

        dev = site["devices"][0]
        assert dev["id"] == "dev-uuid-201"
        assert dev["name"] == "Medidor Energia 01"
        assert dev["type"] == "power_meter"
        assert dev["label"] == "PM-01 Tablero Principal"
        assert dev["additional_info"] == {"gateway": "GW-01"}


@pytest.mark.asyncio
async def test_list_sites_with_devices_inward_fallback(mock_tenant_and_server):
    """
    Verifica que si no hay relaciones salientes ("Desde"), se consulte el fallback
    de relaciones entrantes ("Hacia": to_id=asset, to_type='ASSET').
    """
    tenant, server = mock_tenant_and_server

    site_asset = {
        "id": {"id": "asset-uuid-200", "entityType": "ASSET"},
        "name": "Subestación Norte",
        "type": "Substation",
        "label": "Sub-Norte"
    }

    # Primera llamada (outward) retorna vacía, segunda (inward) retorna relación entrante
    async def relations_side_effect(**kwargs):
        if kwargs.get("from_id"):
            return []
        if kwargs.get("to_id"):
            return [
                {
                    "from": {"id": "dev-uuid-301", "entityType": "DEVICE"},
                    "to": {"id": "asset-uuid-200", "entityType": "ASSET"},
                    "type": "InstalledIn",
                    "fromName": "Sensor Infrarrojo 01"
                }
            ]
        return []

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=relations_side_effect):

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        site = data[0]
        assert len(site["devices"]) == 1
        assert site["devices"][0]["id"] == "dev-uuid-301"
        assert site["devices"][0]["name"] == "Sensor Infrarrojo 01"


@pytest.mark.asyncio
async def test_list_sites_with_devices_empty_sites(mock_tenant_and_server):
    """
    Verifica que si no hay sitios/activos, devuelva inmediatamente una lista vacía [].
    """
    tenant, server = mock_tenant_and_server

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_assets", new_callable=AsyncMock) as mock_assets:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": []}
        mock_assets.return_value = {"data": []}

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        assert response.json() == []


@pytest.mark.asyncio
async def test_list_sites_with_devices_deduplication(mock_tenant_and_server):
    """
    Verifica la deduplicación de dispositivos cuando un dispositivo aparece
    en múltiples relaciones o tanto en relaciones como en el array embebido 'devices'.
    """
    tenant, server = mock_tenant_and_server

    site_asset = {
        "id": {"id": "asset-uuid-300", "entityType": "ASSET"},
        "name": "Planta Solar Guadalajara",
        "type": "SolarPlant",
        # Dispositivo embebido con el mismo ID que vendrá en relaciones
        "devices": [
            {
                "id": "dev-dup-1",
                "name": "Inversor Central (Embebido)",
                "type": "inverter"
            },
            {
                "id": "dev-unique-emb",
                "name": "Sensor Ambiental",
                "type": "weather"
            }
        ]
    }

    # Relaciones repetidas para dev-dup-1
    site_relations = [
        {
            "from": {"id": "asset-uuid-300", "entityType": "ASSET"},
            "to": {"id": "dev-dup-1", "entityType": "DEVICE"},
            "type": "Contains",
            "toName": "Inversor Central 01"
        },
        {
            "from": {"id": "asset-uuid-300", "entityType": "ASSET"},
            "to": {"id": "dev-dup-1", "entityType": "DEVICE"},
            "type": "Manages",
            "toName": "Inversor Central 01"
        }
    ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", new_callable=AsyncMock) as mock_relations:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_relations.return_value = site_relations

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        devices = data[0]["devices"]
        # Debe contener exactamente 2 dispositivos: dev-dup-1 (sin duplicar) y dev-unique-emb
        assert len(devices) == 2
        dev_ids = [d["id"] for d in devices]
        assert dev_ids == ["dev-dup-1", "dev-unique-emb"]


@pytest.mark.asyncio
async def test_list_sites_with_devices_not_in_catalog_fallback(mock_tenant_and_server):
    """
    Verifica que si un dispositivo en relaciones no existe en devices_lookup,
    se construya con toName o id, type='default' y additional_info de la relación.
    """
    tenant, server = mock_tenant_and_server

    site_asset = {
        "id": "asset-uuid-400",
        "name": "Almacén Central",
        "type": "Warehouse"
    }

    site_relations = [
        {
            "from": {"id": "asset-uuid-400", "entityType": "ASSET"},
            "to": {"id": "dev-orphan-99", "entityType": "DEVICE"},
            "type": "Contains",
            "toName": "Lector RFID Salida",
            "additionalInfo": {"antennas": 4}
        }
    ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", new_callable=AsyncMock) as mock_relations:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}  # Vacío, no está en el catálogo
        mock_relations.return_value = site_relations

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        site = data[0]
        assert len(site["devices"]) == 1
        dev = site["devices"][0]
        assert dev["id"] == "dev-orphan-99"
        assert dev["name"] == "Lector RFID Salida"
        assert dev["type"] == "default"
        assert dev["additional_info"] == {"antennas": 4}


@pytest.mark.asyncio
async def test_list_sites_with_devices_relations_401_retry(mock_tenant_and_server):
    """
    Verifica que si la llamada a relaciones falla con 401, el endpoint ejecute
    auto-reautenticación y reintente la consulta con el nuevo token.
    """
    tenant, server = mock_tenant_and_server

    site_asset = {
        "id": "asset-uuid-500",
        "name": "Bomba Estación",
        "type": "PumpStation"
    }

    call_count = 0

    async def get_relations_mock(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            req = httpx.Request("GET", "https://tb.tkme.cloud/api/relations/info")
            resp = httpx.Response(status_code=401, request=req)
            raise httpx.HTTPStatusError("Unauthorized", request=req, response=resp)
        return [
            {
                "from": {"id": "asset-uuid-500", "entityType": "ASSET"},
                "to": {"id": "dev-motor-01", "entityType": "DEVICE"},
                "type": "Contains",
                "toName": "Motor Principal"
            }
        ]

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", new_callable=AsyncMock) as mock_resolve, \
         patch("api.endpoints.devices.router.ThingsBoardClient.find_entities_by_query", new_callable=AsyncMock) as mock_find, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_tenant_devices", new_callable=AsyncMock) as mock_get_devices, \
         patch("api.endpoints.devices.router.ThingsBoardClient.get_entity_relations", side_effect=get_relations_mock), \
         patch("api.endpoints.devices.router._reauthenticate_tenant", new_callable=AsyncMock) as mock_reauth:

        mock_resolve.return_value = (tenant, server)
        mock_find.return_value = {"data": [site_asset]}
        mock_get_devices.return_value = {"data": []}
        mock_reauth.return_value = "new-fresh-token"

        app.dependency_overrides[get_current_user] = _create_mock_user
        client = TestClient(app)

        response = client.get(f"/api/v1/tenants/{tenant.id}/devices/sites")
        app.dependency_overrides.clear()

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert len(data[0]["devices"]) == 1
        assert data[0]["devices"][0]["name"] == "Motor Principal"
        mock_reauth.assert_called()
        assert call_count == 2
