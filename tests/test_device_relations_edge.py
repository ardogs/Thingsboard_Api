import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
from httpx import AsyncClient, ASGITransport

from core.models.user import User
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.tb_client import ThingsBoardClient
from api.endpoints.devices.router import (
    DeviceRelationRequest,
    DeviceRelationResponse,
    create_device_physical_relation,
)
from api.deps import get_current_user
from api.main import app


def _create_mock_user(role="admin", is_superuser=True):
    user = MagicMock(spec=User)
    user.id = "user_rel_test"
    user.username = "admin_rel_test"
    user.email = "admin@tkme.cloud"
    user.is_superuser = is_superuser
    user.role = role
    user.is_active = True
    return user


def _create_mock_tenant_and_server():
    tenant = MagicMock(spec=TBTenant)
    tenant.id = "66e1f00b123456789abcdef0"
    tenant.name = "Tenant Edge Topo"
    tenant.username = "tenant_topo"
    tenant.get_token.return_value = "initial-jwt-token"
    tenant.get_refresh_token.return_value = "initial-refresh-token"
    tenant.get_password.return_value = "secret-pass"
    tenant.set_tokens = MagicMock()
    tenant.save = AsyncMock()

    server = MagicMock(spec=TBServer)
    server.id = "server-edge-01"
    server.name = "ThingsBoard Central"
    server.base_url = "https://tb.tkme.cloud"

    tenant.get_server = AsyncMock(return_value=server)
    return tenant, server


# ==============================================================================
# 1. PRUEBAS DE VALIDACIÓN Y DTOs
# ==============================================================================

def test_device_relation_dtos():
    """Verifica la inicialización y contratos de Pydantic v2 en DTOs de relación."""
    req = DeviceRelationRequest(relation_type="Edge_Link")
    assert req.relation_type == "Edge_Link"

    req_empty = DeviceRelationRequest()
    assert req_empty.relation_type is None

    resp = DeviceRelationResponse(
        status="success",
        tenant_id="tenant-123",
        parent_id="gateway-01",
        child_id="sensor-01",
        relation_type="Edge_Link",
        message="Relación creada exitosamente"
    )
    assert resp.status == "success"
    assert resp.parent_id == "gateway-01"
    assert resp.child_id == "sensor-01"
    assert resp.relation_type == "Edge_Link"


@pytest.mark.asyncio
async def test_missing_relation_type_raises_400():
    """Valida que si no se proporciona relation_type ni en query ni en body se levante HTTP 400."""
    user = _create_mock_user()

    # Sin query ni body
    with pytest.raises(HTTPException) as exc_info:
        await create_device_physical_relation(
            tenant_id="tenant-1",
            parent_id="gw-1",
            child_id="dev-1",
            relation_type=None,
            request_data=None,
            current_user=user,
        )
    assert exc_info.value.status_code == 400
    assert "El parámetro 'relation_type' es obligatorio" in exc_info.value.detail

    # Con espacios en blanco únicamente
    with pytest.raises(HTTPException) as exc_info_ws:
        await create_device_physical_relation(
            tenant_id="tenant-1",
            parent_id="gw-1",
            child_id="dev-1",
            relation_type="   ",
            request_data=DeviceRelationRequest(relation_type="   "),
            current_user=user,
        )
    assert exc_info_ws.value.status_code == 400


# ==============================================================================
# 2. PRUEBAS UNITARIAS DE create_device_physical_relation
# ==============================================================================

@pytest.mark.asyncio
async def test_create_device_relation_success_via_primary_url():
    """Verifica la creación exitosa enviando la petición a /api/relations."""
    tenant, server = _create_mock_tenant_and_server()
    user = _create_mock_user()

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))), \
         patch("httpx.AsyncClient.post", AsyncMock(return_value=mock_resp)) as mock_post:

        res = await create_device_physical_relation(
            tenant_id=str(tenant.id),
            parent_id="gw-uuid-100",
            child_id="sensor-uuid-200",
            relation_type="Edge_Link",
            request_data=None,
            current_user=user,
        )

        assert res.status == "success"
        assert res.tenant_id == str(tenant.id)
        assert res.parent_id == "gw-uuid-100"
        assert res.child_id == "sensor-uuid-200"
        assert res.relation_type == "Edge_Link"
        assert "Edge_Link" in res.message

        mock_post.assert_awaited_once()
        called_url, called_kwargs = mock_post.call_args[0][0], mock_post.call_args[1]
        assert called_url == "https://tb.tkme.cloud/api/relations"
        assert called_kwargs["headers"]["X-Authorization"] == "Bearer initial-jwt-token"
        assert called_kwargs["json"] == {
            "from": {"id": "gw-uuid-100", "entityType": "DEVICE"},
            "to": {"id": "sensor-uuid-200", "entityType": "DEVICE"},
            "type": "Edge_Link",
            "typeGroup": "COMMON"
        }


@pytest.mark.asyncio
async def test_create_device_relation_fallback_404():
    """Verifica el fallback transparente a /api/relation cuando /api/relations retorna 404."""
    tenant, server = _create_mock_tenant_and_server()
    user = _create_mock_user()

    resp_404 = MagicMock(spec=httpx.Response)
    resp_404.status_code = 404

    resp_200 = MagicMock(spec=httpx.Response)
    resp_200.status_code = 200
    resp_200.raise_for_status = MagicMock()

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))), \
         patch("httpx.AsyncClient.post", AsyncMock(side_effect=[resp_404, resp_200])) as mock_post:

        res = await create_device_physical_relation(
            tenant_id=str(tenant.id),
            parent_id="gw-uuid-100",
            child_id="sensor-uuid-200",
            relation_type=None,
            request_data=DeviceRelationRequest(relation_type="Custom_Parent"),
            current_user=user,
        )

        assert res.status == "success"
        assert res.relation_type == "Custom_Parent"
        assert mock_post.await_count == 2
        # Primera llamada a /api/relations
        assert mock_post.call_args_list[0][0][0] == "https://tb.tkme.cloud/api/relations"
        # Segunda llamada fallback a /api/relation
        assert mock_post.call_args_list[1][0][0] == "https://tb.tkme.cloud/api/relation"


@pytest.mark.asyncio
async def test_create_device_relation_401_reauthentication():
    """Verifica que ante un 401 Unauthorized se invoque _reauthenticate_tenant y se reintente con el nuevo token."""
    tenant, server = _create_mock_tenant_and_server()
    user = _create_mock_user()

    resp_401 = MagicMock(spec=httpx.Response)
    resp_401.status_code = 401

    resp_200 = MagicMock(spec=httpx.Response)
    resp_200.status_code = 200
    resp_200.raise_for_status = MagicMock()

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))), \
         patch("api.endpoints.devices.router._reauthenticate_tenant", AsyncMock(return_value="renewed-jwt-token")) as mock_reauth, \
         patch("httpx.AsyncClient.post", AsyncMock(side_effect=[resp_401, resp_200])) as mock_post:

        res = await create_device_physical_relation(
            tenant_id=str(tenant.id),
            parent_id="gw-1",
            child_id="sensor-1",
            relation_type="Edge_Link",
            request_data=None,
            current_user=user,
        )

        assert res.status == "success"
        mock_reauth.assert_awaited_once()
        assert mock_post.await_count == 2
        # Segunda llamada debe usar el nuevo token renovado
        second_call_headers = mock_post.call_args_list[1][1]["headers"]
        assert second_call_headers["X-Authorization"] == "Bearer renewed-jwt-token"


@pytest.mark.asyncio
async def test_create_device_relation_tb_error_raises_502():
    """Verifica que fallos HTTP de ThingsBoard sean convertidos a HTTP 502 Bad Gateway."""
    tenant, server = _create_mock_tenant_and_server()
    user = _create_mock_user()

    req = httpx.Request("POST", "https://tb.tkme.cloud/api/relations")
    resp_500 = httpx.Response(500, request=req, text="Internal ThingsBoard Error")

    with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))), \
         patch("httpx.AsyncClient.post", AsyncMock(return_value=resp_500)):

        with pytest.raises(HTTPException) as exc_info:
            await create_device_physical_relation(
                tenant_id=str(tenant.id),
                parent_id="gw-1",
                child_id="sensor-1",
                relation_type="Edge_Link",
                request_data=None,
                current_user=user,
            )

        assert exc_info.value.status_code == 502
        assert "Error en ThingsBoard (500)" in exc_info.value.detail


# ==============================================================================
# 3. PRUEBAS DE INTEGRACIÓN HTTP VÍA FASTAPI APP
# ==============================================================================

@pytest.mark.asyncio
async def test_create_device_relation_http_endpoint():
    """Prueba de integración HTTP completa del endpoint a través de la aplicación FastAPI."""
    tenant, server = _create_mock_tenant_and_server()
    user = _create_mock_user()

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    mock_client_instance = AsyncMock(spec=httpx.AsyncClient)
    mock_client_instance.post.return_value = mock_resp
    mock_client_instance.__aenter__.return_value = mock_client_instance
    mock_client_instance.__aexit__.return_value = None

    app.dependency_overrides[get_current_user] = lambda: user

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with patch("api.endpoints.devices.router._resolve_tenant_and_server", AsyncMock(return_value=(tenant, server))), \
             patch("api.endpoints.devices.router.httpx.AsyncClient", return_value=mock_client_instance):

            # Caso A: con body JSON
            res_body = await client.post(
                f"/api/v1/tenants/{tenant.id}/devices/gw-root-01/relations/sensor-leaf-99",
                json={"relation_type": "Edge_Link"}
            )
            assert res_body.status_code == 201
            data = res_body.json()
            assert data["status"] == "success"
            assert data["parent_id"] == "gw-root-01"
            assert data["child_id"] == "sensor-leaf-99"
            assert data["relation_type"] == "Edge_Link"

            # Caso B: con query string
            res_query = await client.post(
                f"/api/v1/tenants/{tenant.id}/devices/gw-root-01/relations/sensor-leaf-99?relation_type=Parent_Of"
            )
            assert res_query.status_code == 201
            assert res_query.json()["relation_type"] == "Parent_Of"

            # Caso C: sin relation_type -> 400 Bad Request
            res_missing = await client.post(
                f"/api/v1/tenants/{tenant.id}/devices/gw-root-01/relations/sensor-leaf-99"
            )
            assert res_missing.status_code == 400

    app.dependency_overrides.clear()

