import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from core.models.user import User
from api.deps import require_superadmin, get_current_user
from api.main import app


def _create_mock_user(role="superadmin", is_superuser=True, is_active=True, user_id="65f01234567890abcdef0001"):
    user = MagicMock(spec=User)
    user.id = user_id
    user.username = f"user_{role}"
    user.email = f"{role}@platform.io"
    user.role = role
    user.is_superuser = is_superuser
    user.is_active = is_active
    user.must_change_password = False
    return user


# =====================================================================
# Unit Tests for require_superadmin dependency
# =====================================================================

@pytest.mark.asyncio
async def test_require_superadmin_allowed_superuser():
    """Usuario con is_superuser=True debe ser autorizado incluso si el rol no es superadmin."""
    user = _create_mock_user(role="operator", is_superuser=True)
    result = await require_superadmin(current_user=user)
    assert result == user


@pytest.mark.asyncio
async def test_require_superadmin_allowed_superadmin_role():
    """Usuario con role='superadmin' y is_superuser=False debe ser autorizado."""
    user = _create_mock_user(role="superadmin", is_superuser=False)
    result = await require_superadmin(current_user=user)
    assert result == user


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["admin", "tenant_admin", "operator", "viewer", "user", "custom_role"])
async def test_require_superadmin_blocked_non_superadmin_roles(role):
    """Cualquier rol que no sea superadmin (incluyendo admin) debe recibir HTTP 403."""
    user = _create_mock_user(role=role, is_superuser=False)
    with pytest.raises(HTTPException) as exc_info:
        await require_superadmin(current_user=user)

    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
    assert "Se requieren privilegios exclusivos de superadministrador" in exc_info.value.detail


# =====================================================================
# Integration Tests for IAM Endpoints using TestClient
# =====================================================================

@pytest.fixture
def non_superadmin_user():
    return _create_mock_user(role="admin", is_superuser=False, user_id="65f01234567890abcdef0001")


@pytest.fixture
def superadmin_user():
    return _create_mock_user(role="superadmin", is_superuser=True, user_id="65f01234567890abcdef0002")


def test_iam_endpoints_blocked_for_non_superadmin(non_superadmin_user):
    """
    Verifica que TODOS los endpoints de IAM bloqueen con HTTP 403
    a usuarios que no posean rol exclusivo de superadministrador.
    """
    app.dependency_overrides[get_current_user] = lambda: non_superadmin_user

    client = TestClient(app)

    endpoints_to_test = [
        ("GET", "/api/v1/iam/roles", None),
        ("GET", "/api/v1/iam/domains", None),
        ("POST", "/api/v1/iam/roles/assign", {"user_id": "u1", "role": "operator"}),
        ("POST", "/api/v1/iam/roles/revoke", {"user_id": "u1", "role": "operator"}),
        ("GET", "/api/v1/iam/users/u1/roles", None),
        ("GET", "/api/v1/iam/roles/operator/users", None),
        ("POST", "/api/v1/iam/policies", {"sub": "r1", "dom": "*", "obj": "devices", "act": "read"}),
        ("DELETE", "/api/v1/iam/policies", {"sub": "r1", "dom": "*", "obj": "devices", "act": "read"}),
        ("GET", "/api/v1/iam/policies", None),
        ("POST", "/api/v1/iam/enforce-check", {"domain": "*", "resource": "devices", "action": "read"}),
    ]

    try:
        for method, url, body in endpoints_to_test:
            if method == "GET":
                response = client.get(url)
            elif method == "POST":
                response = client.post(url, json=body)
            elif method == "DELETE":
                response = client.request("DELETE", url, json=body)
            else:
                pytest.fail(f"Método no soportado: {method}")

            assert response.status_code == status.HTTP_403_FORBIDDEN, (
                f"Endpoint {method} {url} devolvió {response.status_code}, se esperaba 403 Forbidden"
            )
            assert "Se requieren privilegios exclusivos de superadministrador" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


def test_iam_endpoints_allowed_for_superadmin(superadmin_user):
    """
    Verifica que un usuario superadmin pueda acceder exitosamente a los endpoints del IAM.
    """
    app.dependency_overrides[get_current_user] = lambda: superadmin_user

    client = TestClient(app)

    try:
        # Mocking get_casbin_enforcer so /roles, /domains and Casbin calls don't require MongoDB
        mock_enforcer = MagicMock()
        mock_enforcer.get_policy.return_value = [["admin", "*", "iam", "read"]]
        mock_enforcer.get_grouping_policy.return_value = []

        with patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):
            # 1. /roles debe responder 200 OK con el catálogo
            resp_roles = client.get("/api/v1/iam/roles")
            assert resp_roles.status_code == status.HTTP_200_OK
            roles_data = resp_roles.json()
            assert isinstance(roles_data, list)
            assert len(roles_data) > 0

            # 2. /domains con scope='global' debe responder 200 OK
            resp_domains = client.get("/api/v1/iam/domains?scope=global")
            assert resp_domains.status_code == status.HTTP_200_OK
            domains_data = resp_domains.json()
            assert isinstance(domains_data, list)
            assert any(d["urn"] == "*" for d in domains_data)
    finally:
        app.dependency_overrides.clear()
