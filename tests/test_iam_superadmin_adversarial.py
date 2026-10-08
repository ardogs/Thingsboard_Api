"""
Adversarial QA & Stress Test Suite: Superadmin Restriction on IAM Endpoints
=============================================================================
Lead Adversarial QA Engineer: qa-adversario
Task ID: task_20260921_iam_superadmin_restriction

Vector Matrix Tested:
1. Direct require_superadmin matrix (Roles & Superuser combinations) -> 200 / 403
2. Unauthenticated requests across all 10 IAM endpoints -> 401 Unauthorized
3. Token tampering (invalid signature, expired, 'none' alg, revoked in Redis, inactive user) -> 401 Unauthorized
4. Standard non-superadmin roles matrix across all 10 endpoints -> 403 Forbidden
5. Header injection & spoofing (X-Role, X-Is-Superuser, X-Tenant-Id, etc.) -> 401 / 403
6. One-time password forced change (must_change_password=True) bypass resistance -> 403 Forbidden
7. Casbin In-DB Rule Subversion Resistance -> 403 Forbidden
8. High-concurrency race condition & state bleed stress test (150 concurrent requests: 50 admin, 50 superadmin, 50 unauth)
9. Superadmin positive control verification across all 10 endpoints -> 200 OK
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport
from jose import jwt

from api.deps import get_current_user, require_superadmin
from api.main import app
from core.config import settings
from core.models.user import User
from core.security import create_access_token


def _create_mock_user(
    role="superadmin",
    is_superuser=True,
    is_active=True,
    must_change_password=False,
    user_id="65f01234567890abcdef0001",
    username=None
):
    user = MagicMock(spec=User)
    user.id = user_id
    user.username = username or f"user_{role}"
    user.email = f"{user.username}@adversary.test"
    user.role = role
    user.is_superuser = is_superuser
    user.is_active = is_active
    user.must_change_password = must_change_password
    return user


IAM_ENDPOINTS_CATALOG = [
    ("GET", "/api/v1/iam/roles", None),
    ("GET", "/api/v1/iam/domains?scope=global", None),
    ("POST", "/api/v1/iam/roles/assign", {"user_id": "u1", "role": "operator", "domain": "*", "domain_type": "tenant"}),
    ("POST", "/api/v1/iam/roles/revoke", {"user_id": "u1", "role": "operator", "domain": "*", "domain_type": "tenant"}),
    ("GET", "/api/v1/iam/users/u1/roles", None),
    ("GET", "/api/v1/iam/roles/operator/users", None),
    ("POST", "/api/v1/iam/policies", {"sub": "operator", "dom": "*", "obj": "devices", "act": "read"}),
    ("DELETE", "/api/v1/iam/policies", {"sub": "operator", "dom": "*", "obj": "devices", "act": "read"}),
    ("GET", "/api/v1/iam/policies", None),
    ("POST", "/api/v1/iam/enforce-check", {"domain": "*", "resource": "devices", "action": "read"}),
]


# =====================================================================
# 1. Unit Tests: require_superadmin Direct Evaluation Matrix
# =====================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("role,is_superuser,should_pass", [
    ("superadmin", True, True),
    ("superadmin", False, True),
    ("admin", True, True),
    ("operator", True, True),
    ("viewer", True, True),
    ("user", True, True),
    (None, True, True),
    ("admin", False, False),
    ("tenant_admin", False, False),
    ("operator", False, False),
    ("viewer", False, False),
    ("user", False, False),
    ("custom_role", False, False),
    ("SUPERADMIN", False, False),       # Case sensitivity
    ("SuperAdmin", False, False),
    ("superadmin ", False, False),      # Whitespace trailing
    (" superadmin", False, False),      # Whitespace leading
    ("super_admin", False, False),      # Underscore
    ("super-admin", False, False),      # Hyphen
    ("root", False, False),             # Common escalation targets
    ("sudo", False, False),
    ("administrator", False, False),
    ("", False, False),                 # Empty string
    (None, False, False),               # None
])
async def test_require_superadmin_matrix(role, is_superuser, should_pass):
    """Prueba exhaustiva de combinaciones de rol y superuser en require_superadmin."""
    user = _create_mock_user(role=role, is_superuser=is_superuser)
    if should_pass:
        result = await require_superadmin(current_user=user)
        assert result == user
    else:
        with pytest.raises(HTTPException) as exc_info:
            await require_superadmin(current_user=user)
        assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
        assert "Se requieren privilegios exclusivos de superadministrador" in exc_info.value.detail


# =====================================================================
# 2. Adversarial Vector: Unauthenticated Requests (No Token)
# =====================================================================

@pytest.mark.parametrize("method,url,body", IAM_ENDPOINTS_CATALOG)
def test_unauthenticated_requests_blocked_401(method, url, body):
    """
    Cualquier petición sin cabecera Authorization a cualquiera de los 10 endpoints de IAM
    debe ser repelida inmediatamente con HTTP 401 Unauthorized.
    """
    client = TestClient(app)
    if method == "GET":
        resp = client.get(url)
    elif method == "POST":
        resp = client.post(url, json=body or {})
    elif method == "DELETE":
        resp = client.request("DELETE", url, json=body or {})
    else:
        pytest.fail(f"Método desconocido: {method}")

    assert resp.status_code == status.HTTP_401_UNAUTHORIZED, (
        f"Endpoint {method} {url} devolvió {resp.status_code}, se esperaba 401 Unauthorized"
    )
    assert "Credenciales de autenticación inválidas o expiradas" in resp.json()["detail"]


# =====================================================================
# 3. Adversarial Vector: Token Tampering & Cryptographic Probing
# =====================================================================

@patch("api.deps.redis_client.get", new_callable=AsyncMock)
def test_token_tampering_forged_signature(mock_redis_get):
    """Token con firma falsificada (firmado con clave ajena) debe retornar 401."""
    mock_redis_get.return_value = None
    forged_token = jwt.encode(
        {"sub": "65f01234567890abcdef0001", "user_id": "65f01234567890abcdef0001", "exp": int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())},
        "wrong_secret_key_666_attacker",
        algorithm="HS256"
    )
    client = TestClient(app)
    resp = client.get("/api/v1/iam/roles", headers={"Authorization": f"Bearer {forged_token}"})
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED


@patch("api.deps.redis_client.get", new_callable=AsyncMock)
def test_token_tampering_algorithm_none(mock_redis_get):
    """Ataque de confusión de algoritmo ('none') debe ser rechazado con 401."""
    mock_redis_get.return_value = None
    header_b64 = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0"
    payload_b64 = "eyJzdWIiOiI2NWYwMTIzNDU2Nzg5MGFiY2RlZjAwMDEiLCJ1c2VyX2lkIjoiNjVmMDEyMzQ1Njc4OTBhYmNkZWYwMDAxIn0"
    unsigned_token = f"{header_b64}.{payload_b64}."

    client = TestClient(app)
    resp = client.get("/api/v1/iam/roles", headers={"Authorization": f"Bearer {unsigned_token}"})
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED


@patch("api.deps.redis_client.get", new_callable=AsyncMock)
def test_token_expired(mock_redis_get):
    """Token expirado debe retornar 401."""
    mock_redis_get.return_value = None
    expired_token = create_access_token(
        subject="65f01234567890abcdef0001",
        expires_delta=timedelta(seconds=-10)
    )
    client = TestClient(app)
    resp = client.get("/api/v1/iam/roles", headers={"Authorization": f"Bearer {expired_token}"})
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED


@patch("api.deps.redis_client.get", new_callable=AsyncMock)
def test_token_revoked_in_redis(mock_redis_get):
    """Token legítimo pero revocado en Redis (Logout) debe retornar 401."""
    mock_redis_get.return_value = "true"  # Simula token en blacklist
    token = create_access_token(subject="65f01234567890abcdef0001")
    client = TestClient(app)
    resp = client.get("/api/v1/iam/roles", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED
    assert "El token de sesión ha sido revocado" in resp.json()["detail"]


@patch("api.deps.redis_client.get", new_callable=AsyncMock)
@patch("core.models.user.User.get", new_callable=AsyncMock)
def test_token_user_inactive_or_disabled(mock_user_get, mock_redis_get):
    """Usuario deshabilitado en base de datos (is_active=False) debe retornar 401."""
    mock_redis_get.return_value = None
    disabled_superadmin = _create_mock_user(role="superadmin", is_superuser=True, is_active=False)
    mock_user_get.return_value = disabled_superadmin

    token = create_access_token(subject="65f01234567890abcdef0001")
    client = TestClient(app)
    resp = client.get("/api/v1/iam/roles", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED


# =====================================================================
# 4. Adversarial Vector: Non-Superadmin Roles Forbidden Across All Endpoints
# =====================================================================

@pytest.mark.parametrize("role", ["admin", "tenant_admin", "operator", "viewer", "user"])
@pytest.mark.parametrize("method,url,body", IAM_ENDPOINTS_CATALOG)
def test_all_iam_endpoints_forbidden_for_standard_roles(role, method, url, body):
    """
    Verifica que cada uno de los 10 endpoints de IAM rechace con 403 Forbidden
    a usuarios con roles operativos estándar.
    """
    mock_user = _create_mock_user(role=role, is_superuser=False)
    app.dependency_overrides[get_current_user] = lambda: mock_user

    client = TestClient(app)
    try:
        if method == "GET":
            resp = client.get(url)
        elif method == "POST":
            resp = client.post(url, json=body or {})
        elif method == "DELETE":
            resp = client.request("DELETE", url, json=body or {})
        else:
            pytest.fail(f"Método desconocido: {method}")

        assert resp.status_code == status.HTTP_403_FORBIDDEN, (
            f"Fallo de seguridad: Endpoint {method} {url} permitió rol '{role}' con status {resp.status_code}"
        )
        assert "Se requieren privilegios exclusivos de superadministrador" in resp.json()["detail"]
    finally:
        app.dependency_overrides.clear()


# =====================================================================
# 5. Adversarial Vector: Header Injection & Spoofing Probing
# =====================================================================

SPOOFED_HEADERS = {
    "X-Role": "superadmin",
    "X-User-Role": "superadmin",
    "X-Is-Superuser": "true",
    "X-Superuser": "1",
    "X-Tenant-Id": "*",
    "X-Server-Id": "*",
    "X-Domain-Id": "*",
    "X-Forwarded-For": "127.0.0.1",
    "X-Real-IP": "127.0.0.1",
    "X-Original-URL": "/api/v1/auth/me"
}


def test_header_injection_unauthenticated():
    """Inyección de cabeceras de suplantación en petición no autenticada debe dar 401."""
    client = TestClient(app)
    resp = client.get("/api/v1/iam/roles", headers=SPOOFED_HEADERS)
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED


def test_header_injection_authenticated_as_admin():
    """Inyección de cabeceras de suplantación en petición de 'admin' debe dar 403."""
    admin_user = _create_mock_user(role="admin", is_superuser=False)
    app.dependency_overrides[get_current_user] = lambda: admin_user

    client = TestClient(app)
    try:
        resp = client.get("/api/v1/iam/roles", headers=SPOOFED_HEADERS)
        assert resp.status_code == status.HTTP_403_FORBIDDEN
    finally:
        app.dependency_overrides.clear()


# =====================================================================
# 6. Adversarial Vector: must_change_password One-Time Password Bypass
# =====================================================================

def test_must_change_password_blocks_iam_access():
    """
    Usuario con must_change_password=True y sin ser superuser estricto
    debe ser bloqueado con 403 Forbidden impidiéndole consultar el IAM.
    """
    admin_with_otp = _create_mock_user(role="admin", is_superuser=False, must_change_password=True)
    app.dependency_overrides[get_current_user] = lambda: admin_with_otp

    client = TestClient(app)
    try:
        resp = client.get("/api/v1/iam/roles")
        assert resp.status_code == status.HTTP_403_FORBIDDEN
    finally:
        app.dependency_overrides.clear()


def test_must_change_password_on_superadmin_role_without_is_superuser():
    """
    Usuario con role='superadmin' pero is_superuser=False y must_change_password=True
    debe ser bloqueado por la regla de cambio obligatorio de contraseña.
    """
    superadmin_not_superuser = _create_mock_user(
        role="superadmin",
        is_superuser=False,
        must_change_password=True,
        user_id="65f01234567890abcdef0003"
    )

    mock_request = MagicMock()
    mock_request.url.path = "/api/v1/iam/roles"
    mock_request.cookies = {}

    with patch("api.deps.decode_access_token", return_value={"user_id": "65f01234567890abcdef0003", "must_change_password": True}), \
         patch("core.models.user.User.get", new_callable=AsyncMock, return_value=superadmin_not_superuser), \
         patch("api.deps.redis_client.get", new_callable=AsyncMock, return_value=None):

        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(get_current_user(request=mock_request, bearer_token="valid_token"))

        assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
        assert "Debe cambiar su contraseña de un solo uso" in exc_info.value.detail


# =====================================================================
# 7. Adversarial Vector: In-DB Casbin Rule Subversion Resistance
# =====================================================================

def test_casbin_rule_subversion_does_not_grant_iam_access():
    """
    Incluso si en MongoDB existiera una regla PyCasbin 'p, admin, *, iam, write',
    el usuario con rol 'admin' y is_superuser=False NO debe poder acceder a IAM,
    porque CasbinAuth fue completamente desacoplado del router de IAM.
    """
    admin_user = _create_mock_user(role="admin", is_superuser=False)
    app.dependency_overrides[get_current_user] = lambda: admin_user

    mock_enforcer = MagicMock()
    mock_enforcer.enforce.return_value = True  # Pretende que Casbin permitiría todo
    mock_enforcer.get_policy.return_value = [["admin", "*", "iam", "write"]]

    with patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):
        client = TestClient(app)
        try:
            resp = client.get("/api/v1/iam/roles")
            assert resp.status_code == status.HTTP_403_FORBIDDEN

            resp_post = client.post(
                "/api/v1/iam/policies",
                json={"sub": "hacker", "dom": "*", "obj": "*", "act": "*"}
            )
            assert resp_post.status_code == status.HTTP_403_FORBIDDEN
        finally:
            app.dependency_overrides.clear()


# =====================================================================
# 8. Stress Test: Concurrency, Race Condition & State Bleed
# =====================================================================

@pytest.mark.asyncio
async def test_concurrent_stress_and_state_bleed():
    """
    Ráfaga concurrente de 150 peticiones asíncronas mezcladas:
    - 50 peticiones enviadas con JWT de 'admin' (deben responder 403 Forbidden)
    - 50 peticiones enviadas con JWT de 'superadmin' (deben responder 200 OK)
    - 50 peticiones unauthenticated sin token (deben responder 401 Unauthorized)
    Garantiza que no hay 'state bleed', retención en memoria ni condiciones de carrera en el event loop.
    """
    admin_id = "65f01234567890abcdef0001"
    superadmin_id = "65f01234567890abcdef0002"

    admin_token = create_access_token(subject=admin_id)
    superadmin_token = create_access_token(subject=superadmin_id)

    admin_user = _create_mock_user(role="admin", is_superuser=False, user_id=admin_id)
    superadmin_user = _create_mock_user(role="superadmin", is_superuser=True, user_id=superadmin_id)

    mock_enforcer = MagicMock()
    mock_enforcer.get_policy.return_value = [["superadmin", "*", "*", "*"]]
    mock_enforcer.get_grouping_policy.return_value = []

    async def mock_user_get(obj_id):
        s_id = str(obj_id)
        if s_id == admin_id:
            return admin_user
        elif s_id == superadmin_id:
            return superadmin_user
        return None

    transport = ASGITransport(app=app)

    async def _send_request(req_type: str):
        headers = {}
        if req_type == "admin":
            headers["Authorization"] = f"Bearer {admin_token}"
        elif req_type == "superadmin":
            headers["Authorization"] = f"Bearer {superadmin_token}"
        # Si req_type == "unauth", sin Authorization

        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            resp = await client.get("/api/v1/iam/roles", headers=headers)
            return resp.status_code

    with patch("core.models.user.User.get", new=AsyncMock(side_effect=mock_user_get)), \
         patch("api.deps.redis_client.get", new=AsyncMock(return_value=None)), \
         patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):

        tasks = []
        # 150 peticiones intercaladas en rondas de 3 (admin, superadmin, unauth)
        for _ in range(50):
            tasks.append(_send_request("admin"))
            tasks.append(_send_request("superadmin"))
            tasks.append(_send_request("unauth"))

        results = await asyncio.gather(*tasks)

        admin_results = [results[i] for i in range(len(results)) if i % 3 == 0]
        superadmin_results = [results[i] for i in range(len(results)) if i % 3 == 1]
        unauth_results = [results[i] for i in range(len(results)) if i % 3 == 2]

        assert len(admin_results) == 50
        assert len(superadmin_results) == 50
        assert len(unauth_results) == 50

        # Verificación estricta de aislamiento
        assert all(sc == status.HTTP_403_FORBIDDEN for sc in admin_results), (
            f"Fallo de fuga de privilegios: códigos recibidos para admin: {set(admin_results)}"
        )
        assert all(sc == status.HTTP_200_OK for sc in superadmin_results), (
            f"Fallo de disponibilidad para superadmin: códigos recibidos: {set(superadmin_results)}"
        )
        assert all(sc == status.HTTP_401_UNAUTHORIZED for sc in unauth_results), (
            f"Fallo en control no autenticado: códigos recibidos: {set(unauth_results)}"
        )


# =====================================================================
# 9. Positive Control: Superadmin Allowed Across All 10 Endpoints
# =====================================================================

@pytest.mark.parametrize("method,url,body", IAM_ENDPOINTS_CATALOG)
def test_superadmin_allowed_all_10_endpoints(method, url, body):
    """
    Verifica que un superadministrador legítimo pueda ejecutar exitosamente
    cada uno de los 10 endpoints de IAM.
    """
    superadmin = _create_mock_user(role="superadmin", is_superuser=True)
    app.dependency_overrides[get_current_user] = lambda: superadmin

    mock_enforcer = MagicMock()
    mock_enforcer.get_policy.return_value = [["superadmin", "*", "*", "*"]]
    mock_enforcer.get_grouping_policy.return_value = []
    mock_enforcer.has_grouping_policy.return_value = False
    mock_enforcer.add_role_for_user_in_domain = AsyncMock(return_value=True)
    mock_enforcer.remove_grouping_policy = AsyncMock(return_value=True)
    mock_enforcer.get_roles_for_user_in_domain = AsyncMock(return_value=["operator"])
    mock_enforcer.get_roles_for_user = AsyncMock(return_value=["operator"])
    mock_enforcer.get_users_for_role_in_domain = AsyncMock(return_value=["u1"])
    mock_enforcer.has_policy.return_value = False
    mock_enforcer.add_policy = AsyncMock(return_value=True)
    mock_enforcer.remove_policy = AsyncMock(return_value=True)
    mock_enforcer.enforce.return_value = True

    client = TestClient(app)
    try:
        with patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):
            if method == "GET":
                resp = client.get(url)
            elif method == "POST":
                resp = client.post(url, json=body or {})
            elif method == "DELETE":
                resp = client.request("DELETE", url, json=body or {})
            else:
                pytest.fail(f"Método desconocido: {method}")

            assert resp.status_code == status.HTTP_200_OK, (
                f"Superadmin no pudo acceder a {method} {url}: {resp.status_code} - {resp.text}"
            )
    finally:
        app.dependency_overrides.clear()
