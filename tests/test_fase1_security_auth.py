import os
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException, Request, Response, status
from beanie import init_beanie, PydanticObjectId
from mongomock_motor import AsyncMongoMockClient
from jose import jwt, JWTError

from core.config import settings
from core.models.user import User
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.tb_email_config import TBEmailConfig
from core.models.tb_node import TBNode
from core.models.tb_backup import TBBackup
from core.models.audit_log import AuditLog
from core.models.tb_scheduled_task import TBScheduledTask

from core.crypto import _get_fernet_instance, encrypt_data, decrypt_data
from core.security import (
    verify_password,
    get_password_hash,
    create_access_token,
    validate_password_policy,
    decode_access_token,
)
from core.casbin_enforcer import (
    _resolve_model_path,
    init_casbin_enforcer,
    get_casbin_enforcer,
    reload_casbin_policy,
)
from core.bootstrap import bootstrap_superadmin
from api.deps import (
    get_current_user,
    get_current_active_superuser,
    require_superadmin,
    CasbinAuth,
)
from api.endpoints.auth.router import (
    _get_client_ip,
    login,
    change_password,
    logout,
    get_me,
    get_my_accessible_tenants,
    get_tb_token,
    ChangePasswordRequest,
    LoginRequest,
)


async def init_mock_db(suffix: str = "fase1"):
    client = AsyncMongoMockClient()
    db = client[f"test_fase1_db_{suffix}_{os.getpid()}"]
    await init_beanie(
        database=db,
        document_models=[
            User,
            TBServer,
            TBTenant,
            TBNode,
            TBBackup,
            AuditLog,
            TBScheduledTask,
            TBEmailConfig,
        ],
    )
    return db


# ==============================================================================
# 1. PRUEBAS DE CORE/CRYPTO.PY (100% COBERTURA)
# ==============================================================================

def test_fernet_instance_deterministic_derivation():
    valid_key = settings.ENCRYPTION_KEY
    inst1 = _get_fernet_instance(valid_key)
    assert inst1 is not None

    arbitrary_key = "mi_clave_secreta_arbitraria_de_cualquier_longitud"
    inst2 = _get_fernet_instance(arbitrary_key)
    assert inst2 is not None

    token = inst2.encrypt(b"mensaje secreto")
    assert inst2.decrypt(token) == b"mensaje secreto"


def test_encrypt_data_all_branches():
    assert encrypt_data(None) is None
    assert encrypt_data("") == ""

    token_str = encrypt_data("texto sensible")
    assert token_str is not None
    assert token_str != "texto sensible"

    token_bytes = encrypt_data(b"bytes sensibles")
    assert token_bytes is not None

    token_int = encrypt_data(12345)
    assert token_int is not None
    assert decrypt_data(token_int) == "12345"

    with patch("core.crypto._fernet.encrypt", side_effect=Exception("Fallo criptografico simulado")):
        with pytest.raises(ValueError, match="Fallo en el cifrado simétrico"):
            encrypt_data("datos")


def test_decrypt_data_all_branches():
    assert decrypt_data(None) is None
    assert decrypt_data("") == ""

    original = "contraseña_ultra_secreta_2026!"
    cipher = encrypt_data(original)
    recovered = decrypt_data(cipher)
    assert recovered == original

    recovered_from_bytes = decrypt_data(cipher.encode("utf-8"))
    assert recovered_from_bytes == original

    with pytest.raises(ValueError, match="Fallo en el descifrado simétrico"):
        decrypt_data(12345)

    with pytest.raises(ValueError, match="Fallo en el descifrado simétrico"):
        decrypt_data("token_invalido_corrupto")


# ==============================================================================
# 2. PRUEBAS DE CORE/SECURITY.PY (100% COBERTURA)
# ==============================================================================

def test_verify_password_all_branches():
    assert verify_password("", "hash") is False
    assert verify_password("pass", "") is False
    assert verify_password(None, None) is False

    hashed = get_password_hash("Secreta123!")
    assert verify_password("Secreta123!", hashed) is True
    assert verify_password("Incorrecta123!", hashed) is False
    assert verify_password("Secreta123!", "hash_no_bcrypt_corrupto") is False


def test_validate_password_policy_all_branches():
    with pytest.raises(HTTPException) as exc1:
        validate_password_policy("Abc1!")
    assert exc1.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "al menos 10 caracteres" in exc1.value.detail

    with pytest.raises(HTTPException) as exc2:
        validate_password_policy("Abcdefghij!")
    assert exc2.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "al menos un número" in exc2.value.detail

    with pytest.raises(HTTPException) as exc3:
        validate_password_policy("Abcdefghij12")
    assert exc3.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "al menos un símbolo o carácter especial" in exc3.value.detail

    validate_password_policy("CorrectPass2026!#")


def test_jwt_create_and_decode_all_branches():
    sub = "user_12345"
    token = create_access_token(
        subject=sub,
        user_data={"role": "operator", "tenant": "T1"},
        expires_delta=timedelta(hours=2)
    )
    payload = decode_access_token(token)
    assert payload["sub"] == sub
    assert payload["user_id"] == sub
    assert payload["role"] == "operator"
    assert payload["tenant"] == "T1"
    assert "jti" in payload
    assert "exp" in payload

    token_default = create_access_token(subject=sub)
    payload_def = decode_access_token(token_default)
    assert payload_def["sub"] == sub

    with pytest.raises(JWTError):
        jwt.decode(token, "clave_secreta_falsa_123", algorithms=[settings.ALGORITHM])

    expired_token = create_access_token(subject=sub, expires_delta=timedelta(seconds=-10))
    with pytest.raises(JWTError):
        decode_access_token(expired_token)


# ==============================================================================
# 3. PRUEBAS DE CORE/CASBIN_ENFORCER.PY Y BOOTSTRAP (100% COBERTURA)
# ==============================================================================

def test_resolve_model_path():
    abs_path = os.path.abspath(settings.CASBIN_MODEL_PATH)
    assert _resolve_model_path(abs_path) == abs_path

    rel_path = _resolve_model_path("core/rbac_with_domains_model.conf")
    assert os.path.exists(rel_path)

    fallback = _resolve_model_path("modelo_inexistente_123.conf")
    assert os.path.isabs(fallback)


@pytest.mark.asyncio
async def test_casbin_enforcer_lifecycle():
    await init_mock_db("casbin_lifecycle")
    with pytest.raises(FileNotFoundError):
        await init_casbin_enforcer(model_path="archivo_no_existente_999.conf")

    mock_client = AsyncMongoMockClient()
    with patch("core.database.get_mongo_client", return_value=mock_client):
        enforcer = await init_casbin_enforcer()
        assert enforcer is not None
        assert get_casbin_enforcer() == enforcer

    await reload_casbin_policy()


def test_get_casbin_enforcer_uninitialized():
    with patch("core.casbin_enforcer._enforcer", None):
        with pytest.raises(RuntimeError, match="Casbin AsyncEnforcer no ha sido inicializado"):
            get_casbin_enforcer()


@pytest.mark.asyncio
async def test_bootstrap_superadmin_all_branches():
    await init_mock_db("bootstrap")
    mock_enforcer = MagicMock()
    mock_enforcer.adapter = MagicMock()
    mock_enforcer.adapter._collection = AsyncMock()
    mock_enforcer.adapter._collection.delete_many.return_value = MagicMock(deleted_count=2)
    mock_enforcer.remove_filtered_grouping_policy = AsyncMock()
    mock_enforcer.add_policy = AsyncMock()
    mock_enforcer.add_grouping_policy = AsyncMock()

    with patch("core.bootstrap.get_casbin_enforcer", return_value=mock_enforcer):
        await bootstrap_superadmin()

    user = await User.find_one(User.username == settings.FIRST_SUPERUSER_USERNAME)
    assert user is not None
    assert user.is_superuser is True
    assert user.role == "superadmin"

    with patch("core.bootstrap.get_casbin_enforcer", return_value=mock_enforcer):
        await bootstrap_superadmin()

    count = await User.find(User.username == settings.FIRST_SUPERUSER_USERNAME).count()
    assert count == 1

    mock_enforcer.add_policy.side_effect = Exception("Fallo Casbin simulado")
    with patch("core.bootstrap.get_casbin_enforcer", return_value=mock_enforcer):
        await bootstrap_superadmin()


# ==============================================================================
# 4. PRUEBAS DE API/DEPS.PY (100% COBERTURA)
# ==============================================================================

@pytest.mark.asyncio
async def test_get_current_user_all_branches():
    await init_mock_db("deps_current_user")
    user = User(
        username="test_user",
        email="test@example.com",
        hashed_password=get_password_hash("Password123!"),
        role="user",
        is_active=True,
        must_change_password=False
    )
    await user.insert()

    token = create_access_token(subject=str(user.id))

    req1 = MagicMock(spec=Request)
    req1.cookies = {}
    with pytest.raises(HTTPException) as exc1:
        await get_current_user(request=req1, bearer_token=None)
    assert exc1.value.status_code == status.HTTP_401_UNAUTHORIZED

    mock_redis = AsyncMock()
    mock_redis.get.return_value = "revoked"
    with patch("api.deps.redis_client", mock_redis):
        req2 = MagicMock(spec=Request)
        req2.cookies = {}
        with pytest.raises(HTTPException) as exc2:
            await get_current_user(request=req2, bearer_token=token)
        assert exc2.value.status_code == status.HTTP_401_UNAUTHORIZED
        assert "revocado" in exc2.value.detail

    mock_redis.get.return_value = None
    with patch("api.deps.redis_client", mock_redis):
        req3 = MagicMock(spec=Request)
        req3.cookies = {}
        req3.url.path = "/api/v1/telemetry/data"
        res_user = await get_current_user(request=req3, bearer_token=token)
        assert res_user.username == "test_user"

    with patch("api.deps.redis_client", mock_redis):
        req4 = MagicMock(spec=Request)
        req4.cookies = {"access_token": f"Bearer {token}"}
        req4.url.path = "/api/v1/telemetry/data"
        cookie_user = await get_current_user(request=req4, bearer_token=None)
        assert cookie_user.username == "test_user"

    with patch("api.deps.redis_client", mock_redis):
        req5 = MagicMock(spec=Request)
        req5.cookies = {}
        with pytest.raises(HTTPException):
            await get_current_user(request=req5, bearer_token="token.corrupto.jwt")

    user.is_active = False
    await user.save()
    with patch("api.deps.redis_client", mock_redis):
        req6 = MagicMock(spec=Request)
        req6.cookies = {}
        with pytest.raises(HTTPException) as exc6:
            await get_current_user(request=req6, bearer_token=token)
        assert exc6.value.status_code == status.HTTP_401_UNAUTHORIZED

    user.is_active = True
    user.must_change_password = True
    await user.save()
    with patch("api.deps.redis_client", mock_redis):
        req7 = MagicMock(spec=Request)
        req7.cookies = {}
        req7.url.path = "/api/v1/telemetry/data"
        with pytest.raises(HTTPException) as exc7:
            await get_current_user(request=req7, bearer_token=token)
        assert exc7.value.status_code == status.HTTP_403_FORBIDDEN

    with patch("api.deps.redis_client", mock_redis):
        req8 = MagicMock(spec=Request)
        req8.cookies = {}
        req8.url.path = "/api/v1/auth/change-password"
        allowed_user = await get_current_user(request=req8, bearer_token=token)
        assert allowed_user.username == "test_user"


@pytest.mark.asyncio
async def test_get_current_active_superuser_and_require_superadmin():
    normal_user = User(username="norm", hashed_password="h", role="user", is_superuser=False)
    admin_user = User(username="adm", hashed_password="h", role="admin", is_superuser=False)
    super_user = User(username="sup", hashed_password="h", role="superadmin", is_superuser=True)

    assert (await get_current_active_superuser(super_user)) == super_user
    assert (await get_current_active_superuser(admin_user)) == admin_user
    with pytest.raises(HTTPException) as exc1:
        await get_current_active_superuser(normal_user)
    assert exc1.value.status_code == status.HTTP_403_FORBIDDEN

    assert (await require_superadmin(super_user)) == super_user
    with pytest.raises(HTTPException) as exc2:
        await require_superadmin(admin_user)
    assert exc2.value.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.asyncio
async def test_casbin_auth_dependency_all_branches():
    with pytest.raises(ValueError, match="CasbinAuth requiere especificar"):
        CasbinAuth(resource=None, action=None)
    with pytest.raises(ValueError, match="domain_type inválido"):
        CasbinAuth(resource="telemetry", action="read", domain_type="invalid_type")

    auth_dep = CasbinAuth(resource="telemetry", action="read", domain_type="tenant")

    super_user = User(username="sup", hashed_password="h", is_superuser=True)
    req = MagicMock(spec=Request)
    assert (await auth_dep(request=req, current_user=super_user)) == super_user

    norm_user = User(id=PydanticObjectId(), username="op", hashed_password="h", is_superuser=False)
    req.path_params = {"tenant_id": "tenant_123"}
    req.query_params = {}
    req.headers = {}

    mock_enforcer = MagicMock()
    mock_enforcer.enforce.return_value = True
    with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
        res = await auth_dep(request=req, current_user=norm_user)
        assert res == norm_user
        mock_enforcer.enforce.assert_called_with(str(norm_user.id), "tenant:tenant_123", "telemetry", "read")

    mock_enforcer.enforce.return_value = False
    with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
        with pytest.raises(HTTPException) as exc:
            await auth_dep(request=req, current_user=norm_user)
        assert exc.value.status_code == status.HTTP_403_FORBIDDEN


# ==============================================================================
# 5. PRUEBAS DE API/ENDPOINTS/AUTH/ROUTER.PY (100% COBERTURA)
# ==============================================================================

def test_get_client_ip():
    req1 = MagicMock(spec=Request)
    req1.headers = {"x-forwarded-for": "203.0.113.195, 70.41.3.18"}
    assert _get_client_ip(req1) == "203.0.113.195"

    req2 = MagicMock(spec=Request)
    req2.headers = {}
    req2.client = MagicMock(host="198.51.100.1")
    assert _get_client_ip(req2) == "198.51.100.1"

    req3 = MagicMock(spec=Request)
    req3.headers = {}
    req3.client = None
    assert _get_client_ip(req3) == "127.0.0.1"


@pytest.mark.asyncio
async def test_auth_login_endpoint_all_branches():
    await init_mock_db("auth_login")
    user = User(
        username="login_test_user",
        email="login_test@example.com",
        hashed_password=get_password_hash("ValidPass123!"),
        role="user",
        is_active=True,
        must_change_password=True
    )
    await user.insert()

    req = MagicMock(spec=Request)
    req.headers = {}
    req.client = MagicMock(host="127.0.0.1")
    resp = MagicMock(spec=Response)

    mock_form = MagicMock()
    mock_form.username = "login_test_user"
    mock_form.password = "ValidPass123!"

    mock_redis = AsyncMock()
    mock_redis.get.return_value = None
    mock_redis.delete.return_value = 1

    # 1. Login exitoso con must_change_password=True
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        token_resp = await login(request=req, response=resp, form_data=mock_form)
        assert token_resp.username == "login_test_user"
        assert token_resp.must_change_password is True
        assert "cambiar su contraseña" in token_resp.message
        resp.set_cookie.assert_called_once()

    # 2. Contraseña incorrecta
    mock_form.password = "ContraseñaErronea123!"
    mock_redis.incr.return_value = 1
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        with pytest.raises(HTTPException) as exc_401:
            await login(request=req, response=resp, form_data=mock_form)
        assert exc_401.value.status_code == status.HTTP_401_UNAUTHORIZED
        mock_redis.incr.assert_called_once()

    # 3. Rate limiting bloqueado (5 intentos previos) -> 429
    mock_redis.get.return_value = "5"
    mock_redis.ttl.return_value = 600
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        with pytest.raises(HTTPException) as exc_429:
            await login(request=req, response=resp, form_data=mock_form)
        assert exc_429.value.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        assert "Demasiados intentos fallidos" in exc_429.value.detail

    # 4. Cuenta inactiva -> 400
    mock_redis.get.return_value = None
    user.is_active = False
    await user.save()
    mock_form.password = "ValidPass123!"
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        with pytest.raises(HTTPException) as exc_400:
            await login(request=req, response=resp, form_data=mock_form)
        assert exc_400.value.status_code == status.HTTP_400_BAD_REQUEST

    # 5. Excepciones toleradas en Redis durante rate limiting
    mock_redis.get.side_effect = Exception("Fallo Redis temporal")
    mock_redis.incr.side_effect = Exception("Fallo Redis incr")
    mock_redis.delete.side_effect = Exception("Fallo Redis delete")
    user.is_active = True
    await user.save()
    mock_form.password = "ValidPass123!"
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        token_resp2 = await login(request=req, response=resp, form_data=mock_form)
        assert token_resp2.username == "login_test_user"


@pytest.mark.asyncio
async def test_auth_change_password_endpoint_all_branches():
    await init_mock_db("auth_change_pwd")
    user = User(
        username="change_pwd_user",
        hashed_password=get_password_hash("OldPassword123!"),
        role="user",
        is_active=True,
        must_change_password=True
    )
    await user.insert()

    resp = MagicMock(spec=Response)

    req_wrong_curr = ChangePasswordRequest(
        current_password="WrongOld123!",
        new_password="BrandNewPass2026!#"
    )
    with pytest.raises(HTTPException) as exc_wrong_old:
        await change_password(request=req_wrong_curr, response=resp, current_user=user)
    assert exc_wrong_old.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "incorrecta" in exc_wrong_old.value.detail

    req_same = ChangePasswordRequest(
        current_password="OldPassword123!",
        new_password="OldPassword123!"
    )
    with pytest.raises(HTTPException) as exc_same:
        await change_password(request=req_same, response=resp, current_user=user)
    assert exc_same.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "diferente" in exc_same.value.detail

    req_valid = ChangePasswordRequest(
        current_password="OldPassword123!",
        new_password="BrandNewPass2026!#"
    )
    res = await change_password(request=req_valid, response=resp, current_user=user)
    assert res.status == "ok"
    assert user.must_change_password is False


@pytest.mark.asyncio
async def test_auth_logout_and_get_me_endpoints():
    await init_mock_db("auth_logout_me")
    user = User(
        username="me_user",
        email="me@example.com",
        hashed_password=get_password_hash("Pass123!"),
        role="admin",
        is_superuser=False,
        is_active=True,
        must_change_password=False
    )
    await user.insert()

    me_resp = await get_me(current_user=user)
    assert me_resp.username == "me_user"
    assert me_resp.role == "admin"

    mock_redis = AsyncMock()
    req = MagicMock(spec=Request)
    req.cookies = {"access_token": "Bearer token_abc"}
    resp = MagicMock(spec=Response)
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        logout_resp = await logout(
            request=req,
            response=resp,
            bearer_token=None,
            current_user=user
        )
        assert logout_resp["status"] == "ok"
        mock_redis.setex.assert_called_once()
        resp.delete_cookie.assert_called_once_with(key="access_token", path="/")

    # Logout con fallo tolerado en Redis
    mock_redis.setex.side_effect = Exception("Fallo Redis setex")
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        logout_resp2 = await logout(
            request=req,
            response=resp,
            bearer_token=None,
            current_user=user
        )
        assert logout_resp2["status"] == "ok"


@pytest.mark.asyncio
async def test_auth_get_my_accessible_tenants_superadmin():
    await init_mock_db("auth_tenants_superadmin")
    server = TBServer(name="SRV1", base_url="http://srv1.com")
    await server.insert()
    tenant = TBTenant(name="T1", server_id=server, username="admin", encrypted_password="enc")
    await tenant.insert()

    super_user = User(username="sup", hashed_password="h", is_superuser=True)
    tenants = await get_my_accessible_tenants(current_user=super_user)
    assert len(tenants) == 1
    assert tenants[0].name == "T1"
    assert tenants[0].permissions["telemetry"].canRead is True
    assert tenants[0].permissions["telemetry"].canWrite is True
    assert tenants[0].permissions["telemetry"].canDelete is True


@pytest.mark.asyncio
async def test_auth_get_my_accessible_tenants_standard_user():
    await init_mock_db("auth_tenants_std_user")
    server = TBServer(name="SRV2", base_url="http://srv2.com")
    await server.insert()
    
    tenant_owned = TBTenant(name="T_OWNED", server_id=server, username="adm1", encrypted_password="enc")
    await tenant_owned.insert()

    tenant_shared = TBTenant(name="T_SHARED", server_id=server, username="adm2", encrypted_password="enc")
    await tenant_shared.insert()

    tenant_none = TBTenant(name="T_NONE", server_id=server, username="adm3", encrypted_password="enc")
    await tenant_none.insert()

    std_user = User(
        username="regular_op",
        email="op@example.com",
        hashed_password="h",
        role="operator",
        is_superuser=False,
        is_active=True
    )
    await std_user.insert()

    # std_user es propietario de tenant_owned
    tenant_owned.user_id = str(std_user.id)
    await tenant_owned.save()

    mock_enforcer = MagicMock()
    # Para T_SHARED el usuario tiene permiso de lectura
    def mock_enforce(sub, dom, obj, act):
        if dom == f"tenant:{tenant_shared.id}" and act == "read":
            return True
        return False
    mock_enforcer.enforce.side_effect = mock_enforce
    mock_enforcer.get_policy.return_value = [
        [str(std_user.id), f"tenant:{tenant_shared.id}", "telemetry", "read"]
    ]
    mock_enforcer.get_roles_for_user_in_domain = AsyncMock(return_value=["viewer"])

    with patch("core.casbin_enforcer.get_casbin_enforcer", return_value=mock_enforcer):
        accessible = await get_my_accessible_tenants(current_user=std_user)
        # Debe acceder a T_OWNED (propietario) y a T_SHARED (permiso lectura), pero NO a T_NONE
        tenant_names = [t.name for t in accessible]
        assert "T_OWNED" in tenant_names
        assert "T_SHARED" in tenant_names
        assert "T_NONE" not in tenant_names


@pytest.mark.asyncio
async def test_auth_get_tb_token_cached_and_live():
    mock_user = User(id=PydanticObjectId(), username="tb_user", hashed_password="h")
    mock_redis = AsyncMock()

    # 1. Hit de caché
    mock_redis.get.side_effect = ["token_cached_123", "refresh_cached_456"]
    login_req = LoginRequest(server_url="http://tb.example.com", username="tenant_adm", password="pass")
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        resp_cache = await get_tb_token(request=login_req, current_user=mock_user)
        assert resp_cache["source"] == "redis_cache"
        assert resp_cache["token"] == "token_cached_123"

    # 2. Miss de caché -> login en vivo exitoso
    mock_redis.get.side_effect = [None, None]
    mock_tb_client = MagicMock()
    mock_tb_client.login = AsyncMock(return_value={"token": "live_tok", "refreshToken": "live_ref"})
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        with patch("api.endpoints.auth.router.ThingsBoardClient", return_value=mock_tb_client):
            resp_live = await get_tb_token(request=login_req, current_user=mock_user)
            assert resp_live["source"] == "thingsboard"
            assert resp_live["token"] == "live_tok"
            mock_redis.setex.assert_called()

    # 3. Miss de caché -> fallo de credenciales ThingsBoard (401)
    mock_redis.get.side_effect = [None, None]
    mock_tb_client.login = AsyncMock(return_value=None)
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        with patch("api.endpoints.auth.router.ThingsBoardClient", return_value=mock_tb_client):
            with pytest.raises(HTTPException) as exc_tb:
                await get_tb_token(request=login_req, current_user=mock_user)
            assert exc_tb.value.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_deps_edge_branches():
    await init_mock_db("deps_edge")
    user = User(
        username="edge_user",
        email="edge@example.com",
        hashed_password="h",
        role="user",
        is_active=True
    )
    await user.insert()

    # 1. Redis falla en get_current_user -> debe continuar con validación JWT
    token = create_access_token(subject=str(user.id))
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = Exception("Fallo Redis temporal")
    req = MagicMock(spec=Request)
    req.cookies = {}
    req.url.path = "/api/v1/test"
    with patch("api.deps.redis_client", mock_redis):
        res_user = await get_current_user(request=req, bearer_token=token)
        assert res_user.username == "edge_user"

    # 2. Token sin user_id ni sub -> 401
    bad_token = jwt.encode({"algo": "nada"}, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    with patch("api.deps.redis_client", AsyncMock(get=AsyncMock(return_value=None))):
        with pytest.raises(HTTPException) as exc_no_sub:
            await get_current_user(request=req, bearer_token=bad_token)
        assert exc_no_sub.value.status_code == status.HTTP_401_UNAUTHORIZED

    # 3. Token con username en vez de PydanticObjectId -> resuelve por find_one(username)
    user_name_token = create_access_token(subject="edge_user")
    with patch("api.deps.redis_client", AsyncMock(get=AsyncMock(return_value=None))):
        res_by_name = await get_current_user(request=req, bearer_token=user_name_token)
        assert res_by_name.username == "edge_user"

    # 4. CasbinAuth: extracción desde query_params para server y tenant
    auth_srv = CasbinAuth(resource="servers", action="write", domain_type="server")
    req_query_srv = MagicMock(spec=Request)
    req_query_srv.path_params = {}
    req_query_srv.query_params = {"server_id": "srv_999"}
    req_query_srv.headers = {}
    mock_enforcer = MagicMock()
    mock_enforcer.enforce.return_value = True
    with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
        await auth_srv(request=req_query_srv, current_user=user)
        mock_enforcer.enforce.assert_called_with(str(user.id), "server:srv_999", "servers", "write")

    auth_tnt = CasbinAuth(resource="devices", action="read", domain_type="tenant")
    req_query_tnt = MagicMock(spec=Request)
    req_query_tnt.path_params = {}
    req_query_tnt.query_params = {"tenant_id": "tnt_888"}
    req_query_tnt.headers = {}
    with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
        await auth_tnt(request=req_query_tnt, current_user=user)
        mock_enforcer.enforce.assert_called_with(str(user.id), "tenant:tnt_888", "devices", "read")

    # 5. CasbinAuth: extracción desde headers (X-Server-Id, X-Tenant-Id)
    req_hdr_srv = MagicMock(spec=Request)
    req_hdr_srv.path_params = {}
    req_hdr_srv.query_params = {}
    req_hdr_srv.headers = {"X-Server-Id": "srv_header_1"}
    with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
        await auth_srv(request=req_hdr_srv, current_user=user)
        mock_enforcer.enforce.assert_called_with(str(user.id), "server:srv_header_1", "servers", "write")

    req_hdr_tnt = MagicMock(spec=Request)
    req_hdr_tnt.path_params = {}
    req_hdr_tnt.query_params = {}
    req_hdr_tnt.headers = {"X-Tenant-Id": "tnt_header_2"}
    with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
        await auth_tnt(request=req_hdr_tnt, current_user=user)
        mock_enforcer.enforce.assert_called_with(str(user.id), "tenant:tnt_header_2", "devices", "read")

    # 6. CasbinAuth: fallback a default_domain = "*" y prefijos ya existentes
    auth_wildcard = CasbinAuth(resource="all", action="read", domain_type="tenant", default_domain="*")
    req_empty = MagicMock(spec=Request)
    req_empty.path_params = {}
    req_empty.query_params = {}
    req_empty.headers = {}
    with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
        await auth_wildcard(request=req_empty, current_user=user)
        mock_enforcer.enforce.assert_called_with(str(user.id), "*", "all", "read")

    # 7. CasbinAuth: Enforcer lanza excepción interna -> 403
    mock_enforcer.enforce.side_effect = Exception("Casbin enforcer failure")
    with patch("api.deps.get_casbin_enforcer", return_value=mock_enforcer):
        with pytest.raises(HTTPException) as exc_casbin_err:
            await auth_srv(request=req_hdr_srv, current_user=user)
        assert exc_casbin_err.value.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.asyncio
async def test_auth_login_with_email_and_redis_incr_failure():
    await init_mock_db("auth_login_email")
    user = User(
        username="nick_user",
        email="email_login@example.com",
        hashed_password=get_password_hash("Pass12345!"),
        role="user",
        is_active=True
    )
    await user.insert()

    req = MagicMock(spec=Request)
    req.headers = {}
    req.client = MagicMock(host="10.0.0.1")
    resp = MagicMock(spec=Response)

    mock_redis = AsyncMock()
    mock_redis.get.return_value = None
    mock_redis.delete.return_value = 1

    # Login por email
    form = MagicMock()
    form.username = "email_login@example.com"
    form.password = "Pass12345!"
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        token_resp = await login(request=req, response=resp, form_data=form)
        assert token_resp.username == "nick_user"

    # Fallo de contraseña con Redis incr arrojando excepción (debe capturarse defensivamente y lanzar 401)
    form.password = "Incorrecta!"
    mock_redis.incr.side_effect = Exception("Redis connection lost")
    with patch("api.endpoints.auth.router.redis_client", mock_redis):
        with pytest.raises(HTTPException) as exc_401:
            await login(request=req, response=resp, form_data=form)
        assert exc_401.value.status_code == status.HTTP_401_UNAUTHORIZED

