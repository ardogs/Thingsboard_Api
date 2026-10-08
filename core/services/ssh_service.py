import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, Any, Set
from fastapi import HTTPException, status
import asyncssh

from core.models.tb_server import TBServer, SSHAuthMethod
from core.logger import logger


ALLOWED_SSH_COMMANDS: Set[str] = {
    "systemctl restart thingsboard",
    "systemctl status thingsboard",
    "systemctl start thingsboard",
    "systemctl stop thingsboard",
    "systemctl restart thingsboard.service",
    "systemctl status thingsboard.service",
    "systemctl start thingsboard.service",
    "systemctl stop thingsboard.service",
    "journalctl -u thingsboard -n 100 --no-pager",
    "journalctl -u thingsboard.service -n 100 --no-pager",
    "docker restart thingsboard",
    "docker-compose restart thingsboard",
    "uptime",
    "df -h",
    "free -m",
}

FORBIDDEN_METASYMBOLS = {";", "&", "|", "`", "$", ">", "<", "\n", "\r", "(", ")", "\0", "{", "}"}


def validate_ssh_command(command: str) -> str:
    """
    Valida estrictamente un comando SSH contra la lista blanca permitida.
    Detecta y bloquea intentos de concatenación de comandos, metacaracteres peligrosos e inyección shell.
    Retorna el comando sanitizado y normalizado si es válido.
    """
    if not command or not command.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El comando SSH no puede estar vacío."
        )

    # 1. Detección y rechazo de metacaracteres peligrosos y encadenamiento de shell
    for char in FORBIDDEN_METASYMBOLS:
        if char in command:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Comando no permitido: contiene caracteres o secuencias de escape no autorizadas ('{char}')."
            )

    # 2. Normalización de espacios en blanco
    normalized = " ".join(command.strip().split())

    # 3. Verificación estricta de pertenencia en la lista blanca
    if normalized not in ALLOWED_SSH_COMMANDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Comando no permitido: '{normalized}'. Sólo se permiten comandos autorizados de mantenimiento del servicio ThingsBoard."
        )

    return normalized


async def execute_ssh_command_on_server(
    server: TBServer,
    command: str,
    timeout_seconds: int = 30
) -> Dict[str, Any]:
    """
    Ejecuta un comando SSH previamente validado contra la lista blanca en el servidor ThingsBoard especificado.
    Descifra las credenciales estrictamente en memoria RAM (Zero Plaintext Persistence / Logging).
    Garantiza aislamiento de fallos, manejo estricto de timeouts y telemetría de ejecución.
    """
    normalized = validate_ssh_command(command)

    host = server.ssh_host
    if not host:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Configuración SSH incompleta: host no definido en el servidor"
        )

    port = server.ssh_port or 22
    username = server.ssh_username
    if not username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Configuración SSH incompleta: usuario SSH no configurado en el servidor"
        )

    auth_method = server.ssh_auth_method
    if not auth_method:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Configuración SSH incompleta: método de autenticación SSH no configurado"
        )

    password = None
    client_keys = None

    # Descifrado seguro de credenciales exclusivamente en memoria RAM
    if auth_method == SSHAuthMethod.PASSWORD:
        password = server.get_ssh_password()
        if not password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Configuración SSH incompleta: contraseña SSH no configurada en el servidor"
            )
    elif auth_method == SSHAuthMethod.PEM_KEY:
        pem_file = server.get_ssh_pem_file()
        if not pem_file:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Configuración SSH incompleta: llave privada SSH PEM no configurada en el servidor"
            )
        passphrase = server.get_ssh_passphrase()
        try:
            client_key = asyncssh.import_private_key(pem_file, passphrase=passphrase)
            client_keys = [client_key]
        except (asyncssh.KeyImportError, asyncssh.KeyEncryptionError, ValueError, Exception) as key_err:
            logger.error(f"[SSH Service] Error al importar llave privada PEM para servidor {server.id}: {type(key_err).__name__}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Configuración SSH inválida: la llave privada PEM o passphrase son incorrectas"
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Configuración SSH incompleta: método de autenticación SSH no soportado"
        )

    start_time = time.perf_counter()
    executed_at = datetime.now(timezone.utc)
    logger.info(f"[SSH Service] Conectando por SSH a '{host}:{port}' para ejecutar comando autorizado '{normalized}' en servidor '{server.name}'")

    try:
        async with asyncssh.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            client_keys=client_keys,
            known_hosts=None
        ) as conn:
            result = await conn.run(normalized, timeout=timeout_seconds, check=False)
    except (TimeoutError, asyncio.TimeoutError, asyncssh.TimeoutError) as te:
        logger.error(f"[SSH Service] Timeout al ejecutar SSH en servidor '{server.name}': {te}")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Tiempo de espera agotado ({timeout_seconds}s) al ejecutar el comando SSH en el servidor '{server.name}'"
        )
    except asyncssh.PermissionDenied as pd:
        logger.error(f"[SSH Service] Error de autenticación SSH en servidor '{server.name}': {pd}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Error de autenticación SSH al conectar con el servidor '{server.name}'"
        )
    except (asyncssh.Error, OSError) as e:
        logger.error(f"[SSH Service] Error de conexión SSH en servidor '{server.name}': {type(e).__name__}: {e}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Error de conexión SSH con el servidor '{server.name}': {type(e).__name__}"
        )

    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
    exit_status = result.exit_status if result.exit_status is not None else (result.returncode if result.returncode is not None else 0)

    stdout_str = (
        result.stdout if isinstance(result.stdout, str)
        else (result.stdout.decode("utf-8", errors="replace") if result.stdout else "")
    )
    stderr_str = (
        result.stderr if isinstance(result.stderr, str)
        else (result.stderr.decode("utf-8", errors="replace") if result.stderr else "")
    )

    logger.info(f"[SSH Service] Comando '{normalized}' ejecutado exitosamente en '{server.name}' con exit_status={exit_status} en {duration_ms}ms")

    return {
        "server_id": str(server.id),
        "server_name": server.name,
        "host": host,
        "command": normalized,
        "exit_status": exit_status,
        "stdout": stdout_str,
        "stderr": stderr_str,
        "executed_at": executed_at,
        "duration_ms": duration_ms,
    }
