import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from email.utils import parseaddr
from fastapi.testclient import TestClient

from core.models.user import User
from core.models.tb_email_config import TBEmailConfig
from core.services.email_service import build_mime_message, send_email_async
from api.main import app
from api.endpoints.utils.schemas import EmailConfigTestRequest, TestEmailRequest
from workers.tasks import send_email_task

# Evitar que pytest confunda el esquema Pydantic TestEmailRequest con una clase de prueba
TestEmailRequest.__test__ = False


def _create_mock_user():
    user = MagicMock(spec=User)
    user.id = "user_test_999"
    user.username = "test_admin"
    user.email = "admin@empresa.com"
    user.is_superuser = True
    user.is_active = True
    return user


# ==============================================================================
# 1. PRUEBAS UNITARIAS DE build_mime_message Y send_email_async
# ==============================================================================

@pytest.mark.asyncio
async def test_build_mime_message_sender_name_formatting():
    """
    Verifica que build_mime_message formatee el encabezado From con el nombre
    descriptivo del remitente de acuerdo al estándar RFC 5322.
    """
    # 1. Con nombre del remitente explícito
    msg = await build_mime_message(
        to_email="destinatario@empresa.com",
        subject="Prueba Sender Name",
        body="Contenido de prueba",
        from_email="notificaciones@tkme.cloud",
        from_name="ThingsBoard Gateway",
    )
    assert msg["From"] == "ThingsBoard Gateway <notificaciones@tkme.cloud>"
    assert msg["To"] == "destinatario@empresa.com"

    # 2. Con caracteres no-ASCII (tildes / UTF-8)
    msg_utf8 = await build_mime_message(
        to_email="destinatario@empresa.com",
        subject="Prueba UTF8",
        body="Contenido",
        from_email="notificaciones@tkme.cloud",
        from_name="Jesús Alarcón - Notificaciones",
    )
    # Debe ser analizable mediante email.utils.parseaddr y email.header.decode_header
    from email.header import decode_header, make_header
    parsed_name, parsed_email = parseaddr(msg_utf8["From"])
    decoded_name = str(make_header(decode_header(parsed_name)))
    assert parsed_email == "notificaciones@tkme.cloud"
    assert "Jesús" in decoded_name

    # 3. Sin nombre del remitente (None o cadena vacía) -> degrada limpiamente al correo
    msg_no_name = await build_mime_message(
        to_email="destinatario@empresa.com",
        subject="Prueba Sin Nombre",
        body="Contenido",
        from_email="notificaciones@tkme.cloud",
        from_name=None,
    )
    assert msg_no_name["From"] == "notificaciones@tkme.cloud"

    msg_empty_name = await build_mime_message(
        to_email="destinatario@empresa.com",
        subject="Prueba Nombre Espacios",
        body="Contenido",
        from_email="notificaciones@tkme.cloud",
        from_name="   ",
    )
    assert msg_empty_name["From"] == "notificaciones@tkme.cloud"


@pytest.mark.asyncio
async def test_build_mime_message_extracts_name_from_email_if_formatted():
    """
    Verifica que si from_email ya contiene un formato 'Nombre <correo@dominio>',
    se preserve adecuadamente o se sobreescriba si se pasa from_name explícito.
    """
    # A) Sin from_name explícito, pero from_email incluye el display name
    msg_auto = await build_mime_message(
        to_email="destinatario@empresa.com",
        subject="Prueba Auto",
        body="Contenido",
        from_email="Soporte TKmE <soporte@tkme.cloud>",
        from_name=None,
    )
    parsed_name, parsed_email = parseaddr(msg_auto["From"])
    assert parsed_name == "Soporte TKmE"
    assert parsed_email == "soporte@tkme.cloud"

    # B) Con from_name explícito que tiene precedencia
    msg_override = await build_mime_message(
        to_email="destinatario@empresa.com",
        subject="Prueba Override",
        body="Contenido",
        from_email="Soporte TKmE <soporte@tkme.cloud>",
        from_name="Gateway Central",
    )
    assert msg_override["From"] == "Gateway Central <soporte@tkme.cloud>"


@pytest.mark.asyncio
async def test_send_email_async_preserves_clean_envelope_sender():
    """
    Verifica que send_email_async formatee el encabezado From del mensaje MIME
    con el nombre del remitente, pero entregue al sobre SMTP (MAIL FROM)
    estrictamente la dirección pura de correo sin corchetes angulares ni display name.
    """
    with patch("aiosmtplib.send", AsyncMock(return_value=(MagicMock(), "250 OK"))) as mock_send:
        res = await send_email_async(
            to_email="cliente@empresa.com",
            subject="Reporte con Nombre",
            body="Hola mundo",
            from_email="notificaciones@tkme.cloud",
            from_name="ThingsBoard Gateway",
            host="smtp.servidor.com",
            port=587,
            username="notif_user",
            password="secret_pass",
        )

        assert res["status"] == "SENT"
        assert res["from_email"] == "notificaciones@tkme.cloud"
        assert res["from_name"] == "ThingsBoard Gateway"
        assert res["from"] == "ThingsBoard Gateway <notificaciones@tkme.cloud>"

        # Validar la llamada a aiosmtplib.send
        mock_send.assert_called_once()
        sent_message = mock_send.call_args[0][0]
        call_kwargs = mock_send.call_args[1]

        # 1. El encabezado visible From en el MIME debe tener el nombre
        assert sent_message["From"] == "ThingsBoard Gateway <notificaciones@tkme.cloud>"

        # 2. El remitente del sobre SMTP (MAIL FROM) debe ser la dirección limpia
        assert call_kwargs["sender"] == "notificaciones@tkme.cloud"
        assert "<" not in call_kwargs["sender"]


# ==============================================================================
# 2. PRUEBAS DE LA TAREA ARQ send_email_task
# ==============================================================================

@pytest.mark.asyncio
async def test_send_email_task_resolves_sender_name_from_db_config():
    """
    Verifica que send_email_task resuelva sender_name desde el documento
    TBEmailConfig cuando no se proporciona explícitamente en el payload.
    """
    mock_config = MagicMock()
    mock_config.host = "smtp.servidor.com"
    mock_config.port = 587
    mock_config.username = "smtp_user@empresa.com"
    mock_config.sender_email = "notificaciones@empresa.com"
    mock_config.sender_name = "Nombre Desde MongoDB"
    mock_config.use_tls = True
    mock_config.get_password = AsyncMock(return_value="smtp_pass")

    ctx = {"job_id": "job_sender_name_001", "job_try": 1, "redis": AsyncMock()}

    with patch("workers.tasks.TBEmailConfig.get_singleton", AsyncMock(return_value=mock_config)), \
         patch("workers.tasks.TBEmailConfig.find_one", AsyncMock(return_value=mock_config)), \
         patch("workers.tasks.send_email_async", AsyncMock(return_value={"status": "SENT"})) as mock_send_email, \
         patch("workers.tasks.publish_task_event", AsyncMock()):

        await send_email_task(
            ctx=ctx,
            to_email="cliente@destino.com",
            subject="Asunto de Prueba",
            body="Cuerpo de prueba",
        )

        mock_send_email.assert_called_once()
        _, kwargs = mock_send_email.call_args
        assert kwargs["from_email"] == "notificaciones@empresa.com"
        assert kwargs["from_name"] == "Nombre Desde MongoDB"


@pytest.mark.asyncio
async def test_send_email_task_allows_override_sender_name_in_payload():
    """
    Verifica que si el payload contiene from_name o sender_name explícito,
    este tenga precedencia sobre la configuración de MongoDB.
    """
    mock_config = MagicMock()
    mock_config.host = "smtp.servidor.com"
    mock_config.port = 587
    mock_config.username = "smtp_user@empresa.com"
    mock_config.sender_email = "notificaciones@empresa.com"
    mock_config.sender_name = "Nombre Por Defecto BD"
    mock_config.use_tls = True
    mock_config.get_password = AsyncMock(return_value="smtp_pass")

    ctx = {"job_id": "job_sender_name_002", "job_try": 1, "redis": AsyncMock()}

    with patch("workers.tasks.TBEmailConfig.get_singleton", AsyncMock(return_value=mock_config)), \
         patch("workers.tasks.TBEmailConfig.find_one", AsyncMock(return_value=mock_config)), \
         patch("workers.tasks.send_email_async", AsyncMock(return_value={"status": "SENT"})) as mock_send_email, \
         patch("workers.tasks.publish_task_event", AsyncMock()):

        await send_email_task(
            ctx=ctx,
            to_email="cliente@destino.com",
            subject="Asunto con Override",
            payload={"from_name": "Nombre Sobrescrito En Tarea"},
        )

        mock_send_email.assert_called_once()
        _, kwargs = mock_send_email.call_args
        assert kwargs["from_name"] == "Nombre Sobrescrito En Tarea"


# ==============================================================================
# 3. PRUEBAS DE ESQUEMAS PYDANTIC (EmailConfigTestRequest y TestEmailRequest)
# ==============================================================================

def test_schemas_accept_and_sync_from_name_and_sender_name():
    """
    Verifica que tanto from_name como sender_name sean aceptados y sincronizados
    en los esquemas de solicitud.
    """
    # EmailConfigTestRequest con sender_name
    req1 = EmailConfigTestRequest(
        to_email="dest@empresa.com",
        sender_name="Mi Remitente 1"
    )
    assert req1.sender_name == "Mi Remitente 1"
    assert req1.from_name == "Mi Remitente 1"

    # EmailConfigTestRequest con from_name
    req2 = EmailConfigTestRequest(
        to_email="dest@empresa.com",
        from_name="Mi Remitente 2"
    )
    assert req2.sender_name == "Mi Remitente 2"
    assert req2.from_name == "Mi Remitente 2"

    # TestEmailRequest con sender_name
    req3 = TestEmailRequest(
        to_email="dest@empresa.com",
        sender_name="Mi Remitente 3"
    )
    assert req3.sender_name == "Mi Remitente 3"
    assert req3.from_name == "Mi Remitente 3"

    # TestEmailRequest con from_name
    req4 = TestEmailRequest(
        to_email="dest@empresa.com",
        from_name="Mi Remitente 4"
    )
    assert req4.sender_name == "Mi Remitente 4"
    assert req4.from_name == "Mi Remitente 4"


# ==============================================================================
# 4. PRUEBAS DE ENDPOINTS DE API (test-email y email-config/test)
# ==============================================================================

def test_api_test_email_sync_propagates_sender_name():
    """
    Verifica que POST /api/v1/utils/test-email en modo síncrono resuelva
    el nombre del remitente desde la BD y lo propague a send_email_async.
    """
    from api.deps import get_current_user

    client = TestClient(app)
    mock_user = _create_mock_user()
    app.dependency_overrides[get_current_user] = lambda: mock_user

    mock_config = MagicMock()
    mock_config.host = "smtp.empresa.com"
    mock_config.port = 587
    mock_config.username = "sender@empresa.com"
    mock_config.sender_email = "notif@empresa.com"
    mock_config.sender_name = "Notificaciones Gateway"
    mock_config.use_tls = True
    mock_config.get_password = AsyncMock(return_value="secret")

    with patch("api.endpoints.utils.router.TBEmailConfig.get_singleton", AsyncMock(return_value=mock_config)), \
         patch("api.endpoints.utils.router.send_email_async", AsyncMock(return_value={"status": "SENT"})) as mock_send_email:

        # A) Sin sender_name en la solicitud -> toma el de la configuración activa
        resp = client.post(
            "/api/v1/utils/test-email",
            json={
                "to_email": "destino@empresa.com",
                "sync": True
            }
        )
        assert resp.status_code == 200
        mock_send_email.assert_called_once()
        _, call_kwargs = mock_send_email.call_args
        assert call_kwargs["from_email"] == "notif@empresa.com"
        assert call_kwargs["from_name"] == "Notificaciones Gateway"

        mock_send_email.reset_mock()

        # B) Con sender_name personalizado en la solicitud -> sobreescribe
        resp2 = client.post(
            "/api/v1/utils/test-email",
            json={
                "to_email": "destino@empresa.com",
                "sender_name": "Nombre Personalizado API",
                "sync": True
            }
        )
        assert resp2.status_code == 200
        mock_send_email.assert_called_once()
        _, call_kwargs2 = mock_send_email.call_args
        assert call_kwargs2["from_name"] == "Nombre Personalizado API"

    app.dependency_overrides.clear()


def test_api_test_email_async_propagates_sender_name_to_arq():
    """
    Verifica que POST /api/v1/utils/test-email en modo asíncrono propague
    from_name / sender_name al encolar el trabajo en ARQ.
    """
    from api.deps import get_current_user

    client = TestClient(app)
    mock_user = _create_mock_user()
    app.dependency_overrides[get_current_user] = lambda: mock_user

    mock_pool = MagicMock()
    mock_job = MagicMock()
    mock_job.job_id = "job_email_async_test_001"
    mock_pool.enqueue_job = AsyncMock(return_value=mock_job)

    with patch("api.endpoints.utils.router.get_arq_pool", AsyncMock(return_value=mock_pool)):
        resp = client.post(
            "/api/v1/utils/test-email",
            json={
                "to_email": "destino@empresa.com",
                "sender_name": "Alerta Asíncrona",
                "sync": False
            }
        )
        assert resp.status_code == 202
        data = resp.json()
        assert data["task_id"] == "job_email_async_test_001"

        mock_pool.enqueue_job.assert_called_once()
        _, kwargs = mock_pool.enqueue_job.call_args
        assert kwargs["from_name"] == "Alerta Asíncrona"

    app.dependency_overrides.clear()


def test_api_email_config_test_propagates_sender_name():
    """
    Verifica que POST /api/v1/utils/email-config/test propague el nombre del remitente
    tanto en modo síncrono como asíncrono.
    """
    from api.deps import get_current_user

    client = TestClient(app)
    mock_user = _create_mock_user()
    app.dependency_overrides[get_current_user] = lambda: mock_user

    mock_config = MagicMock()
    mock_config.id = "650000000000000000000001"
    mock_config.host = "smtp.empresa.com"
    mock_config.port = 587
    mock_config.username = "sender@empresa.com"
    mock_config.sender_email = "notif@empresa.com"
    mock_config.sender_name = "Config Test Default"
    mock_config.use_tls = True
    mock_config.get_password = AsyncMock(return_value="secret")

    with patch("api.endpoints.utils.router.TBEmailConfig.get_singleton", AsyncMock(return_value=mock_config)), \
         patch("api.endpoints.utils.router.send_email_async", AsyncMock(return_value={"status": "SENT"})) as mock_send_email:

        # Modo síncrono tomando sender_name de la configuración
        resp = client.post(
            "/api/v1/utils/email-config/test",
            json={
                "to_email": "destino@empresa.com",
                "sync": True
            }
        )
        assert resp.status_code == 200
        mock_send_email.assert_called_once()
        _, call_kwargs = mock_send_email.call_args
        assert call_kwargs["from_name"] == "Config Test Default"

    app.dependency_overrides.clear()
