import io
import json
import logging
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient

from core.logger import get_logger, setup_logger
from api.endpoints.utils.schemas import EmailConfigTestRequest, TestEmailRequest as EmailSendRequest
from api.endpoints.tasks.schemas import ActiveTaskResponse, TaskStatusResponse
from api.main import app


def test_logger_modularity():
    """Verifica que cada módulo obtenga su logger nombrado y no se use 'telemetry_downloader' por defecto."""
    email_log = get_logger("email_service")
    worker_log = get_logger("arq_worker")
    default_log = get_logger()

    assert email_log.name == "email_service"
    assert worker_log.name == "arq_worker"
    assert default_log.name == "tb_gateway"
    assert "telemetry_downloader" not in (email_log.name, worker_log.name, default_log.name)


def test_logger_output_formatting():
    """Verifica que el formateador incluya el nombre del logger específico."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    handler.setFormatter(formatter)

    test_logger = logging.getLogger("custom_email_test")
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.INFO)
    test_logger.propagate = False

    test_logger.info("[EmailService] Mensaje de prueba modular")
    output = stream.getvalue()

    assert "custom_email_test" in output
    assert "telemetry_downloader" not in output
    assert "[EmailService] Mensaje de prueba modular" in output


def test_email_schema_sanitization():
    """Verifica que los placeholders de Swagger como 'string' sean sanitizados automáticamente."""
    # Caso 1: Array con placeholder 'string'
    req1 = EmailSendRequest(
        to_email="test@empresa.com",
        attachment_paths=["string"]
    )
    assert req1.attachment_paths is None

    # Caso 2: Array mixto con strings vacíos y placeholders
    req2 = EmailConfigTestRequest(
        to_email="test@empresa.com",
        attachment_paths=["", "string", "  ", "null", "none", "report.xlsx"]
    )
    assert req2.attachment_paths == ["report.xlsx"]

    # Caso 3: Archivo válido único
    req3 = EmailSendRequest(
        to_email="test@empresa.com",
        attachment_paths=["/path/to/archive.zip"]
    )
    assert req3.attachment_paths == ["/path/to/archive.zip"]


def test_tasks_domain_routes_present_and_telemetry_cleaned():
    """
    Verifica que las rutas de gestión de tareas estén presentes en /api/v1/tasks
    y hayan sido completamente erradicadas de /api/v1/telemetry.
    """
    openapi_paths = app.openapi()["paths"]

    # Rutas que DEBEN existir en /api/v1/tasks
    assert "/api/v1/tasks/active" in openapi_paths
    assert "/api/v1/tasks/{task_id}" in openapi_paths
    assert "/api/v1/tasks/{task_id}/stream" in openapi_paths
    assert "/api/v1/tasks/{job_id}/cancel" in openapi_paths

    # Rutas que DEBEN HABER SIDO ELIMINADAS de /api/v1/telemetry
    assert "/api/v1/telemetry/tasks/active" not in openapi_paths
    assert "/api/v1/telemetry/stream/{task_id}" not in openapi_paths
    assert "/api/v1/telemetry/status/{task_id}" not in openapi_paths
    assert "/api/v1/telemetry/tasks/{job_id}/cancel" not in openapi_paths

    # Telemetría solo mantiene endpoints de negocio de telemetría y respaldos
    assert "/api/v1/telemetry/download" in openapi_paths
    assert "/api/v1/telemetry/report/excel" in openapi_paths
    assert "/api/v1/telemetry/report/heatmap" in openapi_paths
    assert "/api/v1/telemetry/backups" in openapi_paths
    assert "/api/v1/telemetry/download/file/{task_id}" in openapi_paths


@pytest.mark.asyncio
async def test_send_email_task_publishes_lifecycle_events():
    """
    Verifica que send_email_task reporte sus eventos a Redis Pub/Sub y registro de tareas.
    """
    from workers.tasks import send_email_task

    mock_redis = AsyncMock()
    ctx = {"job_id": "test_job_123", "job_try": 1, "redis": mock_redis}

    # Mock de TBEmailConfig
    mock_config = MagicMock()
    mock_config.host = "smtp.empresa.com"
    mock_config.port = 587
    mock_config.username = "sender@empresa.com"
    mock_config.sender_email = "sender@empresa.com"
    mock_config.use_tls = True
    mock_config.get_password = AsyncMock(return_value="plain_pwd")

    with patch("workers.tasks.TBEmailConfig.get_singleton", AsyncMock(return_value=mock_config)), \
         patch("workers.tasks.TBEmailConfig.find_one", AsyncMock(return_value=mock_config)), \
         patch("workers.tasks.send_email_async", AsyncMock(return_value={"success": True, "details": "ok"})), \
         patch("workers.tasks.publish_task_event", AsyncMock()) as mock_pub:

        result = await send_email_task(
            ctx=ctx,
            to_email="receiver@empresa.com",
            subject="Test Subject",
            html_body="<p>Test</p>",
            user_id="user_test_456"
        )

        assert result == {"success": True, "details": "ok"}
        # Debe haber llamado a publish_task_event al menos dos veces: PROCESSING y SUCCESS
        assert mock_pub.call_count >= 2

        # Primer llamado: PROCESSING
        first_call = mock_pub.call_args_list[0]
        assert first_call.kwargs["status"] == "PROCESSING"
        assert first_call.kwargs["user_id"] == "user_test_456"
        assert first_call.kwargs["task_id"] == "test_job_123"

        # Último llamado: SUCCESS con terminal=True
        last_call = mock_pub.call_args_list[-1]
        assert last_call.kwargs["status"] == "SUCCESS"
        assert last_call.kwargs["cleanup_on_terminal"] is True


def _create_mock_user():
    user = MagicMock()
    user.id = "507f1f77bcf86cd799439011"
    user.username = "admin"
    user.email = "admin@empresa.com"
    user.role = "admin"
    user.is_active = True
    user.is_superuser = True
    return user


def test_send_test_email_async_returns_status_and_stream_urls():
    """
    Verifica que al encolar un correo (sync=false) se retorne HTTP 202 con status_url y stream_url.
    """
    from api.deps import get_current_user

    client = TestClient(app)
    mock_user = _create_mock_user()
    app.dependency_overrides[get_current_user] = lambda: mock_user

    mock_job = MagicMock()
    mock_job.job_id = "job_email_abc123"
    mock_pool = AsyncMock()
    mock_pool.enqueue_job = AsyncMock(return_value=mock_job)

    with patch("api.endpoints.utils.router.get_arq_pool", AsyncMock(return_value=mock_pool)):
        response = client.post(
            "/api/v1/utils/test-email",
            json={
                "to_email": "destino@empresa.com",
                "sync": False,
                "attachment_paths": ["string"]  # Swagger placeholder que debe limpiarse
            }
        )

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "ACCEPTED"
        assert data["task_id"] == "job_email_abc123"
        assert data["status_url"] == "/api/v1/tasks/job_email_abc123"
        assert data["stream_url"] == "/api/v1/tasks/job_email_abc123/stream"

        # Verificar que se pasó el user_id y los attachment_paths limpios
        mock_pool.enqueue_job.assert_called_once()
        call_kwargs = mock_pool.enqueue_job.call_args.kwargs
        assert call_kwargs["user_id"] == "507f1f77bcf86cd799439011"
        assert call_kwargs["attachment_paths"] is None

    app.dependency_overrides.clear()


def test_send_test_email_sync_success_and_failure():
    """
    Verifica que al solicitar envío síncrono (sync=true) se devuelva HTTP 200 en éxito o HTTP 502 en error.
    """
    from api.deps import get_current_user

    client = TestClient(app)
    mock_user = _create_mock_user()
    app.dependency_overrides[get_current_user] = lambda: mock_user

    mock_config = MagicMock()
    mock_config.host = "smtp.empresa.com"
    mock_config.port = 587
    mock_config.username = "sender@empresa.com"
    mock_config.sender_email = "sender@empresa.com"
    mock_config.use_tls = True
    mock_config.get_password = AsyncMock(return_value="secret")

    # Caso Exitoso (HTTP 200)
    with patch("api.endpoints.utils.router.TBEmailConfig.get_singleton", AsyncMock(return_value=mock_config)), \
         patch("api.endpoints.utils.router.send_email_async", AsyncMock(return_value={"message_id": "msg_001"})):

        response = client.post(
            "/api/v1/utils/test-email",
            json={
                "to_email": "destino@empresa.com",
                "sync": True
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert "confirmado exitosamente" in data["message"]
        assert data["details"] == {"message_id": "msg_001"}

    # Caso Fallido por error SMTP (HTTP 502)
    with patch("api.endpoints.utils.router.TBEmailConfig.get_singleton", AsyncMock(return_value=mock_config)), \
         patch("api.endpoints.utils.router.send_email_async", AsyncMock(side_effect=ConnectionRefusedError("Connection refused"))):

        response = client.post(
            "/api/v1/utils/test-email",
            json={
                "to_email": "destino@empresa.com",
                "sync": True
            }
        )
        assert response.status_code == 502
        assert "Fallo de conexión o entrega" in response.json()["detail"]

    app.dependency_overrides.clear()


def test_tasks_status_endpoint_returns_arq_and_registry_info():
    """
    Verifica que /api/v1/tasks/{task_id} combine la información de ARQ con los metadatos de Redis.
    """
    from api.deps import get_current_user
    from arq.jobs import JobStatus

    client = TestClient(app)
    mock_user = _create_mock_user()
    app.dependency_overrides[get_current_user] = lambda: mock_user

    mock_result_info = MagicMock()
    mock_result_info.function = "send_email_task"
    mock_result_info.success = True
    mock_result_info.result = {"delivered": True}
    mock_result_info.enqueue_time = None
    mock_result_info.start_time = None
    mock_result_info.finish_time = None

    mock_job = MagicMock()
    mock_job.status = AsyncMock(return_value=JobStatus.complete)
    mock_job.result_info = AsyncMock(return_value=mock_result_info)

    mock_pool = AsyncMock()
    mock_redis = AsyncMock()
    mock_redis.hget = AsyncMock(return_value=json.dumps({
        "progress_pct": 100.0,
        "message": "Correo enviado exitosamente.",
        "task_type": "email"
    }))

    with patch("api.endpoints.tasks.router.get_arq_pool", AsyncMock(return_value=mock_pool)), \
         patch("api.endpoints.tasks.router.Job", return_value=mock_job), \
         patch("api.endpoints.tasks.router.redis_client", mock_redis):

        response = client.get("/api/v1/tasks/task_email_999")
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == "task_email_999"
        assert data["status"] == "complete"
        assert data["success"] is True
        assert data["result"] == {"delivered": True}
        assert data["progress_pct"] == 100.0

    app.dependency_overrides.clear()

