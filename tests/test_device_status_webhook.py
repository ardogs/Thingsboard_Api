import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from httpx import AsyncClient, ASGITransport
from pydantic import ValidationError

from api.endpoints.telemetry.router import DeviceStatusWebhookRequest, device_status_webhook
from api.main import app


def test_device_status_webhook_request_validation():
    """Valida la sanitización y validaciones de Pydantic v2 en DeviceStatusWebhookRequest."""
    # Caso válido estándar
    req = DeviceStatusWebhookRequest(
        device_name="  sensor-temp-01  ",
        status="  CRITICAL  ",
        layer=1,
        tenant_id="  tenant-100  ",
        message="  Temperatura fuera de rango  ",
        details={"temp_c": 85.5, "unit": "C"}
    )
    assert req.device_name == "sensor-temp-01"
    assert req.status == "CRITICAL"
    assert req.layer == 1
    assert req.tenant_id == "tenant-100"
    assert req.message == "Temperatura fuera de rango"
    assert req.details == {"temp_c": 85.5, "unit": "C"}

    # Caso con capa en string
    req_str_layer = DeviceStatusWebhookRequest(
        device_name="device-02",
        status="OFFLINE",
        layer="Layer-Core",
        tenant_id="tenant-200"
    )
    assert req_str_layer.layer == "Layer-Core"
    assert req_str_layer.details is None
    assert req_str_layer.message is None


def test_device_status_webhook_request_rejects_empty_and_whitespace():
    """Verifica que cadenas vacías o compuestas exclusivamente de espacios sean rechazadas con ValidationError."""
    # device_name vacío
    with pytest.raises(ValidationError):
        DeviceStatusWebhookRequest(
            device_name="   ",
            status="ONLINE",
            layer=1,
            tenant_id="tenant-1"
        )

    # status vacío
    with pytest.raises(ValidationError):
        DeviceStatusWebhookRequest(
            device_name="sensor-01",
            status=" \t ",
            layer=1,
            tenant_id="tenant-1"
        )

    # tenant_id vacío
    with pytest.raises(ValidationError):
        DeviceStatusWebhookRequest(
            device_name="sensor-01",
            status="ONLINE",
            layer=1,
            tenant_id="  "
        )

    # layer vacío
    with pytest.raises(ValidationError):
        DeviceStatusWebhookRequest(
            device_name="sensor-01",
            status="ONLINE",
            layer="   ",
            tenant_id="tenant-1"
        )


@pytest.mark.asyncio
async def test_device_status_webhook_endpoint_direct():
    """Invoca la función async device_status_webhook directamente con un mock del pool ARQ."""
    mock_job = MagicMock()
    mock_job.job_id = "test-job-uuid-1234"

    mock_pool = MagicMock()
    mock_pool.enqueue_job = AsyncMock(return_value=mock_job)

    payload = DeviceStatusWebhookRequest(
        device_name="edge-gateway-alpha",
        status="CRITICAL",
        layer=2,
        tenant_id="tenant-xyz-888",
        message="Voltaje crítico detectado en nodo de alimentación",
        details={"voltage_v": 10.2, "threshold_v": 11.5}
    )

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_pool)):
        response = await device_status_webhook(payload)

    assert response == {
        "status": "accepted",
        "job_id": "test-job-uuid-1234",
        "device_name": "edge-gateway-alpha",
        "device_status": "CRITICAL",
        "layer": "2",
        "tenant_id": "tenant-xyz-888",
    }

    mock_pool.enqueue_job.assert_awaited_once()
    called_task, called_kwargs = mock_pool.enqueue_job.call_args[0][0], mock_pool.enqueue_job.call_args[1]
    assert called_task == "send_telegram_alert_task"
    job_payload = called_kwargs["payload"]
    assert job_payload["alert_key"] == "device_status:tenant-xyz-888:edge-gateway-alpha:CRITICAL"
    assert job_payload["ttl_seconds"] == 300
    assert "edge-gateway-alpha" in job_payload["message"]
    assert "CRITICAL" in job_payload["message"]
    assert "tenant-xyz-888" in job_payload["message"]


@pytest.mark.asyncio
async def test_device_status_webhook_via_http_client():
    """Valida la llamada HTTP al endpoint POST /api/v1/telemetry/webhooks/device-status sin autenticación JWT previa."""
    mock_job = MagicMock()
    mock_job.job_id = "arq-job-9999"

    mock_pool = MagicMock()
    mock_pool.enqueue_job = AsyncMock(return_value=mock_job)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_pool)):
            response = await client.post(
                "/api/v1/telemetry/webhooks/device-status",
                json={
                    "device_name": "modbus-inverter-01",
                    "status": "OFFLINE",
                    "layer": "Level-3",
                    "tenant_id": "tenant-corp-42",
                    "details": {"last_seen_epoch": 1727611200}
                }
            )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["job_id"] == "arq-job-9999"
    assert body["device_name"] == "modbus-inverter-01"
    assert body["device_status"] == "OFFLINE"
    assert body["layer"] == "Level-3"
    assert body["tenant_id"] == "tenant-corp-42"
