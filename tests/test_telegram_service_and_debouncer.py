import hashlib
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from arq import Retry

from core.config import settings
from core.services.telegram_service import (
    escape_html_text,
    format_alert_message,
)
from core.services.alert_dispatcher import (
    calculate_alert_hash,
    get_alert_lock_key,
    dispatch_debounced_alert,
    send_telegram_alert,
)
from workers.tasks import send_telegram_alert_task
from workers.arq_settings import REGISTERED_FUNCTIONS


class MockRedis:
    """In-memory Async Redis mock for deterministic testing of locks and TTLs."""
    def __init__(self):
        self.store = {}
        self.ttls = {}

    async def set(self, key: str, value: str, nx: bool = False, ex: int = None):
        if nx and key in self.store:
            return None
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex
        return True

    async def get(self, key: str):
        return self.store.get(key)

    async def delete(self, key: str):
        existed = key in self.store
        self.store.pop(key, None)
        self.ttls.pop(key, None)
        return 1 if existed else 0


@pytest.mark.asyncio
async def test_config_telegram_fields():
    """Verifica que core.config.Settings cuente con los campos TG_BOT_TOKEN y TG_CHAT_ID."""
    assert hasattr(settings, "TG_BOT_TOKEN")
    assert hasattr(settings, "TG_CHAT_ID")


def test_calculate_alert_hash_and_helpers():
    """Valida el cálculo del hash SHA-256 para mensajes y alert_keys, y helpers de formato."""
    msg = " Servidor TB caído  "
    expected_hash = hashlib.sha256("Servidor TB caído".encode("utf-8")).hexdigest()
    assert calculate_alert_hash(msg) == expected_hash

    # Con alert_key explícito
    custom_key = "server_down:server_101"
    expected_custom_hash = hashlib.sha256(custom_key.encode("utf-8")).hexdigest()
    assert calculate_alert_hash(msg, alert_key=custom_key) == expected_custom_hash

    # Key en Redis
    assert get_alert_lock_key(expected_hash) == f"tb_alert_lock:{expected_hash}"

    # Escapado HTML
    raw_text = "<b>Prueba</b> & 'test' <tag>"
    escaped = escape_html_text(raw_text)
    assert "&lt;b&gt;" in escaped
    assert "&amp;" in escaped

    # Formateador de mensaje
    formatted = format_alert_message(
        title="Alerta de CPU",
        body="CPU > 95%",
        level="CRITICAL",
        tags=["PROD", "SERVER1"],
        details={"Servidor": "TB-01", "Carga": "98%"},
    )
    assert "🚨" in formatted
    assert "Alerta de CPU" in formatted
    assert "#PROD" in formatted
    assert "• <b>Servidor</b>: <code>TB-01</code>" in formatted


@pytest.mark.asyncio
async def test_send_telegram_alert_missing_credentials():
    """Valida que si no hay credenciales, no se intente enviar y se libere cualquier candado."""
    mock_redis = MockRedis()
    result = await send_telegram_alert(
        message="Prueba sin credenciales",
        bot_token="",
        chat_id="",
        redis_conn=mock_redis,
    )
    assert result["sent"] is False
    assert result["reason"] == "missing_credentials"
    assert len(mock_redis.store) == 0  # El candado debe haberse liberado


@pytest.mark.asyncio
async def test_send_telegram_alert_success_and_debouncer():
    """Valida el ciclo completo de envío exitoso y deduplicación anti-spam en Redis."""
    mock_redis = MockRedis()
    bot_token = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
    chat_id = "-100123456789"
    message = "Notificación de respaldo finalizado con éxito"

    # Mock de respuesta HTTP de Telegram
    telegram_resp = {
        "ok": True,
        "result": {
            "message_id": 999123,
            "chat": {"id": int(chat_id), "type": "supergroup"},
            "text": message,
        }
    }

    mock_http_client = AsyncMock(spec=httpx.AsyncClient)
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = telegram_resp
    mock_response.raise_for_status = MagicMock()
    mock_http_client.post.return_value = mock_response

    # 1. Primer envío -> Debe adquirirse el candado y enviarse a Telegram
    res1 = await send_telegram_alert(
        message=message,
        bot_token=bot_token,
        chat_id=chat_id,
        ttl_seconds=300,
        redis_conn=mock_redis,
        http_client=mock_http_client,
    )
    assert res1["sent"] is True
    assert res1["message_id"] == 999123
    assert mock_http_client.post.call_count == 1

    # Verificar que el candado esté en Redis con TTL 300
    alert_hash = res1["alert_hash"]
    lock_key = f"tb_alert_lock:{alert_hash}"
    assert mock_redis.store.get(lock_key) == "1"
    assert mock_redis.ttls.get(lock_key) == 300

    # 2. Segundo envío idéntico inmediato -> DEBOUNCER debe descartarlo
    res2 = await send_telegram_alert(
        message=message,
        bot_token=bot_token,
        chat_id=chat_id,
        ttl_seconds=300,
        redis_conn=mock_redis,
        http_client=mock_http_client,
    )
    assert res2["sent"] is False
    assert res2["reason"] == "debounced"
    assert res2["alert_hash"] == alert_hash
    # HTTP client NO debió ser llamado una segunda vez
    assert mock_http_client.post.call_count == 1

    # 3. Envío con skip_debounce=True -> Debe enviar ignorando el debouncer
    res3 = await send_telegram_alert(
        message=message,
        bot_token=bot_token,
        chat_id=chat_id,
        skip_debounce=True,
        redis_conn=mock_redis,
        http_client=mock_http_client,
    )
    assert res3["sent"] is True
    assert mock_http_client.post.call_count == 2


@pytest.mark.asyncio
async def test_send_telegram_alert_releases_lock_on_failure():
    """Valida que ante un error HTTP o de red, el candado se libere para permitir reintentos."""
    mock_redis = MockRedis()
    bot_token = "fake_token"
    chat_id = "fake_chat"
    message = "Mensaje con fallo transitorio"

    mock_http_client = AsyncMock(spec=httpx.AsyncClient)
    req = httpx.Request("POST", "https://api.telegram.org")
    resp = httpx.Response(502, request=req, text="Bad Gateway")
    mock_http_client.post.side_effect = httpx.HTTPStatusError("502 Bad Gateway", request=req, response=resp)

    # 1. Envío con raise_on_error=False
    res = await send_telegram_alert(
        message=message,
        bot_token=bot_token,
        chat_id=chat_id,
        redis_conn=mock_redis,
        http_client=mock_http_client,
        raise_on_error=False,
        release_lock_on_failure=True,
    )
    assert res["sent"] is False
    assert res["reason"] == "http_error"
    assert res["status_code"] == 502

    # El candado en Redis debe haberse borrado
    alert_hash = res["alert_hash"]
    lock_key = f"tb_alert_lock:{alert_hash}"
    assert lock_key not in mock_redis.store


@pytest.mark.asyncio
async def test_send_telegram_alert_task_successful():
    """Valida la ejecución exitosa de send_telegram_alert_task como tarea ARQ."""
    mock_redis = MockRedis()
    mock_http_client = AsyncMock(spec=httpx.AsyncClient)
    telegram_resp = {"ok": True, "result": {"message_id": 4567}}
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = telegram_resp
    mock_http_client.post.return_value = mock_response

    ctx = {
        "job_id": "test_job_1",
        "job_try": 1,
        "redis": mock_redis,
        "http_client": mock_http_client,
    }

    payload = {
        "message": "Alerta enviada desde worker ARQ",
        "bot_token": "token123",
        "chat_id": "chat123",
    }

    result = await send_telegram_alert_task(ctx, payload=payload)
    assert result["sent"] is True
    assert result["message_id"] == 4567


@pytest.mark.asyncio
async def test_send_telegram_alert_task_debounced():
    """Valida que la tarea ARQ no reintente cuando la alerta es descartada por el debouncer."""
    mock_redis = MockRedis()
    mock_http_client = AsyncMock(spec=httpx.AsyncClient)
    telegram_resp = {"ok": True, "result": {"message_id": 1111}}
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = telegram_resp
    mock_http_client.post.return_value = mock_response

    ctx = {
        "job_id": "test_job_2",
        "job_try": 1,
        "redis": mock_redis,
        "http_client": mock_http_client,
    }

    payload = {
        "message": "Alerta duplicada",
        "bot_token": "token123",
        "chat_id": "chat123",
    }

    # Primer intento: éxito
    res1 = await send_telegram_alert_task(ctx, payload=payload)
    assert res1["sent"] is True

    # Segundo intento con el mismo mensaje: debounced (sin reintentos)
    res2 = await send_telegram_alert_task(ctx, payload=payload)
    assert res2["sent"] is False
    assert res2["reason"] == "debounced"


@pytest.mark.asyncio
async def test_send_telegram_alert_task_transient_retry():
    """Valida que la tarea ARQ eleve arq.Retry ante errores HTTP 429 / 5xx."""
    mock_redis = MockRedis()
    mock_http_client = AsyncMock(spec=httpx.AsyncClient)

    req = httpx.Request("POST", "https://api.telegram.org")
    resp = httpx.Response(429, request=req, headers={"Retry-After": "10"}, text="Too Many Requests")
    mock_http_client.post.side_effect = httpx.HTTPStatusError("429 Rate Limit", request=req, response=resp)

    ctx = {
        "job_id": "test_job_3",
        "job_try": 1,
        "redis": mock_redis,
        "http_client": mock_http_client,
    }

    payload = {
        "message": "Alerta con rate limit",
        "bot_token": "token123",
        "chat_id": "chat123",
    }

    with pytest.raises(Retry) as exc_info:
        await send_telegram_alert_task(ctx, payload=payload)

    assert exc_info.value.defer_score == 10 or exc_info.value.defer_score is not None


def test_registered_functions_in_arq():
    """Valida que send_telegram_alert_task esté registrada en REGISTERED_FUNCTIONS de arq_settings."""
    function_names = []
    for item in REGISTERED_FUNCTIONS:
        if hasattr(item, "coroutine"):
            function_names.append(item.name)
        elif hasattr(item, "__name__"):
            function_names.append(item.__name__)

    assert "send_telegram_alert_task" in function_names
    assert "tasks.send_telegram_alert" in function_names
