import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx

from core.services.telegram_service import (
    escape_html_text,
    format_alert_message,
    send_telegram_message,
    send_telegram_alert,
)


def test_html_helpers_pure():
    """Valida los helpers puros de formateo HTML."""
    raw = "<script>alert('xss')</script> & 123"
    escaped = escape_html_text(raw)
    assert "&lt;script&gt;" in escaped
    assert "&amp;" in escaped

    msg = format_alert_message(
        title="Alerta de Inversor",
        body="Voltaje bajo",
        level="WARNING",
        tags=["PROD", "SOLAR"],
        details={"nodo": "inv-01", "v": 11.2},
    )
    assert "⚠️" in msg
    assert "<b>Alerta de Inversor</b>" in msg
    assert "#PROD #SOLAR" in msg
    assert "• <b>nodo</b>: <code>inv-01</code>" in msg


@pytest.mark.asyncio
async def test_send_telegram_message_missing_credentials():
    """Valida retorno limpio de missing_credentials cuando faltan variables."""
    with patch("core.services.telegram_service.settings") as mock_settings:
        mock_settings.TG_BOT_TOKEN = None
        mock_settings.TG_CHAT_ID = None
        with patch.dict("os.environ", {}, clear=True):
            res = await send_telegram_message("Hola mundo", bot_token=None, chat_id=None)
            assert res["sent"] is False
            assert res["reason"] == "missing_credentials"

            with pytest.raises(ValueError):
                await send_telegram_message(
                    "Hola mundo",
                    bot_token=None,
                    chat_id=None,
                    raise_on_error=True
                )


@pytest.mark.asyncio
async def test_send_telegram_message_success():
    """Valida despacho exitoso a la API de Telegram con respuesta estructurada."""
    bot_token = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
    chat_id = "-100987654321"
    text = "Notificación de prueba"

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "ok": True,
        "result": {
            "message_id": 4321,
            "chat": {"id": int(chat_id)},
            "text": text,
        },
    }
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.post.return_value = mock_resp

    res = await send_telegram_message(
        message=text,
        chat_id=chat_id,
        bot_token=bot_token,
        parse_mode="HTML",
        disable_web_page_preview=True,
        http_client=mock_client,
    )

    assert res["sent"] is True
    assert res["message_id"] == 4321
    assert res["chat_id"] == chat_id
    assert res["response"]["ok"] is True

    mock_client.post.assert_awaited_once()
    called_url, called_kwargs = mock_client.post.call_args[0][0], mock_client.post.call_args[1]
    assert called_url == f"https://api.telegram.org/bot{bot_token}/sendMessage"
    assert called_kwargs["json"]["chat_id"] == chat_id
    assert called_kwargs["json"]["text"] == text
    assert called_kwargs["json"]["parse_mode"] == "HTML"
    assert called_kwargs["json"]["disable_web_page_preview"] is True


@pytest.mark.asyncio
async def test_send_telegram_message_truncation():
    """Valida truncado seguro cuando el texto supera los 4096 caracteres."""
    bot_token = "fake_token"
    chat_id = "12345"
    long_text = "A" * 5000

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"ok": True, "result": {"message_id": 111}}
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.post.return_value = mock_resp

    res = await send_telegram_message(
        message=long_text,
        chat_id=chat_id,
        bot_token=bot_token,
        http_client=mock_client,
    )

    assert res["sent"] is True
    sent_text = mock_client.post.call_args[1]["json"]["text"]
    assert len(sent_text) == 4093
    assert sent_text.endswith("...")


@pytest.mark.asyncio
async def test_send_telegram_message_http_error():
    """Valida captura de errores HTTP (4xx / 5xx) o propagación según raise_on_error."""
    bot_token = "fake_token"
    chat_id = "12345"

    req = httpx.Request("POST", "https://api.telegram.org")
    resp = httpx.Response(400, request=req, text='{"ok":false,"description":"Bad Request: chat not found"}')
    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.post.side_effect = httpx.HTTPStatusError("400 Bad Request", request=req, response=resp)

    # 1. Sin raise_on_error
    res = await send_telegram_message(
        message="error test",
        chat_id=chat_id,
        bot_token=bot_token,
        http_client=mock_client,
        raise_on_error=False,
    )
    assert res["sent"] is False
    assert res["reason"] == "http_error"
    assert res["status_code"] == 400

    # 2. Con raise_on_error
    with pytest.raises(httpx.HTTPStatusError):
        await send_telegram_message(
            message="error test",
            chat_id=chat_id,
            bot_token=bot_token,
            http_client=mock_client,
            raise_on_error=True,
        )


@pytest.mark.asyncio
async def test_send_telegram_message_request_error():
    """Valida captura de errores de conexión y transporte."""
    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.post.side_effect = httpx.ConnectError("Connection refused")

    res = await send_telegram_message(
        message="network test",
        chat_id="123",
        bot_token="token",
        http_client=mock_client,
        raise_on_error=False,
    )
    assert res["sent"] is False
    assert res["reason"] == "request_error"

    with pytest.raises(httpx.RequestError):
        await send_telegram_message(
            message="network test",
            chat_id="123",
            bot_token="token",
            http_client=mock_client,
            raise_on_error=True,
        )


@pytest.mark.asyncio
async def test_send_telegram_alert_alias_and_legacy_kwargs():
    """Valida que send_telegram_alert sea un alias a send_telegram_message y absorba kwargs legados sin fallar."""
    assert send_telegram_alert is send_telegram_message

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"ok": True, "result": {"message_id": 99}}
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.post.return_value = mock_resp

    # Pasa kwargs legados (alert_key, ttl_seconds, redis_conn, skip_debounce)
    res = await send_telegram_alert(
        message="Alerta con parámetros legados",
        chat_id="123",
        bot_token="token",
        http_client=mock_client,
        alert_key="legacy:key",
        ttl_seconds=300,
        redis_conn=None,
        skip_debounce=True,
        release_lock_on_failure=True,
    )
    assert res["sent"] is True
    assert res["message_id"] == 99
