from __future__ import annotations

import html
import os
from typing import Any, Dict, List, Optional, Union

import httpx

from core.config import settings
from core.logger import get_logger

logger = get_logger("telegram_service")


def escape_html_text(text: str) -> str:
    """
    Escapa caracteres especiales (&, <, >) para parse_mode='HTML' en Telegram Bot API.
    """
    return html.escape(str(text), quote=False)


def format_alert_message(
    title: str,
    body: str,
    level: str = "INFO",
    tags: Optional[List[str]] = None,
    details: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Helper utilitario para dar formato HTML limpio y estandarizado a mensajes de alerta de Telegram.

    Args:
        title: Título o encabezado de la alerta.
        body: Descripción o texto principal del suceso.
        level: Nivel de severidad ('INFO', 'WARNING', 'ERROR', 'CRITICAL', 'SUCCESS').
        tags: Lista de etiquetas o tópicos (ej: ['PROD', 'DATABASE', 'BACKUP']).
        details: Diccionario de metadatos o parámetros clave-valor a incluir como código.

    Returns:
        Mensaje formateado en HTML compatible con Telegram.
    """
    icons = {
        "CRITICAL": "🚨",
        "ERROR": "❌",
        "WARNING": "⚠️",
        "INFO": "ℹ️",
        "SUCCESS": "✅",
    }
    icon = icons.get(level.upper(), "🔔")

    lines = [f"{icon} <b>{escape_html_text(title)}</b>"]

    if tags:
        tag_str = " ".join([f"#{escape_html_text(t)}" for t in tags])
        lines.append(f"<i>{tag_str}</i>")

    lines.append("")
    lines.append(escape_html_text(body))

    if details:
        lines.append("")
        lines.append("<b>Detalles:</b>")
        for k, v in details.items():
            lines.append(f"• <b>{escape_html_text(str(k))}</b>: <code>{escape_html_text(str(v))}</code>")

    return "\n".join(lines)


async def send_telegram_message(
    message: str,
    chat_id: Optional[str] = None,
    bot_token: Optional[str] = None,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = True,
    http_client: Optional[httpx.AsyncClient] = None,
    raise_on_error: bool = False,
    timeout_seconds: float = 15.0,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Cliente HTTP agnóstico, 100% asíncrono y no bloqueante para Telegram Bot API (sendMessage).

    Responsabilidades únicas:
    - Resolver credenciales TG_BOT_TOKEN y TG_CHAT_ID (parámetros, settings o env).
    - Formatear payload de Telegram (chat_id, text, parse_mode, disable_web_page_preview).
    - Truncar de forma segura mensajes > 4096 caracteres.
    - Despachar petición HTTP POST a https://api.telegram.org/bot{bot_token}/sendMessage vía HTTPX.
    - Retornar diccionario estructurado con resultado o propagar excepción si raise_on_error=True.

    Agnóstico: Totalmente libre de dependencias de Redis, debouncing o candados distribuidos.
    """
    # 1. Resolución de Credenciales de Telegram
    resolved_bot_token = bot_token or getattr(settings, "TG_BOT_TOKEN", None) or os.getenv("TG_BOT_TOKEN")
    resolved_chat_id = chat_id or getattr(settings, "TG_CHAT_ID", None) or os.getenv("TG_CHAT_ID")

    if not resolved_bot_token or not resolved_chat_id:
        logger.warning(
            "[TelegramService] TG_BOT_TOKEN o TG_CHAT_ID no configurados. Omitiendo envío de mensaje a Telegram."
        )
        if raise_on_error:
            raise ValueError(
                "TG_BOT_TOKEN y TG_CHAT_ID deben estar configurados en parámetros, settings o variables de entorno."
            )
        return {
            "sent": False,
            "reason": "missing_credentials",
            "error": "TG_BOT_TOKEN o TG_CHAT_ID no se encuentran configurados en settings ni en variables de entorno.",
        }

    # 2. Preparación de Petición HTTP a Telegram Bot API
    url = f"https://api.telegram.org/bot{resolved_bot_token}/sendMessage"

    # Truncar con seguridad si excede el límite estricto de Telegram (4096 caracteres)
    final_message = str(message) if message is not None else ""
    if len(final_message) > 4096:
        logger.warning(
            f"[TelegramService] El mensaje excede 4096 caracteres ({len(final_message)} chars). "
            "Truncando a 4090 caracteres para cumplir con la API de Telegram."
        )
        final_message = final_message[:4090] + "..."

    payload: Dict[str, Any] = {
        "chat_id": str(resolved_chat_id),
        "text": final_message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }

    # 3. Envío Asíncrono con HTTPX
    timeout = httpx.Timeout(
        connect=min(5.0, timeout_seconds),
        read=timeout_seconds,
        write=timeout_seconds,
        pool=timeout_seconds,
    )

    try:
        if http_client is not None:
            resp = await http_client.post(url, json=payload, timeout=timeout)
        else:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(url, json=payload)

        resp.raise_for_status()
        resp_json = resp.json()

        if not resp_json.get("ok"):
            error_desc = resp_json.get("description", "Error desconocido devuelto por Telegram API")
            raise httpx.HTTPStatusError(
                message=f"Telegram API ok=False: {error_desc}",
                request=resp.request,
                response=resp,
            )

        message_id = resp_json.get("result", {}).get("message_id")
        logger.info(
            f"[TelegramService] Mensaje enviado a Telegram con éxito. ChatID: {resolved_chat_id}, MsgID: {message_id}"
        )
        return {
            "sent": True,
            "message_id": message_id,
            "chat_id": str(resolved_chat_id),
            "response": resp_json,
        }

    except httpx.HTTPStatusError as exc:
        logger.error(
            f"[TelegramService] Error HTTP al contactar Telegram ({exc.response.status_code}): {exc.response.text}"
        )
        if raise_on_error:
            raise
        return {
            "sent": False,
            "reason": "http_error",
            "status_code": exc.response.status_code,
            "error": str(exc),
            "response_text": exc.response.text,
        }

    except httpx.RequestError as exc:
        logger.error(f"[TelegramService] Error de red/transporte al contactar Telegram: {exc}")
        if raise_on_error:
            raise
        return {
            "sent": False,
            "reason": "request_error",
            "error": str(exc),
        }

    except Exception as exc:
        logger.error(f"[TelegramService] Error inesperado en send_telegram_message: {exc}", exc_info=True)
        if raise_on_error:
            raise
        return {
            "sent": False,
            "reason": "unexpected_error",
            "error": str(exc),
        }


# Alias de compatibilidad hacia atrás sin dependencias de Redis
send_telegram_alert = send_telegram_message

__all__ = [
    "escape_html_text",
    "format_alert_message",
    "send_telegram_message",
    "send_telegram_alert",
]
