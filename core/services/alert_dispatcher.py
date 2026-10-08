from __future__ import annotations

import hashlib
from typing import Any, Dict, Optional

import httpx
import redis.asyncio as redis

from core.logger import get_logger
from core.redis_client import get_redis_client
from core.services.telegram_service import send_telegram_message

logger = get_logger("alert_dispatcher")

# Prefijo global de claves Redis para candados distribuidos de debouncing
ALERT_LOCK_PREFIX = "tb_alert_lock"


def calculate_alert_hash(message: str, alert_key: Optional[str] = None) -> str:
    """
    Calcula un hash SHA-256 a partir de una clave identificadora (alert_key)
    o directamente del contenido del mensaje si no se provee la clave.

    Args:
        message: Contenido del mensaje de alerta.
        alert_key: Identificador único de la alerta (opcional).

    Returns:
        String hexadecimal con el hash SHA-256.
    """
    base_content = str(alert_key).strip() if alert_key is not None else (str(message).strip() if message is not None else "")
    return hashlib.sha256(base_content.encode("utf-8")).hexdigest()


def get_alert_lock_key(alert_hash: str) -> str:
    """
    Retorna la clave formateada de Redis para el candado distribuido de una alerta.
    """
    return f"{ALERT_LOCK_PREFIX}:{alert_hash}"


async def dispatch_debounced_alert(
    message: str,
    alert_key: Optional[str] = None,
    ttl_seconds: int = 300,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = True,
    skip_debounce: bool = False,
    redis_conn: Optional[redis.Redis] = None,
    http_client: Optional[httpx.AsyncClient] = None,
    raise_on_error: bool = False,
    release_lock_on_failure: bool = True,
    timeout_seconds: float = 15.0,
) -> Dict[str, Any]:
    """
    Despachador de alertas con circuito anti-spam / debouncer distribuido en Redis (SET NX EX).

    Responsabilidades:
    1. Calcular el hash de deduplicación SHA-256 (alert_key o message).
    2. Gestionar la cerradura atómica en Redis 'tb_alert_lock:{alert_hash}'.
    3. Si la alerta es duplicada dentro de ttl_seconds, descartarla silenciosamente (debounced).
    4. Si Redis falla, aplicar Fail-Open para garantizar la entrega de alertas críticas.
    5. Delegar el envío HTTP puro al cliente agnóstico 'send_telegram_message'.
    6. Liberar el candado si el envío falla para permitir reintentos válidos.

    Args:
        message: Texto del mensaje a enviar.
        alert_key: Clave identificadora opcional para el cálculo del hash de deduplicación.
        ttl_seconds: Tiempo de vida en segundos de la ventana de debounce (defecto: 300s = 5m).
        bot_token: Token de autenticación del Bot de Telegram (opcional).
        chat_id: ID del chat o canal de Telegram receptor (opcional).
        parse_mode: Modo de renderizado del texto en Telegram ('HTML' o 'MarkdownV2').
        disable_web_page_preview: Deshabilita la vista previa de hipervínculos si es True.
        skip_debounce: Si es True, omite la comprobación y registro en el debouncer de Redis.
        redis_conn: Instancia de redis.asyncio.Redis a reutilizar; fallback a get_redis_client().
        http_client: Instancia compartida de httpx.AsyncClient para reutilizar pools de conexiones.
        raise_on_error: Si es True, propaga excepciones HTTP/red para reintentos en ARQ.
        release_lock_on_failure: Si es True, borra el candado en Redis si el envío HTTP falla.
        timeout_seconds: Tiempo límite para la petición HTTP hacia la API de Telegram.

    Returns:
        Dict con el resultado estructurado de la operación, incluyendo 'alert_hash'.
    """
    alert_hash = calculate_alert_hash(message=message, alert_key=alert_key)
    lock_key = get_alert_lock_key(alert_hash)
    effective_redis = redis_conn or get_redis_client()
    lock_acquired = False

    # 1. Validación de Debouncer en Redis (Anti-Spam)
    if not skip_debounce:
        try:
            # Atomic SET if Not Exists with Expiration (NX EX)
            is_set = await effective_redis.set(lock_key, "1", nx=True, ex=ttl_seconds)
            if not is_set:
                logger.info(
                    f"[AlertDispatcher] Alerta descartada por debouncer (hash: {alert_hash}, lock: {lock_key}, TTL: {ttl_seconds}s)."
                )
                return {
                    "sent": False,
                    "reason": "debounced",
                    "alert_hash": alert_hash,
                }
            lock_acquired = True
            logger.debug(f"[AlertDispatcher] Candado de alerta adquirido: {lock_key} (TTL: {ttl_seconds}s)")
        except redis.RedisError as r_err:
            logger.error(
                f"[AlertDispatcher] Error en Redis al verificar debouncer para '{lock_key}': {r_err}. "
                "Procediendo con envío para garantizar entrega de alerta crítica (Fail-Open)."
            )

    # 2. Despacho a través del cliente agnóstico de Telegram
    try:
        result = await send_telegram_message(
            message=message,
            chat_id=chat_id,
            bot_token=bot_token,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            http_client=http_client,
            raise_on_error=raise_on_error,
            timeout_seconds=timeout_seconds,
        )

        # Inyectar alert_hash en la respuesta
        if isinstance(result, dict) and "alert_hash" not in result:
            result["alert_hash"] = alert_hash

        # Si el envío no fue exitoso (ej: credenciales faltantes o error HTTP con raise_on_error=False)
        if not result.get("sent"):
            if lock_acquired and release_lock_on_failure:
                try:
                    await effective_redis.delete(lock_key)
                    logger.debug(f"[AlertDispatcher] Candado '{lock_key}' liberado tras envío no exitoso.")
                except Exception as del_err:
                    logger.warning(f"[AlertDispatcher] Error liberando candado '{lock_key}': {del_err}")

        return result

    except Exception as exc:
        # Liberar candado ante excepción para permitir que los reintentos procedan
        if lock_acquired and release_lock_on_failure:
            try:
                await effective_redis.delete(lock_key)
                logger.debug(f"[AlertDispatcher] Candado '{lock_key}' liberado tras excepción en despacho.")
            except Exception as del_err:
                logger.warning(f"[AlertDispatcher] Error liberando candado '{lock_key}': {del_err}")

        if raise_on_error:
            raise

        logger.error(f"[AlertDispatcher] Error durante el despacho de alerta: {exc}", exc_info=True)
        return {
            "sent": False,
            "reason": "dispatch_error",
            "error": str(exc),
            "alert_hash": alert_hash,
        }


# Alias de conveniencia y compatibilidad
send_telegram_alert = dispatch_debounced_alert

__all__ = [
    "ALERT_LOCK_PREFIX",
    "calculate_alert_hash",
    "get_alert_lock_key",
    "dispatch_debounced_alert",
    "send_telegram_alert",
]
