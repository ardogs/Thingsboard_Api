import json
import redis.asyncio as redis
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from core.logger import get_logger

logger = get_logger("task_registry")


def get_user_stream_channel(user_id: str, task_id: str) -> str:
    """Retorna el canal de Redis Pub/Sub para streaming SSE por usuario y tarea."""
    return f"user:{user_id}:stream:{task_id}"


def get_user_registry_key(user_id: str) -> str:
    """Retorna la clave Hash de Redis donde se indexan las tareas activas del usuario."""
    return f"tb_events:user:{user_id}:registry"


async def publish_task_event(
    redis_client: redis.Redis,
    user_id: str,
    task_id: str,
    status: str,
    task_type: str = "general",
    progress_pct: float = 0.0,
    message: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    cleanup_on_terminal: bool = True,
    **extra_fields
) -> Dict[str, Any]:
    """
    Publica un evento universal de ciclo de vida de tarea en Redis Pub/Sub (SSE)
    y sincroniza el registro Hash del usuario en Redis.

    Soporta cualquier tipo de tarea: telemetry, email, excel_report, heatmap, etc.
    """
    normalized_pct = max(0.0, min(100.0, round(float(progress_pct), 2)))
    payload: Dict[str, Any] = {
        "task_id": task_id,
        "user_id": user_id,
        "task_type": task_type,
        "status": status,
        "progress_pct": normalized_pct,
        "message": message,
        "details": details or {},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    # Integrar campos adicionales (ej: tenant_name, current_device para telemetría)
    payload.update(extra_fields)

    payload_json = json.dumps(payload)
    channel = get_user_stream_channel(user_id, task_id)
    registry_key = get_user_registry_key(user_id)

    try:
        await redis_client.publish(channel, payload_json)
        is_terminal = status in ("SUCCESS", "ERROR", "FAILURE", "CANCELLED")
        if is_terminal and cleanup_on_terminal:
            # En estado terminal se elimina del hash activo para no inflar la lista de tareas en ejecución
            await redis_client.hdel(registry_key, task_id)
        else:
            await redis_client.hset(registry_key, task_id, payload_json)
    except Exception as e:
        logger.error(f"[TaskRegistry] Error publicando evento para tarea {task_id} (user {user_id}): {e}")

    return payload
