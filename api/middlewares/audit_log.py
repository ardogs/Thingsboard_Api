import json
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from core.models.audit_log import AuditLog
from core.security import decode_access_token
from core.logger import logger

SENSITIVE_FIELD_PATTERNS = {
    "password",
    "token",
    "refresh_token",
    "new_password",
    "setup_token",
    "secret",
    "access_token",
    "authorization",
    "old_password",
    "current_password",
    "client_secret",
    "api_key"
}


def sanitize_dict_or_list(data: Any) -> Any:
    """
    Sanitiza recursivamente un diccionario o lista enmascarando cualquier valor asociado
    a claves sensibles (password, token, refresh_token, etc.) con '***'.
    """
    if isinstance(data, dict):
        sanitized = {}
        for k, v in data.items():
            k_lower = str(k).lower()
            if any(pattern in k_lower for pattern in SENSITIVE_FIELD_PATTERNS):
                sanitized[k] = "***"
            elif isinstance(v, (dict, list)):
                sanitized[k] = sanitize_dict_or_list(v)
            else:
                sanitized[k] = v
        return sanitized
    elif isinstance(data, list):
        return [sanitize_dict_or_list(item) for item in data]
    return data


def parse_and_sanitize_payload(body_bytes: bytes, content_type: str) -> Optional[Dict[str, Any]]:
    """
    Parsea los bytes del cuerpo de la petición HTTP según su Content-Type y retorna un diccionario sanitizado.
    """
    if not body_bytes:
        return None

    ct = content_type.lower()

    # 1. Payload en formato JSON
    if "application/json" in ct or not ct:
        try:
            parsed = json.loads(body_bytes.decode("utf-8"))
            if isinstance(parsed, dict):
                return sanitize_dict_or_list(parsed)
            elif isinstance(parsed, list):
                return {"_items": sanitize_dict_or_list(parsed)}
        except Exception:
            pass

    # 2. Payload en formato Form Data / x-www-form-urlencoded
    if "application/x-www-form-urlencoded" in ct:
        try:
            parsed_qs = urllib.parse.parse_qs(body_bytes.decode("utf-8"))
            flattened = {k: v[0] if len(v) == 1 else v for k, v in parsed_qs.items()}
            return sanitize_dict_or_list(flattened)
        except Exception:
            return {"_raw": "[Unparseable Form Data]"}

    # 3. Payload Multipart o Binario
    if "multipart/form-data" in ct:
        return {"_info": "[Multipart Form Data - Masked]"}

    # Fallback general para texto plano
    try:
        text_str = body_bytes.decode("utf-8")
        parsed = json.loads(text_str)
        return sanitize_dict_or_list(parsed) if isinstance(parsed, dict) else {"_data": str(parsed)}
    except Exception:
        return {"_info": f"[Non-JSON Payload: {len(body_bytes)} bytes]"}


class AuditLogMiddleware(BaseHTTPMiddleware):
    """
    Middleware de FastAPI / Starlette para auditoría y trazabilidad DevSecOps.
    Intercepta todas las operaciones mutantes (POST, PUT, DELETE, PATCH), sanitiza credenciales y
    guarda el registro en la colección 'audit_logs' de MongoDB sin interferir con la petición del cliente.
    """

    async def dispatch(self, request: Request, call_next):
        # 1. Solo registrar operaciones mutantes
        if request.method not in ("POST", "PUT", "DELETE", "PATCH"):
            return await call_next(request)

        # 2. Interceptar body preservando el stream ASGI para downstream handlers
        body_bytes = await request.body()

        async def receive():
            return {"type": "http.request", "body": body_bytes}

        request = Request(request.scope, receive=receive)

        # 3. Sanitizar payload
        content_type = request.headers.get("content-type", "")
        sanitized_payload = parse_and_sanitize_payload(body_bytes, content_type)

        # 4. Extraer IP del cliente (con soporte para proxies y balanceadores)
        ip_address = "127.0.0.1"
        if "x-forwarded-for" in request.headers:
            ip_address = request.headers["x-forwarded-for"].split(",")[0].strip()
        elif request.client and request.client.host:
            ip_address = request.client.host

        # 5. Extraer user_id del Authorization Bearer token si viene presente
        user_id: Optional[str] = None
        auth_header = request.headers.get("authorization")
        if auth_header and auth_header.startswith("Bearer "):
            raw_token = auth_header.split(" ", 1)[1].strip()
            try:
                jwt_data = decode_access_token(raw_token)
                user_id = jwt_data.get("user_id") or jwt_data.get("sub")
            except Exception:
                pass

        # 6. Procesar la petición HTTP downstream
        response: Response = await call_next(request)

        # 7. Registrar evento de auditoría asíncronamente en MongoDB
        try:
            audit_entry = AuditLog(
                timestamp=datetime.now(timezone.utc),
                user_id=str(user_id) if user_id else None,
                ip_address=ip_address,
                method=request.method,
                endpoint=request.url.path,
                status_code=response.status_code,
                payload=sanitized_payload
            )
            await audit_entry.insert()
        except Exception as e:
            logger.error(f"[AuditLogMiddleware] Error al registrar evento de auditoría en MongoDB: {e}")

        return response
