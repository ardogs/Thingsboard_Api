from datetime import datetime, timezone
from typing import Optional, List, Dict, Union, Any
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from beanie import PydanticObjectId
from jose import JWTError

from core.models.user import User
from core.tb_client import ThingsBoardClient
from core.redis_client import redis_client
from core.config import settings
from core.logger import logger
from core.security import (
    verify_password,
    get_password_hash,
    create_access_token,
    decode_access_token,
    validate_password_policy
)
from api.deps import get_current_user, oauth2_scheme
from api.endpoints.users.router import UserResponse, _to_user_response

router = APIRouter()

MAX_FAILED_LOGIN_ATTEMPTS = 5
LOGIN_LOCKOUT_SECONDS = 15 * 60  # 15 minutos (900 segundos)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    username: str
    must_change_password: bool = False
    message: Optional[str] = None


class TenantPermissionsMap(BaseModel):
    canRead: bool = False
    canWrite: bool = False
    canDelete: bool = False


class UserTenantAccessResponse(BaseModel):
    id: str = Field(..., description="ID de MongoDB del Tenant")
    name: str = Field(..., description="Nombre comercial o identificador del Tenant")
    server_id: Optional[str] = Field(default=None, description="ID de MongoDB del Servidor ThingsBoard padre")
    server_name: Optional[str] = Field(default=None, description="Nombre descriptivo del Servidor")
    server_base_url: Optional[str] = Field(default=None, description="URL base del Servidor ThingsBoard")
    is_active: bool = Field(default=True, description="Estado de activación del Tenant")
    role: Optional[str] = Field(default=None, description="Rol asignado en el Tenant (ej. tenant_admin, operator, viewer, custom, superadmin)")
    permissions: Dict[str, TenantPermissionsMap] = Field(
        default_factory=dict,
        description="Permisos efectivos por recurso (ej: telemetry, devices, scheduler)"
    )


class LoginRequest(BaseModel):
    server_url: str = "https://thingsboard.cloud"
    username: str
    password: str


class ChangePasswordRequest(BaseModel):
    new_password: str = Field(..., description="Nueva contraseña permanente que cumple con la política estricta de seguridad")
    current_password: Optional[str] = Field(default=None, description="Contraseña actual de un solo uso (opcional si se envía token Bearer)")


class ChangePasswordResponse(BaseModel):
    status: str = "ok"
    message: str
    user_id: str
    username: str
    access_token: str
    token_type: str = "bearer"


def _get_client_ip(request: Request) -> str:
    """
    Extrae la IP del cliente considerando posibles cabeceras de proxy inverso (X-Forwarded-For).
    """
    forwarded = request.headers.get("X-Forwarded-For") or request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "127.0.0.1"


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Iniciar sesión OAuth2 / JWT (con protección anti fuerza bruta)",
    description="""
Autentica un usuario mediante credenciales estándar OAuth2 (Form Data: `username` y `password`).

### 🛡️ Protección contra Fuerza Bruta y Rate Limiting:
- **Umbral de Intentos:** Máximo 5 intentos fallidos consecutivos por combinación de `IP` + `username`.
- **Ventana de Bloqueo:** Tras el 5to intento fallido, el acceso queda **bloqueado temporalmente durante 15 minutos (900 segundos)**.
- **Respuesta de Bloqueo (`HTTP 429 Too Many Requests`):**
  - **Cabecera `Retry-After`:** Especifica el número exacto de segundos restantes antes de que expire el bloqueo.
  - **Mensaje Dinámico:** Retorna el tiempo restante calculado en minutos y segundos (ej. *15 minuto(s) (899 segundos)*).
- **Desbloqueo Automático:** Tras expirar el tiempo de bloqueo en Redis o ingresar credenciales correctas, el contador se reinicia a 0.

### 🔑 Primer Inicio de Sesión / Contraseña Temporal:
- Si el usuario fue creado con una contraseña de un solo uso, el token emitido contendrá `must_change_password: true`.
- En este estado, el usuario tendrá acceso **restringido exclusivamente** a `/api/v1/auth/change-password` hasta que defina su contraseña permanente.
""",
    responses={
        200: {
            "description": "Autenticación exitosa. Retorna el token JWT en el cuerpo JSON e inyecta la cookie HttpOnly 'access_token'.",
            "model": TokenResponse
        },
        400: {
            "description": "Cuenta inactiva o deshabilitada.",
            "content": {
                "application/json": {
                    "example": {"detail": "La cuenta de usuario se encuentra inactiva o deshabilitada"}
                }
            }
        },
        401: {
            "description": "Credenciales inválidas (Usuario o contraseña incorrectos). Incrementa el contador de intentos fallidos.",
            "content": {
                "application/json": {
                    "example": {"detail": "Usuario o contraseña incorrectos"}
                }
            }
        },
        429: {
            "description": "Demasiados intentos fallidos. Acceso bloqueado temporalmente por política anti fuerza bruta (15 minutos).",
            "headers": {
                "Retry-After": {
                    "description": "Número de segundos restantes antes de que expire el bloqueo temporal.",
                    "schema": {"type": "integer", "example": 899}
                }
            },
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Demasiados intentos fallidos de inicio de sesión. Acceso bloqueado temporalmente. Intente nuevamente en 15 minuto(s) (899 segundos)."
                    }
                }
            }
        }
    }
)
async def login(
    request: Request,
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends()
):
    client_ip = _get_client_ip(request)
    rate_limit_key = f"tb_auth_failed:{client_ip}:{form_data.username}"

    # 1. Verificar si la IP o cuenta ha superado el umbral de intentos fallidos
    try:
        failed_attempts = await redis_client.get(rate_limit_key)
        if failed_attempts and int(failed_attempts) >= MAX_FAILED_LOGIN_ATTEMPTS:
            ttl = await redis_client.ttl(rate_limit_key)
            ttl = max(ttl, 1)
            minutes = (ttl + 59) // 60
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Demasiados intentos fallidos de inicio de sesión. Acceso bloqueado temporalmente. Intente nuevamente en {minutes} minuto(s) ({ttl} segundos).",
                headers={"Retry-After": str(ttl)}
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"[RateLimit] No se pudo consultar Redis para rate limiting: {e}")

    # 2. Buscar usuario por nombre de usuario o por email
    user = await User.find_one(User.username == form_data.username)
    if not user:
        user = await User.find_one(User.email == form_data.username)

    # 3. Validar existencia y coincidencia de contraseña bcrypt
    if not user or not user.hashed_password or not verify_password(form_data.password, user.hashed_password):
        # Incrementar contador de intentos fallidos en Redis si está disponible
        try:
            attempts = await redis_client.incr(rate_limit_key)
            if attempts == 1:
                await redis_client.expire(rate_limit_key, LOGIN_LOCKOUT_SECONDS)
        except Exception as e:
            logger.warning(f"[RateLimit] No se pudo actualizar contador de intentos fallidos en Redis: {e}")

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La cuenta de usuario se encuentra inactiva o deshabilitada"
        )

    # 4. Autenticación exitosa: Resetear contador de fallos
    try:
        await redis_client.delete(rate_limit_key)
    except Exception:
        pass

    access_token = create_access_token(
        subject=str(user.id),
        user_data={
            "username": user.username,
            "role": user.role,
            "is_superuser": user.is_superuser,
            "must_change_password": user.must_change_password
        }
    )

    # Inyectar cookie HttpOnly para sesiones web
    response.set_cookie(
        key="access_token",
        value=f"Bearer {access_token}",
        httponly=True,
        samesite="lax",
        secure=settings.COOKIE_SECURE,
        path="/",
        max_age=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )

    login_message = None
    if user.must_change_password:
        login_message = "Inicio de sesión inicial con contraseña de un solo uso detectado. Debe cambiar su contraseña inmediatamente en /api/v1/auth/change-password."

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        user_id=str(user.id),
        username=user.username,
        must_change_password=user.must_change_password,
        message=login_message
    )


@router.post(
    "/change-password",
    response_model=ChangePasswordResponse,
    summary="Cambiar contraseña / Asignar contraseña permanente tras primer login",
    description="""
Permite a cualquier usuario autenticado (incluyendo aquellos con `must_change_password: true` en su primer login) 
configurar o actualizar su contraseña permanente.

### 📋 Requisitos de Seguridad:
- **Política Estricta:** Mínimo 10 caracteres, al menos un número (0-9) y al menos un símbolo especial.
- **Validación de Cambio:** La nueva contraseña debe ser distinta a la contraseña temporal / actual.
- **Desbloqueo de Cuenta:** Apaga el flag `must_change_password = False` en MongoDB y emite un nuevo token JWT sin restricciones.
""",
    responses={
        200: {
            "description": "Contraseña actualizada exitosamente. Retorna el nuevo JWT definitivo y actualiza la cookie HttpOnly.",
            "model": ChangePasswordResponse
        },
        400: {
            "description": "Contraseña no cumple la política estricta, la contraseña actual es incorrecta, o la nueva contraseña es idéntica a la actual.",
            "content": {
                "application/json": {
                    "example": {"detail": "La contraseña no cumple con la política de seguridad: debe tener al menos 10 caracteres"}
                }
            }
        },
        401: {
            "description": "No autenticado o token revocado / expirado.",
            "content": {
                "application/json": {
                    "example": {"detail": "No autenticado"}
                }
            }
        }
    }
)
async def change_password(
    request: ChangePasswordRequest,
    response: Response,
    current_user: User = Depends(get_current_user)
):
    # 1. Validar política estricta de complejidad
    validate_password_policy(request.new_password)

    # 2. Si se proporciona current_password, verificar coincidencia
    if request.current_password:
        if not current_user.hashed_password or not verify_password(request.current_password, current_user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="La contraseña actual proporcionada es incorrecta"
            )

    # 3. Validar que la nueva contraseña sea diferente a la actual
    if current_user.hashed_password and verify_password(request.new_password, current_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La nueva contraseña debe ser diferente a la contraseña actual"
        )

    # 4. Hashear y persistir nueva contraseña permanente
    current_user.hashed_password = get_password_hash(request.new_password)
    current_user.must_change_password = False
    current_user.updated_at = datetime.now(timezone.utc)
    await current_user.save()

    # 5. Emitir nuevo access_token definitivo
    new_access_token = create_access_token(
        subject=str(current_user.id),
        user_data={
            "username": current_user.username,
            "role": current_user.role,
            "is_superuser": current_user.is_superuser,
            "must_change_password": False
        }
    )

    # Inyectar cookie HttpOnly actualizada
    response.set_cookie(
        key="access_token",
        value=f"Bearer {new_access_token}",
        httponly=True,
        samesite="lax",
        secure=settings.COOKIE_SECURE,
        path="/",
        max_age=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )

    return ChangePasswordResponse(
        status="ok",
        message=f"Contraseña actualizada exitosamente para el usuario '{current_user.username}'.",
        user_id=str(current_user.id),
        username=current_user.username,
        access_token=new_access_token,
        token_type="bearer"
    )


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    bearer_token: Optional[str] = Depends(oauth2_scheme),
    current_user: User = Depends(get_current_user)
):
    """
    Cierra la sesión del usuario revocando el token JWT en Redis mediante lista negra
    y eliminando la cookie HttpOnly en el cliente.
    """
    token = bearer_token
    if not token and request.cookies:
        cookie_token = request.cookies.get("access_token")
        if cookie_token:
            token = cookie_token.replace("Bearer ", "").strip()

    if token:
        ttl_seconds = settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
        try:
            await redis_client.setex(f"tb_revoked_token:{token}", ttl_seconds, "revoked")
        except Exception:
            pass

    response.delete_cookie(key="access_token", path="/")

    return {
        "status": "ok",
        "message": f"Sesión cerrada exitosamente para el usuario {current_user.username}"
    }


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    """
    Retorna el perfil del usuario autenticado actualmente en MongoDB.
    """
    return _to_user_response(current_user)


@router.get("/me/tenants", response_model=List[UserTenantAccessResponse])
async def get_my_accessible_tenants(
    current_user: User = Depends(get_current_user)
) -> List[UserTenantAccessResponse]:
    """
    Retorna la lista de tenants accesibles para el usuario autenticado junto con su
    matriz de permisos efectivos (telemetry, devices, scheduler, etc.) resueltos con Casbin.
    - Superadministradores: obtienen todos los tenants registrados con acceso completo (*).
    - Usuarios estándar: evalúa políticas directas (p) y roles por tenant (g).
    """
    from core.models.tb_tenant import TBTenant
    from core.models.tb_server import TBServer
    from core.casbin_enforcer import get_casbin_enforcer

    servers = await TBServer.find_all().to_list()
    server_map: dict = {str(s.id): s for s in servers}

    tenants = await TBTenant.find_all().to_list()
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]

    resources = ["telemetry", "devices", "scheduler"]
    result: List[UserTenantAccessResponse] = []

    if is_admin:
        for t in tenants:
            t_id = str(t.id)
            srv_id = t.get_server_id_str()
            srv = server_map.get(srv_id) if srv_id else None
            result.append(
                UserTenantAccessResponse(
                    id=t_id,
                    name=t.name,
                    server_id=srv_id,
                    server_name=srv.name if srv else None,
                    server_base_url=srv.base_url if srv else None,
                    is_active=t.is_active,
                    role="superadmin",
                    permissions={
                        res: TenantPermissionsMap(canRead=True, canWrite=True, canDelete=True)
                        for res in resources
                    }
                )
            )
        return result

    # Non-superadmin: evaluate Casbin RBAC for current_user
    user_id_str = str(current_user.id)
    username_str = current_user.username or ""
    enforcer = get_casbin_enforcer()

    # Pre-fetch user policies and grouping
    all_policies = enforcer.get_policy()
    user_policies = [
        p for p in all_policies
        if len(p) >= 4 and p[0] in [user_id_str, username_str, current_user.role]
    ]

    for t in tenants:
        t_id = str(t.id)
        t_dom = f"tenant:{t_id}"
        srv_id = t.get_server_id_str()
        srv = server_map.get(srv_id) if srv_id else None

        is_owner = t.user_id in [user_id_str, current_user.id]

        # Domain roles
        domain_roles = await enforcer.get_roles_for_user_in_domain(user_id_str, t_dom)
        if not domain_roles and username_str:
            domain_roles = await enforcer.get_roles_for_user_in_domain(username_str, t_dom)
        global_roles = await enforcer.get_roles_for_user_in_domain(user_id_str, "*")

        assigned_role: Optional[str] = None
        if domain_roles:
            assigned_role = domain_roles[0]
        elif global_roles:
            assigned_role = global_roles[0]

        perms_map: dict = {}
        has_any_perm = is_owner

        for res in resources:
            can_read = is_owner or enforcer.enforce(user_id_str, t_dom, res, "read")
            can_write = is_owner or enforcer.enforce(user_id_str, t_dom, res, "write")
            can_delete = is_owner or enforcer.enforce(user_id_str, t_dom, res, "delete")

            if not can_read and username_str:
                can_read = enforcer.enforce(username_str, t_dom, res, "read")
            if not can_write and username_str:
                can_write = enforcer.enforce(username_str, t_dom, res, "write")
            if not can_delete and username_str:
                can_delete = enforcer.enforce(username_str, t_dom, res, "delete")

            # Fallback direct inspection if wildcard or role match
            if not (can_read and can_write and can_delete):
                for p in user_policies:
                    p_dom = p[1]
                    p_obj = p[2]
                    p_act = p[3]
                    if p_dom in ["*", t_dom, t_id] and p_obj in ["*", res]:
                        if p_act in ["*", "read"]:
                            can_read = True
                        if p_act in ["*", "write"]:
                            can_write = True
                        if p_act in ["*", "delete"]:
                            can_delete = True

            if can_read or can_write or can_delete:
                has_any_perm = True

            perms_map[res] = TenantPermissionsMap(
                canRead=can_read,
                canWrite=can_write,
                canDelete=can_delete
            )

        if has_any_perm:
            result.append(
                UserTenantAccessResponse(
                    id=t_id,
                    name=t.name,
                    server_id=srv_id,
                    server_name=srv.name if srv else None,
                    server_base_url=srv.base_url if srv else None,
                    is_active=t.is_active,
                    role=assigned_role or ("owner" if is_owner else "custom"),
                    permissions=perms_map
                )
            )

    return result


@router.post("/token")
async def get_tb_token(
    request: LoginRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Endpoint para autenticar y cachear tokens de ThingsBoard asociado al usuario autenticado.
    """
    cache_key_token = f"tb_token:{current_user.id}:{request.server_url}:{request.username}"
    cache_key_refresh = f"tb_refresh:{current_user.id}:{request.server_url}:{request.username}"

    # Buscar en caché
    cached_token = await redis_client.get(cache_key_token)
    cached_refresh = await redis_client.get(cache_key_refresh)

    if cached_token and cached_refresh:
        return {
            "source": "redis_cache",
            "token": cached_token,
            "refreshToken": cached_refresh
        }

    # Si no hay caché, autenticar contra ThingsBoard dinámicamente
    tb = ThingsBoardClient(base_url=request.server_url)
    auth_data = await tb.login(request.username, request.password)

    if not auth_data:
        raise HTTPException(status_code=401, detail="Credenciales inválidas en ThingsBoard")

    # Guardar en Redis. Token: 2 horas. Refresh Token: 7 días
    await redis_client.setex(cache_key_token, 7200, auth_data["token"])
    await redis_client.setex(cache_key_refresh, 604800, auth_data["refreshToken"])

    return {
        "source": "thingsboard",
        "token": auth_data["token"],
        "refreshToken": auth_data["refreshToken"]
    }
