from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, status
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

router = APIRouter()

MAX_FAILED_LOGIN_ATTEMPTS = 5
LOGIN_LOCKOUT_SECONDS = 15 * 60  # 15 minutos (900 segundos)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    username: str


class UserResponse(BaseModel):
    id: str
    username: str
    email: Optional[str] = None
    role: str = "user"
    is_active: bool = True
    is_superuser: bool = False
    created_at: datetime
    updated_at: datetime


class LoginRequest(BaseModel):
    server_url: str = "https://thingsboard.cloud"
    username: str
    password: str


class SetPasswordRequest(BaseModel):
    setup_token: str = Field(..., description="Token JWT de un solo uso recibido durante la creación de la cuenta")
    new_password: str = Field(..., description="Nueva contraseña que cumple con la política de seguridad estricta")


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


@router.post("/login", response_model=TokenResponse)
async def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends()
):
    """
    Endpoint de inicio de sesión estándar OAuth2 / JWT con protección contra ataques de fuerza bruta.
    Bloquea la IP/usuario con HTTP 429 tras 5 intentos fallidos en una ventana de 15 minutos.
    """
    client_ip = _get_client_ip(request)
    rate_limit_key = f"tb_auth_failed:{client_ip}:{form_data.username}"

    # 1. Verificar si la IP o cuenta ha superado el umbral de intentos fallidos
    try:
        failed_attempts = await redis_client.get(rate_limit_key)
        if failed_attempts and int(failed_attempts) >= MAX_FAILED_LOGIN_ATTEMPTS:
            ttl = await redis_client.ttl(rate_limit_key)
            ttl = max(ttl, 1)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Demasiados intentos fallidos de inicio de sesión. Acceso bloqueado temporalmente. Intente nuevamente en {ttl} segundos."
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
            "is_superuser": user.is_superuser
        }
    )

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        user_id=str(user.id),
        username=user.username
    )


@router.post("/set-password")
async def set_password(request: SetPasswordRequest):
    """
    Endpoint para establecer o configurar la contraseña inicial mediante un setup_token de un solo uso.
    Valida la política de complejidad, hashea la contraseña y marca el token como consumido en Redis.
    """
    # 1. Validar política estricta de complejidad
    validate_password_policy(request.new_password)

    # 2. Decodificar y validar criptográficamente el setup_token
    try:
        payload = decode_access_token(request.setup_token)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token de configuración inválido o expirado"
        )

    token_type = payload.get("type")
    if token_type != "password_setup":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El token proporcionado no es un token válido de configuración de contraseña"
        )

    jti = payload.get("jti")
    user_id = payload.get("sub") or payload.get("user_id")

    if not user_id or not jti:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payload del token de configuración incompleto o inválido"
        )

    # 3. Garantizar un solo uso mediante verificación en Redis
    used_key = f"tb_used_setup_token:{jti}"
    try:
        is_used = await redis_client.get(used_key)
        if is_used:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Este token de configuración ya ha sido utilizado previamente"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"[SetPassword] No se pudo verificar reuso de token en Redis: {e}")

    # 4. Buscar usuario en MongoDB
    user: Optional[User] = None
    try:
        user = await User.get(PydanticObjectId(user_id))
    except Exception:
        pass

    if not user:
        user = await User.find_one(User.username == user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado"
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La cuenta de usuario se encuentra inactiva o deshabilitada"
        )

    # 5. Hashear con bcrypt y persistir contraseña
    user.hashed_password = get_password_hash(request.new_password)
    user.updated_at = datetime.now(timezone.utc)
    await user.save()

    # 6. Marcar el jti como consumido en Redis con el TTL remanente del JWT
    exp_ts = payload.get("exp", int(datetime.now(timezone.utc).timestamp()) + 86400)
    current_ts = int(datetime.now(timezone.utc).timestamp())
    ttl = max(exp_ts - current_ts, 3600)
    try:
        await redis_client.setex(used_key, ttl, "used")
    except Exception as e:
        logger.warning(f"[SetPassword] No se pudo registrar token usado en Redis: {e}")

    return {
        "status": "ok",
        "message": f"Contraseña configurada exitosamente para el usuario '{user.username}'",
        "user_id": str(user.id)
    }


@router.post("/logout")
async def logout(
    token: str = Depends(oauth2_scheme),
    current_user: User = Depends(get_current_user)
):
    """
    Cierra la sesión del usuario revocando el token JWT en Redis mediante lista negra.
    """
    ttl_seconds = settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    try:
        await redis_client.setex(f"tb_revoked_token:{token}", ttl_seconds, "revoked")
    except Exception:
        pass

    return {
        "status": "ok",
        "message": f"Sesión cerrada exitosamente para el usuario {current_user.username}"
    }


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    """
    Retorna el perfil del usuario autenticado actualmente en MongoDB.
    """
    return UserResponse(
        id=str(current_user.id),
        username=current_user.username,
        email=current_user.email,
        role=current_user.role,
        is_active=current_user.is_active,
        is_superuser=current_user.is_superuser,
        created_at=current_user.created_at,
        updated_at=current_user.updated_at
    )


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
