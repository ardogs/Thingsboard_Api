from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Optional

from core.tb_client import ThingsBoardClient
from core.redis_client import redis_client
from core.config import settings
from core.security import verify_password, create_access_token
from api.deps import User, get_current_user, oauth2_scheme, DUMMY_USERS_DB

router = APIRouter()


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    username: str


class LoginRequest(BaseModel):
    server_url: str = "https://thingsboard.cloud"
    username: str
    password: str


@router.post("/login", response_model=TokenResponse)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Endpoint de inicio de sesión estándar OAuth2 / JWT.
    Valida credenciales contra el repositorio de usuarios y emite un token de acceso JWT.
    """
    user_dict = DUMMY_USERS_DB.get(form_data.username)
    if not user_dict or not verify_password(form_data.password, user_dict["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user_dict.get("is_active", True):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Usuario inactivo"
        )

    access_token = create_access_token(
        subject=user_dict["id"],
        user_data={
            "username": user_dict["username"],
            "role": user_dict["role"]
        }
    )

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        user_id=user_dict["id"],
        username=user_dict["username"]
    )


@router.post("/logout")
async def logout(
    token: str = Depends(oauth2_scheme),
    current_user: User = Depends(get_current_user)
):
    """
    Cierra la sesión del usuario revocando el token JWT en Redis.
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


@router.get("/me", response_model=User)
async def get_me(current_user: User = Depends(get_current_user)):
    """
    Retorna el perfil del usuario autenticado actualmente.
    """
    return current_user


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
