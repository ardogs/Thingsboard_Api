from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError
from pydantic import BaseModel

from core.config import settings
from core.security import decode_access_token, get_password_hash
from core.redis_client import redis_client

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


class User(BaseModel):
    id: str
    username: str
    email: Optional[str] = None
    role: str = "user"
    is_active: bool = True


# Base de datos simulada en memoria con contraseñas hasheadas en bcrypt
DUMMY_USERS_DB = {
    "user_a": {
        "id": "user_a_001",
        "username": "user_a",
        "email": "user_a@tkmecloud.com",
        "hashed_password": get_password_hash("secret_a_123"),
        "role": "user",
        "is_active": True,
    },
    "user_b": {
        "id": "user_b_002",
        "username": "user_b",
        "email": "user_b@tkmecloud.com",
        "hashed_password": get_password_hash("secret_b_456"),
        "role": "user",
        "is_active": True,
    },
    "admin": {
        "id": "admin_001",
        "username": "admin",
        "email": "admin@tkmecloud.com",
        "hashed_password": get_password_hash("admin_pass_789"),
        "role": "admin",
        "is_active": True,
    },
}


async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    """
    Dependencia de FastAPI para extraer y validar el JWT del header Authorization.
    Verifica firma, expiración, lista negra en Redis y estado activo del usuario.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciales de autenticación inválidas o expiradas",
        headers={"WWW-Authenticate": "Bearer"},
    )

    # 1. Verificar si el token ha sido revocado en Redis
    try:
        is_revoked = await redis_client.get(f"tb_revoked_token:{token}")
        if is_revoked:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="El token de sesión ha sido revocado (logout realizado)",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except HTTPException:
        raise
    except Exception as e:
        # Si Redis no está disponible, registramos la advertencia pero permitimos continuar con validación JWT
        pass

    # 2. Decodificar y validar el JWT
    try:
        payload = decode_access_token(token)
        user_id: Optional[str] = payload.get("user_id") or payload.get("sub")
        if not user_id:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    # 3. Consultar usuario en el repositorio
    user_dict = next((u for u in DUMMY_USERS_DB.values() if u["id"] == user_id), None)
    if user_dict is None or not user_dict.get("is_active", True):
        raise credentials_exception

    return User(
        id=user_dict["id"],
        username=user_dict["username"],
        email=user_dict.get("email"),
        role=user_dict.get("role", "user"),
        is_active=user_dict.get("is_active", True)
    )
