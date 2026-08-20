from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union
import bcrypt
from jose import jwt, JWTError

from core.config import settings


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifica una contraseña en texto plano contra su hash bcrypt.
    """
    if not plain_password or not hashed_password:
        return False
    try:
        return bcrypt.checkpw(
            plain_password.encode("utf-8"),
            hashed_password.encode("utf-8")
        )
    except Exception:
        return False


def get_password_hash(password: str) -> str:
    """
    Genera un hash bcrypt seguro para la contraseña proporcionada.
    """
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def create_access_token(
    subject: Union[str, Any],
    user_data: Optional[dict] = None,
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Crea un token JWT de acceso inyectando subject (user_id), iat, exp y claims adicionales.
    """
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode = {
        "sub": str(subject),
        "user_id": str(subject),
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp())
    }

    if user_data:
        to_encode.update(user_data)

    encoded_jwt = jwt.encode(
        to_encode,
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM
    )
    return encoded_jwt


def decode_access_token(token: str) -> dict:
    """
    Decodifica y valida un token JWT usando la clave secreta y algoritmo configurados.
    Lanza JWTError si el token es inválido o ha expirado.
    """
    return jwt.decode(
        token,
        settings.SECRET_KEY,
        algorithms=[settings.ALGORITHM]
    )
