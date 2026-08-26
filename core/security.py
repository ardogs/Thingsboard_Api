import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union
import bcrypt
from jose import jwt, JWTError

from fastapi import HTTPException, status

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
    Crea un token JWT de acceso inyectando subject (user_id), jti único, iat, exp y claims adicionales.
    """
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode = {
        "sub": str(subject),
        "user_id": str(subject),
        "jti": str(uuid.uuid4()),
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


def create_password_setup_token(
    subject: Union[str, Any],
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Crea un token JWT de un solo uso para la configuración inicial o restablecimiento de contraseña.
    Inyecta type="password_setup", sub=subject, jti único y exp por defecto a 24 horas.
    """
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(hours=24)

    to_encode = {
        "sub": str(subject),
        "user_id": str(subject),
        "type": "password_setup",
        "jti": str(uuid.uuid4()),
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp())
    }

    encoded_jwt = jwt.encode(
        to_encode,
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM
    )
    return encoded_jwt


def validate_password_policy(password: str) -> None:
    """
    Valida que la contraseña cumpla con la política de seguridad estricta:
    - Mínimo 10 caracteres
    - Al menos un número (0-9)
    - Al menos un símbolo o carácter especial
    """
    if len(password) < 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La contraseña no cumple con la política de seguridad: debe tener al menos 10 caracteres"
        )
    if not any(c.isdigit() for c in password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La contraseña no cumple con la política de seguridad: debe incluir al menos un número (0-9)"
        )
    if not any(not c.isalnum() for c in password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La contraseña no cumple con la política de seguridad: debe incluir al menos un símbolo o carácter especial"
        )


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

