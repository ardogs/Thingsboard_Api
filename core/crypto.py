import base64
import hashlib
from typing import Optional
from cryptography.fernet import Fernet
from core.config import settings
from core.logger import logger


def _get_fernet_instance(raw_key: str) -> Fernet:
    """
    Inicializa una instancia de Fernet de forma segura y resiliente.
    Si raw_key es una clave válida de 32 bytes base64 url-safe (44 caracteres), la utiliza directamente.
    Si raw_key es una frase de paso o string arbitrario, deriva una clave de 32 bytes con SHA-256
    y la codifica en url-safe base64 para garantizar interoperabilidad robusta.
    """
    try:
        key_bytes = raw_key.encode("utf-8") if isinstance(raw_key, str) else raw_key
        return Fernet(key_bytes)
    except Exception:
        # Derivación determinística de 32 bytes vía SHA-256
        derived_key = base64.urlsafe_b64encode(hashlib.sha256(raw_key.encode("utf-8")).digest())
        return Fernet(derived_key)


_fernet: Fernet = _get_fernet_instance(settings.ENCRYPTION_KEY)


def encrypt_data(plain_text: Optional[str]) -> Optional[str]:
    """
    Cifra una cadena de texto plano utilizando Fernet (cifrado simétrico autenticado AES-128-CBC + HMAC-SHA256).
    Retorna el texto cifrado codificado en base64 url-safe o None si la entrada es None.
    """
    if plain_text is None:
        return None
    if not isinstance(plain_text, str):
        plain_text = str(plain_text)
    if plain_text == "":
        return ""
    try:
        encrypted_bytes = _fernet.encrypt(plain_text.encode("utf-8"))
        return encrypted_bytes.decode("utf-8")
    except Exception as e:
        logger.error(f"[Crypto] Error al cifrar datos sensibles: {e}")
        raise ValueError(f"Fallo en el cifrado simétrico: {e}") from e


def decrypt_data(cipher_text: Optional[str]) -> Optional[str]:
    """
    Descifra una cadena de texto cifrada con Fernet y retorna el texto plano en memoria RAM.
    Retorna None si cipher_text es None.
    """
    if cipher_text is None:
        return None
    if not isinstance(cipher_text, str):
        cipher_text = str(cipher_text)
    if cipher_text == "":
        return ""
    try:
        decrypted_bytes = _fernet.decrypt(cipher_text.encode("utf-8"))
        return decrypted_bytes.decode("utf-8")
    except Exception as e:
        logger.error(f"[Crypto] Error al descifrar datos sensibles: {e}")
        raise ValueError(f"Fallo en el descifrado simétrico: {e}") from e
