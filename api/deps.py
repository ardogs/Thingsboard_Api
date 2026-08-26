from typing import Optional
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError
from beanie import PydanticObjectId

from core.config import settings
from core.models.user import User
from core.security import decode_access_token
from core.redis_client import redis_client
from core.casbin_enforcer import get_casbin_enforcer
from core.logger import logger

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="/api/v1/auth/login",
    auto_error=True
)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    """
    Dependencia de FastAPI para extraer y validar el JWT del header Authorization.
    Verifica firma, expiración, lista negra en Redis y resuelve el documento User real en MongoDB.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciales de autenticación inválidas o expiradas",
        headers={"WWW-Authenticate": "Bearer"},
    )

    # 1. Verificar si el token ha sido revocado en Redis (Logout)
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
    except Exception:
        # Si Redis no responde temporalmente, se procede con la validación criptográfica del JWT
        pass

    # 2. Decodificar y validar el JWT
    try:
        payload = decode_access_token(token)
        user_id: Optional[str] = payload.get("user_id") or payload.get("sub")
        if not user_id:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    # 3. Consultar usuario real en MongoDB
    user: Optional[User] = None
    try:
        obj_id = PydanticObjectId(user_id)
        user = await User.get(obj_id)
    except Exception:
        pass

    if user is None:
        user = await User.find_one(User.username == user_id)

    if user is None or not user.is_active:
        raise credentials_exception

    return user


async def get_current_active_superuser(current_user: User = Depends(get_current_user)) -> User:
    """
    Dependencia que asegura que el usuario autenticado tiene privilegios de superadministrador.
    """
    if not current_user.is_superuser and current_user.role not in ["superadmin", "admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Se requieren privilegios de superadministrador para realizar esta acción"
        )
    return current_user


class CasbinAuth:
    """
    Dependencia de autorización basada en RBAC con Dominios (Tenants / Servidores) mediante PyCasbin.
    Evalúa enforcer.enforce(sub, dom, obj, act) dinámicamente según el contexto de la petición.
    Implementa un patrón de URN simplificado mediante prefijos de dominio ('tenant:' o 'server:').
    """
    def __init__(
        self,
        resource: str,
        action: str,
        default_domain: Optional[str] = None,
        domain_type: str = "tenant"
    ):
        if domain_type not in ("tenant", "server"):
            raise ValueError(f"domain_type inválido: '{domain_type}'. Debe ser 'tenant' o 'server'.")
        self.resource = resource
        self.action = action
        self.default_domain = default_domain
        self.domain_type = domain_type

    async def __call__(
        self,
        request: Request,
        current_user: User = Depends(get_current_user)
    ) -> User:
        # 1. Superadministradores tienen acceso global concedido
        if current_user.is_superuser or current_user.role == "superadmin":
            return current_user

        # 2. Extraer el ID de dominio dinámicamente según domain_type
        extracted_id: Optional[str] = None

        # a) Desde parámetros de ruta
        if request.path_params:
            if self.domain_type == "server":
                extracted_id = (
                    request.path_params.get("server_id")
                    or request.path_params.get("domain")
                    or request.path_params.get("tenant_id")
                )
            else:
                extracted_id = (
                    request.path_params.get("tenant_id")
                    or request.path_params.get("domain")
                    or request.path_params.get("server_id")
                )

        # b) Desde parámetros de consulta (query)
        if not extracted_id and request.query_params:
            if self.domain_type == "server":
                extracted_id = (
                    request.query_params.get("server_id")
                    or request.query_params.get("domain")
                    or request.query_params.get("tenant_id")
                )
            else:
                extracted_id = (
                    request.query_params.get("tenant_id")
                    or request.query_params.get("domain")
                    or request.query_params.get("server_id")
                )

        # c) Desde cabecera HTTP personalizada
        if not extracted_id:
            if self.domain_type == "server":
                extracted_id = (
                    request.headers.get("X-Server-Id")
                    or request.headers.get("x-server-id")
                    or request.headers.get("X-Domain-Id")
                    or request.headers.get("x-domain-id")
                )
            else:
                extracted_id = (
                    request.headers.get("X-Tenant-Id")
                    or request.headers.get("x-tenant-id")
                    or request.headers.get("X-Domain-Id")
                    or request.headers.get("x-domain-id")
                )

        # d) Fallback a dominio por defecto o wildcard
        if not extracted_id:
            extracted_id = self.default_domain or "*"

        # 3. Construir domain_urn con el prefijo simplificado URN
        if extracted_id == "*":
            domain_urn = "*"
        elif extracted_id.startswith(f"{self.domain_type}:"):
            domain_urn = extracted_id
        elif extracted_id.startswith(("tenant:", "server:")):
            domain_urn = extracted_id
        else:
            domain_urn = f"{self.domain_type}:{extracted_id}"

        # 4. Evaluar con PyCasbin AsyncEnforcer
        try:
            enforcer = get_casbin_enforcer()
            user_id_str = str(current_user.id)
            
            # Evaluar por user_id
            is_allowed = enforcer.enforce(user_id_str, domain_urn, self.resource, self.action)
            
            # Fallback evaluar por username
            if not is_allowed and current_user.username:
                is_allowed = enforcer.enforce(current_user.username, domain_urn, self.resource, self.action)
        except Exception as e:
            logger.error(f"[CasbinAuth] Error al evaluar políticas Casbin: {e}")
            is_allowed = False

        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permisos insuficientes para el recurso '{self.resource}' con acción '{self.action}' en el dominio '{domain_urn}'"
            )

        return current_user
