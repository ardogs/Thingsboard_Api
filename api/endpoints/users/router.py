from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from beanie import PydanticObjectId

from core.models.user import User
from core.security import get_password_hash, create_password_setup_token
from core.casbin_enforcer import get_casbin_enforcer
from api.deps import CasbinAuth

router = APIRouter()


# ==========================================
# DTOs / Schemas de Usuarios
# ==========================================

class UserCreateRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="Nombre de usuario único")
    email: Optional[str] = Field(default=None, description="Correo electrónico del usuario")
    role: str = Field(default="user", description="Rol o categoría descriptiva (superadmin, admin, operator, viewer, user)")
    is_active: bool = Field(default=True, description="Estado de activación de la cuenta")
    is_superuser: bool = Field(default=False, description="Flag de superadministrador con acceso total")


class UserUpdateRequest(BaseModel):
    username: Optional[str] = Field(default=None, min_length=3, max_length=50)
    email: Optional[str] = None
    password: Optional[str] = Field(default=None, min_length=6, description="Nueva contraseña (opcional)")
    role: Optional[str] = None
    is_active: Optional[bool] = None
    is_superuser: Optional[bool] = None


class UserResponse(BaseModel):
    id: str
    username: str
    email: Optional[str] = None
    role: str
    is_active: bool
    is_superuser: bool
    created_at: datetime
    updated_at: datetime
    setup_token: Optional[str] = Field(default=None, description="Token JWT de un solo uso para establecer la contraseña inicial")


def _to_user_response(user: User, setup_token: Optional[str] = None) -> UserResponse:
    return UserResponse(
        id=str(user.id),
        username=user.username,
        email=user.email,
        role=user.role,
        is_active=user.is_active,
        is_superuser=user.is_superuser,
        created_at=user.created_at,
        updated_at=user.updated_at,
        setup_token=setup_token
    )


# ==========================================
# Endpoints CRUD de Usuarios
# ==========================================

@router.post("", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    request: UserCreateRequest,
    current_user: User = Depends(CasbinAuth(resource="users", action="write"))
):
    """
    Crea un nuevo usuario en la base de datos de MongoDB sin contraseña.
    Genera un token JWT de un solo uso (setup_token) con validez de 24 horas para que el usuario configure su contraseña.
    """
    # Verificar si el username ya está en uso
    existing_username = await User.find_one(User.username == request.username)
    if existing_username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"El nombre de usuario '{request.username}' ya se encuentra registrado"
        )

    # Verificar si el email ya está en uso
    if request.email:
        existing_email = await User.find_one(User.email == request.email)
        if existing_email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"El correo electrónico '{request.email}' ya se encuentra registrado"
            )

    new_user = User(
        username=request.username,
        email=request.email,
        hashed_password=None,
        role=request.role,
        is_active=request.is_active,
        is_superuser=request.is_superuser
    )
    await new_user.insert()

    # Generar setup_token de un solo uso (24h de validez)
    setup_token = create_password_setup_token(str(new_user.id))

    # Si es superuser, registrar rol en Casbin automáticamente
    if new_user.is_superuser:
        try:
            enforcer = get_casbin_enforcer()
            await enforcer.add_role_for_user_in_domain(str(new_user.id), "superadmin", "*")
        except Exception:
            pass

    return _to_user_response(new_user, setup_token=setup_token)


@router.get("", response_model=List[UserResponse])
async def list_users(
    skip: int = Query(default=0, ge=0, description="Número de registros a omitir"),
    limit: int = Query(default=50, ge=1, le=200, description="Cantidad máxima de registros a retornar"),
    role: Optional[str] = Query(default=None, description="Filtrar por rol"),
    is_active: Optional[bool] = Query(default=None, description="Filtrar por estado activo/inactivo"),
    current_user: User = Depends(CasbinAuth(resource="users", action="read"))
):
    """
    Lista usuarios registrados en MongoDB con soporte para paginación y filtros opcionales.
    """
    query = {}
    if role is not None:
        query["role"] = role
    if is_active is not None:
        query["is_active"] = is_active

    if query:
        users = await User.find(query).skip(skip).limit(limit).to_list()
    else:
        users = await User.find_all().skip(skip).limit(limit).to_list()

    return [_to_user_response(u) for u in users]


@router.get("/{user_id}", response_model=UserResponse)
async def get_user_by_id(
    user_id: str,
    current_user: User = Depends(CasbinAuth(resource="users", action="read"))
):
    """
    Obtiene el detalle de un usuario específico por su ID de MongoDB o por su username.
    """
    user: Optional[User] = None
    try:
        obj_id = PydanticObjectId(user_id)
        user = await User.get(obj_id)
    except Exception:
        pass

    if not user:
        user = await User.find_one(User.username == user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Usuario '{user_id}' no encontrado"
        )

    return _to_user_response(user)


@router.put("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str,
    request: UserUpdateRequest,
    current_user: User = Depends(CasbinAuth(resource="users", action="write"))
):
    """
    Actualiza la información de un usuario (email, contraseña, estado, rol, superuser).
    """
    user: Optional[User] = None
    try:
        obj_id = PydanticObjectId(user_id)
        user = await User.get(obj_id)
    except Exception:
        pass

    if not user:
        user = await User.find_one(User.username == user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Usuario '{user_id}' no encontrado"
        )

    # Validar unicidad de username si se modifica
    if request.username and request.username != user.username:
        existing = await User.find_one(User.username == request.username)
        if existing and existing.id != user.id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"El nombre de usuario '{request.username}' ya está en uso"
            )
        user.username = request.username

    # Validar unicidad de email si se modifica
    if request.email and request.email != user.email:
        existing_mail = await User.find_one(User.email == request.email)
        if existing_mail and existing_mail.id != user.id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"El correo electrónico '{request.email}' ya está en uso"
            )
        user.email = request.email

    if request.password:
        user.hashed_password = get_password_hash(request.password)

    if request.role is not None:
        user.role = request.role

    if request.is_active is not None:
        user.is_active = request.is_active

    if request.is_superuser is not None:
        user.is_superuser = request.is_superuser

    user.updated_at = datetime.now(timezone.utc)
    await user.save()

    return _to_user_response(user)


@router.delete("/{user_id}")
async def delete_user(
    user_id: str,
    current_user: User = Depends(CasbinAuth(resource="users", action="delete"))
):
    """
    Elimina un usuario de la base de datos de MongoDB.
    Protege contra la eliminación del último superadministrador activo del sistema.
    """
    user: Optional[User] = None
    try:
        obj_id = PydanticObjectId(user_id)
        user = await User.get(obj_id)
    except Exception:
        pass

    if not user:
        user = await User.find_one(User.username == user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Usuario '{user_id}' no encontrado"
        )

    # Proteger contra la eliminación del último superadmin
    if user.is_superuser or user.role == "superadmin":
        superuser_count = await User.find(User.is_superuser == True, User.is_active == True).count()
        if superuser_count <= 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No es posible eliminar al único superadministrador activo del sistema"
            )

    # Limpiar asignaciones de rol en Casbin para este usuario
    try:
        enforcer = get_casbin_enforcer()
        await enforcer.delete_roles_for_user(str(user.id))
        if user.username:
            await enforcer.delete_roles_for_user(user.username)
    except Exception:
        pass

    username = user.username
    await user.delete()

    return {
        "status": "ok",
        "message": f"Usuario '{username}' (ID: {user_id}) eliminado exitosamente"
    }
