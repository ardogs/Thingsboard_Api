from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from beanie import PydanticObjectId

from core.models.user import User
from core.security import get_password_hash, validate_password_policy
from core.casbin_enforcer import get_casbin_enforcer
from core.pagination import PaginationMetadata, PaginatedResponse, build_pagination_metadata
from api.deps import CasbinAuth

router = APIRouter()


# ==========================================
# DTOs / Schemas de Usuarios
# ==========================================

class UserCreateRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="Nombre de usuario único")
    email: Optional[str] = Field(default=None, description="Correo electrónico del usuario")
    password: str = Field(..., description="Contraseña inicial de un solo uso asignada al usuario")
    role: str = Field(default="user", description="Rol o categoría descriptiva (superadmin, admin, operator, viewer, user)")
    is_active: bool = Field(default=True, description="Estado de activación de la cuenta")
    is_superuser: bool = Field(default=False, description="Flag de superadministrador con acceso total")
    must_change_password: bool = Field(default=True, description="Indica si el usuario debe cambiar su contraseña obligatoriamente en su primer login")


class UserUpdateRequest(BaseModel):
    username: Optional[str] = Field(default=None, min_length=3, max_length=50)
    email: Optional[str] = None
    password: Optional[str] = Field(default=None, min_length=6, description="Nueva contraseña (opcional)")
    role: Optional[str] = None
    is_active: Optional[bool] = None
    is_superuser: Optional[bool] = None
    must_change_password: Optional[bool] = Field(default=None, description="Modificar flag de cambio obligatorio de contraseña")


class UserResponse(BaseModel):
    id: str = Field(..., description="Identificador único del usuario (MongoDB ObjectID)")
    username: str = Field(..., description="Nombre de usuario")
    email: Optional[str] = Field(default=None, description="Correo electrónico del usuario")
    role: str = Field(..., description="Rol descriptivo asignado")
    is_active: bool = Field(..., description="Indica si la cuenta se encuentra activa")
    is_superuser: bool = Field(..., description="Indica si posee privilegios globales de superadministrador")
    must_change_password: bool = Field(default=False, description="Indica si el usuario debe cambiar su contraseña en el siguiente inicio de sesión")
    created_at: datetime = Field(..., description="Fecha y hora de creación de la cuenta en formato UTC")
    updated_at: datetime = Field(..., description="Fecha y hora de la última modificación en formato UTC")


class PaginatedUserResponse(PaginatedResponse[UserResponse]):
    """
    Contenedor de respuesta paginada específico para el catálogo de usuarios.
    """
    pass


def _to_user_response(user: User) -> UserResponse:
    return UserResponse(
        id=str(user.id),
        username=user.username,
        email=user.email,
        role=user.role,
        is_active=user.is_active,
        is_superuser=user.is_superuser,
        must_change_password=user.must_change_password,
        created_at=user.created_at,
        updated_at=user.updated_at
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
    Crea un nuevo usuario en la base de datos de MongoDB con una contraseña de un solo uso.
    El usuario deberá autenticarse y asignar su nueva contraseña definitiva en su primer inicio de sesión.
    """
    # 1. Validar política de contraseñas estricta para la contraseña inicial
    validate_password_policy(request.password)

    # 2. Verificar si el username ya está en uso
    existing_username = await User.find_one(User.username == request.username)
    if existing_username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"El nombre de usuario '{request.username}' ya se encuentra registrado"
        )

    # 3. Verificar si el email ya está en uso
    if request.email:
        existing_email = await User.find_one(User.email == request.email)
        if existing_email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"El correo electrónico '{request.email}' ya se encuentra registrado"
            )

    # 4. Hashear la contraseña de un solo uso
    hashed_password = get_password_hash(request.password)

    new_user = User(
        username=request.username,
        email=request.email,
        hashed_password=hashed_password,
        role=request.role,
        is_active=request.is_active,
        is_superuser=request.is_superuser,
        must_change_password=request.must_change_password
    )
    await new_user.insert()

    # Si es superuser, registrar rol en Casbin automáticamente
    if new_user.is_superuser:
        try:
            enforcer = get_casbin_enforcer()
            await enforcer.add_role_for_user_in_domain(str(new_user.id), "superadmin", "*")
        except Exception:
            pass

    return _to_user_response(new_user)


@router.get(
    "",
    response_model=PaginatedUserResponse,
    status_code=status.HTTP_200_OK,
    summary="Listar usuarios con paginación avanzada",
    description="""
    Recupera el listado paginado de usuarios registrados en el sistema con soporte para filtros por rol y estado.

    ### 📌 Características de Paginación:
    - **Paginación base 1 (`page`):** La primera página es la `1` (`page=1`).
    - **Control de tamaño (`page_size`):** Cantidad de usuarios por página (por defecto `20`, mín. `1`, máx. `100`).
    - **Metadatos agrupados en `pagination`:**
      - `total`: Conteo total de usuarios que cumplen con los filtros.
      - `page`: Número de página actual consultada.
      - `page_size`: Tamaño de página configurado.
      - `total_pages`: Total de páginas calculadas.
      - `has_next`: `true` si hay páginas posteriores disponibles.
      - `has_prev`: `true` si hay páginas previas disponibles.

    ### 🔒 Seguridad y Permisos:
    - Requiere token JWT de autenticación en cabecera `Authorization: Bearer <token>`.
    - Requiere permiso Casbin: recurso `users`, acción `read`.
    """,
    responses={
        200: {
            "description": "Usuarios obtenidos exitosamente con metadatos de paginación estructurados.",
            "content": {
                "application/json": {
                    "example": {
                        "items": [
                            {
                                "id": "66d5b0a1f2e8c3b4a5d6e7f8",
                                "username": "admin_central",
                                "email": "admin@empresa.com",
                                "role": "admin",
                                "is_active": True,
                                "is_superuser": False,
                                "must_change_password": False,
                                "created_at": "2026-08-15T12:00:00Z",
                                "updated_at": "2026-08-15T12:00:00Z"
                            }
                        ],
                        "pagination": {
                            "total": 45,
                            "page": 1,
                            "page_size": 20,
                            "total_pages": 3,
                            "has_next": True,
                            "has_prev": False
                        }
                    }
                }
            }
        },
        401: {
            "description": "No autenticado, token JWT ausente, expirado o revocado."
        },
        403: {
            "description": "Acceso denegado. El usuario no posee permisos suficientes en el IAM (Casbin)."
        }
    }
)
async def list_users(
    page: int = Query(
        default=1,
        ge=1,
        description="Número de la página a consultar (1-indexed, comienza en 1)",
        examples=[1]
    ),
    page_size: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Cantidad máxima de registros a retornar por página (mínimo 1, máximo 100)",
        examples=[20]
    ),
    role: Optional[str] = Query(
        default=None,
        description="Filtrar usuarios por rol asignado (ej: superadmin, admin, operator, viewer, user)",
        examples=["admin"]
    ),
    is_active: Optional[bool] = Query(
        default=None,
        description="Filtrar usuarios por estado de la cuenta (true: activos, false: inactivos)"
    ),
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

    skip = (page - 1) * page_size

    if query:
        find_query = User.find(query)
    else:
        find_query = User.find_all()

    total = await find_query.count()
    users = await find_query.skip(skip).limit(page_size).to_list()

    items = [_to_user_response(u) for u in users]
    pagination = build_pagination_metadata(total=total, page=page, page_size=page_size)

    return PaginatedUserResponse(
        items=items,
        pagination=pagination
    )


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
        validate_password_policy(request.password)
        user.hashed_password = get_password_hash(request.password)

    if request.must_change_password is not None:
        user.must_change_password = request.must_change_password

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

    # Proteger contra la eliminación de la propia cuenta
    if str(user.id) == str(current_user.id) or user.username == current_user.username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No puedes eliminar tu propia cuenta de usuario"
        )

    # Proteger contra la eliminación del último superadmin activo
    if (user.is_superuser or user.role == "superadmin") and user.is_active:
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
