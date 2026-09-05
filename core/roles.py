"""
Módulo de Definición y Catálogo Maestro de Roles (IAM)
======================================================
Centraliza la definición de roles del sistema y de dominios (tenants/servers),
sus nombres legibles para la interfaz gráfica, descripciones y permisos asociados.
"""

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class RoleScope(str, Enum):
    """
    Ámbito o contexto de aplicación del rol.
    - SYSTEM: Rol a nivel global o de plataforma (ej: superadmin, admin, user).
    - TENANT: Rol específico para la administración y operación de un Tenant (ej: tenant_admin, operator, viewer).
    - SERVER: Rol aplicable a un servidor ThingsBoard específico.
    """
    SYSTEM = "system"
    TENANT = "tenant"
    SERVER = "server"


class RolePermission(BaseModel):
    """
    Permiso individual asignado por defecto a un rol.
    """
    resource: str = Field(
        ...,
        description="Recurso protegido en el Gateway (ej: telemetry, devices, servers, users, iam, *)",
        examples=["telemetry"]
    )
    action: str = Field(
        ...,
        description="Acción autorizada sobre el recurso (ej: read, write, delete, *)",
        examples=["read"]
    )


class RoleDetailResponse(BaseModel):
    """
    Detalle completo de un rol registrado en la plataforma.
    """
    name: str = Field(
        ...,
        description="Identificador técnico del rol (utilizado en endpoints de autenticación y Casbin)",
        examples=["tenant_admin"]
    )
    display_name: str = Field(
        ...,
        description="Nombre amigable y legible para interfaces de usuario (UI/Frontend)",
        examples=["Administrador de Tenant"]
    )
    description: str = Field(
        ...,
        description="Descripción detallada de las capacidades y responsabilidades del rol",
        examples=["Administración completa sobre un Tenant específico (dispositivos, telemetría, automatizaciones y credenciales)."]
    )
    scope: RoleScope = Field(
        ...,
        description="Ámbito de aplicación del rol ('system', 'tenant' o 'server')",
        examples=[RoleScope.TENANT]
    )
    is_system_role: bool = Field(
        ...,
        description="Indica si es un rol global de plataforma o un rol multi-tenant/dominio",
        examples=[False]
    )
    permissions: List[RolePermission] = Field(
        default_factory=list,
        description="Lista de permisos y acciones autorizadas por defecto para este rol"
    )


# ==========================================
# Catálogo Maestro Centralizado de Roles
# ==========================================

SYSTEM_ROLES_CATALOG: List[RoleDetailResponse] = [
    RoleDetailResponse(
        name="superadmin",
        display_name="Superadministrador",
        description="Acceso global total e irrestricto a toda la infraestructura, servidores, tenants, usuarios, IAM y configuraciones.",
        scope=RoleScope.SYSTEM,
        is_system_role=True,
        permissions=[
            RolePermission(resource="*", action="*")
        ]
    ),
    RoleDetailResponse(
        name="admin",
        display_name="Administrador de Sistema / Servidor",
        description="Administración de servidores ThingsBoard, creación de tenants y supervisión de infraestructura.",
        scope=RoleScope.SYSTEM,
        is_system_role=True,
        permissions=[
            RolePermission(resource="servers", action="*"),
            RolePermission(resource="tenants", action="*"),
            RolePermission(resource="telemetry", action="*"),
            RolePermission(resource="devices", action="*"),
        ]
    ),
    RoleDetailResponse(
        name="tenant_admin",
        display_name="Administrador de Tenant",
        description="Administración y control operativo completo sobre un Tenant específico (dispositivos, telemetría, automatizaciones y credenciales).",
        scope=RoleScope.TENANT,
        is_system_role=False,
        permissions=[
            RolePermission(resource="telemetry", action="*"),
            RolePermission(resource="devices", action="*"),
            RolePermission(resource="scheduler", action="*"),
        ]
    ),
    RoleDetailResponse(
        name="operator",
        display_name="Operador de Tenant",
        description="Operación y supervisión de datos de telemetría, consulta de dispositivos y ejecución de tareas programadas.",
        scope=RoleScope.TENANT,
        is_system_role=False,
        permissions=[
            RolePermission(resource="telemetry", action="read"),
            RolePermission(resource="devices", action="read"),
            RolePermission(resource="scheduler", action="read"),
        ]
    ),
    RoleDetailResponse(
        name="viewer",
        display_name="Visualizador de Tenant",
        description="Acceso exclusivo de solo lectura a métricas e históricos de telemetría.",
        scope=RoleScope.TENANT,
        is_system_role=False,
        permissions=[
            RolePermission(resource="telemetry", action="read")
        ]
    ),
    RoleDetailResponse(
        name="user",
        display_name="Usuario Estándar",
        description="Usuario base del sistema con permisos mínimos y acceso sujeto a asignación de dominios específicos.",
        scope=RoleScope.SYSTEM,
        is_system_role=True,
        permissions=[]
    )
]


def get_available_roles(scope: Optional[RoleScope | str] = None) -> List[RoleDetailResponse]:
    """
    Retorna los roles disponibles en la plataforma, opcionalmente filtrados por ámbito (system, tenant, server).
    """
    if not scope:
        return SYSTEM_ROLES_CATALOG

    scope_val = scope.value if isinstance(scope, RoleScope) else str(scope).lower()
    return [r for r in SYSTEM_ROLES_CATALOG if r.scope.value == scope_val]
