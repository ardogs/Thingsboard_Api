from enum import Enum
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from core.models.user import User
from core.casbin_enforcer import get_casbin_enforcer
from api.deps import CasbinAuth

router = APIRouter()


# ==========================================
# Enums y Funciones Auxiliares de URN
# ==========================================

class DomainType(str, Enum):
    TENANT = "tenant"
    SERVER = "server"


def _build_domain_urn(domain: str, domain_type: DomainType | str = DomainType.TENANT) -> str:
    """
    Construye el URN simplificado con prefijo de dominio ('tenant:' o 'server:').
    Preserva el comodín global '*' intacto y evita duplicación de prefijos.
    """
    if domain == "*":
        return "*"
    prefix = domain_type.value if isinstance(domain_type, DomainType) else str(domain_type)
    if domain.startswith(f"{prefix}:"):
        return domain
    if domain.startswith(("tenant:", "server:")):
        return domain
    return f"{prefix}:{domain}"


# ==========================================
# DTOs / Schemas de IAM y Casbin
# ==========================================

class RoleAssignRequest(BaseModel):
    user_id: str = Field(..., description="ID de MongoDB o username del usuario")
    role: str = Field(..., description="Nombre del rol (ej: tenant_admin, operator, viewer)")
    domain: str = Field(default="*", description="ID del Tenant, Servidor o dominio ('*' para global)")
    domain_type: DomainType = Field(default=DomainType.TENANT, description="Tipo de dominio ('tenant' o 'server')")


class RoleRevokeRequest(BaseModel):
    user_id: str = Field(..., description="ID de MongoDB o username del usuario")
    role: str = Field(..., description="Nombre del rol a revocar")
    domain: str = Field(default="*", description="ID del Tenant, Servidor o dominio ('*' para global)")
    domain_type: DomainType = Field(default=DomainType.TENANT, description="Tipo de dominio ('tenant' o 'server')")


class PolicyRuleRequest(BaseModel):
    sub: str = Field(..., description="Sujeto o Rol (ej: tenant_admin, operator, user_123)")
    dom: str = Field(default="*", description="Dominio o URN (ej: tenant:123, server:456, o '*' para todos)")
    domain_type: Optional[DomainType] = Field(default=None, description="Tipo de dominio opcional si 'dom' no contiene el prefijo URN")
    obj: str = Field(..., description="Recurso protegido (ej: telemetry, devices, servers, users, *)")
    act: str = Field(..., description="Acción permitida (ej: read, write, delete, *)")


class PolicyRuleResponse(BaseModel):
    sub: str
    dom: str
    obj: str
    act: str


class EnforceCheckRequest(BaseModel):
    user_id: Optional[str] = Field(default=None, description="ID o username del usuario (opcional, por defecto el usuario actual)")
    domain: str = Field(default="*", description="ID del Tenant, Servidor o dominio ('*' para global)")
    domain_type: DomainType = Field(default=DomainType.TENANT, description="Tipo de dominio ('tenant' o 'server')")
    resource: str = Field(..., description="Recurso a verificar (ej: telemetry)")
    action: str = Field(..., description="Acción a verificar (ej: read, write)")


# ==========================================
# Endpoints de Gestión de Roles
# ==========================================

@router.post("/roles/assign")
async def assign_role_to_user(
    request: RoleAssignRequest,
    current_user: User = Depends(CasbinAuth(resource="iam", action="write"))
):
    """
    Asigna un rol a un usuario dentro de un Tenant / Servidor específico o globalmente mediante URN.
    """
    enforcer = get_casbin_enforcer()
    domain_urn = _build_domain_urn(request.domain, request.domain_type)

    already_assigned = enforcer.has_grouping_policy(
        request.user_id,
        request.role,
        domain_urn
    )
    if already_assigned:
        return {
            "status": "ok",
            "message": f"El usuario '{request.user_id}' ya posee el rol '{request.role}' en el dominio '{domain_urn}'"
        }

    success = await enforcer.add_role_for_user_in_domain(
        request.user_id,
        request.role,
        domain_urn
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al persistir la asignación de rol en Casbin"
        )

    return {
        "status": "ok",
        "message": f"Rol '{request.role}' asignado exitosamente al usuario '{request.user_id}' en el dominio '{domain_urn}'"
    }


@router.post("/roles/revoke")
async def revoke_role_from_user(
    request: RoleRevokeRequest,
    current_user: User = Depends(CasbinAuth(resource="iam", action="write"))
):
    """
    Revoca un rol asignado a un usuario dentro de un Tenant / Servidor específico mediante URN.
    """
    enforcer = get_casbin_enforcer()
    domain_urn = _build_domain_urn(request.domain, request.domain_type)

    success = await enforcer.remove_grouping_policy(
        request.user_id,
        request.role,
        domain_urn
    )
    if not success:
        return {
            "status": "ok",
            "message": f"El usuario '{request.user_id}' no poseía el rol '{request.role}' en el dominio '{domain_urn}'"
        }

    return {
        "status": "ok",
        "message": f"Rol '{request.role}' revocado exitosamente del usuario '{request.user_id}' en el dominio '{domain_urn}'"
    }


@router.get("/users/{user_id}/roles")
async def get_user_roles(
    user_id: str,
    domain: Optional[str] = Query(default=None, description="Dominio, ID de Tenant o Servidor opcional"),
    domain_type: DomainType = Query(default=DomainType.TENANT, description="Tipo de dominio ('tenant' o 'server')"),
    current_user: User = Depends(CasbinAuth(resource="iam", action="read"))
):
    """
    Retorna la lista de roles asociados a un usuario en un dominio específico (con prefijo URN) o en todos los dominios.
    """
    enforcer = get_casbin_enforcer()
    if domain:
        domain_urn = _build_domain_urn(domain, domain_type)
        roles = await enforcer.get_roles_for_user_in_domain(user_id, domain_urn)
    else:
        roles = await enforcer.get_roles_for_user(user_id)
        domain_urn = "*"

    return {
        "user_id": user_id,
        "domain": domain_urn,
        "roles": roles
    }


@router.get("/roles/{role}/users")
async def get_users_with_role(
    role: str,
    domain: str = Query(default="*", description="Dominio, ID de Tenant o Servidor ('*' para todos)"),
    domain_type: DomainType = Query(default=DomainType.TENANT, description="Tipo de dominio ('tenant' o 'server')"),
    current_user: User = Depends(CasbinAuth(resource="iam", action="read"))
):
    """
    Retorna la lista de usuarios que poseen un rol determinado dentro de un Tenant / Servidor / Dominio (con prefijo URN).
    """
    enforcer = get_casbin_enforcer()
    domain_urn = _build_domain_urn(domain, domain_type)
    users = await enforcer.get_users_for_role_in_domain(role, domain_urn)
    return {
        "role": role,
        "domain": domain_urn,
        "users": users
    }


# ==========================================
# Endpoints de Gestión de Políticas (Permissions)
# ==========================================

@router.post("/policies")
async def add_policy_rule(
    request: PolicyRuleRequest,
    current_user: User = Depends(CasbinAuth(resource="iam", action="write"))
):
    """
    Agrega una nueva regla de política RBAC (sub, dom, obj, act) a Casbin soportando prefijos URN.
    """
    enforcer = get_casbin_enforcer()
    dom = request.dom
    if dom != "*" and request.domain_type is not None:
        dom = _build_domain_urn(dom, request.domain_type)

    has_p = enforcer.has_policy(request.sub, dom, request.obj, request.act)
    if has_p:
        return {
            "status": "ok",
            "message": f"La política ({request.sub}, {dom}, {request.obj}, {request.act}) ya existe"
        }

    success = await enforcer.add_policy(request.sub, dom, request.obj, request.act)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No se pudo agregar la política en la base de datos"
        )

    return {
        "status": "ok",
        "message": f"Política registrada: ({request.sub}, {dom}, {request.obj}, {request.act})"
    }


@router.delete("/policies")
async def remove_policy_rule(
    request: PolicyRuleRequest,
    current_user: User = Depends(CasbinAuth(resource="iam", action="delete"))
):
    """
    Elimina una regla de política RBAC (sub, dom, obj, act) de Casbin soportando prefijos URN.
    """
    enforcer = get_casbin_enforcer()
    dom = request.dom
    if dom != "*" and request.domain_type is not None:
        dom = _build_domain_urn(dom, request.domain_type)

    success = await enforcer.remove_policy(request.sub, dom, request.obj, request.act)
    if not success:
        return {
            "status": "ok",
            "message": "La política especificada no fue encontrada"
        }

    return {
        "status": "ok",
        "message": f"Política eliminada: ({request.sub}, {dom}, {request.obj}, {request.act})"
    }


@router.get("/policies", response_model=List[PolicyRuleResponse])
async def list_policy_rules(
    domain: Optional[str] = Query(default=None, description="Filtrar por dominio / URN (ej: tenant:123 o 123)"),
    domain_type: Optional[DomainType] = Query(default=None, description="Tipo de dominio opcional si domain no está prefijado"),
    current_user: User = Depends(CasbinAuth(resource="iam", action="read"))
):
    """
    Lista las reglas de política Casbin activas persistidas en MongoDB.
    """
    enforcer = get_casbin_enforcer()
    raw_policies = enforcer.get_policy()

    filter_urn = None
    if domain:
        if domain != "*":
            filter_urn = _build_domain_urn(domain, domain_type or DomainType.TENANT)
        else:
            filter_urn = "*"

    results: List[PolicyRuleResponse] = []
    for p in raw_policies:
        if len(p) >= 4:
            sub, dom, obj, act = p[0], p[1], p[2], p[3]
            if filter_urn and dom != filter_urn and dom != "*":
                continue
            results.append(PolicyRuleResponse(sub=sub, dom=dom, obj=obj, act=act))

    return results


@router.post("/enforce-check")
async def check_enforcement(
    request: EnforceCheckRequest,
    current_user: User = Depends(CasbinAuth(resource="iam", action="read"))
):
    """
    Endpoint de diagnóstico para evaluar si un usuario tiene acceso a un recurso y acción en un tenant o servidor (con URN).
    """
    enforcer = get_casbin_enforcer()
    target_user_id = request.user_id or str(current_user.id)
    domain_urn = _build_domain_urn(request.domain, request.domain_type)
    
    is_allowed = enforcer.enforce(target_user_id, domain_urn, request.resource, request.action)
    if not is_allowed and current_user.username and not request.user_id:
        is_allowed = enforcer.enforce(current_user.username, domain_urn, request.resource, request.action)
    
    return {
        "user_id": target_user_id,
        "domain": domain_urn,
        "resource": request.resource,
        "action": request.action,
        "is_allowed": is_allowed
    }
