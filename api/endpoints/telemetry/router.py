import os
import json
import asyncio
import calendar
import mimetypes
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional, List, Union, Any, Dict

from fastapi import APIRouter, HTTPException, Request, Depends, status, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator, model_validator
import redis.asyncio as redis
from beanie import PydanticObjectId

from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.pagination import PaginatedResponse, build_pagination_metadata
from core.services.task_registry import publish_task_event
from core.config import settings
from core.logger import logger
from core.redis_client import redis_client
from core.arq_pool import get_arq_pool
from core.casbin_enforcer import get_casbin_enforcer
from core.services.telegram_service import format_alert_message
from api.deps import User, get_current_user
from workers.tasks import (
    download_telemetry_task,
    generate_monthly_heatmap_task,
    get_server_lock_key
)

router = APIRouter()


class DownloadTelemetryRequest(BaseModel):
    tenant_id: str = Field(..., description="ID de MongoDB del documento TBTenant")
    start_date: str = Field(..., description="Fecha inicial ISO (ej: 2026-01-01T00:00:00)")
    end_date: str = Field(..., description="Fecha final ISO (ej: 2026-08-01T23:59:59)")
    entity_type: str = Field(default="DEVICE", description="Tipo de entidad en ThingsBoard (DEVICE, ASSET)")
    entity_id: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="UUID o lista de UUIDs de dispositivos específicos. Dejar en null para descargar todos los dispositivos del Tenant."
    )
    time_zone: str = Field(default="UTC", description="Zona horaria (ej: America/Mexico_City)")
    concurrency_limit: int = Field(default=3, description="Límite de concurrencia de descarga simultánea")
    page_limit: int = Field(default=2000, description="Tamaño de página por petición de telemetría")
    force_reload: bool = Field(default=False, description="Sobrescribir checkpoints previos e iniciar desde cero")

    @field_validator("entity_id", mode="before")
    @classmethod
    def sanitize_entity_id(cls, v: Any) -> Optional[Union[str, List[str]]]:
        if v is None:
            return None
        placeholder_values = {"string", "null", "none", "undefined", "", "{}", "[]"}
        if isinstance(v, str):
            clean_v = v.strip()
            if clean_v.lower() in placeholder_values:
                return None
            return clean_v
        if isinstance(v, list):
            cleaned_list = [
                str(item).strip() for item in v
                if str(item).strip() and str(item).strip().lower() not in placeholder_values
            ]
            return cleaned_list if cleaned_list else None
        return v


class ExcelReportRequest(BaseModel):
    tenant_id: str = Field(..., description="ID de MongoDB del documento TBTenant")
    combine_in_single_file: bool = Field(default=True, description="True para un solo XLSX multi-hoja; False para ZIP con múltiples XLSX independientes")
    start_date: Optional[str] = Field(default=None, description="Fecha inicial ISO (ej: 2026-01-01T00:00:00)")
    end_date: Optional[str] = Field(default=None, description="Fecha final ISO (ej: 2026-01-31T23:59:59)")
    year: Optional[int] = Field(default=None, description="Año numérico (ej: 2026)")
    month: Optional[int] = Field(default=None, description="Mes numérico (1-12)")
    entity_type: str = Field(default="DEVICE", description="Tipo de entidad en ThingsBoard (DEVICE, ASSET)")
    entity_id: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="UUID o lista de UUIDs de dispositivos específicos. Dejar en null para todos."
    )
    time_zone: Optional[str] = Field(default=None, description="Zona horaria (default: settings.APP_TIMEZONE)")
    concurrency_limit: int = Field(default=3, description="Límite de concurrencia de descarga simultánea")
    page_limit: int = Field(default=2000, description="Tamaño de página por petición de telemetría")
    force_reload: bool = Field(default=False, description="Sobrescribir checkpoints previos e iniciar desde cero")

    @field_validator("entity_id", mode="before")
    @classmethod
    def sanitize_entity_id(cls, v: Any) -> Optional[Union[str, List[str]]]:
        if v is None:
            return None
        placeholder_values = {"string", "null", "none", "undefined", "", "{}", "[]"}
        if isinstance(v, str):
            clean_v = v.strip()
            if clean_v.lower() in placeholder_values:
                return None
            return clean_v
        if isinstance(v, list):
            cleaned_list = [
                str(item).strip() for item in v
                if str(item).strip() and str(item).strip().lower() not in placeholder_values
            ]
            return cleaned_list if cleaned_list else None
        return v

    @model_validator(mode="after")
    def validate_and_compute_dates(self):
        has_exact = bool(self.start_date and self.end_date)
        has_month = bool(self.year is not None and self.month is not None)

        if has_exact == has_month:
            raise ValueError("Debe especificar O BIEN ('start_date' y 'end_date') O BIEN ('year' y 'month'), pero no ambos ni ninguno.")

        tz_name = self.time_zone or settings.APP_TIMEZONE
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo(settings.APP_TIMEZONE)

        if has_month:
            if not (1 <= self.month <= 12):
                raise ValueError("El mes debe estar comprendido entre 1 y 12.")
            if self.year < 1970 or self.year > 3000:
                raise ValueError("Año fuera del rango soportado.")

            _, last_day = calendar.monthrange(self.year, self.month)
            start_dt = datetime(self.year, self.month, 1, 0, 0, 0, 0, tzinfo=tz)
            end_dt = datetime(self.year, self.month, last_day, 23, 59, 59, 999000, tzinfo=tz)

            self.start_date = start_dt.isoformat()
            self.end_date = end_dt.isoformat()
        else:
            try:
                start_dt = datetime.fromisoformat(self.start_date)
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=tz)
                end_dt = datetime.fromisoformat(self.end_date)
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=tz)
            except Exception as e:
                raise ValueError(f"Formato ISO inválido para las fechas: {e}")

            if start_dt > end_dt:
                raise ValueError("start_date no puede ser posterior a end_date.")

            self.start_date = start_dt.isoformat()
            self.end_date = end_dt.isoformat()

        return self


class HeatmapEmailOptions(BaseModel):
    """Opciones avanzadas para el envío de reportes mensuales de mapas de calor por correo electrónico."""
    enabled: bool = Field(default=False, description="Bandera que indica si el envío de correo está activo y debe ser procesado")
    to_email: Optional[Union[str, List[str]]] = Field(default=None, description="Para (destinatario o lista de destinatarios)")
    subject: Optional[str] = Field(default=None, description="Asunto del correo")
    cc: Optional[Union[str, List[str]]] = Field(default=None, description="Con copia (CC)")
    bcc: Optional[Union[str, List[str]]] = Field(default=None, description="Con copia oculta (BCC)")
    body: Optional[str] = Field(default=None, description="Cuerpo del mensaje (texto plano o HTML)")
    from_email: Optional[str] = Field(default=None, description="De* (opcional para sobreescribir el remitente de la configuración SMTP cargada)")
    from_name: Optional[str] = Field(default=None, description="Nombre descriptivo del remitente (From Name)")
    sender_name: Optional[str] = Field(default=None, description="Alias para from_name (Nombre descriptivo del remitente)")


class HeatmapReportRequest(BaseModel):
    tenant_id: str = Field(..., description="ID de MongoDB del documento TBTenant")
    year: Optional[int] = Field(default=None, description="Año numérico (ej: 2026)")
    month: Optional[int] = Field(default=None, description="Mes numérico (1-12)")
    time_zone: Optional[str] = Field(default=None, description="Zona horaria (default: settings.APP_TIMEZONE)")
    heatmap_config: Optional[dict] = Field(default=None, description="Configuración personalizada de heatmap (whitelist, reglas, agregación)")

    # Opciones de envío por correo electrónico
    send_email: bool = Field(default=False, description="Bandera que indica si el envío por correo electrónico está activo y debe ser procesado")
    email_options: Optional[HeatmapEmailOptions] = Field(default=None, description="Opciones avanzadas de correo electrónico")

    # Atajos directos para máxima comodidad del usuario:
    to_email: Optional[Union[str, List[str]]] = Field(default=None, description="Para (destinatario)")
    subject: Optional[str] = Field(default=None, description="Asunto del correo")
    email_subject: Optional[str] = Field(default=None, description="Alias para Asunto")
    cc: Optional[Union[str, List[str]]] = Field(default=None, description="Con copia (CC)")
    email_cc: Optional[Union[str, List[str]]] = Field(default=None, description="Alias para Con copia (CC)")
    bcc: Optional[Union[str, List[str]]] = Field(default=None, description="Con copia oculta (BCC)")
    email_bcc: Optional[Union[str, List[str]]] = Field(default=None, description="Alias para Con copia oculta (BCC)")
    body: Optional[str] = Field(default=None, description="Cuerpo del correo (texto plano o HTML)")
    email_body: Optional[str] = Field(default=None, description="Alias para Cuerpo del correo")
    from_email: Optional[str] = Field(default=None, description="De* (opcional para sobreescribir el remitente de la configuración SMTP cargada)")
    from_name: Optional[str] = Field(default=None, description="Nombre descriptivo del remitente (From Name)")
    sender_name: Optional[str] = Field(default=None, description="Alias para from_name (Nombre descriptivo del remitente)")


class BackupResponse(BaseModel):
    id: str = Field(..., description="ID del documento de respaldo en MongoDB")
    tenant_id: str = Field(..., description="ID del Tenant respaldado")
    tenant_name: Optional[str] = Field(default=None, description="Nombre del Tenant")
    task_id: str = Field(..., description="ID del trabajo/tarea de respaldo en ARQ")
    requested_by: str = Field(..., description="ID del usuario que solicitó el respaldo")
    file_name: str = Field(..., description="Nombre del archivo generado")
    backup_type: str = Field(default="telemetry", description="Tipo de respaldo o artefacto")
    start_date: datetime = Field(..., description="Fecha de inicio del rango de telemetría")
    end_date: datetime = Field(..., description="Fecha de fin del rango de telemetría")
    file_size_bytes: int = Field(default=0, description="Tamaño del archivo en bytes")
    created_at: datetime = Field(..., description="Fecha y hora de creación del respaldo")
    download_url: str = Field(..., description="URL para la descarga directa del archivo")


class PaginatedBackupResponse(PaginatedResponse[BackupResponse]):
    """Respuesta paginada estándar para el catálogo de respaldos y artefactos."""
    pass


class DeviceStatusWebhookRequest(BaseModel):
    """
    DTO para la recepción de eventos y alertas de estado de dispositivos emitidos
    desde el Rule Engine de ThingsBoard o servicios de monitoreo perimetral.
    """
    device_name: str = Field(..., min_length=1, description="Nombre del dispositivo en ThingsBoard")
    status: str = Field(..., min_length=1, description="Estado reportado (ej: ONLINE, OFFLINE, CRITICAL, WARNING, ERROR)")
    layer: Union[int, str] = Field(..., description="Capa de monitoreo (1, 2, 3, 4 o identificador de capa)")
    tenant_id: str = Field(..., min_length=1, description="ID del tenant asociado en ThingsBoard o MongoDB")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Detalles adicionales o metadatos del estado")
    message: Optional[str] = Field(default=None, description="Mensaje descriptivo opcional del evento")

    @field_validator("device_name", "status", "tenant_id", mode="before")
    @classmethod
    def strip_and_validate_non_empty(cls, v: Any) -> str:
        if v is None:
            raise ValueError("El valor no puede ser nulo.")
        if isinstance(v, bool) or not isinstance(v, str):
            raise ValueError("El valor debe ser una cadena de texto (string).")
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("El valor no puede estar vacío ni contener únicamente espacios en blanco.")
        return cleaned

    @field_validator("layer", mode="before")
    @classmethod
    def validate_layer(cls, v: Any) -> Union[int, str]:
        if v is None:
            raise ValueError("La capa de monitoreo no puede ser nula.")
        if isinstance(v, bool):
            raise ValueError("La capa de monitoreo no puede ser un booleano.")
        if not isinstance(v, (int, str)):
            raise ValueError("La capa de monitoreo debe ser un entero o una cadena de texto.")
        if isinstance(v, str):
            cleaned = v.strip()
            if not cleaned:
                raise ValueError("La capa de monitoreo no puede estar vacía ni contener únicamente espacios en blanco.")
            return cleaned
        return v

    @field_validator("message", mode="before")
    @classmethod
    def strip_message(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, bool) or not isinstance(v, str):
            raise ValueError("El mensaje descriptivo debe ser una cadena de texto (string).")
        cleaned = v.strip()
        return cleaned if cleaned else None


@router.post("/download", status_code=status.HTTP_200_OK)
async def download_telemetry(
    request: DownloadTelemetryRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Inicia una tarea asíncrona de descarga masiva de telemetría para un Tenant registrado en MongoDB.
    Valida la existencia del Tenant y el aislamiento por usuario antes de encolar en Celery.
    Aplica Rechazo Temprano (Fail Fast / Distributed Lock) si el servidor ThingsBoard se encuentra ocupado.
    El cliente se comunica exclusivamente mediante el JWT del API Gateway, sin necesidad de enviar tokens de ThingsBoard.
    """
    # 1. Validar existencia del Tenant en MongoDB usando Beanie
    try:
        obj_id = PydanticObjectId(request.tenant_id)
        tenant_doc = await TBTenant.get(obj_id)
    except Exception:
        tenant_doc = await TBTenant.get(request.tenant_id)

    if not tenant_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant de ThingsBoard con ID '{request.tenant_id}' no encontrado en MongoDB"
        )

    # 2. Validar aislamiento y permisos de usuario (Casbin RBAC + Ownership + Superadmin)
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    is_owner = tenant_doc.user_id in [str(current_user.id), current_user.id]
    if not is_admin and not is_owner:
        try:
            enforcer = get_casbin_enforcer()
            user_id_str = str(current_user.id)
            tenant_domain = f"tenant:{tenant_doc.id}"
            is_allowed = enforcer.enforce(user_id_str, tenant_domain, "telemetry", "write")
            if not is_allowed and current_user.username:
                is_allowed = enforcer.enforce(current_user.username, tenant_domain, "telemetry", "write")
        except Exception:
            is_allowed = False

        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos para iniciar descargas en este Tenant"
            )

    # 3. Resolver servidor ThingsBoard padre
    server_doc = await tenant_doc.get_server()
    if not server_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"El servidor ThingsBoard asociado al Tenant '{tenant_doc.name}' no existe o fue eliminado"
        )

    # 4. Adquisición Atómica del Candado Distribuido (Fail Fast / Prevención TOCTOU)
    server_id = str(server_doc.id)
    lock_key = get_server_lock_key(server_id)
    lock_acquired = await redis_client.set(lock_key, "locked", nx=True, ex=300)
    if not lock_acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El servidor ThingsBoard se encuentra actualmente ocupado procesando otro respaldo. Por favor, intente más tarde."
        )

    # 5. Preparar payload desacoplado para el Celery Worker
    task_payload = {
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "entity_type": request.entity_type,
        "entity_id": request.entity_id,
        "time_zone": request.time_zone,
        "concurrency_limit": request.concurrency_limit,
        "page_limit": request.page_limit,
        "force_reload": request.force_reload,
        "user_id": str(current_user.id)
    }

    # 6. Encolar la tarea en ARQ de forma completamente asíncrona y no bloqueante
    try:
        arq_pool = await get_arq_pool()
        job = await arq_pool.enqueue_job("download_telemetry_task", payload=task_payload)
        job_id = job.job_id if job else "unknown_job"
    except Exception as e:
        await redis_client.delete(lock_key)
        raise e

    # Publicar evento inicial en Redis (SSE / Registro Hash de tareas activas)
    await publish_task_event(
        redis_client=redis_client,
        user_id=str(current_user.id),
        task_id=job_id,
        status="QUEUED",
        task_type="telemetry",
        progress_pct=0.0,
        message="Tarea de descarga de telemetría encolada en ARQ.",
        tenant_name=tenant_doc.name
    )

    return {
        "task_id": job_id,
        "task_type": "telemetry",
        "status": "Task enqueued",
        "user_id": str(current_user.id),
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url
    }


@router.post("/report/excel", status_code=status.HTTP_200_OK)
async def generate_excel_report(
    request: ExcelReportRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Inicia una tarea asíncrona de exportación de telemetría a formato Excel (.xlsx).
    Soporta fechas exactas o mes/año calculado matemáticamente, modo mono-archivo (.xlsx multi-hoja)
    o multi-archivo (.zip con .xlsx individuales y recolección de basura por dispositivo).
    Aplica Distributed Lock Fail Fast y encola en ARQ.
    """
    # 1. Validar existencia del Tenant en MongoDB usando Beanie
    try:
        obj_id = PydanticObjectId(request.tenant_id)
        tenant_doc = await TBTenant.get(obj_id)
    except Exception:
        tenant_doc = await TBTenant.get(request.tenant_id)

    if not tenant_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant de ThingsBoard con ID '{request.tenant_id}' no encontrado en MongoDB"
        )

    # 2. Validar aislamiento y permisos de usuario (Casbin RBAC + Ownership + Superadmin)
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    is_owner = tenant_doc.user_id in [str(current_user.id), current_user.id]
    if not is_admin and not is_owner:
        try:
            enforcer = get_casbin_enforcer()
            user_id_str = str(current_user.id)
            tenant_domain = f"tenant:{tenant_doc.id}"
            is_allowed = enforcer.enforce(user_id_str, tenant_domain, "telemetry", "write")
            if not is_allowed and current_user.username:
                is_allowed = enforcer.enforce(current_user.username, tenant_domain, "telemetry", "write")
        except Exception:
            is_allowed = False

        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos para generar reportes en este Tenant"
            )

    # 3. Resolver servidor ThingsBoard padre
    server_doc = await tenant_doc.get_server()
    if not server_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"El servidor ThingsBoard asociado al Tenant '{tenant_doc.name}' no existe o fue eliminado"
        )

    # 4. Adquisición Atómica del Candado Distribuido (Fail Fast)
    server_id = str(server_doc.id)
    lock_key = get_server_lock_key(server_id)
    lock_acquired = await redis_client.set(lock_key, "locked", nx=True, ex=300)
    if not lock_acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El servidor ThingsBoard se encuentra actualmente ocupado procesando otro respaldo o reporte. Por favor, intente más tarde."
        )

    # 5. Extraer report_config de custom_metadata
    report_config = tenant_doc.custom_metadata.get("report_config", {}) if tenant_doc.custom_metadata else {}

    # 6. Preparar payload desacoplado para el ARQ Worker
    task_payload = {
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "combine_in_single_file": request.combine_in_single_file,
        "entity_type": request.entity_type,
        "entity_id": request.entity_id,
        "time_zone": request.time_zone or settings.APP_TIMEZONE,
        "concurrency_limit": request.concurrency_limit,
        "page_limit": request.page_limit,
        "force_reload": request.force_reload,
        "report_config": report_config,
        "user_id": str(current_user.id)
    }

    # 7. Encolar la tarea en ARQ de forma asíncrona
    try:
        arq_pool = await get_arq_pool()
        job = await arq_pool.enqueue_job("generate_excel_report_task", payload=task_payload)
        job_id = job.job_id if job else "unknown_job"
    except Exception as e:
        await redis_client.delete(lock_key)
        raise e

    # Publicar evento inicial en Redis (SSE / Registro Hash de tareas activas)
    await publish_task_event(
        redis_client=redis_client,
        user_id=str(current_user.id),
        task_id=job_id,
        status="QUEUED",
        task_type="excel_report",
        progress_pct=0.0,
        message="Tarea de generación de reporte Excel encolada en ARQ.",
        tenant_name=tenant_doc.name
    )

    return {
        "task_id": job_id,
        "task_type": "excel_report",
        "status": "Task enqueued",
        "user_id": str(current_user.id),
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url,
        "combine_in_single_file": request.combine_in_single_file,
        "start_date": request.start_date,
        "end_date": request.end_date
    }


@router.post("/report/heatmap", status_code=status.HTTP_200_OK)
async def generate_heatmap_report_endpoint(
    request: HeatmapReportRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Inicia una tarea asíncrona de generación de reporte mensual de Mapa de Calor (Heatmap) en PDF.
    - Resuelve el Tenant y Servidor en MongoDB.
    - Valida permisos RBAC con Casbin y propiedad del Tenant.
    - Adquiere distributed lock con fail fast.
    - Encola la tarea 'generate_monthly_heatmap_task' en ARQ.
    """
    # 1. Validar existencia del Tenant en MongoDB
    try:
        obj_id = PydanticObjectId(request.tenant_id)
        tenant_doc = await TBTenant.get(obj_id)
    except Exception:
        tenant_doc = await TBTenant.get(request.tenant_id)

    if not tenant_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant de ThingsBoard con ID '{request.tenant_id}' no encontrado en MongoDB"
        )

    # 2. Validar aislamiento y permisos de usuario (Casbin RBAC + Ownership + Superadmin)
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    is_owner = tenant_doc.user_id in [str(current_user.id), current_user.id]
    if not is_admin and not is_owner:
        try:
            enforcer = get_casbin_enforcer()
            user_id_str = str(current_user.id)
            tenant_domain = f"tenant:{tenant_doc.id}"
            is_allowed = enforcer.enforce(user_id_str, tenant_domain, "telemetry", "write")
            if not is_allowed and current_user.username:
                is_allowed = enforcer.enforce(current_user.username, tenant_domain, "telemetry", "write")
        except Exception:
            is_allowed = False

        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos para generar reportes en este Tenant"
            )

    # 3. Resolver servidor ThingsBoard padre
    server_doc = await tenant_doc.get_server()
    if not server_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"El servidor ThingsBoard asociado al Tenant '{tenant_doc.name}' no existe o fue eliminado"
        )

    # 4. Adquisición Atómica del Candado Distribuido
    server_id = str(server_doc.id)
    lock_key = get_server_lock_key(server_id)
    lock_acquired = await redis_client.set(lock_key, "locked", nx=True, ex=300)
    if not lock_acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El servidor ThingsBoard se encuentra actualmente ocupado procesando otra tarea. Por favor, intente más tarde."
        )

    # 5. Resolver y validar opciones de envío de correo electrónico
    send_email_active = bool(
        request.send_email
        or (request.email_options and request.email_options.enabled)
    )

    resolved_to_email = (
        request.to_email
        or (request.email_options.to_email if request.email_options else None)
    )
    resolved_subject = (
        request.subject
        or request.email_subject
        or (request.email_options.subject if request.email_options else None)
    )
    resolved_cc = (
        request.cc
        or request.email_cc
        or (request.email_options.cc if request.email_options else None)
    )
    resolved_bcc = (
        request.bcc
        or request.email_bcc
        or (request.email_options.bcc if request.email_options else None)
    )
    resolved_body = (
        request.body
        or request.email_body
        or (request.email_options.body if request.email_options else None)
    )
    resolved_from = (
        request.from_email
        or (request.email_options.from_email if request.email_options else None)
    )
    resolved_from_name = (
        request.from_name
        or request.sender_name
        or (request.email_options.from_name if request.email_options else None)
        or (request.email_options.sender_name if request.email_options else None)
    )

    if send_email_active and not resolved_to_email:
        await redis_client.delete(lock_key)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="El campo 'to_email' (destinatario) es obligatorio cuando el envío por correo electrónico está activo (send_email=True)."
        )

    # 6. Preparar payload para ARQ
    task_payload = {
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url,
        "year": request.year,
        "month": request.month,
        "time_zone": request.time_zone or settings.APP_TIMEZONE,
        "heatmap_config": request.heatmap_config,
        "user_id": str(current_user.id),
        "send_email": send_email_active,
        "to_email": resolved_to_email,
        "subject": resolved_subject,
        "cc": resolved_cc,
        "bcc": resolved_bcc,
        "body": resolved_body,
        "from_email": resolved_from,
        "from_name": resolved_from_name,
        "sender_name": resolved_from_name,
        "email_options": request.email_options.model_dump() if request.email_options else None
    }

    # 6. Encolar la tarea en ARQ
    try:
        arq_pool = await get_arq_pool()
        job = await arq_pool.enqueue_job(
            "generate_monthly_heatmap_task",
            tenant_id=str(tenant_doc.id),
            payload=task_payload
        )
        job_id = job.job_id if job else "unknown_job"
    except Exception as e:
        await redis_client.delete(lock_key)
        raise e

    # Publicar evento inicial en Redis (SSE / Registro Hash de tareas activas)
    await publish_task_event(
        redis_client=redis_client,
        user_id=str(current_user.id),
        task_id=job_id,
        status="QUEUED",
        task_type="heatmap",
        progress_pct=0.0,
        message="Tarea de generación de mapa de calor encolada en ARQ.",
        tenant_name=tenant_doc.name
    )

    return {
        "task_id": job_id,
        "task_type": "heatmap",
        "status": "Task enqueued",
        "user_id": str(current_user.id),
        "tenant_id": str(tenant_doc.id),
        "tenant_name": tenant_doc.name,
        "server_id": server_id,
        "server_url": server_doc.base_url
    }




def build_single_backup_type_filter(backup_type: str) -> dict:
    b_type = backup_type.strip().lower()
    null_or_missing = [
        {"backup_type": None},
        {"backup_type": {"$exists": False}},
        {"backup_type": ""}
    ]
    if b_type == "heatmap":
        return {
            "$or": [
                {"backup_type": "heatmap"},
                {
                    "$and": [
                        {"$or": null_or_missing},
                        {"file_name": {"$regex": r"\.pdf$", "$options": "i"}}
                    ]
                }
            ]
        }
    elif b_type == "excel_report":
        return {
            "$or": [
                {"backup_type": "excel_report"},
                {
                    "$and": [
                        {"$or": null_or_missing},
                        {"file_name": {"$regex": r"\.xlsx$", "$options": "i"}}
                    ]
                }
            ]
        }
    elif b_type == "telemetry":
        return {
            "$or": [
                {"backup_type": "telemetry"},
                {
                    "$and": [
                        {"$or": null_or_missing},
                        {"file_name": {"$not": {"$regex": r"\.(pdf|xlsx)$", "$options": "i"}}}
                    ]
                }
            ]
        }
    else:
        return {"backup_type": b_type}


def build_backup_type_filter(backup_type: str) -> dict:
    """
    Construye el filtro de MongoDB para backup_type con soporte hacia atrás y tipos múltiples:
    Acepta tipos individuales ('telemetry', 'excel_report', 'heatmap') o combinados separados
    por coma ('telemetry,excel_report').
    """
    types = [t.strip() for t in backup_type.split(",") if t.strip()]
    if not types:
        return {}
    if len(types) == 1:
        return build_single_backup_type_filter(types[0])
    return {"$or": [build_single_backup_type_filter(t) for t in types]}


@router.get("/backups", response_model=PaginatedBackupResponse, status_code=status.HTTP_200_OK)
async def list_tenant_backups(
    page: int = Query(default=1, ge=1, description="Número de página (1-indexed)"),
    page_size: int = Query(default=20, ge=1, le=100, description="Cantidad de registros por página"),
    tenant_id: Optional[str] = Query(default=None, description="Filtrar respaldos por ID de tenant ('all' o None para todos los autorizados)"),
    backup_type: Optional[str] = Query(default=None, description="Filtrar por tipo de respaldo (telemetry, excel_report, heatmap)"),
    current_user: User = Depends(get_current_user)
):
    """
    Lista el historial de respaldos de telemetría y artefactos disponibles en MongoDB con paginación y filtrado.
    Soporta filtros por tenant_id y backup_type (telemetry, excel_report, heatmap).
    Preserva las reglas de control de acceso RBAC con Casbin y propiedad del Tenant.
    """
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]

    # Caso A: Se especifica tenant_id (distinto de 'all')
    if tenant_id and tenant_id.lower() != "all":
        try:
            obj_id = PydanticObjectId(tenant_id)
            tenant_doc = await TBTenant.get(obj_id)
        except Exception:
            tenant_doc = await TBTenant.get(tenant_id)

        if not tenant_doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tenant de ThingsBoard con ID '{tenant_id}' no encontrado en MongoDB"
            )

        is_owner = tenant_doc.user_id in [str(current_user.id), current_user.id]
        if not is_admin and not is_owner:
            try:
                enforcer = get_casbin_enforcer()
                user_id_str = str(current_user.id)
                tenant_domain = f"tenant:{tenant_doc.id}"
                is_allowed = enforcer.enforce(user_id_str, tenant_domain, "telemetry", "read")
                if not is_allowed and current_user.username:
                    is_allowed = enforcer.enforce(current_user.username, tenant_domain, "telemetry", "read")
            except Exception:
                is_allowed = False

            if not is_allowed:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="No tienes permisos para consultar los respaldos de este Tenant"
                )

        ref = tenant_doc.to_ref()
        tenant_query = {"$or": [{"tenant_id": ref}, {"tenant_id.$id": tenant_doc.id}, {"tenant_id": tenant_doc.id}]}

        if backup_type:
            mongo_filter = {"$and": [tenant_query, build_backup_type_filter(backup_type)]}
        else:
            mongo_filter = tenant_query

        query = TBBackup.find(mongo_filter)
        total = await query.count()
        backups = await query.sort("-created_at").skip((page - 1) * page_size).limit(page_size).to_list()

        results: List[BackupResponse] = []
        for b in backups:
            results.append(
                BackupResponse(
                    id=str(b.id),
                    tenant_id=str(tenant_doc.id),
                    tenant_name=tenant_doc.name,
                    task_id=b.task_id,
                    requested_by=b.requested_by,
                    file_name=b.file_name,
                    backup_type=b.get_backup_type(),
                    start_date=b.start_date,
                    end_date=b.end_date,
                    file_size_bytes=b.file_size_bytes,
                    created_at=b.created_at,
                    download_url=f"/api/v1/telemetry/download/file/{b.task_id}"
                )
            )

        return PaginatedBackupResponse(
            items=results,
            pagination=build_pagination_metadata(total=total, page=page, page_size=page_size)
        )

    # Caso B: tenant_id es None o 'all' -> listar respaldos autorizados
    if is_admin:
        if backup_type:
            mongo_filter = build_backup_type_filter(backup_type)
        else:
            mongo_filter = {}
    else:
        # Resolver tenants permitidos para el usuario
        enforcer = get_casbin_enforcer()
        all_tenants = await TBTenant.find_all().to_list()
        allowed_tenant_ids = []
        user_id_str = str(current_user.id)
        for t in all_tenants:
            if t.user_id in [user_id_str, current_user.id]:
                allowed_tenant_ids.append(t.id)
                continue
            t_dom = f"tenant:{t.id}"
            if enforcer.enforce(user_id_str, t_dom, "telemetry", "read") or (
                current_user.username and enforcer.enforce(current_user.username, t_dom, "telemetry", "read")
            ):
                allowed_tenant_ids.append(t.id)

        if not allowed_tenant_ids:
            return PaginatedBackupResponse(
                items=[],
                pagination=build_pagination_metadata(total=0, page=page, page_size=page_size)
            )

        tenant_query = {
            "$or": [
                {"tenant_id.$id": {"$in": allowed_tenant_ids}},
                {"tenant_id": {"$in": allowed_tenant_ids}}
            ]
        }
        if backup_type:
            mongo_filter = {"$and": [tenant_query, build_backup_type_filter(backup_type)]}
        else:
            mongo_filter = tenant_query

    query = TBBackup.find(mongo_filter)
    total = await query.count()
    backups = await query.sort("-created_at").skip((page - 1) * page_size).limit(page_size).to_list()

    results = []
    for b in backups:
        t_doc = await b.get_tenant()
        results.append(
            BackupResponse(
                id=str(b.id),
                tenant_id=b.get_tenant_id_str(),
                tenant_name=t_doc.name if t_doc else None,
                task_id=b.task_id,
                requested_by=b.requested_by,
                file_name=b.file_name,
                backup_type=b.get_backup_type(),
                start_date=b.start_date,
                end_date=b.end_date,
                file_size_bytes=b.file_size_bytes,
                created_at=b.created_at,
                download_url=f"/api/v1/telemetry/download/file/{b.task_id}"
            )
        )

    return PaginatedBackupResponse(
        items=results,
        pagination=build_pagination_metadata(total=total, page=page, page_size=page_size)
    )


@router.get("/download/file/{task_id}")
async def download_file(
    task_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Permite descargar el archivo ZIP respaldado consultando el catálogo TBBackup en MongoDB.
    Valida permisos de acceso al Tenant asociado antes de servir el archivo.
    """
    backup_dir = "backups"
    if not os.path.exists(backup_dir):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="El directorio de backups no existe"
        )

    # 1. Consultar el documento TBBackup en MongoDB
    backup_doc = None
    try:
        backup_doc = await TBBackup.find_one({"task_id": task_id})
    except Exception as e:
        logger.warning(f"[Download File] No se pudo consultar TBBackup para task {task_id}: {e}")

    if backup_doc:
        tenant_doc = await backup_doc.get_tenant()
        is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
        is_requester = backup_doc.requested_by in [str(current_user.id), current_user.id]
        is_tenant_owner = tenant_doc and tenant_doc.user_id in [str(current_user.id), current_user.id]

        if not is_admin and not is_requester and not is_tenant_owner:
            # Validar con Casbin si tiene permisos de lectura para ese tenant
            tenant_id_str = f"tenant:{tenant_doc.id}" if tenant_doc else "*"
            try:
                enforcer = get_casbin_enforcer()
                is_allowed = enforcer.enforce(str(current_user.id), tenant_id_str, "telemetry", "read")
            except Exception:
                is_allowed = False

            if not is_allowed:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="No tienes permisos para descargar este respaldo"
                )

        file_path = os.path.join(backup_dir, backup_doc.file_name)
        if not os.path.exists(file_path):
            heatmap_path = os.path.join(backup_dir, "heatmaps", backup_doc.file_name)
            if os.path.exists(heatmap_path):
                file_path = heatmap_path

        if os.path.exists(file_path):
            if backup_doc.file_name.endswith(".xlsx"):
                media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif backup_doc.file_name.endswith(".zip"):
                media_type = "application/zip"
            elif backup_doc.file_name.endswith(".pdf"):
                media_type = "application/pdf"
            else:
                guessed, _ = mimetypes.guess_type(backup_doc.file_name)
                media_type = guessed or "application/octet-stream"

            return FileResponse(
                path=file_path,
                media_type=media_type,
                filename=backup_doc.file_name
            )

    # 2. Fallback de compatibilidad si el archivo existe en disco
    search_dirs = [backup_dir, os.path.join(backup_dir, "heatmaps")]
    for s_dir in search_dirs:
        if os.path.exists(s_dir):
            for filename in os.listdir(s_dir):
                is_match = (
                    filename.startswith(f"{task_id}_")
                    or filename.endswith(f"_{task_id}.zip")
                    or filename.endswith(f"_{task_id}.xlsx")
                    or filename.endswith(f"_{task_id}.pdf")
                    or task_id in filename
                )
                if is_match and (filename.endswith(".zip") or filename.endswith(".xlsx") or filename.endswith(".pdf")):
                    file_path = os.path.join(s_dir, filename)
                    if filename.endswith(".xlsx"):
                        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    elif filename.endswith(".pdf"):
                        media_type = "application/pdf"
                    else:
                        media_type = "application/zip"
                    return FileResponse(
                        path=file_path,
                        media_type=media_type,
                        filename=filename
                    )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="El archivo de backup no existe o no ha terminado de procesarse"
    )


@router.delete("/backups/{backup_id}", status_code=status.HTTP_200_OK)
async def delete_tenant_backup(
    backup_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Elimina permanentemente un archivo de respaldo del catálogo en MongoDB y del disco físico.
    Requiere permisos de eliminación ('delete' o superadmin) sobre el recurso 'telemetry' para el Tenant.
    """
    # 1. Buscar documento TBBackup por MongoDB ID o task_id
    backup_doc = None
    try:
        obj_id = PydanticObjectId(backup_id)
        backup_doc = await TBBackup.get(obj_id)
    except Exception:
        pass

    if not backup_doc:
        try:
            backup_doc = await TBBackup.get(backup_id)
        except Exception:
            pass

    if not backup_doc:
        backup_doc = await TBBackup.find_one({"task_id": backup_id})

    if not backup_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Respaldo con ID '{backup_id}' no encontrado"
        )

    # 2. Validar permisos RBAC
    is_admin = current_user.is_superuser or current_user.role in ["admin", "superadmin"]
    if not is_admin:
        tenant_doc = await backup_doc.get_tenant()
        is_owner = tenant_doc and tenant_doc.user_id in [str(current_user.id), current_user.id]
        if not is_owner:
            tenant_id_str = f"tenant:{tenant_doc.id}" if tenant_doc else "*"
            try:
                enforcer = get_casbin_enforcer()
                user_id_str = str(current_user.id)
                is_allowed = enforcer.enforce(user_id_str, tenant_id_str, "telemetry", "delete")
                if not is_allowed and current_user.username:
                    is_allowed = enforcer.enforce(current_user.username, tenant_id_str, "telemetry", "delete")
            except Exception:
                is_allowed = False

            if not is_allowed:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="No tienes permisos para eliminar este respaldo"
                )

    # 3. Eliminar archivo físico de disco si existe
    backup_dir = "backups"
    candidate_paths = [
        os.path.join(backup_dir, backup_doc.file_name),
        os.path.join(backup_dir, "heatmaps", backup_doc.file_name)
    ]
    for cp in candidate_paths:
        try:
            if os.path.exists(cp):
                os.remove(cp)
                logger.info(f"[Backup Delete] Archivo físico eliminado: {cp}")
        except Exception as e:
            logger.warning(f"[Backup Delete] Error al eliminar archivo físico {cp}: {e}")

    # 4. Eliminar documento de MongoDB
    doc_id = str(backup_doc.id)
    await backup_doc.delete()
    logger.info(f"[Backup Delete] Documento TBBackup '{doc_id}' eliminado por usuario '{current_user.username}'")

    return {
        "status": "DELETED",
        "message": "Respaldo de telemetría eliminado exitosamente",
        "id": doc_id
    }


@router.post(
    "/webhooks/device-status",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Webhook para eventos de estado de dispositivos de ThingsBoard",
    description="Recibe eventos de estado y telemetría de dispositivos enviados automáticamente por ThingsBoard Rule Engine y encola notificaciones de alerta a Telegram vía ARQ."
)
async def device_status_webhook(payload: DeviceStatusWebhookRequest) -> dict:
    """
    Webhook público (machine-to-machine) consumido por ThingsBoard Rule Engine u otros sistemas perimetrales.
    No requiere sesión interactiva JWT de usuario.

    1. Valida el esquema del payload estructurado con Pydantic v2.
    2. Construye un mensaje formateado en HTML claro y profesional usando format_alert_message.
    3. Encola la tarea send_telegram_alert_task de forma 100% asíncrona en ARQ.
    4. Retorna de inmediato HTTP 202 Accepted con los metadatos correspondientes.
    """
    # Mapeo semántico de niveles de severidad para iconos visuales en Telegram
    status_level_map = {
        "CRITICAL": "CRITICAL",
        "ERROR": "ERROR",
        "FAIL": "ERROR",
        "FAILED": "ERROR",
        "WARNING": "WARNING",
        "WARN": "WARNING",
        "OFFLINE": "WARNING",
        "ONLINE": "SUCCESS",
        "SUCCESS": "SUCCESS",
        "OK": "SUCCESS",
        "INFO": "INFO",
    }
    severity_level = status_level_map.get(payload.status.upper(), "INFO")

    title = f"Evento de Dispositivo: {payload.device_name}"
    body = payload.message or f"El dispositivo '{payload.device_name}' reportó el estado '{payload.status}' en la Capa {payload.layer}."
    tags = ["ThingsBoard", "DeviceStatus", f"Capa{payload.layer}", payload.status.upper()]

    alert_details: Dict[str, Any] = {
        "Dispositivo": payload.device_name,
        "Estado": payload.status,
        "Capa": str(payload.layer),
        "Tenant ID": payload.tenant_id,
    }
    if payload.details:
        for k, v in payload.details.items():
            alert_details[str(k)] = v

    formatted_message = format_alert_message(
        title=title,
        body=body,
        level=severity_level,
        tags=tags,
        details=alert_details,
    )

    alert_key = f"device_status:{payload.tenant_id}:{payload.device_name}:{payload.status}"

    try:
        arq_pool = await get_arq_pool()
        job = await arq_pool.enqueue_job(
            "send_telegram_alert_task",
            payload={
                "message": formatted_message,
                "alert_key": alert_key,
                "ttl_seconds": 300,
                "layer": payload.layer,
                "device_name": payload.device_name,
                "status": payload.status,
                "tenant_id": payload.tenant_id,
                "details": payload.details,
            }
        )
    except Exception as arq_err:
        logger.error(f"[Webhook] Error al encolar tarea en ARQ/Redis: {arq_err}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Servicio de encolamiento temporalmente no disponible: {arq_err}",
        )

    job_id = job.job_id if job else None
    logger.info(
        f"[Webhook] Evento de estado encolado en ARQ (Job: {job_id}) para dispositivo '{payload.device_name}' "
        f"[Estado: {payload.status}, Capa: {payload.layer}, Tenant: {payload.tenant_id}]"
    )

    return {
        "status": "accepted",
        "job_id": job_id,
        "device_name": payload.device_name,
        "device_status": payload.status,
        "layer": str(payload.layer),
        "tenant_id": payload.tenant_id,
    }



