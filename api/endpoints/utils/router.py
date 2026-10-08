import asyncio
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Depends, status, HTTPException, Query, Response

from core.arq_pool import get_arq_pool
from core.config import settings
from core.logger import get_logger
from core.models.user import User
from core.models.tb_email_config import TBEmailConfig
from core.services.email_service import send_email_async
from api.deps import get_current_user
from api.endpoints.utils.schemas import (
    EmailConfigCreateRequest,
    EmailConfigUpdateRequest,
    EmailConfigResponse,
    EmailConfigTestRequest,
    TestEmailRequest,
    TestEmailResponse,
)

logger = get_logger("utils_router")

router = APIRouter()


def _to_config_response(config: TBEmailConfig) -> EmailConfigResponse:
    """Transforma un documento TBEmailConfig en un DTO de respuesta seguro."""
    return EmailConfigResponse(
        id=str(config.id),
        host=config.host,
        port=config.port,
        username=config.username,
        has_password=bool(config.encrypted_password),
        use_tls=config.use_tls,
        sender_email=config.sender_email,
        sender_name=config.sender_name,
        is_active=config.is_active,
        created_at=config.created_at,
        updated_at=config.updated_at,
    )



# =============================================================================
# Helper Interno para Actualización de Configuración
# =============================================================================

async def _apply_config_update(
    config: TBEmailConfig,
    payload: EmailConfigUpdateRequest,
    current_user: User
) -> EmailConfigResponse:
    """Aplica modificaciones parciales a un documento TBEmailConfig y persiste en MongoDB."""
    if payload.host is not None:
        config.host = payload.host
    if payload.port is not None:
        config.port = payload.port
    if payload.username is not None:
        config.username = payload.username
    if payload.use_tls is not None:
        config.use_tls = payload.use_tls
    if payload.sender_email is not None:
        config.sender_email = payload.sender_email
    if payload.sender_name is not None:
        config.sender_name = payload.sender_name
    if payload.is_active is not None:
        config.is_active = payload.is_active

    # Cifrado de nueva contraseña en RAM si fue provista
    if payload.password is not None:
        await config.set_password(payload.password)

    config.updated_at = datetime.now(timezone.utc)
    await config.save()

    logger.info(
        f"[EmailConfig API] Configuración SMTP '{config.id}' actualizada exitosamente por '{current_user.username}'"
    )
    return _to_config_response(config)


async def _enqueue_test_email(
    config: TBEmailConfig,
    payload: EmailConfigTestRequest,
    current_user: User
) -> TestEmailResponse:
    """
    Encola un correo de prueba en ARQ o lo ejecuta síncronamente según el parámetro sync.
    """
    subject = payload.subject or f"Prueba de Configuración SMTP ({config.host}) - ThingsBoard Super API Gateway"
    html_body = payload.html_body or f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #e2e8f0; border-radius: 8px;">
        <h2 style="color: #0284c7;">ThingsBoard Super API Gateway</h2>
        <p>Prueba de conectividad para el servidor SMTP configurado en MongoDB.</p>
        <ul>
            <li><strong>Host:</strong> {config.host}:{config.port}</li>
            <li><strong>Usuario:</strong> {config.username}</li>
            <li><strong>TLS:</strong> {'Habilitado' if config.use_tls else 'Deshabilitado'}</li>
            <li><strong>Solicitado por:</strong> {current_user.email or current_user.username}</li>
            <li><strong>Fecha:</strong> {datetime.now(timezone.utc).isoformat()} UTC</li>
        </ul>
    </div>
    """

    # Modo Síncrono (confirmación inmediata para pruebas de Swagger / UI)
    sender_email = payload.from_email or config.sender_email or config.username
    sender_name = payload.from_name or payload.sender_name or config.sender_name

    if payload.sync:
        plain_password = await config.get_password()
        try:
            result = await asyncio.wait_for(
                send_email_async(
                    to_email=payload.to_email,
                    subject=subject,
                    html_body=html_body,
                    body=payload.body,
                    attachment_paths=payload.attachment_paths,
                    cc=payload.cc,
                    bcc=payload.bcc,
                    host=config.host,
                    port=config.port,
                    username=config.username,
                    password=plain_password,
                    use_tls=config.use_tls,
                    from_email=sender_email,
                    from_name=sender_name,
                ),
                timeout=15.0
            )
            return TestEmailResponse(
                status="SUCCESS",
                message=f"Correo de prueba enviado y confirmado exitosamente vía '{config.host}:{config.port}'.",
                to_email=str(payload.to_email),
                subject=subject,
                details=result
            )
        except Exception as exc:
            logger.warning(f"[EmailConfig API] Fallo en envío síncrono de correo: {exc}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Fallo de conexión o entrega con el servidor SMTP '{config.host}:{config.port}': {str(exc)}"
            )

    # Modo Asíncrono (encolado en ARQ con trazabilidad completa)
    try:
        arq_pool = await get_arq_pool()
        job = await arq_pool.enqueue_job(
            "send_email_task",
            to_email=payload.to_email,
            subject=subject,
            html_body=html_body,
            body=payload.body,
            cc=payload.cc,
            bcc=payload.bcc,
            from_email=sender_email,
            from_name=sender_name,
            attachment_paths=payload.attachment_paths,
            config_id=str(config.id),
            user_id=str(current_user.id),
        )
        task_id = job.job_id if job else None
    except Exception as exc:
        logger.error(f"[EmailConfig API] Error al encolar prueba de SMTP en ARQ: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Fallo al encolar la tarea de correo en ARQ: {str(exc)}"
        )

    return TestEmailResponse(
        status="ACCEPTED",
        message=f"Correo de prueba encolado exitosamente utilizando el servidor SMTP '{config.host}:{config.port}'.",
        task_id=task_id,
        to_email=str(payload.to_email),
        subject=subject,
        status_url=f"/api/v1/tasks/{task_id}" if task_id else None,
        stream_url=f"/api/v1/tasks/{task_id}/stream" if task_id else None,
    )


# =============================================================================
# 1. Endpoints CRUD para la Configuración SMTP Única (Singleton Pattern)
# =============================================================================

@router.post(
    "/email-config",
    response_model=EmailConfigResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Crear configuración SMTP única",
    description=(
        "Almacena la configuración de servidor SMTP en MongoDB con contraseña cifrada vía Fernet. "
        "RESTRICCIÓN SINGLETON: Solo puede existir una única configuración en todo el sistema. "
        "Si ya existe una registrada, retornará HTTP 409 Conflict."
    )
)
@router.post(
    "/email-configs",
    response_model=EmailConfigResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Crear configuración SMTP (alias plural)",
    include_in_schema=False
)
async def create_email_config(
    payload: EmailConfigCreateRequest,
    current_user: User = Depends(get_current_user),
):
    """Crea la configuración SMTP única del sistema en MongoDB."""
    if await TBEmailConfig.exists_config():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Ya existe una configuración SMTP registrada en el sistema. "
                "Solo se permite una única configuración. Utilice PUT /api/v1/utils/email-config "
                "para modificarla o DELETE /api/v1/utils/email-config para eliminarla primero."
            )
        )

    new_config = TBEmailConfig(
        singleton_key="global_smtp_config",
        host=payload.host,
        port=payload.port,
        username=payload.username,
        use_tls=payload.use_tls,
        sender_email=payload.sender_email or payload.username,
        sender_name=payload.sender_name,
        is_active=payload.is_active,
    )

    # Cifrar la contraseña en RAM antes de persistir
    await new_config.set_password(payload.password)
    await new_config.insert()

    logger.info(
        f"[EmailConfig API] Configuración SMTP única '{new_config.id}' creada exitosamente "
        f"(Host: {new_config.host}:{new_config.port}, Usuario: {new_config.username}) por '{current_user.username}'"
    )
    return _to_config_response(new_config)


@router.get(
    "/email-config",
    response_model=EmailConfigResponse,
    status_code=status.HTTP_200_OK,
    summary="Obtener la configuración SMTP del sistema",
    description="Retorna la única configuración SMTP registrada en MongoDB sin exponer contraseñas en texto plano."
)
async def get_singleton_email_config(
    current_user: User = Depends(get_current_user),
):
    """Consulta la configuración SMTP única del sistema."""
    config = await TBEmailConfig.get_singleton()
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No se ha registrado ninguna configuración SMTP en el sistema. Debe registrar una mediante POST /api/v1/utils/email-config."
        )
    return _to_config_response(config)


@router.put(
    "/email-config",
    response_model=EmailConfigResponse,
    status_code=status.HTTP_200_OK,
    summary="Modificar la configuración SMTP del sistema",
    description="Actualiza la configuración SMTP única del sistema sin necesidad de especificar un ID en la URL."
)
async def update_singleton_email_config(
    payload: EmailConfigUpdateRequest,
    current_user: User = Depends(get_current_user),
):
    """Modifica la configuración SMTP única en MongoDB."""
    config = await TBEmailConfig.get_singleton()
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe ninguna configuración SMTP registrada para modificar. Cree una primero mediante POST /api/v1/utils/email-config."
        )
    return await _apply_config_update(config, payload, current_user)


@router.delete(
    "/email-config",
    status_code=status.HTTP_200_OK,
    summary="Eliminar la configuración SMTP del sistema",
    description="Elimina de forma permanente la configuración SMTP única del sistema sin necesidad de especificar un ID."
)
async def delete_singleton_email_config(
    current_user: User = Depends(get_current_user),
):
    """Elimina la configuración SMTP única del sistema."""
    config = await TBEmailConfig.get_singleton()
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe ninguna configuración SMTP registrada en el sistema para eliminar."
        )
    config_id = str(config.id)
    await config.delete()

    logger.info(
        f"[EmailConfig API] Configuración SMTP única '{config_id}' eliminada por '{current_user.username}'"
    )
    return {
        "status": "DELETED",
        "message": "Configuración SMTP eliminada exitosamente del sistema.",
        "id": config_id
    }


@router.post(
    "/email-config/test",
    response_model=TestEmailResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Probar la configuración SMTP del sistema",
    description="Prueba la configuración SMTP única registrada en el sistema de forma síncrona (sync=true) o asíncrona en ARQ."
)
async def test_singleton_email_config(
    payload: EmailConfigTestRequest,
    response: Response,
    current_user: User = Depends(get_current_user),
):
    """Prueba la configuración SMTP única del sistema encolando el envío en ARQ o de forma síncrona."""
    config = await TBEmailConfig.get_singleton()
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No se puede realizar la prueba: no existe ninguna configuración SMTP registrada en el sistema."
        )
    res = await _enqueue_test_email(config, payload, current_user)
    if payload.sync:
        response.status_code = status.HTTP_200_OK
    return res


# =============================================================================
# Rutas de Compatibilidad y Acceso por ID
# =============================================================================

@router.get(
    "/email-configs",
    response_model=List[EmailConfigResponse],
    status_code=status.HTTP_200_OK,
    summary="Listar configuraciones SMTP (compatibilidad)",
    description="Retorna la lista de configuraciones SMTP registradas en MongoDB (máximo 1 debido a la restricción singleton)."
)
async def list_email_configs(
    is_active: Optional[bool] = Query(default=None, description="Filtrar por estado activo"),
    current_user: User = Depends(get_current_user),
):
    """Lista las configuraciones SMTP de la base de datos."""
    if is_active is not None:
        configs = await TBEmailConfig.find(TBEmailConfig.is_active == is_active).to_list()
    else:
        configs = await TBEmailConfig.find().to_list()

    return [_to_config_response(c) for c in configs]



# =============================================================================
# 2. Endpoint General de Diagnóstico / Test de Correo
# =============================================================================

@router.post(
    "/test-email",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TestEmailResponse,
    summary="Disparar envío de correo de prueba (síncrono o asíncrono)",
    description=(
        "Endpoint protegido para verificar la conectividad del proveedor SMTP activo. "
        "Si sync=true, ejecuta el envío de forma inmediata esperando confirmación (HTTP 200/502). "
        "Si sync=false, encola el trabajo en ARQ retornando HTTP 202 con URLs de seguimiento en /api/v1/tasks."
    )
)
@router.post(
    "/send-test-email",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TestEmailResponse,
    include_in_schema=False
)
async def send_test_email(
    request: TestEmailRequest,
    response: Response,
    current_user: User = Depends(get_current_user),
):
    """
    Envía un correo de prueba utilizando la configuración SMTP activa en MongoDB.
    Soporta ejecución síncrona inmediata o encolamiento asíncrono en ARQ.
    """
    subject = request.subject or "Prueba de Configuración SMTP - ThingsBoard Super API Gateway"

    html_body = request.html_body
    if not html_body:
        html_body = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 24px; border: 1px solid #e0e0e0; border-radius: 8px; background-color: #ffffff;">
            <h2 style="color: #0284c7; margin-top: 0;">ThingsBoard Super API Gateway</h2>
            <p style="font-size: 15px; color: #333333;">
                Este es un correo de prueba emitido mediante el servicio asíncrono no bloqueante con <code>aiosmtplib</code>.
            </p>
            <div style="background-color: #f8fafc; border-left: 4px solid #0284c7; padding: 12px; margin: 20px 0;">
                <p style="margin: 4px 0; font-size: 13px;"><strong>Servidor SMTP:</strong> Configuración activa en MongoDB</p>
                <p style="margin: 4px 0; font-size: 13px;"><strong>Solicitado por:</strong> {current_user.email or current_user.username}</p>
                <p style="margin: 4px 0; font-size: 13px;"><strong>Timestamp:</strong> {datetime.now(timezone.utc).isoformat()} UTC</p>
            </div>
            <p style="font-size: 12px; color: #94a3b8; margin-bottom: 0;">
                Este mensaje fue generado automáticamente para verificar la infraestructura de notificaciones.
            </p>
        </div>
        """

    # 1. Modo Síncrono (confirmación inmediata para pruebas de Swagger / UI)
    if request.sync:
        config = await TBEmailConfig.get_singleton()
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No se encontró ninguna configuración SMTP registrada en el sistema. Registre una mediante POST /api/v1/utils/email-config."
            )
        plain_password = await config.get_password()
        sender_email = request.from_email or config.sender_email or config.username
        sender_name = request.from_name or request.sender_name or config.sender_name
        try:
            result = await asyncio.wait_for(
                send_email_async(
                    to_email=request.to_email,
                    subject=subject,
                    html_body=html_body,
                    text_body=request.text_body,
                    body=request.body,
                    cc=request.cc,
                    bcc=request.bcc,
                    attachment_paths=request.attachment_paths,
                    host=config.host,
                    port=config.port,
                    username=config.username,
                    password=plain_password,
                    use_tls=config.use_tls,
                    from_email=sender_email,
                    from_name=sender_name,
                ),
                timeout=15.0
            )
            response.status_code = status.HTTP_200_OK
            return TestEmailResponse(
                status="SUCCESS",
                message=f"Correo de prueba enviado y confirmado exitosamente vía '{config.host}:{config.port}'.",
                to_email=str(request.to_email),
                subject=subject,
                details=result
            )
        except Exception as exc:
            logger.warning(f"[API Utils] Fallo en envío síncrono de correo: {exc}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Fallo de conexión o entrega con el servidor SMTP '{config.host}:{config.port}': {str(exc)}"
            )

    # 2. Modo Asíncrono (encolado en ARQ con trazabilidad completa en /api/v1/tasks)
    sender_email = request.from_email
    sender_name = request.from_name or request.sender_name
    try:
        arq_pool = await get_arq_pool()
        job = await arq_pool.enqueue_job(
            "send_email_task",
            to_email=request.to_email,
            subject=subject,
            html_body=html_body,
            text_body=request.text_body,
            body=request.body,
            cc=request.cc,
            bcc=request.bcc,
            from_email=sender_email,
            from_name=sender_name,
            attachment_paths=request.attachment_paths,
            user_id=str(current_user.id),
        )
        task_id = job.job_id if job else None
        logger.info(
            f"[API Utils] Correo de prueba hacia '{request.to_email}' encolado exitosamente en ARQ "
            f"(Job ID: {task_id}) por usuario '{current_user.username}'"
        )
    except Exception as exc:
        logger.error(f"[API Utils] Error al encolar tarea send_email_task en ARQ: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Fallo al encolar la tarea de correo en ARQ: {str(exc)}"
        )

    return TestEmailResponse(
        status="ACCEPTED",
        message="Solicitud de correo electrónico de prueba encolada exitosamente en el worker de ARQ.",
        task_id=task_id,
        to_email=str(request.to_email),
        subject=subject,
        status_url=f"/api/v1/tasks/{task_id}" if task_id else None,
        stream_url=f"/api/v1/tasks/{task_id}/stream" if task_id else None,
    )
