import asyncio
import os
import mimetypes
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional, List, Dict, Any

import aiofiles
import aiosmtplib

from core.logger import logger


async def build_mime_message(
    to_email: str,
    subject: str,
    html_body: Optional[str] = None,
    text_body: Optional[str] = None,
    from_email: Optional[str] = None,
    attachment_paths: Optional[List[str]] = None,
) -> MIMEMultipart:
    """
    Construye un mensaje MIMEMultipart con soporte para texto plano, HTML y archivos adjuntos.
    Si hay archivos adjuntos, estructura el mensaje en 'mixed' conteniendo una sub-parte 'alternative'
    (texto plano y HTML) y las partes adjuntas en 'MIMEBase' codificadas en Base64.
    La lectura de archivos del disco local se realiza de forma 100% no bloqueante con aiofiles.
    """
    sender = from_email or "no-reply@thingsboard.gateway"
    has_attachments = bool(attachment_paths)

    # 1. Estructuración MIME según la presencia de archivos adjuntos
    if has_attachments:
        message = MIMEMultipart("mixed")
        body_container = MIMEMultipart("alternative")
    else:
        message = MIMEMultipart("alternative")
        body_container = message

    message["From"] = sender
    message["To"] = to_email
    message["Subject"] = subject
    message["Date"] = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

    # 2. Cascarón en texto plano (fallback RFC 2046)
    plain_content = text_body
    if not plain_content:
        if html_body:
            plain_content = (
                "Este correo contiene formato HTML y posibles archivos adjuntos. "
                "Por favor utilice un cliente de correo compatible con HTML para visualizarlo."
            )
        else:
            plain_content = ""

    if plain_content:
        body_container.attach(MIMEText(plain_content, "plain", "utf-8"))

    # 3. Cascarón en HTML
    if html_body:
        body_container.attach(MIMEText(html_body, "html", "utf-8"))

    # Si hay adjuntos, acoplar el contenedor alternativo a la raíz 'mixed'
    if has_attachments:
        message.attach(body_container)

        # 4. Procesamiento de archivos adjuntos sin bloqueo de I/O
        for file_path in attachment_paths:
            if not file_path:
                continue

            file_exists = await asyncio.to_thread(os.path.isfile, file_path)
            if not file_exists:
                logger.warning(f"[EmailService] Archivo adjunto no encontrado en disco: '{file_path}'. Se omitirá.")
                continue

            file_name = os.path.basename(file_path)
            ctype, encoding = mimetypes.guess_type(file_path)
            if ctype is None or encoding is not None:
                maintype, subtype = "application", "octet-stream"
            else:
                maintype, subtype = ctype.split("/", 1)

            part = MIMEBase(maintype, subtype)

            # Lectura asíncrona de bytes con aiofiles para no bloquear el Event Loop
            async with aiofiles.open(file_path, mode="rb") as f:
                content = await f.read()

            part.set_payload(content)
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{file_name}"'
            )
            message.attach(part)
            logger.debug(f"[EmailService] Archivo '{file_name}' ({len(content)} bytes) adjuntado exitosamente.")

    return message


async def send_email_async(
    to_email: str,
    subject: str,
    html_body: Optional[str] = None,
    text_body: Optional[str] = None,
    from_email: Optional[str] = None,
    attachment_paths: Optional[List[str]] = None,
    host: Optional[str] = None,
    port: int = 587,
    username: Optional[str] = None,
    password: Optional[str] = None,
    use_tls: bool = True,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """
    Envía un correo electrónico a través de un servidor SMTP dinámico utilizando aiosmtplib.
    No consume variables de entorno globales; todas las credenciales y parámetros de conexión
    son inyectados dinámicamente desde el worker (descifradas en memoria RAM desde MongoDB).

    Parámetros:
        to_email: Dirección del destinatario.
        subject: Asunto del mensaje.
        html_body: Cuerpo del mensaje formateado en HTML.
        text_body: Cuerpo opcional en texto plano.
        from_email: Remitente opcional (por defecto toma username).
        attachment_paths: Lista opcional de rutas a archivos locales en disco duro.
        host: Host o FQDN del servidor SMTP.
        port: Puerto de conexión SMTP.
        username: Usuario para autenticación SMTP.
        password: Contraseña en texto plano (descifrada en RAM).
        use_tls: Flag para habilitar TLS/STARTTLS.
        timeout: Timeout en segundos para la conexión y envío.

    Retorna:
        Dict con los metadatos y estado del envío.
    """
    if not host:
        raise ValueError("El parámetro 'host' del servidor SMTP es obligatorio para el envío.")
    if not to_email:
        raise ValueError("El destinatario 'to_email' es obligatorio.")

    sender = from_email or username or "no-reply@thingsboard.gateway"

    message = await build_mime_message(
        to_email=to_email,
        subject=subject,
        html_body=html_body,
        text_body=text_body,
        from_email=sender,
        attachment_paths=attachment_paths,
    )

    # Configuración estricta de TLS / STARTTLS para aiosmtplib:
    # Port 465 utiliza SMTPS directo (use_tls=True, start_tls=False)
    # Port 587/25 utiliza STARTTLS explícito (use_tls=False, start_tls=True)
    if port == 465:
        smtp_use_tls = bool(use_tls)
        smtp_start_tls = False
    else:
        smtp_use_tls = False
        smtp_start_tls = bool(use_tls)

    smtp_kwargs: Dict[str, Any] = {
        "hostname": host,
        "port": port,
        "timeout": timeout,
        "use_tls": smtp_use_tls,
        "start_tls": smtp_start_tls,
    }

    if username and password:
        smtp_kwargs["username"] = username
        smtp_kwargs["password"] = password

    logger.info(
        f"[EmailService] Enviando correo asíncrono dinámico a '{to_email}' vía SMTP {host}:{port} "
        f"(TLS: {use_tls}, STARTTLS: {smtp_start_tls}, SMTPS: {smtp_use_tls}, Adjuntos: {len(attachment_paths or [])})..."
    )

    # Envío asíncrono puro sin bloqueo de I/O
    response, server_response_str = await aiosmtplib.send(
        message,
        sender=sender,
        recipients=[to_email],
        **smtp_kwargs,
    )

    sent_at = datetime.now(timezone.utc).isoformat()
    logger.info(
        f"[EmailService] Correo enviado exitosamente a '{to_email}'. "
        f"Respuesta del servidor SMTP: {server_response_str}"
    )

    return {
        "status": "SENT",
        "to_email": to_email,
        "from_email": sender,
        "subject": subject,
        "smtp_server": f"{host}:{port}",
        "attachments_count": len(attachment_paths or []),
        "server_response": str(server_response_str),
        "sent_at": sent_at,
    }
