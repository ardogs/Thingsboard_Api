import re
import asyncio
import os
import mimetypes
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr, parseaddr
from typing import Optional, List, Dict, Any, Union

import aiofiles
import aiosmtplib

from core.logger import get_logger

logger = get_logger("email_service")


def _parse_email_addresses(val: Optional[Union[str, List[str]]]) -> List[str]:
    """Normaliza y extrae una lista limpia de correos electrónicos a partir de str o lista."""
    if not val:
        return []
    if isinstance(val, str):
        return [p.strip() for p in re.split(r"[,;]", val) if p.strip()]
    if isinstance(val, (list, tuple, set)):
        res = []
        for item in val:
            if isinstance(item, str):
                res.extend([p.strip() for p in re.split(r"[,;]", item) if p.strip()])
        return res
    return []


async def build_mime_message(
    to_email: Union[str, List[str]],
    subject: str,
    html_body: Optional[str] = None,
    text_body: Optional[str] = None,
    body: Optional[str] = None,
    from_email: Optional[str] = None,
    attachment_paths: Optional[List[str]] = None,
    cc: Optional[Union[str, List[str]]] = None,
    bcc: Optional[Union[str, List[str]]] = None,
    from_name: Optional[str] = None,
) -> MIMEMultipart:
    """
    Construye un mensaje MIMEMultipart con soporte para texto plano, HTML, CC, BCC,
    archivos adjuntos y formateo del remitente (From) con nombre descriptivo (RFC 5322).
    Si hay archivos adjuntos, estructura el mensaje en 'mixed' conteniendo una sub-parte 'alternative'
    (texto plano y HTML) y las partes adjuntas en 'MIMEBase' codificadas en Base64.
    La lectura de archivos del disco local se realiza de forma 100% no bloqueante con aiofiles.
    """
    raw_from = from_email or "no-reply@thingsboard.gateway"
    parsed_name, parsed_addr = parseaddr(raw_from)
    clean_addr = parsed_addr if parsed_addr else raw_from
    effective_name = (
        from_name.strip()
        if (from_name and isinstance(from_name, str) and from_name.strip())
        else (parsed_name.strip() if parsed_name and parsed_name.strip() else None)
    )

    if effective_name:
        sender_header = formataddr((effective_name, clean_addr), charset="utf-8")
    else:
        sender_header = clean_addr

    has_attachments = bool(attachment_paths)
    to_list = _parse_email_addresses(to_email)
    cc_list = _parse_email_addresses(cc)
    # Nota RFC 5322: BCC no se incluye en las cabeceras MIME visibles para preservar privacidad,
    # sino que se inyecta en el sobre SMTP de destinatarios (envelope recipients).

    # 1. Estructuración MIME según la presencia de archivos adjuntos
    if has_attachments:
        message = MIMEMultipart("mixed")
        body_container = MIMEMultipart("alternative")
    else:
        message = MIMEMultipart("alternative")
        body_container = message

    message["From"] = sender_header
    message["To"] = ", ".join(to_list) if to_list else str(to_email)
    if cc_list:
        message["Cc"] = ", ".join(cc_list)
    message["Subject"] = subject
    message["Date"] = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

    # 2. Resolución unificada de cuerpo (HTML / Texto / Body genérico)
    actual_html = html_body
    actual_text = text_body

    if body and not actual_html and not actual_text:
        body_str = str(body)
        if any(tag in body_str.lower() for tag in ("<html", "<p", "<div", "<br", "<table", "<body")):
            actual_html = body_str
        else:
            actual_text = body_str
            actual_html = f"<div style='font-family: Arial, sans-serif; font-size: 14px; color: #1e293b; line-height: 1.6;'>{body_str.replace(chr(10), '<br/>')}</div>"
    elif body and not actual_html:
        actual_html = f"<div style='font-family: Arial, sans-serif; font-size: 14px; color: #1e293b; line-height: 1.6;'>{str(body).replace(chr(10), '<br/>')}</div>"

    plain_content = actual_text
    if not plain_content:
        if actual_html:
            plain_content = (
                "Este correo contiene formato HTML y posibles archivos adjuntos. "
                "Por favor utilice un cliente de correo compatible con HTML para visualizarlo."
            )
        else:
            plain_content = ""

    if plain_content:
        body_container.attach(MIMEText(plain_content, "plain", "utf-8"))

    # 3. Cascarón en HTML
    if actual_html:
        body_container.attach(MIMEText(actual_html, "html", "utf-8"))

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
    to_email: Union[str, List[str]],
    subject: str,
    html_body: Optional[str] = None,
    text_body: Optional[str] = None,
    body: Optional[str] = None,
    from_email: Optional[str] = None,
    attachment_paths: Optional[List[str]] = None,
    cc: Optional[Union[str, List[str]]] = None,
    bcc: Optional[Union[str, List[str]]] = None,
    host: Optional[str] = None,
    port: int = 587,
    username: Optional[str] = None,
    password: Optional[str] = None,
    use_tls: bool = True,
    timeout: float = 30.0,
    from_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Envía un correo electrónico a través de un servidor SMTP dinámico utilizando aiosmtplib.
    No consume variables de entorno globales; todas las credenciales y parámetros de conexión
    son inyectados dinámicamente desde el worker (descifradas en memoria RAM desde MongoDB).

    Parámetros:
        to_email: Dirección o lista de destinatarios (Para).
        subject: Asunto del mensaje.
        html_body: Cuerpo del mensaje formateado en HTML.
        text_body: Cuerpo opcional en texto plano.
        body: Cuerpo genérico (soporta texto plano o tags HTML).
        from_email: Remitente (De*, por defecto toma username/sender_email de configuración).
        attachment_paths: Lista opcional de rutas a archivos locales en disco duro.
        cc: Con copia (CC).
        bcc: Con copia oculta (BCC).
        host: Host o FQDN del servidor SMTP.
        port: Puerto de conexión SMTP.
        username: Usuario para autenticación SMTP.
        password: Contraseña en texto plano (descifrada en RAM).
        use_tls: Flag para habilitar TLS/STARTTLS.
        timeout: Timeout en segundos para la conexión y envío.
        from_name: Nombre descriptivo del remitente (ej: "ThingsBoard Gateway").

    Retorna:
        Dict con los metadatos y estado del envío.
    """
    if not host:
        raise ValueError("El parámetro 'host' del servidor SMTP es obligatorio para el envío.")
    if not to_email:
        raise ValueError("El destinatario 'to_email' es obligatorio.")

    to_list = _parse_email_addresses(to_email)
    cc_list = _parse_email_addresses(cc)
    bcc_list = _parse_email_addresses(bcc)

    all_envelope_recipients = to_list + cc_list + bcc_list
    if not all_envelope_recipients:
        raise ValueError("Se requiere al menos un destinatario válido ('to_email').")

    raw_sender = from_email or username or "no-reply@thingsboard.gateway"
    parsed_name, parsed_addr = parseaddr(raw_sender)
    clean_sender = parsed_addr if parsed_addr else raw_sender
    effective_from_name = (
        from_name.strip()
        if (from_name and isinstance(from_name, str) and from_name.strip())
        else (parsed_name.strip() if parsed_name and parsed_name.strip() else None)
    )

    message = await build_mime_message(
        to_email=to_email,
        subject=subject,
        html_body=html_body,
        text_body=text_body,
        body=body,
        from_email=clean_sender,
        from_name=effective_from_name,
        attachment_paths=attachment_paths,
        cc=cc,
        bcc=bcc,
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

    # Cálculo dinámico de timeout para permitir transmisión de adjuntos de gran tamaño
    total_att_size_bytes = 0
    if attachment_paths:
        for ap in attachment_paths:
            try:
                if os.path.exists(ap):
                    total_att_size_bytes += os.path.getsize(ap)
            except Exception:
                pass

    if total_att_size_bytes > 0:
        att_mb = total_att_size_bytes / (1024 * 1024)
        effective_timeout = max(timeout, 60.0 + (att_mb * 15.0), 120.0)
    else:
        effective_timeout = timeout

    smtp_kwargs: Dict[str, Any] = {
        "hostname": host,
        "port": port,
        "timeout": effective_timeout,
        "use_tls": smtp_use_tls,
        "start_tls": smtp_start_tls,
    }

    if username and password:
        smtp_kwargs["username"] = username
        smtp_kwargs["password"] = password

    formatted_from = formataddr((effective_from_name, clean_sender), charset="utf-8") if effective_from_name else clean_sender

    logger.info(
        f"[EmailService] Enviando correo asíncrono dinámico a {len(all_envelope_recipients)} destinatarios "
        f"(Para: '{', '.join(to_list)}', CC: {len(cc_list)}, BCC: {len(bcc_list)}) vía SMTP {host}:{port} "
        f"(De: '{formatted_from}', Sobre: '{clean_sender}', TLS: {use_tls}, STARTTLS: {smtp_start_tls}, SMTPS: {smtp_use_tls}, Adjuntos: {len(attachment_paths or [])}, Timeout: {effective_timeout:.1f}s)..."
    )

    # Envío asíncrono puro sin bloqueo de I/O entregando al sobre SMTP con dirección pura (clean_sender)
    try:
        response, server_response_str = await aiosmtplib.send(
            message,
            sender=clean_sender,
            recipients=all_envelope_recipients,
            **smtp_kwargs,
        )
    except aiosmtplib.errors.SMTPServerDisconnected as disc_err:
        err_msg = str(disc_err)
        logger.error(
            f"[EmailService] Conexión cerrada abruptamente por el servidor SMTP '{host}:{port}': {err_msg}."
        )
        raise aiosmtplib.errors.SMTPServerDisconnected(
            f"El servidor SMTP '{host}:{port}' cerró la conexión abruptamente ({err_msg}). "
            f"Causas habituales: "
            f"1) Proveedores como Gmail o Microsoft 365 requieren una 'Contraseña de Aplicación' (App Password) de 16 dígitos en vez de la contraseña normal. "
            f"2) La autenticación básica SMTP AUTH está desactivada en la organización/buzón (frecuente en Office 365 / Exchange Online). "
            f"3) Desajuste de puerto o TLS: intente alternar entre puerto 587 (STARTTLS) y puerto 465 (SMTPS directo) con use_tls=True. "
            f"4) Credenciales incorrectas o bloqueo defensivo por firewall/antispam en el servidor de correo."
        ) from disc_err

    sent_at = datetime.now(timezone.utc).isoformat()
    logger.info(
        f"[EmailService] Correo enviado exitosamente hacia '{', '.join(to_list)}'. "
        f"Respuesta del servidor SMTP: {server_response_str}"
    )

    return {
        "status": "SENT",
        "to_email": to_list if len(to_list) > 1 else (to_list[0] if to_list else str(to_email)),
        "cc": cc_list if cc_list else None,
        "bcc": bcc_list if bcc_list else None,
        "from_email": clean_sender,
        "from_name": effective_from_name,
        "from": formatted_from,
        "subject": subject,
        "smtp_server": f"{host}:{port}",
        "attachments_count": len(attachment_paths or []),
        "server_response": str(server_response_str),
        "sent_at": sent_at,
    }
