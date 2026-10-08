import os
import sys
import json
import pytest
import asyncio
import aiosmtplib
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone
from beanie import init_beanie
from mongomock_motor import AsyncMongoMockClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.models.tb_email_config import TBEmailConfig
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.models.user import User
from core.services.email_service import (
    _parse_email_addresses,
    build_mime_message,
    send_email_async,
)
from api.endpoints.telemetry.router import HeatmapReportRequest, HeatmapEmailOptions


async def setup_mock_db():
    client = AsyncMongoMockClient()
    db = client[f"test_heatmap_email_{os.getpid()}"]
    await init_beanie(
        database=db,
        document_models=[
            TBEmailConfig,
            TBServer,
            TBTenant,
            TBBackup,
            User,
        ]
    )


# ==============================================================================
# 1. PRUEBAS DEL SERVICIO DE CORREO: PARSER, MIME Y SOBRE SMTP (CC / BCC / BODY)
# ==============================================================================

def test_parse_email_addresses():
    """Verifica la normalización de correos desde string (coma/punto y coma) o listas."""
    assert _parse_email_addresses(None) == []
    assert _parse_email_addresses("") == []
    assert _parse_email_addresses("user1@example.com") == ["user1@example.com"]
    assert _parse_email_addresses("user1@example.com, user2@example.com ; user3@example.com") == [
        "user1@example.com", "user2@example.com", "user3@example.com"
    ]
    assert _parse_email_addresses(["user1@example.com", "user2@example.com, user3@example.com"]) == [
        "user1@example.com", "user2@example.com", "user3@example.com"
    ]


@pytest.mark.asyncio
async def test_build_mime_message_with_cc_bcc_and_body(tmp_path):
    """
    Verifica que build_mime_message configure adecuadamente:
    - From, To, Subject, Cc
    - Que Bcc NO sea visible en las cabeceras MIME (RFC 5322)
    - Que el cuerpo genérico (body) se formatee y adjuntos se procesen.
    """
    sample_file = tmp_path / "test_report.pdf"
    sample_file.write_bytes(b"%PDF-1.4 test report content")

    msg = await build_mime_message(
        to_email=["dest1@empresa.com", "dest2@empresa.com"],
        subject="Reporte Mensual de Heatmap Julio 2026",
        body="Estimado cliente,\nAdjunto encontrará el mapa de calor.",
        from_email="notificaciones@tkme.cloud",
        cc=["copia1@empresa.com", "copia2@empresa.com"],
        bcc=["oculto@empresa.com"],
        attachment_paths=[str(sample_file)]
    )

    assert msg["From"] == "notificaciones@tkme.cloud"
    assert msg["To"] == "dest1@empresa.com, dest2@empresa.com"
    assert msg["Subject"] == "Reporte Mensual de Heatmap Julio 2026"
    assert msg["Cc"] == "copia1@empresa.com, copia2@empresa.com"
    # Privacidad estricta: Bcc no debe estar en los headers MIME
    assert "Bcc" not in msg

    # Verificar que el mensaje es multipart con attachments
    assert msg.is_multipart()
    parts = msg.get_payload()
    # Debe tener contenedor del cuerpo + adjunto
    assert len(parts) >= 2


@pytest.mark.asyncio
async def test_send_email_async_envelope_recipients():
    """
    Verifica que send_email_async envíe a todos los destinatarios del sobre SMTP
    (To + Cc + Bcc) y retorne los metadatos correspondientes.
    """
    with patch("aiosmtplib.send", AsyncMock(return_value=(MagicMock(), "250 2.0.0 OK Message accepted"))) as mock_send:
        res = await send_email_async(
            to_email="destinatario@empresa.com",
            subject="Asunto de Prueba",
            body="Cuerpo de prueba",
            from_email="origen@empresa.com",
            cc="copia@empresa.com",
            bcc="oculto@empresa.com",
            host="smtp.servidor.com",
            port=587,
            username="user_smtp",
            password="secret_password"
        )

        assert res["status"] == "SENT"
        assert res["from_email"] == "origen@empresa.com"
        assert res["to_email"] == "destinatario@empresa.com"
        assert res["cc"] == ["copia@empresa.com"]
        assert res["bcc"] == ["oculto@empresa.com"]

        # Verificar qué recipients recibió aiosmtplib.send en el sobre SMTP:
        mock_send.assert_called_once()
        _, kwargs = mock_send.call_args
        envelope_recipients = kwargs.get("recipients")
        assert "destinatario@empresa.com" in envelope_recipients
        assert "copia@empresa.com" in envelope_recipients
        assert "oculto@empresa.com" in envelope_recipients
        assert len(envelope_recipients) == 3


# ==============================================================================
# 2. PRUEBA DE LA TAREA ARQ GENERATE_MONTHLY_HEATMAP CON ENVÍO DE CORREO
# ==============================================================================

@pytest.mark.asyncio
async def test_generate_monthly_heatmap_task_email_delivery_active(tmp_path):
    """
    Verifica que la tarea generate_monthly_heatmap_task:
    1. Si send_email=True, consulte TBEmailConfig de MongoDB.
    2. Utilice 'from_email' configurado o custom.
    3. Adjunte el último archivo creado ({job_id}_{tenant}_heatmap.pdf).
    4. Envíe el correo con send_email_async preservando el archivo en disco (no borrado).
    5. Retorne 'email_sent': True y detalles de entrega.
    """
    # 1. Configurar servidor SMTP en MongoDB
    await setup_mock_db()
    email_cfg = TBEmailConfig(
        singleton_key="global_smtp_config",
        host="smtp.testserver.com",
        port=587,
        username="smtp_user@tkme.cloud",
        sender_email="notificaciones@tkme.cloud",
        sender_name="TKmE Cloud",
        use_tls=True,
        is_active=True
    )
    await email_cfg.set_password("MockAppPassword2026!")
    await email_cfg.insert()

    # 2. Configurar TBServer y TBTenant
    server = TBServer(
        name="TestServerHeatmap",
        base_url="https://tb-test.tkme.cloud",
        rate_limit_rpm=600,
        username="sysadmin@thingsboard.org"
    )
    server.set_password("SysPass123!")
    server.set_tokens("sys_jwt_token", "sys_refresh_token")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Prueba-Correo",
        username="tenantadmin@tkme.cloud",
        custom_metadata={"heatmap_config": ["temperatura"]}
    )
    tenant.set_password("TenantPass123!")
    tenant.set_tokens("tenant_jwt_token", "tenant_refresh_token")
    await tenant.insert()

    mock_redis = AsyncMock()
    mock_redis.expire = AsyncMock(return_value=True)
    mock_redis.publish = AsyncMock(return_value=1)
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=True)
    mock_redis.hdel = AsyncMock(return_value=True)

    fake_devices = {
        "data": [
            {"id": {"id": "dev-001"}, "name": "Dispositivo-Tanque"}
        ],
        "hasNext": False
    }

    fake_attrs = [
        {"key": "heatmap_active", "value": True}
    ]

    fake_telem = {
        "temperatura": [
            {"ts": 1785542400000, "value": "24.5"}
        ]
    }

    ctx = {"job_id": "job_email_test_123", "job_try": 1, "redis": mock_redis}

    payload = {
        "tenant_id": str(tenant.id),
        "user_id": "user_operador_01",
        "keys": ["temperatura"],
        "send_email": True,
        "to_email": "cliente@empresa.com",
        "email_subject": "Reporte de Mapa de Calor - Test",
        "email_cc": ["supervisor@empresa.com"],
        "email_bcc": ["auditor@empresa.com"],
        "email_body": "<p>Estimado cliente, se adjunta el reporte solicitado.</p>"
    }

    from workers.tasks import generate_monthly_heatmap_task

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", AsyncMock(return_value=fake_telem)), \
         patch("workers.tasks.generate_heatmap_report_pdf", AsyncMock(return_value=True)), \
         patch("workers.tasks.send_email_async", AsyncMock(return_value={"status": "SENT", "to_email": "cliente@empresa.com"})) as mock_send_email:

        # Simular que el archivo PDF existe en disco antes de medir tamaño
        pdf_path = os.path.join("backups", "heatmaps", "job_email_test_123_Tenant-Prueba-Correo_heatmap.pdf")
        os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
        with open(pdf_path, "wb") as f:
            f.write(b"%PDF-1.4 test heatmap pdf data")

        try:
            result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)

            assert result["status"] == "SUCCESS"
            assert result["email_sent"] is True
            assert result["email_delivery"]["status"] == "SENT"
            assert result["file_name"] == "job_email_test_123_Tenant-Prueba-Correo_heatmap.pdf"

            # Validar llamada a send_email_async
            mock_send_email.assert_called_once()
            _, call_kwargs = mock_send_email.call_args

            assert call_kwargs["to_email"] == "cliente@empresa.com"
            assert call_kwargs["from_email"] == "notificaciones@tkme.cloud"
            assert call_kwargs["subject"] == "Reporte de Mapa de Calor - Test"
            assert call_kwargs["cc"] == ["supervisor@empresa.com"]
            assert call_kwargs["bcc"] == ["auditor@empresa.com"]
            assert call_kwargs["attachment_paths"] == [pdf_path]
            # El archivo en disco debe conservarse
            assert os.path.exists(pdf_path)

        finally:
            if os.path.exists(pdf_path):
                os.remove(pdf_path)


@pytest.mark.asyncio
async def test_generate_monthly_heatmap_task_email_delivery_inactive():
    """
    Verifica que si send_email=False, la tarea no intente enviar ningún correo.
    """
    await setup_mock_db()
    server = TBServer(
        name="TestServerHeatmap2",
        base_url="https://tb-test.tkme.cloud",
        username="sysadmin@thingsboard.org"
    )
    server.set_password("SysPass123!")
    server.set_tokens("sys_jwt_token", "sys_refresh_token")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Sin-Correo",
        username="tenantadmin@tkme.cloud"
    )
    tenant.set_password("TenantPass123!")
    tenant.set_tokens("tenant_jwt_token", "tenant_refresh_token")
    await tenant.insert()

    mock_redis = AsyncMock()
    ctx = {"job_id": "job_no_email_456", "job_try": 1, "redis": mock_redis}

    payload = {
        "tenant_id": str(tenant.id),
        "send_email": False
    }

    from workers.tasks import generate_monthly_heatmap_task

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value={"data": [], "hasNext": False})), \
         patch("workers.tasks.generate_heatmap_report_pdf", AsyncMock(return_value=True)), \
         patch("workers.tasks.send_email_async", AsyncMock()) as mock_send_email:

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)

        assert result["status"] == "SUCCESS"
        assert result["email_sent"] is False
        assert result["email_delivery"] is None
        mock_send_email.assert_not_called()


@pytest.mark.asyncio
async def test_generate_monthly_heatmap_task_email_failure_preserves_pdf_and_success_status(tmp_path):
    """
    Verifica que si el servidor SMTP falla (ej: SMTPServerDisconnected / Unexpected EOF),
    la tarea NO falle por completo: el reporte PDF generado y catalogado se preserva
    y el resultado refleja 'email_sent': False con el detalle del error SMTP.
    """
    import aiosmtplib
    from workers.tasks import generate_monthly_heatmap_task

    server = TBServer(
        name="Test Server Heatmap Fail",
        base_url="https://tb.test.com",
        rate_limit_rpm=60
    )
    server.set_password("SysPass123!")
    server.set_tokens("sys_jwt", "sys_refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Heatmap-Fail",
        username="admin@test.com"
    )
    tenant.set_password("TenantPass123!")
    tenant.set_tokens("tenant_jwt", "tenant_refresh")
    await tenant.insert()

    mock_email_config = MagicMock()
    mock_email_config.host = "smtp.test.com"
    mock_email_config.port = 587
    mock_email_config.username = "sender@test.com"
    mock_email_config.sender_email = "sender@test.com"
    mock_email_config.use_tls = True
    mock_email_config.get_password = AsyncMock(return_value="plain_secret")

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.delete = AsyncMock(return_value=True)
    mock_redis.publish = AsyncMock(return_value=1)
    mock_redis.hset = AsyncMock(return_value=1)
    mock_redis.hdel = AsyncMock(return_value=1)

    job_id = "job_email_fail_789"
    ctx = {"job_id": job_id, "job_try": 1, "redis": mock_redis}

    payload = {
        "tenant_id": str(tenant.id),
        "send_email": True,
        "to_email": "gerencia@empresa.com",
        "subject": "Reporte Heatmap con Fallo SMTP Simulado"
    }

    simulated_error = aiosmtplib.errors.SMTPServerDisconnected("Unexpected EOF received")

    with patch("workers.tasks.TBEmailConfig.get_singleton", AsyncMock(return_value=mock_email_config)), \
         patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value={"data": [], "hasNext": False})), \
         patch("workers.tasks.generate_heatmap_report_pdf", AsyncMock(return_value=True)), \
         patch("workers.tasks.send_email_async", AsyncMock(side_effect=simulated_error)):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)

        # La tarea debe culminar como SUCCESS para no descartar el PDF generado
        assert result["status"] == "SUCCESS"
        assert result["email_sent"] is False
        assert result["email_delivery"] is not None
        assert result["email_delivery"]["status"] == "FAILED"
        assert "Unexpected EOF received" in result["email_delivery"]["error"]
        assert result["file_name"].endswith(".pdf")


# ==============================================================================
# 3. PRUEBA DE VALIDACIÓN EN FASTAPI DTO (HeatmapReportRequest)
# ==============================================================================

def test_heatmap_report_request_dto_email_fields():
    """Verifica que HeatmapReportRequest acepte las opciones de correo electrónico."""
    req = HeatmapReportRequest(
        tenant_id="66aa1234567890abcdef1234",
        send_email=True,
        to_email="operador@empresa.com",
        subject="Reporte Mensual",
        cc=["copia@empresa.com"],
        bcc=["oculto@empresa.com"],
        body="Cuerpo del reporte",
        from_email="custom_sender@tkme.cloud"
    )
    assert req.send_email is True
    assert req.to_email == "operador@empresa.com"
    assert req.subject == "Reporte Mensual"
    assert req.cc == ["copia@empresa.com"]
    assert req.bcc == ["oculto@empresa.com"]
    assert req.body == "Cuerpo del reporte"
    assert req.from_email == "custom_sender@tkme.cloud"

    # Soporte mediante email_options anidado
    req_nested = HeatmapReportRequest(
        tenant_id="66aa1234567890abcdef1234",
        email_options=HeatmapEmailOptions(
            enabled=True,
            to_email=["dest1@empresa.com", "dest2@empresa.com"],
            subject="Reporte Vía Opciones Anidadas",
            body="Hola mundo"
        )
    )
    assert req_nested.email_options.enabled is True
    assert req_nested.email_options.to_email == ["dest1@empresa.com", "dest2@empresa.com"]


# ==============================================================================
# 4. PRUEBAS DE INTERPOLACIÓN DINÁMICA DE ASUNTO Y CUERPO ({mes año})
# ==============================================================================

def test_interpolate_heatmap_placeholders_unit():
    """Verifica que la función interpolate_heatmap_placeholders reemplace correctamente todos los comodines."""
    from workers.tasks import interpolate_heatmap_placeholders

    # 1. Comodín principal {mes año} y variantes
    assert interpolate_heatmap_placeholders(
        "Reporte Mensual de Mapas de Calor - {mes año}",
        mes_nombre="Agosto",
        anio=2026,
        period="2026-08",
        tenant="Tenant Alpha"
    ) == "Reporte Mensual de Mapas de Calor - Agosto 2026"

    # Variante sin tilde: {mes ano}
    assert interpolate_heatmap_placeholders(
        "Reporte Mensual de Mapas de Calor - {mes ano}",
        mes_nombre="Agosto",
        anio=2026,
        period="2026-08",
        tenant="Tenant Alpha"
    ) == "Reporte Mensual de Mapas de Calor - Agosto 2026"

    # Variante con guion bajo: {mes_año}
    assert interpolate_heatmap_placeholders(
        "Reporte_{mes_año}",
        mes_nombre="Septiembre",
        anio=2026,
        period="2026-09",
        tenant="Tenant Alpha"
    ) == "Reporte_Septiembre 2026"

    # Comodines individuales: {mes}, {año}, {ano}, {year}, {period}, {tenant}
    assert interpolate_heatmap_placeholders(
        "Reporte de {mes} del {año} para {tenant} (Período: {period})",
        mes_nombre="Julio",
        anio=2026,
        period="2026-07",
        tenant="Industrias Acme"
    ) == "Reporte de Julio del 2026 para Industrias Acme (Período: 2026-07)"

    # Mayúsculas e insensibilidad a mayúsculas/minúsculas
    assert interpolate_heatmap_placeholders(
        "REPORTE - {MES AÑO} - {TENANT}",
        mes_nombre="Agosto",
        anio=2026,
        period="2026-08",
        tenant="Empresa X"
    ) == "REPORTE - Agosto 2026 - Empresa X"

    # Strings vacíos o None
    assert interpolate_heatmap_placeholders(None, "Agosto", 2026, "2026-08", "T") is None
    assert interpolate_heatmap_placeholders("", "Agosto", 2026, "2026-08", "T") == ""


@pytest.mark.asyncio
async def test_generate_monthly_heatmap_task_interpolates_subject_and_body_in_worker(tmp_path):
    """
    Verifica que al despachar generate_monthly_heatmap_task con {mes año} en subject y body,
    el trabajador de ARQ resuelva dinámicamente el mes y año en lugar de enviar el texto literal.
    """
    await setup_mock_db()

    email_cfg = TBEmailConfig(
        singleton_key="global_smtp_config",
        host="smtp.testserver.com",
        port=587,
        username="smtp_user@tkme.cloud",
        sender_email="notificaciones@tkme.cloud",
        sender_name="TKmE Cloud",
        use_tls=True,
        is_active=True
    )
    await email_cfg.set_password("MockAppPassword2026!")
    await email_cfg.insert()

    server = TBServer(
        name="TestServerInterpolation",
        base_url="https://tb-test.tkme.cloud",
        username="sysadmin@thingsboard.org"
    )
    server.set_password("SysPass123!")
    server.set_tokens("sys_jwt_token", "sys_refresh_token")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Planta-Norte",
        username="admin_planta@tkme.cloud"
    )
    tenant.set_password("TenantPass123!")
    tenant.set_tokens("tenant_jwt_token", "tenant_refresh_token")
    await tenant.insert()

    mock_redis = AsyncMock()
    ctx = {"job_id": "job_interp_test_456", "job_try": 1, "redis": mock_redis}

    payload = {
        "tenant_id": str(tenant.id),
        "year": 2026,
        "month": 8,
        "send_email": True,
        "to_email": "gerencia@planta.com",
        "subject": "Reporte Mensual de Mapas de Calor - {mes año}",
        "body": "<p>Estimado equipo, se adjunta el reporte de {mes año} para {tenant} (período {periodo}).</p>"
    }

    from workers.tasks import generate_monthly_heatmap_task

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value={"data": [], "hasNext": False})), \
         patch("workers.tasks.generate_heatmap_report_pdf", AsyncMock(return_value=True)), \
         patch("workers.tasks.send_email_async", AsyncMock(return_value={"status": "SENT", "to_email": "gerencia@planta.com"})) as mock_send_email:

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)

        assert result["status"] == "SUCCESS"
        assert result["email_sent"] is True

        # Validar que send_email_async recibió el asunto y cuerpo con los comodines reemplazados
        mock_send_email.assert_called_once()
        _, call_kwargs = mock_send_email.call_args

        sent_subject = call_kwargs["subject"]
        sent_body = call_kwargs["html_body"]

        # NO debe contener el comodín literal {mes año}
        assert "{mes año}" not in sent_subject
        assert "{mes año}" not in sent_body

        # Debe contener el valor interpolado "Agosto 2026"
        assert sent_subject == "Reporte Mensual de Mapas de Calor - Agosto 2026"
        assert "Agosto 2026" in sent_body
        assert "Planta-Norte" in sent_body
        assert "2026-08" in sent_body


@pytest.mark.asyncio
async def test_generate_monthly_heatmap_task_interpolates_subject_based_on_telemetry_minus_one_month():
    """
    Verifica el requerimiento exacto:
    'la fecha debe de estar en función de la telemetría leída menos 1 mes.
    Por ejemplo, hay data del 1 de agosto, entonces el título debería de ser "Reporte Mensual de Mapas de Calor - Julio 2026"'
    """
    await setup_mock_db()

    email_cfg = TBEmailConfig(
        singleton_key="global_smtp_config",
        host="smtp.testserver.com",
        port=587,
        username="smtp_user@tkme.cloud",
        sender_email="notificaciones@tkme.cloud",
        sender_name="TKmE Cloud",
        use_tls=True,
        is_active=True
    )
    await email_cfg.set_password("MockAppPassword2026!")
    await email_cfg.insert()

    server = TBServer(
        name="TestServerTelemetryMinusMonth",
        base_url="https://tb-test.tkme.cloud",
        username="sysadmin@thingsboard.org"
    )
    server.set_password("SysPass123!")
    server.set_tokens("sys_jwt_token", "sys_refresh_token")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Agosto",
        username="admin_agosto@tkme.cloud",
        custom_metadata={"heatmap_config": ["temperatura"]}
    )
    tenant.set_password("TenantPass123!")
    tenant.set_tokens("tenant_jwt_token", "tenant_refresh_token")
    await tenant.insert()

    fake_devices = {
        "data": [
            {"id": {"id": "dev-aug-001"}, "name": "Dispositivo-Sensor-01"}
        ],
        "hasNext": False
    }

    fake_attrs = [
        {"key": "heatmap_active", "value": True}
    ]

    # Data con fecha del 1 de agosto de 2026 (timestamp: 1785607200000 = 2026-08-01 12:00:00 local)
    fake_telem = {
        "temperatura": [
            {"ts": 1785607200000, "value": "23.8"}
        ]
    }

    mock_redis = AsyncMock()
    ctx = {"job_id": "job_telem_minus_1_month_789", "job_try": 1, "redis": mock_redis}

    payload = {
        "tenant_id": str(tenant.id),
        "keys": ["temperatura"],
        "send_email": True,
        "to_email": "operaciones@empresa.com",
        "subject": "Reporte Mensual de Mapas de Calor - {mes año}",
        "body": "<p>Reporte mensual de mapas de calor para {mes año} ({periodo}).</p>"
    }

    from workers.tasks import generate_monthly_heatmap_task

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", AsyncMock(return_value=fake_telem)), \
         patch("workers.tasks.generate_heatmap_report_pdf", AsyncMock(return_value=True)), \
         patch("workers.tasks.send_email_async", AsyncMock(return_value={"status": "SENT", "to_email": "operaciones@empresa.com"})) as mock_send_email:

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)

        assert result["status"] == "SUCCESS"
        assert result["email_sent"] is True

        mock_send_email.assert_called_once()
        _, call_kwargs = mock_send_email.call_args

        sent_subject = call_kwargs["subject"]
        sent_body = call_kwargs["html_body"]

        # Telemetría del 1 de agosto -> Mes del reporte = Julio 2026 (telemetría menos 1 mes)
        assert sent_subject == "Reporte Mensual de Mapas de Calor - Julio 2026"
        assert "Julio 2026" in sent_body
        assert "2026-07" in sent_body


# ==============================================================================
# 5. PRUEBAS DE RESOLUCIÓN RESILIENTE DE LLAVES Y FALLBACK DE RANGO DE TELEMETRÍA
# ==============================================================================

@pytest.mark.asyncio
async def test_generate_monthly_heatmap_task_telemetry_key_resolution_and_fallback(tmp_path):
    """
    Verifica que:
    1. Si whitelist contiene 'HM_AVG_Humedad' pero el dispositivo tiene 'AVG_Humedad', resuelva la llave alternativa.
    2. Si el rango estricto del mes retorna 0 puntos, el fallback extendido recupere el punto precalculado.
    3. La matriz de datos resultante no sea 'NA' sino la matriz con datos reales.
    """
    await setup_mock_db()

    server = TBServer(
        name="TestServerFallback",
        base_url="https://tb-test.tkme.cloud",
        username="sysadmin@thingsboard.org"
    )
    server.set_password("SysPass123!")
    server.set_tokens("sys_jwt_token", "sys_refresh_token")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Fallback",
        username="tenant_fallback@tkme.cloud",
        custom_metadata={"heatmap_config": ["HM_AVG_Humedad"]}
    )
    tenant.set_password("TenantPass123!")
    tenant.set_tokens("tenant_jwt_token", "tenant_refresh_token")
    await tenant.insert()

    fake_devices = {
        "data": [
            {"id": {"id": "dev-fallback-01"}, "name": "Sensor_TH_Fallback"}
        ],
        "hasNext": False
    }

    fake_attrs = [
        {"key": "heatmap_active", "value": True}
    ]

    # El dispositivo sólo tiene 'AVG_Humedad' (sin prefijo HM_)
    dev_keys = ["AVG_Humedad", "temperature"]

    # Simular que el rango estricto del mes no retorna datos, pero el rango extendido sí
    precalc_matrix = [[55.0 for _ in range(24)] for _ in range(31)]
    precalc_val = json.dumps({
        "data": precalc_matrix,
        "rules": [{"limit": 60, "operator": ">=", "color": "#E53935"}]
    })

    call_count = {"count": 0}

    async def mock_get_telemetry(entity_id, keys, start_ts, end_ts, limit=100, token=None):
        call_count["count"] += 1
        # Primera llamada (rango estricto del mes): retorna vacío
        if call_count["count"] == 1:
            return {}
        # Siguiente llamada (fallback extendido): retorna el punto con la llave resuelta
        return {
            keys: [
                {"ts": 1789458265716, "value": precalc_val}
            ]
        }

    mock_redis = AsyncMock()
    ctx = {"job_id": "job_fallback_heatmap_456", "job_try": 1, "redis": mock_redis}
    payload = {
        "tenant_id": str(tenant.id),
        "send_email": False
    }

    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    from workers.tasks import generate_monthly_heatmap_task

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=dev_keys)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", side_effect=mock_get_telemetry), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)

        assert result["status"] == "SUCCESS"
        assert len(captured_sections) == 1
        sec = captured_sections[0]
        assert sec["metric_name"] == "AVG_Humedad"  # Llave resuelta correctamente
        # Verificar que la matriz no sea 'NA'
        assert sec["data"][0][0] == 55.0
        assert sec["rules"][0]["limit"] == 60


@pytest.mark.asyncio
async def test_generate_monthly_heatmap_task_email_retry_on_timeout():
    """
    Verifica que si send_email_async experimenta SMTPReadTimeoutError,
    la tarea reintente y logre el envío en el siguiente intento.
    """
    await setup_mock_db()

    email_cfg = TBEmailConfig(
        singleton_key="global_smtp_config",
        host="smtp.testserver.com",
        port=587,
        username="smtp_user@tkme.cloud",
        sender_email="notificaciones@tkme.cloud",
        use_tls=True,
        is_active=True
    )
    await email_cfg.set_password("MockAppPassword2026!")
    await email_cfg.insert()

    server = TBServer(
        name="TestServerRetry",
        base_url="https://tb-test.tkme.cloud",
        username="sysadmin@thingsboard.org"
    )
    server.set_password("SysPass123!")
    server.set_tokens("sys_jwt_token", "sys_refresh_token")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Retry",
        username="tenant_retry@tkme.cloud",
        custom_metadata={"heatmap_config": ["temperatura"]}
    )
    tenant.set_password("TenantPass123!")
    tenant.set_tokens("tenant_jwt_token", "tenant_refresh_token")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-retry-01"}, "name": "Sensor_Retry"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]
    fake_telem = {"temperatura": [{"ts": 1785564000000, "value": "21.0"}]}

    mock_redis = AsyncMock()
    ctx = {"job_id": "job_retry_email_789", "job_try": 1, "redis": mock_redis}
    payload = {
        "tenant_id": str(tenant.id),
        "keys": ["temperatura"],
        "send_email": True,
        "to_email": "destino_retry@empresa.com"
    }

    send_attempts = {"count": 0}

    async def mock_send_with_retry(**kwargs):
        send_attempts["count"] += 1
        if send_attempts["count"] == 1:
            raise aiosmtplib.errors.SMTPReadTimeoutError("Timed out waiting for server response")
        return {"status": "SENT", "to_email": kwargs.get("to_email")}

    from workers.tasks import generate_monthly_heatmap_task

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=["temperatura"])), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", AsyncMock(return_value=fake_telem)), \
         patch("workers.tasks.generate_heatmap_report_pdf", AsyncMock(return_value=True)), \
         patch("workers.tasks.send_email_async", side_effect=mock_send_with_retry), \
         patch("workers.tasks._heartbeat_server_lock", AsyncMock()), \
         patch("workers.tasks.asyncio.sleep", AsyncMock(return_value=None)):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)

        assert result["status"] == "SUCCESS"
        assert result["email_sent"] is True
        assert send_attempts["count"] == 2


@pytest.mark.asyncio
async def test_send_email_async_dynamic_timeout_with_attachments(tmp_path):
    """
    Verifica que send_email_async escale dinámicamente el timeout a >= 120s cuando hay adjuntos.
    """
    sample_file = tmp_path / "large_report.pdf"
    sample_file.write_bytes(b"0" * (2 * 1024 * 1024))  # 2 MB

    with patch("aiosmtplib.send", AsyncMock(return_value=(None, "250 OK"))) as mock_send:
        res = await send_email_async(
            to_email="test_timeout@empresa.com",
            subject="Prueba Timeout Dinamico",
            body="Mensaje con adjunto",
            attachment_paths=[str(sample_file)],
            host="smtp.servidor.com",
            timeout=30.0
        )
        assert res["status"] == "SENT"
        mock_send.assert_called_once()
        _, kwargs = mock_send.call_args
        # El timeout efectivo debe haber escalado a >= 120.0s debido al archivo adjunto
        assert kwargs.get("timeout") >= 120.0



