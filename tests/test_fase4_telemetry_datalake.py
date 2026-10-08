"""
Test Suite Fase 4: Servicios de Telemetría, Data Lake, Reportes y Endpoints Asociados
Cubre:
- core/services/ssh_service.py
- core/services/email_service.py
- core/services/telegram_service.py
- core/services/alert_dispatcher.py
- core/services/hierarchical_suppression_service.py
- core/services/task_registry.py
- core/services/system_info_service.py
- core/services/incremental_backup_service.py
- core/services/excel_report_service.py
- core/services/heatmap_report_service.py
- core/services/telemetry_service.py
- api/endpoints/telemetry/router.py
- api/endpoints/devices/router.py
"""

import os
import io
import json
import asyncio
import tempfile
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx
import asyncssh
import pandas as pd
import openpyxl
from fastapi import HTTPException, status
from beanie import init_beanie, PydanticObjectId
from mongomock_motor import AsyncMongoMockClient

from core.models.user import User
from core.models.tb_server import TBServer, InstallationType, SSHAuthMethod
from core.models.tb_tenant import TBTenant
from core.models.tb_node import TBNode
from core.models.tb_backup import TBBackup
from core.models.tb_scheduled_task import TBScheduledTask
from core.models.tb_email_config import TBEmailConfig
from core.models.audit_log import AuditLog

# Services
from core.services.ssh_service import (
    validate_ssh_command,
    execute_ssh_command_on_server,
    ALLOWED_SSH_COMMANDS,
)
from core.services.email_service import (
    _parse_email_addresses,
    build_mime_message,
    send_email_async,
)
from core.services.telegram_service import (
    escape_html_text,
    format_alert_message,
    send_telegram_message,
)
from core.services.alert_dispatcher import (
    calculate_alert_hash,
    get_alert_lock_key,
    dispatch_debounced_alert,
)
from core.services.hierarchical_suppression_service import (
    check_parent_gateway_status,
)
from core.services.task_registry import (
    get_user_stream_channel,
    get_user_registry_key,
    publish_task_event,
)
from core.services.system_info_service import (
    _normalize_system_info,
    _build_metric_point,
    refresh_server_tokens_in_db,
    collect_server_system_info,
)
from core.services.incremental_backup_service import (
    calculate_previous_month_boundaries,
    is_retryable_http_exception,
)
from core.services.excel_report_service import (
    sanitize_sheet_name,
    _autofit_worksheet_columns,
    _build_dataframe_from_telemetry,
)
from core.services.heatmap_report_service import (
    HeatmapRule,
    parse_heatmap_rules,
    ensure_tkme_logo_badge,
)
from core.services.telemetry_service import (
    sanitize_name,
    publish_task_status,
    publish_task_status_sync,
    get_month_intervals,
    calculate_telemetry_delta_plan,
)

# Routers & Schemas
from api.endpoints.telemetry.router import (
    download_telemetry,
    generate_excel_report,
    generate_heatmap_report_endpoint as generate_heatmap_report,
    list_tenant_backups,
    download_file,
    delete_tenant_backup,
    device_status_webhook,
    DownloadTelemetryRequest,
    ExcelReportRequest,
    HeatmapReportRequest,
    DeviceStatusWebhookRequest,
)
from api.endpoints.devices.router import (
    list_tenant_devices,
    get_device_details,
    list_sites_with_devices,
    provision_devices as batch_provision_devices,
    create_device_physical_relation,
    DeviceProvisionBatchRequest,
    DeviceProvisionRequest,
    DeviceRelationRequest,
)


async def init_mock_db(db_name: str = "fase4_test_db"):
    client = AsyncMongoMockClient()
    db = client[db_name]
    await init_beanie(
        database=db,
        document_models=[
            User,
            TBServer,
            TBTenant,
            TBNode,
            TBBackup,
            TBScheduledTask,
            TBEmailConfig,
            AuditLog,
        ],
    )
    return db


# ==============================================================================
# 1. CORE/SERVICES/SSH_SERVICE.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_ssh_service_validation_and_execution():
    await init_mock_db("ssh_service_tests")
    # 1. Validación de comandos permitidos
    assert validate_ssh_command("uptime") == "uptime"
    assert validate_ssh_command("  df -h  ") == "df -h"
    assert validate_ssh_command("systemctl status thingsboard") == "systemctl status thingsboard"

    # 2. Comando vacío
    with pytest.raises(HTTPException) as exc_empty:
        validate_ssh_command("   ")
    assert exc_empty.value.status_code == status.HTTP_400_BAD_REQUEST

    # 3. Metacaracteres prohibidos
    for meta in [";", "&", "|", "`", "$", ">", "<", "\n", "\0", "{"]:
        with pytest.raises(HTTPException) as exc_meta:
            validate_ssh_command(f"uptime {meta} whoami")
        assert exc_meta.value.status_code == status.HTTP_400_BAD_REQUEST

    # 4. Comando no en lista blanca
    with pytest.raises(HTTPException) as exc_unauth:
        validate_ssh_command("rm -rf /")
    assert exc_unauth.value.status_code == status.HTTP_400_BAD_REQUEST

    # 5. Ejecución en servidor con configuraciones incompletas
    srv = TBServer(name="SRV_SSH", base_url="https://tb.com")
    
    # Falta host
    with pytest.raises(HTTPException) as exc_host:
        await execute_ssh_command_on_server(srv, "uptime")
    assert exc_host.value.status_code == status.HTTP_400_BAD_REQUEST

    # Falta username
    srv.base_url = "https://192.168.1.10:8080"
    with pytest.raises(HTTPException) as exc_user:
        await execute_ssh_command_on_server(srv, "uptime")
    assert exc_user.value.status_code == status.HTTP_400_BAD_REQUEST

    # Falta auth_method
    srv.ssh_username = "ubuntu"
    with pytest.raises(HTTPException) as exc_auth:
        await execute_ssh_command_on_server(srv, "uptime")
    assert exc_auth.value.status_code == status.HTTP_400_BAD_REQUEST

    # Auth method PASSWORD pero sin contraseña
    srv.ssh_auth_method = SSHAuthMethod.PASSWORD
    with pytest.raises(HTTPException) as exc_pwd:
        await execute_ssh_command_on_server(srv, "uptime")
    assert exc_pwd.value.status_code == status.HTTP_400_BAD_REQUEST

    # Auth method PEM_KEY sin pem file
    srv.ssh_auth_method = SSHAuthMethod.PEM_KEY
    with pytest.raises(HTTPException) as exc_pem:
        await execute_ssh_command_on_server(srv, "uptime")
    assert exc_pem.value.status_code == status.HTTP_400_BAD_REQUEST

    # 6. Ejecución exitosa con mock de asyncssh
    srv.ssh_auth_method = SSHAuthMethod.PASSWORD
    srv.set_ssh_credentials(ssh_password="ValidPassword123!")

    mock_conn = AsyncMock()
    mock_run_result = MagicMock(exit_status=0, stdout="12:00 up 10 days", stderr="")
    mock_conn.run = AsyncMock(return_value=mock_run_result)

    class MockAsyncSSHContext:
        async def __aenter__(self):
            return mock_conn
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    with patch("asyncssh.connect", return_value=MockAsyncSSHContext()):
        res = await execute_ssh_command_on_server(srv, "uptime", timeout_seconds=10)
        assert res["exit_status"] == 0
        assert "up 10 days" in res["stdout"]
        assert res["command"] == "uptime"

    # 7. Manejo de excepciones (Timeout, PermissionDenied, Error)
    with patch("asyncssh.connect", side_effect=asyncio.TimeoutError()):
        with pytest.raises(HTTPException) as exc_to:
            await execute_ssh_command_on_server(srv, "uptime")
        assert exc_to.value.status_code == status.HTTP_504_GATEWAY_TIMEOUT

    with patch("asyncssh.connect", side_effect=asyncssh.PermissionDenied(reason="Auth failed")):
        with pytest.raises(HTTPException) as exc_pd:
            await execute_ssh_command_on_server(srv, "uptime")
        assert exc_pd.value.status_code == status.HTTP_502_BAD_GATEWAY

    with patch("asyncssh.connect", side_effect=asyncssh.Error(code=1, reason="Network down")):
        with pytest.raises(HTTPException) as exc_err:
            await execute_ssh_command_on_server(srv, "uptime")
        assert exc_err.value.status_code == status.HTTP_502_BAD_GATEWAY


# ==============================================================================
# 2. CORE/SERVICES/EMAIL_SERVICE.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_email_service_all_branches():
    # 1. _parse_email_addresses
    assert _parse_email_addresses(None) == []
    assert _parse_email_addresses("a@b.com, c@d.com; e@f.com") == ["a@b.com", "c@d.com", "e@f.com"]
    assert _parse_email_addresses(["a@b.com, x@y.com", "z@w.com"]) == ["a@b.com", "x@y.com", "z@w.com"]

    # 2. build_mime_message sin adjuntos
    msg = await build_mime_message(
        to_email=["user1@tkme.com", "user2@tkme.com"],
        subject="Reporte Mensual",
        html_body="<h1>Hola</h1>",
        text_body="Hola",
        from_email="no-reply@tkme.com",
        from_name="TKmE Cloud Notifications",
        cc="supervisor@tkme.com",
    )
    assert msg["Subject"] == "Reporte Mensual"
    assert "user1@tkme.com" in msg["To"]
    assert "supervisor@tkme.com" in msg["Cc"]
    assert "TKmE Cloud Notifications" in msg["From"]

    # 3. build_mime_message con body genérico
    msg_gen = await build_mime_message(
        to_email="test@tkme.com",
        subject="Aviso",
        body="Línea 1\nLínea 2"
    )
    assert msg_gen["Subject"] == "Aviso"

    # 4. build_mime_message con adjuntos en disco
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".txt") as tmp:
        tmp.write("Contenido de prueba")
        tmp_path = tmp.name

    try:
        msg_att = await build_mime_message(
            to_email="test@tkme.com",
            subject="Con adjunto",
            text_body="Ver archivo",
            attachment_paths=[tmp_path]
        )
        assert msg_att.is_multipart()
        assert msg_att.get_content_type() == "multipart/mixed"
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    # 5. send_email_async validaciones de entrada
    with pytest.raises(ValueError, match="host"):
        await send_email_async(to_email="a@b.com", subject="Test", host=None)

    with pytest.raises(ValueError, match="to_email"):
        await send_email_async(to_email="", subject="Test", host="smtp.gmail.com")

    with pytest.raises(ValueError, match="destinatario"):
        await send_email_async(to_email="   ", subject="Test", host="smtp.gmail.com")

    # 6. send_email_async envío exitoso con mock de aiosmtplib.send
    with patch("aiosmtplib.send", AsyncMock(return_value=(None, "250 OK"))):
        res = await send_email_async(
            to_email="recipient@tkme.com",
            subject="Envío Real Mockeado",
            html_body="<p>Enviado</p>",
            host="smtp.tkme.com",
            port=587,
            username="smtp_user",
            password="secret_password",
            use_tls=True
        )
        assert res["status"] == "SENT"
        assert res["from_name"] == "TKmE Cloud Notifications" or res["from_email"] == "smtp_user"


# ==============================================================================
# 3. CORE/SERVICES/TELEGRAM_SERVICE.PY & ALERT_DISPATCHER.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_telegram_service_and_alert_dispatcher():
    # 1. escape_html_text
    assert escape_html_text("A & B < C > D") == "A &amp; B &lt; C &gt; D"

    # 2. format_alert_message con diferentes severidades
    msg_crit = format_alert_message("Fallo Sensor", "Temperatura > 90C", level="CRITICAL", tags=["S1", "TEMP"], details={"Sensor": "S1_01", "Valor": 95.2})
    assert "🚨" in msg_crit
    assert "#S1" in msg_crit
    assert "<code>95.2</code>" in msg_crit

    msg_info = format_alert_message("Inicio Proceso", "Comenzando", level="INFO")
    assert "ℹ️" in msg_info

    # 3. send_telegram_message sin tokens configurados
    res_no_tok = await send_telegram_message("Hola", bot_token=None, chat_id=None, raise_on_error=False)
    assert res_no_tok["sent"] is False

    with pytest.raises(ValueError):
        await send_telegram_message("Hola", bot_token=None, chat_id=None, raise_on_error=True)

    # 4. Truncamiento de mensajes largos (> 4096 caracteres)
    long_msg = "X" * 5000
    mock_post = AsyncMock()
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"ok": True, "result": {"message_id": 12345}})

    mock_client = MagicMock()
    mock_client.post = mock_post

    res_long = await send_telegram_message(
        message=long_msg,
        bot_token="fake_bot_token",
        chat_id="fake_chat_id",
        http_client=mock_client
    )
    assert res_long["sent"] is True
    # Validar que el payload enviado no exceda los 4096 caracteres
    call_args = mock_post.call_args[1]["json"]
    assert len(call_args["text"]) <= 4096

    # 5. alert_dispatcher hashing y debouncing
    h1 = calculate_alert_hash("Mensaje repetido", alert_key="sensor_01")
    h2 = calculate_alert_hash("Mensaje repetido", alert_key="sensor_01")
    assert h1 == h2
    assert get_alert_lock_key(h1).startswith("tb_alert_lock:")

    # 6. dispatch_debounced_alert con mock redis
    mock_redis = AsyncMock()
    # Caso A: lock adquirido exitosamente (SET NX retornó True)
    mock_redis.set.return_value = True
    with patch("core.services.alert_dispatcher.send_telegram_message", AsyncMock(return_value={"sent": True})):
        res_disp = await dispatch_debounced_alert(
            message="Alerta de fuego",
            alert_key="fire_01",
            bot_token="tok",
            chat_id="chat",
            redis_conn=mock_redis
        )
        assert res_disp["sent"] is True

    # Caso B: lock ya existe (SET NX retornó None o False)
    mock_redis.set.return_value = False
    res_debounced = await dispatch_debounced_alert(
        message="Alerta de fuego",
        alert_key="fire_01",
        bot_token="tok",
        chat_id="chat",
        redis_conn=mock_redis
    )
    assert res_debounced["sent"] is False
    assert res_debounced["reason"] == "debounced"


# ==============================================================================
# 4. CORE/SERVICES/HIERARCHICAL_SUPPRESSION_SERVICE.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_hierarchical_suppression_service_all_branches():
    await init_mock_db("hierarchical_tests")
    server = TBServer(name="SRV_HIER", base_url="https://tb-hier.com")
    await server.insert()
    tenant = TBTenant(name="TNT_HIER", server_id=server, username="admin_hier")
    await tenant.insert()
    tenant_id_str = str(tenant.id)

    # 1. Semántica Fail-Open ante error
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = Exception("Redis crash")
    is_suppressed, parent_gw, meta = await check_parent_gateway_status(
        tenant_id=tenant_id_str,
        device_name="Sensor_01",
        redis_conn=mock_redis
    )
    assert is_suppressed is False

    # 2. Caché hit: Gateway padre registrado como inactivo
    mock_redis_cached = AsyncMock()
    mock_redis_cached.get.side_effect = [
        json.dumps({"gateway_id": "gw_uuid_1", "gateway_name": "GW_Principal"}),  # cache tb_parent_gw
        json.dumps({"is_inactive": True, "status": "OFFLINE", "gateway_name": "GW_Principal", "metadata": {"battery": 10, "status": "OFFLINE"}})  # cache tb_gw_status
    ]
    is_suppressed, parent_gw, meta = await check_parent_gateway_status(
        tenant_id=tenant_id_str,
        device_name="Sensor_01",
        redis_conn=mock_redis_cached
    )
    assert is_suppressed is True
    assert parent_gw == "GW_Principal"
    assert meta["status"] == "OFFLINE"

    # 3. Caché hit: Gateway padre registrado como activo (NO suprimir alerta de sensor)
    mock_redis_active = AsyncMock()
    mock_redis_active.get.side_effect = [
        json.dumps({"gateway_id": "gw_uuid_1", "gateway_name": "GW_Principal"}),
        json.dumps({"is_inactive": False, "status": "ONLINE", "gateway_name": "GW_Principal", "metadata": {}})
    ]
    is_suppressed_act, parent_gw_act, _ = await check_parent_gateway_status(
        tenant_id=tenant_id_str,
        device_name="Sensor_01",
        redis_conn=mock_redis_active
    )
    assert is_suppressed_act is False
    assert parent_gw_act == "GW_Principal"


# ==============================================================================
# 5. CORE/SERVICES/TASK_REGISTRY.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_task_registry_events():
    assert get_user_stream_channel("u1", "t1") == "user:u1:stream:t1"
    assert get_user_registry_key("u1") == "tb_events:user:u1:registry"

    mock_redis = AsyncMock()

    # Evento de progreso (no terminal)
    ev_prog = await publish_task_event(
        redis_client=mock_redis,
        user_id="u1",
        task_id="t1",
        status="IN_PROGRESS",
        task_type="telemetry",
        progress_pct=45.5,
        message="Descargando"
    )
    assert ev_prog["progress_pct"] == 45.5
    assert mock_redis.publish.called
    assert mock_redis.hset.called

    # Evento terminal (SUCCESS) -> hdel
    ev_term = await publish_task_event(
        redis_client=mock_redis,
        user_id="u1",
        task_id="t1",
        status="SUCCESS",
        task_type="telemetry",
        progress_pct=100.0,
        cleanup_on_terminal=True
    )
    assert ev_term["status"] == "SUCCESS"
    assert mock_redis.hdel.called


# ==============================================================================
# 6. CORE/SERVICES/SYSTEM_INFO_SERVICE.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_system_info_service_all_branches():
    await init_mock_db("system_info_tests")

    # 1. _normalize_system_info
    # None/Vacío
    norm_empty = _normalize_system_info(None)
    assert norm_empty["cpu_usage"] is None

    # Nodo único (Monolito)
    single_node_raw = {
        "cpuUsage": 12.345,
        "memoryUsage": 45.678,
        "discUsage": 78.9,
        "totalMemory": 16000,
        "freeMemory": 8000,
        "totalDisc": 100000,
        "freeDisc": 50000,
    }
    norm_single = _normalize_system_info(single_node_raw)
    assert norm_single["cpu_usage"] == 12.35
    assert norm_single["memory_usage"] == 45.68

    # Cluster (múltiples nodos)
    cluster_raw = [
        {"cpuUsage": 10.0, "memoryUsage": 20.0, "discUsage": 30.0, "totalMemory": 8000, "freeMemory": 4000, "totalDisc": 50000, "freeDisc": 25000},
        {"cpuUsage": 30.0, "memoryUsage": 40.0, "discUsage": 50.0, "totalMemory": 8000, "freeMemory": 4000, "totalDisc": 50000, "freeDisc": 25000},
    ]
    norm_cluster = _normalize_system_info(cluster_raw)
    assert norm_cluster["cpu_usage"] == 20.0  # promedio (10 + 30) / 2
    assert norm_cluster["total_memory"] == 16000

    # 2. _build_metric_point
    pt = _build_metric_point(single_node_raw, "HEALTHY", "2026-10-08T00:00:00Z")
    assert pt["status"] == "HEALTHY"
    assert pt["cpu_usage"] == 12.35

    # 3. refresh_server_tokens_in_db
    server = TBServer(name="SRV_TOK", base_url="https://tb-tok.com", username="sysadmin")
    server.set_password("sys_pass")
    await server.insert()

    mock_tb = MagicMock()
    mock_tb.base_url = server.base_url
    mock_tb.username = "sysadmin"
    mock_tb.password = "sys_pass"
    mock_tb.refresh_token = "old_ref_tok"
    mock_tb.refresh_jwt_token = AsyncMock(return_value={"token": "new_tok_1", "refreshToken": "new_ref_1"})

    token_ref = ["old_tok"]
    new_t, new_r = await refresh_server_tokens_in_db(str(server.id), mock_tb, token_ref)
    assert new_t == "new_tok_1"
    assert token_ref[0] == "new_tok_1"

    # 4. collect_server_system_info
    server.set_tokens("valid_tok_1", "valid_ref_1")
    await server.save()
    mock_tb.get_system_info = AsyncMock(return_value=single_node_raw)
    with patch("core.services.system_info_service.ThingsBoardClient", return_value=mock_tb):
        res_sys = await collect_server_system_info(server)
        assert res_sys["status"] == "SUCCESS"
        assert res_sys["metric_point"]["status"] == "HEALTHY"
        assert res_sys["metric_point"]["cpu_usage"] == 12.35


# ==============================================================================
# 7. CORE/SERVICES/INCREMENTAL_BACKUP_SERVICE.PY
# ==============================================================================

def test_incremental_backup_service_helpers():
    # 1. calculate_previous_month_boundaries
    # Probar con fecha en Octubre 2026 -> debe retornar Septiembre 2026
    ref_dt = datetime(2026, 10, 15, 12, 0, 0, tzinfo=ZoneInfo("America/Mexico_City"))
    start_dt, end_dt, start_ts, end_ts, y_str, m_str = calculate_previous_month_boundaries(ref_dt, "America/Mexico_City")
    assert y_str == "2026"
    assert m_str == "09"
    assert start_dt.month == 9
    assert start_dt.day == 1
    assert end_dt.month == 9
    assert end_dt.day == 30

    # Probar con fecha en Enero 2026 -> debe retornar Diciembre 2025
    ref_jan = datetime(2026, 1, 10, 12, 0, 0, tzinfo=ZoneInfo("America/Mexico_City"))
    start_jan, end_jan, _, _, y_jan, m_jan = calculate_previous_month_boundaries(ref_jan, "America/Mexico_City")
    assert y_jan == "2025"
    assert m_jan == "12"
    assert end_jan.day == 31

    # 2. is_retryable_http_exception
    assert is_retryable_http_exception(httpx.ReadTimeout("Timeout")) is True
    assert is_retryable_http_exception(httpx.ConnectError("Conn reset")) is True
    
    mock_req = httpx.Request("GET", "https://tb.com")
    resp_500 = httpx.Response(500, request=mock_req)
    assert is_retryable_http_exception(httpx.HTTPStatusError("500 Error", request=mock_req, response=resp_500)) is True

    resp_404 = httpx.Response(404, request=mock_req)
    assert is_retryable_http_exception(httpx.HTTPStatusError("404 Not Found", request=mock_req, response=resp_404)) is False


# ==============================================================================
# 8. CORE/SERVICES/EXCEL_REPORT_SERVICE.PY
# ==============================================================================

def test_excel_report_service_helpers():
    # 1. sanitize_sheet_name
    assert sanitize_sheet_name("Normal_Sheet") == "Normal_Sheet"
    assert sanitize_sheet_name("Sheet/With:Forbidden*Chars?[Name]") == "Sheet_With_Forbidden_Chars__Nam"
    long_name = "A" * 50
    assert len(sanitize_sheet_name(long_name)) == 31
    assert sanitize_sheet_name("") == "Sheet1"

    # 2. _build_dataframe_from_telemetry
    device_data = {
        "temperature": [
            {"ts": 1700000000000, "value": 24.5},
            {"ts": 1700000060000, "value": 25.0},
        ],
        "humidity": [
            {"ts": 1700000000000, "value": 60.0},
            {"ts": 1700000060000, "value": 58.5},
        ]
    }
    df = _build_dataframe_from_telemetry(device_data, "UTC")
    assert len(df) == 2
    assert "timestamp" in df.columns
    assert "temperature" in df.columns
    assert "humidity" in df.columns
    assert df.iloc[0]["temperature"] == 24.5

    # 3. _autofit_worksheet_columns con openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Encabezado Largo de Telemetría", "Corto"])
    ws.append(["Valor 123", 456])
    _autofit_worksheet_columns(ws)
    assert ws.column_dimensions["A"].width > 20


# ==============================================================================
# 9. CORE/SERVICES/HEATMAP_REPORT_SERVICE.PY
# ==============================================================================

def test_heatmap_report_service_helpers():
    # 1. HeatmapRule operadores matemáticos seguros
    r_ge = HeatmapRule(">=", 90.0, "#22c55e", "Excelente")
    assert r_ge.matches(95.0) is True
    assert r_ge.matches(90.0) is True
    assert r_ge.matches(89.9) is False
    assert r_ge.matches(None) is False
    assert r_ge.matches("NA") is False
    assert r_ge.matches("invalid") is False

    r_lt = HeatmapRule("<", 75.0, "#ef4444", "Bajo")
    assert r_lt.matches(70.0) is True
    assert r_lt.matches(75.0) is False

    r_eq = HeatmapRule("==", 100.0, "#3b82f6", "Exacto")
    assert r_eq.matches(100.0) is True
    assert r_eq.matches(99.0) is False

    # Operador no soportado
    with pytest.raises(ValueError):
        HeatmapRule("invalid_op", 50.0, "#000")

    # 2. parse_heatmap_rules
    # Reglas por defecto
    default_rules = parse_heatmap_rules(None)
    assert len(default_rules) == 3

    # Lista de dicts con diferentes formatos de clave (limit, threshold, etc.)
    custom_raw = [
        {"operator": ">=", "limit": 90.0, "color": "#00ff00"},
        {"operator": "<", "threshold": 50.0, "color": "#ff0000"},
    ]
    parsed = parse_heatmap_rules(custom_raw)
    assert len(parsed) == 2
    assert parsed[0].threshold == 90.0
    assert parsed[1].threshold == 50.0

    # 3. ensure_tkme_logo_badge
    # Asegurar que se ejecuta sin lanzar excepciones
    badge_path = ensure_tkme_logo_badge()
    # Si existe o no depende del fs local, pero no debe crashear
    assert badge_path is None or isinstance(badge_path, str)


# ==============================================================================
# 10. CORE/SERVICES/TELEMETRY_SERVICE.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_telemetry_service_helpers():
    # 1. sanitize_name
    assert sanitize_name("Device:01/Sensor*A") == "Device_01_Sensor_A"
    assert sanitize_name("   ") == "unknown_device"

    # 2. publish_task_status
    mock_redis = AsyncMock()
    pub_res = await publish_task_status(
        redis_client=mock_redis,
        user_id="user_test",
        task_id="task_test_1",
        status="IN_PROGRESS",
        tenant_name="Tenant Alpha",
        progress_pct=25.0
    )
    assert pub_res["progress_pct"] == 25.0
    assert pub_res["tenant_name"] == "Tenant Alpha"

    # 3. get_month_intervals
    start_dt = datetime(2026, 1, 15, tzinfo=timezone.utc)
    end_dt = datetime(2026, 3, 10, tzinfo=timezone.utc)
    now_dt = datetime(2026, 3, 15, tzinfo=timezone.utc)
    intervals = get_month_intervals(start_dt, end_dt, now_dt)
    # Debe abarcar 3 meses: Enero, Febrero, Marzo 2026
    assert len(intervals) == 3
    assert intervals[0]["year_str"] == "2026"
    assert intervals[0]["month_str"] == "01"
    assert intervals[2]["month_str"] == "03"

    # 4. calculate_telemetry_delta_plan
    with tempfile.TemporaryDirectory() as tmp_dir:
        tenant_folder = os.path.join(tmp_dir, "TenantX", "Dev1", "2026", "01")
        os.makedirs(tenant_folder, exist_ok=True)
        # Archivo parcial simulado
        parcial_file = os.path.join(tenant_folder, "dev1.temperature.01-2026.parcial.json")
        with open(parcial_file, "w") as f:
            json.dump({"data": [{"ts": 1700000000000, "value": 20.0}]}, f)

        interval = intervals[0]
        plan = await calculate_telemetry_delta_plan(
            tenant_name="TenantX",
            device_name="Dev1",
            entity_id="dev1",
            key="temperature",
            interval=interval,
            base_storage_dir=tmp_dir
        )
        assert plan["plan_type"] in ("FULL_LOCAL", "HYBRID", "FULL_REMOTE")


# ==============================================================================
# 11. API/ENDPOINTS/TELEMETRY/ROUTER.PY & DEVICES/ROUTER.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_telemetry_and_devices_routers_all_branches():
    await init_mock_db("telemetry_router_tests")
    server = TBServer(name="SRV_TELEM", base_url="https://tb-telem.com")
    await server.insert()
    tenant = TBTenant(name="TNT_TELEM", server_id=server, username="admin_telem")
    tenant.set_password("pass_telem")
    tenant.set_tokens("valid_jwt_token", "valid_refresh_token")
    await tenant.insert()
    user = User(username="telem_boss", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    # 1. Download telemetry endpoint (POST /download)
    dl_req = DownloadTelemetryRequest(
        tenant_id=str(tenant.id),
        start_date="2026-08-01T00:00:00",
        end_date="2026-08-10T23:59:59",
    )
    mock_pool = MagicMock()
    mock_pool.enqueue_job = AsyncMock(return_value=MagicMock(job_id="job_dl_1"))
    mock_redis = AsyncMock()
    mock_redis.set.return_value = True

    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_pool)), \
         patch("api.endpoints.telemetry.router.redis_client", mock_redis):
        res_dl = await download_telemetry(request=dl_req, current_user=user)
        assert res_dl["status"] == "Task enqueued"
        assert res_dl["task_id"] == "job_dl_1"

    # 2. Excel report endpoint (POST /report/excel)
    exc_req = ExcelReportRequest(
        tenant_id=str(tenant.id),
        year=2026,
        month=8,
        combine_in_single_file=True
    )
    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_pool)), \
         patch("api.endpoints.telemetry.router.redis_client", mock_redis):
        res_exc = await generate_excel_report(request=exc_req, current_user=user)
        assert res_exc["status"] == "Task enqueued"
        assert res_exc["task_id"] == "job_dl_1"

    # 3. Heatmap report endpoint (POST /report/heatmap)
    hm_req = HeatmapReportRequest(
        tenant_id=str(tenant.id),
        year=2026,
        month=8,
    )
    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_pool)), \
         patch("api.endpoints.telemetry.router.redis_client", mock_redis):
        res_hm = await generate_heatmap_report(request=hm_req, current_user=user)
        assert res_hm["status"] == "Task enqueued"

    # 4. Webhook device-status M2M (POST /webhooks/device-status)
    wh_req = DeviceStatusWebhookRequest(
        device_name="Sensor_P1",
        status="OFFLINE",
        tenant_id=str(tenant.id),
        layer=4,
        details={"battery": 15}
    )
    with patch("api.endpoints.telemetry.router.get_arq_pool", AsyncMock(return_value=mock_pool)):
        res_wh = await device_status_webhook(payload=wh_req)
        assert res_wh["status"] == "accepted"
        assert res_wh["job_id"] == "job_dl_1"

    # 5. CRUD de Backups en MongoDB
    backup = TBBackup(
        tenant_id=tenant,
        task_id="task_backup_100",
        file_name="backup_test.zip",
        file_path="backups/backup_test.zip",
        file_size=1024,
        backup_type="telemetry",
        requested_by=str(user.id),
        start_date="2026-08-01T00:00:00",
        end_date="2026-08-10T23:59:59"
    )
    await backup.insert()

    # List backups
    b_list = await list_tenant_backups(
        page=1,
        page_size=10,
        tenant_id=str(tenant.id),
        backup_type=None,
        current_user=user
    )
    assert b_list.pagination.total == 1
    assert len(b_list.items) == 1

    # Download backup file inexistente físicamente -> 404
    with pytest.raises(HTTPException) as exc_down:
        await download_file(task_id=backup.task_id, current_user=user)
    assert exc_down.value.status_code == status.HTTP_404_NOT_FOUND

    # Delete backup
    b_del = await delete_tenant_backup(backup_id=str(backup.id), current_user=user)
    assert b_del["status"] == "DELETED"

    # 6. Devices Router (/api/v1/tenants/{tenant_id}/devices)
    mock_tb_client = MagicMock()
    mock_tb_client.get_tenant_devices = AsyncMock(return_value={
        "data": [{"id": {"id": "dev_uuid_1"}, "name": "Device_1", "type": "default"}],
        "totalElements": 1,
        "totalPages": 1,
        "hasNext": False
    })
    mock_tb_client.get_device_by_id = AsyncMock(return_value={"id": {"id": "dev_uuid_1"}, "name": "Device_1"})

    with patch("api.endpoints.devices.router.ThingsBoardClient", return_value=mock_tb_client):
        # List devices
        devs_res = await list_tenant_devices(
            tenant_id=str(tenant.id),
            limit=10,
            page=0,
            current_user=user
        )
        assert devs_res["totalElements"] == 1

        # Get device by ID
        dev_res = await get_device_details(
            tenant_id=str(tenant.id),
            device_id="dev_uuid_1",
            current_user=user
        )
        assert dev_res["name"] == "Device_1"

        # Batch provisioning
        prov_req = DeviceProvisionBatchRequest(
            devices=[
                DeviceProvisionRequest(name="New_Sensor_1", type="temperature"),
                DeviceProvisionRequest(name="New_Sensor_2", type="humidity"),
            ]
        )
        mock_tb_client.create_device = AsyncMock(side_effect=[
            {"id": {"id": "uuid_new_1"}, "name": "New_Sensor_1", "type": "temperature"},
            {"id": {"id": "uuid_new_2"}, "name": "New_Sensor_2", "type": "humidity"},
        ])
        prov_res = await batch_provision_devices(
            tenant_id=str(tenant.id),
            request=prov_req,
            current_user=user
        )
        assert prov_res.status == "success"
        assert prov_res.successfully_created == 2

        # Create device relation
        mock_post_resp = MagicMock(status_code=200)
        mock_post_resp.raise_for_status = MagicMock()
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_post_resp)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=None)
        with patch("httpx.AsyncClient", return_value=mock_http):
            rel_res = await create_device_physical_relation(
                tenant_id=str(tenant.id),
                parent_id="dev_uuid_1",
                child_id="dev_uuid_2",
                relation_type="Edge_Link",
                request_data=None,
                current_user=user
            )
            assert rel_res.status == "success"
            assert rel_res.relation_type == "Edge_Link"
