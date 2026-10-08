"""
Test Suite Fase 5: Workers ARQ, Encolamiento Distribuido, Router de Tareas y Middleware de Auditoría
Cubre:
- workers/tasks.py
- workers/arq_settings.py
- api/endpoints/tasks/router.py
- api/middlewares/audit_log.py
- api/main.py
"""

import os
import io
import json
import time
import uuid
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx
from starlette.requests import Request
from starlette.responses import Response
from fastapi import FastAPI, HTTPException, status
from beanie import init_beanie, PydanticObjectId
from mongomock_motor import AsyncMongoMockClient
from arq import Retry
from arq.jobs import JobStatus

from core.models.user import User
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.tb_node import TBNode
from core.models.tb_backup import TBBackup
from core.models.tb_scheduled_task import TBScheduledTask
from core.models.tb_email_config import TBEmailConfig
from core.models.audit_log import AuditLog

# Workers & Settings
from workers.tasks import (
    get_server_lock_key,
    _heartbeat_server_lock,
    _get_directory_latest_mtime,
    _is_task_active_in_redis,
    _execute_cleanup_old_backups,
    cleanup_old_backups_task,
    master_dispatcher_task,
    download_telemetry_task,
    generate_excel_report_task,
    generate_monthly_heatmap_task,
    execute_incremental_tenant_backup_task,
    collect_servers_system_info_task,
    send_email_task,
    send_telegram_alert_task,
)
from workers.arq_settings import (
    startup,
    shutdown,
    WorkerSettings,
    REGISTERED_FUNCTIONS,
)

# Tasks Router
from api.endpoints.tasks.router import (
    get_active_tasks,
    get_task_status,
    stream_task_progress,
    cancel_task,
)
from api.endpoints.tasks.schemas import (
    ActiveTaskResponse,
    TaskStatusResponse,
    CancelTaskResponse,
)

# Middleware de Auditoría
from api.middlewares.audit_log import (
    sanitize_dict_or_list,
    parse_and_sanitize_payload,
    AuditLogMiddleware,
    SENSITIVE_FIELD_PATTERNS,
)

# Main Application Lifespan & Root Endpoints
from api.main import app, lifespan, root_get, root_post


async def init_mock_db(db_name: str = "fase5_test_db"):
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
# 1. AUDIT_LOG MIDDLEWARE Y SANITIZACIÓN DEVSECOPS
# ==============================================================================

def test_audit_log_sanitization_helpers():
    # 1. sanitize_dict_or_list con campos sensibles anidados
    raw_payload = {
        "username": "admin",
        "password": "secret_password",
        "nested": {
            "token": "jwt_token_123",
            "refresh_token": "ref_456",
            "client_secret": "my_secret",
            "safe_field": 42
        },
        "items": [
            {"access_token": "token_abc", "id": 1},
            {"name": "test_device"}
        ]
    }
    sanitized = sanitize_dict_or_list(raw_payload)
    assert sanitized["username"] == "admin"
    assert sanitized["password"] == "***"
    assert sanitized["nested"]["token"] == "***"
    assert sanitized["nested"]["refresh_token"] == "***"
    assert sanitized["nested"]["client_secret"] == "***"
    assert sanitized["nested"]["safe_field"] == 42
    assert sanitized["items"][0]["access_token"] == "***"
    assert sanitized["items"][0]["id"] == 1
    assert sanitized["items"][1]["name"] == "test_device"

    # 2. parse_and_sanitize_payload: Form-data
    form_bytes = b"username=test&password=secret_password&api_key=key123"
    parsed_form = parse_and_sanitize_payload(form_bytes, "application/x-www-form-urlencoded")
    assert parsed_form["username"] == "test"
    assert parsed_form["password"] == "***"
    assert parsed_form["api_key"] == "***"

    # 3. parse_and_sanitize_payload: Multipart
    mp_bytes = b"multipart_boundary_data"
    parsed_mp = parse_and_sanitize_payload(mp_bytes, "multipart/form-data; boundary=something")
    assert parsed_mp["_info"] == "[Multipart Form Data - Masked]"

    # 4. parse_and_sanitize_payload: Payload vacío
    assert parse_and_sanitize_payload(b"", "application/json") is None

    # 5. parse_and_sanitize_payload: Texto no JSON
    raw_text = b"random plain text"
    parsed_text = parse_and_sanitize_payload(raw_text, "text/plain")
    assert "_info" in parsed_text or "_data" in parsed_text


@pytest.mark.asyncio
async def test_audit_log_middleware_execution():
    await init_mock_db("audit_middleware_db")
    middleware = AuditLogMiddleware(app=MagicMock())

    # 1. Petición no mutante (GET) -> se procesa sin registrar en BD
    scope_get = {
        "type": "http",
        "method": "GET",
        "path": "/api/v1/users",
        "headers": [],
        "query_string": b"",
    }
    req_get = Request(scope_get)
    call_next_mock = AsyncMock(return_value=Response(status_code=200))
    resp = await middleware.dispatch(req_get, call_next_mock)
    assert resp.status_code == 200
    assert await AuditLog.count() == 0

    # 2. Petición mutante (POST) -> se registra en BD con payload sanitizado
    payload_dict = {"username": "new_user", "password": "super_secret"}
    body_bytes = json.dumps(payload_dict).encode("utf-8")

    async def mock_receive():
        return {"type": "http.request", "body": body_bytes}

    scope_post = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/auth/login",
        "headers": [
            (b"content-type", b"application/json"),
            (b"x-forwarded-for", b"203.0.113.195, 10.0.0.1"),
        ],
        "client": ("127.0.0.1", 12345),
        "query_string": b"",
    }
    req_post = Request(scope_post, receive=mock_receive)
    call_next_post = AsyncMock(return_value=Response(status_code=200))

    resp_post = await middleware.dispatch(req_post, call_next_post)
    assert resp_post.status_code == 200

    # Verificar registro en MongoDB
    logs = await AuditLog.find_all().to_list()
    assert len(logs) == 1
    log_entry = logs[0]
    assert log_entry.method == "POST"
    assert log_entry.endpoint == "/api/v1/auth/login"
    assert log_entry.ip_address == "203.0.113.195"
    assert log_entry.payload["password"] == "***"
    assert log_entry.payload["username"] == "new_user"


# ==============================================================================
# 2. ARQ WORKER SETTINGS Y CICLO DE VIDA (STARTUP / SHUTDOWN)
# ==============================================================================

@pytest.mark.asyncio
async def test_arq_settings_and_lifecycles():
    # 1. Atributos de WorkerSettings
    assert WorkerSettings.max_jobs == 1
    assert WorkerSettings.job_timeout == 864000
    assert WorkerSettings.allow_abort_jobs is True
    assert len(WorkerSettings.cron_jobs) >= 1
    assert len(REGISTERED_FUNCTIONS) >= 10

    # 2. Ciclo de startup
    ctx = {}
    with patch("workers.arq_settings.init_db", AsyncMock()) as mock_init_db:
        await startup(ctx)
        assert mock_init_db.called
        assert "http_client" in ctx
        assert isinstance(ctx["http_client"], httpx.AsyncClient)

    # 3. Ciclo de shutdown
    with patch("workers.arq_settings.close_db", AsyncMock()) as mock_close_db:
        await shutdown(ctx)
        assert mock_close_db.called
        assert ctx["http_client"].is_closed


# ==============================================================================
# 3. TASKS.PY: HEARTBEAT, BLOQUEOS Y UTILIDADES DE RETENCIÓN
# ==============================================================================

@pytest.mark.asyncio
async def test_worker_lock_heartbeat_and_mtime_helpers():
    # 1. get_server_lock_key
    assert get_server_lock_key("srv_123") == "tb_server_lock:srv_123"

    # 2. _heartbeat_server_lock
    mock_redis = AsyncMock()
    # Lanzar latido con intervalo de 0.01s y cancelarlo tras 1 ciclo
    hb_task = asyncio.create_task(
        _heartbeat_server_lock(
            redis_conn=mock_redis,
            lock_key="tb_server_lock:srv_123",
            interval_seconds=0.01,
            ttl_seconds=3600
        )
    )
    await asyncio.sleep(0.03)
    hb_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await hb_task
    assert mock_redis.expire.called

    # 3. _get_directory_latest_mtime
    import tempfile
    with tempfile.TemporaryDirectory() as tmp_dir:
        sub_file = os.path.join(tmp_dir, "test.txt")
        with open(sub_file, "w") as f:
            f.write("hello")
        mtime = _get_directory_latest_mtime(tmp_dir)
        assert mtime > 0

    # 4. _is_task_active_in_redis
    mock_r = AsyncMock()
    async def mock_scan(match):
        yield "tb_events:user:u1:registry"
    mock_r.scan_iter = mock_scan
    mock_r.hexists = AsyncMock(return_value=True)
    is_act = await _is_task_active_in_redis("job_123", redis_conn=mock_r)
    assert is_act is True


@pytest.mark.asyncio
async def test_cleanup_old_backups_task_all_branches():
    await init_mock_db("cleanup_tests_db")
    import tempfile
    with tempfile.TemporaryDirectory() as tmp_backup_dir:
        # Configurar BACKUP_DIR temporal
        with patch("workers.tasks.settings.BACKUP_DIR", tmp_backup_dir):
            server = TBServer(name="SRV_CLEAN", base_url="https://tb.com")
            await server.insert()
            tenant = TBTenant(name="TNT_CLEAN", server_id=server, username="admin")
            await tenant.insert()

            # Crear archivo físico de respaldo caducado (>30 días)
            old_zip = os.path.join(tmp_backup_dir, "old_backup.zip")
            with open(old_zip, "w") as f:
                f.write("zip content")

            # Documento de respaldo caducado en MongoDB
            old_date = datetime.now(timezone.utc) - timedelta(days=45)
            b_old = TBBackup(
                tenant_id=tenant,
                task_id="task_old_1",
                file_name="old_backup.zip",
                file_path=old_zip,
                file_size=10,
                backup_type="telemetry",
                requested_by="user_1",
                start_date="2026-01-01T00:00:00",
                end_date="2026-01-10T00:00:00",
                created_at=old_date
            )
            await b_old.insert()

            # Crear carpeta temporal zombi huérfana 'tmp_zombie' (modificada hace > 25 horas)
            zombie_dir = os.path.join(tmp_backup_dir, "tmp_zombie_1")
            os.makedirs(zombie_dir, exist_ok=True)
            old_mtime = time.time() - 100000  # > 27 horas atrás
            os.utime(zombie_dir, (old_mtime, old_mtime))

            # Ejecutar tarea de limpieza
            mock_redis = AsyncMock()
            async def empty_scan(match):
                if False:
                    yield None
            mock_redis.scan_iter = empty_scan

            res = await cleanup_old_backups_task(
                ctx={"redis": mock_redis},
                days_to_keep=30
            )

            assert res["status"] == "SUCCESS"
            assert res["deleted_db_records"] == 1
            assert res["deleted_files"] == 1
            assert res["zombie_dirs_deleted"] >= 1
            assert not os.path.exists(old_zip)
            assert not os.path.exists(zombie_dir)


# ==============================================================================
# 4. TASKS.PY: MASTER DISPATCHER Y TAREAS PROGRAMADAS
# ==============================================================================

@pytest.mark.asyncio
async def test_master_dispatcher_task_branches():
    await init_mock_db("dispatcher_tests_db")

    # 1. Caso sin tareas programadas vencidas
    mock_pool = MagicMock()
    mock_pool.enqueue_job = AsyncMock(return_value=MagicMock(job_id="job_cron_1"))
    await master_dispatcher_task(ctx={"redis": mock_pool})
    assert not mock_pool.enqueue_job.called

    # 2. Caso con tarea vencida
    due_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    sched_task = TBScheduledTask(
        name="Auto Backup Incremental",
        task_name="tasks.execute_incremental_tenant_backup",
        cron_expression="0 3 1 * *",
        is_active=True,
        next_run_time=due_time,
        payload={"tenant_name": "Tenant_A"}
    )
    await sched_task.insert()

    await master_dispatcher_task(ctx={"redis": mock_pool})
    assert mock_pool.enqueue_job.called

    # Verificar que next_run_time fue recalculado y last_run_status actualizado
    updated_task = await TBScheduledTask.get(sched_task.id)
    assert "DISPATCHED" in updated_task.last_run_status
    assert updated_task.next_run_time > datetime.now(timezone.utc)


# ==============================================================================
# 5. TASKS.PY: DOWNLOAD_TELEMETRY_TASK & GENERATE_EXCEL_REPORT_TASK
# ==============================================================================

@pytest.mark.asyncio
async def test_download_telemetry_and_excel_tasks():
    await init_mock_db("workers_orchestrator_db")
    server = TBServer(name="SRV_WRK", base_url="https://tb-wrk.com")
    await server.insert()
    tenant = TBTenant(name="TNT_WRK", server_id=server, username="adm_wrk")
    tenant.set_tokens("valid_jwt", "valid_ref")
    await tenant.insert()

    mock_redis = AsyncMock()
    mock_redis.expire = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=True)

    ctx = {"redis": mock_redis, "job_id": "job_worker_dl_1", "job_try": 1}
    payload = {
        "tenant_id": str(tenant.id),
        "user_id": "usr_test",
        "start_date": "2026-08-01T00:00:00",
        "end_date": "2026-08-05T00:00:00",
    }

    # 1. download_telemetry_task: Ejecución delegada a run_download_orchestrator
    with patch("workers.tasks.run_download_orchestrator", AsyncMock(return_value={"status": "SUCCESS", "records": 100})):
        res_dl = await download_telemetry_task(ctx, payload)
        assert res_dl["status"] == "SUCCESS"
        assert res_dl["records"] == 100
        assert mock_redis.delete.called

    # 2. download_telemetry_task: Manejo de error de red transitorio -> Retry
    with patch("workers.tasks.run_download_orchestrator", AsyncMock(side_effect=httpx.ConnectError("Network drop"))):
        with pytest.raises(Retry):
            await download_telemetry_task(ctx, payload)

    # 3. generate_excel_report_task: Ejecución delegada a run_excel_report_orchestrator
    with patch("workers.tasks.run_excel_report_orchestrator", AsyncMock(return_value={"status": "SUCCESS", "file": "report.xlsx"})):
        res_exc = await generate_excel_report_task(ctx, payload)
        assert res_exc["status"] == "SUCCESS"
        assert res_exc["file"] == "report.xlsx"


# ==============================================================================
# 6. TASKS.PY: HEATMAP, INCREMENTAL BACKUP, SYSTEM INFO, EMAIL & TELEGRAM
# ==============================================================================

@pytest.mark.asyncio
async def test_heatmap_incremental_sysinfo_email_telegram_tasks():
    await init_mock_db("workers_misc_db")
    server = TBServer(name="SRV_MISC", base_url="https://tb-misc.com")
    await server.insert()
    tenant = TBTenant(name="TNT_MISC", server_id=server, username="adm_misc")
    tenant.set_tokens("valid_jwt", "valid_ref")
    await tenant.insert()

    mock_redis = AsyncMock()
    ctx = {"redis": mock_redis, "job_id": "job_misc_1", "job_try": 1}

    # 1. execute_incremental_tenant_backup_task
    with patch("workers.tasks.run_incremental_tenant_backup", AsyncMock(return_value={"status": "SUCCESS", "files_created": 5})):
        res_incr = await execute_incremental_tenant_backup_task(ctx, payload={"tenant_id": str(tenant.id)})
        assert res_incr["status"] == "SUCCESS"
        assert res_incr["files_created"] == 5

    # Incremental con error transitorio -> Retry
    with patch("workers.tasks.run_incremental_tenant_backup", AsyncMock(side_effect=httpx.ReadTimeout("Timeout"))):
        with pytest.raises(Retry):
            await execute_incremental_tenant_backup_task(ctx, payload={"tenant_id": str(tenant.id)})

    # 2. collect_servers_system_info_task
    with patch("core.services.system_info_service.collect_all_servers_system_info", AsyncMock(return_value={"status": "COLLECTED", "count": 1})):
        res_sys = await collect_servers_system_info_task(ctx, payload={"server_id": str(server.id)})
        assert res_sys["status"] == "COLLECTED"

    # 3. send_email_task
    email_cfg = TBEmailConfig(
        host="smtp.tkme.com",
        port=587,
        username="smtp_user",
        sender_email="alerts@tkme.com",
        sender_name="TKmE Alerts"
    )
    await email_cfg.set_password("smtp_pass")
    await email_cfg.insert()

    with patch("workers.tasks.send_email_async", AsyncMock(return_value={"status": "SENT", "message_id": "123"})):
        res_email = await send_email_task(
            ctx,
            to_email="user@example.com",
            subject="Prueba Worker",
            html_body="<p>OK</p>"
        )
        assert res_email["status"] == "SENT"

    # send_email_task sin destinatario -> ValueError
    with pytest.raises(ValueError, match="to_email"):
        await send_email_task(ctx, to_email="")

    # 4. send_telegram_alert_task
    # Alerta sin mensaje
    res_tg_no_msg = await send_telegram_alert_task(ctx, payload={"message": ""})
    assert res_tg_no_msg["sent"] is False
    assert res_tg_no_msg["reason"] == "missing_message"

    # Alerta de Capa 4 suprimida por gateway inactivo
    with patch("workers.tasks.check_parent_gateway_status", AsyncMock(return_value=(True, "GW_Central", {"status": "OFFLINE"}))):
        res_tg_supp = await send_telegram_alert_task(
            ctx,
            payload={
                "message": "Falla Sensor",
                "layer": 4,
                "device_name": "Sensor_01",
                "tenant_id": str(tenant.id)
            }
        )
        assert res_tg_supp["sent"] is False
        assert res_tg_supp["reason"] == "suppressed_by_parent_layer"
        assert res_tg_supp["parent_gateway"] == "GW_Central"

    # Alerta despachada normalmente
    with patch("workers.tasks.dispatch_debounced_alert", AsyncMock(return_value={"sent": True, "hash": "h123"})):
        res_tg_sent = await send_telegram_alert_task(
            ctx,
            payload={
                "message": "Alerta Normal",
                "bot_token": "tg_tok",
                "chat_id": "tg_chat"
            }
        )
        assert res_tg_sent["sent"] is True


# ==============================================================================
# 7. TASKS ROUTER: /api/v1/tasks (ACTIVE, STATUS, STREAM, CANCEL)
# ==============================================================================

@pytest.mark.asyncio
async def test_tasks_router_endpoints_all_branches():
    await init_mock_db("tasks_router_tests_db")
    user = User(username="task_admin", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    mock_redis = AsyncMock()

    # 1. get_active_tasks para Superuser
    mock_redis.keys = AsyncMock(return_value=["tb_events:user:usr1:registry"])
    task_payload = json.dumps({
        "task_id": "t_active_1",
        "task_type": "telemetry",
        "status": "IN_PROGRESS",
        "progress_pct": 50.0,
        "tenant_name": "TNT_A"
    })
    mock_redis.hgetall = AsyncMock(return_value={"t_active_1": task_payload})

    with patch("api.endpoints.tasks.router.redis_client", mock_redis):
        # Todos los tipos
        act_all = await get_active_tasks(task_type=None, current_user=user)
        assert len(act_all) == 1
        assert act_all[0].task_id == "t_active_1"

        # Filtrando por tipo
        act_filtered = await get_active_tasks(task_type="telemetry", current_user=user)
        assert len(act_filtered) == 1

        act_mismatch = await get_active_tasks(task_type="email", current_user=user)
        assert len(act_mismatch) == 0

    # 2. get_task_status
    mock_job = MagicMock()
    mock_job.status = AsyncMock(return_value=JobStatus.complete)
    res_info = MagicMock()
    res_info.function = "download_telemetry_task"
    res_info.success = True
    res_info.enqueue_time = datetime.now(timezone.utc)
    res_info.start_time = datetime.now(timezone.utc)
    res_info.finish_time = datetime.now(timezone.utc)
    res_info.result = {"files": 1}
    mock_job.result_info = AsyncMock(return_value=res_info)

    mock_pool = MagicMock()
    with patch("api.endpoints.tasks.router.get_arq_pool", AsyncMock(return_value=mock_pool)), \
         patch("api.endpoints.tasks.router.Job", return_value=mock_job):
        st_res = await get_task_status(task_id="t_active_1", current_user=user)
        assert st_res.task_id == "t_active_1"
        assert st_res.status == "complete"
        assert st_res.success is True
        assert st_res.task_type == "telemetry"

    # 3. cancel_task (Universal Panic Button)
    # Caso A: Job not found -> 404
    mock_job_notfound = MagicMock()
    mock_job_notfound.status = AsyncMock(return_value=JobStatus.not_found)
    with patch("api.endpoints.tasks.router.get_arq_pool", AsyncMock(return_value=mock_pool)), \
         patch("api.endpoints.tasks.router.Job", return_value=mock_job_notfound):
        with pytest.raises(HTTPException) as exc_nf:
            await cancel_task(job_id="job_non_existent", current_user=user)
        assert exc_nf.value.status_code == status.HTTP_404_NOT_FOUND

    # Caso B: Job ya completado -> 400
    mock_job_done = MagicMock()
    mock_job_done.status = AsyncMock(return_value=JobStatus.complete)
    with patch("api.endpoints.tasks.router.get_arq_pool", AsyncMock(return_value=mock_pool)), \
         patch("api.endpoints.tasks.router.Job", return_value=mock_job_done):
        with pytest.raises(HTTPException) as exc_done:
            await cancel_task(job_id="job_done_1", current_user=user)
        assert exc_done.value.status_code == status.HTTP_400_BAD_REQUEST

    # Caso C: Job activo -> Cancelación exitosa
    mock_job_active = MagicMock()
    mock_job_active.status = AsyncMock(return_value=JobStatus.in_progress)
    mock_job_active.abort = AsyncMock(return_value=True)
    with patch("api.endpoints.tasks.router.get_arq_pool", AsyncMock(return_value=mock_pool)), \
         patch("api.endpoints.tasks.router.Job", return_value=mock_job_active), \
         patch("api.endpoints.tasks.router.redis_client", mock_redis):
        can_res = await cancel_task(job_id="job_active_1", current_user=user)
        assert can_res.status == "cancelled"
        assert can_res.aborted is True
        assert mock_redis.publish.called
        assert mock_redis.hdel.called


# ==============================================================================
# 8. MAIN.PY: ROOT ENDPOINTS Y LIFESPAN
# ==============================================================================

@pytest.mark.asyncio
async def test_main_root_endpoints_and_lifespan():
    # 1. root_get
    res_get = await root_get()
    assert res_get["status"] == "ok"
    assert "ThingsBoard Super API Gateway" in res_get["message"]

    # 2. root_post
    res_post = await root_post()
    assert res_post["status"] == "ok"

    # 3. lifespan
    with patch("api.main.init_db", AsyncMock()) as mock_init_db, \
         patch("api.main.init_casbin_enforcer", AsyncMock()) as mock_casbin, \
         patch("api.main.bootstrap_superadmin", AsyncMock()) as mock_bootstrap, \
         patch("api.main.get_arq_pool", AsyncMock(return_value=MagicMock())) as mock_get_arq, \
         patch("api.main.close_arq_pool", AsyncMock()) as mock_close_arq, \
         patch("api.main.close_db", AsyncMock()) as mock_close_db:

        test_app = FastAPI()
        async with lifespan(test_app):
            assert mock_init_db.called
            assert mock_casbin.called
            assert mock_bootstrap.called
            assert mock_get_arq.called

        assert mock_close_arq.called
        assert mock_close_db.called
