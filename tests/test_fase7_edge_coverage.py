"""
Test Suite Fase 7: Cobertura Profunda de Rutas de Borde y Ramas Restantes
Cubre:
- api/endpoints/servers/router.py (get_all_servers_system_info live/saved, test_tenant_connection, execute_server_ssh_command)
- api/endpoints/scheduler/router.py (update_scheduled_task, delete_scheduled_task, trigger_scheduled_task)
- api/endpoints/telemetry/router.py (download_file con archivo físico en disco, validaciones de rango)
- workers/tasks.py (generate_monthly_heatmap_task completa, send_email_task con listas y adjuntos)
"""

import os
import io
import json
import tempfile
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx
from fastapi import HTTPException, status
from fastapi.responses import FileResponse
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
from core.tb_client import ThingsBoardClient

# Servers router
from api.endpoints.servers.router import (
    get_all_servers_system_info_overview,
    test_tenant_connection as endpoint_test_tenant_connection,
    execute_server_ssh_command,
)
from api.endpoints.servers.schemas import SSHExecuteRequest

# Scheduler router
from api.endpoints.scheduler.router import (
    update_scheduled_task,
    delete_scheduled_task,
    trigger_scheduled_task,
)
from api.endpoints.scheduler.schemas import ScheduledTaskUpdate

# Telemetry router
from api.endpoints.telemetry.router import (
    download_file,
    download_telemetry,
    DownloadTelemetryRequest,
)

# Workers tasks
from workers.tasks import (
    generate_monthly_heatmap_task,
    send_email_task,
)


async def init_mock_db(db_name: str = "fase7_edge_db"):
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
# 1. SERVERS ROUTER: SYSTEM INFO, TEST CONNECTION & SSH EXECUTE
# ==============================================================================

@pytest.mark.asyncio
async def test_servers_router_edge_branches():
    await init_mock_db("servers_edge_db")
    user = User(username="super_srv", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    server = TBServer(
        name="SRV_EDGE",
        base_url="https://tb-edge.com",
        username="sysadmin",
        ssh_host="192.168.1.50",
        ssh_username="admin"
    )
    server.set_password("sys_pass")
    server.set_tokens("srv_tok", "srv_ref")
    await server.insert()

    tenant = TBTenant(name="TNT_EDGE", server_id=server, username="tntadmin")
    tenant.set_tokens("tnt_tok", "tnt_ref")
    await tenant.insert()

    # 1. get_all_servers_system_info (live=True y live=False)
    with patch("core.services.system_info_service.collect_server_system_info", AsyncMock(return_value={
        "status": "HEALTHY",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "system_info": {"cpuUsage": 15.0, "memoryUsage": 30.0, "discUsage": 50.0}
    })):
        res_live = await get_all_servers_system_info_overview(live=True, current_user=user)
        assert len(res_live) == 1
        assert res_live[0].server_id == str(server.id)
        assert res_live[0].status == "HEALTHY"

        res_saved = await get_all_servers_system_info_overview(live=False, current_user=user)
        assert len(res_saved) == 1

    # 2. test_tenant_connection
    mock_tb = MagicMock(spec=ThingsBoardClient)
    mock_tb.test_connection = AsyncMock(return_value={"status": "CONNECTED", "latency_ms": 42})
    with patch("api.endpoints.servers.router.ThingsBoardClient", return_value=mock_tb):
        test_res = await endpoint_test_tenant_connection(
            server_id=str(server.id),
            tenant_id=str(tenant.id),
            current_user=user
        )
        assert test_res["status"] == "CONNECTED"
        assert test_res["tenant_id"] == str(tenant.id)

    # 3. execute_server_ssh_command
    ssh_req = SSHExecuteRequest(command="uptime")
    with patch("api.endpoints.servers.router.execute_ssh_command_on_server", AsyncMock(return_value={
        "server_id": str(server.id),
        "server_name": server.name,
        "host": "192.168.1.50",
        "status": "success",
        "command": "uptime",
        "exit_status": 0,
        "stdout": "up 10 days",
        "stderr": "",
        "executed_at": datetime.now(timezone.utc),
        "duration_ms": 50.0
    })):
        ssh_res = await execute_server_ssh_command(
            server_id=str(server.id),
            request_data=ssh_req,
            current_user=user
        )
        assert ssh_res.exit_status == 0
        assert ssh_res.command == "uptime"
        assert "up 10 days" in ssh_res.stdout


# ==============================================================================
# 2. SCHEDULER ROUTER: UPDATE, DELETE & TRIGGER
# ==============================================================================

@pytest.mark.asyncio
async def test_scheduler_router_edge_branches():
    await init_mock_db("scheduler_edge_db")
    user = User(username="sched_boss", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    server = TBServer(name="SRV_SCHED", base_url="https://tb-sched.com")
    await server.insert()
    tenant = TBTenant(name="TNT_SCHED", server_id=server, username="admin")
    await tenant.insert()

    task = TBScheduledTask(
        name="Tarea Original",
        task_name="tasks.cleanup_old_backups",
        cron_expression="0 2 * * *",
        is_active=True,
        next_run_time=datetime.now(timezone.utc) + timedelta(hours=1),
        payload={"days": 15}
    )
    await task.insert()
    task_id_str = str(task.id)

    # 1. update_scheduled_task
    upd_payload = ScheduledTaskUpdate(
        name="Tarea Actualizada",
        cron_expression="0 3 * * *",
        payload={"days": 30}
    )
    upd_res = await update_scheduled_task(
        task_id=task_id_str,
        payload=upd_payload,
        current_user=user
    )
    assert upd_res.name == "Tarea Actualizada"
    assert upd_res.cron_expression == "0 3 * * *"

    # 2. trigger_scheduled_task (disparo manual)
    mock_pool = MagicMock()
    mock_pool.enqueue_job = AsyncMock(return_value=MagicMock(job_id="manual_job_999"))
    with patch("api.endpoints.scheduler.router.get_arq_pool", AsyncMock(return_value=mock_pool)):
        trig_res = await trigger_scheduled_task(
            task_id=task_id_str,
            current_user=user
        )
        assert trig_res.status == "DISPATCHED"
        assert trig_res.job_id == "manual_job_999"

    # 3. delete_scheduled_task
    del_res = await delete_scheduled_task(
        task_id=task_id_str,
        current_user=user
    )
    assert del_res["status"] == "success"
    assert await TBScheduledTask.get(task.id) is None


# ==============================================================================
# 3. TELEMETRY ROUTER: DOWNLOAD FILE CON ARCHIVO REAL EN DISCO
# ==============================================================================

@pytest.mark.asyncio
async def test_telemetry_router_download_file_on_disk():
    await init_mock_db("telemetry_edge_db")
    user = User(username="telem_edge_user", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    server = TBServer(name="SRV_TELEM_EDGE", base_url="https://tb.com")
    await server.insert()
    tenant = TBTenant(name="TNT_TELEM_EDGE", server_id=server, username="admin")
    await tenant.insert()

    os.makedirs("backups", exist_ok=True)
    real_zip = os.path.join("backups", "real_download.zip")
    with open(real_zip, "w") as f:
        f.write("real zip content for testing")

    try:
        backup = TBBackup(
            tenant_id=tenant,
            task_id="task_real_zip_1",
            file_name="real_download.zip",
            file_path=real_zip,
            file_size=28,
            backup_type="telemetry",
            requested_by=str(user.id),
            start_date="2026-08-01T00:00:00",
            end_date="2026-08-05T00:00:00"
        )
        await backup.insert()

        resp = await download_file(task_id=backup.task_id, current_user=user)
        assert isinstance(resp, FileResponse)
        assert resp.path == real_zip
        assert resp.filename == "real_download.zip"
    finally:
        if os.path.exists(real_zip):
            os.remove(real_zip)


# ==============================================================================
# 4. WORKERS: GENERATE_MONTHLY_HEATMAP_TASK COMPLETA & SEND_EMAIL_TASK AVANZADO
# ==============================================================================

@pytest.mark.asyncio
async def test_generate_monthly_heatmap_task_full_flow():
    await init_mock_db("workers_heatmap_full_db")
    server = TBServer(name="SRV_HM_FULL", base_url="https://tb-hm.com")
    await server.insert()

    tenant = TBTenant(
        name="TNT_HM_FULL",
        server_id=server,
        username="admin_hm",
        custom_metadata={
            "heatmap_config": {
                "keys": ["temperature"],
                "rules": [{"operator": ">=", "value": 25.0, "color": "#f00", "label": "Alto"}],
                "time_zone": "America/Mexico_City"
            }
        }
    )
    tenant.set_tokens("valid_jwt", "valid_ref")
    await tenant.insert()

    mock_redis = AsyncMock()
    mock_redis.expire = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=True)

    mock_tb = MagicMock(spec=ThingsBoardClient)
    mock_tb.base_url = server.base_url
    mock_tb.token = "valid_jwt"
    mock_tb.get_tenant_devices = AsyncMock(return_value={
        "data": [{"id": {"id": "dev_hm_1"}, "name": "Sensor_Clima", "type": "default"}],
        "hasNext": False
    })
    mock_tb.get_entity_attributes = AsyncMock(return_value=[
        {"key": "heatmap_active", "value": True}
    ])
    mock_tb.get_entity_telemetry = AsyncMock(return_value={
        "temperature": [{"ts": 1754000000000, "value": "26.5"}]
    })

    ctx = {"redis": mock_redis, "job_id": "job_hm_worker_1", "job_try": 1}
    payload = {
        "tenant_id": str(tenant.id),
        "year": 2026,
        "month": 8,
    }

    with patch("workers.tasks.ThingsBoardClient", return_value=mock_tb), \
         patch("workers.tasks.generate_heatmap_report_pdf", AsyncMock(return_value="backups/heatmaps/report.pdf")), \
         patch("os.path.exists", return_value=True), \
         patch("os.path.getsize", return_value=5000):

        res_hm = await generate_monthly_heatmap_task(ctx, payload=payload)
        assert res_hm["status"] == "SUCCESS"
        assert res_hm["task_id"] == "job_hm_worker_1"
        assert res_hm["active_devices_count"] == 1


@pytest.mark.asyncio
async def test_send_email_task_advanced_branches():
    await init_mock_db("workers_email_full_db")
    email_cfg = TBEmailConfig(
        host="smtp.tkme.com",
        port=587,
        username="smtp_tester",
        sender_email="no-reply@tkme.com",
        sender_name="TKmE System"
    )
    await email_cfg.set_password("pass123")
    await email_cfg.insert()

    with tempfile.TemporaryDirectory() as tmp_dir:
        temp_att = os.path.join(tmp_dir, "report.pdf")
        with open(temp_att, "w") as f:
            f.write("pdf data")

        mock_redis = AsyncMock()
        ctx = {"redis": mock_redis, "job_id": "job_email_multi_1", "job_try": 1}

        with patch("workers.tasks.send_email_async", AsyncMock(return_value={"status": "SENT", "message_id": "abc_999"})):
            res_mail = await send_email_task(
                ctx,
                to_email=["dest1@tkme.com", "dest2@tkme.com"],
                cc=["boss@tkme.com"],
                bcc=["audit@tkme.com"],
                subject="Reporte Mensual TKmE",
                html_body="<p>Adjunto</p>",
                attachment_paths=[temp_att],
                delete_attachments=True
            )
            assert res_mail["status"] == "SENT"
            # Verificar que el adjunto fue eliminado por el bloque finally
            assert not os.path.exists(temp_att)
