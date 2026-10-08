import os
import json
import uuid
import tempfile
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx
from arq.jobs import JobStatus
from fastapi import HTTPException
from beanie import init_beanie, PydanticObjectId
from mongomock_motor import AsyncMongoMockClient

from core.models.user import User
from core.models.tb_server import TBServer, SSHAuthMethod
from core.models.tb_node import TBNode
from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.models.tb_scheduled_task import TBScheduledTask
from core.models.tb_email_config import TBEmailConfig
from core.models.audit_log import AuditLog
from core.tb_client import ThingsBoardClient

from api.endpoints.telemetry.router import (
    DownloadTelemetryRequest,
    ExcelReportRequest,
    delete_tenant_backup,
    download_file
)
from api.endpoints.tasks.router import (
    get_active_tasks,
    stream_task_progress,
    cancel_task
)
from api.endpoints.scheduler.router import (
    _resolve_tenant_or_404,
    _get_task_or_404,
    update_scheduled_task,
    delete_scheduled_task,
    trigger_scheduled_task
)
from api.endpoints.scheduler.schemas import ScheduledTaskUpdate
from api.endpoints.servers.router import (
    create_server,
    list_servers,
    update_server,
    update_server_node
)
from api.endpoints.servers.schemas import (
    ServerCreateRequest,
    ServerUpdateRequest,
    NodeUpdateRequest
)
from workers.tasks import (
    master_dispatcher_task,
    cleanup_old_backups_task,
    _execute_cleanup_old_backups,
    _get_directory_latest_mtime,
    _is_task_active_in_redis,
    execute_incremental_tenant_backup_task,
    collect_servers_system_info_task,
    send_telegram_alert_task
)
from core.services.telemetry_service import (
    refresh_tenant_tokens_in_db,
    _sync_get_highest_ts,
    _sync_read_records_in_range
)
from core.services.system_info_service import (
    collect_all_servers_system_info
)


async def setup_db(db_name: str = "fase9_boost_db"):
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
    user = User(
        username="boost_superadmin",
        email="superadmin@boost.org",
        role="superadmin",
        is_superuser=True,
        is_active=True
    )
    await user.insert()
    return user


@pytest.mark.asyncio
async def test_telemetry_excel_request_validators():
    # 1. DownloadTelemetryRequest sanitize_entity_id
    req1 = DownloadTelemetryRequest(
        tenant_id="t1",
        start_date="2026-01-01T00:00:00",
        end_date="2026-01-02T00:00:00",
        entity_id="string"
    )
    assert req1.entity_id is None

    req2 = DownloadTelemetryRequest(
        tenant_id="t1",
        start_date="2026-01-01T00:00:00",
        end_date="2026-01-02T00:00:00",
        entity_id=["dev1", "none", "   ", "dev2"]
    )
    assert req2.entity_id == ["dev1", "dev2"]

    # 2. ExcelReportRequest validators
    with pytest.raises(ValueError, match="Debe especificar O BIEN"):
        ExcelReportRequest(
            tenant_id="t1",
            start_date="2026-01-01T00:00:00",
            end_date="2026-01-31T23:59:59",
            year=2026,
            month=1
        )

    with pytest.raises(ValueError, match="Debe especificar O BIEN"):
        ExcelReportRequest(tenant_id="t1")

    with pytest.raises(ValueError, match="El mes debe estar comprendido"):
        ExcelReportRequest(tenant_id="t1", year=2026, month=13)

    with pytest.raises(ValueError, match="Año fuera del rango"):
        ExcelReportRequest(tenant_id="t1", year=1960, month=1)

    with pytest.raises(ValueError, match="Formato ISO inválido"):
        ExcelReportRequest(tenant_id="t1", start_date="bad-date", end_date="bad-date")

    with pytest.raises(ValueError, match="start_date no puede ser posterior"):
        ExcelReportRequest(
            tenant_id="t1",
            start_date="2026-02-01T00:00:00",
            end_date="2026-01-01T00:00:00"
        )

    req_feb = ExcelReportRequest(tenant_id="t1", year=2026, month=2)
    assert "2026-02-01" in req_feb.start_date
    assert "2026-02-28" in req_feb.end_date


@pytest.mark.asyncio
async def test_telemetry_router_download_and_delete_backups():
    fake_user = await setup_db("fase9_db_backups")
    server = TBServer(name="ServTB", base_url="http://tb.srv", user_id=str(fake_user.id))
    await server.insert()
    tenant = TBTenant(name="TenTB", username="tadmin", server_id=server, user_id=str(fake_user.id))
    await tenant.insert()

    os.makedirs("backups", exist_ok=True)
    os.makedirs("backups/heatmaps", exist_ok=True)

    dummy_zip = "backups/test_boost_1.zip"
    dummy_pdf = "backups/heatmaps/test_boost_2.pdf"
    with open(dummy_zip, "w") as f:
        f.write("dummy-zip")
    with open(dummy_pdf, "w") as f:
        f.write("dummy-pdf")

    backup1 = TBBackup(
        tenant_id=tenant,
        task_id="boost_t1",
        file_name="test_boost_1.zip",
        file_size=9,
        backup_type="telemetry",
        requested_by=str(fake_user.id),
        start_date="2026-01-01T00:00:00",
        end_date="2026-01-02T00:00:00"
    )
    await backup1.insert()

    backup2 = TBBackup(
        tenant_id=tenant,
        task_id="boost_t2",
        file_name="test_boost_2.pdf",
        file_size=9,
        backup_type="heatmap",
        requested_by=str(fake_user.id),
        start_date="2026-01-01T00:00:00",
        end_date="2026-01-02T00:00:00"
    )
    await backup2.insert()

    try:
        resp1 = await download_file(task_id="boost_t1", current_user=fake_user)
        assert resp1.status_code == 200
        assert resp1.media_type == "application/zip"

        resp2 = await download_file(task_id="boost_t2", current_user=fake_user)
        assert resp2.status_code == 200
        assert resp2.media_type == "application/pdf"

        with pytest.raises(HTTPException) as exc404:
            await delete_tenant_backup(backup_id="non_existent", current_user=fake_user)
        assert exc404.value.status_code == 404

        del_res = await delete_tenant_backup(backup_id=str(backup1.id), current_user=fake_user)
        assert del_res["status"] == "DELETED"
        assert not os.path.exists(dummy_zip)
        assert await TBBackup.get(backup1.id) is None

        del_res2 = await delete_tenant_backup(backup_id=str(backup2.id), current_user=fake_user)
        assert del_res2["status"] == "DELETED"
        assert not os.path.exists(dummy_pdf)
    finally:
        for p in [dummy_zip, dummy_pdf]:
            if os.path.exists(p):
                os.remove(p)


@pytest.mark.asyncio
async def test_tasks_router_stream_and_cancel():
    fake_user = await setup_db("fase9_db_tasks")

    mock_job = MagicMock()
    mock_job.status = AsyncMock(return_value=JobStatus.not_found)

    with patch("api.endpoints.tasks.router.Job", return_value=mock_job), \
         patch("api.endpoints.tasks.router.get_arq_pool", new_callable=AsyncMock):
        with pytest.raises(HTTPException) as exc404:
            await cancel_task(job_id="job_404", current_user=fake_user)
        assert exc404.value.status_code == 404

        mock_job.status = AsyncMock(return_value=JobStatus.complete)
        with pytest.raises(HTTPException) as exc400:
            await cancel_task(job_id="job_complete", current_user=fake_user)
        assert exc400.value.status_code == 400

        mock_job.status = AsyncMock(side_effect=Exception("Redis conn error"))
        with pytest.raises(HTTPException) as exc500:
            await cancel_task(job_id="job_err", current_user=fake_user)
        assert exc500.value.status_code == 500

        mock_job.status = AsyncMock(return_value=JobStatus.in_progress)
        mock_job.abort = AsyncMock(return_value=True)
        with patch("api.endpoints.tasks.router.redis_client") as r_mock:
            r_mock.publish = AsyncMock()
            r_mock.hdel = AsyncMock()
            cancel_res = await cancel_task(job_id="job_running", current_user=fake_user)
            assert cancel_res.status == "cancelled"
            assert cancel_res.aborted is True

    fake_request = MagicMock()
    fake_request.is_disconnected = AsyncMock(side_effect=[False, False, True])

    mock_pubsub = MagicMock()
    success_msg = {
        "type": "message",
        "data": json.dumps({"status": "SUCCESS", "progress_pct": 100.0, "message": "Done!"})
    }
    mock_pubsub.get_message = AsyncMock(return_value=success_msg)
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()
    mock_pubsub.aclose = AsyncMock()

    with patch("api.endpoints.tasks.router.redis_client") as r_mock:
        r_mock.pubsub = MagicMock(return_value=mock_pubsub)
        r_mock.hget = AsyncMock(return_value=None)

        resp = await stream_task_progress(task_id="t_stream_123", request=fake_request, current_user=fake_user)
        assert resp.status_code == 200

        chunks = []
        async for chunk in resp.body_iterator:
            chunks.append(chunk)
            if "SUCCESS" in chunk:
                break
        assert any("SUCCESS" in c for c in chunks)


@pytest.mark.asyncio
async def test_scheduler_router_extended():
    fake_user = await setup_db("fase9_db_sched")
    server = TBServer(name="SchedSrv", base_url="http://tb.srv", user_id=str(fake_user.id))
    await server.insert()
    tenant = TBTenant(name="SchedTen", username="tadmin", server_id=server, user_id=str(fake_user.id))
    await tenant.insert()

    with pytest.raises(HTTPException) as exc404_t:
        await _resolve_tenant_or_404("non_existent_tenant")
    assert exc404_t.value.status_code == 404

    with pytest.raises(HTTPException) as exc404_task:
        await _get_task_or_404("non_existent_task")
    assert exc404_task.value.status_code == 404

    now_utc = datetime.now(timezone.utc)
    task = TBScheduledTask(
        name="Scheduled Task 1",
        task_name="tasks.download_telemetry",
        cron_expression="0 0 * * *",
        tenant_id=tenant,
        payload={"limit": 50},
        is_active=True,
        next_run_time=now_utc + timedelta(days=1)
    )
    await task.insert()

    with pytest.raises(HTTPException) as exc422:
        await update_scheduled_task(
            task_id=str(task.id),
            payload=ScheduledTaskUpdate.model_construct(cron_expression="invalid-cron"),
            current_user=fake_user
        )
    assert exc422.value.status_code == 422

    upd_res = await update_scheduled_task(
        task_id=str(task.id),
        payload=ScheduledTaskUpdate(cron_expression="*/15 * * * *"),
        current_user=fake_user
    )
    assert upd_res.cron_expression == "*/15 * * * *"

    with patch("api.endpoints.scheduler.router.get_arq_pool", side_effect=Exception("ARQ pool offline")):
        with pytest.raises(HTTPException) as exc500:
            await trigger_scheduled_task(task_id=str(task.id), current_user=fake_user)
        assert exc500.value.status_code == 500

    del_res = await delete_scheduled_task(task_id=str(task.id), current_user=fake_user)
    assert del_res["status"] == "success"
    assert await TBScheduledTask.get(task.id) is None


@pytest.mark.asyncio
async def test_servers_and_nodes_router_extended():
    fake_user = await setup_db("fase9_db_srv_nodes")

    req_srv = ServerCreateRequest(
        name="ColdStartServer",
        base_url="http://coldstart.srv",
        username="sysadmin@tb.org",
        password="sysadmin_password"
    )

    with patch("api.endpoints.servers.router.ThingsBoardClient") as MockTB:
        mock_instance = MagicMock()
        mock_instance.login = AsyncMock(return_value={"token": "sys_tok_123", "refreshToken": "sys_ref_123"})
        MockTB.return_value = mock_instance

        created_srv = await create_server(request=req_srv, current_user=fake_user)
        assert created_srv.name == "ColdStartServer"

    server_doc = await TBServer.get(PydanticObjectId(created_srv.id))
    assert server_doc.get_token() == "sys_tok_123"

    fake_user.is_superuser = False
    fake_user.role = "user"
    srv_list = await list_servers(current_user=fake_user)
    assert isinstance(srv_list, list)

    fake_user.is_superuser = True
    upd_req = ServerUpdateRequest(
        ssh_auth_method=SSHAuthMethod.PASSWORD,
        ssh_password="ssh_secret_pw"
    )
    upd_srv = await update_server(server_id=str(server_doc.id), request=upd_req, current_user=fake_user)
    assert upd_srv.ssh_auth_method == SSHAuthMethod.PASSWORD

    node = TBNode(
        server_id=server_doc,
        name="WorkerNode1",
        node_role="worker",
        ssh_host="192.168.1.50",
        ssh_port=22,
        ssh_username="root",
        ssh_auth_method=SSHAuthMethod.PEM_KEY
    )
    node.set_ssh_pem_file("-----BEGIN RSA PRIVATE KEY-----\nMIIE...\n-----END RSA PRIVATE KEY-----")
    await node.insert()

    node_upd_req = NodeUpdateRequest(
        ssh_auth_method=SSHAuthMethod.PASSWORD,
        ssh_password="new_node_password"
    )
    upd_node_res = await update_server_node(
        server_id=str(server_doc.id),
        node_id=str(node.id),
        request=node_upd_req,
        current_user=fake_user
    )
    assert upd_node_res.ssh_auth_method == SSHAuthMethod.PASSWORD


@pytest.mark.asyncio
async def test_workers_dispatcher_and_cleanup():
    await setup_db("fase9_db_workers")
    now_utc = datetime.now(timezone.utc)
    due_task = TBScheduledTask(
        name="Due Task",
        task_name="tasks.download_telemetry",
        cron_expression="*/5 * * * *",
        is_active=True,
        next_run_time=now_utc - timedelta(minutes=2)
    )
    await due_task.insert()

    mock_arq_redis = MagicMock()
    mock_job = MagicMock()
    mock_job.job_id = "arq_dispatched_job_123"
    mock_arq_redis.enqueue_job = AsyncMock(return_value=mock_job)

    ctx = {"redis": mock_arq_redis}
    await master_dispatcher_task(ctx)

    reloaded = await TBScheduledTask.get(due_task.id)
    assert "DISPATCHED" in reloaded.last_run_status
    assert reloaded.next_run_time > now_utc

    server = TBServer(name="OldSrv", base_url="http://oldsrv.org")
    await server.insert()
    tenant = TBTenant(name="OldTen", username="tadmin", server_id=server)
    await tenant.insert()

    old_date = now_utc - timedelta(days=40)
    old_backup = TBBackup(
        tenant_id=tenant,
        task_id="old_backup_task",
        file_name="old_non_existent.zip",
        file_size=100,
        backup_type="telemetry",
        requested_by="admin",
        start_date="2026-01-01T00:00:00",
        end_date="2026-01-02T00:00:00"
    )
    old_backup.created_at = old_date
    await old_backup.insert()

    mock_redis = MagicMock()
    async def mock_scan(match):
        return
        yield
    mock_redis.scan_iter = mock_scan

    stats = await cleanup_old_backups_task(ctx={"redis": mock_redis}, days_to_keep=30)
    assert stats["status"] == "SUCCESS"
    assert stats["deleted_db_records"] >= 1
    assert await TBBackup.get(old_backup.id) is None

    mtime = _get_directory_latest_mtime(".")
    assert mtime > 0

    active_redis = await _is_task_active_in_redis("any_id", mock_redis)
    assert active_redis is False


@pytest.mark.asyncio
async def test_workers_incremental_and_sysinfo_and_telegram():
    with patch("workers.tasks.run_incremental_tenant_backup", new_callable=AsyncMock) as mock_incr:
        mock_incr.return_value = {"status": "SUCCESS", "records": 100}
        res = await execute_incremental_tenant_backup_task(
            ctx={"job_id": "job_incr_1"},
            payload={"tenant_id": "ten1"}
        )
        assert res["status"] == "SUCCESS"

    with patch("core.services.system_info_service.collect_all_servers_system_info", new_callable=AsyncMock) as mock_sys:
        mock_sys.return_value = [{"server_id": "srv1", "status": "SUCCESS"}]
        res_sys = await collect_servers_system_info_task(
            ctx={"job_id": "job_sys_1"},
            payload={"server_id": "srv1"}
        )
        assert len(res_sys) == 1

    res_tg_no_msg = await send_telegram_alert_task(ctx={}, payload={})
    assert res_tg_no_msg["sent"] is False
    assert res_tg_no_msg["reason"] == "missing_message"

    with patch("workers.tasks.dispatch_debounced_alert", new_callable=AsyncMock) as mock_disp:
        mock_disp.return_value = {"sent": True, "message_id": 999, "alert_hash": "hash123"}
        res_tg = await send_telegram_alert_task(
            ctx={},
            payload={"message": "Test alert notification", "ttl_seconds": 60}
        )
        assert res_tg["sent"] is True


@pytest.mark.asyncio
async def test_telemetry_service_helpers_and_token_refresh():
    await setup_db("fase9_db_telemetry")
    server = TBServer(name="RefreshSrv", base_url="http://ref.srv")
    await server.insert()
    tenant = TBTenant(name="RefreshTen", username="tadmin", password="tpassword", server_id=server)
    await tenant.insert()

    mock_tb = MagicMock()
    mock_tb.base_url = "http://ref.srv"
    mock_tb.username = "tadmin"
    mock_tb.password = "tpassword"
    mock_tb.refresh_token = "old_refresh_tok"
    mock_tb.refresh_jwt_token = AsyncMock(return_value={"token": "new_tok_refreshed", "refreshToken": "new_ref_tok"})

    token_ref = ["old_tok"]
    payload = {"refresh_token": "old_refresh_tok"}

    new_t, new_r = await refresh_tenant_tokens_in_db(
        tenant_id=str(tenant.id),
        tb=mock_tb,
        token_ref=token_ref,
        payload=payload
    )
    assert new_t == "new_tok_refreshed"
    assert token_ref[0] == "new_tok_refreshed"

    reloaded_ten = await TBTenant.get(tenant.id)
    assert reloaded_ten.get_token() == "new_tok_refreshed"

    sample_data = {
        "data": [
            {"ts": 1000, "val": 10},
            {"ts": 2000, "val": 20},
            {"ts": 3000, "val": 30}
        ]
    }
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as f:
        json.dump(sample_data, f)
        temp_file = f.name

    try:
        highest = _sync_get_highest_ts(temp_file)
        assert highest == 3000

        records = _sync_read_records_in_range(temp_file, start_ts=1500, end_ts=2500)
        assert len(records) == 1
        assert records[0]["ts"] == 2000
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)


@pytest.mark.asyncio
async def test_system_info_cold_start_without_credentials():
    await setup_db("fase9_db_sysinfo")
    server = TBServer(
        name="NoCredsServer",
        base_url="http://nocreds.srv",
        username=None,
        password=None
    )
    await server.insert()

    res = await collect_all_servers_system_info(server_id=str(server.id))
    assert res["status"] == "ERROR"
    assert res["failed"] == 1
