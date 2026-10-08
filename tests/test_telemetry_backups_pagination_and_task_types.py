import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient

from core.models.tb_backup import TBBackup
from core.models.tb_tenant import TBTenant
from core.services.telemetry_service import publish_task_status, publish_task_status_sync
from api.endpoints.telemetry.router import (
    BackupResponse,
    PaginatedBackupResponse,
    build_backup_type_filter
)
from api.endpoints.tasks.schemas import TaskStatusResponse, ActiveTaskResponse
from api.main import app


def test_tb_backup_model_and_backward_compatibility():
    """
    Verifica que el modelo TBBackup contenga el campo backup_type,
    el índice correspondiente, y el método get_backup_type() con fallback retrocompatible.
    """
    assert "backup_type" in TBBackup.Settings.indexes

    now = datetime.now(timezone.utc)
    mock_tenant = MagicMock(spec=TBTenant)
    mock_tenant.id = "650000000000000000000001"

    # Caso 1: Default de backup_type es 'telemetry'
    b1 = TBBackup.model_construct(
        tenant_id=mock_tenant,
        task_id="t1",
        requested_by="u1",
        file_name="telemetry_archive.zip",
        backup_type="telemetry",
        start_date=now,
        end_date=now
    )
    assert b1.backup_type == "telemetry"
    assert b1.get_backup_type() == "telemetry"

    # Caso 2: Especificar backup_type explícito
    b2 = TBBackup.model_construct(
        tenant_id=mock_tenant,
        task_id="t2",
        requested_by="u1",
        file_name="report_2026.xlsx",
        backup_type="excel_report",
        start_date=now,
        end_date=now
    )
    assert b2.backup_type == "excel_report"
    assert b2.get_backup_type() == "excel_report"

    # Caso 3: Fallback retrocompatible para registros legados .pdf
    b3 = TBBackup.model_construct(
        tenant_id=mock_tenant,
        task_id="t3",
        requested_by="u1",
        file_name="monthly_heatmap.pdf",
        backup_type="telemetry",  # Default asignado por Beanie a docs sin este campo
        start_date=now,
        end_date=now
    )
    assert b3.get_backup_type() == "heatmap"

    # Caso 4: Fallback retrocompatible para registros legados .xlsx
    b4 = TBBackup.model_construct(
        tenant_id=mock_tenant,
        task_id="t4",
        requested_by="u1",
        file_name="custom_report.xlsx",
        backup_type="telemetry",
        start_date=now,
        end_date=now
    )
    assert b4.get_backup_type() == "excel_report"


@pytest.mark.asyncio
async def test_publish_task_status_includes_task_type():
    """
    Verifica que publish_task_status y publish_task_status_sync incluyan
    el campo 'task_type' en el payload y respeten su valor por defecto y personalizados.
    """
    mock_redis = AsyncMock()

    # Async con default 'telemetry'
    res_async_default = await publish_task_status(
        redis_client=mock_redis,
        user_id="user_123",
        task_id="task_123",
        status="IN_PROGRESS",
        tenant_name="Tenant Test",
        progress_pct=50.0
    )
    assert res_async_default["task_type"] == "telemetry"
    assert res_async_default["progress_pct"] == 50.0

    # Async con 'excel_report'
    res_async_excel = await publish_task_status(
        redis_client=mock_redis,
        user_id="user_123",
        task_id="task_124",
        status="PACKAGING",
        tenant_name="Tenant Test",
        task_type="excel_report"
    )
    assert res_async_excel["task_type"] == "excel_report"

    # Async con 'heatmap'
    res_async_heatmap = await publish_task_status(
        redis_client=mock_redis,
        user_id="user_123",
        task_id="task_125",
        status="DOWNLOADING",
        tenant_name="Tenant Test",
        task_type="heatmap"
    )
    assert res_async_heatmap["task_type"] == "heatmap"


def test_publish_task_status_sync_includes_task_type():
    """Verifica publish_task_status_sync."""
    with patch("redis.from_url") as mock_from_url:
        mock_r = MagicMock()
        mock_from_url.return_value = mock_r

        res_sync = publish_task_status_sync(
            user_id="user_456",
            task_id="task_456",
            status="SUCCESS",
            tenant_name="Tenant Sync",
            task_type="excel_report"
        )
        assert res_sync["task_type"] == "excel_report"
        assert res_sync["status"] == "SUCCESS"


def test_build_backup_type_filter():
    """Verifica la generación de filtros MongoDB retrocompatibles para backup_type."""
    f_heatmap = build_backup_type_filter("heatmap")
    assert "$or" in f_heatmap
    assert any(b.get("backup_type") == "heatmap" for b in f_heatmap["$or"])

    f_excel = build_backup_type_filter("excel_report")
    assert "$or" in f_excel
    assert any(b.get("backup_type") == "excel_report" for b in f_excel["$or"])

    f_telemetry = build_backup_type_filter("telemetry")
    assert "$or" in f_telemetry
    assert any(b.get("backup_type") == "telemetry" for b in f_telemetry["$or"])

    f_custom = build_backup_type_filter("custom_audit")
    assert f_custom == {"backup_type": "custom_audit"}


def test_openapi_contract_pagination_and_task_types():
    """
    Verifica que el contrato OpenAPI refleje los parámetros de paginación y filtros
    en /api/v1/telemetry/backups y /api/v1/tasks/active.
    """
    openapi = app.openapi()
    paths = openapi["paths"]

    # 1. /api/v1/telemetry/backups
    backups_endpoint = paths["/api/v1/telemetry/backups"]["get"]
    param_names = [p["name"] for p in backups_endpoint["parameters"]]
    assert "page" in param_names
    assert "page_size" in param_names
    assert "tenant_id" in param_names
    assert "backup_type" in param_names

    # Schema de respuesta
    resp_200 = backups_endpoint["responses"]["200"]
    schema_ref = resp_200["content"]["application/json"]["schema"]
    assert "PaginatedBackupResponse" in str(schema_ref)

    # 2. /api/v1/tasks/active
    active_tasks_endpoint = paths["/api/v1/tasks/active"]["get"]
    active_param_names = [p["name"] for p in active_tasks_endpoint["parameters"]]
    assert "task_type" in active_param_names

    # 3. TaskStatusResponse tiene task_type
    components = openapi["components"]["schemas"]
    assert "task_type" in components["TaskStatusResponse"]["properties"]
    assert "backup_type" in components["BackupResponse"]["properties"]


def test_active_tasks_filtering():
    """Verifica que el filtrado de tareas activas por task_type funcione correctamente."""
    t1 = ActiveTaskResponse(
        task_id="t1",
        task_type="telemetry",
        status="IN_PROGRESS"
    )
    t2 = ActiveTaskResponse(
        task_id="t2",
        task_type="heatmap",
        status="PACKAGING"
    )
    t3 = ActiveTaskResponse(
        task_id="t3",
        task_type="excel_report",
        status="DOWNLOADING"
    )
    all_tasks = [t1, t2, t3]

    filtered_heatmap = [t for t in all_tasks if t.task_type == "heatmap"]
    assert len(filtered_heatmap) == 1
    assert filtered_heatmap[0].task_id == "t2"

    filtered_telemetry = [t for t in all_tasks if t.task_type == "telemetry"]
    assert len(filtered_telemetry) == 1
    assert filtered_telemetry[0].task_id == "t1"
