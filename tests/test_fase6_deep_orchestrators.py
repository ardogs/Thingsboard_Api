"""
Test Suite Fase 6: Orquestadores Profundos de Telemetría, Data Lake, Reportes Excel y Relaciones de Sitios
Cubre en profundidad:
- core/services/telemetry_service.py (run_download_orchestrator)
- core/services/excel_report_service.py (run_excel_report_orchestrator)
- core/services/incremental_backup_service.py (run_incremental_tenant_backup)
- core/services/system_info_service.py (collect_all_servers_system_info)
- api/endpoints/devices/router.py (list_sites_with_devices)
"""

import os
import io
import json
import shutil
import tempfile
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx
from beanie import init_beanie, PydanticObjectId
from mongomock_motor import AsyncMongoMockClient

from core.models.user import User
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.tb_node import TBNode
from core.models.tb_backup import TBBackup
from core.models.tb_scheduled_task import TBScheduledTask
from core.models.tb_email_config import TBEmailConfig
from core.models.audit_log import AuditLog
from core.tb_client import ThingsBoardClient

# Services
from core.services.telemetry_service import run_download_orchestrator
from core.services.excel_report_service import run_excel_report_orchestrator
from core.services.incremental_backup_service import run_incremental_tenant_backup
from core.services.system_info_service import collect_all_servers_system_info
from api.endpoints.devices.router import list_sites_with_devices


async def init_mock_db(db_name: str = "fase6_deep_db"):
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
# 1. RUN_DOWNLOAD_ORCHESTRATOR PROFUNDO
# ==============================================================================

@pytest.mark.asyncio
async def test_run_download_orchestrator_deep():
    await init_mock_db("download_deep_db")
    server = TBServer(name="SRV_DEEP", base_url="https://tb-deep.com")
    await server.insert()
    tenant = TBTenant(name="TNT_DEEP", server_id=server, username="admin_deep")
    tenant.set_tokens("valid_jwt", "valid_ref")
    await tenant.insert()
    user = User(username="deep_user", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Mocking ThingsBoard Client
        mock_tb = MagicMock(spec=ThingsBoardClient)
        mock_tb.base_url = server.base_url
        mock_tb.token = "valid_jwt"
        mock_tb.get_tenant_devices = AsyncMock(return_value={
            "data": [{"id": {"id": "dev_uuid_1"}, "name": "Sensor_Deep_1", "type": "default"}],
            "hasNext": False
        })
        mock_tb.get_entity_timeseries_keys = AsyncMock(return_value=["temperature", "humidity"])
        mock_tb.get_entity_telemetry = AsyncMock(return_value={
            "temperature": [{"ts": 1754000000000, "value": "22.5"}],
            "humidity": [{"ts": 1754000000000, "value": "60.0"}]
        })

        mock_redis = AsyncMock()
        mock_redis.publish = AsyncMock(return_value=1)
        mock_redis.hset = AsyncMock(return_value=1)
        mock_redis.hdel = AsyncMock(return_value=1)
        mock_redis.delete = AsyncMock(return_value=1)

        payload = {
            "tenant_id": str(tenant.id),
            "tenant_name": tenant.name,
            "start_date": "2026-08-01T00:00:00Z",
            "end_date": "2026-08-05T23:59:59Z",
            "time_zone": "America/Mexico_City",
            "entity_type": "DEVICE",
            "concurrency_limit": 2,
            "base_storage_dir": os.path.join(tmp_dir, "tenant_backups"),
        }

        # Ejecutar el orquestador masivo
        await run_download_orchestrator(
            task_id="orch_task_100",
            tb=mock_tb,
            user_id=str(user.id),
            payload=payload,
            redis_client=mock_redis
        )

        # Verificar que el documento TBBackup se insertó en MongoDB
        backup = await TBBackup.find_one(TBBackup.task_id == "orch_task_100")
        assert backup is not None
        assert backup.backup_type == "telemetry"
        assert backup.file_name.endswith(".zip")


# ==============================================================================
# 2. RUN_EXCEL_REPORT_ORCHESTRATOR PROFUNDO (SINGLE FILE & MULTI FILE)
# ==============================================================================

@pytest.mark.asyncio
async def test_run_excel_report_orchestrator_deep():
    await init_mock_db("excel_deep_db")
    server = TBServer(name="SRV_EXC", base_url="https://tb-exc.com")
    await server.insert()
    tenant = TBTenant(name="TNT_EXC", server_id=server, username="admin_exc")
    tenant.set_tokens("valid_jwt", "valid_ref")
    await tenant.insert()
    user = User(username="exc_user", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    with tempfile.TemporaryDirectory() as tmp_dir:
        mock_tb = MagicMock(spec=ThingsBoardClient)
        mock_tb.base_url = server.base_url
        mock_tb.token = "valid_jwt"
        mock_tb.get_tenant_devices = AsyncMock(return_value={
            "data": [{"id": {"id": "dev_uuid_2"}, "name": "Sensor_Exc_1", "type": "default"}],
            "hasNext": False
        })
        mock_tb.get_entity_timeseries_keys = AsyncMock(return_value=["temp"])
        mock_tb.get_entity_telemetry = AsyncMock(return_value={
            "temp": [
                {"ts": 1754000000000, "value": "21.0"},
                {"ts": 1754000060000, "value": "21.5"}
            ]
        })

        mock_redis = AsyncMock()

        # Caso 1: combine_in_single_file = True (un solo archivo multi-hoja)
        payload_single = {
            "tenant_id": str(tenant.id),
            "tenant_name": tenant.name,
            "start_date": "2026-08-01T00:00:00Z",
            "end_date": "2026-08-03T23:59:59Z",
            "combine_in_single_file": True,
            "base_storage_dir": os.path.join(tmp_dir, "tenant_backups"),
        }

        res_single = await run_excel_report_orchestrator(
            task_id="excel_task_single",
            tb=mock_tb,
            user_id=str(user.id),
            payload=payload_single,
            redis_client=mock_redis
        )
        assert res_single["status"] == "SUCCESS"
        assert res_single["file_name"].endswith(".xlsx")

        backup_single = await TBBackup.find_one(TBBackup.task_id == "excel_task_single")
        assert backup_single is not None
        assert backup_single.backup_type == "excel_report"

        # Caso 2: combine_in_single_file = False (múltiples archivos empaquetados en ZIP)
        payload_multi = {
            "tenant_id": str(tenant.id),
            "tenant_name": tenant.name,
            "start_date": "2026-08-01T00:00:00Z",
            "end_date": "2026-08-03T23:59:59Z",
            "combine_in_single_file": False,
            "base_storage_dir": os.path.join(tmp_dir, "tenant_backups"),
        }

        res_multi = await run_excel_report_orchestrator(
            task_id="excel_task_multi",
            tb=mock_tb,
            user_id=str(user.id),
            payload=payload_multi,
            redis_client=mock_redis
        )
        assert res_multi["status"] == "SUCCESS"
        assert res_multi["file_name"].endswith(".zip")


# ==============================================================================
# 3. RUN_INCREMENTAL_TENANT_BACKUP PROFUNDO
# ==============================================================================

@pytest.mark.asyncio
async def test_run_incremental_tenant_backup_deep():
    await init_mock_db("incremental_deep_db")
    server = TBServer(name="SRV_INCR", base_url="https://tb-incr.com")
    await server.insert()
    tenant = TBTenant(name="TNT_INCR", server_id=server, username="admin_incr")
    tenant.set_tokens("valid_jwt", "valid_ref")
    await tenant.insert()

    with tempfile.TemporaryDirectory() as tmp_dir:
        mock_tb = MagicMock(spec=ThingsBoardClient)
        mock_tb.base_url = server.base_url
        mock_tb.token = "valid_jwt"
        mock_tb.get_tenant_devices = AsyncMock(return_value={
            "data": [{"id": {"id": "dev_uuid_incr"}, "name": "Sensor_Incr_1", "type": "default"}],
            "hasNext": False
        })
        mock_tb.get_entity_timeseries_keys = AsyncMock(return_value=["power"])
        mock_tb.get_entity_telemetry = AsyncMock(return_value={
            "power": [{"ts": 1754000000000, "value": "120.5"}]
        })

        payload = {
            "tenant_id": str(tenant.id),
            "year": 2026,
            "month": 8,
            "concurrency_limit": 2,
            "base_storage_dir": os.path.join(tmp_dir, "tenant_backups"),
        }

        with patch("core.services.incremental_backup_service.init_db", AsyncMock()), \
             patch("core.services.incremental_backup_service.ThingsBoardClient", return_value=mock_tb):
            res_incr = await run_incremental_tenant_backup(payload)
            assert res_incr["tenant_id"] == str(tenant.id)
            assert res_incr["year_str"] == "2026"
            assert res_incr["month_str"] == "08"
            assert res_incr["total_keys"] == 1


# ==============================================================================
# 4. COLLECT_ALL_SERVERS_SYSTEM_INFO PROFUNDO
# ==============================================================================

@pytest.mark.asyncio
async def test_collect_all_servers_system_info_deep():
    await init_mock_db("sysinfo_deep_db")
    server1 = TBServer(name="SRV_SYS_1", base_url="https://tb-sys1.com", username="sys1")
    server1.set_tokens("tok1", "ref1")
    await server1.insert()

    server2 = TBServer(name="SRV_SYS_2", base_url="https://tb-sys2.com", username="sys2")
    server2.set_tokens("tok2", "ref2")
    await server2.insert()

    mock_info = {
        "cpuUsage": 25.5,
        "memoryUsage": 50.0,
        "discUsage": 65.0,
        "totalMemory": 16000,
        "freeMemory": 8000,
        "totalDisc": 100000,
        "freeDisc": 35000,
    }

    mock_tb = MagicMock(spec=ThingsBoardClient)
    mock_tb.get_system_info = AsyncMock(return_value=mock_info)

    with patch("core.services.system_info_service.ThingsBoardClient", return_value=mock_tb):
        # 1. Servidor individual
        res_single = await collect_all_servers_system_info(server_id=str(server1.id))
        assert res_single["total_servers"] == 1
        assert res_single["successful"] == 1
        assert len(res_single["servers"]) == 1

        # 2. Todos los servidores
        res_all = await collect_all_servers_system_info(server_id="all")
        assert res_all["total_servers"] == 2
        assert res_all["successful"] == 2


# ==============================================================================
# 5. LIST_SITES_WITH_DEVICES PROFUNDO
# ==============================================================================

@pytest.mark.asyncio
async def test_list_sites_with_devices_deep():
    await init_mock_db("sites_deep_db")
    server = TBServer(name="SRV_SITES", base_url="https://tb-sites.com")
    await server.insert()
    tenant = TBTenant(name="TNT_SITES", server_id=server, username="admin_sites")
    tenant.set_tokens("valid_jwt", "valid_ref")
    await tenant.insert()
    user = User(username="sites_user", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    mock_tb = MagicMock(spec=ThingsBoardClient)
    mock_tb.base_url = server.base_url
    mock_tb.token = "valid_jwt"
    mock_tb.timeout = 30.0

    # 1. find_entities_by_query retorna un Sitio (ASSET tipo SITE)
    mock_tb.find_entities_by_query = AsyncMock(return_value={
        "data": [
            {
                "id": {"id": "site_uuid_1", "entityType": "ASSET"},
                "name": "Planta Industrial Norte",
                "type": "SITE",
                "label": "Planta 1"
            }
        ]
    })

    # 2. get_tenant_devices precarga los dispositivos
    mock_tb.get_tenant_devices = AsyncMock(return_value={
        "data": [
            {
                "id": {"id": "dev_uuid_10", "entityType": "DEVICE"},
                "name": "Medidor_Potencia_01",
                "type": "powermeter",
                "label": "Tablero 1"
            }
        ]
    })

    # 3. get_entity_relations retorna la relación entre el Sitio y el Dispositivo
    mock_tb.get_entity_relations = AsyncMock(return_value=[
        {
            "from": {"id": "site_uuid_1", "entityType": "ASSET"},
            "to": {"id": "dev_uuid_10", "entityType": "DEVICE"},
            "type": "Contains"
        }
    ])

    with patch("api.endpoints.devices.router.ThingsBoardClient", return_value=mock_tb):
        sites_res = await list_sites_with_devices(
            tenant_id=str(tenant.id),
            current_user=user
        )

        assert len(sites_res) == 1
        site_obj = sites_res[0]
        assert site_obj.name == "Planta Industrial Norte"
        assert len(site_obj.devices) == 1
        dev_obj = site_obj.devices[0]
        assert dev_obj.name == "Medidor_Potencia_01"
        assert dev_obj.type == "powermeter"


# ==============================================================================
# 6. HEATMAP PDF GENERATION SÍNCRONA & PLACEHOLDER INTERPOLATION
# ==============================================================================

def test_heatmap_sync_pdf_and_interpolation():
    from core.services.heatmap_report_service import (
        _sync_generate_heatmap_pdf,
        interpolate_heatmap_placeholders,
        extract_heatmap_whitelist_and_config,
    )

    # 1. interpolate_heatmap_placeholders
    template = "Reporte de {mes año} para {tenant} (Periodo {period})"
    res = interpolate_heatmap_placeholders(
        template,
        mes_nombre="Agosto",
        anio=2026,
        period="2026-08",
        tenant="Planta Central"
    )
    assert res == "Reporte de Agosto 2026 para Planta Central (Periodo 2026-08)"

    # 2. extract_heatmap_whitelist_and_config
    cfg_dict = {
        "keys": ["temperature", "humidity"],
        "rules": [{"operator": ">=", "value": 30, "color": "#f00", "label": "Alto"}],
        "time_zone": "America/Mexico_City"
    }
    keys, rules, tz, agg, yr, mo = extract_heatmap_whitelist_and_config(cfg_dict, {})
    assert "temperature" in keys
    assert "humidity" in keys
    assert tz == "America/Mexico_City"

    # 3. _sync_generate_heatmap_pdf generación real
    with tempfile.TemporaryDirectory() as tmp_dir:
        pdf_path = os.path.join(tmp_dir, "test_heatmap.pdf")
        sections = [
            {
                "device_name": "Sensor_Norte",
                "metric_name": "Temperatura",
                "data": [[20.0] * 24, [28.0] * 24],
                "col_labels": [f"{h:02d}" for h in range(24)],
                "row_labels": ["01", "02"],
                "rules": [
                    {"operator": ">=", "value": 25.0, "color": "#ef4444", "label": "Alerta"},
                    {"operator": "<", "value": 25.0, "color": "#10b981", "label": "Normal"}
                ],
                "title": "Reporte Mensual Agosto 2026",
                "period": "2026-08",
                "tenant_name": "Tenant_Demo"
            }
        ]

        out_pdf = _sync_generate_heatmap_pdf(
            matrix_data=sections,
            rules_data=[],
            output_pdf_path=pdf_path,
            title="Reporte mensual | mapas de calor",
            subtitle="Tenant_Demo"
        )

        assert os.path.exists(out_pdf)
        assert os.path.getsize(out_pdf) > 1000

