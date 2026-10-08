import os
import sys
import json
import pytest
from unittest.mock import AsyncMock, patch
from beanie import init_beanie
from mongomock_motor import AsyncMongoMockClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.models.tb_email_config import TBEmailConfig
from core.models.tb_server import TBServer
from core.models.tb_tenant import TBTenant
from core.models.tb_backup import TBBackup
from core.models.user import User
from workers.tasks import generate_monthly_heatmap_task


async def setup_test_db(db_suffix: str):
    client = AsyncMongoMockClient()
    db = client[f"test_heatmap_whitelist_{db_suffix}_{os.getpid()}"]
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


def build_mock_redis():
    mock_redis = AsyncMock()
    mock_redis.expire = AsyncMock(return_value=True)
    mock_redis.publish = AsyncMock(return_value=1)
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=True)
    mock_redis.hdel = AsyncMock(return_value=1)
    mock_redis.hset = AsyncMock(return_value=1)
    return mock_redis


@pytest.mark.asyncio
async def test_heatmap_whitelist_from_list():
    """Verifica que whitelist_keys se extraiga fielmente si heatmap_config es una lista."""
    await setup_test_db("list")
    server = TBServer(name="Server-WL-List", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-WL-List",
        username="admin_list@test.com",
        custom_metadata={"heatmap_config": ["temperatura", "humedad"]}
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-01"}, "name": "Sensor-01"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_wl_list_1", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=["temperatura", "humedad"])), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", AsyncMock(return_value={})), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"
        assert len(captured_sections) == 2
        metrics = [sec["metric_name"] for sec in captured_sections]
        assert "temperatura" in metrics
        assert "humedad" in metrics


@pytest.mark.asyncio
async def test_heatmap_whitelist_from_comma_string():
    """Verifica que whitelist_keys se extraiga fielmente si heatmap_config es una cadena separada por comas."""
    await setup_test_db("str")
    server = TBServer(name="Server-WL-Str", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-WL-Str",
        username="admin_str@test.com",
        custom_metadata={"heatmap_config": "temperatura, presion, flujo"}
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-02"}, "name": "Sensor-02"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_wl_str_2", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=["temperatura", "presion", "flujo"])), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", AsyncMock(return_value={})), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"
        assert len(captured_sections) == 3
        metrics = [sec["metric_name"] for sec in captured_sections]
        assert metrics == ["temperatura", "presion", "flujo"]


@pytest.mark.asyncio
async def test_heatmap_whitelist_from_dict_variables():
    """Verifica que whitelist_keys reconozca la llave 'variables' en un dict heatmap_config."""
    await setup_test_db("variables")
    server = TBServer(name="Server-WL-Vars", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-WL-Vars",
        username="admin_vars@test.com",
        custom_metadata={"heatmap_config": {"variables": ["temp_aire", "hum_suelo"]}}
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-03"}, "name": "Sensor-03"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_wl_vars_3", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=["temp_aire", "hum_suelo"])), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", AsyncMock(return_value={})), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"
        assert len(captured_sections) == 2
        metrics = [sec["metric_name"] for sec in captured_sections]
        assert "temp_aire" in metrics
        assert "hum_suelo" in metrics


@pytest.mark.asyncio
async def test_heatmap_whitelist_from_dict_keys_excluding_reserved():
    """Verifica que si heatmap_config define variables como llaves del dict, se excluyan llaves reservadas."""
    await setup_test_db("keys_dict")
    server = TBServer(name="Server-WL-DictKeys", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-WL-DictKeys",
        username="admin_dictkeys@test.com",
        custom_metadata={
            "heatmap_config": {
                "temperatura": {"min": 10},
                "presion": {"max": 50},
                "rules": [{"limit": 30, "operator": ">=", "color": "#FF0000"}],
                "time_zone": "America/Mexico_City",
                "aggregation": "AVG",
                "year": 2026,
                "month": 6
            }
        }
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-04"}, "name": "Sensor-04"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_wl_dictkeys_4", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=["temperatura", "presion"])), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", AsyncMock(return_value={})), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"
        assert len(captured_sections) == 2
        metrics = [sec["metric_name"] for sec in captured_sections]
        assert "temperatura" in metrics
        assert "presion" in metrics
        # Asegurar que las llaves reservadas NO se convirtieron en métricas
        assert "rules" not in metrics
        assert "time_zone" not in metrics
        assert "aggregation" not in metrics


@pytest.mark.asyncio
async def test_heatmap_whitelist_payload_override_and_fallback_rejected():
    """
    Verifica que:
    1. Si tenant no tiene custom_metadata.heatmap_config, cualquier payload.keys sea estrictamente RECHAZADO.
    2. NO se consulte a ThingsBoard para adivinar las llaves del dispositivo.
    3. Se emita el warning correspondiente y se genere la sección 'Sin Variables Admitidas'.
    """
    await setup_test_db("rejection")
    server = TBServer(name="Server-WL-Reject", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Sin-Config",
        username="admin_sin_config@test.com"
        # Sin custom_metadata.heatmap_config
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-05"}, "name": "Sensor-05"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_wl_reject_5", "job_try": 1, "redis": mock_redis}
    # Intento de inyectar variables ajenas por payload
    payload = {
        "tenant_id": str(tenant.id),
        "keys": ["variable_inyectada", "otra_variable"],
        "heatmap_config": {"keys": ["hacked_key"]},
        "send_email": False
    }

    mock_get_keys = AsyncMock(return_value=["device_internal_var1", "device_internal_var2"])
    mock_get_telem = AsyncMock(return_value={})

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", mock_get_keys), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", mock_get_telem), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"

        # get_entity_timeseries_keys NO debió ser llamado para adivinar variables
        mock_get_keys.assert_not_called()
        # get_entity_telemetry NO debió ser llamado
        mock_get_telem.assert_not_called()

        assert len(captured_sections) == 1
        sec = captured_sections[0]
        assert sec["metric_name"] == "Sin Variables Admitidas"
        assert sec["device_name"] == "Sensor-05"


@pytest.mark.asyncio
async def test_heatmap_whitelist_device_keys_mismatch_skips_unmatched_variables():
    """
    Verifica que si whitelist_keys contiene 'temperatura' pero el dispositivo sólo posee 'presion',
    la tarea NO consulte telemetría de 'presion' ni intente sustituirla.
    """
    await setup_test_db("mismatch")
    server = TBServer(name="Server-WL-Mismatch", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Mismatch",
        username="admin_mismatch@test.com",
        custom_metadata={"heatmap_config": ["temperatura"]}
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-06"}, "name": "Sensor-Solo-Presion"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    # El dispositivo sólo reporta 'presion'
    dev_keys = ["presion", "voltaje"]

    mock_get_telem = AsyncMock(return_value={})
    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_wl_mismatch_6", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=dev_keys)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", mock_get_telem), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"

        # Como el dispositivo no tiene 'temperatura', get_entity_telemetry NO debió ser llamado
        mock_get_telem.assert_not_called()

        # Debe generar sección informativa de Sin Datos
        assert len(captured_sections) == 1
        sec = captured_sections[0]
        assert sec["metric_name"] == "Sin Datos"


@pytest.mark.asyncio
async def test_heatmap_telemetry_point_substitution_eradicated():
    """
    Verifica que si ThingsBoard retorna un diccionario con una llave ajena (ej: {'otra_variable': [...]}),
    la tarea NO la sustituya ciegamente para la métrica solicitada 'temperatura'.
    """
    await setup_test_db("no_substitution")
    server = TBServer(name="Server-No-Subst", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-No-Subst",
        username="admin_nosubst@test.com",
        custom_metadata={"heatmap_config": ["temperatura"]}
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-07"}, "name": "Sensor-07"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    # El dispositivo dice tener 'temperatura'
    dev_keys = ["temperatura"]

    # Pero la API de telemetría retorna datos bajo una llave completamente diferente
    unrelated_telem = {
        "otra_variable_ajena": [
            {"ts": 1785542400000, "value": "99.9"}
        ]
    }

    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_no_subst_7", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=dev_keys)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", AsyncMock(return_value=unrelated_telem)), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"
        assert len(captured_sections) == 1
        sec = captured_sections[0]
        assert sec["metric_name"] == "temperatura"
        # Todos los puntos deben ser 'NA', no se aceptó la data de 'otra_variable_ajena'
        assert all(all(val == "NA" for val in row) for row in sec["data"])
