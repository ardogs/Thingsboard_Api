import os
import sys
import re
import json
import time
import asyncio
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
    """Inicializa la base de datos mock aislada con mongomock_motor y Beanie."""
    client = AsyncMongoMockClient()
    db = client[f"test_adv_whitelist_{db_suffix}_{os.getpid()}"]
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
    """Genera un cliente Redis simulado con todas las operaciones requeridas."""
    mock_redis = AsyncMock()
    mock_redis.expire = AsyncMock(return_value=True)
    mock_redis.publish = AsyncMock(return_value=1)
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=True)
    mock_redis.hdel = AsyncMock(return_value=1)
    mock_redis.hset = AsyncMock(return_value=1)
    return mock_redis


# ==============================================================================
# VECTOR 1: ATAQUES CON CONFIGURACIÓN MALFORMADA (MALFORMED HEATMAP_CONFIG)
# ==============================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("malformed_config, expected_metrics, desc", [
    (None, [], "Configuración None"),
    (12345, [], "Configuración entera escalar"),
    (99.99, [], "Configuración flotante escalar"),
    (True, [], "Configuración booleana True"),
    (False, [], "Configuración booleana False"),
    ("", [], "Cadena vacía"),
    ("   ,   \t\n,   ", [], "Cadena con espacios en blanco y comas vacías"),
    ([None, "", "   ", True, False], [], "Lista con None, vacíos y booleanos"),
    ({"keys": []}, [], "Dict con keys como lista vacía"),
    ({"variables": []}, [], "Dict con variables como lista vacía"),
    ({"whitelist": []}, [], "Dict con whitelist como lista vacía"),
    ({"telemetry_keys": []}, [], "Dict con telemetry_keys como lista vacía"),
    ({"allowed_keys": []}, [], "Dict con allowed_keys como lista vacía"),
    ({"default": []}, [], "Dict con default como lista vacía"),
    ({"keys": None}, [], "Dict con keys None"),
    ({"variables": None}, [], "Dict con variables None"),
    ({"keys": ""}, [], "Dict con keys cadena vacía"),
    ({"keys": "   ,   "}, [], "Dict con keys comas de solo espacios"),
    ({"keys": [None, "", "   ", True, False]}, [], "Dict con keys sucias sin strings válidos"),
    ({"send_email": True, "to_email": "attacker@evil.com", "timeout": 30}, [], "Dict con solo metadatos reservados"),
    (
        {"keys": [101, 202.5, "temp_celsius", "  humedad_relativa  "]},
        ["101", "202.5", "temp_celsius", "humedad_relativa"],
        "Lista con números y cadenas con padding"
    ),
    (
        {"keys": {"temp_sensor_1": "active", "presion_baro": "inactive", "   ": "empty", "": "empty"}},
        ["temp_sensor_1", "presion_baro"],
        "Dict donde las llaves de 'keys' son métricas y contienen entradas vacías"
    ),
    (
        "temp1, , temp2, temp1, , temp3",
        ["temp1", "temp2", "temp3"],
        "Cadena con duplicados y separadores vacíos"
    )
])
async def test_heatmap_whitelist_malformed_config_parsing(malformed_config, expected_metrics, desc):
    """
    Somete el parser de custom_metadata.heatmap_config a esquemas malformados, tipos inesperados,
    booleanos camuflados como enteros, diccionarios anidados y valores vacíos.
    Verifica que:
    1. Si no hay variables válidas, genere 'Sin Variables Admitidas' y NUNCA consulte llaves a ThingsBoard.
    2. Si hay variables válidas, extraiga fielmente solo las variables sanitizadas sin duplicados ni booleanos.
    """
    safe_suffix = re.sub(r'[^a-zA-Z0-9]', '_', desc)[:20] if 're' in globals() else str(abs(hash(desc)))[:8]
    await setup_test_db(f"malformed_{safe_suffix}")

    server = TBServer(name=f"Server-{safe_suffix}", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name=f"Tenant-{safe_suffix}",
        username=f"admin_{safe_suffix}@test.com",
        custom_metadata={"heatmap_config": malformed_config} if malformed_config is not None else {}
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-malformed"}, "name": "Device-Malformed"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    dev_keys = expected_metrics if expected_metrics else ["dev_key_1", "dev_key_2"]
    mock_get_keys = AsyncMock(return_value=dev_keys)
    mock_get_telem = AsyncMock(return_value={})
    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": f"job_malformed_{safe_suffix}", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", mock_get_keys), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", mock_get_telem), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"

        if not expected_metrics:
            # NO debe haber consultado a ThingsBoard para adivinar llaves
            mock_get_keys.assert_not_called()
            mock_get_telem.assert_not_called()
            assert len(captured_sections) == 1
            assert captured_sections[0]["metric_name"] == "Sin Variables Admitidas"
        else:
            # Se deben haber extraído exactamente las métricas esperadas
            metrics = [sec["metric_name"] for sec in captured_sections]
            for em in expected_metrics:
                assert em in metrics
            # Ningún booleano debe haber ingresado
            assert "True" not in metrics
            assert "False" not in metrics
            # Ninguna palabra reservada debe haber ingresado
            assert "to_email" not in metrics
            assert "send_email" not in metrics


# ==============================================================================
# VECTOR 2: INYECCIÓN DE PAYLOAD EXTERNO (PAYLOAD INJECTION ATTACK)
# ==============================================================================

@pytest.mark.asyncio
async def test_adversarial_payload_injection_cannot_override_whitelist():
    """
    Intenta subvertir la lista blanca inyectando parámetros en el cuerpo de la tarea:
    - 'keys'
    - 'whitelist'
    - 'heatmap_config'
    - 'telemetry_keys'
    - 'variables'
    Verifica que:
    1. Las variables legítimas del tenant ('temperatura') prevalecen.
    2. NINGUNA de las variables inyectadas en payload es procesada, consultada o reflejada.
    """
    await setup_test_db("payload_inj")

    server = TBServer(name="Server-PayloadInj", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-PayloadInj",
        username="admin_payloadinj@test.com",
        custom_metadata={"heatmap_config": ["temperatura"]}
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-inj"}, "name": "Device-Inj"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    # Inyección adversaria masiva en payload
    malicious_payload = {
        "tenant_id": str(tenant.id),
        "keys": ["bateria_secreta", "co2_confidencial", "password_hash"],
        "whitelist": ["injected_whitelist_var"],
        "telemetry_keys": ["injected_telem_var"],
        "variables": ["injected_variables_var"],
        "heatmap_config": {
            "keys": ["hacked_key_1", "hacked_key_2"],
            "variables": ["hacked_var_3"]
        },
        "send_email": False
    }

    mock_telemetry_called_keys = []

    async def mock_get_entity_telemetry(entity_id, keys, start_ts, end_ts, limit, token):
        mock_telemetry_called_keys.append(keys)
        return {keys: [{"ts": 1785542400000, "value": "22.5"}]}

    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_adv_payload_inj", "job_try": 1, "redis": mock_redis}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=["temperatura", "bateria_secreta"])), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", side_effect=mock_get_entity_telemetry), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=malicious_payload)
        assert result["status"] == "SUCCESS"

        # Solo debe haber consultado 'temperatura'
        assert mock_telemetry_called_keys == ["temperatura"]

        # Las secciones generadas solo deben incluir 'temperatura'
        assert len(captured_sections) == 1
        assert captured_sections[0]["metric_name"] == "temperatura"

        # Ninguna de las llaves inyectadas debe haber aparecido
        forbidden_injections = [
            "bateria_secreta", "co2_confidencial", "password_hash",
            "injected_whitelist_var", "injected_telem_var", "injected_variables_var",
            "hacked_key_1", "hacked_key_2", "hacked_var_3"
        ]
        for forb in forbidden_injections:
            assert forb not in mock_telemetry_called_keys
            assert forb != captured_sections[0]["metric_name"]


# ==============================================================================
# VECTOR 3: ATAQUE DE FALLBACK Y EXTRACCIÓN NO AUTORIZADA DE VARIABLES
# ==============================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("config_type, custom_meta", [
    ("missing_metadata", {}),
    ("empty_dict", {}),
    ("empty_heatmap_config", {"heatmap_config": {}}),
    ("empty_keys_list", {"heatmap_config": {"keys": []}}),
    ("empty_variables_list", {"heatmap_config": {"variables": []}}),
    ("empty_whitelist_list", {"heatmap_config": {"whitelist": []}}),
    ("null_keys", {"heatmap_config": {"keys": None}}),
    ("whitespace_keys", {"heatmap_config": {"keys": ["  ", "   "]}}),
    ("only_reserved_keys", {"heatmap_config": {"rules": [], "time_zone": "UTC", "aggregation": "AVG"}})
])
async def test_adversarial_fallback_attack_never_queries_or_guesses_keys(config_type, custom_meta):
    """
    Verifica que bajo NINGUNA circunstancia el worker recurra a tb_client.get_entity_timeseries_keys
    para adivinar variables o extraer telemetría cuando el tenant no tiene variables admitidas en custom_metadata.
    """
    await setup_test_db(f"fallback_{config_type}")

    server = TBServer(name=f"Server-FB-{config_type}", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name=f"Tenant-FB-{config_type}",
        username=f"admin_{config_type}@test.com",
        custom_metadata=custom_meta
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [
            {"id": {"id": "dev-priv-1"}, "name": "Dispositivo-Privado-1"},
            {"id": {"id": "dev-priv-2"}, "name": "Dispositivo-Privado-2"}
        ],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    # Simular que ThingsBoard tiene llaves privadas
    mock_get_keys = AsyncMock(return_value=["sensor_privado_alpha", "sensor_privado_beta"])
    mock_get_telem = AsyncMock(return_value={})
    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": f"job_adv_fb_{config_type}", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", mock_get_keys), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", mock_get_telem), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"

        # Cero llamadas a consulta de llaves o telemetría
        mock_get_keys.assert_not_called()
        mock_get_telem.assert_not_called()

        # Debe generar sección informativa "Sin Variables Admitidas"
        assert len(captured_sections) == 1
        assert captured_sections[0]["metric_name"] == "Sin Variables Admitidas"
        assert captured_sections[0]["device_name"] == "Dispositivo-Privado-1"
        assert all(all(val == "NA" for val in row) for row in captured_sections[0]["data"])


# ==============================================================================
# VECTOR 4: SUPLANTACIÓN Y MANIPULACIÓN DE TELEMETRÍA (TELEMETRY KEY SPOOFING)
# ==============================================================================

@pytest.mark.asyncio
async def test_adversarial_telemetry_key_spoofing_rejection():
    """
    Verifica que cuando ThingsBoard responde con datos asociados a llaves ajenas o no solicitadas,
    el worker NUNCA adopte dichos puntos en el mapa de calor:
    1. Si se solicitó 'temperatura' y ThingsBoard retorna 'bateria', los puntos son ignorados ('NA').
    2. Si retorna llaves legítimas con prefijo 'HM_temperatura', las adopta correctamente.
    3. Si retorna un array con puntos corruptos (None, enteros, strings no JSON), no se produce crash.
    """
    await setup_test_db("spoofing")

    server = TBServer(name="Server-Spoofing", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Spoofing",
        username="admin_spoofing@test.com",
        custom_metadata={"heatmap_config": ["temperatura", "humedad"]}
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-spoof"}, "name": "Device-Spoof"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    # Dispositivo reporta tener 'temperatura' y 'humedad'
    dev_keys = ["temperatura", "HM_humedad"]

    # Simular respuestas de telemetría:
    # - Para 'temperatura': Thingsboard responde con llave foránea 'voltaje_ajeno' y puntos corruptos
    # - Para 'humedad': Thingsboard responde con 'HM_humedad' con puntos válidos
    async def mock_get_entity_telemetry(entity_id, keys, start_ts, end_ts, limit, token):
        sample_ts = (start_ts + 3600000 * 36) if (start_ts and start_ts > 0) else 1785542400000
        if keys == "temperatura":
            return {
                "voltaje_ajeno": [
                    {"ts": sample_ts, "value": "120.0"}
                ],
                # Caso extremo: puntos con elementos no diccionarios
                "temperatura": [
                    None,
                    "string_malformado",
                    12345,
                    {"ts": sample_ts, "value": "bad_non_numeric"}
                ]
            }
        elif keys == "HM_humedad":
            return {
                "HM_humedad": [
                    {"ts": sample_ts, "value": "55.0"}
                ]
            }
        return {}

    captured_sections = []

    async def mock_pdf_gen(matrix_data, rules, output_pdf_path, title, subtitle, metadata):
        captured_sections.extend(matrix_data)
        return True

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_adv_spoofing", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=dev_keys)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", side_effect=mock_get_entity_telemetry), \
         patch("workers.tasks.generate_heatmap_report_pdf", side_effect=mock_pdf_gen):

        result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
        assert result["status"] == "SUCCESS"
        assert len(captured_sections) == 2

        sec_temp = next(s for s in captured_sections if s["metric_name"] == "temperatura")
        sec_hum = next(s for s in captured_sections if s["metric_name"] == "HM_humedad")

        # La data de temperatura no adoptó voltaje_ajeno ni crasheó con los puntos no-dict.
        # Todos los puntos numéricos fallaron validación, por lo que quedan en 'NA'.
        assert all(all(val == "NA" for val in row) for row in sec_temp["data"])

        # La data de HM_humedad adoptó legítimamente el valor 55.0 en al menos una celda
        hum_values = [val for row in sec_hum["data"] for val in row if val != "NA"]
        assert 55.0 in hum_values


# ==============================================================================
# VECTOR 5: INTEGRIDAD NO BLOQUEANTE DEL EVENT LOOP (ASYNCIO EVENT LOOP STALL)
# ==============================================================================

@pytest.mark.asyncio
async def test_adversarial_non_blocking_event_loop_integrity():
    """
    Verifica que la tarea de generación de mapas de calor no bloquee sincrónicamente el bucle
    de eventos de asyncio durante la ejecución de tareas de renderizado o procesamiento.
    Lanza una tarea latente en segundo plano (Heartbeat Pulse) que mide la latencia máxima del bucle.
    Si el worker bloqueara el hilo principal, la latencia excedería el umbral de tolerancia.
    """
    await setup_test_db("event_loop")

    server = TBServer(name="Server-Loop", base_url="https://tb.test.com")
    server.set_password("pass")
    server.set_tokens("jwt", "refresh")
    await server.insert()

    tenant = TBTenant(
        server_id=server,
        name="Tenant-Loop",
        username="admin_loop@test.com",
        custom_metadata={"heatmap_config": ["temperatura"]}
    )
    tenant.set_password("pass")
    tenant.set_tokens("jwt", "refresh")
    await tenant.insert()

    fake_devices = {
        "data": [{"id": {"id": "dev-loop"}, "name": "Device-Loop"}],
        "hasNext": False
    }
    fake_attrs = [{"key": "heatmap_active", "value": True}]

    mock_redis = build_mock_redis()
    ctx = {"job_id": "job_adv_loop", "job_try": 1, "redis": mock_redis}
    payload = {"tenant_id": str(tenant.id), "send_email": False}

    # Monitor de latencia del Event Loop
    max_drift = 0.0
    tick_interval = 0.01  # 10ms
    stop_monitor = False

    async def loop_lag_monitor():
        nonlocal max_drift
        while not stop_monitor:
            t0 = time.perf_counter()
            await asyncio.sleep(tick_interval)
            drift = time.perf_counter() - t0 - tick_interval
            if drift > max_drift:
                max_drift = drift

    monitor_task = asyncio.create_task(loop_lag_monitor())

    with patch("core.tb_client.ThingsBoardClient.get_tenant_devices", AsyncMock(return_value=fake_devices)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_attributes", AsyncMock(return_value=fake_attrs)), \
         patch("core.tb_client.ThingsBoardClient.get_entity_timeseries_keys", AsyncMock(return_value=["temperatura"])), \
         patch("core.tb_client.ThingsBoardClient.get_entity_telemetry", AsyncMock(return_value={"temperatura": []})), \
         patch("workers.tasks.generate_heatmap_report_pdf", AsyncMock(return_value=True)):

        try:
            result = await generate_monthly_heatmap_task(ctx=ctx, tenant_id=str(tenant.id), payload=payload)
            assert result["status"] == "SUCCESS"
        finally:
            stop_monitor = True
            await monitor_task

    # El event loop no debe presentar bloqueos sostenidos mayores a 200ms
    assert max_drift < 0.20, f"El Event Loop presentó un bloqueo excesivo: {max_drift * 1000:.2f}ms"
