import os
import shutil
import tempfile
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import respx
import httpx
from beanie import init_beanie, PydanticObjectId, Link
from mongomock_motor import AsyncMongoMockClient

from core.config import settings
from core.database import init_db, close_db, get_mongo_client
from core.io_limiter import (
    get_zip_semaphore,
    get_io_semaphore,
    async_create_zip_archive,
    async_rmtree,
    _sync_remove,
    async_remove_file,
    _sync_replace,
    async_replace_file,
)
from core.pagination import (
    PaginationMetadata,
    PaginatedResponse,
    build_pagination_metadata,
)
from core.models.tb_server import TBServer, SSHAuthMethod
from core.models.tb_tenant import TBTenant
from core.models.tb_node import TBNode
from core.models.tb_backup import TBBackup
from core.models.tb_email_config import TBEmailConfig
from core.models.tb_scheduled_task import TBScheduledTask
from core.models.audit_log import AuditLog
from core.models.user import User
from core.tb_client import ThingsBoardClient


async def init_mock_db(suffix: str = "fase2"):
    client = AsyncMongoMockClient()
    db = client[f"test_fase2_db_{suffix}_{os.getpid()}"]
    await init_beanie(
        database=db,
        document_models=[
            User,
            TBServer,
            TBTenant,
            TBNode,
            TBBackup,
            AuditLog,
            TBScheduledTask,
            TBEmailConfig,
        ],
    )
    return db, client


# ==============================================================================
# 1. CORE/DATABASE.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_database_init_db_and_lifecycle():
    mock_client = AsyncMongoMockClient()
    db_name = f"test_db_lifecycle_{os.getpid()}"

    # Simular índice legacy sin unique para probar reconciliación
    col = mock_client[db_name]["tb_email_configs"]
    await col.create_index("singleton_key_1", unique=False)

    await init_db(custom_client=mock_client, database_name=db_name)
    assert get_mongo_client() == mock_client

    # Llamar close_db
    await close_db()


@pytest.mark.asyncio
async def test_database_init_db_retry_on_index_conflict():
    mock_client = AsyncMongoMockClient()
    db_name = f"test_db_retry_{os.getpid()}"

    # Simular excepción de conflicto de índice en el primer intento
    with patch("core.database.init_beanie") as mock_beanie:
        mock_beanie.side_effect = [
            Exception("IndexKeySpecsConflict code 86: conflicting index"),
            None,
        ]
        await init_db(custom_client=mock_client, database_name=db_name)
        assert mock_beanie.call_count == 2


# ==============================================================================
# 2. CORE/IO_LIMITER.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_io_limiter_semaphores_and_zip():
    sem_zip = get_zip_semaphore()
    sem_io = get_io_semaphore()
    assert isinstance(sem_zip, asyncio.Semaphore)
    assert isinstance(sem_io, asyncio.Semaphore)

    tmp_dir = tempfile.mkdtemp(prefix="test_io_limiter_")
    try:
        sample_file = os.path.join(tmp_dir, "data.txt")
        with open(sample_file, "w", encoding="utf-8") as f:
            f.write("test content for compression")

        zip_target = os.path.join(tmp_dir, "archive")
        created_zip = await async_create_zip_archive(base_name=zip_target, root_dir=tmp_dir)
        assert os.path.exists(created_zip)
    finally:
        await async_rmtree(tmp_dir)
        assert not os.path.exists(tmp_dir)


@pytest.mark.asyncio
async def test_io_limiter_file_operations():
    tmp_dir = tempfile.mkdtemp(prefix="test_file_ops_")
    try:
        file_a = os.path.join(tmp_dir, "file_a.txt")
        file_b = os.path.join(tmp_dir, "file_b.txt")
        with open(file_a, "w", encoding="utf-8") as f:
            f.write("content a")

        # async_replace_file
        await async_replace_file(file_a, file_b)
        assert not os.path.exists(file_a)
        assert os.path.exists(file_b)

        # async_replace_file sobrescribiendo archivo existente
        with open(file_a, "w", encoding="utf-8") as f:
            f.write("content a new")
        await async_replace_file(file_a, file_b)
        assert os.path.exists(file_b)

        # async_remove_file
        await async_remove_file(file_b)
        assert not os.path.exists(file_b)

        # _sync_remove ignorando y no ignorando errores
        _sync_remove("archivo_que_no_existe_999.txt", ignore_errors=True)
        with patch("os.path.exists", return_value=True):
            with patch("os.remove", side_effect=OSError("Disk error")):
                with pytest.raises(OSError):
                    _sync_remove("mocked.txt", ignore_errors=False)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ==============================================================================
# 3. CORE/PAGINATION.PY
# ==============================================================================

def test_pagination_metadata_and_builder():
    meta1 = build_pagination_metadata(total=105, page=1, page_size=20)
    assert meta1.total == 105
    assert meta1.page == 1
    assert meta1.page_size == 20
    assert meta1.total_pages == 6
    assert meta1.has_next is True
    assert meta1.has_prev is False

    meta_last = build_pagination_metadata(total=105, page=6, page_size=20)
    assert meta_last.has_next is False
    assert meta_last.has_prev is True

    meta_empty = build_pagination_metadata(total=0, page=1, page_size=10)
    assert meta_empty.total_pages == 1
    assert meta_empty.has_next is False
    assert meta_empty.has_prev is False

    paginated_resp = PaginatedResponse[str](
        items=["item1", "item2"],
        pagination=meta1
    )
    assert len(paginated_resp.items) == 2
    assert paginated_resp.pagination.total == 105


# ==============================================================================
# 4. CORE/MODELS/TB_SERVER.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_tb_server_model_branches():
    await init_mock_db("tb_server")
    # ensure_tz_aware
    naive_dt = datetime(2026, 1, 1, 12, 0, 0)
    server = TBServer(
        name="Server Main",
        base_url="https://tb.enterprise.local:8080/path",
        created_at=naive_dt,
        updated_at=naive_dt,
    )
    assert server.created_at.tzinfo == timezone.utc
    assert server.ssh_host == "tb.enterprise.local"

    # ssh_host sin hostname válido
    server_invalid_url = TBServer(name="S2", base_url="tb_host_only")
    assert server_invalid_url.ssh_host == "tb_host_only"

    # Passwords y tokens
    assert server.get_password() is None
    server.set_password("sysadmin_secret")
    assert server.get_password() == "sysadmin_secret"
    server.set_password(None)
    assert server.get_password() is None

    assert server.get_token() is None
    assert server.get_refresh_token() is None
    server.set_tokens("jwt_token_123", "refresh_token_456")
    assert server.get_token() == "jwt_token_123"
    assert server.get_refresh_token() == "refresh_token_456"
    server.set_tokens(None, None)
    assert server.get_token() == "jwt_token_123"  # token=None no sobreescribe si se pasa None
    server.set_tokens("", "")
    assert server.get_token() is None
    assert server.get_refresh_token() is None

    # SSH credentials: Password mode
    server.ssh_auth_method = SSHAuthMethod.PASSWORD
    server.set_ssh_credentials(ssh_password="ssh_pass_secure")
    assert server.get_ssh_password() == "ssh_pass_secure"
    assert server.get_ssh_pem_file() is None

    # SSH credentials: PEM_KEY mode
    server.ssh_auth_method = SSHAuthMethod.PEM_KEY
    server.set_ssh_credentials(ssh_pem_file="-----BEGIN RSA PRIVATE KEY-----...", ssh_passphrase="passphrase_123")
    assert server.get_ssh_pem_file() == "-----BEGIN RSA PRIVATE KEY-----..."
    assert server.get_ssh_passphrase() == "passphrase_123"
    assert server.get_ssh_password() is None

    # SSH credentials: NONE/OTHER mode
    server.ssh_auth_method = "custom_mode"
    server.set_ssh_credentials(ssh_password="p", ssh_pem_file="k", ssh_passphrase="ph")
    assert server.get_ssh_password() == "p"
    assert server.get_ssh_pem_file() == "k"
    assert server.get_ssh_passphrase() == "ph"

    # Reset credentials to None
    server.set_ssh_password(None)
    server.set_ssh_pem_file(None)
    server.set_ssh_passphrase(None)
    assert server.get_ssh_password() is None
    assert server.get_ssh_pem_file() is None
    assert server.get_ssh_passphrase() is None


# ==============================================================================
# 5. CORE/MODELS/TB_TENANT.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_tb_tenant_model_branches():
    await init_mock_db("tb_tenant")
    server = TBServer(name="Parent Server", base_url="http://tb.srv")
    await server.insert()

    tenant = TBTenant(name="Tenant A", server_id=server, username="admin_a")
    await tenant.insert()

    # Passwords y tokens
    assert tenant.get_password() is None
    tenant.set_password("tenant_pwd_123")
    assert tenant.get_password() == "tenant_pwd_123"
    tenant.set_password(None)
    assert tenant.get_password() is None

    assert tenant.get_token() is None
    assert tenant.get_refresh_token() is None
    tenant.set_tokens("t_token", "t_refresh")
    assert tenant.get_token() == "t_token"
    assert tenant.get_refresh_token() == "t_refresh"
    tenant.set_tokens("", "")
    assert tenant.get_token() is None
    assert tenant.get_refresh_token() is None

    # get_server y get_server_id_str
    resolved_srv = await tenant.get_server()
    assert resolved_srv.id == server.id
    assert tenant.get_server_id_str() == str(server.id)

    # Con server_id como Link/PydanticObjectId
    tenant.server_id = server.id
    resolved_srv2 = await tenant.get_server()
    assert resolved_srv2.id == server.id
    assert tenant.get_server_id_str() == str(server.id)

    # Con server_id inexistente
    tenant.server_id = PydanticObjectId()
    assert (await tenant.get_server()) is None


# ==============================================================================
# 6. CORE/MODELS/TB_NODE.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_tb_node_model_branches():
    await init_mock_db("tb_node")
    server = TBServer(name="Master Server", base_url="http://master.srv")
    await server.insert()

    node = TBNode(
        server_id=server,
        node_role="cassandra",
        ssh_host="192.168.1.50",
        ssh_username="ubuntu",
        ssh_auth_method=SSHAuthMethod.PASSWORD
    )
    await node.insert()

    # Password SSH
    node.set_ssh_credentials(ssh_password="ubuntu_secret_pass")
    assert node.get_ssh_password() == "ubuntu_secret_pass"
    assert node.get_ssh_pem_file() is None

    # PEM Key SSH
    node.ssh_auth_method = SSHAuthMethod.PEM_KEY
    node.set_ssh_credentials(ssh_pem_file="pem_content_rsa", ssh_passphrase="node_passphrase")
    assert node.get_ssh_pem_file() == "pem_content_rsa"
    assert node.get_ssh_passphrase() == "node_passphrase"
    assert node.get_ssh_password() is None

    # Other SSH
    node.ssh_auth_method = "custom_mode"
    node.set_ssh_credentials(ssh_password="p_node", ssh_pem_file="k_node", ssh_passphrase="ph_node")
    assert node.get_ssh_password() == "p_node"
    assert node.get_ssh_pem_file() == "k_node"
    assert node.get_ssh_passphrase() == "ph_node"

    node.set_ssh_password(None)
    node.set_ssh_pem_file(None)
    node.set_ssh_passphrase(None)
    assert node.get_ssh_password() is None
    assert node.get_ssh_pem_file() is None
    assert node.get_ssh_passphrase() is None

    # get_server y get_server_id_str
    resolved_srv = await node.get_server()
    assert resolved_srv.id == server.id
    assert node.get_server_id_str() == str(server.id)

    node.server_id = server.id
    resolved_srv2 = await node.get_server()
    assert resolved_srv2.id == server.id
    assert node.get_server_id_str() == str(server.id)

    node.server_id = PydanticObjectId()
    assert (await node.get_server()) is None


# ==============================================================================
# 7. CORE/MODELS/TB_BACKUP.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_tb_backup_model_branches():
    await init_mock_db("tb_backup")
    server = TBServer(name="Backup SRV", base_url="http://srv")
    await server.insert()
    tenant = TBTenant(name="Backup Tenant", server_id=server, username="admin")
    await tenant.insert()

    now = datetime.now(timezone.utc)
    backup = TBBackup(
        tenant_id=tenant,
        task_id="task_001",
        requested_by="admin_user",
        start_date=now,
        end_date=now,
        file_name="reporte_mensual.pdf",
        file_path="/tmp/reporte_mensual.pdf",
        file_size_bytes=1024,
    )
    await backup.insert()

    # get_backup_type
    assert backup.get_backup_type() == "heatmap"

    backup.file_name = "data.xlsx"
    assert backup.get_backup_type() == "excel_report"

    backup.file_name = "raw_archive.zip"
    assert backup.get_backup_type() == "telemetry"

    backup.backup_type = "excel_report"
    assert backup.get_backup_type() == "excel_report"

    # get_tenant y get_tenant_id_str
    resolved_tnt = await backup.get_tenant()
    assert resolved_tnt.id == tenant.id
    assert backup.get_tenant_id_str() == str(tenant.id)

    backup.tenant_id = tenant.id
    resolved_tnt2 = await backup.get_tenant()
    assert resolved_tnt2.id == tenant.id
    assert backup.get_tenant_id_str() == str(tenant.id)

    backup.tenant_id = PydanticObjectId()
    assert (await backup.get_tenant()) is None


# ==============================================================================
# 8. CORE/MODELS/TB_EMAIL_CONFIG.PY
# ==============================================================================

@pytest.mark.asyncio
async def test_tb_email_config_model_branches():
    await init_mock_db("tb_email_config")
    assert (await TBEmailConfig.exists_config()) is False
    assert (await TBEmailConfig.get_singleton()) is None

    cfg = TBEmailConfig(
        host="smtp.office365.com",
        port=587,
        username="smtp_user@tkmecloud.com",
        sender_email="noreply@tkmecloud.com"
    )
    # set_password y get_password async
    await cfg.set_password("super_smtp_secret")
    assert (await cfg.get_password()) == "super_smtp_secret"
    await cfg.set_password(None)
    assert (await cfg.get_password()) is None

    # set_password_sync y get_password_sync
    cfg.set_password_sync("sync_smtp_secret")
    assert cfg.get_password_sync() == "sync_smtp_secret"
    cfg.set_password_sync(None)
    assert cfg.get_password_sync() is None

    await cfg.insert()
    assert (await TBEmailConfig.exists_config()) is True
    singleton = await TBEmailConfig.get_singleton()
    assert singleton is not None
    assert singleton.host == "smtp.office365.com"


# ==============================================================================
# 9. CORE/MODELS/TB_SCHEDULED_TASK.PY
# ==============================================================================

def test_tb_scheduled_task_compute_next_run():
    task = TBScheduledTask(
        name="Backup Diario",
        task_name="execute_incremental_tenant_backup",
        cron_expression="0 3 * * *",  # A las 3:00 AM
        next_run_time=datetime.now(timezone.utc),
    )
    # Con base_time naive
    base_naive = datetime(2026, 3, 10, 2, 0, 0)
    next_dt = task.compute_next_run(base_time=base_naive, tz_str="America/Mexico_City")
    assert next_dt.tzinfo == timezone.utc

    # Con base_time aware
    base_aware = datetime(2026, 3, 10, 4, 0, 0, tzinfo=timezone.utc)
    next_dt2 = task.compute_next_run(base_time=base_aware)
    assert next_dt2.tzinfo == timezone.utc

    # Con base_time=None
    next_dt3 = task.compute_next_run()
    assert next_dt3.tzinfo == timezone.utc


# ==============================================================================
# 10. CORE/TB_CLIENT.PY (100% COBERTURA CON RESXP)
# ==============================================================================

@pytest.mark.asyncio
async def test_tb_client_initialization_and_validation():
    with pytest.raises(ValueError, match="base_url es obligatorio"):
        ThingsBoardClient(base_url="")

    tb = ThingsBoardClient(
        base_url="http://tb.example.com/",
        token="tok1",
        refresh_token="ref1",
        username="user1",
        password="pwd",
        timeout=15.0
    )
    assert tb.base_url == "http://tb.example.com"
    assert tb._resolve_token() == "tok1"
    assert tb._resolve_token("override_tok") == "override_tok"

    tb_no_tok = ThingsBoardClient(base_url="http://tb.example.com")
    with pytest.raises(ValueError, match="Se requiere un token JWT"):
        tb_no_tok._resolve_token()

    with pytest.raises(ValueError, match="Se requiere usuario y contraseña"):
        await tb_no_tok.login()

    with pytest.raises(ValueError, match="Se requiere un refreshToken"):
        await tb_no_tok.refresh_jwt_token()


@pytest.mark.asyncio
@respx.mock
async def test_tb_client_login_and_refresh():
    tb = ThingsBoardClient(
        base_url="http://tb.example.com",
        username="tb_user",
        password="tb_password"
    )

    # 1. Login exitoso
    respx.post("http://tb.example.com/api/auth/login").respond(
        status_code=200,
        json={"token": "tok_login", "refreshToken": "ref_login"}
    )
    data = await tb.login()
    assert data["token"] == "tok_login"
    assert tb.token == "tok_login"
    assert tb.refresh_token == "ref_login"

    # 2. Login fallido (401)
    respx.post("http://tb.example.com/api/auth/login").respond(status_code=401)
    assert (await tb.login()) is None

    # 3. Refresh token exitoso
    respx.post("http://tb.example.com/api/auth/token").respond(
        status_code=200,
        json={"token": "tok_new", "refreshToken": "ref_new"}
    )
    ref_data = await tb.refresh_jwt_token("ref_login")
    assert ref_data["token"] == "tok_new"
    assert tb.token == "tok_new"
    assert tb.refresh_token == "ref_new"

    # 4. Verify token
    respx.get("http://tb.example.com/api/auth/user").respond(status_code=200)
    assert (await tb.verify_token("tok_new")) is True

    respx.get("http://tb.example.com/api/auth/user").respond(status_code=401)
    assert (await tb.verify_token("tok_new")) is False

    assert (await tb.verify_token(None)) is False


@pytest.mark.asyncio
@respx.mock
async def test_tb_client_test_connection_branches():
    tb = ThingsBoardClient(base_url="http://tb.example.com", token="tok123")

    # 1. Conexión exitosa
    respx.get("http://tb.example.com/api/noauth/activate").respond(status_code=200)
    respx.get("http://tb.example.com/api/auth/user").respond(status_code=200)
    res1 = await tb.test_connection()
    assert res1["success"] is True
    assert res1["reachable"] is True
    assert res1["authenticated"] is True

    # 2. ConnectError (DNS error)
    respx.get("http://tb.example.com/api/noauth/activate").mock(
        side_effect=httpx.ConnectError("getaddrinfo failed")
    )
    res2 = await tb.test_connection()
    assert res2["reachable"] is False
    assert "DNS" in res2["error"]

    # 3. TimeoutException
    respx.get("http://tb.example.com/api/noauth/activate").mock(
        side_effect=httpx.TimeoutException("Read timed out")
    )
    res3 = await tb.test_connection()
    assert res3["reachable"] is False
    assert "Tiempo de espera agotado" in res3["error"]

    # 4. Excepción genérica
    respx.get("http://tb.example.com/api/noauth/activate").mock(
        side_effect=Exception("Error genérico socket")
    )
    res4 = await tb.test_connection()
    assert res4["reachable"] is False
    assert "socket" in res4["error"]


@pytest.mark.asyncio
@respx.mock
async def test_tb_client_entity_methods():
    tb = ThingsBoardClient(base_url="http://tb.example.com", token="valid_tok")

    # get_tenant_devices
    respx.get("http://tb.example.com/api/tenant/devices").respond(
        status_code=200,
        json={"data": [{"id": {"id": "dev1"}, "name": "Device 1"}]}
    )
    devices = await tb.get_tenant_devices()
    assert len(devices["data"]) == 1

    # get_device_by_id (con y sin client)
    respx.get("http://tb.example.com/api/device/dev1").respond(
        status_code=200,
        json={"name": "Device 1"}
    )
    dev_obj = await tb.get_device_by_id("dev1")
    assert dev_obj["name"] == "Device 1"

    async with httpx.AsyncClient() as ac:
        dev_obj2 = await tb.get_device_by_id("dev1", client=ac)
        assert dev_obj2["name"] == "Device 1"

    # get_device_by_id 404
    respx.get("http://tb.example.com/api/device/dev_nonexistent").respond(status_code=404)
    assert (await tb.get_device_by_id("dev_nonexistent")) is None

    # get_device_by_name (con y sin client)
    respx.get("http://tb.example.com/api/tenant/devices").respond(
        status_code=200,
        json={"name": "Device Name Exact"}
    )
    dev_by_name = await tb.get_device_by_name("Device Name Exact")
    assert dev_by_name["name"] == "Device Name Exact"

    async with httpx.AsyncClient() as ac:
        dev_by_name2 = await tb.get_device_by_name("Device Name Exact", client=ac)
        assert dev_by_name2["name"] == "Device Name Exact"

    # create_device (con y sin client)
    respx.post("http://tb.example.com/api/device").respond(
        status_code=200,
        json={"id": {"id": "new_dev"}, "name": "Created Device"}
    )
    created = await tb.create_device({"name": "Created Device"})
    assert created["id"]["id"] == "new_dev"

    async with httpx.AsyncClient() as ac:
        created2 = await tb.create_device({"name": "Created Device"}, client=ac)
        assert created2["id"]["id"] == "new_dev"

    # get_entity_timeseries_keys
    respx.get("http://tb.example.com/api/plugins/telemetry/DEVICE/dev1/keys/timeseries").respond(
        status_code=200,
        json=["temperature", "humidity"]
    )
    keys = await tb.get_entity_timeseries_keys("dev1")
    assert "temperature" in keys

    # get_entity_telemetry
    respx.get("http://tb.example.com/api/plugins/telemetry/DEVICE/dev1/values/timeseries").respond(
        status_code=200,
        json={"temperature": [{"ts": 1700000000000, "value": "25.5"}]}
    )
    telem = await tb.get_entity_telemetry("dev1", keys="temperature", start_ts=0, end_ts=1700000000000)
    assert "temperature" in telem

    # get_entity_attributes (con scope y sin scope, con y sin client)
    respx.get("http://tb.example.com/api/plugins/telemetry/DEVICE/dev1/values/attributes/SERVER_SCOPE").respond(
        status_code=200,
        json=[{"key": "active", "value": True}]
    )
    attrs = await tb.get_entity_attributes("dev1", scope="SERVER_SCOPE")
    assert len(attrs) == 1
    assert attrs[0]["key"] == "active"

    async with httpx.AsyncClient() as ac:
        attrs2 = await tb.get_entity_attributes("dev1", scope="SERVER_SCOPE", client=ac)
        assert len(attrs2) == 1

    # find_entities_by_query
    respx.post("http://tb.example.com/api/entitiesQuery/find").respond(
        status_code=200,
        json={"data": [{"id": "entity_1"}]}
    )
    eq_res = await tb.find_entities_by_query({"entityFilter": {}})
    assert len(eq_res["data"]) == 1

    # get_tenant_assets
    respx.get("http://tb.example.com/api/tenant/assets").respond(
        status_code=200,
        json={"data": [{"id": {"id": "asset1"}, "name": "Asset 1"}]}
    )
    assets = await tb.get_tenant_assets()
    assert len(assets["data"]) == 1

    # get_entity_relations (from, to, empty, 401)
    assert (await tb.get_entity_relations()) == []

    respx.get("http://tb.example.com/api/relations/info").respond(
        status_code=200,
        json=[{"from": {"id": "asset1"}, "to": {"id": "dev1"}}]
    )
    rels = await tb.get_entity_relations(from_id="asset1", from_type="ASSET")
    assert len(rels) == 1

    async with httpx.AsyncClient() as ac:
        rels2 = await tb.get_entity_relations(from_id="asset1", from_type="ASSET", client=ac)
        assert len(rels2) == 1

    # 401 en get_entity_relations debe elevar HTTPStatusError
    respx.get("http://tb.example.com/api/relations/info").respond(status_code=401)
    with pytest.raises(httpx.HTTPStatusError):
        await tb.get_entity_relations(from_id="asset1", from_type="ASSET")

    # get_entity_attributes sin scope y con client
    respx.get("http://tb.example.com/api/plugins/telemetry/DEVICE/dev1/values/attributes").respond(
        status_code=200,
        json=[{"key": "shared_attr", "value": "xyz"}]
    )
    attrs_no_scope = await tb.get_entity_attributes("dev1", scope=None)
    assert len(attrs_no_scope) == 1

    # get_entity_relations con to_id
    respx.get("http://tb.example.com/api/relations/info").respond(
        status_code=200,
        json=[{"from": {"id": "asset2"}, "to": {"id": "dev1"}}]
    )
    rels_to = await tb.get_entity_relations(to_id="dev1", to_type="DEVICE")
    assert len(rels_to) == 1


@pytest.mark.asyncio
async def test_database_init_db_default_client():
    mock_motor = MagicMock()
    mock_motor.__getitem__.return_value = AsyncMongoMockClient()["test_default_db"]
    with patch("core.database._mongo_client", None):
        with patch("core.database.AsyncIOMotorClient", return_value=mock_motor):
            await init_db()
            assert get_mongo_client() == mock_motor


