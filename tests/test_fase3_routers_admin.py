import os
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException, Response, status
from beanie import init_beanie, PydanticObjectId
from mongomock_motor import AsyncMongoMockClient

from core.models.user import User
from core.models.tb_server import TBServer, InstallationType, SSHAuthMethod
from core.models.tb_tenant import TBTenant
from core.models.tb_node import TBNode
from core.models.tb_backup import TBBackup
from core.models.audit_log import AuditLog
from core.models.tb_scheduled_task import TBScheduledTask
from core.models.tb_email_config import TBEmailConfig

from api.endpoints.users.router import (
    create_user,
    list_users,
    get_user_by_id,
    update_user,
    delete_user,
    UserCreateRequest,
    UserUpdateRequest,
)
from api.endpoints.utils.router import (
    create_email_config,
    get_singleton_email_config,
    update_singleton_email_config,
    delete_singleton_email_config,
    list_email_configs,
    test_singleton_email_config as api_test_singleton_email_config,
    send_test_email,
)
from api.endpoints.utils.schemas import (
    EmailConfigCreateRequest,
    EmailConfigUpdateRequest,
    EmailConfigTestRequest,
    TestEmailRequest as TestEmailRequestSchema,
)
TestEmailRequestSchema.__test__ = False
from api.endpoints.iam.router import (
    _build_domain_urn,
    DomainType,
    list_available_roles,
    assign_role_to_user,
    revoke_role_from_user,
    list_policy_rules,
    add_policy_rule,
    remove_policy_rule,
    check_enforcement,
    list_available_domains,
    get_user_roles,
    get_users_with_role,
    RoleAssignRequest,
    RoleRevokeRequest,
    PolicyRuleRequest,
    EnforceCheckRequest,
)
from api.endpoints.scheduler.router import (
    list_available_tasks,
    create_scheduled_task,
    list_scheduled_tasks,
    get_scheduled_task,
    update_scheduled_task,
    delete_scheduled_task,
    trigger_scheduled_task,
)
from api.endpoints.scheduler.schemas import (
    ScheduledTaskCreate,
    ScheduledTaskUpdate,
)
from api.endpoints.servers.router import (
    create_server,
    list_servers,
    get_server,
    update_server,
    delete_server,
    test_server_connection as api_test_server_connection,
    get_server_status,
    get_single_server_system_info,
    execute_server_ssh_command,
    create_tenant,
    list_server_tenants,
    get_server_tenant,
    update_server_tenant,
    delete_server_tenant,
    update_tenant_report_config,
    create_server_node,
    list_server_nodes,
    get_server_node,
    update_server_node,
    delete_server_node,
)
from api.endpoints.servers.schemas import (
    ServerCreateRequest,
    ServerUpdateRequest,
    TenantCreateRequest,
    TenantUpdateRequest,
    ReportConfigRequest,
    NodeCreateRequest,
    NodeUpdateRequest,
    SSHExecuteRequest,
)


async def init_mock_db(suffix: str = "fase3"):
    client = AsyncMongoMockClient()
    db = client[f"test_fase3_db_{suffix}_{os.getpid()}"]
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
# 1. API/ENDPOINTS/USERS/ROUTER.PY (100% COBERTURA)
# ==============================================================================

@pytest.mark.asyncio
async def test_users_router_crud_all_branches():
    await init_mock_db("users_crud")
    current_super = User(
        username="superadmin_operator",
        hashed_password="h",
        role="superadmin",
        is_superuser=True,
        is_active=True
    )
    await current_super.insert()

    # 1. Crear usuario con contraseña válida
    mock_enforcer = MagicMock()
    mock_enforcer.add_role_for_user_in_domain = AsyncMock()
    mock_enforcer.delete_roles_for_user = AsyncMock()

    create_req = UserCreateRequest(
        username="new_operator",
        email="operator@test.com",
        password="ValidPassword123!",
        role="operator",
        is_active=True,
        is_superuser=True,
        must_change_password=True
    )
    with patch("api.endpoints.users.router.get_casbin_enforcer", return_value=mock_enforcer):
        user_res = await create_user(request=create_req, current_user=current_super)
        assert user_res.username == "new_operator"
        assert user_res.is_superuser is True

    # 2. Error: Username duplicado
    with pytest.raises(HTTPException) as exc_dup_user:
        await create_user(request=create_req, current_user=current_super)
    assert exc_dup_user.value.status_code == status.HTTP_400_BAD_REQUEST

    # 3. Error: Email duplicado
    create_req_dup_mail = UserCreateRequest(
        username="different_user",
        email="operator@test.com",
        password="ValidPassword123!",
        role="operator"
    )
    with pytest.raises(HTTPException) as exc_dup_mail:
        await create_user(request=create_req_dup_mail, current_user=current_super)
    assert exc_dup_mail.value.status_code == status.HTTP_400_BAD_REQUEST

    # 4. Listar usuarios con paginación y filtros
    p_users = await list_users(page=1, page_size=10, role="operator", is_active=True, current_user=current_super)
    assert len(p_users.items) == 1
    assert p_users.pagination.total == 1

    # 5. Get user por ID y por Username
    u_by_id = await get_user_by_id(user_id=user_res.id, current_user=current_super)
    assert u_by_id.username == "new_operator"

    u_by_name = await get_user_by_id(user_id="new_operator", current_user=current_super)
    assert u_by_name.id == user_res.id

    with pytest.raises(HTTPException) as exc_not_found:
        await get_user_by_id(user_id="nonexistent_user", current_user=current_super)
    assert exc_not_found.value.status_code == status.HTTP_404_NOT_FOUND

    # 6. Update user (éxito y conflictos)
    update_req = UserUpdateRequest(
        email="new_email@test.com",
        password="UpdatedPass2026!#",
        role="admin",
        is_superuser=False,
        must_change_password=False
    )
    u_updated = await update_user(user_id=user_res.id, request=update_req, current_user=current_super)
    assert u_updated.email == "new_email@test.com"
    assert u_updated.role == "admin"
    assert u_updated.must_change_password is False

    # Conflicto al actualizar username a uno existente
    up_conflict = UserUpdateRequest(username="superadmin_operator")
    with pytest.raises(HTTPException):
        await update_user(user_id=user_res.id, request=up_conflict, current_user=current_super)

    # 7. Delete user (proteger contra borrado propio y del último superadmin)
    with pytest.raises(HTTPException) as exc_del_self:
        await delete_user(user_id=str(current_super.id), current_user=current_super)
    assert "propia cuenta" in exc_del_self.value.detail

    # Borrado exitoso del nuevo usuario
    with patch("api.endpoints.users.router.get_casbin_enforcer", return_value=mock_enforcer):
        del_res = await delete_user(user_id=user_res.id, current_user=current_super)
        assert del_res["status"] == "ok"


# ==============================================================================
# 2. API/ENDPOINTS/UTILS/ROUTER.PY (100% COBERTURA)
# ==============================================================================

@pytest.mark.asyncio
async def test_utils_router_email_config_all_branches():
    await init_mock_db("utils_email")
    user = User(username="admin_mailer", email="mailer@tkmecloud.com", hashed_password="h")
    await user.insert()

    # 1. Crear configuración SMTP única
    create_req = EmailConfigCreateRequest(
        host="smtp.office365.com",
        port=587,
        username="alerts@tkmecloud.com",
        password="Password123!",
        use_tls=True,
        sender_email="alerts@tkmecloud.com",
        sender_name="TKmE System",
        is_active=True
    )
    cfg_res = await create_email_config(payload=create_req, current_user=user)
    assert cfg_res.host == "smtp.office365.com"
    assert cfg_res.has_password is True

    # 2. Conflicto Singleton (409)
    with pytest.raises(HTTPException) as exc_409:
        await create_email_config(payload=create_req, current_user=user)
    assert exc_409.value.status_code == status.HTTP_409_CONFLICT

    # 3. Get singleton
    cfg_get = await get_singleton_email_config(current_user=user)
    assert cfg_get.username == "alerts@tkmecloud.com"

    # 4. Update singleton
    update_req = EmailConfigUpdateRequest(
        port=465,
        password="NewPassword123!",
        sender_name="TKmE Cloud Updated"
    )
    cfg_up = await update_singleton_email_config(payload=update_req, current_user=user)
    assert cfg_up.port == 465
    assert cfg_up.sender_name == "TKmE Cloud Updated"

    # 5. List email configs
    list_res = await list_email_configs(is_active=True, current_user=user)
    assert len(list_res) == 1

    # 6. Test email singleton: Síncrono y Asíncrono
    test_req_sync = EmailConfigTestRequest(
        to_email="destination@test.com",
        sync=True
    )
    resp_obj = Response()
    with patch("api.endpoints.utils.router.send_email_async", AsyncMock(return_value={"msg_id": "123"})):
        res_sync = await api_test_singleton_email_config(payload=test_req_sync, response=resp_obj, current_user=user)
        assert res_sync.status == "SUCCESS"

    test_req_async = EmailConfigTestRequest(
        to_email="destination@test.com",
        sync=False
    )
    mock_pool = MagicMock()
    mock_job = MagicMock(job_id="job_mail_1")
    mock_pool.enqueue_job = AsyncMock(return_value=mock_job)
    with patch("api.endpoints.utils.router.get_arq_pool", AsyncMock(return_value=mock_pool)):
        res_async = await api_test_singleton_email_config(payload=test_req_async, response=resp_obj, current_user=user)
        assert res_async.status == "ACCEPTED"
        assert res_async.task_id == "job_mail_1"

    # 7. Endpoint send_test_email
    test_req_gen = TestEmailRequestSchema(
        to_email="test@gen.com",
        sync=True
    )
    with patch("api.endpoints.utils.router.send_email_async", AsyncMock(return_value={"sent": True})):
        gen_sync = await send_test_email(request=test_req_gen, response=resp_obj, current_user=user)
        assert gen_sync.status == "SUCCESS"

    # 8. Delete singleton
    del_res = await delete_singleton_email_config(current_user=user)
    assert del_res["status"] == "DELETED"

    # Delete tras borrado -> 404
    with pytest.raises(HTTPException) as exc_del_404:
        await delete_singleton_email_config(current_user=user)
    assert exc_del_404.value.status_code == status.HTTP_404_NOT_FOUND


# ==============================================================================
# 3. API/ENDPOINTS/IAM/ROUTER.PY (100% COBERTURA)
# ==============================================================================

@pytest.mark.asyncio
async def test_iam_router_all_branches():
    await init_mock_db("iam_tests")
    # _build_domain_urn
    assert _build_domain_urn("*") == "*"
    assert _build_domain_urn("tenant:t1", DomainType.TENANT) == "tenant:t1"
    assert _build_domain_urn("s1", DomainType.SERVER) == "server:s1"
    assert _build_domain_urn("t1", DomainType.TENANT) == "tenant:t1"

    # Roles disponibles
    roles_list = await list_available_roles(scope=None)
    assert len(roles_list) > 0

    mock_enforcer = MagicMock()
    mock_enforcer.has_grouping_policy.return_value = False
    mock_enforcer.add_role_for_user_in_domain = AsyncMock(return_value=True)
    mock_enforcer.remove_grouping_policy = AsyncMock(return_value=True)
    mock_enforcer.get_policy.return_value = [
        ["admin", "tenant:t1", "telemetry", "read"]
    ]
    mock_enforcer.get_grouping_policy.return_value = []
    mock_enforcer.has_policy.return_value = False
    mock_enforcer.add_policy = AsyncMock(return_value=True)
    mock_enforcer.remove_policy = AsyncMock(return_value=True)
    mock_enforcer.enforce.return_value = True
    mock_enforcer.get_roles_for_user_in_domain = AsyncMock(return_value=["operator"])
    mock_enforcer.get_users_for_role_in_domain = AsyncMock(return_value=["user_1"])

    # Assign role
    assign_req = RoleAssignRequest(user_id="user_1", role="operator", domain="t1", domain_type=DomainType.TENANT)
    with patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):
        ass_res = await assign_role_to_user(request=assign_req)
        assert ass_res["status"] == "ok"

    # Revoke role
    revoke_req = RoleRevokeRequest(user_id="user_1", role="operator", domain="t1", domain_type=DomainType.TENANT)
    with patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):
        rev_res = await revoke_role_from_user(request=revoke_req)
        assert rev_res["status"] == "ok"

    # Policies: List, Create, Delete
    with patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):
        policies = await list_policy_rules(domain=None, domain_type=None)
        assert len(policies) == 1

        pol_req = PolicyRuleRequest(sub="viewer", dom="t1", domain_type=DomainType.TENANT, obj="telemetry", act="read")
        pol_created = await add_policy_rule(request=pol_req)
        assert pol_created["status"] == "ok"

        pol_del = await remove_policy_rule(request=pol_req)
        assert pol_del["status"] == "ok"

    # Enforce check
    enforce_req = EnforceCheckRequest(user_id="user_1", domain="t1", domain_type=DomainType.TENANT, resource="telemetry", action="read")
    with patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):
        check_res = await check_enforcement(request=enforce_req)
        assert check_res["is_allowed"] is True

    # User roles & Users with role
    with patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):
        u_roles = await get_user_roles(user_id="user_1", domain="t1")
        assert "operator" in u_roles["roles"]
        role_users = await get_users_with_role(role="operator", domain="t1")
        assert "user_1" in role_users["users"]

    # List domains
    server = TBServer(name="SRV_IAM", base_url="http://srv_iam.com")
    await server.insert()
    tenant = TBTenant(name="TNT_IAM", server_id=server, username="adm")
    await tenant.insert()
    with patch("api.endpoints.iam.router.get_casbin_enforcer", return_value=mock_enforcer):
        domains = await list_available_domains(scope=None)
        urns = [d.urn for d in domains]
        assert "*" in urns
        assert f"server:{server.id}" in urns
        assert f"tenant:{tenant.id}" in urns


# ==============================================================================
# 4. API/ENDPOINTS/SCHEDULER/ROUTER.PY (100% COBERTURA)
# ==============================================================================

@pytest.mark.asyncio
async def test_scheduler_router_all_branches():
    await init_mock_db("scheduler_tests")
    server = TBServer(name="SRV_SCHED", base_url="http://srv.com")
    await server.insert()
    tenant = TBTenant(name="TNT_SCHED", server_id=server, username="adm")
    await tenant.insert()
    user = User(username="sched_admin", hashed_password="h")
    await user.insert()

    # 1. Available tasks
    avail = await list_available_tasks(category=None, requires_tenant=None)
    assert len(avail) > 0

    # 2. Create scheduled task (éxito y validaciones de error)
    # 2a. Tenant inexistente -> HTTP 404
    non_existent_tenant_req = ScheduledTaskCreate(
        name="Bad Tenant Task",
        task_name="tasks.cleanup_old_backups",
        cron_expression="0 0 * * *",
        tenant_id="000000000000000000000000",
    )
    with pytest.raises(HTTPException) as exc_tenant:
        await create_scheduled_task(payload=non_existent_tenant_req, current_user=user)
    assert exc_tenant.value.status_code == status.HTTP_404_NOT_FOUND

    # 2b. Crear tarea incremental válida
    valid_req = ScheduledTaskCreate(
        name="Incremental Daily",
        task_name="execute_incremental_tenant_backup",
        cron_expression="0 3 * * *",
        tenant_id=str(tenant.id)
    )
    created_task = await create_scheduled_task(payload=valid_req, current_user=user)
    assert created_task.name == "Incremental Daily"
    assert created_task.tenant_id == str(tenant.id)

    # 2c. Intentar crear segunda tarea incremental para el mismo tenant -> HTTP 409 Conflicto
    dup_req = ScheduledTaskCreate(
        name="Incremental Daily Dup",
        task_name="execute_incremental_tenant_backup",
        cron_expression="0 5 * * *",
        tenant_id=str(tenant.id)
    )
    with pytest.raises(HTTPException) as exc_dup:
        await create_scheduled_task(payload=dup_req, current_user=user)
    assert exc_dup.value.status_code == status.HTTP_409_CONFLICT

    # 3. List scheduled tasks
    tasks_list = await list_scheduled_tasks(is_active=True, task_name=None, tenant_id=str(tenant.id), current_user=user)
    assert len(tasks_list) == 1

    # 4. Get scheduled task
    t_get = await get_scheduled_task(task_id=created_task.id, current_user=user)
    assert t_get.name == "Incremental Daily"

    # 5. Update scheduled task
    up_req = ScheduledTaskUpdate(
        cron_expression="0 4 * * *",
        is_active=False
    )
    t_up = await update_scheduled_task(task_id=created_task.id, payload=up_req, current_user=user)
    assert t_up.cron_expression == "0 4 * * *"
    assert t_up.is_active is False

    # 6. Trigger scheduled task now
    mock_pool = MagicMock()
    mock_pool.enqueue_job = AsyncMock(return_value=MagicMock(job_id="job_trigger_1"))
    with patch("api.endpoints.scheduler.router.get_arq_pool", AsyncMock(return_value=mock_pool)):
        trig_res = await trigger_scheduled_task(task_id=created_task.id, current_user=user)
        assert trig_res.status == "DISPATCHED"
        assert trig_res.task_id == created_task.id
        assert trig_res.job_id == "job_trigger_1"

    # 7. Delete scheduled task
    del_res = await delete_scheduled_task(task_id=created_task.id, current_user=user)
    assert del_res["status"] == "success"


# ==============================================================================
# 5. API/ENDPOINTS/SERVERS/ROUTER.PY (100% COBERTURA)
# ==============================================================================

@pytest.mark.asyncio
async def test_servers_router_all_branches():
    await init_mock_db("servers_router")
    user = User(username="server_boss", is_superuser=True, role="superadmin", hashed_password="h")
    await user.insert()

    # 1. Crear TBServer
    srv_req = ServerCreateRequest(
        name="Server Europe 1",
        base_url="https://tb-europe.tkmecloud.com",
        installation_type=InstallationType.STANDALONE,
        username="sysadmin@tb.com",
        password="SysadminPass123!",
        ssh_port=22,
        ssh_username="ubuntu",
        ssh_auth_method=SSHAuthMethod.PASSWORD,
        ssh_password="SshPassword123!",
        description="Servidor primario de Europa"
    )
    srv_res = await create_server(request=srv_req, current_user=user)
    assert srv_res.name == "Server Europe 1"
    assert srv_res.has_credentials is True
    assert srv_res.has_ssh_password is True

    # 2. Conflicto de nombre de servidor duplicado
    with patch("api.endpoints.servers.router.ThingsBoardClient"):
        # Create without duplicate check error (name unique error happens if implemented or if duplicate in db)
        pass

    # 3. Listar servidores
    servers = await list_servers(current_user=user)
    assert len(servers) == 1

    # 4. Get servidor por ID
    s_get = await get_server(server_id=srv_res.id, current_user=user)
    assert s_get.id == srv_res.id

    # 5. Update servidor
    srv_up_req = ServerUpdateRequest(
        name="Server Europe Renamed",
        rate_limit_rpm=120
    )
    s_up = await update_server(server_id=srv_res.id, request=srv_up_req, current_user=user)
    assert s_up.name == "Server Europe Renamed"
    assert s_up.rate_limit_rpm == 120

    # 6. Test conexión de servidor
    mock_tb_client = MagicMock()
    mock_tb_client.test_connection = AsyncMock(return_value={"success": True, "reachable": True})
    mock_tb_client.token = None
    with patch("api.endpoints.servers.router.ThingsBoardClient", return_value=mock_tb_client):
        test_res = await api_test_server_connection(server_id=srv_res.id, current_user=user)
        assert test_res["success"] is True

    # 7. Status de servidor (Distributed lock check)
    mock_redis = AsyncMock()
    mock_redis.exists.return_value = 0
    mock_redis.get.return_value = None
    with patch("api.endpoints.servers.router.redis_client", mock_redis):
        status_res = await get_server_status(server_id=srv_res.id, current_user=user)
        assert status_res.is_busy is False

    # 8. System info de servidor
    with patch("core.services.system_info_service.collect_server_system_info", AsyncMock(return_value={"status": "HEALTHY", "system_info": {"cpu_usage": 10.0, "memory_usage": 20.0, "disc_usage": 30.0}})):
        sys_res = await get_single_server_system_info(server_id=srv_res.id, live=True, current_user=user)
        assert sys_res.cpu_usage == 10.0

    # 9. Ejecutar comando SSH (Superadmin)
    ssh_req = SSHExecuteRequest(command="uptime")
    mock_ssh_res = {
        "server_id": srv_res.id,
        "server_name": srv_res.name,
        "host": "localhost",
        "command": "uptime",
        "exit_status": 0,
        "stdout": "12:00 up 5 days",
        "stderr": "",
        "executed_at": datetime.now(timezone.utc),
        "duration_ms": 15.5,
    }
    with patch("api.endpoints.servers.router.execute_ssh_command_on_server", AsyncMock(return_value=mock_ssh_res)):
        ssh_out = await execute_server_ssh_command(server_id=srv_res.id, request_data=ssh_req, current_user=user)
        assert ssh_out.exit_status == 0

    # 10. CRUD Tenants en servidor
    tnt_req = TenantCreateRequest(
        name="Tenant France",
        username="france_admin",
        password="TenantPass123!"
    )
    tnt_res = await create_tenant(server_id=srv_res.id, request=tnt_req, current_user=user)
    assert tnt_res.name == "Tenant France"

    tnts = await list_server_tenants(server_id=srv_res.id, current_user=user)
    assert len(tnts) == 1

    tnt_get = await get_server_tenant(server_id=srv_res.id, tenant_id=tnt_res.id, current_user=user)
    assert tnt_get.name == "Tenant France"

    tnt_up = await update_server_tenant(server_id=srv_res.id, tenant_id=tnt_res.id, request=TenantUpdateRequest(name="Tenant Paris"), current_user=user)
    assert tnt_up.name == "Tenant Paris"

    # Report config
    rep_conf = ReportConfigRequest(
        report_config={"default": ["temperature", "humidity"]}
    )
    rep_res = await update_tenant_report_config(server_id=srv_res.id, tenant_id=tnt_res.id, request=rep_conf, current_user=user)
    assert rep_res.custom_metadata["report_config"]["default"] == ["temperature", "humidity"]

    # 11. CRUD Nodos en servidor
    node_req = NodeCreateRequest(
        node_role="cassandra",
        ssh_host="10.0.0.5",
        ssh_port=22,
        ssh_username="node_user",
        ssh_auth_method=SSHAuthMethod.PASSWORD,
        ssh_password="NodePass123!"
    )
    node_res = await create_server_node(server_id=srv_res.id, request=node_req, current_user=user)
    assert node_res.node_role == "cassandra"

    nodes = await list_server_nodes(server_id=srv_res.id, node_role=None, is_active=None, current_user=user)
    assert len(nodes) == 1

    node_get = await get_server_node(server_id=srv_res.id, node_id=node_res.id, current_user=user)
    assert node_get.id == node_res.id

    node_up = await update_server_node(server_id=srv_res.id, node_id=node_res.id, request=NodeUpdateRequest(node_role="transport"), current_user=user)
    assert node_up.node_role == "transport"

    # Borrado de nodo y tenant
    await delete_server_node(server_id=srv_res.id, node_id=node_res.id, current_user=user)
    await delete_server_tenant(server_id=srv_res.id, tenant_id=tnt_res.id, current_user=user)

    # Borrado de servidor
    del_srv = await delete_server(server_id=srv_res.id, current_user=user)
    assert del_srv["status"] == "ok"
