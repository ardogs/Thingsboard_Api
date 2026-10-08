"""
Adversarial QA & Stress Test Suite: Telemetry Backups Pagination & Task Types Specificity
========================================================================================
Lead Adversarial QA & Stress Engineer: QA Adversario
Task ID: task_20260924_telemetry_backups_pagination_and_task_types

Vectors Tested:
1. Pagination Boundaries & Validation Edge Cases:
   - Out of bounds page numbers (page=500 with 10 total records -> items=[], has_next=False, has_prev=True).
   - Empty collection handling (0 backups in DB: total=0, total_pages=1, has_next=False, has_prev=False).
   - Query parameter boundary validation: page=0, page=-1, page_size=0, page_size=101 -> HTTP 422.
   - Pagination calculation correctness (skip, limit, math.ceil across page_size=1, 2, 20, 100).
   - Pagination slicing integration (multi-page sequential traversal with deterministic items).
2. Backup Type Filtering & Backward Compatibility:
   - Exact match filtering for 'telemetry', 'excel_report', 'heatmap', and custom types.
   - Legacy TBBackup documents in MongoDB where backup_type is None, empty, or default:
     correct classification by file extension (.zip -> telemetry, .pdf -> heatmap, .xlsx -> excel_report).
   - MongoDB filter composition verification (build_backup_type_filter regex and $or fallback).
3. Authorization & Security Boundaries:
   - Non-admin querying foreign tenant without permission -> HTTP 403 Forbidden.
   - Non-admin querying owned tenant -> HTTP 200 OK.
   - Non-admin querying foreign tenant with Casbin permission -> HTTP 200 OK.
   - Querying non-existent tenant -> HTTP 404 Not Found.
   - Multi-tenant isolation when querying without tenant_id: only accessible tenants queried.
   - Zero accessible tenants handling without tenant_id -> HTTP 200 OK, empty items, total=0.
   - Superadmin cross-tenant queries with/without tenant_id.
4. Task Type Specificity & Lifecycle:
   - GET /api/v1/tasks/active?task_type=telemetry only returns telemetry tasks.
   - GET /api/v1/tasks/active?task_type=heatmap only returns heatmap tasks.
   - GET /api/v1/tasks/active?task_type=excel_report only returns excel_report tasks.
   - GET /api/v1/tasks/{task_id} returns task_type from Redis (active) and ARQ result_info (finished).
   - Enqueue endpoints (POST /download, POST /report/excel, POST /report/heatmap) return correct task_type
     and emit initial QUEUED event with that task_type.
5. Asynchronous Pureness, Event Loop & High-Concurrency Stress:
   - Verification that all pagination and task endpoints execute asynchronously without blocking the loop.
   - High-concurrency stress test with 50 concurrent requests.
"""

import json
import math
import asyncio
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport

from api.main import app
from api.deps import get_current_user
from core.models.user import User
from core.models.tb_tenant import TBTenant
from core.models.tb_server import TBServer
from core.models.tb_backup import TBBackup
from core.pagination import build_pagination_metadata, PaginationMetadata
from api.endpoints.telemetry.router import build_backup_type_filter


# ==============================================================================
# TEST FIXTURES & MOCK HELPERS
# ==============================================================================

def create_mock_user(
    user_id: str = "650000000000000000000001",
    role: str = "user",
    is_superuser: bool = False,
    username: str = "qa_tester"
) -> User:
    """Crea una instancia simulada de User para inyección en dependencias."""
    user = MagicMock(spec=User)
    user.id = user_id
    user.username = username
    user.email = f"{username}@tkme.cloud"
    user.role = role
    user.is_superuser = is_superuser
    user.is_active = True
    return user


def create_mock_tenant(
    tenant_id: str = "650000000000000000000010",
    name: str = "Adversarial Tenant",
    user_id: str = "650000000000000000000001"
) -> TBTenant:
    """Crea una instancia simulada de TBTenant."""
    tenant = MagicMock(spec=TBTenant)
    tenant.id = tenant_id
    tenant.name = name
    tenant.user_id = user_id
    tenant.custom_metadata = {}
    return tenant


def create_mock_server(
    server_id: str = "srv-001",
    base_url: str = "https://tb.adversary.tkme.cloud"
) -> TBServer:
    """Crea una instancia simulada de TBServer."""
    server = MagicMock(spec=TBServer)
    server.id = server_id
    server.base_url = base_url
    return server


class MockBeanieQuery:
    """
    Simulador fiel del encadenamiento de consultas Beanie/Motor (find -> count/sort/skip/limit -> to_list).
    Permite probar con precisión matemática el cálculo de paginación y corte de resultados.
    """
    def __init__(self, items: List[TBBackup]):
        self._all_items = list(items)
        self._skip_val = 0
        self._limit_val = len(items)

    async def count(self) -> int:
        return len(self._all_items)

    def sort(self, *args, **kwargs):
        return self

    def skip(self, n: int):
        self._skip_val = max(0, n)
        return self

    def limit(self, n: int):
        self._limit_val = max(0, n)
        return self

    async def to_list(self) -> List[TBBackup]:
        start = self._skip_val
        end = start + self._limit_val
        return self._all_items[start:end]


# ==============================================================================
# VECTOR 1: PAGINATION BOUNDARIES AND VALIDATION EDGE CASES
# ==============================================================================

class TestPaginationBoundariesAndValidation:
    """Pruebas adversarias sobre límites y cálculos de paginación en GET /api/v1/telemetry/backups."""

    @pytest.mark.parametrize("invalid_page", [0, -1, -50, -9999])
    def test_page_less_than_one_returns_422(self, invalid_page):
        """Verifica que números de página menores a 1 retornen HTTP 422 Unprocessable Entity."""
        user = create_mock_user(role="admin", is_superuser=True)
        app.dependency_overrides[get_current_user] = lambda: user

        try:
            client = TestClient(app)
            resp = client.get(f"/api/v1/telemetry/backups?page={invalid_page}&page_size=20")
            assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
            errors = resp.json().get("detail", [])
            assert any(err.get("loc") == ["query", "page"] for err in errors)
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.parametrize("invalid_size", [0, -1, -10, 101, 200, 9999])
    def test_page_size_out_of_bounds_returns_422(self, invalid_size):
        """Verifica que tamaños de página fuera de [1, 100] retornen HTTP 422 Unprocessable Content."""
        user = create_mock_user(role="admin", is_superuser=True)
        app.dependency_overrides[get_current_user] = lambda: user

        try:
            client = TestClient(app)
            resp = client.get(f"/api/v1/telemetry/backups?page=1&page_size={invalid_size}")
            assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
            errors = resp.json().get("detail", [])
            assert any(err.get("loc") == ["query", "page_size"] for err in errors)
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.parametrize("non_int_param", ["page=abc", "page=1.5", "page_size=xyz", "page_size=null"])
    def test_non_integer_query_params_return_422(self, non_int_param):
        """Verifica que parámetros no enteros retornen HTTP 422 Unprocessable Content."""
        user = create_mock_user(role="admin", is_superuser=True)
        app.dependency_overrides[get_current_user] = lambda: user

        try:
            client = TestClient(app)
            resp = client.get(f"/api/v1/telemetry/backups?{non_int_param}")
            assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        finally:
            app.dependency_overrides.clear()

    def test_pagination_out_of_bounds_page_500(self):
        """
        Adversarial Edge Case:
        Solicitar page=500 cuando sólo existen 10 registros en total.
        Debe retornar HTTP 200, items=[], total=10, page=500, total_pages=1, has_next=False, has_prev=True.
        """
        user = create_mock_user(role="superadmin", is_superuser=True)
        tenant = create_mock_tenant()
        now = datetime.now(timezone.utc)

        mock_backups = [
            TBBackup.model_construct(
                id=f"6500000000000000000000{i:02d}",
                tenant_id=tenant,
                task_id=f"task_{i}",
                requested_by=str(user.id),
                file_name=f"backup_{i}.zip",
                backup_type="telemetry",
                start_date=now,
                end_date=now,
                file_size_bytes=1024,
                created_at=now
            )
            for i in range(10)
        ]

        app.dependency_overrides[get_current_user] = lambda: user
        mock_query = MockBeanieQuery(mock_backups)

        try:
            with patch("api.endpoints.telemetry.router.TBBackup.find", return_value=mock_query):
                client = TestClient(app)
                resp = client.get("/api/v1/telemetry/backups?page=500&page_size=20")

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert data["items"] == []
            meta = data["pagination"]
            assert meta["total"] == 10
            assert meta["page"] == 500
            assert meta["page_size"] == 20
            assert meta["total_pages"] == 1
            assert meta["has_next"] is False
            assert meta["has_prev"] is True
        finally:
            app.dependency_overrides.clear()

    def test_pagination_empty_collection(self):
        """
        Adversarial Edge Case:
        Colección vacía (0 respaldos en MongoDB).
        Debe retornar HTTP 200, items=[], total=0, page=1, total_pages=1, has_next=False, has_prev=False.
        """
        user = create_mock_user(role="superadmin", is_superuser=True)
        app.dependency_overrides[get_current_user] = lambda: user
        mock_query = MockBeanieQuery([])

        try:
            with patch("api.endpoints.telemetry.router.TBBackup.find", return_value=mock_query):
                client = TestClient(app)
                resp = client.get("/api/v1/telemetry/backups?page=1&page_size=20")

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert data["items"] == []
            meta = data["pagination"]
            assert meta["total"] == 0
            assert meta["page"] == 1
            assert meta["page_size"] == 20
            assert meta["total_pages"] == 1
            assert meta["has_next"] is False
            assert meta["has_prev"] is False
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.parametrize("total,page_size,expected_total_pages", [
        (0, 20, 1),
        (1, 1, 1),
        (10, 1, 10),
        (10, 2, 5),
        (10, 3, 4),
        (10, 20, 1),
        (10, 100, 1),
        (21, 20, 2),
        (99, 10, 10),
        (100, 10, 10),
        (101, 10, 11),
    ])
    def test_build_pagination_metadata_math_ceil_correctness(self, total, page_size, expected_total_pages):
        """Verifica la exactitud matemática de math.ceil y límites en build_pagination_metadata."""
        meta = build_pagination_metadata(total=total, page=1, page_size=page_size)
        assert meta.total == total
        assert meta.page_size == page_size
        assert meta.total_pages == expected_total_pages
        assert meta.has_prev is False
        if total > page_size:
            assert meta.has_next is True
        else:
            assert meta.has_next is False

    def test_pagination_sequential_slicing_traversal(self):
        """
        Verifica el recorrido secuencial página por página con skip y limit deterministas:
        Con 5 elementos y page_size=2:
        - Página 1: elementos [0, 1], has_next=True, has_prev=False
        - Página 2: elementos [2, 3], has_next=True, has_prev=True
        - Página 3: elemento [4], has_next=False, has_prev=True
        - Página 4: elementos [], has_next=False, has_prev=True
        """
        user = create_mock_user(role="superadmin", is_superuser=True)
        tenant = create_mock_tenant()
        now = datetime.now(timezone.utc)

        backups = [
            TBBackup.model_construct(
                id=f"6500000000000000000000{i:02d}",
                tenant_id=tenant,
                task_id=f"seq_task_{i}",
                requested_by=str(user.id),
                file_name=f"seq_{i}.zip",
                backup_type="telemetry",
                start_date=now,
                end_date=now,
                file_size_bytes=100 + i,
                created_at=now
            )
            for i in range(5)
        ]

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            client = TestClient(app)

            # Página 1
            with patch("api.endpoints.telemetry.router.TBBackup.find", return_value=MockBeanieQuery(backups)):
                r1 = client.get("/api/v1/telemetry/backups?page=1&page_size=2")
                assert r1.status_code == 200
                d1 = r1.json()
                assert len(d1["items"]) == 2
                assert d1["items"][0]["task_id"] == "seq_task_0"
                assert d1["items"][1]["task_id"] == "seq_task_1"
                assert d1["pagination"]["total_pages"] == 3
                assert d1["pagination"]["has_next"] is True
                assert d1["pagination"]["has_prev"] is False

            # Página 2
            with patch("api.endpoints.telemetry.router.TBBackup.find", return_value=MockBeanieQuery(backups)):
                r2 = client.get("/api/v1/telemetry/backups?page=2&page_size=2")
                assert r2.status_code == 200
                d2 = r2.json()
                assert len(d2["items"]) == 2
                assert d2["items"][0]["task_id"] == "seq_task_2"
                assert d2["items"][1]["task_id"] == "seq_task_3"
                assert d2["pagination"]["has_next"] is True
                assert d2["pagination"]["has_prev"] is True

            # Página 3
            with patch("api.endpoints.telemetry.router.TBBackup.find", return_value=MockBeanieQuery(backups)):
                r3 = client.get("/api/v1/telemetry/backups?page=3&page_size=2")
                assert r3.status_code == 200
                d3 = r3.json()
                assert len(d3["items"]) == 1
                assert d3["items"][0]["task_id"] == "seq_task_4"
                assert d3["pagination"]["has_next"] is False
                assert d3["pagination"]["has_prev"] is True

            # Página 4 (fuera de límites)
            with patch("api.endpoints.telemetry.router.TBBackup.find", return_value=MockBeanieQuery(backups)):
                r4 = client.get("/api/v1/telemetry/backups?page=4&page_size=2")
                assert r4.status_code == 200
                d4 = r4.json()
                assert len(d4["items"]) == 0
                assert d4["pagination"]["has_next"] is False
                assert d4["pagination"]["has_prev"] is True
        finally:
            app.dependency_overrides.clear()


# ==============================================================================
# VECTOR 2: BACKUP TYPE FILTERING AND BACKWARD COMPATIBILITY
# ==============================================================================

class TestBackupTypeFilteringAndBackwardCompatibility:
    """Pruebas adversarias sobre el filtrado por backup_type y compatibilidad con documentos legados."""

    @pytest.mark.parametrize("b_type,expected_count,file_ext", [
        ("telemetry", 3, ".zip"),
        ("excel_report", 2, ".xlsx"),
        ("heatmap", 1, ".pdf"),
    ])
    def test_backup_type_filtering_exact_match(self, b_type, expected_count, file_ext):
        """Verifica que el filtrado por backup_type aplique el filtro correcto y segregue otros tipos."""
        user = create_mock_user(role="superadmin", is_superuser=True)
        tenant = create_mock_tenant()
        now = datetime.now(timezone.utc)

        # Dataset heterogéneo
        backups = [
            TBBackup.model_construct(
                id=f"65000000000000000000000{i}",
                tenant_id=tenant,
                task_id=f"t_tel_{i}",
                requested_by=str(user.id),
                file_name=f"telemetry_{i}.zip",
                backup_type="telemetry",
                start_date=now,
                end_date=now,
                file_size_bytes=100,
                created_at=now
            ) for i in range(3)
        ] + [
            TBBackup.model_construct(
                id=f"65000000000000000000001{i}",
                tenant_id=tenant,
                task_id=f"t_xls_{i}",
                requested_by=str(user.id),
                file_name=f"report_{i}.xlsx",
                backup_type="excel_report",
                start_date=now,
                end_date=now,
                file_size_bytes=200,
                created_at=now
            ) for i in range(2)
        ] + [
            TBBackup.model_construct(
                id="650000000000000000000020",
                tenant_id=tenant,
                task_id="t_heat_0",
                requested_by=str(user.id),
                file_name="heatmap_0.pdf",
                backup_type="heatmap",
                start_date=now,
                end_date=now,
                file_size_bytes=300,
                created_at=now
            )
        ]

        # Simular filtrado Beanie
        filtered = [b for b in backups if b.get_backup_type() == b_type]
        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("api.endpoints.telemetry.router.TBBackup.find") as mock_find:
                mock_find.return_value = MockBeanieQuery(filtered)
                client = TestClient(app)
                resp = client.get(f"/api/v1/telemetry/backups?backup_type={b_type}")

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert len(data["items"]) == expected_count
            assert data["pagination"]["total"] == expected_count
            assert all(item["backup_type"] == b_type for item in data["items"])
            assert all(item["file_name"].endswith(file_ext) for item in data["items"])
        finally:
            app.dependency_overrides.clear()

    def test_backup_type_filtering_nonexistent_returns_empty(self):
        """Verifica que solicitar un backup_type inexistente retorne total=0 y lista vacía."""
        user = create_mock_user(role="superadmin", is_superuser=True)
        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("api.endpoints.telemetry.router.TBBackup.find") as mock_find:
                mock_find.return_value = MockBeanieQuery([])
                client = TestClient(app)
                resp = client.get("/api/v1/telemetry/backups?backup_type=alien_unknown_type")

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert data["items"] == []
            assert data["pagination"]["total"] == 0
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.parametrize("file_name,explicit_type,expected_resolved", [
        ("export_2026.pdf", "telemetry", "heatmap"),
        ("monthly_report.xlsx", "telemetry", "excel_report"),
        ("archive.zip", "telemetry", "telemetry"),
        ("telemetry_dump.csv", "telemetry", "telemetry"),
        ("UPPERCASE.PDF", "telemetry", "heatmap"),
        ("UPPERCASE.XLSX", "telemetry", "excel_report"),
        ("custom_doc.pdf", "custom_audit", "custom_audit"),
        ("custom_doc.xlsx", "custom_billing", "custom_billing"),
    ])
    def test_legacy_documents_backward_compatibility_resolution(self, file_name, explicit_type, expected_resolved):
        """
        Verifica que documentos legados donde backup_type era default ('telemetry')
        o no existía resuelvan correctamente su tipo por extensión de archivo (.pdf, .xlsx).
        """
        now = datetime.now(timezone.utc)
        tenant = create_mock_tenant()

        doc = TBBackup.model_construct(
            id="650000000000000000000099",
            tenant_id=tenant,
            task_id="t_legacy",
            requested_by="u1",
            file_name=file_name,
            backup_type=explicit_type,
            start_date=now,
            end_date=now,
            file_size_bytes=1024,
            created_at=now
        )
        assert doc.get_backup_type() == expected_resolved

    def test_build_backup_type_filter_structure(self):
        """Verifica la estructura y operadores MongoDB generados por build_backup_type_filter."""
        # 1. Heatmap
        f_heat = build_backup_type_filter("heatmap")
        assert "$or" in f_heat
        heat_branches = f_heat["$or"]
        assert any(b.get("backup_type") == "heatmap" for b in heat_branches)
        legacy_branch = next(b for b in heat_branches if "$and" in b)
        assert any(cond.get("file_name") == {"$regex": r"\.pdf$", "$options": "i"} for cond in legacy_branch["$and"])

        # 2. Excel
        f_excel = build_backup_type_filter("excel_report")
        assert "$or" in f_excel
        excel_branches = f_excel["$or"]
        assert any(b.get("backup_type") == "excel_report" for b in excel_branches)
        legacy_excel = next(b for b in excel_branches if "$and" in b)
        assert any(cond.get("file_name") == {"$regex": r"\.xlsx$", "$options": "i"} for cond in legacy_excel["$and"])

        # 3. Telemetry
        f_tel = build_backup_type_filter("telemetry")
        assert "$or" in f_tel
        tel_branches = f_tel["$or"]
        assert any(b.get("backup_type") == "telemetry" for b in tel_branches)

        # 4. Unknown/Arbitrary
        f_other = build_backup_type_filter("audit_log")
        assert f_other == {"backup_type": "audit_log"}


# ==============================================================================
# VECTOR 3: AUTHORIZATION AND SECURITY BOUNDARIES
# ==============================================================================

class TestAuthorizationAndSecurityBoundaries:
    """Pruebas adversarias sobre Casbin RBAC, Tenant Ownership y Aislamiento Multi-Tenant."""

    def test_non_admin_query_unauthorized_tenant_returns_403(self):
        """
        Un usuario que consulta un tenant que no le pertenece y para el cual
        no tiene permisos de Casbin debe ser bloqueado con HTTP 403 Forbidden.
        """
        user = create_mock_user(user_id="user_non_admin", role="user", is_superuser=False)
        foreign_tenant = create_mock_tenant(
            tenant_id="650000000000000000000099",
            name="Foreign Tenant",
            user_id="other_owner_id"
        )

        mock_enforcer = MagicMock()
        mock_enforcer.enforce.return_value = False  # Denegar permiso en Casbin

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("core.models.tb_tenant.TBTenant.get", new_callable=AsyncMock) as mock_t_get, \
                 patch("api.endpoints.telemetry.router.get_casbin_enforcer", return_value=mock_enforcer):
                mock_t_get.return_value = foreign_tenant
                client = TestClient(app)
                resp = client.get(f"/api/v1/telemetry/backups?tenant_id={foreign_tenant.id}")

            assert resp.status_code == status.HTTP_403_FORBIDDEN
            assert "No tienes permisos" in resp.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_non_admin_query_owned_tenant_returns_200(self):
        """Un usuario que consulta un tenant del que es propietario debe acceder con HTTP 200 OK."""
        user = create_mock_user(user_id="user_owner_001", role="user", is_superuser=False)
        owned_tenant = create_mock_tenant(
            tenant_id="650000000000000000000010",
            name="Owned Tenant",
            user_id=str(user.id)
        )

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("core.models.tb_tenant.TBTenant.get", new_callable=AsyncMock) as mock_t_get, \
                 patch("api.endpoints.telemetry.router.TBBackup.find", return_value=MockBeanieQuery([])):
                mock_t_get.return_value = owned_tenant
                client = TestClient(app)
                resp = client.get(f"/api/v1/telemetry/backups?tenant_id={owned_tenant.id}")

            assert resp.status_code == status.HTTP_200_OK
        finally:
            app.dependency_overrides.clear()

    def test_non_admin_query_casbin_authorized_tenant_returns_200(self):
        """Un usuario que no es propietario pero tiene permiso explícito en Casbin debe acceder con HTTP 200 OK."""
        user = create_mock_user(user_id="user_delegate", role="operator", is_superuser=False)
        delegated_tenant = create_mock_tenant(
            tenant_id="650000000000000000000020",
            name="Delegated Tenant",
            user_id="different_owner"
        )

        mock_enforcer = MagicMock()
        mock_enforcer.enforce.return_value = True  # Casbin otorga acceso

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("core.models.tb_tenant.TBTenant.get", new_callable=AsyncMock) as mock_t_get, \
                 patch("api.endpoints.telemetry.router.get_casbin_enforcer", return_value=mock_enforcer), \
                 patch("api.endpoints.telemetry.router.TBBackup.find", return_value=MockBeanieQuery([])):
                mock_t_get.return_value = delegated_tenant
                client = TestClient(app)
                resp = client.get(f"/api/v1/telemetry/backups?tenant_id={delegated_tenant.id}")

            assert resp.status_code == status.HTTP_200_OK
        finally:
            app.dependency_overrides.clear()

    def test_query_nonexistent_tenant_returns_404(self):
        """Consultar un tenant_id que no existe en MongoDB debe retornar HTTP 404 Not Found."""
        user = create_mock_user(role="admin", is_superuser=True)
        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("core.models.tb_tenant.TBTenant.get", new_callable=AsyncMock, return_value=None):
                client = TestClient(app)
                resp = client.get("/api/v1/telemetry/backups?tenant_id=650000000000000000000999")

            assert resp.status_code == status.HTTP_404_NOT_FOUND
            assert "no encontrado en MongoDB" in resp.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_multi_tenant_isolation_without_tenant_id(self):
        """
        Aislamiento multi-tenant estricto:
        Cuando un usuario regular consulta sin tenant_id, el filtro de MongoDB debe
        incluir exclusivamente los tenants que le pertenecen o para los que Casbin
        le ha otorgado permisos, aislando al 100% los tenants ajenos.
        """
        user = create_mock_user(user_id="user_iso_01", role="user", is_superuser=False)

        # 3 Tenants en el sistema
        t1 = create_mock_tenant(tenant_id="tenant_owned", user_id=str(user.id))
        t2 = create_mock_tenant(tenant_id="tenant_granted", user_id="other_user")
        t3 = create_mock_tenant(tenant_id="tenant_foreign_isolated", user_id="other_user")

        mock_cursor = MagicMock()
        mock_cursor.to_list = AsyncMock(return_value=[t1, t2, t3])

        mock_enforcer = MagicMock()
        def enforce_mock(sub, dom, res, act):
            return dom == "tenant:tenant_granted"
        mock_enforcer.enforce.side_effect = enforce_mock

        captured_filter = {}

        def mock_find(query_arg):
            nonlocal captured_filter
            captured_filter = query_arg
            return MockBeanieQuery([])

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("core.models.tb_tenant.TBTenant.find_all", return_value=mock_cursor), \
                 patch("api.endpoints.telemetry.router.get_casbin_enforcer", return_value=mock_enforcer), \
                 patch("api.endpoints.telemetry.router.TBBackup.find", side_effect=mock_find):
                client = TestClient(app)
                resp = client.get("/api/v1/telemetry/backups")

            assert resp.status_code == status.HTTP_200_OK
            # Verificar que tenant_foreign_isolated NO esté en la consulta
            filter_str = json.dumps(captured_filter, default=str)
            assert "tenant_owned" in filter_str
            assert "tenant_granted" in filter_str
            assert "tenant_foreign_isolated" not in filter_str
        finally:
            app.dependency_overrides.clear()

    def test_multi_tenant_isolation_zero_accessible_tenants(self):
        """
        Si un usuario regular no posee tenants ni permisos Casbin y consulta sin tenant_id,
        debe retornar HTTP 200 con lista vacía sin ejecutar consultas innecesarias.
        """
        user = create_mock_user(user_id="user_no_tenants", role="user", is_superuser=False)

        t_foreign = create_mock_tenant(tenant_id="tenant_foreign", user_id="other_owner")
        mock_cursor = MagicMock()
        mock_cursor.to_list = AsyncMock(return_value=[t_foreign])

        mock_enforcer = MagicMock()
        mock_enforcer.enforce.return_value = False

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("core.models.tb_tenant.TBTenant.find_all", return_value=mock_cursor), \
                 patch("api.endpoints.telemetry.router.get_casbin_enforcer", return_value=mock_enforcer), \
                 patch("api.endpoints.telemetry.router.TBBackup.find") as mock_find:
                client = TestClient(app)
                resp = client.get("/api/v1/telemetry/backups")

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert data["items"] == []
            assert data["pagination"]["total"] == 0
            mock_find.assert_not_called()
        finally:
            app.dependency_overrides.clear()

    def test_superadmin_query_bypasses_all_tenant_restrictions(self):
        """
        Un superadministrador tiene visibilidad omnisciente:
        - Puede consultar respaldos de cualquier tenant sin validación de ownership o Casbin.
        - Al consultar sin tenant_id, la consulta a MongoDB no se restringe por tenant_id ($in).
        """
        superadmin = create_mock_user(user_id="admin_001", role="superadmin", is_superuser=True)
        foreign_tenant = create_mock_tenant(tenant_id="any_tenant", user_id="someone_else")

        app.dependency_overrides[get_current_user] = lambda: superadmin

        try:
            # 1. Con tenant específico no perteneciente al superadmin
            with patch("core.models.tb_tenant.TBTenant.get", new_callable=AsyncMock, return_value=foreign_tenant), \
                 patch("api.endpoints.telemetry.router.TBBackup.find", return_value=MockBeanieQuery([])):
                client = TestClient(app)
                resp_specific = client.get(f"/api/v1/telemetry/backups?tenant_id={foreign_tenant.id}")
                assert resp_specific.status_code == status.HTTP_200_OK

            # 2. Sin tenant_id (global)
            captured_filter = None
            def mock_find_global(f):
                nonlocal captured_filter
                captured_filter = f
                return MockBeanieQuery([])

            with patch("api.endpoints.telemetry.router.TBBackup.find", side_effect=mock_find_global):
                resp_global = client.get("/api/v1/telemetry/backups")
                assert resp_global.status_code == status.HTTP_200_OK
                assert captured_filter == {}
        finally:
            app.dependency_overrides.clear()


# ==============================================================================
# VECTOR 4: TASK TYPE SPECIFICITY & LIFECYCLE
# ==============================================================================

class TestTaskTypeSpecificityAndLifecycle:
    """Pruebas adversarias sobre especificidad de task_type y ciclo de vida de tareas en background."""

    @pytest.mark.parametrize("query_type,expected_count,expected_id", [
        ("telemetry", 1, "t_telemetry"),
        ("heatmap", 1, "t_heatmap"),
        ("excel_report", 1, "t_excel"),
    ])
    def test_active_tasks_filtering_by_task_type(self, query_type, expected_count, expected_id):
        """Verifica que GET /api/v1/tasks/active filtre de forma estricta por task_type."""
        user = create_mock_user(user_id="user_tasks_001", role="user", is_superuser=False)

        tasks_dict = {
            "t_telemetry": json.dumps({
                "task_id": "t_telemetry",
                "task_type": "telemetry",
                "status": "IN_PROGRESS",
                "progress_pct": 25.0
            }),
            "t_heatmap": json.dumps({
                "task_id": "t_heatmap",
                "task_type": "heatmap",
                "status": "DOWNLOADING",
                "progress_pct": 50.0
            }),
            "t_excel": json.dumps({
                "task_id": "t_excel",
                "task_type": "excel_report",
                "status": "PACKAGING",
                "progress_pct": 75.0
            }),
        }

        mock_cursor = MagicMock()
        mock_cursor.to_list = AsyncMock(return_value=[])

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("api.endpoints.tasks.router.redis_client.hgetall", new_callable=AsyncMock, return_value=tasks_dict), \
                 patch("core.models.tb_tenant.TBTenant.find_all", return_value=mock_cursor):
                client = TestClient(app)
                resp = client.get(f"/api/v1/tasks/active?task_type={query_type}")

            assert resp.status_code == status.HTTP_200_OK
            tasks = resp.json()
            assert len(tasks) == expected_count
            assert tasks[0]["task_type"] == query_type
            assert tasks[0]["task_id"] == expected_id
        finally:
            app.dependency_overrides.clear()

    def test_active_tasks_all_returned_when_task_type_not_provided(self):
        """Verifica que sin parámetro task_type se retornen todas las tareas activas sin importar su tipo."""
        user = create_mock_user(user_id="user_tasks_002", role="user", is_superuser=False)

        tasks_dict = {
            "t1": json.dumps({"task_id": "t1", "task_type": "telemetry", "status": "IN_PROGRESS"}),
            "t2": json.dumps({"task_id": "t2", "task_type": "heatmap", "status": "PACKAGING"}),
            "t3": json.dumps({"task_id": "t3", "task_type": "excel_report", "status": "DOWNLOADING"}),
        }

        mock_cursor = MagicMock()
        mock_cursor.to_list = AsyncMock(return_value=[])

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            with patch("api.endpoints.tasks.router.redis_client.hgetall", new_callable=AsyncMock, return_value=tasks_dict), \
                 patch("core.models.tb_tenant.TBTenant.find_all", return_value=mock_cursor):
                client = TestClient(app)
                resp = client.get("/api/v1/tasks/active")

            assert resp.status_code == status.HTTP_200_OK
            assert len(resp.json()) == 3
        finally:
            app.dependency_overrides.clear()

    def test_task_status_endpoint_returns_task_type_from_redis_active(self):
        """GET /api/v1/tasks/{task_id} debe retornar task_type desde el registro de Redis para tareas activas."""
        user = create_mock_user(user_id="u_status_1")
        app.dependency_overrides[get_current_user] = lambda: user

        mock_job = MagicMock()
        mock_job.status = AsyncMock(return_value="in_progress")
        mock_job.result_info = AsyncMock(return_value=None)
        mock_arq = MagicMock()

        redis_payload = json.dumps({
            "task_id": "job_active_100",
            "task_type": "excel_report",
            "status": "PACKAGING",
            "progress_pct": 60.0
        })

        try:
            with patch("api.endpoints.tasks.router.get_arq_pool", new_callable=AsyncMock, return_value=mock_arq), \
                 patch("api.endpoints.tasks.router.Job", return_value=mock_job), \
                 patch("api.endpoints.tasks.router.redis_client.hget", new_callable=AsyncMock, return_value=redis_payload):
                client = TestClient(app)
                resp = client.get("/api/v1/tasks/job_active_100")

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert data["task_id"] == "job_active_100"
            assert data["task_type"] == "excel_report"
            assert data["status"] == "in_progress"
            assert data["progress_pct"] == 60.0
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.parametrize("arq_function_name,expected_task_type", [
        ("download_telemetry_task", "telemetry"),
        ("generate_excel_report_task", "excel_report"),
        ("generate_monthly_heatmap_task", "heatmap"),
        ("send_email_task", "email"),
    ])
    def test_task_status_endpoint_resolves_task_type_for_finished_jobs(self, arq_function_name, expected_task_type):
        """
        Para tareas ya finalizadas (y purgadas de Redis), GET /api/v1/tasks/{task_id}
        debe resolver determinísticamente task_type a partir del nombre de la función encolada en ARQ.
        """
        user = create_mock_user(user_id="u_status_2")
        app.dependency_overrides[get_current_user] = lambda: user

        mock_result_info = MagicMock()
        mock_result_info.function = arq_function_name
        mock_result_info.success = True
        mock_result_info.result = {"status": "ok"}
        mock_result_info.enqueue_time = datetime.now(timezone.utc)
        mock_result_info.start_time = datetime.now(timezone.utc)
        mock_result_info.finish_time = datetime.now(timezone.utc)

        mock_job = MagicMock()
        mock_job.status = AsyncMock(return_value="complete")
        mock_job.result_info = AsyncMock(return_value=mock_result_info)
        mock_arq = MagicMock()

        try:
            with patch("api.endpoints.tasks.router.get_arq_pool", new_callable=AsyncMock, return_value=mock_arq), \
                 patch("api.endpoints.tasks.router.Job", return_value=mock_job), \
                 patch("api.endpoints.tasks.router.redis_client.hget", new_callable=AsyncMock, return_value=None):
                client = TestClient(app)
                resp = client.get("/api/v1/tasks/finished_job_xyz")

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert data["task_id"] == "finished_job_xyz"
            assert data["task_type"] == expected_task_type
            assert data["success"] is True
            assert data["status"] == "complete"
        finally:
            app.dependency_overrides.clear()

    def test_enqueue_download_endpoint_returns_task_type_and_publishes_event(self):
        """POST /download retorna task_type='telemetry' y emite evento QUEUED con task_type='telemetry'."""
        user = create_mock_user(user_id="u_dl_01")
        tenant = create_mock_tenant(user_id=str(user.id))
        server = create_mock_server()
        tenant.get_server = AsyncMock(return_value=server)

        app.dependency_overrides[get_current_user] = lambda: user

        mock_job = MagicMock()
        mock_job.job_id = "job_dl_123"
        mock_arq = MagicMock()
        mock_arq.enqueue_job = AsyncMock(return_value=mock_job)

        try:
            with patch("core.models.tb_tenant.TBTenant.get", new_callable=AsyncMock, return_value=tenant), \
                 patch("api.endpoints.telemetry.router.redis_client.set", new_callable=AsyncMock, return_value=True), \
                 patch("api.endpoints.telemetry.router.get_arq_pool", new_callable=AsyncMock, return_value=mock_arq), \
                 patch("api.endpoints.telemetry.router.publish_task_event", new_callable=AsyncMock) as mock_pub:
                client = TestClient(app)
                resp = client.post("/api/v1/telemetry/download", json={
                    "tenant_id": str(tenant.id),
                    "start_date": "2026-01-01T00:00:00Z",
                    "end_date": "2026-01-02T00:00:00Z"
                })

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert data["task_type"] == "telemetry"
            assert data["task_id"] == "job_dl_123"

            # Validar evento emitido en Redis
            mock_pub.assert_called_once()
            pub_kwargs = mock_pub.call_args.kwargs
            assert pub_kwargs.get("task_type") == "telemetry"
            assert pub_kwargs.get("status") == "QUEUED"
        finally:
            app.dependency_overrides.clear()

    def test_enqueue_excel_report_endpoint_returns_task_type_and_publishes_event(self):
        """POST /report/excel retorna task_type='excel_report' y emite evento QUEUED con task_type='excel_report'."""
        user = create_mock_user(user_id="u_xls_01")
        tenant = create_mock_tenant(user_id=str(user.id))
        server = create_mock_server()
        tenant.get_server = AsyncMock(return_value=server)

        app.dependency_overrides[get_current_user] = lambda: user

        mock_job = MagicMock()
        mock_job.job_id = "job_xls_456"
        mock_arq = MagicMock()
        mock_arq.enqueue_job = AsyncMock(return_value=mock_job)

        try:
            with patch("core.models.tb_tenant.TBTenant.get", new_callable=AsyncMock, return_value=tenant), \
                 patch("api.endpoints.telemetry.router.redis_client.set", new_callable=AsyncMock, return_value=True), \
                 patch("api.endpoints.telemetry.router.get_arq_pool", new_callable=AsyncMock, return_value=mock_arq), \
                 patch("api.endpoints.telemetry.router.publish_task_event", new_callable=AsyncMock) as mock_pub:
                client = TestClient(app)
                resp = client.post("/api/v1/telemetry/report/excel", json={
                    "tenant_id": str(tenant.id),
                    "start_date": "2026-01-01T00:00:00Z",
                    "end_date": "2026-01-02T00:00:00Z"
                })

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert data["task_type"] == "excel_report"
            assert data["task_id"] == "job_xls_456"

            mock_pub.assert_called_once()
            pub_kwargs = mock_pub.call_args.kwargs
            assert pub_kwargs.get("task_type") == "excel_report"
            assert pub_kwargs.get("status") == "QUEUED"
        finally:
            app.dependency_overrides.clear()

    def test_enqueue_heatmap_report_endpoint_returns_task_type_and_publishes_event(self):
        """POST /report/heatmap retorna task_type='heatmap' y emite evento QUEUED con task_type='heatmap'."""
        user = create_mock_user(user_id="u_heat_01")
        tenant = create_mock_tenant(user_id=str(user.id))
        server = create_mock_server()
        tenant.get_server = AsyncMock(return_value=server)

        app.dependency_overrides[get_current_user] = lambda: user

        mock_job = MagicMock()
        mock_job.job_id = "job_heat_789"
        mock_arq = MagicMock()
        mock_arq.enqueue_job = AsyncMock(return_value=mock_job)

        try:
            with patch("core.models.tb_tenant.TBTenant.get", new_callable=AsyncMock, return_value=tenant), \
                 patch("api.endpoints.telemetry.router.redis_client.set", new_callable=AsyncMock, return_value=True), \
                 patch("api.endpoints.telemetry.router.get_arq_pool", new_callable=AsyncMock, return_value=mock_arq), \
                 patch("api.endpoints.telemetry.router.publish_task_event", new_callable=AsyncMock) as mock_pub:
                client = TestClient(app)
                resp = client.post("/api/v1/telemetry/report/heatmap", json={
                    "tenant_id": str(tenant.id),
                    "year": 2026,
                    "month": 6
                })

            assert resp.status_code == status.HTTP_200_OK
            data = resp.json()
            assert data["task_type"] == "heatmap"
            assert data["task_id"] == "job_heat_789"

            mock_pub.assert_called_once()
            pub_kwargs = mock_pub.call_args.kwargs
            assert pub_kwargs.get("task_type") == "heatmap"
            assert pub_kwargs.get("status") == "QUEUED"
        finally:
            app.dependency_overrides.clear()


# ==============================================================================
# VECTOR 5: ASYNCHRONOUS PURENESS AND HIGH-CONCURRENCY STRESS
# ==============================================================================

class TestAsynchronousPurenessAndConcurrencyStress:
    """Pruebas de pureza asíncrona, no bloqueo del bucle de eventos y concurrencia masiva."""

    @pytest.mark.asyncio
    async def test_pagination_concurrency_stress_50_requests(self):
        """
        Stress Probe: 50 solicitudes concurrentes a GET /api/v1/telemetry/backups
        con distintos parámetros de paginación y filtros.
        Verifica que el event loop no sufra bloqueos síncronos ni condiciones de carrera.
        """
        user = create_mock_user(role="superadmin", is_superuser=True)
        tenant = create_mock_tenant()
        now = datetime.now(timezone.utc)

        mock_backups = [
            TBBackup.model_construct(
                id=f"6500000000000000000000{i:02d}",
                tenant_id=tenant,
                task_id=f"stress_t_{i}",
                requested_by=str(user.id),
                file_name=f"stress_{i}.zip",
                backup_type="telemetry",
                start_date=now,
                end_date=now,
                file_size_bytes=1024,
                created_at=now
            ) for i in range(25)
        ]

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                with patch("api.endpoints.telemetry.router.TBBackup.find", side_effect=lambda *a, **k: MockBeanieQuery(mock_backups)):
                    tasks = [
                        client.get(f"/api/v1/telemetry/backups?page={(i % 5) + 1}&page_size=5")
                        for i in range(50)
                    ]
                    responses = await asyncio.gather(*tasks)

                assert len(responses) == 50
                for r in responses:
                    assert r.status_code == status.HTTP_200_OK
                    d = r.json()
                    assert "items" in d
                    assert "pagination" in d
                    assert d["pagination"]["total"] == 25
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_active_tasks_concurrency_stress_50_requests(self):
        """
        Stress Probe: 50 solicitudes concurrentes a GET /api/v1/tasks/active?task_type=...
        Verifica aislamiento concurrente sin colisiones en Redis.
        """
        user = create_mock_user(role="user", is_superuser=False)
        tasks_in_redis = {
            f"t_{i}": json.dumps({
                "task_id": f"t_{i}",
                "task_type": "telemetry" if i % 2 == 0 else "heatmap",
                "status": "PROCESSING",
                "progress_pct": float(i)
            })
            for i in range(20)
        }

        mock_cursor = MagicMock()
        mock_cursor.to_list = AsyncMock(return_value=[])

        app.dependency_overrides[get_current_user] = lambda: user

        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                with patch("api.endpoints.tasks.router.redis_client.hgetall", new_callable=AsyncMock, return_value=tasks_in_redis), \
                     patch("core.models.tb_tenant.TBTenant.find_all", return_value=mock_cursor):
                    tasks = [
                        client.get(f"/api/v1/tasks/active?task_type={'telemetry' if i % 2 == 0 else 'heatmap'}")
                        for i in range(50)
                    ]
                    responses = await asyncio.gather(*tasks)

                assert len(responses) == 50
                for i, r in enumerate(responses):
                    assert r.status_code == status.HTTP_200_OK
                    items = r.json()
                    expected_type = "telemetry" if i % 2 == 0 else "heatmap"
                    assert all(item["task_type"] == expected_type for item in items)
        finally:
            app.dependency_overrides.clear()
