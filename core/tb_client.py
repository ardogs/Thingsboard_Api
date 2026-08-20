import httpx
from typing import Optional, List, Dict, Any


class ThingsBoardClient:
    """
    Cliente HTTP asíncrono dinámico para la API REST de ThingsBoard.
    Se inicializa dinámicamente con la base_url y credenciales de cualquier instancia ThingsBoard registrada.
    """
    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 30.0
    ):
        if not base_url:
            raise ValueError("base_url es obligatorio para inicializar ThingsBoardClient")
        self.base_url = str(base_url).rstrip("/")
        self.token = token
        self.refresh_token = refresh_token
        self.username = username
        self.password = password
        self.timeout = timeout

    def _resolve_token(self, token: Optional[str] = None) -> str:
        resolved = token or self.token
        if not resolved:
            raise ValueError("Se requiere un token JWT válido de ThingsBoard para esta operación")
        return resolved

    async def login(self, username: Optional[str] = None, password: Optional[str] = None) -> dict | None:
        """
        Autentica credenciales contra ThingsBoard y actualiza los tokens de la instancia.
        """
        user = username or self.username
        pwd = password or self.password
        if not user or not pwd:
            raise ValueError("Se requiere usuario y contraseña para iniciar sesión en ThingsBoard")

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/api/auth/login",
                json={"username": user, "password": pwd}
            )
            if response.status_code == 200:
                data = response.json()
                self.token = data.get("token")
                self.refresh_token = data.get("refreshToken")
                return data
            return None

    async def refresh_jwt_token(self, refresh_token: Optional[str] = None) -> dict:
        """
        Renueva el token de acceso mediante el refresh token.
        """
        ref_token = refresh_token or self.refresh_token
        if not ref_token:
            raise ValueError("Se requiere un refreshToken para renovar el token de ThingsBoard")

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/api/auth/token",
                json={"refreshToken": ref_token}
            )
            response.raise_for_status()
            data = response.json()
            if "token" in data:
                self.token = data["token"]
            if "refreshToken" in data:
                self.refresh_token = data["refreshToken"]
            return data

    async def verify_token(self, token: Optional[str] = None) -> bool:
        """
        Valida la vigencia de un JWT consultando /api/auth/user.
        """
        tok = token or self.token
        if not tok:
            return False
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.get(
                    f"{self.base_url}/api/auth/user",
                    headers={"X-Authorization": f"Bearer {tok}"}
                )
                return response.status_code == 200
            except Exception:
                return False

    async def test_connection(self) -> dict:
        """
        Prueba la conectividad hacia la instancia ThingsBoard y valida credenciales si están disponibles.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                # 1. Probar conectividad al endpoint de sistema
                res = await client.get(f"{self.base_url}/api/noauth/activate")
                is_reachable = res.status_code in (200, 400, 404)
            except Exception as e:
                return {"success": False, "reachable": False, "authenticated": False, "error": str(e)}

            authenticated = False
            if self.token:
                authenticated = await self.verify_token(self.token)
            elif self.username and self.password:
                login_data = await self.login()
                authenticated = login_data is not None

            return {
                "success": is_reachable,
                "reachable": is_reachable,
                "authenticated": authenticated,
                "base_url": self.base_url
            }

    async def get_tenant_devices(self, token: Optional[str] = None, limit: int = 100, page: int = 0) -> dict:
        tok = self._resolve_token(token)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/api/tenant/devices",
                headers={"X-Authorization": f"Bearer {tok}"},
                params={"pageSize": limit, "page": page}
            )
            response.raise_for_status()
            return response.json()

    async def get_device_by_id(self, device_id: str, token: Optional[str] = None) -> dict | None:
        tok = self._resolve_token(token)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/api/device/{device_id}",
                headers={"X-Authorization": f"Bearer {tok}"}
            )
            if response.status_code == 200:
                return response.json()
            return None

    async def get_entity_timeseries_keys(self, entity_id: str, entity_type: str = "DEVICE", token: Optional[str] = None) -> list[str]:
        tok = self._resolve_token(token)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/api/plugins/telemetry/{entity_type}/{entity_id}/keys/timeseries",
                headers={"X-Authorization": f"Bearer {tok}"}
            )
            response.raise_for_status()
            return response.json()

    async def get_entity_telemetry(
        self,
        entity_id: str,
        keys: str,
        start_ts: int,
        end_ts: int,
        entity_type: str = "DEVICE",
        token: Optional[str] = None,
        limit: int = 100
    ) -> dict:
        tok = self._resolve_token(token)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/api/plugins/telemetry/{entity_type}/{entity_id}/values/timeseries",
                headers={"X-Authorization": f"Bearer {tok}"},
                params={
                    "keys": keys,
                    "startTs": start_ts,
                    "endTs": end_ts,
                    "limit": limit,
                    "orderBy": "ASC"
                }
            )
            response.raise_for_status()
            return response.json()
