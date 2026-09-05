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
        if isinstance(timeout, (int, float)):
            self.timeout = httpx.Timeout(timeout=timeout if timeout > 30.0 else 120.0, connect=30.0, read=120.0, write=30.0, pool=30.0)
        else:
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
            try:
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
            except Exception:
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
        Proporciona diagnósticos claros para fallos de DNS, timeouts o errores de autenticación.
        """
        async with httpx.AsyncClient(timeout=self.timeout, verify=False) as client:
            try:
                res = await client.get(f"{self.base_url}/api/noauth/activate")
                is_reachable = res.status_code in (200, 400, 404)
            except httpx.ConnectError as e:
                err_str = str(e)
                if "Name or service not known" in err_str or "getaddrinfo failed" in err_str or "[Errno -2]" in err_str:
                    clean_msg = f"No se pudo resolver el nombre de host o dominio en '{self.base_url}'. Verifica que la URL no tenga errores tipográficos y que el dominio exista en DNS."
                else:
                    clean_msg = f"Fallo al conectar con el servidor '{self.base_url}': {err_str}"
                return {
                    "success": False,
                    "reachable": False,
                    "authenticated": False,
                    "base_url": self.base_url,
                    "error": clean_msg
                }
            except httpx.TimeoutException:
                return {
                    "success": False,
                    "reachable": False,
                    "authenticated": False,
                    "base_url": self.base_url,
                    "error": f"Tiempo de espera agotado al conectar a '{self.base_url}'. El servidor ThingsBoard no responde o está bloqueado por firewall."
                }
            except Exception as e:
                return {
                    "success": False,
                    "reachable": False,
                    "authenticated": False,
                    "base_url": self.base_url,
                    "error": str(e)
                }

            authenticated = False
            auth_error = None
            if self.token:
                authenticated = await self.verify_token(self.token)

            # Si el token no es válido o expiró, intentar login con credenciales si están presentes
            if not authenticated and self.username and self.password:
                try:
                    login_data = await self.login()
                    authenticated = login_data is not None
                    if not authenticated:
                        auth_error = "Credenciales de usuario o contraseña incorrectas en ThingsBoard"
                except Exception as auth_exc:
                    authenticated = False
                    auth_error = f"Error al autenticar: {auth_exc}"
            elif not authenticated and self.token:
                auth_error = "Token JWT expirado o no válido"

            result = {
                "success": is_reachable and (authenticated or (not self.token and not self.username)),
                "reachable": is_reachable,
                "authenticated": authenticated,
                "base_url": self.base_url
            }
            if auth_error:
                result["auth_error"] = auth_error
            return result

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

    async def create_device(
        self,
        device_payload: dict,
        token: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None
    ) -> dict:
        """
        Crea o actualiza un dispositivo en ThingsBoard mediante POST /api/device.
        Soporta reutilización de cliente HTTPX para operaciones masivas por lotes.
        """
        tok = self._resolve_token(token)
        headers = {"X-Authorization": f"Bearer {tok}"}
        url = f"{self.base_url}/api/device"
        if client is not None:
            response = await client.post(url, headers=headers, json=device_payload)
            response.raise_for_status()
            return response.json()
        else:
            async with httpx.AsyncClient(timeout=self.timeout) as ac:
                response = await ac.post(url, headers=headers, json=device_payload)
                response.raise_for_status()
                return response.json()

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

    async def find_entities_by_query(
        self,
        query: dict,
        token: Optional[str] = None
    ) -> dict:
        """
        Ejecuta una consulta avanzada contra el motor de Entity Query de ThingsBoard (/api/entitiesQuery/find).
        Permite recuperar entidades y relaciones en una sola llamada de red sin N+1.
        """
        tok = self._resolve_token(token)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/api/entitiesQuery/find",
                headers={"X-Authorization": f"Bearer {tok}"},
                json=query
            )
            response.raise_for_status()
            return response.json()

    async def get_tenant_assets(
        self,
        token: Optional[str] = None,
        limit: int = 100,
        page: int = 0
    ) -> dict:
        """
        Obtiene los activos (Assets/Sitios) registrados para el tenant en ThingsBoard (/api/tenant/assets).
        """
        tok = self._resolve_token(token)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/api/tenant/assets",
                headers={"X-Authorization": f"Bearer {tok}"},
                params={"pageSize": limit, "page": page}
            )
            response.raise_for_status()
            return response.json()

    async def get_entity_relations(
        self,
        from_id: str,
        from_type: str = "ASSET",
        token: Optional[str] = None
    ) -> list[dict]:
        """
        Obtiene las relaciones salientes de una entidad en ThingsBoard (/api/relations/info).
        """
        tok = self._resolve_token(token)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.get(
                    f"{self.base_url}/api/relations/info",
                    headers={"X-Authorization": f"Bearer {tok}"},
                    params={"fromId": from_id, "fromType": from_type}
                )
                if response.status_code == 200:
                    return response.json()
                return []
            except Exception:
                return []

    async def get_system_info(self, token: Optional[str] = None) -> dict:
        """
        Obtiene la información del sistema (CPU, RAM, Disco, JVM, etc.) mediante GET /api/admin/systemInfo.
        Requiere privilegios de Sysadmin en ThingsBoard.
        """
        tok = self._resolve_token(token)
        async with httpx.AsyncClient(timeout=self.timeout, verify=False) as client:
            response = await client.get(
                f"{self.base_url}/api/admin/systemInfo",
                headers={"X-Authorization": f"Bearer {tok}"}
            )
            response.raise_for_status()
            return response.json()




