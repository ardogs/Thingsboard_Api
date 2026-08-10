import httpx

class ThingsBoardClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    async def login(self, username: str, password: str) -> str | None:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/auth/login",
                json={"username": username, "password": password}
            )
            if response.status_code == 200:
                return response.json().get("token")
            return None