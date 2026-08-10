from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from core.tb_client import ThingsBoardClient
from core.redis_client import redis_client
from core.config import settings

router = APIRouter()

class LoginRequest(BaseModel):
    server_url: str = settings.TB_BASE_URL
    username: str
    password: str

@router.post("/token")
async def get_tb_token(request: LoginRequest):
    # 1. Crear una clave única para Redis (Soporta múltiples servidores y usuarios)
    cache_key = f"tb_token:{request.server_url}:{request.username}"
    
    # 2. Buscar en caché
    cached_token = await redis_client.get(cache_key)
    if cached_token:
        return {"source": "redis_cache", "token": cached_token}
    
    # 3. Si no hay caché, autenticar contra ThingsBoard
    tb = ThingsBoardClient(base_url=request.server_url)
    token = await tb.login(request.username, request.password)
    
    if not token:
        raise HTTPException(status_code=401, detail="Credenciales inválidas en ThingsBoard")
    
    # 4. Guardar en Redis. Los tokens suelen durar 2.5 hrs. Guardamos por 2 horas (7200 segs)
    await redis_client.setex(cache_key, 7200, token)
    
    return {"source": "thingsboard", "token": token}