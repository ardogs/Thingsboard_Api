from fastapi import FastAPI
from api.endpoints import auth

app = FastAPI(title="ThingsBoard Super API", version="1.0.0")

app.include_router(auth.router, prefix="/api/auth", tags=["Autenticación"])

@app.get("/")
async def root():
    return {"status": "ok", "message": "ThingsBoard Super API activa"}