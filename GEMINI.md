# GEMINI.md - ThingsBoard Super API

## 📌 Descripción General
**ThingsBoard Super API** es una API backend construida con **FastAPI** que actúa como una capa intermedia / wrapper optimizado para la interacción con instancias de **ThingsBoard**. Proporciona autenticación centralizada con caché en **Redis**, cliente HTTP asíncrono para ThingsBoard y arquitectura preparada para el procesamiento de tareas en segundo plano mediante **Celery**.

---

## 🛠️ Tecnología y Stack
- **Framework Web**: [FastAPI](https://fastapi.tiangolo.com/) (Python 3.10+)
- **Servidor ASGI**: [Uvicorn](https://www.uvicorn.org/)
- **Caché y Mensajería**: [Redis](https://redis.io/) (`redis.asyncio`)
- **Cliente HTTP Asíncrono**: [httpx](https://www.python-httpx.org/)
- **Tareas en Segundo Plano**: [Celery](https://docs.celeryq.dev/)
- **Gestión de Configuración**: [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)

---

## 📁 Estructura del Proyecto

```
Thingsboard_Api/
├── api/                   # Aplicación FastAPI y rutas
│   ├── endpoints/         # Controladores de endpoints por dominio
│   │   ├── __init__.py
│   │   └── auth.py        # Endpoints de autenticación y token caching con Redis
│   ├── __init__.py
│   └── main.py            # Punto de entrada de FastAPI
├── core/                  # Módulos centrales y configuraciones
│   ├── __init__.py
│   ├── config.py          # Configuración global mediante Pydantic BaseSettings
│   ├── redis_client.py    # Cliente Redis asíncrono
│   └── tb_client.py       # Cliente HTTP asíncrono para la API REST de ThingsBoard
├── workers/               # Tareas y workers asíncronos (Celery)
├── docker-compose.yml     # Orquestación de servicios (Redis, API, Workers)
├── requirements.txt       # Dependencias Python
└── GEMINI.md              # Guía y contexto del proyecto
```

---

## ⚙️ Configuración de Variables de Entorno (`.env`)

Para ejecutar la aplicación correctamente, crea un archivo `.env` en la raíz del proyecto:

```env
PROJECT_NAME="ThingsBoard Super API"
REDIS_URL="redis://localhost:6379/0"
TB_BASE_URL="https://tu-servidor-thingsboard.com"
```

---

## 🚀 Guía de Inicio Rápido

### 1. Entorno Virtual e Instalación de Dependencias
```bash
# Crear entorno virtual
python -m venv venv

# Activar en Windows (PowerShell)
.\venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Iniciar Redis
Asegúrate de contar con un servidor Redis en ejecución en `localhost:6379` (o la URL configurada en `.env`):
```bash
docker run -d --name redis-tb -p 6379:6379 redis:alpine
```

### 3. Ejecutar el Servidor de Desarrollo
```bash
uvicorn api.main:app --reload
```
- **API Base**: `http://127.0.0.1:8000`
- **Documentación Swagger UI**: `http://127.0.0.1:8000/docs`
- **Documentación ReDoc**: `http://127.0.0.1:8000/redoc`

---

## 🔑 Funcionalidades y Flujos Implementados

### Autenticación y Token Caching (`/api/auth/token`)
- **Método**: `POST /api/auth/token`
- **Estrategia**:
  1. Genera una clave única en Redis: `tb_token:<server_url>:<username>`.
  2. Consulta la caché en Redis. Si el token existe y sigue válido, lo retorna con `"source": "redis_cache"`.
  3. Si no existe en caché, solicita la autenticación a ThingsBoard usando `ThingsBoardClient`.
  4. Guarda el nuevo token JWT en Redis con un TTL de 2 horas (7200 segundos) y lo retorna con `"source": "thingsboard"`.

---

## 📌 Guía de Estilo y Convenciones para Desarrolladores / IA

1. **Programación Asíncrona**: Mantener el uso de `async/await` en endpoints, cliente HTTP (`httpx.AsyncClient`) y cliente Redis (`redis.asyncio`).
2. **Manejo de Errores**: Usar `fastapi.HTTPException` con códigos HTTP estándar (p. ej., `401 Unauthorized`).
3. **Organización de Endpoints**: Nuevas rutas deben residir en `api/endpoints/<dominio>.py` e incluirse en `api/main.py` mediante `APIRouter`.
4. **Configuración Centralizada**: Toda nueva variable de entorno debe agregarse a la clase `Settings` en `core/config.py`.
