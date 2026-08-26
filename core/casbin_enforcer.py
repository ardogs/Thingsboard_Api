import os
from typing import Optional
import casbin
import casbin_motor_adapter

from core.config import settings
from core.logger import logger

_enforcer: Optional[casbin.AsyncEnforcer] = None
_adapter: Optional[casbin_motor_adapter.Adapter] = None


def _resolve_model_path(model_path: Optional[str] = None) -> str:
    path = model_path or settings.CASBIN_MODEL_PATH
    if os.path.isabs(path):
        return path

    # Intentar relativo al directorio actual de ejecución
    if os.path.exists(path):
        return os.path.abspath(path)

    # Intentar relativo al directorio raíz del proyecto
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    candidate = os.path.join(base_dir, path)
    if os.path.exists(candidate):
        return candidate

    # Intentar en el directorio core
    core_candidate = os.path.join(base_dir, "core", os.path.basename(path))
    if os.path.exists(core_candidate):
        return core_candidate

    return os.path.abspath(path)


async def init_casbin_enforcer(
    uri: Optional[str] = None,
    db_name: Optional[str] = None,
    model_path: Optional[str] = None,
    custom_adapter: Optional[casbin_motor_adapter.Adapter] = None
) -> casbin.AsyncEnforcer:
    """
    Inicializa el AsyncEnforcer de Casbin utilizando casbin-motor-adapter contra MongoDB.
    Carga todas las políticas existentes desde la colección de Casbin.
    """
    global _enforcer, _adapter

    resolved_model = _resolve_model_path(model_path)
    if not os.path.exists(resolved_model):
        raise FileNotFoundError(f"Archivo de modelo Casbin no encontrado en: {resolved_model}")

    if custom_adapter is not None:
        _adapter = custom_adapter
    else:
        mongo_uri = uri or settings.MONGO_URI
        database_name = db_name or settings.MONGO_DB_NAME
        _adapter = casbin_motor_adapter.Adapter(
            uri=mongo_uri,
            dbname=database_name,
            collection=settings.CASBIN_COLLECTION_NAME
        )
        # Si existe un cliente Motor activo/inyectado en database.py (ej. mongomock), reutilizarlo
        from core.database import get_mongo_client
        active_client = get_mongo_client()
        if active_client is not None:
            _adapter._client = active_client
            _adapter._db = active_client[database_name]
            _adapter._collection = active_client[database_name][settings.CASBIN_COLLECTION_NAME]

    _enforcer = casbin.AsyncEnforcer(resolved_model, _adapter)
    await _enforcer.load_policy()
    logger.info(f"[Casbin] AsyncEnforcer inicializado exitosamente (Modelo: {resolved_model}).")
    return _enforcer


def get_casbin_enforcer() -> casbin.AsyncEnforcer:
    """
    Retorna la instancia global activa de AsyncEnforcer.
    """
    global _enforcer
    if _enforcer is None:
        raise RuntimeError("Casbin AsyncEnforcer no ha sido inicializado. Llama a init_casbin_enforcer primero.")
    return _enforcer


async def reload_casbin_policy():
    """
    Recarga las políticas de Casbin desde la base de datos de MongoDB.
    """
    global _enforcer
    if _enforcer is not None:
        await _enforcer.load_policy()
        logger.info("[Casbin] Políticas recargadas exitosamente desde MongoDB.")
