"""
Módulo de arranque e inicialización (Bootstrap) para ThingsBoard Super API Gateway.
Garantiza la idempotencia absoluta en la creación del superadministrador inicial,
elimina reglas residuales/huérfanas en PyCasbin y asegura la inyección segura de
políticas base (p) y agrupamiento (g) asociadas estrictamente al ObjectId del usuario.
"""

from typing import List, Tuple
from core.config import settings
from core.models.user import User
from core.security import get_password_hash
from core.casbin_enforcer import get_casbin_enforcer
from core.logger import logger


async def bootstrap_superadmin():
    """
    Script de arranque idempotente ejecutado durante el lifespan de FastAPI.
    
    1. Idempotencia en la Creación del Superadmin:
       - Busca si ya existe un usuario con username == settings.FIRST_SUPERUSER_USERNAME.
       - Si existe, recupera su ObjectId sin crear duplicados ni sobrescribir.
       - Si no existe, lo crea a partir de las credenciales del entorno.

    2. Limpieza de Basura Residual (Orphan Rules):
       - Elimina cualquier regla 'g' huérfana donde v0 == 'superadmin' en MongoDB y memoria.
       - Garantiza que v0 en reglas 'g' sea siempre un ObjectId real en formato string.

    3. Inyección Segura de Políticas Base y Agrupamiento:
       - Registra políticas base p (superadmin, tenant_admin, operator, viewer) de forma segura.
       - Asocia el rol superadmin estrictamente al ID real (str(super_user.id)) en dominio '*'.
    """
    try:
        # ======================================================================
        # 1. Idempotencia en la Creación del Superadmin
        # ======================================================================
        super_user = await User.find_one(User.username == settings.FIRST_SUPERUSER_USERNAME)
        if super_user:
            logger.info(
                f"[Bootstrap] Usuario superadmin '{super_user.username}' ya existe con ID: {super_user.id}."
            )
        else:
            # Creación inicial segura con credenciales de entorno
            super_user = User(
                username=settings.FIRST_SUPERUSER_USERNAME,
                email=settings.FIRST_SUPERUSER_EMAIL,
                hashed_password=get_password_hash(settings.FIRST_SUPERUSER_PASSWORD),
                role="superadmin",
                is_active=True,
                is_superuser=True
            )
            await super_user.insert()
            logger.info(
                f"[Bootstrap] Usuario superadmin inicial '{super_user.username}' creado con ID: {super_user.id}."
            )

        # ======================================================================
        # 2. Configuración y Limpieza de Casbin (IAM)
        # ======================================================================
        try:
            enforcer = get_casbin_enforcer()

            # 2.1 Limpieza de Basura Residual (Orphan Rules)
            # Eliminar documentos de la colección de Casbin donde ptype == "g" y v0 == "superadmin"
            adapter = enforcer.adapter
            if hasattr(adapter, "_collection") and adapter._collection is not None:
                delete_result = await adapter._collection.delete_many({
                    "ptype": "g",
                    "v0": "superadmin"
                })
                if delete_result.deleted_count > 0:
                    logger.info(
                        f"[Bootstrap] Se eliminaron {delete_result.deleted_count} reglas 'g' huérfanas residuales (v0='superadmin') de MongoDB."
                    )

            # Limpiar de la memoria del enforcer cualquier regla residual con v0 == "superadmin"
            await enforcer.remove_filtered_grouping_policy(0, "superadmin")

            # 2.2 Inyección Segura de Políticas Base ('p')
            # add_policy verifica la existencia previa y evita inserciones duplicadas (retorna False si ya existe)
            base_policies: List[Tuple[str, str, str, str]] = [
                ("superadmin", "*", "*", "*"),
                ("tenant_admin", "*", "*", "*"),
                ("operator", "*", "telemetry", "read"),
                ("operator", "*", "devices", "read"),
                ("viewer", "*", "telemetry", "read"),
            ]

            for sub, dom, obj, act in base_policies:
                await enforcer.add_policy(sub, dom, obj, act)

            # 2.3 Vinculación Segura del Superadmin ('g') usando estrictamente su ObjectId real
            superadmin_id_str = str(super_user.id)
            await enforcer.add_grouping_policy(superadmin_id_str, "superadmin", "*")

            logger.info(
                f"[Bootstrap] Políticas raíz y vinculación de rol 'superadmin' para ID '{superadmin_id_str}' sincronizadas exitosamente."
            )

        except Exception as ce:
            logger.warning(f"[Bootstrap] No se pudieron sincronizar políticas de Casbin: {ce}")

    except Exception as e:
        logger.error(f"[Bootstrap] Error crítico durante el arranque de superadmin: {e}")
