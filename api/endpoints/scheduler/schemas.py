from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator, model_validator
from croniter import croniter

INCREMENTAL_BACKUP_TASKS = {
    "tasks.execute_incremental_tenant_backup",
    "execute_incremental_tenant_backup_task",
    "execute_incremental_tenant_backup"
}


def validate_cron_expression(v: Optional[str]) -> Optional[str]:
    """Valida que la expresión cron tenga un formato estándar válido de 5 campos interpretables por croniter."""
    if v is not None:
        clean_v = v.strip()
        if not croniter.is_valid(clean_v):
            raise ValueError(
                "Expresión cron inválida. Debe contener un formato estándar válido de 5 campos (ej: '0 8 * * *', '*/15 * * * *')."
            )
        return clean_v
    return v


class ScheduledTaskCreate(BaseModel):
    """Esquema de entrada para crear una nueva tarea programada con soporte completo de payloads específicos."""
    name: str = Field(..., description="Nombre descriptivo de la tarea programada", min_length=3, max_length=100)
    task_name: str = Field(..., description="Nombre canónico de la función registrada en ARQ (ej: 'tasks.cleanup_old_backups', 'tasks.execute_incremental_tenant_backup')", min_length=3, max_length=100)
    cron_expression: str = Field(..., description="Expresión cron estándar de 5 campos (ej: '0 3 * * *', '0 2 1 * *')")
    tenant_id: Optional[str] = Field(default=None, description="Identificador único (ObjectId) del Tenant en MongoDB. Estrictamente obligatorio para tareas por cliente ('tasks.execute_incremental_tenant_backup', 'tasks.download_telemetry', 'tasks.generate_excel_report').")
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Diccionario de parámetros específicos (kwargs) inyectados a la función de fondo en ARQ.\n\n"
            "### 📋 Parámetros Especiales por Tarea:\n\n"
            "#### 1. `tasks.cleanup_old_backups` (Purga de Respaldos)\n"
            "- **`days_to_keep`** (*int*, opcional, default: `30`): Días de antigüedad máxima de retención. Se purgan los archivos ZIP y registros en `TBBackup` anteriores a este umbral y directorios temporales zombis (`tmp_*`) con más de 24h de inactividad.\n\n"
            "#### 2. `tasks.execute_incremental_tenant_backup` (Respaldo Incremental Tenant)\n"
            "- **`concurrency_limit`** (*int*, opcional, default: `4`): Límite de concurrencia simultánea en descarga de llaves de telemetría.\n"
            "- **`page_limit`** (*int*, opcional, default: `2000`): Tamaño de lote por consulta REST a ThingsBoard.\n"
            "- **`year`** (*int*, opcional): Año específico a respaldar (ej: `2026`). Si se omite, calcula de forma autónoma el mes cerrado anterior.\n"
            "- **`month`** (*int*, opcional): Mes específico (1 a 12) a respaldar. Si se omite, calcula de forma autónoma el mes cerrado anterior.\n"
            "- **`entity_type`** (*str*, opcional, default: `'DEVICE'`): Tipo de entidad ThingsBoard (`'DEVICE'` o `'ASSET'`).\n"
            "- **`base_storage_dir`** (*str*, opcional, default: `'tenant_backups'`): Directorio raíz del Data Lake persistente.\n"
            "- **`start_ts`** / **`end_ts`** (*int*, opcional): Marcas de tiempo en milisegundos si se desea delimitar un rango exacto manual.\n\n"
            "#### 3. `tasks.collect_servers_system_info` (Métricas de Sistema)\n"
            "- **`server_id`** (*str*, opcional, default: `null`): ID en MongoDB del servidor ThingsBoard específico a inspeccionar. Si se omite o es `null`, recolecta métricas de hardware (CPU, RAM, Disco) de todos los servidores registrados.\n\n"
            "#### 4. `tasks.download_telemetry` (Descarga Masiva en ZIP)\n"
            "- **`start_date`** (*str*, ISO-8601): Fecha inicial del rango (ej: `'2026-08-01T00:00:00'`).\n"
            "- **`end_date`** (*str*, ISO-8601): Fecha final del rango (ej: `'2026-08-31T23:59:59'`).\n"
            "- **`time_zone`** (*str*, opcional, default: `'America/Mexico_City'`): Zona horaria para interpretar el rango de marcas de tiempo.\n"
            "- **`entity_type`** (*str*, opcional, default: `'DEVICE'`): Tipo de entidad ThingsBoard (`'DEVICE'` o `'ASSET'`).\n"
            "- **`entity_id`** (*str* o *list[str]*, opcional, default: `null`): UUID o lista de UUIDs de dispositivos puntuales. Dejar en `null` para abarcar todos los dispositivos del Tenant.\n"
            "- **`concurrency_limit`** (*int*, opcional, default: `3`): Concurrencia de descarga simultánea.\n"
            "- **`page_limit`** (*int*, opcional, default: `2000`): Tamaño de página por llamada a la API de ThingsBoard.\n"
            "- **`force_reload`** (*bool*, opcional, default: `false`): Si es `true`, descarta checkpoints en Redis e inicia la extracción desde cero.\n\n"
            "#### 5. `tasks.generate_excel_report` (Reporte de Telemetría en Excel)\n"
            "- **`combine_in_single_file`** (*bool*, opcional, default: `true`): `true` para consolidar todos los dispositivos en un único libro `.xlsx` con una hoja por dispositivo; `false` para generar múltiples archivos `.xlsx` empaquetados en un archivo `.zip`.\n"
            "- **`year`** (*int*, opcional): Año del reporte mensual (ej: `2026`). Si se omite junto con `month`, calcula el mes cerrado anterior.\n"
            "- **`month`** (*int*, opcional): Mes del reporte (1-12). Si se omite junto con `year`, calcula el mes cerrado anterior.\n"
            "- **`start_date`** / **`end_date`** (*str*, ISO-8601, opcional): Rango exacto si no se utiliza el modo año/mes.\n"
            "- **`time_zone`** (*str*, opcional, default: `'America/Mexico_City'`): Zona horaria de formateo.\n"
            "- **`concurrency_limit`** (*int*, opcional, default: `3`): Concurrencia de extracción.\n"
            "- **`page_limit`** (*int*, opcional, default: `2000`): Límite de página por petición REST."
        )
    )
    is_active: bool = Field(default=True, description="Estado activo o inactivo de la programación")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "Purga Diaria de Respaldos Caducados",
                    "task_name": "tasks.cleanup_old_backups",
                    "cron_expression": "0 3 * * *",
                    "is_active": True,
                    "payload": {
                        "days_to_keep": 30
                    }
                },
                {
                    "name": "Respaldo Incremental Mensual CFE",
                    "task_name": "tasks.execute_incremental_tenant_backup",
                    "cron_expression": "0 2 1 * *",
                    "tenant_id": "64b1f2e3d4c5b6a789012345",
                    "is_active": True,
                    "payload": {
                        "concurrency_limit": 4,
                        "page_limit": 2000,
                        "year": 2026,
                        "month": 8
                    }
                },
                {
                    "name": "Monitoreo de Recursos de Servidores",
                    "task_name": "tasks.collect_servers_system_info",
                    "cron_expression": "*/30 * * * *",
                    "is_active": True,
                    "payload": {
                        "server_id": None
                    }
                },
                {
                    "name": "Descarga Semanal de Telemetría CONAFOR",
                    "task_name": "tasks.download_telemetry",
                    "cron_expression": "0 4 * * 0",
                    "tenant_id": "64b1f2e3d4c5b6a789012345",
                    "is_active": True,
                    "payload": {
                        "start_date": "2026-08-01T00:00:00",
                        "end_date": "2026-08-31T23:59:59",
                        "entity_type": "DEVICE",
                        "entity_id": None,
                        "time_zone": "America/Mexico_City",
                        "concurrency_limit": 3,
                        "page_limit": 2000,
                        "force_reload": False
                    }
                },
                {
                    "name": "Reporte Mensual en Excel (.xlsx)",
                    "task_name": "tasks.generate_excel_report",
                    "cron_expression": "0 5 1 * *",
                    "tenant_id": "64b1f2e3d4c5b6a789012345",
                    "is_active": True,
                    "payload": {
                        "combine_in_single_file": True,
                        "year": 2026,
                        "month": 8,
                        "time_zone": "America/Mexico_City",
                        "concurrency_limit": 3,
                        "page_limit": 2000
                    }
                }
            ]
        }
    }

    @field_validator("cron_expression", mode="after")
    @classmethod
    def check_cron(cls, v: str) -> str:
        return validate_cron_expression(v)

    @model_validator(mode="after")
    def validate_tenant_for_incremental_backup(self) -> "ScheduledTaskCreate":
        """Exige estrictamente que se proporcione el tenant_id si la tarea corresponde al respaldo incremental."""
        if self.task_name in INCREMENTAL_BACKUP_TASKS:
            if not self.tenant_id or not str(self.tenant_id).strip():
                raise ValueError(
                    f"El campo 'tenant_id' es estrictamente obligatorio para la tarea de respaldo incremental ('{self.task_name}')."
                )
        return self


class ScheduledTaskUpdate(BaseModel):
    """Esquema de entrada para actualizar una tarea programada existente (todos los campos opcionales)."""
    name: Optional[str] = Field(default=None, description="Nombre descriptivo de la tarea", min_length=3, max_length=100)
    task_name: Optional[str] = Field(default=None, description="Nombre de la función en ARQ", min_length=3, max_length=100)
    cron_expression: Optional[str] = Field(default=None, description="Nueva expresión cron de 5 campos")
    tenant_id: Optional[str] = Field(default=None, description="Nuevo identificador de Tenant asociado")
    payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Nuevos argumentos (kwargs). Ver la documentación de ScheduledTaskCreate para el detalle de parámetros especiales por tarea."
    )
    is_active: Optional[bool] = Field(default=None, description="Activar o desactivar la tarea")

    @field_validator("cron_expression", mode="after")
    @classmethod
    def check_cron(cls, v: Optional[str]) -> Optional[str]:
        return validate_cron_expression(v)

    @model_validator(mode="after")
    def validate_tenant_on_update(self) -> "ScheduledTaskUpdate":
        """Valida que si se actualiza la tarea a incremental, el tenant_id no esté vacío."""
        if self.task_name in INCREMENTAL_BACKUP_TASKS:
            if self.tenant_id is not None and not str(self.tenant_id).strip():
                raise ValueError(
                    f"El campo 'tenant_id' no puede estar vacío para la tarea de respaldo incremental ('{self.task_name}')."
                )
        return self


class ScheduledTaskResponse(BaseModel):
    """Esquema de salida con la información completa de la tarea programada."""
    id: str = Field(..., description="Identificador único del documento en MongoDB")
    name: str = Field(..., description="Nombre descriptivo de la tarea programada")
    task_name: str = Field(..., description="Nombre de la función registrada en ARQ")
    cron_expression: str = Field(..., description="Expresión cron estándar")
    tenant_id: Optional[str] = Field(default=None, description="ID del Tenant asociado si aplica")
    tenant_name: Optional[str] = Field(default=None, description="Nombre del Tenant asociado si aplica")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Argumentos kwargs asociados")
    is_active: bool = Field(..., description="Flag de estado activo/inactivo")
    next_run_time: datetime = Field(..., description="Próxima fecha y hora de ejecución programada en UTC")
    last_run_status: Optional[str] = Field(default=None, description="Estado de la última ejecución despachada")
    last_run_at: Optional[datetime] = Field(default=None, description="Marca de tiempo UTC del último despacho")
    created_at: datetime = Field(..., description="Fecha de creación")
    updated_at: Optional[datetime] = Field(default=None, description="Fecha de última actualización")


class ScheduledTaskTriggerResponse(BaseModel):
    """Esquema de respuesta para disparos manuales inmediatos en ARQ."""
    task_id: str = Field(..., description="ID de la tarea programada en MongoDB")
    job_id: str = Field(..., description="UUID del trabajo encolado en ARQ")
    status: str = Field(..., description="Estado del despacho manual")
    message: str = Field(..., description="Mensaje explicativo del despacho")
    dispatched_at: datetime = Field(..., description="Marca de tiempo del despacho en UTC")


class TaskParameterInfo(BaseModel):
    """Información de un parámetro soportado en el payload de la tarea."""
    name: str = Field(..., description="Nombre de la propiedad en el diccionario payload")
    type: str = Field(..., description="Tipo de dato esperado (ej: 'integer', 'string', 'boolean', 'list')")
    description: str = Field(..., description="Descripción del propósito del parámetro")
    required: bool = Field(default=False, description="Indica si el parámetro es obligatorio dentro del payload")
    default: Optional[Any] = Field(default=None, description="Valor sugerido por defecto")


class AvailableTaskResponse(BaseModel):
    """Esquema descriptivo de salida para las tareas disponibles para programación en ARQ."""
    task_name: str = Field(..., description="Nombre canónico registrado en ARQ para el campo task_name")
    display_name: str = Field(..., description="Nombre amigable y legible para mostrar en la interfaz de usuario")
    description: str = Field(..., description="Descripción detallada de la operación que realiza la tarea")
    category: str = Field(..., description="Categoría temática de la tarea (Mantenimiento, Respaldos, Monitoreo, Telemetría, Reportes)")
    requires_tenant: bool = Field(..., description="Indica si la tarea exige obligatoriamente un tenant_id")
    default_cron: str = Field(..., description="Expresión cron de 5 campos recomendada por defecto")
    default_payload: Dict[str, Any] = Field(default_factory=dict, description="Plantilla o ejemplo de payload sugerido")
    parameters: list[TaskParameterInfo] = Field(default_factory=list, description="Lista de parámetros soportados en el payload")
    aliases: list[str] = Field(default_factory=list, description="Nombres o alias alternativos reconocidos por el worker de ARQ")


AVAILABLE_TASKS_CATALOG: list[AvailableTaskResponse] = [
    AvailableTaskResponse(
        task_name="tasks.cleanup_old_backups",
        display_name="Purga y Limpieza de Respaldos",
        description="Elimina del disco los archivos ZIP de respaldos caducados según el tiempo de retención configurado y purga carpetas temporales huérfanas/zombis (tmp_*) inactivas durante más de 24 horas.",
        category="Mantenimiento",
        requires_tenant=False,
        default_cron="0 3 * * *",
        default_payload={"days_to_keep": 30},
        parameters=[
            TaskParameterInfo(
                name="days_to_keep",
                type="integer",
                description="Días de antigüedad máxima para conservar respaldos antes de eliminarlos del disco y de la base de datos.",
                required=False,
                default=30
            )
        ],
        aliases=["cleanup_old_backups_task", "cleanup_old_backups"]
    ),
    AvailableTaskResponse(
        task_name="tasks.execute_incremental_tenant_backup",
        display_name="Respaldo Incremental por Tenant (Mes Vencido)",
        description="Ejecuta la descarga y sincronización incremental de telemetría del mes cerrado anterior hacia el Data Lake local (en streaming JSON) para un Tenant específico.",
        category="Respaldos",
        requires_tenant=True,
        default_cron="0 2 1 * *",
        default_payload={
            "concurrency_limit": 4,
            "page_limit": 2000,
            "entity_type": "DEVICE"
        },
        parameters=[
            TaskParameterInfo(
                name="concurrency_limit",
                type="integer",
                description="Cantidad máxima de descargas simultáneas concurrentes de llaves de telemetría.",
                required=False,
                default=4
            ),
            TaskParameterInfo(
                name="page_limit",
                type="integer",
                description="Tamaño del lote/página por consulta REST a ThingsBoard.",
                required=False,
                default=2000
            ),
            TaskParameterInfo(
                name="year",
                type="integer",
                description="Año específico a respaldar (opcional; si se omite junto con 'month', calcula automáticamente el mes cerrado anterior).",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="month",
                type="integer",
                description="Mes específico (1-12) a respaldar (opcional; si se omite junto con 'year', calcula automáticamente el mes cerrado anterior).",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="entity_type",
                type="string",
                description="Tipo de entidad en ThingsBoard ('DEVICE' o 'ASSET').",
                required=False,
                default="DEVICE"
            ),
            TaskParameterInfo(
                name="base_storage_dir",
                type="string",
                description="Directorio raíz del Data Lake persistente en disco.",
                required=False,
                default="tenant_backups"
            ),
            TaskParameterInfo(
                name="start_ts",
                type="integer",
                description="Marca de tiempo UNIX en milisegundos de inicio si se delimita un rango temporal manual.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="end_ts",
                type="integer",
                description="Marca de tiempo UNIX en milisegundos de fin si se delimita un rango temporal manual.",
                required=False,
                default=None
            )
        ],
        aliases=["execute_incremental_tenant_backup_task", "execute_incremental_tenant_backup"]
    ),
    AvailableTaskResponse(
        task_name="tasks.collect_servers_system_info",
        display_name="Recolección de Métricas de Servidores (CPU, RAM, Disco)",
        description="Consulta periódicamente la API de ThingsBoard (/api/admin/systemInfo) para registrar el estado de salud y consumo de recursos de hardware en el catálogo de servidores.",
        category="Monitoreo",
        requires_tenant=False,
        default_cron="*/30 * * * *",
        default_payload={"server_id": None},
        parameters=[
            TaskParameterInfo(
                name="server_id",
                type="string",
                description="ID del servidor en MongoDB para limitar la recolección a una instancia específica (dejar en null para todos los servidores registrados).",
                required=False,
                default=None
            )
        ],
        aliases=["collect_servers_system_info_task", "collect_servers_system_info"]
    ),
    AvailableTaskResponse(
        task_name="tasks.download_telemetry",
        display_name="Descarga Masiva de Telemetría a ZIP",
        description="Orquesta la descarga histórica completa de telemetría para los dispositivos de un Tenant en un rango de fechas delimitado y genera un archivo ZIP descargable.",
        category="Telemetría",
        requires_tenant=True,
        default_cron="0 4 * * 0",
        default_payload={
            "time_zone": "America/Mexico_City",
            "concurrency_limit": 3,
            "page_limit": 2000,
            "entity_type": "DEVICE",
            "force_reload": False
        },
        parameters=[
            TaskParameterInfo(
                name="start_date",
                type="string",
                description="Fecha inicial en formato ISO (ej: '2026-08-01T00:00:00').",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="end_date",
                type="string",
                description="Fecha final en formato ISO (ej: '2026-08-31T23:59:59').",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="entity_type",
                type="string",
                description="Tipo de entidad en ThingsBoard ('DEVICE' o 'ASSET').",
                required=False,
                default="DEVICE"
            ),
            TaskParameterInfo(
                name="entity_id",
                type="string",
                description="UUID o lista de UUIDs de dispositivos específicos (dejar en blanco para todos los dispositivos del Tenant).",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="time_zone",
                type="string",
                description="Zona horaria para delimitar las marcas de tiempo (ej: 'America/Mexico_City', 'UTC').",
                required=False,
                default="America/Mexico_City"
            ),
            TaskParameterInfo(
                name="concurrency_limit",
                type="integer",
                description="Cantidad máxima de descargas simultáneas concurrentes.",
                required=False,
                default=3
            ),
            TaskParameterInfo(
                name="page_limit",
                type="integer",
                description="Tamaño del lote/página por consulta REST a ThingsBoard.",
                required=False,
                default=2000
            ),
            TaskParameterInfo(
                name="force_reload",
                type="boolean",
                description="Si es True, descarta checkpoints previos en Redis e inicia la descarga desde cero.",
                required=False,
                default=False
            )
        ],
        aliases=["download_telemetry_task", "download_telemetry"]
    ),
    AvailableTaskResponse(
        task_name="tasks.generate_excel_report",
        display_name="Generación de Reportes de Telemetría en Excel (.xlsx)",
        description="Genera hojas de cálculo Excel (.xlsx) estructuradas con la telemetría histórica de los dispositivos de un Tenant según las llaves configuradas en su lista blanca.",
        category="Reportes",
        requires_tenant=True,
        default_cron="0 5 1 * *",
        default_payload={
            "combine_in_single_file": True,
            "time_zone": "America/Mexico_City",
            "concurrency_limit": 3,
            "page_limit": 2000
        },
        parameters=[
            TaskParameterInfo(
                name="combine_in_single_file",
                type="boolean",
                description="Consolidar todos los dispositivos en un único libro Excel con múltiples hojas (True) o generar archivos individuales empaquetados en ZIP (False).",
                required=False,
                default=True
            ),
            TaskParameterInfo(
                name="year",
                type="integer",
                description="Año del reporte (1970-3000). Si se omite junto con month, calcula automáticamente el mes cerrado anterior.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="month",
                type="integer",
                description="Mes del reporte de 1 a 12. Si se omite junto con year, calcula automáticamente el mes cerrado anterior.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="start_date",
                type="string",
                description="Fecha inicial exacta en formato ISO si no se utiliza año/mes (ej: '2026-08-01T00:00:00').",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="end_date",
                type="string",
                description="Fecha final exacta en formato ISO si no se utiliza año/mes (ej: '2026-08-31T23:59:59').",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="time_zone",
                type="string",
                description="Zona horaria para formatear marcas de tiempo en las celdas de Excel.",
                required=False,
                default="America/Mexico_City"
            ),
            TaskParameterInfo(
                name="entity_type",
                type="string",
                description="Tipo de entidad ThingsBoard ('DEVICE' o 'ASSET').",
                required=False,
                default="DEVICE"
            ),
            TaskParameterInfo(
                name="entity_id",
                type="string",
                description="UUID o lista de UUIDs de dispositivos específicos para limitar el reporte.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="concurrency_limit",
                type="integer",
                description="Concurrencia máxima de extracción simultánea de telemetría.",
                required=False,
                default=3
            ),
            TaskParameterInfo(
                name="page_limit",
                type="integer",
                description="Tamaño de lote por consulta REST a ThingsBoard.",
                required=False,
                default=2000
            ),
            TaskParameterInfo(
                name="force_reload",
                type="boolean",
                description="Si es True, ignora checkpoints y extrae desde cero.",
                required=False,
                default=False
            )
        ],
        aliases=["generate_excel_report_task", "generate_excel_report"]
    ),
    AvailableTaskResponse(
        task_name="tasks.generate_monthly_heatmap",
        display_name="Generación Mensual de Mapa de Calor en PDF (Heatmap)",
        description="Genera reportes de cumplimiento y telemetría mensual en PDF con celdas cuadradas y semaforización mediante reglas de negocio seguras, evaluando dispositivos activos con heatmap_active=True.",
        category="Reportes",
        requires_tenant=True,
        default_cron="0 6 1 * *",
        default_payload={
            "year": None,
            "month": None,
            "time_zone": "America/Mexico_City"
        },
        parameters=[
            TaskParameterInfo(
                name="year",
                type="integer",
                description="Año del reporte (1970-3000). Si se omite junto con month, calcula automáticamente el mes cerrado anterior.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="month",
                type="integer",
                description="Mes del reporte de 1 a 12. Si se omite junto con year, calcula automáticamente el mes cerrado anterior.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="time_zone",
                type="string",
                description="Zona horaria para agrupar las marcas temporales por día del mes.",
                required=False,
                default="America/Mexico_City"
            ),
            TaskParameterInfo(
                name="heatmap_config",
                type="dict",
                description="Configuración opcional de llaves (whitelist), reglas de color y agregación.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="send_email",
                type="boolean",
                description="Bandera que indica si el envío de correo está activo y debe ser procesado.",
                required=False,
                default=False
            ),
            TaskParameterInfo(
                name="to_email",
                type="string",
                description="Para: Correo o lista de destinatarios (obligatorio si send_email=True).",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="subject",
                type="string",
                description="Asunto personalizado del correo (opcional).",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="cc",
                type="string",
                description="Con copia (CC): Dirección o lista de correos en copia.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="bcc",
                type="string",
                description="Con copia oculta (BCC): Dirección o lista de correos en copia oculta.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="body",
                type="string",
                description="Cuerpo personalizado del correo (texto plano o HTML).",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="from_email",
                type="string",
                description="De*: Remitente opcional para sobreescribir la configuración SMTP cargada.",
                required=False,
                default=None
            )
        ],
        aliases=["generate_monthly_heatmap_task", "generate_monthly_heatmap"]
    ),
    AvailableTaskResponse(
        task_name="tasks.send_email",
        display_name="Envío Asíncrono de Correo Electrónico",
        description="Despacha correos electrónicos SMTP autenticados con soporte para formato HTML, texto plano y archivos adjuntos locales en disco.",
        category="Utilidades",
        requires_tenant=False,
        default_cron="0 8 * * 1-5",
        default_payload={
            "to": ["usuario@empresa.com"],
            "subject": "Notificación Programada del Sistema",
            "body_text": "Este es un mensaje automático programado desde el API Gateway."
        },
        parameters=[
            TaskParameterInfo(
                name="to",
                type="list",
                description="Lista de correos electrónicos de los destinatarios.",
                required=True
            ),
            TaskParameterInfo(
                name="subject",
                type="string",
                description="Asunto del correo electrónico.",
                required=True
            ),
            TaskParameterInfo(
                name="body_html",
                type="string",
                description="Contenido en formato HTML enriquecido del mensaje.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="body_text",
                type="string",
                description="Contenido alternativo en texto plano.",
                required=False,
                default=None
            ),
            TaskParameterInfo(
                name="attachment_paths",
                type="list",
                description="Rutas locales en disco de los archivos adjuntos a incluir.",
                required=False,
                default=[]
            )
        ],
        aliases=["send_email_task", "send_email"]
    )
]

# Diccionario de búsqueda rápida para metadatos enriquecidos de tareas conocidas
_TASK_METADATA_MAP: Dict[str, AvailableTaskResponse] = {}
for _task_meta in AVAILABLE_TASKS_CATALOG:
    _TASK_METADATA_MAP[_task_meta.task_name] = _task_meta
    for _alias in _task_meta.aliases:
        _TASK_METADATA_MAP[_alias] = _task_meta


def get_available_tasks(
    category: Optional[str] = None,
    requires_tenant: Optional[bool] = None
) -> list[AvailableTaskResponse]:
    """
    Retorna dinámicamente el catálogo completo de tareas disponibles inspeccionando
    REGISTERED_FUNCTIONS de ARQ (WorkerSettings).

    Capacidades de Auto-Discovery:
    1. Resuelve la lista de funciones registradas en tiempo real desde el worker.
    2. Si la función cuenta con metadatos enriquecidos en _TASK_METADATA_MAP, los reutiliza.
    3. Si es una tarea nueva añadida al worker sin metadatos explícitos, realiza
       introspección automática (inspect.signature y docstrings) para inferir parámetros,
       requerimiento de Tenant y categoría.
    4. Omite tareas internas del sistema como el cron dispatcher (master_dispatcher_task).
    """
    import inspect
    from workers.arq_settings import REGISTERED_FUNCTIONS

    excluded_internal_tasks = {"tasks.master_dispatcher", "master_dispatcher_task"}
    tasks_by_func: Dict[Any, list[str]] = {}

    # 1. Agrupar nombres y alias por función subyacente
    for item in REGISTERED_FUNCTIONS:
        name = getattr(item, "name", None) or getattr(item, "__name__", None)
        if not name or name in excluded_internal_tasks or name.startswith("_"):
            continue
        func = getattr(item, "coroutine", item)
        if func not in tasks_by_func:
            tasks_by_func[func] = []
        tasks_by_func[func].append(name)

    discovered_catalog: Dict[str, AvailableTaskResponse] = {}

    # 2. Construir la respuesta para cada tarea descubierta
    for func, names in tasks_by_func.items():
        # Determinar el nombre canónico preferido (priorizando el prefijo 'tasks.')
        canonical_name = next((n for n in names if n.startswith("tasks.")), names[0])
        aliases = [n for n in names if n != canonical_name]

        # Verificar si existe metadatos enriquecidos predefinidos
        matched_meta: Optional[AvailableTaskResponse] = None
        for n in names:
            if n in _TASK_METADATA_MAP:
                matched_meta = _TASK_METADATA_MAP[n]
                break

        if matched_meta:
            # Combinar aliases descubiertos
            combined_aliases = sorted(list(set(matched_meta.aliases + aliases)))
            discovered_catalog[canonical_name] = AvailableTaskResponse(
                task_name=canonical_name,
                display_name=matched_meta.display_name,
                description=matched_meta.description,
                category=matched_meta.category,
                requires_tenant=matched_meta.requires_tenant,
                default_cron=matched_meta.default_cron,
                default_payload=matched_meta.default_payload,
                parameters=matched_meta.parameters,
                aliases=combined_aliases
            )
            continue

        # 3. Auto-Discovery por Introspección si la tarea es nueva y no tiene metadatos explícitos
        doc = (func.__doc__ or "").strip()
        first_doc_line = doc.split("\n")[0].strip() if doc else f"Tarea automatizada {canonical_name}"

        params_info: list[TaskParameterInfo] = []
        req_tenant = False

        try:
            sig = inspect.signature(func)
            for p_name, p_param in sig.parameters.items():
                if p_name in ("ctx", "kwargs", "payload"):
                    continue
                if p_name == "tenant_id":
                    req_tenant = True
                    continue

                type_str = "string"
                if p_param.annotation != inspect.Parameter.empty:
                    ann_name = getattr(p_param.annotation, "__name__", str(p_param.annotation)).lower()
                    if "int" in ann_name:
                        type_str = "integer"
                    elif "bool" in ann_name:
                        type_str = "boolean"
                    elif "list" in ann_name:
                        type_str = "list"
                    elif "dict" in ann_name:
                        type_str = "dict"

                is_req = p_param.default == inspect.Parameter.empty
                default_val = None if is_req else p_param.default

                params_info.append(
                    TaskParameterInfo(
                        name=p_name,
                        type=type_str,
                        description=f"Parámetro {p_name} inferido por introspección.",
                        required=is_req,
                        default=default_val
                    )
                )
        except Exception:
            pass

        # Inferencia de categoría
        lower_name = canonical_name.lower()
        if "report" in lower_name or "heatmap" in lower_name or "excel" in lower_name:
            cat = "Reportes"
        elif "backup" in lower_name or "telemetry" in lower_name:
            cat = "Respaldos"
        elif "email" in lower_name or "mail" in lower_name or "clean" in lower_name:
            cat = "Mantenimiento"
        elif "info" in lower_name or "monitor" in lower_name or "system" in lower_name:
            cat = "Monitoreo"
        else:
            cat = "General"

        display_name = canonical_name.replace("tasks.", "").replace("_", " ").title()

        discovered_catalog[canonical_name] = AvailableTaskResponse(
            task_name=canonical_name,
            display_name=display_name,
            description=first_doc_line,
            category=cat,
            requires_tenant=req_tenant,
            default_cron="0 0 * * *",
            default_payload={},
            parameters=params_info,
            aliases=aliases
        )

    tasks = list(discovered_catalog.values())

    if category is not None:
        tasks = [t for t in tasks if t.category.lower() == category.strip().lower()]
    if requires_tenant is not None:
        tasks = [t for t in tasks if t.requires_tenant == requires_tenant]

    return sorted(tasks, key=lambda t: t.task_name)


def get_available_task_names() -> list[str]:
    """
    Retorna dinámicamente la lista única y ordenada de nombres canónicos de tareas
    disponibles registradas en ARQ (WorkerSettings).
    """
    tasks = get_available_tasks()
    return sorted(list({t.task_name for t in tasks}))


