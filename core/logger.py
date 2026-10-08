import logging
import os
from typing import Optional

DEFAULT_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_FILE = os.path.join(DEFAULT_LOG_DIR, "gateway.log")

_INITIALIZED_LOGGERS = set()


def setup_logger(
    name: str = "tb_gateway",
    log_file: Optional[str] = DEFAULT_LOG_FILE,
    level: int = logging.INFO
) -> logging.Logger:
    """
    Configura y retorna una instancia de logging.Logger con formateador estándar
    y handlers hacia consola y archivo. Evita handlers duplicados.
    """
    logger_instance = logging.getLogger(name)
    logger_instance.setLevel(level)

    if name not in _INITIALIZED_LOGGERS:
        os.makedirs(DEFAULT_LOG_DIR, exist_ok=True)
        formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

        # Stream / Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger_instance.addHandler(console_handler)

        # File Handler si se especificó archivo
        if log_file:
            try:
                file_handler = logging.FileHandler(log_file, encoding="utf-8")
                file_handler.setLevel(level)
                file_handler.setFormatter(formatter)
                logger_instance.addHandler(file_handler)
            except Exception as e:
                logger_instance.warning(f"No se pudo inicializar FileHandler en '{log_file}': {e}")

        logger_instance.propagate = False
        _INITIALIZED_LOGGERS.add(name)

    return logger_instance


def get_logger(name: str = "tb_gateway") -> logging.Logger:
    """
    Factoría modular de loggers. Retorna un logger nombrado específico para cada servicio o módulo.
    Si el logger no ha sido configurado previamente, lo inicializa con los handlers estándar.
    """
    if not name:
        name = "tb_gateway"
    return setup_logger(name=name)


# Instancia base exportada para compatibilidad retroactiva limpia
logger = get_logger("tb_gateway")

