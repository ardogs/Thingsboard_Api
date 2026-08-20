import logging
import os
from datetime import datetime

def setup_logger(name: str = "telemetry_downloader") -> logging.Logger:
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Crear directorio de logs si no existe
        os.makedirs("logs", exist_ok=True)
        
        # Handler de archivo
        file_handler = logging.FileHandler("logs/telemetry.log", encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        
        # Formato: Fecha - Nivel - Contexto - Mensaje
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        
        # También agregamos a stdout para ver en consola durante desarrollo
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    return logger

logger = setup_logger()
