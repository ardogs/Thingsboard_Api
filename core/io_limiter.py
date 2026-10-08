import os
import shutil
import asyncio
from typing import Optional

from core.config import settings
from core.logger import logger


_zip_semaphore: Optional[asyncio.Semaphore] = None
_io_semaphore: Optional[asyncio.Semaphore] = None
_semaphore_loop = None


def get_zip_semaphore() -> asyncio.Semaphore:
    """
    Retorna el semáforo global para controlar la concurrencia máxima de compresión ZIP.
    Garantiza estar vinculado al Event Loop activo actual.
    """
    global _zip_semaphore, _semaphore_loop
    current_loop = asyncio.get_running_loop()
    if _zip_semaphore is None or _semaphore_loop != current_loop:
        max_zip = getattr(settings, "MAX_CONCURRENT_ZIP_PACKAGING", 3)
        _zip_semaphore = asyncio.Semaphore(max_zip)
        _semaphore_loop = current_loop
    return _zip_semaphore


def get_io_semaphore() -> asyncio.Semaphore:
    """
    Retorna el semáforo global para limitar operaciones masivas de E/S en disco.
    """
    global _io_semaphore, _semaphore_loop
    current_loop = asyncio.get_running_loop()
    if _io_semaphore is None or _semaphore_loop != current_loop:
        max_io = getattr(settings, "MAX_CONCURRENT_IO_OPERATIONS", 10)
        _io_semaphore = asyncio.Semaphore(max_io)
        _semaphore_loop = current_loop
    return _io_semaphore


def _sync_make_archive(base_name: str, format: str, root_dir: str, base_dir: Optional[str] = None) -> str:
    """Función síncrona para ser ejecutada en un hilo secundario mediante asyncio.to_thread."""
    return shutil.make_archive(
        base_name=base_name,
        format=format,
        root_dir=root_dir,
        base_dir=base_dir
    )


async def async_create_zip_archive(
    base_name: str,
    root_dir: str,
    base_dir: Optional[str] = None,
    format: str = "zip"
) -> str:
    """
    Crea un archivo comprimido ZIP delegando la ejecución síncrona a un hilo secundario
    (asyncio.to_thread) y regulando la concurrencia con get_zip_semaphore() para prevenir
    saturación de CPU y bus de disco.
    """
    sem = get_zip_semaphore()
    async with sem:
        logger.debug(f"[I/O Limiter] Adquirido slot de compresión ZIP para '{base_name}'")
        archive_path = await asyncio.to_thread(
            _sync_make_archive,
            base_name=base_name,
            format=format,
            root_dir=root_dir,
            base_dir=base_dir
        )
        logger.debug(f"[I/O Limiter] Compresión ZIP finalizada: '{archive_path}'")
        return archive_path


async def async_rmtree(path: str, ignore_errors: bool = True) -> None:
    """
    Elimina de forma recursiva un árbol de directorios delegando a un hilo secundario
    para evitar congelar el Event Loop en purgas de carpetas pesadas.
    """
    if os.path.exists(path):
        await asyncio.to_thread(shutil.rmtree, path, ignore_errors)


def _sync_remove(path: str, ignore_errors: bool) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as e:
        if not ignore_errors:
            raise e


async def async_remove_file(path: str, ignore_errors: bool = True) -> None:
    """
    Elimina un archivo del sistema de archivos local de forma no bloqueante.
    """
    await asyncio.to_thread(_sync_remove, path, ignore_errors)


def _sync_replace(src: str, dst: str) -> None:
    if os.path.exists(dst):
        try:
            os.remove(dst)
        except Exception:
            pass
    os.replace(src, dst)


async def async_replace_file(src: str, dst: str) -> None:
    """
    Renombra o mueve atómicamente un archivo en disco en un hilo secundario.
    """
    await asyncio.to_thread(_sync_replace, src, dst)
