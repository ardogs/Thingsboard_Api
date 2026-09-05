import math
from typing import Generic, List, TypeVar
from pydantic import BaseModel, Field

T = TypeVar("T")


class PaginationMetadata(BaseModel):
    """
    Metadatos de paginación estructurados para facilitar la navegación y renderizado en clientes y UI.
    """
    total: int = Field(
        ...,
        ge=0,
        description="Número total de registros disponibles que coinciden con los criterios de búsqueda",
        examples=[58]
    )
    page: int = Field(
        ...,
        ge=1,
        description="Número de la página actual consultada (1-indexed)",
        examples=[1]
    )
    page_size: int = Field(
        ...,
        ge=1,
        description="Cantidad de registros solicitados por página",
        examples=[20]
    )
    total_pages: int = Field(
        ...,
        ge=1,
        description="Número total de páginas calculadas en función del total y page_size",
        examples=[3]
    )
    has_next: bool = Field(
        ...,
        description="Indica si existe al menos una página posterior disponible",
        examples=[True]
    )
    has_prev: bool = Field(
        ...,
        description="Indica si existe al menos una página anterior disponible",
        examples=[False]
    )


class PaginatedResponse(BaseModel, Generic[T]):
    """
    Contenedor estándar y genérico de respuesta paginada con items y metadatos de paginación.
    """
    items: List[T] = Field(
        ...,
        description="Lista de elementos correspondientes a la página actual solicitada"
    )
    pagination: PaginationMetadata = Field(
        ...,
        description="Detalle de metadatos de paginación (total, página actual, total de páginas, etc.)"
    )


def build_pagination_metadata(total: int, page: int, page_size: int) -> PaginationMetadata:
    """
    Calcula de forma centralizada y segura los metadatos de paginación.
    
    :param total: Número total de registros coincidentes en base de datos.
    :param page: Página actual solicitada (1-indexed).
    :param page_size: Cantidad de registros por página.
    :return: Instancia de PaginationMetadata con todos los indicadores calculados.
    """
    total_pages = math.ceil(total / page_size) if total > 0 else 1
    has_next = page < total_pages
    has_prev = 1 < page <= total_pages + 1

    return PaginationMetadata(
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        has_next=has_next,
        has_prev=has_prev
    )
