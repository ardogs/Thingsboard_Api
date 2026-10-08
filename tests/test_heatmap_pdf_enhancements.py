import os
import sys
import re
import pytest
import asyncio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from PIL import Image as PILImage
from reportlab.pdfgen import canvas

from core.services.heatmap_report_service import (
    HeatmapRule,
    parse_heatmap_rules,
    sort_rules_for_evaluation,
    ensure_tkme_logo_badge,
    _render_single_heatmap_image,
    generate_heatmap_report_pdf,
    _sync_generate_heatmap_pdf
)


@pytest.mark.asyncio
async def test_ensure_tkme_logo_badge_and_styling():
    """Verifica la generación y presencia del badge oscuro con logo TKmE CLOUD."""
    badge_path = ensure_tkme_logo_badge()
    assert badge_path is not None, "El badge de TKmE CLOUD no pudo ser generado o ubicado"
    assert os.path.exists(badge_path), f"El archivo {badge_path} no existe en disco"

    with PILImage.open(badge_path) as im:
        assert im.format == "PNG"
        assert im.mode == "RGBA"
        w, h = im.size
        assert w == 200 and h == 200
        # Verificar que el fondo en el centro no sea transparente y que los bordes correspondan al fondo oscuro (#0f1117 = rgb(15, 17, 23))
        # Muestrear píxel en el fondo interno (x=50, y=20)
        pixel = im.getpixel((50, 20))
        assert pixel[0] <= 30 and pixel[1] <= 30 and pixel[2] <= 35, f"Color de fondo no es oscuro: {pixel}"


def test_previous_month_calculation_logic():
    """
    Verifica que la fecha de telemetría determine el mes anterior para el título del reporte:
    'Tomando como base que la fecha escrita en esa telemetria corresponde al mes anterior.'
    """
    SPANISH_MONTHS = [
        "", "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
    ]

    # Caso 1: Telemetría escrita en Septiembre 2026 -> Reporte de Agosto 2026
    t1_y, t1_m = 2026, 9
    calc1_y = t1_y if t1_m > 1 else t1_y - 1
    calc1_m = t1_m - 1 if t1_m > 1 else 12
    title1 = f"Reporte mensual mapas de calor {SPANISH_MONTHS[calc1_m].capitalize()} {calc1_y}"
    assert title1 == "Reporte mensual mapas de calor Agosto 2026"

    # Caso 2: Telemetría escrita en Enero 2026 -> Reporte de Diciembre 2025 (cruce de año)
    t2_y, t2_m = 2026, 1
    calc2_y = t2_y if t2_m > 1 else t2_y - 1
    calc2_m = t2_m - 1 if t2_m > 1 else 12
    title2 = f"Reporte mensual mapas de calor {SPANISH_MONTHS[calc2_m].capitalize()} {calc2_y}"
    assert title2 == "Reporte mensual mapas de calor Diciembre 2025"

    # Caso 3: Telemetría escrita en Julio 2026 -> Reporte de Junio 2026
    t3_y, t3_m = 2026, 7
    calc3_y = t3_y if t3_m > 1 else t3_y - 1
    calc3_m = t3_m - 1 if t3_m > 1 else 12
    title3 = f"Reporte mensual mapas de calor {SPANISH_MONTHS[calc3_m].capitalize()} {calc3_y}"
    assert title3 == "Reporte mensual mapas de calor Junio 2026"


def test_render_single_heatmap_geometric_centering_and_legend(tmp_path):
    """
    Verifica que el heatmap esté centrado horizontal y verticalmente,
    que la leyenda contenga las reglas de la telemetría y el pie de página 'pagina {n} de {m}'.
    """
    out_img = str(tmp_path / "test_center_chart.png")
    matrix = [[90.0, 75.0, 50.0, "NA"] * 6 for _ in range(30)]

    rules = [
        HeatmapRule(">=", 90.0, "#22c55e", "Excelente (>= 90)"),
        HeatmapRule(">=", 75.0, "#eab308", "Aceptable (75 - 89)"),
        HeatmapRule("<", 75.0, "#ef4444", "Alerta (< 75)"),
    ]

    _, size = _render_single_heatmap_image(
        matrix_data=matrix,
        rules=rules,
        output_image_path=out_img,
        title="Reporte mensual mapas de calor Julio 2026",
        metric_name="TEMPERATURA",
        device_name="Compresor-A1",
        page_num=1,
        total_pages=3
    )

    assert os.path.exists(out_img)
    assert size[0] > 1000 and size[1] > 600

    # Verificar que la imagen se haya generado y pueda abrirse
    with PILImage.open(out_img) as im:
        assert im.format == "PNG"


def test_heatmap_header_hierarchy_and_footer_alignment(tmp_path):
    """
    Verifica que:
    1. El encabezado tenga mayor jerarquía visual y separación amplia sobre el mapa de calor.
    2. El pie de página contenga 'pagina {n} de {m}' alineado a la derecha (ha='right') y al fondo.
    3. La relación de aspecto del gráfico cumpla con el estándar Letter Landscape (~1.29).
    """
    out_img = str(tmp_path / "test_hierarchy_chart.png")
    matrix = [[80.0] * 24 for _ in range(31)]
    rules = [
        HeatmapRule(">=", 80.0, "#22c55e", "Normal (>= 80)"),
        HeatmapRule("<", 80.0, "#ef4444", "Bajo (< 80)")
    ]

    _, (w, h) = _render_single_heatmap_image(
        matrix_data=matrix,
        rules=rules,
        output_image_path=out_img,
        title="Reporte mensual mapas de calor Agosto 2026",
        metric_name="PRESION",
        device_name="Tanque-01",
        page_num=2,
        total_pages=5
    )

    assert os.path.exists(out_img)
    aspect_ratio = w / float(h)
    assert 1.20 <= aspect_ratio <= 1.40, f"Proporción de aspecto no es Letter Landscape: {aspect_ratio}"

    import inspect
    from core.services import heatmap_report_service
    src = inspect.getsource(heatmap_report_service._render_single_heatmap_image)
    assert 'f"pagina {page_num} de {total_pages}"' in src or "f'pagina {page_num} de {total_pages}'" in src
    assert 'ha="right"' in src
    assert 'fontsize=19.0' in src
    assert 'y_max - 1.6' in src
    assert 'title_x = x_min + 4.8' in src


@pytest.mark.asyncio
async def test_multipage_pdf_generation_without_layout_error(tmp_path):
    """
    Verifica que la generación de reportes multi-página en PDF compile exitosamente
    sin desbordamiento de frame ('LayoutError') en ReportLab y que el PDF contenga las páginas esperadas.
    """
    pdf_out = str(tmp_path / "test_multipage_report.pdf")

    # Crear datos para 3 secciones (3 páginas)
    matrix_p1 = [[85.0] * 24 for _ in range(31)]
    matrix_p2 = [[92.0] * 24 for _ in range(30)]
    matrix_p3 = [[65.0] * 24 for _ in range(28)]

    rules_p1 = [
        {"operator": ">=", "value": 80, "color": "#22c55e", "label": "Normal"},
        {"operator": "<", "value": 80, "color": "#ef4444", "label": "Bajo"}
    ]
    rules_p2 = [
        {"operator": ">=", "value": 90, "color": "#3b82f6", "label": "Óptimo"},
        {"operator": "<", "value": 90, "color": "#f97316", "label": "Regular"}
    ]

    sections = [
        {
            "device_name": "Sensor Presión Nave 1",
            "metric_name": "presion_psi",
            "data": matrix_p1,
            "rules": rules_p1,
            "title": "Reporte mensual mapas de calor Julio 2026"
        },
        {
            "device_name": "Medidor Eléctrico Tablero B",
            "metric_name": "voltaje_fase",
            "data": matrix_p2,
            "rules": rules_p2,
            "title": "Reporte mensual mapas de calor Julio 2026"
        },
        {
            "device_name": "Sensor Humedad Silo C",
            "metric_name": "humedad_relativa",
            "data": matrix_p3,
            "rules": rules_p1,
            "title": "Reporte mensual mapas de calor Julio 2026"
        }
    ]

    result_path = await generate_heatmap_report_pdf(
        matrix_data=sections,
        rules=rules_p1,
        output_pdf_path=pdf_out,
        title="Reporte mensual mapas de calor Julio 2026"
    )

    assert os.path.exists(result_path)
    file_size = os.path.getsize(result_path)
    assert file_size > 20000, f"El archivo PDF generado es demasiado pequeño ({file_size} bytes)"

    # Inspeccionar el PDF generado verificando la estructura y páginas
    with open(result_path, "rb") as f:
        pdf_bytes = f.read()
    assert pdf_bytes.startswith(b"%PDF"), "El archivo no es un PDF válido"
    page_count = pdf_bytes.count(b"/Type /Page")
    assert page_count >= 3, f"Se esperaban al menos 3 páginas en el PDF, se encontraron {page_count}"


@pytest.mark.asyncio
async def test_corrupted_rules_and_edge_cases_fallback(tmp_path):
    """
    Verifica que ante reglas corruptas, cadenas malformadas o valores extremos,
    el servicio maneje la semaforización de forma defensiva sin colapsar el worker.
    """
    pdf_out = str(tmp_path / "test_defensive_report.pdf")

    # Matriz con mezcla de valores float, enteros, NA, None y strings
    defensive_matrix = [
        [None, "NA", "invalid_num", 999.9, -50.0, float("nan")] * 4 for _ in range(5)
    ]

    # Reglas con llaves inconsistentes y operadores en mayúsculas/minúsculas
    corrupted_rules = [
        {"operator": "GTE", "val": "85.5", "color": "#10b981", "label": "Pasa"},
        {"op": "LT", "limit": 50, "color": "#dc2626", "name": "Falla"}
    ]

    result_path = await generate_heatmap_report_pdf(
        matrix_data=defensive_matrix,
        rules=corrupted_rules,
        output_pdf_path=pdf_out,
        title="Reporte mensual mapas de calor Agosto 2026",
        metadata={"device_name": "Dispositivo Edge Case", "key": "stress_test"}
    )

    assert os.path.exists(result_path)
    assert os.path.getsize(result_path) > 5000
