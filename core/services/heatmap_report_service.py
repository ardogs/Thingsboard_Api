import os
import re
import gc
import json
import asyncio
import operator
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, List, Dict, Any, Tuple, Union

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # Forzar backend headless sin interfaz gráfica
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from PIL import Image as PILImage, ImageDraw

from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import (
    SimpleDocTemplate,
    Image as RLImage,
    PageBreak
)

from core.logger import logger


def ensure_tkme_logo_badge() -> Optional[str]:
    """
    Garantiza la existencia del badge con fondo oscuro y logo TKmE CLOUD para el encabezado.
    Replica exactamente el diseño visual del badge del login (fondo oscuro #0f1117 y borde #1e293b).
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    assets_dir = os.path.join(base_dir, "assets")
    os.makedirs(assets_dir, exist_ok=True)
    badge_path = os.path.join(assets_dir, "tkme_logo_badge.png")

    if os.path.exists(badge_path):
        return badge_path

    candidate_sources = [
        os.path.join(assets_dir, "logo.webp"),
        os.path.abspath(os.path.join(base_dir, "..", "ThingsboardApiGateway-front", "src", "renderer", "src", "assets", "logo.webp")),
        os.path.abspath(os.path.join(base_dir, "..", "ThingsboardApiGateway-front", "resources", "icon.png"))
    ]

    source_found = None
    for src in candidate_sources:
        if os.path.exists(src):
            source_found = src
            break

    if not source_found:
        return None

    try:
        logo = PILImage.open(source_found).convert("RGBA")
        badge_size = 200
        badge = PILImage.new("RGBA", (badge_size, badge_size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(badge)
        draw.rounded_rectangle(
            [(4, 4), (badge_size - 5, badge_size - 5)],
            radius=40,
            fill=(15, 17, 23, 255),
            outline=(30, 41, 59, 255),
            width=4
        )
        target_w = 140
        ratio = target_w / float(logo.width)
        target_h = int(logo.height * ratio)
        logo_resized = logo.resize((target_w, target_h), PILImage.Resampling.LANCZOS)
        pos_x = (badge_size - target_w) // 2
        pos_y = (badge_size - target_h) // 2
        badge.paste(logo_resized, (pos_x, pos_y), logo_resized)
        badge.save(badge_path)
        return badge_path
    except Exception as e:
        logger.warning(f"No se pudo generar badge de logo TKmE: {e}")
        return None


# ==============================================================================
# 1. MAPEO SEGURO DE REGLAS (OPERATOR NATIVO - ESTRICTAMENTE SIN EVAL)
# ==============================================================================

OPERATOR_MAP = {
    ">=": operator.ge,
    "<=": operator.le,
    ">": operator.gt,
    "<": operator.lt,
    "==": operator.eq,
    "=": operator.eq,
    "!=": operator.ne,
    "gte": operator.ge,
    "lte": operator.le,
    "gt": operator.gt,
    "lt": operator.lt,
    "eq": operator.eq,
    "ne": operator.ne,
}

RULE_REGEX = re.compile(
    r"^\s*(>=|<=|>|<|==|!=|=|gte|lte|gt|lt|eq|ne)\s*([+-]?\d+(?:\.\d+)?)"
    r"(?:\s*[:,\->]\s*([#\w]+))?(?:\s*\((.*?)\))?\s*$",
    re.IGNORECASE
)


class HeatmapRule:
    """
    Representa una regla de color para celdas del mapa de calor.
    Mapea operadores matemáticos de forma 100% segura usando el módulo 'operator' nativo de Python.
    Prohíbe absolutamente el uso de eval() o exec().
    """
    def __init__(
        self,
        op_str: str,
        threshold: float,
        color: str,
        label: Optional[str] = None
    ):
        clean_op = op_str.strip().lower()
        if clean_op not in OPERATOR_MAP:
            raise ValueError(
                f"Operador no soportado o inválido: '{op_str}'. "
                f"Operadores permitidos: {list(OPERATOR_MAP.keys())}"
            )
        self.op_str = op_str.strip()
        self.threshold = float(threshold)
        self.color = color.strip()
        self.op_func = OPERATOR_MAP[clean_op]
        self.label = label.strip() if label else f"{self.op_str} {self.threshold}"

    def matches(self, val: Any) -> bool:
        """Evalúa un valor contra el umbral utilizando operator de manera segura."""
        if val is None or pd.isna(val):
            return False
        if str(val).strip().upper() == "NA":
            return False
        try:
            numeric_val = float(val)
            return bool(self.op_func(numeric_val, self.threshold))
        except (ValueError, TypeError):
            return False

    def to_dict(self) -> dict:
        return {
            "operator": self.op_str,
            "threshold": self.threshold,
            "limit": self.threshold,
            "color": self.color,
            "label": self.label
        }


def parse_heatmap_rules(rules: Any) -> List[HeatmapRule]:
    """
    Analizador (parser) seguro de reglas de semaforización para Heatmaps.
    Soporta llaves 'limit', 'threshold', 'value', 'val' (incluso con límite en 0).
    Soporta formato lista de diccionarios, regex strings o reglas por defecto.
    """
    if not rules:
        return [
            HeatmapRule(">=", 90.0, "#22c55e", "Excelente (>= 90)"),
            HeatmapRule(">=", 75.0, "#eab308", "Aceptable (75 - 89)"),
            HeatmapRule("<", 75.0, "#ef4444", "Alerta / Crítico (< 75)"),
        ]

    parsed: List[HeatmapRule] = []
    items: list = []

    if isinstance(rules, dict):
        if "rules" in rules and isinstance(rules["rules"], list):
            items = rules["rules"]
        else:
            items = [rules]
    elif isinstance(rules, list):
        items = rules
    else:
        items = [rules]

    for item in items:
        if isinstance(item, HeatmapRule):
            parsed.append(item)
            continue

        if isinstance(item, dict):
            op = item.get("operator") or item.get("op")
            val = None
            for k in ["limit", "threshold", "value", "val"]:
                if k in item and item[k] is not None:
                    val = item[k]
                    break

            color = item.get("color") or item.get("hex") or "#3b82f6"
            label = item.get("label") or item.get("name") or item.get("description")

            if op is not None and val is not None:
                parsed.append(HeatmapRule(str(op), float(val), str(color), label))
                continue

            cond = item.get("rule") or item.get("condition") or item.get("expr")
            if cond:
                m = RULE_REGEX.match(str(cond))
                if m:
                    op_match = m.group(1)
                    val_match = float(m.group(2))
                    color_match = color if color != "#3b82f6" else (m.group(3) or "#3b82f6")
                    label_match = label or m.group(4) or f"{op_match} {val_match}"
                    parsed.append(HeatmapRule(op_match, val_match, color_match, label_match))
                    continue
                else:
                    raise ValueError(f"Expresión de regla inválida en diccionario: '{cond}'")

        elif isinstance(item, str):
            m = RULE_REGEX.match(item)
            if m:
                op_match = m.group(1)
                val_match = float(m.group(2))
                color_match = m.group(3) or "#3b82f6"
                label_match = m.group(4) or f"{op_match} {val_match}"
                parsed.append(HeatmapRule(op_match, val_match, color_match, label_match))
                continue
            else:
                raise ValueError(f"Formato de regla en texto no reconocido: '{item}'")

    if not parsed:
        return [
            HeatmapRule(">=", 90.0, "#22c55e", "Excelente (>= 90)"),
            HeatmapRule(">=", 75.0, "#eab308", "Aceptable (75 - 89)"),
            HeatmapRule("<", 75.0, "#ef4444", "Alerta / Crítico (< 75)"),
        ]

    return parsed


def sort_rules_for_evaluation(rules: List[HeatmapRule]) -> List[HeatmapRule]:
    """Ordena las reglas de manera óptima para evaluación en cascada."""
    if not rules:
        return []
    # Si todas las reglas son >= o >, evaluar de mayor a menor umbral
    if all(r.op_str in (">=", ">", "gte", "gt") for r in rules):
        return sorted(rules, key=lambda r: r.threshold, reverse=True)
    # Si todas las reglas son <= o <, evaluar de menor a mayor umbral
    if all(r.op_str in ("<=", "<", "lte", "lt") for r in rules):
        return sorted(rules, key=lambda r: r.threshold)
    return rules


# ==============================================================================
# 2. PARSEO Y NORMALIZACIÓN DE MATRICES
# ==============================================================================

def parse_matrix_data(matrix_data: Union[dict, list, pd.DataFrame]) -> pd.DataFrame:
    """Normaliza formatos de matriz JSON a DataFrame de Pandas."""
    if isinstance(matrix_data, pd.DataFrame):
        return matrix_data.copy()

    if isinstance(matrix_data, dict):
        if "columns" in matrix_data and "data" in matrix_data:
            idx = matrix_data.get("index")
            cols = matrix_data.get("columns")
            data = matrix_data.get("data")
            return pd.DataFrame(data=data, index=idx, columns=cols)
        return pd.DataFrame.from_dict(matrix_data, orient="index")

    if isinstance(matrix_data, list):
        if not matrix_data:
            return pd.DataFrame()
        first = matrix_data[0]
        if isinstance(first, dict):
            row_col = next((k for k in ["device", "entity", "site", "row", "name"] if k in first), None)
            col_col = next((k for k in ["date", "day", "column", "time", "ts", "col"] if k in first), None)
            val_col = next((k for k in ["value", "val", "metric", "telemetry"] if k in first), None)
            if row_col and col_col and val_col:
                raw_df = pd.DataFrame(matrix_data)
                return raw_df.pivot(index=row_col, columns=col_col, values=val_col)
        return pd.DataFrame(matrix_data)

    raise ValueError(f"Estructura de matriz no soportada: {type(matrix_data)}")


def _calculate_luminance(hex_color: str) -> float:
    """Calcula la luminancia relativa para asegurar contraste de texto legible."""
    c = hex_color.lstrip("#")
    if len(c) == 3:
        c = "".join([x * 2 for x in c])
    try:
        r = int(c[0:2], 16) / 255.0
        g = int(c[2:4], 16) / 255.0
        b = int(c[4:6], 16) / 255.0
        return 0.299 * r + 0.587 * g + 0.114 * b
    except Exception:
        return 0.5


# ==============================================================================
# 3. RENDERIZADO VISUAL EXACTO (PILL TILES, OPERATOR, Y AISLAMIENTO DE CPU)
# ==============================================================================

def _render_single_heatmap_image(
    matrix_data: list,
    rules: List[HeatmapRule],
    output_image_path: str,
    title: str = "Reporte mensual mapas de calor",
    metric_name: str = "MÉTRICA",
    device_name: Optional[str] = None,
    col_labels: Optional[List[str]] = None,
    row_labels: Optional[List[str]] = None,
    na_color: str = "#5C2D91",
    page_num: int = 1,
    total_pages: int = 1,
    logo_path: Optional[str] = None
) -> Tuple[str, Tuple[int, int]]:
    """
    Genera la imagen PNG del heatmap con celdas redondeadas tipo píldora (Pill Tiles).
    Cumple con los requisitos de diseño para ThingsBoard Super API Gateway:
    - Encabezado: Logo TKmE CLOUD con fondo oscuro (estilo login) a la izquierda, título al lado,
      y DISPOSITIVO | MÉTRICA a la derecha.
    - Mapa de calor: Centrado vertical y horizontalmente en la página con relación Letter Landscape.
    - Leyenda: Ubicada directamente debajo del heatmap, centrada horizontalmente, explicando cada regla de color y NA.
    - Pie de página: Número de página 'pagina {n} de {m}' en la esquina inferior derecha.
    """
    num_rows = len(matrix_data)
    num_cols = len(matrix_data[0]) if num_rows > 0 else 24

    if col_labels is None:
        if num_cols == 24:
            col_labels = [f"{h:02d}:00" for h in range(num_cols)]
        else:
            col_labels = [f"{c+1:02d}" for c in range(num_cols)]

    if row_labels is None:
        row_labels = [f"Día {d+1}" for d in range(num_rows)]

    # Determinar si la primera etiqueta es Día o Dispositivo
    if row_labels and any(str(r).lower().startswith("dispositivo") for r in row_labels):
        corner_label = "DISPOSITIVO"
    else:
        corner_label = "DÍA"

    # Reglas ordenadas para evaluación en cascada
    eval_rules = sort_rules_for_evaluation(rules)

    # Dimensionamiento dinámico para maximizar el tamaño y legibilidad del mapa de calor en Letter Landscape
    desired_grid_w = 34.5
    gap = 0.08 if num_cols <= 24 else 0.06
    cell_w = max(0.85, (desired_grid_w / max(1, num_cols)) - gap)
    cell_h = max(0.55, min(0.70, cell_w * 0.52))
    corner_rad = min(0.14, cell_h * 0.22)

    total_w = num_cols * (cell_w + gap)
    total_h = num_rows * (cell_h + gap)

    # Ancho total ocupado por el grid + etiquetas de fila (las etiquetas van de x = -1.6 a 0)
    grid_left = -1.6
    grid_right = total_w
    grid_center_x = (grid_left + grid_right) / 2.0
    grid_center_y = total_h / 2.0

    # Margen horizontal simétrico a los lados del grid
    side_margin = 1.6
    needed_span_x = (grid_right - grid_left) + (side_margin * 2.0)

    # Espacio vertical necesario para encabezado superior, separación nítida, leyenda y pie de página
    top_space = 3.6
    bottom_space = 2.9
    needed_span_y = total_h + top_space + bottom_space

    # Relación de aspecto estándar Letter Landscape (11 x 8.5)
    page_ratio = 11.0 / 8.5
    target_span_x = max(needed_span_x, needed_span_y * page_ratio)
    target_span_y = target_span_x / page_ratio

    if target_span_y < needed_span_y:
        target_span_y = needed_span_y
        target_span_x = target_span_y * page_ratio

    fig_w = 28.0
    fig_h = fig_w / page_ratio

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=100)
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")

    cell_fontsize = 9.5 if cell_w >= 1.2 else 8.5

    # 1. Dibujar celdas del heatmap
    for r_idx in range(num_rows):
        y = (num_rows - 1 - r_idx) * (cell_h + gap)
        row_vals = matrix_data[r_idx]

        for c_idx in range(num_cols):
            x = c_idx * (cell_w + gap)
            val = row_vals[c_idx] if c_idx < len(row_vals) else "NA"

            if val is None or pd.isna(val) or val == "NA" or str(val).strip().upper() == "NA":
                cell_color = na_color
                cell_text = "NA"
                text_color = "#ffffff"
            else:
                try:
                    num_val = float(val)
                    matched_color = None
                    for rule in eval_rules:
                        if rule.matches(num_val):
                            matched_color = rule.color
                            break

                    cell_color = matched_color or "#e2e8f0"

                    if num_val.is_integer():
                        cell_text = str(int(num_val))
                    elif abs(num_val - round(num_val, 1)) < 0.001:
                        cell_text = f"{num_val:.1f}"
                    else:
                        cell_text = f"{num_val:.2f}"

                    lum = _calculate_luminance(cell_color)
                    text_color = "#ffffff" if lum < 0.52 else "#111827"
                except (ValueError, TypeError):
                    cell_color = na_color
                    cell_text = str(val)
                    text_color = "#ffffff"

            rect = patches.FancyBboxPatch(
                (x, y),
                cell_w,
                cell_h,
                boxstyle=f"round,pad=0,rounding_size={corner_rad}",
                facecolor=cell_color,
                edgecolor="#ffffff",
                linewidth=1.2
            )
            ax.add_patch(rect)

            ax.text(
                x + cell_w / 2.0,
                y + cell_h / 2.0,
                cell_text,
                color=text_color,
                fontsize=cell_fontsize,
                fontweight="bold",
                ha="center",
                va="center"
            )

    # Cabecera de columnas y esquina DÍA
    header_y = total_h + 0.38
    ax.text(-0.8, header_y, corner_label, color="#64748b", fontsize=9.5, fontweight="bold", ha="center", va="center")
    for c_idx in range(num_cols):
        x = c_idx * (cell_w + gap) + cell_w / 2.0
        ax.text(x, header_y, str(col_labels[c_idx]), color="#64748b", fontsize=9.0, fontweight="normal", ha="center", va="center")

    # Etiquetas de filas (Día 1 ... Día N)
    for r_idx in range(num_rows):
        y = (num_rows - 1 - r_idx) * (cell_h + gap) + cell_h / 2.0
        ax.text(-0.8, y, str(row_labels[r_idx]), color="#64748b", fontsize=9.0, fontweight="normal", ha="center", va="center")

    # 2. Leyenda con significado de cada color directamente debajo del heatmap
    legend_items = []
    for rule in eval_rules:
        legend_items.append((rule.color, rule.label))
    legend_items.append((na_color, "Sin datos (NA)"))

    item_widths = [1.0 + 0.3 + len(label) * 0.18 + 1.2 for _, label in legend_items]
    total_legend_w = sum(item_widths) - 1.2
    leg_start_x = grid_center_x - (total_legend_w / 2.0)
    legend_y = -1.25

    curr_lx = leg_start_x
    for idx, (l_color, l_label) in enumerate(legend_items):
        swatch = patches.FancyBboxPatch(
            (curr_lx, legend_y - 0.20),
            0.9,
            0.42,
            boxstyle="round,pad=0,rounding_size=0.10",
            facecolor=l_color,
            edgecolor="#ffffff",
            linewidth=1.0
        )
        ax.add_patch(swatch)
        ax.text(curr_lx + 1.1, legend_y, l_label, color="#334155", fontsize=10.0, fontweight="bold", ha="left", va="center")
        curr_lx += item_widths[idx]

    # Límites del lienzo para centrado perfecto en Letter Landscape (11 x 8.5)
    ax.set_xlim(grid_center_x - target_span_x / 2.0, grid_center_x + target_span_x / 2.0)
    ax.set_ylim(grid_center_y - target_span_y / 2.0, grid_center_y + target_span_y / 2.0)

    x_min, x_max = ax.get_xlim()
    y_min, y_max = ax.get_ylim()

    # 3. Encabezado superior con separación horizontal generosa entre Logo y Título
    y_header = y_max - 1.6
    badge_file = logo_path or ensure_tkme_logo_badge()

    if badge_file and os.path.exists(badge_file):
        try:
            badge_im = PILImage.open(badge_file)
            imagebox = OffsetImage(badge_im, zoom=0.38)
            ab = AnnotationBbox(imagebox, (x_min + 1.8, y_header), frameon=False, box_alignment=(0.0, 0.5))
            ax.add_artist(ab)
            # Separación horizontal evidente entre el logo y el título
            title_x = x_min + 4.8
        except Exception as img_err:
            logger.warning(f"Error renderizando logo badge en heatmap: {img_err}")
            title_x = x_min + 1.8
    else:
        title_x = x_min + 1.8

    # Título con mayor jerarquía visual (fontsize 19.0, bold, #0f172a)
    ax.text(title_x, y_header, title, color="#0f172a", fontsize=19.0, fontweight="bold", ha="left", va="center")

    # Encabezado superior derecho: DISPOSITIVO | MÉTRICA (fontsize 13.5, bold, #334155)
    clean_metric = metric_name if metric_name.upper().startswith("MÉTRICA") else f"MÉTRICA: {metric_name}"
    if device_name and device_name.strip():
        right_header = f"DISPOSITIVO: {device_name.strip()}   |   {clean_metric}"
    else:
        right_header = clean_metric
    ax.text(x_max - 1.8, y_header, right_header, color="#334155", fontsize=13.5, fontweight="bold", ha="right", va="center")

    # Línea divisoria sutil bajo el encabezado
    y_divider = y_header - 0.85
    ax.plot([x_min + 1.8, x_max - 1.8], [y_divider, y_divider], color="#cbd5e1", linewidth=1.3)

    # 4. Pie de página al fondo y a la derecha: 'pagina {n} de {m}'
    footer_y = y_min + 0.7
    ax.text(x_max - 1.8, footer_y, f"pagina {page_num} de {total_pages}", color="#64748b", fontsize=10.0, fontweight="normal", ha="right", va="center")

    ax.axis("off")
    plt.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)
    os.makedirs(os.path.dirname(output_image_path), exist_ok=True)
    plt.savefig(output_image_path, facecolor=fig.get_facecolor(), edgecolor="none", dpi=100, bbox_inches="tight", pad_inches=0.25)

    with PILImage.open(output_image_path) as im:
        img_size = im.size

    plt.close("all")
    del fig
    del ax
    gc.collect()

    return output_image_path, img_size


def _sync_generate_heatmap_image(
    df: pd.DataFrame,
    rules: List[HeatmapRule],
    image_path: str,
    title: str = "Mapa de Calor de Telemetría",
    default_color: str = "#cbd5e1",
    nan_color: str = "#f1f5f9"
) -> Tuple[str, Dict[str, int]]:
    """Adaptador de compatibilidad para llamadas directas de pruebas unitarias."""
    matrix = df.values.tolist()
    row_labels = [str(idx) for idx in df.index]
    col_labels = [str(col) for col in df.columns]
    _render_single_heatmap_image(
        matrix_data=matrix,
        rules=rules,
        output_image_path=image_path,
        title=title,
        col_labels=col_labels,
        row_labels=row_labels
    )
    rule_counts = {r.label: 0 for r in rules}
    rule_counts["Sin datos"] = 0
    return image_path, rule_counts


# ==============================================================================
# 4. COMPILACIÓN DE DOCUMENTO PDF MULTI-PÁGINA (REPORTLAB LANDSCAPE)
# ==============================================================================

def _sync_generate_heatmap_pdf(
    matrix_data: Any,
    rules_data: Any,
    output_pdf_path: str,
    title: str = "Reporte mensual | mapas de calor",
    subtitle: Optional[str] = None,
    metadata: Optional[dict] = None
) -> str:
    """
    Función sincrónica aislada para ejecución en subproceso (ProcessPoolExecutor).
    Genera un PDF Landscape multi-página con un mapa de calor independiente por cada llave/dispositivo.
    """
    os.makedirs(os.path.dirname(output_pdf_path), exist_ok=True)
    meta = metadata or {}
    temp_images_created: List[str] = []

    try:
        # 1. Normalizar entrada hacia una lista de secciones de heatmaps
        heatmap_items: List[dict] = []

        if isinstance(matrix_data, list) and len(matrix_data) > 0 and isinstance(matrix_data[0], dict) and "data" in matrix_data[0]:
            # Ya es una lista estructurada de heatmaps
            heatmap_items = matrix_data
        elif isinstance(matrix_data, dict) and "heatmaps" in matrix_data:
            heatmap_items = matrix_data["heatmaps"]
        elif isinstance(matrix_data, dict) and "data" in matrix_data and "columns" in matrix_data:
            heatmap_items = [{
                "device_name": meta.get("device_name"),
                "metric_name": meta.get("key", "MÉTRICA"),
                "data": matrix_data.get("data", []),
                "col_labels": matrix_data.get("columns"),
                "row_labels": matrix_data.get("index"),
                "rules": rules_data,
                "title": title
            }]
        elif isinstance(matrix_data, pd.DataFrame):
            heatmap_items = [{
                "device_name": meta.get("device_name"),
                "metric_name": meta.get("key", "MÉTRICA"),
                "data": matrix_data.values.tolist(),
                "col_labels": [str(c) for c in matrix_data.columns],
                "row_labels": [str(i) for i in matrix_data.index],
                "rules": rules_data,
                "title": title
            }]
        else:
            df = parse_matrix_data(matrix_data)
            heatmap_items = [{
                "device_name": meta.get("device_name"),
                "metric_name": meta.get("key", "MÉTRICA"),
                "data": df.values.tolist(),
                "col_labels": [str(c) for c in df.columns],
                "row_labels": [str(i) for i in df.index],
                "rules": rules_data,
                "title": title
            }]

        if not heatmap_items:
            raise ValueError("No se proporcionaron datos de mapas de calor para generar el reporte.")

        rendered_pages: List[Tuple[str, int, int]] = []
        total_pages = len(heatmap_items)

        # 2. Renderizar cada mapa de calor individual en imagen temporal
        base_no_ext = os.path.splitext(output_pdf_path)[0]
        for idx, item in enumerate(heatmap_items):
            item_data = item.get("data", [])
            item_rules_raw = item.get("rules") or rules_data
            item_rules = parse_heatmap_rules(item_rules_raw)
            item_device = item.get("device_name") or meta.get("device_name")
            item_metric = item.get("metric_name") or meta.get("key", "MÉTRICA")
            item_title = item.get("title") or title
            item_col_labels = item.get("col_labels")
            item_row_labels = item.get("row_labels")
            page_num = idx + 1

            temp_img = f"{base_no_ext}_chart_p{page_num}.png"
            temp_images_created.append(temp_img)

            _, (w, h) = _render_single_heatmap_image(
                matrix_data=item_data,
                rules=item_rules,
                output_image_path=temp_img,
                title=item_title,
                metric_name=item_metric,
                device_name=item_device,
                col_labels=item_col_labels,
                row_labels=item_row_labels,
                page_num=page_num,
                total_pages=total_pages
            )
            rendered_pages.append((temp_img, w, h))

        # 3. Compilar PDF Landscape con ReportLab (Letter Landscape: 792 x 612 pt)
        doc = SimpleDocTemplate(
            output_pdf_path,
            pagesize=landscape(letter),
            leftMargin=15,
            rightMargin=15,
            topMargin=15,
            bottomMargin=15
        )

        story = []
        for p_idx, (img_path, orig_w, orig_h) in enumerate(rendered_pages):
            if p_idx > 0:
                story.append(PageBreak())

            # Márgenes de seguridad para evitar desbordamiento en frames secundarios de ReportLab
            avail_w = doc.width - 10
            avail_h = doc.height - 20

            scale_w = avail_w / float(orig_w)
            scale_h = avail_h / float(orig_h)
            scale = min(scale_w, scale_h, 1.0)

            final_w = orig_w * scale
            final_h = orig_h * scale

            rl_img = RLImage(img_path, width=final_w, height=final_h)
            rl_img.hAlign = "CENTER"
            story.append(rl_img)

        doc.build(story)
        logger.info(f"[Heatmap Report] PDF multi-página generado exitosamente ({len(rendered_pages)} páginas) en: {output_pdf_path}")
        return output_pdf_path

    finally:
        # Limpieza defensiva de todas las imágenes temporales
        for tmp in temp_images_created:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception as e:
                    logger.warning(f"No se pudo eliminar imagen temporal {tmp}: {e}")
        plt.close("all")
        gc.collect()


# ==============================================================================
# 5. SERVICIO DE DOMINIO ASÍNCRONO CON PROCESSPOOLEXECUTOR
# ==============================================================================

async def generate_heatmap_report_pdf(
    matrix_data: Union[dict, list, pd.DataFrame],
    rules: Union[list, dict],
    output_pdf_path: str,
    title: str = "Reporte mensual | mapas de calor",
    subtitle: Optional[str] = None,
    metadata: Optional[dict] = None
) -> str:
    """
    Servicio de Dominio Asíncrono para la generación de reportes de mapas de calor en PDF.
    
    Aislamiento de CPU (Crucial):
    Encapsula el renderizado gráfico y compilación de PDF dentro de una función
    sincrónica delegada a un subproceso vía ProcessPoolExecutor, erradicando cualquier
    bloqueo sobre el GIL y el Event Loop del worker de ARQ.
    """
    # Serializar DataFrame a diccionario para transporte limpio inter-procesos
    if isinstance(matrix_data, pd.DataFrame):
        serializable_matrix = {
            "columns": list(matrix_data.columns),
            "index": [str(idx) for idx in matrix_data.index],
            "data": matrix_data.values.tolist()
        }
    else:
        serializable_matrix = matrix_data

    # Serializar instancias de HeatmapRule si existen
    if isinstance(rules, list) and any(isinstance(r, HeatmapRule) for r in rules):
        serializable_rules = [r.to_dict() if isinstance(r, HeatmapRule) else r for r in rules]
    else:
        serializable_rules = rules

    loop = asyncio.get_running_loop()

    with ProcessPoolExecutor(max_workers=1) as executor:
        result_pdf_path = await loop.run_in_executor(
            executor,
            _sync_generate_heatmap_pdf,
            serializable_matrix,
            serializable_rules,
            output_pdf_path,
            title,
            subtitle,
            metadata
        )

    return result_pdf_path


def interpolate_heatmap_placeholders(
    text: Optional[str],
    mes_nombre: str,
    anio: Union[int, str],
    period: str,
    tenant: str
) -> Optional[str]:
    """
    Interpola variables dinámicas en el asunto o cuerpo de los correos de mapa de calor.
    Soporta:
      - {mes año}, {mes ano}, {mes_año}, {mes_ano} -> 'Agosto 2026'
      - {month year}, {month_year} -> 'Agosto 2026'
      - {mes}, {month} -> 'Agosto'
      - {año}, {ano}, {year} -> '2026'
      - {periodo}, {period} -> '2026-08'
      - {tenant}, {tenant_name} -> Nombre del Tenant
    """
    if not text:
        return text

    mes_ano = f"{mes_nombre} {anio}"

    # Reemplazo de mes y año juntos (más específico primero)
    res = re.sub(r'\{mes[\s_]+a[ñn]o\}', mes_ano, text, flags=re.IGNORECASE)
    res = re.sub(r'\{month[\s_]+year\}', mes_ano, res, flags=re.IGNORECASE)

    # Reemplazos individuales de mes y año
    res = re.sub(r'\{mes\}', mes_nombre, res, flags=re.IGNORECASE)
    res = re.sub(r'\{month\}', mes_nombre, res, flags=re.IGNORECASE)
    res = re.sub(r'\{a[ñn]o\}', str(anio), res, flags=re.IGNORECASE)
    res = re.sub(r'\{year\}', str(anio), res, flags=re.IGNORECASE)

    # Reemplazo de período y tenant
    res = re.sub(r'\{period(?:o)?\}', period, res, flags=re.IGNORECASE)
    res = re.sub(r'\{tenant(?:_name)?\}', tenant, res, flags=re.IGNORECASE)

    return res


def extract_heatmap_whitelist_and_config(
    heatmap_config: Any,
    actual_payload: Dict[str, Any],
    default_timezone: str = "America/Mexico_City"
) -> Tuple[List[str], list, str, str, Optional[Any], Optional[Any]]:
    """
    Extrae y sanitiza la lista blanca de variables, reglas y parámetros temporales
    a partir de custom_metadata.heatmap_config y del payload de la petición.
    """
    canonical_candidates = ("keys", "variables", "whitelist", "telemetry_keys", "allowed_keys", "default")
    reserved_keys = {
        "rules", "time_zone", "aggregation", "year", "month",
        "period", "send_email", "email_options", "email_enabled",
        "from_email", "from_name", "sender_name", "to_email", "recipient",
        "subject", "email_subject", "cc", "email_cc", "bcc", "email_bcc",
        "body", "email_body", "html_body", "timeout", "title", "subtitle",
        "keys", "variables", "whitelist", "telemetry_keys", "allowed_keys", "default"
    }

    whitelist_keys: List[str] = []
    rules = []
    time_zone_str = actual_payload.get("time_zone") or default_timezone
    agg_func = "AVG"
    raw_year = actual_payload.get("year")
    raw_month = actual_payload.get("month")

    if isinstance(heatmap_config, list):
        whitelist_keys = [
            str(k).strip() for k in heatmap_config
            if not isinstance(k, bool) and isinstance(k, (str, int, float)) and str(k).strip()
        ]
        rules = actual_payload.get("rules") or []
    elif isinstance(heatmap_config, str):
        whitelist_keys = [str(k).strip() for k in heatmap_config.split(",") if str(k).strip()]
        rules = actual_payload.get("rules") or []
    elif isinstance(heatmap_config, dict):
        canonical_found = False
        raw_keys = None
        for cand in canonical_candidates:
            if cand in heatmap_config:
                raw_keys = heatmap_config[cand]
                canonical_found = True
                break

        if canonical_found:
            if raw_keys is not None:
                if isinstance(raw_keys, list):
                    whitelist_keys = [
                        str(k).strip() for k in raw_keys
                        if not isinstance(k, bool) and isinstance(k, (str, int, float)) and str(k).strip()
                    ]
                elif isinstance(raw_keys, str):
                    whitelist_keys = [str(k).strip() for k in raw_keys.split(",") if str(k).strip()]
                elif isinstance(raw_keys, dict):
                    whitelist_keys = [str(k).strip() for k in raw_keys.keys() if str(k).strip()]
                elif not isinstance(raw_keys, bool) and isinstance(raw_keys, (int, float)):
                    whitelist_keys = [str(raw_keys).strip()]
                else:
                    whitelist_keys = []
            else:
                whitelist_keys = []
        else:
            whitelist_keys = [
                str(k).strip() for k in heatmap_config.keys()
                if k not in reserved_keys and str(k).strip()
            ]

        rules = heatmap_config.get("rules") or actual_payload.get("rules") or []
        time_zone_str = (
            heatmap_config.get("time_zone")
            or actual_payload.get("time_zone")
            or default_timezone
        )
        agg_func = str(heatmap_config.get("aggregation", "AVG")).strip().upper()
        raw_year = actual_payload.get("year") or heatmap_config.get("year")
        raw_month = actual_payload.get("month") or heatmap_config.get("month")
    else:
        whitelist_keys = []
        rules = actual_payload.get("rules") or []

    # Limpieza y desduplicación preservando orden
    cleaned_whitelist: List[str] = []
    for k in whitelist_keys:
        if "," in k:
            for sub_k in k.split(","):
                sub_clean = sub_k.strip()
                if sub_clean and sub_clean not in cleaned_whitelist:
                    cleaned_whitelist.append(sub_clean)
        else:
            clean_k = k.strip()
            if clean_k and clean_k not in cleaned_whitelist:
                cleaned_whitelist.append(clean_k)

    return cleaned_whitelist, rules, time_zone_str, agg_func, raw_year, raw_month


def build_or_normalize_heatmap_matrix(
    points: List[dict],
    last_day: int,
    agg_func: str,
    tz: Any,
    existing_matrix: Optional[List[List[Any]]] = None
) -> List[List[Any]]:
    """
    Normaliza una matriz precalculada a dimensiones (last_day x 24) con relleno 'NA'
    o agrega puntos de telemetría por día y hora según la función de agregación especificada.
    """
    if existing_matrix is not None:
        matrix_data = [list(r) if isinstance(r, (tuple, list)) else [r] for r in existing_matrix]
        while len(matrix_data) < last_day:
            matrix_data.append(["NA"] * 24)
        if len(matrix_data) > last_day:
            matrix_data = matrix_data[:last_day]
        for r_idx in range(len(matrix_data)):
            row = matrix_data[r_idx]
            while len(row) < 24:
                row.append("NA")
            if len(row) > 24:
                row = row[:24]
            matrix_data[r_idx] = row
        return matrix_data

    hourly_buckets: List[List[List[float]]] = [[[] for _ in range(24)] for _ in range(last_day)]
    for pt in points:
        if not isinstance(pt, dict):
            continue
        ts = pt.get("ts")
        val = pt.get("value")
        if ts is not None and val is not None:
            try:
                num_v = float(val)
                pt_dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).astimezone(tz)
                if 1 <= pt_dt.day <= last_day and 0 <= pt_dt.hour < 24:
                    hourly_buckets[pt_dt.day - 1][pt_dt.hour].append(num_v)
            except (ValueError, TypeError):
                pass

    result_matrix = []
    for d_idx in range(last_day):
        row = []
        for h_idx in range(24):
            b = hourly_buckets[d_idx][h_idx]
            if not b:
                row.append("NA")
            elif agg_func == "MAX":
                row.append(round(max(b), 2))
            elif agg_func == "MIN":
                row.append(round(min(b), 2))
            elif agg_func == "SUM":
                row.append(round(sum(b), 2))
            elif agg_func == "LAST":
                row.append(round(b[-1], 2))
            else:  # Default: AVG
                row.append(round(sum(b) / len(b), 2))
        result_matrix.append(row)

    return result_matrix

