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
from PIL import Image as PILImage

from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import (
    SimpleDocTemplate,
    Image as RLImage,
    PageBreak
)

from core.logger import logger


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
    title: str = "Reporte mensual | mapas de calor",
    metric_name: str = "MÉTRICA",
    device_name: Optional[str] = None,
    col_labels: Optional[List[str]] = None,
    row_labels: Optional[List[str]] = None,
    na_color: str = "#5C2D91"
) -> Tuple[str, Tuple[int, int]]:
    """
    Genera la imagen PNG del heatmap con celdas redondeadas tipo píldora (Pill Tiles).
    Replica visualmente de manera exacta el diseño de la referencia de ThingsBoard:
    - Eje X: 24 horas (00:00 a 23:00) con cabecera 'DÍA' en esquina superior izquierda.
    - Eje Y: Días del mes (Día 1 a Día 30/31).
    - Celdas con bordes redondeados, borde blanco y tipografía contrastante.
    - Celdas 'NA' con color púrpura (#5C2D91) y texto en blanco.
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

    cell_w = 1.0
    cell_h = 0.65
    gap = 0.08
    corner_rad = 0.12

    total_w = num_cols * (cell_w + gap)
    total_h = num_rows * (cell_h + gap)

    fig_w = max(18.0, total_w + 3.2)
    fig_h = max(7.0, total_h * 0.55 + 2.8)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=160)
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")

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
                fontsize=8.5,
                fontweight="bold",
                ha="center",
                va="center"
            )

    ax.set_xlim(-1.6, total_w + 0.5)
    ax.set_ylim(-0.8, total_h + 1.6)

    # Cabecera de columnas y esquina
    header_y = total_h + 0.35
    ax.text(-0.8, header_y, corner_label, color="#64748b", fontsize=9.5, fontweight="bold", ha="center", va="center")
    for c_idx in range(num_cols):
        x = c_idx * (cell_w + gap) + cell_w / 2.0
        ax.text(x, header_y, str(col_labels[c_idx]), color="#64748b", fontsize=9, fontweight="medium", ha="center", va="center")

    # Etiquetas de filas
    for r_idx in range(num_rows):
        y = (num_rows - 1 - r_idx) * (cell_h + gap) + cell_h / 2.0
        ax.text(-0.8, y, str(row_labels[r_idx]), color="#64748b", fontsize=9, fontweight="medium", ha="center", va="center")

    # Título superior izquierdo
    title_y = total_h + 1.25
    ax.text(0.0, title_y, title, color="#1e293b", fontsize=16, fontweight="bold", ha="left", va="center")

    # Encabezado superior derecho: DISPOSITIVO | MÉTRICA
    clean_metric = metric_name if metric_name.upper().startswith("MÉTRICA") else f"MÉTRICA: {metric_name}"
    if device_name and device_name.strip():
        right_header = f"DISPOSITIVO: {device_name.strip()}   |   {clean_metric}"
    else:
        right_header = clean_metric
    ax.text(total_w - gap, title_y, right_header, color="#475569", fontsize=13, fontweight="bold", ha="right", va="center")

    ax.axis("off")
    plt.tight_layout(pad=1.0)
    os.makedirs(os.path.dirname(output_image_path), exist_ok=True)
    plt.savefig(output_image_path, facecolor=fig.get_facecolor(), edgecolor="none", dpi=160, bbox_inches="tight")

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

            temp_img = f"{base_no_ext}_chart_p{idx+1}.png"
            temp_images_created.append(temp_img)

            _, (w, h) = _render_single_heatmap_image(
                matrix_data=item_data,
                rules=item_rules,
                output_image_path=temp_img,
                title=item_title,
                metric_name=item_metric,
                device_name=item_device,
                col_labels=item_col_labels,
                row_labels=item_row_labels
            )
            rendered_pages.append((temp_img, w, h))

        # 3. Compilar PDF Landscape con ReportLab (Letter Landscape: 792 x 612 pt)
        doc = SimpleDocTemplate(
            output_pdf_path,
            pagesize=landscape(letter),
            leftMargin=20,
            rightMargin=20,
            topMargin=20,
            bottomMargin=20
        )

        story = []
        for p_idx, (img_path, orig_w, orig_h) in enumerate(rendered_pages):
            if p_idx > 0:
                story.append(PageBreak())

            avail_w = doc.width
            avail_h = doc.height

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
