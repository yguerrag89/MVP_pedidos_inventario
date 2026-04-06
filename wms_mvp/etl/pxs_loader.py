from __future__ import annotations

import shutil
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from wms_mvp.core.config import ARCHIVE_DIR, INPUT_DIR, ensure_directories
from wms_mvp.core.rules.pedidos_rules import (
    aggregate_lineas_metrics,
    build_linea_from_import_row,
    build_pedido_from_import_row,
    normalize_pause_fields,
    normalize_text,
    validate_linea_quantities,
    validate_pedido_header,
)
from wms_mvp.core.services.asignacion_service import get_oferta_para_pedido_nuevo_lineas
from wms_mvp.core.services.entradas_service import finish_import_job, start_import_job
from wms_mvp.core.services.pedidos_service import create_new_pedido_without_autoassignment
from wms_mvp.db.connection import transaction
from wms_mvp.db.repositories import auditoria_repository, partidas_repository, pedidos_repository
from wms_mvp.etl.excel_utils import (
    add_missing_columns,
    dataframe_from_detected_header,
    dataframe_to_records,
    detect_header_row,
    ensure_path,
    ensure_required_columns,
    extract_sku_from_text,
    parse_excel_date,
    read_excel_sheet_names,
    read_raw_sheet,
    rename_columns_by_aliases,
    safe_str,
    split_producto_y_color,
    trim_excel_number_text,
)


PREFERRED_SHEET_NAMES = [
    "PXS",
    "PXS limpio",
    "PEDIDOS",
    "PEDIDOS ACTIVOS",
    "Hoja1",
    "Hoja 1",
    "Datos",
]

IGNORED_SHEET_NAMES = {
    "PENDIENTES",
    "ESTATUS 2025",
    "CODIGOS",
    "CLIENTES",
    "CODIGO",
    "TABLA DE CODIGOS",
    "2026",  # histórico, no debe entrar como PXS limpio
}

CODIGOS_SHEET_CANDIDATES = [
    "codigos",
    "codigo",
    "tabla de codigos",
]

PXS_HEADER_MARKERS_STANDARD = [
    "DESCRIPCION",
    "PRODUCTO",
    "COLOR",
    "CLIENTE",
    "Serie documento",
    "DOCUMENTO",
    "FECHA CAPTURA",
    "Cantidad",
    "EXISTENTE",
    "PEDIDO FALTANTE",
]

PXS_HEADER_MARKERS_MINIMAL = [
    "Serie documento",
    "Numero documento",
    "Fecha",
    "Cantidad",
    "Tipo de documento pedido",
]

PXS_HEADER_MARKERS = list(dict.fromkeys(PXS_HEADER_MARKERS_STANDARD + PXS_HEADER_MARKERS_MINIMAL))

PXS_ALIASES = {
    "descripcion": [
        "DESCRIPCION",
        "Descripción",
        "descripcion",
        "descripcion_producto",
        "Unnamed: 0",
        "unnamed: 0",
        "nan",
    ],
    "producto": [
        "PRODUCTO",
        "producto",
        "familia",
    ],
    "color": [
        "COLOR",
        "color",
    ],
    "cliente": [
        "CLIENTE",
        "cliente",
    ],
    "serie_documento": [
        "Serie documento",
        "SERIE DOCUMENTO",
        "serie_documento",
        "serie",
    ],
    "documento": [
        "DOCUMENTO",
        "documento",
        "folio",
        "Numero documento",
        "numero_documento",
    ],
    "fecha_captura": [
        "FECHA CAPTURA",
        "fecha_captura",
        "Fecha",
        "fecha",
    ],
    "fecha_produccion": [
        "FECHA PRODUCCION",
        "FECHA DE PRODUCCION",
        "FECHA PROD",
        "fecha_produccion",
        "fecha_producción",
        "fecha_prod",
        "fecha_producccion",
    ],
    "fecha_logistica": [
        "FECHA LOGISTICA",
        "fecha_logistica",
        "FECHA DE ENTREGA",
        "fecha_de_entrega",
    ],
    "cantidad": [
        "Cantidad",
        "CANTIDAD",
        "cantidad",
    ],
    "existente": [
        "EXISTENTE",
        "existente",
        "cantidad_asignada",
    ],
    "pedido_faltante": [
        "PEDIDO FALTANTE",
        "pedido_faltante",
        "faltante",
    ],
    "porcentaje_fuente": [
        "%",
        "PORCENTAJE",
        "porcentaje",
    ],
    "estatus_venta": [
        "ESTATUS VENTA",
        "estatus_venta",
        "estatus venta",
    ],
    "motivo_pausa": [
        "MOTIVO PAUSA",
        "Motivo de pausa",
        "motivo_pausa",
        "motivo pausa",
        "razon_pausa",
        "razón pausa",
        "razon de pausa",
        "razón de pausa",
    ],
    "proyecto": [
        "PROYECTO",
        "proyecto",
    ],
    "sku": [
        "SKU",
        "sku",
        "CLAVE",
        "clave",
        "clave_producto",
    ],
    "tipo_documento_pedido": [
        "Tipo de documento pedido",
        "tipo_documento_pedido",
        "tipo de documento pedido",
    ],
}

REQUIRED_CANONICAL_COLUMNS = [
    "descripcion",
    "serie_documento",
    "documento",
]

OPTIONAL_CANONICAL_COLUMNS = [
    "producto",
    "color",
    "cliente",
    "fecha_captura",
    "fecha_produccion",
    "fecha_logistica",
    "cantidad",
    "existente",
    "pedido_faltante",
    "porcentaje_fuente",
    "estatus_venta",
    "motivo_pausa",
    "proyecto",
    "sku",
    "tipo_documento_pedido",
]

DEFAULT_EXCLUDED_CLIENT_TOKENS = {
    "ENVIO",
    "FLETE",
    "SERVICIO",
}


# -----------------------------------------------------------------------------
# Helpers internos
# -----------------------------------------------------------------------------
def _normalize_upper_text(value: Any) -> str | None:
    text = normalize_text(value)
    return text.upper() if text else None


def _normalize_documento_text(value: Any) -> str | None:
    """Normaliza documento preservando mejor el formato de Excel."""
    text = trim_excel_number_text(value)
    if not text:
        return None

    text = str(text).strip()
    if text.isdigit() and len(text) < 8:
        return text.zfill(8)

    return text


def _normalize_serie_documento(value: Any) -> str | None:
    text = normalize_text(value)
    return text.upper() if text else None


def _choose_preferred_iso_date(*values: Any) -> str | None:
    """
    Devuelve la fecha ISO más temprana entre los valores recibidos.
    Se usa para consolidar fechas del encabezado de pedido.
    """
    parsed = [parse_excel_date(value) for value in values if parse_excel_date(value)]
    return min(parsed) if parsed else None


def _get_table_columns(conn: Any, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row.get("name")) for row in rows if row.get("name")}


def _insert_dynamic_row(conn: Any, table_name: str, payload: dict[str, Any]) -> int:
    columns = [column for column, _ in payload.items()]
    placeholders = ", ".join(["?"] * len(columns))
    sql = (
        f"INSERT INTO {table_name} (" + ", ".join(columns) + ") "
        f"VALUES ({placeholders})"
    )
    cursor = conn.execute(sql, tuple(payload[column] for column in columns))
    return int(cursor.lastrowid or 0)


def _copy_file_to_dir(source_path: str | Path, target_dir: Path) -> Path:
    ensure_directories()
    source = ensure_path(source_path)
    target_dir.mkdir(parents=True, exist_ok=True)

    candidate = target_dir / source.name
    if not candidate.exists():
        shutil.copy2(source, candidate)
        return candidate

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = target_dir / f"{source.stem}_{timestamp}{source.suffix}"
    shutil.copy2(source, candidate)
    return candidate


def _is_sheet_explicitly_ignored(sheet_name: str) -> bool:
    return (_normalize_upper_text(sheet_name) or "") in IGNORED_SHEET_NAMES


def _choose_candidate_sheets(
    file_path: str | Path,
    sheet_names: list[str | int] | None = None,
) -> list[str | int]:
    """Resuelve las hojas candidatas para importación PXS."""
    path = ensure_path(file_path)

    if path.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
        return [0]

    workbook_sheets = read_excel_sheet_names(path)

    if sheet_names:
        resolved: list[str | int] = []
        for sheet_name in sheet_names:
            if isinstance(sheet_name, int):
                resolved.append(sheet_name)
                continue
            if sheet_name in workbook_sheets:
                resolved.append(sheet_name)
        return resolved

    ordered: list[str | int] = []

    existing_upper = {
        (_normalize_upper_text(name) or ""): name
        for name in workbook_sheets
    }

    for preferred in PREFERRED_SHEET_NAMES:
        preferred_upper = _normalize_upper_text(preferred)
        if preferred_upper and preferred_upper in existing_upper:
            ordered.append(existing_upper[preferred_upper])

    for sheet_name in workbook_sheets:
        if sheet_name in ordered:
            continue
        if _is_sheet_explicitly_ignored(sheet_name):
            continue
        ordered.append(sheet_name)

    return ordered


def _sheet_looks_like_pxs(
    file_path: str | Path,
    sheet_name: str | int,
    search_rows: int = 15,
) -> bool:
    try:
        raw_df = read_raw_sheet(file_path, sheet_name=sheet_name, max_rows=search_rows)
        for markers, min_matches in (
            (PXS_HEADER_MARKERS_STANDARD, 5),
            (PXS_HEADER_MARKERS_MINIMAL, 4),
            (PXS_HEADER_MARKERS, 4),
        ):
            try:
                detect_header_row(
                    raw_df,
                    markers=markers,
                    search_rows=search_rows,
                    min_matches=min_matches,
                )
                return True
            except Exception:
                continue
        return False
    except Exception:
        return False


def _safe_numeric(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _infer_cantidad_pedida(
    cantidad: Any,
    existente: Any,
    pedido_faltante: Any,
) -> float:
    """
    PXS a veces trae inconsistencias entre Cantidad, Existente y Pedido faltante.
    Para no romper el importador, tomamos la mejor estimación coherente:
    - Cantidad
    - Existente
    - Existente + Faltante
    y usamos el máximo positivo.
    """
    cantidad_num = _safe_numeric(cantidad, 0.0)
    existente_num = _safe_numeric(existente, 0.0)
    faltante_num = _safe_numeric(pedido_faltante, 0.0)

    candidates = [cantidad_num, existente_num, existente_num + faltante_num]
    positive = [value for value in candidates if value > 0]
    return round(max(positive), 6) if positive else 0.0


def _to_lookup_key(value: Any) -> int | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _normalize_lookup_text(value: Any) -> str | None:
    text = normalize_text(value)
    return text if text else None


def _resolve_codigos_sheet_name(file_path: str | Path) -> str | None:
    path = ensure_path(file_path)
    if path.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
        return None

    names = read_excel_sheet_names(path)
    upper_map = {(_normalize_upper_text(name) or ""): name for name in names}
    for candidate in CODIGOS_SHEET_CANDIDATES:
        normalized = _normalize_upper_text(candidate)
        if normalized in upper_map:
            return str(upper_map[normalized])
    return None


def _load_codigos_lookup(file_path: str | Path) -> dict[str, dict[int, str]]:
    """
    Carga las tablas maestras desde la hoja codigos.

    Estructura esperada:
    - A:B  => artículo
    - E:F  => modelo
    - H:I  => color base
    - K:L  => filete
    """
    sheet_name = _resolve_codigos_sheet_name(file_path)
    if not sheet_name:
        return {
            "articulo": {},
            "modelo": {},
            "color": {},
            "filete": {},
        }

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=object)

    def build_map(col_key: int, col_value: int) -> dict[int, str]:
        mapping: dict[int, str] = {}
        if col_key >= df.shape[1] or col_value >= df.shape[1]:
            return mapping
        for row_idx in range(1, len(df)):
            key = _to_lookup_key(df.iat[row_idx, col_key])
            value = _normalize_lookup_text(df.iat[row_idx, col_value])
            if key is None or not value:
                continue
            mapping[key] = value
        return mapping

    return {
        "articulo": build_map(0, 1),
        "modelo": build_map(4, 5),
        "color": build_map(7, 8),
        "filete": build_map(10, 11),
    }


def _lookup_exact(mapping: dict[int, str], key: int | None) -> str | None:
    if key is None:
        return None
    return mapping.get(key)


def _lookup_approx(mapping: dict[int, str], key: int | None) -> str | None:
    """
    Emula BUSCARV(...; ...; ...; 1): busca el valor con llave <= key más cercana.
    """
    if key is None or not mapping:
        return None
    candidates = [candidate for candidate in mapping.keys() if candidate <= key]
    if not candidates:
        return None
    return mapping[max(candidates)]


def _derive_producto_color_from_sku(
    sku: str | None,
    codigos_lookup: dict[str, dict[int, str]] | None,
) -> tuple[str | None, str | None]:
    if not sku or not codigos_lookup:
        return None, None

    sku_text = trim_excel_number_text(sku)
    if not sku_text or not str(sku_text).isdigit():
        return None, None

    sku_text = str(sku_text)
    if len(sku_text) < 12:
        return None, None

    articulo_code = _to_lookup_key(sku_text[2:4])
    modelo_code = _to_lookup_key(sku_text[4:8])
    color_code = _to_lookup_key(sku_text[8:10])
    filete_code = _to_lookup_key(sku_text[10:12])

    articulo_text = _lookup_approx(codigos_lookup.get("articulo", {}), articulo_code)
    modelo_text = _lookup_exact(codigos_lookup.get("modelo", {}), modelo_code)
    color_text = _lookup_approx(codigos_lookup.get("color", {}), color_code)
    filete_text = _lookup_exact(codigos_lookup.get("filete", {}), filete_code)

    producto = " ".join(part for part in [articulo_text, modelo_text] if part)
    color = " ".join(part for part in [color_text, filete_text] if part)

    return producto or None, color or None


def _normalize_pxs_row(
    row: dict[str, Any],
    sheet_name: str | int,
    row_number: int,
    codigos_lookup: dict[str, dict[int, str]] | None = None,
    excluded_client_tokens: set[str] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """
    Normaliza una fila PXS al formato operativo.
    Admite tanto el formato rico tradicional como el formato mínimo de demanda.
    Devuelve (row_normalized, reason_if_rejected).
    """
    descripcion = normalize_text(row.get("descripcion"))
    cliente = normalize_text(row.get("cliente"))
    proyecto = normalize_text(row.get("proyecto"))
    serie_documento = _normalize_serie_documento(row.get("serie_documento"))
    documento = _normalize_documento_text(row.get("documento"))
    tipo_documento_pedido = normalize_text(row.get("tipo_documento_pedido"))

    if not descripcion:
        return None, "descripcion_vacia"

    if not serie_documento or not documento:
        return None, "id_pedido_incompleto"

    excluded_tokens = {token.upper() for token in (excluded_client_tokens or set())}
    combined_text = " ".join(
        part for part in [cliente, proyecto] if normalize_text(part)
    ).upper()
    if cliente and excluded_tokens and any(token in combined_text for token in excluded_tokens):
        return None, "cliente_o_proyecto_excluido"

    id_pedido = f"{serie_documento}-{documento}"

    fecha_captura = parse_excel_date(row.get("fecha_captura"))
    fecha_produccion = parse_excel_date(row.get("fecha_produccion"))
    fecha_logistica = parse_excel_date(row.get("fecha_logistica"))

    sku = trim_excel_number_text(row.get("sku"))
    if not sku:
        sku = extract_sku_from_text(descripcion)

    producto_excel = normalize_text(row.get("producto"))
    color_excel = normalize_text(row.get("color"))
    producto_codigos, color_codigos = _derive_producto_color_from_sku(
        sku=sku,
        codigos_lookup=codigos_lookup,
    )
    producto_split, color_split = split_producto_y_color(descripcion)

    producto = producto_codigos or producto_excel or producto_split
    color = color_codigos or color_excel or color_split

    cantidad_pedida = _infer_cantidad_pedida(
        cantidad=row.get("cantidad"),
        existente=row.get("existente"),
        pedido_faltante=row.get("pedido_faltante"),
    )
    cantidad_asignada_fuente = round(_safe_numeric(row.get("existente"), 0.0), 6)
    cantidad_faltante_fuente = round(_safe_numeric(row.get("pedido_faltante"), 0.0), 6)
    cantidad_asignada = 0.0

    if cantidad_pedida <= 0 and cantidad_asignada_fuente <= 0 and cantidad_faltante_fuente <= 0:
        return None, "linea_sin_cantidades_utiles"
    if cantidad_pedida <= 0:
        return None, "cantidad_pedida_no_valida"

    try:
        estatus_venta, motivo_pausa = normalize_pause_fields(
            row.get("estatus_venta"),
            row.get("motivo_pausa"),
        )
    except ValueError as exc:
        return None, f"estatus_venta_invalido: {exc}"

    normalized = {
        "__sheet_name__": str(sheet_name),
        "__row_number__": int(row_number),
        "id_pedido": id_pedido,
        "serie_documento": serie_documento,
        "documento": documento,
        "cliente": cliente,
        "cliente_faltante": cliente is None,
        "tipo_documento_pedido": tipo_documento_pedido,
        "fecha_captura": fecha_captura,
        "fecha_produccion": fecha_produccion,
        "fecha_logistica": fecha_logistica,
        "estatus_venta": estatus_venta,
        "motivo_pausa": motivo_pausa,
        "descripcion": descripcion,
        "sku": sku,
        "producto": producto,
        "color": color,
        "cantidad_pedida": cantidad_pedida,
        "cantidad_asignada_fuente": cantidad_asignada_fuente,
        "cantidad_faltante_fuente": cantidad_faltante_fuente,
        "cantidad_asignada": cantidad_asignada,
        "cantidad_enviada": 0.0,
        "autoasignado": False,
        "requiere_decision_usuario": True,
        "autoassignment_policy": "manual_only",
        "origen_registro": "PXS_LIMPIO",
        "tipo_registro": "PEDIDO_NORMAL",
        "estado_operativo": "ACTIVO",
        "observaciones": f"Importado desde PXS limpio sin autoasignacion. hoja={sheet_name}, fila={row_number}",
    }
    return normalized, None



def _group_rows_into_pedidos(normalized_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Agrupa filas normalizadas por id_pedido y consolida líneas repetidas por SKU."""
    buckets: dict[str, dict[str, Any]] = {}

    for row in sorted(
        normalized_rows,
        key=lambda r: (
            safe_str(r.get("id_pedido")),
            safe_str(r.get("__sheet_name__")),
            int(r.get("__row_number__", 0)),
        ),
    ):
        id_pedido = safe_str(row.get("id_pedido"))
        if not id_pedido:
            continue

        if id_pedido not in buckets:
            buckets[id_pedido] = {
                "pedido": {
                    "id_pedido": id_pedido,
                    "serie_documento": row.get("serie_documento"),
                    "documento": row.get("documento"),
                    "tipo_registro": row.get("tipo_registro", "PEDIDO_NORMAL"),
                    "origen_registro": row.get("origen_registro", "PXS_LIMPIO"),
                    "cliente": row.get("cliente"),
                    "fecha_captura": row.get("fecha_captura"),
                    "fecha_produccion": row.get("fecha_produccion"),
                    "fecha_logistica": row.get("fecha_logistica"),
                    "estatus_venta": row.get("estatus_venta"),
                    "motivo_pausa": row.get("motivo_pausa"),
                    "estado_operativo": row.get("estado_operativo", "ACTIVO"),
                    "observaciones": row.get("observaciones"),
                    "tipo_documento_pedido": row.get("tipo_documento_pedido"),
                    "cliente_faltante": bool(row.get("cliente_faltante")),
                },
                "lineas": [],
                "source_rows": [],
            }

        bucket = buckets[id_pedido]
        pedido_header = bucket["pedido"]

        for field_name in [
            "cliente",
            "estatus_venta",
            "motivo_pausa",
            "serie_documento",
            "documento",
            "observaciones",
            "tipo_documento_pedido",
        ]:
            if not pedido_header.get(field_name) and row.get(field_name):
                pedido_header[field_name] = row.get(field_name)

        pedido_header["cliente_faltante"] = bool(pedido_header.get("cliente_faltante") and not pedido_header.get("cliente"))
        pedido_header["fecha_captura"] = _choose_preferred_iso_date(
            pedido_header.get("fecha_captura"),
            row.get("fecha_captura"),
        )
        pedido_header["fecha_produccion"] = _choose_preferred_iso_date(
            pedido_header.get("fecha_produccion"),
            row.get("fecha_produccion"),
        )
        pedido_header["fecha_logistica"] = _choose_preferred_iso_date(
            pedido_header.get("fecha_logistica"),
            row.get("fecha_logistica"),
        )

        bucket["lineas"].append(
            {
                "sku": row.get("sku"),
                "descripcion": row.get("descripcion"),
                "producto": row.get("producto"),
                "color": row.get("color"),
                "cantidad_pedida": row.get("cantidad_pedida", 0.0),
                "cantidad_asignada": row.get("cantidad_asignada", 0.0),
                "cantidad_enviada": row.get("cantidad_enviada", 0.0),
                "fecha_captura": row.get("fecha_captura"),
                "fecha_produccion": row.get("fecha_produccion"),
                "fecha_logistica": row.get("fecha_logistica"),
                "estatus_venta": row.get("estatus_venta"),
                "motivo_pausa": row.get("motivo_pausa"),
                "observaciones": row.get("observaciones"),
                "origen_filas": [row.get("__row_number__")],
                "tipo_documento_pedido": row.get("tipo_documento_pedido"),
            }
        )
        bucket["source_rows"].append(row)

    grouped = list(buckets.values())
    for bucket in grouped:
        agregadas: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for linea in bucket["lineas"]:
            key = (
                safe_str(linea.get("sku")),
                safe_str(linea.get("descripcion")),
                safe_str(linea.get("producto")),
                safe_str(linea.get("color")),
            )
            if key not in agregadas:
                agregadas[key] = dict(linea)
                continue
            merged = agregadas[key]
            merged["cantidad_pedida"] = round(
                _safe_numeric(merged.get("cantidad_pedida"), 0.0)
                + _safe_numeric(linea.get("cantidad_pedida"), 0.0),
                6,
            )
            merged["origen_filas"] = sorted(
                {
                    *[item for item in merged.get("origen_filas", []) if item is not None],
                    *[item for item in linea.get("origen_filas", []) if item is not None],
                }
            )
            merged["fecha_captura"] = _choose_preferred_iso_date(
                merged.get("fecha_captura"), linea.get("fecha_captura")
            )
            merged["fecha_produccion"] = _choose_preferred_iso_date(
                merged.get("fecha_produccion"), linea.get("fecha_produccion")
            )
            merged["fecha_logistica"] = _choose_preferred_iso_date(
                merged.get("fecha_logistica"), linea.get("fecha_logistica")
            )

        bucket["lineas"] = list(agregadas.values())
        metrics = aggregate_lineas_metrics(bucket["lineas"])
        bucket["pedido"]["porcentaje_asignacion"] = metrics["porcentaje_asignacion"]

    return grouped


def _find_matching_existing_lines(
    existing_lines: list[dict[str, Any]],
    *,
    sku: Any,
    descripcion: Any,
) -> list[dict[str, Any]]:
    sku_text = safe_str(sku)
    descripcion_text = safe_str(descripcion)
    if sku_text:
        matches = [line for line in existing_lines if safe_str(line.get("sku")) == sku_text]
        if matches:
            return matches
    if descripcion_text:
        return [line for line in existing_lines if safe_str(line.get("descripcion")) == descripcion_text]
    return []


def _build_line_resolution(
    pedido_header: dict[str, Any],
    linea: dict[str, Any],
    existing_pedido: dict[str, Any] | None,
    existing_lines: list[dict[str, Any]],
) -> dict[str, Any]:
    cantidad_archivo = round(_safe_numeric(linea.get("cantidad_pedida"), 0.0), 6)
    matching_lines = _find_matching_existing_lines(
        existing_lines,
        sku=linea.get("sku"),
        descripcion=linea.get("descripcion"),
    )

    existing_estado = safe_str((existing_pedido or {}).get("estado_operativo"))
    cliente_archivo = normalize_text(pedido_header.get("cliente"))
    cliente_sistema = normalize_text((existing_pedido or {}).get("cliente"))

    if existing_pedido is None:
        clasificacion = "PEDIDO_NUEVO_COMPLETO" if cliente_archivo else "PEDIDO_NUEVO_INCOMPLETO"
    elif existing_estado in {"ENVIADO_TOTAL", "ENVIADO_PARCIAL"}:
        duplicate = any(
            abs(_safe_numeric(item.get("cantidad_pedida"), 0.0) - cantidad_archivo) < 1e-9
            for item in matching_lines
        )
        clasificacion = "DUPLICADO_EN_CERRADO" if duplicate else "CONFLICTO_PEDIDO_CERRADO"
    else:
        if not matching_lines:
            clasificacion = "AMPLIACION_PEDIDO_ACTIVO"
        else:
            duplicate = any(
                abs(_safe_numeric(item.get("cantidad_pedida"), 0.0) - cantidad_archivo) < 1e-9
                for item in matching_lines
            )
            clasificacion = "DUPLICADO_EXACTO" if duplicate else "CAMBIO_EN_PEDIDO_ACTIVO"

    action_map = {
        "PEDIDO_NUEVO_COMPLETO": "CREAR_PEDIDO",
        "PEDIDO_NUEVO_INCOMPLETO": "COMPLETAR_CLIENTE",
        "DUPLICADO_EXACTO": "OMITIR_DUPLICADO",
        "DUPLICADO_EN_CERRADO": "OMITIR_DUPLICADO",
        "AMPLIACION_PEDIDO_ACTIVO": "AGREGAR_LINEA_ACTIVA",
        "CAMBIO_EN_PEDIDO_ACTIVO": "REVISAR_CAMBIO_CANTIDAD",
        "CONFLICTO_PEDIDO_CERRADO": "REVISAR_Y_CREAR_BO",
    }

    return {
        "id_pedido": pedido_header.get("id_pedido"),
        "pedido_db_id": (existing_pedido or {}).get("id"),
        "estado_operativo_sistema": existing_estado or None,
        "cliente_archivo": cliente_archivo,
        "cliente_sistema": cliente_sistema,
        "sku": linea.get("sku"),
        "descripcion": linea.get("descripcion"),
        "producto": linea.get("producto"),
        "color": linea.get("color"),
        "cantidad_archivo": cantidad_archivo,
        "cantidad_existente_sistema": round(
            sum(_safe_numeric(item.get("cantidad_pedida"), 0.0) for item in matching_lines), 6
        ) if matching_lines else 0.0,
        "cantidad_asignada_sistema": round(
            sum(_safe_numeric(item.get("cantidad_asignada"), 0.0) for item in matching_lines), 6
        ) if matching_lines else 0.0,
        "cantidad_enviada_sistema": round(
            sum(_safe_numeric(item.get("cantidad_enviada"), 0.0) for item in matching_lines), 6
        ) if matching_lines else 0.0,
        "matching_linea_ids": [item.get("id") for item in matching_lines if item.get("id") is not None],
        "matching_line_nos": [item.get("line_no") for item in matching_lines if item.get("line_no") is not None],
        "matching_count": len(matching_lines),
        "clasificacion": clasificacion,
        "accion_sugerida": action_map[clasificacion],
        "fecha_captura": pedido_header.get("fecha_captura"),
        "fecha_produccion": pedido_header.get("fecha_produccion"),
        "fecha_logistica": pedido_header.get("fecha_logistica"),
        "estatus_venta": pedido_header.get("estatus_venta"),
        "tipo_documento_pedido": pedido_header.get("tipo_documento_pedido"),
        "cliente_faltante": bool(pedido_header.get("cliente_faltante")),
        "origen_filas": ", ".join(str(item) for item in linea.get("origen_filas", []) if item is not None),
    }


def _summarize_group_resolution(
    pedido_header: dict[str, Any],
    line_resolutions: list[dict[str, Any]],
    existing_pedido: dict[str, Any] | None,
) -> dict[str, Any]:
    classification_counts: dict[str, int] = {}
    for item in line_resolutions:
        classification_counts[item["clasificacion"]] = classification_counts.get(item["clasificacion"], 0) + 1

    classes = set(classification_counts)
    if classes <= {"DUPLICADO_EXACTO", "DUPLICADO_EN_CERRADO"}:
        categoria = "SOLO_DUPLICADOS"
        accion = "OMITIR_DUPLICADO"
        safe_to_apply = True
    elif "CONFLICTO_PEDIDO_CERRADO" in classes:
        categoria = "CONFLICTO_CERRADO"
        accion = "REVISAR_Y_CREAR_BO"
        safe_to_apply = False
    elif "CAMBIO_EN_PEDIDO_ACTIVO" in classes:
        categoria = "CAMBIO_PEDIDO_ACTIVO"
        accion = "REVISAR_CAMBIO_CANTIDAD"
        safe_to_apply = False
    elif classes <= {"AMPLIACION_PEDIDO_ACTIVO", "DUPLICADO_EXACTO"}:
        categoria = "AMPLIACION_PEDIDO_ACTIVO"
        accion = "AGREGAR_A_PEDIDO_ACTIVO"
        safe_to_apply = True
    elif classes == {"PEDIDO_NUEVO_COMPLETO"}:
        categoria = "PEDIDO_NUEVO_COMPLETO"
        accion = "CREAR_PEDIDO"
        safe_to_apply = True
    elif "PEDIDO_NUEVO_INCOMPLETO" in classes:
        categoria = "PEDIDO_NUEVO_INCOMPLETO"
        accion = "COMPLETAR_CLIENTE"
        safe_to_apply = False
    else:
        categoria = "REVISAR_MANUAL"
        accion = "REVISAR_MANUAL"
        safe_to_apply = False

    return {
        "id_pedido": pedido_header.get("id_pedido"),
        "pedido_db_id": (existing_pedido or {}).get("id"),
        "cliente_archivo": pedido_header.get("cliente"),
        "cliente_sistema": (existing_pedido or {}).get("cliente"),
        "estado_operativo_sistema": (existing_pedido or {}).get("estado_operativo"),
        "fecha_captura": pedido_header.get("fecha_captura"),
        "fecha_produccion": pedido_header.get("fecha_produccion"),
        "fecha_logistica": pedido_header.get("fecha_logistica"),
        "estatus_venta": pedido_header.get("estatus_venta"),
        "tipo_documento_pedido": pedido_header.get("tipo_documento_pedido"),
        "cliente_faltante": bool(pedido_header.get("cliente_faltante")),
        "lineas_en_archivo": len(line_resolutions),
        "categorias_linea": ", ".join(sorted(classes)),
        "clasificacion_pedido": categoria,
        "accion_sugerida": accion,
        "safe_to_apply": safe_to_apply,
        "resumen_clasificacion": " | ".join(f"{k}={v}" for k, v in sorted(classification_counts.items())),
        "classification_counts": classification_counts,
    }


def _classify_pedido_groups(pedido_groups: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pedido_resolutions: list[dict[str, Any]] = []
    line_resolutions: list[dict[str, Any]] = []

    for group in pedido_groups:
        pedido_header = dict(group.get("pedido", {}))
        existing_pedido = pedidos_repository.get_pedido_by_id_pedido(safe_str(pedido_header.get("id_pedido")))
        existing_lines = partidas_repository.list_lineas_by_pedido_id(int(existing_pedido["id"])) if existing_pedido else []

        group_line_resolutions = [
            _build_line_resolution(pedido_header, linea, existing_pedido, existing_lines)
            for linea in group.get("lineas", [])
        ]
        pedido_resolution = _summarize_group_resolution(pedido_header, group_line_resolutions, existing_pedido)

        group["pedido_resolution"] = pedido_resolution
        group["line_resolutions"] = group_line_resolutions
        group["existing_pedido"] = existing_pedido
        pedido_resolutions.append(pedido_resolution)
        line_resolutions.extend(group_line_resolutions)

    return pedido_resolutions, line_resolutions


def _append_lineas_to_existing_active_pedido(
    pedido_data: dict[str, Any],
    lineas_data: list[dict[str, Any]],
    import_job_id: int,
    usuario: str | None = None,
    motivo_auditoria: str | None = None,
) -> int:
    existing = pedidos_repository.get_pedido_by_id_pedido(safe_str(pedido_data.get("id_pedido")))
    if not existing:
        raise ValueError("No existe el pedido activo para agregar líneas.")
    if safe_str(existing.get("estado_operativo")) != "ACTIVO":
        raise ValueError("Solo se pueden agregar líneas a pedidos activos.")

    current_lineas = partidas_repository.list_lineas_by_pedido_id(int(existing["id"]))
    next_line_no = max([int(item.get("line_no") or 0) for item in current_lineas] + [0])

    rows_to_insert: list[dict[str, Any]] = []
    for raw_linea in lineas_data:
        next_line_no += 1
        payload = build_linea_from_import_row(raw_linea, pedido_id=int(existing["id"]), line_no=next_line_no)
        payload["cantidad_asignada"] = 0.0
        payload["cantidad_enviada"] = 0.0
        payload["cantidad_faltante"] = round(_safe_numeric(payload.get("cantidad_pedida"), 0.0), 6)
        validate_linea_quantities(
            cantidad_pedida=payload["cantidad_pedida"],
            cantidad_asignada=payload["cantidad_asignada"],
            allow_overassignment=False,
        )
        rows_to_insert.append(payload)

    partidas_repository.create_many_pedido_lineas(rows_to_insert)
    partidas_repository.recalculate_all_lineas_faltante_by_pedido(int(existing["id"]))
    pedidos_repository.recalculate_pedido_metrics(int(existing["id"]))
    auditoria_repository.create_auditoria_cambio(
        {
            "tabla": "pedidos",
            "registro_id": existing["id"],
            "campo": "import_pxs_limpio_append",
            "valor_anterior": None,
            "valor_nuevo": f"lineas_agregadas={len(rows_to_insert)} | import_job_id={import_job_id}",
            "usuario": usuario,
            "motivo": motivo_auditoria or "Importación de ampliación desde PXS limpio",
        }
    )
    return int(existing["id"])


def _create_new_pedido_group(
    pedido_data: dict[str, Any],
    lineas_data: list[dict[str, Any]],
    import_job_id: int,
    usuario: str | None = None,
    motivo_auditoria: str | None = None,
) -> int:
    """Crea un pedido nuevo delegando en el servicio sin autoasignacion."""
    if not lineas_data:
        raise ValueError("No se puede insertar un pedido sin líneas.")

    pedido_payload = dict(pedido_data or {})
    pedido_payload["import_job_id"] = import_job_id
    pedido_payload["origen_registro"] = "PXS_LIMPIO"
    pedido_payload["tipo_registro"] = "PEDIDO_NORMAL"
    pedido_payload["estado_operativo"] = "ACTIVO"

    observaciones = normalize_text(pedido_payload.get("observaciones")) or ""
    if "sin autoasignacion" not in observaciones.lower():
        extra = "Creado desde PXS limpio sin autoasignacion; requiere decision manual posterior."
        pedido_payload["observaciones"] = f"{observaciones} | {extra}".strip(" |")

    prepared_lineas: list[dict[str, Any]] = []
    for raw_linea in lineas_data:
        linea = dict(raw_linea or {})
        linea["cantidad_asignada"] = 0.0
        linea["cantidad_enviada"] = 0.0
        linea["cantidad_faltante"] = round(_safe_numeric(linea.get("cantidad_pedida"), 0.0), 6)
        prepared_lineas.append(linea)

    return create_new_pedido_without_autoassignment(
        pedido_data=pedido_payload,
        lineas_data=prepared_lineas,
        usuario=usuario,
        motivo_auditoria=motivo_auditoria or "Importación de nuevos pedidos desde PXS limpio",
    )


def _build_oferta_preview_for_groups(
    pedido_groups: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """
    Genera la vista de oferta por SKU para pedidos nuevos.
    No asigna nada; solo ayuda a la toma de decisión.
    """
    oferta_rows: list[dict[str, Any]] = []
    oferta_by_pedido: dict[str, list[dict[str, Any]]] = {}

    for group in pedido_groups:
        pedido = group.get("pedido", {})
        id_pedido = safe_str(pedido.get("id_pedido"))
        cliente = safe_str(pedido.get("cliente")) or safe_str((group.get("existing_pedido") or {}).get("cliente"))
        pedido_resolution = group.get("pedido_resolution") or {}
        if pedido_resolution.get("clasificacion_pedido") == "PEDIDO_NUEVO_INCOMPLETO":
            oferta_group = []
        else:
            oferta_group = get_oferta_para_pedido_nuevo_lineas(group.get("lineas", []))
        oferta_by_pedido[id_pedido] = oferta_group
        group["oferta_preview"] = oferta_group

        for row in oferta_group:
            oferta_rows.append(
                {
                    "id_pedido": id_pedido,
                    "cliente": cliente,
                    "fecha_produccion": pedido.get("fecha_produccion"),
                    "fecha_logistica": pedido.get("fecha_logistica"),
                    **row,
                }
            )

    return oferta_rows, oferta_by_pedido


# -----------------------------------------------------------------------------
# API pública del loader
# -----------------------------------------------------------------------------
def resolve_pxs_sheets(
    file_path: str | Path,
    sheet_names: list[str | int] | None = None,
) -> list[str | int]:
    """Devuelve las hojas candidatas para importación PXS."""
    candidates = _choose_candidate_sheets(file_path, sheet_names=sheet_names)
    valid: list[str | int] = []

    for sheet_name in candidates:
        if isinstance(sheet_name, str) and _is_sheet_explicitly_ignored(sheet_name):
            continue
        if _sheet_looks_like_pxs(file_path, sheet_name):
            valid.append(sheet_name)

    return valid


def detect_pxs_header_row(
    file_path: str | Path,
    sheet_name: str | int,
    search_rows: int = 15,
) -> int:
    raw_df = read_raw_sheet(file_path, sheet_name=sheet_name, max_rows=search_rows)
    for markers, min_matches in (
        (PXS_HEADER_MARKERS_STANDARD, 5),
        (PXS_HEADER_MARKERS_MINIMAL, 4),
        (PXS_HEADER_MARKERS, 4),
    ):
        try:
            return detect_header_row(
                raw_df,
                markers=markers,
                search_rows=search_rows,
                min_matches=min_matches,
            )
        except Exception:
            continue
    raise ValueError(f"No se detectó encabezado PXS válido en la hoja {sheet_name!r}.")


def read_pxs_sheet_dataframe(
    file_path: str | Path,
    sheet_name: str | int,
    header_row: int | None = None,
) -> pd.DataFrame:
    detected_header_row = (
        detect_pxs_header_row(file_path, sheet_name=sheet_name)
        if header_row is None
        else int(header_row)
    )

    raw_df = read_raw_sheet(file_path, sheet_name=sheet_name, max_rows=None)
    return dataframe_from_detected_header(raw_df, header_row=detected_header_row)


def normalize_pxs_sheet_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas a formato canónico para PXS limpio."""
    renamed = rename_columns_by_aliases(df, PXS_ALIASES, keep_unmapped=True)
    renamed = add_missing_columns(
        renamed,
        {column: None for column in REQUIRED_CANONICAL_COLUMNS + OPTIONAL_CANONICAL_COLUMNS},
    )
    ensure_required_columns(renamed, REQUIRED_CANONICAL_COLUMNS)
    return renamed


def _normalize_override_date(value: Any) -> str | None:
    return parse_excel_date(value)


def _apply_pedido_overrides_to_groups(
    pedido_groups: list[dict[str, Any]],
    pedido_overrides: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if not pedido_overrides:
        return pedido_groups

    updated_groups: list[dict[str, Any]] = []
    for original_group in pedido_groups:
        group = deepcopy(original_group)
        pedido = dict(group.get("pedido", {}))
        override = pedido_overrides.get(safe_str(pedido.get("id_pedido")), {})

        if override:
            cliente_override = normalize_text(override.get("cliente"))
            if cliente_override:
                pedido["cliente"] = cliente_override
                pedido["cliente_faltante"] = False

            for field in ("fecha_captura", "fecha_produccion", "fecha_logistica"):
                if field in override and normalize_text(override.get(field)):
                    normalized_date = _normalize_override_date(override.get(field))
                    if normalized_date:
                        pedido[field] = normalized_date

            if "estatus_venta" in override or "motivo_pausa" in override:
                estatus_venta, motivo_pausa = normalize_pause_fields(
                    override.get("estatus_venta", pedido.get("estatus_venta")),
                    override.get("motivo_pausa", pedido.get("motivo_pausa")),
                )
                pedido["estatus_venta"] = estatus_venta
                pedido["motivo_pausa"] = motivo_pausa

            observaciones_base = normalize_text(pedido.get("observaciones")) or ""
            observaciones_extra = normalize_text(override.get("observaciones"))
            if observaciones_extra:
                pedido["observaciones"] = f"{observaciones_base} | {observaciones_extra}".strip(" |")

            for linea in group.get("lineas", []):
                if pedido.get("fecha_captura"):
                    linea["fecha_captura"] = pedido.get("fecha_captura")
                if pedido.get("fecha_produccion"):
                    linea["fecha_produccion"] = pedido.get("fecha_produccion")
                if pedido.get("fecha_logistica"):
                    linea["fecha_logistica"] = pedido.get("fecha_logistica")
                if pedido.get("estatus_venta") is not None:
                    linea["estatus_venta"] = pedido.get("estatus_venta")
                if pedido.get("motivo_pausa") is not None:
                    linea["motivo_pausa"] = pedido.get("motivo_pausa")

        group["pedido"] = pedido
        updated_groups.append(group)

    return updated_groups


def analyze_pxs_limpio_file(
    file_path: str | Path,
    sheet_names: list[str | int] | None = None,
    excluded_client_tokens: set[str] | None = None,
    pedido_overrides: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Analiza un archivo PXS limpio sin insertarlo en la base."""
    path = ensure_path(file_path)
    candidate_sheets = resolve_pxs_sheets(path, sheet_names=sheet_names)

    if not candidate_sheets:
        raise ValueError("No se detectaron hojas válidas de PXS limpio en el archivo.")

    codigos_lookup = _load_codigos_lookup(path)
    codigos_detected = any(bool(section) for section in codigos_lookup.values())

    normalized_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    per_sheet_summary: list[dict[str, Any]] = []
    derived_from_sku_count = 0

    for sheet_name in candidate_sheets:
        detected_header_row = detect_pxs_header_row(path, sheet_name=sheet_name)
        df_raw = read_pxs_sheet_dataframe(
            path,
            sheet_name=sheet_name,
            header_row=detected_header_row,
        )
        df_norm = normalize_pxs_sheet_dataframe(df_raw)

        sheet_valid = 0
        sheet_rejected = 0
        sheet_derived_from_sku = 0

        for local_index, row in enumerate(
            dataframe_to_records(df_norm),
            start=detected_header_row + 2,
        ):
            normalized_row, reject_reason = _normalize_pxs_row(
                row=row,
                sheet_name=sheet_name,
                row_number=local_index,
                codigos_lookup=codigos_lookup,
                excluded_client_tokens=excluded_client_tokens or DEFAULT_EXCLUDED_CLIENT_TOKENS,
            )

            if normalized_row is None:
                sheet_rejected += 1
                rejected_rows.append(
                    {
                        "__sheet_name__": str(sheet_name),
                        "__row_number__": int(local_index),
                        "reason": reject_reason,
                        **row,
                    }
                )
                continue

            producto_excel = normalize_text(row.get("producto"))
            color_excel = normalize_text(row.get("color"))
            if codigos_detected and normalized_row.get("sku"):
                if normalized_row.get("producto") and normalized_row.get("color"):
                    if (
                        normalized_row.get("producto") != producto_excel
                        or normalized_row.get("color") != color_excel
                    ):
                        sheet_derived_from_sku += 1
                        derived_from_sku_count += 1

            sheet_valid += 1
            normalized_rows.append(normalized_row)

        per_sheet_summary.append(
            {
                "sheet_name": str(sheet_name),
                "header_row": int(detected_header_row),
                "valid_rows": int(sheet_valid),
                "rejected_rows": int(sheet_rejected),
                "rows_producto_color_from_sku": int(sheet_derived_from_sku),
            }
        )

    pedido_groups = _group_rows_into_pedidos(normalized_rows)
    pedido_groups = _apply_pedido_overrides_to_groups(pedido_groups, pedido_overrides=pedido_overrides)
    pedido_resolution_rows, line_resolution_rows = _classify_pedido_groups(pedido_groups)
    oferta_rows, oferta_by_pedido = _build_oferta_preview_for_groups(pedido_groups)

    classification_summary: dict[str, int] = {}
    for row in pedido_resolution_rows:
        key = safe_str(row.get("clasificacion_pedido")) or "SIN_CLASIFICACION"
        classification_summary[key] = classification_summary.get(key, 0) + 1

    normalized_df = pd.DataFrame(normalized_rows)
    rejected_df = pd.DataFrame(rejected_rows)
    oferta_df = pd.DataFrame(oferta_rows)
    pedido_resolution_df = pd.DataFrame(pedido_resolution_rows)
    line_resolution_df = pd.DataFrame(line_resolution_rows)

    return {
        "file_path": str(path),
        "file_name": path.name,
        "candidate_sheets": candidate_sheets,
        "codigos_detected": codigos_detected,
        "per_sheet_summary": per_sheet_summary,
        "normalized_rows": normalized_rows,
        "normalized_df": normalized_df,
        "rejected_rows": rejected_rows,
        "rejected_df": rejected_df,
        "pedido_groups": pedido_groups,
        "pedido_resolution_rows": pedido_resolution_rows,
        "pedido_resolution_df": pedido_resolution_df,
        "line_resolution_rows": line_resolution_rows,
        "line_resolution_df": line_resolution_df,
        "classification_summary": classification_summary,
        "oferta_rows": oferta_rows,
        "oferta_df": oferta_df,
        "oferta_by_pedido": oferta_by_pedido,
        "total_valid_rows": len(normalized_rows),
        "total_rejected_rows": len(rejected_rows),
        "total_pedidos_detected": len(pedido_groups),
        "rows_producto_color_from_sku": derived_from_sku_count,
        "pedido_overrides_applied": sorted((pedido_overrides or {}).keys()),
    }


def import_pxs_limpio_from_file(
    file_path: str | Path,
    sheet_names: list[str | int] | None = None,
    created_by: str | None = None,
    notes: str | None = None,
    save_copy_to_input: bool = True,
    archive_source_copy: bool = False,
    skip_existing: bool = True,
    excluded_client_tokens: set[str] | None = None,
    pedido_overrides: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Importa un archivo PXS limpio usando clasificación previa.

    Regla de esta fase:
    - crea pedidos nuevos completos
    - agrega líneas nuevas solo a pedidos activos
    - omite duplicados exactos
    - deja en revisión conflictos/cambios/incompletos
    """
    analysis = analyze_pxs_limpio_file(
        file_path=file_path,
        sheet_names=sheet_names,
        excluded_client_tokens=excluded_client_tokens or DEFAULT_EXCLUDED_CLIENT_TOKENS,
        pedido_overrides=pedido_overrides,
    )

    pedido_groups = analysis["pedido_groups"]
    normalized_rows = analysis["normalized_rows"]
    rejected_rows = analysis["rejected_rows"]

    if not pedido_groups:
        raise ValueError("No se detectaron pedidos válidos para importar.")

    ensure_directories()
    source_path = ensure_path(file_path)

    registered_file_path: str | Path | None = source_path
    saved_copy_path: Path | None = None
    archived_copy_path: Path | None = None

    if save_copy_to_input:
        saved_copy_path = _copy_file_to_dir(source_path, INPUT_DIR)
        registered_file_path = saved_copy_path

    import_job_id = start_import_job(
        file_name=source_path.name,
        file_path=registered_file_path,
        import_type="PXS_LIMPIO",
        created_by=created_by,
        notes=notes,
    )

    created_pedidos: list[dict[str, Any]] = []
    appended_activos: list[dict[str, Any]] = []
    skipped_existing: list[dict[str, Any]] = []
    unresolved_pedidos: list[dict[str, Any]] = []
    failed_pedidos: list[dict[str, Any]] = []
    inserted_line_rows = 0

    try:
        for group in pedido_groups:
            pedido_header = group["pedido"]
            lineas = group["lineas"]
            resolution = group.get("pedido_resolution") or {}
            id_pedido = safe_str(pedido_header.get("id_pedido"))
            accion = safe_str(resolution.get("accion_sugerida"))

            duplicate_count = sum(
                1
                for item in group.get("line_resolutions", [])
                if item.get("clasificacion") in {"DUPLICADO_EXACTO", "DUPLICADO_EN_CERRADO"}
            )
            if duplicate_count:
                skipped_existing.append(
                    {
                        "id_pedido": id_pedido,
                        "reason": "duplicado_en_archivo_o_sistema",
                        "lineas": duplicate_count,
                        "clasificacion_pedido": resolution.get("clasificacion_pedido"),
                    }
                )

            try:
                if accion == "CREAR_PEDIDO":
                    pedido_id = _create_new_pedido_group(
                        pedido_data=pedido_header,
                        lineas_data=lineas,
                        import_job_id=import_job_id,
                        usuario=created_by,
                        motivo_auditoria="Importación de nuevos pedidos desde PXS limpio",
                    )
                    created_pedidos.append(
                        {
                            "pedido_id": pedido_id,
                            "id_pedido": id_pedido,
                            "lineas": len(lineas),
                        }
                    )
                    inserted_line_rows += len(lineas)
                elif accion == "AGREGAR_A_PEDIDO_ACTIVO":
                    importable_lineas = [
                        linea
                        for linea, row_resolution in zip(lineas, group.get("line_resolutions", []), strict=False)
                        if row_resolution.get("clasificacion") == "AMPLIACION_PEDIDO_ACTIVO"
                    ]
                    if importable_lineas:
                        pedido_id = _append_lineas_to_existing_active_pedido(
                            pedido_data=pedido_header,
                            lineas_data=importable_lineas,
                            import_job_id=import_job_id,
                            usuario=created_by,
                            motivo_auditoria="Ampliación de pedido activo desde PXS limpio",
                        )
                        appended_activos.append(
                            {
                                "pedido_id": pedido_id,
                                "id_pedido": id_pedido,
                                "lineas_agregadas": len(importable_lineas),
                            }
                        )
                        inserted_line_rows += len(importable_lineas)
                elif accion == "OMITIR_DUPLICADO" and skip_existing:
                    continue
                else:
                    unresolved_pedidos.append(
                        {
                            "id_pedido": id_pedido,
                            "lineas": len(lineas),
                            "clasificacion_pedido": resolution.get("clasificacion_pedido"),
                            "accion_sugerida": accion,
                            "motivo": resolution.get("resumen_clasificacion"),
                        }
                    )
            except Exception as exc:
                failed_pedidos.append(
                    {
                        "id_pedido": id_pedido,
                        "reason": str(exc),
                        "lineas": len(lineas),
                        "clasificacion_pedido": resolution.get("clasificacion_pedido"),
                    }
                )

        total_rows = len(normalized_rows)
        error_rows = len(rejected_rows) + sum(item["lineas"] for item in failed_pedidos)

        if (created_pedidos or appended_activos) and not failed_pedidos and not unresolved_pedidos and not rejected_rows:
            final_status = "COMPLETADO"
        elif created_pedidos or appended_activos or skipped_existing:
            final_status = "COMPLETADO_CON_REVISION"
        else:
            final_status = "FALLIDO"

        summary_note = (
            f"Pedidos creados={len(created_pedidos)} | "
            f"Pedidos activos ampliados={len(appended_activos)} | "
            f"Pedidos duplicados/omitidos={len(skipped_existing)} | "
            f"Pedidos por revisar={len(unresolved_pedidos)} | "
            f"Pedidos fallidos={len(failed_pedidos)} | "
            f"Filas válidas={len(normalized_rows)} | "
            f"Filas rechazadas={len(rejected_rows)} | "
            f"Filas producto/color desde SKU={analysis.get('rows_producto_color_from_sku', 0)}"
        )
        final_notes = f"{notes} | {summary_note}" if notes else summary_note

        finish_import_job(
            import_job_id=import_job_id,
            status=final_status,
            total_rows=total_rows,
            inserted_rows=inserted_line_rows,
            updated_rows=0,
            error_rows=error_rows,
            notes=final_notes,
        )

        if archive_source_copy:
            archived_copy_path = _copy_file_to_dir(source_path, ARCHIVE_DIR)

        return {
            "import_job_id": import_job_id,
            "created_pedidos": created_pedidos,
            "appended_activos": appended_activos,
            "skipped_existing": skipped_existing,
            "unresolved_pedidos": unresolved_pedidos,
            "failed_pedidos": failed_pedidos,
            "inserted_line_rows": inserted_line_rows,
            "analysis": analysis,
            "saved_copy_path": str(saved_copy_path) if saved_copy_path else None,
            "archived_copy_path": str(archived_copy_path) if archived_copy_path else None,
        }

    except Exception as exc:
        finish_import_job(
            import_job_id=import_job_id,
            status="FALLIDO",
            total_rows=len(normalized_rows),
            inserted_rows=0,
            updated_rows=0,
            error_rows=len(normalized_rows),
            notes=f"Error en import_pxs_limpio_from_file: {exc}",
        )
        raise


__all__ = [
    "DEFAULT_EXCLUDED_CLIENT_TOKENS",
    "IGNORED_SHEET_NAMES",
    "PREFERRED_SHEET_NAMES",
    "PXS_ALIASES",
    "PXS_HEADER_MARKERS",
    "PXS_HEADER_MARKERS_STANDARD",
    "PXS_HEADER_MARKERS_MINIMAL",
    "analyze_pxs_limpio_file",
    "detect_pxs_header_row",
    "import_pxs_limpio_from_file",
    "normalize_pxs_sheet_dataframe",
    "read_pxs_sheet_dataframe",
    "resolve_pxs_sheets",
]
