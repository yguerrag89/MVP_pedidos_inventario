from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from wms_mvp.core.config import ARCHIVE_DIR, INPUT_DIR, ensure_directories
from wms_mvp.core.rules.pedidos_rules import (
    VALID_ESTATUS_VENTA,
    normalize_estatus_venta,
    normalize_motivo_pausa,
    aggregate_lineas_metrics,
    build_id_pedido,
    build_linea_from_import_row,
    build_pedido_from_import_row,
    normalize_text,
    validate_linea_quantities,
    validate_pedido_header,
)
from wms_mvp.core.services.entradas_service import finish_import_job, start_import_job
from wms_mvp.db.connection import transaction
from wms_mvp.db.repositories import auditoria_repository, pedidos_repository
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
    "Hoja 1",
    "2026",
]

IGNORED_SHEET_NAMES = {
    "PENDIENTES",
    "ESTATUS 2025",
    "CODIGOS",
    "CLIENTES",
    "CODIGO",
    "TABLA DE CODIGOS",
}

PEDIDOS_HEADER_MARKERS = [
    "DESCRIPCION",
    "PRODUCTO",
    "COLOR",
    "CLIENTE",
    "Serie documento",
    "DOCUMENTO",
    "FECHA CAPTURA",
    "FECHA PRODUCCION",
    "Cantidad",
    "EXISTENTE",
    "PEDIDO FALTANTE",
]

PEDIDOS_ALIASES = {
    "descripcion": [
        "DESCRIPCION",
        "Descripción",
        "descripcion",
    ],
    "producto": [
        "PRODUCTO",
        "producto",
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
    ],
    "fecha_captura": [
        "FECHA CAPTURA",
        "fecha_captura",
    ],
    "fecha_produccion": [
        "FECHA PRODUCCION",
        "FECHA PRODUCCIÓN",
        "fecha_produccion",
        "fecha producción",
        "FECHA PROD",
        "FECHA PRODUC",
        "PRODUCCION",
        "PRODUCCIÓN",
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
        "cantidad_enviada",
    ],
    "pedido_faltante": [
        "PEDIDO FALTANTE",
        "pedido_faltante",
        "faltante",
        "cantidad_no_enviada",
    ],
    "porcentaje_fuente": [
        "%",
        "PORCENTAJE",
        "porcentaje",
    ],
    "fecha_logistica": [
        "FECHA LOGISTICA",
        "FECHA LOGÍSTICA",
        "fecha_logistica",
        "FECHA DE ENTREGA",
        "fecha_de_entrega",
    ],
    "estatus_venta": [
        "ESTATUS VENTA",
        "estatus_venta",
    ],
    "motivo_pausa": [
        "MOTIVO PAUSA",
        "motivo_pausa",
        "MOTIVO DE PAUSA",
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
}

REQUIRED_CANONICAL_COLUMNS = [
    "descripcion",
    "cliente",
    "serie_documento",
    "documento",
]

OPTIONAL_CANONICAL_COLUMNS = [
    "producto",
    "color",
    "fecha_captura",
    "fecha_produccion",
    "cantidad",
    "existente",
    "pedido_faltante",
    "porcentaje_fuente",
    "fecha_logistica",
    "estatus_venta",
    "motivo_pausa",
    "proyecto",
    "sku",
]

DEFAULT_EXCLUDED_CLIENT_TOKENS = {
    "ENVIO",
    "FLETE",
    "SERVICIO",
}

SHEET_ROLE_ACTIVE = "ACTIVO"
SHEET_ROLE_ENVIADO = "ENVIADO_HISTORICO"

ACTIVE_SHEET_NAMES = {
    "HOJA 1",
}

ENVIADO_SHEET_NAMES = {
    "2026",
}

CODIGOS_SHEET_CANDIDATES = [
    "codigos",
    "Tabla de codigos",
    "Codigo ",
    "Codigo",
]


# -----------------------------------------------------------------------------
# Helpers internos
# -----------------------------------------------------------------------------
def _normalize_upper_text(value: Any) -> str | None:
    text = normalize_text(value)
    return text.upper() if text else None


def _normalize_documento_text(value: Any) -> str | None:
    """
    Normaliza documento preservando ceros a la izquierda cuando sea posible.
    """
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


def _canonicalize_estatus_venta(value: Any) -> str | None:
    """Tolerancia ligera para estatus comerciales comunes."""
    text = normalize_text(value)
    if not text:
        return None

    normalized_key = text.strip().upper().replace("_", " ")
    normalized_key = " ".join(normalized_key.split())

    mapping = {
        "ENTREGAR": "Entregar",
        "EN COBRO": "En cobro",
        "PAUSADO": "Pausa",
    }

    candidate = mapping.get(normalized_key, text)
    return normalize_estatus_venta(candidate)


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


def _classify_sheet_role(sheet_name: str | int) -> str:
    if isinstance(sheet_name, int):
        return SHEET_ROLE_ACTIVE

    normalized = _normalize_upper_text(sheet_name) or ""
    if normalized in ENVIADO_SHEET_NAMES:
        return SHEET_ROLE_ENVIADO
    return SHEET_ROLE_ACTIVE


def _choose_candidate_sheets(
    file_path: str | Path,
    sheet_names: list[str | int] | None = None,
) -> list[str | int]:
    """
    Resuelve las hojas candidatas para carga inicial.
    """
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
    existing_upper = {(_normalize_upper_text(name) or ""): name for name in workbook_sheets}

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


def _sheet_looks_like_pedidos(
    file_path: str | Path,
    sheet_name: str | int,
    search_rows: int = 15,
) -> bool:
    try:
        raw_df = read_raw_sheet(file_path, sheet_name=sheet_name, max_rows=search_rows)
        detect_header_row(
            raw_df,
            markers=PEDIDOS_HEADER_MARKERS,
            search_rows=search_rows,
            min_matches=5,
        )
        return True
    except Exception:
        return False


def _safe_numeric(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        numeric = float(value)
        if pd.isna(numeric):
            return default
        return numeric
    except (TypeError, ValueError):
        return default


def _infer_cantidad_pedida(
    cantidad: Any,
    existente: Any,
    pedido_faltante: Any,
) -> float:
    """
    Infere la cantidad pedida priorizando la coherencia operativa.
    """
    cantidad_num = _safe_numeric(cantidad, 0.0)
    existente_num = _safe_numeric(existente, 0.0)
    faltante_num = _safe_numeric(pedido_faltante, 0.0)

    inferred = max(cantidad_num, existente_num, existente_num + faltante_num)
    return inferred if inferred > 0 else 0.0


def _coerce_lookup_code(value: Any) -> int | None:
    text = trim_excel_number_text(value)
    if not text:
        return None
    text = str(text).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _resolve_codigos_sheet_name(file_path: str | Path) -> str | None:
    workbook_sheets = read_excel_sheet_names(file_path)
    existing_upper = {(_normalize_upper_text(name) or ""): name for name in workbook_sheets}

    for candidate in CODIGOS_SHEET_CANDIDATES:
        key = _normalize_upper_text(candidate)
        if key and key in existing_upper:
            return existing_upper[key]
    return None


def _load_codigos_lookup(file_path: str | Path) -> dict[str, dict[int, str]]:
    """
    Lee la hoja de códigos del mismo workbook y construye tablas maestras.
    """
    sheet_name = _resolve_codigos_sheet_name(file_path)
    empty_result = {
        "articulo": {},
        "modelo": {},
        "color": {},
        "filete": {},
    }
    if not sheet_name:
        return empty_result

    try:
        df_raw = read_raw_sheet(file_path, sheet_name=sheet_name, max_rows=None)
    except Exception:
        return empty_result

    lookups: dict[str, dict[int, str]] = {
        "articulo": {},
        "modelo": {},
        "color": {},
        "filete": {},
    }

    column_pairs = {
        "articulo": (0, 1),
        "modelo": (4, 5),
        "color": (7, 8),
        "filete": (10, 11),
    }

    for _, row in df_raw.iterrows():
        values = row.tolist()
        for lookup_name, (key_idx, value_idx) in column_pairs.items():
            if key_idx >= len(values) or value_idx >= len(values):
                continue

            code = _coerce_lookup_code(values[key_idx])
            label = normalize_text(values[value_idx])
            if code is None or not label:
                continue

            if code not in lookups[lookup_name]:
                lookups[lookup_name][code] = label.strip()

    return lookups


def _lookup_codigo_value(
    mapping: dict[int, str],
    code: int | None,
    *,
    exact: bool = True,
) -> str | None:
    if code is None or not mapping:
        return None

    if code in mapping:
        return mapping[code]

    if exact:
        return None

    candidates = [key for key in mapping if key <= code]
    if not candidates:
        return None
    return mapping[max(candidates)]


def _derive_producto_color_from_sku(
    sku: Any,
    codigos_lookup: dict[str, dict[int, str]] | None = None,
) -> tuple[str | None, str | None]:
    sku_text = trim_excel_number_text(sku)
    if not sku_text:
        return None, None

    sku_digits = "".join(ch for ch in str(sku_text) if ch.isdigit())
    if len(sku_digits) < 12:
        return None, None

    try:
        codigo_articulo = int(sku_digits[2:4])
        codigo_modelo = int(sku_digits[4:8])
        codigo_color = int(sku_digits[8:10])
        codigo_filete = int(sku_digits[10:12])
    except ValueError:
        return None, None

    lookups = codigos_lookup or {}
    articulo_desc = _lookup_codigo_value(lookups.get("articulo", {}), codigo_articulo, exact=False)
    modelo_desc = _lookup_codigo_value(lookups.get("modelo", {}), codigo_modelo, exact=True)
    color_desc = _lookup_codigo_value(lookups.get("color", {}), codigo_color, exact=False)
    filete_desc = _lookup_codigo_value(lookups.get("filete", {}), codigo_filete, exact=True)

    producto = " ".join(part for part in [articulo_desc, modelo_desc] if part).strip() or None
    color = " ".join(part for part in [color_desc, filete_desc] if part).strip() or None
    return producto, color


def _normalize_pedido_row(
    row: dict[str, Any],
    sheet_name: str | int,
    row_number: int,
    excluded_client_tokens: set[str] | None = None,
    codigos_lookup: dict[str, dict[int, str]] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """
    Normaliza una fila de pedidos al formato operativo.
    Devuelve (row_normalized, reason_if_rejected).
    """
    sheet_role = _classify_sheet_role(sheet_name)
    descripcion = normalize_text(row.get("descripcion"))
    cliente = normalize_text(row.get("cliente"))
    proyecto = normalize_text(row.get("proyecto"))
    serie_documento = _normalize_serie_documento(row.get("serie_documento"))
    documento = _normalize_documento_text(row.get("documento"))

    if not descripcion:
        return None, "descripcion_vacia"

    if not cliente:
        return None, "cliente_vacio"

    if not serie_documento or not documento:
        return None, "id_pedido_incompleto"

    excluded_tokens = {token.upper() for token in (excluded_client_tokens or set())}
    combined_text = " ".join(part for part in [cliente, proyecto] if normalize_text(part)).upper()
    if excluded_tokens and any(token in combined_text for token in excluded_tokens):
        return None, "cliente_o_proyecto_excluido"

    try:
        id_pedido = build_id_pedido(serie_documento, documento)
    except Exception:
        return None, "id_pedido_no_construible"

    fecha_captura = parse_excel_date(row.get("fecha_captura"))
    fecha_produccion = parse_excel_date(row.get("fecha_produccion"))
    fecha_logistica = parse_excel_date(row.get("fecha_logistica"))

    sku = extract_sku_from_text(descripcion)
    if not sku:
        sku = trim_excel_number_text(row.get("sku"))

    producto_from_sku, color_from_sku = _derive_producto_color_from_sku(
        sku,
        codigos_lookup=codigos_lookup,
    )
    producto = producto_from_sku or normalize_text(row.get("producto"))
    color = color_from_sku or normalize_text(row.get("color"))

    if not producto or not color:
        producto_split, color_split = split_producto_y_color(descripcion)
        producto = producto or producto_split
        color = color or color_split

    cantidad_pedida = _infer_cantidad_pedida(
        cantidad=row.get("cantidad"),
        existente=row.get("existente"),
        pedido_faltante=row.get("pedido_faltante"),
    )
    cantidad_existente = _safe_numeric(row.get("existente"), 0.0)
    cantidad_faltante_fuente = _safe_numeric(row.get("pedido_faltante"), 0.0)

    if cantidad_pedida <= 0 and cantidad_existente <= 0 and cantidad_faltante_fuente <= 0:
        return None, "linea_sin_cantidades_utiles"

    if sheet_role == SHEET_ROLE_ENVIADO:
        cantidad_enviada = cantidad_existente
        cantidad_asignada = cantidad_enviada
        cantidad_pedida = max(cantidad_pedida, cantidad_enviada + cantidad_faltante_fuente)
        fecha_envio = fecha_logistica or fecha_produccion or fecha_captura
        estado_operativo = "ENVIADO_PARCIAL" if cantidad_faltante_fuente > 0 else "ENVIADO_TOTAL"
    else:
        cantidad_enviada = 0.0
        cantidad_asignada = cantidad_existente
        fecha_envio = None
        estado_operativo = "ACTIVO"

    normalized = {
        "__sheet_name__": str(sheet_name),
        "__sheet_role__": sheet_role,
        "__row_number__": int(row_number),
        "id_pedido": id_pedido,
        "serie_documento": serie_documento,
        "documento": documento,
        "cliente": cliente,
        "fecha_captura": fecha_captura,
        "fecha_produccion": fecha_produccion,
        "fecha_logistica": fecha_logistica,
        "fecha_envio": fecha_envio,
        "estatus_venta": _canonicalize_estatus_venta(row.get("estatus_venta")),
        "motivo_pausa": normalize_motivo_pausa(row.get("motivo_pausa")),
        "descripcion": descripcion,
        "sku": sku,
        "producto": producto,
        "color": color,
        "cantidad_pedida": round(cantidad_pedida, 6),
        "cantidad_asignada": round(cantidad_asignada, 6),
        "cantidad_enviada": round(cantidad_enviada, 6),
        "origen_registro": "CARGA_INICIAL",
        "tipo_registro": "PEDIDO_NORMAL",
        "estado_operativo": estado_operativo,
        "observaciones": f"Importado de hoja={sheet_name}, fila={row_number}",
    }
    return normalized, None


def _group_rows_into_pedidos(normalized_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Agrupa filas normalizadas por id_pedido y rol de hoja."""
    buckets: dict[tuple[str, str], dict[str, Any]] = {}

    for row in sorted(
        normalized_rows,
        key=lambda r: (
            safe_str(r.get("id_pedido")),
            safe_str(r.get("__sheet_role__")),
            safe_str(r.get("__sheet_name__")),
            int(r.get("__row_number__", 0)),
        ),
    ):
        id_pedido = safe_str(row.get("id_pedido"))
        sheet_role = safe_str(row.get("__sheet_role__")) or SHEET_ROLE_ACTIVE
        if not id_pedido:
            continue

        bucket_key = (sheet_role, id_pedido)
        if bucket_key not in buckets:
            buckets[bucket_key] = {
                "pedido": {
                    "id_pedido": id_pedido,
                    "serie_documento": row.get("serie_documento"),
                    "documento": row.get("documento"),
                    "tipo_registro": row.get("tipo_registro", "PEDIDO_NORMAL"),
                    "origen_registro": row.get("origen_registro", "CARGA_INICIAL"),
                    "cliente": row.get("cliente"),
                    "fecha_captura": row.get("fecha_captura"),
                    "fecha_produccion": row.get("fecha_produccion"),
                    "fecha_logistica": row.get("fecha_logistica"),
                    "fecha_envio": row.get("fecha_envio"),
                    "estatus_venta": row.get("estatus_venta"),
                    "estado_operativo": row.get("estado_operativo", "ACTIVO"),
                    "motivo_pausa": row.get("motivo_pausa"),
                    "sheet_role": sheet_role,
                    "observaciones": row.get("observaciones"),
                },
                "lineas": [],
                "source_rows": [],
            }

        bucket = buckets[bucket_key]
        pedido_header = bucket["pedido"]

        for field_name in [
            "cliente",
            "fecha_captura",
            "fecha_produccion",
            "fecha_logistica",
            "fecha_envio",
            "estatus_venta",
            "motivo_pausa",
            "serie_documento",
            "documento",
            "observaciones",
        ]:
            if not pedido_header.get(field_name) and row.get(field_name):
                pedido_header[field_name] = row.get(field_name)

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
            }
        )
        bucket["source_rows"].append(row)

    grouped = list(buckets.values())
    for bucket in grouped:
        metrics = aggregate_lineas_metrics(bucket["lineas"])
        pedido_header = bucket["pedido"]
        pedido_header["porcentaje_asignacion"] = metrics["porcentaje_asignacion"]

        if pedido_header.get("sheet_role") == SHEET_ROLE_ENVIADO:
            has_pending = any(
                float(linea.get("cantidad_pedida", 0) or 0)
                > float(linea.get("cantidad_enviada", 0) or 0)
                for linea in bucket["lineas"]
            )
            pedido_header["tipo_cierre"] = "PARCIAL" if has_pending else "TOTAL"
            pedido_header["estado_operativo"] = "ENVIADO_PARCIAL" if has_pending else "ENVIADO_TOTAL"
        else:
            pedido_header["tipo_cierre"] = None
            pedido_header["estado_operativo"] = "ACTIVO"

    return grouped


def _insert_pedido_with_lineas(
    pedido_data: dict[str, Any],
    lineas_data: list[dict[str, Any]],
    import_job_id: int,
    usuario: str | None = None,
    motivo_auditoria: str | None = None,
) -> int:
    """Inserta un pedido activo con líneas de forma atómica."""
    if not lineas_data:
        raise ValueError("No se puede insertar un pedido sin líneas.")

    pedido_payload = build_pedido_from_import_row(pedido_data)
    pedido_payload["import_job_id"] = import_job_id
    pedido_payload["origen_registro"] = pedido_payload.get("origen_registro") or "CARGA_INICIAL"
    pedido_payload["tipo_registro"] = pedido_payload.get("tipo_registro") or "PEDIDO_NORMAL"
    pedido_payload["estado_operativo"] = pedido_payload.get("estado_operativo") or "ACTIVO"

    validate_pedido_header(
        id_pedido=pedido_payload["id_pedido"],
        cliente=pedido_payload["cliente"],
        estado_operativo=pedido_payload["estado_operativo"],
        estatus_venta=pedido_payload.get("estatus_venta"),
    )

    if pedidos_repository.pedido_exists(pedido_payload["id_pedido"]):
        raise ValueError(f"Ya existe el pedido '{pedido_payload['id_pedido']}'.")

    normalized_lineas: list[dict[str, Any]] = []
    for index, raw_linea in enumerate(lineas_data, start=1):
        linea_payload = build_linea_from_import_row(raw_linea, pedido_id=-1, line_no=index)
        validate_linea_quantities(
            cantidad_pedida=linea_payload["cantidad_pedida"],
            cantidad_asignada=linea_payload["cantidad_asignada"],
            allow_overassignment=False,
        )
        normalized_lineas.append(linea_payload)

    metrics = aggregate_lineas_metrics(normalized_lineas)
    pedido_payload["porcentaje_asignacion"] = metrics["porcentaje_asignacion"]

    with transaction() as conn:
        cursor = conn.execute(
            """
            INSERT INTO pedidos (
                id_pedido,
                serie_documento,
                documento,
                tipo_registro,
                origen_registro,
                import_job_id,
                pedido_origen_id,
                cliente,
                fecha_captura,
                fecha_produccion,
                fecha_logistica,
                estatus_venta,
                porcentaje_asignacion,
                estado_operativo,
                observaciones
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pedido_payload["id_pedido"],
                pedido_payload.get("serie_documento"),
                pedido_payload.get("documento"),
                pedido_payload.get("tipo_registro", "PEDIDO_NORMAL"),
                pedido_payload.get("origen_registro", "CARGA_INICIAL"),
                pedido_payload.get("import_job_id"),
                pedido_payload.get("pedido_origen_id"),
                pedido_payload.get("cliente"),
                pedido_payload.get("fecha_captura"),
                pedido_payload.get("fecha_produccion"),
                pedido_payload.get("fecha_logistica"),
                pedido_payload.get("estatus_venta"),
                pedido_payload.get("porcentaje_asignacion", 0.0),
                pedido_payload.get("estado_operativo", "ACTIVO"),
                pedido_payload.get("observaciones"),
            ),
        )
        pedido_id = int(cursor.lastrowid)

        for line_no, linea in enumerate(normalized_lineas, start=1):
            conn.execute(
                """
                INSERT INTO pedido_lineas (
                    pedido_id,
                    line_no,
                    sku,
                    descripcion,
                    producto,
                    color,
                    cantidad_pedida,
                    cantidad_asignada,
                    cantidad_faltante,
                    cantidad_enviada,
                    fecha_captura,
                    fecha_produccion,
                    fecha_logistica,
                    estatus_venta,
                    observaciones
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pedido_id,
                    line_no,
                    linea.get("sku"),
                    linea.get("descripcion"),
                    linea.get("producto"),
                    linea.get("color"),
                    linea.get("cantidad_pedida", 0.0),
                    linea.get("cantidad_asignada", 0.0),
                    linea.get("cantidad_faltante", 0.0),
                    linea.get("cantidad_enviada", 0.0),
                    linea.get("fecha_captura"),
                    linea.get("fecha_produccion"),
                    linea.get("fecha_logistica"),
                    linea.get("estatus_venta"),
                    linea.get("observaciones"),
                ),
            )

    auditoria_repository.create_auditoria_cambio(
        {
            "tabla": "pedidos",
            "registro_id": str(pedido_id),
            "campo": "__created__",
            "valor_anterior": None,
            "valor_nuevo": pedido_payload["id_pedido"],
            "usuario": usuario,
            "motivo": motivo_auditoria,
        }
    )

    return pedido_id


def _insert_historical_enviado_with_lineas(
    pedido_data: dict[str, Any],
    lineas_data: list[dict[str, Any]],
    import_job_id: int,
    usuario: str | None = None,
    motivo_auditoria: str | None = None,
) -> int:
    """
    Inserta un pedido histórico ya enviado durante la carga inicial.
    """
    if not lineas_data:
        raise ValueError("No se puede insertar un pedido enviado sin líneas.")

    pedido_payload = build_pedido_from_import_row(pedido_data)
    pedido_payload["import_job_id"] = import_job_id
    pedido_payload["origen_registro"] = pedido_payload.get("origen_registro") or "CARGA_INICIAL"
    pedido_payload["tipo_registro"] = pedido_payload.get("tipo_registro") or "PEDIDO_NORMAL"
    pedido_payload["estado_operativo"] = pedido_payload.get("estado_operativo") or "ENVIADO_TOTAL"

    tipo_cierre = normalize_text(pedido_data.get("tipo_cierre")) or "TOTAL"
    tipo_cierre = tipo_cierre.upper().strip()
    if tipo_cierre not in {"TOTAL", "PARCIAL"}:
        raise ValueError(f"Tipo de cierre inválido para histórico: {tipo_cierre}")

    fecha_envio = (
        normalize_text(pedido_data.get("fecha_envio"))
        or pedido_payload.get("fecha_logistica")
        or pedido_payload.get("fecha_produccion")
        or pedido_payload.get("fecha_captura")
    )
    if not fecha_envio:
        raise ValueError("El pedido enviado histórico requiere fecha_envio o una fecha equivalente.")

    validate_pedido_header(
        id_pedido=pedido_payload["id_pedido"],
        cliente=pedido_payload["cliente"],
        estado_operativo=pedido_payload["estado_operativo"],
        estatus_venta=pedido_payload.get("estatus_venta"),
    )

    if pedidos_repository.pedido_exists(pedido_payload["id_pedido"]):
        raise ValueError(f"Ya existe el pedido '{pedido_payload['id_pedido']}'.")

    normalized_lineas: list[dict[str, Any]] = []
    for index, raw_linea in enumerate(lineas_data, start=1):
        linea_payload = build_linea_from_import_row(raw_linea, pedido_id=-1, line_no=index)
        validate_linea_quantities(
            cantidad_pedida=linea_payload["cantidad_pedida"],
            cantidad_asignada=linea_payload["cantidad_asignada"],
            allow_overassignment=False,
        )
        normalized_lineas.append(linea_payload)

    metrics = aggregate_lineas_metrics(normalized_lineas)
    pedido_payload["porcentaje_asignacion"] = metrics["porcentaje_asignacion"]

    with transaction() as conn:
        cursor_pedido = conn.execute(
            """
            INSERT INTO pedidos (
                id_pedido,
                serie_documento,
                documento,
                tipo_registro,
                origen_registro,
                import_job_id,
                pedido_origen_id,
                cliente,
                fecha_captura,
                fecha_produccion,
                fecha_logistica,
                estatus_venta,
                porcentaje_asignacion,
                estado_operativo,
                observaciones
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pedido_payload["id_pedido"],
                pedido_payload.get("serie_documento"),
                pedido_payload.get("documento"),
                pedido_payload.get("tipo_registro", "PEDIDO_NORMAL"),
                pedido_payload.get("origen_registro", "CARGA_INICIAL"),
                pedido_payload.get("import_job_id"),
                pedido_payload.get("pedido_origen_id"),
                pedido_payload.get("cliente"),
                pedido_payload.get("fecha_captura"),
                pedido_payload.get("fecha_produccion"),
                pedido_payload.get("fecha_logistica"),
                pedido_payload.get("estatus_venta"),
                pedido_payload.get("porcentaje_asignacion", 0.0),
                pedido_payload.get("estado_operativo", "ENVIADO_TOTAL"),
                pedido_payload.get("observaciones"),
            ),
        )
        pedido_id = int(cursor_pedido.lastrowid)

        inserted_lineas: list[dict[str, Any]] = []
        for line_no, linea in enumerate(normalized_lineas, start=1):
            cursor_linea = conn.execute(
                """
                INSERT INTO pedido_lineas (
                    pedido_id,
                    line_no,
                    sku,
                    descripcion,
                    producto,
                    color,
                    cantidad_pedida,
                    cantidad_asignada,
                    cantidad_faltante,
                    cantidad_enviada,
                    fecha_captura,
                    fecha_produccion,
                    fecha_logistica,
                    estatus_venta,
                    observaciones
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pedido_id,
                    line_no,
                    linea.get("sku"),
                    linea.get("descripcion"),
                    linea.get("producto"),
                    linea.get("color"),
                    linea.get("cantidad_pedida", 0.0),
                    linea.get("cantidad_asignada", 0.0),
                    linea.get("cantidad_faltante", 0.0),
                    linea.get("cantidad_enviada", 0.0),
                    linea.get("fecha_captura"),
                    linea.get("fecha_produccion"),
                    linea.get("fecha_logistica"),
                    linea.get("estatus_venta"),
                    linea.get("observaciones"),
                ),
            )
            inserted_lineas.append({**linea, "id": int(cursor_linea.lastrowid)})

        cursor_cierre = conn.execute(
            """
            INSERT INTO pedido_cierres (
                pedido_id,
                tipo_cierre,
                fecha_envio,
                usuario,
                observacion
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                pedido_id,
                tipo_cierre,
                fecha_envio,
                usuario,
                pedido_payload.get("observaciones"),
            ),
        )
        pedido_cierre_id = int(cursor_cierre.lastrowid)

        for linea in inserted_lineas:
            cantidad_pedida_original = float(linea.get("cantidad_pedida") or 0)
            cantidad_asignada_al_cierre = float(linea.get("cantidad_asignada") or 0)
            cantidad_enviada = float(linea.get("cantidad_enviada") or 0)
            cantidad_no_enviada = cantidad_pedida_original - cantidad_enviada
            if cantidad_no_enviada < 0:
                cantidad_no_enviada = 0.0

            conn.execute(
                """
                INSERT INTO pedido_linea_cierres (
                    pedido_cierre_id,
                    pedido_linea_id,
                    sku,
                    descripcion,
                    producto,
                    color,
                    fecha_produccion,
                    cantidad_pedida_original,
                    cantidad_asignada_al_cierre,
                    cantidad_enviada,
                    cantidad_no_enviada
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pedido_cierre_id,
                    linea["id"],
                    linea.get("sku"),
                    linea.get("descripcion"),
                    linea.get("producto"),
                    linea.get("color"),
                    linea.get("fecha_produccion"),
                    cantidad_pedida_original,
                    cantidad_asignada_al_cierre,
                    cantidad_enviada,
                    cantidad_no_enviada,
                ),
            )

    auditoria_repository.create_auditoria_cambio(
        {
            "tabla": "pedidos",
            "registro_id": str(pedido_id),
            "campo": "__created__",
            "valor_anterior": None,
            "valor_nuevo": pedido_payload["id_pedido"],
            "usuario": usuario,
            "motivo": motivo_auditoria or "Carga inicial histórica de pedidos enviados",
        }
    )

    auditoria_repository.create_auditoria_cambio(
        {
            "tabla": "pedido_cierres",
            "registro_id": str(pedido_id),
            "campo": "__created__",
            "valor_anterior": None,
            "valor_nuevo": f"tipo_cierre={tipo_cierre}, fecha_envio={fecha_envio}",
            "usuario": usuario,
            "motivo": motivo_auditoria or "Carga inicial histórica de pedidos enviados",
        }
    )

    return pedido_id


# -----------------------------------------------------------------------------
# API pública del loader
# -----------------------------------------------------------------------------
def resolve_pedidos_sheets(
    file_path: str | Path,
    sheet_names: list[str | int] | None = None,
) -> list[str | int]:
    """Devuelve las hojas candidatas de pedidos para la carga inicial."""
    path = ensure_path(file_path)
    workbook_sheets = read_excel_sheet_names(path) if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"} else []
    present_allowed = [name for name in workbook_sheets if (_normalize_upper_text(name) or "") in {"HOJA 1", "2026"}]
    if not present_allowed and workbook_sheets:
        raise ValueError("El archivo debe incluir al menos una de las hojas requeridas: Hoja 1, 2026")

    candidates = _choose_candidate_sheets(path, sheet_names=sheet_names)
    valid: list[str | int] = []

    for sheet_name in candidates:
        if isinstance(sheet_name, str) and _is_sheet_explicitly_ignored(sheet_name):
            continue
        if _sheet_looks_like_pedidos(path, sheet_name):
            valid.append(sheet_name)

    return valid


def detect_pedidos_header_row(
    file_path: str | Path,
    sheet_name: str | int,
    search_rows: int = 15,
) -> int:
    raw_df = read_raw_sheet(file_path, sheet_name=sheet_name, max_rows=search_rows)
    return detect_header_row(
        raw_df,
        markers=PEDIDOS_HEADER_MARKERS,
        search_rows=search_rows,
        min_matches=5,
    )


def read_pedidos_sheet_dataframe(
    file_path: str | Path,
    sheet_name: str | int,
    header_row: int | None = None,
) -> pd.DataFrame:
    detected_header_row = (
        detect_pedidos_header_row(file_path, sheet_name=sheet_name)
        if header_row is None
        else int(header_row)
    )

    raw_df = read_raw_sheet(file_path, sheet_name=sheet_name, max_rows=None)
    return dataframe_from_detected_header(raw_df, header_row=detected_header_row)


def normalize_pedidos_sheet_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas a formato canónico para pedidos."""
    renamed = rename_columns_by_aliases(df, PEDIDOS_ALIASES, keep_unmapped=True)
    renamed = add_missing_columns(
        renamed,
        {column: None for column in REQUIRED_CANONICAL_COLUMNS + OPTIONAL_CANONICAL_COLUMNS},
    )
    ensure_required_columns(renamed, REQUIRED_CANONICAL_COLUMNS)
    return renamed


def analyze_carga_inicial_file(
    file_path: str | Path,
    sheet_names: list[str | int] | None = None,
    excluded_client_tokens: set[str] | None = None,
) -> dict[str, Any]:
    """
    Analiza el archivo de carga inicial sin insertarlo en la base.

    Reglas importantes:
    - Hoja 1 -> pedidos activos
    - 2026   -> pedidos enviados históricos
    - producto/color se derivan primero desde SKU + hoja codigos
    - fecha_produccion entra como campo operativo de primer nivel
    """
    path = ensure_path(file_path)
    workbook_sheets = read_excel_sheet_names(path) if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"} else []
    present_allowed_sheets = [name for name in workbook_sheets if (_normalize_upper_text(name) or "") in {"HOJA 1", "2026"}]
    ignored_present_sheets = [name for name in workbook_sheets if _is_sheet_explicitly_ignored(name)]
    disallowed_present_sheets = [name for name in workbook_sheets if name not in present_allowed_sheets and name not in ignored_present_sheets]
    try:
        candidate_sheets = resolve_pedidos_sheets(path, sheet_names=sheet_names)
    except ValueError as exc:
        raise ValueError("No se detectaron hojas válidas de pedidos en el archivo.") from exc

    if not candidate_sheets:
        raise ValueError("No se detectaron hojas válidas de pedidos en el archivo.")

    codigos_lookup = _load_codigos_lookup(path)
    normalized_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    per_sheet_summary: list[dict[str, Any]] = []

    for sheet_name in candidate_sheets:
        detected_header_row = detect_pedidos_header_row(path, sheet_name=sheet_name)
        df_raw = read_pedidos_sheet_dataframe(path, sheet_name=sheet_name, header_row=detected_header_row)
        df_norm = normalize_pedidos_sheet_dataframe(df_raw)

        sheet_valid = 0
        sheet_rejected = 0
        sheet_role = _classify_sheet_role(sheet_name)

        for local_index, row in enumerate(dataframe_to_records(df_norm), start=detected_header_row + 2):
            normalized_row, reject_reason = _normalize_pedido_row(
                row=row,
                sheet_name=sheet_name,
                row_number=local_index,
                excluded_client_tokens=excluded_client_tokens or DEFAULT_EXCLUDED_CLIENT_TOKENS,
                codigos_lookup=codigos_lookup,
            )

            if normalized_row is None:
                sheet_rejected += 1
                rejected_rows.append(
                    {
                        "__sheet_name__": str(sheet_name),
                        "__sheet_role__": sheet_role,
                        "__row_number__": int(local_index),
                        "reason": reject_reason,
                        **row,
                    }
                )
                continue

            sheet_valid += 1
            normalized_rows.append(normalized_row)

        per_sheet_summary.append(
            {
                "sheet_name": str(sheet_name),
                "sheet_role": sheet_role,
                "header_row": int(detected_header_row),
                "valid_rows": int(sheet_valid),
                "rejected_rows": int(sheet_rejected),
            }
        )

    pedido_groups = _group_rows_into_pedidos(normalized_rows)
    normalized_df = pd.DataFrame(normalized_rows)
    rejected_df = pd.DataFrame(rejected_rows)

    total_pedidos_activos = sum(
        1 for group in pedido_groups if group["pedido"].get("sheet_role") == SHEET_ROLE_ACTIVE
    )
    total_pedidos_enviados = sum(
        1 for group in pedido_groups if group["pedido"].get("sheet_role") == SHEET_ROLE_ENVIADO
    )

    return {
        "file_path": str(path),
        "file_name": path.name,
        "candidate_sheets": candidate_sheets,
        "present_allowed_sheets": present_allowed_sheets,
        "ignored_present_sheets": ignored_present_sheets,
        "disallowed_present_sheets": disallowed_present_sheets,
        "per_sheet_summary": per_sheet_summary,
        "codigos_lookup": codigos_lookup,
        "normalized_rows": normalized_rows,
        "normalized_df": normalized_df,
        "rejected_rows": rejected_rows,
        "rejected_df": rejected_df,
        "pedido_groups": pedido_groups,
        "total_valid_rows": len(normalized_rows),
        "total_rejected_rows": len(rejected_rows),
        "total_pedidos_detected": len(pedido_groups),
        "total_pedidos_activos_detected": total_pedidos_activos,
        "total_pedidos_enviados_detected": total_pedidos_enviados,
    }


def import_carga_inicial_from_file(
    file_path: str | Path,
    sheet_names: list[str | int] | None = None,
    created_by: str | None = None,
    notes: str | None = None,
    save_copy_to_input: bool = True,
    archive_source_copy: bool = False,
    skip_existing: bool = True,
    excluded_client_tokens: set[str] | None = None,
) -> dict[str, Any]:
    """
    Importa pedidos de carga inicial desde el archivo definido.
    """
    analysis = analyze_carga_inicial_file(
        file_path=file_path,
        sheet_names=sheet_names,
        excluded_client_tokens=excluded_client_tokens or DEFAULT_EXCLUDED_CLIENT_TOKENS,
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
        import_type="CARGA_INICIAL",
        created_by=created_by,
        notes=notes,
    )

    created_pedidos: list[dict[str, Any]] = []
    skipped_existing: list[dict[str, Any]] = []
    failed_pedidos: list[dict[str, Any]] = []
    inserted_line_rows = 0

    try:
        for group in pedido_groups:
            pedido_header = group["pedido"]
            lineas = group["lineas"]
            id_pedido = safe_str(pedido_header.get("id_pedido"))
            sheet_role = safe_str(pedido_header.get("sheet_role")) or SHEET_ROLE_ACTIVE

            if skip_existing and pedidos_repository.pedido_exists(id_pedido):
                skipped_existing.append(
                    {
                        "id_pedido": id_pedido,
                        "reason": "pedido_ya_existente",
                        "lineas": len(lineas),
                        "sheet_role": sheet_role,
                    }
                )
                continue

            try:
                if sheet_role == SHEET_ROLE_ENVIADO:
                    pedido_id = _insert_historical_enviado_with_lineas(
                        pedido_data=pedido_header,
                        lineas_data=lineas,
                        import_job_id=import_job_id,
                        usuario=created_by,
                        motivo_auditoria="Carga inicial histórica desde hoja 2026",
                    )
                else:
                    pedido_id = _insert_pedido_with_lineas(
                        pedido_data=pedido_header,
                        lineas_data=lineas,
                        import_job_id=import_job_id,
                        usuario=created_by,
                        motivo_auditoria="Carga inicial desde hoja de pedidos activos",
                    )

                created_pedidos.append(
                    {
                        "pedido_id": pedido_id,
                        "id_pedido": id_pedido,
                        "lineas": len(lineas),
                        "sheet_role": sheet_role,
                        "estado_operativo": pedido_header.get("estado_operativo"),
                    }
                )
                inserted_line_rows += len(lineas)

            except Exception as exc:
                failed_pedidos.append(
                    {
                        "id_pedido": id_pedido,
                        "reason": str(exc),
                        "lineas": len(lineas),
                        "sheet_role": sheet_role,
                    }
                )

        total_rows = len(normalized_rows)
        error_rows = len(rejected_rows) + sum(item["lineas"] for item in failed_pedidos)

        if created_pedidos and not failed_pedidos and not rejected_rows:
            final_status = "COMPLETADO"
        elif created_pedidos:
            final_status = "COMPLETADO_CON_REVISION"
        else:
            final_status = "FALLIDO"

        created_activos = sum(1 for item in created_pedidos if item.get("sheet_role") == SHEET_ROLE_ACTIVE)
        created_enviados = sum(1 for item in created_pedidos if item.get("sheet_role") == SHEET_ROLE_ENVIADO)

        summary_note = (
            f"Pedidos activos creados={created_activos} | "
            f"Pedidos enviados históricos creados={created_enviados} | "
            f"Pedidos duplicados omitidos={len(skipped_existing)} | "
            f"Pedidos fallidos={len(failed_pedidos)} | "
            f"Filas válidas={len(normalized_rows)} | "
            f"Filas rechazadas={len(rejected_rows)}"
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
            "skipped_existing": skipped_existing,
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
            notes=f"Error en import_carga_inicial_from_file: {exc}",
        )
        raise


__all__ = [
    "ACTIVE_SHEET_NAMES",
    "CODIGOS_SHEET_CANDIDATES",
    "DEFAULT_EXCLUDED_CLIENT_TOKENS",
    "ENVIADO_SHEET_NAMES",
    "IGNORED_SHEET_NAMES",
    "PEDIDOS_ALIASES",
    "PEDIDOS_HEADER_MARKERS",
    "PREFERRED_SHEET_NAMES",
    "SHEET_ROLE_ACTIVE",
    "SHEET_ROLE_ENVIADO",
    "analyze_carga_inicial_file",
    "detect_pedidos_header_row",
    "import_carga_inicial_from_file",
    "normalize_pedidos_sheet_dataframe",
    "read_pedidos_sheet_dataframe",
    "resolve_pedidos_sheets",
]
