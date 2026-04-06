from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from wms_mvp.core.config import ARCHIVE_DIR, INPUT_DIR, ensure_directories
from wms_mvp.core.rules.asignacion_rules import infer_tipo_sugerencia_from_text
from wms_mvp.core.rules.pedidos_rules import normalize_text, to_float
from wms_mvp.core.services.entradas_service import (
    detect_cortes_from_rows,
    finish_import_job,
    load_staging_rows_for_corte,
    register_detected_cortes,
    start_import_job,
)
from wms_mvp.etl.excel_utils import (
    add_missing_columns,
    dataframe_from_detected_header,
    dataframe_to_records,
    detect_header_row,
    drop_fully_empty_rows,
    ensure_path,
    ensure_required_columns,
    explode_multiline_column,
    extract_sku_from_text,
    parse_excel_date,
    parse_excel_time,
    read_excel_sheet_names,
    read_raw_sheet,
    rename_columns_by_aliases,
    safe_str,
    split_producto_y_color,
    trim_excel_number_text,
)


PREFERRED_SHEET_NAMES = [
    "Entrada Inv.",
    "Entrada Inv",
    "Entradas",
    "Entrada",
    "Entradas Almacen",
]

ENTRADAS_HEADER_MARKERS = [
    "FECHA",
    "CLAVE",
    "PIEZAS",
    "HORA",
    "ALMACEN Y/O PEDIDO (SERIE Y FOLIO)",
    "NOMBRE DEL PRODUCTO",
]

ENTRADAS_ALIASES = {
    "fecha": ["FECHA", "Fecha", "fecha"],
    "hora": ["HORA", "Hora", "hora"],
    "clave": ["CLAVE", "SKU", "sku", "clave", "codigo", "código"],
    "piezas": ["PIEZAS", "Cantidad", "cantidad", "Piezas", "pzas", "pz"],
    "almacen_y_o_pedido": [
        "ALMACEN Y/O PEDIDO (SERIE Y FOLIO)",
        "ALMACEN Y/O PEDIDO",
        "ALMACEN Y PEDIDO",
        "almacen_y_o_pedido",
        "destino",
        "pedido",
        "sugerencia_pedido",
        "sugerencia destino",
        "sugerencia_destino",
    ],
    "nombre_del_producto": [
        "NOMBRE DEL PRODUCTO",
        "Descripcion",
        "Descripción",
        "descripcion",
        "producto",
        "nombre_del_producto",
        "nombre producto",
    ],
    "cajas": ["CAJAS", "Cajas", "cajas"],
}

REQUIRED_CANONICAL_COLUMNS = ["fecha", "hora", "piezas"]
OPTIONAL_CANONICAL_COLUMNS = [
    "clave",
    "almacen_y_o_pedido",
    "nombre_del_producto",
    "cajas",
]

MINIMAL_ENTRY_COLUMNS = [
    "sku",
    "descripcion",
    "cantidad",
    "fecha",
    "hora",
    "texto_original_destino",
]


# -----------------------------------------------------------------------------
# Helpers internos
# -----------------------------------------------------------------------------
def _choose_preferred_sheet(sheet_names: list[str]) -> str | int:
    """Elige la hoja más probable para entradas."""
    if not sheet_names:
        raise ValueError("El archivo no contiene hojas disponibles.")

    normalized_map = {
        normalize_text(sheet_name).lower(): sheet_name
        for sheet_name in sheet_names
        if normalize_text(sheet_name)
    }

    for preferred in PREFERRED_SHEET_NAMES:
        key = normalize_text(preferred)
        if key and key.lower() in normalized_map:
            return normalized_map[key.lower()]

    return sheet_names[0]



def _copy_file_to_dir(source_path: str | Path, target_dir: Path) -> Path:
    """Copia un archivo a un directorio evitando sobrescrituras."""
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



def _looks_empty_operational_row(row: dict[str, Any]) -> bool:
    """Determina si una fila normalizada está realmente vacía."""
    fields = [
        row.get("fecha"),
        row.get("hora"),
        row.get("clave"),
        row.get("nombre_del_producto"),
        row.get("piezas"),
        row.get("almacen_y_o_pedido"),
    ]
    return all(normalize_text(value) in (None, "") for value in fields)



def _build_review_reasons(
    *,
    sku: str | None,
    cantidad: float,
    fecha: str | None,
    hora: str | None,
    sugerencia_texto: str | None,
    tipo_sugerencia: str | None,
) -> list[str]:
    reasons: list[str] = []
    if not normalize_text(sku):
        reasons.append("sin_sku")
    if cantidad <= 0:
        reasons.append("cantidad_invalida")
    if not normalize_text(fecha):
        reasons.append("fecha_invalida")
    if not normalize_text(hora):
        reasons.append("hora_invalida")
    if not normalize_text(sugerencia_texto):
        reasons.append("sugerencia_sin_dato")
    if normalize_text(tipo_sugerencia) in {"AMBIGUA"}:
        reasons.append("sugerencia_ambigua")
    return reasons



def _normalize_entrada_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normaliza una fila al formato canónico de entradas."""
    fecha = parse_excel_date(row.get("fecha"))
    hora = parse_excel_time(row.get("hora"))

    clave_raw = row.get("clave")
    descripcion_raw = row.get("nombre_del_producto")
    destino_raw = row.get("almacen_y_o_pedido")

    sku = trim_excel_number_text(clave_raw)
    if not sku:
        sku = extract_sku_from_text(descripcion_raw)
    if not sku:
        sku = extract_sku_from_text(destino_raw)

    descripcion = normalize_text(descripcion_raw)
    if not descripcion:
        descripcion = normalize_text(destino_raw)

    producto, color = split_producto_y_color(descripcion)

    piezas = to_float(row.get("piezas"), 0.0)
    cajas = None
    if row.get("cajas") is not None:
        cajas = to_float(row.get("cajas"), 0.0)

    sugerencia_destino_texto = normalize_text(destino_raw)
    tipo_sugerencia = infer_tipo_sugerencia_from_text(sugerencia_destino_texto)
    review_reasons = _build_review_reasons(
        sku=sku,
        cantidad=piezas,
        fecha=fecha,
        hora=hora,
        sugerencia_texto=sugerencia_destino_texto,
        tipo_sugerencia=tipo_sugerencia,
    )

    normalized = {
        "fecha": fecha,
        "hora": hora,
        "clave": sku,
        "nombre_del_producto": descripcion,
        "piezas": piezas,
        "almacen_y_o_pedido": sugerencia_destino_texto,
        "cajas": cajas,
        # Campos adicionales para trazabilidad y staging.
        "sku": sku,
        "descripcion": descripcion,
        "cantidad": piezas,
        "producto": producto,
        "color": color,
        "texto_original_destino": safe_str(destino_raw),
        "pedido_sugerido_texto": sugerencia_destino_texto,
        "sugerencia_destino_texto": sugerencia_destino_texto,
        "tipo_sugerencia": tipo_sugerencia,
        "estado_revision": "PENDIENTE" if review_reasons else "RESUELTA",
        "review_reasons": review_reasons,
    }
    return normalized



def _normalize_entradas_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza un DataFrame de entradas al formato canónico."""
    renamed = rename_columns_by_aliases(df, ENTRADAS_ALIASES, keep_unmapped=True)
    renamed = add_missing_columns(
        renamed,
        {column: None for column in REQUIRED_CANONICAL_COLUMNS + OPTIONAL_CANONICAL_COLUMNS},
    )
    ensure_required_columns(renamed, REQUIRED_CANONICAL_COLUMNS)

    # En algunos archivos la clave o la descripción pueden venir multilínea.
    if "clave" in renamed.columns:
        renamed = explode_multiline_column(renamed, "clave")
    if "nombre_del_producto" in renamed.columns:
        renamed = explode_multiline_column(renamed, "nombre_del_producto")

    normalized_rows = [_normalize_entrada_row(row) for row in renamed.to_dict(orient="records")]
    normalized_df = pd.DataFrame(normalized_rows)
    if normalized_df.empty:
        return normalized_df

    normalized_df = drop_fully_empty_rows(normalized_df)
    normalized_df = normalized_df[
        ~normalized_df.apply(lambda row: _looks_empty_operational_row(row.to_dict()), axis=1)
    ].reset_index(drop=True)

    for text_col in [
        "nombre_del_producto",
        "almacen_y_o_pedido",
        "descripcion",
        "producto",
        "color",
        "texto_original_destino",
        "pedido_sugerido_texto",
        "sugerencia_destino_texto",
        "tipo_sugerencia",
        "estado_revision",
    ]:
        if text_col in normalized_df.columns:
            normalized_df[text_col] = normalized_df[text_col].apply(safe_str)

    if "review_reasons" in normalized_df.columns:
        normalized_df["review_reasons"] = normalized_df["review_reasons"].apply(
            lambda value: value if isinstance(value, list) else []
        )

    return normalized_df


def _build_rows_by_status(normalized_df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Separa las filas para el flujo operativo del MVP."""
    if normalized_df is None or normalized_df.empty:
        return {"rows_valid": [], "rows_review": [], "rows_error": []}

    review_mask = normalized_df.get("estado_revision", pd.Series(dtype=str)).fillna("") == "PENDIENTE"
    rows_review = dataframe_to_records(normalized_df[review_mask].copy()) if review_mask.any() else []
    rows_valid = dataframe_to_records(normalized_df[~review_mask].copy()) if (~review_mask).any() else []
    return {
        "rows_valid": rows_valid,
        "rows_review": rows_review,
        # En este MVP las filas con problema viajan en la misma bandeja de revisión.
        "rows_error": rows_review,
    }



def _build_summary_by_sku(normalized_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Resume una entrada concreta por SKU para la bandeja operativa."""
    if normalized_df is None or normalized_df.empty:
        return []

    working = normalized_df.copy()
    working["cantidad"] = working.get("cantidad", 0).apply(lambda value: to_float(value, 0.0))
    working["requiere_revision"] = working.get("estado_revision", "").fillna("") == "PENDIENTE"
    for time_col in ["fecha", "hora"]:
        if time_col in working.columns:
            working[time_col] = working[time_col].apply(lambda value: normalize_text(value) or "")

    summary = (
        working.groupby(["sku", "producto", "descripcion", "tipo_sugerencia"], dropna=False)
        .agg(
            cantidad_recibida=("cantidad", "sum"),
            filas=("cantidad", "size"),
            filas_en_revision=("requiere_revision", "sum"),
            fecha_min=("fecha", "min"),
            fecha_max=("fecha", "max"),
            hora_min=("hora", "min"),
            hora_max=("hora", "max"),
        )
        .reset_index()
    )
    summary["cantidad_recibida"] = summary["cantidad_recibida"].round(4)
    summary["estado_operativo"] = summary["filas_en_revision"].apply(
        lambda value: "EN_REVISION" if int(value or 0) > 0 else "LISTA"
    )
    return dataframe_to_records(summary)



def _build_preview_metadata(path: Path, resolved_sheet: str | int, detected_header_row: int) -> dict[str, Any]:
    return {
        "file_path": str(path),
        "file_name": path.name,
        "sheet_name": resolved_sheet,
        "header_row": detected_header_row,
    }



def _build_preview_result(
    *,
    path: Path,
    resolved_sheet: str | int,
    detected_header_row: int,
    raw_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    detected_cortes: list[dict[str, Any]],
) -> dict[str, Any]:
    rows_by_status = _build_rows_by_status(normalized_df)
    preview_rows = dataframe_to_records(normalized_df.head(200)) if normalized_df is not None and not normalized_df.empty else []
    review_rows = rows_by_status["rows_review"]
    valid_rows = rows_by_status["rows_valid"]
    rows_with_review = len(review_rows)
    minimal_columns_present = [column for column in MINIMAL_ENTRY_COLUMNS if column in normalized_df.columns]
    missing_minimal_columns = [column for column in MINIMAL_ENTRY_COLUMNS if column not in minimal_columns_present]

    return {
        **_build_preview_metadata(path, resolved_sheet, detected_header_row),
        "raw_df": raw_df,
        "normalized_df": normalized_df,
        "normalized_rows": dataframe_to_records(normalized_df),
        "preview_rows": preview_rows,
        "rows_valid": valid_rows,
        "rows_review": review_rows,
        "rows_error": rows_by_status["rows_error"],
        "detected_cortes": detected_cortes,
        "summary_by_sku": _build_summary_by_sku(normalized_df),
        "total_rows": len(normalized_df),
        "rows_valid_count": len(valid_rows),
        "rows_review_count": rows_with_review,
        "rows_error_count": len(rows_by_status["rows_error"]),
        "total_cortes": len(detected_cortes),
        "minimal_columns_present": minimal_columns_present,
        "missing_minimal_columns": missing_minimal_columns,
        "ready_for_recepcion": len(normalized_df) > 0,
    }



# -----------------------------------------------------------------------------
# API pública del loader
# -----------------------------------------------------------------------------
def resolve_entradas_sheet(file_path: str | Path, sheet_name: str | int | None = None) -> str | int:
    """Resuelve qué hoja usar para el archivo de entradas."""
    path = ensure_path(file_path)
    if path.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
        return 0

    if sheet_name is not None:
        return sheet_name

    sheet_names = read_excel_sheet_names(path)
    return _choose_preferred_sheet(sheet_names)



def detect_entradas_header_row(
    file_path: str | Path,
    sheet_name: str | int | None = None,
    search_rows: int = 15,
) -> int:
    """Detecta la fila de encabezado más probable."""
    resolved_sheet = resolve_entradas_sheet(file_path, sheet_name=sheet_name)
    raw_df = read_raw_sheet(file_path, sheet_name=resolved_sheet, max_rows=search_rows)
    return detect_header_row(
        raw_df,
        markers=ENTRADAS_HEADER_MARKERS,
        search_rows=search_rows,
        min_matches=3,
    )



def read_entradas_dataframe(
    file_path: str | Path,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
) -> pd.DataFrame:
    """Lee el archivo de entradas y devuelve el DataFrame con header real."""
    resolved_sheet = resolve_entradas_sheet(file_path, sheet_name=sheet_name)
    detected_header_row = (
        detect_entradas_header_row(file_path, sheet_name=resolved_sheet)
        if header_row is None
        else int(header_row)
    )

    raw_df = read_raw_sheet(file_path, sheet_name=resolved_sheet, max_rows=None)
    return dataframe_from_detected_header(raw_df, header_row=detected_header_row)



def normalize_entradas_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza un DataFrame ya leído al formato canónico de entradas."""
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "fecha",
                "hora",
                "clave",
                "nombre_del_producto",
                "piezas",
                "almacen_y_o_pedido",
                "cajas",
                "sku",
                "descripcion",
                "cantidad",
                "producto",
                "color",
                "texto_original_destino",
                "pedido_sugerido_texto",
                "sugerencia_destino_texto",
                "tipo_sugerencia",
                "estado_revision",
                "review_reasons",
            ]
        )

    return _normalize_entradas_dataframe(df)



def analyze_entradas_file(
    file_path: str | Path,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
) -> dict[str, Any]:
    """Analiza un archivo de entradas sin importarlo aún."""
    path = ensure_path(file_path)
    resolved_sheet = resolve_entradas_sheet(path, sheet_name=sheet_name)
    detected_header_row = (
        detect_entradas_header_row(path, sheet_name=resolved_sheet)
        if header_row is None
        else int(header_row)
    )

    raw_df = read_entradas_dataframe(path, sheet_name=resolved_sheet, header_row=detected_header_row)
    normalized_df = normalize_entradas_dataframe(raw_df)
    detected_cortes = detect_cortes_from_rows(dataframe_to_records(normalized_df))
    return _build_preview_result(
        path=path,
        resolved_sheet=resolved_sheet,
        detected_header_row=detected_header_row,
        raw_df=raw_df,
        normalized_df=normalized_df,
        detected_cortes=detected_cortes,
    )



def analyze_entrada_concreta(
    file_path: str | Path,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
) -> dict[str, Any]:
    """Flujo principal del MVP: analiza una entrada concreta del día o de una corrida."""
    analysis = analyze_entradas_file(file_path=file_path, sheet_name=sheet_name, header_row=header_row)
    analysis["flow_mode"] = "ENTRADA_CONCRETA"
    analysis["internal_cortes_supported"] = True
    return analysis



def build_entrada_concreta_preview(
    file_path: str | Path,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
) -> dict[str, Any]:
    """Alias explícito para la UI del MVP."""
    return analyze_entrada_concreta(file_path=file_path, sheet_name=sheet_name, header_row=header_row)



def detect_cortes_from_file(
    file_path: str | Path,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
) -> list[dict[str, Any]]:
    """Atajo para obtener solo los cortes detectados desde un archivo."""
    analysis = analyze_entradas_file(file_path=file_path, sheet_name=sheet_name, header_row=header_row)
    return analysis["detected_cortes"]



def get_corte_preview(
    file_path: str | Path,
    selected_corte_index: int,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
) -> dict[str, Any]:
    """Devuelve una vista previa de un corte detectado sin importarlo."""
    analysis = analyze_entradas_file(file_path=file_path, sheet_name=sheet_name, header_row=header_row)
    detected_cortes = analysis["detected_cortes"]

    if selected_corte_index < 0 or selected_corte_index >= len(detected_cortes):
        raise IndexError(
            f"selected_corte_index fuera de rango: {selected_corte_index}. Cortes detectados: {len(detected_cortes)}"
        )

    corte = detected_cortes[selected_corte_index]
    rows = corte.get("rows", [])
    preview_df = pd.DataFrame(rows)
    return {
        "corte": corte,
        "preview_df": preview_df,
        "rows": rows,
        "analysis": analysis,
    }



def find_corte_index(
    detected_cortes: list[dict[str, Any]],
    fecha_corte: str,
    hora_corte_normalizada: str,
) -> int:
    """Busca el índice de un corte detectado por fecha y hora."""
    normalized_fecha = normalize_text(fecha_corte)
    normalized_hora = normalize_text(hora_corte_normalizada)

    for index, corte in enumerate(detected_cortes):
        if (
            normalize_text(corte.get("fecha_corte")) == normalized_fecha
            and normalize_text(corte.get("hora_corte_normalizada")) == normalized_hora
        ):
            return index

    raise ValueError(
        "No se encontró un corte con "
        f"fecha_corte={fecha_corte} y hora_corte_normalizada={hora_corte_normalizada}."
    )



def import_selected_corte_from_file(
    file_path: str | Path,
    selected_corte_index: int,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
    created_by: str | None = None,
    notes: str | None = None,
    save_copy_to_input: bool = True,
    archive_source_copy: bool = False,
) -> dict[str, Any]:
    """Importa a staging un único corte detectado desde el archivo de entradas."""
    analysis = analyze_entradas_file(file_path=file_path, sheet_name=sheet_name, header_row=header_row)

    normalized_rows = analysis["normalized_rows"]
    detected_cortes = analysis["detected_cortes"]

    if not normalized_rows:
        raise ValueError("El archivo no contiene filas útiles para importar.")
    if not detected_cortes:
        raise ValueError("No se detectaron cortes válidos. Revisa especialmente las columnas fecha y hora.")
    if selected_corte_index < 0 or selected_corte_index >= len(detected_cortes):
        raise IndexError(
            f"selected_corte_index fuera de rango: {selected_corte_index}. Cortes detectados: {len(detected_cortes)}"
        )

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
        import_type="ENTRADAS_PRODUCCION",
        created_by=created_by,
        notes=notes,
    )

    try:
        registered_cortes = register_detected_cortes(import_job_id, normalized_rows)
        selected_registered_corte = registered_cortes[selected_corte_index]
        selected_rows = selected_registered_corte.get("rows", [])

        inserted_rows = load_staging_rows_for_corte(
            corte_id=int(selected_registered_corte["id"]),
            raw_rows=selected_rows,
        )

        total_rows = len(normalized_rows)
        error_rows = max(total_rows - inserted_rows, 0)

        finish_import_job(
            import_job_id=import_job_id,
            status="COMPLETADO_CON_REVISION",
            total_rows=total_rows,
            inserted_rows=inserted_rows,
            updated_rows=0,
            error_rows=error_rows,
            notes=((notes or "").strip() + " | " if (notes or "").strip() else "")
            + f"Archivo analizado con {len(detected_cortes)} cortes detectados. "
            + f"Se cargó a staging solo el corte_id={selected_registered_corte['id']} "
            + f"(índice detectado={selected_corte_index}).",
        )

        if archive_source_copy:
            archived_copy_path = _copy_file_to_dir(source_path, ARCHIVE_DIR)

        return {
            "import_job_id": import_job_id,
            "selected_corte_id": int(selected_registered_corte["id"]),
            "selected_corte_index": int(selected_corte_index),
            "selected_corte_fecha": selected_registered_corte.get("fecha_corte"),
            "selected_corte_hora": selected_registered_corte.get("hora_corte_normalizada"),
            "inserted_rows": inserted_rows,
            "total_rows": total_rows,
            "detected_cortes_count": len(detected_cortes),
            "saved_copy_path": str(saved_copy_path) if saved_copy_path else None,
            "archived_copy_path": str(archived_copy_path) if archived_copy_path else None,
            "analysis": analysis,
        }

    except Exception as exc:
        finish_import_job(
            import_job_id=import_job_id,
            status="FALLIDO",
            total_rows=len(normalized_rows),
            inserted_rows=0,
            updated_rows=0,
            error_rows=len(normalized_rows),
            notes=f"Error en import_selected_corte_from_file: {exc}",
        )
        raise



def import_entrada_concreta_from_file(
    file_path: str | Path,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
    created_by: str | None = None,
    notes: str | None = None,
    save_copy_to_input: bool = True,
    archive_source_copy: bool = False,
) -> dict[str, Any]:
    """Importa una entrada concreta completa, manteniendo cortes solo como detalle técnico interno."""
    analysis = analyze_entrada_concreta(file_path=file_path, sheet_name=sheet_name, header_row=header_row)
    normalized_rows = analysis["normalized_rows"]
    detected_cortes = analysis["detected_cortes"]

    if not normalized_rows:
        raise ValueError("El archivo no contiene filas útiles para importar.")

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
        import_type="ENTRADAS_PRODUCCION",
        created_by=created_by,
        notes=notes,
    )

    try:
        registered_cortes = register_detected_cortes(import_job_id, normalized_rows)
        inserted_rows = 0
        loaded_cortes: list[dict[str, Any]] = []
        for corte in registered_cortes:
            raw_rows = corte.get("rows", [])
            inserted_for_corte = load_staging_rows_for_corte(
                corte_id=int(corte["id"]),
                raw_rows=raw_rows,
            )
            inserted_rows += inserted_for_corte
            loaded_cortes.append(
                {
                    "corte_id": int(corte["id"]),
                    "fecha_corte": corte.get("fecha_corte"),
                    "hora_corte_normalizada": corte.get("hora_corte_normalizada"),
                    "rows_detected": len(raw_rows),
                    "rows_loaded": inserted_for_corte,
                }
            )

        total_rows = len(normalized_rows)
        error_rows = max(total_rows - inserted_rows, 0)
        review_rows = analysis.get("rows_review_count", 0)
        status = "COMPLETADO_CON_REVISION" if review_rows or error_rows else "COMPLETADO"

        finish_import_job(
            import_job_id=import_job_id,
            status=status,
            total_rows=total_rows,
            inserted_rows=inserted_rows,
            updated_rows=0,
            error_rows=error_rows,
            notes=((notes or "").strip() + " | " if (notes or "").strip() else "")
            + "Entrada concreta importada como una sola recepción operativa del MVP. "
            + f"Cortes internos detectados={len(detected_cortes)}. Filas en revisión={review_rows}.",
        )

        if archive_source_copy:
            archived_copy_path = _copy_file_to_dir(source_path, ARCHIVE_DIR)

        return {
            "import_job_id": import_job_id,
            "import_scope": "ENTRADA_CONCRETA",
            "total_rows": total_rows,
            "inserted_rows": inserted_rows,
            "error_rows": error_rows,
            "rows_review_count": review_rows,
            "internal_cortes_count": len(detected_cortes),
            "loaded_cortes": loaded_cortes,
            "saved_copy_path": str(saved_copy_path) if saved_copy_path else None,
            "archived_copy_path": str(archived_copy_path) if archived_copy_path else None,
            "analysis": analysis,
        }
    except Exception as exc:
        finish_import_job(
            import_job_id=import_job_id,
            status="FALLIDO",
            total_rows=len(normalized_rows),
            inserted_rows=0,
            updated_rows=0,
            error_rows=len(normalized_rows),
            notes=f"Error en import_entrada_concreta_from_file: {exc}",
        )
        raise



def import_corte_by_fecha_hora(
    file_path: str | Path,
    fecha_corte: str,
    hora_corte_normalizada: str,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
    created_by: str | None = None,
    notes: str | None = None,
    save_copy_to_input: bool = True,
    archive_source_copy: bool = False,
) -> dict[str, Any]:
    """Importa un corte localizándolo por fecha y hora normalizada."""
    analysis = analyze_entradas_file(file_path=file_path, sheet_name=sheet_name, header_row=header_row)
    corte_index = find_corte_index(
        detected_cortes=analysis["detected_cortes"],
        fecha_corte=fecha_corte,
        hora_corte_normalizada=hora_corte_normalizada,
    )

    return import_selected_corte_from_file(
        file_path=file_path,
        selected_corte_index=corte_index,
        sheet_name=analysis["sheet_name"],
        header_row=analysis["header_row"],
        created_by=created_by,
        notes=notes,
        save_copy_to_input=save_copy_to_input,
        archive_source_copy=archive_source_copy,
    )


__all__ = [
    "ENTRADAS_ALIASES",
    "ENTRADAS_HEADER_MARKERS",
    "OPTIONAL_CANONICAL_COLUMNS",
    "PREFERRED_SHEET_NAMES",
    "REQUIRED_CANONICAL_COLUMNS",
    "analyze_entrada_concreta",
    "analyze_entradas_file",
    "build_entrada_concreta_preview",
    "detect_cortes_from_file",
    "detect_entradas_header_row",
    "find_corte_index",
    "get_corte_preview",
    "import_corte_by_fecha_hora",
    "import_entrada_concreta_from_file",
    "import_selected_corte_from_file",
    "normalize_entradas_dataframe",
    "read_entradas_dataframe",
    "resolve_entradas_sheet",
]
