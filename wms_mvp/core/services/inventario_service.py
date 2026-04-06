from __future__ import annotations

import io
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from wms_mvp.core.config import INPUT_DIR, PROCESSED_DIR, ensure_directories
from wms_mvp.core.rules.inventario_rules import (
    REQUIRED_INVENTARIO_COLUMNS,
    canonical_inventory_columns,
    missing_required_inventory_columns,
    normalize_descripcion,
    normalize_header,
    normalize_manual_adjustment_kind,
    normalize_sku,
    normalize_text,
    parse_required_date_to_iso,
    parse_stock_value,
    validate_manual_adjustment_quantity,
)
from wms_mvp.core.services.entradas_service import finish_import_job, start_import_job
from wms_mvp.db.repositories import asignaciones_repository
from wms_mvp.db.connection import fetch_all, fetch_one, transaction


INVENTARIO_IMPORT_NOTE_TAG = "[INVENTARIO_INICIAL]"
INVENTARIO_UPDATE_NOTE_TAG = "[INVENTARIO_ACTUALIZACION]"
INVENTARIO_CUT_NOTE_TAG = "[CORTE_FISICO_SIMPLE]"
INVENTARIO_NOTE_TAGS = (INVENTARIO_IMPORT_NOTE_TAG, INVENTARIO_UPDATE_NOTE_TAG, INVENTARIO_CUT_NOTE_TAG)
DEFAULT_INVENTARIO_SHEET = "Hoja1"
PREVIEW_VALID_MAX_ROWS = 3000
PREVIEW_REVIEW_MAX_ROWS = 3000


class InventarioImportError(RuntimeError):
    """Error controlado del flujo de importación de inventario."""


# -----------------------------------------------------------------------------
# Helpers generales
# -----------------------------------------------------------------------------
def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        try:
            if pd.isna(value):
                return default
        except Exception:
            pass
        return float(value)

    text = str(value).strip().replace(",", "")
    if not text:
        return default
    try:
        return float(text)
    except (TypeError, ValueError):
        return default



def _safe_int(value: Any, default: int = 0) -> int:
    return int(round(_safe_float(value, default)))



def _safe_now() -> str:
    return datetime.now().isoformat(timespec="seconds")



def _normalize_import_filename(file_name: str) -> str:
    raw = Path(file_name or "inventario_inicial.xlsx").name
    if not raw:
        return "inventario_inicial.xlsx"
    return raw



def _unique_output_path(base_dir: Path, filename: str) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    candidate = base_dir / Path(filename).name
    if not candidate.exists():
        return candidate
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = candidate.stem
    suffix = candidate.suffix
    return base_dir / f"{stem}_{stamp}{suffix}"



def _sanitize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    renamed = df.copy()
    renamed.columns = [str(col) for col in renamed.columns]
    mapping = canonical_inventory_columns(list(renamed.columns))
    renamed = renamed.rename(columns=mapping)

    cols_to_drop = [col for col in renamed.columns if normalize_header(col).startswith("unnamed")]
    if cols_to_drop:
        renamed = renamed.drop(columns=cols_to_drop, errors="ignore")
    return renamed



def _pick_inventory_sheet(sheet_to_columns: dict[str, list[Any]]) -> str | None:
    best_sheet: str | None = None
    best_score = -1

    for sheet_name, columns in sheet_to_columns.items():
        score = len(REQUIRED_INVENTARIO_COLUMNS) - len(missing_required_inventory_columns(columns))
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
        if score == len(REQUIRED_INVENTARIO_COLUMNS) and sheet_name == DEFAULT_INVENTARIO_SHEET:
            return sheet_name
    return best_sheet




def _ensure_inventario_movimientos_schema(conn: Any) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'inventario_movimientos'"
    ).fetchone()
    create_sql = "" if row is None else str((row["sql"] if isinstance(row, dict) else row[0]) or "")
    required_tokens = (
        "LIBERACION_ASIGNADO_POR_CIERRE",
        "PEDIDO_CIERRE",
    )

    create_target_sql = """
        CREATE TABLE inventario_movimientos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            movimiento_tipo TEXT NOT NULL
                CHECK (movimiento_tipo IN (
                    'ENTRADA_A_STOCK',
                    'ASIGNACION_DESDE_STOCK',
                    'DESASIGNACION_A_STOCK',
                    'SALIDA_DESDE_ASIGNADO',
                    'SALIDA_DESDE_STOCK',
                    'AJUSTE_MANUAL',
                    'CARGA_INICIAL',
                    'LIBERACION_ASIGNADO_POR_CIERRE'
                )),
            afecta_total REAL NOT NULL DEFAULT 0,
            afecta_stock_libre REAL NOT NULL DEFAULT 0,
            afecta_stock_asignado REAL NOT NULL DEFAULT 0,
            referencia_tipo TEXT
                CHECK (referencia_tipo IN (
                    'CORTE_ENTRADA',
                    'ENTRADA_STAGING',
                    'ENTRADA_DECISION',
                    'ASIGNACION',
                    'PEDIDO_CIERRE',
                    'PEDIDO',
                    'AJUSTE_MANUAL',
                    'CARGA_INICIAL'
                )),
            referencia_id INTEGER,
            observacion TEXT,
            created_by TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """
    if not create_sql:
        conn.execute(create_target_sql)
        return
    if all(token in create_sql for token in required_tokens):
        return

    conn.execute("ALTER TABLE inventario_movimientos RENAME TO inventario_movimientos_old")
    conn.execute(create_target_sql)
    conn.execute(
        """
        INSERT INTO inventario_movimientos (
            id, sku, movimiento_tipo, afecta_total, afecta_stock_libre, afecta_stock_asignado,
            referencia_tipo, referencia_id, observacion, created_by, created_at
        )
        SELECT
            id, sku, movimiento_tipo, afecta_total, afecta_stock_libre, afecta_stock_asignado,
            referencia_tipo, referencia_id, observacion, created_by, created_at
        FROM inventario_movimientos_old
        """
    )
    conn.execute("DROP TABLE inventario_movimientos_old")


def _get_excel_file(file_bytes: bytes) -> pd.ExcelFile:
    try:
        return pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as exc:  # pragma: no cover - depende del engine disponible
        raise InventarioImportError(f"No fue posible leer el archivo Excel: {exc}") from exc


def ensure_inventory_phase1_schema() -> None:
    ensure_directories()
    migrations = (
        ("inventario_sku", "fecha_actualizacion", "ALTER TABLE inventario_sku ADD COLUMN fecha_actualizacion TEXT"),
        ("inventario_sku", "import_job_id", "ALTER TABLE inventario_sku ADD COLUMN import_job_id INTEGER"),
        (
            "inventario_sku",
            "origen_carga",
            "ALTER TABLE inventario_sku ADD COLUMN origen_carga TEXT DEFAULT 'DESCONOCIDO'",
        ),
        (
            "inventario_sku",
            "stock_base_libre",
            "ALTER TABLE inventario_sku ADD COLUMN stock_base_libre REAL NOT NULL DEFAULT 0",
        ),
        (
            "inventario_sku",
            "stock_base_asignado",
            "ALTER TABLE inventario_sku ADD COLUMN stock_base_asignado REAL NOT NULL DEFAULT 0",
        ),
    )

    with transaction() as conn:
        _ensure_inventario_movimientos_schema(conn)
        for table_name, column_name, sql in migrations:
            columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            existing = {str((row.get('name') if isinstance(row, dict) else row[1])) for row in columns}
            if column_name not in existing:
                conn.execute(sql)

        conn.execute(
            """
            UPDATE inventario_sku
            SET stock_base_libre = COALESCE(stock_libre, 0),
                stock_base_asignado = COALESCE(stock_asignado, 0)
            WHERE COALESCE(stock_base_libre, 0) = 0
              AND COALESCE(stock_base_asignado, 0) = 0
              AND (COALESCE(stock_libre, 0) <> 0 OR COALESCE(stock_asignado, 0) <> 0)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inventario_cortes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                import_job_id INTEGER,
                tipo_corte TEXT NOT NULL DEFAULT 'CONTEO_PARCIAL'
                    CHECK (tipo_corte IN ('CONTEO_PARCIAL', 'CONTEO_TOTAL')),
                archivo_nombre TEXT,
                hoja_nombre TEXT,
                fecha_corte TEXT NOT NULL,
                total_filas INTEGER NOT NULL DEFAULT 0,
                filas_aplicadas INTEGER NOT NULL DEFAULT 0,
                filas_revision INTEGER NOT NULL DEFAULT 0,
                diferencia_total REAL NOT NULL DEFAULT 0,
                estado TEXT NOT NULL DEFAULT 'APLICADO'
                    CHECK (estado IN ('PREVIEW', 'APLICADO', 'APLICADO_CON_REVISION', 'ANULADO')),
                usuario TEXT,
                observacion TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inventario_corte_detalle (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                corte_id INTEGER NOT NULL,
                row_num INTEGER,
                sku TEXT,
                descripcion TEXT,
                stock_sistema REAL NOT NULL DEFAULT 0,
                stock_asignado REAL NOT NULL DEFAULT 0,
                stock_contado REAL NOT NULL DEFAULT 0,
                diferencia REAL NOT NULL DEFAULT 0,
                fecha_actualizacion TEXT,
                estado_detalle TEXT NOT NULL DEFAULT 'APLICADO'
                    CHECK (estado_detalle IN ('APLICADO', 'REVISION')),
                motivo_revision TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (corte_id) REFERENCES inventario_cortes(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inventario_cortes_fecha ON inventario_cortes (fecha_corte, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inventario_corte_detalle_corte ON inventario_corte_detalle (corte_id, estado_detalle)"
        )


# -----------------------------------------------------------------------------
# Lectura y preview del archivo
# -----------------------------------------------------------------------------
def get_inventory_excel_sheets(file_bytes: bytes) -> list[str]:
    xls = _get_excel_file(file_bytes)
    return list(xls.sheet_names)



def suggest_inventory_sheet(file_bytes: bytes) -> str | None:
    with _get_excel_file(file_bytes) as xls:
        sheet_to_columns: dict[str, list[Any]] = {}
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, nrows=5)
            sheet_to_columns[sheet_name] = list(df.columns)
    return _pick_inventory_sheet(sheet_to_columns)



def read_inventory_sheet(file_bytes: bytes, sheet_name: str | None = None) -> pd.DataFrame:
    with _get_excel_file(file_bytes) as xls:
        target_sheet = sheet_name or _pick_inventory_sheet({name: list(pd.read_excel(xls, sheet_name=name, nrows=5).columns) for name in xls.sheet_names})
        if not target_sheet:
            raise InventarioImportError("No se pudo identificar una hoja válida para inventario.")
        try:
            raw_df = pd.read_excel(xls, sheet_name=target_sheet)
        except Exception as exc:
            raise InventarioImportError(f"No fue posible leer la hoja '{target_sheet}': {exc}") from exc

    df = _sanitize_dataframe_columns(raw_df)
    missing = [col for col in REQUIRED_INVENTARIO_COLUMNS if col not in df.columns]
    if missing:
        raise InventarioImportError(
            "La hoja seleccionada no contiene las columnas requeridas: " + ", ".join(missing)
        )
    return df



def build_initial_inventory_preview(
    *,
    file_name: str,
    file_bytes: bytes,
    sheet_name: str | None = None,
) -> dict[str, Any]:
    ensure_inventory_phase1_schema()

    suggested_sheet = suggest_inventory_sheet(file_bytes)
    target_sheet = sheet_name or suggested_sheet or DEFAULT_INVENTARIO_SHEET
    df = read_inventory_sheet(file_bytes, sheet_name=target_sheet)

    working = df.copy()
    working = working[[col for col in working.columns if col in REQUIRED_INVENTARIO_COLUMNS]].copy()
    working = working.iloc[: PREVIEW_VALID_MAX_ROWS + PREVIEW_REVIEW_MAX_ROWS].copy()

    provisional_rows: list[dict[str, Any]] = []
    sku_counter: dict[str, int] = {}

    for idx, row in working.iterrows():
        row_num = int(idx) + 2  # encabezado en fila 1
        sku = normalize_sku(row.get("SKU"))
        descripcion = normalize_descripcion(row.get("DESCRIPCION"))
        stock, stock_was_blank = parse_stock_value(row.get("STOCK"), treat_blank_as_zero=True)
        fecha_actualizacion = parse_required_date_to_iso(row.get("FECHA_ACTUALIZACION"))

        reasons: list[str] = []
        if not sku:
            reasons.append("SKU vacío")
        if stock is None:
            reasons.append("STOCK inválido")
        if not fecha_actualizacion:
            reasons.append("FECHA_ACTUALIZACION obligatoria o inválida")

        provisional = {
            "row_num": row_num,
            "sku": sku,
            "descripcion": descripcion,
            "stock": round(float(stock or 0), 6),
            "fecha_actualizacion": fecha_actualizacion,
            "stock_vacio_convertido_a_cero": bool(stock_was_blank),
            "motivos_revision": reasons[:],
        }
        provisional_rows.append(provisional)

        if sku:
            sku_counter[sku] = sku_counter.get(sku, 0) + 1

    duplicated_skus = {sku for sku, count in sku_counter.items() if count > 1}

    valid_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []

    for row in provisional_rows:
        reasons = list(row.get("motivos_revision", []))
        sku = row.get("sku")
        if sku and sku in duplicated_skus:
            reasons.append("SKU repetido en el archivo")

        export_row = {
            "Fila": row["row_num"],
            "SKU": row["sku"],
            "Descripción": row["descripcion"],
            "Stock": round(_safe_float(row.get("stock"), 0.0), 6),
            "Fecha actualización": row.get("fecha_actualizacion") or "",
            "Nota": "STOCK vacío convertido a 0" if row.get("stock_vacio_convertido_a_cero") else "",
        }

        if reasons:
            export_row["Motivo revisión"] = " | ".join(sorted(set(reasons)))
            review_rows.append(export_row)
        else:
            valid_rows.append(export_row)

    total_stock = round(sum(_safe_float(item.get("Stock"), 0.0) for item in valid_rows), 6)
    stock_cero = sum(1 for item in valid_rows if _safe_float(item.get("Stock"), 0.0) <= 0)

    return {
        "file_name": _normalize_import_filename(file_name),
        "selected_sheet": target_sheet,
        "suggested_sheet": suggested_sheet,
        "available_sheets": get_inventory_excel_sheets(file_bytes),
        "total_rows": int(len(provisional_rows)),
        "valid_rows": valid_rows,
        "review_rows": review_rows,
        "valid_count": int(len(valid_rows)),
        "review_count": int(len(review_rows)),
        "total_stock_valid": total_stock,
        "stock_cero_count": int(stock_cero),
        "duplicated_sku_count": int(len(duplicated_skus)),
        "preview_created_at": _safe_now(),
    }


def build_partial_inventory_update_preview(
    *,
    file_name: str,
    file_bytes: bytes,
    sheet_name: str | None = None,
) -> dict[str, Any]:
    ensure_inventory_phase1_schema()

    existing_map = _get_existing_inventory_map()
    if not existing_map:
        raise InventarioImportError(
            "No existe inventario base cargado. Primero aplica una carga inicial antes de usar una actualización parcial."
        )

    suggested_sheet = suggest_inventory_sheet(file_bytes)
    target_sheet = sheet_name or suggested_sheet or DEFAULT_INVENTARIO_SHEET
    df = read_inventory_sheet(file_bytes, sheet_name=target_sheet)

    working = df.copy()
    working = working[[col for col in working.columns if col in REQUIRED_INVENTARIO_COLUMNS]].copy()
    working = working.iloc[: PREVIEW_VALID_MAX_ROWS + PREVIEW_REVIEW_MAX_ROWS].copy()

    provisional_rows: list[dict[str, Any]] = []
    sku_counter: dict[str, int] = {}

    for idx, row in working.iterrows():
        row_num = int(idx) + 2
        sku = normalize_sku(row.get("SKU"))
        descripcion = normalize_descripcion(row.get("DESCRIPCION"))
        stock, stock_was_blank = parse_stock_value(row.get("STOCK"), treat_blank_as_zero=False)
        fecha_actualizacion = parse_required_date_to_iso(row.get("FECHA_ACTUALIZACION"))

        reasons: list[str] = []
        if not sku:
            reasons.append("SKU vacío")
        if stock is None:
            if stock_was_blank:
                reasons.append("STOCK vacío en actualización parcial")
            else:
                reasons.append("STOCK inválido")
        if not fecha_actualizacion:
            reasons.append("FECHA_ACTUALIZACION obligatoria o inválida")
        if sku and sku not in existing_map:
            reasons.append("SKU no existe en inventario base")

        current_row = existing_map.get(sku or "", {})
        current_total = round(_safe_float(current_row.get("stock_libre"), 0.0) + _safe_float(current_row.get("stock_asignado"), 0.0), 6)

        provisional = {
            "row_num": row_num,
            "sku": sku,
            "descripcion": descripcion or normalize_text(current_row.get("descripcion")),
            "stock": round(float(stock or 0), 6) if stock is not None else None,
            "fecha_actualizacion": fecha_actualizacion,
            "stock_vacio_detectado": bool(stock_was_blank),
            "stock_actual_sistema": current_total,
            "motivos_revision": reasons[:],
        }
        provisional_rows.append(provisional)

        if sku:
            sku_counter[sku] = sku_counter.get(sku, 0) + 1

    duplicated_skus = {sku for sku, count in sku_counter.items() if count > 1}

    valid_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []

    for row in provisional_rows:
        reasons = list(row.get("motivos_revision", []))
        sku = row.get("sku")
        if sku and sku in duplicated_skus:
            reasons.append("SKU repetido en el archivo")

        stock_nuevo = row.get("stock")
        stock_actual = _safe_float(row.get("stock_actual_sistema"), 0.0)
        export_row = {
            "Fila": row["row_num"],
            "SKU": row["sku"],
            "Descripción": row["descripcion"],
            "Stock actual sistema": round(stock_actual, 6),
            "Stock nuevo": "" if stock_nuevo is None else round(_safe_float(stock_nuevo, 0.0), 6),
            "Diferencia": "" if stock_nuevo is None else round(_safe_float(stock_nuevo, 0.0) - stock_actual, 6),
            "Fecha actualización": row.get("fecha_actualizacion") or "",
            "Nota": "STOCK vacío: fila excluida y enviada a revisión" if row.get("stock_vacio_detectado") else "",
        }

        if reasons:
            export_row["Motivo revisión"] = " | ".join(sorted(set(reasons)))
            review_rows.append(export_row)
        else:
            valid_rows.append(export_row)

    total_stock = round(sum(_safe_float(item.get("Stock nuevo"), 0.0) for item in valid_rows), 6)

    return {
        "file_name": _normalize_import_filename(file_name),
        "selected_sheet": target_sheet,
        "suggested_sheet": suggested_sheet,
        "available_sheets": get_inventory_excel_sheets(file_bytes),
        "total_rows": int(len(provisional_rows)),
        "valid_rows": valid_rows,
        "review_rows": review_rows,
        "valid_count": int(len(valid_rows)),
        "review_count": int(len(review_rows)),
        "total_stock_valid": total_stock,
        "duplicated_sku_count": int(len(duplicated_skus)),
        "preview_created_at": _safe_now(),
        "operation_mode": "ACTUALIZACION_PARCIAL",
    }


# -----------------------------------------------------------------------------
# Persistencia
# -----------------------------------------------------------------------------
def _save_inventory_source_file(file_name: str, file_bytes: bytes) -> Path:
    ensure_directories()
    base_dir = INPUT_DIR / "inventario_inicial"
    target = _unique_output_path(base_dir, file_name)
    target.write_bytes(file_bytes)
    return target





def _inventory_jobs_where_clause(alias: str = "") -> tuple[str, list[Any]]:
    prefix = f"{alias}." if alias else ""
    parts: list[str] = []
    params: list[Any] = []
    for tag in INVENTARIO_NOTE_TAGS:
        parts.append(f"{prefix}notes LIKE ?")
        params.append(f"%{tag}%")
    return "(" + " OR ".join(parts) + ")", params


def _save_inventory_update_source_file(file_name: str, file_bytes: bytes) -> Path:
    ensure_directories()
    base_dir = INPUT_DIR / "inventario_actualizacion"
    target = _unique_output_path(base_dir, file_name)
    target.write_bytes(file_bytes)
    return target


def _save_inventory_cut_source_file(file_name: str, file_bytes: bytes) -> Path:
    ensure_directories()
    base_dir = INPUT_DIR / "inventario_cortes"
    target = _unique_output_path(base_dir, file_name)
    target.write_bytes(file_bytes)
    return target


def _get_existing_inventory_map() -> dict[str, dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT
            sku,
            descripcion,
            total_recibido,
            stock_libre,
            stock_asignado,
            total_enviado,
            fecha_actualizacion,
            import_job_id,
            origen_carga,
            updated_at
        FROM inventario_sku
        """
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        sku = normalize_sku(row.get("sku"))
        if sku:
            result[sku] = row
    return result

def _save_review_report(import_job_id: int, review_rows: list[dict[str, Any]]) -> Path | None:
    if not review_rows:
        return None

    ensure_directories()
    report_dir = PROCESSED_DIR / "inventario_review"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"inventario_review_job_{import_job_id}.csv"
    pd.DataFrame(review_rows).to_csv(report_path, index=False, encoding="utf-8-sig")
    return report_path



def apply_initial_inventory_load(
    *,
    preview_payload: dict[str, Any],
    file_bytes: bytes,
    created_by: str | None = None,
) -> dict[str, Any]:
    ensure_inventory_phase1_schema()

    file_name = _normalize_import_filename(preview_payload.get("file_name") or "inventario_inicial.xlsx")
    valid_rows = list(preview_payload.get("valid_rows") or [])
    review_rows = list(preview_payload.get("review_rows") or [])
    selected_sheet = normalize_text(preview_payload.get("selected_sheet")) or DEFAULT_INVENTARIO_SHEET

    if not valid_rows:
        raise InventarioImportError("No hay filas válidas para aplicar la carga inicial del inventario.")

    saved_input_path = _save_inventory_source_file(file_name, file_bytes)

    notes_start = (
        f"{INVENTARIO_IMPORT_NOTE_TAG} Hoja={selected_sheet} | "
        f"Carga inicial de inventario base"
    )
    import_job_id = start_import_job(
        file_name=file_name,
        file_path=saved_input_path,
        import_type="CARGA_INICIAL",
        created_by=created_by,
        notes=notes_start,
    )

    review_report_path: Path | None = None
    try:
        now_iso = _safe_now()
        with transaction() as conn:
            conn.execute("DELETE FROM inventario_sku")
            conn.execute("DELETE FROM inventario_movimientos WHERE movimiento_tipo = 'CARGA_INICIAL'")

            inventory_rows = [
                (
                    row["SKU"],
                    row.get("Descripción") or None,
                    float(row.get("Stock") or 0),
                    float(row.get("Stock") or 0),
                    0.0,
                    0.0,
                    row.get("Fecha actualización") or None,
                    import_job_id,
                    "CARGA_INICIAL",
                    float(row.get("Stock") or 0),
                    0.0,
                    now_iso,
                )
                for row in valid_rows
            ]

            conn.executemany(
                """
                INSERT INTO inventario_sku (
                    sku,
                    descripcion,
                    total_recibido,
                    stock_libre,
                    stock_asignado,
                    total_enviado,
                    fecha_actualizacion,
                    import_job_id,
                    origen_carga,
                    stock_base_libre,
                    stock_base_asignado,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                inventory_rows,
            )

            movement_rows = [
                (
                    row["SKU"],
                    "CARGA_INICIAL",
                    float(row.get("Stock") or 0),
                    float(row.get("Stock") or 0),
                    0.0,
                    "CARGA_INICIAL",
                    import_job_id,
                    f"Carga inicial de inventario | fecha_actualizacion={row.get('Fecha actualización') or ''}",
                    normalize_text(created_by) or "Consulta",
                )
                for row in valid_rows
            ]

            conn.executemany(
                """
                INSERT INTO inventario_movimientos (
                    sku,
                    movimiento_tipo,
                    afecta_total,
                    afecta_stock_libre,
                    afecta_stock_asignado,
                    referencia_tipo,
                    referencia_id,
                    observacion,
                    created_by
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                movement_rows,
            )

        review_report_path = _save_review_report(import_job_id, review_rows)
        finish_import_job(
            import_job_id=import_job_id,
            status="COMPLETADO_CON_REVISION" if review_rows else "COMPLETADO",
            total_rows=_safe_int(preview_payload.get("total_rows"), len(valid_rows) + len(review_rows)),
            inserted_rows=len(valid_rows),
            updated_rows=0,
            error_rows=len(review_rows),
            notes=(
                f"{INVENTARIO_IMPORT_NOTE_TAG} Hoja={selected_sheet} | "
                f"Filas válidas={len(valid_rows)} | Revisión={len(review_rows)} | "
                f"Stock total válido={round(sum(_safe_float(r.get('Stock'), 0.0) for r in valid_rows), 2)}"
                + (
                    f" | Review CSV={review_report_path}"
                    if review_report_path is not None
                    else ""
                )
            ),
        )
    except Exception as exc:
        finish_import_job(
            import_job_id=import_job_id,
            status="FALLIDO",
            total_rows=_safe_int(preview_payload.get("total_rows"), 0),
            inserted_rows=0,
            updated_rows=0,
            error_rows=_safe_int(preview_payload.get("review_count"), 0),
            notes=f"{INVENTARIO_IMPORT_NOTE_TAG} Error en carga inicial: {exc}",
        )
        raise

    return {
        "import_job_id": import_job_id,
        "saved_input_path": str(saved_input_path),
        "review_report_path": str(review_report_path) if review_report_path else None,
        "inserted_rows": len(valid_rows),
        "review_rows": len(review_rows),
        "total_stock_valid": round(sum(_safe_float(r.get("Stock"), 0.0) for r in valid_rows), 2),
    }


def apply_partial_inventory_update(
    *,
    preview_payload: dict[str, Any],
    file_bytes: bytes,
    created_by: str | None = None,
) -> dict[str, Any]:
    ensure_inventory_phase1_schema()

    file_name = _normalize_import_filename(preview_payload.get("file_name") or "inventario_actualizacion.xlsx")
    valid_rows = list(preview_payload.get("valid_rows") or [])
    review_rows = list(preview_payload.get("review_rows") or [])
    selected_sheet = normalize_text(preview_payload.get("selected_sheet")) or DEFAULT_INVENTARIO_SHEET

    if not valid_rows:
        raise InventarioImportError("No hay filas válidas para aplicar la actualización parcial del inventario.")

    existing_map = _get_existing_inventory_map()
    if not existing_map:
        raise InventarioImportError("No existe inventario base cargado. Primero aplica una carga inicial.")

    saved_input_path = _save_inventory_update_source_file(file_name, file_bytes)
    notes_start = (
        f"{INVENTARIO_UPDATE_NOTE_TAG} Hoja={selected_sheet} | "
        f"Actualización parcial de inventario"
    )
    import_job_id = start_import_job(
        file_name=file_name,
        file_path=saved_input_path,
        import_type="CARGA_INICIAL",
        created_by=created_by,
        notes=notes_start,
    )

    review_report_path: Path | None = None
    touched_skus: list[str] = []
    updated_rows = 0
    try:
        now_iso = _safe_now()
        movement_rows: list[tuple[Any, ...]] = []
        metadata_rows: list[tuple[Any, ...]] = []

        for row in valid_rows:
            sku = normalize_sku(row.get("SKU"))
            if not sku:
                continue
            current = existing_map.get(sku)
            if not current:
                continue

            current_free = round(_safe_float(current.get("stock_libre"), 0.0), 6)
            current_assigned = round(_safe_float(current.get("stock_asignado"), 0.0), 6)
            current_total = round(current_free + current_assigned, 6)
            new_total = round(_safe_float(row.get("Stock nuevo"), 0.0), 6)
            new_assigned = round(min(current_assigned, new_total), 6)
            new_free = round(max(0.0, new_total - new_assigned), 6)

            delta_total = round(new_total - current_total, 6)
            delta_free = round(new_free - current_free, 6)
            delta_assigned = round(new_assigned - current_assigned, 6)

            movement_rows.append(
                (
                    sku,
                    "AJUSTE_MANUAL",
                    delta_total,
                    delta_free,
                    delta_assigned,
                    "AJUSTE_MANUAL",
                    import_job_id,
                    f"Actualización parcial de inventario | stock_anterior={current_total} | stock_nuevo={new_total} | fecha_actualizacion={row.get('Fecha actualización') or ''}",
                    normalize_text(created_by) or "Consulta",
                )
            )
            metadata_rows.append(
                (
                    normalize_text(row.get("Descripción")) or normalize_text(current.get("descripcion")) or None,
                    row.get("Fecha actualización") or None,
                    "ACTUALIZACION_INVENTARIO",
                    import_job_id,
                    new_free,
                    new_assigned,
                    sku,
                )
            )
            touched_skus.append(sku)

        if not touched_skus:
            raise InventarioImportError("No se encontró ningún SKU válido para actualizar.")

        with transaction() as conn:
            conn.executemany(
                """
                INSERT INTO inventario_movimientos (
                    sku,
                    movimiento_tipo,
                    afecta_total,
                    afecta_stock_libre,
                    afecta_stock_asignado,
                    referencia_tipo,
                    referencia_id,
                    observacion,
                    created_by
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                movement_rows,
            )

        unique_touched = sorted(set(touched_skus))
        for sku in unique_touched:
            descripcion = next((item[0] for item in metadata_rows if item[6] == sku), None)
            asignaciones_repository.recalculate_inventario_sku_from_movimientos(sku=sku, descripcion=descripcion)

        with transaction() as conn:
            conn.executemany(
                """
                UPDATE inventario_sku
                SET descripcion = COALESCE(?, descripcion),
                    fecha_actualizacion = ?,
                    origen_carga = ?,
                    import_job_id = ?,
                    stock_base_libre = ?,
                    stock_base_asignado = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE sku = ?
                """,
                metadata_rows,
            )

        updated_rows = len(set(touched_skus))
        review_report_path = _save_review_report(import_job_id, review_rows)
        finish_import_job(
            import_job_id=import_job_id,
            status="COMPLETADO_CON_REVISION" if review_rows else "COMPLETADO",
            total_rows=_safe_int(preview_payload.get("total_rows"), len(valid_rows) + len(review_rows)),
            inserted_rows=0,
            updated_rows=updated_rows,
            error_rows=len(review_rows),
            notes=(
                f"{INVENTARIO_UPDATE_NOTE_TAG} Hoja={selected_sheet} | "
                f"SKU actualizados={updated_rows} | Revisión={len(review_rows)} | "
                f"Stock total archivo válido={round(sum(_safe_float(r.get('Stock nuevo'), 0.0) for r in valid_rows), 2)}"
                + (f" | Review CSV={review_report_path}" if review_report_path is not None else "")
            ),
        )
    except Exception as exc:
        finish_import_job(
            import_job_id=import_job_id,
            status="FALLIDO",
            total_rows=_safe_int(preview_payload.get("total_rows"), 0),
            inserted_rows=0,
            updated_rows=0,
            error_rows=_safe_int(preview_payload.get("review_count"), 0),
            notes=f"{INVENTARIO_UPDATE_NOTE_TAG} Error en actualización parcial: {exc}",
        )
        raise

    return {
        "import_job_id": import_job_id,
        "saved_input_path": str(saved_input_path),
        "review_report_path": str(review_report_path) if review_report_path else None,
        "updated_rows": updated_rows,
        "review_rows": len(review_rows),
        "total_stock_valid": round(sum(_safe_float(r.get("Stock nuevo"), 0.0) for r in valid_rows), 2),
        "touched_skus": sorted(set(touched_skus)),
    }


# -----------------------------------------------------------------------------
# Consultas para la UI
# -----------------------------------------------------------------------------
def build_inventory_cut_preview(
    *,
    file_name: str,
    file_bytes: bytes,
    sheet_name: str | None = None,
) -> dict[str, Any]:
    ensure_inventory_phase1_schema()
    existing_map = _get_existing_inventory_map()
    if not existing_map:
        raise InventarioImportError("No existe inventario base cargado. Primero aplica una carga inicial.")

    suggested_sheet = suggest_inventory_sheet(file_bytes)
    target_sheet = sheet_name or suggested_sheet or DEFAULT_INVENTARIO_SHEET
    df = read_inventory_sheet(file_bytes, sheet_name=target_sheet)

    working = df.copy()
    working = working[[col for col in working.columns if col in REQUIRED_INVENTARIO_COLUMNS]].copy()
    working = working.iloc[: PREVIEW_VALID_MAX_ROWS + PREVIEW_REVIEW_MAX_ROWS].copy()

    provisional_rows: list[dict[str, Any]] = []
    sku_counter: dict[str, int] = {}

    for idx, row in working.iterrows():
        row_num = int(idx) + 2
        sku = normalize_sku(row.get("SKU"))
        descripcion = normalize_descripcion(row.get("DESCRIPCION"))
        stock, stock_was_blank = parse_stock_value(row.get("STOCK"), treat_blank_as_zero=False)
        fecha_actualizacion = parse_required_date_to_iso(row.get("FECHA_ACTUALIZACION"))

        reasons: list[str] = []
        if not sku:
            reasons.append("SKU vacío")
        if stock is None:
            reasons.append("STOCK obligatorio o inválido")
        if not fecha_actualizacion:
            reasons.append("FECHA_ACTUALIZACION obligatoria o inválida")

        current = existing_map.get(sku) if sku else None
        current_free = round(_safe_float(current.get("stock_libre"), 0.0), 6) if current else 0.0
        current_assigned = round(_safe_float(current.get("stock_asignado"), 0.0), 6) if current else 0.0
        current_total = round(current_free + current_assigned, 6) if current else 0.0

        if sku and current is None:
            reasons.append("SKU no existe en inventario base")
        if stock is not None and current is not None and float(stock) < current_assigned - 1e-9:
            reasons.append("Conteo menor que stock asignado actual")

        provisional = {
            "row_num": row_num,
            "sku": sku,
            "descripcion": descripcion,
            "stock_contado": round(float(stock or 0), 6) if stock is not None else None,
            "fecha_actualizacion": fecha_actualizacion,
            "stock_vacio": bool(stock_was_blank),
            "stock_sistema": current_total,
            "stock_asignado": current_assigned,
            "stock_libre_sistema": current_free,
            "motivos_revision": reasons[:],
        }
        provisional_rows.append(provisional)
        if sku:
            sku_counter[sku] = sku_counter.get(sku, 0) + 1

    duplicated_skus = {sku for sku, count in sku_counter.items() if count > 1}

    valid_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []

    for row in provisional_rows:
        reasons = list(row.get("motivos_revision", []))
        sku = row.get("sku")
        if sku and sku in duplicated_skus:
            reasons.append("SKU repetido en el archivo")

        stock_contado = row.get("stock_contado")
        stock_sistema = round(_safe_float(row.get("stock_sistema"), 0.0), 6)
        stock_asignado = round(_safe_float(row.get("stock_asignado"), 0.0), 6)
        diferencia = round(_safe_float(stock_contado, 0.0) - stock_sistema, 6) if stock_contado is not None else None
        nuevo_stock_libre = round(_safe_float(stock_contado, 0.0) - stock_asignado, 6) if stock_contado is not None else None

        export_base = {
            "Fila": row["row_num"],
            "SKU": row["sku"],
            "Descripción": row["descripcion"],
            "Stock sistema": stock_sistema,
            "Stock asignado": stock_asignado,
            "Stock contado": round(_safe_float(stock_contado, 0.0), 6) if stock_contado is not None else "",
            "Diferencia": round(_safe_float(diferencia, 0.0), 6) if diferencia is not None else "",
            "Stock libre resultante": round(_safe_float(nuevo_stock_libre, 0.0), 6) if nuevo_stock_libre is not None else "",
            "Fecha actualización": row.get("fecha_actualizacion") or "",
        }

        if reasons:
            review_rows.append({**export_base, "Motivo revisión": " | ".join(dict.fromkeys(reasons))})
        else:
            valid_rows.append(export_base)

    total_diferencia = round(sum(_safe_float(r.get("Diferencia"), 0.0) for r in valid_rows), 6)
    positivos = sum(1 for r in valid_rows if _safe_float(r.get("Diferencia"), 0.0) > 0)
    negativos = sum(1 for r in valid_rows if _safe_float(r.get("Diferencia"), 0.0) < 0)
    sin_cambio = sum(1 for r in valid_rows if abs(_safe_float(r.get("Diferencia"), 0.0)) <= 1e-9)

    return {
        "file_name": _normalize_import_filename(file_name),
        "selected_sheet": target_sheet,
        "suggested_sheet": suggested_sheet,
        "available_sheets": get_inventory_excel_sheets(file_bytes),
        "total_rows": int(len(provisional_rows)),
        "valid_rows": valid_rows,
        "review_rows": review_rows,
        "valid_count": int(len(valid_rows)),
        "review_count": int(len(review_rows)),
        "total_diferencia": total_diferencia,
        "conteos_positivos": int(positivos),
        "conteos_negativos": int(negativos),
        "conteos_sin_cambio": int(sin_cambio),
        "preview_created_at": _safe_now(),
        "operation_mode": "CORTE_FISICO_SIMPLE",
    }


def apply_inventory_cut(
    *,
    preview_payload: dict[str, Any],
    file_bytes: bytes,
    created_by: str | None = None,
    observacion: str | None = None,
) -> dict[str, Any]:
    ensure_inventory_phase1_schema()

    file_name = _normalize_import_filename(preview_payload.get("file_name") or "inventario_corte.xlsx")
    valid_rows = list(preview_payload.get("valid_rows") or [])
    review_rows = list(preview_payload.get("review_rows") or [])
    selected_sheet = normalize_text(preview_payload.get("selected_sheet")) or DEFAULT_INVENTARIO_SHEET

    if not valid_rows:
        raise InventarioImportError("No hay filas válidas para aplicar el corte físico simple.")

    existing_map = _get_existing_inventory_map()
    if not existing_map:
        raise InventarioImportError("No existe inventario base cargado. Primero aplica una carga inicial.")

    saved_input_path = _save_inventory_cut_source_file(file_name, file_bytes)
    notes_start = f"{INVENTARIO_CUT_NOTE_TAG} Hoja={selected_sheet} | Corte físico simple"
    import_job_id = start_import_job(
        file_name=file_name,
        file_path=saved_input_path,
        import_type="CARGA_INICIAL",
        created_by=created_by,
        notes=notes_start,
    )

    review_report_path: Path | None = None
    applied_rows = 0
    total_difference = 0.0
    corte_id = None
    try:
        user = normalize_text(created_by) or "Consulta"
        now_iso = _safe_now()
        fecha_corte = max(
            [normalize_text(r.get("Fecha actualización")) for r in valid_rows if normalize_text(r.get("Fecha actualización"))] or [now_iso[:10]]
        )

        with transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO inventario_cortes (
                    import_job_id, tipo_corte, archivo_nombre, hoja_nombre, fecha_corte, total_filas,
                    filas_aplicadas, filas_revision, diferencia_total, estado, usuario, observacion
                )
                VALUES (?, 'CONTEO_PARCIAL', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    import_job_id,
                    file_name,
                    selected_sheet,
                    fecha_corte,
                    _safe_int(preview_payload.get("total_rows"), len(valid_rows) + len(review_rows)),
                    len(valid_rows),
                    len(review_rows),
                    round(sum(_safe_float(r.get("Diferencia"), 0.0) for r in valid_rows), 6),
                    "APLICADO_CON_REVISION" if review_rows else "APLICADO",
                    user,
                    normalize_text(observacion),
                ),
            )
            corte_id = int(cursor.lastrowid)

            detail_rows = []
            audit_rows = []
            movement_rows = []
            update_rows = []
            row_num = 0
            for item in valid_rows:
                row_num += 1
                sku = normalize_sku(item.get("SKU"))
                current = existing_map.get(sku)
                if not current:
                    continue
                current_free = round(_safe_float(current.get("stock_libre"), 0.0), 6)
                current_assigned = round(_safe_float(current.get("stock_asignado"), 0.0), 6)
                current_total = round(current_free + current_assigned, 6)
                counted_total = round(_safe_float(item.get("Stock contado"), 0.0), 6)
                new_free = round(max(0.0, counted_total - current_assigned), 6)
                delta_total = round(counted_total - current_total, 6)
                delta_free = round(new_free - current_free, 6)
                total_difference += delta_total

                detail_rows.append((
                    corte_id, row_num, sku, normalize_text(item.get("Descripción")) or normalize_text(current.get("descripcion")),
                    current_total, current_assigned, counted_total, delta_total, normalize_text(item.get("Fecha actualización")) or None,
                    "APLICADO", None,
                ))

                update_rows.append((
                    normalize_text(item.get("Descripción")) or normalize_text(current.get("descripcion")) or None,
                    new_free,
                    normalize_text(item.get("Fecha actualización")) or None,
                    "CORTE_FISICO_SIMPLE",
                    import_job_id,
                    new_free,
                    current_assigned,
                    sku,
                ))
                movement_rows.append((
                    sku, "AJUSTE_MANUAL", delta_total, delta_free, 0.0,
                    "AJUSTE_MANUAL", corte_id,
                    f"{INVENTARIO_CUT_NOTE_TAG} Corte físico simple | stock_anterior={current_total} | stock_contado={counted_total} | fecha_actualizacion={normalize_text(item.get('Fecha actualización'))}",
                    user,
                ))
                audit_rows.extend([
                    ("inventario_sku", sku, "stock_libre", str(current_free), str(new_free), user, f"{INVENTARIO_CUT_NOTE_TAG} corte_id={corte_id}"),
                    ("inventario_sku", sku, "existencia_fisica", str(current_total), str(counted_total), user, f"{INVENTARIO_CUT_NOTE_TAG} corte_id={corte_id}"),
                ])
                applied_rows += 1

            for rr in review_rows:
                row_num += 1
                detail_rows.append((
                    corte_id, row_num, normalize_sku(rr.get("SKU")), normalize_text(rr.get("Descripción")),
                    _safe_float(rr.get("Stock sistema"), 0.0), _safe_float(rr.get("Stock asignado"), 0.0),
                    _safe_float(rr.get("Stock contado"), 0.0), _safe_float(rr.get("Diferencia"), 0.0),
                    normalize_text(rr.get("Fecha actualización")) or None, "REVISION", normalize_text(rr.get("Motivo revisión")),
                ))

            conn.executemany(
                """
                INSERT INTO inventario_corte_detalle (
                    corte_id, row_num, sku, descripcion, stock_sistema, stock_asignado, stock_contado,
                    diferencia, fecha_actualizacion, estado_detalle, motivo_revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                detail_rows,
            )
            if movement_rows:
                conn.executemany(
                    """
                    INSERT INTO inventario_movimientos (
                        sku, movimiento_tipo, afecta_total, afecta_stock_libre, afecta_stock_asignado,
                        referencia_tipo, referencia_id, observacion, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    movement_rows,
                )
            if update_rows:
                conn.executemany(
                    """
                    UPDATE inventario_sku
                    SET descripcion = COALESCE(?, descripcion),
                        stock_libre = ?,
                        fecha_actualizacion = ?,
                        origen_carga = ?,
                        import_job_id = ?,
                        stock_base_libre = ?,
                        stock_base_asignado = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE sku = ?
                    """,
                    update_rows,
                )
            if audit_rows:
                conn.executemany(
                    """
                    INSERT INTO auditoria_cambios (
                        tabla, registro_id, campo, valor_anterior, valor_nuevo, usuario, motivo
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    audit_rows,
                )

        review_report_path = _save_review_report(import_job_id, review_rows)
        finish_import_job(
            import_job_id=import_job_id,
            status="COMPLETADO_CON_REVISION" if review_rows else "COMPLETADO",
            total_rows=_safe_int(preview_payload.get("total_rows"), len(valid_rows) + len(review_rows)),
            inserted_rows=applied_rows,
            updated_rows=applied_rows,
            error_rows=len(review_rows),
            notes=(
                f"{INVENTARIO_CUT_NOTE_TAG} Hoja={selected_sheet} | Filas aplicadas={applied_rows} | Revisión={len(review_rows)} | "
                f"Diferencia total={round(total_difference, 2)}"
                + (f" | Review CSV={review_report_path}" if review_report_path is not None else "")
            ),
        )
    except Exception as exc:
        finish_import_job(
            import_job_id=import_job_id,
            status="FALLIDO",
            total_rows=_safe_int(preview_payload.get("total_rows"), 0),
            inserted_rows=0,
            updated_rows=0,
            error_rows=_safe_int(preview_payload.get("review_count"), 0),
            notes=f"{INVENTARIO_CUT_NOTE_TAG} Error en corte físico simple: {exc}",
        )
        raise

    return {
        "import_job_id": import_job_id,
        "corte_id": corte_id,
        "saved_input_path": str(saved_input_path),
        "review_report_path": str(review_report_path) if review_report_path else None,
        "applied_rows": applied_rows,
        "review_rows": len(review_rows),
        "total_difference": round(total_difference, 2),
    }


def list_inventario_cortes(limit: int = 50) -> list[dict[str, Any]]:
    ensure_inventory_phase1_schema()
    rows = fetch_all(
        """
        SELECT
            id, fecha_corte, archivo_nombre, hoja_nombre, tipo_corte, total_filas, filas_aplicadas,
            filas_revision, diferencia_total, estado, usuario, observacion, created_at
        FROM inventario_cortes
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    )
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({
            "ID corte": _safe_int(row.get("id"), 0),
            "Fecha corte": normalize_text(row.get("fecha_corte")),
            "Archivo": normalize_text(row.get("archivo_nombre")),
            "Hoja": normalize_text(row.get("hoja_nombre")),
            "Tipo": normalize_text(row.get("tipo_corte")),
            "Filas": _safe_int(row.get("total_filas"), 0),
            "Aplicadas": _safe_int(row.get("filas_aplicadas"), 0),
            "Revisión": _safe_int(row.get("filas_revision"), 0),
            "Diferencia total": round(_safe_float(row.get("diferencia_total"), 0.0), 2),
            "Estado": normalize_text(row.get("estado")),
            "Usuario": normalize_text(row.get("usuario")),
            "Observación": normalize_text(row.get("observacion")),
            "Creado": normalize_text(row.get("created_at")),
        })
    return normalized


def get_inventario_resumen() -> dict[str, Any]:
    ensure_inventory_phase1_schema()

    row = fetch_one(
        """
        SELECT
            COUNT(*) AS total_skus,
            COALESCE(SUM(COALESCE(stock_libre, 0) + COALESCE(stock_asignado, 0)), 0) AS total_piezas,
            COALESCE(SUM(CASE WHEN COALESCE(stock_libre, 0) + COALESCE(stock_asignado, 0) <= 0 THEN 1 ELSE 0 END), 0) AS skus_en_cero,
            MAX(fecha_actualizacion) AS ultima_fecha_actualizacion,
            MAX(updated_at) AS ultima_actualizacion_sistema
        FROM inventario_sku
        """
    ) or {}

    inventory_where_sql, inventory_where_params = _inventory_jobs_where_clause()
    latest_job = fetch_one(
        f"""
        SELECT *
        FROM import_jobs
        WHERE {inventory_where_sql}
        ORDER BY id DESC
        LIMIT 1
        """,
        tuple(inventory_where_params),
    )

    return {
        "total_skus": _safe_int(row.get("total_skus"), 0),
        "total_piezas": round(_safe_float(row.get("total_piezas"), 0.0), 2),
        "skus_en_cero": _safe_int(row.get("skus_en_cero"), 0),
        "ultima_fecha_actualizacion": normalize_text(row.get("ultima_fecha_actualizacion")),
        "ultima_actualizacion_sistema": normalize_text(row.get("ultima_actualizacion_sistema")),
        "ultimo_import_job": latest_job,
        "skus_en_revision_ultimo_job": _safe_int((latest_job or {}).get("error_rows"), 0),
    }



def list_inventario_tabla(
    *,
    search_text: str | None = None,
    only_zero_stock: bool = False,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    ensure_inventory_phase1_schema()

    where_parts: list[str] = []
    params: list[Any] = []

    search = normalize_text(search_text)
    if search:
        where_parts.append("(lower(sku) LIKE lower(?) OR lower(COALESCE(descripcion, '')) LIKE lower(?))")
        like = f"%{search}%"
        params.extend([like, like])

    if only_zero_stock:
        where_parts.append("(COALESCE(stock_libre, 0) + COALESCE(stock_asignado, 0)) <= 0")

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    params.append(limit)

    rows = fetch_all(
        f"""
        SELECT
            sku,
            descripcion,
            total_recibido,
            stock_libre,
            stock_asignado,
            total_enviado,
            fecha_actualizacion,
            import_job_id,
            origen_carga,
            updated_at,
            (COALESCE(stock_libre, 0) + COALESCE(stock_asignado, 0)) AS stock_actual
        FROM inventario_sku
        {where_sql}
        ORDER BY sku ASC
        LIMIT ?
        """,
        params,
    )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "SKU": normalize_text(row.get("sku")),
                "Descripción": normalize_text(row.get("descripcion")),
                "Stock actual": round(_safe_float(row.get("stock_actual"), 0.0), 2),
                "Stock libre": round(_safe_float(row.get("stock_libre"), 0.0), 2),
                "Stock asignado": round(_safe_float(row.get("stock_asignado"), 0.0), 2),
                "Total recibido": round(_safe_float(row.get("total_recibido"), 0.0), 2),
                "Total enviado": round(_safe_float(row.get("total_enviado"), 0.0), 2),
                "Fecha actualización": normalize_text(row.get("fecha_actualizacion")),
                "Origen": normalize_text(row.get("origen_carga")) or "DESCONOCIDO",
                "Import job": _safe_int(row.get("import_job_id"), 0),
                "Actualizado en sistema": normalize_text(row.get("updated_at")),
            }
        )
    return normalized



def list_inventario_import_jobs(limit: int = 20) -> list[dict[str, Any]]:
    inventory_where_sql, inventory_where_params = _inventory_jobs_where_clause()
    rows = fetch_all(
        f"""
        SELECT *
        FROM import_jobs
        WHERE {inventory_where_sql}
        ORDER BY id DESC
        LIMIT ?
        """,
        [*inventory_where_params, limit],
    )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        job_notes = normalize_text(row.get("notes"))
        tipo_job = "ACTUALIZACIÓN PARCIAL" if INVENTARIO_UPDATE_NOTE_TAG in job_notes else "CARGA INICIAL"
        normalized.append(
            {
                "ID": _safe_int(row.get("id"), 0),
                "Tipo job": tipo_job,
                "Archivo": normalize_text(row.get("file_name")),
                "Estado": normalize_text(row.get("status")),
                "Total filas": _safe_int(row.get("total_rows"), 0),
                "Insertadas": _safe_int(row.get("inserted_rows"), 0),
                "Revisión": _safe_int(row.get("error_rows"), 0),
                "Creado por": normalize_text(row.get("created_by")),
                "Inicio": normalize_text(row.get("started_at")),
                "Fin": normalize_text(row.get("finished_at")),
                "Notas": normalize_text(row.get("notes")),
            }
        )
    return normalized


# -----------------------------------------------------------------------------
# Operación inventario + pedidos activos
# -----------------------------------------------------------------------------
def _row_to_operativo(row: dict[str, Any]) -> dict[str, Any]:
    sku = normalize_text(row.get("sku"))
    descripcion = normalize_text(row.get("descripcion"))
    base_total = round(_safe_float(row.get("stock_base_total"), 0.0), 6)
    entradas_post = round(_safe_float(row.get("entradas_post_total"), 0.0), 6)
    salidas_post_neto = round(_safe_float(row.get("salidas_post_total"), 0.0), 6)
    ajustes_post_neto = round(_safe_float(row.get("ajustes_post_total"), 0.0), 6)
    existencia = round(base_total + entradas_post + salidas_post_neto + ajustes_post_neto, 6)
    reservado_activo = round(_safe_float(row.get("reservado_activo"), 0.0), 6)
    cantidad_pedida = round(_safe_float(row.get("cantidad_pedida_activa"), 0.0), 6)
    cantidad_faltante = round(_safe_float(row.get("cantidad_faltante_activa"), 0.0), 6)
    pedidos_activos = _safe_int(row.get("pedidos_activos"), 0)
    clientes_activos = _safe_int(row.get("clientes_activos"), 0)
    stock_asignado_operativo = round(reservado_activo, 6)
    stock_libre_operativo = round(existencia - stock_asignado_operativo, 6)
    diferencia_reserva = round(stock_asignado_operativo - reservado_activo, 6)
    missing_base = not bool(row.get("inventario_base"))

    semaforo = "VERDE"
    if stock_libre_operativo < 0:
        semaforo = "ROJO"
    elif stock_libre_operativo == 0:
        semaforo = "NARANJA"

    alertas: list[str] = []
    if missing_base and cantidad_pedida > 0:
        alertas.append("SKU con pedidos activos sin inventario base")
    if reservado_activo > existencia:
        alertas.append("Sobreasignado contra existencia operativa")
    if cantidad_pedida > 0 and existencia <= 0:
        alertas.append("Sin existencia con demanda activa")
    if cantidad_pedida > 0 and stock_libre_operativo <= 0:
        alertas.append("Sin stock libre")

    return {
        "SKU": sku,
        "Descripción": descripcion,
        "Fecha base SKU": normalize_text(row.get("fecha_actualizacion")),
        "Existencia base": round(base_total, 2),
        "Entradas posteriores": round(entradas_post, 2),
        "Salidas posteriores": round(abs(salidas_post_neto), 2),
        "Ajustes posteriores netos": round(ajustes_post_neto, 2),
        "Existencia operativa": round(existencia, 2),
        "Stock asignado operativo": round(stock_asignado_operativo, 2),
        "Stock libre operativo": round(stock_libre_operativo, 2),
        "Reservado activo": round(reservado_activo, 2),
        "Disponible": round(stock_libre_operativo, 2),
        "Cantidad pedida activa": round(cantidad_pedida, 2),
        "Cantidad faltante activa": round(cantidad_faltante, 2),
        "Pedidos activos": pedidos_activos,
        "Clientes activos": clientes_activos,
        "Semáforo": semaforo,
        "Alerta": " | ".join(alertas),
        "Sin inventario base": "Sí" if missing_base else "No",
        "Diferencia reserva": round(diferencia_reserva, 2),
    }


def list_inventario_operativo(
    *,
    search_text: str | None = None,
    semaforo: str | None = None,
    only_zero_stock: bool = False,
    only_negative: bool = False,
    only_with_demanda: bool = False,
    only_missing_base: bool = False,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    ensure_inventory_phase1_schema()

    search = normalize_text(search_text)
    where_parts: list[str] = []
    params: list[Any] = []

    if search:
        where_parts.append("(lower(base.sku) LIKE lower(?) OR lower(COALESCE(base.descripcion, '')) LIKE lower(?))")
        like = f"%{search}%"
        params.extend([like, like])

    if only_zero_stock:
        where_parts.append("COALESCE(final.existencia_operativa, 0) <= 0")
    if only_negative:
        where_parts.append("(COALESCE(final.existencia_operativa, 0) - COALESCE(final.reservado_activo, 0)) < 0")
    if only_with_demanda:
        where_parts.append("COALESCE(final.cantidad_pedida_activa, 0) > 0")
    if only_missing_base:
        where_parts.append("COALESCE(base.inventario_base, 0) = 0")
    normalized_semaforo = normalize_text(semaforo).upper()
    if normalized_semaforo in {"ROJO", "NARANJA", "VERDE"}:
        if normalized_semaforo == "ROJO":
            where_parts.append("(COALESCE(final.existencia_operativa, 0) - COALESCE(final.reservado_activo, 0)) < 0")
        elif normalized_semaforo == "NARANJA":
            where_parts.append("(COALESCE(final.existencia_operativa, 0) - COALESCE(final.reservado_activo, 0)) = 0")
        else:
            where_parts.append("(COALESCE(final.existencia_operativa, 0) - COALESCE(final.reservado_activo, 0)) > 0")

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    params.append(limit)

    rows = fetch_all(
        f"""
        WITH demanda AS (
            SELECT
                pl.sku AS sku,
                COUNT(DISTINCT p.id) AS pedidos_activos,
                COUNT(DISTINCT COALESCE(p.cliente, '')) AS clientes_activos,
                COALESCE(SUM(pl.cantidad_pedida), 0) AS cantidad_pedida_activa,
                COALESCE(SUM(pl.cantidad_asignada), 0) AS reservado_activo,
                COALESCE(SUM(pl.cantidad_faltante), 0) AS cantidad_faltante_activa
            FROM pedido_lineas pl
            JOIN pedidos p ON p.id = pl.pedido_id
            WHERE COALESCE(p.estado_operativo, 'ACTIVO') = 'ACTIVO'
              AND COALESCE(pl.sku, '') <> ''
            GROUP BY pl.sku
        ),
        base AS (
            SELECT
                i.sku AS sku,
                COALESCE(i.descripcion, '') AS descripcion,
                COALESCE(i.stock_base_libre, COALESCE(i.stock_libre, 0)) AS stock_base_libre,
                COALESCE(i.stock_base_asignado, COALESCE(i.stock_asignado, 0)) AS stock_base_asignado,
                (COALESCE(i.stock_base_libre, COALESCE(i.stock_libre, 0)) + COALESCE(i.stock_base_asignado, COALESCE(i.stock_asignado, 0))) AS stock_base_total,
                i.fecha_actualizacion AS fecha_actualizacion,
                1 AS inventario_base,
                COALESCE(d.pedidos_activos, 0) AS pedidos_activos,
                COALESCE(d.clientes_activos, 0) AS clientes_activos,
                COALESCE(d.cantidad_pedida_activa, 0) AS cantidad_pedida_activa,
                COALESCE(d.reservado_activo, 0) AS reservado_activo,
                COALESCE(d.cantidad_faltante_activa, 0) AS cantidad_faltante_activa
            FROM inventario_sku i
            LEFT JOIN demanda d ON d.sku = i.sku

            UNION ALL

            SELECT
                d.sku AS sku,
                '' AS descripcion,
                0 AS stock_base_libre,
                0 AS stock_base_asignado,
                0 AS stock_base_total,
                NULL AS fecha_actualizacion,
                0 AS inventario_base,
                d.pedidos_activos,
                d.clientes_activos,
                d.cantidad_pedida_activa,
                d.reservado_activo,
                d.cantidad_faltante_activa
            FROM demanda d
            LEFT JOIN inventario_sku i ON i.sku = d.sku
            WHERE i.sku IS NULL
        ),
        entradas_source AS (
            SELECT
                m.sku AS sku,
                COALESCE(es.fecha, substr(COALESCE(m.created_at, ''), 1, 10), '') AS fecha_evento,
                COALESCE(SUM(m.afecta_total), 0) AS delta_total,
                COALESCE(SUM(m.afecta_stock_libre), 0) AS delta_libre,
                COALESCE(SUM(m.afecta_stock_asignado), 0) AS delta_asignado
            FROM inventario_movimientos m
            LEFT JOIN entrada_decisiones ed_direct
                ON m.referencia_tipo = 'ENTRADA_DECISION'
               AND m.referencia_id = ed_direct.id
            LEFT JOIN asignaciones a
                ON m.referencia_tipo = 'ASIGNACION'
               AND m.referencia_id = a.id
            LEFT JOIN entrada_decisiones ed_asig
                ON a.decision_id = ed_asig.id
            LEFT JOIN entradas_staging es
                ON es.id = COALESCE(ed_direct.entrada_staging_id, ed_asig.entrada_staging_id)
            WHERE m.movimiento_tipo = 'ENTRADA_A_STOCK'
              AND COALESCE(es.fecha, substr(COALESCE(m.created_at, ''), 1, 10), '') <> ''
            GROUP BY m.sku, COALESCE(es.fecha, substr(COALESCE(m.created_at, ''), 1, 10), '')
        ),
        salidas_source AS (
            SELECT
                plc.sku AS sku,
                COALESCE(pc.fecha_envio, '') AS fecha_evento,
                COALESCE(-SUM(plc.cantidad_enviada), 0) AS delta_total,
                0 AS delta_libre,
                0 AS delta_asignado
            FROM pedido_linea_cierres plc
            JOIN pedido_cierres pc
              ON pc.id = plc.pedido_cierre_id
            WHERE COALESCE(plc.sku, '') <> ''
              AND COALESCE(pc.fecha_envio, '') <> ''
              AND COALESCE(plc.cantidad_enviada, 0) > 0
            GROUP BY plc.sku, COALESCE(pc.fecha_envio, '')
        ),
        ajustes_source AS (
            SELECT
                m.sku AS sku,
                substr(COALESCE(m.created_at, ''), 1, 10) AS fecha_evento,
                COALESCE(SUM(m.afecta_total), 0) AS delta_total,
                COALESCE(SUM(m.afecta_stock_libre), 0) AS delta_libre,
                COALESCE(SUM(m.afecta_stock_asignado), 0) AS delta_asignado
            FROM inventario_movimientos m
            WHERE m.movimiento_tipo = 'AJUSTE_MANUAL'
              AND COALESCE(m.observacion, '') NOT LIKE '%{INVENTARIO_UPDATE_NOTE_TAG}%'
              AND COALESCE(m.observacion, '') NOT LIKE '%{INVENTARIO_CUT_NOTE_TAG}%'
            GROUP BY m.sku, substr(COALESCE(m.created_at, ''), 1, 10)
        ),
        entradas_post AS (
            SELECT
                b.sku,
                COALESCE(SUM(e.delta_total), 0) AS entradas_post_total,
                COALESCE(SUM(e.delta_libre), 0) AS entradas_post_libre,
                COALESCE(SUM(e.delta_asignado), 0) AS entradas_post_asignado
            FROM base b
            LEFT JOIN entradas_source e
              ON e.sku = b.sku
             AND (
                 COALESCE(b.fecha_actualizacion, '') = ''
                 OR date(e.fecha_evento) > date(b.fecha_actualizacion)
             )
            GROUP BY b.sku
        ),
        salidas_post AS (
            SELECT
                b.sku,
                COALESCE(SUM(s.delta_total), 0) AS salidas_post_total,
                COALESCE(SUM(s.delta_libre), 0) AS salidas_post_libre,
                COALESCE(SUM(s.delta_asignado), 0) AS salidas_post_asignado
            FROM base b
            LEFT JOIN salidas_source s
              ON s.sku = b.sku
             AND (
                 COALESCE(b.fecha_actualizacion, '') = ''
                 OR date(s.fecha_evento) > date(b.fecha_actualizacion)
             )
            GROUP BY b.sku
        ),
        ajustes_post AS (
            SELECT
                b.sku,
                COALESCE(SUM(a.delta_total), 0) AS ajustes_post_total,
                COALESCE(SUM(a.delta_libre), 0) AS ajustes_post_libre,
                COALESCE(SUM(a.delta_asignado), 0) AS ajustes_post_asignado
            FROM base b
            LEFT JOIN ajustes_source a
              ON a.sku = b.sku
             AND (
                 COALESCE(b.fecha_actualizacion, '') = ''
                 OR date(a.fecha_evento) > date(b.fecha_actualizacion)
             )
            GROUP BY b.sku
        ),
        final AS (
            SELECT
                base.*,
                COALESCE(ep.entradas_post_total, 0) AS entradas_post_total,
                COALESCE(ep.entradas_post_libre, 0) AS entradas_post_libre,
                COALESCE(ep.entradas_post_asignado, 0) AS entradas_post_asignado,
                COALESCE(sp.salidas_post_total, 0) AS salidas_post_total,
                COALESCE(sp.salidas_post_libre, 0) AS salidas_post_libre,
                COALESCE(sp.salidas_post_asignado, 0) AS salidas_post_asignado,
                COALESCE(ap.ajustes_post_total, 0) AS ajustes_post_total,
                COALESCE(ap.ajustes_post_libre, 0) AS ajustes_post_libre,
                COALESCE(ap.ajustes_post_asignado, 0) AS ajustes_post_asignado,
                (
                    COALESCE(base.stock_base_total, 0)
                    + COALESCE(ep.entradas_post_total, 0)
                    + COALESCE(sp.salidas_post_total, 0)
                    + COALESCE(ap.ajustes_post_total, 0)
                ) AS existencia_operativa
            FROM base
            LEFT JOIN entradas_post ep ON ep.sku = base.sku
            LEFT JOIN salidas_post sp ON sp.sku = base.sku
            LEFT JOIN ajustes_post ap ON ap.sku = base.sku
        )
        SELECT *
        FROM final
        {where_sql}
        ORDER BY
            CASE
                WHEN (COALESCE(final.existencia_operativa, 0) - COALESCE(final.reservado_activo, 0)) < 0 THEN 0
                WHEN (COALESCE(final.existencia_operativa, 0) - COALESCE(final.reservado_activo, 0)) = 0 THEN 1
                ELSE 2
            END,
            final.sku ASC
        LIMIT ?
        """,
        params,
    )
    return [_row_to_operativo(row) for row in rows]




def materialize_operational_stock_snapshot(*, created_by: str | None = None) -> dict[str, Any]:
    """
    Materializa en inventario_sku una foto operativa actual usando la lógica
    de existencia física operativa, asignado activo y stock libre.

    Esta rutina es útil después de un bootstrap inicial o una reconstrucción
    histórica para que los módulos diarios trabajen con stock_libre y
    stock_asignado consistentes sin perder la base histórica del SKU.
    """
    ensure_inventory_phase1_schema()
    rows = list_inventario_operativo(limit=200000)
    if not rows:
        return {"updated_skus": 0, "total_existencia": 0.0, "total_stock_libre": 0.0, "total_stock_asignado": 0.0}

    total_existencia = 0.0
    total_stock_libre = 0.0
    total_stock_asignado = 0.0
    payload: list[tuple[Any, ...]] = []

    for row in rows:
        sku = normalize_sku(row.get("SKU"))
        if not sku:
            continue
        descripcion = normalize_descripcion(row.get("Descripción")) or None
        existencia = round(_safe_float(row.get("Existencia operativa"), 0.0), 6)
        reservado = round(_safe_float(row.get("Reservado activo"), 0.0), 6)
        stock_asignado = round(max(0.0, min(existencia, reservado)), 6)
        stock_libre = round(max(0.0, existencia - stock_asignado), 6)

        total_existencia += existencia
        total_stock_libre += stock_libre
        total_stock_asignado += stock_asignado
        total_registrado = round(max(existencia, 0.0), 6)
        payload.append((descripcion, total_registrado, stock_libre, stock_asignado, sku))

    with transaction() as conn:
        for descripcion, total_registrado, stock_libre, stock_asignado, sku in payload:
            conn.execute(
                """
                INSERT INTO inventario_sku (
                    sku, descripcion, total_recibido, stock_libre, stock_asignado, total_enviado, origen_carga, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 0, 'BOOTSTRAP_OPERATIVO', CURRENT_TIMESTAMP)
                ON CONFLICT(sku) DO UPDATE SET
                    descripcion = COALESCE(excluded.descripcion, inventario_sku.descripcion),
                    total_recibido = excluded.total_recibido,
                    stock_libre = excluded.stock_libre,
                    stock_asignado = excluded.stock_asignado,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (sku, descripcion, total_registrado, stock_libre, stock_asignado),
            )

    return {
        "updated_skus": len(payload),
        "total_existencia": round(total_existencia, 2),
        "total_stock_libre": round(total_stock_libre, 2),
        "total_stock_asignado": round(total_stock_asignado, 2),
        "created_by": normalize_text(created_by) or "Sistema",
    }

def get_inventario_operativo_resumen() -> dict[str, Any]:
    rows = list_inventario_operativo(limit=100000)
    total_skus = len(rows)
    total_existencia = round(sum(_safe_float(r.get("Existencia operativa"), 0) for r in rows), 2)
    total_reservado = round(sum(_safe_float(r.get("Reservado activo"), 0) for r in rows), 2)
    total_disponible = round(sum(_safe_float(r.get("Disponible"), 0) for r in rows), 2)
    rojos = sum(1 for r in rows if normalize_text(r.get("Semáforo")) == "ROJO")
    naranjas = sum(1 for r in rows if normalize_text(r.get("Semáforo")) == "NARANJA")
    con_demanda = sum(1 for r in rows if _safe_float(r.get("Cantidad pedida activa"), 0) > 0)
    sin_base = sum(1 for r in rows if normalize_text(r.get("Sin inventario base")) == "Sí")
    return {
        "total_skus_operativos": total_skus,
        "total_existencia": total_existencia,
        "total_reservado_activo": total_reservado,
        "total_disponible": total_disponible,
        "skus_rojos": rojos,
        "skus_naranjas": naranjas,
        "skus_con_demanda": con_demanda,
        "skus_sin_base": sin_base,
    }


def list_inventario_alertas(limit: int = 5000) -> list[dict[str, Any]]:
    rows = list_inventario_operativo(limit=limit)
    return [row for row in rows if normalize_text(row.get("Alerta"))]


def list_inventario_movimientos(
    *,
    search_text: str | None = None,
    movimiento_tipo: str | None = None,
    referencia_tipo: str | None = None,
    created_by: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    ensure_inventory_phase1_schema()
    where_parts: list[str] = []
    params: list[Any] = []

    search = normalize_text(search_text)
    if search:
        like = f"%{search}%"
        where_parts.append("(lower(m.sku) LIKE lower(?) OR lower(COALESCE(i.descripcion, '')) LIKE lower(?) OR lower(COALESCE(m.observacion, '')) LIKE lower(?))")
        params.extend([like, like, like])

    mov = normalize_text(movimiento_tipo).upper()
    if mov:
        where_parts.append("m.movimiento_tipo = ?")
        params.append(mov)

    ref = normalize_text(referencia_tipo).upper()
    if ref:
        where_parts.append("COALESCE(m.referencia_tipo, '') = ?")
        params.append(ref)

    user = normalize_text(created_by)
    if user:
        where_parts.append("COALESCE(m.created_by, '') = ?")
        params.append(user)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    params.append(limit)

    rows = fetch_all(
        f"""
        SELECT
            m.id, m.sku, COALESCE(i.descripcion, '') AS descripcion, m.movimiento_tipo,
            m.afecta_total, m.afecta_stock_libre, m.afecta_stock_asignado,
            m.referencia_tipo, m.referencia_id, m.observacion, m.created_by, m.created_at
        FROM inventario_movimientos m
        LEFT JOIN inventario_sku i ON i.sku = m.sku
        {where_sql}
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT ?
        """,
        params,
    )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({
            "ID": _safe_int(row.get("id"), 0),
            "Fecha": normalize_text(row.get("created_at")),
            "SKU": normalize_text(row.get("sku")),
            "Descripción": normalize_text(row.get("descripcion")),
            "Tipo": normalize_text(row.get("movimiento_tipo")),
            "Afecta total": round(_safe_float(row.get("afecta_total"), 0), 2),
            "Afecta stock libre": round(_safe_float(row.get("afecta_stock_libre"), 0), 2),
            "Afecta stock asignado": round(_safe_float(row.get("afecta_stock_asignado"), 0), 2),
            "Referencia tipo": normalize_text(row.get("referencia_tipo")),
            "Referencia id": normalize_text(row.get("referencia_id")),
            "Observación": normalize_text(row.get("observacion")),
            "Usuario": normalize_text(row.get("created_by")),
        })
    return normalized



def list_salidas_por_envio(
    *,
    search_text: str | None = None,
    tipo_cierre: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    ensure_inventory_phase1_schema()
    where_parts: list[str] = []
    params: list[Any] = []

    search = normalize_text(search_text)
    if search:
        like = f"%{search}%"
        where_parts.append(
            "(lower(p.id_pedido) LIKE lower(?) OR lower(COALESCE(p.cliente, '')) LIKE lower(?) OR "
            "lower(COALESCE(pc.usuario, '')) LIKE lower(?))"
        )
        params.extend([like, like, like])

    tipo = normalize_text(tipo_cierre).upper()
    if tipo in {"TOTAL", "PARCIAL"}:
        where_parts.append("COALESCE(pc.tipo_cierre, '') = ?")
        params.append(tipo)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    params.append(limit)

    rows = fetch_all(
        f"""
        WITH impacto AS (
            SELECT
                pc.id AS pedido_cierre_id,
                pc.pedido_id,
                pc.fecha_envio,
                pc.tipo_cierre,
                pc.usuario,
                COUNT(DISTINCT CASE WHEN COALESCE(m.sku, '') <> '' THEN m.sku END) AS skus_tocados,
                COALESCE(SUM(CASE WHEN m.movimiento_tipo = 'SALIDA_DESDE_ASIGNADO' THEN ABS(COALESCE(m.afecta_total, 0)) ELSE 0 END), 0) AS total_salida,
                COALESCE(SUM(CASE WHEN m.movimiento_tipo = 'LIBERACION_ASIGNADO_POR_CIERRE' THEN COALESCE(m.afecta_stock_libre, 0) ELSE 0 END), 0) AS total_liberado,
                COALESCE(SUM(CASE WHEN m.movimiento_tipo = 'SALIDA_DESDE_ASIGNADO' THEN 1 ELSE 0 END), 0) AS movimientos_salida,
                COALESCE(SUM(CASE WHEN m.movimiento_tipo = 'LIBERACION_ASIGNADO_POR_CIERRE' THEN 1 ELSE 0 END), 0) AS movimientos_liberacion
            FROM pedido_cierres pc
            LEFT JOIN inventario_movimientos m
                ON m.referencia_tipo = 'PEDIDO_CIERRE'
               AND m.referencia_id = pc.id
            GROUP BY pc.id, pc.pedido_id, pc.fecha_envio, pc.tipo_cierre, pc.usuario
        )
        SELECT
            impacto.pedido_cierre_id,
            impacto.pedido_id,
            impacto.fecha_envio,
            impacto.tipo_cierre,
            impacto.usuario,
            impacto.skus_tocados,
            impacto.total_salida,
            impacto.total_liberado,
            impacto.movimientos_salida,
            impacto.movimientos_liberacion,
            p.id_pedido AS id_pedido_negocio,
            p.cliente,
            p.fecha_logistica
        FROM impacto
        JOIN pedidos p ON p.id = impacto.pedido_id
        {where_sql}
        ORDER BY impacto.fecha_envio DESC, impacto.pedido_cierre_id DESC
        LIMIT ?
        """,
        params,
    )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({
            "Pedido cierre id": _safe_int(row.get("pedido_cierre_id"), 0),
            "Pedido id": _safe_int(row.get("pedido_id"), 0),
            "ID_pedido": normalize_text(row.get("id_pedido_negocio")),
            "Cliente": normalize_text(row.get("cliente")),
            "Fecha envío": normalize_text(row.get("fecha_envio")),
            "Fecha logística": normalize_text(row.get("fecha_logistica")),
            "Tipo cierre": normalize_text(row.get("tipo_cierre")),
            "Usuario": normalize_text(row.get("usuario")),
            "SKU tocados": _safe_int(row.get("skus_tocados"), 0),
            "Total salida física": round(_safe_float(row.get("total_salida"), 0.0), 2),
            "Total liberado a stock": round(_safe_float(row.get("total_liberado"), 0.0), 2),
            "Movimientos salida": _safe_int(row.get("movimientos_salida"), 0),
            "Movimientos liberación": _safe_int(row.get("movimientos_liberacion"), 0),
        })
    return normalized


def get_salidas_por_envio_resumen() -> dict[str, Any]:
    rows = list_salidas_por_envio(limit=100000)
    return {
        "cierres_con_impacto": len(rows),
        "total_salida_fisica": round(sum(_safe_float(r.get("Total salida física"), 0) for r in rows), 2),
        "total_liberado_stock": round(sum(_safe_float(r.get("Total liberado a stock"), 0) for r in rows), 2),
        "total_skus_tocados": sum(_safe_int(r.get("SKU tocados"), 0) for r in rows),
    }


def get_pedido_cierre_inventory_impact(
    *,
    pedido_cierre_id: int | None = None,
    pedido_id: int | None = None,
) -> list[dict[str, Any]]:
    ensure_inventory_phase1_schema()
    cierre_id = pedido_cierre_id
    if cierre_id is None and pedido_id is not None:
        cierre = fetch_one("SELECT id FROM pedido_cierres WHERE pedido_id = ?", (pedido_id,))
        cierre_id = _safe_int((cierre or {}).get("id"), 0)
    if not cierre_id:
        return []

    rows = fetch_all(
        """
        SELECT
            m.sku,
            COALESCE(i.descripcion, plc.descripcion, '') AS descripcion,
            SUM(CASE WHEN m.movimiento_tipo = 'SALIDA_DESDE_ASIGNADO' THEN ABS(COALESCE(m.afecta_total, 0)) ELSE 0 END) AS cantidad_salida,
            SUM(CASE WHEN m.movimiento_tipo = 'LIBERACION_ASIGNADO_POR_CIERRE' THEN COALESCE(m.afecta_stock_libre, 0) ELSE 0 END) AS cantidad_liberada,
            SUM(COALESCE(m.afecta_total, 0)) AS afecta_total,
            SUM(COALESCE(m.afecta_stock_libre, 0)) AS afecta_stock_libre,
            SUM(COALESCE(m.afecta_stock_asignado, 0)) AS afecta_stock_asignado,
            MAX(m.created_at) AS fecha_movimiento
        FROM inventario_movimientos m
        LEFT JOIN inventario_sku i ON i.sku = m.sku
        LEFT JOIN pedido_linea_cierres plc ON plc.pedido_cierre_id = ? AND plc.sku = m.sku
        WHERE m.referencia_tipo = 'PEDIDO_CIERRE'
          AND m.referencia_id = ?
        GROUP BY m.sku, COALESCE(i.descripcion, plc.descripcion, '')
        ORDER BY m.sku ASC
        """,
        (cierre_id, cierre_id),
    )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({
            "SKU": normalize_text(row.get("sku")),
            "Descripción": normalize_text(row.get("descripcion")),
            "Cantidad salida física": round(_safe_float(row.get("cantidad_salida"), 0.0), 2),
            "Cantidad liberada a stock": round(_safe_float(row.get("cantidad_liberada"), 0.0), 2),
            "Afecta total": round(_safe_float(row.get("afecta_total"), 0.0), 2),
            "Afecta stock libre": round(_safe_float(row.get("afecta_stock_libre"), 0.0), 2),
            "Afecta stock asignado": round(_safe_float(row.get("afecta_stock_asignado"), 0.0), 2),
            "Fecha movimiento": normalize_text(row.get("fecha_movimiento")),
        })
    return normalized



# -----------------------------------------------------------------------------
# Entradas de producción + impacto en inventario
# -----------------------------------------------------------------------------
def _build_entradas_produccion_source_cte() -> str:
    return """
        WITH movimientos_desde_decision AS (
            SELECT
                m.id AS movimiento_id,
                ce.import_job_id AS import_job_id,
                es.id AS entrada_staging_id,
                ed.id AS decision_id,
                COALESCE(NULLIF(m.sku, ''), es.sku) AS sku,
                COALESCE(NULLIF(es.descripcion, ''), '') AS descripcion_entrada,
                ed.decision_type AS decision_type,
                m.movimiento_tipo AS movimiento_tipo,
                COALESCE(m.afecta_total, 0) AS afecta_total,
                COALESCE(m.afecta_stock_libre, 0) AS afecta_stock_libre,
                COALESCE(m.afecta_stock_asignado, 0) AS afecta_stock_asignado,
                m.created_at AS created_at,
                m.created_by AS created_by
            FROM inventario_movimientos m
            JOIN entrada_decisiones ed
              ON m.referencia_tipo = 'ENTRADA_DECISION'
             AND m.referencia_id = ed.id
            JOIN entradas_staging es ON es.id = ed.entrada_staging_id
            JOIN cortes_entrada ce ON ce.id = es.corte_id
            WHERE m.movimiento_tipo = 'ENTRADA_A_STOCK'

            UNION ALL

            SELECT
                m.id AS movimiento_id,
                ce.import_job_id AS import_job_id,
                es.id AS entrada_staging_id,
                ed.id AS decision_id,
                COALESCE(NULLIF(m.sku, ''), es.sku) AS sku,
                COALESCE(NULLIF(es.descripcion, ''), '') AS descripcion_entrada,
                ed.decision_type AS decision_type,
                m.movimiento_tipo AS movimiento_tipo,
                COALESCE(m.afecta_total, 0) AS afecta_total,
                COALESCE(m.afecta_stock_libre, 0) AS afecta_stock_libre,
                COALESCE(m.afecta_stock_asignado, 0) AS afecta_stock_asignado,
                m.created_at AS created_at,
                m.created_by AS created_by
            FROM inventario_movimientos m
            JOIN asignaciones a
              ON m.referencia_tipo = 'ASIGNACION'
             AND m.referencia_id = a.id
            JOIN entrada_decisiones ed ON ed.id = a.decision_id
            JOIN entradas_staging es ON es.id = ed.entrada_staging_id
            JOIN cortes_entrada ce ON ce.id = es.corte_id
            WHERE m.movimiento_tipo = 'ENTRADA_A_STOCK'
        )
    """


def list_entradas_produccion_jobs(
    *,
    search_text: str | None = None,
    status: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    ensure_inventory_phase1_schema()

    where_parts = ["ij.import_type = 'ENTRADAS_PRODUCCION'"]
    params: list[Any] = []
    search = normalize_text(search_text)
    if search:
        like = f"%{search}%"
        where_parts.append("(lower(COALESCE(ij.file_name, '')) LIKE lower(?) OR lower(COALESCE(ij.created_by, '')) LIKE lower(?))")
        params.extend([like, like])

    normalized_status = normalize_text(status).upper()
    if normalized_status:
        where_parts.append("COALESCE(ij.status, '') = ?")
        params.append(normalized_status)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    params.append(limit)

    rows = fetch_all(
        f"""
        {_build_entradas_produccion_source_cte()}
        , jobs_base AS (
            SELECT
                ij.id AS import_job_id,
                ij.file_name,
                ij.status,
                ij.created_by,
                ij.started_at,
                ij.finished_at,
                COALESCE(SUM(CASE WHEN COALESCE(es.estado_revision, '') <> 'IGNORADA' THEN COALESCE(es.cantidad, 0) ELSE 0 END), 0) AS total_recibido_archivo,
                COUNT(DISTINCT ce.id) AS cortes,
                COUNT(DISTINCT es.id) AS entradas_archivo,
                COUNT(DISTINCT CASE WHEN COALESCE(es.sku, '') <> '' THEN es.sku END) AS skus_archivo,
                SUM(CASE WHEN COALESCE(es.estado_revision, '') = 'RESUELTA' THEN 1 ELSE 0 END) AS entradas_resueltas,
                SUM(CASE WHEN COALESCE(es.estado_revision, '') = 'EN_REVISION' THEN 1 ELSE 0 END) AS entradas_revision,
                SUM(CASE WHEN COALESCE(es.estado_revision, '') = 'IGNORADA' THEN 1 ELSE 0 END) AS entradas_ignoradas
            FROM import_jobs ij
            LEFT JOIN cortes_entrada ce ON ce.import_job_id = ij.id
            LEFT JOIN entradas_staging es ON es.corte_id = ce.id
            {where_sql}
            GROUP BY ij.id, ij.file_name, ij.status, ij.created_by, ij.started_at, ij.finished_at
        )
        SELECT
            jb.import_job_id,
            jb.file_name,
            jb.status,
            jb.created_by,
            jb.started_at,
            jb.finished_at,
            jb.total_recibido_archivo,
            jb.cortes,
            jb.entradas_archivo,
            jb.skus_archivo,
            jb.entradas_resueltas,
            jb.entradas_revision,
            jb.entradas_ignoradas,
            COUNT(DISTINCT ms.entrada_staging_id) AS entradas_aplicadas,
            COUNT(DISTINCT ms.decision_id) AS decisiones_aplicadas,
            COUNT(DISTINCT CASE WHEN COALESCE(ms.sku, '') <> '' THEN ms.sku END) AS skus_tocados,
            COALESCE(SUM(COALESCE(ms.afecta_total, 0)), 0) AS total_aplicado_inventario,
            COALESCE(SUM(COALESCE(ms.afecta_stock_libre, 0)), 0) AS total_a_stock_libre,
            COALESCE(SUM(COALESCE(ms.afecta_stock_asignado, 0)), 0) AS total_a_stock_asignado,
            MAX(ms.created_at) AS ultima_aplicacion
        FROM jobs_base jb
        LEFT JOIN movimientos_desde_decision ms ON ms.import_job_id = jb.import_job_id
        GROUP BY
            jb.import_job_id, jb.file_name, jb.status, jb.created_by, jb.started_at, jb.finished_at,
            jb.total_recibido_archivo, jb.cortes, jb.entradas_archivo, jb.skus_archivo,
            jb.entradas_resueltas, jb.entradas_revision, jb.entradas_ignoradas
        ORDER BY jb.import_job_id DESC
        LIMIT ?
        """,
        params,
    )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({
            'Import job id': _safe_int(row.get('import_job_id'), 0),
            'Archivo': normalize_text(row.get('file_name')),
            'Estado': normalize_text(row.get('status')),
            'Creado por': normalize_text(row.get('created_by')),
            'Inicio': normalize_text(row.get('started_at')),
            'Fin': normalize_text(row.get('finished_at')),
            'Cortes': _safe_int(row.get('cortes'), 0),
            'Entradas archivo': _safe_int(row.get('entradas_archivo'), 0),
            'SKU archivo': _safe_int(row.get('skus_archivo'), 0),
            'Entradas resueltas': _safe_int(row.get('entradas_resueltas'), 0),
            'Entradas revisión': _safe_int(row.get('entradas_revision'), 0),
            'Entradas ignoradas': _safe_int(row.get('entradas_ignoradas'), 0),
            'Entradas aplicadas': _safe_int(row.get('entradas_aplicadas'), 0),
            'Decisiones aplicadas': _safe_int(row.get('decisiones_aplicadas'), 0),
            'SKU tocados': _safe_int(row.get('skus_tocados'), 0),
            'Total recibido archivo': round(_safe_float(row.get('total_recibido_archivo'), 0.0), 2),
            'Total aplicado inventario': round(_safe_float(row.get('total_aplicado_inventario'), 0.0), 2),
            'Total a stock libre': round(_safe_float(row.get('total_a_stock_libre'), 0.0), 2),
            'Total a stock asignado': round(_safe_float(row.get('total_a_stock_asignado'), 0.0), 2),
            'Última aplicación': normalize_text(row.get('ultima_aplicacion')),
        })
    return normalized


def get_entradas_produccion_resumen() -> dict[str, Any]:
    rows = list_entradas_produccion_jobs(limit=10000)
    return {
        'jobs_entradas': len(rows),
        'total_recibido_archivo': round(sum(_safe_float(r.get('Total recibido archivo'), 0) for r in rows), 2),
        'total_aplicado_inventario': round(sum(_safe_float(r.get('Total aplicado inventario'), 0) for r in rows), 2),
        'total_a_stock_libre': round(sum(_safe_float(r.get('Total a stock libre'), 0) for r in rows), 2),
        'total_a_stock_asignado': round(sum(_safe_float(r.get('Total a stock asignado'), 0) for r in rows), 2),
        'jobs_con_aplicacion': sum(1 for r in rows if _safe_float(r.get('Total aplicado inventario'), 0) > 0),
    }


def get_entrada_produccion_job_impact(import_job_id: int) -> list[dict[str, Any]]:
    ensure_inventory_phase1_schema()
    job_id = _safe_int(import_job_id, 0)
    if job_id <= 0:
        return []

    rows = fetch_all(
        f"""
        {_build_entradas_produccion_source_cte()}
        SELECT
            ms.sku,
            COALESCE(NULLIF(i.descripcion, ''), NULLIF(ms.descripcion_entrada, ''), '') AS descripcion,
            COUNT(DISTINCT ms.entrada_staging_id) AS entradas_origen,
            COUNT(DISTINCT ms.decision_id) AS decisiones_aplicadas,
            COALESCE(SUM(COALESCE(ms.afecta_total, 0)), 0) AS total_aplicado,
            COALESCE(SUM(COALESCE(ms.afecta_stock_libre, 0)), 0) AS total_stock_libre,
            COALESCE(SUM(COALESCE(ms.afecta_stock_asignado, 0)), 0) AS total_stock_asignado,
            MAX(ms.created_at) AS ultima_aplicacion
        FROM movimientos_desde_decision ms
        LEFT JOIN inventario_sku i ON i.sku = ms.sku
        WHERE ms.import_job_id = ?
        GROUP BY ms.sku, COALESCE(NULLIF(i.descripcion, ''), NULLIF(ms.descripcion_entrada, ''), '')
        ORDER BY ms.sku ASC
        """,
        (job_id,),
    )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({
            'SKU': normalize_text(row.get('sku')),
            'Descripción': normalize_text(row.get('descripcion')),
            'Entradas origen': _safe_int(row.get('entradas_origen'), 0),
            'Decisiones aplicadas': _safe_int(row.get('decisiones_aplicadas'), 0),
            'Total aplicado': round(_safe_float(row.get('total_aplicado'), 0.0), 2),
            'A stock libre': round(_safe_float(row.get('total_stock_libre'), 0.0), 2),
            'A stock asignado': round(_safe_float(row.get('total_stock_asignado'), 0.0), 2),
            'Última aplicación': normalize_text(row.get('ultima_aplicacion')),
        })
    return normalized


def get_entrada_produccion_job_resumen(import_job_id: int) -> dict[str, Any]:
    rows = list_entradas_produccion_jobs(limit=10000)
    target = next((row for row in rows if _safe_int(row.get('Import job id'), 0) == _safe_int(import_job_id, 0)), None)
    return target or {}

def list_inventory_distinct_filter_values() -> dict[str, list[str]]:
    tipos = fetch_all("SELECT DISTINCT movimiento_tipo FROM inventario_movimientos WHERE COALESCE(movimiento_tipo, '') <> '' ORDER BY movimiento_tipo")
    refs = fetch_all("SELECT DISTINCT referencia_tipo FROM inventario_movimientos WHERE COALESCE(referencia_tipo, '') <> '' ORDER BY referencia_tipo")
    users = fetch_all("SELECT DISTINCT created_by FROM inventario_movimientos WHERE COALESCE(created_by, '') <> '' ORDER BY created_by")
    return {
        "movimiento_tipo": [normalize_text(r.get("movimiento_tipo")) for r in tipos if normalize_text(r.get("movimiento_tipo"))],
        "referencia_tipo": [normalize_text(r.get("referencia_tipo")) for r in refs if normalize_text(r.get("referencia_tipo"))],
        "created_by": [normalize_text(r.get("created_by")) for r in users if normalize_text(r.get("created_by"))],
    }


def create_manual_inventory_adjustment(
    *,
    sku: Any,
    adjustment_kind: Any,
    quantity: Any,
    observacion: Any = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    ensure_inventory_phase1_schema()
    normalized_sku = normalize_sku(sku)
    if not normalized_sku:
        raise ValueError("El SKU es obligatorio para registrar un ajuste manual.")

    kind = normalize_manual_adjustment_kind(adjustment_kind)
    qty = round(validate_manual_adjustment_quantity(quantity), 6)
    user = normalize_text(created_by) or "Consulta"
    obs = normalize_text(observacion)

    with transaction() as conn:
        row = conn.execute(
            "SELECT sku, descripcion, stock_libre, stock_asignado FROM inventario_sku WHERE sku = ?",
            (normalized_sku,),
        ).fetchone()
        if row is None:
            raise ValueError(f"No existe el SKU {normalized_sku} en inventario_sku. Para esta fase ajusta solo SKU ya cargados.")
        row = dict(row) if not isinstance(row, dict) else row

        stock_libre_actual = _safe_float(row.get("stock_libre"), 0)
        stock_asignado_actual = _safe_float(row.get("stock_asignado"), 0)
        existencia_actual = round(stock_libre_actual + stock_asignado_actual, 6)

        if kind == "SALIDA" and qty > stock_libre_actual + 1e-9:
            raise ValueError(
                f"El ajuste de salida excede el stock libre actual del SKU {normalized_sku}. "
                f"Stock libre={round(stock_libre_actual, 2)}, solicitado={round(qty, 2)}."
            )

        delta = qty if kind == "ENTRADA" else -qty
        nuevo_stock_libre = round(stock_libre_actual + delta, 6)
        nueva_existencia = round(nuevo_stock_libre + stock_asignado_actual, 6)

        conn.execute(
            """
            UPDATE inventario_sku
            SET stock_libre = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE sku = ?
            """,
            (nuevo_stock_libre, normalized_sku),
        )

        movimiento_obs = f"AJUSTE_{kind} | {obs}" if obs else f"AJUSTE_{kind}"
        cursor = conn.execute(
            """
            INSERT INTO inventario_movimientos (
                sku, movimiento_tipo, afecta_total, afecta_stock_libre, afecta_stock_asignado,
                referencia_tipo, referencia_id, observacion, created_by
            )
            VALUES (?, 'AJUSTE_MANUAL', ?, ?, 0, 'AJUSTE_MANUAL', NULL, ?, ?)
            """,
            (normalized_sku, delta, delta, movimiento_obs, user),
        )
        movimiento_id = int(cursor.lastrowid)

        conn.executemany(
            """
            INSERT INTO auditoria_cambios (
                tabla, registro_id, campo, valor_anterior, valor_nuevo, usuario, motivo
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "inventario_sku",
                    normalized_sku,
                    "stock_libre",
                    str(stock_libre_actual),
                    str(nuevo_stock_libre),
                    user,
                    movimiento_obs,
                ),
                (
                    "inventario_sku",
                    normalized_sku,
                    "existencia_fisica",
                    str(existencia_actual),
                    str(nueva_existencia),
                    user,
                    movimiento_obs,
                ),
            ],
        )

    return {
        "movimiento_id": movimiento_id,
        "sku": normalized_sku,
        "descripcion": normalize_text(row.get("descripcion")),
        "adjustment_kind": kind,
        "cantidad": round(qty, 2),
        "stock_libre_anterior": round(stock_libre_actual, 2),
        "stock_libre_nuevo": round(nuevo_stock_libre, 2),
        "existencia_anterior": round(existencia_actual, 2),
        "existencia_nueva": round(nueva_existencia, 2),
        "usuario": user,
        "observacion": movimiento_obs,
    }
