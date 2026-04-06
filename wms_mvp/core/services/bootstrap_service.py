from __future__ import annotations

import hashlib
import json
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from wms_mvp.core.rules.inventario_rules import normalize_descripcion, normalize_sku, normalize_text
from wms_mvp.core.services.inventario_service import (
    apply_initial_inventory_load,
    build_initial_inventory_preview,
    materialize_operational_stock_snapshot,
)
from wms_mvp.db.connection import fetch_all, fetch_one, table_exists, transaction
from wms_mvp.etl.entradas_loader import analyze_entradas_file
from wms_mvp.etl.pedidos_loader import analyze_carga_inicial_file, import_carga_inicial_from_file

BOOTSTRAP_TAG = "[BOOTSTRAP_INICIAL]"


class BootstrapError(RuntimeError):
    """Error controlado del proceso de bootstrap inicial."""


@contextmanager
def _temp_file_from_bytes(file_name: str, file_bytes: bytes) -> Iterator[Path]:
    suffix = Path(file_name or "archivo.xlsx").suffix or ".xlsx"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(file_bytes)
        tmp_path = Path(tmp.name)
    try:
        yield tmp_path
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _file_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()



def ensure_bootstrap_schema() -> None:
    with transaction() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bootstrap_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL DEFAULT 'PENDIENTE',
                inventory_file_name TEXT,
                pedidos_file_name TEXT,
                entradas_file_name TEXT,
                inventory_file_hash TEXT,
                pedidos_file_hash TEXT,
                entradas_file_hash TEXT,
                summary_json TEXT,
                notes TEXT,
                created_by TEXT,
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TEXT
            )
            """
        )



def _register_bootstrap_run(
    *,
    inventory_file_name: str,
    pedidos_file_name: str,
    entradas_file_name: str,
    inventory_file_bytes: bytes,
    pedidos_file_bytes: bytes,
    entradas_file_bytes: bytes,
    created_by: str | None,
    notes: str | None = None,
) -> int:
    ensure_bootstrap_schema()
    with transaction() as conn:
        cur = conn.execute(
            """
            INSERT INTO bootstrap_runs (
                status,
                inventory_file_name,
                pedidos_file_name,
                entradas_file_name,
                inventory_file_hash,
                pedidos_file_hash,
                entradas_file_hash,
                notes,
                created_by
            )
            VALUES ('PROCESANDO', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                inventory_file_name,
                pedidos_file_name,
                entradas_file_name,
                _file_hash(inventory_file_bytes),
                _file_hash(pedidos_file_bytes),
                _file_hash(entradas_file_bytes),
                notes,
                normalize_text(created_by) or "Admin",
            ),
        )
        return int(cur.lastrowid)



def _finish_bootstrap_run(run_id: int, *, status: str, summary: dict[str, Any], notes: str | None = None) -> None:
    ensure_bootstrap_schema()
    with transaction() as conn:
        conn.execute(
            """
            UPDATE bootstrap_runs
            SET status = ?,
                summary_json = ?,
                notes = COALESCE(?, notes),
                finished_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (status, json.dumps(summary, ensure_ascii=False), notes, run_id),
        )



def list_bootstrap_runs(limit: int = 20) -> list[dict[str, Any]]:
    ensure_bootstrap_schema()
    return fetch_all(
        """
        SELECT *
        FROM bootstrap_runs
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )



def _inventory_base_map_from_preview(preview_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    base_map: dict[str, dict[str, Any]] = {}
    for row in preview_payload.get("valid_rows", []):
        sku = normalize_sku(row.get("SKU"))
        if not sku:
            continue
        base_map[sku] = {
            "sku": sku,
            "descripcion": normalize_descripcion(row.get("Descripción")),
            "stock": float(row.get("Stock") or 0),
            "fecha_base": normalize_text(row.get("Fecha actualización")),
        }
    return base_map



def _summarize_entries_against_inventory(entries_rows: list[dict[str, Any]], inventory_base_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    total_rows = 0
    valid_rows = 0
    skipped_pre_base = 0
    missing_inventory = 0
    invalid_rows = 0
    total_qty_valid = 0.0
    skus_touched: set[str] = set()
    min_date: str | None = None
    max_date: str | None = None

    for row in entries_rows:
        total_rows += 1
        sku = normalize_sku(row.get("sku"))
        fecha = normalize_text(row.get("fecha"))
        cantidad = float(row.get("cantidad") or 0)
        if fecha:
            min_date = fecha if min_date is None or fecha < min_date else min_date
            max_date = fecha if max_date is None or fecha > max_date else max_date
        if not sku or not fecha or cantidad <= 0:
            invalid_rows += 1
            continue
        base = inventory_base_map.get(sku)
        if not base:
            missing_inventory += 1
            continue
        fecha_base = normalize_text(base.get("fecha_base"))
        if fecha_base and fecha <= fecha_base:
            skipped_pre_base += 1
            continue
        valid_rows += 1
        total_qty_valid += cantidad
        skus_touched.add(sku)

    return {
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "skipped_pre_base": skipped_pre_base,
        "missing_inventory": missing_inventory,
        "invalid_rows": invalid_rows,
        "total_qty_valid": round(total_qty_valid, 2),
        "sku_validos": len(skus_touched),
        "min_fecha": min_date,
        "max_fecha": max_date,
    }



def build_bootstrap_preview(
    *,
    inventory_file_name: str,
    inventory_file_bytes: bytes,
    pedidos_file_name: str,
    pedidos_file_bytes: bytes,
    entradas_file_name: str,
    entradas_file_bytes: bytes,
) -> dict[str, Any]:
    ensure_bootstrap_schema()

    inventory_preview = build_initial_inventory_preview(
        file_name=inventory_file_name,
        file_bytes=inventory_file_bytes,
    )
    inventory_base_map = _inventory_base_map_from_preview(inventory_preview)

    with _temp_file_from_bytes(pedidos_file_name, pedidos_file_bytes) as pedidos_path:
        pedidos_analysis = analyze_carga_inicial_file(file_path=pedidos_path)

    with _temp_file_from_bytes(entradas_file_name, entradas_file_bytes) as entradas_path:
        entradas_analysis = analyze_entradas_file(file_path=entradas_path)

    inventory_dates = sorted(
        {
            normalize_text(row.get("Fecha actualización"))
            for row in inventory_preview.get("valid_rows", [])
            if normalize_text(row.get("Fecha actualización"))
        }
    )
    entradas_summary = _summarize_entries_against_inventory(
        entradas_analysis.get("normalized_rows", []),
        inventory_base_map,
    )

    return {
        "inventory_preview": inventory_preview,
        "pedidos_analysis": {
            "file_name": pedidos_analysis.get("file_name"),
            "total_valid_rows": pedidos_analysis.get("total_valid_rows", 0),
            "total_rejected_rows": pedidos_analysis.get("total_rejected_rows", 0),
            "total_pedidos_detected": pedidos_analysis.get("total_pedidos_detected", 0),
            "total_pedidos_activos_detected": pedidos_analysis.get("total_pedidos_activos_detected", 0),
            "total_pedidos_enviados_detected": pedidos_analysis.get("total_pedidos_enviados_detected", 0),
            "per_sheet_summary": pedidos_analysis.get("per_sheet_summary", []),
        },
        "entradas_analysis": {
            "file_name": entradas_analysis.get("file_name"),
            "resolved_sheet": entradas_analysis.get("resolved_sheet"),
            "detected_header_row": entradas_analysis.get("detected_header_row"),
            "total_normalized_rows": len(entradas_analysis.get("normalized_rows", [])),
            "total_review_rows": len(entradas_analysis.get("review_rows", [])),
            "detected_cortes": len(entradas_analysis.get("detected_cortes", [])),
            "entries_summary": entradas_summary,
        },
        "bootstrap_summary": {
            "inventario_valid_skus": inventory_preview.get("valid_count", 0),
            "inventario_review_rows": inventory_preview.get("review_count", 0),
            "inventario_fecha_min": inventory_dates[0] if inventory_dates else None,
            "inventario_fecha_max": inventory_dates[-1] if inventory_dates else None,
            "pedidos_activos_detectados": pedidos_analysis.get("total_pedidos_activos_detected", 0),
            "pedidos_enviados_detectados": pedidos_analysis.get("total_pedidos_enviados_detected", 0),
            "entradas_validas_post_corte": entradas_summary.get("valid_rows", 0),
            "entradas_sku_sin_base": entradas_summary.get("missing_inventory", 0),
            "entradas_omitidas_por_fecha_base": entradas_summary.get("skipped_pre_base", 0),
            "entradas_invalidas": entradas_summary.get("invalid_rows", 0),
        },
        "preview_created_at": pd.Timestamp.utcnow().isoformat(),
    }



def _clear_bootstrap_target_tables() -> dict[str, int]:
    table_order = [
        "reasignaciones_reserva",
        "asignaciones",
        "pedido_linea_cierres",
        "pedido_cierres",
        "pedido_lineas",
        "pedidos",
        "inventario_corte_detalle",
        "inventario_cortes",
        "entrada_decisiones",
        "entradas_staging",
        "cortes_entrada",
        "inventario_movimientos",
        "inventario_sku",
    ]
    counts: dict[str, int] = {}
    with transaction() as conn:
        for table_name in table_order:
            if not table_exists(table_name):
                continue
            row = conn.execute(f"SELECT COUNT(*) AS total FROM {table_name}").fetchone()
            counts[table_name] = int((row["total"] if isinstance(row, dict) else row[0]) or 0)
            conn.execute(f"DELETE FROM {table_name}")
    return counts



def _load_inventory_base_dates() -> dict[str, dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT sku, descripcion, fecha_actualizacion, stock_base_libre, stock_base_asignado
        FROM inventario_sku
        """
    )
    base_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        sku = normalize_sku(row.get("sku"))
        if sku:
            base_map[sku] = {
                "sku": sku,
                "descripcion": normalize_descripcion(row.get("descripcion")),
                "fecha_base": normalize_text(row.get("fecha_actualizacion")),
                "stock_base_total": float(row.get("stock_base_libre") or 0) + float(row.get("stock_base_asignado") or 0),
            }
    return base_map



def _apply_historical_entries(
    *,
    entradas_file_name: str,
    entradas_file_bytes: bytes,
    created_by: str | None,
    bootstrap_run_id: int,
) -> dict[str, Any]:
    base_map = _load_inventory_base_dates()
    if not base_map:
        raise BootstrapError("No existe inventario base después de la carga inicial. No se pueden aplicar entradas históricas.")

    with _temp_file_from_bytes(entradas_file_name, entradas_file_bytes) as entradas_path:
        analysis = analyze_entradas_file(file_path=entradas_path)

    valid_movements: list[tuple[Any, ...]] = []
    review_rows: list[dict[str, Any]] = []
    touched_skus: set[str] = set()
    total_qty = 0.0

    for idx, row in enumerate(analysis.get("normalized_rows", []), start=1):
        sku = normalize_sku(row.get("sku"))
        fecha = normalize_text(row.get("fecha"))
        hora = normalize_text(row.get("hora")) or "00:00:00"
        descripcion = normalize_descripcion(row.get("descripcion"))
        cantidad = float(row.get("cantidad") or 0)

        reasons: list[str] = []
        if not sku:
            reasons.append("sin_sku")
        if not fecha:
            reasons.append("sin_fecha")
        if cantidad <= 0:
            reasons.append("cantidad_invalida")

        base = base_map.get(sku or "") if sku else None
        if not base:
            reasons.append("sku_sin_base_inventario")
        else:
            fecha_base = normalize_text(base.get("fecha_base"))
            if fecha_base and fecha <= fecha_base:
                reasons.append("entrada_ya_incluida_en_corte_base")

        if reasons:
            review_rows.append(
                {
                    "Fila": idx,
                    "SKU": sku,
                    "Descripción": descripcion,
                    "Fecha": fecha,
                    "Hora": hora,
                    "Cantidad": cantidad,
                    "Motivo revisión": " | ".join(reasons),
                }
            )
            continue

        ts = f"{fecha} {hora}"[:19]
        valid_movements.append(
            (
                sku,
                "ENTRADA_A_STOCK",
                cantidad,
                cantidad,
                0.0,
                "CARGA_INICIAL",
                bootstrap_run_id,
                f"{BOOTSTRAP_TAG} Entrada histórica aplicada automáticamente",
                normalize_text(created_by) or "Bootstrap",
                ts,
            )
        )
        touched_skus.add(sku)
        total_qty += cantidad

    if valid_movements:
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
                    created_by,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                valid_movements,
            )

    return {
        "total_rows": len(analysis.get("normalized_rows", [])),
        "applied_rows": len(valid_movements),
        "review_rows": len(review_rows),
        "total_qty_applied": round(total_qty, 2),
        "touched_skus": len(touched_skus),
        "review_preview": review_rows[:200],
        "detected_cortes": len(analysis.get("detected_cortes", [])),
    }



def apply_bootstrap_inicial(
    *,
    inventory_file_name: str,
    inventory_file_bytes: bytes,
    pedidos_file_name: str,
    pedidos_file_bytes: bytes,
    entradas_file_name: str,
    entradas_file_bytes: bytes,
    created_by: str | None = None,
    reset_existing: bool = True,
) -> dict[str, Any]:
    ensure_bootstrap_schema()

    run_id = _register_bootstrap_run(
        inventory_file_name=inventory_file_name,
        pedidos_file_name=pedidos_file_name,
        entradas_file_name=entradas_file_name,
        inventory_file_bytes=inventory_file_bytes,
        pedidos_file_bytes=pedidos_file_bytes,
        entradas_file_bytes=entradas_file_bytes,
        created_by=created_by,
        notes=f"{BOOTSTRAP_TAG} Inicialización integral del MVP",
    )

    try:
        deleted_counts = _clear_bootstrap_target_tables() if reset_existing else {}

        inventory_preview = build_initial_inventory_preview(
            file_name=inventory_file_name,
            file_bytes=inventory_file_bytes,
        )
        inventory_result = apply_initial_inventory_load(
            preview_payload=inventory_preview,
            file_bytes=inventory_file_bytes,
            created_by=created_by,
        )

        with _temp_file_from_bytes(pedidos_file_name, pedidos_file_bytes) as pedidos_path:
            pedidos_result = import_carga_inicial_from_file(
                file_path=pedidos_path,
                created_by=created_by,
                notes=f"{BOOTSTRAP_TAG} Carga inicial integral",
                save_copy_to_input=True,
                archive_source_copy=False,
                skip_existing=False,
            )

        entradas_result = _apply_historical_entries(
            entradas_file_name=entradas_file_name,
            entradas_file_bytes=entradas_file_bytes,
            created_by=created_by,
            bootstrap_run_id=run_id,
        )

        materialized = materialize_operational_stock_snapshot(created_by=created_by)

        summary = {
            "bootstrap_run_id": run_id,
            "deleted_counts": deleted_counts,
            "inventory": {
                "inserted_rows": inventory_result.get("inserted_rows", 0),
                "review_rows": inventory_result.get("review_rows", 0),
                "total_stock_valid": inventory_result.get("total_stock_valid", 0),
                "import_job_id": inventory_result.get("import_job_id"),
            },
            "pedidos": {
                "created_pedidos": len(pedidos_result.get("created_pedidos", [])),
                "created_activos": sum(1 for x in pedidos_result.get("created_pedidos", []) if x.get("sheet_role") == 'ACTIVO'),
                "created_enviados": sum(1 for x in pedidos_result.get("created_pedidos", []) if x.get("sheet_role") == 'ENVIADO_HISTORICO'),
                "skipped_existing": len(pedidos_result.get("skipped_existing", [])),
                "failed_pedidos": len(pedidos_result.get("failed_pedidos", [])),
                "import_job_id": pedidos_result.get("import_job_id"),
            },
            "entradas": entradas_result,
            "materialized": materialized,
        }
        _finish_bootstrap_run(run_id, status="COMPLETADO", summary=summary)
        return summary
    except Exception as exc:
        _finish_bootstrap_run(run_id, status="FALLIDO", summary={"error": str(exc)}, notes=f"{BOOTSTRAP_TAG} Error: {exc}")
        raise
