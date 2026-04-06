from __future__ import annotations

from datetime import datetime
from typing import Any

from wms_mvp.core.rules.pedidos_rules import normalize_text, to_float
from wms_mvp.core.rules.reservas_rules import (
    VALID_MOTIVO_REASIGNACION,
    normalize_motivo_reasignacion,
    validate_active_order_state,
    validate_distinct_lines,
    validate_reassign_quantity,
    validate_release_quantity,
    validate_same_sku,
)
from wms_mvp.core.services.asignacion_service import asignar_desde_stock_libre_a_linea
from wms_mvp.db.connection import fetch_all, fetch_one, transaction
from wms_mvp.db.repositories import asignaciones_repository, auditoria_repository, partidas_repository, pedidos_repository


RESERVA_TAG = "[RESERVAS_MVP]"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default



def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(round(float(value)))
    except Exception:
        return default



def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")



def ensure_reservas_phase3_schema() -> None:
    with transaction() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reasignaciones_reserva (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                sku TEXT NOT NULL,
                pedido_origen_id INTEGER NOT NULL,
                pedido_destino_id INTEGER NOT NULL,
                pedido_linea_origen_id INTEGER NOT NULL,
                pedido_linea_destino_id INTEGER NOT NULL,
                cantidad REAL NOT NULL DEFAULT 0 CHECK (cantidad > 0),
                motivo TEXT,
                observacion TEXT,
                usuario TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pedido_origen_id) REFERENCES pedidos(id) ON DELETE CASCADE,
                FOREIGN KEY (pedido_destino_id) REFERENCES pedidos(id) ON DELETE CASCADE,
                FOREIGN KEY (pedido_linea_origen_id) REFERENCES pedido_lineas(id) ON DELETE CASCADE,
                FOREIGN KEY (pedido_linea_destino_id) REFERENCES pedido_lineas(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reasignaciones_reserva_sku ON reasignaciones_reserva (sku, created_at DESC)"
        )



def _build_linea_operativa_row(row: dict[str, Any]) -> dict[str, Any]:
    cantidad_pedida = _safe_float(row.get("cantidad_pedida"), 0)
    cantidad_asignada = _safe_float(row.get("cantidad_asignada"), 0)
    cantidad_enviada = _safe_float(row.get("cantidad_enviada"), 0)
    cantidad_faltante = _safe_float(row.get("cantidad_faltante"), max(cantidad_pedida - cantidad_asignada, 0))
    return {
        "pedido_id": _safe_int(row.get("pedido_id"), 0),
        "pedido_linea_id": _safe_int(row.get("pedido_linea_id"), 0),
        "id_pedido": normalize_text(row.get("id_pedido")) or "",
        "cliente": normalize_text(row.get("cliente")) or "",
        "sku": normalize_text(row.get("sku")) or "",
        "descripcion": normalize_text(row.get("descripcion")) or "",
        "producto": normalize_text(row.get("producto")) or "",
        "color": normalize_text(row.get("color")) or "",
        "cantidad_pedida": round(cantidad_pedida, 6),
        "cantidad_asignada": round(cantidad_asignada, 6),
        "cantidad_enviada": round(cantidad_enviada, 6),
        "cantidad_faltante": round(cantidad_faltante, 6),
        "fecha_captura": normalize_text(row.get("fecha_captura") or row.get("pedido_fecha_captura")) or "",
        "fecha_produccion": normalize_text(row.get("fecha_produccion") or row.get("pedido_fecha_produccion")) or "",
        "fecha_logistica": normalize_text(row.get("fecha_logistica") or row.get("pedido_fecha_logistica")) or "",
        "estatus_venta": normalize_text(row.get("estatus_venta") or row.get("estatus_venta_operativo")) or "",
        "prioridad_manual": row.get("prioridad_manual"),
        "estado_operativo": normalize_text(row.get("estado_operativo")) or "",
    }



def _create_audit(tabla: str, registro_id: Any, campo: str, valor_anterior: Any, valor_nuevo: Any, usuario: str | None, motivo: str | None) -> None:
    auditoria_repository.create_auditoria_cambio(
        {
            "tabla": tabla,
            "registro_id": str(registro_id),
            "campo": campo,
            "valor_anterior": None if valor_anterior is None else str(valor_anterior),
            "valor_nuevo": None if valor_nuevo is None else str(valor_nuevo),
            "usuario": usuario,
            "motivo": motivo,
        }
    )



def _append_obs(base: Any, extra: str) -> str:
    current = normalize_text(base) or ""
    return f"{current} | {extra}" if current else extra



def _active_assignment_rows_for_line(conn: Any, pedido_linea_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM asignaciones
        WHERE pedido_linea_id = ?
          AND estado = 'ACTIVA'
          AND COALESCE(cantidad, 0) > 0
        ORDER BY id DESC
        """,
        (pedido_linea_id,),
    ).fetchall()
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append(dict(row) if not isinstance(row, dict) else row)
    return normalized



def _take_assignment_chunks(conn: Any, *, pedido_linea_id: int, cantidad: float, usuario: str | None, observacion: str | None, motivo_tag: str) -> list[dict[str, Any]]:
    remaining = float(cantidad)
    rows = _active_assignment_rows_for_line(conn, pedido_linea_id)
    chunks: list[dict[str, Any]] = []

    total_activo = sum(_safe_float(row.get("cantidad"), 0) for row in rows)
    if remaining > total_activo + 1e-9:
        raise ValueError("La línea no tiene suficiente reserva activa para completar la operación.")

    for row in rows:
        if remaining <= 1e-9:
            break
        row_qty = _safe_float(row.get("cantidad"), 0)
        if row_qty <= 0:
            continue
        taken = min(row_qty, remaining)
        remaining -= taken
        chunks.append(
            {
                "cantidad": round(taken, 6),
                "fuente_asignacion": row.get("fuente_asignacion"),
                "entrada_staging_id": row.get("entrada_staging_id"),
                "decision_id": row.get("decision_id"),
                "sku": row.get("sku"),
            }
        )
        if taken >= row_qty - 1e-9:
            conn.execute(
                """
                UPDATE asignaciones
                SET estado = 'CANCELADA',
                    observacion = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (_append_obs(row.get("observacion"), motivo_tag), int(row.get("id"))),
            )
        else:
            new_qty = row_qty - taken
            conn.execute(
                """
                UPDATE asignaciones
                SET cantidad = ?,
                    observacion = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (new_qty, _append_obs(row.get("observacion"), motivo_tag), int(row.get("id"))),
            )
            _create_audit(
                tabla="asignaciones",
                registro_id=row.get("id"),
                campo="cantidad",
                valor_anterior=row_qty,
                valor_nuevo=new_qty,
                usuario=usuario,
                motivo=observacion or motivo_tag,
            )

    if remaining > 1e-9:
        raise ValueError("No fue posible descomponer completamente la reserva activa de la línea.")
    return chunks



def _update_linea_assignment(conn: Any, *, linea_id: int, delta: float) -> tuple[float, float, float]:
    linea = conn.execute(
        "SELECT cantidad_pedida, cantidad_asignada, cantidad_faltante FROM pedido_lineas WHERE id = ?",
        (linea_id,),
    ).fetchone()
    if linea is None:
        raise ValueError(f"No existe la línea {linea_id}.")
    linea = dict(linea) if not isinstance(linea, dict) else linea
    pedida = _safe_float(linea.get("cantidad_pedida"), 0)
    asignada_actual = _safe_float(linea.get("cantidad_asignada"), 0)
    nueva_asignada = asignada_actual + float(delta)
    if nueva_asignada < -1e-9:
        raise ValueError("La asignación resultante no puede ser negativa.")
    nueva_asignada = max(0.0, round(nueva_asignada, 6))
    nuevo_faltante = max(0.0, round(pedida - nueva_asignada, 6))
    conn.execute(
        """
        UPDATE pedido_lineas
        SET cantidad_asignada = ?,
            cantidad_faltante = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (nueva_asignada, nuevo_faltante, linea_id),
    )
    return asignada_actual, nueva_asignada, nuevo_faltante


# -----------------------------------------------------------------------------
# Consultas para UI
# -----------------------------------------------------------------------------
def get_reservas_resumen() -> dict[str, Any]:
    ensure_reservas_phase3_schema()
    row = fetch_one(
        """
        SELECT
            COUNT(DISTINCT CASE WHEN p.estado_operativo = 'ACTIVO' AND COALESCE(pl.cantidad_asignada, 0) > 0 THEN pl.sku END) AS skus_con_reserva,
            COUNT(DISTINCT CASE WHEN p.estado_operativo = 'ACTIVO' AND COALESCE(pl.cantidad_faltante, 0) > 0 THEN pl.sku END) AS skus_con_faltante,
            COALESCE(SUM(CASE WHEN p.estado_operativo = 'ACTIVO' THEN COALESCE(pl.cantidad_asignada, 0) ELSE 0 END), 0) AS reservado_total,
            COALESCE(SUM(CASE WHEN p.estado_operativo = 'ACTIVO' THEN COALESCE(pl.cantidad_faltante, 0) ELSE 0 END), 0) AS faltante_total
        FROM pedido_lineas pl
        INNER JOIN pedidos p ON p.id = pl.pedido_id
        """
    ) or {}
    libre = fetch_one("SELECT COALESCE(SUM(stock_libre), 0) AS total_stock_libre FROM inventario_sku") or {}
    reasig = fetch_one("SELECT COUNT(*) AS total FROM reasignaciones_reserva") or {}
    return {
        "skus_con_reserva": _safe_int(row.get("skus_con_reserva"), 0),
        "skus_con_faltante": _safe_int(row.get("skus_con_faltante"), 0),
        "reservado_total": round(_safe_float(row.get("reservado_total"), 0), 2),
        "faltante_total": round(_safe_float(row.get("faltante_total"), 0), 2),
        "stock_libre_total": round(_safe_float(libre.get("total_stock_libre"), 0), 2),
        "reasignaciones_total": _safe_int(reasig.get("total"), 0),
    }



def list_skus_para_asignar(search_text: str | None = None) -> list[dict[str, Any]]:
    ensure_reservas_phase3_schema()
    params: list[Any] = []
    where = ["COALESCE(i.stock_libre, 0) > 0", "COALESCE(dem.faltante_total, 0) > 0"]
    search = normalize_text(search_text)
    if search:
        where.append("(LOWER(i.sku) LIKE LOWER(?) OR LOWER(COALESCE(i.descripcion, '')) LIKE LOWER(?))")
        like = f"%{search}%"
        params.extend([like, like])
    rows = fetch_all(
        f"""
        SELECT
            i.sku,
            i.descripcion,
            COALESCE(i.stock_libre, 0) AS stock_libre,
            COALESCE(dem.faltante_total, 0) AS faltante_total,
            COALESCE(dem.lineas_con_faltante, 0) AS lineas_con_faltante
        FROM inventario_sku i
        LEFT JOIN (
            SELECT pl.sku,
                   SUM(COALESCE(pl.cantidad_faltante, 0)) AS faltante_total,
                   COUNT(*) AS lineas_con_faltante
            FROM pedido_lineas pl
            INNER JOIN pedidos p ON p.id = pl.pedido_id
            WHERE p.estado_operativo = 'ACTIVO'
              AND COALESCE(pl.cantidad_faltante, 0) > 0
            GROUP BY pl.sku
        ) dem ON dem.sku = i.sku
        WHERE {' AND '.join(where)}
        ORDER BY i.sku ASC
        """,
        params,
    )
    return [
        {
            "SKU": normalize_text(r.get("sku")) or "",
            "Descripción": normalize_text(r.get("descripcion")) or "",
            "Stock libre": round(_safe_float(r.get("stock_libre"), 0), 2),
            "Faltante activo": round(_safe_float(r.get("faltante_total"), 0), 2),
            "Líneas con faltante": _safe_int(r.get("lineas_con_faltante"), 0),
        }
        for r in rows
    ]



def list_skus_con_reserva(search_text: str | None = None) -> list[dict[str, Any]]:
    ensure_reservas_phase3_schema()
    params: list[Any] = []
    where = ["COALESCE(act.total_asignado, 0) > 0"]
    search = normalize_text(search_text)
    if search:
        where.append("(LOWER(act.sku) LIKE LOWER(?) OR LOWER(COALESCE(act.descripcion, '')) LIKE LOWER(?))")
        like = f"%{search}%"
        params.extend([like, like])
    rows = fetch_all(
        f"""
        SELECT
            act.sku,
            act.descripcion,
            act.total_asignado,
            act.lineas_asignadas,
            COALESCE(i.stock_libre, 0) AS stock_libre
        FROM (
            SELECT
                pl.sku,
                MAX(COALESCE(pl.descripcion, '')) AS descripcion,
                SUM(COALESCE(pl.cantidad_asignada, 0)) AS total_asignado,
                COUNT(*) AS lineas_asignadas
            FROM pedido_lineas pl
            INNER JOIN pedidos p ON p.id = pl.pedido_id
            WHERE p.estado_operativo = 'ACTIVO'
              AND COALESCE(pl.cantidad_asignada, 0) > 0
            GROUP BY pl.sku
        ) act
        LEFT JOIN inventario_sku i ON i.sku = act.sku
        WHERE {' AND '.join(where)}
        ORDER BY act.sku ASC
        """,
        params,
    )
    return [
        {
            "SKU": normalize_text(r.get("sku")) or "",
            "Descripción": normalize_text(r.get("descripcion")) or "",
            "Reservado activo": round(_safe_float(r.get("total_asignado"), 0), 2),
            "Líneas asignadas": _safe_int(r.get("lineas_asignadas"), 0),
            "Stock libre": round(_safe_float(r.get("stock_libre"), 0), 2),
        }
        for r in rows
    ]



def get_lineas_activas_por_sku(sku: str) -> list[dict[str, Any]]:
    ensure_reservas_phase3_schema()
    normalized_sku = normalize_text(sku)
    if not normalized_sku:
        return []
    rows = fetch_all(
        """
        SELECT
            p.id AS pedido_id,
            p.id_pedido,
            p.cliente,
            p.estado_operativo,
            p.estatus_venta,
            p.fecha_captura AS pedido_fecha_captura,
            p.fecha_produccion AS pedido_fecha_produccion,
            p.fecha_logistica AS pedido_fecha_logistica,
            p.prioridad_manual,
            pl.id AS pedido_linea_id,
            pl.line_no,
            pl.sku,
            pl.descripcion,
            pl.producto,
            pl.color,
            pl.cantidad_pedida,
            pl.cantidad_asignada,
            pl.cantidad_faltante,
            pl.cantidad_enviada,
            pl.fecha_captura,
            pl.fecha_produccion,
            pl.fecha_logistica,
            pl.estatus_venta
        FROM pedido_lineas pl
        INNER JOIN pedidos p ON p.id = pl.pedido_id
        WHERE p.estado_operativo = 'ACTIVO'
          AND pl.sku = ?
        ORDER BY
            CASE WHEN COALESCE(pl.cantidad_asignada, 0) > 0 THEN 0 ELSE 1 END,
            CASE WHEN COALESCE(pl.cantidad_faltante, 0) > 0 THEN 0 ELSE 1 END,
            COALESCE(pl.fecha_produccion, p.fecha_produccion) ASC,
            COALESCE(pl.fecha_logistica, p.fecha_logistica) ASC,
            p.id_pedido ASC,
            COALESCE(pl.line_no, 0) ASC,
            pl.id ASC
        """,
        (normalized_sku,),
    )
    return [_build_linea_operativa_row(r) for r in rows]



def get_contexto_reservas_por_sku(sku: str, cantidad_entrada: float | None = None) -> dict[str, Any]:
    """
    Resume el contexto operativo de reservas para un SKU, útil desde Entradas.

    No modifica nada. Solo ayuda a decidir si el SKU conviene ir directo a pedidos,
    quedarse en stock libre o requerirá trabajo posterior en Reservas.
    """
    ensure_reservas_phase3_schema()
    normalized_sku = normalize_text(sku)
    if not normalized_sku:
        return {
            "sku": "",
            "stock_libre_actual": 0.0,
            "reservado_activo_total": 0.0,
            "faltante_activo_total": 0.0,
            "pedidos_con_reserva": 0,
            "pedidos_con_faltante": 0,
            "lineas_con_reserva": 0,
            "lineas_con_faltante": 0,
            "cantidad_entrada": round(_safe_float(cantidad_entrada, 0.0), 6),
            "cobertura_entrada_sobre_faltante": 0.0,
            "faltante_despues_entrada": 0.0,
            "entrada_excedente": round(_safe_float(cantidad_entrada, 0.0), 6),
            "requiere_reservas": False,
            "lineas_reserva": [],
            "lineas_faltante": [],
            "lineas": [],
        }

    lineas = get_lineas_activas_por_sku(normalized_sku)
    lineas_reserva = [row for row in lineas if _safe_float(row.get("cantidad_asignada"), 0.0) > 0]
    lineas_faltante = [row for row in lineas if _safe_float(row.get("cantidad_faltante"), 0.0) > 0]

    inv = fetch_one(
        "SELECT COALESCE(stock_libre, 0) AS stock_libre FROM inventario_sku WHERE sku = ?",
        (normalized_sku,),
    ) or {}
    stock_libre_actual = round(_safe_float(inv.get("stock_libre"), 0.0), 6)
    reservado_activo_total = round(sum(_safe_float(row.get("cantidad_asignada"), 0.0) for row in lineas_reserva), 6)
    faltante_activo_total = round(sum(_safe_float(row.get("cantidad_faltante"), 0.0) for row in lineas_faltante), 6)
    entrada_qty = round(_safe_float(cantidad_entrada, 0.0), 6)
    cobertura = round(min(entrada_qty, faltante_activo_total), 6)
    faltante_post = round(max(0.0, faltante_activo_total - entrada_qty), 6)
    excedente = round(max(0.0, entrada_qty - faltante_activo_total), 6)

    pedidos_con_reserva = len({row.get("pedido_id") for row in lineas_reserva if _safe_int(row.get("pedido_id"), 0) > 0})
    pedidos_con_faltante = len({row.get("pedido_id") for row in lineas_faltante if _safe_int(row.get("pedido_id"), 0) > 0})

    requiere_reservas = bool(lineas_reserva and lineas_faltante)

    return {
        "sku": normalized_sku,
        "stock_libre_actual": stock_libre_actual,
        "reservado_activo_total": reservado_activo_total,
        "faltante_activo_total": faltante_activo_total,
        "pedidos_con_reserva": pedidos_con_reserva,
        "pedidos_con_faltante": pedidos_con_faltante,
        "lineas_con_reserva": len(lineas_reserva),
        "lineas_con_faltante": len(lineas_faltante),
        "cantidad_entrada": entrada_qty,
        "cobertura_entrada_sobre_faltante": cobertura,
        "faltante_despues_entrada": faltante_post,
        "entrada_excedente": excedente,
        "requiere_reservas": requiere_reservas,
        "lineas_reserva": lineas_reserva,
        "lineas_faltante": lineas_faltante,
        "lineas": lineas,
    }


def list_recent_reasignaciones(limit: int = 100) -> list[dict[str, Any]]:
    ensure_reservas_phase3_schema()
    rows = fetch_all(
        """
        SELECT rr.*, po.id_pedido AS id_pedido_origen, pd.id_pedido AS id_pedido_destino
        FROM reasignaciones_reserva rr
        INNER JOIN pedidos po ON po.id = rr.pedido_origen_id
        INNER JOIN pedidos pd ON pd.id = rr.pedido_destino_id
        ORDER BY rr.id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [
        {
            "ID": _safe_int(r.get("id"), 0),
            "Fecha": normalize_text(r.get("fecha")) or normalize_text(r.get("created_at")) or "",
            "SKU": normalize_text(r.get("sku")) or "",
            "Pedido origen": normalize_text(r.get("id_pedido_origen")) or "",
            "Pedido destino": normalize_text(r.get("id_pedido_destino")) or "",
            "Cantidad": round(_safe_float(r.get("cantidad"), 0), 2),
            "Motivo": normalize_text(r.get("motivo")) or "",
            "Usuario": normalize_text(r.get("usuario")) or "",
            "Observación": normalize_text(r.get("observacion")) or "",
        }
        for r in rows
    ]


# -----------------------------------------------------------------------------
# Preview helpers
# -----------------------------------------------------------------------------
def preview_reasignacion(linea_origen_id: int, linea_destino_id: int, cantidad: Any) -> dict[str, Any]:
    ensure_reservas_phase3_schema()
    origen = partidas_repository.get_pedido_linea_by_id(int(linea_origen_id))
    destino = partidas_repository.get_pedido_linea_by_id(int(linea_destino_id))
    if origen is None or destino is None:
        raise ValueError("No fue posible localizar las líneas origen y destino.")
    pedido_origen = pedidos_repository.get_pedido_by_id(int(origen["pedido_id"]))
    pedido_destino = pedidos_repository.get_pedido_by_id(int(destino["pedido_id"]))
    if pedido_origen is None or pedido_destino is None:
        raise ValueError("No fue posible localizar los pedidos origen y destino.")

    validate_active_order_state(pedido_origen.get("estado_operativo"))
    validate_active_order_state(pedido_destino.get("estado_operativo"))
    validate_distinct_lines(linea_origen_id, linea_destino_id)
    sku = validate_same_sku(origen.get("sku"), destino.get("sku"))
    qty = validate_reassign_quantity(
        cantidad_a_reasignar=cantidad,
        cantidad_asignada_origen=origen.get("cantidad_asignada"),
        cantidad_faltante_destino=destino.get("cantidad_faltante"),
    )

    return {
        "sku": sku,
        "cantidad": round(qty, 6),
        "origen": {
            "pedido_id": int(pedido_origen["id"]),
            "pedido_linea_id": int(origen["id"]),
            "id_pedido": normalize_text(pedido_origen.get("id_pedido")) or "",
            "cliente": normalize_text(pedido_origen.get("cliente")) or "",
            "cantidad_asignada_antes": round(_safe_float(origen.get("cantidad_asignada"), 0), 6),
            "cantidad_asignada_despues": round(_safe_float(origen.get("cantidad_asignada"), 0) - qty, 6),
        },
        "destino": {
            "pedido_id": int(pedido_destino["id"]),
            "pedido_linea_id": int(destino["id"]),
            "id_pedido": normalize_text(pedido_destino.get("id_pedido")) or "",
            "cliente": normalize_text(pedido_destino.get("cliente")) or "",
            "cantidad_asignada_antes": round(_safe_float(destino.get("cantidad_asignada"), 0), 6),
            "cantidad_asignada_despues": round(_safe_float(destino.get("cantidad_asignada"), 0) + qty, 6),
        },
    }


# -----------------------------------------------------------------------------
# Operaciones
# -----------------------------------------------------------------------------
def liberar_reserva_a_stock_libre(
    pedido_linea_id: int,
    cantidad: Any,
    usuario: str | None = None,
    observacion: str | None = None,
    motivo_auditoria: str | None = None,
) -> dict[str, Any]:
    ensure_reservas_phase3_schema()
    linea = partidas_repository.get_pedido_linea_by_id(int(pedido_linea_id))
    if linea is None:
        raise ValueError(f"No existe la línea {pedido_linea_id}.")
    pedido = pedidos_repository.get_pedido_by_id(int(linea["pedido_id"]))
    if pedido is None:
        raise ValueError("No existe el pedido de la línea a liberar.")
    validate_active_order_state(pedido.get("estado_operativo"))
    sku = normalize_text(linea.get("sku"))
    if not sku:
        raise ValueError("La línea no tiene SKU válido.")

    qty = validate_release_quantity(
        cantidad_a_liberar=cantidad,
        cantidad_asignada_linea=linea.get("cantidad_asignada"),
    )

    motivo = motivo_auditoria or "LIBERACION_RESERVA_A_STOCK_LIBRE"
    nota = observacion or "Liberación manual de reserva"

    with transaction() as conn:
        _take_assignment_chunks(
            conn,
            pedido_linea_id=int(pedido_linea_id),
            cantidad=qty,
            usuario=usuario,
            observacion=nota,
            motivo_tag=f"{RESERVA_TAG} Liberada a stock libre",
        )
        anterior, nueva_asignada, _ = _update_linea_assignment(conn, linea_id=int(pedido_linea_id), delta=-qty)
        cursor_mov = conn.execute(
            """
            INSERT INTO inventario_movimientos (
                sku, movimiento_tipo, afecta_total, afecta_stock_libre, afecta_stock_asignado,
                referencia_tipo, referencia_id, observacion, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sku,
                "DESASIGNACION_A_STOCK",
                0.0,
                qty,
                -qty,
                "PEDIDO",
                int(pedido.get("id")),
                nota,
                usuario,
            ),
        )
        movimiento_id = int(cursor_mov.lastrowid)

    asignaciones_repository.recalculate_inventario_sku_from_movimientos(
        sku=sku,
        descripcion=normalize_text(linea.get("descripcion")),
    )
    pedidos_repository.recalculate_pedido_metrics(int(pedido["id"]))
    _create_audit("pedido_lineas", pedido_linea_id, "cantidad_asignada", anterior, nueva_asignada, usuario, motivo)
    return {
        "pedido_linea_id": int(pedido_linea_id),
        "pedido_id": int(pedido["id"]),
        "sku": sku,
        "cantidad_liberada": round(qty, 6),
        "movimiento_id": movimiento_id,
        "nueva_asignada": round(nueva_asignada, 6),
    }



def reasignar_reserva_entre_pedidos(
    linea_origen_id: int,
    linea_destino_id: int,
    cantidad: Any,
    usuario: str | None = None,
    motivo: str | None = None,
    observacion: str | None = None,
) -> dict[str, Any]:
    ensure_reservas_phase3_schema()
    preview = preview_reasignacion(linea_origen_id, linea_destino_id, cantidad)

    linea_origen = partidas_repository.get_pedido_linea_by_id(int(linea_origen_id))
    linea_destino = partidas_repository.get_pedido_linea_by_id(int(linea_destino_id))
    pedido_origen = pedidos_repository.get_pedido_by_id(int(linea_origen["pedido_id"]))
    pedido_destino = pedidos_repository.get_pedido_by_id(int(linea_destino["pedido_id"]))
    qty = float(preview["cantidad"])
    normalized_motivo = normalize_motivo_reasignacion(motivo)
    nota = observacion or f"Reasignación de reserva {preview['origen']['id_pedido']} -> {preview['destino']['id_pedido']}"

    with transaction() as conn:
        chunks = _take_assignment_chunks(
            conn,
            pedido_linea_id=int(linea_origen_id),
            cantidad=qty,
            usuario=usuario,
            observacion=nota,
            motivo_tag=f"{RESERVA_TAG} Reasignada a línea {int(linea_destino_id)}",
        )

        for chunk in chunks:
            conn.execute(
                """
                INSERT INTO asignaciones (
                    pedido_id, pedido_linea_id, sku, cantidad, fuente_asignacion,
                    entrada_staging_id, decision_id, estado, observacion, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVA', ?, ?)
                """,
                (
                    int(pedido_destino["id"]),
                    int(linea_destino_id),
                    preview["sku"],
                    float(chunk["cantidad"]),
                    chunk.get("fuente_asignacion") or "STOCK_LIBRE",
                    chunk.get("entrada_staging_id"),
                    chunk.get("decision_id"),
                    f"{nota} | reasignada desde línea {int(linea_origen_id)}",
                    usuario,
                ),
            )

        origen_anterior, origen_nueva, _ = _update_linea_assignment(conn, linea_id=int(linea_origen_id), delta=-qty)
        destino_anterior, destino_nueva, _ = _update_linea_assignment(conn, linea_id=int(linea_destino_id), delta=qty)

        cursor = conn.execute(
            """
            INSERT INTO reasignaciones_reserva (
                fecha, sku, pedido_origen_id, pedido_destino_id,
                pedido_linea_origen_id, pedido_linea_destino_id,
                cantidad, motivo, observacion, usuario
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _now_iso(),
                preview["sku"],
                int(pedido_origen["id"]),
                int(pedido_destino["id"]),
                int(linea_origen_id),
                int(linea_destino_id),
                qty,
                normalized_motivo,
                nota,
                usuario,
            ),
        )
        reasignacion_id = int(cursor.lastrowid)

    pedidos_repository.recalculate_pedido_metrics(int(pedido_origen["id"]))
    pedidos_repository.recalculate_pedido_metrics(int(pedido_destino["id"]))
    _create_audit("pedido_lineas", linea_origen_id, "cantidad_asignada", origen_anterior, origen_nueva, usuario, normalized_motivo)
    _create_audit("pedido_lineas", linea_destino_id, "cantidad_asignada", destino_anterior, destino_nueva, usuario, normalized_motivo)

    return {
        "reasignacion_id": reasignacion_id,
        "sku": preview["sku"],
        "cantidad": round(qty, 6),
        "origen": preview["origen"],
        "destino": preview["destino"],
        "motivo": normalized_motivo,
    }



def asignar_stock_libre_a_linea(
    pedido_linea_id: int,
    cantidad: Any,
    usuario: str | None = None,
    observacion: str | None = None,
) -> int:
    ensure_reservas_phase3_schema()
    return int(
        asignar_desde_stock_libre_a_linea(
            pedido_linea_id=pedido_linea_id,
            cantidad=cantidad,
            usuario=usuario,
            observacion=observacion,
            motivo_auditoria="ASIGNACION_DESDE_RESERVAS_UI",
        )
    )



def get_reservas_ui_options() -> dict[str, list[str]]:
    asignables = [row["SKU"] for row in list_skus_para_asignar()]
    con_reserva = [row["SKU"] for row in list_skus_con_reserva()]
    return {
        "skus_asignables": asignables,
        "skus_con_reserva": con_reserva,
        "motivos_reasignacion": sorted(VALID_MOTIVO_REASIGNACION),
    }
