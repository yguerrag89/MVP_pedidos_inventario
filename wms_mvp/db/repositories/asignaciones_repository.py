from __future__ import annotations

from typing import Any

from wms_mvp.db.connection import execute, fetch_all, fetch_one, transaction


# =========================================================
# HELPERS
# =========================================================
def _coalesce_user(data: dict[str, Any]) -> str | None:
    return data.get("created_by") or data.get("usuario")


def _coalesce_fuente(data: dict[str, Any], default: str = "AJUSTE_MANUAL") -> str:
    return str(
        data.get("fuente_asignacion")
        or data.get("tipo_asignacion")
        or default
    )


def _normalize_inventario_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None

    normalized = dict(row)
    total_recibido = float(normalized.get("total_recibido") or 0)
    stock_libre = float(normalized.get("stock_libre") or 0)
    stock_asignado = float(normalized.get("stock_asignado") or 0)
    total_enviado = float(normalized.get("total_enviado") or 0)
    sku = normalized.get("sku")

    # Aliases de compatibilidad útiles para otras iteraciones del MVP.
    normalized.setdefault("id", sku)
    normalized.setdefault("entradas_recibidas", total_recibido)
    normalized.setdefault("stock_total", total_recibido)
    normalized.setdefault("usuario", normalized.get("created_by"))
    normalized["total_recibido"] = total_recibido
    normalized["stock_libre"] = stock_libre
    normalized["stock_asignado"] = stock_asignado
    normalized["total_enviado"] = total_enviado
    normalized["disponible_para_asignar"] = stock_libre
    return normalized


def _normalize_movimiento_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    normalized = dict(row)
    normalized.setdefault("usuario", normalized.get("created_by"))
    normalized.setdefault("tipo_movimiento", normalized.get("movimiento_tipo"))
    return normalized


# =========================================================
# ASIGNACIONES
# =========================================================
def create_asignacion(data: dict[str, Any]) -> int:
    """
    Inserta una asignación real a una línea de pedido.
    Acepta aliases suaves para compatibilidad con otras iteraciones.
    """
    query = """
        INSERT INTO asignaciones (
            pedido_id,
            pedido_linea_id,
            sku,
            cantidad,
            fuente_asignacion,
            entrada_staging_id,
            decision_id,
            estado,
            observacion,
            created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        data["pedido_id"],
        data["pedido_linea_id"],
        data["sku"],
        float(data.get("cantidad", 0) or 0),
        _coalesce_fuente(data),
        data.get("entrada_staging_id") or data.get("corte_entrada_id"),
        data.get("decision_id"),
        data.get("estado", "ACTIVA"),
        data.get("observacion"),
        _coalesce_user(data),
    )
    return execute(query, params)


def create_many_asignaciones(rows: list[dict[str, Any]]) -> None:
    """
    Inserta múltiples asignaciones dentro de una sola transacción.
    """
    if not rows:
        return

    query = """
        INSERT INTO asignaciones (
            pedido_id,
            pedido_linea_id,
            sku,
            cantidad,
            fuente_asignacion,
            entrada_staging_id,
            decision_id,
            estado,
            observacion,
            created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    params_seq = [
        (
            row["pedido_id"],
            row["pedido_linea_id"],
            row["sku"],
            float(row.get("cantidad", 0) or 0),
            _coalesce_fuente(row),
            row.get("entrada_staging_id") or row.get("corte_entrada_id"),
            row.get("decision_id"),
            row.get("estado", "ACTIVA"),
            row.get("observacion"),
            _coalesce_user(row),
        )
        for row in rows
    ]

    with transaction() as conn:
        conn.executemany(query, params_seq)


def get_asignacion_by_id(asignacion_id: int) -> dict[str, Any] | None:
    query = """
        SELECT *
        FROM asignaciones
        WHERE id = ?
    """
    return fetch_one(query, (asignacion_id,))


def list_asignaciones_by_pedido(pedido_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM asignaciones
        WHERE pedido_id = ?
        ORDER BY id ASC
    """
    return fetch_all(query, (pedido_id,))


def list_asignaciones_by_linea(pedido_linea_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM asignaciones
        WHERE pedido_linea_id = ?
        ORDER BY id ASC
    """
    return fetch_all(query, (pedido_linea_id,))


def list_asignaciones_activas_by_sku(sku: str) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM asignaciones
        WHERE sku = ?
          AND estado = 'ACTIVA'
        ORDER BY id ASC
    """
    return fetch_all(query, (sku,))


def list_asignaciones_by_entrada_staging(entrada_staging_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM asignaciones
        WHERE entrada_staging_id = ?
        ORDER BY id ASC
    """
    return fetch_all(query, (entrada_staging_id,))


def list_asignaciones_by_decision(decision_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM asignaciones
        WHERE decision_id = ?
        ORDER BY id ASC
    """
    return fetch_all(query, (decision_id,))


def sum_asignaciones_activas_by_linea(pedido_linea_id: int) -> float:
    row = fetch_one(
        """
        SELECT COALESCE(SUM(cantidad), 0) AS total_asignado
        FROM asignaciones
        WHERE pedido_linea_id = ?
          AND estado = 'ACTIVA'
        """,
        (pedido_linea_id,),
    )
    return float(row["total_asignado"]) if row else 0.0


def sum_asignaciones_activas_by_sku(sku: str) -> float:
    row = fetch_one(
        """
        SELECT COALESCE(SUM(cantidad), 0) AS total_asignado
        FROM asignaciones
        WHERE sku = ?
          AND estado = 'ACTIVA'
        """,
        (sku,),
    )
    return float(row["total_asignado"]) if row else 0.0


def update_asignacion_fields(asignacion_id: int, fields: dict[str, Any]) -> None:
    """
    Actualiza campos permitidos de una asignación.
    Mantiene updated_at consistente.
    """
    if not fields:
        return

    alias_map = {
        "tipo_asignacion": "fuente_asignacion",
        "usuario": "created_by",
    }
    translated: dict[str, Any] = {}
    for key, value in fields.items():
        translated[alias_map.get(key, key)] = value

    allowed_fields = {
        "cantidad",
        "fuente_asignacion",
        "entrada_staging_id",
        "decision_id",
        "estado",
        "observacion",
        "created_by",
    }

    safe_fields = {k: v for k, v in translated.items() if k in allowed_fields}
    if not safe_fields:
        return

    safe_fields["updated_at"] = "__CURRENT_TIMESTAMP__"

    set_parts: list[str] = []
    params: list[Any] = []
    for field, value in safe_fields.items():
        if value == "__CURRENT_TIMESTAMP__":
            set_parts.append(f"{field} = CURRENT_TIMESTAMP")
        else:
            set_parts.append(f"{field} = ?")
            params.append(value)
    params.append(asignacion_id)

    query = f"""
        UPDATE asignaciones
        SET {', '.join(set_parts)}
        WHERE id = ?
    """
    execute(query, params)


def set_asignacion_estado(asignacion_id: int, estado: str) -> None:
    query = """
        UPDATE asignaciones
        SET estado = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """
    execute(query, (estado, asignacion_id))


def cancel_asignacion(asignacion_id: int, observacion: str | None = None) -> None:
    query = """
        UPDATE asignaciones
        SET estado = 'CANCELADA',
            observacion = COALESCE(?, observacion),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """
    execute(query, (observacion, asignacion_id))


def consume_asignacion(asignacion_id: int, observacion: str | None = None) -> None:
    query = """
        UPDATE asignaciones
        SET estado = 'CONSUMIDA',
            observacion = COALESCE(?, observacion),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """
    execute(query, (observacion, asignacion_id))


def delete_asignacion(asignacion_id: int) -> None:
    query = """
        DELETE FROM asignaciones
        WHERE id = ?
    """
    execute(query, (asignacion_id,))


# =========================================================
# INVENTARIO SKU (RESUMEN)
# =========================================================
def get_inventario_sku(sku: str) -> dict[str, Any] | None:
    query = """
        SELECT *
        FROM inventario_sku
        WHERE sku = ?
    """
    return _normalize_inventario_row(fetch_one(query, (sku,)))


def upsert_inventario_sku(
    sku: str,
    descripcion: str | None = None,
    total_recibido: float | None = None,
    stock_libre: float | None = None,
    stock_asignado: float | None = None,
    total_enviado: float | None = None,
) -> None:
    """
    Inserta o actualiza el resumen de inventario de un SKU.
    Solo reemplaza los campos enviados; los demás conservan su valor.
    """
    existing = get_inventario_sku(sku)

    if existing is None:
        query = """
            INSERT INTO inventario_sku (
                sku,
                descripcion,
                total_recibido,
                stock_libre,
                stock_asignado,
                total_enviado,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        params = (
            sku,
            descripcion,
            float(total_recibido or 0),
            float(stock_libre or 0),
            float(stock_asignado or 0),
            float(total_enviado or 0),
        )
        execute(query, params)
        return

    new_descripcion = descripcion if descripcion is not None else existing.get("descripcion")
    new_total_recibido = float(total_recibido) if total_recibido is not None else float(existing.get("total_recibido", 0) or 0)
    new_stock_libre = float(stock_libre) if stock_libre is not None else float(existing.get("stock_libre", 0) or 0)
    new_stock_asignado = float(stock_asignado) if stock_asignado is not None else float(existing.get("stock_asignado", 0) or 0)
    new_total_enviado = float(total_enviado) if total_enviado is not None else float(existing.get("total_enviado", 0) or 0)

    query = """
        UPDATE inventario_sku
        SET descripcion = ?,
            total_recibido = ?,
            stock_libre = ?,
            stock_asignado = ?,
            total_enviado = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE sku = ?
    """
    params = (
        new_descripcion,
        new_total_recibido,
        new_stock_libre,
        new_stock_asignado,
        new_total_enviado,
        sku,
    )
    execute(query, params)


def list_inventario_sku(limit: int = 1000) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM inventario_sku
        ORDER BY sku ASC
        LIMIT ?
    """
    return [_normalize_inventario_row(row) for row in fetch_all(query, (limit,))]


def list_inventario_disponible(limit: int = 1000) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM inventario_sku
        WHERE COALESCE(stock_libre, 0) > 0
        ORDER BY stock_libre DESC, sku ASC
        LIMIT ?
    """
    return [_normalize_inventario_row(row) for row in fetch_all(query, (limit,))]


def get_stock_libre_by_sku(sku: str) -> float:
    row = fetch_one(
        """
        SELECT COALESCE(stock_libre, 0) AS stock_libre
        FROM inventario_sku
        WHERE sku = ?
        """,
        (sku,),
    )
    return float(row["stock_libre"]) if row else 0.0


def get_stock_asignado_by_sku(sku: str) -> float:
    row = fetch_one(
        """
        SELECT COALESCE(stock_asignado, 0) AS stock_asignado
        FROM inventario_sku
        WHERE sku = ?
        """,
        (sku,),
    )
    return float(row["stock_asignado"]) if row else 0.0


def get_total_recibido_by_sku(sku: str) -> float:
    row = fetch_one(
        """
        SELECT COALESCE(total_recibido, 0) AS total_recibido
        FROM inventario_sku
        WHERE sku = ?
        """,
        (sku,),
    )
    return float(row["total_recibido"]) if row else 0.0


def get_total_enviado_by_sku(sku: str) -> float:
    row = fetch_one(
        """
        SELECT COALESCE(total_enviado, 0) AS total_enviado
        FROM inventario_sku
        WHERE sku = ?
        """,
        (sku,),
    )
    return float(row["total_enviado"]) if row else 0.0


# =========================================================
# MOVIMIENTOS DE INVENTARIO
# =========================================================
def create_inventario_movimiento(data: dict[str, Any]) -> int:
    """
    Inserta un movimiento de inventario.
    Acepta aliases suaves para compatibilidad.
    """
    query = """
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
    """
    params = (
        data["sku"],
        data.get("movimiento_tipo") or data.get("tipo_movimiento"),
        float(data.get("afecta_total", 0) or 0),
        float(data.get("afecta_stock_libre", 0) or 0),
        float(data.get("afecta_stock_asignado", 0) or 0),
        data.get("referencia_tipo"),
        data.get("referencia_id"),
        data.get("observacion"),
        _coalesce_user(data),
    )
    return execute(query, params)


def create_many_inventario_movimientos(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    query = """
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
    """

    params_seq = [
        (
            row["sku"],
            row.get("movimiento_tipo") or row.get("tipo_movimiento"),
            float(row.get("afecta_total", 0) or 0),
            float(row.get("afecta_stock_libre", 0) or 0),
            float(row.get("afecta_stock_asignado", 0) or 0),
            row.get("referencia_tipo"),
            row.get("referencia_id"),
            row.get("observacion"),
            _coalesce_user(row),
        )
        for row in rows
    ]

    with transaction() as conn:
        conn.executemany(query, params_seq)


def get_inventario_movimiento_by_id(movimiento_id: int) -> dict[str, Any] | None:
    query = """
        SELECT *
        FROM inventario_movimientos
        WHERE id = ?
    """
    return _normalize_movimiento_row(fetch_one(query, (movimiento_id,)))


def list_inventario_movimientos_by_sku(sku: str, limit: int = 1000) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM inventario_movimientos
        WHERE sku = ?
        ORDER BY id DESC
        LIMIT ?
    """
    return [_normalize_movimiento_row(row) for row in fetch_all(query, (sku, limit))]


def list_inventario_movimientos_by_referencia(
    referencia_tipo: str,
    referencia_id: int,
) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM inventario_movimientos
        WHERE referencia_tipo = ?
          AND referencia_id = ?
        ORDER BY id ASC
    """
    return [_normalize_movimiento_row(row) for row in fetch_all(query, (referencia_tipo, referencia_id))]


def recalculate_inventario_sku_from_movimientos(sku: str, descripcion: str | None = None) -> None:
    """
    Recalcula inventario_sku a partir de la suma de movimientos de inventario.
    También calcula total enviado desde pedido_lineas.
    """
    row = fetch_one(
        """
        SELECT
            COALESCE(SUM(afecta_total), 0) AS total_recibido_neto,
            COALESCE(SUM(afecta_stock_libre), 0) AS stock_libre_neto,
            COALESCE(SUM(afecta_stock_asignado), 0) AS stock_asignado_neto
        FROM inventario_movimientos
        WHERE sku = ?
        """,
        (sku,),
    )

    total_recibido_neto = float(row["total_recibido_neto"]) if row else 0.0
    stock_libre_neto = float(row["stock_libre_neto"]) if row else 0.0
    stock_asignado_neto = float(row["stock_asignado_neto"]) if row else 0.0

    enviados_row = fetch_one(
        """
        SELECT COALESCE(SUM(cantidad_enviada), 0) AS total_enviado
        FROM pedido_lineas
        WHERE sku = ?
        """,
        (sku,),
    )
    total_enviado = float(enviados_row["total_enviado"]) if enviados_row else 0.0

    upsert_inventario_sku(
        sku=sku,
        descripcion=descripcion,
        total_recibido=total_recibido_neto,
        stock_libre=stock_libre_neto,
        stock_asignado=stock_asignado_neto,
        total_enviado=total_enviado,
    )


def apply_movimiento_and_refresh(
    movimiento_data: dict[str, Any],
    descripcion: str | None = None,
) -> int:
    """
    Inserta un movimiento y recalcula el resumen de inventario del SKU.
    """
    movimiento_id = create_inventario_movimiento(movimiento_data)
    recalculate_inventario_sku_from_movimientos(
        sku=movimiento_data["sku"],
        descripcion=descripcion,
    )
    return movimiento_id
