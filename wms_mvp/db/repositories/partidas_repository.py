from __future__ import annotations

from typing import Any

from wms_mvp.db.connection import column_exists, execute, fetch_all, fetch_one, transaction


LINEA_ALLOWED_UPDATE_FIELDS = {
    "line_no",
    "sku",
    "descripcion",
    "producto",
    "color",
    "cantidad_pedida",
    "cantidad_asignada",
    "cantidad_faltante",
    "cantidad_enviada",
    "fecha_captura",
    "fecha_produccion",
    "fecha_logistica",
    "estatus_venta",
    "observaciones",
}


# =========================================================
# HELPERS
# =========================================================
def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


# =========================================================
# CREATE
# =========================================================
def create_pedido_linea(data: dict[str, Any]) -> int:
    """
    Inserta una línea de pedido y devuelve su id interno.

    Ajustes incorporados:
    - soporte para fecha_produccion, fecha_logistica y estatus_venta
    - cantidades normalizadas y faltante calculado si no viene explícito
    """
    cantidad_pedida = _to_float(data.get("cantidad_pedida", 0))
    cantidad_asignada = _to_float(data.get("cantidad_asignada", 0))
    cantidad_enviada = _to_float(data.get("cantidad_enviada", 0))

    cantidad_faltante = data.get("cantidad_faltante")
    if cantidad_faltante is None:
        cantidad_faltante = max(0.0, cantidad_pedida - cantidad_asignada)
    else:
        cantidad_faltante = _to_float(cantidad_faltante, 0)

    query = """
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
    """
    params = (
        data["pedido_id"],
        data.get("line_no"),
        data.get("sku"),
        data.get("descripcion"),
        data.get("producto"),
        data.get("color"),
        cantidad_pedida,
        cantidad_asignada,
        cantidad_faltante,
        cantidad_enviada,
        data.get("fecha_captura"),
        data.get("fecha_produccion"),
        data.get("fecha_logistica"),
        data.get("estatus_venta"),
        data.get("observaciones"),
    )
    return execute(query, params)



def create_many_pedido_lineas(rows: list[dict[str, Any]]) -> None:
    """
    Inserta múltiples líneas de pedido dentro de una misma transacción.
    """
    if not rows:
        return

    query = """
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
    """

    params_seq = []
    for row in rows:
        cantidad_pedida = _to_float(row.get("cantidad_pedida", 0))
        cantidad_asignada = _to_float(row.get("cantidad_asignada", 0))
        cantidad_enviada = _to_float(row.get("cantidad_enviada", 0))
        cantidad_faltante = row.get("cantidad_faltante")
        if cantidad_faltante is None:
            cantidad_faltante = max(0.0, cantidad_pedida - cantidad_asignada)
        else:
            cantidad_faltante = _to_float(cantidad_faltante, 0)

        params_seq.append(
            (
                row["pedido_id"],
                row.get("line_no"),
                row.get("sku"),
                row.get("descripcion"),
                row.get("producto"),
                row.get("color"),
                cantidad_pedida,
                cantidad_asignada,
                cantidad_faltante,
                cantidad_enviada,
                row.get("fecha_captura"),
                row.get("fecha_produccion"),
                row.get("fecha_logistica"),
                row.get("estatus_venta"),
                row.get("observaciones"),
            )
        )

    with transaction() as conn:
        conn.executemany(query, params_seq)


# =========================================================
# READ
# =========================================================
def get_pedido_linea_by_id(linea_id: int) -> dict[str, Any] | None:
    """
    Devuelve una línea por su id interno.
    """
    query = """
        SELECT *
        FROM pedido_lineas
        WHERE id = ?
    """
    return fetch_one(query, (linea_id,))



def get_pedido_linea_by_pedido_and_line_no(pedido_id: int, line_no: int) -> dict[str, Any] | None:
    """
    Devuelve una línea de pedido por pedido_id y número de línea.
    """
    query = """
        SELECT *
        FROM pedido_lineas
        WHERE pedido_id = ?
          AND line_no = ?
    """
    return fetch_one(query, (pedido_id, line_no))



def list_lineas_by_pedido_id(pedido_id: int) -> list[dict[str, Any]]:
    """
    Devuelve todas las líneas de un pedido.

    Se ordenan por line_no para mantener estabilidad visual en la UI.
    """
    query = """
        SELECT *
        FROM pedido_lineas
        WHERE pedido_id = ?
        ORDER BY
            CASE WHEN line_no IS NULL THEN 1 ELSE 0 END,
            line_no ASC,
            id ASC
    """
    return fetch_all(query, (pedido_id,))



def list_lineas_activas_con_faltante_by_sku(sku: str) -> list[dict[str, Any]]:
    """
    Devuelve todas las líneas activas que solicitan el SKU y aún tienen faltante.

    Esta consulta es la base de la asignación asistida por SKU.
    Ajustes importantes:
    - incorpora fecha_produccion en la salida
    - incorpora fecha_logistica y estatus_venta tanto a nivel pedido como línea
    - ordena con lógica operativa: estatus comercial, fecha_produccion,
      fecha_logistica, fecha_captura e id_pedido
    """
    prioridad_pedido_select = "p.prioridad_manual AS prioridad_manual," if column_exists("pedidos", "prioridad_manual") else "NULL AS prioridad_manual,"
    prioridad_pedido_order = "CASE WHEN p.prioridad_manual IS NULL THEN 1 ELSE 0 END, p.prioridad_manual ASC," if column_exists("pedidos", "prioridad_manual") else ""

    query = f"""
        SELECT
            p.id AS pedido_id,
            p.id_pedido,
            p.cliente,
            {prioridad_pedido_select}
            p.estatus_venta,
            p.fecha_captura AS pedido_fecha_captura,
            p.fecha_produccion AS pedido_fecha_produccion,
            p.fecha_logistica AS pedido_fecha_logistica,
            ROUND(COALESCE(p.porcentaje_asignacion, 0), 2) AS porcentaje_asignacion,
            pl.id AS pedido_linea_id,
            pl.line_no,
            pl.sku,
            pl.descripcion,
            pl.producto,
            pl.color,
            COALESCE(pl.cantidad_pedida, 0) AS cantidad_pedida,
            COALESCE(pl.cantidad_asignada, 0) AS cantidad_asignada,
            COALESCE(pl.cantidad_faltante, 0) AS cantidad_faltante,
            COALESCE(pl.cantidad_enviada, 0) AS cantidad_enviada,
            pl.fecha_captura AS linea_fecha_captura,
            pl.fecha_produccion AS linea_fecha_produccion,
            pl.fecha_logistica AS linea_fecha_logistica,
            pl.estatus_venta AS linea_estatus_venta,
            COALESCE(pl.fecha_produccion, p.fecha_produccion) AS fecha_produccion,
            COALESCE(pl.fecha_logistica, p.fecha_logistica) AS fecha_logistica,
            COALESCE(pl.estatus_venta, p.estatus_venta) AS estatus_venta_operativo,
            CASE
                WHEN UPPER(COALESCE(pl.estatus_venta, p.estatus_venta, '')) = 'ENTREGAR' THEN 0
                WHEN UPPER(COALESCE(pl.estatus_venta, p.estatus_venta, '')) = 'EN COBRO' THEN 1
                ELSE 2
            END AS prioridad_estatus
        FROM pedido_lineas pl
        INNER JOIN pedidos p
            ON p.id = pl.pedido_id
        WHERE p.estado_operativo = 'ACTIVO'
          AND pl.sku = ?
          AND COALESCE(pl.cantidad_faltante, 0) > 0
        ORDER BY
            {prioridad_pedido_order}
            prioridad_estatus ASC,
            CASE
                WHEN COALESCE(pl.fecha_produccion, p.fecha_produccion) IS NULL
                  OR TRIM(COALESCE(pl.fecha_produccion, p.fecha_produccion)) = ''
                THEN 1 ELSE 0
            END,
            COALESCE(pl.fecha_produccion, p.fecha_produccion) ASC,
            CASE
                WHEN COALESCE(pl.fecha_logistica, p.fecha_logistica) IS NULL
                  OR TRIM(COALESCE(pl.fecha_logistica, p.fecha_logistica)) = ''
                THEN 1 ELSE 0
            END,
            COALESCE(pl.fecha_logistica, p.fecha_logistica) ASC,
            CASE
                WHEN COALESCE(pl.fecha_captura, p.fecha_captura) IS NULL
                  OR TRIM(COALESCE(pl.fecha_captura, p.fecha_captura)) = ''
                THEN 1 ELSE 0
            END,
            COALESCE(pl.fecha_captura, p.fecha_captura) ASC,
            p.id_pedido ASC,
            pl.line_no ASC,
            pl.id ASC
    """
    return fetch_all(query, (sku,))



def list_distinct_productos_activos() -> list[str]:
    """
    Devuelve la lista única de productos presentes en líneas de pedidos activos.
    """
    rows = fetch_all(
        """
        SELECT DISTINCT pl.producto
        FROM pedido_lineas pl
        INNER JOIN pedidos p
            ON p.id = pl.pedido_id
        WHERE p.estado_operativo = 'ACTIVO'
          AND pl.producto IS NOT NULL
          AND TRIM(pl.producto) <> ''
        ORDER BY pl.producto ASC
        """
    )
    return [row["producto"] for row in rows]


# =========================================================
# UPDATE / RECALC
# =========================================================
def update_pedido_linea_fields(linea_id: int, fields: dict[str, Any]) -> None:
    """
    Actualiza campos permitidos de una línea de pedido.
    No hace validaciones de negocio; eso corresponde a reglas/servicios.
    """
    if not fields:
        return

    safe_fields = {key: value for key, value in fields.items() if key in LINEA_ALLOWED_UPDATE_FIELDS}

    if not safe_fields:
        return

    # Normalización mínima de numéricos para no enviar cadenas vacías a SQLite.
    for numeric_field in {
        "cantidad_pedida",
        "cantidad_asignada",
        "cantidad_faltante",
        "cantidad_enviada",
    }:
        if numeric_field in safe_fields:
            safe_fields[numeric_field] = _to_float(safe_fields[numeric_field], 0)

    set_clause = ", ".join(f"{field} = ?" for field in safe_fields.keys())
    params = list(safe_fields.values())
    params.append(linea_id)

    query = f"""
        UPDATE pedido_lineas
        SET {set_clause},
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """

    execute(query, params)



def recalculate_linea_faltante(linea_id: int) -> None:
    """
    Recalcula cantidad_faltante = cantidad_pedida - cantidad_asignada.
    Si el resultado es negativo, se guarda en 0.
    """
    linea = get_pedido_linea_by_id(linea_id)
    if linea is None:
        return

    cantidad_pedida = _to_float(linea.get("cantidad_pedida") or 0)
    cantidad_asignada = _to_float(linea.get("cantidad_asignada") or 0)

    cantidad_faltante = max(0.0, cantidad_pedida - cantidad_asignada)

    execute(
        """
        UPDATE pedido_lineas
        SET cantidad_faltante = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (cantidad_faltante, linea_id),
    )



def recalculate_all_lineas_faltante_by_pedido(pedido_id: int) -> None:
    """
    Recalcula cantidad_faltante para todas las líneas de un pedido.
    """
    lineas = list_lineas_by_pedido_id(pedido_id)
    if not lineas:
        return

    with transaction() as conn:
        for linea in lineas:
            cantidad_pedida = _to_float(linea.get("cantidad_pedida") or 0)
            cantidad_asignada = _to_float(linea.get("cantidad_asignada") or 0)
            cantidad_faltante = max(0.0, cantidad_pedida - cantidad_asignada)

            conn.execute(
                """
                UPDATE pedido_lineas
                SET cantidad_faltante = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (cantidad_faltante, linea["id"]),
            )



def add_asignacion_to_linea(linea_id: int, cantidad: float) -> None:
    """
    Suma una cantidad a cantidad_asignada y recalcula cantidad_faltante.
    """
    linea = get_pedido_linea_by_id(linea_id)
    if linea is None:
        raise ValueError(f"No existe la línea con id {linea_id}")

    cantidad_actual = _to_float(linea.get("cantidad_asignada") or 0)
    nueva_cantidad = cantidad_actual + _to_float(cantidad)

    with transaction() as conn:
        conn.execute(
            """
            UPDATE pedido_lineas
            SET cantidad_asignada = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (nueva_cantidad, linea_id),
        )

        cantidad_pedida = _to_float(linea.get("cantidad_pedida") or 0)
        cantidad_faltante = max(0.0, cantidad_pedida - nueva_cantidad)

        conn.execute(
            """
            UPDATE pedido_lineas
            SET cantidad_faltante = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (cantidad_faltante, linea_id),
        )



def set_linea_enviada(linea_id: int, cantidad_enviada: float) -> None:
    """
    Actualiza la cantidad_enviada de una línea.
    """
    execute(
        """
        UPDATE pedido_lineas
        SET cantidad_enviada = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (_to_float(cantidad_enviada), linea_id),
    )



def sync_lineas_context_from_pedido(
    pedido_id: int,
    fecha_captura: str | None = None,
    fecha_produccion: str | None = None,
    fecha_logistica: str | None = None,
    estatus_venta: str | None = None,
) -> None:
    """
    Propaga contexto operativo del encabezado a las líneas del pedido.

    Este helper ayuda a mantener coherencia visual y de filtros cuando el MVP
    trabaja tanto a nivel pedido como a nivel partida.
    """
    fields_to_set: list[str] = []
    params: list[Any] = []

    if fecha_captura is not None:
        fields_to_set.append("fecha_captura = ?")
        params.append(fecha_captura)
    if fecha_produccion is not None:
        fields_to_set.append("fecha_produccion = ?")
        params.append(fecha_produccion)
    if fecha_logistica is not None:
        fields_to_set.append("fecha_logistica = ?")
        params.append(fecha_logistica)
    if estatus_venta is not None:
        fields_to_set.append("estatus_venta = ?")
        params.append(estatus_venta)

    if not fields_to_set:
        return

    fields_to_set.append("updated_at = CURRENT_TIMESTAMP")
    params.append(pedido_id)

    query = f"""
        UPDATE pedido_lineas
        SET {', '.join(fields_to_set)}
        WHERE pedido_id = ?
    """
    execute(query, params)


# =========================================================
# AGREGADOS / DELETE
# =========================================================
def get_linea_metrics_by_pedido(pedido_id: int) -> dict[str, Any]:
    """
    Devuelve métricas agregadas de las líneas de un pedido.
    """
    query = """
        SELECT
            COALESCE(SUM(cantidad_pedida), 0) AS total_pedido,
            COALESCE(SUM(cantidad_asignada), 0) AS total_asignado,
            COALESCE(SUM(cantidad_faltante), 0) AS total_faltante,
            COALESCE(SUM(cantidad_enviada), 0) AS total_enviado,
            COUNT(*) AS total_lineas
        FROM pedido_lineas
        WHERE pedido_id = ?
    """
    row = fetch_one(query, (pedido_id,))
    return row or {
        "total_pedido": 0,
        "total_asignado": 0,
        "total_faltante": 0,
        "total_enviado": 0,
        "total_lineas": 0,
    }



def delete_linea(linea_id: int) -> None:
    """
    Elimina una línea de pedido por id.
    """
    execute(
        """
        DELETE FROM pedido_lineas
        WHERE id = ?
        """,
        (linea_id,),
    )



def delete_lineas_by_pedido(pedido_id: int) -> None:
    """
    Elimina todas las líneas asociadas a un pedido.
    """
    execute(
        """
        DELETE FROM pedido_lineas
        WHERE pedido_id = ?
        """,
        (pedido_id,),
    )
