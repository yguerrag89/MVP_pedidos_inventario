from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from wms_mvp.db.connection import column_exists, execute, fetch_all, fetch_one, transaction


PEDIDO_ALLOWED_UPDATE_FIELDS = {
    "cliente",
    "fecha_captura",
    "fecha_produccion",
    "fecha_logistica",
    "estatus_venta",
    "prioridad_manual",
    "porcentaje_asignacion",
    "estado_operativo",
    "observaciones",
    "tipo_registro",
    "origen_registro",
    "serie_documento",
    "documento",
    "pedido_origen_id",
}


# =========================================================
# HELPERS
# =========================================================
def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _is_iterable_filter_value(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray, dict))


def _normalize_filter_values(value: Any) -> list[Any]:
    """
    Normaliza un valor de filtro para soportar tanto escalares como múltiples.

    Reglas:
    - None, '', listas vacías o elementos vacíos se ignoran.
    - strings se limpian con strip.
    - otros tipos se conservan tal cual si aportan valor.
    """
    if value is None:
        return []

    raw_values = list(value) if _is_iterable_filter_value(value) else [value]
    normalized: list[Any] = []

    for item in raw_values:
        if item is None:
            continue
        if isinstance(item, str):
            cleaned = item.strip()
            if cleaned:
                normalized.append(cleaned)
            continue
        normalized.append(item)

    return normalized


def _append_filter_clause(
    query: str,
    params: list[Any],
    column_sql: str,
    value: Any,
) -> tuple[str, list[Any]]:
    """
    Agrega una cláusula de filtro soportando escalar o lista.

    - un solo valor -> columna = ?
    - varios valores -> columna IN (?, ?, ...)
    - vacío -> no agrega nada
    """
    values = _normalize_filter_values(value)
    if not values:
        return query, params

    if len(values) == 1:
        query += f" AND {column_sql} = ?"
        params.append(values[0])
        return query, params

    placeholders = ", ".join("?" for _ in values)
    query += f" AND {column_sql} IN ({placeholders})"
    params.extend(values)
    return query, params


def _append_pedidos_activos_filters(
    base_query: str,
    params: list[Any],
    filters: dict[str, Any] | None,
) -> tuple[str, list[Any]]:
    """
    Aplica filtros operativos a consultas de pedidos activos.

    Reglas importantes del MVP:
    - la tabla resumen NO se filtra
    - el filtro principal de fecha es fecha_produccion
    - el filtro por producto actúa sobre el detalle y el gráfico
    - se mantienen filtros de apoyo por fecha_captura y fecha_logistica

    Compatibilidad temporal:
    - acepta porcentaje_min / porcentaje_max
    - y también pct_min / pct_max mientras se corrige la UI
    """
    filters = filters or {}
    query = base_query

    query, params = _append_filter_clause(query, params, "p.id_pedido", filters.get("id_pedido"))
    query, params = _append_filter_clause(query, params, "p.cliente", filters.get("cliente"))
    query, params = _append_filter_clause(query, params, "p.estatus_venta", filters.get("estatus_venta"))

    # Filtro operativo principal
    query, params = _append_filter_clause(query, params, "p.fecha_produccion", filters.get("fecha_produccion"))

    fecha_produccion_desde = _clean_text(filters.get("fecha_produccion_desde"))
    if fecha_produccion_desde:
        query += " AND p.fecha_produccion >= ?"
        params.append(fecha_produccion_desde)

    fecha_produccion_hasta = _clean_text(filters.get("fecha_produccion_hasta"))
    if fecha_produccion_hasta:
        query += " AND p.fecha_produccion <= ?"
        params.append(fecha_produccion_hasta)

    # Filtros secundarios de apoyo
    fecha_captura_desde = _clean_text(filters.get("fecha_captura_desde"))
    if fecha_captura_desde:
        query += " AND p.fecha_captura >= ?"
        params.append(fecha_captura_desde)

    fecha_captura_hasta = _clean_text(filters.get("fecha_captura_hasta"))
    if fecha_captura_hasta:
        query += " AND p.fecha_captura <= ?"
        params.append(fecha_captura_hasta)

    fecha_logistica_desde = _clean_text(filters.get("fecha_logistica_desde"))
    if fecha_logistica_desde:
        query += " AND p.fecha_logistica >= ?"
        params.append(fecha_logistica_desde)

    fecha_logistica_hasta = _clean_text(filters.get("fecha_logistica_hasta"))
    if fecha_logistica_hasta:
        query += " AND p.fecha_logistica <= ?"
        params.append(fecha_logistica_hasta)

    query, params = _append_filter_clause(query, params, "pl.producto", filters.get("producto"))
    query, params = _append_filter_clause(query, params, "pl.sku", filters.get("sku"))

    porcentaje_min = filters.get("porcentaje_min", filters.get("pct_min"))
    if porcentaje_min is not None and porcentaje_min != "":
        query += " AND p.porcentaje_asignacion >= ?"
        params.append(_to_float(porcentaje_min))

    porcentaje_max = filters.get("porcentaje_max", filters.get("pct_max"))
    if porcentaje_max is not None and porcentaje_max != "":
        query += " AND p.porcentaje_asignacion <= ?"
        params.append(_to_float(porcentaje_max))

    return query, params


# =========================================================
# CREATE / READ BÁSICO
# =========================================================
def create_pedido(data: dict[str, Any]) -> int:
    """
    Inserta un pedido y devuelve su id interno.

    Este ajuste incorpora fecha_produccion y pedido_origen_id para alinear
    el repositorio con las reglas consolidadas del MVP.
    """
    query = """
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
    """
    params = (
        data["id_pedido"],
        data.get("serie_documento"),
        data.get("documento"),
        data.get("tipo_registro", "PEDIDO_NORMAL"),
        data.get("origen_registro", "OTRO"),
        data.get("import_job_id"),
        data.get("pedido_origen_id"),
        data.get("cliente"),
        data.get("fecha_captura"),
        data.get("fecha_produccion"),
        data.get("fecha_logistica"),
        data.get("estatus_venta"),
        _to_float(data.get("porcentaje_asignacion", 0)),
        data.get("estado_operativo", "ACTIVO"),
        data.get("observaciones"),
    )
    return execute(query, params)


def get_pedido_by_id(pedido_id: int) -> dict[str, Any] | None:
    """
    Devuelve un pedido por su id interno.
    """
    query = """
        SELECT *
        FROM pedidos
        WHERE id = ?
    """
    return fetch_one(query, (pedido_id,))


def get_pedido_by_id_pedido(id_pedido: str) -> dict[str, Any] | None:
    """
    Devuelve un pedido por su id_pedido de negocio.
    """
    query = """
        SELECT *
        FROM pedidos
        WHERE id_pedido = ?
    """
    return fetch_one(query, (_clean_text(id_pedido),))


# =========================================================
# LISTADOS DE PEDIDOS ACTIVOS
# =========================================================
def list_pedidos_activos_summary() -> list[dict[str, Any]]:
    """
    Devuelve la tabla resumen de pedidos activos.

    Esta consulta NO aplica filtros porque, por regla del MVP,
    la tabla resumen debe permanecer completa y estable.
    """
    prioridad_select = "p.prioridad_manual," if column_exists("pedidos", "prioridad_manual") else "NULL AS prioridad_manual,"
    prioridad_order = "CASE WHEN p.prioridad_manual IS NULL OR p.prioridad_manual = '' THEN 1 ELSE 0 END, p.prioridad_manual ASC," if column_exists("pedidos", "prioridad_manual") else ""
    query = f"""
        SELECT
            p.id,
            p.id_pedido,
            p.cliente,
            {prioridad_select}
            ROUND(COALESCE(p.porcentaje_asignacion, 0), 2) AS porcentaje_asignacion,
            p.fecha_captura,
            p.fecha_produccion,
            p.fecha_logistica,
            p.estatus_venta,
            p.estado_operativo,
            p.tipo_registro,
            p.origen_registro,
            p.pedido_origen_id,
            p.created_at,
            p.updated_at
        FROM pedidos p
        WHERE p.estado_operativo = 'ACTIVO'
        ORDER BY
            {prioridad_order}
            CASE
                WHEN p.fecha_produccion IS NULL OR TRIM(p.fecha_produccion) = '' THEN 1
                ELSE 0
            END,
            p.fecha_produccion ASC,
            CASE
                WHEN p.fecha_logistica IS NULL OR TRIM(p.fecha_logistica) = '' THEN 1
                ELSE 0
            END,
            p.fecha_logistica ASC,
            p.fecha_captura ASC,
            p.id_pedido ASC
    """
    return fetch_all(query)


def list_pedidos_detalle_filtered(filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """
    Devuelve el detalle de pedidos activos con filtros.

    Se prioriza el orden operativo de columnas pedido por las reglas:
    ID_pedido, cliente, producto, color, cantidades, %, fechas,
    estatus_venta, SKU y descripción.
    """
    prioridad_select = "p.prioridad_manual," if column_exists("pedidos", "prioridad_manual") else "NULL AS prioridad_manual,"
    prioridad_order = "CASE WHEN p.prioridad_manual IS NULL OR p.prioridad_manual = '' THEN 1 ELSE 0 END, p.prioridad_manual ASC," if column_exists("pedidos", "prioridad_manual") else ""
    query = f"""
        SELECT
            p.id AS pedido_id,
            p.id_pedido,
            p.cliente,
            {prioridad_select}
            COALESCE(pl.producto, '') AS producto,
            COALESCE(pl.color, '') AS color,
            COALESCE(pl.cantidad_pedida, 0) AS cantidad_pedida,
            COALESCE(pl.cantidad_asignada, 0) AS cantidad_asignada,
            COALESCE(pl.cantidad_faltante, 0) AS cantidad_faltante,
            ROUND(COALESCE(p.porcentaje_asignacion, 0), 2) AS porcentaje_asignacion,
            p.fecha_captura,
            COALESCE(pl.fecha_produccion, p.fecha_produccion) AS fecha_produccion,
            COALESCE(pl.fecha_logistica, p.fecha_logistica) AS fecha_logistica,
            COALESCE(pl.estatus_venta, p.estatus_venta) AS estatus_venta,
            COALESCE(pl.sku, '') AS sku,
            COALESCE(pl.descripcion, '') AS descripcion,
            pl.id AS pedido_linea_id,
            pl.line_no,
            COALESCE(pl.cantidad_enviada, 0) AS cantidad_enviada,
            pl.fecha_captura AS linea_fecha_captura,
            p.estado_operativo,
            p.tipo_registro,
            p.origen_registro
        FROM pedidos p
        INNER JOIN pedido_lineas pl
            ON pl.pedido_id = p.id
        WHERE p.estado_operativo = 'ACTIVO'
    """
    params: list[Any] = []
    query, params = _append_pedidos_activos_filters(query, params, filters)
    query += f"""
        ORDER BY
            {prioridad_order}
            CASE
                WHEN COALESCE(pl.fecha_produccion, p.fecha_produccion) IS NULL
                     OR TRIM(COALESCE(pl.fecha_produccion, p.fecha_produccion)) = '' THEN 1
                ELSE 0
            END,
            COALESCE(pl.fecha_produccion, p.fecha_produccion) ASC,
            p.id_pedido ASC,
            pl.line_no ASC,
            pl.producto ASC,
            pl.color ASC
    """
    return fetch_all(query, params)


def list_pedidos_for_chart(filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """
    Devuelve pedidos únicos para alimentar el gráfico final.

    Incluye label_x = "ID_pedido + cliente" para cumplir con la regla del eje X.
    La fijación del eje Y en 0-100 corresponde a la capa de UI/gráfico,
    pero aquí ya se deja el dataset preparado.
    """
    query = """
        SELECT DISTINCT
            p.id,
            p.id_pedido,
            p.cliente,
            ROUND(COALESCE(p.porcentaje_asignacion, 0), 2) AS porcentaje_asignacion,
            p.fecha_captura,
            p.fecha_produccion,
            p.fecha_logistica,
            p.estatus_venta,
            (p.id_pedido || ' - ' || COALESCE(p.cliente, '')) AS label_x
        FROM pedidos p
        INNER JOIN pedido_lineas pl
            ON pl.pedido_id = p.id
        WHERE p.estado_operativo = 'ACTIVO'
    """
    params: list[Any] = []
    query, params = _append_pedidos_activos_filters(query, params, filters)
    query += """
        ORDER BY
            p.porcentaje_asignacion ASC,
            p.id_pedido ASC
    """
    return fetch_all(query, params)


# =========================================================
# LISTADOS DE ENVIADOS
# =========================================================
def list_pedidos_enviados_summary() -> list[dict[str, Any]]:
    """
    Devuelve la tabla resumen de pedidos enviados.

    Se toma el último cierre por pedido y se incluyen columnas operativas
    necesarias para la pestaña de histórico.
    """
    prioridad_select = "p.prioridad_manual," if column_exists("pedidos", "prioridad_manual") else "NULL AS prioridad_manual,"
    prioridad_order = "CASE WHEN p.prioridad_manual IS NULL OR p.prioridad_manual = '' THEN 1 ELSE 0 END, p.prioridad_manual ASC," if column_exists("pedidos", "prioridad_manual") else ""
    query = f"""
        SELECT
            p.id,
            p.id_pedido,
            p.cliente,
            {prioridad_select}
            p.fecha_captura,
            p.fecha_produccion,
            p.fecha_logistica,
            p.estatus_venta,
            ROUND(COALESCE(p.porcentaje_asignacion, 0), 2) AS porcentaje_asignacion,
            p.estado_operativo,
            pc.tipo_cierre,
            pc.fecha_envio,
            pc.usuario AS cerrado_por,
            pc.observacion AS observacion_cierre,
            p.tipo_registro,
            p.origen_registro,
            p.pedido_origen_id,
            p.created_at,
            p.updated_at
        FROM pedidos p
        LEFT JOIN pedido_cierres pc
            ON pc.id = (
                SELECT pc2.id
                FROM pedido_cierres pc2
                WHERE pc2.pedido_id = p.id
                ORDER BY pc2.id DESC
                LIMIT 1
            )
        WHERE p.estado_operativo IN ('ENVIADO_TOTAL', 'ENVIADO_PARCIAL')
        ORDER BY
            {prioridad_order}
            CASE
                WHEN pc.fecha_envio IS NULL OR TRIM(pc.fecha_envio) = '' THEN 1
                ELSE 0
            END,
            pc.fecha_envio DESC,
            p.id_pedido DESC
    """
    return fetch_all(query)


# =========================================================
# UPDATE / RECÁLCULO / CIERRE
# =========================================================
def update_pedido_fields(pedido_id: int, fields: dict[str, Any]) -> None:
    """
    Actualiza campos permitidos del pedido.
    No hace validaciones de negocio; eso debe ocurrir en servicios/reglas.
    """
    if not fields:
        return

    safe_fields = {
        key: value for key, value in fields.items() if key in PEDIDO_ALLOWED_UPDATE_FIELDS
    }
    if not safe_fields:
        return

    set_clause = ", ".join(f"{field} = ?" for field in safe_fields.keys())
    params = list(safe_fields.values())
    params.append(pedido_id)

    query = f"""
        UPDATE pedidos
        SET {set_clause}
        WHERE id = ?
    """
    execute(query, params)


def recalculate_pedido_metrics(pedido_id: int) -> None:
    """
    Recalcula porcentaje_asignacion del pedido con base en sus líneas.
    """
    metrics_query = """
        SELECT
            COALESCE(SUM(cantidad_pedida), 0) AS total_pedido,
            COALESCE(SUM(cantidad_asignada), 0) AS total_asignado
        FROM pedido_lineas
        WHERE pedido_id = ?
    """
    metrics = fetch_one(metrics_query, (pedido_id,))

    total_pedido = _to_float(metrics["total_pedido"]) if metrics else 0.0
    total_asignado = _to_float(metrics["total_asignado"]) if metrics else 0.0

    porcentaje = 0.0
    if total_pedido > 0:
        porcentaje = round((total_asignado / total_pedido) * 100, 2)

    if porcentaje < 0:
        porcentaje = 0.0
    if porcentaje > 100:
        porcentaje = 100.0

    update_query = """
        UPDATE pedidos
        SET porcentaje_asignacion = ?
        WHERE id = ?
    """
    execute(update_query, (porcentaje, pedido_id))


def close_pedido(
    pedido_id: int,
    tipo_cierre: str,
    fecha_envio: str,
    usuario: str | None = None,
    observacion: str | None = None,
) -> None:
    """
    Cierra un pedido y lo mueve operativamente a enviados.

    Esta versión también guarda snapshot mínimo de las líneas en
    pedido_linea_cierres para no perder historia si el flujo la usa
    desde el repositorio.
    """
    pedido = get_pedido_by_id(pedido_id)
    if pedido is None:
        raise ValueError(f"No existe el pedido con id {pedido_id}.")

    lineas = fetch_all(
        """
        SELECT *
        FROM pedido_lineas
        WHERE pedido_id = ?
        ORDER BY line_no ASC, id ASC
        """,
        (pedido_id,),
    )

    tipo_cierre_norm = (_clean_text(tipo_cierre) or "").upper()
    if tipo_cierre_norm not in {"TOTAL", "PARCIAL"}:
        raise ValueError("tipo_cierre debe ser 'TOTAL' o 'PARCIAL'.")

    nuevo_estado = "ENVIADO_TOTAL" if tipo_cierre_norm == "TOTAL" else "ENVIADO_PARCIAL"

    with transaction() as conn:
        conn.execute(
            """
            UPDATE pedidos
            SET estado_operativo = ?
            WHERE id = ?
            """,
            (nuevo_estado, pedido_id),
        )

        cursor = conn.execute(
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
            (pedido_id, tipo_cierre_norm, fecha_envio, usuario, observacion),
        )
        pedido_cierre_id = int(cursor.lastrowid)

        for linea in lineas:
            cantidad_pedida = _to_float(linea.get("cantidad_pedida"))
            cantidad_asignada = _to_float(linea.get("cantidad_asignada"))
            cantidad_enviada = _to_float(linea.get("cantidad_enviada"))
            cantidad_no_enviada = max(0.0, cantidad_pedida - cantidad_enviada)

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
                    linea.get("fecha_produccion") or pedido.get("fecha_produccion"),
                    cantidad_pedida,
                    cantidad_asignada,
                    cantidad_enviada,
                    cantidad_no_enviada,
                ),
            )


def pedido_exists(id_pedido: str) -> bool:
    """
    Verifica si ya existe un pedido con ese id_pedido.
    """
    normalized = _clean_text(id_pedido)
    if not normalized:
        return False

    row = fetch_one(
        """
        SELECT 1 AS exists_flag
        FROM pedidos
        WHERE id_pedido = ?
        LIMIT 1
        """,
        (normalized,),
    )
    return row is not None


# =========================================================
# OPCIONES DE FILTRO
# =========================================================
def list_distinct_clientes_activos() -> list[str]:
    """
    Devuelve clientes únicos de pedidos activos.
    """
    rows = fetch_all(
        """
        SELECT DISTINCT cliente
        FROM pedidos
        WHERE estado_operativo = 'ACTIVO'
          AND cliente IS NOT NULL
          AND TRIM(cliente) <> ''
        ORDER BY cliente ASC
        """
    )
    return [row["cliente"] for row in rows]


def list_distinct_estatus_venta_activos() -> list[str]:
    """
    Devuelve estatus_venta únicos de pedidos activos.
    """
    rows = fetch_all(
        """
        SELECT DISTINCT estatus_venta
        FROM pedidos
        WHERE estado_operativo = 'ACTIVO'
          AND estatus_venta IS NOT NULL
          AND TRIM(estatus_venta) <> ''
        ORDER BY estatus_venta ASC
        """
    )
    return [row["estatus_venta"] for row in rows]


def list_distinct_fechas_produccion_activas() -> list[str]:
    """
    Devuelve fechas de producción únicas presentes en pedidos activos.
    Útil para filtros con selector calendario o listas auxiliares.
    """
    rows = fetch_all(
        """
        SELECT DISTINCT fecha_produccion
        FROM pedidos
        WHERE estado_operativo = 'ACTIVO'
          AND fecha_produccion IS NOT NULL
          AND TRIM(fecha_produccion) <> ''
        ORDER BY fecha_produccion ASC
        """
    )
    return [row["fecha_produccion"] for row in rows]


# =========================================================
# ESTRUCTURA COMPLETA DEL PEDIDO
# =========================================================
def get_pedido_full(pedido_id: int) -> dict[str, Any] | None:
    """
    Devuelve un pedido y todas sus líneas en una sola estructura.
    """
    pedido = get_pedido_by_id(pedido_id)
    if pedido is None:
        return None

    lineas = fetch_all(
        """
        SELECT *
        FROM pedido_lineas
        WHERE pedido_id = ?
        ORDER BY line_no ASC, id ASC
        """,
        (pedido_id,),
    )

    cierre = fetch_one(
        """
        SELECT *
        FROM pedido_cierres
        WHERE pedido_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (pedido_id,),
    )

    lineas_cierre: list[dict[str, Any]] = []
    if cierre is not None:
        lineas_cierre = fetch_all(
            """
            SELECT *
            FROM pedido_linea_cierres
            WHERE pedido_cierre_id = ?
            ORDER BY id ASC
            """,
            (cierre["id"],),
        )

    return {
        "pedido": pedido,
        "lineas": lineas,
        "cierre": cierre,
        "lineas_cierre": lineas_cierre,
    }
