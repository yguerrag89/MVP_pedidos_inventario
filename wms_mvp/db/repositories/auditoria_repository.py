from __future__ import annotations

from typing import Any

from wms_mvp.db.connection import execute, fetch_all, fetch_one, get_connection, transaction


_TABLE_NAME = "auditoria_cambios"


# -----------------------------------------------------------------------------
# Helpers de esquema / compatibilidad
# -----------------------------------------------------------------------------

def _get_columns() -> set[str]:
    with get_connection(as_dict=False) as conn:
        rows = conn.execute(f"PRAGMA table_info({_TABLE_NAME})").fetchall()
    return {str(row[1]) for row in rows}


def _col(*candidates: str) -> str | None:
    cols = _get_columns()
    for name in candidates:
        if name in cols:
            return name
    return None


def _usuario_col() -> str:
    return _col("usuario", "created_by") or "usuario"


def _fecha_col() -> str:
    return _col("fecha_cambio", "created_at") or "fecha_cambio"


def _base_select() -> str:
    usuario_col = _usuario_col()
    fecha_col = _fecha_col()
    return f"""
        SELECT
            id,
            tabla,
            registro_id,
            campo,
            valor_anterior,
            valor_nuevo,
            {usuario_col} AS usuario,
            motivo,
            {fecha_col} AS fecha_cambio
        FROM {_TABLE_NAME}
    """


# -----------------------------------------------------------------------------
# Escritura
# -----------------------------------------------------------------------------

def create_auditoria_cambio(data: dict[str, Any]) -> int:
    """
    Inserta un registro de auditoría y devuelve su id.

    Compatibilidad:
    - usuario <-> created_by
    - fecha_cambio / created_at se dejan al DEFAULT del esquema.
    """
    usuario_col = _usuario_col()

    query = f"""
        INSERT INTO {_TABLE_NAME} (
            tabla,
            registro_id,
            campo,
            valor_anterior,
            valor_nuevo,
            {usuario_col},
            motivo
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        str(data["tabla"]),
        str(data["registro_id"]),
        str(data["campo"]),
        None if data.get("valor_anterior") is None else str(data.get("valor_anterior")),
        None if data.get("valor_nuevo") is None else str(data.get("valor_nuevo")),
        data.get("usuario") or data.get("created_by"),
        data.get("motivo"),
    )
    return execute(query, params)



def create_many_auditoria_cambios(rows: list[dict[str, Any]]) -> None:
    """
    Inserta múltiples registros de auditoría.
    """
    if not rows:
        return

    usuario_col = _usuario_col()
    query = f"""
        INSERT INTO {_TABLE_NAME} (
            tabla,
            registro_id,
            campo,
            valor_anterior,
            valor_nuevo,
            {usuario_col},
            motivo
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    params_seq = [
        (
            str(row["tabla"]),
            str(row["registro_id"]),
            str(row["campo"]),
            None if row.get("valor_anterior") is None else str(row.get("valor_anterior")),
            None if row.get("valor_nuevo") is None else str(row.get("valor_nuevo")),
            row.get("usuario") or row.get("created_by"),
            row.get("motivo"),
        )
        for row in rows
    ]

    with transaction() as conn:
        conn.executemany(query, params_seq)


# -----------------------------------------------------------------------------
# Lectura
# -----------------------------------------------------------------------------

def get_auditoria_cambio_by_id(auditoria_id: int) -> dict[str, Any] | None:
    query = _base_select() + " WHERE id = ?"
    return fetch_one(query, (auditoria_id,))



def list_auditoria_by_tabla(tabla: str, limit: int = 500) -> list[dict[str, Any]]:
    query = (
        _base_select()
        + " WHERE tabla = ? ORDER BY fecha_cambio DESC, id DESC LIMIT ?"
    )
    return fetch_all(query, (tabla, int(limit)))



def list_auditoria_by_registro(
    tabla: str,
    registro_id: str | int,
    limit: int = 500,
) -> list[dict[str, Any]]:
    query = (
        _base_select()
        + " WHERE tabla = ? AND registro_id = ? ORDER BY fecha_cambio DESC, id DESC LIMIT ?"
    )
    return fetch_all(query, (tabla, str(registro_id), int(limit)))



def list_auditoria_by_usuario(usuario: str, limit: int = 500) -> list[dict[str, Any]]:
    usuario_col = _usuario_col()
    query = (
        _base_select()
        + f" WHERE {usuario_col} = ? ORDER BY fecha_cambio DESC, id DESC LIMIT ?"
    )
    return fetch_all(query, (usuario, int(limit)))



def list_auditoria_recent(limit: int = 500) -> list[dict[str, Any]]:
    query = _base_select() + " ORDER BY fecha_cambio DESC, id DESC LIMIT ?"
    return fetch_all(query, (int(limit),))



def list_auditoria_filtered(filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """
    Lista auditoría con filtros opcionales.

    Filtros soportados:
    - tabla
    - registro_id
    - campo
    - usuario
    - fecha_desde
    - fecha_hasta
    - limit
    """
    filters = filters or {}
    limit = int(filters.get("limit", 500))
    usuario_col = _usuario_col()

    query = _base_select() + " WHERE 1 = 1"
    params: list[Any] = []

    if filters.get("tabla"):
        query += " AND tabla = ?"
        params.append(filters["tabla"])

    registro_id = filters.get("registro_id")
    if registro_id is not None and str(registro_id).strip() != "":
        query += " AND registro_id = ?"
        params.append(str(registro_id))

    if filters.get("campo"):
        query += " AND campo = ?"
        params.append(filters["campo"])

    if filters.get("usuario"):
        query += f" AND {usuario_col} = ?"
        params.append(filters["usuario"])

    if filters.get("fecha_desde"):
        query += " AND fecha_cambio >= ?"
        params.append(str(filters["fecha_desde"]))

    if filters.get("fecha_hasta"):
        query += " AND fecha_cambio <= ?"
        params.append(str(filters["fecha_hasta"]))

    query += " ORDER BY fecha_cambio DESC, id DESC LIMIT ?"
    params.append(limit)

    return fetch_all(query, params)



def get_distinct_auditoria_tablas() -> list[str]:
    rows = fetch_all(
        f"""
        SELECT DISTINCT tabla
        FROM {_TABLE_NAME}
        WHERE tabla IS NOT NULL
          AND TRIM(tabla) <> ''
        ORDER BY tabla ASC
        """
    )
    return [row["tabla"] for row in rows if row.get("tabla")]



def get_distinct_auditoria_usuarios() -> list[str]:
    usuario_col = _usuario_col()
    rows = fetch_all(
        f"""
        SELECT DISTINCT {usuario_col} AS usuario
        FROM {_TABLE_NAME}
        WHERE {usuario_col} IS NOT NULL
          AND TRIM({usuario_col}) <> ''
        ORDER BY {usuario_col} ASC
        """
    )
    return [row["usuario"] for row in rows if row.get("usuario")]



def count_auditoria_by_tabla(tabla: str) -> int:
    row = fetch_one(
        f"""
        SELECT COUNT(*) AS total
        FROM {_TABLE_NAME}
        WHERE tabla = ?
        """,
        (tabla,),
    )
    return int(row["total"]) if row else 0



def count_auditoria_by_registro(tabla: str, registro_id: str | int) -> int:
    row = fetch_one(
        f"""
        SELECT COUNT(*) AS total
        FROM {_TABLE_NAME}
        WHERE tabla = ?
          AND registro_id = ?
        """,
        (tabla, str(registro_id)),
    )
    return int(row["total"]) if row else 0


# -----------------------------------------------------------------------------
# API de alto nivel
# -----------------------------------------------------------------------------

def audit_field_change(
    tabla: str,
    registro_id: str | int,
    campo: str,
    valor_anterior: Any,
    valor_nuevo: Any,
    usuario: str | None = None,
    motivo: str | None = None,
) -> int:
    """
    Registra un cambio de campo solo si el valor realmente cambió.
    """
    old_value = None if valor_anterior is None else str(valor_anterior)
    new_value = None if valor_nuevo is None else str(valor_nuevo)

    if old_value == new_value:
        return 0

    return create_auditoria_cambio(
        {
            "tabla": tabla,
            "registro_id": str(registro_id),
            "campo": campo,
            "valor_anterior": old_value,
            "valor_nuevo": new_value,
            "usuario": usuario,
            "motivo": motivo,
        }
    )
