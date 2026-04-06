from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any

from wms_mvp.db.connection import db_exists, fetch_all, fetch_one, transaction

try:
    from wms_mvp.db.connection import column_exists
except Exception:  # pragma: no cover
    column_exists = None  # type: ignore


# -----------------------------------------------------------------------------
# Compatibilidad de esquema
# -----------------------------------------------------------------------------
_AUDIT_USER_COL_CANDIDATES = ("usuario", "created_by")
_AUDIT_DATE_COL_CANDIDATES = ("fecha_cambio", "created_at")


def _table_has_column(table_name: str, column_name: str) -> bool:
    if callable(column_exists):
        try:
            return bool(column_exists(table_name, column_name))
        except Exception:
            pass

    try:
        row = fetch_one(f"PRAGMA table_info({table_name})")
        if row is None:
            return False
        rows = fetch_all(f"PRAGMA table_info({table_name})")
        cols = {str(r.get("name") or "").strip() for r in rows}
        return column_name in cols
    except Exception:
        return False


def _pick_existing_column(table_name: str, candidates: tuple[str, ...], fallback: str) -> str:
    for candidate in candidates:
        if _table_has_column(table_name, candidate):
            return candidate
    return fallback


def _audit_user_col() -> str:
    return _pick_existing_column("auditoria_cambios", _AUDIT_USER_COL_CANDIDATES, "usuario")


def _audit_date_col() -> str:
    return _pick_existing_column("auditoria_cambios", _AUDIT_DATE_COL_CANDIDATES, "fecha_cambio")


def _select_usuario_sql() -> str:
    user_col = _audit_user_col()
    if user_col == "usuario":
        return "usuario"
    return f"{user_col} AS usuario"


def _select_fecha_sql() -> str:
    date_col = _audit_date_col()
    if date_col == "fecha_cambio":
        return "fecha_cambio"
    return f"{date_col} AS fecha_cambio"


# -----------------------------------------------------------------------------
# Helpers internos
# -----------------------------------------------------------------------------
def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _serialize_value(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, bool):
        return "1" if value else "0"

    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)

    return str(value)


def _normalize_end_of_day(value: str | None) -> str | None:
    text = _safe_text(value)
    if not text:
        return None

    if len(text) == 10 and text.count("-") == 2:
        return f"{text} 23:59:59"
    return text


def _build_where_clause(filters: dict[str, Any] | None) -> tuple[str, list[Any]]:
    filters = filters or {}
    clauses: list[str] = []
    params: list[Any] = []

    user_col = _audit_user_col()
    date_col = _audit_date_col()

    tabla = _safe_text(filters.get("tabla"))
    if tabla:
        clauses.append("tabla = ?")
        params.append(tabla)

    usuario = _safe_text(filters.get("usuario"))
    if usuario:
        clauses.append(f"{user_col} = ?")
        params.append(usuario)

    registro_id = _safe_text(filters.get("registro_id"))
    if registro_id:
        clauses.append("registro_id = ?")
        params.append(registro_id)

    campo = _safe_text(filters.get("campo"))
    if campo:
        clauses.append("campo = ?")
        params.append(campo)

    fecha_desde = _safe_text(filters.get("fecha_desde"))
    if fecha_desde:
        clauses.append(f"{date_col} >= ?")
        params.append(fecha_desde)

    fecha_hasta = _normalize_end_of_day(filters.get("fecha_hasta"))
    if fecha_hasta:
        clauses.append(f"{date_col} <= ?")
        params.append(fecha_hasta)

    if not clauses:
        return "", params

    return "WHERE " + " AND ".join(clauses), params


# -----------------------------------------------------------------------------
# Escritura de auditoría
# -----------------------------------------------------------------------------
def create_audit_change(
    *,
    tabla: str,
    registro_id: str | int,
    campo: str,
    valor_anterior: Any = None,
    valor_nuevo: Any = None,
    usuario: str | None = None,
    motivo: str | None = None,
    skip_if_equal: bool = True,
) -> int | None:
    if not db_exists():
        raise FileNotFoundError("La base de datos no existe.")

    tabla_text = _safe_text(tabla)
    registro_id_text = _safe_text(registro_id)
    campo_text = _safe_text(campo)

    if not tabla_text:
        raise ValueError("tabla es obligatoria.")
    if not registro_id_text:
        raise ValueError("registro_id es obligatorio.")
    if not campo_text:
        raise ValueError("campo es obligatorio.")

    old_value = _serialize_value(valor_anterior)
    new_value = _serialize_value(valor_nuevo)

    if skip_if_equal and old_value == new_value:
        return None

    user_col = _audit_user_col()
    columns = [
        "tabla",
        "registro_id",
        "campo",
        "valor_anterior",
        "valor_nuevo",
        user_col,
        "motivo",
    ]
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"""
        INSERT INTO auditoria_cambios ({", ".join(columns)})
        VALUES ({placeholders})
    """

    with transaction() as conn:
        cursor = conn.execute(
            sql,
            (
                tabla_text,
                registro_id_text,
                campo_text,
                old_value,
                new_value,
                _safe_text(usuario),
                _safe_text(motivo),
            ),
        )
        return int(cursor.lastrowid)


def create_auditoria_cambio(payload: dict[str, Any], skip_if_equal: bool = True) -> int | None:
    return create_audit_change(
        tabla=payload.get("tabla"),
        registro_id=payload.get("registro_id"),
        campo=payload.get("campo"),
        valor_anterior=payload.get("valor_anterior"),
        valor_nuevo=payload.get("valor_nuevo"),
        usuario=payload.get("usuario") or payload.get("created_by"),
        motivo=payload.get("motivo"),
        skip_if_equal=skip_if_equal,
    )


def bulk_create_audit_changes(
    cambios: list[dict[str, Any]],
    skip_if_equal: bool = True,
) -> list[int]:
    created_ids: list[int] = []
    for cambio in cambios:
        audit_id = create_auditoria_cambio(cambio, skip_if_equal=skip_if_equal)
        if audit_id is not None:
            created_ids.append(audit_id)
    return created_ids


# -----------------------------------------------------------------------------
# Lectura de auditoría
# -----------------------------------------------------------------------------
def get_audit_by_id(audit_id: int) -> dict[str, Any] | None:
    if not db_exists():
        return None

    return fetch_one(
        f"""
        SELECT
            id,
            tabla,
            registro_id,
            campo,
            valor_anterior,
            valor_nuevo,
            {_select_usuario_sql()},
            motivo,
            {_select_fecha_sql()}
        FROM auditoria_cambios
        WHERE id = ?
        """,
        (audit_id,),
    )


def list_audits(
    limit: int = 100,
    offset: int = 0,
    filters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if not db_exists():
        return []

    limit = max(int(limit), 1)
    offset = max(int(offset), 0)
    date_col = _audit_date_col()

    where_sql, params = _build_where_clause(filters)
    query = f"""
        SELECT
            id,
            tabla,
            registro_id,
            campo,
            valor_anterior,
            valor_nuevo,
            {_select_usuario_sql()},
            motivo,
            {_select_fecha_sql()}
        FROM auditoria_cambios
        {where_sql}
        ORDER BY {date_col} DESC, id DESC
        LIMIT ? OFFSET ?
    """
    return fetch_all(query, tuple([*params, limit, offset]))


def get_recent_audits(limit: int = 20) -> list[dict[str, Any]]:
    return list_audits(limit=limit, offset=0, filters=None)


def count_audits(filters: dict[str, Any] | None = None) -> int:
    if not db_exists():
        return 0

    where_sql, params = _build_where_clause(filters)
    row = fetch_one(
        f"""
        SELECT COUNT(*) AS total
        FROM auditoria_cambios
        {where_sql}
        """,
        tuple(params),
    )
    return int((row or {}).get("total") or 0)


def get_audit_summary(days: int = 30) -> dict[str, Any]:
    if not db_exists():
        return {
            "total": 0,
            "ultimos_dias": 0,
            "por_tabla": [],
            "por_usuario": [],
        }

    days = max(int(days), 1)
    date_col = _audit_date_col()
    user_col = _audit_user_col()

    total_row = fetch_one("SELECT COUNT(*) AS total FROM auditoria_cambios")

    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    recent_row = fetch_one(
        f"""
        SELECT COUNT(*) AS total
        FROM auditoria_cambios
        WHERE {date_col} >= ?
        """,
        (cutoff,),
    )

    por_tabla = fetch_all(
        """
        SELECT
            tabla,
            COUNT(*) AS total
        FROM auditoria_cambios
        GROUP BY tabla
        ORDER BY total DESC, tabla ASC
        """
    )

    por_usuario = fetch_all(
        f"""
        SELECT
            COALESCE({user_col}, 'N/D') AS usuario,
            COUNT(*) AS total
        FROM auditoria_cambios
        GROUP BY COALESCE({user_col}, 'N/D')
        ORDER BY total DESC, usuario ASC
        """
    )

    return {
        "total": int((total_row or {}).get("total") or 0),
        "ultimos_dias": int((recent_row or {}).get("total") or 0),
        "por_tabla": por_tabla,
        "por_usuario": por_usuario,
    }


# -----------------------------------------------------------------------------
# Utilidades de uso frecuente
# -----------------------------------------------------------------------------
def audit_simple_field_change(
    *,
    tabla: str,
    registro_id: str | int,
    campo: str,
    valor_anterior: Any,
    valor_nuevo: Any,
    usuario: str | None = None,
    motivo: str | None = None,
) -> int | None:
    return create_audit_change(
        tabla=tabla,
        registro_id=registro_id,
        campo=campo,
        valor_anterior=valor_anterior,
        valor_nuevo=valor_nuevo,
        usuario=usuario,
        motivo=motivo,
        skip_if_equal=True,
    )


__all__ = [
    "audit_simple_field_change",
    "bulk_create_audit_changes",
    "count_audits",
    "create_audit_change",
    "create_auditoria_cambio",
    "get_audit_by_id",
    "get_audit_summary",
    "get_recent_audits",
    "list_audits",
]
