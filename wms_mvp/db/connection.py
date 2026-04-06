from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Iterable, Mapping, Sequence

from wms_mvp.core.config import DB_PATH, ensure_directories


DEFAULT_TIMEOUT_SECONDS = 30.0

# Columnas equivalentes usadas en distintas iteraciones del MVP.
SCHEMA_ALIASES: dict[str, set[str]] = {
    "fecha_cierre": {"fecha_envio"},
    "fecha_envio": {"fecha_cierre"},
    "corte_entrada_id": {"corte_id"},
    "corte_id": {"corte_entrada_id"},
    "cierre_id": {"pedido_cierre_id"},
    "pedido_cierre_id": {"cierre_id"},
    "usuario": {"created_by", "procesado_por"},
    "created_by": {"usuario"},
    "procesado_por": {"usuario"},
    "procesado_en": {"procesado_at"},
    "procesado_at": {"procesado_en"},
    "estado": {"estado_corte"},
    "estado_corte": {"estado"},
    "hora_corte": {"hora_corte_normalizada"},
    "hora_corte_normalizada": {"hora_corte"},
    "sugerencia_pedido": {"pedido_sugerido_texto"},
    "pedido_sugerido_texto": {"sugerencia_pedido"},
    "decision_tipo": {"decision_type"},
    "decision_type": {"decision_tipo"},
    "created_at": {"fecha_cambio"},
    "fecha_cambio": {"created_at"},
    "tipo_asignacion": {"fuente_asignacion"},
    "fuente_asignacion": {"tipo_asignacion"},
    "entradas_recibidas": {"total_recibido"},
    "total_recibido": {"entradas_recibidas"},
}


def _dict_row_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    """Convierte cada fila del cursor en un diccionario simple."""
    return {column[0]: row[index] for index, column in enumerate(cursor.description)}


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """Aplica configuraciones recomendadas para SQLite."""
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("PRAGMA busy_timeout = 30000;")


def _normalize_name(name: str) -> str:
    return str(name or "").strip().lower()


def equivalent_column_names(column_name: str) -> set[str]:
    """
    Devuelve el conjunto de nombres aceptables para una columna,
    contemplando aliases históricos del MVP.
    """
    normalized = _normalize_name(column_name)
    aliases = {_normalize_name(alias) for alias in SCHEMA_ALIASES.get(normalized, set())}
    return {normalized, *aliases}


def get_connection(
    as_dict: bool = True,
    db_path: Path | None = None,
    *,
    read_only: bool = False,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> sqlite3.Connection:
    """
    Crea y devuelve una conexión a SQLite.

    Args:
        as_dict: Si es True, las filas se devuelven como diccionarios.
        db_path: Ruta alternativa a la base de datos.
        read_only: Abre la base en modo solo lectura.
        timeout: Tiempo de espera para bloqueo de SQLite.

    Returns:
        Conexión sqlite3 abierta.
    """
    ensure_directories()

    target_db = db_path or DB_PATH
    target_db.parent.mkdir(parents=True, exist_ok=True)

    if read_only:
        uri = f"file:{target_db.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=timeout, check_same_thread=False)
    else:
        conn = sqlite3.connect(target_db, timeout=timeout, check_same_thread=False)

    _apply_pragmas(conn)

    if as_dict:
        conn.row_factory = _dict_row_factory
    else:
        conn.row_factory = sqlite3.Row

    return conn


@contextmanager
def get_db(
    as_dict: bool = True,
    db_path: Path | None = None,
    *,
    read_only: bool = False,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Generator[sqlite3.Connection, None, None]:
    """Context manager para abrir y cerrar la conexión automáticamente."""
    conn = get_connection(as_dict=as_dict, db_path=db_path, read_only=read_only, timeout=timeout)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(
    as_dict: bool = True,
    db_path: Path | None = None,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager para ejecutar operaciones dentro de una transacción.
    Hace commit si todo sale bien y rollback si ocurre un error.
    """
    conn = get_connection(as_dict=as_dict, db_path=db_path, read_only=False, timeout=timeout)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def table_exists(table_name: str, db_path: Path | None = None) -> bool:
    """Indica si una tabla existe físicamente en la base."""
    sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND lower(name)=lower(?) LIMIT 1"
    row = fetch_one(sql, [table_name], db_path=db_path)
    return row is not None


def get_table_columns(table_name: str, db_path: Path | None = None) -> list[str]:
    """Devuelve la lista de columnas reales de una tabla."""
    if not table_exists(table_name, db_path=db_path):
        return []

    with get_db(as_dict=True, db_path=db_path, read_only=True) as conn:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row["name"]) for row in rows]


def column_exists(
    table_name: str,
    column_name: str,
    db_path: Path | None = None,
    *,
    allow_aliases: bool = True,
) -> bool:
    """
    Verifica si una columna existe en una tabla.
    Si allow_aliases=True, acepta nombres equivalentes definidos en SCHEMA_ALIASES.
    """
    real_columns = {_normalize_name(col) for col in get_table_columns(table_name, db_path=db_path)}
    if not real_columns:
        return False

    accepted = equivalent_column_names(column_name) if allow_aliases else {_normalize_name(column_name)}
    return bool(real_columns.intersection(accepted))


def schema_has_columns(
    table_name: str,
    required_columns: Sequence[str],
    db_path: Path | None = None,
    *,
    allow_aliases: bool = True,
) -> tuple[bool, list[str]]:
    """
    Verifica si una tabla contiene todas las columnas requeridas.

    Returns:
        (cumple, faltantes)
    """
    missing = [
        column_name
        for column_name in required_columns
        if not column_exists(
            table_name,
            column_name,
            db_path=db_path,
            allow_aliases=allow_aliases,
        )
    ]
    return len(missing) == 0, missing


def fetch_one(
    query: str,
    params: Iterable[Any] | None = None,
    db_path: Path | None = None,
) -> dict[str, Any] | None:
    """Ejecuta una consulta y devuelve una sola fila."""
    with get_db(as_dict=True, db_path=db_path, read_only=True) as conn:
        cursor = conn.execute(query, tuple(params or ()))
        row = cursor.fetchone()
        return dict(row) if isinstance(row, Mapping) else row


def fetch_all(
    query: str,
    params: Iterable[Any] | None = None,
    db_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Ejecuta una consulta y devuelve todas las filas."""
    with get_db(as_dict=True, db_path=db_path, read_only=True) as conn:
        cursor = conn.execute(query, tuple(params or ()))
        rows = cursor.fetchall()
        return [dict(row) if isinstance(row, Mapping) else row for row in rows]


def execute(
    query: str,
    params: Iterable[Any] | None = None,
    db_path: Path | None = None,
) -> int:
    """
    Ejecuta una sentencia INSERT, UPDATE o DELETE.

    Returns:
        lastrowid si existe; en otro caso 0.
    """
    with transaction(as_dict=True, db_path=db_path) as conn:
        cursor = conn.execute(query, tuple(params or ()))
        return int(cursor.lastrowid or 0)


def execute_script(script: str, db_path: Path | None = None) -> None:
    """Ejecuta un script SQL completo."""
    with transaction(as_dict=True, db_path=db_path) as conn:
        conn.executescript(script)


def executemany(
    query: str,
    params_seq: Iterable[Iterable[Any]],
    db_path: Path | None = None,
) -> int:
    """
    Ejecuta la misma sentencia SQL múltiples veces.

    Returns:
        Número de filas afectadas reportadas por SQLite.
    """
    with transaction(as_dict=True, db_path=db_path) as conn:
        cursor = conn.executemany(query, [tuple(params) for params in params_seq])
        return int(cursor.rowcount if cursor.rowcount is not None else 0)


def db_exists(db_path: Path | None = None) -> bool:
    """Indica si la base de datos existe físicamente."""
    target_db = db_path or DB_PATH
    return target_db.exists()


def get_sqlite_version(db_path: Path | None = None) -> str:
    """Devuelve la versión de SQLite disponible en la conexión."""
    row = fetch_one("SELECT sqlite_version() AS version", db_path=db_path)
    return str((row or {}).get("version", "desconocida"))
