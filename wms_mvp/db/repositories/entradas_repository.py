from __future__ import annotations

from functools import lru_cache
from typing import Any, Iterable

from wms_mvp.db.connection import execute, fetch_all, fetch_one, transaction


# =========================================================
# Helpers de compatibilidad de esquema
# =========================================================
@lru_cache(maxsize=64)
def _table_columns(table_name: str) -> set[str]:
    """
    Devuelve el conjunto de columnas existentes en una tabla.
    Se cachea para evitar repetir PRAGMA en cada operación.
    """
    with transaction() as conn:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    columns: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            columns.add(str(row.get("name")))
        else:
            columns.add(str(row[1]))
    return columns


def _has_column(table_name: str, column_name: str) -> bool:
    return column_name in _table_columns(table_name)


def _filter_fields_for_table(
    table_name: str,
    data: dict[str, Any],
    aliases: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Filtra un diccionario para dejar únicamente columnas reales de la tabla.
    También soporta aliases suaves para no romper compatibilidad entre versiones.
    """
    aliases = aliases or {}
    table_cols = _table_columns(table_name)
    safe: dict[str, Any] = {}

    for key, value in data.items():
        target = aliases.get(key, key)
        if target in table_cols:
            safe[target] = value

    return safe


def _insert_dynamic(table_name: str, data: dict[str, Any]) -> int:
    if not data:
        raise ValueError(f"No hay datos válidos para insertar en {table_name}.")

    fields = list(data.keys())
    placeholders = ", ".join("?" for _ in fields)
    columns = ", ".join(fields)
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    params = [data[field] for field in fields]
    return execute(query, params)


def _update_dynamic(table_name: str, record_id: int, fields: dict[str, Any]) -> None:
    if not fields:
        return

    set_clause = ", ".join(f"{field} = ?" for field in fields.keys())
    params = [*fields.values(), record_id]
    query = f"UPDATE {table_name} SET {set_clause} WHERE id = ?"
    execute(query, params)


# =========================================================
# IMPORT JOBS
# =========================================================
def create_import_job(data: dict[str, Any]) -> int:
    """
    Inserta un registro de importación y devuelve su id.
    Compatible con el esquema corregido y con variaciones menores.
    """
    payload = _filter_fields_for_table("import_jobs", data)
    return _insert_dynamic("import_jobs", payload)


def get_import_job_by_id(import_job_id: int) -> dict[str, Any] | None:
    query = """
        SELECT *
        FROM import_jobs
        WHERE id = ?
    """
    return fetch_one(query, (import_job_id,))


def update_import_job_fields(import_job_id: int, fields: dict[str, Any]) -> None:
    safe_fields = _filter_fields_for_table("import_jobs", fields)
    _update_dynamic("import_jobs", import_job_id, safe_fields)


def list_import_jobs(limit: int = 100) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM import_jobs
        ORDER BY id DESC
        LIMIT ?
    """
    return fetch_all(query, (limit,))


# =========================================================
# CORTES
# =========================================================
def create_corte_entrada(data: dict[str, Any]) -> int:
    """
    Inserta un corte de entrada y devuelve su id.
    Acepta aliases suaves para compatibilidad entre iteraciones del MVP.
    """
    payload = _filter_fields_for_table(
        "cortes_entrada",
        data,
        aliases={
            "hora_corte": "hora_corte_normalizada",
            "estado": "estado_corte",
            "usuario": "procesado_por",
            "procesado_en": "procesado_at",
            "archivo_nombre": "file_name",  # solo se ignora si no existe
        },
    )
    return _insert_dynamic("cortes_entrada", payload)


def get_corte_by_id(corte_id: int) -> dict[str, Any] | None:
    query = """
        SELECT *
        FROM cortes_entrada
        WHERE id = ?
    """
    return fetch_one(query, (corte_id,))


def get_corte_by_fecha_hora(fecha_corte: str, hora_corte_normalizada: str) -> dict[str, Any] | None:
    query = """
        SELECT *
        FROM cortes_entrada
        WHERE fecha_corte = ?
          AND hora_corte_normalizada = ?
    """
    return fetch_one(query, (fecha_corte, hora_corte_normalizada))


def corte_exists(fecha_corte: str, hora_corte_normalizada: str) -> bool:
    row = fetch_one(
        """
        SELECT 1 AS exists_flag
        FROM cortes_entrada
        WHERE fecha_corte = ?
          AND hora_corte_normalizada = ?
        LIMIT 1
        """,
        (fecha_corte, hora_corte_normalizada),
    )
    return row is not None


def list_cortes(limit: int = 200) -> list[dict[str, Any]]:
    """
    Lista cortes recientes junto con metadatos del archivo de importación.
    Devuelve aliases amigables para la UI sin perder columnas reales.
    """
    query = """
        SELECT
            c.*,
            ij.import_type,
            ij.file_name,
            ij.file_name AS archivo_nombre,
            c.hora_corte_normalizada AS hora_corte,
            c.estado_corte AS estado,
            c.procesado_por AS usuario,
            c.procesado_at AS procesado_en
        FROM cortes_entrada c
        INNER JOIN import_jobs ij
            ON ij.id = c.import_job_id
        ORDER BY c.fecha_corte DESC, c.hora_corte_normalizada DESC, c.id DESC
        LIMIT ?
    """
    return fetch_all(query, (limit,))


def list_cortes_by_import_job(import_job_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM cortes_entrada
        WHERE import_job_id = ?
        ORDER BY fecha_corte ASC, hora_corte_normalizada ASC, id ASC
    """
    return fetch_all(query, (import_job_id,))


def update_corte_fields(corte_id: int, fields: dict[str, Any]) -> None:
    safe_fields = _filter_fields_for_table(
        "cortes_entrada",
        fields,
        aliases={
            "hora_corte": "hora_corte_normalizada",
            "estado": "estado_corte",
            "usuario": "procesado_por",
            "procesado_en": "procesado_at",
        },
    )
    _update_dynamic("cortes_entrada", corte_id, safe_fields)


def mark_corte_as_processed(
    corte_id: int,
    procesado_por: str | None = None,
    procesado_at: str | None = None,
    reprocesado: bool = False,
) -> None:
    estado = "REPROCESADO" if reprocesado else "PROCESADO"
    update_corte_fields(
        corte_id,
        {
            "estado_corte": estado,
            "procesado_por": procesado_por,
            "procesado_at": procesado_at,
        },
    )


# =========================================================
# ENTRADAS STAGING
# =========================================================
def _prepare_staging_payload(data: dict[str, Any]) -> dict[str, Any]:
    payload = dict(data)
    if "sugerencia_pedido" in payload and "pedido_sugerido_texto" not in payload:
        payload["pedido_sugerido_texto"] = payload.get("sugerencia_pedido")
    if "texto_original_destino" not in payload and payload.get("sugerencia_destino_texto"):
        payload["texto_original_destino"] = payload.get("sugerencia_destino_texto")

    return _filter_fields_for_table(
        "entradas_staging",
        payload,
        aliases={
            "sugerencia_pedido": "pedido_sugerido_texto",
        },
    )


def create_entrada_staging(data: dict[str, Any]) -> int:
    payload = _prepare_staging_payload(data)
    return _insert_dynamic("entradas_staging", payload)


def create_many_entradas_staging(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    prepared_rows = [_prepare_staging_payload(row) for row in rows]
    if not prepared_rows:
        return

    # Homogeneizar columnas según el primer payload válido.
    fields = list(prepared_rows[0].keys())
    query = f"""
        INSERT INTO entradas_staging ({", ".join(fields)})
        VALUES ({", ".join("?" for _ in fields)})
    """
    params_seq = [tuple(row.get(field) for field in fields) for row in prepared_rows]

    with transaction() as conn:
        conn.executemany(query, params_seq)


def get_entrada_staging_by_id(entrada_id: int) -> dict[str, Any] | None:
    query = """
        SELECT
            es.*,
            es.pedido_sugerido_texto AS sugerencia_pedido
        FROM entradas_staging es
        WHERE es.id = ?
    """
    return fetch_one(query, (entrada_id,))


def get_entrada_staging_by_corte_and_row_num(corte_id: int, row_num: int) -> dict[str, Any] | None:
    query = """
        SELECT
            es.*,
            es.pedido_sugerido_texto AS sugerencia_pedido
        FROM entradas_staging es
        WHERE es.corte_id = ?
          AND es.row_num = ?
    """
    return fetch_one(query, (corte_id, row_num))


def list_entradas_staging_by_corte(
    corte_id: int,
    estado_revision: str | None = None,
) -> list[dict[str, Any]]:
    query = """
        SELECT
            es.*,
            es.pedido_sugerido_texto AS sugerencia_pedido
        FROM entradas_staging es
        WHERE es.corte_id = ?
    """
    params: list[Any] = [corte_id]

    if estado_revision:
        query += " AND es.estado_revision = ?"
        params.append(estado_revision)

    query += " ORDER BY es.row_num ASC, es.id ASC"
    return fetch_all(query, params)


def list_entradas_staging_problematicas(corte_id: int) -> list[dict[str, Any]]:
    """
    Lista filas que típicamente requieren revisión.
    La lógica profunda vive en el servicio, pero aquí dejamos una preselección útil.
    """
    query = """
        SELECT
            es.*,
            es.pedido_sugerido_texto AS sugerencia_pedido
        FROM entradas_staging es
        WHERE es.corte_id = ?
          AND (
                es.estado_revision IN ('PENDIENTE', 'EN_REVISION')
                OR es.sku IS NULL
                OR TRIM(COALESCE(es.sku, '')) = ''
                OR es.cantidad IS NULL
                OR es.cantidad <= 0
                OR es.fecha IS NULL
                OR TRIM(COALESCE(es.fecha, '')) = ''
                OR es.hora IS NULL
                OR TRIM(COALESCE(es.hora, '')) = ''
                OR es.tipo_sugerencia IN ('AMBIGUA', 'SIN_DATO')
              )
        ORDER BY es.row_num ASC, es.id ASC
    """
    return fetch_all(query, (corte_id,))


def list_entradas_staging_by_sku(corte_id: int, sku: str) -> list[dict[str, Any]]:
    query = """
        SELECT
            es.*,
            es.pedido_sugerido_texto AS sugerencia_pedido
        FROM entradas_staging es
        WHERE es.corte_id = ?
          AND es.sku = ?
        ORDER BY es.row_num ASC, es.id ASC
    """
    return fetch_all(query, (corte_id, sku))


def update_entrada_staging_fields(entrada_id: int, fields: dict[str, Any]) -> None:
    payload = _prepare_staging_payload(fields)
    _update_dynamic("entradas_staging", entrada_id, payload)


def set_entrada_staging_estado(entrada_id: int, estado_revision: str) -> None:
    query = """
        UPDATE entradas_staging
        SET estado_revision = ?
        WHERE id = ?
    """
    execute(query, (estado_revision, entrada_id))


def count_entradas_staging_by_corte(corte_id: int) -> dict[str, Any]:
    query = """
        SELECT
            COUNT(*) AS total_filas,
            COALESCE(SUM(cantidad), 0) AS total_piezas,
            SUM(CASE WHEN estado_revision = 'PENDIENTE' THEN 1 ELSE 0 END) AS pendientes,
            SUM(CASE WHEN estado_revision = 'RESUELTA' THEN 1 ELSE 0 END) AS resueltas,
            SUM(CASE WHEN estado_revision = 'EN_REVISION' THEN 1 ELSE 0 END) AS en_revision,
            SUM(CASE WHEN estado_revision = 'IGNORADA' THEN 1 ELSE 0 END) AS ignoradas
        FROM entradas_staging
        WHERE corte_id = ?
    """
    row = fetch_one(query, (corte_id,))
    return row or {
        "total_filas": 0,
        "total_piezas": 0,
        "pendientes": 0,
        "resueltas": 0,
        "en_revision": 0,
        "ignoradas": 0,
    }


def delete_entradas_staging_by_corte(corte_id: int) -> None:
    query = "DELETE FROM entradas_staging WHERE corte_id = ?"
    execute(query, (corte_id,))


# =========================================================
# DECISIONES SOBRE ENTRADAS
# =========================================================
def _prepare_decision_payload(data: dict[str, Any]) -> dict[str, Any]:
    return _filter_fields_for_table(
        "entrada_decisiones",
        data,
        aliases={
            "decision_tipo": "decision_type",
            "usuario": "created_by",
        },
    )


def create_entrada_decision(data: dict[str, Any]) -> int:
    payload = _prepare_decision_payload(data)
    return _insert_dynamic("entrada_decisiones", payload)


def create_many_entrada_decisiones(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    prepared_rows = [_prepare_decision_payload(row) for row in rows]
    if not prepared_rows:
        return

    fields = list(prepared_rows[0].keys())
    query = f"""
        INSERT INTO entrada_decisiones ({", ".join(fields)})
        VALUES ({", ".join("?" for _ in fields)})
    """
    params_seq = [tuple(row.get(field) for field in fields) for row in prepared_rows]

    with transaction() as conn:
        conn.executemany(query, params_seq)


def get_entrada_decision_by_id(decision_id: int) -> dict[str, Any] | None:
    query = """
        SELECT *
        FROM entrada_decisiones
        WHERE id = ?
    """
    return fetch_one(query, (decision_id,))


def list_decisiones_by_entrada(entrada_staging_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM entrada_decisiones
        WHERE entrada_staging_id = ?
        ORDER BY id ASC
    """
    return fetch_all(query, (entrada_staging_id,))


def list_decisiones_by_corte(corte_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT
            ed.*,
            es.corte_id,
            es.row_num,
            es.sugerencia_destino_texto,
            es.pedido_sugerido_texto
        FROM entrada_decisiones ed
        INNER JOIN entradas_staging es
            ON es.id = ed.entrada_staging_id
        WHERE es.corte_id = ?
        ORDER BY es.row_num ASC, ed.id ASC
    """
    return fetch_all(query, (corte_id,))


def sum_decisiones_cantidad_by_entrada(entrada_staging_id: int) -> float:
    row = fetch_one(
        """
        SELECT COALESCE(SUM(cantidad), 0) AS total_decidido
        FROM entrada_decisiones
        WHERE entrada_staging_id = ?
        """,
        (entrada_staging_id,),
    )
    return float(row["total_decidido"]) if row else 0.0


def delete_decisiones_by_entrada(entrada_staging_id: int) -> None:
    query = "DELETE FROM entrada_decisiones WHERE entrada_staging_id = ?"
    execute(query, (entrada_staging_id,))


# =========================================================
# UTILIDADES DE REVISIÓN
# =========================================================
def get_corte_full(corte_id: int) -> dict[str, Any] | None:
    corte = get_corte_by_id(corte_id)
    if corte is None:
        return None

    entradas = list_entradas_staging_by_corte(corte_id)
    decisiones = list_decisiones_by_corte(corte_id)
    counts = count_entradas_staging_by_corte(corte_id)

    return {
        "corte": corte,
        "entradas": entradas,
        "decisiones": decisiones,
        "counts": counts,
    }


def mark_entrada_as_resolved_if_fully_decided(entrada_staging_id: int) -> None:
    entrada = get_entrada_staging_by_id(entrada_staging_id)
    if entrada is None:
        return

    cantidad_entrada = float(entrada.get("cantidad") or 0)
    total_decidido = sum_decisiones_cantidad_by_entrada(entrada_staging_id)

    if abs(cantidad_entrada - total_decidido) < 1e-9:
        set_entrada_staging_estado(entrada_staging_id, "RESUELTA")


def mark_corte_in_revision_if_has_pending_rows(corte_id: int) -> None:
    counts = count_entradas_staging_by_corte(corte_id)
    pendientes = int(counts.get("pendientes") or 0)
    en_revision = int(counts.get("en_revision") or 0)

    if pendientes > 0 or en_revision > 0:
        update_corte_fields(corte_id, {"estado_corte": "EN_REVISION"})


# =========================================================
# ENTRADAS CONCRETAS / VISTAS OPERATIVAS POR IMPORT_JOB Y SKU
# =========================================================
def list_import_jobs_operativos(limit: int = 100) -> list[dict[str, Any]]:
    """
    Lista import_jobs con métricas agregadas para tratar una entrada concreta
    como unidad operativa visible del MVP.
    """
    query = """
        SELECT
            ij.*,
            COALESCE((
                SELECT COUNT(*)
                FROM cortes_entrada c
                WHERE c.import_job_id = ij.id
            ), 0) AS cortes_count,
            COALESCE((
                SELECT COUNT(*)
                FROM entradas_staging es
                INNER JOIN cortes_entrada c
                    ON c.id = es.corte_id
                WHERE c.import_job_id = ij.id
            ), 0) AS staging_rows_count,
            COALESCE((
                SELECT SUM(COALESCE(es.cantidad, 0))
                FROM entradas_staging es
                INNER JOIN cortes_entrada c
                    ON c.id = es.corte_id
                WHERE c.import_job_id = ij.id
            ), 0) AS staging_piezas_count,
            COALESCE((
                SELECT SUM(CASE WHEN es.estado_revision = 'PENDIENTE' THEN 1 ELSE 0 END)
                FROM entradas_staging es
                INNER JOIN cortes_entrada c
                    ON c.id = es.corte_id
                WHERE c.import_job_id = ij.id
            ), 0) AS pendientes,
            COALESCE((
                SELECT SUM(CASE WHEN es.estado_revision = 'RESUELTA' THEN 1 ELSE 0 END)
                FROM entradas_staging es
                INNER JOIN cortes_entrada c
                    ON c.id = es.corte_id
                WHERE c.import_job_id = ij.id
            ), 0) AS resueltas,
            COALESCE((
                SELECT SUM(CASE WHEN es.estado_revision = 'EN_REVISION' THEN 1 ELSE 0 END)
                FROM entradas_staging es
                INNER JOIN cortes_entrada c
                    ON c.id = es.corte_id
                WHERE c.import_job_id = ij.id
            ), 0) AS en_revision,
            COALESCE((
                SELECT SUM(CASE WHEN es.estado_revision = 'IGNORADA' THEN 1 ELSE 0 END)
                FROM entradas_staging es
                INNER JOIN cortes_entrada c
                    ON c.id = es.corte_id
                WHERE c.import_job_id = ij.id
            ), 0) AS ignoradas,
            COALESCE((
                SELECT COUNT(*)
                FROM entrada_decisiones ed
                INNER JOIN entradas_staging es
                    ON es.id = ed.entrada_staging_id
                INNER JOIN cortes_entrada c
                    ON c.id = es.corte_id
                WHERE c.import_job_id = ij.id
            ), 0) AS decisiones_count
        FROM import_jobs ij
        ORDER BY ij.id DESC
        LIMIT ?
    """
    return fetch_all(query, (limit,))


def get_import_job_operativo(import_job_id: int) -> dict[str, Any] | None:
    rows = list_import_jobs_operativos(limit=1000)
    for row in rows:
        if int(row.get('id') or 0) == int(import_job_id):
            return row
    job = get_import_job_by_id(import_job_id)
    if job is None:
        return None
    counts = count_entradas_staging_by_import_job(import_job_id)
    decisiones_count = len(list_decisiones_by_import_job(import_job_id))
    cortes_count = len(list_cortes_by_import_job(import_job_id))
    return {
        **job,
        'cortes_count': cortes_count,
        'staging_rows_count': int(counts.get('total_filas') or 0),
        'staging_piezas_count': float(counts.get('total_piezas') or 0),
        'pendientes': int(counts.get('pendientes') or 0),
        'resueltas': int(counts.get('resueltas') or 0),
        'en_revision': int(counts.get('en_revision') or 0),
        'ignoradas': int(counts.get('ignoradas') or 0),
        'decisiones_count': decisiones_count,
    }


def list_entradas_staging_by_import_job(
    import_job_id: int,
    estado_revision: str | None = None,
) -> list[dict[str, Any]]:
    query = """
        SELECT
            es.*,
            es.pedido_sugerido_texto AS sugerencia_pedido,
            c.import_job_id,
            c.fecha_corte,
            c.hora_corte_texto,
            c.hora_corte_normalizada,
            c.estado_corte
        FROM entradas_staging es
        INNER JOIN cortes_entrada c
            ON c.id = es.corte_id
        WHERE c.import_job_id = ?
    """
    params: list[Any] = [import_job_id]

    if estado_revision:
        query += " AND es.estado_revision = ?"
        params.append(estado_revision)

    query += " ORDER BY c.fecha_corte ASC, c.hora_corte_normalizada ASC, es.row_num ASC, es.id ASC"
    return fetch_all(query, params)


def list_entradas_staging_problematicas_by_import_job(import_job_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT
            es.*,
            es.pedido_sugerido_texto AS sugerencia_pedido,
            c.import_job_id,
            c.fecha_corte,
            c.hora_corte_texto,
            c.hora_corte_normalizada,
            c.estado_corte
        FROM entradas_staging es
        INNER JOIN cortes_entrada c
            ON c.id = es.corte_id
        WHERE c.import_job_id = ?
          AND (
                es.estado_revision IN ('PENDIENTE', 'EN_REVISION')
                OR es.sku IS NULL
                OR TRIM(COALESCE(es.sku, '')) = ''
                OR es.cantidad IS NULL
                OR es.cantidad <= 0
                OR es.fecha IS NULL
                OR TRIM(COALESCE(es.fecha, '')) = ''
                OR es.hora IS NULL
                OR TRIM(COALESCE(es.hora, '')) = ''
                OR es.tipo_sugerencia IN ('AMBIGUA', 'SIN_DATO')
              )
        ORDER BY c.fecha_corte ASC, c.hora_corte_normalizada ASC, es.row_num ASC, es.id ASC
    """
    return fetch_all(query, (import_job_id,))


def list_entradas_staging_by_import_job_and_sku(import_job_id: int, sku: str) -> list[dict[str, Any]]:
    query = """
        SELECT
            es.*,
            es.pedido_sugerido_texto AS sugerencia_pedido,
            c.import_job_id,
            c.fecha_corte,
            c.hora_corte_texto,
            c.hora_corte_normalizada,
            c.estado_corte
        FROM entradas_staging es
        INNER JOIN cortes_entrada c
            ON c.id = es.corte_id
        WHERE c.import_job_id = ?
          AND es.sku = ?
        ORDER BY c.fecha_corte ASC, c.hora_corte_normalizada ASC, es.row_num ASC, es.id ASC
    """
    return fetch_all(query, (import_job_id, sku))


def count_entradas_staging_by_import_job(import_job_id: int) -> dict[str, Any]:
    query = """
        SELECT
            COUNT(*) AS total_filas,
            COALESCE(SUM(es.cantidad), 0) AS total_piezas,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'PENDIENTE' THEN 1 ELSE 0 END), 0) AS pendientes,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'RESUELTA' THEN 1 ELSE 0 END), 0) AS resueltas,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'EN_REVISION' THEN 1 ELSE 0 END), 0) AS en_revision,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'IGNORADA' THEN 1 ELSE 0 END), 0) AS ignoradas
        FROM entradas_staging es
        INNER JOIN cortes_entrada c
            ON c.id = es.corte_id
        WHERE c.import_job_id = ?
    """
    row = fetch_one(query, (import_job_id,))
    return row or {
        'total_filas': 0,
        'total_piezas': 0,
        'pendientes': 0,
        'resueltas': 0,
        'en_revision': 0,
        'ignoradas': 0,
    }


def summarize_entradas_staging_by_import_job_sku(import_job_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT
            COALESCE(NULLIF(TRIM(es.sku), ''), '(SIN_SKU)') AS sku,
            COALESCE(NULLIF(TRIM(es.descripcion), ''), '(Sin descripción)') AS descripcion,
            COALESCE(NULLIF(TRIM(es.tipo_sugerencia), ''), 'SIN_DATO') AS tipo_sugerencia,
            COALESCE(NULLIF(TRIM(es.pedido_sugerido_texto), ''), NULL) AS pedido_sugerido_texto,
            COUNT(*) AS filas,
            COALESCE(SUM(es.cantidad), 0) AS cantidad_recibida,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'EN_REVISION' THEN 1 ELSE 0 END), 0) AS filas_en_revision,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'PENDIENTE' THEN 1 ELSE 0 END), 0) AS filas_pendientes,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'RESUELTA' THEN 1 ELSE 0 END), 0) AS filas_resueltas,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'IGNORADA' THEN 1 ELSE 0 END), 0) AS filas_ignoradas,
            MIN(es.fecha) AS fecha_min,
            MAX(es.fecha) AS fecha_max,
            MIN(es.hora) AS hora_min,
            MAX(es.hora) AS hora_max
        FROM entradas_staging es
        INNER JOIN cortes_entrada c
            ON c.id = es.corte_id
        WHERE c.import_job_id = ?
        GROUP BY
            COALESCE(NULLIF(TRIM(es.sku), ''), '(SIN_SKU)'),
            COALESCE(NULLIF(TRIM(es.descripcion), ''), '(Sin descripción)'),
            COALESCE(NULLIF(TRIM(es.tipo_sugerencia), ''), 'SIN_DATO'),
            COALESCE(NULLIF(TRIM(es.pedido_sugerido_texto), ''), NULL)
        ORDER BY sku ASC, descripcion ASC
    """
    return fetch_all(query, (import_job_id,))


def list_skus_operativos_by_import_job(import_job_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT
            es.sku,
            COALESCE(NULLIF(TRIM(es.descripcion), ''), '(Sin descripción)') AS descripcion,
            COALESCE(NULLIF(TRIM(es.tipo_sugerencia), ''), 'SIN_DATO') AS tipo_sugerencia,
            COALESCE(NULLIF(TRIM(es.pedido_sugerido_texto), ''), NULL) AS pedido_sugerido_texto,
            COUNT(*) AS filas,
            COALESCE(SUM(es.cantidad), 0) AS cantidad_recibida,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'PENDIENTE' THEN 1 ELSE 0 END), 0) AS filas_pendientes,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'RESUELTA' THEN 1 ELSE 0 END), 0) AS filas_resueltas,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'EN_REVISION' THEN 1 ELSE 0 END), 0) AS filas_en_revision,
            COALESCE(SUM(CASE WHEN es.estado_revision = 'IGNORADA' THEN 1 ELSE 0 END), 0) AS filas_ignoradas
        FROM entradas_staging es
        INNER JOIN cortes_entrada c ON c.id = es.corte_id
        WHERE c.import_job_id = ?
          AND es.sku IS NOT NULL
          AND TRIM(COALESCE(es.sku, '')) <> ''
          AND COALESCE(es.cantidad, 0) > 0
          AND es.estado_revision IN ('PENDIENTE', 'RESUELTA')
        GROUP BY es.sku, COALESCE(NULLIF(TRIM(es.descripcion), ''), '(Sin descripción)'), COALESCE(NULLIF(TRIM(es.tipo_sugerencia), ''), 'SIN_DATO'), COALESCE(NULLIF(TRIM(es.pedido_sugerido_texto), ''), NULL)
        ORDER BY es.sku ASC
    """
    return fetch_all(query, (import_job_id,))


def list_skus_en_revision_by_import_job(import_job_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT
            COALESCE(NULLIF(TRIM(es.sku), ''), '(SIN_SKU)') AS sku,
            COALESCE(NULLIF(TRIM(es.descripcion), ''), '(Sin descripción)') AS descripcion,
            COUNT(*) AS filas,
            COALESCE(SUM(es.cantidad), 0) AS cantidad_recibida,
            GROUP_CONCAT(DISTINCT COALESCE(NULLIF(TRIM(es.tipo_sugerencia), ''), 'SIN_DATO')) AS tipos_sugerencia,
            GROUP_CONCAT(DISTINCT COALESCE(NULLIF(TRIM(es.pedido_sugerido_texto), ''), '(sin sugerido)')) AS pedidos_sugeridos
        FROM entradas_staging es
        INNER JOIN cortes_entrada c ON c.id = es.corte_id
        WHERE c.import_job_id = ?
          AND (
                es.estado_revision = 'EN_REVISION'
                OR es.sku IS NULL
                OR TRIM(COALESCE(es.sku, '')) = ''
                OR es.cantidad IS NULL
                OR es.cantidad <= 0
                OR es.tipo_sugerencia IN ('AMBIGUA', 'SIN_DATO')
              )
        GROUP BY COALESCE(NULLIF(TRIM(es.sku), ''), '(SIN_SKU)'), COALESCE(NULLIF(TRIM(es.descripcion), ''), '(Sin descripción)')
        ORDER BY sku ASC
    """
    return fetch_all(query, (import_job_id,))


def list_decisiones_by_import_job(import_job_id: int) -> list[dict[str, Any]]:
    query = """
        SELECT
            ed.*,
            es.corte_id,
            es.row_num,
            es.sugerencia_destino_texto,
            es.pedido_sugerido_texto,
            c.import_job_id
        FROM entrada_decisiones ed
        INNER JOIN entradas_staging es ON es.id = ed.entrada_staging_id
        INNER JOIN cortes_entrada c ON c.id = es.corte_id
        WHERE c.import_job_id = ?
        ORDER BY es.row_num ASC, ed.id ASC
    """
    return fetch_all(query, (import_job_id,))


def list_decisiones_by_import_job_and_sku(import_job_id: int, sku: str) -> list[dict[str, Any]]:
    query = """
        SELECT
            ed.*,
            es.corte_id,
            es.row_num,
            es.sugerencia_destino_texto,
            es.pedido_sugerido_texto,
            c.import_job_id
        FROM entrada_decisiones ed
        INNER JOIN entradas_staging es ON es.id = ed.entrada_staging_id
        INNER JOIN cortes_entrada c ON c.id = es.corte_id
        WHERE c.import_job_id = ?
          AND ed.sku = ?
        ORDER BY es.row_num ASC, ed.id ASC
    """
    return fetch_all(query, (import_job_id, sku))


def summarize_import_job_final(import_job_id: int) -> dict[str, Any]:
    row = fetch_one(
        """
        SELECT
            COALESCE(SUM(CASE WHEN ed.decision_type IN ('ASIGNAR_PEDIDO', 'ASIGNACION_PEDIDO', 'PEDIDO') THEN ed.cantidad ELSE 0 END), 0) AS total_a_pedidos,
            COALESCE(SUM(CASE WHEN ed.decision_type IN ('STOCK_LIBRE', 'LIBRE') THEN ed.cantidad ELSE 0 END), 0) AS total_a_stock_libre,
            COALESCE(SUM(CASE WHEN ed.decision_type IN ('PENDIENTE', 'DEJAR_PENDIENTE') THEN ed.cantidad ELSE 0 END), 0) AS total_pendiente,
            COALESCE(COUNT(DISTINCT ed.sku), 0) AS sku_con_decision,
            COALESCE(COUNT(*), 0) AS decisiones_count
        FROM entrada_decisiones ed
        INNER JOIN entradas_staging es ON es.id = ed.entrada_staging_id
        INNER JOIN cortes_entrada c ON c.id = es.corte_id
        WHERE c.import_job_id = ?
        """,
        (import_job_id,),
    ) or {}
    counts = count_entradas_staging_by_import_job(import_job_id)
    return {
        'total_sku': len(summarize_entradas_staging_by_import_job_sku(import_job_id)),
        'sku_con_decision': int(row.get('sku_con_decision') or 0),
        'decisiones_count': int(row.get('decisiones_count') or 0),
        'total_a_pedidos': float(row.get('total_a_pedidos') or 0),
        'total_a_stock_libre': float(row.get('total_a_stock_libre') or 0),
        'total_pendiente': float(row.get('total_pendiente') or 0),
        'filas_en_revision': int(counts.get('en_revision') or 0),
        'filas_ignoradas': int(counts.get('ignoradas') or 0),
    }


def delete_entradas_staging_by_import_job(import_job_id: int) -> None:
    query = """
        DELETE FROM entradas_staging
        WHERE corte_id IN (
            SELECT id
            FROM cortes_entrada
            WHERE import_job_id = ?
        )
    """
    execute(query, (import_job_id,))


def get_import_job_full(import_job_id: int) -> dict[str, Any] | None:
    job = get_import_job_operativo(import_job_id)
    if job is None:
        return None

    cortes = list_cortes_by_import_job(import_job_id)
    entradas = list_entradas_staging_by_import_job(import_job_id)
    decisiones = list_decisiones_by_import_job(import_job_id)
    counts = count_entradas_staging_by_import_job(import_job_id)
    summary_by_sku = summarize_entradas_staging_by_import_job_sku(import_job_id)
    resumen_final = summarize_import_job_final(import_job_id)

    return {
        'import_job': job,
        'cortes': cortes,
        'entradas': entradas,
        'decisiones': decisiones,
        'counts': counts,
        'summary_by_sku': summary_by_sku,
        'skus_operativos': list_skus_operativos_by_import_job(import_job_id),
        'skus_en_revision': list_skus_en_revision_by_import_job(import_job_id),
        'resumen_final': resumen_final,
    }
