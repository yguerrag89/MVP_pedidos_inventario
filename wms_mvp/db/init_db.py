from __future__ import annotations

import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from wms_mvp.core.config import BACKUPS_DIR, DB_PATH, ensure_directories


SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


@dataclass(frozen=True)
class SchemaExpectation:
    table: str
    required_columns: tuple[str, ...]


# Este validador debe reflejar EXACTAMENTE el schema.sql vigente.
EXPECTED_SCHEMA: tuple[SchemaExpectation, ...] = (
    SchemaExpectation(
        table="usuarios",
        required_columns=(
            "id",
            "username",
            "role",
            "is_active",
            "created_at",
            "updated_at",
        ),
    ),
    SchemaExpectation(
        table="import_jobs",
        required_columns=(
            "id",
            "import_type",
            "file_name",
            "status",
            "created_by",
            "started_at",
        ),
    ),
    SchemaExpectation(
        table="pedidos",
        required_columns=(
            "id",
            "id_pedido",
            "serie_documento",
            "documento",
            "tipo_registro",
            "origen_registro",
            "import_job_id",
            "pedido_origen_id",
            "cliente",
            "fecha_captura",
            "fecha_produccion",
            "fecha_logistica",
            "estatus_venta",
            "prioridad_manual",
            "porcentaje_asignacion",
            "estado_operativo",
            "created_at",
            "updated_at",
        ),
    ),
    SchemaExpectation(
        table="pedido_lineas",
        required_columns=(
            "id",
            "pedido_id",
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
            "created_at",
            "updated_at",
        ),
    ),
    SchemaExpectation(
        table="pedido_cierres",
        required_columns=(
            "id",
            "pedido_id",
            "tipo_cierre",
            "fecha_envio",
            "usuario",
            "observacion",
            "created_at",
        ),
    ),
    SchemaExpectation(
        table="pedido_linea_cierres",
        required_columns=(
            "id",
            "pedido_cierre_id",
            "pedido_linea_id",
            "sku",
            "descripcion",
            "producto",
            "color",
            "fecha_produccion",
            "cantidad_pedida_original",
            "cantidad_asignada_al_cierre",
            "cantidad_enviada",
            "cantidad_no_enviada",
            "created_at",
        ),
    ),
    SchemaExpectation(
        table="auditoria_cambios",
        required_columns=(
            "id",
            "tabla",
            "registro_id",
            "campo",
            "valor_anterior",
            "valor_nuevo",
            "usuario",
            "motivo",
            "fecha_cambio",
        ),
    ),
    SchemaExpectation(
        table="cortes_entrada",
        required_columns=(
            "id",
            "import_job_id",
            "fecha_corte",
            "hora_corte_texto",
            "hora_corte_normalizada",
            "total_filas",
            "total_piezas",
            "estado_corte",
            "procesado_por",
            "procesado_at",
            "created_at",
        ),
    ),
    SchemaExpectation(
        table="entradas_staging",
        required_columns=(
            "id",
            "corte_id",
            "row_num",
            "sku",
            "descripcion",
            "cantidad",
            "fecha",
            "hora",
            "texto_original_destino",
            "sugerencia_destino_texto",
            "tipo_sugerencia",
            "pedido_sugerido_texto",
            "estado_revision",
            "row_hash",
            "created_at",
            "updated_at",
        ),
    ),
    SchemaExpectation(
        table="entrada_decisiones",
        required_columns=(
            "id",
            "entrada_staging_id",
            "decision_type",
            "pedido_id",
            "pedido_linea_id",
            "sku",
            "cantidad",
            "observacion",
            "created_by",
            "created_at",
        ),
    ),
    SchemaExpectation(
        table="inventario_sku",
        required_columns=(
            "sku",
            "descripcion",
            "total_recibido",
            "stock_libre",
            "stock_asignado",
            "total_enviado",
            "fecha_actualizacion",
            "import_job_id",
            "origen_carga",
            "updated_at",
        ),
    ),
    SchemaExpectation(
        table="inventario_movimientos",
        required_columns=(
            "id",
            "sku",
            "movimiento_tipo",
            "afecta_total",
            "afecta_stock_libre",
            "afecta_stock_asignado",
            "referencia_tipo",
            "referencia_id",
            "observacion",
            "created_by",
            "created_at",
        ),
    ),
    SchemaExpectation(
        table="inventario_cortes",
        required_columns=(
            "id",
            "import_job_id",
            "tipo_corte",
            "archivo_nombre",
            "hoja_nombre",
            "fecha_corte",
            "total_filas",
            "filas_aplicadas",
            "filas_revision",
            "diferencia_total",
            "estado",
            "usuario",
            "observacion",
            "created_at",
        ),
    ),
    SchemaExpectation(
        table="inventario_corte_detalle",
        required_columns=(
            "id",
            "corte_id",
            "row_num",
            "sku",
            "descripcion",
            "stock_sistema",
            "stock_asignado",
            "stock_contado",
            "diferencia",
            "fecha_actualizacion",
            "estado_detalle",
            "motivo_revision",
            "created_at",
        ),
    ),
    SchemaExpectation(
        table="asignaciones",
        required_columns=(
            "id",
            "pedido_id",
            "pedido_linea_id",
            "sku",
            "cantidad",
            "fuente_asignacion",
            "entrada_staging_id",
            "decision_id",
            "estado",
            "observacion",
            "created_by",
            "created_at",
            "updated_at",
        ),
    ),
)


class SchemaCompatibilityError(RuntimeError):
    """Se lanza cuando la base existe pero no cumple el esquema esperado."""


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")


def _read_schema_sql() -> str:
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"No se encontró el archivo de esquema: {SCHEMA_PATH}")
    return SCHEMA_PATH.read_text(encoding="utf-8")


def _backup_db(db_path: Path) -> Path | None:
    if not db_path.exists():
        return None

    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUPS_DIR / f"{db_path.stem}_{timestamp}.db"
    shutil.copy2(db_path, backup_path)
    return backup_path


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1;",
        (table_name,),
    ).fetchone()
    return row is not None


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name});").fetchall()
    return {str(row[1]) for row in rows}


def validate_existing_schema(db_path: Path | None = None) -> tuple[bool, list[str]]:
    """
    Valida si una base existente cumple con las tablas y columnas mínimas esperadas.

    Returns:
        (es_compatible, lista_de_problemas)
    """
    target_db = db_path or DB_PATH
    if not target_db.exists():
        return True, []

    conn = sqlite3.connect(target_db)
    try:
        problems: list[str] = []
        for expected in EXPECTED_SCHEMA:
            if not _table_exists(conn, expected.table):
                problems.append(f"Falta la tabla '{expected.table}'.")
                continue

            existing_columns = _get_table_columns(conn, expected.table)
            missing = [col for col in expected.required_columns if col not in existing_columns]
            if missing:
                problems.append(
                    f"La tabla '{expected.table}' no tiene las columnas requeridas: {', '.join(missing)}."
                )

        return len(problems) == 0, problems
    finally:
        conn.close()


def _create_db_from_schema(db_path: Path) -> None:
    schema_sql = _read_schema_sql()
    conn = sqlite3.connect(db_path)
    try:
        _apply_pragmas(conn)
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


def _apply_light_migrations(db_path: Path) -> None:
    """
    Aplica migraciones ligeras y seguras para bases creadas por iteraciones previas.

    En esta fase solo se agregan columnas opcionales faltantes; nunca se elimina ni
    transforma información existente. También resuelve la compatibilidad de pruebas
    sobre bases temporales recién creadas.
    """
    if not db_path.exists():
        return

    conn = sqlite3.connect(db_path)
    try:
        _apply_pragmas(conn)

        def _column_exists(table_name: str, column_name: str) -> bool:
            rows = conn.execute(f"PRAGMA table_info({table_name});").fetchall()
            return any(str(row[1]) == column_name for row in rows)

        migrations: tuple[tuple[str, str, str], ...] = (
            ("pedidos", "prioridad_manual", "ALTER TABLE pedidos ADD COLUMN prioridad_manual INTEGER"),
            ("pedido_lineas", "prioridad_manual", "ALTER TABLE pedido_lineas ADD COLUMN prioridad_manual INTEGER"),
            ("inventario_sku", "fecha_actualizacion", "ALTER TABLE inventario_sku ADD COLUMN fecha_actualizacion TEXT"),
            ("inventario_sku", "import_job_id", "ALTER TABLE inventario_sku ADD COLUMN import_job_id INTEGER"),
            (
                "inventario_sku",
                "origen_carga",
                "ALTER TABLE inventario_sku ADD COLUMN origen_carga TEXT DEFAULT 'DESCONOCIDO'",
            ),
        )

        for table_name, column_name, sql in migrations:
            if not _column_exists(table_name, column_name):
                conn.execute(sql)

        conn.commit()
    finally:
        conn.close()


def init_db(
    force_reset: bool = False,
    backup_existing: bool = True,
    db_path: Path | None = None,
) -> None:
    """
    Inicializa la base de datos con el esquema actual del MVP.

    Comportamiento:
    - Si no existe base, la crea.
    - Si existe y es compatible, asegura el esquema ejecutándolo de nuevo.
    - Si existe y NO es compatible:
        * con force_reset=False: lanza un error explicando el problema.
        * con force_reset=True: respalda la base anterior y la recrea.
    """
    ensure_directories()
    target_db = db_path or DB_PATH
    target_db.parent.mkdir(parents=True, exist_ok=True)

    if target_db.exists():
        _apply_light_migrations(target_db)

    compatible, problems = validate_existing_schema(target_db)

    if target_db.exists() and not compatible:
        if not force_reset:
            detail = "\n- ".join(problems)
            raise SchemaCompatibilityError(
                "La base actual no es compatible con el esquema esperado del MVP. "
                "Para recrearla, ejecuta init_db(force_reset=True).\n"
                f"Problemas detectados:\n- {detail}"
            )

        backup_path = _backup_db(target_db) if backup_existing else None
        target_db.unlink(missing_ok=True)
        _create_db_from_schema(target_db)

        compatible_after_reset, problems_after_reset = validate_existing_schema(target_db)
        if not compatible_after_reset:
            detail = "\n- ".join(problems_after_reset)
            raise SchemaCompatibilityError(
                "La base fue recreada, pero aún no cumple el esquema esperado.\n"
                f"Problemas detectados:\n- {detail}"
            )

        message = f"Base de datos recreada correctamente en: {target_db}"
        if backup_path is not None:
            message += f"\nRespaldo generado en: {backup_path}"
        print(message)
        return

    _create_db_from_schema(target_db)
    _apply_light_migrations(target_db)

    compatible_after, problems_after = validate_existing_schema(target_db)
    if not compatible_after:
        detail = "\n- ".join(problems_after)
        raise SchemaCompatibilityError(
            "La base se creó, pero no cumple la validación del esquema esperado.\n"
            f"Problemas detectados:\n- {detail}"
        )

    print(f"Base de datos inicializada correctamente en: {target_db}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Inicializa o recrea la base de datos del WMS MVP.")
    parser.add_argument(
        "--force-reset",
        action="store_true",
        help="Recrea la base si detecta incompatibilidad con el esquema actual.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="No generar respaldo antes de recrear una base incompatible.",
    )
    args = parser.parse_args()

    init_db(force_reset=args.force_reset, backup_existing=not args.no_backup)
