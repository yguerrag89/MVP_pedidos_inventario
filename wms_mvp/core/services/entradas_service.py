from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from wms_mvp.core.rules.asignacion_rules import (
    infer_tipo_sugerencia_from_text,
    validate_non_negative,
)
from wms_mvp.core.rules.pedidos_rules import normalize_text, to_float
from wms_mvp.db.connection import transaction
from wms_mvp.db.repositories import asignaciones_repository
from wms_mvp.db.repositories import auditoria_repository
from wms_mvp.db.repositories import entradas_repository
from wms_mvp.db.repositories import partidas_repository
from wms_mvp.db.repositories import pedidos_repository


VALID_IMPORT_STATUS = {
    "PENDIENTE",
    "PROCESANDO",
    "COMPLETADO",
    "COMPLETADO_CON_REVISION",
    "FALLIDO",
    "ANULADO",
}

VALID_IMPORT_TYPES = {
    "CARGA_INICIAL",
    "PXS_LIMPIO",
    "ENTRADAS_PRODUCCION",
}

VALID_CORTE_STATUS = {
    "PENDIENTE",
    "EN_REVISION",
    "PROCESADO",
    "REPROCESADO",
    "ANULADO",
}

VALID_ESTADO_REVISION = {
    "PENDIENTE",
    "RESUELTA",
    "EN_REVISION",
    "IGNORADA",
}


# =========================================================
# Helpers básicos
# =========================================================
def compute_file_hash(file_path: str | Path, algorithm: str = "sha256") -> str:
    """
    Calcula el hash de un archivo.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    hasher = hashlib.new(algorithm)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def normalize_date_value(value: Any) -> str | None:
    """
    Normaliza una fecha a texto YYYY-MM-DD cuando es posible.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    text = normalize_text(value)
    if not text:
        return None

    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%d-%m-%y",
    ):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue

    return text


def normalize_time_value(value: Any) -> str | None:
    """
    Normaliza una hora a HH:MM cuando es posible.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.strftime("%H:%M")

    if isinstance(value, time):
        return value.strftime("%H:%M")

    text = normalize_text(value)
    if not text:
        return None

    for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M:%S %p"):
        try:
            return datetime.strptime(text, fmt).strftime("%H:%M")
        except ValueError:
            continue

    return text


def build_row_hash(row: dict[str, Any]) -> str:
    """
    Construye un hash estable para una fila de entradas staging.
    """
    parts = [
        normalize_text(row.get("sku")) or "",
        normalize_text(row.get("descripcion")) or "",
        str(to_float(row.get("cantidad"), 0.0)),
        normalize_text(row.get("fecha")) or "",
        normalize_text(row.get("hora")) or "",
        normalize_text(row.get("sugerencia_destino_texto")) or "",
    ]
    payload = "||".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_import_type(import_type: Any) -> str:
    normalized = normalize_text(import_type)
    if normalized not in VALID_IMPORT_TYPES:
        raise ValueError(
            f"import_type inválido: {import_type}. Valores permitidos: {sorted(VALID_IMPORT_TYPES)}"
        )
    return normalized


def _normalize_import_status(status: Any) -> str:
    normalized = normalize_text(status)
    if normalized not in VALID_IMPORT_STATUS:
        raise ValueError(
            f"status inválido: {status}. Valores permitidos: {sorted(VALID_IMPORT_STATUS)}"
        )
    return normalized


def _normalize_corte_status(status: Any) -> str:
    normalized = normalize_text(status)
    if normalized not in VALID_CORTE_STATUS:
        raise ValueError(
            f"estado_corte inválido: {status}. Valores permitidos: {sorted(VALID_CORTE_STATUS)}"
        )
    return normalized


def _normalize_pedido_hint(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    normalized = "".join(ch for ch in text.upper() if ch.isalnum())
    return normalized or None


def _safe_iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _find_pedido_from_hint(hint: Any) -> dict[str, Any] | None:
    text = normalize_text(hint)
    if not text:
        return None

    direct = pedidos_repository.get_pedido_by_id_pedido(text)
    if direct is not None:
        return direct

    hint_key = _normalize_pedido_hint(text)
    if not hint_key:
        return None

    for pedido in pedidos_repository.list_pedidos_activos_summary():
        if _normalize_pedido_hint(pedido.get("id_pedido")) == hint_key:
            return pedido
    return None


def _pedido_has_sku(pedido_id: int, sku: Any) -> bool:
    sku_text = normalize_text(sku)
    if not sku_text:
        return False
    for linea in partidas_repository.list_lineas_by_pedido_id(pedido_id):
        if normalize_text(linea.get("sku")) == sku_text:
            return True
    return False


def _find_duplicate_rows(corte_id: int) -> dict[int, int]:
    rows = entradas_repository.list_entradas_staging_by_corte(corte_id)
    counter = Counter(normalize_text(row.get("row_hash")) for row in rows if normalize_text(row.get("row_hash")))
    duplicated_hashes = {row_hash for row_hash, count in counter.items() if count > 1}

    duplicates: dict[int, int] = {}
    if not duplicated_hashes:
        return duplicates

    for row in rows:
        row_hash = normalize_text(row.get("row_hash"))
        if row_hash in duplicated_hashes:
            duplicates[int(row["id"])] = counter[row_hash]
    return duplicates


def _find_historic_duplicate_count(entrada_id: int, row_hash: Any, corte_id: int) -> int:
    row_hash_text = normalize_text(row_hash)
    if not row_hash_text:
        return 0

    with transaction() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM entradas_staging
            WHERE row_hash = ?
              AND corte_id <> ?
              AND id <> ?
            """,
            (row_hash_text, corte_id, entrada_id),
        ).fetchone()
    if not row:
        return 0
    try:
        return int(row.get("total") or 0)
    except AttributeError:
        return int(row[0]) if row else 0


def _collect_review_reasons(
    staging_row: dict[str, Any],
    duplicate_count_same_corte: int = 0,
    duplicate_count_other_cortes: int = 0,
) -> list[str]:
    """
    Motivos que realmente bloquean la operación y obligan a revisión manual.

    Nota operativa del MVP:
    - la sugerencia de producción NO es obligatoria
    - que el pedido sugerido no exista o no contenga el SKU NO bloquea
    - esos casos deben resolverse en la bandeja operativa por SKU, no en revisión
    """
    reasons: list[str] = []

    sku = normalize_text(staging_row.get("sku"))
    descripcion = normalize_text(staging_row.get("descripcion"))
    cantidad = to_float(staging_row.get("cantidad"), 0.0)
    fecha = normalize_text(staging_row.get("fecha"))
    hora = normalize_text(staging_row.get("hora"))

    if not sku:
        reasons.append("Fila sin SKU válido")
    if not descripcion:
        reasons.append("Fila sin descripción")
    if cantidad <= 0:
        reasons.append("Fila sin cantidad válida")
    if not fecha:
        reasons.append("Fila sin fecha válida")
    if not hora:
        reasons.append("Fila sin hora válida")

    if duplicate_count_same_corte > 1:
        reasons.append(f"Posible duplicado dentro del mismo corte ({duplicate_count_same_corte} filas)")
    if duplicate_count_other_cortes > 0:
        reasons.append(f"Posible duplicado respecto a otros cortes ({duplicate_count_other_cortes} coincidencias)")

    return reasons


def _needs_review(staging_row: dict[str, Any]) -> bool:
    duplicate_count_same_corte = int(staging_row.get("duplicate_count_same_corte") or 0)
    duplicate_count_other_cortes = int(staging_row.get("duplicate_count_other_cortes") or 0)
    reasons = _collect_review_reasons(
        staging_row,
        duplicate_count_same_corte=duplicate_count_same_corte,
        duplicate_count_other_cortes=duplicate_count_other_cortes,
    )
    return bool(reasons)


def _enrich_entrada_review_context(row: dict[str, Any], duplicate_map: dict[int, int] | None = None) -> dict[str, Any]:
    enriched = dict(row)
    entrada_id = int(enriched.get("id") or 0)
    corte_id = int(enriched.get("corte_id") or 0)
    duplicate_count_same_corte = 0
    if duplicate_map is not None and entrada_id in duplicate_map:
        duplicate_count_same_corte = duplicate_map[entrada_id]

    duplicate_count_other_cortes = 0
    if entrada_id and corte_id:
        duplicate_count_other_cortes = _find_historic_duplicate_count(
            entrada_id=entrada_id,
            row_hash=enriched.get("row_hash"),
            corte_id=corte_id,
        )

    enriched["duplicate_count_same_corte"] = duplicate_count_same_corte
    enriched["duplicate_count_other_cortes"] = duplicate_count_other_cortes
    enriched["review_reasons"] = _collect_review_reasons(
        enriched,
        duplicate_count_same_corte=duplicate_count_same_corte,
        duplicate_count_other_cortes=duplicate_count_other_cortes,
    )
    return enriched


def _derive_estado_revision(row: dict[str, Any]) -> str:
    current = normalize_text(row.get("estado_revision")) or "PENDIENTE"
    if current == "IGNORADA":
        return "IGNORADA"
    if current == "RESUELTA":
        return "RESUELTA"
    return "EN_REVISION" if _needs_review(row) else "PENDIENTE"


# =========================================================
# Import jobs
# =========================================================
def start_import_job(
    file_name: str,
    file_path: str | Path | None = None,
    import_type: str = "ENTRADAS_PRODUCCION",
    created_by: str | None = None,
    notes: str | None = None,
) -> int:
    """
    Crea un import_job en estado PROCESANDO.
    """
    normalized_file_name = normalize_text(file_name)
    if not normalized_file_name:
        raise ValueError("file_name es obligatorio para crear un import_job.")

    normalized_import_type = _normalize_import_type(import_type)
    normalized_file_path = str(file_path) if file_path else None

    file_hash = None
    if file_path:
        path = Path(file_path)
        if path.exists():
            file_hash = compute_file_hash(path)

    return entradas_repository.create_import_job(
        {
            "import_type": normalized_import_type,
            "file_name": normalized_file_name,
            "file_path": normalized_file_path,
            "file_hash": file_hash,
            "status": "PROCESANDO",
            "notes": normalize_text(notes),
            "created_by": normalize_text(created_by),
        }
    )


def finish_import_job(
    import_job_id: int,
    status: str,
    total_rows: int = 0,
    inserted_rows: int = 0,
    updated_rows: int = 0,
    error_rows: int = 0,
    notes: str | None = None,
) -> dict[str, Any]:
    """
    Cierra un import_job con sus contadores finales.
    """
    normalized_status = _normalize_import_status(status)

    entradas_repository.update_import_job_fields(
        import_job_id,
        {
            "status": normalized_status,
            "total_rows": int(total_rows),
            "inserted_rows": int(inserted_rows),
            "updated_rows": int(updated_rows),
            "error_rows": int(error_rows),
            "notes": normalize_text(notes),
            "finished_at": _safe_iso_now(),
        },
    )

    job = entradas_repository.get_import_job_by_id(import_job_id)
    if job is None:
        raise ValueError(f"No existe el import_job con id {import_job_id}.")
    return job


# =========================================================
# Cortes
# =========================================================
def detect_cortes_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Detecta cortes agrupando por fecha + hora normalizada.
    """
    if not rows:
        return []

    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "fecha_corte": None,
            "hora_corte_texto": None,
            "hora_corte_normalizada": None,
            "total_filas": 0,
            "total_piezas": 0.0,
            "rows": [],
        }
    )

    for index, row in enumerate(rows, start=1):
        fecha_corte = normalize_date_value(row.get("fecha"))
        hora_corte_normalizada = normalize_time_value(row.get("hora"))

        if not fecha_corte or not hora_corte_normalizada:
            continue

        key = (fecha_corte, hora_corte_normalizada)
        bucket = grouped[key]
        bucket["fecha_corte"] = fecha_corte
        bucket["hora_corte_texto"] = normalize_text(row.get("hora")) or hora_corte_normalizada
        bucket["hora_corte_normalizada"] = hora_corte_normalizada
        bucket["total_filas"] += 1
        bucket["total_piezas"] += to_float(row.get("cantidad"), 0.0)
        bucket["rows"].append({**row, "__source_index__": index})

    cortes = list(grouped.values())
    cortes.sort(key=lambda x: (x["fecha_corte"], x["hora_corte_normalizada"]))
    return cortes


def create_or_get_corte(
    import_job_id: int,
    fecha_corte: str,
    hora_corte_normalizada: str,
    hora_corte_texto: str | None = None,
    total_filas: int = 0,
    total_piezas: float = 0.0,
) -> dict[str, Any]:
    """
    Crea un corte si no existe; si ya existe, devuelve el existente.
    Si el corte aún no está procesado, refresca sus metadatos de resumen.
    """
    existing = entradas_repository.get_corte_by_fecha_hora(
        fecha_corte=fecha_corte,
        hora_corte_normalizada=hora_corte_normalizada,
    )
    if existing is not None:
        estado = normalize_text(existing.get("estado_corte"))
        if estado not in {"PROCESADO", "REPROCESADO", "ANULADO"}:
            entradas_repository.update_corte_fields(
                int(existing["id"]),
                {
                    "hora_corte_texto": hora_corte_texto or existing.get("hora_corte_texto"),
                    "total_filas": int(total_filas),
                    "total_piezas": round(float(total_piezas), 6),
                },
            )
            refreshed = entradas_repository.get_corte_by_id(int(existing["id"]))
            if refreshed is not None:
                return refreshed
        return existing

    corte_id = entradas_repository.create_corte_entrada(
        {
            "import_job_id": import_job_id,
            "fecha_corte": fecha_corte,
            "hora_corte_texto": hora_corte_texto,
            "hora_corte_normalizada": hora_corte_normalizada,
            "total_filas": int(total_filas),
            "total_piezas": round(float(total_piezas), 6),
            "estado_corte": "PENDIENTE",
        }
    )
    created = entradas_repository.get_corte_by_id(corte_id)
    if created is None:
        raise ValueError(f"No se pudo recuperar el corte recién creado con id {corte_id}.")
    return created


def register_detected_cortes(import_job_id: int, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Detecta cortes desde filas crudas y los registra en base.
    """
    cortes_detectados = detect_cortes_from_rows(rows)
    registered: list[dict[str, Any]] = []

    for corte in cortes_detectados:
        registered_corte = create_or_get_corte(
            import_job_id=import_job_id,
            fecha_corte=corte["fecha_corte"],
            hora_corte_normalizada=corte["hora_corte_normalizada"],
            hora_corte_texto=corte.get("hora_corte_texto"),
            total_filas=int(corte.get("total_filas", 0)),
            total_piezas=float(corte.get("total_piezas", 0.0)),
        )
        registered.append({**registered_corte, "rows": corte.get("rows", [])})

    return registered


# =========================================================
# Staging
# =========================================================
def build_staging_row(raw_row: dict[str, Any], corte_id: int, row_num: int) -> dict[str, Any]:
    """
    Construye una fila staging normalizada desde una fila cruda del archivo de entradas.
    """
    sku = normalize_text(raw_row.get("sku") or raw_row.get("clave"))
    descripcion = normalize_text(raw_row.get("descripcion") or raw_row.get("nombre_del_producto"))
    cantidad = to_float(raw_row.get("cantidad") or raw_row.get("piezas"), 0.0)
    cajas = (
        to_float(raw_row.get("cajas"), 0.0)
        if raw_row.get("cajas") is not None
        else None
    )
    fecha = normalize_date_value(raw_row.get("fecha"))
    hora = normalize_time_value(raw_row.get("hora"))
    sugerencia_destino_texto = normalize_text(
        raw_row.get("sugerencia_destino_texto")
        or raw_row.get("sugerencia_pedido")
        or raw_row.get("almacen_y_o_pedido")
        or raw_row.get("destino")
    )

    tipo_sugerencia = infer_tipo_sugerencia_from_text(sugerencia_destino_texto)
    pedido_sugerido_texto = sugerencia_destino_texto if tipo_sugerencia == "PEDIDO" else None

    staging_row = {
        "corte_id": int(corte_id),
        "row_num": int(row_num),
        "sku": sku,
        "descripcion": descripcion,
        "cantidad": cantidad,
        "cajas": cajas,
        "fecha": fecha,
        "hora": hora,
        "sugerencia_destino_texto": sugerencia_destino_texto,
        "tipo_sugerencia": tipo_sugerencia,
        "pedido_sugerido_texto": pedido_sugerido_texto,
    }
    staging_row["row_hash"] = build_row_hash(staging_row)
    staging_row["estado_revision"] = _derive_estado_revision(staging_row)
    return staging_row


def load_staging_rows_for_corte(corte_id: int, raw_rows: list[dict[str, Any]]) -> int:
    """
    Carga filas normalizadas a staging para un corte específico.
    Reemplaza staging previo del corte cuando el corte aún no ha sido procesado.
    """
    corte = entradas_repository.get_corte_by_id(corte_id)
    if corte is None:
        raise ValueError(f"No existe el corte con id {corte_id}.")

    estado_corte = normalize_text(corte.get("estado_corte"))
    if estado_corte in {"PROCESADO", "REPROCESADO", "ANULADO"}:
        raise ValueError(
            "No se puede recargar staging en un corte ya procesado/anulado. "
            "Crea o reprocesa el corte de forma controlada."
        )

    if not raw_rows:
        entradas_repository.delete_entradas_staging_by_corte(corte_id)
        entradas_repository.update_corte_fields(
            corte_id,
            {
                "total_filas": 0,
                "total_piezas": 0,
                "estado_corte": "PENDIENTE",
            },
        )
        return 0

    rows_to_insert: list[dict[str, Any]] = []
    total_piezas = 0.0

    for idx, raw_row in enumerate(raw_rows, start=1):
        row_num = raw_row.get("__source_index__", idx)
        staging_row = build_staging_row(raw_row=raw_row, corte_id=corte_id, row_num=int(row_num))
        rows_to_insert.append(staging_row)
        total_piezas += to_float(staging_row.get("cantidad"), 0.0)

    duplicate_counter = Counter(
        normalize_text(row.get("row_hash")) for row in rows_to_insert if normalize_text(row.get("row_hash"))
    )
    for row in rows_to_insert:
        row_hash = normalize_text(row.get("row_hash"))
        row["duplicate_count_same_corte"] = duplicate_counter.get(row_hash, 0)
        row["estado_revision"] = _derive_estado_revision(row)

    with transaction() as conn:
        conn.execute("DELETE FROM entradas_staging WHERE corte_id = ?", (corte_id,))
        conn.executemany(
            """
            INSERT INTO entradas_staging (
                corte_id,
                row_num,
                sku,
                descripcion,
                cantidad,
                cajas,
                fecha,
                hora,
                sugerencia_destino_texto,
                tipo_sugerencia,
                pedido_sugerido_texto,
                estado_revision,
                row_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["corte_id"],
                    row["row_num"],
                    row.get("sku"),
                    row.get("descripcion"),
                    row.get("cantidad", 0),
                    row.get("cajas"),
                    row.get("fecha"),
                    row.get("hora"),
                    row.get("sugerencia_destino_texto"),
                    row.get("tipo_sugerencia", "SIN_DATO"),
                    row.get("pedido_sugerido_texto"),
                    row.get("estado_revision", "PENDIENTE"),
                    row.get("row_hash"),
                )
                for row in rows_to_insert
            ],
        )

    reconcile_corte_estado(corte_id)
    entradas_repository.update_corte_fields(
        corte_id,
        {
            "total_filas": len(rows_to_insert),
            "total_piezas": round(total_piezas, 6),
        },
    )

    return len(rows_to_insert)


# =========================================================
# Resúmenes y revisión
# =========================================================
def get_corte_resumen(corte_id: int) -> dict[str, Any]:
    """
    Devuelve un resumen enriquecido del corte para la interfaz.
    """
    corte = entradas_repository.get_corte_by_id(corte_id)
    if corte is None:
        raise ValueError(f"No existe el corte con id {corte_id}.")

    counts = entradas_repository.count_entradas_staging_by_corte(corte_id)
    entradas_raw = entradas_repository.list_entradas_staging_by_corte(corte_id)
    duplicate_map = _find_duplicate_rows(corte_id)
    entradas = [_enrich_entrada_review_context(row, duplicate_map=duplicate_map) for row in entradas_raw]
    problematicas = [row for row in entradas if row.get("review_reasons") or row.get("estado_revision") in {"PENDIENTE", "EN_REVISION"}]
    decisiones = entradas_repository.list_decisiones_by_corte(corte_id)

    total_decisiones = len(decisiones)
    entradas_resueltas_con_decision = len(
        {
            int(dec["entrada_staging_id"])
            for dec in decisiones
            if dec.get("entrada_staging_id") is not None
        }
    )

    return {
        "corte": corte,
        "counts": counts,
        "entradas": entradas,
        "problematicas": problematicas,
        "decisiones": decisiones,
        "metrics": {
            "total_decisiones": total_decisiones,
            "entradas_resueltas_con_decision": entradas_resueltas_con_decision,
        },
    }


def get_corte_for_review(corte_id: int) -> dict[str, Any] | None:
    """
    Devuelve corte + entradas + decisiones para revisión.
    """
    payload = entradas_repository.get_corte_full(corte_id)
    if payload is None:
        return None

    duplicate_map = _find_duplicate_rows(corte_id)
    payload["entradas"] = [
        _enrich_entrada_review_context(row, duplicate_map=duplicate_map)
        for row in (payload.get("entradas") or [])
    ]

    decisiones_by_entrada: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for decision in payload.get("decisiones") or []:
        entrada_id = int(decision["entrada_staging_id"])
        decisiones_by_entrada[entrada_id].append(decision)

    payload["decisiones_by_entrada"] = decisiones_by_entrada
    return payload


def list_cortes_disponibles(limit: int = 200) -> list[dict[str, Any]]:
    return entradas_repository.list_cortes(limit=limit)


def list_import_jobs(limit: int = 100) -> list[dict[str, Any]]:
    return entradas_repository.list_import_jobs(limit=limit)


# =========================================================
# Flujo simplificado MVP: entrada concreta
# =========================================================
def _get_import_job_or_raise(import_job_id: int) -> dict[str, Any]:
    job = entradas_repository.get_import_job_by_id(import_job_id)
    if job is None:
        raise ValueError(f"No existe el import_job con id {import_job_id}.")
    return job


def _list_staging_rows_by_import_job(import_job_id: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for corte in entradas_repository.list_cortes_by_import_job(import_job_id):
        corte_id = int(corte["id"])
        rows.extend(entradas_repository.list_entradas_staging_by_corte(corte_id))
    return rows


def _aggregate_summary_by_sku(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        sku = normalize_text(row.get("sku")) or "SIN_SKU"
        bucket = grouped.setdefault(
            sku,
            {
                "sku": sku if sku != "SIN_SKU" else None,
                "producto": None,
                "descripcion": None,
                "cantidad_recibida": 0.0,
                "filas": 0,
                "filas_en_revision": 0,
                "filas_resueltas": 0,
                "tipo_sugerencia": None,
                "pedido_sugerido_texto": None,
                "fecha_min": None,
                "fecha_max": None,
                "hora_min": None,
                "hora_max": None,
            },
        )

        bucket["producto"] = bucket.get("producto") or normalize_text(row.get("producto"))
        bucket["descripcion"] = bucket.get("descripcion") or normalize_text(row.get("descripcion"))
        bucket["tipo_sugerencia"] = bucket.get("tipo_sugerencia") or normalize_text(row.get("tipo_sugerencia"))
        bucket["pedido_sugerido_texto"] = bucket.get("pedido_sugerido_texto") or normalize_text(
            row.get("pedido_sugerido_texto") or row.get("sugerencia_destino_texto")
        )
        bucket["cantidad_recibida"] += to_float(row.get("cantidad"), 0.0)
        bucket["filas"] += 1

        estado = normalize_text(row.get("estado_revision")) or "PENDIENTE"
        if estado in {"PENDIENTE", "EN_REVISION"}:
            bucket["filas_en_revision"] += 1
        elif estado == "RESUELTA":
            bucket["filas_resueltas"] += 1

        fecha = normalize_text(row.get("fecha"))
        hora = normalize_text(row.get("hora"))
        if fecha and (bucket["fecha_min"] is None or fecha < bucket["fecha_min"]):
            bucket["fecha_min"] = fecha
        if fecha and (bucket["fecha_max"] is None or fecha > bucket["fecha_max"]):
            bucket["fecha_max"] = fecha
        if hora and (bucket["hora_min"] is None or hora < bucket["hora_min"]):
            bucket["hora_min"] = hora
        if hora and (bucket["hora_max"] is None or hora > bucket["hora_max"]):
            bucket["hora_max"] = hora

    return sorted(grouped.values(), key=lambda item: ((item.get("sku") or "ZZZ"), item.get("descripcion") or ""))


def preview_entrada_concreta_from_file(
    file_path: str | Path,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
) -> dict[str, Any]:
    """
    Punto de entrada del MVP para vista previa de una entrada concreta.
    Usa import lazy para evitar dependencia circular con el loader.
    """
    from wms_mvp.etl.entradas_loader import build_entrada_concreta_preview

    preview = build_entrada_concreta_preview(
        file_path=file_path,
        sheet_name=sheet_name,
        header_row=header_row,
    )
    preview["flow_mode"] = "ENTRADA_CONCRETA"
    preview["ux_principal"] = "preview_recepcion_asignacion"
    return preview


def confirmar_recepcion_entrada_concreta(
    file_path: str | Path,
    sheet_name: str | int | None = None,
    header_row: int | None = None,
    created_by: str | None = None,
    notes: str | None = None,
    save_copy_to_input: bool = True,
    archive_source_copy: bool = False,
) -> dict[str, Any]:
    """
    Registra la recepción física de una entrada concreta del MVP.
    La recepción y la asignación quedan separadas: aquí solo se carga staging.
    """
    from wms_mvp.etl.entradas_loader import import_entrada_concreta_from_file

    result = import_entrada_concreta_from_file(
        file_path=file_path,
        sheet_name=sheet_name,
        header_row=header_row,
        created_by=created_by,
        notes=notes,
        save_copy_to_input=save_copy_to_input,
        archive_source_copy=archive_source_copy,
    )
    import_job_id = int(result["import_job_id"])
    resumen = get_entrada_concreta_resumen(import_job_id)
    return {
        **result,
        "recepcion_confirmada": True,
        "recepcion_separada_de_asignacion": True,
        "resumen_operativo": resumen,
    }


def get_entrada_concreta_resumen(import_job_id: int) -> dict[str, Any]:
    """
    Devuelve una entrada concreta como unidad operativa del MVP.
    Mantiene los cortes como detalle técnico interno, no como eje de navegación.
    """
    job = _get_import_job_or_raise(import_job_id)
    cortes = entradas_repository.list_cortes_by_import_job(import_job_id)
    staging_rows_raw = _list_staging_rows_by_import_job(import_job_id)

    duplicate_map: dict[int, int] = {}
    for corte in cortes:
        duplicate_map.update(_find_duplicate_rows(int(corte["id"])))

    staging_rows = [
        _enrich_entrada_review_context(row, duplicate_map=duplicate_map)
        for row in staging_rows_raw
    ]
    summary_by_sku = _aggregate_summary_by_sku(staging_rows)
    decision_rows: list[dict[str, Any]] = []
    for corte in cortes:
        decision_rows.extend(entradas_repository.list_decisiones_by_corte(int(corte["id"])))

    total_rows = len(staging_rows)
    rows_review = [
        row
        for row in staging_rows
        if row.get("review_reasons")
        and normalize_text(row.get("estado_revision")) in {"PENDIENTE", "EN_REVISION"}
    ]
    rows_ignored = [row for row in staging_rows if normalize_text(row.get("estado_revision")) == "IGNORADA"]
    rows_resolved = [row for row in staging_rows if normalize_text(row.get("estado_revision")) == "RESUELTA"]
    rows_ready = [row for row in staging_rows if not row.get("review_reasons") and normalize_text(row.get("estado_revision")) not in {"IGNORADA"}]

    return {
        "import_job": job,
        "entrada": {
            "import_job_id": int(job["id"]),
            "file_name": job.get("file_name"),
            "file_path": job.get("file_path"),
            "status": job.get("status"),
            "created_at": job.get("created_at"),
            "finished_at": job.get("finished_at"),
            "flow_mode": "ENTRADA_CONCRETA",
            "recepcion_registrada": True,
            "asignacion_aplicada": False,
            "internal_cortes_count": len(cortes),
        },
        "cortes_internos": cortes,
        "summary_by_sku": summary_by_sku,
        "rows": staging_rows,
        "rows_valid": rows_ready,
        "rows_review": rows_review,
        "rows_resolved": rows_resolved,
        "rows_ignored": rows_ignored,
        "decisiones": decision_rows,
        "metrics": {
            "total_rows": total_rows,
            "rows_valid_count": len(rows_ready),
            "rows_review_count": len(rows_review),
            "rows_resolved_count": len(rows_resolved),
            "rows_ignored_count": len(rows_ignored),
            "summary_by_sku_count": len(summary_by_sku),
            "decisiones_count": len(decision_rows),
        },
    }


def get_bandeja_asignacion_entrada_concreta(import_job_id: int) -> dict[str, Any]:
    """
    Devuelve la bandeja operativa del MVP para decidir destinos por SKU.
    La recepción física ya debió haberse registrado previamente.
    """
    from wms_mvp.core.services.asignacion_service import get_demanda_asistida_por_sku

    resumen = get_entrada_concreta_resumen(import_job_id)
    bandeja: list[dict[str, Any]] = []
    for sku_row in resumen["summary_by_sku"]:
        sku = normalize_text(sku_row.get("sku"))
        if not sku:
            continue
        pedido_sugerido_texto = normalize_text(sku_row.get("pedido_sugerido_texto"))
        demanda = get_demanda_asistida_por_sku(sku=sku, pedido_sugerido_texto=pedido_sugerido_texto)
        bandeja.append(
            {
                "sku": sku,
                "producto": sku_row.get("producto"),
                "descripcion": sku_row.get("descripcion"),
                "cantidad_recibida": round(float(sku_row.get("cantidad_recibida") or 0), 6),
                "filas": int(sku_row.get("filas") or 0),
                "filas_en_revision": int(sku_row.get("filas_en_revision") or 0),
                "tipo_sugerencia": sku_row.get("tipo_sugerencia") or "SIN_DATO",
                "pedido_sugerido": pedido_sugerido_texto,
                "demanda_activa": demanda,
                "modos_mvp": [
                    "TODO_A_STOCK_LIBRE",
                    "APLICAR_SUGERENCIA_VALIDA",
                    "ASIGNACION_MANUAL_ASISTIDA",
                    "DEJAR_PENDIENTE",
                ],
                "requiere_decision_usuario": True,
            }
        )

    return {
        "import_job_id": import_job_id,
        "flow_mode": "ENTRADA_CONCRETA",
        "recepcion_confirmada": True,
        "bandeja": bandeja,
        "metrics": {
            "skus_en_bandeja": len(bandeja),
            "rows_review_count": resumen["metrics"]["rows_review_count"],
            "total_rows": resumen["metrics"]["total_rows"],
        },
        "resumen": resumen,
    }


def get_resumen_final_entrada_concreta(import_job_id: int) -> dict[str, Any]:
    """
    Devuelve el resumen final visible del MVP para una entrada concreta.
    Aún cuando internamente existan cortes, la unidad operativa sigue siendo el import_job.
    """
    resumen = get_entrada_concreta_resumen(import_job_id)
    decisiones = resumen.get("decisiones") or []

    total_stock_libre = 0.0
    total_asignado_pedidos = 0.0
    total_pendiente = 0.0

    for decision in decisiones:
        cantidad = to_float(decision.get("cantidad"), 0.0)
        decision_type = normalize_text(decision.get("decision_type"))
        if decision_type == "STOCK_LIBRE":
            total_stock_libre += cantidad
        elif decision_type == "ASIGNACION_PEDIDO":
            total_asignado_pedidos += cantidad
        elif decision_type == "PENDIENTE":
            total_pendiente += cantidad

    return {
        "import_job_id": import_job_id,
        "flow_mode": "ENTRADA_CONCRETA",
        "resumen": resumen,
        "final_metrics": {
            "total_rows": resumen["metrics"]["total_rows"],
            "rows_review_count": resumen["metrics"]["rows_review_count"],
            "rows_resolved_count": resumen["metrics"]["rows_resolved_count"],
            "cantidad_a_stock_libre": round(total_stock_libre, 6),
            "cantidad_asignada_a_pedidos": round(total_asignado_pedidos, 6),
            "cantidad_pendiente": round(total_pendiente, 6),
        },
    }


def list_entradas_concretas_recientes(limit: int = 100) -> list[dict[str, Any]]:
    jobs = [job for job in entradas_repository.list_import_jobs(limit=limit) if normalize_text(job.get("import_type")) == "ENTRADAS_PRODUCCION"]
    result: list[dict[str, Any]] = []
    for job in jobs:
        import_job_id = int(job["id"])
        cortes = entradas_repository.list_cortes_by_import_job(import_job_id)
        result.append(
            {
                "import_job_id": import_job_id,
                "file_name": job.get("file_name"),
                "status": job.get("status"),
                "created_at": job.get("created_at"),
                "finished_at": job.get("finished_at"),
                "internal_cortes_count": len(cortes),
                "flow_mode": "ENTRADA_CONCRETA",
            }
        )
    return result


# =========================================================
# Edición y auditoría
# =========================================================
def _audit_simple_change(
    tabla: str,
    registro_id: int | str,
    campo: str,
    valor_anterior: Any,
    valor_nuevo: Any,
    usuario: str | None = None,
    motivo: str | None = None,
) -> None:
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


def mark_entrada_en_revision(
    entrada_staging_id: int,
    usuario: str | None = None,
    motivo: str | None = None,
) -> dict[str, Any]:
    """
    Marca una entrada staging como EN_REVISION.
    """
    entrada = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if entrada is None:
        raise ValueError(f"No existe la entrada staging con id {entrada_staging_id}.")

    entradas_repository.set_entrada_staging_estado(entrada_staging_id, "EN_REVISION")
    _audit_simple_change(
        tabla="entradas_staging",
        registro_id=entrada_staging_id,
        campo="estado_revision",
        valor_anterior=entrada.get("estado_revision"),
        valor_nuevo="EN_REVISION",
        usuario=usuario,
        motivo=motivo,
    )
    reconcile_corte_estado(int(entrada["corte_id"]))

    updated = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if updated is None:
        raise ValueError(f"No se pudo recuperar la entrada staging {entrada_staging_id}.")
    return updated


def mark_entrada_ignorada(
    entrada_staging_id: int,
    usuario: str | None = None,
    motivo: str | None = None,
) -> dict[str, Any]:
    """
    Marca una entrada staging como IGNORADA.
    """
    entrada = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if entrada is None:
        raise ValueError(f"No existe la entrada staging con id {entrada_staging_id}.")

    entradas_repository.set_entrada_staging_estado(entrada_staging_id, "IGNORADA")
    _audit_simple_change(
        tabla="entradas_staging",
        registro_id=entrada_staging_id,
        campo="estado_revision",
        valor_anterior=entrada.get("estado_revision"),
        valor_nuevo="IGNORADA",
        usuario=usuario,
        motivo=motivo,
    )
    reconcile_corte_estado(int(entrada["corte_id"]))

    updated = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if updated is None:
        raise ValueError(f"No se pudo recuperar la entrada staging {entrada_staging_id}.")
    return updated


CRITICAL_STAGING_FIELDS = {
    "sku",
    "descripcion",
    "cantidad",
    "fecha",
    "hora",
    "sugerencia_destino_texto",
    "tipo_sugerencia",
    "pedido_sugerido_texto",
}


def update_entrada_staging_with_audit(
    entrada_staging_id: int,
    changes: dict[str, Any],
    usuario: str | None = None,
    motivo: str | None = None,
) -> dict[str, Any]:
    """
    Actualiza una fila staging y registra auditoría campo por campo.
    Si cambia información crítica y ya existían decisiones, las elimina para forzar
    una nueva revisión coherente.
    """
    current = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if current is None:
        raise ValueError(f"No existe la entrada staging con id {entrada_staging_id}.")

    if not changes:
        return current

    safe_changes = dict(changes)

    if "cantidad" in safe_changes:
        safe_changes["cantidad"] = validate_non_negative(safe_changes["cantidad"], "cantidad")

    if "cajas" in safe_changes and safe_changes["cajas"] not in (None, ""):
        safe_changes["cajas"] = validate_non_negative(safe_changes["cajas"], "cajas")

    if "fecha" in safe_changes:
        safe_changes["fecha"] = normalize_date_value(safe_changes["fecha"])

    if "hora" in safe_changes:
        safe_changes["hora"] = normalize_time_value(safe_changes["hora"])

    if "sku" in safe_changes:
        safe_changes["sku"] = normalize_text(safe_changes["sku"])

    if "descripcion" in safe_changes:
        safe_changes["descripcion"] = normalize_text(safe_changes["descripcion"])

    if "sugerencia_destino_texto" in safe_changes:
        safe_changes["sugerencia_destino_texto"] = normalize_text(safe_changes["sugerencia_destino_texto"])
        safe_changes["tipo_sugerencia"] = infer_tipo_sugerencia_from_text(
            safe_changes["sugerencia_destino_texto"]
        )
        safe_changes["pedido_sugerido_texto"] = (
            safe_changes["sugerencia_destino_texto"]
            if safe_changes["tipo_sugerencia"] == "PEDIDO"
            else None
        )

    if "estado_revision" in safe_changes:
        estado_revision = normalize_text(safe_changes["estado_revision"])
        if estado_revision not in VALID_ESTADO_REVISION:
            raise ValueError(
                f"estado_revision inválido: {safe_changes['estado_revision']}. "
                f"Valores permitidos: {sorted(VALID_ESTADO_REVISION)}"
            )
        safe_changes["estado_revision"] = estado_revision

    simulated = current.copy()
    simulated.update(safe_changes)
    simulated["row_hash"] = build_row_hash(simulated)
    safe_changes["row_hash"] = simulated["row_hash"]

    decisions_before = entradas_repository.list_decisiones_by_entrada(entrada_staging_id)
    critical_changed = False
    audit_rows: list[dict[str, Any]] = []

    for field_name, new_value in safe_changes.items():
        old_value = current.get(field_name)
        old_text = None if old_value is None else str(old_value)
        new_text = None if new_value is None else str(new_value)
        if old_text == new_text:
            continue

        if field_name in CRITICAL_STAGING_FIELDS:
            critical_changed = True

        audit_rows.append(
            {
                "tabla": "entradas_staging",
                "registro_id": str(entrada_staging_id),
                "campo": field_name,
                "valor_anterior": old_text,
                "valor_nuevo": new_text,
                "usuario": usuario,
                "motivo": motivo,
            }
        )

    if not audit_rows:
        return current

    duplicate_count_other_cortes = _find_historic_duplicate_count(
        entrada_id=entrada_staging_id,
        row_hash=safe_changes["row_hash"],
        corte_id=int(current["corte_id"]),
    )
    simulated["duplicate_count_same_corte"] = 0
    simulated["duplicate_count_other_cortes"] = duplicate_count_other_cortes

    desired_estado = safe_changes.get("estado_revision")
    if desired_estado not in {"IGNORADA", "RESUELTA"}:
        safe_changes["estado_revision"] = _derive_estado_revision(simulated)
        current_estado = normalize_text(current.get("estado_revision"))
        if current_estado != safe_changes["estado_revision"]:
            audit_rows.append(
                {
                    "tabla": "entradas_staging",
                    "registro_id": str(entrada_staging_id),
                    "campo": "estado_revision",
                    "valor_anterior": current_estado,
                    "valor_nuevo": safe_changes["estado_revision"],
                    "usuario": usuario,
                    "motivo": (motivo or "") + " | Recalculado por validación de staging",
                }
            )

    if critical_changed and decisions_before:
        safe_changes["estado_revision"] = "EN_REVISION"
        audit_rows.append(
            {
                "tabla": "entradas_staging",
                "registro_id": str(entrada_staging_id),
                "campo": "estado_revision",
                "valor_anterior": str(current.get("estado_revision")),
                "valor_nuevo": "EN_REVISION",
                "usuario": usuario,
                "motivo": (motivo or "") + " | Se reinicia revisión por cambio crítico",
            }
        )

    with transaction() as conn:
        conn.execute(
            f"""
            UPDATE entradas_staging
            SET {", ".join(f"{field} = ?" for field in safe_changes.keys())},
                updated_at = ?
            WHERE id = ?
            """,
            [*safe_changes.values(), _safe_iso_now(), entrada_staging_id],
        )

        if critical_changed and decisions_before:
            conn.execute(
                "DELETE FROM entrada_decisiones WHERE entrada_staging_id = ?",
                (entrada_staging_id,),
            )

    auditoria_repository.create_many_auditoria_cambios(audit_rows)

    if critical_changed and decisions_before:
        _audit_simple_change(
            tabla="entrada_decisiones",
            registro_id=entrada_staging_id,
            campo="reset_por_edicion",
            valor_anterior=f"{len(decisions_before)} decisiones",
            valor_nuevo="0 decisiones",
            usuario=usuario,
            motivo=motivo or "Se eliminaron decisiones por cambio crítico en staging",
        )

    reconcile_corte_estado(int(current["corte_id"]))

    updated = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if updated is None:
        raise ValueError(f"No se pudo recuperar la entrada staging actualizada {entrada_staging_id}.")
    return updated


# =========================================================
# Estado del corte / procesado
# =========================================================
def can_mark_corte_as_processed(corte_id: int) -> bool:
    """
    Indica si el corte ya no tiene entradas pendientes ni resueltas sin aplicar.
    """
    corte = entradas_repository.get_corte_by_id(corte_id)
    if corte is None:
        raise ValueError(f"No existe el corte con id {corte_id}.")

    entradas = entradas_repository.list_entradas_staging_by_corte(corte_id)
    if not entradas:
        return False

    for entrada in entradas:
        estado = normalize_text(entrada.get("estado_revision"))
        if estado in {"PENDIENTE", "EN_REVISION"}:
            return False

        if estado == "RESUELTA":
            decisiones = entradas_repository.list_decisiones_by_entrada(int(entrada["id"]))
            if not decisiones:
                return False

            for decision in decisiones:
                decision_type = normalize_text(decision.get("decision_type"))
                decision_id = int(decision["id"])
                if decision_type == "STOCK_LIBRE":
                    with transaction() as conn:
                        row = conn.execute(
                            """
                            SELECT 1
                            FROM inventario_movimientos
                            WHERE referencia_tipo = 'ENTRADA_DECISION'
                              AND referencia_id = ?
                            LIMIT 1
                            """,
                            (decision_id,),
                        ).fetchone()
                    if row is None:
                        return False
                elif decision_type == "ASIGNACION_PEDIDO":
                    with transaction() as conn:
                        row = conn.execute(
                            """
                            SELECT 1
                            FROM asignaciones
                            WHERE decision_id = ?
                            LIMIT 1
                            """,
                            (decision_id,),
                        ).fetchone()
                    if row is None:
                        return False

    return True


def mark_corte_as_processed_if_ready(
    corte_id: int,
    usuario: str | None = None,
    reprocesado: bool = False,
) -> bool:
    """
    Marca el corte como PROCESADO/REPROCESADO si ya quedó totalmente aplicado.
    """
    if not can_mark_corte_as_processed(corte_id):
        return False

    entradas_repository.mark_corte_as_processed(
        corte_id=corte_id,
        procesado_por=normalize_text(usuario),
        procesado_at=_safe_iso_now(),
        reprocesado=reprocesado,
    )
    return True


def reconcile_corte_estado(corte_id: int) -> dict[str, Any]:
    """
    Recalcula el estado operativo del corte según sus filas staging.
    No sobrescribe PROCESADO/REPROCESADO/ANULADO salvo que no existan filas.
    """
    corte = entradas_repository.get_corte_by_id(corte_id)
    if corte is None:
        raise ValueError(f"No existe el corte con id {corte_id}.")

    estado_actual = normalize_text(corte.get("estado_corte"))
    counts = entradas_repository.count_entradas_staging_by_corte(corte_id)
    total = int(counts.get("total_filas") or 0)
    pendientes = int(counts.get("pendientes") or 0)
    resueltas = int(counts.get("resueltas") or 0)
    en_revision = int(counts.get("en_revision") or 0)

    if total == 0:
        nuevo_estado = "PENDIENTE"
    elif estado_actual in {"PROCESADO", "REPROCESADO", "ANULADO"}:
        return corte
    elif pendientes > 0 or en_revision > 0 or resueltas > 0:
        nuevo_estado = "EN_REVISION"
    else:
        nuevo_estado = "PENDIENTE"

    if nuevo_estado != estado_actual:
        entradas_repository.update_corte_fields(corte_id, {"estado_corte": nuevo_estado})

    refreshed = entradas_repository.get_corte_by_id(corte_id)
    if refreshed is None:
        raise ValueError(f"No se pudo recuperar el corte {corte_id} tras reconciliar estado.")
    return refreshed


# =========================================================
# Dashboard operativo por SKU (rediseño fase 5)
# =========================================================
def _group_rows_by_sku_for_dashboard(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows or []:
        sku = normalize_text(row.get("sku"))
        if not sku:
            continue
        grouped[sku].append(row)
    return grouped


def _build_estado_sku(*, has_review: bool, has_decision: bool, has_ready_rows: bool) -> str:
    if has_decision:
        return "DECIDIDO"
    if has_review and not has_ready_rows:
        return "EN_REVISION"
    if has_review and has_ready_rows:
        return "PROPUESTA_LISTA"
    if has_ready_rows:
        return "PROPUESTA_LISTA"
    return "SIN_DECIDIR"


def _build_sku_dashboard_item(import_job_id: int, sku_summary: dict[str, Any], sku_rows: list[dict[str, Any]], decisiones: list[dict[str, Any]]) -> dict[str, Any]:
    from wms_mvp.core.services.asignacion_service import (
        build_propuesta_automatica_para_sku,
        get_pedidos_candidatos_para_sku,
    )

    sku = normalize_text(sku_summary.get("sku"))
    pedido_sugerido_texto = normalize_text(sku_summary.get("pedido_sugerido_texto"))
    candidatos = get_pedidos_candidatos_para_sku(sku=sku, pedido_sugerido_texto=pedido_sugerido_texto)
    propuesta = build_propuesta_automatica_para_sku(
        sku=sku,
        cantidad_recibida=to_float(sku_summary.get("cantidad_recibida"), 0.0),
        pedido_sugerido_texto=pedido_sugerido_texto,
        demanda_rows=candidatos,
    )

    review_rows = [row for row in sku_rows if row.get("review_reasons")]
    ready_rows = [row for row in sku_rows if not row.get("review_reasons") and normalize_text(row.get("estado_revision")) != "IGNORADA"]
    sku_decisiones = [d for d in decisiones if normalize_text(d.get("sku")) == sku]

    return {
        "sku": sku,
        "producto": sku_summary.get("producto"),
        "descripcion": sku_summary.get("descripcion"),
        "cantidad_recibida": round(to_float(sku_summary.get("cantidad_recibida"), 0.0), 6),
        "pedido_sugerido": pedido_sugerido_texto,
        "tipo_sugerencia": sku_summary.get("tipo_sugerencia") or "SIN_DATO",
        "stock_libre_actual": round(to_float(asignaciones_repository.get_stock_libre_by_sku(sku), 0.0), 6),
        "demanda_activa_total": round(to_float(propuesta.get("demanda_total"), 0.0), 6),
        "pedidos_candidatos": candidatos,
        "pedidos_candidatos_count": len(candidatos),
        "propuesta": propuesta,
        "rows": sku_rows,
        "rows_ready": ready_rows,
        "rows_review": review_rows,
        "rows_count": len(sku_rows),
        "rows_ready_count": len(ready_rows),
        "rows_review_count": len(review_rows),
        "decisiones": sku_decisiones,
        "decision_count": len(sku_decisiones),
        "estado_sku": _build_estado_sku(
            has_review=bool(review_rows),
            has_decision=bool(sku_decisiones),
            has_ready_rows=bool(ready_rows),
        ),
        "acciones_disponibles": [
            "ACEPTAR_PROPUESTA",
            "REPARTIR_MANUALMENTE",
            "TODO_A_STOCK_LIBRE",
            "DEJAR_PENDIENTE",
        ],
    }


def get_bandeja_sku_listos(import_job_id: int) -> list[dict[str, Any]]:
    resumen = get_entrada_concreta_resumen(import_job_id)
    grouped_rows = _group_rows_by_sku_for_dashboard(resumen.get("rows") or [])
    decisiones = resumen.get("decisiones") or []
    items: list[dict[str, Any]] = []
    for sku_summary in resumen.get("summary_by_sku") or []:
        sku = normalize_text(sku_summary.get("sku"))
        if not sku:
            continue
        sku_rows = grouped_rows.get(sku, [])
        item = _build_sku_dashboard_item(import_job_id, sku_summary, sku_rows, decisiones)
        if item["rows_ready_count"] > 0:
            items.append(item)
    return sorted(items, key=lambda x: (x.get("estado_sku") != "PROPUESTA_LISTA", x.get("sku") or ""))


def get_bandeja_sku_revision(import_job_id: int) -> list[dict[str, Any]]:
    resumen = get_entrada_concreta_resumen(import_job_id)
    grouped_rows = _group_rows_by_sku_for_dashboard(resumen.get("rows") or [])
    decisiones = resumen.get("decisiones") or []
    items: list[dict[str, Any]] = []
    for sku_summary in resumen.get("summary_by_sku") or []:
        sku = normalize_text(sku_summary.get("sku"))
        if not sku:
            continue
        sku_rows = grouped_rows.get(sku, [])
        item = _build_sku_dashboard_item(import_job_id, sku_summary, sku_rows, decisiones)
        if item["rows_review_count"] > 0:
            item["motivos_revision"] = sorted({reason for row in item["rows_review"] for reason in (row.get("review_reasons") or []) if reason})
            items.append(item)
    return sorted(items, key=lambda x: (x.get("sku") or "", x.get("descripcion") or ""))


def get_resumen_final_entrada(import_job_id: int) -> dict[str, Any]:
    resumen = get_entrada_concreta_resumen(import_job_id)
    sku_listos = get_bandeja_sku_listos(import_job_id)
    sku_revision = get_bandeja_sku_revision(import_job_id)
    decisiones = resumen.get("decisiones") or []

    total_pedidos = 0.0
    total_stock = 0.0
    total_pendiente = 0.0
    for d in decisiones:
        qty = to_float(d.get("cantidad"), 0.0)
        decision_type = normalize_text(d.get("decision_type"))
        if decision_type == "ASIGNACION_PEDIDO":
            total_pedidos += qty
        elif decision_type == "STOCK_LIBRE":
            total_stock += qty
        elif decision_type == "PENDIENTE":
            total_pendiente += qty

    sku_decididos = sum(1 for item in sku_listos if item.get("decision_count"))
    sku_pendientes = sum(1 for item in sku_listos if not item.get("decision_count"))

    return {
        "import_job_id": import_job_id,
        "sku_totales": len(resumen.get("summary_by_sku") or []),
        "sku_listos_count": len(sku_listos),
        "sku_revision_count": len(sku_revision),
        "sku_decididos_count": sku_decididos,
        "sku_pendientes_count": sku_pendientes,
        "total_asignado_pedidos": round(total_pedidos, 6),
        "total_stock_libre": round(total_stock, 6),
        "total_pendiente": round(total_pendiente, 6),
        "rows_review_count": resumen.get("metrics", {}).get("rows_review_count", 0),
        "rows_total": resumen.get("metrics", {}).get("total_rows", 0),
        "detalle_sku": [
            {
                "sku": item.get("sku"),
                "descripcion": item.get("descripcion"),
                "estado_sku": item.get("estado_sku"),
                "decision_count": item.get("decision_count"),
                "cantidad_recibida": item.get("cantidad_recibida"),
                "stock_libre_propuesto": to_float((item.get("propuesta") or {}).get("cantidad_stock_libre"), 0.0),
            }
            for item in sku_listos
        ],
    }


def get_entrada_concreta_dashboard(import_job_id: int) -> dict[str, Any]:
    resumen = get_entrada_concreta_resumen(import_job_id)
    sku_listos = get_bandeja_sku_listos(import_job_id)
    sku_revision = get_bandeja_sku_revision(import_job_id)
    resumen_final = get_resumen_final_entrada(import_job_id)
    return {
        "entrada": resumen.get("entrada"),
        "import_job": resumen.get("import_job"),
        "metrics": resumen.get("metrics"),
        "summary_by_sku": resumen.get("summary_by_sku"),
        "sku_listos": sku_listos,
        "sku_revision": sku_revision,
        "resumen_final": resumen_final,
        "rows_review": resumen.get("rows_review"),
        "rows_ignored": resumen.get("rows_ignored"),
        "rows_resolved": resumen.get("rows_resolved"),
        "flow_mode": "ENTRADA_CONCRETA",
    }
