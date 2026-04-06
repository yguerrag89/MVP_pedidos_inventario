from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping

from wms_mvp.core.rules.pedidos_rules import (
    aggregate_lineas_metrics,
    build_linea_from_import_row,
    build_pedido_from_import_row,
    enrich_linea_with_pedido_context,
    normalize_pause_fields,
    normalize_text,
    require_valid_date,
    validate_linea_quantities,
    validate_pedido_header,
)
from wms_mvp.db.connection import column_exists, transaction
from wms_mvp.db.repositories import auditoria_repository
from wms_mvp.db.repositories import partidas_repository
from wms_mvp.db.repositories import pedidos_repository


PEDIDO_EDITABLE_FIELDS = {
    "fecha_produccion",
    "fecha_logistica",
    "estatus_venta",
    "motivo_pausa",
    "prioridad_manual",
}

LINEA_EDITABLE_FIELDS = {
    "sku",
    "descripcion",
    "producto",
    "color",
    "cantidad_pedida",
    "cantidad_asignada",
    "cantidad_enviada",
    "fecha_captura",
    "fecha_produccion",
    "fecha_logistica",
    "estatus_venta",
    "observaciones",
}

PEDIDO_CONTEXT_FIELDS = {
    "fecha_produccion",
    "fecha_logistica",
    "estatus_venta",
    "motivo_pausa",
    "prioridad_manual",
}

NEW_PEDIDO_ORIGIN_KEYS = {
    "PXS LIMPIO",
    "PXS_LIMPIO",
    "PXS",
    "PXS NUEVO",
    "PXS_NUEVO",
}

NEW_PEDIDO_TIPO_KEYS = {
    "PEDIDO NORMAL",
    "PEDIDO_NORMAL",
    "PEDIDO NUEVO",
    "PEDIDO_NUEVO",
}


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().replace(",", "")
    if not text:
        return default

    try:
        return float(text)
    except (TypeError, ValueError):
        return default



def _to_iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return require_valid_date(value, "fecha")



def _normalize_filter_dates(filters: Mapping[str, Any] | None) -> dict[str, Any]:
    normalized = dict(filters or {})
    for field in (
        "fecha_produccion",
        "fecha_produccion_desde",
        "fecha_produccion_hasta",
        "fecha_captura_desde",
        "fecha_captura_hasta",
        "fecha_logistica_desde",
        "fecha_logistica_hasta",
    ):
        if field in normalized:
            raw_value = normalized.get(field)
            if isinstance(raw_value, (list, tuple, set)):
                normalized_dates: list[str] = []
                for item in raw_value:
                    parsed = _to_iso_date(item)
                    if parsed is not None:
                        normalized_dates.append(parsed)
                normalized[field] = normalized_dates
            else:
                normalized[field] = _to_iso_date(raw_value)
    return normalized



def _normalize_filter_contract(filters: Mapping[str, Any] | None) -> dict[str, Any]:
    normalized = _normalize_filter_dates(filters)

    porcentaje_min = normalized.pop("pct_min", None)
    porcentaje_max = normalized.pop("pct_max", None)

    if normalized.get("porcentaje_min") in (None, "") and porcentaje_min not in (None, ""):
        normalized["porcentaje_min"] = _to_float(porcentaje_min)
    elif normalized.get("porcentaje_min") not in (None, ""):
        normalized["porcentaje_min"] = _to_float(normalized.get("porcentaje_min"))

    if normalized.get("porcentaje_max") in (None, "") and porcentaje_max not in (None, ""):
        normalized["porcentaje_max"] = _to_float(porcentaje_max)
    elif normalized.get("porcentaje_max") not in (None, ""):
        normalized["porcentaje_max"] = _to_float(normalized.get("porcentaje_max"))

    cleaned: dict[str, Any] = {}
    for key, value in normalized.items():
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                cleaned[key] = stripped
            continue
        if isinstance(value, (list, tuple, set)):
            cleaned_list = []
            for item in value:
                if item is None:
                    continue
                if isinstance(item, str):
                    stripped_item = item.strip()
                    if stripped_item:
                        cleaned_list.append(stripped_item)
                else:
                    cleaned_list.append(item)
            if cleaned_list:
                cleaned[key] = cleaned_list
            continue
        cleaned[key] = value

    return cleaned



def _pedidos_supports_motivo_pausa() -> bool:
    return column_exists("pedidos", "motivo_pausa")



def _pedidos_supports_prioridad_manual() -> bool:
    return column_exists("pedidos", "prioridad_manual")



def _lineas_supports_motivo_pausa() -> bool:
    return column_exists("pedido_lineas", "motivo_pausa")



def _persistable_pedido_fields(changes: Mapping[str, Any]) -> dict[str, Any]:
    persistable = dict(changes)
    if not _pedidos_supports_motivo_pausa():
        persistable.pop("motivo_pausa", None)
    if not _pedidos_supports_prioridad_manual():
        persistable.pop("prioridad_manual", None)
    return persistable



def _normalize_pedido_changes(changes: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    invalid_fields: list[str] = []

    for field, value in changes.items():
        if field not in PEDIDO_EDITABLE_FIELDS:
            invalid_fields.append(field)
            continue

        if field in {"fecha_produccion", "fecha_logistica"}:
            normalized[field] = _to_iso_date(value)
        elif field in {"estatus_venta", "motivo_pausa"}:
            normalized[field] = normalize_text(value)
        elif field == "prioridad_manual":
            normalized[field] = int(_to_float(value, 0)) if value not in (None, "") else None
        else:
            normalized[field] = value

    if invalid_fields:
        allowed = ", ".join(sorted(PEDIDO_EDITABLE_FIELDS))
        invalid = ", ".join(sorted(invalid_fields))
        raise ValueError(
            "En esta fase del MVP solo se permite editar: "
            f"{allowed}. Campos rechazados: {invalid}."
        )

    estatus_venta, motivo_pausa = normalize_pause_fields(
        normalized.get("estatus_venta"),
        normalized.get("motivo_pausa"),
    )
    if "estatus_venta" in normalized:
        normalized["estatus_venta"] = estatus_venta
    if "motivo_pausa" in normalized or estatus_venta == "Pausa":
        normalized["motivo_pausa"] = motivo_pausa

    return normalized



def _normalize_linea_changes(changes: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field, value in changes.items():
        if field not in LINEA_EDITABLE_FIELDS:
            continue

        if field in {"cantidad_pedida", "cantidad_asignada", "cantidad_enviada"}:
            normalized[field] = _to_float(value)
        elif field in {"fecha_captura", "fecha_produccion", "fecha_logistica"}:
            normalized[field] = _to_iso_date(value)
        else:
            normalized[field] = normalize_text(value)
    return normalized



def _normalize_key_token(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    token = text.upper().replace("-", " ").replace("_", " ")
    return " ".join(token.split())


def _is_new_pedido_source(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False

    origen = _normalize_key_token(payload.get("origen_registro"))
    tipo = _normalize_key_token(payload.get("tipo_registro"))

    origen_match = origen in {_normalize_key_token(v) for v in NEW_PEDIDO_ORIGIN_KEYS}
    tipo_match = tipo in {_normalize_key_token(v) for v in NEW_PEDIDO_TIPO_KEYS}

    return bool(origen_match or tipo_match)


def _force_manual_assignment_for_new_linea(linea_payload: Mapping[str, Any]) -> dict[str, Any]:
    sanitized = dict(linea_payload)
    cantidad_pedida = _to_float(sanitized.get("cantidad_pedida"), 0.0)
    sanitized["cantidad_asignada"] = 0.0
    sanitized["cantidad_enviada"] = 0.0
    sanitized["cantidad_faltante"] = round(max(0.0, cantidad_pedida), 6)
    return sanitized


def _build_manual_assignment_note(enabled: bool) -> str | None:
    if not enabled:
        return None
    return (
        "Pedido nuevo creado sin autoasignación. "
        "La oferta por SKU es informativa y requiere decisión manual del usuario."
    )



def _build_audit_rows_for_changes(
    tabla: str,
    registro_id: str | int,
    old_data: Mapping[str, Any],
    new_data: Mapping[str, Any],
    usuario: str | None = None,
    motivo: str | None = None,
) -> list[dict[str, Any]]:
    audit_rows: list[dict[str, Any]] = []

    for field_name, new_value in new_data.items():
        old_value = old_data.get(field_name)
        old_text = None if old_value is None else str(old_value)
        new_text = None if new_value is None else str(new_value)

        if old_text == new_text:
            continue

        audit_rows.append(
            {
                "tabla": tabla,
                "registro_id": str(registro_id),
                "campo": field_name,
                "valor_anterior": old_text,
                "valor_nuevo": new_text,
                "usuario": usuario,
                "motivo": motivo,
            }
        )

    return audit_rows



def _sync_lineas_context_from_pedido_force(pedido_id: int, changes: Mapping[str, Any]) -> None:
    fields_to_set: list[str] = []
    params: list[Any] = []

    sync_fields = ["fecha_produccion", "fecha_logistica", "estatus_venta"]
    if _lineas_supports_motivo_pausa():
        sync_fields.append("motivo_pausa")

    for field in sync_fields:
        if field in changes:
            fields_to_set.append(f"{field} = ?")
            params.append(changes.get(field))

    if not fields_to_set:
        return

    fields_to_set.append("updated_at = CURRENT_TIMESTAMP")
    params.append(pedido_id)

    query = f"""
        UPDATE pedido_lineas
        SET {', '.join(fields_to_set)}
        WHERE pedido_id = ?
    """

    with transaction() as conn:
        conn.execute(query, params)


# -----------------------------------------------------------------------------
# Consultas para UI
# -----------------------------------------------------------------------------
def get_pedidos_activos_summary() -> list[dict[str, Any]]:
    """
    Devuelve la tabla resumen de pedidos activos.
    Esta vista no debe ser afectada por filtros.
    """
    return pedidos_repository.list_pedidos_activos_summary()



def get_pedidos_enviados_summary() -> list[dict[str, Any]]:
    """
    Devuelve la tabla resumen de pedidos enviados.
    """
    return pedidos_repository.list_pedidos_enviados_summary()



def get_pedidos_activos_detalle(filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """
    Devuelve la tabla detalle de pedidos activos con filtros operativos.
    El filtro principal es fecha_produccion.
    """
    return pedidos_repository.list_pedidos_detalle_filtered(_normalize_filter_contract(filters))



def get_pedidos_chart_data(filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """
    Devuelve los datos para el gráfico de porcentaje de asignación.
    """
    return pedidos_repository.list_pedidos_for_chart(_normalize_filter_contract(filters))



def get_filter_options() -> dict[str, list[str]]:
    """
    Devuelve opciones útiles para construir filtros en la interfaz.
    """
    return {
        "clientes": pedidos_repository.list_distinct_clientes_activos(),
        "estatus_venta": pedidos_repository.list_distinct_estatus_venta_activos(),
        "fechas_produccion": pedidos_repository.list_distinct_fechas_produccion_activas(),
        "productos": partidas_repository.list_distinct_productos_activos(),
    }



def get_pedido_full_by_id(pedido_id: int) -> dict[str, Any] | None:
    """
    Devuelve un pedido y sus líneas por id interno.
    """
    return pedidos_repository.get_pedido_full(pedido_id)



def get_pedido_full_by_id_pedido(id_pedido: str) -> dict[str, Any] | None:
    """
    Devuelve un pedido y sus líneas por id_pedido de negocio.
    """
    pedido = pedidos_repository.get_pedido_by_id_pedido(id_pedido)
    if pedido is None:
        return None
    return pedidos_repository.get_pedido_full(int(pedido["id"]))



def pedido_exists(id_pedido: str) -> bool:
    """
    Indica si ya existe un pedido con ese id_pedido.
    """
    normalized = normalize_text(id_pedido)
    if not normalized:
        return False
    return pedidos_repository.pedido_exists(normalized)


# -----------------------------------------------------------------------------
# Creación / recálculo
# -----------------------------------------------------------------------------
def create_pedido_with_lineas(
    pedido_data: dict[str, Any],
    lineas_data: list[dict[str, Any]],
    usuario: str | None = None,
    motivo_auditoria: str | None = None,
    force_manual_assignment: bool | None = None,
) -> int:
    """
    Crea un pedido con todas sus líneas.

    Ajustes clave respecto a la versión previa:
    - incorpora fecha_produccion a nivel pedido y línea
    - propaga contexto operativo del pedido hacia líneas cuando falte
    - usa el catálogo comercial canónico del MVP
    - valida Pausa + motivo antes de persistir
    - persiste motivo_pausa solo si el esquema actual lo soporta
    - para pedidos nuevos tipo PXS limpio fuerza asignación manual por defecto

    Regla de fase 3:
    crear un pedido nuevo no debe reservar stock libre ni generar asignaciones
    automáticas. La oferta por SKU es únicamente asistida.
    """
    if not lineas_data:
        raise ValueError("No se puede crear un pedido sin líneas.")

    pedido_payload = build_pedido_from_import_row(pedido_data)

    manual_assignment_mode = (
        _is_new_pedido_source(pedido_payload)
        if force_manual_assignment is None
        else bool(force_manual_assignment)
    )
    manual_assignment_note = _build_manual_assignment_note(manual_assignment_mode)

    validate_pedido_header(
        id_pedido=pedido_payload["id_pedido"],
        cliente=pedido_payload["cliente"],
        estado_operativo=pedido_payload["estado_operativo"],
        estatus_venta=pedido_payload.get("estatus_venta"),
        fecha_captura=pedido_payload.get("fecha_captura"),
        fecha_produccion=pedido_payload.get("fecha_produccion"),
        fecha_logistica=pedido_payload.get("fecha_logistica"),
        motivo_pausa=pedido_payload.get("motivo_pausa"),
    )

    if pedidos_repository.pedido_exists(pedido_payload["id_pedido"]):
        raise ValueError(f"Ya existe un pedido con id_pedido '{pedido_payload['id_pedido']}'.")

    normalized_lineas: list[dict[str, Any]] = []
    for index, raw_linea in enumerate(lineas_data, start=1):
        linea_payload = build_linea_from_import_row(raw_linea, pedido_id=-1, line_no=index)
        linea_payload = enrich_linea_with_pedido_context(linea_payload, pedido_payload)
        if manual_assignment_mode:
            linea_payload = _force_manual_assignment_for_new_linea(linea_payload)
        validate_linea_quantities(
            cantidad_pedida=linea_payload["cantidad_pedida"],
            cantidad_asignada=linea_payload["cantidad_asignada"],
            allow_overassignment=False,
        )
        normalized_lineas.append(linea_payload)

    metrics = aggregate_lineas_metrics(normalized_lineas)
    pedido_payload["porcentaje_asignacion"] = metrics["porcentaje_asignacion"]

    if manual_assignment_note:
        existing_obs = normalize_text(pedido_payload.get("observaciones"))
        pedido_payload["observaciones"] = (
            f"{existing_obs} | {manual_assignment_note}" if existing_obs else manual_assignment_note
        )
        for linea in normalized_lineas:
            linea_obs = normalize_text(linea.get("observaciones"))
            linea["observaciones"] = (
                f"{linea_obs} | {manual_assignment_note}" if linea_obs else manual_assignment_note
            )

    pedido_columns = [
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
        "porcentaje_asignacion",
        "estado_operativo",
        "observaciones",
    ]
    if _pedidos_supports_motivo_pausa():
        pedido_columns.insert(12, "motivo_pausa")

    lineas_columns = [
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
        "observaciones",
    ]
    if _lineas_supports_motivo_pausa():
        lineas_columns.insert(14, "motivo_pausa")

    with transaction() as conn:
        pedido_values = [pedido_payload.get(column) for column in pedido_columns]
        pedido_placeholders = ", ".join("?" for _ in pedido_columns)
        pedido_sql = f"""
            INSERT INTO pedidos (
                {', '.join(pedido_columns)}
            )
            VALUES ({pedido_placeholders})
        """
        cursor = conn.execute(pedido_sql, pedido_values)
        pedido_id = int(cursor.lastrowid)

        for line_no, linea in enumerate(normalized_lineas, start=1):
            linea_row = dict(linea)
            linea_row["pedido_id"] = pedido_id
            linea_row["line_no"] = line_no
            linea_values = [linea_row.get(column) for column in lineas_columns]
            linea_placeholders = ", ".join("?" for _ in lineas_columns)
            linea_sql = f"""
                INSERT INTO pedido_lineas (
                    {', '.join(lineas_columns)}
                )
                VALUES ({linea_placeholders})
            """
            conn.execute(linea_sql, linea_values)

    auditoria_repository.create_auditoria_cambio(
        {
            "tabla": "pedidos",
            "registro_id": str(pedido_id),
            "campo": "__created__",
            "valor_anterior": None,
            "valor_nuevo": pedido_payload["id_pedido"],
            "usuario": usuario,
            "motivo": motivo_auditoria,
        }
    )

    return pedido_id



def create_new_pedido_without_autoassignment(
    pedido_data: dict[str, Any],
    lineas_data: list[dict[str, Any]],
    usuario: str | None = None,
    motivo_auditoria: str | None = None,
) -> int:
    """
    Wrapper explícito para pedidos nuevos.

    Se usa cuando se quiere dejar claro que la creación del pedido no debe
    interpretar stock disponible como reserva ni generar asignaciones automáticas.
    """
    return create_pedido_with_lineas(
        pedido_data=pedido_data,
        lineas_data=lineas_data,
        usuario=usuario,
        motivo_auditoria=motivo_auditoria,
        force_manual_assignment=True,
    )



def recalculate_pedido(pedido_id: int) -> dict[str, Any]:
    """
    Recalcula faltantes por línea y porcentaje de asignación del pedido.
    Devuelve el pedido actualizado con sus líneas.
    """
    partidas_repository.recalculate_all_lineas_faltante_by_pedido(pedido_id)
    pedidos_repository.recalculate_pedido_metrics(pedido_id)

    pedido_full = pedidos_repository.get_pedido_full(pedido_id)
    if pedido_full is None:
        raise ValueError(f"No existe el pedido con id {pedido_id}.")

    return pedido_full


# -----------------------------------------------------------------------------
# Edición controlada con auditoría
# -----------------------------------------------------------------------------
def update_pedido_with_audit(
    pedido_id: int,
    changes: dict[str, Any],
    usuario: str | None = None,
    motivo: str | None = None,
) -> dict[str, Any]:
    """
    Actualiza campos permitidos del pedido y registra auditoría por cada cambio real.

    Reglas de esta fase:
    - el servicio canoniza estatus_venta y motivo_pausa
    - Pausa requiere motivo
    - motivo_pausa solo se persiste si el esquema actual lo soporta
    """
    current = pedidos_repository.get_pedido_by_id(pedido_id)
    if current is None:
        raise ValueError(f"No existe el pedido con id {pedido_id}.")

    if not changes:
        return current

    normalized_changes = _normalize_pedido_changes(changes)
    if not normalized_changes:
        return current

    current_motivo = current.get("motivo_pausa") if isinstance(current, Mapping) else None
    simulated = dict(current)
    simulated.update(normalized_changes)

    simulated_estatus, simulated_motivo = normalize_pause_fields(
        simulated.get("estatus_venta"),
        normalized_changes.get("motivo_pausa", current_motivo),
    )
    simulated["estatus_venta"] = simulated_estatus
    simulated["motivo_pausa"] = simulated_motivo

    validate_pedido_header(
        id_pedido=simulated.get("id_pedido"),
        cliente=simulated.get("cliente"),
        estado_operativo=simulated.get("estado_operativo"),
        estatus_venta=simulated.get("estatus_venta"),
        fecha_captura=simulated.get("fecha_captura"),
        fecha_produccion=simulated.get("fecha_produccion"),
        fecha_logistica=simulated.get("fecha_logistica"),
        motivo_pausa=simulated.get("motivo_pausa"),
    )

    # Persistir solo columnas soportadas por el esquema/repositorio actual.
    persistable_changes = dict(normalized_changes)
    persistable_changes["estatus_venta"] = simulated.get("estatus_venta")
    if simulated.get("estatus_venta") == "Pausa" or "motivo_pausa" in persistable_changes:
        persistable_changes["motivo_pausa"] = simulated.get("motivo_pausa")
    else:
        persistable_changes.pop("motivo_pausa", None)
    persistable_changes = _persistable_pedido_fields(persistable_changes)

    audit_source_after = dict(persistable_changes)
    if _pedidos_supports_motivo_pausa() and "motivo_pausa" in simulated:
        audit_source_after["motivo_pausa"] = simulated.get("motivo_pausa")

    audit_rows = _build_audit_rows_for_changes(
        tabla="pedidos",
        registro_id=pedido_id,
        old_data=current,
        new_data=audit_source_after,
        usuario=usuario,
        motivo=motivo,
    )

    if persistable_changes:
        pedidos_repository.update_pedido_fields(pedido_id, persistable_changes)

    if PEDIDO_CONTEXT_FIELDS.intersection(persistable_changes.keys()):
        _sync_lineas_context_from_pedido_force(pedido_id, persistable_changes)

    if audit_rows:
        auditoria_repository.create_many_auditoria_cambios(audit_rows)

    updated = pedidos_repository.get_pedido_by_id(pedido_id)
    if updated is None:
        raise ValueError(f"No se pudo recuperar el pedido actualizado con id {pedido_id}.")

    # Si motivo_pausa todavía no existe en el esquema, se devuelve el estado canónico
    # para la respuesta inmediata, pero no queda persistido todavía.
    if not _pedidos_supports_motivo_pausa() and simulated.get("estatus_venta") == "Pausa":
        updated = dict(updated)
        updated["motivo_pausa"] = simulated.get("motivo_pausa")

    return updated



def update_pedido_linea_with_audit(
    pedido_linea_id: int,
    changes: dict[str, Any],
    usuario: str | None = None,
    motivo: str | None = None,
) -> dict[str, Any]:
    """
    Actualiza campos permitidos de una línea de pedido, recalcula faltante y porcentaje,
    y registra auditoría.
    """
    current = partidas_repository.get_pedido_linea_by_id(pedido_linea_id)
    if current is None:
        raise ValueError(f"No existe la línea con id {pedido_linea_id}.")

    if not changes:
        return current

    normalized_changes = _normalize_linea_changes(changes)
    if not normalized_changes:
        return current

    simulated = dict(current)
    simulated.update(normalized_changes)

    validate_linea_quantities(
        cantidad_pedida=simulated.get("cantidad_pedida"),
        cantidad_asignada=simulated.get("cantidad_asignada"),
        allow_overassignment=False,
    )

    audit_rows = _build_audit_rows_for_changes(
        tabla="pedido_lineas",
        registro_id=pedido_linea_id,
        old_data=current,
        new_data=normalized_changes,
        usuario=usuario,
        motivo=motivo,
    )

    partidas_repository.update_pedido_linea_fields(pedido_linea_id, normalized_changes)
    partidas_repository.recalculate_linea_faltante(pedido_linea_id)

    pedido_id = int(current["pedido_id"])
    pedidos_repository.recalculate_pedido_metrics(pedido_id)

    if audit_rows:
        auditoria_repository.create_many_auditoria_cambios(audit_rows)

    updated = partidas_repository.get_pedido_linea_by_id(pedido_linea_id)
    if updated is None:
        raise ValueError(f"No se pudo recuperar la línea actualizada con id {pedido_linea_id}.")

    return updated


# -----------------------------------------------------------------------------
# Demanda / oferta por SKU
# -----------------------------------------------------------------------------
def get_demanda_by_sku(sku: str) -> list[dict[str, Any]]:
    """
    Devuelve todas las líneas activas con faltante para un SKU.
    """
    normalized_sku = normalize_text(sku)
    if not normalized_sku:
        return []
    return partidas_repository.list_lineas_activas_con_faltante_by_sku(normalized_sku)



def get_oferta_for_new_pedido_lineas(
    lineas_data: list[dict[str, Any]],
    stock_lookup: dict[str, float],
) -> list[dict[str, Any]]:
    """
    Construye una vista simple de oferta para líneas de un pedido nuevo.
    stock_lookup debe venir como {sku: stock_libre_actual}.

    Importante: esta función es solo asistida.
    No reserva stock, no crea asignaciones y no modifica inventario.
    Sirve únicamente para mostrar cuánto podría surtirse de inmediato si el
    usuario decide asignar después en un flujo separado.
    """
    oferta_rows: list[dict[str, Any]] = []

    for raw_linea in lineas_data:
        sku = normalize_text(raw_linea.get("sku"))
        producto = normalize_text(raw_linea.get("producto"))
        descripcion = normalize_text(raw_linea.get("descripcion"))
        cantidad_pedida = _to_float(raw_linea.get("cantidad_pedida") or 0)
        stock_libre = _to_float(stock_lookup.get(sku or "", 0) or 0)

        cantidad_surtible = min(cantidad_pedida, stock_libre)
        faltante = max(0.0, cantidad_pedida - cantidad_surtible)

        oferta_rows.append(
            {
                "sku": sku,
                "producto": producto,
                "descripcion": descripcion,
                "cantidad_pedida": round(cantidad_pedida, 6),
                "stock_libre_actual": round(stock_libre, 6),
                "cantidad_surtible": round(cantidad_surtible, 6),
                "cantidad_faltante_si_asigno": round(faltante, 6),
                "autoasignado": False,
                "requiere_decision_usuario": True,
            }
        )

    return oferta_rows
