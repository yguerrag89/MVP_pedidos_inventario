from __future__ import annotations

from typing import Any

from wms_mvp.core.rules.pedidos_rules import normalize_text, to_float


VALID_MOTIVO_REASIGNACION = {
    "PRIORIDAD_CLIENTE",
    "CAMBIO_FECHA_LOGISTICA",
    "COMPROMISO_COMERCIAL",
    "REPROGRAMACION",
    "CORRECCION_OPERATIVA",
    "OTRO",
}


def validate_positive_quantity(value: Any, field_name: str = "cantidad") -> float:
    cantidad = to_float(value)
    if cantidad <= 0:
        raise ValueError(f"El campo '{field_name}' debe ser mayor que cero.")
    return float(cantidad)



def validate_same_sku(sku_origen: Any, sku_destino: Any) -> str:
    origin = normalize_text(sku_origen)
    dest = normalize_text(sku_destino)
    if not origin or not dest:
        raise ValueError("Ambas líneas deben tener SKU válido.")
    if origin != dest:
        raise ValueError("La reasignación solo puede hacerse entre líneas del mismo SKU.")
    return origin



def validate_distinct_lines(linea_origen_id: Any, linea_destino_id: Any) -> None:
    if int(linea_origen_id or 0) == int(linea_destino_id or 0):
        raise ValueError("La línea origen y la línea destino deben ser diferentes.")



def validate_active_order_state(estado_operativo: Any) -> None:
    if normalize_text(estado_operativo) != "ACTIVO":
        raise ValueError("Solo se permiten operaciones de reserva sobre pedidos activos.")



def validate_release_quantity(*, cantidad_a_liberar: Any, cantidad_asignada_linea: Any) -> float:
    cantidad = validate_positive_quantity(cantidad_a_liberar, "cantidad_a_liberar")
    asignada = max(0.0, to_float(cantidad_asignada_linea))
    if cantidad > asignada:
        raise ValueError("No se puede liberar más cantidad de la que está asignada en la línea.")
    return cantidad



def validate_reassign_quantity(*, cantidad_a_reasignar: Any, cantidad_asignada_origen: Any, cantidad_faltante_destino: Any) -> float:
    cantidad = validate_positive_quantity(cantidad_a_reasignar, "cantidad_a_reasignar")
    asignada_origen = max(0.0, to_float(cantidad_asignada_origen))
    faltante_destino = max(0.0, to_float(cantidad_faltante_destino))

    if cantidad > asignada_origen:
        raise ValueError("No se puede reasignar más cantidad de la que tiene asignada la línea origen.")
    if cantidad > faltante_destino:
        raise ValueError("No se puede reasignar más cantidad del faltante actual de la línea destino.")
    return cantidad



def normalize_motivo_reasignacion(value: Any) -> str:
    raw = (normalize_text(value) or "OTRO").upper()
    alias = {
        "PRIORIDAD": "PRIORIDAD_CLIENTE",
        "FECHA": "CAMBIO_FECHA_LOGISTICA",
        "COMERCIAL": "COMPROMISO_COMERCIAL",
        "OPERATIVA": "CORRECCION_OPERATIVA",
    }
    normalized = alias.get(raw, raw)
    if normalized not in VALID_MOTIVO_REASIGNACION:
        return "OTRO"
    return normalized
