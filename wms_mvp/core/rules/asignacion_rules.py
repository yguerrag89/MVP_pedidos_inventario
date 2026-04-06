from __future__ import annotations

from typing import Any

from wms_mvp.core.rules.pedidos_rules import normalize_text, to_float


VALID_DECISION_TYPES = {
    "STOCK_LIBRE",
    "ASIGNACION_PEDIDO",
    "PENDIENTE",
    "IGNORADA",
}

VALID_TIPO_SUGERENCIA = {
    "PEDIDO",
    "STOCK",
    "AMBIGUA",
    "SIN_DATO",
}

MODO_DECISION_TODO_A_STOCK_LIBRE = "TODO_A_STOCK_LIBRE"
MODO_DECISION_ACEPTAR_PROPUESTA = "ACEPTAR_PROPUESTA"
MODO_DECISION_REPARTIR_MANUAL = "REPARTIR_MANUAL"
MODO_DECISION_DEJAR_PENDIENTE = "DEJAR_PENDIENTE"

# Alias compatibles con fases previas
MODO_DECISION_APLICAR_SUGERENCIA_VALIDA = MODO_DECISION_ACEPTAR_PROPUESTA
MODO_DECISION_ASIGNACION_MANUAL_ASISTIDA = MODO_DECISION_REPARTIR_MANUAL

VALID_MODO_DECISION_MVP = {
    MODO_DECISION_TODO_A_STOCK_LIBRE,
    MODO_DECISION_ACEPTAR_PROPUESTA,
    MODO_DECISION_REPARTIR_MANUAL,
    MODO_DECISION_DEJAR_PENDIENTE,
}


# =========================================================
# Helpers básicos
# =========================================================
def normalize_upper_text(value: Any) -> str | None:
    text = normalize_text(value)
    return text.upper() if text else None



def normalize_id_pedido(value: Any) -> str | None:
    text = normalize_upper_text(value)
    if not text:
        return None

    compact = text.replace(" ", "").replace("_", "-")
    while "--" in compact:
        compact = compact.replace("--", "-")
    return compact



def _date_key(value: Any, fallback: str = "9999-12-31") -> str:
    text = normalize_text(value)
    return text or fallback



def _estatus_priority(value: Any) -> int:
    estatus = (normalize_text(value) or "").strip().lower()
    if estatus == "entregar":
        return 0
    if estatus == "en cobro":
        return 1
    if estatus == "pausa":
        return 2
    return 3



def _manual_priority(value: Any) -> int:
    try:
        if value in (None, ""):
            return 9999
        return int(float(value))
    except Exception:
        return 9999



def validate_non_negative(value: Any, field_name: str) -> float:
    numeric_value = to_float(value)
    if numeric_value < 0:
        raise ValueError(f"El campo '{field_name}' no puede ser negativo.")
    return numeric_value



def validate_positive(value: Any, field_name: str) -> float:
    numeric_value = to_float(value)
    if numeric_value <= 0:
        raise ValueError(f"El campo '{field_name}' debe ser mayor que cero.")
    return numeric_value



def validate_decision_type(decision_type: Any) -> str:
    normalized = normalize_upper_text(decision_type)
    if normalized not in VALID_DECISION_TYPES:
        raise ValueError(
            f"Tipo de decisión inválido: {decision_type}. "
            f"Valores permitidos: {sorted(VALID_DECISION_TYPES)}"
        )
    return normalized



def validate_tipo_sugerencia(tipo_sugerencia: Any) -> str:
    normalized = normalize_upper_text(tipo_sugerencia)
    if normalized not in VALID_TIPO_SUGERENCIA:
        raise ValueError(
            f"Tipo de sugerencia inválido: {tipo_sugerencia}. "
            f"Valores permitidos: {sorted(VALID_TIPO_SUGERENCIA)}"
        )
    return normalized



def normalize_modo_decision_mvp(value: Any) -> str:
    raw = normalize_upper_text(value)
    alias_map = {
        "TODO_A_STOCK_LIBRE": MODO_DECISION_TODO_A_STOCK_LIBRE,
        "STOCK_LIBRE": MODO_DECISION_TODO_A_STOCK_LIBRE,
        "ACEPTAR_PROPUESTA": MODO_DECISION_ACEPTAR_PROPUESTA,
        "APLICAR_SUGERENCIA_VALIDA": MODO_DECISION_ACEPTAR_PROPUESTA,
        "PROPUESTA": MODO_DECISION_ACEPTAR_PROPUESTA,
        "REPARTIR_MANUAL": MODO_DECISION_REPARTIR_MANUAL,
        "ASIGNACION_MANUAL_ASISTIDA": MODO_DECISION_REPARTIR_MANUAL,
        "MANUAL": MODO_DECISION_REPARTIR_MANUAL,
        "DEJAR_PENDIENTE": MODO_DECISION_DEJAR_PENDIENTE,
        "PENDIENTE": MODO_DECISION_DEJAR_PENDIENTE,
    }
    normalized = alias_map.get(raw, raw)
    if normalized not in VALID_MODO_DECISION_MVP:
        raise ValueError(
            f"Modo de decisión inválido: {value}. Valores permitidos: {sorted(VALID_MODO_DECISION_MVP)}"
        )
    return normalized



def validate_modo_decision_mvp(value: Any) -> str:
    return normalize_modo_decision_mvp(value)



def get_modos_decision_mvp() -> list[str]:
    return [
        MODO_DECISION_ACEPTAR_PROPUESTA,
        MODO_DECISION_REPARTIR_MANUAL,
        MODO_DECISION_TODO_A_STOCK_LIBRE,
        MODO_DECISION_DEJAR_PENDIENTE,
    ]


# =========================================================
# Cálculos operativos
# =========================================================
def calculate_stock_disponible_para_asignar(stock_libre: Any) -> float:
    return validate_non_negative(stock_libre, "stock_libre")



def calculate_cantidad_surtible(cantidad_pedida: Any, stock_libre: Any) -> float:
    pedida = validate_non_negative(cantidad_pedida, "cantidad_pedida")
    libre = validate_non_negative(stock_libre, "stock_libre")
    return min(pedida, libre)



def calculate_faltante_post_asignacion(cantidad_pedida: Any, cantidad_a_asignar: Any) -> float:
    pedida = validate_non_negative(cantidad_pedida, "cantidad_pedida")
    asignar = validate_non_negative(cantidad_a_asignar, "cantidad_a_asignar")
    faltante = pedida - asignar
    return faltante if faltante > 0 else 0.0



def calculate_decision_restante(cantidad_entrada: Any, decisiones: list[dict[str, Any]]) -> float:
    total_entrada = validate_non_negative(cantidad_entrada, "cantidad_entrada")
    total_decidido = 0.0
    for decision in decisiones:
        total_decidido += validate_non_negative(decision.get("cantidad", 0), "cantidad_decision")
    restante = total_entrada - total_decidido
    return restante if restante > 0 else 0.0



def decisions_sum(decisiones: list[dict[str, Any]]) -> float:
    total = 0.0
    for decision in decisiones:
        total += validate_non_negative(decision.get("cantidad", 0), "cantidad_decision")
    return round(total, 6)



def validate_decisiones_match_total(cantidad_entrada: Any, decisiones: list[dict[str, Any]]) -> None:
    total_entrada = validate_non_negative(cantidad_entrada, "cantidad_entrada")
    total_decidido = decisions_sum(decisiones)
    if abs(total_entrada - total_decidido) > 1e-9:
        raise ValueError(
            "La suma de las decisiones no coincide con la cantidad total de la entrada."
        )


# =========================================================
# Payloads de decisiones
# =========================================================
def validate_decision_payload(decision: dict[str, Any]) -> dict[str, Any]:
    decision_type = validate_decision_type(decision.get("decision_type"))
    cantidad = validate_positive(decision.get("cantidad"), "cantidad")

    normalized_decision = {
        "decision_type": decision_type,
        "pedido_id": decision.get("pedido_id"),
        "pedido_linea_id": decision.get("pedido_linea_id"),
        "sku": normalize_text(decision.get("sku")),
        "cantidad": cantidad,
        "observacion": normalize_text(decision.get("observacion")),
        "created_by": normalize_text(decision.get("created_by")),
        "entrada_staging_id": decision.get("entrada_staging_id"),
        "modo_decision": normalize_text(decision.get("modo_decision")),
    }

    if decision_type == "ASIGNACION_PEDIDO":
        if normalized_decision["pedido_id"] in (None, ""):
            raise ValueError("Una decisión ASIGNACION_PEDIDO requiere pedido_id.")
        if normalized_decision["pedido_linea_id"] in (None, ""):
            raise ValueError("Una decisión ASIGNACION_PEDIDO requiere pedido_linea_id.")
        if not normalized_decision["sku"]:
            raise ValueError("Una decisión ASIGNACION_PEDIDO requiere sku.")

    if decision_type in {"STOCK_LIBRE", "PENDIENTE"}:
        if not normalized_decision["sku"]:
            raise ValueError(f"Una decisión {decision_type} requiere sku.")

    if normalized_decision["modo_decision"]:
        normalized_decision["modo_decision"] = validate_modo_decision_mvp(normalized_decision["modo_decision"])

    return normalized_decision



def validate_decisiones_payloads(cantidad_entrada: Any, decisiones: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not decisiones:
        raise ValueError("Debe existir al menos una decisión.")

    normalized = [validate_decision_payload(decision) for decision in decisiones]
    validate_decisiones_match_total(cantidad_entrada, normalized)
    return normalized



def validate_asignacion_to_linea(
    cantidad_disponible: Any,
    cantidad_a_asignar: Any,
    cantidad_faltante_linea: Any,
    allow_overassignment: bool = False,
) -> None:
    disponible = validate_non_negative(cantidad_disponible, "cantidad_disponible")
    asignar = validate_positive(cantidad_a_asignar, "cantidad_a_asignar")
    faltante = validate_non_negative(cantidad_faltante_linea, "cantidad_faltante_linea")

    if asignar > disponible:
        raise ValueError(
            "La cantidad a asignar no puede ser mayor que la cantidad disponible."
        )

    if not allow_overassignment and asignar > faltante:
        raise ValueError(
            "La cantidad a asignar no puede ser mayor que el faltante de la línea."
        )



def build_stock_libre_decision(
    entrada_staging_id: int,
    sku: str,
    cantidad: Any,
    observacion: str | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    return validate_decision_payload(
        {
            "entrada_staging_id": entrada_staging_id,
            "decision_type": "STOCK_LIBRE",
            "sku": sku,
            "cantidad": cantidad,
            "observacion": observacion,
            "created_by": created_by,
        }
    )



def build_asignacion_pedido_decision(
    entrada_staging_id: int,
    pedido_id: int,
    pedido_linea_id: int,
    sku: str,
    cantidad: Any,
    observacion: str | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    return validate_decision_payload(
        {
            "entrada_staging_id": entrada_staging_id,
            "decision_type": "ASIGNACION_PEDIDO",
            "pedido_id": pedido_id,
            "pedido_linea_id": pedido_linea_id,
            "sku": sku,
            "cantidad": cantidad,
            "observacion": observacion,
            "created_by": created_by,
        }
    )



def build_pendiente_decision(
    entrada_staging_id: int | None,
    sku: str,
    cantidad: Any,
    observacion: str | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    return validate_decision_payload(
        {
            "entrada_staging_id": entrada_staging_id,
            "decision_type": "PENDIENTE",
            "sku": sku,
            "cantidad": cantidad,
            "observacion": observacion,
            "created_by": created_by,
        }
    )


# =========================================================
# Sugerencias de producción
# =========================================================
def infer_tipo_sugerencia_from_text(raw_text: Any) -> str:
    text = normalize_upper_text(raw_text)
    if not text:
        return "SIN_DATO"

    if "STOCK" in text or "LIBRE" in text:
        return "STOCK"

    if any(token in text for token in ("VT", "BO", "COT", "ALM", "VTA", "BOC")):
        return "PEDIDO"

    return "AMBIGUA"



def extract_pedido_sugerido_text(raw_text: Any) -> str | None:
    tipo_sugerencia = infer_tipo_sugerencia_from_text(raw_text)
    if tipo_sugerencia != "PEDIDO":
        return None
    return normalize_text(raw_text)


# =========================================================
# Filas operativas de demanda / oferta
# =========================================================
def build_demanda_row(
    pedido_id: int,
    pedido_linea_id: int,
    id_pedido: str,
    cliente: str | None,
    estatus_venta: str | None,
    fecha_logistica: str | None,
    fecha_captura: str | None,
    producto: str | None,
    cantidad_pedida: Any,
    cantidad_asignada: Any,
    porcentaje_asignacion: Any,
    pedido_sugerido_texto: str | None = None,
    fecha_produccion: str | None = None,
    color: str | None = None,
    sku: str | None = None,
    descripcion: str | None = None,
    prioridad_manual: Any = None,
) -> dict[str, Any]:
    pedida = validate_non_negative(cantidad_pedida, "cantidad_pedida")
    asignada = validate_non_negative(cantidad_asignada, "cantidad_asignada")
    faltante = pedida - asignada
    if faltante < 0:
        faltante = 0.0

    id_pedido_norm = normalize_text(id_pedido)
    id_pedido_cmp = normalize_id_pedido(id_pedido)
    sugerido_cmp = normalize_id_pedido(pedido_sugerido_texto)

    return {
        "pedido_id": pedido_id,
        "pedido_linea_id": pedido_linea_id,
        "id_pedido": id_pedido_norm,
        "cliente": normalize_text(cliente),
        "estatus_venta": normalize_text(estatus_venta),
        "fecha_captura": normalize_text(fecha_captura),
        "fecha_produccion": normalize_text(fecha_produccion),
        "fecha_logistica": normalize_text(fecha_logistica),
        "producto": normalize_text(producto),
        "color": normalize_text(color),
        "sku": normalize_text(sku),
        "descripcion": normalize_text(descripcion),
        "prioridad_manual": _manual_priority(prioridad_manual),
        "cantidad_pedida": round(pedida, 6),
        "cantidad_asignada": round(asignada, 6),
        "cantidad_faltante": round(faltante, 6),
        "porcentaje_asignacion": round(to_float(porcentaje_asignacion), 2),
        "es_pedido_sugerido": bool(id_pedido_cmp and sugerido_cmp and id_pedido_cmp == sugerido_cmp),
    }



def sort_demanda_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Orden oficial del MVP para demanda/candidatos por SKU:
    1) prioridad_manual
    2) estatus_venta
    3) fecha_logistica
    4) fecha_produccion
    5) fecha_captura
    6) id_pedido
    7) pedido_linea_id

    El pedido sugerido de producción NO altera el ranking; se conserva solo como
    marca visual para apoyar la decisión del usuario.
    """
    def sort_key(row: dict[str, Any]) -> tuple[Any, Any, Any, Any, Any, Any, Any]:
        prioridad_manual = _manual_priority(row.get("prioridad_manual"))
        estatus = _estatus_priority(row.get("estatus_venta"))
        fecha_logistica = _date_key(row.get("fecha_logistica"))
        fecha_produccion = _date_key(row.get("fecha_produccion"))
        fecha_captura = _date_key(row.get("fecha_captura"))
        id_pedido = normalize_text(row.get("id_pedido")) or "ZZZZZZ"
        pedido_linea_id = int(row.get("pedido_linea_id") or 0)
        return (
            prioridad_manual,
            estatus,
            fecha_logistica,
            fecha_produccion,
            fecha_captura,
            id_pedido,
            pedido_linea_id,
        )

    return sorted(rows, key=sort_key)



def sort_pedidos_candidatos_por_prioridad(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sort_demanda_rows(rows)



def build_oferta_row(
    sku: str,
    producto: str | None,
    cantidad_pedida: Any,
    stock_libre: Any,
) -> dict[str, Any]:
    pedida = validate_non_negative(cantidad_pedida, "cantidad_pedida")
    libre = validate_non_negative(stock_libre, "stock_libre")
    surtible = min(pedida, libre)
    faltante = pedida - surtible
    if faltante < 0:
        faltante = 0.0

    return {
        "sku": normalize_text(sku),
        "producto": normalize_text(producto),
        "cantidad_pedida": round(pedida, 6),
        "stock_libre_actual": round(libre, 6),
        "cantidad_surtible": round(surtible, 6),
        "cantidad_faltante_si_asigno": round(faltante, 6),
    }



def validate_stock_libre_move(stock_libre_actual: Any, cantidad_a_mover: Any) -> None:
    libre = validate_non_negative(stock_libre_actual, "stock_libre_actual")
    mover = validate_positive(cantidad_a_mover, "cantidad_a_mover")

    if mover > libre:
        raise ValueError(
            "No se puede mover más cantidad de la que existe en stock libre."
        )


# =========================================================
# Reglas puras del flujo por SKU del MVP
# =========================================================
def _validate_non_negative_sku(value: Any, field_name: str) -> float:
    numeric_value = to_float(value)
    if numeric_value < 0:
        raise ValueError(f"El campo '{field_name}' no puede ser negativo.")
    return round(numeric_value, 6)



def validate_split_total(total_esperado: Any, partes: list[Any], tolerance: float = 1e-6) -> float:
    total = _validate_non_negative_sku(total_esperado, "total_esperado")
    suma = 0.0
    for idx, value in enumerate(partes, start=1):
        suma += _validate_non_negative_sku(value, f"parte_{idx}")
    suma = round(suma, 6)
    if abs(total - suma) > tolerance:
        raise ValueError(
            f"La suma del reparto ({suma}) no coincide con el total esperado ({total})."
        )
    return suma



def validate_total_decision_sku(
    *,
    cantidad_recibida: Any,
    cantidad_a_pedidos: Any = 0,
    cantidad_stock_libre: Any = 0,
    cantidad_pendiente: Any = 0,
) -> dict[str, float]:
    total_qty = _validate_non_negative_sku(cantidad_recibida, "cantidad_recibida")
    pedidos_qty = _validate_non_negative_sku(cantidad_a_pedidos, "cantidad_a_pedidos")
    stock_qty = _validate_non_negative_sku(cantidad_stock_libre, "cantidad_stock_libre")
    pendiente_qty = _validate_non_negative_sku(cantidad_pendiente, "cantidad_pendiente")
    validate_split_total(total_qty, [pedidos_qty, stock_qty, pendiente_qty])
    return {
        "cantidad_recibida": total_qty,
        "cantidad_a_pedidos": pedidos_qty,
        "cantidad_stock_libre": stock_qty,
        "cantidad_pendiente": pendiente_qty,
    }



def validate_cantidad_recibida_vs_decision(cantidad_recibida: Any, cantidad_decidida: Any) -> None:
    recibida = _validate_non_negative_sku(cantidad_recibida, "cantidad_recibida")
    decidida = _validate_non_negative_sku(cantidad_decidida, "cantidad_decidida")
    if decidida > recibida:
        raise ValueError("La decisión no puede exceder la cantidad recibida del SKU.")



def validate_manual_split_payload(
    *,
    cantidad_recibida: Any,
    pedidos: list[dict[str, Any]] | None = None,
    cantidad_stock_libre: Any = 0,
    cantidad_pendiente: Any = 0,
) -> dict[str, Any]:
    pedidos = pedidos or []
    total = _validate_non_negative_sku(cantidad_recibida, "cantidad_recibida")
    stock_qty = _validate_non_negative_sku(cantidad_stock_libre, "cantidad_stock_libre")
    pendiente_qty = _validate_non_negative_sku(cantidad_pendiente, "cantidad_pendiente")

    normalized_pedidos: list[dict[str, Any]] = []
    total_pedidos = 0.0
    for idx, item in enumerate(pedidos, start=1):
        qty = _validate_non_negative_sku(item.get("cantidad", 0), f"cantidad_pedido_{idx}")
        pedido_id = item.get("pedido_id")
        pedido_linea_id = item.get("pedido_linea_id")
        faltante = item.get("cantidad_faltante")
        if pedido_id in (None, ""):
            raise ValueError(f"El reparto manual requiere pedido_id en la fila {idx}.")
        if pedido_linea_id in (None, ""):
            raise ValueError(f"El reparto manual requiere pedido_linea_id en la fila {idx}.")
        if faltante is not None and qty > _validate_non_negative_sku(faltante, f"faltante_pedido_{idx}"):
            raise ValueError(
                f"La cantidad asignada al pedido {pedido_id} excede su faltante operativo."
            )
        total_pedidos += qty
        normalized_pedidos.append(
            {
                **item,
                "cantidad": qty,
            }
        )

    total_pedidos = round(total_pedidos, 6)
    validate_total_decision_sku(
        cantidad_recibida=total,
        cantidad_a_pedidos=total_pedidos,
        cantidad_stock_libre=stock_qty,
        cantidad_pendiente=pendiente_qty,
    )

    return {
        "cantidad_recibida": total,
        "pedidos": normalized_pedidos,
        "cantidad_a_pedidos": total_pedidos,
        "cantidad_stock_libre": stock_qty,
        "cantidad_pendiente": pendiente_qty,
    }



def validate_reparto_manual_sku(
    *,
    cantidad_recibida: Any,
    pedidos: list[dict[str, Any]] | None = None,
    cantidad_stock_libre: Any = 0,
    cantidad_pendiente: Any = 0,
) -> dict[str, Any]:
    return validate_manual_split_payload(
        cantidad_recibida=cantidad_recibida,
        pedidos=pedidos,
        cantidad_stock_libre=cantidad_stock_libre,
        cantidad_pendiente=cantidad_pendiente,
    )



def validate_manual_assignment_split(
    *,
    cantidad_recibida: Any | None = None,
    pedidos: list[dict[str, Any]] | None = None,
    cantidad_stock_libre: Any = 0,
    cantidad_pendiente: Any = 0,
    cantidad_total: Any | None = None,
    destinos: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """
    Compatibilidad:
    - API nueva: devuelve payload validado completo
    - API legada: acepta `cantidad_total` + `destinos` y devuelve la lista normalizada
    """
    if cantidad_recibida is None and cantidad_total is not None:
        payload = validate_manual_split_payload(
            cantidad_recibida=cantidad_total,
            pedidos=destinos,
            cantidad_stock_libre=0,
            cantidad_pendiente=0,
        )
        return payload["pedidos"]

    payload = validate_manual_split_payload(
        cantidad_recibida=cantidad_recibida,
        pedidos=pedidos,
        cantidad_stock_libre=cantidad_stock_libre,
        cantidad_pendiente=cantidad_pendiente,
    )
    return payload



def validate_propuesta_resultado(
    *,
    cantidad_recibida: Any,
    asignaciones_pedidos: list[dict[str, Any]] | None = None,
    cantidad_stock_libre: Any = 0,
    cantidad_pendiente: Any = 0,
) -> dict[str, Any]:
    payload = validate_manual_split_payload(
        cantidad_recibida=cantidad_recibida,
        pedidos=asignaciones_pedidos,
        cantidad_stock_libre=cantidad_stock_libre,
        cantidad_pendiente=cantidad_pendiente,
    )
    payload["remanente_final"] = round(payload["cantidad_stock_libre"] + payload["cantidad_pendiente"], 6)
    return payload



def build_pendiente_decision_sku(*, sku: str, cantidad: Any, observacion: str | None = None) -> dict[str, Any]:
    qty = _validate_non_negative_sku(cantidad, "cantidad")
    if qty <= 0:
        raise ValueError("La cantidad pendiente debe ser mayor que cero.")
    return {
        "decision_type": "PENDIENTE",
        "sku": normalize_text(sku),
        "cantidad": qty,
        "observacion": normalize_text(observacion),
        "modo_decision": MODO_DECISION_DEJAR_PENDIENTE,
    }
