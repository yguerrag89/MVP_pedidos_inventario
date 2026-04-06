from __future__ import annotations

from typing import Any

from wms_mvp.core.rules.pedidos_rules import (
    aggregate_lineas_metrics,
    build_linea_cierre_snapshot,
    can_close_pedido,
    infer_estado_operativo_from_cierre,
    normalize_text,
    to_float,
    validate_tipo_cierre,
)


def validate_fecha_envio(fecha_envio: Any) -> str:
    """
    Valida que la fecha de envío exista como texto no vacío.
    En esta etapa no impone formato estricto de fecha; eso puede endurecerse después.
    """
    normalized = normalize_text(fecha_envio)
    if not normalized:
        raise ValueError("La fecha de envío es obligatoria para cerrar un pedido.")
    return normalized


def validate_usuario_cierre(usuario: Any) -> str | None:
    """
    Normaliza el usuario de cierre.
    """
    return normalize_text(usuario)


def validate_observacion_cierre(observacion: Any) -> str | None:
    """
    Normaliza la observación de cierre.
    """
    return normalize_text(observacion)


def build_cierre_header(
    pedido: dict[str, Any],
    lineas: list[dict[str, Any]],
    tipo_cierre: Any,
    fecha_envio: Any,
    usuario: Any | None = None,
    observacion: Any | None = None,
) -> dict[str, Any]:
    """
    Construye el encabezado normalizado del cierre de pedido.
    """
    ok, error = can_close_pedido(pedido, lineas, tipo_cierre)
    if not ok:
        raise ValueError(error or "El pedido no puede cerrarse.")

    normalized_tipo_cierre = validate_tipo_cierre(tipo_cierre)
    normalized_fecha_envio = validate_fecha_envio(fecha_envio)

    return {
        "pedido_id": pedido["id"],
        "tipo_cierre": normalized_tipo_cierre,
        "fecha_envio": normalized_fecha_envio,
        "usuario": validate_usuario_cierre(usuario),
        "observacion": validate_observacion_cierre(observacion),
        "estado_operativo_destino": infer_estado_operativo_from_cierre(normalized_tipo_cierre),
    }


def build_cierre_lineas_snapshot(
    lineas: list[dict[str, Any]],
    tipo_cierre: Any,
) -> list[dict[str, Any]]:
    """
    Construye el snapshot por línea que se guardará al momento del cierre.
    """
    normalized_tipo_cierre = validate_tipo_cierre(tipo_cierre)

    snapshots: list[dict[str, Any]] = []
    for linea in lineas:
        snapshot = build_linea_cierre_snapshot(linea, normalized_tipo_cierre)
        snapshot["pedido_linea_id"] = linea["id"]
        snapshots.append(snapshot)

    return snapshots


def build_cierre_payload(
    pedido: dict[str, Any],
    lineas: list[dict[str, Any]],
    tipo_cierre: Any,
    fecha_envio: Any,
    usuario: Any | None = None,
    observacion: Any | None = None,
) -> dict[str, Any]:
    """
    Construye el payload completo del cierre:
    - encabezado
    - snapshots de líneas
    - métricas agregadas del pedido al momento del cierre
    """
    header = build_cierre_header(
        pedido=pedido,
        lineas=lineas,
        tipo_cierre=tipo_cierre,
        fecha_envio=fecha_envio,
        usuario=usuario,
        observacion=observacion,
    )
    snapshots = build_cierre_lineas_snapshot(lineas, tipo_cierre)
    metrics = aggregate_lineas_metrics(lineas)

    return {
        "header": header,
        "lineas_snapshot": snapshots,
        "metrics": metrics,
    }


def calculate_total_enviado_en_cierre(lineas_snapshot: list[dict[str, Any]]) -> float:
    """
    Suma la cantidad enviada total de un cierre.
    """
    total = 0.0
    for row in lineas_snapshot:
        total += to_float(row.get("cantidad_enviada"))
    return round(total, 6)


def calculate_total_no_enviado_en_cierre(lineas_snapshot: list[dict[str, Any]]) -> float:
    """
    Suma la cantidad no enviada total de un cierre.
    """
    total = 0.0
    for row in lineas_snapshot:
        total += to_float(row.get("cantidad_no_enviada"))
    return round(total, 6)


def pedido_queda_totalmente_enviado(lineas_snapshot: list[dict[str, Any]]) -> bool:
    """
    Indica si el snapshot del cierre deja todo el pedido enviado.
    """
    return calculate_total_no_enviado_en_cierre(lineas_snapshot) == 0.0


def validate_cierre_consistency(
    pedido: dict[str, Any],
    lineas: list[dict[str, Any]],
    cierre_payload: dict[str, Any],
) -> None:
    """
    Valida consistencia básica del payload final de cierre.
    """
    if not pedido:
        raise ValueError("No se recibió el pedido para validar el cierre.")

    if not lineas:
        raise ValueError("No se recibieron líneas para validar el cierre.")

    header = cierre_payload.get("header")
    lineas_snapshot = cierre_payload.get("lineas_snapshot", [])
    metrics = cierre_payload.get("metrics", {})

    if not header:
        raise ValueError("El payload de cierre no contiene encabezado.")

    if not lineas_snapshot:
        raise ValueError("El payload de cierre no contiene snapshots de líneas.")

    if len(lineas_snapshot) != len(lineas):
        raise ValueError(
            "El número de snapshots de cierre no coincide con el número de líneas del pedido."
        )

    tipo_cierre = header["tipo_cierre"]
    total_no_enviado = calculate_total_no_enviado_en_cierre(lineas_snapshot)

    if tipo_cierre == "TOTAL" and total_no_enviado > 0:
        raise ValueError(
            "Un cierre TOTAL no puede dejar cantidad no enviada en el snapshot."
        )

    total_enviado_snapshot = calculate_total_enviado_en_cierre(lineas_snapshot)
    total_asignado_metrics = round(to_float(metrics.get("total_asignado")), 6)

    if abs(total_enviado_snapshot - total_asignado_metrics) > 1e-9:
        raise ValueError(
            "La cantidad enviada en el cierre no coincide con la cantidad asignada total."
        )


def build_linea_update_for_cierre(
    linea: dict[str, Any],
    tipo_cierre: Any,
) -> dict[str, float]:
    """
    Construye los campos que deben actualizarse en pedido_lineas al cerrar.
    Regla MVP:
    - cantidad_enviada toma la cantidad asignada al momento del cierre
    - cantidad_faltante se conserva como dato histórico del pedido original
    """
    normalized_tipo_cierre = validate_tipo_cierre(tipo_cierre)
    snapshot = build_linea_cierre_snapshot(linea, normalized_tipo_cierre)

    return {
        "cantidad_enviada": snapshot["cantidad_enviada"],
        "cantidad_faltante": max(
            0.0,
            to_float(linea.get("cantidad_pedida")) - to_float(linea.get("cantidad_asignada")),
        ),
    }


def should_offer_backorder_creation(lineas_snapshot: list[dict[str, Any]]) -> bool:
    """
    Indica si, tras el cierre, conviene ofrecer la creación de un nuevo pedido
    para los faltantes.
    """
    return calculate_total_no_enviado_en_cierre(lineas_snapshot) > 0


def extract_backorder_candidates(
    pedido: dict[str, Any],
    lineas: list[dict[str, Any]],
    tipo_cierre: Any,
) -> list[dict[str, Any]]:
    """
    Extrae las líneas candidatas para crear un nuevo pedido posterior
    con las cantidades faltantes.
    """
    normalized_tipo_cierre = validate_tipo_cierre(tipo_cierre)
    candidates: list[dict[str, Any]] = []

    for linea in lineas:
        snapshot = build_linea_cierre_snapshot(linea, normalized_tipo_cierre)
        cantidad_no_enviada = to_float(snapshot["cantidad_no_enviada"])

        if cantidad_no_enviada <= 0:
            continue

        candidates.append(
            {
                "pedido_origen_id": pedido["id"],
                "id_pedido_origen": pedido.get("id_pedido"),
                "pedido_linea_id_origen": linea["id"],
                "sku": linea.get("sku"),
                "descripcion": linea.get("descripcion"),
                "producto": linea.get("producto"),
                "color": linea.get("color"),
                "cantidad_pendiente_para_nuevo_pedido": round(cantidad_no_enviada, 6),
            }
        )

    return candidates