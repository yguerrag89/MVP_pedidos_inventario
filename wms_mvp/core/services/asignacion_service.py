from __future__ import annotations

from typing import Any

from wms_mvp.core.rules.asignacion_rules import (
    MODO_DECISION_ACEPTAR_PROPUESTA,
    MODO_DECISION_DEJAR_PENDIENTE,
    MODO_DECISION_REPARTIR_MANUAL,
    MODO_DECISION_TODO_A_STOCK_LIBRE,
    build_asignacion_pedido_decision,
    build_pendiente_decision,
    build_demanda_row,
    build_oferta_row,
    build_stock_libre_decision,
    calculate_decision_restante,
    extract_pedido_sugerido_text,
    infer_tipo_sugerencia_from_text,
    normalize_modo_decision_mvp as normalize_modo_decision_mvp_rule,
    sort_demanda_rows,
    validate_asignacion_to_linea,
    validate_decision_payload,
    validate_decisiones_payloads,
    validate_positive,
)
from wms_mvp.core.rules.pedidos_rules import normalize_text
from wms_mvp.db.connection import fetch_one, transaction
from wms_mvp.db.repositories import asignaciones_repository
from wms_mvp.db.repositories import auditoria_repository
from wms_mvp.db.repositories import entradas_repository
from wms_mvp.db.repositories import partidas_repository
from wms_mvp.db.repositories import pedidos_repository


MODO_DECISION_APLICAR_SUGERENCIA_VALIDA = MODO_DECISION_ACEPTAR_PROPUESTA
MODO_DECISION_ASIGNACION_MANUAL_ASISTIDA = MODO_DECISION_REPARTIR_MANUAL

VALID_MODO_DECISION_MVP = {
    MODO_DECISION_TODO_A_STOCK_LIBRE,
    MODO_DECISION_ACEPTAR_PROPUESTA,
    MODO_DECISION_REPARTIR_MANUAL,
    MODO_DECISION_DEJAR_PENDIENTE,
}


# =========================================================
# Helpers internos
# =========================================================
def _date_key(value: Any, fallback: str = "9999-12-31") -> str:
    text = normalize_text(value)
    return text or fallback



def _sort_demanda_rows_operativo(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Delega al orden oficial definido en rules.asignacion_rules."""
    return sort_demanda_rows(rows)



def _build_demanda_row_operativo(
    raw_row: dict[str, Any],
    pedido_sugerido_texto: str | None = None,
) -> dict[str, Any]:
    """Construye una fila operativa usando la misma regla/base del módulo rules."""
    return build_demanda_row(
        pedido_id=int(raw_row.get("pedido_id") or 0),
        pedido_linea_id=int(raw_row.get("pedido_linea_id") or 0),
        id_pedido=normalize_text(raw_row.get("id_pedido")) or "",
        cliente=raw_row.get("cliente"),
        estatus_venta=(
            raw_row.get("estatus_venta_operativo")
            or raw_row.get("estatus_venta")
            or raw_row.get("linea_estatus_venta")
        ),
        fecha_logistica=(
            raw_row.get("linea_fecha_logistica")
            or raw_row.get("pedido_fecha_logistica")
            or raw_row.get("fecha_logistica")
        ),
        fecha_captura=(
            raw_row.get("linea_fecha_captura")
            or raw_row.get("pedido_fecha_captura")
            or raw_row.get("fecha_captura")
        ),
        producto=raw_row.get("producto"),
        cantidad_pedida=raw_row.get("cantidad_pedida"),
        cantidad_asignada=raw_row.get("cantidad_asignada"),
        porcentaje_asignacion=raw_row.get("porcentaje_asignacion"),
        pedido_sugerido_texto=pedido_sugerido_texto,
        fecha_produccion=(
            raw_row.get("linea_fecha_produccion")
            or raw_row.get("pedido_fecha_produccion")
            or raw_row.get("fecha_produccion")
        ),
        color=raw_row.get("color"),
        sku=raw_row.get("sku"),
        descripcion=raw_row.get("descripcion"),
        prioridad_manual=raw_row.get("prioridad_manual"),
    )


def _update_linea_asignacion(conn: Any, linea: dict[str, Any], cantidad_delta: float) -> tuple[float, float]:
    cantidad_pedida = float(linea.get("cantidad_pedida") or 0)
    cantidad_asignada_actual = float(linea.get("cantidad_asignada") or 0)
    nueva_asignada = cantidad_asignada_actual + float(cantidad_delta)
    nuevo_faltante = cantidad_pedida - nueva_asignada
    if nuevo_faltante < 0:
        nuevo_faltante = 0.0

    conn.execute(
        """
        UPDATE pedido_lineas
        SET cantidad_asignada = ?,
            cantidad_faltante = ?
        WHERE id = ?
        """,
        (
            nueva_asignada,
            nuevo_faltante,
            linea["id"],
        ),
    )
    return nueva_asignada, nuevo_faltante



def _insert_inventario_movimiento(
    conn: Any,
    *,
    sku: str,
    movimiento_tipo: str,
    afecta_total: float,
    afecta_stock_libre: float,
    afecta_stock_asignado: float,
    referencia_tipo: str | None,
    referencia_id: int | None,
    observacion: str | None,
    created_by: str | None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO inventario_movimientos (
            sku,
            movimiento_tipo,
            afecta_total,
            afecta_stock_libre,
            afecta_stock_asignado,
            referencia_tipo,
            referencia_id,
            observacion,
            created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sku,
            movimiento_tipo,
            float(afecta_total or 0),
            float(afecta_stock_libre or 0),
            float(afecta_stock_asignado or 0),
            referencia_tipo,
            referencia_id,
            observacion,
            created_by,
        ),
    )
    return int(cursor.lastrowid)



def _create_audit(
    tabla: str,
    registro_id: str,
    campo: str,
    valor_anterior: Any,
    valor_nuevo: Any,
    usuario: str | None,
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


# =========================================================
# Consultas operativas
# =========================================================
def get_demanda_asistida_por_sku(
    sku: str,
    pedido_sugerido_texto: str | None = None,
) -> list[dict[str, Any]]:
    """
    Devuelve la demanda activa de un SKU para mostrar candidatos de asignación.

    Ajustes clave respecto al MVP:
    - incluye fecha_produccion
    - incluye color, SKU y descripción para claridad operativa
    - ordena por prioridad operativa real
    """
    normalized_sku = normalize_text(sku)
    if not normalized_sku:
        return []

    raw_rows = partidas_repository.list_lineas_activas_con_faltante_by_sku(normalized_sku)
    demanda_rows = [
        _build_demanda_row_operativo(row, pedido_sugerido_texto=pedido_sugerido_texto)
        for row in raw_rows
    ]
    return _sort_demanda_rows_operativo(demanda_rows)



def get_oferta_para_pedido_nuevo_lineas(
    lineas_data: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Para cada línea de un pedido nuevo, devuelve cuánto stock libre existe.
    No asigna nada automáticamente; solo asiste la decisión.
    """
    oferta_rows: list[dict[str, Any]] = []

    for linea in lineas_data:
        sku = normalize_text(linea.get("sku"))
        if not sku:
            continue

        stock_libre = asignaciones_repository.get_stock_libre_by_sku(sku)
        oferta_rows.append(
            build_oferta_row(
                sku=sku,
                producto=linea.get("producto"),
                cantidad_pedida=linea.get("cantidad_pedida", 0),
                stock_libre=stock_libre,
            )
        )

    return oferta_rows



def infer_sugerencia_desde_texto(raw_text: Any) -> dict[str, Any]:
    """
    Interpreta el texto original de sugerencia proveniente de producción.
    No decide nada; solo clasifica para mostrarlo al usuario.
    """
    tipo_sugerencia = infer_tipo_sugerencia_from_text(raw_text)
    pedido_sugerido_texto = extract_pedido_sugerido_text(raw_text)

    return {
        "sugerencia_destino_texto": normalize_text(raw_text),
        "tipo_sugerencia": tipo_sugerencia,
        "pedido_sugerido_texto": pedido_sugerido_texto,
    }


def get_modos_decision_mvp() -> list[str]:
    """
    Devuelve los modos operativos recomendados por el MVP.
    """
    return [
        MODO_DECISION_TODO_A_STOCK_LIBRE,
        MODO_DECISION_ACEPTAR_PROPUESTA,
        MODO_DECISION_REPARTIR_MANUAL,
        MODO_DECISION_DEJAR_PENDIENTE,
    ]


def _normalize_modo_decision_mvp(value: Any) -> str:
    return normalize_modo_decision_mvp_rule(value)


def _build_demanda_resumen_operativo(
    demanda_rows: list[dict[str, Any]],
    pedido_sugerido_texto: str | None = None,
) -> dict[str, Any]:
    pedido_sugerido_norm = normalize_text(pedido_sugerido_texto)
    total_faltante = 0.0
    total_pedida = 0.0
    total_asignada = 0.0
    filas_sugeridas = 0
    pedidos_unicos: set[str] = set()
    sugerencia_valida = False

    for row in demanda_rows:
        total_faltante += float(row.get("cantidad_faltante") or 0)
        total_pedida += float(row.get("cantidad_pedida") or 0)
        total_asignada += float(row.get("cantidad_asignada") or 0)
        if normalize_text(row.get("id_pedido")):
            pedidos_unicos.add(normalize_text(row.get("id_pedido")) or "")
        if row.get("es_pedido_sugerido"):
            filas_sugeridas += 1
            if float(row.get("cantidad_faltante") or 0) > 0:
                sugerencia_valida = True

    if not sugerencia_valida and pedido_sugerido_norm:
        sugerencia_valida = any(
            (normalize_text(row.get("id_pedido")) == pedido_sugerido_norm and float(row.get("cantidad_faltante") or 0) > 0)
            for row in demanda_rows
        )

    return {
        "pedidos_candidatos": len(pedidos_unicos),
        "filas_candidatas": len(demanda_rows),
        "filas_sugeridas": filas_sugeridas,
        "total_pedido": round(total_pedida, 6),
        "total_asignado": round(total_asignada, 6),
        "total_faltante": round(total_faltante, 6),
        "pedido_sugerido": pedido_sugerido_norm,
        "sugerencia_valida": bool(sugerencia_valida),
    }


def _build_acciones_operativas_mvp(
    demanda_rows: list[dict[str, Any]],
    stock_libre_actual: float,
    pedido_sugerido_texto: str | None = None,
) -> list[dict[str, Any]]:
    resumen = _build_demanda_resumen_operativo(
        demanda_rows=demanda_rows,
        pedido_sugerido_texto=pedido_sugerido_texto,
    )
    sugerencia_valida = bool(resumen.get("sugerencia_valida"))

    return [
        {
            "modo": MODO_DECISION_TODO_A_STOCK_LIBRE,
            "label": "Todo a stock libre",
            "descripcion": "Registrar toda la recepción como stock libre disponible.",
            "requiere_demanda": False,
            "requiere_captura_manual": False,
            "enabled": True,
        },
        {
            "modo": MODO_DECISION_ACEPTAR_PROPUESTA,
            "label": "Aceptar propuesta",
            "descripcion": "Aplicar la propuesta automática usando la prioridad operativa oficial del SKU.",
            "requiere_demanda": True,
            "requiere_captura_manual": False,
            "enabled": bool(demanda_rows),
            "pedido_sugerido": resumen.get("pedido_sugerido"),
            "sugerencia_valida": sugerencia_valida,
        },
        {
            "modo": MODO_DECISION_REPARTIR_MANUAL,
            "label": "Repartir manual",
            "descripcion": "Decidir manualmente cuánto asignar a uno o varios pedidos candidatos.",
            "requiere_demanda": True,
            "requiere_captura_manual": True,
            "enabled": bool(demanda_rows),
        },
        {
            "modo": MODO_DECISION_DEJAR_PENDIENTE,
            "label": "Dejar pendiente",
            "descripcion": "No mover inventario todavía y dejar la recepción pendiente de decisión.",
            "requiere_demanda": False,
            "requiere_captura_manual": False,
            "enabled": True,
        },
    ]


def _build_decision_pendiente(
    entrada_staging_id: int,
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


def _build_sugerencia_decisiones(
    entrada_staging_id: int,
    sku: str,
    cantidad_total: Any,
    demanda_rows: list[dict[str, Any]],
    created_by: str | None = None,
    observacion: str | None = None,
    remanente_a_pendiente: bool = False,
) -> list[dict[str, Any]]:
    """Convierte la propuesta automática oficial del SKU en decisiones persistibles."""
    propuesta = build_propuesta_automatica_para_sku(
        sku=sku,
        cantidad_recibida=cantidad_total,
        demanda_rows=demanda_rows,
    )
    decisiones: list[dict[str, Any]] = []
    for alloc in propuesta.get("allocations", []):
        decision_type = normalize_text(alloc.get("decision_type"))
        cantidad = float(alloc.get("cantidad") or 0)
        if cantidad <= 0:
            continue
        if decision_type == "ASIGNACION_PEDIDO":
            decisiones.append(
                build_decision_asignacion_pedido(
                    entrada_staging_id=entrada_staging_id,
                    pedido_id=int(alloc["pedido_id"]),
                    pedido_linea_id=int(alloc["pedido_linea_id"]),
                    sku=sku,
                    cantidad=cantidad,
                    observacion=observacion or "Aceptación de propuesta automática",
                    created_by=created_by,
                )
            )
        elif decision_type == "STOCK_LIBRE":
            if remanente_a_pendiente:
                decisiones.append(
                    _build_decision_pendiente(
                        entrada_staging_id=entrada_staging_id,
                        sku=sku,
                        cantidad=cantidad,
                        observacion=observacion or "Remanente pendiente por definición.",
                        created_by=created_by,
                    )
                )
            else:
                decisiones.append(
                    build_decision_stock_libre(
                        entrada_staging_id=entrada_staging_id,
                        sku=sku,
                        cantidad=cantidad,
                        observacion=observacion or "Sobrante de propuesta automática",
                        created_by=created_by,
                    )
                )
        elif decision_type == "PENDIENTE":
            decisiones.append(
                _build_decision_pendiente(
                    entrada_staging_id=entrada_staging_id,
                    sku=sku,
                    cantidad=cantidad,
                    observacion=observacion or "Pendiente desde propuesta automática",
                    created_by=created_by,
                )
            )
    return decisiones


def get_bandeja_operativa_por_sku(
    sku: str,
    cantidad_recibida: Any,
    pedido_sugerido_texto: str | None = None,
    producto: str | None = None,
    descripcion: str | None = None,
    entrada_staging_ids: list[int] | None = None,
) -> dict[str, Any]:
    """
    Construye la bandeja operativa del MVP para un SKU recibido.
    """
    normalized_sku = normalize_text(sku)
    if not normalized_sku:
        raise ValueError("SKU inválido para construir la bandeja operativa.")

    cantidad_num = validate_positive(cantidad_recibida, "cantidad_recibida")
    pedido_sugerido = normalize_text(pedido_sugerido_texto)
    demanda = get_demanda_asistida_por_sku(
        sku=normalized_sku,
        pedido_sugerido_texto=pedido_sugerido,
    )
    stock_libre_actual = float(asignaciones_repository.get_stock_libre_by_sku(normalized_sku) or 0)
    demanda_resumen = _build_demanda_resumen_operativo(demanda, pedido_sugerido_texto=pedido_sugerido)

    return {
        "sku": normalized_sku,
        "producto": normalize_text(producto),
        "descripcion": normalize_text(descripcion),
        "cantidad_recibida": round(cantidad_num, 6),
        "pedido_sugerido": pedido_sugerido,
        "stock_libre_actual": round(stock_libre_actual, 6),
        "demanda_activa": demanda,
        "demanda_resumen": demanda_resumen,
        "acciones_disponibles": _build_acciones_operativas_mvp(
            demanda_rows=demanda,
            stock_libre_actual=stock_libre_actual,
            pedido_sugerido_texto=pedido_sugerido,
        ),
        "modos_mvp": get_modos_decision_mvp(),
        "requiere_decision_usuario": True,
        "entrada_staging_ids": list(entrada_staging_ids or []),
    }


def build_decisiones_para_modo_mvp(
    entrada_staging_id: int,
    modo_decision: str,
    sku: str,
    cantidad_total: Any,
    pedido_sugerido_texto: str | None = None,
    decisiones_manuales: list[dict[str, Any]] | None = None,
    observacion: str | None = None,
    created_by: str | None = None,
) -> list[dict[str, Any]]:
    """
    Construye decisiones compatibles con la persistencia actual a partir de un modo del MVP.
    """
    raw_modo_decision = normalize_text(modo_decision)
    modo = _normalize_modo_decision_mvp(modo_decision)
    normalized_sku = normalize_text(sku)
    if not normalized_sku:
        raise ValueError("SKU inválido para construir decisiones del MVP.")

    cantidad_num = validate_positive(cantidad_total, "cantidad_total")

    if modo == MODO_DECISION_TODO_A_STOCK_LIBRE:
        return [
            build_decision_stock_libre(
                entrada_staging_id=entrada_staging_id,
                sku=normalized_sku,
                cantidad=cantidad_num,
                observacion=observacion,
                created_by=created_by,
            )
        ]

    if modo == MODO_DECISION_ACEPTAR_PROPUESTA:
        demanda = get_demanda_asistida_por_sku(
            sku=normalized_sku,
            pedido_sugerido_texto=pedido_sugerido_texto,
        )
        return _build_sugerencia_decisiones(
            entrada_staging_id=entrada_staging_id,
            sku=normalized_sku,
            cantidad_total=cantidad_num,
            demanda_rows=demanda,
            created_by=created_by,
            observacion=observacion,
            remanente_a_pendiente=(raw_modo_decision == "APLICAR_SUGERENCIA_VALIDA"),
        )

    if modo == MODO_DECISION_REPARTIR_MANUAL:
        manuales = list(decisiones_manuales or [])
        if not manuales:
            raise ValueError("La asignación manual asistida requiere decisiones_manuales.")
        normalized: list[dict[str, Any]] = []
        for item in manuales:
            payload = dict(item)
            payload.setdefault("entrada_staging_id", entrada_staging_id)
            payload.setdefault("sku", normalized_sku)
            payload.setdefault("created_by", created_by)
            if payload.get("decision_type") in (None, ""):
                payload["decision_type"] = "ASIGNACION_PEDIDO"
            normalized.append(validate_decision_payload(payload))
        validate_decisiones_payloads(cantidad_num, normalized)
        return normalized

    if modo == MODO_DECISION_DEJAR_PENDIENTE:
        return [
            _build_decision_pendiente(
                entrada_staging_id=entrada_staging_id,
                sku=normalized_sku,
                cantidad=cantidad_num,
                observacion=observacion,
                created_by=created_by,
            )
        ]

    raise ValueError(f"Modo de decisión no soportado: {modo}")


def save_decisiones_for_entrada_from_modo_mvp(
    entrada_staging_id: int,
    modo_decision: str,
    sku: str,
    cantidad_total: Any,
    pedido_sugerido_texto: str | None = None,
    decisiones_manuales: list[dict[str, Any]] | None = None,
    usuario: str | None = None,
    observacion: str | None = None,
    motivo_auditoria: str | None = None,
) -> list[int]:
    decisiones = build_decisiones_para_modo_mvp(
        entrada_staging_id=entrada_staging_id,
        modo_decision=modo_decision,
        sku=sku,
        cantidad_total=cantidad_total,
        pedido_sugerido_texto=pedido_sugerido_texto,
        decisiones_manuales=decisiones_manuales,
        observacion=observacion,
        created_by=usuario,
    )
    return save_decisiones_for_entrada(
        entrada_staging_id=entrada_staging_id,
        decisiones=decisiones,
        usuario=usuario,
        motivo_auditoria=motivo_auditoria or f"Modo MVP: {modo_decision}",
    )


# =========================================================
# Builders de decisiones
# =========================================================
def build_decision_stock_libre(
    entrada_staging_id: int,
    sku: str,
    cantidad: Any,
    observacion: str | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    return build_stock_libre_decision(
        entrada_staging_id=entrada_staging_id,
        sku=sku,
        cantidad=cantidad,
        observacion=observacion,
        created_by=created_by,
    )



def build_decision_asignacion_pedido(
    entrada_staging_id: int,
    pedido_id: int,
    pedido_linea_id: int,
    sku: str,
    cantidad: Any,
    observacion: str | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    return build_asignacion_pedido_decision(
        entrada_staging_id=entrada_staging_id,
        pedido_id=pedido_id,
        pedido_linea_id=pedido_linea_id,
        sku=sku,
        cantidad=cantidad,
        observacion=observacion,
        created_by=created_by,
    )


# =========================================================
# Validaciones y estado de entrada
# =========================================================
def get_entrada_disponible_restante(entrada_staging_id: int) -> float:
    """
    Devuelve la cantidad restante sin decidir de una entrada staging.
    """
    entrada = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if entrada is None:
        raise ValueError(f"No existe la entrada staging con id {entrada_staging_id}.")

    decisiones = entradas_repository.list_decisiones_by_entrada(entrada_staging_id)
    return calculate_decision_restante(
        cantidad_entrada=entrada.get("cantidad", 0),
        decisiones=decisiones,
    )



def validate_asignacion_manual_a_linea(
    pedido_linea_id: int,
    cantidad_a_asignar: Any,
    allow_overassignment: bool = False,
) -> dict[str, Any]:
    """
    Valida si una cantidad puede asignarse manualmente a una línea de pedido.
    """
    linea = partidas_repository.get_pedido_linea_by_id(pedido_linea_id)
    if linea is None:
        raise ValueError(f"No existe la línea con id {pedido_linea_id}.")

    pedido = pedidos_repository.get_pedido_by_id(int(linea["pedido_id"]))
    if pedido is None:
        raise ValueError(f"No existe el pedido de la línea {pedido_linea_id}.")

    if pedido["estado_operativo"] != "ACTIVO":
        raise ValueError("Solo se puede asignar producto a líneas de pedidos activos.")

    validate_asignacion_to_linea(
        cantidad_disponible=linea.get("cantidad_faltante", 0),
        cantidad_a_asignar=cantidad_a_asignar,
        cantidad_faltante_linea=linea.get("cantidad_faltante", 0),
        allow_overassignment=allow_overassignment,
    )

    return {
        "pedido": pedido,
        "linea": linea,
        "cantidad_a_asignar": float(cantidad_a_asignar),
    }


# =========================================================
# Guardado de decisiones
# =========================================================
def save_decisiones_for_entrada(
    entrada_staging_id: int,
    decisiones: list[dict[str, Any]],
    usuario: str | None = None,
    motivo_auditoria: str | None = None,
) -> list[int]:
    """
    Guarda las decisiones de una entrada staging.
    Reemplaza decisiones previas y marca la entrada como RESUELTA cuando la suma cuadra.
    """
    entrada = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if entrada is None:
        raise ValueError(f"No existe la entrada staging con id {entrada_staging_id}.")

    normalized_decisiones = validate_decisiones_payloads(
        cantidad_entrada=entrada.get("cantidad", 0),
        decisiones=decisiones,
    )

    inserted_ids: list[int] = []
    old_count = len(entradas_repository.list_decisiones_by_entrada(entrada_staging_id))

    with transaction() as conn:
        conn.execute(
            """
            DELETE FROM entrada_decisiones
            WHERE entrada_staging_id = ?
            """,
            (entrada_staging_id,),
        )

        for decision in normalized_decisiones:
            cursor = conn.execute(
                """
                INSERT INTO entrada_decisiones (
                    entrada_staging_id,
                    decision_type,
                    pedido_id,
                    pedido_linea_id,
                    sku,
                    cantidad,
                    observacion,
                    created_by
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entrada_staging_id,
                    decision["decision_type"],
                    decision.get("pedido_id"),
                    decision.get("pedido_linea_id"),
                    normalize_text(decision.get("sku")) or normalize_text(entrada.get("sku")),
                    float(decision["cantidad"]),
                    decision.get("observacion"),
                    decision.get("created_by") or usuario,
                ),
            )
            inserted_ids.append(int(cursor.lastrowid))

        conn.execute(
            """
            UPDATE entradas_staging
            SET estado_revision = 'RESUELTA'
            WHERE id = ?
            """,
            (entrada_staging_id,),
        )

    _create_audit(
        tabla="entradas_staging",
        registro_id=str(entrada_staging_id),
        campo="decisiones",
        valor_anterior=f"{old_count} decisiones",
        valor_nuevo=f"{len(inserted_ids)} decisiones guardadas",
        usuario=usuario,
        motivo=motivo_auditoria,
    )

    return inserted_ids


# =========================================================
# Aplicación de decisiones
# =========================================================
def apply_decisiones_to_inventory_and_pedidos(
    entrada_staging_id: int,
    usuario: str | None = None,
    motivo_auditoria: str | None = None,
) -> dict[str, Any]:
    """
    Aplica las decisiones guardadas de una entrada staging.

    Reglas respetadas:
    - la entrada física se registra primero como movimiento
    - solo con decisión confirmada impacta inventario/pedidos
    - permite dividir una entrada entre varios destinos
    - recalcula inventario por SKU y métricas del pedido afectado
    """
    entrada = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if entrada is None:
        raise ValueError(f"No existe la entrada staging con id {entrada_staging_id}.")

    if normalize_text(entrada.get("estado_revision")) != "RESUELTA":
        raise ValueError("La entrada staging debe estar RESUELTA antes de aplicar decisiones.")

    decisiones = entradas_repository.list_decisiones_by_entrada(entrada_staging_id)
    if not decisiones:
        raise ValueError("No existen decisiones para aplicar en esta entrada.")

    entrada_sku = normalize_text(entrada.get("sku"))
    entrada_descripcion = normalize_text(entrada.get("descripcion"))
    if not entrada_sku:
        raise ValueError("La entrada staging no tiene SKU válido para aplicar decisiones.")

    # Evita aplicar dos veces la misma entrada.
    existing_move = fetch_one(
        """
        SELECT id
        FROM inventario_movimientos
        WHERE referencia_tipo = 'ENTRADA_DECISION'
          AND referencia_id IN (
              SELECT id FROM entrada_decisiones WHERE entrada_staging_id = ?
          )
        LIMIT 1
        """,
        (entrada_staging_id,),
    )
    if existing_move is not None:
        raise ValueError(
            "Esta entrada ya tiene movimientos aplicados. Para reprocesarla se requiere limpieza controlada."
        )

    pedidos_tocados: set[int] = set()
    movimientos_ids: list[int] = []
    asignaciones_ids: list[int] = []
    sku_tocados: set[str] = set()

    with transaction() as conn:
        for decision in decisiones:
            decision_id = int(decision["id"])
            decision_type = normalize_text(decision["decision_type"]) or ""
            cantidad = validate_positive(decision.get("cantidad", 0), "cantidad")
            sku = normalize_text(decision.get("sku")) or entrada_sku
            observacion = decision.get("observacion")
            sku_tocados.add(sku)

            if decision_type == "STOCK_LIBRE":
                movimiento_id = _insert_inventario_movimiento(
                    conn,
                    sku=sku,
                    movimiento_tipo="ENTRADA_A_STOCK",
                    afecta_total=cantidad,
                    afecta_stock_libre=cantidad,
                    afecta_stock_asignado=0,
                    referencia_tipo="ENTRADA_DECISION",
                    referencia_id=decision_id,
                    observacion=observacion,
                    created_by=usuario,
                )
                movimientos_ids.append(movimiento_id)
                continue

            if decision_type == "ASIGNACION_PEDIDO":
                pedido_id = int(decision.get("pedido_id") or 0)
                pedido_linea_id = int(decision.get("pedido_linea_id") or 0)
                if not pedido_id or not pedido_linea_id:
                    raise ValueError(
                        f"La decisión {decision_id} requiere pedido_id y pedido_linea_id."
                    )

                linea = partidas_repository.get_pedido_linea_by_id(pedido_linea_id)
                if linea is None:
                    raise ValueError(
                        f"No existe la línea de pedido {pedido_linea_id} para la decisión {decision_id}."
                    )

                pedido = pedidos_repository.get_pedido_by_id(pedido_id)
                if pedido is None:
                    raise ValueError(
                        f"No existe el pedido {pedido_id} para la decisión {decision_id}."
                    )

                if normalize_text(pedido.get("estado_operativo")) != "ACTIVO":
                    raise ValueError(
                        f"No se puede asignar a un pedido no activo: {pedido.get('id_pedido')}."
                    )

                linea_sku = normalize_text(linea.get("sku"))
                if linea_sku and sku and linea_sku != sku:
                    raise ValueError(
                        f"La decisión {decision_id} usa SKU {sku}, pero la línea {pedido_linea_id} requiere {linea_sku}."
                    )

                validate_asignacion_to_linea(
                    cantidad_disponible=linea.get("cantidad_faltante", 0),
                    cantidad_a_asignar=cantidad,
                    cantidad_faltante_linea=linea.get("cantidad_faltante", 0),
                    allow_overassignment=False,
                )

                cursor_asig = conn.execute(
                    """
                    INSERT INTO asignaciones (
                        pedido_id,
                        pedido_linea_id,
                        sku,
                        cantidad,
                        fuente_asignacion,
                        entrada_staging_id,
                        decision_id,
                        estado,
                        observacion,
                        created_by
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pedido_id,
                        pedido_linea_id,
                        sku,
                        cantidad,
                        "ENTRADA_PRODUCCION",
                        entrada_staging_id,
                        decision_id,
                        "ACTIVA",
                        observacion,
                        usuario,
                    ),
                )
                asignacion_id = int(cursor_asig.lastrowid)
                asignaciones_ids.append(asignacion_id)

                # Registra la entrada física ya comprometida a stock asignado.
                movimiento_id = _insert_inventario_movimiento(
                    conn,
                    sku=sku,
                    movimiento_tipo="ENTRADA_A_STOCK",
                    afecta_total=cantidad,
                    afecta_stock_libre=0,
                    afecta_stock_asignado=cantidad,
                    referencia_tipo="ASIGNACION",
                    referencia_id=asignacion_id,
                    observacion=observacion,
                    created_by=usuario,
                )
                movimientos_ids.append(movimiento_id)

                _update_linea_asignacion(conn, linea, cantidad)
                pedidos_tocados.add(pedido_id)
                continue

            if decision_type in {"PENDIENTE", "IGNORADA"}:
                # No alteran inventario ni pedidos en esta etapa.
                continue

            raise ValueError(f"Tipo de decisión no soportado: {decision_type}")

    for sku in sorted(sku_tocados):
        asignaciones_repository.recalculate_inventario_sku_from_movimientos(
            sku=sku,
            descripcion=entrada_descripcion,
        )

    for pedido_id in sorted(pedidos_tocados):
        pedidos_repository.recalculate_pedido_metrics(pedido_id)

    _create_audit(
        tabla="entradas_staging",
        registro_id=str(entrada_staging_id),
        campo="aplicacion_decisiones",
        valor_anterior=None,
        valor_nuevo=(
            f"movimientos={len(movimientos_ids)}, "
            f"asignaciones={len(asignaciones_ids)}, "
            f"pedidos_tocados={len(pedidos_tocados)}"
        ),
        usuario=usuario,
        motivo=motivo_auditoria,
    )

    return {
        "entrada_staging_id": entrada_staging_id,
        "movimientos_ids": movimientos_ids,
        "asignaciones_ids": asignaciones_ids,
        "pedidos_tocados": sorted(pedidos_tocados),
        "sku_tocados": sorted(sku_tocados),
    }


# =========================================================
# Asignación manual desde stock libre
# =========================================================
def asignar_desde_stock_libre_a_linea(
    pedido_linea_id: int,
    cantidad: Any,
    usuario: str | None = None,
    observacion: str | None = None,
    motivo_auditoria: str | None = None,
) -> int:
    """
    Asigna producto desde stock libre a una línea de pedido activa.
    """
    cantidad_num = validate_positive(cantidad, "cantidad")

    linea = partidas_repository.get_pedido_linea_by_id(pedido_linea_id)
    if linea is None:
        raise ValueError(f"No existe la línea con id {pedido_linea_id}.")

    pedido = pedidos_repository.get_pedido_by_id(int(linea["pedido_id"]))
    if pedido is None:
        raise ValueError(f"No existe el pedido de la línea {pedido_linea_id}.")

    if normalize_text(pedido.get("estado_operativo")) != "ACTIVO":
        raise ValueError("Solo se puede asignar stock libre a pedidos activos.")

    sku = normalize_text(linea.get("sku"))
    if not sku:
        raise ValueError("La línea no tiene SKU válido.")

    stock_libre_actual = asignaciones_repository.get_stock_libre_by_sku(sku)

    validate_asignacion_to_linea(
        cantidad_disponible=stock_libre_actual,
        cantidad_a_asignar=cantidad_num,
        cantidad_faltante_linea=linea.get("cantidad_faltante", 0),
        allow_overassignment=False,
    )

    with transaction() as conn:
        cursor_asig = conn.execute(
            """
            INSERT INTO asignaciones (
                pedido_id,
                pedido_linea_id,
                sku,
                cantidad,
                fuente_asignacion,
                entrada_staging_id,
                decision_id,
                estado,
                observacion,
                created_by
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(pedido["id"]),
                int(pedido_linea_id),
                sku,
                cantidad_num,
                "STOCK_LIBRE",
                None,
                None,
                "ACTIVA",
                observacion,
                usuario,
            ),
        )
        asignacion_id = int(cursor_asig.lastrowid)

        _insert_inventario_movimiento(
            conn,
            sku=sku,
            movimiento_tipo="ASIGNACION_DESDE_STOCK",
            afecta_total=0,
            afecta_stock_libre=-cantidad_num,
            afecta_stock_asignado=cantidad_num,
            referencia_tipo="ASIGNACION",
            referencia_id=asignacion_id,
            observacion=observacion,
            created_by=usuario,
        )

        nueva_asignada, _ = _update_linea_asignacion(conn, linea, cantidad_num)

    asignaciones_repository.recalculate_inventario_sku_from_movimientos(
        sku=sku,
        descripcion=normalize_text(linea.get("descripcion")),
    )
    pedidos_repository.recalculate_pedido_metrics(int(pedido["id"]))

    _create_audit(
        tabla="pedido_lineas",
        registro_id=str(pedido_linea_id),
        campo="cantidad_asignada",
        valor_anterior=linea.get("cantidad_asignada"),
        valor_nuevo=nueva_asignada,
        usuario=usuario,
        motivo=motivo_auditoria or "Asignación desde stock libre",
    )

    return asignacion_id


# Alias de compatibilidad para evitar errores de import antiguos.
def asignar_desde_stock_libre_(
    pedido_linea_id: int,
    cantidad: Any,
    usuario: str | None = None,
    observacion: str | None = None,
    motivo_auditoria: str | None = None,
) -> int:
    return asignar_desde_stock_libre_a_linea(
        pedido_linea_id=pedido_linea_id,
        cantidad=cantidad,
        usuario=usuario,
        observacion=observacion,
        motivo_auditoria=motivo_auditoria,
    )


# =========================================================
# Resumen para UI de asignación
# =========================================================
def get_resumen_entrada_para_asignacion(entrada_staging_id: int) -> dict[str, Any]:
    """
    Devuelve una estructura práctica para la UI de asignación de una entrada.
    """
    entrada = entradas_repository.get_entrada_staging_by_id(entrada_staging_id)
    if entrada is None:
        raise ValueError(f"No existe la entrada staging con id {entrada_staging_id}.")

    sugerencia = {
        "tipo_sugerencia": entrada.get("tipo_sugerencia"),
        "pedido_sugerido_texto": entrada.get("pedido_sugerido_texto"),
        "sugerencia_destino_texto": entrada.get("sugerencia_destino_texto"),
    }

    demanda = get_demanda_asistida_por_sku(
        sku=entrada.get("sku"),
        pedido_sugerido_texto=entrada.get("pedido_sugerido_texto"),
    )

    decisiones = entradas_repository.list_decisiones_by_entrada(entrada_staging_id)
    restante = calculate_decision_restante(entrada.get("cantidad", 0), decisiones)

    return {
        "entrada": entrada,
        "sugerencia": sugerencia,
        "demanda": demanda,
        "demanda_resumen": _build_demanda_resumen_operativo(
            demanda_rows=demanda,
            pedido_sugerido_texto=entrada.get("pedido_sugerido_texto"),
        ),
        "decisiones": decisiones,
        "cantidad_restante": restante,
        "bandeja_operativa": get_bandeja_operativa_por_sku(
            sku=entrada.get("sku"),
            cantidad_recibida=entrada.get("cantidad", 0),
            pedido_sugerido_texto=entrada.get("pedido_sugerido_texto"),
            producto=entrada.get("producto"),
            descripcion=entrada.get("descripcion"),
            entrada_staging_ids=[entrada_staging_id],
        ),
    }


# =========================================================
# Helpers operativos por SKU para la nueva pantalla del MVP
# =========================================================
def _validate_non_negative_sku(value: Any, field_name: str) -> float:
    try:
        numeric = float(value or 0)
    except (TypeError, ValueError):
        raise ValueError(f'{field_name} debe ser numérico y no negativo.')
    if numeric < 0:
        raise ValueError(f'{field_name} no puede ser negativo.')
    return numeric


def get_pedidos_candidatos_para_sku(
    sku: str,
    pedido_sugerido_texto: str | None = None,
) -> list[dict[str, Any]]:
    """Alias explícito para la UI: devuelve la demanda activa/candidatos del SKU."""
    return get_demanda_asistida_por_sku(sku=sku, pedido_sugerido_texto=pedido_sugerido_texto)



def build_propuesta_automatica_para_sku(
    *,
    sku: str,
    cantidad_recibida: float,
    pedido_sugerido_texto: str | None = None,
    demanda_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Construye la propuesta automática del MVP para un SKU recibido:
    1) pedido sugerido
    2) otros pedidos activos del mismo SKU
    3) sobrante final a stock libre
    """
    normalized_sku = normalize_text(sku)
    total_qty = _validate_non_negative_sku(cantidad_recibida, 'cantidad_recibida')
    if not normalized_sku:
        raise ValueError('El SKU es obligatorio para construir la propuesta automática.')

    demanda = list(demanda_rows or get_demanda_asistida_por_sku(normalized_sku, pedido_sugerido_texto))
    demanda = _sort_demanda_rows_operativo(demanda)

    restante = total_qty
    asignaciones: list[dict[str, Any]] = []
    explicacion: list[str] = []

    for row in demanda:
        faltante = float(row.get('cantidad_faltante') or 0)
        if faltante <= 0 or restante <= 0:
            continue
        qty = min(faltante, restante)
        asignaciones.append(
            {
                'decision_type': 'ASIGNACION_PEDIDO',
                'pedido_id': row.get('pedido_id'),
                'pedido_linea_id': row.get('pedido_linea_id'),
                'sku': normalized_sku,
                'cantidad': round(qty, 6),
                'id_pedido': row.get('id_pedido'),
                'cliente': row.get('cliente'),
                'es_pedido_sugerido': bool(row.get('es_pedido_sugerido')),
                'destino_label': f"{normalize_text(row.get('id_pedido')) or 'SIN_PEDIDO'} · {normalize_text(row.get('cliente')) or 'SIN_CLIENTE'}",
            }
        )
        restante -= qty
        if row.get('es_pedido_sugerido'):
            explicacion.append(f"Sugerido {row.get('id_pedido')}: {round(qty, 6)}")
        else:
            explicacion.append(f"Pedido {row.get('id_pedido')}: {round(qty, 6)}")

    cantidad_stock_libre = round(max(restante, 0.0), 6)
    allocations = list(asignaciones)
    if cantidad_stock_libre > 0:
        allocations.append(
            {
                'decision_type': 'STOCK_LIBRE',
                'sku': normalized_sku,
                'cantidad': cantidad_stock_libre,
                'destino_label': 'Stock libre',
            }
        )
        explicacion.append(f"Stock libre: {cantidad_stock_libre}")

    return {
        'sku': normalized_sku,
        'cantidad_recibida': round(total_qty, 6),
        'demanda_total': round(sum(float(r.get('cantidad_faltante') or 0) for r in demanda), 6),
        'cantidad_a_pedidos': round(sum(float(a.get('cantidad') or 0) for a in asignaciones), 6),
        'cantidad_stock_libre': cantidad_stock_libre,
        'cantidad_pendiente': 0.0,
        'remanente_final': cantidad_stock_libre,
        'allocations': allocations,
        'explicacion_propuesta': ' | '.join(explicacion) if explicacion else 'Sin demanda activa; propuesta: stock libre.',
        'pedidos_candidatos': demanda,
    }



def validate_reparto_manual_para_sku(
    *,
    sku: str,
    cantidad_recibida: float,
    pedidos: list[dict[str, Any]] | None = None,
    cantidad_stock_libre: float = 0.0,
    cantidad_pendiente: float = 0.0,
) -> dict[str, Any]:
    """Valida un reparto manual del SKU entre pedidos, stock libre y pendiente."""
    normalized_sku = normalize_text(sku)
    if not normalized_sku:
        raise ValueError('El SKU es obligatorio en el reparto manual.')

    total = _validate_non_negative_sku(cantidad_recibida, 'cantidad_recibida')
    pedidos = list(pedidos or [])
    stock_qty = _validate_non_negative_sku(cantidad_stock_libre, 'cantidad_stock_libre')
    pendiente_qty = _validate_non_negative_sku(cantidad_pendiente, 'cantidad_pendiente')

    allocations: list[dict[str, Any]] = []
    total_pedidos = 0.0
    for item in pedidos:
        qty = _validate_non_negative_sku(item.get('cantidad', 0), 'cantidad pedido')
        if qty <= 0:
            continue
        pedido_id = item.get('pedido_id')
        pedido_linea_id = item.get('pedido_linea_id')
        if not pedido_id or not pedido_linea_id:
            raise ValueError('Cada reparto manual a pedido requiere pedido_id y pedido_linea_id.')
        allocations.append(
            {
                'decision_type': 'ASIGNACION_PEDIDO',
                'pedido_id': pedido_id,
                'pedido_linea_id': pedido_linea_id,
                'sku': normalized_sku,
                'cantidad': round(qty, 6),
                'destino_label': item.get('destino_label') or item.get('id_pedido') or str(pedido_linea_id),
            }
        )
        total_pedidos += qty

    if stock_qty > 0:
        allocations.append(
            {
                'decision_type': 'STOCK_LIBRE',
                'sku': normalized_sku,
                'cantidad': round(stock_qty, 6),
                'destino_label': 'Stock libre',
            }
        )
    if pendiente_qty > 0:
        allocations.append(
            {
                'decision_type': 'PENDIENTE',
                'sku': normalized_sku,
                'cantidad': round(pendiente_qty, 6),
                'destino_label': 'Pendiente',
            }
        )

    total_decidido = round(total_pedidos + stock_qty + pendiente_qty, 6)
    if abs(total_decidido - total) > 1e-6:
        raise ValueError(
            f'La suma del reparto manual ({total_decidido}) debe coincidir con la cantidad recibida ({round(total, 6)}).'
        )

    return {
        'sku': normalized_sku,
        'cantidad_recibida': round(total, 6),
        'cantidad_a_pedidos': round(total_pedidos, 6),
        'cantidad_stock_libre': round(stock_qty, 6),
        'cantidad_pendiente': round(pendiente_qty, 6),
        'allocations': allocations,
    }



def build_decision_pendiente_sku(*, sku: str, cantidad: float) -> dict[str, Any]:
    normalized_sku = normalize_text(sku)
    if not normalized_sku:
        raise ValueError('El SKU es obligatorio para dejar pendiente.')
    qty = _validate_non_negative_sku(cantidad, 'cantidad')
    return {
        'sku': normalized_sku,
        'cantidad_recibida': round(qty, 6),
        'cantidad_a_pedidos': 0.0,
        'cantidad_stock_libre': 0.0,
        'cantidad_pendiente': round(qty, 6),
        'allocations': [
            {
                'decision_type': 'PENDIENTE',
                'sku': normalized_sku,
                'cantidad': round(qty, 6),
                'destino_label': 'Pendiente',
            }
        ],
    }
