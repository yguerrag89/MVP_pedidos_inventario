from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

from wms_mvp.core.rules.cierres_rules import (
    build_cierre_payload,
    build_linea_update_for_cierre,
    calculate_total_enviado_en_cierre,
    extract_backorder_candidates,
    should_offer_backorder_creation,
    validate_cierre_consistency,
)
from wms_mvp.core.rules.pedidos_rules import (
    aggregate_lineas_metrics,
    build_id_pedido,
    build_linea_from_import_row,
    build_pedido_from_import_row,
    enrich_linea_with_pedido_context,
    infer_estado_operativo_from_cierre,
    normalize_date,
    normalize_pause_fields,
    normalize_text,
    validate_linea_quantities,
    validate_pedido_header,
    validate_tipo_cierre,
)
from wms_mvp.db.connection import column_exists, fetch_all, fetch_one, transaction
from wms_mvp.db.repositories import asignaciones_repository
from wms_mvp.db.repositories import auditoria_repository
from wms_mvp.db.repositories import pedidos_repository


# =========================================================
# Helpers generales
# =========================================================
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



def _to_optional_iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return normalize_date(value)



def _normalize_required_iso_date(value: Any, field_name: str) -> str:
    normalized = _to_optional_iso_date(value)
    if not normalized:
        raise ValueError(f"El campo '{field_name}' es obligatorio y debe contener una fecha válida.")
    return normalized


def _pedidos_supports_motivo_pausa() -> bool:
    return column_exists("pedidos", "motivo_pausa")


def _lineas_supports_motivo_pausa() -> bool:
    return column_exists("pedido_lineas", "motivo_pausa")


def _insert_pedido_row(conn: Any, pedido_payload: dict[str, Any], *, estado_operativo: str) -> int:
    columns = [
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
    values = [
        pedido_payload.get("id_pedido"),
        pedido_payload.get("serie_documento"),
        pedido_payload.get("documento"),
        pedido_payload.get("tipo_registro"),
        pedido_payload.get("origen_registro"),
        pedido_payload.get("import_job_id"),
        pedido_payload.get("pedido_origen_id"),
        pedido_payload.get("cliente"),
        pedido_payload.get("fecha_captura"),
        pedido_payload.get("fecha_produccion"),
        pedido_payload.get("fecha_logistica"),
        pedido_payload.get("estatus_venta"),
        pedido_payload.get("porcentaje_asignacion", 0),
        estado_operativo,
        pedido_payload.get("observaciones"),
    ]

    if _pedidos_supports_motivo_pausa():
        columns.append("motivo_pausa")
        values.append(pedido_payload.get("motivo_pausa"))

    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT INTO pedidos ({', '.join(columns)}) VALUES ({placeholders})"
    cursor = conn.execute(sql, tuple(values))
    return int(cursor.lastrowid)


def _insert_pedido_linea_row(
    conn: Any,
    *,
    pedido_id: int,
    line_no: int,
    linea_payload: dict[str, Any],
    cantidad_faltante: float | None = None,
) -> int:
    columns = [
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
    values = [
        pedido_id,
        line_no,
        linea_payload.get("sku"),
        linea_payload.get("descripcion"),
        linea_payload.get("producto"),
        linea_payload.get("color"),
        _to_float(linea_payload.get("cantidad_pedida")),
        _to_float(linea_payload.get("cantidad_asignada")),
        _to_float(linea_payload.get("cantidad_faltante") if cantidad_faltante is None else cantidad_faltante),
        _to_float(linea_payload.get("cantidad_enviada")),
        linea_payload.get("fecha_captura"),
        linea_payload.get("fecha_produccion"),
        linea_payload.get("fecha_logistica"),
        linea_payload.get("estatus_venta"),
        linea_payload.get("observaciones"),
    ]

    if _lineas_supports_motivo_pausa():
        columns.append("motivo_pausa")
        values.append(linea_payload.get("motivo_pausa"))

    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT INTO pedido_lineas ({', '.join(columns)}) VALUES ({placeholders})"
    cursor = conn.execute(sql, tuple(values))
    return int(cursor.lastrowid)



def _insert_inventario_movimiento(
    conn: Any,
    *,
    sku: str,
    movimiento_tipo: str,
    afecta_total: float,
    afecta_stock_libre: float,
    afecta_stock_asignado: float,
    referencia_tipo: str,
    referencia_id: int,
    observacion: str | None,
    created_by: str | None,
) -> None:
    conn.execute(
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
            afecta_total,
            afecta_stock_libre,
            afecta_stock_asignado,
            referencia_tipo,
            referencia_id,
            observacion,
            created_by,
        ),
    )



def _insert_snapshot_linea_cierre(
    conn: Any,
    *,
    pedido_cierre_id: int,
    pedido_linea_id: int,
    snapshot: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO pedido_linea_cierres (
            pedido_cierre_id,
            pedido_linea_id,
            sku,
            descripcion,
            producto,
            color,
            fecha_produccion,
            cantidad_pedida_original,
            cantidad_asignada_al_cierre,
            cantidad_enviada,
            cantidad_no_enviada
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pedido_cierre_id,
            pedido_linea_id,
            snapshot.get("sku"),
            snapshot.get("descripcion"),
            snapshot.get("producto"),
            snapshot.get("color"),
            snapshot.get("fecha_produccion"),
            _to_float(snapshot.get("cantidad_pedida_original")),
            _to_float(snapshot.get("cantidad_asignada_al_cierre")),
            _to_float(snapshot.get("cantidad_enviada")),
            _to_float(snapshot.get("cantidad_no_enviada")),
        ),
    )



def _clone_asignacion_with_new_state(
    conn: Any,
    asignacion: dict[str, Any],
    *,
    cantidad: float,
    estado: str,
    observacion: str | None,
) -> int:
    cursor = conn.execute(
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
            asignacion["pedido_id"],
            asignacion["pedido_linea_id"],
            asignacion["sku"],
            cantidad,
            asignacion.get("fuente_asignacion"),
            asignacion.get("entrada_staging_id"),
            asignacion.get("decision_id"),
            estado,
            observacion or asignacion.get("observacion"),
            asignacion.get("created_by"),
        ),
    )
    return int(cursor.lastrowid)



def _split_and_resolve_active_asignaciones_for_line(
    conn: Any,
    *,
    pedido_linea_id: int,
    cantidad_a_consumir: float,
    observacion: str | None,
) -> dict[str, Any]:
    """
    Parte y resuelve las asignaciones activas de una línea al cerrar.

    Resultado:
    - lo realmente enviado queda CONSUMIDA
    - lo que se libera del pedido cerrado queda CANCELADA
    """
    rows = conn.execute(
        """
        SELECT *
        FROM asignaciones
        WHERE pedido_linea_id = ?
          AND estado = 'ACTIVA'
        ORDER BY id ASC
        """,
        (pedido_linea_id,),
    ).fetchall()

    active_rows = [dict(row) for row in rows]
    if not active_rows:
        return {
            "consumidas_ids": [],
            "canceladas_ids": [],
            "total_activo": 0.0,
            "total_consumido": 0.0,
            "total_cancelado": 0.0,
        }

    total_activo = round(sum(_to_float(row.get("cantidad")) for row in active_rows), 6)
    restante_consumir = max(0.0, min(round(cantidad_a_consumir, 6), total_activo))
    restante_cancelar = max(0.0, round(total_activo - restante_consumir, 6))

    consumidas_ids: list[int] = []
    canceladas_ids: list[int] = []
    total_consumido = 0.0
    total_cancelado = 0.0

    for asignacion in active_rows:
        asignacion_id = int(asignacion["id"])
        cantidad = round(_to_float(asignacion.get("cantidad")), 6)
        if cantidad <= 0:
            conn.execute(
                "UPDATE asignaciones SET estado = 'CANCELADA' WHERE id = ?",
                (asignacion_id,),
            )
            canceladas_ids.append(asignacion_id)
            continue

        tomar_para_consumir = round(min(cantidad, restante_consumir), 6)
        restante_despues_consumo = round(cantidad - tomar_para_consumir, 6)
        tomar_para_cancelar = round(min(restante_despues_consumo, restante_cancelar), 6)
        sobrante = round(cantidad - tomar_para_consumir - tomar_para_cancelar, 6)
        if abs(sobrante) <= 1e-6:
            sobrante = 0.0

        if tomar_para_consumir > 0 and tomar_para_cancelar == 0:
            if abs(tomar_para_consumir - cantidad) <= 1e-6:
                conn.execute(
                    """
                    UPDATE asignaciones
                    SET estado = 'CONSUMIDA',
                        observacion = COALESCE(?, observacion)
                    WHERE id = ?
                    """,
                    (observacion, asignacion_id),
                )
                consumidas_ids.append(asignacion_id)
            else:
                conn.execute(
                    """
                    UPDATE asignaciones
                    SET cantidad = ?,
                        estado = 'CONSUMIDA',
                        observacion = COALESCE(?, observacion)
                    WHERE id = ?
                    """,
                    (tomar_para_consumir, observacion, asignacion_id),
                )
                consumidas_ids.append(asignacion_id)
                new_cancel_id = _clone_asignacion_with_new_state(
                    conn,
                    asignacion,
                    cantidad=round(cantidad - tomar_para_consumir, 6),
                    estado="CANCELADA",
                    observacion=observacion,
                )
                canceladas_ids.append(new_cancel_id)
                tomar_para_cancelar = round(cantidad - tomar_para_consumir, 6)

        elif tomar_para_consumir == 0 and tomar_para_cancelar > 0:
            conn.execute(
                """
                UPDATE asignaciones
                SET estado = 'CANCELADA',
                    observacion = COALESCE(?, observacion)
                WHERE id = ?
                """,
                (observacion, asignacion_id),
            )
            canceladas_ids.append(asignacion_id)

        elif tomar_para_consumir > 0 and tomar_para_cancelar > 0:
            conn.execute(
                """
                UPDATE asignaciones
                SET cantidad = ?,
                    estado = 'CONSUMIDA',
                    observacion = COALESCE(?, observacion)
                WHERE id = ?
                """,
                (tomar_para_consumir, observacion, asignacion_id),
            )
            consumidas_ids.append(asignacion_id)

            new_cancel_id = _clone_asignacion_with_new_state(
                conn,
                asignacion,
                cantidad=tomar_para_cancelar,
                estado="CANCELADA",
                observacion=observacion,
            )
            canceladas_ids.append(new_cancel_id)

            if sobrante > 0:
                _clone_asignacion_with_new_state(
                    conn,
                    asignacion,
                    cantidad=sobrante,
                    estado="CANCELADA",
                    observacion=observacion,
                )
                tomar_para_cancelar = round(tomar_para_cancelar + sobrante, 6)

        restante_consumir = round(max(0.0, restante_consumir - tomar_para_consumir), 6)
        restante_cancelar = round(max(0.0, restante_cancelar - tomar_para_cancelar), 6)
        total_consumido = round(total_consumido + tomar_para_consumir, 6)
        total_cancelado = round(total_cancelado + tomar_para_cancelar, 6)

    return {
        "consumidas_ids": consumidas_ids,
        "canceladas_ids": canceladas_ids,
        "total_activo": total_activo,
        "total_consumido": total_consumido,
        "total_cancelado": total_cancelado,
    }



def _insert_historic_pedido_line_and_snapshot(
    conn: Any,
    *,
    pedido_id: int,
    pedido_cierre_id: int,
    line_no: int,
    linea_payload: dict[str, Any],
) -> dict[str, Any]:
    pedido_linea_id = _insert_pedido_linea_row(
        conn,
        pedido_id=pedido_id,
        line_no=line_no,
        linea_payload=linea_payload,
    )

    snapshot = {
        "sku": linea_payload.get("sku"),
        "descripcion": linea_payload.get("descripcion"),
        "producto": linea_payload.get("producto"),
        "color": linea_payload.get("color"),
        "fecha_produccion": linea_payload.get("fecha_produccion"),
        "cantidad_pedida_original": _to_float(linea_payload.get("cantidad_pedida")),
        "cantidad_asignada_al_cierre": _to_float(linea_payload.get("cantidad_asignada")),
        "cantidad_enviada": _to_float(linea_payload.get("cantidad_enviada")),
        "cantidad_no_enviada": _to_float(linea_payload.get("cantidad_faltante")),
    }
    _insert_snapshot_linea_cierre(
        conn,
        pedido_cierre_id=pedido_cierre_id,
        pedido_linea_id=pedido_linea_id,
        snapshot=snapshot,
    )

    return {**linea_payload, "id": pedido_linea_id}



def _audit_many(rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        return
    auditoria_repository.create_many_auditoria_cambios(rows)


# =========================================================
# CONSULTA
# =========================================================
def get_pedido_enviado_full(pedido_id: int) -> dict[str, Any] | None:
    pedido_full = pedidos_repository.get_pedido_full(pedido_id)
    if pedido_full is None:
        return None

    cierre = fetch_one(
        """
        SELECT *
        FROM pedido_cierres
        WHERE pedido_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (pedido_id,),
    )

    lineas_cierre: list[dict[str, Any]] = []
    if cierre is not None:
        lineas_cierre = fetch_all(
            """
            SELECT *
            FROM pedido_linea_cierres
            WHERE pedido_cierre_id = ?
            ORDER BY id ASC
            """,
            (cierre["id"],),
        )

    return {
        "pedido": pedido_full["pedido"],
        "lineas": pedido_full["lineas"],
        "cierre": cierre,
        "lineas_cierre": lineas_cierre,
    }


# =========================================================
# CIERRE OPERATIVO DE PEDIDOS ACTIVOS
# =========================================================


def _build_inventory_impact_preview(lineas: list[dict[str, Any]], lineas_snapshot: list[dict[str, Any]]) -> dict[str, Any]:
    by_sku: dict[str, dict[str, Any]] = {}
    lineas_map = {int(_to_float(linea.get("id"), 0)): linea for linea in lineas if linea.get("id") is not None}

    for snapshot in lineas_snapshot:
        pedido_linea_id = int(_to_float(snapshot.get("pedido_linea_id"), 0))
        linea = lineas_map.get(pedido_linea_id, {})
        sku = normalize_text(snapshot.get("sku") or linea.get("sku"))
        if not sku:
            continue
        descripcion = normalize_text(snapshot.get("descripcion") or linea.get("descripcion"))
        cantidad_enviada = round(_to_float(snapshot.get("cantidad_enviada")), 6)
        cantidad_asignada_al_cierre = round(_to_float(snapshot.get("cantidad_asignada_al_cierre")), 6)
        cantidad_liberada = round(max(0.0, cantidad_asignada_al_cierre - cantidad_enviada), 6)

        bucket = by_sku.setdefault(
            sku,
            {
                "SKU": sku,
                "Descripción": descripcion,
                "Cantidad salida física": 0.0,
                "Cantidad liberada a stock": 0.0,
                "Afecta total": 0.0,
                "Afecta stock libre": 0.0,
                "Afecta stock asignado": 0.0,
            },
        )
        bucket["Cantidad salida física"] += cantidad_enviada
        bucket["Cantidad liberada a stock"] += cantidad_liberada
        bucket["Afecta total"] += -cantidad_enviada
        bucket["Afecta stock libre"] += cantidad_liberada
        bucket["Afecta stock asignado"] += -(cantidad_enviada + cantidad_liberada)

    rows = []
    for sku in sorted(by_sku):
        row = by_sku[sku]
        rows.append({
            "SKU": row["SKU"],
            "Descripción": row["Descripción"],
            "Cantidad salida física": round(_to_float(row["Cantidad salida física"]), 2),
            "Cantidad liberada a stock": round(_to_float(row["Cantidad liberada a stock"]), 2),
            "Afecta total": round(_to_float(row["Afecta total"]), 2),
            "Afecta stock libre": round(_to_float(row["Afecta stock libre"]), 2),
            "Afecta stock asignado": round(_to_float(row["Afecta stock asignado"]), 2),
        })

    return {
        "rows": rows,
        "sku_tocados": len(rows),
        "total_salida_fisica": round(sum(_to_float(r.get("Cantidad salida física")) for r in rows), 2),
        "total_liberado_stock": round(sum(_to_float(r.get("Cantidad liberada a stock")) for r in rows), 2),
    }


def preview_cierre_pedido(
    pedido_id: int,
    tipo_cierre: str,
    fecha_envio: str,
    usuario: str | None = None,
    observacion: str | None = None,
) -> dict[str, Any]:
    pedido_full = pedidos_repository.get_pedido_full(pedido_id)
    if pedido_full is None:
        raise ValueError(f"No existe el pedido con id {pedido_id}.")

    pedido = pedido_full["pedido"]
    lineas = pedido_full["lineas"]
    fecha_envio_iso = _normalize_required_iso_date(fecha_envio, "fecha_envio")

    cierre_payload = build_cierre_payload(
        pedido=pedido,
        lineas=lineas,
        tipo_cierre=tipo_cierre,
        fecha_envio=fecha_envio_iso,
        usuario=usuario,
        observacion=observacion,
    )
    validate_cierre_consistency(
        pedido=pedido,
        lineas=lineas,
        cierre_payload=cierre_payload,
    )

    return {
        "pedido": pedido,
        "lineas": lineas,
        "cierre_preview": cierre_payload,
        "inventario_impact_preview": _build_inventory_impact_preview(
            lineas=lineas,
            lineas_snapshot=cierre_payload["lineas_snapshot"],
        ),
        "ofrecer_backorder": should_offer_backorder_creation(
            cierre_payload["lineas_snapshot"]
        ),
        "backorder_candidates": extract_backorder_candidates(
            pedido=pedido,
            lineas=lineas,
            tipo_cierre=tipo_cierre,
        ),
    }



def cerrar_pedido(
    pedido_id: int,
    tipo_cierre: str,
    fecha_envio: str,
    usuario: str | None = None,
    observacion: str | None = None,
    motivo_auditoria: str | None = None,
) -> dict[str, Any]:
    pedido_full = pedidos_repository.get_pedido_full(pedido_id)
    if pedido_full is None:
        raise ValueError(f"No existe el pedido con id {pedido_id}.")

    pedido = pedido_full["pedido"]
    lineas = pedido_full["lineas"]

    fecha_envio_iso = _normalize_required_iso_date(fecha_envio, "fecha_envio")
    cierre_payload = build_cierre_payload(
        pedido=pedido,
        lineas=lineas,
        tipo_cierre=tipo_cierre,
        fecha_envio=fecha_envio_iso,
        usuario=usuario,
        observacion=observacion,
    )
    validate_cierre_consistency(
        pedido=pedido,
        lineas=lineas,
        cierre_payload=cierre_payload,
    )

    header = cierre_payload["header"]
    lineas_snapshot = cierre_payload["lineas_snapshot"]

    if fetch_one("SELECT id FROM pedido_cierres WHERE pedido_id = ?", (pedido_id,)) is not None:
        raise ValueError(f"El pedido {pedido.get('id_pedido')} ya tiene un cierre registrado.")

    sku_tocados: set[str] = set()
    asignaciones_consumidas: list[int] = []
    asignaciones_canceladas: list[int] = []
    pedido_cierre_id: int | None = None

    with transaction() as conn:
        cursor_cierre = conn.execute(
            """
            INSERT INTO pedido_cierres (
                pedido_id,
                tipo_cierre,
                fecha_envio,
                usuario,
                observacion
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                header["pedido_id"],
                header["tipo_cierre"],
                header["fecha_envio"],
                header.get("usuario"),
                header.get("observacion"),
            ),
        )
        pedido_cierre_id = int(cursor_cierre.lastrowid)

        for linea, snapshot in zip(lineas, lineas_snapshot, strict=True):
            _insert_snapshot_linea_cierre(
                conn,
                pedido_cierre_id=pedido_cierre_id,
                pedido_linea_id=int(snapshot["pedido_linea_id"]),
                snapshot=snapshot,
            )

            linea_updates = build_linea_update_for_cierre(
                linea=linea,
                tipo_cierre=header["tipo_cierre"],
            )
            conn.execute(
                """
                UPDATE pedido_lineas
                SET cantidad_enviada = ?,
                    cantidad_faltante = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    linea_updates["cantidad_enviada"],
                    linea_updates["cantidad_faltante"],
                    linea["id"],
                ),
            )

            cantidad_asignada_al_cierre = round(_to_float(snapshot["cantidad_asignada_al_cierre"]), 6)
            cantidad_enviada = round(_to_float(snapshot["cantidad_enviada"]), 6)
            cantidad_liberada = round(max(0.0, cantidad_asignada_al_cierre - cantidad_enviada), 6)
            sku = normalize_text(linea.get("sku"))

            if sku and cantidad_enviada > 0:
                sku_tocados.add(sku)
                _insert_inventario_movimiento(
                    conn,
                    sku=sku,
                    movimiento_tipo="SALIDA_DESDE_ASIGNADO",
                    afecta_total=-cantidad_enviada,
                    afecta_stock_libre=0,
                    afecta_stock_asignado=-cantidad_enviada,
                    referencia_tipo="PEDIDO_CIERRE",
                    referencia_id=pedido_cierre_id,
                    observacion=observacion,
                    created_by=usuario,
                )

            if sku and cantidad_liberada > 0:
                sku_tocados.add(sku)
                _insert_inventario_movimiento(
                    conn,
                    sku=sku,
                    movimiento_tipo="LIBERACION_ASIGNADO_POR_CIERRE",
                    afecta_total=0,
                    afecta_stock_libre=cantidad_liberada,
                    afecta_stock_asignado=-cantidad_liberada,
                    referencia_tipo="PEDIDO_CIERRE",
                    referencia_id=pedido_cierre_id,
                    observacion=observacion,
                    created_by=usuario,
                )

            resolucion = _split_and_resolve_active_asignaciones_for_line(
                conn,
                pedido_linea_id=int(linea["id"]),
                cantidad_a_consumir=cantidad_enviada,
                observacion=observacion,
            )
            asignaciones_consumidas.extend(resolucion["consumidas_ids"])
            asignaciones_canceladas.extend(resolucion["canceladas_ids"])

        conn.execute(
            """
            UPDATE pedidos
            SET estado_operativo = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                header["estado_operativo_destino"],
                pedido_id,
            ),
        )

    for sku in sku_tocados:
        asignaciones_repository.recalculate_inventario_sku_from_movimientos(sku=sku)

    _audit_many(
        [
            {
                "tabla": "pedidos",
                "registro_id": str(pedido_id),
                "campo": "estado_operativo",
                "valor_anterior": pedido.get("estado_operativo"),
                "valor_nuevo": header["estado_operativo_destino"],
                "usuario": usuario,
                "motivo": motivo_auditoria or "Cierre de pedido",
            },
            {
                "tabla": "pedido_cierres",
                "registro_id": str(pedido_cierre_id),
                "campo": "__created__",
                "valor_anterior": None,
                "valor_nuevo": f"tipo_cierre={header['tipo_cierre']}, fecha_envio={header['fecha_envio']}",
                "usuario": usuario,
                "motivo": motivo_auditoria or "Cierre de pedido",
            },
        ]
    )

    updated = get_pedido_enviado_full(pedido_id)
    if updated is None:
        raise ValueError(f"No se pudo recuperar el pedido enviado {pedido_id} tras el cierre.")

    return {
        "pedido_enviado": updated,
        "pedido_cierre_id": pedido_cierre_id,
        "total_enviado": calculate_total_enviado_en_cierre(lineas_snapshot),
        "sku_tocados": sorted(sku_tocados),
        "inventario_impact": _build_inventory_impact_preview(
            lineas=lineas,
            lineas_snapshot=lineas_snapshot,
        ),
        "asignaciones_consumidas": asignaciones_consumidas,
        "asignaciones_canceladas": asignaciones_canceladas,
        "backorder_candidates": extract_backorder_candidates(
            pedido=pedido,
            lineas=lineas,
            tipo_cierre=header["tipo_cierre"],
        ),
    }


# =========================================================
# ALTA MANUAL DE VENTA DIRECTA
# =========================================================
def create_venta_directa_enviada(
    pedido_data: dict[str, Any],
    lineas_data: list[dict[str, Any]],
    fecha_envio: str,
    usuario: str | None = None,
    observacion_cierre: str | None = None,
    motivo_auditoria: str | None = None,
) -> dict[str, Any]:
    if not lineas_data:
        raise ValueError("No se puede crear una venta directa sin líneas.")

    fecha_envio_iso = _normalize_required_iso_date(fecha_envio, "fecha_envio")

    pedido_payload = build_pedido_from_import_row(pedido_data)
    pedido_payload["tipo_registro"] = "VENTA_DIRECTA"
    pedido_payload["origen_registro"] = "ALTA_MANUAL"
    pedido_payload["estado_operativo"] = "ENVIADO_TOTAL"

    if not pedido_payload.get("id_pedido"):
        pedido_payload["id_pedido"] = build_id_pedido(
            pedido_payload.get("serie_documento"),
            pedido_payload.get("documento"),
        )

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
        raise ValueError(
            f"Ya existe un pedido/venta directa con id_pedido '{pedido_payload['id_pedido']}'."
        )

    normalized_lineas: list[dict[str, Any]] = []
    for index, raw_linea in enumerate(lineas_data, start=1):
        cantidad_pedida = _to_float(raw_linea.get("cantidad_pedida", raw_linea.get("cantidad_enviada", 0)))

        linea_payload = build_linea_from_import_row(
            {
                **raw_linea,
                "cantidad_pedida": cantidad_pedida,
                "cantidad_asignada": cantidad_pedida,
                "cantidad_faltante": 0,
                "cantidad_enviada": cantidad_pedida,
                "fecha_captura": raw_linea.get("fecha_captura") or pedido_payload.get("fecha_captura"),
                "fecha_produccion": raw_linea.get("fecha_produccion") or pedido_payload.get("fecha_produccion"),
                "fecha_logistica": raw_linea.get("fecha_logistica") or pedido_payload.get("fecha_logistica"),
                "estatus_venta": raw_linea.get("estatus_venta") or pedido_payload.get("estatus_venta"),
            },
            pedido_id=-1,
            line_no=index,
        )
        linea_payload = enrich_linea_with_pedido_context(linea_payload, pedido_payload)
        validate_linea_quantities(
            cantidad_pedida=linea_payload["cantidad_pedida"],
            cantidad_asignada=linea_payload["cantidad_asignada"],
            allow_overassignment=False,
        )
        normalized_lineas.append(linea_payload)

    metrics = aggregate_lineas_metrics(normalized_lineas)
    pedido_payload["porcentaje_asignacion"] = metrics["porcentaje_asignacion"]

    sku_tocados: set[str] = set()
    pedido_id: int | None = None
    pedido_cierre_id: int | None = None

    with transaction() as conn:
        pedido_id = _insert_pedido_row(
            conn,
            pedido_payload,
            estado_operativo="ENVIADO_TOTAL",
        )

        lineas_insertadas: list[dict[str, Any]] = []
        for line_no, linea in enumerate(normalized_lineas, start=1):
            pedido_linea_id = _insert_pedido_linea_row(
                conn,
                pedido_id=pedido_id,
                line_no=line_no,
                linea_payload=linea,
                cantidad_faltante=0,
            )
            lineas_insertadas.append({**linea, "id": pedido_linea_id})

        cursor_cierre = conn.execute(
            """
            INSERT INTO pedido_cierres (
                pedido_id,
                tipo_cierre,
                fecha_envio,
                usuario,
                observacion
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                pedido_id,
                "TOTAL",
                fecha_envio_iso,
                usuario,
                observacion_cierre,
            ),
        )
        pedido_cierre_id = int(cursor_cierre.lastrowid)

        for linea in lineas_insertadas:
            snapshot = {
                "sku": linea.get("sku"),
                "descripcion": linea.get("descripcion"),
                "producto": linea.get("producto"),
                "color": linea.get("color"),
                "fecha_produccion": linea.get("fecha_produccion"),
                "cantidad_pedida_original": _to_float(linea.get("cantidad_pedida")),
                "cantidad_asignada_al_cierre": _to_float(linea.get("cantidad_asignada")),
                "cantidad_enviada": _to_float(linea.get("cantidad_enviada")),
                "cantidad_no_enviada": 0,
            }
            _insert_snapshot_linea_cierre(
                conn,
                pedido_cierre_id=pedido_cierre_id,
                pedido_linea_id=int(linea["id"]),
                snapshot=snapshot,
            )

            cantidad = _to_float(linea.get("cantidad_enviada"))
            sku = normalize_text(linea.get("sku"))
            if sku and cantidad > 0:
                sku_tocados.add(sku)
                _insert_inventario_movimiento(
                    conn,
                    sku=sku,
                    movimiento_tipo="SALIDA_DESDE_STOCK",
                    afecta_total=-cantidad,
                    afecta_stock_libre=-cantidad,
                    afecta_stock_asignado=0,
                    referencia_tipo="PEDIDO_CIERRE",
                    referencia_id=pedido_cierre_id,
                    observacion=observacion_cierre,
                    created_by=usuario,
                )

    for sku in sku_tocados:
        asignaciones_repository.recalculate_inventario_sku_from_movimientos(sku=sku)

    _audit_many(
        [
            {
                "tabla": "pedidos",
                "registro_id": str(pedido_id),
                "campo": "__created__",
                "valor_anterior": None,
                "valor_nuevo": pedido_payload["id_pedido"],
                "usuario": usuario,
                "motivo": motivo_auditoria or "Alta manual de venta directa",
            },
            {
                "tabla": "pedido_cierres",
                "registro_id": str(pedido_cierre_id),
                "campo": "__created__",
                "valor_anterior": None,
                "valor_nuevo": f"tipo_cierre=TOTAL, fecha_envio={fecha_envio_iso}",
                "usuario": usuario,
                "motivo": motivo_auditoria or "Alta manual de venta directa",
            },
        ]
    )

    if pedido_id is None:
        raise ValueError("No se pudo crear la venta directa.")

    result = get_pedido_enviado_full(pedido_id)
    if result is None:
        raise ValueError(f"No se pudo recuperar la venta directa creada {pedido_id}.")

    return {
        "pedido_enviado": result,
        "pedido_cierre_id": pedido_cierre_id,
        "sku_tocados": sorted(sku_tocados),
    }


# =========================================================
# CARGA HISTÓRICA DE PEDIDOS ENVIADOS
# =========================================================
def _build_historic_enviado_payload(
    pedido_data: dict[str, Any],
    *,
    estado_destino: str,
    source_sheet: str | None = None,
) -> dict[str, Any]:
    """
    Normaliza la carga histórica de enviados para el MVP.

    Reglas de fase 3:
    - el histórico de la hoja 2026 entra al bloque de Enviados, no a Activos
    - origen_registro se fuerza a CARGA_INICIAL
    - el estado operativo nunca puede quedar ACTIVO en este flujo
    """
    if estado_destino not in {"ENVIADO_TOTAL", "ENVIADO_PARCIAL"}:
        raise ValueError(
            "La carga histórica de enviados solo puede crear pedidos en estado ENVIADO_TOTAL o ENVIADO_PARCIAL."
        )

    observaciones = normalize_text(pedido_data.get("observaciones"))
    if source_sheet:
        note = f"Carga histórica desde hoja {source_sheet}"
        observaciones = f"{observaciones} | {note}" if observaciones else note

    return build_pedido_from_import_row(
        {
            **pedido_data,
            "estado_operativo": estado_destino,
            "origen_registro": "CARGA_INICIAL",
            "tipo_registro": pedido_data.get("tipo_registro") or "PEDIDO_NORMAL",
            "observaciones": observaciones,
        }
    )


def create_pedido_enviado_historico_from_carga_inicial(
    pedido_data: dict[str, Any],
    lineas_data: list[dict[str, Any]],
    *,
    fecha_envio: str,
    tipo_cierre: str = "TOTAL",
    source_sheet: str = "2026",
    usuario: str | None = None,
    observacion_cierre: str | None = None,
    motivo_auditoria: str | None = None,
    registrar_movimientos: bool = False,
    allow_existing: bool = False,
) -> dict[str, Any]:
    """Wrapper explícito para la carga inicial de enviados históricos del MVP."""
    result = create_pedido_enviado_historico(
        {**pedido_data, "origen_registro": "CARGA_INICIAL"},
        lineas_data,
        fecha_envio=fecha_envio,
        tipo_cierre=tipo_cierre,
        usuario=usuario,
        observacion_cierre=observacion_cierre,
        motivo_auditoria=motivo_auditoria or "Carga inicial de enviados históricos",
        registrar_movimientos=registrar_movimientos,
        allow_existing=allow_existing,
        source_sheet=source_sheet,
    )
    result["destino_bloque"] = "ENVIADOS"
    result["visible_en_activos"] = False
    result["source_sheet"] = source_sheet
    return result

def create_pedido_enviado_historico(
    pedido_data: dict[str, Any],
    lineas_data: list[dict[str, Any]],
    *,
    fecha_envio: str,
    tipo_cierre: str = "TOTAL",
    usuario: str | None = None,
    observacion_cierre: str | None = None,
    motivo_auditoria: str | None = None,
    registrar_movimientos: bool = False,
    allow_existing: bool = False,
    source_sheet: str | None = None,
) -> dict[str, Any]:
    if not lineas_data:
        raise ValueError("No se puede crear un pedido histórico enviado sin líneas.")

    fecha_envio_iso = _normalize_required_iso_date(fecha_envio, "fecha_envio")
    normalized_tipo_cierre = validate_tipo_cierre(tipo_cierre)
    estado_destino = infer_estado_operativo_from_cierre(normalized_tipo_cierre)

    pedido_payload = _build_historic_enviado_payload(
        pedido_data,
        estado_destino=estado_destino,
        source_sheet=source_sheet,
    )

    if not pedido_payload.get("id_pedido"):
        pedido_payload["id_pedido"] = build_id_pedido(
            pedido_payload.get("serie_documento"),
            pedido_payload.get("documento"),
        )

    validate_pedido_header(
        id_pedido=pedido_payload["id_pedido"],
        cliente=pedido_payload["cliente"],
        estado_operativo=estado_destino,
        estatus_venta=pedido_payload.get("estatus_venta"),
        fecha_captura=pedido_payload.get("fecha_captura"),
        fecha_produccion=pedido_payload.get("fecha_produccion"),
        fecha_logistica=pedido_payload.get("fecha_logistica"),
        motivo_pausa=pedido_payload.get("motivo_pausa"),
    )

    if pedidos_repository.pedido_exists(pedido_payload["id_pedido"]):
        if allow_existing:
            existing = pedidos_repository.get_pedido_by_id_pedido(pedido_payload["id_pedido"])
            if existing is None:
                raise ValueError(f"El pedido '{pedido_payload['id_pedido']}' existe pero no pudo recuperarse.")
            result = get_pedido_enviado_full(int(existing["id"]))
            if result is None:
                raise ValueError(f"No se pudo recuperar el pedido histórico existente {pedido_payload['id_pedido']}.")
            return {
                "pedido_enviado": result,
                "pedido_cierre_id": result.get("cierre", {}).get("id") if result.get("cierre") else None,
                "sku_tocados": [],
                "existing": True,
            }
        raise ValueError(
            f"Ya existe un pedido/registro enviado con id_pedido '{pedido_payload['id_pedido']}'."
        )

    normalized_lineas: list[dict[str, Any]] = []
    for index, raw_linea in enumerate(lineas_data, start=1):
        cantidad_pedida = _to_float(raw_linea.get("cantidad_pedida"))
        cantidad_asignada = _to_float(raw_linea.get("cantidad_asignada", raw_linea.get("cantidad_enviada", 0)))
        cantidad_enviada = _to_float(raw_linea.get("cantidad_enviada", cantidad_asignada))
        cantidad_faltante = _to_float(raw_linea.get("cantidad_faltante", max(0.0, cantidad_pedida - cantidad_enviada)))

        if cantidad_asignada <= 0 and cantidad_enviada > 0:
            cantidad_asignada = cantidad_enviada

        linea_payload = build_linea_from_import_row(
            {
                **raw_linea,
                "cantidad_pedida": cantidad_pedida,
                "cantidad_asignada": cantidad_asignada,
                "cantidad_enviada": cantidad_enviada,
                "cantidad_faltante": cantidad_faltante,
                "fecha_captura": raw_linea.get("fecha_captura") or pedido_payload.get("fecha_captura"),
                "fecha_produccion": raw_linea.get("fecha_produccion") or pedido_payload.get("fecha_produccion"),
                "fecha_logistica": raw_linea.get("fecha_logistica") or pedido_payload.get("fecha_logistica"),
                "estatus_venta": raw_linea.get("estatus_venta") or pedido_payload.get("estatus_venta"),
            },
            pedido_id=-1,
            line_no=index,
        )
        linea_payload = enrich_linea_with_pedido_context(linea_payload, pedido_payload)
        normalized_lineas.append(linea_payload)

    metrics = aggregate_lineas_metrics(normalized_lineas)
    pedido_payload["porcentaje_asignacion"] = metrics["porcentaje_asignacion"]
    pedido_payload["estado_operativo"] = estado_destino
    pedido_payload["origen_registro"] = "CARGA_INICIAL"

    # Blindaje MVP fase 3: los históricos importados desde 2026 pertenecen a Enviados.
    if pedido_payload.get("estado_operativo") == "ACTIVO":
        raise ValueError("Un pedido histórico importado no puede quedar en estado ACTIVO.")

    pedido_id: int | None = None
    pedido_cierre_id: int | None = None
    sku_tocados: set[str] = set()

    with transaction() as conn:
        pedido_id = _insert_pedido_row(
            conn,
            pedido_payload,
            estado_operativo=estado_destino,
        )

        cursor_cierre = conn.execute(
            """
            INSERT INTO pedido_cierres (
                pedido_id,
                tipo_cierre,
                fecha_envio,
                usuario,
                observacion
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                pedido_id,
                normalized_tipo_cierre,
                fecha_envio_iso,
                normalize_text(usuario),
                normalize_text(observacion_cierre),
            ),
        )
        pedido_cierre_id = int(cursor_cierre.lastrowid)

        for line_no, linea in enumerate(normalized_lineas, start=1):
            inserted = _insert_historic_pedido_line_and_snapshot(
                conn,
                pedido_id=pedido_id,
                pedido_cierre_id=pedido_cierre_id,
                line_no=line_no,
                linea_payload=linea,
            )

            if registrar_movimientos:
                sku = normalize_text(inserted.get("sku"))
                cantidad_enviada = _to_float(inserted.get("cantidad_enviada"))
                if sku and cantidad_enviada > 0:
                    sku_tocados.add(sku)
                    _insert_inventario_movimiento(
                        conn,
                        sku=sku,
                        movimiento_tipo="SALIDA_HISTORICA_IMPORTADA",
                        afecta_total=-cantidad_enviada,
                        afecta_stock_libre=0,
                        afecta_stock_asignado=0,
                        referencia_tipo="PEDIDO_CIERRE",
                        referencia_id=pedido_cierre_id,
                        observacion=observacion_cierre,
                        created_by=usuario,
                    )

    for sku in sku_tocados:
        asignaciones_repository.recalculate_inventario_sku_from_movimientos(sku=sku)

    _audit_many(
        [
            {
                "tabla": "pedidos",
                "registro_id": str(pedido_id),
                "campo": "__created__",
                "valor_anterior": None,
                "valor_nuevo": pedido_payload["id_pedido"],
                "usuario": usuario,
                "motivo": motivo_auditoria or "Carga histórica de pedido enviado",
            },
            {
                "tabla": "pedido_cierres",
                "registro_id": str(pedido_cierre_id),
                "campo": "__created__",
                "valor_anterior": None,
                "valor_nuevo": f"tipo_cierre={normalized_tipo_cierre}, fecha_envio={fecha_envio_iso}",
                "usuario": usuario,
                "motivo": motivo_auditoria or "Carga histórica de pedido enviado",
            },
        ]
    )

    result = get_pedido_enviado_full(int(pedido_id))
    if result is None:
        raise ValueError(f"No se pudo recuperar el pedido histórico enviado {pedido_id}.")

    return {
        "pedido_enviado": result,
        "pedido_cierre_id": pedido_cierre_id,
        "sku_tocados": sorted(sku_tocados),
        "existing": False,
        "destino_bloque": "ENVIADOS",
        "visible_en_activos": False,
        "source_sheet": source_sheet,
        "estado_operativo": estado_destino,
    }
