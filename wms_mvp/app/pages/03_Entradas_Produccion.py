from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

PACKAGE_PARENT = Path(__file__).resolve().parents[3]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

import pandas as pd
import streamlit as st

from wms_mvp.app.bootstrap import bootstrap_runtime
from wms_mvp.core.services.asignacion_service import (
    apply_decisiones_to_inventory_and_pedidos,
    asignar_desde_stock_libre_a_linea,
    build_decision_asignacion_pedido,
    build_decision_stock_libre,
    build_propuesta_automatica_para_sku,
    get_demanda_asistida_por_sku,
    save_decisiones_for_entrada,
)
from wms_mvp.core.services.entradas_service import (
    list_import_jobs,
    mark_entrada_en_revision,
    mark_entrada_ignorada,
    update_entrada_staging_with_audit,
)
from wms_mvp.core.services.inventario_service import (
    get_entrada_produccion_job_impact,
    get_entrada_produccion_job_resumen,
)
from wms_mvp.core.services.reservas_service import get_contexto_reservas_por_sku
from wms_mvp.core.rules.pedidos_rules import normalize_text
from wms_mvp.db.connection import db_exists, fetch_all, fetch_one
from wms_mvp.db.repositories import asignaciones_repository, entradas_repository

PAGE_TITLE = "Entradas de Producción"
SESSION_USER_KEYS = ("wms_current_user", "current_user", "auth_user")
SESSION_ROLE_KEYS = ("wms_current_role", "current_role", "auth_role")
SESSION_ADMIN_KEYS = ("wms_admin_unlocked", "is_admin", "auth_is_admin")

VALID_REVIEW_STATES = {"EN_REVISION", "IGNORADA"}


# =========================================================
# Helpers base
# =========================================================
def _rerun() -> None:
    rerun_fn = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if rerun_fn:
        rerun_fn()


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    return int(_safe_float(value, default))


def _session_value(keys: tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        if key in st.session_state:
            return st.session_state.get(key)
    return default


def _current_user() -> str:
    user = _safe_text(_session_value(SESSION_USER_KEYS, ""))
    return user or "Consulta"


def _is_admin() -> bool:
    if bool(_session_value(SESSION_ADMIN_KEYS, False)):
        return True
    return _safe_text(_session_value(SESSION_ROLE_KEYS, "")).lower() == "admin"


def _normalize_date_text(value: Any) -> str:
    text = _safe_text(value)
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def _open_reservas_for_sku(sku: str, default_tab: str = "reasignar") -> None:
    st.session_state["reservas_prefill_sku"] = _safe_text(sku)
    st.session_state["reservas_prefill_tab"] = _safe_text(default_tab) or "reasignar"
    switch_page = getattr(st, "switch_page", None)
    if callable(switch_page):
        for page_path in ("app/pages/04_Reservas.py", "wms_mvp/app/pages/04_Reservas.py"):
            try:
                switch_page(page_path)
                return
            except Exception:
                continue
    st.info("SKU preparado para Reservas. Abre la página de Reservas y el sistema lo precargará automáticamente.")


# =========================================================
# Carga operativa por import_job
# =========================================================
def _list_recent_entrada_jobs(limit: int = 50) -> list[dict[str, Any]]:
    jobs = [row for row in list_import_jobs(limit=limit) if _safe_text(row.get("import_type")) == "ENTRADAS_PRODUCCION"]
    enriched: list[dict[str, Any]] = []
    for job in jobs:
        job_id = _safe_int(job.get("id"))
        counts = fetch_one(
            """
            SELECT
                COUNT(DISTINCT c.id) AS cortes,
                COUNT(es.id) AS staging_rows,
                COUNT(DISTINCT CASE WHEN COALESCE(es.sku, '') <> '' THEN es.sku END) AS skus,
                SUM(CASE WHEN es.estado_revision = 'PENDIENTE' THEN 1 ELSE 0 END) AS pendientes,
                SUM(CASE WHEN es.estado_revision = 'RESUELTA' THEN 1 ELSE 0 END) AS resueltas,
                SUM(CASE WHEN es.estado_revision = 'EN_REVISION' THEN 1 ELSE 0 END) AS en_revision,
                SUM(CASE WHEN es.estado_revision = 'IGNORADA' THEN 1 ELSE 0 END) AS ignoradas
            FROM cortes_entrada c
            LEFT JOIN entradas_staging es ON es.corte_id = c.id
            WHERE c.import_job_id = ?
            """,
            (job_id,),
        ) or {}
        enriched.append({**job, **counts})
    return enriched


def _build_import_job_label(job: dict[str, Any]) -> str:
    return (
        f"{_safe_int(job.get('id'))} | {_safe_text(job.get('file_name')) or 'Sin nombre'} | "
        f"filas={_safe_int(job.get('staging_rows'))} | sku={_safe_int(job.get('skus'))} | "
        f"estado={_safe_text(job.get('status')) or 'N/D'}"
    )


def _load_import_job_payload(import_job_id: int) -> dict[str, Any]:
    job = entradas_repository.get_import_job_by_id(import_job_id)
    if job is None:
        raise ValueError(f"No existe la entrada/importación con id {import_job_id}.")

    cortes = entradas_repository.list_cortes_by_import_job(import_job_id)
    entradas: list[dict[str, Any]] = []
    for corte in cortes:
        corte_rows = entradas_repository.list_entradas_staging_by_corte(_safe_int(corte.get("id")))
        for row in corte_rows:
            row["fecha_corte"] = corte.get("fecha_corte")
            row["hora_corte_normalizada"] = corte.get("hora_corte_normalizada")
            row["estado_corte"] = corte.get("estado_corte")
        entradas.extend(corte_rows)

    decisiones_by_entrada: dict[int, list[dict[str, Any]]] = {}
    for row in entradas:
        entrada_id = _safe_int(row.get("id"))
        decisiones_by_entrada[entrada_id] = entradas_repository.list_decisiones_by_entrada(entrada_id)

    return {
        "job": job,
        "cortes": cortes,
        "entradas": entradas,
        "decisiones_by_entrada": decisiones_by_entrada,
    }


def _entry_review_reasons(row: dict[str, Any]) -> list[str]:
    """
    Solo motivos realmente bloqueantes para la operación diaria.
    La sugerencia de producción es una ayuda, no un requisito para salir de revisión.
    """
    reasons: list[str] = []
    if not _safe_text(row.get("sku")):
        reasons.append("SKU vacío")
    if not _safe_text(row.get("descripcion")):
        reasons.append("Descripción vacía")
    if _safe_float(row.get("cantidad"), 0.0) <= 0:
        reasons.append("Cantidad inválida")
    if not _safe_text(row.get("fecha")):
        reasons.append("Fecha vacía")
    if not _safe_text(row.get("hora")):
        reasons.append("Hora vacía")
    duplicate_same = _safe_int(row.get("duplicate_count_same_corte"))
    duplicate_other = _safe_int(row.get("duplicate_count_other_cortes"))
    if duplicate_same > 1:
        reasons.append(f"Posible duplicado en el mismo corte ({duplicate_same} filas)")
    if duplicate_other > 0:
        reasons.append(f"Posible duplicado histórico ({duplicate_other} coincidencias)")
    return reasons


def _entry_total_decidido(decisiones: list[dict[str, Any]]) -> float:
    return round(sum(_safe_float(d.get("cantidad"), 0.0) for d in decisiones), 6)


def _entry_is_applied(entrada_id: int) -> bool:
    row = fetch_one(
        """
        SELECT 1 AS exists_flag
        FROM inventario_movimientos
        WHERE referencia_tipo = 'ENTRADA_DECISION'
          AND referencia_id IN (
              SELECT id FROM entrada_decisiones WHERE entrada_staging_id = ?
          )
        LIMIT 1
        """,
        (entrada_id,),
    )
    if row:
        return True

    row = fetch_one(
        """
        SELECT 1 AS exists_flag
        FROM asignaciones
        WHERE decision_id IN (
            SELECT id FROM entrada_decisiones WHERE entrada_staging_id = ?
        )
        LIMIT 1
        """,
        (entrada_id,),
    )
    return row is not None


def _group_entries_by_sku(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    decisiones_by_entrada = payload["decisiones_by_entrada"]
    grouped_ready: dict[str, dict[str, Any]] = {}
    grouped_review: dict[str, dict[str, Any]] = {}

    for row in payload["entradas"]:
        if _safe_text(row.get("estado_revision")) == "IGNORADA":
            continue

        sku = _safe_text(row.get("sku")) or "SIN_SKU"
        reasons = _entry_review_reasons(row)
        is_review = len(reasons) > 0
        bucket = grouped_review if is_review else grouped_ready

        if sku not in bucket:
            bucket[sku] = {
                "sku": sku,
                "producto": _safe_text(row.get("producto")),
                "descripcion": _safe_text(row.get("descripcion")),
                "cantidad_recibida": 0.0,
                "pedido_sugerido_texto": _safe_text(row.get("pedido_sugerido_texto")),
                "sugerencia_destino_texto": _safe_text(row.get("sugerencia_destino_texto")),
                "tipo_sugerencia": _safe_text(row.get("tipo_sugerencia")),
                "entradas": [],
                "review_reasons": set(),
                "total_decidido": 0.0,
                "total_stock_libre": 0.0,
                "total_pendiente": 0.0,
                "total_asignado_pedidos": 0.0,
                "fully_applied": True,
            }

        group = bucket[sku]
        qty = _safe_float(row.get("cantidad"), 0.0)
        group["cantidad_recibida"] += qty
        group["entradas"].append(row)
        group["producto"] = group["producto"] or _safe_text(row.get("producto"))
        group["descripcion"] = group["descripcion"] or _safe_text(row.get("descripcion"))
        group["pedido_sugerido_texto"] = group["pedido_sugerido_texto"] or _safe_text(row.get("pedido_sugerido_texto"))
        group["tipo_sugerencia"] = group["tipo_sugerencia"] or _safe_text(row.get("tipo_sugerencia"))
        group["review_reasons"].update(reasons)

        entry_id = _safe_int(row.get("id"))
        decisiones = decisiones_by_entrada.get(entry_id) or []
        group["total_decidido"] += _entry_total_decidido(decisiones)
        if decisiones:
            for decision in decisiones:
                decision_type = _safe_text(decision.get("decision_type"))
                decision_qty = _safe_float(decision.get("cantidad"), 0.0)
                if decision_type == "STOCK_LIBRE":
                    group["total_stock_libre"] += decision_qty
                elif decision_type == "ASIGNACION_PEDIDO":
                    group["total_asignado_pedidos"] += decision_qty
                elif decision_type == "PENDIENTE":
                    group["total_pendiente"] += decision_qty
        else:
            group["fully_applied"] = False

        if not _entry_is_applied(entry_id):
            group["fully_applied"] = False

    ready_groups = []
    for group in grouped_ready.values():
        group["cantidad_recibida"] = round(group["cantidad_recibida"], 6)
        group["total_decidido"] = round(group["total_decidido"], 6)
        group["total_stock_libre"] = round(group["total_stock_libre"], 6)
        group["total_asignado_pedidos"] = round(group["total_asignado_pedidos"], 6)
        group["total_pendiente"] = round(group["total_pendiente"], 6)
        group["estado_sku"] = _compute_sku_state(group)
        group["stock_libre_actual"] = asignaciones_repository.get_stock_libre_by_sku(group["sku"])
        group["demanda"] = get_demanda_asistida_por_sku(group["sku"], group["pedido_sugerido_texto"])
        group["demanda_activa_total"] = round(sum(_safe_float(r.get("cantidad_faltante"), 0.0) for r in group["demanda"]), 6)
        group["pedidos_candidatos_count"] = len(group["demanda"])
        group["contexto_reservas"] = get_contexto_reservas_por_sku(group["sku"], group["cantidad_recibida"])
        group["propuesta"] = _build_auto_propuesta(group)
        ready_groups.append(group)

    review_groups = []
    for group in grouped_review.values():
        group["cantidad_recibida"] = round(group["cantidad_recibida"], 6)
        group["review_reasons"] = sorted(reason for reason in group["review_reasons"] if reason)
        review_groups.append(group)

    ready_groups.sort(key=lambda g: (_estado_order(g["estado_sku"]), g["sku"]))
    review_groups.sort(key=lambda g: g["sku"])
    return ready_groups, review_groups


def _compute_sku_state(group: dict[str, Any]) -> str:
    qty = _safe_float(group.get("cantidad_recibida"), 0.0)
    decided = _safe_float(group.get("total_decidido"), 0.0)
    if decided <= 0:
        return "SIN_DECIDIR"
    if group.get("total_pendiente", 0.0) > 0:
        return "PENDIENTE"
    if abs(decided - qty) <= 1e-6:
        return "DECIDIDO" if group.get("fully_applied") else "PROPUESTA_GUARDADA"
    return "PARCIAL"


def _estado_order(state: str) -> int:
    order = {
        "SIN_DECIDIR": 0,
        "PROPUESTA_GUARDADA": 1,
        "PARCIAL": 2,
        "PENDIENTE": 3,
        "DECIDIDO": 4,
    }
    return order.get(state, 9)


# =========================================================
# Propuesta automática y persistencia
# =========================================================
def _ordered_demanda_rows(demanda: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list(demanda or [])


def _build_auto_propuesta(group: dict[str, Any]) -> dict[str, Any]:
    propuesta = build_propuesta_automatica_para_sku(
        sku=_safe_text(group.get("sku")),
        cantidad_recibida=_safe_float(group.get("cantidad_recibida"), 0.0),
        pedido_sugerido_texto=_safe_text(group.get("pedido_sugerido_texto")) or None,
        demanda_rows=_ordered_demanda_rows(group.get("demanda") or []),
    )
    propuesta["explicacion"] = (
        "Prioridad operativa: prioridad manual, estatus venta, fecha logística, "
        "fecha programación, fecha captura e id pedido. El pedido sugerido solo se muestra como referencia."
    )
    return propuesta


def _build_pending_allocations(sku: str, total_qty: float) -> list[dict[str, Any]]:
    return [{"decision_type": "PENDIENTE", "sku": sku, "cantidad": round(total_qty, 6), "destino_label": "Pendiente"}]


def _build_stock_only_allocations(sku: str, total_qty: float) -> list[dict[str, Any]]:
    return [{"decision_type": "STOCK_LIBRE", "sku": sku, "cantidad": round(total_qty, 6), "destino_label": "Stock libre"}]


def _expand_allocations_over_entries(
    entries: list[dict[str, Any]],
    allocations: list[dict[str, Any]],
    usuario: str,
    observacion: str | None,
) -> dict[int, list[dict[str, Any]]]:
    entries_sorted = sorted(entries, key=lambda r: (_safe_int(r.get("row_num")), _safe_int(r.get("id"))))
    alloc_queue = [{**alloc, "remaining": _safe_float(alloc.get("cantidad"), 0.0)} for alloc in allocations if _safe_float(alloc.get("cantidad"), 0.0) > 0]
    results: dict[int, list[dict[str, Any]]] = {}
    alloc_idx = 0

    for entry in entries_sorted:
        entry_id = _safe_int(entry.get("id"))
        row_remaining = _safe_float(entry.get("cantidad"), 0.0)
        row_decisions: list[dict[str, Any]] = []

        while row_remaining > 1e-9 and alloc_idx < len(alloc_queue):
            alloc = alloc_queue[alloc_idx]
            alloc_remaining = _safe_float(alloc.get("remaining"), 0.0)
            if alloc_remaining <= 1e-9:
                alloc_idx += 1
                continue

            take = min(row_remaining, alloc_remaining)
            decision_type = _safe_text(alloc.get("decision_type"))
            if decision_type == "STOCK_LIBRE":
                payload = build_decision_stock_libre(
                    entrada_staging_id=entry_id,
                    sku=_safe_text(entry.get("sku")),
                    cantidad=take,
                    observacion=observacion,
                    created_by=usuario,
                )
            elif decision_type == "ASIGNACION_PEDIDO":
                payload = build_decision_asignacion_pedido(
                    entrada_staging_id=entry_id,
                    pedido_id=_safe_int(alloc.get("pedido_id")),
                    pedido_linea_id=_safe_int(alloc.get("pedido_linea_id")),
                    sku=_safe_text(entry.get("sku")),
                    cantidad=take,
                    observacion=observacion,
                    created_by=usuario,
                )
            elif decision_type == "PENDIENTE":
                payload = {
                    "decision_type": "PENDIENTE",
                    "entrada_staging_id": entry_id,
                    "sku": _safe_text(entry.get("sku")),
                    "cantidad": round(take, 6),
                    "observacion": observacion,
                    "created_by": usuario,
                }
            else:
                raise ValueError(f"Tipo de decisión no soportado en la UI: {decision_type}")

            row_decisions.append(payload)
            row_remaining -= take
            alloc["remaining"] = alloc_remaining - take
            if alloc["remaining"] <= 1e-9:
                alloc_idx += 1

        if row_remaining > 1e-9:
            raise ValueError("La distribución por SKU no pudo descomponerse correctamente sobre las filas de staging.")
        results[entry_id] = row_decisions

    remaining_total = sum(_safe_float(a.get("remaining"), 0.0) for a in alloc_queue)
    if remaining_total > 1e-6:
        raise ValueError("La suma de destinos no coincide con la cantidad recibida del SKU.")

    return results


def _save_allocations_for_group(group: dict[str, Any], allocations: list[dict[str, Any]], usuario: str, observacion: str) -> None:
    expanded = _expand_allocations_over_entries(group["entradas"], allocations, usuario=usuario, observacion=observacion)
    for entry in group["entradas"]:
        entry_id = _safe_int(entry.get("id"))
        save_decisiones_for_entrada(
            entrada_staging_id=entry_id,
            decisiones=expanded[entry_id],
            usuario=usuario,
            motivo_auditoria=observacion,
        )


def _apply_all_for_group_entries(groups: list[dict[str, Any]], usuario: str) -> dict[str, int]:
    applied = 0
    skipped = 0
    failed = 0
    for group in groups:
        for entry in group.get("entradas") or []:
            entry_id = _safe_int(entry.get("id"))
            decisiones = entradas_repository.list_decisiones_by_entrada(entry_id)
            if not decisiones or _entry_is_applied(entry_id):
                skipped += 1
                continue
            try:
                apply_decisiones_to_inventory_and_pedidos(
                    entrada_staging_id=entry_id,
                    usuario=usuario,
                    motivo_auditoria="Aplicación masiva desde bandeja por SKU",
                )
                applied += 1
            except Exception:
                failed += 1
    return {"applied": applied, "skipped": skipped, "failed": failed}


# =========================================================
# UI builders
# =========================================================
def _render_page_header() -> None:
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)
    c1, c2 = st.columns(2)
    c1.metric("Modo actual", "Administrador" if _is_admin() else "Consulta")
    c2.metric("Usuario", _current_user())
    st.info(
        "Flujo sugerido: 1) elegir la entrada recibida, 2) revisar los SKU listos, 3) decidir qué va a pedidos, a stock libre o pendiente, 4) reaplicar desde stock libre cuando haga falta, 5) confirmar la aplicación final."
    )
    st.caption(
        "La carga del archivo sigue entrando desde Cargas y actualizaciones. Aquí te concentras solo en decidir cada SKU de forma clara y rápida."
    )


def _render_recent_jobs(jobs: list[dict[str, Any]]) -> int | None:
    st.subheader("1. Elige la entrada a trabajar")
    if not jobs:
        st.warning("No existen importaciones recientes de entradas de producción.")
        return None

    df = pd.DataFrame(
        [
            {
                "ID": _safe_int(j.get("id")),
                "Archivo": _safe_text(j.get("file_name")),
                "Estado": _safe_text(j.get("status")),
                "Cortes": _safe_int(j.get("cortes")),
                "Filas": _safe_int(j.get("staging_rows")),
                "SKU distintos": _safe_int(j.get("skus")),
                "Por decidir": _safe_int(j.get("pendientes")),
                "Listas": _safe_int(j.get("resueltas")),
                "En revisión": _safe_int(j.get("en_revision")),
                "Ignoradas": _safe_int(j.get("ignoradas")),
                "Inicio": _safe_text(j.get("started_at")),
            }
            for j in jobs
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)

    option_map = {_build_import_job_label(j): _safe_int(j.get("id")) for j in jobs}
    default_label = next(iter(option_map.keys()))
    selected_label = st.selectbox(
        "Entrada recibida",
        options=list(option_map.keys()),
        index=0,
        help="Trabajarás la asignación por SKU sobre la entrada seleccionada.",
    )
    return option_map.get(selected_label)


def _render_dashboard(payload: dict[str, Any], ready_groups: list[dict[str, Any]], review_groups: list[dict[str, Any]]) -> None:
    job = payload["job"]
    entries = payload["entradas"]
    total_qty = round(sum(_safe_float(r.get("cantidad"), 0.0) for r in entries), 2)

    st.subheader("2. Resumen de la entrada")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Entrada", _safe_int(job.get("id")))
    c2.metric("Archivo", _safe_text(job.get("file_name")) or "N/D")
    c3.metric("Filas recibidas", len(entries))
    c4.metric("SKU listos", len(ready_groups))
    c5.metric("SKU por corregir", len(review_groups))
    c6.metric("Piezas recibidas", total_qty)


def _build_propuesta_df(group: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for alloc in group.get("propuesta", {}).get("allocations", []):
        rows.append(
            {
                "Destino": _safe_text(alloc.get("destino_label")),
                "Tipo": _safe_text(alloc.get("decision_type")),
                "Cantidad": round(_safe_float(alloc.get("cantidad"), 0.0), 2),
                "Pedido sugerido": "Sí" if bool(alloc.get("es_pedido_sugerido")) else "",
            }
        )
    return pd.DataFrame(rows)


def _build_demanda_df(demanda: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Pedido": _safe_text(row.get("id_pedido")),
                "Cliente": _safe_text(row.get("cliente")),
                "Producto": _safe_text(row.get("producto")),
                "Color": _safe_text(row.get("color")),
                "Cantidad pedida": round(_safe_float(row.get("cantidad_pedida"), 0.0), 2),
                "Cantidad asignada": round(_safe_float(row.get("cantidad_asignada"), 0.0), 2),
                "Cantidad faltante": round(_safe_float(row.get("cantidad_faltante"), 0.0), 2),
                "Fecha captura": _safe_text(row.get("fecha_captura")),
                "Fecha logística": _safe_text(row.get("fecha_logistica")),
                "Fecha programación": _safe_text(row.get("fecha_produccion")),
                "%": round(_safe_float(row.get("porcentaje_asignacion"), 0.0), 2),
                "Estatus venta": _safe_text(row.get("estatus_venta")),
                "SKU": _safe_text(row.get("sku")),
                "Prioridad manual": _safe_text(row.get("prioridad_manual")),
                "Sugerido": "Sí" if bool(row.get("es_pedido_sugerido")) else "",
            }
            for row in demanda
        ]
    )


def _build_sugerencia_map(group: dict[str, Any]) -> dict[int, float]:
    suggested: dict[int, float] = {}
    for alloc in group.get("propuesta", {}).get("allocations", []):
        if _safe_text(alloc.get("decision_type")) != "ASIGNACION_PEDIDO":
            continue
        linea_id = _safe_int(alloc.get("pedido_linea_id"))
        if not linea_id:
            continue
        suggested[linea_id] = round(_safe_float(alloc.get("cantidad"), 0.0), 6)
    return suggested


def _build_demanda_editor_df(group: dict[str, Any]) -> pd.DataFrame:
    sugerida = _build_sugerencia_map(group)
    rows: list[dict[str, Any]] = []
    for row in group.get("demanda") or []:
        linea_id = _safe_int(row.get("pedido_linea_id"))
        rows.append(
            {
                "Pedido": _safe_text(row.get("id_pedido")),
                "Cliente": _safe_text(row.get("cliente")),
                "Producto": _safe_text(row.get("producto")),
                "Color": _safe_text(row.get("color")),
                "Cantidad pedida": round(_safe_float(row.get("cantidad_pedida"), 0.0), 2),
                "Cantidad asignada": round(_safe_float(row.get("cantidad_asignada"), 0.0), 2),
                "Cantidad faltante": round(_safe_float(row.get("cantidad_faltante"), 0.0), 2),
                "Fecha captura": _safe_text(row.get("fecha_captura")),
                "Fecha logística": _safe_text(row.get("fecha_logistica")),
                "Fecha programación": _safe_text(row.get("fecha_produccion")),
                "%": round(_safe_float(row.get("porcentaje_asignacion"), 0.0), 2),
                "Estatus venta": _safe_text(row.get("estatus_venta")),
                "SKU": _safe_text(row.get("sku")),
                "Prioridad manual": _safe_text(row.get("prioridad_manual")),
                "Cantidad sugerida": round(_safe_float(sugerida.get(linea_id), 0.0), 2),
                "Asignar ahora": 0.0,
            }
        )
    return pd.DataFrame(rows)


def _build_stock_libre_groups(limit: int = 1000) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for row in asignaciones_repository.list_inventario_disponible(limit=limit):
        sku = _safe_text(row.get("sku"))
        if not sku:
            continue
        demanda = get_demanda_asistida_por_sku(sku)
        stock_libre = round(_safe_float(row.get("stock_libre"), 0.0), 6)
        groups.append(
            {
                "sku": sku,
                "producto": _safe_text(row.get("producto")) or _safe_text(row.get("descripcion")),
                "descripcion": _safe_text(row.get("descripcion")),
                "stock_libre": stock_libre,
                "stock_asignado": round(_safe_float(row.get("stock_asignado"), 0.0), 6),
                "total_recibido": round(_safe_float(row.get("total_recibido"), 0.0), 6),
                "total_enviado": round(_safe_float(row.get("total_enviado"), 0.0), 6),
                "demanda": demanda,
                "demanda_activa_total": round(sum(_safe_float(r.get("cantidad_faltante"), 0.0) for r in demanda), 6),
                "pedidos_candidatos_count": len(demanda),
                "estado_stock": "CON_DEMANDA" if demanda else "SIN_DEMANDA",
                "propuesta_stock": _build_stock_suggestion_map(demanda, stock_libre),
            }
        )
    groups.sort(key=lambda g: (0 if g["estado_stock"] == "CON_DEMANDA" else 1, -g["stock_libre"], g["sku"]))
    return groups


def _build_stock_suggestion_map(demanda: list[dict[str, Any]], stock_libre: float) -> dict[int, float]:
    remaining = _safe_float(stock_libre, 0.0)
    suggested: dict[int, float] = {}
    for row in demanda:
        linea_id = _safe_int(row.get("pedido_linea_id"))
        faltante = _safe_float(row.get("cantidad_faltante"), 0.0)
        if not linea_id or remaining <= 0 or faltante <= 0:
            continue
        qty = min(faltante, remaining)
        suggested[linea_id] = round(qty, 6)
        remaining -= qty
    return suggested


def _build_stock_libre_table_df(groups: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "SKU": _safe_text(g.get("sku")),
                "Producto": _safe_text(g.get("producto")) or _safe_text(g.get("descripcion")),
                "Stock libre": round(_safe_float(g.get("stock_libre"), 0.0), 2),
                "Demanda activa": round(_safe_float(g.get("demanda_activa_total"), 0.0), 2),
                "Pedidos candidatos": _safe_int(g.get("pedidos_candidatos_count")),
                "Estado": _safe_text(g.get("estado_stock")),
            }
            for g in groups
        ]
    )


def _build_stock_group_option_label(group: dict[str, Any]) -> str:
    return (
        f"{_safe_text(group.get('sku'))} | "
        f"{_safe_text(group.get('producto')) or _safe_text(group.get('descripcion')) or 'Sin descripción'} | "
        f"stock libre={round(_safe_float(group.get('stock_libre'), 0.0), 2)} | "
        f"estado={_safe_text(group.get('estado_stock'))}"
    )


def _pick_stock_group(groups: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not groups:
        return None

    st.write("**SKU disponibles en stock libre**")
    st.dataframe(_build_stock_libre_table_df(groups), use_container_width=True, hide_index=True)

    option_map = {_build_stock_group_option_label(group): group for group in groups}
    labels = list(option_map.keys())
    selected_label = st.selectbox(
        "SKU de stock libre a trabajar",
        options=labels,
        index=0,
        help="Selecciona un SKU que ya esté en stock libre para asignarlo después a pedidos activos.",
    )
    return option_map.get(selected_label)


def _build_stock_libre_editor_df(group: dict[str, Any]) -> pd.DataFrame:
    sugerida = group.get("propuesta_stock") or {}
    rows: list[dict[str, Any]] = []
    for row in group.get("demanda") or []:
        linea_id = _safe_int(row.get("pedido_linea_id"))
        rows.append(
            {
                "Pedido": _safe_text(row.get("id_pedido")),
                "Cliente": _safe_text(row.get("cliente")),
                "Producto": _safe_text(row.get("producto")),
                "Color": _safe_text(row.get("color")),
                "Cantidad pedida": round(_safe_float(row.get("cantidad_pedida"), 0.0), 2),
                "Cantidad asignada": round(_safe_float(row.get("cantidad_asignada"), 0.0), 2),
                "Cantidad faltante": round(_safe_float(row.get("cantidad_faltante"), 0.0), 2),
                "Fecha captura": _safe_text(row.get("fecha_captura")),
                "Fecha logística": _safe_text(row.get("fecha_logistica")),
                "Fecha programación": _safe_text(row.get("fecha_produccion")),
                "%": round(_safe_float(row.get("porcentaje_asignacion"), 0.0), 2),
                "Estatus venta": _safe_text(row.get("estatus_venta")),
                "SKU": _safe_text(row.get("sku")),
                "Prioridad manual": _safe_text(row.get("prioridad_manual")),
                "Cantidad sugerida": round(_safe_float(sugerida.get(linea_id), 0.0), 2),
                "Asignar desde stock libre": 0.0,
            }
        )
    return pd.DataFrame(rows)


def _apply_stock_libre_assignments(group: dict[str, Any], assignments: list[dict[str, Any]]) -> int:
    applied = 0
    for item in assignments:
        cantidad = _safe_float(item.get("cantidad"), 0.0)
        if cantidad <= 0:
            continue
        asignar_desde_stock_libre_a_linea(
            pedido_linea_id=_safe_int(item.get("pedido_linea_id")),
            cantidad=cantidad,
            usuario=_current_user(),
            observacion=f"Asignación manual desde bandeja de stock libre para SKU {_safe_text(group.get('sku'))}",
            motivo_auditoria="Asignación desde stock libre en bandeja operativa",
        )
        applied += 1
    return applied


def _render_stock_group(group: dict[str, Any]) -> None:
    st.write("**Detalle operativo del SKU de stock libre**")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stock libre", round(_safe_float(group.get("stock_libre")), 2))
    c2.metric("Demanda activa", round(_safe_float(group.get("demanda_activa_total")), 2))
    c3.metric("Pedidos candidatos", _safe_int(group.get("pedidos_candidatos_count")))
    c4.metric("Stock asignado actual", round(_safe_float(group.get("stock_asignado")), 2))

    st.write("**SKU:**", _safe_text(group.get("sku")))
    st.write("**Producto:**", _safe_text(group.get("producto")) or _safe_text(group.get("descripcion")) or "Sin descripción")

    demanda = group.get("demanda") or []
    if not demanda:
        st.info("Este SKU tiene stock libre, pero no hay pedidos activos que hoy lo estén solicitando.")
        return

    st.write("**Pedidos activos que solicitan este SKU**")
    st.dataframe(_build_demanda_df(demanda), use_container_width=True, hide_index=True)

    if not _is_admin():
        st.caption("Desbloquea modo administrador para asignar stock libre a pedidos activos.")
        return

    if st.button("Aplicar sugerencia desde stock libre", key=f"stock_suggest_{group['sku']}"):
        try:
            suggested = [
                {"pedido_linea_id": linea_id, "cantidad": qty}
                for linea_id, qty in (group.get("propuesta_stock") or {}).items()
                if _safe_float(qty, 0.0) > 0
            ]
            if not suggested:
                raise ValueError("No hay sugerencia aplicable para este SKU desde stock libre.")
            applied = _apply_stock_libre_assignments(group, suggested)
            st.success(f"Sugerencia aplicada desde stock libre. Líneas afectadas: {applied}.")
            _rerun()
        except Exception as exc:
            st.error(f"No fue posible aplicar la sugerencia desde stock libre: {exc}")

    st.write("**Reparto manual desde stock libre**")
    st.caption("Captura cuánto mover desde stock libre a cada pedido. La suma no puede exceder el stock libre actual del SKU.")

    with st.form(f"stock_form_{group['sku']}"):
        editor_df = _build_stock_libre_editor_df(group)
        edited_df = st.data_editor(
            editor_df,
            use_container_width=True,
            hide_index=True,
            key=f"stock_editor_{group['sku']}",
            disabled=[
                "Pedido",
                "Cliente",
                "Producto",
                "Color",
                "Cantidad pedida",
                "Cantidad asignada",
                "Cantidad faltante",
                "Fecha captura",
                "Fecha logística",
                "Fecha programación",
                "%",
                "Estatus venta",
                "SKU",
                "Cantidad sugerida",
            ],
            column_config={
                "Asignar desde stock libre": st.column_config.NumberColumn(
                    "Asignar desde stock libre",
                    min_value=0.0,
                    step=1.0,
                    format="%.2f",
                ),
                "Cantidad sugerida": st.column_config.NumberColumn(
                    "Cantidad sugerida",
                    format="%.2f",
                ),
            },
        )

        assignments: list[dict[str, Any]] = []
        total_capture = 0.0
        for idx, row in enumerate(demanda):
            qty = _safe_float(edited_df.iloc[idx]["Asignar desde stock libre"], 0.0)
            faltante = _safe_float(row.get("cantidad_faltante"), 0.0)
            if qty > faltante + 1e-6:
                st.warning(f"{_safe_text(row.get('id_pedido'))}: lo capturado rebasa el faltante de {round(faltante, 2)}.")
            if qty > 0:
                assignments.append({"pedido_linea_id": _safe_int(row.get("pedido_linea_id")), "cantidad": round(qty, 6)})
                total_capture += qty

        remaining = round(_safe_float(group.get("stock_libre"), 0.0) - total_capture, 6)
        q1, q2, q3 = st.columns(3)
        q1.metric("Stock libre actual", round(_safe_float(group.get("stock_libre"), 0.0), 2))
        q2.metric("Total capturado", round(total_capture, 2))
        q3.metric("Stock libre restante", round(remaining, 2))

        submitted = st.form_submit_button("Asignar desde stock libre")
        if submitted:
            try:
                if not assignments:
                    raise ValueError("Debes capturar al menos una línea para asignar desde stock libre.")
                if total_capture - _safe_float(group.get("stock_libre"), 0.0) > 1e-6:
                    raise ValueError("La suma capturada excede el stock libre actual del SKU.")
                for idx, row in enumerate(demanda):
                    qty = _safe_float(edited_df.iloc[idx]["Asignar desde stock libre"], 0.0)
                    faltante = _safe_float(row.get("cantidad_faltante"), 0.0)
                    if qty > faltante + 1e-6:
                        raise ValueError(f"La captura para {_safe_text(row.get('id_pedido'))} rebasa el faltante permitido.")
                applied = _apply_stock_libre_assignments(group, assignments)
                st.success(f"Asignación desde stock libre completada. Líneas afectadas: {applied}.")
                _rerun()
            except Exception as exc:
                st.error(f"No fue posible asignar desde stock libre: {exc}")



def _build_ready_groups_table_df(groups: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "SKU": _safe_text(g.get("sku")),
                "Producto": _safe_text(g.get("producto")) or _safe_text(g.get("descripcion")),
                "Recibido": round(_safe_float(g.get("cantidad_recibida"), 0.0), 2),
                "Demanda activa": round(_safe_float(g.get("demanda_activa_total"), 0.0), 2),
                "Pedidos candidatos": _safe_int(g.get("pedidos_candidatos_count")),
                "Stock libre actual": round(_safe_float(g.get("stock_libre_actual"), 0.0), 2),
                "Estado": _safe_text(g.get("estado_sku")),
                "Pedido sugerido": _safe_text(g.get("pedido_sugerido_texto")),
            }
            for g in groups
        ]
    )


def _build_review_groups_table_df(groups: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "SKU": _safe_text(g.get("sku")),
                "Producto": _safe_text(g.get("producto")) or _safe_text(g.get("descripcion")),
                "Recibido": round(_safe_float(g.get("cantidad_recibida"), 0.0), 2),
                "Filas en revisión": len(g.get("entradas") or []),
                "Motivos": ", ".join(g.get("review_reasons") or []),
            }
            for g in groups
        ]
    )


def _build_review_group_option_label(group: dict[str, Any]) -> str:
    motivos = ", ".join(group.get("review_reasons") or []) or "Sin detalle"
    return (
        f"{_safe_text(group.get('sku'))} | "
        f"{_safe_text(group.get('producto')) or _safe_text(group.get('descripcion')) or 'Sin descripción'} | "
        f"filas={len(group.get('entradas') or [])} | "
        f"motivos={motivos}"
    )


def _pick_review_group(groups: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not groups:
        return None

    st.write("**Bandeja superior de revisión**")
    st.dataframe(_build_review_groups_table_df(groups), use_container_width=True, hide_index=True)

    option_map = {_build_review_group_option_label(group): group for group in groups}
    labels = list(option_map.keys())
    selected_label = st.selectbox(
        "SKU en revisión a trabajar",
        options=labels,
        index=0,
        help="Selecciona un SKU para revisar sus filas, corregir datos y devolverlo a operación o mandarlo directo a stock libre.",
    )
    return option_map.get(selected_label)


def _build_group_option_label(group: dict[str, Any]) -> str:
    return (
        f"{_safe_text(group.get('sku'))} | "
        f"{_safe_text(group.get('producto')) or _safe_text(group.get('descripcion')) or 'Sin descripción'} | "
        f"recibido={round(_safe_float(group.get('cantidad_recibida'), 0.0), 2)} | "
        f"estado={_safe_text(group.get('estado_sku'))}"
    )


def _pick_ready_group(groups: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not groups:
        return None

    st.write("**Bandeja superior por SKU**")
    st.dataframe(_build_ready_groups_table_df(groups), use_container_width=True, hide_index=True)

    option_map = {_build_group_option_label(group): group for group in groups}
    labels = list(option_map.keys())
    selected_label = st.selectbox(
        "SKU a trabajar",
        options=labels,
        index=0,
        help="Selecciona un SKU de la bandeja superior para ver abajo el detalle de pedidos y capturar la asignación.",
    )
    return option_map.get(selected_label)





def _build_reservas_context_df(rows: list[dict[str, Any]], role: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=[
            "Pedido", "Cliente", "Asignada", "Faltante", "Enviada", "Fecha logística", "Estatus venta"
        ])
    data = []
    for row in rows:
        data.append({
            "Pedido": _safe_text(row.get("id_pedido")),
            "Cliente": _safe_text(row.get("cliente")),
            "Asignada": round(_safe_float(row.get("cantidad_asignada"), 0.0), 2),
            "Faltante": round(_safe_float(row.get("cantidad_faltante"), 0.0), 2),
            "Enviada": round(_safe_float(row.get("cantidad_enviada"), 0.0), 2),
            "Fecha logística": _safe_text(row.get("fecha_logistica")),
            "Estatus venta": _safe_text(row.get("estatus_venta")),
        })
    return pd.DataFrame(data)


def _render_reservas_context(group: dict[str, Any]) -> None:
    ctx = group.get("contexto_reservas") or {}
    st.write("**Contexto de reservas del SKU**")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Reservado activo", round(_safe_float(ctx.get("reservado_activo_total"), 0.0), 2))
    c2.metric("Faltante activo", round(_safe_float(ctx.get("faltante_activo_total"), 0.0), 2))
    c3.metric("Cobertura de esta entrada", round(_safe_float(ctx.get("cobertura_entrada_sobre_faltante"), 0.0), 2))
    c4.metric("Excedente después de cubrir faltante", round(_safe_float(ctx.get("entrada_excedente"), 0.0), 2))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Pedidos con reserva", _safe_int(ctx.get("pedidos_con_reserva"), 0))
    c6.metric("Pedidos con faltante", _safe_int(ctx.get("pedidos_con_faltante"), 0))
    c7.metric("Líneas con reserva", _safe_int(ctx.get("lineas_con_reserva"), 0))
    c8.metric("Stock libre actual", round(_safe_float(ctx.get("stock_libre_actual"), 0.0), 2))

    if bool(ctx.get("requiere_reservas")):
        st.warning(
            "Este SKU ya tiene reserva activa y también líneas con faltante. Además de decidir la entrada, conviene revisar Reservas para posibles liberaciones o reasignaciones."
        )
    else:
        st.caption(
            "Este contexto ayuda a decidir si la entrada puede cubrir faltantes directamente o si conviene dejar parte a stock libre."
        )

    lineas_reserva = ctx.get("lineas_reserva") or []
    lineas_faltante = ctx.get("lineas_faltante") or []
    rc1, rc2 = st.columns(2)
    with rc1:
        st.write("**Pedidos que ya tienen reserva de este SKU**")
        if lineas_reserva:
            st.dataframe(_build_reservas_context_df(lineas_reserva, role="RESERVA"), use_container_width=True, hide_index=True)
        else:
            st.info("No hay líneas activas con reserva para este SKU.")
    with rc2:
        st.write("**Pedidos que todavía tienen faltante de este SKU**")
        if lineas_faltante:
            st.dataframe(_build_reservas_context_df(lineas_faltante, role="FALTANTE"), use_container_width=True, hide_index=True)
        else:
            st.info("No hay líneas activas con faltante para este SKU.")

    b1, b2 = st.columns(2)
    if b1.button("Preparar SKU en Reservas (asignar/liberar)", key=f"go_reservas_asignar_{group['sku']}"):
        _open_reservas_for_sku(group["sku"], default_tab="asignar")
    if b2.button("Preparar SKU en Reservas (reasignar)", key=f"go_reservas_reasignar_{group['sku']}"):
        _open_reservas_for_sku(group["sku"], default_tab="reasignar")

def _render_ready_group(group: dict[str, Any]) -> None:
    st.write("**Detalle operativo del SKU seleccionado**")
    header = (
        f"{group['sku']} · {_safe_text(group.get('producto')) or _safe_text(group.get('descripcion')) or 'Sin descripción'} "
        f"· recibido={round(_safe_float(group.get('cantidad_recibida')), 2)} · estado={group.get('estado_sku')}"
    )
    with st.expander(header, expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Cantidad recibida", round(_safe_float(group.get("cantidad_recibida")), 2))
        c2.metric("Demanda activa", round(_safe_float(group.get("demanda_activa_total")), 2))
        c3.metric("Pedidos candidatos", _safe_int(group.get("pedidos_candidatos_count")))
        c4.metric("Stock libre actual", round(_safe_float(group.get("stock_libre_actual")), 2))

        st.write("**Pedido sugerido por producción:**", _safe_text(group.get("pedido_sugerido_texto")) or "N/D")
        st.write("**Regla de propuesta:**", group.get("propuesta", {}).get("explicacion") or "N/D")

        propuesta_df = _build_propuesta_df(group)
        if not propuesta_df.empty:
            st.write("**Propuesta automática del sistema**")
            st.dataframe(propuesta_df, use_container_width=True, hide_index=True)
        else:
            st.info("No hay pedidos activos para este SKU. La opción natural es enviarlo a stock libre o dejarlo pendiente.")

        demanda_df = _build_demanda_df(group.get("demanda") or [])
        if not demanda_df.empty:
            st.write("**Detalle de pedidos que solicitan este SKU**")
            st.dataframe(demanda_df, use_container_width=True, hide_index=True)

        _render_reservas_context(group)

        st.write("**Estado actual del SKU en la entrada**")
        c5, c6, c7 = st.columns(3)
        c5.metric("Total a pedidos guardado", round(_safe_float(group.get("total_asignado_pedidos")), 2))
        c6.metric("Total a stock libre guardado", round(_safe_float(group.get("total_stock_libre")), 2))
        c7.metric("Total pendiente guardado", round(_safe_float(group.get("total_pendiente")), 2))

        if not _is_admin():
            st.caption("Desbloquea modo administrador para guardar o aplicar decisiones.")
            return

        button_col1, button_col2, button_col3, button_col4 = st.columns(4)
        if button_col1.button("Aceptar propuesta", key=f"accept_{group['sku']}"):
            try:
                _save_allocations_for_group(
                    group,
                    allocations=group["propuesta"]["allocations"],
                    usuario=_current_user(),
                    observacion="Propuesta automática aceptada desde bandeja por SKU",
                )
                st.success("Propuesta guardada correctamente para este SKU.")
                _rerun()
            except Exception as exc:
                st.error(f"No fue posible guardar la propuesta: {exc}")

        if button_col2.button("Todo a stock libre", key=f"stock_{group['sku']}"):
            try:
                _save_allocations_for_group(
                    group,
                    allocations=_build_stock_only_allocations(group["sku"], _safe_float(group.get("cantidad_recibida"))),
                    usuario=_current_user(),
                    observacion="Todo el SKU enviado a stock libre desde bandeja por SKU",
                )
                st.success("Decisión guardada: todo a stock libre.")
                _rerun()
            except Exception as exc:
                st.error(f"No fue posible guardar la decisión a stock libre: {exc}")

        if button_col3.button("Dejar pendiente", key=f"pending_{group['sku']}"):
            try:
                _save_allocations_for_group(
                    group,
                    allocations=_build_pending_allocations(group["sku"], _safe_float(group.get("cantidad_recibida"))),
                    usuario=_current_user(),
                    observacion="SKU recibido y dejado pendiente desde bandeja por SKU",
                )
                st.success("Decisión guardada: SKU pendiente de destino.")
                _rerun()
            except Exception as exc:
                st.error(f"No fue posible guardar la decisión pendiente: {exc}")

        if button_col4.button("Aplicar este SKU", key=f"apply_group_{group['sku']}"):
            result = _apply_all_for_group_entries([group], _current_user())
            if result["failed"]:
                st.error(f"Hubo errores al aplicar el SKU. Aplicadas: {result['applied']}, fallidas: {result['failed']}.")
            else:
                st.success(f"Aplicación completada. Filas aplicadas: {result['applied']} | omitidas: {result['skipped']}.")
            _rerun()

        st.write("**Captura manual por tabla**")
        st.caption(
            "En la columna 'Asignar ahora' captura cuánto enviar a cada pedido. La suma con stock libre y pendiente debe cuadrar exactamente con la cantidad recibida."
        )

        with st.form(f"manual_{group['sku']}"):
            editor_df = _build_demanda_editor_df(group)
            edited_df = st.data_editor(
                editor_df,
                use_container_width=True,
                hide_index=True,
                key=f"editor_{group['sku']}",
                disabled=[
                    "Pedido",
                    "Cliente",
                    "Producto",
                    "Color",
                    "Cantidad pedida",
                    "Cantidad asignada",
                    "Cantidad faltante",
                    "Fecha captura",
                    "Fecha logística",
                    "Fecha programación",
                    "%",
                    "Estatus venta",
                    "SKU",
                    "Cantidad sugerida",
                ],
                column_config={
                    "Asignar ahora": st.column_config.NumberColumn(
                        "Asignar ahora",
                        min_value=0.0,
                        step=1.0,
                        format="%.2f",
                    ),
                    "Prioridad manual": st.column_config.TextColumn(
                        "Prioridad manual",
                        help="Prioridad manual persistida del pedido cuando exista.",
                        disabled=True,
                        width="small",
                    ),
                    "Cantidad sugerida": st.column_config.NumberColumn(
                        "Cantidad sugerida",
                        format="%.2f",
                    ),
                },
            )

            stock_qty = st.number_input(
                "Cantidad a stock libre",
                min_value=0.0,
                value=0.0,
                step=1.0,
                key=f"manual_stock_{group['sku']}",
            )
            pending_qty = st.number_input(
                "Cantidad pendiente",
                min_value=0.0,
                value=0.0,
                step=1.0,
                key=f"manual_pending_{group['sku']}",
            )

            manual_allocs: list[dict[str, Any]] = []
            assigned_preview = 0.0
            demanda_rows = group.get("demanda") or []
            for idx, row in enumerate(demanda_rows):
                qty = _safe_float(edited_df.iloc[idx]["Asignar ahora"], 0.0)
                faltante = _safe_float(row.get("cantidad_faltante"), 0.0)
                if qty > faltante + 1e-6:
                    st.warning(
                        f"{_safe_text(row.get('id_pedido'))}: lo capturado rebasa el faltante de {round(faltante, 2)}."
                    )
                if qty > 0:
                    manual_allocs.append(
                        {
                            "decision_type": "ASIGNACION_PEDIDO",
                            "pedido_id": _safe_int(row.get("pedido_id")),
                            "pedido_linea_id": _safe_int(row.get("pedido_linea_id")),
                            "sku": group["sku"],
                            "cantidad": round(float(qty), 6),
                            "destino_label": _safe_text(row.get("id_pedido")),
                        }
                    )
                    assigned_preview += float(qty)

            if stock_qty > 0:
                manual_allocs.append(
                    {
                        "decision_type": "STOCK_LIBRE",
                        "sku": group["sku"],
                        "cantidad": round(float(stock_qty), 6),
                        "destino_label": "Stock libre",
                    }
                )
                assigned_preview += float(stock_qty)

            if pending_qty > 0:
                manual_allocs.append(
                    {
                        "decision_type": "PENDIENTE",
                        "sku": group["sku"],
                        "cantidad": round(float(pending_qty), 6),
                        "destino_label": "Pendiente",
                    }
                )
                assigned_preview += float(pending_qty)

            total_qty = _safe_float(group.get("cantidad_recibida"), 0.0)
            remaining_preview = round(total_qty - assigned_preview, 6)
            p1, p2, p3 = st.columns(3)
            p1.metric("Cantidad recibida", round(total_qty, 2))
            p2.metric("Total capturado", round(assigned_preview, 2))
            p3.metric("Pendiente por cuadrar", round(remaining_preview, 2))

            submitted = st.form_submit_button("Guardar reparto manual")
            if submitted:
                try:
                    if not manual_allocs:
                        raise ValueError("Debes capturar al menos un destino manual.")
                    for idx, row in enumerate(demanda_rows):
                        qty = _safe_float(edited_df.iloc[idx]["Asignar ahora"], 0.0)
                        faltante = _safe_float(row.get("cantidad_faltante"), 0.0)
                        if qty > faltante + 1e-6:
                            raise ValueError(
                                f"La captura para {_safe_text(row.get('id_pedido'))} rebasa el faltante permitido."
                            )
                    if abs(total_qty - assigned_preview) > 1e-6:
                        raise ValueError("La suma del reparto manual no coincide con la cantidad recibida del SKU.")
                    _save_allocations_for_group(
                        group,
                        allocations=manual_allocs,
                        usuario=_current_user(),
                        observacion="Reparto manual desde bandeja por SKU",
                    )
                    st.success("Reparto manual guardado correctamente.")
                    _rerun()
                except Exception as exc:
                    st.error(f"No fue posible guardar el reparto manual: {exc}")


def _render_review_group(group: dict[str, Any]) -> None:
    st.write(
        f"**SKU en revisión:** {_safe_text(group.get('sku'))} · "
        f"{_safe_text(group.get('producto')) or _safe_text(group.get('descripcion')) or 'Sin descripción'} · "
        f"recibido={round(_safe_float(group.get('cantidad_recibida')), 2)}"
    )
    st.write("**Motivos de revisión:**", ", ".join(group.get("review_reasons") or ["Sin detalle"]))
    df = pd.DataFrame(
        [
            {
                "Entrada": _safe_int(row.get("id")),
                "Fila": _safe_int(row.get("row_num")),
                "SKU": _safe_text(row.get("sku")),
                "Descripción": _safe_text(row.get("descripcion")),
                "Cantidad": round(_safe_float(row.get("cantidad"), 0.0), 2),
                "Fecha": _safe_text(row.get("fecha")),
                "Hora": _safe_text(row.get("hora")),
                "Tipo sugerencia": _safe_text(row.get("tipo_sugerencia")),
                "Pedido sugerido": _safe_text(row.get("pedido_sugerido_texto")),
                "Estado": _safe_text(row.get("estado_revision")),
            }
            for row in group.get("entradas") or []
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)

    if not _is_admin():
        st.caption("Desbloquea modo administrador para corregir, devolver a operación o mandar a stock libre una fila en revisión.")
        return

    st.write("**Corrección rápida por fila**")
    st.caption("Puedes corregir la fila y devolverla a operación, o corregirla y mandarla directo a stock libre si ya está lista.")
    for row in group.get("entradas") or []:
        entry_id = _safe_int(row.get("id"))
        with st.container(border=True):
            st.caption(
                f"Entrada {entry_id} · fila {_safe_int(row.get('row_num'))} · motivos: {', '.join(_entry_review_reasons(row)) or 'Sin motivos'}"
            )
            c1, c2, c3 = st.columns(3)
            sku = c1.text_input("SKU", value=_safe_text(row.get("sku")), key=f"rev_sku_{entry_id}")
            descripcion = c2.text_input("Descripción", value=_safe_text(row.get("descripcion")), key=f"rev_desc_{entry_id}")
            cantidad = c3.number_input(
                "Cantidad",
                min_value=0.0,
                value=float(_safe_float(row.get("cantidad"), 0.0)),
                step=1.0,
                key=f"rev_qty_{entry_id}",
            )
            c4, c5, c6 = st.columns(3)
            fecha = c4.text_input("Fecha", value=_safe_text(row.get("fecha")), key=f"rev_fecha_{entry_id}")
            hora = c5.text_input("Hora", value=_safe_text(row.get("hora")), key=f"rev_hora_{entry_id}")
            sugerencia = c6.text_input(
                "Sugerencia destino",
                value=_safe_text(row.get("sugerencia_destino_texto")),
                key=f"rev_sug_{entry_id}",
            )

            proposed_changes = {
                "sku": sku,
                "descripcion": descripcion,
                "cantidad": cantidad,
                "fecha": fecha,
                "hora": hora,
                "sugerencia_destino_texto": sugerencia,
                "estado_revision": "PENDIENTE",
            }

            preview_row = dict(row)
            preview_row.update(proposed_changes)
            preview_reasons = _entry_review_reasons(preview_row)
            if preview_reasons:
                st.warning("Seguiría en revisión con estos datos: " + ", ".join(preview_reasons))
            else:
                st.success("Con estos datos la fila quedaría lista para operación.")

            b1, b2, b3, b4 = st.columns(4)
            if b1.button("Guardar y devolver a operación", key=f"rev_save_{entry_id}"):
                try:
                    updated = update_entrada_staging_with_audit(
                        entrada_staging_id=entry_id,
                        changes=proposed_changes,
                        usuario=_current_user(),
                        motivo="Corrección desde bandeja de revisión",
                    )
                    if _entry_review_reasons(updated):
                        st.warning("La fila se guardó, pero aún tiene errores y seguirá en revisión.")
                    else:
                        st.success("Fila corregida y devuelta a la bandeja operativa.")
                    _rerun()
                except Exception as exc:
                    st.error(f"No fue posible guardar la corrección: {exc}")

            if b2.button("Corregir y mandar a stock libre", key=f"rev_stock_{entry_id}"):
                try:
                    if preview_reasons:
                        raise ValueError("Antes de mandar a stock libre debes corregir todos los errores bloqueantes de la fila.")
                    updated = update_entrada_staging_with_audit(
                        entrada_staging_id=entry_id,
                        changes=proposed_changes,
                        usuario=_current_user(),
                        motivo="Corrección y envío directo a stock libre",
                    )
                    if _entry_review_reasons(updated):
                        raise ValueError("La fila todavía presenta errores bloqueantes después de la corrección.")
                    decision = build_decision_stock_libre(
                        entrada_staging_id=entry_id,
                        sku=_safe_text(updated.get("sku")),
                        cantidad=_safe_float(updated.get("cantidad"), 0.0),
                        observacion="Salida directa desde revisión a stock libre",
                        created_by=_current_user(),
                    )
                    save_decisiones_for_entrada(
                        entrada_staging_id=entry_id,
                        decisiones=[decision],
                        usuario=_current_user(),
                        motivo_auditoria="Corrección desde revisión y envío a stock libre",
                    )
                    apply_decisiones_to_inventory_and_pedidos(
                        entrada_staging_id=entry_id,
                        usuario=_current_user(),
                        motivo_auditoria="Aplicación directa a stock libre desde revisión",
                    )
                    st.success("Fila corregida y enviada a stock libre correctamente.")
                    _rerun()
                except Exception as exc:
                    st.error(f"No fue posible mandar la fila a stock libre: {exc}")

            if b3.button("Mantener en revisión", key=f"rev_hold_{entry_id}"):
                try:
                    mark_entrada_en_revision(entry_id, usuario=_current_user(), motivo="Revisión manual confirmada")
                    st.success("La fila quedó marcada en revisión.")
                    _rerun()
                except Exception as exc:
                    st.error(f"No fue posible mantener la fila en revisión: {exc}")

            if b4.button("Ignorar fila", key=f"rev_ignore_{entry_id}"):
                try:
                    mark_entrada_ignorada(entry_id, usuario=_current_user(), motivo="Ignorada desde bandeja de revisión")
                    st.success("Fila ignorada correctamente.")
                    _rerun()
                except Exception as exc:
                    st.error(f"No fue posible ignorar la fila: {exc}")


def _render_summary(ready_groups: list[dict[str, Any]], review_groups: list[dict[str, Any]]) -> None:
    total_pedidos = round(sum(_safe_float(g.get("total_asignado_pedidos"), 0.0) for g in ready_groups), 2)
    total_stock = round(sum(_safe_float(g.get("total_stock_libre"), 0.0) for g in ready_groups), 2)
    total_pending = round(sum(_safe_float(g.get("total_pendiente"), 0.0) for g in ready_groups), 2)
    decided_count = sum(1 for g in ready_groups if g.get("estado_sku") in {"PROPUESTA_GUARDADA", "DECIDIDO", "PENDIENTE"})

    st.subheader("4. Resumen final")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("SKU listos", len(ready_groups))
    c2.metric("SKU con decisión guardada", decided_count)
    c3.metric("Total a pedidos", total_pedidos)
    c4.metric("Total a stock libre", total_stock)
    c5.metric("Total pendiente", total_pending)
    st.caption(f"SKU por corregir: {len(review_groups)}")

    summary_df = pd.DataFrame(
        [
            {
                "SKU": g.get("sku"),
                "Producto": _safe_text(g.get("producto")) or _safe_text(g.get("descripcion")),
                "Recibido": round(_safe_float(g.get("cantidad_recibida"), 0.0), 2),
                "Estado": g.get("estado_sku"),
                "A pedidos": round(_safe_float(g.get("total_asignado_pedidos"), 0.0), 2),
                "A stock": round(_safe_float(g.get("total_stock_libre"), 0.0), 2),
                "Pendiente": round(_safe_float(g.get("total_pendiente"), 0.0), 2),
            }
            for g in ready_groups
        ]
    )
    if not summary_df.empty:
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

    if _is_admin():
        if st.button("Aplicar todas las asignaciones guardadas", key="apply_all_groups"):
            result = _apply_all_for_group_entries(ready_groups, _current_user())
            if result["failed"]:
                st.error(
                    f"Aplicación terminada con errores. Aplicadas: {result['applied']} | omitidas: {result['skipped']} | fallidas: {result['failed']}"
                )
            else:
                st.success(
                    f"Aplicación completada. Aplicadas: {result['applied']} | omitidas: {result['skipped']} | fallidas: {result['failed']}"
                )
            _rerun()



def _render_inventory_impact_tab(import_job_id: int) -> None:
    st.subheader("Impacto en inventario de esta importación")
    st.caption(
        "Resume cuánto de esta entrada ya terminó realmente en inventario, distinguiendo entre stock libre y stock asignado."
    )

    resumen = get_entrada_produccion_job_resumen(import_job_id)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total recibido archivo", round(_safe_float(resumen.get("Total recibido archivo"), 0.0), 2))
    c2.metric("Aplicado inventario", round(_safe_float(resumen.get("Total aplicado inventario"), 0.0), 2))
    c3.metric("A stock libre", round(_safe_float(resumen.get("Total a stock libre"), 0.0), 2))
    c4.metric("A stock asignado", round(_safe_float(resumen.get("Total a stock asignado"), 0.0), 2))
    c5.metric("SKU tocados", _safe_int(resumen.get("SKU tocados"), 0))

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Entradas archivo", _safe_int(resumen.get("Entradas archivo"), 0))
    d2.metric("Entradas resueltas", _safe_int(resumen.get("Entradas resueltas"), 0))
    d3.metric("Entradas aplicadas", _safe_int(resumen.get("Entradas aplicadas"), 0))
    d4.metric("Decisiones aplicadas", _safe_int(resumen.get("Decisiones aplicadas"), 0))

    impact_rows = get_entrada_produccion_job_impact(import_job_id)
    if not impact_rows:
        st.info("Esta importación todavía no tiene impacto aplicado en inventario.")
        return

    impact_df = pd.DataFrame(impact_rows)
    st.markdown("**Detalle por SKU**")
    st.dataframe(impact_df, use_container_width=True, hide_index=True)
    st.caption(
        "Total aplicado = entrada física que ya impactó inventario. "
        "A stock libre y a stock asignado muestran cómo quedó repartido."
    )


def _render_technical_support(payload: dict[str, Any]) -> None:
    with st.expander("Ver detalle técnico (opcional)", expanded=False):
        st.write("**Detalle interno de entradas detectadas**")
        cortes_df = pd.DataFrame(
            [
                {
                    "ID": _safe_int(c.get("id")),
                    "Fecha": _safe_text(c.get("fecha_corte")),
                    "Hora": _safe_text(c.get("hora_corte_normalizada")),
                    "Estado": _safe_text(c.get("estado_corte")),
                    "Filas": _safe_int(c.get("total_filas")),
                    "Piezas": round(_safe_float(c.get("total_piezas"), 0.0), 2),
                }
                for c in payload.get("cortes") or []
            ]
        )
        if not cortes_df.empty:
            st.dataframe(cortes_df, use_container_width=True, hide_index=True)

        st.write("**Detalle interno de filas recibidas**")
        entries_df = pd.DataFrame(
            [
                {
                    "ID": _safe_int(r.get("id")),
                    "Corte": _safe_int(r.get("corte_id")),
                    "Fila": _safe_int(r.get("row_num")),
                    "SKU": _safe_text(r.get("sku")),
                    "Descripción": _safe_text(r.get("descripcion")),
                    "Cantidad": round(_safe_float(r.get("cantidad"), 0.0), 2),
                    "Fecha": _safe_text(r.get("fecha")),
                    "Hora": _safe_text(r.get("hora")),
                    "Tipo sugerencia": _safe_text(r.get("tipo_sugerencia")),
                    "Pedido sugerido": _safe_text(r.get("pedido_sugerido_texto")),
                    "Estado": _safe_text(r.get("estado_revision")),
                }
                for r in payload.get("entradas") or []
            ]
        )
        if not entries_df.empty:
            st.dataframe(entries_df, use_container_width=True, hide_index=True)


def main() -> None:
    bootstrap_runtime()
    _render_page_header()
    if not db_exists():
        st.error("La base de datos no existe. Inicialízala primero con `python -m wms_mvp.db.init_db`.")
        st.stop()

    try:
        jobs = _list_recent_entrada_jobs(limit=50)
    except Exception as exc:
        st.error(f"No fue posible cargar las entradas recientes: {exc}")
        st.stop()

    selected_import_job_id = _render_recent_jobs(jobs)
    if not selected_import_job_id:
        st.stop()

    try:
        payload = _load_import_job_payload(selected_import_job_id)
    except Exception as exc:
        st.error(f"No fue posible cargar la entrada seleccionada: {exc}")
        st.stop()

    ready_groups, review_groups = _group_entries_by_sku(payload)
    _render_dashboard(payload, ready_groups, review_groups)

    tabs = st.tabs(["Pendientes de decidir", "Revisión de datos", "Stock libre", "Resumen", "Impacto inventario", "Soporte técnico"])

    with tabs[0]:
        st.subheader("3. Decisión por SKU")
        if not ready_groups:
            st.warning("No hay SKU listos para asignar en esta entrada.")
        else:
            filter_col1, filter_col2 = st.columns([2, 1])
            search = filter_col1.text_input("Buscar SKU o producto", value="")
            available_states = sorted({_safe_text(g.get("estado_sku")) for g in ready_groups if _safe_text(g.get("estado_sku"))})
            selected_states = filter_col2.multiselect(
                "Estado",
                options=available_states,
                default=available_states,
                help="Puedes enfocarte solo en SKU sin decidir, con propuesta guardada, parciales o ya decididos.",
            )
            filtered = [
                g for g in ready_groups
                if (not search
                    or search.lower() in _safe_text(g.get("sku")).lower()
                    or search.lower() in _safe_text(g.get("producto")).lower()
                    or search.lower() in _safe_text(g.get("descripcion")).lower())
                and (not selected_states or _safe_text(g.get("estado_sku")) in selected_states)
            ]
            if not filtered:
                st.info("No hay SKU que coincidan con el filtro.")
            else:
                selected_group = _pick_ready_group(filtered)
                if selected_group is not None:
                    st.divider()
                    _render_ready_group(selected_group)

    with tabs[1]:
        if not review_groups:
            st.success("No hay SKU en revisión en esta entrada.")
        else:
            st.caption("Corrige aquí los datos necesarios para devolver la fila a operación, ignorarla o mandarla directo a stock libre.")
            selected_review_group = _pick_review_group(review_groups)
            if selected_review_group is not None:
                st.divider()
                _render_review_group(selected_review_group)

    with tabs[2]:
        st.subheader("3B. Reparto desde stock libre")
        stock_groups = _build_stock_libre_groups(limit=1000)
        if not stock_groups:
            st.info("No hay SKU disponibles en stock libre en este momento.")
        else:
            filter_col1, filter_col2 = st.columns([2, 1])
            stock_search = filter_col1.text_input("Buscar SKU o producto disponible", value="")
            stock_states = sorted({_safe_text(g.get("estado_stock")) for g in stock_groups if _safe_text(g.get("estado_stock"))})
            selected_stock_states = filter_col2.multiselect(
                "Situación",
                options=stock_states,
                default=stock_states,
                help="Puedes enfocarte en SKU con demanda activa o en SKU que hoy no tienen pedidos relacionados.",
            )
            filtered_stock = [
                g for g in stock_groups
                if (not stock_search
                    or stock_search.lower() in _safe_text(g.get("sku")).lower()
                    or stock_search.lower() in _safe_text(g.get("producto")).lower()
                    or stock_search.lower() in _safe_text(g.get("descripcion")).lower())
                and (not selected_stock_states or _safe_text(g.get("estado_stock")) in selected_stock_states)
            ]
            if not filtered_stock:
                st.info("No hay SKU disponibles que coincidan con el filtro.")
            else:
                selected_stock_group = _pick_stock_group(filtered_stock)
                if selected_stock_group is not None:
                    st.divider()
                    _render_stock_group(selected_stock_group)

    with tabs[3]:
        _render_summary(ready_groups, review_groups)

    with tabs[4]:
        _render_inventory_impact_tab(selected_import_job_id)

    with tabs[5]:
        _render_technical_support(payload)


if __name__ == "__main__":
    main()
