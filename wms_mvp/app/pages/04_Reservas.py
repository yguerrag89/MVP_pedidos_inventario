from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

PACKAGE_PARENT = Path(__file__).resolve().parents[3]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

import pandas as pd
import streamlit as st

from wms_mvp.app.bootstrap import bootstrap_runtime
from wms_mvp.core.services.reservas_service import (
    asignar_stock_libre_a_linea,
    ensure_reservas_phase3_schema,
    get_lineas_activas_por_sku,
    get_reservas_resumen,
    get_reservas_ui_options,
    liberar_reserva_a_stock_libre,
    list_recent_reasignaciones,
    list_skus_con_reserva,
    list_skus_para_asignar,
    preview_reasignacion,
    reasignar_reserva_entre_pedidos,
)
from wms_mvp.db.connection import db_exists

PAGE_TITLE = "Reservas"
FLASH_KEY = "reservas_flash"
SESSION_USER_KEYS = ("wms_current_user", "current_user", "auth_user")
SESSION_ROLE_KEYS = ("wms_current_role", "current_role", "auth_role")
SESSION_ADMIN_KEYS = ("wms_admin_unlocked", "is_admin", "auth_is_admin")


def _rerun() -> None:
    rerun_fn = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if rerun_fn:
        rerun_fn()



def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()



def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default



def _get_session_value(keys: tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        if key in st.session_state:
            return st.session_state.get(key)
    return default



def _is_admin_session() -> bool:
    if bool(_get_session_value(SESSION_ADMIN_KEYS, False)):
        return True
    return _safe_text(_get_session_value(SESSION_ROLE_KEYS, "")).lower() == "admin"



def _get_current_user() -> str:
    return _safe_text(_get_session_value(SESSION_USER_KEYS, "")) or "Consulta"



def _set_flash(message: str, kind: str = "success") -> None:
    st.session_state[FLASH_KEY] = {"message": message, "kind": kind}



def _render_flash() -> None:
    payload = st.session_state.pop(FLASH_KEY, None)
    if not payload:
        return
    kind = payload.get("kind")
    message = payload.get("message")
    if kind == "error":
        st.error(message)
    elif kind == "warning":
        st.warning(message)
    else:
        st.success(message)


def _prefill_sku_for_options(options: list[str]) -> tuple[str, int]:
    preferred = _safe_text(st.session_state.get("reservas_prefill_sku"))
    if preferred and preferred in options:
        return preferred, options.index(preferred)
    return "", 0


def _render_prefill_banner() -> None:
    preferred = _safe_text(st.session_state.get("reservas_prefill_sku"))
    preferred_tab = _safe_text(st.session_state.get("reservas_prefill_tab"))
    if preferred:
        label_tab = {"asignar": "Asignar/Liberar", "reasignar": "Reasignar"}.get(preferred_tab, "Reservas")
        st.caption(f"SKU sugerido desde Entradas: {preferred}. Revisa primero la pestaña {label_tab}.")



def _render_header() -> None:
    st.title(PAGE_TITLE)
    st.caption(
        "Operaciones de reserva sobre pedidos activos. Aquí puedes asignar desde stock libre, liberar reservas y reasignar entre pedidos sin tocar el inventario físico total."
    )
    resumen = get_reservas_resumen()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("SKU con reserva", int(resumen.get("skus_con_reserva", 0)))
    c2.metric("SKU con faltante", int(resumen.get("skus_con_faltante", 0)))
    c3.metric("Reservado total", round(_safe_float(resumen.get("reservado_total"), 0), 2))
    c4.metric("Faltante total", round(_safe_float(resumen.get("faltante_total"), 0), 2))
    c5.metric("Stock libre total", round(_safe_float(resumen.get("stock_libre_total"), 0), 2))
    if _is_admin_session():
        st.success(f"Modo Administrador | Usuario: {_get_current_user()}")
    else:
        st.info("Modo Consulta. Puedes revisar las reservas, pero no aplicar cambios.")



def _lineas_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=[
            "pedido_linea_id", "id_pedido", "cliente", "sku", "descripcion", "cantidad_pedida",
            "cantidad_asignada", "cantidad_faltante", "cantidad_enviada", "fecha_produccion", "fecha_logistica", "estatus_venta"
        ])
    df = pd.DataFrame(rows)
    return df.rename(columns={
        "pedido_linea_id": "Línea ID",
        "id_pedido": "ID_pedido",
        "cliente": "Cliente",
        "sku": "SKU",
        "descripcion": "Descripción",
        "cantidad_pedida": "Pedida",
        "cantidad_asignada": "Asignada",
        "cantidad_faltante": "Faltante",
        "cantidad_enviada": "Enviada",
        "fecha_produccion": "Fecha producción",
        "fecha_logistica": "Fecha logística",
        "estatus_venta": "Estatus venta",
    })



def _render_asignar_tab() -> None:
    st.subheader("Asignar desde stock libre")
    search = st.text_input("Buscar SKU o descripción con stock libre y faltante", key="reservas_asignar_search")
    skus = list_skus_para_asignar(search)
    if skus:
        st.dataframe(pd.DataFrame(skus), use_container_width=True, hide_index=True)
    else:
        st.info("No hay SKU con stock libre y faltante activo para el criterio actual.")
        return

    options = [row["SKU"] for row in skus]
    preferred_sku, default_index = _prefill_sku_for_options(options)
    selected_sku = st.selectbox("SKU a trabajar", options=options, index=default_index, key="reservas_asignar_sku")
    lineas = [row for row in get_lineas_activas_por_sku(selected_sku) if _safe_float(row.get("cantidad_faltante"), 0) > 0]
    st.dataframe(_lineas_df(lineas), use_container_width=True, hide_index=True)
    if not lineas:
        st.info("El SKU seleccionado ya no tiene líneas con faltante activo.")
        return
    line_map = {f"{row['id_pedido']} | {row['cliente']} | línea {row['pedido_linea_id']} | faltante={row['cantidad_faltante']}": row for row in lineas}
    selected_line_label = st.selectbox("Línea destino", options=list(line_map.keys()), key="reservas_asignar_linea")
    selected_line = line_map[selected_line_label]
    cantidad = st.number_input("Cantidad a asignar", min_value=0.0, step=1.0, key="reservas_asignar_cantidad")
    observacion = st.text_input("Observación", key="reservas_asignar_obs")
    if st.button("Asignar desde stock libre", type="primary", key="reservas_btn_asignar", disabled=not _is_admin_session()):
        try:
            asignacion_id = asignar_stock_libre_a_linea(
                pedido_linea_id=int(selected_line["pedido_linea_id"]),
                cantidad=cantidad,
                usuario=_get_current_user(),
                observacion=observacion,
            )
            _set_flash(f"Asignación creada correctamente. ID asignación: {asignacion_id}.")
            _rerun()
        except Exception as exc:
            _set_flash(f"No fue posible asignar desde stock libre: {exc}", kind="error")
            _rerun()



def _render_liberar_tab() -> None:
    st.subheader("Liberar reserva a stock libre")
    search = st.text_input("Buscar SKU o descripción con reserva activa", key="reservas_liberar_search")
    skus = list_skus_con_reserva(search)
    if skus:
        st.dataframe(pd.DataFrame(skus), use_container_width=True, hide_index=True)
    else:
        st.info("No hay SKU con reserva activa para el criterio actual.")
        return

    options = [row["SKU"] for row in skus]
    preferred_sku, default_index = _prefill_sku_for_options(options)
    selected_sku = st.selectbox("SKU a liberar", options=options, index=default_index, key="reservas_liberar_sku")
    lineas = [row for row in get_lineas_activas_por_sku(selected_sku) if _safe_float(row.get("cantidad_asignada"), 0) > 0]
    st.dataframe(_lineas_df(lineas), use_container_width=True, hide_index=True)
    if not lineas:
        st.info("El SKU seleccionado ya no tiene líneas con reserva activa.")
        return
    line_map = {f"{row['id_pedido']} | {row['cliente']} | línea {row['pedido_linea_id']} | asignada={row['cantidad_asignada']}": row for row in lineas}
    selected_line_label = st.selectbox("Línea origen", options=list(line_map.keys()), key="reservas_liberar_linea")
    selected_line = line_map[selected_line_label]
    cantidad = st.number_input("Cantidad a liberar", min_value=0.0, step=1.0, key="reservas_liberar_cantidad")
    observacion = st.text_input("Observación de liberación", key="reservas_liberar_obs")
    if st.button("Liberar a stock libre", type="primary", key="reservas_btn_liberar", disabled=not _is_admin_session()):
        try:
            result = liberar_reserva_a_stock_libre(
                pedido_linea_id=int(selected_line["pedido_linea_id"]),
                cantidad=cantidad,
                usuario=_get_current_user(),
                observacion=observacion,
            )
            _set_flash(
                f"Reserva liberada correctamente. SKU {result['sku']} | cantidad={result['cantidad_liberada']} | movimiento={result['movimiento_id']}"
            )
            _rerun()
        except Exception as exc:
            _set_flash(f"No fue posible liberar la reserva: {exc}", kind="error")
            _rerun()



def _render_reasignar_tab() -> None:
    st.subheader("Reasignar entre pedidos")
    options = get_reservas_ui_options()
    sku_options = options.get("skus_con_reserva", [])
    preferred_sku, default_index = _prefill_sku_for_options(sku_options)
    selected_sku = st.selectbox(
        "SKU a reasignar",
        options=sku_options,
        index=default_index if sku_options else 0,
        key="reservas_reasignar_sku",
        help="Se muestran SKU con al menos una línea activa con reserva.",
    )
    if not selected_sku:
        st.info("Aún no hay SKU con reserva activa para reasignar.")
        return

    lineas = get_lineas_activas_por_sku(selected_sku)
    st.dataframe(_lineas_df(lineas), use_container_width=True, hide_index=True)
    origen_rows = [row for row in lineas if _safe_float(row.get("cantidad_asignada"), 0) > 0]
    destino_rows = [row for row in lineas if _safe_float(row.get("cantidad_faltante"), 0) > 0]

    if not origen_rows or not destino_rows:
        st.info("Para reasignar se necesita al menos una línea con asignado y otra con faltante del mismo SKU.")
        return

    origen_map = {f"{row['id_pedido']} | {row['cliente']} | línea {row['pedido_linea_id']} | asignada={row['cantidad_asignada']}": row for row in origen_rows}
    destino_map = {f"{row['id_pedido']} | {row['cliente']} | línea {row['pedido_linea_id']} | faltante={row['cantidad_faltante']}": row for row in destino_rows}

    c1, c2 = st.columns(2)
    origen_label = c1.selectbox("Línea origen", options=list(origen_map.keys()), key="reservas_reasignar_origen")
    destino_label = c2.selectbox("Línea destino", options=list(destino_map.keys()), key="reservas_reasignar_destino")
    origen = origen_map[origen_label]
    destino = destino_map[destino_label]

    cantidad = st.number_input("Cantidad a reasignar", min_value=0.0, step=1.0, key="reservas_reasignar_cantidad")
    motivo = st.selectbox("Motivo", options=options.get("motivos_reasignacion", []), key="reservas_reasignar_motivo")
    observacion = st.text_input("Observación", key="reservas_reasignar_obs")

    if st.button("Generar vista previa", key="reservas_preview_reasignacion"):
        try:
            preview = preview_reasignacion(
                linea_origen_id=int(origen["pedido_linea_id"]),
                linea_destino_id=int(destino["pedido_linea_id"]),
                cantidad=cantidad,
            )
            st.session_state["reservas_preview_payload"] = preview
        except Exception as exc:
            _set_flash(f"No fue posible generar la vista previa: {exc}", kind="error")
            _rerun()

    preview = st.session_state.get("reservas_preview_payload")
    if preview and preview.get("sku") == selected_sku:
        st.markdown("**Vista previa**")
        st.dataframe(
            pd.DataFrame([
                {
                    "Pedido": preview["origen"]["id_pedido"],
                    "Cliente": preview["origen"]["cliente"],
                    "Rol": "Origen",
                    "Asignada antes": preview["origen"]["cantidad_asignada_antes"],
                    "Asignada después": preview["origen"]["cantidad_asignada_despues"],
                },
                {
                    "Pedido": preview["destino"]["id_pedido"],
                    "Cliente": preview["destino"]["cliente"],
                    "Rol": "Destino",
                    "Asignada antes": preview["destino"]["cantidad_asignada_antes"],
                    "Asignada después": preview["destino"]["cantidad_asignada_despues"],
                },
            ]),
            use_container_width=True,
            hide_index=True,
        )
        if st.button("Confirmar reasignación", type="primary", key="reservas_btn_reasignar", disabled=not _is_admin_session()):
            try:
                result = reasignar_reserva_entre_pedidos(
                    linea_origen_id=int(origen["pedido_linea_id"]),
                    linea_destino_id=int(destino["pedido_linea_id"]),
                    cantidad=cantidad,
                    usuario=_get_current_user(),
                    motivo=motivo,
                    observacion=observacion,
                )
                st.session_state.pop("reservas_preview_payload", None)
                _set_flash(
                    f"Reasignación creada. ID={result['reasignacion_id']} | SKU={result['sku']} | cantidad={result['cantidad']}"
                )
                _rerun()
            except Exception as exc:
                _set_flash(f"No fue posible reasignar la reserva: {exc}", kind="error")
                _rerun()



def _render_historial_tab() -> None:
    st.subheader("Historial reciente de reasignaciones")
    rows = list_recent_reasignaciones(limit=200)
    if not rows:
        st.info("Aún no hay reasignaciones registradas.")
        return
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)



def main() -> None:
    bootstrap_runtime()
    if not db_exists():
        st.error("La base de datos no existe. Inicialízala primero con python -m wms_mvp.db.init_db")
        st.stop()

    ensure_reservas_phase3_schema()
    _render_header()
    _render_prefill_banner()
    _render_flash()

    tab1, tab2, tab3, tab4 = st.tabs([
        "Asignar desde stock libre",
        "Liberar reserva",
        "Reasignar entre pedidos",
        "Historial",
    ])
    with tab1:
        _render_asignar_tab()
    with tab2:
        _render_liberar_tab()
    with tab3:
        _render_reasignar_tab()
    with tab4:
        _render_historial_tab()


if __name__ == "__main__":
    main()
