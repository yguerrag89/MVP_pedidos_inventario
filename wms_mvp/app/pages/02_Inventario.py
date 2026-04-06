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
from wms_mvp.core.services.inventario_service import (
    InventarioImportError,
    apply_initial_inventory_load,
    apply_partial_inventory_update,
    apply_inventory_cut,
    build_initial_inventory_preview,
    build_inventory_cut_preview,
    build_partial_inventory_update_preview,
    create_manual_inventory_adjustment,
    ensure_inventory_phase1_schema,
    get_inventory_excel_sheets,
    get_entrada_produccion_job_impact,
    get_entrada_produccion_job_resumen,
    get_entradas_produccion_resumen,
    get_inventario_operativo_resumen,
    get_inventario_resumen,
    get_pedido_cierre_inventory_impact,
    get_salidas_por_envio_resumen,
    list_entradas_produccion_jobs,
    list_inventory_distinct_filter_values,
    list_inventario_alertas,
    list_inventario_cortes,
    list_inventario_import_jobs,
    list_inventario_movimientos,
    list_inventario_operativo,
    list_inventario_tabla,
    list_salidas_por_envio,
    suggest_inventory_sheet,
)
from wms_mvp.db.connection import db_exists


PAGE_TITLE = "Inventario"
FLASH_MESSAGE_KEY = "inventario_flash_message"
FLASH_TYPE_KEY = "inventario_flash_type"
PREVIEW_KEY = "inventario_phase1_preview"
PREVIEW_FILE_BYTES_KEY = "inventario_phase1_file_bytes"
PREVIEW_FILE_NAME_KEY = "inventario_phase1_file_name"
UPDATE_PREVIEW_KEY = "inventario_update_preview"
UPDATE_PREVIEW_FILE_BYTES_KEY = "inventario_update_file_bytes"
UPDATE_PREVIEW_FILE_NAME_KEY = "inventario_update_file_name"
CUT_PREVIEW_KEY = "inventario_cut_preview"
CUT_PREVIEW_FILE_BYTES_KEY = "inventario_cut_file_bytes"
CUT_PREVIEW_FILE_NAME_KEY = "inventario_cut_file_name"
SESSION_USER_KEYS = ("wms_current_user", "current_user", "auth_user")
SESSION_ROLE_KEYS = ("wms_current_role", "current_role", "auth_role")
SESSION_ADMIN_KEYS = ("wms_admin_unlocked", "is_admin", "auth_is_admin")


# ---------------------------------------------------------
# Helpers generales
# ---------------------------------------------------------
def _rerun() -> None:
    rerun_fn = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if rerun_fn:
        rerun_fn()



def _set_flash(message: str, kind: str = "success") -> None:
    st.session_state[FLASH_MESSAGE_KEY] = str(message)
    st.session_state[FLASH_TYPE_KEY] = str(kind)



def _render_flash() -> None:
    message = st.session_state.pop(FLASH_MESSAGE_KEY, None)
    kind = st.session_state.pop(FLASH_TYPE_KEY, "success")
    if not message:
        return
    if kind == "error":
        st.error(message)
    elif kind == "warning":
        st.warning(message)
    else:
        st.success(message)



def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()



def _safe_float(value: Any, default: float = 0.0) -> float:
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



def _safe_int(value: Any, default: int = 0) -> int:
    return int(round(_safe_float(value, default)))



def _get_session_value(keys: tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        if key in st.session_state:
            return st.session_state.get(key)
    return default



def _is_admin_session() -> bool:
    if bool(_get_session_value(SESSION_ADMIN_KEYS, False)):
        return True
    role = _safe_text(_get_session_value(SESSION_ROLE_KEYS, "")).lower()
    return role == "admin"



def _get_current_user() -> str:
    user = _safe_text(_get_session_value(SESSION_USER_KEYS, ""))
    return user or "Consulta"



def _render_access_notice() -> None:
    is_admin = _is_admin_session()
    current_user = _get_current_user()
    c1, c2 = st.columns(2)
    c1.metric("Modo actual", "Administrador" if is_admin else "Consulta")
    c2.metric("Usuario", current_user)
    if is_admin:
        st.success("La sesión está en modo administrador. Las acciones de carga y ajustes están habilitadas.")
    else:
        st.info("La sesión está en modo consulta. Puedes revisar inventario y movimientos, pero no aplicar cambios.")



def _preview_to_df(rows: list[dict[str, Any]], columns: list[str] | None = None) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if columns:
        for col in columns:
            if col not in df.columns:
                df[col] = ""
        df = df[columns]
    return df



def _clear_preview() -> None:
    st.session_state.pop(PREVIEW_KEY, None)
    st.session_state.pop(PREVIEW_FILE_BYTES_KEY, None)
    st.session_state.pop(PREVIEW_FILE_NAME_KEY, None)



def _clear_update_preview() -> None:
    st.session_state.pop(UPDATE_PREVIEW_KEY, None)
    st.session_state.pop(UPDATE_PREVIEW_FILE_BYTES_KEY, None)
    st.session_state.pop(UPDATE_PREVIEW_FILE_NAME_KEY, None)


def _clear_cut_preview() -> None:
    st.session_state.pop(CUT_PREVIEW_KEY, None)
    st.session_state.pop(CUT_PREVIEW_FILE_BYTES_KEY, None)
    st.session_state.pop(CUT_PREVIEW_FILE_NAME_KEY, None)



def _download_df_button(df: pd.DataFrame, label: str, file_name: str) -> None:
    if df.empty:
        return
    st.download_button(
        label,
        data=df.to_csv(index=False).encode("utf-8-sig"),
        file_name=file_name,
        mime="text/csv",
    )



def _render_resumen_cards() -> None:
    resumen = get_inventario_resumen()
    last_job = resumen.get("ultimo_import_job") or {}

    st.subheader("Resumen base del inventario")
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("SKU cargados", _safe_int(resumen.get("total_skus"), 0))
    r2.metric("Piezas totales", round(_safe_float(resumen.get("total_piezas"), 0.0), 2))
    r3.metric("SKU con stock 0", _safe_int(resumen.get("skus_en_cero"), 0))
    r4.metric("SKU en revisión último job", _safe_int(resumen.get("skus_en_revision_ultimo_job"), 0))

    c1, c2 = st.columns(2)
    c1.caption(
        "Última fecha de actualización declarada: "
        + (_safe_text(resumen.get("ultima_fecha_actualizacion")) or "N/D")
    )
    c2.caption(
        "Última actualización del sistema: "
        + (_safe_text(resumen.get("ultima_actualizacion_sistema")) or "N/D")
    )

    if last_job:
        st.caption(
            "Último job de inventario: "
            f"ID {_safe_int(last_job.get('id'))} | estado={_safe_text(last_job.get('status'))} | "
            f"archivo={_safe_text(last_job.get('file_name'))}"
        )



def _render_operativo_cards() -> None:
    resumen = get_inventario_operativo_resumen()
    st.subheader("Resumen operativo")
    st.caption("La existencia operativa se calcula por SKU usando su fecha base de inventario: base + entradas posteriores + ajustes posteriores - salidas por envío posteriores.")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Existencia operativa", round(_safe_float(resumen.get("total_existencia"), 0), 2))
    c2.metric("Reservado activo", round(_safe_float(resumen.get("total_reservado_activo"), 0), 2))
    c3.metric("Disponible", round(_safe_float(resumen.get("total_disponible"), 0), 2))
    c4.metric("SKU con demanda", _safe_int(resumen.get("skus_con_demanda"), 0))

    d1, d2, d3 = st.columns(3)
    d1.metric("SKU rojos", _safe_int(resumen.get("skus_rojos"), 0))
    d2.metric("SKU naranjas", _safe_int(resumen.get("skus_naranjas"), 0))
    d3.metric("SKU sin inventario base", _safe_int(resumen.get("skus_sin_base"), 0))



def _render_carga_inicial_tab() -> None:
    st.subheader("Carga inicial de inventario")
    st.caption(
        "Esta carga define la foto base del inventario. Reemplaza la base actual de inventario_sku y genera movimientos tipo CARGA_INICIAL."
    )
    _render_access_notice()

    uploaded_file = st.file_uploader(
        "Sube el archivo base del inventario (.xlsx, .xls)",
        type=["xlsx", "xls"],
        key="inventario_phase1_uploader",
        help="Se espera una hoja con columnas SKU, DESCRIPCION, STOCK y FECHA_ACTUALIZACION.",
    )

    if uploaded_file is not None:
        file_bytes = uploaded_file.getvalue()
        st.session_state[PREVIEW_FILE_BYTES_KEY] = file_bytes
        st.session_state[PREVIEW_FILE_NAME_KEY] = uploaded_file.name
        try:
            sheets = get_inventory_excel_sheets(file_bytes)
            suggested = suggest_inventory_sheet(file_bytes)
        except Exception as exc:
            st.error(f"No fue posible inspeccionar el archivo: {exc}")
            return

        selected_sheet = st.selectbox(
            "Hoja a analizar",
            options=sheets,
            index=sheets.index(suggested) if suggested in sheets else 0,
            key="inventario_phase1_selected_sheet",
        )

        col1, col2 = st.columns([1, 1])
        if col1.button("Analizar archivo", type="primary"):
            try:
                preview = build_initial_inventory_preview(
                    file_name=uploaded_file.name,
                    file_bytes=file_bytes,
                    sheet_name=selected_sheet,
                )
                st.session_state[PREVIEW_KEY] = preview
                _set_flash("Archivo analizado correctamente. Revisa válidos y revisión antes de confirmar.")
                _rerun()
            except InventarioImportError as exc:
                _set_flash(str(exc), kind="error")
                _rerun()
            except Exception as exc:
                _set_flash(f"No fue posible analizar el archivo: {exc}", kind="error")
                _rerun()

        if col2.button("Limpiar preview"):
            _clear_preview()
            _rerun()

    preview = st.session_state.get(PREVIEW_KEY)
    if not preview:
        return

    st.markdown("---")
    st.markdown("**Resultado del análisis**")
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Filas totales", _safe_int(preview.get("total_rows"), 0))
    p2.metric("Filas válidas", _safe_int(preview.get("valid_count"), 0))
    p3.metric("A revisión", _safe_int(preview.get("review_count"), 0))
    p4.metric("Stock total válido", round(_safe_float(preview.get("total_stock_valid"), 0.0), 2))

    c1, c2 = st.columns(2)
    c1.caption(f"Archivo: {_safe_text(preview.get('file_name'))}")
    c2.caption(f"Hoja analizada: {_safe_text(preview.get('selected_sheet'))}")

    valid_df = _preview_to_df(
        preview.get("valid_rows", []),
        columns=["Fila", "SKU", "Descripción", "Stock", "Fecha actualización", "Nota"],
    )
    review_df = _preview_to_df(
        preview.get("review_rows", []),
        columns=["Fila", "SKU", "Descripción", "Stock", "Fecha actualización", "Nota", "Motivo revisión"],
    )

    with st.expander(f"Ver filas válidas ({len(valid_df)})", expanded=True):
        st.dataframe(valid_df, use_container_width=True, hide_index=True)
    with st.expander(f"Ver filas en revisión ({len(review_df)})", expanded=bool(len(review_df))):
        if review_df.empty:
            st.info("No hay filas en revisión en este análisis.")
        else:
            st.dataframe(review_df, use_container_width=True, hide_index=True)
            _download_df_button(review_df, "Descargar revisión en CSV", "inventario_review_preview.csv")

    if not _is_admin_session():
        st.warning("Para aplicar la carga inicial debes entrar en modo Administrador.")
        return

    if st.button("Confirmar y aplicar carga inicial", type="primary"):
        try:
            file_bytes = st.session_state.get(PREVIEW_FILE_BYTES_KEY)
            if not file_bytes:
                raise InventarioImportError("Se perdió el archivo cargado en la sesión. Vuelve a subirlo.")
            result = apply_initial_inventory_load(
                preview_payload=preview,
                file_bytes=file_bytes,
                created_by=_get_current_user(),
            )
            _clear_preview()
            _set_flash(
                f"Carga inicial aplicada correctamente. Job {result['import_job_id']} | "
                f"insertadas={result['inserted_rows']} | revisión={result['review_rows']}"
            )
            _rerun()
        except Exception as exc:
            _set_flash(f"No fue posible aplicar la carga inicial: {exc}", kind="error")
            _rerun()



def _render_actualizacion_parcial_tab() -> None:
    st.subheader("Actualización parcial de inventario")
    st.caption(
        "Usa esta carga para conteos posteriores. Solo actualiza los SKU incluidos en el archivo; los que no aparezcan se quedan como están."
    )
    _render_access_notice()

    uploaded_file = st.file_uploader(
        "Sube el archivo de actualización parcial (.xlsx, .xls)",
        type=["xlsx", "xls"],
        key="inventario_update_uploader",
        help="Se esperan las mismas columnas base: SKU, DESCRIPCION, STOCK y FECHA_ACTUALIZACION.",
    )

    if uploaded_file is not None:
        file_bytes = uploaded_file.getvalue()
        st.session_state[UPDATE_PREVIEW_FILE_BYTES_KEY] = file_bytes
        st.session_state[UPDATE_PREVIEW_FILE_NAME_KEY] = uploaded_file.name
        try:
            sheets = get_inventory_excel_sheets(file_bytes)
            suggested = suggest_inventory_sheet(file_bytes)
        except Exception as exc:
            st.error(f"No fue posible inspeccionar el archivo: {exc}")
            return

        selected_sheet = st.selectbox(
            "Hoja a analizar para actualización",
            options=sheets,
            index=sheets.index(suggested) if suggested in sheets else 0,
            key="inventario_update_selected_sheet",
        )

        c1, c2 = st.columns([1, 1])
        if c1.button("Analizar actualización parcial", type="primary"):
            try:
                preview = build_partial_inventory_update_preview(
                    file_name=uploaded_file.name,
                    file_bytes=file_bytes,
                    sheet_name=selected_sheet,
                )
                st.session_state[UPDATE_PREVIEW_KEY] = preview
                _set_flash("Archivo de actualización analizado correctamente. Revisa válidos y revisión antes de confirmar.")
                _rerun()
            except InventarioImportError as exc:
                _set_flash(str(exc), kind="error")
                _rerun()
            except Exception as exc:
                _set_flash(f"No fue posible analizar la actualización: {exc}", kind="error")
                _rerun()

        if c2.button("Limpiar preview de actualización"):
            _clear_update_preview()
            _rerun()

    preview = st.session_state.get(UPDATE_PREVIEW_KEY)
    if not preview:
        return

    st.markdown("---")
    st.markdown("**Resultado del análisis de actualización parcial**")
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Filas totales", _safe_int(preview.get("total_rows"), 0))
    p2.metric("SKU válidos", _safe_int(preview.get("valid_count"), 0))
    p3.metric("A revisión", _safe_int(preview.get("review_count"), 0))
    p4.metric("Stock total archivo válido", round(_safe_float(preview.get("total_stock_valid"), 0.0), 2))

    c1, c2 = st.columns(2)
    c1.caption(f"Archivo: {_safe_text(preview.get('file_name'))}")
    c2.caption(f"Hoja analizada: {_safe_text(preview.get('selected_sheet'))}")

    valid_df = _preview_to_df(
        preview.get("valid_rows", []),
        columns=[
            "Fila",
            "SKU",
            "Descripción",
            "Stock actual sistema",
            "Stock nuevo",
            "Diferencia",
            "Fecha actualización",
            "Nota",
        ],
    )
    review_df = _preview_to_df(
        preview.get("review_rows", []),
        columns=[
            "Fila",
            "SKU",
            "Descripción",
            "Stock actual sistema",
            "Stock nuevo",
            "Diferencia",
            "Fecha actualización",
            "Nota",
            "Motivo revisión",
        ],
    )

    with st.expander(f"Ver SKU válidos ({len(valid_df)})", expanded=True):
        st.dataframe(valid_df, use_container_width=True, hide_index=True)
    with st.expander(f"Ver filas en revisión ({len(review_df)})", expanded=bool(len(review_df))):
        if review_df.empty:
            st.info("No hay filas en revisión en este análisis.")
        else:
            st.dataframe(review_df, use_container_width=True, hide_index=True)
            _download_df_button(review_df, "Descargar revisión de actualización", "inventario_actualizacion_review.csv")

    if not _is_admin_session():
        st.warning("Para aplicar la actualización parcial debes entrar en modo Administrador.")
        return

    if st.button("Confirmar y aplicar actualización parcial", type="primary"):
        try:
            file_bytes = st.session_state.get(UPDATE_PREVIEW_FILE_BYTES_KEY)
            if not file_bytes:
                raise InventarioImportError("Se perdió el archivo cargado en la sesión. Vuelve a subirlo.")
            result = apply_partial_inventory_update(
                preview_payload=preview,
                file_bytes=file_bytes,
                created_by=_get_current_user(),
            )
            _clear_update_preview()
            _set_flash(
                f"Actualización parcial aplicada correctamente. Job {result['import_job_id']} | "
                f"SKU actualizados={result['updated_rows']} | revisión={result['review_rows']}"
            )
            _rerun()
        except Exception as exc:
            _set_flash(f"No fue posible aplicar la actualización parcial: {exc}", kind="error")
            _rerun()


def _render_operativo_tab() -> None:
    _render_resumen_cards()
    st.markdown("---")
    _render_operativo_cards()
    st.markdown("---")

    st.subheader("Tabla operativa de inventario")
    f1, f2 = st.columns([3, 1])
    search_text = f1.text_input(
        "Buscar por SKU o descripción",
        value=_safe_text(st.session_state.get("inventario_operativo_search_text")),
        key="inventario_operativo_search_text",
    )
    semaforo = f2.selectbox(
        "Semáforo",
        options=["TODOS", "ROJO", "NARANJA", "VERDE"],
        index=0,
        key="inventario_operativo_semaforo",
    )

    c1, c2, c3, c4 = st.columns(4)
    only_zero_stock = c1.checkbox("Solo stock 0", key="inventario_operativo_only_zero")
    only_negative = c2.checkbox("Solo disponibles negativos", key="inventario_operativo_only_negative")
    only_with_demanda = c3.checkbox("Solo con demanda", key="inventario_operativo_only_demanda")
    only_missing_base = c4.checkbox("Solo sin inventario base", key="inventario_operativo_only_missing_base")

    st.caption("Para evitar duplicados, cada SKU solo suma movimientos con fecha estrictamente posterior a su FECHA_ACTUALIZACION base.")

    rows = list_inventario_operativo(
        search_text=search_text,
        semaforo=None if semaforo == "TODOS" else semaforo,
        only_zero_stock=only_zero_stock,
        only_negative=only_negative,
        only_with_demanda=only_with_demanda,
        only_missing_base=only_missing_base,
        limit=10000,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        st.info("No hay filas para el filtro seleccionado.")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"Filas mostradas: {len(df)}")
    _download_df_button(df, "Descargar inventario operativo", "inventario_operativo.csv")



def _render_alertas_tab() -> None:
    st.subheader("Alertas de inventario")
    rows = list_inventario_alertas(limit=10000)
    df = pd.DataFrame(rows)
    if df.empty:
        st.success("No hay alertas activas en este momento.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"SKU con alerta: {len(df)}")
    _download_df_button(df, "Descargar alertas", "inventario_alertas.csv")



def _render_ajustes_tab() -> None:
    st.subheader("Ajustes manuales de inventario")
    st.caption(
        "En esta fase los ajustes manuales afectan únicamente el stock libre. No modifican reservas ni pedidos activos."
    )
    _render_access_notice()

    if not _is_admin_session():
        st.warning("Solo un administrador puede registrar ajustes manuales.")
        return

    base_rows = list_inventario_tabla(limit=100000)
    if not base_rows:
        st.info("Primero carga inventario base para poder registrar ajustes manuales.")
        return

    option_map = {
        row["SKU"]: f"{row['SKU']} - {row.get('Descripción', '')}".strip(" -")
        for row in base_rows
        if _safe_text(row.get("SKU"))
    }
    sku_options = sorted(option_map.keys())

    selected_sku = st.selectbox(
        "SKU a ajustar",
        options=sku_options,
        format_func=lambda sku: option_map.get(sku, sku),
        key="inventario_ajuste_sku",
    )

    current_row = next((row for row in base_rows if row.get("SKU") == selected_sku), None)
    if current_row:
        a1, a2, a3 = st.columns(3)
        a1.metric("Stock actual", round(_safe_float(current_row.get("Stock actual"), 0), 2))
        a2.metric("Stock libre", round(_safe_float(current_row.get("Stock libre"), 0), 2))
        a3.metric("Stock asignado", round(_safe_float(current_row.get("Stock asignado"), 0), 2))
        st.caption(f"Descripción: {_safe_text(current_row.get('Descripción')) or 'N/D'}")

    k1, k2 = st.columns(2)
    adjustment_kind = k1.selectbox(
        "Tipo de ajuste",
        options=["ENTRADA", "SALIDA"],
        key="inventario_ajuste_kind",
        help="ENTRADA suma a stock libre. SALIDA descuenta de stock libre.",
    )
    quantity = k2.number_input(
        "Cantidad",
        min_value=0.0,
        value=0.0,
        step=1.0,
        key="inventario_ajuste_qty",
    )
    observacion = st.text_area(
        "Observación / motivo",
        key="inventario_ajuste_obs",
        height=100,
        help="Describe por qué se está haciendo el ajuste manual.",
    )

    if st.button("Aplicar ajuste manual", type="primary"):
        try:
            result = create_manual_inventory_adjustment(
                sku=selected_sku,
                adjustment_kind=adjustment_kind,
                quantity=quantity,
                observacion=observacion,
                created_by=_get_current_user(),
            )
            _set_flash(
                f"Ajuste {result['adjustment_kind']} aplicado a {result['sku']} | "
                f"existencia {result['existencia_anterior']} -> {result['existencia_nueva']}"
            )
            _rerun()
        except Exception as exc:
            _set_flash(f"No fue posible aplicar el ajuste manual: {exc}", kind="error")
            _rerun()



def _render_corte_fisico_tab() -> None:
    st.subheader("Corte físico simple")
    st.caption(
        "Compara el conteo físico contra el sistema y aplica solo las diferencias válidas. "
        "No modifica stock asignado; si el conteo queda por debajo del stock asignado, la fila se manda a revisión."
    )

    uploader = st.file_uploader(
        "Sube el archivo Excel del corte físico",
        type=["xlsx", "xlsm", "xls"],
        key="inventario_cut_uploader",
        help="Debe contener al menos: SKU, DESCRIPCION, STOCK, FECHA_ACTUALIZACION.",
    )

    if uploader is not None:
        file_name = uploader.name
        file_bytes = uploader.getvalue()
        st.session_state[CUT_PREVIEW_FILE_BYTES_KEY] = file_bytes
        st.session_state[CUT_PREVIEW_FILE_NAME_KEY] = file_name

        try:
            available_sheets = get_inventory_excel_sheets(file_bytes)
            suggested = suggest_inventory_sheet(file_bytes)
        except Exception as exc:
            st.error(f"No fue posible leer el archivo: {exc}")
            return

        selected_sheet = st.selectbox(
            "Hoja a utilizar en el corte",
            options=available_sheets,
            index=available_sheets.index(suggested) if suggested in available_sheets else 0,
            key="inventario_cut_sheet",
        )

        if st.button("Generar preview del corte", key="inventario_cut_preview_button"):
            try:
                preview = build_inventory_cut_preview(
                    file_name=file_name,
                    file_bytes=file_bytes,
                    sheet_name=selected_sheet,
                )
                st.session_state[CUT_PREVIEW_KEY] = preview
                _set_flash(
                    f"Preview del corte generado. Válidas={preview['valid_count']} | revisión={preview['review_count']}",
                    kind="success",
                )
                _rerun()
            except Exception as exc:
                _set_flash(f"No fue posible generar el preview del corte: {exc}", kind="error")
                _rerun()

    preview = st.session_state.get(CUT_PREVIEW_KEY)
    if preview:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Filas totales", _safe_int(preview.get("total_rows"), 0))
        c2.metric("Aplicables", _safe_int(preview.get("valid_count"), 0))
        c3.metric("Revisión", _safe_int(preview.get("review_count"), 0))
        c4.metric("Diferencia total", round(_safe_float(preview.get("total_diferencia"), 0.0), 2))
        c5.metric("Sin cambio", _safe_int(preview.get("conteos_sin_cambio"), 0))

        d1, d2 = st.columns(2)
        d1.metric("Conteos positivos", _safe_int(preview.get("conteos_positivos"), 0))
        d2.metric("Conteos negativos", _safe_int(preview.get("conteos_negativos"), 0))

        valid_df = _preview_to_df(
            preview.get("valid_rows", []),
            ["Fila", "SKU", "Descripción", "Stock sistema", "Stock asignado", "Stock contado", "Diferencia", "Stock libre resultante", "Fecha actualización"],
        )
        review_df = _preview_to_df(
            preview.get("review_rows", []),
            ["Fila", "SKU", "Descripción", "Stock sistema", "Stock asignado", "Stock contado", "Diferencia", "Fecha actualización", "Motivo revisión"],
        )

        st.markdown("**Filas aplicables del corte**")
        st.dataframe(valid_df, use_container_width=True, hide_index=True)
        _download_df_button(valid_df, "Descargar preview aplicable", "inventario_corte_preview_aplicable.csv")

        st.markdown("**Filas en revisión**")
        st.dataframe(review_df, use_container_width=True, hide_index=True)
        _download_df_button(review_df, "Descargar filas en revisión", "inventario_corte_preview_revision.csv")

        corte_observacion = st.text_area(
            "Observación del corte (opcional)",
            key="inventario_cut_observacion",
            height=80,
        )

        cta1, cta2 = st.columns(2)
        if cta1.button("Descartar preview del corte"):
            _clear_cut_preview()
            _rerun()

        if not _is_admin_session():
            st.warning("Para aplicar el corte debes entrar en modo Administrador.")
        elif cta2.button("Confirmar y aplicar corte", type="primary"):
            try:
                file_bytes = st.session_state.get(CUT_PREVIEW_FILE_BYTES_KEY)
                if not file_bytes:
                    raise InventarioImportError("Se perdió el archivo del corte en la sesión. Vuelve a subirlo.")
                result = apply_inventory_cut(
                    preview_payload=preview,
                    file_bytes=file_bytes,
                    created_by=_get_current_user(),
                    observacion=corte_observacion,
                )
                _clear_cut_preview()
                _set_flash(
                    f"Corte aplicado correctamente. Corte {result['corte_id']} | job {result['import_job_id']} | "
                    f"aplicadas={result['applied_rows']} | revisión={result['review_rows']} | diferencia={result['total_difference']}",
                    kind="success",
                )
                _rerun()
            except Exception as exc:
                _set_flash(f"No fue posible aplicar el corte: {exc}", kind="error")
                _rerun()

    st.markdown("---")
    st.markdown("**Últimos cortes físicos**")
    cuts_df = pd.DataFrame(list_inventario_cortes(limit=30))
    if cuts_df.empty:
        st.info("Todavía no hay cortes físicos registrados.")
    else:
        st.dataframe(cuts_df, use_container_width=True, hide_index=True)
        _download_df_button(cuts_df, "Descargar historial de cortes", "inventario_cortes_historial.csv")




def _render_salidas_envio_tab() -> None:
    st.subheader("Salidas por envío")
    st.caption(
        "Muestra el impacto del cierre de pedidos sobre inventario físico y sobre el stock asignado. "
        "Aquí se resume cuánto salió realmente y cuánto se liberó de regreso a stock por cada cierre."
    )

    resumen = get_salidas_por_envio_resumen()
    c1, c2, c3 = st.columns(3)
    c1.metric("Cierres con impacto", _safe_int(resumen.get("cierres_con_impacto"), 0))
    c2.metric("Salida física total", round(_safe_float(resumen.get("total_salida_fisica"), 0), 2))
    c3.metric("Liberado a stock", round(_safe_float(resumen.get("total_liberado_stock"), 0), 2))

    filters_col1, filters_col2 = st.columns(2)
    search_text = filters_col1.text_input(
        "Buscar por pedido, cliente o usuario",
        value="",
        key="inventario_salidas_search",
    )
    tipo_cierre = filters_col2.selectbox(
        "Tipo cierre",
        options=["", "TOTAL", "PARCIAL"],
        index=0,
        key="inventario_salidas_tipo_cierre",
    )

    rows = list_salidas_por_envio(
        search_text=search_text,
        tipo_cierre=tipo_cierre or None,
        limit=2000,
    )
    if not rows:
        st.info("Todavía no hay cierres con impacto en inventario para los filtros seleccionados.")
        return

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
    _download_df_button(df, "Descargar salidas por envío", "inventario_salidas_por_envio.csv")

    selection_options = [""] + [
        f"{_safe_text(r.get('ID_pedido'))} | {_safe_text(r.get('Cliente'))} | {_safe_text(r.get('Fecha envío'))} | cierre {_safe_text(r.get('Pedido cierre id'))}"
        for r in rows
    ]
    selected_label = st.selectbox(
        "Ver detalle de impacto por cierre",
        options=selection_options,
        index=0,
        key="inventario_salidas_selected_label",
    )
    if not selected_label:
        return

    selected_row = next(
        (r for r in rows if f"{_safe_text(r.get('ID_pedido'))} | {_safe_text(r.get('Cliente'))} | {_safe_text(r.get('Fecha envío'))} | cierre {_safe_text(r.get('Pedido cierre id'))}" == selected_label),
        None,
    )
    if not selected_row:
        return

    impact_rows = get_pedido_cierre_inventory_impact(
        pedido_cierre_id=_safe_int(selected_row.get("Pedido cierre id"), 0)
    )
    impact_df = pd.DataFrame(impact_rows)
    if impact_df.empty:
        st.info("Este cierre no tiene detalle de impacto en inventario disponible.")
        return

    st.markdown("**Detalle del impacto por SKU**")
    st.dataframe(impact_df, use_container_width=True, hide_index=True)
    _download_df_button(
        impact_df,
        "Descargar detalle del cierre",
        f"inventario_impacto_cierre_{_safe_text(selected_row.get('Pedido cierre id'))}.csv",
    )



def _render_entradas_produccion_tab() -> None:
    st.subheader("Entradas de producción con impacto en inventario")
    st.caption(
        "Resume por importación cuánto se recibió en archivo y cuánto terminó realmente en inventario, "
        "separando stock libre y stock asignado."
    )

    resumen = get_entradas_produccion_resumen()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Jobs de entradas", _safe_int(resumen.get("jobs_entradas"), 0))
    c2.metric("Recibido en archivo", round(_safe_float(resumen.get("total_recibido_archivo"), 0), 2))
    c3.metric("Aplicado a inventario", round(_safe_float(resumen.get("total_aplicado_inventario"), 0), 2))
    c4.metric("A stock libre", round(_safe_float(resumen.get("total_a_stock_libre"), 0), 2))
    c5.metric("A stock asignado", round(_safe_float(resumen.get("total_a_stock_asignado"), 0), 2))

    f1, f2 = st.columns(2)
    search_text = f1.text_input(
        "Buscar por archivo o usuario",
        value="",
        key="inventario_entradas_search",
    )
    status = f2.selectbox(
        "Estado job",
        options=["", "PENDIENTE", "PROCESANDO", "COMPLETADO", "COMPLETADO_CON_REVISION", "FALLIDO", "ANULADO"],
        index=0,
        key="inventario_entradas_status",
    )

    rows = list_entradas_produccion_jobs(
        search_text=search_text,
        status=status or None,
        limit=500,
    )
    if not rows:
        st.info("Todavía no hay importaciones de entradas de producción para los filtros seleccionados.")
        return

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
    _download_df_button(df, "Descargar resumen de entradas", "inventario_entradas_produccion.csv")

    options = [""] + [
        f"job { _safe_text(r.get('Import job id')) } | {_safe_text(r.get('Archivo'))} | {_safe_text(r.get('Estado'))}"
        for r in rows
    ]
    selected_label = st.selectbox(
        "Ver detalle por importación",
        options=options,
        index=0,
        key="inventario_entradas_selected_label",
    )
    if not selected_label:
        return

    selected_row = next(
        (r for r in rows if f"job { _safe_text(r.get('Import job id')) } | {_safe_text(r.get('Archivo'))} | {_safe_text(r.get('Estado'))}" == selected_label),
        None,
    )
    if not selected_row:
        return

    job_id = _safe_int(selected_row.get("Import job id"), 0)
    job_resumen = get_entrada_produccion_job_resumen(job_id)
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Entradas aplicadas", _safe_int(job_resumen.get("Entradas aplicadas"), 0))
    d2.metric("Decisiones aplicadas", _safe_int(job_resumen.get("Decisiones aplicadas"), 0))
    d3.metric("SKU tocados", _safe_int(job_resumen.get("SKU tocados"), 0))
    d4.metric("Última aplicación", _safe_text(job_resumen.get("Última aplicación")) or "N/D")

    impact_rows = get_entrada_produccion_job_impact(job_id)
    impact_df = pd.DataFrame(impact_rows)
    if impact_df.empty:
        st.info("Esta importación todavía no tiene impacto aplicado en inventario.")
        return

    st.markdown("**Detalle del impacto por SKU**")
    st.dataframe(impact_df, use_container_width=True, hide_index=True)
    _download_df_button(
        impact_df,
        "Descargar detalle por SKU",
        f"inventario_entradas_job_{job_id}_detalle.csv",
    )

def _render_movimientos_tab() -> None:
    st.subheader("Movimientos de inventario")
    filters = list_inventory_distinct_filter_values()

    f1, f2 = st.columns([2, 2])
    search_text = f1.text_input(
        "Buscar en movimientos",
        value=_safe_text(st.session_state.get("inventario_mov_search")),
        key="inventario_mov_search",
        help="Busca por SKU, descripción u observación.",
    )
    movimiento_tipo = f2.selectbox(
        "Tipo de movimiento",
        options=["TODOS", *filters.get("movimiento_tipo", [])],
        key="inventario_mov_tipo",
    )

    s1, s2 = st.columns(2)
    referencia_tipo = s1.selectbox(
        "Referencia tipo",
        options=["TODOS", *filters.get("referencia_tipo", [])],
        key="inventario_mov_ref",
    )
    created_by = s2.selectbox(
        "Usuario",
        options=["TODOS", *filters.get("created_by", [])],
        key="inventario_mov_user",
    )

    rows = list_inventario_movimientos(
        search_text=search_text,
        movimiento_tipo=None if movimiento_tipo == "TODOS" else movimiento_tipo,
        referencia_tipo=None if referencia_tipo == "TODOS" else referencia_tipo,
        created_by=None if created_by == "TODOS" else created_by,
        limit=2000,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        st.info("No hay movimientos para el filtro seleccionado.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"Movimientos mostrados: {len(df)}")
    _download_df_button(df, "Descargar movimientos", "inventario_movimientos.csv")



def _render_historial_tab() -> None:
    st.subheader("Historial de cargas de inventario")
    rows = list_inventario_import_jobs(limit=50)
    df = pd.DataFrame(rows)
    if df.empty:
        st.info("Todavía no hay jobs de inventario registrados.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)
    _download_df_button(df, "Descargar historial de cargas", "inventario_historial_cargas.csv")



def main() -> None:
    bootstrap_runtime()
    st.title(PAGE_TITLE)
    st.caption(
        "Inventario del MVP: carga inicial, actualización parcial, corte físico simple, cruce operativo con pedidos activos, entradas de producción, alertas, ajustes manuales y movimientos."
    )
    _render_flash()

    if not db_exists():
        st.error("La base de datos no existe. Inicialízala primero con `python -m wms_mvp.db.init_db`.")
        st.stop()

    try:
        ensure_inventory_phase1_schema()
    except Exception as exc:
        st.error(f"No fue posible preparar el esquema de inventario: {exc}")
        st.stop()

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs(
        [
            "Carga inicial",
            "Actualización parcial",
            "Corte físico",
            "Inventario operativo",
            "Alertas",
            "Ajustes",
            "Salidas por envío",
            "Entradas producción",
            "Movimientos",
            "Historial",
        ]
    )
    with tab1:
        _render_carga_inicial_tab()
    with tab2:
        _render_actualizacion_parcial_tab()
    with tab3:
        _render_corte_fisico_tab()
    with tab4:
        _render_operativo_tab()
    with tab5:
        _render_alertas_tab()
    with tab6:
        _render_ajustes_tab()
    with tab7:
        _render_salidas_envio_tab()
    with tab8:
        _render_entradas_produccion_tab()
    with tab9:
        _render_movimientos_tab()
    with tab10:
        _render_historial_tab()


if __name__ == "__main__":
    main()
