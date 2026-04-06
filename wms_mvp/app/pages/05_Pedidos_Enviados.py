from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

# -------------------------------------------------------------------
# Ajuste de path para permitir imports del paquete wms_mvp
# -------------------------------------------------------------------
PACKAGE_PARENT = Path(__file__).resolve().parents[3]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

import pandas as pd
import streamlit as st

from wms_mvp.app.bootstrap import bootstrap_runtime
from wms_mvp.core.rules.pedidos_rules import (
    VALID_ESTATUS_VENTA,
    VALID_MOTIVOS_PAUSA,
    normalize_estatus_venta,
    normalize_pause_fields,
)
from wms_mvp.core.services.enviados_service import (
    create_venta_directa_enviada,
    get_pedido_enviado_full,
)
from wms_mvp.core.services.inventario_service import get_pedido_cierre_inventory_impact
from wms_mvp.core.services.pedidos_service import get_pedidos_enviados_summary
from wms_mvp.db.connection import db_exists


PAGE_TITLE = "Pedidos Enviados"
DEFAULT_VENTA_DIRECTA_ROWS = 3
SESSION_USER_KEYS = ("wms_current_user", "current_user", "auth_user")
SESSION_ROLE_KEYS = ("wms_current_role", "current_role", "auth_role")
SESSION_ADMIN_KEYS = ("wms_admin_unlocked", "is_admin", "auth_is_admin")

SUMMARY_COLUMNS = [
    "ID_pedido",
    "Cliente",
    "Fecha captura",
    "Fecha producción",
    "Fecha logística",
    "Estatus venta",
    "% al cierre",
    "Fecha envío",
    "Tipo cierre",
    "Cerrado por",
    "Tipo registro",
    "Origen",
]

DETAIL_COLUMNS = [
    "ID_pedido",
    "Cliente",
    "Producto",
    "Color",
    "Cantidad pedida original",
    "Cantidad enviada",
    "Cantidad no enviada",
    "Fecha producción",
    "SKU",
    "Descripción",
]


# =========================================================
# Helpers generales
# =========================================================
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



def _fmt_number(value: Any) -> str:
    number = _safe_float(value)
    if float(number).is_integer():
        return str(int(number))
    return f"{number:.2f}"



def _to_optional_date(value: Any) -> date | None:
    if value in (None, "", pd.NaT):
        return None

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, pd.Timestamp):
        return value.date()

    text = _safe_text(value)
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue

    try:
        return pd.to_datetime(text, errors="coerce").date()  # type: ignore[union-attr]
    except Exception:
        return None



def _date_to_iso(value: date | datetime | str | None) -> str | None:
    parsed = _to_optional_date(value)
    return parsed.isoformat() if parsed else None



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



def _render_mode_banner() -> None:
    mode_label = "Administrador" if _is_admin_session() else "Consulta"
    st.caption(f"Modo actual: {mode_label} | Usuario: {_get_current_user()}")



def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", pd.NaT):
            if isinstance(value, str) and not value.strip():
                continue
            return value
    return None

def _canonical_estatus_venta(value: Any) -> str:
    try:
        return normalize_estatus_venta(value) or ""
    except ValueError:
        return _safe_text(value)


def _canonical_motivo_pausa(value: Any) -> str:
    try:
        _, motivo = normalize_pause_fields("Pausa", value)
        return motivo or ""
    except ValueError:
        return _safe_text(value)



# =========================================================
# Transformaciones de datos
# =========================================================
def _build_summary_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    data: list[dict[str, Any]] = []
    for row in rows:
        data.append(
            {
                "ID_pedido": _safe_text(row.get("id_pedido")),
                "Cliente": _safe_text(row.get("cliente")),
                "Fecha captura": _safe_text(row.get("fecha_captura")),
                "Fecha producción": _safe_text(row.get("fecha_produccion")),
                "Fecha logística": _safe_text(row.get("fecha_logistica")),
                "Estatus venta": _canonical_estatus_venta(row.get("estatus_venta")),
                "% al cierre": round(_safe_float(row.get("porcentaje_asignacion")), 2),
                "Fecha envío": _safe_text(row.get("fecha_envio")),
                "Tipo cierre": _safe_text(row.get("tipo_cierre")),
                "Cerrado por": _safe_text(row.get("cerrado_por") or row.get("usuario")),
                "Tipo registro": _safe_text(row.get("tipo_registro")),
                "Origen": _safe_text(row.get("origen_registro")),
            }
        )

    return pd.DataFrame(data, columns=SUMMARY_COLUMNS)



def _build_lineas_cierre_map(lineas_cierre: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    for row in lineas_cierre:
        pedido_linea_id = row.get("pedido_linea_id")
        if pedido_linea_id is None:
            continue
        try:
            result[int(pedido_linea_id)] = row
        except (TypeError, ValueError):
            continue
    return result



def _build_full_detail_df(pedido_full: dict[str, Any]) -> pd.DataFrame:
    pedido = pedido_full.get("pedido") or {}
    lineas = pedido_full.get("lineas") or []
    cierre = pedido_full.get("cierre") or {}
    lineas_cierre = pedido_full.get("lineas_cierre") or []
    cierre_map = _build_lineas_cierre_map(lineas_cierre)

    data: list[dict[str, Any]] = []
    for linea in lineas:
        cierre_linea = cierre_map.get(int(linea.get("id", 0)), {})

        cantidad_pedida_original = _safe_float(
            _first_non_empty(cierre_linea.get("cantidad_pedida_original"), linea.get("cantidad_pedida"))
        )
        cantidad_enviada = _safe_float(
            _first_non_empty(cierre_linea.get("cantidad_enviada"), linea.get("cantidad_enviada"))
        )
        cantidad_no_enviada = _safe_float(
            _first_non_empty(cierre_linea.get("cantidad_no_enviada"), linea.get("cantidad_faltante"), 0)
        )

        fecha_produccion = _safe_text(
            _first_non_empty(
                linea.get("fecha_produccion"),
                cierre_linea.get("fecha_produccion"),
                pedido.get("fecha_produccion"),
            )
        )

        data.append(
            {
                "ID_pedido": _safe_text(pedido.get("id_pedido")),
                "Cliente": _safe_text(pedido.get("cliente")),
                "Producto": _safe_text(linea.get("producto")),
                "Color": _safe_text(linea.get("color")),
                "Cantidad pedida original": cantidad_pedida_original,
                "Cantidad enviada": cantidad_enviada,
                "Cantidad no enviada": cantidad_no_enviada,
                "Fecha producción": fecha_produccion,
                "SKU": _safe_text(linea.get("sku")),
                "Descripción": _safe_text(linea.get("descripcion")),
            }
        )

    detail_df = pd.DataFrame(data, columns=DETAIL_COLUMNS)
    if not detail_df.empty:
        detail_df = detail_df.sort_values(
            by=["Producto", "Color", "SKU"],
            ascending=[True, True, True],
            kind="stable",
        )
    return detail_df



def _build_filter_options(summary_rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    def distinct(field: str) -> list[str]:
        values = {
            _safe_text(row.get(field))
            for row in summary_rows
            if _safe_text(row.get(field))
        }
        return sorted(values)

    estatus_values = {
        _canonical_estatus_venta(row.get("estatus_venta"))
        for row in summary_rows
        if _canonical_estatus_venta(row.get("estatus_venta"))
    }
    estatus_values.update(sorted(VALID_ESTATUS_VENTA))

    return {
        "id_pedido": distinct("id_pedido"),
        "cliente": distinct("cliente"),
        "estatus_venta": sorted(estatus_values),
        "tipo_cierre": distinct("tipo_cierre"),
        "tipo_registro": distinct("tipo_registro"),
    }



def _apply_filters(
    rows: list[dict[str, Any]],
    *,
    id_pedido: str | None,
    cliente: str | None,
    estatus_venta: str | None,
    tipo_cierre: str | None,
    tipo_registro: str | None,
    fecha_envio_desde: date | None,
    fecha_envio_hasta: date | None,
    fecha_produccion_desde: date | None,
    fecha_produccion_hasta: date | None,
) -> list[dict[str, Any]]:
    filtered = rows

    if id_pedido:
        filtered = [row for row in filtered if _safe_text(row.get("id_pedido")) == id_pedido]

    if cliente:
        filtered = [row for row in filtered if _safe_text(row.get("cliente")) == cliente]

    if estatus_venta:
        canonical_estatus = _canonical_estatus_venta(estatus_venta)
        filtered = [
            row
            for row in filtered
            if _canonical_estatus_venta(row.get("estatus_venta")) == canonical_estatus
        ]

    if tipo_cierre:
        filtered = [row for row in filtered if _safe_text(row.get("tipo_cierre")) == tipo_cierre]

    if tipo_registro:
        filtered = [row for row in filtered if _safe_text(row.get("tipo_registro")) == tipo_registro]

    if fecha_envio_desde:
        filtered = [
            row for row in filtered
            if (parsed := _to_optional_date(row.get("fecha_envio"))) is not None and parsed >= fecha_envio_desde
        ]

    if fecha_envio_hasta:
        filtered = [
            row for row in filtered
            if (parsed := _to_optional_date(row.get("fecha_envio"))) is not None and parsed <= fecha_envio_hasta
        ]

    if fecha_produccion_desde:
        filtered = [
            row for row in filtered
            if (parsed := _to_optional_date(row.get("fecha_produccion"))) is not None and parsed >= fecha_produccion_desde
        ]

    if fecha_produccion_hasta:
        filtered = [
            row for row in filtered
            if (parsed := _to_optional_date(row.get("fecha_produccion"))) is not None and parsed <= fecha_produccion_hasta
        ]

    return filtered



def _build_selection_label(row: dict[str, Any]) -> str:
    id_pedido = _safe_text(row.get("id_pedido")) or "SIN_ID"
    cliente = _safe_text(row.get("cliente")) or "SIN_CLIENTE"
    fecha_envio = _safe_text(row.get("fecha_envio")) or "SIN_FECHA"
    tipo_cierre = _safe_text(row.get("tipo_cierre")) or "SIN_CIERRE"
    return f"{id_pedido} | {cliente} | {fecha_envio} | {tipo_cierre}"




def _build_inventory_impact_df(pedido_id: int) -> pd.DataFrame:
    rows = get_pedido_cierre_inventory_impact(pedido_id=pedido_id)
    columns = [
        "SKU",
        "Descripción",
        "Cantidad salida física",
        "Cantidad liberada a stock",
        "Afecta total",
        "Afecta stock libre",
        "Afecta stock asignado",
        "Fecha movimiento",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    df = pd.DataFrame(rows)
    for col in columns:
        if col not in df.columns:
            df[col] = "" if col in {"SKU", "Descripción", "Fecha movimiento"} else 0
    return df[columns]


# =========================================================
# Render
# =========================================================
def _render_header(summary_rows: list[dict[str, Any]]) -> None:
    st.title(PAGE_TITLE)
    _render_mode_banner()
    st.caption("Histórico operativo de pedidos cerrados y ventas directas ya enviadas.")

    total_enviados = len(summary_rows)
    total_enviado_total = sum(1 for row in summary_rows if _safe_text(row.get("tipo_cierre")).upper() == "TOTAL")
    total_enviado_parcial = sum(1 for row in summary_rows if _safe_text(row.get("tipo_cierre")).upper() == "PARCIAL")
    total_ventas_directas = sum(
        1 for row in summary_rows if _safe_text(row.get("tipo_registro")).upper() == "VENTA_DIRECTA"
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Pedidos enviados", total_enviados)
    col2.metric("Envío total", total_enviado_total)
    col3.metric("Envío parcial", total_enviado_parcial)
    col4.metric("Ventas directas", total_ventas_directas)



def _render_venta_directa_form() -> None:
    st.subheader("Alta manual de venta directa")
    st.caption(
        "Este formulario registra ventas directas ya enviadas y las guarda en el histórico con trazabilidad operativa. "
        "El estatus de venta usa catálogo controlado del MVP; ya no se permite texto libre."
    )

    is_admin = _is_admin_session()
    current_user = _get_current_user()

    if not is_admin:
        st.info(
            "Modo consulta. Puedes revisar el histórico de enviados, pero el alta manual de venta directa solo está disponible en modo administrador."
        )
        return

    st.success(f"Modo administrador activo. Usuario operativo: {current_user}.")

    estatus_options = ["Entregar", "En cobro", "Pausa"]
    motivo_options = sorted(VALID_MOTIVOS_PAUSA)

    with st.form("venta_directa_form", clear_on_submit=False):
        col1, col2, col3 = st.columns(3)
        usuario = col1.text_input("Usuario", value=current_user)
        serie_documento = col2.text_input("Serie documento", value="VD")
        documento = col3.text_input("Documento", value="")

        col4, col5, col6 = st.columns(3)
        cliente = col4.text_input("Cliente", value="")
        estatus_venta = col5.selectbox(
            "Estatus venta",
            options=estatus_options,
            index=0,
            help="Catálogo controlado: Entregar, En cobro o Pausa.",
        )
        fecha_envio = col6.date_input("Fecha de envío", value=date.today(), format="YYYY-MM-DD")

        if estatus_venta == "Pausa":
            motivo_pausa = st.selectbox(
                "Motivo de pausa",
                options=[""] + motivo_options,
                index=0,
                help="Obligatorio cuando el estatus de venta es Pausa.",
            )
        else:
            motivo_pausa = ""
            st.caption("Motivo de pausa: solo aplica cuando el estatus es Pausa.")

        col7, col8, col9 = st.columns(3)
        fecha_captura = col7.date_input("Fecha de captura", value=date.today(), format="YYYY-MM-DD")
        fecha_produccion = col8.date_input("Fecha producción", value=date.today(), format="YYYY-MM-DD")
        fecha_logistica = col9.date_input("Fecha logística", value=date.today(), format="YYYY-MM-DD")

        observaciones = st.text_area("Observaciones", value="")

        default_rows = pd.DataFrame(
            [
                {
                    "sku": "",
                    "descripcion": "",
                    "producto": "",
                    "color": "",
                    "cantidad_enviada": 0,
                }
                for _ in range(DEFAULT_VENTA_DIRECTA_ROWS)
            ]
        )

        lineas_df = st.data_editor(
            default_rows,
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            column_config={
                "sku": st.column_config.TextColumn("SKU", required=False),
                "descripcion": st.column_config.TextColumn("Descripción"),
                "producto": st.column_config.TextColumn("Producto"),
                "color": st.column_config.TextColumn("Color"),
                "cantidad_enviada": st.column_config.NumberColumn(
                    "Cantidad enviada",
                    min_value=0.0,
                    step=1.0,
                    format="%.2f",
                ),
            },
            key="venta_directa_lineas_editor",
        )

        submitted = st.form_submit_button("Guardar venta directa", type="primary")

    if not submitted:
        return

    fecha_envio_iso = _date_to_iso(fecha_envio)
    fecha_captura_iso = _date_to_iso(fecha_captura)
    fecha_produccion_iso = _date_to_iso(fecha_produccion)
    fecha_logistica_iso = _date_to_iso(fecha_logistica)

    if not cliente.strip():
        st.error("Debes capturar el cliente.")
        return

    if not documento.strip():
        st.error("Debes capturar el documento.")
        return

    try:
        canonical_estatus, canonical_motivo = normalize_pause_fields(estatus_venta, motivo_pausa)
    except ValueError as exc:
        st.error(str(exc))
        return

    lineas_data: list[dict[str, Any]] = []
    for _, row in lineas_df.iterrows():
        sku = _safe_text(row.get("sku"))
        descripcion = _safe_text(row.get("descripcion"))
        producto = _safe_text(row.get("producto"))
        color = _safe_text(row.get("color"))
        cantidad = _safe_float(row.get("cantidad_enviada"), 0)

        if not sku and not descripcion and not producto and not color and cantidad <= 0:
            continue

        if cantidad <= 0:
            st.error("Todas las líneas capturadas deben tener una cantidad enviada mayor que cero.")
            return

        lineas_data.append(
            {
                "sku": sku,
                "descripcion": descripcion,
                "producto": producto,
                "color": color,
                "cantidad_enviada": cantidad,
                "fecha_captura": fecha_captura_iso,
                "fecha_produccion": fecha_produccion_iso,
                "fecha_logistica": fecha_logistica_iso,
                "estatus_venta": canonical_estatus,
                "motivo_pausa": canonical_motivo,
            }
        )

    if not lineas_data:
        st.error("Debes capturar al menos una línea válida para la venta directa.")
        return

    pedido_data = {
        "serie_documento": serie_documento,
        "documento": documento,
        "cliente": cliente,
        "fecha_captura": fecha_captura_iso,
        "fecha_produccion": fecha_produccion_iso,
        "fecha_logistica": fecha_logistica_iso,
        "estatus_venta": canonical_estatus,
        "motivo_pausa": canonical_motivo,
        "observaciones": observaciones,
    }

    if not _is_admin_session():
        st.error("La sesión ya no está en modo administrador. Vuelve a desbloquearla antes de guardar la venta directa.")
        return

    try:
        result = create_venta_directa_enviada(
            pedido_data=pedido_data,
            lineas_data=lineas_data,
            fecha_envio=fecha_envio_iso or date.today().isoformat(),
            usuario=usuario or None,
            observacion_cierre=observaciones or None,
            motivo_auditoria="Alta manual desde pantalla de Pedidos Enviados",
        )
    except Exception as exc:
        st.error(f"No fue posible crear la venta directa: {exc}")
        return

    pedido = (result.get("pedido_enviado") or {}).get("pedido", {})
    st.success(
        f"Venta directa guardada correctamente: {_safe_text(pedido.get('id_pedido')) or 'sin id_pedido'}"
    )
    st.rerun()



def _render_summary_table(summary_rows: list[dict[str, Any]]) -> pd.DataFrame:
    st.subheader("Histórico completo")
    summary_df = _build_summary_df(summary_rows)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
    return summary_df



def _render_filters(filter_options: dict[str, list[str]]) -> dict[str, Any]:
    st.subheader("Filtros del histórico")

    col1, col2, col3 = st.columns(3)
    id_pedido = col1.selectbox(
        "ID_pedido",
        options=[""] + filter_options["id_pedido"],
        index=0,
        key="enviados_filter_id_pedido",
    )
    cliente = col2.selectbox(
        "Cliente",
        options=[""] + filter_options["cliente"],
        index=0,
        key="enviados_filter_cliente",
    )
    estatus_venta = col3.selectbox(
        "Estatus venta",
        options=[""] + sorted({*filter_options["estatus_venta"], *VALID_ESTATUS_VENTA}),
        index=0,
        key="enviados_filter_estatus_venta",
    )

    col4, col5, col6 = st.columns(3)
    tipo_cierre = col4.selectbox(
        "Tipo cierre",
        options=[""] + filter_options["tipo_cierre"],
        index=0,
        key="enviados_filter_tipo_cierre",
    )
    tipo_registro = col5.selectbox(
        "Tipo registro",
        options=[""] + filter_options["tipo_registro"],
        index=0,
        key="enviados_filter_tipo_registro",
    )
    col6.caption("Los filtros usan calendario para evitar errores de captura.")

    col7, col8 = st.columns(2)
    fecha_produccion_desde = col7.date_input(
        "Fecha producción desde",
        value=None,
        format="YYYY-MM-DD",
        key="enviados_filter_fecha_produccion_desde",
    )
    fecha_produccion_hasta = col8.date_input(
        "Fecha producción hasta",
        value=None,
        format="YYYY-MM-DD",
        key="enviados_filter_fecha_produccion_hasta",
    )

    col9, col10 = st.columns(2)
    fecha_envio_desde = col9.date_input(
        "Fecha envío desde",
        value=None,
        format="YYYY-MM-DD",
        key="enviados_filter_fecha_envio_desde",
    )
    fecha_envio_hasta = col10.date_input(
        "Fecha envío hasta",
        value=None,
        format="YYYY-MM-DD",
        key="enviados_filter_fecha_envio_hasta",
    )

    return {
        "id_pedido": id_pedido or None,
        "cliente": cliente or None,
        "estatus_venta": estatus_venta or None,
        "tipo_cierre": tipo_cierre or None,
        "tipo_registro": tipo_registro or None,
        "fecha_envio_desde": _to_optional_date(fecha_envio_desde),
        "fecha_envio_hasta": _to_optional_date(fecha_envio_hasta),
        "fecha_produccion_desde": _to_optional_date(fecha_produccion_desde),
        "fecha_produccion_hasta": _to_optional_date(fecha_produccion_hasta),
    }



def _render_filtered_summary(summary_rows: list[dict[str, Any]], filters: dict[str, Any]) -> list[dict[str, Any]]:
    filtered_rows = _apply_filters(
        summary_rows,
        id_pedido=filters.get("id_pedido"),
        cliente=filters.get("cliente"),
        estatus_venta=filters.get("estatus_venta"),
        tipo_cierre=filters.get("tipo_cierre"),
        tipo_registro=filters.get("tipo_registro"),
        fecha_envio_desde=filters.get("fecha_envio_desde"),
        fecha_envio_hasta=filters.get("fecha_envio_hasta"),
        fecha_produccion_desde=filters.get("fecha_produccion_desde"),
        fecha_produccion_hasta=filters.get("fecha_produccion_hasta"),
    )

    st.subheader("Resultado filtrado")
    filtered_df = _build_summary_df(filtered_rows)

    if filtered_df.empty:
        st.info("No hay pedidos enviados para los filtros seleccionados.")
    else:
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)

    return filtered_rows



def _render_selected_detail(summary_rows: list[dict[str, Any]]) -> None:
    st.subheader("Detalle de pedido enviado")

    if not summary_rows:
        st.info("No hay pedidos enviados para mostrar detalle.")
        return

    options_map = {_build_selection_label(row): row for row in summary_rows if row.get("id") is not None}
    labels = [""] + list(options_map.keys())

    selected_label = st.selectbox(
        "Selecciona un pedido enviado",
        options=labels,
        index=0,
        key="selected_id_pedido_enviado",
    )

    if not selected_label:
        st.info("Selecciona un pedido enviado para mostrar su detalle.")
        return

    selected_row = options_map.get(selected_label)
    if not selected_row:
        st.warning("No se encontró el pedido seleccionado.")
        return

    pedido_id = selected_row.get("id")
    if pedido_id is None:
        st.warning("El pedido seleccionado no tiene id interno disponible.")
        return

    pedido_full = get_pedido_enviado_full(int(pedido_id))
    if not pedido_full:
        st.warning("No fue posible recuperar el detalle del pedido enviado.")
        return

    pedido = pedido_full.get("pedido") or {}
    cierre = pedido_full.get("cierre") or {}
    detail_df = _build_full_detail_df(pedido_full)

    total_lineas = len(detail_df)
    total_enviado = detail_df["Cantidad enviada"].sum() if not detail_df.empty else 0
    total_no_enviado = detail_df["Cantidad no enviada"].sum() if not detail_df.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tipo cierre", _safe_text(cierre.get("tipo_cierre")) or "N/D")
    col2.metric("Fecha envío", _safe_text(cierre.get("fecha_envio")) or "N/D")
    col3.metric("Total enviado", _fmt_number(total_enviado))
    col4.metric("Total no enviado", _fmt_number(total_no_enviado))

    st.markdown(
        "  \n".join(
            [
                f"**Cliente:** {_safe_text(pedido.get('cliente')) or 'N/D'}",
                f"**ID_pedido:** {_safe_text(pedido.get('id_pedido')) or 'N/D'}",
                f"**Fecha captura:** {_safe_text(pedido.get('fecha_captura')) or 'N/D'}",
                f"**Fecha producción:** {_safe_text(pedido.get('fecha_produccion')) or 'N/D'}",
                f"**Fecha logística:** {_safe_text(pedido.get('fecha_logistica')) or 'N/D'}",
                f"**Estatus venta:** {_safe_text(pedido.get('estatus_venta')) or 'N/D'}",
                f"**Motivo de pausa:** {_safe_text(pedido.get('motivo_pausa')) or 'N/D'}",
                f"**Tipo registro:** {_safe_text(pedido.get('tipo_registro')) or 'N/D'}",
                f"**Origen:** {_safe_text(pedido.get('origen_registro')) or 'N/D'}",
                f"**Cerrado por:** {_safe_text(cierre.get('usuario') or cierre.get('cerrado_por')) or 'N/D'}",
                f"**Observación de cierre:** {_safe_text(cierre.get('observacion')) or 'N/D'}",
                f"**Líneas:** {total_lineas}",
            ]
        )
    )

    st.dataframe(detail_df, use_container_width=True, hide_index=True)


# =========================================================
# Main
# =========================================================
def main() -> None:
    bootstrap_runtime()
    if not db_exists():
        st.error(
            "La base de datos no existe. Inicialízala primero con:\n\n"
            "`python -m wms_mvp.db.init_db`"
        )
        st.stop()

    try:
        summary_rows = get_pedidos_enviados_summary()
    except Exception as exc:
        st.error(f"No fue posible cargar la información de pedidos enviados: {exc}")
        st.stop()

    _render_header(summary_rows)

    st.markdown("---")
    with st.expander("Registrar venta directa manual", expanded=False):
        _render_venta_directa_form()

    st.markdown("---")
    _render_summary_table(summary_rows)

    st.markdown("---")
    filter_options = _build_filter_options(summary_rows)
    filters = _render_filters(filter_options)

    st.markdown("---")
    filtered_rows = _render_filtered_summary(summary_rows, filters)

    st.markdown("---")
    _render_selected_detail(filtered_rows if filtered_rows else summary_rows)


if __name__ == "__main__":
    main()
