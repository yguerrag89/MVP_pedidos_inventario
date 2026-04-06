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

import altair as alt
import pandas as pd
import streamlit as st

from wms_mvp.app.bootstrap import bootstrap_runtime
from wms_mvp.app.components.forms import render_pedido_header_edit_form
from wms_mvp.core.rules.pedidos_rules import (
    VALID_ESTATUS_VENTA,
    VALID_MOTIVOS_PAUSA,
    normalize_estatus_venta,
    normalize_motivo_pausa,
)
from wms_mvp.core.services.enviados_service import cerrar_pedido, preview_cierre_pedido
from wms_mvp.core.services.pedidos_service import (
    get_filter_options,
    get_pedido_full_by_id_pedido,
    get_pedidos_activos_detalle,
    get_pedidos_activos_summary,
    get_pedidos_chart_data,
    update_pedido_with_audit,
)
from wms_mvp.db.connection import column_exists, db_exists


PAGE_TITLE = "Pedidos Activos"
FLASH_SUCCESS_KEY = "pedidos_activos_flash_success"
CIERRE_PREVIEW_KEY = "pedidos_activos_cierre_preview"
CIERRE_PREVIEW_META_KEY = "pedidos_activos_cierre_preview_meta"
PENDING_EDIT_KEY = "pedidos_activos_pending_edit"
SUMMARY_EDITOR_KEY = "pedidos_activos_summary_editor"
SUMMARY_EDITOR_ERRORS_KEY = "pedidos_activos_summary_editor_errors"
SESSION_USER_KEYS = ("wms_current_user", "current_user", "auth_user")
SESSION_ROLE_KEYS = ("wms_current_role", "current_role", "auth_role")
SESSION_ADMIN_KEYS = ("wms_admin_unlocked", "is_admin", "auth_is_admin")

SUMMARY_COLUMNS = [
    "ID_pedido",
    "Cliente",
    "%",
    "Fecha captura",
    "Fecha producción",
    "Fecha logística",
    "Estatus venta",
    "Motivo pausa",
    "Prioridad manual",
]


# ===================================================================
# Helpers genéricos
# ===================================================================
def _rerun() -> None:
    rerun_fn = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if rerun_fn:
        rerun_fn()



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



def _fmt_percent(value: Any) -> str:
    return f"{_safe_float(value):.2f}%"



def _parse_iso_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.date()

    text = _safe_text(value)
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None



def _date_to_iso(value: date | datetime | str | None) -> str | None:
    parsed = _parse_iso_date(value)
    return parsed.isoformat() if parsed else None



def _normalize_priority(value: Any) -> int | None:
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    text = _safe_text(value)
    if not text:
        return None

    try:
        return int(round(float(text)))
    except (TypeError, ValueError):
        try:
            return int(text)
        except (TypeError, ValueError):
            return None



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



def _canonical_estatus_display(value: Any) -> str:
    return normalize_estatus_venta(value) or _safe_text(value)



def _canonical_motivo_display(value: Any) -> str:
    return normalize_motivo_pausa(value) or _safe_text(value)



def _normalize_status_for_select(current_value: str, options: list[str]) -> list[str]:
    base = {normalize_estatus_venta(opt) or _safe_text(opt) for opt in options if _safe_text(opt)}
    if current_value:
        base.add(normalize_estatus_venta(current_value) or current_value)
    base.update(VALID_ESTATUS_VENTA)
    return sorted({opt for opt in base if _safe_text(opt)})



def _series_to_date_bounds(series: pd.Series) -> tuple[date | None, date | None]:
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.isna().all():
        return None, None
    return parsed.min().date(), parsed.max().date()



def _clear_cierre_preview() -> None:
    st.session_state.pop(CIERRE_PREVIEW_KEY, None)
    st.session_state.pop(CIERRE_PREVIEW_META_KEY, None)
    st.session_state.pop("pedidos_activos_confirmar_cierre", None)



def _clear_pending_edit() -> None:
    st.session_state.pop(PENDING_EDIT_KEY, None)



def _clear_filters() -> None:
    keys_to_clear = [
        "pedidos_activos_selected_id_pedido",
        "pedidos_activos_filter_id_pedido",
        "pedidos_activos_filter_cliente",
        "pedidos_activos_filter_estatus_venta",
        "pedidos_activos_filter_producto",
        "pedidos_activos_filter_porcentaje_min",
        "pedidos_activos_filter_porcentaje_max",
        "pedidos_activos_filter_use_fecha_produccion",
        "pedidos_activos_filter_fecha_produccion_desde",
        "pedidos_activos_filter_fecha_produccion_hasta",
        "pedidos_activos_filter_use_fecha_captura",
        "pedidos_activos_filter_fecha_captura_desde",
        "pedidos_activos_filter_fecha_captura_hasta",
        "pedidos_activos_filter_use_fecha_logistica",
        "pedidos_activos_filter_fecha_logistica_desde",
        "pedidos_activos_filter_fecha_logistica_hasta",
        "pedidos_activos_chart_bars_per_view",
        "pedidos_activos_chart_start_index",
    ]
    for key in keys_to_clear:
        st.session_state.pop(key, None)



def _clear_selected_pedido_after_cierre() -> None:
    st.session_state.pop("pedidos_activos_selected_id_pedido", None)
    _clear_cierre_preview()
    _clear_pending_edit()



def _build_date_filter_inputs(
    *,
    label: str,
    use_key: str,
    desde_key: str,
    hasta_key: str,
    min_date: date | None,
    max_date: date | None,
) -> tuple[date | None, date | None]:
    use_filter = st.checkbox(
        f"Usar filtro de {label}",
        value=bool(st.session_state.get(use_key, False)),
        key=use_key,
        disabled=not (min_date and max_date),
    )

    if not use_filter or not min_date or not max_date:
        return None, None

    col1, col2 = st.columns(2)
    desde_default = st.session_state.get(desde_key) or min_date
    hasta_default = st.session_state.get(hasta_key) or max_date

    fecha_desde = col1.date_input(
        f"{label} desde",
        value=desde_default,
        key=desde_key,
        format="YYYY-MM-DD",
    )
    fecha_hasta = col2.date_input(
        f"{label} hasta",
        value=hasta_default,
        key=hasta_key,
        format="YYYY-MM-DD",
    )

    if fecha_hasta and fecha_desde and fecha_hasta < fecha_desde:
        fecha_hasta = fecha_desde

    return fecha_desde, fecha_hasta



def _build_edit_changes(pedido: dict[str, Any], payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, str]]]:
    current_estatus = _canonical_estatus_display(pedido.get("estatus_venta"))
    current_motivo = _canonical_motivo_display(pedido.get("motivo_pausa"))

    current_values = {
        "estatus_venta": current_estatus,
        "motivo_pausa": current_motivo if current_estatus == "Pausa" else None,
        "fecha_produccion": _date_to_iso(pedido.get("fecha_produccion")),
        "fecha_logistica": _date_to_iso(pedido.get("fecha_logistica")),
        "prioridad_manual": _normalize_priority(pedido.get("prioridad_manual")),
    }

    proposed_estatus = _canonical_estatus_display(payload.get("estatus_venta"))
    proposed_motivo = _canonical_motivo_display(payload.get("motivo_pausa"))
    proposed_values = {
        "estatus_venta": proposed_estatus,
        "motivo_pausa": proposed_motivo if proposed_estatus == "Pausa" else None,
        "fecha_produccion": _date_to_iso(payload.get("fecha_produccion")),
        "fecha_logistica": _date_to_iso(payload.get("fecha_logistica")),
        "prioridad_manual": _normalize_priority(payload.get("prioridad_manual")),
    }

    labels = {
        "estatus_venta": "Estatus venta",
        "motivo_pausa": "Motivo de pausa",
        "fecha_produccion": "Fecha producción",
        "fecha_logistica": "Fecha logística",
        "prioridad_manual": "Prioridad manual",
    }

    changes: dict[str, Any] = {}
    preview_rows: list[dict[str, str]] = []

    for field, new_value in proposed_values.items():
        old_value = current_values.get(field)
        if field == "prioridad_manual":
            same_value = _normalize_priority(old_value) == _normalize_priority(new_value)
        elif field in {"fecha_produccion", "fecha_logistica"}:
            same_value = _date_to_iso(old_value) == _date_to_iso(new_value)
        else:
            same_value = _safe_text(old_value) == _safe_text(new_value)

        if same_value:
            continue

        changes[field] = new_value
        preview_rows.append(
            {
                "Campo": labels[field],
                "Valor anterior": _safe_text(old_value) or "(vacío)",
                "Valor nuevo": _safe_text(new_value) or "(vacío)",
            }
        )

    return changes, preview_rows



def _get_summary_editor_base_rows(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    En la tabla resumen necesitamos motivo_pausa para no perderlo cuando el
    estatus actual ya es Pausa. La consulta resumen no siempre lo trae, por eso
    se complementa solo cuando hace falta.
    """
    if not summary_rows:
        return []

    rows: list[dict[str, Any]] = []
    for row in summary_rows:
        enriched = dict(row)
        enriched.setdefault("motivo_pausa", None)

        if _canonical_estatus_display(enriched.get("estatus_venta")) == "Pausa" and not _safe_text(enriched.get("motivo_pausa")):
            try:
                pedido_full = get_pedido_full_by_id_pedido(_safe_text(enriched.get("id_pedido")))
            except Exception:
                pedido_full = None
            if pedido_full:
                enriched["motivo_pausa"] = pedido_full.get("pedido", {}).get("motivo_pausa")

        rows.append(enriched)

    return rows



def _sanitize_summary_editor_df(summary_df: pd.DataFrame) -> pd.DataFrame:
    editor_df = summary_df.copy()

    if "Fecha producción" in editor_df.columns:
        editor_df["Fecha producción"] = pd.to_datetime(
            editor_df["Fecha producción"], errors="coerce"
        ).dt.date

    if "Fecha logística" in editor_df.columns:
        editor_df["Fecha logística"] = pd.to_datetime(
            editor_df["Fecha logística"], errors="coerce"
        ).dt.date

    if "Prioridad manual" in editor_df.columns:
        editor_df["Prioridad manual"] = pd.to_numeric(
            editor_df["Prioridad manual"], errors="coerce"
        ).astype("Int64")

    if "%" in editor_df.columns:
        editor_df["%"] = pd.to_numeric(editor_df["%"], errors="coerce").astype(float)

    for col in ["ID_pedido", "Cliente", "Fecha captura", "Estatus venta", "Motivo pausa"]:
        if col in editor_df.columns:
            editor_df[col] = editor_df[col].fillna("").astype(str)

    return editor_df


# ===================================================================
# Builders de DataFrames para UI
# ===================================================================
def _build_summary_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    supports_prioridad_manual = column_exists("pedidos", "prioridad_manual")
    data = []
    for row in rows:
        prioridad = _normalize_priority(row.get("prioridad_manual")) if supports_prioridad_manual else None
        data.append(
            {
                "ID_pedido": _safe_text(row.get("id_pedido")),
                "Cliente": _safe_text(row.get("cliente")),
                "%": round(_safe_float(row.get("porcentaje_asignacion")), 2),
                "Fecha captura": _safe_text(row.get("fecha_captura")),
                "Fecha producción": _parse_iso_date(row.get("fecha_produccion")),
                "Fecha logística": _parse_iso_date(row.get("fecha_logistica")),
                "Estatus venta": _canonical_estatus_display(row.get("estatus_venta")),
                "Motivo pausa": _canonical_motivo_display(row.get("motivo_pausa")) or "",
                "Prioridad manual": prioridad,
            }
        )

    df = pd.DataFrame(data, columns=SUMMARY_COLUMNS)
    return _sanitize_summary_editor_df(df)



def _build_detail_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "ID_pedido",
        "Cliente",
        "Producto",
        "Color",
        "Cantidad pedida",
        "Cantidad asignada",
        "Cantidad faltante",
        "%",
        "Fecha captura",
        "Fecha producción",
        "Fecha logística",
        "Estatus venta",
        "SKU",
        "Descripción",
        "Cantidad enviada",
        "Línea",
    ]

    if not rows:
        return pd.DataFrame(columns=columns)

    data = []
    for row in rows:
        fecha_produccion = (
            row.get("fecha_produccion")
            or row.get("pedido_fecha_produccion")
            or row.get("linea_fecha_produccion")
        )
        fecha_logistica = row.get("fecha_logistica") or row.get("pedido_fecha_logistica")
        estatus_venta = row.get("estatus_venta") or row.get("estatus_venta_operativo")
        data.append(
            {
                "ID_pedido": _safe_text(row.get("id_pedido")),
                "Cliente": _safe_text(row.get("cliente")),
                "Producto": _safe_text(row.get("producto")),
                "Color": _safe_text(row.get("color")),
                "Cantidad pedida": _safe_float(row.get("cantidad_pedida")),
                "Cantidad asignada": _safe_float(row.get("cantidad_asignada")),
                "Cantidad faltante": _safe_float(row.get("cantidad_faltante")),
                "%": round(_safe_float(row.get("porcentaje_asignacion")), 2),
                "Fecha captura": _safe_text(row.get("pedido_fecha_captura") or row.get("fecha_captura")),
                "Fecha producción": _safe_text(fecha_produccion),
                "Fecha logística": _safe_text(fecha_logistica),
                "Estatus venta": _canonical_estatus_display(estatus_venta),
                "SKU": _safe_text(row.get("sku")),
                "Descripción": _safe_text(row.get("descripcion")),
                "Cantidad enviada": _safe_float(row.get("cantidad_enviada")),
                "Línea": int(_safe_float(row.get("line_no"), 0)),
            }
        )

    return pd.DataFrame(data, columns=columns)



def _build_chart_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["Etiqueta", "ID_pedido", "Cliente", "% Asignación"])

    data = []
    seen: set[str] = set()
    for row in rows:
        id_pedido = _safe_text(row.get("id_pedido"))
        if not id_pedido or id_pedido in seen:
            continue
        seen.add(id_pedido)
        cliente = _safe_text(row.get("cliente"))
        data.append(
            {
                "Etiqueta": f"{id_pedido} - {cliente}" if cliente else id_pedido,
                "ID_pedido": id_pedido,
                "Cliente": cliente,
                "% Asignación": round(_safe_float(row.get("porcentaje_asignacion")), 2),
            }
        )

    chart_df = pd.DataFrame(data)
    if chart_df.empty:
        return chart_df

    return chart_df.sort_values(
        by=["% Asignación", "ID_pedido", "Cliente"],
        ascending=[True, True, True],
    ).reset_index(drop=True)



def _build_cierre_preview_df(preview_payload: dict[str, Any]) -> pd.DataFrame:
    lineas_snapshot = preview_payload.get("cierre_preview", {}).get("lineas_snapshot", [])
    lineas = preview_payload.get("lineas", [])
    lineas_map = {int(row["id"]): row for row in lineas if row.get("id") is not None}

    data = []
    for snapshot in lineas_snapshot:
        linea = lineas_map.get(int(snapshot.get("pedido_linea_id", 0)), {})
        data.append(
            {
                "Línea": int(_safe_float(linea.get("line_no"), 0)),
                "SKU": _safe_text(linea.get("sku")),
                "Descripción": _safe_text(linea.get("descripcion")),
                "Producto": _safe_text(linea.get("producto")),
                "Color": _safe_text(linea.get("color")),
                "Pedida": _safe_float(snapshot.get("cantidad_pedida_original")),
                "Asignada al cierre": _safe_float(snapshot.get("cantidad_asignada_al_cierre")),
                "Se enviará": _safe_float(snapshot.get("cantidad_enviada")),
                "Quedará no enviado": _safe_float(snapshot.get("cantidad_no_enviada")),
            }
        )

    return pd.DataFrame(data)



def _build_filter_options(summary_rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    service_options = get_filter_options()

    id_pedido_options = sorted(
        {
            _safe_text(row.get("id_pedido"))
            for row in summary_rows
            if _safe_text(row.get("id_pedido"))
        }
    )
    cliente_options = sorted(
        set(service_options.get("clientes", []))
        | {
            _safe_text(row.get("cliente"))
            for row in summary_rows
            if _safe_text(row.get("cliente"))
        }
    )
    estatus_options = sorted(
        {
            canonical
            for canonical in (
                *[_canonical_estatus_display(item) for item in service_options.get("estatus_venta", [])],
                *[_canonical_estatus_display(row.get("estatus_venta")) for row in summary_rows],
                *sorted(VALID_ESTATUS_VENTA),
            )
            if canonical in VALID_ESTATUS_VENTA
        }
    )
    producto_options = sorted(set(service_options.get("productos", [])))

    return {
        "id_pedido": id_pedido_options,
        "cliente": cliente_options,
        "estatus_venta": estatus_options,
        "producto": producto_options,
    }



def _get_selected_summary_row(summary_rows: list[dict[str, Any]], id_pedido: str | None) -> dict[str, Any] | None:
    if not id_pedido:
        return None
    for row in summary_rows:
        if _safe_text(row.get("id_pedido")) == id_pedido:
            return row
    return None



def _build_summary_editor_changes(
    source_rows: list[dict[str, Any]],
    edited_df: pd.DataFrame,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    if edited_df is None or edited_df.empty:
        return [], pd.DataFrame(columns=["ID_pedido", "Campo", "Valor anterior", "Valor nuevo"])

    source_map = {
        _safe_text(row.get("id_pedido")): dict(row)
        for row in source_rows
        if _safe_text(row.get("id_pedido"))
    }

    changes_to_save: list[dict[str, Any]] = []
    preview_rows: list[dict[str, str]] = []

    for _, edited_row in edited_df.iterrows():
        id_pedido = _safe_text(edited_row.get("ID_pedido"))
        if not id_pedido or id_pedido not in source_map:
            continue

        source_row = source_map[id_pedido]
        payload = {
            "estatus_venta": edited_row.get("Estatus venta"),
            "motivo_pausa": edited_row.get("Motivo pausa"),
            "fecha_produccion": edited_row.get("Fecha producción"),
            "fecha_logistica": edited_row.get("Fecha logística"),
            "prioridad_manual": edited_row.get("Prioridad manual"),
        }

        changes, row_preview = _build_edit_changes(source_row, payload)
        if not changes:
            continue

        changes_to_save.append(
            {
                "pedido_id": int(source_row.get("id", 0) or 0),
                "id_pedido": id_pedido,
                "changes": changes,
            }
        )
        for item in row_preview:
            preview_rows.append(
                {
                    "ID_pedido": id_pedido,
                    "Campo": item.get("Campo", ""),
                    "Valor anterior": item.get("Valor anterior", ""),
                    "Valor nuevo": item.get("Valor nuevo", ""),
                }
            )

    return changes_to_save, pd.DataFrame(preview_rows, columns=["ID_pedido", "Campo", "Valor anterior", "Valor nuevo"])


# ===================================================================
# Render principal
# ===================================================================
def _render_header(summary_rows: list[dict[str, Any]]) -> None:
    st.title(PAGE_TITLE)
    st.caption(
        "Vista principal operativa. La tabla resumen no cambia con filtros; los filtros solo afectan el detalle y el gráfico."
    )

    if st.session_state.get(FLASH_SUCCESS_KEY):
        st.success(st.session_state.get(FLASH_SUCCESS_KEY))
        st.session_state.pop(FLASH_SUCCESS_KEY, None)

    total_pedidos = len(summary_rows)
    total_lineas_estimado = int(sum(_safe_float(row.get("total_lineas"), 0) for row in summary_rows))
    promedio_asignacion = round(
        sum(_safe_float(row.get("porcentaje_asignacion")) for row in summary_rows) / total_pedidos,
        2,
    ) if total_pedidos else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("Pedidos activos", total_pedidos)
    col2.metric("Promedio % asignación", _fmt_percent(promedio_asignacion))
    col3.metric("Líneas estimadas", total_lineas_estimado)



def _render_summary_table(summary_rows: list[dict[str, Any]]) -> pd.DataFrame:
    st.subheader("Tabla resumen de pedidos activos")

    editor_rows = _get_summary_editor_base_rows(summary_rows)
    summary_df = _build_summary_df(editor_rows)

    if summary_df.empty:
        st.info("No hay pedidos activos cargados.")
        return summary_df

    if not _is_admin_session():
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        st.caption("Esta tabla es una vista general fija. Los filtros de abajo no cambian este resumen.")
        return summary_df

    st.caption(
        "Modo Administrador: puedes editar directamente en la tabla los campos permitidos del encabezado. "
        "Después pulsa **Guardar cambios de la tabla** para persistir y auditar los cambios."
    )

    status_options = _normalize_status_for_select("", sorted(VALID_ESTATUS_VENTA))
    motivo_options = ["", *sorted(VALID_MOTIVOS_PAUSA)]
    editor_df = _sanitize_summary_editor_df(summary_df)

    edited_df = st.data_editor(
        editor_df,
        key=SUMMARY_EDITOR_KEY,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=["ID_pedido", "Cliente", "%", "Fecha captura"],
        column_config={
            "ID_pedido": st.column_config.TextColumn("ID_pedido", disabled=True),
            "Cliente": st.column_config.TextColumn("Cliente", disabled=True),
            "%": st.column_config.NumberColumn("%", format="%.2f", disabled=True),
            "Fecha captura": st.column_config.TextColumn("Fecha captura", disabled=True),
            "Fecha producción": st.column_config.DateColumn("Fecha producción"),
            "Fecha logística": st.column_config.DateColumn("Fecha logística"),
            "Estatus venta": st.column_config.SelectboxColumn(
                "Estatus venta",
                options=status_options,
                required=False,
            ),
            "Motivo pausa": st.column_config.SelectboxColumn(
                "Motivo pausa",
                options=motivo_options,
                required=False,
                help="Solo aplica cuando el estatus venta es Pausa.",
            ),
            "Prioridad manual": st.column_config.NumberColumn(
                "Prioridad manual",
                min_value=1,
                max_value=999,
                step=1,
                help="1 = más alta prioridad operativa. 3 = normal. 5 = baja.",
            ),
        },
    )

    changes_to_save, preview_df = _build_summary_editor_changes(editor_rows, edited_df)
    if not preview_df.empty:
        with st.expander("Ver cambios detectados en la tabla", expanded=False):
            st.dataframe(preview_df, use_container_width=True, hide_index=True)

    btn1, btn2 = st.columns([1, 1])
    if btn1.button("Guardar cambios de la tabla", type="primary"):
        if not changes_to_save:
            st.info("No hay cambios por guardar en la tabla resumen.")
        else:
            ok_items: list[str] = []
            error_items: list[str] = []
            for item in changes_to_save:
                pedido_id = int(item.get("pedido_id", 0) or 0)
                id_pedido = _safe_text(item.get("id_pedido"))
                if pedido_id <= 0:
                    error_items.append(f"{id_pedido}: no se identificó el id interno del pedido.")
                    continue
                try:
                    update_pedido_with_audit(
                        pedido_id=pedido_id,
                        changes=dict(item.get("changes", {})),
                        usuario=_get_current_user(),
                        motivo="EDICION_MASIVA_DESDE_TABLA_PEDIDOS_ACTIVOS_UI",
                    )
                    ok_items.append(id_pedido)
                except Exception as exc:
                    error_items.append(f"{id_pedido}: {exc}")

            if ok_items:
                st.session_state[FLASH_SUCCESS_KEY] = (
                    f"Se actualizaron correctamente {len(ok_items)} pedido(s): {', '.join(ok_items)}."
                )
            if error_items:
                st.session_state[SUMMARY_EDITOR_ERRORS_KEY] = error_items
            _clear_pending_edit()
            _clear_cierre_preview()
            _rerun()

    if btn2.button("Recargar tabla y descartar ediciones no guardadas"):
        st.session_state.pop(SUMMARY_EDITOR_KEY, None)
        _rerun()

    errors = st.session_state.pop(SUMMARY_EDITOR_ERRORS_KEY, None)
    if errors:
        st.error("No se pudieron guardar algunos cambios:")
        for item in errors:
            st.write(f"- {item}")

    st.caption("Esta tabla resumen sigue siendo fija respecto a los filtros operativos de abajo.")
    return summary_df



def _render_summary_selector(summary_rows: list[dict[str, Any]]) -> str | None:
    st.subheader("Seleccionar pedido del resumen")

    options = [""] + [
        _safe_text(row.get("id_pedido")) for row in summary_rows if _safe_text(row.get("id_pedido"))
    ]
    label_map = {
        _safe_text(row.get("id_pedido")): (
            f"{_safe_text(row.get('id_pedido'))} - {_safe_text(row.get('cliente'))}"
            if _safe_text(row.get("cliente"))
            else _safe_text(row.get("id_pedido"))
        )
        for row in summary_rows
        if _safe_text(row.get("id_pedido"))
    }
    label_map[""] = ""

    current_value = _safe_text(st.session_state.get("pedidos_activos_selected_id_pedido", ""))
    if current_value and current_value not in options:
        options.append(current_value)
        label_map[current_value] = current_value

    selected_id = st.selectbox(
        "Pedido (ID_pedido - cliente)",
        options=options,
        index=options.index(current_value) if current_value in options else 0,
        key="pedidos_activos_selected_id_pedido",
        format_func=lambda value: label_map.get(value, value),
        help="Busca y selecciona por ID_pedido o por cliente para ver su snapshot y acciones debajo.",
    )

    if selected_id != _safe_text(st.session_state.get("pedidos_activos_selected_id_pedido_prev", "")):
        _clear_cierre_preview()
        _clear_pending_edit()
        st.session_state["pedidos_activos_selected_id_pedido_prev"] = selected_id

    return _safe_text(selected_id) or None



def _render_filters(filter_options: dict[str, list[str]], summary_df: pd.DataFrame) -> dict[str, Any]:
    st.subheader("Filtros operativos")
    st.caption(
        "Estos filtros afectan únicamente la tabla detalle y el gráfico. El filtro principal es Fecha producción."
    )

    fecha_prod_min, fecha_prod_max = _series_to_date_bounds(summary_df.get("Fecha producción", pd.Series(dtype="object")))
    fecha_cap_min, fecha_cap_max = _series_to_date_bounds(summary_df.get("Fecha captura", pd.Series(dtype="object")))
    fecha_log_min, fecha_log_max = _series_to_date_bounds(summary_df.get("Fecha logística", pd.Series(dtype="object")))

    with st.container(border=True):
        row1 = st.columns(4)
        selected_id = row1[0].multiselect(
            "ID_pedido",
            options=filter_options.get("id_pedido", []),
            default=st.session_state.get("pedidos_activos_filter_id_pedido", []),
            key="pedidos_activos_filter_id_pedido",
        )
        selected_cliente = row1[1].multiselect(
            "Cliente",
            options=filter_options.get("cliente", []),
            default=st.session_state.get("pedidos_activos_filter_cliente", []),
            key="pedidos_activos_filter_cliente",
        )
        selected_estatus = row1[2].multiselect(
            "Estatus venta",
            options=filter_options.get("estatus_venta", []),
            default=st.session_state.get("pedidos_activos_filter_estatus_venta", []),
            key="pedidos_activos_filter_estatus_venta",
        )
        selected_producto = row1[3].multiselect(
            "Producto",
            options=filter_options.get("producto", []),
            default=st.session_state.get("pedidos_activos_filter_producto", []),
            key="pedidos_activos_filter_producto",
        )

        row2 = st.columns(2)
        porcentaje_min = row2[0].number_input(
            "% mínimo",
            min_value=0.0,
            max_value=100.0,
            value=float(st.session_state.get("pedidos_activos_filter_porcentaje_min", 0.0)),
            step=1.0,
            key="pedidos_activos_filter_porcentaje_min",
        )
        porcentaje_max = row2[1].number_input(
            "% máximo",
            min_value=0.0,
            max_value=100.0,
            value=float(st.session_state.get("pedidos_activos_filter_porcentaje_max", 100.0)),
            step=1.0,
            key="pedidos_activos_filter_porcentaje_max",
        )
        if porcentaje_max < porcentaje_min:
            porcentaje_max = porcentaje_min

        st.markdown("**Filtros de fecha opcionales**")
        fecha_produccion_desde, fecha_produccion_hasta = _build_date_filter_inputs(
            label="Fecha producción",
            use_key="pedidos_activos_filter_use_fecha_produccion",
            desde_key="pedidos_activos_filter_fecha_produccion_desde",
            hasta_key="pedidos_activos_filter_fecha_produccion_hasta",
            min_date=fecha_prod_min,
            max_date=fecha_prod_max,
        )
        fecha_captura_desde, fecha_captura_hasta = _build_date_filter_inputs(
            label="Fecha captura",
            use_key="pedidos_activos_filter_use_fecha_captura",
            desde_key="pedidos_activos_filter_fecha_captura_desde",
            hasta_key="pedidos_activos_filter_fecha_captura_hasta",
            min_date=fecha_cap_min,
            max_date=fecha_cap_max,
        )
        fecha_logistica_desde, fecha_logistica_hasta = _build_date_filter_inputs(
            label="Fecha logística",
            use_key="pedidos_activos_filter_use_fecha_logistica",
            desde_key="pedidos_activos_filter_fecha_logistica_desde",
            hasta_key="pedidos_activos_filter_fecha_logistica_hasta",
            min_date=fecha_log_min,
            max_date=fecha_log_max,
        )

        btn1, btn2 = st.columns([1, 1])
        if btn1.button("Limpiar filtros"):
            _clear_filters()
            _rerun()
        btn2.caption("La tabla resumen no cambia con estos filtros.")

    filters: dict[str, Any] = {}
    if selected_id:
        filters["id_pedido"] = selected_id
    if selected_cliente:
        filters["cliente"] = selected_cliente
    if selected_estatus:
        filters["estatus_venta"] = selected_estatus
    if selected_producto:
        filters["producto"] = selected_producto

    filters["porcentaje_min"] = porcentaje_min
    filters["porcentaje_max"] = porcentaje_max

    iso_prod_desde = _date_to_iso(fecha_produccion_desde)
    iso_prod_hasta = _date_to_iso(fecha_produccion_hasta)
    iso_cap_desde = _date_to_iso(fecha_captura_desde)
    iso_cap_hasta = _date_to_iso(fecha_captura_hasta)
    iso_log_desde = _date_to_iso(fecha_logistica_desde)
    iso_log_hasta = _date_to_iso(fecha_logistica_hasta)

    if iso_prod_desde:
        filters["fecha_produccion_desde"] = iso_prod_desde
    if iso_prod_hasta:
        filters["fecha_produccion_hasta"] = iso_prod_hasta
    if iso_cap_desde:
        filters["fecha_captura_desde"] = iso_cap_desde
    if iso_cap_hasta:
        filters["fecha_captura_hasta"] = iso_cap_hasta
    if iso_log_desde:
        filters["fecha_logistica_desde"] = iso_log_desde
    if iso_log_hasta:
        filters["fecha_logistica_hasta"] = iso_log_hasta

    return filters



def _render_selected_pedido_snapshot(summary_rows: list[dict[str, Any]], selected_id_pedido: str | None) -> dict[str, Any] | None:
    st.subheader("Pedido seleccionado")

    if not selected_id_pedido:
        st.info("Selecciona un pedido de la tabla resumen para ver su snapshot operativo.")
        return None

    summary_row = _get_selected_summary_row(summary_rows, selected_id_pedido)
    if summary_row is None:
        st.warning(f"No se encontró el pedido {selected_id_pedido} en el resumen actual.")
        return None

    pedido_full = get_pedido_full_by_id_pedido(selected_id_pedido)
    if pedido_full is None:
        st.warning(f"No se pudo recuperar el detalle del pedido {selected_id_pedido}.")
        return None

    pedido = pedido_full.get("pedido", {})
    lineas = pedido_full.get("lineas", [])

    top1, top2, top3, top4 = st.columns(4)
    top1.metric("Cliente", _safe_text(pedido.get("cliente")) or "N/D")
    top2.metric("% asignación", _fmt_percent(pedido.get("porcentaje_asignacion")))
    top3.metric("Fecha producción", _safe_text(pedido.get("fecha_produccion")) or "N/D")
    top4.metric("Fecha logística", _safe_text(pedido.get("fecha_logistica")) or "N/D")
    st.caption(f"Prioridad manual: {_safe_text(pedido.get('prioridad_manual')) or 'N/D'}")

    estatus_display = _canonical_estatus_display(pedido.get("estatus_venta")) or "N/D"
    motivo_pausa = _canonical_motivo_display(pedido.get("motivo_pausa"))
    estado_operativo = _safe_text(pedido.get("estado_operativo")) or "N/D"
    caption = f"Estatus venta: {estatus_display}"
    if estatus_display == "Pausa" and motivo_pausa:
        caption += f" | Motivo de pausa: {motivo_pausa}"
    caption += f" | Estado operativo: {estado_operativo}"
    st.caption(caption)

    lineas_df = _build_detail_df(lineas)
    if not lineas_df.empty:
        st.dataframe(lineas_df, use_container_width=True, hide_index=True)

    return pedido_full



def _render_edit_section(pedido_full: dict[str, Any] | None, filter_options: dict[str, list[str]]) -> None:
    st.subheader("Edición controlada")

    if pedido_full is None:
        st.info("Selecciona un pedido para habilitar la edición controlada del encabezado.")
        return

    if not _is_admin_session():
        st.caption("La edición está disponible solo en modo Administrador.")
        return

    pedido = pedido_full.get("pedido", {})
    pedido_id = int(pedido.get("id", 0) or 0)
    pedido_id_negocio = _safe_text(pedido.get("id_pedido"))
    if pedido_id <= 0:
        st.warning("No se pudo identificar el pedido interno para edición.")
        return

    pending_edit = st.session_state.get(PENDING_EDIT_KEY)
    if pending_edit and pending_edit.get("pedido_id") != pedido_id:
        _clear_pending_edit()
        pending_edit = None

    extra_estatus = filter_options.get("estatus_venta", [])
    extra_motivos = sorted(VALID_MOTIVOS_PAUSA)

    with st.expander("Editar campos permitidos del pedido", expanded=bool(pending_edit)):
        st.caption(
            "Esta sección sigue disponible como edición puntual. Ahora también puedes editar directamente desde la tabla resumen de arriba. "
            "Aquí solo se permite modificar: estatus_venta, motivo_pausa, prioridad manual, fecha_producción y fecha_logística."
        )

        payload = render_pedido_header_edit_form(
            pedido,
            is_admin=True,
            key_prefix="pedidos_activos",
            extra_estatus_options=extra_estatus,
            extra_motivo_pausa_options=extra_motivos,
        )

        if payload is not None:
            changes, preview_rows = _build_edit_changes(pedido, payload)
            if not changes:
                _clear_pending_edit()
                st.info("No hay cambios para guardar.")
            else:
                st.session_state[PENDING_EDIT_KEY] = {
                    "pedido_id": pedido_id,
                    "id_pedido": pedido_id_negocio,
                    "changes": changes,
                    "preview_rows": preview_rows,
                }
                _rerun()

        pending_edit = st.session_state.get(PENDING_EDIT_KEY)
        if not pending_edit or pending_edit.get("pedido_id") != pedido_id:
            return

        st.markdown("**Confirmación previa al guardado**")
        preview_df = pd.DataFrame(pending_edit.get("preview_rows", []))
        if not preview_df.empty:
            st.dataframe(preview_df, use_container_width=True, hide_index=True)

        col1, col2 = st.columns([1, 1])
        if col1.button("Confirmar y guardar cambios", type="primary"):
            try:
                update_pedido_with_audit(
                    pedido_id=pedido_id,
                    changes=dict(pending_edit.get("changes", {})),
                    usuario=_get_current_user(),
                    motivo="EDICION_DESDE_PEDIDOS_ACTIVOS_UI",
                )
                st.session_state[FLASH_SUCCESS_KEY] = f"Pedido {pedido_id_negocio} actualizado correctamente."
                _clear_pending_edit()
                _clear_cierre_preview()
                _rerun()
            except Exception as exc:
                st.error(f"No fue posible guardar los cambios del pedido: {exc}")

        if col2.button("Cancelar cambios"):
            _clear_pending_edit()
            _rerun()



def _render_cierre_action(pedido_full: dict[str, Any] | None) -> None:
    st.subheader("Cerrar y mover a enviados")

    if pedido_full is None:
        st.info("Selecciona un pedido para habilitar la vista previa y el cierre.")
        return

    pedido = pedido_full.get("pedido", {})
    lineas = pedido_full.get("lineas", [])
    selected_id = _safe_text(pedido.get("id_pedido"))
    estado_operativo = _safe_text(pedido.get("estado_operativo"))

    if estado_operativo and estado_operativo != "ACTIVO":
        st.warning(f"El pedido {selected_id} ya no está ACTIVO. Estado actual: {estado_operativo}.")
        return

    if not _is_admin_session():
        total_pedido = sum(_safe_float(row.get("cantidad_pedida")) for row in lineas)
        total_asignado = sum(_safe_float(row.get("cantidad_asignada")) for row in lineas)
        total_faltante = sum(_safe_float(row.get("cantidad_faltante")) for row in lineas)
        col1, col2, col3 = st.columns(3)
        col1.metric("Total pedido", _fmt_number(total_pedido))
        col2.metric("Total asignado", _fmt_number(total_asignado))
        col3.metric("Total faltante", _fmt_number(total_faltante))
        st.caption("La vista previa y el cierre solo están disponibles en modo Administrador.")
        return

    porcentaje_asignacion = _safe_float(pedido.get("porcentaje_asignacion"))
    default_tipo_cierre = "TOTAL" if porcentaje_asignacion >= 100.0 else "PARCIAL"
    default_user = _get_current_user()
    if default_user == "Consulta":
        default_user = "admin"

    with st.form("pedidos_activos_form_preview_cierre", clear_on_submit=False):
        col1, col2 = st.columns(2)
        tipo_cierre = col1.selectbox(
            "Tipo de cierre",
            options=["TOTAL", "PARCIAL"],
            index=0 if default_tipo_cierre == "TOTAL" else 1,
        )
        fecha_envio_input = col2.date_input("Fecha de envío", value=date.today(), format="YYYY-MM-DD")

        col3, col4 = st.columns([2, 3])
        usuario = col3.text_input(
            "Usuario que cierra",
            value=_safe_text(st.session_state.get("pedidos_activos_usuario_cierre")) or default_user,
            key="pedidos_activos_usuario_cierre",
        )
        observacion = col4.text_area(
            "Observación del cierre (opcional)",
            value=_safe_text(st.session_state.get("pedidos_activos_observacion_cierre")),
            key="pedidos_activos_observacion_cierre",
            height=80,
        )
        submitted_preview = st.form_submit_button("Generar vista previa de cierre")

    if submitted_preview:
        try:
            fecha_envio = _date_to_iso(fecha_envio_input)
            if not fecha_envio:
                raise ValueError("La fecha de envío no es válida.")

            preview_payload = preview_cierre_pedido(
                pedido_id=int(pedido["id"]),
                tipo_cierre=tipo_cierre,
                fecha_envio=fecha_envio,
                usuario=usuario,
                observacion=observacion,
            )
            st.session_state[CIERRE_PREVIEW_KEY] = preview_payload
            st.session_state[CIERRE_PREVIEW_META_KEY] = {
                "id_pedido": selected_id,
                "tipo_cierre": tipo_cierre,
                "fecha_envio": fecha_envio,
                "usuario": _safe_text(usuario),
                "observacion": _safe_text(observacion),
            }
            _rerun()
        except Exception as exc:
            _clear_cierre_preview()
            st.error(f"No fue posible generar la vista previa del cierre: {exc}")

    preview_meta = st.session_state.get(CIERRE_PREVIEW_META_KEY, {})
    preview_payload = st.session_state.get(CIERRE_PREVIEW_KEY)

    if not preview_payload or preview_meta.get("id_pedido") != selected_id:
        st.caption("Genera una vista previa antes de cerrar. El cierre deja evidencia en base y mueve el pedido a enviados.")
        return

    cierre_preview = preview_payload.get("cierre_preview", {})
    header = cierre_preview.get("header", {})
    lineas_snapshot = cierre_preview.get("lineas_snapshot", [])
    backorder_candidates = preview_payload.get("backorder_candidates", [])

    total_enviado = sum(_safe_float(row.get("cantidad_enviada")) for row in lineas_snapshot)
    total_no_enviado = sum(_safe_float(row.get("cantidad_no_enviada")) for row in lineas_snapshot)
    total_asignado = sum(_safe_float(row.get("cantidad_asignada")) for row in lineas)

    st.success("Vista previa de cierre generada correctamente.")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tipo de cierre", _safe_text(header.get("tipo_cierre")) or "N/D")
    col2.metric("Fecha envío", _safe_text(header.get("fecha_envio")) or "N/D")
    col3.metric("Total asignado", _fmt_number(total_asignado))
    col4.metric("Total a enviar", _fmt_number(total_enviado))

    if total_no_enviado > 0:
        st.warning(
            f"Este cierre dejará {_fmt_number(total_no_enviado)} piezas no enviadas en el histórico del pedido original."
        )

    preview_df = _build_cierre_preview_df(preview_payload)
    st.dataframe(preview_df, use_container_width=True, hide_index=True)

    if backorder_candidates:
        with st.expander("Líneas candidatas para un posible nuevo pedido posterior", expanded=False):
            st.dataframe(pd.DataFrame(backorder_candidates), use_container_width=True, hide_index=True)
            st.caption("Estas cantidades no se crean automáticamente como nuevo pedido. Solo se muestran como referencia para captura posterior.")

    confirm = st.checkbox(
        "Confirmo que deseo cerrar y mover este pedido a enviados.",
        key="pedidos_activos_confirmar_cierre",
    )

    btn1, btn2 = st.columns([1, 2])
    if btn1.button("Cerrar y mover a enviados", type="primary"):
        if not confirm:
            st.error("Debes confirmar el cierre antes de continuar.")
        else:
            try:
                result = cerrar_pedido(
                    pedido_id=int(pedido["id"]),
                    tipo_cierre=_safe_text(header.get("tipo_cierre")),
                    fecha_envio=_safe_text(header.get("fecha_envio")),
                    usuario=_safe_text(preview_meta.get("usuario")) or None,
                    observacion=_safe_text(preview_meta.get("observacion")) or None,
                    motivo_auditoria="CIERRE_DESDE_PEDIDOS_ACTIVOS_UI",
                )
                pedido_enviado = result.get("pedido_enviado", {})
                cierre = pedido_enviado.get("cierre", {}) if isinstance(pedido_enviado, dict) else {}
                tipo_cierre_result = _safe_text(cierre.get("tipo_cierre")) or _safe_text(header.get("tipo_cierre"))
                st.session_state[FLASH_SUCCESS_KEY] = f"Pedido {selected_id} cerrado como {tipo_cierre_result} y movido a enviados."
                _clear_selected_pedido_after_cierre()
                _rerun()
            except Exception as exc:
                st.error(f"No fue posible cerrar el pedido: {exc}")

    if btn2.button("Descartar vista previa"):
        _clear_cierre_preview()
        _rerun()



def _render_detail(filters: dict[str, Any]) -> list[dict[str, Any]]:
    st.subheader("Tabla detalle")
    try:
        detail_rows = get_pedidos_activos_detalle(filters)
    except Exception as exc:
        st.error(f"No fue posible cargar el detalle de pedidos activos: {exc}")
        return []

    detail_df = _build_detail_df(detail_rows)
    if detail_df.empty:
        st.info("No hay líneas de pedidos activos para los filtros seleccionados.")
        return []

    st.dataframe(detail_df, use_container_width=True, hide_index=True)
    st.caption(f"Filas mostradas: {len(detail_df)}")
    return detail_rows



def _render_chart(filters: dict[str, Any]) -> None:
    st.subheader("Gráfico de % de asignación")

    try:
        chart_rows = get_pedidos_chart_data(filters)
    except Exception as exc:
        st.error(f"No fue posible cargar el gráfico de pedidos activos: {exc}")
        return

    chart_df = _build_chart_df(chart_rows)
    if chart_df.empty:
        st.info("No hay pedidos activos para el conjunto filtrado.")
        return

    st.caption(
        "Para que se vean todas las etiquetas del eje X, el gráfico se muestra por ventana. "
        "Usa la barra para moverte horizontalmente entre pedidos."
    )

    total_barras = len(chart_df)
    max_visible_default = min(12, total_barras)
    max_visible_limit = min(40, total_barras)

    col1, col2 = st.columns([1, 2])
    barras_por_vista = col1.slider(
        "Pedidos visibles",
        min_value=1,
        max_value=max_visible_limit,
        value=min(max_visible_default, max_visible_limit),
        key="pedidos_activos_chart_bars_per_view",
    )

    if total_barras > barras_por_vista:
        max_start = total_barras - barras_por_vista
        start_index = col2.slider(
            "Barra para mover el gráfico",
            min_value=0,
            max_value=max_start,
            value=min(int(st.session_state.get("pedidos_activos_chart_start_index", 0)), max_start),
            step=1,
            key="pedidos_activos_chart_start_index",
            help="Desplaza la ventana horizontal del gráfico para ver el resto de pedidos.",
        )
    else:
        start_index = 0

    visible_df = chart_df.iloc[start_index:start_index + barras_por_vista].reset_index(drop=True)
    chart_width = max(900, len(visible_df) * 110)

    chart = (
        alt.Chart(visible_df)
        .mark_bar()
        .encode(
            x=alt.X(
                "Etiqueta:N",
                sort=visible_df["Etiqueta"].tolist(),
                axis=alt.Axis(
                    title="ID_pedido + cliente",
                    labelAngle=-35,
                    labelLimit=1000,
                    labelOverlap=False,
                ),
            ),
            y=alt.Y(
                "% Asignación:Q",
                scale=alt.Scale(domain=[0, 100]),
                axis=alt.Axis(title="% de asignación", format=".0f"),
            ),
            tooltip=[
                alt.Tooltip("ID_pedido:N", title="ID_pedido"),
                alt.Tooltip("Cliente:N", title="Cliente"),
                alt.Tooltip("% Asignación:Q", title="% Asignación", format=".2f"),
            ],
        )
        .properties(width=chart_width, height=380)
    )

    st.caption(
        f"Mostrando pedidos {start_index + 1} a {min(start_index + barras_por_vista, total_barras)} de {total_barras}."
    )
    st.altair_chart(chart, use_container_width=False)

    with st.expander("Ver datos del gráfico", expanded=False):
        st.dataframe(chart_df, use_container_width=True, hide_index=True)



def main() -> None:
    bootstrap_runtime()

    if not db_exists():
        st.error(
            "La base de datos no existe. Inicialízala primero con:\n\n"
            "`python -m wms_mvp.db.init_db`"
        )
        st.stop()

    try:
        summary_rows = get_pedidos_activos_summary()
    except Exception as exc:
        st.error(f"No fue posible cargar los pedidos activos: {exc}")
        st.stop()

    _render_header(summary_rows)

    st.markdown("---")
    summary_df = _render_summary_table(summary_rows)

    st.markdown("---")
    selected_id_pedido = _render_summary_selector(summary_rows)

    st.markdown("---")
    pedido_full = _render_selected_pedido_snapshot(summary_rows, selected_id_pedido)

    st.markdown("---")
    filter_options = _build_filter_options(summary_rows)
    _render_edit_section(pedido_full, filter_options)

    st.markdown("---")
    _render_cierre_action(pedido_full)

    st.markdown("---")
    filters = _render_filters(filter_options, summary_df)

    st.markdown("---")
    _render_detail(filters)

    st.markdown("---")
    _render_chart(filters)


if __name__ == "__main__":
    main()
