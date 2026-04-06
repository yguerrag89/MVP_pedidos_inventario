from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
import streamlit as st


# -----------------------------------------------------------------------------
# Column orders aligned to MVP operational rules
# -----------------------------------------------------------------------------
SUMMARY_ACTIVOS_ORDER: list[str] = [
    "ID_pedido",
    "Cliente",
    "%",
    "Fecha captura",
    "Fecha producción",
    "Fecha logística",
    "Estatus venta",
]

DETAIL_ACTIVOS_ORDER: list[str] = [
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
    "Línea",
]

SUMMARY_ENVIADOS_ORDER: list[str] = [
    "ID_pedido",
    "Cliente",
    "Fecha captura",
    "Fecha producción",
    "Fecha logística",
    "Estatus venta",
    "% al cierre",
    "Fecha de envío",
    "Tipo de cierre",
]

DETAIL_ENVIADOS_ORDER: list[str] = [
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
    "Línea",
]

DEMANDA_SKU_ORDER: list[str] = [
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
    "Pedido sugerido",
]


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------
def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().replace(",", "")
    if not text:
        return default

    try:
        return float(text)
    except (TypeError, ValueError):
        return default


def _to_display_date(value: Any) -> str:
    text = _safe_text(value)
    if not text:
        return ""

    # Keeps ISO date as-is and trims datetime to date when appropriate.
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10]
    return text


def _yes_no(value: Any) -> str:
    if isinstance(value, bool):
        return "Sí" if value else "No"
    text = _safe_text(value).lower()
    if text in {"1", "true", "si", "sí", "y", "yes"}:
        return "Sí"
    if text in {"0", "false", "no", "n"}:
        return "No"
    return _safe_text(value)


def _ensure_dataframe(data: pd.DataFrame | Sequence[Mapping[str, Any]] | None) -> pd.DataFrame:
    if data is None:
        return pd.DataFrame()
    if isinstance(data, pd.DataFrame):
        return data.copy()
    return pd.DataFrame(list(data))


def reorder_columns(df: pd.DataFrame, preferred_order: Sequence[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=list(preferred_order))

    existing_preferred = [col for col in preferred_order if col in df.columns]
    remaining = [col for col in df.columns if col not in existing_preferred]
    return df.loc[:, existing_preferred + remaining]


# -----------------------------------------------------------------------------
# Builders for page dataframes
# -----------------------------------------------------------------------------
def build_summary_activos_df(rows: Sequence[Mapping[str, Any]] | pd.DataFrame | None) -> pd.DataFrame:
    df = _ensure_dataframe(rows)
    if df.empty:
        return pd.DataFrame(columns=SUMMARY_ACTIVOS_ORDER)

    out = pd.DataFrame(
        {
            "ID_pedido": df.get("id_pedido", "").map(_safe_text),
            "Cliente": df.get("cliente", "").map(_safe_text),
            "%": df.get("porcentaje_asignacion", 0).map(lambda v: round(_safe_float(v), 2)),
            "Fecha captura": df.get("fecha_captura", "").map(_to_display_date),
            "Fecha producción": df.get("fecha_produccion", "").map(_to_display_date),
            "Fecha logística": df.get("fecha_logistica", "").map(_to_display_date),
            "Estatus venta": df.get("estatus_venta", "").map(_safe_text),
        }
    )
    return reorder_columns(out, SUMMARY_ACTIVOS_ORDER)


def build_detail_activos_df(rows: Sequence[Mapping[str, Any]] | pd.DataFrame | None) -> pd.DataFrame:
    df = _ensure_dataframe(rows)
    if df.empty:
        return pd.DataFrame(columns=DETAIL_ACTIVOS_ORDER)

    out = pd.DataFrame(
        {
            "ID_pedido": df.get("id_pedido", "").map(_safe_text),
            "Cliente": df.get("cliente", "").map(_safe_text),
            "Producto": df.get("producto", "").map(_safe_text),
            "Color": df.get("color", "").map(_safe_text),
            "Cantidad pedida": df.get("cantidad_pedida", 0).map(_safe_float),
            "Cantidad asignada": df.get("cantidad_asignada", 0).map(_safe_float),
            "Cantidad faltante": df.get("cantidad_faltante", 0).map(_safe_float),
            "%": df.get("porcentaje_asignacion", 0).map(lambda v: round(_safe_float(v), 2)),
            "Fecha captura": df.get("pedido_fecha_captura", df.get("fecha_captura", "")).map(_to_display_date),
            "Fecha producción": df.get("fecha_produccion", "").map(_to_display_date),
            "Fecha logística": df.get("fecha_logistica", "").map(_to_display_date),
            "Estatus venta": df.get("estatus_venta", "").map(_safe_text),
            "SKU": df.get("sku", "").map(_safe_text),
            "Descripción": df.get("descripcion", "").map(_safe_text),
            "Línea": df.get("line_no", 0).map(lambda v: int(_safe_float(v, 0))),
        }
    )
    return reorder_columns(out, DETAIL_ACTIVOS_ORDER)


def build_summary_enviados_df(rows: Sequence[Mapping[str, Any]] | pd.DataFrame | None) -> pd.DataFrame:
    df = _ensure_dataframe(rows)
    if df.empty:
        return pd.DataFrame(columns=SUMMARY_ENVIADOS_ORDER)

    out = pd.DataFrame(
        {
            "ID_pedido": df.get("id_pedido", "").map(_safe_text),
            "Cliente": df.get("cliente", "").map(_safe_text),
            "Fecha captura": df.get("fecha_captura", "").map(_to_display_date),
            "Fecha producción": df.get("fecha_produccion", "").map(_to_display_date),
            "Fecha logística": df.get("fecha_logistica", "").map(_to_display_date),
            "Estatus venta": df.get("estatus_venta", "").map(_safe_text),
            "% al cierre": df.get("porcentaje_asignacion_cierre", df.get("porcentaje_asignacion", 0)).map(
                lambda v: round(_safe_float(v), 2)
            ),
            "Fecha de envío": df.get("fecha_envio", df.get("fecha_cierre", "")).map(_to_display_date),
            "Tipo de cierre": df.get("tipo_cierre", "").map(_safe_text),
        }
    )
    return reorder_columns(out, SUMMARY_ENVIADOS_ORDER)


def build_detail_enviados_df(rows: Sequence[Mapping[str, Any]] | pd.DataFrame | None) -> pd.DataFrame:
    df = _ensure_dataframe(rows)
    if df.empty:
        return pd.DataFrame(columns=DETAIL_ENVIADOS_ORDER)

    out = pd.DataFrame(
        {
            "ID_pedido": df.get("id_pedido", "").map(_safe_text),
            "Cliente": df.get("cliente", "").map(_safe_text),
            "Producto": df.get("producto", "").map(_safe_text),
            "Color": df.get("color", "").map(_safe_text),
            "Cantidad pedida original": df.get("cantidad_pedida_original", df.get("cantidad_pedida", 0)).map(_safe_float),
            "Cantidad enviada": df.get("cantidad_enviada", 0).map(_safe_float),
            "Cantidad no enviada": df.get("cantidad_no_enviada", 0).map(_safe_float),
            "Fecha producción": df.get("fecha_produccion", "").map(_to_display_date),
            "SKU": df.get("sku", "").map(_safe_text),
            "Descripción": df.get("descripcion", "").map(_safe_text),
            "Línea": df.get("line_no", 0).map(lambda v: int(_safe_float(v, 0))),
        }
    )
    return reorder_columns(out, DETAIL_ENVIADOS_ORDER)


def build_demanda_sku_df(rows: Sequence[Mapping[str, Any]] | pd.DataFrame | None) -> pd.DataFrame:
    df = _ensure_dataframe(rows)
    if df.empty:
        return pd.DataFrame(columns=DEMANDA_SKU_ORDER)

    out = pd.DataFrame(
        {
            "ID_pedido": df.get("id_pedido", "").map(_safe_text),
            "Cliente": df.get("cliente", "").map(_safe_text),
            "Producto": df.get("producto", "").map(_safe_text),
            "Color": df.get("color", "").map(_safe_text),
            "Cantidad pedida": df.get("cantidad_pedida", 0).map(_safe_float),
            "Cantidad asignada": df.get("cantidad_asignada", 0).map(_safe_float),
            "Cantidad faltante": df.get("cantidad_faltante", 0).map(_safe_float),
            "%": df.get("porcentaje_asignacion", 0).map(lambda v: round(_safe_float(v), 2)),
            "Fecha captura": df.get("fecha_captura", "").map(_to_display_date),
            "Fecha producción": df.get("fecha_produccion", "").map(_to_display_date),
            "Fecha logística": df.get("fecha_logistica", "").map(_to_display_date),
            "Estatus venta": df.get("estatus_venta", "").map(_safe_text),
            "SKU": df.get("sku", "").map(_safe_text),
            "Descripción": df.get("descripcion", "").map(_safe_text),
            "Pedido sugerido": df.get("pedido_sugerido", df.get("is_suggested", False)).map(_yes_no),
        }
    )
    return reorder_columns(out, DEMANDA_SKU_ORDER)


# -----------------------------------------------------------------------------
# Render helpers for Streamlit pages
# -----------------------------------------------------------------------------
def render_readonly_table(
    df: pd.DataFrame,
    *,
    key: str | None = None,
    height: int = 360,
    use_container_width: bool = True,
    hide_index: bool = True,
    help_text: str | None = None,
) -> None:
    if help_text:
        st.caption(help_text)

    st.dataframe(
        df,
        use_container_width=use_container_width,
        height=height,
        hide_index=hide_index,
        key=key,
    )


def render_summary_table(df: pd.DataFrame, *, key: str = "tabla_resumen_activos") -> None:
    render_readonly_table(
        reorder_columns(df, SUMMARY_ACTIVOS_ORDER),
        key=key,
        height=320,
        help_text=(
            "Tabla resumen de solo lectura. Para modificar información utiliza el bloque de edición controlada "
            "del pedido seleccionado."
        ),
    )


def render_detail_table(df: pd.DataFrame, *, key: str = "tabla_detalle_activos") -> None:
    render_readonly_table(reorder_columns(df, DETAIL_ACTIVOS_ORDER), key=key, height=420)


def render_enviados_summary_table(df: pd.DataFrame, *, key: str = "tabla_resumen_enviados") -> None:
    render_readonly_table(reorder_columns(df, SUMMARY_ENVIADOS_ORDER), key=key, height=320)


def render_enviados_detail_table(df: pd.DataFrame, *, key: str = "tabla_detalle_enviados") -> None:
    render_readonly_table(reorder_columns(df, DETAIL_ENVIADOS_ORDER), key=key, height=420)


def render_demanda_sku_table(df: pd.DataFrame, *, key: str = "tabla_demanda_sku") -> None:
    render_readonly_table(reorder_columns(df, DEMANDA_SKU_ORDER), key=key, height=420)


__all__ = [
    "SUMMARY_ACTIVOS_ORDER",
    "DETAIL_ACTIVOS_ORDER",
    "SUMMARY_ENVIADOS_ORDER",
    "DETAIL_ENVIADOS_ORDER",
    "DEMANDA_SKU_ORDER",
    "reorder_columns",
    "build_summary_activos_df",
    "build_detail_activos_df",
    "build_summary_enviados_df",
    "build_detail_enviados_df",
    "build_demanda_sku_df",
    "render_readonly_table",
    "render_summary_table",
    "render_detail_table",
    "render_enviados_summary_table",
    "render_enviados_detail_table",
    "render_demanda_sku_table",
]
