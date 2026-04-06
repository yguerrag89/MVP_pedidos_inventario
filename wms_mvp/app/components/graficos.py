from __future__ import annotations

"""
app/components/graficos.py

Componentes reutilizables para gráficos del MVP.

Objetivo
--------
Centralizar la construcción y render del gráfico de porcentaje de asignación
para que las páginas operativas mantengan una salida visual consistente con las
reglas del proyecto:
- gráfico de columnas
- eje Y fijo de 0 a 100
- eje X con "ID_pedido + cliente"
- orden inicial de menor a mayor porcentaje de asignación
- legibilidad razonable cuando hay muchos pedidos

El módulo es tolerante con diferentes formas de entrada porque el dataset puede
venir desde servicios o desde páginas que ya construyeron DataFrames.
"""

from typing import Any, Iterable, Mapping, Sequence

import altair as alt
import pandas as pd
import streamlit as st


# ============================================================================
# Utilidades base
# ============================================================================

def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text



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



def _clamp_percent(value: Any) -> float:
    pct = _safe_float(value, 0.0)
    if pct < 0:
        return 0.0
    if pct > 100:
        return 100.0
    return round(pct, 2)


# ============================================================================
# Construcción del dataset del gráfico
# ============================================================================

def build_pedidos_chart_df(rows: Sequence[Mapping[str, Any]] | pd.DataFrame | None) -> pd.DataFrame:
    """
    Construye un DataFrame estándar para el gráfico de % de asignación.

    Columnas devueltas:
    - Etiqueta
    - ID_pedido
    - Cliente
    - % Asignación
    """
    empty = pd.DataFrame(columns=["Etiqueta", "ID_pedido", "Cliente", "% Asignación"])
    if rows is None:
        return empty

    if isinstance(rows, pd.DataFrame):
        source_df = rows.copy()
        if source_df.empty:
            return empty
        normalized_rows = source_df.to_dict(orient="records")
    else:
        normalized_rows = list(rows)
        if not normalized_rows:
            return empty

    data: list[dict[str, Any]] = []
    seen: set[str] = set()

    for row in normalized_rows:
        id_pedido = _safe_text(
            row.get("id_pedido")
            or row.get("ID_pedido")
        )
        if not id_pedido or id_pedido in seen:
            continue
        seen.add(id_pedido)

        cliente = _safe_text(
            row.get("cliente")
            or row.get("Cliente")
        )
        porcentaje = row.get("porcentaje_asignacion")
        if porcentaje is None:
            porcentaje = row.get("% Asignación")
        if porcentaje is None:
            porcentaje = row.get("%")

        data.append(
            {
                "Etiqueta": f"{id_pedido} | {cliente}" if cliente else id_pedido,
                "ID_pedido": id_pedido,
                "Cliente": cliente,
                "% Asignación": _clamp_percent(porcentaje),
            }
        )

    if not data:
        return empty

    chart_df = pd.DataFrame(data)
    return chart_df.sort_values(
        by=["% Asignación", "ID_pedido", "Cliente"],
        ascending=[True, True, True],
    ).reset_index(drop=True)


# ============================================================================
# Render del gráfico
# ============================================================================

def render_pedidos_asignacion_chart(
    rows: Sequence[Mapping[str, Any]] | pd.DataFrame | None = None,
    *,
    chart_df: pd.DataFrame | None = None,
    title: str = "% de asignación de pedidos filtrados",
    empty_message: str = "No hay datos para construir el gráfico con los filtros actuales.",
    show_data_table: bool = False,
    min_width: int = 700,
    bar_width_hint: int = 60,
    height: int = 380,
) -> pd.DataFrame:
    """
    Renderiza el gráfico estándar de pedidos activos.

    Puede recibir:
    - rows: lista de diccionarios o DataFrame con datos crudos
    - chart_df: DataFrame ya normalizado

    Devuelve el DataFrame finalmente usado para el gráfico.
    """
    if chart_df is None:
        chart_df = build_pedidos_chart_df(rows)
    else:
        chart_df = build_pedidos_chart_df(chart_df)

    if chart_df.empty:
        st.info(empty_message)
        return chart_df

    chart_width = max(min_width, len(chart_df) * bar_width_hint)

    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X(
                "Etiqueta:N",
                sort=chart_df["Etiqueta"].tolist(),
                title="Pedido | Cliente",
                axis=alt.Axis(labelAngle=-35, labelLimit=240),
            ),
            y=alt.Y(
                "% Asignación:Q",
                scale=alt.Scale(domain=[0, 100]),
                title="% de asignación",
            ),
            tooltip=[
                alt.Tooltip("ID_pedido:N", title="ID_pedido"),
                alt.Tooltip("Cliente:N", title="Cliente"),
                alt.Tooltip("% Asignación:Q", title="% asignación", format=".2f"),
            ],
        )
        .properties(
            title=title,
            width=chart_width,
            height=height,
        )
    )

    # use_container_width=False mantiene el ancho calculado cuando hay muchos pedidos.
    st.altair_chart(chart, use_container_width=False)

    if show_data_table:
        st.dataframe(chart_df, use_container_width=True, hide_index=True)

    return chart_df


# ============================================================================
# Helpers de resumen rápido
# ============================================================================

def get_chart_kpis(chart_df: pd.DataFrame | None) -> dict[str, Any]:
    """KPIs simples útiles para páginas o encabezados."""
    if chart_df is None or chart_df.empty:
        return {
            "pedidos": 0,
            "porcentaje_promedio": 0.0,
            "porcentaje_minimo": 0.0,
            "porcentaje_maximo": 0.0,
        }

    pct = chart_df["% Asignación"].astype(float)
    return {
        "pedidos": int(len(chart_df)),
        "porcentaje_promedio": round(float(pct.mean()), 2),
        "porcentaje_minimo": round(float(pct.min()), 2),
        "porcentaje_maximo": round(float(pct.max()), 2),
    }


__all__ = [
    "build_pedidos_chart_df",
    "render_pedidos_asignacion_chart",
    "get_chart_kpis",
]
