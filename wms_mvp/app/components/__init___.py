"""Componentes reutilizables de la UI del MVP.

Este módulo centraliza las exportaciones públicas de los componentes
reutilizables para evitar imports dispersos y hacer más estable la
integración entre páginas.
"""

from .filtros import (
    build_date_bounds,
    build_filter_catalog,
    clear_filter_state,
    date_to_iso,
    parse_date_like,
    render_date_range_filter,
    render_operational_filters,
)
from .forms import (
    build_venta_directa_dataframe,
    render_cierre_pedido_form,
    render_pedido_header_edit_form,
    render_staging_datetime_editor,
    render_venta_directa_form,
)
from .graficos import (
    build_pedidos_chart_df,
    get_chart_kpis,
    render_pedidos_asignacion_chart,
)
from .tablas import (
    build_demanda_sku_df,
    build_detail_activos_df,
    build_detail_enviados_df,
    build_summary_activos_df,
    build_summary_enviados_df,
    render_demanda_sku_table,
    render_detail_table,
    render_enviados_detail_table,
    render_enviados_summary_table,
    render_readonly_table,
    render_summary_table,
)

__all__ = [
    # filtros
    "build_date_bounds",
    "build_filter_catalog",
    "clear_filter_state",
    "date_to_iso",
    "parse_date_like",
    "render_date_range_filter",
    "render_operational_filters",
    # forms
    "build_venta_directa_dataframe",
    "render_cierre_pedido_form",
    "render_pedido_header_edit_form",
    "render_staging_datetime_editor",
    "render_venta_directa_form",
    # graficos
    "build_pedidos_chart_df",
    "get_chart_kpis",
    "render_pedidos_asignacion_chart",
    # tablas
    "build_demanda_sku_df",
    "build_detail_activos_df",
    "build_detail_enviados_df",
    "build_summary_activos_df",
    "build_summary_enviados_df",
    "render_demanda_sku_table",
    "render_detail_table",
    "render_enviados_detail_table",
    "render_enviados_summary_table",
    "render_readonly_table",
    "render_summary_table",
]
