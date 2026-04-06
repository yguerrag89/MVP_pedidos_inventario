from __future__ import annotations

"""
app/components/filtros.py

Helpers reutilizables para construir filtros visuales del MVP.

Objetivo
--------
Centralizar los controles de filtros de fechas y catálogos para que las páginas
mantengan una UX consistente con las reglas del proyecto:
- usar selector calendario para fechas
- tratar fecha_produccion como filtro principal cuando exista
- permitir filtros secundarios por fecha_captura y fecha_logistica
- devolver filtros limpios en formato compatible con servicios/repositorios

Notas para desarrolladores
--------------------------
- Los filtros de fecha deben poder quedar realmente vacíos. En el MVP no se
  debe forzar un rango solo porque existan fechas en los datos.
- La salida del componente usa llaves canónicas (porcentaje_min/
  porcentaje_max). Se conserva compatibilidad de lectura con llaves antiguas
  solo a nivel de session_state.
- Cuando la UI usa multiselect, el contrato devuelto conserva listas para que
  servicio y repositorio puedan decidir si construyen un IN (...).
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
import streamlit as st


# ============================================================================
# Normalización y utilidades básicas
# ============================================================================

def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text



def unique_sorted_text(values: Iterable[Any]) -> list[str]:
    data = {normalize_text(v) for v in values}
    data.discard("")
    return sorted(data)



def parse_date_like(value: Any) -> date | None:
    if value in (None, "", pd.NaT):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(ts):
        return None
    if isinstance(ts, pd.Timestamp):
        return ts.date()
    return None



def date_to_iso(value: date | None) -> str | None:
    return value.isoformat() if isinstance(value, date) else None



def build_date_bounds(values: Iterable[Any]) -> tuple[date | None, date | None]:
    parsed = [parse_date_like(v) for v in values]
    parsed = [v for v in parsed if v is not None]
    if not parsed:
        return None, None
    return min(parsed), max(parsed)



def clamp_date(default_value: date | None, minimum: date | None, maximum: date | None) -> date | None:
    if default_value is None:
        return None
    if minimum and default_value < minimum:
        return minimum
    if maximum and default_value > maximum:
        return maximum
    return default_value



def _read_state_with_aliases(*keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in st.session_state:
            return st.session_state.get(key)
    return default



def _clean_multiselect_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [normalize_text(v) for v in value if normalize_text(v)]
    text = normalize_text(value)
    return [text] if text else []


# ============================================================================
# Catálogo de filtros
# ============================================================================

@dataclass(slots=True)
class FilterCatalog:
    id_pedido: list[str]
    cliente: list[str]
    estatus_venta: list[str]
    producto: list[str]

    def as_dict(self) -> dict[str, list[str]]:
        return {
            "id_pedido": self.id_pedido,
            "cliente": self.cliente,
            "estatus_venta": self.estatus_venta,
            "producto": self.producto,
        }



def build_filter_catalog(
    summary_rows: Sequence[Mapping[str, Any]] | None = None,
    service_options: Mapping[str, Sequence[Any]] | None = None,
) -> FilterCatalog:
    summary_rows = summary_rows or []
    service_options = service_options or {}

    id_pedido = unique_sorted_text(
        [row.get("id_pedido") for row in summary_rows]
        + list(service_options.get("id_pedido", []))
        + list(service_options.get("ids_pedido", []))
    )
    cliente = unique_sorted_text(
        [row.get("cliente") for row in summary_rows]
        + list(service_options.get("clientes", []))
        + list(service_options.get("cliente", []))
    )
    estatus_venta = unique_sorted_text(
        [row.get("estatus_venta") for row in summary_rows]
        + list(service_options.get("estatus_venta", []))
    )
    producto = unique_sorted_text(
        list(service_options.get("productos", []))
        + list(service_options.get("producto", []))
    )

    return FilterCatalog(
        id_pedido=id_pedido,
        cliente=cliente,
        estatus_venta=estatus_venta,
        producto=producto,
    )


# ============================================================================
# Render reutilizable de fechas
# ============================================================================

def render_date_range_filter(
    *,
    label: str,
    key_prefix: str,
    values: Iterable[Any],
    help_text: str | None = None,
    column_layout: Sequence[Any] = (1, 1),
) -> tuple[date | None, date | None]:
    """
    Renderiza un rango de fechas opcional con date_input.

    Devuelve (desde, hasta) como objetos date o None.
    Si no hay datos válidos, no renderiza inputs y devuelve (None, None).
    El usuario puede dejar el filtro desactivado, en cuyo caso devuelve
    (None, None) aunque existan fechas disponibles.
    """
    min_date, max_date = build_date_bounds(values)
    if min_date is None or max_date is None:
        st.caption(f"{label}: sin fechas disponibles para filtrar.")
        return None, None

    use_key = f"{key_prefix}_enabled"
    from_key = f"{key_prefix}_desde"
    to_key = f"{key_prefix}_hasta"

    use_filter = st.checkbox(
        f"Usar filtro de {label}",
        value=bool(st.session_state.get(use_key, False)),
        key=use_key,
        help=help_text,
    )
    if not use_filter:
        return None, None

    current_from = clamp_date(st.session_state.get(from_key), min_date, max_date) or min_date
    current_to = clamp_date(st.session_state.get(to_key), min_date, max_date) or max_date
    if current_to < current_from:
        current_to = current_from

    col_from, col_to = st.columns(column_layout)
    with col_from:
        from_value = st.date_input(
            f"{label} desde",
            value=current_from,
            min_value=min_date,
            max_value=max_date,
            key=from_key,
            format="YYYY-MM-DD",
            help=help_text,
        )
    with col_to:
        to_value = st.date_input(
            f"{label} hasta",
            value=current_to,
            min_value=min_date,
            max_value=max_date,
            key=to_key,
            format="YYYY-MM-DD",
        )

    if to_value < from_value:
        to_value = from_value
        st.session_state[to_key] = to_value

    return from_value, to_value


# ============================================================================
# Render reutilizable de filtros operativos
# ============================================================================

def render_operational_filters(
    *,
    summary_df: pd.DataFrame,
    catalog: Mapping[str, Sequence[str]] | FilterCatalog,
    key_prefix: str,
    include_producto: bool = True,
    show_caption: bool = True,
) -> dict[str, Any]:
    """
    Renderiza un bloque estándar de filtros operativos para páginas de pedidos.

    Reglas UX reflejadas:
    - fecha_produccion es el filtro principal
    - fecha_captura y fecha_logistica se tratan como secundarios
    - el rango porcentual trabaja siempre de 0 a 100
    - los filtros de fecha pueden quedar vacíos
    """
    if isinstance(catalog, FilterCatalog):
        catalog = catalog.as_dict()

    if show_caption:
        st.caption(
            "El filtro principal es Fecha producción. Fecha captura y Fecha logística se mantienen como filtros secundarios. Todos los filtros son opcionales."
        )

    with st.container(border=True):
        row1 = st.columns(4 if include_producto else 3)
        selected_id = row1[0].multiselect(
            "ID_pedido",
            options=list(catalog.get("id_pedido", [])),
            default=_clean_multiselect_value(st.session_state.get(f"{key_prefix}_id_pedido", [])),
            key=f"{key_prefix}_id_pedido",
        )
        selected_cliente = row1[1].multiselect(
            "Cliente",
            options=list(catalog.get("cliente", [])),
            default=_clean_multiselect_value(st.session_state.get(f"{key_prefix}_cliente", [])),
            key=f"{key_prefix}_cliente",
        )
        selected_estatus = row1[2].multiselect(
            "Estatus venta",
            options=list(catalog.get("estatus_venta", [])),
            default=_clean_multiselect_value(st.session_state.get(f"{key_prefix}_estatus_venta", [])),
            key=f"{key_prefix}_estatus_venta",
        )
        selected_producto: list[str] = []
        if include_producto:
            selected_producto = row1[3].multiselect(
                "Producto",
                options=list(catalog.get("producto", [])),
                default=_clean_multiselect_value(st.session_state.get(f"{key_prefix}_producto", [])),
                key=f"{key_prefix}_producto",
            )

        row2 = st.columns(2)
        pct_min_default = float(
            _read_state_with_aliases(
                f"{key_prefix}_porcentaje_min",
                f"{key_prefix}_pct_min",
                default=0.0,
            )
        )
        pct_max_default = float(
            _read_state_with_aliases(
                f"{key_prefix}_porcentaje_max",
                f"{key_prefix}_pct_max",
                default=100.0,
            )
        )
        pct_min = row2[0].number_input(
            "% mínimo",
            min_value=0.0,
            max_value=100.0,
            value=pct_min_default,
            step=1.0,
            key=f"{key_prefix}_porcentaje_min",
        )
        pct_max = row2[1].number_input(
            "% máximo",
            min_value=0.0,
            max_value=100.0,
            value=pct_max_default,
            step=1.0,
            key=f"{key_prefix}_porcentaje_max",
        )
        if pct_max < pct_min:
            pct_max = pct_min
            st.session_state[f"{key_prefix}_porcentaje_max"] = pct_max

        fecha_prod_desde, fecha_prod_hasta = render_date_range_filter(
            label="Fecha producción",
            key_prefix=f"{key_prefix}_fecha_produccion",
            values=summary_df.get("Fecha producción", pd.Series(dtype="object")),
            help_text="Filtro operativo principal.",
        )
        fecha_cap_desde, fecha_cap_hasta = render_date_range_filter(
            label="Fecha captura",
            key_prefix=f"{key_prefix}_fecha_captura",
            values=summary_df.get("Fecha captura", pd.Series(dtype="object")),
            help_text="Filtro secundario.",
        )
        fecha_log_desde, fecha_log_hasta = render_date_range_filter(
            label="Fecha logística",
            key_prefix=f"{key_prefix}_fecha_logistica",
            values=summary_df.get("Fecha logística", pd.Series(dtype="object")),
            help_text="Filtro secundario.",
        )

    filters: dict[str, Any] = {}
    if selected_id:
        filters["id_pedido"] = list(selected_id)
    if selected_cliente:
        filters["cliente"] = list(selected_cliente)
    if selected_estatus:
        filters["estatus_venta"] = list(selected_estatus)
    if include_producto and selected_producto:
        filters["producto"] = list(selected_producto)

    filters["porcentaje_min"] = float(pct_min)
    filters["porcentaje_max"] = float(pct_max)

    if fecha_prod_desde:
        filters["fecha_produccion_desde"] = date_to_iso(fecha_prod_desde)
    if fecha_prod_hasta:
        filters["fecha_produccion_hasta"] = date_to_iso(fecha_prod_hasta)
    if fecha_cap_desde:
        filters["fecha_captura_desde"] = date_to_iso(fecha_cap_desde)
    if fecha_cap_hasta:
        filters["fecha_captura_hasta"] = date_to_iso(fecha_cap_hasta)
    if fecha_log_desde:
        filters["fecha_logistica_desde"] = date_to_iso(fecha_log_desde)
    if fecha_log_hasta:
        filters["fecha_logistica_hasta"] = date_to_iso(fecha_log_hasta)

    return filters


# ============================================================================
# Session state
# ============================================================================

def clear_filter_state(key_prefix: str) -> None:
    suffixes = [
        "id_pedido",
        "cliente",
        "estatus_venta",
        "producto",
        "porcentaje_min",
        "porcentaje_max",
        # Compatibilidad temporal con llaves antiguas de porcentaje.
        "pct_min",
        "pct_max",
        # Fechas opcionales del componente reutilizable.
        "fecha_produccion_enabled",
        "fecha_produccion_desde",
        "fecha_produccion_hasta",
        "fecha_captura_enabled",
        "fecha_captura_desde",
        "fecha_captura_hasta",
        "fecha_logistica_enabled",
        "fecha_logistica_desde",
        "fecha_logistica_hasta",
        # Compatibilidad con variantes previas de la UI.
        "use_fecha_produccion",
        "use_fecha_captura",
        "use_fecha_logistica",
    ]
    for suffix in suffixes:
        st.session_state.pop(f"{key_prefix}_{suffix}", None)


__all__ = [
    "FilterCatalog",
    "build_date_bounds",
    "build_filter_catalog",
    "clear_filter_state",
    "date_to_iso",
    "normalize_text",
    "parse_date_like",
    "render_date_range_filter",
    "render_operational_filters",
    "unique_sorted_text",
]
