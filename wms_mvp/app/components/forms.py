from __future__ import annotations

from datetime import date, datetime, time
from typing import Any

import pandas as pd
import streamlit as st

from wms_mvp.db.connection import column_exists
from wms_mvp.core.rules.pedidos_rules import (
    VALID_ESTATUS_VENTA,
    VALID_MOTIVOS_PAUSA,
    normalize_estatus_venta,
    normalize_motivo_pausa,
    normalize_pause_fields,
)


_FRAGMENT_DECORATOR = getattr(st, "fragment", None) or getattr(st, "experimental_fragment", None)
if _FRAGMENT_DECORATOR is None:
    def _fragment_decorator(func):
        return func
else:
    _fragment_decorator = _FRAGMENT_DECORATOR

DEFAULT_ESTATUS_VENTA = ["", *sorted(VALID_ESTATUS_VENTA)]
DEFAULT_MOTIVOS_PAUSA = ["", *sorted(VALID_MOTIVOS_PAUSA)]
DEFAULT_TIPOS_CIERRE = ["envio total", "envio parcial"]


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


def _parse_iso_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
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


def _build_estatus_options(extra_options: list[str] | None = None) -> list[str]:
    options = list(DEFAULT_ESTATUS_VENTA)
    for item in extra_options or []:
        text = _safe_text(item)
        if not text:
            continue
        canonical = normalize_estatus_venta(text) or text
        if canonical not in options:
            options.append(canonical)
    return options


def _build_motivo_pausa_options(extra_options: list[str] | None = None) -> list[str]:
    options = list(DEFAULT_MOTIVOS_PAUSA)
    for item in extra_options or []:
        text = _safe_text(item)
        if not text:
            continue
        canonical = normalize_motivo_pausa(text) or text
        if canonical not in options:
            options.append(canonical)
    return options


@_fragment_decorator
def render_pedido_header_edit_form(
    pedido: dict[str, Any],
    *,
    is_admin: bool,
    key_prefix: str = "pedido_edit",
    extra_estatus_options: list[str] | None = None,
    extra_motivo_pausa_options: list[str] | None = None,
) -> dict[str, Any] | None:
    """
    Formulario controlado para la edición del encabezado del pedido en el MVP.

    Alcance permitido en esta etapa:
        - estatus_venta
        - motivo_pausa (solo si el estatus es Pausa)
        - fecha_produccion
        - fecha_logistica

    Este formulario no debe reabrir edición libre de fecha_captura, observaciones,
    ni otros campos estructurales del pedido. La confirmación final del cambio
    debe realizarse en la página o flujo que invoque este componente.

    Devuelve un payload listo para el servicio cuando el usuario envía el formulario:
        {
            "estatus_venta": str | None,
            "motivo_pausa": str | None,
            "fecha_produccion": str | None,
            "fecha_logistica": str | None,
        }

    Si no se envía o no hay permisos, devuelve None.
    """
    if not is_admin:
        st.info("Solo el modo Administrador puede editar los datos del pedido.")
        return None

    estatus_actual = normalize_estatus_venta(pedido.get("estatus_venta")) or _safe_text(pedido.get("estatus_venta"))
    motivo_actual = normalize_motivo_pausa(pedido.get("motivo_pausa")) or _safe_text(pedido.get("motivo_pausa"))
    estatus_options = _build_estatus_options((extra_estatus_options or []) + [estatus_actual])
    motivo_options = _build_motivo_pausa_options((extra_motivo_pausa_options or []) + [motivo_actual])

    fecha_produccion = _parse_iso_date(pedido.get("fecha_produccion"))
    fecha_logistica = _parse_iso_date(pedido.get("fecha_logistica"))

    with st.form(f"{key_prefix}_form", clear_on_submit=False):
        st.caption(
            "Edición controlada del MVP. Este formulario solo prepara cambios en estatus venta, motivo de pausa, "
            "fecha producción y fecha logística. La confirmación final debe mostrarse antes de guardar."
        )

        supports_prioridad_manual = column_exists("pedidos", "prioridad_manual")
        col1, col2 = st.columns(2)
        with col1:
            estatus_venta = st.selectbox(
                "Estatus venta",
                options=estatus_options,
                index=estatus_options.index(estatus_actual) if estatus_actual in estatus_options else 0,
                key=f"{key_prefix}_estatus_venta",
                help="Catálogo controlado del MVP: Entregar, En cobro o Pausa.",
            )
            if estatus_venta == "Pausa":
                motivo_pausa = st.selectbox(
                    "Motivo de pausa",
                    options=motivo_options,
                    index=motivo_options.index(motivo_actual) if motivo_actual in motivo_options else 0,
                    key=f"{key_prefix}_motivo_pausa",
                    help="Selecciona el motivo operativo/comercial de la pausa.",
                )
            else:
                motivo_pausa = None
                st.caption("Motivo de pausa: solo aplica cuando el estatus es Pausa.")
        with col2:
            prioridad_manual_actual = pedido.get("prioridad_manual")
            if supports_prioridad_manual:
                prioridad_manual = st.number_input(
                    "Prioridad manual",
                    min_value=1,
                    max_value=999,
                    value=int(prioridad_manual_actual or 3),
                    step=1,
                    key=f"{key_prefix}_prioridad_manual",
                    help="1 = más alta prioridad operativa. 3 = normal. 5 = baja."
                )
            else:
                prioridad_manual = None
                st.caption("Prioridad manual: el esquema actual todavía no soporta este campo.")
            nueva_fecha_produccion = st.date_input(
                "Fecha producción",
                value=fecha_produccion,
                format="YYYY-MM-DD",
                key=f"{key_prefix}_fecha_produccion",
            )
            nueva_fecha_logistica = st.date_input(
                "Fecha logística",
                value=fecha_logistica,
                format="YYYY-MM-DD",
                key=f"{key_prefix}_fecha_logistica",
            )

        submitted = st.form_submit_button("Preparar cambios")
        if not submitted:
            return None

    try:
        canonical_estatus, canonical_motivo = normalize_pause_fields(estatus_venta, motivo_pausa)
    except ValueError as exc:
        st.error(str(exc))
        return None

    return {
        "estatus_venta": canonical_estatus,
        "motivo_pausa": canonical_motivo,
        "fecha_produccion": _date_to_iso(nueva_fecha_produccion),
        "fecha_logistica": _date_to_iso(nueva_fecha_logistica),
        "prioridad_manual": int(prioridad_manual) if prioridad_manual is not None else None,
    }


@_fragment_decorator
def render_cierre_pedido_form(
    preview: dict[str, Any] | None,
    *,
    is_admin: bool,
    key_prefix: str = "cierre_pedido",
    tipos_cierre: list[str] | None = None,
) -> dict[str, Any] | None:
    """
    Formulario controlado para confirmar cierre de pedido.

    Devuelve un payload listo para el servicio:
        {
            "tipo_cierre": "envio total" | "envio parcial",
            "fecha_envio": "YYYY-MM-DD",
            "observacion": str,
            "confirmado": bool,
        }
    """
    if not preview:
        st.info("Primero genera una vista previa del cierre para confirmar cantidades y faltantes.")
        return None

    if not is_admin:
        st.info("Solo el modo Administrador puede cerrar y mover pedidos a enviados.")
        return None

    tipos = tipos_cierre or list(DEFAULT_TIPOS_CIERRE)
    fecha_sugerida = _parse_iso_date(preview.get("fecha_envio_sugerida")) or date.today()

    with st.form(f"{key_prefix}_form", clear_on_submit=False):
        st.caption("El cierre es una acción operativa auditada. El pedido dejará de aparecer en Activos.")

        c1, c2 = st.columns(2)
        with c1:
            tipo_cierre = st.selectbox(
                "Tipo de cierre",
                options=tipos,
                key=f"{key_prefix}_tipo_cierre",
            )
        with c2:
            fecha_envio = st.date_input(
                "Fecha de envío",
                value=fecha_sugerida,
                format="YYYY-MM-DD",
                key=f"{key_prefix}_fecha_envio",
            )

        observacion = st.text_area(
            "Observación opcional",
            value="",
            height=80,
            key=f"{key_prefix}_observacion",
        )

        st.markdown("**Resumen de la vista previa**")
        left, right, third = st.columns(3)
        left.metric("Pedido", _safe_text(preview.get("id_pedido")) or "-")
        right.metric("Cliente", _safe_text(preview.get("cliente")) or "-")
        third.metric("% al cierre", f"{_safe_float(preview.get('porcentaje_asignacion_cierre'), 0.0):.2f}%")

        confirmado = st.checkbox(
            "Confirmo que deseo cerrar y mover este pedido a Enviados.",
            value=False,
            key=f"{key_prefix}_confirmado",
        )

        submitted = st.form_submit_button("Cerrar y mover a enviados")
        if not submitted:
            return None

    return {
        "tipo_cierre": _safe_text(tipo_cierre).lower(),
        "fecha_envio": _date_to_iso(fecha_envio),
        "observacion": _safe_text(observacion),
        "confirmado": bool(confirmado),
    }



def _build_empty_venta_row() -> dict[str, Any]:
    return {
        "sku": "",
        "descripcion": "",
        "producto": "",
        "color": "",
        "cantidad_pedida": 0.0,
        "cantidad_enviada": 0.0,
        "fecha_produccion": None,
    }


@_fragment_decorator
def render_venta_directa_form(
    *,
    is_admin: bool,
    key_prefix: str = "venta_directa",
    default_rows: int = 3,
    extra_estatus_options: list[str] | None = None,
    extra_motivo_pausa_options: list[str] | None = None,
) -> dict[str, Any] | None:
    """
    Formulario para alta manual de ventas directas en Enviados.

    Devuelve un payload con encabezado y líneas:
        {
            "cliente": str,
            "fecha_captura": str | None,
            "fecha_produccion": str | None,
            "fecha_logistica": str | None,
            "fecha_envio": str | None,
            "estatus_venta": str | None,
            "motivo_pausa": str | None,
            "tipo_cierre": "envio total",
            "observacion": str,
            "lineas": [ ... ],
        }
    """
    if not is_admin:
        st.info("Solo el modo Administrador puede registrar ventas directas en Enviados.")
        return None

    rows_key = f"{key_prefix}_rows"
    if rows_key not in st.session_state:
        st.session_state[rows_key] = [_build_empty_venta_row() for _ in range(max(1, default_rows))]

    estatus_options = _build_estatus_options(extra_estatus_options)
    motivo_options = _build_motivo_pausa_options(extra_motivo_pausa_options)

    with st.form(f"{key_prefix}_form", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            cliente = st.text_input("Cliente", key=f"{key_prefix}_cliente")
            estatus_venta = st.selectbox(
                "Estatus venta",
                options=estatus_options,
                key=f"{key_prefix}_estatus_venta",
                help="Catálogo controlado del MVP. Ya no se permite texto libre.",
            )
            if estatus_venta == "Pausa":
                motivo_pausa = st.selectbox(
                    "Motivo de pausa",
                    options=motivo_options,
                    key=f"{key_prefix}_motivo_pausa",
                    help="Obligatorio cuando el estatus de venta es Pausa.",
                )
            else:
                motivo_pausa = None
                st.caption("Motivo de pausa: solo aplica cuando el estatus es Pausa.")
            fecha_captura = st.date_input(
                "Fecha captura",
                value=date.today(),
                format="YYYY-MM-DD",
                key=f"{key_prefix}_fecha_captura",
            )
        with c2:
            fecha_produccion = st.date_input(
                "Fecha producción",
                value=None,
                format="YYYY-MM-DD",
                key=f"{key_prefix}_fecha_produccion",
            )
            fecha_logistica = st.date_input(
                "Fecha logística",
                value=None,
                format="YYYY-MM-DD",
                key=f"{key_prefix}_fecha_logistica",
            )
            fecha_envio = st.date_input(
                "Fecha envío",
                value=date.today(),
                format="YYYY-MM-DD",
                key=f"{key_prefix}_fecha_envio",
            )

        observacion = st.text_area(
            "Observación",
            value="",
            height=80,
            key=f"{key_prefix}_observacion",
        )

        st.markdown("**Líneas de la venta directa**")
        edited_rows: list[dict[str, Any]] = []
        for idx, row in enumerate(st.session_state[rows_key], start=1):
            st.markdown(f"Línea {idx}")
            a, b, c, d = st.columns([2, 3, 2, 2])
            with a:
                sku = st.text_input("SKU", value=_safe_text(row.get("sku")), key=f"{key_prefix}_sku_{idx}")
                producto = st.text_input("Producto", value=_safe_text(row.get("producto")), key=f"{key_prefix}_producto_{idx}")
            with b:
                descripcion = st.text_input(
                    "Descripción",
                    value=_safe_text(row.get("descripcion")),
                    key=f"{key_prefix}_descripcion_{idx}",
                )
                color = st.text_input("Color", value=_safe_text(row.get("color")), key=f"{key_prefix}_color_{idx}")
            with c:
                cantidad_pedida = st.number_input(
                    "Cantidad pedida",
                    min_value=0.0,
                    step=1.0,
                    value=_safe_float(row.get("cantidad_pedida")),
                    key=f"{key_prefix}_cantidad_pedida_{idx}",
                )
                cantidad_enviada = st.number_input(
                    "Cantidad enviada",
                    min_value=0.0,
                    step=1.0,
                    value=_safe_float(row.get("cantidad_enviada")),
                    key=f"{key_prefix}_cantidad_enviada_{idx}",
                )
            with d:
                linea_fecha_prod = st.date_input(
                    "Fecha producción línea",
                    value=_parse_iso_date(row.get("fecha_produccion")),
                    format="YYYY-MM-DD",
                    key=f"{key_prefix}_linea_fecha_produccion_{idx}",
                )

            edited_rows.append(
                {
                    "sku": _safe_text(sku),
                    "descripcion": _safe_text(descripcion),
                    "producto": _safe_text(producto),
                    "color": _safe_text(color),
                    "cantidad_pedida": float(cantidad_pedida),
                    "cantidad_enviada": float(cantidad_enviada),
                    "fecha_produccion": _date_to_iso(linea_fecha_prod),
                }
            )
            st.divider()

        submitted = st.form_submit_button("Registrar venta directa")
        if not submitted:
            return None

    try:
        canonical_estatus, canonical_motivo = normalize_pause_fields(estatus_venta, motivo_pausa)
    except ValueError as exc:
        st.error(str(exc))
        return None

    valid_rows = []
    for row in edited_rows:
        if row["sku"] or row["descripcion"] or row["producto"] or row["cantidad_pedida"] or row["cantidad_enviada"]:
            valid_rows.append(row)

    return {
        "cliente": _safe_text(cliente),
        "fecha_captura": _date_to_iso(fecha_captura),
        "fecha_produccion": _date_to_iso(fecha_produccion),
        "fecha_logistica": _date_to_iso(fecha_logistica),
        "fecha_envio": _date_to_iso(fecha_envio),
        "estatus_venta": canonical_estatus,
        "motivo_pausa": canonical_motivo,
        "tipo_cierre": "envio total",
        "observacion": _safe_text(observacion),
        "lineas": valid_rows,
    }


@_fragment_decorator
def render_staging_datetime_editor(
    row: dict[str, Any],
    *,
    key_prefix: str = "staging_dt",
) -> dict[str, Any] | None:
    """
    Editor visual para fecha/hora en staging.
    """
    fecha_actual = _parse_iso_date(row.get("fecha")) or date.today()

    hora_actual_raw = row.get("hora") or row.get("hora_normalizada") or row.get("hora_corte")
    hora_actual = time(0, 0)
    if isinstance(hora_actual_raw, time):
        hora_actual = hora_actual_raw
    elif hora_actual_raw:
        text = str(hora_actual_raw).strip()
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                hora_actual = datetime.strptime(text, fmt).time()
                break
            except ValueError:
                continue

    with st.form(f"{key_prefix}_form", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            fecha = st.date_input(
                "Fecha",
                value=fecha_actual,
                format="YYYY-MM-DD",
                key=f"{key_prefix}_fecha",
            )
        with c2:
            hora = st.time_input(
                "Hora",
                value=hora_actual,
                step=60,
                key=f"{key_prefix}_hora",
            )
        submitted = st.form_submit_button("Guardar fecha y hora")
        if not submitted:
            return None

    return {
        "fecha": _date_to_iso(fecha),
        "hora": hora.strftime("%H:%M:%S"),
    }



def build_venta_directa_dataframe(lineas: list[dict[str, Any]] | None) -> pd.DataFrame:
    """
    Helper para revisar visualmente las líneas antes de enviar al servicio.
    """
    df = pd.DataFrame(lineas or [])
    if df.empty:
        return df

    ordered = [
        "sku",
        "producto",
        "color",
        "descripcion",
        "cantidad_pedida",
        "cantidad_enviada",
        "fecha_produccion",
    ]
    ordered = [col for col in ordered if col in df.columns] + [col for col in df.columns if col not in ordered]
    return df[ordered]
