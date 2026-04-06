from __future__ import annotations

import pandas as pd
import streamlit as st

from wms_mvp.core.services.bootstrap_service import (
    BootstrapError,
    apply_bootstrap_inicial,
    build_bootstrap_preview,
    list_bootstrap_runs,
)
from wms_mvp.db.connection import db_exists

PAGE_TITLE = "Bootstrap inicial"
PREVIEW_KEY = "bootstrap_inicial_preview"
RESULT_KEY = "bootstrap_inicial_result"


def _safe_text(value: object) -> str:
    return "" if value is None else str(value)



def _get_current_user() -> str:
    for key in ("wms_current_user", "current_user", "auth_user"):
        value = st.session_state.get(key)
        if value:
            return str(value)
    return "Admin"



def _render_header() -> None:
    st.title(PAGE_TITLE)
    st.caption(
        "Herramienta especial de una sola vez para inicializar el MVP: carga inventario base, "
        "pedidos iniciales y entradas históricas sin contaminar la operación diaria."
    )
    st.warning(
        "Este proceso está pensado para la regularización inicial del sistema. Reemplaza los datos operativos principales "
        "cuando se ejecuta con reinicio completo."
    )



def _render_uploads() -> tuple[object | None, object | None, object | None]:
    col1, col2, col3 = st.columns(3)
    inventory_file = col1.file_uploader(
        "Inventario base",
        type=["xlsx", "xlsm", "xls"],
        key="bootstrap_inventory_file",
        help="Archivo de inventario inicial con SKU, descripción, stock y fecha de actualización.",
    )
    pedidos_file = col2.file_uploader(
        "Pedidos iniciales",
        type=["xlsx", "xlsm", "xls"],
        key="bootstrap_pedidos_file",
        help="Archivo inicial de pedidos. Del mismo archivo se separarán activos y enviados históricos.",
    )
    entradas_file = col3.file_uploader(
        "Entradas históricas",
        type=["xlsx", "xlsm", "xls"],
        key="bootstrap_entradas_file",
        help="Archivo de entradas históricas desde la fecha más antigua del corte de inventario.",
    )
    return inventory_file, pedidos_file, entradas_file



def _render_preview_block(preview: dict[str, object]) -> None:
    summary = preview.get("bootstrap_summary", {}) or {}
    inv = preview.get("inventory_preview", {}) or {}
    ped = preview.get("pedidos_analysis", {}) or {}
    ent = preview.get("entradas_analysis", {}) or {}

    row1 = st.columns(5)
    row1[0].metric("SKU inventario válidos", summary.get("inventario_valid_skus", 0))
    row1[1].metric("Filas revisión inventario", summary.get("inventario_review_rows", 0))
    row1[2].metric("Pedidos activos detectados", summary.get("pedidos_activos_detectados", 0))
    row1[3].metric("Pedidos enviados detectados", summary.get("pedidos_enviados_detectados", 0))
    row1[4].metric("Entradas válidas post corte", summary.get("entradas_validas_post_corte", 0))

    row2 = st.columns(4)
    row2[0].metric("Entradas sin base", summary.get("entradas_sku_sin_base", 0))
    row2[1].metric("Entradas omitidas por fecha base", summary.get("entradas_omitidas_por_fecha_base", 0))
    row2[2].metric("Entradas inválidas", summary.get("entradas_invalidas", 0))
    row2[3].metric("Stock total base válido", round(float(inv.get("total_stock_valid") or 0), 2))

    st.markdown("### Resumen de inventario base")
    st.write(
        f"Fechas base detectadas: **{summary.get('inventario_fecha_min') or 'N/D'}** a "
        f"**{summary.get('inventario_fecha_max') or 'N/D'}**"
    )
    with st.expander("Ver filas en revisión del inventario", expanded=False):
        review_df = pd.DataFrame(inv.get("review_rows", [])[:200])
        if review_df.empty:
            st.info("Sin filas en revisión en el inventario base.")
        else:
            st.dataframe(review_df, use_container_width=True, hide_index=True)

    st.markdown("### Resumen de pedidos iniciales")
    per_sheet_df = pd.DataFrame(ped.get("per_sheet_summary", []))
    if per_sheet_df.empty:
        st.info("No se detectaron hojas válidas de pedidos.")
    else:
        st.dataframe(per_sheet_df, use_container_width=True, hide_index=True)

    st.markdown("### Resumen de entradas históricas")
    entries_summary = ent.get("entries_summary", {}) or {}
    entries_df = pd.DataFrame([
        {
            "Filas normalizadas": ent.get("total_normalized_rows", 0),
            "Filas revisión origen": ent.get("total_review_rows", 0),
            "Cortes detectados": ent.get("detected_cortes", 0),
            "Aplicables post corte": entries_summary.get("valid_rows", 0),
            "Omitidas por fecha base": entries_summary.get("skipped_pre_base", 0),
            "SKU sin base": entries_summary.get("missing_inventory", 0),
            "Inválidas": entries_summary.get("invalid_rows", 0),
            "Cantidad aplicable": entries_summary.get("total_qty_valid", 0),
            "Fecha mínima": entries_summary.get("min_fecha") or "",
            "Fecha máxima": entries_summary.get("max_fecha") or "",
        }
    ])
    st.dataframe(entries_df, use_container_width=True, hide_index=True)



def _render_history() -> None:
    st.markdown("### Historial de bootstrap")
    rows = list_bootstrap_runs(limit=15)
    if not rows:
        st.info("Aún no hay corridas de bootstrap registradas.")
        return

    history_df = pd.DataFrame(
        [
            {
                "ID": row.get("id"),
                "Estado": row.get("status"),
                "Inventario": row.get("inventory_file_name"),
                "Pedidos": row.get("pedidos_file_name"),
                "Entradas": row.get("entradas_file_name"),
                "Usuario": row.get("created_by"),
                "Inicio": row.get("started_at"),
                "Fin": row.get("finished_at"),
            }
            for row in rows
        ]
    )
    st.dataframe(history_df, use_container_width=True, hide_index=True)

    with st.expander("Ver último resumen JSON", expanded=False):
        st.json(rows[0].get("summary_json") or "{}")



def main() -> None:
    if not db_exists():
        st.error("La base de datos aún no existe. Inicialízala desde la portada antes de ejecutar el bootstrap.")
        st.stop()

    _render_header()
    inventory_file, pedidos_file, entradas_file = _render_uploads()

    can_preview = all([inventory_file, pedidos_file, entradas_file])
    if st.button("Generar vista previa del bootstrap", type="primary", disabled=not can_preview):
        if not can_preview:
            st.error("Debes cargar los tres archivos para generar la vista previa.")
        else:
            try:
                preview = build_bootstrap_preview(
                    inventory_file_name=inventory_file.name,
                    inventory_file_bytes=inventory_file.getvalue(),
                    pedidos_file_name=pedidos_file.name,
                    pedidos_file_bytes=pedidos_file.getvalue(),
                    entradas_file_name=entradas_file.name,
                    entradas_file_bytes=entradas_file.getvalue(),
                )
                st.session_state[PREVIEW_KEY] = preview
                st.session_state.pop(RESULT_KEY, None)
            except Exception as exc:
                st.error(f"No fue posible generar la vista previa: {exc}")

    preview = st.session_state.get(PREVIEW_KEY)
    if preview:
        _render_preview_block(preview)
        st.markdown("### Ejecutar bootstrap")
        reset_existing = st.checkbox(
            "Reemplazar completamente los datos operativos actuales",
            value=True,
            help="Borra inventario, pedidos, cierres, reservas y staging antes de reconstruir el estado inicial.",
        )
        confirm = st.checkbox(
            "Entiendo que este proceso es especial y puede reemplazar la información operativa actual.",
            value=False,
        )
        if st.button("Ejecutar bootstrap inicial", disabled=not confirm):
            if not all([inventory_file, pedidos_file, entradas_file]):
                st.error("Vuelve a cargar los tres archivos antes de ejecutar el bootstrap.")
            else:
                try:
                    result = apply_bootstrap_inicial(
                        inventory_file_name=inventory_file.name,
                        inventory_file_bytes=inventory_file.getvalue(),
                        pedidos_file_name=pedidos_file.name,
                        pedidos_file_bytes=pedidos_file.getvalue(),
                        entradas_file_name=entradas_file.name,
                        entradas_file_bytes=entradas_file.getvalue(),
                        created_by=_get_current_user(),
                        reset_existing=reset_existing,
                    )
                    st.session_state[RESULT_KEY] = result
                    st.success("Bootstrap inicial ejecutado correctamente.")
                except BootstrapError as exc:
                    st.error(f"Bootstrap detenido: {exc}")
                except Exception as exc:
                    st.error(f"No fue posible ejecutar el bootstrap: {exc}")

    result = st.session_state.get(RESULT_KEY)
    if result:
        st.markdown("### Resultado de la corrida")
        st.json(result)

    st.markdown("---")
    _render_history()


if __name__ == "__main__":
    main()
