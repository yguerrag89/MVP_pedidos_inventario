from __future__ import annotations

import importlib
import io
import re
import sys
import unicodedata
from datetime import datetime
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
from wms_mvp.core.config import INPUT_DIR, PROCESSED_DIR, ensure_directories
from wms_mvp.core.services.entradas_service import (
    detect_cortes_from_rows,
    finish_import_job,
    list_cortes_disponibles,
    list_import_jobs,
    load_staging_rows_for_corte,
    register_detected_cortes,
    start_import_job,
)
from wms_mvp.db.connection import db_exists


PAGE_TITLE = "Cargas y actualizaciones"
FLASH_MESSAGE_KEY = "admin_import_flash_message"
FLASH_TYPE_KEY = "admin_import_flash_type"
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

    col1, col2 = st.columns(2)
    col1.metric("Modo actual", "Administrador" if is_admin else "Consulta")
    col2.metric("Usuario", current_user)

    if is_admin:
        st.success(
            "La sesión está en modo administrador. Las acciones de importación y escritura están habilitadas."
        )
    else:
        st.info(
            "La sesión está en modo consulta. Puedes revisar los análisis y el historial, pero las cargas quedan bloqueadas hasta desbloquear el modo administrador desde la portada."
        )



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
    return int(_safe_float(value, default))



def _normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text



def _unique_output_path(base_dir: Path, filename: str) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)

    raw_name = Path(filename).name
    stem = Path(raw_name).stem
    suffix = Path(raw_name).suffix
    candidate = base_dir / raw_name

    if not candidate.exists():
        return candidate

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return base_dir / f"{stem}_{timestamp}{suffix}"



def _build_preview_df(df: pd.DataFrame, max_rows: int = 30) -> pd.DataFrame:
    if df.empty:
        return df

    preview = df.head(max_rows).copy()
    preview.columns = [str(c) for c in preview.columns]
    return preview



def _get_uploaded_bytes(uploaded_file: Any) -> bytes:
    return uploaded_file.getvalue()



def _save_uploaded_temp_copy(file_name: str, file_bytes: bytes, prefix: str) -> Path:
    temp_dir = PROCESSED_DIR / "_tmp_admin_imports" / prefix
    target = _unique_output_path(temp_dir, file_name)
    target.write_bytes(file_bytes)
    return target



def _save_uploaded_copy(file_name: str, file_bytes: bytes) -> Path:
    target = _unique_output_path(INPUT_DIR, file_name)
    target.write_bytes(file_bytes)
    return target



def _read_uploaded_dataframe(
    file_name: str,
    file_bytes: bytes,
    sheet_name: str | None = None,
    header_row: int = 0,
) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(io.BytesIO(file_bytes), header=header_row)

    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return pd.read_excel(
            io.BytesIO(file_bytes),
            sheet_name=sheet_name,
            header=header_row,
        )

    raise ValueError(f"Formato no soportado: {suffix}")



def _get_excel_sheet_names(file_bytes: bytes) -> list[str]:
    with pd.ExcelFile(io.BytesIO(file_bytes)) as xls:
        return list(xls.sheet_names)



def _try_import(module_name: str):
    try:
        return importlib.import_module(module_name)
    except Exception as exc:
        st.error(f"No fue posible importar `{module_name}`: {exc}")
        return None



def _build_import_jobs_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "ID",
        "Tipo",
        "Archivo",
        "Estado",
        "Total filas",
        "Insertadas",
        "Actualizadas",
        "Error",
        "Notas",
        "Creado por",
        "Inicio",
        "Fin",
    ]

    if not rows:
        return pd.DataFrame(columns=columns)

    data: list[dict[str, Any]] = []
    for row in rows:
        data.append(
            {
                "ID": _safe_int(row.get("id")),
                "Tipo": _safe_text(row.get("import_type")),
                "Archivo": _safe_text(row.get("file_name")),
                "Estado": _safe_text(row.get("status")),
                "Total filas": _safe_int(row.get("total_rows")),
                "Insertadas": _safe_int(row.get("inserted_rows")),
                "Actualizadas": _safe_int(row.get("updated_rows")),
                "Error": _safe_int(row.get("error_rows")),
                "Notas": _safe_text(row.get("notes")),
                "Creado por": _safe_text(row.get("created_by")),
                "Inicio": _safe_text(row.get("started_at")),
                "Fin": _safe_text(row.get("finished_at")),
            }
        )

    return pd.DataFrame(data, columns=columns)



def _build_cortes_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "ID",
        "Fecha",
        "Hora",
        "Estado",
        "Total filas",
        "Total piezas",
        "Archivo",
        "Procesado por",
        "Procesado at",
    ]

    if not rows:
        return pd.DataFrame(columns=columns)

    data: list[dict[str, Any]] = []
    for row in rows:
        data.append(
            {
                "ID": _safe_int(row.get("id")),
                "Fecha": _safe_text(row.get("fecha_corte")),
                "Hora": _safe_text(row.get("hora_corte_normalizada") or row.get("hora_corte_texto")),
                "Estado": _safe_text(row.get("estado_corte")),
                "Total filas": _safe_int(row.get("total_filas")),
                "Total piezas": round(_safe_float(row.get("total_piezas")), 2),
                "Archivo": _safe_text(row.get("file_name")),
                "Procesado por": _safe_text(row.get("procesado_por")),
                "Procesado at": _safe_text(row.get("procesado_at")),
            }
        )

    return pd.DataFrame(data, columns=columns)



def _build_detected_cortes_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "Índice",
        "Fecha corte",
        "Hora corte",
        "Total filas",
        "Total piezas",
    ]

    if not rows:
        return pd.DataFrame(columns=columns)

    data: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        data.append(
            {
                "Índice": index,
                "Fecha corte": _safe_text(row.get("fecha_corte")),
                "Hora corte": _safe_text(row.get("hora_corte_normalizada") or row.get("hora_corte_texto")),
                "Total filas": _safe_int(row.get("total_filas")),
                "Total piezas": round(_safe_float(row.get("total_piezas")), 2),
            }
        )

    return pd.DataFrame(data, columns=columns)



def _build_sheet_summary_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = ["Hoja", "Rol", "Header row", "Filas válidas", "Filas rechazadas", "Observaciones"]
    if not rows:
        return pd.DataFrame(columns=columns)

    data: list[dict[str, Any]] = []
    for row in rows:
        data.append(
            {
                "Hoja": _safe_text(row.get("sheet_name")),
                "Rol": _safe_text(row.get("sheet_role")),
                "Header row": _safe_int(row.get("header_row")),
                "Filas válidas": _safe_int(row.get("valid_rows")),
                "Filas rechazadas": _safe_int(row.get("rejected_rows") or row.get("error_rows")),
                "Observaciones": _safe_text(row.get("notes") or row.get("observaciones")),
            }
        )
    return pd.DataFrame(data, columns=columns)



def _build_simple_result_df(rows: list[dict[str, Any]], kind: str) -> pd.DataFrame:
    if kind == "created_carga_inicial":
        columns = ["ID pedido", "Pedido DB", "Líneas", "Rol hoja", "Estado operativo"]
        data = [
            {
                "ID pedido": _safe_text(row.get("id_pedido")),
                "Pedido DB": _safe_int(row.get("pedido_id")),
                "Líneas": _safe_int(row.get("lineas")),
                "Rol hoja": _safe_text(row.get("sheet_role")),
                "Estado operativo": _safe_text(row.get("estado_operativo")),
            }
            for row in rows
        ]
        return pd.DataFrame(data, columns=columns)

    if kind == "skipped_carga_inicial":
        columns = ["ID pedido", "Motivo", "Líneas", "Rol hoja"]
        data = [
            {
                "ID pedido": _safe_text(row.get("id_pedido")),
                "Motivo": _safe_text(row.get("reason")),
                "Líneas": _safe_int(row.get("lineas")),
                "Rol hoja": _safe_text(row.get("sheet_role")),
            }
            for row in rows
        ]
        return pd.DataFrame(data, columns=columns)

    if kind == "failed_carga_inicial":
        columns = ["ID pedido", "Motivo", "Líneas", "Rol hoja"]
        data = [
            {
                "ID pedido": _safe_text(row.get("id_pedido")),
                "Motivo": _safe_text(row.get("reason")),
                "Líneas": _safe_int(row.get("lineas")),
                "Rol hoja": _safe_text(row.get("sheet_role")),
            }
            for row in rows
        ]
        return pd.DataFrame(data, columns=columns)

    if kind == "created_pxs":
        columns = ["ID pedido", "Pedido DB", "Líneas"]
        data = [
            {
                "ID pedido": _safe_text(row.get("id_pedido")),
                "Pedido DB": _safe_int(row.get("pedido_id")),
                "Líneas": _safe_int(row.get("lineas")),
            }
            for row in rows
        ]
        return pd.DataFrame(data, columns=columns)

    columns = ["ID pedido", "Motivo", "Líneas"]
    data = [
        {
            "ID pedido": _safe_text(row.get("id_pedido")),
            "Motivo": _safe_text(row.get("reason")),
            "Líneas": _safe_int(row.get("lineas")),
        }
        for row in rows
    ]
    return pd.DataFrame(data, columns=columns)



def _render_key_metrics(metrics: list[tuple[str, Any]]) -> None:
    if not metrics:
        return
    cols = st.columns(len(metrics))
    for col, (label, value) in zip(cols, metrics):
        col.metric(label, value)



def _render_dataframe_or_info(df: pd.DataFrame, empty_message: str) -> None:
    if df is None or df.empty:
        st.info(empty_message)
        return
    st.dataframe(df, use_container_width=True, hide_index=True)



def _render_analysis_common(
    normalized_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    key_columns: list[str],
) -> None:
    st.write("**Vista previa de filas normalizadas**")
    if normalized_df is None or normalized_df.empty:
        st.info("No hay filas normalizadas para mostrar.")
    else:
        preview_columns = [col for col in key_columns if col in normalized_df.columns]
        preview_df = normalized_df[preview_columns].copy() if preview_columns else normalized_df.copy()
        st.dataframe(_build_preview_df(preview_df, max_rows=30), use_container_width=True, hide_index=True)

    st.write("**Filas rechazadas**")
    if rejected_df is None or rejected_df.empty:
        st.info("No se detectaron filas rechazadas en el análisis.")
    else:
        st.dataframe(_build_preview_df(rejected_df, max_rows=30), use_container_width=True, hide_index=True)



def _render_result_tables(result: dict[str, Any], result_kind: str) -> None:
    if result_kind == "carga_inicial":
        tabs = st.tabs(["Creados", "Duplicados omitidos", "Fallidos"])
        with tabs[0]:
            _render_dataframe_or_info(
                _build_simple_result_df(result.get("created_pedidos", []), "created_carga_inicial"),
                "No se crearon pedidos en esta ejecución.",
            )
        with tabs[1]:
            _render_dataframe_or_info(
                _build_simple_result_df(result.get("skipped_existing", []), "skipped_carga_inicial"),
                "No hubo pedidos duplicados omitidos.",
            )
        with tabs[2]:
            _render_dataframe_or_info(
                _build_simple_result_df(result.get("failed_pedidos", []), "failed_carga_inicial"),
                "No hubo pedidos fallidos.",
            )
        return

    tabs = st.tabs(["Creados", "Activos ampliados", "Duplicados omitidos", "Por revisar", "Fallidos"])
    with tabs[0]:
        _render_dataframe_or_info(
            _build_simple_result_df(result.get("created_pedidos", []), "created_pxs"),
            "No se crearon pedidos en esta ejecución.",
        )
    with tabs[1]:
        _render_dataframe_or_info(
            _build_simple_result_df(result.get("appended_activos", []), "appended_pxs"),
            "No se ampliaron pedidos activos en esta ejecución.",
        )
    with tabs[2]:
        _render_dataframe_or_info(
            _build_simple_result_df(result.get("skipped_existing", []), "skipped_pxs"),
            "No hubo pedidos duplicados omitidos.",
        )
    with tabs[3]:
        _render_dataframe_or_info(
            _build_simple_result_df(result.get("unresolved_pedidos", []), "unresolved_pxs"),
            "No quedaron pedidos por revisar.",
        )
    with tabs[4]:
        _render_dataframe_or_info(
            _build_simple_result_df(result.get("failed_pedidos", []), "failed_pxs"),
            "No hubo pedidos fallidos.",
        )


# ---------------------------------------------------------
# Mapeo de columnas para entradas asistidas
# ---------------------------------------------------------
COLUMN_SYNONYMS: dict[str, list[str]] = {
    "fecha": ["fecha", "fecha_corte", "fecha_entrada"],
    "hora": ["hora", "hora_corte", "hora_entrada"],
    "clave": ["clave", "sku", "clave_producto", "producto_clave"],
    "nombre_del_producto": [
        "nombre_del_producto",
        "descripcion",
        "producto",
        "nombre_producto",
        "descripcion_producto",
    ],
    "piezas": ["piezas", "cantidad", "qty", "unidades"],
    "almacen_y_o_pedido": [
        "almacen_y_o_pedido",
        "almacen_y_pedido",
        "destino",
        "serie_y_folio",
        "pedido",
        "almacen",
        "almacen_pedido",
    ],
    "cajas": ["cajas", "no_cajas", "cantidad_cajas"],
}



def _auto_detect_mapping(df: pd.DataFrame) -> dict[str, str]:
    normalized_columns = {_normalize_name(col): str(col) for col in df.columns}

    mapping: dict[str, str] = {}
    for canonical_name, aliases in COLUMN_SYNONYMS.items():
        detected = ""
        for alias in aliases:
            alias_norm = _normalize_name(alias)
            if alias_norm in normalized_columns:
                detected = normalized_columns[alias_norm]
                break
        mapping[canonical_name] = detected

    return mapping



def _render_column_mapping(df: pd.DataFrame) -> dict[str, str]:
    st.subheader("Mapeo de columnas")
    st.caption(
        "Aquí eliges qué columna del archivo corresponde a cada campo canónico de entradas."
    )

    auto_mapping = _auto_detect_mapping(df)
    source_columns = [""] + [str(col) for col in df.columns]

    st.write("**Columnas detectadas en el archivo**")
    st.write(", ".join([str(col) for col in df.columns]))

    mapping: dict[str, str] = {}
    cols = st.columns(2)
    canonical_order = [
        "fecha",
        "hora",
        "clave",
        "nombre_del_producto",
        "piezas",
        "almacen_y_o_pedido",
        "cajas",
    ]

    for idx, canonical_name in enumerate(canonical_order):
        default_value = auto_mapping.get(canonical_name, "")
        default_index = source_columns.index(default_value) if default_value in source_columns else 0

        selected = cols[idx % 2].selectbox(
            f"{canonical_name}",
            options=source_columns,
            index=default_index,
            key=f"admin_import_mapping_{canonical_name}",
        )
        mapping[canonical_name] = selected

    return mapping



def _build_normalized_df(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    normalized = pd.DataFrame(index=df.index)

    for canonical_name, source_name in mapping.items():
        if source_name and source_name in df.columns:
            normalized[canonical_name] = df[source_name]
        else:
            normalized[canonical_name] = None

    normalized.columns = [str(c) for c in normalized.columns]
    return normalized



def _render_normalized_preview(normalized_df: pd.DataFrame) -> None:
    st.subheader("Vista previa normalizada")

    preview_df = _build_preview_df(normalized_df, max_rows=30)
    if preview_df.empty:
        st.info("La vista normalizada está vacía.")
        return

    st.dataframe(preview_df, use_container_width=True, hide_index=True)

    total_rows = len(normalized_df)
    rows_sin_fecha = int(normalized_df["fecha"].isna().sum()) if "fecha" in normalized_df.columns else total_rows
    rows_sin_hora = int(normalized_df["hora"].isna().sum()) if "hora" in normalized_df.columns else total_rows
    rows_sin_sku = int(normalized_df["clave"].isna().sum()) if "clave" in normalized_df.columns else total_rows
    total_piezas = (
        pd.to_numeric(normalized_df["piezas"], errors="coerce").fillna(0).sum()
        if "piezas" in normalized_df.columns
        else 0
    )

    _render_key_metrics(
        [
            ("Filas archivo", total_rows),
            ("Total piezas", round(float(total_piezas), 2)),
            ("Filas sin fecha", rows_sin_fecha),
            ("Filas sin hora", rows_sin_hora),
        ]
    )
    st.caption(
        f"Filas sin SKU/clave detectada: {rows_sin_sku}. "
        "Eso no impide cargar staging, pero esas filas luego pueden terminar en revisión."
    )


# ---------------------------------------------------------
# Flujo de importación asistida de entradas
# ---------------------------------------------------------
def _build_detected_corte_label(corte: dict[str, Any], index: int) -> str:
    return (
        f"{index} | "
        f"{_safe_text(corte.get('fecha_corte'))} "
        f"{_safe_text(corte.get('hora_corte_normalizada') or corte.get('hora_corte_texto'))} | "
        f"filas={_safe_int(corte.get('total_filas'))} | "
        f"piezas={round(_safe_float(corte.get('total_piezas')), 2)}"
    )



def _run_assisted_import(
    import_type: str,
    original_file_name: str,
    file_bytes: bytes,
    normalized_df: pd.DataFrame,
    selected_detected_index: int,
    usuario: str | None,
    notes: str | None,
    save_copy: bool,
) -> tuple[int, int, int]:
    normalized_rows = normalized_df.where(pd.notna(normalized_df), None).to_dict(orient="records")

    if not normalized_rows:
        raise ValueError("No hay filas normalizadas para importar.")

    detected_cortes = detect_cortes_from_rows(normalized_rows)
    if not detected_cortes:
        raise ValueError(
            "No se detectaron cortes válidos. Revisa que el archivo tenga fecha y hora interpretables."
        )

    if selected_detected_index < 0 or selected_detected_index >= len(detected_cortes):
        raise ValueError("El índice del corte seleccionado no es válido.")

    file_path: Path | None = None
    if save_copy:
        file_path = _save_uploaded_copy(original_file_name, file_bytes)

    import_job_id = start_import_job(
        file_name=original_file_name,
        file_path=file_path,
        import_type=import_type,
        created_by=usuario or None,
        notes=notes or None,
    )

    registered_cortes = register_detected_cortes(import_job_id, normalized_rows)
    selected_registered_corte = registered_cortes[selected_detected_index]
    selected_rows = selected_registered_corte.get("rows", [])

    inserted_count = load_staging_rows_for_corte(
        corte_id=_safe_int(selected_registered_corte["id"]),
        raw_rows=selected_rows,
    )

    total_rows = len(normalized_rows)
    error_rows = max(total_rows - inserted_count, 0)

    finish_import_job(
        import_job_id=import_job_id,
        status="COMPLETADO_CON_REVISION",
        total_rows=total_rows,
        inserted_rows=inserted_count,
        updated_rows=0,
        error_rows=error_rows,
        notes=((notes or "").strip() + " | " if (notes or "").strip() else "")
        + f"Importación asistida. Cortes detectados: {len(registered_cortes)}. "
        + f"Staging cargado solo para corte_id={selected_registered_corte['id']}.",
    )

    return import_job_id, _safe_int(selected_registered_corte["id"]), inserted_count


# ---------------------------------------------------------
# UI general
# ---------------------------------------------------------
def _render_header(import_jobs: list[dict[str, Any]], cortes: list[dict[str, Any]]) -> None:
    st.title(PAGE_TITLE)
    st.caption(
        "Desde aquí puedes cargar pedidos iniciales, pedidos nuevos y entradas de producción. "
        "La idea es separar claramente cada tipo de carga para que el trabajo diario sea más simple."
    )

    total_jobs = len(import_jobs)
    total_cortes = len(cortes)
    jobs_fallidos = sum(1 for row in import_jobs if _safe_text(row.get("status")) == "FALLIDO")
    cortes_pendientes = sum(1 for row in cortes if _safe_text(row.get("estado_corte")) == "PENDIENTE")

    _render_key_metrics(
        [
            ("Cargas registradas", total_jobs),
            ("Entradas detectadas", total_cortes),
            ("Cargas con error", jobs_fallidos),
            ("Entradas pendientes", cortes_pendientes),
        ]
    )

    st.info(
        "Regla clave para la carga inicial: **Hoja 1 = pedidos activos** y **2026 = pedidos ya enviados**. "
        "La hoja `codigos` solo se usa para completar producto y color cuando exista."
    )



def _render_history(import_jobs: list[dict[str, Any]], cortes: list[dict[str, Any]]) -> None:
    tab1, tab2 = st.tabs(["Historial de cargas", "Historial de entradas detectadas"])

    with tab1:
        jobs_df = _build_import_jobs_df(import_jobs)
        _render_dataframe_or_info(jobs_df, "Aún no hay importaciones registradas.")

    with tab2:
        cortes_df = _build_cortes_df(cortes)
        _render_dataframe_or_info(cortes_df, "Aún no hay cortes registrados.")



def _render_directories_info() -> None:
    with st.expander("Ubicación de archivos (opcional)", expanded=False):
        st.subheader("Ubicación de archivos")
        col1, col2 = st.columns(2)
        col1.write(f"**INPUT_DIR:** `{INPUT_DIR}`")
        col2.write(f"**PROCESSED_DIR:** `{PROCESSED_DIR}`")
        st.caption(
            "Las cargas desde pantalla pueden guardar una copia del archivo en `data/input`. "
            "También se generan archivos temporales internos cuando hace falta analizar antes de importar."
        )


# ---------------------------------------------------------
# UI - Carga inicial
# ---------------------------------------------------------
def _render_carga_inicial_import() -> None:
    st.subheader("Carga inicial de pedidos")
    with st.expander("Qué hace esta importación", expanded=False):
        st.markdown(
            """
            - lee el archivo inicial de pedidos
            - toma **Hoja 1** como pedidos activos
            - toma **2026** como pedidos enviados históricos
            - **no permite seleccionar hojas libremente** en esta etapa del MVP
            - ignora `PENDIENTES`, `Estatus 2025`, `codigos` y `CLIENTES` como fuentes de pedidos
            - usa `codigos` solo para enriquecer **producto**, **color** y **descripción**
            - crea pedidos y líneas en la base
            - para la hoja `2026`, también crea el cierre histórico del pedido
            """
        )

    pedidos_loader = _try_import("wms_mvp.etl.pedidos_loader")
    if pedidos_loader is None:
        return

    uploaded_file = st.file_uploader(
        "Archivo de carga inicial",
        type=["xlsx", "xlsm", "xls"],
        key="admin_ci_uploaded_file",
    )
    if not uploaded_file:
        st.info("Sube el archivo de carga inicial para continuar.")
        return

    file_name = uploaded_file.name
    file_bytes = _get_uploaded_bytes(uploaded_file)
    st.write(f"**Archivo:** {file_name}")
    st.write(f"**Tamaño:** {round(len(file_bytes) / 1024, 2)} KB")

    try:
        workbook_sheets = _get_excel_sheet_names(file_bytes)
    except Exception as exc:
        st.error(f"No fue posible leer las hojas del archivo: {exc}")
        return

    normalized_sheet_map = {_normalize_name(sheet): sheet for sheet in workbook_sheets}
    expected_sheet_names = [
        normalized_sheet_map.get("hoja_1", "Hoja 1"),
        normalized_sheet_map.get("2026", "2026"),
    ]
    present_expected = [sheet for sheet in expected_sheet_names if sheet in workbook_sheets]
    missing_expected = [sheet for sheet in ["Hoja 1", "2026"] if _normalize_name(sheet) not in normalized_sheet_map]
    ignored_present = [
        sheet for sheet in workbook_sheets
        if _normalize_name(sheet) in {"pendientes", "estatus_2025", "codigos", "clientes"}
    ]
    disallowed_present = [
        sheet for sheet in workbook_sheets
        if _normalize_name(sheet) not in {"hoja_1", "2026", "pendientes", "estatus_2025", "codigos", "clientes"}
    ]

    st.markdown("**Regla operativa del MVP para esta carga**")
    st.info(
        "La app analizará automáticamente solo las hojas válidas del MVP: "
        "**Hoja 1** como pedidos activos y **2026** como enviados históricos. "
        "La hoja `codigos` es opcional y solo se usa para enriquecer producto/color/descripción."
    )

    col_rule_1, col_rule_2 = st.columns(2)
    col_rule_1.write("**Hojas válidas detectadas**")
    if present_expected:
        col_rule_1.success(", ".join(present_expected))
    else:
        col_rule_1.error("No se detectaron `Hoja 1` ni `2026`.")

    col_rule_2.write("**Otras hojas detectadas**")
    if ignored_present:
        col_rule_2.caption("Ignoradas por regla: " + ", ".join(ignored_present))
    if disallowed_present:
        col_rule_2.warning("Fuera del flujo del MVP: " + ", ".join(disallowed_present))
    if not ignored_present and not disallowed_present:
        col_rule_2.caption("No se detectaron hojas adicionales.")

    if missing_expected:
        st.error(
            "El archivo no cumple con la estructura mínima del MVP. "
            "Faltan las hojas requeridas: " + ", ".join(missing_expected) + "."
        )
        st.stop()

    col1, col2, col3 = st.columns(3)
    usuario = col1.text_input("Usuario", value=_get_current_user() if _is_admin_session() else "", key="admin_ci_usuario")
    notes = col2.text_input(
        "Notas",
        value="Carga inicial desde Admin Importaciones",
        key="admin_ci_notes",
    )
    skip_existing = col3.checkbox(
        "Omitir pedidos ya existentes",
        value=True,
        key="admin_ci_skip_existing",
    )

    col4, col5 = st.columns(2)
    save_copy = col4.checkbox(
        "Guardar copia en data/input",
        value=True,
        key="admin_ci_save_copy",
    )
    archive_copy = col5.checkbox(
        "Guardar copia en archive",
        value=False,
        key="admin_ci_archive_copy",
    )

    selected_sheet_names = None

    analysis: dict[str, Any] | None = None
    analysis_error: str | None = None
    temp_analysis_path: Path | None = None

    try:
        temp_analysis_path = _save_uploaded_temp_copy(file_name, file_bytes, prefix="carga_inicial")
        analysis = pedidos_loader.analyze_carga_inicial_file(
            file_path=temp_analysis_path,
            sheet_names=selected_sheet_names,
        )
    except Exception as exc:
        analysis_error = str(exc)

    if analysis_error:
        st.error(f"No fue posible analizar la carga inicial: {analysis_error}")
        return
    if not analysis:
        st.warning("No hubo resultado de análisis para la carga inicial.")
        return

    allowed_present = analysis.get("present_allowed_sheets", []) or []
    ignored_present = analysis.get("ignored_present_sheets", []) or []
    disallowed_present = analysis.get("disallowed_present_sheets", []) or []
    missing_allowed = analysis.get("missing_allowed_sheets", []) or []

    info_rows = []
    for sheet in allowed_present:
        info_rows.append({"Hoja": sheet, "Uso en MVP": "Se importará"})
    for sheet in ignored_present:
        info_rows.append({"Hoja": sheet, "Uso en MVP": "Ignorada por regla"})
    for sheet in disallowed_present:
        info_rows.append({"Hoja": sheet, "Uso en MVP": "Fuera del flujo principal"})
    if info_rows:
        st.write("**Interpretación de hojas detectadas**")
        st.dataframe(pd.DataFrame(info_rows), use_container_width=True, hide_index=True)

    if missing_allowed:
        st.error(
            "La importación no puede ejecutarse porque faltan hojas requeridas: "
            + ", ".join(missing_allowed)
            + "."
        )
        return

    _render_key_metrics(
        [
            ("Filas válidas", analysis.get("total_valid_rows", 0)),
            ("Filas rechazadas", analysis.get("total_rejected_rows", 0)),
            ("Pedidos detectados", analysis.get("total_pedidos_detected", 0)),
            ("Activos detectados", analysis.get("total_pedidos_activos_detected", 0)),
            ("Enviados detectados", analysis.get("total_pedidos_enviados_detected", 0)),
        ]
    )

    st.write("**Resumen por hoja**")
    _render_dataframe_or_info(
        _build_sheet_summary_df(analysis.get("per_sheet_summary", [])),
        "No se generó resumen por hoja.",
    )

    _render_analysis_common(
        normalized_df=analysis.get("normalized_df", pd.DataFrame()),
        rejected_df=analysis.get("rejected_df", pd.DataFrame()),
        key_columns=[
            "sheet_name",
            "sheet_role",
            "id_pedido",
            "cliente",
            "sku",
            "producto",
            "color",
            "cantidad_pedida",
            "cantidad_asignada",
            "cantidad_faltante",
            "fecha_captura",
            "fecha_produccion",
            "fecha_logistica",
            "estatus_venta",
            "estado_operativo",
        ],
    )

    st.warning(
        "Esta acción insertará pedidos en la base. La app solo usará `Hoja 1` como activos y `2026` como enviados históricos. "
        "Las demás hojas no participarán en la creación de pedidos en esta etapa del MVP."
    )

    is_admin = _is_admin_session()
    if not is_admin:
        st.caption("Desbloquea modo administrador desde la portada para ejecutar esta carga.")

    if st.button("Ejecutar carga inicial", type="primary", key="admin_ci_run", disabled=not is_admin):
        if not _is_admin_session():
            st.error("La sesión ya no está en modo administrador. Vuelve a desbloquearla antes de importar.")
            st.stop()
        temp_import_path = _save_uploaded_temp_copy(file_name, file_bytes, prefix="carga_inicial")
        try:
            result = pedidos_loader.import_carga_inicial_from_file(
                file_path=temp_import_path,
                sheet_names=selected_sheet_names,
                created_by=usuario or None,
                notes=notes or None,
                save_copy_to_input=save_copy,
                archive_source_copy=archive_copy,
                skip_existing=skip_existing,
            )
            created = result.get("created_pedidos", [])
            activos = sum(1 for row in created if _safe_text(row.get("sheet_role")) == "ACTIVO")
            enviados = sum(1 for row in created if _safe_text(row.get("sheet_role")) == "ENVIADO_HISTORICO")
            _set_flash(
                "Carga inicial completada. "
                f"Pedidos creados={len(created)} | activos={activos} | enviados históricos={enviados} | "
                f"duplicados omitidos={len(result.get('skipped_existing', []))} | "
                f"fallidos={len(result.get('failed_pedidos', []))}."
            )
            st.session_state["admin_ci_last_result"] = result
            _rerun()
        except Exception as exc:
            st.error(f"No fue posible ejecutar la carga inicial: {exc}")

    last_result = st.session_state.get("admin_ci_last_result")
    if last_result:
        st.markdown("---")
        st.write("**Último resultado de carga inicial**")
        _render_result_tables(last_result, result_kind="carga_inicial")


# ---------------------------------------------------------
# UI - PXS limpio
# ---------------------------------------------------------
def _render_pxs_limpio_import() -> None:
    st.subheader("Cargar pedidos nuevos desde PXS")
    with st.expander("Qué hace esta importación", expanded=False):
        st.markdown(
            """
            - analiza un archivo PXS limpio
            - detecta hojas válidas de pedidos
            - agrupa partidas por `ID_pedido`
            - crea pedidos activos nuevos
            - omite duplicados si así lo indicas
            - no asigna stock automáticamente
            """
        )

    pxs_loader = _try_import("wms_mvp.etl.pxs_loader")
    if pxs_loader is None:
        return

    uploaded_file = st.file_uploader(
        "Archivo de pedidos nuevos (PXS)",
        type=["xlsx", "xlsm", "xls", "csv"],
        key="admin_pxs_uploaded_file",
    )
    if not uploaded_file:
        st.info("Sube el archivo de pedidos nuevos para continuar.")
        return

    file_name = uploaded_file.name
    file_bytes = _get_uploaded_bytes(uploaded_file)
    suffix = Path(file_name).suffix.lower()

    st.write(f"**Archivo:** {file_name}")
    st.write(f"**Tamaño:** {round(len(file_bytes) / 1024, 2)} KB")

    workbook_sheets: list[str] = []
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        try:
            workbook_sheets = _get_excel_sheet_names(file_bytes)
        except Exception as exc:
            st.error(f"No fue posible leer las hojas del archivo: {exc}")
            return

    default_sheet_names = []
    preferred_names = {"pxs", "pxs_limpio", "pedidos", "pedidos_activos", "hoja1", "hoja_1"}
    if workbook_sheets:
        default_sheet_names = [sheet for sheet in workbook_sheets if _normalize_name(sheet) in preferred_names]
        selected_sheets = st.multiselect(
            "Hojas a considerar",
            options=workbook_sheets,
            default=default_sheet_names or workbook_sheets,
            key="admin_pxs_selected_sheets",
        )
        selected_sheet_names = selected_sheets or None
    else:
        selected_sheet_names = None

    col1, col2, col3 = st.columns(3)
    usuario = col1.text_input("Usuario", value=_get_current_user() if _is_admin_session() else "", key="admin_pxs_usuario")
    notes = col2.text_input(
        "Notas",
        value="Importación PXS limpio desde Admin Importaciones",
        key="admin_pxs_notes",
    )
    skip_existing = col3.checkbox(
        "Omitir pedidos ya existentes",
        value=True,
        key="admin_pxs_skip_existing",
    )

    col4, col5 = st.columns(2)
    save_copy = col4.checkbox(
        "Guardar copia en data/input",
        value=True,
        key="admin_pxs_save_copy",
    )
    archive_copy = col5.checkbox(
        "Guardar copia en archive",
        value=False,
        key="admin_pxs_archive_copy",
    )

    analysis: dict[str, Any] | None = None
    analysis_error: str | None = None
    temp_analysis_path = _save_uploaded_temp_copy(file_name, file_bytes, prefix="pxs_limpio")

    try:
        analysis = pxs_loader.analyze_pxs_limpio_file(
            file_path=temp_analysis_path,
            sheet_names=selected_sheet_names,
        )
    except Exception as exc:
        analysis_error = str(exc)

    if analysis_error:
        st.error(f"No fue posible analizar el PXS limpio: {analysis_error}")
        return
    if not analysis:
        st.warning("No hubo resultado de análisis para PXS limpio.")
        return

    pedido_resolution_df = analysis.get("pedido_resolution_df", pd.DataFrame())
    pedido_overrides: dict[str, dict[str, Any]] = {}

    incomplete_df = pd.DataFrame()
    if pedido_resolution_df is not None and not pedido_resolution_df.empty and "clasificacion_pedido" in pedido_resolution_df.columns:
        incomplete_df = pedido_resolution_df[pedido_resolution_df["clasificacion_pedido"] == "PEDIDO_NUEVO_INCOMPLETO"].copy()

    if not incomplete_df.empty:
        st.write("**Completar pedidos nuevos incompletos antes de importar**")
        st.caption(
            "Aquí puedes capturar el cliente final para los pedidos nuevos incompletos. "
            "La vista previa y la importación segura usarán ese valor sin alterar el archivo original."
        )

        editor_cols = [
            col
            for col in [
                "id_pedido",
                "lineas_en_archivo",
                "resumen_clasificacion",
                "cliente_archivo",
            ]
            if col in incomplete_df.columns
        ]
        editor_df = incomplete_df[editor_cols].copy()
        editor_df["cliente_final"] = editor_df.get("cliente_archivo", "").fillna("")
        editor_df["observaciones_importacion"] = ""

        edited_df = st.data_editor(
            editor_df,
            key="admin_pxs_incomplete_editor",
            hide_index=True,
            use_container_width=True,
            disabled=[col for col in editor_df.columns if col not in {"cliente_final", "observaciones_importacion"}],
            num_rows="fixed",
        )

        for row in edited_df.to_dict(orient="records"):
            id_pedido = _safe_text(row.get("id_pedido"))
            cliente_final = _safe_text(row.get("cliente_final"))
            observaciones_importacion = _safe_text(row.get("observaciones_importacion"))
            if not id_pedido or not cliente_final:
                continue
            pedido_overrides[id_pedido] = {
                "cliente": cliente_final,
                "observaciones": observaciones_importacion or None,
            }

        if pedido_overrides:
            st.info(
                f"Se aplicará vista previa con cliente completado en {len(pedido_overrides)} pedido(s) incompleto(s)."
            )
            try:
                analysis = pxs_loader.analyze_pxs_limpio_file(
                    file_path=temp_analysis_path,
                    sheet_names=selected_sheet_names,
                    pedido_overrides=pedido_overrides,
                )
                pedido_resolution_df = analysis.get("pedido_resolution_df", pd.DataFrame())
            except Exception as exc:
                st.error(f"No fue posible recalcular la vista previa con pedidos completados: {exc}")
                return

    classification_summary = analysis.get("classification_summary") or {}
    _render_key_metrics(
        [
            ("Filas válidas", analysis.get("total_valid_rows", 0)),
            ("Filas rechazadas", analysis.get("total_rejected_rows", 0)),
            ("Pedidos detectados", analysis.get("total_pedidos_detected", 0)),
            ("Pedidos seguros", sum(classification_summary.get(key, 0) for key in ["PEDIDO_NUEVO_COMPLETO", "AMPLIACION_PEDIDO_ACTIVO", "SOLO_DUPLICADOS"])),
        ]
    )

    st.write("**Resumen por hoja**")
    per_sheet = pd.DataFrame(analysis.get("per_sheet_summary", []))
    _render_dataframe_or_info(per_sheet, "No se generó resumen por hoja.")

    classification_rows = [
        {"Clasificación": key, "Pedidos": value}
        for key, value in sorted(classification_summary.items())
    ]
    st.write("**Clasificación por pedido**")
    _render_dataframe_or_info(pd.DataFrame(classification_rows), "No se generó clasificación por pedido.")

    if pedido_resolution_df is not None and not pedido_resolution_df.empty:
        preferred_cols = [
            col
            for col in [
                "id_pedido",
                "cliente_archivo",
                "cliente_sistema",
                "estado_operativo_sistema",
                "lineas_en_archivo",
                "clasificacion_pedido",
                "accion_sugerida",
                "resumen_clasificacion",
                "safe_to_apply",
            ]
            if col in pedido_resolution_df.columns
        ]
        st.write("**Resolución propuesta por pedido**")
        st.dataframe(
            _build_preview_df(pedido_resolution_df[preferred_cols] if preferred_cols else pedido_resolution_df, max_rows=60),
            use_container_width=True,
            hide_index=True,
        )

    line_resolution_df = analysis.get("line_resolution_df", pd.DataFrame())
    if line_resolution_df is not None and not line_resolution_df.empty:
        preferred_cols = [
            col
            for col in [
                "id_pedido",
                "sku",
                "producto",
                "color",
                "cantidad_archivo",
                "cantidad_existente_sistema",
                "estado_operativo_sistema",
                "clasificacion",
                "accion_sugerida",
                "origen_filas",
            ]
            if col in line_resolution_df.columns
        ]
        with st.expander("Detalle por línea / SKU", expanded=False):
            st.dataframe(
                _build_preview_df(line_resolution_df[preferred_cols] if preferred_cols else line_resolution_df, max_rows=120),
                use_container_width=True,
                hide_index=True,
            )

    _render_analysis_common(
        normalized_df=analysis.get("normalized_df", pd.DataFrame()),
        rejected_df=analysis.get("rejected_df", pd.DataFrame()),
        key_columns=[
            "id_pedido",
            "cliente",
            "sku",
            "producto",
            "color",
            "cantidad_pedida",
            "cantidad_asignada",
            "fecha_captura",
            "fecha_produccion",
            "fecha_logistica",
            "estatus_venta",
        ],
    )

    st.warning(
        "Esta importación ya no trata el archivo como pedido completo automático. "
        "Primero clasifica: duplicados, pedidos nuevos incompletos, ampliaciones de pedidos activos, cambios y conflictos sobre pedidos cerrados. "
        "La ejecución solo aplica los casos seguros."
    )

    oferta_rows = analysis.get("oferta_rows") or analysis.get("oferta_por_sku") or analysis.get("oferta_sku") or []
    if oferta_rows:
        st.write("**Oferta actual por SKU para los pedidos que sí podrían importarse o revisarse**")
        oferta_df = pd.DataFrame(oferta_rows)
        preferred_cols = [
            col
            for col in [
                "id_pedido",
                "sku",
                "producto",
                "cantidad_pedida",
                "stock_libre_actual",
                "cantidad_surtible_inmediato",
                "faltante_si_se_asigna_ahora",
            ]
            if col in oferta_df.columns
        ]
        if preferred_cols:
            oferta_df = oferta_df[preferred_cols]
        st.dataframe(_build_preview_df(oferta_df, max_rows=40), use_container_width=True, hide_index=True)

    is_admin = _is_admin_session()
    if not is_admin:
        st.caption("Desbloquea modo administrador desde la portada para ejecutar esta importación.")

    if st.button("Ejecutar carga segura de pedidos nuevos", type="primary", key="admin_pxs_run", disabled=not is_admin):
        if not _is_admin_session():
            st.error("La sesión ya no está en modo administrador. Vuelve a desbloquearla antes de importar.")
            st.stop()
        temp_import_path = _save_uploaded_temp_copy(file_name, file_bytes, prefix="pxs_limpio")
        try:
            result = pxs_loader.import_pxs_limpio_from_file(
                file_path=temp_import_path,
                sheet_names=selected_sheet_names,
                created_by=usuario or None,
                notes=notes or None,
                save_copy_to_input=save_copy,
                archive_source_copy=archive_copy,
                skip_existing=skip_existing,
                pedido_overrides=pedido_overrides or None,
            )
            _set_flash(
                "Importación PXS limpio completada. "
                f"Pedidos creados={len(result.get('created_pedidos', []))} | "
                f"activos ampliados={len(result.get('appended_activos', []))} | "
                f"duplicados omitidos={len(result.get('skipped_existing', []))} | "
                f"por revisar={len(result.get('unresolved_pedidos', []))} | "
                f"fallidos={len(result.get('failed_pedidos', []))}."
            )
            st.session_state["admin_pxs_last_result"] = result
            _rerun()
        except Exception as exc:
            st.error(f"No fue posible ejecutar la importación PXS limpio: {exc}")

    last_result = st.session_state.get("admin_pxs_last_result")
    if last_result:
        st.markdown("---")
        st.write("**Último resultado de carga de pedidos nuevos**")
        _render_result_tables(last_result, result_kind="pxs")


# ---------------------------------------------------------
# UI - Entradas asistidas
# ---------------------------------------------------------
def _render_entradas_produccion_import() -> None:
    st.subheader("Cargar entradas de producción")
    with st.expander("Qué hace esta carga asistida", expanded=False):
        st.markdown(
            """
            - lee un archivo CSV/XLSX
            - permite elegir hoja y fila de encabezado
            - mapea columnas a un formato canónico
            - detecta cortes por **fecha + hora**
            - registra un **import_job**
            - registra los **cortes**
            - carga a **staging solo el corte seleccionado**
            """
        )

    uploaded_file = st.file_uploader(
        "Sube el archivo de entradas (CSV o Excel)",
        type=["csv", "xlsx", "xlsm", "xls"],
        key="admin_import_uploaded_file",
    )

    if not uploaded_file:
        st.info("Sube un archivo para continuar.")
        return

    file_name = uploaded_file.name
    file_bytes = _get_uploaded_bytes(uploaded_file)
    suffix = Path(file_name).suffix.lower()

    st.write(f"**Archivo:** {file_name}")
    st.write(f"**Tamaño:** {round(len(file_bytes) / 1024, 2)} KB")

    sheet_name: str | None = None
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        try:
            sheet_names = _get_excel_sheet_names(file_bytes)
        except Exception as exc:
            st.error(f"No fue posible leer las hojas del archivo Excel: {exc}")
            return

        sheet_name = st.selectbox(
            "Hoja del archivo",
            options=sheet_names,
            index=0,
            key="admin_import_sheet_name",
        )

    header_row = st.number_input(
        "Fila de encabezado (base 0)",
        min_value=0,
        max_value=20,
        value=0,
        step=1,
        key="admin_import_header_row",
    )

    try:
        raw_df = _read_uploaded_dataframe(
            file_name=file_name,
            file_bytes=file_bytes,
            sheet_name=sheet_name,
            header_row=int(header_row),
        )
    except Exception as exc:
        st.error(f"No fue posible leer el archivo: {exc}")
        return

    if raw_df is None or raw_df.empty:
        st.warning("El archivo no contiene filas útiles después de aplicar la configuración actual.")
        return

    raw_df = raw_df.copy()
    raw_df.columns = [str(col) for col in raw_df.columns]

    st.subheader("Vista previa cruda")
    st.dataframe(_build_preview_df(raw_df, max_rows=25), use_container_width=True, hide_index=True)

    mapping = _render_column_mapping(raw_df)
    normalized_df = _build_normalized_df(raw_df, mapping)

    st.markdown("---")
    _render_normalized_preview(normalized_df)

    normalized_rows = normalized_df.where(pd.notna(normalized_df), None).to_dict(orient="records")
    detected_cortes = detect_cortes_from_rows(normalized_rows)

    st.subheader("Cortes detectados")
    if not detected_cortes:
        st.warning(
            "No se detectaron entradas válidas. Revisa especialmente la columna de fecha y la columna de hora."
        )
        return

    st.dataframe(_build_detected_cortes_df(detected_cortes), use_container_width=True, hide_index=True)
    st.caption(
        "Cada corte se define por fecha + hora normalizada. Solo el corte seleccionado se carga a staging; "
        "después la decisión final se toma en Entradas de producción."
    )

    corte_options: list[str] = []
    corte_map: dict[str, int] = {}
    for idx, corte in enumerate(detected_cortes, start=1):
        label = _build_detected_corte_label(corte, idx)
        corte_options.append(label)
        corte_map[label] = idx - 1

    selected_corte_label = st.selectbox(
        "Entrada detectada a cargar",
        options=corte_options,
        index=0,
        key="admin_import_selected_corte_label",
    )
    selected_detected_index = corte_map[selected_corte_label]
    selected_detected_corte = detected_cortes[selected_detected_index]

    selected_rows_preview = selected_detected_corte.get("rows", [])
    selected_rows_df = pd.DataFrame(selected_rows_preview)
    if not selected_rows_df.empty:
        st.caption("Vista previa de la entrada que sí se cargará")
        st.dataframe(
            _build_preview_df(selected_rows_df, max_rows=20),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Confirmación de importación")
    save_copy = st.checkbox(
        "Guardar copia física del archivo en data/input",
        value=True,
        key="admin_import_save_copy",
    )

    col1, col2 = st.columns(2)
    usuario = col1.text_input("Usuario", value=_get_current_user() if _is_admin_session() else "", key="admin_import_usuario")
    notes = col2.text_input(
        "Notas",
        value="Importación asistida desde Admin Importaciones",
        key="admin_import_notes",
    )

    st.warning(
        "Esta acción registra la carga, detecta las entradas del archivo y carga **solo una entrada** para trabajarla después en la pantalla de Entradas. "
        "Si vuelves a cargar el mismo corte, el staging de ese corte se reemplaza."
    )

    is_admin = _is_admin_session()
    if not is_admin:
        st.caption("Desbloquea modo administrador desde la portada para ejecutar esta importación.")

    if st.button("Ejecutar carga de entrada", type="primary", key="admin_import_run", disabled=not is_admin):
        if not _is_admin_session():
            st.error("La sesión ya no está en modo administrador. Vuelve a desbloquearla antes de importar.")
            st.stop()
        try:
            import_job_id, corte_id, inserted_count = _run_assisted_import(
                import_type="ENTRADAS_PRODUCCION",
                original_file_name=file_name,
                file_bytes=file_bytes,
                normalized_df=normalized_df,
                selected_detected_index=selected_detected_index,
                usuario=usuario or None,
                notes=notes or None,
                save_copy=save_copy,
            )
            _set_flash(
                "Carga de entrada completada. "
                f"import_job_id={import_job_id}, corte_id={corte_id}, filas staging={inserted_count}."
            )
            _rerun()
        except Exception as exc:
            st.error(f"No fue posible completar la carga de entrada: {exc}")



def _render_import_center() -> None:
    st.subheader("¿Qué quieres cargar?")
    tabs = st.tabs([
        "Pedidos iniciales",
        "Pedidos nuevos (PXS)",
        "Entradas de producción",
    ])

    with tabs[0]:
        _render_carga_inicial_import()
    with tabs[1]:
        _render_pxs_limpio_import()
    with tabs[2]:
        _render_entradas_produccion_import()



def main() -> None:
    bootstrap_runtime()
    page_config = getattr(st, "set_page_config", None)
    if callable(page_config):
        try:
            page_config(page_title=PAGE_TITLE, layout="wide")
        except Exception:
            pass

    ensure_directories()

    if not db_exists():
        st.error(
            "La base de datos no existe. Inicialízala primero con:\n\n"
            "`python -m wms_mvp.db.init_db`"
        )
        st.stop()

    try:
        import_jobs = list_import_jobs(limit=150)
        cortes = list_cortes_disponibles(limit=250)
    except Exception as exc:
        st.error(f"No fue posible cargar el historial de importaciones: {exc}")
        st.stop()

    _render_header(import_jobs, cortes)
    _render_flash()
    _render_access_notice()

    st.markdown("---")
    _render_directories_info()

    st.markdown("---")
    _render_history(import_jobs, cortes)

    st.markdown("---")
    _render_import_center()


if __name__ == "__main__":
    main()
