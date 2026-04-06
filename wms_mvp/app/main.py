from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# -------------------------------------------------------------------
# Ajuste de path para permitir:
# - streamlit run wms_mvp/app/main.py
# - streamlit run app/main.py
# -------------------------------------------------------------------
PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

import pandas as pd
import streamlit as st

from wms_mvp.app.bootstrap import bootstrap_runtime
from wms_mvp.core.config import (
    APP_NAME,
    APP_VERSION,
    DB_PATH,
    STREAMLIT_LAYOUT,
    STREAMLIT_PAGE_TITLE,
    ensure_directories,
)
from wms_mvp.core.services.auditoria_service import get_recent_audits
from wms_mvp.core.services.pedidos_service import (
    get_pedidos_activos_summary,
    get_pedidos_enviados_summary,
)
from wms_mvp.db.connection import db_exists, fetch_all, fetch_one
from wms_mvp.db.init_db import init_db


PAGE_TITLE = f"{APP_NAME} | Inicio"
SESSION_USER_KEY = "wms_current_user"
SESSION_ROLE_KEY = "wms_current_role"
SESSION_ADMIN_KEY = "wms_admin_unlocked"
SESSION_AUTH_NOTE_KEY = "wms_auth_note"

REQUIRED_TABLE_COLUMNS: dict[str, set[str]] = {
    "pedidos": {
        "id",
        "id_pedido",
        "cliente",
        "fecha_captura",
        "fecha_logistica",
        "estatus_venta",
        "porcentaje_asignacion",
        "estado_operativo",
    },
    "pedido_lineas": {
        "id",
        "pedido_id",
        "sku",
        "producto",
        "color",
        "descripcion",
        "cantidad_pedida",
        "cantidad_asignada",
        "cantidad_faltante",
    },
    "pedido_cierres": {
        "id",
        "pedido_id",
        "tipo_cierre",
        "usuario",
    },
    "pedido_linea_cierres": {
        "id",
        "pedido_cierre_id",
        "pedido_linea_id",
        "cantidad_pedida_original",
        "cantidad_enviada",
        "cantidad_no_enviada",
    },
    "cortes_entrada": {
        "id",
        "import_job_id",
        "fecha_corte",
        "hora_corte_normalizada",
        "estado_corte",
    },
    "entradas_staging": {
        "id",
        "sku",
        "cantidad",
        "fecha",
        "hora",
        "pedido_sugerido_texto",
        "estado_revision",
    },
    "entrada_decisiones": {
        "id",
        "entrada_staging_id",
        "decision_type",
        "cantidad",
        "created_by",
    },
    "inventario_sku": {
        "sku",
        "stock_libre",
        "stock_asignado",
    },
    "auditoria_cambios": {
        "id",
        "tabla",
        "campo",
        "usuario",
        "fecha_cambio",
    },
}

# Columnas que pueden variar entre iteraciones del MVP sin que la base esté realmente rota.
# La validación acepta cualquiera de estos nombres equivalentes y solo los reporta como nota.
OPTIONAL_COMPATIBILITY_ALIASES: dict[str, dict[str, set[str]]] = {
    "pedidos": {
        "fecha_produccion": {"fecha_produccion"},
    },
    "pedido_lineas": {
        "fecha_produccion": {"fecha_produccion"},
        "fecha_logistica": {"fecha_logistica"},
        "estatus_venta": {"estatus_venta"},
    },
    "pedido_cierres": {
        "fecha_cierre": {"fecha_cierre", "fecha_envio", "created_at"},
    },
    "pedido_linea_cierres": {
        "pedido_cierre_id": {"pedido_cierre_id", "cierre_id"},
        "sku": {"sku"},
        "descripcion": {"descripcion"},
        "fecha_produccion": {"fecha_produccion"},
        "fecha_logistica": {"fecha_logistica"},
        "estatus_venta": {"estatus_venta"},
    },
    "cortes_entrada": {
        "hora_corte": {"hora_corte", "hora_corte_texto", "hora_corte_normalizada"},
        "estado": {"estado", "estado_corte"},
        "usuario": {"usuario", "procesado_por"},
        "procesado_en": {"procesado_en", "procesado_at"},
        "archivo_nombre": {"archivo_nombre", "file_name"},
    },
    "entradas_staging": {
        "corte_entrada_id": {"corte_entrada_id", "corte_id"},
        "sugerencia_pedido": {"sugerencia_pedido", "pedido_sugerido_texto"},
        "texto_original_destino": {"texto_original_destino", "sugerencia_destino_texto"},
    },
    "entrada_decisiones": {
        "decision_tipo": {"decision_tipo", "decision_type"},
        "usuario": {"usuario", "created_by"},
    },
    "inventario_sku": {
        "entradas_recibidas": {"entradas_recibidas", "total_recibido"},
    },
    "asignaciones": {
        "tipo_asignacion": {"tipo_asignacion", "fuente_asignacion"},
        "usuario": {"usuario", "created_by"},
    },
    "auditoria_cambios": {
        "created_at": {"created_at", "fecha_cambio"},
    },
}


# =========================
# Utilidades generales
# =========================
def _set_page_config() -> None:
    try:
        st.set_page_config(
            page_title=STREAMLIT_PAGE_TITLE,
            layout=STREAMLIT_LAYOUT,
            initial_sidebar_state="expanded",
        )
    except Exception:
        # Evita romper la portada si Streamlit ya configuró la página.
        pass



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



def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return "N/D"
    return value.strftime("%Y-%m-%d %H:%M:%S")


# =========================
# Autenticación ligera
# =========================
def _init_session_auth() -> None:
    if SESSION_USER_KEY not in st.session_state:
        st.session_state[SESSION_USER_KEY] = "Consulta"
    if SESSION_ROLE_KEY not in st.session_state:
        st.session_state[SESSION_ROLE_KEY] = "viewer"
    if SESSION_ADMIN_KEY not in st.session_state:
        st.session_state[SESSION_ADMIN_KEY] = False
    if SESSION_AUTH_NOTE_KEY not in st.session_state:
        st.session_state[SESSION_AUTH_NOTE_KEY] = ""



def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()



def _get_admin_password_from_config() -> str:
    try:
        auth_section = st.secrets.get("auth", {})
        secret_pwd = auth_section.get("admin_password") or st.secrets.get("admin_password")
        if secret_pwd:
            return str(secret_pwd)
    except Exception:
        pass

    env_pwd = os.getenv("WMS_ADMIN_PASSWORD") or os.getenv("ADMIN_PASSWORD")
    if env_pwd:
        return env_pwd

    return "admin123"



def _users_table_exists() -> bool:
    if not db_exists():
        return False

    row = fetch_one(
        """
        SELECT COUNT(*) AS total
        FROM sqlite_master
        WHERE type = 'table' AND name = 'usuarios';
        """
    )
    return bool(row and int(row.get("total", 0) or 0) > 0)



def _validate_admin_from_users(username: str, password: str) -> bool:
    if not _users_table_exists():
        return False

    row = fetch_one(
        """
        SELECT username, display_name, password_hash, role, is_active
        FROM usuarios
        WHERE LOWER(username) = LOWER(?)
        LIMIT 1;
        """,
        (username,),
    )
    if not row:
        return False

    if int(row.get("is_active", 0) or 0) != 1:
        return False

    if _safe_text(row.get("role")).lower() != "admin":
        return False

    stored = _safe_text(row.get("password_hash"))
    if not stored:
        return False

    password_sha = _hash_password(password)
    if stored == password_sha or stored == password:
        st.session_state[SESSION_USER_KEY] = (
            _safe_text(row.get("display_name"))
            or _safe_text(row.get("username"))
            or "Administrador"
        )
        return True

    return False



def _unlock_admin(username: str, password: str) -> bool:
    username = _safe_text(username) or "admin"
    password = _safe_text(password)
    if not password:
        return False

    if _validate_admin_from_users(username=username, password=password):
        st.session_state[SESSION_ROLE_KEY] = "admin"
        st.session_state[SESSION_ADMIN_KEY] = True
        return True

    configured_password = _get_admin_password_from_config()
    if password == configured_password:
        st.session_state[SESSION_USER_KEY] = username
        st.session_state[SESSION_ROLE_KEY] = "admin"
        st.session_state[SESSION_ADMIN_KEY] = True
        return True

    return False



def _logout_admin() -> None:
    st.session_state[SESSION_USER_KEY] = "Consulta"
    st.session_state[SESSION_ROLE_KEY] = "viewer"
    st.session_state[SESSION_ADMIN_KEY] = False
    st.session_state[SESSION_AUTH_NOTE_KEY] = "Sesión administrativa cerrada."



def _render_auth_panel() -> None:
    with st.sidebar:
        st.markdown("---")
        st.subheader("Acceso")

        current_user = _safe_text(st.session_state.get(SESSION_USER_KEY)) or "Consulta"
        is_admin = bool(st.session_state.get(SESSION_ADMIN_KEY))

        st.write(f"**Modo actual:** {'Administrador' if is_admin else 'Consulta'}")
        st.write(f"**Usuario:** {current_user}")

        note = _safe_text(st.session_state.get(SESSION_AUTH_NOTE_KEY))
        if note:
            st.caption(note)
            st.session_state[SESSION_AUTH_NOTE_KEY] = ""

        if not is_admin:
            with st.expander("Desbloquear modo administrador", expanded=False):
                with st.form("auth_admin_form"):
                    username = st.text_input("Usuario", value="admin")
                    password = st.text_input("Contraseña", type="password")
                    submitted = st.form_submit_button("Entrar como administrador", type="primary")

                if submitted:
                    if _unlock_admin(username=username, password=password):
                        st.session_state[SESSION_AUTH_NOTE_KEY] = "Modo administrador activado."
                        _rerun()
                    else:
                        st.error("Usuario o contraseña incorrectos.")

            fallback = _get_admin_password_from_config()
            if fallback == "admin123":
                st.warning(
                    "La contraseña administrativa sigue con el valor temporal `admin123`. "
                    "Cámbiala en `secrets.toml` o en `WMS_ADMIN_PASSWORD`."
                )
        else:
            st.success("El modo administrador está habilitado en esta sesión.")
            if st.button("Cerrar sesión administrativa"):
                _logout_admin()
                _rerun()



def _get_permission_matrix() -> pd.DataFrame:
    rows = [
        {"Módulo": "Inicio", "Consulta": "Sí", "Admin": "Sí", "Acciones críticas": "No"},
        {"Módulo": "Pedidos activos", "Consulta": "Sí", "Admin": "Sí", "Acciones críticas": "Cierre / edición"},
        {"Módulo": "Pedidos enviados", "Consulta": "Sí", "Admin": "Sí", "Acciones críticas": "Venta directa"},
        {"Módulo": "Entradas producción", "Consulta": "Sí", "Admin": "Sí", "Acciones críticas": "Aplicar decisiones"},
        {"Módulo": "Admin importaciones", "Consulta": "No", "Admin": "Sí", "Acciones críticas": "Importación"},
    ]
    return pd.DataFrame(rows)


# =========================
# Estado de BD / KPIs
# =========================
def _get_db_file_info() -> dict[str, Any]:
    if not DB_PATH.exists():
        return {
            "exists": False,
            "path": str(DB_PATH),
            "size_mb": 0.0,
            "modified_at": None,
        }

    stat = DB_PATH.stat()
    modified_at = datetime.fromtimestamp(stat.st_mtime)
    return {
        "exists": True,
        "path": str(DB_PATH),
        "size_mb": round(stat.st_size / (1024 * 1024), 3),
        "modified_at": modified_at,
    }



def _get_table_names() -> list[str]:
    if not db_exists():
        return []

    rows = fetch_all(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
        """
    )
    return [row["name"] for row in rows]



def _get_table_columns(table_name: str) -> set[str]:
    if not db_exists():
        return set()

    try:
        rows = fetch_all(f"PRAGMA table_info({table_name});")
    except Exception:
        return set()

    return {_safe_text(row.get("name")) for row in rows if row.get("name") is not None}



def _get_schema_health() -> dict[str, Any]:
    if not db_exists():
        return {
            "ok": False,
            "missing_tables": sorted(REQUIRED_TABLE_COLUMNS.keys()),
            "issues": ["La base todavía no existe."],
            "compatibility_notes": [],
        }

    existing_tables = set(_get_table_names())
    missing_tables = [name for name in REQUIRED_TABLE_COLUMNS if name not in existing_tables]
    issues: list[str] = []
    compatibility_notes: list[str] = []

    for table_name, required_columns in REQUIRED_TABLE_COLUMNS.items():
        if table_name in missing_tables:
            continue

        present = _get_table_columns(table_name)
        missing_columns = sorted(col for col in required_columns if col not in present)
        if missing_columns:
            issues.append(f"{table_name}: faltan columnas -> {', '.join(missing_columns)}")

        alias_map = OPTIONAL_COMPATIBILITY_ALIASES.get(table_name, {})
        for canonical_name, aliases in alias_map.items():
            present_aliases = sorted(alias for alias in aliases if alias in present)
            if present_aliases and canonical_name not in present:
                compatibility_notes.append(
                    f"{table_name}: se usa '{present_aliases[0]}' como equivalente de '{canonical_name}'."
                )

    return {
        "ok": not missing_tables and not issues,
        "missing_tables": missing_tables,
        "issues": issues,
        "compatibility_notes": compatibility_notes,
    }



def _query_scalar(query: str, params: tuple[Any, ...] = ()) -> int | float:
    if not db_exists():
        return 0

    row = fetch_one(query, params)
    if not row:
        return 0

    first_value = next(iter(row.values()))
    if first_value is None:
        return 0
    return first_value



def _get_operational_kpis() -> dict[str, Any]:
    if not db_exists():
        return {
            "pedidos_activos": 0,
            "pedidos_enviados": 0,
            "promedio_asignacion_activos": 0.0,
            "lineas_con_faltante": 0,
            "skus_inventario": 0,
            "skus_con_stock_libre": 0,
            "import_jobs": 0,
            "cortes_entrada": 0,
            "entradas_en_revision": 0,
            "auditorias": 0,
        }

    promedio_asignacion = _query_scalar(
        """
        SELECT COALESCE(AVG(porcentaje_asignacion), 0) AS total
        FROM pedidos
        WHERE estado_operativo = 'ACTIVO';
        """
    )

    return {
        "pedidos_activos": int(_query_scalar("SELECT COUNT(*) AS total FROM pedidos WHERE estado_operativo = 'ACTIVO';")),
        "pedidos_enviados": int(
            _query_scalar(
                """
                SELECT COUNT(*) AS total
                FROM pedidos
                WHERE estado_operativo IN ('ENVIADO_TOTAL', 'ENVIADO_PARCIAL');
                """
            )
        ),
        "promedio_asignacion_activos": round(_safe_float(promedio_asignacion), 2),
        "lineas_con_faltante": int(
            _query_scalar(
                """
                SELECT COUNT(*) AS total
                FROM pedido_lineas pl
                INNER JOIN pedidos p ON p.id = pl.pedido_id
                WHERE p.estado_operativo = 'ACTIVO'
                  AND COALESCE(pl.cantidad_faltante, 0) > 0;
                """
            )
        ),
        "skus_inventario": int(_query_scalar("SELECT COUNT(*) AS total FROM inventario_sku;")),
        "skus_con_stock_libre": int(
            _query_scalar(
                """
                SELECT COUNT(*) AS total
                FROM inventario_sku
                WHERE COALESCE(stock_libre, 0) > 0;
                """
            )
        ),
        "import_jobs": int(_query_scalar("SELECT COUNT(*) AS total FROM import_jobs;")),
        "cortes_entrada": int(_query_scalar("SELECT COUNT(*) AS total FROM cortes_entrada;")),
        "entradas_en_revision": int(
            _query_scalar(
                """
                SELECT COUNT(*) AS total
                FROM entradas_staging
                WHERE estado_revision IN ('PENDIENTE', 'EN_REVISION');
                """
            )
        ),
        "auditorias": int(_query_scalar("SELECT COUNT(*) AS total FROM auditoria_cambios;")),
    }



def _get_module_status() -> pd.DataFrame:
    package_root = Path(__file__).resolve().parents[1]

    targets = [
        ("UI", "Home", package_root / "app" / "main.py"),
        ("UI", "Pedidos activos", package_root / "app" / "pages" / "01_Pedidos_Activos.py"),
        ("UI", "Inventario", package_root / "app" / "pages" / "02_Inventario.py"),
        ("UI", "Entradas producción", package_root / "app" / "pages" / "03_Entradas_Produccion.py"),
        ("UI", "Reservas", package_root / "app" / "pages" / "04_Reservas.py"),
        ("UI", "Pedidos enviados", package_root / "app" / "pages" / "05_Pedidos_Enviados.py"),
        ("UI", "Admin importaciones", package_root / "app" / "pages" / "06_Admin_Importaciones.py"),
        ("ETL", "Utilidades Excel", package_root / "etl" / "excel_utils.py"),
        ("ETL", "Loader carga inicial", package_root / "etl" / "pedidos_loader.py"),
        ("ETL", "Loader PXS limpio", package_root / "etl" / "pxs_loader.py"),
        ("ETL", "Loader entradas", package_root / "etl" / "entradas_loader.py"),
        ("Servicio", "Pedidos", package_root / "core" / "services" / "pedidos_service.py"),
        ("Servicio", "Enviados", package_root / "core" / "services" / "enviados_service.py"),
        ("Servicio", "Entradas", package_root / "core" / "services" / "entradas_service.py"),
        ("Servicio", "Asignación", package_root / "core" / "services" / "asignacion_service.py"),
        ("Servicio", "Auditoría", package_root / "core" / "services" / "auditoria_service.py"),
    ]

    rows: list[dict[str, Any]] = []
    for categoria, modulo, file_path in targets:
        exists = file_path.exists()
        size = file_path.stat().st_size if exists else 0

        if not exists:
            estado = "NO EXISTE"
        elif size == 0:
            estado = "PENDIENTE"
        elif size < 1000:
            estado = "BÁSICO"
        else:
            estado = "IMPLEMENTADO"

        rows.append(
            {
                "Categoría": categoria,
                "Módulo": modulo,
                "Archivo": str(file_path.relative_to(package_root)),
                "Estado": estado,
                "Tamaño (bytes)": size,
            }
        )

    return pd.DataFrame(rows)



def _get_recent_audit_df(limit: int = 15) -> pd.DataFrame:
    if not db_exists():
        return pd.DataFrame()

    try:
        rows = get_recent_audits(limit=limit)
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    data = [
        {
            "Fecha": _safe_text(row.get("fecha_cambio")),
            "Tabla": _safe_text(row.get("tabla")),
            "Registro": _safe_text(row.get("registro_id")),
            "Campo": _safe_text(row.get("campo")),
            "Antes": _safe_text(row.get("valor_anterior")),
            "Después": _safe_text(row.get("valor_nuevo")),
            "Usuario": _safe_text(row.get("usuario")),
        }
        for row in rows
    ]

    return pd.DataFrame(data)


# =========================
# Render UI
# =========================
def _render_sidebar(db_info: dict[str, Any], kpis: dict[str, Any]) -> None:
    with st.sidebar:
        st.header("Navegación")
        st.caption("Usa esta portada como panel general para revisar pedidos, entradas y cargas.")

        if hasattr(st, "page_link"):
            st.page_link("pages/01_Pedidos_Activos.py", label="Pedidos activos", icon="📋")
            st.page_link("pages/02_Inventario.py", label="Inventario", icon="📦")
            st.page_link("pages/03_Entradas_Produccion.py", label="Entradas y asignación", icon="🏭")
            st.page_link("pages/04_Reservas.py", label="Reservas", icon="🔀")
            st.page_link("pages/05_Pedidos_Enviados.py", label="Pedidos enviados", icon="🚚")
            st.page_link("pages/06_Admin_Importaciones.py", label="Cargas y actualizaciones", icon="⚙️")
            st.page_link("pages/07_Bootstrap_Inicial.py", label="Bootstrap inicial", icon="🧱")
        else:
            st.info("Usa la barra lateral de Streamlit para cambiar de página.")

        st.markdown("---")
        st.subheader("Base de datos")
        st.write(f"**Ruta:** `{db_info['path']}`")
        st.write(f"**Existe:** {'Sí' if db_info['exists'] else 'No'}")
        st.write(f"**Tamaño:** {db_info['size_mb']} MB")
        st.write(f"**Actualizada:** {_format_datetime(db_info['modified_at'])}")

        st.markdown("---")
        st.subheader("Resumen rápido")
        st.write(f"**Pedidos activos:** {kpis['pedidos_activos']}")
        st.write(f"**Pedidos enviados:** {kpis['pedidos_enviados']}")
        st.write(f"**Entradas por resolver:** {kpis['entradas_en_revision']}")
        st.write(f"**SKU disponibles para repartir:** {kpis['skus_con_stock_libre']}")



def _render_header() -> None:
    st.title(PAGE_TITLE)
    st.caption(
        "Portada operativa del sistema. Desde aquí puedes revisar el estado general, entrar a cada módulo "
        "y confirmar rápidamente si la base y los flujos principales están listos para trabajar."
    )



def _render_db_status(db_info: dict[str, Any], table_names: list[str], schema_health: dict[str, Any]) -> None:
    st.subheader("Estado del sistema")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Base SQLite", "Disponible" if db_info["exists"] else "No creada")
    col2.metric("Tablas detectadas", len(table_names))
    col3.metric("Tamaño archivo", f"{db_info['size_mb']} MB")
    col4.metric("Versión app", APP_VERSION)

    if not db_info["exists"]:
        st.warning(
            "La base de datos aún no existe. Puedes inicializarla desde aquí o hacerlo por consola con:\n\n"
            "`python -m wms_mvp.db.init_db`"
        )

        if st.button("Inicializar base de datos", type="primary"):
            try:
                init_db()
                st.success("Base de datos inicializada correctamente.")
                _rerun()
            except Exception as exc:
                st.error(f"No fue posible inicializar la base de datos: {exc}")
        return

    if schema_health["ok"]:
        st.success("La base existe y cumple la validación mínima del esquema del MVP.")
    else:
        if schema_health["missing_tables"]:
            st.error(
                "Faltan tablas requeridas: " + ", ".join(schema_health["missing_tables"])
            )
        if schema_health["issues"]:
            st.warning("Se detectaron diferencias entre la base y el esquema esperado del MVP.")
            with st.expander("Ver diferencias detectadas", expanded=False):
                for issue in schema_health["issues"]:
                    st.write(f"- {issue}")

    with st.expander("Ver tablas detectadas", expanded=False):
        if table_names:
            st.dataframe(pd.DataFrame({"Tabla": table_names}), use_container_width=True, hide_index=True)
        else:
            st.info("No se detectaron tablas en la base.")



def _render_kpis(kpis: dict[str, Any]) -> None:
    st.subheader("Indicadores principales")

    row1 = st.columns(5)
    row1[0].metric("Pedidos activos", kpis["pedidos_activos"])
    row1[1].metric("Pedidos enviados", kpis["pedidos_enviados"])
    row1[2].metric("% prom. asignación activos", f"{kpis['promedio_asignacion_activos']:.2f}%")
    row1[3].metric("Líneas con faltante", kpis["lineas_con_faltante"])
    row1[4].metric("Entradas por resolver", kpis["entradas_en_revision"])

    row2 = st.columns(5)
    row2[0].metric("SKUs inventario", kpis["skus_inventario"])
    row2[1].metric("SKU disponibles", kpis["skus_con_stock_libre"])
    row2[2].metric("Cargas registradas", kpis["import_jobs"])
    row2[3].metric("Entradas detectadas", kpis["cortes_entrada"])
    row2[4].metric("Auditorías registradas", kpis["auditorias"])



def _render_operational_snapshot() -> None:
    st.subheader("Resumen operativo de pedidos")

    if not db_exists():
        st.info("Aún no hay base de datos disponible para mostrar pedidos.")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Pedidos activos**")
        try:
            activos = get_pedidos_activos_summary()
            if not activos:
                st.info("No hay pedidos activos.")
            else:
                active_df = pd.DataFrame(
                    [
                        {
                            "ID_pedido": _safe_text(row.get("id_pedido")),
                            "Cliente": _safe_text(row.get("cliente")),
                            "%": round(_safe_float(row.get("porcentaje_asignacion")), 2),
                            "Fecha captura": _safe_text(row.get("fecha_captura")),
                            "Fecha producción": _safe_text(row.get("fecha_produccion")),
                            "Fecha logística": _safe_text(row.get("fecha_logistica")),
                            "Estatus venta": _safe_text(row.get("estatus_venta")),
                        }
                        for row in activos[:10]
                    ]
                )
                st.dataframe(active_df, use_container_width=True, hide_index=True)
        except Exception as exc:
            st.error(f"No fue posible cargar pedidos activos: {exc}")

    with col2:
        st.markdown("**Pedidos enviados**")
        try:
            enviados = get_pedidos_enviados_summary()
            if not enviados:
                st.info("No hay pedidos enviados.")
            else:
                sent_df = pd.DataFrame(
                    [
                        {
                            "ID_pedido": _safe_text(row.get("id_pedido")),
                            "Cliente": _safe_text(row.get("cliente")),
                            "%": round(_safe_float(row.get("porcentaje_asignacion")), 2),
                            "Fecha producción": _safe_text(row.get("fecha_produccion")),
                            "Tipo cierre": _safe_text(row.get("tipo_cierre")),
                            "Fecha envío": _safe_text(row.get("fecha_envio")),
                            "Cerrado por": _safe_text(row.get("cerrado_por")),
                        }
                        for row in enviados[:10]
                    ]
                )
                st.dataframe(sent_df, use_container_width=True, hide_index=True)
        except Exception as exc:
            st.error(f"No fue posible cargar pedidos enviados: {exc}")



def _render_navigation_help() -> None:
    st.subheader("Cómo navegar en este MVP")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            """
            **Flujo recomendado**
            1. Revisa que la base SQLite exista y que el esquema esté sano.
            2. Entra a **Pedidos activos** para el trabajo diario.
            3. Usa **Pedidos enviados** para histórico y ventas directas.
            4. Continúa con **Entradas producción** y **Admin importaciones**.
            """
        )

    with col2:
        st.markdown(
            """
            **Principio operativo**
            - El sistema sugiere.
            - El usuario decide.
            - La base registra.

            **Importante**
            - Esta portada no modifica pedidos ni inventario.
            - Sirve como tablero general y punto de entrada.
            """
        )



def _render_access_model() -> None:
    st.subheader("Modelo de acceso actual")

    left, right = st.columns([1.4, 1])
    with left:
        st.dataframe(_get_permission_matrix(), use_container_width=True, hide_index=True)
    with right:
        current_role = _safe_text(st.session_state.get(SESSION_ROLE_KEY)) or "viewer"
        current_user = _safe_text(st.session_state.get(SESSION_USER_KEY)) or "Consulta"
        is_admin = bool(st.session_state.get(SESSION_ADMIN_KEY))

        st.metric("Rol activo", "Administrador" if is_admin else "Consulta")
        st.write(f"**Usuario actual:** {current_user}")
        st.write(f"**Clave interna del rol:** `{current_role}`")
        st.caption(
            "La portada centraliza el desbloqueo de administrador para que las páginas operativas respeten la sesión."
        )



def _render_module_status() -> None:
    st.subheader("Estado del desarrollo")
    st.dataframe(_get_module_status(), use_container_width=True, hide_index=True)



def _render_recent_audits() -> None:
    st.subheader("Auditoría reciente")
    audit_df = _get_recent_audit_df(limit=15)

    if audit_df.empty:
        st.info("Aún no hay cambios auditados para mostrar.")
        return

    st.dataframe(audit_df, use_container_width=True, hide_index=True)



def main() -> None:
    _set_page_config()
    bootstrap_note = bootstrap_runtime()
    ensure_directories()
    _init_session_auth()

    db_info = _get_db_file_info()
    table_names = _get_table_names()
    schema_health = _get_schema_health()
    kpis = _get_operational_kpis()

    _render_sidebar(db_info, kpis)
    _render_auth_panel()
    _render_header()
    if bootstrap_note:
        st.warning(bootstrap_note)
    _render_db_status(db_info, table_names, schema_health)

    st.markdown("---")
    _render_kpis(kpis)

    st.markdown("---")
    _render_access_model()

    st.markdown("---")
    _render_navigation_help()

    st.markdown("---")
    _render_operational_snapshot()

    st.markdown("---")
    _render_module_status()

    st.markdown("---")
    _render_recent_audits()


if __name__ == "__main__":
    main()
