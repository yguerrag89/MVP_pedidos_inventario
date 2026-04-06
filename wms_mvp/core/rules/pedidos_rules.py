from __future__ import annotations

from datetime import date, datetime, timedelta
import unicodedata
from typing import Any, Iterable, Mapping


VALID_ESTATUS_VENTA = {
    "En cobro",
    "Entregar",
    "Pausa",
}


VALID_MOTIVOS_PAUSA = {
    "espera de pago",
    "instrucción del cliente",
    "revisión comercial",
    "pendiente de confirmación",
    "otro",
}


VALID_ESTADOS_OPERATIVOS = {
    "ACTIVO",
    "ENVIADO_TOTAL",
    "ENVIADO_PARCIAL",
    "CANCELADO",
}


VALID_TIPOS_CIERRE = {
    "TOTAL",
    "PARCIAL",
}


_DATE_ALIASES_FECHA_CAPTURA = (
    "fecha_captura",
    "fecha captura",
    "fecha_alta",
    "fecha alta",
    "fecha",
)

_DATE_ALIASES_FECHA_PRODUCCION = (
    "fecha_produccion",
    "fecha producción",
    "fecha_produccion_compromiso",
    "fecha producción compromiso",
    "fecha_prod",
    "fecha prod",
    "fecha_fabricacion",
    "fecha fabricación",
    "compromiso_produccion",
    "compromiso producción",
)

_DATE_ALIASES_FECHA_LOGISTICA = (
    "fecha_logistica",
    "fecha logística",
    "fecha_entrega",
    "fecha entrega",
    "fecha_despacho",
    "fecha despacho",
)

_TEXT_ALIASES_ESTATUS_VENTA = (
    "estatus_venta",
    "estatus venta",
    "status_venta",
    "status venta",
)

_TEXT_ALIASES_MOTIVO_PAUSA = (
    "motivo_pausa",
    "motivo pausa",
    "motivo_de_pausa",
    "motivo de pausa",
    "razon_pausa",
    "razón pausa",
    "razon de pausa",
    "razón de pausa",
)

_TEXT_ALIASES_OBSERVACIONES = (
    "observaciones",
    "observacion",
    "comentarios",
    "notas",
)

_TEXT_ALIASES_CLIENTE = (
    "cliente",
    "razon_social",
    "razón social",
    "nombre_cliente",
    "nombre cliente",
)


_ESTATUS_VENTA_NORMALIZATION_MAP = {
    "entregar": "Entregar",
    "entrega": "Entregar",
    "en cobro": "En cobro",
    "encobro": "En cobro",
    "cobro": "En cobro",
    "en cobrar": "En cobro",
    "pausa": "Pausa",
    "en pausa": "Pausa",
    "pausado": "Pausa",
}

_MOTIVO_PAUSA_NORMALIZATION_MAP = {
    "espera de pago": "espera de pago",
    "esperando pago": "espera de pago",
    "pendiente de pago": "espera de pago",
    "instruccion del cliente": "instrucción del cliente",
    "instruccion cliente": "instrucción del cliente",
    "indicacion del cliente": "instrucción del cliente",
    "revision comercial": "revisión comercial",
    "rev comercial": "revisión comercial",
    "pendiente de confirmacion": "pendiente de confirmación",
    "por confirmar": "pendiente de confirmación",
    "otro": "otro",
    "otros": "otro",
}


# -----------------------------------------------------------------------------
# Helpers de normalización
# -----------------------------------------------------------------------------
def to_float(value: Any, default: float = 0.0) -> float:
    """
    Convierte un valor a float de forma segura.

    Reglas adicionales:
    - permite separadores de miles con coma
    - soporta porcentajes como "50%"
    - soporta negativos entre paréntesis: "(12.5)"
    - convierte NaN o infinitos al valor por defecto
    """
    if value is None:
        return default

    if isinstance(value, bool):
        return float(value)

    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric != numeric or numeric in (float("inf"), float("-inf")):
            return default
        return numeric

    text = str(value).strip()
    if text == "":
        return default

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1].strip()

    if text.endswith("%"):
        text = text[:-1].strip()

    text = text.replace(",", "")

    try:
        numeric = float(text)
    except (TypeError, ValueError):
        return default

    if numeric != numeric or numeric in (float("inf"), float("-inf")):
        return default

    return -numeric if negative else numeric



def normalize_text(value: Any) -> str | None:
    """
    Normaliza texto para comparaciones simples.
    """
    if value is None:
        return None

    text = str(value).strip()
    return text if text else None



def normalize_upper_text(value: Any) -> str | None:
    """
    Normaliza texto y lo convierte a mayúsculas.
    """
    text = normalize_text(value)
    return text.upper() if text else None



def normalize_key_text(value: Any) -> str | None:
    """
    Normaliza texto para búsquedas en catálogos tolerantes.

    - trim
    - lower
    - elimina acentos
    - colapsa espacios internos
    """
    text = normalize_text(value)
    if text is None:
        return None

    collapsed = " ".join(text.split())
    lowered = collapsed.lower()
    without_accents = "".join(
        ch for ch in unicodedata.normalize("NFKD", lowered) if not unicodedata.combining(ch)
    )
    return without_accents



def normalize_catalog_value(
    value: Any,
    canonical_map: Mapping[str, str],
    field_name: str,
    *,
    allow_empty: bool = True,
) -> str | None:
    """
    Normaliza un texto libre hacia un catálogo canónico.

    Si allow_empty es True, valores vacíos devuelven None.
    Si llega un valor no reconocido, levanta ValueError con mensaje claro.
    """
    text = normalize_text(value)
    if text is None:
        if allow_empty:
            return None
        raise ValueError(f"El campo '{field_name}' es obligatorio.")

    normalized_key = normalize_key_text(text)
    canonical = canonical_map.get(normalized_key or "")
    if canonical is None:
        allowed_values = sorted(set(canonical_map.values()))
        label_map = {
            "estatus_venta": "Estatus de venta",
            "motivo_pausa": "Motivo de pausa",
        }
        label = label_map.get(field_name, field_name.replace('_', ' ').capitalize())
        raise ValueError(
            f"{label} inválido: {value}. "
            f"Valores permitidos: {allowed_values}"
        )
    return canonical



def normalize_estatus_venta(value: Any) -> str | None:
    """
    Normaliza el estatus de venta al catálogo canónico del MVP.
    """
    return normalize_catalog_value(
        value,
        _ESTATUS_VENTA_NORMALIZATION_MAP,
        "estatus_venta",
        allow_empty=True,
    )



def normalize_motivo_pausa(value: Any) -> str | None:
    """
    Normaliza el motivo de pausa al catálogo canónico del MVP.
    """
    return normalize_catalog_value(
        value,
        _MOTIVO_PAUSA_NORMALIZATION_MAP,
        "motivo_pausa",
        allow_empty=True,
    )



def normalize_pause_fields(
    estatus_venta: Any,
    motivo_pausa: Any,
) -> tuple[str | None, str | None]:
    """
    Normaliza el par estatus/motivo respetando la regla del MVP:
    - si estatus == Pausa, el motivo es obligatorio
    - si estatus != Pausa, el motivo no debe persistirse
    """
    canonical_estatus = normalize_estatus_venta(estatus_venta)
    raw_motivo = normalize_text(motivo_pausa)

    if canonical_estatus == "Pausa":
        canonical_motivo = normalize_motivo_pausa(raw_motivo)
        if canonical_motivo is None:
            raise ValueError(
                "Si estatus_venta es 'Pausa', el campo 'motivo_pausa' es obligatorio. "
                f"Valores permitidos: {sorted(VALID_MOTIVOS_PAUSA)}"
            )
        return canonical_estatus, canonical_motivo

    return canonical_estatus, None



def _mapping_get_case_insensitive(row: Mapping[str, Any], key: str) -> Any:
    for existing_key, existing_value in row.items():
        if str(existing_key).strip().lower() == key.strip().lower():
            return existing_value
    return None



def pick_first_present(row: Mapping[str, Any], aliases: Iterable[str]) -> Any:
    """
    Devuelve el primer valor no vacío encontrado en una lista de aliases.
    Busca primero por clave exacta y luego de forma case-insensitive.
    """
    for alias in aliases:
        if alias in row:
            value = row.get(alias)
            if normalize_text(value) is not None or value is not None:
                return value

    for alias in aliases:
        value = _mapping_get_case_insensitive(row, alias)
        if normalize_text(value) is not None or value is not None:
            return value

    return None



def normalize_date(value: Any) -> str | None:
    """
    Convierte fechas comunes a formato ISO YYYY-MM-DD.

    Soporta:
    - date / datetime
    - strings ISO
    - strings dd/mm/yyyy o dd-mm-yyyy
    - seriales de Excel

    Si no puede interpretarse, devuelve None.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Seriales típicos de Excel.
        numeric = float(value)
        if 20_000 <= numeric <= 80_000:
            base = datetime(1899, 12, 30)
            dt = base + timedelta(days=numeric)
            return dt.date().isoformat()

    text = normalize_text(value)
    if text is None:
        return None

    cleaned = text.replace("T", " ")
    # Quitar hora si viene en formato ISO extendido.
    if " " in cleaned:
        maybe_date = cleaned.split(" ", 1)[0].strip()
    else:
        maybe_date = cleaned

    # ISO directo.
    try:
        return datetime.fromisoformat(maybe_date).date().isoformat()
    except ValueError:
        pass

    formats = (
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%Y-%m-%d",
        "%d/%m/%y",
        "%d-%m-%y",
    )
    for fmt in formats:
        try:
            return datetime.strptime(maybe_date, fmt).date().isoformat()
        except ValueError:
            continue

    return None



def require_valid_date(value: Any, field_name: str) -> str | None:
    """
    Valida una fecha opcional. Si el valor viene vacío, devuelve None.
    Si viene informado y no es interpretable, lanza error.
    """
    original = normalize_text(value)
    if original is None:
        return None

    normalized = normalize_date(original)
    if normalized is None:
        raise ValueError(f"El campo '{field_name}' contiene una fecha inválida: {value!r}.")
    return normalized



def build_id_pedido(serie_documento: Any, documento: Any) -> str:
    """
    Construye el ID de negocio del pedido a partir de serie + documento.
    """
    serie = normalize_text(serie_documento)
    doc = normalize_text(documento)

    if not serie or not doc:
        raise ValueError("No se puede construir id_pedido sin serie_documento y documento.")

    return f"{serie}-{doc}"


# -----------------------------------------------------------------------------
# Reglas matemáticas
# -----------------------------------------------------------------------------
def calculate_linea_faltante(cantidad_pedida: Any, cantidad_asignada: Any) -> float:
    """
    Calcula el faltante de una línea.
    Regla:
        faltante = max(0, cantidad_pedida - cantidad_asignada)
    """
    pedida = to_float(cantidad_pedida)
    asignada = to_float(cantidad_asignada)
    faltante = pedida - asignada
    return faltante if faltante > 0 else 0.0



def calculate_porcentaje_asignacion(
    total_pedido: Any,
    total_asignado: Any,
    decimals: int = 2,
) -> float:
    """
    Calcula el porcentaje de asignación de un pedido.
    """
    pedido = to_float(total_pedido)
    asignado = to_float(total_asignado)

    if pedido <= 0:
        return 0.0

    porcentaje = (asignado / pedido) * 100
    if porcentaje < 0:
        porcentaje = 0.0
    return round(porcentaje, decimals)



def aggregate_lineas_metrics(lineas: list[dict[str, Any]]) -> dict[str, float]:
    """
    Agrega métricas de una lista de líneas de pedido.
    """
    total_pedido = 0.0
    total_asignado = 0.0
    total_faltante = 0.0
    total_enviado = 0.0

    for linea in lineas:
        cantidad_pedida = to_float(linea.get("cantidad_pedida"))
        cantidad_asignada = to_float(linea.get("cantidad_asignada"))
        cantidad_enviada = to_float(linea.get("cantidad_enviada"))

        total_pedido += cantidad_pedida
        total_asignado += cantidad_asignada
        total_enviado += cantidad_enviada
        total_faltante += calculate_linea_faltante(cantidad_pedida, cantidad_asignada)

    porcentaje_asignacion = calculate_porcentaje_asignacion(total_pedido, total_asignado)

    return {
        "total_pedido": round(total_pedido, 6),
        "total_asignado": round(total_asignado, 6),
        "total_faltante": round(total_faltante, 6),
        "total_enviado": round(total_enviado, 6),
        "porcentaje_asignacion": porcentaje_asignacion,
    }


# -----------------------------------------------------------------------------
# Validaciones
# -----------------------------------------------------------------------------
def validate_non_negative_quantity(value: Any, field_name: str) -> None:
    """
    Valida que una cantidad no sea negativa.
    """
    numeric_value = to_float(value)
    if numeric_value < 0:
        raise ValueError(f"El campo '{field_name}' no puede ser negativo.")



def validate_linea_quantities(
    cantidad_pedida: Any,
    cantidad_asignada: Any,
    allow_overassignment: bool = False,
) -> None:
    """
    Valida cantidades de una línea de pedido.
    """
    pedida = to_float(cantidad_pedida)
    asignada = to_float(cantidad_asignada)

    validate_non_negative_quantity(pedida, "cantidad_pedida")
    validate_non_negative_quantity(asignada, "cantidad_asignada")

    if not allow_overassignment and asignada > pedida:
        raise ValueError(
            "La cantidad asignada no puede ser mayor que la cantidad pedida."
        )



def validate_estatus_venta(estatus_venta: Any) -> None:
    """
    Valida el estatus comercial del pedido.
    Permite valores vacíos, pero si viene un valor debe ser válido.
    """
    normalize_estatus_venta(estatus_venta)



def validate_motivo_pausa(
    estatus_venta: Any,
    motivo_pausa: Any,
) -> None:
    """
    Valida la coherencia entre estatus de venta y motivo de pausa.
    """
    normalize_pause_fields(estatus_venta, motivo_pausa)



def validate_pedido_header(
    id_pedido: Any,
    cliente: Any,
    estado_operativo: Any,
    estatus_venta: Any | None = None,
    fecha_captura: Any | None = None,
    fecha_produccion: Any | None = None,
    fecha_logistica: Any | None = None,
    motivo_pausa: Any | None = None,
) -> None:
    """
    Valida datos mínimos del encabezado del pedido.
    """
    normalized_id = normalize_text(id_pedido)
    normalized_cliente = normalize_text(cliente)
    normalized_estado = normalize_upper_text(estado_operativo)

    if not normalized_id:
        raise ValueError("El pedido debe tener un id_pedido válido.")

    if not normalized_cliente:
        raise ValueError("El pedido debe tener un cliente válido.")

    if normalized_estado not in VALID_ESTADOS_OPERATIVOS:
        raise ValueError(
            f"Estado operativo inválido: {estado_operativo}. "
            f"Valores permitidos: {sorted(VALID_ESTADOS_OPERATIVOS)}"
        )

    if estatus_venta is not None or motivo_pausa is not None:
        validate_motivo_pausa(estatus_venta, motivo_pausa)

    require_valid_date(fecha_captura, "fecha_captura")
    require_valid_date(fecha_produccion, "fecha_produccion")
    require_valid_date(fecha_logistica, "fecha_logistica")



def validate_tipo_cierre(tipo_cierre: Any) -> str:
    """
    Valida y devuelve el tipo de cierre normalizado.
    """
    normalized = normalize_upper_text(tipo_cierre)
    if normalized not in VALID_TIPOS_CIERRE:
        raise ValueError(
            f"Tipo de cierre inválido: {tipo_cierre}. "
            f"Valores permitidos: {sorted(VALID_TIPOS_CIERRE)}"
        )
    return normalized


# -----------------------------------------------------------------------------
# Estado del pedido
# -----------------------------------------------------------------------------
def pedido_tiene_lineas(lineas: list[dict[str, Any]]) -> bool:
    """
    Indica si el pedido tiene al menos una línea.
    """
    return len(lineas) > 0



def pedido_esta_completo_por_asignacion(lineas: list[dict[str, Any]]) -> bool:
    """
    Un pedido está completo por asignación si ninguna línea tiene faltante.
    """
    if not lineas:
        return False

    for linea in lineas:
        faltante = calculate_linea_faltante(
            linea.get("cantidad_pedida"),
            linea.get("cantidad_asignada"),
        )
        if faltante > 0:
            return False

    return True



def pedido_tiene_asignacion_parcial(lineas: list[dict[str, Any]]) -> bool:
    """
    Indica si el pedido tiene al menos algo asignado, pero no está completo.
    """
    if not lineas:
        return False

    total_asignado = 0.0
    total_pedido = 0.0

    for linea in lineas:
        total_pedido += to_float(linea.get("cantidad_pedida"))
        total_asignado += to_float(linea.get("cantidad_asignada"))

    return total_asignado > 0 and total_asignado < total_pedido



def can_close_pedido(
    pedido: dict[str, Any],
    lineas: list[dict[str, Any]],
    tipo_cierre: Any,
) -> tuple[bool, str | None]:
    """
    Evalúa si un pedido puede cerrarse operativamente.
    """
    if not pedido:
        return False, "No se recibió información del pedido."

    estado_operativo = normalize_upper_text(pedido.get("estado_operativo"))
    if estado_operativo != "ACTIVO":
        return False, "Solo se pueden cerrar pedidos con estado operativo ACTIVO."

    if not pedido_tiene_lineas(lineas):
        return False, "No se puede cerrar un pedido sin líneas."

    normalized_tipo_cierre = validate_tipo_cierre(tipo_cierre)

    if normalized_tipo_cierre == "TOTAL":
        if not pedido_esta_completo_por_asignacion(lineas):
            return False, (
                "No se puede cerrar como TOTAL porque aún existen líneas con faltante."
            )

    if normalized_tipo_cierre == "PARCIAL":
        total_asignado = aggregate_lineas_metrics(lineas)["total_asignado"]
        if total_asignado <= 0:
            return False, (
                "No se puede cerrar como PARCIAL porque el pedido no tiene producto asignado."
            )

    return True, None


# -----------------------------------------------------------------------------
# Cierre
# -----------------------------------------------------------------------------
def build_linea_cierre_snapshot(linea: dict[str, Any], tipo_cierre: Any) -> dict[str, Any]:
    """
    Construye el snapshot de cierre por línea.

    Regla:
    - cantidad_pedida_original = cantidad_pedida
    - cantidad_asignada_al_cierre = cantidad_asignada
    - cantidad_enviada:
        * TOTAL   -> min(cantidad_asignada, cantidad_pedida)
        * PARCIAL -> cantidad_asignada
    - cantidad_no_enviada = max(0, cantidad_pedida - cantidad_enviada)

    También arrastra campos descriptivos útiles para snapshot histórico.
    """
    normalized_tipo_cierre = validate_tipo_cierre(tipo_cierre)

    cantidad_pedida = to_float(linea.get("cantidad_pedida"))
    cantidad_asignada = to_float(linea.get("cantidad_asignada"))

    cantidad_enviada = cantidad_asignada
    if normalized_tipo_cierre == "TOTAL" and cantidad_asignada > cantidad_pedida:
        cantidad_enviada = cantidad_pedida

    cantidad_no_enviada = cantidad_pedida - cantidad_enviada
    if cantidad_no_enviada < 0:
        cantidad_no_enviada = 0.0

    return {
        "cantidad_pedida_original": round(cantidad_pedida, 6),
        "cantidad_asignada_al_cierre": round(cantidad_asignada, 6),
        "cantidad_enviada": round(cantidad_enviada, 6),
        "cantidad_no_enviada": round(cantidad_no_enviada, 6),
        "sku": normalize_text(linea.get("sku")),
        "descripcion": normalize_text(linea.get("descripcion")),
        "producto": normalize_text(linea.get("producto")),
        "color": normalize_text(linea.get("color")),
        "fecha_produccion": normalize_date(linea.get("fecha_produccion")),
        "fecha_logistica": normalize_date(linea.get("fecha_logistica")),
    }



def infer_estado_operativo_from_cierre(tipo_cierre: Any) -> str:
    """
    Traduce tipo de cierre a estado operativo destino.
    """
    normalized_tipo_cierre = validate_tipo_cierre(tipo_cierre)
    return "ENVIADO_TOTAL" if normalized_tipo_cierre == "TOTAL" else "ENVIADO_PARCIAL"


# -----------------------------------------------------------------------------
# Builders desde filas importadas
# -----------------------------------------------------------------------------
def build_pedido_from_import_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """
    Construye un diccionario base de pedido desde una fila importada.
    No toca base de datos; solo normaliza datos.

    Mejora respecto a la versión original:
    - incorpora fecha_produccion
    - permite pedido_origen_id para backorders
    - tolera aliases de columnas comunes de Excel
    - normaliza fechas a ISO cuando es posible
    - canoniza estatus_venta y motivo_pausa
    """
    serie_documento = normalize_text(
        pick_first_present(row, ("serie_documento", "serie documento", "serie"))
    )
    documento = normalize_text(pick_first_present(row, ("documento", "folio", "doc")))
    id_pedido = normalize_text(pick_first_present(row, ("id_pedido", "id pedido")))

    if not id_pedido:
        id_pedido = build_id_pedido(serie_documento, documento)

    estatus_venta, motivo_pausa = normalize_pause_fields(
        pick_first_present(row, _TEXT_ALIASES_ESTATUS_VENTA),
        pick_first_present(row, _TEXT_ALIASES_MOTIVO_PAUSA),
    )

    pedido = {
        "id_pedido": normalize_text(id_pedido),
        "serie_documento": serie_documento,
        "documento": documento,
        "tipo_registro": normalize_text(row.get("tipo_registro")) or "PEDIDO_NORMAL",
        "origen_registro": normalize_text(row.get("origen_registro")) or "OTRO",
        "import_job_id": row.get("import_job_id"),
        "cliente": normalize_text(pick_first_present(row, _TEXT_ALIASES_CLIENTE)),
        "fecha_captura": normalize_date(pick_first_present(row, _DATE_ALIASES_FECHA_CAPTURA)),
        "fecha_produccion": normalize_date(
            pick_first_present(row, _DATE_ALIASES_FECHA_PRODUCCION)
        ),
        "fecha_logistica": normalize_date(
            pick_first_present(row, _DATE_ALIASES_FECHA_LOGISTICA)
        ),
        "estatus_venta": estatus_venta,
        "motivo_pausa": motivo_pausa,
        "porcentaje_asignacion": to_float(row.get("porcentaje_asignacion"), 0.0),
        "estado_operativo": normalize_upper_text(row.get("estado_operativo")) or "ACTIVO",
        "pedido_origen_id": row.get("pedido_origen_id"),
        "observaciones": normalize_text(pick_first_present(row, _TEXT_ALIASES_OBSERVACIONES)),
    }

    validate_pedido_header(
        id_pedido=pedido["id_pedido"],
        cliente=pedido["cliente"],
        estado_operativo=pedido["estado_operativo"],
        estatus_venta=pedido.get("estatus_venta"),
        fecha_captura=pedido.get("fecha_captura"),
        fecha_produccion=pedido.get("fecha_produccion"),
        fecha_logistica=pedido.get("fecha_logistica"),
        motivo_pausa=pedido.get("motivo_pausa"),
    )
    return pedido



def build_linea_from_import_row(
    row: Mapping[str, Any],
    pedido_id: int,
    line_no: int | None = None,
) -> dict[str, Any]:
    """
    Construye un diccionario base de línea de pedido desde una fila importada.

    Incluye el contexto operativo mínimo requerido por las reglas:
    - fecha_captura
    - fecha_produccion
    - fecha_logistica
    - estatus_venta
    - motivo_pausa
    """
    cantidad_pedida = to_float(row.get("cantidad_pedida"), 0.0)
    cantidad_asignada = to_float(row.get("cantidad_asignada"), 0.0)
    cantidad_enviada = to_float(row.get("cantidad_enviada"), 0.0)
    cantidad_faltante = calculate_linea_faltante(cantidad_pedida, cantidad_asignada)

    validate_linea_quantities(
        cantidad_pedida=cantidad_pedida,
        cantidad_asignada=cantidad_asignada,
        allow_overassignment=False,
    )

    fecha_captura = normalize_date(pick_first_present(row, _DATE_ALIASES_FECHA_CAPTURA))
    fecha_produccion = normalize_date(pick_first_present(row, _DATE_ALIASES_FECHA_PRODUCCION))
    fecha_logistica = normalize_date(pick_first_present(row, _DATE_ALIASES_FECHA_LOGISTICA))

    require_valid_date(fecha_captura, "fecha_captura")
    require_valid_date(fecha_produccion, "fecha_produccion")
    require_valid_date(fecha_logistica, "fecha_logistica")

    estatus_venta, motivo_pausa = normalize_pause_fields(
        pick_first_present(row, _TEXT_ALIASES_ESTATUS_VENTA),
        pick_first_present(row, _TEXT_ALIASES_MOTIVO_PAUSA),
    )

    linea = {
        "pedido_id": pedido_id,
        "line_no": line_no if line_no is not None else row.get("line_no"),
        "sku": normalize_text(row.get("sku")),
        "descripcion": normalize_text(row.get("descripcion")),
        "producto": normalize_text(row.get("producto")),
        "color": normalize_text(row.get("color")),
        "cantidad_pedida": cantidad_pedida,
        "cantidad_asignada": cantidad_asignada,
        "cantidad_faltante": cantidad_faltante,
        "cantidad_enviada": cantidad_enviada,
        "fecha_captura": fecha_captura,
        "fecha_produccion": fecha_produccion,
        "fecha_logistica": fecha_logistica,
        "estatus_venta": estatus_venta,
        "motivo_pausa": motivo_pausa,
        "observaciones": normalize_text(pick_first_present(row, _TEXT_ALIASES_OBSERVACIONES)),
    }

    return linea



def enrich_linea_with_pedido_context(
    linea: Mapping[str, Any],
    pedido: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Devuelve una copia de la línea heredando el contexto operativo del pedido
    cuando la línea no lo trae explícito.
    """
    result = dict(linea)

    result["fecha_captura"] = normalize_date(
        result.get("fecha_captura") or pedido.get("fecha_captura")
    )
    result["fecha_produccion"] = normalize_date(
        result.get("fecha_produccion") or pedido.get("fecha_produccion")
    )
    result["fecha_logistica"] = normalize_date(
        result.get("fecha_logistica") or pedido.get("fecha_logistica")
    )
    result["estatus_venta"], result["motivo_pausa"] = normalize_pause_fields(
        result.get("estatus_venta") or pedido.get("estatus_venta"),
        result.get("motivo_pausa") or pedido.get("motivo_pausa"),
    )

    return result
