from __future__ import annotations

import math
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from wms_mvp.core.rules.pedidos_rules import normalize_text, to_float


DEFAULT_HEADER_SEARCH_ROWS = 15
DEFAULT_HEADER_MIN_MATCHES = 2
EXCEL_EPOCH = datetime(1899, 12, 30)

SKU_PATTERN = re.compile(r"Producto:\s*([0-9]{6,})", re.IGNORECASE)
LONG_INTEGER_PATTERN = re.compile(r"\b([0-9]{6,})\b")
UNNAMED_PATTERN = re.compile(r"^Unnamed(?::\s*\d+)?$", re.IGNORECASE)
PRODUCTO_PREFIX_PATTERN = re.compile(r"^\s*Producto:\s*[0-9]{6,}\s*/\s*", re.IGNORECASE)
EXPLICIT_PRODUCT_COLOR_SPLIT_PATTERN = re.compile(r"\s+/\s+")

DEFAULT_CODIGOS_SHEET_CANDIDATES = [
    "codigos",
    "Tabla de codigos",
    "Codigo ",
    "Codigo",
]


# -----------------------------------------------------------------------------
# Helpers básicos
# -----------------------------------------------------------------------------
def ensure_path(file_path: str | Path) -> Path:
    """
    Normaliza una ruta a Path y valida su existencia.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")
    return path



def normalize_column_name(value: Any) -> str:
    """
    Normaliza nombres de columnas ignorando mayúsculas, espacios y símbolos.
    """
    text = normalize_text(value) or ""
    text = re.sub(r"\s+", " ", text)
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text



def is_missing_like(value: Any) -> bool:
    """
    Identifica valores equivalentes a vacío en ETL tabular.
    """
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    text = normalize_text(value)
    return text is None



def safe_str(value: Any) -> str:
    """
    Convierte a texto sin devolver None.
    """
    text = normalize_text(value)
    return text or ""



def trim_excel_number_text(value: Any) -> str | None:
    """
    Convierte un valor a texto preservando mejor los enteros exportados por Excel.

    Ejemplo:
        1103001319010101.0 -> "1103001319010101"
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return str(int(value))

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
        return format(value, ".15g")

    text = normalize_text(value)
    if not text:
        return None

    if text.endswith(".0"):
        numeric_part = text[:-2]
        if numeric_part.isdigit():
            return numeric_part

    return text


# -----------------------------------------------------------------------------
# Lectura de archivos
# -----------------------------------------------------------------------------
def read_excel_sheet_names(file_path: str | Path) -> list[str]:
    """
    Devuelve la lista de hojas de un archivo Excel.
    """
    path = ensure_path(file_path)
    with pd.ExcelFile(path) as xls:
        return list(xls.sheet_names)



def read_tabular_file(
    file_path: str | Path,
    sheet_name: str | int | None = 0,
    header: int | None = 0,
    dtype: Any = object,
) -> pd.DataFrame:
    """
    Lee un archivo tabular CSV/XLSX como DataFrame.
    """
    path = ensure_path(file_path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path, header=header, dtype=dtype)

    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name, header=header, dtype=dtype)

    raise ValueError(f"Formato no soportado: {suffix}")



def read_raw_sheet(
    file_path: str | Path,
    sheet_name: str | int | None = 0,
    max_rows: int | None = None,
) -> pd.DataFrame:
    """
    Lee una hoja sin encabezado para detectar la fila header manualmente.
    """
    df = read_tabular_file(file_path, sheet_name=sheet_name, header=None, dtype=object)
    if max_rows is not None:
        return df.head(max_rows).copy()
    return df


# -----------------------------------------------------------------------------
# Detección y limpieza de encabezados
# -----------------------------------------------------------------------------
def row_to_normalized_tokens(row: Iterable[Any]) -> list[str]:
    """
    Convierte una fila a tokens normalizados no vacíos.
    """
    tokens: list[str] = []
    for value in row:
        token = normalize_column_name(value)
        if token:
            tokens.append(token)
    return tokens



def detect_header_row(
    df_raw: pd.DataFrame,
    markers: Iterable[str],
    search_rows: int = DEFAULT_HEADER_SEARCH_ROWS,
    min_matches: int = DEFAULT_HEADER_MIN_MATCHES,
) -> int:
    """
    Busca la fila más probable de encabezados usando marcadores.
    """
    normalized_markers = {
        normalize_column_name(marker)
        for marker in markers
        if normalize_column_name(marker)
    }
    if not normalized_markers:
        raise ValueError("Debe existir al menos un marcador para detectar encabezados.")

    best_row = 0
    best_score = -1

    limit = min(len(df_raw), max(search_rows, 1))
    for row_idx in range(limit):
        row_tokens = set(row_to_normalized_tokens(df_raw.iloc[row_idx].tolist()))
        score = len(normalized_markers.intersection(row_tokens))
        if score > best_score:
            best_row = row_idx
            best_score = score

    if best_score < min_matches:
        raise ValueError(
            "No se pudo detectar una fila de encabezado con suficiente confianza. "
            f"Mejor score={best_score}, mínimo requerido={min_matches}."
        )

    return best_row



def dataframe_from_detected_header(
    df_raw: pd.DataFrame,
    header_row: int,
    drop_empty_rows: bool = True,
    drop_empty_cols: bool = True,
) -> pd.DataFrame:
    """
    Reconstruye un DataFrame usando una fila detectada como header.
    """
    if header_row < 0 or header_row >= len(df_raw):
        raise ValueError(f"header_row fuera de rango: {header_row}")

    headers = [safe_str(value) for value in df_raw.iloc[header_row].tolist()]
    df = df_raw.iloc[header_row + 1 :].copy()
    df.columns = make_unique_headers(headers)
    df = df.reset_index(drop=True)

    if drop_empty_cols:
        df = drop_fully_empty_columns(df)
        df = drop_fully_unnamed_columns(df)

    if drop_empty_rows:
        df = drop_fully_empty_rows(df)

    return df



def make_unique_headers(headers: Iterable[Any]) -> list[str]:
    """
    Hace únicos los nombres de encabezado conservando la mayor legibilidad posible.
    """
    seen: dict[str, int] = {}
    result: list[str] = []

    for idx, raw_header in enumerate(headers, start=1):
        header = safe_str(raw_header) or f"Unnamed: {idx - 1}"
        count = seen.get(header, 0)
        if count == 0:
            result.append(header)
        else:
            result.append(f"{header}__{count}")
        seen[header] = count + 1

    return result



def drop_fully_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina columnas completamente vacías.
    """
    keep_columns = [
        col
        for col in df.columns
        if not all(is_missing_like(value) for value in df[col].tolist())
    ]
    return df.loc[:, keep_columns].copy()



def drop_fully_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas completamente vacías.
    """
    keep_mask = [
        not all(is_missing_like(value) for value in row)
        for row in df.to_numpy().tolist()
    ]
    return df.loc[keep_mask].reset_index(drop=True)



def drop_fully_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina columnas tipo Unnamed siempre que además estén vacías.
    """
    keep_columns: list[str] = []

    for col in df.columns:
        col_name = str(col)
        is_unnamed = bool(UNNAMED_PATTERN.match(col_name)) or normalize_column_name(
            col_name
        ).startswith("unnamed")

        if not is_unnamed:
            keep_columns.append(col)
            continue

        values = df[col].tolist()
        if any(not is_missing_like(value) for value in values):
            keep_columns.append(col)

    return df.loc[:, keep_columns].copy()


# -----------------------------------------------------------------------------
# Renombrado canónico de columnas
# -----------------------------------------------------------------------------
def rename_columns_by_aliases(
    df: pd.DataFrame,
    aliases_map: dict[str, Iterable[str]],
    keep_unmapped: bool = True,
) -> pd.DataFrame:
    """
    Renombra columnas usando alias canónicos.

    Ejemplo:
        aliases_map = {
            "fecha": ["FECHA", "Fecha entrada"],
            "sku": ["CLAVE", "SKU"],
        }
    """
    normalized_current = {normalize_column_name(col): str(col) for col in df.columns}
    rename_map: dict[str, str] = {}

    for canonical_name, aliases in aliases_map.items():
        if canonical_name in df.columns:
            continue

        candidates = [canonical_name, *list(aliases)]
        for candidate in candidates:
            normalized_candidate = normalize_column_name(candidate)
            matched_column = normalized_current.get(normalized_candidate)
            if matched_column and matched_column not in rename_map:
                rename_map[matched_column] = canonical_name
                break

    renamed = df.rename(columns=rename_map).copy()

    if keep_unmapped:
        return renamed

    wanted = [canonical for canonical in aliases_map if canonical in renamed.columns]
    return renamed.loc[:, wanted].copy()



def ensure_required_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> None:
    """
    Valida que existan columnas obligatorias.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {', '.join(missing)}")


# -----------------------------------------------------------------------------
# Conversión de fechas y horas
# -----------------------------------------------------------------------------
def parse_excel_date(value: Any, dayfirst: bool = True) -> str | None:
    """
    Convierte distintos formatos de fecha a YYYY-MM-DD.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and math.isnan(value):
            return None
        if value > 59:
            try:
                return (EXCEL_EPOCH + timedelta(days=float(value))).date().isoformat()
            except OverflowError:
                return None

    text = trim_excel_number_text(value)
    if not text:
        return None

    for fmt in (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%d-%m-%y",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue

    parsed = pd.to_datetime(text, errors="coerce", dayfirst=dayfirst)
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()



def parse_excel_time(value: Any) -> str | None:
    """
    Convierte distintos formatos de hora a HH:MM.

    Soporta fracciones de día de Excel, objetos time/datetime y texto.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.strftime("%H:%M")

    if isinstance(value, time):
        return value.strftime("%H:%M")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and math.isnan(value):
            return None
        if 0 <= float(value) < 1:
            total_seconds = int(round(float(value) * 24 * 60 * 60))
            hours = (total_seconds // 3600) % 24
            minutes = (total_seconds % 3600) // 60
            return f"{hours:02d}:{minutes:02d}"

        text_time = (
            str(int(value)) if float(value).is_integer() else format(float(value), ".15g")
        )
        maybe = _parse_time_text(text_time)
        if maybe:
            return maybe

    text = trim_excel_number_text(value)
    if not text:
        return None

    return _parse_time_text(text)



def _parse_time_text(text: str) -> str | None:
    text = text.strip()
    if not text:
        return None

    text = text.replace(".", ":")

    for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M:%S %p", "%H%M"):
        try:
            return datetime.strptime(text, fmt).strftime("%H:%M")
        except ValueError:
            continue

    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%H:%M")



def combine_date_and_time(
    date_value: Any,
    time_value: Any,
    dayfirst: bool = True,
) -> str | None:
    """
    Combina fecha y hora normalizadas en ISO local YYYY-MM-DD HH:MM:SS.
    """
    normalized_date = parse_excel_date(date_value, dayfirst=dayfirst)
    normalized_time = parse_excel_time(time_value)

    if not normalized_date and not normalized_time:
        return None
    if normalized_date and not normalized_time:
        return f"{normalized_date} 00:00:00"
    if not normalized_date:
        return None
    return f"{normalized_date} {normalized_time}:00"


# -----------------------------------------------------------------------------
# Utilidades para texto operativo
# -----------------------------------------------------------------------------
def extract_sku_from_text(value: Any) -> str | None:
    """
    Extrae SKU largo desde texto tipo:
        'Producto: 1103001319010101 / BISA ...'
    o desde un entero largo suelto.
    """
    text = trim_excel_number_text(value)
    if not text:
        return None

    match = SKU_PATTERN.search(text)
    if match:
        return match.group(1)

    match = LONG_INTEGER_PATTERN.search(text)
    if match:
        return match.group(1)

    return None



def strip_producto_prefix(value: Any) -> str | None:
    """
    Elimina el prefijo 'Producto: <sku> /' cuando exista.
    """
    text = normalize_text(value)
    if not text:
        return None
    stripped = PRODUCTO_PREFIX_PATTERN.sub("", text).strip()
    return stripped or None



def split_producto_y_color(value: Any) -> tuple[str | None, str | None]:
    """
    Intenta dividir 'producto / color' desde descripciones comunes del negocio.

    Reglas principales:
    - primero elimina prefijo 'Producto: <sku> /'
    - luego divide solo por un separador explícito con espacios alrededor (' / '),
      para no romper colores como 'A/T'
    - si no existe separador explícito, usa una heurística ligera de último token
      como color para casos simples como 'BISA COMAL 36 NEGRO'

    Ejemplo:
        'BISA BUDINERA 42 / A/T' -> ('BISA BUDINERA 42', 'A/T')
        'Producto: 123 / BISA BUDINERA 42 A/T' -> ('BISA BUDINERA 42', 'A/T')
    """
    text = strip_producto_prefix(value)
    if not text:
        return None, None

    explicit_parts = EXPLICIT_PRODUCT_COLOR_SPLIT_PATTERN.split(text, maxsplit=1)
    if len(explicit_parts) == 2:
        producto = normalize_text(explicit_parts[0])
        color = normalize_text(explicit_parts[1])
        if producto and color:
            return producto, color

    tokens = text.split()
    if len(tokens) >= 2:
        last_token = tokens[-1].strip()
        if 1 <= len(last_token) <= 12 and re.fullmatch(
            r"[A-Za-zÁÉÍÓÚÜÑ0-9\-_\/]+", last_token
        ):
            producto = normalize_text(" ".join(tokens[:-1]))
            color = normalize_text(last_token)
            if producto and color:
                return producto, color

    return normalize_text(text), None



def normalize_id_pedido(serie_documento: Any, documento: Any) -> str | None:
    """
    Construye Serie-Documento si ambos existen.
    """
    serie = normalize_text(serie_documento)
    doc = trim_excel_number_text(documento)
    if not serie or not doc:
        return None
    return f"{serie}-{doc}"



def explode_multiline_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Explota una columna multilinea en múltiples filas.
    """
    if column not in df.columns:
        raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        raw_value = row[column]
        text = normalize_text(raw_value)

        if not text or "\n" not in text:
            rows.append(row.to_dict())
            continue

        for part in [segment.strip() for segment in text.splitlines() if segment.strip()]:
            cloned = row.to_dict()
            cloned[column] = part
            rows.append(cloned)

    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# Helpers de hoja codigos / SKU
# -----------------------------------------------------------------------------
def resolve_codigos_sheet_name(
    file_path: str | Path,
    sheet_candidates: Iterable[str] | None = None,
) -> str | None:
    """
    Resuelve el nombre real de la hoja de códigos dentro de un workbook.
    """
    candidates = list(sheet_candidates or DEFAULT_CODIGOS_SHEET_CANDIDATES)
    workbook_sheets = read_excel_sheet_names(file_path)
    existing_normalized = {
        normalize_text(name).upper(): name
        for name in workbook_sheets
        if normalize_text(name)
    }

    for candidate in candidates:
        key = (normalize_text(candidate) or "").upper()
        if key and key in existing_normalized:
            return existing_normalized[key]
    return None



def coerce_lookup_code(value: Any) -> int | None:
    """
    Convierte una clave de tabla de códigos a entero tolerante.
    """
    text = trim_excel_number_text(value)
    if not text:
        return None

    digits = "".join(ch for ch in str(text) if ch.isdigit())
    if not digits:
        return None

    try:
        return int(digits)
    except ValueError:
        return None



def load_codigos_lookup(
    file_path: str | Path,
    sheet_name: str | None = None,
) -> dict[str, dict[int, str]]:
    """
    Lee la hoja de códigos del workbook y construye cuatro tablas maestras:
    - articulo -> A:B
    - modelo   -> E:F
    - color    -> H:I
    - filete   -> K:L
    """
    resolved_sheet_name = sheet_name or resolve_codigos_sheet_name(file_path)
    empty_result = {
        "articulo": {},
        "modelo": {},
        "color": {},
        "filete": {},
    }
    if not resolved_sheet_name:
        return empty_result

    try:
        df_raw = read_raw_sheet(file_path, sheet_name=resolved_sheet_name, max_rows=None)
    except Exception:
        return empty_result

    lookups: dict[str, dict[int, str]] = {
        "articulo": {},
        "modelo": {},
        "color": {},
        "filete": {},
    }

    column_pairs = {
        "articulo": (0, 1),
        "modelo": (4, 5),
        "color": (7, 8),
        "filete": (10, 11),
    }

    for _, row in df_raw.iterrows():
        values = row.tolist()
        for lookup_name, (key_idx, value_idx) in column_pairs.items():
            if key_idx >= len(values) or value_idx >= len(values):
                continue

            code = coerce_lookup_code(values[key_idx])
            label = normalize_text(values[value_idx])
            if code is None or not label:
                continue

            if code not in lookups[lookup_name]:
                lookups[lookup_name][code] = label.strip()

    return lookups



def lookup_codigo_value(
    mapping: dict[int, str],
    code: int | None,
    *,
    exact: bool = True,
) -> str | None:
    """
    Busca un código en una tabla maestra.

    exact=False emula BUSCARV aproximado tomando la clave máxima <= code.
    """
    if code is None or not mapping:
        return None

    if code in mapping:
        return mapping[code]

    if exact:
        return None

    candidates = [key for key in mapping if key <= code]
    if not candidates:
        return None
    return mapping[max(candidates)]



def derive_producto_color_from_sku(
    sku: Any,
    codigos_lookup: dict[str, dict[int, str]] | None = None,
) -> tuple[str | None, str | None]:
    """
    Deriva producto y color desde el SKU usando la hoja codigos.

    Estructura usada:
    - dígitos 3-4   -> artículo     -> A:B   (búsqueda aproximada)
    - dígitos 5-8   -> modelo       -> E:F   (búsqueda exacta)
    - dígitos 9-10  -> color base   -> H:I   (búsqueda aproximada)
    - dígitos 11-12 -> filete       -> K:L   (búsqueda exacta)
    """
    sku_text = trim_excel_number_text(sku)
    if not sku_text:
        return None, None

    sku_digits = "".join(ch for ch in str(sku_text) if ch.isdigit())
    if len(sku_digits) < 12:
        return None, None

    try:
        codigo_articulo = int(sku_digits[2:4])
        codigo_modelo = int(sku_digits[4:8])
        codigo_color = int(sku_digits[8:10])
        codigo_filete = int(sku_digits[10:12])
    except ValueError:
        return None, None

    lookups = codigos_lookup or {}
    articulo_desc = lookup_codigo_value(
        lookups.get("articulo", {}),
        codigo_articulo,
        exact=False,
    )
    modelo_desc = lookup_codigo_value(
        lookups.get("modelo", {}),
        codigo_modelo,
        exact=True,
    )
    color_desc = lookup_codigo_value(
        lookups.get("color", {}),
        codigo_color,
        exact=False,
    )
    filete_desc = lookup_codigo_value(
        lookups.get("filete", {}),
        codigo_filete,
        exact=True,
    )

    producto = " ".join(part for part in [articulo_desc, modelo_desc] if part).strip() or None
    color = " ".join(part for part in [color_desc, filete_desc] if part).strip() or None
    return producto, color


# -----------------------------------------------------------------------------
# Helpers para loaders
# -----------------------------------------------------------------------------
def coerce_numeric_column(
    df: pd.DataFrame,
    column: str,
    default: float = 0.0,
) -> pd.Series:
    """
    Convierte una columna a float usando reglas tolerantes.
    """
    if column not in df.columns:
        raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
    return df[column].apply(lambda value: to_float(value, default))



def normalize_text_column(df: pd.DataFrame, column: str) -> pd.Series:
    """
    Normaliza una columna textual sin perder el tamaño del DataFrame.
    """
    if column not in df.columns:
        raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
    return df[column].apply(normalize_text)



def add_missing_columns(df: pd.DataFrame, columns_with_defaults: dict[str, Any]) -> pd.DataFrame:
    """
    Agrega columnas faltantes con un valor por defecto.
    """
    result = df.copy()
    for column, default in columns_with_defaults.items():
        if column not in result.columns:
            result[column] = default
    return result



def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Convierte DataFrame a lista de diccionarios reemplazando NaN por None.
    """
    safe_df = df.where(pd.notna(df), None)
    return safe_df.to_dict(orient="records")


__all__ = [
    "DEFAULT_CODIGOS_SHEET_CANDIDATES",
    "DEFAULT_HEADER_MIN_MATCHES",
    "DEFAULT_HEADER_SEARCH_ROWS",
    "add_missing_columns",
    "coerce_lookup_code",
    "combine_date_and_time",
    "coerce_numeric_column",
    "dataframe_from_detected_header",
    "dataframe_to_records",
    "derive_producto_color_from_sku",
    "detect_header_row",
    "drop_fully_empty_columns",
    "drop_fully_empty_rows",
    "drop_fully_unnamed_columns",
    "ensure_path",
    "ensure_required_columns",
    "explode_multiline_column",
    "extract_sku_from_text",
    "load_codigos_lookup",
    "lookup_codigo_value",
    "make_unique_headers",
    "normalize_column_name",
    "normalize_id_pedido",
    "normalize_text_column",
    "parse_excel_date",
    "parse_excel_time",
    "read_excel_sheet_names",
    "read_raw_sheet",
    "read_tabular_file",
    "rename_columns_by_aliases",
    "resolve_codigos_sheet_name",
    "row_to_normalized_tokens",
    "safe_str",
    "split_producto_y_color",
    "strip_producto_prefix",
    "trim_excel_number_text",
]
