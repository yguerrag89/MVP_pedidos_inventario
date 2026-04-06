from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


REQUIRED_INVENTARIO_COLUMNS: tuple[str, ...] = (
    "SKU",
    "DESCRIPCION",
    "STOCK",
    "FECHA_ACTUALIZACION",
)

HEADER_ALIASES: dict[str, str] = {
    "sku": "SKU",
    "descripcion": "DESCRIPCION",
    "descripcion_": "DESCRIPCION",
    "stock": "STOCK",
    "fecha_actualizacion": "FECHA_ACTUALIZACION",
    "fecha_actualizacion_": "FECHA_ACTUALIZACION",
}


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text



def normalize_header(value: Any) -> str:
    text = unicodedata.normalize("NFKD", normalize_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text



def canonical_inventory_columns(columns: list[Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for original in columns:
        normalized = normalize_header(original)
        canonical = HEADER_ALIASES.get(normalized)
        if canonical:
            mapping[str(original)] = canonical
    return mapping



def missing_required_inventory_columns(columns: list[Any]) -> list[str]:
    mapping = canonical_inventory_columns(columns)
    present = set(mapping.values())
    return [column for column in REQUIRED_INVENTARIO_COLUMNS if column not in present]



def normalize_sku(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, bool):
        return ""

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        if value != value:
            return ""
        if value.is_integer():
            return str(int(value))
        return format(Decimal(str(value)).normalize(), "f").rstrip("0").rstrip(".")

    text = normalize_text(value)
    if not text:
        return ""

    compact = re.sub(r"\s+", "", text)
    try:
        decimal_value = Decimal(compact)
        if decimal_value == decimal_value.to_integral():
            return format(decimal_value.quantize(Decimal("1")), "f")
        return format(decimal_value.normalize(), "f").rstrip("0").rstrip(".")
    except (InvalidOperation, ValueError):
        return compact.upper()



def normalize_descripcion(value: Any) -> str:
    return normalize_text(value)



def parse_stock_value(value: Any, *, treat_blank_as_zero: bool) -> tuple[float | None, bool]:
    if value is None:
        return (0.0, True) if treat_blank_as_zero else (None, True)

    if isinstance(value, str) and not value.strip():
        return (0.0, True) if treat_blank_as_zero else (None, True)

    text = normalize_text(value).replace(",", "")
    if not text:
        return (0.0, True) if treat_blank_as_zero else (None, True)

    try:
        return float(text), False
    except (TypeError, ValueError):
        return None, False



def parse_required_date_to_iso(value: Any) -> str | None:
    if value is None:
        return None

    try:
        if value != value:
            return None
    except Exception:
        pass

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    raw_text = str(value).strip()
    if raw_text.lower() in {"", "nan", "nat", "none"}:
        return None

    text = normalize_text(value)
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


VALID_MANUAL_ADJUSTMENT_KINDS: tuple[str, ...] = (
    "ENTRADA",
    "SALIDA",
)


def normalize_manual_adjustment_kind(value: Any) -> str:
    text = normalize_text(value).upper()
    aliases = {
        "+": "ENTRADA",
        "ENTRADA": "ENTRADA",
        "AJUSTE_ENTRADA": "ENTRADA",
        "INCREMENTO": "ENTRADA",
        "SALIDA": "SALIDA",
        "AJUSTE_SALIDA": "SALIDA",
        "-": "SALIDA",
        "DECREMENTO": "SALIDA",
    }
    normalized = aliases.get(text, text)
    if normalized not in VALID_MANUAL_ADJUSTMENT_KINDS:
        raise ValueError(
            f"Tipo de ajuste inválido: {value}. Valores permitidos: {list(VALID_MANUAL_ADJUSTMENT_KINDS)}"
        )
    return normalized


def validate_manual_adjustment_quantity(value: Any) -> float:
    text = normalize_text(value).replace(",", "")
    if not text:
        raise ValueError("La cantidad del ajuste es obligatoria.")
    try:
        qty = float(text)
    except (TypeError, ValueError):
        raise ValueError("La cantidad del ajuste debe ser numérica.")
    if qty <= 0:
        raise ValueError("La cantidad del ajuste debe ser mayor que 0.")
    return qty
