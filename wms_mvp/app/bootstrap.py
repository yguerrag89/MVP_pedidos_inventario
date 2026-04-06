from __future__ import annotations

from typing import Optional

from wms_mvp.core.config import DB_PATH, ensure_directories
from wms_mvp.db.connection import db_exists
from wms_mvp.db.init_db import SchemaCompatibilityError, init_db

_BOOTSTRAP_DONE = False
_BOOTSTRAP_NOTE: Optional[str] = None


def bootstrap_runtime() -> Optional[str]:
    """
    Asegura directorios, base y migraciones ligeras antes de renderizar páginas.

    Devuelve una nota informativa si la base existente necesita atención manual.
    """
    global _BOOTSTRAP_DONE, _BOOTSTRAP_NOTE
    if _BOOTSTRAP_DONE:
        return _BOOTSTRAP_NOTE

    ensure_directories()

    try:
        if not db_exists(DB_PATH):
            init_db()
        else:
            init_db()
    except SchemaCompatibilityError as exc:
        _BOOTSTRAP_NOTE = str(exc)
    except Exception as exc:  # pragma: no cover - defensa de arranque
        _BOOTSTRAP_NOTE = f"No fue posible preparar la base automáticamente: {exc}"

    _BOOTSTRAP_DONE = True
    return _BOOTSTRAP_NOTE
