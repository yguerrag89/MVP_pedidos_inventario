from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Final


PACKAGE_DIR: Final[Path] = Path(__file__).resolve().parents[1]
REPO_DIR: Final[Path] = PACKAGE_DIR.parent
SOURCE_DATA_DIR: Final[Path] = PACKAGE_DIR / 'data'


def _running_in_streamlit_cloud() -> bool:
    current = str(PACKAGE_DIR)
    return current.startswith('/mount/src/') or bool(os.getenv('STREAMLIT_SERVER_PORT'))


def _resolve_runtime_data_dir() -> Path:
    env_dir = os.getenv('WMS_RUNTIME_DATA_DIR', '').strip()
    if env_dir:
        return Path(env_dir).expanduser().resolve()

    if _running_in_streamlit_cloud():
        return Path('/tmp/wms_mvp_runtime').resolve()

    return (PACKAGE_DIR / 'data').resolve()


BASE_DIR: Final[Path] = PACKAGE_DIR
DATA_DIR: Final[Path] = _resolve_runtime_data_dir()
INPUT_DIR: Final[Path] = DATA_DIR / 'input'
PROCESSED_DIR: Final[Path] = DATA_DIR / 'processed'
ARCHIVE_DIR: Final[Path] = DATA_DIR / 'archive'
BACKUPS_DIR: Final[Path] = DATA_DIR / 'backups'

DOCS_DIR: Final[Path] = PACKAGE_DIR / 'docs'
TESTS_DIR: Final[Path] = PACKAGE_DIR / 'tests'

DB_PATH: Final[Path] = DATA_DIR / 'wms_mvp.db'

STREAMLIT_PAGE_TITLE: Final[str] = 'WMS MVP'
STREAMLIT_LAYOUT: Final[str] = 'wide'

APP_NAME: Final[str] = 'WMS MVP'
APP_VERSION: Final[str] = '0.1.0'


def ensure_directories() -> None:
    """
    Crea las carpetas base necesarias para el funcionamiento del proyecto.
    No falla si las carpetas ya existen.
    
    En Streamlit Community Cloud se usan rutas de runtime (por ejemplo /tmp)
    para evitar escribir la base SQLite dentro del checkout del repositorio.
    """
    required_dirs = (
        DATA_DIR,
        INPUT_DIR,
        PROCESSED_DIR,
        ARCHIVE_DIR,
        BACKUPS_DIR,
        DOCS_DIR,
        TESTS_DIR,
    )

    for path in required_dirs:
        path.mkdir(parents=True, exist_ok=True)

    source_db = SOURCE_DATA_DIR / 'wms_mvp.db'
    if DATA_DIR != SOURCE_DATA_DIR and not DB_PATH.exists() and source_db.exists():
        shutil.copy2(source_db, DB_PATH)
