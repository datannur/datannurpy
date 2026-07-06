"""Persist and reload the scan-derived base (the ``_scan`` cache).

The exported DB is a disposable materialization of the *current scan* plus the
*current metadata* layers. To keep incremental runs both fast and correct, the
scan-derived rows are cached separately under ``<db_path>/_scan/`` before any
metadata is applied. A later run reloads this pristine base and rebuilds the
final DB from ``_scan`` + current metadata, never from a previously-overlaid
export. This is what lets an emptied metadata cell fall back to the scanned
value instead of leaving a stale overlay behind.

``_scan`` is an internal, inspectable cache: it uses the same JSON table format
as the final DB, but its ``__table__.json`` is never listed as an app table, so
the app and the export's stale-file cleanup both ignore it.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from jsonjsdb.writer import table_index_df, table_json_content, write_text_if_changed

if TYPE_CHECKING:
    from .catalog import Catalog

SCAN_CACHE_DIRNAME = "_scan"

# App-level tables are rebuilt from app_config on every run, so they are never
# part of the scan base. Every other table in the registry is scan/metadata data
# the cache persists — deriving the set this way keeps it in sync with the schema.
_APP_TABLES = {"config", "configFilter"}


def scan_cache_dir(db_path: Path) -> Path:
    """Return the deterministic, non-configurable scan cache directory."""
    return db_path / SCAN_CACHE_DIRNAME


def scan_cache_load_path(db_path: Path | None) -> str | None:
    """Return the ``_scan`` directory to load as incremental base, if present."""
    if db_path is None:
        return None
    cache = scan_cache_dir(db_path)
    if (cache / "__table__.json").exists():
        return str(cache)
    return None


def write_scan_cache(catalog: Catalog, db_path: Path, timestamp: int) -> None:
    """Write the current (pre-metadata) scan base to ``<db_path>/_scan/``."""
    cache = scan_cache_dir(db_path)
    cache.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    for name, table in catalog._tables.items():
        if name in _APP_TABLES:
            continue
        df = table.get_persistable_df()
        if df.is_empty():
            continue
        write_text_if_changed(cache / f"{name}.json", table_json_content(df))
        written.append(name)

    # Internal manifest so jsonjsdb can reload the cache; skipped as an app table.
    write_text_if_changed(
        cache / "__table__.json",
        table_json_content(table_index_df(written, timestamp)),
    )
    _clean_scan_cache(cache, written)


def _clean_scan_cache(cache: Path, written: list[str]) -> None:
    """Remove cached table files that are no longer part of the scan base."""
    keep = {f"{name}.json" for name in written} | {"__table__.json"}
    for entry in cache.iterdir():
        if entry.name.endswith(".json") and entry.name not in keep:
            entry.unlink()
