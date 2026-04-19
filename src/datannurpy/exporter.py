"""Export catalog to JSON database and web app."""

from __future__ import annotations

import shutil
import sys
import time
import webbrowser
from pathlib import Path
from typing import TYPE_CHECKING

from .utils.log import _write_log
from .utils.params import validate_params

if TYPE_CHECKING:
    from .catalog import Catalog

from .add_metadata import ensure_metadata_applied
from .errors import ConfigError


@validate_params
def export_db(
    catalog: Catalog,
    output_dir: str | Path | None = None,
    *,
    track_evolution: bool = True,
    quiet: bool | None = None,
) -> None:
    """Write all catalog entities to JSON files."""
    ensure_metadata_applied(catalog)
    # Only finalize (cleanup unseen entities) if a scan was performed
    if catalog._has_scanned:
        catalog.finalize()

    path = output_dir or catalog.db_path
    if path is None:
        msg = "output_dir is required when app_path was not set at init"
        raise ConfigError(msg)

    # Parent relations for cascade suppression in evolution tracking
    parent_relations = {
        "dataset": "folder",
        "variable": "dataset",
        "freq": "variable",
        "value": "modality",
    }
    catalog.save(
        path,
        track_evolution=track_evolution,
        timestamp=catalog._now,
        parent_relations=parent_relations,
    )


def _get_app_path() -> Path:
    """Return path to bundled app directory."""
    return Path(__file__).parent / "app"


def _copy_app(output_dir: Path) -> None:
    """Copy datannur app to output directory."""
    app_src = _get_app_path()
    if not app_src.exists():
        raise ConfigError(
            "datannur app not found. Run `make download-app` to download it, "
            "or install datannurpy with the app bundled."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    # Copy app files to output_dir (merge with existing files)
    for item in app_src.iterdir():
        dest = output_dir / item.name
        if item.is_dir():
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)


@validate_params
def export_app(
    catalog: Catalog,
    output_dir: str | Path | None = None,
    *,
    open_browser: bool = False,
    track_evolution: bool = True,
    quiet: bool | None = None,
) -> None:
    """Export a standalone datannur visualization app with catalog data."""
    ensure_metadata_applied(catalog)
    # Only finalize (cleanup unseen entities) if a scan was performed
    if catalog._has_scanned:
        catalog.finalize()

    if output_dir is None:
        if catalog.app_path is None:
            msg = "output_dir is required when app_path was not set at init"
            raise ConfigError(msg)
        output_dir = catalog.app_path

    q = quiet if quiet is not None else catalog.quiet
    output_dir = Path(output_dir)

    start_time = time.perf_counter()
    header = f"\n[export_app] {output_dir.name}"
    if not q:
        print(header, file=sys.stderr)
    _write_log(header)

    # Copy app files
    _copy_app(output_dir)

    # Write to data/db/
    db_dir = output_dir / "data" / "db"
    catalog.export_db(db_dir, quiet=True, track_evolution=track_evolution)

    elapsed = time.perf_counter() - start_time
    index_uri = (output_dir / "index.html").resolve().as_uri()
    summary = f"\n  → exported in {elapsed:.1f}s: {index_uri}"
    if not q:
        print(summary, file=sys.stderr)
    _write_log(summary)

    if open_browser:
        index_path = output_dir / "index.html"
        webbrowser.open(index_path.resolve().as_uri())
