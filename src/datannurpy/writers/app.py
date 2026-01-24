"""App export utilities for datannur visualization."""

from __future__ import annotations

import shutil
import webbrowser
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..catalog import Catalog


def get_app_path() -> Path:
    """Return path to bundled app directory."""
    return Path(__file__).parent.parent / "app"


def copy_app(output_dir: Path) -> None:
    """Copy datannur app to output directory."""
    app_src = get_app_path()
    if not app_src.exists():
        raise FileNotFoundError(
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


def export_app(
    catalog: Catalog,
    output_dir: str | Path,
    *,
    open_browser: bool = False,
) -> None:
    """Export a standalone datannur visualization app with catalog data."""
    output_dir = Path(output_dir)

    # Copy app files
    copy_app(output_dir)

    # Clear and write to data/db/
    db_dir = output_dir / "data" / "db"
    if db_dir.exists():
        shutil.rmtree(db_dir)

    catalog.write(db_dir)

    if open_browser:
        index_path = output_dir / "index.html"
        webbrowser.open(index_path.resolve().as_uri())
