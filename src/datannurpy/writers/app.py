"""App export utilities for datannur visualization."""

from __future__ import annotations

import shutil
from pathlib import Path


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
