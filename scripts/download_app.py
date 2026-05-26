#!/usr/bin/env python3
"""Download the datannur app for bundling in the package.

Usage: make download-app
"""

from __future__ import annotations

import io
import shutil
import urllib.request
import zipfile
from pathlib import Path

_URL = "https://github.com/datannur/datannur/releases/download/pre-release/datannur-app-pre-release.zip"
_APP_DIR = Path(__file__).resolve().parent.parent / "src" / "datannurpy" / "app"
_DEMO_DB_SOURCE_DIR = (
    Path(__file__).resolve().parent.parent / "examples" / "datannur_app" / "db-source"
)
_DEMO_PDF_DIR = (
    Path(__file__).resolve().parent.parent / "examples" / "datannur_app" / "pdf"
)
_DEMO_MD_DOC_DIR = (
    Path(__file__).resolve().parent.parent / "examples" / "datannur_app" / "md-doc"
)


def _replace_dir(source: Path | None, destination: Path, label: str) -> None:
    """Replace a preserved demo directory with downloaded content."""
    if destination.exists():
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source is None or not source.exists():
        destination.mkdir(parents=True, exist_ok=True)
        return
    shutil.copytree(source, destination)
    print(f"✓ Saved demo {label} to {destination}")


def download_app() -> None:
    """Download and extract the datannur app."""
    print(f"Downloading from {_URL}...")

    with urllib.request.urlopen(_URL, timeout=60) as r:
        zip_data = r.read()

    print(f"Downloaded {len(zip_data) / 1024:.0f} KB")

    if _APP_DIR.exists():
        shutil.rmtree(_APP_DIR)

    _APP_DIR.mkdir(parents=True)

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        zf.extractall(_APP_DIR)

    # Preserve the manually curated demo source files for later reuse in this repo.
    db_source_dir = _APP_DIR / "data" / "db-source"
    if not db_source_dir.exists():
        db_source_dir = _APP_DIR / "app" / "data-template" / "db-source"
    if db_source_dir.exists():
        _replace_dir(db_source_dir, _DEMO_DB_SOURCE_DIR, "db-source")

    pdf_dir = _APP_DIR / "data" / "pdf"
    _replace_dir(pdf_dir if pdf_dir.exists() else None, _DEMO_PDF_DIR, "pdf")

    md_doc_dir = _APP_DIR / "data" / "db" / "md-doc"
    _replace_dir(
        md_doc_dir if md_doc_dir.exists() else None, _DEMO_MD_DOC_DIR, "md-doc"
    )

    # Clear demo data content (keep the data/ folder structure)
    data_dir = _APP_DIR / "data"
    if data_dir.exists():
        for item in data_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        print("✓ Cleared demo data/")

    print("✓ Done")


if __name__ == "__main__":
    download_app()
