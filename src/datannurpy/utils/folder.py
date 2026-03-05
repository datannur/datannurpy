"""Folder management utilities for incremental scan."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..schema import Folder

if TYPE_CHECKING:
    from ..catalog import Catalog


def upsert_folder(catalog: Catalog, folder: Folder) -> None:
    """Add or update a folder, marking it as seen for incremental scan."""
    folder._seen = True
    catalog.folder.upsert(folder)
