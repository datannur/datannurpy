"""Folder management utilities for incremental scan."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..entities import Folder

if TYPE_CHECKING:
    from ..catalog import Catalog


def upsert_folder(catalog: Catalog, folder: Folder) -> None:
    """Add or update a folder, marking it as seen for incremental scan."""
    existing = catalog._folder_index.get(folder.id)
    if existing is not None:
        # Update existing folder
        existing.data_path = folder.data_path
        existing.last_update_date = folder.last_update_date
        existing.type = folder.type
        existing.parent_id = folder.parent_id
        existing.name = folder.name
        existing._seen = True
    else:
        # Add new folder
        folder._seen = True
        catalog.folders.append(folder)
        catalog._folder_index[folder.id] = folder
