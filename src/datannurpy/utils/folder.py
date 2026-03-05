"""Folder management utilities for incremental scan."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..schema import Folder

if TYPE_CHECKING:
    from ..catalog import Catalog


def upsert_folder(catalog: Catalog, folder: Folder) -> None:
    """Add or update a folder, marking it as seen for incremental scan."""
    existing = catalog.folder.get(folder.id)
    if existing is not None:
        # Update existing folder
        catalog.folder.update(
            folder.id,
            data_path=folder.data_path,
            last_update_date=folder.last_update_date,
            type=folder.type,
            parent_id=folder.parent_id,
            name=folder.name,
            _seen=True,
        )
    else:
        # Add new folder
        folder._seen = True
        catalog.folder.add(folder)
