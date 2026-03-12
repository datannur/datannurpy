"""YAML configuration support for datannurpy catalogs."""

from __future__ import annotations

from pathlib import Path

import yaml

from ..catalog import Catalog
from ..schema import Folder


def run_config(path: str | Path) -> Catalog:
    """Load and execute a YAML catalog configuration."""
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Extract catalog init params
    catalog_params = {
        k: v for k, v in config.items() if k not in ("add", "export_app", "export_db")
    }
    catalog = Catalog(**catalog_params)

    # Process add entries
    for item in config.get("add", []):
        item = dict(item)
        item_type = item.pop("type")
        if "folder" in item:
            item["folder"] = Folder(**item["folder"])

        if item_type == "folder":
            catalog.add_folder(item.pop("path"), **item)
        elif item_type == "database":
            catalog.add_database(item.pop("uri"), **item)
        elif item_type == "metadata":
            catalog.add_metadata(item.pop("path"), **item)

    # Export
    if "export_app" in config:
        catalog.export_app(**config["export_app"])
    elif "export_db" in config:
        catalog.export_db(**config["export_db"])

    return catalog
