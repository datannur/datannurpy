"""YAML configuration support for datannurpy catalogs."""

from __future__ import annotations

import warnings
from pathlib import Path

import yaml

from ..catalog import Catalog
from ..schema import Folder

VALID_TYPES = {"folder", "dataset", "database", "metadata"}


def _resolve_path(p: str, base_dir: Path) -> str:
    """Resolve a path relative to base_dir if not absolute."""
    path = Path(p)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return str(path)


def run_config(path: str | Path) -> Catalog:
    """Load and execute a YAML catalog configuration."""
    config_path = Path(path).resolve()
    base_dir = config_path.parent

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Extract catalog init params and resolve paths
    catalog_params = {
        k: v for k, v in config.items() if k not in ("add", "export_app", "export_db")
    }
    if "app_path" in catalog_params:
        catalog_params["app_path"] = _resolve_path(catalog_params["app_path"], base_dir)

    catalog = Catalog(**catalog_params)

    # Process add entries
    for item in config.get("add", []):
        item = dict(item)
        item_type = item.pop("type")
        if "folder" in item:
            item["folder"] = Folder(**item["folder"])

        if item_type == "folder":
            folder_path = _resolve_path(item.pop("path"), base_dir)
            catalog.add_folder(folder_path, **item)
        elif item_type == "dataset":
            dataset_path = _resolve_path(item.pop("path"), base_dir)
            catalog.add_dataset(dataset_path, **item)
        elif item_type == "database":
            uri = item.pop("uri")
            # Resolve sqlite:/// paths
            if uri.startswith("sqlite:///") and not uri.startswith("sqlite:////"):
                db_path = uri[len("sqlite:///") :]
                uri = f"sqlite:///{_resolve_path(db_path, base_dir)}"
            catalog.add_database(uri, **item)
        elif item_type == "metadata":
            meta_path = _resolve_path(item.pop("path"), base_dir)
            catalog.add_metadata(meta_path, **item)
        else:
            warnings.warn(
                f"Unknown type '{item_type}' in config. Valid types: {', '.join(sorted(VALID_TYPES))}",
                stacklevel=2,
            )

    # Export
    if "export_app" in config:
        catalog.export_app(**config["export_app"])
    elif "export_db" in config:
        catalog.export_db(**config["export_db"])

    return catalog
