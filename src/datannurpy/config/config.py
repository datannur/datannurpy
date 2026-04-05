"""YAML configuration support for datannurpy catalogs."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from ..catalog import Catalog
from ..errors import ConfigError
from ..schema import Folder

VALID_TYPES = {"folder", "dataset", "database", "metadata"}
RESERVED_KEYS = {"add", "export_app", "export_db", "env_file", "env"}


def _expand_vars(obj: Any) -> Any:
    """Recursively expand $VAR and ${VAR} references in string values."""
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    if isinstance(obj, dict):
        return {k: _expand_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_vars(item) for item in obj]
    return obj


def _resolve_path(p: str, base_dir: Path) -> str:
    """Resolve a path relative to base_dir if not absolute."""
    if "://" in p:
        return p
    path = Path(p)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return str(path)


def _resolve_paths(p: str | list[str], base_dir: Path) -> str | list[str]:
    """Resolve a path or list of paths relative to base_dir."""
    if isinstance(p, list):
        return [_resolve_path(x, base_dir) for x in p]
    return _resolve_path(p, base_dir)


def run_config(path: str | Path) -> Catalog:
    """Load and execute a YAML catalog configuration."""
    config_path = Path(path).resolve()
    base_dir = config_path.parent

    try:
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {config_path}") from None
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in {config_path.name}: {e}") from None

    if not isinstance(config, dict):
        raise ConfigError(f"{config_path.name} must be a YAML mapping")

    # Load .env: explicit env_file path takes priority, fallback to .env next to YAML
    # interpolate=False so that $ in passwords is kept literal
    env_file = config.pop("env_file", None)
    if env_file:
        load_dotenv(
            _resolve_path(env_file, base_dir), override=False, interpolate=False
        )
    else:
        load_dotenv(base_dir / ".env", override=False, interpolate=False)

    # Inject env: vars (lowest priority — never overrides system or .env vars)
    env_vars = config.pop("env", None)
    if env_vars:
        if not isinstance(env_vars, dict):
            raise ConfigError("'env' must be a mapping of key: value pairs")
        for key, val in env_vars.items():
            os.environ.setdefault(str(key), str(val))

    # Expand environment variables in all string values
    config = _expand_vars(config)

    # Extract catalog init params and resolve paths
    catalog_params = {k: v for k, v in config.items() if k not in RESERVED_KEYS}
    if "app_path" in catalog_params:
        catalog_params["app_path"] = _resolve_path(catalog_params["app_path"], base_dir)
    if "log_file" in catalog_params:
        catalog_params["log_file"] = _resolve_path(catalog_params["log_file"], base_dir)

    catalog = Catalog(**catalog_params)

    # Process add entries (metadata always last to override auto-scanned values)
    entries = config.get("add", [])
    entries.sort(key=lambda e: e.get("type") == "metadata")
    for item in entries:
        item = dict(item)
        item_type = item.pop("type")
        if "folder" in item:
            item["folder"] = Folder(**item["folder"])

        if item_type == "folder":
            folder_path = _resolve_paths(item.pop("path"), base_dir)
            catalog.add_folder(folder_path, **item)
        elif item_type == "dataset":
            dataset_path = _resolve_paths(item.pop("path"), base_dir)
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
            valid = ", ".join(sorted(VALID_TYPES))
            raise ConfigError(
                f"Unknown type '{item_type}' in config. Valid types: {valid}"
            )

    # Export
    if "export_app" in config:
        catalog.export_app(**config["export_app"])
    elif "export_db" in config:
        catalog.export_db(**config["export_db"])

    return catalog
