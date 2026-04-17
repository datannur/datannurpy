"""YAML configuration support for datannurpy catalogs."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from ..catalog import Catalog
from ..errors import ConfigError
from ..schema import Folder

VALID_TYPES = {"folder", "dataset", "database", "metadata"}
RESERVED_KEYS = {
    "add",
    "env",
    "env_file",
    "open_browser",
    "output_dir",
    "post_export",
    "track_evolution",
}


def _normalize_entry(
    item: dict[str, Any],
) -> tuple[str, dict[str, Any], Any]:
    """Normalize an add entry to (type, params, primary_value).

    Old format: {type: folder, path: ./data, ...}
    New format: {folder: ./data, ...} or {database: uri, ...}
    """
    item = dict(item)

    # New format: type key carries the primary value (path or URI)
    type_keys = VALID_TYPES & item.keys()
    # Exclude 'folder' when its value is a dict (it's a Folder metadata, not a type)
    if "folder" in type_keys and isinstance(item.get("folder"), dict):
        type_keys.discard("folder")
    if type_keys:
        if "type" in item:
            raise ConfigError(
                "Cannot mix 'type' key with shorthand keys "
                f"({', '.join(sorted(type_keys))})"
            )
        if len(type_keys) > 1:
            raise ConfigError(
                f"Entry has multiple type keys: {', '.join(sorted(type_keys))}"
            )
        item_type = type_keys.pop()
        primary_value = item.pop(item_type)
        return item_type, item, primary_value

    # Old format: type + path/uri
    if "type" not in item:
        raise ConfigError(
            "Each 'add' entry must have a type key "
            "(folder, dataset, database, or metadata)"
        )
    item_type = item.pop("type")
    if item_type not in VALID_TYPES:
        valid = ", ".join(sorted(VALID_TYPES))
        raise ConfigError(f"Unknown type '{item_type}' in config. Valid types: {valid}")
    return item_type, item, None


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


def _resolve_script(name: str, output_dir: Path) -> Path:
    """Resolve a post_export script name to an absolute path."""
    if os.path.isabs(name):
        return Path(name)
    if "/" in name or name.endswith(".py"):
        return output_dir / name
    return output_dir / "python-scripts" / f"{name}.py"


def _run_post_export(scripts: str | list[str], output_dir: Path, quiet: bool) -> None:
    """Run post_export scripts after export."""
    names = scripts if isinstance(scripts, list) else [scripts]
    for name in names:
        script = _resolve_script(name, output_dir)
        if not script.exists():
            raise ConfigError(f"post_export script not found: {script}")
        if not quiet:
            print(f"  → post_export: {script.name}", file=sys.stderr)
        try:
            subprocess.run(
                [sys.executable, str(script)],
                cwd=str(output_dir),
                check=True,
            )
        except KeyboardInterrupt:
            return


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

    # Load environment variables in priority order (first set wins via setdefault/override=False):
    # 1. System env vars (already in os.environ)
    # 2. env: YAML (explicit, specific to this scan)
    # 3. env_file: (secrets — supports str or list, last in list = highest priority)
    # 4. .env local next to YAML (shared defaults)
    # interpolate=False so that $ in passwords is kept literal
    env_vars = config.pop("env", None)
    if env_vars:
        if not isinstance(env_vars, dict):
            raise ConfigError("'env' must be a mapping of key: value pairs")
        for key, val in env_vars.items():
            os.environ.setdefault(str(key), str(val))

    env_file = config.pop("env_file", None)
    if env_file:
        paths = env_file if isinstance(env_file, list) else [env_file]
        for ef in reversed(paths):
            load_dotenv(_resolve_path(ef, base_dir), override=False, interpolate=False)

    load_dotenv(base_dir / ".env", override=False, interpolate=False)

    # Expand environment variables in all string values
    config = _expand_vars(config)

    # Pop export options before building catalog params
    open_browser = config.pop("open_browser", False)
    track_evolution = config.pop("track_evolution", True)
    post_export = config.pop("post_export", None)
    output_dir = config.pop("output_dir", None)
    if output_dir:
        output_dir = _resolve_path(output_dir, base_dir)

    # Extract catalog init params and resolve paths
    catalog_params = {k: v for k, v in config.items() if k not in RESERVED_KEYS}
    if "app_path" in catalog_params:
        catalog_params["app_path"] = _resolve_path(catalog_params["app_path"], base_dir)
    if "log_file" in catalog_params:
        catalog_params["log_file"] = _resolve_path(catalog_params["log_file"], base_dir)

    catalog = Catalog(**catalog_params)

    # Process add entries (metadata always last to override auto-scanned values)
    entries = config.get("add", [])
    entries.sort(key=lambda e: "metadata" in e or e.get("type") == "metadata")
    for raw_item in entries:
        item_type, item, primary_value = _normalize_entry(raw_item)
        if "folder" in item and isinstance(item["folder"], dict):
            item["folder"] = Folder(**item["folder"])

        if item_type == "folder":
            if primary_value is not None:
                folder_path = _resolve_paths(primary_value, base_dir)
            else:
                folder_path = _resolve_paths(item.pop("path"), base_dir)
            catalog.add_folder(folder_path, **item)
        elif item_type == "dataset":
            if primary_value is not None:
                dataset_path = _resolve_paths(primary_value, base_dir)
            else:
                dataset_path = _resolve_paths(item.pop("path"), base_dir)
            catalog.add_dataset(dataset_path, **item)
        elif item_type == "database":
            if primary_value is not None:
                uri = primary_value
            else:
                uri = item.pop("uri")
            # Resolve sqlite:/// paths
            if (
                isinstance(uri, str)
                and uri.startswith("sqlite:///")
                and not uri.startswith("sqlite:////")
            ):
                db_path = uri[len("sqlite:///") :]
                uri = f"sqlite:///{_resolve_path(db_path, base_dir)}"
            catalog.add_database(uri, **item)
        else:
            if primary_value is not None:
                meta_path = _resolve_path(primary_value, base_dir)
            else:
                meta_path = _resolve_path(item.pop("path"), base_dir)
            catalog.add_metadata(meta_path, **item)

    # Export: app_path implies app export, output_dir implies db-only export
    export_dir: Path | None = None
    if output_dir:
        catalog.export_db(output_dir, track_evolution=track_evolution)
        export_dir = Path(output_dir)
    elif catalog.app_path is not None:
        catalog.export_app(open_browser=open_browser, track_evolution=track_evolution)
        export_dir = Path(catalog.app_path)

    if post_export and export_dir is not None:
        quiet = catalog_params.get("quiet", False)
        _run_post_export(post_export, export_dir, bool(quiet))

    return catalog
