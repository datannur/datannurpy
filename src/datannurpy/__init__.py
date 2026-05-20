"""datannurpy - Python library for datannur catalog metadata management."""

from __future__ import annotations

from importlib import import_module
from importlib.metadata import version
from typing import TYPE_CHECKING

__version__ = version("datannurpy")

if TYPE_CHECKING:
    from .catalog import Catalog
    from .config import run_config
    from .errors import ConfigError
    from .exporter import copy_assets
    from .schema import EntityMetadata, Folder
    from .utils.ids import build_dataset_id, build_variable_id, sanitize_id

_LAZY_IMPORTS = {
    "Catalog": ("datannurpy.catalog", "Catalog"),
    "ConfigError": ("datannurpy.errors", "ConfigError"),
    "EntityMetadata": ("datannurpy.schema", "EntityMetadata"),
    "Folder": ("datannurpy.schema", "Folder"),
    "build_dataset_id": ("datannurpy.utils.ids", "build_dataset_id"),
    "build_variable_id": ("datannurpy.utils.ids", "build_variable_id"),
    "copy_assets": ("datannurpy.exporter", "copy_assets"),
    "run_config": ("datannurpy.config", "run_config"),
    "sanitize_id": ("datannurpy.utils.ids", "sanitize_id"),
}

__all__ = [
    "Catalog",
    "ConfigError",
    "EntityMetadata",
    "Folder",
    "build_dataset_id",
    "build_variable_id",
    "copy_assets",
    "run_config",
    "sanitize_id",
]


def __getattr__(name: str) -> object:
    """Load public runtime objects on first access."""
    if name not in _LAZY_IMPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attribute_name = _LAZY_IMPORTS[name]
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value
