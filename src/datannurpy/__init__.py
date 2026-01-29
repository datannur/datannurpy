"""datannurpy - Python library for datannur catalog metadata management."""

from importlib.metadata import version

__version__ = version("datannurpy")

from .catalog import Catalog
from .entities import Folder
from .utils.ids import build_dataset_id, build_variable_id, sanitize_id

__all__ = [
    "Catalog",
    "Folder",
    "build_dataset_id",
    "build_variable_id",
    "sanitize_id",
]
