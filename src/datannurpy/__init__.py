"""datannurpy - Python library for datannur catalog metadata management."""

from importlib.metadata import version

__version__ = version("datannurpy")

from .catalog import Catalog
from .config import run_config
from .errors import ConfigError
from .schema import Folder
from .utils.ids import build_dataset_id, build_variable_id, sanitize_id

__all__ = [
    "Catalog",
    "ConfigError",
    "Folder",
    "build_dataset_id",
    "build_variable_id",
    "run_config",
    "sanitize_id",
]
