"""datannurpy - Python library for datannur catalog metadata management."""

from importlib.metadata import version

__version__ = version("datannurpy")

from .catalog import Catalog
from .entities import Folder

__all__ = ["Catalog", "Folder"]
