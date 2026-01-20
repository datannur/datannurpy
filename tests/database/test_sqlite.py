"""SQLite backend tests."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from datannurpy.readers.database import connect

from .base import BaseDatabaseTests

if TYPE_CHECKING:
    import ibis


class TestSQLite(BaseDatabaseTests):
    """SQLite backend tests."""

    @pytest.fixture
    def db(
        self, sample_sqlite_db: Path
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        yield con, "sqlite", "sqlite"

    @pytest.fixture
    def db_with_employees(
        self, sample_sqlite_db: Path
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        yield con, "sqlite", "sqlite"
