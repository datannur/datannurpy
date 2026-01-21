"""Connection string parsing tests."""

from __future__ import annotations

import pytest

from datannurpy.readers.database import parse_connection_string


class TestParseConnectionString:
    """Tests for connection string parsing (all backends)."""

    def test_sqlite_relative_path(self) -> None:
        backend, kwargs = parse_connection_string("sqlite:///data.db")
        assert backend == "sqlite"
        assert kwargs["path"] == "data.db"

    def test_sqlite_absolute_path(self) -> None:
        backend, kwargs = parse_connection_string("sqlite:////tmp/data.db")
        assert backend == "sqlite"
        assert kwargs["path"] == "/tmp/data.db"

    def test_postgres_full(self) -> None:
        backend, kwargs = parse_connection_string(
            "postgresql://user:pass@localhost:5432/mydb"
        )
        assert backend == "postgres"
        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == "5432"
        assert kwargs["user"] == "user"
        assert kwargs["password"] == "pass"
        assert kwargs["database"] == "mydb"

    def test_mysql_full(self) -> None:
        backend, kwargs = parse_connection_string(
            "mysql://root:secret@db.example.com/app"
        )
        assert backend == "mysql"
        assert kwargs["host"] == "db.example.com"
        assert kwargs["user"] == "root"
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "app"

    def test_oracle_full(self) -> None:
        backend, kwargs = parse_connection_string(
            "oracle://system:secret@db.example.com:1521/ORCL"
        )
        assert backend == "oracle"
        assert kwargs["host"] == "db.example.com"
        assert kwargs["port"] == "1521"
        assert kwargs["user"] == "system"
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "ORCL"

    def test_unsupported_scheme(self) -> None:
        with pytest.raises(ValueError, match="Unsupported database scheme"):
            parse_connection_string("mssql://user:pass@host/db")
