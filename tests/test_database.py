"""Tests for database reader functionality."""

from __future__ import annotations

import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from datannurpy import Catalog, Folder
from datannurpy.readers.database import (
    connect,
    list_tables,
    parse_connection_string,
    scan_table,
)


@pytest.fixture
def sample_sqlite_db() -> Generator[Path, None, None]:
    """Create a temporary SQLite database with sample data."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create employees table
    cursor.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department TEXT,
            salary REAL,
            hire_date TEXT
        )
    """)
    cursor.executemany(
        "INSERT INTO employees (name, department, salary, hire_date) VALUES (?, ?, ?, ?)",
        [
            ("Alice", "Engineering", 75000.0, "2020-01-15"),
            ("Bob", "Engineering", 80000.0, "2019-06-01"),
            ("Charlie", "Sales", 65000.0, "2021-03-20"),
            ("Diana", "Sales", 70000.0, "2020-11-10"),
            ("Eve", "HR", 60000.0, "2022-02-28"),
        ],
    )

    # Create departments table
    cursor.execute("""
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            budget REAL
        )
    """)
    cursor.executemany(
        "INSERT INTO departments (name, budget) VALUES (?, ?)",
        [
            ("Engineering", 500000.0),
            ("Sales", 300000.0),
            ("HR", 150000.0),
        ],
    )

    # Create a view (should not be included by default)
    cursor.execute("""
        CREATE VIEW employee_summary AS
        SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
    """)

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    db_path.unlink(missing_ok=True)


class TestParseConnectionString:
    """Tests for connection string parsing."""

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

    def test_unsupported_scheme(self) -> None:
        with pytest.raises(ValueError, match="Unsupported database scheme"):
            parse_connection_string("oracle://user:pass@host/db")


class TestConnect:
    """Tests for database connection."""

    def test_sqlite_connect(self, sample_sqlite_db: Path) -> None:
        con, backend = connect(f"sqlite:////{sample_sqlite_db}")
        assert backend == "sqlite"
        tables = con.list_tables()
        assert "employees" in tables
        assert "departments" in tables


class TestListTables:
    """Tests for table listing."""

    def test_list_all_tables(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        tables = list_tables(con)
        assert "employees" in tables
        assert "departments" in tables
        # Views should NOT be included
        assert "employee_summary" not in tables

    def test_include_filter(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        tables = list_tables(con, include=["employees"])
        assert tables == ["employees"]

    def test_include_wildcard(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        tables = list_tables(con, include=["emp*"])
        assert tables == ["employees"]

    def test_exclude_filter(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        tables = list_tables(con, exclude=["departments"])
        assert "employees" in tables
        assert "departments" not in tables

    def test_exclude_wildcard(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        tables = list_tables(con, exclude=["dep*"])
        assert "employees" in tables
        assert "departments" not in tables


class TestScanTable:
    """Tests for table scanning."""

    def test_scan_employees(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        variables, row_count, freq_table = scan_table(con, "employees")

        assert row_count == 5
        assert len(variables) == 5  # id, name, department, salary, hire_date

        # Check variable names
        var_names = {v.name for v in variables}
        assert var_names == {"id", "name", "department", "salary", "hire_date"}

        # Check types
        var_by_name = {v.name: v for v in variables}
        assert var_by_name["id"].type == "integer"
        assert var_by_name["name"].type == "string"
        assert var_by_name["salary"].type == "float"

    def test_scan_with_stats(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        variables, row_count, _ = scan_table(con, "employees", infer_stats=True)

        var_by_name = {v.name: v for v in variables}

        # department has 3 distinct values
        assert var_by_name["department"].nb_distinct == 3
        assert var_by_name["department"].nb_missing == 0

        # All names are unique
        assert var_by_name["name"].nb_distinct == 5

    def test_scan_without_stats(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        variables, row_count, _ = scan_table(con, "employees", infer_stats=False)

        var_by_name = {v.name: v for v in variables}
        assert var_by_name["name"].nb_distinct is None
        assert var_by_name["name"].nb_missing is None

    def test_scan_with_freq_threshold(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        variables, row_count, freq_table = scan_table(
            con, "employees", freq_threshold=10
        )

        # freq_table should exist for columns with <= 10 distinct values
        assert freq_table is not None

    def test_scan_with_sample_size(self, sample_sqlite_db: Path) -> None:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        variables, row_count, _ = scan_table(con, "employees", sample_size=2)

        # Row count should still be the full count
        assert row_count == 5

        # Stats are computed on sample, so distinct count may be lower
        var_by_name = {v.name: v for v in variables}
        # With only 2 rows sampled, we can't have more than 2 distinct
        assert var_by_name["name"].nb_distinct is not None
        assert var_by_name["name"].nb_distinct <= 2


class TestCatalogAddDatabase:
    """Tests for Catalog.add_database()."""

    def test_add_sqlite_database(self, sample_sqlite_db: Path) -> None:
        catalog = Catalog()
        catalog.add_database(f"sqlite:////{sample_sqlite_db}")

        # Should have 1 folder (database)
        assert len(catalog.folders) == 1
        assert catalog.folders[0].name == sample_sqlite_db.stem

        # Should have 2 datasets (tables)
        assert len(catalog.datasets) == 2
        dataset_names = {d.name for d in catalog.datasets}
        assert dataset_names == {"employees", "departments"}

        # Check delivery_format
        assert all(d.delivery_format == "sqlite" for d in catalog.datasets)

        # Should have variables
        assert len(catalog.variables) == 8  # 5 + 3 columns

    def test_add_database_with_custom_folder(self, sample_sqlite_db: Path) -> None:
        catalog = Catalog()
        folder = Folder(id="my_source", name="My Data Source")
        catalog.add_database(f"sqlite:////{sample_sqlite_db}", folder=folder)

        assert catalog.folders[0].id == "my_source"
        assert catalog.folders[0].name == "My Data Source"

        # Dataset IDs should use folder prefix
        assert all(d.id.startswith("my_source---") for d in catalog.datasets)

    def test_add_database_with_include(self, sample_sqlite_db: Path) -> None:
        catalog = Catalog()
        catalog.add_database(f"sqlite:////{sample_sqlite_db}", include=["employees"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "employees"

    def test_add_database_with_exclude(self, sample_sqlite_db: Path) -> None:
        catalog = Catalog()
        catalog.add_database(f"sqlite:////{sample_sqlite_db}", exclude=["departments"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "employees"

    def test_add_database_no_stats(self, sample_sqlite_db: Path) -> None:
        catalog = Catalog()
        catalog.add_database(f"sqlite:////{sample_sqlite_db}", infer_stats=False)

        # Variables should have no stats
        for var in catalog.variables:
            assert var.nb_distinct is None
            assert var.nb_missing is None

    def test_add_database_with_sample(self, sample_sqlite_db: Path) -> None:
        catalog = Catalog()
        catalog.add_database(f"sqlite:////{sample_sqlite_db}", sample_size=2)

        # Should still have correct row counts
        emp_dataset = next(d for d in catalog.datasets if d.name == "employees")
        assert emp_dataset.nb_row == 5  # Full count, not sampled

    def test_export_database_catalog(
        self, sample_sqlite_db: Path, tmp_path: Path
    ) -> None:
        catalog = Catalog()
        catalog.add_database(f"sqlite:////{sample_sqlite_db}")
        catalog.write(tmp_path)

        # Check output files
        assert (tmp_path / "folder.json").exists()
        assert (tmp_path / "dataset.json").exists()
        assert (tmp_path / "variable.json").exists()
