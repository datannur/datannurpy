"""Tests for database reader functionality."""

from __future__ import annotations

import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from datannurpy import Catalog, Folder
from datannurpy.readers.database import (
    connect,
    list_schemas,
    list_tables,
    parse_connection_string,
    scan_table,
)

if TYPE_CHECKING:
    import ibis


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

    # Create an empty table
    cursor.execute("""
        CREATE TABLE empty_table (
            id INTEGER PRIMARY KEY,
            value TEXT
        )
    """)

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


@pytest.fixture
def duckdb_with_schemas() -> Generator[ibis.BaseBackend, None, None]:
    """Create a DuckDB database with multiple schemas."""
    import ibis

    con = ibis.duckdb.connect(":memory:")

    # Create schemas
    con.raw_sql("CREATE SCHEMA sales")
    con.raw_sql("CREATE SCHEMA inventory")

    # Create tables in sales schema
    con.raw_sql("""
        CREATE TABLE sales.orders (
            id INTEGER PRIMARY KEY,
            customer TEXT,
            amount DECIMAL(10, 2)
        )
    """)
    con.raw_sql("""
        INSERT INTO sales.orders VALUES
        (1, 'Alice', 100.00),
        (2, 'Bob', 250.50)
    """)

    con.raw_sql("""
        CREATE TABLE sales.customers (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    """)
    con.raw_sql("INSERT INTO sales.customers VALUES (1, 'Alice'), (2, 'Bob')")

    # Create tables in inventory schema
    con.raw_sql("""
        CREATE TABLE inventory.products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            stock INTEGER
        )
    """)
    con.raw_sql("INSERT INTO inventory.products VALUES (1, 'Widget', 100)")

    # Create table in default schema (main)
    con.raw_sql("CREATE TABLE main_table (id INTEGER)")

    yield con

    con.disconnect()


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

    def test_connect_with_existing_backend(self, sample_sqlite_db: Path) -> None:
        """Test passing an existing Ibis connection."""
        import ibis

        existing_con = ibis.sqlite.connect(sample_sqlite_db)
        con, backend = connect(existing_con)
        assert con is existing_con
        assert backend == "sqlite"

    def test_connect_nonexistent_file(self) -> None:
        """Test connecting to a non-existent SQLite file creates it."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nonexistent.db"
            # SQLite creates the file if it doesn't exist
            con, backend = connect(f"sqlite:////{db_path}")
            assert backend == "sqlite"
            assert db_path.exists()


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
        tables = list_tables(con, include=["dep*"])
        assert tables == ["departments"]

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

    def test_scan_empty_table(self, sample_sqlite_db: Path) -> None:
        """Test scanning a table with no rows."""
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        variables, row_count, freq_table = scan_table(
            con, "empty_table", infer_stats=True
        )

        assert row_count == 0
        assert len(variables) == 2  # id, value

        # Stats should be None or 0 for empty table
        var_by_name = {v.name: v for v in variables}
        assert var_by_name["id"].nb_distinct is None
        assert var_by_name["value"].nb_missing is None

        # No freq table for empty table
        assert freq_table is None


class TestCatalogAddDatabase:
    """Tests for Catalog.add_database()."""

    def test_add_sqlite_database(self, sample_sqlite_db: Path) -> None:
        catalog = Catalog()
        catalog.add_database(f"sqlite:////{sample_sqlite_db}")

        # Should have 1 folder (database)
        assert len(catalog.folders) == 1
        assert catalog.folders[0].name == sample_sqlite_db.stem

        # Should have 3 datasets (tables)
        assert len(catalog.datasets) == 3
        dataset_names = {d.name for d in catalog.datasets}
        assert dataset_names == {"employees", "departments", "empty_table"}

        # Check delivery_format
        assert all(d.delivery_format == "sqlite" for d in catalog.datasets)

        # Should have variables
        assert len(catalog.variables) == 10  # 5 + 3 + 2 columns

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
        catalog.add_database(
            f"sqlite:////{sample_sqlite_db}", exclude=["departments", "empty_table"]
        )

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


class TestDuckDBWithSchemas:
    """Tests for databases with multiple schemas (using DuckDB)."""

    def test_list_schemas(self, duckdb_with_schemas: ibis.BaseBackend) -> None:
        """Test listing schemas."""
        schemas = list_schemas(duckdb_with_schemas)
        assert "sales" in schemas
        assert "inventory" in schemas
        assert "main" in schemas

    def test_list_tables_in_schema(self, duckdb_with_schemas: ibis.BaseBackend) -> None:
        """Test listing tables in a specific schema."""
        tables = list_tables(duckdb_with_schemas, schema="sales")
        assert "orders" in tables
        assert "customers" in tables
        assert "products" not in tables  # In inventory schema

    def test_scan_table_in_schema(self, duckdb_with_schemas: ibis.BaseBackend) -> None:
        """Test scanning a table in a schema."""
        variables, row_count, _ = scan_table(
            duckdb_with_schemas, "orders", schema="sales"
        )
        assert row_count == 2
        var_names = {v.name for v in variables}
        assert var_names == {"id", "customer", "amount"}

    def test_catalog_with_schemas(self, duckdb_with_schemas: ibis.BaseBackend) -> None:
        """Test Catalog.add_database with multiple schemas."""
        catalog = Catalog()
        catalog.add_database(
            duckdb_with_schemas,
            folder=Folder(id="mydb", name="My Database"),
        )

        # Should have root folder + schema folders (sales, inventory, main)
        folder_ids = {f.id for f in catalog.folders}
        assert "mydb" in folder_ids
        assert "mydb---sales" in folder_ids
        assert "mydb---inventory" in folder_ids

        # Check schema folders have correct parent
        sales_folder = next(f for f in catalog.folders if f.id == "mydb---sales")
        assert sales_folder.parent_id == "mydb"
        assert sales_folder.name == "sales"

        # Check datasets are in correct folders
        orders_dataset = next(d for d in catalog.datasets if d.name == "orders")
        assert orders_dataset.folder_id == "mydb---sales"

        products_dataset = next(d for d in catalog.datasets if d.name == "products")
        assert products_dataset.folder_id == "mydb---inventory"

    def test_catalog_single_schema(self, duckdb_with_schemas: ibis.BaseBackend) -> None:
        """Test Catalog.add_database with a single schema specified."""
        catalog = Catalog()
        catalog.add_database(
            duckdb_with_schemas,
            folder=Folder(id="sales_db", name="Sales DB"),
            schema="sales",
        )

        # Should have only root folder (no sub-folders when single schema)
        assert len(catalog.folders) == 1
        assert catalog.folders[0].id == "sales_db"

        # Should only have tables from sales schema
        dataset_names = {d.name for d in catalog.datasets}
        assert dataset_names == {"orders", "customers"}

    def test_catalog_schema_include_exclude(
        self, duckdb_with_schemas: ibis.BaseBackend
    ) -> None:
        """Test include/exclude with schemas."""
        catalog = Catalog()
        catalog.add_database(
            duckdb_with_schemas,
            folder=Folder(id="mydb", name="My DB"),
            schema="sales",
            include=["orders"],
        )

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "orders"
