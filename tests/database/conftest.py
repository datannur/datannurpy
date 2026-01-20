"""Fixtures and test data for database tests."""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest

if TYPE_CHECKING:
    import ibis


# Shared test data (single source of truth)
EMPLOYEES_DATA = pa.table(
    {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "department": ["Engineering", "Engineering", "Sales", "Sales", "HR"],
        "salary": [75000.0, 80000.0, 65000.0, 70000.0, 60000.0],
        "hire_date": [
            "2020-01-15",
            "2019-06-01",
            "2021-03-20",
            "2020-11-10",
            "2022-02-28",
        ],
    }
)

DEPARTMENTS_DATA = pa.table(
    {
        "id": [1, 2, 3],
        "name": ["Engineering", "Sales", "HR"],
        "budget": [500000.0, 300000.0, 150000.0],
    }
)

# Schema test data
ORDERS_DATA = pa.table(
    {
        "id": [1, 2],
        "customer": ["Alice", "Bob"],
        "amount": [100.0, 250.50],
    }
)

CUSTOMERS_DATA = pa.table(
    {
        "id": [1, 2],
        "name": ["Alice", "Bob"],
    }
)

PRODUCTS_DATA = pa.table(
    {
        "id": [1],
        "name": ["Widget"],
        "stock": [100],
    }
)


# Empty table for testing edge cases
EMPTY_TABLE_DATA = pa.table(
    {
        "id": pa.array([], type=pa.int64()),
        "value": pa.array([], type=pa.string()),
    }
)


def create_test_tables(con: ibis.BaseBackend) -> None:
    """Create test tables and view using Ibis (backend-agnostic)."""
    raw_sql: Any = getattr(con, "raw_sql")

    con.create_table("employees", EMPLOYEES_DATA, overwrite=True)
    con.create_table("departments", DEPARTMENTS_DATA, overwrite=True)
    con.create_table("empty_table", EMPTY_TABLE_DATA, overwrite=True)

    # Create a view (standard SQL, works on all backends)
    try:
        raw_sql("DROP VIEW IF EXISTS employee_summary")
    except Exception:
        pass
    raw_sql("""
        CREATE VIEW employee_summary AS
        SELECT department, COUNT(*) as count
        FROM employees
        GROUP BY department
    """)


def drop_test_tables(con: ibis.BaseBackend) -> None:
    """Drop test tables and view if they exist."""
    raw_sql: Any = getattr(con, "raw_sql")

    try:
        raw_sql("DROP VIEW IF EXISTS employee_summary")
    except Exception:
        pass

    for table in ["employees", "departments", "empty_table"]:
        try:
            con.drop_table(table, force=True)
        except Exception:
            pass


def create_schema_tables(con: ibis.BaseBackend, backend: str) -> None:
    """Create schemas and tables for schema tests (backend-agnostic where possible).

    Creates:
    - sales schema: orders, customers tables
    - inventory schema: products table
    - main_table in default schema
    """
    raw_sql: Any = getattr(con, "raw_sql")

    # Schema creation syntax varies slightly but is standard SQL
    if backend == "mysql":
        # MySQL uses databases as schemas, we create them if not exists
        raw_sql("CREATE DATABASE IF NOT EXISTS sales")
        raw_sql("CREATE DATABASE IF NOT EXISTS inventory")
    else:
        # PostgreSQL, DuckDB use CREATE SCHEMA
        raw_sql("CREATE SCHEMA IF NOT EXISTS sales")
        raw_sql("CREATE SCHEMA IF NOT EXISTS inventory")

    # Create tables in schemas using Ibis
    con.create_table("orders", ORDERS_DATA, overwrite=True, database="sales")
    con.create_table("customers", CUSTOMERS_DATA, overwrite=True, database="sales")
    con.create_table("products", PRODUCTS_DATA, overwrite=True, database="inventory")

    # Create table in default schema
    con.create_table("main_table", pa.table({"id": [1]}), overwrite=True)


def drop_schema_tables(con: ibis.BaseBackend, backend: str) -> None:
    """Drop schema test tables and schemas."""
    raw_sql: Any = getattr(con, "raw_sql")

    # Drop tables first
    for schema, table in [
        ("sales", "orders"),
        ("sales", "customers"),
        ("inventory", "products"),
    ]:
        try:
            con.drop_table(table, database=schema, force=True)
        except Exception:
            pass
    try:
        con.drop_table("main_table", force=True)
    except Exception:
        pass

    # Drop schemas
    if backend == "mysql":
        for schema in ["sales", "inventory"]:
            try:
                raw_sql(f"DROP DATABASE IF EXISTS {schema}")
            except Exception:
                pass
    else:
        for schema in ["sales", "inventory"]:
            try:
                raw_sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            except Exception:
                pass


@pytest.fixture
def sample_sqlite_db() -> Generator[Path, None, None]:
    """Create a temporary SQLite database with sample data."""
    import ibis

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    con = ibis.sqlite.connect(db_path)
    create_test_tables(con)

    yield db_path

    db_path.unlink(missing_ok=True)


@pytest.fixture
def duckdb_with_schemas() -> Generator[ibis.BaseBackend, None, None]:
    """Create a DuckDB database with multiple schemas for testing."""
    import ibis

    con = ibis.duckdb.connect(":memory:")
    create_schema_tables(con, "duckdb")
    yield con
    con.disconnect()
