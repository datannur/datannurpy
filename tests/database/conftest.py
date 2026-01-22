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


def create_test_tables(con: ibis.BaseBackend, backend: str | None = None) -> None:
    """Create test tables and view using Ibis (backend-agnostic)."""
    raw_sql: Any = getattr(con, "raw_sql")
    backend = backend or ""

    if backend == "oracle":
        # Oracle: use raw SQL to control identifiers (avoid case-sensitivity issues)
        # Drop existing objects (ignore errors if they don't exist)
        for table in ["EMPLOYEES", "DEPARTMENTS", "EMPTY_TABLE"]:
            try:
                raw_sql(f"DROP TABLE {table} PURGE")
            except Exception:
                pass
        try:
            raw_sql("DROP VIEW EMPLOYEE_SUMMARY")
        except Exception:
            pass

        # Create tables - don't catch errors here so we see what fails
        raw_sql("""
            CREATE TABLE EMPLOYEES (
                id NUMBER(10),
                name VARCHAR2(100),
                department VARCHAR2(100),
                salary NUMBER(10,2),
                hire_date VARCHAR2(20)
            )
        """)
        # Insert data
        for row in [
            (1, "Alice", "Engineering", 75000.0, "2020-01-15"),
            (2, "Bob", "Engineering", 80000.0, "2019-06-01"),
            (3, "Charlie", "Sales", 65000.0, "2021-03-20"),
            (4, "Diana", "Sales", 70000.0, "2020-11-10"),
            (5, "Eve", "HR", 60000.0, "2022-02-28"),
        ]:
            raw_sql(
                f"INSERT INTO EMPLOYEES VALUES ({row[0]}, '{row[1]}', '{row[2]}', {row[3]}, '{row[4]}')"
            )

        raw_sql("""
            CREATE TABLE DEPARTMENTS (
                id NUMBER(10),
                name VARCHAR2(100),
                budget NUMBER(10,2)
            )
        """)
        for row in [
            (1, "Engineering", 500000.0),
            (2, "Sales", 300000.0),
            (3, "HR", 150000.0),
        ]:
            raw_sql(f"INSERT INTO DEPARTMENTS VALUES ({row[0]}, '{row[1]}', {row[2]})")

        raw_sql("""
            CREATE TABLE EMPTY_TABLE (
                id NUMBER(10),
                value VARCHAR2(100)
            )
        """)

        raw_sql("""
            CREATE VIEW EMPLOYEE_SUMMARY AS
            SELECT department, COUNT(*) as count
            FROM EMPLOYEES
            GROUP BY department
        """)
        return

    if backend == "mssql":
        # SQL Server: use raw SQL for proper table/view handling
        # Drop existing objects (ignore errors if they don't exist)
        try:
            raw_sql("DROP VIEW IF EXISTS employee_summary")
        except Exception:
            pass
        for table in ["employees", "departments", "empty_table"]:
            try:
                raw_sql(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass

        raw_sql("""
            CREATE TABLE employees (
                id INT,
                name NVARCHAR(100),
                department NVARCHAR(100),
                salary DECIMAL(10,2),
                hire_date NVARCHAR(20)
            )
        """)
        for row in [
            (1, "Alice", "Engineering", 75000.0, "2020-01-15"),
            (2, "Bob", "Engineering", 80000.0, "2019-06-01"),
            (3, "Charlie", "Sales", 65000.0, "2021-03-20"),
            (4, "Diana", "Sales", 70000.0, "2020-11-10"),
            (5, "Eve", "HR", 60000.0, "2022-02-28"),
        ]:
            raw_sql(
                f"INSERT INTO employees VALUES ({row[0]}, '{row[1]}', '{row[2]}', {row[3]}, '{row[4]}')"
            )

        raw_sql("""
            CREATE TABLE departments (
                id INT,
                name NVARCHAR(100),
                budget DECIMAL(10,2)
            )
        """)
        for row in [
            (1, "Engineering", 500000.0),
            (2, "Sales", 300000.0),
            (3, "HR", 150000.0),
        ]:
            raw_sql(f"INSERT INTO departments VALUES ({row[0]}, '{row[1]}', {row[2]})")

        raw_sql("""
            CREATE TABLE empty_table (
                id INT,
                value NVARCHAR(100)
            )
        """)

        raw_sql("""
            CREATE VIEW employee_summary AS
            SELECT department, COUNT(*) as count
            FROM employees
            GROUP BY department
        """)
        return

    # Non-Oracle/MSSQL backends: use Ibis
    for table in ["employees", "departments", "empty_table"]:
        try:
            con.drop_table(table, force=True)
        except Exception:
            pass

    con.create_table("employees", EMPLOYEES_DATA)
    con.create_table("departments", DEPARTMENTS_DATA)
    con.create_table("empty_table", EMPTY_TABLE_DATA)

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


def drop_test_tables(con: ibis.BaseBackend, backend: str | None = None) -> None:
    """Drop test tables and view if they exist."""
    raw_sql: Any = getattr(con, "raw_sql")
    backend = backend or ""

    if backend == "mssql":
        try:
            raw_sql("DROP VIEW IF EXISTS employee_summary")
        except Exception:
            pass
        for table in ["employees", "departments", "empty_table"]:
            try:
                raw_sql(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass
        return

    if backend == "oracle":
        try:
            raw_sql("DROP VIEW EMPLOYEE_SUMMARY")
        except Exception:
            pass
        for table in ["EMPLOYEES", "DEPARTMENTS", "EMPTY_TABLE"]:
            try:
                raw_sql(f"DROP TABLE {table} PURGE")
            except Exception:
                pass
        return

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
    """Create schemas and tables for schema tests.

    Creates:
    - sales schema: orders, customers tables
    - inventory schema: products table
    - main_table in default schema
    """
    raw_sql: Any = getattr(con, "raw_sql")

    # Schema creation (MySQL uses databases, Oracle uses users as schemas)
    if backend == "mysql":
        raw_sql("CREATE DATABASE IF NOT EXISTS sales")
        raw_sql("CREATE DATABASE IF NOT EXISTS inventory")
    elif backend == "mssql":
        # SQL Server: create schemas within database
        # Drop tables first, then schemas
        for schema in ["sales", "inventory"]:
            for table in ["orders", "customers", "products"]:
                try:
                    raw_sql(f"DROP TABLE IF EXISTS {schema}.{table}")
                except Exception:
                    pass
            try:
                raw_sql(f"DROP SCHEMA IF EXISTS {schema}")
            except Exception:
                pass
            raw_sql(f"CREATE SCHEMA {schema}")
        try:
            raw_sql("DROP TABLE IF EXISTS main_table")
        except Exception:
            pass

        raw_sql("""
            CREATE TABLE sales.orders (
                id INT,
                customer NVARCHAR(100),
                amount DECIMAL(10,2)
            )
        """)
        raw_sql("INSERT INTO sales.orders VALUES (1, 'Alice', 100.0)")
        raw_sql("INSERT INTO sales.orders VALUES (2, 'Bob', 250.50)")
        raw_sql("""
            CREATE TABLE sales.customers (
                id INT,
                name NVARCHAR(100)
            )
        """)
        raw_sql("INSERT INTO sales.customers VALUES (1, 'Alice')")
        raw_sql("INSERT INTO sales.customers VALUES (2, 'Bob')")
        raw_sql("""
            CREATE TABLE inventory.products (
                id INT,
                name NVARCHAR(100),
                stock INT
            )
        """)
        raw_sql("INSERT INTO inventory.products VALUES (1, 'Widget', 100)")
        raw_sql("CREATE TABLE main_table (id INT)")
        raw_sql("INSERT INTO main_table VALUES (1)")
        return
    elif backend == "oracle":
        # Oracle: schemas are users, create them with DBA privileges
        for schema in ["sales", "inventory"]:
            try:
                raw_sql(f"DROP USER {schema} CASCADE")
            except Exception:
                pass
            raw_sql(f"CREATE USER {schema} IDENTIFIED BY test")
            raw_sql(f"GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO {schema}")
        # Oracle: create tables via raw SQL in other users' schemas
        raw_sql("""
            CREATE TABLE sales.orders (
                id NUMBER(10),
                customer VARCHAR2(100),
                amount NUMBER(10,2)
            )
        """)
        raw_sql("INSERT INTO sales.orders VALUES (1, 'Alice', 100.0)")
        raw_sql("INSERT INTO sales.orders VALUES (2, 'Bob', 250.50)")
        raw_sql("""
            CREATE TABLE sales.customers (
                id NUMBER(10),
                name VARCHAR2(100)
            )
        """)
        raw_sql("INSERT INTO sales.customers VALUES (1, 'Alice')")
        raw_sql("INSERT INTO sales.customers VALUES (2, 'Bob')")
        raw_sql("""
            CREATE TABLE inventory.products (
                id NUMBER(10),
                name VARCHAR2(100),
                stock NUMBER(10)
            )
        """)
        raw_sql("INSERT INTO inventory.products VALUES (1, 'Widget', 100)")
        # Main table in current user schema (SYSTEM)
        try:
            raw_sql("DROP TABLE MAIN_TABLE PURGE")
        except Exception:
            pass
        raw_sql("CREATE TABLE MAIN_TABLE (id NUMBER(10))")
        raw_sql("INSERT INTO MAIN_TABLE VALUES (1)")
        return
    else:
        raw_sql("CREATE SCHEMA IF NOT EXISTS sales")
        raw_sql("CREATE SCHEMA IF NOT EXISTS inventory")

    # Drop then create tables (works on all backends except Oracle)
    for schema, table in [
        ("sales", "orders"),
        ("sales", "customers"),
        ("inventory", "products"),
    ]:
        try:
            con.drop_table(table, database=schema, force=True)
        except Exception:
            pass

    con.create_table("orders", ORDERS_DATA, database="sales")
    con.create_table("customers", CUSTOMERS_DATA, database="sales")
    con.create_table("products", PRODUCTS_DATA, database="inventory")

    # Main table in default schema
    try:
        con.drop_table("main_table", force=True)
    except Exception:
        pass
    con.create_table("main_table", pa.table({"id": [1]}))


def drop_schema_tables(con: ibis.BaseBackend, backend: str) -> None:
    """Drop schema test tables and schemas."""
    raw_sql: Any = getattr(con, "raw_sql")

    # Drop tables first (not needed for Oracle since DROP USER CASCADE removes them)
    if backend == "mssql":
        # SQL Server: drop tables then schemas
        for schema, table in [
            ("sales", "orders"),
            ("sales", "customers"),
            ("inventory", "products"),
        ]:
            try:
                raw_sql(f"DROP TABLE IF EXISTS {schema}.{table}")
            except Exception:
                pass
        try:
            raw_sql("DROP TABLE IF EXISTS main_table")
        except Exception:
            pass
        for schema in ["sales", "inventory"]:
            try:
                raw_sql(f"DROP SCHEMA IF EXISTS {schema}")
            except Exception:
                pass
        return
    elif backend == "oracle":
        try:
            raw_sql("DROP TABLE MAIN_TABLE PURGE")
        except Exception:
            pass
    else:
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
    elif backend == "oracle":
        for schema in ["sales", "inventory"]:
            try:
                raw_sql(f"DROP USER {schema} CASCADE")
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
