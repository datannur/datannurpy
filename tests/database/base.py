"""Base test classes for database backends.

This module provides:
- BaseDatabaseTests: Common tests for all database backends
- BaseSchemaTests: Common tests for backends that support schemas (PostgreSQL, MySQL, DuckDB)

Subclasses only need to implement the required fixtures.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from datannurpy import Catalog, Folder
from datannurpy.readers.database import connect, list_schemas, list_tables, scan_table

if TYPE_CHECKING:
    from pathlib import Path

    import ibis


class BaseDatabaseTests(ABC):
    """Base class for database backend tests.

    Subclasses must provide fixtures that return a tuple of:
    - ibis.BaseBackend: The database connection
    - str: The expected backend name
    - str: The expected delivery_format
    """

    @pytest.fixture
    @abstractmethod
    def db(self) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        """Provide database connection, backend name, and delivery format."""
        ...

    @pytest.fixture
    @abstractmethod
    def db_with_employees(
        self,
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        """Provide database with employees table (5 rows) and departments table."""
        ...

    def test_connect(self, db: tuple[ibis.BaseBackend, str, str]) -> None:
        """Test database connection works."""
        con, expected_backend, _ = db
        assert con is not None
        assert con.name == expected_backend

    def test_list_tables(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test listing tables."""
        con, _, _ = db_with_employees
        tables = list_tables(con)
        assert "employees" in tables
        assert "departments" in tables

    def test_list_tables_include(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test listing tables with include filter."""
        con, _, _ = db_with_employees
        tables = list_tables(con, include=["employees"])
        assert tables == ["employees"]

    def test_list_tables_include_wildcard(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test listing tables with wildcard include."""
        con, _, _ = db_with_employees
        tables = list_tables(con, include=["dep*"])
        assert tables == ["departments"]

    def test_list_tables_exclude(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test listing tables with exclude filter."""
        con, _, _ = db_with_employees
        tables = list_tables(con, exclude=["departments"])
        assert "employees" in tables
        assert "departments" not in tables

    def test_scan_table(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test scanning a table."""
        con, _, _ = db_with_employees
        variables, row_count, _ = scan_table(con, "employees")

        assert row_count == 5
        assert len(variables) == 5  # id, name, department, salary, hire_date

        var_names = {v.name for v in variables}
        assert var_names == {"id", "name", "department", "salary", "hire_date"}

    def test_scan_table_types(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test that variable types are correctly inferred."""
        con, _, _ = db_with_employees
        variables, _, _ = scan_table(con, "employees")

        var_by_name = {v.name: v for v in variables}
        assert var_by_name["id"].type == "integer"
        assert var_by_name["name"].type == "string"
        # salary can be float or decimal depending on backend
        assert var_by_name["salary"].type in ("float", "decimal")

    def test_scan_table_with_stats(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test scanning with statistics."""
        con, backend, _ = db_with_employees
        variables, _, _ = scan_table(con, "employees", infer_stats=True)

        var_by_name = {v.name: v for v in variables}
        # Oracle doesn't support nunique (CLOB issues), so nb_distinct is None
        if backend == "oracle":
            assert var_by_name["department"].nb_distinct is None
        else:
            # department has 3 distinct values
            assert var_by_name["department"].nb_distinct == 3
        assert var_by_name["department"].nb_missing == 0

    def test_scan_table_without_stats(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test scanning without statistics."""
        con, _, _ = db_with_employees
        variables, _, _ = scan_table(con, "employees", infer_stats=False)

        var_by_name = {v.name: v for v in variables}
        assert var_by_name["name"].nb_distinct is None
        assert var_by_name["name"].nb_missing is None

    def test_scan_table_with_sample(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test scanning with sample size."""
        con, backend, _ = db_with_employees
        variables, row_count, _ = scan_table(con, "employees", sample_size=2)

        # Row count should still be the full count
        assert row_count == 5

        # Stats are computed on sample
        var_by_name = {v.name: v for v in variables}
        # Oracle doesn't support nunique (CLOB issues)
        if backend != "oracle":
            assert var_by_name["name"].nb_distinct is not None
            assert var_by_name["name"].nb_distinct <= 2

    def test_catalog_add_database(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test adding database to catalog."""
        con, _, delivery_format = db_with_employees

        catalog = Catalog()
        catalog.add_database(con, folder=Folder(id="testdb", name="Test DB"))

        # Should have datasets
        assert len(catalog.datasets) >= 2
        dataset_names = {d.name for d in catalog.datasets}
        assert "employees" in dataset_names
        assert "departments" in dataset_names

        # Check delivery_format
        emp_dataset = next(d for d in catalog.datasets if d.name == "employees")
        assert emp_dataset.delivery_format == delivery_format

    def test_catalog_with_include(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test catalog with include filter."""
        con, _, _ = db_with_employees

        catalog = Catalog()
        catalog.add_database(
            con,
            folder=Folder(id="testdb", name="Test DB"),
            include=["employees"],
        )

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "employees"

    def test_catalog_with_exclude(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test catalog with exclude filter."""
        con, _, _ = db_with_employees

        catalog = Catalog()
        catalog.add_database(
            con,
            folder=Folder(id="testdb", name="Test DB"),
            exclude=["departments", "empty_table"],
        )

        dataset_names = {d.name for d in catalog.datasets}
        assert "employees" in dataset_names
        assert "departments" not in dataset_names

    def test_connect_with_existing_backend(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test passing an existing Ibis connection to connect()."""
        existing_con, expected_backend, _ = db_with_employees
        con, backend = connect(existing_con)
        assert con is existing_con
        assert backend == expected_backend

    def test_scan_empty_table(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test scanning a table with no rows."""
        con, _, _ = db_with_employees
        variables, row_count, freq_table = scan_table(
            con, "empty_table", infer_stats=True
        )
        assert row_count == 0
        assert len(variables) == 2
        assert freq_table is None

    def test_catalog_export(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str], tmp_path: Path
    ) -> None:
        """Test full catalog export to JSON files."""
        con, _, _ = db_with_employees
        catalog = Catalog()
        catalog.add_database(con)
        catalog.write(tmp_path)

        assert (tmp_path / "folder.json").exists()
        assert (tmp_path / "dataset.json").exists()
        assert (tmp_path / "variable.json").exists()

    def test_views_not_included(
        self, db_with_employees: tuple[ibis.BaseBackend, str, str]
    ) -> None:
        """Test that views are not included in table list."""
        con, _, _ = db_with_employees
        tables = list_tables(con)
        assert "employee_summary" not in tables


class BaseSchemaTests(ABC):
    """Base class for schema-related tests.

    Tests for databases that support schemas (PostgreSQL, MySQL, DuckDB).
    SQLite does not support schemas and should not inherit from this class.

    Subclasses must provide a fixture that returns a tuple of:
    - ibis.BaseBackend: The database connection with schemas created
    - str: The backend name
    """

    @pytest.fixture
    @abstractmethod
    def db_with_schemas(
        self,
    ) -> Generator[tuple[ibis.BaseBackend, str], None, None]:
        """Provide database with sales/inventory schemas and test tables."""
        ...

    def test_list_schemas(self, db_with_schemas: tuple[ibis.BaseBackend, str]) -> None:
        """Test listing schemas."""
        con, _ = db_with_schemas
        schemas = list_schemas(con)
        assert "sales" in schemas
        assert "inventory" in schemas

    def test_list_tables_in_schema(
        self, db_with_schemas: tuple[ibis.BaseBackend, str]
    ) -> None:
        """Test listing tables in a specific schema."""
        con, _ = db_with_schemas
        tables = list_tables(con, schema="sales")
        assert "orders" in tables
        assert "customers" in tables
        assert "products" not in tables

    def test_scan_table_in_schema(
        self, db_with_schemas: tuple[ibis.BaseBackend, str]
    ) -> None:
        """Test scanning a table in a schema."""
        con, _ = db_with_schemas
        variables, row_count, _ = scan_table(con, "orders", schema="sales")
        assert row_count == 2
        var_names = {v.name for v in variables}
        assert var_names == {"id", "customer", "amount"}

    def test_catalog_with_multiple_schemas(
        self, db_with_schemas: tuple[ibis.BaseBackend, str]
    ) -> None:
        """Test Catalog.add_database with multiple schemas."""
        con, _ = db_with_schemas
        catalog = Catalog()
        catalog.add_database(
            con,
            folder=Folder(id="mydb", name="My Database"),
        )

        # Should have root folder + schema folders
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

    def test_catalog_single_schema(
        self, db_with_schemas: tuple[ibis.BaseBackend, str]
    ) -> None:
        """Test Catalog.add_database with a single schema specified."""
        con, _ = db_with_schemas
        catalog = Catalog()
        catalog.add_database(
            con,
            folder=Folder(id="sales_db", name="Sales DB"),
            schema="sales",
        )

        assert len(catalog.folders) == 1
        assert catalog.folders[0].id == "sales_db"

        dataset_names = {d.name for d in catalog.datasets}
        assert dataset_names == {"orders", "customers"}
