"""Tests for database introspection (PK, FK, comments, constraints)."""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest

from datannurpy import Catalog, Folder
from datannurpy.scanner.database import connect
from datannurpy.scanner.db_introspect import (
    TableMetadata,
    introspect_table,
)
from datannurpy.schema import Variable
from datannurpy.utils.db_enrich import update_cached_metadata


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Minimal cursor that returns pre-defined rows."""

    def __init__(self, rows: list) -> None:
        self._rows = rows

    def fetchall(self) -> list:
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _RaiseCursor:
    """Cursor that raises on access."""

    def fetchall(self):
        raise RuntimeError("boom")

    def fetchone(self):
        raise RuntimeError("boom")


def _make_con(cursors: list):
    """Return a mock connection whose raw_sql returns cursors in order."""
    it = iter(cursors)

    class _Con:
        def raw_sql(self, _q: str):
            return next(it)

    return _Con()


def _fc(rows=None):
    """Shorthand for _FakeCursor."""
    return _FakeCursor(rows or [])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_with_constraints() -> Generator[Path, None, None]:
    """Create SQLite DB with PK, FK, UNIQUE, NOT NULL, INDEX constraints."""
    import sqlite3

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, budget REAL
        )
    """)
    conn.execute("INSERT INTO departments VALUES (1, 'Engineering', 500000)")
    conn.execute("INSERT INTO departments VALUES (2, 'Sales', 300000)")
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL,
            department_id INTEGER NOT NULL, salary REAL, email TEXT UNIQUE,
            FOREIGN KEY (department_id) REFERENCES departments(id)
        )
    """)
    conn.execute("INSERT INTO employees VALUES (1, 'A', 1, 75000, 'a@co.com')")
    conn.execute("INSERT INTO employees VALUES (2, 'B', 1, 80000, 'b@co.com')")
    conn.execute("INSERT INTO employees VALUES (3, 'C', 2, 65000, 'c@co.com')")
    conn.execute("CREATE INDEX idx_emp_name ON employees(name)")
    conn.execute("CREATE INDEX idx_emp_dept ON employees(department_id)")
    conn.execute("""
        CREATE TABLE order_items (
            order_id INTEGER NOT NULL, item_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL, PRIMARY KEY (order_id, item_id)
        )
    """)
    conn.execute("INSERT INTO order_items VALUES (1, 1, 5)")
    conn.commit()
    conn.close()
    yield db_path
    db_path.unlink(missing_ok=True)


@pytest.fixture
def duckdb_with_constraints():
    """Create DuckDB with PK, FK, UNIQUE, NOT NULL constraints and comments."""
    import ibis

    con = ibis.duckdb.connect(":memory:")
    raw = con.raw_sql
    raw(
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL UNIQUE, budget DOUBLE)"
    )
    raw("INSERT INTO departments VALUES (1, 'Engineering', 500000)")
    raw("INSERT INTO departments VALUES (2, 'Sales', 300000)")
    raw(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, department_id INTEGER NOT NULL REFERENCES departments(id), salary DOUBLE, email VARCHAR UNIQUE)"
    )
    raw("INSERT INTO employees VALUES (1, 'A', 1, 75000, 'a@co.com')")
    raw("INSERT INTO employees VALUES (2, 'B', 1, 80000, 'b@co.com')")
    raw("INSERT INTO employees VALUES (3, 'C', 2, 65000, 'c@co.com')")
    raw("COMMENT ON TABLE departments IS 'Company departments'")
    raw("COMMENT ON COLUMN departments.name IS 'Department name'")
    raw("COMMENT ON COLUMN departments.budget IS 'Annual budget'")
    yield con
    con.disconnect()


@pytest.fixture
def sqlite_catalog(sqlite_with_constraints: Path) -> Catalog:
    """Catalog populated from SQLite DB with constraints."""
    catalog = Catalog()
    catalog.add_database(
        f"sqlite:////{sqlite_with_constraints}", Folder(id="db", name="Test DB")
    )
    return catalog


def _emp_vars(catalog: Catalog) -> dict[str, Variable]:
    """Return {name: Variable} for employees table."""
    return {v.name: v for v in catalog.variable.all() if "employees" in v.dataset_id}


# ===========================================================================
# Unit tests: SQLite + DuckDB (real backends)
# ===========================================================================


class TestSQLiteIntrospection:
    def test_simple_pk(self, sqlite_with_constraints: Path) -> None:
        con, _ = connect(f"sqlite:////{sqlite_with_constraints}")
        assert introspect_table(con, "sqlite", None, "departments").pk_map == {"id": 1}
        con.disconnect()

    def test_composite_pk(self, sqlite_with_constraints: Path) -> None:
        con, _ = connect(f"sqlite:////{sqlite_with_constraints}")
        assert introspect_table(con, "sqlite", None, "order_items").pk_map == {
            "order_id": 1,
            "item_id": 2,
        }
        con.disconnect()

    def test_no_pk(self) -> None:
        import ibis

        con = ibis.sqlite.connect(":memory:")
        con.raw_sql("CREATE TABLE nopk (a TEXT, b TEXT)")
        assert introspect_table(con, "sqlite", None, "nopk").pk_map == {}
        con.disconnect()

    def test_fk(self, sqlite_with_constraints: Path) -> None:
        con, _ = connect(f"sqlite:////{sqlite_with_constraints}")
        fks = introspect_table(con, "sqlite", None, "employees").fks
        assert len(fks) == 1
        assert (fks[0].local_col, fks[0].ref_table, fks[0].ref_col) == (
            "department_id",
            "departments",
            "id",
        )
        con.disconnect()

    def test_no_fk(self, sqlite_with_constraints: Path) -> None:
        con, _ = connect(f"sqlite:////{sqlite_with_constraints}")
        assert introspect_table(con, "sqlite", None, "departments").fks == []
        con.disconnect()

    def test_comments_empty(self, sqlite_with_constraints: Path) -> None:
        con, _ = connect(f"sqlite:////{sqlite_with_constraints}")
        meta = introspect_table(con, "sqlite", None, "employees")
        assert meta.table_comment is None and meta.col_comments == {}
        con.disconnect()

    def test_not_null(self, sqlite_with_constraints: Path) -> None:
        con, _ = connect(f"sqlite:////{sqlite_with_constraints}")
        assert {"name", "department_id"} <= introspect_table(
            con, "sqlite", None, "employees"
        ).not_null
        con.disconnect()

    def test_unique(self, sqlite_with_constraints: Path) -> None:
        con, _ = connect(f"sqlite:////{sqlite_with_constraints}")
        assert "email" in introspect_table(con, "sqlite", None, "employees").unique
        assert "name" in introspect_table(con, "sqlite", None, "departments").unique
        con.disconnect()

    def test_indexed(self, sqlite_with_constraints: Path) -> None:
        con, _ = connect(f"sqlite:////{sqlite_with_constraints}")
        assert {"name", "department_id"} <= introspect_table(
            con, "sqlite", None, "employees"
        ).indexed
        con.disconnect()

    def test_auto_increment(self, sqlite_with_constraints: Path) -> None:
        con, _ = connect(f"sqlite:////{sqlite_with_constraints}")
        assert "id" in introspect_table(con, "sqlite", None, "employees").auto_inc
        con.disconnect()

    def test_non_integer_pk_not_autoincrement(self) -> None:
        import ibis

        con = ibis.sqlite.connect(":memory:")
        con.raw_sql("CREATE TABLE t (code TEXT PRIMARY KEY, val INTEGER)")
        assert introspect_table(con, "sqlite", None, "t").auto_inc == set()
        con.disconnect()


class TestDuckDBIntrospection:
    def test_pk(self, duckdb_with_constraints) -> None:
        assert introspect_table(
            duckdb_with_constraints, "duckdb", None, "departments"
        ).pk_map == {"id": 1}

    def test_no_pk(self) -> None:
        import ibis

        con = ibis.duckdb.connect(":memory:")
        con.raw_sql("CREATE TABLE nopk (a TEXT, b TEXT)")
        assert introspect_table(con, "duckdb", None, "nopk").pk_map == {}
        con.disconnect()

    def test_fk(self, duckdb_with_constraints) -> None:
        fks = introspect_table(duckdb_with_constraints, "duckdb", None, "employees").fks
        assert len(fks) == 1
        assert (fks[0].local_col, fks[0].ref_table, fks[0].ref_col) == (
            "department_id",
            "departments",
            "id",
        )

    def test_comments(self, duckdb_with_constraints) -> None:
        meta = introspect_table(duckdb_with_constraints, "duckdb", None, "departments")
        assert meta.table_comment == "Company departments"
        assert meta.col_comments == {
            "name": "Department name",
            "budget": "Annual budget",
        }

    def test_no_comments(self, duckdb_with_constraints) -> None:
        meta = introspect_table(duckdb_with_constraints, "duckdb", None, "employees")
        assert meta.table_comment is None and meta.col_comments == {}

    def test_not_null(self, duckdb_with_constraints) -> None:
        meta = introspect_table(duckdb_with_constraints, "duckdb", None, "employees")
        assert {"name", "department_id"} <= meta.not_null
        assert "salary" not in meta.not_null

    def test_unique(self, duckdb_with_constraints) -> None:
        assert (
            "email"
            in introspect_table(
                duckdb_with_constraints, "duckdb", None, "employees"
            ).unique
        )

    def test_multi_column_unique_skipped(self) -> None:
        # DuckDB: 7 cursors (pk, fk, comments_tbl, comments_col, not_null, unique, auto_inc)
        con = _make_con(
            [
                _fc(),
                _fc(),
                _fc(),
                _fc(),
                _fc(),
                _FakeCursor([("t", ["a", "b"]), ("t", ["email"])]),
                _fc(),
            ]
        )
        assert introspect_table(con, "duckdb", None, "t").unique == {"email"}


# ===========================================================================
# Mock-based tests: no raw_sql, exceptions
# ===========================================================================


class TestNoRawSql:
    def test_returns_empty_metadata(self) -> None:
        meta = introspect_table(object(), "postgres", None, "t")
        assert meta == TableMetadata()


class TestExceptionHandling:
    def test_all_queries_raise(self) -> None:
        """Each query fails independently; returns empty TableMetadata."""

        class _BadCon:
            def raw_sql(self, _q: str):
                raise RuntimeError("boom")

        meta = introspect_table(_BadCon(), "postgres", None, "t")
        assert meta == TableMetadata()

    def test_sqlite_partial_failure(self) -> None:
        """If index_list fails, pk/not_null/fk still work."""
        con = _make_con(
            [
                _FakeCursor([(0, "id", "INTEGER", 1, None, 1)]),  # table_info
                _fc(),  # foreign_key_list
                _RaiseCursor(),  # index_list
            ]
        )
        meta = introspect_table(con, "sqlite", None, "t")
        assert meta.pk_map == {"id": 1}
        assert "id" in meta.not_null
        assert meta.indexed == set()

    def test_sqlite_table_info_fails(self) -> None:
        """If table_info fails, pk/not_null/auto_inc all empty."""
        con = _make_con(
            [
                _RaiseCursor(),  # table_info fails
                _fc(),  # foreign_key_list
                _fc(),  # index_list (no indexes)
            ]
        )
        meta = introspect_table(con, "sqlite", None, "t")
        assert meta.pk_map == {} and meta.not_null == set()

    def test_sqlite_fk_list_fails(self) -> None:
        """If foreign_key_list fails, fks empty."""
        con = _make_con(
            [
                _fc(),  # table_info
                _RaiseCursor(),  # foreign_key_list fails
                _fc(),  # index_list
            ]
        )
        meta = introspect_table(con, "sqlite", None, "t")
        assert meta.fks == []

    def test_oracle_partial_failure(self) -> None:
        """If PK+UNIQUE query fails, FK and others still work."""
        con = _make_con(
            [
                _RaiseCursor(),  # pk+unique fails
                _FakeCursor([("T", "DEPT_ID", "HR", "DEPARTMENTS", "ID")]),  # fk
                _fc(),
                _fc(),  # comments
                _FakeCursor([("T", "COL_A", "N", None)]),  # not_null+auto_inc
                _fc(),  # indexed
            ]
        )
        meta = introspect_table(con, "oracle", "HR", "t")
        assert meta.pk_map == {}
        assert meta.unique == set()
        assert len(meta.fks) == 1
        assert meta.not_null == {"col_a"}

    def test_oracle_each_query_can_fail(self) -> None:
        """Each Oracle try/except block handles failures independently."""
        # Oracle uses 6 cursors: pk+unique, fk, tab_comment, col_comments,
        #                        not_null+auto_inc, indexed

        # fk fails
        con = _make_con([_fc(), _RaiseCursor(), _fc(), _fc(), _fc(), _fc()])
        assert introspect_table(con, "oracle", None, "t").fks == []
        # comments fail
        con = _make_con([_fc(), _fc(), _RaiseCursor(), _fc(), _fc()])
        assert introspect_table(con, "oracle", None, "t").table_comment is None
        # not_null+auto_inc fails
        con = _make_con([_fc(), _fc(), _fc(), _fc(), _RaiseCursor(), _fc()])
        meta = introspect_table(con, "oracle", None, "t")
        assert meta.not_null == set()
        assert meta.auto_inc == set()
        # indexed fails
        con = _make_con([_fc(), _fc(), _fc(), _fc(), _fc(), _RaiseCursor()])
        assert introspect_table(con, "oracle", None, "t").indexed == set()


# ===========================================================================
# Mock-based tests: Oracle
# ===========================================================================


class TestOracleMock:
    def test_with_schema(self) -> None:
        con = _make_con(
            [
                _FakeCursor([("T", "P", "ID", 1), ("T", "U", "EMAIL", 1)]),  # pk+unique
                _FakeCursor([("T", "DEPT_ID", "HR", "DEPARTMENTS", "ID")]),  # fk
                _FakeCursor([("T", "Table desc")]),  # table comment
                _FakeCursor(
                    [("T", "COL_A", "Column A"), ("T", "COL_B", None)]
                ),  # col comments
                _FakeCursor(
                    [
                        ("T", "ID", "N", "ID"),
                        ("T", "NAME", "N", None),
                        ("T", "EMAIL", "Y", None),
                    ]
                ),  # not_null+auto_inc
                _FakeCursor([("T", "COL_A"), ("T", "COL_B")]),  # indexed
            ]
        )
        meta = introspect_table(con, "oracle", "HR", "t")
        assert meta.pk_map == {"id": 1}
        assert len(meta.fks) == 1
        fk = meta.fks[0]
        assert (fk.local_col, fk.ref_schema, fk.ref_table, fk.ref_col) == (
            "dept_id",
            "hr",
            "departments",
            "id",
        )
        assert meta.table_comment == "Table desc"
        assert meta.col_comments == {"col_a": "Column A"}
        assert meta.not_null == {"id", "name"}
        assert meta.unique == {"email"}
        assert meta.indexed == {"col_a", "col_b"}
        assert meta.auto_inc == {"id"}

    def test_without_schema(self) -> None:
        con = _make_con(
            [
                _FakeCursor(
                    [("T", "P", "COL_A", 1), ("T", "P", "COL_B", 2)]
                ),  # pk+unique
                _fc(),  # fk
                _FakeCursor([("T", "Desc")]),  # table comment
                _FakeCursor([("T", "COL", "c")]),  # col comments
                _FakeCursor([("T", "COL", "N", None)]),  # not_null+auto_inc
                _FakeCursor([("T", "IX")]),  # indexed
            ]
        )
        meta = introspect_table(con, "oracle", None, "t")
        assert meta.pk_map == {"col_a": 1, "col_b": 2}
        assert meta.table_comment == "Desc"
        assert meta.col_comments == {"col": "c"}
        assert meta.indexed == {"ix"}

    def test_empty_comments(self) -> None:
        con = _make_con(
            [
                _fc(),  # pk+unique
                _fc(),  # fk
                _FakeCursor([("T", None)]),  # table comment = None
                _fc(),  # col comments
                _fc(),  # not_null+auto_inc
                _fc(),  # indexed
            ]
        )
        meta = introspect_table(con, "oracle", "HR", "t")
        assert meta.table_comment is None


# ===========================================================================
# Mock-based tests: info_schema backends (8 cursors each)
# ===========================================================================


class TestPostgresMock:
    def test_full(self) -> None:
        con = _make_con(
            [
                _FakeCursor([("t", "id", 1)]),  # pk
                _FakeCursor([("t", "dept_id", "public", "depts", "id")]),  # fk
                _FakeCursor([("t", "Table X")]),  # pg table comment
                _FakeCursor([("t", "col_a", "desc A")]),  # pg col comments
                _FakeCursor([("t", "id"), ("t", "name")]),  # not_null
                _FakeCursor([("t", "email")]),  # unique
                _FakeCursor([("t", "col_a")]),  # indexed
                _FakeCursor([("t", "id")]),  # auto_inc
            ]
        )
        meta = introspect_table(con, "postgres", "public", "t")
        assert meta.pk_map == {"id": 1}
        assert len(meta.fks) == 1
        assert meta.table_comment == "Table X"
        assert meta.col_comments == {"col_a": "desc A"}
        assert meta.not_null == {"id", "name"}
        assert meta.unique == {"email"}
        assert meta.indexed == {"col_a"}
        assert meta.auto_inc == {"id"}

    def test_no_schema(self) -> None:
        con = _make_con([_fc() for _ in range(8)])
        meta = introspect_table(con, "postgres", None, "t")
        assert meta == TableMetadata()


class TestMySQLMock:
    def test_full(self) -> None:
        con = _make_con(
            [
                _FakeCursor([("t", "id", 1)]),  # pk
                _FakeCursor([("t", "fk_col", "mydb", "ref", "pk_col")]),  # fk
                _FakeCursor([("t", "My table")]),  # mysql table comment
                _FakeCursor([("t", "col_a", "desc A")]),  # mysql col comments
                _FakeCursor([("t", "id")]),  # not_null
                _FakeCursor([("t", "code")]),  # unique
                _FakeCursor([("t", "col_a")]),  # indexed
                _FakeCursor([("t", "id")]),  # auto_inc
            ]
        )
        meta = introspect_table(con, "mysql", "mydb", "t")
        assert meta.pk_map == {"id": 1}
        assert len(meta.fks) == 1
        assert meta.table_comment == "My table"
        assert meta.unique == {"code"}
        assert meta.indexed == {"col_a"}
        assert meta.auto_inc == {"id"}

    def test_no_schema(self) -> None:
        con = _make_con([_fc() for _ in range(8)])
        meta = introspect_table(con, "mysql", None, "t")
        assert meta == TableMetadata()


class TestMSSQLMock:
    def test_full(self) -> None:
        con = _make_con(
            [
                _FakeCursor([("t", "id", 1)]),  # pk
                _FakeCursor([("t", "fk_col", "dbo", "ref", "pk_col")]),  # fk
                _FakeCursor([("t", "Desc")]),  # mssql table comment
                _FakeCursor(
                    [("t", "col_a", "A"), ("t", "col_b", None)]
                ),  # mssql col comments
                _FakeCursor([("t", "id")]),  # not_null
                _FakeCursor([("t", "email")]),  # unique
                _FakeCursor([("t", "col_a")]),  # indexed
                _FakeCursor([("t", "id")]),  # auto_inc
            ]
        )
        meta = introspect_table(con, "mssql", "dbo", "t")
        assert meta.pk_map == {"id": 1}
        assert meta.table_comment == "Desc"
        assert meta.col_comments == {"col_a": "A"}
        assert meta.auto_inc == {"id"}

    def test_no_schema(self) -> None:
        con = _make_con([_fc() for _ in range(8)])
        meta = introspect_table(con, "mssql", None, "t")
        assert meta == TableMetadata()


class TestDuckDBMock:
    def test_full(self) -> None:
        # DuckDB: 7 cursors (no indexed raw_sql call)
        con = _make_con(
            [
                _FakeCursor([("t", "id", 1)]),  # pk
                _FakeCursor([("t", "fk_col", "main", "ref", "pk_col")]),  # fk
                _FakeCursor([("t", "Table comment")]),  # duckdb table comment
                _FakeCursor([("t", "col_a", "desc")]),  # duckdb col comments
                _FakeCursor([("t", "id")]),  # not_null
                _FakeCursor([("t", ["email"])]),  # unique (duckdb_constraints)
                _FakeCursor([("t", "id")]),  # auto_inc
            ]
        )
        meta = introspect_table(con, "duckdb", None, "t")
        assert meta.pk_map == {"id": 1}
        assert len(meta.fks) == 1
        assert meta.table_comment == "Table comment"
        assert meta.unique == {"email"}
        assert meta.indexed == set()
        assert meta.auto_inc == {"id"}

    def test_with_schema(self) -> None:
        """Exercise schema-filter branches for DuckDB."""
        con = _make_con([_fc() for _ in range(7)])
        meta = introspect_table(con, "duckdb", "main", "t")
        assert meta == TableMetadata()


class TestUnknownBackend:
    def test_returns_partial(self) -> None:
        """Unknown backend: comments/indexed/auto_inc have no branch."""
        # pk(1), fk(1), comments(0 raw_sql), not_null(1), unique(1), indexed(0), auto_inc(0)
        con = _make_con([_fc(), _fc(), _fc(), _fc()])
        meta = introspect_table(con, "unknown_db", None, "t")
        assert meta.indexed == set()
        assert meta.auto_inc == set()


class TestBatchIgnoresUnknownTables:
    """Batch queries may return rows for tables not requested — they must be skipped."""

    def test_oracle(self) -> None:
        con = _make_con(
            [
                _FakeCursor([("T", "P", "ID", 1), ("X", "P", "Z", 1)]),
                _FakeCursor([("X", "COL", "HR", "REF", "ID")]),
                _FakeCursor([("X", "c")]),
                _FakeCursor([("X", "C", "d")]),
                _FakeCursor([("X", "C", "N", None)]),
                _FakeCursor([("X", "C")]),
            ]
        )
        meta = introspect_table(con, "oracle", "HR", "t")
        assert meta.pk_map == {"id": 1}
        assert meta.fks == []

    def test_postgres(self) -> None:
        con = _make_con(
            [
                _FakeCursor([("x", "z", 1)]),
                _FakeCursor([("x", "c", "public", "ref", "id")]),
                _FakeCursor([("x", "d")]),
                _FakeCursor([("x", "c", "d")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", "c")]),
            ]
        )
        assert introspect_table(con, "postgres", "public", "t") == TableMetadata()

    def test_mysql(self) -> None:
        con = _make_con(
            [
                _FakeCursor([("x", "z", 1)]),
                _FakeCursor([("x", "c", "db", "ref", "id")]),
                _FakeCursor([("x", "d")]),
                _FakeCursor([("x", "c", "d")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", "c")]),
            ]
        )
        assert introspect_table(con, "mysql", "mydb", "t") == TableMetadata()

    def test_mssql(self) -> None:
        con = _make_con(
            [
                _FakeCursor([("x", "z", 1)]),
                _FakeCursor([("x", "c", "dbo", "ref", "id")]),
                _FakeCursor([("x", "d")]),
                _FakeCursor([("x", "c", "d")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", "c")]),
            ]
        )
        assert introspect_table(con, "mssql", "dbo", "t") == TableMetadata()

    def test_duckdb(self) -> None:
        con = _make_con(
            [
                _FakeCursor([("x", "z", 1)]),
                _FakeCursor([("x", "c", "main", "ref", "id")]),
                _FakeCursor([("x", "d")]),
                _FakeCursor([("x", "c", "d")]),
                _FakeCursor([("x", "c")]),
                _FakeCursor([("x", ["c"])]),
                _FakeCursor([("x", "c")]),
            ]
        )
        assert introspect_table(con, "duckdb", None, "t") == TableMetadata()


# ===========================================================================
# Unit tests: _update_cached_metadata
# ===========================================================================


class TestUpdateCachedMetadata:
    def _setup_catalog(self) -> tuple[Catalog, str]:
        """Return (catalog, dataset_id) with one dataset + 2 variables."""
        from datannurpy.schema import Dataset, Variable

        catalog = Catalog()
        ds = Dataset(id="ds", name="t", folder_id="f", _seen=True)
        catalog.dataset.add(ds)
        catalog.variable.add(Variable(id="ds---col_a", name="col_a", dataset_id="ds"))
        catalog.variable.add(Variable(id="ds---col_b", name="col_b", dataset_id="ds"))
        return catalog, "ds"

    def test_table_comment_applied(self) -> None:
        catalog, ds_id = self._setup_catalog()
        update_cached_metadata(catalog, ds_id, TableMetadata(table_comment="New desc"))
        ds = catalog.dataset.get(ds_id)
        assert ds is not None
        assert ds.description == "New desc"

    def test_pk_updated(self) -> None:
        catalog, ds_id = self._setup_catalog()
        update_cached_metadata(catalog, ds_id, TableMetadata(pk_map={"col_a": 1}))
        va = catalog.variable.get("ds---col_a")
        vb = catalog.variable.get("ds---col_b")
        assert va is not None and va.key == 1
        assert vb is not None and vb.key is None

    def test_col_comment_applied(self) -> None:
        catalog, ds_id = self._setup_catalog()
        update_cached_metadata(
            catalog,
            ds_id,
            TableMetadata(col_comments={"col_a": "desc A"}),
        )
        va = catalog.variable.get("ds---col_a")
        assert va is not None
        assert va.description == "desc A"

    def test_tags_updated(self) -> None:
        catalog, ds_id = self._setup_catalog()
        update_cached_metadata(
            catalog,
            ds_id,
            TableMetadata(not_null={"col_a"}),
        )
        va = catalog.variable.get("ds---col_a")
        assert va is not None
        assert "db---not-null" in va.tag_ids


# ===========================================================================
# Integration tests: full catalog + add_database
# ===========================================================================


class TestCatalogDatabaseIntrospection:
    """Test that add_database populates PK, FK, comments, and tags."""

    def test_pk_populated(self, sqlite_catalog: Catalog) -> None:
        v = _emp_vars(sqlite_catalog)
        assert v["id"].key == 1
        assert v["name"].key is None

    def test_composite_pk(self, sqlite_catalog: Catalog) -> None:
        oi = {
            v.name: v
            for v in sqlite_catalog.variable.all()
            if "order_items" in v.dataset_id
        }
        assert oi["order_id"].key == 1
        assert oi["item_id"].key == 2

    def test_fk_resolved(self, sqlite_catalog: Catalog) -> None:
        fk_var_id = _emp_vars(sqlite_catalog)["department_id"].fk_var_id
        assert fk_var_id is not None
        assert "departments" in fk_var_id and fk_var_id.endswith("---id")

    def test_db_tags_created(self, sqlite_catalog: Catalog) -> None:
        tag_ids = {t.id for t in sqlite_catalog.tag.all()}
        assert {
            "scan",
            "db",
            "db---not-null",
            "db---unique",
            "db---indexed",
            "db---auto-increment",
        } <= tag_ids
        db_tag = sqlite_catalog.tag.get("db")
        assert db_tag is not None and db_tag.parent_id == "scan"

    def test_constraint_tags_assigned(self, sqlite_catalog: Catalog) -> None:
        v = _emp_vars(sqlite_catalog)
        assert "db---not-null" in v["name"].tag_ids
        assert "db---not-null" not in v["salary"].tag_ids
        assert "db---indexed" in v["name"].tag_ids
        assert "db---unique" in v["email"].tag_ids
        assert "db---auto-increment" in v["id"].tag_ids

    def test_duckdb_comments(self, duckdb_with_constraints) -> None:
        catalog = Catalog()
        catalog.add_database(duckdb_with_constraints, Folder(id="db", name="Test DB"))
        dept = next(d for d in catalog.dataset.all() if d.name == "departments")
        assert dept.description == "Company departments"
        dv = {v.name: v for v in catalog.variable.all() if v.dataset_id == dept.id}
        assert dv["name"].description == "Department name"
        assert dv["budget"].description == "Annual budget"

    def test_dataset_depth_skips(self, sqlite_with_constraints: Path) -> None:
        catalog = Catalog(depth="dataset")
        catalog.add_database(
            f"sqlite:////{sqlite_with_constraints}",
            Folder(id="db", name="Test DB"),
        )
        assert catalog.tag.count == 0
        assert catalog.variable.count == 0

    def test_schema_depth_introspects(self, sqlite_with_constraints: Path) -> None:
        catalog = Catalog(depth="variable")
        catalog.add_database(
            f"sqlite:////{sqlite_with_constraints}",
            Folder(id="db", name="Test DB"),
        )
        assert catalog.tag.count > 0
        assert _emp_vars(catalog)["id"].key == 1


class TestCatalogIncrementalIntrospection:
    """Test introspection across incremental rescans."""

    def test_cached_metadata_refreshed(
        self,
        sqlite_with_constraints: Path,
        tmp_path: Path,
    ) -> None:
        conn_str = f"sqlite:////{sqlite_with_constraints}"
        folder = Folder(id="db", name="Test DB")

        cat1 = Catalog(app_path=tmp_path, quiet=True)
        cat1.add_database(conn_str, folder)
        cat1.export_db()
        assert _emp_vars(cat1)["id"].key == 1

        cat2 = Catalog(app_path=tmp_path, quiet=True)
        cat2.add_database(conn_str, folder)
        v = _emp_vars(cat2)
        assert v["id"].key == 1
        assert "db---not-null" in v["name"].tag_ids

    def test_schema_depth_cached_metadata(
        self,
        sqlite_with_constraints: Path,
        tmp_path: Path,
    ) -> None:
        """Schema-depth cache hit still refreshes introspection metadata."""
        conn_str = f"sqlite:////{sqlite_with_constraints}"
        folder = Folder(id="db", name="Test DB")

        cat1 = Catalog(app_path=tmp_path, depth="variable", quiet=True)
        cat1.add_database(conn_str, folder)
        cat1.export_db()

        cat2 = Catalog(app_path=tmp_path, depth="variable", quiet=True)
        cat2.add_database(conn_str, folder)
        v = _emp_vars(cat2)
        assert v["id"].key == 1
        assert "db---not-null" in v["name"].tag_ids

    def test_tags_survive_finalize(
        self,
        sqlite_with_constraints: Path,
        tmp_path: Path,
    ) -> None:
        cat = Catalog(app_path=tmp_path, quiet=True)
        cat.add_database(
            f"sqlite:////{sqlite_with_constraints}",
            Folder(id="db", name="Test DB"),
        )
        cat.finalize()
        tag_ids = {t.id for t in cat.tag.all()}
        assert {"db", "db---not-null", "db---indexed"} <= tag_ids

    def test_introspect_returns_empty(
        self,
        sqlite_with_constraints: Path,
    ) -> None:
        """Empty introspection result should not break the scan."""
        with patch(
            "datannurpy.add_database.introspect_schema",
            side_effect=lambda _c, _b, _s, tables: {t: TableMetadata() for t in tables},
        ):
            catalog = Catalog()
            catalog.add_database(
                f"sqlite:////{sqlite_with_constraints}",
                Folder(id="db", name="Test DB"),
            )
        v = _emp_vars(catalog)
        assert v["id"].key is None
        assert v["name"].tag_ids == []

    def test_cached_table_comment_applied(
        self,
        duckdb_with_constraints,
        tmp_path: Path,
    ) -> None:
        """Table comment should propagate to cached datasets."""
        folder = Folder(id="db", name="Test DB")
        cat1 = Catalog(app_path=tmp_path, quiet=True)
        cat1.add_database(duckdb_with_constraints, folder)
        cat1.export_db()

        cat2 = Catalog(app_path=tmp_path, quiet=True)
        cat2.add_database(duckdb_with_constraints, folder)
        dept = next(d for d in cat2.dataset.all() if d.name == "departments")
        assert dept.description == "Company departments"

    def test_pk_removal_on_rescan(
        self,
        sqlite_with_constraints: Path,
        tmp_path: Path,
    ) -> None:
        """If a column is no longer PK, key should be cleared on rescan."""
        conn_str = f"sqlite:////{sqlite_with_constraints}"
        folder = Folder(id="db", name="Test DB")

        cat1 = Catalog(app_path=tmp_path, quiet=True)
        cat1.add_database(conn_str, folder)
        emp = [
            v
            for v in cat1.variable.all()
            if "employees" in v.dataset_id and v.name == "salary"
        ]
        cat1.variable.update(emp[0].id, key=99)
        cat1.export_db()

        cat2 = Catalog(app_path=tmp_path, quiet=True)
        cat2.add_database(conn_str, folder)
        assert _emp_vars(cat2)["salary"].key is None

    def test_unresolvable_fk_ignored(self, tmp_path: Path) -> None:
        """FK referencing a table not in scope should be silently ignored."""
        import sqlite3

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE ref_target (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO ref_target VALUES (1)")
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES ref_target(id))"
        )
        conn.execute("INSERT INTO child VALUES (1, 1)")
        conn.commit()
        conn.close()

        catalog = Catalog()
        catalog.add_database(
            f"sqlite:////{db_path}",
            Folder(id="db", name="Test DB"),
            include=["child"],
        )
        v = {v.name: v for v in catalog.variable.all()}
        assert v["parent_id"].fk_var_id is None
        db_path.unlink(missing_ok=True)
