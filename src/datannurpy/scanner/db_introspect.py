"""Database introspection: extract structural metadata (PK, FK, comments, constraints)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ForeignKey:
    """A foreign key relationship from a local column to a referenced column."""

    local_col: str
    ref_schema: str | None
    ref_table: str
    ref_col: str


@dataclass
class TableMetadata:
    """All structural metadata for a single table."""

    pk_map: dict[str, int] = field(default_factory=dict)
    fks: list[ForeignKey] = field(default_factory=list)
    table_comment: str | None = None
    col_comments: dict[str, str] = field(default_factory=dict)
    not_null: set[str] = field(default_factory=set)
    unique: set[str] = field(default_factory=set)
    indexed: set[str] = field(default_factory=set)
    auto_inc: set[str] = field(default_factory=set)


def _quote(value: str) -> str:
    """Escape single quotes for safe SQL interpolation in system catalog queries."""
    return value.replace("'", "''")


def introspect_schema(
    con: object,
    backend: str,
    schema: str | None,
    tables: list[str],
) -> dict[str, TableMetadata]:
    """Batch-extract structural metadata for all tables in a schema."""
    raw: Any = getattr(con, "raw_sql", None)
    if raw is None:
        return {t: TableMetadata() for t in tables}
    if backend == "sqlite":
        return {t: _introspect_sqlite(raw, t) for t in tables}
    if backend == "oracle":
        return _introspect_oracle_batch(raw, schema, tables)
    return _introspect_info_schema_batch(raw, backend, schema, tables)


def introspect_table(
    con: object,
    backend: str,
    schema: str | None,
    table: str,
) -> TableMetadata:
    """Extract structural metadata for a single table."""
    return introspect_schema(con, backend, schema, [table]).get(table, TableMetadata())


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


def _introspect_sqlite(raw: Any, table: str) -> TableMetadata:
    meta = TableMetadata()
    qt = _quote(table)

    try:
        rows = raw(f"PRAGMA table_info('{qt}')").fetchall()
        for _cid, name, col_type, notnull, _dflt, pk in rows:
            if pk > 0:
                meta.pk_map[name] = pk
            if notnull:
                meta.not_null.add(name)
            if pk == 1 and col_type.upper() == "INTEGER":
                meta.auto_inc.add(name)
    except Exception:
        pass

    try:
        rows = raw(f"PRAGMA foreign_key_list('{qt}')").fetchall()
        meta.fks = [
            ForeignKey(local_col=r[3], ref_schema=None, ref_table=r[2], ref_col=r[4])
            for r in rows
        ]
    except Exception:
        pass

    try:
        indexes = raw(f"PRAGMA index_list('{qt}')").fetchall()
        for idx in indexes:
            cols = raw(f"PRAGMA index_info('{_quote(idx[1])}')").fetchall()
            for col in cols:
                meta.indexed.add(col[2])
            if idx[2] and len(cols) == 1:
                meta.unique.add(cols[0][2])
    except Exception:
        pass

    return meta


# ---------------------------------------------------------------------------
# Oracle (batch — all tables in one pass)
# ---------------------------------------------------------------------------


def _introspect_oracle_batch(
    raw: Any, schema: str | None, tables: list[str]
) -> dict[str, TableMetadata]:
    result = {t: TableMetadata() for t in tables}
    upper_to_orig = {t.upper(): t for t in tables}

    def _meta(tname_upper: str) -> TableMetadata | None:
        orig = upper_to_orig.get(tname_upper)
        return result[orig] if orig else None

    def _run(all_sql: str, user_sql: str) -> list[Any]:
        if schema:
            return raw(all_sql.format(owner=_quote(schema.upper()))).fetchall()
        return raw(user_sql).fetchall()

    # 1) PK + UNIQUE
    try:
        rows = _run(
            "SELECT c.table_name, c.constraint_type, cc.column_name, cc.position "
            "FROM all_constraints c "
            "JOIN all_cons_columns cc ON c.owner = cc.owner "
            "  AND c.constraint_name = cc.constraint_name "
            "WHERE c.owner = '{owner}' AND c.constraint_type IN ('P', 'U')",
            "SELECT c.table_name, c.constraint_type, cc.column_name, cc.position "
            "FROM user_constraints c "
            "JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name "
            "WHERE c.constraint_type IN ('P', 'U')",
        )
        for tname, ctype, col, pos in rows:
            m = _meta(tname)
            if m is None:
                continue
            if ctype == "P":
                m.pk_map[col.lower()] = int(pos)
            else:
                m.unique.add(col.lower())
    except Exception:
        pass

    # 2) FK
    try:
        rows = _run(
            "SELECT c.table_name, cc.column_name, rc.owner, "
            "  rc.table_name, rcc.column_name "
            "FROM all_constraints c "
            "JOIN all_cons_columns cc ON c.owner = cc.owner "
            "  AND c.constraint_name = cc.constraint_name "
            "JOIN all_constraints rc ON c.r_owner = rc.owner "
            "  AND c.r_constraint_name = rc.constraint_name "
            "JOIN all_cons_columns rcc ON rc.owner = rcc.owner "
            "  AND rc.constraint_name = rcc.constraint_name "
            "  AND cc.position = rcc.position "
            "WHERE c.owner = '{owner}' AND c.constraint_type = 'R'",
            "SELECT c.table_name, cc.column_name, rc.owner, "
            "  rc.table_name, rcc.column_name "
            "FROM user_constraints c "
            "JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name "
            "JOIN all_constraints rc ON c.r_owner = rc.owner "
            "  AND c.r_constraint_name = rc.constraint_name "
            "JOIN all_cons_columns rcc ON rc.owner = rcc.owner "
            "  AND rc.constraint_name = rcc.constraint_name "
            "  AND cc.position = rcc.position "
            "WHERE c.constraint_type = 'R'",
        )
        for tname, col, ref_owner, ref_table, ref_col in rows:
            m = _meta(tname)
            if m is None:
                continue
            m.fks.append(
                ForeignKey(
                    local_col=col.lower(),
                    ref_schema=ref_owner.lower(),
                    ref_table=ref_table.lower(),
                    ref_col=ref_col.lower(),
                )
            )
    except Exception:
        pass

    # 3) Comments (table + column)
    try:
        for tname, comment in _run(
            "SELECT table_name, comments FROM all_tab_comments "
            "WHERE owner = '{owner}' AND comments IS NOT NULL",
            "SELECT table_name, comments FROM user_tab_comments "
            "WHERE comments IS NOT NULL",
        ):
            m = _meta(tname)
            if m and comment:
                m.table_comment = comment
        for tname, col, comment in _run(
            "SELECT table_name, column_name, comments FROM all_col_comments "
            "WHERE owner = '{owner}' AND comments IS NOT NULL",
            "SELECT table_name, column_name, comments FROM user_col_comments "
            "WHERE comments IS NOT NULL",
        ):
            m = _meta(tname)
            if m and comment:
                m.col_comments[col.lower()] = comment
    except Exception:
        pass

    # 4) NOT NULL + auto-increment
    try:
        rows = _run(
            "SELECT c.table_name, c.column_name, c.nullable, i.column_name "
            "FROM all_tab_columns c "
            "LEFT JOIN all_tab_identity_cols i "
            "  ON c.owner = i.owner AND c.table_name = i.table_name "
            "  AND c.column_name = i.column_name "
            "WHERE c.owner = '{owner}'",
            "SELECT c.table_name, c.column_name, c.nullable, i.column_name "
            "FROM user_tab_columns c "
            "LEFT JOIN user_tab_identity_cols i "
            "  ON c.table_name = i.table_name "
            "  AND c.column_name = i.column_name",
        )
        for tname, col, nullable, identity_col in rows:
            m = _meta(tname)
            if m is None:
                continue
            if nullable == "N":
                m.not_null.add(col.lower())
            if identity_col is not None:
                m.auto_inc.add(col.lower())
    except Exception:
        pass

    # 5) Indexes
    try:
        for tname, col in _run(
            "SELECT table_name, column_name FROM all_ind_columns "
            "WHERE table_owner = '{owner}'",
            "SELECT table_name, column_name FROM user_ind_columns",
        ):
            m = _meta(tname)
            if m:
                m.indexed.add(col.lower())
    except Exception:
        pass

    return result


# ---------------------------------------------------------------------------
# Information Schema — batch (postgres, mysql, mssql, duckdb)
# ---------------------------------------------------------------------------


def _introspect_info_schema_batch(
    raw: Any,
    backend: str,
    schema: str | None,
    tables: list[str],
) -> dict[str, TableMetadata]:
    result = {t: TableMetadata() for t in tables}
    sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""

    # 1) PK
    try:
        q = (
            "SELECT tc.table_name, kcu.column_name, kcu.ordinal_position "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name"
        )
        if backend != "mysql":
            q += " AND tc.table_schema = kcu.table_schema"
        tsf = f" AND tc.table_schema = '{_quote(schema)}'" if schema else ""
        q += f" WHERE tc.constraint_type = 'PRIMARY KEY'{tsf}"
        for tname, col, pos in raw(q).fetchall():
            m = result.get(tname)
            if m:
                m.pk_map[col] = int(pos)
    except Exception:
        pass

    # 2) FK (backend-specific)
    try:
        if backend == "mysql":
            rows = raw(
                "SELECT table_name, column_name, referenced_table_schema, "
                "  referenced_table_name, referenced_column_name "
                "FROM information_schema.key_column_usage "
                f"WHERE referenced_table_name IS NOT NULL{sf}"
            ).fetchall()
        elif backend in ("postgres", "mssql"):
            tsf = f" AND tc.table_schema = '{_quote(schema)}'" if schema else ""
            rows = raw(
                "SELECT tc.table_name, kcu.column_name, "
                "  ccu.table_schema, ccu.table_name, ccu.column_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name "
                "  AND tc.table_schema = kcu.table_schema "
                "JOIN information_schema.constraint_column_usage ccu "
                "  ON tc.constraint_name = ccu.constraint_name "
                "  AND tc.table_schema = ccu.table_schema "
                f"WHERE tc.constraint_type = 'FOREIGN KEY'{tsf}"
            ).fetchall()
        else:
            kcu_sf = f" WHERE kcu.table_schema = '{_quote(schema)}'" if schema else ""
            rows = raw(
                "SELECT kcu.table_name, kcu.column_name, "
                "  rkcu.table_schema, rkcu.table_name, rkcu.column_name "
                "FROM information_schema.referential_constraints rc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON rc.constraint_schema = kcu.constraint_schema "
                "  AND rc.constraint_name = kcu.constraint_name "
                "JOIN information_schema.key_column_usage rkcu "
                "  ON rc.unique_constraint_schema = rkcu.constraint_schema "
                "  AND rc.unique_constraint_name = rkcu.constraint_name"
                f"{kcu_sf}"
            ).fetchall()
        for row in rows:
            m = result.get(row[0])
            if m:
                m.fks.append(
                    ForeignKey(
                        local_col=row[1],
                        ref_schema=row[2],
                        ref_table=row[3],
                        ref_col=row[4],
                    )
                )
    except Exception:
        pass

    # 3) Comments (backend-specific)
    try:
        _comments_batch(raw, backend, schema, result)
    except Exception:
        pass

    # 4) NOT NULL
    try:
        for tname, col in raw(
            "SELECT table_name, column_name FROM information_schema.columns "
            f"WHERE is_nullable = 'NO'{sf}"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.not_null.add(col)
    except Exception:
        pass

    # 5) UNIQUE
    try:
        if backend == "duckdb":
            dsf = f" AND schema_name = '{_quote(schema)}'" if schema else ""
            rows = raw(
                "SELECT table_name, constraint_column_names "
                "FROM duckdb_constraints() "
                f"WHERE constraint_type = 'UNIQUE'{dsf}"
            ).fetchall()
            for tname, cols in rows:
                m = result.get(tname)
                if m and isinstance(cols, list) and len(cols) == 1:
                    m.unique.add(cols[0])
        else:
            q = (
                "SELECT tc.table_name, kcu.column_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name"
            )
            if backend != "mysql":
                q += " AND tc.table_schema = kcu.table_schema"
            tsf = f" AND tc.table_schema = '{_quote(schema)}'" if schema else ""
            q += f" WHERE tc.constraint_type = 'UNIQUE'{tsf}"
            for tname, col in raw(q).fetchall():
                m = result.get(tname)
                if m:
                    m.unique.add(col)
    except Exception:
        pass

    # 6) Indexed
    try:
        _indexed_batch(raw, backend, schema, result)
    except Exception:
        pass

    # 7) Auto-increment
    try:
        _auto_inc_batch(raw, backend, schema, result)
    except Exception:
        pass

    return result


# ---------------------------------------------------------------------------
# Batch helpers for info_schema backends
# ---------------------------------------------------------------------------


def _comments_batch(
    raw: Any,
    backend: str,
    schema: str | None,
    result: dict[str, TableMetadata],
) -> None:
    if backend == "postgres":
        sn = _quote(schema or "public")
        for tname, comment in raw(
            "SELECT c.relname, obj_description(c.oid) "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{sn}' AND c.relkind = 'r'"
        ).fetchall():
            m = result.get(tname)
            if m and comment:
                m.table_comment = comment
        for tname, col, desc in raw(
            "SELECT c.relname, a.attname, d.description "
            "FROM pg_attribute a "
            "JOIN pg_class c ON a.attrelid = c.oid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "LEFT JOIN pg_description d "
            "  ON d.objoid = c.oid AND d.objsubid = a.attnum "
            f"WHERE n.nspname = '{sn}' AND c.relkind = 'r' "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "AND d.description IS NOT NULL"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.col_comments[col] = desc

    elif backend == "mysql":
        sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""
        for tname, comment in raw(
            "SELECT table_name, table_comment FROM information_schema.tables "
            f"WHERE table_comment != ''{sf}"
        ).fetchall():
            m = result.get(tname)
            if m and comment:
                m.table_comment = comment
        for tname, col, comment in raw(
            "SELECT table_name, column_name, column_comment "
            "FROM information_schema.columns "
            f"WHERE column_comment != ''{sf}"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.col_comments[col] = comment

    elif backend == "mssql":
        sn = _quote(schema or "dbo")
        for tname, comment in raw(
            "SELECT t.name, CAST(ep.value AS NVARCHAR(MAX)) "
            "FROM sys.extended_properties ep "
            "JOIN sys.tables t ON ep.major_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE s.name = '{sn}' "
            "AND ep.minor_id = 0 AND ep.name = 'MS_Description'"
        ).fetchall():
            m = result.get(tname)
            if m and comment:
                m.table_comment = comment
        for tname, col, comment in raw(
            "SELECT t.name, c.name, CAST(ep.value AS NVARCHAR(MAX)) "
            "FROM sys.extended_properties ep "
            "JOIN sys.columns c ON ep.major_id = c.object_id "
            "  AND ep.minor_id = c.column_id "
            "JOIN sys.tables t ON c.object_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE s.name = '{sn}' AND ep.name = 'MS_Description'"
        ).fetchall():
            m = result.get(tname)
            if m and comment:
                m.col_comments[col] = comment

    elif backend == "duckdb":
        dsf = f" AND schema_name = '{_quote(schema)}'" if schema else ""
        for tname, comment in raw(
            "SELECT table_name, comment FROM duckdb_tables() "
            f"WHERE comment IS NOT NULL{dsf}"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.table_comment = comment
        for tname, col, comment in raw(
            "SELECT table_name, column_name, comment FROM duckdb_columns() "
            f"WHERE comment IS NOT NULL{dsf}"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.col_comments[col] = comment


def _indexed_batch(
    raw: Any,
    backend: str,
    schema: str | None,
    result: dict[str, TableMetadata],
) -> None:
    if backend == "postgres":
        sn = _quote(schema or "public")
        for tname, col in raw(
            "SELECT c.relname, a.attname "
            "FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid "
            "  AND a.attnum = ANY(i.indkey) "
            f"WHERE n.nspname = '{sn}'"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.indexed.add(col)

    elif backend == "mysql":
        sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""
        for tname, col in raw(
            "SELECT table_name, column_name "
            "FROM information_schema.statistics "
            f"WHERE 1=1{sf}"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.indexed.add(col)

    elif backend == "mssql":
        sn = _quote(schema or "dbo")
        for tname, col in raw(
            "SELECT t.name, c.name "
            "FROM sys.index_columns ic "
            "JOIN sys.columns c ON ic.object_id = c.object_id "
            "  AND ic.column_id = c.column_id "
            "JOIN sys.tables t ON ic.object_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE s.name = '{sn}'"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.indexed.add(col)


def _auto_inc_batch(
    raw: Any,
    backend: str,
    schema: str | None,
    result: dict[str, TableMetadata],
) -> None:
    if backend == "postgres":
        sn = _quote(schema or "public")
        for tname, col in raw(
            "SELECT table_name, column_name FROM information_schema.columns "
            f"WHERE table_schema = '{sn}' "
            "AND (column_default LIKE 'nextval%' OR is_identity = 'YES')"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.auto_inc.add(col)

    elif backend == "mysql":
        sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""
        for tname, col in raw(
            "SELECT table_name, column_name FROM information_schema.columns "
            f"WHERE extra LIKE '%auto_increment%'{sf}"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.auto_inc.add(col)

    elif backend == "mssql":
        sn = _quote(schema or "dbo")
        for tname, col in raw(
            "SELECT t.name, c.name "
            "FROM sys.columns c "
            "JOIN sys.tables t ON c.object_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE s.name = '{sn}' AND c.is_identity = 1"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.auto_inc.add(col)

    elif backend == "duckdb":
        sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""
        for tname, col in raw(
            "SELECT table_name, column_name FROM information_schema.columns "
            f"WHERE column_default LIKE 'nextval%'{sf}"
        ).fetchall():
            m = result.get(tname)
            if m:
                m.auto_inc.add(col)
