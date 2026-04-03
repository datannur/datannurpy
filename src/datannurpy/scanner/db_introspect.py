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


def introspect_table(
    con: object,
    backend: str,
    schema: str | None,
    table: str,
) -> TableMetadata:
    """Extract all structural metadata for a table. Each query fails independently."""
    raw: Any = getattr(con, "raw_sql", None)
    if raw is None:
        return TableMetadata()
    if backend == "sqlite":
        return _introspect_sqlite(raw, table)
    if backend == "oracle":
        return _introspect_oracle(raw, schema, table)
    return _introspect_info_schema(raw, backend, schema, table)


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
# Oracle
# ---------------------------------------------------------------------------


def _ora(
    raw: Any,
    schema: str | None,
    table: str,
    all_sql: str,
    user_sql: str,
) -> list[Any]:
    """Run Oracle catalog query, choosing all_* or user_* view variant."""
    t = _quote(table.upper())
    if schema:
        return raw(all_sql.format(owner=_quote(schema.upper()), table=t)).fetchall()
    return raw(user_sql.format(table=t)).fetchall()


def _introspect_oracle(raw: Any, schema: str | None, table: str) -> TableMetadata:
    meta = TableMetadata()

    # 1) PK + UNIQUE in one query on all_constraints/user_constraints
    try:
        rows = _ora(
            raw,
            schema,
            table,
            "SELECT c.constraint_type, cc.column_name, cc.position "
            "FROM all_constraints c "
            "JOIN all_cons_columns cc ON c.owner = cc.owner "
            "  AND c.constraint_name = cc.constraint_name "
            "WHERE c.owner = '{owner}' AND c.table_name = '{table}' "
            "AND c.constraint_type IN ('P', 'U')",
            "SELECT c.constraint_type, cc.column_name, cc.position "
            "FROM user_constraints c "
            "JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name "
            "WHERE c.table_name = '{table}' AND c.constraint_type IN ('P', 'U')",
        )
        for ctype, col, pos in rows:
            if ctype == "P":
                meta.pk_map[col.lower()] = int(pos)
            else:
                meta.unique.add(col.lower())
    except Exception:
        pass

    # 2) FK (complex join, kept separate)
    try:
        rows = _ora(
            raw,
            schema,
            table,
            "SELECT cc.column_name, rc.owner, rc.table_name, rcc.column_name "
            "FROM all_constraints c "
            "JOIN all_cons_columns cc ON c.owner = cc.owner "
            "  AND c.constraint_name = cc.constraint_name "
            "JOIN all_constraints rc ON c.r_owner = rc.owner "
            "  AND c.r_constraint_name = rc.constraint_name "
            "JOIN all_cons_columns rcc ON rc.owner = rcc.owner "
            "  AND rc.constraint_name = rcc.constraint_name "
            "  AND cc.position = rcc.position "
            "WHERE c.owner = '{owner}' AND c.table_name = '{table}' "
            "AND c.constraint_type = 'R'",
            "SELECT cc.column_name, rc.owner, rc.table_name, rcc.column_name "
            "FROM user_constraints c "
            "JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name "
            "JOIN all_constraints rc ON c.r_owner = rc.owner "
            "  AND c.r_constraint_name = rc.constraint_name "
            "JOIN all_cons_columns rcc ON rc.owner = rcc.owner "
            "  AND rc.constraint_name = rcc.constraint_name "
            "  AND cc.position = rcc.position "
            "WHERE c.table_name = '{table}' AND c.constraint_type = 'R'",
        )
        meta.fks = [
            ForeignKey(
                local_col=r[0].lower(),
                ref_schema=r[1].lower(),
                ref_table=r[2].lower(),
                ref_col=r[3].lower(),
            )
            for r in rows
        ]
    except Exception:
        pass

    # 3) Comments (table + column, one try/except)
    try:
        tc = _ora(
            raw,
            schema,
            table,
            "SELECT comments FROM all_tab_comments "
            "WHERE owner = '{owner}' AND table_name = '{table}'",
            "SELECT comments FROM user_tab_comments WHERE table_name = '{table}'",
        )
        if tc and tc[0][0]:
            meta.table_comment = tc[0][0]
        cc = _ora(
            raw,
            schema,
            table,
            "SELECT column_name, comments FROM all_col_comments "
            "WHERE owner = '{owner}' AND table_name = '{table}'",
            "SELECT column_name, comments FROM user_col_comments "
            "WHERE table_name = '{table}'",
        )
        for r in cc:
            if r[1]:
                meta.col_comments[r[0].lower()] = r[1]
    except Exception:
        pass

    # 4) NOT NULL + auto-increment in one query via LEFT JOIN
    try:
        rows = _ora(
            raw,
            schema,
            table,
            "SELECT c.column_name, c.nullable, i.column_name "
            "FROM all_tab_columns c "
            "LEFT JOIN all_tab_identity_cols i "
            "  ON c.owner = i.owner AND c.table_name = i.table_name "
            "  AND c.column_name = i.column_name "
            "WHERE c.owner = '{owner}' AND c.table_name = '{table}'",
            "SELECT c.column_name, c.nullable, i.column_name "
            "FROM user_tab_columns c "
            "LEFT JOIN user_tab_identity_cols i "
            "  ON c.table_name = i.table_name "
            "  AND c.column_name = i.column_name "
            "WHERE c.table_name = '{table}'",
        )
        for col, nullable, identity_col in rows:
            if nullable == "N":
                meta.not_null.add(col.lower())
            if identity_col is not None:
                meta.auto_inc.add(col.lower())
    except Exception:
        pass

    # 5) Indexes
    try:
        rows = _ora(
            raw,
            schema,
            table,
            "SELECT column_name FROM all_ind_columns "
            "WHERE table_owner = '{owner}' AND table_name = '{table}'",
            "SELECT column_name FROM user_ind_columns WHERE table_name = '{table}'",
        )
        meta.indexed = {r[0].lower() for r in rows}
    except Exception:
        pass

    return meta


# ---------------------------------------------------------------------------
# Information Schema (postgres, mysql, mssql, duckdb)
# ---------------------------------------------------------------------------


def _introspect_info_schema(
    raw: Any,
    backend: str,
    schema: str | None,
    table: str,
) -> TableMetadata:
    meta = TableMetadata()
    qt = _quote(table)
    sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""

    try:
        q = (
            "SELECT kcu.column_name, kcu.ordinal_position "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name"
        )
        if backend != "mysql":
            q += " AND tc.table_schema = kcu.table_schema"
        tsf = f" AND tc.table_schema = '{_quote(schema)}'" if schema else ""
        q += (
            f" WHERE tc.table_name = '{qt}'{tsf} AND tc.constraint_type = 'PRIMARY KEY'"
        )
        meta.pk_map = {r[0]: int(r[1]) for r in raw(q).fetchall()}
    except Exception:
        pass

    try:
        if backend == "mysql":
            rows = raw(
                "SELECT column_name, referenced_table_schema, "
                "  referenced_table_name, referenced_column_name "
                "FROM information_schema.key_column_usage "
                f"WHERE table_name = '{qt}'"
                f"{sf} AND referenced_table_name IS NOT NULL"
            ).fetchall()
        elif backend in ("postgres", "mssql"):
            tsf = f" AND tc.table_schema = '{_quote(schema)}'" if schema else ""
            rows = raw(
                "SELECT kcu.column_name, "
                "  ccu.table_schema, ccu.table_name, ccu.column_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name "
                "  AND tc.table_schema = kcu.table_schema "
                "JOIN information_schema.constraint_column_usage ccu "
                "  ON tc.constraint_name = ccu.constraint_name "
                "  AND tc.table_schema = ccu.table_schema "
                f"WHERE tc.table_name = '{qt}'"
                f"{tsf} AND tc.constraint_type = 'FOREIGN KEY'"
            ).fetchall()
        else:
            kcu_sf = f" AND kcu.table_schema = '{_quote(schema)}'" if schema else ""
            rows = raw(
                "SELECT kcu.column_name, "
                "  rkcu.table_schema, rkcu.table_name, rkcu.column_name "
                "FROM information_schema.referential_constraints rc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON rc.constraint_schema = kcu.constraint_schema "
                "  AND rc.constraint_name = kcu.constraint_name "
                "JOIN information_schema.key_column_usage rkcu "
                "  ON rc.unique_constraint_schema = rkcu.constraint_schema "
                "  AND rc.unique_constraint_name = rkcu.constraint_name "
                f"WHERE kcu.table_name = '{qt}'"
                f"{kcu_sf}"
            ).fetchall()
        meta.fks = [
            ForeignKey(
                local_col=r[0],
                ref_schema=r[1],
                ref_table=r[2],
                ref_col=r[3],
            )
            for r in rows
        ]
    except Exception:
        pass

    try:
        meta.table_comment, meta.col_comments = _comments_info_schema(
            raw,
            backend,
            schema,
            table,
        )
    except Exception:
        pass

    try:
        meta.not_null = {
            r[0]
            for r in raw(
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{qt}'{sf} AND is_nullable = 'NO'"
            ).fetchall()
        }
    except Exception:
        pass

    try:
        if backend == "duckdb":
            dsf = f" AND schema_name = '{_quote(schema)}'" if schema else ""
            rows = raw(
                "SELECT constraint_column_names FROM duckdb_constraints() "
                f"WHERE table_name = '{qt}'"
                f"{dsf} AND constraint_type = 'UNIQUE'"
            ).fetchall()
            meta.unique = {
                r[0][0] for r in rows if isinstance(r[0], list) and len(r[0]) == 1
            }
        else:
            q = (
                "SELECT kcu.column_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name"
            )
            if backend != "mysql":
                q += " AND tc.table_schema = kcu.table_schema"
            tsf = f" AND tc.table_schema = '{_quote(schema)}'" if schema else ""
            q += f" WHERE tc.table_name = '{qt}'{tsf} AND tc.constraint_type = 'UNIQUE'"
            meta.unique = {r[0] for r in raw(q).fetchall()}
    except Exception:
        pass

    try:
        meta.indexed = _indexed_info_schema(raw, backend, schema, table)
    except Exception:
        pass

    try:
        meta.auto_inc = _auto_inc_info_schema(raw, backend, schema, table)
    except Exception:
        pass

    return meta


def _comments_info_schema(
    raw: Any,
    backend: str,
    schema: str | None,
    table: str,
) -> tuple[str | None, dict[str, str]]:
    qt = _quote(table)
    table_comment: str | None = None
    col_comments: dict[str, str] = {}

    if backend == "postgres":
        sn = _quote(schema or "public")
        row = raw(
            "SELECT obj_description(c.oid) "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE c.relname = '{qt}' AND n.nspname = '{sn}'"
        ).fetchone()
        if row and row[0]:
            table_comment = row[0]
        rows = raw(
            "SELECT a.attname, d.description "
            "FROM pg_attribute a "
            "JOIN pg_class c ON a.attrelid = c.oid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "LEFT JOIN pg_description d "
            "  ON d.objoid = c.oid AND d.objsubid = a.attnum "
            f"WHERE c.relname = '{qt}' AND n.nspname = '{sn}' "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "AND d.description IS NOT NULL"
        ).fetchall()
        col_comments = {r[0]: r[1] for r in rows}

    elif backend == "mysql":
        sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""
        row = raw(
            "SELECT table_comment FROM information_schema.tables "
            f"WHERE table_name = '{qt}'{sf}"
        ).fetchone()
        if row and row[0]:
            table_comment = row[0]
        rows = raw(
            "SELECT column_name, column_comment "
            "FROM information_schema.columns "
            f"WHERE table_name = '{qt}'{sf} AND column_comment != ''"
        ).fetchall()
        col_comments = {r[0]: r[1] for r in rows}

    elif backend == "mssql":
        sn = _quote(schema or "dbo")
        row = raw(
            "SELECT CAST(ep.value AS NVARCHAR(MAX)) "
            "FROM sys.extended_properties ep "
            "JOIN sys.tables t ON ep.major_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE t.name = '{qt}' AND s.name = '{sn}' "
            "AND ep.minor_id = 0 AND ep.name = 'MS_Description'"
        ).fetchone()
        if row and row[0]:
            table_comment = row[0]
        rows = raw(
            "SELECT c.name, CAST(ep.value AS NVARCHAR(MAX)) "
            "FROM sys.extended_properties ep "
            "JOIN sys.columns c ON ep.major_id = c.object_id "
            "  AND ep.minor_id = c.column_id "
            "JOIN sys.tables t ON c.object_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE t.name = '{qt}' AND s.name = '{sn}' "
            "AND ep.name = 'MS_Description'"
        ).fetchall()
        for r in rows:
            if r[1]:
                col_comments[r[0]] = r[1]

    elif backend == "duckdb":
        dsf = f" AND schema_name = '{_quote(schema)}'" if schema else ""
        row = raw(
            f"SELECT comment FROM duckdb_tables() WHERE table_name = '{qt}'{dsf}"
        ).fetchone()
        if row and row[0]:
            table_comment = row[0]
        rows = raw(
            "SELECT column_name, comment FROM duckdb_columns() "
            f"WHERE table_name = '{qt}'{dsf} AND comment IS NOT NULL"
        ).fetchall()
        col_comments = {r[0]: r[1] for r in rows}

    return table_comment, col_comments


def _indexed_info_schema(
    raw: Any,
    backend: str,
    schema: str | None,
    table: str,
) -> set[str]:
    qt = _quote(table)

    if backend == "postgres":
        sn = _quote(schema or "public")
        rows = raw(
            "SELECT a.attname "
            "FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid "
            "  AND a.attnum = ANY(i.indkey) "
            f"WHERE c.relname = '{qt}' AND n.nspname = '{sn}'"
        ).fetchall()
        return {r[0] for r in rows}

    if backend == "mysql":
        sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""
        rows = raw(
            "SELECT DISTINCT column_name "
            "FROM information_schema.statistics "
            f"WHERE table_name = '{qt}'{sf}"
        ).fetchall()
        return {r[0] for r in rows}

    if backend == "mssql":
        sn = _quote(schema or "dbo")
        rows = raw(
            "SELECT c.name "
            "FROM sys.index_columns ic "
            "JOIN sys.columns c ON ic.object_id = c.object_id "
            "  AND ic.column_id = c.column_id "
            "JOIN sys.tables t ON ic.object_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE t.name = '{qt}' AND s.name = '{sn}'"
        ).fetchall()
        return {r[0] for r in rows}

    return set()


def _auto_inc_info_schema(
    raw: Any,
    backend: str,
    schema: str | None,
    table: str,
) -> set[str]:
    qt = _quote(table)

    if backend == "postgres":
        sn = _quote(schema or "public")
        rows = raw(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{qt}' AND table_schema = '{sn}' "
            "AND (column_default LIKE 'nextval%' OR is_identity = 'YES')"
        ).fetchall()
        return {r[0] for r in rows}

    if backend == "mysql":
        sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""
        rows = raw(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{qt}'{sf} "
            "AND extra LIKE '%auto_increment%'"
        ).fetchall()
        return {r[0] for r in rows}

    if backend == "mssql":
        sn = _quote(schema or "dbo")
        rows = raw(
            "SELECT c.name "
            "FROM sys.columns c "
            "JOIN sys.tables t ON c.object_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE t.name = '{qt}' AND s.name = '{sn}' "
            "AND c.is_identity = 1"
        ).fetchall()
        return {r[0] for r in rows}

    if backend == "duckdb":
        sf = f" AND table_schema = '{_quote(schema)}'" if schema else ""
        rows = raw(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{qt}'{sf} "
            "AND column_default LIKE 'nextval%'"
        ).fetchall()
        return {r[0] for r in rows}

    return set()
