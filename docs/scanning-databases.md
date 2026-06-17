# Scanning databases

Use [Scan depth](/scan-depth) to choose how much metadata datannurpy extracts. The same `depth` setting applies to `add_database`, `add_folder`, and `add_dataset`, either globally or per `add` entry.

## Connection strings

| Backend    | Format                                              |
| ---------- | --------------------------------------------------- |
| SQLite     | `sqlite:///path/to/db.sqlite`                       |
| GeoPackage | `sqlite:///path/to/geodata.gpkg`                    |
| PostgreSQL | `postgresql://user:pass@host:5432/database`         |
| MySQL      | `mysql://user:pass@host:3306/database`              |
| Oracle     | `oracle://user:pass@host:1521/service_name`         |
| SQL Server | `mssql://user:pass@host:1433/database`              |
| Remote SQLite | `sftp://host/path/db.sqlite`                     |
| DuckDB     | pass an `ibis.duckdb.connect(...)` backend directly |

## Basic usage

```yaml
add:
  - database: sqlite:///path/to/db.sqlite
  - database: postgresql://user:pass@host:5432/mydb
  - database: mysql://user:pass@host:3306/mydb
  - database: oracle://user:pass@host:1521/service_name
  - database: mssql://user:pass@host:1433/mydb
```

## SSL/TLS

```yaml
add:
  - database: postgresql://user:pass@host/db?sslmode=require
```

## SQL Server with Windows authentication

Requires proper Kerberos setup.

```yaml
add:
  - database: mssql://host/db?TrustedConnection=yes
```

## Filtering and sampling

```yaml
add:
  - database: postgresql://localhost/mydb
    schema: public
    include: ["sales_*"]
    exclude: ["*_tmp"]
    sample_size: 10000
    preview_rows: 25
    group_by_prefix: true       # group tables by common prefix (default)
    prefix_min_tables: 2        # minimum tables to form a group
```

  `include` and `exclude` are matched against table names after the optional `schema` filter has selected which schema(s) to scan. They use standard glob-style patterns (`*`, `?`, and character classes such as `[abc]`), not filesystem paths. Filtering first keeps tables that match at least one `include` pattern when `include` is set, then removes tables that match any `exclude` pattern.

  `sample_size` controls rows used for frequency and automatic enumeration detection. Set `auto_enumerations: false` globally or on a database entry to keep `depth: value` frequencies without creating automatic enumeration entities or generated variable links. `preview_rows` controls the maximum rows exported for each table preview at `stat` and `value` depth; the default is `100`, and `0` or `false` disables previews. Table-series datasets preview the latest period table, matching the statistics scan.

  Examples:

  | Pattern | Meaning |
  | ------- | ------- |
  | `employees` | Exact table name |
  | `sales_*` | Tables whose names start with `sales_` |
  | `*_tmp` | Tables whose names end with `_tmp` |
  | `fact_????` | Tables such as `fact_2024` |
  | `[de]*` | Tables starting with `d` or `e` |

## Time series detection

When `time_series: true` (default), tables whose names differ only by a temporal pattern are grouped into a single dataset after `include` and `exclude` filters are applied:

```text
sales_fact_202401
sales_fact_202402
sales_fact_202403
```

This creates one dataset named `sales_fact_[YYYY/MM]` with `nb_resources=3`. Set `time_series: false` to treat each table as a separate dataset.

See [Time series grouping](/time-series) for supported patterns, file grouping, schema evolution, and false-positive rules.

## Multiple schemas

```yaml
add:
  - database: postgresql://localhost/mydb
    schema: [public, sales, hr]
```

## SSH tunnel

For databases behind a firewall. Requires `pip install datannurpy[ssh]`.

```yaml
add:
  - database: mysql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: ssh.example.com
      user: sshuser
```

With more options:

```yaml
add:
  - database: postgresql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: bastion.example.com
      port: 2222
      user: admin
      key_file: ~/.ssh/id_rsa
```

## Remote SQLite / GeoPackage

SQLite and GeoPackage files can be scanned over SFTP or cloud storage. The `storage_options` dict is passed to [fsspec](https://filesystem-spec.readthedocs.io/).

```yaml
add:
  - database: sftp://host/path/to/db.sqlite
    storage_options:
      key_filename: /path/to/key

  - database: s3://bucket/geodata.gpkg
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

See [Remote storage](/scanning-files#remote-storage) for the list of supported providers and required extras.

## Database metadata enrichment

Requires [`depth: variable` or higher](/scan-depth).

| Metadata                | Target field          | Backends           |
| ----------------------- | --------------------- | ------------------ |
| Primary keys            | `Variable.key`        | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Foreign keys            | `Variable.fk_variable_id`  | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Table/column comments   | `description`         | PostgreSQL, MySQL, Oracle, SQL Server, DuckDB |
| NOT NULL, UNIQUE, INDEX | Auto tags (`db---*`)  | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Auto-increment          | Auto tag              | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |

This metadata is always refreshed, even when table data is unchanged (cache hit).

