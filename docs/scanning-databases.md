# Scanning databases

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
    group_by_prefix: true       # group tables by common prefix (default)
    prefix_min_tables: 2        # minimum tables to form a group
```

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

Requires `depth: variable` or higher.

| Metadata                | Target field          | Backends           |
| ----------------------- | --------------------- | ------------------ |
| Primary keys            | `Variable.key`        | All 6              |
| Foreign keys            | `Variable.fk_var_id`  | All 6              |
| Table/column comments   | `description`         | All except SQLite  |
| NOT NULL, UNIQUE, INDEX | Auto tags (`db---*`)  | All 6              |
| Auto-increment          | Auto tag              | All 6              |

This metadata is always refreshed, even when table data is unchanged (cache hit).

