# Scanning databases

## Scanning databases


```yaml
add:
  # SQLite / GeoPackage
  - database: sqlite:///path/to/db.sqlite
  - database: sqlite:///path/to/geodata.gpkg

  # PostgreSQL / MySQL / Oracle / SQL Server
  - database: postgresql://user:pass@host:5432/mydb
  - database: mysql://user:pass@host:3306/mydb
  - database: oracle://user:pass@host:1521/service_name
  - database: mssql://user:pass@host:1433/mydb

  # SSL/TLS
  - database: postgresql://user:pass@host/db?sslmode=require

  # SQL Server with Windows auth (requires proper Kerberos setup)
  - database: mssql://host/db?TrustedConnection=yes

  # With options
  - database: postgresql://localhost/mydb
    schema: public
    include: ["sales_*"]
    exclude: ["*_tmp"]
    sample_size: 10000
    group_by_prefix: true       # group tables by common prefix (default)
    prefix_min_tables: 2        # minimum tables to form a group

  # Multiple schemas
  - database: postgresql://localhost/mydb
    schema: [public, sales, hr]

  # SSH tunnel (for databases behind a firewall)
  # Requires: pip install datannurpy[ssh]
  - database: mysql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: ssh.example.com
      user: sshuser

  # SSH tunnel with more options
  - database: postgresql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: bastion.example.com
      port: 2222
      user: admin
      key_file: ~/.ssh/id_rsa
```

**Connection string formats:**

- SQLite: `sqlite:///path/to/db.sqlite` or `sftp://host/path/db.sqlite` (remote)
- PostgreSQL: `postgresql://user:pass@host:5432/database`
- MySQL: `mysql://user:pass@host:3306/database`
- Oracle: `oracle://user:pass@host:1521/service_name`
- SQL Server: `mssql://user:pass@host:1433/database`
- DuckDB: pass an `ibis.duckdb.connect(...)` backend directly (no connection string)

**Database metadata enrichment** (requires `depth: variable` or higher):

| Metadata                | Target field          | Backends           |
| ----------------------- | --------------------- | ------------------ |
| Primary keys            | `Variable.key`        | All 6              |
| Foreign keys            | `Variable.fk_var_id`  | All 6              |
| Table/column comments   | `description`         | All except SQLite  |
| NOT NULL, UNIQUE, INDEX | Auto tags (`db---*`)  | All 6              |
| Auto-increment          | Auto tag              | All 6              |

This metadata is always refreshed, even when table data is unchanged (cache hit).
