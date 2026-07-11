# Datenbanken scannen

Über die [Scan-Tiefe](/de/scan-depth) wird festgelegt, wie viele Metadaten datannurpy extrahiert. Dieselbe `depth`-Einstellung gilt für `add_database`, `add_folder` und `add_dataset`, entweder global oder pro `add`-Eintrag.

## Verbindungszeichenfolgen

| Backend    | Format                                              |
| ---------- | --------------------------------------------------- |
| SQLite     | `sqlite:///path/to/db.sqlite`                       |
| GeoPackage | `sqlite:///path/to/geodata.gpkg`                    |
| PostgreSQL | `postgresql://user:pass@host:5432/database`         |
| MySQL      | `mysql://user:pass@host:3306/database`              |
| Oracle     | `oracle://user:pass@host:1521/service_name`         |
| SQL Server | `mssql://user:pass@host:1433/database`              |
| Entferntes SQLite | `sftp://host/path/db.sqlite`                 |
| DuckDB     | ein `ibis.duckdb.connect(...)`-Backend direkt übergeben |

Ein GeoPackage ist eine SQLite-Datei und wird daher als Datenbank gescannt — jeder Layer wird zu einem Datensatz, zusätzlich angereichert mit `crs`/`geometry_type`/`bbox`. Siehe [Geodatenformate](/de/scanning-files#geospatial-formats).

## Grundlegende Verwendung

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

## SQL Server mit Windows-Authentifizierung

Erfordert eine korrekte Kerberos-Einrichtung.

```yaml
add:
  - database: mssql://host/db?TrustedConnection=yes
```

## Filterung und Sampling

```yaml
add:
  - database: postgresql://localhost/mydb
    schema: public
    include: ["sales_*"]
    exclude: ["*_tmp"]
    sample_size: 10000
    preview_rows: 25
    group_by_prefix: true       # Tabellen nach gemeinsamem Präfix gruppieren (Standard)
    prefix_min_tables: 2        # Mindestanzahl Tabellen für eine Gruppe
```

  `include` und `exclude` werden gegen Tabellennamen abgeglichen, nachdem der optionale `schema`-Filter festgelegt hat, welche(s) Schema(ta) gescannt werden. Sie verwenden Standard-Glob-Muster (`*`, `?` und Zeichenklassen wie `[abc]`), keine Dateisystempfade. Die Filterung behält zuerst Tabellen, die mindestens einem `include`-Muster entsprechen (sofern `include` gesetzt ist), und entfernt anschließend Tabellen, die einem `exclude`-Muster entsprechen.

  `sample_size` steuert die Zeilen, die für Häufigkeits- und automatische Enumerationserkennung verwendet werden. `auto_enumerations: false` global oder auf einem Datenbankeintrag setzen, um die `depth: value`-Häufigkeiten zu behalten, ohne automatische Enumerationsentitäten oder generierte Variablenverknüpfungen zu erzeugen. `preview_rows` steuert die maximale Zeilenzahl, die für jede Tabellenvorschau bei den Tiefen `stat` und `value` exportiert wird; der Standard ist `100`, und `0` oder `false` deaktiviert Vorschauen. Tabellen-Serien-Datensätze zeigen die Tabelle der letzten Periode in der Vorschau, passend zum Statistik-Scan.

  Beispiele:

  | Muster | Bedeutung |
  | ------- | ------- |
  | `employees` | Exakter Tabellenname |
  | `sales_*` | Tabellen, deren Namen mit `sales_` beginnen |
  | `*_tmp` | Tabellen, deren Namen auf `_tmp` enden |
  | `fact_????` | Tabellen wie `fact_2024` |
  | `[de]*` | Tabellen, die mit `d` oder `e` beginnen |

## Zeitreihen-Erkennung

Bei `time_series: true` (Standard) werden Tabellen, deren Namen sich nur durch ein zeitliches Muster unterscheiden, nach Anwendung der `include`- und `exclude`-Filter zu einem einzigen Datensatz gruppiert:

```text
sales_fact_202401
sales_fact_202402
sales_fact_202403
```

Dies erzeugt einen Datensatz namens `sales_fact_[YYYY/MM]` mit `nb_resources=3`. Mit `time_series: false` wird jede Tabelle als eigener Datensatz behandelt.

Siehe [Zeitreihen-Gruppierung](/de/time-series) für unterstützte Muster, Dateigruppierung, Schema-Evolution und Regeln gegen Fehlerkennungen.

## Mehrere Schemata

```yaml
add:
  - database: postgresql://localhost/mydb
    schema: [public, sales, hr]
```

## SSH-Tunnel

Für Datenbanken hinter einer Firewall. Erfordert `pip install datannurpy[ssh]`.

```yaml
add:
  - database: mysql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: ssh.example.com
      user: sshuser
```

Mit weiteren Optionen:

```yaml
add:
  - database: postgresql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: bastion.example.com
      port: 2222
      user: admin
      key_file: ~/.ssh/id_rsa
```

## Entferntes SQLite / GeoPackage

SQLite- und GeoPackage-Dateien können über SFTP oder Cloud-Speicher gescannt werden. Das `storage_options`-Dict wird an [fsspec](https://filesystem-spec.readthedocs.io/) weitergereicht.

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

Siehe [Remote-Speicher](/de/scanning-files#remote-storage) für die Liste der unterstützten Anbieter und erforderlichen Extras.

## Anreicherung mit Datenbank-Metadaten

Erfordert [`depth: variable` oder höher](/de/scan-depth).

| Metadaten               | Zielfeld              | Backends           |
| ----------------------- | --------------------- | ------------------ |
| Primärschlüssel         | `Variable.key`        | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Fremdschlüssel          | `Variable.fk_variable_id`  | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Tabellen-/Spaltenkommentare | `description`     | PostgreSQL, MySQL, Oracle, SQL Server, DuckDB |
| NOT NULL, UNIQUE, INDEX | Auto-Tags (`db---*`)  | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Auto-Increment          | Auto-Tag              | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |

Diese Metadaten werden immer aktualisiert, auch wenn die Tabellendaten unverändert sind (Cache-Treffer).
