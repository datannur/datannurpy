# Erste Schritte

Python-Bibliothek für die Metadaten-Verwaltung des [datannur](https://github.com/datannur/datannur)-Katalogs.

## Unterstützte Formate


Ein leichtgewichtiger Katalog, kompatibel mit den meisten Datenquellen:

| Kategorie          | Formate                                               |
| ------------------ | ----------------------------------------------------- |
| **Tabellenkalkulation** | CSV, Excel (.xlsx, .xls), OpenDocument (.ods)    |
| **Spaltenorientiert** | Parquet, Delta Lake, Apache Iceberg, Hive-partitioniert |
| **Statistik**      | SAS (.sas7bdat), SPSS (.sav), Stata (.dta)            |
| **Geodaten**       | GeoJSON, Shapefile, GeoPackage, GeoParquet, GeoTIFF, GML, KML, GPX, ESRI File Geodatabase |
| **Datenbanken**    | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| **Remote-Speicher** | Öffentliche HTTP(S)-URLs, SFTP, Amazon S3, Azure Blob Storage, Google Cloud Storage |

Alle gescannten Datenformate unterstützen automatische Schema-Erkennung und Statistikberechnung.

## Installation


```bash
pip install datannurpy
```

### Optionale Extras

```bash
# Datenbanken
pip install datannurpy[postgres]  # PostgreSQL
pip install datannurpy[mysql]     # MySQL
pip install datannurpy[oracle]    # Oracle
pip install datannurpy[mssql]     # SQL Server
pip install datannurpy[ssh]       # SFTP und SSH-Tunneling zu entfernten Datenbanken

# Dateiformate
pip install datannurpy[stat]      # SAS, SPSS, Stata
pip install datannurpy[delta]     # Delta-Lake-Metadatenextraktion
pip install datannurpy[iceberg]   # Apache-Iceberg-Metadatenextraktion
pip install datannurpy[geo]       # Geodatenformate + ISO 19139 / STAC-Exporte

# Cloud-Speicher
pip install datannurpy[s3]        # Amazon S3
pip install datannurpy[azure]     # Azure Blob Storage
pip install datannurpy[gcs]       # Google Cloud Storage
pip install datannurpy[cloud]     # Alle Cloud-Anbieter

# Mehrere Extras
pip install datannurpy[postgres,stat,delta]
```

> **Hinweis:** Die Extras `iceberg`, `s3`, `azure`, `gcs` und `cloud` erfordern Python 3.10+.

**Hinweis zu SQL Server:** Erfordert einen ODBC-Treiber auf dem System:

- macOS: `brew install unixodbc freetds`
- Linux: `apt install unixodbc-dev tdsodbc`
- Windows: [Microsoft ODBC Driver](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

### Installation ohne Internetzugang (Air-Gap)

Für Umgebungen ohne direkten Internetzugang (strikte Unternehmens-Proxys,
klassifizierte Netzwerke) die Wheels auf einer verbundenen Maschine herunterladen und
auf das Zielsystem übertragen:

```bash
# Auf einer verbundenen Maschine (bei Abweichung Ziel-OS/-Python angeben):
pip download 'datannurpy[postgres,ssh]' -d ./wheels/ \
    --platform manylinux2014_x86_64 --python-version 3.11 \
    --only-binary=:all:

# Auf dem Air-Gap-Zielsystem:
pip install --no-index --find-links ./wheels/ 'datannurpy[postgres,ssh]'
```

`--platform`/`--python-version` weglassen, wenn Quelle und Ziel dasselbe
Betriebssystem und dieselbe Python-Version verwenden. Für jede Version wird ein
CycloneDX-SBOM (`*-sbom.cyclonedx.json`) als GitHub-Release-Asset veröffentlicht,
um CVE-Audits mit Werkzeugen wie Dependency-Track oder Grype zu unterstützen.

## Schnellstart

Eine `catalog.yml`-Datei anlegen, die datannurpy mitteilt, wo Daten gescannt und wohin der Katalog exportiert werden soll:

```yaml
# catalog.yml
app_path: ./my-catalog
open_browser: true

add:
  - folder: ./data
    include: ["*.csv", "*.xlsx", "*.parquet"]

  - database: sqlite:///mydb.sqlite
```

```bash
python -m datannurpy catalog.yml
```

Dies scannt die konfigurierten Dateien und die Datenbank, schreibt eine eigenständige datannur-App nach `./my-catalog` und öffnet sie im Browser. Ein erneuter Lauf mit derselben Konfiguration aktualisiert den bestehenden Katalog inkrementell, sodass unveränderte Dateien und Tabellen nach Möglichkeit wiederverwendet werden.

Für Export-Optionen wie `update_app`, `copy_assets` oder `post_export` siehe [Ausgabe & Exporte](/de/output). Für Konfigurationsoptionen wie `metadata_path`, `env`, `env_file` oder `app_config` siehe [Metadaten & Konfiguration](/de/metadata).

`output_dir` statt `app_path` verwenden, wenn nur JSON-Metadaten für eine bestehende datannur-App benötigt werden:

```yaml
output_dir: ./metadata-export

add:
  - folder: ./data
```

Die meisten Projekte beginnen mit diesem Ablauf: die Quellen in `add` festlegen, eine [Scan-Tiefe](/de/scan-depth) wählen, den Katalog optional mit [manuellen Metadaten](/de/metadata) anreichern und dann entweder eine vollständige App oder JSON-Metadaten exportieren. Ob die Ausgabe ein kurzlebiges CI-Artefakt oder eine langlebige In-Place-Installation ist, bestimmt, was bearbeitet werden darf — siehe [Katalog-Lebenszyklus](/de/output#catalog-lifecycle).

Oder die Python-API verwenden:

```python
from datannurpy import Catalog

catalog = Catalog()
catalog.add_folder("./data", include=["*.csv", "*.xlsx", "*.parquet"])
catalog.add_database("sqlite:///mydb.sqlite")
catalog.export_app("./my-catalog", open_browser=True)
```

## CLI


```bash
python -m datannurpy catalog.yml
python -m datannurpy --help     # Verwendung anzeigen
python -m datannurpy --version  # Version anzeigen
```
