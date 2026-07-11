# Per iniziare

Libreria Python per la gestione dei metadati del catalogo [datannur](https://github.com/datannur/datannur).

## Formati supportati


Un catalogo leggero compatibile con la maggior parte delle sorgenti dati:

| Categoria          | Formati                                               |
| ------------------ | ----------------------------------------------------- |
| **Fogli di calcolo** | CSV, Excel (.xlsx, .xls)                            |
| **Colonnari**      | Parquet, Delta Lake, Apache Iceberg, partizionamento Hive |
| **Statistici**     | SAS (.sas7bdat), SPSS (.sav), Stata (.dta)            |
| **Geospaziali**    | GeoJSON, Shapefile, GeoPackage, GeoParquet, GeoTIFF, GML, KML, ESRI File Geodatabase |
| **Database**       | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| **Storage remoto** | URL HTTP(S) pubblici, SFTP, Amazon S3, Azure Blob Storage, Google Cloud Storage |

Tutti i formati di dati scansionati supportano l'inferenza automatica dello schema e il calcolo delle statistiche.

## Installazione


```bash
pip install datannurpy
```

### Extra opzionali

```bash
# Database
pip install datannurpy[postgres]  # PostgreSQL
pip install datannurpy[mysql]     # MySQL
pip install datannurpy[oracle]    # Oracle
pip install datannurpy[mssql]     # SQL Server
pip install datannurpy[ssh]       # SFTP e tunneling SSH verso database remoti

# Formati di file
pip install datannurpy[stat]      # SAS, SPSS, Stata
pip install datannurpy[delta]     # estrazione dei metadati Delta Lake
pip install datannurpy[iceberg]   # estrazione dei metadati Apache Iceberg
pip install datannurpy[geo]       # formati geospaziali + esportazioni ISO 19139 / STAC

# Cloud storage
pip install datannurpy[s3]        # Amazon S3
pip install datannurpy[azure]     # Azure Blob Storage
pip install datannurpy[gcs]       # Google Cloud Storage
pip install datannurpy[cloud]     # tutti i provider cloud

# Extra multipli
pip install datannurpy[postgres,stat,delta]
```

> **Nota:** gli extra `iceberg`, `s3`, `azure`, `gcs` e `cloud` richiedono Python 3.10+.

**Nota per SQL Server:** richiede un driver ODBC sul sistema:

- macOS: `brew install unixodbc freetds`
- Linux: `apt install unixodbc-dev tdsodbc`
- Windows: [Microsoft ODBC Driver](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

### Installazione air-gapped

Per ambienti senza accesso diretto a internet (proxy aziendali restrittivi,
reti classificate), scaricare i wheel su una macchina connessa e trasferirli
sulla macchina di destinazione:

```bash
# Su una macchina connessa (adattare OS/Python di destinazione se diversi):
pip download 'datannurpy[postgres,ssh]' -d ./wheels/ \
    --platform manylinux2014_x86_64 --python-version 3.11 \
    --only-binary=:all:

# Sulla macchina air-gapped di destinazione:
pip install --no-index --find-links ./wheels/ 'datannurpy[postgres,ssh]'
```

Omettere `--platform`/`--python-version` se sorgente e destinazione condividono
lo stesso OS e la stessa versione di Python. Un SBOM CycloneDX
(`*-sbom.cyclonedx.json`) viene pubblicato come asset della GitHub Release per
ogni versione, a supporto degli audit CVE con strumenti come Dependency-Track
o Grype.

## Avvio rapido

Creare un file `catalog.yml` che indichi a datannurpy dove scansionare i dati e dove esportare il catalogo:

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

Questo comando scansiona i file e il database configurati, scrive un'app datannur autonoma in `./my-catalog` e la apre nel browser. Rieseguendo la stessa configurazione, il catalogo esistente viene aggiornato in modo incrementale: file e tabelle invariati vengono riutilizzati quando possibile.

Per le opzioni di esportazione come `update_app`, `copy_assets` o `post_export`, vedere [Output & esportazioni](/it/output). Per le opzioni di configurazione come `metadata_path`, `env`, `env_file` o `app_config`, vedere [Metadati & configurazione](/it/metadata).

Usare `output_dir` al posto di `app_path` quando servono soltanto i metadati JSON per un'app datannur esistente:

```yaml
output_dir: ./metadata-export

add:
  - folder: ./data
```

La maggior parte dei progetti parte da questo flusso: scegliere le sorgenti in `add`, scegliere una [profondità di scansione](/it/scan-depth), eventualmente arricchire il catalogo con [metadati manuali](/it/metadata), quindi esportare un'app completa oppure i metadati JSON. Che l'output sia un artefatto CI usa e getta o un'installazione permanente in loco determina cosa è possibile modificare — vedere [Ciclo di vita del catalogo](/it/output#catalog-lifecycle).

Oppure usare l'API Python:

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
python -m datannurpy --help     # mostra l'uso
python -m datannurpy --version  # mostra la versione
```
