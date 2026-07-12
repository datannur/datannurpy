# Démarrage

Bibliothèque Python de gestion des métadonnées du catalogue [datannur](https://github.com/datannur/datannur).

## Formats pris en charge


Un catalogue léger compatible avec la plupart des sources de données :

| Catégorie            | Formats                                               |
| -------------------- | ----------------------------------------------------- |
| **Tableurs**         | CSV, Excel (.xlsx, .xls), OpenDocument (.ods)         |
| **Colonnaires**      | Parquet, Delta Lake, Apache Iceberg, partitionnement Hive |
| **Statistiques**     | SAS (.sas7bdat), SPSS (.sav), Stata (.dta)            |
| **Géospatiaux**      | GeoJSON, Shapefile, GeoPackage, GeoParquet, GeoTIFF, GML, KML, GPX, ESRI File Geodatabase |
| **Bases de données** | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| **Stockage distant** | URLs HTTP(S) publiques, SFTP, Amazon S3, Azure Blob Storage, Google Cloud Storage |

Tous les formats de données scannés prennent en charge l'inférence automatique de schéma et le calcul de statistiques.

## Installation


```bash
pip install datannurpy
```

### Extras optionnels

```bash
# Bases de données
pip install datannurpy[postgres]  # PostgreSQL
pip install datannurpy[mysql]     # MySQL
pip install datannurpy[oracle]    # Oracle
pip install datannurpy[mssql]     # SQL Server
pip install datannurpy[ssh]       # SFTP et tunnel SSH vers des bases distantes

# Formats de fichiers
pip install datannurpy[stat]      # SAS, SPSS, Stata
pip install datannurpy[delta]     # extraction des métadonnées Delta Lake
pip install datannurpy[iceberg]   # extraction des métadonnées Apache Iceberg
pip install datannurpy[geo]       # formats géospatiaux + exports ISO 19139 / STAC

# Stockage cloud
pip install datannurpy[s3]        # Amazon S3
pip install datannurpy[azure]     # Azure Blob Storage
pip install datannurpy[gcs]       # Google Cloud Storage
pip install datannurpy[cloud]     # tous les fournisseurs cloud

# Plusieurs extras
pip install datannurpy[postgres,stat,delta]
```

> **Note :** Les extras `iceberg`, `s3`, `azure`, `gcs` et `cloud` requièrent Python 3.10+.

**Note SQL Server :** nécessite un pilote ODBC sur le système :

- macOS : `brew install unixodbc freetds`
- Linux : `apt install unixodbc-dev tdsodbc`
- Windows : [Microsoft ODBC Driver](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

### Installation hors ligne (air-gapped)

Pour les environnements sans accès direct à internet (proxies d'entreprise
stricts, réseaux classifiés), télécharger les wheels sur une machine connectée
puis les transférer vers la machine cible :

```bash
# Sur une machine connectée (adapter l'OS/Python de la cible si différents) :
pip download 'datannurpy[postgres,ssh]' -d ./wheels/ \
    --platform manylinux2014_x86_64 --python-version 3.11 \
    --only-binary=:all:

# Sur la machine cible isolée :
pip install --no-index --find-links ./wheels/ 'datannurpy[postgres,ssh]'
```

Omettre `--platform`/`--python-version` si la source et la cible partagent
le même OS et la même version de Python. Un SBOM CycloneDX
(`*-sbom.cyclonedx.json`) est publié comme asset de chaque GitHub Release
pour permettre les audits CVE avec des outils comme Dependency-Track ou Grype.

## Démarrage rapide

Créer un fichier `catalog.yml` qui indique à datannurpy où scanner les données et où exporter le catalogue :

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

Cette commande scanne les fichiers et la base de données configurés, écrit une app datannur autonome dans `./my-catalog` et l'ouvre dans le navigateur. Relancer la même configuration met à jour le catalogue existant de manière incrémentale : les fichiers et tables inchangés sont réutilisés quand c'est possible.

Pour les options d'export telles que `update_app`, `copy_assets` ou `post_export`, voir [Sortie & exports](/fr/output). Pour les options de configuration telles que `metadata_path`, `env`, `env_file` ou `app_config`, voir [Métadonnées & configuration](/fr/metadata).

Utiliser `output_dir` au lieu de `app_path` pour n'obtenir que les métadonnées JSON destinées à une app datannur existante :

```yaml
output_dir: ./metadata-export

add:
  - folder: ./data
```

La plupart des projets commencent par ce flux : choisir les sources dans `add`, choisir une [profondeur de scan](/fr/scan-depth), éventuellement enrichir le catalogue avec des [métadonnées manuelles](/fr/metadata), puis exporter soit une app complète soit des métadonnées JSON. Le fait que la sortie soit un artefact CI jetable ou une installation pérenne en place détermine ce que vous pouvez éditer — voir [Cycle de vie du catalogue](/fr/output#catalog-lifecycle).

Ou utiliser l'API Python :

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
python -m datannurpy --help     # afficher l'aide
python -m datannurpy --version  # afficher la version
```
