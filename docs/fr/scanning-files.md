# Scanner des fichiers

Utilisez la [Profondeur de scan](/fr/scan-depth) pour choisir la quantité de métadonnées extraites par datannurpy. Le même paramètre `depth` s'applique à `add_folder`, `add_dataset`, `add_database` et `add_geodatabase`, globalement ou par entrée `add`.

## Scanner des fichiers


```yaml
add:
  # Scanner un dossier (CSV, Excel, SAS)
  - folder: ./data

  # Avec des métadonnées de dossier personnalisées
  - folder: ./data
    id: prod
    name: Production

  # Avec des options de filtrage
  - folder: ./data
    include: ["*.csv", "*.xlsx"]
    exclude: ["**/tmp/**"]
    recursive: true
    csv_encoding: utf-8        # ou cp1252, iso-8859-1 (auto-détecté par défaut)

  # Plusieurs dossiers avec options partagées
  - folder: [./data/sales, ./data/hr]
    include: ["*.csv"]

  # Un fichier unique
  - dataset: ./data/sales.csv

  # Plusieurs fichiers
  - dataset:
      - ./data/sales.csv
      - ./data/products.csv
```

### Motifs de filtrage

Les motifs `include` et `exclude` sont comparés aux chemins relatifs normalisés depuis le dossier scanné. Les chemins utilisent le séparateur `/` sur toutes les plateformes, et les `/` ou `./` en tête de motif sont ignorés. Le filtrage conserve d'abord les fichiers qui correspondent à au moins un motif `include` quand `include` est défini, puis retire les fichiers qui correspondent à un motif `exclude`.

Exemples :

| Motif | Signification |
| ------- | ------- |
| `name.csv` | Fichier exact à la racine du dossier scanné |
| `subdir/name.csv` | Chemin relatif exact du fichier |
| `*.csv` | Tout fichier CSV à n'importe quelle profondeur |
| `subdir/*.csv` | Fichiers CSV directement dans `subdir` |
| `**/tmp/**` | Fichiers sous n'importe quel répertoire `tmp` |
| `tmp/` | Tout le contenu du répertoire `tmp` à la racine |
| `**/tmp/` | Tout le contenu de n'importe quel répertoire nommé `tmp` |

### Détection de séries temporelles

Quand `time_series: true` (défaut), les fichiers présentant des motifs temporels dans leur nom ou leurs dossiers parents sont automatiquement regroupés en un seul dataset :

```
data/
├── enquete_2020.csv    ─┐
├── enquete_2021.csv     ├─→ Dataset unique "enquete" avec nb_resources=3
├── enquete_2022.csv    ─┘
└── reference.csv       ─→ Dataset distinct "reference"
```

Le dataset résultant inclut `nb_resources`, `start_date` et `end_date`. Les variables portent leurs propres `start_date` et `end_date` quand leur présence change d'une période à l'autre.

Définissez `time_series: false` pour traiter chaque fichier comme un dataset distinct.

Voir [Regroupement de séries temporelles](/fr/time-series) pour les motifs pris en charge, le regroupement de tables de bases de données, l'évolution de schéma et les règles anti-faux positifs.

## Formats Parquet


Prise en charge des fichiers Parquet simples et des datasets partitionnés (Delta, Hive, Iceberg) :

```yaml
add:
  # add_folder détecte automatiquement tous les formats
  - folder: ./data             # scanne *.parquet + les répertoires Delta/Hive/Iceberg

  # Répertoire partitionné unique avec surcharge de métadonnées
  - dataset: ./data/sales_delta
    name: Sales Data
    description: Monthly sales
    folder:
      id: sales
      name: Sales
```

Avec les extras `[delta]` et `[iceberg]`, les métadonnées (nom, description, documentation des colonnes) sont extraites quand elles sont disponibles.

## Formats géospatiaux {#geospatial-formats}

datannurpy scanne les fichiers géospatiaux vectoriels et raster et enrichit chaque dataset de métadonnées spatiales. L'extra `geo` fournit le lecteur vectoriel (pyogrio), le lecteur raster (rasterio), la reprojection de CRS (pyproj) et les bibliothèques pour les exports ISO 19139 / STAC (pygeometa, pystac) :

```bash
pip install datannurpy[geo]
```

GeoPackage et GeoParquet sont lus sans l'extra ; leur `bbox` n'est reprojetée en WGS84 que lorsque pyproj est disponible.

```yaml
add:
  # Les fichiers vectoriels et raster sont auto-détectés par add_folder, comme tout autre format
  - folder: ./geodata          # *.geojson, *.shp, *.gml, *.kml, *.gpx, *.tif, *.parquet

  # Un fichier géospatial unique
  - dataset: ./geodata/parcels.shp

  # Un GeoPackage est un conteneur SQLite — scanné comme une base de données
  - database: sqlite:///./geodata/cadastre.gpkg

  # Une ESRI File Geodatabase est un conteneur multi-couches — un dataset par couche
  - geodatabase: ./geodata/cadastre.gdb
```

| Format | Extension | Ajouté avec |
| ------ | --------- | ---------- |
| GeoJSON | `.geojson` | `folder` / `dataset` |
| Shapefile | `.shp` (+ `.shx`/`.dbf`/`.prj`) | `folder` / `dataset` |
| Shapefile zippé | `.zip` (un `.shp` + fichiers annexes) | `dataset` |
| GML | `.gml` | `folder` / `dataset` |
| KML | `.kml` | `folder` / `dataset` |
| GPX | `.gpx` | `folder` / `dataset` |
| GeoTIFF (raster) | `.tif`, `.tiff` | `folder` / `dataset` |
| GeoParquet | `.parquet` | `folder` / `dataset` |
| GeoPackage | `.gpkg` | `database: sqlite:///…` |
| ESRI File Geodatabase | `.gdb` | `geodatabase` |

Un Shapefile est souvent distribué sous forme d'un unique `.zip` (IGN, Census TIGER, Eurostat …). Pointez un `dataset:` dessus et le Shapefile interne est extrait et scanné comme un Shapefile ordinaire — reprojection de CRS incluse — depuis n'importe quelle source :

```yaml
add:
  - dataset: https://data.example.org/ADMIN-EXPRESS_FRA.zip
```

La même approche fonctionne pour toute [archive zip](#archives-zip) contenant exactement un fichier scannable.

Les fichiers GPX, KML et GML peuvent contenir plusieurs couches (un GPX expose toujours waypoints, routes et tracks) ; scannés comme un seul `dataset:`, c'est la première couche non vide qui est lue — un enregistrement de trace sans waypoints livre donc bien ses tracks.

### Métadonnées spatiales

Chaque dataset spatial reçoit les champs suivants (laissés null pour les données non spatiales) :

| Champ | Description |
| ----- | ----------- |
| `crs` | Système de référence de coordonnées natif, par exemple `EPSG:2056` |
| `geometry_type` | Type de géométrie OGC (`point`, `polygon`, …) ; null pour les rasters et les couches mixtes |
| `bbox` | Emprise `west,south,east,north` en WGS84 (EPSG:4326), ordre lon/lat |
| `spatial_resolution` | Taille de pixel raster en mètres (CRS projeté uniquement) ; null pour les vecteurs |

Pour les couches vectorielles, les colonnes attributaires deviennent des variables avec le schéma et les statistiques habituels ; la géométrie elle-même est conservée comme variable binaire non profilée. Pour les rasters, chaque bande devient une variable (`type: band`) portant ses min/max/mean/std.

### Conteneurs multi-couches

GeoPackage et File Geodatabase contiennent plusieurs couches, chaque couche devient donc son propre dataset sous un dossier conteneur — exactement comme les tables de base de données. Un GeoPackage est un fichier SQLite, il est donc ajouté via `database:` (`sqlite:///…`) ; une File Geodatabase est un répertoire GDAL, elle dispose donc de sa propre entrée `geodatabase:` (ou `catalog.add_geodatabase(path)` en Python). Les deux acceptent des motifs glob `include`/`exclude` pour filtrer les couches par nom :

```yaml
add:
  - geodatabase: ./geodata/cadastre.gdb
    include: ["parcels_*"]      # uniquement les couches commençant par parcels_
    exclude: ["*_tmp"]
```

Les sources géospatiales fonctionnent sur le [stockage distant](#remote-storage) comme tout autre fichier : les Shapefiles distants récupèrent automatiquement leurs fichiers compagnons `.shx`/`.dbf`/`.prj`, et un répertoire File Geodatabase distant est téléchargé avant le scan.

## Options CSV


Éviter la copie temporaire UTF-8 quand les fichiers sont déjà locaux et en UTF-8 (repli automatique si la détection d'encodage échoue) :

```yaml
csv_skip_copy: true
```

## CSV compressé

Un CSV compressé en gzip (`.csv.gz`) est scanné comme tout autre fichier — depuis n'importe quelle source, à n'importe quelle [profondeur](./scan-depth.md) — et est catalogué exactement comme son jumeau non compressé (mêmes `id`/`name`, mêmes variables) :

```yaml
add:
  - dataset: ./data/sales.csv.gz
  - folder: sftp://user@host/exports    # *.csv.gz aux côtés de *.csv
```

Seul le **gzip de CSV** à flux unique est pris en charge pour `.gz` ; `.parquet.gz` ne l'est pas — extrayez-le d'abord. La compression au niveau du transport HTTP (`Content-Encoding: gzip`) est gérée automatiquement et ne nécessite rien de particulier.

## Archives zip

Un `.zip` contenant exactement un fichier de données — un fichier CSV, Excel, ODS ou Parquet, ou un Shapefile avec ses fichiers annexes — est scanné comme un `dataset:` depuis n'importe quelle source. Le membre est extrait de manière sûre (protection contre le Zip Slip et les bombes de décompression) et scanné comme un fichier ordinaire, à n'importe quelle [profondeur](./scan-depth.md) :

```yaml
add:
  - dataset: https://data.example.org/exports/sales.csv.zip
```

Les archives contenant plusieurs fichiers de données et la découverte automatique de zips lors d'un scan `folder:` ne sont pas prises en charge — extrayez-les d'abord.

## Stockage distant {#remote-storage}

Scannez des fichiers sur des URLs HTTP(S) publiques, des serveurs SFTP ou du stockage cloud (S3, Azure, GCS). Le dict `storage_options` est passé directement à [fsspec](https://filesystem-spec.readthedocs.io/) — voir la documentation de chaque fournisseur pour les options disponibles :

- [SFTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem)
- [S3](https://s3fs.readthedocs.io/en/latest/)
- [Azure](https://github.com/fsspec/adlfs)
- [GCS](https://gcsfs.readthedocs.io/en/latest/)

```yaml
env_file: .env               # SFTP_PASSWORD, AWS_KEY, AWS_SECRET, etc.
```

### SFTP

Nécessite `pip install datannurpy[ssh]`.

```yaml
add:
  - folder: sftp://user@host/path/to/data
    storage_options:
      password: ${SFTP_PASSWORD}   # ou key_filename: /path/to/key
```

### Amazon S3

Nécessite `pip install datannurpy[s3]`.

```yaml
add:
  - folder: s3://my-bucket/data
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

### Azure Blob

Nécessite `pip install datannurpy[azure]`.

```yaml
add:
  - folder: az://container/data
    storage_options:
      account_name: ${AZURE_ACCOUNT}
      account_key: ${AZURE_KEY}
```

### Google Cloud Storage

Nécessite `pip install datannurpy[gcs]`.

```yaml
add:
  - folder: gs://my-bucket/data
    storage_options:
      token: /path/to/credentials.json
```

### URLs HTTP(S) publiques

Pointez un `dataset:` sur une URL publique et obtenez variables documentées, statistiques et aperçu — la façon naturelle de cataloguer des données ouvertes déjà en ligne (portails open data, Zenodo, etc.). Fonctionne d'emblée (aucun extra à installer).

```yaml
add:
  - dataset: https://data.example.org/opendata/sales.csv
```

- Une URL correspond à un fichier — `dataset:` uniquement ; `folder:` (listage de répertoire) n'est pas pris en charge sur HTTP.
- Public, sans authentification.
- Les redirections sont suivies ; `https://` est recommandé.

**Détection de format.** Une URL avec une extension reconnue (`.csv`, `.xlsx`, …) fonctionne directement — toute chaîne de requête est ignorée (`.../sales.csv?token=…`). Les points de terminaison d'API n'ont souvent pas d'extension, le format est alors détecté automatiquement, dans l'ordre : le dernier segment de chemin utilisé comme jeton (`.../HCL_NOGA/multiplelevels/CSV`), un paramètre de requête `?format=` (ou WFS `?outputFormat=`), le `Content-Type` HTTP, et enfin l'analyse des premiers octets du contenu (au mieux, journalisée avec un avertissement ; ignorée à `depth: dataset`). Quand rien n'est concluant, l'exécution échoue en vous demandant de définir `format:`.

Définissez `format:` explicitement pour contourner la détection (ou la forcer quand les signaux se contredisent). Cela évite aussi la requête de détection — un petit gain de vitesse quand vous connaissez déjà le format, appréciable sur de nombreux points de terminaison d'une même API :

```yaml
add:
  # CSV servi par une API sans extension dans l'URL
  - dataset: https://www.i14y.admin.ch/api/Nomenclatures/HCL_NOGA/multiplelevels/CSV?language=fr
    format: csv
    id: noga_08
  # Excel servi par un point de terminaison ".../xls?SnapshotDate=..."
  - dataset: https://www.agvchapp.bfs.admin.ch/fr/state/results/xls?SnapshotDate=31.12.2025
    format: excel
    id: commune_district
  # Couche WFS : une URL GetFeature renvoyant du GeoJSON est un dataset ordinaire
  # (outputFormat=application/json ou json est détecté automatiquement comme geojson)
  - dataset: https://wfs.geo.example.ch/?service=WFS&version=2.0.0&request=GetFeature&typeName=ns:parcels&outputFormat=application/json
    id: parcels
```

Les valeurs `format:` acceptées sont les formats de livraison (`csv`, `excel`, `ods`, `parquet`, `sas`, `spss`, `stata`, `geojson`, `shapefile`, `gml`, `kml`, `gpx`, `geotiff`) ou une graphie d'extension correspondante (`xlsx`, `xls`, `pq`, …).
- Une URL manquante (404, erreur DNS, timeout) fait échouer l'exécution avec un code de sortie non nul, exactement comme un fichier local manquant — un build CI échoue en rouge plutôt que de publier un catalogue tronqué.
- Les scans incrémentaux utilisent l'en-tête `Last-Modified` du serveur : une URL est ignorée quand elle est inchangée, et sa date alimente `last_update_date`. Un serveur qui n'envoie pas de `Last-Modified` (par exemple un point de terminaison dynamique) est rescanné à chaque exécution.

### Fichier distant unique

```yaml
add:
  - dataset: s3://my-bucket/data/sales.parquet
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

## Échantillonnage


Par défaut, `sample_size` vaut `100000`. Toutes les entrées héritent de cette valeur. Surchargez-la par entrée, ou définissez `null` pour la désactiver :

```yaml
sample_size: 100000               # défaut

add:
  - folder: ./data                # hérite de 100000

  - folder: ./small
    sample_size: null             # pas d'échantillonnage

  - database: postgresql://localhost/mydb
    sample_size: 50000            # surcharge
```

Pour désactiver l'échantillonnage globalement :

```yaml
sample_size: null
```

Quand un dataset compte plus de lignes que `sample_size`, un échantillon aléatoire uniforme est utilisé pour les comptages de fréquences et la détection automatique d'énumérations. Toutes les autres statistiques (`nb_row`, `nb_missing`, `nb_distinct`, `min`, `max`, `mean`, `std`) sont calculées sur le dataset complet.

Définissez `auto_enumerations: false` pour conserver les tables de fréquences de `depth: value` sans créer d'entités d'énumération automatiques ni de liens de variables générés. C'est utile quand les énumérations sont fournies manuellement via `metadata_path`. Comme `sample_size`, ce paramètre peut être défini globalement ou par entrée `add`.

Le nombre réel de lignes échantillonnées est enregistré dans `Dataset.sample_size` (`null` quand aucun échantillonnage n'a été appliqué).

## Aperçus de datasets

Par défaut, `preview_rows` vaut `100`. Aux profondeurs `stat` et `value`, chaque dataset scanné exporte jusqu'à ce nombre de lignes dans `preview/<dataset_id>.json` et `preview/<dataset_id>.json.js`. Ces lignes proviennent quand c'est possible des données déjà lues pendant le scan, y compris des échantillons réservoir utilisés pour la détection de fréquences.

Surchargez la limite par source de fichiers, ou définissez `false` pour désactiver les aperçus d'une source tout en conservant le défaut global :

```yaml
preview_rows: 100

add:
  - folder: ./data/public
    preview_rows: 50

  - dataset: ./data/private.csv
    preview_rows: false
```

Les aperçus sont des données produites au moment du scan. Ils ne sont pas générés aux profondeurs `dataset` ou `variable`, et les commandes d'export n'offrent pas de surcharge `preview_rows` séparée.
