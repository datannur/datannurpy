# API Python

## `Catalog`

```python
Catalog(
    *,
    app_path=None,
    metadata_path=None,
    depth="value",
    refresh=False,
    on_scan_error="warn",
    on_metadata_error="warn",
    freq_threshold=100,
    auto_enumerations=True,
    csv_encoding=None,
    sample_size=100_000,
    preview_rows=100,
    csv_skip_copy=False,
    app_config=None,
    quiet=False,
    verbose=False,
    log_file=None,
)
```

| Attribut       | Type                              | Description                                        |
| -------------- | --------------------------------- | -------------------------------------------------- |
| app_path       | str \| Path \| None               | Chemin pour l'export d'app autonome ; s'il contient déjà `data/db`, il est réutilisé pour les scans incrémentaux |
| metadata_path  | str \| Path \| list[str \| Path] \| None | Dossier source de métadonnées, URI de base de données, ou liste de sources |
| depth          | "dataset" \| "variable" \| "stat" \| "value" | Profondeur de scan par défaut (défaut : "value")   |
| refresh        | bool                              | Forcer un rescan complet en ignorant le cache (défaut : False) |
| on_scan_error  | "warn" \| "fail"                  | Le scan continue toujours malgré les erreurs (un fichier/une table en échec est journalisé et ignoré). `"warn"` (défaut) sort avec le code 0 ; `"fail"` fait sortir le CLI avec le code 2 quand un dataset a échoué, pour que la CI ne publie pas en vert un catalogue tronqué |
| on_metadata_error | "warn" \| "fail"               | Le chargement des métadonnées continue aussi malgré les erreurs (une table invalide est journalisée et ignorée, les tables valides s'appliquent quand même). `"warn"` (défaut) sort avec le code 0 ; `"fail"` fait sortir le CLI avec le code 3 quand une table de métadonnées a échoué la validation, pour que la CI ne publie pas en vert un catalogue privé de sa curation |
| freq_threshold | int                               | Nombre maximum de valeurs distinctes pour la détection de fréquences et d'énumérations automatiques. Au-delà de ce seuil, les chaînes reçoivent des fréquences de motifs à la place |
| auto_enumerations | bool                           | Créer des énumérations automatiques à partir des fréquences de valeurs et les rattacher aux variables (défaut : True) |
| csv_encoding   | str \| None                       | Encodage CSV par défaut (utf-8, cp1252, etc.)      |
| sample_size    | int \| None                       | Taille d'échantillon par défaut pour la détection de fréquences et d'énumérations automatiques (défaut : 100_000) |
| preview_rows   | int \| Literal[False]             | Nombre maximum de lignes exportées par défaut dans les aperçus de datasets aux profondeurs `stat`/`value` (défaut : 100 ; 0 ou false désactive) |
| csv_skip_copy      | bool                              | Sauter la copie temporaire UTF-8 pour les CSV locaux (défaut : False) |
| app_config     | dict[str, str] \| None            | Configuration clé-valeur pour l'app web            |
| quiet          | bool                              | Supprimer la journalisation de progression (défaut : False) |
| verbose        | bool                              | Afficher les tracebacks complets en cas d'erreur (défaut : False) |
| log_file       | str \| Path \| None               | Écrire le journal complet du scan dans un fichier (tronqué à chaque exécution) |
| folder         | Table[Folder]                     | Table des dossiers (`.all()`, `.count`, `.get_by(...)`) |
| dataset        | Table[Dataset]                    | Table des datasets                                 |
| variable       | Table[Variable]                   | Table des variables                                |
| enumeration    | Table[Enumeration]                | Table des énumérations                             |
| value          | Table[Value]                      | Table des valeurs d'énumération                    |
| frequency      | Table[Frequency]                  | Table des fréquences (calculée)                    |
| organization   | Table[Organization]               | Table des organisations |
| tag            | Table[Tag]                        | Table des tags                                     |
| doc            | Table[Doc]                        | Table des documents                                |
| concept        | Table[Concept]                    | Table des concepts du glossaire métier             |
| config         | Table[Config]                     | Table clé-valeur de configuration de l'app web     |

## `Catalog.add_folder()`

```python
catalog.add_folder(
    path,
    metadata=None,
    *,
    depth=None,
    include=None,
    exclude=None,
    recursive=True,
    csv_encoding=None,
    sample_size=None,
    auto_enumerations=None,
    preview_rows=None,
    csv_skip_copy=None,
    storage_options=None,
    refresh=None,
    quiet=None,
    time_series=True,
    create_folders=True,
    on_unmatched="warn",
)
```

| Paramètre       | Type                                      | Défaut   | Description                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | requis   | Répertoire ou liste de répertoires à scanner  |
| metadata        | EntityMetadata \| None                    | None     | Identité, rattachement au parent et métadonnées du dossier racine |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Profondeur de scan (utilise catalog.depth si None) |
| include         | list[str] \| None                         | None     | Motifs glob à inclure                         |
| exclude         | list[str] \| None                         | None     | Motifs glob à exclure                         |
| recursive       | bool                                      | True     | Scanner les sous-répertoires                  |
| csv_encoding    | str \| None                               | None     | Surcharger l'encodage CSV                     |
| sample_size     | int \| None                               | None     | Lignes échantillonnées pour la détection de fréquences et d'énumérations automatiques (surcharge le catalogue) |
| auto_enumerations | bool \| None                            | None     | Créer des énumérations automatiques à partir des fréquences de valeurs pour ce dossier (surcharge le catalogue) |
| preview_rows    | int \| Literal[False] \| None              | None     | Nombre maximum de lignes d'aperçu pour les datasets trouvés dans ce dossier (surcharge le catalogue ; 0 ou false désactive) |
| csv_skip_copy       | bool \| None                              | None     | Sauter la copie temporaire UTF-8 (surcharge le catalogue) |
| storage_options | dict \| None                              | None     | Options pour le stockage distant (passées à fsspec) |
| refresh         | bool \| None                              | None     | Forcer le rescan (surcharge le réglage du catalogue) |
| quiet           | bool \| None                              | None     | Surcharger le réglage quiet du catalogue      |
| time_series     | bool                                      | True     | Regrouper les fichiers présentant des motifs temporels |
| create_folders  | bool                                      | True     | Si False, ne pas créer de dossiers depuis le disque ; s'appuyer sur `metadata_path` pour la structure (metadata-first) |
| on_unmatched    | "skip" \| "warn" \| "error"               | "warn"   | Politique quand un fichier scanné n'a pas de correspondance dans les métadonnées (uniquement quand `create_folders=False`) |

## `Catalog.add_dataset()`

```python
catalog.add_dataset(
    path,
    *,
    metadata=None,
    depth=None,
    csv_encoding=None,
    sample_size=None,
    auto_enumerations=None,
    preview_rows=None,
    csv_skip_copy=None,
    storage_options=None,
    refresh=None,
    quiet=None,
)
```

| Paramètre       | Type                                      | Défaut   | Description                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | requis   | Fichier(s) ou répertoire partitionné (local/distant) |
| metadata        | EntityMetadata \| None                    | None     | Identité du dataset, rattachement au parent et métadonnées |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Profondeur de scan (utilise catalog.depth si None) |
| csv_encoding    | str \| None                               | None     | Surcharger l'encodage CSV                     |
| sample_size     | int \| None                               | None     | Lignes échantillonnées pour la détection de fréquences et d'énumérations automatiques (surcharge le catalogue) |
| auto_enumerations | bool \| None                            | None     | Créer des énumérations automatiques à partir des fréquences de valeurs pour ce dataset (surcharge le catalogue) |
| preview_rows    | int \| Literal[False] \| None              | None     | Nombre maximum de lignes d'aperçu pour ce dataset (surcharge le catalogue ; 0 ou false désactive) |
| csv_skip_copy       | bool \| None                              | None     | Sauter la copie temporaire UTF-8 (surcharge le catalogue) |
| storage_options | dict \| None                              | None     | Options pour le stockage distant (passées à fsspec) |
| refresh         | bool \| None                              | None     | Forcer le rescan (surcharge le réglage du catalogue) |
| quiet           | bool \| None                              | None     | Surcharger le réglage quiet du catalogue      |

## `Catalog.add_database()`

```python
catalog.add_database(
    connection,
    metadata=None,
    *,
    depth=None,
    schema=None,
    include=None,
    exclude=None,
    sample_size=None,
    auto_enumerations=None,
    preview_rows=None,
    group_by_prefix=True,
    prefix_min_tables=2,
    time_series=True,
    storage_options=None,
    refresh=None,
    quiet=None,
    oracle_client_path=None,
    ssh_tunnel=None,
)
```

| Paramètre          | Type                                            | Défaut   | Description                                |
| ------------------ | ----------------------------------------------- | -------- | ------------------------------------------ |
| connection         | str \| ibis.BaseBackend                          | requis   | Chaîne de connexion ou objet backend ibis  |
| metadata           | EntityMetadata \| None                          | None     | Identité, rattachement au parent et métadonnées du dossier racine |
| depth              | \"dataset\" \| \"variable\" \| \"stat\" \| \"value\" \| None | None     | Profondeur de scan (utilise catalog.depth si None) |
| schema             | str \| list[str] \| None                         | None     | Schéma(s) à scanner                        |
| include            | list[str] \| None                               | None     | Motifs glob comparés aux noms de tables à inclure |
| exclude            | list[str] \| None                               | None     | Motifs glob comparés aux noms de tables à exclure |
| sample_size        | int \| None                                     | None     | Lignes échantillonnées pour la détection de fréquences et d'énumérations automatiques (surcharge le catalogue) |
| auto_enumerations  | bool \| None                                    | None     | Créer des énumérations automatiques à partir des fréquences de valeurs pour les tables scannées (surcharge le catalogue) |
| preview_rows       | int \| Literal[False] \| None                    | None     | Nombre maximum de lignes d'aperçu pour les datasets de tables scannées (surcharge le catalogue ; 0 ou false désactive) |
| group_by_prefix    | bool \| str                                     | True     | Regrouper les tables par préfixe en sous-dossiers |
| prefix_min_tables  | int                                             | 2        | Nombre minimum de tables pour former un groupe de préfixe |
| time_series        | bool                                            | True     | Détecter les motifs temporels de tables    |
| storage_options    | dict \| None                                    | None     | Options pour SQLite/GeoPackage distant     |
| refresh            | bool \| None                                    | None     | Forcer le rescan (surcharge le réglage du catalogue) |
| quiet              | bool \| None                                    | None     | Surcharger le réglage quiet du catalogue   |
| oracle_client_path | str \| None                                     | None     | Chemin des bibliothèques Oracle Instant Client |
| ssh_tunnel         | dict \| None                                    | None     | Configuration du tunnel SSH (host, user, port, etc.) |


## `Catalog.add_geodatabase()`

```python
catalog.add_geodatabase(
    path,
    metadata=None,
    *,
    depth=None,
    include=None,
    exclude=None,
    auto_enumerations=None,
    preview_rows=None,
    quiet=None,
    refresh=None,
    storage_options=None,
)
```

| Paramètre         | Type                                            | Défaut   | Description                                |
| ----------------- | ----------------------------------------------- | -------- | ------------------------------------------ |
| path              | str \| Path                                      | requis   | Chemin ou URL distante du répertoire `.gdb` |
| metadata          | EntityMetadata \| None                          | None     | Identité, rattachement au parent et métadonnées du dossier conteneur |
| depth             | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Profondeur de scan (utilise catalog.depth si None) |
| include           | list[str] \| None                               | None     | Motifs glob comparés aux noms de couches à inclure |
| exclude           | list[str] \| None                               | None     | Motifs glob comparés aux noms de couches à exclure |
| auto_enumerations | bool \| None                                    | None     | Créer des énumérations automatiques à partir des fréquences de valeurs pour les couches scannées (surcharge le catalogue) |
| preview_rows      | int \| Literal[False] \| None                    | None     | Nombre maximum de lignes d'aperçu pour les datasets de couches scannées (surcharge le catalogue ; 0 ou false désactive) |
| quiet             | bool \| None                                    | None     | Surcharger le réglage quiet du catalogue   |
| refresh           | bool \| None                                    | None     | Forcer le rescan (surcharge le réglage du catalogue) |
| storage_options   | dict \| None                                    | None     | Options pour le stockage distant (passées à fsspec) |

## `Catalog.export_db()`

```python
catalog.export_db(
    output_dir=None,
    *,
    track_evolution=True,
    copy_assets=None,
    base_dir=None,
    export_size_report=False,
    quiet=None,
)
```

| Paramètre       | Type             | Défaut  | Description                                |
| --------------- | ---------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Répertoire de sortie (utilise app_path si None) |
| track_evolution | bool             | True    | Suivre les changements entre exports       |
| copy_assets     | dict \| list[dict] \| None | None    | Copier des fichiers/répertoires locaux supplémentaires dans l'export selon les mêmes règles que `copy_assets()` |
| base_dir        | str \| Path \| None | None    | Répertoire de base pour les chemins relatifs `copy_assets.from` (défaut : répertoire de travail courant) |
| export_size_report | bool         | False   | Afficher un rapport de taille d'export par table, y compris les tailles `.json` gzippées estimées |
| quiet           | bool \| None     | None    | Surcharger le réglage quiet du catalogue   |

Exporte les fichiers de métadonnées JSON. Appelle `finalize()` automatiquement quand des données ont été scannées.

## `Catalog.export_app()`

```python
catalog.export_app(
    output_dir=None,
    *,
    open_browser=False,
    track_evolution=True,
    update_app=False,
    copy_assets=None,
    base_dir=None,
    export_size_report=False,
    quiet=None,
)
```

| Paramètre       | Type                | Défaut  | Description                                |
| --------------- | ------------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Répertoire de sortie (utilise app_path si None) |
| open_browser    | bool                | False   | Ouvrir l'app dans le navigateur après l'export |
| track_evolution | bool                | True    | Suivre les changements entre exports       |
| update_app      | bool                | False   | Rafraîchir les fichiers embarqués du front-end quand l'app existe déjà |
| copy_assets     | dict \| list[dict] \| None | None    | Copier des fichiers/répertoires locaux supplémentaires dans l'app exportée selon les mêmes règles que `copy_assets()` |
| base_dir        | str \| Path \| None | None    | Répertoire de base pour les chemins relatifs `copy_assets.from` (défaut : répertoire de travail courant) |
| export_size_report | bool            | False   | Afficher un rapport de taille d'export par table pour `data/db`, y compris les tailles `.json` gzippées estimées |
| quiet           | bool \| None        | None    | Surcharger le réglage quiet du catalogue   |

Exporte une app datannur autonome complète avec les données. Utilise `app_path` par défaut s'il est défini à l'initialisation. Les apps existantes mettent à jour `data/db` par défaut ; passez `update_app=True` pour rafraîchir les fichiers embarqués du front-end.

## `Catalog.finalize()`

```python
catalog.finalize()
```

Méthode avancée de cycle de vie. Supprime les entités qui n'ont plus été vues pendant le scan.

En usage normal, il n'est généralement pas nécessaire de l'appeler directement : `export_db()` et `export_app()` l'appellent automatiquement après le scan.


## `run_config()`

```python
from datannurpy import run_config

catalog = run_config(path)
```

| Paramètre | Type       | Défaut   | Description |
| --------- | ---------- | -------- | ----------- |
| path      | str \| Path | requis   | Fichier de configuration YAML à charger et exécuter |

Exécute un workflow `catalog.yml` et retourne le `Catalog` résultant.

Clés YAML de premier niveau reconnues par `run_config()` :

| Clé | Type | Description |
| --- | --- | --- |
| `add` | list[dict] | Étapes de scan à exécuter avec des entrées raccourcies telles que `folder`, `dataset` ou `database` |
| `env` | dict[str, str] | Variables d'environnement à injecter avant l'expansion des valeurs YAML |
| `env_file` | str \| Path \| list[str \| Path] | Un ou plusieurs fichiers dotenv à charger avant l'expansion |
| `output_dir` | str \| Path \| None | Exporter uniquement les métadonnées JSON dans ce répertoire au lieu d'exporter une app complète |
| `open_browser` | bool | Ouvrir l'app générée dans le navigateur après l'export d'app |
| `copy_assets` | dict \| list[dict] \| None | Copier des fichiers ou répertoires locaux supplémentaires dans l'export |
| `track_evolution` | bool | Activer ou désactiver la génération de `evolution.json` pendant l'export |
| `export_size_report` | bool | Activer le rapport de taille d'export par table après l'écriture de la base |
| `update_app` | bool | Rafraîchir les fichiers embarqués du front-end lors d'un export vers un `app_path` existant |
| `post_export` | str \| list[str] \| None | Exécuter des scripts Python après la fin de l'export |

Toutes les autres clés de premier niveau non réservées sont transmises à `Catalog(...)` avec les mêmes noms de paramètres, comme `app_path`, `metadata_path`, `depth`, `refresh`, `sample_size`, `auto_enumerations`, `preview_rows`, `app_config`, `quiet`, `verbose` et `log_file`.

Pour garder cette page d'API concise, le comportement YAML détaillé est documenté dans les guides thématiques :

- options d'export telles que `output_dir`, `open_browser`, `track_evolution`, `update_app`, `copy_assets` et `post_export` : voir [Sortie & exports](/fr/output)
- options de métadonnées et de configuration telles que `metadata_path`, `env`, `env_file` et `app_config` : voir [Métadonnées & configuration](/fr/metadata)

Les valeurs basées sur des chemins sont résolues relativement au répertoire du fichier YAML, sauf mention contraire dans ces guides.

## `copy_assets()`

```python
from datannurpy import copy_assets

copy_assets(output_dir, rules, *, base_dir=None, quiet=False)
```

| Paramètre  | Type                          | Défaut  | Description |
| ---------- | ----------------------------- | ------- | ----------- |
| output_dir | str \| Path                    | requis  | Répertoire d'export à alimenter |
| rules      | dict \| list[dict]             | requis  | Règles de copie de la même forme que le `copy_assets` YAML |
| base_dir   | str \| Path \| None           | None    | Répertoire de base pour les chemins `from` relatifs (défaut : répertoire de travail courant) |
| quiet      | bool                           | False   | Supprimer la journalisation de progression des copies |

Chaque règle accepte `from`, `to`, `include` optionnel et `clean` optionnel.

`Catalog.export_db()` et `Catalog.export_app()` acceptent aussi `copy_assets=` et `base_dir=` comme enrobages pratiques autour de cet utilitaire.

## `EntityMetadata`

```python
EntityMetadata(
    id=None,
    parent_id=None,
    manager_organization_id=None,
    owner_organization_id=None,
    tag_ids=None,
    doc_ids=None,
    name=None,
    description=None,
    license=None,
    type=None,
    link=None,
    localisation=None,
    start_date=None,
    end_date=None,
    updating_each=None,
    no_more_update=None,
)
```

| Paramètre      | Type              | Description |
| -------------- | ----------------- | ----------- |
| id             | str \| None       | ID d'entité explicite. Si omis, les valeurs par défaut dérivées du scan sont utilisées. |
| parent_id      | str \| None       | ID du dossier parent (`Folder.parent_id` pour les dossiers, `Dataset.folder_id` pour les datasets). |
| manager_organization_id | str \| None       | ID de l'organisation gestionnaire. |
| owner_organization_id   | str \| None       | ID de l'organisation propriétaire. |
| tag_ids        | list[str] \| None | IDs des tags associés. |
| doc_ids        | list[str] \| None | IDs des documents associés. |
| name           | str \| None       | Nom d'affichage. |
| description    | str \| None       | Texte de description. |
| license        | str \| None       | Chaîne de licence. |
| type           | str \| None       | Type/catégorie de l'entité. |
| link           | str \| None       | URL de référence externe. |
| localisation   | str \| None       | Couverture géographique. |
| start_date     | str \| None       | Début de la période couverte. |
| end_date       | str \| None       | Fin de la période couverte. |
| updating_each  | str \| None       | Fréquence de mise à jour. |
| no_more_update | str \| None       | Marqueur indiquant qu'aucune mise à jour n'est plus attendue. |

Dans les configurations YAML, les mêmes métadonnées s'écrivent généralement comme clés de premier niveau sur une entrée `add` :

```yaml
add:
  - folder: ./data
    id: source
    name: Source data
    description: Curated files used by the analytics team.

  - dataset: ./data/sales.csv
    id: source---sales
    folder_id: source
    name: Sales
    description: Monthly sales by product and region.
```

`EntityMetadata` est l'équivalent en API Python de ces clés de métadonnées YAML.

## Utilitaires d'ID

```python
from datannurpy import sanitize_id, build_dataset_id, build_variable_id
```

| Fonction                                        | Description                | Exemple                                                     |
| ----------------------------------------------- | -------------------------- | ----------------------------------------------------------- |
| sanitize_id(s)                                  | Nettoyer une chaîne pour l'utiliser comme ID | "My File (v2)" → "My File _v2_"                          |
| build_dataset_id(folder_id, dataset_name)       | Construire un ID de dataset | (\"src\", \"sales\") → \"src---sales\"                      |
| build_variable_id(folder_id, dataset_name, var) | Construire un ID de variable | (\"src\", \"sales\", \"amount\") → \"src---sales---amount\" |
