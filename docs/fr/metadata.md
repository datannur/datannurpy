# Métadonnées & configuration

## Métadonnées manuelles


```yaml
# Charger depuis un dossier contenant des fichiers de métadonnées
metadata_path: ./metadata

# Ou depuis une base de données
metadata_path: sqlite:///metadata.db

# Ou une liste de sources appliquées dans l'ordre (les suivantes surchargent/complètent les précédentes)
metadata_path:
  - ./metadata-base        # métadonnées partagées ou générées
  - ./metadata-overlay     # surcharges locales (par exemple ajout de tags de politique)
```

Utilisable seul ou combiné avec des sources auto-scannées (`add_folder`, `add_database`). Les métadonnées sont appliquées automatiquement avant l'export.

**Structure attendue :** un fichier/une table par entité, nommé d'après le type d'entité :

```
metadata/
├── variable.csv      # Variables (descriptions, tags...)
├── dataset.xlsx      # Datasets
├── organization.json # Organisations (propriétaires, gestionnaires)
├── tag.csv           # Tags
├── concept.csv       # Concepts du glossaire métier
├── enumeration.csv   # Énumérations
├── value.csv         # Valeurs d'énumération
├── config.csv        # Config de l'app web (voir app_config)
└── ...
```

**Formats pris en charge :** CSV, Excel (.xlsx), JSON, SAS (.sas7bdat) ou tables de base de données.

**Format de fichier :** structure tabulaire standard suivant les [schémas datannur](https://github.com/datannur/datannur/tree/main/public/schemas).

```csv
# variable.csv
id,description,tag_ids
source---employees_csv---salary,"Monthly gross salary in euros","finance,hr"
source---employees_csv---department,"Department code","hr"
```

Les colonnes de métadonnées les plus courantes sont les mêmes que la source soit CSV, Excel, JSON, SAS ou une table de base de données :

| Entité/table | Colonnes utiles |
| ------------ | -------------- |
| `folder` | `id`, `parent_id`, `name`, `description`, `manager_organization_id`, `owner_organization_id`, `tag_ids`, `doc_ids`, `license`, `link`, `localisation` |
| `dataset` | `id`, `folder_id`, `name`, `description`, `manager_organization_id`, `owner_organization_id`, `tag_ids`, `doc_ids`, `license`, `data_path`, `link`, `localisation`, `start_date`, `end_date`, `updating_each` |
| `variable` | `id`, `name`, `dataset_id`, `description`, `tag_ids`, `enumeration_ids`, `concept_id`, `type`, `start_date`, `end_date` |
| `organization` | `id`, `parent_id`, `name`, `description`, `email`, `phone`, `tag_ids`, `doc_ids` |
| `tag` | `id`, `parent_id`, `name`, `description`, `doc_ids` |
| `doc` | `id`, `name`, `description`, `path`, `type`, `last_update` |
| `concept` | `id`, `parent_id`, `name`, `description`, `tag_ids`, `doc_ids` |
| `enumeration` | `id`, `folder_id`, `name`, `description`, `type` |
| `value` | `enumeration_id`, `value`, `description` |
| `config` | `id`, `value` |

Les champs de texte localisés peuvent être fournis avec la convention `field:lang` de l'app. Par exemple, conservez `name` et `description` comme langue par défaut/de repli, puis ajoutez `name:fr` ou `description:fr` pour le texte français, ou `name:de` ou `description:de` pour l'allemand. Datannurpy préserve les colonnes localisées pour tout champ d'entité qui existe déjà dans le schéma, comme `name:fr`, `name:de` et `description:fr` sur les datasets, variables, tags, organisations, concepts, énumérations, valeurs, docs et libellés de filtres de l'app. Une cellule localisée vide ne contribue rien depuis cette source (la valeur se replie sur une source de niveau inférieur ou sur le scan) ; utilisez `!` pour effacer explicitement une valeur localisée.

Pour les sources de métadonnées basées sur des dossiers, nommez chaque fichier d'après l'entité (`folder.csv`, `folder.xlsx`, `folder.json`, etc.). Les champs de type liste tels que `tag_ids`, `doc_ids` et `enumeration_ids` peuvent s'écrire comme valeurs séparées par des virgules dans les fichiers tabulaires, ou comme tableaux en JSON. Pour le détail complet des schémas, utilisez les schémas datannur liés ci-dessus comme référence.

**Comportement de fusion :**

- Les entités existantes sont mises à jour (les valeurs manuelles surchargent les valeurs auto-scannées)
- Les nouvelles entités sont créées
- Les champs de type liste (`tag_ids`, `doc_ids`, etc.) sont fusionnés
- Une cellule vide, un tableau JSON vide ou une colonne absente signifie « pas de valeur courante depuis cette source de métadonnées » — elle ne contribue rien, la valeur se replie donc sur ce qu'une source de métadonnées de niveau inférieur ou le scan fournit. Cela ne fige **pas** la valeur de l'export précédent. Videz une cellule pour abandonner votre surcharge et revenir à la valeur scannée ; utilisez `!` pour forcer la valeur finale à vide même quand le scan en fournit une

**Ordre d'application :** les métadonnées sont appliquées automatiquement avant l'export/la finalisation, après tous les appels `add_folder`, `add_dataset` et `add_database`, de sorte que les valeurs manuelles priment. Si `app_path/data/db-ui` existe, il est chargé automatiquement comme dernière source de métadonnées, après les sources `metadata_path` configurées. Cela permet aux éditions locales dans l'app de surcharger les métadonnées scannées sans ajouter `data/db-ui` à la configuration. `data/db-ui` utilise les mêmes formats de métadonnées pris en charge que toute autre source de métadonnées ; `data/db` reste l'export généré.

### Instructions d'overlay

Les sources de métadonnées peuvent inclure de petites instructions pour effacer des champs, retirer des relations individuelles ou supprimer des entités avant l'export. Ces instructions sont consommées par le builder et ne sont pas écrites dans `data/db`.

Utilisez `!` comme valeur exacte d'un champ scalaire pour l'effacer :

```csv
id,description
source---employees_csv,!
```

Utilisez `!` comme valeur exacte d'un champ de relation pour effacer toutes les relations accumulées de ce champ :

```csv
id,tag_ids
source---employees_csv,!
```

Utilisez `!id` à l'intérieur d'une liste de relations pour retirer une relation accumulée tout en conservant ou en ajoutant les autres :

```json
[
  {
    "id": "source---employees_csv---salary",
    "tag_ids": ["finance", "!auto---numeric"]
  }
]
```

Les retraits de relations sont appliqués dans l'ordre des sources de métadonnées. Si une même ligne contient à la fois `id` et `!id`, le retrait l'emporte pour cette relation. Les champs de relation pris en charge sont `tag_ids`, `doc_ids`, `enumeration_ids` et `source_variable_ids`.

### Suppression {#deletion}

L'export [reflète les sources courantes](/fr/output#incremental-scan), mais la façon dont une entité disparaît dépend de sa nature :

- **Les entités parentes** (`folder`, `dataset`, `enumeration`, `organization`, `tag`, `doc`, `concept`) sont réconciliées automatiquement : une entité qui n'est plus produite par le scan et absente des métadonnées est supprimée à l'exécution suivante.
- **Les entités enfants** (`variable`, `value`, `frequency`) ne sont pas suivies individuellement. Retirer leur ligne d'un fichier de métadonnées ne les supprime **pas** — elles ne disparaissent que lorsque leur parent est rescanné ou supprimé (cascade), ou, pour les variables, via un `_delete` explicite. Comme filet de sécurité, une variable dont le dataset n'existe plus est retirée à l'export pour que le catalogue reste référentiellement cohérent.

Utilisez `_delete: true` pour retirer immédiatement une entité du catalogue final :

```json
[
  {
    "id": "source---old_dataset",
    "_delete": true
  }
]
```

`_delete` est pris en charge pour toutes les entités du catalogue identifiées par ID (y compris `variable`). Les tables composites (`value` et `frequency`) ne peuvent pas être supprimées directement ; elles sont retirées par cascade depuis leur énumération, variable, dataset ou dossier parent.

Les cascades sont appliquées avant l'export :

- supprimer un dossier retire ses dossiers descendants, les datasets qu'il contient, les variables de ces datasets, les fréquences, les aperçus et les énumérations qu'il contient ;
- supprimer un dataset retire ses variables, fréquences et aperçus ;
- supprimer une variable retire ses fréquences ;
- supprimer des énumérations, tags, docs, concepts ou organisations nettoie aussi les références associées quand nécessaire.

## Pattern metadata-first

Quand la structure du catalogue (dossiers, IDs de datasets, hiérarchie) est entièrement définie par `metadata_path` et que le système de fichiers ne fournit que des fichiers à scanner pour les informations techniques (variables, statistiques, tailles, formats), utilisez `create_folders=False` :

```yaml
metadata_path: ./metadata
add:
  - folder: ./data/parquet
    create_folders: false
```

Dans ce mode :

- Aucun dossier n'est créé depuis le répertoire scanné ; la hiérarchie provient de `metadata/folder.csv`.
- Chaque fichier scanné est associé à son entrée de métadonnées via `_match_path` quand il est présent, ou via `data_path` en repli. Le scan réutilise les `id` et `folder_id` définis dans les métadonnées.
- Les fichiers sans correspondance dans les métadonnées sont signalés selon `on_unmatched` : `"warn"` (défaut), `"skip"` ou `"error"`.

**Correspondance :** `_match_path` est une clé technique optionnelle utilisée uniquement pour rattacher les fichiers scannés aux lignes de métadonnées. Utilisez-la quand la clé de scan diffère du `data_path` public, par exemple pour des sources distantes ou des datasets logiques de séries temporelles. Si `_match_path` est omis, `data_path` sert de clé de correspondance.

Pour les chemins distants, les identifiants de connexion ne font pas partie de l'identité de correspondance : `sftp://user@example.org/path/file.csv` et `sftp://example.org/path/file.csv` correspondent à la même clé de scan. L'identité distante canonique conserve le protocole, l'hôte, le port optionnel et le chemin.

Pour les datasets de séries temporelles, `_match_path` peut utiliser la même syntaxe de période normalisée produite par datannurpy : `[YYYY]`, `[YYYY/MM]`, `[YYYY]Q[N]` ou `[YYYY/MM/DD]`. Cela correspond à la série logique, et pas seulement au dernier fichier physique.

**Exemple** — `./metadata/dataset.csv` :

```csv
id,name,folder_id,data_path
sales-2024,Sales 2024,finance,../data/parquet/sales-2024.parquet
hr-headcount,HR headcount,hr,../data/parquet/hr-headcount.parquet
```

Combiné avec `add_folder('./data/parquet', create_folders=False)`, le scan rattache variables et statistiques à `sales-2024` et `hr-headcount` (les IDs déclarés dans les métadonnées) sans créer aucun dossier à partir de l'arborescence disque.

**Exemple distant et séries temporelles** — `./metadata/dataset.csv` :

```csv
id,name,folder_id,data_path,_match_path
sales,Sales,finance,sftp://example.org/shared/data/sales_2024.csv,sftp://example.org/shared/data/sales_[YYYY].csv
traffic,Traffic,transport,sftp://example.org/shared/data/traffic_2024-03.csv,sftp://example.org/shared/data/traffic_[YYYY/MM].csv
```

Ici `data_path` reste le chemin public affiché dans le catalogue, tandis que `_match_path` sert uniquement à rattacher le résultat du scan à la ligne de métadonnées. Si le scan SFTP est configuré comme `sftp://user@example.org/shared/data`, la partie utilisateur est ignorée pour la correspondance.

**Contraintes :**

- `create_folders=False` est incompatible avec `metadata=` dans `add_folder()` (aucun dossier n'est créé).
- Le `data_path` que vous écrivez dans `metadata/dataset.csv` est exporté tel quel dans la sortie. Utilisez `_match_path` quand la clé technique de scan doit différer du chemin public affiché dans le front-end.

## Variables d'environnement


Les variables d'environnement (`$VAR` ou `${VAR}`) sont développées dans toutes les valeurs YAML. Toutes les sources sont chargées — `env:`, `env_file` et le `.env` à côté du fichier YAML :

```yaml
env:
  data_dir: /shared/data
  db_host: db.example.com
env_file: /secure/path/.env    # secrets : DB_USER, DB_PASSWORD

add:
  - folder: ${data_dir}/sales
  - folder: ${data_dir}/hr
  - database: oracle://${DB_USER}:${DB_PASSWORD}@${db_host}:1521/ORCL
```

`env_file` accepte une liste de chemins (le dernier surcharge le premier) :

```yaml
env_file:
  - /shared/common.env         # valeurs par défaut
  - /secure/credentials.env    # surcharge common.env
```

Priorité (la première définie gagne) : variables d'environnement système > `env:` YAML > `env_file` > `.env` local.

## app_config


Configurez l'app web avec des entrées clé-valeur (écrites dans `config.json`) :

```yaml
app_path: ./my-catalog
app_config:
  contact_email: contact@example.com
  more_info: "Data from [open data portal](https://example.com)."
```

Si `app_config` n'est pas fourni, le fichier `config.csv`/`config.xlsx`/`config.json` (colonnes `id`, `value`) de `metadata_path` est utilisé à la place. Si aucun des deux n'est fourni, aucun `config.json` n'est généré.
