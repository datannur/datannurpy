# Metadata-first pattern

Design note for supporting catalogs where **metadata is the source of truth** and the filesystem scan only enriches existing entities with technical info.

## Pattern actuel : filesystem-first

Aujourd'hui :

1. `add_folder('./data')` parcourt le disque, crée folders + datasets à partir de l'arborescence. IDs dérivés du chemin.
2. `metadata/*.csv` enrichit ensuite (override scalaires, merge listes).
3. La metadata « gagne » sur l'éditorial parce qu'elle s'applique en dernier (`ensure_metadata_applied` est appelé par `export_*`).

Convient quand l'arborescence disque définit la structure du catalogue.

## Pattern visé : metadata-first

`metadata/folder.csv` et `metadata/dataset.csv` définissent **toute** la structure (IDs, hiérarchie, noms). Les fichiers physiques ne sont là que pour :

- variables découvertes par scan (schema),
- stats descriptives, modalités, fréquences,
- `data_size`, `last_update_timestamp`, `delivery_format`.

Cas d'usage : exports CKAN/DCAT, inventaires SQL, YAML maintenu à la main, etc. — bref, dès qu'une source externe est autoritative.

## Problèmes bloquants aujourd'hui

Soit `metadata/dataset.csv` contenant un dataset `id=abc-123, data_path=data/parquet/abc-123.parquet`, et un yaml :

```yaml
metadata_path: ./metadata
add:
  - folder: ./data/parquet
```

Trois bugs s'enchaînent :

1. **Folder fantôme** : `add_folder('./data/parquet')` crée toujours un folder racine `parquet` (et des sous-folders). Or il ne devrait rien créer : la hiérarchie vient de `folder.csv`.
2. **Pas de pré-existence visible** : au moment du scan, `catalog.dataset` est vide (la metadata n'est *appliquée* qu'à l'export). Le scan ne peut pas savoir que `abc-123` existe déjà.
3. **Matching impossible** : même si la metadata était appliquée, le scan cherche `get_by("data_path", "/Users/.../data/parquet/abc-123.parquet")` (absolu) alors que le CSV stocke `data/parquet/abc-123.parquet` (relatif). Pas de match → dataset dupliqué avec ID `parquet---abc-123_parquet`.

## Note préalable : `data_path` cumule deux rôles

Avant de proposer la solution, expliciter une ambiguïté existante de `data_path` :

1. **Clé de matching** scan ↔ catalogue : doit être absolue et comparable (utilisée par `get_by("data_path", …)`).
2. **Lien public** dans le front exportable : doit être lisible par l'utilisateur final — URL publique (`https://opendata.swiss/…`), chemin relatif au déploiement, ou rien.

Aujourd'hui `add_folder` stocke `/Users/bassim/proj/…` qui part tel quel dans le JSON exporté. Bug latent (pas introduit par cette feature, mais qu'il faut ne pas amplifier).

La solution sépare les deux concerns via un **runtime field** `_match_path` (préfixe `_` = non exporté, comme `_seen`). Mécanisme déjà en place dans le schema, voir [catalog.py L116-126](src/datannurpy/catalog.py#L116-L126).

- **`data_path`** : champ public, stocké tel que fourni (string brute du CSV en mode metadata, absolu de scan en mode filesystem-first — comme aujourd'hui). Exporté dans le JSON. Peut être URL, relatif, absolu — responsabilité du user.
- **`_match_path`** : runtime field, toujours absolu, jamais exporté. Calculé :
  - au scan : `str(info.path)` (déjà absolu) ;
  - au load metadata : `resolve(metadata_dir / data_path)` si relatif et que le fichier existe ; sinon `None` (cas URL → pas de match possible, OK).

Le matching `get_by` et le peek travaillent sur `_match_path`, jamais sur `data_path`.

## Solution : 3 changements non-breaking

### C. Calcul de `_match_path` (runtime, non exporté)

Ajouter `_match_path: str | None = None` aux runtime fields de `Dataset` (analogue à `_seen`).

Deux points de calcul :

1. **Scan** (`add_folder`, `_scan_time_series`, etc.) : à la création/update du dataset, `_match_path = str(info.path)` (déjà absolu via `Path(path).resolve()`). En pratique : remplacer aussi `data_path=str(info.path)` par `data_path=str(info.path)` pour rester non-breaking *aujourd'hui*, plus `_match_path=str(info.path)` ; futur travail possible pour rendre `data_path` configurable (URL publique, relatif au déploiement).
2. **Load metadata** (`load_metadata` dans `add_metadata.py`) : pour chaque ligne de `dataset.csv`, si `data_path` est non-vide, tenter `resolve(metadata_dir / data_path)`. Si le fichier existe, `_match_path` = absolu résolu. Sinon (URL, fichier absent) `_match_path = None`.

**Base de résolution : toujours le répertoire du fichier metadata lui-même.** Choix délibéré :

- cohérent avec la convention UNIX (paths relatifs au document qui les contient) ;
- comportement identique en API Python et via `run_config` (pas de magie spécifique au yaml) ;
- localisé : on peut déplacer le couple `metadata/*.csv` + ses chemins relatifs sans casser quoi que ce soit.

Un user dont le CSV est dans `./metadata/` et les fichiers dans `./data/parquet/` doit donc écrire `../data/parquet/<id>.parquet` dans la colonne `data_path`. Explicite, pas devinable.

**Migration des call sites existants** : les trois `get_by("data_path", …)` dans `add_folder.py` deviennent `get_by("_match_path", …)`. Comportement identique en mode filesystem-first (les deux valent l'absolu du scan).

**Non-breaking** : `data_path` exporté reste exactement ce qu'il était. Aucun consommateur du JSON n'est impacté.

### B'. Peek dans `_loaded_metadata` depuis `add_folder`

**Préserver l'invariant actuel** : la metadata s'applique *après* le scan (sinon le scan écraserait l'éditorial). Donc on n'inverse pas l'ordre.

À la place, `add_folder` *peek* en lecture dans `catalog._loaded_metadata` — pattern déjà utilisé pour `_freq_hidden_ids` ([utils/modality.py L116](src/datannurpy/utils/modality.py#L116)).

Helper à ajouter dans `add_metadata.py`, retournant un type minimal et stable (pas de leak pandas) :

```python
class LoadedDatasetRef(NamedTuple):
    id: str
    folder_id: str | None

def find_loaded_dataset_by_match_path(
    catalog: Catalog, abs_match_path: str
) -> LoadedDatasetRef | None:
    """Return id/folder_id of pre-loaded dataset whose _match_path == abs_match_path."""
```

**Perf** : index lazy `dict[str, LoadedDatasetRef]` construit au premier appel (ou en fin de `load_metadata`), pour O(1) par lookup. Sans index : 841 fichiers × 6664 datasets = 5,6M comparaisons inutiles. Avec index : 841 lookups dict.

Dans `add_folder`, pour chaque fichier scanné (s'applique aux **trois** chemins de création : mode `depth=dataset` structure-only, mode scan complet, et `_scan_time_series`) :

```python
match_path = str(info.path)  # absolu
peek = find_loaded_dataset_by_match_path(catalog, match_path)
if peek is not None:
    dataset_id = peek.id
    folder_id = peek.folder_id
else:
    dataset_id, _ = build_dataset_id_name(info.path, root, prefix)
    folder_id = get_folder_id(info.path, root, prefix, subdir_ids)
```

Le scan crée alors le dataset avec **l'ID que la metadata attend**. Quand `ensure_metadata_applied` tournera plus tard, le merge tombe sur `existing` (via `id`, pas via `data_path` ou `_match_path`) au lieu de créer un doublon. Variables : même logique implicite, puisque `build_variable_id(dataset_id, var_name)` utilise désormais le dataset_id metadata.

### A. `create_folders: bool = True` (+ `on_unmatched: "skip" | "warn" | "error" = "warn"`)

Params sur `add_folder`. Quand `create_folders=False` :

- pas de création du folder racine ni des sous-folders depuis le disque ;
- pour chaque fichier scanné : si peek trouve un dataset → enrichir ; sinon, comportement piloté par `on_unmatched` :
  - `"skip"` : silencieux, ignorer le fichier ;
  - `"warn"` (défaut) : `log_warn` + skip ;
  - `"error"` : `raise ConfigError`.

Le défaut `create_folders=True` préserve le comportement actuel ; `on_unmatched` n'a alors pas d'effet (le scan crée le dataset comme avant).

## Politique de merge (rappel, non modifiée)

- **Metadata gagne** sur les champs éditoriaux : `name`, `description`, `tag_ids`, `owner_id`, `manager_id`, dates curées, etc.
- **Scan gagne** sur les champs techniques : `data_size`, `last_update_timestamp`, `delivery_format`, `nb_row`, variables/stats/modalités.

C'est le comportement *de facto* aujourd'hui (le merge override tout scalaire non-vide, mais les CSV metadata ne contiennent pas les champs techniques). Convention implicite : ne pas mettre de champs techniques dans `dataset.csv`. Pas de doc utilisateur nécessaire.

## YAML cible après les 3 changements

```yaml
app_path: ./catalog
metadata_path: ./metadata
depth: variable
add:
  - folder: ./data/parquet
    create_folders: false
```

Comportement :

1. `Catalog(...)` → `load_metadata` lit les CSV en mémoire (`_loaded_metadata`), calcule `_match_path` absolu pour chaque dataset (C). `data_path` reste tel quel dans le CSV.
2. `add_folder('./data/parquet', create_folders=False)` parcourt 841 parquets ; pour chacun, peek (B') sur `_match_path` trouve l'entrée metadata → utilise son `id`/`folder_id` ; scan attache variables/stats. Aucun folder créé (A).
3. `export_app` → `ensure_metadata_applied` merge l'éditorial sur les datasets (déjà aux bons IDs). 6664 datasets, 2574 folders, 0 doublon. `data_path` exporté = ce que le user a écrit dans son CSV (URL publique ou autre).

## Ordre d'implémentation

1. **C** — ajout du runtime field `_match_path` + calcul au scan et au load metadata. Précondition du matching.
2. **B'** — helper `find_loaded_dataset_by_match_path` + intégration dans `add_folder` (modes `dataset` et `variable`/`stat`/`value`, et `_scan_time_series`).
3. **A** — params `create_folders` + `on_unmatched` sur `add_folder`.

Tests à ajouter à chaque étape — notamment un e2e metadata-first vérifiant 0 folder/dataset créé hors metadata.
