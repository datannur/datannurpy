# Time Series Dataset Feature

## Contexte

Pattern courant : un dossier contient plusieurs fichiers avec un suffixe temporel (année, trimestre, mois). Le schéma est quasi identique d'une période à l'autre.

```
data/
  enquete_2020.csv
  enquete_2021.csv
  enquete_2023.csv
  enquete_2024.csv
```

## Objectifs

1. **Un seul dataset** dans le catalogue (éviter la duplication)
2. **Variables = union de tous les fichiers** (avec `start_date`/`end_date` pour chaque)
3. **Stats et types = version la plus récente**
4. **Performance** : scan complet uniquement sur le dernier fichier

## API

```python
catalog.add_folder(folder, time_series=False)  # Désactive la détection
```

Par défaut `time_series=True`.

## Détection automatique

### Principe

On **normalise** le chemin complet en remplaçant les patterns temporels par `{{PERIOD}}`. Les fichiers avec le même chemin normalisé forment une série.

| Chemin original           | Normalisé                     | Période   |
| ------------------------- | ----------------------------- | --------- |
| `data/enquete_2020.csv`   | `data/enquete_{{PERIOD}}.csv` | `2020`    |
| `data/2020/enquete.csv`   | `data/{{PERIOD}}/enquete.csv` | `2020`    |
| `data/2024/Q1/export.csv` | `data/{{PERIOD}}/export.csv`  | `2024-Q1` |

### Patterns reconnus

Appliqués **par longueur décroissante** (le plus long d'abord pour éviter les faux positifs) :

| Pattern            | Regex                             | Longueur | Exemple             |
| ------------------ | --------------------------------- | -------- | ------------------- |
| Date complète      | `(19\|20)\d{2}[_-]\d{2}[_-]\d{2}` | 10       | `2024-03-15`        |
| Année-mois compact | `(19\|20)\d{2}(0[1-9]\|1[0-2])`   | 6        | `202403`            |
| Année-mois         | `(19\|20)\d{2}[_-]\d{2}`          | 7        | `2024-03`           |
| Trimestre          | `(19\|20)\d{2}[_-]?[QqTt][1-4]`   | 5-6      | `2024Q1`, `2024-T2` |
| Année seule        | `(19\|20)\d{2}`                   | 4        | `2024`              |

- Années : 1900-2099
- Trimestre : `Q` ou `T`, majuscule ou minuscule
- Plusieurs patterns dans un chemin → combinés par hiérarchie (année > trimestre/mois > jour)

### Combinaison des patterns

Si plusieurs patterns temporels sont détectés dans un chemin, ils sont combinés par ordre hiérarchique :

| Chemin                  | Patterns détectés  | Période combinée |
| ----------------------- | ------------------ | ---------------- |
| `2024/Q1/export.csv`    | `2024`, `Q1`       | `2024-Q1`        |
| `2024/03/export.csv`    | `2024`, `03`       | `2024-03`        |
| `2024/03/15/export.csv` | `2024`, `03`, `15` | `2024-03-15`     |
| `data_2024_Q1.csv`      | `2024`, `Q1`       | `2024-Q1`        |

Règle : on prend l'année la plus proche de la racine, puis le trimestre/mois, puis le jour.

### Tri des périodes

Les périodes sont triées chronologiquement pour déterminer "premier" et "dernier" fichier :

```python
def period_sort_key(period: str) -> tuple[int, int, int]:
    """Convertit une période en tuple triable (année, sous-période, jour)."""
    # "2024" → (2024, 0, 0)
    # "2024-Q1" → (2024, 1, 0)
    # "2024-03" → (2024, 3, 0)
    # "202403" → (2024, 3, 0)
    # "2024-03-15" → (2024, 3, 15)
```

### Groupement

1. Normaliser chaque fichier (chemin complet **incluant l'extension**)
2. Grouper par chemin normalisé identique
3. Si ≥2 fichiers → série temporelle

> **Note** : L'extension fait partie du chemin normalisé. `enquete_2020.csv` et `enquete_2021.xlsx` ne forment **pas** une série car leurs chemins normalisés diffèrent (`enquete_{{PERIOD}}.csv` vs `enquete_{{PERIOD}}.xlsx`).

## Comportement

### Scan

- **Scan léger** (tous les fichiers) : équivalent à `depth="schema"` — noms de colonnes uniquement
- **Scan complet** (dernier fichier) : types, stats, modalités

### Variables : start_date / end_date

| Situation                | start_date           | end_date         |
| ------------------------ | -------------------- | ---------------- |
| Présente depuis le début | `null`               | -                |
| Ajoutée en cours         | période d'apparition | -                |
| Présente jusqu'à la fin  | -                    | `null`           |
| Supprimée                | -                    | dernière période |

Exemple :

```
enquete_2020.csv : id, nom, revenu
enquete_2021.csv : id, nom, revenu, email
enquete_2023.csv : id, nom, email        # revenu supprimé
```

→ `revenu` : `end_date=2021`, `email` : `start_date=2021`

### Dataset

- `data_path` = chemin du dernier fichier
- `start_date` / `end_date` = première / dernière période
- `nb_row` = lignes du dernier fichier
- `nb_files` = nombre de fichiers (>1 = série temporelle)

### ID du dataset

L'ID utilise le chemin normalisé avec `---PERIOD---` comme placeholder (valide pour un ID) :

- `enquete_2024.csv` → `id = "source---enquete_---PERIOD---_csv"`
- `2024/enquete.csv` → `id = "source------PERIOD------enquete_csv"`

### Nom du dataset

Le champ `name` du Dataset contient le pattern lisible :

Date dans le **nom du fichier** :

- `enquete_2024.csv` → `name = "enquete_[YYYY]"`
- `rapport_2024Q1.csv` → `name = "rapport_[YYYY]Q[N]"`

Date uniquement dans le **dossier** :

- `2024/enquete.csv` → `name = "enquete"`

### Rescan

`mtime` de la série = `max()` de tous les fichiers. Si un fichier change ou est ajouté → rescan de toute la série.

## Limitations

- Stats descriptives entre périodes non trackées (bruit)
- Variables supprimées : seuls `name`, `dataset_id`, `start_date` et `end_date` sont renseignés (pas de `type`, `nb_distinct`, etc.)
- Pas de détection des changements de type entre périodes (type = dernier fichier uniquement)

## Architecture technique

### DatasetInfo (scanner)

```python
@dataclass
class DatasetInfo:
    path: Path
    format: str
    mtime: int
    series_files: list[tuple[str, Path]] | None = None  # [(period, path), ...]
```

### Dataset (schema)

Nouveau champ :

```python
nb_files: int | None = None  # Nombre de fichiers dans la série (>1 = série temporelle)
```

### Flow

```
discover_datasets()
├── discover_parquet_datasets()  → Delta/Iceberg/Hive
├── find other files             → CSV, Excel, etc.
└── group_time_series()          → fusionne en séries
```

### Scan de la série

```python
if info.series_files:
    # Trier par période (chronologique)
    sorted_files = sorted(info.series_files, key=lambda x: period_sort_key(x[0]))

    # Scan léger → colonnes par période
    columns_by_period = {period: get_columns(path) for period, path in sorted_files}

    # Calculer start_date/end_date pour chaque variable
    variable_periods = compute_variable_periods(columns_by_period)

    # Scan complet du dernier fichier uniquement
    last_period, last_path = sorted_files[-1]
    variables = scan_file(last_path, infer_stats=True)

    # Appliquer les périodes aux variables scannées
    scanned_names = {var.name for var in variables}
    for var in variables:
        var.start_date, var.end_date = variable_periods[var.name]

    # Créer les variables "fantômes" (supprimées avant la dernière période)
    all_columns = set(variable_periods.keys())
    for col_name in all_columns - scanned_names:
        start, end = variable_periods[col_name]
        variables.append(Variable(
            id=build_variable_id(dataset_id, col_name),
            name=col_name,
            dataset_id=dataset_id,
            start_date=start,
            end_date=end,
            # type, nb_distinct, etc. = None (pas d'info disponible)
        ))
```

## TODO

- [ ] `series_files` dans `DatasetInfo`
- [ ] `nb_files` dans `Dataset`
- [ ] `normalize_path()` → remplace patterns par `{{PERIOD}}`
- [ ] `extract_period()` → extrait et combine les périodes d'un chemin
- [ ] `period_sort_key()` → tri chronologique des périodes
- [ ] `group_time_series()` → groupe les fichiers par chemin normalisé
- [ ] Intégrer dans `discover_datasets()`
- [ ] `time_series: bool = True` dans `add_folder()`
- [ ] `get_columns()` (scan léger par format : CSV header, Parquet schema, etc.)
- [ ] `compute_variable_periods()` → calcule start_date/end_date par variable
- [ ] Adapter le scan pour `series_files` (variables fantômes)
- [ ] Tests
