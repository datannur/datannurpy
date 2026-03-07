# Specs: Refactoring scan progressif

## Objectif

Refactorer `add_folder()` en 2 étapes avec 3 niveaux de profondeur.

## Flow

```
add_folder(path, depth="schema")
    │
    ├─► Étape 1: STRUCTURE (toujours)
    │   ├── Parcours filesystem → list[DatasetInfo]
    │   ├── Diff avec catalog → ScanPlan (to_scan, to_skip)
    │   └── Upsert folders + datasets metadata
    │
    │   if depth == "structure": return
    │
    └─► Étape 2: SCAN (branches alternatives)
        ├── depth="schema" → scan léger (headers)
        └── depth="full"   → scan complet (données)
```

## Structures de données

```python
@dataclass
class DatasetInfo:
    path: str
    parent: str
    name: str
    format: str    # csv, parquet, delta, hive, iceberg, sas, spss, stata, excel
    mtime: int
    size: int
    folder_id: str

@dataclass
class ScanPlan:
    to_scan: list[DatasetInfo]   # Nouveaux ou modifiés
    to_skip: list[DatasetInfo]   # Inchangés → marquer _seen
```

## API (nouveau param `depth`)

```python
def add_folder(
    path: str | Path,
    folder: Folder | None = None,
    *,
    depth: Literal["structure", "schema", "full"] = "full",  # NOUVEAU
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    recursive: bool = True,
    infer_stats: bool = True,      # Existant (ignoré si depth != "full")
    csv_encoding: str | None = None,
    refresh: bool | None = None,
    quiet: bool | None = None,
) -> None:
```

| depth         | Output                               |
| ------------- | ------------------------------------ |
| `"structure"` | Arborescence, formats, mtime, taille |
| `"schema"`    | + Variables (noms, types)            |
| `"full"`      | + nb_row, stats, modalités           |

---

## Plan d'implémentation

### A1: Extraire `compute_scan_plan()`

Factoriser la logique mtime/diff (dupliquée dans les 2 boucles actuelles).

### A2: Unifier les boucles

Créer `list[DatasetInfo]` unifiée (parquet + autres formats), une seule boucle.

### A3: Ajouter `depth` param

Default `"full"` (comportement actuel). Early return si `"structure"`.

### A4: Branches schema/full

Implémenter `scan_schema()` (lecture légère). Default reste `"full"` (rétro-compatibilité).
