# Migration vers jsonjsdb

## Contexte

Remplacement de la représentation interne du catalogue par `jsonjsdb` pour :

- Réduire le code (~860 → ~140 lignes)
- Externaliser la gestion des données (CRUD, relations, import/export)
- Bénéficier du typage avec TypedDict

## Déjà fait

- `src/datannurpy/schema.py` : TypedDict pour toutes les entités + `DatannurDB(Jsonjsdb)`
- `jsonjsdb>=0.2.1` ajouté aux dépendances dev
- Test de compatibilité validé sur un export existant

## À faire

### 1. Adapter `Catalog`

Faire hériter de `DatannurDB` ou composer avec :

```python
class Catalog(DatannurDB):
    # Options de scan
    refresh: bool
    freq_threshold: int
    csv_encoding: str | None
    quiet: bool

    # Runtime state
    _now: int
    _freq_tables: list[pa.Table]
    modality_manager: ModalityManager
```

### 2. Migrer les listes → tables jsonjsdb

| Actuel                     | Nouveau                      |
| -------------------------- | ---------------------------- |
| `self.folders.append(f)`   | `self.folder.add(f)`         |
| `self.datasets.extend(ds)` | `self.dataset.add_all(ds)`   |
| `for f in self.folders`    | `for f in self.folder.all()` |
| `self._folder_index[id]`   | `self.folder.get(id)`        |

### 3. Adapter les modules

- `add_folder.py` : utiliser `catalog.folder.add()`, `catalog.folder.get()`
- `add_dataset.py` : utiliser `catalog.dataset.add()`, `catalog.variable.add_all()`
- `add_database.py` : idem
- `add_metadata.py` : utiliser `catalog.*.update()`
- `finalize.py` : utiliser `catalog.*.remove()` pour les entités non vues
- `exporter/db.py` : remplacer par `catalog.save()`

### 4. Supprimer le code obsolète

- `importer/db.py` (161 lignes)
- `exporter/db.py` (264 lignes)
- `entities/` : garder uniquement pour export public ou migrer vers `schema.py`
- Index manuels (`_dataset_index`, `_folder_index`, etc.)

### 5. Adapter les tests

Les tests utilisent les dataclasses → adapter pour TypedDict/dicts.

## Estimation

~4-6h de travail, à faire en une session pour éviter un état intermédiaire cassé.

## Validation

```bash
make check  # doit passer à 100%
```
