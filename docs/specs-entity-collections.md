# Specs: Entity Collections

## Objectif

Remplacer les listes d'entités (`list[Entity]`) par des collections encapsulées qui maintiennent automatiquement leurs index internes. Cela garantit la cohérence entre les listes et les index, et simplifie le code appelant.

## Problème actuel

```python
class Catalog:
    datasets: list[Dataset] = []
    _dataset_index: dict[str, Dataset] = {}  # Doit être synchronisé manuellement

    # Risque: oubli de mise à jour de l'index
    self.datasets.append(dataset)  # ❌ Index non mis à jour
```

## Solution cible

```python
class Catalog:
    datasets: DatasetCollection

    # Usage
    self.datasets.add(dataset)           # ✅ Index mis à jour automatiquement
    self.datasets.get_by_path(path)      # ✅ Lookup O(1)
    for ds in self.datasets:             # ✅ Itération
```

---

## Collections à créer

| Collection              | Entité        | Index            | Clé d'index                              |
| ----------------------- | ------------- | ---------------- | ---------------------------------------- |
| `FolderCollection`      | `Folder`      | `_by_id`         | `folder.id`                              |
| `DatasetCollection`     | `Dataset`     | `_by_data_path`  | `dataset.data_path`                      |
| `VariableCollection`    | `Variable`    | `_by_dataset_id` | `variable.dataset_id` → `list[Variable]` |
| `ModalityCollection`    | `Modality`    | `_by_id`         | `modality.id`                            |
| `ValueCollection`       | `Value`       | _(aucun)_        | —                                        |
| `InstitutionCollection` | `Institution` | _(aucun)_        | —                                        |
| `TagCollection`         | `Tag`         | _(aucun)_        | —                                        |
| `DocCollection`         | `Doc`         | _(aucun)_        | —                                        |

> Note: Les collections sans index (Value, Institution, Tag, Doc) peuvent utiliser une classe de base générique.

---

## Interface commune (ABC)

```python
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Iterator

T = TypeVar("T")

class EntityCollection(ABC, Generic[T]):
    """Base class for entity collections."""

    def __init__(self) -> None:
        self._items: list[T] = []

    def add(self, entity: T) -> None:
        """Add entity to collection."""
        self._items.append(entity)
        self._on_add(entity)

    def remove(self, entity: T) -> None:
        """Remove entity from collection."""
        self._items.remove(entity)
        self._on_remove(entity)

    def clear(self) -> None:
        """Remove all entities."""
        for entity in list(self._items):
            self.remove(entity)

    def filter(self, predicate: Callable[[T], bool]) -> None:
        """Keep only entities matching predicate (in-place)."""
        to_remove = [e for e in self._items if not predicate(e)]
        for entity in to_remove:
            self.remove(entity)

    def _on_add(self, entity: T) -> None:
        """Hook for subclasses to update indexes."""
        pass

    def _on_remove(self, entity: T) -> None:
        """Hook for subclasses to update indexes."""
        pass

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return len(self._items) > 0

    def __getitem__(self, index: int) -> T:
        return self._items[index]
```

---

## Collections spécialisées

### FolderCollection

```python
class FolderCollection(EntityCollection[Folder]):
    """Collection of folders with id index."""

    def __init__(self) -> None:
        super().__init__()
        self._by_id: dict[str, Folder] = {}

    def _on_add(self, folder: Folder) -> None:
        self._by_id[folder.id] = folder

    def _on_remove(self, folder: Folder) -> None:
        self._by_id.pop(folder.id, None)

    def get_by_id(self, folder_id: str) -> Folder | None:
        """Get folder by id (O(1))."""
        return self._by_id.get(folder_id)
```

### DatasetCollection

```python
class DatasetCollection(EntityCollection[Dataset]):
    """Collection of datasets with data_path index."""

    def __init__(self) -> None:
        super().__init__()
        self._by_data_path: dict[str, Dataset] = {}

    def _on_add(self, dataset: Dataset) -> None:
        if dataset.data_path:
            self._by_data_path[dataset.data_path] = dataset

    def _on_remove(self, dataset: Dataset) -> None:
        if dataset.data_path:
            self._by_data_path.pop(dataset.data_path, None)

    def get_by_data_path(self, data_path: str) -> Dataset | None:
        """Get dataset by data_path (O(1))."""
        return self._by_data_path.get(data_path)
```

### VariableCollection

```python
class VariableCollection(EntityCollection[Variable]):
    """Collection of variables with dataset_id index."""

    def __init__(self) -> None:
        super().__init__()
        self._by_dataset_id: dict[str, list[Variable]] = {}

    def _on_add(self, variable: Variable) -> None:
        if variable.dataset_id not in self._by_dataset_id:
            self._by_dataset_id[variable.dataset_id] = []
        self._by_dataset_id[variable.dataset_id].append(variable)

    def _on_remove(self, variable: Variable) -> None:
        if variable.dataset_id in self._by_dataset_id:
            vars_list = self._by_dataset_id[variable.dataset_id]
            if variable in vars_list:
                vars_list.remove(variable)
            if not vars_list:
                del self._by_dataset_id[variable.dataset_id]

    def get_by_dataset_id(self, dataset_id: str) -> list[Variable]:
        """Get all variables for a dataset (O(1))."""
        return self._by_dataset_id.get(dataset_id, [])

    def count_by_dataset_id(self, dataset_id: str) -> int:
        """Count variables for a dataset (O(1))."""
        return len(self._by_dataset_id.get(dataset_id, []))

    def remove_by_dataset_id(self, dataset_id: str) -> None:
        """Remove all variables for a dataset."""
        for var in list(self.get_by_dataset_id(dataset_id)):
            self.remove(var)
```

### ModalityCollection

```python
class ModalityCollection(EntityCollection[Modality]):
    """Collection of modalities with id index."""

    def __init__(self) -> None:
        super().__init__()
        self._by_id: dict[str, Modality] = {}

    def _on_add(self, modality: Modality) -> None:
        self._by_id[modality.id] = modality

    def _on_remove(self, modality: Modality) -> None:
        self._by_id.pop(modality.id, None)

    def get_by_id(self, modality_id: str) -> Modality | None:
        """Get modality by id (O(1))."""
        return self._by_id.get(modality_id)
```

### SimpleCollection (pour entités sans index)

```python
class SimpleCollection(EntityCollection[T]):
    """Collection without indexes for simple entities."""
    pass

# Usage
values: SimpleCollection[Value]
institutions: SimpleCollection[Institution]
tags: SimpleCollection[Tag]
docs: SimpleCollection[Doc]
```

---

## Méthodes utilitaires à ajouter

### Bulk operations

```python
class EntityCollection(ABC, Generic[T]):
    def extend(self, entities: Iterable[T]) -> None:
        """Add multiple entities."""
        for entity in entities:
            self.add(entity)

    def to_list(self) -> list[T]:
        """Return a copy of the internal list."""
        return list(self._items)
```

### Load from list (pour importer)

```python
class EntityCollection(ABC, Generic[T]):
    @classmethod
    def from_list(cls, items: list[T]) -> Self:
        """Create collection from existing list."""
        collection = cls()
        for item in items:
            collection.add(item)
        return collection
```

---

## Migration du Catalog

### Avant

```python
class Catalog:
    def __init__(self):
        self.folders: list[Folder] = []
        self.datasets: list[Dataset] = []
        self.variables: list[Variable] = []
        self.modalities: list[Modality] = []
        self.values: list[Value] = []
        self.institutions: list[Institution] = []
        self.tags: list[Tag] = []
        self.docs: list[Doc] = []

        # Index manuels
        self._dataset_index: dict[str, Dataset] = {}
        self._variables_by_dataset: dict[str, list[Variable]] = {}
        self._folder_index: dict[str, Folder] = {}
        self._modality_index: dict[str, Modality] = {}
```

### Après

```python
class Catalog:
    def __init__(self):
        self.folders = FolderCollection()
        self.datasets = DatasetCollection()
        self.variables = VariableCollection()
        self.modalities = ModalityCollection()
        self.values = SimpleCollection[Value]()
        self.institutions = SimpleCollection[Institution]()
        self.tags = SimpleCollection[Tag]()
        self.docs = SimpleCollection[Doc]()

        # Plus besoin d'index manuels !
```

---

## Changements d'API

| Avant                                        | Après                                       |
| -------------------------------------------- | ------------------------------------------- |
| `catalog.datasets.append(ds)`                | `catalog.datasets.add(ds)`                  |
| `catalog.datasets.extend(list)`              | `catalog.datasets.extend(list)`             |
| `catalog._dataset_index.get(path)`           | `catalog.datasets.get_by_data_path(path)`   |
| `catalog._dataset_index[path] = ds`          | `catalog.datasets.add(ds)`                  |
| `catalog.datasets = [d for d if ...]`        | `catalog.datasets.filter(lambda d: ...)`    |
| `len(catalog.datasets)`                      | `len(catalog.datasets)` ✅                  |
| `for ds in catalog.datasets:`                | `for ds in catalog.datasets:` ✅            |
| `catalog._variables_by_dataset.get(id, [])`  | `catalog.variables.get_by_dataset_id(id)`   |
| `sum(1 for v in vars if v.dataset_id == id)` | `catalog.variables.count_by_dataset_id(id)` |
| `catalog._folder_index.get(id)`              | `catalog.folders.get_by_id(id)`             |
| `catalog._modality_index.get(id)`            | `catalog.modalities.get_by_id(id)`          |

---

## Plan de migration

### Étape 1 : Créer le module collections (sans intégration)

1. Créer `src/datannurpy/collections/__init__.py`
2. Créer `src/datannurpy/collections/base.py` avec `EntityCollection`
3. Créer `src/datannurpy/collections/folder.py` avec `FolderCollection`
4. Créer `src/datannurpy/collections/dataset.py` avec `DatasetCollection`
5. Créer `src/datannurpy/collections/variable.py` avec `VariableCollection`
6. Créer `src/datannurpy/collections/modality.py` avec `ModalityCollection`
7. Créer `src/datannurpy/collections/simple.py` avec `SimpleCollection`
8. Écrire les tests unitaires pour chaque collection

### Étape 2 : Migrer Catalog (une collection à la fois)

Pour chaque collection, dans cet ordre :

1. **FolderCollection** (peu de dépendances)
   - Modifier `Catalog.__init__`
   - Mettre à jour `importer/db.py`
   - Mettre à jour `finalize.py`
   - Mettre à jour `utils/folder.py`
   - Mettre à jour `utils/modality.py` (ensure_modalities_folder)
   - Supprimer `_folder_index`

2. **ModalityCollection**
   - Mettre à jour `Catalog.__init__`
   - Mettre à jour `importer/db.py`
   - Mettre à jour `finalize.py`
   - Mettre à jour `utils/modality.py`
   - Supprimer `_modality_index`

3. **DatasetCollection**
   - Mettre à jour `Catalog.__init__`
   - Mettre à jour `importer/db.py`
   - Mettre à jour `add_folder.py`, `add_dataset.py`, `add_database.py`
   - Mettre à jour `finalize.py`
   - Supprimer `_dataset_index`

4. **VariableCollection**
   - Mettre à jour `Catalog.__init__`
   - Mettre à jour `importer/db.py`
   - Mettre à jour `add_folder.py`, `add_dataset.py`, `add_database.py`
   - Mettre à jour `finalize.py`
   - Supprimer `_variables_by_dataset` et `_add_variables`, `_get_variable_count`

5. **SimpleCollection** (values, institutions, tags, docs)
   - Migrer les 4 collections simples
   - Mettre à jour `importer/db.py`
   - Mettre à jour `finalize.py`
   - Mettre à jour `add_metadata.py`

### Étape 3 : Nettoyage

1. Supprimer les index manuels du Catalog
2. Supprimer les méthodes helper (`_add_variables`, `_get_variable_count`)
3. Mettre à jour les tests d'intégrité
4. Mettre à jour la documentation

### Étape 4 : Validation

1. `make check` passe
2. Coverage 100%
3. Exécuter `demo.py` et vérifier le résultat
4. Test de performance comparatif (optionnel)

---

## Structure de fichiers cible

```
src/datannurpy/
├── collections/
│   ├── __init__.py          # Exports publics
│   ├── base.py              # EntityCollection ABC
│   ├── folder.py            # FolderCollection
│   ├── dataset.py           # DatasetCollection
│   ├── variable.py          # VariableCollection
│   ├── modality.py          # ModalityCollection
│   └── simple.py            # SimpleCollection[T]
├── catalog.py               # Utilise les collections
└── ...
```

---

## Risques et mitigations

| Risque                          | Mitigation                                        |
| ------------------------------- | ------------------------------------------------- |
| Régression fonctionnelle        | Tests existants + nouveaux tests unitaires        |
| Oubli de migration d'un fichier | Recherche exhaustive avec grep avant chaque étape |
| Performance dégradée            | Benchmark avant/après (optionnel)                 |
| Breaking change API publique    | `catalog.datasets` reste itérable et indexable    |

---

## Critères de succès

- [ ] Toutes les listes d'entités utilisent des Collections
- [ ] Aucun index manuel dans Catalog (`_dataset_index`, etc.)
- [ ] `make check` passe (ruff, pyright, pytest, coverage 100%)
- [ ] API publique compatible (itération, len, indexation)
- [ ] Code plus simple dans `finalize.py` et `add_*.py`
