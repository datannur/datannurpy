# Remote Storage Support - Plan de migration

## Objectif

Permettre à `add_folder()` et `add_dataset()` de fonctionner avec des sources distantes (SFTP, S3, Azure, GCS) sans dupliquer la logique métier existante.

```python
# API cible (inchangée)
catalog.add_folder("/data/local")                        # local
catalog.add_folder("sftp://user@server.com/data")        # SFTP (serveur Linux)
catalog.add_folder("s3://bucket/data")                   # S3
catalog.add_folder("az://container/data")                # Azure Blob
catalog.add_folder("gs://bucket/data")                   # GCS
```

## Cas d'usage prioritaire

**Contexte institutionnel/corporate** :

- Serveur Linux accessible en SFTP
- Postes Windows sans droits admin
- Authentification par clé SSH

→ Solution : fsspec + paramiko (Python pur, `pip install` suffit)

## Approche

Abstraire les opérations filesystem dans une couche dédiée utilisant **fsspec**, qui unifie l'accès local et cloud.

## Fichiers impactés

```
scanner/
├── filesystem.py     # NOUVEAU - Abstraction fsspec
├── utils.py          # find_files(), get_mtime_*()
├── discovery.py      # discover_datasets()
└── parquet/
    └── discovery.py  # is_delta_table(), is_hive_partitioned(), is_iceberg_table()
```

**Aucun changement** dans `add_folder.py`, `add_dataset.py`, `catalog.py`.

---

## Phases d'implémentation

### Phase 1 : Abstraction filesystem (local uniquement)

**Objectif** : Introduire fsspec sans changer le comportement, valider l'architecture.

1. Créer `scanner/filesystem.py` avec une classe `FileSystem` :

   ```python
   class FileSystem:
       def __init__(self, path: str):
           self.fs, self.root = fsspec.core.url_to_fs(path)

       def glob(self, pattern: str) -> list[str]: ...
       def isdir(self, path: str) -> bool: ...
       def isfile(self, path: str) -> bool: ...
       def exists(self, path: str) -> bool: ...
       def info(self, path: str) -> dict: ...  # mtime, size
       def open(self, path: str, mode: str = "rb"): ...
   ```

2. Adapter `find_files()` pour utiliser `FileSystem.glob()`

3. Adapter `get_mtime_*()` pour utiliser `FileSystem.info()`

4. **Tests** : Tous les tests existants doivent passer (comportement identique)

**Critère de succès** : `make check` passe, zéro régression.

---

### Phase 2 : Detection formats (Delta/Hive/Iceberg)

**Objectif** : Adapter la détection de formats pour fonctionner avec fsspec.

1. Refactoriser `is_delta_table(path)` → `is_delta_table(fs, path)`
2. Refactoriser `is_hive_partitioned(path)` → `is_hive_partitioned(fs, path)`
3. Refactoriser `is_iceberg_table(path)` → `is_iceberg_table(fs, path)`

**Changements clés** :

```python
# Avant
def is_delta_table(path: Path) -> bool:
    delta_log = path / "_delta_log"
    return delta_log.is_dir() and any(delta_log.glob("*.json"))

# Après
def is_delta_table(fs: FileSystem, path: str) -> bool:
    delta_log = f"{path}/_delta_log"
    return fs.isdir(delta_log) and fs.glob(f"{delta_log}/*.json")
```

**Critère de succès** : Tests existants passent, detection fonctionne en local.

---

### Phase 3 : Support remote dans scanners

**Objectif** : Permettre la lecture de fichiers distants (S3, SFTP...).

1. Implémenter `FileSystem.ensure_local()` pour le download temporaire
2. Adapter `scan_file()` pour utiliser `ensure_local()` sur formats non-streamables
3. Adapter les scanners Statistical (SAS/SPSS/Stata) et Excel
4. Ajouter le paramètre `storage_options` pour l'authentification

**Formats streamables** (pas de download) :

- Parquet : PyArrow supporte fsspec nativement
- CSV sur S3 : DuckDB supporte S3 nativement

**Formats non-streamables** (download temp) :

- SAS, SPSS, Stata : pyreadstat exige un fichier local
- Excel : openpyxl fonctionne avec file-like mais download plus simple
- CSV sur SFTP : DuckDB ne supporte pas SFTP

**Critère de succès** : `add_folder("sftp://server/data")` fonctionne avec des fichiers SAS.

---

### Phase 4 : Tests et documentation

1. Tests unitaires avec mocks fsspec (pas de vrai cloud/SFTP)
2. Tests d'intégration optionnels (avec credentials)
3. Documentation utilisateur (README, exemples SFTP/S3)
4. Gestion des credentials (clé SSH, variables d'environnement)

---

## Dépendances

| Package    | Usage                  | Taille | Statut                          |
| ---------- | ---------------------- | ------ | ------------------------------- |
| `fsspec`   | Abstraction filesystem | -      | Dépendance transitive (pyarrow) |
| `paramiko` | Support SFTP/SSH       | ~2 MB  | **Inclus par défaut**           |
| `s3fs`     | Support S3             | ~50 MB | Optional extra                  |
| `adlfs`    | Support Azure          | ~30 MB | Optional extra                  |
| `gcsfs`    | Support GCS            | ~40 MB | Optional extra                  |

**Logique** : paramiko est léger et couvre le cas prioritaire (SFTP institutionnel). Les SDKs cloud sont lourds et rarement tous nécessaires.

**pyproject.toml** :

```toml
[project.dependencies]
# ... autres dépendances existantes
paramiko = ">=4.0"

[project.optional-dependencies]
s3 = ["s3fs"]
azure = ["adlfs"]
gcs = ["gcsfs"]
cloud = ["s3fs", "adlfs", "gcsfs"]
```

**Usage** :

```bash
pip install datannurpy          # SFTP fonctionne direct
pip install datannurpy[s3]      # + support S3
pip install datannurpy[cloud]   # + tous les clouds
```

---

## Stratégie de lecture par format

Certains formats supportent le streaming, d'autres nécessitent un fichier local.

| Format         | Lib de lecture | Streaming distant | Stratégie             |
| -------------- | -------------- | ----------------- | --------------------- |
| Parquet        | PyArrow        | ✅                | Stream direct         |
| Delta          | delta-rs       | ✅ (S3/GCS/Azure) | Stream direct         |
| CSV            | DuckDB         | ⚠️ (S3 seulement) | Download temp si SFTP |
| SAS/SPSS/Stata | pyreadstat     | ❌                | Download temp         |
| Excel          | openpyxl       | ❌                | Download temp         |

**Download temporaire** (transparent pour l'utilisateur) :

```python
# Implémenté dans FileSystem
def ensure_local(self, path: str) -> str:
    """Retourne un chemin local (télécharge si distant + format non-streamable)."""
    if self.is_local or self._supports_streaming(path):
        return path
    # Télécharge dans un fichier temporaire
    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(path).suffix) as tmp:
        self.fs.download(path, tmp.name)
        return tmp.name
```

---

## Risques et mitigations

| Risque                           | Mitigation                     |
| -------------------------------- | ------------------------------ |
| Performance listing gros buckets | Pagination, lazy loading       |
| Credentials non configurés       | Message d'erreur clair         |
| Formats non-streamables (SAS...) | Download temp transparent      |
| Latence SFTP sur gros fichiers   | Progress bar, cache local      |
| Incompatibilité fsspec/pyarrow   | Vérifier versions compatibles  |
| Timeout connexion SSH            | Paramètre timeout configurable |

---

## Authentification SFTP

Authentification par **clé SSH** (best practice sécurité).

```python
catalog.add_folder(
    "sftp://serveur.institution.com/data",
    storage_options={
        "username": "user",
        "key_filename": "~/.ssh/ma_cle",  # Chemin vers la clé privée
    }
)
```
