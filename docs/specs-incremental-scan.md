# Spécifications : Scan incrémental

## Principe général

Le catalog JSON exporté sert aussi d'état pour le scan incrémental. On le charge à l'init, on compare avec l'état actuel des sources, et on ne rescanne que ce qui a changé.

## Chargement et export

- `Catalog(db_path=...)` charge le catalog existant depuis le dossier JSON.
- `export_db()` utilise `db_path` par défaut si défini à l'init, sinon paramètre obligatoire.

## Paramètre `refresh`

Peut être défini globalement à l'init du `Catalog`, puis overridé par méthode. Valeur par défaut `False` (mode incrémental). Si `True`, force le rescan complet en ignorant le cache.

S'applique à `add_folder`, `add_dataset` et `add_database`. Ne s'applique pas à `add_metadata` qui fonctionne toujours en mode fresh.

## Détection des changements

### Fichiers

Comparaison du mtime via `last_update_timestamp` (nouveau champ, Unix timestamp int). Identifiant unique : chemin absolu (`data_path`).

Pour les fichiers, `last_update_timestamp` contient le mtime du fichier.

### Tables de base de données

Comparaison de la signature composée du hash du schéma (colonnes et types) et du nombre de lignes. Champ `schema_signature` pour le hash, champ `nb_row` existant pour le nombre de lignes.

Pour les tables, `last_update_timestamp` contient le moment du dernier scan ayant détecté un changement.

Limitation connue : ne détecte pas les UPDATE purs sans changement de volume.

## Comportements selon les situations

| Situation       | Comportement                 |
| --------------- | ---------------------------- |
| Inchangé        | Skip                         |
| Modifié         | Suppression cascade → Rescan |
| Supprimé/absent | Suppression cascade          |
| Nouveau         | Scan et ajout                |

Note : un fichier déplacé ou renommé est traité comme une suppression de l'ancien chemin et une création au nouveau chemin. Le système ne détecte pas le lien entre les deux.

## Finalisation

Méthode `finalize()` appelable manuellement ou déclenchée automatiquement une seule fois lors de `export_db()` ou `export_app()`. Les appels suivants sont des no-op silencieux (idempotent).

Fonctionnement :

1. Au chargement du catalog, chaque folder, dataset, modalité, institution, tag et doc a un flag interne `_seen = False`
2. Pendant `add_folder`, `add_dataset`, `add_database` ou `add_metadata`, quand une entité est traitée (inchangée, modifiée, ou nouvelle), on met `_seen = True`
3. `finalize()` supprime en cascade tous les folders avec `_seen = False` et leurs datasets
4. `finalize()` supprime en cascade tous les datasets avec `_seen = False` :
   - Supprimer ses variables
   - Supprimer les fréquences liées à ces variables
   - Supprimer le dataset
5. `finalize()` supprime les modalités, institutions, tags et docs avec `_seen = False`
6. `finalize()` supprime les values des modalités supprimées

Note : les variables doivent toujours avoir un `dataset_id` valide. Si une variable référence un dataset inexistant, un warning est loggé.

## Nouveaux champs entité

`Dataset.last_update_timestamp` : Unix timestamp (int, secondes depuis epoch). Pour les fichiers : mtime du fichier. Pour les tables DB : moment du dernier scan ayant détecté un changement.

`Dataset.schema_signature` : hash des colonnes et types, utilisé pour les tables de base de données.

Le champ `last_update_date` existant est conservé pour compatibilité legacy mais sera supprimé à terme.

## Modifications à implémenter

### Chargement et persistance du catalog

Fichiers : `catalog.py`, `importer/db.py` (nouveau)

Le constructeur `Catalog` accepte un paramètre optionnel `db_path`. Si le dossier n'existe pas ou est vide, un catalog vide est créé. Si le chemin est invalide (pas un dossier), une erreur est levée et l'exécution s'arrête.

Si `db_path` pointe vers un catalog existant, les fichiers JSON (folder.json, dataset.json, variable.json, modality.json, value.json, freq.json, institution.json, tag.json, doc.json) sont parsés et les entités reconstruites en mémoire. Chaque folder, dataset, modalité, institution, tag et doc chargé reçoit un flag interne `_seen = False`.

Pour les fréquences, `freq.json` est lu et reconstruit en PyArrow Table pour `_freq_tables`.

La méthode `export_db()` utilise `db_path` comme chemin par défaut si défini à l'init. Le paramètre reste overridable explicitement.

Un index interne `{data_path: Dataset}` est construit pour permettre des lookups rapides pendant le scan.

### Paramètre refresh global

Fichiers : `catalog.py`, `add_folder.py`, `add_dataset.py`, `add_database.py`

Le constructeur `Catalog` accepte un paramètre `refresh` (défaut `False`). Ce paramètre est transmis par défaut aux méthodes `add_folder`, `add_dataset` et `add_database`, mais peut être overridé à chaque appel. Quand `refresh=True`, la comparaison avec le cache est ignorée et tout est rescanné.

### Logique incrémentale pour les fichiers

Fichiers : `add_folder.py`, `entities/dataset.py`

Nouveau champ `Dataset.last_update_timestamp` (Unix timestamp int). Pour chaque fichier trouvé, on compare le mtime actuel avec celui stocké. Si identique, on skip et on marque `_seen = True`. Si différent ou nouveau, on supprime l'ancien dataset en cascade puis on rescanne.

### Logique incrémentale pour les tables

Fichiers : `add_database.py`, `entities/dataset.py`

Nouveau champ `Dataset.schema_signature` (hash des colonnes et types). Pour chaque table, on calcule la signature actuelle (schema + nb_row) et on compare. Si identique, on skip et on marque `_seen = True`. Si différent ou nouveau, on supprime l'ancien dataset en cascade puis on rescanne. Le champ `last_update_timestamp` est mis à jour avec le moment du scan quand un changement est détecté.

### Finalisation et nettoyage

Fichiers : `finalize.py` (nouveau)

Nouvelle méthode `finalize()` déclenchée automatiquement avant `export_db()` ou `export_app()`, ou appelable manuellement. Les appels suivants sont des no-op silencieux (idempotent).

Elle effectue dans l'ordre :

- Suppression cascade des folders avec `_seen = False` et de leurs datasets
- Suppression cascade des datasets avec `_seen = False` (variables, fréquences associées)
- Suppression des modalités, institutions, tags et docs avec `_seen = False`
- Suppression des values des modalités supprimées

## Étapes d'implémentation

### Étape 1 : Nouveaux champs entité

Ajouter `Dataset.last_update_timestamp` et `Dataset.schema_signature`. Vérifier que l'export JSON inclut ces champs. Pas de changement de comportement.

### Étape 2 : Import du catalog

Créer `importer/db.py` avec `load_db()` qui parse les JSON et reconstruit les entités. Tester le round-trip : export → import → export doit donner le même résultat. Pas encore utilisé par `Catalog.__init__`.

### Étape 3 : Intégration dans Catalog

Ajouter le paramètre `db_path` à l'init qui appelle `load_db()`. Ajouter le paramètre `refresh` global. Modifier `export_db()` pour utiliser `db_path` par défaut. Tester que `Catalog(db_path=...)` charge bien les entités.

### Étape 4 : Logique incrémentale fichiers

Modifier `add_folder` et `add_dataset` pour comparer le mtime. Ajouter le flag `_seen` sur les entités. Construire l'index `{data_path: Dataset}`. Tester : fichier inchangé → skip, fichier modifié → rescan.

### Étape 5 : Logique incrémentale tables

Modifier `add_database` pour comparer la signature (schema + nb_row). Tester : table inchangée → skip, table modifiée → rescan.

### Étape 6 : Finalisation

Créer `finalize.py`. Intégrer l'appel dans `export_db()` et `export_app()`. Tester : entités non vues → supprimées.
