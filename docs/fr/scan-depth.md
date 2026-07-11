# Profondeur de scan

Le paramètre `depth` contrôle la quantité de métadonnées extraites des fichiers, dossiers, datasets et bases de données. Définissez-le une fois comme valeur par défaut globale, ou surchargez-le pour n'importe quelle entrée `add`.

```yaml
depth: variable                   # valeur par défaut globale

add:
  - folder: ./data                # hérite de "variable"

  - folder: ./big
    depth: stat                   # surcharge pour cette entrée

  - database: sqlite:///db.sqlite
    depth: dataset
```

| Fonctionnalité                                | `dataset` | `variable` | `stat`         | `value` (défaut)   |
| --------------------------------------------- | :-------: | :--------: | :------------: | :----------------: |
| Dossiers                                      | ✓         | ✓          | ✓              | ✓                  |
| Datasets (format, chemin, mtime)              | ✓         | ✓          | ✓              | ✓                  |
| Variables (noms, types)                       |           | ✓          | ✓              | ✓                  |
| Introspection BD (PK, FK, commentaires)       |           | ✓          | ✓              | ✓                  |
| Nombre de lignes, statistiques                |           |            | ✓              | ✓                  |
| Énumérations, fréquences, motifs              |           |            |                | ✓                  |
| Auto-tagging (format, sécurité, texte)        |           |            |                | ✓                  |

> **Note :** À `depth="variable"`, les fichiers CSV et Excel n'extraient que les **noms** de colonnes (les types nécessitent de lire les données, disponibles à partir de `depth="stat"`). Tous les autres formats fournissent les types à ce niveau.

**Cas d'usage typiques :**

- **`dataset`** — inventaire rapide des fichiers/tables disponibles sans lire les données
- **`variable`** — découverte légère du schéma (noms et types de colonnes)
- **`stat`** — profilage des données sans détection d'énumérations (plus rapide que `value`)
- **`value`** — catalogue complet avec tables de fréquences et affectation d'énumérations (défaut)

Si vous avez besoin des fréquences de valeurs mais gérez les énumérations manuellement via `metadata_path`, utilisez `auto_enumerations: false` avec `depth: value`. Les fréquences restent disponibles, mais les entités d'énumération automatiques et les liens de variables générés sont ignorés.

## Auto-tagging

À `depth="value"` (défaut), les colonnes de type chaîne sont automatiquement taguées selon leur type de contenu. Les tags utilisent une hiérarchie à deux niveaux sous le parent `auto` :

| Catégorie    | Tags                                                       |
| ------------ | ---------------------------------------------------------- |
| **Format**   | `auto---email`, `auto---phone`, `auto---uuid`, `auto---iban` |
| **Sécurité** | `auto---bcrypt`, `auto---argon2`, `auto---jwt`, `auto---secret` |
| **Texte**    | `auto---structured`, `auto---semi-structured`, `auto---free-text` → `auto---natural-text` |

Chaque variable reçoit au plus **un tag feuille**. Le frontend peut utiliser `parent_id` pour filtrer par catégorie (par exemple, sélectionner `auto---security` affiche toutes les variables bcrypt/argon2/jwt/secret).

**Les colonnes taguées sécurité** (bcrypt, argon2, jwt, secret) voient leurs valeurs de fréquence brutes supprimées — seules les fréquences de motifs sont émises, de sorte qu'aucun secret réel n'apparaît dans le catalogue exporté.

## Tags de politique

Les tags de politique permettent de contrôler manuellement le comportement du scan pour des variables spécifiques. Comme les auto-tags, ils vivent sous la hiérarchie `scan` et sont créés automatiquement — aucune entrée dans `tag.csv` n'est nécessaire.

| Tag                      | Effet                                                          |
| ------------------------ | -------------------------------------------------------------- |
| `policy---frequency-hidden`   | Supprime toutes les données de fréquence et d'énumération (les statistiques restent visibles) |

Affectez le tag dans vos métadonnées `variable.csv` :

```csv
id,tag_ids
source---employees_csv---first_name,"policy---frequency-hidden"
```

C'est utile pour les colonnes sensibles que l'auto-tagging ne détecte pas (par exemple les prénoms, identifiants internes, codes métier).
