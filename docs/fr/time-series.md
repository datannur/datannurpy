# Regroupement de séries temporelles

datannurpy peut regrouper des fichiers ou des tables de base de données répétés en un seul dataset quand leurs noms ne diffèrent que par une période temporelle. Ce comportement est activé par défaut pour `add_folder()` et `add_database()` avec `time_series: true`.

## Motifs pris en charge {#supported-patterns}

Le détecteur reconnaît les années, trimestres, mois et dates complètes dans les noms ou les chemins :

| Type de période | Exemples | Stocké comme | Motif metadata-first |
| --------------- | -------- | ------------ | ---------------------- |
| Année | `sales_2024.csv`, `stats_2024` | `2024` | `[YYYY]` |
| Trimestre | `sales_2024Q1.csv`, `stats_2024T2` | `2024Q1`, `2024Q2` | `[YYYY]Q[N]` |
| Mois | `sales_2024-03.csv`, `sales_202403` | `2024/03` | `[YYYY/MM]` |
| Date | `sales_2024-03-15.csv`, `sales_20240315` | `2024/03/15` | `[YYYY/MM/DD]` |

Les périodes partielles telles que `01`, `02`, `Q1` ou `day15` ne sont utilisables que lorsqu'une année complète à 4 chiffres est disponible ailleurs dans le même chemin ou nom de table. Une séquence telle que `report_01.csv`, `report_02.csv` est conservée en datasets séparés car elle n'a aucun contexte d'année.

## Fichiers

Lors du scan de dossiers, les datasets de fichiers simples de même format peuvent être regroupés :

```text
data/
+-- enquete_2020.csv
+-- enquete_2021.csv
+-- enquete_2022.csv
`-- reference.csv
```

Cela crée un dataset nommé `enquete_[YYYY]` avec `nb_resources=3` ; `reference.csv` reste un dataset distinct.

Les périodes peuvent aussi apparaître dans les dossiers :

```text
data/
+-- 2024/01/sales.csv
+-- 2024/02/sales.csv
`-- 2024/03/sales.csv
```

Cela crée un dataset `sales` avec les périodes `2024/01`, `2024/02` et `2024/03`. Les dossiers temporels ne sont pas créés comme dossiers du catalogue pour la série ; seuls les dossiers parents non temporels sont conservés.

Les datasets partitionnés tels que Delta, Hive et Iceberg sont gérés par leurs propres scanners de datasets et ne sont pas regroupés par ce détecteur de séries temporelles.

## Tables de base de données

Lors du scan de bases de données, les tables sont regroupées par schéma après application des filtres `include` et `exclude` :

```text
sales_fact_202401
sales_fact_202402
sales_fact_202403
dim_customer
```

Cela crée un dataset nommé `sales_fact_[YYYY/MM]` avec `nb_resources=3` ; `dim_customer` reste un dataset distinct.

Les mêmes règles de validation s'appliquent aux tables et aux fichiers. Par exemple, `dim_age_01`, `dim_age_02` et `dim_age_03` ne sont pas regroupées car il n'y a pas d'année à 4 chiffres. Les granularités mixtes sont scindées en datasets distincts : `data_2021`, `data_2022`, `data_2021Q1` et `data_2021Q2` deviennent un dataset annuel et un dataset trimestriel.

## Métadonnées résultantes

Pour une série regroupée, le dataset reçoit :

- `nb_resources` : nombre de fichiers ou de tables de la série
- `start_date` : première période détectée
- `end_date` : dernière période détectée
- `data_path` : le fichier ou la table le plus récent, utilisé comme ressource canonique

Dans les scans metadata-first (`create_folders=False`), le `_match_path` technique de `metadata/dataset.csv` peut utiliser la syntaxe de série normalisée de la colonne [Motif metadata-first](#supported-patterns) ci-dessus — par exemple `sales_[YYYY].csv` ou `sales_[YYYY/MM].csv` — pour correspondre à la série logique plutôt qu'à un dernier fichier spécifique.

À `depth="stat"` ou `depth="value"`, les statistiques et les tables de fréquences sont calculées uniquement sur la dernière période. Les périodes plus anciennes sont scannées en mode schéma seul afin que datannurpy puisse détecter la disponibilité des variables dans le temps.

Les variables reçoivent aussi `start_date` et `end_date` quand leur présence change d'une période à l'autre. Une variable présente dans toutes les périodes a ces deux champs vides. Une variable ajoutée après la première période reçoit `start_date` ; une variable retirée avant la dernière période reçoit `end_date`.

## Éviter les faux positifs

datannurpy conserve les candidats en datasets séparés quand le regroupement serait ambigu ou incomplet :

- moins de deux fichiers ou tables correspondants
- aucune année à 4 chiffres nulle part dans le groupe candidat
- fragments de mois ou de jour sans contexte d'année/de mois suffisant
- tables ou fichiers dont le nom entier n'est qu'un jeton temporel
- granularités de période mixtes qui doivent être représentées comme des séries distinctes

Les jetons constants d'apparence temporelle, comme un dossier de date de livraison ou un préfixe numérique fixe, sont préservés comme texte littéral quand une autre période variable identifie la série.

## Désactiver la détection

Définissez `time_series: false` pour traiter chaque fichier ou table indépendamment :

```yaml
add:
  - folder: ./data
    time_series: false

  - database: postgresql://localhost/mydb
    time_series: false
```
