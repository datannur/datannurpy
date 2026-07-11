# Scanner des bases de données

Utilisez la [Profondeur de scan](/fr/scan-depth) pour choisir la quantité de métadonnées extraites par datannurpy. Le même paramètre `depth` s'applique à `add_database`, `add_folder` et `add_dataset`, globalement ou par entrée `add`.

## Chaînes de connexion

| Backend    | Format                                              |
| ---------- | --------------------------------------------------- |
| SQLite     | `sqlite:///path/to/db.sqlite`                       |
| GeoPackage | `sqlite:///path/to/geodata.gpkg`                    |
| PostgreSQL | `postgresql://user:pass@host:5432/database`         |
| MySQL      | `mysql://user:pass@host:3306/database`              |
| Oracle     | `oracle://user:pass@host:1521/service_name`         |
| SQL Server | `mssql://user:pass@host:1433/database`              |
| SQLite distant | `sftp://host/path/db.sqlite`                    |
| DuckDB     | passer directement un backend `ibis.duckdb.connect(...)` |

Un GeoPackage est un fichier SQLite, il est donc scanné comme une base de données — chaque couche devient un dataset, enrichi en plus de `crs`/`geometry_type`/`bbox`. Voir [Formats géospatiaux](/fr/scanning-files#geospatial-formats).

## Utilisation de base

```yaml
add:
  - database: sqlite:///path/to/db.sqlite
  - database: postgresql://user:pass@host:5432/mydb
  - database: mysql://user:pass@host:3306/mydb
  - database: oracle://user:pass@host:1521/service_name
  - database: mssql://user:pass@host:1433/mydb
```

## SSL/TLS

```yaml
add:
  - database: postgresql://user:pass@host/db?sslmode=require
```

## SQL Server avec authentification Windows

Nécessite une configuration Kerberos appropriée.

```yaml
add:
  - database: mssql://host/db?TrustedConnection=yes
```

## Filtrage et échantillonnage

```yaml
add:
  - database: postgresql://localhost/mydb
    schema: public
    include: ["sales_*"]
    exclude: ["*_tmp"]
    sample_size: 10000
    preview_rows: 25
    group_by_prefix: true       # regrouper les tables par préfixe commun (défaut)
    prefix_min_tables: 2        # nombre minimum de tables pour former un groupe
```

  `include` et `exclude` sont comparés aux noms de tables après que le filtre optionnel `schema` a sélectionné le ou les schémas à scanner. Ils utilisent des motifs glob standard (`*`, `?` et classes de caractères telles que `[abc]`), pas des chemins de fichiers. Le filtrage conserve d'abord les tables qui correspondent à au moins un motif `include` quand `include` est défini, puis retire les tables qui correspondent à un motif `exclude`.

  `sample_size` contrôle les lignes utilisées pour la détection de fréquences et d'énumérations automatiques. Définissez `auto_enumerations: false` globalement ou sur une entrée de base de données pour conserver les fréquences de `depth: value` sans créer d'entités d'énumération automatiques ni de liens de variables générés. `preview_rows` contrôle le nombre maximum de lignes exportées pour l'aperçu de chaque table aux profondeurs `stat` et `value` ; la valeur par défaut est `100`, et `0` ou `false` désactive les aperçus. Les datasets de séries de tables prévisualisent la table de la période la plus récente, en cohérence avec le scan des statistiques.

  Exemples :

  | Motif | Signification |
  | ------- | ------- |
  | `employees` | Nom de table exact |
  | `sales_*` | Tables dont le nom commence par `sales_` |
  | `*_tmp` | Tables dont le nom se termine par `_tmp` |
  | `fact_????` | Tables telles que `fact_2024` |
  | `[de]*` | Tables commençant par `d` ou `e` |

## Détection de séries temporelles

Quand `time_series: true` (défaut), les tables dont les noms ne diffèrent que par un motif temporel sont regroupées en un seul dataset après application des filtres `include` et `exclude` :

```text
sales_fact_202401
sales_fact_202402
sales_fact_202403
```

Cela crée un dataset nommé `sales_fact_[YYYY/MM]` avec `nb_resources=3`. Définissez `time_series: false` pour traiter chaque table comme un dataset distinct.

Voir [Regroupement de séries temporelles](/fr/time-series) pour les motifs pris en charge, le regroupement de fichiers, l'évolution de schéma et les règles anti-faux positifs.

## Schémas multiples

```yaml
add:
  - database: postgresql://localhost/mydb
    schema: [public, sales, hr]
```

## Tunnel SSH

Pour les bases de données derrière un pare-feu. Nécessite `pip install datannurpy[ssh]`.

```yaml
add:
  - database: mysql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: ssh.example.com
      user: sshuser
```

Avec davantage d'options :

```yaml
add:
  - database: postgresql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: bastion.example.com
      port: 2222
      user: admin
      key_file: ~/.ssh/id_rsa
```

## SQLite / GeoPackage distant

Les fichiers SQLite et GeoPackage peuvent être scannés via SFTP ou du stockage cloud. Le dict `storage_options` est passé à [fsspec](https://filesystem-spec.readthedocs.io/).

```yaml
add:
  - database: sftp://host/path/to/db.sqlite
    storage_options:
      key_filename: /path/to/key

  - database: s3://bucket/geodata.gpkg
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

Voir [Stockage distant](/fr/scanning-files#remote-storage) pour la liste des fournisseurs pris en charge et les extras requis.

## Enrichissement des métadonnées de base de données

Nécessite [`depth: variable` ou supérieur](/fr/scan-depth).

| Métadonnées             | Champ cible           | Backends           |
| ----------------------- | --------------------- | ------------------ |
| Clés primaires          | `Variable.key`        | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Clés étrangères         | `Variable.fk_variable_id`  | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Commentaires de tables/colonnes | `description`         | PostgreSQL, MySQL, Oracle, SQL Server, DuckDB |
| NOT NULL, UNIQUE, INDEX | Tags automatiques (`db---*`)  | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Auto-incrément          | Tag automatique       | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |

Ces métadonnées sont toujours rafraîchies, même quand les données des tables sont inchangées (cache hit).
