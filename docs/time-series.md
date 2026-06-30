# Time series grouping

datannurpy can group repeated files or database tables into one dataset when their names differ only by a time period. This is enabled by default for both `add_folder()` and `add_database()` with `time_series: true`.

## Supported patterns

The detector recognizes years, quarters, months, and full dates in names or paths:

| Period type | Examples | Stored as | Metadata-first pattern |
| ----------- | -------- | --------- | ---------------------- |
| Year | `sales_2024.csv`, `stats_2024` | `2024` | `[YYYY]` |
| Quarter | `sales_2024Q1.csv`, `stats_2024T2` | `2024Q1`, `2024Q2` | `[YYYY]Q[N]` |
| Month | `sales_2024-03.csv`, `sales_202403` | `2024/03` | `[YYYY/MM]` |
| Date | `sales_2024-03-15.csv`, `sales_20240315` | `2024/03/15` | `[YYYY/MM/DD]` |

Partial periods such as `01`, `02`, `Q1`, or `day15` can be used only when a full 4-digit year is available elsewhere in the same path or table name. A sequence such as `report_01.csv`, `report_02.csv` is kept as separate datasets because it has no year context.

## Files

When scanning folders, simple file datasets with the same format can be grouped:

```text
data/
+-- enquete_2020.csv
+-- enquete_2021.csv
+-- enquete_2022.csv
`-- reference.csv
```

This creates one dataset named `enquete_[YYYY]` with `nb_resources=3`; `reference.csv` remains a separate dataset.

Periods may also appear in folders:

```text
data/
+-- 2024/01/sales.csv
+-- 2024/02/sales.csv
`-- 2024/03/sales.csv
```

This creates one `sales` dataset with periods `2024/01`, `2024/02`, and `2024/03`. Temporal folders are not created as catalog folders for the series; only non-temporal parent folders are kept.

Partitioned datasets such as Delta, Hive, and Iceberg are handled by their own dataset scanners and are not grouped by this time series detector.

## Database Tables

When scanning databases, tables are grouped per schema after `include` and `exclude` filters are applied:

```text
sales_fact_202401
sales_fact_202402
sales_fact_202403
dim_customer
```

This creates one dataset named `sales_fact_[YYYY/MM]` with `nb_resources=3`; `dim_customer` remains a separate dataset.

The same validation rules apply to tables and files. For example, `dim_age_01`, `dim_age_02`, and `dim_age_03` are not grouped because there is no 4-digit year. Mixed granularities are split into distinct datasets, so `data_2021`, `data_2022`, `data_2021Q1`, and `data_2021Q2` become one yearly dataset and one quarterly dataset.

## Resulting Metadata

For a grouped series, the dataset receives:

- `nb_resources`: number of files or tables in the series
- `start_date`: first detected period
- `end_date`: last detected period
- `data_path`: the latest file or table, used as the canonical resource

In metadata-first scans (`create_folders=False`), the technical `_match_path` in `metadata/dataset.csv` can use the normalized series syntax from the [Metadata-first pattern](#supported-patterns) column above — for example `sales_[YYYY].csv` or `sales_[YYYY/MM].csv` — to match the logical series instead of a specific latest file.

At `depth="stat"` or `depth="value"`, statistics and frequency tables are computed from the latest period only. Older periods are scanned in schema-only mode so datannurpy can detect variable availability over time.

Variables also receive `start_date` and `end_date` when their presence changes across periods. A variable present in every period has both fields empty. A variable added after the first period gets `start_date`; a variable removed before the last period gets `end_date`.

## Avoiding False Positives

datannurpy keeps candidates as separate datasets when grouping would be ambiguous or incomplete:

- fewer than two matching files or tables
- no 4-digit year anywhere in the candidate group
- month or day fragments without enough year/month context
- tables or files whose entire name is only a temporal token
- mixed period granularities that should be represented as separate series

Constant temporal-looking tokens, such as a delivery date folder or a fixed numeric prefix, are preserved as literal text when another varying period identifies the series.

## Disable Detection

Set `time_series: false` to treat each file or table independently:

```yaml
add:
  - folder: ./data
    time_series: false

  - database: postgresql://localhost/mydb
    time_series: false
```
