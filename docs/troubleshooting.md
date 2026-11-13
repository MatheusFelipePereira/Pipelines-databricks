# 🧯 Troubleshooting log

Real errors found while building the pipelines, and how they were fixed.

## 1. `CLOUD_INVALID_PATH` on the Bronze layer

```text
Category: Error
Message: The specified path contains invalid characters and cannot be resolved.
Path: workspace.default.marketing_vendas_10000_linhas.
Reason: Path must be absolute: workspace.default.marketing_vendas_10000_linhas
Error class: CLOUD_INVALID_PATH
SQL state: 42K05
Table: workspace.default.marketing_bronze
```

Followed by cascading warnings on Silver and Gold:

```text
UnresolvedDatasetException: Failed to read dataset 'workspace.default.marketing_bronze'.
Dataset is defined in the pipeline but could not be resolved.
```

### Root cause

The Bronze layer used a **file reader** (`spark.read.format("csv").load(...)`) but received a **table name** (`catalog.schema.table`). File readers need an absolute path such as `/Volumes/<catalog>/<schema>/<volume>/file.csv`.

The Silver and Gold errors were only **consequences** — they depend on Bronze.

### Fix

```python
# ❌ before — file reader with a table name
spark.read.format("csv").option("header", "true").load("workspace.default.marketing_vendas_10000_linhas")

# ✅ after — table reader
spark.read.table("workspace.default.marketing_vendas_10000_linhas")
```

| Reader | Reads | Needs |
|---|---|---|
| `spark.read.csv()` / `.load()` | Physical files | Absolute path (`/Volumes/...`) |
| `spark.read.table()` | Registered tables | `catalog.schema.table` |

Validated before re-running:

```sql
SELECT * FROM workspace.default.marketing_vendas_10000_linhas LIMIT 10;
```

✅ Pipeline re-run → **success**.

## 2. Incremental test: don't replace the source

To test incremental ingestion, the 10k CSV must **not** be replaced by a 12k file. New rows are uploaded to a separate table and **appended** with `INSERT INTO ... BY NAME` (see [`sql/04_incremental_insert.sql`](../sql/04_incremental_insert.sql)). Running the INSERT twice would duplicate the 2,000 rows.

## 3. `dropDuplicates()` in streaming

Global `dropDuplicates()` was removed from the incremental Silver layer: deduplication over an unbounded stream needs a different approach (watermark / key-based dedup), planned as a next step.
