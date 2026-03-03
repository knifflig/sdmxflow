# Integration Patterns

This page shows practical “fetch → load → model” patterns for common warehouse tools, including a small end-to-end example that joins fact rows to codelists.

## Warehouse loading basics

`sdmxflow` produces files; it does not load into a warehouse directly.

Typical flow:

1. Run `fetch()` into a durable `out_dir`.
2. Load `<out_dir>/dataset.csv` into a staging table.
3. Load `<out_dir>/codelists/*.csv` into reference tables.
4. Model “latest version” using the `last_updated` column.

See [Output Artifacts (Contract)](output-layout.md) for file semantics.

## Postgres (COPY)

```sql
-- Create a wide staging table that matches the CSV header.
-- (The provider columns vary by dataset, so define this based on the header.)

CREATE TABLE IF NOT EXISTS sdmx_stage_lfsa_egai2d (
  last_updated timestamptz NOT NULL,
  -- provider columns...
  -- geo text,
  -- sex text,
  -- time_period text,
  -- value numeric,
  raw_line text
);

-- COPY requires the server to see the file. In many deployments you will use
-- \copy from psql (client-side) or a file ingestion service.
```

Client-side load (psql `\copy`):

```bash
psql "$DATABASE_URL" -c "\\copy sdmx_stage_lfsa_egai2d FROM '/data/lfsa_egai2d/dataset.csv' WITH (FORMAT csv, HEADER true)"
```

> **Note**
> If you re-load the full append-only file each run, your staging table will accumulate duplicates. Consider loading incrementally by filtering new `last_updated` values (or truncate+reload staging, then build downstream models for “latest”).

## DuckDB (read_csv_auto)

DuckDB is convenient for local validation and “lightweight warehouse” pipelines:

```python
from pathlib import Path

import duckdb

out_dir = Path("./out/lfsa_egai2d")
con = duckdb.connect()

con.execute(
    """
    CREATE OR REPLACE TABLE facts AS
    SELECT *
    FROM read_csv_auto(?, header=true)
    """,
    [str(out_dir / "dataset.csv")],
)

# Example: select the newest upstream version present in the file.
latest = con.execute("SELECT max(last_updated) FROM facts").fetchone()[0]
print("latest last_updated:", latest)
```

## BigQuery / Snowflake / Redshift (generic pattern)

For managed warehouses:

- stage the file in object storage (GCS/S3/Azure Blob)
- load into a staging table
- either:
  - model latest version in SQL (`QUALIFY`/window function), or
  - snapshot by `last_updated`

The key operational decision is whether your staging table is:

- **truncate+reload** (simple, robust), or
- **incremental append** (more efficient; requires tracking `last_updated` values already loaded)

## dbt pattern (staging + latest view)

A common dbt approach:

- define `dataset.csv` as an external source (or load it to a raw table)
- create a staging model that picks the latest version

Pseudo-SQL for “latest”:

```sql
WITH ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY /* your natural key columns here */
      ORDER BY last_updated DESC
    ) AS rn
  FROM {{ source('sdmx', 'facts') }}
)
SELECT *
FROM ranked
WHERE rn = 1
```

## End-to-end example: fetch → join to a codelist

This example is intentionally generic: it demonstrates the mechanics without assuming a specific dataset schema.

```python
from __future__ import annotations

from pathlib import Path

import duckdb

from sdmxflow import SdmxDataset

out_dir = Path("./out/example")

# 1) Fetch artifacts.
ds = SdmxDataset(out_dir=out_dir, source_id="ESTAT", dataset_id="lfsa_egai2d")
ds.fetch()

# 2) Load facts.
con = duckdb.connect()
con.execute(
    "CREATE OR REPLACE TABLE facts AS SELECT * FROM read_csv_auto(?, header=true)",
    [str(out_dir / "dataset.csv")],
)

# 3) Load one codelist CSV (pick one that exists).
# In a real pipeline you would iterate `out_dir/codelists/*.csv`.
codelist_path = next((out_dir / "codelists").glob("*.csv"))
con.execute(
    "CREATE OR REPLACE TABLE cl AS SELECT * FROM read_csv_auto(?, header=true)",
    [str(codelist_path)],
)

print("codelist file:", codelist_path.name)
print(con.execute("SELECT * FROM cl LIMIT 5").fetchall())

# 4) Join strategy: join facts.<dimension_column> = cl.code
# The exact dimension column depends on your dataset and the codelist.
```

To make joins deterministic in production, use the column → codelist mapping stored in `metadata.json` (see [Output Artifacts (Contract)](output-layout.md)).

Next:

- See [Scheduling & Deployment](scheduling-and-deployment.md) for orchestration patterns.
- See [Provider Support](provider-support.md) for provider semantics and differences.
