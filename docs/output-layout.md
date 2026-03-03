# Output Artifacts (Contract)

This page defines the on-disk artifact contract `sdmxflow` produces: file names, semantics, and how to use them in a warehouse ingestion workflow.

> **Artifact Contract (stable)**
> Given an `out_dir`, `sdmxflow` writes:
>
> - `dataset.csv` (append-only facts; first column `last_updated`)
> - `metadata.json` (metadata history + operational fields)
> - `codelists/` (reference tables with `code,name`)
>
> Optional: `logs/` when `save_logs=True`.

## Directory layout

```text
<out_dir>/
    dataset.csv
    metadata.json
    codelists/
        <CODELIST_ID>.csv
    logs/                     # only when save_logs=True
        <agency>__<dataset>__<timestamp>.log
```

## `dataset.csv`

### Schema

`dataset.csv` is the provider dataset schema with one additional leading column:

- `last_updated` (string, UTC ISO-8601, e.g. `2026-03-03T00:00:00Z`)

All remaining columns are provider-defined and depend on the dataset.

> **Important**
> `sdmxflow` does not deduplicate across versions. When a new upstream version is appended, it appends the full downloaded slice tagged with that version’s `last_updated`.

### Example rows

Example (illustrative — provider columns vary):

```csv
last_updated,geo,sex,time_period,value
2026-03-01T00:00:00Z,DE,M,2024-12,123.4
2026-03-01T00:00:00Z,DE,F,2024-12,120.1
2026-02-01T00:00:00Z,DE,M,2024-11,122.9
```

### Append behavior

- On the first successful fetch, `dataset.csv` is created with a header.
- On later fetches, `sdmxflow` appends rows only when the upstream `last_updated` timestamp changed.
- If the provider changes the CSV header (column names or ordering), append fails with a schema/metadata error instead of producing a corrupted mixed-schema history.

### How to use in a warehouse

Common patterns:

- **Truncate + reload staging:** load the full append-only file each run, then model “latest” downstream.
- **Incremental load:** load only rows where `last_updated` is newer than what your warehouse already has.

See [Integration Patterns](integration-patterns.md).

## `metadata.json`

`metadata.json` is the operational and lineage record.

Key semantics:

- `last_fetched_at` updates on every fetch attempt (even if no new data appended).
- `versions[]` is append-only and records one entry per appended upstream version.
- `codelists[]` maps dataset columns to codelist files.

### Example structure (snippet)

The exact file contains more fields; this snippet matches the current schema shape:

```json
{
  "schema_version": 1,
  "agency_id": "ESTAT",
  "dataset_id": "lfsa_egai2d",
  "created_at": "2026-03-03T01:02:03Z",
  "last_fetched_at": "2026-03-03T02:00:00Z",
  "last_updated_at": "2026-03-03T02:00:00Z",
  "last_updated_data_at": "2026-03-01T00:00:00Z",
  "files": {
    "datasets": {"csv": "dataset.csv"},
    "codelists": {
      "sex": "codelists/CL_SEX.csv"
    }
  },
  "versions": [
    {
      "agency_id": "ESTAT",
      "created_at": "2026-03-03T02:00:00Z",
      "last_updated_data_at": "2026-03-01T00:00:00Z",
      "dataset": {"rows_appended": 12345, "last_updated_column": "last_updated"},
      "http": {"url": "https://…", "status_code": 200, "headers": {"etag": "…"}}
    }
  ],
  "codelists": [
    {
      "column_name": "sex",
      "column_pos": 2,
      "codelist_id": "CL_SEX",
      "codelist_path": "codelists/CL_SEX.csv",
      "codelist_kind": "dimension",
      "codelist_type": "reference",
      "codelist_labels": {"en": "Sex"}
    }
  ]
}
```

> **Note**
> Timestamps are written as UTC ISO-8601 `...Z` strings.

## `codelists/`

`codelists/` contains one CSV per used codelist, with a stable two-column schema:

- `code` (string)
- `name` (best-effort label, typically English if available)

Example:

```csv
code,name
M,Male
F,Female
```

### How to join to fact columns

- Identify the fact column (dimension/attribute) you want to decode.
- Load the codelist CSV matching that column (use `metadata.json` → `codelists[]` mapping).
- Join `facts.<column>` = `codelist.code`.

See [Integration Patterns](integration-patterns.md) for examples.

## Logs (`save_logs=True`)

If you enabled per-run log capture, a log file is written under `<out_dir>/logs/`.

See [Logging](logging.md) for the INFO-level logging contract.
