# Output layout

`sdmxflow` writes a stable, warehouse-friendly folder structure under your chosen `out_dir`:

```text
<out_dir>/
  dataset.csv
  metadata.json
  codelists/
    ... reference CSVs ...
  logs/                     # only when save_logs=True
    <agency>__<dataset>__<timestamp>.log
```

## dataset.csv

- Append-only across upstream versions.
- Includes a leading `last_updated` column (UTC ISO-8601) indicating which upstream version each row belongs to.

This makes it easy to:

- reprocess from scratch (read the full file), or
- process incrementally (filter by `last_updated`).

## metadata.json

`metadata.json` stores operational metadata and version history, such as:

- upstream timestamps,
- fetch times,
- URL / HTTP status (when available),
- number of appended rows per version.

The schema is owned by `sdmxflow` and is intended to be compact and stable.

## codelists/

The `codelists/` directory contains exported reference data needed to interpret coded dataset columns (dimensions/attributes).

Codelists tend to change less frequently than facts, but `fetch()` ensures you have the required reference tables alongside the dataset.
