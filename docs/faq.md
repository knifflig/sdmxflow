# FAQ & Troubleshooting

This page lists common operational issues when running `sdmxflow` in scheduled jobs and how to debug them.

## No new data appended

Symptoms:

- `FetchResult.appended` is `False` repeatedly.
- Logs say “Already up to date; skipping download.”

Likely causes:

- The upstream dataset has not changed.
- The provider’s “last updated” signal did not change even though the data changed.

What to do:

- Enable `DEBUG` logging and re-run.
- Inspect `metadata.json` → `last_updated_data_at` and `versions[]`.
- Compare to the provider’s UI/metadata if available.

See [Logging](logging.md) and [Provider Support](provider-support.md).

## Output folder already exists

That’s expected.

- `fetch()` will reuse the same `out_dir`.
- `dataset.csv` is appended only when the upstream version changes.
- `metadata.json` is updated on every run (`last_fetched_at` always bumps).

## Large dataset performance tips

- Prefer running on a machine with fast local disk for `out_dir`.
- Treat `dataset.csv` as a warehouse staging input; avoid repeatedly reading it end-to-end in downstream steps if you can load incrementally.
- Consider splitting “download” and “warehouse load” into separate steps so warehouse loads can be retried without re-downloading.

## Network failures and retries

`sdmxflow` classifies common failures into typed exceptions (timeout/unreachable/interrupted), but it does not implement a global retry policy at the top-level API.

Operational pattern:

- Implement retries in your scheduler (Airflow retries / Prefect retries / Kubernetes Job backoff).
- Use `save_logs=True` to capture per-run debug logs for postmortems.

## How to reset/rebuild artifacts safely

Sometimes the safest response to upstream schema changes or local corruption is to rebuild from scratch.

Recommended approach:

1. Move the existing folder aside:

```bash
mv /data/sdmx/my_dataset /data/sdmx/my_dataset.backup.$(date +%Y%m%d%H%M%S)
```

2. Run `fetch()` again into a clean `out_dir`.

> **Warning**
> Rebuilding means you lose the local append history in `dataset.csv` and `metadata.json` for that dataset folder. Keep backups if you need auditability.

## Common errors

### Unsupported provider

Error:

- `SdmxDownloadError: Unsupported source_id=... Only 'ESTAT' is implemented.`

Fix:

- Use `source_id="ESTAT"` for now.
- See [Provider Support](provider-support.md) for roadmap and contribution path.

### CSV schema mismatch

Error (paraphrased):

- “CSV schema mismatch: source columns differ from destination columns”

Meaning:

- The provider CSV header changed compared to what you previously stored in `dataset.csv`.

Fix:

- Rebuild into a new `out_dir` (see “reset/rebuild” above), or pin the dataset/key/params so the schema is stable.
