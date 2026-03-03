# Configuration Reference

This page documents the current user-facing configuration surface: `SdmxDataset` parameters, defaults, outputs, logging, and error behavior.

## `SdmxDataset`

`SdmxDataset` is the primary entrypoint.

### Constructor

```python
from pathlib import Path

from sdmxflow import SdmxDataset

ds = SdmxDataset(

    out_dir=Path("./out/my_dataset"),
    source_id="ESTAT",
    dataset_id="lfsa_egai2d",
)
```

Parameters:

- `out_dir` (required, `str | Path`): output directory for all artifacts. It is expanded and resolved.
- `source_id` (required, `str`): provider/source identifier.
- `dataset_id` (required, `str`): dataset identifier within the provider.

Optional parameters:

- `agency_id` (`str | None`, default: `None`)

  - For `ESTAT`, `agency_id` defaults to the upper-cased `source_id`.
- `key` (`str | dict[str, object] | None`, default: `""`)

  - Provider-specific dataset restriction.
  - For `ESTAT`, use `None` to request the full dataset. The empty string `""` means “provider default” (which is currently also the full dataset).
  - Dict keys are supported for Eurostat bulk downloads and are converted deterministically to a SDMX key string.
- `params` (`dict[str, object] | None`, default: `None`)

  - Provider-specific passthrough parameters.
  - For Eurostat bulk CSV downloads, `sdmxflow` recognizes common SDMX 3.0 bulk params such as time-window filters.
- `logger` (`logging.Logger | None`, default: `None`)

  - If omitted, `sdmxflow` uses a default library logger.
- `save_logs` (`bool`, default: `False`)

  - If `True`, writes a per-run log file under `<out_dir>/logs/`.

### Output paths

`sdmxflow` uses fixed names under `out_dir`:

- `dataset.csv`
- `metadata.json`
- `codelists/`

See [Output Artifacts (Contract)](output-layout.md).

### `setup()`

Creates the output directory structure (safe to call multiple times):

- `<out_dir>/`
- `<out_dir>/codelists/`

`fetch()` calls `setup()` internally.

### `fetch()`

Runs one refresh cycle:

1. Ensures local directories exist.
2. Resolves the upstream “last updated” timestamp.
3. Loads or initializes `metadata.json`.
4. Appends a new slice only if upstream changed.
5. Ensures codelists and updates metadata.

Return value:

- a `FetchResult` pointing at artifact paths and a boolean `appended`.

#### Refresh semantics

- On “no change”: `dataset.csv` is not modified, but `metadata.json` is updated (e.g., `last_fetched_at`) and codelists are ensured.
- On “changed”: a new slice is downloaded and appended to `dataset.csv` with a leading `last_updated` column value.

## `FetchResult`

`FetchResult` fields:

- `out_dir`: output directory used for this fetch
- `dataset_csv`: path to the facts CSV (`<out_dir>/dataset.csv`)
- `metadata_json`: path to metadata (`<out_dir>/metadata.json`)
- `codelists_dir`: path to codelists directory (`<out_dir>/codelists/`)
- `appended`: whether a new upstream version was appended

Example:

```python
result = ds.fetch()
if result.appended:
    # trigger a warehouse load step
    pass
```

## Logging configuration

`sdmxflow` uses Python’s standard `logging` module and does **not** configure handlers.

Minimal configuration:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

At `INFO` level, each `fetch()` emits exactly **three** user-facing messages:

1. intention (what will be fetched and where)
2. decision (download vs already up to date)
3. completion summary (paths to artifacts)

For detailed diagnostics:

```python
logging.basicConfig(level=logging.DEBUG)
```

For per-run capture:

- `save_logs=True` writes `<out_dir>/logs/<agency>__<dataset>__<timestamp>.log`.

See [Logging](logging.md).

## Timeouts, retries, and network settings

Current behavior:

- `fetch()` does not expose a top-level `timeout_seconds` or retry policy.
- Some lower-level download components support timeouts internally, but they are not part of the stable public API yet.

Operational guidance:

- implement retries/backoff in your scheduler (Airflow/Prefect/Kubernetes)
- use `save_logs=True` to capture diagnostics

## Errors and exceptions

`fetch()` raises typed `sdmxflow` errors for common operational failure modes:

- `SdmxDownloadError`: unsupported provider or a download failed
- `SdmxTimeoutError`: upstream request timed out
- `SdmxUnreachableError`: DNS/connection failures
- `SdmxInterruptedError`: user interruption (Ctrl+C)

Troubleshooting guidance is in [FAQ & Troubleshooting](faq.md).
