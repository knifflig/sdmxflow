# API

This documentation covers the user-facing API shipped in version **0.1.0**.

## `SdmxDataset`

Create a downloader bound to one dataset and one output folder.

Constructor parameters:

- `out_dir` (required): root folder for artifacts
- `source_id` (required): provider id (currently only `"ESTAT"`)
- `dataset_id` (required): dataset id within the provider
- `agency_id` (optional): defaults to `source_id` for Eurostat
- `key` (optional): provider-specific SDMX key restriction (string or mapping)
- `params` (optional): provider-specific passthrough query parameters
- `logger` (optional): custom `logging.Logger`
- `save_logs` (optional): write a per-run log file under `<out_dir>/logs/`

Common usage:

```python
from sdmxflow.dataset import SdmxDataset

ds = SdmxDataset(out_dir="./out", source_id="ESTAT", dataset_id="lfsa_egai2d")
result = ds.fetch()
```

### `fetch()`

Runs a single refresh cycle:

- checks upstream "last updated",
- compares to local metadata,
- downloads/appends if changed,
- updates metadata and codelists.

Returns a `FetchResult`.

## `FetchResult`

`FetchResult` is a small value object that points to written artifacts:

- `out_dir`
- `dataset_csv`
- `metadata_json`
- `codelists_dir`
- `appended` (bool)
