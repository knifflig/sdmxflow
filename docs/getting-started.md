# Getting Started

This page gets you from “install” to a first successful fetch, and explains what changes on the second run (append-only, incremental refresh).

## Installation

### Python versions

`sdmxflow` supports Python **3.11** and **3.12**.

### From PyPI

```bash
pip install sdmxflow
```

If you use `uv`:

```bash
uv add sdmxflow
```

### From source (contributors)

```bash
git clone https://github.com/knifflig/sdmxflow
cd sdmxflow
uv sync --group dev
```

## Minimal working example

The main entrypoint is `SdmxDataset`.

```python
from pathlib import Path

from sdmxflow import SdmxDataset

ds = SdmxDataset(
    out_dir=Path("./out/lfsa_egai2d"),
    source_id="ESTAT",
    dataset_id="lfsa_egai2d",
)

result = ds.fetch()
print("appended:", result.appended)
```

## Parameters (what they mean)

- `out_dir` (required): where artifacts are written and persisted between runs.
- `source_id` (required): the provider/source identifier (currently `"ESTAT"`).
- `dataset_id` (required): dataset identifier within the provider.

Common optional parameters:

- `agency_id`: defaults to `source_id` for `ESTAT`.
- `key`: provider-specific SDMX key restriction.
    - For `ESTAT`, use `None` to request the full dataset; `""` means “provider default” (currently also the full dataset).
- `params`: provider-specific passthrough parameters (e.g., time window).
- `save_logs=True`: writes a per-run debug log file under `<out_dir>/logs/`.

For the full parameter reference and defaults, see [Configuration Reference](api.md).

## What happens on first run vs second run

### First successful run

- No `metadata.json` exists yet, so `sdmxflow` initializes metadata.
- It fetches upstream “last updated” metadata.
- It downloads the dataset slice and creates `dataset.csv`.
- It writes `metadata.json` and exports `codelists/`.

### Second (and later) runs

`sdmxflow` compares the upstream “last updated” timestamp to the latest locally recorded one:

- If unchanged: it **skips the dataset download** and does not append to `dataset.csv`.
- If changed: it **downloads and appends** a new slice and updates metadata/codelists.

> **Important**
> `dataset.csv` is append-only across versions. It is normal for the same “logical row” to appear multiple times across different `last_updated` values.

## Expected output folder tree

After a successful fetch you should see:

```text
<out_dir>/
    dataset.csv
    metadata.json
    codelists/
        <CODELIST_ID>.csv
```

If you enabled per-run log capture:

```text
<out_dir>/logs/
    <agency>__<dataset>__<timestamp>.log
```

Next:

- Read [Output Artifacts (Contract)](output-layout.md) for file semantics and examples.
- See [Scheduling & Deployment](scheduling-and-deployment.md) for production patterns.
- See [Integration Patterns](integration-patterns.md) for warehouse loading examples.
