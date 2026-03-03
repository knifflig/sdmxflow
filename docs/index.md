# sdmxflow

Version: **0.1.0**

`sdmxflow` downloads SDMX datasets into a reproducible, append-only on-disk layout designed for data warehouse and periodic refresh workflows.

## Status and provider support

- Status: early but functional
- Provider support: **Eurostat** (`source_id="ESTAT"`)

## What you get

Given an output folder, `sdmxflow` writes:

- `dataset.csv` — append-only facts table
- `metadata.json` — version history and operational metadata
- `codelists/` — exported reference tables needed to interpret coded columns

## Quickstart

```python
from pathlib import Path

from sdmxflow.dataset import SdmxDataset

ds = SdmxDataset(
    out_dir=Path("./out/lfsa_egai2d"),
    source_id="ESTAT",
    dataset_id="lfsa_egai2d",
    # Optional:
    # agency_id="ESTAT",
    # key=None,
    # params={...},
    save_logs=True,  # writes <out_dir>/logs/<agency>__<dataset>__<timestamp>.log
)

result = ds.fetch()
print("Appended new version:", result.appended)
print("Dataset CSV:", result.dataset_csv)
print("Metadata JSON:", result.metadata_json)
print("Codelists dir:", result.codelists_dir)
```

Next:

- See [Getting started](getting-started.md) for installation and usage patterns.
- See [Output layout](output-layout.md) for the artifact contract.
- See [Logging](logging.md) for the INFO-level contract and per-run log files.
