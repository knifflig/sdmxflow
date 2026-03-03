# Getting started

## Installation

### From PyPI

Once published:

```bash
uv pip install sdmxflow
```

If you want to manage `sdmxflow` as part of a Python project, you can also add it
to your project's dependencies with `uv`:

```bash
uv add sdmxflow
```

### From source (this repository)

This project uses `uv` for development.

```bash
git clone https://github.com/knifflig/sdmxflow
cd sdmxflow
uv sync --group dev
```

## Documentation site (Zensical)

This repository uses Zensical to render the docs.

Preview locally:

```bash
uv run zensical serve
```

Build the static site:

```bash
uv run zensical build --clean
```

## Basic usage

The main entrypoint is `SdmxDataset`.

```python
from pathlib import Path

from sdmxflow.dataset import SdmxDataset

ds = SdmxDataset(
    out_dir=Path("./out/my_dataset"),
    source_id="ESTAT",
    dataset_id="lfsa_egai2d",
)

result = ds.fetch()
print(result.appended)
```

## Refresh behavior

`fetch()` is designed for scheduled refresh jobs:

1. Query the provider for the dataset's upstream "last updated" timestamp.
2. Compare it to the latest recorded version in `metadata.json`.
3. If unchanged, skip download.
4. If changed, download and append a new slice to `dataset.csv`, then update metadata and codelists.

## Parameters you will commonly use

- `out_dir`: the dataset artifact folder
- `source_id`: currently only `"ESTAT"`
- `dataset_id`: Eurostat dataset id (e.g. `"lfsa_egai2d"`)
- `save_logs=True`: write a per-run debug log file under `<out_dir>/logs/`

For the full parameter list, see [API](api.md).

## Runnable example script

For an end-to-end example you can run immediately (including logging and output
artifacts), see [Examples](examples.md).
