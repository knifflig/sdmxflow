
# sdmxflow

Download SDMX datasets into a reproducible, append-only on-disk layout for data warehouse and periodic refresh workflows.

`sdmxflow` is designed for the common “ELT input dataset” pattern:

- pull a dataset from an SDMX provider,
- store it locally in a stable folder structure,
- refresh it periodically,
- keep a minimal but useful metadata trail (versions, timestamps, URLs, status, row counts),
- export the reference data (codelists) required to interpret coded columns.

> Status: early but functional. Current provider support is **Eurostat** (`source_id="ESTAT"`).

---

## Why sdmxflow

Many SDMX ingestion solutions focus on “get me data” (often very flexibly), but stop short of the metadata needed for downstream analytics and governance:

- dataset versioning (what changed upstream and when),
- artifact locations and repeatability,
- codelists/reference data exported alongside the facts.

There are also community solutions (for example a dlt extension shared by Martin Salo) that are great for flexible extraction, and this project started from that direction. `sdmxflow` builds on those ideas but focuses more strongly on a warehouse-friendly artifact layout and richer metadata + codelist outputs.

`sdmxflow` aims to be a pragmatic building block for warehouse pipelines: straightforward API, deterministic output layout, and predictable refresh behavior.

Where we come from:

- Early prototyping and the “bring SDMX into warehouse refresh workflows” motivation was influenced by Martin Salo’s SDMX `dlt` extension gist.
- The heavy lifting for SDMX protocol/model parsing is powered by the `sdmx1` Python package.

---

## Features

- **Append-only refresh**: only downloads and appends when upstream changed.
- **Warehouse-friendly layout**:
	- `dataset.csv` (facts)
	- `metadata.json` (versions + fetch info)
	- `codelists/` (reference tables)
- **Fast upstream change detection** (Eurostat): uses SDMX annotations for last-updated.
- **User-friendly logging** at `INFO` and detailed diagnostics at `DEBUG`.
- Optional per-run log file capture via `save_logs=True`.

Non-goals (for now):

- full multi-provider support,
- a full-blown orchestration framework,
- a “do everything” SDMX exploration UI.

---

## Installation

### From PyPI (recommended)

Once published:

```bash
pip install sdmxflow
```

### From source (this repository)

This project uses `uv` for development.

```bash
git clone https://github.com/knifflig/sdmxflow
cd sdmxflow
uv sync --group dev
```

---

## Quickstart

The primary entrypoint is `SdmxDataset`.

```python
from pathlib import Path

from sdmxflow.dataset import SdmxDataset

ds = SdmxDataset(
	out_dir=Path("./out/lfsa_egai2d"),
	source_id="ESTAT",
	dataset_id="lfsa_egai2d",
	# Optional:
	# agency_id="ESTAT",
	# key=...,        # provider-specific key restriction
	# params={...},   # provider-specific passthrough params
	save_logs=True,  # writes <out_dir>/logs/<agency>__<dataset>__<timestamp>.log
)

result = ds.fetch()
print("Appended new version:", result.appended)
print("Dataset CSV:", result.dataset_csv)
print("Metadata JSON:", result.metadata_json)
print("Codelists dir:", result.codelists_dir)
```

### What `fetch()` does

`fetch()` is designed for scheduled refresh jobs:

1. Fetch upstream “last updated” timestamp.
2. Compare with the latest locally recorded timestamp in `metadata.json`.
3. If unchanged: do nothing to the dataset (but still ensures metadata + codelists).
4. If changed: download and append a new slice to `dataset.csv`, then update metadata + codelists.

---

## Output layout

`sdmxflow` writes a stable folder structure under your chosen `out_dir`:

```text
<out_dir>/
	dataset.csv
	metadata.json
	codelists/
		... generated reference CSVs ...
	logs/                     # only when save_logs=True
		<agency>__<dataset>__<timestamp>.log
```

### `dataset.csv`

- Append-only across versions.
- Includes a leading `last_updated` column (UTC ISO-8601) indicating which upstream version a row belongs to.

### `metadata.json`

Stores dataset identity and version history, such as:

- upstream timestamps,
- fetch times,
- HTTP URL/status/headers (when available),
- number of rows appended for each version.

### `codelists/`

Contains exported codelists needed to interpret coded dataset columns.

---

## Logging

`sdmxflow` is built to be readable in production logs.

- At `INFO` level, `fetch()` emits exactly three user-facing messages:
	1. intention (what, where),
	2. version decision (download vs. already up to date),
	3. completion summary (artifact paths).
- Enable `DEBUG` for rich diagnostics.
- If you pass `save_logs=True`, `sdmxflow` writes a per-run debug log file under `<out_dir>/logs/`.

---

## Integrating into warehouse workflows

Typical patterns:

- **Airflow / Dagster / Prefect task**: call `fetch()` on a schedule; downstream tasks ingest `dataset.csv` into your warehouse.
- **dbt sources**: load `dataset.csv` into a staging table and build models on top.
- **Lakehouse**: treat `<out_dir>` as a partitioned artifact folder; `metadata.json` provides lineage.

Because the dataset is append-only, you can:

- reprocess from scratch (read the full file), or
- incrementally process “new versions” by filtering on `last_updated`.

---

## Provider support and limitations

- Supported:
	- Eurostat (`source_id="ESTAT"`)

Planned/possible future work (not guaranteed):

- additional SDMX sources,
- richer metadata capture (more SDMX structure fields),
- export formats beyond CSV/JSON.

---

## Development

Install dev dependencies:

```bash
uv sync --group dev
```

Run tests:

```bash
uv run pytest
```

Run lint/format:

```bash
uv run ruff check .
uv run ruff format .
```

---

## Contributing

Contributions are welcome.

Good first contributions:

- improvements to metadata extraction,
- better codelist export coverage,
- adding new provider support behind a clean interface,
- documentation and examples.

Please open an issue before large changes.

---

## Contact

- Henry Zehe: https://github.com/knifflig

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE.md](LICENSE.md).

---

## Credits and acknowledgements

- **Martin Salo** (https://github.com/salomartin) and the SDMX `dlt` extension gist that helped inform early direction and requirements:
	https://gist.github.com/salomartin/d4ee7170f678b0b44554af46fe8efb3f
- **`sdmx1`** (https://github.com/khaeru/sdmx/) and its maintainers/contributors: `sdmxflow` relies on `sdmx1` for core SDMX handling.
- **Zensical** (https://zensical.org/, https://github.com/zensical/zensical) is used to build and publish this documentation site. Zensical is licensed under the MIT License.

