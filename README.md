# sdmxflow

[![PyPI](https://img.shields.io/pypi/v/sdmxflow.svg)](https://pypi.org/project/sdmxflow/)
[![Python versions](https://img.shields.io/pypi/pyversions/sdmxflow.svg)](https://pypi.org/project/sdmxflow/)
[![License](https://img.shields.io/pypi/l/sdmxflow.svg)](LICENSE.md)
[![CI](https://github.com/knifflig/sdmxflow/actions/workflows/ci.yml/badge.svg)](https://github.com/knifflig/sdmxflow/actions/workflows/ci.yml)

`sdmxflow` turns SDMX datasets (Eurostat today) into deterministic, append-only warehouse refresh artifacts: facts CSV + versioned metadata trail + exported codelists.

**Problem:** SDMX is easy to query, but harder to operationalize for warehouses (repeatable artifacts, refresh semantics, reference data, governance).

**Solution:** `sdmxflow` fetches a dataset and writes a stable on-disk layout that you can load into your warehouse on a schedule.

**Proof:** Eurostat is supported now (`source_id="ESTAT"`), with append-only refresh and last-updated change detection.

> [!NOTE]
> **Status:** early but functional
> **Supported providers:** **Eurostat** (`source_id="ESTAT"`)
> **Docs:** https://knifflig.github.io/sdmxflow/

`sdmxflow` is designed for the common “ELT input dataset” pattern:

- pull a dataset from an SDMX provider,
- store it locally in a stable folder structure,
- refresh it periodically,
- keep a minimal but useful metadata trail (versions, timestamps, URLs, status, row counts),
- export the reference data (codelists) required to interpret coded columns.

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

# `result` contains paths to the artifacts that were created/updated:
# - result.dataset_csv
# - result.metadata_json
# - result.codelists_dir
```

### What you get on disk

```text
<out_dir>/
	dataset.csv          # append-only facts across versions
	metadata.json        # version history + fetch metadata
	codelists/           # exported reference tables
	logs/                # only when save_logs=True
		<agency>__<dataset>__<timestamp>.log
```

---

## Integrations (Airflow/dbt style)

The intended workflow is: fetch artifacts → load into your warehouse → model downstream.

Example (Airflow task pseudocode):

```python
from pathlib import Path

from sdmxflow.dataset import SdmxDataset


def refresh_eurostat_lfsa_egai2d() -> None:
	ds = SdmxDataset(
		out_dir=Path("/data/sdmx/lfsa_egai2d"),
		source_id="ESTAT",
		dataset_id="lfsa_egai2d",
	)
	ds.fetch()
```

Then:

- load `<out_dir>/dataset.csv` into a staging table,
- define it as a dbt source,
- build models on top; select the newest version via the `last_updated` column.

---

## How refresh works

`fetch()` is designed for scheduled refresh jobs:

1. Fetch upstream “last updated” timestamp.
2. Compare with the latest locally recorded timestamp in `metadata.json`.
3. If unchanged: do nothing to the dataset (but still ensures metadata + codelists).
4. If changed: download and append a new slice to `dataset.csv`, then update metadata + codelists.

---

## Use cases

- Refresh Eurostat indicators nightly into Postgres/Snowflake/BigQuery staging.
- Keep reference codelists versioned alongside fact extracts for governance.
- Produce reproducible ELT inputs (facts + metadata + reference tables) for analysts.

---

## Why sdmxflow

`sdmxflow` is intentionally opinionated about *operationalizing* SDMX datasets for warehouse refresh jobs.

- Compared to SDMX client libraries: they fetch data; `sdmxflow` produces deterministic refresh artifacts + metadata trail + codelists.
- Compared to flexible extractors: `sdmxflow` focuses on stable layout and predictable refresh semantics.

See “Credits and acknowledgements” below for project influences and dependencies.

---

## Features

- **Append-only refresh**: only downloads and appends when upstream changed.
- **Warehouse-friendly layout**: `dataset.csv` (facts), `metadata.json` (versions + fetch info), `codelists/` (reference tables).
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

## Provider support and limitations

- Supported:
	- Eurostat (`source_id="ESTAT"`)

Planned/possible future work (not guaranteed):

- additional SDMX sources,
- richer metadata capture (more SDMX structure fields),
- export formats beyond CSV/JSON.

---

## FAQ

**Does `sdmxflow` load into my warehouse directly?**

No. It produces deterministic on-disk artifacts (CSV/JSON/codelists). You load them using your existing tooling (Airflow, dbt, COPY/LOAD jobs, etc.).

**Does it support providers besides Eurostat?**

Not yet. Eurostat (`source_id="ESTAT"`) is the current supported provider.

**Does it deduplicate data?**

It is append-only across upstream versions. Each appended slice is marked with a `last_updated` value so downstream jobs can select the newest version (or reprocess full history).

**How does it detect upstream changes?**

For Eurostat, it uses SDMX annotations to obtain a last-updated timestamp and compares it to the latest locally recorded timestamp.

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

