# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Canonical links:

- GitHub releases: https://github.com/knifflig/sdmxflow/releases

## [Unreleased]

## [0.1.1] - 2026-03-03

### Fixed

- Relaxed the `sdmx1` dependency upper bound to avoid a transitive `packaging>=26` requirement introduced in `sdmx1==2.25.1`, which can make `sdmxflow` incompatible with orchestration stacks that currently pin `packaging<25.1` (e.g. Prefect 3.6.x).

## [0.1.0] - 2026-03-03

Initial public release.

### Added

- High-level, user-facing API via `SdmxDataset`:
  - Configure a dataset (`source_id`, `dataset_id`, optional `agency_id`, `key`, `params`) and an output folder (`out_dir`).
  - Run `fetch()` for a single refresh cycle.
- Append-only dataset materialization:
  - Writes/maintains a single `dataset.csv` under `out_dir`.
  - Ensures a leading `last_updated` column (UTC ISO-8601) so each row is tied to the upstream version it belongs to.
- Deterministic on-disk artifact layout designed for scheduled warehouse refresh jobs:
  - `dataset.csv` (facts)
  - `metadata.json` (operational metadata + version history)
  - `codelists/` (reference tables used to interpret coded columns)
- Provider support:
  - Eurostat (`source_id="ESTAT"`) as the initial supported source.
- Fast upstream change detection for Eurostat:
  - Queries a last-updated timestamp and uses it to decide whether a new version needs downloading.
- User-friendly logging contract for dataset refresh:
  - `fetch()` emits exactly three `INFO` messages per call (intent → version decision → completion summary).
  - All other detail is emitted at `DEBUG` to keep production logs readable.
- Optional per-run log capture:
  - `save_logs=True` writes a dedicated log file under `<out_dir>/logs/` for each `fetch()` run.
  - File handler is attached/detached safely for the duration of the call.
- Robust download plumbing:
  - Native SDMX REST downloader that streams responses to disk and writes a small `.meta.json` sidecar describing the HTTP response.
  - Typed exceptions for common failure modes (timeouts, unreachable server, user interrupts, metadata errors).
- Project documentation:
  - User-facing docs in `docs/` rendered with Zensical.
  - GitHub Pages workflow to build and publish the site.
- Packaging and quality gates:
  - `pyproject.toml` with complete PEP 621 metadata for publishing.
  - Ruff configured and repository kept lint/format clean.
  - Pytest suite covering core behaviors and edge cases.

### Changed

- Established Apache License 2.0 licensing for `sdmxflow`.
- Added explicit credits/acknowledgements for upstream inspirations and dependencies, including SDMX tooling and documentation tooling.

### Known limitations

- Provider coverage is intentionally narrow in 0.1.0: only Eurostat is supported.
- `fetch()` is optimized for “refresh into stable artifacts” workflows, not for interactive SDMX exploration.
- Output format is focused on CSV + JSON sidecar metadata; additional export formats may be added in future versions.

[0.1.0]: https://github.com/knifflig/sdmxflow/releases/tag/v0.1.0
[0.1.1]: https://github.com/knifflig/sdmxflow/releases/tag/v0.1.1
