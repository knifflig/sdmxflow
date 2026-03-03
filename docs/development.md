# Development & Contributing

This page explains how to work on `sdmxflow` locally: dev environment, tests/lint, docs preview, and how to add provider support in a disciplined way.

## Local dev setup

This project uses `uv` for development.

```bash
uv sync --group dev
```

## Run tests

```bash
uv run pytest
```

## Lint and format

```bash
uv run ruff check .
uv run ruff format .
```

## Docs preview (Zensical)

Preview locally:

```bash
uv run zensical serve
```

Build the static site:

```bash
uv run zensical build --clean
```

## Repository structure (high level)

- `sdmxflow/dataset.py`: user-facing `SdmxDataset` entrypoint and refresh workflow
- `sdmxflow/download/`: provider download implementations
- `sdmxflow/query/`: upstream metadata queries (e.g., “last updated”)
- `sdmxflow/metadata/`: metadata schema + read/write helpers
- `sdmxflow/extract/`: structure/codelist extraction
- `tests/`: unit tests (provider branches, error classification, contracts)

## How to add a provider (implementation outline)

Provider support should preserve the stable artifact contract:

- `dataset.csv` with leading `last_updated`
- `metadata.json` with append-only version history
- `codelists/` extraction and mapping

Acceptance criteria for a new provider:

1. **Change detection**

   - Implement an upstream “last updated” resolver for the provider.
   - Add unit tests for changed vs unchanged behavior.
2. **Download and materialization**

   - Implement a downloader that writes a provider slice to a CSV without the internal `last_updated` column.
   - Ensure `append_version_slice()` can tag it and append deterministically.
3. **Structures and codelists**

   - Ensure structures can be downloaded and codelists can be mapped from dataset columns.
4. **Error classification**

   - Timeouts, unreachable errors, and interruptions should raise typed `sdmxflow` errors.
5. **Docs update**

   - Add the provider to [Provider Support](provider-support.md) and describe any provider-specific caveats.

## PR expectations

- Keep changes focused and additive.
- Update docs when behavior changes.
- Add tests for new branches and error cases.

See the repository contributing guide for details:

- https://github.com/knifflig/sdmxflow/blob/main/CONTRIBUTING.md

## Release notes (PyPI)

Publishing to PyPI is automated via GitHub Actions on version tags matching `v*` (e.g. `v0.1.1`).

The publish workflow validates that the tag version matches `pyproject.toml` before building and publishing.

Prerequisite: configure PyPI “Trusted Publishing” (OIDC) for the GitHub repository so the publish step can upload without storing an API token in GitHub secrets.
