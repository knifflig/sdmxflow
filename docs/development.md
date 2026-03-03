# Development

## Setup

This project uses `uv` for development.

```bash
uv sync --group dev
```

## Tests

```bash
uv run pytest
```

## Lint / format

```bash
uv run ruff check .
uv run ruff format .
```

## Build / preview docs locally

Install docs dependencies (included in the dev group) and run:

```bash
uv run zensical serve
```

Zensical will print a local URL (usually `http://localhost:8000/`).

## Upgrade Zensical

Zensical is a dev dependency. To upgrade it with `uv`:

1. Update the `zensical` version spec in `pyproject.toml` (both the
	`[project.optional-dependencies].dev` and `[dependency-groups].dev` lists are
	currently present in this project).
2. Re-sync the environment:

```bash
uv sync --group dev
```

To show the currently installed version:

```bash
uv pip show zensical
```

## Release to PyPI

Publishing to PyPI is automated via GitHub Actions on version tags matching
`v*` (e.g. `v0.1.0`). The workflow validates that the tag version matches
`pyproject.toml` before building and publishing.

Prerequisite: configure PyPI "Trusted Publishing" (OIDC) for the GitHub
repository so the `pypa/gh-action-pypi-publish` step can upload without storing
an API token in GitHub secrets.
