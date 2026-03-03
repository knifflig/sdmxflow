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
