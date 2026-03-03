# Contributing to sdmxflow

Thanks for considering a contribution!

## Quick start (development)

This project uses `uv`.

```bash
uv sync --group dev
```

Run the tests:

```bash
uv run pytest
```

Run lint / formatting:

```bash
uv run ruff check .
uv run ruff format .
```

Preview docs locally:

```bash
uv run zensical serve
```

## What to work on

Good contributions for a small, early-stage project:

- Documentation improvements (examples, clarifications, troubleshooting)
- Better metadata capture / validation
- Codelist export improvements
- New SDMX provider support behind a small, testable interface

If you plan a larger change, please open an issue first so we can align on scope.

## Pull requests

- Keep changes focused and small.
- Add/adjust tests when behavior changes.
- Ensure `pytest` and `ruff` pass.
- Prefer clear APIs and predictable output artifacts over cleverness.

## Reporting bugs

Please open an issue with:

- What you expected vs. what happened
- Your OS + Python version
- A minimal snippet to reproduce (or a dataset id and provider)
- The relevant log output (redact secrets if any)
