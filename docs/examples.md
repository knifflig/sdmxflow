# Examples

This page provides runnable end-to-end examples (live network) that produce the full artifact layout and are suitable for copy/paste into scheduled jobs.

## CLI demo (live network)

If you want a runnable, end-to-end example (good for trying `sdmxflow` the first time
or referencing from blog posts / README snippets), use the script in:

- `scripts/download_dataset.py`

This script performs live network I/O against the provider and writes a stable on-disk
artifact layout (dataset CSV + metadata + codelists).

### Run from this repository

```bash
uv sync --group dev
uv run python scripts/download_dataset.py --help
```

Download the default Eurostat example dataset into `./out/estat/lfsa_egai2d`:

```bash
uv run python scripts/download_dataset.py
```

Write a per-run log file under `<out_dir>/logs/` (useful for debugging provider behavior):

```bash
uv run python scripts/download_dataset.py --save-logs
```

Pick an explicit output folder and increase verbosity:

```bash
uv run python scripts/download_dataset.py \
  --out-dir ./out/lfsa_egai2d \
  --log-level DEBUG \
  --save-logs
```

### What to expect

- Console logs at the level you chose via `--log-level`.
- Output artifacts under your `--out-dir` (see Output layout).

Next:

- See [Output Artifacts (Contract)](output-layout.md) for the artifact contract.
- See [Scheduling & Deployment](scheduling-and-deployment.md) for orchestration patterns.
- See [Integration Patterns](integration-patterns.md) for warehouse loading examples.
