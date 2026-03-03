# Examples

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

- See [Output layout](output-layout.md) for the artifact contract.
- See [Logging](logging.md) for the library's INFO-level logging contract.
