# Logging

This page documents how `sdmxflow` logs in production and how to capture debug logs for troubleshooting.

`sdmxflow` uses the standard library `logging` module and **does not configure handlers**.

## INFO-level contract

At `INFO` level, `SdmxDataset.fetch()` emits exactly three user-facing messages per call:

1. Fetch intention (what will be fetched and where)
2. Version decision (download vs already up to date)
3. Completion summary (artifact paths)

All other detail is emitted at `DEBUG`.

## Enabling logs

In applications, configure logging once at process startup:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

For troubleshooting:

```python
logging.basicConfig(level=logging.DEBUG)
```

## Per-run log files

If you pass `save_logs=True` to `SdmxDataset`, a per-run log file is written under:

- `<out_dir>/logs/<agency>__<dataset>__<timestamp>.log`

The file handler is attached only for the duration of `fetch()` and is detached/closed in a `finally` block.

Next:

- [Configuration Reference](api.md)
- [FAQ & Troubleshooting](faq.md)
