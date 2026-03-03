# Scheduling & Deployment

This page shows production-style patterns for running `sdmxflow` on a schedule (cron, Airflow, Prefect) and operational practices for reliable SDMX ingestion.

## The baseline pattern

A job run should look like:

1. Call `SdmxDataset(...).fetch()`
2. Load/update your warehouse using the artifacts (`dataset.csv`, `codelists/`, `metadata.json`)
3. Emit logs/metrics based on `FetchResult.appended`

> **Recommendation**
> Treat `out_dir` as durable state. Put it on persistent storage (VM disk, network volume, object store sync target) so your scheduler can run stateless workers.

## Cron (simple and effective)

Create a small Python entrypoint (recommended):

```python
# refresh_dataset.py
from pathlib import Path

from sdmxflow import SdmxDataset


def main() -> None:
    ds = SdmxDataset(
        out_dir=Path("/var/lib/sdmxflow/lfsa_egai2d"),
        source_id="ESTAT",
        dataset_id="lfsa_egai2d",
        save_logs=True,
    )
    result = ds.fetch()
    # Use `result.appended` for downstream behavior (e.g., trigger a load step).
    print(f"appended={result.appended}")


if __name__ == "__main__":
    main()
```

Cron entry (daily at 02:00 UTC):

```cron
0 2 * * * /usr/bin/python3 /opt/jobs/refresh_dataset.py >> /var/log/sdmxflow.log 2>&1
```

## Airflow

### PythonOperator

```python
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from sdmxflow import SdmxDataset


def refresh() -> bool:
    ds = SdmxDataset(
        out_dir=Path("/data/sdmx/lfsa_egai2d"),
        source_id="ESTAT",
        dataset_id="lfsa_egai2d",
    )
    result = ds.fetch()
    return result.appended


with DAG(
    dag_id="sdmxflow_refresh",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
) as dag:
    refresh_task = PythonOperator(task_id="refresh", python_callable=refresh)
```

### TaskFlow API

```python
from __future__ import annotations

from pathlib import Path

from airflow.decorators import dag, task

from sdmxflow import SdmxDataset


@task
def refresh() -> bool:
    ds = SdmxDataset(
        out_dir=Path("/data/sdmx/lfsa_egai2d"),
        source_id="ESTAT",
        dataset_id="lfsa_egai2d",
    )
    return ds.fetch().appended


@dag(schedule="0 2 * * *", catchup=False)
def sdmxflow_refresh():
    appended = refresh()
    # Use `appended` to branch into a downstream load task.


sdmxflow_refresh()
```

## Prefect

`sdmxflow` does not require Prefect, but it composes well:

```python
from __future__ import annotations

from pathlib import Path

from prefect import flow, task

from sdmxflow import SdmxDataset


@task(retries=3, retry_delay_seconds=60)
def refresh() -> bool:
    ds = SdmxDataset(
        out_dir=Path("/data/sdmx/lfsa_egai2d"),
        source_id="ESTAT",
        dataset_id="lfsa_egai2d",
        save_logs=True,
    )
    return ds.fetch().appended


@flow
def refresh_flow() -> None:
    appended = refresh()
    if appended:
        # trigger warehouse load
        pass


if __name__ == "__main__":
    refresh_flow()
```

## Docker (wrapping pattern)

This repository does not ship an official container image. The typical pattern is:

- build a thin image that installs `sdmxflow`
- mount a persistent volume to the container at the `out_dir`
- run a small entrypoint script

Example `Dockerfile`:

```dockerfile
FROM python:3.12-slim

RUN pip install --no-cache-dir sdmxflow

WORKDIR /app
COPY refresh_dataset.py /app/refresh_dataset.py

CMD ["python", "/app/refresh_dataset.py"]
```

Run (mount persistent artifacts):

> **Note**
> In your container entrypoint, set `out_dir` to a path inside the mounted volume (e.g. `/data/lfsa_egai2d`).

```bash
docker run --rm \
  -v /srv/sdmxflow/lfsa_egai2d:/data/lfsa_egai2d \
  my-sdmxflow-job:latest
```

## Operational practices

### Retries and idempotency

- `fetch()` is designed to be safe to run repeatedly.
- There is no built-in exponential backoff at the top-level API today; implement retries in your scheduler (Airflow/Pefect/K8s Jobs).

### Logging

- At `INFO`, each fetch emits exactly 3 messages (intent, decision, completion summary).
- Use `DEBUG` for diagnostics.
- Use `save_logs=True` for per-run log capture under `<out_dir>/logs/`.

See [Configuration Reference](api.md) and [Logging](logging.md).

### Storage and permissions

- Put `out_dir` on durable storage.
- Ensure the job user can create/write: `dataset.csv`, `metadata.json`, `codelists/`, and optionally `logs/`.

### When to trigger warehouse loads

Use `FetchResult.appended`:

- `True`: new facts were appended → run your load job.
- `False`: upstream unchanged → you might skip loading (but metadata/codelists may still be updated).

Next:

- See [Integration Patterns](integration-patterns.md) for load examples.
