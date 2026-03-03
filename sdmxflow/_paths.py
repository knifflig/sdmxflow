from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DatasetPaths:
    out_dir: Path
    dataset_csv: Path
    metadata_json: Path
    codelists_dir: Path


def dataset_paths(out_dir: str | Path) -> DatasetPaths:
    base = Path(out_dir).expanduser().resolve()
    return DatasetPaths(
        out_dir=base,
        dataset_csv=base / "dataset.csv",
        metadata_json=base / "metadata.json",
        codelists_dir=base / "codelists",
    )
