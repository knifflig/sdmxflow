"""Integration test that (re)generates real-world SDMX fixtures.

This is intentionally marked as both `integration` and `network` and is excluded
from the default test run via pytest.ini.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from sdmxflow.download.native import SdmxNativeDownloader
from sdmxflow.download.structures import SdmxStructureDownloader


@pytest.mark.integration
@pytest.mark.network
def test_download_estat_structure_fixtures() -> None:
    dataset_id = "lfsa_egai2d"
    out_dir = Path(__file__).resolve().parents[1] / "data" / "sdmx" / "estat" / dataset_id / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    native = SdmxNativeDownloader(source_id="ESTAT")
    structures = SdmxStructureDownloader(native)
    artifacts = structures.download_flow_structures(
        source_id="ESTAT",
        dataset_id=dataset_id,
        out_dir=out_dir,
        if_exists="overwrite",
    )

    # Rename to stable, expected fixture names.
    df = out_dir / "dataflow_all.xml"
    dsd = out_dir / "datastructure_descendants.xml"

    artifacts.dataflow.replace(df)
    artifacts.datastructure.replace(dsd)

    # And keep their meta sidecars aligned.
    old_df_meta = artifacts.dataflow.with_name(artifacts.dataflow.name + ".meta.json")
    old_dsd_meta = artifacts.datastructure.with_name(artifacts.datastructure.name + ".meta.json")
    if old_df_meta.exists():
        old_df_meta.replace(df.with_name(df.name + ".meta.json"))
    if old_dsd_meta.exists():
        old_dsd_meta.replace(dsd.with_name(dsd.name + ".meta.json"))

    assert df.exists()
    assert dsd.exists()
