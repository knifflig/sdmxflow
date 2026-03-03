from __future__ import annotations

from pathlib import Path

from sdmxflow.download.native import SdmxNativeDownloader
from sdmxflow.download.structures import SdmxStructureDownloader


def test_download_flow_structures_writes_artifacts(tmp_path: Path, patch_sdmx_client) -> None:
    _ = patch_sdmx_client
    native = SdmxNativeDownloader(source_id="ESTAT")
    dl = SdmxStructureDownloader(native)

    artifacts = dl.download_flow_structures(
        source_id="ESTAT",
        dataset_id="lfsa_egai2d",
        out_dir=tmp_path,
        if_exists="overwrite",
    )

    assert artifacts.dataflow.exists()
    assert artifacts.datastructure.exists()
    assert artifacts.dataflow.with_name(artifacts.dataflow.name + ".meta.json").exists()
    assert artifacts.datastructure.with_name(artifacts.datastructure.name + ".meta.json").exists()
