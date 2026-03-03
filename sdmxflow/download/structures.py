"""Higher-level SDMX structure downloads."""

from __future__ import annotations

from pathlib import Path

from .._types import IfExists
from ..models import FlowStructureArtifacts, SdmxRequest
from .native import SdmxNativeDownloader


class SdmxStructureDownloader:
    """Download SDMX structure artifacts for datasets."""

    def __init__(self, native: SdmxNativeDownloader) -> None:
        self._native = native

    def download_flow_structures(
        self,
        *,
        source_id: str,
        dataset_id: str,
        out_dir: str | Path,
        if_exists: IfExists = "skip",
    ) -> FlowStructureArtifacts:
        """Download dataflow + datastructure artifacts into `out_dir`.

        Writes two files and their `.meta.json` sidecars:
        - `dataflow_all.*` (references=none)
        - `datastructure_descendants.*` (references=descendants)
        """
        out_path = Path(out_dir).expanduser()
        out_path.mkdir(parents=True, exist_ok=True)

        df = self._native.download(
            SdmxRequest(
                source_id=source_id,
                resource_type="dataflow",
                resource_id=dataset_id,
                # Eurostat's SDMX endpoint does not support references=all for this call
                # (the upstream SDMX client downgrades it to 'none' and logs about it).
                params={"references": "none"},
            ),
            out_dir=out_path,
            filename="dataflow_all",
            if_exists=if_exists,
        )

        dsd = self._native.download(
            SdmxRequest(
                source_id=source_id,
                resource_type="datastructure",
                resource_id=dataset_id,
                params={"references": "descendants"},
            ),
            out_dir=out_path,
            filename="datastructure_descendants",
            if_exists=if_exists,
        )

        return FlowStructureArtifacts(dataflow=df.path, datastructure=dsd.path)
