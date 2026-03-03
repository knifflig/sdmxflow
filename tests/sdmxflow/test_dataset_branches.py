from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from sdmxflow.dataset import SdmxDataset
from sdmxflow.download.providers.eurostat_bulk_csv import EurostatBulkCsvResult
from sdmxflow.errors import (
    SdmxDownloadError,
    SdmxInterruptedError,
    SdmxTimeoutError,
    SdmxUnreachableError,
)
from sdmxflow.metadata.writer import init_metadata
from sdmxflow.models import FlowStructureArtifacts
from sdmxflow.query.last_updated_data import LastUpdatedInfo


def _fixed_last_updated(dataset_id: str, updated_at: dt.datetime):
    def _impl(*, dataset_id: str, logger=None, timeout_seconds=None):  # noqa: ANN001,ARG001
        return LastUpdatedInfo(source_id="ESTAT", dataset_id=dataset_id, updated_at=updated_at)

    return _impl


def test_fetch_ignores_tmp_unlink_oserror_and_result_unlink_oserror(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_sdmx_client,
) -> None:
    _ = patch_sdmx_client

    payload = "A,B\n1,2\n"
    updated_at = dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC)

    def _fake_bulk_download(
        self, *, dataset_id, out_path, key="", params=None, if_exists="skip", timeout_seconds=None
    ):  # noqa: ANN001,ARG001
        out_path = Path(out_path)
        out_path.write_text(payload, encoding="utf-8")
        return EurostatBulkCsvResult(
            csv_path=out_path, url="https://example.invalid", status_code=200, headers={}
        )

    import pathlib

    import sdmxflow.dataset as dataset_mod
    import sdmxflow.download.providers.eurostat_bulk_csv as bulk_mod

    monkeypatch.setattr(dataset_mod, "eurostat_last_updated", _fixed_last_updated("X", updated_at))
    monkeypatch.setattr(bulk_mod.EurostatBulkCsvDownloader, "download", _fake_bulk_download)

    # Create the temp download file so the pre-cleanup runs.
    tmp_download = tmp_path / ".sdmxflow.download.csv"
    tmp_download.write_text("stale", encoding="utf-8")

    orig_unlink = pathlib.Path.unlink

    def _unlink(self: pathlib.Path, *args, **kwargs):  # noqa: ANN001
        if self == tmp_download:
            raise OSError("boom")
        return orig_unlink(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "unlink", _unlink, raising=True)

    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="X")
    res = ds.fetch()
    assert res.appended is True
    assert res.dataset_csv.exists()
    assert res.metadata_json.exists()


@pytest.mark.parametrize(
    "exc",
    [
        SdmxTimeoutError("t"),
        SdmxUnreachableError("u"),
        SdmxDownloadError("d"),
    ],
)
def test_fetch_propagates_typed_download_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_sdmx_client,
    exc: Exception,
) -> None:
    _ = patch_sdmx_client
    updated_at = dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC)

    def _fake_bulk_download(self, **kwargs):  # noqa: ANN001,ARG001
        raise exc

    import sdmxflow.dataset as dataset_mod
    import sdmxflow.download.providers.eurostat_bulk_csv as bulk_mod

    monkeypatch.setattr(dataset_mod, "eurostat_last_updated", _fixed_last_updated("X", updated_at))
    monkeypatch.setattr(bulk_mod.EurostatBulkCsvDownloader, "download", _fake_bulk_download)

    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="X")
    with pytest.raises(type(exc)):
        ds.fetch()


def test_fetch_wraps_keyboard_interrupt_in_bulk_download(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_sdmx_client,
) -> None:
    _ = patch_sdmx_client
    updated_at = dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC)

    def _fake_bulk_download(self, **kwargs):  # noqa: ANN001,ARG001
        raise KeyboardInterrupt()

    import sdmxflow.dataset as dataset_mod
    import sdmxflow.download.providers.eurostat_bulk_csv as bulk_mod

    monkeypatch.setattr(dataset_mod, "eurostat_last_updated", _fixed_last_updated("X", updated_at))
    monkeypatch.setattr(bulk_mod.EurostatBulkCsvDownloader, "download", _fake_bulk_download)

    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="X")
    with pytest.raises(SdmxInterruptedError, match="Bulk download interrupted"):
        ds.fetch()


def test_ensure_codelists_returns_when_dataset_missing(tmp_path: Path) -> None:
    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="X")
    meta = init_metadata(agency_id="ESTAT", dataset_id="X", key="", params={})
    # No dataset.csv created.
    ds._ensure_codelists(meta)  # noqa: SLF001


def test_ensure_codelists_unsupported_source_raises(tmp_path: Path) -> None:
    ds = SdmxDataset(out_dir=tmp_path, source_id="NOPE", dataset_id="X")
    ds.paths.dataset_csv.write_text("last_updated,A\n2026-01-01T00:00:00Z,1\n", encoding="utf-8")
    meta = init_metadata(agency_id="ESTAT", dataset_id="X", key="", params={})
    with pytest.raises(SdmxDownloadError, match="Unsupported source_id"):
        ds._ensure_codelists(meta)  # noqa: SLF001


def test_ensure_codelists_cleans_tmp_dir_and_structure_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="X")
    ds.paths.dataset_csv.write_text("last_updated,A\n2026-01-01T00:00:00Z,1\n", encoding="utf-8")
    meta = init_metadata(agency_id="ESTAT", dataset_id="X", key="", params={})

    # Pre-create tmp structures dir with a subdir so unlink/rmdir error branches execute.
    tmp_dir = tmp_path / ".sdmxflow.structures.tmp"
    (tmp_dir / "child_dir").mkdir(parents=True, exist_ok=True)

    # Return "structure" artifacts as directories so unlink fails in cleanup.
    dataflow_dir = tmp_dir / "dataflow.xml"
    datastructure_dir = tmp_dir / "datastructure.xml"
    dataflow_dir.mkdir(parents=True, exist_ok=True)
    datastructure_dir.mkdir(parents=True, exist_ok=True)

    def _fake_download_flow_structures(self, **kwargs):  # noqa: ANN001,ARG001
        return FlowStructureArtifacts(dataflow=dataflow_dir, datastructure=datastructure_dir)

    import sdmxflow.dataset as dataset_mod

    monkeypatch.setattr(
        dataset_mod.SdmxStructureDownloader,
        "download_flow_structures",
        _fake_download_flow_structures,
    )
    monkeypatch.setattr(dataset_mod, "write_codelists_csvs", lambda **kwargs: [])

    ds._ensure_codelists(meta)  # noqa: SLF001


def test_ensure_codelists_keyboard_interrupt_is_wrapped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="X")
    ds.paths.dataset_csv.write_text("last_updated,A\n2026-01-01T00:00:00Z,1\n", encoding="utf-8")
    meta = init_metadata(agency_id="ESTAT", dataset_id="X", key="", params={})

    def _interrupt(self, **kwargs):  # noqa: ANN001,ARG001
        raise KeyboardInterrupt()

    import sdmxflow.dataset as dataset_mod

    monkeypatch.setattr(dataset_mod.SdmxStructureDownloader, "download_flow_structures", _interrupt)

    with pytest.raises(SdmxInterruptedError, match="Codelist generation interrupted"):
        ds._ensure_codelists(meta)  # noqa: SLF001
