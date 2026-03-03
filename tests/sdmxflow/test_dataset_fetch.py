from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from sdmxflow.dataset import SdmxDataset
from sdmxflow.download.providers.eurostat_bulk_csv import EurostatBulkCsvResult
from sdmxflow.query.last_updated_data import LastUpdatedInfo


def test_fetch_appends_only_on_new_upstream_version(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_sdmx_client,
) -> None:
    _ = patch_sdmx_client

    v1_payload = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "bulk"
        / "estat"
        / "lfsa_egai2d"
        / "bulk_v1.csv"
    ).read_text(encoding="utf-8")
    v2_payload = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "bulk"
        / "estat"
        / "lfsa_egai2d"
        / "bulk_v2.csv"
    ).read_text(encoding="utf-8")

    updated_at = dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC)

    def _fake_last_updated(*, dataset_id: str, logger=None, timeout_seconds=None):  # noqa: ANN001,ARG001
        return LastUpdatedInfo(source_id="ESTAT", dataset_id=dataset_id, updated_at=updated_at)

    def _fake_bulk_download(
        self, *, dataset_id, out_path, key="", params=None, if_exists="skip", timeout_seconds=None
    ):  # noqa: ANN001,ARG001
        out_path = Path(out_path)

        # Choose a deterministic bulk CSV payload based on the upstream timestamp.
        if updated_at == dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC):
            out_path.write_text(v1_payload, encoding="utf-8")
        elif updated_at == dt.datetime(2026, 2, 1, 0, 0, 0, tzinfo=dt.UTC):
            out_path.write_text(v2_payload, encoding="utf-8")
        else:
            raise AssertionError(f"Unexpected updated_at in test: {updated_at!r}")

        return EurostatBulkCsvResult(
            csv_path=out_path,
            url="https://example.invalid",
            status_code=200,
            headers={"etag": "abc"},
        )

    import sdmxflow.dataset as dataset_mod
    import sdmxflow.download.providers.eurostat_bulk_csv as bulk_mod

    monkeypatch.setattr(dataset_mod, "eurostat_last_updated", _fake_last_updated)
    monkeypatch.setattr(bulk_mod.EurostatBulkCsvDownloader, "download", _fake_bulk_download)

    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="lfsa_egai2d")
    r1 = ds.fetch()
    assert r1.appended is True
    assert r1.dataset_csv.exists()
    assert r1.metadata_json.exists()
    assert r1.codelists_dir.exists()

    payload = r1.metadata_json.read_text(encoding="utf-8")
    assert '"agency_id": "ESTAT"' in payload
    assert '"source_id"' not in payload
    assert '"last_fetched_at"' in payload
    assert '"last_updated_at"' in payload
    assert '"last_updated_data_at"' in payload

    meta1 = json.loads(payload)
    assert meta1["last_updated_data_at"] == "2026-01-01T00:00:00Z"
    assert len(meta1["versions"]) == 1
    assert meta1["versions"][0]["dataset"]["rows_appended"] == 2

    # Same upstream timestamp -> no append.
    r2 = ds.fetch()
    assert r2.appended is False

    meta2 = json.loads(r2.metadata_json.read_text(encoding="utf-8"))
    assert meta2["last_updated_data_at"] == "2026-01-01T00:00:00Z"
    assert len(meta2["versions"]) == 1

    # New upstream timestamp -> append.
    updated_at = dt.datetime(2026, 2, 1, 0, 0, 0, tzinfo=dt.UTC)
    r3 = ds.fetch()
    assert r3.appended is True

    meta3 = json.loads(r3.metadata_json.read_text(encoding="utf-8"))
    assert meta3["last_updated_data_at"] == "2026-02-01T00:00:00Z"
    assert len(meta3["versions"]) == 2
    assert meta3["versions"][1]["dataset"]["rows_appended"] == 1

    lines = r3.dataset_csv.read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("last_updated,")
    # bulk_v1 has 2 rows, bulk_v2 has 1 row
    assert len(lines) == 1 + 2 + 1

    # Verify per-row upstream timestamps are correct across appended versions.
    assert lines[1].startswith("2026-01-01T00:00:00Z,")
    assert lines[2].startswith("2026-01-01T00:00:00Z,")
    assert lines[3].startswith("2026-02-01T00:00:00Z,")


def test_fetch_save_logs_writes_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_sdmx_client,
) -> None:
    _ = patch_sdmx_client

    v1_payload = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "bulk"
        / "estat"
        / "lfsa_egai2d"
        / "bulk_v1.csv"
    ).read_text(encoding="utf-8")

    updated_at = dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC)

    def _fake_last_updated(*, dataset_id: str, logger=None, timeout_seconds=None):  # noqa: ANN001,ARG001
        return LastUpdatedInfo(source_id="ESTAT", dataset_id=dataset_id, updated_at=updated_at)

    def _fake_bulk_download(
        self, *, dataset_id, out_path, key="", params=None, if_exists="skip", timeout_seconds=None
    ):  # noqa: ANN001,ARG001
        out_path = Path(out_path)
        out_path.write_text(v1_payload, encoding="utf-8")
        return EurostatBulkCsvResult(
            csv_path=out_path,
            url="https://example.invalid",
            status_code=200,
            headers={"etag": "abc"},
        )

    import sdmxflow.dataset as dataset_mod
    import sdmxflow.download.providers.eurostat_bulk_csv as bulk_mod

    monkeypatch.setattr(dataset_mod, "eurostat_last_updated", _fake_last_updated)
    monkeypatch.setattr(bulk_mod.EurostatBulkCsvDownloader, "download", _fake_bulk_download)

    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="lfsa_egai2d", save_logs=True)
    _ = ds.fetch()

    logs_dir = tmp_path / "logs"
    assert logs_dir.exists()
    log_files = sorted(p for p in logs_dir.iterdir() if p.is_file() and p.suffix == ".log")
    assert log_files, "Expected at least one log file"
    text = log_files[-1].read_text(encoding="utf-8")
    assert "Fetch requested:" in text
    assert "Download complete" in text
