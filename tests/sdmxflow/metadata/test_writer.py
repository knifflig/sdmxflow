from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import pytest

from sdmxflow.errors import SdmxMetadataError
from sdmxflow.metadata.writer import (
    _LegacyMetadata,
    _LegacyVersionInfo,
    append_version,
    format_utc_iso,
    init_metadata,
    latest_upstream_last_updated,
    load_metadata,
    mark_fetched,
    save_metadata,
    upsert_top_level,
)


def test_init_and_append_and_save_roundtrip(tmp_path: Path) -> None:
    meta_path = tmp_path / "metadata.json"

    meta = init_metadata(
        agency_id="ESTAT",
        dataset_id="X",
        key="",
        params={"startPeriod": "2020"},
    )
    assert latest_upstream_last_updated(meta) is None

    # Fetch should update last_fetched_at even before any append.
    before = meta.last_fetched_at
    mark_fetched(meta, fetched_at=dt.datetime(2026, 3, 3, 11, 0, 0, tzinfo=dt.UTC))
    assert meta.last_fetched_at != before

    append_version(
        meta,
        upstream_last_updated="2026-01-01T00:00:00Z",
        fetched_at=dt.datetime(2026, 3, 3, 12, 0, 0, tzinfo=dt.UTC),
        http_url="https://example.invalid",
        http_status_code=200,
        http_headers={"etag": "abc"},
        rows_appended=123,
        last_updated_column="last_updated",
    )

    assert latest_upstream_last_updated(meta) == "2026-01-01T00:00:00Z"

    save_metadata(meta_path, meta)
    loaded = load_metadata(meta_path)
    assert loaded is not None
    assert loaded.agency_id == "ESTAT"
    assert loaded.versions[-1].dataset.rows_appended == 123
    assert loaded.files.datasets.csv == "dataset.csv"
    assert loaded.last_updated_data_at is not None


def test_upsert_top_level_preserves_versions(tmp_path: Path) -> None:
    meta = init_metadata(agency_id="ESTAT", dataset_id="X", key="", params={})
    append_version(
        meta,
        upstream_last_updated="2026-01-01T00:00:00Z",
        fetched_at=dt.datetime.now(dt.UTC),
        http_url=None,
        http_status_code=None,
        http_headers=None,
        rows_appended=1,
        last_updated_column="last_updated",
    )
    upsert_top_level(meta, agency_id="ESTAT", dataset_id="X", key="", params={"endPeriod": "2024"})
    assert len(meta.versions) == 1


def test_load_metadata_rejects_non_object(tmp_path: Path) -> None:
    p = tmp_path / "metadata.json"
    p.write_text("[]", encoding="utf-8")
    with pytest.raises(SdmxMetadataError):
        load_metadata(p)


def test_save_metadata_rejects_non_serializable(tmp_path: Path) -> None:
    class _NoJson:
        pass

    p = tmp_path / "metadata.json"
    with pytest.raises(SdmxMetadataError, match="not JSON-serializable"):
        save_metadata(p, {"x": _NoJson()})


def test_format_utc_iso_rejects_naive_datetime() -> None:
    with pytest.raises(SdmxMetadataError, match="timezone-aware"):
        format_utc_iso(dt.datetime(2026, 1, 1, 0, 0, 0))


def test_writer_utc_rejects_naive_datetime() -> None:
    import sdmxflow.metadata.writer as writer_mod

    with pytest.raises(ValueError, match="timezone-aware"):
        writer_mod._utc(dt.datetime(2026, 1, 1, 0, 0, 0))


def test_append_version_rejects_invalid_timestamp(tmp_path: Path) -> None:
    meta = init_metadata(agency_id="ESTAT", dataset_id="X", key="", params={})
    with pytest.raises(SdmxMetadataError, match="Invalid upstream_last_updated"):
        append_version(
            meta,
            upstream_last_updated="not-a-ts",
            fetched_at=dt.datetime.now(dt.UTC),
            http_url=None,
            http_status_code=None,
            http_headers=None,
            rows_appended=1,
            last_updated_column="last_updated",
        )


def test_save_metadata_mapping_branch_debug_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Ensure the mapping-compat branch is executed (and its debug log line covered).
    import sdmxflow.metadata.writer as writer_mod

    prev = writer_mod._logger.level
    writer_mod._logger.setLevel(logging.DEBUG)
    p = tmp_path / "metadata.json"
    try:
        save_metadata(p, {"agency_id": "ESTAT", "dataset_id": "X"})
        assert p.exists()
    finally:
        writer_mod._logger.setLevel(prev)


def test_legacy_models_validate_and_serialize() -> None:
    legacy = _LegacyVersionInfo.model_validate(
        {
            "upstream_last_updated": "2026-01-01T00:00:00Z",
            "fetched_at": "2026-01-02T00:00:00Z",
            "http": {"url": "u", "status_code": 200, "headers": {}},
            "dataset": {"rows_appended": 1, "last_updated_column": "last_updated"},
        }
    )
    payload = legacy.model_dump(mode="json")
    assert payload["upstream_last_updated"].endswith("Z")

    legacy_meta = _LegacyMetadata.model_validate(
        {
            "schema_version": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "source_id": "ESTAT",
            "dataset_id": "X",
            "agency_id": None,
            "key": "",
            "params": {},
            "files": {"dataset_csv": "dataset.csv", "codelists_dir": "codelists"},
            "versions": [payload],
            "codelists": [],
        }
    )
    payload2 = legacy_meta.model_dump(mode="json")
    assert payload2["created_at"].endswith("Z")


def test_load_metadata_migrates_legacy_schema(tmp_path: Path) -> None:
    # Legacy shape should fall back to the migration path.
    p = tmp_path / "metadata.json"
    p.write_text(
        """
{
  "schema_version": 1,
  "created_at": "2026-01-01T00:00:00Z",
  "updated_at": "2026-01-01T00:00:00Z",
  "source_id": "ESTAT",
  "dataset_id": "X",
  "agency_id": null,
  "key": "",
  "params": {},
  "files": {"dataset_csv": "dataset.csv", "codelists_dir": "codelists"},
  "versions": [
    {
      "upstream_last_updated": "2026-01-01T00:00:00Z",
      "fetched_at": "2026-01-02T00:00:00Z",
      "http": {"url": "u", "status_code": 200, "headers": {"etag": "abc"}},
      "dataset": {"rows_appended": 1, "last_updated_column": "last_updated"}
    }
  ],
  "codelists": []
}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    meta = load_metadata(p)
    assert meta is not None
    assert meta.agency_id == "ESTAT"
    assert meta.versions
    assert meta.versions[-1].http.headers.get("etag") == "abc"
