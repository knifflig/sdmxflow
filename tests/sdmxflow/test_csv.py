from __future__ import annotations

from pathlib import Path

import pytest

from sdmxflow._csv import (
    LAST_UPDATED_COLUMN,
    append_version_slice,
    ensure_last_updated_first_column,
)
from sdmxflow.errors import SdmxMetadataError


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_append_version_slice_creates_dst_with_last_updated(tmp_path: Path) -> None:
    src = tmp_path / "src.csv"
    dst = tmp_path / "dataset.csv"
    _write(src, "A,B\n1,2\n3,4\n")

    n = append_version_slice(src_csv=src, dst_csv=dst, upstream_last_updated="2026-01-01T00:00:00Z")
    assert n == 2
    lines = dst.read_text(encoding="utf-8").splitlines()
    assert lines[0] == f"{LAST_UPDATED_COLUMN},A,B"
    assert lines[1] == "2026-01-01T00:00:00Z,1,2"
    assert lines[2] == "2026-01-01T00:00:00Z,3,4"


def test_append_version_slice_rejects_schema_mismatch(tmp_path: Path) -> None:
    src1 = tmp_path / "src1.csv"
    src2 = tmp_path / "src2.csv"
    dst = tmp_path / "dataset.csv"
    _write(src1, "A,B\n1,2\n")
    _write(src2, "A,C\n1,2\n")

    append_version_slice(src_csv=src1, dst_csv=dst, upstream_last_updated="2026-01-01T00:00:00Z")
    with pytest.raises(SdmxMetadataError):
        append_version_slice(
            src_csv=src2, dst_csv=dst, upstream_last_updated="2026-02-01T00:00:00Z"
        )


def test_ensure_last_updated_first_column_rewrites_existing(tmp_path: Path) -> None:
    p = tmp_path / "dataset.csv"
    _write(p, "A,B\n1,2\n")
    ensure_last_updated_first_column(csv_path=p)
    lines = p.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "last_updated,A,B"
    assert lines[1] == ",1,2"


def test_parse_header_invalid_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from sdmxflow import _csv as mod

    def _boom(*args, **kwargs):  # noqa: ANN001,ARG001
        raise RuntimeError("bad csv")

    monkeypatch.setattr(mod.csv, "reader", _boom)
    with pytest.raises(SdmxMetadataError):
        mod._parse_header_line("A,B")


def test_normalize_header_strips_bom_and_rejects_empty() -> None:
    from sdmxflow import _csv as mod

    assert mod._normalize_provider_header(["\ufeffA", "B"]) == ["A", "B"]
    with pytest.raises(SdmxMetadataError):
        mod._normalize_provider_header([LAST_UPDATED_COLUMN])


def test_ensure_last_updated_first_column_noop_on_missing_or_empty(tmp_path: Path) -> None:
    p_missing = tmp_path / "missing.csv"
    ensure_last_updated_first_column(csv_path=p_missing)

    p_empty = tmp_path / "empty.csv"
    p_empty.write_text("", encoding="utf-8")
    ensure_last_updated_first_column(csv_path=p_empty)
    assert p_empty.read_text(encoding="utf-8") == ""


def test_append_version_slice_errors(tmp_path: Path) -> None:
    src_missing = tmp_path / "missing.csv"
    with pytest.raises(SdmxMetadataError, match="does not exist"):
        append_version_slice(
            src_csv=src_missing, dst_csv=tmp_path / "dst.csv", upstream_last_updated="x"
        )

    src_empty = tmp_path / "empty.csv"
    src_empty.write_text("", encoding="utf-8")
    with pytest.raises(SdmxMetadataError, match="empty"):
        append_version_slice(
            src_csv=src_empty, dst_csv=tmp_path / "dst.csv", upstream_last_updated="x"
        )


def test_append_version_slice_rejects_bad_destination_header(tmp_path: Path) -> None:
    src = tmp_path / "src.csv"
    dst = tmp_path / "dst.csv"
    _write(src, "A,B\n1,2\n")
    _write(dst, "\n")
    with pytest.raises(SdmxMetadataError, match="empty/invalid header"):
        append_version_slice(src_csv=src, dst_csv=dst, upstream_last_updated="x")
