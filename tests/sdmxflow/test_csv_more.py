from __future__ import annotations

from pathlib import Path

import pytest

from sdmxflow._csv import append_version_slice, ensure_last_updated_first_column
from sdmxflow.errors import SdmxMetadataError


def test_ensure_last_updated_first_column_rewrites_lines_without_newline(tmp_path: Path) -> None:
    path = tmp_path / "dataset.csv"
    # Second line intentionally has no trailing newline.
    path.write_text("a,b\n1,2", encoding="utf-8")
    ensure_last_updated_first_column(csv_path=path)
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    assert lines[0] == "last_updated,a,b"
    # Rewritten line should include prefixed comma and preserve data.
    assert lines[1] == ",1,2"


def test_append_version_slice_appends_source_line_without_newline(tmp_path: Path) -> None:
    src = tmp_path / "src.csv"
    dst = tmp_path / "dst.csv"
    src.write_text("a,b\n1,2", encoding="utf-8")
    rows = append_version_slice(
        src_csv=src, dst_csv=dst, upstream_last_updated="2026-01-01T00:00:00Z"
    )
    assert rows == 1
    lines = dst.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "last_updated,a,b"
    assert lines[1] == "2026-01-01T00:00:00Z,1,2"


def test_append_version_slice_rejects_missing_src_file(tmp_path: Path) -> None:
    with pytest.raises(SdmxMetadataError):
        append_version_slice(
            src_csv=tmp_path / "nope.csv",
            dst_csv=tmp_path / "dst.csv",
            upstream_last_updated="2026-01-01T00:00:00Z",
        )
