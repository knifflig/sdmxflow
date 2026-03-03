from __future__ import annotations

from pathlib import Path

import pytest

from sdmxflow._csv import append_version_slice, ensure_last_updated_first_column
from sdmxflow.errors import SdmxMetadataError


def test_ensure_last_updated_first_column_skips_blank_lines(tmp_path: Path) -> None:
    p = tmp_path / "x.csv"
    p.write_text("A,B\n\n1,2\n", encoding="utf-8")

    ensure_last_updated_first_column(csv_path=p)

    lines = p.read_text(encoding="utf-8").splitlines()
    assert lines == ["last_updated,A,B", ",1,2"]


def test_append_version_slice_skips_blank_lines(tmp_path: Path) -> None:
    src = tmp_path / "src.csv"
    src.write_text("A,B\n\n1,2\n", encoding="utf-8")
    dst = tmp_path / "dst.csv"

    rows = append_version_slice(
        src_csv=src, dst_csv=dst, upstream_last_updated="2026-01-01T00:00:00Z"
    )
    assert rows == 1

    lines = dst.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "last_updated,A,B"
    assert lines[1] == "2026-01-01T00:00:00Z,1,2"


def test_append_version_slice_destination_has_no_header_branch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import os

    src = tmp_path / "src.csv"
    src.write_text("A\n1\n", encoding="utf-8")

    dst = tmp_path / "dst.csv"
    dst.write_text("", encoding="utf-8")

    orig_stat = Path.stat

    def _fake_stat(self: Path, *args, **kwargs):  # noqa: ANN001
        if self == dst:
            st = orig_stat(self, *args, **kwargs)
            values = list(st)
            values[6] = 1  # st_size
            return os.stat_result(values)
        return orig_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", _fake_stat, raising=True)

    with pytest.raises(SdmxMetadataError, match="Destination CSV has no header"):
        append_version_slice(src_csv=src, dst_csv=dst, upstream_last_updated="2026-01-01T00:00:00Z")


def test_append_version_slice_destination_header_missing_last_updated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import sdmxflow._csv as csv_mod

    # Bypass the normal header-rewrite so we can exercise the explicit error branch.
    monkeypatch.setattr(csv_mod, "ensure_last_updated_first_column", lambda *, csv_path: None)

    src = tmp_path / "src.csv"
    src.write_text("A,B\n1,2\n", encoding="utf-8")

    dst = tmp_path / "dst.csv"
    dst.write_text("A,B\n", encoding="utf-8")

    with pytest.raises(SdmxMetadataError, match="Destination CSV header is missing"):
        append_version_slice(src_csv=src, dst_csv=dst, upstream_last_updated="2026-01-01T00:00:00Z")
