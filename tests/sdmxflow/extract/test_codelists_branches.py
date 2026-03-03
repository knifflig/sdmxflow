from __future__ import annotations

from pathlib import Path

import pytest

from sdmxflow.errors import SdmxMetadataError
from sdmxflow.extract import codelists as mod
from sdmxflow.models import FlowStructureArtifacts


def test_items_dict_returns_none_on_bad_pairs() -> None:
    class _X:
        def items(self):
            return [("a", "b", "c")]

    assert mod._items_dict(_X()) is None  # noqa: SLF001


def test_best_label_none_branch() -> None:
    assert mod._best_label(None) is None  # noqa: SLF001


def test_read_structure_message_wraps_parse_errors(tmp_path: Path) -> None:
    p = tmp_path / "broken.xml"
    p.write_text("not-xml", encoding="utf-8")
    with pytest.raises(SdmxMetadataError, match="Failed to parse SDMX structure file"):
        mod._read_structure_message(p)  # noqa: SLF001


def test_extract_all_codelists_handles_missing_property(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class _Msg:
        @property
        def codelist(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(mod, "_read_structure_message", lambda path: _Msg())
    structures = FlowStructureArtifacts(dataflow=tmp_path / "a", datastructure=tmp_path / "b")
    assert mod._extract_all_codelists(structures=structures) == {}  # noqa: SLF001


def test_extract_codelist_usages_empty_structure_returns_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class _Msg:
        structure = None

    monkeypatch.setattr(mod, "_read_structure_message", lambda path: _Msg())
    structures = FlowStructureArtifacts(dataflow=tmp_path / "a", datastructure=tmp_path / "b")
    assert mod._extract_codelist_usages(structures=structures) == []  # noqa: SLF001


def test_write_codelists_csvs_skips_unmapped_columns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    structures = FlowStructureArtifacts(dataflow=tmp_path / "a", datastructure=tmp_path / "b")

    monkeypatch.setattr(mod, "_extract_all_codelists", lambda *, structures: {"CL": {"codes": []}})
    monkeypatch.setattr(
        mod,
        "_extract_codelist_usages",
        lambda *, structures: [{"kind": "dimension", "column_name": "NOPE", "codelist_id": "CL"}],
    )

    out = mod.write_codelists_csvs(
        structures=structures,
        out_dir=tmp_path / "out",
        dataset_columns=["A"],
        relative_prefix="codelists",
    )
    assert out == []


def test_write_codelists_csvs_skips_non_dict_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    structures = FlowStructureArtifacts(dataflow=tmp_path / "a", datastructure=tmp_path / "b")

    monkeypatch.setattr(mod, "_extract_all_codelists", lambda *, structures: {"CL": "bad"})
    monkeypatch.setattr(
        mod,
        "_extract_codelist_usages",
        lambda *, structures: [{"kind": "dimension", "column_name": "A", "codelist_id": "CL"}],
    )

    entries = mod.write_codelists_csvs(
        structures=structures,
        out_dir=tmp_path / "out",
        dataset_columns=["A"],
        relative_prefix="codelists",
    )
    assert entries and entries[0]["codelist_id"] == "CL"
    # No file written because payload is not a dict.
    assert not (tmp_path / "out" / "CL.csv").exists()


def test_write_codelists_csvs_non_list_codes_and_non_dict_code_entries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    structures = FlowStructureArtifacts(dataflow=tmp_path / "a", datastructure=tmp_path / "b")

    # Hit the `codes` normalization branch (non-list => []).
    monkeypatch.setattr(
        mod, "_extract_all_codelists", lambda *, structures: {"CL": {"codes": "bad"}}
    )
    monkeypatch.setattr(
        mod,
        "_extract_codelist_usages",
        lambda *, structures: [{"kind": "dimension", "column_name": "A", "codelist_id": "CL"}],
    )

    _ = mod.write_codelists_csvs(
        structures=structures,
        out_dir=tmp_path / "out",
        dataset_columns=["A"],
        relative_prefix="codelists",
    )

    csv_path = tmp_path / "out" / "CL.csv"
    assert csv_path.exists()
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines == ["code,name"]


def test_write_codelists_csvs_skips_non_dict_code_entries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    structures = FlowStructureArtifacts(dataflow=tmp_path / "a", datastructure=tmp_path / "b")

    monkeypatch.setattr(
        mod,
        "_extract_all_codelists",
        lambda *, structures: {"CL": {"codes": ["x", {"id": None, "name": None}]}},
    )
    monkeypatch.setattr(
        mod,
        "_extract_codelist_usages",
        lambda *, structures: [{"kind": "dimension", "column_name": "A", "codelist_id": "CL"}],
    )

    _ = mod.write_codelists_csvs(
        structures=structures,
        out_dir=tmp_path / "out",
        dataset_columns=["A"],
        relative_prefix="codelists",
    )

    csv_path = tmp_path / "out" / "CL.csv"
    assert csv_path.exists()
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "code,name"
    assert lines[1] == ","
