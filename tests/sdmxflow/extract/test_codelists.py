from __future__ import annotations

from pathlib import Path

from sdmxflow.extract.codelists import write_codelists_csvs
from sdmxflow.models import FlowStructureArtifacts
from tests.conftest import skip_if_real_world_fixtures_missing


def test_write_codelists_csvs_from_real_world_fixture(tmp_path: Path) -> None:
    paths = skip_if_real_world_fixtures_missing()

    out_dir = tmp_path / "codelists"
    structures = FlowStructureArtifacts(
        dataflow=paths["dataflow"],
        datastructure=paths["datastructure"],
    )

    # Provider columns (exclude the internal 'last_updated' column).
    dataset_columns = [
        "STRUCTURE",
        "STRUCTURE_ID",
        "freq",
        "isco08",
        "age",
        "sex",
        "unit",
        "geo",
        "TIME_PERIOD",
        "OBS_VALUE",
        "OBS_FLAG",
        "CONF_STATUS",
    ]

    entries = write_codelists_csvs(
        structures=structures, out_dir=out_dir, dataset_columns=dataset_columns
    )

    assert out_dir.exists()
    assert (out_dir / "FREQ.csv").exists()
    assert (out_dir / "SEX.csv").exists()

    header = (out_dir / "FREQ.csv").read_text(encoding="utf-8").splitlines()[0]
    assert header == "code,name"

    # Spot-check one mapping entry.
    freq = next(e for e in entries if e["codelist_id"] == "FREQ")
    assert freq["codelist_path"] == "codelists/FREQ.csv"
    assert freq["codelist_kind"] == "dimension"
    assert freq["column_name"] == "freq"
    assert freq["column_pos"] == 3


def test_codelist_helper_functions_cover_branches() -> None:
    from sdmxflow.extract import codelists as mod

    class _WithDictItems:
        items = {"a": 1}

    assert mod._items_dict(_WithDictItems()) == {"a": 1}

    class _WithCallableItems:
        def items(self):
            return [("x", 1), ("y", 2)]

    assert mod._items_dict(_WithCallableItems()) == {"x": 1, "y": 2}

    class _BadItems:
        def items(self):
            raise TypeError("nope")

    assert mod._items_dict(_BadItems()) is None

    assert mod._labels(object()) is None
    assert mod._best_label({"en": "Hello", "fr": "Bonjour"}) == "Hello"
    assert mod._best_label({"fr": "Bonjour", "de": "Hallo"}) in {"Bonjour", "Hallo"}
    assert mod._opt_str("") is None
    assert mod._opt_str("x") == "x"
    assert mod._extract_codes(object()) == []
