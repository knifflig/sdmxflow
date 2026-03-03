from __future__ import annotations

import datetime as dt

import pytest

from sdmxflow.metadata.models import CodelistEntry, Metadata


def test_metadata_rejects_naive_datetimes() -> None:
    naive = dt.datetime(2026, 1, 1, 0, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        Metadata(
            agency_id="ESTAT",
            dataset_id="x",
            created_at=naive,
            last_fetched_at=naive,
            last_updated_at=naive,
        )


def test_codelist_entry_validators() -> None:
    with pytest.raises(ValueError, match="codelist_id"):
        CodelistEntry(
            codelist_id=" ",
            codelist_path="codelists/X.csv",
            codelist_kind="dimension",
            column_name="GEO",
            column_pos=1,
        )

    with pytest.raises(ValueError, match="column_name"):
        CodelistEntry(
            codelist_id="X",
            codelist_path="codelists/X.csv",
            codelist_kind="dimension",
            column_name=" ",
            column_pos=1,
        )

    with pytest.raises(ValueError, match="1-based"):
        CodelistEntry(
            codelist_id="X",
            codelist_path="codelists/X.csv",
            codelist_kind="dimension",
            column_name="GEO",
            column_pos=0,
        )


def test_metadata_optional_last_updated_data_at_none_roundtrips() -> None:
    meta = Metadata(
        agency_id="ESTAT",
        dataset_id="X",
        key="",
        params={},
        created_at=dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
        last_fetched_at=dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
        last_updated_at=dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
        last_updated_data_at=None,
    )
    payload = meta.model_dump(mode="json")
    assert payload.get("last_updated_data_at") is None
