"""Metadata schema models.

The metadata schema captures:

- the dataset identity (agency, dataset id, key, params),
- file layout (dataset CSV/parquet and codelists), and
- an append-only list of fetched versions with timestamps and HTTP details.

These models are implemented with Pydantic v2 and are used for reading and
writing `metadata.json`.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from .schema import SCHEMA_VERSION


def _utc(dt_value: dt.datetime) -> dt.datetime:
    if dt_value.tzinfo is None or dt_value.tzinfo.utcoffset(dt_value) is None:
        raise ValueError("timestamp must be timezone-aware")
    return dt_value.astimezone(dt.UTC).replace(microsecond=0)


def _format_utc_z(dt_value: dt.datetime) -> str:
    return _utc(dt_value).isoformat().replace("+00:00", "Z")


class HttpInfo(BaseModel):
    """HTTP response details for a fetch."""

    model_config = ConfigDict(extra="forbid")

    url: str | None = None
    status_code: int | None = None
    headers: dict[str, str] = Field(default_factory=dict)


class DatasetVersionInfo(BaseModel):
    """Dataset-specific details recorded for an appended version."""

    model_config = ConfigDict(extra="forbid")

    rows_appended: int
    last_updated_column: str


class VersionInfo(BaseModel):
    """One appended dataset version.

    This represents a single append event (even if the fetch produced multiple
    files) and records the upstream timestamps and the HTTP request metadata.
    """

    model_config = ConfigDict(extra="forbid")

    agency_id: str
    key: Any = ""
    params: dict[str, Any] = Field(default_factory=dict)

    created_at: dt.datetime
    last_updated_at: dt.datetime
    last_updated_data_at: dt.datetime

    dataset: DatasetVersionInfo
    http: HttpInfo

    @field_validator("created_at", "last_updated_at", "last_updated_data_at")
    @classmethod
    def _validate_dt(cls, v: dt.datetime) -> dt.datetime:
        return _utc(v)

    @field_serializer("created_at", "last_updated_at", "last_updated_data_at")
    def _ser_dt(self, v: dt.datetime) -> str:
        return _format_utc_z(v)


class DatasetFiles(BaseModel):
    """Relative paths to dataset materializations within an output directory."""

    model_config = ConfigDict(extra="forbid")

    csv: str = "dataset.csv"
    parquet: str | None = None


class FilesInfo(BaseModel):
    """Relative paths to files produced by a dataset fetch."""

    model_config = ConfigDict(extra="forbid")

    datasets: DatasetFiles = Field(default_factory=DatasetFiles)
    # Map from dataset column name -> relative path to the codelist CSV.
    codelists: dict[str, str] = Field(default_factory=dict)


CodelistType = Literal["reference"]
CodelistKind = Literal["dimension", "attribute"]


class CodelistEntry(BaseModel):
    """One codelist used by a dataset column.

    A dataset may reference multiple codelists across its dimensions and
    attributes. Each entry maps a dataset column to a codelist file along with
    basic descriptive metadata.
    """

    model_config = ConfigDict(extra="forbid")

    codelist_id: str
    codelist_path: str
    codelist_type: CodelistType = "reference"
    codelist_kind: CodelistKind
    codelist_labels: dict[str, str] = Field(default_factory=dict)
    column_name: str
    column_pos: int

    @field_validator("codelist_id")
    @classmethod
    def _id_non_empty(cls, v: str) -> str:
        v2 = v.strip()
        if not v2:
            raise ValueError("codelist_id must be non-empty")
        return v2

    @field_validator("column_name")
    @classmethod
    def _col_non_empty(cls, v: str) -> str:
        v2 = v.strip()
        if not v2:
            raise ValueError("column_name must be non-empty")
        return v2

    @field_validator("column_pos")
    @classmethod
    def _pos_1_based(cls, v: int) -> int:
        if int(v) < 1:
            raise ValueError("column_pos must be 1-based (>= 1)")
        return int(v)


class Metadata(BaseModel):
    """Top-level metadata for a materialized dataset directory."""

    model_config = ConfigDict(extra="forbid")

    # First field at the top of the file.
    schema_version: int = SCHEMA_VERSION

    agency_id: str
    dataset_id: str
    key: Any = ""
    params: dict[str, Any] = Field(default_factory=dict)

    # First created stays constant.
    created_at: dt.datetime

    # Updated on each fetch (even if no new upstream version).
    last_fetched_at: dt.datetime

    # Updated only when new data is appended.
    last_updated_at: dt.datetime

    # The upstream "last updated" timestamp for the latest appended data.
    last_updated_data_at: dt.datetime | None = None

    files: FilesInfo = Field(default_factory=FilesInfo)
    versions: list[VersionInfo] = Field(default_factory=list)

    # Codelists used by the dataset (one entry per dataset column that references a codelist).
    codelists: list[CodelistEntry] = Field(default_factory=list)

    @field_validator("created_at", "last_fetched_at", "last_updated_at")
    @classmethod
    def _validate_dt(cls, v: dt.datetime) -> dt.datetime:
        return _utc(v)

    @field_validator("last_updated_data_at")
    @classmethod
    def _validate_opt_dt(cls, v: dt.datetime | None) -> dt.datetime | None:
        if v is None:
            return None
        return _utc(v)

    @field_serializer("created_at", "last_fetched_at", "last_updated_at")
    def _ser_dt(self, v: dt.datetime) -> str:
        return _format_utc_z(v)

    @field_serializer("last_updated_data_at")
    def _ser_opt_dt(self, v: dt.datetime | None) -> str | None:
        if v is None:
            return None
        return _format_utc_z(v)

    def latest_upstream_last_updated(self) -> dt.datetime | None:
        """Return the latest upstream "last updated" timestamp, if known."""
        return self.last_updated_data_at
