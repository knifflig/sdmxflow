"""Metadata I/O.

Implements a stable `metadata.json` file using an explicit Pydantic v2 schema.

This module is intentionally the *only* place that reads/writes the metadata
JSON representation.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from .._json import read_json
from .._types import JsonValue
from ..errors import SdmxMetadataError
from .models import CodelistEntry, HttpInfo, Metadata, VersionInfo

_logger = logging.getLogger("sdmxflow")


def format_utc_iso(ts: dt.datetime) -> str:
    """Format a timezone-aware datetime as canonical UTC `...Z`."""
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        raise SdmxMetadataError("timestamp must be timezone-aware")
    return ts.astimezone(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(microsecond=0)


def _write_json_unsorted(path: Path, data: JsonValue, *, indent: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=indent, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _utc(dt_value: dt.datetime) -> dt.datetime:
    if dt_value.tzinfo is None or dt_value.tzinfo.utcoffset(dt_value) is None:
        raise ValueError("timestamp must be timezone-aware")
    return dt_value.astimezone(dt.UTC).replace(microsecond=0)


def _format_utc_z(dt_value: dt.datetime) -> str:
    return _utc(dt_value).isoformat().replace("+00:00", "Z")


class _LegacyHttpInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str | None = None
    status_code: int | None = None
    headers: dict[str, str] = Field(default_factory=dict)


class _LegacyDatasetVersionInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rows_appended: int
    last_updated_column: str


class _LegacyVersionInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    upstream_last_updated: dt.datetime
    fetched_at: dt.datetime
    http: _LegacyHttpInfo
    dataset: _LegacyDatasetVersionInfo

    @field_validator("upstream_last_updated", "fetched_at")
    @classmethod
    def _validate_dt(cls, v: dt.datetime) -> dt.datetime:
        return _utc(v)

    @field_serializer("upstream_last_updated", "fetched_at")
    def _ser_dt(self, v: dt.datetime) -> str:
        return _format_utc_z(v)


class _LegacyFilesInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_csv: str = "dataset.csv"
    codelists_dir: str = "codelists"


class _LegacyMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int
    created_at: dt.datetime
    updated_at: dt.datetime

    source_id: str
    dataset_id: str
    agency_id: str | None = None
    key: Any = ""
    params: dict[str, Any] = Field(default_factory=dict)

    files: _LegacyFilesInfo = Field(default_factory=_LegacyFilesInfo)
    versions: list[_LegacyVersionInfo] = Field(default_factory=list)
    codelists: list[CodelistEntry] = Field(default_factory=list)

    @field_validator("created_at", "updated_at")
    @classmethod
    def _validate_dt(cls, v: dt.datetime) -> dt.datetime:
        return _utc(v)

    @field_serializer("created_at", "updated_at")
    def _ser_dt(self, v: dt.datetime) -> str:
        return _format_utc_z(v)


def load_metadata(path: Path) -> Metadata | None:
    """Load and validate `metadata.json`.

    Args:
        path: Path to the JSON metadata file.

    Returns:
        A validated `Metadata` instance, or `None` if the file does not exist.

    Raises:
        SdmxMetadataError: If the file exists but cannot be validated (including
            legacy migration failures).
    """
    if not path.exists():
        return None
    data = read_json(path)
    try:
        return Metadata.model_validate(data)
    except Exception:
        pass

    # Best-effort legacy schema migration.
    try:
        legacy = _LegacyMetadata.model_validate(data)
    except Exception as exc:  # noqa: BLE001
        raise SdmxMetadataError(f"Invalid metadata.json: {path}: {exc}") from exc

    agency_id = legacy.agency_id or legacy.source_id
    versions_new: list[VersionInfo] = []
    for v in legacy.versions:
        versions_new.append(
            VersionInfo(
                agency_id=agency_id,
                key=legacy.key,
                params=dict(legacy.params),
                created_at=v.fetched_at,
                last_updated_at=v.fetched_at,
                last_updated_data_at=v.upstream_last_updated,
                dataset=v.dataset.model_dump(),
                http=HttpInfo(
                    url=v.http.url,
                    status_code=v.http.status_code,
                    headers=dict(v.http.headers or {}),
                ),
            )
        )

    last_fetched = legacy.versions[-1].fetched_at if legacy.versions else legacy.updated_at
    last_updated = legacy.versions[-1].fetched_at if legacy.versions else legacy.updated_at
    last_updated_data = legacy.versions[-1].upstream_last_updated if legacy.versions else None

    files_codelists: dict[str, str] = {e.column_name: e.codelist_path for e in legacy.codelists}
    meta = Metadata(
        schema_version=legacy.schema_version,
        agency_id=agency_id,
        dataset_id=legacy.dataset_id,
        key=legacy.key,
        params=dict(legacy.params),
        created_at=legacy.created_at,
        last_fetched_at=last_fetched,
        last_updated_at=last_updated,
        last_updated_data_at=last_updated_data,
        files={"datasets": {"csv": legacy.files.dataset_csv}, "codelists": files_codelists},
        versions=versions_new,
        codelists=legacy.codelists,
    )
    return meta


def init_metadata(
    *,
    agency_id: str,
    dataset_id: str,
    key: JsonValue,
    params: Mapping[str, JsonValue],
) -> Metadata:
    """Create a new `Metadata` object for a dataset output directory.

    Args:
        agency_id: Dataset agency identifier.
        dataset_id: Dataset identifier.
        key: Dataset key (string or mapping; stored as JSON).
        params: Request parameters stored as JSON.

    Returns:
        A new `Metadata` instance with timestamps initialized to now (UTC).
    """
    now = _now_utc()
    return Metadata(
        created_at=now,
        last_fetched_at=now,
        last_updated_at=now,
        dataset_id=str(dataset_id),
        agency_id=str(agency_id),
        key=key,
        params=dict(params),
    )


def upsert_top_level(
    metadata: Metadata,
    *,
    agency_id: str,
    dataset_id: str,
    key: JsonValue,
    params: Mapping[str, JsonValue],
) -> None:
    """Update top-level identifying fields in-place.

    This function updates the dataset identity (agency, dataset id, key and
    params) on an existing `Metadata` instance. It intentionally does not change
    any timestamps by itself.
    """
    new_agency_id = str(agency_id)
    new_dataset_id = str(dataset_id)
    new_params = dict(params)
    changed = (
        metadata.agency_id != new_agency_id
        or metadata.dataset_id != new_dataset_id
        or metadata.key != key
        or metadata.params != new_params
    )

    metadata.agency_id = new_agency_id
    metadata.dataset_id = new_dataset_id
    metadata.key = key
    metadata.params = new_params
    if changed:
        # Intentional: changing query metadata does not imply data update.
        pass


def mark_fetched(metadata: Metadata, *, fetched_at: dt.datetime | None = None) -> None:
    """Update `metadata.last_fetched_at` to reflect a completed fetch."""
    metadata.last_fetched_at = fetched_at or _now_utc()


def latest_upstream_last_updated(metadata: Metadata) -> str | None:
    """Return the latest upstream last-updated timestamp as a `...Z` string."""
    latest = metadata.latest_upstream_last_updated()
    if latest is None:
        return None
    return format_utc_iso(latest)


def append_version(
    metadata: Metadata,
    *,
    upstream_last_updated: str,
    fetched_at: dt.datetime,
    http_url: str | None,
    http_status_code: int | None,
    http_headers: Mapping[str, str] | None,
    rows_appended: int,
    last_updated_column: str,
) -> None:
    """Append a new version entry and update top-level timestamps.

    Args:
        metadata: Metadata object to mutate.
        upstream_last_updated: Upstream timestamp string (ISO 8601; `...Z` is accepted).
        fetched_at: Local fetch completion time (timezone-aware).
        http_url: Final request URL.
        http_status_code: HTTP status code.
        http_headers: HTTP response headers.
        rows_appended: Number of dataset rows appended.
        last_updated_column: Name of the `last_updated` column in the dataset CSV.

    Raises:
        SdmxMetadataError: If `upstream_last_updated` is not a valid ISO timestamp.
    """
    try:
        up_dt = dt.datetime.fromisoformat(upstream_last_updated.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SdmxMetadataError(
            f"Invalid upstream_last_updated: {upstream_last_updated!r}"
        ) from exc

    # Append/update implies a fetch.
    metadata.last_fetched_at = fetched_at
    metadata.last_updated_at = fetched_at
    metadata.last_updated_data_at = up_dt

    metadata.versions.append(
        VersionInfo(
            agency_id=metadata.agency_id,
            key=metadata.key,
            params=dict(metadata.params),
            created_at=fetched_at,
            last_updated_at=fetched_at,
            last_updated_data_at=up_dt,
            http=HttpInfo(
                url=http_url, status_code=http_status_code, headers=dict(http_headers or {})
            ),
            dataset={
                "rows_appended": int(rows_appended),
                "last_updated_column": str(last_updated_column),
            },
        )
    )


def set_codelists(metadata: Metadata, codelists: list[CodelistEntry]) -> None:
    """Replace codelist entries and update the `files.codelists` mapping."""
    metadata.codelists = list(codelists)
    metadata.files.codelists = {e.column_name: e.codelist_path for e in metadata.codelists}


def save_metadata(path: Path, metadata: Metadata | Mapping[str, Any]) -> None:
    """Persist metadata to disk.

    Accepts either a typed `Metadata` instance (preferred) or a plain mapping for
    backwards compatibility with older call sites/tests.
    """
    try:
        if _logger.isEnabledFor(logging.DEBUG):
            extra: dict[str, object] = {
                "component": "sdmxflow",
                "path": str(path),
            }
            if isinstance(metadata, Metadata):
                extra.update(
                    {
                        "schema_version": metadata.schema_version,
                        "versions": len(metadata.versions),
                        "codelists": len(metadata.codelists),
                    }
                )
            if isinstance(metadata, Metadata):
                _logger.debug(
                    "Saving metadata.json (%s) (schema_version=%s versions=%d codelists=%d).",
                    str(path),
                    metadata.schema_version,
                    len(metadata.versions),
                    len(metadata.codelists),
                    extra=extra,
                )
            else:
                _logger.debug("Saving metadata.json (%s).", str(path), extra=extra)

        if isinstance(metadata, Metadata):
            payload: dict[str, Any] = metadata.model_dump(mode="json", exclude_none=True)
        else:
            payload = dict(metadata)
        _write_json_unsorted(path, payload)

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Saved metadata.json (%s) (bytes=%d).",
                str(path),
                path.stat().st_size,
                extra={"component": "sdmxflow", "path": str(path), "bytes": path.stat().st_size},
            )
    except TypeError as exc:
        raise SdmxMetadataError(f"Metadata is not JSON-serializable: {exc}") from exc
