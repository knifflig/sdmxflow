"""Core data contracts used across sdmxflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from ._types import JsonValue


@dataclass(frozen=True)
class SdmxRequest:
    """A single SDMX REST request to be downloaded as a native file."""

    source_id: str
    resource_type: str
    resource_id: str | None = None
    key: str | dict[str, JsonValue] | None = None
    params: dict[str, JsonValue] | None = None
    provider: str | None = None
    version: str | None = None
    force: bool = False


@dataclass(frozen=True)
class SdmxDownloadResult:
    """Result metadata for a downloaded SDMX response."""

    path: Path
    url: str | None
    status_code: int | None
    content_type: str | None
    content_disposition: str | None
    downloaded_at: datetime


@dataclass(frozen=True)
class FlowStructureArtifacts:
    """Common SDMX structure files for a dataset."""

    dataflow: Path
    datastructure: Path


__all__ = [
    "FlowStructureArtifacts",
    "SdmxDownloadResult",
    "SdmxRequest",
]
