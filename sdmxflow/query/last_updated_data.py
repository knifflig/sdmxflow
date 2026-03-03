"""Fast last-updated queries.

Goal: detect whether a dataset has changed upstream.

For Eurostat, this timestamp is exposed as SDMX Annotations on the *Dataflow*:
- UPDATE_DATA (preferred)
- DISSEMINATION_TIMESTAMP_DATA (fallback)

We intentionally avoid `sdmx.read_sdmx(...)`: scanning a small XML payload is
significantly faster than building the full SDMX object graph.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any
from xml.etree import ElementTree as ET

from ..download.native import SdmxNativeDownloader
from ..errors import SdmxDownloadError, SdmxInterruptedError, SdmxTimeoutError, SdmxUnreachableError
from ..models import SdmxRequest


@dataclass(frozen=True)
class LastUpdatedInfo:
    """Result of a last-updated query for a dataset.

    Attributes:
        source_id: SDMX source identifier (e.g. `ESTAT`).
        dataset_id: Dataset identifier.
        updated_at: Upstream timestamp in UTC.
    """

    source_id: str
    dataset_id: str
    updated_at: dt.datetime


def _local(tag: str) -> str:
    # '{namespace}LocalName' -> 'LocalName'
    return tag.rsplit("}", 1)[-1]


def _parse_sdmx_timestamp(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    s = value.strip()
    if not s:
        return None

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # Eurostat commonly uses +0200 (no colon) offsets.
    if len(s) >= 5 and (s[-5] in {"+", "-"}) and (":" not in s[-5:]):
        s = s[:-2] + ":" + s[-2:]

    try:
        parsed = dt.datetime.fromisoformat(s)
    except ValueError:
        try:
            parsed = dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            return None

    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        return None
    return parsed.astimezone(dt.UTC)


def extract_last_updated_data_from_dataflow_xml(xml_bytes: bytes) -> dt.datetime | None:
    """Extract last-updated timestamp from a Dataflow SDMX-ML payload."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None

    preferred_types = ["UPDATE_DATA", "DISSEMINATION_TIMESTAMP_DATA"]
    best: dict[str, str] = {}

    for ann in root.iter():
        if _local(ann.tag) != "Annotation":
            continue

        ann_type: str | None = None
        ann_title: str | None = None

        for child in list(ann):
            name = _local(child.tag)
            if name == "AnnotationType":
                if child.text:
                    ann_type = child.text.strip()
            elif name == "AnnotationTitle":
                if child.text:
                    ann_title = child.text.strip()

        if ann_type and ann_title and ann_type in preferred_types:
            best.setdefault(ann_type, ann_title)

    for t in preferred_types:
        parsed = _parse_sdmx_timestamp(best.get(t))
        if parsed is not None:
            return parsed

    return None


def eurostat_last_updated(
    *,
    dataset_id: str,
    logger: logging.Logger | None = None,
    timeout_seconds: float | None = None,
) -> LastUpdatedInfo:
    """Return Eurostat upstream `updated_at` for `dataset_id`."""
    dataset_id = str(dataset_id).strip()

    if logger is not None:
        logger.debug(
            "Querying Eurostat last-updated (dataflow annotations)",
            extra={"component": "sdmx", "source_id": "ESTAT", "dataset_id": dataset_id},
        )

    native = SdmxNativeDownloader(source_id="ESTAT", timeout_seconds=timeout_seconds, logger=logger)

    # Build a minimal dataflow request.
    req = SdmxRequest(
        source_id="ESTAT",
        resource_type="dataflow",
        resource_id=dataset_id,
        params={"references": "none"},
    )

    client: Any = native._client  # noqa: SLF001
    session: Any = getattr(client, "session", None)
    if session is None:
        raise SdmxDownloadError("sdmx client has no 'session' attribute")

    get_kwargs: dict[str, Any] = {
        "resource_type": req.resource_type,
        "resource_id": req.resource_id,
        "dry_run": True,
    }
    if req.params:
        get_kwargs.update(req.params)
    if req.provider is not None:
        get_kwargs["provider"] = req.provider
    if req.version is not None:
        get_kwargs["version"] = req.version
    if req.force:
        get_kwargs["force"] = True

    try:
        request_obj = client.get(**get_kwargs)
        prepared = (
            session.prepare_request(request_obj) if hasattr(request_obj, "cookies") else request_obj
        )
        resp = session.send(prepared, stream=False, timeout=timeout_seconds)
    except KeyboardInterrupt as exc:
        if logger is not None:
            logger.warning(
                "Last-updated query interrupted by user.",
                extra={"component": "sdmx", "source_id": "ESTAT", "dataset_id": dataset_id},
            )
        raise SdmxInterruptedError(
            f"Last-updated query interrupted for ESTAT/{dataset_id}"
        ) from exc
    except Exception as exc:  # noqa: BLE001
        # Best-effort classification for common network errors.
        try:  # pragma: no cover
            import requests

            if isinstance(exc, requests.exceptions.Timeout):
                raise SdmxTimeoutError(
                    f"Last-updated query timed out for ESTAT/{dataset_id}"
                ) from exc
            if isinstance(exc, requests.exceptions.ConnectionError):
                raise SdmxUnreachableError(
                    f"Eurostat server not reachable for ESTAT/{dataset_id}"
                ) from exc
        except Exception:
            pass
        raise SdmxDownloadError(
            f"Failed to request dataflow for last_updated_data ESTAT/{dataset_id}: {exc}"
        ) from exc

    status = getattr(resp, "status_code", None)
    if status is None or int(status) >= 400:
        url = getattr(resp, "url", None)
        raise SdmxDownloadError(
            f"SDMX dataflow request failed for ESTAT/{dataset_id}: status={status} url={url}"
        )

    content: bytes = getattr(resp, "content", b"") or b""
    if logger is not None:
        logger.debug(
            "Last-updated payload received.",
            extra={
                "component": "sdmx",
                "source_id": "ESTAT",
                "dataset_id": dataset_id,
                "status_code": status,
                "bytes": len(content),
            },
        )
    updated_at = extract_last_updated_data_from_dataflow_xml(content)
    if updated_at is None:
        raise SdmxDownloadError(
            f"Could not extract last_updated_data from dataflow annotations for ESTAT/{dataset_id}"
        )

    return LastUpdatedInfo(source_id="ESTAT", dataset_id=dataset_id, updated_at=updated_at)
