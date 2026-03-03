"""Download SDMX REST resources to local files.

This module is intentionally limited to:
- constructing an `sdmx1` client,
- calling `sdmx.Client.get(..., dry_run=True)` and streaming the HTTP response,
- writing a small `.meta.json` sidecar describing the HTTP response.

It does not parse SDMX payloads.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import sdmx

try:  # pragma: no cover
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore[assignment]

from .._types import IfExists
from ..errors import SdmxDownloadError, SdmxInterruptedError, SdmxTimeoutError, SdmxUnreachableError
from ..models import SdmxDownloadResult, SdmxRequest

_HTML_CONTENT_TYPES: Final[set[str]] = {"text/html", "application/xhtml+xml"}


def _safe_component(value: str) -> str:
    value2 = value.strip()
    value2 = re.sub(r"[^A-Za-z0-9._-]+", "_", value2)
    value2 = value2.strip("._-")
    return value2 or "_"


def _infer_extension(content_type: str | None, content_disposition: str | None) -> str:
    if content_disposition:
        match = re.search(
            r"filename=\"?([^\";]+)\"?",
            content_disposition,
            flags=re.IGNORECASE,
        )
        if match:
            ext = Path(match.group(1)).suffix
            if ext:
                return ext

    if not content_type:
        return ".sdmx"

    ct = content_type.lower().split(";", 1)[0].strip()
    if ct.endswith("+xml") or ct in {"application/xml", "text/xml"}:
        return ".xml"
    if ct.endswith("+json") or ct == "application/json":
        return ".json"
    if ct in {"application/zip", "application/x-zip-compressed"}:
        return ".zip"
    return ".sdmx"


class SdmxNativeDownloader:
    """Download raw SDMX responses to disk.

    This class is I/O focused and does not parse SDMX messages.
    """

    def __init__(
        self,
        *,
        source_id: str | None = None,
        base_url: str | None = None,
        timeout_seconds: float | None = None,
        session_opts: Mapping[str, object] | None = None,
        logger: logging.Logger | None = None,
        _client: object | None = None,
    ) -> None:
        self._source_id = source_id
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds
        self._session_opts = session_opts
        self._logger = logger

        self._client: Any

        if _client is not None:
            self._client = _client
            return

        if base_url and not source_id:
            raise ValueError("source_id is required when base_url is provided")

        if base_url and source_id:
            sdmx.add_source(
                json.dumps({"id": source_id, "name": source_id, "url": base_url, "supports": {}}),
                id=source_id,
                override=True,
            )

        opts: dict[str, Any] = {}
        if session_opts:
            opts.update(dict(session_opts))
        if timeout_seconds is not None:
            opts["timeout"] = timeout_seconds

        self._client = sdmx.Client(source_id, **opts) if source_id else sdmx.Client(**opts)

    def download(
        self,
        req: SdmxRequest,
        *,
        out_dir: str | Path,
        filename: str | None = None,
        if_exists: IfExists = "overwrite",
    ) -> SdmxDownloadResult:
        """Download the SDMX request to `out_dir`, returning the final file path."""
        out_path = Path(out_dir).expanduser()
        out_path.mkdir(parents=True, exist_ok=True)

        base_name = filename
        if not base_name:
            parts = [req.resource_type]
            if req.resource_id:
                parts.append(req.resource_id)
            base_name = "__".join(_safe_component(p) for p in parts)

        requested_suffix = Path(base_name).suffix
        tmp_path = out_path / f"{base_name}.part"
        final_path = out_path / base_name

        # Avoid stale partial artifacts from previous failed/aborted runs.
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

        if requested_suffix:
            if final_path.exists() and if_exists == "skip":
                return SdmxDownloadResult(
                    path=final_path,
                    url=None,
                    status_code=None,
                    content_type=None,
                    content_disposition=None,
                    downloaded_at=datetime.now(UTC),
                )
        else:
            if if_exists == "skip":
                for candidate in out_path.glob(f"{base_name}.*"):
                    if candidate.name.endswith(".meta.json"):
                        continue
                    if candidate.is_file():
                        return SdmxDownloadResult(
                            path=candidate,
                            url=None,
                            status_code=None,
                            content_type=None,
                            content_disposition=None,
                            downloaded_at=datetime.now(UTC),
                        )

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
        if req.resource_type == "data" and req.key is not None:
            get_kwargs["key"] = req.key

        def _peek_body() -> str:
            if not tmp_path.exists():
                return ""
            try:
                head = tmp_path.read_bytes()[:512]
            except OSError:
                return ""
            try:
                return head.decode("utf-8", errors="ignore").strip()
            except Exception:
                return ""

        def _cleanup_tmp() -> None:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

        def _redirect_chain() -> str | None:
            try:
                history = getattr(response, "history", None) or []
                if not history:
                    return None
                return " -> ".join(
                    f"{getattr(h, 'status_code', '')}:{getattr(h, 'url', '')}" for h in history
                )
            except Exception:
                return None

        def _log_extra() -> dict[str, object]:
            return {
                "component": "sdmx",
                "sdmx_source_id": self._source_id,
                "resource_type": req.resource_type,
                "resource_id": req.resource_id,
            }

        try:
            if self._logger is not None:
                references = None
                try:
                    references = (req.params or {}).get("references")
                except Exception:
                    references = None
                self._logger.debug(
                    "Starting SDMX download: %s/%s (references=%s out_dir=%s).",
                    req.resource_type,
                    req.resource_id or "",
                    references,
                    str(out_path),
                    extra={
                        **_log_extra(),
                        "out_dir": str(out_path),
                        "references": references,
                        "timeout_seconds": self._timeout_seconds,
                        "get_kwargs": str(get_kwargs),
                    },
                )

            request_obj = self._client.get(**get_kwargs)

            session = getattr(self._client, "session", None)
            if session is None:
                raise AttributeError("sdmx client has no 'session' attribute")

            # `sdmx1` may already return a `requests.PreparedRequest` when `dry_run=True`.
            # In that case, calling `requests.Session.prepare_request()` again fails.
            prepared = (
                session.prepare_request(request_obj)
                if hasattr(request_obj, "cookies")
                else request_obj
            )

            send_kwargs: dict[str, object] = {
                "stream": True,
                "allow_redirects": True,
            }
            if self._timeout_seconds is not None:
                send_kwargs["timeout"] = self._timeout_seconds

            response = session.send(prepared, **send_kwargs)

            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            with tmp_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        except KeyboardInterrupt as exc:
            _cleanup_tmp()
            if self._logger is not None:
                self._logger.warning("SDMX download interrupted by user.", extra=_log_extra())
            raise SdmxInterruptedError(
                f"SDMX download interrupted for {req.resource_type!r} "
                f"({req.source_id}/{req.resource_id})"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            _cleanup_tmp()
            if requests is not None:
                try:
                    if isinstance(exc, requests.exceptions.Timeout):  # type: ignore[attr-defined]
                        raise SdmxTimeoutError(
                            f"SDMX request timed out for {req.resource_type!r} "
                            f"(source={req.source_id!r})"
                        ) from exc
                    if isinstance(exc, requests.exceptions.ConnectionError):  # type: ignore[attr-defined]
                        raise SdmxUnreachableError(
                            f"SDMX server not reachable for {req.resource_type!r} "
                            f"(source={req.source_id!r})"
                        ) from exc
                except Exception:
                    pass
            if self._logger is not None:
                self._logger.exception(
                    "SDMX download failed (request could not be completed).",
                    extra={**_log_extra(), "get_kwargs": str(get_kwargs)},
                )
            raise SdmxDownloadError(
                f"SDMX download failed for {req.resource_type!r}: {exc}"
            ) from exc

        url = getattr(response, "url", None)
        status_code = getattr(response, "status_code", None)
        headers = getattr(response, "headers", {}) or {}
        content_type = headers.get("Content-Type") or headers.get("content-type")
        content_disposition = headers.get("Content-Disposition") or headers.get(
            "content-disposition"
        )

        if self._logger is not None:
            self._logger.debug(
                "SDMX response received.",
                extra={
                    **_log_extra(),
                    "url": url,
                    "status_code": status_code,
                    "content_type": content_type,
                },
            )

        if isinstance(status_code, int) and (status_code < 200 or status_code >= 300):
            snippet = _peek_body()
            _cleanup_tmp()
            redirects = _redirect_chain()
            extra = f" Redirects: {redirects!r}." if redirects else ""
            if self._logger is not None:
                self._logger.error(
                    "SDMX server returned non-2xx response.",
                    extra={
                        **_log_extra(),
                        "url": url,
                        "status_code": status_code,
                        "redirects": redirects,
                        "content_type": content_type,
                        "body_prefix": snippet[:200],
                    },
                )
            raise SdmxDownloadError(
                f"SDMX server returned HTTP {status_code} for {req.resource_type!r} "
                f"(url={url!r}).{extra} Body starts with: {snippet[:200]!r}"
            )

        ct = (content_type or "").lower().split(";", 1)[0].strip()
        if ct in _HTML_CONTENT_TYPES:
            snippet = _peek_body()
            _cleanup_tmp()
            redirects = _redirect_chain()
            if self._logger is not None:
                self._logger.error(
                    "SDMX server returned HTML payload.",
                    extra={
                        **_log_extra(),
                        "url": url,
                        "status_code": status_code,
                        "redirects": redirects,
                        "content_type": content_type,
                        "body_prefix": snippet[:200],
                    },
                )
            raise SdmxDownloadError(
                f"SDMX server returned HTML for {req.resource_type!r} (url={url!r}). "
                f"Body starts with: {snippet[:200]!r}"
            )

        # Content-Type is sometimes missing/incorrect; also sniff the file header.
        sniff = _peek_body().lower()
        if sniff.startswith("<!doctype html") or sniff.startswith("<html"):
            _cleanup_tmp()
            redirects = _redirect_chain()
            if self._logger is not None:
                self._logger.error(
                    "SDMX payload sniffed as HTML despite non-HTML Content-Type.",
                    extra={
                        **_log_extra(),
                        "url": url,
                        "status_code": status_code,
                        "redirects": redirects,
                        "content_type": content_type,
                        "body_prefix": sniff[:200],
                    },
                )
            raise SdmxDownloadError(
                f"Downloaded payload looks like HTML, not SDMX, for {req.resource_type!r} "
                f"(url={url!r})."
            )

        suffix = requested_suffix or _infer_extension(content_type, content_disposition)
        final_path = out_path / f"{base_name}{suffix}" if not requested_suffix else final_path

        final_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp_path, final_path)

        downloaded_at = datetime.now(UTC)
        result = SdmxDownloadResult(
            path=final_path,
            url=url,
            status_code=status_code,
            content_type=content_type,
            content_disposition=content_disposition,
            downloaded_at=downloaded_at,
        )

        sidecar = final_path.with_name(final_path.name + ".meta.json")
        sidecar.write_text(
            json.dumps(
                {
                    "downloaded_at": downloaded_at.isoformat(),
                    "request": asdict(req),
                    "response": {
                        "url": url,
                        "status_code": status_code,
                        "content_type": content_type,
                        "content_disposition": content_disposition,
                        "headers": {
                            "date": headers.get("Date") or headers.get("date"),
                            "last_modified": headers.get("Last-Modified")
                            or headers.get("last-modified"),
                            "etag": headers.get("ETag") or headers.get("etag"),
                        },
                    },
                },
                indent=2,
                sort_keys=True,
                default=str,
            )
            + "\n",
            encoding="utf-8",
        )

        if self._logger is not None:
            try:
                bytes_written = final_path.stat().st_size
            except OSError:
                bytes_written = None
            self._logger.debug(
                "SDMX payload saved: %s (bytes=%s status=%s).",
                str(final_path),
                bytes_written,
                status_code,
                extra={
                    **_log_extra(),
                    "path": str(final_path),
                    "sidecar": str(sidecar),
                    "bytes": bytes_written,
                    "url": url,
                    "status_code": status_code,
                },
            )

        return result
