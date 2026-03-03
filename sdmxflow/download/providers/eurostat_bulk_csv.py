"""Eurostat bulk CSV adapter.

This is a focused downloader for Eurostat's SDMX 3.0 dissemination endpoint
that returns CSV payloads.
"""

from __future__ import annotations

import gzip
import logging
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ..._types import IfExists
from ...errors import (
    SdmxDownloadError,
    SdmxInterruptedError,
    SdmxTimeoutError,
    SdmxUnreachableError,
)

# Eurostat SDMX 3.0 bulk data endpoint.
# Note the required `/data` segment (without it, the server returns 404).
_EUROSTAT_BASE: Final[str] = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data"
_USER_AGENT: Final[str] = "sdmxflow/0.1"
_PASSTHROUGH_PARAMS: Final[tuple[str, ...]] = (
    "lastTimePeriod",
    "firstTimePeriod",
    "startPeriod",
    "endPeriod",
    "updatedAfter",
)


def _build_key_string(key: str | Mapping[str, object] | None) -> str:
    """Build a stable SDMX key string.

    For dict keys we keep it deterministic (alphabetical order).
    """
    if key is None:
        return ""
    if isinstance(key, str):
        return key

    parts: list[str] = []
    for dim in sorted(key.keys()):
        value = key[dim]
        if value is None:
            parts.append("")
        elif isinstance(value, list):
            parts.append("+".join(map(str, value)))
        else:
            parts.append(str(value))
    return ".".join(parts)


def _to_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "t", "yes", "y"}:
            return True
        if v in {"0", "false", "f", "no", "n"}:
            return False
    return default


def _build_eurostat_bulk_url(
    *,
    dataset_id: str,
    key: str,
    params: Mapping[str, object] | None,
) -> str:
    """Build a Eurostat bulk URL for SDMX 3.0 dissemination."""
    params = dict(params or {})

    fmt = str(params.get("format") or "csvdata")
    fmt_version = str(params.get("formatVersion") or params.get("format_version") or "2.0")
    compress = _to_bool(params.get("compress"), default=True)
    lang = str(params.get("lang") or "EN")

    compress_str = "true" if compress else "false"
    url = (
        f"{_EUROSTAT_BASE}/dataflow/ESTAT/{dataset_id}/1.0/{key}?"
        f"format={fmt}&formatVersion={fmt_version}"
        f"&compress={compress_str}&lang={lang}"
    )

    for k in _PASSTHROUGH_PARAMS:
        if k in params and params[k] is not None:
            url += f"&{k}={params[k]}"

    return url


@dataclass(frozen=True)
class EurostatBulkCsvResult:
    """Result of a Eurostat bulk CSV download.

    Attributes:
        csv_path: Path to the final (decompressed) CSV file on disk.
        url: Fully expanded request URL.
        status_code: HTTP status code if available.
        headers: Response headers (lower-cased keys).
    """

    csv_path: Path
    url: str
    status_code: int | None
    headers: dict[str, str]


class EurostatBulkCsvDownloader:
    """Download dataset data via Eurostat bulk endpoint and materialize as CSV."""

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self._logger = logger
        self._timeout_seconds = timeout_seconds

    def download(
        self,
        *,
        dataset_id: str,
        out_path: str | Path,
        key: str | Mapping[str, object] | None = "",
        params: Mapping[str, object] | None = None,
        if_exists: IfExists = "skip",
        timeout_seconds: float | None = None,
    ) -> EurostatBulkCsvResult:
        """Download SDMX dataset data and write it as CSV (final, uncompressed)."""
        final_csv = Path(out_path).expanduser().resolve()
        final_csv.parent.mkdir(parents=True, exist_ok=True)

        if if_exists == "skip" and final_csv.exists() and final_csv.is_file():
            return EurostatBulkCsvResult(csv_path=final_csv, url="", status_code=None, headers={})

        params2 = dict(params or {})
        compress = _to_bool(params2.get("compress"), default=True)

        key_str = _build_key_string(key)
        url = _build_eurostat_bulk_url(
            dataset_id=str(dataset_id).strip(),
            key=key_str,
            params=params2,
        )

        # Download to a temporary file first.
        tmp_download = final_csv.with_name(final_csv.name + ".part")
        gz_download = final_csv.with_suffix(final_csv.suffix + ".gz")
        tmp_gz = gz_download.with_name(gz_download.name + ".part")

        def _cleanup(path: Path) -> None:
            if path.exists():
                try:
                    path.unlink()
                except OSError:
                    pass

        _cleanup(tmp_download)
        _cleanup(tmp_gz)

        timeout = timeout_seconds if timeout_seconds is not None else self._timeout_seconds

        if self._logger is not None:
            self._logger.debug(
                "Bulk downloading Eurostat CSV",
                extra={
                    "component": "sdmx",
                    "source_id": "ESTAT",
                    "dataset_id": dataset_id,
                    "out": str(final_csv),
                },
            )
            self._logger.debug(
                "Eurostat bulk request built.",
                extra={
                    "component": "sdmx",
                    "source_id": "ESTAT",
                    "dataset_id": dataset_id,
                    "url": url,
                    "compress": compress,
                    "timeout_seconds": timeout,
                    "tmp_download": str(tmp_download),
                    "tmp_gz": str(tmp_gz),
                },
            )
        status_code: int | None = None
        headers: dict[str, str] = {}
        try:
            target_tmp = tmp_gz if compress else tmp_download
            req = Request(url, headers={"User-Agent": _USER_AGENT})
            with urlopen(req, timeout=timeout) as resp:  # noqa: S310
                status_code = getattr(resp, "status", None)
                headers = {k.lower(): v for k, v in resp.headers.items()}
                with target_tmp.open("wb") as f:
                    while True:
                        chunk = resp.read(1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
        except KeyboardInterrupt as exc:
            _cleanup(tmp_download)
            _cleanup(tmp_gz)
            if self._logger is not None:
                self._logger.warning(
                    "Bulk download interrupted by user.",
                    extra={
                        "component": "sdmx",
                        "source_id": "ESTAT",
                        "dataset_id": dataset_id,
                        "url": url,
                    },
                )
            raise SdmxInterruptedError(f"Bulk download interrupted for ESTAT/{dataset_id}") from exc
        except HTTPError as exc:
            status_code = exc.code
            headers = {k.lower(): v for k, v in exc.headers.items()} if exc.headers else {}
            _cleanup(tmp_download)
            _cleanup(tmp_gz)
            raise SdmxDownloadError(
                f"Bulk download failed for ESTAT/{dataset_id} (url={url}, status={exc.code}): {exc}"
            ) from exc
        except URLError as exc:
            _cleanup(tmp_download)
            _cleanup(tmp_gz)
            reason = getattr(exc, "reason", None)
            # urllib wraps timeouts and connectivity issues into URLError(reason=...).
            if isinstance(reason, (TimeoutError, socket.timeout)) or getattr(
                reason, "errno", None
            ) in {110}:
                raise SdmxTimeoutError(
                    f"Bulk download timed out for ESTAT/{dataset_id} (url={url})"
                ) from exc
            # DNS failures (`socket.gaierror`) and connection refused/reset typically land here.
            raise SdmxUnreachableError(
                f"Eurostat server not reachable for ESTAT/{dataset_id} (url={url}): {exc}"
            ) from exc
        except TimeoutError as exc:
            _cleanup(tmp_download)
            _cleanup(tmp_gz)
            raise SdmxTimeoutError(
                f"Bulk download timed out for ESTAT/{dataset_id} (url={url})"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            _cleanup(tmp_download)
            _cleanup(tmp_gz)
            raise SdmxDownloadError(
                f"Bulk download failed for ESTAT/{dataset_id} (url={url}): {exc}"
            ) from exc

        if compress:
            if self._logger is not None:
                self._logger.debug(
                    "Decompressing gz bulk download.",
                    extra={
                        "component": "sdmx",
                        "source_id": "ESTAT",
                        "dataset_id": dataset_id,
                        "gz": str(gz_download),
                    },
                )
            if tmp_gz.exists():
                tmp_gz.replace(gz_download)

            _cleanup(tmp_download)
            try:
                with gzip.open(gz_download, "rb") as src_stream, tmp_download.open("wb") as out:
                    while True:
                        buf = src_stream.read(1024 * 1024)
                        if not buf:
                            break
                        out.write(buf)
                tmp_download.replace(final_csv)
            except KeyboardInterrupt as exc:
                _cleanup(tmp_download)
                raise SdmxInterruptedError(
                    f"Bulk download interrupted during decompression for ESTAT/{dataset_id}"
                ) from exc
            except Exception as exc:  # noqa: BLE001
                _cleanup(tmp_download)
                raise SdmxDownloadError(
                    f"Bulk download gzip decompression failed for ESTAT/{dataset_id}: {exc}"
                ) from exc
            finally:
                _cleanup(gz_download)
        else:
            tmp_download.replace(final_csv)

        if not final_csv.exists() or not final_csv.is_file() or final_csv.stat().st_size <= 0:
            raise SdmxDownloadError(f"Bulk download created no CSV (or empty file) at {final_csv}")

        if self._logger is not None:
            self._logger.debug(
                "Bulk download complete",
                extra={
                    "component": "sdmx",
                    "source_id": "ESTAT",
                    "dataset_id": dataset_id,
                    "bytes": final_csv.stat().st_size,
                },
            )

        return EurostatBulkCsvResult(
            csv_path=final_csv, url=url, status_code=status_code, headers=headers
        )

    def download_csv(
        self,
        *,
        dataset_id: str,
        out_path: str | Path,
        key: str | Mapping[str, object] | None = "",
        params: Mapping[str, object] | None = None,
        if_exists: IfExists = "skip",
        timeout_seconds: float | None = None,
    ) -> Path:
        """Backward-compatible helper: returns only the CSV path."""
        return self.download(
            dataset_id=dataset_id,
            out_path=out_path,
            key=key,
            params=params,
            if_exists=if_exists,
            timeout_seconds=timeout_seconds,
        ).csv_path
