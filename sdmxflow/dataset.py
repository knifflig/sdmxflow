"""User-facing dataset downloader entrypoint.

This module contains the main high-level API for downloading SDMX datasets into
an on-disk folder layout suitable for analytics and repeatable refreshes.

The primary entrypoint is :class:`SdmxDataset`.

Quickstart:

    >>> from pathlib import Path
    >>> from sdmxflow.dataset import SdmxDataset
    >>> ds = SdmxDataset(
    ...     out_dir=Path("./out/lfsa_egai2d"),
    ...     source_id="ESTAT",
    ...     dataset_id="lfsa_egai2d",
    ...     # Optional: agency_id defaults to source_id for ESTAT
    ...     agency_id="ESTAT",
    ...     # Optional: restrict the dataset via a SDMX key
    ...     key="....",  # see provider docs
    ...     # Optional: provider-specific passthrough params
    ...     params={"compressed": True},
    ...     # Optional: save a per-run DEBUG log file under <out_dir>/logs/
    ...     save_logs=True,
    ... )
    >>> result = ds.fetch()
    >>> result.appended
    True

Folder layout:

    <out_dir>/
        dataset.csv        # appended over time, with a leading last_updated column
        metadata.json      # dataset metadata and version history
        codelists/         # codelists used by the dataset columns
        logs/              # optional per-run log files when save_logs=True

Logging:

The dataset fetch is designed to be user-friendly at INFO level. The fetch
workflow emits exactly three INFO messages per call:

1) fetch intention (what will be fetched and where),
2) version decision (download vs. already up to date),
3) completion summary (paths of written artifacts).

All other details are emitted at DEBUG level.

Notes:
* Currently only the Eurostat ("ESTAT") source is implemented.
* Network and user-cancel exceptions are raised as typed sdmxflow errors.
"""

from __future__ import annotations

import csv
import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path

from ._csv import LAST_UPDATED_COLUMN, append_version_slice
from ._logging import get_logger
from ._paths import DatasetPaths, dataset_paths
from .download.native import SdmxNativeDownloader
from .download.providers.eurostat_bulk_csv import EurostatBulkCsvDownloader
from .download.structures import SdmxStructureDownloader
from .errors import (
    SdmxDownloadError,
    SdmxInterruptedError,
    SdmxTimeoutError,
    SdmxUnreachableError,
)
from .extract.codelists import write_codelists_csvs
from .metadata.models import CodelistEntry, Metadata
from .metadata.writer import (
    append_version,
    format_utc_iso,
    init_metadata,
    latest_upstream_last_updated,
    load_metadata,
    mark_fetched,
    save_metadata,
    set_codelists,
    upsert_top_level,
)
from .query.last_updated_data import eurostat_last_updated


@dataclass(frozen=True)
class FetchResult:
    """Result returned by :meth:`SdmxDataset.fetch`.

    Attributes:
        out_dir: Output directory used for this fetch.
        dataset_csv: Path to the dataset CSV (appended in-place across versions).
        metadata_json: Path to the metadata JSON for the dataset.
        codelists_dir: Directory containing generated codelists CSVs.
        appended: Whether a new upstream version was downloaded and appended.
    """

    out_dir: Path
    dataset_csv: Path
    metadata_json: Path
    codelists_dir: Path
    appended: bool


class SdmxDataset:
    """User-facing dataset downloader.

    This class is intended to be the main entrypoint for sdmxflow users.

    It encapsulates a standard workflow:

    1) Configure an instance (output folder, identifiers, optional key/params).
    2) Call :meth:`setup` (optional) to create the output folder layout.
    3) Call :meth:`fetch` to download *only* when upstream data changed.

    The downloader is append-only: when the upstream dataset changes, a new
    slice is appended to the local :attr:`paths.dataset_csv`.

    Examples:
        Basic usage:

        >>> from sdmxflow.dataset import SdmxDataset
        >>> ds = SdmxDataset(out_dir="./out", source_id="ESTAT", dataset_id="lfsa_egai2d")
        >>> result = ds.fetch()
        >>> print(result.dataset_csv)

        Capture a per-run log file:

        >>> ds = SdmxDataset(
        ...     out_dir="./out",
        ...     source_id="ESTAT",
        ...     dataset_id="lfsa_egai2d",
        ...     save_logs=True,
        ... )
        >>> _ = ds.fetch()

    Attributes:
        out_dir: Output directory (root folder for all artifacts).
        source_id: Provider/source identifier (currently only "ESTAT").
        dataset_id: Dataset identifier within the source/provider.
        agency_id: Optional SDMX agency identifier. Defaults to source_id.
        key: Optional SDMX key (provider-specific format).
        params: Optional passthrough query parameters.
        paths: Computed filesystem paths under :attr:`out_dir`.
    """

    def __init__(
        self,
        *,
        out_dir: str | Path,
        source_id: str,
        dataset_id: str,
        agency_id: str | None = None,
        key: str | dict[str, object] | None = "",
        params: dict[str, object] | None = None,
        logger: logging.Logger | None = None,
        save_logs: bool = False,
    ) -> None:
        """Initialize a dataset downloader.

        Args:
            out_dir: Output directory where artifacts will be written.
            source_id: Provider/source id (currently only "ESTAT").
            dataset_id: Dataset id within the provider.
            agency_id: Optional SDMX agency id. For "ESTAT" this defaults to
                the value of source_id.
            key: Optional SDMX key. For Eurostat bulk downloads this may be a
                provider-specific string or mapping. Use ``None`` to request the
                full dataset, or an empty string for provider defaults.
            params: Optional provider-specific query parameters. These are
                treated as passthrough parameters for the underlying downloader.
            logger: Optional logger. If omitted, sdmxflow will use its default
                logger configuration.
            save_logs: If True, writes a per-run log file under ``<out_dir>/logs``.
                The file handler is attached for the duration of :meth:`fetch`
                and safely detached/closed in a ``finally`` block.

        Raises:
            ValueError: If any required identifiers are invalid after coercion.
        """
        self.out_dir = Path(out_dir)
        self.source_id = str(source_id)
        self.dataset_id = str(dataset_id)
        self.agency_id = str(agency_id) if agency_id is not None else None
        self.key = key
        self.params = dict(params or {})
        self._logger = get_logger(logger)
        self._save_logs = bool(save_logs)

        self.paths: DatasetPaths = dataset_paths(out_dir)

    def _attach_file_logger(
        self, *, agency_id: str
    ) -> tuple[logging.Handler | None, int | None, Path | None]:
        """Attach a per-run file handler under ``<out_dir>/logs``.

        This is an internal helper used by :meth:`fetch`.

        If :attr:`_save_logs` is disabled, this returns ``(None, None, None)``.

        Args:
            agency_id: Agency identifier used to namespace the log filename.

        Returns:
            A tuple ``(handler, previous_logger_level, log_path)``.

            - ``handler`` is the file handler that was attached (or None).
            - ``previous_logger_level`` is the logger level before modification
              (or None if no modification occurred).
            - ``log_path`` is the filesystem path of the log file (or None).

        Notes:
            The caller is responsible for removing/closing the handler and
            restoring the logger level, typically via :meth:`_detach_file_logger`.
        """
        if not self._save_logs:
            return (None, None, None)

        logs_dir = self.paths.out_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # One log file per fetch run.
        ts = dt.datetime.now(dt.UTC).replace(microsecond=0)
        ts_str = ts.isoformat().replace(":", "").replace("+00:00", "Z")
        agency = str(agency_id).strip().lower() or "_"
        dataset = str(self.dataset_id).strip().lower() or "_"
        log_path = logs_dir / f"{agency}__{dataset}__{ts_str}.log"

        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))

        prev_level: int | None = None
        # If caller didn't configure the logger at all, ensure we actually emit
        # logs for the file handler (without forcing console verbosity).
        if self._logger.level == logging.NOTSET:
            prev_level = self._logger.level
            self._logger.setLevel(logging.DEBUG)

        self._logger.addHandler(handler)
        return (handler, prev_level, log_path)

    def _detach_file_logger(self, handler: logging.Handler | None, prev_level: int | None) -> None:
        """Detach and close a previously-attached file handler.

        This is best-effort cleanup:

        - Removing the handler from the logger is attempted.
        - The handler is closed.
        - The previous logger level is restored if it was changed.

        Any errors during cleanup are swallowed to avoid masking the original
        exception from :meth:`fetch`.

        Args:
            handler: The handler returned by :meth:`_attach_file_logger`.
            prev_level: The previous logger level returned by
                :meth:`_attach_file_logger`.
        """
        if handler is None:
            return
        try:
            self._logger.removeHandler(handler)
        except Exception:
            pass
        try:
            handler.close()
        except Exception:
            pass
        if prev_level is not None:
            try:
                self._logger.setLevel(prev_level)
            except Exception:
                pass

    def setup(self) -> None:
        """Create the output folder layout.

        This is safe to call multiple times.

        The following directories are created:

        - ``<out_dir>/`` (root)
        - ``<out_dir>/codelists/``

        Notes:
            This method currently only performs filesystem setup. Validation of
            identifiers and remote availability happens during :meth:`fetch`.
        """
        self.paths.out_dir.mkdir(parents=True, exist_ok=True)
        self.paths.codelists_dir.mkdir(parents=True, exist_ok=True)
        self._logger.debug(
            "sdmxflow dataset setup complete", extra={"out_dir": str(self.paths.out_dir)}
        )

    def fetch(self) -> FetchResult:
        """Fetch data and append a new version only when upstream changed.

        This method performs the full workflow:

        - Ensure local directory structure exists.
        - Query upstream "last updated" metadata.
        - Compare upstream timestamp to the latest locally recorded timestamp.
        - If changed: download the dataset and append it to ``dataset.csv``.
        - Ensure codelists are present and update ``metadata.json``.

        Returns:
            A :class:`FetchResult` describing artifact locations and whether a
            new version was appended.

        Raises:
            SdmxDownloadError: If the source is unsupported or a download fails.
            SdmxTimeoutError: If a network operation times out.
            SdmxUnreachableError: If the upstream host cannot be reached.
            SdmxInterruptedError: If the user interrupts the fetch.

        Logging:
            INFO level emits exactly three user-facing messages per call:

            1) Fetch requested (intent + output folder)
            2) Version decision (download vs. already up to date)
            3) Completion summary (paths to dataset/metadata/codelists)

            All other details are logged at DEBUG level.
        """
        source = self.source_id.strip().upper()
        if source != "ESTAT":
            raise SdmxDownloadError(
                f"Unsupported source_id={self.source_id!r}. Only 'ESTAT' is implemented."
            )

        agency_id = self.agency_id or source

        handler, prev_level, _log_path = self._attach_file_logger(agency_id=agency_id)
        try:
            # INFO (1/3): intention / what we are about to do.
            self._logger.info(
                "Fetch requested: agency_id=%s dataset_id=%s out_dir=%s",
                agency_id,
                self.dataset_id,
                str(self.paths.out_dir),
                extra={
                    "component": "sdmxflow",
                    "agency_id": agency_id,
                    "dataset_id": self.dataset_id,
                },
            )

            self._logger.debug(
                "Fetch started.",
                extra={
                    "component": "sdmxflow",
                    "source_id": self.source_id,
                    "dataset_id": self.dataset_id,
                    "out_dir": str(self.paths.out_dir),
                },
            )

            try:
                self.setup()
            except KeyboardInterrupt as exc:
                raise SdmxInterruptedError("Dataset fetch interrupted by user") from exc

            last_updated_info = eurostat_last_updated(
                dataset_id=self.dataset_id,
                logger=self._logger,
            )
            upstream_last_updated_str = format_utc_iso(last_updated_info.updated_at)
            self._logger.debug(
                "Upstream last_updated_data resolved.",
                extra={
                    "component": "sdmxflow",
                    "source_id": source,
                    "dataset_id": self.dataset_id,
                    "upstream_last_updated": upstream_last_updated_str,
                },
            )

            metadata = load_metadata(self.paths.metadata_json)
            if metadata is None:
                self._logger.debug(
                    "No metadata.json found; initializing new metadata.",
                    extra={"component": "sdmxflow", "dataset_id": self.dataset_id},
                )
                metadata = init_metadata(
                    dataset_id=self.dataset_id,
                    agency_id=agency_id,
                    key=self.key,  # type: ignore[arg-type]
                    params=self.params,  # type: ignore[arg-type]
                )
            else:
                self._logger.debug(
                    "Loaded existing metadata.json.",
                    extra={"component": "sdmxflow", "dataset_id": self.dataset_id},
                )
                upsert_top_level(
                    metadata,
                    dataset_id=self.dataset_id,
                    agency_id=agency_id,
                    key=self.key,  # type: ignore[arg-type]
                    params=self.params,  # type: ignore[arg-type]
                )

            # Always bump last_fetched_at on each fetch attempt.
            mark_fetched(metadata)

            latest = latest_upstream_last_updated(metadata)
            will_append = latest != upstream_last_updated_str
            self._logger.debug(
                (
                    "Version check: local_last_updated_data=%s "
                    "upstream_last_updated_data=%s will_append=%s"
                ),
                latest,
                upstream_last_updated_str,
                will_append,
                extra={"component": "sdmxflow", "dataset_id": self.dataset_id},
            )
            # INFO (2/3): decision.
            if will_append:
                self._logger.info(
                    "New upstream version available: local=%s upstream=%s; downloading.",
                    latest,
                    upstream_last_updated_str,
                    extra={"component": "sdmxflow", "dataset_id": self.dataset_id},
                )
            else:
                self._logger.info(
                    "Already up to date: latest=%s; skipping download.",
                    upstream_last_updated_str,
                    extra={"component": "sdmxflow", "dataset_id": self.dataset_id},
                )

            if not will_append:
                self._logger.debug(
                    "No upstream change detected; skipping append (local=%s upstream=%s).",
                    latest,
                    upstream_last_updated_str,
                    extra={
                        "component": "sdmxflow",
                        "dataset_id": self.dataset_id,
                        "local_last_updated_data": latest,
                        "upstream_last_updated_data": upstream_last_updated_str,
                    },
                )
                self._ensure_codelists(metadata)
                save_metadata(self.paths.metadata_json, metadata)
                self._logger.debug(
                    "Fetch finished (appended=%s).",
                    False,
                    extra={
                        "component": "sdmxflow",
                        "dataset_id": self.dataset_id,
                        "appended": False,
                    },
                )
                # INFO (3/3): final result + where to find files.
                self._logger.info(
                    (
                        "Download complete (appended=%s): out_dir=%s dataset_csv=%s "
                        "metadata_json=%s codelists_dir=%s"
                    ),
                    False,
                    str(self.paths.out_dir),
                    str(self.paths.dataset_csv),
                    str(self.paths.metadata_json),
                    str(self.paths.codelists_dir),
                    extra={
                        "component": "sdmxflow",
                        "dataset_id": self.dataset_id,
                        "appended": False,
                    },
                )
                return FetchResult(
                    out_dir=self.paths.out_dir,
                    dataset_csv=self.paths.dataset_csv,
                    metadata_json=self.paths.metadata_json,
                    codelists_dir=self.paths.codelists_dir,
                    appended=False,
                )

            tmp_download = self.paths.out_dir / ".sdmxflow.download.csv"
            if tmp_download.exists():
                try:
                    tmp_download.unlink()
                except OSError:
                    pass

            downloader = EurostatBulkCsvDownloader(logger=self._logger)
            try:
                result = downloader.download(
                    dataset_id=self.dataset_id,
                    out_path=tmp_download,
                    key=self.key,  # type: ignore[arg-type]
                    params=self.params,
                    if_exists="overwrite",
                )
            except (SdmxTimeoutError, SdmxUnreachableError, SdmxInterruptedError):
                # Let dedicated network/user-cancel errors bubble up.
                raise
            except SdmxDownloadError:
                raise
            except KeyboardInterrupt as exc:
                raise SdmxInterruptedError("Bulk download interrupted by user") from exc

            rows_appended = append_version_slice(
                src_csv=result.csv_path,
                dst_csv=self.paths.dataset_csv,
                upstream_last_updated=upstream_last_updated_str,
            )
            self._logger.debug(
                "Dataset CSV appended.",
                extra={
                    "component": "sdmxflow",
                    "dataset_id": self.dataset_id,
                    "rows_appended": rows_appended,
                    "dst_csv": str(self.paths.dataset_csv),
                },
            )

            try:
                result.csv_path.unlink()
            except OSError:
                pass

            # Keep headers compact.
            headers_small: dict[str, str] = {}
            for k in ("etag", "last-modified", "date", "content-type"):
                if k in result.headers and result.headers[k]:
                    headers_small[k] = result.headers[k]

            append_version(
                metadata,
                upstream_last_updated=upstream_last_updated_str,
                fetched_at=dt.datetime.now(dt.UTC).replace(microsecond=0),
                http_url=result.url or None,
                http_status_code=result.status_code,
                http_headers=headers_small,
                rows_appended=rows_appended,
                last_updated_column=LAST_UPDATED_COLUMN,
            )

            save_metadata(self.paths.metadata_json, metadata)
            try:
                self._ensure_codelists(metadata)
            finally:
                # Persist metadata even if codelist generation fails.
                save_metadata(self.paths.metadata_json, metadata)
                self._logger.debug(
                    "Fetch finished (appended=%s).",
                    True,
                    extra={
                        "component": "sdmxflow",
                        "dataset_id": self.dataset_id,
                        "appended": True,
                    },
                )
                # INFO (3/3): final result + where to find files.
                self._logger.info(
                    (
                        "Download complete (appended=%s): out_dir=%s dataset_csv=%s "
                        "metadata_json=%s codelists_dir=%s"
                    ),
                    True,
                    str(self.paths.out_dir),
                    str(self.paths.dataset_csv),
                    str(self.paths.metadata_json),
                    str(self.paths.codelists_dir),
                    extra={
                        "component": "sdmxflow",
                        "dataset_id": self.dataset_id,
                        "appended": True,
                    },
                )

            return FetchResult(
                out_dir=self.paths.out_dir,
                dataset_csv=self.paths.dataset_csv,
                metadata_json=self.paths.metadata_json,
                codelists_dir=self.paths.codelists_dir,
                appended=True,
            )
        finally:
            self._detach_file_logger(handler, prev_level)

    def _ensure_codelists(self, metadata: Metadata) -> None:
        """Ensure codelist CSVs exist on disk and metadata is updated.

        This method is called by :meth:`fetch` after either:

        - a dataset download/append completed, or
        - a no-op fetch where the dataset is already up to date.

        The method downloads SDMX structures to a temporary folder, extracts
        relevant codelists, writes them under ``<out_dir>/codelists/``, and
        updates the provided :class:`~sdmxflow.metadata.models.Metadata` instance
        if the codelist entries changed.

        Args:
            metadata: The in-memory metadata object to update.

        Raises:
            SdmxDownloadError: If the source is unsupported.
            SdmxInterruptedError: If the user interrupts codelist generation.
        """
        # Ensure the dataset exists before attempting to map columns -> codelists.
        if not (self.paths.dataset_csv.exists() and self.paths.dataset_csv.is_file()):
            return

        source = self.source_id.strip().upper()
        if source != "ESTAT":
            raise SdmxDownloadError(
                f"Unsupported source_id={self.source_id!r}. Only 'ESTAT' is implemented."
            )

        # Download structures to a temporary directory, then delete them.
        tmp_dir = self.paths.out_dir / ".sdmxflow.structures.tmp"
        if tmp_dir.exists():
            # Best-effort cleanup of any previous aborted run.
            for p in tmp_dir.glob("*"):
                try:
                    p.unlink()
                except OSError:
                    pass
            try:
                tmp_dir.rmdir()
            except OSError:
                pass

        structures = None
        try:
            self._logger.debug(
                "Downloading SDMX structures for codelists.",
                extra={
                    "component": "sdmxflow",
                    "dataset_id": self.dataset_id,
                    "tmp_dir": str(tmp_dir),
                },
            )
            native = SdmxNativeDownloader(source_id=source, logger=self._logger)
            structures_downloader = SdmxStructureDownloader(native)
            structures = structures_downloader.download_flow_structures(
                source_id=source,
                dataset_id=self.dataset_id,
                out_dir=tmp_dir,
                if_exists="overwrite",
            )

            # Read provider column names from the dataset header (exclude internal last_updated).
            with self.paths.dataset_csv.open("r", encoding="utf-8", newline="") as f:
                header = f.readline()
            cols = next(csv.reader([header])) if header else []
            provider_cols = cols[1:] if cols and cols[0] == LAST_UPDATED_COLUMN else cols
            cols_sample = provider_cols[:10]
            self._logger.debug(
                "Mapping codelists using dataset header (columns=%d; sample=%s).",
                len(provider_cols),
                cols_sample,
                extra={
                    "component": "sdmxflow",
                    "dataset_id": self.dataset_id,
                    "columns_count": len(provider_cols),
                    "columns_sample": cols_sample,
                },
            )

            entries_raw = write_codelists_csvs(
                structures=structures,
                out_dir=self.paths.codelists_dir,
                dataset_columns=provider_cols,
                relative_prefix="codelists",
            )
            entries = [CodelistEntry.model_validate(e) for e in entries_raw]
            codelists_written = len({e.codelist_id for e in entries})
            self._logger.debug(
                "Codelists CSVs ensured on disk (codelists=%d entries=%d dir=%s).",
                codelists_written,
                len(entries),
                str(self.paths.codelists_dir),
                extra={
                    "component": "sdmxflow",
                    "dataset_id": self.dataset_id,
                    "out_dir": str(self.paths.codelists_dir),
                    "codelists_written": codelists_written,
                    "entries": len(entries),
                },
            )

            old = [e.model_dump(mode="json") for e in metadata.codelists]
            new = [e.model_dump(mode="json") for e in entries]
            if old != new:
                set_codelists(metadata, entries)
                self._logger.debug(
                    "Codelists metadata updated (entries=%d).",
                    len(entries),
                    extra={
                        "component": "sdmxflow",
                        "dataset_id": self.dataset_id,
                        "count": len(entries),
                    },
                )
            else:
                self._logger.debug(
                    "Codelists metadata already up to date (entries=%d).",
                    len(entries),
                    extra={
                        "component": "sdmxflow",
                        "dataset_id": self.dataset_id,
                        "count": len(entries),
                    },
                )
        except KeyboardInterrupt as exc:
            raise SdmxInterruptedError("Codelist generation interrupted by user") from exc
        finally:
            # Clean up temporary structure artifacts and their sidecars.
            if structures is not None:
                for path in [structures.dataflow, structures.datastructure]:
                    for candidate in [path, path.with_name(path.name + ".meta.json")]:
                        if candidate.exists():
                            try:
                                candidate.unlink()
                            except OSError:
                                pass
            # Best-effort tmp dir cleanup.
            if tmp_dir.exists():
                for p in tmp_dir.glob("*"):
                    try:
                        p.unlink()
                    except OSError:
                        pass
                try:
                    tmp_dir.rmdir()
                except OSError:
                    pass
