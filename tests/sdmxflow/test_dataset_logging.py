from __future__ import annotations

import logging
from pathlib import Path

import pytest

from sdmxflow.dataset import SdmxDataset
from sdmxflow.errors import SdmxDownloadError, SdmxInterruptedError


def test_attach_file_logger_disabled_returns_none(tmp_path: Path) -> None:
    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="x", save_logs=False)
    h, prev, path = ds._attach_file_logger(agency_id="ESTAT")  # noqa: SLF001
    assert h is None
    assert prev is None
    assert path is None


def test_detach_file_logger_ignores_handler_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logger = logging.getLogger("sdmxflow.test_detach")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)

    ds = SdmxDataset(
        out_dir=tmp_path, source_id="ESTAT", dataset_id="x", logger=logger, save_logs=True
    )

    class _BadHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover
            return

        def close(self) -> None:
            raise RuntimeError("boom")

    h = _BadHandler()
    logger.addHandler(h)

    def _raise_remove(_h: logging.Handler) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(logger, "removeHandler", _raise_remove)

    def _raise_set_level(_lvl: int) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(logger, "setLevel", _raise_set_level)
    # Should not raise.
    ds._detach_file_logger(h, prev_level=logging.NOTSET)  # noqa: SLF001

    # Ensure we don't trigger an exception during `logging.shutdown()` at interpreter exit.
    # The logging module keeps weakrefs to all handlers ever created.
    h.close = lambda: None  # type: ignore[assignment]


def test_attach_file_logger_enables_debug_when_logger_level_notset(tmp_path: Path) -> None:
    logger = logging.getLogger("sdmxflow.test_attach_notset")
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)

    ds = SdmxDataset(
        out_dir=tmp_path, source_id="ESTAT", dataset_id="x", logger=logger, save_logs=True
    )
    h, prev, path = ds._attach_file_logger(agency_id="ESTAT")  # noqa: SLF001
    try:
        assert h is not None
        assert prev == logging.NOTSET
        assert path is not None
        assert path.exists()
    finally:
        ds._detach_file_logger(h, prev)  # noqa: SLF001


def test_fetch_unsupported_source_raises(tmp_path: Path) -> None:
    ds = SdmxDataset(out_dir=tmp_path, source_id="NOPE", dataset_id="x")
    with pytest.raises(SdmxDownloadError):
        ds.fetch()


def test_fetch_keyboard_interrupt_in_setup_is_wrapped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ds = SdmxDataset(out_dir=tmp_path, source_id="ESTAT", dataset_id="x")

    def _interrupt() -> None:
        raise KeyboardInterrupt()

    monkeypatch.setattr(ds, "setup", _interrupt)
    with pytest.raises(SdmxInterruptedError):
        ds.fetch()
