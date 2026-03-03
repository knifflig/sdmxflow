from __future__ import annotations

import gzip
import io
import logging
from pathlib import Path
from urllib.error import URLError

import pytest

from sdmxflow.download.providers.eurostat_bulk_csv import (
    EurostatBulkCsvDownloader,
    _build_key_string,
    _to_bool,
)
from sdmxflow.errors import SdmxDownloadError, SdmxInterruptedError, SdmxTimeoutError


def test_build_key_string_handles_none_and_none_values() -> None:
    assert _build_key_string(None) == ""
    assert _build_key_string({"A": None, "B": "1"}) == ".1"


def test_to_bool_default_fallback() -> None:
    assert _to_bool("maybe", default=True) is True


def test_download_cleanup_unlink_oserror_is_ignored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = b"A,B\n1,2\n"

    class _Resp(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(payload)
            self.status = 200
            self.headers = {"Content-Type": "text/csv"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    # Make the pre-cleanup hit the OSError branch by raising from `unlink()`.
    out = tmp_path / "dataset.csv"
    tmp_download = out.with_name(out.name + ".part")
    gz_download = out.with_suffix(out.suffix + ".gz")
    tmp_gz = gz_download.with_name(gz_download.name + ".part")
    tmp_download.write_text("stale", encoding="utf-8")
    tmp_gz.write_text("stale", encoding="utf-8")

    orig_unlink = Path.unlink

    def _unlink(self: Path, *args, **kwargs):  # noqa: ANN001
        if self in {tmp_download, tmp_gz}:
            raise OSError("boom")
        return orig_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", _unlink, raising=True)

    monkeypatch.setattr(mod, "urlopen", lambda req, timeout=None: _Resp())

    dl = EurostatBulkCsvDownloader(logger=logging.getLogger("sdmxflow.tests"))
    res = dl.download(
        dataset_id="X", out_path=out, params={"compress": False}, if_exists="overwrite"
    )
    assert res.csv_path.exists()


def test_download_keyboard_interrupt_is_wrapped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    def _raise(req, timeout=None):  # noqa: ANN001,ARG001
        raise KeyboardInterrupt()

    monkeypatch.setattr(mod, "urlopen", _raise)

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader(logger=logging.getLogger("sdmxflow.tests"))
    with pytest.raises(SdmxInterruptedError):
        dl.download(dataset_id="X", out_path=out, if_exists="overwrite")


def test_download_urLError_timeout_reason_is_classified(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    def _raise(req, timeout=None):  # noqa: ANN001,ARG001
        raise URLError(reason=TimeoutError("boom"))

    monkeypatch.setattr(mod, "urlopen", _raise)

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    with pytest.raises(SdmxTimeoutError):
        dl.download(dataset_id="X", out_path=out, if_exists="overwrite")


def test_download_timeout_error_is_classified(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    def _raise(req, timeout=None):  # noqa: ANN001,ARG001
        raise TimeoutError("boom")

    monkeypatch.setattr(mod, "urlopen", _raise)

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    with pytest.raises(SdmxTimeoutError):
        dl.download(dataset_id="X", out_path=out, if_exists="overwrite")


def test_download_generic_exception_is_wrapped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    def _raise(req, timeout=None):  # noqa: ANN001,ARG001
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "urlopen", _raise)

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    with pytest.raises(SdmxDownloadError, match="boom"):
        dl.download(dataset_id="X", out_path=out, if_exists="overwrite")


def test_download_empty_file_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class _Resp(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(b"")
            self.status = 200
            self.headers = {"Content-Type": "text/csv"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    monkeypatch.setattr(mod, "urlopen", lambda req, timeout=None: _Resp())

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    with pytest.raises(SdmxDownloadError, match="empty file"):
        dl.download(dataset_id="X", out_path=out, params={"compress": False}, if_exists="overwrite")


def test_download_decompression_keyboard_interrupt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    csv_bytes = b"A,B\n1,2\n"
    gz = gzip.compress(csv_bytes)

    class _Resp(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(gz)
            self.status = 200
            self.headers = {"Content-Type": "application/gzip"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    monkeypatch.setattr(mod, "urlopen", lambda req, timeout=None: _Resp())
    monkeypatch.setattr(
        mod.gzip, "open", lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt())
    )

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    with pytest.raises(SdmxInterruptedError):
        dl.download(dataset_id="X", out_path=out, params={"compress": True}, if_exists="overwrite")


def test_download_csv_helper_returns_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    payload = b"A,B\n1,2\n"

    class _Resp(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(payload)
            self.status = 200
            self.headers = {"Content-Type": "text/csv"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    monkeypatch.setattr(mod, "urlopen", lambda req, timeout=None: _Resp())

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    p = dl.download_csv(
        dataset_id="X", out_path=out, params={"compress": False}, if_exists="overwrite"
    )
    assert p == out
