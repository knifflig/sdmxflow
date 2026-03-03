from __future__ import annotations

import gzip
import io
import logging
from pathlib import Path

import pytest

from sdmxflow.download.providers.eurostat_bulk_csv import (
    EurostatBulkCsvDownloader,
    _build_eurostat_bulk_url,
    _build_key_string,
    _to_bool,
)
from sdmxflow.errors import SdmxDownloadError, SdmxUnreachableError


def test_to_bool_parsing() -> None:
    assert _to_bool(None, default=True) is True
    assert _to_bool("false", default=True) is False
    assert _to_bool("YES", default=False) is True


def test_build_key_string_is_deterministic() -> None:
    key = {"B": ["2", "3"], "A": "1"}
    assert _build_key_string(key) == "1.2+3"


def test_build_url_includes_passthrough_params() -> None:
    url = _build_eurostat_bulk_url(
        dataset_id="X", key="", params={"startPeriod": "2020", "endPeriod": "2021"}
    )
    assert "startPeriod=2020" in url
    assert "endPeriod=2021" in url


def test_download_decompresses_gzip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_bytes = b"A,B\n1,2\n"
    gz = gzip.compress(csv_bytes)

    class _Resp(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(gz)
            self.status = 200
            self.headers = {"Content-Type": "application/gzip"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_urlopen(req, timeout=None):  # noqa: ANN001,ARG001
        return _Resp()

    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    monkeypatch.setattr(mod, "urlopen", _fake_urlopen)

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    res = dl.download(
        dataset_id="X", out_path=out, params={"compress": True}, if_exists="overwrite"
    )

    assert res.csv_path.exists()
    assert res.csv_path.read_bytes() == csv_bytes


def test_download_with_logger_and_no_timeout_does_not_crash(
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

    monkeypatch.setattr(mod, "urlopen", lambda req, timeout=None: _Resp())

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader(logger=logging.getLogger("sdmxflow.tests"))
    res = dl.download(
        dataset_id="X", out_path=out, params={"compress": False}, if_exists="overwrite"
    )
    assert res.csv_path.exists()


def test_download_skip_existing_file(tmp_path: Path) -> None:
    out = tmp_path / "dataset.csv"
    out.write_text("A,B\n1,2\n", encoding="utf-8")

    dl = EurostatBulkCsvDownloader()
    res = dl.download(dataset_id="X", out_path=out, if_exists="skip")
    assert res.csv_path == out
    assert res.status_code is None


def test_download_compress_false(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
    res = dl.download(
        dataset_id="X", out_path=out, params={"compress": False}, if_exists="overwrite"
    )
    assert res.csv_path == out
    assert out.read_text(encoding="utf-8") == "A,B\n1,2\n"


def test_download_raises_on_http_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from email.message import Message
    from urllib.error import HTTPError

    hdrs = Message()
    hdrs["Content-Type"] = "text/plain"
    exc = HTTPError(url="https://example.invalid", code=404, msg="nope", hdrs=hdrs, fp=None)

    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    def _raise(req, timeout=None):  # noqa: ANN001,ARG001
        raise exc

    monkeypatch.setattr(mod, "urlopen", _raise)

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    with pytest.raises(SdmxDownloadError, match="status=404"):
        dl.download(dataset_id="X", out_path=out, if_exists="overwrite")


def test_download_raises_on_url_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from urllib.error import URLError

    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    def _raise(req, timeout=None):  # noqa: ANN001,ARG001
        raise URLError("boom")

    monkeypatch.setattr(mod, "urlopen", _raise)

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    with pytest.raises(SdmxUnreachableError, match="not reachable"):
        dl.download(dataset_id="X", out_path=out, if_exists="overwrite")


def test_download_raises_on_gzip_decompression_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = b"not-a-gzip"

    class _Resp(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(payload)
            self.status = 200
            self.headers = {"Content-Type": "application/gzip"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    import sdmxflow.download.providers.eurostat_bulk_csv as mod

    monkeypatch.setattr(mod, "urlopen", lambda req, timeout=None: _Resp())

    out = tmp_path / "dataset.csv"
    dl = EurostatBulkCsvDownloader()
    with pytest.raises(SdmxDownloadError, match="gzip decompression"):
        dl.download(dataset_id="X", out_path=out, params={"compress": True}, if_exists="overwrite")
