from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from sdmxflow.download.native import SdmxNativeDownloader, _infer_extension
from sdmxflow.errors import SdmxDownloadError, SdmxInterruptedError
from sdmxflow.models import SdmxRequest


class _Response:
    def __init__(
        self,
        *,
        url: str,
        status_code: int,
        headers: dict[str, str],
        payload: bytes,
        iter_raises: Exception | None = None,
    ) -> None:
        self.url = url
        self.status_code = status_code
        self.headers = headers
        self._payload = payload
        self._iter_raises = iter_raises

    @property
    def history(self):  # noqa: ANN201
        raise RuntimeError("boom")

    def iter_content(self, chunk_size: int = 1024):  # noqa: ANN001
        _ = chunk_size
        if self._iter_raises is None:
            return [self._payload]

        def _gen():
            yield self._payload
            raise self._iter_raises

        return _gen()


class _Session:
    def __init__(self, resp: _Response) -> None:
        self._resp = resp

    def prepare_request(self, request: object) -> object:
        return request

    def send(self, prepared: object, **kwargs: object) -> _Response:
        _ = prepared
        _ = kwargs
        return self._resp


class _Client:
    def __init__(self, *, session: _Session, request_obj: object) -> None:
        self.session = session
        self._request_obj = request_obj
        self.calls: list[dict[str, object]] = []

    def get(self, **kwargs: object) -> object:
        self.calls.append(dict(kwargs))
        return self._request_obj


def test_infer_extension_unknown_defaults_to_sdmx() -> None:
    assert _infer_extension("application/octet-stream", None) == ".sdmx"


def test_init_raises_when_base_url_without_source_id() -> None:
    with pytest.raises(ValueError, match="source_id is required"):
        SdmxNativeDownloader(base_url="https://example.invalid")


def test_init_calls_add_source_when_base_url_and_source_id(monkeypatch: pytest.MonkeyPatch) -> None:
    import sdmxflow.download.native as mod

    calls: list[dict[str, object]] = []

    def _fake_add_source(payload: str, *, id: str, override: bool) -> None:  # noqa: A002
        calls.append({"payload": payload, "id": id, "override": override})

    def _fake_client(source_id=None, **opts):  # noqa: ANN001
        return SimpleNamespace(source_id=source_id, opts=opts, session=None)

    monkeypatch.setattr(mod.sdmx, "add_source", _fake_add_source)
    monkeypatch.setattr(mod.sdmx, "Client", _fake_client)

    _ = SdmxNativeDownloader(source_id="X", base_url="https://example.invalid")
    assert calls
    assert calls[0]["id"] == "X"
    assert calls[0]["override"] is True


def test_init_passes_session_opts_and_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    import sdmxflow.download.native as mod

    captured: dict[str, object] = {}

    def _fake_client(source_id=None, **opts):  # noqa: ANN001
        captured["source_id"] = source_id
        captured["opts"] = dict(opts)
        return SimpleNamespace(source_id=source_id, opts=opts, session=None)

    monkeypatch.setattr(mod.sdmx, "Client", _fake_client)

    _ = SdmxNativeDownloader(source_id="ESTAT", session_opts={"verify": False}, timeout_seconds=3.0)
    assert captured["source_id"] == "ESTAT"
    assert captured["opts"]["verify"] is False
    assert captured["opts"]["timeout"] == 3.0


def test_download_logs_references_extraction_error_and_redirect_chain_exception(
    tmp_path: Path,
) -> None:
    class _BadParams(dict):
        def get(self, key: str, default=None):  # noqa: ANN001
            raise RuntimeError("boom")

    resp = _Response(
        url="https://example.invalid",
        status_code=404,
        headers={"Content-Type": "application/xml"},
        payload=b"<error/>",
    )
    client = _Client(session=_Session(resp), request_obj=object())

    log = logging.getLogger("sdmxflow.native.more")
    log.handlers.clear()
    log.setLevel(logging.DEBUG)

    d = SdmxNativeDownloader(_client=client, logger=log)
    with pytest.raises(SdmxDownloadError, match=r"HTTP 404"):
        d.download(
            SdmxRequest(
                source_id="TEST",
                resource_type="dataflow",
                resource_id="FLOW",
                params=_BadParams({"references": "none"}),  # type: ignore[arg-type]
            ),
            out_dir=tmp_path,
            if_exists="overwrite",
        )


def test_download_raises_when_client_has_no_session(tmp_path: Path) -> None:
    class _NoSessionClient:
        def get(self, **kwargs: object) -> object:  # noqa: ANN401
            return object()

    d = SdmxNativeDownloader(_client=_NoSessionClient())
    with pytest.raises(SdmxDownloadError, match="no 'session' attribute"):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
        )


def test_download_keyboard_interrupt_during_streaming_is_wrapped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import pathlib

    resp = _Response(
        url="https://example.invalid",
        status_code=200,
        headers={"Content-Type": "application/xml"},
        payload=b"<x/>",
        iter_raises=KeyboardInterrupt(),
    )
    client = _Client(session=_Session(resp), request_obj=object())

    log = logging.getLogger("sdmxflow.native.interrupt")
    log.handlers.clear()
    log.setLevel(logging.DEBUG)

    # Make tmp cleanup hit the OSError branch.
    orig_unlink = pathlib.Path.unlink

    def _unlink(self: pathlib.Path, *args, **kwargs):  # noqa: ANN001
        if str(self).endswith(".part"):
            raise OSError("boom")
        return orig_unlink(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "unlink", _unlink, raising=True)

    d = SdmxNativeDownloader(_client=client, logger=log)
    with pytest.raises(SdmxInterruptedError):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
        )


def test_download_logs_bytes_written_stat_oserror(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resp = _Response(
        url="https://example.invalid",
        status_code=200,
        headers={"Content-Type": "application/xml"},
        payload=b"<root/>",
    )
    client = _Client(session=_Session(resp), request_obj=object())

    log = logging.getLogger("sdmxflow.native.bytes")
    log.handlers.clear()
    log.setLevel(logging.DEBUG)

    d = SdmxNativeDownloader(_client=client, logger=log)
    res = d.download(
        SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
        out_dir=tmp_path,
        if_exists="overwrite",
    )

    orig_stat = Path.stat

    def _stat(self: Path, *args, **kwargs):  # noqa: ANN001
        if self == res.path:
            raise OSError("boom")
        return orig_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", _stat, raising=True)
    # Re-run to execute the stat error path.
    _ = d.download(
        SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
        out_dir=tmp_path,
        if_exists="overwrite",
    )


def test_download_ignores_stale_part_unlink_oserror(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import pathlib

    resp = _Response(
        url="https://example.invalid",
        status_code=200,
        headers={"Content-Type": "application/xml"},
        payload=b"<root/>",
    )
    client = _Client(session=_Session(resp), request_obj=object())

    # Force the stale `.part` cleanup to hit the OSError branch.
    stale = tmp_path / "dataflow__FLOW.part"
    stale.write_text("stale", encoding="utf-8")

    orig_unlink = pathlib.Path.unlink

    def _unlink(self: pathlib.Path, *args, **kwargs):  # noqa: ANN001
        if self == stale:
            raise OSError("boom")
        return orig_unlink(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "unlink", _unlink, raising=True)

    d = SdmxNativeDownloader(_client=client)
    res = d.download(
        SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
        out_dir=tmp_path,
    )
    assert res.path.exists()


def test_peek_body_returns_empty_when_tmp_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import pathlib

    resp = _Response(
        url="https://example.invalid",
        status_code=404,
        headers={"Content-Type": "application/xml"},
        payload=b"<error>nope</error>",
    )
    client = _Client(session=_Session(resp), request_obj=object())

    orig_exists = pathlib.Path.exists

    def _exists(self: pathlib.Path) -> bool:
        if self.name.endswith(".part"):
            return False
        return orig_exists(self)

    monkeypatch.setattr(pathlib.Path, "exists", _exists, raising=True)

    d = SdmxNativeDownloader(_client=client)
    with pytest.raises(SdmxDownloadError, match=r"HTTP 404"):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
        )


def test_peek_body_handles_read_bytes_oserror(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import pathlib

    resp = _Response(
        url="https://example.invalid",
        status_code=404,
        headers={"Content-Type": "application/xml"},
        payload=b"<error>nope</error>",
    )
    client = _Client(session=_Session(resp), request_obj=object())

    orig_read_bytes = pathlib.Path.read_bytes

    def _read_bytes(self: pathlib.Path) -> bytes:
        if self.name.endswith(".part"):
            raise OSError("boom")
        return orig_read_bytes(self)

    monkeypatch.setattr(pathlib.Path, "read_bytes", _read_bytes, raising=True)

    d = SdmxNativeDownloader(_client=client)
    with pytest.raises(SdmxDownloadError, match=r"HTTP 404"):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
        )


def test_peek_body_handles_decode_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import pathlib

    class _BadBytes:
        def __getitem__(self, item):  # noqa: ANN001
            return self

        def decode(self, *args: Any, **kwargs: Any) -> str:
            raise UnicodeError("boom")

    resp = _Response(
        url="https://example.invalid",
        status_code=404,
        headers={"Content-Type": "application/xml"},
        payload=b"<error>nope</error>",
    )
    client = _Client(session=_Session(resp), request_obj=object())

    orig_read_bytes = pathlib.Path.read_bytes

    def _read_bytes(self: pathlib.Path):  # noqa: ANN001
        if self.name.endswith(".part"):
            return _BadBytes()
        return orig_read_bytes(self)

    monkeypatch.setattr(pathlib.Path, "read_bytes", _read_bytes, raising=True)

    d = SdmxNativeDownloader(_client=client)
    with pytest.raises(SdmxDownloadError, match=r"HTTP 404"):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
        )


@pytest.mark.parametrize("exc_type", ["Timeout", "ConnectionError"])
def test_download_requests_exception_mapping_lines_executed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, exc_type: str
) -> None:
    import sdmxflow.download.native as native_mod

    class _Timeout(Exception):
        pass

    class _Conn(Exception):
        pass

    class _Reqs:
        class exceptions:  # noqa: N801
            Timeout = _Timeout
            ConnectionError = _Conn

    monkeypatch.setattr(native_mod, "requests", _Reqs(), raising=False)

    class _BadClient:
        def get(self, **kwargs: object) -> object:  # noqa: ANN401
            if exc_type == "Timeout":
                raise _Timeout("boom")
            raise _Conn("boom")

    log = logging.getLogger(f"sdmxflow.native.{exc_type}")
    log.handlers.clear()
    log.setLevel(logging.DEBUG)

    d = SdmxNativeDownloader(_client=_BadClient(), logger=log)
    with pytest.raises(SdmxDownloadError):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
        )


def test_download_html_content_type_logs_error_with_logger(tmp_path: Path) -> None:
    resp = _Response(
        url="https://example.invalid",
        status_code=200,
        headers={"Content-Type": "text/html"},
        payload=b"<html>oops</html>",
    )
    client = _Client(session=_Session(resp), request_obj=object())

    log = logging.getLogger("sdmxflow.native.html")
    log.handlers.clear()
    log.setLevel(logging.DEBUG)

    d = SdmxNativeDownloader(_client=client, logger=log)
    with pytest.raises(SdmxDownloadError, match="returned HTML"):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
        )


def test_download_html_sniff_logs_error_with_logger(tmp_path: Path) -> None:
    resp = _Response(
        url="https://example.invalid",
        status_code=200,
        headers={"Content-Type": "application/xml"},
        payload=b"<html>oops</html>",
    )
    client = _Client(session=_Session(resp), request_obj=object())

    log = logging.getLogger("sdmxflow.native.sniff")
    log.handlers.clear()
    log.setLevel(logging.DEBUG)

    d = SdmxNativeDownloader(_client=client, logger=log)
    with pytest.raises(SdmxDownloadError, match="looks like HTML"):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
        )
