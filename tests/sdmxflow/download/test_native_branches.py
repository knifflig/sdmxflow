from __future__ import annotations

import logging
from pathlib import Path

import pytest

from sdmxflow.download.native import SdmxNativeDownloader
from sdmxflow.errors import SdmxDownloadError
from sdmxflow.models import SdmxRequest


class _Response:
    def __init__(
        self,
        *,
        url: str,
        status_code: int,
        headers: dict[str, str],
        payload: bytes,
        history: list[object] | None = None,
    ) -> None:
        self.url = url
        self.status_code = status_code
        self.headers = headers
        self._payload = payload
        self.history = history or []

    def iter_content(self, chunk_size: int = 1024) -> list[bytes]:
        _ = chunk_size
        return [self._payload]


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


def test_download_skip_with_explicit_suffix(tmp_path: Path) -> None:
    out = tmp_path / "dataflow.xml"
    out.write_text("<x/>", encoding="utf-8")

    client = _Client(
        session=_Session(
            _Response(
                url="https://example.invalid",
                status_code=200,
                headers={"Content-Type": "application/xml"},
                payload=b"<x/>",
            )
        ),
        request_obj=object(),
    )

    d = SdmxNativeDownloader(_client=client)
    res = d.download(
        SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
        out_dir=tmp_path,
        filename="dataflow.xml",
        if_exists="skip",
    )
    assert res.path == out
    assert client.calls == []


def test_download_skip_by_glob_when_no_suffix(tmp_path: Path) -> None:
    candidate = tmp_path / "dataflow__FLOW.xml"
    candidate.write_text("<x/>", encoding="utf-8")
    (candidate.with_name(candidate.name + ".meta.json")).write_text("{}", encoding="utf-8")

    client = _Client(
        session=_Session(
            _Response(
                url="https://example.invalid",
                status_code=200,
                headers={"Content-Type": "application/xml"},
                payload=b"<x/>",
            )
        ),
        request_obj=object(),
    )
    d = SdmxNativeDownloader(_client=client)
    res = d.download(
        SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
        out_dir=tmp_path,
        if_exists="skip",
    )
    assert res.path == candidate
    assert client.calls == []


def test_download_includes_key_provider_version_force(tmp_path: Path) -> None:
    resp = _Response(
        url="https://example.invalid",
        status_code=200,
        headers={
            "Content-Type": "application/xml",
            "Content-Disposition": 'attachment; filename="x.xml"',
        },
        payload=b"<root/>",
    )

    request_obj = object()  # no cookies attribute => already prepared
    client = _Client(session=_Session(resp), request_obj=request_obj)

    d = SdmxNativeDownloader(_client=client, timeout_seconds=1.0, logger=logging.getLogger("t"))
    _ = d.download(
        SdmxRequest(
            source_id="TEST",
            resource_type="data",
            resource_id="FLOW",
            key="A.B",
            provider="P",
            version="1.0",
            force=True,
        ),
        out_dir=tmp_path,
        if_exists="overwrite",
    )

    assert client.calls
    call = client.calls[0]
    assert call["key"] == "A.B"
    assert call["provider"] == "P"
    assert call["version"] == "1.0"
    assert call["force"] is True


def test_download_raises_on_non_2xx_and_includes_redirects(tmp_path: Path) -> None:
    resp = _Response(
        url="https://example.invalid",
        status_code=404,
        headers={"Content-Type": "application/xml"},
        payload=b"<error>nope</error>",
        history=[type("H", (), {"status_code": 302, "url": "https://a.invalid"})()],
    )
    client = _Client(session=_Session(resp), request_obj=object())
    d = SdmxNativeDownloader(_client=client)

    with pytest.raises(SdmxDownloadError, match=r"HTTP 404"):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
            if_exists="overwrite",
        )


def test_download_raises_on_html_content_type(tmp_path: Path) -> None:
    resp = _Response(
        url="https://example.invalid",
        status_code=200,
        headers={"Content-Type": "text/html"},
        payload=b"<html>oops</html>",
    )
    client = _Client(session=_Session(resp), request_obj=object())
    d = SdmxNativeDownloader(_client=client)

    with pytest.raises(SdmxDownloadError, match="returned HTML"):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
            if_exists="overwrite",
        )


def test_download_raises_on_html_sniff(tmp_path: Path) -> None:
    resp = _Response(
        url="https://example.invalid",
        status_code=200,
        headers={"Content-Type": "application/xml"},
        payload=b"<html>oops</html>",
    )
    client = _Client(session=_Session(resp), request_obj=object())
    d = SdmxNativeDownloader(_client=client)

    with pytest.raises(SdmxDownloadError, match="looks like HTML"):
        d.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW"),
            out_dir=tmp_path,
            if_exists="overwrite",
        )
