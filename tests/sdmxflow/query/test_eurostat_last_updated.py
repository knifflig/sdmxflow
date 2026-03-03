from __future__ import annotations

import datetime as dt
import logging

import pytest

from sdmxflow.errors import SdmxDownloadError
from sdmxflow.query import last_updated_data as mod


class _Resp:
    def __init__(
        self, *, status_code: int | None, url: str = "https://example.invalid", content: bytes = b""
    ) -> None:
        self.status_code = status_code
        self.url = url
        self.content = content


class _Session:
    def __init__(self, *, resp: _Resp | None = None, raise_send: Exception | None = None) -> None:
        self._resp = resp
        self._raise_send = raise_send
        self.sent: list[object] = []

    def prepare_request(self, request: object) -> object:
        return {"prepared": request}

    def send(self, prepared: object, **kwargs: object) -> _Resp:
        _ = kwargs
        self.sent.append(prepared)
        if self._raise_send is not None:
            raise self._raise_send
        assert self._resp is not None
        return self._resp


class _Client:
    def __init__(self, *, session: object | None) -> None:
        self.session = session
        self.calls: list[dict[str, object]] = []

    def get(self, **kwargs: object) -> object:
        self.calls.append(dict(kwargs))

        class _Req:
            cookies = object()

        return _Req()


def test_eurostat_last_updated_happy_path(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    xml = b"""<?xml version='1.0' encoding='utf-8'?>
<m:Structure xmlns:m='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message'
             xmlns:c='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'>
  <c:Annotation>
    <c:AnnotationTitle>2026-01-01T00:00:00Z</c:AnnotationTitle>
    <c:AnnotationType>UPDATE_DATA</c:AnnotationType>
  </c:Annotation>
</m:Structure>
"""

    sess = _Session(resp=_Resp(status_code=200, content=xml))
    client = _Client(session=sess)

    class _Native:
        def __init__(self, *, source_id: str, timeout_seconds=None, logger=None):  # noqa: ANN001,ARG002
            self._client = client

    monkeypatch.setattr(mod, "SdmxNativeDownloader", _Native)

    caplog.set_level(logging.DEBUG)
    info = mod.eurostat_last_updated(dataset_id="lfsa_egai2d", logger=logging.getLogger("t"))
    assert info.source_id == "ESTAT"
    assert info.dataset_id == "lfsa_egai2d"
    assert info.updated_at == dt.datetime(2026, 1, 1, 0, 0, tzinfo=dt.UTC)
    assert client.calls


def test_eurostat_last_updated_raises_when_session_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _Client(session=None)

    class _Native:
        def __init__(self, *, source_id: str, timeout_seconds=None, logger=None):  # noqa: ANN001,ARG002
            self._client = client

    monkeypatch.setattr(mod, "SdmxNativeDownloader", _Native)
    with pytest.raises(SdmxDownloadError, match="no 'session'"):
        mod.eurostat_last_updated(dataset_id="x")


def test_eurostat_last_updated_raises_on_send_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    sess = _Session(raise_send=RuntimeError("boom"))
    client = _Client(session=sess)

    class _Native:
        def __init__(self, *, source_id: str, timeout_seconds=None, logger=None):  # noqa: ANN001,ARG002
            self._client = client

    monkeypatch.setattr(mod, "SdmxNativeDownloader", _Native)
    with pytest.raises(SdmxDownloadError, match="Failed to request dataflow"):
        mod.eurostat_last_updated(dataset_id="x")


@pytest.mark.parametrize("status", [None, 500])
def test_eurostat_last_updated_raises_on_bad_status(
    monkeypatch: pytest.MonkeyPatch, status: int | None
) -> None:
    sess = _Session(resp=_Resp(status_code=status, url="https://bad.invalid", content=b"<x/>"))
    client = _Client(session=sess)

    class _Native:
        def __init__(self, *, source_id: str, timeout_seconds=None, logger=None):  # noqa: ANN001,ARG002
            self._client = client

    monkeypatch.setattr(mod, "SdmxNativeDownloader", _Native)
    with pytest.raises(SdmxDownloadError, match="status="):
        mod.eurostat_last_updated(dataset_id="x")


def test_eurostat_last_updated_raises_when_timestamp_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    xml = b"""<?xml version='1.0' encoding='utf-8'?>
<m:Structure xmlns:m='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message'
             xmlns:c='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'>
  <c:Annotation>
    <c:AnnotationTitle>not-a-date</c:AnnotationTitle>
    <c:AnnotationType>UPDATE_DATA</c:AnnotationType>
  </c:Annotation>
</m:Structure>
"""

    sess = _Session(resp=_Resp(status_code=200, content=xml))
    client = _Client(session=sess)

    class _Native:
        def __init__(self, *, source_id: str, timeout_seconds=None, logger=None):  # noqa: ANN001,ARG002
            self._client = client

    monkeypatch.setattr(mod, "SdmxNativeDownloader", _Native)
    with pytest.raises(SdmxDownloadError, match="Could not extract"):
        mod.eurostat_last_updated(dataset_id="x")
