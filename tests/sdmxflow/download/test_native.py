from __future__ import annotations

import json
from pathlib import Path

import pytest

from sdmxflow.download.native import SdmxNativeDownloader, _infer_extension, _safe_component
from sdmxflow.errors import SdmxDownloadError
from sdmxflow.models import SdmxRequest
from tests.conftest import FakeSdmxTransport


def test_safe_component_sanitizes() -> None:
    assert _safe_component("  abc  ") == "abc"
    assert _safe_component("a/b c") == "a_b_c"
    assert _safe_component("...") == "_"


def test_infer_extension_prefers_content_disposition_filename() -> None:
    assert (
        _infer_extension(
            content_type="application/octet-stream",
            content_disposition='attachment; filename="hello.zip"',
        )
        == ".zip"
    )


def test_infer_extension_uses_content_type() -> None:
    assert _infer_extension("application/xml; charset=utf-8", None) == ".xml"
    assert _infer_extension("application/vnd.sdmx.genericdata+xml", None) == ".xml"
    assert _infer_extension("application/json", None) == ".json"
    assert _infer_extension("application/zip", None) == ".zip"
    assert _infer_extension(None, None) == ".sdmx"


def test_download_writes_file_and_sidecar(
    tmp_path: Path, fake_sdmx_transport: FakeSdmxTransport
) -> None:
    downloader = SdmxNativeDownloader(_client=fake_sdmx_transport)

    res = downloader.download(
        SdmxRequest(
            source_id="TEST",
            resource_type="dataflow",
            resource_id="FLOW_1",
            params={"references": "none"},
        ),
        out_dir=tmp_path,
        if_exists="overwrite",
    )

    assert res.path.exists()
    assert res.path.suffix == ".xml"

    sidecar = res.path.with_name(res.path.name + ".meta.json")
    assert sidecar.exists()
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["request"]["resource_type"] == "dataflow"
    assert payload["response"]["status_code"] == 200


def test_download_wraps_client_exception(tmp_path: Path) -> None:
    class _BadClient:
        def get(self, **kwargs: object) -> object:  # noqa: ANN401
            raise RuntimeError("boom")

    downloader = SdmxNativeDownloader(_client=_BadClient())
    with pytest.raises(SdmxDownloadError):
        downloader.download(
            SdmxRequest(source_id="TEST", resource_type="dataflow", resource_id="FLOW_1"),
            out_dir=tmp_path,
            if_exists="overwrite",
        )
