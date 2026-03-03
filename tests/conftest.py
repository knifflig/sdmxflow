"""Shared pytest fixtures for sdmxflow tests.

Testing strategy (mirrors the archived suite):
- Unit tests are offline/deterministic.
- Real SDMX structure payloads are stored under `tests/data/...`.
- A small integration+network test can (re)generate those fixtures.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import pytest
import requests

_REAL_WORLD_SOURCE_ID = "ESTAT"
_REAL_WORLD_DATAFLOW_ID = "lfsa_egai2d"


def real_world_fixture_raw_dir() -> Path:
    return (
        Path(__file__).resolve().parent
        / "data"
        / "sdmx"
        / "estat"
        / _REAL_WORLD_DATAFLOW_ID
        / "raw"
    )


def real_world_fixture_paths() -> dict[str, Path]:
    raw_dir = real_world_fixture_raw_dir()
    return {
        "dataflow": raw_dir / "dataflow_all.xml",
        "dataflow_meta": raw_dir / "dataflow_all.xml.meta.json",
        "datastructure": raw_dir / "datastructure_descendants.xml",
        "datastructure_meta": raw_dir / "datastructure_descendants.xml.meta.json",
    }


def skip_if_real_world_fixtures_missing() -> dict[str, Path]:
    paths = real_world_fixture_paths()
    missing = [p for p in paths.values() if not p.exists()]
    if missing:
        pytest.skip(
            "Real-world SDMX structure fixtures are missing. "
            "Run: pytest -m 'integration and network' "
            "tests/integration/test_000_download_estat_real_world_fixtures.py"
        )
    return paths


class FakeSdmxTransport:
    """A realistic SDMX transport mock.

    Mimics `sdmx.Client.get(dry_run=True)` returning a requests.Request and
    serves bytes via `client.session.send(...)`.
    """

    def __init__(
        self,
        *,
        payload_by_resource_type: dict[str, bytes],
        content_type: str = "application/xml",
        content_disposition: str | None = 'attachment; filename="fixture.xml"',
        base_url: str = "https://example.invalid/sdmx",
        status_code: int = 200,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.payload_by_resource_type = payload_by_resource_type
        self.content_type = content_type
        self.content_disposition = content_disposition
        self.base_url = base_url
        self.status_code = status_code

        self.session = _FakeSession(self)

    def get(
        self,
        resource_type: str | None = None,
        resource_id: str | None = None,
        tofile: object | None = None,  # noqa: ARG002
        use_cache: bool = False,  # noqa: ARG002
        dry_run: bool = False,
        **kwargs: object,
    ) -> object:  # noqa: ANN401
        call: dict[str, object] = {
            "resource_type": resource_type,
            "resource_id": resource_id,
            **kwargs,
        }
        self.calls.append(call)

        if not dry_run:
            raise AssertionError("FakeSdmxTransport only supports dry_run=True")

        rt = str(resource_type or "resource")
        rid = str(resource_id) if resource_id else ""
        url = f"{self.base_url.rstrip('/')}/{rt}"
        if rid:
            url += f"/{rid}"
        if kwargs:
            url += "?" + "&".join(f"{k}={v}" for k, v in kwargs.items())
        return requests.Request("GET", url)


class _FakeResponse:
    def __init__(
        self, *, url: str, status_code: int, headers: dict[str, str], payload_bytes: bytes
    ) -> None:
        self.url = url
        self.status_code = status_code
        self.headers = headers
        self._payload_bytes = payload_bytes

    def iter_content(self, chunk_size: int = 1024) -> list[bytes]:
        _ = chunk_size
        return [self._payload_bytes]


class _FakeSession:
    def __init__(self, transport: FakeSdmxTransport) -> None:
        self._transport = transport
        self._requests_session = requests.Session()

    def prepare_request(self, request: requests.Request) -> requests.PreparedRequest:
        return self._requests_session.prepare_request(request)

    def send(self, request: requests.PreparedRequest, **kwargs: object) -> _FakeResponse:
        _ = kwargs
        headers = {"Content-Type": self._transport.content_type}
        if self._transport.content_disposition is not None:
            headers["Content-Disposition"] = self._transport.content_disposition

        url = str(getattr(request, "url", ""))
        payload = b"<root/>"

        mapping = self._transport.payload_by_resource_type
        if mapping:
            try:
                parts = [p for p in urlparse(url).path.split("/") if p]
                if parts:
                    # best-effort SDMX resource type inference
                    rt = parts[-1]
                    if rt not in mapping and len(parts) >= 2:
                        rt = parts[-2]
                    payload = mapping.get(rt, payload)
            except Exception:
                payload = b"<root/>"

        return _FakeResponse(
            url=url, status_code=self._transport.status_code, headers=headers, payload_bytes=payload
        )


@pytest.fixture
def fake_sdmx_transport() -> FakeSdmxTransport:
    paths = skip_if_real_world_fixtures_missing()
    return FakeSdmxTransport(
        payload_by_resource_type={
            "dataflow": paths["dataflow"].read_bytes(),
            "datastructure": paths["datastructure"].read_bytes(),
        }
    )


@pytest.fixture
def patch_sdmx_client(
    monkeypatch: pytest.MonkeyPatch, fake_sdmx_transport: FakeSdmxTransport
) -> FakeSdmxTransport:
    import sdmx

    monkeypatch.setattr(sdmx, "Client", lambda *args, **kwargs: fake_sdmx_transport)
    return fake_sdmx_transport
