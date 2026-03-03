from __future__ import annotations

import builtins
import logging
from types import SimpleNamespace

import pytest

import sdmxflow.query.last_updated_data as mod
from sdmxflow.errors import SdmxDownloadError, SdmxInterruptedError


def test_parse_sdmx_timestamp_blank_and_naive_returns_none() -> None:
    assert mod._parse_sdmx_timestamp("   ") is None  # noqa: SLF001
    assert mod._parse_sdmx_timestamp("2026-01-01T00:00:00") is None  # noqa: SLF001


def test_extract_last_updated_parse_error_returns_none() -> None:
    assert mod.extract_last_updated_data_from_dataflow_xml(b"not-xml") is None


def test_eurostat_last_updated_builds_provider_version_force(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Req:
        def __init__(self, **kwargs):  # noqa: ANN001
            self.source_id = "ESTAT"
            self.resource_type = "dataflow"
            self.resource_id = "X"
            self.params = {"references": "none"}
            self.provider = "P"
            self.version = "1.0"
            self.force = True

    monkeypatch.setattr(mod, "SdmxRequest", _Req)

    calls: list[dict[str, object]] = []

    class _Client:
        def __init__(self):
            self.session = SimpleNamespace(
                prepare_request=lambda r: r,
                send=lambda *a, **k: SimpleNamespace(status_code=200, url="u", content=b""),
            )

        def get(self, **kwargs):  # noqa: ANN001
            calls.append(dict(kwargs))
            return object()

    class _Native:
        def __init__(self, *a, **k):  # noqa: ANN001
            self._client = _Client()

    monkeypatch.setattr(mod, "SdmxNativeDownloader", _Native)

    with pytest.raises(SdmxDownloadError, match="Could not extract"):
        mod.eurostat_last_updated(dataset_id="X")

    assert calls
    assert calls[0]["provider"] == "P"
    assert calls[0]["version"] == "1.0"
    assert calls[0]["force"] is True


def test_eurostat_last_updated_keyboard_interrupt_is_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        def __init__(self):
            self.session = SimpleNamespace(
                prepare_request=lambda r: r,
                send=lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt()),
            )

        def get(self, **kwargs):  # noqa: ANN001
            return object()

    class _Native:
        def __init__(self, *a, **k):  # noqa: ANN001
            self._client = _Client()

    monkeypatch.setattr(mod, "SdmxNativeDownloader", _Native)

    with pytest.raises(SdmxInterruptedError):
        mod.eurostat_last_updated(dataset_id="X", logger=logging.getLogger("sdmxflow.tests"))


def test_eurostat_last_updated_requests_import_failure_hits_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        def __init__(self):
            self.session = SimpleNamespace(
                prepare_request=lambda r: r,
                send=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
            )

        def get(self, **kwargs):  # noqa: ANN001
            return object()

    class _Native:
        def __init__(self, *a, **k):  # noqa: ANN001
            self._client = _Client()

    monkeypatch.setattr(mod, "SdmxNativeDownloader", _Native)

    orig_import = builtins.__import__

    def _import(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "requests":
            raise ImportError("nope")
        return orig_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _import)

    with pytest.raises(SdmxDownloadError, match="boom"):
        mod.eurostat_last_updated(dataset_id="X")
