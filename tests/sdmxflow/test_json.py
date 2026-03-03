from __future__ import annotations

import json
from pathlib import Path

from sdmxflow._json import read_json, write_json


def test_write_json_creates_parent_and_trailing_newline(tmp_path: Path) -> None:
    out = tmp_path / "a" / "b" / "c.json"
    write_json(out, {"b": 2, "a": 1})
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert text.endswith("\n")
    # sort_keys=True in write_json
    assert text.strip().startswith("{")
    assert '"a": 1' in text
    assert '"b": 2' in text


def test_read_json_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "x.json"
    p.write_text(json.dumps({"x": [1, 2, 3]}), encoding="utf-8")
    assert read_json(p) == {"x": [1, 2, 3]}
