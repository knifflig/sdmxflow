from __future__ import annotations

import json
from pathlib import Path

from ._types import JsonValue


def read_json(path: Path) -> JsonValue:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: JsonValue, *, indent: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=indent, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
