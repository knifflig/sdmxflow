from __future__ import annotations

from pathlib import Path
from typing import Literal, TypeAlias

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]

IfExists = Literal["skip", "overwrite"]

PathLike: TypeAlias = str | Path
