"""Codelist extraction.

Parses SDMX structure messages and emits one CSV per codelist.

This is intentionally file-oriented:
- input: SDMX structure files on disk (dataflow/datastructure)
- output: stable CSV files in the dataset folder (e.g. `codelists/SEX.csv`)
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import sdmx
from sdmx.message import StructureMessage

from ..errors import SdmxMetadataError
from ..models import FlowStructureArtifacts


def _items_dict(obj: object) -> dict[object, object] | None:
    """Normalize SDMX "dict-like" containers to a plain dict."""
    items_attr = getattr(obj, "items", None)
    if isinstance(items_attr, dict):
        return items_attr
    if callable(items_attr):
        try:
            pairs = items_attr()
        except TypeError:
            return None
        try:
            return {k: v for (k, v) in cast(Iterable[tuple[object, object]], pairs)}
        except Exception:
            return None
    return None


def _labels(value: object) -> dict[str, str] | None:
    loc = getattr(value, "localizations", None)
    if isinstance(loc, dict) and loc:
        return {str(k): str(v) for k, v in loc.items()}
    return None


def _best_label(labels: dict[str, str] | None) -> str | None:
    if not labels:
        return None
    if "en" in labels:
        return labels["en"]
    k = sorted(labels.keys())[0]
    return labels[k]


def _opt_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _extract_codes(codelist: object) -> list[dict[str, Any]]:
    items = getattr(codelist, "items", None)
    if not isinstance(items, dict) or not items:
        return []

    codes: list[dict[str, Any]] = []
    for code_id in sorted(items.keys(), key=lambda v: str(v)):
        code = items[code_id]
        name = _best_label(_labels(getattr(code, "name", None)))
        codes.append({"id": str(code_id), "name": name})
    return codes


def _read_structure_message(path: Path) -> StructureMessage:
    try:
        with path.open("rb") as fp:
            return cast(StructureMessage, sdmx.read_sdmx(fp))
    except Exception as exc:  # noqa: BLE001
        raise SdmxMetadataError(f"Failed to parse SDMX structure file: {path}: {exc}") from exc


def _extract_all_codelists(*, structures: FlowStructureArtifacts) -> dict[str, dict[str, Any]]:
    """Extract all codelists (id -> payload) from a structure artifact bundle."""
    msg = _read_structure_message(structures.datastructure)

    try:
        codelists_container = msg.codelist
    except Exception:
        return {}

    items = _items_dict(codelists_container) or {}
    out: dict[str, dict[str, Any]] = {}
    for cl_id in sorted(items.keys(), key=lambda v: str(v)):
        cl = items[cl_id]
        labels = _labels(getattr(cl, "name", None))
        out[str(cl_id)] = {
            "id": str(getattr(cl, "id", cl_id)),
            "labels": labels or {},
            "codes": _extract_codes(cl),
        }
    return out


def _extract_codelist_usages(*, structures: FlowStructureArtifacts) -> list[dict[str, str]]:
    """Return codelist usages from the DSD.

    One entry per component with an enumerated codelist, including:
    - kind: "dimension" | "attribute"
    - column_name: the component id (expected to match the CSV header)
    - codelist_id: the enumerated codelist id
    """
    msg = _read_structure_message(structures.datastructure)
    struct_container = getattr(msg, "structure", None)
    struct_items = _items_dict(struct_container) or {}
    if not struct_items:
        return []

    dsd = next(iter(struct_items.values()))
    usages: list[dict[str, str]] = []

    def _maybe_add(kind: str, comp: object) -> None:
        col = _opt_str(getattr(comp, "id", None))
        lr = getattr(comp, "local_representation", None)
        enum = getattr(lr, "enumerated", None) if lr else None
        cl_id = _opt_str(getattr(enum, "id", None)) if enum is not None else None
        if col and cl_id:
            usages.append({"kind": kind, "column_name": col, "codelist_id": cl_id})

    dims = getattr(getattr(dsd, "dimensions", None), "components", None) or []
    for d in dims:
        _maybe_add("dimension", d)

    attrs = getattr(getattr(dsd, "attributes", None), "components", None) or []
    for a in attrs:
        _maybe_add("attribute", a)

    return usages


def write_codelists_csvs(
    *,
    structures: FlowStructureArtifacts,
    out_dir: Path,
    dataset_columns: list[str],
    relative_prefix: str = "codelists",
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    """Write one CSV per used codelist and return metadata entries.

    `dataset_columns` should be the provider CSV columns (i.e. excluding the
    internal `last_updated` column added by sdmxflow).

    Returns a list of dicts ready to feed into the Pydantic `CodelistEntry`.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    log = logger if logger is not None else logging.getLogger("sdmxflow")
    if log.isEnabledFor(logging.DEBUG):
        log.debug(
            "Extracting codelists from SDMX structures (dataset_columns=%d -> %s).",
            len(dataset_columns),
            str(out_dir),
            extra={
                "component": "sdmxflow",
                "out_dir": str(out_dir),
                "dataset_columns": len(dataset_columns),
            },
        )

    # 1-based positions in the provider CSV.
    pos_by_lower: dict[str, tuple[str, int]] = {
        str(col).strip().lower(): (str(col).strip(), i + 1)
        for i, col in enumerate(dataset_columns)
        if str(col).strip()
    }

    all_codelists = _extract_all_codelists(structures=structures)
    usages = _extract_codelist_usages(structures=structures)

    entries: list[dict[str, Any]] = []
    used_codelist_ids: set[str] = set()
    for u in usages:
        mapped = pos_by_lower.get(u["column_name"].lower())
        if not mapped:
            continue

        col_name, col_pos = mapped
        cl_id = u["codelist_id"]
        cl_payload = all_codelists.get(cl_id, {})
        labels = cl_payload.get("labels") if isinstance(cl_payload, dict) else {}
        labels2 = labels if isinstance(labels, dict) else {}

        entries.append(
            {
                "codelist_id": cl_id,
                "codelist_path": f"{relative_prefix}/{cl_id}.csv",
                "codelist_type": "reference",
                "codelist_kind": u["kind"],
                "codelist_labels": {str(k): str(v) for k, v in labels2.items()},
                "column_name": col_name,
                "column_pos": col_pos,
            }
        )
        used_codelist_ids.add(cl_id)

    for cl_id in sorted(used_codelist_ids):
        payload = all_codelists.get(cl_id)
        if not isinstance(payload, dict):
            continue

        codes = payload.get("codes")
        if not isinstance(codes, list):
            codes = []

        out_path = out_dir / f"{cl_id}.csv"
        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(["code", "name"])
            for code in codes:
                if not isinstance(code, dict):
                    continue
                cid = code.get("id")
                name = code.get("name")
                writer.writerow(
                    ["" if cid is None else str(cid), "" if name is None else str(name)]
                )

    entries.sort(key=lambda e: (int(e.get("column_pos") or 0), str(e.get("codelist_id") or "")))
    if log.isEnabledFor(logging.DEBUG):
        log.debug(
            "Codelist CSV extraction complete (codelists_written=%d entries=%d).",
            len(used_codelist_ids),
            len(entries),
            extra={
                "component": "sdmxflow",
                "out_dir": str(out_dir),
                "codelists_written": len(used_codelist_ids),
                "entries": len(entries),
            },
        )
    return entries
