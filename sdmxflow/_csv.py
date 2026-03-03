"""CSV helpers.

This module contains small, focused helpers for writing dataset slices into a
canonical CSV layout.

The canonical format used by sdmxflow prepends a `last_updated` column to the
provider CSV schema. This column is used to tag appended rows with the upstream
"last updated" timestamp.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

from .errors import SdmxMetadataError

LAST_UPDATED_COLUMN = "last_updated"


def _parse_header_line(line: str) -> list[str]:
    try:
        return next(csv.reader([line]))
    except Exception as exc:  # noqa: BLE001
        raise SdmxMetadataError(f"Failed to parse CSV header: {exc}") from exc


def _format_header_line(columns: list[str]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(columns)
    return buf.getvalue()


def _normalize_provider_header(columns: list[str]) -> list[str]:
    # Remove BOM from first column name if present.
    if columns and columns[0].startswith("\ufeff"):
        columns = [columns[0].lstrip("\ufeff"), *columns[1:]]

    cols = [c.strip() for c in columns if c is not None]
    cols2 = [c for c in cols if c != LAST_UPDATED_COLUMN]
    if not cols2:
        raise SdmxMetadataError("Source CSV has an empty/invalid header")
    return cols2


def ensure_last_updated_first_column(*, csv_path: Path) -> None:
    """Ensure the CSV header starts with the `last_updated` column.

    If the file exists and its header does not start with `last_updated`, the
    file is rewritten in-place so that:

    - the header is updated to include `last_updated` as the first column, and
    - each subsequent non-empty data row is prefixed with a blank value for
        `last_updated`.
    """
    if not csv_path.exists() or not csv_path.is_file():
        return

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        header_line = f.readline()
        if not header_line:
            return
        header_cols = _parse_header_line(header_line)

        if header_cols and header_cols[0] == LAST_UPDATED_COLUMN:
            return

        provider_cols = _normalize_provider_header(header_cols)
        new_header_cols = [LAST_UPDATED_COLUMN, *provider_cols]

        tmp_path = csv_path.with_name(csv_path.name + ".tmp")
        with tmp_path.open("w", encoding="utf-8", newline="") as out:
            out.write(_format_header_line(new_header_cols))

            # Rewrite all remaining lines as-is, but prefix a blank last_updated.
            for line in f:
                if not line.strip():
                    continue
                if not line.endswith("\n"):
                    line += "\n"
                out.write("," + line)

    tmp_path.replace(csv_path)


def append_version_slice(*, src_csv: Path, dst_csv: Path, upstream_last_updated: str) -> int:
    """Append a downloaded CSV into a destination CSV.

    The source CSV is expected to *not* contain a `last_updated` column. This
    function appends its data rows to `dst_csv` while prepending
    `upstream_last_updated` as the first column.

    Returns the number of appended data rows.
    """
    if not src_csv.exists() or not src_csv.is_file():
        raise SdmxMetadataError(f"Source CSV does not exist: {src_csv}")

    with src_csv.open("r", encoding="utf-8", newline="") as src:
        src_header_line = src.readline()
        if not src_header_line:
            raise SdmxMetadataError(f"Source CSV is empty: {src_csv}")

        src_cols = _normalize_provider_header(_parse_header_line(src_header_line))

        if dst_csv.exists() and dst_csv.is_file() and dst_csv.stat().st_size > 0:
            ensure_last_updated_first_column(csv_path=dst_csv)

            with dst_csv.open("r", encoding="utf-8", newline="") as dst_in:
                dst_header_line = dst_in.readline()
                if not dst_header_line:
                    raise SdmxMetadataError(f"Destination CSV has no header: {dst_csv}")
                dst_cols = _parse_header_line(dst_header_line)
                if not dst_cols or dst_cols[0] != LAST_UPDATED_COLUMN:
                    raise SdmxMetadataError(
                        f"Destination CSV header is missing {LAST_UPDATED_COLUMN}: {dst_csv}"
                    )
                if dst_cols[1:] != src_cols:
                    raise SdmxMetadataError(
                        "CSV schema mismatch: source columns differ from destination columns "
                        f"(dst={dst_cols[1:]}, src={src_cols})"
                    )

            dst = dst_csv.open("a", encoding="utf-8", newline="")
            close_dst = True
        else:
            dst_csv.parent.mkdir(parents=True, exist_ok=True)
            dst = dst_csv.open("w", encoding="utf-8", newline="")
            close_dst = True
            dst.write(_format_header_line([LAST_UPDATED_COLUMN, *src_cols]))

        rows = 0
        try:
            for line in src:
                if not line.strip():
                    continue
                if not line.endswith("\n"):
                    line += "\n"
                dst.write(upstream_last_updated)
                dst.write(",")
                dst.write(line)
                rows += 1
        finally:
            if close_dst:
                dst.close()

    return rows
