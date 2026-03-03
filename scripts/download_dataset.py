"""Example: download an SDMX dataset.

This is a *live* script (not run in pytest) intended for:

- first-time users trying sdmxflow quickly,
- maintainers demonstrating the project in docs/blog posts,
- debugging real network/provider behavior.

Notes:
- This script performs network I/O.
- The library itself does not configure logging handlers; this script enables a
    basic console logger for convenience.

Examples:
    uv run python scripts/download_dataset.py
    uv run python scripts/download_dataset.py --dataset-id lfsa_egai2d --save-logs
    uv run python scripts/download_dataset.py --out-dir ./out/lfsa_egai2d --log-level DEBUG
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

LOGGER = logging.getLogger("sdmxflow.example")


def _ensure_repo_on_syspath() -> None:
    """Allow running the script without installing the package."""
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parents[1]  # scripts/.. -> repo root
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="download_dataset",
        description=(
            "Download an SDMX dataset into a local folder using sdmxflow. "
            "(Live network I/O; intended as a runnable example.)"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  uv run python scripts/download_dataset.py\n"
            "  uv run python scripts/download_dataset.py --dataset-id lfsa_egai2d --save-logs\n"
            "  uv run python scripts/download_dataset.py --out-dir ./out/lfsa_egai2d "
            "--log-level DEBUG\n"
        ),
    )

    parser.add_argument(
        "--source-id",
        default="ESTAT",
        help="SDMX source ID (default: ESTAT)",
    )
    parser.add_argument(
        "--agency-id",
        default=None,
        help=(
            "SDMX agency ID (optional). If omitted, provider defaults are used. "
            "For Eurostat, this is usually ESTAT."
        ),
    )
    parser.add_argument(
        "--dataset-id",
        default="lfsa_egai2d",
        help="Dataset ID (default: lfsa_egai2d)",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help=(
            "Output directory. Default: ./out/<source>/<dataset> "
            "(relative to current working directory)"
        ),
    )
    parser.add_argument(
        "--save-logs",
        action="store_true",
        help="Write a per-run debug log file under <out_dir>/logs/.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log level (default: INFO)",
    )

    return parser.parse_args()


def main() -> int:
    """Run the download script.

    Returns:
        Process exit code. Returns 0 on success.
    """
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Keep third-party HTTP plumbing quieter; sdmxflow emits its own request logs.
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    _ensure_repo_on_syspath()

    from sdmxflow.dataset import SdmxDataset  # imported after sys.path setup

    if args.out_dir:
        out_dir = Path(args.out_dir).expanduser().resolve()
    else:
        out_dir = (Path.cwd() / "out" / args.source_id.lower() / args.dataset_id).resolve()

    LOGGER.info(
        "Starting download: source_id=%s agency_id=%s dataset_id=%s out_dir=%s",
        args.source_id,
        args.agency_id,
        args.dataset_id,
        out_dir,
    )

    ds = SdmxDataset(
        out_dir=out_dir,
        source_id=args.source_id,
        dataset_id=args.dataset_id,
        agency_id=args.agency_id,
        save_logs=args.save_logs,
    )

    result = ds.fetch()

    LOGGER.info(
        "Done. appended=%s dataset_csv=%s metadata_json=%s codelists_dir=%s",
        result.appended,
        result.dataset_csv,
        result.metadata_json,
        result.codelists_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
