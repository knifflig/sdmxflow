"""sdmxflow: download SDMX datasets to a versioned folder layout.

Public API (stable):
- `SdmxDataset`: configure dataset + output folder, then call `.setup()` and `.fetch()`.
- `FetchResult`: paths + whether a new version was appended.

This package intentionally uses stdlib `logging` and does not configure handlers.
"""

from __future__ import annotations

import logging

from .dataset import FetchResult, SdmxDataset

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "FetchResult",
    "SdmxDataset",
]
