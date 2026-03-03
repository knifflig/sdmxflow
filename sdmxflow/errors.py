"""Typed exceptions for sdmxflow."""

from __future__ import annotations


class SdmxflowError(RuntimeError):
    """Base error for sdmxflow."""


class SdmxDownloadError(SdmxflowError):
    """Raised when an SDMX download fails."""


class SdmxInterruptedError(SdmxflowError):
    """Raised when the process is interrupted (e.g. Ctrl+C).

    We raise a dedicated error so callers can distinguish user-initiated
    cancellation from network/validation failures.
    """


class SdmxTimeoutError(SdmxDownloadError):
    """Raised when a network operation times out."""


class SdmxUnreachableError(SdmxDownloadError):
    """Raised when the server/host is not reachable (DNS/refused/reset/etc)."""


class SdmxMetadataError(SdmxflowError):
    """Raised when metadata extraction/serialization fails."""
