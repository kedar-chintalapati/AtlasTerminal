"""
Atlas Terminal — typed exception hierarchy.

Design principles
-----------------
* Every error is a subclass of ``AtlasError`` so callers can catch the whole
  tree with a single except clause.
* Connector errors carry the source name and HTTP status (when applicable) so
  they surface cleanly in logs without digging through tracebacks.
* All errors are raised with a human-readable message AND structured context
  kwargs so that structured-logging decorators can attach them automatically.
"""

from __future__ import annotations

from typing import Any, Optional


class AtlasError(Exception):
    """Base for every exception raised by atlas_core."""

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.context: dict[str, Any] = context

    def __repr__(self) -> str:
        ctx = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
        return f"{self.__class__.__name__}({self.args[0]!r}{', ' + ctx if ctx else ''})"


# ─────────────────────────────────────────────────────────────────────────────
# Connector / data-source errors
# ─────────────────────────────────────────────────────────────────────────────


class ConnectorError(AtlasError):
    """Base for errors raised by data-source connectors."""

    def __init__(
        self,
        message: str,
        source: str = "unknown",
        **context: Any,
    ) -> None:
        super().__init__(message, source=source, **context)
        self.source = source


class ConnectorAuthError(ConnectorError):
    """Authentication / API-key failure (HTTP 401 / 403)."""


class ConnectorRateLimitError(ConnectorError):
    """Rate-limit hit (HTTP 429).  Caller may back off and retry."""

    def __init__(
        self,
        message: str,
        source: str = "unknown",
        retry_after: Optional[float] = None,
        **context: Any,
    ) -> None:
        super().__init__(message, source=source, retry_after=retry_after, **context)
        self.retry_after = retry_after


class ConnectorHTTPError(ConnectorError):
    """Non-2xx HTTP response that isn't auth or rate-limit."""

    def __init__(
        self,
        message: str,
        source: str = "unknown",
        status_code: int = 0,
        **context: Any,
    ) -> None:
        super().__init__(message, source=source, status_code=status_code, **context)
        self.status_code = status_code


class ConnectorParseError(ConnectorError):
    """Response body couldn't be parsed into the expected schema."""


class ConnectorTimeoutError(ConnectorError):
    """Request timed out before a response was received."""


class ConnectorNotConfiguredError(ConnectorError):
    """Required API key / credential is missing from settings."""


# ─────────────────────────────────────────────────────────────────────────────
# Storage errors
# ─────────────────────────────────────────────────────────────────────────────


class StoreError(AtlasError):
    """Base for DuckDB / Parquet storage errors."""


class StoreTableNotFoundError(StoreError):
    """Query references a table that hasn't been populated yet."""

    def __init__(self, table: str, **context: Any) -> None:
        super().__init__(f"Table '{table}' not found in store", table=table, **context)
        self.table = table


class StoreSchemaError(StoreError):
    """Incoming data doesn't match the expected table schema."""


# ─────────────────────────────────────────────────────────────────────────────
# Feature / signal errors
# ─────────────────────────────────────────────────────────────────────────────


class FeatureError(AtlasError):
    """Raised during feature-engineering computations."""


class SignalError(AtlasError):
    """Raised when a signal cannot be computed (e.g. insufficient data)."""

    def __init__(self, message: str, signal_name: str = "unknown", **context: Any) -> None:
        super().__init__(message, signal_name=signal_name, **context)
        self.signal_name = signal_name


class InsufficientDataError(SignalError):
    """Not enough rows to compute the signal reliably."""


# ─────────────────────────────────────────────────────────────────────────────
# Alert errors
# ─────────────────────────────────────────────────────────────────────────────


class AlertError(AtlasError):
    """Raised during alert evaluation or dispatch."""


class AlertRuleError(AlertError):
    """An alert rule definition is malformed."""


# ─────────────────────────────────────────────────────────────────────────────
# Research errors
# ─────────────────────────────────────────────────────────────────────────────


class ResearchError(AtlasError):
    """Raised during backtesting or event-study computations."""
