"""
Base signal class.

All Atlas signals inherit from ``BaseSignal`` and expose:
  compute(store)  → pd.DataFrame with a standardised set of columns
  latest(store)   → SignalResult (current value + decomposition)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from atlas_core.exceptions import InsufficientDataError
from atlas_core.schemas.events import SignalComponent


@dataclass
class SignalResult:
    """The current value of a signal plus its decomposition."""
    signal_name: str
    value: float                             # normalised score [−1, 1]
    direction: str                           # "bullish" | "bearish" | "neutral"
    confidence: float                        # [0, 1]
    computed_at: datetime = field(default_factory=datetime.utcnow)
    components: list[SignalComponent] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_extreme(self) -> bool:
        return abs(self.value) >= 0.75


class BaseSignal(ABC):
    """Abstract base for all Atlas signals."""

    name: str = "base_signal"
    description: str = ""
    min_rows: int = 10   # minimum observations needed

    def __init__(self, store: Optional[Any] = None) -> None:
        self._store = store

    def require_store(self) -> Any:
        if self._store is None:
            raise RuntimeError(f"Signal '{self.name}' requires a DuckDB store — pass store=")
        return self._store

    def _check_rows(self, df: pd.DataFrame, context: str = "") -> None:
        if len(df) < self.min_rows:
            raise InsufficientDataError(
                f"Signal '{self.name}'{' (' + context + ')' if context else ''}: "
                f"need ≥{self.min_rows} rows, got {len(df)}",
                signal_name=self.name,
            )

    @abstractmethod
    def compute(self, **kwargs: Any) -> pd.DataFrame:
        """
        Compute the signal over its full history.

        Returns a DataFrame with at minimum:
          period, value, z_score, direction, confidence
        """

    @abstractmethod
    def latest(self, **kwargs: Any) -> SignalResult:
        """Return the most-recent single signal reading."""
