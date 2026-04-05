"""
Storage-surprise signal.

Bullish/bearish surprise relative to seasonal expectations.
Covers crude oil and natural gas weekly inventory.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import pandas as pd

from atlas_core.exceptions import InsufficientDataError
from atlas_core.features.energy import compute_storage_surprise
from atlas_core.schemas.events import EventDomain, SignalComponent
from atlas_core.signals.base import BaseSignal, SignalResult
from atlas_core.utils.math import rolling_zscore


class StorageSurpriseSignal(BaseSignal):
    """
    Weekly storage-surprise signal for crude oil or natural gas.

    Value: z-score of (actual change − seasonal expectation)
    Range: approximately [−3, +3]; clipped to [−1, 1] for standardisation.

    Positive z (bullish): drew more than seasonal average (tighter supply).
    Negative z (bearish): built more than seasonal average (looser supply).
    """

    name = "storage_surprise"
    description = "Weekly EIA storage report vs. seasonal expectations"
    min_rows = 8

    def __init__(
        self,
        commodity: str = "crude",
        region: str = "US",
        store: Optional[Any] = None,
    ) -> None:
        super().__init__(store)
        self.commodity = commodity
        self.region = region

    def compute(self, df: Optional[pd.DataFrame] = None, **kwargs: Any) -> pd.DataFrame:
        """
        Compute surprise history.

        Parameters
        ----------
        df : pre-fetched storage DataFrame.  If None, queries the store.
        """
        if df is None:
            store = self.require_store()
            table = "crude_storage" if self.commodity == "crude" else "gas_storage"
            df = store.query(
                f"SELECT * FROM {table} WHERE region = ? ORDER BY report_date",
                [self.region],
            )

        self._check_rows(df, context=f"{self.commodity}/{self.region}")
        return compute_storage_surprise(df, commodity=self.commodity)

    def latest(self, df: Optional[pd.DataFrame] = None, **kwargs: Any) -> SignalResult:
        history = self.compute(df=df)
        if history.empty:
            raise InsufficientDataError(
                "No storage surprise data", signal_name=self.name
            )

        row = history.iloc[-1]
        z = float(row["z_score"])
        # Standardise to [−1, 1]
        value = max(-1.0, min(1.0, z / 3.0))
        direction = row["signal_direction"]
        confidence = float(row["confidence"])

        component = SignalComponent(
            name="storage_surprise_z",
            description=f"{self.commodity.title()} storage change vs. seasonal avg (z-score)",
            value=z,
            direction=direction,
            weight=1.0,
            historical_hit_rate=_estimate_hit_rate(history),
            data_table="crude_storage" if self.commodity == "crude" else "gas_storage",
            source_domain=EventDomain.ENERGY,
        )

        return SignalResult(
            signal_name=self.name,
            value=value,
            direction=direction,
            confidence=confidence,
            components=[component],
            metadata={
                "commodity": self.commodity,
                "region": self.region,
                "report_date": str(row["report_date"]),
                "actual_change": float(row["actual_change"]),
                "consensus_change": float(row["consensus_change"]),
                "surprise": float(row["surprise"]),
                "z_score": z,
                "five_year_pct_dev": row.get("five_year_pct_dev"),
            },
        )


class GasStorageSurpriseSignal(StorageSurpriseSignal):
    """Convenience: natural-gas storage surprise for US total."""

    name = "gas_storage_surprise"
    description = "Weekly EIA natural-gas storage vs. seasonal expectations (Henry Hub region)"

    def __init__(self, region: str = "US", store: Optional[Any] = None) -> None:
        super().__init__(commodity="natgas", region=region, store=store)


# ─── helpers ─────────────────────────────────────────────────────────────────

def _estimate_hit_rate(history: pd.DataFrame, forward_weeks: int = 1) -> Optional[float]:
    """
    Rough hit-rate: does a surprise in week t predict the change in week t+n?

    Without actual price data we use sign(surprise t) == sign(change t+n)
    as a proxy for the commodity response.
    """
    if len(history) < forward_weeks + 4:
        return None
    h = history.sort_values("report_date").reset_index(drop=True)
    sig = h["surprise"].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    fwd = h["actual_change"].shift(-forward_weeks).apply(
        lambda x: 1 if x > 0 else (-1 if x < 0 else 0)
    )
    valid = sig != 0
    if valid.sum() < 4:
        return None
    return float((sig[valid] == fwd[valid]).mean())


from typing import Optional  # noqa: E402
