"""
Port-congestion signal.

Derives a congestion index for key LNG export terminals using AIS vessel
positions.  High congestion = vessels backing up = potential export disruption.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

from atlas_core.features.spatial import vessels_near_terminals
from atlas_core.schemas.events import EventDomain, SignalComponent
from atlas_core.schemas.geo import LNG_TERMINALS, PhysicalAsset
from atlas_core.schemas.vessels import VesselPosition
from atlas_core.signals.base import BaseSignal, SignalResult
from atlas_core.utils.math import rolling_zscore


class CongestionSignal(BaseSignal):
    """
    Export-terminal congestion signal derived from AIS vessel positions.

    Positive value (rare) = vessels clearing quickly (bearish for LNG export).
    Negative value = vessels congesting / backing up (bullish for freight cost;
    bearish for LNG supply certainty).
    """

    name = "export_congestion"
    description = "LNG/crude export terminal congestion from AIS vessel positions"
    min_rows = 0

    def __init__(
        self,
        terminals: Optional[list[PhysicalAsset]] = None,
        radius_km: float = 25.0,
        store: Optional[Any] = None,
    ) -> None:
        super().__init__(store)
        self.terminals = terminals or LNG_TERMINALS
        self.radius_km = radius_km

    def compute(
        self,
        positions: Optional[list[VesselPosition]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        if positions is None:
            # Try to load from store
            if self._store is not None:
                df = self._store.query(
                    "SELECT * FROM vessel_positions ORDER BY timestamp DESC LIMIT 50000"
                )
                positions = _df_to_positions(df)
            else:
                positions = []

        return vessels_near_terminals(self.terminals, positions, radius_km=self.radius_km)

    def latest(
        self,
        positions: Optional[list[VesselPosition]] = None,
        **kwargs: Any,
    ) -> SignalResult:
        df = self.compute(positions=positions)
        if df.empty:
            return SignalResult(
                signal_name=self.name,
                value=0.0,
                direction="neutral",
                confidence=0.2,
                metadata={"data_available": False},
            )

        avg_congestion = float(df["congestion_proxy"].mean())
        max_congestion = float(df["congestion_proxy"].max())
        total_tankers = int(df["tanker_count"].sum())
        total_anchored = int(df["vessels_at_anchor"].sum())

        value = -min(1.0, avg_congestion)   # negative = risk
        direction = "bearish" if avg_congestion > 0.4 else ("bullish" if avg_congestion < 0.1 else "neutral")

        most_congested = df.sort_values("congestion_proxy", ascending=False).iloc[0]
        component = SignalComponent(
            name="terminal_congestion",
            description=(
                f"{total_tankers} tankers near terminals, {total_anchored} at anchor"
            ),
            value=avg_congestion,
            direction=direction,
            weight=1.0,
            source_domain=EventDomain.SHIPPING,
            data_table="vessel_positions",
        )

        return SignalResult(
            signal_name=self.name,
            value=value,
            direction=direction,
            confidence=min(1.0, 0.4 + 0.1 * total_tankers),
            components=[component],
            metadata={
                "avg_congestion_index": avg_congestion,
                "max_congestion_index": max_congestion,
                "total_tankers": total_tankers,
                "vessels_at_anchor": total_anchored,
                "most_congested_terminal": most_congested["terminal_id"],
                "terminal_congestion": df.set_index("terminal_id")["congestion_proxy"].to_dict(),
            },
        )


def _df_to_positions(df: pd.DataFrame) -> list[VesselPosition]:
    """Convert a store DataFrame to VesselPosition objects."""
    from atlas_core.schemas.vessels import NavigationStatus, VesselType
    from datetime import datetime, timezone
    positions = []
    for _, row in df.iterrows():
        try:
            positions.append(
                VesselPosition(
                    mmsi=str(row.get("mmsi", "")),
                    vessel_name=str(row.get("vessel_name", "")),
                    vessel_type=VesselType(row.get("vessel_type", "unknown")),
                    timestamp=row.get("timestamp", datetime.now(tz=timezone.utc)),
                    lat=float(row["lat"]),
                    lon=float(row["lon"]),
                    speed_kts=row.get("speed_kts"),
                    nav_status=NavigationStatus(row.get("nav_status", "unknown")),
                )
            )
        except Exception:
            pass
    return positions
