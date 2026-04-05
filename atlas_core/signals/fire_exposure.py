"""
Fire-exposure signal.

Aggregates NASA FIRMS detections near tracked energy assets into a
composite exposure score.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from atlas_core.features.spatial import assets_near_fires
from atlas_core.schemas.events import EventDomain, FIRMSDetection, SignalComponent
from atlas_core.schemas.geo import LNG_TERMINALS, PhysicalAsset
from atlas_core.signals.base import BaseSignal, SignalResult


class FireExposureSignal(BaseSignal):
    """
    NASA FIRMS-based fire-exposure signal for energy assets.

    Negative value = bearish risk (fire near production/infrastructure).
    """

    name = "fire_exposure"
    description = "Active fire exposure score for tracked energy assets (NASA FIRMS)"
    min_rows = 0    # 0 detections is valid (no fire = zero score)

    def __init__(
        self,
        assets: Optional[list[PhysicalAsset]] = None,
        radius_km: float = 75.0,
        store: Optional[Any] = None,
    ) -> None:
        super().__init__(store)
        self.assets = assets or LNG_TERMINALS
        self.radius_km = radius_km

    def compute(
        self,
        detections: Optional[list[FIRMSDetection]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Compute per-asset fire-exposure metrics.

        Parameters
        ----------
        detections : list of FIRMSDetection objects (recent, e.g. last 3 days)
        """
        detections = detections or []
        return assets_near_fires(self.assets, detections, radius_km=self.radius_km)

    def latest(
        self,
        detections: Optional[list[FIRMSDetection]] = None,
        **kwargs: Any,
    ) -> SignalResult:
        df = self.compute(detections=detections)
        if df.empty:
            return SignalResult(
                signal_name=self.name,
                value=0.0,
                direction="neutral",
                confidence=0.3,
            )

        max_score = float(df["fire_exposure_score"].max())
        total_fires = int(df["fire_count"].sum())
        max_frp = float(df["max_frp_mw"].max())

        # Negative value = bearish risk
        value = -min(1.0, max_score)
        direction = "bearish" if max_score > 0.2 else "neutral"
        confidence = min(1.0, 0.5 + max_score * 0.5)

        most_exposed = df.sort_values("fire_exposure_score", ascending=False).iloc[0]

        component = SignalComponent(
            name="fire_proximity",
            description=f"{total_fires} active fire detections near tracked assets",
            value=max_score,
            direction=direction,
            weight=1.0,
            source_domain=EventDomain.FIRE,
            data_table="firms_detections",
        )

        return SignalResult(
            signal_name=self.name,
            value=value,
            direction=direction,
            confidence=confidence,
            components=[component],
            metadata={
                "total_detections": total_fires,
                "max_frp_mw": max_frp,
                "most_exposed_asset": most_exposed["asset_id"],
                "most_exposed_score": float(most_exposed["fire_exposure_score"]),
                "asset_scores": df.set_index("asset_id")["fire_exposure_score"].to_dict(),
            },
        )
