"""
Weather-risk signal.

Aggregates active NWS alerts and HDD/CDD deviations into a composite
weather-risk score for the Gulf Coast energy corridor.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from atlas_core.features.weather import score_weather_risk_for_asset
from atlas_core.schemas.events import EventDomain, SignalComponent
from atlas_core.schemas.geo import LNG_TERMINALS, PhysicalAsset
from atlas_core.schemas.weather import NWSAlert
from atlas_core.signals.base import BaseSignal, SignalResult
from atlas_core.utils.math import rolling_zscore


class WeatherRiskSignal(BaseSignal):
    """
    Composite weather-risk signal for a set of tracked assets.

    Aggregates per-asset NWS alert exposure and HDD/CDD deviations.
    """

    name = "weather_risk"
    description = "Composite weather-risk score for tracked energy assets"
    min_rows = 1

    def __init__(
        self,
        assets: Optional[list[PhysicalAsset]] = None,
        store: Optional[Any] = None,
    ) -> None:
        super().__init__(store)
        self.assets = assets or LNG_TERMINALS  # default: Gulf Coast LNG terminals

    def compute(
        self,
        alerts: Optional[list[NWSAlert]] = None,
        hdd_df: Optional[pd.DataFrame] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Compute weather-risk scores for all tracked assets.

        Parameters
        ----------
        alerts  : list of active NWSAlert objects
        hdd_df  : historical HDD/CDD DataFrame (for seasonal context)
        """
        alerts = alerts or []
        rows = []
        for asset in self.assets:
            risk = score_weather_risk_for_asset(asset, alerts)
            rows.append(
                {
                    "asset_id": asset.asset_id,
                    "asset_name": asset.name,
                    "asset_type": asset.asset_type.value,
                    "score": risk.score,
                    "active_alert_count": len(risk.active_alerts),
                    "storm_proximity_km": risk.storm_proximity_km,
                    "extreme_temp_flag": risk.extreme_temp_flag,
                    "high_wind_flag": risk.high_wind_flag,
                }
            )
        return pd.DataFrame(rows)

    def latest(
        self,
        alerts: Optional[list[NWSAlert]] = None,
        hdd_cdd_z: Optional[float] = None,
        **kwargs: Any,
    ) -> SignalResult:
        df = self.compute(alerts=alerts)
        if df.empty:
            return SignalResult(
                signal_name=self.name,
                value=0.0,
                direction="neutral",
                confidence=0.5,
            )

        max_score = float(df["score"].max())
        avg_score = float(df["score"].mean())
        alerts_count = int(df["active_alert_count"].sum())

        # Composite value: blend max and avg
        value = -(max_score * 0.6 + avg_score * 0.4)   # negative = bearish (risk event)
        direction = "bearish" if value < -0.2 else ("bullish" if value > 0.2 else "neutral")
        confidence = min(1.0, max_score + 0.3 * (alerts_count > 0))

        components = [
            SignalComponent(
                name="nws_alert_exposure",
                description=f"{alerts_count} active NWS alerts affecting tracked assets",
                value=float(max_score),
                direction="bearish" if max_score > 0.3 else "neutral",
                weight=0.6,
                source_domain=EventDomain.WEATHER,
                data_table="nws_alerts",
            ),
        ]

        if hdd_cdd_z is not None:
            components.append(
                SignalComponent(
                    name="hdd_cdd_z",
                    description="HDD/CDD deviation from seasonal norm",
                    value=float(hdd_cdd_z),
                    direction="bullish" if hdd_cdd_z > 1.0 else "neutral",
                    weight=0.4,
                    source_domain=EventDomain.WEATHER,
                )
            )

        return SignalResult(
            signal_name=self.name,
            value=float(value),
            direction=direction,
            confidence=float(confidence),
            components=components,
            metadata={
                "max_asset_score": max_score,
                "avg_asset_score": avg_score,
                "active_alerts": alerts_count,
                "asset_scores": df.set_index("asset_id")["score"].to_dict(),
            },
        )
