"""
Composite / cross-domain signal.

The signature feature: WTI/natgas bearish-surprise-risk decomposed across
all physical-intelligence domains.  This is the "causal drilldown" engine.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

from atlas_core.schemas.events import EventDomain, SignalComponent
from atlas_core.signals.base import BaseSignal, SignalResult
from atlas_core.signals.congestion import CongestionSignal
from atlas_core.signals.fire_exposure import FireExposureSignal
from atlas_core.signals.news_flow import NewsFlowSignal
from atlas_core.signals.storage_surprise import StorageSurpriseSignal
from atlas_core.signals.weather_risk import WeatherRiskSignal


class CompositeRiskSignal(BaseSignal):
    """
    Cross-domain composite signal: bearish-risk decomposition.

    Decomposes commodity-risk into:
      1. Storage surprise (EIA)
      2. Weather risk (NWS)
      3. Fire / infrastructure exposure (NASA FIRMS)
      4. Export terminal congestion (AIS)
      5. News-flow acceleration (GDELT)

    Each component is normalised to [−1, 1].  The composite is a
    confidence-weighted average.
    """

    name = "composite_bearish_risk"
    description = "Cross-domain composite bearish-risk signal (storage + weather + fire + congestion + news)"
    min_rows = 0

    # Component weights
    WEIGHTS = {
        "storage": 0.35,
        "weather": 0.25,
        "fire": 0.15,
        "congestion": 0.15,
        "news": 0.10,
    }

    def __init__(
        self,
        commodity: str = "crude",
        store: Optional[Any] = None,
    ) -> None:
        super().__init__(store)
        self.commodity = commodity
        # Sub-signals
        self._storage = StorageSurpriseSignal(commodity=commodity, store=store)
        self._weather = WeatherRiskSignal(store=store)
        self._fire = FireExposureSignal(store=store)
        self._congestion = CongestionSignal(store=store)
        self._news = NewsFlowSignal(store=store)

    def compute(self, **kwargs: Any) -> pd.DataFrame:
        """Returns a single-row DataFrame with all component scores."""
        result = self.latest(**kwargs)
        return pd.DataFrame(
            [
                {
                    "signal_name": c.name,
                    "description": c.description,
                    "value": c.value,
                    "direction": c.direction,
                    "weight": c.weight,
                    "source_domain": c.source_domain.value,
                }
                for c in result.components
            ]
        )

    def latest(self, **kwargs: Any) -> SignalResult:
        """
        Compute the composite risk signal.

        All sub-signals are attempted; failures return zero contribution
        (with reduced composite confidence).
        """
        components: list[SignalComponent] = []
        sub_scores: list[tuple[float, float]] = []   # (value, weight)
        failed_domains: list[str] = []

        def _try(domain: str, fn: Any, weight: float) -> None:
            try:
                result: SignalResult = fn()
                sub_scores.append((result.value, weight * result.confidence))
                for c in result.components:
                    components.append(c)
            except Exception as exc:
                failed_domains.append(domain)
                # Add a zero-weight placeholder
                sub_scores.append((0.0, 0.0))

        _try("storage", lambda: self._storage.latest(**kwargs), self.WEIGHTS["storage"])
        _try("weather", lambda: self._weather.latest(**kwargs), self.WEIGHTS["weather"])
        _try("fire", lambda: self._fire.latest(**kwargs), self.WEIGHTS["fire"])
        _try("congestion", lambda: self._congestion.latest(**kwargs), self.WEIGHTS["congestion"])
        _try("news", lambda: self._news.latest(**kwargs), self.WEIGHTS["news"])

        # Weighted average
        total_weight = sum(w for _, w in sub_scores)
        if total_weight == 0:
            composite_value = 0.0
        else:
            composite_value = sum(v * w for v, w in sub_scores) / total_weight

        composite_value = max(-1.0, min(1.0, composite_value))
        direction = "bearish" if composite_value < -0.15 else (
            "bullish" if composite_value > 0.15 else "neutral"
        )
        confidence = min(1.0, total_weight / sum(self.WEIGHTS.values()))

        return SignalResult(
            signal_name=self.name,
            value=composite_value,
            direction=direction,
            confidence=confidence,
            components=components,
            metadata={
                "commodity": self.commodity,
                "component_weights": self.WEIGHTS,
                "failed_domains": failed_domains,
                "composite_value": composite_value,
            },
        )
