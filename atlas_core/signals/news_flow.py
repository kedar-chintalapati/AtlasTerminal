"""
News-flow signal.

Scores GDELT article volume and tone for energy-relevant narratives.
High negative-tone acceleration or sudden volume spikes precede price moves.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

from atlas_core.schemas.events import EventDomain, GDELTEvent, SignalComponent
from atlas_core.signals.base import BaseSignal, SignalResult
from atlas_core.utils.math import ewma_zscore


class NewsFlowSignal(BaseSignal):
    """
    GDELT-based news-flow signal for a specific energy topic.

    Positive value: positive-tone acceleration (mildly bullish).
    Negative value: negative-tone acceleration / volume spike (risk signal).
    """

    name = "news_flow"
    description = "GDELT news volume and sentiment signal for energy topics"
    min_rows = 1

    def __init__(
        self,
        topic: str = "natural_gas",
        store: Optional[Any] = None,
    ) -> None:
        super().__init__(store)
        self.topic = topic

    def compute(
        self,
        articles: Optional[list[GDELTEvent]] = None,
        history_df: Optional[pd.DataFrame] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Compute article volume and sentiment time series.

        Supply either ``articles`` (list of GDELTEvent) or a pre-built
        ``history_df`` from the store.
        """
        if articles is not None:
            df = _events_to_df(articles)
        elif history_df is not None:
            df = history_df
        elif self._store is not None:
            df = self._store.query(
                "SELECT * FROM gdelt_events ORDER BY publish_date"
            )
        else:
            return pd.DataFrame()

        if df.empty:
            return df

        # Aggregate by date
        df["date"] = pd.to_datetime(df["publish_date"]).dt.date
        daily = (
            df.groupby("date")
            .agg(
                article_count=("event_id" if "event_id" in df.columns else "url", "count"),
                avg_tone=("tone", "mean"),
                negative_count=("tone", lambda x: (x < -3).sum()),
            )
            .reset_index()
        )
        daily = daily.sort_values("date")

        # Signal: EWMA z-score of negative tone count
        daily["neg_z"] = ewma_zscore(daily["negative_count"].astype(float), span=7, long_span=30)
        daily["vol_z"] = ewma_zscore(daily["article_count"].astype(float), span=3, long_span=14)

        return daily

    def latest(
        self,
        articles: Optional[list[GDELTEvent]] = None,
        **kwargs: Any,
    ) -> SignalResult:
        df = self.compute(articles=articles)

        if df.empty:
            return SignalResult(
                signal_name=self.name,
                value=0.0,
                direction="neutral",
                confidence=0.2,
                metadata={"data_available": False},
            )

        row = df.iloc[-1]
        neg_z = float(row.get("neg_z", 0) or 0)
        vol_z = float(row.get("vol_z", 0) or 0)
        avg_tone = float(row.get("avg_tone", 0) or 0)
        count = int(row.get("article_count", 0) or 0)

        # Composite: negative z drives direction
        value = max(-1.0, min(1.0, -(neg_z * 0.6 + vol_z * 0.4) / 3.0))
        direction = "bearish" if value < -0.2 else ("bullish" if value > 0.2 else "neutral")
        confidence = min(1.0, 0.3 + 0.1 * min(count, 7))

        components = [
            SignalComponent(
                name="negative_tone_acceleration",
                description=f"EWMA z-score of negative-tone articles (topic: {self.topic})",
                value=neg_z,
                direction="bearish" if neg_z > 1.5 else "neutral",
                weight=0.6,
                source_domain=EventDomain.GEOPOLITICS,
                data_table="gdelt_events",
            ),
            SignalComponent(
                name="volume_acceleration",
                description=f"EWMA z-score of article volume (topic: {self.topic})",
                value=vol_z,
                direction="bearish" if vol_z > 2.0 else "neutral",
                weight=0.4,
                source_domain=EventDomain.GEOPOLITICS,
            ),
        ]

        return SignalResult(
            signal_name=self.name,
            value=value,
            direction=direction,
            confidence=confidence,
            components=components,
            metadata={
                "topic": self.topic,
                "article_count_today": count,
                "avg_tone_today": avg_tone,
                "negative_z_today": neg_z,
                "volume_z_today": vol_z,
            },
        )


def _events_to_df(events: list[GDELTEvent]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "event_id": e.event_id,
                "publish_date": e.publish_date,
                "url": e.url,
                "tone": e.tone,
                "relevance_score": e.relevance_score,
                "lat": e.lat,
                "lon": e.lon,
            }
            for e in events
        ]
    )
