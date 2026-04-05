"""
GDELT (Global Database of Events, Language, and Tone) connector.

GDELT DOC 2.0 API — free, no authentication.
Documentation: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/

Endpoint: https://api.gdeltproject.org/api/v2/doc/doc

Key parameters
--------------
query   : free-text search + Boolean operators
mode    : ArtList (articles), TimelineVol (volume timeline), ToneLine, etc.
maxrecords : up to 250
timespan   : e.g. "7d" "24h" "1w"
format  : json

Energy entity keywords we build from: "LNG", "crude oil", "natural gas",
"pipeline", "refinery", "OPEC", "Cushing", "Henry Hub", etc.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote

from loguru import logger

from atlas_core.connectors.base import BaseConnector
from atlas_core.schemas.events import GDELTEvent

_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

# Pre-built search queries for energy topics
ENERGY_QUERIES: dict[str, str] = {
    "lng_exports": '(LNG OR "liquefied natural gas") (export OR terminal OR shipment)',
    "crude_oil": '("crude oil" OR WTI OR Brent) (pipeline OR tanker OR refinery OR storage)',
    "natural_gas": '("natural gas" OR "Henry Hub" OR Cushing) (storage OR production OR demand)',
    "refinery_outage": '(refinery OR "refining capacity") (outage OR fire OR shutdown OR maintenance)',
    "hurricane_energy": '(hurricane OR tropical OR "storm surge") (gulf OR pipeline OR offshore OR platform)',
    "opec_supply": '(OPEC OR "Saudi Arabia" OR "oil production") (cut OR supply OR quota)',
    "wildfire_power": '(wildfire OR "forest fire") (transmission OR utility OR pipeline OR power)',
}


class GDELTConnector(BaseConnector):
    """GDELT DOC 2.0 API connector for energy-relevant news and events."""

    source_name = "GDELT"
    _rate_limit_rps = 0.3      # Be conservative; GDELT is a shared free resource
    _rate_limit_burst = 2.0

    async def search_articles(
        self,
        query: str,
        timespan: str = "7d",
        max_records: int = 250,
        source_country: Optional[str] = None,
        source_language: Optional[str] = None,
        sort: str = "DateDesc",
    ) -> list[GDELTEvent]:
        """
        Search GDELT for articles matching ``query``.

        Parameters
        ----------
        query       : GDELT query string (supports AND, OR, NOT, quotes)
        timespan    : "7d", "24h", "1w", "1m", etc.
        max_records : 1–250 (GDELT maximum)
        sort        : "DateDesc" | "DateAsc" | "Relevance" | "ToneDesc"
        """
        params: dict[str, Any] = {
            "query": query,
            "mode": "ArtList",
            "maxrecords": min(max_records, 250),
            "timespan": timespan,
            "sort": sort,
            "format": "json",
        }
        if source_country:
            params["sourcelang"] = source_country
        if source_language:
            params["sourcelang"] = source_language

        data = await self._get(_BASE, params, use_cache=True)
        return _parse_articles(data)

    async def get_timeline_volume(
        self,
        query: str,
        timespan: str = "30d",
    ) -> list[dict[str, Any]]:
        """
        Fetch article-volume timeline for a query.

        Returns a list of {date, value} dicts.
        """
        params = {
            "query": query,
            "mode": "TimelineVol",
            "timespan": timespan,
            "format": "json",
        }
        data = await self._get(_BASE, params, use_cache=True)
        if isinstance(data, dict):
            return data.get("timeline", [{}])[0].get("data", [])  # type: ignore[return-value]
        return []

    async def get_tone_timeline(
        self,
        query: str,
        timespan: str = "30d",
    ) -> list[dict[str, Any]]:
        """Fetch sentiment-tone timeline for a query."""
        params = {
            "query": query,
            "mode": "ToneLine",
            "timespan": timespan,
            "format": "json",
        }
        data = await self._get(_BASE, params, use_cache=True)
        if isinstance(data, dict):
            return data.get("timeline", [{}])[0].get("data", [])  # type: ignore[return-value]
        return []

    async def get_energy_feed(
        self,
        topic_key: str = "natural_gas",
        timespan: str = "24h",
    ) -> list[GDELTEvent]:
        """Get articles for a pre-built energy topic query."""
        query = ENERGY_QUERIES.get(topic_key, topic_key)
        return await self.search_articles(query, timespan=timespan)

    async def get_all_energy_topics(
        self, timespan: str = "24h"
    ) -> dict[str, list[GDELTEvent]]:
        """Fetch all energy topic feeds."""
        results: dict[str, list[GDELTEvent]] = {}
        for topic, query in ENERGY_QUERIES.items():
            try:
                results[topic] = await self.search_articles(
                    query, timespan=timespan, max_records=50
                )
            except Exception as exc:
                logger.warning(f"[GDELT] topic {topic!r} failed: {exc}")
                results[topic] = []
        return results

    async def search_with_location_filter(
        self,
        query: str,
        lat: float,
        lon: float,
        radius_km: float = 200.0,
        timespan: str = "7d",
    ) -> list[GDELTEvent]:
        """
        Search and filter results to those with geocoordinates within
        ``radius_km`` of the given point.
        """
        articles = await self.search_articles(query, timespan=timespan)
        result = []
        for a in articles:
            if a.lat is not None and a.lon is not None:
                dist = _haversine_km(lat, lon, a.lat, a.lon)
                if dist <= radius_km:
                    result.append(a)
        return result

    async def health_check(self) -> bool:
        try:
            articles = await self.search_articles("oil", timespan="1d", max_records=5)
            return isinstance(articles, list)
        except Exception as exc:
            logger.warning(f"[GDELT] health check failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_articles(data: Any) -> list[GDELTEvent]:
    events = []
    if not isinstance(data, dict):
        return events

    for art in data.get("articles", []):
        try:
            # Parse date
            raw_date = art.get("seendate", "")
            try:
                pub_date = datetime.strptime(raw_date, "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                pub_date = datetime.now(tz=timezone.utc)

            url = art.get("url", "")
            title = art.get("title", art.get("seentitle", ""))
            tone = float(art.get("tone", 0))
            relevance = float(art.get("artrelevance", art.get("relevance", 0)))
            source_country = art.get("sourcecountry", "")
            language = art.get("language", "English")
            lat_raw = art.get("actiongeo_lat")
            lon_raw = art.get("actiongeo_long")

            # Stable ID from URL hash
            event_id = hashlib.md5(url.encode()).hexdigest()[:16]

            events.append(
                GDELTEvent(
                    event_id=event_id,
                    publish_date=pub_date,
                    url=url,
                    title=title[:500],
                    source_country=source_country,
                    language=language,
                    tone=tone,
                    relevance_score=relevance,
                    lat=float(lat_raw) if lat_raw else None,
                    lon=float(lon_raw) if lon_raw else None,
                    actors=[
                        a for a in [art.get("actor1name"), art.get("actor2name")] if a
                    ],
                    themes=art.get("themes", "").split(",") if art.get("themes") else [],
                )
            )
        except Exception as exc:
            logger.debug(f"[GDELT] skip article: {exc}")

    return events


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    import math
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(
        math.radians(lat2)
    ) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))
