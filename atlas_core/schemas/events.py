"""
Event / OSINT schemas.

Covers GDELT global events, NASA FIRMS fire detections, and composite
atlas-level alert records.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class EventDomain(str, Enum):
    ENERGY = "energy"
    WEATHER = "weather"
    FIRE = "fire"
    SHIPPING = "shipping"
    AVIATION = "aviation"
    GEOPOLITICS = "geopolitics"
    SPACE = "space"
    COMPOSITE = "composite"


class GDELTEvent(BaseModel):
    """
    GDELT DOC 2.0 article / event record.

    Field names follow GDELT column spec; we carry only what we need for
    commodity-relevant signal extraction.
    """
    event_id: str
    publish_date: datetime
    url: str = ""
    title: str
    source_country: str = ""
    language: str = "English"
    tone: float = 0.0                   # positive = positive sentiment
    relevance_score: float = 0.0        # GDELT artRelevance
    actors: list[str] = Field(default_factory=list)    # GDELT Actor1, Actor2
    locations: list[str] = Field(default_factory=list)
    themes: list[str] = Field(default_factory=list)
    lat: Optional[float] = None
    lon: Optional[float] = None
    source: str = "GDELT"


class FIRMSDetection(BaseModel):
    """NASA FIRMS active-fire thermal anomaly detection."""
    detection_id: str                       # unique per detection; derived
    satellite: str                          # "MODIS", "VIIRS_NOAA20", "VIIRS_SUOMI"
    acq_datetime: datetime
    lat: float
    lon: float
    brightness_k: float                     # brightness temperature (K)
    frp_mw: Optional[float] = None          # fire radiative power (MW) — VIIRS only
    confidence: str                         # "low" | "nominal" | "high" (VIIRS) or 0–100 int
    daynight: str = "D"                     # "D" | "N"
    source: str = "NASA_FIRMS"


class AtlasAlert(BaseModel):
    """
    Normalised atlas-level alert record.

    These are the items that appear in the Event Tape.  They are generated
    from raw connector data by the alert engine and scored for relevance to
    tracked assets.
    """

    class Severity(str, Enum):
        CRITICAL = "critical"
        HIGH = "high"
        MEDIUM = "medium"
        LOW = "low"
        INFO = "info"

    alert_id: str
    created_at: datetime
    domain: EventDomain
    severity: Severity
    title: str
    summary: str
    score: float = Field(ge=0.0, le=1.0, description="0–1 composite relevance/severity score")

    # Geographic anchor
    lat: Optional[float] = None
    lon: Optional[float] = None
    region: Optional[str] = None

    # Affected assets
    affected_assets: list[str] = Field(default_factory=list)
    affected_regions: list[str] = Field(default_factory=list)

    # Cross-domain links
    related_alert_ids: list[str] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)   # IDs in source tables

    # Raw metadata passthrough for drilldown
    metadata: dict = Field(default_factory=dict)

    # Optional LLM-generated explanation (only if Gemini key provided)
    llm_explanation: Optional[str] = None

    # Signal components for causal drilldown
    signal_components: list["SignalComponent"] = Field(default_factory=list)


class SignalComponent(BaseModel):
    """One component in a multi-factor signal decomposition."""
    name: str
    description: str
    value: float
    direction: str                      # "bullish" | "bearish" | "neutral"
    weight: float = Field(ge=0.0, le=1.0)
    historical_hit_rate: Optional[float] = None
    data_table: Optional[str] = None    # DuckDB table name for drilldown
    source_domain: EventDomain = EventDomain.COMPOSITE
