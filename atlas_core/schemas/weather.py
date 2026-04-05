"""
Weather and meteorology Pydantic schemas.

Covers NWS, NOAA CDO, NDBC buoys, NOAA SWPC space weather.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class AlertSeverity(str, Enum):
    EXTREME = "Extreme"
    SEVERE = "Severe"
    MODERATE = "Moderate"
    MINOR = "Minor"
    UNKNOWN = "Unknown"


class AlertCertainty(str, Enum):
    OBSERVED = "Observed"
    LIKELY = "Likely"
    POSSIBLE = "Possible"
    UNLIKELY = "Unlikely"
    UNKNOWN = "Unknown"


# ─── NWS Alerts ──────────────────────────────────────────────────────────────

class NWSAlert(BaseModel):
    """National Weather Service active alert."""
    alert_id: str
    headline: str
    description: str = ""
    event_type: str                     # "Tornado Warning", "Hurricane Watch", etc.
    severity: AlertSeverity
    certainty: AlertCertainty
    urgency: str
    onset: Optional[datetime] = None
    expires: Optional[datetime] = None
    affected_zones: list[str] = Field(default_factory=list)
    affected_area_wkt: Optional[str] = None   # WKT geometry
    centroid_lat: Optional[float] = None
    centroid_lon: Optional[float] = None
    source: str = "NWS"


# ─── NWS Forecast ────────────────────────────────────────────────────────────

class ForecastPeriod(BaseModel):
    """Single period in an NWS gridpoint forecast."""
    name: str                           # "Today", "Tonight", "Monday", ...
    start_time: datetime
    end_time: datetime
    is_daytime: bool
    temperature_f: float
    wind_speed: str                     # NWS returns strings like "10 mph"
    wind_direction: str
    short_forecast: str
    detailed_forecast: str
    precipitation_pct: Optional[float] = None


class GridpointForecast(BaseModel):
    """NWS point forecast for a lat/lon location."""
    lat: float
    lon: float
    office: str
    grid_x: int
    grid_y: int
    timezone: str
    periods: list[ForecastPeriod] = Field(default_factory=list)
    generated_at: Optional[datetime] = None


# ─── NOAA CDO Climate ─────────────────────────────────────────────────────────

class ClimateStation(BaseModel):
    """NOAA CDO station metadata."""
    station_id: str
    name: str
    latitude: float
    longitude: float
    elevation_m: Optional[float] = None
    data_types: list[str] = Field(default_factory=list)
    min_date: Optional[str] = None
    max_date: Optional[str] = None


class ClimateRecord(BaseModel):
    """NOAA CDO data record (one observation for one station/date/type)."""
    station_id: str
    date: str                           # ISO date
    data_type: str                      # "TMAX", "TMIN", "PRCP", "SNOW", etc.
    value: float
    attributes: str = ""                # QC flags
    unit: str = ""


class HDDCDDRecord(BaseModel):
    """Heating / cooling degree-day record."""
    date: str
    region: str
    hdd: float = 0.0                    # heating degree days
    cdd: float = 0.0                    # cooling degree days
    baseline_temp_f: float = 65.0
    source: str = "NOAA"


# ─── NDBC Buoy ────────────────────────────────────────────────────────────────

class BuoyObservation(BaseModel):
    """NDBC standard meteorological observation."""
    station_id: str
    timestamp: datetime
    lat: float
    lon: float
    wind_dir_deg: Optional[float] = None
    wind_speed_ms: Optional[float] = None
    wind_gust_ms: Optional[float] = None
    wave_height_m: Optional[float] = None
    dominant_period_s: Optional[float] = None
    air_temp_c: Optional[float] = None
    sea_surface_temp_c: Optional[float] = None
    air_pressure_hpa: Optional[float] = None
    visibility_nmi: Optional[float] = None
    source: str = "NDBC"


# ─── NOAA SWPC Space Weather ──────────────────────────────────────────────────

class GeomagneticKIndex(BaseModel):
    """3-hour K-index observation from NOAA SWPC."""
    timestamp: datetime
    k_index: float = Field(ge=0.0, le=9.0)
    station: str = "Boulder"
    source: str = "NOAA_SWPC"


class SpaceWeatherAlert(BaseModel):
    """NOAA SWPC space weather alert / watch / warning."""
    alert_id: str
    issued_time: datetime
    product: str                        # "G1", "R2", "S3", etc.
    category: str                       # "Geomagnetic", "Radio", "Solar Radiation"
    message: str
    source: str = "NOAA_SWPC"


# ─── Derived: Weather risk ────────────────────────────────────────────────────

class WeatherRiskScore(BaseModel):
    """Weather-driven risk score for a physical asset."""
    asset_id: str
    asset_type: str                     # "refinery", "pipeline", "port", "terminal"
    lat: float
    lon: float
    score: float = Field(ge=0.0, le=1.0)
    active_alerts: list[str] = Field(default_factory=list)
    storm_proximity_km: Optional[float] = None
    extreme_temp_flag: bool = False
    high_wind_flag: bool = False
    flood_risk_flag: bool = False
    computed_at: Optional[datetime] = None
