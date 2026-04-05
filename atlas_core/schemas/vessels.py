"""
Vessel / aviation movement schemas.

AIS vessel positions, port congestion metrics, OpenSky aircraft positions.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class VesselType(str, Enum):
    TANKER = "tanker"
    LNG_CARRIER = "lng_carrier"
    BULK_CARRIER = "bulk_carrier"
    CONTAINER = "container"
    OFFSHORE = "offshore"
    OTHER = "other"
    UNKNOWN = "unknown"


class NavigationStatus(str, Enum):
    UNDERWAY = "underway"
    AT_ANCHOR = "at_anchor"
    MOORED = "moored"
    RESTRICTED = "restricted"
    UNKNOWN = "unknown"


class VesselPosition(BaseModel):
    """Single AIS position report."""
    mmsi: str                           # Maritime Mobile Service Identity
    vessel_name: str = ""
    vessel_type: VesselType = VesselType.UNKNOWN
    flag: str = ""                      # ISO 2-letter country code
    timestamp: datetime
    lat: float
    lon: float
    speed_kts: Optional[float] = None   # Speed over ground
    course_deg: Optional[float] = None  # Course over ground
    heading_deg: Optional[float] = None
    nav_status: NavigationStatus = NavigationStatus.UNKNOWN
    draught_m: Optional[float] = None
    destination: str = ""
    eta: Optional[datetime] = None
    source: str = "AIS"


class PortCongestion(BaseModel):
    """Derived congestion metric for a port or terminal."""
    port_id: str
    port_name: str
    lat: float
    lon: float
    computed_at: datetime
    vessels_at_anchor: int = 0
    vessels_moored: int = 0
    avg_dwell_hours: Optional[float] = None
    congestion_index: float = Field(ge=0.0, le=1.0)
    # Change from prior period
    congestion_delta: Optional[float] = None
    top_vessel_types: list[str] = Field(default_factory=list)


class VesselTrack(BaseModel):
    """Time-ordered sequence of positions for one voyage segment."""
    mmsi: str
    vessel_name: str = ""
    vessel_type: VesselType = VesselType.UNKNOWN
    positions: list[VesselPosition] = Field(default_factory=list)
    origin_port: Optional[str] = None
    destination_port: Optional[str] = None
    departure_time: Optional[datetime] = None
    arrival_time: Optional[datetime] = None
    total_distance_nm: Optional[float] = None


# ─── Aviation ─────────────────────────────────────────────────────────────────

class AircraftState(BaseModel):
    """OpenSky state vector for one aircraft."""
    icao24: str                         # ICAO 24-bit address
    callsign: str = ""
    origin_country: str = ""
    timestamp: datetime
    lat: Optional[float] = None
    lon: Optional[float] = None
    altitude_m: Optional[float] = None  # barometric altitude
    velocity_ms: Optional[float] = None
    heading_deg: Optional[float] = None
    vertical_rate_ms: Optional[float] = None
    on_ground: bool = False
    source: str = "OpenSky"


class FlightDensityGrid(BaseModel):
    """Aggregated flight density over a geographic grid cell."""
    cell_lat: float
    cell_lon: float
    cell_size_deg: float = 1.0
    period_start: datetime
    period_end: datetime
    flight_count: int
    unique_aircraft: int
    commercial_pct: Optional[float] = None
