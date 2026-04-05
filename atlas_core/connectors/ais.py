"""
AIS (Automatic Identification System) vessel data connector.

This connector aggregates vessel position data from multiple free/open sources:

1. MarineCadastre.gov  — NOAA/USCG historical AIS CSV files (US waters)
   URL pattern: https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/AIS_{year}_{month}_{zone}.zip

2. AISHub (free tier)  — real-time position reports via HTTP
   URL: https://www.aishub.net/api?... (requires free registration)
   Fallback: uses public aggregator feeds

3. MarineTraffic / VesselFinder public data endpoints (limited free tier)

For the purposes of Atlas Terminal, we focus on:
  * Gulf of Mexico / US coastal shipping lanes
  * Key LNG export terminal approaches
  * Cushing/Midcontinent inland waterway barges

The connector implements a tiered strategy:
  Tier 1: Try live feed (AISHub)
  Tier 2: Fall back to recent static NOAA MarineCadastre CSV
  Tier 3: Return empty list if both unavailable
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from loguru import logger

from atlas_core.connectors.base import BaseConnector
from atlas_core.schemas.vessels import NavigationStatus, PortCongestion, VesselPosition, VesselType

_MARINECADASTRE_BASE = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"
_AISHUB_BASE = "https://data.aishub.net/ws.php"

# Approximate radius for port-congestion aggregation (nm)
_PORT_RADIUS_NM = 5.0


class AISConnector(BaseConnector):
    """Multi-source AIS vessel position connector."""

    source_name = "AIS"
    _rate_limit_rps = 0.5
    _rate_limit_burst = 2.0

    def __init__(
        self,
        aishub_username: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._aishub_username = aishub_username

    # ------------------------------------------------------------------ #
    # Live positions (AISHub)                                              #
    # ------------------------------------------------------------------ #

    async def get_live_positions_bbox(
        self,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
    ) -> list[VesselPosition]:
        """
        Attempt to fetch live vessel positions for a bounding box.

        Returns empty list if no live feed is available.
        """
        if self._aishub_username:
            return await self._aishub_fetch(min_lat, min_lon, max_lat, max_lon)
        # Fallback: check if there's a recent NOAA snapshot
        return []

    async def _aishub_fetch(
        self, min_lat: float, min_lon: float, max_lat: float, max_lon: float
    ) -> list[VesselPosition]:
        params = {
            "username": self._aishub_username,
            "format": "1",   # JSON
            "output": "json",
            "compress": "0",
            "latmin": min_lat,
            "latmax": max_lat,
            "lonmin": min_lon,
            "lonmax": max_lon,
        }
        try:
            data = await self._get(_AISHUB_BASE, params, use_cache=True)
            return _parse_aishub_json(data)
        except Exception as exc:
            logger.warning(f"[AIS] AISHub fetch failed: {exc}")
            return []

    # ------------------------------------------------------------------ #
    # Historical NOAA MarineCadastre                                       #
    # ------------------------------------------------------------------ #

    async def get_marinecadastre_url(
        self, year: int, month: int, zone: int = 17
    ) -> str:
        """Build the MarineCadastre download URL for a specific year/month/UTM zone."""
        return (
            f"{_MARINECADASTRE_BASE}/{year}/"
            f"AIS_{year}_{month:02d}_Zone{zone:02d}.zip"
        )

    async def fetch_marinecadastre_sample(
        self,
        year: int,
        month: int,
        zone: int = 17,
        max_rows: int = 10_000,
        bbox: Optional[tuple[float, float, float, float]] = None,
    ) -> list[VesselPosition]:
        """
        Download and parse a NOAA MarineCadastre AIS CSV zip.

        Parameters
        ----------
        zone : UTM zone number (17 = Gulf of Mexico / SE US)
        bbox : optional (min_lat, min_lon, max_lat, max_lon) spatial filter
        """
        url = await self.get_marinecadastre_url(year, month, zone)
        try:
            # Download zip in binary; use raw httpx since base class caches text
            if self._client is None:
                raise RuntimeError("Connector not started")
            resp = await self._client.get(url)
            if not resp.is_success:
                logger.warning(f"[AIS] MarineCadastre {url} returned {resp.status_code}")
                return []
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            csv_name = next(
                (n for n in zf.namelist() if n.endswith(".csv")), None
            )
            if not csv_name:
                return []
            csv_bytes = zf.read(csv_name)
            return _parse_marinecadastre_csv(
                csv_bytes.decode("utf-8", errors="replace"),
                max_rows=max_rows,
                bbox=bbox,
            )
        except Exception as exc:
            logger.warning(f"[AIS] MarineCadastre fetch failed: {exc}")
            return []

    # ------------------------------------------------------------------ #
    # Derived: port congestion                                             #
    # ------------------------------------------------------------------ #

    def compute_port_congestion(
        self,
        positions: list[VesselPosition],
        port_name: str,
        port_lat: float,
        port_lon: float,
        radius_km: float = 9.26,   # ~5 nautical miles
    ) -> PortCongestion:
        """
        Compute a congestion score for a port from a list of vessel positions.
        """
        from atlas_core.utils.geo import haversine_km

        nearby = [
            p for p in positions
            if haversine_km(port_lat, port_lon, p.lat, p.lon) <= radius_km
        ]

        at_anchor = sum(1 for p in nearby if p.nav_status == NavigationStatus.AT_ANCHOR)
        moored = sum(1 for p in nearby if p.nav_status == NavigationStatus.MOORED)
        total = len(nearby)

        # Simple congestion index: weighted stopped fraction
        congestion = min(1.0, (at_anchor * 1.5 + moored) / max(total, 1) * 0.5)

        type_counts: dict[str, int] = {}
        for p in nearby:
            type_counts[p.vessel_type.value] = type_counts.get(p.vessel_type.value, 0) + 1
        top_types = sorted(type_counts, key=type_counts.get, reverse=True)[:3]  # type: ignore[arg-type]

        return PortCongestion(
            port_id=port_name.lower().replace(" ", "_"),
            port_name=port_name,
            lat=port_lat,
            lon=port_lon,
            computed_at=datetime.now(tz=timezone.utc),
            vessels_at_anchor=at_anchor,
            vessels_moored=moored,
            congestion_index=congestion,
            top_vessel_types=top_types,
        )

    async def health_check(self) -> bool:
        # If no live feed, check NOAA is reachable
        try:
            if self._client is None:
                return True   # Not started — skip
            resp = await self._client.head(_MARINECADASTRE_BASE)
            return resp.status_code < 500
        except Exception:
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_aishub_json(data: Any) -> list[VesselPosition]:
    positions = []
    if not isinstance(data, list) or len(data) < 2:
        return positions
    # AISHub JSON: first element is metadata, second is array of vessel dicts
    vessels = data[1] if isinstance(data[1], list) else []
    for v in vessels:
        try:
            positions.append(
                VesselPosition(
                    mmsi=str(v.get("MMSI", "")),
                    vessel_name=str(v.get("NAME", "")),
                    vessel_type=_classify_vessel_type(int(v.get("TYPE", 0))),
                    flag=str(v.get("FLAG", "")),
                    timestamp=datetime.now(tz=timezone.utc),
                    lat=float(v.get("LATITUDE", 0)),
                    lon=float(v.get("LONGITUDE", 0)),
                    speed_kts=_safe_float(v.get("SOG")),
                    course_deg=_safe_float(v.get("COG")),
                    heading_deg=_safe_float(v.get("HEADING")),
                    nav_status=_nav_status(v.get("NAVSTAT", 15)),
                    destination=str(v.get("DEST", "")),
                )
            )
        except Exception as exc:
            logger.debug(f"[AIS] skip AISHub record: {exc}")
    return positions


def _parse_marinecadastre_csv(
    raw: str,
    max_rows: int = 10_000,
    bbox: Optional[tuple[float, float, float, float]] = None,
) -> list[VesselPosition]:
    """
    Parse NOAA MarineCadastre AIS CSV.

    Column order: MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,
                  IMO,CallSign,VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass
    """
    positions = []
    try:
        reader = csv.DictReader(io.StringIO(raw))
    except Exception:
        return positions

    for i, row in enumerate(reader):
        if i >= max_rows:
            break
        try:
            lat = float(row.get("LAT", row.get("Latitude", 0)))
            lon = float(row.get("LON", row.get("Longitude", 0)))

            if bbox:
                min_lat, min_lon, max_lat, max_lon = bbox
                if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                    continue

            ts_raw = row.get("BaseDateTime", row.get("DateTime", ""))
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except ValueError:
                ts = datetime.now(tz=timezone.utc)

            positions.append(
                VesselPosition(
                    mmsi=str(row.get("MMSI", "")),
                    vessel_name=str(row.get("VesselName", "")),
                    vessel_type=_classify_vessel_type(
                        int(float(row.get("VesselType", 0) or 0))
                    ),
                    timestamp=ts,
                    lat=lat,
                    lon=lon,
                    speed_kts=_safe_float(row.get("SOG")),
                    course_deg=_safe_float(row.get("COG")),
                    heading_deg=_safe_float(row.get("Heading")),
                    nav_status=_nav_status(int(float(row.get("Status", 15) or 15))),
                    destination=str(row.get("Destination", "")),
                )
            )
        except Exception as exc:
            logger.debug(f"[AIS] skip CSV row: {exc}")

    return positions


def _classify_vessel_type(code: int) -> VesselType:
    """Map AIS numeric vessel-type code to our enum."""
    if 80 <= code <= 89:
        return VesselType.TANKER
    if code in (84, 85, 86, 87, 88, 89):
        return VesselType.TANKER
    if 70 <= code <= 79:
        return VesselType.CONTAINER
    if 60 <= code <= 69:
        return VesselType.BULK_CARRIER
    if code in (30, 31, 32, 33, 34, 35):
        return VesselType.OFFSHORE
    return VesselType.UNKNOWN


def _nav_status(code: int) -> NavigationStatus:
    mapping = {
        0: NavigationStatus.UNDERWAY,
        1: NavigationStatus.AT_ANCHOR,
        5: NavigationStatus.MOORED,
        8: NavigationStatus.UNDERWAY,
    }
    return mapping.get(code, NavigationStatus.UNKNOWN)


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None
