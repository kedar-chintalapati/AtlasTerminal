"""
NASA FIRMS (Fire Information for Resource Management System) connector.

Documentation : https://firms.modaps.eosdis.nasa.gov/api/
MAP_KEY       : https://firms.modaps.eosdis.nasa.gov/api/area/  (free, registration)

Satellite products
------------------
MODIS_NRT  — MODIS Near Real-Time (Terra + Aqua), ~3h latency
VIIRS_NOAA20_NRT — VIIRS NOAA-20, ~3h latency
VIIRS_SNPP_NRT   — VIIRS Suomi NPP, ~3h latency

The API returns CSV; we parse it into FIRMSDetection records.

Energy relevance
----------------
Active fires near:
  * Production basins (Permian, Eagle Ford, Bakken, Appalachian, etc.)
  * Pipeline corridors
  * Transmission lines and substations
  * Ports and LNG terminals
  * Refinery complexes

can affect operations, logistics, power loads, and prices.
"""

from __future__ import annotations

import csv
import io
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger

from atlas_core.config import settings
from atlas_core.connectors.base import BaseConnector
from atlas_core.exceptions import ConnectorNotConfiguredError
from atlas_core.schemas.events import FIRMSDetection

_BASE = "https://firms.modaps.eosdis.nasa.gov/api"

# Supported FIRMS products
FIRMS_PRODUCTS = {
    "MODIS_NRT": "MODIS_C6_1",
    "VIIRS_NOAA20": "VIIRS_NOAA20_NRT",
    "VIIRS_SNPP": "VIIRS_SNPP_NRT",
}


class NASAFIRMSConnector(BaseConnector):
    """NASA FIRMS fire detection connector."""

    source_name = "NASA_FIRMS"
    _rate_limit_rps = 0.5   # FIRMS documents conservative rate limits
    _rate_limit_burst = 2.0

    def __init__(self, map_key: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._map_key = map_key or settings.nasa_firms_map_key
        if not self._map_key:
            raise ConnectorNotConfiguredError(
                "NASA FIRMS MAP_KEY not configured.  Set NASA_FIRMS_MAP_KEY env var.",
                source=self.source_name,
            )

    # ------------------------------------------------------------------ #
    # Area fire detections                                                 #
    # ------------------------------------------------------------------ #

    async def get_fire_detections_bbox(
        self,
        west: float,
        south: float,
        east: float,
        north: float,
        days: int = 1,
        product: str = "VIIRS_SNPP",
    ) -> list[FIRMSDetection]:
        """
        Fetch fire detections for a bounding box over the last ``days`` days.

        Parameters
        ----------
        west, south, east, north : bounding box in WGS-84 decimal degrees
        days : 1, 2, 3, 5, 7, or 10
        product : key from FIRMS_PRODUCTS dict
        """
        days = max(1, min(days, 10))
        product_code = FIRMS_PRODUCTS.get(product, product)
        area_str = f"{west},{south},{east},{north}"

        url = f"{_BASE}/area/csv/{self._map_key}/{product_code}/{area_str}/{days}"
        raw = await self._get(url, use_cache=True)
        if not isinstance(raw, str):
            return []
        return _parse_firms_csv(raw, satellite=product)

    async def get_fires_gulf_coast(self, days: int = 3) -> list[FIRMSDetection]:
        """Convenience: fires in the Gulf Coast energy corridor."""
        return await self.get_fire_detections_bbox(
            west=-97.0, south=25.0, east=-88.0, north=31.5, days=days
        )

    async def get_fires_permian_basin(self, days: int = 3) -> list[FIRMSDetection]:
        """Fires in the Permian Basin production area."""
        return await self.get_fire_detections_bbox(
            west=-105.0, south=28.0, east=-99.0, north=34.5, days=days
        )

    async def get_fires_west_coast(self, days: int = 3) -> list[FIRMSDetection]:
        """Fires in western US (wildfire / transmission corridor risk)."""
        return await self.get_fire_detections_bbox(
            west=-125.0, south=32.0, east=-114.0, north=49.5, days=days
        )

    async def get_fires_for_bbox(
        self,
        min_lat: float, min_lon: float,
        max_lat: float, max_lon: float,
        days: int = 2,
    ) -> list[FIRMSDetection]:
        """Generic wrapper matching GeoBoundingBox field order."""
        return await self.get_fire_detections_bbox(
            west=min_lon, south=min_lat, east=max_lon, north=max_lat, days=days
        )

    # ------------------------------------------------------------------ #
    # Country-level feed                                                   #
    # ------------------------------------------------------------------ #

    async def get_fires_country(
        self,
        country: str = "USA",
        days: int = 1,
        product: str = "VIIRS_SNPP",
    ) -> list[FIRMSDetection]:
        """Fetch all fire detections for a country over last ``days`` days."""
        product_code = FIRMS_PRODUCTS.get(product, product)
        url = f"{_BASE}/country/csv/{self._map_key}/{product_code}/{country}/{days}"
        raw = await self._get(url, use_cache=True)
        if not isinstance(raw, str):
            return []
        return _parse_firms_csv(raw, satellite=product)

    # ------------------------------------------------------------------ #
    # Health                                                               #
    # ------------------------------------------------------------------ #

    async def health_check(self) -> bool:
        try:
            # Validate MAP_KEY against a tiny area
            fires = await self.get_fire_detections_bbox(
                west=-90.5, south=29.9, east=-90.4, north=30.0, days=1
            )
            return isinstance(fires, list)
        except Exception as exc:
            logger.warning(f"[NASA_FIRMS] health check failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_firms_csv(raw: str, satellite: str = "VIIRS_SNPP") -> list[FIRMSDetection]:
    detections = []
    try:
        reader = csv.DictReader(io.StringIO(raw))
    except Exception:
        return detections

    for row in reader:
        try:
            lat = float(row.get("latitude", 0))
            lon = float(row.get("longitude", 0))

            # Build acquisition datetime
            acq_date = row.get("acq_date", "")
            acq_time = str(row.get("acq_time", "0000")).zfill(4)
            try:
                ts = datetime.strptime(
                    f"{acq_date} {acq_time}", "%Y-%m-%d %H%M"
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                ts = datetime.now(tz=timezone.utc)

            brightness_raw = row.get("bright_ti4") or row.get("brightness") or row.get("bright_t31", "0")
            frp_raw = row.get("frp")

            # Deterministic ID from lat/lon/time
            det_id = f"{satellite}_{lat:.4f}_{lon:.4f}_{ts.strftime('%Y%m%d%H%M')}"

            detections.append(
                FIRMSDetection(
                    detection_id=det_id,
                    satellite=satellite,
                    acq_datetime=ts,
                    lat=lat,
                    lon=lon,
                    brightness_k=float(brightness_raw) if brightness_raw else 300.0,
                    frp_mw=float(frp_raw) if frp_raw else None,
                    confidence=str(row.get("confidence", "nominal")),
                    daynight=str(row.get("daynight", "D")),
                )
            )
        except Exception as exc:
            logger.debug(f"[FIRMS] skip row: {exc}")

    return detections
