"""
NOAA NDBC (National Data Buoy Center) connector.

No API key required.  Data is served as fixed-width text files.

Documentation : https://www.ndbc.noaa.gov/docs/ndbc_web_data_guide.pdf
Base URL      : https://www.ndbc.noaa.gov/data/realtime2/

Key buoy stations for Gulf of Mexico / offshore energy:
  42001 – Gulf of Mexico (mid)
  42002 – Gulf of Mexico (west)
  42003 – Gulf of Mexico (east)
  42036 – Gulf of Mexico (offshore FL)
  41047 – NE Atlantic (hurricane track)
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from loguru import logger

from atlas_core.connectors.base import BaseConnector
from atlas_core.schemas.weather import BuoyObservation

_BASE = "https://www.ndbc.noaa.gov/data/realtime2"

# Standard meteorological column order per NDBC spec
_STD_COLS = [
    "#YY", "MM", "DD", "hh", "mm",
    "WDIR", "WSPD", "GST", "WVHT", "DPD",
    "APD", "MWD", "PRES", "ATMP", "WTMP",
    "DEWP", "VIS", "PTDY", "TIDE",
]


class NDBCConnector(BaseConnector):
    """NOAA NDBC buoy data connector (real-time standard meteorological)."""

    source_name = "NDBC"
    _rate_limit_rps = 0.5
    _rate_limit_burst = 2.0

    # Known Gulf of Mexico / offshore energy-relevant stations
    GULF_STATIONS = ["42001", "42002", "42003", "42019", "42020",
                     "42035", "42036", "42039", "42040", "42055"]
    ATLANTIC_STATIONS = ["41047", "41048", "41049", "44025", "44013"]

    async def get_latest_observations(
        self, station_id: str, max_rows: int = 48
    ) -> list[BuoyObservation]:
        """
        Fetch the latest standard meteorological data for a buoy station.

        Returns at most ``max_rows`` records (45-day rolling file).
        """
        url = f"{_BASE}/{station_id}.txt"
        raw = await self._get(url, use_cache=True)
        if not isinstance(raw, str):
            return []
        return _parse_stdmet(station_id, raw, max_rows)

    async def get_latest_wave_data(
        self, station_id: str
    ) -> list[BuoyObservation]:
        """Fetch spectral wave summary data (.spec file)."""
        url = f"{_BASE}/{station_id}.spec"
        raw = await self._get(url, use_cache=True)
        if not isinstance(raw, str):
            return []
        return _parse_stdmet(station_id, raw, max_rows=48)

    async def get_all_gulf_stations(self) -> dict[str, list[BuoyObservation]]:
        """Fetch latest obs for all tracked Gulf of Mexico stations."""
        results = {}
        for sid in self.GULF_STATIONS:
            try:
                obs = await self.get_latest_observations(sid, max_rows=1)
                if obs:
                    results[sid] = obs
            except Exception as exc:
                logger.debug(f"[NDBC] station {sid} unavailable: {exc}")
        return results

    async def get_station_metadata(self) -> pd.DataFrame:
        """
        Fetch the NDBC active station list as a DataFrame.
        Returns: station_id, name, lat, lon, type, etc.
        """
        url = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
        raw = await self._get(url, use_cache=True)
        if not isinstance(raw, str):
            return pd.DataFrame()
        return _parse_station_table(raw)

    async def health_check(self) -> bool:
        try:
            obs = await self.get_latest_observations("42001", max_rows=1)
            return len(obs) > 0
        except Exception as exc:
            logger.warning(f"[NDBC] health check failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_stdmet(
    station_id: str, raw: str, max_rows: int = 48
) -> list[BuoyObservation]:
    observations = []
    lines = [l for l in raw.splitlines() if l.strip()]
    if len(lines) < 3:
        return observations

    # NDBC files: line 0 = column names (#YY MM DD ...), line 1 = units, line 2+ = data
    header_line = lines[0].lstrip("#").split()
    # Skip units line
    data_lines = lines[2:]

    for line in data_lines[:max_rows]:
        parts = line.split()
        if len(parts) < 5:
            continue
        try:
            row = dict(zip(header_line, parts))
            yr = int(row.get("YY", 0))
            if yr < 100:
                yr += 2000
            ts = datetime(
                yr,
                int(row.get("MM", 1)),
                int(row.get("DD", 1)),
                int(row.get("hh", 0)),
                int(row.get("mm", 0)),
                tzinfo=timezone.utc,
            )

            def _val(key: str) -> Optional[float]:
                v = row.get(key)
                if v in (None, "MM", "99.0", "999.0", "9999.0"):
                    return None
                try:
                    return float(v)  # type: ignore[arg-type]
                except ValueError:
                    return None

            observations.append(
                BuoyObservation(
                    station_id=station_id,
                    timestamp=ts,
                    lat=0.0,    # populated by caller if needed
                    lon=0.0,
                    wind_dir_deg=_val("WDIR"),
                    wind_speed_ms=_val("WSPD"),
                    wind_gust_ms=_val("GST"),
                    wave_height_m=_val("WVHT"),
                    dominant_period_s=_val("DPD"),
                    air_temp_c=_val("ATMP"),
                    sea_surface_temp_c=_val("WTMP"),
                    air_pressure_hpa=_val("PRES"),
                    visibility_nmi=_val("VIS"),
                )
            )
        except Exception as exc:
            logger.debug(f"[NDBC] skip row: {exc}")

    return observations


def _parse_station_table(raw: str) -> pd.DataFrame:
    try:
        # NDBC station table is pipe-separated
        lines = [l for l in raw.splitlines() if "|" in l]
        if not lines:
            return pd.DataFrame()
        rows = [l.split("|") for l in lines]
        if not rows:
            return pd.DataFrame()
        cols = [c.strip() for c in rows[0]]
        data = [[c.strip() for c in r] for r in rows[1:] if len(r) >= len(cols)]
        return pd.DataFrame(data, columns=cols)
    except Exception as exc:
        logger.warning(f"[NDBC] station table parse error: {exc}")
        return pd.DataFrame()
