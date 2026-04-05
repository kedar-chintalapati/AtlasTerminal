"""
OpenSky Network connector.

Documentation : https://openskynetwork.github.io/opensky-api/
REST API base : https://opensky-network.org/api

Authentication is optional (anonymous allowed) but rate limits are tighter:
  anonymous : 100 reqs / day, 10-second delay
  registered: 4 000 reqs / day, 5-second delay (state vectors)

We gracefully degrade to anonymous if credentials are not set.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger

from atlas_core.config import settings
from atlas_core.connectors.base import BaseConnector
from atlas_core.schemas.vessels import AircraftState, FlightDensityGrid

_BASE = "https://opensky-network.org/api"


class OpenSkyConnector(BaseConnector):
    """OpenSky Network live and historical aircraft data connector."""

    source_name = "OpenSky"
    _rate_limit_rps = 0.2   # very conservative for anonymous access
    _rate_limit_burst = 1.0

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._username = username or settings.opensky_username
        self._password = password or settings.opensky_password

    def _auth_headers(self) -> dict[str, str]:
        if self._username and self._password:
            creds = base64.b64encode(
                f"{self._username}:{self._password}".encode()
            ).decode()
            return {"Authorization": f"Basic {creds}"}
        return {}

    async def _opensky_get(
        self, endpoint: str, params: Optional[dict[str, Any]] = None
    ) -> Any:
        return await self._get(
            f"{_BASE}/{endpoint}",
            params or {},
            headers=self._auth_headers(),
        )

    # ------------------------------------------------------------------ #
    # State vectors (live positions)                                       #
    # ------------------------------------------------------------------ #

    async def get_states_all(
        self,
        bbox: Optional[tuple[float, float, float, float]] = None,  # (lamin, lomin, lamax, lomax)
        time: Optional[int] = None,  # Unix timestamp; 0 = latest
    ) -> list[AircraftState]:
        """
        Fetch all state vectors (live aircraft positions).

        Parameters
        ----------
        bbox : (min_lat, min_lon, max_lat, max_lon) — optional bounding box
        time : Unix timestamp (0 or None = latest)
        """
        params: dict[str, Any] = {}
        if time:
            params["time"] = time
        if bbox:
            params["lamin"], params["lomin"], params["lamax"], params["lomax"] = bbox

        data = await self._opensky_get("states/all", params)
        return _parse_states(data)

    async def get_states_bbox(
        self,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
    ) -> list[AircraftState]:
        return await self.get_states_all(bbox=(min_lat, min_lon, max_lat, max_lon))

    # ------------------------------------------------------------------ #
    # Aircraft tracks / flights                                            #
    # ------------------------------------------------------------------ #

    async def get_flights_by_airport(
        self,
        airport_icao: str,
        begin: int,
        end: int,
        direction: str = "departure",
    ) -> list[dict[str, Any]]:
        """
        Historical flight data for an airport (arrivals or departures).

        Interval: begin–end may not exceed 7 days.  Unix timestamps.
        """
        endpoint = (
            f"flights/departure" if direction == "departure" else "flights/arrival"
        )
        data = await self._opensky_get(
            endpoint, {"airport": airport_icao, "begin": begin, "end": end}
        )
        return data if isinstance(data, list) else []  # type: ignore[return-value]

    async def get_track(
        self, icao24: str, time: int = 0
    ) -> list[AircraftState]:
        """Get full track for one aircraft."""
        data = await self._opensky_get("tracks/all", {"icao24": icao24, "time": time})
        if not isinstance(data, dict):
            return []
        path = data.get("path", [])
        states = []
        for point in path:
            # [time, lat, lon, baro_alt, true_track, on_ground]
            try:
                states.append(
                    AircraftState(
                        icao24=icao24,
                        timestamp=datetime.fromtimestamp(point[0], tz=timezone.utc),
                        lat=point[1],
                        lon=point[2],
                        altitude_m=point[3],
                        heading_deg=point[4],
                        on_ground=bool(point[5]),
                    )
                )
            except (IndexError, TypeError):
                pass
        return states

    # ------------------------------------------------------------------ #
    # Derived metrics                                                      #
    # ------------------------------------------------------------------ #

    async def get_flight_density(
        self,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
        cell_size_deg: float = 1.0,
    ) -> list[FlightDensityGrid]:
        """
        Compute flight density over a geographic grid by binning live states.
        """
        states = await self.get_states_bbox(min_lat, min_lon, max_lat, max_lon)
        if not states:
            return []

        now = datetime.now(tz=timezone.utc)
        grid: dict[tuple[float, float], list[AircraftState]] = {}

        for s in states:
            if s.lat is None or s.lon is None:
                continue
            cell = (
                round(s.lat // cell_size_deg * cell_size_deg, 6),
                round(s.lon // cell_size_deg * cell_size_deg, 6),
            )
            grid.setdefault(cell, []).append(s)

        cells = []
        for (cell_lat, cell_lon), cell_states in grid.items():
            unique_icao = len({s.icao24 for s in cell_states})
            cells.append(
                FlightDensityGrid(
                    cell_lat=cell_lat,
                    cell_lon=cell_lon,
                    cell_size_deg=cell_size_deg,
                    period_start=now,
                    period_end=now,
                    flight_count=len(cell_states),
                    unique_aircraft=unique_icao,
                )
            )
        return cells

    # ------------------------------------------------------------------ #
    # Health                                                               #
    # ------------------------------------------------------------------ #

    async def health_check(self) -> bool:
        try:
            # Tiny bbox over KIAH
            states = await self.get_states_bbox(29.9, -95.4, 30.0, -95.3)
            return isinstance(states, list)
        except Exception as exc:
            logger.warning(f"[OpenSky] health check failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_states(data: Any) -> list[AircraftState]:
    states = []
    if not isinstance(data, dict):
        return states

    ts = data.get("time", 0)
    timestamp = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(tz=timezone.utc)

    for sv in data.get("states", []):
        # OpenSky state vector fields:
        # [icao24, callsign, origin_country, time_position, last_contact,
        #  longitude, latitude, baro_altitude, on_ground, velocity,
        #  true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source]
        try:
            if sv is None or len(sv) < 8:
                continue
            states.append(
                AircraftState(
                    icao24=str(sv[0] or ""),
                    callsign=str(sv[1] or "").strip(),
                    origin_country=str(sv[2] or ""),
                    timestamp=timestamp,
                    lat=float(sv[6]) if sv[6] is not None else None,
                    lon=float(sv[5]) if sv[5] is not None else None,
                    altitude_m=float(sv[7]) if sv[7] is not None else None,
                    velocity_ms=float(sv[9]) if sv[9] is not None else None,
                    heading_deg=float(sv[10]) if sv[10] is not None else None,
                    vertical_rate_ms=float(sv[11]) if sv[11] is not None else None,
                    on_ground=bool(sv[8]),
                )
            )
        except (IndexError, TypeError, ValueError) as exc:
            logger.debug(f"[OpenSky] skip state vector: {exc}")

    return states
