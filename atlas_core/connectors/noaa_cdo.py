"""
NOAA Climate Data Online (CDO) connector.

Documentation : https://www.ncdc.noaa.gov/cdo-web/webservices/v2
Token         : https://www.ncdc.noaa.gov/cdo-web/token  (free, email)

Provides historical climate observations: TMAX, TMIN, PRCP, SNOW, AWND, etc.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from loguru import logger

from atlas_core.config import settings
from atlas_core.connectors.base import BaseConnector
from atlas_core.exceptions import ConnectorNotConfiguredError
from atlas_core.schemas.weather import ClimateRecord, ClimateStation

_BASE = "https://www.ncdc.noaa.gov/cdo-web/api/v2"


class NOAACDOConnector(BaseConnector):
    """NOAA Climate Data Online API v2 connector."""

    source_name = "NOAA_CDO"
    _rate_limit_rps = 0.5      # NOAA CDO limit: ~1 000 reqs/day → ~0.01/s; be gentle
    _rate_limit_burst = 2.0

    def __init__(self, token: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._token = token or settings.noaa_cdo_token
        if not self._token:
            raise ConnectorNotConfiguredError(
                "NOAA CDO token not configured.  Set NOAA_CDO_TOKEN env var.",
                source=self.source_name,
            )

    def _auth_headers(self) -> dict[str, str]:
        return {"token": self._token}

    async def _cdo_get(self, endpoint: str, params: dict[str, Any]) -> Any:
        return await self._get(
            f"{_BASE}/{endpoint}",
            params,
            headers=self._auth_headers(),
        )

    # ------------------------------------------------------------------ #
    # Stations                                                             #
    # ------------------------------------------------------------------ #

    async def find_stations(
        self,
        dataset_id: str = "GHCND",
        extent: Optional[str] = None,   # "lat_min,lon_min,lat_max,lon_max"
        location_id: Optional[str] = None,
        data_type_id: Optional[str] = None,
        limit: int = 25,
    ) -> list[ClimateStation]:
        params: dict[str, Any] = {"datasetid": dataset_id, "limit": limit}
        if extent:
            params["extent"] = extent
        if location_id:
            params["locationid"] = location_id
        if data_type_id:
            params["datatypeid"] = data_type_id

        data = await self._cdo_get("stations", params)
        return _parse_stations(data)

    async def get_station(self, station_id: str) -> Optional[ClimateStation]:
        try:
            data = await self._cdo_get(f"stations/{station_id}", {})
            return _parse_station(data)
        except Exception:
            return None

    # ------------------------------------------------------------------ #
    # Data fetch                                                           #
    # ------------------------------------------------------------------ #

    async def get_data(
        self,
        dataset_id: str,
        data_type_ids: list[str],
        station_ids: list[str],
        start_date: str,      # "YYYY-MM-DD"
        end_date: str,
        limit: int = 1000,
        offset: int = 1,
    ) -> list[ClimateRecord]:
        """
        Fetch CDO observations for one or more stations and data types.

        CDO returns at most 1 000 rows per request; callers should paginate
        if they need more.
        """
        params: dict[str, Any] = {
            "datasetid": dataset_id,
            "datatypeid": ",".join(data_type_ids),
            "stationid": ",".join(station_ids),
            "startdate": start_date,
            "enddate": end_date,
            "limit": min(limit, 1000),
            "offset": offset,
            "units": "standard",
            "includemetadata": "false",
        }
        data = await self._cdo_get("data", params)
        return _parse_records(data)

    async def get_data_all_pages(
        self,
        dataset_id: str,
        data_type_ids: list[str],
        station_ids: list[str],
        start_date: str,
        end_date: str,
        max_records: int = 10_000,
    ) -> list[ClimateRecord]:
        """Paginate through all available records (up to ``max_records``)."""
        records: list[ClimateRecord] = []
        offset = 1
        while len(records) < max_records:
            page = await self.get_data(
                dataset_id, data_type_ids, station_ids,
                start_date, end_date,
                limit=1000, offset=offset,
            )
            if not page:
                break
            records.extend(page)
            offset += 1000
        return records[:max_records]

    # ------------------------------------------------------------------ #
    # Health                                                               #
    # ------------------------------------------------------------------ #

    async def health_check(self) -> bool:
        try:
            data = await self._cdo_get("datasets", {"limit": 1})
            return isinstance(data, dict) and "results" in data
        except Exception as exc:
            logger.warning(f"[NOAA_CDO] health check failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_stations(data: Any) -> list[ClimateStation]:
    if not isinstance(data, dict):
        return []
    return [_parse_station(s) for s in data.get("results", []) if s]


def _parse_station(raw: Any) -> ClimateStation:
    return ClimateStation(
        station_id=raw.get("id", ""),
        name=raw.get("name", ""),
        latitude=float(raw.get("latitude", 0)),
        longitude=float(raw.get("longitude", 0)),
        elevation_m=raw.get("elevation"),
        min_date=raw.get("mindate"),
        max_date=raw.get("maxdate"),
    )


def _parse_records(data: Any) -> list[ClimateRecord]:
    if not isinstance(data, dict):
        return []
    records = []
    for r in data.get("results", []):
        try:
            records.append(
                ClimateRecord(
                    station_id=r.get("station", ""),
                    date=str(r.get("date", ""))[:10],
                    data_type=r.get("datatype", ""),
                    value=float(r.get("value", 0)),
                    attributes=r.get("attributes", ""),
                )
            )
        except Exception as exc:
            logger.debug(f"[NOAA_CDO] skip record: {exc}")
    return records
