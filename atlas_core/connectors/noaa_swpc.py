"""
NOAA Space Weather Prediction Center (SWPC) connector.

No authentication required.
JSON feeds: https://services.swpc.noaa.gov/json/

Key products
------------
* K-index (planetary, 3-hour)         — planetary_k_index_1m.json
* Current alerts/warnings/watches     — alerts.json
* Solar wind (DSCOVR L1)              — rtsw/rtsw_wind.json
* Geomagnetic storm forecast          — geospace/forecast/geomagnetic_forecast.json
* Energetic proton flux               — goes/secondary/energetics.json
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger

from atlas_core.connectors.base import BaseConnector
from atlas_core.schemas.weather import GeomagneticKIndex, SpaceWeatherAlert

_BASE = "https://services.swpc.noaa.gov"


class NOAASWPCConnector(BaseConnector):
    """NOAA SWPC space-weather data connector."""

    source_name = "NOAA_SWPC"
    _rate_limit_rps = 0.5
    _rate_limit_burst = 3.0

    async def get_k_index(self) -> list[GeomagneticKIndex]:
        """Fetch last 24h of 1-minute planetary K-index estimates."""
        data = await self._get(
            f"{_BASE}/json/planetary_k_index_1m.json", use_cache=True
        )
        return _parse_k_index(data)

    async def get_3hr_k_index(self) -> list[GeomagneticKIndex]:
        """Fetch 3-hour K-index going back ~30 days."""
        data = await self._get(
            f"{_BASE}/products/noaa-planetary-k-index.json", use_cache=True
        )
        return _parse_k_index(data)

    async def get_alerts(self) -> list[SpaceWeatherAlert]:
        """Fetch current SWPC alerts, watches, and warnings."""
        data = await self._get(f"{_BASE}/products/alerts.json", use_cache=False)
        return _parse_swpc_alerts(data)

    async def get_solar_wind(self) -> dict[str, Any]:
        """Real-time solar-wind data from DSCOVR L1."""
        return await self._get(  # type: ignore[return-value]
            f"{_BASE}/products/solar-wind/mag-7-day.json", use_cache=True
        )

    async def get_geomagnetic_forecast(self) -> list[dict[str, Any]]:
        """3-day geomagnetic storm forecast (Kp index)."""
        data = await self._get(
            f"{_BASE}/products/noaa-geomagnetic-forecast.json", use_cache=True
        )
        return data if isinstance(data, list) else []  # type: ignore[return-value]

    async def get_aurora_forecast(self) -> dict[str, Any]:
        """30-minute aurora forecast probabilities."""
        return await self._get(  # type: ignore[return-value]
            f"{_BASE}/json/ovation_aurora_latest.json", use_cache=True
        )

    async def health_check(self) -> bool:
        try:
            k = await self.get_k_index()
            return len(k) > 0
        except Exception as exc:
            logger.warning(f"[NOAA_SWPC] health check failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_k_index(data: Any) -> list[GeomagneticKIndex]:
    records = []
    if not isinstance(data, list):
        return records
    for row in data:
        try:
            # Some endpoints return ["2024-01-01 00:00:00", "3.33"]
            # Others return {"time_tag": "...", "kp": 3.33}
            if isinstance(row, list) and len(row) >= 2:
                ts_str, kp_str = str(row[0]), str(row[1])
                kp = float(kp_str) if kp_str not in ("-1", "") else None
            elif isinstance(row, dict):
                ts_str = row.get("time_tag", "")
                kp = float(row.get("kp_index", row.get("kp", -1)))
                if kp < 0:
                    continue
            else:
                continue

            if kp is None:
                continue

            ts = _parse_swpc_dt(ts_str)
            if ts is None:
                continue

            records.append(GeomagneticKIndex(timestamp=ts, k_index=min(9.0, max(0.0, kp))))
        except Exception as exc:
            logger.debug(f"[SWPC] skip k-index row: {exc}")
    return records


def _parse_swpc_alerts(data: Any) -> list[SpaceWeatherAlert]:
    alerts = []
    if not isinstance(data, list):
        return alerts
    for item in data:
        try:
            if not isinstance(item, dict):
                continue
            alerts.append(
                SpaceWeatherAlert(
                    alert_id=str(item.get("message_code", item.get("serial_number", ""))),
                    issued_time=_parse_swpc_dt(item.get("issue_time", "")) or datetime.now(tz=timezone.utc),
                    product=str(item.get("message_code", "")),
                    category=_classify_swpc_product(str(item.get("message_code", ""))),
                    message=str(item.get("message", ""))[:500],
                )
            )
        except Exception as exc:
            logger.debug(f"[SWPC] skip alert: {exc}")
    return alerts


def _parse_swpc_dt(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _classify_swpc_product(code: str) -> str:
    code = code.upper()
    if code.startswith("G"):
        return "Geomagnetic"
    if code.startswith("R"):
        return "Radio"
    if code.startswith("S"):
        return "Solar Radiation"
    return "Other"
