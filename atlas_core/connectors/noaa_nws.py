"""
NOAA / NWS connector.

Covers
------
* NWS active alerts          — api.weather.gov/alerts/active
* NWS gridpoint forecasts    — api.weather.gov/points/{lat},{lon}
* NWS point-observation data — api.weather.gov/stations/{id}/observations

No authentication required.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger

from atlas_core.connectors.base import BaseConnector
from atlas_core.exceptions import ConnectorParseError
from atlas_core.schemas.weather import (
    AlertCertainty,
    AlertSeverity,
    ForecastPeriod,
    GridpointForecast,
    NWSAlert,
)

_BASE = "https://api.weather.gov"


class NWSConnector(BaseConnector):
    """National Weather Service API connector (api.weather.gov)."""

    source_name = "NWS"
    _rate_limit_rps = 2.0
    _rate_limit_burst = 5.0

    # ------------------------------------------------------------------ #
    # Alerts                                                               #
    # ------------------------------------------------------------------ #

    async def get_active_alerts(
        self,
        area: Optional[str] = None,         # state code e.g. "TX"
        event: Optional[str] = None,        # "Tornado Warning"
        severity: Optional[str] = None,     # "Extreme"
        limit: int = 500,                   # kept for API compat, NWS ignores it
    ) -> list[NWSAlert]:
        """Fetch all currently active NWS alerts, optionally filtered."""
        # NWS does NOT support a ?limit param — omit it entirely
        params: dict[str, Any] = {}
        if area:
            params["area"] = area
        if event:
            params["event"] = event
        if severity:
            params["severity"] = severity

        data = await self._get(f"{_BASE}/alerts/active", params, use_cache=False)
        return _parse_alerts(data)

    async def get_alerts_for_state(self, state: str) -> list[NWSAlert]:
        return await self.get_active_alerts(area=state.upper())

    async def get_alerts_by_zone(self, zone_id: str) -> list[NWSAlert]:
        data = await self._get(f"{_BASE}/alerts/active/zone/{zone_id}", use_cache=False)
        return _parse_alerts(data)

    # ------------------------------------------------------------------ #
    # Point forecasts                                                      #
    # ------------------------------------------------------------------ #

    async def get_point_metadata(self, lat: float, lon: float) -> dict[str, Any]:
        """Resolve a lat/lon to an NWS grid office + coordinates."""
        return await self._get(  # type: ignore[return-value]
            f"{_BASE}/points/{lat:.4f},{lon:.4f}",
            use_cache=True,
        )

    async def get_forecast(self, lat: float, lon: float) -> GridpointForecast:
        """7-day gridpoint forecast for a location."""
        meta = await self.get_point_metadata(lat, lon)
        props = meta.get("properties", {})
        forecast_url = props.get("forecast")
        if not forecast_url:
            raise ConnectorParseError(
                f"No forecast URL returned for ({lat}, {lon})", source=self.source_name
            )
        forecast_data = await self._get(forecast_url, use_cache=True)
        return _parse_forecast(lat, lon, props, forecast_data)

    async def get_hourly_forecast(self, lat: float, lon: float) -> GridpointForecast:
        """Hourly forecast for a location."""
        meta = await self.get_point_metadata(lat, lon)
        props = meta.get("properties", {})
        forecast_url = props.get("forecastHourly")
        if not forecast_url:
            raise ConnectorParseError(
                f"No hourly forecast URL for ({lat}, {lon})", source=self.source_name
            )
        forecast_data = await self._get(forecast_url, use_cache=True)
        return _parse_forecast(lat, lon, props, forecast_data)

    # ------------------------------------------------------------------ #
    # Health                                                               #
    # ------------------------------------------------------------------ #

    async def health_check(self) -> bool:
        try:
            data = await self._get(f"{_BASE}/", use_cache=False)
            return isinstance(data, dict)
        except Exception as exc:
            logger.warning(f"[NWS] health check failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_alerts(data: Any) -> list[NWSAlert]:
    alerts = []
    if not isinstance(data, dict):
        return alerts
    features = data.get("features", [])
    for feat in features:
        props = feat.get("properties", {})
        try:
            # Centroid from geometry
            geom = feat.get("geometry") or {}
            lat = lon = None
            if geom.get("type") == "Point":
                coords = geom.get("coordinates", [])
                if len(coords) >= 2:
                    lon, lat = coords[0], coords[1]

            severity_raw = props.get("severity", "Unknown")
            certainty_raw = props.get("certainty", "Unknown")

            alerts.append(
                NWSAlert(
                    alert_id=props.get("id", feat.get("id", "")),
                    headline=props.get("headline", props.get("event", "")),
                    description=props.get("description", "")[:2000],
                    event_type=props.get("event", ""),
                    severity=AlertSeverity(severity_raw)
                    if severity_raw in AlertSeverity._value2member_map_  # type: ignore
                    else AlertSeverity.UNKNOWN,
                    certainty=AlertCertainty(certainty_raw)
                    if certainty_raw in AlertCertainty._value2member_map_  # type: ignore
                    else AlertCertainty.UNKNOWN,
                    urgency=props.get("urgency", "Unknown"),
                    onset=_parse_dt(props.get("onset")),
                    expires=_parse_dt(props.get("expires")),
                    affected_zones=props.get("affectedZones", []),
                    centroid_lat=lat,
                    centroid_lon=lon,
                )
            )
        except Exception as exc:
            logger.debug(f"[NWS] skip alert: {exc}")
    return alerts


def _parse_forecast(
    lat: float, lon: float, meta_props: dict[str, Any], data: Any
) -> GridpointForecast:
    periods = []
    if isinstance(data, dict):
        for p in data.get("properties", {}).get("periods", []):
            try:
                periods.append(
                    ForecastPeriod(
                        name=p.get("name", ""),
                        start_time=_parse_dt(p.get("startTime")) or datetime.now(timezone.utc),
                        end_time=_parse_dt(p.get("endTime")) or datetime.now(timezone.utc),
                        is_daytime=bool(p.get("isDaytime", True)),
                        temperature_f=float(p.get("temperature", 0)),
                        wind_speed=str(p.get("windSpeed", "")),
                        wind_direction=str(p.get("windDirection", "")),
                        short_forecast=p.get("shortForecast", ""),
                        detailed_forecast=p.get("detailedForecast", "")[:500],
                        precipitation_pct=_safe_float(p.get("probabilityOfPrecipitation", {}).get("value")),
                    )
                )
            except Exception as exc:
                logger.debug(f"[NWS] skip period: {exc}")

    rel_loc = meta_props.get("relativeLocation", {}).get("properties", {})
    return GridpointForecast(
        lat=lat,
        lon=lon,
        office=meta_props.get("gridId", ""),
        grid_x=int(meta_props.get("gridX", 0)),
        grid_y=int(meta_props.get("gridY", 0)),
        timezone=meta_props.get("timeZone", "UTC"),
        periods=periods,
    )


def _parse_dt(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None
