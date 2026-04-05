"""Weather router — NWS, NOAA, NDBC, SWPC endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from atlas_app.backend.deps import get_store
from atlas_core.connectors.noaa_nws import NWSConnector
from atlas_core.connectors.noaa_swpc import NOAASWPCConnector
from atlas_core.connectors.noaa_ndbc import NDBCConnector
from atlas_core.store.duckdb_store import DuckDBStore

router = APIRouter(prefix="/weather", tags=["weather"])


@router.get("/alerts/active")
async def active_alerts(
    state: Optional[str] = Query(None, description="US state abbreviation, e.g. 'TX'"),
    event: Optional[str] = Query(None, description="Event type filter"),
    severity: Optional[str] = Query(None),
) -> dict:
    """Fetch currently active NWS weather alerts."""
    async with NWSConnector() as nws:
        alerts = await nws.get_active_alerts(area=state, event=event, severity=severity)
    return {
        "count": len(alerts),
        "data": [a.model_dump() for a in alerts],
    }


@router.get("/forecast")
async def point_forecast(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
) -> dict:
    """7-day NWS gridpoint forecast for a lat/lon."""
    async with NWSConnector() as nws:
        try:
            forecast = await nws.get_forecast(lat, lon)
            return forecast.model_dump()
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/buoy/{station_id}")
async def buoy_observations(
    station_id: str,
    max_rows: int = Query(48, ge=1, le=200),
) -> dict:
    """Latest NDBC buoy observations for a station."""
    async with NDBCConnector() as ndbc:
        obs = await ndbc.get_latest_observations(station_id, max_rows=max_rows)
    return {"station_id": station_id, "count": len(obs), "data": [o.model_dump() for o in obs]}


@router.get("/buoy/gulf/all")
async def gulf_buoys() -> dict:
    """Latest observations for all tracked Gulf of Mexico buoy stations."""
    async with NDBCConnector() as ndbc:
        results = await ndbc.get_all_gulf_stations()
    return {
        sid: [o.model_dump() for o in obs]
        for sid, obs in results.items()
    }


@router.get("/space/kindex")
async def k_index() -> dict:
    """Latest NOAA SWPC geomagnetic K-index readings."""
    async with NOAASWPCConnector() as swpc:
        k = await swpc.get_k_index()
    return {"count": len(k), "data": [r.model_dump() for r in k]}


@router.get("/space/alerts")
async def space_weather_alerts() -> dict:
    """Active NOAA SWPC space-weather alerts, watches, and warnings."""
    async with NOAASWPCConnector() as swpc:
        alerts = await swpc.get_alerts()
    return {"count": len(alerts), "data": [a.model_dump() for a in alerts]}


@router.get("/risk/score")
async def weather_risk_score(
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Composite weather-risk signal for tracked Gulf Coast assets."""
    try:
        async with NWSConnector() as nws:
            alerts = await nws.get_active_alerts(area="TX") + await nws.get_active_alerts(area="LA")
        from atlas_core.signals.weather_risk import WeatherRiskSignal
        sig = WeatherRiskSignal(store=store)
        result = sig.latest(alerts=alerts)
        return {
            "value": result.value,
            "direction": result.direction,
            "confidence": result.confidence,
            "metadata": result.metadata,
            "components": [c.model_dump() for c in result.components],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
