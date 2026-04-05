"""Events router — FIRMS fires, GDELT news, atlas alerts."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from atlas_app.backend.deps import get_store
from atlas_core.connectors.gdelt import GDELTConnector
from atlas_core.connectors.nasa_firms import NASAFIRMSConnector
from atlas_core.config import settings
from atlas_core.exceptions import ConnectorNotConfiguredError
from atlas_core.store.duckdb_store import DuckDBStore

router = APIRouter(prefix="/events", tags=["events"])


# ─── Fires ────────────────────────────────────────────────────────────────────

@router.get("/fires/gulf-coast")
async def fires_gulf_coast(
    days: int = Query(3, ge=1, le=10),
) -> dict:
    """NASA FIRMS fire detections in the Gulf Coast energy corridor.
    Returns empty list (not an error) when NASA_FIRMS_MAP_KEY is not configured.
    Get a free key at: https://firms.modaps.eosdis.nasa.gov/api/area/
    """
    if not settings.nasa_firms_map_key or settings.nasa_firms_map_key == "DEMO_KEY":
        return {"count": 0, "days": days, "data": [], "info": "Add NASA_FIRMS_MAP_KEY to .env for fire data"}
    async with NASAFIRMSConnector() as firms:
        fires = await firms.get_fires_gulf_coast(days=days)
    return {"count": len(fires), "days": days, "data": [f.model_dump() for f in fires]}


@router.get("/fires/bbox")
async def fires_bbox(
    min_lat: float = Query(...),
    min_lon: float = Query(...),
    max_lat: float = Query(...),
    max_lon: float = Query(...),
    days: int = Query(2, ge=1, le=10),
) -> dict:
    """NASA FIRMS fire detections for a bounding box."""
    if not settings.nasa_firms_map_key or settings.nasa_firms_map_key == "DEMO_KEY":
        return {"count": 0, "data": [], "info": "Add NASA_FIRMS_MAP_KEY to .env for fire data"}
    async with NASAFIRMSConnector() as firms:
        fires = await firms.get_fires_for_bbox(min_lat, min_lon, max_lat, max_lon, days=days)
    return {"count": len(fires), "data": [f.model_dump() for f in fires]}


@router.get("/fires/score")
async def fire_exposure_score(
    days: int = Query(3, ge=1, le=10),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Fire exposure signal for tracked energy assets."""
    if not settings.nasa_firms_map_key or settings.nasa_firms_map_key == "DEMO_KEY":
        return {"value": 0.0, "direction": "neutral", "confidence": 0.0, "metadata": {"data_available": False}}
    try:
        async with NASAFIRMSConnector() as firms:
            fires = await firms.get_fires_gulf_coast(days=days)
        from atlas_core.signals.fire_exposure import FireExposureSignal
        sig = FireExposureSignal(store=store)
        result = sig.latest(detections=fires)
        return {
            "value": result.value,
            "direction": result.direction,
            "confidence": result.confidence,
            "metadata": result.metadata,
            "components": [c.model_dump() for c in result.components],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ─── News / GDELT ─────────────────────────────────────────────────────────────

@router.get("/news/search")
async def news_search(
    query: str = Query(..., min_length=2),
    timespan: str = Query("7d"),
    max_records: int = Query(50, ge=1, le=250),
) -> dict:
    """Search GDELT for energy-relevant articles."""
    async with GDELTConnector() as gdelt:
        articles = await gdelt.search_articles(query, timespan=timespan, max_records=max_records)
    return {"count": len(articles), "data": [a.model_dump() for a in articles]}


@router.get("/news/feed/{topic}")
async def news_feed(
    topic: str,
    timespan: str = Query("24h"),
) -> dict:
    """Pre-built energy topic news feed from GDELT."""
    from atlas_core.connectors.gdelt import ENERGY_QUERIES
    if topic not in ENERGY_QUERIES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown topic. Available: {list(ENERGY_QUERIES.keys())}",
        )
    async with GDELTConnector() as gdelt:
        articles = await gdelt.get_energy_feed(topic_key=topic, timespan=timespan)
    return {"topic": topic, "count": len(articles), "data": [a.model_dump() for a in articles]}


@router.get("/news/signal")
async def news_signal(
    topic: str = Query("natural_gas"),
    timespan: str = Query("30d"),
) -> dict:
    """News-flow signal for an energy topic."""
    async with GDELTConnector() as gdelt:
        articles = await gdelt.get_energy_feed(topic_key=topic, timespan=timespan)
    from atlas_core.signals.news_flow import NewsFlowSignal
    sig = NewsFlowSignal(topic=topic)
    result = sig.latest(articles=articles)
    return {
        "value": result.value,
        "direction": result.direction,
        "confidence": result.confidence,
        "metadata": result.metadata,
        "components": [c.model_dump() for c in result.components],
    }


# ─── Atlas Alerts ─────────────────────────────────────────────────────────────

@router.get("/alerts")
async def get_alerts(
    domain: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Retrieve atlas alerts from the store."""
    sql = "SELECT * FROM atlas_alerts"
    params: list = []
    conditions: list[str] = []
    if domain:
        conditions.append("domain = ?")
        params.append(domain)
    if severity:
        conditions.append("severity = ?")
        params.append(severity)
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    try:
        df = store.query(sql, params)
        return {"count": len(df), "data": df.to_dict(orient="records")}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
