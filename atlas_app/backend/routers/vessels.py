"""Vessels router — AIS and OpenSky endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from atlas_app.backend.deps import get_store
from atlas_core.connectors.opensky import OpenSkyConnector
from atlas_core.store.duckdb_store import DuckDBStore

router = APIRouter(prefix="/vessels", tags=["vessels"])


@router.get("/aircraft/states")
async def aircraft_states(
    min_lat: float = Query(25.0),
    min_lon: float = Query(-100.0),
    max_lat: float = Query(33.0),
    max_lon: float = Query(-88.0),
) -> dict:
    """Live aircraft positions over a bounding box (OpenSky)."""
    async with OpenSkyConnector() as sky:
        states = await sky.get_states_bbox(min_lat, min_lon, max_lat, max_lon)
    return {"count": len(states), "data": [s.model_dump() for s in states]}


@router.get("/aircraft/density")
async def aircraft_density(
    min_lat: float = Query(25.0),
    min_lon: float = Query(-100.0),
    max_lat: float = Query(33.0),
    max_lon: float = Query(-88.0),
    cell_size_deg: float = Query(0.5, ge=0.1, le=5.0),
) -> dict:
    """Grid-level flight density from live OpenSky data."""
    async with OpenSkyConnector() as sky:
        cells = await sky.get_flight_density(min_lat, min_lon, max_lat, max_lon, cell_size_deg)
    return {"count": len(cells), "data": [c.model_dump() for c in cells]}


@router.get("/positions")
async def vessel_positions(
    limit: int = Query(1000, ge=1, le=5000),
    vessel_type: Optional[str] = Query(None),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Recent vessel positions from the store."""
    sql = "SELECT * FROM vessel_positions"
    params: list = []
    if vessel_type:
        sql += " WHERE vessel_type = ?"
        params.append(vessel_type)
    sql += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)
    try:
        df = store.query(sql, params)
        return {"count": len(df), "data": df.to_dict(orient="records")}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/congestion")
async def congestion_signal(store: DuckDBStore = Depends(get_store)) -> dict:
    """Export terminal congestion signal from vessel positions."""
    try:
        from atlas_core.signals.congestion import CongestionSignal
        sig = CongestionSignal(store=store)
        result = sig.latest()
        return {
            "value": result.value,
            "direction": result.direction,
            "confidence": result.confidence,
            "metadata": result.metadata,
            "components": [c.model_dump() for c in result.components],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/terminals")
async def lng_terminals() -> dict:
    """Static metadata for tracked LNG export terminals."""
    from atlas_core.schemas.geo import LNG_TERMINALS
    return {"data": [t.model_dump() for t in LNG_TERMINALS]}
