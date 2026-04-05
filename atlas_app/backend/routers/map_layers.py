"""
Map-layers router — serves GeoJSON for all deck.gl map layers.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from atlas_app.backend.deps import get_store
from atlas_core.schemas.geo import LNG_TERMINALS, GAS_HUBS
from atlas_core.store.duckdb_store import DuckDBStore

router = APIRouter(prefix="/map", tags=["map"])


def _feature(geometry: dict, properties: dict) -> dict:
    return {"type": "Feature", "geometry": geometry, "properties": properties}


def _point(lon: float, lat: float) -> dict:
    return {"type": "Point", "coordinates": [lon, lat]}


@router.get("/layers/assets")
async def assets_layer() -> dict:
    """GeoJSON FeatureCollection of all known energy assets."""
    features = []
    for t in LNG_TERMINALS:
        features.append(
            _feature(
                _point(t.lon, t.lat),
                {
                    "asset_id": t.asset_id,
                    "name": t.name,
                    "type": t.asset_type.value,
                    "capacity": t.capacity,
                    "capacity_unit": t.capacity_unit,
                    "operator": t.operator,
                    "region": t.region,
                },
            )
        )
    # Gas hubs
    for name, (lat, lon) in GAS_HUBS.items():
        features.append(
            _feature(
                _point(lon, lat),
                {"asset_id": name, "name": name.replace("_", " ").title(), "type": "gas_hub"},
            )
        )
    return {"type": "FeatureCollection", "features": features}


@router.get("/layers/fires")
async def fires_layer(
    days: int = Query(3, ge=1, le=10),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """GeoJSON FeatureCollection of FIRMS fire detections."""
    try:
        df = store.query(
            """
            SELECT lat, lon, brightness_k, frp_mw, confidence, satellite, acq_datetime
            FROM firms_detections
            WHERE acq_datetime >= NOW() - INTERVAL ? DAY
            ORDER BY acq_datetime DESC
            LIMIT 5000
            """,
            [days],
        )
        features = [
            _feature(
                _point(float(row["lon"]), float(row["lat"])),
                {
                    "brightness_k": row.get("brightness_k"),
                    "frp_mw": row.get("frp_mw"),
                    "confidence": row.get("confidence"),
                    "satellite": row.get("satellite"),
                    "acq_datetime": str(row.get("acq_datetime", "")),
                },
            )
            for _, row in df.iterrows()
        ]
        return {"type": "FeatureCollection", "features": features}
    except Exception:
        return {"type": "FeatureCollection", "features": []}


@router.get("/layers/vessels")
async def vessels_layer(
    limit: int = Query(2000, ge=1, le=10000),
    vessel_type: Optional[str] = Query(None),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """GeoJSON FeatureCollection of recent vessel positions."""
    try:
        sql = """
            SELECT DISTINCT ON (mmsi) mmsi, vessel_name, vessel_type, lat, lon,
                   speed_kts, nav_status, destination, timestamp
            FROM vessel_positions
        """
        params: list[Any] = []
        if vessel_type:
            sql += " WHERE vessel_type = ?"
            params.append(vessel_type)
        sql += f" ORDER BY mmsi, timestamp DESC LIMIT {limit}"
        df = store.query(sql, params)
        features = [
            _feature(
                _point(float(row["lon"]), float(row["lat"])),
                {
                    "mmsi": row.get("mmsi"),
                    "name": row.get("vessel_name"),
                    "type": row.get("vessel_type"),
                    "speed": row.get("speed_kts"),
                    "status": row.get("nav_status"),
                    "destination": row.get("destination"),
                },
            )
            for _, row in df.iterrows()
        ]
        return {"type": "FeatureCollection", "features": features}
    except Exception:
        return {"type": "FeatureCollection", "features": []}


@router.get("/layers/alerts")
async def alerts_layer(
    severity: Optional[str] = Query(None),
    domain: Optional[str] = Query(None),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """GeoJSON for atlas alerts with coordinates."""
    try:
        sql = "SELECT * FROM atlas_alerts WHERE lat IS NOT NULL AND lon IS NOT NULL"
        params: list[Any] = []
        if severity:
            sql += " AND severity = ?"
            params.append(severity)
        if domain:
            sql += " AND domain = ?"
            params.append(domain)
        sql += " ORDER BY created_at DESC LIMIT 200"
        df = store.query(sql, params)
        features = [
            _feature(
                _point(float(row["lon"]), float(row["lat"])),
                {
                    "alert_id": row.get("alert_id"),
                    "title": row.get("title"),
                    "severity": row.get("severity"),
                    "domain": row.get("domain"),
                    "score": row.get("score"),
                    "created_at": str(row.get("created_at", "")),
                },
            )
            for _, row in df.iterrows()
        ]
        return {"type": "FeatureCollection", "features": features}
    except Exception:
        return {"type": "FeatureCollection", "features": []}
