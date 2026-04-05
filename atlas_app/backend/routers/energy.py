"""
Energy router — EIA data endpoints.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from atlas_app.backend.deps import get_store
from atlas_core.connectors.eia import EIAConnector
from atlas_core.config import settings
from atlas_core.exceptions import ConnectorNotConfiguredError
from atlas_core.store.duckdb_store import DuckDBStore

router = APIRouter(prefix="/energy", tags=["energy"])


@router.get("/storage/crude")
async def crude_storage(
    region: Optional[str] = Query(None, description="PADD region or 'Cushing', 'US'"),
    limit: int = Query(100, ge=1, le=1000),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Recent crude oil inventory data from EIA."""
    sql = "SELECT * FROM crude_storage"
    params: list = []
    if region:
        sql += " WHERE region = ?"
        params.append(region.upper())
    sql += " ORDER BY report_date DESC LIMIT ?"
    params.append(limit)
    try:
        df = store.query(sql, params)
        return {"data": df.to_dict(orient="records"), "count": len(df)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/storage/gas")
async def gas_storage(
    region: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Recent natural gas storage data from EIA."""
    sql = "SELECT * FROM gas_storage"
    params: list = []
    if region:
        sql += " WHERE region = ?"
        params.append(region)
    sql += " ORDER BY report_date DESC LIMIT ?"
    params.append(limit)
    try:
        df = store.query(sql, params)
        return {"data": df.to_dict(orient="records"), "count": len(df)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/storage/surprise")
async def storage_surprise(
    commodity: str = Query("crude", regex="^(crude|natgas)$"),
    region: str = Query("US"),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Latest storage surprise signal."""
    try:
        from atlas_core.signals.storage_surprise import StorageSurpriseSignal
        sig = StorageSurpriseSignal(commodity=commodity, region=region, store=store)
        result = sig.latest()
        return {
            "signal_name": result.signal_name,
            "value": result.value,
            "direction": result.direction,
            "confidence": result.confidence,
            "metadata": result.metadata,
            "components": [c.model_dump() for c in result.components],
        }
    except Exception as exc:
        logger.warning(f"Storage surprise failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/storage/surprises/history")
async def storage_surprise_history(
    commodity: str = Query("crude", regex="^(crude|natgas)$"),
    region: str = Query("US"),
    limit: int = Query(52, ge=1, le=500),
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Historical storage surprise series."""
    try:
        df = store.query(
            "SELECT * FROM storage_surprises WHERE commodity=? AND region=? ORDER BY report_date DESC LIMIT ?",
            [commodity, region, limit],
        )
        return {"data": df.to_dict(orient="records"), "count": len(df)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/refresh")
async def refresh_energy(store: DuckDBStore = Depends(get_store)) -> dict:
    """Manually trigger an EIA data refresh."""
    if not settings.eia_api_key:
        raise HTTPException(status_code=503, detail="EIA API key not configured")
    try:
        async with EIAConnector() as connector:
            crude = await connector.get_crude_storage()
            gas = await connector.get_gas_storage()

        import pandas as pd
        if crude:
            df = pd.DataFrame([r.model_dump() for r in crude])
            store.upsert_dataframe("crude_storage", df)
        if gas:
            df = pd.DataFrame([r.model_dump() for r in gas])
            store.upsert_dataframe("gas_storage", df)

        return {
            "status": "ok",
            "crude_rows": len(crude),
            "gas_rows": len(gas),
        }
    except ConnectorNotConfiguredError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
