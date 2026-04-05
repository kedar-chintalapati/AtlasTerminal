"""
Research router — event studies, backtests, factor model, causal drilldown.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from atlas_app.backend.deps import get_store
from atlas_core.exceptions import ResearchError
from atlas_core.store.duckdb_store import DuckDBStore

router = APIRouter(prefix="/research", tags=["research"])


class EventStudyRequest(BaseModel):
    signal_table: str = Field(..., description="Table with signal column")
    signal_column: str = Field(default="z_score")
    returns_table: str = Field(..., description="Table with daily returns")
    returns_column: str = Field(default="return")
    date_column: str = Field(default="date")
    windows: list[int] = Field(default=[1, 5, 10, 21])
    threshold: float = Field(default=1.0)
    direction: int = Field(default=0, ge=-1, le=1)


class BacktestRequest(BaseModel):
    signal_table: str
    signal_column: str = "z_score"
    returns_table: str
    returns_column: str = "return"
    date_column: str = "date"
    threshold: float = Field(default=0.5)
    cost_bps: float = Field(default=2.0, ge=0)
    max_position: float = Field(default=1.0)


class DrilldownRequest(BaseModel):
    commodity: str = Field(default="crude")


@router.post("/event-study")
async def event_study(
    req: EventStudyRequest,
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Run an event study: measure average returns around signal events."""
    try:
        import pandas as pd
        from atlas_core.research.event_study import run_event_study

        sig_df = store.query(
            f"SELECT {req.date_column}, {req.signal_column} FROM {req.signal_table} ORDER BY {req.date_column}"
        )
        ret_df = store.query(
            f"SELECT {req.date_column}, {req.returns_column} FROM {req.returns_table} ORDER BY {req.date_column}"
        )
        sig = sig_df.set_index(req.date_column)[req.signal_column]
        rets = ret_df.set_index(req.date_column)[req.returns_column]
        sig.index = pd.to_datetime(sig.index)
        rets.index = pd.to_datetime(rets.index)

        result = run_event_study(sig, rets, windows=req.windows, signal_threshold=req.threshold, direction=req.direction)
        return {
            "event_count": result.event_count,
            "windows": result.windows,
            "avg_returns": result.avg_returns,
            "hit_rates": result.hit_rates,
            "t_stats": result.t_stats,
            "excess_vs_baseline": result.excess_vs_baseline,
            "cumulative_path": result.cumulative_path.to_dict(orient="records"),
            "metadata": result.metadata,
        }
    except ResearchError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/backtest")
async def backtest(
    req: BacktestRequest,
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Run a simple signal-direction backtest."""
    try:
        import pandas as pd
        from atlas_core.research.backtest import run_backtest

        sig_df = store.query(
            f"SELECT {req.date_column}, {req.signal_column} FROM {req.signal_table} ORDER BY {req.date_column}"
        )
        ret_df = store.query(
            f"SELECT {req.date_column}, {req.returns_column} FROM {req.returns_table} ORDER BY {req.date_column}"
        )
        sig = sig_df.set_index(req.date_column)[req.signal_column].astype(float)
        rets = ret_df.set_index(req.date_column)[req.returns_column].astype(float)
        sig.index = pd.to_datetime(sig.index)
        rets.index = pd.to_datetime(rets.index)

        result = run_backtest(sig, rets, threshold=req.threshold, cost_bps=req.cost_bps)
        return {
            "total_return": result.total_return,
            "annualised_return": result.annualised_return,
            "sharpe": result.sharpe,
            "max_drawdown": result.max_drawdown,
            "hit_rate": result.hit_rate,
            "num_trades": result.num_trades,
            "pnl_series": result.pnl_series.reset_index().rename(columns={"index": "date", 0: "value"}).to_dict(orient="records"),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/drilldown")
async def causal_drilldown(
    req: DrilldownRequest,
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """
    Cross-domain causal drilldown for a commodity.

    Returns the composite bearish-risk signal decomposed into all components.
    """
    try:
        from atlas_core.signals.composite import CompositeRiskSignal
        sig = CompositeRiskSignal(commodity=req.commodity, store=store)
        result = sig.latest()
        return {
            "signal_name": result.signal_name,
            "value": result.value,
            "direction": result.direction,
            "confidence": result.confidence,
            "is_extreme": result.is_extreme,
            "components": [c.model_dump() for c in result.components],
            "metadata": result.metadata,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/factor-model/available-factors")
async def available_factors(store: DuckDBStore = Depends(get_store)) -> dict:
    """List factor series available for building a factor model."""
    try:
        tables = store.list_tables()
        return {"tables": tables, "suggested_factors": [
            "storage_surprises.z_score",
            "nws_alerts aggregated severity",
            "gdelt_events avg_tone",
        ]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
