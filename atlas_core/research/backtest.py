"""
Simple vectorised backtest engine.

Converts a signal series into daily long/short/flat positions, then computes
PnL, Sharpe, max drawdown, and hit rate.  No transaction-cost model by default
but you can pass a cost_bps parameter.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from atlas_core.exceptions import ResearchError
from atlas_core.utils.math import hit_rate, information_ratio, max_drawdown


@dataclass
class BacktestResult:
    """Summary statistics from a simple signal backtest."""
    signal_name: str
    start_date: str
    end_date: str
    total_return: float
    annualised_return: float
    sharpe: float
    max_drawdown: float
    hit_rate: float
    num_trades: int
    avg_trade_days: float
    pnl_series: pd.Series = field(default_factory=pd.Series)
    positions: pd.Series = field(default_factory=pd.Series)
    metadata: dict = field(default_factory=dict)


def run_backtest(
    signal_series: pd.Series,          # daily signal values
    returns_series: pd.Series,         # daily returns of the instrument
    threshold: float = 0.2,            # signal must exceed ±threshold for a position
    cost_bps: float = 0.0,             # round-trip cost per trade in basis points
    max_position: float = 1.0,         # max position size (1 = fully invested)
    holding_days: Optional[int] = None,  # force exit after N days
    signal_name: str = "signal",
) -> BacktestResult:
    """
    Vectorised long/short backtest.

    Position: +1 when signal > threshold, -1 when signal < -threshold, 0 otherwise.
    If ``holding_days`` is set, position is reset after that many days.
    """
    sig = signal_series.dropna().sort_index()
    rets = returns_series.dropna().sort_index()
    common = sig.index.intersection(rets.index)
    if len(common) < 10:
        raise ResearchError(
            f"Insufficient overlapping data: {len(common)} days",
        )
    sig = sig[common]
    rets = rets[common]

    # Raw positions from signal
    pos = pd.Series(0.0, index=sig.index)
    pos[sig > threshold] = max_position
    pos[sig < -threshold] = -max_position

    if holding_days is not None:
        # Zero out position if signal has been in the same direction > holding_days
        current_pos = 0.0
        days_held = 0
        new_pos = []
        for i, (p, _) in enumerate(zip(pos, sig)):
            if p != current_pos:
                current_pos = p
                days_held = 0
            elif days_held >= holding_days:
                current_pos = 0.0
                days_held = 0
            new_pos.append(current_pos)
            days_held += 1
        pos = pd.Series(new_pos, index=sig.index)

    # PnL = lagged position * next-day return (positions set at close, returns on next day)
    strategy_rets = pos.shift(1).fillna(0) * rets

    # Apply trading costs at position changes
    if cost_bps > 0:
        turnover = pos.diff().abs().fillna(0)
        cost = turnover * (cost_bps / 10_000)
        strategy_rets -= cost

    cum = (1 + strategy_rets).cumprod()
    total_return = float(cum.iloc[-1] - 1)
    n_years = len(common) / 252
    ann_return = float((1 + total_return) ** (1 / max(n_years, 0.01)) - 1)

    trade_starts = (pos.diff() != 0) & (pos != 0)
    trade_ends = (pos.diff() != 0) & (pos.shift(1) != 0)
    num_trades = int(trade_starts.sum())
    avg_holding = float(pos.abs().sum() / max(num_trades, 1))

    return BacktestResult(
        signal_name=signal_name,
        start_date=str(common[0])[:10],
        end_date=str(common[-1])[:10],
        total_return=total_return,
        annualised_return=ann_return,
        sharpe=information_ratio(strategy_rets),
        max_drawdown=max_drawdown(strategy_rets),
        hit_rate=hit_rate(pos.shift(1).fillna(0), rets),
        num_trades=num_trades,
        avg_trade_days=avg_holding,
        pnl_series=cum,
        positions=pos,
        metadata={"cost_bps": cost_bps, "threshold": threshold},
    )
