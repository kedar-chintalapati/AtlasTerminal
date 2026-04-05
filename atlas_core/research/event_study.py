"""
Event-study analysis.

Measures the average price/return response around signal events.
Used to validate and tune signal definitions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from atlas_core.exceptions import ResearchError
from atlas_core.utils.math import information_ratio, max_drawdown


@dataclass
class EventStudyResult:
    """Output of an event study."""
    signal_name: str
    event_count: int
    windows: list[int]                          # horizon days e.g. [1, 5, 10]
    avg_returns: dict[int, float]               # window → avg cumulative return
    hit_rates: dict[int, float]                 # window → fraction correct direction
    t_stats: dict[int, float]                   # window → t-statistic
    excess_vs_baseline: dict[int, float]        # vs. unconditional mean
    cumulative_path: pd.DataFrame = field(default_factory=pd.DataFrame)
    metadata: dict = field(default_factory=dict)


def run_event_study(
    signal_series: pd.Series,          # DatetimeIndex, values: signal (non-zero = event)
    returns_series: pd.Series,         # DatetimeIndex, values: % returns (same symbol)
    windows: list[int] = (1, 5, 10, 21),
    signal_threshold: float = 1.0,     # |signal| above this = event
    direction: int = 0,                # +1 bullish events, -1 bearish, 0 both
    min_gap_days: int = 3,             # minimum days between events (avoid overlap)
) -> EventStudyResult:
    """
    Classic event-study: measure average forward returns around signal events.

    Parameters
    ----------
    signal_series : pd.Series indexed by date with signal values
    returns_series: pd.Series of daily returns for the same instrument
    windows       : forward-return horizons (calendar days)
    signal_threshold: abs(signal) must exceed this to count as an event
    direction     : filter to bullish (+1), bearish (−1), or both (0) events
    min_gap_days  : exclude events within this many days of a prior event
    """
    signal = signal_series.dropna().sort_index()
    returns = returns_series.dropna().sort_index()

    # Identify event dates
    event_mask = signal.abs() >= signal_threshold
    if direction == 1:
        event_mask &= signal > 0
    elif direction == -1:
        event_mask &= signal < 0

    event_dates = signal[event_mask].index.tolist()
    if not event_dates:
        raise ResearchError("No events found matching criteria")

    # Apply min-gap filter
    filtered_dates: list = []
    last_event = None
    for d in event_dates:
        if last_event is None or (d - last_event).days >= min_gap_days:
            filtered_dates.append(d)
            last_event = d

    # Compute cumulative returns for each window
    all_paths: list[pd.Series] = []
    avg_returns: dict[int, float] = {}
    hit_rates: dict[int, float] = {}
    t_stats: dict[int, float] = {}
    excess: dict[int, float] = {}

    baseline_mean = float(returns.mean())

    for window in windows:
        window_rets: list[float] = []
        for event_date in filtered_dates:
            # Find daily returns from event date + 1 to event date + window
            fwd = returns[
                (returns.index > event_date) &
                (returns.index <= event_date + pd.Timedelta(days=window))
            ]
            if len(fwd) == 0:
                continue
            # Cumulative return
            cum = float((1 + fwd).prod() - 1)
            window_rets.append(cum)

        if len(window_rets) >= 2:
            arr = np.array(window_rets)
            mean = float(arr.mean())
            se = float(arr.std(ddof=1) / np.sqrt(len(arr)))
            avg_returns[window] = mean
            hit_rates[window] = float((arr > 0).mean())
            t_stats[window] = float(mean / se) if se > 0 else 0.0
            excess[window] = mean - baseline_mean * window
        else:
            avg_returns[window] = 0.0
            hit_rates[window] = 0.5
            t_stats[window] = 0.0
            excess[window] = 0.0

    # Build cumulative path: average daily return for ±20 days around event
    path_cols: dict[int, list[float]] = {d: [] for d in range(-10, 22)}
    for event_date in filtered_dates:
        for offset in range(-10, 22):
            target = event_date + pd.Timedelta(days=offset)
            slice_ = returns[
                (returns.index > target - pd.Timedelta(days=1)) &
                (returns.index <= target)
            ]
            path_cols[offset].append(float(slice_.sum()) if len(slice_) else 0.0)

    path_data = {
        "offset": list(path_cols.keys()),
        "avg_return": [
            np.mean(v) if v else 0.0 for v in path_cols.values()
        ],
    }
    cum_path = pd.DataFrame(path_data)
    cum_path["cumulative"] = (1 + cum_path["avg_return"]).cumprod() - 1

    return EventStudyResult(
        signal_name=getattr(signal_series, "name", "signal"),
        event_count=len(filtered_dates),
        windows=list(windows),
        avg_returns=avg_returns,
        hit_rates=hit_rates,
        t_stats=t_stats,
        excess_vs_baseline=excess,
        cumulative_path=cum_path,
        metadata={"baseline_mean": baseline_mean, "event_dates": [str(d)[:10] for d in filtered_dates]},
    )
