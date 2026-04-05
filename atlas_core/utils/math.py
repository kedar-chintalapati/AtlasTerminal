"""Numerical utility functions for signal and feature engineering."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def rolling_zscore(
    series: pd.Series,
    window: int = 52,
    min_periods: int = 8,
) -> pd.Series:
    """Rolling z-score normalisation."""
    mu = series.rolling(window, min_periods=min_periods).mean()
    sigma = series.rolling(window, min_periods=min_periods).std()
    return (series - mu) / sigma.replace(0, np.nan)


def seasonal_zscore(
    series: pd.Series,
    period: int = 52,       # 52 weeks for weekly data
    min_obs: int = 3,
) -> pd.Series:
    """
    Z-score relative to same week/month across historical years.

    Groups by (period mod ``period``) and computes mean/std across groups.
    """
    idx = np.arange(len(series)) % period
    z = series.copy() * np.nan
    for i in range(period):
        mask = idx == i
        vals = series[mask].dropna()
        if len(vals) >= min_obs:
            mu, sigma = vals.mean(), vals.std()
            if sigma > 0:
                z[mask] = (series[mask] - mu) / sigma
    return z


def pct_deviation(series: pd.Series, window: int = 52) -> pd.Series:
    """Percentage deviation from rolling mean."""
    mu = series.rolling(window, min_periods=4).mean()
    return ((series - mu) / mu.abs().replace(0, np.nan)) * 100


def ewma_zscore(
    series: pd.Series,
    span: int = 26,
    long_span: int = 52,
) -> pd.Series:
    """Z-score using EWMA mean and EWMA std."""
    mu = series.ewm(span=span).mean()
    sigma = series.ewm(span=long_span).std()
    return (series - mu) / sigma.replace(0, np.nan)


def percentile_rank(series: pd.Series, window: Optional[int] = None) -> pd.Series:
    """
    Percentile rank [0, 1] of each value within its rolling window.
    If ``window`` is None, uses the full history.
    """
    if window is None:
        return series.expanding().rank(pct=True)
    return series.rolling(window, min_periods=4).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1],
        raw=False,
    )


def information_ratio(returns: pd.Series, benchmark: Optional[pd.Series] = None) -> float:
    """Annualised information ratio (or Sharpe if no benchmark)."""
    excess = returns if benchmark is None else returns - benchmark
    if excess.std() == 0:
        return 0.0
    return float(excess.mean() / excess.std() * np.sqrt(252))


def max_drawdown(returns: pd.Series) -> float:
    """Maximum drawdown from cumulative returns series."""
    cum = (1 + returns).cumprod()
    peak = cum.expanding().max()
    dd = (cum - peak) / peak
    return float(dd.min())


def hit_rate(signal: pd.Series, returns: pd.Series) -> float:
    """Fraction of times signal direction matches return direction."""
    aligned = pd.DataFrame({"sig": signal, "ret": returns}).dropna()
    if aligned.empty:
        return float("nan")
    correct = (np.sign(aligned["sig"]) == np.sign(aligned["ret"])).sum()
    return float(correct / len(aligned))


def winsorise(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """Winsorise at specified quantile levels."""
    lo = series.quantile(lower)
    hi = series.quantile(upper)
    return series.clip(lo, hi)
