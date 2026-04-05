"""
Multi-factor model for cross-domain signal decomposition.

Regresses an instrument's returns (or spread changes) against a set of
physical-intelligence factors to attribute performance and measure betas.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats


@dataclass
class FactorModelResult:
    """OLS factor-model output."""
    r_squared: float
    adj_r_squared: float
    betas: dict[str, float]
    t_stats: dict[str, float]
    p_values: dict[str, float]
    residuals: pd.Series = field(default_factory=pd.Series)
    fitted: pd.Series = field(default_factory=pd.Series)
    factor_contributions: pd.DataFrame = field(default_factory=pd.DataFrame)
    metadata: dict = field(default_factory=dict)


def run_factor_model(
    returns: pd.Series,
    factors: pd.DataFrame,
    add_constant: bool = True,
    min_obs: int = 20,
) -> FactorModelResult:
    """
    OLS factor model: returns = α + β₁F₁ + β₂F₂ + … + ε

    Parameters
    ----------
    returns : dependent variable (daily/weekly returns)
    factors : DataFrame of factor time series (columns = factor names)
    add_constant : include intercept
    min_obs : minimum overlapping observations required
    """
    # Align
    common = returns.dropna().index.intersection(factors.dropna(how="all").index)
    if len(common) < min_obs:
        raise ValueError(
            f"Insufficient overlapping data: {len(common)} < {min_obs} required"
        )

    y = returns[common].values
    X = factors.loc[common].values
    factor_names = list(factors.columns)

    if add_constant:
        X = np.column_stack([np.ones(len(X)), X])
        factor_names = ["alpha"] + factor_names

    # OLS via numpy
    coeffs, residuals, rank, sv = np.linalg.lstsq(X, y, rcond=None)

    fitted = X @ coeffs
    resid = y - fitted
    n, k = len(y), len(coeffs)
    sse = float(np.dot(resid, resid))
    sst = float(np.dot(y - y.mean(), y - y.mean()))
    r2 = 1 - sse / sst if sst > 0 else 0.0
    adj_r2 = 1 - (1 - r2) * (n - 1) / max(n - k, 1)

    # Standard errors
    var_e = sse / max(n - k, 1)
    try:
        XtX_inv = np.linalg.inv(X.T @ X)
        se = np.sqrt(np.diag(XtX_inv * var_e))
    except np.linalg.LinAlgError:
        se = np.ones(k) * np.nan

    t_stats_arr = coeffs / np.where(se > 0, se, np.nan)
    p_vals_arr = 2 * stats.t.sf(np.abs(t_stats_arr), df=n - k)

    betas = dict(zip(factor_names, coeffs.tolist()))
    t_stats_dict = dict(zip(factor_names, t_stats_arr.tolist()))
    p_values_dict = dict(zip(factor_names, p_vals_arr.tolist()))

    # Factor contributions (beta × factor value at each point)
    contrib_data = {}
    for i, fname in enumerate(factor_names):
        contrib_data[fname] = X[:, i] * coeffs[i]
    factor_contributions = pd.DataFrame(contrib_data, index=common)

    return FactorModelResult(
        r_squared=r2,
        adj_r_squared=adj_r2,
        betas=betas,
        t_stats=t_stats_dict,
        p_values=p_values_dict,
        residuals=pd.Series(resid, index=common, name="residuals"),
        fitted=pd.Series(fitted, index=common, name="fitted"),
        factor_contributions=factor_contributions,
        metadata={"n_obs": n, "n_factors": k - int(add_constant), "factor_names": factor_names},
    )


def basis_spread_analysis(
    series_a: pd.Series,
    series_b: pd.Series,
    window: int = 52,
) -> pd.DataFrame:
    """
    Compute spread, rolling z-score, and regime flags between two price series.

    Useful for: Henry Hub vs. NYMEX futures, physical vs. paper crude, etc.
    """
    common = series_a.dropna().index.intersection(series_b.dropna().index)
    a = series_a[common]
    b = series_b[common]
    spread = a - b
    rolling_mean = spread.rolling(window, min_periods=4).mean()
    rolling_std = spread.rolling(window, min_periods=4).std()
    z = (spread - rolling_mean) / rolling_std.replace(0, np.nan)
    percentile = spread.rolling(window, min_periods=4).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    )
    result = pd.DataFrame(
        {
            "series_a": a,
            "series_b": b,
            "spread": spread,
            "rolling_mean": rolling_mean,
            "z_score": z,
            "percentile_rank": percentile,
        }
    )
    result["regime"] = pd.cut(
        z,
        bins=[-np.inf, -2, -1, 1, 2, np.inf],
        labels=["very_tight", "tight", "normal", "wide", "very_wide"],
    )
    return result
