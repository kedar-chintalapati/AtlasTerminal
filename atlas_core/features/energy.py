"""
Energy feature engineering.

Pure functions that transform raw EIA DataFrames (as returned by the store)
into derived analytical features.  No I/O, no side effects — all functions
take a DataFrame and return a DataFrame.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from atlas_core.schemas.energy import StorageSurprise
from atlas_core.utils.math import rolling_zscore, seasonal_zscore, pct_deviation
from atlas_core.utils.time import week_of_year


def compute_storage_surprise(
    df: pd.DataFrame,
    commodity: str = "crude",
    window: int = 52,
) -> pd.DataFrame:
    """
    Compute storage surprise signal from weekly storage data.

    Input columns: report_date, region, stocks_{mmbbl|bcf}, change_{mmbbl|bcf}
    Returns DataFrame with columns matching ``StorageSurprise`` schema.
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy().sort_values("report_date")
    val_col = "stocks_mmbbl" if commodity == "crude" else "stocks_bcf"
    chg_col = "change_mmbbl" if commodity == "crude" else "change_bcf"

    if val_col not in df.columns:
        raise ValueError(f"Expected column {val_col!r} not in DataFrame")

    # If change column missing, compute it
    if chg_col not in df.columns:
        df[chg_col] = df.groupby("region")[val_col].diff()

    results = []
    for region, grp in df.groupby("region"):
        grp = grp.sort_values("report_date").reset_index(drop=True)
        chg = grp[chg_col].astype(float)

        # Rolling z-score (last ``window`` weeks)
        z = rolling_zscore(chg, window=window, min_periods=4)

        # Seasonal average (same week-of-year over all available history)
        doy = pd.to_datetime(grp["report_date"]).dt.isocalendar().week
        for i, row in grp.iterrows():
            week_mask = doy == doy.iloc[i]
            hist = chg[week_mask]
            consensus = hist.mean() if len(hist) > 1 else 0.0
            surprise = chg.iloc[i] - consensus

            five_yr = None
            avg_col = f"five_year_avg_{val_col.split('_')[1]}"
            if avg_col in grp.columns and not pd.isna(grp[avg_col].iloc[i]) and grp[avg_col].iloc[i]:
                five_yr = (grp[val_col].iloc[i] / grp[avg_col].iloc[i] - 1) * 100

            z_val = float(z.iloc[i]) if not np.isnan(z.iloc[i]) else 0.0
            direction = "bearish" if surprise < 0 else ("bullish" if surprise > 0 else "neutral")
            confidence = min(1.0, abs(z_val) / 3.0)

            results.append(
                {
                    "report_date": row["report_date"],
                    "commodity": commodity,
                    "region": region,
                    "actual_change": float(chg.iloc[i]),
                    "consensus_change": float(consensus),
                    "surprise": float(surprise),
                    "z_score": z_val,
                    "five_year_pct_dev": five_yr,
                    "signal_direction": direction,
                    "confidence": float(confidence),
                }
            )

    return pd.DataFrame(results)


def compute_seasonal_deviation(
    df: pd.DataFrame,
    value_col: str,
    period: int = 52,
) -> pd.DataFrame:
    """Add ``seasonal_z`` and ``seasonal_pct_dev`` columns."""
    df = df.copy().sort_values("report_date")
    df["seasonal_z"] = seasonal_zscore(df[value_col].astype(float), period=period)
    df["seasonal_pct_dev"] = pct_deviation(df[value_col].astype(float), window=period)
    return df


def compute_refinery_margin_proxy(
    crude_df: pd.DataFrame,
    products_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Simple 3-2-1 crack spread proxy from EIA data.

    Without product prices, we estimate a margin from utilization changes:
    high utilization + building product stocks → narrowing margin.
    """
    if crude_df.empty:
        return pd.DataFrame()

    df = crude_df.copy().sort_values("report_date")
    df["util_zscore"] = rolling_zscore(df["utilization_pct"].astype(float))
    df["margin_proxy"] = -df["util_zscore"]  # inverse: high util → margin pressure
    return df


def compute_supply_demand_balance(
    production_df: pd.DataFrame,
    storage_df: pd.DataFrame,
    imports_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Merge production + storage to derive implied demand / balance.

    implied_demand = production + imports - storage_change
    """
    if production_df.empty or storage_df.empty:
        return pd.DataFrame()

    prod = production_df.groupby("report_date")["production"].sum().reset_index()
    stor = storage_df.groupby("report_date")["change_mmbbl"].sum().reset_index()

    merged = pd.merge(prod, stor, on="report_date", how="inner")
    merged["implied_demand"] = merged["production"] - merged["change_mmbbl"]
    merged = merged.sort_values("report_date")
    merged["demand_z"] = rolling_zscore(merged["implied_demand"])
    return merged


# Allow Optional import without adding typing to imports
from typing import Optional  # noqa: E402
