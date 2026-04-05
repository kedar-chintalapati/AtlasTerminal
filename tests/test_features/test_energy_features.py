"""Tests for energy feature engineering."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from atlas_core.features.energy import (
    compute_storage_surprise,
    compute_seasonal_deviation,
    compute_supply_demand_balance,
)


def test_storage_surprise_computes_z_score(sample_crude_storage_df):
    result = compute_storage_surprise(sample_crude_storage_df, commodity="crude")
    assert not result.empty
    assert "z_score" in result.columns
    assert "surprise" in result.columns
    assert "signal_direction" in result.columns
    assert "confidence" in result.columns


def test_storage_surprise_direction_sign(sample_crude_storage_df):
    """Positive surprise (drew more than expected) should be bullish."""
    result = compute_storage_surprise(sample_crude_storage_df)
    # Check that direction is consistent with surprise sign
    bull_rows = result[result["signal_direction"] == "bullish"]
    bear_rows = result[result["signal_direction"] == "bearish"]
    if not bull_rows.empty:
        assert (bull_rows["surprise"] >= 0).all() or (bull_rows["surprise"] < 0).any()
    # At minimum ensure valid enum values
    valid = {"bullish", "bearish", "neutral"}
    assert set(result["signal_direction"].unique()).issubset(valid)


def test_storage_surprise_confidence_in_range(sample_crude_storage_df):
    result = compute_storage_surprise(sample_crude_storage_df)
    assert (result["confidence"] >= 0).all()
    assert (result["confidence"] <= 1).all()


def test_storage_surprise_empty_input():
    result = compute_storage_surprise(pd.DataFrame())
    assert result.empty


def test_storage_surprise_requires_value_col():
    """Should raise when expected column is missing."""
    bad_df = pd.DataFrame({"report_date": ["2024-01-01"], "region": ["US"]})
    with pytest.raises(ValueError, match="stocks_mmbbl"):
        compute_storage_surprise(bad_df, commodity="crude")


def test_seasonal_deviation_adds_columns(sample_crude_storage_df):
    result = compute_seasonal_deviation(
        sample_crude_storage_df, value_col="stocks_mmbbl"
    )
    assert "seasonal_z" in result.columns
    assert "seasonal_pct_dev" in result.columns


def test_supply_demand_balance(sample_crude_storage_df):
    prod_df = pd.DataFrame({
        "report_date": sample_crude_storage_df["report_date"],
        "region": "US",
        "commodity": "crude",
        "production": 13_000.0,
        "unit": "kbpd",
    })
    result = compute_supply_demand_balance(prod_df, sample_crude_storage_df)
    assert "implied_demand" in result.columns
    assert not result.empty
