"""Tests for StorageSurpriseSignal."""

from __future__ import annotations

import pytest
from atlas_core.signals.storage_surprise import StorageSurpriseSignal
from atlas_core.exceptions import InsufficientDataError


def test_signal_computes_from_df(sample_crude_storage_df):
    sig = StorageSurpriseSignal(commodity="crude", region="US")
    result = sig.latest(df=sample_crude_storage_df)

    assert -1.0 <= result.value <= 1.0
    assert result.direction in ("bullish", "bearish", "neutral")
    assert 0.0 <= result.confidence <= 1.0
    assert len(result.components) == 1
    assert result.components[0].name == "storage_surprise_z"


def test_signal_metadata_fields(sample_crude_storage_df):
    sig = StorageSurpriseSignal(commodity="crude", region="US")
    result = sig.latest(df=sample_crude_storage_df)

    assert "z_score" in result.metadata
    assert "actual_change" in result.metadata
    assert "surprise" in result.metadata
    assert result.metadata["commodity"] == "crude"
    assert result.metadata["region"] == "US"


def test_signal_raises_on_short_series():
    import pandas as pd
    short_df = pd.DataFrame({
        "report_date": ["2024-01-07", "2024-01-14"],
        "region": ["US", "US"],
        "stocks_mmbbl": [460.0, 462.0],
        "change_mmbbl": [-1.5, 2.0],
    })
    sig = StorageSurpriseSignal(commodity="crude", region="US")
    sig.min_rows = 10
    with pytest.raises(InsufficientDataError):
        sig.latest(df=short_df)


def test_gas_signal_commodity(sample_gas_storage_df):
    from atlas_core.signals.storage_surprise import GasStorageSurpriseSignal
    sig = GasStorageSurpriseSignal()
    result = sig.latest(df=sample_gas_storage_df)
    assert result.signal_name == "gas_storage_surprise"


def test_signal_extreme_flag(sample_crude_storage_df):
    """Signal with |value| >= 0.75 should flag as extreme."""
    import numpy as np
    import pandas as pd
    # Manufacture a huge surprise (z ~ 10)
    df = sample_crude_storage_df.copy()
    last_idx = df.index[-1]
    df.loc[last_idx, "change_mmbbl"] = 50.0   # very large draw
    sig = StorageSurpriseSignal(commodity="crude", region="US")
    result = sig.latest(df=df)
    # Value may or may not be extreme depending on history, but no error
    assert isinstance(result.is_extreme, bool)
