"""Tests for vectorised backtest engine."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from atlas_core.research.backtest import run_backtest


@pytest.fixture
def signal_and_returns():
    np.random.seed(1)
    idx = pd.date_range("2020-01-01", periods=500)
    returns = pd.Series(np.random.normal(0, 0.01, 500), index=idx)
    signal = pd.Series(np.random.normal(0, 1, 500), index=idx)
    return signal, returns


def test_backtest_runs(signal_and_returns):
    signal, returns = signal_and_returns
    result = run_backtest(signal, returns, threshold=0.5)
    assert isinstance(result.total_return, float)
    assert isinstance(result.sharpe, float)
    assert isinstance(result.hit_rate, float)
    assert result.num_trades >= 0


def test_backtest_hit_rate_in_range(signal_and_returns):
    signal, returns = signal_and_returns
    result = run_backtest(signal, returns, threshold=0.5)
    assert 0.0 <= result.hit_rate <= 1.0


def test_backtest_pnl_series_length(signal_and_returns):
    signal, returns = signal_and_returns
    result = run_backtest(signal, returns, threshold=0.5)
    # PnL series should cover common index
    assert len(result.pnl_series) > 0


def test_backtest_costs_reduce_return(signal_and_returns):
    """With transaction costs, total return should be <= without."""
    signal, returns = signal_and_returns
    r_no_cost = run_backtest(signal, returns, cost_bps=0).total_return
    r_with_cost = run_backtest(signal, returns, cost_bps=10).total_return
    assert r_with_cost <= r_no_cost


def test_backtest_dates(signal_and_returns):
    signal, returns = signal_and_returns
    result = run_backtest(signal, returns)
    assert result.start_date[:4] == "2020"
    assert result.end_date[:4] >= "2021"


def test_backtest_insufficient_data():
    idx = pd.date_range("2020-01-01", periods=5)
    signal = pd.Series([1, -1, 0, 1, -1], index=idx, dtype=float)
    returns = pd.Series([0.01, -0.01, 0, 0.01, -0.01], index=idx, dtype=float)
    with pytest.raises(ValueError, match="Insufficient"):
        run_backtest(signal, returns, threshold=0.5)
