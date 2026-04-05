"""Tests for event study research tool."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from atlas_core.research.event_study import run_event_study
from atlas_core.exceptions import ResearchError


@pytest.fixture
def returns_and_signal():
    """Synthetic daily returns and signal series."""
    np.random.seed(0)
    idx = pd.date_range("2020-01-01", periods=500)
    returns = pd.Series(np.random.normal(0, 0.01, 500), index=idx, name="return")
    signal = pd.Series(np.random.normal(0, 1, 500), index=idx, name="signal")
    return signal, returns


def test_event_study_runs(returns_and_signal):
    signal, returns = returns_and_signal
    result = run_event_study(signal, returns, windows=[1, 5, 10], signal_threshold=1.0)
    assert result.event_count > 0
    assert set(result.windows) == {1, 5, 10}
    assert set(result.avg_returns.keys()) == {1, 5, 10}
    assert set(result.hit_rates.keys()) == {1, 5, 10}


def test_event_study_hit_rates_in_range(returns_and_signal):
    signal, returns = returns_and_signal
    result = run_event_study(signal, returns, windows=[1, 5])
    for w, hr in result.hit_rates.items():
        assert 0.0 <= hr <= 1.0, f"hit_rate[{w}] = {hr}"


def test_event_study_cumulative_path(returns_and_signal):
    signal, returns = returns_and_signal
    result = run_event_study(signal, returns, windows=[5])
    assert not result.cumulative_path.empty
    assert "offset" in result.cumulative_path.columns
    assert "cumulative" in result.cumulative_path.columns


def test_event_study_no_events_raises():
    idx = pd.date_range("2020-01-01", periods=100)
    signal = pd.Series(np.zeros(100), index=idx)
    returns = pd.Series(np.random.normal(0, 0.01, 100), index=idx)
    with pytest.raises(ResearchError):
        run_event_study(signal, returns, signal_threshold=2.0)


def test_event_study_direction_filter(returns_and_signal):
    signal, returns = returns_and_signal
    result_bull = run_event_study(signal, returns, windows=[5], direction=1, signal_threshold=0.5)
    result_bear = run_event_study(signal, returns, windows=[5], direction=-1, signal_threshold=0.5)
    # Bullish and bearish event sets should be different
    bull_dates = set(result_bull.metadata["event_dates"])
    bear_dates = set(result_bear.metadata["event_dates"])
    assert bull_dates.isdisjoint(bear_dates)
