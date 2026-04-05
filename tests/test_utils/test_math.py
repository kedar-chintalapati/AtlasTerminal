"""Tests for math utilities."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from atlas_core.utils.math import (
    rolling_zscore,
    seasonal_zscore,
    pct_deviation,
    information_ratio,
    max_drawdown,
    hit_rate,
    winsorise,
)


def test_rolling_zscore_mean_zero():
    s = pd.Series(np.random.normal(10, 2, 100))
    z = rolling_zscore(s, window=20)
    # Z-scores should roughly have mean 0 and std 1 (for long window)
    valid = z.dropna()
    assert abs(valid.mean()) < 0.5


def test_rolling_zscore_min_periods():
    """Short series with min_periods should produce NaN at start."""
    s = pd.Series(range(20), dtype=float)
    z = rolling_zscore(s, window=10, min_periods=8)
    assert z.iloc[:7].isna().all()
    assert not z.iloc[-5:].isna().all()


def test_seasonal_zscore_output_length():
    s = pd.Series(np.random.normal(0, 1, 200), dtype=float)
    z = seasonal_zscore(s, period=52)
    assert len(z) == 200


def test_pct_deviation_returns_series():
    s = pd.Series(np.random.normal(100, 5, 100), dtype=float)
    d = pct_deviation(s)
    assert len(d) == 100


def test_information_ratio_zero_std():
    r = pd.Series([0.0, 0.0, 0.0])
    ir = information_ratio(r)
    assert ir == 0.0


def test_information_ratio_positive():
    r = pd.Series([0.01] * 252)
    ir = information_ratio(r)
    assert ir > 0


def test_max_drawdown_no_loss():
    r = pd.Series([0.01] * 50)
    assert max_drawdown(r) == pytest.approx(0.0, abs=1e-10)


def test_max_drawdown_with_loss():
    r = pd.Series([0.1, -0.5, 0.2, -0.3])
    dd = max_drawdown(r)
    assert dd < 0


def test_hit_rate_all_correct():
    signal = pd.Series([1.0, -1.0, 1.0, -1.0])
    returns = pd.Series([0.01, -0.01, 0.02, -0.02])
    assert hit_rate(signal, returns) == pytest.approx(1.0)


def test_hit_rate_all_wrong():
    signal = pd.Series([1.0, -1.0])
    returns = pd.Series([-0.01, 0.01])
    assert hit_rate(signal, returns) == pytest.approx(0.0)


def test_winsorise_clips_extremes():
    s = pd.Series([-100.0, 0.0, 1.0, 2.0, 100.0])
    w = winsorise(s, lower=0.2, upper=0.8)
    assert w.max() <= 2.0
    assert w.min() >= 0.0
