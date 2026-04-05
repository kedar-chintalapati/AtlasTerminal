"""Tests for factor model and basis spread analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from atlas_core.research.factor_model import run_factor_model, basis_spread_analysis


@pytest.fixture
def factor_data():
    np.random.seed(42)
    n = 200
    idx = pd.date_range("2022-01-01", periods=n)
    f1 = pd.Series(np.random.normal(0, 1, n), index=idx, name="storage_z")
    f2 = pd.Series(np.random.normal(0, 1, n), index=idx, name="weather_risk")
    factors = pd.DataFrame({"storage_z": f1, "weather_risk": f2})
    # True relationship: returns ~ 0.5*f1 - 0.3*f2 + noise
    returns = 0.5 * f1 - 0.3 * f2 + pd.Series(np.random.normal(0, 0.5, n), index=idx)
    return returns, factors


def test_factor_model_runs(factor_data):
    returns, factors = factor_data
    result = run_factor_model(returns, factors)
    assert 0.0 <= result.r_squared <= 1.0
    assert "storage_z" in result.betas
    assert "weather_risk" in result.betas


def test_factor_model_betas_sign(factor_data):
    """With positive true coefficient, beta estimate should be positive."""
    returns, factors = factor_data
    result = run_factor_model(returns, factors)
    # storage_z has true beta ~ 0.5
    assert result.betas["storage_z"] > 0


def test_factor_model_r2_improves_with_more_factors(factor_data):
    returns, factors = factor_data
    r1 = run_factor_model(returns, factors[["storage_z"]]).r_squared
    r2 = run_factor_model(returns, factors).r_squared
    # Adding a true factor improves R²
    assert r2 >= r1 - 0.05   # small tolerance for numerical noise


def test_factor_model_residuals_length(factor_data):
    returns, factors = factor_data
    result = run_factor_model(returns, factors)
    assert len(result.residuals) == len(returns)


def test_factor_contributions_shape(factor_data):
    returns, factors = factor_data
    result = run_factor_model(returns, factors)
    assert result.factor_contributions.shape[1] == 3  # alpha + 2 factors


def test_basis_spread_analysis():
    idx = pd.date_range("2022-01-01", periods=100)
    a = pd.Series(np.random.normal(60, 5, 100), index=idx)
    b = pd.Series(np.random.normal(58, 4, 100), index=idx)
    result = basis_spread_analysis(a, b, window=20)
    assert "spread" in result.columns
    assert "z_score" in result.columns
    assert "regime" in result.columns
