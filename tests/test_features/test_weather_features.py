"""Tests for weather feature engineering."""

from __future__ import annotations

import pytest
from atlas_core.features.weather import (
    compute_hdd,
    compute_cdd,
    score_weather_risk_for_asset,
)
from atlas_core.schemas.geo import LNG_TERMINALS


def test_hdd_above_base():
    """Temperature below base should produce positive HDD."""
    assert compute_hdd(50.0) == pytest.approx(15.0)


def test_hdd_at_or_above_base():
    """Temperature at or above base should produce zero HDD."""
    assert compute_hdd(65.0) == 0.0
    assert compute_hdd(80.0) == 0.0


def test_cdd_below_base():
    assert compute_cdd(50.0) == 0.0


def test_cdd_above_base():
    assert compute_cdd(85.0) == pytest.approx(20.0)


def test_weather_risk_no_alerts():
    """No nearby alerts → score = 0."""
    asset = LNG_TERMINALS[0]  # Sabine Pass
    score = score_weather_risk_for_asset(asset, alerts=[])
    assert score.score == 0.0


def test_weather_risk_with_nearby_extreme_alert(sample_nws_alerts):
    """Extreme alert close to asset → high score."""
    asset = LNG_TERMINALS[0]  # Sabine Pass at (29.73, -93.87)
    # sample alert is at (29.5, -91.5) — ~200 km away
    score = score_weather_risk_for_asset(asset, alerts=sample_nws_alerts, radius_km=300)
    assert score.score > 0.0
    assert len(score.active_alerts) > 0


def test_weather_risk_extreme_temp_flag():
    asset = LNG_TERMINALS[0]
    score = score_weather_risk_for_asset(
        asset, alerts=[], current_temp_f=105.0, extreme_temp_f_high=100.0
    )
    assert score.extreme_temp_flag is True


def test_weather_risk_asset_id():
    asset = LNG_TERMINALS[0]
    score = score_weather_risk_for_asset(asset, alerts=[])
    assert score.asset_id == asset.asset_id
