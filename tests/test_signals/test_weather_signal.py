"""Tests for WeatherRiskSignal."""

from __future__ import annotations

import pytest
from atlas_core.signals.weather_risk import WeatherRiskSignal


def test_no_alerts_neutral(sample_nws_alerts):
    """WeatherRiskSignal with no alerts should be neutral / low score."""
    sig = WeatherRiskSignal()
    result = sig.latest(alerts=[])
    assert result.direction in ("neutral", "bearish")
    assert 0.0 <= result.confidence <= 1.0


def test_extreme_alert_raises_score(sample_nws_alerts):
    sig = WeatherRiskSignal()
    result = sig.latest(alerts=sample_nws_alerts)
    # Extreme alert within range → some risk
    assert isinstance(result.value, float)
    assert -1.0 <= result.value <= 1.0


def test_components_present(sample_nws_alerts):
    sig = WeatherRiskSignal()
    result = sig.latest(alerts=sample_nws_alerts)
    assert len(result.components) >= 1
    assert result.components[0].name == "nws_alert_exposure"


def test_hdd_cdd_component_added(sample_nws_alerts):
    sig = WeatherRiskSignal()
    result = sig.latest(alerts=sample_nws_alerts, hdd_cdd_z=2.5)
    names = [c.name for c in result.components]
    assert "hdd_cdd_z" in names
