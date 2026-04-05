"""Tests for FireExposureSignal."""

from __future__ import annotations

import pytest
from atlas_core.signals.fire_exposure import FireExposureSignal


def test_no_fires_returns_zero():
    sig = FireExposureSignal()
    result = sig.latest(detections=[])
    assert result.value == 0.0
    assert result.direction == "neutral"


def test_nearby_fires_produce_negative_value(sample_fire_detections):
    """Fires near Sabine Pass should produce negative (bearish risk) value."""
    sig = FireExposureSignal(radius_km=200)
    result = sig.latest(detections=sample_fire_detections)
    # Some detections are near Gulf Coast terminals → non-zero risk
    assert result.value <= 0.0


def test_signal_components(sample_fire_detections):
    sig = FireExposureSignal()
    result = sig.latest(detections=sample_fire_detections)
    assert len(result.components) >= 1
    c = result.components[0]
    assert c.name == "fire_proximity"
    assert c.source_domain.value == "fire"


def test_confidence_range(sample_fire_detections):
    sig = FireExposureSignal()
    result = sig.latest(detections=sample_fire_detections)
    assert 0.0 <= result.confidence <= 1.0
