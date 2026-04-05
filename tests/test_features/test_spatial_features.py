"""Tests for spatial feature engineering."""

from __future__ import annotations

import pytest
from atlas_core.features.spatial import assets_near_fires, vessels_near_terminals
from atlas_core.schemas.geo import LNG_TERMINALS


def test_assets_near_fires_with_nearby_fire(sample_fire_detections):
    """Fire detections near Sabine Pass should register exposure."""
    result = assets_near_fires(LNG_TERMINALS[:2], sample_fire_detections, radius_km=100)
    assert not result.empty
    sabine = result[result["asset_id"] == "sabine_pass"]
    assert not sabine.empty
    # fire-001 is at (29.8, -93.9), Sabine Pass at (29.73, -93.87) — ~8 km
    assert sabine["fire_count"].iloc[0] > 0
    assert sabine["fire_exposure_score"].iloc[0] > 0.0


def test_assets_near_fires_no_fires():
    """No detections → all fire counts should be 0."""
    result = assets_near_fires(LNG_TERMINALS, detections=[], radius_km=100)
    assert (result["fire_count"] == 0).all()
    assert (result["fire_exposure_score"] == 0.0).all()


def test_assets_near_fires_exposure_score_range(sample_fire_detections):
    """Exposure score must be in [0, 1]."""
    result = assets_near_fires(LNG_TERMINALS, sample_fire_detections, radius_km=200)
    assert (result["fire_exposure_score"] >= 0).all()
    assert (result["fire_exposure_score"] <= 1).all()


def test_vessels_near_terminals_empty():
    from atlas_core.schemas.vessels import VesselPosition, VesselType, NavigationStatus
    from datetime import datetime, timezone
    pos = VesselPosition(
        mmsi="123456789",
        vessel_type=VesselType.TANKER,
        timestamp=datetime.now(tz=timezone.utc),
        lat=35.0,
        lon=-75.0,  # far from Gulf Coast terminals
    )
    result = vessels_near_terminals(LNG_TERMINALS, [pos], radius_km=25)
    assert (result["vessel_count"] == 0).all()
