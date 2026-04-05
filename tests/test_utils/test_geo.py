"""Tests for geo utilities."""

from __future__ import annotations

import math
import pytest
from atlas_core.utils.geo import haversine_km, haversine_nm, bearing_deg, bounding_box_from_point


def test_haversine_same_point():
    assert haversine_km(29.5, -91.5, 29.5, -91.5) == pytest.approx(0.0)


def test_haversine_known_distance():
    # New Orleans (29.95, -90.07) to Houston (29.76, -95.37) ≈ 510 km
    d = haversine_km(29.95, -90.07, 29.76, -95.37)
    assert abs(d - 510) < 15  # within 15 km


def test_haversine_nm_conversion():
    d_km = haversine_km(0, 0, 1, 0)
    d_nm = haversine_nm(0, 0, 1, 0)
    assert d_nm == pytest.approx(d_km / 1.852, rel=1e-6)


def test_bearing_north():
    b = bearing_deg(0, 0, 1, 0)
    assert abs(b - 0) < 1.0  # north


def test_bearing_east():
    b = bearing_deg(0, 0, 0, 1)
    assert abs(b - 90) < 1.0


def test_bounding_box_symmetric():
    min_lat, min_lon, max_lat, max_lon = bounding_box_from_point(30.0, -90.0, 100.0)
    assert min_lat < 30.0 < max_lat
    assert min_lon < -90.0 < max_lon
    assert abs(30.0 - min_lat - (max_lat - 30.0)) < 0.001


def test_bounding_box_radius():
    """Box should encompass at least the radius distance."""
    min_lat, min_lon, max_lat, max_lon = bounding_box_from_point(30.0, -90.0, 50.0)
    # Top side distance should be at least 50 km
    from atlas_core.utils.geo import haversine_km
    d = haversine_km(30.0, -90.0, max_lat, -90.0)
    assert d >= 49.0  # allow small numerical error
