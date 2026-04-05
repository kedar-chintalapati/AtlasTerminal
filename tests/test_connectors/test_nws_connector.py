"""Tests for NWS connector parsing logic."""

from __future__ import annotations

import pytest
from atlas_core.connectors.noaa_nws import _parse_alerts, _parse_dt
from atlas_core.schemas.weather import AlertSeverity


def test_parse_active_alerts_basic():
    data = {
        "features": [
            {
                "id": "https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0.001",
                "properties": {
                    "id": "alert-001",
                    "headline": "Hurricane Watch issued for coastal areas",
                    "event": "Hurricane Watch",
                    "severity": "Extreme",
                    "certainty": "Likely",
                    "urgency": "Immediate",
                    "onset": "2024-08-15T12:00:00-05:00",
                    "expires": "2024-08-16T12:00:00-05:00",
                    "affectedZones": ["TXZ100", "TXZ101"],
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-91.5, 29.5],
                },
            }
        ]
    }
    alerts = _parse_alerts(data)
    assert len(alerts) == 1
    a = alerts[0]
    assert a.alert_id == "alert-001"
    assert a.severity == AlertSeverity.EXTREME
    assert a.centroid_lat == 29.5
    assert a.centroid_lon == -91.5
    assert "TXZ100" in a.affected_zones


def test_parse_active_alerts_empty():
    assert _parse_alerts({}) == []
    assert _parse_alerts({"features": []}) == []


def test_parse_active_alerts_missing_geometry():
    """Alerts without geometry should still parse."""
    data = {
        "features": [
            {
                "properties": {
                    "id": "no-geo",
                    "event": "Wind Advisory",
                    "severity": "Minor",
                    "certainty": "Likely",
                    "urgency": "Expected",
                },
                "geometry": None,
            }
        ]
    }
    alerts = _parse_alerts(data)
    assert len(alerts) == 1
    assert alerts[0].centroid_lat is None


def test_parse_datetime_iso():
    dt = _parse_dt("2024-08-15T12:00:00-05:00")
    assert dt is not None
    assert dt.year == 2024


def test_parse_datetime_z():
    dt = _parse_dt("2024-08-15T12:00:00Z")
    assert dt is not None


def test_parse_datetime_none():
    assert _parse_dt(None) is None
    assert _parse_dt("") is None
