"""Tests for NASA FIRMS connector parsing."""

from __future__ import annotations

from atlas_core.connectors.nasa_firms import _parse_firms_csv


SAMPLE_CSV = """latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight
29.783,-93.890,344.5,0.39,0.36,2024-08-01,1415,N,nominal,2.0NRT,295.2,15.3,D
30.123,-94.231,378.1,0.40,0.37,2024-08-01,1416,N,high,2.0NRT,298.0,87.5,D
"""


def test_parse_firms_csv_basic():
    detections = _parse_firms_csv(SAMPLE_CSV, satellite="VIIRS_SNPP")
    assert len(detections) == 2


def test_parse_firms_csv_fields():
    detections = _parse_firms_csv(SAMPLE_CSV, satellite="VIIRS_SNPP")
    d = detections[0]
    assert d.lat == pytest.approx(29.783)
    assert d.lon == pytest.approx(-93.890)
    assert d.satellite == "VIIRS_SNPP"
    assert d.confidence in ("nominal", "high", "low")
    assert d.daynight == "D"


def test_parse_firms_csv_empty():
    assert _parse_firms_csv("") == []


def test_parse_firms_csv_header_only():
    header = "latitude,longitude,brightness,acq_date,acq_time\n"
    assert _parse_firms_csv(header) == []


def test_parse_firms_csv_detection_id_deterministic():
    """Same inputs should produce the same detection_id."""
    d1 = _parse_firms_csv(SAMPLE_CSV, satellite="MODIS_NRT")
    d2 = _parse_firms_csv(SAMPLE_CSV, satellite="MODIS_NRT")
    assert d1[0].detection_id == d2[0].detection_id


import pytest
