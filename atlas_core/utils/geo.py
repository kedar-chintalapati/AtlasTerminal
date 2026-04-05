"""Geospatial utility functions."""

from __future__ import annotations

import math
from typing import Optional


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    R = 6_371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    return haversine_km(lat1, lon1, lat2, lon2) / 1.852


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing from point 1 to point 2, in degrees [0, 360)."""
    lat1, lat2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def bounding_box_from_point(
    lat: float, lon: float, radius_km: float
) -> tuple[float, float, float, float]:
    """Return (min_lat, min_lon, max_lat, max_lon) around a point."""
    dlat = math.degrees(radius_km / 6371.0)
    dlon = math.degrees(radius_km / (6371.0 * math.cos(math.radians(lat))))
    return lat - dlat, lon - dlon, lat + dlat, lon + dlon


def points_within_radius(
    center_lat: float,
    center_lon: float,
    radius_km: float,
    points: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    """Filter a list of (lat, lon) tuples to those within ``radius_km``."""
    return [
        p for p in points
        if haversine_km(center_lat, center_lon, p[0], p[1]) <= radius_km
    ]


def grid_cells(
    min_lat: float,
    min_lon: float,
    max_lat: float,
    max_lon: float,
    cell_deg: float = 1.0,
) -> list[tuple[float, float]]:
    """
    Return the lower-left corners of a uniform grid covering the bbox.
    """
    cells = []
    lat = min_lat
    while lat < max_lat:
        lon = min_lon
        while lon < max_lon:
            cells.append((round(lat, 6), round(lon, 6)))
            lon += cell_deg
        lat += cell_deg
    return cells
