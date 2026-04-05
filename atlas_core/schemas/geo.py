"""
Geospatial reference schemas: assets, regions, routes.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class AssetType(str, Enum):
    REFINERY = "refinery"
    LNG_TERMINAL = "lng_terminal"
    PIPELINE = "pipeline"
    STORAGE_HUB = "storage_hub"
    PORT = "port"
    POWER_PLANT = "power_plant"
    PRODUCTION_BASIN = "production_basin"
    OFFSHORE_PLATFORM = "offshore_platform"
    SUBSTATION = "substation"


class PhysicalAsset(BaseModel):
    """A named, located physical commodity asset."""
    asset_id: str
    name: str
    asset_type: AssetType
    lat: float
    lon: float
    region: str = ""
    country: str = "US"
    capacity: Optional[float] = None
    capacity_unit: str = ""
    operator: str = ""
    # WKT for polygons (basins) or linestrings (pipelines)
    geometry_wkt: Optional[str] = None
    tags: dict[str, str] = Field(default_factory=dict)


class GeoBoundingBox(BaseModel):
    """Axis-aligned bounding box for spatial filtering."""
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

    def contains(self, lat: float, lon: float) -> bool:
        return (
            self.min_lat <= lat <= self.max_lat
            and self.min_lon <= lon <= self.max_lon
        )


class PADDRegion(str, Enum):
    PADD1 = "PADD1"    # East Coast
    PADD2 = "PADD2"    # Midwest
    PADD3 = "PADD3"    # Gulf Coast
    PADD4 = "PADD4"    # Rocky Mountain
    PADD5 = "PADD5"    # West Coast
    US = "US"


# Key US energy geography bounding boxes
PADD_BBOXES: dict[str, GeoBoundingBox] = {
    "PADD1": GeoBoundingBox(min_lat=24.0, min_lon=-82.0, max_lat=47.5, max_lon=-66.0),
    "PADD2": GeoBoundingBox(min_lat=36.5, min_lon=-104.0, max_lat=49.5, max_lon=-80.0),
    "PADD3": GeoBoundingBox(min_lat=25.0, min_lon=-107.0, max_lat=37.0, max_lon=-88.0),
    "PADD4": GeoBoundingBox(min_lat=40.0, min_lon=-117.0, max_lat=49.5, max_lon=-102.0),
    "PADD5": GeoBoundingBox(min_lat=18.0, min_lon=-180.0, max_lat=65.0, max_lon=-108.0),
}

# Canonical US natural-gas hubs
GAS_HUBS: dict[str, tuple[float, float]] = {
    "henry_hub": (29.9558, -90.3508),
    "chicago": (41.8781, -87.6298),
    "dominion_south": (38.5976, -82.7541),
    "algonquin": (41.2568, -73.9474),
    "socal_border": (34.0522, -118.2437),
    "waha": (31.4993, -104.0200),
    "permian": (31.9686, -99.9018),
}

# Key LNG export terminals
LNG_TERMINALS: list[PhysicalAsset] = [
    PhysicalAsset(
        asset_id="sabine_pass",
        name="Sabine Pass LNG",
        asset_type=AssetType.LNG_TERMINAL,
        lat=29.7326, lon=-93.8738,
        region="Gulf Coast", operator="Cheniere",
        capacity=30.0, capacity_unit="mtpa",
    ),
    PhysicalAsset(
        asset_id="corpus_christi",
        name="Corpus Christi LNG",
        asset_type=AssetType.LNG_TERMINAL,
        lat=27.7828, lon=-97.3893,
        region="Gulf Coast", operator="Cheniere",
        capacity=15.0, capacity_unit="mtpa",
    ),
    PhysicalAsset(
        asset_id="freeport_lng",
        name="Freeport LNG",
        asset_type=AssetType.LNG_TERMINAL,
        lat=28.9503, lon=-95.3679,
        region="Gulf Coast", operator="Freeport LNG",
        capacity=15.0, capacity_unit="mtpa",
    ),
    PhysicalAsset(
        asset_id="cameron_lng",
        name="Cameron LNG",
        asset_type=AssetType.LNG_TERMINAL,
        lat=30.0357, lon=-93.3552,
        region="Gulf Coast", operator="Sempra",
        capacity=12.0, capacity_unit="mtpa",
    ),
    PhysicalAsset(
        asset_id="cove_point",
        name="Cove Point LNG",
        asset_type=AssetType.LNG_TERMINAL,
        lat=38.4046, lon=-76.3853,
        region="East Coast", operator="Dominion",
        capacity=5.75, capacity_unit="mtpa",
    ),
]
