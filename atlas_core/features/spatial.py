"""
Spatial feature engineering.

Cross-domain geospatial joins: link fire detections, weather events, vessel
positions, and news geo-references to known physical assets.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from atlas_core.schemas.events import FIRMSDetection
from atlas_core.schemas.geo import PhysicalAsset
from atlas_core.schemas.vessels import VesselPosition
from atlas_core.utils.geo import haversine_km


def assets_near_fires(
    assets: list[PhysicalAsset],
    detections: list[FIRMSDetection],
    radius_km: float = 50.0,
) -> pd.DataFrame:
    """
    For each asset, count nearby FIRMS detections and compute aggregate metrics.

    Returns a DataFrame with columns:
      asset_id, asset_name, asset_type, fire_count, max_frp_mw,
      min_distance_km, avg_brightness_k
    """
    rows = []
    for asset in assets:
        nearby = [
            d for d in detections
            if haversine_km(asset.lat, asset.lon, d.lat, d.lon) <= radius_km
        ]
        if not nearby:
            rows.append(
                {
                    "asset_id": asset.asset_id,
                    "asset_name": asset.name,
                    "asset_type": asset.asset_type.value,
                    "fire_count": 0,
                    "max_frp_mw": 0.0,
                    "min_distance_km": float("nan"),
                    "avg_brightness_k": float("nan"),
                    "fire_exposure_score": 0.0,
                }
            )
            continue

        distances = [haversine_km(asset.lat, asset.lon, d.lat, d.lon) for d in nearby]
        frp_vals = [d.frp_mw for d in nearby if d.frp_mw is not None]
        brightness_vals = [d.brightness_k for d in nearby]

        # Exposure score: count × avg intensity × proximity decay
        min_dist = min(distances)
        proximity_decay = max(0.0, 1.0 - min_dist / radius_km)
        avg_frp = np.mean(frp_vals) if frp_vals else 0.0
        exposure = min(1.0, (len(nearby) * 0.3 + avg_frp / 1000.0) * proximity_decay)

        rows.append(
            {
                "asset_id": asset.asset_id,
                "asset_name": asset.name,
                "asset_type": asset.asset_type.value,
                "fire_count": len(nearby),
                "max_frp_mw": max(frp_vals) if frp_vals else 0.0,
                "min_distance_km": min_dist,
                "avg_brightness_k": float(np.mean(brightness_vals)),
                "fire_exposure_score": float(exposure),
            }
        )

    return pd.DataFrame(rows)


def vessels_near_terminals(
    terminals: list[PhysicalAsset],
    positions: list[VesselPosition],
    radius_km: float = 20.0,
) -> pd.DataFrame:
    """
    Group vessel positions around known export terminals.

    Returns: terminal_id, terminal_name, vessel_count, tanker_count,
             avg_speed_kts, vessels_at_anchor
    """
    from atlas_core.schemas.vessels import NavigationStatus, VesselType

    rows = []
    for terminal in terminals:
        nearby = [
            p for p in positions
            if haversine_km(terminal.lat, terminal.lon, p.lat, p.lon) <= radius_km
        ]
        tankers = [p for p in nearby if p.vessel_type in (VesselType.TANKER, VesselType.LNG_CARRIER)]
        anchored = [p for p in nearby if p.nav_status == NavigationStatus.AT_ANCHOR]
        speeds = [p.speed_kts for p in nearby if p.speed_kts is not None]

        rows.append(
            {
                "terminal_id": terminal.asset_id,
                "terminal_name": terminal.name,
                "vessel_count": len(nearby),
                "tanker_count": len(tankers),
                "vessels_at_anchor": len(anchored),
                "avg_speed_kts": float(np.mean(speeds)) if speeds else 0.0,
                "congestion_proxy": min(1.0, len(anchored) / max(len(nearby), 1)),
            }
        )

    return pd.DataFrame(rows)


def news_proximity_score(
    asset: PhysicalAsset,
    news_df: pd.DataFrame,      # must have lat, lon, relevance_score, tone
    radius_km: float = 200.0,
) -> dict[str, Any]:
    """
    Compute news-flow metrics for an asset: volume, sentiment, and proximity.
    """
    if news_df.empty or "lat" not in news_df.columns:
        return {"article_count": 0, "avg_tone": 0.0, "proximity_weighted_volume": 0.0}

    nearby = news_df.dropna(subset=["lat", "lon"])
    nearby = nearby[
        nearby.apply(
            lambda r: haversine_km(asset.lat, asset.lon, r["lat"], r["lon"]) <= radius_km,
            axis=1,
        )
    ]

    if nearby.empty:
        return {"article_count": 0, "avg_tone": 0.0, "proximity_weighted_volume": 0.0}

    dists = nearby.apply(
        lambda r: haversine_km(asset.lat, asset.lon, r["lat"], r["lon"]), axis=1
    )
    weights = (1.0 - dists / radius_km).clip(0, 1)
    weighted_vol = float((weights * nearby.get("relevance_score", pd.Series(1.0))).sum())

    return {
        "article_count": len(nearby),
        "avg_tone": float(nearby["tone"].mean()) if "tone" in nearby else 0.0,
        "proximity_weighted_volume": weighted_vol,
        "negative_fraction": float((nearby["tone"] < -3).mean()) if "tone" in nearby else 0.0,
    }


def spatial_join_to_padd(
    lat_lon_df: pd.DataFrame,
    lat_col: str = "lat",
    lon_col: str = "lon",
) -> pd.DataFrame:
    """
    Assign each row a PADD region based on lat/lon.
    Uses bounding-box assignment (fast; close enough for coarse aggregation).
    """
    from atlas_core.schemas.geo import PADD_BBOXES

    def _assign(row: Any) -> str:
        for padd, bbox in PADD_BBOXES.items():
            if bbox.contains(row[lat_col], row[lon_col]):
                return padd
        return "OTHER"

    df = lat_lon_df.copy()
    df["padd"] = df.apply(_assign, axis=1)
    return df
