"""
Weather feature engineering.

Transforms raw NWS/NOAA/NDBC data into energy-relevant weather features:
* Heating/Cooling Degree Days (HDD/CDD)
* Storm exposure scores per asset
* Wind/solar generation regime flags
* Offshore marine conditions
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd

from atlas_core.schemas.geo import PhysicalAsset
from atlas_core.schemas.weather import NWSAlert, WeatherRiskScore
from atlas_core.utils.geo import haversine_km
from atlas_core.utils.math import rolling_zscore

# HDD/CDD base temperature (Fahrenheit)
_BASE_TEMP_F = 65.0
_BASE_TEMP_C = (_BASE_TEMP_F - 32) * 5 / 9  # 18.33 °C


def compute_hdd(avg_temp_f: float, base_f: float = _BASE_TEMP_F) -> float:
    """Heating Degree Days for one day."""
    return max(0.0, base_f - avg_temp_f)


def compute_cdd(avg_temp_f: float, base_f: float = _BASE_TEMP_F) -> float:
    """Cooling Degree Days for one day."""
    return max(0.0, avg_temp_f - base_f)


def compute_hdd_cdd_series(
    climate_df: pd.DataFrame,
    tmax_col: str = "TMAX",
    tmin_col: str = "TMIN",
    base_f: float = _BASE_TEMP_F,
    is_celsius: bool = False,
) -> pd.DataFrame:
    """
    Compute HDD/CDD from a NOAA CDO climate observation DataFrame.

    Expected columns: station_id, date, TMAX, TMIN (tenths-of-degrees or °C/°F).
    NOAA GHCND values are in tenths of degrees Celsius by default.
    """
    df = climate_df.copy()
    if df.empty:
        return df

    # NOAA GHCND uses tenths of Celsius
    scale = 10.0 if not is_celsius else 1.0

    # Pivot to wide if long format
    if "data_type" in df.columns:
        df = df.pivot_table(
            index=["station_id", "date"], columns="data_type", values="value"
        ).reset_index()

    for col in [tmax_col, tmin_col]:
        if col not in df.columns:
            df[col] = np.nan

    df["tmax_f"] = (df[tmax_col] / scale) * 9 / 5 + 32
    df["tmin_f"] = (df[tmin_col] / scale) * 9 / 5 + 32
    df["tavg_f"] = (df["tmax_f"] + df["tmin_f"]) / 2

    df["hdd"] = df["tavg_f"].apply(lambda t: compute_hdd(t, base_f))
    df["cdd"] = df["tavg_f"].apply(lambda t: compute_cdd(t, base_f))

    return df


def compute_population_weighted_hdd_cdd(
    station_hdd_cdd: pd.DataFrame,
    region_weights: Optional[dict[str, float]] = None,
) -> pd.DataFrame:
    """
    Aggregate station-level HDD/CDD to a region-level pop-weighted value.

    If ``region_weights`` is not provided, gives equal weight to all stations.
    """
    df = station_hdd_cdd.copy()
    if "date" not in df.columns:
        raise ValueError("Expected 'date' column")

    if region_weights:
        df["weight"] = df["station_id"].map(region_weights).fillna(1.0)
    else:
        df["weight"] = 1.0

    result = (
        df.groupby("date")
        .apply(
            lambda g: pd.Series(
                {
                    "hdd_wtd": np.average(g["hdd"].fillna(0), weights=g["weight"]),
                    "cdd_wtd": np.average(g["cdd"].fillna(0), weights=g["weight"]),
                    "station_count": len(g),
                }
            )
        )
        .reset_index()
    )
    result["hdd_z"] = rolling_zscore(result["hdd_wtd"])
    result["cdd_z"] = rolling_zscore(result["cdd_wtd"])
    return result


def score_weather_risk_for_asset(
    asset: PhysicalAsset,
    active_alerts: list[NWSAlert],
    radius_km: float = 150.0,
    extreme_temp_f_high: float = 100.0,
    extreme_temp_f_low: float = 15.0,
    current_temp_f: Optional[float] = None,
) -> WeatherRiskScore:
    """
    Compute a composite weather-risk score for a physical asset.

    Factors:
    1. Active NWS alerts within radius (weighted by severity)
    2. Proximity to storm centres
    3. Extreme-temperature flag
    """
    from atlas_core.schemas.weather import AlertSeverity

    severity_weights = {
        AlertSeverity.EXTREME: 1.0,
        AlertSeverity.SEVERE: 0.75,
        AlertSeverity.MODERATE: 0.5,
        AlertSeverity.MINOR: 0.25,
        AlertSeverity.UNKNOWN: 0.1,
    }

    relevant_alerts: list[str] = []
    alert_score = 0.0
    storm_proximity_km: Optional[float] = None

    for alert in active_alerts:
        if alert.centroid_lat is None or alert.centroid_lon is None:
            continue
        dist = haversine_km(asset.lat, asset.lon, alert.centroid_lat, alert.centroid_lon)
        if dist <= radius_km:
            w = severity_weights.get(alert.severity, 0.1)
            # Distance decay
            decay = max(0.0, 1.0 - dist / radius_km)
            alert_score += w * decay
            relevant_alerts.append(alert.alert_id)
            if storm_proximity_km is None or dist < storm_proximity_km:
                storm_proximity_km = dist

    # Clamp
    alert_score = min(1.0, alert_score)

    extreme_temp = False
    high_wind = False
    if current_temp_f is not None:
        extreme_temp = current_temp_f >= extreme_temp_f_high or current_temp_f <= extreme_temp_f_low

    # Check for wind alerts
    wind_events = {"High Wind Warning", "Wind Advisory", "High Wind Watch"}
    high_wind = any(a.event_type in wind_events for a in active_alerts if a.alert_id in relevant_alerts)

    # Final composite score
    score = min(1.0, alert_score + (0.2 if extreme_temp else 0.0) + (0.15 if high_wind else 0.0))

    return WeatherRiskScore(
        asset_id=asset.asset_id,
        asset_type=asset.asset_type.value,
        lat=asset.lat,
        lon=asset.lon,
        score=score,
        active_alerts=relevant_alerts,
        storm_proximity_km=storm_proximity_km,
        extreme_temp_flag=extreme_temp,
        high_wind_flag=high_wind,
        computed_at=datetime.utcnow(),
    )


def compute_marine_risk(
    buoy_obs_df: pd.DataFrame,
    significant_wave_height_threshold_m: float = 2.5,
    high_wind_threshold_ms: float = 15.0,
) -> pd.DataFrame:
    """
    Flag dangerous marine conditions from NDBC buoy observations.

    Returns a DataFrame with added columns: high_seas_flag, high_wind_flag, risk_score.
    """
    df = buoy_obs_df.copy()
    if df.empty or "wave_height_m" not in df.columns:
        return df

    df["high_seas_flag"] = df["wave_height_m"].fillna(0) >= significant_wave_height_threshold_m
    df["high_wind_flag"] = df["wind_speed_ms"].fillna(0) >= high_wind_threshold_ms
    df["risk_score"] = (
        df["high_seas_flag"].astype(float) * 0.6
        + df["high_wind_flag"].astype(float) * 0.4
    )
    return df
