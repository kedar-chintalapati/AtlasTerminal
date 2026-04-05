"""
Background data-refresh scheduler.

Uses APScheduler to periodically fetch data from all connectors and
refresh the DuckDB store.  Runs as a background task inside FastAPI.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from atlas_app.backend.config import backend_settings
from atlas_core.config import settings
from atlas_core.store.duckdb_store import DuckDBStore


def build_scheduler(store: DuckDBStore) -> AsyncIOScheduler:
    """Build and return a configured APScheduler instance."""
    scheduler = AsyncIOScheduler(timezone="UTC")

    # ── Energy (EIA) ──────────────────────────────────────────────────
    if settings.eia_api_key:
        scheduler.add_job(
            _refresh_energy,
            "interval",
            seconds=backend_settings.energy_refresh_interval,
            id="energy_refresh",
            kwargs={"store": store},
            next_run_time=datetime.now(tz=timezone.utc),  # run immediately on start
        )

    # ── Weather (NWS) ──────────────────────────────────────────────────
    scheduler.add_job(
        _refresh_weather_alerts,
        "interval",
        seconds=backend_settings.weather_refresh_interval,
        id="weather_alerts",
        kwargs={"store": store},
        next_run_time=datetime.now(tz=timezone.utc),
    )

    # ── NASA FIRMS ─────────────────────────────────────────────────────
    if settings.nasa_firms_map_key and settings.nasa_firms_map_key != "DEMO_KEY":
        scheduler.add_job(
            _refresh_fires,
            "interval",
            seconds=backend_settings.firms_refresh_interval,
            id="firms_refresh",
            kwargs={"store": store},
            next_run_time=datetime.now(tz=timezone.utc),
        )

    # ── GDELT ─────────────────────────────────────────────────────────
    scheduler.add_job(
        _refresh_news,
        "interval",
        seconds=backend_settings.gdelt_refresh_interval,
        id="gdelt_refresh",
        kwargs={"store": store},
        next_run_time=datetime.now(tz=timezone.utc),
    )

    return scheduler


# ─── Job implementations ──────────────────────────────────────────────────────

async def _refresh_energy(store: DuckDBStore) -> None:
    try:
        import pandas as pd
        from atlas_core.connectors.eia import EIAConnector
        from atlas_core.features.energy import compute_storage_surprise

        async with EIAConnector() as eia:
            crude = await eia.get_crude_storage()
            gas = await eia.get_gas_storage()

        if crude:
            df = pd.DataFrame([r.model_dump() for r in crude])
            store.upsert_dataframe("crude_storage", df)
            # Compute and store surprises
            surprise_df = compute_storage_surprise(df, commodity="crude")
            if not surprise_df.empty:
                store.upsert_dataframe("storage_surprises", surprise_df)

        if gas:
            df = pd.DataFrame([r.model_dump() for r in gas])
            store.upsert_dataframe("gas_storage", df)

        logger.info(f"[scheduler] energy: {len(crude)} crude, {len(gas)} gas rows")
    except Exception as exc:
        logger.error(f"[scheduler] energy refresh failed: {exc}")


async def _refresh_weather_alerts(store: DuckDBStore) -> None:
    try:
        import pandas as pd
        from atlas_core.connectors.noaa_nws import NWSConnector

        async with NWSConnector() as nws:
            # Gulf-adjacent states
            all_alerts = []
            for state in ["TX", "LA", "MS", "AL", "FL", "GA", "SC", "NC"]:
                try:
                    alerts = await nws.get_alerts_for_state(state)
                    all_alerts.extend(alerts)
                except Exception:
                    pass

        if all_alerts:
            df = pd.DataFrame(
                [
                    {
                        "alert_id": a.alert_id,
                        "headline": a.headline[:500],
                        "event_type": a.event_type,
                        "severity": a.severity.value,
                        "onset": a.onset,
                        "expires": a.expires,
                        "centroid_lat": a.centroid_lat,
                        "centroid_lon": a.centroid_lon,
                    }
                    for a in all_alerts
                ]
            )
            store.upsert_dataframe("nws_alerts", df)
        logger.info(f"[scheduler] weather: {len(all_alerts)} alerts")
    except Exception as exc:
        logger.error(f"[scheduler] weather refresh failed: {exc}")


async def _refresh_fires(store: DuckDBStore) -> None:
    try:
        import pandas as pd
        from atlas_core.connectors.nasa_firms import NASAFIRMSConnector

        async with NASAFIRMSConnector() as firms:
            fires = await firms.get_fires_gulf_coast(days=2)
            fires += await firms.get_fires_permian_basin(days=2)

        if fires:
            df = pd.DataFrame([f.model_dump() for f in fires])
            store.upsert_dataframe("firms_detections", df)
        logger.info(f"[scheduler] firms: {len(fires)} fire detections")
    except Exception as exc:
        logger.error(f"[scheduler] firms refresh failed: {exc}")


async def _refresh_news(store: DuckDBStore) -> None:
    try:
        import pandas as pd
        from atlas_core.connectors.gdelt import GDELTConnector

        async with GDELTConnector() as gdelt:
            articles = await gdelt.search_articles(
                "LNG OR (natural gas) OR (crude oil) (pipeline OR terminal OR storage OR refinery)",
                timespan="24h",
                max_records=250,
            )

        if articles:
            df = pd.DataFrame(
                [
                    {
                        "event_id": a.event_id,
                        "publish_date": a.publish_date,
                        "url": a.url[:500],
                        "title": a.title[:500],
                        "tone": a.tone,
                        "relevance_score": a.relevance_score,
                        "lat": a.lat,
                        "lon": a.lon,
                    }
                    for a in articles
                ]
            )
            store.upsert_dataframe("gdelt_events", df)
        logger.info(f"[scheduler] gdelt: {len(articles)} articles")
    except Exception as exc:
        logger.error(f"[scheduler] gdelt refresh failed: {exc}")
