"""
Shared pytest fixtures.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

from atlas_core.store.duckdb_store import DuckDBStore


@pytest.fixture(scope="session")
def event_loop_policy():
    return asyncio.DefaultEventLoopPolicy()


@pytest.fixture
def tmp_store(tmp_path: Path) -> Generator[DuckDBStore, None, None]:
    """In-memory DuckDB store for tests."""
    store = DuckDBStore(in_memory=True)
    store.initialize()
    yield store
    store.close()


@pytest.fixture
def sample_crude_storage_df() -> pd.DataFrame:
    """Synthetic crude storage DataFrame."""
    dates = pd.date_range("2022-01-07", periods=104, freq="W")
    import numpy as np
    rng = np.random.default_rng(42)
    stocks = 440.0 + rng.normal(0, 10, 104).cumsum() * 0.1
    changes = rng.normal(-0.5, 3.5, 104)
    return pd.DataFrame(
        {
            "report_date": [d.date().isoformat() for d in dates],
            "region": "US",
            "stocks_mmbbl": stocks,
            "change_mmbbl": changes,
            "five_year_avg_mmbbl": 440.0,
            "source": "EIA",
        }
    )


@pytest.fixture
def sample_gas_storage_df() -> pd.DataFrame:
    """Synthetic natural-gas storage DataFrame."""
    dates = pd.date_range("2022-01-07", periods=104, freq="W")
    import numpy as np
    rng = np.random.default_rng(7)
    stocks = 2000.0 + rng.normal(0, 50, 104).cumsum() * 0.2
    changes = rng.normal(0, 80, 104)
    return pd.DataFrame(
        {
            "report_date": [d.date().isoformat() for d in dates],
            "region": "US",
            "stocks_bcf": stocks,
            "change_bcf": changes,
            "five_year_avg_bcf": 2000.0,
            "source": "EIA",
        }
    )


@pytest.fixture
def sample_nws_alerts():
    """Synthetic NWS alert list."""
    from atlas_core.schemas.weather import AlertCertainty, AlertSeverity, NWSAlert
    return [
        NWSAlert(
            alert_id="test-001",
            headline="Hurricane Watch",
            event_type="Hurricane Watch",
            severity=AlertSeverity.EXTREME,
            certainty=AlertCertainty.LIKELY,
            urgency="Immediate",
            centroid_lat=29.5,
            centroid_lon=-91.5,
        ),
        NWSAlert(
            alert_id="test-002",
            headline="High Wind Advisory",
            event_type="High Wind Advisory",
            severity=AlertSeverity.MODERATE,
            certainty=AlertCertainty.OBSERVED,
            urgency="Expected",
            centroid_lat=30.0,
            centroid_lon=-90.0,
        ),
    ]


@pytest.fixture
def sample_fire_detections():
    """Synthetic FIRMS fire detections."""
    from atlas_core.schemas.events import FIRMSDetection
    return [
        FIRMSDetection(
            detection_id="fire-001",
            satellite="VIIRS_SNPP",
            acq_datetime=datetime(2024, 8, 1, 14, 0, tzinfo=timezone.utc),
            lat=29.8,
            lon=-93.9,
            brightness_k=340.5,
            frp_mw=25.3,
            confidence="nominal",
        ),
        FIRMSDetection(
            detection_id="fire-002",
            satellite="VIIRS_SNPP",
            acq_datetime=datetime(2024, 8, 1, 15, 0, tzinfo=timezone.utc),
            lat=30.1,
            lon=-94.2,
            brightness_k=380.0,
            frp_mw=87.5,
            confidence="high",
        ),
    ]
