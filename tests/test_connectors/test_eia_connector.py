"""Tests for EIA connector parsing logic (no live API calls)."""

from __future__ import annotations

import pytest

from atlas_core.connectors.eia import _parse_crude_storage, _parse_gas_storage, _parse_power
from atlas_core.exceptions import ConnectorNotConfiguredError
import pandas as pd


def test_parse_crude_storage_us():
    df = pd.DataFrame([
        {"period": "2024-06-07", "area": "NUS", "value": 460.5},
        {"period": "2024-06-14", "area": "NUS", "value": 462.1},
    ])
    records = _parse_crude_storage(df)
    assert len(records) == 2
    assert records[0].region == "US"
    assert records[0].stocks_mmbbl == pytest.approx(460.5)
    assert records[1].report_date.isoformat() == "2024-06-14"


def test_parse_crude_storage_cushing():
    df = pd.DataFrame([
        {"period": "2024-06-07", "area": "OK", "value": 32.5},
    ])
    records = _parse_crude_storage(df)
    assert records[0].region == "Cushing"


def test_parse_crude_storage_empty():
    records = _parse_crude_storage(pd.DataFrame())
    assert records == []


def test_parse_crude_storage_null_value():
    """Null/empty values should produce 0.0, not crash."""
    df = pd.DataFrame([
        {"period": "2024-06-07", "area": "NUS", "value": None},
    ])
    records = _parse_crude_storage(df)
    assert len(records) == 1
    assert records[0].stocks_mmbbl == 0.0


def test_parse_gas_storage_regions():
    df = pd.DataFrame([
        {"period": "2024-06-07", "area": "NUS", "value": 2100.0},
        {"period": "2024-06-07", "area": "R10", "value": 620.0},
        {"period": "2024-06-07", "area": "R20", "value": 810.0},
    ])
    records = _parse_gas_storage(df)
    regions = {r.region for r in records}
    assert "US" in regions
    assert "East" in regions
    assert "South Central" in regions


def test_eia_connector_raises_without_key():
    """EIAConnector should raise ConnectorNotConfiguredError if no key."""
    from atlas_core.connectors.eia import EIAConnector
    with pytest.raises(ConnectorNotConfiguredError):
        EIAConnector(api_key="")
