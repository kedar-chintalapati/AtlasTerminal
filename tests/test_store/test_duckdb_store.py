"""Tests for DuckDB store."""

from __future__ import annotations

import pandas as pd
import pytest

from atlas_core.exceptions import StoreError
from atlas_core.store.duckdb_store import DuckDBStore


def test_store_initializes(tmp_store):
    tables = tmp_store.list_tables()
    assert "crude_storage" in tables
    assert "gas_storage" in tables
    assert "nws_alerts" in tables
    assert "firms_detections" in tables


def test_upsert_and_query(tmp_store):
    df = pd.DataFrame({
        "report_date": ["2024-01-07", "2024-01-14"],
        "region": ["US", "US"],
        "stocks_mmbbl": [460.0, 462.5],
        "change_mmbbl": [-1.5, 2.5],
        "source": ["EIA", "EIA"],
    })
    rows = tmp_store.upsert_dataframe("crude_storage", df)
    assert rows == 2

    result = tmp_store.query("SELECT * FROM crude_storage")
    assert len(result) == 2


def test_query_returns_dataframe(tmp_store):
    result = tmp_store.query("SELECT 1 AS n")
    assert isinstance(result, pd.DataFrame)
    assert result["n"].iloc[0] == 1


def test_row_count(tmp_store):
    df = pd.DataFrame({"report_date": ["2024-01-07"], "region": ["US"],
                        "stocks_mmbbl": [460.0], "source": ["EIA"]})
    tmp_store.upsert_dataframe("crude_storage", df)
    count = tmp_store.row_count("crude_storage")
    assert count >= 1


def test_upsert_empty_dataframe(tmp_store):
    """Upserting empty DataFrame should return 0 without error."""
    rows = tmp_store.upsert_dataframe("crude_storage", pd.DataFrame())
    assert rows == 0


def test_store_raises_on_bad_sql(tmp_store):
    with pytest.raises(StoreError):
        tmp_store.query("SELECT * FROM nonexistent_table_xyz")


def test_parquet_write_creates_view(tmp_store, tmp_path):
    """write_parquet should create a queryable view."""
    from atlas_core.store.duckdb_store import DuckDBStore
    store = DuckDBStore(in_memory=True, parquet_dir=tmp_path)
    store.initialize()

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    store.write_parquet("test_view", df)
    result = store.query("SELECT * FROM test_view")
    assert len(result) == 3
    store.close()


def test_list_tables_includes_all_managed(tmp_store):
    tables = tmp_store.list_tables()
    for expected in ["crude_storage", "gas_storage", "nws_alerts",
                      "firms_detections", "gdelt_events", "vessel_positions",
                      "atlas_alerts", "storage_surprises"]:
        assert expected in tables, f"Missing table: {expected}"
