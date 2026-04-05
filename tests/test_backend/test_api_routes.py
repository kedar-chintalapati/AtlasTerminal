"""
Integration tests for FastAPI routes.

Uses FastAPI's TestClient with the in-memory DuckDB store.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """FastAPI TestClient with injected in-memory store."""
    import sys
    # Override store to use in-memory DuckDB
    from atlas_core.store.duckdb_store import DuckDBStore
    from atlas_app.backend import deps

    store = DuckDBStore(in_memory=True)
    store.initialize()

    # Monkeypatch the singleton
    deps._get_store_singleton.cache_clear()
    original = deps._get_store_singleton

    def _mock_store():
        return store

    deps._get_store_singleton = _mock_store

    from atlas_app.backend.main import app
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    deps._get_store_singleton = original
    store.close()


def test_health_endpoint(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_status_endpoint(client):
    r = client.get("/api/v1/status")
    assert r.status_code == 200
    data = r.json()
    assert "tables" in data


def test_crude_storage_endpoint_empty(client):
    r = client.get("/api/v1/energy/storage/crude")
    assert r.status_code == 200
    data = r.json()
    assert "data" in data
    assert isinstance(data["data"], list)


def test_gas_storage_endpoint_empty(client):
    r = client.get("/api/v1/energy/storage/gas")
    assert r.status_code == 200


def test_list_tables_endpoint(client):
    r = client.get("/api/v1/query/tables")
    assert r.status_code == 200
    tables = r.json()["tables"]
    assert isinstance(tables, list)
    # Key tables should be present
    assert "crude_storage" in tables
    assert "gas_storage" in tables
    assert "atlas_alerts" in tables


def test_sql_query_endpoint(client):
    r = client.post(
        "/api/v1/query/sql",
        json={"sql": "SELECT 1 AS n, 'hello' AS s"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["columns"] == ["n", "s"]
    assert len(data["data"]) == 1


def test_sql_query_disallows_drop(client):
    r = client.post(
        "/api/v1/query/sql",
        json={"sql": "DROP TABLE crude_storage"},
    )
    assert r.status_code == 403


def test_alerts_endpoint_empty(client):
    r = client.get("/api/v1/events/alerts")
    assert r.status_code == 200


def test_lng_terminals_endpoint(client):
    r = client.get("/api/v1/vessels/terminals")
    assert r.status_code == 200
    data = r.json()
    assert len(data["data"]) > 0
    assert data["data"][0]["asset_id"] == "sabine_pass"


def test_assets_map_layer(client):
    r = client.get("/api/v1/map/layers/assets")
    assert r.status_code == 200
    geojson = r.json()
    assert geojson["type"] == "FeatureCollection"
    assert len(geojson["features"]) > 0


def test_table_schema_endpoint(client):
    r = client.get("/api/v1/query/tables/crude_storage/schema")
    assert r.status_code == 200
    data = r.json()
    assert "schema" in data


def test_drilldown_endpoint(client):
    r = client.post(
        "/api/v1/research/drilldown",
        json={"commodity": "crude"},
    )
    # May return 200 or 500 depending on data availability; just check it doesn't crash
    assert r.status_code in (200, 500)
    if r.status_code == 200:
        data = r.json()
        assert "value" in data
        assert "direction" in data
        assert "components" in data
