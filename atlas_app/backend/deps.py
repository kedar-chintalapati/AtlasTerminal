"""FastAPI dependency injection helpers."""

from __future__ import annotations

from functools import lru_cache
from typing import Generator

from atlas_core.store.duckdb_store import DuckDBStore


@lru_cache(maxsize=1)
def _get_store_singleton() -> DuckDBStore:
    store = DuckDBStore()
    store.initialize()
    return store


def get_store() -> Generator[DuckDBStore, None, None]:
    """FastAPI dependency: yields the shared DuckDB store."""
    yield _get_store_singleton()
