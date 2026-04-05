"""
Atlas Terminal — Parquet cache layer.

Provides a simple key-value store backed by Parquet files.  Use this when
you want the analytical store to be the DuckDB file but also want raw
connector responses cached on disk for offline / fast-reload scenarios.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from atlas_core.config import settings


class ParquetCache:
    """
    Parquet-backed data cache.

    Each entry is stored as ``{namespace}/{key}.parquet``.
    Metadata (ingestion timestamp) is embedded in Parquet schema metadata.
    """

    def __init__(self, base_dir: Optional[Path] = None) -> None:
        self._base = base_dir or settings.parquet_dir

    def _path(self, namespace: str, key: str) -> Path:
        d = self._base / namespace
        d.mkdir(parents=True, exist_ok=True)
        safe_key = key.replace("/", "__").replace("?", "_").replace("&", "_")
        return d / f"{safe_key}.parquet"

    def put(self, namespace: str, key: str, df: pd.DataFrame) -> Path:
        """Write ``df`` to cache, overwriting any existing entry."""
        path = self._path(namespace, key)
        table = pa.Table.from_pandas(df, preserve_index=False)
        # Embed write timestamp in schema metadata
        meta = {b"atlas_ingested_at": str(time.time()).encode()}
        table = table.replace_schema_metadata({**table.schema.metadata, **meta})
        pq.write_table(table, path, compression="snappy")
        logger.debug(f"ParquetCache.put: {path} ({len(df)} rows)")
        return path

    def get(
        self,
        namespace: str,
        key: str,
        max_age_seconds: Optional[float] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve cached DataFrame, or ``None`` if not found / expired.

        Parameters
        ----------
        max_age_seconds : if set, entries older than this are treated as missing.
        """
        path = self._path(namespace, key)
        if not path.exists():
            return None
        try:
            table = pq.read_table(path)
            if max_age_seconds is not None:
                raw_meta = table.schema.metadata or {}
                ts_bytes = raw_meta.get(b"atlas_ingested_at")
                if ts_bytes:
                    age = time.time() - float(ts_bytes.decode())
                    if age > max_age_seconds:
                        logger.debug(f"ParquetCache: stale ({age:.0f}s) — {path}")
                        return None
            return table.to_pandas()
        except Exception as exc:
            logger.warning(f"ParquetCache read error {path}: {exc}")
            return None

    def exists(self, namespace: str, key: str, max_age_seconds: Optional[float] = None) -> bool:
        return self.get(namespace, key, max_age_seconds) is not None

    def invalidate(self, namespace: str, key: str) -> None:
        self._path(namespace, key).unlink(missing_ok=True)

    def list_keys(self, namespace: str) -> list[str]:
        d = self._base / namespace
        if not d.exists():
            return []
        return [p.stem for p in d.glob("*.parquet")]


# Module-level default instance
cache = ParquetCache()
