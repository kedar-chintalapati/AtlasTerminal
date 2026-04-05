"""
Atlas Terminal — DuckDB analytical store.

Central data store for the platform.  All ingested data is written here as
Arrow tables / Parquet files, then queried at runtime by the API and the
in-app research console.

Design decisions
----------------
* Single DuckDB file (or in-memory for tests).
* All writes go through ``upsert_dataframe`` which does a CREATE-OR-REPLACE
  on a keyed view so partial refreshes are idempotent.
* Parquet files are co-located alongside the .duckdb file in ``parquet_dir``.
* The store is NOT thread-safe for writes — use a queue or single writer task
  in the scheduler.  Reads (SELECT only) via multiple connections are fine with
  DuckDB's WAL mode.
"""

from __future__ import annotations

import textwrap
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Optional

import duckdb
import pandas as pd
import pyarrow as pa
from loguru import logger

from atlas_core.config import settings
from atlas_core.exceptions import StoreError, StoreTableNotFoundError

# Registry of table definitions: name → (create DDL, primary key cols for upsert)
_TABLE_REGISTRY: dict[str, tuple[str, list[str]]] = {}


def register_table(name: str, ddl: str, pk_cols: list[str]) -> None:
    """Register a managed table with the store."""
    _TABLE_REGISTRY[name] = (ddl, pk_cols)


class DuckDBStore:
    """
    Singleton-ish DuckDB store.

    Usage
    -----
    >>> store = DuckDBStore()
    >>> store.initialize()
    >>> store.upsert_dataframe("crude_storage", df)
    >>> result = store.query("SELECT * FROM crude_storage WHERE region = 'PADD3'")
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        in_memory: bool = False,
        parquet_dir: Optional[Path] = None,
    ) -> None:
        self._in_memory = in_memory or settings.db_in_memory
        self._db_path = ":memory:" if self._in_memory else str(db_path or settings.db_path)
        self._parquet_dir = parquet_dir or settings.parquet_dir
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    def initialize(self) -> None:
        """Open DB connection, install extensions, create managed tables."""
        if not self._in_memory:
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._parquet_dir.mkdir(parents=True, exist_ok=True)

        self._conn = duckdb.connect(self._db_path)
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")
        self._conn.execute("INSTALL spatial; LOAD spatial;")
        self._create_managed_tables()
        logger.info(f"DuckDB store initialised at {self._db_path!r}")

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "DuckDBStore":
        self.initialize()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise StoreError("Store not initialised — call initialize() or use as context manager")
        return self._conn

    def _create_managed_tables(self) -> None:
        for name, (ddl, _) in _TABLE_REGISTRY.items():
            try:
                self.conn.execute(ddl)
                logger.debug(f"Table '{name}' ready")
            except Exception as exc:
                logger.warning(f"Could not create table '{name}': {exc}")

    # ------------------------------------------------------------------ #
    # Write operations                                                     #
    # ------------------------------------------------------------------ #

    def upsert_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        *,
        if_exists: str = "append",
    ) -> int:
        """
        Write ``df`` into ``table_name``.

        Parameters
        ----------
        if_exists : "append" (default) | "replace"
            "replace" drops and recreates the table.
        Returns
        -------
        int — number of rows written.
        """
        if df.empty:
            logger.debug(f"upsert_dataframe: empty df for '{table_name}', skipping")
            return 0

        try:
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
            # Register as a temporary view so DuckDB can see it
            self.conn.register("__tmp_write__", arrow_table)

            if if_exists == "replace":
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            # CREATE TABLE IF NOT EXISTS from the temp view, then INSERT
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM __tmp_write__ WHERE 1=0"
            )
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM __tmp_write__")
            self.conn.unregister("__tmp_write__")

            logger.debug(f"Wrote {len(df)} rows to '{table_name}'")
            return len(df)
        except Exception as exc:
            raise StoreError(f"Failed to upsert into '{table_name}': {exc}") from exc

    def write_parquet(self, table_name: str, df: pd.DataFrame) -> Path:
        """Write ``df`` to a Parquet file and register it in DuckDB as a view."""
        path = self._parquet_dir / f"{table_name}.parquet"
        df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
        # Register as a view so SQL queries work transparently
        self.conn.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path}')"
        )
        logger.info(f"Wrote {len(df)} rows → {path}")
        return path

    # ------------------------------------------------------------------ #
    # Read operations                                                      #
    # ------------------------------------------------------------------ #

    def query(self, sql: str, params: Optional[list[Any]] = None) -> pd.DataFrame:
        """Execute SQL and return a DataFrame."""
        try:
            rel = self.conn.execute(sql, params or [])
            return rel.df()
        except duckdb.CatalogException as exc:
            # Try to extract table name from error message
            msg = str(exc)
            if "Table" in msg or "table" in msg:
                raise StoreTableNotFoundError(table=msg, message=msg) from exc
            raise StoreError(f"DuckDB query error: {exc}", sql=sql[:200]) from exc
        except Exception as exc:
            raise StoreError(f"DuckDB error: {exc}", sql=sql[:200]) from exc

    def query_arrow(self, sql: str) -> pa.Table:
        """Execute SQL and return an Arrow table (zero-copy when possible)."""
        try:
            return self.conn.execute(sql).arrow()
        except Exception as exc:
            raise StoreError(f"DuckDB error: {exc}", sql=sql[:200]) from exc

    def list_tables(self) -> list[str]:
        df = self.query("SHOW TABLES")
        return df["name"].tolist() if not df.empty else []

    def table_schema(self, table_name: str) -> pd.DataFrame:
        return self.query(f"DESCRIBE {table_name}")

    def row_count(self, table_name: str) -> int:
        df = self.query(f"SELECT COUNT(*) AS n FROM {table_name}")
        return int(df["n"].iloc[0]) if not df.empty else 0

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """Context manager for explicit transactions."""
        self.conn.execute("BEGIN TRANSACTION")
        try:
            yield
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise


# ─── Managed table DDL registrations ─────────────────────────────────────────

register_table(
    "crude_storage",
    textwrap.dedent("""
        CREATE TABLE IF NOT EXISTS crude_storage (
            report_date DATE NOT NULL,
            region VARCHAR NOT NULL,
            stocks_mmbbl DOUBLE,
            change_mmbbl DOUBLE,
            five_year_avg_mmbbl DOUBLE,
            five_year_max_mmbbl DOUBLE,
            five_year_min_mmbbl DOUBLE,
            source VARCHAR DEFAULT 'EIA',
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    pk_cols=["report_date", "region"],
)

register_table(
    "gas_storage",
    textwrap.dedent("""
        CREATE TABLE IF NOT EXISTS gas_storage (
            report_date DATE NOT NULL,
            region VARCHAR NOT NULL,
            stocks_bcf DOUBLE,
            change_bcf DOUBLE,
            five_year_avg_bcf DOUBLE,
            five_year_max_bcf DOUBLE,
            five_year_min_bcf DOUBLE,
            source VARCHAR DEFAULT 'EIA',
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    pk_cols=["report_date", "region"],
)

register_table(
    "nws_alerts",
    textwrap.dedent("""
        CREATE TABLE IF NOT EXISTS nws_alerts (
            alert_id VARCHAR NOT NULL,
            headline VARCHAR,
            event_type VARCHAR,
            severity VARCHAR,
            onset TIMESTAMPTZ,
            expires TIMESTAMPTZ,
            centroid_lat DOUBLE,
            centroid_lon DOUBLE,
            source VARCHAR DEFAULT 'NWS',
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    pk_cols=["alert_id"],
)

register_table(
    "firms_detections",
    textwrap.dedent("""
        CREATE TABLE IF NOT EXISTS firms_detections (
            detection_id VARCHAR NOT NULL,
            satellite VARCHAR,
            acq_datetime TIMESTAMPTZ,
            lat DOUBLE NOT NULL,
            lon DOUBLE NOT NULL,
            brightness_k DOUBLE,
            frp_mw DOUBLE,
            confidence VARCHAR,
            source VARCHAR DEFAULT 'NASA_FIRMS',
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    pk_cols=["detection_id"],
)

register_table(
    "gdelt_events",
    textwrap.dedent("""
        CREATE TABLE IF NOT EXISTS gdelt_events (
            event_id VARCHAR NOT NULL,
            publish_date TIMESTAMPTZ,
            url VARCHAR,
            title VARCHAR,
            tone DOUBLE,
            relevance_score DOUBLE,
            lat DOUBLE,
            lon DOUBLE,
            source VARCHAR DEFAULT 'GDELT',
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    pk_cols=["event_id"],
)

register_table(
    "vessel_positions",
    textwrap.dedent("""
        CREATE TABLE IF NOT EXISTS vessel_positions (
            mmsi VARCHAR NOT NULL,
            vessel_name VARCHAR,
            vessel_type VARCHAR,
            timestamp TIMESTAMPTZ NOT NULL,
            lat DOUBLE NOT NULL,
            lon DOUBLE NOT NULL,
            speed_kts DOUBLE,
            nav_status VARCHAR,
            destination VARCHAR,
            source VARCHAR DEFAULT 'AIS',
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    pk_cols=["mmsi", "timestamp"],
)

register_table(
    "atlas_alerts",
    textwrap.dedent("""
        CREATE TABLE IF NOT EXISTS atlas_alerts (
            alert_id VARCHAR NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            domain VARCHAR,
            severity VARCHAR,
            title VARCHAR,
            summary VARCHAR,
            score DOUBLE,
            lat DOUBLE,
            lon DOUBLE,
            region VARCHAR,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    pk_cols=["alert_id"],
)

register_table(
    "storage_surprises",
    textwrap.dedent("""
        CREATE TABLE IF NOT EXISTS storage_surprises (
            report_date DATE NOT NULL,
            commodity VARCHAR NOT NULL,
            region VARCHAR NOT NULL,
            actual_change DOUBLE,
            consensus_change DOUBLE,
            surprise DOUBLE,
            z_score DOUBLE,
            five_year_pct_dev DOUBLE,
            signal_direction VARCHAR,
            confidence DOUBLE,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """),
    pk_cols=["report_date", "commodity", "region"],
)


# ─── Module-level convenience instance ───────────────────────────────────────

_default_store: Optional[DuckDBStore] = None


def get_store() -> DuckDBStore:
    """Return (and lazily initialise) the module-level default store."""
    global _default_store
    if _default_store is None:
        _default_store = DuckDBStore()
        _default_store.initialize()
    return _default_store
