"""
EIA Open Data API v2 connector.

Documentation : https://www.eia.gov/opendata/documentation.php
API key       : https://www.eia.gov/opendata/register.php  (free, instant)

Key design decisions
--------------------
* All route paths come from the EIA v2 path tree. The connector exposes
  typed helpers for the most-used endpoints (storage, production, refinery)
  and a generic ``get_series`` for everything else.
* EIA returns rows in reverse-chronological order; we always sort ascending
  before returning.
* EIA paginates at 5 000 rows; ``get_series`` handles multi-page fetch
  automatically.
* Raw JSON is cached to disk so repeated calls during a session are fast.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional

import pandas as pd
from loguru import logger

from atlas_core.config import settings
from atlas_core.connectors.base import BaseConnector
from atlas_core.exceptions import ConnectorNotConfiguredError, ConnectorParseError
from atlas_core.schemas.energy import (
    CrudeStorageRecord,
    EIAFrequency,
    EIASeries,
    EIASeriesPoint,
    GasStorageRecord,
    PowerGenerationRecord,
    ProductionRecord,
    RefineryUtilizationRecord,
)

_BASE = "https://api.eia.gov/v2"
_PAGE_SIZE = 5_000


class EIAConnector(BaseConnector):
    """Connector for EIA Open Data API v2."""

    source_name = "EIA"
    _rate_limit_rps = 1.0      # EIA asks for reasonable use; 1 req/s is safe
    _rate_limit_burst = 3.0

    def __init__(self, api_key: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._api_key = api_key or settings.eia_api_key
        if not self._api_key:
            raise ConnectorNotConfiguredError(
                "EIA API key not configured.  Set EIA_API_KEY env var or pass api_key=",
                source=self.source_name,
            )

    # ------------------------------------------------------------------ #
    # Core fetch                                                           #
    # ------------------------------------------------------------------ #

    async def _fetch_route(
        self,
        route: str,
        facets: Optional[dict[str, list[str]]] = None,
        frequency: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        length: int = _PAGE_SIZE,
        offset: int = 0,
        data_cols: Optional[list[str]] = None,
        sort: Optional[list[dict[str, str]]] = None,
    ) -> dict[str, Any]:
        """Low-level fetch of one page from a v2 route."""
        params: dict[str, Any] = {
            "api_key": self._api_key,
            "length": length,
            "offset": offset,
        }
        if frequency:
            params["frequency"] = frequency
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if data_cols:
            for col in data_cols:
                params[f"data[]"] = col  # type: ignore  # EIA uses repeated params
            # httpx handles list values correctly
            params["data[]"] = data_cols
        if facets:
            for k, vals in facets.items():
                params[f"facets[{k}][]"] = vals
        if sort:
            for i, s in enumerate(sort):
                params[f"sort[{i}][column]"] = s["column"]
                params[f"sort[{i}][direction]"] = s.get("direction", "asc")

        url = f"{_BASE}/{route.strip('/')}/data"
        return await self._get(url, params)  # type: ignore[return-value]

    async def get_series(
        self,
        route: str,
        facets: Optional[dict[str, list[str]]] = None,
        frequency: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        data_cols: Optional[list[str]] = None,
        max_rows: int = 50_000,
    ) -> pd.DataFrame:
        """
        Fetch all pages for a route and return a flat DataFrame.

        Parameters
        ----------
        route     : EIA v2 path, e.g. "petroleum/sum/sndw"
        facets    : e.g. {"area": ["SAK"]}
        frequency : "daily" | "weekly" | "monthly" | "annual"
        start/end : ISO date strings
        data_cols : columns to request (default: ["value"])
        max_rows  : safety cap on total rows fetched
        """
        data_cols = data_cols or ["value"]
        rows: list[dict[str, Any]] = []
        offset = 0

        while offset < max_rows:
            chunk = await self._fetch_route(
                route,
                facets=facets,
                frequency=frequency,
                start=start,
                end=end,
                length=min(_PAGE_SIZE, max_rows - offset),
                offset=offset,
                data_cols=data_cols,
            )
            response = chunk.get("response", {})
            page_data = response.get("data", [])
            if not page_data:
                break
            rows.extend(page_data)
            total = int(response.get("total", 0))
            offset += len(page_data)
            if offset >= total:
                break
            logger.debug(f"[EIA] fetched {offset}/{total} rows for {route!r}")

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        if "period" in df.columns:
            df = df.sort_values("period")
        return df

    # ------------------------------------------------------------------ #
    # Typed helpers                                                         #
    # ------------------------------------------------------------------ #

    async def get_crude_storage(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> list[CrudeStorageRecord]:
        """
        Weekly crude oil stocks by PADD region + Cushing.

        EIA route: petroleum/sum/sndw
        """
        df = await self.get_series(
            route="petroleum/sum/sndw",
            frequency="weekly",
            start=start or _weeks_ago(104),  # 2 years default
            end=end,
            data_cols=["value"],
            facets={"product": ["EPC0"]},  # Total crude oil
        )
        return _parse_crude_storage(df)

    async def get_gas_storage(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> list[GasStorageRecord]:
        """
        Weekly natural gas storage by region.

        EIA route: natural-gas/sum/lsum
        """
        df = await self.get_series(
            route="natural-gas/sum/lsum",
            frequency="weekly",
            start=start or _weeks_ago(104),
            end=end,
            data_cols=["value"],
        )
        return _parse_gas_storage(df)

    async def get_crude_production(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> list[ProductionRecord]:
        """Weekly US field production of crude oil."""
        df = await self.get_series(
            route="petroleum/sum/sndw",
            frequency="weekly",
            start=start or _weeks_ago(104),
            end=end,
            facets={"product": ["EPC0"], "process": ["FPF"]},
            data_cols=["value"],
        )
        return _parse_production(df, commodity="crude", unit="kbpd")

    async def get_refinery_utilization(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> list[RefineryUtilizationRecord]:
        """Weekly refinery gross inputs and utilization rates by PADD."""
        df = await self.get_series(
            route="petroleum/pnp/wiup",
            frequency="weekly",
            start=start or _weeks_ago(104),
            end=end,
            data_cols=["value"],
        )
        return _parse_refinery(df)

    async def get_power_generation(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
        fuel_types: Optional[list[str]] = None,
    ) -> list[PowerGenerationRecord]:
        """Monthly electricity generation by fuel type."""
        facets: dict[str, list[str]] = {}
        if fuel_types:
            facets["fuelTypeDescription"] = fuel_types
        df = await self.get_series(
            route="electricity/electric-power-operational-data",
            frequency="monthly",
            start=start,
            end=end,
            facets=facets,
            data_cols=["generation"],
        )
        return _parse_power(df)

    async def health_check(self) -> bool:
        try:
            result = await self._get(
                f"{_BASE}/petroleum",
                {"api_key": self._api_key},
            )
            return isinstance(result, dict) and "response" in result
        except Exception as exc:
            logger.warning(f"[EIA] health check failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Parse helpers (pure functions — easy to unit-test in isolation)
# ─────────────────────────────────────────────────────────────────────────────

def _weeks_ago(n: int) -> str:
    return (date.today() - timedelta(weeks=n)).isoformat()


def _parse_crude_storage(df: pd.DataFrame) -> list[CrudeStorageRecord]:
    records = []
    if df.empty:
        return records
    # EIA sndw weekly data has columns: period, area, product, process, value, ...
    region_map = {
        "SAK": "PADD1", "MAK": "PADD2", "GAK": "PADD3",
        "RAK": "PADD4", "PAK": "PADD5", "NUS": "US",
        "OK": "Cushing",
    }
    for _, row in df.iterrows():
        area_code = str(row.get("area", "NUS"))
        region = region_map.get(area_code, area_code)
        try:
            val = row.get("value")
            stocks = 0.0 if (val is None or pd.isna(val)) else float(val)
            records.append(
                CrudeStorageRecord(
                    report_date=str(row.get("period", ""))[:10],
                    region=region,
                    stocks_mmbbl=stocks,
                )
            )
        except Exception as exc:
            logger.debug(f"[EIA] skip crude row: {exc}")
    return records


def _parse_gas_storage(df: pd.DataFrame) -> list[GasStorageRecord]:
    records = []
    if df.empty:
        return records
    region_map = {
        "NUS": "US", "R10": "East", "R30": "West",
        "R20": "South Central", "R40": "Mountain", "R50": "Pacific",
    }
    for _, row in df.iterrows():
        area = str(row.get("area", row.get("duoarea", "NUS")))
        region = region_map.get(area, area)
        try:
            records.append(
                GasStorageRecord(
                    report_date=str(row.get("period", ""))[:10],
                    region=region,
                    stocks_bcf=float(row.get("value", 0) or 0),
                )
            )
        except Exception as exc:
            logger.debug(f"[EIA] skip gas row: {exc}")
    return records


def _parse_production(
    df: pd.DataFrame, commodity: str, unit: str
) -> list[ProductionRecord]:
    records = []
    for _, row in df.iterrows():
        try:
            records.append(
                ProductionRecord(
                    report_date=str(row.get("period", ""))[:10],
                    region=str(row.get("area", row.get("duoarea", "US"))),
                    commodity=commodity,
                    production=float(row.get("value", 0) or 0),
                    unit=unit,
                )
            )
        except Exception as exc:
            logger.debug(f"[EIA] skip prod row: {exc}")
    return records


def _parse_refinery(df: pd.DataFrame) -> list[RefineryUtilizationRecord]:
    records = []
    for _, row in df.iterrows():
        try:
            gross = float(row.get("value", 0) or 0)
            capacity = float(row.get("capacity", gross * 1.1) or gross * 1.1)
            util = (gross / capacity * 100) if capacity else 0.0
            records.append(
                RefineryUtilizationRecord(
                    report_date=str(row.get("period", ""))[:10],
                    padd=str(row.get("area", row.get("duoarea", "US"))),
                    gross_input_kbpd=gross,
                    capacity_kbpd=capacity,
                    utilization_pct=util,
                )
            )
        except Exception as exc:
            logger.debug(f"[EIA] skip refinery row: {exc}")
    return records


def _parse_power(df: pd.DataFrame) -> list[PowerGenerationRecord]:
    records = []
    for _, row in df.iterrows():
        try:
            records.append(
                PowerGenerationRecord(
                    report_date=str(row.get("period", ""))[:7] + "-01",
                    region=str(row.get("location", "US")),
                    fuel_type=str(row.get("fuelTypeDescription", "other")).lower().replace(" ", "_"),
                    generation_gwh=float(row.get("generation", 0) or 0),
                )
            )
        except Exception as exc:
            logger.debug(f"[EIA] skip power row: {exc}")
    return records
