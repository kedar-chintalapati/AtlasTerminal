"""
Energy-domain Pydantic schemas.

Covers EIA data: storage, production, refinery utilisation, imports/exports,
power generation, and derived features.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class EIAFrequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ANNUAL = "annual"


class EIAFacet(BaseModel):
    """A single facet key-value pair from EIA API v2."""
    name: str
    description: str


class EIASeriesPoint(BaseModel):
    """Single data point in an EIA time series."""
    period: str                              # ISO date or partial ("2024-W12", "2024-03")
    value: Optional[float] = None
    unit: str = ""

    @field_validator("value", mode="before")
    @classmethod
    def coerce_null(cls, v: object) -> Optional[float]:
        if v is None or v == "":
            return None
        return float(v)  # type: ignore[arg-type]


class EIASeries(BaseModel):
    """Metadata + data for one EIA v2 series."""
    series_id: str
    name: str
    description: str = ""
    unit: str
    frequency: EIAFrequency
    facets: dict[str, str] = Field(default_factory=dict)
    data: list[EIASeriesPoint] = Field(default_factory=list)


# ─── Storage ─────────────────────────────────────────────────────────────────

class CrudeStorageRecord(BaseModel):
    """Weekly EIA crude oil storage report record."""
    report_date: date
    region: str                             # "PADD1" … "PADD5", "Cushing", "US"
    stocks_mmbbl: float                     # million barrels
    change_mmbbl: Optional[float] = None
    five_year_avg_mmbbl: Optional[float] = None
    five_year_max_mmbbl: Optional[float] = None
    five_year_min_mmbbl: Optional[float] = None
    source: str = "EIA"

    @property
    def pct_five_year(self) -> Optional[float]:
        if self.five_year_avg_mmbbl and self.five_year_avg_mmbbl != 0:
            return (self.stocks_mmbbl / self.five_year_avg_mmbbl - 1) * 100
        return None


class GasStorageRecord(BaseModel):
    """Weekly EIA natural-gas storage report record."""
    report_date: date
    region: str                             # "East", "West", "South Central", "Mountain", "US"
    stocks_bcf: float                       # billion cubic feet
    change_bcf: Optional[float] = None
    five_year_avg_bcf: Optional[float] = None
    five_year_max_bcf: Optional[float] = None
    five_year_min_bcf: Optional[float] = None
    source: str = "EIA"


# ─── Production ──────────────────────────────────────────────────────────────

class ProductionRecord(BaseModel):
    """Weekly crude production by PADD region."""
    report_date: date
    region: str
    commodity: str                          # "crude", "natgas", "ngpl"
    production: float                       # kbpd or MMcf/d
    unit: str
    source: str = "EIA"


# ─── Refinery ────────────────────────────────────────────────────────────────

class RefineryUtilizationRecord(BaseModel):
    """Weekly refinery runs and utilization by PADD."""
    report_date: date
    padd: str
    gross_input_kbpd: float
    capacity_kbpd: float
    utilization_pct: float
    source: str = "EIA"


# ─── Trade flows ─────────────────────────────────────────────────────────────

class TradeFlowRecord(BaseModel):
    """Weekly crude/product import or export record."""
    report_date: date
    direction: str                          # "import" | "export"
    origin_or_dest: str
    commodity: str
    volume_kbpd: float
    source: str = "EIA"


# ─── Power ───────────────────────────────────────────────────────────────────

class PowerGenerationRecord(BaseModel):
    """Monthly electricity generation by fuel type and region."""
    report_date: date
    region: str
    fuel_type: str                          # "natural_gas", "coal", "nuclear", "wind", etc.
    generation_gwh: float
    source: str = "EIA"


# ─── Derived features ────────────────────────────────────────────────────────

class StorageSurprise(BaseModel):
    """Computed storage-surprise signal for one period."""
    report_date: date
    commodity: str                          # "crude" | "natgas"
    region: str
    actual_change: float
    consensus_change: Optional[float] = None    # from historical seasonal avg
    surprise: float                         # actual − consensus (or seasonal avg)
    z_score: float
    five_year_pct_dev: Optional[float] = None
    signal_direction: str                   # "bullish" | "bearish" | "neutral"
    confidence: float = Field(ge=0.0, le=1.0)
