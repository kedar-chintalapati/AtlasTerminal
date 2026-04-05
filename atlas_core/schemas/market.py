"""
Market and price schemas.

Kept deliberately thin — the market connector is modular / pluggable.
Platform functions as a physical-intelligence layer and is agnostic to the
specific price feed; users can inject their own price data.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class PriceFrequency(str, Enum):
    TICK = "tick"
    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    HOURLY = "1h"
    DAILY = "1d"
    WEEKLY = "1w"
    MONTHLY = "1M"


class Commodity(str, Enum):
    WTI = "WTI"
    BRENT = "BRENT"
    NATGAS_HH = "HH"          # Henry Hub
    RBOB = "RBOB"
    HEATING_OIL = "HO"
    LNG_JKM = "JKM"           # Japan-Korea Marker
    POWER_ERCOT = "ERCOT_RT"
    POWER_PJM = "PJM_RT"
    POWER_CAISO = "CAISO_RT"


class OHLCVBar(BaseModel):
    """Standard OHLCV price bar."""
    symbol: str
    commodity: Optional[Commodity] = None
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    open_interest: Optional[float] = None
    frequency: PriceFrequency = PriceFrequency.DAILY
    source: str = "user"


class SpreadRecord(BaseModel):
    """Price spread between two instruments."""
    timestamp: datetime
    leg1_symbol: str
    leg2_symbol: str
    spread: float
    z_score: Optional[float] = None
    percentile_52w: Optional[float] = None


class BasisRecord(BaseModel):
    """Physical-to-futures basis at a delivery point."""
    trade_date: date
    delivery_location: str
    futures_symbol: str
    futures_price: float
    physical_price: Optional[float] = None
    basis: Optional[float] = None       # physical − futures
    source: str = "user"


class SignalReturn(BaseModel):
    """Forward return used in event studies and backtests."""
    date: date
    symbol: str
    horizon_days: int
    fwd_return: float
    fwd_return_pct: float
    signal_value: Optional[float] = None
    signal_name: Optional[str] = None
