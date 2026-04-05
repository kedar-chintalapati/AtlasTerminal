"""
atlas_core — Physical Commodities Intelligence Library.

Quick-start
-----------
>>> from atlas_core.connectors import EIAConnector, NWSConnector
>>> from atlas_core.signals import CompositeRiskSignal, StorageSurpriseSignal
>>> from atlas_core.research import run_event_study, run_backtest
>>> from atlas_core.store import get_store

All public API surfaces are importable from these five top-level modules.
"""

__version__ = "0.1.0"

from atlas_core import (
    alerts,
    connectors,
    features,
    research,
    schemas,
    signals,
    store,
    utils,
)

__all__ = [
    "alerts", "connectors", "features", "research",
    "schemas", "signals", "store", "utils",
]
