"""Atlas core schemas — re-export for convenience."""

from atlas_core.schemas.energy import (
    CrudeStorageRecord,
    EIAFrequency,
    EIASeries,
    EIASeriesPoint,
    GasStorageRecord,
    PowerGenerationRecord,
    ProductionRecord,
    RefineryUtilizationRecord,
    StorageSurprise,
    TradeFlowRecord,
)
from atlas_core.schemas.events import (
    AtlasAlert,
    EventDomain,
    FIRMSDetection,
    GDELTEvent,
    SignalComponent,
)
from atlas_core.schemas.geo import (
    AssetType,
    GAS_HUBS,
    LNG_TERMINALS,
    PADD_BBOXES,
    PADDRegion,
    PhysicalAsset,
)
from atlas_core.schemas.market import (
    BasisRecord,
    Commodity,
    OHLCVBar,
    SignalReturn,
    SpreadRecord,
)
from atlas_core.schemas.vessels import (
    AircraftState,
    FlightDensityGrid,
    NavigationStatus,
    PortCongestion,
    VesselPosition,
    VesselTrack,
    VesselType,
)
from atlas_core.schemas.weather import (
    AlertCertainty,
    AlertSeverity,
    BuoyObservation,
    ClimateRecord,
    ClimateStation,
    GeomagneticKIndex,
    GridpointForecast,
    HDDCDDRecord,
    NWSAlert,
    SpaceWeatherAlert,
    WeatherRiskScore,
)

__all__ = [
    # energy
    "CrudeStorageRecord", "EIAFrequency", "EIASeries", "EIASeriesPoint",
    "GasStorageRecord", "PowerGenerationRecord", "ProductionRecord",
    "RefineryUtilizationRecord", "StorageSurprise", "TradeFlowRecord",
    # events
    "AtlasAlert", "EventDomain", "FIRMSDetection", "GDELTEvent", "SignalComponent",
    # geo
    "AssetType", "GAS_HUBS", "LNG_TERMINALS", "PADD_BBOXES", "PADDRegion", "PhysicalAsset",
    # market
    "BasisRecord", "Commodity", "OHLCVBar", "SignalReturn", "SpreadRecord",
    # vessels
    "AircraftState", "FlightDensityGrid", "NavigationStatus", "PortCongestion",
    "VesselPosition", "VesselTrack", "VesselType",
    # weather
    "AlertCertainty", "AlertSeverity", "BuoyObservation", "ClimateRecord",
    "ClimateStation", "GeomagneticKIndex", "GridpointForecast", "HDDCDDRecord",
    "NWSAlert", "SpaceWeatherAlert", "WeatherRiskScore",
]
