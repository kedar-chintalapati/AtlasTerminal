# Atlas Terminal

**Physical Commodities Intelligence Platform**

> Built Atlas Terminal: a geospatial physical-commodities intelligence platform combining EIA energy fundamentals, NOAA/NWS weather, NASA FIRMS satellite fire detections, AIS/aviation movement data, and GDELT global news into an interactive analytics and alerting system with Python/SQL APIs, event studies, and real-time risk signals.

---

## What It Is

Atlas Terminal is a decision system, not just a dashboard. You open it every day to answer questions like:

- **"Will Gulf Coast refinery margins get hit by weather?"**
- **"Are Cushing or Henry Hub conditions tightening?"**
- **"Do wildfire or heat events threaten production, power loads, or pipelines?"**
- **"Are tankers bunching at key LNG export terminals?"**
- **"Is satellite or news flow confirming a real physical disruption before futures fully price it?"**

Everything is derived from **free, public data sources**.

---

## Architecture

```
Atlas_Terminal/
├── atlas_core/          # Python intelligence library (importable)
│   ├── connectors/      # 9 data-source connectors (EIA, NWS, NOAA CDO,
│   │                    #   NDBC, SWPC, NASA FIRMS, GDELT, OpenSky, AIS)
│   ├── schemas/         # Pydantic models for all domains
│   ├── store/           # DuckDB analytical store + Parquet cache
│   ├── features/        # Feature engineering (energy, weather, spatial)
│   ├── signals/         # 6 signal definitions + composite
│   ├── alerts/          # Alert engine + rule definitions
│   ├── research/        # Event study, backtest, factor model
│   └── utils/           # Geo, time, math utilities
│
├── atlas_app/
│   ├── backend/         # FastAPI server
│   │   ├── routers/     # 7 domain routers + query + research
│   │   └── services/    # APScheduler background data refresh
│   └── frontend/        # React + TypeScript + deck.gl
│       └── src/
│           ├── components/map/       # deck.gl / MapLibre map
│           ├── components/analytics/ # Recharts analytics workspace
│           ├── components/feed/      # Event tape / OSINT feed
│           ├── components/notebook/  # SQL research console
│           └── components/drilldown/ # Causal drilldown panel
│
└── tests/               # Comprehensive test suite (~50 tests)
```

---

## Four Panels

### 1. Map (deck.gl + MapLibre)
GPU-accelerated layers:
- **Assets**: LNG terminals, gas hubs (ScatterplotLayer + labels)
- **Fires**: NASA FIRMS heatmap + scatter, configurable day window
- **Vessels**: AIS tanker/LNG carrier positions, colour-coded by type
- **Alerts**: Atlas alert points with domain colour coding
- **Weather**: NWS alert zones (planned overlay)

### 2. Analytics Workspace
- Storage surprise z-score bar chart (52-week rolling)
- Geomagnetic K-index timeline (NOAA SWPC)
- Composite risk signal with component decomposition
- Switchable tabs: Storage | Weather | Composite

### 3. Event Tape / OSINT Feed
- Scored, time-ordered alert stream
- Filterable by domain and minimum severity
- Domain colour coding (energy/weather/fire/shipping/geopolitics)
- Score progress bars

### 4. Research Notebook
- SQL console querying the live DuckDB store
- Sortable, copyable results table
- Built-in example queries
- Ctrl+Enter to execute

---

## Signature Feature: Cross-Domain Causal Drilldown

Click any risk alert and the app decomposes it into:

| Component | Source | Weight |
|-----------|--------|--------|
| Storage surprise (bearish) | EIA weekly | 35% |
| Weather risk | NWS alerts | 25% |
| Fire / infrastructure exposure | NASA FIRMS | 15% |
| Export terminal congestion | AIS vessel positions | 15% |
| News-flow acceleration | GDELT | 10% |

Each component shows its value, direction, historical hit rate, and links to the underlying raw data table.

---

## Data Sources (All Free)

| Source | What | Auth |
|--------|------|------|
| [EIA Open Data](https://www.eia.gov/opendata/) | Crude/gas storage, production, refinery utilisation, power | API key (free) |
| [NWS api.weather.gov](https://www.weather.gov/documentation/services-web-api) | Active alerts, gridpoint forecasts | None |
| [NOAA CDO](https://www.ncdc.noaa.gov/cdo-web/webservices/v2) | Historical climate (TMAX, TMIN, PRCP, HDD/CDD) | Token (free) |
| [NOAA NDBC](https://www.ndbc.noaa.gov/) | Buoy observations (Gulf of Mexico) | None |
| [NOAA SWPC](https://www.swpc.noaa.gov/) | K-index, space weather alerts, solar wind | None |
| [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/) | MODIS + VIIRS fire detections | MAP_KEY (free) |
| [GDELT DOC 2.0](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) | Energy news, sentiment, volume | None |
| [OpenSky](https://opensky-network.org/) | Live aircraft positions | Optional |
| [MarineCadastre / AIS](https://marinecadastre.gov/) | Vessel positions + historical AIS | None |

---

## Quick Start

### Prerequisites
- Python ≥ 3.11
- Node.js ≥ 18
- Git

### Backend
```bash
cd Atlas_Terminal

# Install Python dependencies
pip install -e ".[server,dev]"

# Copy and fill environment variables
cp .env.example .env
# → Edit .env with your API keys (all optional)

# Start the API server
python -m atlas_app.backend.main
# → Listening on http://localhost:8000
```

### Frontend
```bash
cd atlas_app/frontend
npm install
npm run dev
# → http://localhost:5173
```

### Using atlas_core as a Library
```python
from atlas_core.connectors import EIAConnector, NWSConnector
from atlas_core.signals import CompositeRiskSignal, StorageSurpriseSignal
from atlas_core.research import run_event_study, run_backtest, run_factor_model
from atlas_core.store import get_store
import asyncio

# Example: fetch and analyse crude storage
async def main():
    async with EIAConnector() as eia:
        storage = await eia.get_crude_storage()

    store = get_store()
    store.upsert_dataframe("crude_storage", pd.DataFrame([r.model_dump() for r in storage]))

    sig = StorageSurpriseSignal(commodity="crude", region="US", store=store)
    result = sig.latest()
    print(f"Signal: {result.direction} (z={result.metadata['z_score']:.2f})")

asyncio.run(main())
```

### Running Tests
```bash
pip install -e ".[dev]"
pytest tests/ -v
```

---

## Signal Definitions

### Storage Surprise (`StorageSurpriseSignal`)
Weekly EIA crude/gas storage change vs. seasonal average (52-week window).
- **Value**: z-score ÷ 3, clamped to [−1, 1]
- **Bullish**: drew more than seasonal average → tighter supply
- **Bearish**: built more than seasonal average → looser supply

### Weather Risk (`WeatherRiskSignal`)
NWS alert exposure for tracked energy assets, weighted by severity and proximity.
- **Score**: 0–1, distance-decayed alert severity sum

### Fire Exposure (`FireExposureSignal`)
NASA FIRMS detections within configurable radius of energy assets.
- **Score**: fire count × FRP intensity × proximity decay

### Export Terminal Congestion (`CongestionSignal`)
Fraction of vessels at anchor near LNG export terminals from AIS.

### News Flow (`NewsFlowSignal`)
GDELT negative-tone EWMA z-score for energy topics.

### Composite Risk (`CompositeRiskSignal`)
Confidence-weighted blend of all five signals above.

---

## Tech Stack

**Backend**: Python 3.11, FastAPI, DuckDB, APScheduler, httpx, tenacity, Pydantic v2, loguru

**Frontend**: React 18, TypeScript 5, Vite, Tailwind CSS, deck.gl v9, MapLibre GL, Recharts, Zustand, TanStack Query, react-resizable-panels

**Data**: DuckDB + Apache Parquet (analytical store), in-memory cache + disk cache

---

## License

MIT
