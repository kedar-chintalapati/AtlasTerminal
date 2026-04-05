/**
 * Atlas Terminal API client.
 *
 * Thin typed wrapper around fetch.  Every function returns the parsed JSON
 * with a concrete TypeScript type.  Throws on non-2xx responses.
 */

const BASE = "/api/v1";

async function get<T>(path: string, params?: Record<string, string | number | boolean | undefined | null>): Promise<T> {
  const url = new URL(BASE + path, window.location.origin);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
    }
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json() as Promise<T>;
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json() as Promise<T>;
}

// ─── Energy ──────────────────────────────────────────────────────────────────

export const energyApi = {
  getCrudeStorage: (region?: string, limit = 100) =>
    get<{ data: StorageRecord[]; count: number }>("/energy/storage/crude", { region, limit }),
  getGasStorage: (region?: string, limit = 100) =>
    get<{ data: StorageRecord[]; count: number }>("/energy/storage/gas", { region, limit }),
  getStorageSurprise: (commodity = "crude", region = "US") =>
    get<SignalResponse>("/energy/storage/surprise", { commodity, region }),
  getSurpriseHistory: (commodity = "crude", region = "US", limit = 52) =>
    get<{ data: SurpriseRecord[]; count: number }>("/energy/storage/surprises/history", { commodity, region, limit }),
  refreshEnergy: () => post<{ status: string }>("/energy/refresh", {}),
};

// ─── Weather ─────────────────────────────────────────────────────────────────

export const weatherApi = {
  getActiveAlerts: (state?: string) =>
    get<{ data: NWSAlertRecord[]; count: number }>("/weather/alerts/active", { state }),
  getForecast: (lat: number, lon: number) =>
    get<GridpointForecast>("/weather/forecast", { lat, lon }),
  getBuoy: (stationId: string, maxRows = 48) =>
    get<{ data: BuoyRecord[]; count: number }>(`/weather/buoy/${stationId}`, { max_rows: maxRows }),
  getKIndex: () =>
    get<{ data: KIndexRecord[]; count: number }>("/weather/space/kindex"),
  getSpaceAlerts: () =>
    get<{ data: SpaceAlertRecord[]; count: number }>("/weather/space/alerts"),
  getWeatherRiskScore: () =>
    get<SignalResponse>("/weather/risk/score"),
};

// ─── Events ──────────────────────────────────────────────────────────────────

export const eventsApi = {
  getFiresGulfCoast: (days = 3) =>
    get<{ data: FireRecord[]; count: number }>("/events/fires/gulf-coast", { days }),
  getFiresBbox: (min_lat: number, min_lon: number, max_lat: number, max_lon: number, days = 2) =>
    get<{ data: FireRecord[]; count: number }>("/events/fires/bbox", { min_lat, min_lon, max_lat, max_lon, days }),
  getFireScore: (days = 3) =>
    get<SignalResponse>("/events/fires/score", { days }),
  searchNews: (query: string, timespan = "7d", max_records = 50) =>
    get<{ data: NewsRecord[]; count: number }>("/events/news/search", { query, timespan, max_records }),
  getNewsFeed: (topic: string, timespan = "24h") =>
    get<{ data: NewsRecord[]; count: number }>(`/events/news/feed/${topic}`, { timespan }),
  getNewsSignal: (topic = "natural_gas", timespan = "30d") =>
    get<SignalResponse>("/events/news/signal", { topic, timespan }),
  getAlerts: (domain?: string, severity?: string, limit = 50) =>
    get<{ data: AtlasAlertRecord[]; count: number }>("/events/alerts", { domain, severity, limit }),
};

// ─── Vessels ─────────────────────────────────────────────────────────────────

export const vesselsApi = {
  getAircraftStates: (min_lat = 25, min_lon = -100, max_lat = 33, max_lon = -88) =>
    get<{ data: AircraftRecord[]; count: number }>("/vessels/aircraft/states", { min_lat, min_lon, max_lat, max_lon }),
  getVesselPositions: (limit = 1000, vessel_type?: string) =>
    get<{ data: VesselRecord[]; count: number }>("/vessels/positions", { limit, vessel_type }),
  getCongestion: () =>
    get<SignalResponse>("/vessels/congestion"),
  getTerminals: () =>
    get<{ data: TerminalRecord[] }>("/vessels/terminals"),
};

// ─── Map layers ───────────────────────────────────────────────────────────────

export const mapApi = {
  getAssetsLayer: () => get<GeoJSON>("/map/layers/assets"),
  getFiresLayer: (days = 3) => get<GeoJSON>("/map/layers/fires", { days }),
  getVesselsLayer: (limit = 2000, vessel_type?: string) =>
    get<GeoJSON>("/map/layers/vessels", { limit, vessel_type }),
  getAlertsLayer: (severity?: string, domain?: string) =>
    get<GeoJSON>("/map/layers/alerts", { severity, domain }),
};

// ─── Query ────────────────────────────────────────────────────────────────────

export const queryApi = {
  runSql: (sql: string, params?: unknown[], limit = 10000, format: "records" | "columns" = "records") =>
    post<QueryResponse>("/query/sql", { sql, params: params || [], limit, format }),
  listTables: () => get<{ tables: string[] }>("/query/tables"),
  getSchema: (table: string) => get<{ schema: SchemaColumn[] }>(`/query/tables/${table}/schema`),
};

// ─── Research ─────────────────────────────────────────────────────────────────

export const researchApi = {
  runDrilldown: (commodity = "crude") =>
    post<DrilldownResponse>("/research/drilldown", { commodity }),
  runBacktest: (req: BacktestRequest) =>
    post<BacktestResponse>("/research/backtest", req),
};

// ─── Status ───────────────────────────────────────────────────────────────────

export const statusApi = {
  getStatus: () => get<{ status: string; tables: Record<string, number> }>("/status"),
};

// ─── Types ───────────────────────────────────────────────────────────────────

export interface StorageRecord {
  report_date: string;
  region: string;
  stocks_mmbbl?: number;
  stocks_bcf?: number;
  change_mmbbl?: number;
  change_bcf?: number;
  five_year_avg_mmbbl?: number;
  five_year_avg_bcf?: number;
}

export interface SurpriseRecord {
  report_date: string;
  commodity: string;
  region: string;
  actual_change: number;
  consensus_change: number;
  surprise: number;
  z_score: number;
  signal_direction: string;
  confidence: number;
}

export interface SignalResponse {
  value: number;
  direction: "bullish" | "bearish" | "neutral";
  confidence: number;
  metadata: Record<string, unknown>;
  components?: SignalComponent[];
  signal_name?: string;
}

export interface SignalComponent {
  name: string;
  description: string;
  value: number;
  direction: string;
  weight: number;
  historical_hit_rate?: number;
  data_table?: string;
  source_domain: string;
}

export interface NWSAlertRecord {
  alert_id: string;
  headline: string;
  event_type: string;
  severity: string;
  onset?: string;
  expires?: string;
  centroid_lat?: number;
  centroid_lon?: number;
}

export interface GridpointForecast {
  lat: number;
  lon: number;
  office: string;
  timezone: string;
  periods: ForecastPeriod[];
}

export interface ForecastPeriod {
  name: string;
  start_time: string;
  end_time: string;
  is_daytime: boolean;
  temperature_f: number;
  wind_speed: string;
  wind_direction: string;
  short_forecast: string;
}

export interface BuoyRecord {
  station_id: string;
  timestamp: string;
  wind_speed_ms?: number;
  wave_height_m?: number;
  sea_surface_temp_c?: number;
  air_temp_c?: number;
  air_pressure_hpa?: number;
}

export interface KIndexRecord {
  timestamp: string;
  k_index: number;
  station: string;
}

export interface SpaceAlertRecord {
  alert_id: string;
  issued_time: string;
  product: string;
  category: string;
  message: string;
}

export interface FireRecord {
  detection_id: string;
  satellite: string;
  acq_datetime: string;
  lat: number;
  lon: number;
  brightness_k: number;
  frp_mw?: number;
  confidence: string;
}

export interface NewsRecord {
  event_id: string;
  publish_date: string;
  url: string;
  title: string;
  tone: number;
  relevance_score: number;
  lat?: number;
  lon?: number;
  source_country?: string;
}

export interface AtlasAlertRecord {
  alert_id: string;
  created_at: string;
  domain: string;
  severity: string;
  title: string;
  summary: string;
  score: number;
  lat?: number;
  lon?: number;
  region?: string;
}

export interface AircraftRecord {
  icao24: string;
  callsign: string;
  origin_country: string;
  timestamp: string;
  lat?: number;
  lon?: number;
  altitude_m?: number;
  velocity_ms?: number;
  heading_deg?: number;
  on_ground: boolean;
}

export interface VesselRecord {
  mmsi: string;
  vessel_name: string;
  vessel_type: string;
  timestamp: string;
  lat: number;
  lon: number;
  speed_kts?: number;
  nav_status: string;
  destination?: string;
}

export interface TerminalRecord {
  asset_id: string;
  name: string;
  asset_type: string;
  lat: number;
  lon: number;
  capacity?: number;
  capacity_unit?: string;
  operator?: string;
}

export interface GeoJSON {
  type: "FeatureCollection";
  features: GeoJSONFeature[];
}

export interface GeoJSONFeature {
  type: "Feature";
  geometry: { type: string; coordinates: number[] };
  properties: Record<string, unknown>;
}

export interface QueryResponse {
  columns: string[];
  data: Record<string, unknown>[];
  row_count: number;
  truncated: boolean;
}

export interface SchemaColumn {
  column_name: string;
  column_type: string;
}

export interface DrilldownResponse {
  signal_name: string;
  value: number;
  direction: string;
  confidence: number;
  is_extreme: boolean;
  components: SignalComponent[];
  metadata: Record<string, unknown>;
}

export interface BacktestRequest {
  signal_table: string;
  signal_column?: string;
  returns_table: string;
  returns_column?: string;
  threshold?: number;
  cost_bps?: number;
}

export interface BacktestResponse {
  total_return: number;
  annualised_return: number;
  sharpe: number;
  max_drawdown: number;
  hit_rate: number;
  num_trades: number;
  pnl_series: { date: string; value: number }[];
}
