/**
 * Zustand global state stores.
 *
 * Split into domain-specific slices:
 *   mapStore      — active layers, viewport, selected feature
 *   alertStore    — real-time alert stream
 *   workspaceStore — panel layout, active panels
 *   settingsStore  — user preferences (API keys, theme, etc.)
 */

import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { AtlasAlertRecord, SignalComponent } from "../api/client";

// ─── Map store ────────────────────────────────────────────────────────────────

export type LayerName =
  | "assets"
  | "fires"
  | "vessels"
  | "alerts"
  | "weather"
  | "aircraft";

interface MapState {
  activeLayers: Set<LayerName>;
  viewport: {
    latitude: number;
    longitude: number;
    zoom: number;
  };
  selectedFeature: Record<string, unknown> | null;
  fireDays: number;
  toggleLayer: (layer: LayerName) => void;
  setViewport: (vp: Partial<MapState["viewport"]>) => void;
  selectFeature: (feature: Record<string, unknown> | null) => void;
  setFireDays: (days: number) => void;
}

export const useMapStore = create<MapState>()((set) => ({
  activeLayers: new Set(["assets", "fires", "vessels"]),
  viewport: { latitude: 29.5, longitude: -91.5, zoom: 5 },
  selectedFeature: null,
  fireDays: 3,
  toggleLayer: (layer) =>
    set((s) => {
      const next = new Set(s.activeLayers);
      next.has(layer) ? next.delete(layer) : next.add(layer);
      return { activeLayers: next };
    }),
  setViewport: (vp) => set((s) => ({ viewport: { ...s.viewport, ...vp } })),
  selectFeature: (feature) => set({ selectedFeature: feature }),
  setFireDays: (days) => set({ fireDays: days }),
}));

// ─── Alert store ──────────────────────────────────────────────────────────────

interface AlertState {
  alerts: AtlasAlertRecord[];
  unreadCount: number;
  wsConnected: boolean;
  addAlert: (alert: AtlasAlertRecord) => void;
  addAlerts: (alerts: AtlasAlertRecord[]) => void;
  markRead: () => void;
  setWsConnected: (v: boolean) => void;
}

export const useAlertStore = create<AlertState>()((set) => ({
  alerts: [],
  unreadCount: 0,
  wsConnected: false,
  addAlert: (alert) =>
    set((s) => ({
      alerts: [alert, ...s.alerts].slice(0, 500),
      unreadCount: s.unreadCount + 1,
    })),
  addAlerts: (alerts) =>
    set((s) => ({
      alerts: [...alerts, ...s.alerts].slice(0, 500),
      unreadCount: s.unreadCount + alerts.length,
    })),
  markRead: () => set({ unreadCount: 0 }),
  setWsConnected: (v) => set({ wsConnected: v }),
}));

// ─── Workspace store ──────────────────────────────────────────────────────────

export type PanelId = "map" | "analytics" | "feed" | "notebook";

interface WorkspaceState {
  layout: "quad" | "map-focus" | "analytics-focus" | "notebook-focus";
  activePanel: PanelId;
  drilldownTarget: {
    commodity: string;
    components: SignalComponent[];
  } | null;
  setLayout: (layout: WorkspaceState["layout"]) => void;
  setActivePanel: (panel: PanelId) => void;
  setDrilldownTarget: (target: WorkspaceState["drilldownTarget"]) => void;
}

export const useWorkspaceStore = create<WorkspaceState>()((set) => ({
  layout: "quad",
  activePanel: "map",
  drilldownTarget: null,
  setLayout: (layout) => set({ layout }),
  setActivePanel: (activePanel) => set({ activePanel }),
  setDrilldownTarget: (drilldownTarget) => set({ drilldownTarget }),
}));

// ─── Settings store (persisted) ───────────────────────────────────────────────

interface SettingsState {
  geminiApiKey: string;
  geminiModel: string;
  eiaApiKey: string;
  firmsMapKey: string;
  defaultCommodity: string;
  alertSoundEnabled: boolean;
  setGeminiApiKey: (key: string) => void;
  setGeminiModel: (model: string) => void;
  setEiaApiKey: (key: string) => void;
  setFirmsMapKey: (key: string) => void;
  setDefaultCommodity: (c: string) => void;
  setAlertSoundEnabled: (v: boolean) => void;
}

export const useSettingsStore = create<SettingsState>()(
  persist(
    (set) => ({
      geminiApiKey: "",
      geminiModel: "gemini-1.5-flash",
      eiaApiKey: "",
      firmsMapKey: "",
      defaultCommodity: "crude",
      alertSoundEnabled: false,
      setGeminiApiKey: (geminiApiKey) => set({ geminiApiKey }),
      setGeminiModel: (geminiModel) => set({ geminiModel }),
      setEiaApiKey: (eiaApiKey) => set({ eiaApiKey }),
      setFirmsMapKey: (firmsMapKey) => set({ firmsMapKey }),
      setDefaultCommodity: (defaultCommodity) => set({ defaultCommodity }),
      setAlertSoundEnabled: (alertSoundEnabled) => set({ alertSoundEnabled }),
    }),
    { name: "atlas-settings" }
  )
);
