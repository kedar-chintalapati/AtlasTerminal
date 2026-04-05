/**
 * Analytics workspace panel.
 *
 * Uses Recharts for signal time-series charts and a data table.
 * Exposes tabs for: Storage, Weather, Fires, Vessels, Composite.
 */

import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine, Cell,
} from "recharts";
import { BarChart2, TrendingDown, TrendingUp, Minus } from "lucide-react";
import { energyApi, eventsApi, weatherApi } from "../../api/client";
import { useWorkspaceStore } from "../../store";
import { cn, formatNumber, signalColor } from "../../lib/utils";
import { DrilldownPanel } from "../drilldown/CausalDrilldown";
import { ApiErrorBanner } from "../layout/ApiErrorBanner";

type Tab = "storage" | "weather" | "composite";

export function AnalyticsWorkspace() {
  const [tab, setTab] = useState<Tab>("storage");
  const { drilldownTarget } = useWorkspaceStore();

  if (drilldownTarget) {
    return <DrilldownPanel />;
  }

  return (
    <div className="h-full flex flex-col bg-atlas-surface">
      {/* Header */}
      <div className="flex items-center gap-1 px-3 py-2 border-b border-atlas-border flex-shrink-0">
        <BarChart2 size={13} className="text-purple-400" />
        <span className="text-xs font-semibold text-atlas-text">Analytics</span>

        <div className="ml-auto flex gap-1">
          {(["storage", "weather", "composite"] as Tab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={cn(
                "px-2.5 py-0.5 rounded text-xs transition-colors",
                tab === t
                  ? "bg-purple-700 text-white"
                  : "text-atlas-muted hover:text-atlas-text"
              )}
            >
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-3">
        {tab === "storage" && <StorageTab />}
        {tab === "weather" && <WeatherTab />}
        {tab === "composite" && <CompositeTab />}
      </div>
    </div>
  );
}

function StorageTab() {
  const [commodity, setCommodity] = useState<"crude" | "natgas">("crude");

  const { data: histData, error: histError, refetch: refetchHist } = useQuery({
    queryKey: ["surprise-history", commodity],
    queryFn: () => energyApi.getSurpriseHistory(commodity, "US", 52),
    retry: 1,
  });

  const { data: surprise, error: surpriseError } = useQuery({
    queryKey: ["storage-surprise", commodity],
    queryFn: () => energyApi.getStorageSurprise(commodity, "US"),
    retry: 1,
  });

  const chartData = (histData?.data ?? []).slice().reverse().map((r) => ({
    date: r.report_date.slice(5),   // MM-DD
    z_score: r.z_score,
    surprise: r.surprise,
    direction: r.signal_direction,
  }));

  const sig = surprise;

  if (histError && surpriseError) {
    return (
      <ApiErrorBanner
        panel="Storage"
        message="Could not load storage data"
        error={histError}
        onRetry={() => refetchHist()}
      />
    );
  }

  return (
    <div className="space-y-4">
      {/* Signal card */}
      <div className="grid grid-cols-2 gap-3">
        <SignalCard
          label="Storage Surprise"
          value={sig?.value ?? 0}
          direction={sig?.direction ?? "neutral"}
          confidence={sig?.confidence ?? 0}
          detail={`z = ${formatNumber(sig?.metadata?.z_score as number)}`}
        />
        <div className="flex gap-2">
          {(["crude", "natgas"] as const).map((c) => (
            <button
              key={c}
              onClick={() => setCommodity(c)}
              className={cn(
                "flex-1 py-1.5 rounded text-xs",
                commodity === c
                  ? "bg-purple-700 text-white"
                  : "bg-atlas-border text-atlas-muted hover:text-atlas-text"
              )}
            >
              {c === "crude" ? "Crude" : "Nat Gas"}
            </button>
          ))}
        </div>
      </div>

      {/* Chart */}
      <div>
        <div className="text-[11px] text-atlas-muted mb-2">Storage Surprise Z-Score (52w)</div>
        <ResponsiveContainer width="100%" height={180}>
          <BarChart data={chartData} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#252a35" />
            <XAxis dataKey="date" tick={{ fontSize: 9, fill: "#64748b" }} interval={7} />
            <YAxis tick={{ fontSize: 9, fill: "#64748b" }} domain={["auto", "auto"]} />
            <Tooltip
              contentStyle={{ background: "#141720", border: "1px solid #252a35", borderRadius: 4, fontSize: 11 }}
              labelStyle={{ color: "#94a3b8" }}
            />
            <ReferenceLine y={0} stroke="#252a35" />
            <ReferenceLine y={1.5} stroke="#f59e0b" strokeDasharray="4 4" />
            <ReferenceLine y={-1.5} stroke="#f59e0b" strokeDasharray="4 4" />
            <Bar dataKey="z_score" name="Z-Score" radius={[2, 2, 0, 0]}>
              {chartData.map((entry, index) => (
                <Cell
                  key={index}
                  fill={entry.z_score > 0 ? "#22c55e" : "#ef4444"}
                  opacity={0.8}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function WeatherTab() {
  const { data: weatherRisk, error: weatherError, refetch: refetchWeather } = useQuery({
    queryKey: ["weather-risk"],
    queryFn: weatherApi.getWeatherRiskScore,
    refetchInterval: 15 * 60 * 1000,
    retry: 1,
  });

  const { data: kIndex } = useQuery({
    queryKey: ["k-index"],
    queryFn: weatherApi.getKIndex,
    refetchInterval: 5 * 60 * 1000,
    retry: 1,
  });

  const kData = (kIndex?.data ?? []).slice(-48).map((r) => ({
    time: r.timestamp.slice(11, 16),
    k: r.k_index,
  }));

  if (weatherError) {
    return (
      <ApiErrorBanner
        panel="Weather"
        message="Could not load weather data"
        error={weatherError}
        onRetry={() => refetchWeather()}
      />
    );
  }

  return (
    <div className="space-y-4">
      <SignalCard
        label="Weather Risk"
        value={weatherRisk?.value ?? 0}
        direction={weatherRisk?.direction ?? "neutral"}
        confidence={weatherRisk?.confidence ?? 0}
        detail={`${weatherRisk?.metadata?.active_alerts ?? 0} active NWS alerts`}
      />

      <div>
        <div className="text-[11px] text-atlas-muted mb-2">Geomagnetic K-Index (48h)</div>
        <ResponsiveContainer width="100%" height={120}>
          <LineChart data={kData} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#252a35" />
            <XAxis dataKey="time" tick={{ fontSize: 9, fill: "#64748b" }} interval={7} />
            <YAxis domain={[0, 9]} tick={{ fontSize: 9, fill: "#64748b" }} />
            <Tooltip contentStyle={{ background: "#141720", border: "1px solid #252a35", fontSize: 11 }} />
            <ReferenceLine y={5} stroke="#f59e0b" strokeDasharray="4 4" label={{ value: "G1", fill: "#f59e0b", fontSize: 9 }} />
            <Line dataKey="k" stroke="#818cf8" dot={false} strokeWidth={1.5} name="K-Index" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function CompositeTab() {
  const { data: composite, isLoading } = useQuery({
    queryKey: ["composite-drilldown"],
    queryFn: () =>
      fetch("/api/v1/research/drilldown", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ commodity: "crude" }),
      }).then((r) => r.json()),
    refetchInterval: 5 * 60 * 1000,
  });

  if (isLoading || !composite) {
    return (
      <div className="flex items-center justify-center h-20 text-atlas-muted text-xs">
        Computing composite signal…
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <SignalCard
        label="Composite Bearish Risk"
        value={composite.value ?? 0}
        direction={composite.direction ?? "neutral"}
        confidence={composite.confidence ?? 0}
        detail="Cross-domain physical intelligence"
      />

      <div className="space-y-2">
        {(composite.components ?? []).map((c: any) => (
          <ComponentBar key={c.name} component={c} />
        ))}
      </div>
    </div>
  );
}

function SignalCard({
  label,
  value,
  direction,
  confidence,
  detail,
}: {
  label: string;
  value: number;
  direction: string;
  confidence: number;
  detail?: string;
}) {
  const Icon =
    direction === "bullish"
      ? TrendingUp
      : direction === "bearish"
      ? TrendingDown
      : Minus;
  const dirClass = signalColor(direction);

  return (
    <div className="bg-atlas-bg rounded-lg p-3 border border-atlas-border">
      <div className="text-[11px] text-atlas-muted mb-1">{label}</div>
      <div className={cn("flex items-center gap-2", dirClass)}>
        <Icon size={16} />
        <span className="text-lg font-bold">{formatNumber(value, 3)}</span>
        <span className="text-xs capitalize font-medium">{direction}</span>
      </div>
      {detail && <div className="text-[10px] text-atlas-muted mt-1">{detail}</div>}
      <div className="mt-2">
        <div className="flex justify-between text-[9px] text-atlas-muted mb-0.5">
          <span>Confidence</span>
          <span>{(confidence * 100).toFixed(0)}%</span>
        </div>
        <div className="h-1 bg-atlas-border rounded-full overflow-hidden">
          <div
            className="h-full rounded-full bg-blue-500"
            style={{ width: `${confidence * 100}%` }}
          />
        </div>
      </div>
    </div>
  );
}

function ComponentBar({ component }: { component: any }) {
  const dirClass = signalColor(component.direction);
  const barColor =
    component.direction === "bullish"
      ? "#22c55e"
      : component.direction === "bearish"
      ? "#ef4444"
      : "#64748b";

  return (
    <div className="bg-atlas-bg rounded p-2.5 border border-atlas-border">
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs text-atlas-text">{component.name.replace(/_/g, " ")}</span>
        <span className={cn("text-xs font-semibold", dirClass)}>
          {formatNumber(component.value, 2)}
        </span>
      </div>
      <div className="text-[10px] text-atlas-muted mb-1.5 line-clamp-1">
        {component.description}
      </div>
      <div className="h-1 bg-atlas-border rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all"
          style={{
            width: `${Math.abs(component.value) * 50 + 50}%`,
            backgroundColor: barColor,
            opacity: 0.8,
          }}
        />
      </div>
    </div>
  );
}
