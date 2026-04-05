/**
 * Cross-domain causal drilldown panel.
 *
 * Shows the composite risk signal decomposed into physical-intelligence components.
 * User can click any component to see raw data, transformation, and historical hit rate.
 */

import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { X, ChevronRight, TrendingDown, TrendingUp, Minus, ExternalLink } from "lucide-react";
import { researchApi } from "../../api/client";
import { useWorkspaceStore } from "../../store";
import { cn, formatNumber, formatPct, signalColor, domainColor } from "../../lib/utils";

export function DrilldownPanel() {
  const { drilldownTarget, setDrilldownTarget } = useWorkspaceStore();
  const [expandedComponent, setExpandedComponent] = useState<string | null>(null);

  const commodity = drilldownTarget?.commodity ?? "crude";

  const { data, isLoading } = useQuery({
    queryKey: ["drilldown", commodity],
    queryFn: () => researchApi.runDrilldown(commodity),
    refetchInterval: 5 * 60 * 1000,
  });

  if (isLoading) {
    return (
      <div className="h-full flex items-center justify-center text-atlas-muted text-xs">
        <div className="text-center">
          <div className="animate-pulse mb-2">Computing cross-domain signal...</div>
          <div className="text-[10px]">Fusing EIA + NWS + FIRMS + AIS + GDELT</div>
        </div>
      </div>
    );
  }

  if (!data) return null;

  const dirColor = signalColor(data.direction);
  const DirIcon =
    data.direction === "bullish"
      ? TrendingUp
      : data.direction === "bearish"
      ? TrendingDown
      : Minus;

  return (
    <div className="h-full flex flex-col bg-atlas-surface overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-atlas-border flex-shrink-0">
        <div>
          <div className="text-xs font-semibold text-atlas-text">
            Causal Drilldown — {commodity.toUpperCase()}
          </div>
          <div className="text-[10px] text-atlas-muted">
            Cross-domain bearish risk decomposition
          </div>
        </div>
        <button
          onClick={() => setDrilldownTarget(null)}
          className="p-1 rounded hover:bg-atlas-border transition-colors"
        >
          <X size={13} className="text-atlas-muted" />
        </button>
      </div>

      {/* Composite score */}
      <div className="px-3 py-3 border-b border-atlas-border flex-shrink-0">
        <div className="bg-atlas-bg rounded-lg p-3 border border-atlas-border">
          <div className="text-[10px] text-atlas-muted mb-1">Composite Bearish Risk Score</div>
          <div className={cn("flex items-center gap-2 mb-2", dirColor)}>
            <DirIcon size={20} />
            <span className="text-2xl font-bold">{formatNumber(data.value, 3)}</span>
            <span className="text-sm capitalize">{data.direction}</span>
            {data.is_extreme && (
              <span className="ml-auto text-[10px] bg-red-900/50 text-red-400 px-2 py-0.5 rounded-full border border-red-800">
                EXTREME
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            <div className="flex-1 h-1.5 bg-atlas-border rounded-full overflow-hidden">
              <div
                className={cn(
                  "h-full rounded-full",
                  data.direction === "bearish" ? "bg-red-500" : "bg-green-500"
                )}
                style={{ width: `${(Math.abs(data.value)) * 100}%` }}
              />
            </div>
            <span className="text-[10px] text-atlas-muted">
              {formatPct(data.confidence)} confidence
            </span>
          </div>
        </div>
      </div>

      {/* Component decomposition */}
      <div className="flex-1 overflow-y-auto px-3 py-2 space-y-2">
        <div className="text-[10px] text-atlas-muted uppercase tracking-wider mb-1">
          Signal Components
        </div>
        {data.components.map((c) => (
          <ComponentCard
            key={c.name}
            component={c}
            isExpanded={expandedComponent === c.name}
            onToggle={() =>
              setExpandedComponent(expandedComponent === c.name ? null : c.name)
            }
          />
        ))}
      </div>
    </div>
  );
}

function ComponentCard({
  component,
  isExpanded,
  onToggle,
}: {
  component: {
    name: string;
    description: string;
    value: number;
    direction: string;
    weight: number;
    historical_hit_rate?: number;
    data_table?: string;
    source_domain: string;
  };
  isExpanded: boolean;
  onToggle: () => void;
}) {
  const dirClass = signalColor(component.direction);
  const color = domainColor(component.source_domain);

  return (
    <div className="rounded-lg border border-atlas-border overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center gap-3 px-3 py-2 bg-atlas-bg hover:bg-atlas-border/50 transition-colors text-left"
      >
        {/* Domain colour dot */}
        <div
          className="w-2 h-2 rounded-full flex-shrink-0"
          style={{ backgroundColor: color }}
        />

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-atlas-text truncate">
              {component.name.replace(/_/g, " ")}
            </span>
            <span
              className="text-[9px] uppercase tracking-wide ml-auto flex-shrink-0"
              style={{ color }}
            >
              {component.source_domain}
            </span>
          </div>
          <div className="text-[10px] text-atlas-muted truncate">{component.description}</div>
        </div>

        <div className={cn("text-sm font-bold flex-shrink-0", dirClass)}>
          {formatNumber(component.value, 2)}
        </div>
        <ChevronRight
          size={12}
          className={cn(
            "text-atlas-muted flex-shrink-0 transition-transform",
            isExpanded && "rotate-90"
          )}
        />
      </button>

      {isExpanded && (
        <div className="px-3 pb-3 bg-atlas-bg border-t border-atlas-border">
          <div className="pt-2 space-y-2">
            {/* Metrics row */}
            <div className="grid grid-cols-3 gap-2">
              <Metric label="Weight" value={formatPct(component.weight)} />
              <Metric
                label="Direction"
                value={component.direction}
                valueClass={dirClass}
              />
              <Metric
                label="Hit Rate"
                value={
                  component.historical_hit_rate != null
                    ? formatPct(component.historical_hit_rate)
                    : "—"
                }
              />
            </div>

            {/* Source table link */}
            {component.data_table && (
              <div className="flex items-center gap-1.5 text-[10px] text-blue-400">
                <ExternalLink size={10} />
                <span>Source table: {component.data_table}</span>
              </div>
            )}

            {/* Value bar */}
            <div>
              <div className="flex justify-between text-[9px] text-atlas-muted mb-0.5">
                <span>−1.0 (bearish)</span>
                <span>+1.0 (bullish)</span>
              </div>
              <div className="relative h-2 bg-atlas-border rounded-full overflow-hidden">
                <div className="absolute inset-y-0 left-1/2 w-px bg-atlas-muted" />
                <div
                  className="absolute inset-y-0 rounded-full"
                  style={{
                    left: component.value < 0 ? `${(0.5 + component.value / 2) * 100}%` : "50%",
                    right: component.value > 0 ? `${(0.5 - component.value / 2) * 100}%` : "50%",
                    backgroundColor:
                      component.direction === "bullish" ? "#22c55e" : "#ef4444",
                    opacity: 0.8,
                  }}
                />
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function Metric({
  label,
  value,
  valueClass,
}: {
  label: string;
  value: string;
  valueClass?: string;
}) {
  return (
    <div className="text-center">
      <div className="text-[9px] text-atlas-muted">{label}</div>
      <div className={cn("text-xs font-semibold text-atlas-text", valueClass)}>{value}</div>
    </div>
  );
}
