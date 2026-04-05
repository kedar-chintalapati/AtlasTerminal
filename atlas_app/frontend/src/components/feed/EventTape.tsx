/**
 * Event tape / OSINT feed panel.
 *
 * Shows a time-ordered stream of atlas alerts, NWS events, FIRMS detections,
 * and GDELT news — scored and colour-coded by domain and severity.
 */

import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Flame, Cloud, Ship, Globe, Zap, RefreshCw } from "lucide-react";
import { eventsApi, type AtlasAlertRecord } from "../../api/client";
import { useAlertStore, useWorkspaceStore } from "../../store";
import { cn, domainColor, relativeTime, severityColor } from "../../lib/utils";

const DOMAIN_ICONS: Record<string, React.ReactNode> = {
  energy: <Zap size={12} />,
  weather: <Cloud size={12} />,
  fire: <Flame size={12} />,
  shipping: <Ship size={12} />,
  geopolitics: <Globe size={12} />,
  composite: <AlertTriangle size={12} />,
};

const SEVERITY_ORDER: Record<string, number> = {
  critical: 0, high: 1, medium: 2, low: 3, info: 4,
};

type FilterDomain = "all" | "energy" | "weather" | "fire" | "shipping" | "geopolitics";

export function EventTape() {
  const [filter, setFilter] = useState<FilterDomain>("all");
  const [minSeverity, setMinSeverity] = useState<string>("low");
  const { alerts: liveAlerts } = useAlertStore();
  const { setDrilldownTarget } = useWorkspaceStore();

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ["alerts", filter, minSeverity],
    queryFn: () =>
      eventsApi.getAlerts(
        filter === "all" ? undefined : filter,
        undefined,
        100
      ),
    refetchInterval: 30_000,
  });

  const storedAlerts: AtlasAlertRecord[] = data?.data ?? [];

  // Merge live WS alerts with stored (deduplicate by alert_id)
  const merged = React.useMemo(() => {
    const seen = new Set<string>();
    const all = [...liveAlerts, ...storedAlerts].filter((a) => {
      if (seen.has(a.alert_id)) return false;
      seen.add(a.alert_id);
      return true;
    });
    return all
      .filter((a) => filter === "all" || a.domain === filter)
      .filter(
        (a) =>
          SEVERITY_ORDER[a.severity?.toLowerCase()] <=
          SEVERITY_ORDER[minSeverity]
      )
      .sort(
        (a, b) =>
          new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      );
  }, [liveAlerts, storedAlerts, filter, minSeverity]);

  return (
    <div className="h-full flex flex-col bg-atlas-surface">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-atlas-border flex-shrink-0">
        <div className="text-xs font-semibold text-atlas-text flex items-center gap-1.5">
          <AlertTriangle size={13} className="text-yellow-400" />
          Event Tape
          <span className="text-atlas-muted font-normal ml-1">
            {merged.length} events
          </span>
        </div>
        <button
          onClick={() => refetch()}
          className="p-1 rounded hover:bg-atlas-border transition-colors"
          title="Refresh"
        >
          <RefreshCw
            size={12}
            className={cn("text-atlas-muted", isFetching && "animate-spin")}
          />
        </button>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-2 px-3 py-1.5 border-b border-atlas-border flex-shrink-0 overflow-x-auto">
        <div className="flex gap-1">
          {(["all", "energy", "weather", "fire", "shipping", "geopolitics"] as FilterDomain[]).map(
            (d) => (
              <button
                key={d}
                onClick={() => setFilter(d)}
                className={cn(
                  "px-2 py-0.5 rounded text-[10px] whitespace-nowrap transition-colors",
                  filter === d
                    ? "bg-blue-600 text-white"
                    : "text-atlas-muted hover:text-atlas-text bg-atlas-border"
                )}
              >
                {d.charAt(0).toUpperCase() + d.slice(1)}
              </button>
            )
          )}
        </div>
        <div className="w-px h-3 bg-atlas-border flex-shrink-0" />
        <select
          value={minSeverity}
          onChange={(e) => setMinSeverity(e.target.value)}
          className="bg-atlas-border text-atlas-muted text-[10px] rounded px-1.5 py-0.5 border-none outline-none"
        >
          <option value="critical">Critical+</option>
          <option value="high">High+</option>
          <option value="medium">Medium+</option>
          <option value="low">Low+</option>
          <option value="info">All</option>
        </select>
      </div>

      {/* Alert list */}
      <div className="flex-1 overflow-y-auto">
        {isLoading && (
          <div className="flex items-center justify-center h-20 text-atlas-muted text-xs">
            Loading...
          </div>
        )}
        {!isLoading && merged.length === 0 && (
          <div className="flex flex-col items-center justify-center h-24 text-atlas-muted text-xs gap-1">
            <AlertTriangle size={20} className="opacity-30" />
            No alerts matching filter
          </div>
        )}
        {merged.map((alert) => (
          <AlertCard key={alert.alert_id} alert={alert} />
        ))}
      </div>
    </div>
  );
}

function AlertCard({ alert }: { alert: AtlasAlertRecord }) {
  const { setDrilldownTarget } = useWorkspaceStore();
  const color = domainColor(alert.domain);
  const severityClass = severityColor(alert.severity);
  const icon = DOMAIN_ICONS[alert.domain] ?? <AlertTriangle size={12} />;

  return (
    <button
      onClick={() => {
        // Open drilldown if it has components
        setDrilldownTarget({ commodity: "crude", components: [] });
      }}
      className="w-full text-left px-3 py-2.5 border-b border-atlas-border hover:bg-atlas-border/40 transition-colors group"
    >
      <div className="flex items-start gap-2">
        {/* Domain indicator */}
        <div
          className="w-1 self-stretch rounded-full flex-shrink-0 mt-0.5"
          style={{ backgroundColor: color }}
        />

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1.5 mb-0.5">
            <span style={{ color }}>{icon}</span>
            <span className={cn("text-[10px] font-semibold uppercase tracking-wide", severityClass)}>
              {alert.severity}
            </span>
            <span className="text-[10px] text-atlas-muted ml-auto flex-shrink-0">
              {relativeTime(alert.created_at)}
            </span>
          </div>
          <div className="text-xs font-medium text-atlas-text truncate">
            {alert.title}
          </div>
          <div className="text-[11px] text-atlas-muted line-clamp-2 mt-0.5">
            {alert.summary}
          </div>

          {/* Score bar */}
          <div className="mt-1.5 flex items-center gap-2">
            <div className="flex-1 h-0.5 bg-atlas-border rounded-full overflow-hidden">
              <div
                className="h-full rounded-full"
                style={{
                  width: `${(alert.score ?? 0) * 100}%`,
                  backgroundColor: color,
                }}
              />
            </div>
            <span className="text-[9px] text-atlas-muted">
              {((alert.score ?? 0) * 100).toFixed(0)}
            </span>
          </div>
        </div>
      </div>
    </button>
  );
}
