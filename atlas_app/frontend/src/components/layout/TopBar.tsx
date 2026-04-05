import React from "react";
import { Activity, Bell, Settings, Zap, BarChart2, Map, Terminal, ServerOff } from "lucide-react";
import { useAlertStore, useWorkspaceStore } from "../../store";
import { useBackendHealth } from "../../hooks/useBackendHealth";
import { cn } from "../../lib/utils";

interface Props {
  onSettings: () => void;
}

export function TopBar({ onSettings }: Props) {
  const { unreadCount, wsConnected, markRead } = useAlertStore();
  const { layout, setLayout } = useWorkspaceStore();
  const { online: backendOnline, checking: backendChecking } = useBackendHealth();

  const layouts = [
    { id: "quad", label: "4-Panel", icon: <Zap size={14} /> },
    { id: "map-focus", label: "Map", icon: <Map size={14} /> },
    { id: "analytics-focus", label: "Analytics", icon: <BarChart2 size={14} /> },
    { id: "notebook-focus", label: "Notebook", icon: <Terminal size={14} /> },
  ] as const;

  return (
    <header className="h-10 bg-atlas-surface border-b border-atlas-border flex items-center px-3 gap-4 flex-shrink-0">
      {/* Logo */}
      <div className="flex items-center gap-2 font-semibold text-sm text-white">
        <Activity size={16} className="text-blue-400" />
        <span className="text-blue-400">Atlas</span>
        <span className="text-atlas-muted font-normal">Terminal</span>
      </div>

      <div className="w-px h-4 bg-atlas-border" />

      {/* Layout switcher */}
      <div className="flex items-center gap-1">
        {layouts.map((l) => (
          <button
            key={l.id}
            onClick={() => setLayout(l.id)}
            className={cn(
              "flex items-center gap-1 px-2 py-1 rounded text-xs transition-colors",
              layout === l.id
                ? "bg-blue-600 text-white"
                : "text-atlas-muted hover:text-atlas-text hover:bg-atlas-border"
            )}
          >
            {l.icon}
            {l.label}
          </button>
        ))}
      </div>

      <div className="flex-1" />

      {/* Backend health */}
      {!backendChecking && !backendOnline && (
        <div
          className="flex items-center gap-1.5 text-xs text-red-400 bg-red-900/30 border border-red-800 rounded px-2 py-0.5"
          title="Backend server is not reachable. Run: python -m atlas_app.backend.main"
        >
          <ServerOff size={12} />
          Backend offline
        </div>
      )}

      {/* WS status */}
      <div className="flex items-center gap-1.5 text-xs text-atlas-muted">
        <span
          className={cn(
            "w-1.5 h-1.5 rounded-full",
            wsConnected ? "bg-green-400" : "bg-red-400"
          )}
        />
        {wsConnected ? "Live" : "Offline"}
      </div>

      {/* Alert bell */}
      <button
        onClick={markRead}
        className="relative p-1.5 rounded hover:bg-atlas-border transition-colors"
        title="Alerts"
      >
        <Bell size={15} className="text-atlas-muted" />
        {unreadCount > 0 && (
          <span className="absolute -top-0.5 -right-0.5 w-4 h-4 bg-red-500 rounded-full text-[9px] flex items-center justify-center text-white font-bold">
            {unreadCount > 99 ? "99+" : unreadCount}
          </span>
        )}
      </button>

      {/* Settings */}
      <button
        onClick={onSettings}
        className="p-1.5 rounded hover:bg-atlas-border transition-colors"
        title="Settings"
      >
        <Settings size={15} className="text-atlas-muted" />
      </button>
    </header>
  );
}
