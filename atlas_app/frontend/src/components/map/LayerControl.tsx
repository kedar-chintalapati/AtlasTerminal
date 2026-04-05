import React from "react";
import { Flame, Ship, AlertTriangle, Building2, Cloud, Plane } from "lucide-react";
import { useMapStore, type LayerName } from "../../store";
import { cn } from "../../lib/utils";

const LAYERS: { id: LayerName; label: string; icon: React.ReactNode; color: string }[] = [
  { id: "assets", label: "Assets", icon: <Building2 size={12} />, color: "text-blue-400" },
  { id: "fires", label: "Fires", icon: <Flame size={12} />, color: "text-orange-400" },
  { id: "vessels", label: "Vessels", icon: <Ship size={12} />, color: "text-green-400" },
  { id: "alerts", label: "Alerts", icon: <AlertTriangle size={12} />, color: "text-yellow-400" },
  { id: "weather", label: "Weather", icon: <Cloud size={12} />, color: "text-sky-400" },
  { id: "aircraft", label: "Aircraft", icon: <Plane size={12} />, color: "text-purple-400" },
];

export function LayerControl() {
  const { activeLayers, toggleLayer, fireDays, setFireDays } = useMapStore();

  return (
    <div className="bg-atlas-surface/90 backdrop-blur border border-atlas-border rounded-lg p-2 flex flex-col gap-1 shadow-xl">
      <div className="text-[10px] text-atlas-muted uppercase tracking-wider px-1 mb-0.5">
        Layers
      </div>
      {LAYERS.map((l) => (
        <button
          key={l.id}
          onClick={() => toggleLayer(l.id)}
          className={cn(
            "flex items-center gap-2 px-2 py-1 rounded text-xs transition-colors",
            activeLayers.has(l.id)
              ? "bg-atlas-border text-atlas-text"
              : "text-atlas-muted hover:text-atlas-text"
          )}
        >
          <span className={activeLayers.has(l.id) ? l.color : "text-atlas-muted"}>
            {l.icon}
          </span>
          {l.label}
        </button>
      ))}

      {activeLayers.has("fires") && (
        <div className="mt-1 pt-1 border-t border-atlas-border">
          <div className="text-[10px] text-atlas-muted px-1 mb-1">Fire window</div>
          <div className="flex gap-1 px-1">
            {[1, 3, 7].map((d) => (
              <button
                key={d}
                onClick={() => setFireDays(d)}
                className={cn(
                  "flex-1 text-[10px] py-0.5 rounded",
                  fireDays === d
                    ? "bg-orange-600 text-white"
                    : "bg-atlas-border text-atlas-muted hover:text-atlas-text"
                )}
              >
                {d}d
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
