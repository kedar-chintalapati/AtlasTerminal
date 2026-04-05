/**
 * Atlas map panel.
 *
 * Renders a MapLibre base map with deck.gl overlays for:
 *   - Energy assets (ScatterplotLayer)
 *   - Fire detections (ScatterplotLayer, heat-mapped by FRP)
 *   - Vessel positions (ScatterplotLayer + IconLayer)
 *   - NWS alert zones (GeoJsonLayer)
 *   - Atlas alerts with coords (ScatterplotLayer)
 */

import React, { useCallback, useMemo } from "react";
import Map, { NavigationControl } from "react-map-gl/maplibre";
import { DeckGL } from "@deck.gl/react";
import { ScatterplotLayer, TextLayer, GeoJsonLayer } from "@deck.gl/layers";
import { HeatmapLayer } from "@deck.gl/aggregation-layers";
import { useQuery } from "@tanstack/react-query";
import { mapApi, type GeoJSONFeature } from "../../api/client";
import { useMapStore, useWorkspaceStore } from "../../store";
import { LayerControl } from "./LayerControl";
import { domainColor } from "../../lib/utils";

const MAP_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

export function AtlasMap() {
  const { activeLayers, viewport, setViewport, selectFeature, fireDays } = useMapStore();
  const { setDrilldownTarget } = useWorkspaceStore();

  // ── Data queries ──────────────────────────────────────────────────────
  const { data: assetsGeo } = useQuery({
    queryKey: ["map-assets"],
    queryFn: mapApi.getAssetsLayer,
    staleTime: Infinity,
  });

  const { data: firesGeo } = useQuery({
    queryKey: ["map-fires", fireDays],
    queryFn: () => mapApi.getFiresLayer(fireDays),
    enabled: activeLayers.has("fires"),
  });

  const { data: vesselsGeo } = useQuery({
    queryKey: ["map-vessels"],
    queryFn: () => mapApi.getVesselsLayer(2000),
    enabled: activeLayers.has("vessels"),
    refetchInterval: 60_000,
  });

  const { data: alertsGeo } = useQuery({
    queryKey: ["map-alerts"],
    queryFn: () => mapApi.getAlertsLayer(),
    enabled: activeLayers.has("alerts"),
    refetchInterval: 30_000,
  });

  // ── deck.gl layers ────────────────────────────────────────────────────
  const layers = useMemo(() => {
    const result = [];

    // Assets layer
    if (activeLayers.has("assets") && assetsGeo) {
      result.push(
        new ScatterplotLayer({
          id: "assets",
          data: assetsGeo.features,
          getPosition: (f: GeoJSONFeature) => f.geometry.coordinates as [number, number],
          getRadius: 8000,
          getFillColor: (f: GeoJSONFeature) => {
            const t = f.properties.type as string;
            if (t === "lng_terminal") return [59, 130, 246, 220];
            if (t === "gas_hub") return [167, 139, 250, 220];
            return [100, 116, 139, 180];
          },
          getLineColor: [255, 255, 255, 100],
          lineWidthMinPixels: 1,
          stroked: true,
          radiusMinPixels: 4,
          radiusMaxPixels: 12,
          pickable: true,
          onClick: ({ object }) => {
            if (object) selectFeature(object.properties as Record<string, unknown>);
          },
        })
      );

      // Asset labels
      result.push(
        new TextLayer({
          id: "asset-labels",
          data: assetsGeo.features.filter(
            (f) => f.properties.type === "lng_terminal"
          ),
          getPosition: (f: GeoJSONFeature) => f.geometry.coordinates as [number, number],
          getText: (f: GeoJSONFeature) => f.properties.name as string,
          getSize: 11,
          getColor: [255, 255, 255, 180],
          getPixelOffset: [0, -14],
          background: true,
          backgroundPadding: [4, 2, 4, 2],
          getBorderColor: [0, 0, 0, 150],
          getBorderWidth: 1,
          getBackgroundColor: [20, 23, 32, 200],
        })
      );
    }

    // Fire detections (heatmap + scatter)
    if (activeLayers.has("fires") && firesGeo && firesGeo.features.length > 0) {
      result.push(
        new HeatmapLayer({
          id: "fires-heat",
          data: firesGeo.features,
          getPosition: (f: GeoJSONFeature) => f.geometry.coordinates as [number, number],
          getWeight: (f: GeoJSONFeature) => {
            const frp = f.properties.frp_mw as number | null;
            return frp ? Math.min(frp / 100, 5) : 1;
          },
          radiusPixels: 40,
          colorRange: [
            [255, 255, 0, 0],
            [255, 200, 0, 128],
            [255, 120, 0, 200],
            [255, 40, 0, 240],
            [180, 0, 0, 255],
          ],
        })
      );

      result.push(
        new ScatterplotLayer({
          id: "fires-scatter",
          data: firesGeo.features,
          getPosition: (f: GeoJSONFeature) => f.geometry.coordinates as [number, number],
          getRadius: 3000,
          getFillColor: [249, 115, 22, 200],
          radiusMinPixels: 2,
          radiusMaxPixels: 8,
          pickable: true,
          onClick: ({ object }) => {
            if (object) selectFeature(object.properties as Record<string, unknown>);
          },
        })
      );
    }

    // Vessel positions
    if (activeLayers.has("vessels") && vesselsGeo) {
      result.push(
        new ScatterplotLayer({
          id: "vessels",
          data: vesselsGeo.features,
          getPosition: (f: GeoJSONFeature) => f.geometry.coordinates as [number, number],
          getRadius: 4000,
          getFillColor: (f: GeoJSONFeature) => {
            const t = f.properties.type as string;
            if (t === "tanker") return [52, 211, 153, 200];
            if (t === "lng_carrier") return [96, 165, 250, 200];
            return [100, 116, 139, 150];
          },
          radiusMinPixels: 3,
          radiusMaxPixels: 10,
          pickable: true,
          onClick: ({ object }) => {
            if (object) selectFeature(object.properties as Record<string, unknown>);
          },
        })
      );
    }

    // Atlas alerts
    if (activeLayers.has("alerts") && alertsGeo) {
      result.push(
        new ScatterplotLayer({
          id: "alert-points",
          data: alertsGeo.features,
          getPosition: (f: GeoJSONFeature) => f.geometry.coordinates as [number, number],
          getRadius: 20000,
          getFillColor: (f: GeoJSONFeature) => {
            const color = domainColor(f.properties.domain as string);
            const r = parseInt(color.slice(1, 3), 16);
            const g = parseInt(color.slice(3, 5), 16);
            const b = parseInt(color.slice(5, 7), 16);
            return [r, g, b, 180];
          },
          getLineColor: [255, 255, 255, 100],
          stroked: true,
          lineWidthMinPixels: 1,
          radiusMinPixels: 6,
          radiusMaxPixels: 20,
          pickable: true,
          onClick: ({ object }) => {
            if (object) selectFeature(object.properties as Record<string, unknown>);
          },
        })
      );
    }

    return result;
  }, [activeLayers, assetsGeo, firesGeo, vesselsGeo, alertsGeo, selectFeature]);

  const onViewStateChange = useCallback(
    ({ viewState }: { viewState: { latitude: number; longitude: number; zoom: number } }) => {
      setViewport(viewState);
    },
    [setViewport]
  );

  return (
    <div className="relative w-full h-full">
      <DeckGL
        initialViewState={viewport}
        controller={true}
        layers={layers}
        onViewStateChange={onViewStateChange as any}
        getTooltip={({ object }) => {
          if (!object || !object.properties) return null;
          const p = object.properties as Record<string, unknown>;
          return {
            html: `<div class="bg-atlas-surface text-atlas-text p-2 rounded text-xs border border-atlas-border max-w-xs">
              <strong>${p.name || p.title || p.mmsi || "Feature"}</strong>
              ${p.type ? `<br/><span class="text-atlas-muted">${p.type}</span>` : ""}
              ${p.speed ? `<br/>Speed: ${(p.speed as number).toFixed(1)} kts` : ""}
              ${p.score ? `<br/>Score: ${(p.score as number).toFixed(2)}` : ""}
            </div>`,
            style: { background: "transparent", border: "none", padding: 0 },
          };
        }}
      >
        <Map
          mapStyle={MAP_STYLE}
          attributionControl={false}
        >
          <NavigationControl position="top-right" />
        </Map>
      </DeckGL>

      {/* Layer control overlay */}
      <div className="absolute top-3 left-3 z-10">
        <LayerControl />
      </div>
    </div>
  );
}
