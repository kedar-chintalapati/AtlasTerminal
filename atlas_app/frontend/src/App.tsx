/**
 * Atlas Terminal — root application component.
 *
 * Provides a four-panel docking layout:
 *   ┌──────────────────────┬──────────────┐
 *   │                      │  Analytics   │
 *   │      Map Panel       │  Workspace   │
 *   │                      ├──────────────┤
 *   ├──────────────────────┤  Event Tape  │
 *   │   Research Notebook  │  (OSINT Feed)│
 *   └──────────────────────┴──────────────┘
 *
 * Layout variants can be switched from the TopBar.
 */

import React, { useState } from "react";
import {
  PanelGroup,
  Panel,
  PanelResizeHandle,
} from "react-resizable-panels";

import { TopBar } from "./components/layout/TopBar";
import { AtlasMap } from "./components/map/AtlasMap";
import { AnalyticsWorkspace } from "./components/analytics/AnalyticsWorkspace";
import { EventTape } from "./components/feed/EventTape";
import { NotebookPanel } from "./components/notebook/NotebookPanel";
import { SettingsModal } from "./components/layout/SettingsModal";
import { ErrorBoundary } from "./components/layout/ErrorBoundary";
import { useAlertWebSocket } from "./hooks/useWebSocket";
import { useWorkspaceStore } from "./store";

export default function App() {
  const [settingsOpen, setSettingsOpen] = useState(false);
  const { layout } = useWorkspaceStore();

  // Connect to live alert WebSocket
  useAlertWebSocket();

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-atlas-bg">
      <TopBar onSettings={() => setSettingsOpen(true)} />

      <div className="flex-1 overflow-hidden">
        {layout === "quad" && <QuadLayout />}
        {layout === "map-focus" && <MapFocusLayout />}
        {layout === "analytics-focus" && <AnalyticsFocusLayout />}
        {layout === "notebook-focus" && <NotebookFocusLayout />}
      </div>

      {settingsOpen && <SettingsModal onClose={() => setSettingsOpen(false)} />}
    </div>
  );
}

function QuadLayout() {
  return (
    <PanelGroup direction="horizontal" className="h-full">
      {/* Left column: Map + Notebook */}
      <Panel defaultSize={60} minSize={30}>
        <PanelGroup direction="vertical" className="h-full">
          <Panel defaultSize={65} minSize={20}>
            <ErrorBoundary name="Map">
              <AtlasMap />
            </ErrorBoundary>
          </Panel>
          <PanelResizeHandle className="h-1 cursor-row-resize hover:bg-blue-600 transition-colors bg-atlas-border" />
          <Panel defaultSize={35} minSize={15}>
            <ErrorBoundary name="Research Notebook">
              <NotebookPanel />
            </ErrorBoundary>
          </Panel>
        </PanelGroup>
      </Panel>

      <PanelResizeHandle className="w-1 cursor-col-resize hover:bg-blue-600 transition-colors bg-atlas-border" />

      {/* Right column: Analytics + Feed */}
      <Panel defaultSize={40} minSize={20}>
        <PanelGroup direction="vertical" className="h-full">
          <Panel defaultSize={55} minSize={20}>
            <ErrorBoundary name="Analytics Workspace">
              <AnalyticsWorkspace />
            </ErrorBoundary>
          </Panel>
          <PanelResizeHandle className="h-1 cursor-row-resize hover:bg-blue-600 transition-colors bg-atlas-border" />
          <Panel defaultSize={45} minSize={15}>
            <ErrorBoundary name="Event Tape">
              <EventTape />
            </ErrorBoundary>
          </Panel>
        </PanelGroup>
      </Panel>
    </PanelGroup>
  );
}

function MapFocusLayout() {
  return (
    <PanelGroup direction="horizontal" className="h-full">
      <Panel defaultSize={75} minSize={50}>
        <ErrorBoundary name="Map">
          <AtlasMap />
        </ErrorBoundary>
      </Panel>
      <PanelResizeHandle className="w-1 cursor-col-resize hover:bg-blue-600 transition-colors bg-atlas-border" />
      <Panel defaultSize={25} minSize={15}>
        <PanelGroup direction="vertical" className="h-full">
          <Panel defaultSize={50}>
            <ErrorBoundary name="Analytics Workspace">
              <AnalyticsWorkspace />
            </ErrorBoundary>
          </Panel>
          <PanelResizeHandle className="h-1 cursor-row-resize hover:bg-blue-600 transition-colors bg-atlas-border" />
          <Panel defaultSize={50}>
            <ErrorBoundary name="Event Tape">
              <EventTape />
            </ErrorBoundary>
          </Panel>
        </PanelGroup>
      </Panel>
    </PanelGroup>
  );
}

function AnalyticsFocusLayout() {
  return (
    <PanelGroup direction="horizontal" className="h-full">
      <Panel defaultSize={40} minSize={20}>
        <ErrorBoundary name="Map">
          <AtlasMap />
        </ErrorBoundary>
      </Panel>
      <PanelResizeHandle className="w-1 cursor-col-resize hover:bg-blue-600 transition-colors bg-atlas-border" />
      <Panel defaultSize={35} minSize={20}>
        <ErrorBoundary name="Analytics Workspace">
          <AnalyticsWorkspace />
        </ErrorBoundary>
      </Panel>
      <PanelResizeHandle className="w-1 cursor-col-resize hover:bg-blue-600 transition-colors bg-atlas-border" />
      <Panel defaultSize={25} minSize={15}>
        <ErrorBoundary name="Event Tape">
          <EventTape />
        </ErrorBoundary>
      </Panel>
    </PanelGroup>
  );
}

function NotebookFocusLayout() {
  return (
    <PanelGroup direction="horizontal" className="h-full">
      <Panel defaultSize={50} minSize={30}>
        <ErrorBoundary name="Research Notebook">
          <NotebookPanel />
        </ErrorBoundary>
      </Panel>
      <PanelResizeHandle className="w-1 cursor-col-resize hover:bg-blue-600 transition-colors bg-atlas-border" />
      <Panel defaultSize={30} minSize={20}>
        <ErrorBoundary name="Map">
          <AtlasMap />
        </ErrorBoundary>
      </Panel>
      <PanelResizeHandle className="w-1 cursor-col-resize hover:bg-blue-600 transition-colors bg-atlas-border" />
      <Panel defaultSize={20} minSize={15}>
        <ErrorBoundary name="Event Tape">
          <EventTape />
        </ErrorBoundary>
      </Panel>
    </PanelGroup>
  );
}
