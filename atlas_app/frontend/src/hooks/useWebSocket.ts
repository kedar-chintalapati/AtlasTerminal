/**
 * WebSocket hook for the live atlas alert stream.
 */

import { useEffect, useRef } from "react";
import { useAlertStore } from "../store";
import type { AtlasAlertRecord } from "../api/client";

const WS_URL = `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.host}/ws/alerts`;

export function useAlertWebSocket(): void {
  const wsRef = useRef<WebSocket | null>(null);
  const { addAlert, addAlerts, setWsConnected } = useAlertStore();

  useEffect(() => {
    let retryTimeout: ReturnType<typeof setTimeout>;
    let unmounted = false;

    function connect() {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        if (!unmounted) setWsConnected(true);
      };

      ws.onmessage = (evt) => {
        try {
          const msg = JSON.parse(evt.data) as {
            type: string;
            alert?: AtlasAlertRecord;
            alerts?: AtlasAlertRecord[];
          };
          if (msg.type === "alert" && msg.alert) {
            addAlert(msg.alert);
          } else if (msg.type === "alerts" && msg.alerts) {
            addAlerts(msg.alerts);
          }
        } catch {
          // ignore malformed messages
        }
      };

      ws.onclose = () => {
        if (!unmounted) {
          setWsConnected(false);
          // Reconnect after 5s
          retryTimeout = setTimeout(connect, 5_000);
        }
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    return () => {
      unmounted = true;
      clearTimeout(retryTimeout);
      wsRef.current?.close();
    };
  }, [addAlert, addAlerts, setWsConnected]);
}
