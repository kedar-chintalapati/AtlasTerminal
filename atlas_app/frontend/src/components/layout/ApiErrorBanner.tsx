/**
 * Reusable in-panel error display for failed API queries.
 *
 * Provides a human-readable message + a copyable technical block so
 * non-technical users can forward the exact error without digging in
 * DevTools.
 */
import React, { useState } from "react";
import { AlertTriangle, Copy, RefreshCw, WifiOff } from "lucide-react";

interface Props {
  /** Friendly one-liner shown prominently */
  message?: string;
  /** The raw error (or error message string) */
  error?: unknown;
  /** Called when the user clicks "Retry" */
  onRetry?: () => void;
  /** Optional panel name for the report header */
  panel?: string;
}

function extractMessage(err: unknown): string {
  if (!err) return "Unknown error";
  if (typeof err === "string") return err;
  if (err instanceof Error) return err.message;
  try {
    return JSON.stringify(err, null, 2);
  } catch {
    return String(err);
  }
}

function isNetworkError(err: unknown): boolean {
  const msg = extractMessage(err).toLowerCase();
  return (
    msg.includes("failed to fetch") ||
    msg.includes("networkerror") ||
    msg.includes("network request failed") ||
    msg.includes("econnrefused") ||
    msg.includes("connection refused")
  );
}

export function ApiErrorBanner({ message, error, onRetry, panel }: Props) {
  const [copied, setCopied] = useState(false);

  const isNetwork = isNetworkError(error);
  const technical = extractMessage(error);

  const report = [
    "=== Atlas Terminal API Error ===",
    `Panel   : ${panel ?? "unknown"}`,
    `Time    : ${new Date().toISOString()}`,
    `URL     : ${window.location.href}`,
    `Message : ${message ?? technical}`,
    "",
    "Technical detail:",
    technical,
  ].join("\n");

  const copy = async () => {
    await navigator.clipboard.writeText(report);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="flex flex-col items-center justify-center h-full p-4 gap-3 text-center">
      <div className="flex items-center gap-2 text-amber-400">
        {isNetwork ? <WifiOff size={16} /> : <AlertTriangle size={16} />}
        <span className="text-xs font-semibold">
          {isNetwork
            ? "Cannot reach the Atlas backend"
            : (message ?? "Data unavailable")}
        </span>
      </div>

      {isNetwork && (
        <p className="text-[11px] text-atlas-muted max-w-xs leading-relaxed">
          Make sure the backend server is running:
          <br />
          <code className="text-blue-400">python -m atlas_app.backend.main</code>
        </p>
      )}

      <div className="flex gap-2">
        {onRetry && (
          <button
            onClick={onRetry}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-atlas-border rounded text-xs text-atlas-text hover:bg-atlas-border/80 transition-colors"
          >
            <RefreshCw size={11} />
            Retry
          </button>
        )}
        <button
          onClick={copy}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-red-900/30 border border-red-800 rounded text-xs text-red-300 hover:bg-red-900/50 transition-colors"
        >
          <Copy size={11} />
          {copied ? "Copied!" : "Copy Error"}
        </button>
      </div>

      <pre className="text-[9px] text-atlas-muted bg-atlas-bg rounded p-2 border border-atlas-border max-w-full overflow-auto max-h-20 text-left w-full">
        {technical.slice(0, 300)}
        {technical.length > 300 ? "\n…(truncated, use Copy Error for full)" : ""}
      </pre>
    </div>
  );
}
