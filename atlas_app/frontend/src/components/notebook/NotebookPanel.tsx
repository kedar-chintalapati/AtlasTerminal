/**
 * Research notebook / code console panel.
 *
 * SQL mode: queries the DuckDB store via the /api/v1/query/sql endpoint.
 * Results are displayed in a sortable table with copy-to-clipboard support.
 *
 * Python mode (planned): Pyodide in a web worker for local computation.
 */

import React, { useState, useRef, useCallback } from "react";
import { useMutation } from "@tanstack/react-query";
import { Terminal, Play, Copy, CheckCheck, Database, ChevronDown } from "lucide-react";
import { queryApi, type QueryResponse } from "../../api/client";
import { cn } from "../../lib/utils";

const EXAMPLE_QUERIES = [
  {
    label: "Storage: latest US crude",
    sql: "SELECT * FROM crude_storage WHERE region = 'US' ORDER BY report_date DESC LIMIT 20",
  },
  {
    label: "Storage surprises",
    sql: "SELECT * FROM storage_surprises ORDER BY report_date DESC LIMIT 20",
  },
  {
    label: "Active alerts",
    sql: "SELECT * FROM atlas_alerts ORDER BY created_at DESC LIMIT 20",
  },
  {
    label: "Fires by region (last 3 days)",
    sql: `SELECT
  ROUND(lat, 1) as lat_bin,
  ROUND(lon, 1) as lon_bin,
  COUNT(*) as fire_count,
  AVG(brightness_k) as avg_brightness,
  MAX(frp_mw) as max_frp
FROM firms_detections
WHERE acq_datetime >= NOW() - INTERVAL 3 DAY
GROUP BY 1, 2
ORDER BY fire_count DESC
LIMIT 30`,
  },
  {
    label: "News sentiment by day",
    sql: `SELECT
  CAST(publish_date AS DATE) as date,
  COUNT(*) as articles,
  ROUND(AVG(tone), 2) as avg_tone,
  SUM(CASE WHEN tone < -3 THEN 1 ELSE 0 END) as negative_count
FROM gdelt_events
WHERE publish_date >= NOW() - INTERVAL 30 DAY
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30`,
  },
];

export function NotebookPanel() {
  const [sql, setSql] = useState(EXAMPLE_QUERIES[0].sql);
  const [mode, setMode] = useState<"sql" | "python">("sql");
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);
  const [copied, setCopied] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const { mutate: runQuery, isPending, error } = useMutation({
    mutationFn: (sqlStr: string) => queryApi.runSql(sqlStr),
    onSuccess: (data) => setResult(data),
  });

  const handleRun = useCallback(() => {
    if (sql.trim()) runQuery(sql);
  }, [sql, runQuery]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        handleRun();
      }
    },
    [handleRun]
  );

  const handleCopy = useCallback(() => {
    if (!result) return;
    const csv = [
      result.columns.join(","),
      ...(result.data as Record<string, unknown>[]).map((row) =>
        result.columns.map((c) => JSON.stringify(row[c] ?? "")).join(",")
      ),
    ].join("\n");
    navigator.clipboard.writeText(csv).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [result]);

  const sortedData = React.useMemo(() => {
    if (!result || !sortCol) return result?.data as Record<string, unknown>[] ?? [];
    return [...(result.data as Record<string, unknown>[])].sort((a, b) => {
      const av = a[sortCol];
      const bv = b[sortCol];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      const cmp = av < bv ? -1 : av > bv ? 1 : 0;
      return sortAsc ? cmp : -cmp;
    });
  }, [result, sortCol, sortAsc]);

  return (
    <div className="h-full flex flex-col bg-atlas-surface">
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-2 border-b border-atlas-border flex-shrink-0">
        <Terminal size={13} className="text-green-400" />
        <span className="text-xs font-semibold text-atlas-text">Research Console</span>

        <div className="ml-3 flex gap-1">
          {(["sql", "python"] as const).map((m) => (
            <button
              key={m}
              onClick={() => setMode(m)}
              className={cn(
                "px-2 py-0.5 rounded text-xs uppercase tracking-wide transition-colors",
                mode === m
                  ? "bg-green-700 text-white"
                  : "text-atlas-muted hover:text-atlas-text"
              )}
            >
              {m}
            </button>
          ))}
        </div>

        {/* Example queries picker */}
        <div className="relative ml-auto">
          <select
            onChange={(e) => setSql(e.target.value)}
            className="bg-atlas-border text-atlas-muted text-[10px] rounded px-2 py-1 border-none outline-none pr-5 appearance-none"
            defaultValue=""
          >
            <option value="" disabled>Examples…</option>
            {EXAMPLE_QUERIES.map((q) => (
              <option key={q.label} value={q.sql}>{q.label}</option>
            ))}
          </select>
          <ChevronDown size={10} className="absolute right-1 top-1.5 text-atlas-muted pointer-events-none" />
        </div>
      </div>

      {/* Query editor */}
      <div className="flex-shrink-0 relative">
        <textarea
          ref={textareaRef}
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={handleKeyDown}
          className="code-editor"
          style={{ height: 120, borderRadius: 0, border: "none", borderBottom: "1px solid #252a35" }}
          placeholder={mode === "sql" ? "-- Write SQL here (Ctrl+Enter to run)" : "# Python (NumPy, Pandas, SciPy available)"}
          spellCheck={false}
        />
        <button
          onClick={handleRun}
          disabled={isPending}
          className="absolute bottom-3 right-3 flex items-center gap-1.5 px-3 py-1.5 bg-green-700 hover:bg-green-600 text-white text-xs rounded transition-colors disabled:opacity-50"
        >
          <Play size={11} />
          {isPending ? "Running…" : "Run"}
        </button>
      </div>

      {/* Error */}
      {error && (
        <div className="px-3 py-2 text-xs text-red-400 bg-red-900/20 border-b border-red-900/30 flex-shrink-0">
          {error.message}
        </div>
      )}

      {/* Result table */}
      {result && (
        <div className="flex-1 overflow-hidden flex flex-col">
          {/* Result toolbar */}
          <div className="flex items-center gap-2 px-3 py-1.5 border-b border-atlas-border flex-shrink-0 bg-atlas-bg">
            <Database size={11} className="text-atlas-muted" />
            <span className="text-[11px] text-atlas-muted">
              {result.row_count.toLocaleString()} rows
              {result.truncated && " (truncated)"}
            </span>
            <button
              onClick={handleCopy}
              className="ml-auto flex items-center gap-1 text-[10px] text-atlas-muted hover:text-atlas-text transition-colors"
            >
              {copied ? <CheckCheck size={11} className="text-green-400" /> : <Copy size={11} />}
              {copied ? "Copied" : "CSV"}
            </button>
          </div>

          {/* Table */}
          <div className="flex-1 overflow-auto">
            <table className="w-full text-[11px] border-collapse">
              <thead className="sticky top-0 bg-atlas-surface z-10">
                <tr>
                  {result.columns.map((col) => (
                    <th
                      key={col}
                      onClick={() => {
                        if (sortCol === col) setSortAsc(!sortAsc);
                        else { setSortCol(col); setSortAsc(true); }
                      }}
                      className="text-left px-3 py-1.5 text-atlas-muted font-medium cursor-pointer hover:text-atlas-text border-b border-atlas-border whitespace-nowrap"
                    >
                      {col}
                      {sortCol === col && (sortAsc ? " ↑" : " ↓")}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedData.map((row, i) => (
                  <tr
                    key={i}
                    className="border-b border-atlas-border hover:bg-atlas-border/30 transition-colors"
                  >
                    {result.columns.map((col) => (
                      <td
                        key={col}
                        className="px-3 py-1 font-mono text-atlas-text whitespace-nowrap max-w-xs truncate"
                        title={String(row[col] ?? "")}
                      >
                        {formatCellValue(row[col])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {!result && !isPending && !error && (
        <div className="flex-1 flex items-center justify-center text-atlas-muted text-xs">
          <div className="text-center space-y-1">
            <Database size={24} className="mx-auto opacity-20" />
            <div>Run a query to see results</div>
            <div className="text-[10px]">Ctrl+Enter to execute</div>
          </div>
        </div>
      )}
    </div>
  );
}

function formatCellValue(v: unknown): string {
  if (v == null) return "NULL";
  if (typeof v === "number") {
    if (Number.isInteger(v)) return v.toLocaleString();
    return v.toFixed(4);
  }
  if (typeof v === "boolean") return v ? "true" : "false";
  const s = String(v);
  if (s.length > 80) return s.slice(0, 77) + "…";
  return s;
}
