/**
 * React Error Boundary — catches render-time crashes and shows a
 * copyable diagnostic report that even non-technical users can forward.
 */
import React, { Component, ReactNode, ErrorInfo } from "react";
import { AlertTriangle, Copy, RefreshCw } from "lucide-react";

interface Props {
  children: ReactNode;
  name?: string; // panel name shown in the report
}

interface State {
  error: Error | null;
  errorInfo: ErrorInfo | null;
  copied: boolean;
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null, errorInfo: null, copied: false };

  static getDerivedStateFromError(error: Error): Partial<State> {
    return { error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    this.setState({ errorInfo });
    console.error(`[AtlasTerminal] Panel "${this.props.name}" crashed:`, error, errorInfo);
  }

  private buildReport(): string {
    const { error, errorInfo } = this.state;
    return [
      "=== Atlas Terminal Error Report ===",
      `Panel    : ${this.props.name ?? "unknown"}`,
      `Time     : ${new Date().toISOString()}`,
      `URL      : ${window.location.href}`,
      `Error    : ${error?.message ?? "unknown"}`,
      "",
      "Stack trace:",
      error?.stack ?? "(none)",
      "",
      "Component stack:",
      errorInfo?.componentStack ?? "(none)",
    ].join("\n");
  }

  private async copy() {
    await navigator.clipboard.writeText(this.buildReport());
    this.setState({ copied: true });
    setTimeout(() => this.setState({ copied: false }), 2000);
  }

  render() {
    if (!this.state.error) return this.props.children;

    return (
      <div className="flex flex-col items-center justify-center h-full p-6 bg-atlas-surface text-center gap-4">
        <div className="flex items-center gap-2 text-red-400">
          <AlertTriangle size={20} />
          <span className="font-semibold text-sm">
            {this.props.name ?? "Panel"} encountered an error
          </span>
        </div>

        <p className="text-xs text-atlas-muted max-w-xs leading-relaxed">
          Something went wrong rendering this panel. Click{" "}
          <strong>Copy Error Report</strong> and paste it into a GitHub issue
          or support message so we can fix it quickly.
        </p>

        <div className="flex gap-2">
          <button
            onClick={() => this.copy()}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-red-900/40 border border-red-700 rounded text-xs text-red-300 hover:bg-red-900/70 transition-colors"
          >
            <Copy size={12} />
            {this.state.copied ? "Copied!" : "Copy Error Report"}
          </button>
          <button
            onClick={() => this.setState({ error: null, errorInfo: null })}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-atlas-border rounded text-xs text-atlas-text hover:bg-atlas-border/80 transition-colors"
          >
            <RefreshCw size={12} />
            Retry
          </button>
        </div>

        <pre className="text-[9px] text-atlas-muted bg-atlas-bg rounded p-3 max-w-full overflow-auto max-h-28 border border-atlas-border text-left">
          {this.state.error.message}
        </pre>
      </div>
    );
  }
}
