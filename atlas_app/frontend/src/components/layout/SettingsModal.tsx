import React from "react";
import { X, Key, Cpu } from "lucide-react";
import { useSettingsStore } from "../../store";

interface Props {
  onClose: () => void;
}

export function SettingsModal({ onClose }: Props) {
  const {
    geminiApiKey, setGeminiApiKey,
    geminiModel, setGeminiModel,
    eiaApiKey, setEiaApiKey,
    firmsMapKey, setFirmsMapKey,
  } = useSettingsStore();

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div className="bg-atlas-surface border border-atlas-border rounded-xl w-full max-w-lg shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-atlas-border">
          <div>
            <h2 className="text-sm font-semibold text-atlas-text">Settings</h2>
            <p className="text-[11px] text-atlas-muted mt-0.5">
              API keys are stored locally in your browser. Never sent to third parties.
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded hover:bg-atlas-border transition-colors"
          >
            <X size={15} className="text-atlas-muted" />
          </button>
        </div>

        {/* Fields */}
        <div className="px-5 py-4 space-y-4">
          <Section title="Data Sources" icon={<Key size={14} />}>
            <Field
              label="EIA Open Data API Key"
              placeholder="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
              value={eiaApiKey}
              onChange={setEiaApiKey}
              hint="Get a free key at eia.gov/opendata/register.php"
            />
            <Field
              label="NASA FIRMS MAP_KEY"
              placeholder="DEMO_KEY or your key"
              value={firmsMapKey}
              onChange={setFirmsMapKey}
              hint="Free at firms.modaps.eosdis.nasa.gov/api/"
            />
          </Section>

          <Section title="AI Layer (Optional)" icon={<Cpu size={14} />}>
            <Field
              label="Google Gemini API Key"
              placeholder="AIza..."
              value={geminiApiKey}
              onChange={setGeminiApiKey}
              hint="Optional. Used for alert explanations. Leave blank to disable."
            />
            <div>
              <label className="block text-[11px] text-atlas-muted mb-1">Gemini Model</label>
              <select
                value={geminiModel}
                onChange={(e) => setGeminiModel(e.target.value)}
                className="w-full bg-atlas-bg border border-atlas-border rounded px-3 py-1.5 text-xs text-atlas-text outline-none focus:border-blue-500"
              >
                <option value="gemini-1.5-flash">gemini-1.5-flash (fast, cheap)</option>
                <option value="gemini-1.5-pro">gemini-1.5-pro (more capable)</option>
              </select>
            </div>
          </Section>
        </div>

        {/* Footer */}
        <div className="flex justify-end gap-2 px-5 py-3 border-t border-atlas-border">
          <button
            onClick={onClose}
            className="px-4 py-1.5 bg-blue-600 hover:bg-blue-500 text-white text-xs rounded transition-colors"
          >
            Save & Close
          </button>
        </div>
      </div>
    </div>
  );
}

function Section({
  title,
  icon,
  children,
}: {
  title: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div>
      <div className="flex items-center gap-1.5 text-xs font-semibold text-atlas-text mb-2">
        <span className="text-atlas-muted">{icon}</span>
        {title}
      </div>
      <div className="space-y-3 pl-4">{children}</div>
    </div>
  );
}

function Field({
  label,
  placeholder,
  value,
  onChange,
  hint,
}: {
  label: string;
  placeholder?: string;
  value: string;
  onChange: (v: string) => void;
  hint?: string;
}) {
  return (
    <div>
      <label className="block text-[11px] text-atlas-muted mb-1">{label}</label>
      <input
        type="password"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full bg-atlas-bg border border-atlas-border rounded px-3 py-1.5 text-xs text-atlas-text outline-none focus:border-blue-500 font-mono"
      />
      {hint && <div className="text-[10px] text-atlas-muted mt-0.5">{hint}</div>}
    </div>
  );
}
