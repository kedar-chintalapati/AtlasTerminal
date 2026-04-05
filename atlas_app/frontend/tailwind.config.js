/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        // Atlas Terminal dark theme palette
        atlas: {
          bg: "#0d0f14",
          surface: "#141720",
          border: "#252a35",
          text: "#e2e8f0",
          muted: "#64748b",
          accent: "#3b82f6",
          success: "#22c55e",
          warning: "#f59e0b",
          danger: "#ef4444",
          fire: "#f97316",
          energy: "#a78bfa",
          weather: "#38bdf8",
          shipping: "#34d399",
        },
      },
      fontFamily: {
        mono: ["'JetBrains Mono'", "'Fira Code'", "monospace"],
        sans: ["'Inter'", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};
