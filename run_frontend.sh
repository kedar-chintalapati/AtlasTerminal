#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
#  Atlas Terminal — Frontend Launcher (macOS / Linux)
#  Run:  bash run_frontend.sh
#  The backend must already be running (run_backend.sh).
# ─────────────────────────────────────────────────────────────────
set -e

echo ""
echo "  ╔══════════════════════════════════════╗"
echo "  ║       Atlas Terminal  Frontend       ║"
echo "  ╚══════════════════════════════════════╝"
echo ""

# ── Check Node ───────────────────────────────────────────────────
if ! command -v node &>/dev/null; then
    echo "  ERROR: Node.js not found."
    echo "  Install Node.js 18+ from https://nodejs.org/"
    exit 1
fi

node --version

# ── Change to frontend dir ────────────────────────────────────────
cd "$(dirname "$0")/atlas_app/frontend"

# ── Install packages if needed ────────────────────────────────────
if [ ! -d "node_modules" ]; then
    echo ""
    echo "  Installing frontend packages (first run only)..."
    npm install
fi

echo ""
echo "  Starting UI at http://localhost:5173  (Ctrl+C to stop)"
echo ""
npm run dev
