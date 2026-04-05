#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
#  Atlas Terminal — Backend Launcher (macOS / Linux)
#  Run:  bash run_backend.sh
# ─────────────────────────────────────────────────────────────────
set -e

echo ""
echo "  ╔══════════════════════════════════════╗"
echo "  ║       Atlas Terminal  Backend        ║"
echo "  ╚══════════════════════════════════════╝"
echo ""

# ── Check Python ─────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo "  ERROR: python3 not found."
    echo "  Install Python 3.11+ from https://www.python.org/downloads/"
    exit 1
fi

python3 --version

# ── Install if needed ─────────────────────────────────────────────
if ! python3 -c "import atlas_core" &>/dev/null; then
    echo ""
    echo "  Installing Atlas Terminal (first run only)..."
    pip3 install -e ".[server]"
fi

# ── Create .env if missing ────────────────────────────────────────
if [ ! -f ".env" ] && [ -f ".env.example" ]; then
    cp .env.example .env
    echo "  Created .env from .env.example"
    echo "  Tip: Edit .env to add your free API keys for richer data."
fi

echo ""
echo "  Starting backend at http://localhost:8000  (Ctrl+C to stop)"
echo ""
python3 -m atlas_app.backend.main
