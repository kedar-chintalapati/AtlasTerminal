#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
#  Atlas Terminal — One-Click Launcher  (macOS / Linux)
#  Run:  bash AtlasTerminal.sh
#  Or make executable once:  chmod +x AtlasTerminal.sh  then  ./AtlasTerminal.sh
# ═══════════════════════════════════════════════════════════════════════════

set -e
cd "$(dirname "$0")"   # always run from the project root

echo ""
echo "  ╔═══════════════════════════════════════════════════════╗"
echo "  ║          Atlas Terminal  —  Starting Up               ║"
echo "  ╚═══════════════════════════════════════════════════════╝"
echo ""

# ── STEP 1: Check Python ─────────────────────────────────────────────────
PYTHON=""
for py in python3 python; do
    if command -v "$py" &>/dev/null; then
        VER=$("$py" -c "import sys; print(sys.version_info[:2])" 2>/dev/null)
        if "$py" -c "import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)" 2>/dev/null; then
            PYTHON="$py"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    echo "  ┌─────────────────────────────────────────────────────┐"
    echo "  │  Python 3.11+ not found.                           │"
    echo "  │                                                     │"
    echo "  │  Install it from: https://www.python.org/downloads/ │"
    echo "  │  or via Homebrew: brew install python               │"
    echo "  └─────────────────────────────────────────────────────┘"
    exit 1
fi
echo "  Python: $($PYTHON --version)"

# ── STEP 2: Install Python packages (first run) ───────────────────────────
if ! "$PYTHON" -c "import atlas_core" 2>/dev/null; then
    echo ""
    echo "  Installing Python packages (first run — 1-3 minutes)..."
    "$PYTHON" -m pip install -e ".[server]" --quiet
    echo "  Packages installed."
fi

# ── STEP 3: Create .env if missing ────────────────────────────────────────
if [ ! -f ".env" ] && [ -f ".env.example" ]; then
    cp .env.example .env
    echo "  Created .env — edit it to add optional API keys."
fi

# ── STEP 4: Build frontend if needed ─────────────────────────────────────
if [ ! -f "atlas_app/frontend/dist/index.html" ]; then
    if ! command -v node &>/dev/null; then
        echo "  ┌─────────────────────────────────────────────────────┐"
        echo "  │  Node.js not found.                                │"
        echo "  │  Install from: https://nodejs.org/                 │"
        echo "  │  or: brew install node                              │"
        echo "  └─────────────────────────────────────────────────────┘"
        exit 1
    fi

    echo ""
    echo "  Building the interface (first run — ~30 seconds)..."
    cd atlas_app/frontend
    [ ! -d "node_modules" ] && npm install --silent
    npm run build
    cd ../..
    echo "  Interface built."
fi

# ── STEP 5: Launch ────────────────────────────────────────────────────────
echo ""
echo "  ╔═══════════════════════════════════════════════════════╗"
echo "  ║  Atlas Terminal is starting...                        ║"
echo "  ║                                                       ║"
echo "  ║  Your browser will open automatically.                ║"
echo "  ║  If it doesn't, go to:  http://localhost:8000         ║"
echo "  ║                                                       ║"
echo "  ║  Press Ctrl+C to stop.                                ║"
echo "  ╚═══════════════════════════════════════════════════════╝"
echo ""

"$PYTHON" -m atlas_app.backend.main
