#!/usr/bin/env bash
# Atlas Terminal launcher (macOS / Linux)
# Usage:  bash AtlasTerminal.sh
#   OR:   chmod +x AtlasTerminal.sh && ./AtlasTerminal.sh

cd "$(dirname "$0")"

# Find a suitable Python
PYTHON=""
for py in python3.13 python3.12 python3.11 python3 python; do
    if command -v "$py" &>/dev/null; then
        if "$py" -c "import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)" 2>/dev/null; then
            PYTHON="$py"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    echo ""
    echo "  ============================================================"
    echo "  ERROR: Python 3.11 or newer was not found."
    echo ""
    echo "  Install it from: https://www.python.org/downloads/"
    echo "  or via Homebrew: brew install python"
    echo ""
    echo "  Then run:  bash AtlasTerminal.sh  again."
    echo "  ============================================================"
    echo ""
    exit 1
fi

"$PYTHON" launcher.py
