"""
Atlas Terminal -- cross-platform launcher.

Called by AtlasTerminal.bat (Windows) and AtlasTerminal.sh (Mac/Linux).
Handles: version check, pip install, npm build, server start.
"""
import os
import subprocess
import sys
from pathlib import Path

# Force UTF-8 output so box-drawing characters work on Windows terminals.
# If that fails (very old Python), fall back to replacing unknown chars.
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT = Path(__file__).parent.resolve()
FRONTEND_DIR = ROOT / "atlas_app" / "frontend"
DIST_INDEX = FRONTEND_DIR / "dist" / "index.html"
ENV_FILE = ROOT / ".env"
ENV_EXAMPLE = ROOT / ".env.example"


def sep():
    print("  " + "=" * 54)


def info(msg: str):
    print(f"  {msg}")


def error_box(lines: list):
    print()
    sep()
    for line in lines:
        print(f"  {line}")
    sep()
    print()


def run(cmd, **kwargs) -> int:
    return subprocess.call(cmd, **kwargs)


# ---------------------------------------------------------------------------
# Step 1 -- Python version
# ---------------------------------------------------------------------------
def check_python_version():
    if sys.version_info < (3, 11):
        error_box([
            "ERROR: Python 3.11 or newer is required.",
            f"You have Python {sys.version.split()[0]}.",
            "",
            "Download Python 3.11+ (free):",
            "  https://www.python.org/downloads/",
            "",
            "During install, tick 'Add Python to PATH'.",
            "Then double-click AtlasTerminal.bat again.",
        ])
        sys.exit(1)
    info(f"Python {sys.version.split()[0]}  OK")


# ---------------------------------------------------------------------------
# Step 2 -- Python packages
# ---------------------------------------------------------------------------
def ensure_packages():
    try:
        import atlas_core  # noqa: F401
        info("Python packages  OK")
        return
    except ImportError:
        pass

    info("Installing Python packages (first run -- 1-3 minutes)...")
    info("Please wait, do not close this window.")
    ret = run(
        [sys.executable, "-m", "pip", "install", "-e", ".[server]",
         "--quiet", "--no-warn-script-location"],
        cwd=ROOT,
    )
    if ret != 0:
        error_box([
            "ERROR: Package installation failed.",
            "",
            "Possible fixes:",
            " 1. Check your internet connection.",
            " 2. Try right-click -> Run as Administrator.",
            " 3. Run this command in a terminal:",
            "      pip install -e \".[server]\"",
        ])
        sys.exit(1)
    info("Python packages installed.")


# ---------------------------------------------------------------------------
# Step 3 -- .env
# ---------------------------------------------------------------------------
def ensure_env():
    if not ENV_FILE.exists() and ENV_EXAMPLE.exists():
        import shutil
        shutil.copy(ENV_EXAMPLE, ENV_FILE)
        info("Created .env from .env.example")
        info("Tip: Open .env in Notepad to add optional API keys.")


# ---------------------------------------------------------------------------
# Step 4 -- Frontend build
# ---------------------------------------------------------------------------
def check_node() -> bool:
    result = subprocess.run(
        ["node", "--version"], capture_output=True, text=True
    )
    return result.returncode == 0


def ensure_frontend_built():
    if DIST_INDEX.exists():
        info("Frontend build  OK")
        return

    if not check_node():
        error_box([
            "ERROR: Node.js not found -- needed to build the interface.",
            "",
            "Install Node.js 18+ (free):",
            "  https://nodejs.org/",
            "",
            "After installing, double-click AtlasTerminal.bat again.",
        ])
        sys.exit(1)

    info("Building the interface (first run -- about 30 seconds)...")
    info("Please wait, do not close this window.")

    node_modules = FRONTEND_DIR / "node_modules"
    if not node_modules.exists():
        info("Downloading interface packages...")
        ret = run(["npm", "install", "--silent"], cwd=FRONTEND_DIR)
        if ret != 0:
            error_box([
                "ERROR: npm install failed.",
                "Check your internet connection and try again.",
            ])
            sys.exit(1)

    ret = run(["npm", "run", "build"], cwd=FRONTEND_DIR)
    if ret != 0:
        error_box(["ERROR: Frontend build failed. See details above."])
        sys.exit(1)

    info("Interface built.")


# ---------------------------------------------------------------------------
# Step 5 -- Launch server
# ---------------------------------------------------------------------------
def start_server():
    print()
    sep()
    info("Atlas Terminal is starting!")
    info("")
    info("Your browser will open automatically.")
    info("If it doesn't, go to:  http://localhost:8000")
    info("")
    info("Press Ctrl+C to stop Atlas Terminal.")
    sep()
    print()

    env = os.environ.copy()
    env["ATLAS_OPEN_BROWSER"] = "1"

    # Load .env values into environment
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env.setdefault(k.strip(), v.strip())

    try:
        subprocess.run(
            [sys.executable, "-m", "atlas_app.backend.main"],
            cwd=ROOT,
            env=env,
        )
    except KeyboardInterrupt:
        info("Atlas Terminal stopped.")


# ---------------------------------------------------------------------------
def main():
    print()
    sep()
    info("    Atlas Terminal  --  Starting Up")
    sep()
    print()

    check_python_version()
    ensure_packages()
    ensure_env()
    ensure_frontend_built()
    start_server()


if __name__ == "__main__":
    main()
