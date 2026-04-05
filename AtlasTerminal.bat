@echo off
setlocal EnableDelayedExpansion
REM ═══════════════════════════════════════════════════════════════════════════
REM  Atlas Terminal — One-Click Launcher  (Windows)
REM  Double-click this file to start Atlas Terminal.
REM  Your browser will open automatically when the app is ready.
REM ═══════════════════════════════════════════════════════════════════════════

title Atlas Terminal

REM ── Working directory = folder containing this script ────────────────────
cd /d "%~dp0"

echo.
echo  ╔═══════════════════════════════════════════════════════╗
echo  ║          Atlas Terminal  —  Starting Up               ║
echo  ╚═══════════════════════════════════════════════════════╝
echo.

REM ────────────────────────────────────────────────────────────────────────
REM  STEP 1: Check Python
REM ────────────────────────────────────────────────────────────────────────
where python >nul 2>&1
if errorlevel 1 (
    echo  ┌─────────────────────────────────────────────────────┐
    echo  │  Python not found.                                  │
    echo  │                                                     │
    echo  │  Please install Python 3.11 or newer:               │
    echo  │  https://www.python.org/downloads/                  │
    echo  │                                                     │
    echo  │  During install, tick "Add Python to PATH"          │
    echo  │  then re-run this file.                             │
    echo  └─────────────────────────────────────────────────────┘
    echo.
    pause
    exit /b 1
)

for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo  Python !PYVER! found.

REM ────────────────────────────────────────────────────────────────────────
REM  STEP 2: Install Python packages (first run only)
REM ────────────────────────────────────────────────────────────────────────
python -c "import atlas_core" >nul 2>&1
if errorlevel 1 (
    echo.
    echo  Installing Python packages (first run — takes 1-3 minutes)...
    echo  Please wait, do not close this window.
    echo.
    pip install -e ".[server]" --quiet
    if errorlevel 1 (
        echo.
        echo  ┌─────────────────────────────────────────────────────┐
        echo  │  Package installation failed.                       │
        echo  │                                                     │
        echo  │  Please check your internet connection and try      │
        echo  │  double-clicking AtlasTerminal.bat again.           │
        echo  │                                                     │
        echo  │  Error details appear above this box.               │
        echo  └─────────────────────────────────────────────────────┘
        echo.
        pause
        exit /b 1
    )
    echo  Packages installed successfully.
)

REM ────────────────────────────────────────────────────────────────────────
REM  STEP 3: Create .env if it doesn't exist
REM ────────────────────────────────────────────────────────────────────────
if not exist ".env" (
    if exist ".env.example" (
        copy ".env.example" ".env" >nul
        echo.
        echo  Created configuration file .env
        echo  Tip: Open .env in Notepad to add optional API keys for richer data.
    )
)

REM ────────────────────────────────────────────────────────────────────────
REM  STEP 4: Build frontend (first run, or if dist is missing)
REM ────────────────────────────────────────────────────────────────────────
if not exist "atlas_app\frontend\dist\index.html" (
    where node >nul 2>&1
    if errorlevel 1 (
        echo.
        echo  ┌─────────────────────────────────────────────────────┐
        echo  │  Node.js not found — needed to build the interface. │
        echo  │                                                     │
        echo  │  Please install Node.js 18+ from:                  │
        echo  │  https://nodejs.org/                                │
        echo  │                                                     │
        echo  │  Then re-run this file.                             │
        echo  └─────────────────────────────────────────────────────┘
        echo.
        pause
        exit /b 1
    )

    echo.
    echo  Building the interface (first run — takes ~30 seconds)...
    echo  Please wait, do not close this window.
    echo.

    cd atlas_app\frontend

    if not exist "node_modules" (
        echo  Downloading interface packages...
        call npm install --silent 2>&1
        if errorlevel 1 (
            cd ..\..
            echo.
            echo  npm install failed.  Check your internet connection.
            pause
            exit /b 1
        )
    )

    call npm run build 2>&1
    if errorlevel 1 (
        cd ..\..
        echo.
        echo  ┌─────────────────────────────────────────────────────┐
        echo  │  Frontend build failed.                             │
        echo  │  Error details appear above.                        │
        echo  └─────────────────────────────────────────────────────┘
        pause
        exit /b 1
    )
    cd ..\..
    echo  Interface built successfully.
)

REM ────────────────────────────────────────────────────────────────────────
REM  STEP 5: Launch!
REM ────────────────────────────────────────────────────────────────────────
echo.
echo  ╔═══════════════════════════════════════════════════════╗
echo  ║  Atlas Terminal is starting...                        ║
echo  ║                                                       ║
echo  ║  Your browser will open automatically.                ║
echo  ║  If it doesn't, go to:  http://localhost:8000         ║
echo  ║                                                       ║
echo  ║  Press Ctrl+C or close this window to stop.           ║
echo  ╚═══════════════════════════════════════════════════════╝
echo.

python -m atlas_app.backend.main

if errorlevel 1 (
    echo.
    echo  ┌─────────────────────────────────────────────────────┐
    echo  │  Atlas Terminal stopped unexpectedly.               │
    echo  │                                                     │
    echo  │  Common fixes:                                      │
    echo  │   - Port 8000 in use: close other apps using it    │
    echo  │   - Missing packages: delete atlas.duckdb and      │
    echo  │     re-run this file                                │
    echo  │                                                     │
    echo  │  Copy the error above and report it at:            │
    echo  │  github.com/kedar-chintalapati/AtlasTerminal        │
    echo  └─────────────────────────────────────────────────────┘
    echo.
    pause
)
