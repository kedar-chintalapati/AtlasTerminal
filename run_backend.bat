@echo off
REM ─────────────────────────────────────────────────────────────────
REM  Atlas Terminal — Backend Launcher (Windows)
REM  Double-click this file to start the API server.
REM ─────────────────────────────────────────────────────────────────

title Atlas Terminal — Backend

echo.
echo  ╔══════════════════════════════════════╗
echo  ║       Atlas Terminal  Backend        ║
echo  ╚══════════════════════════════════════╝
echo.

REM ── Check Python ─────────────────────────────────────────────────
where python >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found.
    echo  Please install Python 3.11+ from https://www.python.org/downloads/
    echo  Make sure to tick "Add Python to PATH" during installation.
    pause
    exit /b 1
)

python --version

REM ── Install / check dependencies ─────────────────────────────────
echo.
echo  Checking dependencies...
pip show atlas-core >nul 2>&1
if errorlevel 1 (
    echo  Installing Atlas Terminal (first run only, may take a minute)...
    pip install -e ".[server]"
    if errorlevel 1 (
        echo.
        echo  ERROR: Dependency installation failed.
        echo  Try running:  pip install -e ".[server]"  in this folder.
        pause
        exit /b 1
    )
)

REM ── Create .env if missing ────────────────────────────────────────
if not exist ".env" (
    if exist ".env.example" (
        copy ".env.example" ".env" >nul
        echo  Created .env from .env.example
        echo  Tip: Edit .env to add your free API keys for richer data.
    )
)

REM ── Start server ─────────────────────────────────────────────────
echo.
echo  Starting Atlas Terminal backend at http://localhost:8000
echo  Press Ctrl+C to stop.
echo.
python -m atlas_app.backend.main

if errorlevel 1 (
    echo.
    echo  The server stopped with an error.  See the message above.
    echo  Common fixes:
    echo    - Port 8000 already in use: close the other process or set PORT=8001 in .env
    echo    - Missing package: run  pip install -e ".[server]"
    echo.
    pause
)
