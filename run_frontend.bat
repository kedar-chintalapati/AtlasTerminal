@echo off
REM ─────────────────────────────────────────────────────────────────
REM  Atlas Terminal — Frontend Launcher (Windows)
REM  Double-click this file to start the web UI.
REM  The backend must already be running (run_backend.bat).
REM ─────────────────────────────────────────────────────────────────

title Atlas Terminal — Frontend

echo.
echo  ╔══════════════════════════════════════╗
echo  ║       Atlas Terminal  Frontend       ║
echo  ╚══════════════════════════════════════╝
echo.

REM ── Check Node ───────────────────────────────────────────────────
where node >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Node.js not found.
    echo  Please install Node.js 18+ from https://nodejs.org/
    pause
    exit /b 1
)

node --version

REM ── Change to frontend directory ─────────────────────────────────
cd /d "%~dp0atlas_app\frontend"

REM ── Install npm packages if needed ───────────────────────────────
if not exist "node_modules" (
    echo.
    echo  Installing frontend packages (first run only, may take a minute)...
    npm install
    if errorlevel 1 (
        echo.
        echo  ERROR: npm install failed.  Try:
        echo    1. Check your internet connection
        echo    2. Run  npm install  manually in atlas_app\frontend\
        pause
        exit /b 1
    )
)

REM ── Start dev server ─────────────────────────────────────────────
echo.
echo  Starting Atlas Terminal UI at http://localhost:5173
echo  Press Ctrl+C to stop.
echo.
npm run dev

pause
