@echo off
cd /d "%~dp0"
title Atlas Terminal

echo.
echo  Atlas Terminal is starting...
echo.

REM Check if python is available
python --version >nul 2>&1
if errorlevel 1 goto NoPython

REM Python found - let the Python launcher handle everything
python launcher.py
if errorlevel 1 (
    echo.
    echo  Atlas Terminal stopped. See the message above.
    echo.
    pause
)
goto End

:NoPython
echo.
echo  ============================================================
echo  ERROR: Python was not found on your computer.
echo.
echo  Please install Python 3.11 or newer (free):
echo    https://www.python.org/downloads/
echo.
echo  IMPORTANT: During install, check the box that says
echo  "Add Python to PATH" before clicking Install.
echo.
echo  After installing Python, double-click AtlasTerminal.bat again.
echo  ============================================================
echo.
pause

:End
