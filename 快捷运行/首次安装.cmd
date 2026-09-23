@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" goto install
py -3.11 -m venv .venv
if errorlevel 1 py -3 -m venv .venv
if errorlevel 1 (
  echo Please install Python 3.11+ from python.org, including Tcl/Tk and the Python launcher.
  pause
  exit /b 1
)
:install
".venv\Scripts\python.exe" -c "import sys, tkinter; assert sys.version_info >= (3,11)"
if errorlevel 1 goto failed
".venv\Scripts\python.exe" -m pip install -r "..\01_risk_events\requirements.txt" -r "..\01_risk_events\requirements-rqdata.txt"
if errorlevel 1 goto failed
".venv\Scripts\python.exe" -c "import tkinter, pandas, duckdb, openpyxl, yaml, rqdatac; print('Environment ready.')"
if errorlevel 1 goto failed
echo Installation completed. You can now close this window and double-click a launcher.
pause
exit /b 0
:failed
echo Installation failed. Keep the error above for troubleshooting.
pause
exit /b 1
