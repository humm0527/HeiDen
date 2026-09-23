@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo Please run the setup file in this folder first.
  pause
  exit /b 1
)
".venv\Scripts\python.exe" -B launcher.py --mode peer
if errorlevel 1 pause
