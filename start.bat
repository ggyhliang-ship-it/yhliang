@echo off
echo Starting ATS Data Monitor Platform...

cd /d D:\Project

REM Kill existing processes
taskkill /F /IM python.exe 2>nul

REM Start backend
start "Backend" python backend.py

REM Wait a bit
timeout /t 2 /nobreak >nul

REM Start frontend
start "Frontend" python -m http.server 8080

REM Open browser
start http://localhost:8080

echo Services started!
echo Backend: http://localhost:8000
echo Frontend: http://localhost:8080