@echo off
cd /d "%~dp0"
title MTR-Saude v1.0
start "" cmd /c "timeout /t 2 /nobreak >nul && start "" "%~dp0frontend\index.html""
python "%~dp0run.py"
