@echo off
cd /d %~dp0
set "PYTHONPATH=%~dp0runtime\Lib\site-packages"
.\runtime\pythonw.exe .\app\mtga_KR_patcher.py

