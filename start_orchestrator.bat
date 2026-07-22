@echo off
title SCADA Orchestrator
echo ===================================================
echo Starting SCADA Orchestrator...
echo Project: c:\Users\opnc\Desktop\SCADA VCOM Automation\VCOM Automation
echo Time: %date% %time%
echo ===================================================
cd /d "c:\Users\opnc\Desktop\SCADA VCOM Automation\VCOM Automation"
C:\Python314\python.exe -u run_monitor.py
echo ===================================================
echo Orchestrator stopped. Press any key to exit.
echo ===================================================
pause
