@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0demo-self-check.ps1" %*
if errorlevel 1 pause
