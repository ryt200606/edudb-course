param([ValidateRange(1,65535)][int]$Port = 8765, [string]$Data = '', [string]$Binary = '', [switch]$PrintBinary)
$ErrorActionPreference = 'Stop'
$taskRoot = $PSScriptRoot
if (-not $Data) { $Data = Join-Path $taskRoot 'data-v41' }
$taskBinary = & (Join-Path $taskRoot 'resolve-program.ps1') -Binary $Binary
if ($PrintBinary) { Write-Output $taskBinary; exit 0 }
Write-Host "EduDB 4.1: http://127.0.0.1:$Port (Ctrl+C to stop)"
Write-Host "Program: $taskBinary"
Write-Host "Data: $Data"
& $taskBinary serve --data $Data --web (Join-Path $taskRoot 'web') --port $Port
if ($LASTEXITCODE -ne 0) { Write-Host 'Startup failed. Check the port, data-directory lock and preceding error. Use -Port 8766 for an independent port.' -ForegroundColor Red }
exit $LASTEXITCODE
