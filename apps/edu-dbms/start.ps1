param([ValidateRange(1,65535)][int]$Port = 8765, [string]$Data = '')
$ErrorActionPreference = 'Stop'
$taskRoot = $PSScriptRoot
if (-not $Data) { $Data = Join-Path $taskRoot 'data-v41' }
$taskBinary = Join-Path $taskRoot 'bin\edudb.exe'
if (-not (Test-Path -LiteralPath $taskBinary)) { $taskBinary = Join-Path $taskRoot '.build\windows\edudb.exe' }
if (-not (Test-Path -LiteralPath $taskBinary)) { $taskBinary = Join-Path $taskRoot '.build\edudb.exe' }
if (-not (Test-Path -LiteralPath $taskBinary)) { throw 'Build first: cmake -S . -B .build -G Ninja; cmake --build .build' }
Write-Host "EduDB 4.1: http://127.0.0.1:$Port (Ctrl+C to stop)"
Write-Host "Program: $taskBinary"
Write-Host "Data: $Data"
& $taskBinary serve --data $Data --web (Join-Path $taskRoot 'web') --port $Port
if ($LASTEXITCODE -ne 0) { Write-Host 'Startup failed. Check the port, data-directory lock and preceding error. Use -Port 8766 for an independent port.' -ForegroundColor Red }
exit $LASTEXITCODE
