param([switch]$Test)
$ErrorActionPreference = 'Stop'
$taskSource = [IO.Path]::GetFullPath($PSScriptRoot)
$taskHasher = [Security.Cryptography.SHA256]::Create()
$taskHash = [BitConverter]::ToString($taskHasher.ComputeHash([Text.Encoding]::UTF8.GetBytes($taskSource))).Replace('-','').Substring(0,12)
$taskHasher.Dispose()
$taskLinkRoot = Join-Path $env:LOCALAPPDATA 'EduDBBuild'
New-Item -ItemType Directory -Force -Path $taskLinkRoot | Out-Null
$taskLink = Join-Path $taskLinkRoot $taskHash
if (Test-Path -LiteralPath $taskLink) {
    $taskItem = Get-Item -LiteralPath $taskLink
    if ($taskItem.LinkType -ne 'Junction' -or [IO.Path]::GetFullPath($taskItem.Target[0]) -ne $taskSource) { throw 'Build junction does not point to this source folder.' }
} else { New-Item -ItemType Junction -Path $taskLink -Target $taskSource | Out-Null }
$taskBuild = Join-Path $taskLink '.build\windows'
& cmake -S $taskLink -B $taskBuild -G Ninja -DCMAKE_BUILD_TYPE=Release
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& cmake --build $taskBuild --parallel 2
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($Test) { & ctest --test-dir $taskBuild --output-on-failure; exit $LASTEXITCODE }
