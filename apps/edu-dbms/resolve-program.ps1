param([string]$Binary = '')
$ErrorActionPreference = 'Stop'
$taskCandidates = if ($Binary) {
    @([IO.Path]::GetFullPath($Binary))
} else {
    @('bin\edudb.exe', '.build\windows\edudb.exe', '.build\edudb.exe') |
        ForEach-Object { Join-Path $PSScriptRoot $_ }
}
foreach ($taskCandidate in $taskCandidates) {
    if (-not (Test-Path -LiteralPath $taskCandidate -PathType Leaf)) { continue }
    try {
        $taskHelp = @(& $taskCandidate help 2>$null)
        if ($LASTEXITCODE -eq 0 -and $taskHelp.Count -gt 0 -and $taskHelp[0] -ceq 'EduDB 4.1.0') {
            return [IO.Path]::GetFullPath($taskCandidate)
        }
    } catch { Write-Verbose "Cannot execute $taskCandidate" }
}
throw 'No runnable EduDB 4.1.0 binary at the requested location. Build this source with build.ps1 or install the 4.1.0 release.'
