param([ValidateRange(1,65535)][int]$Port=18765,[string]$Binary='')
$ErrorActionPreference='Stop'
$taskRoot=$PSScriptRoot
$taskBinary=& (Join-Path $taskRoot 'resolve-program.ps1') -Binary $Binary
$taskRun=Join-Path $taskRoot ('demo-runs\self-check-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')+'-'+[Guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($taskRun)
$taskUrl="http://127.0.0.1:$Port"
$taskProcess=$null;$taskSession=$null;$taskStarted=$null;$taskListening=$null;$taskChecks=New-Object Collections.Generic.List[object]
function Invoke-TaskApi([string]$Path,[string]$Method='GET',$Body=$null){
    if($Method -ne 'GET'){Assert-TaskOwnedServer}
    $taskArgs=@{Uri=$taskUrl+'/api/v1'+$Path;Method=$Method;TimeoutSec=15}
    if($null -ne $Body){$taskArgs.ContentType='application/json; charset=utf-8';$taskArgs.Body=[Text.Encoding]::UTF8.GetBytes(($Body|ConvertTo-Json -Compress -Depth 12))}
    Invoke-RestMethod @taskArgs
}
# The child's flushed event is emitted only after bind_to_port succeeded.
# A preliminary port probe cannot prove ownership: another listener may win
# between releasing the probe and starting the child. Never use such a probe
# or a health response alone to authorize this run's writes.
function Assert-TaskOwnedChild {
    if($null -eq $taskProcess -or $null -eq $taskStarted){throw 'No owned child process.'}
    $taskProcess.Refresh()
    if($taskProcess.HasExited -or $taskProcess.StartTime -ne $taskStarted){throw 'Owned private server is no longer running; no API write is sent.'}
}
function Assert-TaskOwnedServer {
    Assert-TaskOwnedChild
    if($null -eq $taskListening){throw 'No successful bind event from the owned child; no API write is sent.'}
    $taskOwnerHealth=Invoke-RestMethod -Uri ($taskUrl+'/api/v1/health') -TimeoutSec 3
    Assert-TaskOwnedChild
    if($taskOwnerHealth.ok -ne $true -or $taskOwnerHealth.product -ne 'EduDB' -or $taskOwnerHealth.version -ne '4.1.0' -or $taskOwnerHealth.instance_id -cne $taskListening.instance_id){throw 'Expected owned EduDB 4.1.0 health matching its successful bind event; no API write is sent.'}
}
function Invoke-TaskSql([string]$Sql){
    $taskReply=Invoke-TaskApi ('/sessions/'+$taskSession+'/sql') 'POST' @{sql=$Sql}
    if(-not $taskReply.ok){throw ($taskReply.error|ConvertTo-Json -Compress)}
    $taskChecks.Add(@{sql=$Sql;result=$taskReply})
    return $taskReply
}
try{
    $taskArguments='serve --data "'+(Join-Path $taskRun 'data')+'" --web "'+(Join-Path $taskRoot 'web')+'" --port '+$Port
    $taskProcess=Start-Process -FilePath $taskBinary -ArgumentList $taskArguments -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $taskRun 'server.log') -RedirectStandardError (Join-Path $taskRun 'server-error.log')
    $taskStarted=$taskProcess.StartTime
    $taskDeadline=[DateTime]::UtcNow.AddSeconds(25)
    do{
        Assert-TaskOwnedChild
        $taskLog=Join-Path $taskRun 'server.log'
        if(Test-Path -LiteralPath $taskLog){
            foreach($taskLine in @(Get-Content -LiteralPath $taskLog -Encoding UTF8)){
                try{$taskEvent=$taskLine|ConvertFrom-Json}catch{continue}
                if($taskEvent.event -eq 'listening'){
                    if([string]::IsNullOrWhiteSpace($taskEvent.instance_id) -or $taskEvent.url -cne $taskUrl -or [IO.Path]::GetFullPath($taskEvent.data) -ine [IO.Path]::GetFullPath((Join-Path $taskRun 'data'))){throw 'Owned bind event has unexpected identity, URL or data directory.'}
                    $taskListening=$taskEvent;break
                }
            }
        }
        if($null -ne $taskListening){break}
        Start-Sleep -Milliseconds 100
    }while([DateTime]::UtcNow -lt $taskDeadline)
    if($null -eq $taskListening){throw 'No successful owned bind event within 25 seconds; no endpoint was used.'}
    do{
        Assert-TaskOwnedChild
        try{$taskHealth=Invoke-TaskApi '/health';break}catch{Start-Sleep -Milliseconds 100}
    }while([DateTime]::UtcNow -lt $taskDeadline)
    Assert-TaskOwnedServer
    if($null -eq $taskHealth -or $taskHealth.instance_id -cne $taskListening.instance_id){throw 'Server health/bind instance mismatch.'}
    $taskSession=(Invoke-TaskApi '/sessions' 'POST' @{}).session_id
    if([string]::IsNullOrWhiteSpace($taskSession) -or -not $taskSession.StartsWith($taskHealth.instance_id,[StringComparison]::Ordinal)){throw 'Server/session instance mismatch.'}
    $null=Invoke-TaskSql 'CREATE TABLE demo(id INTEGER PRIMARY KEY,name VARCHAR(24),score REAL,g INTEGER)'
    $null=Invoke-TaskSql "INSERT INTO demo VALUES(1,'Alice',90,1),(2,'Bob',NULL,1),(3,'Chen',82.5,2)"
    $null=Invoke-TaskSql 'ALTER TABLE demo ADD COLUMN active BOOLEAN DEFAULT TRUE'
    $null=Invoke-TaskSql 'ALTER TABLE demo RENAME COLUMN name TO student'
    $null=Invoke-TaskSql 'CREATE INDEX ix_demo_g ON demo(g)'
    $null=Invoke-TaskSql 'UPDATE demo SET score=96 WHERE id=1'
    $null=Invoke-TaskSql 'EXPLAIN ANALYZE SELECT id,student FROM demo WHERE id=1 LIMIT 1'
    $null=Invoke-TaskSql 'SELECT g,COUNT(*),AVG(score) FROM demo GROUP BY g HAVING COUNT(*)>0 ORDER BY g'
    $null=Invoke-TaskSql 'BEGIN'
    $null=Invoke-TaskSql 'UPDATE demo SET score=0 WHERE id=1'
    $null=Invoke-TaskSql 'ROLLBACK'
    $taskRow=Invoke-TaskSql 'SELECT score FROM demo WHERE id=1'
    if($taskRow.rows[0][0] -ne 96){throw 'Rollback result mismatch.'}
    $null=Invoke-TaskSql 'DELETE FROM demo WHERE id=3'
    $null=Invoke-TaskSql 'ALTER TABLE demo DROP COLUMN active'
    $taskCount=Invoke-TaskSql 'SELECT COUNT(*) FROM demo'
    if($taskCount.rows[0][0] -ne '2'){throw 'Final row count mismatch.'}
    $null=Invoke-TaskApi ('/sessions/'+$taskSession) 'DELETE';$taskSession=$null
    $taskCheckpoint=Invoke-TaskApi '/checkpoint' 'POST' @{}
    if($taskCheckpoint.status -ne 'PASS'){throw 'Checkpoint failed.'}
    $taskHasher=[Security.Cryptography.SHA256]::Create()
    $taskHashStream=[IO.File]::OpenRead($taskBinary)
    try{$taskBinaryHash=[BitConverter]::ToString($taskHasher.ComputeHash($taskHashStream)).Replace('-','').ToLowerInvariant()}finally{$taskHashStream.Dispose();$taskHasher.Dispose()}
    $taskReceipt=@{status='PASS';health=$taskHealth;ownership=@{child_pid=$taskProcess.Id;child_start=$taskStarted;listening=$taskListening;scope='Owned child successful bind and per-write health recheck; session instance prefix verified.'};binary_sha256=$taskBinaryHash;checks=@($taskChecks.ToArray());checkpoint=$taskCheckpoint;scope='Private fresh demo directory only; no browser window or user database used.'}
    $taskReceipt|ConvertTo-Json -Depth 35|Set-Content -LiteralPath (Join-Path $taskRun 'result.json') -Encoding UTF8
    Write-Host "PASS - EduDB 4.1.0, $($taskChecks.Count) SQL operations, rollback and checkpoint."
    Write-Host "Evidence: $taskRun"
}catch{
    @{status='FAIL';error=$_.Exception.Message;checks=@($taskChecks.ToArray());scope='Private demo self-check; failure preserved.'}|ConvertTo-Json -Depth 35|Set-Content -LiteralPath (Join-Path $taskRun 'result.json') -Encoding UTF8
    throw
}finally{
    if($taskSession){try{$null=Invoke-TaskApi ('/sessions/'+$taskSession) 'DELETE'}catch{}}
    if($taskProcess -and -not $taskProcess.HasExited){
        $taskLive=Get-Process -Id $taskProcess.Id -ErrorAction SilentlyContinue
        if($taskLive -and $taskLive.StartTime -eq $taskStarted){Stop-Process -Id $taskLive.Id; $taskProcess.WaitForExit(5000)|Out-Null}
    }
}
