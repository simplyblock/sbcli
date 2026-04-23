# Monitor soak test running on mgmt node. Checks every 2 minutes.
# Usage: powershell -File monitor_soak.ps1 <mgmt_ip> [logfile]

param(
    [Parameter(Mandatory=$true)][string]$MgmtIp,
    [string]$LogFile = "soak_monitor_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
)

$Key = "C:\ssh\mtes01.pem"

function Log($msg) {
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $msg"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

function SshCmd($cmd) {
    $result = & ssh -i $Key -o StrictHostKeyChecking=no -o ConnectTimeout=10 "ec2-user@$MgmtIp" $cmd 2>$null
    return $result
}

Log "Monitoring soak on $MgmtIp, logging to $LogFile"

while ($true) {
    $soakPid = SshCmd "pgrep -f aws_dual_node_outage_soak_mixed"
    if (-not $soakPid) {
        Log "SOAK STOPPED - process not running"
        $tail = SshCmd "tail -5 ~/soak_mixed.out"
        foreach ($line in $tail) { Log "  $line" }
        Log "EXIT"
        break
    }

    $iterations = SshCmd "grep -c 'completed successfully' ~/soak_mixed.out"
    if (-not $iterations) { $iterations = "0" }

    $errors = SshCmd "grep -c 'ERROR:' ~/soak_mixed.out"
    if (-not $errors) { $errors = "0" }

    $activity = SshCmd "grep -E 'iteration|Waiting|Cluster stable|ERROR|fio.*stopped|Applying outage|completed successfully' ~/soak_mixed.out | tail -1"

    Log "OK pid=$soakPid iterations=$iterations errors=$errors | $activity"

    $fioErrors = SshCmd "grep -c 'fio.*stopped unexpectedly' ~/soak_mixed.out"
    if ($fioErrors -and [int]$fioErrors -gt 0) {
        Log "WARNING: $fioErrors fio job(s) stopped unexpectedly"
    }

    Start-Sleep -Seconds 120
}
