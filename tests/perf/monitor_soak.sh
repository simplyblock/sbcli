#!/bin/bash
# Monitor soak test running on mgmt node. Checks every 2 minutes.
# Usage: bash monitor_soak.sh <mgmt_ip> [logfile]

MGMT_IP="${1:?Usage: monitor_soak.sh <mgmt_ip> [logfile]}"
LOGFILE="${2:-soak_monitor_$(date +%Y%m%d_%H%M%S).log}"
KEY="/c/ssh/mtes01.pem"
SSH="ssh -i $KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@$MGMT_IP"

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg"
    echo "$msg" >> "$LOGFILE"
}

log "Monitoring soak on $MGMT_IP, logging to $LOGFILE"

while true; do
    # Check if soak process is alive
    pid=$($SSH "pgrep -f aws_dual_node_outage_soak_mixed" 2>/dev/null)
    if [ -z "$pid" ]; then
        log "SOAK STOPPED — process not running"
        # Grab last 5 lines for context
        $SSH "tail -5 ~/soak_mixed.out" 2>/dev/null | while read -r line; do log "  $line"; done
        log "EXIT"
        break
    fi

    # Count successful iterations
    iterations=$($SSH "grep -c 'completed successfully' ~/soak_mixed.out" 2>/dev/null || echo 0)

    # Check for errors
    errors=$($SSH "grep -c '^\\[.*\\] ERROR:' ~/soak_mixed.out" 2>/dev/null || echo 0)

    # Get current activity (last meaningful line)
    activity=$($SSH "grep -E 'iteration|Waiting|Cluster stable|ERROR|fio.*stopped|Applying outage|completed successfully' ~/soak_mixed.out | tail -1" 2>/dev/null)

    log "OK pid=$pid iterations=$iterations errors=$errors | $activity"

    # Check for fio errors
    fio_errors=$($SSH "grep -c 'fio.*stopped unexpectedly' ~/soak_mixed.out" 2>/dev/null || echo 0)
    if [ "$fio_errors" -gt 0 ]; then
        log "WARNING: $fio_errors fio job(s) stopped unexpectedly"
    fi

    sleep 120
done
