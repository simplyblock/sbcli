#!/usr/bin/env bash
# Launch the multipath soak detached from the SSH session.
#
# START_ITERATION resumes an interrupted run at the pair it died on rather than
# repeating the ones already covered: pair distance rotation and the NIC-phase
# schedule both key off the iteration number. The loop still ends at
# --iterations, so START_ITERATION=21 runs pairs 21..75.
set -u
cd "$HOME"
TS=$(date +%Y%m%d_%H%M%S)
echo "$TS" > "$HOME/soak_ts"
LOG="$HOME/soak_mp_${TS}.log"
OUT="$HOME/soak_mp_${TS}.out"
setsid nohup python3 "$HOME/aws_dual_node_outage_soak_multipath.py" \
    --run-on-mgmt \
    --metadata "$HOME/cluster_metadata_mp.json" \
    --ssh-key "$HOME/.ssh/mtes01.pem" \
    --iterations 75 \
    --start-iteration "${START_ITERATION:-1}" \
    --runtime 52000 \
    --log-file "$LOG" \
    > "$OUT" 2>&1 < /dev/null &
PID=$!
echo "$PID" > "$HOME/soak_pid"
sleep 3
echo "launched pid=$PID ts=$TS start_iteration=${START_ITERATION:-1}"
echo "log=$LOG"
ps -p "$PID" -o pid=,etime=,cmd= | cut -c1-100
