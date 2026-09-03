#!/usr/bin/env bash
# Launch the outage soak on a NON-multipath (single data NIC) 2+2 cluster.
#
# Reuses the multipath soak deliberately: its outage timing is what we want --
# one thread per node with its own mgmt connection, each node holding its own
# outage and issuing its own restart at its own boundary, so a pair genuinely
# overlaps. The mixed-churn soak applies both outages first and then restarts
# serially against the CP's concurrent-restart guard, which is much slower.
# (Mixed churn also has NO verify at all, so it cannot see data corruption.)
#
# Non-multipath adaptations, both required:
#   --data-nics eth0   the base deploy adds nodes on eth0, so there is ONE path;
#                      path-count, listener-count and client-path expectations
#                      derive from this, and the active_active policy assertions
#                      are skipped below two NICs.
#   --no-nic-phase     phase 1 takes one data NIC down on every node at once,
#                      which with a single NIC isolates the whole cluster.
#                      Note --nic-phase-every 0 does NOT disable it.
#
# Write history is ON by default: a verify failure is only half-diagnosable
# without knowing which writes touched that offset. Bounded by
# --iolog-keep-hours (default 1) via iolog_trimmer.sh, because unbounded it is
# gigabytes per volume per hour. Set WRITE_IOLOG=0 to turn it off.
#
# Env: ITERATIONS, RUNTIME, START_ITERATION, RESTART_TIMEOUT, PLACEMENT_DUMPS,
#      DATA_NIC, WRITE_IOLOG (default 1), IOLOG_KEEP_HOURS (default 1).
set -u
cd "$HOME"
TS=$(date +%Y%m%d_%H%M%S)
echo "$TS" > "$HOME/soak_ts"
LOG="$HOME/soak_base_${TS}.log"
OUT="$HOME/soak_base_${TS}.out"

IOLOG_ARGS=""
if [ "${WRITE_IOLOG:-1}" != "0" ]; then
    IOLOG_ARGS="--write-iolog --iolog-keep-hours ${IOLOG_KEEP_HOURS:-1}"
fi

setsid nohup python3 "$HOME/aws_dual_node_outage_soak_multipath.py" \
    --run-on-mgmt \
    --metadata "$HOME/cluster_metadata_base.json" \
    --ssh-key "$HOME/.ssh/mtes01.pem" \
    --data-nics "${DATA_NIC:-eth0}" \
    --no-nic-phase \
    --iterations "${ITERATIONS:-75}" \
    --start-iteration "${START_ITERATION:-1}" \
    ${PLACEMENT_DUMPS:+--placement-dumps} \
    ${RESTART_TIMEOUT:+--restart-timeout $RESTART_TIMEOUT} \
    ${IOLOG_ARGS} \
    --runtime "${RUNTIME:-52000}" \
    --log-file "$LOG" \
    > "$OUT" 2>&1 < /dev/null &
PID=$!
echo "$PID" > "$HOME/soak_pid"
sleep 3
echo "launched pid=$PID ts=$TS"
echo "log=$LOG"
echo "out=$OUT"
echo "iolog: ${IOLOG_ARGS:-DISABLED}"
ps -p "$PID" -o pid=,etime=,cmd= | cut -c1-120
