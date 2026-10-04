#!/usr/bin/env bash
# Launch the outage soak on a NON-multipath (single data NIC) 2+2 cluster.
#
# Same script as the multipath soak -- we deliberately reuse it rather than the
# mixed-churn soak, because its outage timing is the part we want: one thread
# per node with its own mgmt connection, each node holding its own outage and
# issuing its own restart at its own boundary, so the pair genuinely overlaps.
# The mixed-churn soak applies both outages first and then restarts serially,
# retrying against the CP's concurrent-restart guard, which is far slower.
#
# Non-multipath adaptations (both needed, see the soak's own --help):
#   --data-nics eth0   the base deploy adds nodes on eth0, so there is ONE path;
#                      path-count, listener-count and client-path expectations
#                      all derive from this, and the active_active policy
#                      assertions are skipped when it names fewer than 2 NICs.
#   --no-nic-phase     phase 1 takes "one" data NIC down on every node at once.
#                      With a single NIC per node that isolates the whole
#                      cluster instead of testing path redundancy. Note
#                      --nic-phase-every 0 does NOT disable it (it means
#                      "once, on iteration 1").
#
# Env overrides: ITERATIONS, RUNTIME, START_ITERATION, RESTART_TIMEOUT,
# PLACEMENT_DUMPS=1, DATA_NIC.
set -u
cd "$HOME"
TS=$(date +%Y%m%d_%H%M%S)
echo "$TS" > "$HOME/soak_ts"
LOG="$HOME/soak_base_${TS}.log"
OUT="$HOME/soak_base_${TS}.out"
setsid nohup python3 "$HOME/aws_dual_node_outage_soak_multipath.py"     --run-on-mgmt     --metadata "$HOME/cluster_metadata_base.json"     --ssh-key "$HOME/.ssh/mtes01.pem"     --data-nics "${DATA_NIC:-eth0}"     --no-nic-phase     --iterations "${ITERATIONS:-75}"     --start-iteration "${START_ITERATION:-1}"     ${PLACEMENT_DUMPS:+--placement-dumps}     ${RESTART_TIMEOUT:+--restart-timeout $RESTART_TIMEOUT}     --runtime "${RUNTIME:-52000}"     --log-file "$LOG"     > "$OUT" 2>&1 < /dev/null &
PID=$!
echo "$PID" > "$HOME/soak_pid"
sleep 3
echo "launched pid=$PID ts=$TS"
echo "log=$LOG"
echo "out=$OUT"
ps -p "$PID" -o pid=,etime=,cmd= | cut -c1-110
