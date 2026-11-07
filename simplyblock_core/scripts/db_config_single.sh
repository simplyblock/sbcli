set -u -o pipefail

# Configure the database, once the FoundationDB processes can actually answer.
#
# deploy_stack.sh's fdb phase waits on the swarm services, which tells us the
# containers started -- not that fdbserver is listening. On 2026-11-06 the two
# were about 110s apart: the phase reported Done, this script's single
# `configure new` burned its whole --timeout 100 and printed "The database is
# unavailable", and every simplyblock service deployed after it died on
# FDBError 1031 until bootstrap gave up. The old ordering survived that gap by
# accident, because the 420s all-services wait it ran first doubled as a sleep.
#
# The failure was also silent. This script's last command used to be `sleep 10`,
# so it exited 0 whatever fdbcli did, and cluster_ops logged "Configuring DB >
# Done" over a database that did not exist.
#
# So: poll until the coordinators answer, configure, and confirm. A failure here
# exits non-zero, which __run_script turns into an exception.

FDB_CLUSTER_FILE=${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}
# Generous, because it is only ever reached when something is genuinely wrong:
# a healthy single-node FDB answers within seconds of its container starting.
# Stays well inside scripts.SCRIPT_TIMEOUT_SEC (720), so this reports the real
# reason instead of being killed mid-wait.
READY_TIMEOUT=${FDB_READY_TIMEOUT:-300}

# `status minimal` answers one question honestly -- is the database usable --
# and that is all this is asked to do. It does NOT distinguish "coordinators
# are up and waiting to be configured" from "nothing is running at all": it
# prints "The database is unavailable" for both, which is why the failure path
# below dumps full `status` instead, where a cluster with no processes in it
# says so.
db_available() {
    fdbcli --exec 'status minimal' --timeout 10 -C "$FDB_CLUSTER_FILE" 2>&1         | grep -q 'The database is available'
}

deadline=$(( $(date +%s) + READY_TIMEOUT ))
attempt=0

while :; do
    # Re-read before every attempt. `configure new` is not idempotent -- fdbcli
    # refuses it once the database exists -- and an attempt whose reply timed
    # out may still have taken effect, so "the last call failed" is not a safe
    # assumption to retry on.
    if db_available; then
        echo "Database is available after ${attempt} configure attempt(s)"
        exit 0
    fi

    if [ "$(date +%s)" -ge "$deadline" ]; then
        break
    fi

    attempt=$(( attempt + 1 ))
    echo "Configure attempt ${attempt}"
    fdbcli --exec "configure new single ssd" --timeout 100 -C "$FDB_CLUSTER_FILE" || true
    sleep 10
done

{
    echo "ERROR: the database is not available ${READY_TIMEOUT}s after the FDB"
    echo "services converged, over ${attempt} configure attempt(s). Full status"
    echo "follows. 'Unable to locate all fdbserver processes' here means the FDB"
    echo "containers never started -- check 'docker service ps app_fdb-server'"
    echo "for a task that failed before its process ran."
} >&2
fdbcli --exec 'status' --timeout 20 -C "$FDB_CLUSTER_FILE" >&2 || true
exit 1
