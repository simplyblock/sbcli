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

# `status minimal` separates the three states we care about. Reaching for it
# rather than just retrying `configure new` keeps an unreachable coordinator
# from costing a 100s configure timeout per attempt.
db_state() {
    local out
    out="$(fdbcli --exec 'status minimal' --timeout 10 -C "$FDB_CLUSTER_FILE" 2>&1 || true)"
    case "$out" in
        *'The database is available'*)   echo available ;;
        *'The database is unavailable'*) echo unconfigured ;;
        *)                               echo unreachable ;;
    esac
}

deadline=$(( $(date +%s) + READY_TIMEOUT ))
configured=0

while :; do
    state="$(db_state)"

    case "$state" in
    available)
        echo "Database is available"
        exit 0
        ;;
    unconfigured)
        # `configure new` is not idempotent: fdbcli refuses it once the
        # database exists. That is why every pass through this loop re-reads
        # the state instead of assuming the last attempt failed -- an attempt
        # whose report timed out may still have taken effect.
        if [ "$configured" -eq 0 ]; then
            echo "Coordinators are up; configuring the database"
        else
            echo "Still unconfigured; retrying"
        fi
        fdbcli --exec "configure new single ssd" --timeout 100 -C "$FDB_CLUSTER_FILE" || true
        configured=1
        sleep 10
        ;;
    unreachable)
        echo "Waiting for the FoundationDB coordinators to answer"
        sleep 5
        ;;
    esac

    if [ "$(date +%s)" -ge "$deadline" ]; then
        break
    fi
done

echo "ERROR: the database is not available ${READY_TIMEOUT}s after the FDB services converged (last state: ${state})." >&2
fdbcli --exec 'status minimal' --timeout 20 -C "$FDB_CLUSTER_FILE" >&2 || true
exit 1
