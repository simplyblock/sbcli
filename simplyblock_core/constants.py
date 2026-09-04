import logging
import os
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


def get_config_var(name, default=None):
    """
    OS environment variable is checked first, if not found, check the env_var file.
    """
    if not name:
        return False
    if os.getenv(name):
        return os.getenv(name)
    else:
        with open(f"{SCRIPT_PATH}/env_var", "r", encoding="utf-8") as fh:
            for line in fh.readlines():
                if line.startswith(name):
                    return line.split("=", 1)[1].strip()
    return default


KiB=1024
MiB=1024*1024
GiB=1024*1024*1024

KVD_DB_VERSION = 730
KVD_DB_FILE_PATH = os.getenv('FDB_CLUSTER_FILE', '/etc/foundationdb/fdb.cluster')
KVD_DB_TIMEOUT_MS = 10000
KVD_DB_BACKUP_PATH = "file:///etc/foundationdb/backup"
SPK_DIR = '/home/ec2-user/spdk'
LOG_LEVEL = logging.INFO
LOG_WEB_LEVEL = logging.DEBUG
LOG_WEB_DEBUG = True if LOG_WEB_LEVEL == logging.DEBUG else False

INSTALL_DIR = os.path.dirname(os.path.realpath(__file__))

NODE_MONITOR_INTERVAL_SEC = 3
DEVICE_MONITOR_INTERVAL_SEC = 5
STAT_COLLECTOR_INTERVAL_SEC = 60*5  # 5 minutes
LVOL_STAT_COLLECTOR_INTERVAL_SEC = 30
LVOL_MONITOR_INTERVAL_SEC = 30
# Short cadence used by lvol/snapshot monitors while in-deletion objects
# exist: delete chains advance at most one hop per cycle (clone -> snapshot
# -> parent snapshot), so the idle 30s interval alone added minutes per
# chain (run 20260730).
LVOL_MONITOR_DELETION_INTERVAL_SEC = 2
# How long a monitor waits for an async delete to finish while holding the
# chain lock, so async + the following sync deletes stay one atomic sequence.
SNAP_DELETE_COMPLETION_WAIT_SEC = 15
# Chains deleted concurrently by a monitor. Members of one chain always run on
# one worker (and the chain lock enforces it across processes).
CHAIN_DELETE_WORKERS = 16
# Storage nodes swept concurrently by the lvol monitor.
LVOL_MONITOR_NODE_WORKERS = 16
DEV_MONITOR_INTERVAL_SEC = 10
# Collector cadence (#5, 2026-07-21): the idle-cluster baseline measured
# 4,290 RPCs/min cluster-wide (get_iostat 16k / alceml_get_pages_usage 15k /
# distr events 27k per 28min) — ~9-10 CP threads permanently busy servicing
# monitors, which is the standing GIL convoy that taxed every restart RPC.
# Stats collection is dashboard granularity, not failure detection (that is
# NODE/DEVICE_MONITOR + KA + distr events), so stretch the pure-stats
# cycles; keep the event collector at a failure-latency-compatible cadence.
DEV_STAT_COLLECTOR_INTERVAL_SEC = 15
PROT_STAT_COLLECTOR_INTERVAL_SEC = 10
SPDK_STAT_COLLECTOR_INTERVAL_SEC = 30
DISTR_EVENT_COLLECTOR_INTERVAL_SEC = 5
DISTR_EVENT_COLLECTOR_NUM_OF_EVENTS = 10
#: JM events are polled on their own cadence. Unlike the distrib source there
#: is no discard counterpart -- jm_get_events returns every event it holds on
#: every call -- so the collector filters what it has already logged and the
#: poll can afford to be less frequent than the distrib one.
JM_EVENT_COLLECTOR_INTERVAL_SEC = 10
#: How many recently-logged JM event keys to remember per node for that filter.
JM_EVENT_DEDUPE_MAX = 10000

#: Journal records accumulated for compression above which an lvs is flagged in
#: the cluster event log. A backlog this size means compression is not keeping
#: up with the write rate (or is stuck/suspended), and the journal replay
#: needed by the next restart or failover grows with every record.
JM_COMPRESSION_BACKLOG_ALERT_RECORDS = 500_000_000
#: Re-arm the alert only after the backlog falls below this fraction of the
#: threshold, so a count oscillating around the line does not flap events.
JM_COMPRESSION_BACKLOG_REARM_FRACTION = 0.9
CAP_MONITOR_INTERVAL_SEC = 30
SSD_VENDOR_WHITE_LIST = ["1d0f:cd01", "1d0f:cd00"]
CACHED_LVOL_STAT_COLLECTOR_INTERVAL_SEC = 15
DEV_DISCOVERY_INTERVAL_SEC = 60

PMEM_DIR = '/tmp/pmem'

NVME_PROGRAM_FAIL_COUNT = 50
NVME_ERASE_FAIL_COUNT = 50
NVME_CRC_ERROR_COUNT = 50
DEVICE_OVERLOAD_STDEV_VALUE = 50
DEVICE_OVERLOAD_CAPACITY_THRESHOLD = 50

CLUSTER_NQN = "nqn.2023-02.io.simplyblock"

weights = {
    "lvol": 100,
    # "cpu": 10,
    # "r_io": 10,
    # "w_io": 10,
    # "r_b": 10,
    # "w_b": 10
}


HEALTH_CHECK_INTERVAL_SEC = 30
# Faster cadence used by the per-node health-check loop when a node is NOT
# STATUS_ONLINE.  Accelerates observation of recovery transitions without
# adding polling cost to healthy nodes.
HEALTH_CHECK_FAST_INTERVAL_SEC = 5
# Remote-device sweep gate (run 20260725): sync_remote_devices_from_spdk pays
# one SPDK inventory RPC per node per pass; while peer topology is unchanged
# the sweep is skipped, but never longer than this floor, so drift that no
# topology event announces (manual attach, missed event) is still reconciled.
HEALTH_CHECK_REMOTE_SWEEP_FORCE_SEC = 600

GRAYLOG_CHECK_INTERVAL_SEC = 60

# Stats-retention cleanup cycle. Retention granularity is days
# (LOG_DELETION_INTERVAL, default 7d), so hourly sweeps are more than enough;
# the previous 60s cycle re-cleared the same (mostly empty) ranges ~100
# commits/s all day and contributed to FDB overload under mass create
# (run 2026-07-21).
FDB_CLEANUP_INTERVAL_SEC = 60 * 60

# Continuous per-lvol NVMf subsystem verification + auto-repair in the lvol
# monitor. ON by default since 2026-08-10.
#
# It was previously off for two reasons, both now addressed:
#  - cost (2 RPCs per lvol per 30s cycle): the sweep is now rate-limited to
#    LVOL_MONITOR_SUBSYS_CHECK_INTERVAL_SEC instead of running every cycle;
#  - its repair re-added namespaces of in-deletion lvols mid-delete
#    (2026-07-14 / 2026-07-16): both check_node and add_lvol_thread now
#    re-read the record and refuse to register anything not ONLINE/OFFLINE.
#
# Leaving it off costs far more than it saves: this sweep is the ONLY thing
# that detects a replica whose subsystem exists but carries no namespace, and
# such a replica is invisible everywhere else — the client connects fine and
# simply has one path fewer than it believes. In the 2026-08-09 run that state
# persisted for 36 hours across many volumes (78726d0e 608 degraded reports,
# 64467bfa 437, 638be965 308) and cost one volume all of its I/O when an
# outage took the two paths it had left. Set LVOL_MONITOR_SUBSYS_CHECK=0 to
# disable.
LVOL_MONITOR_SUBSYS_CHECK = str(
    os.getenv("LVOL_MONITOR_SUBSYS_CHECK", "1")).lower() in ("1", "true", "yes")

# How often the per-lvol subsystem verification sweep above actually runs.
# The monitor's own cycle stays at LVOL_MONITOR_INTERVAL_SEC (deletions must
# drain promptly); only the verification sweep is throttled to this period.
LVOL_MONITOR_SUBSYS_CHECK_INTERVAL_SEC = int(
    os.getenv("LVOL_MONITOR_SUBSYS_CHECK_INTERVAL_SEC", "300"))

TASK_EXEC_INTERVAL_SEC = 10
TASK_EXEC_RETRY_COUNT = 8
# Shorter interval + lower ceiling for node/device restart tasks.  Restart
# tasks are time-critical (cluster is degraded until the node is back) and
# each retry does useful work (ping + api check + kill + restart), so we
# don't want an exponential 10→20→40→80 backoff to dominate the recovery
# window.  See incident 2026-04-20: 83 s end-to-end recovery, ~60 s of which
# was TASK_EXEC_INTERVAL doubling between redundant retries.
RESTART_TASK_EXEC_INTERVAL_SEC = 3
# Cap exponential backoff at 1 h. Peer-side recovery (lvstore replay
# across a slow remote-NVMe link, JC reconnect against a peer coming
# back from host_reboot, or simply mutual-exclusion contention while a
# different peer is mid-restart) can legitimately take longer than
# minutes. With max_retry=11 the doubling sequence (3,6,12,24,48,96,
# 192,384,768,1536,3072→capped) reaches the cap on the 10th attempt,
# giving a total budget in the hours range — the right scale for
# transient peer-recovery waits without giving up prematurely.
RESTART_TASK_EXEC_INTERVAL_MAX_SEC = 3600

# A JobSchedule's lease is held by the runner host (by hostname) that last
# touched it. Another host may only take over a task whose lease is older than
# this — i.e. the owning runner is presumed dead. A live owner refreshes the
# lease every TASK_LEASE_HEARTBEAT_SEC from a background thread while driving
# a restart (restart_storage_node wrapper), so the TTL no longer needs to
# exceed the longest blocking RPC — it only needs to be several heartbeats
# wide so a momentarily slow (but alive) owner is never falsely preempted.
# Keeping it short is what makes ownership transfer fast: when the driving
# process dies (pod evicted while its host drains, CLI killed), a live
# tasks-runner claims the stale lease and resumes the restart instead of the
# node staying orphaned in RESTARTING (2026-07-04 MCO rollout deadlock).
# A runner restarting on the SAME host re-claims its own tasks immediately
# regardless of this value (owner id is the hostname).
TASK_LEASE_HEARTBEAT_SEC = 30
TASK_LEASE_TTL_SEC = 180

# Per-node restart claim: cross-ACTOR mutual exclusion for a single node's
# restart. The task lease above serializes runner HOSTS on one task, but a
# manual CLI restart and the restart task runner are DIFFERENT actors sharing
# the same NODE_RESTART task (ensure_node_restart_task dedups to one per
# node) and — on the mgmt host — potentially the same lease owner id
# (hostname), so the lease cannot tell them apart. The claim is an
# (owner-token, timestamp) pair on the StorageNode row, acquired atomically
# inside try_set_node_restarting's FDB tx, heartbeated by the
# restart_storage_node wrapper while the restart runs, and released on exit.
# A claim older than the TTL means its driver died mid-restart and may be
# taken over (this is what keeps the transferable-ownership resume path
# alive). force does NOT bypass a fresh claim: mutual exclusion is not an
# operator-overridable safety check (2026-08-06 soak iter-50: a manual CLI
# restart and the task runner drove the same node concurrently, their
# spdk_process_start calls replacing each other's container mid-restart).
RESTART_CLAIM_HEARTBEAT_SEC = TASK_LEASE_HEARTBEAT_SEC
RESTART_CLAIM_TTL_SEC = TASK_LEASE_TTL_SEC

# Node-add concurrency: the cross-node mesh section of add_node is serialized
# per cluster behind a ClusterAddNodeLock. The holder refreshes the lock every
# CLUSTER_ADD_LOCK_HEARTBEAT_SEC; a lock whose heartbeat is older than
# CLUSTER_ADD_LOCK_TTL_SEC is treated as abandoned (holder crashed) and may be
# reclaimed. TTL is kept well under TASK_LEASE_TTL_SEC so a dead holder's lock
# is reclaimed before its task lease, and is several heartbeats wide so a live
# (but momentarily slow) holder is never falsely preempted. The slow part of
# add_node (SPDK boot) is OUTSIDE this lock, so the locked section is short.
CLUSTER_ADD_LOCK_HEARTBEAT_SEC = 30
CLUSTER_ADD_LOCK_TTL_SEC = 120

# Cluster creation concurrency: add_cluster()'s duplicate-name check
# (does a cluster named X already exist?) is otherwise a plain read-then-write
# with no atomicity, so concurrent/retried create calls for the same name can
# all pass the check before any of them has committed — observed 2026-07-28:
# a control-plane readiness flap caused the operator to retry cluster-create
# ~6 times in a burst, producing 6 separate "simplyblock-cluster" records
# instead of one. A ClusterCreateLock keyed by name serializes create attempts
# for that name; no heartbeat (create is a single synchronous call, not a
# long-lived section), just a generous TTL so a crashed holder's lock is
# eventually reclaimable. Sized above add_cluster's worst realistic runtime
# (the first-cluster bootstrap path retries opensearch/graylog up to ~150s
# each, sequentially).
CLUSTER_CREATE_LOCK_TTL_SEC = 600

# How long a queued add_node waits for the lock before failing for retry.
# "Short" is relative: one mesh section takes minutes on a 32-node cluster,
# and parallel deploys queue up to (workers - 1) adds behind the holder, so
# the tail waiter legitimately needs workers x mesh-time (2026-07-16 perf
# deploy: 300s timed out routinely with 8 parallel adds). A timeout here
# costs a full node-local re-setup on retry, so err on the side of waiting.
CLUSTER_ADD_LOCK_WAIT_TIMEOUT_SEC = 1800

# A node-add port reservation older than this is treated as abandoned and
# ignored/reclaimed. Must exceed the worst-case time from port allocation to
# persisting the node record (which spans the SPDK boot), so a live add never
# loses its reserved port.
PORT_RESERVATION_TTL_SEC = 600

# Snapshot create concurrency: the primary-create + replica-register sequence of
# a snapshot is serialized per lvstore behind an LVStoreMutationLock so that
# concurrent snapshot creates of the same lvstore register on the
# secondary/tertiary in creation (blobid) order. Out-of-order registration
# builds the replica blob tree with a child before its parent and corrupts the
# lvstore. The holder refreshes the lock every LVSTORE_MUTATION_LOCK_HEARTBEAT_SEC;
# a lock whose heartbeat is older than LVSTORE_MUTATION_LOCK_TTL_SEC is treated
# as abandoned (holder crashed) and may be reclaimed. A caller waits at most
# LVSTORE_MUTATION_LOCK_WAIT_SEC for the lock before failing (retryable). TTL is
# several heartbeats wide so a live-but-slow holder (a register RPC can take many
# seconds under load) is never falsely preempted.
LVSTORE_MUTATION_LOCK_HEARTBEAT_SEC = 15
LVSTORE_MUTATION_LOCK_TTL_SEC = 60
LVSTORE_MUTATION_LOCK_WAIT_SEC = 120

# A create/snapshot/clone request may block on two sequential waits before doing
# any RPC work: first the node-level sync-delete drain (LVOL_SYNC_DELETE_WAIT_SEC),
# then the per-lvstore mutation lock (LVSTORE_MUTATION_LOCK_WAIT_SEC). These locks
# MUST time out before the front-end API cuts the connection, otherwise a waiting
# request is severed mid-operation and can leave a half-registered object. The
# invariant the deployment must hold (HAProxy timeout server/client in
# scripts/haproxy.cfg is the binding API timeout; uvicorn imposes none; the CLI
# runs controllers in-process):
#
#   API_OPERATION_TIMEOUT  >  LVOL_SYNC_DELETE_WAIT_SEC
#                             + LVSTORE_MUTATION_LOCK_WAIT_SEC
#                             + worst-case create+register RPC work
#
# Current budget: 60 + 120 = 180s max lock wait, leaving 120s of the 300s API
# timeout for RPC work. Keep haproxy.cfg in sync if these change.
LVOL_SYNC_DELETE_WAIT_SEC = 60
API_OPERATION_TIMEOUT_SEC = 300

# An LVol left in STATUS_IN_CREATION longer than this is treated as an orphaned
# create (the creating process died before reaching ONLINE) and is cleaned up
# by lvol_monitor. Must be comfortably longer than the slowest legitimate
# create (HA multi-node registration) so an in-progress create is never killed.
LVOL_IN_CREATION_STALE_SEC = 600

SIMPLY_BLOCK_SPDK_CORE_IMAGE = "simplyblock/spdk-core:v24.05-tag-latest"
SIMPLY_BLOCK_DOCKER_IMAGE = get_config_var(
        "SIMPLY_BLOCK_DOCKER_IMAGE","simplyblock/simplyblock:main")
SIMPLY_BLOCK_CLI_NAME = get_config_var(
        "SIMPLY_BLOCK_COMMAND_NAME", "sbcli")
SIMPLY_BLOCK_SPDK_ULTRA_IMAGE = get_config_var(
        "SIMPLY_BLOCK_SPDK_ULTRA_IMAGE", "public.ecr.aws/simply-block/ultra:main-latest")
SIMPLY_BLOCK_VERSION = get_config_var("SIMPLY_BLOCK_VERSION", "1")

GELF_PORT = 12202

MIN_HUGE_PAGE_MEMORY_FOR_LVOL = 209715200
MIN_SYS_MEMORY_FOR_LVOL = 524288000
EXTRA_SMALL_POOL_COUNT = 30000
EXTRA_LARGE_POOL_COUNT = 10240
EXTRA_HUGE_PAGE_MEMORY = 3221225472
EXTRA_SYS_MEMORY = 0.10

INSTANCE_STORAGE_DATA = {
        'i4i.large': {'number_of_devices': 1, 'size_per_device_gb': 468},
        'i4i.xlarge': {'number_of_devices': 1, 'size_per_device_gb': 937},
        'i4i.2xlarge': {'number_of_devices': 1, 'size_per_device_gb': 1875},
        'i4i.4xlarge': {'number_of_devices': 1, 'size_per_device_gb': 3750},
        'i4i.8xlarge': {'number_of_devices': 2, 'size_per_device_gb': 3750},
        'i4i.12xlarge': {'number_of_devices': 3, 'size_per_device_gb': 3750},
        'i4i.16xlarge': {'number_of_devices': 4, 'size_per_device_gb': 3750},
        'i4i.24xlarge': {'number_of_devices': 6, 'size_per_device_gb': 3750},
        'i4i.32xlarge': {'number_of_devices': 8, 'size_per_device_gb': 3750},

        'i4i.metal': {'number_of_devices': 8, 'size_per_device_gb': 3750},
        'i3en.large': {'number_of_devices': 1, 'size_per_device_gb': 1250},
        'i3en.xlarge': {'number_of_devices': 1, 'size_per_device_gb': 2500},
        'i3en.2xlarge': {'number_of_devices': 2, 'size_per_device_gb': 2500},
        'i3en.3xlarge': {'number_of_devices': 1, 'size_per_device_gb': 7500},
        'i3en.6xlarge': {'number_of_devices': 2, 'size_per_device_gb': 7500},
        'i3en.12xlarge': {'number_of_devices': 4, 'size_per_device_gb': 7500},
        'i3en.24xlarge': {'number_of_devices': 8, 'size_per_device_gb': 7500},
        'i3en.metal': {'number_of_devices': 8, 'size_per_device_gb': 7500},

        'm6id.large': {'number_of_devices': 1, 'size_per_device_gb': 116},
        'm6id.xlarge': {'number_of_devices': 1, 'size_per_device_gb': 237},
        'm6id.2xlarge': {'number_of_devices': 1, 'size_per_device_gb': 474},
        'm6id.4xlarge': {'number_of_devices': 1, 'size_per_device_gb': 950},
        'm6id.8xlarge': {'number_of_devices': 1, 'size_per_device_gb': 1900},
    }

MAX_SNAP_COUNT = 100

# Hard per-lvstore object cap: an lvstore serves at most this many objects
# (lvols + clones + snapshots), counted against the lvstore's owning node.
# Enforced on every create path (lvol create, snapshot create, clone). A node
# that temporarily serves a second LVS (takeover / acting leader) gets an
# independent budget per LVS — the limit protects each lvstore's blobstore
# and journal, not the host. Replaces the earlier per-core cap
# (cores x 2000); object-count overload precedent: run 20260712-231123,
# ~68k objects on one 12-core instance drove swap thrash and a JC-quartet
# abort -- which is an order of magnitude above this cap, so the headroom
# below it is real.
#
# Raised 6000 -> 12000 on 2026-08-20, lowered back to 6000 on 2026-08-28.
MAX_OBJECTS_PER_LVSTORE = 6000

# Hard cap on namespaces (lvols) sharing one nvmf subsystem. The DEFAULT for
# namespaced creates stays LVO_MAX_NAMESPACES_PER_SUBSYS; this is the ceiling
# a caller-supplied max_namespace_per_subsys may not exceed, and it also
# bounds joins into legacy subsystems recorded with a larger max.
MAX_NAMESPACES_PER_SUBSYSTEM = 50

# Hard cap on lvol subsystems per node (primary subsystems; namespaced
# volumes share one). Applied as a ceiling over the node's configured
# max_lvol: effective limit = min(max_lvol, this).
#
# It is also the admission ceiling for the user-supplied max_lvol itself
# (`sn configure --max-subsys`, `sn restart --max-subsys`, the k8s
# node-configure entrypoint and persist_node_config). A larger value was
# accepted before and then silently clamped at placement time, so the node
# reserved huge pages for subsystems it could never serve and operators
# believed a limit that did not hold. Ingress now rejects anything above
# this; internal readers of an already-stored config clamp with a warning.
MAX_SUBSYSTEMS_PER_NODE = 75

# Cross-cluster cutover: upper bound for the iterative delta-shrink phase
# (snapshot -> wait replicated -> snapshot -> wait) before the final freeze.
# Two rounds normally complete within 2 replication intervals + transfer time.
REPL_CUTOVER_SHRINK_TIMEOUT_SEC = 900
# Safety timeout for the operator preconnect signal. The task suspends indefinitely
# waiting for POST .../replication/cutover-proceed; this is the fallback deadline
# if the operator is unavailable. Cutover proceeds regardless after this many seconds.
REPL_CUTOVER_PROCEED_TIMEOUT_SEC = 120

# --- cutover delta convergence -------------------------------------------
# The IO freeze copies everything written since the last replicated snapshot,
# so the cutover converges the delta FIRST: take a snapshot, transfer it, and
# immediately take the next, until a round transfers in "low seconds". A fixed
# two rounds (the previous behaviour) does not converge under load -- it just
# stops.
REPL_CUTOVER_CONVERGE_TARGET_SEC = 2.0
# Safety bound: a volume written faster than it replicates never converges, so
# stop and freeze rather than looping forever.
REPL_CUTOVER_MAX_SHRINK_ROUNDS = 12
# When to stop converging in the open and take the lvstore for the endgame.
# A round completing within this multiple of the target means the delta is
# nearly converged, so the exclusive window that follows will be short. Claiming
# earlier serialises the bulk catch-up, which is what produced 0/20 cutovers in
# run 20260828_124859 (round 1 growing 340s -> 2584s purely from queueing).
# The endgame starts once ordinary replication has the target within this many
# seconds. Before that the cutover waits and takes NO snapshots of its own --
# the iterative snapshots ARE the endgame.
REPL_CUTOVER_ENDGAME_LAG_SEC = 50
# Rounds must follow each other within MILLISECONDS. Returning to the task
# scheduler between them costs TASK_EXEC_INTERVAL_SEC (10s) of fresh writes
# each time, which puts a floor under the delta no number of rounds can beat.
REPL_CUTOVER_POLL_INTERVAL_SEC = 0.2
# How long a single runner pass may stay inside the convergence loop.
REPL_CUTOVER_CONVERGE_BUDGET_SEC = 60
# Always worth polling inline for at least this long: a round that finishes
# just after the pass is handed back costs a full TASK_EXEC_INTERVAL_SEC of
# writes in the next round.
# The snapshot-replication runner now finishes a transfer in the pass that
# submitted it, so a convergence round completes in about the transfer's own
# duration. Staying inline across that is what makes "next snapshot within a
# second of completion" true; yielding mid-round reintroduces pass latency.
REPL_CUTOVER_MIN_INLINE_SEC = 30

# Whether to block the cutover on the operator's preconnect signal. The wait
# sits BETWEEN the cutover clone's base snapshot and the freeze, so every
# second of it is a second of writes the frozen final step must copy: with no
# operator present the 120s fallback timeout fired 34 times in one soak run and
# fed the 25-72s freezes. Deployments whose operator posts
# .../replication/cutover-proceed set this True and accept that cost until the
# clone's base can be advanced after the signal.
#
# Enabled 2026-09-02: the operator's reconcileCutoverPending runs the
# preconnect Job and posts cutover-proceed for both migration and failback
# (annotFailbackTarget routes the call to the target cluster on failback).
# Without the gate, the flip races the client: the 2026-09-02 failback run
# flipped ANA on listeners no client had connected to and deleted the DR-side
# subsystem 150ms later, orphaning every connected client for ctrl_loss_tmo.
REPL_CUTOVER_PROCEED_REQUIRED = True

# --- noticing a finished transfer ----------------------------------------
# A transfer that has completed must be acted on within a second: the next
# convergence snapshot cannot be taken until the previous one is marked
# replicated, so observation latency lands directly in the IO freeze.
REPL_XFER_POLL_INTERVAL_SEC = 0.1
# How long the submitting pass may wait inline for the transfer. The runner is
# single-threaded, so this is a starvation budget, not a timeout: exceeding it
# just falls back to being noticed on a later pass.
REPL_XFER_INLINE_WAIT_SEC = 5.0
# A volume in its final cutover already owns its lvstore and every other
# transfer on it is held, so there is nothing to starve -- wait as long as the
# transfer needs, because this is exactly the window the client freeze pays for.
REPL_XFER_INLINE_WAIT_CUTOVER_SEC = 300.0
# Cooldown between hub-attach retry attempts when the target node is down or
# recovering (covers control-plane lag before the DB reflects the down state).
REPL_CUTOVER_HUB_RETRY_COOLDOWN_SEC = 30
# Max consecutive hub-attach failures with the node still appearing online
# before we give up and burn a task.retry.  30s × 20 = 10 min of coverage.
REPL_CUTOVER_MAX_HUB_ATTEMPTS = 10
# Pass interval while a cutover is mid-round. NOT sub-second: this loop reads
# the task table per pass, and polling a database at 5Hz to detect an event is
# the wrong shape. Sub-second reaction lives in the RPC-based inline wait.
REPL_CUTOVER_ACTIVE_POLL_SEC = 1.0
# Delete the superseded original volume BEFORE building the fail-back clone
# (_retire_superseded_original). Disabled 2026-09-01: that delete frees the
# original's blob id while its parent snapshot's clone registry is already
# inconsistent ("Clone entry not found for blob ... under snapshot ..."), the
# clone created seconds later reuses the freed id, and every final-step delta
# write to it fails rc -1 (-EPERM) -> transfer_state Failed on all fail-back
# cutovers. The SPDK-side namespace slot is still freed by
# _evict_stale_namespace, and the original's DB record is removed after a
# successful cutover by _swap_failback_lvol_uuid. Re-enable once the fork's
# clone-entry/blob-id-reuse defect is fixed.
REPL_FAILBACK_RETIRE_ORIGINAL_BEFORE_CUTOVER = False

SPDK_PROXY_MULTI_THREADING_ENABLED=True
SPDK_PROXY_TIMEOUT=60*5
LVOL_NVME_CONNECT_RECONNECT_DELAY=2
LVOL_NVME_CONNECT_CTRL_LOSS_TMO=60*60
LVOL_NVME_CONNECT_FAST_IO_FAIL_TO=8
LVOL_NVME_CONNECT_NR_IO_QUEUES=3
# Client keep-alive. A blocked nvmf port stops answering keep-alives, and a
# block is only bounded by SPDK's own reject conversion at ack_timeout * 4 --
# with nvmf_create_transport ack_timeout=2000 that is 8s (rpc_client.py:391).
# At 4s the client gave up mid-fence: on 2026-09-01 a restart fence on a
# healthy peer held port 4440 for the full 8s, the request sat 7999742us in
# TCP_REQUEST_STATE_READY_TO_COMPLETE, and the qpairs were quiesced. The
# client keep-alive must outlast the worst-case fence, not expire inside it.
LVOL_NVME_KEEP_ALIVE_TO=8
LVOL_NVME_KEEP_ALIVE_TO_TCP=8
QPAIR_COUNT=64
CLIENT_QPAIR_COUNT=3
# 8 s, not 4 s. 4 s false-positives during a peer-reset reactor stall:
# when a peer dies, bdev_nvme's per-controller reset state machines run on
# the same SPDK reactor thread that polls JM/heartbeat qpairs to other
# peers, and the reactor can spend ~4 s in that bookkeeping. With a 4 s
# timeout, in-flight heartbeats to *healthy* peers age past the threshold
# during that stall, timeout_cb fires on every controller in lock-step,
# and the JC marks N JM slots blocked simultaneously — dropping
# n_safe_jms below the FT threshold and triggering a JCERR / DISTRIBD
# write fail (observed 2026-04-30 14:14:22 on a dual-outage soak step,
# stall measured at 4.144 s). 8 s absorbs the worst observed stall and
# still fast-fails wedged targets ~10× faster than the previous abort-
# hang path (multi-minute, the 2026-04-27 incident that motivated the
# action_on_timeout=reset switch — that switch stays; only the threshold
# reverts).
NVME_TIMEOUT_US=8000000
PCIE_TIMEOUT_US=2000000

# Max concurrent per-node workers during cluster_activate passes (recreate of
# primary/non-leader LVS, hublvol wiring, ANA flips). Sequential activation is
# O(nodes) at ~40 s/node — 22 min on a 32-node cluster (2026-07-08) — which
# starved the activation watchdog and every observer. Bounded so the mgmt node
# and the FDB layer are not overwhelmed by 32 parallel RPC fan-outs.
# Raised 8 -> 16 (2026-07-13): at 8 the passes ran 4 serial waves on a
# 32-node cluster (~13 min of lvstore passes in the validation run) while
# per-worker time is dominated by waiting on the target node's own SPDK
# (examine), not by mgmt/FDB load.
CLUSTER_ACTIVATION_MAX_PARALLEL_NODES=16

# Number of node-add tasks the runner processes concurrently. Single source of
# truth for "parallel add": also used as the initial storage-MCP maxUnavailable
# during bring-up, so up to this many nodes can reboot for CPU-topology at once
# (matching how many are being added in parallel) instead of a one-at-a-time
# queue. cluster_activate later narrows the pool to the cluster's fault tolerance.
NODE_ADD_MAX_PARALLEL=8

# Max concurrent node-restart tasks while the cluster is SUSPENDED (recovery
# after full-cluster outage/shutdown: every node offline, no client IO — so
# parallel restarts cannot violate FTT). One node restart is ~70 s; strictly
# sequential recovery of a 32-node cluster took ~38 min (2026-07-08). Online
# clusters keep one-restart-at-a-time semantics regardless of this value.
# 32: all nodes of a suspended cluster may restart together — the critical
# bi-directional interconnection phase is serialized by
# storage_node_ops._remote_connect_gate regardless of this fan-out, and
# per-node exclusivity is enforced by the dispatch _node_inflight map.
NODE_RESTART_MAX_PARALLEL_SUSPENDED=32

# Global cap on concurrently-RUNNING connect/reconnect worker threads across
# ALL parallel node restarts. A whole-failure-domain reboot dispatches up to
# NODE_RESTART_MAX_PARALLEL_SUSPENDED restarts, each fanning out per-peer /
# per-remote-device connect threads — 100+ concurrent threads were observed
# (2026-07-20 FD-0 reboot), saturating the single Python GIL and starving the
# (serialized) recreate's between-RPC work so the client-port-block window
# ballooned from ~2s to ~20s. Bounding the concurrent worker threads keeps GIL
# headroom for recreate while preserving enough I/O overlap. Tune via e2e.
RESTART_WORKER_MAX_CONCURRENCY=24

# Cap for the COORDINATOR tier: bounded workers that themselves spawn and
# join LEAF workers (peer-reconnect _one_peer -> per-device connect threads).
# Must be a semaphore distinct from RESTART_WORKER_MAX_CONCURRENCY: sharing
# one pool let 24 coordinators hold every slot while joining leaves that
# waited on the same semaphore — permanent deadlock, all nodes stuck
# in_restart (2026-07-21 FD reboot; py-spy: 24 holders / 469 waiters).
#
# 64, not 16: coordinators are I/O-bound (RPC + FDB waits release the GIL;
# their CPU share collapsed with the BaseModel reflection cache), and 16
# queued a 16-node FD recovery's peer sweeps into 30+ serial waves
# (py-spy 2026-07-21: exactly 16 running / 160 queued while every restart
# thread sat in the sweep join). 64 still bounds thread count but clears a
# full-cluster sweep in ~4 waves.
RESTART_COORDINATOR_MAX_CONCURRENCY=64

# Quiesce after blocking the CONFIGURED PRIMARY's client port in
# recreate_lvstore_on_non_leader, before the peer's examine. A fixed wait,
# NOT a drain: on this node class the only inflight counter
# (bdev_distrib_check_inflight_io) includes data-migration mover IO that a
# port block cannot stop, so poll-to-zero never settles on a migrating
# primary (the 10s regression fixed by 5cf279db). Client IO admitted before
# the block settles in single-digit ms; 200ms keeps ~2 orders of magnitude
# margin (was 500ms — 40% of the whole post-fix window, 2026-07-22).
# Durable replacement: nvmf-layer drain of the blocked port's own
# outstanding commands (nvmf_port_block wait_for_drain, SPDK fork change) —
# migration-immune and exact, on every node class.
NON_LEADER_BLOCK_QUIESCE_SEC = 0.2

# Budgets for RPCs issued while a peer's client port is fenced.
#
# The default RPCClient (timeout=180, retry=3, ~726s worst case) is unusable
# there. A blocked nvmf port auto-converts to REJECT at ack_timeout * 4 --
# nvmf_create_transport sets ack_timeout=2000, so 8s -- and once it does SPDK
# marks every qpair on that port rejected and drives it to QUIESCING, i.e. the
# client loses the path outright rather than merely waiting.
#
# 2026-09-01: a restart fence on a healthy peer ran the full 8s; one request
# sat 7,999,742us in TCP_REQUEST_STATE_READY_TO_COMPLETE and was released by
# the reject timer, not by the fence lifting. That surfaced as an IO timeout,
# the JC demoted the leader, and IO arriving afterwards was failed with a
# generic INTERNAL DEVICE ERROR, which Linux nvme-multipath does not retry on
# another path -- client EIO.
#
# Anything that overruns its budget must release the fence and abort the
# restart; the task runner re-queues it. A retried restart is cheap, a
# quiesced client path is not.
FENCE_RPC_TIMEOUT_SEC = 0.5
#: Per-peer budget for the data-plane quorum vote, and the ceiling on waiting
#: for the vote threads.
#:
#: This vote is a liveness question -- "does this peer still see the node's
#: remote_jm controller?" -- so a slow answer IS the answer. It used to run
#: rpc_client(timeout=8, retry=1) and then join() the vote threads with no
#: timeout at all, which put an unbounded wait inside the restart's port
#: fence.
#:
#: 2026-09-01, LVS_10: three peers voted in 3ms (16:28:29.006-29.009); a
#: fourth was dialling the node the soak had host-rebooted at 16:28:23 and
#: burned one full 8s timeout. The unbounded join held the fence until
#: 16:28:36.961 -- 7.95s, 65% of a 12.2s fence, waiting on a node already
#: known to be rebooting.
DP_VOTE_RPC_TIMEOUT_SEC = 0.5
DP_VOTE_RPC_RETRY = 1
#: Ceiling on the whole vote round. A thread still running when this expires
#: abstains -- which the quorum logic already handles -- rather than holding
#: its caller.
DP_VOTE_JOIN_TIMEOUT_SEC = 1.5
# One retry. Two attempts of 0.5s still fit comfortably under the deadline, and
# the deadline clamp below is what actually bounds the total -- a single
# transient refusal should not abort a restart on its own.
FENCE_RPC_RETRY = 1
# Hard ceiling on how long any peer's client port may stay fenced, measured
# from the first block. Sits just under the reject threshold (ack_timeout * 4 =
# 8s) so the fence is always released by us, never converted to reject by SPDK
# -- the conversion is what quiesces the client's qpairs and costs it the path.
# Checked before every in-fence RPC and on every iteration of the two in-window
# wait loops, and each RPC's timeout is clamped to the time remaining, so a
# call can never run past the deadline.
FENCE_DEADLINE_SEC = 7.5
# bdev_examine only SCHEDULES the examine and returns, so it lives inside the
# 0.5s budget like everything else. bdev_wait_for_examine is the one call that
# genuinely blocks -- it waits for the examine to finish -- and gets the longer
# budget. 6s is above the 0.5s norm but still inside the 8s reject threshold,
# and overrunning it releases the fence rather than stretching the block.
FENCE_WAIT_EXAMINE_TIMEOUT_SEC = 6

NVMF_MAX_SUBSYSTEMS=50000
KATO=5000
# transport_ack_timeout exponent: server tears down a client qpair if it
# stays silent for ~2^ACK_TO ms. ACK_TO=11 (~2 s) is shorter than the LVS
# tertiary rejoin freeze window (≈ 4 s today) — the server kills healthy
# qpairs on the alive primary mid-freeze and clients see a multi-second
# stall on reissue. Bumped to 12 (~4 s) so the freeze fits inside the
# budget. Long-term, the freeze itself is being shortened (single-path
# hublvol attach + deferred failover); this stays as belt-and-braces so
# a stragglier rejoin doesn't immediately re-trip the bug.
ACK_TO=11
# bdev_retry_count must be non-zero for SPDK bdev_nvme to retry an aborted
# IO on the alternate path of an NVMe-oF multipath bdev (per the SPDK
# multipath docs). Multipath is in play whenever a node consumes a hublvol
# bdev that has both a primary-target and a secondary-target listener
# (i.e. any FTT≥1 cluster), independent of how many local NICs the node has.
# So we set the retries unconditionally rather than gating on data_nics.
# Worst-case retry budget: (1+BDEV_RETRY) * (1+TRANSPORT_RETRY) = 3*2 = 6
# transport submissions per failing IO before EIO bubbles to the caller.
BDEV_RETRY=2
TRANSPORT_RETRY=1
CTRL_LOSS_TO=1
FAST_FAIL_TO=0
RECONNECT_DELAY_CLUSTER=1
LVOL_CLUSTER_RATIO=1

# Fixed size (in bytes) each distrib bdev reports up to the raid0/lvstore
# layer, independent of cluster raw capacity or number_of_distribs. 250 TiB.
#
# BIRTH-TIME ONLY: this is the size used when an lvstore is first created
# (create_lvstore). It must NEVER be read on the recreate/restart path --
# recreate_lvstore replays the persisted lvstore_stack verbatim, preserving
# each distrib's original num_blocks. Resizing a distrib under a live
# raid0/lvstore would corrupt the geometry, so existing lvstores must keep
# their persisted size across upgrades even if this constant changes.
DISTRIB_SIZE_BYTES = 274877906944000


SENTRY_SDK_DNS = "https://745047b017ac424b4173550e19910fb7@o4508953941311488.ingest.de.sentry.io/4508996361584720"
ONE_KB = 1024
TEMP_CORES_FILE = "/etc/simplyblock/tmp_cores_config"
PROMETHEUS_MULTIPROC_DIR = "/etc/simplyblock/metrics"

LINUX_DRV_MASS_STORAGE_ID = 1
LINUX_DRV_MASS_STORAGE_NVME_TYPE_ID = 8



NODES_CONFIG_FILE = "/etc/simplyblock/sn_config_file"
SYSTEM_INFO_FILE = "/etc/simplyblock/system_info"

LVO_MAX_NAMESPACES_PER_SUBSYS=32

CR_GROUP = "storage.simplyblock.io"
CR_VERSION  = "v1alpha1"

# Grafana alert rules read from the cluster event log rather than from Thanos,
# provisioned by `sbctl cluster event-alerts`. The plugin id is both the folder
# Grafana installs the plugin into and the data source `type` the rules use.
# 2.12.2 is the last Infinity release compatible with Grafana 10.0.12.
GRAFANA_EVENT_ALERTS_PLUGIN_ID = "yesoreyeram-infinity-datasource"
GRAFANA_EVENT_ALERTS_PLUGIN_URL = (
    "https://grafana.com/api/plugins/yesoreyeram-infinity-datasource/versions/2.12.2/download")

# The control plane as reached from the monitoring stack. HAProxy's default
# backend is the web API, so /api/v2 needs no route of its own; the same host
# prometheus.yml.j2 scrapes /cluster/metrics from.
MONITORING_CONTROL_PLANE_ADDR = "http://HAProxy"
MONITORING_GRAFANA_SERVICE = "monitoring_grafana"

GRAFANA_K8S_ENDPOINT = "http://simplyblock-grafana:3000"
GRAYLOG_K8S_ENDPOINT = "http://simplyblock-graylog:9000"
OS_K8S_ENDPOINT = "http://opensearch-cluster-master:9200"

WEBAPI_K8S_ENDPOINT = "http://simplyblock-webappapi:5000/api/v2"

K8S_NAMESPACE = os.getenv('K8S_NAMESPACE', 'simplyblock')
OS_STATEFULSET_NAME = "simplyblock-opensearch"
MONGODB_STATEFULSET_NAME = "simplyblock-mongo"
GRAYLOG_STATEFULSET_NAME = "simplyblock-graylog"
PROMETHEUS_STATEFULSET_NAME = os.getenv('PROMETHEUS_URL', "simplyblock-prometheus")
PROMETHEUS_STATEFULSET_PORT = os.getenv('PROMETHEUS_PORT', "9090")
FDB_SERVICE_NAME = "simplyblock-fdb-cluster"
FDB_CONFIG_NAME = "simplyblock-fdb-cluster-config"
ADMIN_DEPLOY_NAME = "simplyblock-admin-control"

os_env_patch = [
    {"name": "OPENSEARCH_JAVA_OPTS", "value": "-Xms1g -Xmx1g"},
    {"name": "bootstrap.memory_lock", "value": "false"},
    {"name": "action.auto_create_index", "value": "false"},
    {"name": "plugins.security.ssl.http.enabled", "value": "false"},
    {"name": "plugins.security.disabled", "value": "true"},
    {"name": "discovery.type", "value": ""},
    {"name": "discovery.seed_hosts", "value": ",".join([
        "simplyblock-opensearch-0.opensearch-cluster-master-headless",
        "simplyblock-opensearch-1.opensearch-cluster-master-headless",
        "simplyblock-opensearch-2.opensearch-cluster-master-headless"
    ])},
    {"name": "cluster.initial_master_nodes", "value": ",".join([
        "simplyblock-opensearch-0",
        "simplyblock-opensearch-1",
        "simplyblock-opensearch-2"
    ])}
]

os_patch = {
    "spec": {
        "replicas": 3,
        "template": {
            "spec": {
                "containers": [
                    {
                        "name": "opensearch",
                        "env": os_env_patch
                    }
                ]
            }
        }
    }
}

mongodb_patch = {
    "spec": {
        "members": 3,
    }
}

prometheus_patch = {
    "spec": {
        "replicas": 3,
    }
}

qos_class_meta_and_migration_weight_percent = 25

MIG_PARALLEL_JOBS = 64
MIG_JOB_SIZE = 64

# Live volume migration constants
LVOL_MIG_MAX_RETRIES = 5          # max retries before entering cleanup_target
LVOL_MIG_DEADLINE_SEC = 3600  # 1-hour deadline (0 = no deadline)
LVOL_MIG_MAX_INTERMEDIATE_SNAPS = 3        # max recursive "shrink" snapshot rounds
LVOL_MIG_INTERMEDIATE_SNAP_THRESHOLD_BYTES = 500 * 1024 * 1024  # 500 MiB — skip if delta is smaller
LVOL_MIG_BDEV_SUFFIX = 'm'  # appended to every migration bdev on the target to avoid collision with real bdevs

#: How long a deferred lvol register task tolerates a missing lvol record
#: before treating it as obsolete. add_lvol_ha queues the task in its
#: pre-check but writes the lvol record only at the end of the create, so
#: a task picked up inside that window must wait for the record rather
#: than conclude the volume was deleted and drop the registration.
LVOL_SYNC_OP_RECORD_GRACE_SEC = 600

#: How long a force/recovery delete waits for the chain and lvstore locks
#: before proceeding without them. It used to skip them outright, so a
#: forced delete could interleave with any create/delete/resize on the same
#: chain; it must still not block forever behind a holder that died on a
#: node that is now gone.
FORCE_DELETE_LOCK_WAIT_SEC = 30

#: Consecutive genuine failures of a single deferred lvol sync-delete or
#: registration before it is escalated to the cluster event log. Repeated
#: failure of one object on one node is not a transient condition -- these
#: tasks retry forever by design, so without an alert a permanently stuck
#: leg is invisible except as a volume that never leaves in_deletion, or a
#: replica that is silently missing. Deferrals for a node that is simply
#: not ONLINE yet, or an LVS owned by a restart, are NOT failures and do
#: not count.
TASK_FAILURE_ALERT_THRESHOLD = 3

#: How long an operation holding the chain lock waits for a peer to leave
#: its restart phase before giving up and deferring the leg durably.
#: A restart is a bounded, self-clearing condition, so waiting keeps the
#: whole [create + registers] / [async delete + sync deletes] sequence
#: under one lock instead of fragmenting it across processes. The cap
#: matters: RESTART_TASK_EXEC_INTERVAL_MAX_SEC is 3600 and a wedged
#: restart really can sit for an hour (2026-08-27, a node stuck offline
#: while three restart tasks reported success), and holding a chain lock
#: that long would stall every create and delete on that chain.
DEFERRED_LEG_RESTART_WAIT_SEC = 120

# NVMe-oF TLS / DH-HMAC-CHAP security
VALID_DHCHAP_DIGESTS = ["sha256", "sha384", "sha512"]
VALID_DHCHAP_DHGROUPS = ["null", "ffdhe2048", "ffdhe3072", "ffdhe4096", "ffdhe6144", "ffdhe8192"]

# Fixed pool-level DHCHAP settings: all main digests and weakest DH group only
DHCHAP_DIGESTS = ["sha256", "sha384", "sha512"]
DHCHAP_DHGROUP = "ffdhe2048"

# Default port ranges (configurable per-cluster via Cluster model fields)
NVMF_BASE_PORT = 4420         # Base port for ALL NVMe-oF listeners (lvol, hublvol, device)
RPC_BASE_PORT = 8080          # Base port for SPDK JSON-RPC
SNODE_API_PORT = 50001        # SNodeAPI/firewall port base — allocated per SPDK node, not per host

# Legacy constants kept for backward compatibility with env override
LVOL_NVMF_PORT_ENV = os.getenv("LVOL_NVMF_PORT_START", "")
if LVOL_NVMF_PORT_ENV:
    NVMF_BASE_PORT = int(LVOL_NVMF_PORT_ENV)

# Backward compatibility aliases
RPC_PORT_RANGE_START = RPC_BASE_PORT
FW_PORT_START = SNODE_API_PORT
LVOL_NVMF_PORT_START = NVMF_BASE_PORT
NODE_NVMF_PORT_START = NVMF_BASE_PORT
NODE_HUBLVOL_PORT_START = NVMF_BASE_PORT

# S3 Backup constants
BACKUP_POLL_INTERVAL_SEC = 5
BACKUP_MAX_RETRIES = 10
BACKUP_MERGE_SERVICE_INTERVAL_SEC = 60
BACKUP_S3_METADATA_BUCKET = "simplyblock-backup-metadata"

TASKS_RETENTION_PERIOD_SEC = 60*60*24*30 # 30 days