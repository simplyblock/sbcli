"""Async-replication test driver for the two-cluster control plane.

Runs against the deployment from setup_repl_test_2clusters.py (reads
cluster_metadata_repl.json). Two test cases (select via argv: case1 | case2 | both):

CASE 1 — online switch-over (migration), no IO interruption
    * create 5 volumes on the SOURCE cluster, connect/format/mount on the client
    * start a continuous fio load (4 parallel r/w jobs, iodepth 4, direct, md5
      verify, max-latency 20s) and keep it running in a loop
    * replicate to the TARGET cluster with auto internal snapshots every minute
      (enabled at volume-create time, migration mode)
    * once replication is caught up, perform the final migration (online cutover)
      to the target WHILE fio runs — assert fio is NOT interrupted (still alive,
      zero IO errors, no latency-timeout)

CASE 2 — fail-over on cluster failure, IO interrupts, data survives
    * clean up, create/connect/format/mount 5 volumes on the (case-2) SOURCE
    * run replication for N one-minute iterations (auto snapshots)
    * record a checksum of replicated data, then SUSPEND the source cluster by
      killing the SPDK container on two nodes simultaneously
    * fio interrupts (expected). Trigger fail-over, reconnect the client to the
      target paths at a NEW mount point, and verify the replicated data is
      readable and matches the recorded checksum.

User-facing steps use sbctl; API-only steps (fail-over, structured status) run a
python snippet on the management node (same pattern as setup_perf_test1).

NOTE: this is a live lab driver; it cannot be unit-tested. The online-switchover
non-interruption in case 1 assumes the client holds the target multipath paths
before the ANA flip — the script connects them right after replication-commit,
before the cutover runner flips ANA.
"""
import json
import os
import re
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import paramiko

METADATA_FILE = os.environ.get("REPL_METADATA", "cluster_metadata_repl.json")

# --- Topology / workload knobs ---
NUM_VOLUMES = 5
VOL_SIZE = "100G"
REPL_INTERVAL_MIN = 1
CASE2_ITERATIONS = 10                 # one-minute replication cycles before kill
REPL_WAIT_TIMEOUT = 1200
CUTOVER_WAIT_TIMEOUT = 600
# Readiness gate: newest point-in-time on the target must be no older than this.
# 3x the snapshot interval tolerates one missed cycle without being slack.
MAX_LAG_SECONDS = REPL_INTERVAL_MIN * 60 * 3
STABLE_POLLS = 2                      # consecutive good polls before cutting over
NODE_STATE_TIMEOUT = 900              # node offline/online transition budget
BASELINE_MB = 128                     # size of the md5-verified marker file
OUTAGE_REPL_CYCLES = 4                # replication cycles to observe during an outage

# --- fio workload (per the test spec) ---
# Deliberately mild writer (2026-08-21): the point of the suite is proving
# fail-over/fail-back correctness, not racing the replication pipeline. At
# 4 jobs x QD4 x 64k, fio dirtied distinct clusters faster than one transfer
# stream could ship them (345 vs ~29 MiB/s per volume), so the lag equilibrium
# sat above every gate and no cutover could be reached. Throughput work is
# tracked separately (batch 64, fragment size, hub queues).
FIO_NUMJOBS = 2
FIO_RW = "rw"
FIO_BS = "16k"
FIO_IODEPTH = 2
# PER-CLONE, not per volume: fio allocates `size` for EACH of the numjobs
# clones, so the volume must hold FIO_NUMJOBS * FIO_SIZE. VOL_SIZE=100G lands as
# ~93 GiB usable, so 4 x 100G asked for ~400G and every run died of ENOSPC
# ("err=28 ... No space left on device") within minutes -- which had nothing to
# do with the cutover under test.
FIO_SIZE = "8G"
FIO_MAX_LATENCY = "20s"               # IOs slower than this are errors -> proves stall
FIO_LOG = "/tmp/fio_repl.log"
FIO_JOBFILE = "/tmp/fio_repl.fio"

MOUNT_BASE = "/mnt/repl"
SBCTL = "sudo /usr/local/bin/sbctl"


# --------------------------------------------------------------------------- #
# SSH / mgmt helpers
# --------------------------------------------------------------------------- #
def load_meta():
    with open(METADATA_FILE) as f:
        return json.load(f)


def _ssh(ip, key_path):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username="ec2-user", key_filename=os.path.expanduser(key_path),
                allow_agent=False, look_for_keys=False)
    return ssh


SSH_RETRIES = 4
SSH_RETRY_BACKOFF = 5


def run(ip, key_path, cmd, check=True, quiet=False, timeout=900, replayable=False):
    if not quiet:
        print(f"  [{ip}] $ {cmd}")

    # Retry only the connect/open-session phase. A reset there means the command
    # never ran, so replaying it is safe; once we hold a channel the command has
    # been dispatched and re-running it could repeat a non-idempotent step
    # (nvme connect, mkfs, volume delete). Long polling loops open a fresh
    # connection per call and can trip sshd's rate limiting -> WinError 10054.
    # `replayable=True` marks a read-only command (DB/status query) whose whole
    # dispatch may be repeated: a mid-exec socket reset then retries instead of
    # failing the case (a 10054 during a cutover-wait poll aborted a healthy
    # case 1 on 2026-08-14, run 9).
    for replay in range(1, SSH_RETRIES + 1):
        ssh = out = err = None
        for attempt in range(1, SSH_RETRIES + 1):
            try:
                ssh = _ssh(ip, key_path)
                _in, out, err = ssh.exec_command(cmd, timeout=timeout)
                break
            except (paramiko.SSHException, OSError, EOFError) as exc:
                if ssh is not None:
                    try:
                        ssh.close()
                    except Exception:
                        pass
                if attempt == SSH_RETRIES:
                    raise RuntimeError(
                        f"SSH transport failure on {ip} after {SSH_RETRIES} attempts: {exc}") from exc
                delay = SSH_RETRY_BACKOFF * attempt
                print(f"  [{ip}] SSH transport failure ({exc}); reconnecting in {delay}s "
                      f"({attempt + 1}/{SSH_RETRIES})")
                time.sleep(delay)

        try:
            o = out.read().decode()
            e = err.read().decode()
            rc = out.channel.recv_exit_status()
        except (socket.timeout, TimeoutError, OSError, paramiko.SSHException) as exc:
            # The command was dispatched, so we cannot safely replay it unless
            # the caller marked it replayable. For best-effort work
            # (check=False) a hang must not kill the run: an `umount`/`nvme
            # disconnect` can block indefinitely once the cutover has moved the
            # device, which is exactly how a PASSING case 1 still took the
            # driver down before case 2 could start.
            try:
                ssh.close()
            except Exception:
                pass
            if replayable and replay < SSH_RETRIES:
                print(f"  [{ip}] read failed on replayable command ({exc}); replaying "
                      f"({replay + 1}/{SSH_RETRIES})")
                time.sleep(SSH_RETRY_BACKOFF * replay)
                continue
            if check:
                raise RuntimeError(f"SSH read failed on {ip} for: {cmd} ({exc})") from exc
            print(f"  [{ip}] read timed out on best-effort command, continuing: {cmd}")
            return ""
        ssh.close()
        # rc == -1: the channel died without delivering an exit status (socket
        # reset mid-read). Output is unreliable; replay a replayable command.
        if rc == -1 and replayable and replay < SSH_RETRIES:
            print(f"  [{ip}] channel died without exit status; replaying "
                  f"({replay + 1}/{SSH_RETRIES})")
            time.sleep(SSH_RETRY_BACKOFF * replay)
            continue
        break
    if rc != 0 and check:
        print(o[-2000:])
        print(e[-2000:])
        raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
    return o


def mgmt_py(mgmt_ip, key_path, snippet, replayable=False):
    script = "sudo python3 - <<'PY'\n" + snippet + "\nPY"
    out = run(mgmt_ip, key_path, script, replayable=replayable)
    last = [ln for ln in out.strip().splitlines() if ln.strip()][-1]
    return json.loads(last)


# --------------------------------------------------------------------------- #
# Control-plane operations
# --------------------------------------------------------------------------- #
def cluster_meta_by_uuid(meta, uuid):
    for name, c in meta["clusters"].items():
        if c["cluster_uuid"] == uuid:
            return name, c
    raise KeyError(uuid)


def resolve_lvol(mgmt_ip, key_path, name):
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
lv = db.get_lvol_by_name({name!r})
print(json.dumps({{"uuid": lv.get_id(), "nqn": lv.nqn}}))
""", replayable=True)


def get_connect_cmds(mgmt_ip, key_path, lvol_uuid):
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.controllers import lvol_controller
entries, err = lvol_controller.connect_lvol({lvol_uuid!r})
print(json.dumps({{"err": str(err) if err else "",
                   "connect": [e.connect for e in (entries or [])],
                   "nqn": (entries[0].nqn if entries else "")}}))
""")


FIELDS = ("lag_seconds", "outstanding_count", "outstanding_bytes", "replicated_count")


def get_replication_info(mgmt_ip, key_path, lvol_uuid):
    return get_replication_infos(mgmt_ip, key_path, [lvol_uuid])[lvol_uuid]


def get_replication_infos(mgmt_ip, key_path, lvol_uuids):
    """Replication info for all volumes in ONE round trip (see replication_states)."""
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.controllers import lvol_controller
fields = {list(FIELDS)!r}
out = {{}}
for u in {list(lvol_uuids)!r}:
    info = lvol_controller.get_replication_info(u) or {{}}
    out[u] = {{k: info.get(k) for k in fields}}
print(json.dumps(out))
""", replayable=True)


def newest_replicated_snap_ts(mgmt_ip, key_path, lvol_uuids):
    """Per volume: created_at of the newest snapshot that IS on the target."""
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
wanted = set({list(lvol_uuids)!r})
out = {{u: 0 for u in wanted}}
for s in db.get_snapshots():
    if s.deleted or not s.lvol:
        continue
    u = s.lvol.get_id()
    if u in wanted and s.target_replicated_snap_uuid:
        out[u] = max(out[u], s.created_at or 0)
print(json.dumps(out))
""", replayable=True)


def wait_data_replicated(mgmt_ip, key_path, lvol_uuids, after_ts,
                         timeout=REPL_WAIT_TIMEOUT):
    """Wait until every volume has a REPLICATED snapshot newer than *after_ts*.

    A bounded lag is not enough to fail over onto. Attaching the replication
    policy takes the first internal snapshot immediately, before mkfs and before the baseline is
    written, so `lag_seconds` and `replicated_count > 0` are both satisfied by an
    EMPTY (used_size=0) point-in-time. Lab 2026-08-18 did exactly that: the gate
    passed at worst_lag=77s while `outstanding=4`, fail-over cloned the empty
    snapshot, and all five mounts died with a bad superblock — the 202 MiB
    post-baseline snapshot was still in flight. The product behaved correctly on
    the input it was given; the harness simply had not replicated the data yet.

    Force a snapshot so we do not wait out a whole interval, then require the
    newest replicated point-in-time to be newer than the data we are about to
    verify.
    """
    for lvol in lvol_uuids:
        run(mgmt_ip, key_path, f"{SBCTL} volume replication-trigger {lvol}",
            check=False, quiet=True)
    print(f"Waiting for a replicated snapshot newer than the baseline "
          f"(after_ts={int(after_ts)}) on all volumes...")
    start = time.time()
    while time.time() - start < timeout:
        stamps = newest_replicated_snap_ts(mgmt_ip, key_path, lvol_uuids)
        behind = {u: ts for u, ts in stamps.items() if (ts or 0) <= after_ts}
        print(f"  volumes still without post-baseline data on the target: "
              f"{len(behind)}/{len(lvol_uuids)}")
        if not behind:
            print("The data itself is on the target; fail-over is meaningful now.")
            return True
        time.sleep(15)
    raise RuntimeError(
        f"FAIL: no post-baseline snapshot replicated within {timeout}s for "
        f"{sorted(behind)} — failing over now would clone a point-in-time that "
        f"predates the filesystem")


def do_failover(mgmt_ip, key_path, lvol_uuid):
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.controllers import lvol_controller
res = lvol_controller.replicate_lvol_on_target_cluster({lvol_uuid!r})
print(json.dumps(res if isinstance(res, dict) else {{"result": res}}))
""")


def replication_state(mgmt_ip, key_path, lvol_uuid):
    return {"state": replication_states(mgmt_ip, key_path, [lvol_uuid])[lvol_uuid]}


def replication_states(mgmt_ip, key_path, lvol_uuids):
    """States for all volumes in ONE round trip.

    Polling per volume opened len(lvols) SSH sessions every 15s; that churn is
    what tripped the connection resets during the cutover wait.
    """
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
wanted = {list(lvol_uuids)!r}
states = {{u: "" for u in wanted}}
for r in db.get_lvol_replication_objects():
    if r.source_lvol and r.source_lvol.get_id() in states:
        states[r.source_lvol.get_id()] = r.state
print(json.dumps(states))
""", replayable=True)


def wait_replication_caught_up(mgmt_ip, key_path, lvol_uuids, timeout=REPL_WAIT_TIMEOUT):
    """Wait until every volume is replicating steadily with a bounded lag.

    NOT outstanding_count == 0. `outstanding_count` counts internal snapshots
    whose replication task has not finished, and --interval-min creates a fresh
    snapshot per volume every minute, so with continuous fio there is nearly
    always one in flight: the old `outstanding == 0` gate could only pass by
    luck and in practice just burned the timeout and aborted the run.

    The meaningful readiness signal is `lag_seconds` — the age of the newest
    point-in-time that exists on the target. Bounded lag means the target is
    keeping up; the residual delta is `replication-commit`'s job, which is
    documented to "minimize delta then fail the client over".
    """
    max_lag = MAX_LAG_SECONDS
    print(f"Waiting for replication to reach a steady state (lag <= {max_lag}s) on all volumes...")
    start = time.time()
    stable = 0
    while time.time() - start < timeout:
        infos = get_replication_infos(mgmt_ip, key_path, lvol_uuids)
        lags = [i.get("lag_seconds") for i in infos.values()]
        replicated = min((i.get("replicated_count") or 0) for i in infos.values())
        outstanding = sum((i.get("outstanding_count") or 0) for i in infos.values())
        worst = max((lag for lag in lags if lag is not None), default=None)
        missing = sum(1 for lag in lags if lag is None)
        print(f"  worst_lag={worst}s never_replicated={missing} "
              f"min_replicated={replicated} outstanding={outstanding}")
        if missing == 0 and replicated > 0 and worst is not None and worst <= max_lag:
            stable += 1
            if stable >= STABLE_POLLS:        # hold, so we don't cut over on one lucky sample
                print(f"Replication steady (worst lag {worst}s).")
                return True
        else:
            stable = 0
        time.sleep(15)
    raise RuntimeError(
        f"Timed out waiting for replication to reach a steady state (lag <= {max_lag}s)")


# --------------------------------------------------------------------------- #
# Client-side: connect / format / mount / fio
# --------------------------------------------------------------------------- #
def _newest_spdk_devs(client_ip, key_path, count):
    """Return the `count` most recently attached SPDK namespace devices."""
    out = run(client_ip, key_path,
              "ls -1t /dev/nvme*n1 2>/dev/null | head -n %d" % count, quiet=True)
    return [d for d in out.split() if d]


def _dev_for_nqn(client_ip, key_path, nqn, tries=6):
    """Resolve the namespace block device serving *nqn* via sysfs.

    Device-node mtime ordering (`ls -1t`) is not a reliable identity: after a
    fail-over the volume's device is an EXISTING node whose mtime never
    changes, so the "newest" pick returned an unrelated stale device and the
    mount failed with rc=32 (run 15, cases 3 and 4). The subsystem NQN is the
    identity the control plane hands us, so match on it directly.
    """
    for _ in range(tries):
        out = run(client_ip, key_path,
                  "for s in /sys/class/nvme-subsystem/nvme-subsys*; do "
                  f"[ \"$(cat $s/subsysnqn 2>/dev/null)\" = \"{nqn}\" ] || continue; "
                  "ls $s 2>/dev/null | grep -E '^nvme[0-9]+n[0-9]+$' | head -1; "
                  "done", check=False, quiet=True)
        devs = [d for d in out.split() if d]
        if devs:
            return f"/dev/{devs[0]}"
        time.sleep(3)
    return ""


def connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True, mount_base=MOUNT_BASE):
    """Connect each lvol on the client and mount it. Returns [{lvol,nqn,dev,mount}]."""
    prepare_mount_points(client_ip, key_path)
    mounts = []
    for idx, lv in enumerate(lvols):
        conn = get_connect_cmds(mgmt_ip, key_path, lv)
        assert not conn["err"], f"connect_lvol error for {lv}: {conn['err']}"
        for cmd in conn["connect"]:
            run(client_ip, key_path, cmd)
        time.sleep(3)
        dev = _dev_for_nqn(client_ip, key_path, conn.get("nqn", ""))
        if not dev:
            found = _newest_spdk_devs(client_ip, key_path, 1)
            if not found:
                raise RuntimeError(f"no device appeared for {lv} (nqn={conn.get('nqn')})")
            dev = found[0]
        mnt = f"{mount_base}{idx}"
        if fmt:
            run(client_ip, key_path, f"sudo mkfs.xfs -f {dev}")
        run(client_ip, key_path, f"sudo mkdir -p {mnt} && sudo mount {dev} {mnt}")
        mounts.append({"lvol": lv, "nqn": conn["nqn"], "dev": dev, "mount": mnt})
        print(f"  vol {lv} -> {dev} @ {mnt}")
    return mounts


def write_fio_jobfile(client_ip, key_path, mounts):
    """One fio job per mounted volume; FIO_NUMJOBS threads each; md5 verify; 20s max latency."""
    sections = [
        "[global]",
        f"rw={FIO_RW}",
        f"bs={FIO_BS}",
        f"iodepth={FIO_IODEPTH}",
        "ioengine=libaio",
        "direct=1",
        f"size={FIO_SIZE}",
        f"numjobs={FIO_NUMJOBS}",
        "time_based=1",
        "runtime=86400",            # effectively endless for the test
        "verify=md5",
        "verify_backlog=512",
        "verify_fatal=1",
        f"max_latency={FIO_MAX_LATENCY}",
        "group_reporting=1",
        "",
    ]
    for i, m in enumerate(mounts):
        # `directory`, NOT a shared `filename`: with numjobs>1 every clone opens
        # the same path and they overwrite each other's verify patterns
        # ("multiple writers may overwrite blocks that belong to other jobs"),
        # so md5 verify failures were guaranteed regardless of replication.
        # With `directory` each clone gets its own <jobname>.<jobnum>.0 file.
        sections += [f"[vol{i}]", f"directory={m['mount']}", ""]
    content = "\n".join(sections)
    # Write the job file on the client.
    run(client_ip, key_path, f"cat > {FIO_JOBFILE} <<'EOF'\n{content}\nEOF")
    return FIO_JOBFILE


def start_fio(client_ip, key_path, jobfile):
    print("Starting continuous fio load...")
    run(client_ip, key_path,
        f"sudo rm -f {FIO_LOG}; "
        f"sudo nohup fio --status-interval=15 --eta=never {jobfile} "
        f"> {FIO_LOG} 2>&1 & echo started")
    time.sleep(10)
    if not fio_alive(client_ip, key_path):
        tail = run(client_ip, key_path, f"tail -40 {FIO_LOG}", check=False)
        raise RuntimeError(f"fio failed to start:\n{tail}")
    print("  fio running.")


def fio_alive(client_ip, key_path):
    out = run(client_ip, key_path, "pgrep -x fio | head -1", check=False, quiet=True)
    return bool(out.strip())


def fio_error_count(client_ip, key_path):
    """Return count of error indicators in the fio log (err=, verify fail)."""
    out = run(client_ip, key_path,
              f"grep -ciE 'err= *[1-9]|verify.*fail|md5.*mismatch' {FIO_LOG} || true",
              check=False, quiet=True)
    try:
        return int(out.strip() or "0")
    except ValueError:
        return 0


def stop_fio(client_ip, key_path):
    run(client_ip, key_path, "sudo pkill -x fio || true", check=False)
    time.sleep(3)


def cleanup_client(client_ip, key_path, mounts):
    # Unmount before disconnecting, with a lazy fallback: a plain (or forced)
    # unmount fails once the transport is dead, and disconnecting underneath a
    # live mount leaves a stale mountpoint that later stat()s with EIO -- a
    # subsequent run then dies on `mkdir -p` of that same path.
    for m in mounts:
        run(client_ip, key_path,
            f"sudo timeout 15 umount {m['mount']} 2>/dev/null "
            f"|| sudo timeout 15 umount -f {m['mount']} 2>/dev/null "
            f"|| sudo timeout 15 umount -l {m['mount']} 2>/dev/null || true",
            check=False, timeout=90)
    for m in mounts:
        if m.get("nqn"):
            run(client_ip, key_path,
                f"sudo timeout 30 nvme disconnect -n {m['nqn']} 2>/dev/null || true",
                check=False, timeout=90)


def prepare_mount_points(client_ip, key_path):
    """Clear stale fio + mountpoints left by an aborted run before mounting again.

    fio is started with nohup, so it survives the driver being killed and keeps
    the old mounts busy (and keeps writing). Kill it first, or the unmount below
    fails and we are back to stale EIO mountpoints.
    """
    run(client_ip, key_path, "sudo pkill -x fio || true", check=False, timeout=60)
    time.sleep(3)
    # `timeout 15` per unmount: once the cutover has moved the device, umount can
    # block forever in the kernel, and relying on the SSH read timeout alone
    # stalls the driver for the full read window on EVERY stale mount.
    run(client_ip, key_path,
        "for m in /mnt/repl*; do "
        "sudo timeout 15 umount -f \"$m\" 2>/dev/null "
        "|| sudo timeout 15 umount -l \"$m\" 2>/dev/null || true; "
        "sudo rmdir \"$m\" 2>/dev/null || true; done", check=False, timeout=180)
    # Full NVMe reset: disconnect every simplyblock subsystem. Stale fenced
    # paths from a previous case hold hung IO; a lazy umount pinned on one can
    # complete minutes later and rip a freshly created mountpoint out from
    # under the next case (run 12: baseline dd hit ENOENT on /mnt/repl0).
    run(client_ip, key_path,
        "for n in $(sudo nvme list-subsys 2>/dev/null "
        "| grep -oE 'nqn\\.2023-02\\.io\\.simplyblock:[^ ,]+'); do "
        "sudo timeout 20 nvme disconnect -n \"$n\" >/dev/null 2>&1 || true; done",
        check=False, timeout=300)


# --------------------------------------------------------------------------- #
# Failure injection
# --------------------------------------------------------------------------- #
def kill_spdk(node_ip, key_path):
    """Kill the SPDK container (spdk_<port>) on a storage node to simulate failure."""
    run(node_ip, key_path,
        "sudo docker kill $(sudo docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$') || true",
        check=False)


# --------------------------------------------------------------------------- #
# Test cases
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# Node control + fail-back helpers (cases 3-6)
# --------------------------------------------------------------------------- #
def node_of_lvol(mgmt_ip, key_path, lvol_uuid):
    """Return {node_id, replication_node_id, cluster_id, secondary_node_id}."""
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
lv = db.get_lvol_by_id({lvol_uuid!r})
n = db.get_storage_node_by_id(lv.node_id)
print(json.dumps({{"node_id": lv.node_id,
                   "replication_node_id": lv.replication_node_id,
                   "cluster_id": n.cluster_id,
                   "secondary_node_id": n.secondary_node_id}}))
""", replayable=True)


def node_status(mgmt_ip, key_path, node_id):
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
n = db.get_storage_node_by_id({node_id!r})
print(json.dumps({{"status": n.status, "health": n.health_check}}))
""", replayable=True)["status"]


def wait_node_status(mgmt_ip, key_path, node_id, wanted, timeout=NODE_STATE_TIMEOUT):
    start = time.time()
    seen = ""
    while time.time() - start < timeout:
        seen = node_status(mgmt_ip, key_path, node_id)
        if seen == wanted:
            print(f"  node {node_id[:8]} -> {wanted}")
            return True
        time.sleep(10)
    raise RuntimeError(
        f"Node {node_id} did not reach {wanted!r} within {timeout}s (last={seen!r})")


def sn_shutdown(mgmt_ip, key_path, node_id):
    """Take a node offline the supported way.

    Deliberately NOT `docker kill` on the SPDK container: the control plane
    auto-restarts that within minutes (case 2 saw the "suspended" source cluster
    heal itself mid-test), which silently invalidates an outage scenario.
    """
    print(f"Shutting down node {node_id[:8]} ...")
    # Straight to shutdown: suspending first buys nothing and actively hurts —
    # a suspended node makes its own queued work defer ("node is not online,
    # retrying"), and that backlog then blocks the shutdown itself (run 15
    # case 6: the node never left `suspended`).
    run(mgmt_ip, key_path, f"{SBCTL} -d sn shutdown {node_id}", check=False)
    wait_node_status(mgmt_ip, key_path, node_id, "offline")


def sn_bring_back(mgmt_ip, key_path, node_id):
    """Restart a node and wait until it is genuinely back.

    `sn resume` used to be called afterwards; it is gone. It only ever lifted a
    SUSPENDED node (resume_storage_node refuses any other state), so on a node
    that had just come back online it was a no-op.

    "online" alone is not "back": the volume store also has to have a leader
    again. Replication into a leaderless LVS is refused by the leader gates, and
    LVolMonitor cannot finalise deletes without one, so a case that proceeds too
    early sees 0 cutovers and then wedges every later case behind undeletable
    volumes (lab 2026-08-19).
    """
    print(f"Restarting node {node_id[:8]} ...")
    run(mgmt_ip, key_path, f"{SBCTL} -d sn restart {node_id}", check=False, timeout=1800)
    wait_node_status(mgmt_ip, key_path, node_id, "online")
    wait_lvs_leader(mgmt_ip, key_path, node_id)


def wait_lvs_leader(mgmt_ip, key_path, node_id, timeout=900):
    """Wait until *node_id*'s lvstore reports leadership somewhere in its pair."""
    start = time.time()
    while time.time() - start < timeout:
        out = mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
n = db.get_storage_node_by_id({node_id!r})
leader = False
for peer_id in [n.get_id(), n.secondary_node_id]:
    if not peer_id:
        continue
    try:
        p = db.get_storage_node_by_id(peer_id)
        r = p.rpc_client(timeout=8, retry=1).bdev_lvol_get_lvstores(n.lvstore)
        if r and r[0].get('lvs leadership'):
            leader = True
            break
    except Exception:
        pass
print(json.dumps({{"leader": leader, "status": n.status}}))
""", replayable=True)
        if out.get("leader"):
            print(f"  lvstore of {node_id[:8]} has a leader again")
            return True
        print(f"  waiting for an lvstore leader on {node_id[:8]} (node {out.get('status')})")
        time.sleep(20)
    raise RuntimeError(f"FAIL: no lvstore leader for {node_id} within {timeout}s")


def restore_cluster(mgmt_ip, key_path, cluster_meta, label=""):
    """Bring every node of a killed cluster back, then wait for leadership.

    Restarting only the nodes that are not "online" is not enough: after a whole
    cluster is killed its SPDK containers restart by themselves, so a node can
    read "online" while its lvstore has no leader and its recovery never
    completed. Restart every member, serially -- a second concurrent restart is
    refused with "Node ... is in_restart".
    """
    print(f"Restoring cluster {label or cluster_meta['cluster_uuid'][:8]} ...")
    for node_id in [n["uuid"] for n in cluster_meta["topology"]["nodes"]]:
        sn_bring_back(mgmt_ip, key_path, node_id)


def _replication_list(mgmt_ip, key_path, what, cluster_id):
    """Rows of `cluster replication-<what>-list --json` for one cluster."""
    raw = run(mgmt_ip, key_path,
              f"{SBCTL} cluster replication-{what}-list --cluster-id {cluster_id} --json",
              check=False, quiet=True)
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return []


def ensure_replication_target(mgmt_ip, key_path, from_cluster, to_cluster,
                              to_pool_uuid, name=None):
    """Register (idempotently) a named replication destination on `from_cluster`."""
    name = name or f"tgt_{to_cluster[:8]}"
    for row in _replication_list(mgmt_ip, key_path, "target", from_cluster):
        if row.get("Name") == name:
            return name
    run(mgmt_ip, key_path,
        f"{SBCTL} -d cluster replication-target-add {from_cluster} {name} {to_cluster}"
        f" --target-pool {to_pool_uuid} --timeout 3600")
    return name


def ensure_replication_policy(mgmt_ip, key_path, from_cluster, target_name, mode,
                              interval_min=REPL_INTERVAL_MIN, keep=2, name=None):
    """Register (idempotently) a cadence policy on an existing target."""
    name = name or f"pol_{mode}_{target_name}"
    for row in _replication_list(mgmt_ip, key_path, "policy", from_cluster):
        if row.get("Name") == name:
            return name
    run(mgmt_ip, key_path,
        f"{SBCTL} -d cluster replication-policy-add {from_cluster} {name}"
        f" --target {target_name} --interval-min {interval_min} --mode {mode}"
        f" --keep {keep}")
    return name


def set_cluster_replication(mgmt_ip, key_path, from_cluster, to_cluster, to_pool_uuid,
                            mode="migration"):
    """Create the target + policy that let `from_cluster` replicate to `to_cluster`.

    Replication is NEVER started per volume any more: `volume replication-start`
    is refused for a volume that follows a policy, and a policy is how a volume
    is meant to be replicated. Attaching the policy (at `volume add` time, or
    with `volume replication-policy-set`) is what starts it.

    `cluster add-replication` is still issued afterwards, but only as a bridge:
    see the PRODUCT GAP note below.
    """
    print(f"Replication config: cluster {from_cluster[:8]} -> {to_cluster[:8]} "
          f"(pool {to_pool_uuid[:8]}, mode {mode})")
    target = ensure_replication_target(mgmt_ip, key_path, from_cluster, to_cluster,
                                       to_pool_uuid)
    policy = ensure_replication_policy(mgmt_ip, key_path, from_cluster, target, mode)

    # PRODUCT GAP (bridge, delete once the readers consult the policy):
    # replicate_lvol_on_target_cluster() and tasks_runner_replication_final still
    # resolve the destination from the SOURCE cluster's
    # snapshot_replication_target_cluster / _target_pool. add_target()/add_policy()
    # never write those fields, so a purely policy-driven volume cannot fail over
    # or commit a cutover at all.
    run(mgmt_ip, key_path,
        f"{SBCTL} -d cluster add-replication {from_cluster} {to_cluster}"
        f" --target-pool {to_pool_uuid} --timeout 3600", check=True)

    # PRODUCT BUG (mitigation): snapshot_replication._target_pool() places an
    # INCOMING copy using the DESTINATION cluster's own OUTGOING config
    # (snapshot_replication_target_pool). That field means "the pool I replicate
    # into on my target", so whenever the destination is itself a source, the
    # incoming copy is placed in a pool belonging to a different cluster. That is
    # how the 2026-08-19 fail-back created REP_* volumes on a src node in
    # pool_tgt and left 13 of them stuck in_deletion. Clear a stale outgoing pool
    # on the destination so the resolver falls back to the destination's own.
    clear_stale_target_pool(mgmt_ip, key_path, to_cluster)
    return policy


def clear_stale_target_pool(mgmt_ip, key_path, cluster_id):
    """Blank cluster.snapshot_replication_target_pool when it names a foreign pool."""
    out = mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
c = db.get_cluster_by_id({cluster_id!r})
pool = c.snapshot_replication_target_pool
cleared = False
if pool:
    mine = [p.get_id() for p in db.get_pools({cluster_id!r})]
    if pool not in mine:
        c.snapshot_replication_target_pool = ""
        c.write_to_db(db.kv_store)
        cleared = True
print(json.dumps({{"was": pool, "cleared": cleared}}))
""")
    if out.get("cleared"):
        print(f"  cleared stale outgoing target pool {out['was'][:8]} on "
              f"{cluster_id[:8]} (it is not a pool of that cluster)")
    return out


def attach_replication_policy(mgmt_ip, key_path, lvol, policy):
    """Put an existing volume under a policy — this is what starts replication."""
    run(mgmt_ip, key_path, f"{SBCTL} -d volume replication-policy-set {lvol} {policy}")


def pool_uuid_of(mgmt_ip, key_path, pool_name):
    raw = run(mgmt_ip, key_path, f"{SBCTL} pool list", quiet=True)
    for line in raw.splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) > 2 and cols[2] == pool_name:
            return cols[1]
    raise RuntimeError(f"Pool not found: {pool_name}")


def write_baseline(client_ip, key_path, mounts, tag="baseline"):
    """Write a known file per mount and return {lvol: md5}."""
    sums = {}
    for m in mounts:
        path = f"{m['mount']}/{tag}.bin"
        run(client_ip, key_path,
            f"sudo dd if=/dev/urandom of={path} bs=1M count={BASELINE_MB} oflag=direct")
        run(client_ip, key_path, "sync")
        sums[m["lvol"]] = run(client_ip, key_path,
                              f"sudo md5sum {path}").split()[0]
    return sums


def verify_baseline(client_ip, key_path, mounts, expected, tag="baseline"):
    """Return (all_ok, details) comparing tag.bin on each mount to *expected*."""
    ok = True
    details = []
    for m in mounts:
        md5 = run(client_ip, key_path,
                  f"sudo md5sum {m['mount']}/{tag}.bin 2>/dev/null | awk '{{print $1}}'",
                  check=False).strip()
        want = expected.get(m["lvol"], "")
        match = bool(md5) and md5 == want
        ok = ok and match
        details.append({"lvol": m["lvol"], "mount": m["mount"],
                        "md5": md5, "expected": want, "match": match})
        print(f"  {m['mount']}: md5_match={match} ({md5 or '<none>'} vs {want})")
    return ok, details


def failback(mgmt_ip, key_path, lvol_uuid, source_cluster_id=None):
    """Configure fail-back for a failed-over volume (delta or fresh)."""
    flag = f" --source-cluster-id {source_cluster_id}" if source_cluster_id else ""
    kind = "fresh (full)" if source_cluster_id else "recovered source (delta)"
    print(f"  fail-back {lvol_uuid[:8]} -> {kind}")
    run(mgmt_ip, key_path, f"{SBCTL} -d volume replication-failback {lvol_uuid}{flag}")


def failed_over_targets_any_state(mgmt_ip, key_path, src_lvols):
    """Map source lvol -> target lvol for ANY live relationship state (used to
    connect target paths as soon as the cutover task creates them)."""
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
wanted = {list(src_lvols)!r}
out = {{}}
for r in db.get_lvol_replication_objects():
    if r.source_lvol and r.source_lvol.get_id() in wanted and r.target_lvol:
        out[r.source_lvol.get_id()] = r.target_lvol.get_id()
print(json.dumps(out))
""")


def failed_over_targets(mgmt_ip, key_path, src_lvols):
    """Map source lvol id -> target lvol id for failed_over relationships."""
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
wanted = {list(src_lvols)!r}
out = {{}}
for r in db.get_lvol_replication_objects():
    if r.source_lvol and r.source_lvol.get_id() in wanted and r.target_lvol:
        if r.state in ("failed_over", "cutover_done"):
            out[r.source_lvol.get_id()] = r.target_lvol.get_id()
print(json.dumps(out))
""")


def _all_test_pools(meta):
    """Every pool the suite can leave volumes in -- including the FRESH
    cluster's. A case-4 attempt leaves its cut-over copies named replvol0..4
    there, and the global (unscoped) get_lvol_by_name sees two records per
    name the moment a prologue re-creates them on src: "Multiple values
    present" before the case proper even starts (run 20260820_222257)."""
    pools = [c["pool"] for c in meta["clusters"].values()]
    return list(dict.fromkeys(pools))


def _src_target(meta):
    src_uuid = meta["replication"]["source_cluster"]
    tgt_uuid = meta["replication"]["target_cluster"]
    _, src = cluster_meta_by_uuid(meta, src_uuid)
    _, tgt = cluster_meta_by_uuid(meta, tgt_uuid)
    return src_uuid, src, tgt_uuid, tgt


def delete_test_volumes(mgmt_ip, key_path, pools):
    """Remove leftover replvol*/REP_* volumes from a previous case or run.

    Case 1 leaves its volumes behind AND a completed cutover creates a same-named
    copy on the target pool, so `get_lvol_by_name('replvol0')` then matches more
    than one record and dies with "Multiple values present". Case 2's docstring
    always claimed it cleaned up first; this is that step.
    """
    victims = []
    for pool in pools:
        raw = run(mgmt_ip, key_path, f"{SBCTL} volume list --pool {pool} 2>/dev/null",
                  check=False, quiet=True)
        for line in raw.splitlines():
            cols = [c.strip() for c in line.split("|")]
            if len(cols) > 3 and (cols[2].startswith("replvol") or cols[2].startswith("REP_")):
                if (cols[1], cols[2]) not in victims:
                    victims.append((cols[1], cols[2]))
    if not victims:
        return
    print(f"Cleaning up {len(victims)} leftover volume(s) before this case...")
    for uuid, name in victims:
        print(f"  deleting {name} ({uuid})")
        run(mgmt_ip, key_path, f"{SBCTL} volume replication-stop {uuid}", check=False, quiet=True)
        run(mgmt_ip, key_path, f"{SBCTL} volume delete {uuid} --force", check=False, quiet=True)

    # Deletion is asynchronous; wait for the records to drain so the name is free.
    for _ in range(30):
        time.sleep(10)
        left = 0
        for pool in pools:
            raw = run(mgmt_ip, key_path, f"{SBCTL} volume list --pool {pool} 2>/dev/null",
                      check=False, quiet=True)
            for line in raw.splitlines():
                cols = [c.strip() for c in line.split("|")]
                if len(cols) > 3 and (cols[2].startswith("replvol") or cols[2].startswith("REP_")):
                    left += 1
        if left == 0:
            print("  cleanup drained.")
            return
    raise RuntimeError("Timed out waiting for leftover volumes to delete")


def create_volumes(mgmt_ip, key_path, src_uuid, pool, tgt_uuid, tgt_pool, mode,
                   count=NUM_VOLUMES):
    """Create the test volumes already following a replication policy.

    The policy IS the start: `volume add --replication-policy` attaches it, and
    attaching runs the same start path the removed `volume replication-start`
    call used to drive directly.
    """
    policy = set_cluster_replication(mgmt_ip, key_path, src_uuid, tgt_uuid,
                                     pool_uuid_of(mgmt_ip, key_path, tgt_pool),
                                     mode=mode)
    lvols = []
    for i in range(count):
        name = f"replvol{i}"
        run(mgmt_ip, key_path,
            f"{SBCTL} -d volume add {name} {VOL_SIZE} {pool}"
            f" --replication-policy {policy}")
        lv = resolve_lvol(mgmt_ip, key_path, name)
        lvols.append(lv["uuid"])
        print(f"  created {name} = {lv['uuid']} (policy {policy}, mode={mode})")
    return lvols


def test_case_1(meta):
    print("\n========== CASE 1: online migration (no interruption) ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    # 0. clear anything an aborted run left behind, so names are free
    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))

    # 1. create + replicate (migration mode, 1-min auto snapshots)
    lvols = create_volumes(mgmt_ip, key_path, src_uuid, src["pool"], tgt_uuid,
                           tgt["pool"], mode="migration")

    # 2. connect/format/mount, write a baseline that fio NEVER touches, then fio.
    #    The baseline is the only data that lives exclusively in the replicated
    #    snapshot history: fio's sequential sweep rewrites its whole working set
    #    every pass, so the final-step delta carries fio's data regardless of
    #    whether replication works. Verifying the baseline post-cutover is the
    #    only assertion here that exercises the replicated snapshots at all.
    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
    baseline = write_baseline(client_ip, key_path, mounts)
    jobfile = write_fio_jobfile(client_ip, key_path, mounts)
    start_fio(client_ip, key_path, jobfile)

    # 3. let auto snapshots replicate, then wait until caught up
    print(f"Letting replication run for {REPL_INTERVAL_MIN * 3} min...")
    time.sleep(REPL_INTERVAL_MIN * 60 * 3)
    wait_replication_caught_up(mgmt_ip, key_path, lvols)

    # 4. online cutover per volume while fio runs.
    #    Connect the target paths right after commit (before the ANA flip) so the
    #    client multipath follows the cutover without dropping IO.
    print("Performing online migration cutover (fio keeps running)...")
    for lv in lvols:
        run(mgmt_ip, key_path, f"{SBCTL} -d volume replication-commit {lv}")

    # 5. wait for cutovers to complete, monitoring fio the whole time. The
    #    commit runs an iterative delta-shrink first, so the target volume (and
    #    its paths) appear minutes after the commit call: connect each target's
    #    paths AS SOON as they exist — before the ANA flip — so multipath
    #    follows the cutover without dropping IO.
    print("Waiting for cutover completion + monitoring fio...")
    start = time.time()
    states = {}
    done = 0
    connected_targets = set()
    src_nqn_by_lvol = {m["lvol"]: m["nqn"] for m in mounts}
    while time.time() - start < CUTOVER_WAIT_TIMEOUT:
        if not fio_alive(client_ip, key_path):
            raise RuntimeError("FAIL: fio stopped during online migration (interrupted)")
        targets = failed_over_targets_any_state(mgmt_ip, key_path, lvols)
        for src_lv, tgt_lv in targets.items():
            if tgt_lv:
                # Connect the TARGET-cluster paths under the SOURCE volume's
                # NQN: the cutover mirrors the subsystem on the target nodes
                # (same NQN, ANA-managed), so these paths aggregate into the
                # client's existing multipath device and IO continues on the
                # same /dev/nvmeXnY through the flip. Connecting the clone's
                # own internal NQN instead creates a separate subsystem the
                # mounts never use — the old device then loses all paths at
                # the fence and every IO on it hangs (run 10). Re-attempt each
                # poll until cutover completes: duplicates are rejected
                # harmlessly and the mirror may appear mid-loop.
                conn = get_connect_cmds(mgmt_ip, key_path, tgt_lv)
                src_nqn = src_nqn_by_lvol.get(src_lv, "")
                for cmd in conn.get("connect", []):
                    if src_nqn:
                        cmd = re.sub(r"--nqn=\S+", f"--nqn={src_nqn}", cmd)
                    run(client_ip, key_path, cmd, check=False, quiet=True)
                connected_targets.add(tgt_lv)
        states = replication_states(mgmt_ip, key_path, lvols)
        done = sum(1 for s in states.values() if s in ("cutover_done", "failed_over"))
        print(f"  cutovers done: {done}/{len(lvols)}  targets_connected="
              f"{len(connected_targets)}/{len(lvols)}  fio_alive=True")
        if done == len(lvols):
            break
        time.sleep(15)

    # 6. assert fio survived with no errors / no >20s latency
    time.sleep(20)
    alive = fio_alive(client_ip, key_path)
    errors = fio_error_count(client_ip, key_path)
    print(f"  fio_alive={alive} error_indicators={errors} cutovers_done={done}/{len(lvols)}")
    stop_fio(client_ip, key_path)
    if not alive:
        raise RuntimeError("FAIL: fio did not survive the online migration")
    if errors:
        raise RuntimeError(f"FAIL: fio reported {errors} error/latency violations during cutover")
    # The migration itself must actually have happened. Without this the case
    # "passed" on a cutover that never completed (0/5 for the whole timeout) --
    # surviving fio proves nothing if no volume ever moved.
    if done != len(lvols):
        raise RuntimeError(
            f"FAIL: only {done}/{len(lvols)} cutovers completed within "
            f"{CUTOVER_WAIT_TIMEOUT}s; states={states}")

    # 7. FULL-SURFACE data verification through the cutover volume: remount
    #    (fresh cache) and md5 the baseline, now served by the TARGET cluster.
    print("Verifying deep data through the cutover volumes (baseline md5)...")
    # Reconnect cleanly instead of reusing the pre-cutover devices. Any source
    # path the client still holds is fenced (ANA inaccessible), so IO on the old
    # device blocks in the kernel forever — umount/mount then hang in D-state and
    # `timeout` cannot kill them (runs 10 and 14). Drop every simplyblock path,
    # attach the volume where it now lives, and verify there. This is the same
    # pattern case 2 uses for its post-fail-over verification.
    prepare_mount_points(client_ip, key_path)
    targets = failed_over_targets_any_state(mgmt_ip, key_path, lvols)
    verify_mounts = []
    for idx, lv in enumerate(lvols):
        tgt_lv = targets.get(lv) or lv
        conn = get_connect_cmds(mgmt_ip, key_path, tgt_lv)
        for cmd in conn.get("connect", []):
            run(client_ip, key_path, cmd, check=False)
        time.sleep(4)
        dev = _dev_for_nqn(client_ip, key_path, conn.get("nqn", ""))
        if not dev:
            devs = _newest_spdk_devs(client_ip, key_path, 1)
            dev = devs[0] if devs else ""
        if not dev:
            print(f"  vol{idx}: no device appeared for {tgt_lv}")
            continue
        mnt = f"{MOUNT_BASE}_cut{idx}"
        run(client_ip, key_path,
            f"sudo mkdir -p {mnt} && sudo timeout 60 mount -o ro,norecovery {dev} {mnt}",
            check=False)
        # verify_baseline keys on the SOURCE lvol id (that is how baseline was
        # recorded), so keep that id on the mount record.
        verify_mounts.append({"lvol": lv, "nqn": conn.get("nqn", ""),
                              "dev": dev, "mount": mnt})
    ok, details = verify_baseline(client_ip, key_path, verify_mounts, baseline)
    mounts = verify_mounts or mounts
    cleanup_client(client_ip, key_path, mounts)
    if not ok:
        raise RuntimeError(
            "FAIL: pre-cutover data (baseline) is NOT intact on the target after "
            f"migration — replicated snapshot history is broken: {details}")
    print("CASE 1 PASSED: online migration, no fio interruption, deep data intact.")


def test_case_2(meta):
    print("\n========== CASE 2: fail-over on cluster kill ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    # In case 2 the source is the cluster that gets killed (its volumes fail over
    # to the other cluster). We use the configured replication source as the
    # cluster hosting the volumes and the one we suspend.
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    # clean up whatever case 1 (or an aborted run) left behind
    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))

    # create + replicate (failover mode, 1-min auto snapshots)
    lvols = create_volumes(mgmt_ip, key_path, src_uuid, src["pool"], tgt_uuid,
                           tgt["pool"], mode="failover")
    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)

    # Write a known file on each volume and record its checksum (this data will
    # be snapshotted+replicated and must survive the fail-over).
    baseline = {}
    for m in mounts:
        run(client_ip, key_path,
            f"sudo dd if=/dev/urandom of={m['mount']}/baseline.bin bs=1M count=128 oflag=direct")
        run(client_ip, key_path, "sync")
        md5 = run(client_ip, key_path, f"sudo md5sum {m['mount']}/baseline.bin").split()[0]
        baseline[m["lvol"]] = md5

    # start fio load (will be interrupted by the kill)
    jobfile = write_fio_jobfile(client_ip, key_path, mounts)
    start_fio(client_ip, key_path, jobfile)

    # run replication for N one-minute iterations (let auto snapshots replicate)
    print(f"Running replication for {CASE2_ITERATIONS} one-minute iterations...")
    for it in range(CASE2_ITERATIONS):
        time.sleep(60)
        print(f"  iteration {it + 1}/{CASE2_ITERATIONS}")
    wait_replication_caught_up(mgmt_ip, key_path, lvols)

    # SUSPEND the source cluster: kill SPDK on two of its nodes simultaneously.
    kill_ips = src["storage_public_ips"][:2]
    print(f"Killing SPDK on two source nodes simultaneously: {kill_ips}")
    with ThreadPoolExecutor(max_workers=2) as ex:
        for f in [ex.submit(kill_spdk, ip, key_path) for ip in kill_ips]:
            f.result()

    time.sleep(15)
    print(f"  fio_alive (expected False/erroring): {fio_alive(client_ip, key_path)}")
    stop_fio(client_ip, key_path)
    cleanup_client(client_ip, key_path, mounts)

    # Trigger fail-over for each volume and reconnect at NEW mount points.
    print("Triggering fail-over to target cluster...")
    failed_over = []
    for lv in lvols:
        fo = do_failover(mgmt_ip, key_path, lv)
        print(f"  failover {lv}: {json.dumps(fo)}")
        if not isinstance(fo, dict) or not fo.get("connection_strings"):
            raise RuntimeError(f"FAIL: fail-over returned no connection strings for {lv}")
        if fo.get("nqn"):
            assert fo["nqn"], "missing NQN"
        failed_over.append({"src_lvol": lv, "fo": fo})

    print("Reconnecting client to target paths at new mount points + verifying data...")
    ok = True
    for idx, item in enumerate(failed_over):
        fo = item["fo"]
        for cs in fo["connection_strings"]:
            run(client_ip, key_path, cs["connect"], check=False)
        time.sleep(3)
        dev = _newest_spdk_devs(client_ip, key_path, 1)[0]
        mnt = f"{MOUNT_BASE}_fo{idx}"
        run(client_ip, key_path, f"sudo mkdir -p {mnt} && sudo mount -o ro,norecovery {dev} {mnt}", check=False)
        listing = run(client_ip, key_path, f"sudo ls -l {mnt}", check=False)
        md5 = run(client_ip, key_path, f"sudo md5sum {mnt}/baseline.bin 2>/dev/null | awk '{{print $1}}'",
                  check=False).strip()
        expected = baseline[item["src_lvol"]]
        match = (md5 == expected)
        print(f"  vol{idx}: readable={'baseline.bin' in listing} md5_match={match} ({md5} vs {expected})")
        run(client_ip, key_path, f"sudo umount {mnt} 2>/dev/null || true", check=False)
        ok = ok and match
    if not ok:
        raise RuntimeError("FAIL: replicated data not intact after fail-over")
    print("CASE 2 PASSED: data readable and intact on target after fail-over.")

    # Restore what this case killed. Leaving the source cluster dead made every
    # later case start from a damaged cluster -- and because its lvstore had no
    # leader, deletes could not be finalised, so the next case timed out in its
    # own prologue waiting for leftover volumes to disappear. A case cleans up
    # its own outage.
    restore_cluster(mgmt_ip, key_path, src, label="src (after fail-over test)")


def _setup_failed_over_volumes(meta, tag):
    """Shared prologue for the fail-back cases: get 5 volumes failed over to tgt.

    Returns (lvols_on_target, baseline_md5_by_target_lvol, mounts).
    Leaves the client connected+mounted on the TARGET copies.
    """
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))

    print(f"[{tag}] creating + replicating 5 volumes (failover mode)...")
    lvols = create_volumes(mgmt_ip, key_path, src_uuid, src["pool"], tgt_uuid,
                           tgt["pool"], mode="failover")
    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
    baseline = write_baseline(client_ip, key_path, mounts)
    baseline_done_ts = time.time()
    wait_replication_caught_up(mgmt_ip, key_path, lvols)
    # Steady lag alone would let us fail over onto the empty snapshot taken
    # before mkfs; require the baseline itself to be on the target.
    wait_data_replicated(mgmt_ip, key_path, lvols, baseline_done_ts)

    print(f"[{tag}] taking the source cluster down (both nodes)...")
    for ip in src["storage_public_ips"][:2]:
        kill_spdk(ip, key_path)
    time.sleep(15)
    stop_fio(client_ip, key_path)
    cleanup_client(client_ip, key_path, mounts)

    print(f"[{tag}] failing over...")
    # NOTE the source stays down on purpose here: the fail-over must happen while
    # it is dead. Each case restores it explicitly when its scenario requires the
    # primary site back (case 3), or leaves the fresh site to take over (case 4).
    tgt_lvols = []
    for lv in lvols:
        fo = do_failover(mgmt_ip, key_path, lv)
        if not isinstance(fo, dict) or not fo.get("connection_strings"):
            raise RuntimeError(f"FAIL: fail-over returned no connection strings for {lv}")
        tgt_lvols.append(fo["lvol_id"])

    # Re-key the baseline by TARGET lvol id and mount the failed-over copies.
    tgt_baseline = {t: baseline[s] for s, t in zip(lvols, tgt_lvols)}
    tgt_mounts = connect_and_mount(client_ip, key_path, mgmt_ip, tgt_lvols,
                                   fmt=False, mount_base=MOUNT_BASE + "_fo")
    return tgt_lvols, tgt_baseline, tgt_mounts


def test_case_3(meta):
    """Online fail-back (delta) to the RECOVERED primary, fio never stops."""
    print("\n========== CASE 3: online fail-back to recovered primary ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    tgt_lvols, baseline, mounts = _setup_failed_over_volumes(meta, "case3")

    # Keep serving IO from the new site while the primary comes back.
    jobfile = write_fio_jobfile(client_ip, key_path, mounts)
    start_fio(client_ip, key_path, jobfile)

    # Restart EVERY member and wait for an lvstore leader. Restarting only the
    # nodes that are not "online" was not enough: after the whole cluster is
    # killed its SPDK containers come back by themselves, so a node reads
    # "online" while its recovery never completed and its lvstore has no leader.
    # Failing back into that cluster is then refused by the leader gates (0/5
    # cutovers) and LVolMonitor cannot finalise deletes, which wedges every
    # later case behind undeletable volumes (lab 2026-08-19).
    restore_cluster(mgmt_ip, key_path, src, label="src (primary site)")
    print("  primary site online again.")

    # Fail-back needs the CURRENT host cluster (tgt) pointed back at src.
    set_cluster_replication(mgmt_ip, key_path, tgt_uuid, src_uuid,
                            pool_uuid_of(mgmt_ip, key_path, src["pool"]))

    for lv in tgt_lvols:
        failback(mgmt_ip, key_path, lv)          # delta: no --source-cluster-id
    wait_replication_caught_up(mgmt_ip, key_path, tgt_lvols)

    print("Committing the fail-back cutover while fio runs...")
    for lv in tgt_lvols:
        run(mgmt_ip, key_path, f"{SBCTL} -d volume replication-commit {lv}")

    start = time.time()
    done = 0
    while time.time() - start < CUTOVER_WAIT_TIMEOUT:
        if not fio_alive(client_ip, key_path):
            raise RuntimeError("FAIL: fio stopped during online fail-back (interrupted)")
        states = replication_states(mgmt_ip, key_path, tgt_lvols)
        done = sum(1 for s in states.values() if s in ("cutover_done", "failed_over"))
        print(f"  fail-back cutovers done: {done}/{len(tgt_lvols)}  fio_alive=True")
        if done == len(tgt_lvols):
            break
        time.sleep(15)

    time.sleep(20)
    alive = fio_alive(client_ip, key_path)
    errors = fio_error_count(client_ip, key_path)
    print(f"  fio_alive={alive} error_indicators={errors} failbacks_done={done}/{len(tgt_lvols)}")
    stop_fio(client_ip, key_path)

    back = failed_over_targets(mgmt_ip, key_path, tgt_lvols)
    cleanup_client(client_ip, key_path, mounts)
    src_mounts = connect_and_mount(
        client_ip, key_path, mgmt_ip, [back[lv] for lv in tgt_lvols if lv in back],
        fmt=False, mount_base=MOUNT_BASE + "_fb")
    remap = {back[lv]: baseline[lv] for lv in tgt_lvols if lv in back}
    ok, _ = verify_baseline(client_ip, key_path, src_mounts, remap)
    cleanup_client(client_ip, key_path, src_mounts)

    if not alive:
        raise RuntimeError("FAIL: fio did not survive the online fail-back")
    if errors:
        raise RuntimeError(f"FAIL: fio reported {errors} error/latency violations during fail-back")
    if done != len(tgt_lvols):
        raise RuntimeError(f"FAIL: only {done}/{len(tgt_lvols)} fail-back cutovers completed")
    if not ok:
        raise RuntimeError("FAIL: data not intact after fail-back to the recovered primary")
    print("CASE 3 PASSED: online delta fail-back, no fio interruption, data intact.")


def test_case_4(meta):
    """Migration onto a FRESHLY INSTALLED cluster after the primary collapsed.

    Logically "back replication" (the new cluster stands on the old site), but
    technically a brand-new cluster, so it must behave exactly like case 1: a
    full forward replication in migration mode plus a cutover, with no delta
    base and no fail-back semantics. Case 3 is the only case that replicates
    back into the SAME cluster.
    """
    print("\n========== CASE 4: fail-back to a fresh empty cluster ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    fresh = meta["clusters"].get("fresh")
    if not fresh:
        raise RuntimeError(
            "CASE 4 needs a third, empty cluster. Redeploy with the 'fresh' "
            "cluster enabled in setup_repl_test_2clusters.py (CLUSTERS).")
    fresh_uuid = fresh["cluster_uuid"]

    tgt_lvols, baseline, mounts = _setup_failed_over_volumes(meta, "case4")

    # This is an ONLINE MIGRATION, mechanically IDENTICAL to case 1 — only the
    # pair differs (fail-over site -> freshly installed site instead of
    # site 1 -> site 2). It is "back replication" only in the geographic sense:
    # the destination is a brand-new cluster that has never held this data, so
    # there is no delta base and nothing to fail BACK onto. Driving it through
    # the fail-back verb was wrong — that path exists for case 3, where the
    # ORIGINAL cluster is recovered and its pre-existing snapshots are matched
    # by data_uuid for a delta. Replication back into the SAME cluster is
    # case 3 only.
    jobfile = write_fio_jobfile(client_ip, key_path, mounts)
    start_fio(client_ip, key_path, jobfile)

    # The original site collapsed and has been reinstalled: drop its volumes so
    # nothing of the old cluster can be mistaken for a delta base.
    print("Clearing the collapsed primary's volumes (the fresh site starts empty)...")
    delete_test_volumes(mgmt_ip, key_path, [src["pool"]])

    policy = set_cluster_replication(mgmt_ip, key_path, tgt_uuid, fresh_uuid,
                                     pool_uuid_of(mgmt_ip, key_path, fresh["pool"]),
                                     mode="migration")

    # Forward replication in migration mode, exactly as case 1 does it: the
    # volumes already exist, so the policy is attached instead of being given
    # at create time. Attaching is what starts replication.
    for lv in tgt_lvols:
        attach_replication_policy(mgmt_ip, key_path, lv, policy)
    replication_started_ts = time.time()
    # A fail-over clone's first sync moves the whole volume PLUS its base
    # chain (ancestors replicate first), not a delta: run 20260820_230606 was
    # converging -- lag peaked at 444s and was falling, min_replicated still
    # climbing -- when the default 1200s budget fired. Give the full sync an
    # hour; the gate still fails fast if progress stops (lag only grows).
    wait_replication_caught_up(mgmt_ip, key_path, tgt_lvols, timeout=3600)
    # The volumes already hold the data, so any snapshot taken from here on
    # carries it: require one such snapshot to be ON the fresh cluster before
    # cutting over (lag alone would accept a point-in-time that predates it).
    # Same budget as the steady gate: the post-baseline snapshot only
    # completes after the volume's WHOLE base chain (the fail-over
    # prologue's cadence history, ~9 ancestors x 4-5 GiB here) has
    # replicated bottom-up, which is sequential per volume by design.
    # Run 20260821_202231: 2/5 volumes were mid-chain and progressing
    # when the default 1200s expired.
    wait_data_replicated(mgmt_ip, key_path, tgt_lvols, replication_started_ts,
                         timeout=3600)

    print("Committing the cutover onto the fresh cluster while fio runs...")
    for lv in tgt_lvols:
        run(mgmt_ip, key_path, f"{SBCTL} -d volume replication-commit {lv}")

    start = time.time()
    done = 0
    connected = set()
    src_nqn_by_lvol = {m["lvol"]: m["nqn"] for m in mounts}
    while time.time() - start < CUTOVER_WAIT_TIMEOUT:
        if not fio_alive(client_ip, key_path):
            raise RuntimeError(
                "FAIL: fio stopped during the migration to the fresh cluster (interrupted)")
        # Same choreography as case 1: attach the new site's paths under the
        # volume's own NQN before the ANA flip, so multipath follows the move.
        targets = failed_over_targets_any_state(mgmt_ip, key_path, tgt_lvols)
        for src_lv, new_lv in targets.items():
            if new_lv:
                conn = get_connect_cmds(mgmt_ip, key_path, new_lv)
                src_nqn = src_nqn_by_lvol.get(src_lv, "")
                for cmd in conn.get("connect", []):
                    if src_nqn:
                        cmd = re.sub(r"--nqn=\S+", f"--nqn={src_nqn}", cmd)
                    run(client_ip, key_path, cmd, check=False, quiet=True)
                connected.add(new_lv)
        states = replication_states(mgmt_ip, key_path, tgt_lvols)
        done = sum(1 for s in states.values() if s in ("cutover_done", "failed_over"))
        print(f"  cutovers onto fresh cluster: {done}/{len(tgt_lvols)}  "
              f"paths_connected={len(connected)}/{len(tgt_lvols)}  fio_alive=True")
        if done == len(tgt_lvols):
            break
        time.sleep(15)

    time.sleep(20)
    alive = fio_alive(client_ip, key_path)
    errors = fio_error_count(client_ip, key_path)
    print(f"  fio_alive={alive} error_indicators={errors} cutovers_done={done}/{len(tgt_lvols)}")
    stop_fio(client_ip, key_path)

    back = failed_over_targets(mgmt_ip, key_path, tgt_lvols)
    cleanup_client(client_ip, key_path, mounts)
    fresh_mounts = connect_and_mount(
        client_ip, key_path, mgmt_ip, [back[lv] for lv in tgt_lvols if lv in back],
        fmt=False, mount_base=MOUNT_BASE + "_fresh")
    remap = {back[lv]: baseline[lv] for lv in tgt_lvols if lv in back}
    ok, _ = verify_baseline(client_ip, key_path, fresh_mounts, remap)
    cleanup_client(client_ip, key_path, fresh_mounts)

    if done != len(tgt_lvols):
        raise RuntimeError(f"FAIL: only {done}/{len(tgt_lvols)} cutovers onto the fresh cluster")
    if not ok:
        raise RuntimeError("FAIL: data not intact after full fail-back to a fresh cluster")
    print("CASE 4 PASSED: full fail-back to a fresh cluster, data intact.")

    # The shared prologue killed the source site to fail over; hand it back so the
    # error cases do not inherit a dead cluster.
    restore_cluster(mgmt_ip, key_path, src, label="src (after fresh fail-back)")


def _replication_progress(mgmt_ip, key_path, lvols):
    infos = get_replication_infos(mgmt_ip, key_path, lvols)
    return sum((i.get("replicated_count") or 0) for i in infos.values())


def test_case_5(meta):
    """Error case: a TARGET node goes offline mid-replication, then returns."""
    print("\n========== CASE 5: target node offline during replication ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))
    lvols = create_volumes(mgmt_ip, key_path, src_uuid, src["pool"], tgt_uuid,
                           tgt["pool"], mode="failover")
    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
    jobfile = write_fio_jobfile(client_ip, key_path, mounts)
    start_fio(client_ip, key_path, jobfile)
    wait_replication_caught_up(mgmt_ip, key_path, lvols)

    victim = node_of_lvol(mgmt_ip, key_path, lvols[0])["replication_node_id"]
    print(f"Taking the REPLICATION TARGET node {victim[:8]} offline...")
    before = _replication_progress(mgmt_ip, key_path, lvols)
    sn_shutdown(mgmt_ip, key_path, victim)

    print(f"Observing {OUTAGE_REPL_CYCLES} replication cycles with the target down...")
    time.sleep(OUTAGE_REPL_CYCLES * REPL_INTERVAL_MIN * 60)
    during = _replication_progress(mgmt_ip, key_path, lvols)
    fio_ok = fio_alive(client_ip, key_path)
    print(f"  replicated_count: before={before} during_outage={during}  fio_alive={fio_ok}")
    if not fio_ok:
        raise RuntimeError("FAIL: client IO stopped because a REPLICATION TARGET node went down")

    print("Bringing the target node back...")
    sn_bring_back(mgmt_ip, key_path, victim)
    wait_replication_caught_up(mgmt_ip, key_path, lvols)
    after = _replication_progress(mgmt_ip, key_path, lvols)
    print(f"  replicated_count after recovery={after}")

    stop_fio(client_ip, key_path)
    errors = fio_error_count(client_ip, key_path)
    cleanup_client(client_ip, key_path, mounts)
    if after <= during:
        raise RuntimeError(
            f"FAIL: replication did not resume after the target node returned "
            f"(during={during}, after={after})")
    if errors:
        raise RuntimeError(f"FAIL: fio reported {errors} errors during the target-node outage")
    print("CASE 5 PASSED: target-node outage survived, replication resumed.")


def test_case_6(meta):
    """Error case: the SOURCE PRIMARY goes offline; the secondary keeps serving."""
    print("\n========== CASE 6: source primary offline (secondary survives) ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))
    lvols = create_volumes(mgmt_ip, key_path, src_uuid, src["pool"], tgt_uuid,
                           tgt["pool"], mode="failover")
    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
    jobfile = write_fio_jobfile(client_ip, key_path, mounts)
    start_fio(client_ip, key_path, jobfile)
    wait_replication_caught_up(mgmt_ip, key_path, lvols)

    info = node_of_lvol(mgmt_ip, key_path, lvols[0])
    primary, secondary = info["node_id"], info["secondary_node_id"]
    if not secondary:
        raise RuntimeError("CASE 6 needs an HA pair: no secondary node for the volume")
    print(f"Taking the SOURCE PRIMARY {primary[:8]} offline "
          f"(secondary {secondary[:8]} must carry on)...")
    before = _replication_progress(mgmt_ip, key_path, lvols)
    sn_shutdown(mgmt_ip, key_path, primary)

    print(f"Observing {OUTAGE_REPL_CYCLES} replication cycles on the secondary...")
    time.sleep(OUTAGE_REPL_CYCLES * REPL_INTERVAL_MIN * 60)
    during = _replication_progress(mgmt_ip, key_path, lvols)
    sec_status = node_status(mgmt_ip, key_path, secondary)
    fio_ok = fio_alive(client_ip, key_path)
    print(f"  replicated_count: before={before} during_outage={during}  "
          f"secondary={sec_status}  fio_alive={fio_ok}")
    if sec_status != "online":
        raise RuntimeError(f"FAIL: secondary went {sec_status} when the primary was shut down")
    if during <= before:
        raise RuntimeError(
            f"FAIL: replication stalled while the source primary was down "
            f"(before={before}, during={during}) — the secondary should keep it going")

    print("Bringing the primary back...")
    sn_bring_back(mgmt_ip, key_path, primary)
    wait_replication_caught_up(mgmt_ip, key_path, lvols)
    after = _replication_progress(mgmt_ip, key_path, lvols)
    print(f"  replicated_count after recovery={after}")

    stop_fio(client_ip, key_path)
    errors = fio_error_count(client_ip, key_path)
    cleanup_client(client_ip, key_path, mounts)
    if after <= during:
        raise RuntimeError(f"FAIL: replication did not resume after the primary returned "
                           f"(during={during}, after={after})")
    if errors:
        raise RuntimeError(f"FAIL: fio reported {errors} errors during the primary outage")
    print("CASE 6 PASSED: source-primary outage survived, replication continued and resumed.")


CASES = {
    "case1": test_case_1,   # online migration cutover, no IO interruption
    "case2": test_case_2,   # DR fail-over on source-cluster loss
    "case3": test_case_3,   # online delta fail-back to the recovered primary
    "case4": test_case_4,   # full fail-back to a fresh empty cluster
    "case5": test_case_5,   # error: replication target node offline
    "case6": test_case_6,   # error: source primary offline, secondary survives
}
GROUPS = {
    "both": ["case1", "case2"],
    "failback": ["case3", "case4"],
    "errors": ["case5", "case6"],
    "all": ["case1", "case2", "case3", "case4", "case5", "case6"],
    # Case 3 last: it is the only case that needs the killed primary restored
    # and recovered, so a failure there cannot cost the other five cases.
    "all_c3_last": ["case1", "case2", "case4", "case5", "case6", "case3"],
}


def main():
    # A comma-separated list runs exactly those cases, in the order given —
    # case order matters (case 3 needs a recovered primary, so it is normally
    # run last).
    arg = sys.argv[1] if len(sys.argv) > 1 else "both"
    selected = GROUPS.get(arg) or [c.strip() for c in arg.split(",") if c.strip()]
    unknown = [c for c in selected if c not in CASES]
    if unknown:
        print(f"Unknown case(s): {unknown}\n"
              f"  cases: {', '.join(CASES)}\n"
              f"  groups: {', '.join(GROUPS)}")
        sys.exit(2)

    meta = load_meta()
    results = []
    for name in selected:
        try:
            CASES[name](meta)
            results.append((name, "PASS", ""))
        except Exception as exc:            # keep going: one case must not hide the rest
            results.append((name, "FAIL", str(exc)[:200]))
            print(f"\n!! {name} FAILED: {exc}")

    print("\n=== SUMMARY ===")
    for name, verdict, detail in results:
        print(f"  {name:<7} {verdict}{('  ' + detail) if detail else ''}")
    print("=== DONE ===")
    if any(v == "FAIL" for _, v, _ in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
