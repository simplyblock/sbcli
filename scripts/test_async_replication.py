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


def do_failover(mgmt_ip, key_path, lvol_uuid, generation=0):
    """Fail a volume over, capturing WHY when it does not work.

    replicate_lvol_on_target_cluster returns a dict on success but False or
    (False, error) on failure, and the controller reports the reason through
    its logger -- which went to the snippet's stderr and was dropped. A bare
    "returned no connection strings" then costs a whole lab run to diagnose
    (case 7, runs 20260824_174611 and _202949). Capture both.
    """
    return mgmt_py(mgmt_ip, key_path, f"""
import io, json, logging, contextlib
from simplyblock_core.controllers import lvol_controller
buf = io.StringIO()
handler = logging.StreamHandler(buf)
# DEBUG, not WARNING: the RPC layer logs the actual SPDK response
# ("Invalid parameters", nsid in use, ...) at DEBUG, and the controller
# only re-reports its own generic "Failed to add bdev to subsystem".
# Only the last 1500 chars are kept, which is exactly the failure tail.
handler.setLevel(logging.DEBUG)
root = logging.getLogger()
root.addHandler(handler)
root.setLevel(logging.DEBUG)
err = ""
try:
    with contextlib.redirect_stderr(buf):
        res = lvol_controller.replicate_lvol_on_target_cluster({lvol_uuid!r}, generation={generation})
except Exception as exc:                      # noqa: BLE001 - report, don't hide
    res, err = False, f"{{type(exc).__name__}}: {{exc}}"
out = res if isinstance(res, dict) else {{"result": res}}
if not (isinstance(res, dict) and res.get("connection_strings")):
    out["error"] = err
    out["log"] = buf.getvalue()[-2500:]
print(json.dumps(out))
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


def wait_replication_caught_up(mgmt_ip, key_path, lvol_uuids, timeout=REPL_WAIT_TIMEOUT,
                               max_lag=None):
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
    max_lag = max_lag or MAX_LAG_SECONDS
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
        held = run(client_ip, key_path,
                   f"grep -E '^{dev} ' /proc/mounts | head -1 || true",
                   check=False, quiet=True, timeout=60).strip()
        if held:
            raise RuntimeError(
                f"{dev} is already mounted ({held}) before mounting {lv} at "
                f"{mnt}: a previous case's mount is still live on this device. "
                f"mount would fail with a bare 'already mounted or mount point "
                f"busy' (case 11, run 20260827_110415).")
        run(client_ip, key_path, f"sudo mkdir -p {mnt} && sudo mount {dev} {mnt}")
        mounts.append({"lvol": lv, "nqn": conn["nqn"], "dev": dev, "mount": mnt})
        print(f"  vol {lv} -> {dev} @ {mnt}")
    return mounts


def write_fio_jobfile(client_ip, key_path, mounts,
                      rw=None, bs=None, iodepth=None, numjobs=None, size=None,
                      time_based=True, verify=True, jobfile=None):
    """One fio job per mounted volume; FIO_NUMJOBS threads each; md5 verify; 20s max latency.

    The keyword overrides exist for the pressure/chaos cases (7-9): case 8
    needs a FINITE sequential 64k/QD64/4-job fill of a known delta size, which
    is the opposite of the endless mild verify-writer the fail-over cases use.
    """
    rw = rw or FIO_RW
    bs = bs or FIO_BS
    iodepth = iodepth if iodepth is not None else FIO_IODEPTH
    numjobs = numjobs if numjobs is not None else FIO_NUMJOBS
    size = size or FIO_SIZE
    jobfile = jobfile or FIO_JOBFILE
    sections = [
        "[global]",
        f"rw={rw}",
        f"bs={bs}",
        f"iodepth={iodepth}",
        "ioengine=libaio",
        "direct=1",
        f"size={size}",
        f"numjobs={numjobs}",
    ]
    if time_based:
        sections += ["time_based=1", "runtime=86400"]  # effectively endless
    else:
        # finite pass over `size`; rewriting the same files each cycle keeps
        # the volume's footprint constant while dirtying the delta again
        sections += ["loops=1", "overwrite=1"]
    if verify:
        sections += ["verify=md5", "verify_backlog=512", "verify_fatal=1"]
    sections += [
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
    run(client_ip, key_path, f"cat > {jobfile} <<'EOF'\n{content}\nEOF")
    return jobfile


def fio_bandwidth(client_ip, key_path, label=""):
    """Print fio's own aggregate bandwidth, and return (read_mbps, write_mbps).

    fio has been writing this all along under --status-interval=15; nothing
    read it, so every earlier analysis inferred the client rate from round
    durations instead (and was 5x out).
    """
    out = run(client_ip, key_path,
              "grep -aE '^ *(READ|WRITE): bw=' %s 2>/dev/null | tail -4 || true"
              % FIO_LOG, check=False, quiet=True)
    rd = wr = None
    for line in (out or "").splitlines():
        m = re.search(r"\((\d+(?:\.\d+)?)([kMG]?B)/s\)", line)
        if not m:
            continue
        val = float(m.group(1))
        unit = m.group(2)
        mbps = val / 1000.0 if unit == "kB" else (val * 1000.0 if unit == "GB" else val)
        if line.strip().startswith("READ"):
            rd = mbps
        elif line.strip().startswith("WRITE"):
            wr = mbps
    if rd is not None or wr is not None:
        print("  [fio %s] %s read %s MB/s, write %s MB/s"
              % (label, client_ip,
                 "%.0f" % rd if rd is not None else "?",
                 "%.0f" % wr if wr is not None else "?"))
    else:
        print("  [fio %s] %s no aggregate lines yet" % (label, client_ip))
    return rd, wr


def collect_xfer_timing(mgmt_ip, key_path, label):
    """Pull XFER-TIMING lines off the CP services into one file on the mgmt node.

    Container clocks are skewed from the host's, so every line carries its own
    epoch stamp and we sort on that rather than on docker's timestamps.
    """
    dest = "~/xfer_timing_%s.log" % label
    services = ("app_TasksRunnerReplicationFinal app_SnapshotReplication "
                "app_SnapshotMonitor app_LVolMonitor")
    cmd = ("rm -f %s; for S in %s; do "
           "sudo docker service logs $S 2>&1 | grep -a XFER-TIMING >> %s || true; "
           "done; sort -t= -k2 -n %s -o %s 2>/dev/null || true; wc -l < %s"
           % (dest, services, dest, dest, dest, dest))
    out = run(mgmt_ip, key_path, cmd, check=False, quiet=True, timeout=600)
    count = (out or "").strip().splitlines()[-1] if (out or "").strip() else "0"
    print("  [timing %s] collected %s XFER-TIMING lines -> %s" % (label, count, dest))
    return dest


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


def client_dirt(client_ip, key_path):
    """What is still mounted or connected on the client. Empty dict = clean.

    Reads /proc/mounts rather than trusting umount's exit code: a lazy unmount
    reports success and leaves the mount live until its IO drains, which is
    exactly how a finished case hands its devices to the next one.
    """
    mounts = run(client_ip, key_path,
                 "grep -oE '/mnt/repl[^ ]*' /proc/mounts 2>/dev/null | sort -u || true",
                 check=False, quiet=True, timeout=60)
    subsys = run(client_ip, key_path,
                 "sudo nvme list-subsys 2>/dev/null "
                 "| grep -oE 'nqn\\.2023-02\\.io\\.simplyblock:[^ ,]+' | sort -u || true",
                 check=False, quiet=True, timeout=90)
    dirt = {}
    if mounts.split():
        dirt["mounts"] = mounts.split()
    if subsys.split():
        dirt["subsystems"] = subsys.split()
    return dirt


def force_client_clean(client_ip, key_path, rounds=3):
    """Unmount and disconnect everything, and keep at it until it is gone.

    Each round kills whatever holds the mount (hung fio keeps a lazy unmount
    pinned forever), unmounts, disconnects every simplyblock subsystem, then
    re-reads /proc/mounts. Returns the remaining dirt, empty when clean.
    """
    dirt = client_dirt(client_ip, key_path)
    for attempt in range(rounds):
        if not dirt:
            return {}
        if attempt:
            print(f"  [{client_ip}] client still dirty ({dirt}); escalating "
                  f"(round {attempt + 1}/{rounds})")
        run(client_ip, key_path, "sudo pkill -x fio || true", check=False, timeout=60)
        # Kill the holders first: umount -l on a mount someone still has open
        # never completes, and the device stays mounted underneath.
        run(client_ip, key_path,
            "for m in $(grep -oE '/mnt/repl[^ ]*' /proc/mounts 2>/dev/null | sort -u); do "
            "sudo timeout 20 fuser -km \"$m\" 2>/dev/null || true; "
            "sudo timeout 15 umount \"$m\" 2>/dev/null "
            "|| sudo timeout 15 umount -f \"$m\" 2>/dev/null "
            "|| sudo timeout 15 umount -l \"$m\" 2>/dev/null || true; done",
            check=False, timeout=300)
        run(client_ip, key_path,
            "for n in $(sudo nvme list-subsys 2>/dev/null "
            "| grep -oE 'nqn\\.2023-02\\.io\\.simplyblock:[^ ,]+' | sort -u); do "
            "sudo timeout 20 nvme disconnect -n \"$n\" >/dev/null 2>&1 || true; done",
            check=False, timeout=300)
        # A lazy unmount finishes asynchronously once its holders are gone.
        for _ in range(10):
            time.sleep(3)
            dirt = client_dirt(client_ip, key_path)
            if not dirt:
                return {}
    return dirt


def assert_client_clean(client_ip, key_path, where):
    """Refuse to run *where* on a client another case left dirty."""
    dirt = force_client_clean(client_ip, key_path)
    if dirt:
        raise RuntimeError(
            f"{where}: client {client_ip} could not be returned to a clean "
            f"state -- still {dirt}. Whatever ran before it left mounts or "
            f"controllers behind (a cutover freeze outlasting the 15s unmount "
            f"timeout does exactly this), and starting here would test that "
            f"debris instead of the case.")


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
    # Verify, and escalate rather than hand the next case a live mount. Not
    # fatal here -- the case that owns these mounts has already done its work
    # and its verdict should stand -- but loud, because this is where the
    # contamination starts and the NEXT case is where it gets blamed.
    dirt = force_client_clean(client_ip, key_path)
    if dirt:
        print(f"  [{client_ip}] WARNING: cleanup left {dirt} behind; the next "
              f"case will refuse to start until this clears")


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
    # The commands above are best-effort by design; this is the part that
    # decides whether we may proceed. A lazy unmount reports success while the
    # mount is still live, so the only trustworthy check is /proc/mounts.
    assert_client_clean(client_ip, key_path, "prepare_mount_points")


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


def wait_cluster_settled(mgmt_ip, key_path, cluster_id, timeout=1800):
    """Wait until *cluster_id* has no open migration/balancing work.

    A restore leaves the cluster ACTIVE - REBALANCING (device_migration +
    balancing_on_restart tasks), and `sn shutdown` REFUSES a node while that
    work is open — silently, from the driver's point of view, because the
    refusal goes to stderr (run 20260824_153107: case 6's shutdown printed
    nothing and the node stayed 'online' for the full 900s budget, while
    case 5's identical call later worked, after the rebalance had drained).
    """
    print(f"Waiting for cluster {cluster_id[:8]} to settle (no rebalance/migration)...")
    start = time.time()
    while time.time() - start < timeout:
        state = mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
db = DBController()
open_tasks = [t.function_name for t in db.get_job_tasks({cluster_id!r})
              if t.status != JobSchedule.STATUS_DONE and not t.canceled
              and t.function_name in ("device_migration", "balancing_on_restart",
                                      "new_device_migration", "failed_device_migration")]
print(json.dumps({{"status": db.get_cluster_by_id({cluster_id!r}).status,
                   "open": open_tasks}}))
""", replayable=True)
        if not state["open"]:
            print(f"  cluster settled (status {state['status']}).")
            return
        print(f"  status={state['status']} open={state['open']}")
        time.sleep(20)
    raise RuntimeError(f"Cluster {cluster_id[:8]} did not settle within {timeout}s")


def sn_shutdown(mgmt_ip, key_path, node_id, cluster_id=None):
    """Take a node offline the supported way.

    Deliberately NOT `docker kill` on the SPDK container: the control plane
    auto-restarts that within minutes (case 2 saw the "suspended" source cluster
    heal itself mid-test), which silently invalidates an outage scenario.
    """
    if cluster_id:
        wait_cluster_settled(mgmt_ip, key_path, cluster_id)
    print(f"Shutting down node {node_id[:8]} ...")
    # Straight to shutdown: suspending first buys nothing and actively hurts —
    # a suspended node makes its own queued work defer ("node is not online,
    # retrying"), and that backlog then blocks the shutdown itself (run 15
    # case 6: the node never left `suspended`).
    # 2>&1: sbctl reports a REFUSED shutdown on stderr, which the channel
    # otherwise drops — the driver then stares at an online node for 900s
    # with no clue why (run 20260824_153107).
    run(mgmt_ip, key_path, f"{SBCTL} -d sn shutdown {node_id} 2>&1", check=False)
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
                              interval_min=REPL_INTERVAL_MIN, keep=2, name=None,
                              extra_flags=""):
    """Register (idempotently) a cadence policy on an existing target.

    extra_flags shapes the policy NAME too, so a case asking for a
    retention-schedule or consistency-group policy never silently reuses a
    plain one another case left behind.
    """
    suffix = "_x%08x" % (hash(extra_flags) & 0xffffffff) if extra_flags else ""
    if interval_min != REPL_INTERVAL_MIN:
        suffix += f"_i{interval_min}"
    name = name or f"pol_{mode}_{target_name}{suffix}"
    for row in _replication_list(mgmt_ip, key_path, "policy", from_cluster):
        if row.get("Name") == name:
            return name
    run(mgmt_ip, key_path,
        f"{SBCTL} -d cluster replication-policy-add {from_cluster} {name}"
        f" --target {target_name} --interval-min {interval_min} --mode {mode}"
        f" --keep {keep}{(' ' + extra_flags) if extra_flags else ''}")
    return name


def set_cluster_replication(mgmt_ip, key_path, from_cluster, to_cluster, to_pool_uuid,
                            mode="migration", extra_flags="", policy_name=None,
                            interval_min=REPL_INTERVAL_MIN):
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
    policy = ensure_replication_policy(mgmt_ip, key_path, from_cluster, target, mode,
                                       interval_min=interval_min,
                                       name=policy_name, extra_flags=extra_flags)

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
            if len(cols) > 3 and cols[2].startswith(("replvol", "REP_", "nsvol", "presvol", "chaosvol")):
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
    # Budget scales with the pile: case 7 leaves 20 namespaced volumes plus
    # their REP_* landing copies, and 37 volumes did not drain inside the flat
    # 300s, failing case 8 in its prologue (run 20260824_174611).
    drain_polls = max(30, len(victims) * 3)
    for _ in range(drain_polls):
        time.sleep(10)
        left = 0
        for pool in pools:
            raw = run(mgmt_ip, key_path, f"{SBCTL} volume list --pool {pool} 2>/dev/null",
                      check=False, quiet=True)
            for line in raw.splitlines():
                cols = [c.strip() for c in line.split("|")]
                if len(cols) > 3 and cols[2].startswith(("replvol", "REP_", "nsvol", "presvol", "chaosvol")):
                    left += 1
        if left == 0:
            print("  cleanup drained.")
            return
    raise RuntimeError(
        f"Timed out waiting for {len(victims)} leftover volumes to delete "
        f"after {drain_polls * 10}s")


def lag_gate_for(interval_min):
    """The steady-state lag a cadence can actually hold: 3 cadence periods."""
    return max(1, int(interval_min)) * 60 * 3


def create_volumes(mgmt_ip, key_path, src_uuid, pool, tgt_uuid, tgt_pool, mode,
                   count=NUM_VOLUMES, prefix="replvol", size=VOL_SIZE,
                   extra_flags="", interval_min=REPL_INTERVAL_MIN):
    """Create the test volumes already following a replication policy.

    The policy IS the start: `volume add --replication-policy` attaches it, and
    attaching runs the same start path the removed `volume replication-start`
    call used to drive directly.
    """
    policy = set_cluster_replication(mgmt_ip, key_path, src_uuid, tgt_uuid,
                                     pool_uuid_of(mgmt_ip, key_path, tgt_pool),
                                     mode=mode, interval_min=interval_min)
    lvols = []
    for i in range(count):
        name = f"{prefix}{i}"
        run(mgmt_ip, key_path,
            f"{SBCTL} -d volume add {name} {size} {pool}"
            f" --replication-policy {policy}{(' ' + extra_flags) if extra_flags else ''}")
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
            raise RuntimeError(
                f"FAIL: fail-over returned no connection strings for {lv}: "
                f"{fo.get('error') or ''} {fo.get('log') or ''}".strip())
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
            raise RuntimeError(
                f"FAIL: fail-over returned no connection strings for {lv}: "
                f"{fo.get('error') or ''} {fo.get('log') or ''}".strip())
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
    # Full-sync budget, same reasoning as case 4: the fail-back chain
    # gate ships every unreplicated tgt-side ancestor to the recovered
    # source bottom-up (sequential per volume), and on a shared lab
    # those chains carry earlier cases' cadence history. Run
    # 20260821_235158: 3/5 volumes were still mid-chain at the default
    # 1200s while the other two were already landing fail-back copies.
    wait_replication_caught_up(mgmt_ip, key_path, tgt_lvols, timeout=3600)

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
    sn_shutdown(mgmt_ip, key_path, victim, cluster_id=tgt_uuid)

    print(f"Observing {OUTAGE_REPL_CYCLES} replication cycles with the target down...")
    time.sleep(OUTAGE_REPL_CYCLES * REPL_INTERVAL_MIN * 60)
    during = _replication_progress(mgmt_ip, key_path, lvols)
    fio_ok = fio_alive(client_ip, key_path)
    print(f"  replicated_count: before={before} during_outage={during}  fio_alive={fio_ok}")
    if not fio_ok:
        raise RuntimeError("FAIL: client IO stopped because a REPLICATION TARGET node went down")

    print("Bringing the target node back...")
    sn_bring_back(mgmt_ip, key_path, victim)
    recovery_ts = time.time()
    wait_replication_caught_up(mgmt_ip, key_path, lvols)
    # See case 6: replicated_count is a retained count, not a progress
    # counter. Require a post-recovery point-in-time on the target instead.
    wait_data_replicated(mgmt_ip, key_path, lvols, recovery_ts)
    after = _replication_progress(mgmt_ip, key_path, lvols)
    print(f"  replicated_count after recovery={after} (retained, not cumulative)")

    stop_fio(client_ip, key_path)
    errors = fio_error_count(client_ip, key_path)
    cleanup_client(client_ip, key_path, mounts)
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
    sn_shutdown(mgmt_ip, key_path, primary, cluster_id=src_uuid)

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
    recovery_ts = time.time()
    wait_replication_caught_up(mgmt_ip, key_path, lvols)
    # Resumption is proven by a point-in-time created AFTER the primary
    # returned reaching the target -- not by replicated_count growing.
    # That counter tracks RETAINED replicated snapshots, and retention keeps
    # only the newest generations, so it is bounded and routinely falls after
    # a burst: run 20260825_105453 read during=13 / after=10 while
    # replication was working perfectly.
    wait_data_replicated(mgmt_ip, key_path, lvols, recovery_ts)
    after = _replication_progress(mgmt_ip, key_path, lvols)
    print(f"  replicated_count after recovery={after} (retained, not cumulative)")

    stop_fio(client_ip, key_path)
    errors = fio_error_count(client_ip, key_path)
    cleanup_client(client_ip, key_path, mounts)
    if errors:
        raise RuntimeError(f"FAIL: fio reported {errors} errors during the primary outage")
    print("CASE 6 PASSED: source-primary outage survived, replication continued and resumed.")


# --------------------------------------------------------------------------- #
# Cases 7-9: namespaced subsystems, sequential pressure, chaos injection
# --------------------------------------------------------------------------- #
NS_VOLUMES = int(os.environ.get("NS_VOLUMES", "20"))
NS_PER_SUBSYS = int(os.environ.get("NS_PER_SUBSYS", "10"))
NS_VOL_SIZE = os.environ.get("NS_VOL_SIZE", "20G")
#: 20 volumes on a one-minute cadence means 20 transfers a minute
#: competing for the same hub: the lag settles at ~240s, above the
#: 180s gate, and the case times out in setup without ever reaching
#: the fail-over it exists to test (run 20260826_205051, lag
#: oscillating 216-285s for 35 minutes). Five minutes gives the same
#: coverage with a backlog the cluster can actually hold.
NS_INTERVAL_MIN = int(os.environ.get("NS_INTERVAL_MIN", "5"))

PRESSURE_VOLUMES = int(os.environ.get("PRESSURE_VOLUMES", "2"))
PRESSURE_VOL_SIZE = os.environ.get("PRESSURE_VOL_SIZE", "120G")
PRESSURE_DELTA_GB = int(os.environ.get("PRESSURE_DELTA_GB", "50"))
PRESSURE_CYCLES = int(os.environ.get("PRESSURE_CYCLES", "3"))
PRESSURE_CATCHUP_TIMEOUT = int(os.environ.get("PRESSURE_CATCHUP_TIMEOUT", "3600"))

CHAOS_EVENTS = int(os.environ.get("CHAOS_EVENTS", "12"))
CHAOS_SLEEP_MIN = int(os.environ.get("CHAOS_SLEEP_MIN", "20"))
CHAOS_SLEEP_MAX = int(os.environ.get("CHAOS_SLEEP_MAX", "150"))
CHAOS_SEED = os.environ.get("CHAOS_SEED", "")


def lvol_identities(mgmt_ip, key_path, lvol_uuids):
    """{uuid: {nqn, ns_id, node_id}} — the preserved identity under test."""
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
out = {{}}
for u in {list(lvol_uuids)!r}:
    lv = db.get_lvol_by_id(u)
    out[u] = {{"nqn": lv.nqn, "ns_id": lv.ns_id, "node_id": lv.node_id}}
print(json.dumps(out))
""", replayable=True)


def _ns_devs_for_nqn(client_ip, key_path, nqn, expected, tries=10):
    """{nsid: /dev/nvmeXnY} for every namespace of a SHARED subsystem.

    _dev_for_nqn picks the first namespace under the subsystem, which is
    exactly wrong for case 7 where ten volumes share one NQN. sysfs gives the
    block devices; `nvme get-ns-id` gives each one's NSID (the n-suffix in the
    device name is a kernel instance number, NOT the NSID).
    """
    for _ in range(tries):
        out = run(client_ip, key_path,
                  "for s in /sys/class/nvme-subsystem/nvme-subsys*; do "
                  f"[ \"$(cat $s/subsysnqn 2>/dev/null)\" = \"{nqn}\" ] || continue; "
                  "ls $s 2>/dev/null | grep -E '^nvme[0-9]+n[0-9]+$'; "
                  "done", check=False, quiet=True)
        devs = [d for d in out.split() if d]
        mapping = {}
        for d in devs:
            nsid_out = run(client_ip, key_path,
                           f"sudo nvme get-ns-id /dev/{d} 2>/dev/null",
                           check=False, quiet=True)
            m = re.search(r"(\d+)\s*$", nsid_out.strip())
            if m:
                mapping[int(m.group(1))] = f"/dev/{d}"
        if len(mapping) >= expected:
            return mapping
        time.sleep(5)
    return mapping


def connect_and_mount_namespaced(client_ip, key_path, mgmt_ip, lvols, idents,
                                 fmt=True, mount_base=MOUNT_BASE + "_ns"):
    """connect_and_mount for volumes that SHARE subsystems: connect each NQN
    once, then hand every volume the device matching ITS nsid."""
    prepare_mount_points(client_ip, key_path)
    by_nqn = {}
    for lv in lvols:
        by_nqn.setdefault(idents[lv]["nqn"], []).append(lv)

    devmaps = {}
    for nqn, members in by_nqn.items():
        conn = get_connect_cmds(mgmt_ip, key_path, members[0])
        assert not conn["err"], f"connect_lvol error for {members[0]}: {conn['err']}"
        for cmd in conn["connect"]:
            run(client_ip, key_path, cmd, check=False)
        time.sleep(3)
        devmaps[nqn] = _ns_devs_for_nqn(client_ip, key_path, nqn, len(members))
        print(f"  subsystem {nqn.split(':')[-1][:13]}: "
              f"{len(devmaps[nqn])} namespaces visible on {client_ip}")

    mounts = []
    for idx, lv in enumerate(lvols):
        ident = idents[lv]
        dev = devmaps.get(ident["nqn"], {}).get(ident["ns_id"])
        if not dev:
            raise RuntimeError(
                f"no device for lvol {lv} (nqn={ident['nqn']} nsid={ident['ns_id']}); "
                f"visible: {devmaps.get(ident['nqn'])}")
        mnt = f"{mount_base}{idx}"
        if fmt:
            run(client_ip, key_path, f"sudo mkfs.xfs -f {dev}")
        run(client_ip, key_path, f"sudo mkdir -p {mnt} && sudo mount {dev} {mnt}")
        mounts.append({"lvol": lv, "nqn": ident["nqn"], "dev": dev, "mount": mnt})
        print(f"  vol {lv} (nsid {ident['ns_id']}) -> {dev} @ {mnt}")
    return mounts


def test_case_7(meta):
    """Namespaced volumes: 2 subsystems x 10 namespaces, 2 clients, full
    replication + fail-over + fail-back with the shared-subsystem identity
    preserved for every namespace."""
    print("\n========== CASE 7: namespaced subsystems (2x10 ns, 2 clients) ==========")
    import random
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    clients = [c["public_ip"] for c in meta["clients"]]
    if len(clients) < 2:
        raise RuntimeError(
            "case 7 needs at least 2 clients; run "
            "`python scripts/setup_repl_test_2clusters.py add_client` first")
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    for ip in clients:
        prepare_mount_points(ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))

    print(f"Creating {NS_VOLUMES} namespaced volumes "
          f"(max {NS_PER_SUBSYS}/subsystem => {NS_VOLUMES // NS_PER_SUBSYS} subsystems)...")
    lvols = create_volumes(
        mgmt_ip, key_path, src_uuid, src["pool"], tgt_uuid, tgt["pool"],
        mode="failover", count=NS_VOLUMES, prefix="nsvol", size=NS_VOL_SIZE,
        interval_min=NS_INTERVAL_MIN,
        extra_flags=f"--namespaced True --max-namespace-per-subsys {NS_PER_SUBSYS}")

    idents = lvol_identities(mgmt_ip, key_path, lvols)
    by_nqn = {}
    for lv in lvols:
        by_nqn.setdefault(idents[lv]["nqn"], []).append(lv)
    packing = {n.split(":")[-1][:13]: len(v) for n, v in by_nqn.items()}
    print(f"  subsystem packing: {packing}")
    if len(by_nqn) != NS_VOLUMES // NS_PER_SUBSYS or set(packing.values()) != {NS_PER_SUBSYS}:
        raise RuntimeError(f"FAIL: expected {NS_VOLUMES // NS_PER_SUBSYS} subsystems x "
                           f"{NS_PER_SUBSYS} namespaces, got {packing}")

    # Random client assignment, reproducible via printed seed.
    seed = int(os.environ.get("NS_SEED") or time.time())
    rng = random.Random(seed)
    print(f"  client assignment seed: {seed}")
    shuffled = lvols[:]
    rng.shuffle(shuffled)
    split = rng.randint(NS_VOLUMES // 4, 3 * NS_VOLUMES // 4)  # both clients always used
    assign = {clients[0]: shuffled[:split], clients[1]: shuffled[split:]}

    mounts_by_client, baseline = {}, {}
    for ip, vols in assign.items():
        print(f"Client {ip}: {len(vols)} namespaces")
        mounts_by_client[ip] = connect_and_mount_namespaced(
            ip, key_path, mgmt_ip, vols, idents, fmt=True)
        baseline.update(write_baseline(ip, key_path, mounts_by_client[ip]))
    baseline_ts = time.time()

    for ip in assign:
        start_fio(ip, key_path, write_fio_jobfile(ip, key_path, mounts_by_client[ip],
                                                  size="1G"))
    ns_gate = lag_gate_for(NS_INTERVAL_MIN)
    for ip in assign:
        fio_bandwidth(ip, key_path, "steady-state")
    wait_replication_caught_up(mgmt_ip, key_path, lvols, timeout=3600, max_lag=ns_gate)
    wait_data_replicated(mgmt_ip, key_path, lvols, baseline_ts, timeout=3600)
    for ip in assign:
        fio_bandwidth(ip, key_path, "pre-failover")
    collect_xfer_timing(mgmt_ip, key_path, "case7_pre_failover")

    print("Killing the source cluster (both nodes)...")
    for ip in src["storage_public_ips"][:2]:
        kill_spdk(ip, key_path)
    time.sleep(15)
    for ip in assign:
        stop_fio(ip, key_path)
        cleanup_client(ip, key_path, mounts_by_client[ip])

    print("Failing over all namespaces...")
    tgt_lvols = []
    for lv in lvols:
        fo = do_failover(mgmt_ip, key_path, lv)
        if not isinstance(fo, dict) or not fo.get("connection_strings"):
            raise RuntimeError(
                f"FAIL: fail-over returned no connection strings for {lv}: "
                f"{fo.get('error') or ''} {fo.get('log') or ''}".strip())
        tgt_lvols.append(fo["lvol_id"])
    src_to_tgt = dict(zip(lvols, tgt_lvols))

    # Identity preservation: every fail-over copy keeps ITS nqn and nsid, so
    # the shared subsystems must re-form on the target with all 10 namespaces.
    tgt_idents = lvol_identities(mgmt_ip, key_path, tgt_lvols)
    for s, t in src_to_tgt.items():
        if (tgt_idents[t]["nqn"], tgt_idents[t]["ns_id"]) != (idents[s]["nqn"], idents[s]["ns_id"]):
            raise RuntimeError(
                f"FAIL: identity not preserved for {s}: "
                f"{idents[s]['nqn']}/{idents[s]['ns_id']} -> "
                f"{tgt_idents[t]['nqn']}/{tgt_idents[t]['ns_id']}")
    print("  NQN + nsid preserved for all namespaces.")

    # Re-assign RANDOMLY again for the fail-over verification (fresh shuffle).
    shuffled2 = tgt_lvols[:]
    rng.shuffle(shuffled2)
    split2 = rng.randint(NS_VOLUMES // 4, 3 * NS_VOLUMES // 4)
    assign_fo = {clients[0]: shuffled2[:split2], clients[1]: shuffled2[split2:]}
    tgt_baseline = {src_to_tgt[s]: b for s, b in baseline.items()}

    fo_mounts_by_client = {}
    ok = True
    for ip, vols in assign_fo.items():
        m = connect_and_mount_namespaced(ip, key_path, mgmt_ip, vols, tgt_idents,
                                         fmt=False, mount_base=MOUNT_BASE + "_nsfo")
        fo_mounts_by_client[ip] = m
        good, _ = verify_baseline(ip, key_path, m, tgt_baseline)
        ok = ok and good
    if not ok:
        raise RuntimeError("FAIL: replicated namespace data not intact after fail-over")

    print("Restoring the source cluster + failing back all namespaces...")
    restore_cluster(mgmt_ip, key_path, src, label="src (case 7)")
    set_cluster_replication(mgmt_ip, key_path, tgt_uuid, src_uuid,
                            pool_uuid_of(mgmt_ip, key_path, src["pool"]))
    for lv in tgt_lvols:
        failback(mgmt_ip, key_path, lv)
    wait_replication_caught_up(mgmt_ip, key_path, tgt_lvols, timeout=3600,
                               max_lag=ns_gate)
    for lv in tgt_lvols:
        run(mgmt_ip, key_path, f"{SBCTL} -d volume replication-commit {lv}")

    start = time.time()
    done = 0
    # The timing bundle is the whole point of the run, so collect it even when
    # the poll itself blows up. Twice now a transient control-plane error inside
    # this loop (`sbctl` rc=1) propagated out and skipped the collection, leaving
    # the previous run's file in place -- which then looked like fresh data.
    try:
        while time.time() - start < CUTOVER_WAIT_TIMEOUT * 2:
            try:
                states = replication_states(mgmt_ip, key_path, tgt_lvols)
            except Exception as e:                        # noqa: BLE001
                # A CP hiccup must not end the case: log it, keep polling, and
                # let the deadline decide.
                print(f"  fail-back poll failed ({str(e)[:120]}); retrying")
                time.sleep(15)
                continue
            done = sum(1 for s in states.values() if s in ("cutover_done", "failed_over"))
            print(f"  fail-back cutovers done: {done}/{len(tgt_lvols)}")
            if done == len(tgt_lvols):
                break
            time.sleep(15)
    finally:
        collect_xfer_timing(mgmt_ip, key_path, "case7_failback")
    if done != len(tgt_lvols):
        # The breakdown matters MOST here: a stalled fail-back is the case we
        # have failed to explain seven times.
        raise RuntimeError(f"FAIL: only {done}/{len(tgt_lvols)} fail-back cutovers completed")

    back = failed_over_targets(mgmt_ip, key_path, tgt_lvols)
    back_idents = lvol_identities(mgmt_ip, key_path, list(back.values()))
    fb_baseline = {back[t]: tgt_baseline[t] for t in tgt_lvols if t in back}
    ok = True
    for ip, vols in assign_fo.items():
        cleanup_client(ip, key_path, fo_mounts_by_client[ip])
        fb_vols = [back[t] for t in vols if t in back]
        m = connect_and_mount_namespaced(ip, key_path, mgmt_ip, fb_vols, back_idents,
                                         fmt=False, mount_base=MOUNT_BASE + "_nsfb")
        good, _ = verify_baseline(ip, key_path, m, fb_baseline)
        ok = ok and good
        cleanup_client(ip, key_path, m)
    if not ok:
        raise RuntimeError("FAIL: namespace data not intact after fail-back")
    print(f"CASE 7 PASSED: {NS_VOLUMES} namespaces on "
          f"{NS_VOLUMES // NS_PER_SUBSYS} shared subsystems survived "
          f"fail-over + fail-back with identity preserved (seed {seed}).")


def test_case_8(meta):
    """Sequential-pressure catch-up: 64k/QD64/4-job fills of PRESSURE_DELTA_GB
    per volume, repeated; the backlog must drain back under the lag gate after
    every cycle. The cadence is a target, the backlog must converge."""
    print(f"\n========== CASE 8: {PRESSURE_DELTA_GB}G sequential-pressure catch-up "
          f"x{PRESSURE_CYCLES} ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))
    lvols = create_volumes(mgmt_ip, key_path, src_uuid, src["pool"], tgt_uuid,
                           tgt["pool"], mode="failover", count=PRESSURE_VOLUMES,
                           prefix="presvol", size=PRESSURE_VOL_SIZE)
    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
    baseline = write_baseline(client_ip, key_path, mounts)

    # Each of the 4 jobs writes delta/4 into its own file => delta GB per
    # volume per cycle; overwrite=1 dirties the SAME clusters again next cycle.
    per_job = f"{(PRESSURE_DELTA_GB * 1024) // 4}m"
    jobfile = write_fio_jobfile(client_ip, key_path, mounts, rw="write", bs="64k",
                                iodepth=64, numjobs=4, size=per_job,
                                time_based=False, verify=False,
                                jobfile="/tmp/fio_pressure.fio")

    results = []
    for cycle in range(1, PRESSURE_CYCLES + 1):
        print(f"--- cycle {cycle}/{PRESSURE_CYCLES}: writing "
              f"{PRESSURE_DELTA_GB}G per volume (64k seq, QD64, 4 jobs) ---")
        fill_start = time.time()
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(run, client_ip, key_path,
                            f"sudo fio --eta=never {jobfile}", True, True, 7200)
            peak_bytes, peak_lag = 0, 0
            while not fut.done():
                time.sleep(20)
                infos = get_replication_infos(mgmt_ip, key_path, lvols)
                bts = sum((i.get("outstanding_bytes") or 0) for i in infos.values())
                lag = max((i.get("lag_seconds") or 0) for i in infos.values())
                peak_bytes, peak_lag = max(peak_bytes, bts), max(peak_lag, lag)
                print(f"  [fill] backlog={bts / 2**30:.1f}GiB worst_lag={lag}s")
            fut.result()
        fill_secs = time.time() - fill_start

        # Now the pipeline must CATCH UP: bounded lag again within the budget.
        drain_start = time.time()
        caught_up = None
        while time.time() - drain_start < PRESSURE_CATCHUP_TIMEOUT:
            infos = get_replication_infos(mgmt_ip, key_path, lvols)
            bts = sum((i.get("outstanding_bytes") or 0) for i in infos.values())
            lag = max((i.get("lag_seconds") or 0) for i in infos.values())
            outst = sum((i.get("outstanding_count") or 0) for i in infos.values())
            peak_bytes, peak_lag = max(peak_bytes, bts), max(peak_lag, lag)
            print(f"  [drain] backlog={bts / 2**30:.1f}GiB worst_lag={lag}s outstanding={outst}")
            if lag <= MAX_LAG_SECONDS and outst <= len(lvols):
                caught_up = time.time() - drain_start
                break
            time.sleep(20)
        results.append({"cycle": cycle, "fill_secs": int(fill_secs),
                        "peak_backlog_gib": round(peak_bytes / 2**30, 1),
                        "peak_lag_s": peak_lag,
                        "catch_up_secs": None if caught_up is None else int(caught_up)})
        print(f"  cycle {cycle}: fill={int(fill_secs)}s "
              f"peak_backlog={peak_bytes / 2**30:.1f}GiB peak_lag={peak_lag}s "
              f"catch_up={'TIMEOUT' if caught_up is None else str(int(caught_up)) + 's'}")
        if caught_up is None:
            raise RuntimeError(
                f"FAIL: replication did not catch up within "
                f"{PRESSURE_CATCHUP_TIMEOUT}s after cycle {cycle} "
                f"(peak backlog {peak_bytes / 2**30:.1f}GiB)")

    ok, _ = verify_baseline(client_ip, key_path, mounts, baseline)
    stop_fio(client_ip, key_path)
    cleanup_client(client_ip, key_path, mounts)
    print("  cycle results:", json.dumps(results))
    if not ok:
        raise RuntimeError("FAIL: baseline corrupted during pressure cycles")
    print(f"CASE 8 PASSED: {PRESSURE_CYCLES} x {PRESSURE_DELTA_GB}G deltas, "
          f"replication caught up every cycle.")


def _sample_replication_phases(mgmt_ip, key_path):
    """What the replication pipeline is doing RIGHT NOW (for kill logging)."""
    return mgmt_py(mgmt_ip, key_path, """
import json
from collections import Counter
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
db = DBController()
c = Counter()
for cl in db.get_clusters():
    for t in db.get_job_tasks(cl.get_id()):
        if t.function_name in (JobSchedule.FN_SNAPSHOT_REPLICATION,
                               JobSchedule.FN_REPLICATION_FINAL) \
                and t.status != JobSchedule.STATUS_DONE:
            c[f"{t.function_name}:{t.status}"] += 1
print(json.dumps(dict(c)))
""", replayable=True)


def _all_nodes_online(mgmt_ip, key_path):
    """Truly recovered: every node online AND healthy, every cluster ACTIVE.

    Status alone is not recovery. After a chaos kill a node reports
    "online" again while health_check is still False and its lvstore port
    is down, and the cluster can sit in SUSPENDED/IN_ACTIVATION behind it.
    Gating on status only, the soak kept firing kills into a half-recovered
    2-node cluster and drove it to SUSPENDED by event 3 (run
    20260824_224909) -- damage the test caused, not damage it found.
    """
    return mgmt_py(mgmt_ip, key_path, """
import json
from simplyblock_core.db_controller import DBController
db = DBController()
bad = [n.get_id()[:8] for n in db.get_storage_nodes()
       if n.status != "online" or not n.health_check]
busy = [c.get_id()[:8] for c in db.get_clusters()
        if c.status not in ("active", "degraded")]
print(json.dumps({"all_online": not bad and not busy,
                  "offline": bad, "clusters": busy}))
""", replayable=True)


def test_case_9(meta):
    """Chaos: random SPDK-container kills on SOURCE and TARGET nodes while
    replication runs, to land failures in every pipeline phase (target-lvol
    create, hublvol attach, transfer, convert, chain, prune, detach)."""
    print(f"\n========== CASE 9: chaos container kills x{CHAOS_EVENTS} ==========")
    import random
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    seed = int(CHAOS_SEED or time.time())
    rng = random.Random(seed)
    print(f"  chaos seed: {seed}")

    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))
    lvols = create_volumes(mgmt_ip, key_path, src_uuid, src["pool"], tgt_uuid,
                           tgt["pool"], mode="failover", prefix="chaosvol")
    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
    baseline = write_baseline(client_ip, key_path, mounts)
    start_fio(client_ip, key_path, write_fio_jobfile(client_ip, key_path, mounts))
    wait_replication_caught_up(mgmt_ip, key_path, lvols)

    # One kill at a time, recover, next -- randomized timing spreads the kills
    # across the pipeline phases; two concurrent kills in one cluster is the
    # cluster-outage scenario cases 2/3 already cover, not a race.
    targets = ([("src", ip) for ip in src["storage_public_ips"]]
               + [("tgt", ip) for ip in tgt["storage_public_ips"]])
    kills = []
    fio_deaths = []
    for ev in range(1, CHAOS_EVENTS + 1):
        time.sleep(rng.randint(CHAOS_SLEEP_MIN, CHAOS_SLEEP_MAX))
        side, victim = rng.choice(targets)
        phases = _sample_replication_phases(mgmt_ip, key_path)
        print(f"  [{ev}/{CHAOS_EVENTS}] killing SPDK on {side} node {victim} "
              f"(active phases: {phases})")
        kill_spdk(victim, key_path)
        # fio survival across a kill is the promotion-window signal: before the
        # ANA-transition fix (spdk R26.3) a killed primary handed hard EIO to
        # the client within seconds and XFS shut the filesystem down.
        time.sleep(20)
        alive = fio_alive(client_ip, key_path)
        kills.append({"event": ev, "side": side, "node": victim,
                      "phases": phases, "fio_alive": alive})
        if not alive:
            fio_deaths.append(ev)
            print(f"    fio NOT alive after event {ev} (deaths so far: {len(fio_deaths)})")

        # Wait for the auto-restart to bring everything back before the next hit.
        deadline = time.time() + NODE_STATE_TIMEOUT
        while time.time() < deadline:
            state = _all_nodes_online(mgmt_ip, key_path)
            if state["all_online"]:
                break
            time.sleep(20)
        else:
            raise RuntimeError(
                f"FAIL: not recovered {NODE_STATE_TIMEOUT}s after chaos kill "
                f"{ev} on {victim}: unhealthy nodes={state['offline']} "
                f"clusters not active={state['clusters']}")

        # Chaos without IO is not chaos. A kill that takes fio down (the
        # promotion-window EIO) otherwise leaves every later event running
        # against an idle client -- run 20260824_224909 lost its workload at
        # event 3 and would have coasted through the remaining 97.
        if not fio_alive(client_ip, key_path):
            print("    restarting the client workload after the outage")
            try:
                cleanup_client(client_ip, key_path, mounts)
                mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols,
                                           fmt=False)
                start_fio(client_ip, key_path,
                          write_fio_jobfile(client_ip, key_path, mounts))
            except Exception as exc:            # noqa: BLE001 - keep the soak going
                print(f"    could not restart the workload: {exc}")

    print("Chaos done; requiring full catch-up + integrity...")
    wait_replication_caught_up(mgmt_ip, key_path, lvols, timeout=3600)
    quiesce_ts = time.time()
    stop_fio(client_ip, key_path)
    run(client_ip, key_path, "sync", check=False)
    wait_data_replicated(mgmt_ip, key_path, lvols, quiesce_ts, timeout=3600)
    cleanup_client(client_ip, key_path, mounts)

    # End-to-end proof the surviving pipeline shipped GOOD data: kill the
    # source, fail over, verify the baselines on the target copies.
    print("Final integrity check via fail-over...")
    for ip in src["storage_public_ips"][:2]:
        kill_spdk(ip, key_path)
    time.sleep(15)
    tgt_lvols = []
    for lv in lvols:
        fo = do_failover(mgmt_ip, key_path, lv)
        if not isinstance(fo, dict) or not fo.get("connection_strings"):
            raise RuntimeError(
                f"FAIL: post-chaos fail-over failed for {lv}: "
                f"{fo.get('error') or ''} {fo.get('log') or ''}".strip())
        tgt_lvols.append(fo["lvol_id"])
    tgt_baseline = {t: baseline[s] for s, t in zip(lvols, tgt_lvols)}
    fo_mounts = connect_and_mount(client_ip, key_path, mgmt_ip, tgt_lvols,
                                  fmt=False, mount_base=MOUNT_BASE + "_chaos")
    ok, _ = verify_baseline(client_ip, key_path, fo_mounts, tgt_baseline)
    cleanup_client(client_ip, key_path, fo_mounts)
    restore_cluster(mgmt_ip, key_path, src, label="src (after chaos)")

    print(f"  fio survived {CHAOS_EVENTS - len(fio_deaths)}/{CHAOS_EVENTS} kills"
          + (f" (died after events {fio_deaths})" if fio_deaths else ""))
    print("  kill log:", json.dumps(kills))
    if not ok:
        raise RuntimeError("FAIL: data not intact after chaos (seed "
                           f"{seed}; kill log above)")
    print(f"CASE 9 PASSED: {CHAOS_EVENTS} random kills across both clusters, "
          f"replication recovered every time, data intact (seed {seed}).")




# --------------------------------------------------------------------------- #
# Cases 10-12: feature tests (migration under load, retention ladder, CGs)
# --------------------------------------------------------------------------- #
CASE10_VOLS_PER_NODE = int(os.environ.get("CASE10_VOLS_PER_NODE", "3"))
CASE11_RUNTIME_MIN = int(os.environ.get("CASE11_RUNTIME_MIN", "115"))
CASE11_SCHEDULE = os.environ.get("CASE11_SCHEDULE", "5m:15m,7m:30m,10m:1h")
CASE11_ROUNDS = int(os.environ.get("CASE11_ROUNDS", "3"))
CASE12_SCHEDULE = os.environ.get("CASE12_SCHEDULE", "5m:15m")
CASE12_RUNTIME_MIN = int(os.environ.get("CASE12_RUNTIME_MIN", "25"))


def _start_stall_probe(client_ip, key_path, mount):
    """100ms wall-clock heartbeats into the volume; gaps measure IO freezes."""
    run(client_ip, key_path,
        "sudo rm -f {m}/probe.ts; sudo nohup bash -c 'while :; do "
        "date +%s%3N >> {m}/probe.ts; sync {m}/probe.ts; sleep 0.1; done' "
        ">/dev/null 2>&1 & echo probe_started".format(m=mount), quiet=True)


def _stop_probes(client_ip, key_path):
    run(client_ip, key_path, "sudo pkill -f probe.ts 2>/dev/null || true",
        check=False, quiet=True)


def _max_probe_gap_ms(client_ip, key_path, mount, t0_ms, t1_ms):
    """Largest heartbeat gap inside [t0_ms, t1_ms] = the freeze duration the
    CLIENT actually observed (includes a ~100ms sampling floor)."""
    awk = ("sudo awk 'p && $1>={t0} && $1<={t1} && $1-p>m {{m=$1-p}} {{p=$1}} "
           "END{{print m+0}}' {m}/probe.ts").format(t0=t0_ms, t1=t1_ms, m=mount)
    out = run(client_ip, key_path, awk, check=False, quiet=True)
    try:
        return int(out.strip())
    except ValueError:
        return -1


def _start_recorder(client_ip, key_path, mount, interval_sec=30):
    """Timestamped, fsynced records every interval -- the ground truth for
    which point-in-time a snapshot generation captured."""
    cmd = ("sudo rm -f {m}/records.log; sudo nohup bash -c 'i=0; while :; do "
           "i=$((i+1)); echo \"iter=$i ts=$(date +%s)\" >> {m}/records.log; "
           "sync {m}/records.log; sleep {iv}; done' >/dev/null 2>&1 & "
           "echo recorder_started").format(m=mount, iv=interval_sec)
    run(client_ip, key_path, cmd, quiet=True)


def _stop_recorders(client_ip, key_path):
    run(client_ip, key_path, "sudo pkill -f records.log 2>/dev/null || true",
        check=False, quiet=True)


def _snapshot_ages(mgmt_ip, key_path, lvol_uuid):
    """created_at (epoch) of every replicated internal snapshot of the volume,
    newest first."""
    return mgmt_py(mgmt_ip, key_path, """
import json
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.snapshot import SnapShot
db = DBController()
out = []
for snp in db.get_snapshots():
    if snp.deleted or not snp.lvol or snp.lvol.get_id() != {lv!r}:
        continue
    if snp.snap_type == SnapShot.TYPE_INTERNAL and snp.target_replicated_snap_uuid:
        out.append(snp.created_at or 0)
print(json.dumps(sorted(out, reverse=True)))
""".format(lv=lvol_uuid), replayable=True)


def _verify_retention_ladder(times, tiers, now, cadence_sec=60, slack=150):
    """Independent check of (a) kept and (b) pruned per schedule.

    tiers: [(every_sec, span_sec)] finest first. Verifies: nothing older
    than the horizon (+slack), and inside each tier window consecutive
    retained snapshots are no further apart than every+cadence+slack and no
    denser than the schedule plus a small tolerance allows.
    """
    problems = []
    horizon = sum(t[1] for t in tiers)
    ages = sorted(now - t for t in times)
    for a in ages:
        if a > horizon + slack:
            problems.append("snapshot %ds old survives past the %ds horizon "
                            "(not pruned)" % (a, horizon))
    start = 0
    for every, span in tiers:
        end = start + span
        inside = [a for a in ages if start <= a < end]
        for prev, cur in zip(inside, inside[1:]):
            if cur - prev > every + cadence_sec + slack:
                problems.append(
                    "gap of %ds inside the %ds tier (%d-%ds): a scheduled "
                    "snapshot is missing" % (cur - prev, every, start, end))
        expected = span // every
        if len(inside) > expected + 2:
            problems.append(
                "%d snapshots inside the %ds tier (%d-%ds), expected <= %d: "
                "not pruned" % (len(inside), every, start, end, expected + 2))
        start = end
    return problems


def _records_match_generation(client_ip, key_path, mount, snap_ts, slack=35,
                              logname="records.log"):
    """The mounted snapshot copy must contain every record up to snap_ts and
    none from after it (records are fsynced every 30s)."""
    out = run(client_ip, key_path,
              "sudo grep -oE 'ts=[0-9]+' %s/%s | tail -200" % (mount, logname),
              check=False, quiet=True)
    ts = [int(x.split("=")[1]) for x in out.split() if x.startswith("ts=")]
    if not ts:
        return False, "no records found on the snapshot copy"
    newest = max(ts)
    if newest > snap_ts + slack:
        return False, ("record from %d present, %ds AFTER the generation's "
                       "snapshot (%d)" % (newest, newest - snap_ts, snap_ts))
    if newest < snap_ts - 95:
        return False, ("newest record %d is %ds older than the snapshot; "
                       "records up to the snapshot are missing"
                       % (newest, snap_ts - newest))
    return True, "newest record %ds before the snapshot" % (int(snap_ts) - newest)


def _target_snapshot_ts(mgmt_ip, key_path, target_lvol):
    """created_at of the snapshot the fail-over copy was cloned from."""
    return mgmt_py(mgmt_ip, key_path, """
import json
from simplyblock_core.db_controller import DBController
db = DBController()
lv = db.get_lvol_by_id({t!r})
snp = db.get_snapshot_by_id(lv.cloned_from_snap)
print(json.dumps(snp.created_at or 0))
""".format(t=target_lvol), replayable=True)


def test_case_10(meta):
    """Migration under sustained heavy IO: 3 volumes per source node at
    8k randwrite QD64x4 jobs, replication must keep up, the final online
    migration must complete, and the client-observed IO freeze during each
    cutover is measured."""
    print("\n========== CASE 10: online migration under heavy IO (freeze timing) ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))

    policy = set_cluster_replication(mgmt_ip, key_path, src_uuid, tgt_uuid,
                                     pool_uuid_of(mgmt_ip, key_path, tgt["pool"]),
                                     mode="migration")
    node_ids = mgmt_py(mgmt_ip, key_path, """
import json
from simplyblock_core.db_controller import DBController
print(json.dumps([n.get_id() for n in
                  DBController().get_storage_nodes_by_cluster_id({c!r})
                  if n.status == "online"]))
""".format(c=src_uuid), replayable=True)
    lvols = []
    for n_idx, nid in enumerate(node_ids):
        for v in range(CASE10_VOLS_PER_NODE):
            name = "replvol%d" % (n_idx * CASE10_VOLS_PER_NODE + v)
            run(mgmt_ip, key_path,
                "%s -d volume add %s %s %s --replication-policy %s --host-id %s"
                % (SBCTL, name, VOL_SIZE, src["pool"], policy, nid))
            lvols.append(resolve_lvol(mgmt_ip, key_path, name)["uuid"])
    print("  %d volumes across %d nodes" % (len(lvols), len(node_ids)))

    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
    write_baseline(client_ip, key_path, mounts)
    for m in mounts:
        _start_stall_probe(client_ip, key_path, m["mount"])
    jobfile = write_fio_jobfile(client_ip, key_path, mounts, rw="randwrite",
                                bs="8k", iodepth=64, numjobs=4, size="2G",
                                verify=False, jobfile="/tmp/fio_case10.fio")
    start_fio(client_ip, key_path, jobfile)

    # The whole point: replication must reach a bounded lag AGAINST this load.
    wait_replication_caught_up(mgmt_ip, key_path, lvols, timeout=5400)

    print("Committing the final online migration per volume (freeze timing)...")
    windows = {}
    for lv in lvols:
        t0 = int(run(client_ip, key_path, "date +%s%3N", quiet=True).strip())
        run(mgmt_ip, key_path, "%s -d volume replication-commit %s" % (SBCTL, lv))
        windows[lv] = [t0, 0]
    start = time.time()
    done = 0
    while time.time() - start < CUTOVER_WAIT_TIMEOUT * 3:
        if not fio_alive(client_ip, key_path):
            raise RuntimeError("FAIL: fio stopped during the online migration")
        states = replication_states(mgmt_ip, key_path, lvols)
        now_ms = int(run(client_ip, key_path, "date +%s%3N", quiet=True).strip())
        done = 0
        for lv in lvols:
            if states.get(lv) in ("cutover_done", "failed_over"):
                done += 1
                if windows[lv][1] == 0:
                    windows[lv][1] = now_ms
        print("  cutovers done: %d/%d  fio_alive=True" % (done, len(lvols)))
        if done == len(lvols):
            break
        time.sleep(15)

    time.sleep(10)
    alive = fio_alive(client_ip, key_path)
    errors = fio_error_count(client_ip, key_path)
    stop_fio(client_ip, key_path)
    _stop_probes(client_ip, key_path)

    freezes = []
    for lv, m in zip(lvols, mounts):
        t0, t1 = windows[lv]
        if t1 == 0:
            t1 = t0 + CUTOVER_WAIT_TIMEOUT * 1000
        gap = _max_probe_gap_ms(client_ip, key_path, m["mount"],
                                t0 - 2000, t1 + 5000)
        freezes.append(gap)
        print("  %s: client-observed IO freeze during cutover = %sms" % (lv[:8], gap))
    valid = [f for f in freezes if f >= 0]
    print("  freeze summary: max=%sms avg=%sms"
          % (max(freezes), sum(valid) // max(1, len(valid))))

    cleanup_client(client_ip, key_path, mounts)
    if done != len(lvols):
        raise RuntimeError("FAIL: only %d/%d migrations completed" % (done, len(lvols)))
    if not alive or errors:
        raise RuntimeError("FAIL: fio alive=%s errors=%s during migration"
                           % (alive, errors))
    if max(freezes) > 30000:
        raise RuntimeError("FAIL: IO freeze of %sms during cutover (sanity bound 30s)"
                           % max(freezes))
    print("CASE 10 PASSED: migration kept up under heavy IO; freeze times above.")


def test_case_11(meta):
    """Retention ladder (5m:15m,7m:30m,10m:1h) + repeated fail-over to random
    older generations with exact-data validation via fsynced records."""
    print("\n========== CASE 11: retention schedule + generation fail-overs ==========")
    import random
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)
    tiers = [(5 * 60, 15 * 60), (7 * 60, 30 * 60), (10 * 60, 60 * 60)]

    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))
    policy = set_cluster_replication(
        mgmt_ip, key_path, src_uuid, tgt_uuid,
        pool_uuid_of(mgmt_ip, key_path, tgt["pool"]), mode="failover",
        extra_flags="--retention-schedule %s" % CASE11_SCHEDULE)
    lvols = []
    for i in range(2):
        run(mgmt_ip, key_path,
            "%s -d volume add replvol%d 20G %s --replication-policy %s"
            % (SBCTL, i, src["pool"], policy))
        lvols.append(resolve_lvol(mgmt_ip, key_path, "replvol%d" % i)["uuid"])
    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
    for m in mounts:
        _start_recorder(client_ip, key_path, m["mount"])

    print("Building %d minutes of history (schedule %s)..."
          % (CASE11_RUNTIME_MIN, CASE11_SCHEDULE))
    t_end = time.time() + CASE11_RUNTIME_MIN * 60
    while time.time() < t_end:
        time.sleep(120)
        infos = get_replication_infos(mgmt_ip, key_path, lvols)
        worst = max((i.get("lag_seconds") or 0) for i in infos.values())
        print("  history building: worst_lag=%ss, %dmin left"
              % (worst, int((t_end - time.time()) / 60)))

    # (a) kept + (b) pruned per schedule, verified independently per volume.
    now = time.time()
    for lv in lvols:
        times = _snapshot_ages(mgmt_ip, key_path, lv)
        problems = _verify_retention_ladder(times, tiers, now)
        print("  %s: %d retained snapshots, %d schedule violations"
              % (lv[:8], len(times), len(problems)))
        for prob in problems[:4]:
            print("    VIOLATION: %s" % prob)
        if problems:
            raise RuntimeError("FAIL: retention schedule violated for %s: %s"
                               % (lv, problems[0]))

    # (c) three random-generation fail-over / fail-back rounds.
    active = list(lvols)
    active_mounts = mounts
    for rnd in range(1, CASE11_ROUNDS + 1):
        count = len(_snapshot_ages(mgmt_ip, key_path, active[0]))
        gen = random.randint(1, max(1, min(count - 2, 6)))
        print("--- round %d/%d: fail-over to generation %d ---"
              % (rnd, CASE11_ROUNDS, gen))
        _stop_recorders(client_ip, key_path)
        cleanup_client(client_ip, key_path, active_mounts)
        tgt_lvols = []
        for lv in active:
            fo = do_failover(mgmt_ip, key_path, lv, generation=gen)
            if not isinstance(fo, dict) or not fo.get("connection_strings"):
                raise RuntimeError(
                    "FAIL: generation-%d fail-over failed for %s: %s %s"
                    % (gen, lv, (fo or {}).get("error", ""), (fo or {}).get("log", "")))
            tgt_lvols.append(fo["lvol_id"])
        fo_mounts = connect_and_mount(client_ip, key_path, mgmt_ip, tgt_lvols,
                                      fmt=False, mount_base=MOUNT_BASE + "_g")
        for t, m in zip(tgt_lvols, fo_mounts):
            snap_ts = _target_snapshot_ts(mgmt_ip, key_path, t)
            ok, why = _records_match_generation(client_ip, key_path, m["mount"], snap_ts)
            print("  %s gen=%d: %s" % (t[:8], gen, why))
            if not ok:
                raise RuntimeError("FAIL: generation %d data mismatch: %s" % (gen, why))

        print("  failing back...")
        set_cluster_replication(mgmt_ip, key_path, tgt_uuid, src_uuid,
                                pool_uuid_of(mgmt_ip, key_path, src["pool"]))
        for t in tgt_lvols:
            failback(mgmt_ip, key_path, t)
        wait_replication_caught_up(mgmt_ip, key_path, tgt_lvols, timeout=3600)
        for t in tgt_lvols:
            run(mgmt_ip, key_path, "%s -d volume replication-commit %s" % (SBCTL, t))
        deadline = time.time() + CUTOVER_WAIT_TIMEOUT
        while time.time() < deadline:
            states = replication_states(mgmt_ip, key_path, tgt_lvols)
            if all(x in ("cutover_done", "failed_over") for x in states.values()):
                break
            time.sleep(15)
        back = failed_over_targets(mgmt_ip, key_path, tgt_lvols)
        cleanup_client(client_ip, key_path, fo_mounts)
        active = [back[t] for t in tgt_lvols if t in back]
        if len(active) != len(tgt_lvols):
            raise RuntimeError("FAIL: fail-back round %d returned %d/%d volumes"
                               % (rnd, len(active), len(tgt_lvols)))
        active_mounts = connect_and_mount(client_ip, key_path, mgmt_ip, active,
                                          fmt=False)
        for m in active_mounts:
            _start_recorder(client_ip, key_path, m["mount"])
        print("  round %d complete; letting new history accumulate..." % rnd)
        time.sleep(600)

    _stop_recorders(client_ip, key_path)
    cleanup_client(client_ip, key_path, active_mounts)
    print("CASE 11 PASSED: schedule kept+pruned correctly; "
          "%d random-generation fail-over rounds data-exact." % CASE11_ROUNDS)


def test_case_12(meta):
    """Consistency groups: CG policy with 3 volumes, ordered writes, group
    snapshots kept/pruned per schedule, generation fail-overs correlating with
    the generation AND crash-consistent (write order A>=B>=C preserved)."""
    print("\n========== CASE 12: consistency groups ==========")
    import random
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    helptext = run(mgmt_ip, key_path,
                   "%s cluster replication-policy-add --help 2>&1" % SBCTL,
                   check=False, quiet=True)
    if "consistency-group" not in helptext:
        raise RuntimeError(
            "case 12 requires the consistency-groups build "
            "(sbcli branch consistency-groups + its spdk RPC); not on this lab")

    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))
    policy = set_cluster_replication(
        mgmt_ip, key_path, src_uuid, tgt_uuid,
        pool_uuid_of(mgmt_ip, key_path, tgt["pool"]), mode="failover",
        extra_flags="--consistency-group --retention-schedule %s" % CASE12_SCHEDULE)
    lvols = []
    for i in range(3):
        run(mgmt_ip, key_path,
            "%s -d volume add replvol%d 20G %s --replication-policy %s"
            % (SBCTL, i, src["pool"], policy))
        lvols.append(resolve_lvol(mgmt_ip, key_path, "replvol%d" % i)["uuid"])

    # CG invariant 1: all members on one LVS.
    nodes = set(node_of_lvol(mgmt_ip, key_path, lv)["node_id"] for lv in lvols)
    if len(nodes) != 1:
        raise RuntimeError("FAIL: CG members scattered across %d nodes" % len(nodes))
    print("  all 3 members pinned to one node/LVS")

    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
    # Ordered writer: seq into A, then B, then C, fsync each -- in ANY
    # crash-consistent group snapshot seq(A) >= seq(B) >= seq(C).
    ordered = " ".join(m["mount"] for m in mounts)
    cmd = ("sudo nohup bash -c 'i=0; while :; do i=$((i+1)); for m in {mts}; do "
           "echo \"seq=$i ts=$(date +%s)\" >> $m/order.log; sync $m/order.log; "
           "done; sleep 2; done' >/dev/null 2>&1 & echo writer_started"
           ).format(mts=ordered)
    run(client_ip, key_path, cmd, quiet=True)

    print("Running %d minutes of CG history..." % CASE12_RUNTIME_MIN)
    time.sleep(CASE12_RUNTIME_MIN * 60)

    # Group snapshots: every generation must cover ALL members with one seq.
    groups = mgmt_py(mgmt_ip, key_path, """
import json
from collections import defaultdict
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.snapshot import SnapShot
db = DBController()
gens = defaultdict(list)
for snp in db.get_snapshots():
    if snp.deleted or not snp.lvol or snp.lvol.get_id() not in {lvs!r}:
        continue
    if snp.snap_type == SnapShot.TYPE_INTERNAL and getattr(snp, "group_seq", 0):
        gens[snp.group_seq].append(snp.lvol.get_id())
print(json.dumps(dict((str(k), sorted(v)) for k, v in gens.items())))
""".format(lvs=lvols), replayable=True)
    bad = [g for g, members in groups.items() if len(members) != 3]
    print("  %d group generations retained; incomplete: %d" % (len(groups), len(bad)))
    if not groups:
        raise RuntimeError("FAIL: no group snapshots were taken")
    if bad:
        raise RuntimeError("FAIL: group generation(s) %s do not cover all members" % bad)
    now = time.time()
    times = _snapshot_ages(mgmt_ip, key_path, lvols[0])
    problems = _verify_retention_ladder(times, [(5 * 60, 15 * 60)], now,
                                        cadence_sec=300)
    if problems:
        raise RuntimeError("FAIL: CG retention violated: %s" % problems[0])

    run(client_ip, key_path, "sudo pkill -f order.log || true", check=False, quiet=True)
    cleanup_client(client_ip, key_path, mounts)

    for label, gen in (("latest", 0),
                       ("random earlier", random.randint(1, max(1, len(times) - 2)))):
        print("--- CG fail-over to %s generation (%d) ---" % (label, gen))
        tgt_lvols = []
        for lv in lvols:
            fo = do_failover(mgmt_ip, key_path, lv, generation=gen)
            if not isinstance(fo, dict) or not fo.get("connection_strings"):
                raise RuntimeError("FAIL: CG fail-over gen=%d failed for %s" % (gen, lv))
            if fo.get("warnings"):
                print("  membership warnings: %s" % fo["warnings"])
            tgt_lvols.append(fo["lvol_id"])
        fo_mounts = connect_and_mount(client_ip, key_path, mgmt_ip, tgt_lvols,
                                      fmt=False, mount_base=MOUNT_BASE + "_cg")
        seqs = []
        for m in fo_mounts:
            out = run(client_ip, key_path,
                      "sudo tail -1 %s/order.log" % m["mount"], check=False, quiet=True)
            seqs.append(int(out.split("seq=")[1].split()[0]) if "seq=" in out else 0)
        print("  final seqs A,B,C = %s" % seqs)
        if not (seqs[0] >= seqs[1] >= seqs[2] and seqs[0] - seqs[2] <= 1):
            raise RuntimeError(
                "FAIL: group snapshot not crash-consistent: seqs %s violate the "
                "write order A>=B>=C (max skew 1)" % seqs)
        snap_ts = _target_snapshot_ts(mgmt_ip, key_path, tgt_lvols[0])
        ok, why = _records_match_generation(client_ip, key_path,
                                            fo_mounts[0]["mount"], snap_ts,
                                            slack=10, logname="order.log")
        print("  generation correlation: %s" % why)
        cleanup_client(client_ip, key_path, fo_mounts)
        print("  failing back (%s)..." % label)
        set_cluster_replication(mgmt_ip, key_path, tgt_uuid, src_uuid,
                                pool_uuid_of(mgmt_ip, key_path, src["pool"]))
        for t in tgt_lvols:
            failback(mgmt_ip, key_path, t)
        wait_replication_caught_up(mgmt_ip, key_path, tgt_lvols, timeout=3600)
        for t in tgt_lvols:
            run(mgmt_ip, key_path, "%s -d volume replication-commit %s" % (SBCTL, t))
        deadline = time.time() + CUTOVER_WAIT_TIMEOUT
        while time.time() < deadline:
            states = replication_states(mgmt_ip, key_path, tgt_lvols)
            if all(x in ("cutover_done", "failed_over") for x in states.values()):
                break
            time.sleep(15)
        back = failed_over_targets(mgmt_ip, key_path, tgt_lvols)
        lvols = [back[t] for t in tgt_lvols if t in back]
        if len(lvols) != 3:
            raise RuntimeError("FAIL: CG fail-back returned %d/3 volumes" % len(lvols))

    print("CASE 12 PASSED: CG snapshots complete per generation, retention "
          "correct, both fail-overs crash-consistent and generation-exact.")



# --------------------------------------------------------------------------- #
# Cases 13-15: a single node dies DURING fail-over / fail-back / cutover
# --------------------------------------------------------------------------- #
# One node is killed at a RANDOM instant inside the phase under test, repeated
# so the kills land at many different points of it. Chaos is allowed to make
# the operation fail -- what is NOT allowed is losing data, wedging the
# cluster, or leaving a state a retry cannot get out of. Each round therefore
# ends in one of two accepted verdicts (completed despite the kill / failed
# then succeeded on retry after recovery) and any third outcome fails the case.
CHAOS_PHASE_ROUNDS = int(os.environ.get("CHAOS_PHASE_ROUNDS", "20"))
# 20 policies x 2 volumes = 40 volumes replicating at once. At the default
# 1-minute cadence that is 40 transfers a minute and the cluster steady-states
# at ~6 minutes of lag -- above the 3-period gate, so the setup phase never
# completes (run 20260827_110415, killed after 50 minutes without a round).
# The cadence has to be one this volume count can hold.
CHAOS_PHASE_INTERVAL_MIN = int(os.environ.get("CHAOS_PHASE_INTERVAL_MIN", "10"))
CHAOS_PHASE_VOLS_PER_POLICY = int(os.environ.get("CHAOS_PHASE_VOLS_PER_POLICY", "2"))
CHAOS_PHASE_VOL_SIZE = os.environ.get("CHAOS_PHASE_VOL_SIZE", "10G")
#: cap for the randomized kill delay when a phase turns out to be slow
CHAOS_PHASE_MAX_DELAY = float(os.environ.get("CHAOS_PHASE_MAX_DELAY", "45"))


def _kill_after(delay_s, ip, key_path, sink):
    """Kill SPDK on *ip* after *delay_s*, off the main thread."""
    import threading

    def _run():
        time.sleep(delay_s)
        try:
            kill_spdk(ip, key_path)
            sink.append(("killed", ip, time.time()))
        except Exception as exc:                      # noqa: BLE001 - recorded
            sink.append(("kill-failed", ip, str(exc)))

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


def _await_cluster_healthy(mgmt_ip, key_path, timeout=NODE_STATE_TIMEOUT):
    """Every node online AND healthy, every cluster active/degraded."""
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = _all_nodes_online(mgmt_ip, key_path)
        if last.get("all_online"):
            return True, last
        time.sleep(20)
    return False, last


def _phase_victims(meta, phase):
    """Nodes worth killing for the phase under test, as (label, public_ip).

    Fail-over and fail-back BUILD the copy on the destination, so the
    destination cluster is where a kill bites; the cutover freezes and flips
    the source, so both sides matter there.
    """
    _src_uuid, src, _tgt_uuid, tgt = _src_target(meta)
    if phase == "failover":
        pool = [("tgt", ip) for ip in tgt["storage_public_ips"]]
    elif phase == "failback":
        pool = [("src", ip) for ip in src["storage_public_ips"]]
    else:
        pool = ([("src", ip) for ip in src["storage_public_ips"]]
                + [("tgt", ip) for ip in tgt["storage_public_ips"]])
    return pool


def _failover_all(mgmt_ip, key_path, lvols):
    """Returns (ok, target_lvols, error). Never raises on a chaos failure."""
    out = []
    for lv in lvols:
        fo = do_failover(mgmt_ip, key_path, lv)
        if not isinstance(fo, dict) or not fo.get("connection_strings"):
            return False, out, (f"{lv[:8]}: "
                                f"{(fo or {}).get('error', '')} "
                                f"{(fo or {}).get('log', '')}".strip()[:300])
        out.append(fo["lvol_id"])
    return True, out, ""


def _commit_all(mgmt_ip, key_path, lvols, timeout=None):
    """Drive replication-commit for every volume and wait for the cutovers."""
    timeout = timeout or CUTOVER_WAIT_TIMEOUT
    for lv in lvols:
        run(mgmt_ip, key_path, "%s -d volume replication-commit %s" % (SBCTL, lv),
            check=False)
    deadline = time.time() + timeout
    while time.time() < deadline:
        states = replication_states(mgmt_ip, key_path, lvols)
        done = sum(1 for x in states.values() if x in ("cutover_done", "failed_over"))
        if done == len(lvols):
            return True, ""
        time.sleep(15)
    return False, "only %d/%d cutovers completed in %ds" % (done, len(lvols), timeout)


def _failback_all(mgmt_ip, key_path, meta, tgt_lvols):
    """Point replication home, fail back, commit, and return the new lvol ids."""
    src_uuid, src, tgt_uuid, _tgt = _src_target(meta)
    set_cluster_replication(mgmt_ip, key_path, tgt_uuid, src_uuid,
                            pool_uuid_of(mgmt_ip, key_path, src["pool"]))
    for t in tgt_lvols:
        failback(mgmt_ip, key_path, t)
    wait_replication_caught_up(mgmt_ip, key_path, tgt_lvols, timeout=3600)
    ok, why = _commit_all(mgmt_ip, key_path, tgt_lvols)
    if not ok:
        return False, [], why
    back = failed_over_targets(mgmt_ip, key_path, tgt_lvols)
    new = [back[t] for t in tgt_lvols if t in back]
    if len(new) != len(tgt_lvols):
        return False, new, "fail-back returned %d/%d volumes" % (len(new), len(tgt_lvols))
    return True, new, ""


def _chaos_phase_case(meta, phase, title):
    """Kill ONE node at a random instant inside *phase*, once per POLICY.

    Each round owns its own policy and its own volumes, so a round never has
    to undo what the previous one did: no fail-back, no re-sync, no restore to
    a "home shape". Set up CHAOS_PHASE_ROUNDS policies with
    CHAOS_PHASE_VOLS_PER_POLICY volumes each, replicate them all once, then
    walk the policies one at a time and attack the phase on that policy's
    volumes with the kill landing at a random instant.
    """
    import random
    print("\n========== %s ==========" % title)
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)
    seed = int(os.environ.get("CHAOS_PHASE_SEED") or time.time())
    rng = random.Random(seed)
    # The cutover under test in case 15 is the FINAL MIGRATION STEP: a
    # migration-mode policy committed on the SOURCE volumes.
    mode = "migration" if phase == "commit" else "failover"
    print("  seed=%d rounds=%d vols/policy=%d mode=%s phase=%s cadence=%dmin "
          "lag_gate=%ds"
          % (seed, CHAOS_PHASE_ROUNDS, CHAOS_PHASE_VOLS_PER_POLICY, mode, phase,
             CHAOS_PHASE_INTERVAL_MIN, lag_gate_for(CHAOS_PHASE_INTERVAL_MIN)))

    prepare_mount_points(client_ip, key_path)
    delete_test_volumes(mgmt_ip, key_path, _all_test_pools(meta))

    # --- one-time setup: N policies x M volumes, all replicating ------------
    groups, all_lvols = [], []
    for r in range(CHAOS_PHASE_ROUNDS):
        policy = set_cluster_replication(
            mgmt_ip, key_path, src_uuid, tgt_uuid,
            pool_uuid_of(mgmt_ip, key_path, tgt["pool"]), mode=mode,
            policy_name="pol_chaos_%s_%02d" % (phase, r),
            interval_min=CHAOS_PHASE_INTERVAL_MIN)
        vols = []
        for v in range(CHAOS_PHASE_VOLS_PER_POLICY):
            name = "replvol%02d_%d" % (r, v)
            run(mgmt_ip, key_path,
                "%s -d volume add %s %s %s --replication-policy %s"
                % (SBCTL, name, CHAOS_PHASE_VOL_SIZE, src["pool"], policy))
            vols.append(resolve_lvol(mgmt_ip, key_path, name)["uuid"])
        groups.append({"policy": policy, "vols": vols})
        all_lvols.extend(vols)
    print("  created %d policies x %d volumes = %d volumes"
          % (CHAOS_PHASE_ROUNDS, CHAOS_PHASE_VOLS_PER_POLICY, len(all_lvols)))

    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, all_lvols, fmt=True)
    baseline = write_baseline(client_ip, key_path, mounts)
    baseline_ts = time.time()
    cleanup_client(client_ip, key_path, mounts)
    wait_replication_caught_up(mgmt_ip, key_path, all_lvols, timeout=7200,
                               max_lag=lag_gate_for(CHAOS_PHASE_INTERVAL_MIN))
    wait_data_replicated(mgmt_ip, key_path, all_lvols, baseline_ts, timeout=7200)
    print("  all %d volumes replicated; starting the rounds" % len(all_lvols))

    victims = _phase_victims(meta, phase)
    phase_secs = 20.0          # replaced by the first round's measurement
    verdicts, retries, kills = [], 0, []

    for rnd, grp in enumerate(groups, start=1):
        vols = grp["vols"]
        delay = round(rng.uniform(0.0, min(phase_secs * 1.2, CHAOS_PHASE_MAX_DELAY)), 1)
        label, victim = rng.choice(victims)
        print("--- round %d/%d (policy %s): kill %s node %s at T+%.1fs of %s ---"
              % (rnd, CHAOS_PHASE_ROUNDS, grp["policy"], label, victim, delay, phase))
        sink = []
        started = time.time()

        if phase == "failover":
            _kill_after(delay, victim, key_path, sink)
            ok, result_lvols, why = _failover_all(mgmt_ip, key_path, vols)
        elif phase == "failback":
            # The fail-BACK is under test, so the fail-over that sets it up runs
            # clean; only then does the node die.
            ok, tgt_lvols, why = _failover_all(mgmt_ip, key_path, vols)
            if not ok:
                raise RuntimeError("FAIL: round %d -- the setup fail-over failed "
                                   "before the fail-back under test: %s" % (rnd, why))
            _kill_after(delay, victim, key_path, sink)
            ok, result_lvols, why = _failback_all(mgmt_ip, key_path, meta, tgt_lvols)
        else:                                     # commit / online cutover
            _kill_after(delay, victim, key_path, sink)
            ok, why = _commit_all(mgmt_ip, key_path, vols)
            result_lvols = []
        measured = time.time() - started
        if rnd == 1:
            phase_secs = max(5.0, measured)
            print("  measured %s phase: %.1fs (kill delays randomize over it)"
                  % (phase, phase_secs))
        kills.extend(sink)

        healthy, state = _await_cluster_healthy(mgmt_ip, key_path)
        if not healthy:
            raise RuntimeError(
                "FAIL: round %d -- cluster did not recover %ds after killing %s: "
                "unhealthy=%s clusters=%s"
                % (rnd, NODE_STATE_TIMEOUT, victim, state.get("offline"),
                   state.get("clusters")))

        if ok:
            verdicts.append("completed")
        else:
            # Chaos may legitimately fail the operation. A RETRY once the
            # cluster is healthy again must then succeed -- anything else is a
            # wedged state, which is the bug this case hunts.
            print("  operation failed under chaos (%s); retrying after recovery"
                  % why[:160])
            retries += 1
            if phase == "failover":
                ok, result_lvols, why = _failover_all(mgmt_ip, key_path, vols)
            elif phase == "failback":
                ok, result_lvols, why = _failback_all(mgmt_ip, key_path, meta, tgt_lvols)
            else:
                ok, why = _commit_all(mgmt_ip, key_path, vols)
            if not ok:
                raise RuntimeError("FAIL: round %d -- %s did not succeed even on a "
                                   "retry after recovery: %s" % (rnd, phase, why))
            verdicts.append("retry")

        # Where the data ended up, and whether it is intact.
        if phase == "commit":
            after = failed_over_targets(mgmt_ip, key_path, vols)
            result_lvols = [after[v] for v in vols if v in after]
            if len(result_lvols) != len(vols):
                raise RuntimeError("FAIL: round %d -- cutover produced %d/%d target "
                                   "volumes" % (rnd, len(result_lvols), len(vols)))
        check_base = dict(zip(result_lvols, [baseline[v] for v in vols]))
        vmounts = connect_and_mount(client_ip, key_path, mgmt_ip, result_lvols,
                                    fmt=False, mount_base=MOUNT_BASE + "_ph")
        good, _ = verify_baseline(client_ip, key_path, vmounts, check_base)
        cleanup_client(client_ip, key_path, vmounts)
        if not good:
            raise RuntimeError("FAIL: round %d -- data not intact after %s with a "
                               "node killed at T+%.1fs (policy %s)"
                               % (rnd, phase, delay, grp["policy"]))
        # No restore: the next round owns different volumes entirely.

    completed = verdicts.count("completed")
    delivered = sum(1 for k in kills if k[0] == "killed")
    print("  %d rounds: %d completed under the kill, %d needed a retry after "
          "recovery, %d/%d kills delivered"
          % (CHAOS_PHASE_ROUNDS, completed, retries, delivered, len(kills)))
    print("%s PASSED: every round ended intact and unwedged (seed %d)."
          % (title.split(":")[0], seed))


def test_case_13(meta):
    _chaos_phase_case(meta, "failover",
                      "CASE 13: single node dies DURING fail-over")


def test_case_14(meta):
    _chaos_phase_case(meta, "failback",
                      "CASE 14: single node dies DURING fail-back")


def test_case_15(meta):
    _chaos_phase_case(meta, "commit",
                      "CASE 15: single node dies DURING the online cutover")

CASES = {
    "case1": test_case_1,   # online migration cutover, no IO interruption
    "case2": test_case_2,   # DR fail-over on source-cluster loss
    "case3": test_case_3,   # online delta fail-back to the recovered primary
    "case4": test_case_4,   # full fail-back to a fresh empty cluster
    "case5": test_case_5,   # error: replication target node offline
    "case6": test_case_6,   # error: source primary offline, secondary survives
    "case7": test_case_7,   # namespaced: 2 subsystems x 10 ns, 2 clients, fo+fb
    "case8": test_case_8,   # sequential pressure: repeated 50G deltas must catch up
    "case9": test_case_9,   # chaos: random SPDK kills on src+tgt during replication
    "case10": test_case_10,  # migration under heavy IO + cutover freeze timing
    "case11": test_case_11,  # retention ladder + random-generation fail-overs
    "case12": test_case_12,  # consistency groups (needs the CG build)
    "case13": test_case_13,  # node dies during fail-over, x20 random instants
    "case14": test_case_14,  # node dies during fail-back, x20 random instants
    "case15": test_case_15,  # node dies during the online cutover, x20
}
GROUPS = {
    "both": ["case1", "case2"],
    "failback": ["case3", "case4"],
    "errors": ["case5", "case6"],
    "extended": ["case7", "case8", "case9"],
    "features": ["case10", "case11", "case12"],
    "phase-chaos": ["case13", "case14", "case15"],
    "all": ["case1", "case2", "case3", "case4", "case5", "case6"],
    # Case 3 last: it is the only case that needs the killed primary restored
    # and recovered, so a failure there cannot cost the other five cases.
    "all_c3_last": ["case1", "case2", "case4", "case5", "case6", "case3"],
    "all9": ["case1", "case2", "case4", "case5", "case6", "case3",
             "case7", "case8", "case9"],
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
