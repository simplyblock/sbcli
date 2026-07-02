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

# --- fio workload (per the test spec) ---
FIO_NUMJOBS = 4                       # 4 parallel jobs
FIO_RW = "rw"
FIO_BS = "64k"
FIO_IODEPTH = 4
FIO_SIZE = "100G"
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


def run(ip, key_path, cmd, check=True, quiet=False):
    ssh = _ssh(ip, key_path)
    if not quiet:
        print(f"  [{ip}] $ {cmd}")
    _in, out, err = ssh.exec_command(cmd, timeout=900)
    o = out.read().decode()
    e = err.read().decode()
    rc = out.channel.recv_exit_status()
    ssh.close()
    if rc != 0 and check:
        print(o[-2000:])
        print(e[-2000:])
        raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
    return o


def mgmt_py(mgmt_ip, key_path, snippet):
    script = "sudo python3 - <<'PY'\n" + snippet + "\nPY"
    out = run(mgmt_ip, key_path, script)
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
""")


def get_connect_cmds(mgmt_ip, key_path, lvol_uuid):
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.controllers import lvol_controller
entries, err = lvol_controller.connect_lvol({lvol_uuid!r})
print(json.dumps({{"err": str(err) if err else "",
                   "connect": [e.connect for e in (entries or [])],
                   "nqn": (entries[0].nqn if entries else "")}}))
""")


def get_replication_info(mgmt_ip, key_path, lvol_uuid):
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.controllers import lvol_controller
info = lvol_controller.get_replication_info({lvol_uuid!r}) or {{}}
print(json.dumps({{k: info.get(k) for k in
    ("lag_seconds", "outstanding_count", "outstanding_bytes", "replicated_count")}}))
""")


def do_failover(mgmt_ip, key_path, lvol_uuid):
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.controllers import lvol_controller
res = lvol_controller.replicate_lvol_on_target_cluster({lvol_uuid!r})
print(json.dumps(res if isinstance(res, dict) else {{"result": res}}))
""")


def replication_state(mgmt_ip, key_path, lvol_uuid):
    return mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
state = ""
for r in db.get_lvol_replication_objects():
    if r.source_lvol and r.source_lvol.get_id() == {lvol_uuid!r}:
        state = r.state
print(json.dumps({{"state": state}}))
""")


def wait_replication_caught_up(mgmt_ip, key_path, lvol_uuids, timeout=REPL_WAIT_TIMEOUT):
    print("Waiting for replication to catch up on all volumes...")
    start = time.time()
    while time.time() - start < timeout:
        infos = {lv: get_replication_info(mgmt_ip, key_path, lv) for lv in lvol_uuids}
        outstanding = sum((i.get("outstanding_count") or 0) for i in infos.values())
        replicated = min((i.get("replicated_count") or 0) for i in infos.values())
        print(f"  total_outstanding={outstanding} min_replicated={replicated}")
        if outstanding == 0 and replicated > 0:
            print("Replication caught up.")
            return True
        time.sleep(15)
    raise RuntimeError("Timed out waiting for replication to catch up")


# --------------------------------------------------------------------------- #
# Client-side: connect / format / mount / fio
# --------------------------------------------------------------------------- #
def _newest_spdk_devs(client_ip, key_path, count):
    """Return the `count` most recently attached SPDK namespace devices."""
    out = run(client_ip, key_path,
              "ls -1t /dev/nvme*n1 2>/dev/null | head -n %d" % count, quiet=True)
    return [d for d in out.split() if d]


def connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True, mount_base=MOUNT_BASE):
    """Connect each lvol on the client and mount it. Returns [{lvol,nqn,dev,mount}]."""
    mounts = []
    for idx, lv in enumerate(lvols):
        conn = get_connect_cmds(mgmt_ip, key_path, lv)
        assert not conn["err"], f"connect_lvol error for {lv}: {conn['err']}"
        for cmd in conn["connect"]:
            run(client_ip, key_path, cmd)
        time.sleep(3)
        dev = _newest_spdk_devs(client_ip, key_path, 1)[0]
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
        sections += [f"[vol{i}]", f"filename={m['mount']}/fio.dat", ""]
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
    for m in mounts:
        run(client_ip, key_path, f"sudo umount {m['mount']} 2>/dev/null || true", check=False)
        if m.get("nqn"):
            run(client_ip, key_path, f"sudo nvme disconnect -n {m['nqn']} 2>/dev/null || true", check=False)


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
def _src_target(meta):
    src_uuid = meta["replication"]["source_cluster"]
    tgt_uuid = meta["replication"]["target_cluster"]
    _, src = cluster_meta_by_uuid(meta, src_uuid)
    _, tgt = cluster_meta_by_uuid(meta, tgt_uuid)
    return src_uuid, src, tgt_uuid, tgt


def create_volumes(mgmt_ip, key_path, pool, tgt_uuid, mode, count=NUM_VOLUMES):
    lvols = []
    for i in range(count):
        name = f"replvol{i}"
        run(mgmt_ip, key_path, f"{SBCTL} -d volume add {name} {VOL_SIZE} {pool}")
        lv = resolve_lvol(mgmt_ip, key_path, name)
        run(mgmt_ip, key_path,
            f"{SBCTL} volume replication-start {lv['uuid']}"
            f" --replication-cluster-id {tgt_uuid} --mode {mode} --interval-min {REPL_INTERVAL_MIN}")
        lvols.append(lv["uuid"])
        print(f"  created {name} = {lv['uuid']} (replicating, mode={mode})")
    return lvols


def test_case_1(meta):
    print("\n========== CASE 1: online migration (no interruption) ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    # 1. create + replicate (migration mode, 1-min auto snapshots)
    lvols = create_volumes(mgmt_ip, key_path, src["pool"], tgt_uuid, mode="migration")

    # 2. connect/format/mount + start endless fio
    mounts = connect_and_mount(client_ip, key_path, mgmt_ip, lvols, fmt=True)
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
        # commit returns target lvol id; fetch its connect cmds and attach as extra paths
        st = mgmt_py(mgmt_ip, key_path, f"""
import json
from simplyblock_core.db_controller import DBController
db = DBController()
tgt = ""
for r in db.get_lvol_replication_objects():
    if r.source_lvol and r.source_lvol.get_id() == {lv!r}:
        tgt = r.target_lvol.get_id() if r.target_lvol else ""
print(json.dumps({{"target_lvol": tgt}}))
""")
        if st["target_lvol"]:
            conn = get_connect_cmds(mgmt_ip, key_path, st["target_lvol"])
            for cmd in conn.get("connect", []):
                run(client_ip, key_path, cmd, check=False)

    # 5. wait for cutovers to complete, monitoring fio the whole time
    print("Waiting for cutover completion + monitoring fio...")
    start = time.time()
    while time.time() - start < CUTOVER_WAIT_TIMEOUT:
        if not fio_alive(client_ip, key_path):
            raise RuntimeError("FAIL: fio stopped during online migration (interrupted)")
        states = [replication_state(mgmt_ip, key_path, lv)["state"] for lv in lvols]
        done = sum(1 for s in states if s in ("cutover_done", "failed_over"))
        print(f"  cutovers done: {done}/{len(lvols)}  fio_alive=True")
        if done == len(lvols):
            break
        time.sleep(15)

    # 6. assert fio survived with no errors / no >20s latency
    time.sleep(20)
    alive = fio_alive(client_ip, key_path)
    errors = fio_error_count(client_ip, key_path)
    print(f"  fio_alive={alive} error_indicators={errors}")
    stop_fio(client_ip, key_path)
    cleanup_client(client_ip, key_path, mounts)
    if not alive:
        raise RuntimeError("FAIL: fio did not survive the online migration")
    if errors:
        raise RuntimeError(f"FAIL: fio reported {errors} error/latency violations during cutover")
    print("CASE 1 PASSED: online migration completed with no fio interruption.")


def test_case_2(meta):
    print("\n========== CASE 2: fail-over on cluster kill ==========")
    key_path = meta["key_path"]
    mgmt_ip = meta["mgmt"]["public_ip"]
    client_ip = meta["clients"][0]["public_ip"]
    # In case 2 the source is the cluster that gets killed (its volumes fail over
    # to the other cluster). We use the configured replication source as the
    # cluster hosting the volumes and the one we suspend.
    src_uuid, src, tgt_uuid, tgt = _src_target(meta)

    # create + replicate (failover mode, 1-min auto snapshots)
    lvols = create_volumes(mgmt_ip, key_path, src["pool"], tgt_uuid, mode="failover")
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


def main():
    cases = sys.argv[1] if len(sys.argv) > 1 else "both"
    meta = load_meta()
    if cases in ("case1", "both"):
        test_case_1(meta)
    if cases in ("case2", "both"):
        test_case_2(meta)
    print("\n=== DONE ===")


if __name__ == "__main__":
    main()
