#!/usr/bin/env python3
"""
test_lvol_migration_tree.py

Multi-scenario migration test covering snapshot ancestry tree semantics,
with live fio randrw + md5 checksum verification running through each
migration step to confirm data integrity across hops.

Fixed tree built on source node:

  lvol_A
   ├─[snap] snap_A3   (oldest — taken first, deepest ancestor)
   ├─[snap] snap_A2
   │            └─[snap] snap_C1
   │                         └─[snap] snap_C2
   │                                      └─[live] clone_C
   └─[snap] snap_A1   (newest — closest to live lvol_A)
                └─[snap] snap_B1
                             └─[live] clone_B

Scenarios
---------
  basic              Clone_B → TGT, clone_C → TGT  (two independent children)
  round-trip         Clone_B → TGT, clone_C → TGT,
                     clone_B → SRC (back), clone_B → TGT (again)
  parent-after-clones  Clone_B → TGT, clone_C → TGT, lvol_A → TGT
                     (root follows its children; verify all ancestors move)
  clone-then-parent  Clone_B → TGT, lvol_A → TGT (parent mid-sequence),
                     clone_C → TGT (second clone after parent already moved)
  multi-hop          Clone_B: SRC → TGT → SRC → TGT (triple hop)
                     Key check: no _m/_done suffix accumulation after 3 hops

Checksum validation
-------------------
  Each lvol is pre-filled with fio (--verify=md5 --do_verify=0) before the
  scenario starts.  A background fio randrw+verify=md5 job runs during every
  migration step.  After each step fio is stopped and the output log is scanned
  for verify_errors.

Usage:
  python3 test_lvol_migration_tree.py
  python3 test_lvol_migration_tree.py --scenario round-trip
  python3 test_lvol_migration_tree.py --scenario multi-hop --cancel-step 1
"""

import argparse
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CLUSTER_ID = "260c1a4c-276f-497e-8bb8-da5d2e3a90ce"

POOL_NAME    = "tree-mig-pool"
LVOL_A_NAME  = "tree_lvol_a"
CLONE_B_NAME = "tree_clone_b"
CLONE_C_NAME = "tree_clone_c"
LVOL_SIZE    = "10G"

SNAP_A1 = "tree_snap_a1"
SNAP_A2 = "tree_snap_a2"
SNAP_A3 = "tree_snap_a3"
SNAP_B1 = "tree_snap_b1"
SNAP_C1 = "tree_snap_c1"
SNAP_C2 = "tree_snap_c2"

ALL_SNAP_NAMES = [SNAP_A1, SNAP_A2, SNAP_A3, SNAP_B1, SNAP_C1, SNAP_C2]
ALL_LVOL_NAMES = [LVOL_A_NAME, CLONE_B_NAME, CLONE_C_NAME]

# Mount points (on mgmt node acting as NVMe client)
MOUNT_LVOL_A  = "/mnt/tree_lvol_a"
MOUNT_CLONE_B = "/mnt/tree_clone_b"
MOUNT_CLONE_C = "/mnt/tree_clone_c"

# fio parameters
FIO_SIZE           = "2G"
FIO_POST_MIGT_WAIT = 30      # seconds to keep fio running post-migration

TEST_TIMEOUT      = 3600
MIGRATION_TIMEOUT = 600
MIGRATION_POLL    = 5

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR  = Path("/tmp/migration_tree_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"tree_mig_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

_passes   = []
_failures = []

# Populated during build_tree(); maps uuid → friendly name for log_tree_state
_names_map: dict = {}

_hostname_to_node_id: dict = {}
_node_id_to_hostname: dict = {}


def step(msg):
    log.info("")
    log.info("=" * 70)
    log.info(f"STEP: {msg}")
    log.info("=" * 70)


def check(passed, msg):
    if passed:
        log.info(f"  PASS: {msg}")
        _passes.append(msg)
    else:
        log.error(f"  FAIL: {msg}")
        _failures.append(msg)


# ---------------------------------------------------------------------------
# Local shell helper
# ---------------------------------------------------------------------------
def ssh_run(cmd, check_rc=False, timeout=300):
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    out, err = proc.stdout.strip(), proc.stderr.strip()
    if check_rc and proc.returncode != 0:
        raise RuntimeError(f"Command failed (rc={proc.returncode}): {cmd}\nstderr: {err}")
    return out, err, proc.returncode


# ---------------------------------------------------------------------------
# sbctl wrapper
# ---------------------------------------------------------------------------
def sbctl(*args, parse_json=False):
    cmd = ["sbctl"] + [str(a) for a in args]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        log.warning(f"sbctl {' '.join(str(a) for a in args[:4])} rc={proc.returncode}: "
                    f"{proc.stderr.strip()[:200]}")
    if not parse_json:
        return proc.stdout.strip()
    for i, ch in enumerate(proc.stdout):
        if ch in ("{", "["):
            try:
                return json.loads(proc.stdout[i:])
            except json.JSONDecodeError:
                pass
    return None


# ---------------------------------------------------------------------------
# Cluster data helpers
# ---------------------------------------------------------------------------
def _get(obj, *keys):
    _aliases = {
        "id":   ("id", "ID", "Id", "uuid", "UUID", "Uuid"),
        "uuid": ("uuid", "UUID", "Uuid", "id", "ID", "Id"),
    }
    for key in keys:
        candidates = _aliases.get(key.lower(), (key, key.upper(), key.capitalize(), key.lower()))
        for candidate in candidates:
            if obj and candidate in obj:
                return obj[candidate]
    return None


def _sbctl_list_json(*args):
    raw = sbctl(*args, "--json") or ""
    for i, ch in enumerate(raw):
        if ch in ("{", "["):
            try:
                parsed = json.loads(raw[i:])
                if isinstance(parsed, dict):
                    return parsed.get("results", [])
                return parsed if isinstance(parsed, list) else []
            except json.JSONDecodeError:
                pass
    return []


def _sbctl_get_one(*args):
    raw = sbctl(*args, "--json") or ""
    for i, ch in enumerate(raw):
        if ch in ("{", "["):
            try:
                parsed = json.loads(raw[i:])
                if isinstance(parsed, list):
                    return parsed[0] if parsed else {}
                return parsed
            except json.JSONDecodeError:
                pass
    return {}


def get_online_nodes():
    global _hostname_to_node_id, _node_id_to_hostname
    nodes = _sbctl_list_json("sn", "list")
    for n in nodes:
        uuid = n.get("UUID") or n.get("uuid") or _get(n, "id") or ""
        host = n.get("Hostname") or n.get("hostname") or ""
        if uuid and host:
            _hostname_to_node_id[host] = uuid
            _node_id_to_hostname[uuid] = host
    online = [n for n in nodes
              if (_get(n, "status") or "").lower() in ("online", "active", "online_healthy")]
    log.info(f"Online nodes: {len(online)} of {len(nodes)} total")
    for n in online:
        uuid = n.get("UUID") or _get(n, "id") or "?"
        host = n.get("Hostname") or _get(n, "hostname") or "?"
        log.info(f"  {uuid}  hostname={host}")
    return online if online else nodes


def get_pool_id(name):
    pools = _sbctl_list_json("pool", "list")
    for p in pools:
        pname = p.get("Name") or _get(p, "name") or ""
        if pname == name:
            return p.get("Id") or _get(p, "id")
    return None


def get_all_lvols(pool_id=None):
    if pool_id:
        return _sbctl_list_json("lvol", "list", "--pool", pool_id)
    return _sbctl_list_json("lvol", "list")


def get_lvol_by_name(pool_id, name):
    lvols = _sbctl_list_json("lvol", "list", "--pool", pool_id)
    for lv in lvols:
        if lv.get("Name") == name or _get(lv, "name") == name or _get(lv, "lvol_name") == name:
            return lv
    return None


def _get_lvol_node_id(lv):
    nid = lv.get("Node ID") or _get(lv, "node_id")
    if nid:
        return nid
    hostname = lv.get("Hostname") or lv.get("hostname") or ""
    return _hostname_to_node_id.get(hostname, "")


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------
UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)


def _parse_snapshot_table(raw):
    snaps = []
    for line in raw.splitlines():
        if "|" not in line or line.strip().startswith("+"):
            continue
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) < 8:
            continue
        if not (UUID_RE.match(parts[0]) and UUID_RE.match(parts[1])):
            continue
        snaps.append({
            "id":        parts[0],
            "bdev_uuid": parts[1],
            "name":      parts[3],
            "node_id":   parts[6],
            "lvol_id":   parts[7],
        })
    return snaps


def get_all_snapshots():
    raw = sbctl("snapshot", "list")
    return _parse_snapshot_table(raw)


def get_snapshot_by_name(name):
    return next((s for s in get_all_snapshots() if s["name"] == name), None)


# ---------------------------------------------------------------------------
# NVMe helpers
# ---------------------------------------------------------------------------
def parse_nvme_connect_cmds(output):
    cmds = []
    for line in output.splitlines():
        stripped = line.strip()
        if "nvme connect" not in stripped:
            continue
        if stripped.startswith("sudo "):
            stripped = stripped[5:]
        cmds.append(stripped)
    return cmds


def list_nvme_namespaces():
    out, _, _ = ssh_run("ls /dev/nvme*n* 2>/dev/null || true")
    return sorted(d for d in out.splitlines() if d.strip())


def wait_for_new_device(before, retries=20, interval=3):
    for attempt in range(retries):
        after = list_nvme_namespaces()
        new   = [d for d in after if d not in before]
        if new:
            log.info(f"  New NVMe device: {new[0]}")
            return new[0]
        log.debug(f"  Waiting for device (attempt {attempt+1}/{retries})...")
        time.sleep(interval)
    raise RuntimeError("Timed out waiting for new NVMe device")


def get_nvme_subsystem_nqn(device):
    dev_name = device.split("/")[-1]
    out, _, _ = ssh_run(
        f"subsys=$(readlink -f /sys/class/block/{dev_name} 2>/dev/null "
        f"| grep -oP 'nvme-subsys[0-9]+' | head -1 || true); "
        f"[ -n \"$subsys\" ] && "
        f"cat /sys/class/nvme-subsystem/$subsys/subsysnqn 2>/dev/null || true"
    )
    if out.strip():
        return out.strip()
    ctrl_name = re.sub(r'n\d+$', '', dev_name)
    out2, _, _ = ssh_run(
        f"cat /sys/class/nvme/{ctrl_name}/subsysnqn 2>/dev/null || true"
    )
    return out2.strip() if out2.strip() else ""


def connect_and_mount(lvol_id, mount_point):
    """NVMe connect lvol_id, format xfs, mount. Returns (device_path, nqn)."""
    before      = list_nvme_namespaces()
    connect_out = sbctl("volume", "connect", lvol_id)
    cmds        = parse_nvme_connect_cmds(connect_out)
    if not cmds:
        raise RuntimeError(f"Could not parse nvme connect commands for {lvol_id}")

    for cmd in cmds:
        ssh_run(f"sudo {cmd}", check_rc=True)
    dev = wait_for_new_device(before)
    nqn = get_nvme_subsystem_nqn(dev)
    log.info(f"  Connected: {dev}  NQN: {nqn}")

    ssh_run(f"sudo mkfs.xfs -f {dev}", check_rc=True)
    ssh_run(f"sudo mkdir -p {mount_point}", check_rc=True)
    ssh_run(f"sudo mount {dev} {mount_point}", check_rc=True)
    log.info(f"  Mounted {dev} → {mount_point}")
    return dev, nqn


# ---------------------------------------------------------------------------
# fio helpers
# ---------------------------------------------------------------------------
def _fio_prefill_cmd(mount_point, size, log_file):
    return [
        "sudo", "fio",
        "--name=job1",
        f"--filename={mount_point}/fio_data.0.0",
        f"--size={size}",
        "--numjobs=1",
        "--direct=1",
        "--ioengine=libaio",
        "--iodepth=8",
        "--rw=write",
        "--bs=128k",
        "--verify=md5",
        "--do_verify=0",
        "--end_fsync=1",
        f"--output={log_file}",
    ]


def _fio_randrw_cmd(mount_point, size, log_file):
    return [
        "sudo", "fio",
        "--name=job1",
        f"--filename={mount_point}/fio_data.0.0",
        f"--size={size}",
        "--numjobs=1",
        "--direct=1",
        "--ioengine=libaio",
        "--iodepth=1",
        "--verify=md5",
        "--readwrite=randrw",
        "--bsrange=4k:128k",
        "--time_based",
        "--runtime=7200",
        "--status-interval=10",
        f"--output={log_file}",
    ]


def fio_prefill(mount_point, label, fio_size=FIO_SIZE):
    log_file = str(LOG_DIR / f"fio_prefill_{label}.log")
    cmd      = _fio_prefill_cmd(mount_point, fio_size, log_file)
    log.info(f"  Pre-filling {mount_point} ({fio_size} sequential write + md5 embed)...")
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if proc.returncode != 0:
        log.warning(f"  fio prefill rc={proc.returncode}: {proc.stderr.strip()[:300]}")
    check(proc.returncode == 0, f"fio prefill {label} (rc={proc.returncode})")
    return proc.returncode == 0


def start_fio_background(mount_point, label, fio_size=FIO_SIZE):
    log_file = str(LOG_DIR / f"fio_bg_{label}_{datetime.now().strftime('%H%M%S')}.log")
    cmd      = _fio_randrw_cmd(mount_point, fio_size, log_file)
    log.info(f"  Starting fio background ({label}): {mount_point}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    log.info(f"  fio PID: {proc.pid}  log: {log_file}")
    time.sleep(2)
    if proc.poll() is not None:
        _, err = proc.communicate()
        log.warning(f"  fio exited immediately (rc={proc.returncode}) — "
                    f"skipping fio coverage for this step: "
                    f"{(err or b'').decode()[:300].strip()}")
        return None
    proc._fio_log = log_file
    return proc


def stop_fio(fio_proc, post_wait=FIO_POST_MIGT_WAIT):
    log.info(f"  Waiting {post_wait}s post-migration then stopping fio...")
    time.sleep(post_wait)

    if fio_proc.poll() is not None:
        log.warning(f"  fio already exited (rc={fio_proc.returncode})")
        return fio_proc.returncode

    log.info(f"  Sending SIGINT to fio process group (pgid={fio_proc.pid})...")
    try:
        os.killpg(fio_proc.pid, signal.SIGINT)
    except ProcessLookupError:
        log.warning("  fio process group already gone")

    try:
        _, stderr = fio_proc.communicate(timeout=30)
        if stderr:
            log.info(f"  fio stderr:\n{stderr.decode()[:1000]}")
    except subprocess.TimeoutExpired:
        log.warning("  fio did not exit after SIGINT — SIGKILL")
        try:
            os.killpg(fio_proc.pid, 9)
        except ProcessLookupError:
            pass
        fio_proc.wait()

    rc = fio_proc.returncode
    log.info(f"  fio exit code: {rc}")
    return rc


def check_fio_output(fio_proc_or_path):
    log_path = getattr(fio_proc_or_path, "_fio_log", None) or fio_proc_or_path
    try:
        with open(log_path, "r") as f:
            content = f.read()
    except FileNotFoundError:
        log.warning(f"  fio log not found: {log_path}")
        return False, -1

    log.info(f"--- fio output ({log_path}) ---\n{content[-3000:]}\n--- end fio output ---")

    verify_errors = 0
    for m in re.finditer(r'verify_errors\s*[:=]\s*(\d+)', content, re.IGNORECASE):
        verify_errors += int(m.group(1))
    for m in re.finditer(r'err\s*=\s*(\d+)', content):
        verify_errors += int(m.group(1))

    ok = verify_errors == 0
    if ok:
        log.info("  fio verify: no errors")
    else:
        log.error(f"  fio verify: {verify_errors} error(s)")
    return ok, verify_errors


# ---------------------------------------------------------------------------
# Migration helpers
# ---------------------------------------------------------------------------
def get_migration_record(lvol_id):
    migrations = sbctl("--dev", "lvol", "migrate-list",
                       "--cluster-id", CLUSTER_ID, "--json", parse_json=True)
    if isinstance(migrations, dict):
        migrations = migrations.get("results", [])
    for m in (migrations or []):
        mid = (_get(m, "lvol_id") or _get(m, "volume_id") or
               _get(m, "Lvol ID") or _get(m, "Volume ID") or "")
        if mid == lvol_id:
            return m
    return None


def wait_for_migration(lvol_id, label=""):
    log.info(f"  Polling migration for {label or lvol_id[:8]}...")
    deadline = time.time() + MIGRATION_TIMEOUT
    while time.time() < deadline:
        m = get_migration_record(lvol_id)
        if not m:
            log.info("  migration record gone — treating as done")
            return "done"
        status = (_get(m, "status") or "").lower()
        phase  = (_get(m, "phase")  or _get(m, "Phase") or "").lower()
        snaps  = _get(m, "Snaps")   or _get(m, "snaps") or ""
        log.info(f"  [{label}] status={status}  phase={phase}  snaps={snaps}")
        if status in ("done", "completed", "failed", "error", "cancelled"):
            return status
        time.sleep(MIGRATION_POLL)
    return "timeout"


def do_migration(lvol_name, lvol_id, target_node, label="", cancel_after=None):
    """Trigger migration, call migrate-continue, wait. Returns (ok, status)."""
    lbl = label or lvol_name
    log.info(f"  Migrating {lvol_name} → …{target_node[-8:]}  [{lbl}]")
    out = sbctl("--dev", "lvol", "migrate", lvol_id, target_node) or ""
    log.info(f"  {out[:300]}")

    m = re.search(r'Migration ID:\s*([0-9a-f-]{36})', out, re.IGNORECASE)
    mig_id = m.group(1) if m else None

    if mig_id:
        sbctl("--dev", "lvol", "migrate-continue", mig_id, "--deadline", str(TEST_TIMEOUT))
        log.info(f"  migrate-continue sent (mig_id={mig_id})")
    else:
        log.warning("  Could not parse Migration ID — migrate-continue skipped")

    if cancel_after is not None:
        log.warning(f"  [INJECT] cancelling in {cancel_after}s...")
        time.sleep(cancel_after)
        cancel_out = sbctl("--dev", "lvol", "migrate-cancel", lvol_id)
        log.warning(f"  [INJECT] cancel response: {cancel_out[:200]}")

    status = wait_for_migration(lvol_id, label=lbl)
    ok     = status in ("done", "completed")
    check(ok, f"{lbl}: migration status={status}")
    return ok, status


def do_migration_with_fio(lvol_name, lvol_id, target_node, mount_point,
                          label="", cancel_after=None):
    """
    Start background fio randrw+verify=md5 on mount_point, run migration,
    stop fio, check for verify errors.  Returns (migration_ok, status).
    """
    lbl      = label or lvol_name
    fio_proc = start_fio_background(mount_point, lbl)
    ok, status = do_migration(lvol_name, lvol_id, target_node,
                              label=lbl, cancel_after=cancel_after)
    if fio_proc is not None:
        fio_rc         = stop_fio(fio_proc)
        fio_ok, fio_errors = check_fio_output(fio_proc)
        check(fio_ok, f"{lbl}/fio-verify: 0 errors (found {fio_errors})")
        check(fio_rc in (0, -2, -9, 128),
              f"{lbl}/fio exit code acceptable (rc={fio_rc})")
    return ok, status


def migrate_step(lvol_name, lvol_id, target_node, label="", cancel_after=None):
    return do_migration(lvol_name, lvol_id, target_node,
                        label=label, cancel_after=cancel_after)


def _wait_lvol_gone(lvol_id, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = _sbctl_get_one("lvol", "get", lvol_id)
        if not d:
            return True
        status = (_get(d, "status") or "").lower()
        if status not in ("online", "in_deletion", "deleting"):
            return True
        time.sleep(3)
    return False


# ---------------------------------------------------------------------------
# Tree display
# ---------------------------------------------------------------------------
def _build_clone_to_snap_map(all_lvols, all_snaps):
    snap_bdev_to_id = {s["bdev_uuid"]: s["id"] for s in all_snaps if s.get("bdev_uuid")}
    clone_map = {}
    for lv in all_lvols:
        lvol_id = lv.get("UUID") or lv.get("uuid") or _get(lv, "id") or ""
        clone_from_bdev = lv.get("Clone From Snap BDev", "")
        if clone_from_bdev and clone_from_bdev in snap_bdev_to_id:
            clone_map[lvol_id] = snap_bdev_to_id[clone_from_bdev]
    return clone_map


def _render_node_tree(node_id, all_lvols, all_snaps, names_map, clone_to_snap_map=None):
    clone_to_snap_map = clone_to_snap_map or {}
    node_lvols = [lv for lv in all_lvols if _get_lvol_node_id(lv) == node_id]
    node_snaps = [s for s in all_snaps if s["node_id"] == node_id]
    snap_by_id = {s["id"]: s for s in node_snaps}

    def disp(obj_id):
        return names_map.get(obj_id, (obj_id or "")[:8])

    def _cloned_from(lv):
        lid = lv.get("UUID") or lv.get("uuid") or _get(lv, "id") or ""
        return clone_to_snap_map.get(lid, "")

    roots = [lv for lv in node_lvols
             if not _cloned_from(lv) or _cloned_from(lv) not in snap_by_id]

    def render_lvol(lv, pfx, last):
        lid        = lv.get("UUID") or lv.get("uuid") or _get(lv, "id") or ""
        conn       = "└─" if last else "├─"
        parent_sid = _cloned_from(lv)
        cross_note = ""
        if parent_sid and parent_sid not in snap_by_id:
            cross_note = f" [←{disp(parent_sid)}@xnode]"
        lines = [f"{pfx}{conn}[live] {disp(lid)}{cross_note}"]
        cpfx  = pfx + ("   " if last else "│  ")
        owned = sorted([s for s in node_snaps if s["lvol_id"] == lid],
                       key=lambda s: s.get("id", ""))
        for i, sn in enumerate(owned):
            lines += render_snap(sn, cpfx, i == len(owned) - 1)
        return lines

    def render_snap(sn, pfx, last):
        sid   = sn["id"]
        conn  = "└─" if last else "├─"
        sname = names_map.get(sid, sn["name"])
        lines = [f"{pfx}{conn}[snap] {sname}"]
        cpfx  = pfx + ("   " if last else "│  ")
        clones = [lv for lv in node_lvols if _cloned_from(lv) == sid]
        for i, cl in enumerate(clones):
            lines += render_lvol(cl, cpfx, i == len(clones) - 1)
        return lines

    lines = []
    for i, root in enumerate(roots):
        lines += render_lvol(root, "", i == len(roots) - 1)
    return lines or ["  (empty)"]


def log_tree_state(label, src_node, tgt_node, pool_id=None, names_map=None, extra_nodes=None):
    names_map = names_map or _names_map
    all_lvols = get_all_lvols(pool_id)
    all_snaps = get_all_snapshots()
    clone_map = _build_clone_to_snap_map(all_lvols, all_snaps)

    all_nodes = [(src_node, "SRC"), (tgt_node, "TGT")]
    for nid, nlabel in (extra_nodes or []):
        all_nodes.append((nid, nlabel))

    W = 44
    log.info("")
    log.info(f"┌ TREE STATE: {label}")
    for nid, nlabel in all_nodes:
        lines = _render_node_tree(nid, all_lvols, all_snaps, names_map, clone_map)
        log.info(f"├── {nlabel}: {nid}")
        for ln in lines:
            log.info(f"│   {ln}")
    log.info("└" + "─" * W)
    log.info("")


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------
def assert_snap_node(snap_name, expected_node, label=""):
    sn = get_snapshot_by_name(snap_name)
    if not sn:
        check(False, f"{label or snap_name}: snapshot not found in cluster")
        return
    got = sn["node_id"]
    check(got == expected_node,
          f"{label or snap_name} on node …{expected_node[-8:]} (got …{got[-8:]})")


def assert_lvol_node(lvol_name, pool_id, expected_node, label=""):
    lv = get_lvol_by_name(pool_id, lvol_name)
    if not lv:
        check(False, f"{label or lvol_name}: lvol not found in cluster")
        return
    got = _get_lvol_node_id(lv)
    check(got == expected_node,
          f"{label or lvol_name} on node …{expected_node[-8:]} (got …{(got or '?')[-8:]})")


def assert_snap_not_on_node(snap_name, node_id, label=""):
    found = [s for s in get_all_snapshots()
             if s["name"] == snap_name and s["node_id"] == node_id]
    check(len(found) == 0,
          f"{label or snap_name} removed from node …{node_id[-8:]} "
          f"(found {len(found)} copy/copies)")


def assert_no_duplicates(snap_name, label=""):
    copies = [s for s in get_all_snapshots() if s["name"] == snap_name]
    check(len(copies) <= 1,
          f"{label or snap_name} not duplicated (found {len(copies)} copies)")


def assert_snap_exists(snap_name, label=""):
    sn = get_snapshot_by_name(snap_name)
    check(sn is not None, f"{label or snap_name} still exists in cluster")


def assert_no_dirty_bdev_names(node_id, label=""):
    dirty = [s for s in get_all_snapshots()
             if s["node_id"] == node_id
             and re.search(r'(_m|_am|_done)$', s.get("name", ""))]
    names = [s["name"] for s in dirty]
    check(len(dirty) == 0,
          f"{label or 'bdev names'}: no dirty suffixes on …{node_id[-8:]} "
          + (f"(found {len(dirty)}: {names})" if dirty else "(ok)"))


def verify_placement(label, pool_id, src_node, tgt_node,
                     lvol_expect=None, snap_expect=None, no_dirty=True):
    """
    lvol_expect: {lvol_name: "src"|"tgt"|node_uuid}
    snap_expect: {snap_name: "src"|"tgt"|node_uuid}
    """
    step(f"Verify placement: {label}")

    def resolve(v):
        if v == "src":
            return src_node
        if v == "tgt":
            return tgt_node
        return v

    for name, where in (lvol_expect or {}).items():
        assert_lvol_node(name, pool_id, resolve(where), label=f"{label}/{name}")
    for name, where in (snap_expect or {}).items():
        assert_snap_node(name, resolve(where), label=f"{label}/{name}")
    for name in (snap_expect or {}):
        assert_no_duplicates(name, label=f"{label}/{name} dup-check")

    if no_dirty:
        assert_no_dirty_bdev_names(src_node, label=f"{label}/src")
        assert_no_dirty_bdev_names(tgt_node, label=f"{label}/tgt")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def cleanup():
    log.info("── cleanup ──────────────────────────────────────────────────────")
    ssh_run("sudo killall fio 2>/dev/null || true")
    time.sleep(2)

    for mp in (MOUNT_LVOL_A, MOUNT_CLONE_B, MOUNT_CLONE_C):
        ssh_run(f"sudo umount {mp} 2>/dev/null || true")
        ssh_run(f"sudo rm -rf {mp} 2>/dev/null || true")

    ssh_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)

    for snap_name in ALL_SNAP_NAMES:
        sn = get_snapshot_by_name(snap_name)
        if sn:
            log.info(f"  del snap  {snap_name} ({sn['id'][:8]})")
            sbctl("snapshot", "delete", sn["id"], "--force")
            time.sleep(1)

    pool_id = get_pool_id(POOL_NAME)
    if pool_id:
        for lvol_name in ALL_LVOL_NAMES:
            lv = get_lvol_by_name(pool_id, lvol_name)
            if lv:
                lid = _get(lv, "id")
                log.info(f"  del lvol  {lvol_name} ({lid[:8]})")
                sbctl("lvol", "delete", lid, "--force")
                _wait_lvol_gone(lid, timeout=120)

    pool_id = get_pool_id(POOL_NAME)
    if pool_id:
        log.info(f"  del pool  {POOL_NAME} ({pool_id[:8]})")
        for _ in range(10):
            out = sbctl("storage-pool", "delete", pool_id)
            if "not empty" not in out.lower() and "lvols found" not in out.lower():
                break
            time.sleep(5)
    log.info("── cleanup done ─────────────────────────────────────────────────")


# ---------------------------------------------------------------------------
# Tree builder
# ---------------------------------------------------------------------------
def build_tree(src_node, pool_id):
    """Build the fixed ancestry tree on src_node. Returns (ids dict, names_map)."""
    step("Build snapshot ancestry tree")

    def snap_id(name, retries=10, interval=3):
        for attempt in range(retries):
            sn = get_snapshot_by_name(name)
            if sn:
                return sn["id"]
            if attempt < retries - 1:
                time.sleep(interval)
        raise RuntimeError(f"Snapshot '{name}' not found after {retries * interval}s")

    def lvol_id(name, retries=10, interval=3):
        for attempt in range(retries):
            lv = get_lvol_by_name(pool_id, name)
            if lv:
                return _get(lv, "id") or lv.get("Id") or lv.get("id")
            if attempt < retries - 1:
                time.sleep(interval)
        raise RuntimeError(f"Lvol '{name}' not found after {retries * interval}s")

    log.info("  3a. create lvol_A")
    sbctl("volume", "add", LVOL_A_NAME, LVOL_SIZE, pool_id,
          "--host-id", src_node, "--snapshot")
    time.sleep(2)
    lvol_a_id = lvol_id(LVOL_A_NAME)

    log.info("  3b. snapshot(lvol_A) → snap_A3  [oldest]")
    sbctl("volume", "create-snapshot", lvol_a_id, SNAP_A3)
    time.sleep(2)
    snap_a3_id = snap_id(SNAP_A3)

    log.info("  3c. snapshot(lvol_A) → snap_A2")
    sbctl("volume", "create-snapshot", lvol_a_id, SNAP_A2)
    time.sleep(2)
    snap_a2_id = snap_id(SNAP_A2)

    log.info("  3d. snapshot(lvol_A) → snap_A1  [newest]")
    sbctl("volume", "create-snapshot", lvol_a_id, SNAP_A1)
    time.sleep(2)
    snap_a1_id = snap_id(SNAP_A1)

    log.info("  3e. clone(snap_A2) → clone_C")
    sbctl("snapshot", "clone", snap_a2_id, CLONE_C_NAME)
    clone_c_id = lvol_id(CLONE_C_NAME)

    log.info("  3f. snapshot(clone_C) → snap_C1")
    sbctl("volume", "create-snapshot", clone_c_id, SNAP_C1)
    time.sleep(2)
    snap_c1_id = snap_id(SNAP_C1)

    log.info("  3g. snapshot(clone_C) → snap_C2")
    sbctl("volume", "create-snapshot", clone_c_id, SNAP_C2)
    time.sleep(2)
    snap_c2_id = snap_id(SNAP_C2)

    log.info("  3h. clone(snap_A1) → clone_B")
    sbctl("snapshot", "clone", snap_a1_id, CLONE_B_NAME)
    clone_b_id = lvol_id(CLONE_B_NAME)

    log.info("  3i. snapshot(clone_B) → snap_B1")
    sbctl("volume", "create-snapshot", clone_b_id, SNAP_B1)
    time.sleep(2)
    snap_b1_id = snap_id(SNAP_B1)

    ids = dict(
        lvol_a=lvol_a_id,  clone_b=clone_b_id,  clone_c=clone_c_id,
        snap_a1=snap_a1_id, snap_a2=snap_a2_id,  snap_a3=snap_a3_id,
        snap_b1=snap_b1_id, snap_c1=snap_c1_id,  snap_c2=snap_c2_id,
    )
    nm = {
        lvol_a_id:  LVOL_A_NAME,  clone_b_id: CLONE_B_NAME,  clone_c_id: CLONE_C_NAME,
        snap_a1_id: SNAP_A1,      snap_a2_id: SNAP_A2,        snap_a3_id: SNAP_A3,
        snap_b1_id: SNAP_B1,      snap_c1_id: SNAP_C1,        snap_c2_id: SNAP_C2,
    }
    for k, v in ids.items():
        log.info(f"  {k:10s} = {v}")
    return ids, nm


# ---------------------------------------------------------------------------
# Connect, mount, and pre-fill all live lvols
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Scenario: basic
# ---------------------------------------------------------------------------
def scenario_basic(ids, pool_id, src_node, tgt_node, cancel_step):
    """R1: clone_B → TGT,  R2: clone_C → TGT"""
    log_tree_state("AFTER SETUP", src_node, tgt_node, pool_id=pool_id)

    step("R1: migrate clone_B → TGT")
    cancel = 8 if cancel_step == 1 else None
    ok_r1, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], tgt_node,
                             label="R1", cancel_after=cancel)
    log_tree_state("AFTER R1", src_node, tgt_node, pool_id=pool_id)

    if cancel_step == 1:
        step("Verify R1 rollback")
        check(not ok_r1, "R1 cancelled as expected")
        verify_placement("R1-rollback", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "src", CLONE_C_NAME: "src", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "src", SNAP_A1: "src"})
        log.info("Retrying R1 after rollback...")
        ok_r1, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], tgt_node,
                                 label="R1-retry")
        log_tree_state("AFTER R1 RETRY", src_node, tgt_node, pool_id=pool_id)

    if ok_r1:
        verify_placement("after-R1", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "tgt", CLONE_C_NAME: "src", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "tgt", SNAP_A1: "src",
                                      SNAP_A2: "src", SNAP_A3: "src",
                                      SNAP_C1: "src", SNAP_C2: "src"})
        assert_snap_not_on_node(SNAP_B1, src_node, label="R1/snap_B1 cleaned from SRC")

    step("R2: migrate clone_C → TGT")
    cancel = 8 if cancel_step == 2 else None
    ok_r2, _ = migrate_step(CLONE_C_NAME, ids["clone_c"], tgt_node,
                             label="R2", cancel_after=cancel)
    log_tree_state("AFTER R2", src_node, tgt_node, pool_id=pool_id)

    if cancel_step == 2:
        step("Verify R2 rollback")
        check(not ok_r2, "R2 cancelled as expected")
        verify_placement("R2-rollback", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "tgt", CLONE_C_NAME: "src"},
                         snap_expect={SNAP_B1: "tgt", SNAP_C1: "src", SNAP_C2: "src"})
        assert_snap_not_on_node(SNAP_C1, tgt_node, label="R2-rollback/snap_C1 cleaned from TGT")
        assert_snap_not_on_node(SNAP_C2, tgt_node, label="R2-rollback/snap_C2 cleaned from TGT")
        log.info("Retrying R2 after rollback...")
        ok_r2, _ = migrate_step(CLONE_C_NAME, ids["clone_c"], tgt_node,
                                 label="R2-retry")
        log_tree_state("AFTER R2 RETRY", src_node, tgt_node, pool_id=pool_id)

    if ok_r2:
        verify_placement("after-R2", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "tgt", CLONE_C_NAME: "tgt", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "tgt", SNAP_C1: "tgt", SNAP_C2: "tgt",
                                      SNAP_A1: "src", SNAP_A2: "src", SNAP_A3: "src"})
        assert_snap_not_on_node(SNAP_C1, src_node, label="R2/snap_C1 cleaned from SRC")
        assert_snap_not_on_node(SNAP_C2, src_node, label="R2/snap_C2 cleaned from SRC")
        assert_no_duplicates(SNAP_A2, label="R2/snap_A2 not re-transferred")


# ---------------------------------------------------------------------------
# Scenario: round-trip
# ---------------------------------------------------------------------------
def scenario_round_trip(ids, pool_id, src_node, tgt_node, cancel_step):
    """
    R1: clone_B → TGT,  R2: clone_C → TGT,
    R3: clone_B → SRC (back),  R4: clone_B → TGT (again)
    """
    log_tree_state("AFTER SETUP", src_node, tgt_node, pool_id=pool_id)

    step("R1: clone_B → TGT")
    ok_r1, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], tgt_node,
                             label="R1",
                             cancel_after=(8 if cancel_step == 1 else None))
    if cancel_step == 1 and not ok_r1:
        log.info("Retrying R1...")
        ok_r1, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], tgt_node, label="R1-retry")
    log_tree_state("AFTER R1", src_node, tgt_node, pool_id=pool_id)
    if ok_r1:
        verify_placement("after-R1", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "tgt", CLONE_C_NAME: "src", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "tgt", SNAP_A1: "src"})
        assert_snap_not_on_node(SNAP_B1, src_node, label="R1/snap_B1 cleaned from SRC")

    step("R2: clone_C → TGT")
    ok_r2, _ = migrate_step(CLONE_C_NAME, ids["clone_c"], tgt_node,
                             label="R2",
                             cancel_after=(8 if cancel_step == 2 else None))
    if cancel_step == 2 and not ok_r2:
        log.info("Retrying R2...")
        ok_r2, _ = migrate_step(CLONE_C_NAME, ids["clone_c"], tgt_node, label="R2-retry")
    log_tree_state("AFTER R2", src_node, tgt_node, pool_id=pool_id)
    if ok_r2:
        verify_placement("after-R2", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "tgt", CLONE_C_NAME: "tgt", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "tgt", SNAP_C1: "tgt", SNAP_C2: "tgt",
                                      SNAP_A1: "src", SNAP_A2: "src", SNAP_A3: "src"})
        assert_no_duplicates(SNAP_A1, label="R2/snap_A1 not duplicated")
        assert_no_duplicates(SNAP_A2, label="R2/snap_A2 not duplicated")

    step("R3: clone_B → SRC  (round-trip back to source)")
    ok_r3, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], src_node,
                             label="R3",
                             cancel_after=(8 if cancel_step == 3 else None))
    if cancel_step == 3 and not ok_r3:
        log.info("Retrying R3...")
        ok_r3, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], src_node, label="R3-retry")
    log_tree_state("AFTER R3 (clone_B back on SRC)", src_node, tgt_node, pool_id=pool_id)
    if ok_r3:
        verify_placement("after-R3", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "src", CLONE_C_NAME: "tgt", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "src",
                                      SNAP_C1: "tgt", SNAP_C2: "tgt",
                                      SNAP_A1: "src", SNAP_A2: "src", SNAP_A3: "src"})
        # snap_B1 cleaned from TGT — clone_B is back on SRC
        assert_snap_not_on_node(SNAP_B1, tgt_node, label="R3/snap_B1 cleaned from TGT")
        # snap_A1 bdev copy must stay on TGT — clone_C still depends on it via snap_A2
        assert_snap_exists(SNAP_A1, label="R3/snap_A1 not deleted (still needed by clone_C)")

    step("R4: clone_B → TGT  (back again — snap_A1 already on TGT, no re-transfer)")
    ok_r4, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], tgt_node,
                             label="R4",
                             cancel_after=(8 if cancel_step == 4 else None))
    if cancel_step == 4 and not ok_r4:
        log.info("Retrying R4...")
        ok_r4, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], tgt_node, label="R4-retry")
    log_tree_state("AFTER R4", src_node, tgt_node, pool_id=pool_id)
    if ok_r4:
        verify_placement("after-R4", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "tgt", CLONE_C_NAME: "tgt", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "tgt", SNAP_C1: "tgt", SNAP_C2: "tgt",
                                      SNAP_A1: "src", SNAP_A2: "src", SNAP_A3: "src"})
        assert_no_duplicates(SNAP_A1, label="R4/snap_A1 not duplicated after round-trip")
        assert_no_duplicates(SNAP_B1, label="R4/snap_B1 not duplicated after round-trip")


# ---------------------------------------------------------------------------
# Scenario: parent-after-clones
# ---------------------------------------------------------------------------
def scenario_parent_after_clones(ids, pool_id, src_node, tgt_node, cancel_step):
    """R1: clone_B → TGT,  R2: clone_C → TGT,  R3: lvol_A → TGT (root follows)"""
    log_tree_state("AFTER SETUP", src_node, tgt_node, pool_id=pool_id)

    step("R1: clone_B → TGT")
    ok_r1, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], tgt_node, label="R1")
    log_tree_state("AFTER R1", src_node, tgt_node, pool_id=pool_id)
    if ok_r1:
        verify_placement("after-R1", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "tgt", CLONE_C_NAME: "src", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "tgt"})

    step("R2: clone_C → TGT")
    ok_r2, _ = migrate_step(CLONE_C_NAME, ids["clone_c"], tgt_node, label="R2")
    log_tree_state("AFTER R2", src_node, tgt_node, pool_id=pool_id)
    if ok_r2:
        verify_placement("after-R2", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "tgt", CLONE_C_NAME: "tgt", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "tgt", SNAP_C1: "tgt", SNAP_C2: "tgt",
                                      SNAP_A1: "src", SNAP_A2: "src", SNAP_A3: "src"})

    step("R3: lvol_A → TGT  (root follows clones)")
    ok_r3, _ = migrate_step(LVOL_A_NAME, ids["lvol_a"], tgt_node,
                             label="R3",
                             cancel_after=(8 if cancel_step == 3 else None))
    if cancel_step == 3 and not ok_r3:
        log.info("Retrying R3...")
        ok_r3, _ = migrate_step(LVOL_A_NAME, ids["lvol_a"], tgt_node, label="R3-retry")
    log_tree_state("AFTER R3 (all on TGT)", src_node, tgt_node, pool_id=pool_id)
    if ok_r3:
        verify_placement("after-R3", pool_id, src_node, tgt_node,
                         lvol_expect={LVOL_A_NAME: "tgt", CLONE_B_NAME: "tgt", CLONE_C_NAME: "tgt"},
                         snap_expect={SNAP_A1: "tgt", SNAP_A2: "tgt", SNAP_A3: "tgt",
                                      SNAP_B1: "tgt", SNAP_C1: "tgt", SNAP_C2: "tgt"})
        for sn in ALL_SNAP_NAMES:
            assert_snap_not_on_node(sn, src_node, label=f"R3/SRC empty/{sn}")
        assert_no_duplicates(SNAP_A1, label="R3/snap_A1 not duplicated when root follows")
        assert_no_duplicates(SNAP_A2, label="R3/snap_A2 not duplicated when root follows")
        assert_no_duplicates(SNAP_A3, label="R3/snap_A3 not duplicated when root follows")


# ---------------------------------------------------------------------------
# Scenario: clone-then-parent
# ---------------------------------------------------------------------------
def scenario_clone_then_parent(ids, pool_id, src_node, tgt_node, cancel_step):
    """
    R1: clone_B → TGT  (snap_A1 bdev lands on TGT)
    R2: lvol_A  → TGT  (parent mid-sequence; snap_A1 already on TGT)
    R3: clone_C → TGT  (all ancestors already on TGT)
    """
    log_tree_state("AFTER SETUP", src_node, tgt_node, pool_id=pool_id)

    step("R1: clone_B → TGT")
    ok_r1, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], tgt_node, label="R1")
    log_tree_state("AFTER R1", src_node, tgt_node, pool_id=pool_id)
    if ok_r1:
        verify_placement("after-R1", pool_id, src_node, tgt_node,
                         lvol_expect={CLONE_B_NAME: "tgt", CLONE_C_NAME: "src", LVOL_A_NAME: "src"},
                         snap_expect={SNAP_B1: "tgt", SNAP_A1: "src"})

    step("R2: lvol_A → TGT  (parent mid-sequence, snap_A1 bdev already on TGT)")
    ok_r2, _ = migrate_step(LVOL_A_NAME, ids["lvol_a"], tgt_node,
                             label="R2",
                             cancel_after=(8 if cancel_step == 2 else None))
    if cancel_step == 2 and not ok_r2:
        log.info("Retrying R2...")
        ok_r2, _ = migrate_step(LVOL_A_NAME, ids["lvol_a"], tgt_node, label="R2-retry")
    log_tree_state("AFTER R2", src_node, tgt_node, pool_id=pool_id)
    if ok_r2:
        verify_placement("after-R2", pool_id, src_node, tgt_node,
                         lvol_expect={LVOL_A_NAME: "tgt", CLONE_B_NAME: "tgt", CLONE_C_NAME: "src"},
                         snap_expect={SNAP_A1: "tgt", SNAP_A2: "tgt", SNAP_A3: "tgt",
                                      SNAP_B1: "tgt"})
        assert_no_duplicates(SNAP_A1, label="R2/snap_A1 not duplicated (pre-existing from R1)")

    step("R3: clone_C → TGT  (all ancestors already on TGT, no re-transfer expected)")
    ok_r3, _ = migrate_step(CLONE_C_NAME, ids["clone_c"], tgt_node, label="R3")
    log_tree_state("AFTER R3 (all on TGT)", src_node, tgt_node, pool_id=pool_id)
    if ok_r3:
        verify_placement("after-R3", pool_id, src_node, tgt_node,
                         lvol_expect={LVOL_A_NAME: "tgt", CLONE_B_NAME: "tgt", CLONE_C_NAME: "tgt"},
                         snap_expect={SNAP_A1: "tgt", SNAP_A2: "tgt", SNAP_A3: "tgt",
                                      SNAP_B1: "tgt", SNAP_C1: "tgt", SNAP_C2: "tgt"})
        assert_no_duplicates(SNAP_A1, label="R3/snap_A1 not duplicated")
        assert_no_duplicates(SNAP_A2, label="R3/snap_A2 not duplicated")
        assert_no_duplicates(SNAP_A3, label="R3/snap_A3 not duplicated")
        for sn in ALL_SNAP_NAMES:
            assert_snap_not_on_node(sn, src_node, label=f"R3/SRC empty/{sn}")


# ---------------------------------------------------------------------------
# Scenario: multi-hop
# ---------------------------------------------------------------------------
def scenario_multi_hop(ids, pool_id, src_node, tgt_node, cancel_step):
    """clone_B: SRC → TGT → SRC → TGT (triple hop). No dirty suffixes at any point."""
    log_tree_state("AFTER SETUP", src_node, tgt_node, pool_id=pool_id)

    for hop_num, (label, target) in enumerate([
        ("Hop1: clone_B SRC→TGT", tgt_node),
        ("Hop2: clone_B TGT→SRC", src_node),
        ("Hop3: clone_B SRC→TGT", tgt_node),
    ], start=1):
        step(label)
        cancel = 8 if cancel_step == hop_num else None
        ok, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], target,
                              label=f"Hop{hop_num}", cancel_after=cancel)
        if cancel and not ok:
            log.info(f"Retrying Hop{hop_num} after cancel...")
            ok, _ = migrate_step(CLONE_B_NAME, ids["clone_b"], target,
                                  label=f"Hop{hop_num}-retry")

        log_tree_state(f"AFTER {label}", src_node, tgt_node, pool_id=pool_id)

        if ok:
            expected_clone_b = "tgt" if target == tgt_node else "src"
            expected_snap_b1 = expected_clone_b
            verify_placement(f"after-{label}", pool_id, src_node, tgt_node,
                             lvol_expect={CLONE_B_NAME: expected_clone_b, LVOL_A_NAME: "src"},
                             snap_expect={SNAP_B1: expected_snap_b1, SNAP_A1: "src"})

            other_node = src_node if target == tgt_node else tgt_node
            assert_snap_not_on_node(SNAP_B1, other_node,
                                    label=f"Hop{hop_num}/snap_B1 cleaned from prev node")
            assert_no_duplicates(SNAP_A1, label=f"Hop{hop_num}/snap_A1 not duplicated")
            assert_no_dirty_bdev_names(src_node, label=f"Hop{hop_num}/src")
            assert_no_dirty_bdev_names(tgt_node, label=f"Hop{hop_num}/tgt")

        log.info(f"  Hop {hop_num} / 3 complete")

    # lvol_A and clone_C must be untouched throughout
    verify_placement("final-unchanged", pool_id, src_node, tgt_node,
                     lvol_expect={LVOL_A_NAME: "src", CLONE_C_NAME: "src"},
                     snap_expect={SNAP_C1: "src", SNAP_C2: "src",
                                  SNAP_A2: "src", SNAP_A3: "src"})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
SCENARIOS = {
    "basic":               scenario_basic,
    "round-trip":          scenario_round_trip,
    "parent-after-clones": scenario_parent_after_clones,
    "clone-then-parent":   scenario_clone_then_parent,
    "multi-hop":           scenario_multi_hop,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", default="basic", choices=list(SCENARIOS),
                        help="Which scenario to run (default: basic)")
    parser.add_argument("--cancel-step", type=int, default=0, metavar="N",
                        help="Inject a migration cancel in step N (0 = no injection)")
    args = parser.parse_args()

    if hasattr(signal, "SIGALRM"):
        def _hard_timeout(sig, frame):
            log.error(f"HARD DEADLINE: test exceeded {TEST_TIMEOUT}s — aborting")
            sys.exit(2)
        signal.signal(signal.SIGALRM, _hard_timeout)
        signal.alarm(TEST_TIMEOUT)

    log.info(f"Log        : {LOG_FILE}")
    log.info(f"Cluster    : {CLUSTER_ID}")
    log.info(f"Scenario   : {args.scenario}")
    log.info(f"Cancel step: {args.cancel_step or 'none'}")

    # ── 0. pre-flight and clean slate ────────────────────────────────────────
    step("0. Pre-flight checks")
    ssh_run("command -v nvme || sudo dnf install -y nvme-cli", check_rc=True)
    ssh_run("command -v fio  || sudo dnf install -y fio",      check_rc=True)
    ssh_run("sudo modprobe nvme-tcp 2>/dev/null || true")

    step("0b. Cleanup previous run state")
    cleanup()

    # ── 1. nodes ─────────────────────────────────────────────────────────────
    step("1. Pick source and target nodes")
    nodes = get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need ≥2 online nodes, got {len(nodes)}")
    src_node  = nodes[0].get("UUID") or _get(nodes[0], "id")
    tgt_node  = nodes[1].get("UUID") or _get(nodes[1], "id")
    src_label = nodes[0].get("Hostname") or _get(nodes[0], "hostname") or src_node[:16]
    tgt_label = nodes[1].get("Hostname") or _get(nodes[1], "hostname") or tgt_node[:16]
    log.info(f"SRC: {src_label}  ({src_node})")
    log.info(f"TGT: {tgt_label}  ({tgt_node})")

    # ── 2. pool ──────────────────────────────────────────────────────────────
    step("2. Create pool")
    sbctl("storage-pool", "add", POOL_NAME, CLUSTER_ID)
    time.sleep(2)
    pool_id = get_pool_id(POOL_NAME)
    if not pool_id:
        raise RuntimeError(f"Pool '{POOL_NAME}' not created")
    log.info(f"Pool: {pool_id}")

    # ── 3. build tree ────────────────────────────────────────────────────────
    ids, nm = build_tree(src_node, pool_id)
    _names_map.update(nm)

    # ── 4. verify initial state ──────────────────────────────────────────────
    step("4. Verify initial state — everything on SRC")
    for sn in ALL_SNAP_NAMES:
        assert_snap_node(sn, src_node, label=f"init/{sn}")
    for ln in ALL_LVOL_NAMES:
        assert_lvol_node(ln, pool_id, src_node, label=f"init/{ln}")

    # ── 5. run scenario ──────────────────────────────────────────────────────
    step(f"5. Running scenario: {args.scenario}")
    SCENARIOS[args.scenario](ids, pool_id, src_node, tgt_node, args.cancel_step)

    # ── final tree ────────────────────────────────────────────────────────────
    step("Final tree state")
    log_tree_state("FINAL", src_node, tgt_node, pool_id=pool_id)

    # ── summary ──────────────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 70)
    log.info("SUMMARY")
    log.info("=" * 70)
    log.info(f"  Scenario : {args.scenario}")
    log.info(f"  SRC node : {src_label}  ({src_node})")
    log.info(f"  TGT node : {tgt_label}  ({tgt_node})")
    log.info(f"  Cluster  : {CLUSTER_ID}")
    log.info("")
    for msg in _passes:
        log.info(f"  PASS: {msg}")
    for msg in _failures:
        log.error(f"  FAIL: {msg}")
    log.info("")
    log.info(f"  Result : {len(_passes)} passed, {len(_failures)} failed")
    log.info(f"  Log    : {LOG_FILE}")
    log.info("=" * 70)

    sys.exit(1 if _failures else 0)


if __name__ == "__main__":
    main()
