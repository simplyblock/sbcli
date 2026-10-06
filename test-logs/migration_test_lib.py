#!/usr/bin/env python3
"""
migration_test_lib.py

Shared helper library extracted from the three working migration/namespace
test scripts:

  test-logs/test-plan/test_lvol_migration.py
  test-logs/test-plan/test_migration_chaos.py
  test-logs/test-plan/test_shared_namespace.py

For each concern, the most refined implementation among the three was kept:

  sbctl/_get JSON parsing ......... test_migration_chaos.py   (bidirectional
                                     id/uuid alias dict)
  cluster/pool discovery .......... test_migration_chaos.py + test_shared_namespace.py
                                     (dynamic discover_cluster_id(), no
                                     hardcoded cluster UUID; optional pool
                                     reuse instead of forced creation)
  node/HA topology, target pick ... test_lvol_migration.py    (secondary /
                                     tertiary aware, Case A/B/C/D overlap
                                     targeting)
  device connect/mount ............ test_migration_chaos.py   (repeat-mount
                                     with xfs_repair fallback after a fault)
  NVMe/ANA diagnostics, cutover
    controller cleanup ............ test_lvol_migration.py    (only script
                                     that inspects ana_state and disconnects
                                     stale SRC controllers post-cutover)
  migration status polling ........ test_lvol_migration.py    (distinguishes
                                     the 'cutover' phase from fully-terminal
                                     states)
  fio lifecycle + verify checking .. test_migration_chaos.py   (separates
                                     fatal verify_errors from IO errors that
                                     are expected when a fault was injected)
  fault injection .................. test_migration_chaos.py   (only script
                                     with FaultInjector: spdk_crash / reboot
                                     / nic_down + phase-watcher thread)
  idempotent lvol/snapshot create .. test_shared_namespace.py  (clean,
                                     parameterized, reusable as a function)
  robust teardown .................. test_lvol_migration.py    (polls until
                                     an lvol is actually gone; retries pool
                                     delete until it's actually empty)

This module has no side effects at import time (no log file / directory
creation) — callers configure logging and provide a `log` logger via
`init_logging()` or their own `logging.getLogger(__name__)`.
"""

import glob
import json
import os
import random as _random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

import logging
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SSH defaults (override via module attributes if a caller needs different
# credentials — kept here only because all three scripts hardcoded the same
# values)
# ---------------------------------------------------------------------------
SSH_USER = "root"
SSH_PASS = "3tango11"
DATA_NIC = "eth0"

MIGRATION_TIMEOUT = 600
MIGRATION_POLL    = 5


def init_logging(log_file, level=logging.INFO):
    """Configure root logging to both a file and stdout. Returns the logger."""
    import sys
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(str(log_file)), logging.StreamHandler(sys.stdout)],
        force=True,
    )
    if HAS_PARAMIKO:
        logging.getLogger("paramiko").setLevel(logging.WARNING)
    return logging.getLogger()


# ---------------------------------------------------------------------------
# Colour helpers  (from test_migration_chaos.py)
# ---------------------------------------------------------------------------
USE_COLOR = sys.stdout.isatty()

def _c(code, text): return f"\033[{code}m{text}\033[0m" if USE_COLOR else text
def green(t):  return _c("0;32", t)
def red(t):    return _c("0;31", t)
def yellow(t): return _c("1;33", t)
def cyan(t):   return _c("0;36", t)
def bold(t):   return _c("1",    t)


# ---------------------------------------------------------------------------
# sbctl / JSON helpers  (from test_migration_chaos.py)
# ---------------------------------------------------------------------------
def sbctl(*args, parse_json=False):
    cmd = ["sbctl"] + [str(a) for a in args]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        # stderr is often empty (CLI errors print to stdout) — log both so a
        # failure is actually debuggable instead of showing "rc=1: " with
        # nothing after it.
        detail = proc.stderr.strip()[:200] or proc.stdout.strip()[:200]
        log.warning(f"sbctl {' '.join(str(a) for a in args[:4])} "
                    f"rc={proc.returncode}: {detail}")
    if not parse_json:
        return proc.stdout.strip()
    for i, ch in enumerate(proc.stdout):
        if ch in ("{", "["):
            try:
                return json.loads(proc.stdout[i:])
            except json.JSONDecodeError:
                pass
    return None


def sbctl_list(*args):
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


def get(obj, *keys):
    """Look up any of `keys` in `obj`, trying common casing/alias variants."""
    aliases = {
        "id":   ("id", "ID", "Id", "uuid", "UUID", "Uuid"),
        "uuid": ("uuid", "UUID", "Uuid", "id", "ID", "Id"),
        "name": ("name", "Name", "lvol_name"),
    }
    for key in keys:
        for candidate in aliases.get(key.lower(),
                                      (key, key.upper(), key.capitalize(), key.lower())):
            if candidate in obj:
                return obj[candidate]
    return None


def local_run(cmd, check_rc=False, timeout=300):
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    out, err = proc.stdout.strip(), proc.stderr.strip()
    if check_rc and proc.returncode != 0:
        raise RuntimeError(f"Command failed (rc={proc.returncode}): {cmd}\nstderr: {err}")
    return out, err, proc.returncode


# ---------------------------------------------------------------------------
# SSH to storage nodes  (from test_migration_chaos.py)
# ---------------------------------------------------------------------------
def node_ssh(node_ip, cmd, timeout=60):
    if not HAS_PARAMIKO:
        raise RuntimeError("paramiko not installed — SSH unavailable")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(node_ip, username=SSH_USER, password=SSH_PASS,
                   allow_agent=False, look_for_keys=False, timeout=15)
    try:
        _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode()
        err = stderr.read().decode()
        rc  = stdout.channel.recv_exit_status()
    finally:
        client.close()
    return out.strip(), err.strip(), rc


# ---------------------------------------------------------------------------
# Cluster / pool discovery  (dynamic — no hardcoded cluster UUID)
# ---------------------------------------------------------------------------
def discover_cluster_id():
    proc = subprocess.run(["sbctl", "cluster", "list", "--json"],
                          capture_output=True, text=True)
    for i, ch in enumerate(proc.stdout):
        if ch in ("{", "["):
            try:
                data = json.loads(proc.stdout[i:])
                if isinstance(data, list):
                    data = data[0] if data else {}
                elif isinstance(data, dict):
                    data = (data.get("results") or [{}])[0]
                return get(data, "id") or get(data, "uuid") or ""
            except json.JSONDecodeError:
                pass
    return ""


def get_pool_id(name):
    for p in sbctl_list("pool", "list"):
        if get(p, "name") == name:
            return get(p, "id")
    return None


def ensure_pool(pool_name, cluster_id):
    """Create the pool if missing, otherwise return the existing pool's ID.

    Pass an existing pool name/ID as `pool_name` to reuse it without ever
    touching `cluster_id` — mirrors test_shared_namespace.py's --pool flag,
    which needs no cluster ID at all on the reuse path.
    """
    pool_id = get_pool_id(pool_name)
    if pool_id:
        log.info(f"Pool '{pool_name}' exists: {pool_id}")
        return pool_id
    log.info(f"Creating pool '{pool_name}'...")
    sbctl("storage-pool", "add", pool_name, cluster_id)
    time.sleep(3)
    pool_id = get_pool_id(pool_name)
    if not pool_id:
        raise RuntimeError(f"Pool '{pool_name}' not found after creation")
    log.info(f"Pool created: {pool_id}")
    return pool_id


# ---------------------------------------------------------------------------
# Node / HA topology  (from test_lvol_migration.py)
# ---------------------------------------------------------------------------
def get_online_nodes():
    nodes = sbctl_list("sn", "list")
    online = [n for n in nodes
              if (get(n, "status") or "").lower() in ("online", "active", "online_healthy")]
    return online if online else nodes


def build_node_ip_map(nodes):
    result = {}
    for n in nodes:
        nid = get(n, "id")
        ip = (get(n, "Management IP") or get(n, "mgmt_ip") or
              get(n, "management_ip") or get(n, "ip") or "")
        if nid and ip:
            result[nid] = ip
    return result


def get_node_secondary_id(node_id, nodes):
    keys = ("Secondary node ID", "secondary_node_id", "SecondaryNodeId",
            "secondary_node", "HA Secondary", "ha_secondary")
    for n in nodes:
        if get(n, "id") == node_id:
            for key in keys:
                val = n.get(key)
                if val:
                    return val
    return None


def get_node_tertiary_id(node_id, nodes):
    keys = ("Tertiary node ID", "tertiary_node_id", "TertiaryNodeId",
            "tertiary_node", "HA Tertiary", "ha_tertiary")
    for n in nodes:
        if get(n, "id") == node_id:
            for key in keys:
                val = n.get(key)
                if val:
                    return val
    return None


def pick_source_node(nodes):
    """Return the first online node that has both secondary and tertiary configured."""
    for n in nodes:
        nid = get(n, "id")
        if not nid:
            continue
        sec = get_node_secondary_id(nid, nodes)
        ter = get_node_tertiary_id(nid, nodes)
        if sec and ter:
            log.info(f"Source: {nid}  sec={sec}  ter={ter}")
            return nid
    raise RuntimeError("No online node with both secondary and tertiary configured")


def pick_target_node(source_node_id, nodes, target_arg):
    """target_arg: 'no-overlap' | 'a' | 'b' | 'c' | 'd' | hostname-prefix/UUID.

    a: TGT-prim IS SRC-sec       c: TGT-prim IS SRC-tertiary
    b: SRC-prim IS TGT-sec       d: SRC-prim IS TGT-tertiary
    """
    candidates = [n for n in nodes if get(n, "id") != source_node_id]
    if not candidates:
        raise RuntimeError("No candidate target nodes found")

    secondary_id = get_node_secondary_id(source_node_id, nodes)
    log.info(f"Source secondary: {secondary_id or 'unknown'}")

    if target_arg == "no-overlap":
        tertiary_id = get_node_tertiary_id(source_node_id, nodes)
        ha_ids = {secondary_id, tertiary_id} - {None}
        non_overlap = [n for n in candidates if get(n, "id") not in ha_ids]
        chosen = non_overlap[0] if non_overlap else candidates[0]
        log.info(f"Target (no-overlap): {get(chosen, 'id')}  excluded={ha_ids}")
        return get(chosen, "id")

    if target_arg == "a":
        if secondary_id:
            for n in candidates:
                if get(n, "id") == secondary_id:
                    log.info(f"Target (a, TGT-prim=SRC-sec): {secondary_id}")
                    return secondary_id
        chosen = candidates[0]
        log.info(f"Target (a fallback): {get(chosen, 'id')}")
        return get(chosen, "id")

    if target_arg == "b":
        for n in candidates:
            n_id  = get(n, "id")
            n_sec = get_node_secondary_id(n_id, nodes)
            if n_sec == source_node_id:
                log.info(f"Target (b, SRC-prim=TGT-sec): {n_id}")
                return n_id
        raise RuntimeError(f"b: no node found whose secondary is source ({source_node_id})")

    if target_arg == "c":
        src_tertiary = get_node_tertiary_id(source_node_id, nodes)
        if not src_tertiary:
            raise RuntimeError(f"c: source node {source_node_id} has no tertiary configured")
        for n in candidates:
            if get(n, "id") == src_tertiary:
                log.info(f"Target (c, TGT-prim=SRC-tertiary): {src_tertiary}")
                return src_tertiary
        raise RuntimeError(f"c: source tertiary {src_tertiary} not in candidate list")

    if target_arg == "d":
        for n in candidates:
            n_id  = get(n, "id")
            n_ter = get_node_tertiary_id(n_id, nodes)
            if n_ter == source_node_id:
                log.info(f"Target (d, SRC-prim=TGT-tertiary): {n_id}")
                return n_id
        raise RuntimeError(f"d: no node found whose tertiary is source ({source_node_id})")

    for n in candidates:
        nid      = get(n, "id") or ""
        hostname = (get(n, "hostname") or "").lower()
        if nid == target_arg or hostname.startswith(target_arg.lower()):
            log.info(f"Target (explicit '{target_arg}'): {nid}")
            return nid

    raise RuntimeError(
        f"--target '{target_arg}' did not match any online node.\n"
        f"Available: {[(get(n, 'hostname') or '', get(n, 'id')) for n in candidates]}"
    )


# ---------------------------------------------------------------------------
# Lvol / snapshot lookup + idempotent creation  (from test_shared_namespace.py)
# ---------------------------------------------------------------------------
def get_lvol_by_name(pool_id, name):
    for lv in sbctl_list("lvol", "list", "--pool", pool_id):
        if lv.get("Name") == name or get(lv, "name") == name or get(lv, "lvol_name") == name:
            return lv
    return None


def get_lvol_node(lvol_id):
    raw = sbctl("lvol", "get", lvol_id, "--json") or ""
    for i, ch in enumerate(raw):
        if ch in ("{", "["):
            try:
                d = json.loads(raw[i:])
                if isinstance(d, list):
                    d = d[0] if d else {}
                return get(d or {}, "node_id", "primary_node_id", "Node ID", "host_id")
            except json.JSONDecodeError:
                pass
    return None


def get_snapshot(cluster_id, name):
    uuid_re = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
    raw = sbctl("snapshot", "list", "--cluster-id", cluster_id) or ""
    for line in raw.splitlines():
        if "|" not in line or line.strip().startswith("+"):
            continue
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) < 4:
            continue
        row_id, row_name = parts[0], parts[3]
        if uuid_re.match(row_id) and row_name == name:
            return {"id": row_id, "UUID": row_id, "name": row_name}
    return None


def create_lvol(name, size, pool_id, host_id, crypto=False,
                 namespaced=False, max_ns=None, snapshot=True):
    """Idempotent: returns the existing lvol's ID if `name` is already there."""
    existing = get_lvol_by_name(pool_id, name)
    if existing:
        lvol_id = get(existing, "id")
        log.info(f"  {name}: already exists ({lvol_id})")
        return lvol_id

    add_args = ["volume", "add", name, size, pool_id, "--host-id", host_id]
    if snapshot:
        add_args.append("--snapshot")
    if crypto:
        add_args.append("--encrypt")
    if namespaced:
        add_args += ["--namespaced", "True", "--max-namespace-per-subsys", str(max_ns)]
    elif max_ns is not None:
        # Not joining/opening a shared subsystem, but still pin the cap
        # explicitly: the CLI's own --max-namespace-per-subsys default (32)
        # is applied unconditionally, regardless of --namespaced, so a solo
        # lvol left at the default silently looks "joinable" to
        # get_next_available_subsystem_on_node() and can get hijacked into
        # an unrelated batch group's subsystem. Pass 1 explicitly to keep
        # solo lvols solo until that CLI default is fixed.
        add_args += ["--max-namespace-per-subsys", str(max_ns)]

    sbctl(*add_args)

    for _ in range(15):
        time.sleep(3)
        lv = get_lvol_by_name(pool_id, name)
        if lv:
            lvol_id = get(lv, "id")
            log.info(f"  {name}: created ({lvol_id})")
            return lvol_id
    raise RuntimeError(f"Lvol '{name}' not found after creation")


def create_snapshots(lvol_id, name_prefix, count, interval=2):
    """Create `count` snapshots named f'{name_prefix}{i}', 1-indexed.

    `interval` is the pause (seconds) between each snapshot. Callers running
    background fio against the lvol should pass a larger interval (and start
    fio before calling this) so each snapshot actually captures new data
    instead of back-to-back snapshots of an empty/unwritten volume.
    """
    snap_ids = []
    for i in range(1, count + 1):
        name = f"{name_prefix}{i}"
        sbctl("volume", "create-snapshot", lvol_id, name)
        time.sleep(interval)
        snap_ids.append(name)
    return snap_ids


# ---------------------------------------------------------------------------
# NVMe device / connect helpers
# ---------------------------------------------------------------------------
def parse_nvme_connect_cmds(output):
    cmds = []
    for line in (output or "").splitlines():
        s = line.strip()
        if "nvme connect" not in s:
            continue
        if s.startswith("sudo "):
            s = s[5:]
        cmds.append(s)
    return cmds


def list_nvme_namespaces():
    out, _, _ = local_run("ls /dev/nvme*n* 2>/dev/null || true")
    return sorted(d for d in out.splitlines() if d.strip())


def wait_for_new_device(before, retries=15, interval=3):
    for _ in range(retries):
        after = list_nvme_namespaces()
        new = [d for d in after if d not in before]
        if new:
            log.info(f"  new NVMe device: {new[0]}")
            return new[0]
        time.sleep(interval)
    raise RuntimeError("Timed out waiting for new NVMe device")


def wait_for_new_devices(before, expected_count, retries=20, interval=3):
    """Plural variant (test_shared_namespace.py) — waits for >= expected_count
    new devices instead of just one."""
    for _ in range(retries):
        after = list_nvme_namespaces()
        new = [d for d in after if d not in before]
        if len(new) >= expected_count:
            return new
        time.sleep(interval)
    after = list_nvme_namespaces()
    return [d for d in after if d not in before]


def get_nvme_subsystem_nqn(device):
    dev_name = device.split("/")[-1]
    out, _, _ = local_run(
        f"subsys=$(readlink -f /sys/class/block/{dev_name} 2>/dev/null "
        f"| grep -oP 'nvme-subsys[0-9]+' | head -1 || true); "
        f"[ -n \"$subsys\" ] && "
        f"cat /sys/class/nvme-subsystem/$subsys/subsysnqn 2>/dev/null || true"
    )
    if out.strip():
        return out.strip()
    ctrl = re.sub(r'n\d+$', '', dev_name)
    out2, _, _ = local_run(f"cat /sys/class/nvme/{ctrl}/subsysnqn 2>/dev/null || true")
    return out2.strip()


def log_client_nvme_state(label=""):
    tag = f" [{label}]" if label else ""
    log.info(f"--- NVMe client state{tag} ---")
    out, _, _ = local_run("nvme list-subsys 2>/dev/null || true")
    log.info(f"nvme list-subsys:\n{out}")
    out, _, _ = local_run(
        "for ns in /sys/class/nvme/nvme[0-9]*/nvme[0-9]*n[0-9]*; do "
        "  n=$(basename $ns); "
        "  ana=$(cat $ns/ana_state 2>/dev/null || echo n/a); "
        "  echo \"$n: ana_state=$ana\"; "
        "done 2>/dev/null || true"
    )
    log.info(f"ANA states:\n{out}")
    log.info("--- end NVMe state ---")


def _get_nvme_controllers_for_nqn(nqn):
    out, _, _ = local_run("nvme list-subsys 2>/dev/null || true")
    result = {}
    current_nqn = None
    for line in out.splitlines():
        stripped = line.strip()
        if "NQN=" in stripped:
            m = re.search(r"NQN=(\S+)", stripped)
            current_nqn = m.group(1).rstrip(",") if m else None
            continue
        if current_nqn == nqn and re.match(r"[\\+|`\-]+-?\s+nvme\d", stripped):
            tokens = stripped.split()
            if len(tokens) < 3:
                continue
            ctrl_name = tokens[1]
            addr_block = next((t for t in tokens if "traddr=" in t), "")
            traddr = trsvcid = ""
            for token in addr_block.split(","):
                if token.startswith("traddr="):
                    traddr = token[7:]
                elif token.startswith("trsvcid="):
                    trsvcid = token[8:]
            result[ctrl_name] = {"traddr": traddr, "trsvcid": trsvcid}
    return result


def cleanup_src_controllers(lvol_id, nqn):
    """After a migration cutover, disconnect only the stale SRC controllers
    (i.e. controllers for `nqn` whose endpoint isn't one of the current TGT
    endpoints returned by `volume connect`). Requires ANA-aware inspection —
    only test_lvol_migration.py did this; the other two scripts just ran a
    blanket `nvme disconnect-all`.
    """
    log.info("Cleaning up SRC controllers...")
    connect_out = sbctl("volume", "connect", lvol_id) or ""
    tgt_endpoints = set()
    for cmd in parse_nvme_connect_cmds(connect_out):
        traddr = trsvcid = ""
        for token in cmd.split():
            if token.startswith("--traddr="):
                traddr = token[9:]
            elif token.startswith("--trsvcid="):
                trsvcid = token[10:]
        if traddr and trsvcid:
            tgt_endpoints.add((traddr, trsvcid))
    log.info(f"TGT endpoints: {tgt_endpoints}")

    ctrl_now = _get_nvme_controllers_for_nqn(nqn)
    for ctrl, info in ctrl_now.items():
        ep = (info["traddr"], info["trsvcid"])
        if ep not in tgt_endpoints:
            local_run(f"sudo nvme disconnect -d /dev/{ctrl} 2>&1 || true")
            log.info(f"Disconnected SRC controller {ctrl} ({ep[0]}:{ep[1]})")

    time.sleep(2)
    log_client_nvme_state("post-cutover")


def reconnect_lvol_after_relocation(lvol_id, nqn):
    """Bring an already-connected lvol's NVMe/TCP paths back in line with
    wherever it lives NOW.

    Unlike a manual `lvol migrate` (test_lvol_migration.py's flow), an
    automatic migration -- e.g. the one node removal drives -- gives the
    client no advance notice of the target paths, so there is no proactive
    ANA-inaccessible connect to make beforehand. By the time this runs, the
    lvol's data may already sit in a different lvstore on different
    node(s)/port (same NQN — it's keyed off the lvol UUID — but a different
    subsys_port and host set), and every controller the client holds for
    this NQN is permanently stale: SPDK never pushes new listener addresses
    to an already-connected controller, so the kernel just retries a socket
    nothing answers on, forever ("failed to connect socket: -111" in
    dmesg). Fixing that needs both directions cleanup_src_controllers alone
    doesn't do: connect whatever `volume connect` says the CURRENT live
    endpoints are (in case none of them were ever connected), then drop
    whatever's left over from before.
    Returns True if any new endpoint was connected (a hint the FS/mount may
    need repairing before fio can resume).
    """
    connect_out = sbctl("volume", "connect", lvol_id) or ""
    tgt_endpoints = set()
    cmd_by_ep = {}
    for cmd in parse_nvme_connect_cmds(connect_out):
        traddr = trsvcid = ""
        for token in cmd.split():
            if token.startswith("--traddr="):
                traddr = token[9:]
            elif token.startswith("--trsvcid="):
                trsvcid = token[10:]
        if traddr and trsvcid:
            ep = (traddr, trsvcid)
            tgt_endpoints.add(ep)
            cmd_by_ep[ep] = cmd
    log.info(f"  current TGT endpoints for {lvol_id}: {tgt_endpoints}")

    ctrl_now = _get_nvme_controllers_for_nqn(nqn)
    live_endpoints = {(info["traddr"], info["trsvcid"]) for info in ctrl_now.values()}

    connected_new = False
    for ep in tgt_endpoints - live_endpoints:
        local_run(f"sudo {cmd_by_ep[ep]} 2>&1 || true")
        log.info(f"  connected new path {ep[0]}:{ep[1]}")
        connected_new = True

    for ctrl, info in ctrl_now.items():
        ep = (info["traddr"], info["trsvcid"])
        if ep not in tgt_endpoints:
            local_run(f"sudo nvme disconnect -d /dev/{ctrl} 2>&1 || true")
            log.info(f"  disconnected stale controller {ctrl} ({ep[0]}:{ep[1]})")

    time.sleep(3)
    log_client_nvme_state("post-relocation-reconnect")
    return connected_new


# ---------------------------------------------------------------------------
# Migration helpers
# ---------------------------------------------------------------------------
def parse_migration_id(output):
    m = re.search(r'Migration ID:\s*([0-9a-f-]{36})', output or "", re.IGNORECASE)
    return m.group(1) if m else None


def get_migration_record(lvol_id, cluster_id):
    migrations = sbctl("--dev", "lvol", "migrate-list",
                       "--cluster-id", cluster_id, "--json", parse_json=True)
    if isinstance(migrations, dict):
        migrations = migrations.get("results", [])
    for m in (migrations or []):
        mid = (get(m, "lvol_id") or get(m, "volume_id") or
               get(m, "Lvol ID") or get(m, "Volume ID") or "")
        if mid == lvol_id:
            return m
    return None


def _log_migration_progress(m, prefix="  "):
    """Log status/phase/snaps plus the Error/Retries fields migrate-list
    reports — e.g. 'Failed to attach migration hub controller to <node>'
    with retries 5/10 while status sits in SUSPENDED. Without this, a stuck
    migration just produces an opaque wall of 'status=suspended' lines with
    no clue why, forcing a manual `lvol migrate-list` to diagnose.
    """
    status  = (get(m, "status") or "unknown").lower()
    phase   = (get(m, "phase") or "").lower()
    snaps   = get(m, "Snaps") or get(m, "snaps") or ""
    retries = get(m, "Retries") or get(m, "retries") or ""
    error   = get(m, "Error") or get(m, "error_message") or get(m, "error") or ""
    line = f"{prefix}status={status}  phase={phase}  snaps={snaps}  retries={retries}"
    if error:
        log.warning(f"{line}  error={error!r}")
    else:
        log.info(line)
    return status, phase


def wait_for_migration(lvol_id, cluster_id, terminal_only=False,
                       timeout=MIGRATION_TIMEOUT, poll=MIGRATION_POLL):
    """Cutover-aware polling (test_lvol_migration.py): returns 'cutover' as
    soon as that phase is observed unless `terminal_only` is set, matching
    the real two-phase migration protocol (cutover -> ANA flip -> done).
    """
    log.info(f"Polling migration for lvol {lvol_id}"
             + (" (terminal only)" if terminal_only else "") + "...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = get_migration_record(lvol_id, cluster_id)
        if not m:
            log.info("  migration record gone — treating as done")
            return "done"
        status, _ = _log_migration_progress(m)
        if status in ("done", "completed", "failed", "error", "cancelled"):
            return status
        if status == "cutover" and not terminal_only:
            return "cutover"
        time.sleep(poll)
    return "timeout"


def cancel_migration(migration_id, batch=False):
    """`lvol migrate-cancel` — forces an active migration straight into
    PHASE_CLEANUP_TARGET (same rollback path a real failure takes), ending
    in STATUS_CANCELLED. Deterministic alternative to racing a --deadline.

    Logs and returns the CLI's actual output so a silent failure (or a
    cancel that the runner accepted but then lost — see the migration
    state-machine's canceled-flag race) is visible instead of a blind
    "issued" log line that says nothing about whether it worked.
    """
    args = ["--dev", "lvol", "migrate-cancel", migration_id]
    if batch:
        args.append("--batch")
    out = sbctl(*args)
    log.info(f"migrate-cancel output: {out!r}")
    if "cancelled" not in (out or "").lower():
        log.warning(f"migrate-cancel for {migration_id} did not report success "
                    f"(output above) — migration may keep running")
    return out


def cleanup_migration_target(migration_id):
    """`lvol migrate-cleanup` — idempotently remove anything a migration
    created on the target. Safe to call any time, including after the
    normal CLEANUP_TARGET rollback already ran (already-removed objects are
    reported as not_found).
    """
    return sbctl("--dev", "lvol", "migrate-cleanup", migration_id)


def get_migration_snap_progress(lvol_id, cluster_id):
    """Return (copied, total) ints parsed from the migration's 'Snaps'
    field (e.g. '5/10'), or (None, None) if there's no active migration
    record or the field can't be parsed.
    """
    m = get_migration_record(lvol_id, cluster_id)
    if not m:
        return None, None
    snaps = get(m, "Snaps") or get(m, "snaps") or ""
    match = re.match(r'(\d+)\s*/\s*(\d+)', str(snaps))
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def wait_for_snap_count(lvol_id, cluster_id, target_count, timeout=300, poll=2):
    """Block until at least `target_count` snapshots have been copied to
    the target during PHASE_SNAP_COPY. Returns True once reached; returns
    False if the migration goes terminal or leaves snap_copy before ever
    reaching `target_count`; raises TimeoutError if neither happens within
    `timeout`.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = get_migration_record(lvol_id, cluster_id)
        if not m:
            return False
        status, phase = _log_migration_progress(m, prefix="  snap wait: ")
        if status in ("done", "completed", "failed", "cancelled", "error"):
            return False
        copied, total = get_migration_snap_progress(lvol_id, cluster_id)
        if copied is not None and copied >= target_count:
            return True
        if phase and phase != "snap_copy" and copied is None:
            return False
        time.sleep(poll)
    raise TimeoutError(
        f"Timed out waiting for {target_count} snapshots to copy for lvol {lvol_id}")


def wait_node_status(node_id, expected_statuses=("online", "active", "online_healthy"),
                     timeout=300, poll=10):
    """Poll `sn list` until `node_id` reports one of `expected_statuses`."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for n in sbctl_list("sn", "list"):
            if get(n, "id") == node_id:
                status = (get(n, "status") or "").lower()
                if status in expected_statuses:
                    return True
                log.info(f"  node {node_id}: status={status}")
                break
        time.sleep(poll)
    return False


def wait_all_nodes_status(node_ids, expected_statuses=("online",), timeout=300, poll=10):
    """Poll `sn list` until every ID in `node_ids` reports one of
    `expected_statuses`. Generalizes wait_node_status() to a whole batch
    (e.g. every node after a full-cluster restart) instead of just one.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = {get(n, "id"): (get(n, "status") or "").lower()
                  for n in sbctl_list("sn", "list")}
        pending = {nid: current.get(nid, "?") for nid in node_ids
                  if current.get(nid) not in expected_statuses}
        if not pending:
            return True
        log.info(f"  waiting on nodes: {pending}")
        time.sleep(poll)
    return False


def wait_cluster_healthy(timeout=300, poll=10):
    """Return True once every storage node in `sn list` reports online/
    healthy (from test_migration_chaos.py) — re-fetches the node list fresh
    on every poll, unlike wait_all_nodes_status() which checks a fixed set
    of IDs. Used as the post-failure health gate between chaos iterations.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            nodes = sbctl_list("sn", "list")
            if nodes:
                healthy_statuses = {"online", "active", "online_healthy"}
                if all((get(n, "status") or "").lower() in healthy_statuses for n in nodes):
                    return True
                unhealthy = [(get(n, "hostname") or get(n, "id") or "?")
                            for n in nodes
                            if (get(n, "status") or "").lower() not in healthy_statuses]
                log.info(f"  Waiting for nodes: {unhealthy}")
        except Exception as e:
            log.debug(f"  wait_cluster_healthy poll error: {e}")
        time.sleep(poll)
    return False


# ---------------------------------------------------------------------------
# Cluster lifecycle: shutdown / restart / activate
#
# `sn shutdown` on every node suspends the cluster; `sn restart` brings nodes
# back; `cluster activate` reconciles/rebalances and flips the cluster back
# to ACTIVE. cluster_activate() requires nodes to already be ONLINE (it
# filters to online_nodes and errors on too few), so callers must wait for
# restarted nodes before calling activate_cluster().
# ---------------------------------------------------------------------------
def shutdown_node(node_id, force=False):
    args = ["sn", "shutdown", node_id]
    if force:
        args.append("--force")
    return sbctl(*args)


def restart_node(node_id, force=False):
    args = ["sn", "restart", node_id]
    if force:
        args.append("--force")
    return sbctl(*args)


def activate_cluster(cluster_id, force=False, force_lvstore_create=False):
    args = ["cluster", "activate", cluster_id]
    if force:
        args.append("--force")
    if force_lvstore_create:
        args.append("--force-lvstore-create")
    return sbctl(*args)


# ---------------------------------------------------------------------------
# Docker swarm service env vars — TEST ONLY.
#
# node_removal_orchestrate() (and everything it calls, including
# _pick_drain_target's SB_TEST_PREFER_TARGET_WHOSE_SECONDARY_IS_DEPARTING
# override) runs inside the app_TasksNodeRemovalRunner service, a persistent
# background container — NOT inside the `sbctl` CLI process that issues
# `storage-node remove` (that call just creates a JobSchedule and returns).
# Setting a test env var on this host's shell/process does nothing for it;
# it has to land in that service's own container environment, which means
# `docker service update --env-add` (this rolls the service — safe here
# since node_removal_orchestrate is explicitly documented as idempotent and
# resumable, so a restart of an otherwise-idle runner has nothing to lose).
# ---------------------------------------------------------------------------
def set_docker_service_env(service, key, value, wait=10):
    """Add/replace one env var on a swarm service. Rolls the service."""
    out, err, rc = local_run(f"sudo docker service update --env-add {key}={value} {service} 2>&1")
    log.info(f"  docker service update --env-add {key}={value} {service}: rc={rc}")
    time.sleep(wait)
    return rc == 0


def unset_docker_service_env(service, key, wait=10):
    """Remove one env var from a swarm service (no-op if never added). Rolls the service."""
    out, err, rc = local_run(f"sudo docker service update --env-rm {key} {service} 2>&1")
    log.info(f"  docker service update --env-rm {key} {service}: rc={rc}")
    time.sleep(wait)
    return rc == 0


def wait_service_running(service, timeout=120, poll=5):
    """Poll `docker service ps` until *service* has a task in Running state."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out, _, _ = local_run(
            f"sudo docker service ps {service} --filter desired-state=running "
            f"--format '{{{{.CurrentState}}}}' 2>&1")
        if any(line.strip().lower().startswith("running") for line in out.splitlines()):
            return True
        time.sleep(poll)
    return False


def get_cluster(cluster_id):
    # `cluster get` has no --json flag (unlike `cluster list`) — it always
    # dumps JSON on its own; passing --json is an argparse usage error.
    data = sbctl("cluster", "get", cluster_id, parse_json=True)
    if isinstance(data, list):
        data = data[0] if data else {}
    return data or {}


def get_cluster_status(cluster_id):
    return (get(get_cluster(cluster_id), "status") or "").lower()


def wait_cluster_status(cluster_id, expected_statuses, timeout=300, poll=10):
    """Poll `cluster get` until its status is one of `expected_statuses`
    (pass a single string or an iterable of strings).
    """
    if isinstance(expected_statuses, str):
        expected_statuses = (expected_statuses,)
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = get_cluster_status(cluster_id)
        if status in expected_statuses:
            return True
        log.info(f"  cluster {cluster_id}: status={status}")
        time.sleep(poll)
    return False


# ---------------------------------------------------------------------------
# Pass/fail tracking + step banners
#
# Every one of the three source scripts hand-rolled its own version of this
# (check()/step() in test_lvol_migration.py and test_migration_chaos.py,
# _passes/_failures globals in both). Extracted as one shared, reusable
# helper so new scripts don't repeat it.
# ---------------------------------------------------------------------------
class Checklist:
    def __init__(self):
        self.passes = []
        self.failures = []

    def step(self, msg):
        log.info("")
        log.info("=" * 60)
        log.info(f"STEP: {msg}")
        log.info("=" * 60)

    def check(self, passed, msg):
        if passed:
            log.info(f"PASS: {msg}")
            self.passes.append(msg)
        else:
            log.error(f"FAIL: {msg}")
            self.failures.append(msg)
        return passed

    def summary(self):
        log.info("")
        log.info("=" * 60)
        log.info("SUMMARY")
        log.info("=" * 60)
        for msg in self.passes:
            log.info(f"  PASS : {msg}")
        for msg in self.failures:
            log.error(f"  FAIL : {msg}")
        log.info(f"  Result: {len(self.passes)} passed, {len(self.failures)} failed")
        return len(self.failures) == 0


# ---------------------------------------------------------------------------
# fio lifecycle  (from test_migration_chaos.py)
# ---------------------------------------------------------------------------
def fio_prefill_cmd(fio_file, fio_log, size="2G", offset=None):
    cmd = [
        "sudo", "fio", "--name=job1", f"--filename={fio_file}",
        f"--size={size}",
    ]
    if offset:
        cmd.append(f"--offset={offset}")
    cmd += [
        "--numjobs=1", "--direct=1", "--ioengine=libaio",
        "--iodepth=8", "--rw=write", "--bs=128k",
        "--verify=md5", "--do_verify=0", "--end_fsync=1",
        f"--output={fio_log}",
    ]
    return cmd


def fio_randrw_cmd(fio_file, fio_log, size="2G", runtime=7200, offset=None,
                   iodepth=1, numjobs=1, bs=None):
    """``bs`` overrides the default ``--bsrange=4k:128k`` with a fixed
    ``--bs=`` when given -- pass a larger fixed size along with higher
    iodepth/numjobs to hammer write throughput (e.g. to force a migration's
    dirty-delta threshold above its intermediate-snapshot round trigger)."""
    cmd = [
        "sudo", "fio", "--name=job1", f"--filename={fio_file}",
        f"--size={size}",
    ]
    if offset:
        cmd.append(f"--offset={offset}")
    cmd += [
        f"--numjobs={numjobs}", "--direct=1", "--ioengine=libaio",
        f"--iodepth={iodepth}", "--verify=md5", "--readwrite=randrw",
        f"--bs={bs}" if bs else "--bsrange=4k:128k",
        "--time_based", f"--runtime={runtime}", "--status-interval=10",
        f"--output={fio_log}",
    ]
    return cmd


def fio_prefill(fio_file, fio_log, size="2G", offset=None):
    log.info(f"  Pre-filling {fio_file} ({size}) ...")
    proc = subprocess.run(fio_prefill_cmd(fio_file, fio_log, size, offset=offset),
                          capture_output=True, text=True, timeout=3600)
    if proc.returncode != 0:
        log.warning(f"  fio prefill rc={proc.returncode}: {proc.stderr.strip()[:300]}")
    return proc.returncode == 0


def start_fio_bg(fio_file, fio_log, size="2G", runtime=7200, offset=None,
                 iodepth=1, numjobs=1, bs=None):
    proc = subprocess.Popen(
        fio_randrw_cmd(fio_file, fio_log, size, runtime, offset=offset,
                       iodepth=iodepth, numjobs=numjobs, bs=bs),
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True,
    )
    time.sleep(2)
    if proc.poll() is not None:
        _, err = proc.communicate()
        log.warning(f"  fio exited immediately (rc={proc.returncode}): "
                    f"{(err or b'').decode()[:300]}")
        return None
    log.info(f"  fio bg started (pid={proc.pid})  log: {fio_log}")
    return proc


def stop_fio(proc, post_wait=30):
    if not proc:
        return
    log.info(f"  Waiting {post_wait}s post-migration before stopping fio...")
    time.sleep(post_wait)
    if proc.poll() is not None:
        log.warning(f"  fio already exited (rc={proc.returncode})")
        return
    try:
        os.killpg(proc.pid, signal.SIGINT)
    except ProcessLookupError:
        pass
    try:
        _, stderr = proc.communicate(timeout=30)
        if stderr:
            log.debug(f"  fio stderr: {stderr.decode()[:400]}")
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, 9)
        except ProcessLookupError:
            pass
        proc.wait()


def check_fio_output(fio_log, fault_injected=False):
    """Return (no_corruption, verify_error_count).

    verify_errors are always fatal. General IO errors (err=N) are only
    counted as failures when no fault was injected — a node crash mid-IO is
    expected to produce transient IO errors without corrupting data.
    """
    try:
        content = Path(fio_log).read_text()
    except FileNotFoundError:
        log.warning(f"  fio log not found: {fio_log}")
        return True, 0

    log.info(f"--- fio output (last 2000 chars) ---\n{content[-2000:]}\n---")

    verify_errs = sum(int(m.group(1)) for m in
                       re.finditer(r'verify_errors\s*[:=]\s*(\d+)', content, re.I))
    io_errs = sum(int(m.group(1)) for m in re.finditer(r'\berr\s*=\s*(\d+)', content))

    if verify_errs:
        log.error(f"  fio: {verify_errs} VERIFY ERROR(S) — data corruption!")
    elif io_errs and not fault_injected:
        log.error(f"  fio: {io_errs} IO error(s) (no fault was active)")
    elif io_errs:
        log.warning(f"  fio: {io_errs} IO error(s) (fault was injected — expected)")
    else:
        log.info("  fio: OK (0 errors)")

    no_corruption = (verify_errs == 0) and (io_errs == 0 or fault_injected)
    return no_corruption, verify_errs


# ---------------------------------------------------------------------------
# Fault injection  (from test_migration_chaos.py)
# ---------------------------------------------------------------------------
PHASE_KEYWORDS = {
    "snap_copy":      ["snap"],
    "lvol_migrate":   ["migrate", "lvol_migrate", "bulk"],
    "cleanup_source": ["cleanup"],
}


class FaultInjector:
    """Waits for a target migration phase then fires a node fault."""

    def __init__(self, fault_type, phase, node_role, src_id, tgt_id, nodes,
                 cluster_id, node_ip_map, nic_down_seconds=45, phase_watch_timeout=120,
                 phase_watch_poll=1.0):
        self.fault_type = fault_type
        self.phase      = phase
        self.node_role  = node_role   # "source" | "target" | "both"
        self.src_id     = src_id
        self.tgt_id     = tgt_id
        self.nodes      = nodes
        self.cluster_id = cluster_id
        self.node_ip_map = node_ip_map
        self.nic_down_seconds    = nic_down_seconds
        self.phase_watch_timeout = phase_watch_timeout
        self.phase_watch_poll    = phase_watch_poll
        self.fired      = threading.Event()
        self._thread    = None

    def _node_ip(self, node_id):
        ip = self.node_ip_map.get(node_id, "")
        if not ip:
            for n in self.nodes:
                if get(n, "id") == node_id:
                    ip = (get(n, "Management IP") or get(n, "mgmt_ip") or "")
                    break
        return ip

    def _fire_node(self, node_id, role):
        ip = self._node_ip(node_id)
        if not ip:
            log.warning(f"[fault] no IP for {role} {node_id} — skipping")
            return
        log.info(f"[fault] >>> {self.fault_type} on {role} ({ip}) <<<")
        try:
            if self.fault_type == "spdk_crash":
                rpc_port = 4420
                for n in self.nodes:
                    if get(n, "id") == node_id:
                        rpc_port = int(get(n, "rpc_port") or 4420)
                        break
                cmd = (f"curl -sS 'http://0.0.0.0:5000/snode/spdk_process_kill"
                       f"?rpc_port={rpc_port}&cluster_id={self.cluster_id}'")
                out, _, rc = node_ssh(ip, cmd, timeout=30)
                log.info(f"[fault] spdk_crash rc={rc}: {out[:120]}")

            elif self.fault_type == "reboot":
                try:
                    node_ssh(ip, "reboot", timeout=15)
                except Exception as e:
                    log.info(f"[fault] reboot SSH dropped (node rebooting): {e}")
                log.info(f"[fault] reboot sent for {role}")

            elif self.fault_type == "nic_down":
                cmd = (
                    f"nohup sh -c 'ip link set {DATA_NIC} down"
                    f" && sleep {self.nic_down_seconds}"
                    f" && ip link set {DATA_NIC} up' &"
                )
                try:
                    node_ssh(ip, cmd, timeout=15)
                except Exception as e:
                    log.info(f"[fault] nic_down SSH dropped (NIC is down): {e}")
                log.info(f"[fault] nic_down {role}: {DATA_NIC} down for {self.nic_down_seconds}s")

        except Exception as e:
            log.error(f"[fault] {role} fire error: {e}")

    def fire(self):
        if self.fired.is_set():
            return
        self.fired.set()
        if self.node_role in ("source", "both"):
            self._fire_node(self.src_id, "source")
        if self.node_role in ("target", "both"):
            self._fire_node(self.tgt_id, "target")
        if self.fault_type == "nic_down":
            log.info(f"[fault] waiting {self.nic_down_seconds}s for auto-restore...")
            time.sleep(self.nic_down_seconds + 5)

    def _watch_loop(self, lvol_id, batch_group_id=None):
        keywords = PHASE_KEYWORDS.get(self.phase, ["snap"])
        log.info(f"[fault watcher] waiting for phase {self.phase!r} (keywords={keywords!r})"
                 + (f"  batch_group={batch_group_id[:8]}" if batch_group_id else ""))
        deadline = time.time() + self.phase_watch_timeout
        while time.time() < deadline and not self.fired.is_set():
            if batch_group_id:
                rec = get_batch_record(batch_group_id, self.cluster_id)
            else:
                rec = get_migration_record(lvol_id, self.cluster_id)
            if not rec:
                break
            status = (get(rec, "status") or "").lower()
            phase  = (get(rec, "phase")  or "").lower()
            if status in ("done", "completed", "failed", "cancelled", "error"):
                break
            if any(k in phase or k in status for k in keywords):
                log.info(f"[fault watcher] phase match status={status!r} phase={phase!r} → firing")
                self.fire()
                return
            time.sleep(self.phase_watch_poll)
        if not self.fired.is_set():
            log.warning(f"[fault watcher] phase {self.phase!r} never seen — firing anyway")
            self.fire()

    def start_watching(self, lvol_id, batch_group_id=None):
        self._thread = threading.Thread(
            target=self._watch_loop, args=(lvol_id,),
            kwargs={"batch_group_id": batch_group_id}, daemon=True)
        self._thread.start()

    def wait(self, timeout=105):
        if self._thread:
            self._thread.join(timeout=timeout)


# ---------------------------------------------------------------------------
# Robust cleanup / teardown  (from test_lvol_migration.py)
# ---------------------------------------------------------------------------
def wait_lvol_deleted(lvol_id, cluster_id, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        lvols = sbctl("volume", "list", "--cluster-id", cluster_id,
                      "--json", parse_json=True)
        if isinstance(lvols, dict):
            lvols = lvols.get("results", [])
        matching = [lv for lv in (lvols or []) if get(lv, "id") == lvol_id]
        if not matching:
            return True
        status = (get(matching[0], "status") or "").lower()
        if status not in ("online", "in_deletion", "deleting"):
            return True
        time.sleep(3)
    sbctl("volume", "delete", lvol_id, "--force")
    time.sleep(5)
    return False


def cleanup_pool_and_lvols(pool_name, cluster_id, lvol_names=None, snap_names=None, mount=None):
    """Delete named lvols (polling until actually gone), their snapshots,
    then retry `storage-pool delete` until it succeeds (handles the "pool
    not empty" race right after lvol deletion).
    """
    if mount:
        local_run(f"sudo umount {mount} 2>/dev/null || true")
        local_run(f"sudo rm -rf {mount} 2>/dev/null || true")

    for snap_name in (snap_names or []):
        snap = get_snapshot(cluster_id, snap_name)
        if snap:
            sbctl("snapshot", "delete", get(snap, "id"), "--force")
            time.sleep(2)

    pool_id = get_pool_id(pool_name)
    if pool_id:
        raw = sbctl("volume", "list", "--pool", pool_id, "--json", parse_json=True)
        if isinstance(raw, dict):
            raw = raw.get("results", [])
        for lv in (raw or []):
            if not lvol_names or get(lv, "name") in lvol_names:
                lvol_id = get(lv, "id")
                if (get(lv, "status") or "").lower() != "in_deletion":
                    sbctl("volume", "delete", lvol_id, "--force")
                wait_lvol_deleted(lvol_id, cluster_id)

        for _ in range(10):
            out = sbctl("storage-pool", "delete", pool_id) or ""
            if "not empty" not in out.lower() and "lvols found" not in out.lower():
                break
            time.sleep(5)
        time.sleep(2)


# ---------------------------------------------------------------------------
# Failure log collection  (from test_migration_chaos.py)
# ---------------------------------------------------------------------------
COLLECT_LOGS_PY = (
    "/usr/local/lib/python3.9/site-packages/"
    "simplyblock_core/scripts/collect_logs.py"
)
COLLECT_WINDOW = 15  # minutes of logs to capture around a failure


def snapshot_cluster_state(dest_dir, collect_window=COLLECT_WINDOW,
                           collect_logs_py=COLLECT_LOGS_PY):
    """Dump cluster/node/migration/volume state plus dmesg to `dest_dir`,
    and — if collect_logs.py is present on this host — pull a windowed
    cluster log bundle too. Call this right after detecting an iteration
    failure, while the evidence is still fresh.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    cmds = [
        ("cluster_list.txt", ["sbctl", "cluster", "list"]),
        ("sn_list.txt",      ["sbctl", "sn", "list"]),
        ("migrate_list.txt", ["sbctl", "--dev", "lvol", "migrate-list"]),
        ("volume_list.txt",  ["sbctl", "lvol", "list"]),
        ("dmesg.txt",        ["sudo", "dmesg", "-T"]),
    ]
    for fname, cmd in cmds:
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout
            (dest_dir / fname).write_text(out)
        except Exception as e:
            (dest_dir / fname).write_text(f"ERROR: {e}\n")

    if os.path.exists(collect_logs_py):
        start_dt = (datetime.now() - timedelta(minutes=collect_window)).strftime(
            "%Y-%m-%d %H:%M:%S")
        log.info(f"  Running collect_logs.py (window={collect_window}m)...")
        try:
            subprocess.run(
                ["python3", collect_logs_py, start_dt, str(collect_window),
                 "--use-opensearch"],
                timeout=300,
            )
        except Exception as e:
            log.warning(f"  collect_logs.py: {e}")
        for tb in glob.glob("/root/sb_logs_*.tar.gz"):
            try:
                shutil.move(tb, str(dest_dir / Path(tb).name))
                log.info(f"  Tarball -> {dest_dir / Path(tb).name}")
            except Exception as e:
                log.warning(f"  Could not move {tb}: {e}")

    log.info(f"  Cluster state saved -> {dest_dir}")


# ---------------------------------------------------------------------------
# High-level connect/mount/migrate workflow helpers
#
# These wrap the lower-level primitives above into the same multi-step
# sequences every one of the three source scripts repeated by hand (connect
# -> run local nvme-connect commands -> wait for the device -> mkfs/mount;
# migrate -> connect TGT paths -> migrate-continue). Extracted so new test
# scripts can stay pure iteration/orchestration logic.
# ---------------------------------------------------------------------------
def pick_random_target_node(current_node_id, node_ids, rng=None):
    """Pick a random node ID from `node_ids` that isn't `current_node_id`."""
    rng = rng or _random
    others = [n for n in node_ids if n != current_node_id]
    if not others:
        raise RuntimeError(f"No target node available other than {current_node_id}")
    return rng.choice(others)


def connect_lvol(lvol_id):
    """`volume connect` + run the returned nvme-connect command(s) locally.

    Returns the newly-appeared device path (e.g. '/dev/nvme3n1').
    """
    before = list_nvme_namespaces()
    connect_out = sbctl("volume", "connect", lvol_id)
    cmds = parse_nvme_connect_cmds(connect_out)
    if not cmds:
        raise RuntimeError(f"No nvme connect commands returned for lvol {lvol_id}")
    for cmd in cmds:
        local_run(f"sudo {cmd} 2>&1 || true")
    return wait_for_new_device(before)


def mount_sources_at(mount_point):
    """Devices mounted at `mount_point`, bottom-most first. More than one means
    stale mounts are stacked there; only the last one is visible to the path."""
    out, _, rc = local_run(f"findmnt -rn -o SOURCE --mountpoint {mount_point}")
    return out.split() if rc == 0 else []


def wait_mount_idle(mount_point, timeout=60):
    """Wait until no process has files open under `mount_point`, then kill any
    stragglers. fio's job children can outlive the parent we stopped, and a
    process still holding a shut-down XFS keeps its superblock alive."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, _, rc = local_run(f"sudo fuser -m {mount_point} >/dev/null 2>&1")
        if rc != 0:  # fuser exits non-zero when nothing uses the mount
            return
        time.sleep(2)
    log.warning(f"  {mount_point} still busy after {timeout}s; killing its users")
    local_run(f"sudo fuser -km {mount_point} >/dev/null 2>&1 || true")
    time.sleep(2)


def unmount_all_at(mount_point, max_layers=16, lazy=True, busy_timeout=90):
    """Unmount every layer stacked at `mount_point`. Raises if a layer won't go.

    lazy=True detaches even if busy -- right for dead mounts left by a previous
    cluster. lazy=False is for a mount about to be reused: a lazy detach of a
    busy shut-down XFS leaves its superblock alive, and remounting the same
    device then silently re-attaches that dead filesystem. A non-lazy umount is
    retried for up to `busy_timeout` s while "target is busy": after a path
    reconnect the kernel still holds requeued multipath I/O that must drain
    through the new path before the filesystem can be released."""
    for _ in range(max_layers):
        sources = mount_sources_at(mount_point)
        if not sources:
            return
        deadline = time.time() + (0 if lazy else busy_timeout)
        while True:
            _, err, rc = local_run(f"sudo umount {'-l ' if lazy else ''}{mount_point}")
            if rc == 0 or "busy" not in err or time.time() >= deadline:
                break
            time.sleep(3)
        if rc != 0:
            raise RuntimeError(f"umount {mount_point} ({sources[-1]}) failed: {err}")
    raise RuntimeError(f"{mount_point} still mounted after {max_layers} unmounts: "
                       f"{mount_sources_at(mount_point)}")


def verify_mount_usable(mount_point):
    """Raise unless the filesystem at `mount_point` answers stat and a direct write."""
    probe = f"{mount_point}/.mount_probe"
    _, err, rc = local_run(
        f"sudo stat {mount_point} >/dev/null && "
        f"sudo dd if=/dev/zero of={probe} bs=4k count=1 oflag=direct,sync 2>&1 && sudo rm -f {probe}")
    if rc != 0:
        raise RuntimeError(f"{mount_point} mounted but not usable: {err or 'I/O error'}")


def format_and_mount(device, mount_point, already_formatted=False):
    """mkfs.xfs + mount a fresh device, or plain-mount (with an xfs_repair
    fallback, chaos.py-style) a device that was already formatted in a
    previous pass over the same lvol.
    """
    local_run(f"sudo mkdir -p {mount_point}")
    if not already_formatted:
        local_run(f"sudo mkfs.xfs -f {device}", check_rc=True)
        local_run(f"sudo mount {device} {mount_point}", check_rc=True)
        return
    _, _, rc = local_run(f"sudo mount -t xfs {device} {mount_point} 2>&1")
    if rc != 0:
        log.warning(f"  mount failed (rc={rc}) — running xfs_repair")
        local_run(f"sudo xfs_repair -L {device} 2>&1 || true")
        local_run(f"sudo mount -t xfs {device} {mount_point}", check_rc=True)


def connect_and_mount_lvol(lvol_id, mount_point, already_formatted=False):
    """connect_lvol() + format_and_mount() in one call. Returns the device path."""
    device = connect_lvol(lvol_id)
    format_and_mount(device, mount_point, already_formatted=already_formatted)
    return device


def _nvme_devs_for_nqn(nqn):
    """Return {nsid: /dev/nvmeXnY} for every device belonging to the given NQN,
    keyed by the real NVMe namespace id -- not by device path or connection
    order.

    Simplyblock sets the NVMe controller Model Number to the subsystem UUID
    (the '<uuid>' part of '...:lvol:<uuid>' in the NQN).  `nvme list` exposes
    this in its Model column, making a simple substring match reliable and
    sysfs-independent (the sysfs path may not exist for auto-reconnected
    subsystems). `nvme list`'s Namespace column (the token right after the
    Model) is the nsid; keying by it (instead of just sorting device paths)
    matters because a client that connects to a *shared* (batch) subsystem
    sees every namespace on it, not just the one it means to use, so which
    device is "member N" can only be resolved by nsid, never by position.
    """
    m = re.search(r':lvol:([0-9a-f-]{36})$', nqn, re.IGNORECASE)
    if not m:
        log.warning(f"  _nvme_devs_for_nqn: cannot parse subsystem UUID from {nqn!r}")
        return {}
    subsys_uuid = m.group(1).lower()
    out, _, _ = local_run("nvme list 2>/dev/null || true")
    devs = {}
    for line in out.splitlines():
        if not line.startswith('/dev/nvme'):
            continue
        parts = line.split()
        if not parts or not re.match(r'/dev/nvme\d+n\d+$', parts[0]):
            continue
        match_idx = next((i for i, p in enumerate(parts) if subsys_uuid in p.lower()), None)
        if match_idx is None or match_idx + 1 >= len(parts):
            continue
        # Namespace immediately follows Model
        # (Node, Generic, SN, Model, Namespace, Usage, Format, FW Rev) and is
        # printed in hex (e.g. "0x1"), not decimal -- int(x, 0) handles both
        # that and a plain decimal value, in case an older nvme-cli omits
        # the 0x prefix.
        try:
            nsid = int(parts[match_idx + 1], 0)
        except ValueError:
            log.warning(f"  _nvme_devs_for_nqn: matched {parts[0]} on subsys "
                        f"{subsys_uuid} but could not parse nsid from {parts[match_idx + 1]!r}: "
                        f"{line!r}")
            continue
        devs[nsid] = parts[0]
    if not devs and any(subsys_uuid in ln.lower() for ln in out.splitlines()):
        log.warning(f"  _nvme_devs_for_nqn: subsystem {subsys_uuid} appears in "
                    f"`nvme list` output but no device could be parsed:\n{out}")
    return devs


def _lvol_ns_id(lvol_id):
    """The real NVMe namespace id of a (possibly shared-subsystem) lvol."""
    raw = sbctl("lvol", "get", lvol_id, "--json") or ""
    for i, ch in enumerate(raw):
        if ch in ("{", "["):
            try:
                d = json.loads(raw[i:])
                if isinstance(d, list):
                    d = d[0] if d else {}
                v = get(d or {}, "ns_id", "NS ID", "nsid")
                return int(v) if v is not None else None
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
    return None


def connect_and_mount_batch_members(members, already_formatted=False):
    """Connect, format, and mount all members of a shared-subsystem batch group.

    Namespaced lvols share one NQN.  The cluster may auto-reconnect the
    subsystem after disconnect-all, so "already connected" is a normal outcome
    — not an error.  Rather than relying on a before/after device-list delta
    (which misses pre-existing connections), we look up devices by NQN via
    sysfs after the connect step.

    Each member is matched to its device by real namespace id (never by
    position): a client connecting to a shared subsystem sees every member's
    namespace, not just the ones in `members`, so a positional slice of the
    device list silently binds unrelated members to the same (wrong) device
    whenever `members` is a subset that doesn't start at the group's first
    namespace -- e.g. one member per client, round-robined across several
    clients, which is exactly how multi-client batch tests call this.
    Returns members with 'device' added.
    """
    nqn = None
    for member in members:
        connect_out = sbctl("volume", "connect", member["id"])
        for cmd in parse_nvme_connect_cmds(connect_out):
            if nqn is None:
                m = re.search(r'--nqn=(\S+)', cmd)
                if m:
                    nqn = m.group(1)
            local_run(f"sudo {cmd} 2>&1 || true")

    if not nqn:
        raise RuntimeError(
            f"Could not extract NQN from volume connect output for "
            f"{[m['name'] for m in members]}")

    time.sleep(3)

    devs_by_nsid = _nvme_devs_for_nqn(nqn)
    log.info(f"  batch NQN devs ({len(devs_by_nsid)}): {devs_by_nsid}")

    for member in members:
        ns_id = _lvol_ns_id(member["id"])
        device = devs_by_nsid.get(ns_id) if ns_id is not None else None
        if device is None:
            raise RuntimeError(
                f"Could not resolve device for {member['name']} (ns_id={ns_id}) "
                f"among devices {devs_by_nsid} for NQN {nqn[:50]}…")
        member["device"] = device
        mount_point = member["mount_point"]
        # The cluster daemon may have auto-reconnected the subsystem and left
        # the namespace mounted from a previous run.  Detect and handle:
        mnt_out, _, _ = local_run(
            f"findmnt -n -o TARGET {device} 2>/dev/null || true")
        current_mount = mnt_out.strip()
        if current_mount == mount_point:
            log.info(f"  {member['name']}: {device} already mounted at {mount_point}")
        else:
            if current_mount:
                local_run(f"sudo umount {device} 2>/dev/null || true")
            # Skip mkfs if the device already has a filesystem (persistent
            # namespaced volumes retain their filesystem across runs).
            blkid_out, _, _ = local_run(
                f"sudo blkid {device} 2>/dev/null || true")
            has_fs = bool(blkid_out.strip())
            format_and_mount(device, mount_point,
                             already_formatted=already_formatted or has_fs)
            log.info(f"  {member['name']}: {device} -> {mount_point}")
    return members


def start_migration(lvol_id, target_node_id, retries=5, retry_interval=10,
                    rebalancing_timeout=300):
    """Issue `lvol migrate` (pre-create), connect the TGT (inaccessible-ANA)
    paths, and return the migration ID.

    Retries the pre-create call itself on failure: `sn list` reporting a node
    as online only means its status flipped in the DB, not that its
    lvstore/RPC layer has finished coming back up — right after a restart,
    `create_migration`'s subsystem/bdev RPCs can transiently fail for a few
    seconds after the node already looks online. Retrying the whole
    pre-create (not just re-parsing the same failed output) is what actually
    recovers here.

    "Cluster is rebalancing" responses are handled separately: after a node
    restart the cluster runs a balancing_on_restart task that blocks new
    migrations for ~1-2 minutes. When detected, retries continue with 30s
    intervals for up to rebalancing_timeout seconds (default 300s).
    """
    last_err = None
    _rebalancing_deadline = None
    attempt = 0
    while True:
        attempt += 1
        start_out = sbctl("--dev", "lvol", "migrate", lvol_id, target_node_id)
        migration_id = parse_migration_id(start_out)
        if migration_id:
            for cmd in parse_nvme_connect_cmds(start_out):
                local_run(f"sudo {cmd} 2>&1 || true")
            time.sleep(2)
            return migration_id
        last_err = start_out
        _is_rebalancing = bool(start_out and "rebalancing" in start_out.lower())
        if _is_rebalancing:
            if _rebalancing_deadline is None:
                _rebalancing_deadline = time.time() + rebalancing_timeout
            remaining = _rebalancing_deadline - time.time()
            if remaining <= 0:
                break
            _wait = min(30, remaining)
            log.warning(
                f"start_migration: attempt {attempt}: cluster rebalancing — "
                f"retrying in {_wait:.0f}s ({remaining:.0f}s budget remaining)")
        elif attempt >= retries:
            break
        else:
            _wait = retry_interval
            log.warning(
                f"start_migration: pre-create attempt {attempt}/{retries} "
                f"returned no Migration ID for lvol {lvol_id}; "
                f"retrying in {_wait}s")
        time.sleep(_wait)
    raise RuntimeError(
        f"No Migration ID in migrate output for lvol {lvol_id} after "
        f"{attempt} attempts; last output: {(last_err or '')[:300]!r}")


def start_solo_migration(lvol_id, target_node_id, retries=5, retry_interval=10,
                         rebalancing_timeout=300):
    """Issue `lvol migrate --solo` (EXPERIMENTAL): migrate one member of a
    shared-namespace subsystem on its own, without moving its siblings.

    Mirrors start_migration()'s retry logic exactly. The only difference from
    a plain single-lvol migration is the --solo flag, which lets
    create_migration() proceed for a shared-subsystem member instead of
    raising "belongs to a shared NVMe-oF subsystem" (the normal --batch-or-
    refuse guard). Same NQN throughout — connecting the returned connect
    string(s) should NOT produce a new top-level NVMe device, only a second
    (inaccessible) path under the subsystem the client already has open for
    this lvol's siblings. Callers should verify that with
    list_nvme_namespaces() before/after, the same way the caller of
    start_migration() checks ANA state, not device count.
    """
    last_err = None
    _rebalancing_deadline = None
    attempt = 0
    while True:
        attempt += 1
        start_out = sbctl("--dev", "lvol", "migrate", lvol_id, target_node_id, "--solo")
        migration_id = parse_migration_id(start_out)
        if migration_id:
            for cmd in parse_nvme_connect_cmds(start_out):
                local_run(f"sudo {cmd} 2>&1 || true")
            time.sleep(2)
            return migration_id
        last_err = start_out
        _is_rebalancing = bool(start_out and "rebalancing" in start_out.lower())
        if _is_rebalancing:
            if _rebalancing_deadline is None:
                _rebalancing_deadline = time.time() + rebalancing_timeout
            remaining = _rebalancing_deadline - time.time()
            if remaining <= 0:
                break
            _wait = min(30, remaining)
            log.warning(
                f"start_solo_migration: attempt {attempt}: cluster rebalancing — "
                f"retrying in {_wait:.0f}s ({remaining:.0f}s budget remaining)")
        elif attempt >= retries:
            break
        else:
            _wait = retry_interval
            log.warning(
                f"start_solo_migration: pre-create attempt {attempt}/{retries} "
                f"returned no Migration ID for lvol {lvol_id}; "
                f"retrying in {_wait}s")
        time.sleep(_wait)
    raise RuntimeError(
        f"No Migration ID in migrate --solo output for lvol {lvol_id} after "
        f"{attempt} attempts; last output: {(last_err or '')[:300]!r}")


def migrate_solo_lvol(lvol_id, target_node_id, cluster_id, deadline=3600, terminal_only=True,
                      timeout=MIGRATION_TIMEOUT, poll=MIGRATION_POLL):
    """start_solo_migration() + continue_migration() + wait_for_migration()
    in one call. Returns the terminal status string."""
    migration_id = start_solo_migration(lvol_id, target_node_id)
    continue_migration(migration_id, deadline=deadline)
    return wait_for_migration(lvol_id, cluster_id, terminal_only=terminal_only,
                              timeout=timeout, poll=poll)


def continue_migration(migration_id, deadline=3600, retries=5, retry_interval=10,
                       cleanup_delay_seconds=0):
    """Issue `lvol migrate-continue`, retrying on the same class of
    post-restart timing race as start_migration(): the target node's status
    can still read in_restart/offline for a few seconds after the pre-create
    call above already succeeded against it. A failed attempt here leaves the
    migration permanently stuck in PHASE_PRE_CREATED — SNAP_COPY is never
    entered — so this must retry the call itself, not just wait and hope.
    """
    # TEMPORARY DEBUG: --cleanup-delay holds CLEANUP_TARGET open for fault injection.
    # Remove once debugging is complete.
    extra = ["--cleanup-delay", str(cleanup_delay_seconds)] if cleanup_delay_seconds else []
    last_err = None
    for attempt in range(1, retries + 1):
        out = sbctl("--dev", "lvol", "migrate-continue", migration_id,
                   "--deadline", str(deadline), *extra)
        if "migration started" in (out or "").lower():
            return out
        last_err = out
        log.warning(f"continue_migration: attempt {attempt}/{retries} failed for "
                    f"{migration_id} (output: {(out or '')[:200]!r}); retrying "
                    f"in {retry_interval}s")
        time.sleep(retry_interval)
    raise RuntimeError(
        f"migrate-continue never reported success for {migration_id} after "
        f"{retries} attempts; last output: {(last_err or '')[:300]!r}")


def migrate_lvol(lvol_id, target_node_id, cluster_id, deadline=3600, terminal_only=True,
                 timeout=MIGRATION_TIMEOUT, poll=MIGRATION_POLL):
    """start_migration() + continue_migration() + wait_for_migration() in one
    call. Returns the terminal status string.
    """
    migration_id = start_migration(lvol_id, target_node_id)
    continue_migration(migration_id, deadline=deadline)
    return wait_for_migration(lvol_id, cluster_id, terminal_only=terminal_only,
                              timeout=timeout, poll=poll)


# ---------------------------------------------------------------------------
# Batch migration helpers (namespaced-volume groups)
# ---------------------------------------------------------------------------
def parse_group_id(output):
    m = re.search(r'Migration Group ID:\s*([0-9a-f-]{36})', output or "", re.IGNORECASE)
    return m.group(1) if m else None


def get_batch_record(group_id, cluster_id):
    """Return the migrate-group-list record for *group_id*, or None."""
    groups = sbctl("--dev", "lvol", "migrate-group-list",
                   "--cluster-id", cluster_id, "--json", parse_json=True)
    if isinstance(groups, dict):
        groups = groups.get("results", [])
    for g in (groups or []):
        if get(g, "group_id") == group_id:
            return g
    return None


def wait_for_batch_migration(group_id, cluster_id,
                             timeout=MIGRATION_TIMEOUT, poll=MIGRATION_POLL):
    """Poll migrate-group-list until the group reaches a terminal status.
    Returns (status, phase). status is 'timeout' if the deadline expires.
    """
    log.info(f"Polling batch migration for group {group_id[:8]}...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        g = get_batch_record(group_id, cluster_id)
        if not g:
            log.info("  batch group record gone — treating as done")
            return "done", ""
        status = (get(g, "status") or "unknown").lower()
        phase  = (get(g, "phase")  or "").lower()
        error  = get(g, "error_message") or ""
        line = (f"  batch status={status}  phase={phase}"
                f"  members={get(g, 'member_count')}")
        if error:
            log.warning(f"{line}  error={error!r}")
        else:
            log.info(line)
        if status in ("done", "completed", "failed", "cancelled", "error"):
            return status, phase
        time.sleep(poll)
    return "timeout", ""


def start_batch_migration(any_member_id, target_node_id, retries=5, retry_interval=10,
                          rebalancing_timeout=300):
    """Issue `lvol migrate --batch`, connect the TGT paths, and return the
    Migration Group ID.  Mirrors start_migration() retry logic.
    """
    last_err = None
    _rebalancing_deadline = None
    attempt = 0
    while True:
        attempt += 1
        start_out = sbctl("--dev", "lvol", "migrate",
                          any_member_id, target_node_id, "--batch")
        group_id = parse_group_id(start_out)
        if group_id:
            for cmd in parse_nvme_connect_cmds(start_out):
                local_run(f"sudo {cmd} 2>&1 || true")
            time.sleep(2)
            return group_id
        last_err = start_out
        _is_rebalancing = bool(start_out and "rebalancing" in start_out.lower())
        if _is_rebalancing:
            if _rebalancing_deadline is None:
                _rebalancing_deadline = time.time() + rebalancing_timeout
            remaining = _rebalancing_deadline - time.time()
            if remaining <= 0:
                break
            _wait = min(30, remaining)
            log.warning(
                f"start_batch_migration: attempt {attempt}: cluster rebalancing — "
                f"retrying in {_wait:.0f}s ({remaining:.0f}s budget remaining)")
        elif attempt >= retries:
            break
        else:
            _wait = retry_interval
            log.warning(
                f"start_batch_migration: pre-create attempt {attempt}/{retries} "
                f"returned no Migration Group ID for member {any_member_id}; "
                f"retrying in {_wait}s")
        time.sleep(_wait)
    raise RuntimeError(
        f"No Migration Group ID in migrate --batch output for member {any_member_id} "
        f"after {attempt} attempts; last output: {(last_err or '')[:300]!r}")


def continue_batch_migration(group_id, deadline=3600, retries=5, retry_interval=10):
    """Issue `lvol migrate-continue --batch`, retrying on transient failures."""
    last_err = None
    for attempt in range(1, retries + 1):
        out = sbctl("--dev", "lvol", "migrate-continue", group_id,
                    "--batch", "--deadline", str(deadline))
        out_lower = (out or "").lower()
        if "migration started" in out_lower or "batch migration started" in out_lower:
            return out
        last_err = out
        log.warning(
            f"continue_batch_migration: attempt {attempt}/{retries} failed for "
            f"{group_id} (output: {(out or '')[:200]!r}); retrying in {retry_interval}s")
        time.sleep(retry_interval)
    raise RuntimeError(
        f"migrate-continue --batch never reported success for {group_id} after "
        f"{retries} attempts; last output: {(last_err or '')[:300]!r}")
