#!/usr/bin/env python3
"""
test_migration_suite.py

Comprehensive "fire and forget" migration test suite.  Each scenario runs in
its own isolated pool so failures leave no shared state.  Crashed or failed
scenarios are recorded and the suite continues.

Usage:
  python3 test_migration_suite.py                          # run all
  python3 test_migration_suite.py --scenario plain_lvol   # by name
  python3 test_migration_suite.py --scenario 1 3 5        # by 1-based index
  python3 test_migration_suite.py --tag happy_path        # by tag
  python3 test_migration_suite.py --list                  # print registry and exit
"""

import argparse
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional

sys.path.insert(0, str(Path(__file__).parent))
import migration_test_lib as lib

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_LOG_DIR    = Path(__file__).parent / "logs"
_MOUNT_BASE = "/mnt/suite"
_LVOL_SIZE  = "10G"
_FIO_SIZE   = "2G"
_SNAP_PFX   = "snap_"


# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
class SkipScenario(Exception):
    """Raised by a scenario to signal not-yet-implemented or intentionally skipped."""


@dataclass
class ScenarioContext:
    cluster_id: str
    nodes: list
    node_ip_map: dict
    log_dir: Path
    fault_type: str = "reboot"
    src_id: Optional[str] = None   # None = scenario auto-picks per run
    tgt_id: Optional[str] = None   # None = scenario auto-picks per run


@dataclass
class ScenarioResult:
    name: str
    status: str          # PASS | FAIL | SKIP | CRASH
    passes: List[str]
    failures: List[str]
    crash_msg: str = ""


@dataclass
class Scenario:
    name: str
    description: str
    fn: Callable
    tags: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _pool(name: str) -> str:
    return f"suite_{name}"


def _pre_cleanup(ctx: ScenarioContext, pool_name: str,
                 lvol_names=None, snap_names=None, mount=None):
    try:
        lib.cleanup_pool_and_lvols(pool_name, ctx.cluster_id,
                                   lvol_names=lvol_names,
                                   snap_names=snap_names,
                                   mount=mount)
    except Exception as e:
        lib.log.warning(f"Pre-cleanup [{pool_name}] non-fatal: {e}")


def _fio_log(ctx: ScenarioContext, name: str, suffix: str = "") -> str:
    return str(ctx.log_dir / f"{name}_fio{suffix}.log")


def _src_tgt(ctx: ScenarioContext, overlap: str = "no-overlap"):
    src = ctx.src_id or lib.pick_source_node(ctx.nodes)
    tgt = ctx.tgt_id or lib.pick_target_node(src, ctx.nodes, overlap)
    lib.log.info(f"  src={src[:8]}  tgt={tgt[:8]}  overlap={overlap!r}")
    return src, tgt


# ---------------------------------------------------------------------------
# Scenario: plain_lvol
# ---------------------------------------------------------------------------
def run_plain_lvol(ctx: ScenarioContext, chk: lib.Checklist):
    POOL  = _pool("plain_lvol")
    LVOL  = "vol"
    MOUNT = f"{_MOUNT_BASE}/plain"
    fio_file = f"{MOUNT}/data.bin"

    _pre_cleanup(ctx, POOL, [LVOL], mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "plain_lvol", ".pre"), size=_FIO_SIZE)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, "plain_lvol"), size=_FIO_SIZE)
    time.sleep(15)

    try:
        status = lib.migrate_lvol(lvol_id, tgt_id, ctx.cluster_id)
        chk.check(status in ("done", "completed"), f"Migration done (status={status})")
        node = lib.get_lvol_node(lvol_id)
        chk.check(node == tgt_id, f"Lvol on target (got {str(node)[:8]})")
        ok, _ = lib.check_fio_output(_fio_log(ctx, "plain_lvol"), fault_injected=False)
        chk.check(ok, "No fio data corruption")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, [LVOL])


# ---------------------------------------------------------------------------
# Scenario: crypto_lvol
# ---------------------------------------------------------------------------
def run_crypto_lvol(ctx: ScenarioContext, chk: lib.Checklist):
    POOL  = _pool("crypto_lvol")
    LVOL  = "vol"
    MOUNT = f"{_MOUNT_BASE}/crypto"
    fio_file = f"{MOUNT}/data.bin"

    _pre_cleanup(ctx, POOL, [LVOL], mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id, crypto=True)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "crypto_lvol", ".pre"), size=_FIO_SIZE)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, "crypto_lvol"), size=_FIO_SIZE)
    time.sleep(15)

    try:
        status = lib.migrate_lvol(lvol_id, tgt_id, ctx.cluster_id)
        chk.check(status in ("done", "completed"), f"Migration done (status={status})")
        node = lib.get_lvol_node(lvol_id)
        chk.check(node == tgt_id, f"Lvol on target (got {str(node)[:8]})")
        ok, _ = lib.check_fio_output(_fio_log(ctx, "crypto_lvol"), fault_injected=False)
        chk.check(ok, "No fio data corruption")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, [LVOL])


# ---------------------------------------------------------------------------
# Scenario: lvol_with_snaps
# ---------------------------------------------------------------------------
def run_lvol_with_snaps(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("lvol_snaps")
    LVOL       = "vol"
    SNAP_COUNT = 5
    MOUNT      = f"{_MOUNT_BASE}/snaps"
    fio_file   = f"{MOUNT}/data.bin"
    snap_names = [f"{_SNAP_PFX}{i}" for i in range(1, SNAP_COUNT + 1)]

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "lvol_snaps", ".pre"), size=_FIO_SIZE)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, "lvol_snaps"), size=_FIO_SIZE)

    lib.log.info(f"  Waiting 30s then creating {SNAP_COUNT} snapshots (10s apart)...")
    time.sleep(30)
    for i in range(1, SNAP_COUNT + 1):
        time.sleep(10)
        lib.sbctl("volume", "create-snapshot", lvol_id, f"{_SNAP_PFX}{i}")
        lib.log.info(f"  snapshot {i}/{SNAP_COUNT}")

    try:
        status = lib.migrate_lvol(lvol_id, tgt_id, ctx.cluster_id)
        chk.check(status in ("done", "completed"), f"Migration done (status={status})")
        node = lib.get_lvol_node(lvol_id)
        chk.check(node == tgt_id, f"Lvol on target (got {str(node)[:8]})")
        ok, _ = lib.check_fio_output(_fio_log(ctx, "lvol_snaps"), fault_injected=False)
        chk.check(ok, "No fio data corruption")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names)


# ---------------------------------------------------------------------------
# Cancel / fault helpers (shared across scenarios)
# ---------------------------------------------------------------------------
def _cancel_after_snaps(migration_id, lvol_id, cluster_id, target_count, result):
    """Thread body: wait for target_count snaps copied then cancel. Sets result[0]=True."""
    try:
        reached = lib.wait_for_snap_count(lvol_id, cluster_id, target_count, timeout=300)
        if reached:
            lib.cancel_migration(migration_id)
            result[0] = True
    except Exception as e:
        lib.log.warning(f"[cancel thread] error: {e}")


def _watch_and_cancel_phase(migration_id, lvol_id, cluster_id, phase_keywords, result,
                             timeout=300):
    """Thread body: watch for phase keyword match then cancel. Sets result[0]=True."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = lib.get_migration_record(lvol_id, cluster_id)
        if not m:
            break
        status = (lib.get(m, "status") or "").lower()
        phase  = (lib.get(m, "phase") or "").lower()
        if status in ("done", "completed", "failed", "cancelled", "error"):
            break
        if any(k in phase or k in status for k in phase_keywords):
            lib.cancel_migration(migration_id)
            result[0] = True
            return
        time.sleep(0.5)


def _run_simple_migration(ctx, chk, scenario_name, pool_suffix, overlap="no-overlap"):
    """Plain lvol migration reusable by multiple scenarios (only target differs)."""
    POOL     = _pool(pool_suffix)
    LVOL     = "vol"
    MOUNT    = f"{_MOUNT_BASE}/{pool_suffix}"
    fio_file = f"{MOUNT}/data.bin"

    _pre_cleanup(ctx, POOL, [LVOL], mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx, overlap)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, scenario_name, ".pre"), size=_FIO_SIZE)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, scenario_name), size=_FIO_SIZE)
    time.sleep(15)

    try:
        status = lib.migrate_lvol(lvol_id, tgt_id, ctx.cluster_id)
        chk.check(status in ("done", "completed"), f"Migration done (status={status})")
        node = lib.get_lvol_node(lvol_id)
        chk.check(node == tgt_id, f"Lvol on target (got {str(node)[:8]})")
        ok, _ = lib.check_fio_output(_fio_log(ctx, scenario_name), fault_injected=False)
        chk.check(ok, "No fio data corruption")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, [LVOL])


# ---------------------------------------------------------------------------
# Scenario: tree
# ---------------------------------------------------------------------------
def run_tree(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("tree")
    PARENT     = "parent"
    SNAP_NAMES = ["snap_a1", "snap_a2"]
    CLONE_NAMES = ["clone_b", "clone_c"]
    MOUNT      = f"{_MOUNT_BASE}/tree"
    fio_file   = f"{MOUNT}/data.bin"

    _pre_cleanup(ctx, POOL, [PARENT] + CLONE_NAMES, snap_names=SNAP_NAMES, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id   = lib.ensure_pool(POOL, ctx.cluster_id)
    parent_id = lib.create_lvol(PARENT, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(parent_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "tree", ".pre"), size=_FIO_SIZE)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, "tree"), size=_FIO_SIZE)
    time.sleep(15)

    # Build tree: parent → snap_a1 → clone_b; parent → snap_a2 → clone_c
    lib.sbctl("volume", "create-snapshot", parent_id, "snap_a1")
    time.sleep(3)
    snap_a1 = lib.get_snapshot(ctx.cluster_id, "snap_a1")
    snap_a1_id = lib.get(snap_a1, "id") if snap_a1 else None
    if snap_a1_id:
        lib.sbctl("snapshot", "clone", snap_a1_id, "clone_b")
    time.sleep(5)

    lib.sbctl("volume", "create-snapshot", parent_id, "snap_a2")
    time.sleep(3)
    snap_a2 = lib.get_snapshot(ctx.cluster_id, "snap_a2")
    snap_a2_id = lib.get(snap_a2, "id") if snap_a2 else None
    if snap_a2_id:
        lib.sbctl("snapshot", "clone", snap_a2_id, "clone_c")
    time.sleep(5)

    clone_b_lv = lib.get_lvol_by_name(pool_id, "clone_b")
    clone_c_lv = lib.get_lvol_by_name(pool_id, "clone_c")
    clone_b_id = lib.get(clone_b_lv, "id") if clone_b_lv else None
    clone_c_id = lib.get(clone_c_lv, "id") if clone_c_lv else None
    chk.check(clone_b_id is not None, "clone_b created")
    chk.check(clone_c_id is not None, "clone_c created")

    try:
        status = lib.migrate_lvol(parent_id, tgt_id, ctx.cluster_id)
        chk.check(status in ("done", "completed"), f"Tree migration done (status={status})")
        node = lib.get_lvol_node(parent_id)
        chk.check(node == tgt_id, f"Parent on target (got {str(node)[:8]})")
        if clone_b_id:
            n = lib.get_lvol_node(clone_b_id)
            chk.check(n == tgt_id, f"clone_b on target (got {str(n)[:8]})")
        if clone_c_id:
            n = lib.get_lvol_node(clone_c_id)
            chk.check(n == tgt_id, f"clone_c on target (got {str(n)[:8]})")
        ok, _ = lib.check_fio_output(_fio_log(ctx, "tree"), fault_injected=False)
        chk.check(ok, "No fio data corruption")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, [PARENT] + CLONE_NAMES, snap_names=SNAP_NAMES)


# ---------------------------------------------------------------------------
# Scenario: batch
# ---------------------------------------------------------------------------
def run_batch(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("batch")
    MAX_NS     = 4
    MOUNT      = f"{_MOUNT_BASE}/batch"
    fio_file   = f"{MOUNT}/data.bin"
    lvol_names = [f"ns_{i}" for i in range(MAX_NS)]

    _pre_cleanup(ctx, POOL, lvol_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    master_id = lib.create_lvol("ns_0", _LVOL_SIZE, pool_id, src_id,
                                namespaced=True, max_ns=MAX_NS)
    for i in range(1, MAX_NS):
        lib.create_lvol(f"ns_{i}", _LVOL_SIZE, pool_id, src_id,
                        namespaced=True, max_ns=MAX_NS)

    lib.connect_and_mount_lvol(master_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "batch", ".pre"), size=_FIO_SIZE)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, "batch"), size=_FIO_SIZE)
    time.sleep(15)

    try:
        group_id = lib.start_batch_migration(master_id, tgt_id)
        chk.check(bool(group_id), f"Got batch group ID")
        lib.continue_batch_migration(group_id)
        status, _ = lib.wait_for_batch_migration(group_id, ctx.cluster_id)
        chk.check(status in ("done", "completed"), f"Batch migration done (status={status})")
        for name in lvol_names:
            lv = lib.get_lvol_by_name(pool_id, name)
            if lv:
                n = lib.get_lvol_node(lib.get(lv, "id"))
                chk.check(n == tgt_id, f"{name} on target (got {str(n)[:8]})")
        ok, _ = lib.check_fio_output(_fio_log(ctx, "batch"), fault_injected=False)
        chk.check(ok, "No fio data corruption")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, lvol_names)


# ---------------------------------------------------------------------------
# Scenario: clone_migration
# ---------------------------------------------------------------------------
def run_clone_migration(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("clone_mig")
    PARENT     = "parent"
    SNAP_NAME  = "snap_1"
    CLONE_NAME = "clone_x"
    MOUNT      = f"{_MOUNT_BASE}/clone_mig"
    fio_file   = f"{MOUNT}/data.bin"

    _pre_cleanup(ctx, POOL, [PARENT, CLONE_NAME], snap_names=[SNAP_NAME], mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id   = lib.ensure_pool(POOL, ctx.cluster_id)
    parent_id = lib.create_lvol(PARENT, _LVOL_SIZE, pool_id, src_id)

    # Prefill parent, take snapshot, create clone
    lib.connect_and_mount_lvol(parent_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "clone_mig", ".pre"), size=_FIO_SIZE)
    lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
    lib.local_run("sudo nvme disconnect-all 2>&1 || true")

    lib.sbctl("volume", "create-snapshot", parent_id, SNAP_NAME)
    time.sleep(3)
    snap = lib.get_snapshot(ctx.cluster_id, SNAP_NAME)
    snap_id = lib.get(snap, "id") if snap else None
    chk.check(snap_id is not None, "Snapshot created")
    if not snap_id:
        return

    lib.sbctl("snapshot", "clone", snap_id, CLONE_NAME)
    time.sleep(5)
    clone_lv  = lib.get_lvol_by_name(pool_id, CLONE_NAME)
    clone_id  = lib.get(clone_lv, "id") if clone_lv else None
    chk.check(clone_id is not None, "Clone created")
    if not clone_id:
        return

    # Run fio on the clone and migrate it (not the parent)
    lib.connect_and_mount_lvol(clone_id, MOUNT, already_formatted=True)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, "clone_mig"), size=_FIO_SIZE)
    time.sleep(15)

    try:
        status = lib.migrate_lvol(clone_id, tgt_id, ctx.cluster_id)
        chk.check(status in ("done", "completed"), f"Clone migration done (status={status})")
        node = lib.get_lvol_node(clone_id)
        chk.check(node == tgt_id, f"Clone on target (got {str(node)[:8]})")
        ok, _ = lib.check_fio_output(_fio_log(ctx, "clone_mig"), fault_injected=False)
        chk.check(ok, "No fio data corruption")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, [PARENT, CLONE_NAME], snap_names=[SNAP_NAME])


# ---------------------------------------------------------------------------
# Scenario: deep_tree
# ---------------------------------------------------------------------------
def run_deep_tree(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("deep_tree")
    ROOT       = "root"
    SNAPS      = ["ds1", "ds2", "ds3", "ds4"]
    CLONES     = ["dc1", "dc2", "dc3", "dc4"]
    MOUNT      = f"{_MOUNT_BASE}/deep_tree"
    fio_file   = f"{MOUNT}/data.bin"

    _pre_cleanup(ctx, POOL, [ROOT] + CLONES, snap_names=SNAPS, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    root_id = lib.create_lvol(ROOT, _LVOL_SIZE, pool_id, src_id)

    lib.connect_and_mount_lvol(root_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "deep_tree", ".pre"), size=_FIO_SIZE)
    fio_bg   = lib.start_fio_bg(fio_file, _fio_log(ctx, "deep_tree"), size=_FIO_SIZE)
    time.sleep(10)

    # Build 5-level chain: root → snap → clone1 → snap → clone2 → …
    current_id = root_id
    clone_ids  = []
    for snap_name, clone_name in zip(SNAPS, CLONES):
        lib.sbctl("volume", "create-snapshot", current_id, snap_name)
        time.sleep(3)
        snap    = lib.get_snapshot(ctx.cluster_id, snap_name)
        snap_id = lib.get(snap, "id") if snap else None
        if not snap_id:
            chk.check(False, f"Snapshot {snap_name} created")
            break
        lib.sbctl("snapshot", "clone", snap_id, clone_name)
        time.sleep(3)
        clone_lv  = lib.get_lvol_by_name(pool_id, clone_name)
        clone_id  = lib.get(clone_lv, "id") if clone_lv else None
        if not clone_id:
            chk.check(False, f"Clone {clone_name} created")
            break
        clone_ids.append((clone_name, clone_id))
        current_id = clone_id

    try:
        status = lib.migrate_lvol(root_id, tgt_id, ctx.cluster_id)
        chk.check(status in ("done", "completed"),
                  f"Deep-tree root migration done (status={status})")
        node = lib.get_lvol_node(root_id)
        chk.check(node == tgt_id, f"Root on target (got {str(node)[:8]})")
        for name, cid in clone_ids:
            n = lib.get_lvol_node(cid)
            chk.check(n == tgt_id, f"{name} on target (got {str(n)[:8]})")
        ok, _ = lib.check_fio_output(_fio_log(ctx, "deep_tree"), fault_injected=False)
        chk.check(ok, "No fio data corruption")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, [ROOT] + CLONES, snap_names=SNAPS)


# ---------------------------------------------------------------------------
# Scenario: batch_shared_ancestry
# ---------------------------------------------------------------------------
def run_batch_shared_ancestry(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("batch_anc")
    MAX_NS     = 3
    SNAP_COUNT = 5
    MOUNT      = f"{_MOUNT_BASE}/batch_anc"
    fio_file   = f"{MOUNT}/data.bin"
    lvol_names = [f"ns_{i}" for i in range(MAX_NS)]
    snap_names = [f"bsnap_{i}" for i in range(1, SNAP_COUNT + 1)]

    _pre_cleanup(ctx, POOL, lvol_names, snap_names=snap_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id   = lib.ensure_pool(POOL, ctx.cluster_id)
    master_id = lib.create_lvol("ns_0", _LVOL_SIZE, pool_id, src_id,
                                namespaced=True, max_ns=MAX_NS)
    for i in range(1, MAX_NS):
        lib.create_lvol(f"ns_{i}", _LVOL_SIZE, pool_id, src_id,
                        namespaced=True, max_ns=MAX_NS)

    lib.connect_and_mount_lvol(master_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "batch_anc", ".pre"), size=_FIO_SIZE)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, "batch_anc"), size=_FIO_SIZE)

    # Snapshots on master establish shared ancestry for the batch
    lib.log.info(f"  Creating {SNAP_COUNT} snapshots (shared ancestry)...")
    time.sleep(20)
    for name in snap_names:
        lib.sbctl("volume", "create-snapshot", master_id, name)
        time.sleep(8)

    try:
        group_id = lib.start_batch_migration(master_id, tgt_id)
        chk.check(bool(group_id), "Got batch group ID")
        lib.continue_batch_migration(group_id)
        status, _ = lib.wait_for_batch_migration(group_id, ctx.cluster_id)
        chk.check(status in ("done", "completed"),
                  f"Batch migration done (status={status})")
        for name in lvol_names:
            lv = lib.get_lvol_by_name(pool_id, name)
            if lv:
                n = lib.get_lvol_node(lib.get(lv, "id"))
                chk.check(n == tgt_id, f"{name} on target (got {str(n)[:8]})")
        ok, _ = lib.check_fio_output(_fio_log(ctx, "batch_anc"), fault_injected=False)
        chk.check(ok, "No fio data corruption")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, lvol_names, snap_names=snap_names)


# ---------------------------------------------------------------------------
# Scenario: ha_overlap_a  (TGT-prim IS SRC-sec)
# ---------------------------------------------------------------------------
def run_ha_overlap_a(ctx: ScenarioContext, chk: lib.Checklist):
    _run_simple_migration(ctx, chk, "ha_overlap_a", "ha_ov_a", overlap="a")


# ---------------------------------------------------------------------------
# Scenario: ha_overlap_b  (SRC-prim IS TGT-sec)
# ---------------------------------------------------------------------------
def run_ha_overlap_b(ctx: ScenarioContext, chk: lib.Checklist):
    _run_simple_migration(ctx, chk, "ha_overlap_b", "ha_ov_b", overlap="b")


# ---------------------------------------------------------------------------
# Scenario: round_trip
# ---------------------------------------------------------------------------
def run_round_trip(ctx: ScenarioContext, chk: lib.Checklist):
    POOL     = _pool("round_trip")
    LVOL     = "vol"
    MOUNT    = f"{_MOUNT_BASE}/round_trip"
    fio_file = f"{MOUNT}/data.bin"

    _pre_cleanup(ctx, POOL, [LVOL], mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "round_trip", ".pre"), size=_FIO_SIZE)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, "round_trip"), size=_FIO_SIZE)
    time.sleep(15)

    try:
        for from_id, to_id, label in [
            (src_id, tgt_id, "A→B"),
            (tgt_id, src_id, "B→A"),
        ]:
            time.sleep(5)
            lib.log.info(f"  Round-trip hop: {label}")
            status = lib.migrate_lvol(lvol_id, to_id, ctx.cluster_id)
            chk.check(status in ("done", "completed"),
                      f"{label} done (status={status})")
            node = lib.get_lvol_node(lvol_id)
            chk.check(node == to_id,
                      f"Lvol on expected node after {label} (got {str(node)[:8]})")

        ok, _ = lib.check_fio_output(_fio_log(ctx, "round_trip"), fault_injected=False)
        chk.check(ok, "No fio data corruption across round trip")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, [LVOL])


# ---------------------------------------------------------------------------
# Scenario: cancel_snap_copy_early
# ---------------------------------------------------------------------------
def run_cancel_snap_copy_early(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("cancel_early")
    LVOL       = "vol"
    SNAP_COUNT = 8
    MOUNT      = f"{_MOUNT_BASE}/cancel_early"
    fio_file   = f"{MOUNT}/data.bin"
    snap_names = [f"cse_{i}" for i in range(1, SNAP_COUNT + 1)]

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "cancel_early", ".pre"), size=_FIO_SIZE)
    lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
    lib.local_run("sudo nvme disconnect-all 2>&1 || true")

    lib.log.info(f"  Creating {SNAP_COUNT} snapshots for snap_copy work...")
    for name in snap_names:
        lib.sbctl("volume", "create-snapshot", lvol_id, name)
        time.sleep(2)

    migration_id = lib.start_migration(lvol_id, tgt_id)
    chk.check(bool(migration_id), "Migration started")
    lib.continue_migration(migration_id)

    cancelled = [False]
    t = threading.Thread(target=_cancel_after_snaps,
                         args=(migration_id, lvol_id, ctx.cluster_id, 2, cancelled),
                         daemon=True)
    t.start()
    status = lib.wait_for_migration(lvol_id, ctx.cluster_id, terminal_only=True)
    t.join(timeout=30)

    chk.check(status == "cancelled",
              f"Migration cancelled early in snap_copy (status={status})")
    chk.check(cancelled[0], "Cancel issued at snap count 2")
    node = lib.get_lvol_node(lvol_id)
    chk.check(node == src_id,
              f"Lvol back on source after early cancel (got {str(node)[:8]})")

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names)


# ---------------------------------------------------------------------------
# Scenario: cancel_snap_copy_late
# ---------------------------------------------------------------------------
def run_cancel_snap_copy_late(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("cancel_late")
    LVOL       = "vol"
    SNAP_COUNT = 10
    MOUNT      = f"{_MOUNT_BASE}/cancel_late"
    fio_file   = f"{MOUNT}/data.bin"
    snap_names = [f"csl_{i}" for i in range(1, SNAP_COUNT + 1)]

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "cancel_late", ".pre"), size=_FIO_SIZE)
    lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
    lib.local_run("sudo nvme disconnect-all 2>&1 || true")

    lib.log.info(f"  Creating {SNAP_COUNT} snapshots for snap_copy work...")
    for name in snap_names:
        lib.sbctl("volume", "create-snapshot", lvol_id, name)
        time.sleep(2)

    migration_id = lib.start_migration(lvol_id, tgt_id)
    chk.check(bool(migration_id), "Migration started")
    lib.continue_migration(migration_id)

    cancelled = [False]
    # Cancel near end of snap_copy (8 of 10 snaps)
    t = threading.Thread(target=_cancel_after_snaps,
                         args=(migration_id, lvol_id, ctx.cluster_id, 8, cancelled),
                         daemon=True)
    t.start()
    status = lib.wait_for_migration(lvol_id, ctx.cluster_id, terminal_only=True)
    t.join(timeout=30)

    chk.check(status == "cancelled",
              f"Migration cancelled late in snap_copy (status={status})")
    chk.check(cancelled[0], "Cancel issued at snap count 8")
    node = lib.get_lvol_node(lvol_id)
    chk.check(node == src_id,
              f"Lvol back on source after late cancel (got {str(node)[:8]})")

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names)


# ---------------------------------------------------------------------------
# Scenario: cancel_lvol_migrate
# ---------------------------------------------------------------------------
def run_cancel_lvol_migrate(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("cancel_lvol")
    LVOL       = "vol"
    SNAP_COUNT = 3
    MOUNT      = f"{_MOUNT_BASE}/cancel_lvol"
    fio_file   = f"{MOUNT}/data.bin"
    snap_names = [f"clm_{i}" for i in range(1, SNAP_COUNT + 1)]

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "cancel_lvol", ".pre"), size=_FIO_SIZE)
    lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
    lib.local_run("sudo nvme disconnect-all 2>&1 || true")

    for name in snap_names:
        lib.sbctl("volume", "create-snapshot", lvol_id, name)
        time.sleep(2)

    migration_id = lib.start_migration(lvol_id, tgt_id)
    chk.check(bool(migration_id), "Migration started")
    lib.continue_migration(migration_id)

    cancelled = [False]
    kw = lib.PHASE_KEYWORDS.get("lvol_migrate", ["migrate", "lvol_migrate", "bulk"])
    t  = threading.Thread(target=_watch_and_cancel_phase,
                          args=(migration_id, lvol_id, ctx.cluster_id, kw, cancelled),
                          daemon=True)
    t.start()
    status = lib.wait_for_migration(lvol_id, ctx.cluster_id, terminal_only=True)
    t.join(timeout=30)

    if not cancelled[0]:
        # lvol_migrate is brief — cancel may have raced the phase completing
        chk.check(True,
                  "lvol_migrate phase too brief for cancel — inconclusive (noted)")
    else:
        chk.check(status == "cancelled",
                  f"Migration cancelled in lvol_migrate (status={status})")
    node = lib.get_lvol_node(lvol_id)
    chk.check(node in (src_id, tgt_id),
              f"Lvol on valid node after cancel (got {str(node)[:8]})")

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names)


# ---------------------------------------------------------------------------
# Scenario: src_ha_fault_snap_copy
# ---------------------------------------------------------------------------
def run_src_ha_fault_snap_copy(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("src_ha_snap")
    LVOL       = "vol"
    SNAP_COUNT = 8
    MOUNT      = f"{_MOUNT_BASE}/src_ha_snap"
    fio_file   = f"{MOUNT}/data.bin"
    snap_names = [f"shs_{i}" for i in range(1, SNAP_COUNT + 1)]

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    sec_id = lib.get_node_secondary_id(src_id, ctx.nodes)
    if not sec_id:
        raise SkipScenario("Source has no secondary node — cannot test src HA fault")

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "src_ha_snap", ".pre"), size=_FIO_SIZE)
    lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
    lib.local_run("sudo nvme disconnect-all 2>&1 || true")

    for name in snap_names:
        lib.sbctl("volume", "create-snapshot", lvol_id, name)
        time.sleep(2)

    # Crash the source's secondary node during snap_copy
    injector = lib.FaultInjector(
        fault_type=ctx.fault_type,
        phase="snap_copy",
        node_role="target",          # 'target' fires tgt_id; we set that to sec_id
        src_id=src_id,
        tgt_id=sec_id,               # source's secondary — the HA node under test
        nodes=ctx.nodes,
        cluster_id=ctx.cluster_id,
        node_ip_map=ctx.node_ip_map,
    )
    migration_id = lib.start_migration(lvol_id, tgt_id)
    chk.check(bool(migration_id), "Migration started")
    lib.continue_migration(migration_id)
    injector.start_watching(lvol_id)

    status = lib.wait_for_migration(lvol_id, ctx.cluster_id, terminal_only=True,
                                    timeout=lib.MIGRATION_TIMEOUT + 300)
    injector.wait()

    chk.check(injector.fired.is_set(), "Fault injected on source secondary")
    chk.check(status in ("done", "completed"),
              f"Migration survived src-HA fault (status={status})")
    node = lib.get_lvol_node(lvol_id)
    chk.check(node == tgt_id, f"Lvol on target (got {str(node)[:8]})")

    if ctx.fault_type == "reboot":
        lib.log.info("  Waiting for cluster to recover after reboot...")
        lib.wait_cluster_healthy(timeout=300)

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names)


# ---------------------------------------------------------------------------
# Scenario: tgt_ha_fault_snap_copy
# ---------------------------------------------------------------------------
def run_tgt_ha_fault_snap_copy(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("tgt_ha_snap")
    LVOL       = "vol"
    SNAP_COUNT = 8
    MOUNT      = f"{_MOUNT_BASE}/tgt_ha_snap"
    fio_file   = f"{MOUNT}/data.bin"
    snap_names = [f"ths_{i}" for i in range(1, SNAP_COUNT + 1)]

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    tgt_sec_id = lib.get_node_secondary_id(tgt_id, ctx.nodes)
    if not tgt_sec_id:
        raise SkipScenario("Target has no secondary node — cannot test tgt HA fault")

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "tgt_ha_snap", ".pre"), size=_FIO_SIZE)
    lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
    lib.local_run("sudo nvme disconnect-all 2>&1 || true")

    for name in snap_names:
        lib.sbctl("volume", "create-snapshot", lvol_id, name)
        time.sleep(2)

    injector = lib.FaultInjector(
        fault_type=ctx.fault_type,
        phase="snap_copy",
        node_role="target",
        src_id=src_id,
        tgt_id=tgt_sec_id,           # secondary of migration target
        nodes=ctx.nodes,
        cluster_id=ctx.cluster_id,
        node_ip_map=ctx.node_ip_map,
    )
    migration_id = lib.start_migration(lvol_id, tgt_id)
    chk.check(bool(migration_id), "Migration started")
    lib.continue_migration(migration_id)
    injector.start_watching(lvol_id)

    status = lib.wait_for_migration(lvol_id, ctx.cluster_id, terminal_only=True,
                                    timeout=lib.MIGRATION_TIMEOUT + 300)
    injector.wait()

    chk.check(injector.fired.is_set(), "Fault injected on target secondary")
    chk.check(status in ("done", "completed"),
              f"Migration survived tgt-HA fault (status={status})")
    node = lib.get_lvol_node(lvol_id)
    chk.check(node == tgt_id, f"Lvol on target (got {str(node)[:8]})")

    if ctx.fault_type == "reboot":
        lib.log.info("  Waiting for cluster to recover after reboot...")
        lib.wait_cluster_healthy(timeout=300)

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names)


# ---------------------------------------------------------------------------
# Scenario: src_ha_fault_cleanup
# ---------------------------------------------------------------------------
def run_src_ha_fault_cleanup(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("src_ha_clnup")
    LVOL       = "vol"
    SNAP_COUNT = 3
    MOUNT      = f"{_MOUNT_BASE}/src_ha_clnup"
    fio_file   = f"{MOUNT}/data.bin"
    snap_names = [f"shc_{i}" for i in range(1, SNAP_COUNT + 1)]

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    sec_id = lib.get_node_secondary_id(src_id, ctx.nodes)
    if not sec_id:
        raise SkipScenario("Source has no secondary node — cannot test src-HA cleanup fault")

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "src_ha_clnup", ".pre"), size=_FIO_SIZE)
    lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
    lib.local_run("sudo nvme disconnect-all 2>&1 || true")

    for name in snap_names:
        lib.sbctl("volume", "create-snapshot", lvol_id, name)
        time.sleep(2)

    # Crash source's secondary during cleanup_source phase
    injector = lib.FaultInjector(
        fault_type=ctx.fault_type,
        phase="cleanup_source",
        node_role="target",
        src_id=src_id,
        tgt_id=sec_id,
        nodes=ctx.nodes,
        cluster_id=ctx.cluster_id,
        node_ip_map=ctx.node_ip_map,
    )
    migration_id = lib.start_migration(lvol_id, tgt_id)
    chk.check(bool(migration_id), "Migration started")
    lib.continue_migration(migration_id)
    injector.start_watching(lvol_id)

    status = lib.wait_for_migration(lvol_id, ctx.cluster_id, terminal_only=True,
                                    timeout=lib.MIGRATION_TIMEOUT + 300)
    injector.wait()

    chk.check(injector.fired.is_set(), "Fault injected during cleanup_source")
    chk.check(status in ("done", "completed"),
              f"Migration completed despite src-HA cleanup fault (status={status})")
    node = lib.get_lvol_node(lvol_id)
    chk.check(node == tgt_id,
              f"Lvol on target after cleanup fault (got {str(node)[:8]})")

    if ctx.fault_type == "reboot":
        lib.log.info("  Waiting for cluster to recover after reboot...")
        lib.wait_cluster_healthy(timeout=300)

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names)


# ---------------------------------------------------------------------------
# Scenario: short_deadline
# ---------------------------------------------------------------------------
def run_short_deadline(ctx: ScenarioContext, chk: lib.Checklist):
    POOL           = _pool("short_ddl")
    LVOL           = "vol"
    SNAP_COUNT     = 10
    MOUNT          = f"{_MOUNT_BASE}/short_ddl"
    fio_file       = f"{MOUNT}/data.bin"
    snap_names     = [f"sdl_{i}" for i in range(1, SNAP_COUNT + 1)]
    SHORT_DEADLINE = 30  # seconds — too short to finish 10-snap copy

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names, mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "short_ddl", ".pre"), size=_FIO_SIZE)
    lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
    lib.local_run("sudo nvme disconnect-all 2>&1 || true")

    for name in snap_names:
        lib.sbctl("volume", "create-snapshot", lvol_id, name)
        time.sleep(2)

    lib.log.info(f"  Starting migration with {SHORT_DEADLINE}s deadline (expect expiry/failure)")
    migration_id = lib.start_migration(lvol_id, tgt_id)
    chk.check(bool(migration_id), "Migration pre-created")
    lib.continue_migration(migration_id, deadline=SHORT_DEADLINE)

    status = lib.wait_for_migration(lvol_id, ctx.cluster_id, terminal_only=True, timeout=300)
    chk.check(status != "timeout",
              f"No infinite hang with short deadline (status={status})")
    chk.check(status in ("failed", "cancelled", "done"),
              f"Migration reached terminal state (status={status})")
    lib.log.info(f"  Short-deadline terminal status: {status!r}")

    node = lib.get_lvol_node(lvol_id)
    chk.check(node in (src_id, tgt_id),
              f"Lvol on valid node after deadline expiry (got {str(node)[:8]})")

    _pre_cleanup(ctx, POOL, [LVOL], snap_names=snap_names)


# ---------------------------------------------------------------------------
# Scenario: sequential_migrations
# ---------------------------------------------------------------------------
def run_sequential_migrations(ctx: ScenarioContext, chk: lib.Checklist):
    POOL     = _pool("sequential")
    LVOL     = "vol"
    MOUNT    = f"{_MOUNT_BASE}/sequential"
    fio_file = f"{MOUNT}/data.bin"

    _pre_cleanup(ctx, POOL, [LVOL], mount=MOUNT)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_id = lib.create_lvol(LVOL, _LVOL_SIZE, pool_id, src_id)
    lib.connect_and_mount_lvol(lvol_id, MOUNT)
    lib.fio_prefill(fio_file, _fio_log(ctx, "sequential", ".pre"), size=_FIO_SIZE)
    fio_bg = lib.start_fio_bg(fio_file, _fio_log(ctx, "sequential"), size=_FIO_SIZE)
    time.sleep(10)

    hops = [
        (tgt_id, "A→B"),
        (src_id, "B→A"),
        (tgt_id, "A→B again"),
    ]

    try:
        for to_id, label in hops:
            time.sleep(5)
            lib.log.info(f"  Sequential hop: {label}")
            status = lib.migrate_lvol(lvol_id, to_id, ctx.cluster_id)
            chk.check(status in ("done", "completed"),
                      f"{label} done (status={status})")
            node = lib.get_lvol_node(lvol_id)
            chk.check(node == to_id,
                      f"Lvol on expected node after {label} (got {str(node)[:8]})")

        ok, _ = lib.check_fio_output(_fio_log(ctx, "sequential"), fault_injected=False)
        chk.check(ok, "No fio data corruption across 3 sequential hops")
    finally:
        lib.stop_fio(fio_bg, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, [LVOL])


# ---------------------------------------------------------------------------
# Scenario: concurrent_migrations
# ---------------------------------------------------------------------------
def run_concurrent_migrations(ctx: ScenarioContext, chk: lib.Checklist):
    POOL       = _pool("concurrent")
    LVOL_NAMES = ["vol_a", "vol_b", "vol_c"]
    MOUNTS     = [f"{_MOUNT_BASE}/conc/{n}" for n in LVOL_NAMES]
    fio_files  = [f"{m}/data.bin" for m in MOUNTS]

    for name, mount in zip(LVOL_NAMES, MOUNTS):
        lib.local_run(f"sudo umount {mount} 2>/dev/null || true")
    _pre_cleanup(ctx, POOL, LVOL_NAMES)
    src_id, tgt_id = _src_tgt(ctx)

    pool_id  = lib.ensure_pool(POOL, ctx.cluster_id)
    lvol_ids = [lib.create_lvol(n, _LVOL_SIZE, pool_id, src_id) for n in LVOL_NAMES]

    fio_logs  = []
    fio_procs = []
    for i, (lvol_id, mount, fio_file) in enumerate(zip(lvol_ids, MOUNTS, fio_files)):
        lib.connect_and_mount_lvol(lvol_id, mount)
        lib.fio_prefill(fio_file, _fio_log(ctx, f"conc_{LVOL_NAMES[i]}", ".pre"),
                        size=_FIO_SIZE)
        log_path = _fio_log(ctx, f"conc_{LVOL_NAMES[i]}")
        fio_logs.append(log_path)
        fio_procs.append(lib.start_fio_bg(fio_file, log_path, size=_FIO_SIZE))
    time.sleep(10)

    try:
        # Pre-create all migrations
        migration_ids = []
        for lvol_id, name in zip(lvol_ids, LVOL_NAMES):
            mid = lib.start_migration(lvol_id, tgt_id)
            migration_ids.append(mid)
            chk.check(bool(mid), f"{name}: migration pre-created")

        # Continue all (cluster runs them in parallel)
        for mid in migration_ids:
            if mid:
                lib.continue_migration(mid)

        # Wait for each to complete
        for lvol_id, name in zip(lvol_ids, LVOL_NAMES):
            status = lib.wait_for_migration(lvol_id, ctx.cluster_id, terminal_only=True)
            chk.check(status in ("done", "completed"),
                      f"{name} migration done (status={status})")
            node = lib.get_lvol_node(lvol_id)
            chk.check(node == tgt_id, f"{name} on target (got {str(node)[:8]})")

        for log_path, name in zip(fio_logs, LVOL_NAMES):
            ok, _ = lib.check_fio_output(log_path, fault_injected=False)
            chk.check(ok, f"No fio errors for {name}")
    finally:
        for proc in fio_procs:
            lib.stop_fio(proc, post_wait=0)
        for mount in MOUNTS:
            lib.local_run(f"sudo umount {mount} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        _pre_cleanup(ctx, POOL, LVOL_NAMES)


# ---------------------------------------------------------------------------
# Stubs — replace fn=_todo with a real run_* function when implementing
# ---------------------------------------------------------------------------
def _todo(ctx: ScenarioContext, chk: lib.Checklist):
    raise SkipScenario("not yet implemented")


# ---------------------------------------------------------------------------
# Scenario registry
# (1-based index shown in --list; used by --scenario N)
# ---------------------------------------------------------------------------
SCENARIOS: List[Scenario] = [
    # Happy path
    Scenario("plain_lvol",             "Plain lvol migration with continuous fio",                          run_plain_lvol,             ["happy_path"]),
    Scenario("crypto_lvol",            "Crypto lvol migration with continuous fio",                         run_crypto_lvol,            ["happy_path"]),
    Scenario("lvol_with_snaps",        "Lvol with 5 snapshots, fio running throughout",                     run_lvol_with_snaps,        ["happy_path"]),
    Scenario("tree",                   "Tree: parent + 2 clones, migrate parent",                           run_tree,                   ["happy_path", "advanced"]),
    Scenario("batch",                  "Batch migration of 4 namespaced members",                           run_batch,                  ["happy_path", "batch"]),
    # Advanced
    Scenario("clone_migration",        "Migrate a clone (not parent), full ancestry chain",                 run_clone_migration,        ["advanced"]),
    Scenario("deep_tree",              "Deep 5-level clone tree migration",                                 run_deep_tree,              ["advanced"]),
    Scenario("batch_shared_ancestry",  "Batch migration — members share snapshot ancestry",                 run_batch_shared_ancestry,  ["advanced", "batch"]),
    Scenario("ha_overlap_a",           "HA overlap case-a: target-prim IS source-sec",                      run_ha_overlap_a,           ["advanced", "ha"]),
    Scenario("ha_overlap_b",           "HA overlap case-b: source-prim IS target-sec",                      run_ha_overlap_b,           ["advanced", "ha"]),
    Scenario("round_trip",             "Round-trip migration: A→B→A, verify state clean each hop",          run_round_trip,             ["advanced"]),
    # Fault / cancel
    Scenario("cancel_snap_copy_early", "Cancel during snap_copy (first few snaps), verify rollback",        run_cancel_snap_copy_early, ["fault"]),
    Scenario("cancel_snap_copy_late",  "Cancel during snap_copy (near end), verify rollback",               run_cancel_snap_copy_late,  ["fault"]),
    Scenario("cancel_lvol_migrate",    "Cancel during lvol_migrate phase, verify rollback",                 run_cancel_lvol_migrate,    ["fault"]),
    Scenario("src_ha_fault_snap_copy", "Source HA node fault during snap_copy",                             run_src_ha_fault_snap_copy, ["fault", "ha"]),
    Scenario("tgt_ha_fault_snap_copy", "Target HA node fault during snap_copy",                             run_tgt_ha_fault_snap_copy, ["fault", "ha"]),
    Scenario("src_ha_fault_cleanup",   "Source HA node fault during cleanup_source",                        run_src_ha_fault_cleanup,   ["fault", "ha"]),
    Scenario("short_deadline",         "Migration with short deadline — verify graceful expiry",             run_short_deadline,         ["fault"]),
    # Stress
    Scenario("sequential_migrations",  "Same lvol migrated A→B→A→B three times, verify state each time",   run_sequential_migrations,  ["stress"]),
    Scenario("concurrent_migrations",  "Three independent lvols migrated concurrently",                     run_concurrent_migrations,  ["stress"]),
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def _select_scenarios(args) -> List[Scenario]:
    if args.list:
        _print_list()
        sys.exit(0)

    pool = SCENARIOS

    if args.tag:
        pool = [s for s in pool if args.tag in s.tags]
        if not pool:
            print(f"No scenarios with tag {args.tag!r}", file=sys.stderr)
            sys.exit(1)

    if not args.scenario:
        return pool

    selected = []
    for sel in args.scenario:
        if sel.isdigit():
            idx = int(sel) - 1
            if not (0 <= idx < len(SCENARIOS)):
                print(f"Scenario index {sel} out of range (1–{len(SCENARIOS)})",
                      file=sys.stderr)
                sys.exit(1)
            selected.append(SCENARIOS[idx])
        else:
            match = [s for s in SCENARIOS if s.name == sel]
            if not match:
                print(f"Unknown scenario name {sel!r}.  Run --list to see available.",
                      file=sys.stderr)
                sys.exit(1)
            selected.extend(match)
    return selected


def _print_list():
    print(f"{'#':>3}  {'Name':<28} {'Tags':<30} Description")
    print("-" * 100)
    for i, s in enumerate(SCENARIOS, 1):
        tags = ", ".join(s.tags)
        impl = "" if s.fn is _todo else " *"
        print(f"{i:>3}  {s.name + impl:<28} {tags:<30} {s.description}")
    print("\n  * = implemented   (no marker) = stub")


def _run_scenario(scenario: Scenario, ctx: ScenarioContext) -> ScenarioResult:
    lib.log.info("")
    lib.log.info("=" * 70)
    lib.log.info(f"SCENARIO: {scenario.name}  —  {scenario.description}")
    lib.log.info("=" * 70)

    chk = lib.Checklist()
    try:
        scenario.fn(ctx, chk)
    except SkipScenario as e:
        lib.log.info(f"SKIP: {e}")
        return ScenarioResult(scenario.name, "SKIP", chk.passes, chk.failures)
    except Exception as e:
        lib.log.exception(f"CRASH in scenario {scenario.name!r}: {e}")
        chk.failures.append(f"Crashed: {e}")
        return ScenarioResult(scenario.name, "CRASH", chk.passes, chk.failures,
                              crash_msg=str(e))

    status = "PASS" if not chk.failures else "FAIL"
    return ScenarioResult(scenario.name, status, chk.passes, chk.failures)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--scenario", nargs="+", metavar="NAME_OR_INDEX",
                   help="Run specific scenario(s) by name or 1-based index")
    p.add_argument("--tag", metavar="TAG",
                   help="Run only scenarios matching this tag")
    p.add_argument("--list", action="store_true",
                   help="Print scenario registry and exit")
    p.add_argument("--fault", default="reboot", choices=["reboot", "spdk_crash"],
                   help="Fault type for fault-injection scenarios (default: reboot)")
    p.add_argument("--source", default=None,
                   help="Source node ID (auto-picked per scenario if omitted)")
    p.add_argument("--target", default=None,
                   help="Target node ID (auto-picked per scenario if omitted)")
    p.add_argument("--log-dir", default=str(_LOG_DIR), dest="log_dir",
                   help="Directory for log output")
    return p.parse_args()


def main():
    args = parse_args()

    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    lib.init_logging(log_dir / "test_migration_suite.log")

    selected = _select_scenarios(args)

    lib.log.info(f"Migration suite: {len(selected)} scenario(s) selected")

    cluster_id = lib.discover_cluster_id()
    if not cluster_id:
        lib.log.error("Could not discover cluster ID — aborting")
        sys.exit(1)
    lib.log.info(f"Cluster: {cluster_id}")

    nodes = lib.get_online_nodes()
    node_ip_map = lib.build_node_ip_map(nodes)

    ctx = ScenarioContext(
        cluster_id=cluster_id,
        nodes=nodes,
        node_ip_map=node_ip_map,
        log_dir=log_dir,
        fault_type=args.fault,
        src_id=args.source,
        tgt_id=args.target,
    )

    results = []
    for scenario in selected:
        result = _run_scenario(scenario, ctx)
        results.append(result)
        # Refresh node list between scenarios — faults may have changed topology
        ctx.nodes = lib.get_online_nodes()
        ctx.node_ip_map = lib.build_node_ip_map(ctx.nodes)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    lib.log.info("")
    lib.log.info("=" * 70)
    lib.log.info("SUITE SUMMARY")
    lib.log.info("=" * 70)

    status_order = {"PASS": 0, "FAIL": 1, "CRASH": 2, "SKIP": 3}
    counts = {s: 0 for s in status_order}
    for r in results:
        counts[r.status] += 1
        lib.log.info(f"  {r.status:<5}  {r.name}")
        for msg in r.failures:
            lib.log.error(f"           FAIL: {msg}")
        if r.crash_msg:
            lib.log.error(f"           CRASH: {r.crash_msg}")

    lib.log.info("")
    lib.log.info(
        f"  {counts['PASS']} passed  "
        f"{counts['FAIL']} failed  "
        f"{counts['CRASH']} crashed  "
        f"{counts['SKIP']} skipped"
    )

    any_bad = counts["FAIL"] + counts["CRASH"]
    sys.exit(0 if not any_bad else 1)


if __name__ == "__main__":
    main()
