"""
continuous_lvol_dirfill_stress.py

Race-hunting lvol stress test.

Purpose
-------
Drive the control plane hard while keeping a large inventory of lvols
distributed across storage nodes, with a rolling subset attached, mounted
and filled via fio-generated directory/file trees.  Snapshots and clones
are interleaved with ongoing I/O to surface control-plane races (the kind
that produced the recent  AttributeError: 'SnapShot' object has no
attribute 'node_id' ).

Steady-state targets (tunable at class level or via --testname)
  - LVOL_PER_NODE_MAX   = 100  lvols+clones per storage node
  - ACTIVE_PER_NODE_TGT = 15   attached+mounted lvols+clones per node
  - SNAPSHOT_INV_MAX    = 80   global snapshot inventory cap
  - Global totals are derived: inventory_max = nodes * 100

Shape of a single lvol lifecycle
  create  ->  attach (nvme connect + mkfs + mount)
          ->  fill (fio writes into a random sub-directory)
          ->  snapshot
          ->  more fills / more snapshots
          ->  some snapshots -> clone (clones re-enter the same lifecycle)
          ->  detach  ->  delete

Every stage runs concurrently through a ThreadPoolExecutor — the submit
loop keeps per-op in-flight counts near the configured targets and adds
create/delete bias so the total inventory hovers at the high-water mark.
When the cluster hits a transient error ( max_lvols_reached ,
lvol_sync_deletion_found ) we trigger forced deletes and keep going
rather than aborting, mirroring the behaviour of the existing
TestParallelLvolSnapshotCloneAPI.

Driver
------
Uses sbcli_utils (same REST surface as the existing stress tests) plus
ssh_utils for the client-side mount/unmount/fio work.  The test runs on
the mgmt node of its target cluster, reaches the client node over SSH,
and never touches the jump host.
"""

import os
import random
import string
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
from utils.common_utils import sleep_n_sec

try:
    import requests
except Exception:
    requests = None


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
def _rand_name(prefix: str) -> str:
    return f"{prefix}{generate_random_sequence(10)}_{int(time.time() * 1000) % 10_000_000}"


def _rand_dir_name() -> str:
    return "d_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------
class TestLvolDirFillStress(TestClusterBase):
    """Parallel lvol+snapshot+clone stress with directory/file fills via fio.

    Naming tag used by the runner: ``lvol_dirfill_stress``.
    """

    # ---- tunables ---------------------------------------------------------
    # Per-node targets.  Cluster deploy sets --max-lvol=100 (the per-node
    # NVMe-oF subsystem cap); we keep a 10-lvol headroom below it so
    # transient races around create don't push a node over the cluster cap.
    LVOL_PER_NODE_MAX = 90
    ACTIVE_PER_NODE_TGT = 15

    # Global snapshot cap (shared across nodes)
    SNAPSHOT_INV_MAX = 80

    # In-flight caps per op class (controls concurrency)
    CREATE_INFLIGHT = 4
    ATTACH_INFLIGHT = 4
    DETACH_INFLIGHT = 4
    FILL_INFLIGHT = 6
    SNAPSHOT_INFLIGHT = 3
    CLONE_INFLIGHT = 3
    DELETE_INFLIGHT = 4

    # Global concurrency cap across ALL ops
    MAX_TOTAL_INFLIGHT = 16

    # Sizing
    LVOL_SIZE = "5G"
    FILL_SIZE = "200M"           # size of each fill workload
    FILL_RUNTIME = 60            # max seconds per fill
    FILL_NRFILES = 4             # files per fill directory

    # Mount root on the client
    MOUNT_BASE = "/mnt/lvol_dirfill"

    # Stop controls
    STOP_FILE = "/tmp/stop_lvol_dirfill_stress"
    MAX_RUNTIME_SEC = None

    # Cancel/harvest stale futures after this many seconds
    TASK_TIMEOUT = 900

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_dirfill_stress"

        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        # storage-node -> uuid (populated in setup())
        self._storage_node_ids = []
        self._storage_node_count = 0

        # per-node counters
        # _node_lvols[node_uuid] = set of lvol_names (alive, any state)
        # _node_active[node_uuid] = set of lvol_names (attached+mounted)
        self._node_lvols = defaultdict(set)
        self._node_active = defaultdict(set)

        # Registries keyed by lvol_name.  Clones share the same registry
        # with  kind="clone" — from the control-plane point of view a
        # clone IS an lvol.
        #
        # _lvol_registry[name] = {
        #   id, node_id, kind, client, mount_path, device,
        #   attach_state:    not_attached|attaching|attached|detaching
        #   fill_state:      idle|filling|done
        #   snap_state:      none|in_progress|has_snap
        #   delete_state:    not_queued|queued|in_progress
        #   snapshots:       set(snap_name)
        #   from_snap:       snap_name (only for clones)
        # }
        self._lvol_registry = {}

        # _snap_registry[snap_name] = {
        #   snap_id, src_lvol_name, src_node_id,
        #   clone_state:   none|in_progress|has_clone
        #   delete_state:  not_queued|queued|in_progress
        #   clones:        set(clone_lvol_name)
        # }
        self._snap_registry = {}

        # Delete queues (work items are names; metadata is in registries)
        self._lvol_delete_q = deque()
        self._snap_delete_q = deque()

        # Metrics
        self._metrics = {
            "start_ts": None,
            "end_ts": None,
            "loops": 0,
            "max_workers": 0,
            "targets": {
                "lvol_per_node_max": self.LVOL_PER_NODE_MAX,
                "active_per_node_tgt": self.ACTIVE_PER_NODE_TGT,
                "snapshot_inv_max": self.SNAPSHOT_INV_MAX,
                "create_inflight": self.CREATE_INFLIGHT,
                "attach_inflight": self.ATTACH_INFLIGHT,
                "detach_inflight": self.DETACH_INFLIGHT,
                "fill_inflight": self.FILL_INFLIGHT,
                "snapshot_inflight": self.SNAPSHOT_INFLIGHT,
                "clone_inflight": self.CLONE_INFLIGHT,
                "delete_inflight": self.DELETE_INFLIGHT,
            },
            "attempts": {},
            "success": {},
            "failures": {},
            "counts": {
                "lvols_created": 0,
                "clones_created": 0,
                "snapshots_created": 0,
                "lvols_deleted": 0,
                "clones_deleted": 0,
                "snapshots_deleted": 0,
                "attaches": 0,
                "detaches": 0,
                "fills": 0,
            },
            "peak_inflight": {
                "create": 0, "attach": 0, "detach": 0, "fill": 0,
                "snapshot": 0, "clone": 0, "delete": 0,
            },
            "failure_info": None,
        }
        for op in ("create_lvol", "create_clone", "create_snapshot",
                   "delete_lvol_tree", "delete_snapshot_tree",
                   "attach_mount", "detach_unmount", "fill_dir"):
            self._metrics["attempts"][op] = 0
            self._metrics["success"][op] = 0
            self._metrics["failures"][op] = 0
        self._metrics["failures"]["unknown"] = 0

    # ----------------------------------------------------------------------
    # metrics + failure helpers
    # ----------------------------------------------------------------------
    def _inc(self, bucket: str, key: str, n: int = 1):
        with self._lock:
            self._metrics[bucket][key] = self._metrics[bucket].get(key, 0) + n

    def _set_failure(self, op: str, exc: Exception, details: str = "",
                     ctx: dict = None, api_err: dict = None):
        with self._lock:
            if self._metrics["failure_info"] is None:
                self._metrics["failure_info"] = {
                    "op": op,
                    "exc": repr(exc),
                    "when": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "details": details,
                    "ctx": ctx or {},
                    "api_error": api_err or {},
                }
        self._stop_event.set()

    def _extract_api_error(self, e: Exception) -> dict:
        info = {"type": type(e).__name__, "msg": str(e)}
        resp = getattr(e, "response", None)
        if resp is not None:
            info["status_code"] = getattr(resp, "status_code", None)
            try:
                info["text"] = resp.text
            except Exception:
                info["text"] = "<no-text>"
            try:
                info["json"] = resp.json()
            except Exception:
                pass
        if requests is not None and isinstance(e, requests.exceptions.HTTPError):
            # fields already filled above when response exists
            return info
        return info

    def _is_recoverable_cluster_pressure(self, api_err: dict) -> bool:
        """Transient cluster/node states — keep going, bias deletes."""
        blob = ((api_err.get("text") or "") + " " + (api_err.get("msg") or "")).lower()
        return ("max lvols reached" in blob
                or "lvol sync deletion found" in blob
                or "being recreated" in blob
                or "restart in progress" in blob
                or "lvstore restart" in blob
                or "too many subsystems" in blob
                or "max subsystems reached" in blob)

    def _is_bdev_error(self, api_err: dict) -> bool:
        """Transient SPDK bdev-alloc failure — retry with a fresh name."""
        blob = ((api_err.get("text") or "") + " " + (api_err.get("msg") or "")).lower()
        return "failed to create bdev" in blob

    # ----------------------------------------------------------------------
    # Client picker
    # ----------------------------------------------------------------------
    def _pick_client(self, seed) -> str:
        clients = self.client_machines or []
        if not clients:
            raise RuntimeError("CLIENT_IP env var not set — no client host to attach lvols on")
        if isinstance(clients, str):
            clients = [c for c in clients.split() if c]
        return clients[hash(seed) % len(clients)]

    # ----------------------------------------------------------------------
    # Node balancing
    # ----------------------------------------------------------------------
    def _pick_least_loaded_node(self) -> str:
        """Return the storage node uuid with the fewest lvols+clones."""
        with self._lock:
            best = None
            best_n = None
            for nid in self._storage_node_ids:
                n = len(self._node_lvols[nid])
                if n >= self.LVOL_PER_NODE_MAX:
                    continue
                if best_n is None or n < best_n:
                    best, best_n = nid, n
            return best  # may be None if every node is full

    def _nodes_under_active_target(self) -> list:
        with self._lock:
            return [nid for nid in self._storage_node_ids
                    if len(self._node_active[nid]) < self.ACTIVE_PER_NODE_TGT]

    def _nodes_over_active_target(self) -> list:
        with self._lock:
            return [nid for nid in self._storage_node_ids
                    if len(self._node_active[nid]) > self.ACTIVE_PER_NODE_TGT]

    # ----------------------------------------------------------------------
    # ID lookups with wait
    # ----------------------------------------------------------------------
    def _wait_lvol_id(self, lvol_name: str, timeout=180, interval=5) -> str:
        sleep_n_sec(2)
        start = time.time()
        while time.time() - start < timeout:
            lid = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            if lid:
                return lid
            sleep_n_sec(interval)
        raise TimeoutError(f"lvol id not visible for {lvol_name}")

    def _wait_snapshot_id(self, snap_name: str, timeout=120, interval=5) -> str:
        sleep_n_sec(2)
        start = time.time()
        while time.time() - start < timeout:
            sid = self.sbcli_utils.get_snapshot_id(snap_name=snap_name)
            if sid:
                return sid
            sleep_n_sec(interval)
        raise TimeoutError(f"snapshot id not visible for {snap_name}")

    # ----------------------------------------------------------------------
    # Attach / detach primitives
    # ----------------------------------------------------------------------
    def _attach_mount(self, lvol_name: str, lvol_id: str, client: str, tag: str):
        """nvme connect + format + mount.  Returns (mount_path, device)."""
        connect_cmds = self.sbcli_utils.get_lvol_connect_str(lvol_name)
        if not connect_cmds:
            raise Exception(f"no connect strings for {lvol_name}")
        for cmd in connect_cmds:
            _, err = self.ssh_obj.exec_command(node=client, command=cmd)
            if err:
                raise Exception(f"nvme connect failed for {lvol_name}: {err}")

        device = None
        for _ in range(25):
            device = self.ssh_obj.get_lvol_vs_device(node=client, lvol_id=lvol_id)
            if device:
                break
            sleep_n_sec(2)
        if not device:
            raise Exception(f"no NVMe device resolved for {lvol_name} / {lvol_id}")

        mount_path = f"{self.MOUNT_BASE}/{tag}_{lvol_name}"
        self.ssh_obj.exec_command(node=client, command=f"sudo mkdir -p {mount_path}")
        self.ssh_obj.format_disk(node=client, device=device, fs_type="ext4")
        self.ssh_obj.mount_path(node=client, device=device, mount_path=mount_path)
        return mount_path, device

    def _unmount_disconnect(self, lvol_name: str, mount_path: str, lvol_id_hint: str):
        meta = self._lvol_registry.get(lvol_name)
        if not meta:
            return
        client = meta["client"]
        if mount_path:
            try:
                self.ssh_obj.unmount_path(node=client, device=mount_path)
            except Exception as e:
                self.logger.warning(f"[unmount] {lvol_name}: {e}")
        lvol_id = lvol_id_hint or self.sbcli_utils.get_lvol_id(lvol_name)
        if not lvol_id:
            return  # already gone
        details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id) or []
        if details:
            nqn = details[0].get("nqn")
            if nqn:
                try:
                    self.ssh_obj.disconnect_nvme(node=client, nqn_grep=nqn)
                except Exception as e:
                    self.logger.warning(f"[disconnect] {lvol_name}: {e}")

    # ----------------------------------------------------------------------
    # Fill primitive (fio)
    # ----------------------------------------------------------------------
    def _run_fill(self, client: str, mount_path: str, job_name: str):
        """Wipe old fill data, then run a bounded fio write+verify job.

        Uses a single fixed subdirectory per mount (``workdir``) so the
        filesystem doesn't accumulate data across fill cycles.  Before
        each fill the old files are removed — only the fio output from
        the *current* cycle lives on disk at any time.

        Entire command runs under ``sudo sh -c`` so the shell redirect
        lands in a root-owned dir.
        """
        subdir = f"{mount_path}/workdir"
        log_path = f"{subdir}/fio.log"
        fio_core = (
            # Remove old fill data, keep the directory itself
            f"mkdir -p {subdir} && find {subdir} -mindepth 1 -delete && "
            f"fio --name={job_name} --directory={subdir} "
            f"--ioengine=libaio --direct=1 --iodepth=4 "
            f"--rw=randwrite --bs=64K --size={self.FILL_SIZE} "
            f"--nrfiles={self.FILL_NRFILES} --numjobs=1 "
            f"--verify=md5 --verify_fatal=1 "
            f"--runtime={self.FILL_RUNTIME} --time_based=0 "
            f"--group_reporting --output-format=terse "
            f"> {log_path} 2>&1"
        )
        cmd = f"sudo sh -c '{fio_core}'"
        out, err = self.ssh_obj.exec_command(node=client, command=cmd,
                                             timeout=self.FILL_RUNTIME + 60,
                                             max_retries=1)
        if err:
            raise Exception(f"fill fio failed on {client}: {err}")
        return subdir

    # ----------------------------------------------------------------------
    # Task: create lvol
    # ----------------------------------------------------------------------
    def _task_create_lvol(self, idx: int):
        """Create a fresh lvol, pinned to the least-loaded node.

        On a recoverable (transient) per-node error (LVStore being recreated,
        max-lvols, sync-deletion) we retry on a DIFFERENT node a few times
        before giving up — one node in restart shouldn't fail the whole test.
        """
        CREATE_RETRY_MAX = 5
        tried_nodes = set()
        lvol_name = _rand_name("lvl")
        self._inc("attempts", "create_lvol", 1)

        for attempt in range(CREATE_RETRY_MAX):
            node_id = None
            with self._lock:
                # pick least-loaded node that we haven't tried yet
                best, best_n = None, None
                for nid in self._storage_node_ids:
                    if nid in tried_nodes:
                        continue
                    n = len(self._node_lvols[nid])
                    if n >= self.LVOL_PER_NODE_MAX:
                        continue
                    if best_n is None or n < best_n:
                        best, best_n = nid, n
                node_id = best
            if node_id is None:
                self.logger.info("[create_lvol] no eligible node left to retry; giving up this attempt")
                return None

            tried_nodes.add(node_id)
            ctx = {"lvol_name": lvol_name, "host_id": node_id,
                   "idx": idx, "attempt": attempt + 1}

            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name,
                    pool_name=self.pool_name,
                    size=self.LVOL_SIZE,
                    distr_ndcs=self.ndcs, distr_npcs=self.npcs,
                    distr_bs=self.bs, distr_chunk_bs=self.chunk_bs,
                    host_id=node_id,
                    retry=1,
                )
                break  # success — exit retry loop
            except Exception as e:
                api_err = self._extract_api_error(e)
                if self._is_recoverable_cluster_pressure(api_err):
                    self.logger.warning(
                        f"[create_lvol] transient on node {node_id[:8]} "
                        f"(attempt {attempt + 1}/{CREATE_RETRY_MAX}): {api_err.get('msg')}"
                    )
                    # bias toward deletes in case we're out of space
                    if "max lvols" in (api_err.get("text") or "").lower():
                        self._force_enqueue_lvol_deletes()
                    continue  # try another node
                if self._is_bdev_error(api_err):
                    # Transient SPDK bdev allocation miss — retry with a fresh
                    # name but let _pick next loop pick a (potentially different)
                    # node too.  Don't mark this node as tried, since the failure
                    # is name/bdev-level, not node-level.
                    old = lvol_name
                    lvol_name = _rand_name("lvl")
                    tried_nodes.discard(node_id)  # allow same node again
                    self.logger.warning(
                        f"[create_lvol] bdev transient (attempt {attempt + 1}/{CREATE_RETRY_MAX}) "
                        f"{old} -> {lvol_name}: {api_err.get('msg')}"
                    )
                    sleep_n_sec(2)
                    continue
                self._inc("failures", "create_lvol", 1)
                self._set_failure("create_lvol", e, "api failed", ctx, api_err)
                raise
        else:
            # all retries exhausted on transient errors — non-fatal skip
            self.logger.warning(f"[create_lvol] exhausted retries for {lvol_name}; skipping")
            self._inc("failures", "create_lvol", 1)
            return None

        lvol_id = self._wait_lvol_id(lvol_name)

        with self._lock:
            self._lvol_registry[lvol_name] = {
                "id": lvol_id, "node_id": node_id, "kind": "lvol",
                "client": None, "mount_path": None, "device": None,
                "attach_state": "not_attached", "fill_state": "idle",
                "snap_state": "none", "delete_state": "not_queued",
                "snapshots": set(), "from_snap": None,
            }
            self._node_lvols[node_id].add(lvol_name)
            self._metrics["counts"]["lvols_created"] += 1

        self._inc("success", "create_lvol", 1)
        self.logger.info(f"[create_lvol] ok {lvol_name} on node {node_id[:8]}")
        return lvol_name

    # ----------------------------------------------------------------------
    # Task: attach + mount an existing detached lvol
    # ----------------------------------------------------------------------
    def _task_attach_mount(self, lvol_name: str):
        self._inc("attempts", "attach_mount", 1)
        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
            if not meta or meta["attach_state"] != "attaching" or meta["delete_state"] != "not_queued":
                # state moved underneath us
                self._inc("failures", "attach_mount", 1)
                return
            lvol_id = meta["id"]
            node_id = meta["node_id"]
            kind = meta["kind"]

        client = self._pick_client(lvol_name)
        try:
            mount_path, device = self._attach_mount(lvol_name, lvol_id, client, tag=kind)
        except Exception as e:
            with self._lock:
                meta = self._lvol_registry.get(lvol_name)
                if meta:
                    meta["attach_state"] = "not_attached"
            self._inc("failures", "attach_mount", 1)
            self.logger.warning(f"[attach_mount] {lvol_name}: {e}")
            # non-fatal — the lvol can be retried or deleted
            return

        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
            if meta:
                meta["attach_state"] = "attached"
                meta["client"] = client
                meta["mount_path"] = mount_path
                meta["device"] = device
                self._node_active[node_id].add(lvol_name)
                self._metrics["counts"]["attaches"] += 1
        self._inc("success", "attach_mount", 1)

    # ----------------------------------------------------------------------
    # Task: detach + unmount
    # ----------------------------------------------------------------------
    def _task_detach_unmount(self, lvol_name: str):
        self._inc("attempts", "detach_unmount", 1)
        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
            if not meta or meta["attach_state"] != "detaching":
                self._inc("failures", "detach_unmount", 1)
                return
            lvol_id = meta["id"]
            mount_path = meta["mount_path"]
            node_id = meta["node_id"]

        try:
            self._unmount_disconnect(lvol_name, mount_path, lvol_id)
        except Exception as e:
            self.logger.warning(f"[detach_unmount] {lvol_name}: {e}")
            self._inc("failures", "detach_unmount", 1)
            # continue — record it as detached anyway to avoid a stuck entry
        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
            if meta:
                meta["attach_state"] = "not_attached"
                meta["client"] = None
                meta["mount_path"] = None
                meta["device"] = None
                self._node_active[node_id].discard(lvol_name)
                self._metrics["counts"]["detaches"] += 1
        self._inc("success", "detach_unmount", 1)

    # ----------------------------------------------------------------------
    # Task: fill a random directory with fio
    # ----------------------------------------------------------------------
    def _task_fill(self, lvol_name: str):
        self._inc("attempts", "fill_dir", 1)
        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
            if not meta or meta["fill_state"] != "filling" or meta["attach_state"] != "attached":
                self._inc("failures", "fill_dir", 1)
                return
            client = meta["client"]
            mount_path = meta["mount_path"]

        job_name = f"fill_{lvol_name}_{int(time.time())}"
        try:
            self._run_fill(client, mount_path, job_name)
        except Exception as e:
            self.logger.warning(f"[fill] {lvol_name}: {e}")
            with self._lock:
                meta = self._lvol_registry.get(lvol_name)
                if meta:
                    meta["fill_state"] = "idle"  # allow retry
            self._inc("failures", "fill_dir", 1)
            return

        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
            if meta:
                meta["fill_state"] = "done"
                self._metrics["counts"]["fills"] += 1
        self._inc("success", "fill_dir", 1)

    # ----------------------------------------------------------------------
    # Task: snapshot an lvol that has a completed fill
    # ----------------------------------------------------------------------
    def _task_create_snapshot(self, lvol_name: str):
        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
            if not meta or meta["snap_state"] != "in_progress" or meta["delete_state"] != "not_queued":
                return
            lvol_id = meta["id"]
            node_id = meta["node_id"]

        snap_name = _rand_name("snap")
        self._inc("attempts", "create_snapshot", 1)
        ctx = {"snap_name": snap_name, "src_lvol_name": lvol_name, "src_lvol_id": lvol_id}

        try:
            self.sbcli_utils.add_snapshot(lvol_id=lvol_id, snapshot_name=snap_name, retry=1)
        except Exception as e:
            api_err = self._extract_api_error(e)
            self._inc("failures", "create_snapshot", 1)
            with self._lock:
                m = self._lvol_registry.get(lvol_name)
                if m and m["snap_state"] == "in_progress":
                    m["snap_state"] = "has_snap" if m["snapshots"] else "none"
            if self._is_recoverable_cluster_pressure(api_err):
                self.logger.warning(f"[snapshot] recoverable pressure on {snap_name}")
                self._force_enqueue_lvol_deletes()
                raise
            self._set_failure("create_snapshot", e, "api failed", ctx, api_err)
            raise

        snap_id = self._wait_snapshot_id(snap_name)
        with self._lock:
            self._snap_registry[snap_name] = {
                "snap_id": snap_id, "src_lvol_name": lvol_name,
                "src_node_id": node_id,
                "clone_state": "none", "delete_state": "not_queued",
                "clones": set(),
            }
            m = self._lvol_registry.get(lvol_name)
            if m:
                m["snapshots"].add(snap_name)
                m["snap_state"] = "has_snap"
                # allow another fill now
                if m["fill_state"] == "done":
                    m["fill_state"] = "idle"
            self._metrics["counts"]["snapshots_created"] += 1

        self._inc("success", "create_snapshot", 1)
        self.logger.info(f"[snapshot] ok {snap_name} <- {lvol_name}")

    # ----------------------------------------------------------------------
    # Task: clone a snapshot into a fresh lvol
    # ----------------------------------------------------------------------
    def _task_create_clone(self, snap_name: str):
        with self._lock:
            sm = self._snap_registry.get(snap_name)
            if not sm or sm["delete_state"] != "not_queued" or sm["clone_state"] != "in_progress":
                return
            snap_id = sm["snap_id"]
            src_node_id = sm["src_node_id"]
            src_lvol = sm["src_lvol_name"]
            lm = self._lvol_registry.get(src_lvol)
            if lm and lm["delete_state"] != "not_queued":
                sm["clone_state"] = "has_clone" if sm["clones"] else "none"
                return

        # Clones land on their source snapshot's node (cluster enforces this)
        CLONE_RETRY_MAX = 7
        clone_name = _rand_name("cln")
        self._inc("attempts", "create_clone", 1)
        ctx = {"clone_name": clone_name, "snap_name": snap_name, "snap_id": snap_id}

        succeeded = False
        last_err = None
        for attempt in range(CLONE_RETRY_MAX):
            try:
                self.sbcli_utils.add_clone(snapshot_id=snap_id, clone_name=clone_name, retry=1)
                succeeded = True
                break
            except Exception as e:
                api_err = self._extract_api_error(e)
                last_err = api_err
                if self._is_recoverable_cluster_pressure(api_err):
                    self.logger.warning(
                        f"[clone] transient pressure (attempt {attempt + 1}/{CLONE_RETRY_MAX}) "
                        f"{clone_name}: {api_err.get('msg')}"
                    )
                    self._force_enqueue_lvol_deletes()
                    break  # give up on this snapshot for now
                if self._is_bdev_error(api_err):
                    old = clone_name
                    clone_name = _rand_name("cln")
                    ctx["clone_name"] = clone_name
                    self.logger.warning(
                        f"[clone] bdev transient (attempt {attempt + 1}/{CLONE_RETRY_MAX}) "
                        f"{old} -> {clone_name}: {api_err.get('msg')}"
                    )
                    sleep_n_sec(2)
                    continue
                # unknown error — fatal
                self._inc("failures", "create_clone", 1)
                with self._lock:
                    sm2 = self._snap_registry.get(snap_name)
                    if sm2 and sm2["clone_state"] == "in_progress":
                        sm2["clone_state"] = "has_clone" if sm2["clones"] else "none"
                self._set_failure("create_clone", e, "api failed", ctx, api_err)
                raise

        if not succeeded:
            # either hit recoverable pressure (broke out) or exhausted retries
            self._inc("failures", "create_clone", 1)
            with self._lock:
                sm2 = self._snap_registry.get(snap_name)
                if sm2 and sm2["clone_state"] == "in_progress":
                    sm2["clone_state"] = "has_clone" if sm2["clones"] else "none"
            self.logger.warning(
                f"[clone] failed after {CLONE_RETRY_MAX} attempts for snap={snap_name}; "
                f"last_err={last_err.get('msg') if last_err else 'n/a'}"
            )
            return  # non-fatal

        clone_id = self._wait_lvol_id(clone_name)
        with self._lock:
            self._lvol_registry[clone_name] = {
                "id": clone_id, "node_id": src_node_id, "kind": "clone",
                "client": None, "mount_path": None, "device": None,
                "attach_state": "not_attached", "fill_state": "idle",
                "snap_state": "none", "delete_state": "not_queued",
                "snapshots": set(), "from_snap": snap_name,
            }
            self._node_lvols[src_node_id].add(clone_name)
            sm = self._snap_registry.get(snap_name)
            if sm:
                sm["clones"].add(clone_name)
                sm["clone_state"] = "has_clone"
            self._metrics["counts"]["clones_created"] += 1

        self._inc("success", "create_clone", 1)
        self.logger.info(f"[clone] ok {clone_name} <- {snap_name}")

    # ----------------------------------------------------------------------
    # Delete tree primitives
    # ----------------------------------------------------------------------
    def _delete_lvol_only(self, lvol_name: str):
        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
        if not meta:
            return
        # must be detached first
        if meta["attach_state"] == "attached":
            self._unmount_disconnect(lvol_name, meta["mount_path"], meta["id"])
        node_id = meta["node_id"]

        try:
            # Short wait (max_attempt=6 → 30 s) so a slow in_deletion doesn't
            # block a thread-pool worker and deadlock the executor.
            self.sbcli_utils.delete_lvol(lvol_name=lvol_name,
                                         max_attempt=6, skip_error=True)
        except Exception as e:
            api_err = self._extract_api_error(e)
            if self._is_recoverable_cluster_pressure(api_err):
                self.logger.warning(f"[delete_lvol] transient on {lvol_name}: {api_err.get('msg')}")
                return
            self._set_failure("delete_lvol_tree", e, "lvol delete failed",
                              {"lvol_name": lvol_name}, api_err)
            raise
        with self._lock:
            self._lvol_registry.pop(lvol_name, None)
            self._node_lvols[node_id].discard(lvol_name)
            self._node_active[node_id].discard(lvol_name)
            kind = meta["kind"]
            self._metrics["counts"]["lvols_deleted"] += 1
            if kind == "clone":
                self._metrics["counts"]["clones_deleted"] += 1

    def _delete_snapshot_only(self, snap_name: str, snap_id: str):
        # Fire the DELETE but don't spin waiting for the cluster to purge it
        # from the listing — the registry is our source of truth.  Using
        # skip_error=True + max_attempt=3 (15 s max) so a stuck snapshot
        # doesn't block a thread-pool worker for minutes and deadlock the
        # executor.
        try:
            self.sbcli_utils.delete_snapshot(
                snap_id=snap_id, snap_name=snap_name,
                max_attempt=3, skip_error=True,
            )
        except Exception as e:
            api_err = self._extract_api_error(e)
            if self._is_recoverable_cluster_pressure(api_err):
                self.logger.warning(f"[delete_snapshot] transient on {snap_name}: {api_err.get('msg')}")
                # non-fatal — snapshot stays in registry, will be retried
                return
            self._set_failure("delete_snapshot_tree", e, "snapshot delete failed",
                              {"snap_name": snap_name, "snap_id": snap_id}, api_err)
            raise
        with self._lock:
            self._snap_registry.pop(snap_name, None)
            self._metrics["counts"]["snapshots_deleted"] += 1

    def _task_delete_snapshot_tree(self, snap_name: str):
        """Delete all clones, then the snapshot itself."""
        self._inc("attempts", "delete_snapshot_tree", 1)
        with self._lock:
            sm = self._snap_registry.get(snap_name)
            if not sm:
                self._inc("success", "delete_snapshot_tree", 1)
                return
            sm["delete_state"] = "in_progress"
            snap_id = sm["snap_id"]

        # Wait for any in-flight clone creation
        for _ in range(60):
            with self._lock:
                sm2 = self._snap_registry.get(snap_name)
                if not sm2 or sm2["clone_state"] != "in_progress":
                    break
            sleep_n_sec(1)

        with self._lock:
            sm = self._snap_registry.get(snap_name)
            tracked = set(sm["clones"]) if sm else set()
            extra = {cn for cn, m in self._lvol_registry.items()
                     if m.get("from_snap") == snap_name and cn not in tracked}
            clones = list(tracked | extra)

        for cn in clones:
            try:
                self._delete_lvol_only(cn)
            except Exception:
                return
            with self._lock:
                sm = self._snap_registry.get(snap_name)
                if sm:
                    sm["clones"].discard(cn)

        try:
            self._delete_snapshot_only(snap_name, snap_id)
        except Exception:
            return

        # unlink from source lvol
        with self._lock:
            for m in self._lvol_registry.values():
                m["snapshots"].discard(snap_name)
        self._inc("success", "delete_snapshot_tree", 1)

    def _task_delete_lvol_tree(self, lvol_name: str):
        """Delete all snapshots (+their clones), then the lvol."""
        self._inc("attempts", "delete_lvol_tree", 1)
        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
            if not meta:
                self._inc("success", "delete_lvol_tree", 1)
                return
            meta["delete_state"] = "in_progress"

        # Wait for in-flight snapshot creation
        for _ in range(60):
            with self._lock:
                m = self._lvol_registry.get(lvol_name)
                if not m or m["snap_state"] != "in_progress":
                    break
            sleep_n_sec(1)

        with self._lock:
            m = self._lvol_registry.get(lvol_name)
            tracked = set(m["snapshots"]) if m else set()
            extra = {sn for sn, sm in self._snap_registry.items()
                     if sm["src_lvol_name"] == lvol_name and sn not in tracked}
            snap_names = list(tracked | extra)

        for sn in snap_names:
            self._task_delete_snapshot_tree(sn)

        try:
            self._delete_lvol_only(lvol_name)
        except Exception:
            return
        self._inc("success", "delete_lvol_tree", 1)

    # ----------------------------------------------------------------------
    # Delete enqueue policy
    # ----------------------------------------------------------------------
    def _force_enqueue_lvol_deletes(self):
        """Aggressively queue lvol deletes when the cluster pushes back."""
        with self._lock:
            n = 0
            for ln, m in list(self._lvol_registry.items()):
                if m["delete_state"] == "not_queued" and m["attach_state"] == "not_attached":
                    m["delete_state"] = "queued"
                    self._lvol_delete_q.append(ln)
                    n += 1
                    if n >= self.DELETE_INFLIGHT * 2:
                        break
        self.logger.warning(f"[force_delete] enqueued {n} lvols under cluster pressure")

    def _maybe_enqueue_deletes(self):
        """Keep per-node inventory at or below target high-water."""
        with self._lock:
            # Node-level pruning
            for nid in self._storage_node_ids:
                count = len(self._node_lvols[nid])
                if count <= self.LVOL_PER_NODE_MAX:
                    continue
                excess = count - self.LVOL_PER_NODE_MAX
                queued_here = 0
                # prefer detached, snapshotted lvols (full trees)
                for ln, m in list(self._lvol_registry.items()):
                    if queued_here >= excess:
                        break
                    if m["node_id"] != nid:
                        continue
                    if m["delete_state"] != "not_queued":
                        continue
                    if m["attach_state"] == "not_attached" and m["snap_state"] == "has_snap":
                        m["delete_state"] = "queued"
                        self._lvol_delete_q.append(ln)
                        queued_here += 1
                for ln, m in list(self._lvol_registry.items()):
                    if queued_here >= excess:
                        break
                    if m["node_id"] != nid or m["delete_state"] != "not_queued":
                        continue
                    if m["attach_state"] == "not_attached":
                        m["delete_state"] = "queued"
                        self._lvol_delete_q.append(ln)
                        queued_here += 1

            # Snapshot-level pruning (global cap)
            if len(self._snap_registry) > self.SNAPSHOT_INV_MAX:
                excess = len(self._snap_registry) - self.SNAPSHOT_INV_MAX
                queued = 0
                for sn, sm in list(self._snap_registry.items()):
                    if queued >= excess:
                        break
                    if sm["delete_state"] == "not_queued":
                        sm["delete_state"] = "queued"
                        self._snap_delete_q.append(sn)
                        queued += 1

            # Orphan snapshots whose source lvol is already gone
            for sn, sm in list(self._snap_registry.items()):
                if sm["delete_state"] == "not_queued" and sm["src_lvol_name"] not in self._lvol_registry:
                    sm["delete_state"] = "queued"
                    self._snap_delete_q.append(sn)

    # ----------------------------------------------------------------------
    # Submitters
    # ----------------------------------------------------------------------
    def _submit_creates(self, ex, fut: dict, idx_counter: dict):
        while not self._stop_event.is_set() and len(fut) < self.CREATE_INFLIGHT:
            node = self._pick_least_loaded_node()
            if node is None:
                return
            idx = idx_counter["idx"]
            idx_counter["idx"] += 1
            f = ex.submit(self._task_create_lvol, idx)
            fut[f] = time.time()

    def _submit_attaches(self, ex, fut: dict):
        while not self._stop_event.is_set() and len(fut) < self.ATTACH_INFLIGHT:
            cand = None
            with self._lock:
                under = [nid for nid in self._storage_node_ids
                         if len(self._node_active[nid]) < self.ACTIVE_PER_NODE_TGT]
                if not under:
                    return
                for ln, m in self._lvol_registry.items():
                    if m["delete_state"] != "not_queued":
                        continue
                    if m["attach_state"] != "not_attached":
                        continue
                    if m["node_id"] not in under:
                        continue
                    m["attach_state"] = "attaching"
                    cand = ln
                    break
            if not cand:
                return
            f = ex.submit(self._task_attach_mount, cand)
            fut[f] = time.time()

    def _submit_detaches(self, ex, fut: dict):
        while not self._stop_event.is_set() and len(fut) < self.DETACH_INFLIGHT:
            cand = None
            with self._lock:
                over = [nid for nid in self._storage_node_ids
                        if len(self._node_active[nid]) > self.ACTIVE_PER_NODE_TGT]
                if not over:
                    return
                for ln, m in self._lvol_registry.items():
                    if m["node_id"] not in over:
                        continue
                    if m["attach_state"] != "attached":
                        continue
                    if m["fill_state"] == "filling" or m["snap_state"] == "in_progress":
                        continue
                    m["attach_state"] = "detaching"
                    cand = ln
                    break
            if not cand:
                return
            f = ex.submit(self._task_detach_unmount, cand)
            fut[f] = time.time()

    def _submit_fills(self, ex, fut: dict):
        while not self._stop_event.is_set() and len(fut) < self.FILL_INFLIGHT:
            cand = None
            with self._lock:
                for ln, m in self._lvol_registry.items():
                    if m["attach_state"] != "attached":
                        continue
                    if m["delete_state"] != "not_queued":
                        continue
                    if m["fill_state"] != "idle":
                        continue
                    m["fill_state"] = "filling"
                    cand = ln
                    break
            if not cand:
                return
            f = ex.submit(self._task_fill, cand)
            fut[f] = time.time()

    def _submit_snapshots(self, ex, fut: dict):
        while not self._stop_event.is_set() and len(fut) < self.SNAPSHOT_INFLIGHT:
            if len(self._snap_registry) >= self.SNAPSHOT_INV_MAX:
                return
            cand = None
            with self._lock:
                for ln, m in self._lvol_registry.items():
                    if m["delete_state"] != "not_queued":
                        continue
                    if m["attach_state"] != "attached":
                        continue
                    if m["fill_state"] != "done":
                        continue
                    if m["snap_state"] == "in_progress":
                        continue
                    m["snap_state"] = "in_progress"
                    cand = ln
                    break
            if not cand:
                return
            f = ex.submit(self._task_create_snapshot, cand)
            fut[f] = time.time()

    def _submit_clones(self, ex, fut: dict):
        while not self._stop_event.is_set() and len(fut) < self.CLONE_INFLIGHT:
            cand = None
            with self._lock:
                # Don't pile clones onto a saturated node
                for sn, sm in self._snap_registry.items():
                    if sm["delete_state"] != "not_queued":
                        continue
                    if sm["clone_state"] == "in_progress":
                        continue
                    node = sm["src_node_id"]
                    if len(self._node_lvols[node]) >= self.LVOL_PER_NODE_MAX:
                        continue
                    # bias toward snapshots with fewer clones
                    if random.random() < 0.5 and sm["clone_state"] == "has_clone":
                        continue
                    sm["clone_state"] = "in_progress"
                    cand = sn
                    break
            if not cand:
                return
            f = ex.submit(self._task_create_clone, cand)
            fut[f] = time.time()

    def _submit_deletes(self, ex, fut: dict):
        while not self._stop_event.is_set() and len(fut) < self.DELETE_INFLIGHT:
            with self._lock:
                if self._snap_delete_q:
                    sn = self._snap_delete_q.popleft()
                    f = ex.submit(self._task_delete_snapshot_tree, sn)
                    fut[f] = time.time()
                    continue
                if self._lvol_delete_q:
                    ln = self._lvol_delete_q.popleft()
                    f = ex.submit(self._task_delete_lvol_tree, ln)
                    fut[f] = time.time()
                    continue
            return

    # ----------------------------------------------------------------------
    # Peak tracking + harvest
    # ----------------------------------------------------------------------
    def _update_peaks(self, create_f, attach_f, detach_f, fill_f, snap_f, clone_f, delete_f):
        with self._lock:
            p = self._metrics["peak_inflight"]
            p["create"] = max(p["create"], len(create_f))
            p["attach"] = max(p["attach"], len(attach_f))
            p["detach"] = max(p["detach"], len(detach_f))
            p["fill"] = max(p["fill"], len(fill_f))
            p["snapshot"] = max(p["snapshot"], len(snap_f))
            p["clone"] = max(p["clone"], len(clone_f))
            p["delete"] = max(p["delete"], len(delete_f))

    def _harvest(self, fut: dict):
        now = time.time()
        for f in [f for f in fut if f.done()]:
            del fut[f]
            try:
                f.result()
            except Exception as e:
                self.logger.warning(f"[harvest] task failed: {type(e).__name__}: {e}")
        stale = [f for f, ts in fut.items() if (now - ts) > self.TASK_TIMEOUT and not f.done()]
        for f in stale:
            f.cancel()
            fut.pop(f, None)
            self.logger.warning(f"[harvest] cancelled stale future after {self.TASK_TIMEOUT}s")

    # ----------------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------------
    def _print_summary(self):
        with self._lock:
            self._metrics["end_ts"] = time.time()
            dur = (self._metrics["end_ts"] - self._metrics["start_ts"]) if self._metrics["start_ts"] else 0
            self.logger.info("======== TEST SUMMARY (lvol dirfill stress) ========")
            self.logger.info(f"Duration (sec): {dur:.1f}")
            self.logger.info(f"Loops: {self._metrics['loops']}")
            self.logger.info(f"Targets: {self._metrics['targets']}")
            self.logger.info(f"Peak inflight: {self._metrics['peak_inflight']}")
            self.logger.info(f"Counts: {self._metrics['counts']}")
            self.logger.info(f"Attempts: {self._metrics['attempts']}")
            self.logger.info(f"Success: {self._metrics['success']}")
            self.logger.info(f"Failures: {self._metrics['failures']}")
            self.logger.info(f"Failure info: {self._metrics['failure_info']}")
            live_lvols = sum(1 for m in self._lvol_registry.values() if m["kind"] == "lvol")
            live_clones = sum(1 for m in self._lvol_registry.values() if m["kind"] == "clone")
            live_snaps = len(self._snap_registry)
            self.logger.info(
                f"Live: lvols={live_lvols} clones={live_clones} snaps={live_snaps}"
            )
            for nid in self._storage_node_ids:
                self.logger.info(
                    f"  node {nid[:8]}: total={len(self._node_lvols[nid])} "
                    f"active={len(self._node_active[nid])}"
                )
            self.logger.info("====================================================")

    # ----------------------------------------------------------------------
    # Setup override: skip TestClusterBase's NFS/log-dir assumptions which
    # are hard-coded for the jump-host-backed cluster (nfs_server=10.10.10.140).
    # This test runs on independent clusters that don't have that share.
    # ----------------------------------------------------------------------
    def setup(self):
        self.logger.info("=== TestLvolDirFillStress.setup (minimal, no NFS) ===")
        retry = 30
        while retry > 0:
            try:
                self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
                self.sbcli_utils.list_lvols()
                self.sbcli_utils.list_storage_pools()
                break
            except Exception as e:
                retry -= 1
                if retry == 0:
                    raise
                self.logger.info(f"API retry {30 - retry}/30: {e}")
                sleep_n_sec(2)

        # SSH connect to every storage node and bump aio-max-nr (fio needs headroom)
        for node in self.storage_nodes:
            self.logger.info(f"Connecting to storage node {node}")
            self.ssh_obj.connect(address=node, bastion_server_address=self.bastion_server)
            sleep_n_sec(1)
            try:
                self.ssh_obj.set_aio_max_nr(node)
            except Exception as e:
                self.logger.warning(f"set_aio_max_nr on {node} failed: {e}")

        # Client parsing: CLIENT_IP may be a single host or space-separated list
        if not self.client_machines:
            raise RuntimeError("CLIENT_IP env var is required for this test")
        self.client_machines = self.client_machines.strip().split(" ")
        for client in self.client_machines:
            self.logger.info(f"Connecting to client {client}")
            self.ssh_obj.connect(address=client, bastion_server_address=self.bastion_server)
            sleep_n_sec(1)

        self.fio_node = self.client_machines

        # Local log dir only — no NFS mount anywhere
        from datetime import datetime as _dt
        ts = _dt.now().strftime("%Y%m%d-%H%M%S")
        local_log_root = os.path.expanduser(
            os.environ.get("LOCAL_LOG_BASE", "~/stress/logs")
        )
        self.docker_logs_path = os.path.join(local_log_root, f"{self.test_name}-{ts}")
        self.log_path = os.path.join(self.docker_logs_path, "ClientLogs")
        os.makedirs(self.log_path, exist_ok=True)
        self.logger.info(f"Local log dir: {self.docker_logs_path}")

    # ----------------------------------------------------------------------
    # Main
    # ----------------------------------------------------------------------
    def run(self):
        self.logger.info("=== Starting TestLvolDirFillStress ===")

        # Storage pool
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # Discover storage nodes
        data = self.sbcli_utils.get_storage_nodes()
        self._storage_node_ids = [n["id"] for n in data.get("results", [])]
        self._storage_node_count = len(self._storage_node_ids)
        if self._storage_node_count == 0:
            raise RuntimeError("No storage nodes discovered — cannot run stress")
        self.logger.info(
            f"Discovered {self._storage_node_count} storage nodes: "
            f"{[nid[:8] for nid in self._storage_node_ids]}"
        )
        self._metrics["targets"]["storage_nodes"] = self._storage_node_count
        self._metrics["targets"]["total_inventory_max"] = self._storage_node_count * self.LVOL_PER_NODE_MAX
        self._metrics["targets"]["total_active_target"] = self._storage_node_count * self.ACTIVE_PER_NODE_TGT

        # Prepare client mount root(s)
        clients = self.client_machines
        if isinstance(clients, str):
            clients = [c for c in clients.split() if c]
        for c in clients:
            self.ssh_obj.exec_command(node=c, command=f"sudo mkdir -p {self.MOUNT_BASE}")

        max_workers = self.MAX_TOTAL_INFLIGHT + 6
        with self._lock:
            self._metrics["start_ts"] = time.time()
            self._metrics["max_workers"] = max_workers

        create_f, attach_f, detach_f = {}, {}, {}
        fill_f, snap_f, clone_f, delete_f = {}, {}, {}, {}
        idx_counter = {"idx": 0}

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                self._submit_creates(ex, create_f, idx_counter)

                while not self._stop_event.is_set():
                    if os.path.exists(self.STOP_FILE):
                        self.logger.info(f"Stop file {self.STOP_FILE}. Stopping gracefully.")
                        break
                    if self.MAX_RUNTIME_SEC and (time.time() - self._metrics["start_ts"]) > self.MAX_RUNTIME_SEC:
                        self.logger.info("MAX_RUNTIME_SEC reached.")
                        break

                    with self._lock:
                        self._metrics["loops"] += 1

                    self._maybe_enqueue_deletes()

                    total_inflight = (len(create_f) + len(attach_f) + len(detach_f)
                                      + len(fill_f) + len(snap_f) + len(clone_f)
                                      + len(delete_f))
                    if total_inflight < self.MAX_TOTAL_INFLIGHT:
                        self._submit_creates(ex, create_f, idx_counter)
                        self._submit_attaches(ex, attach_f)
                        self._submit_detaches(ex, detach_f)
                        self._submit_fills(ex, fill_f)
                        self._submit_snapshots(ex, snap_f)
                        self._submit_clones(ex, clone_f)
                        self._submit_deletes(ex, delete_f)

                    self._update_peaks(create_f, attach_f, detach_f, fill_f, snap_f, clone_f, delete_f)
                    for fd in (create_f, attach_f, detach_f, fill_f, snap_f, clone_f, delete_f):
                        self._harvest(fd)

                    sleep_n_sec(1)

                self.logger.info("Shutting down — cancelling pending futures...")
                cancelled = 0
                for fd in (create_f, attach_f, detach_f, fill_f, snap_f, clone_f, delete_f):
                    for f in list(fd.keys()):
                        if f.cancel():
                            cancelled += 1
                        fd.pop(f, None)
                self.logger.info(f"Cancelled {cancelled} pending futures")

        finally:
            self._print_summary()

        with self._lock:
            failure_info = self._metrics["failure_info"]
        if failure_info:
            raise Exception(f"Test stopped due to failure: {failure_info}")
        raise Exception("Test stopped without failure (graceful stop).")
