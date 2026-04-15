import os
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
from utils.common_utils import sleep_n_sec

try:
    import requests
except Exception:
    requests = None


class TestParallelLvolSnapshotCloneAPI(TestClusterBase):
    """
    Continuous parallel stress until failure.

    Desired steady-state behavior:
      - Keep ~10 snapshot creates in-flight and ~10 clone creates in-flight continuously.
      - Allow snapshots inventory to be higher (e.g., 50-60) depending on delete pacing.
      - Deletion constraints:
          * Deleting LVOL: delete ALL its snapshots AND ALL clones of those snapshots first.
          * Deleting snapshot: delete ALL its clones first, then snapshot.
          * Clone is an LVOL: must unmount+disconnect before delete.
      - NO snapshot delete is attempted while clones exist.
      - Prevent snapshotting LVOLs that are queued/in-progress for delete.

    Notes:
      - This test balances create vs delete with "high-water" thresholds so snapshots/clones
        exist most of the time (not 0/0).
      - No changes to sbcli_utils.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "parallel_lvol_snapshot_clone_api_continuous_steady"

        # In-flight targets
        self.CREATE_INFLIGHT = 3
        self.SNAPSHOT_INFLIGHT = 2
        self.CLONE_INFLIGHT = 2
        self.SNAPSHOT_DELETE_TREE_INFLIGHT = 3
        self.LVOL_DELETE_TREE_INFLIGHT = 3

        # Hard cap on total concurrent operations across all types
        self.MAX_TOTAL_INFLIGHT = 5

        # Total inventory cap: lvols + snapshots + clones must not exceed this
        self.TOTAL_INVENTORY_MAX = 60
        # Start enqueuing deletes when total exceeds this
        self.TOTAL_DELETE_THRESHOLD = 30
        # Never delete below this many live (not-queued) lvols
        self.MIN_LIVE_LVOLS = 5

        # LVOL sizing
        self.LVOL_SIZE = "5G"

        # Mount base
        self.MOUNT_BASE = "/mnt/test_location"

        # Optional stop controls
        self.STOP_FILE = "/tmp/stop_api_stress"
        self.MAX_RUNTIME_SEC = None

        # Task timeout: cancel futures running longer than this (seconds)
        self.TASK_TIMEOUT = 600

        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        # Queues (work items are names; metadata is in registries)
        self._snapshot_delete_tree_q = deque()  # snap_name eligible to delete tree (clones->snap)
        self._lvol_delete_tree_q = deque()      # lvol_name eligible to delete tree (clones->snaps->lvol)

        # Registries + relations
        # lvol_registry[lvol_name] = {
        #   id, client, mount_path,
        #   snap_state: pending|in_progress|done,
        #   delete_state: not_queued|queued|in_progress|done,
        #   snapshots: set(snap_name)
        # }
        self._lvol_registry = {}

        # snap_registry[snap_name] = {
        #   snap_id, src_lvol_name,
        #   clone_state: pending|in_progress|done,
        #   delete_state: not_queued|queued|in_progress|done,
        #   clones: set(clone_name)
        # }
        self._snap_registry = {}

        # clone_registry[clone_name] = { id, client, mount_path, snap_name, delete_state }
        self._clone_registry = {}

        # Metrics
        self._metrics = {
            "start_ts": None,
            "end_ts": None,
            "loops": 0,
            "max_workers": 0,
            "targets": {
                "create_inflight": self.CREATE_INFLIGHT,
                "snapshot_inflight": self.SNAPSHOT_INFLIGHT,
                "clone_inflight": self.CLONE_INFLIGHT,
                "snapshot_delete_tree_inflight": self.SNAPSHOT_DELETE_TREE_INFLIGHT,
                "lvol_delete_tree_inflight": self.LVOL_DELETE_TREE_INFLIGHT,
                "total_inventory_max": self.TOTAL_INVENTORY_MAX,
                "total_delete_threshold": self.TOTAL_DELETE_THRESHOLD,
            },
            "attempts": {
                "create_lvol": 0,
                "create_snapshot": 0,
                "create_clone": 0,
                "delete_lvol_tree": 0,
                "delete_snapshot_tree": 0,
                "delete_lvol": 0,
                "delete_snapshot": 0,
                "connect_mount_sanity": 0,
                "unmount_disconnect": 0,
            },
            "success": {k: 0 for k in [
                "create_lvol", "create_snapshot", "create_clone",
                "delete_lvol_tree", "delete_snapshot_tree",
                "delete_lvol", "delete_snapshot",
                "connect_mount_sanity", "unmount_disconnect"
            ]},
            "failures": {k: 0 for k in [
                "create_lvol", "create_snapshot", "create_clone",
                "delete_lvol_tree", "delete_snapshot_tree",
                "delete_lvol", "delete_snapshot",
                "connect_mount_sanity", "unmount_disconnect",
                "unknown"
            ]},
            "counts": {
                "lvols_created": 0,
                "snapshots_created": 0,
                "clones_created": 0,
                "lvols_deleted": 0,
                "snapshots_deleted": 0,
                "clones_deleted": 0,
            },
            "peak_inflight": {
                "create": 0,
                "snapshot": 0,
                "clone": 0,
                "lvol_delete_tree": 0,
                "snapshot_delete_tree": 0,
            },
            "failure_info": None,
        }

    # ----------------------------
    # Metrics + failure helpers
    # ----------------------------
    def _inc(self, bucket: str, key: str, n: int = 1):
        with self._lock:
            self._metrics[bucket][key] += n

    def _set_failure(self, op: str, exc: Exception, details: str = "", ctx: dict = None, api_err: dict = None):
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

    # ----------------------------
    # Exception -> HTTP response (best-effort)
    # ----------------------------
    def _extract_api_error(self, e: Exception) -> dict:
        info = {"type": type(e).__name__, "msg": str(e)}

        if requests is not None:
            try:
                if isinstance(e, requests.exceptions.HTTPError):
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
                        return info
            except Exception:
                pass

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

        return info

    def _is_max_lvols_error(self, api_err: dict) -> bool:
        text = (api_err.get("text") or "").lower()
        msg = (api_err.get("msg") or "").lower()
        return "max lvols reached" in text or "max lvols reached" in msg

    def _is_bdev_error(self, api_err: dict) -> bool:
        text = (api_err.get("text") or "").lower()
        msg = (api_err.get("msg") or "").lower()
        return "failed to create bdev" in text or "failed to create bdev" in msg

    def _is_lvol_sync_deletion_error(self, api_err: dict) -> bool:
        text = (api_err.get("text") or "").lower()
        msg = (api_err.get("msg") or "").lower()
        return "lvol sync deletion found" in text or "lvol sync deletion found" in msg

    def _force_enqueue_deletes(self):
        """Aggressively enqueue lvol tree deletes when the cluster hits its max-lvol limit."""
        with self._lock:
            added = 0
            for ln, lm in list(self._lvol_registry.items()):
                if lm["delete_state"] == "not_queued":
                    lm["delete_state"] = "queued"
                    self._lvol_delete_tree_q.append(ln)
                    added += 1
                    if added >= self.LVOL_DELETE_TREE_INFLIGHT * 2:
                        break
        self.logger.warning(f"[max_lvols] Forced enqueue of {added} lvol tree deletes to recover from cluster max-lvol limit")

    def _api(self, op: str, ctx: dict, fn, retries: int = 10, interval: int = 5):
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                return fn()
            except Exception as e:
                last_exc = e
                api_err = self._extract_api_error(e)
                if self._is_max_lvols_error(api_err):
                    self._inc("failures", op if op in self._metrics["failures"] else "unknown", 1)
                    self.logger.warning(f"[max_lvols] op={op} ctx={ctx}: cluster hit max-lvol limit; triggering forced deletes and continuing")
                    self._force_enqueue_deletes()
                    raise  # task fails but test keeps running
                if attempt < retries:
                    self.logger.warning(f"[retry] op={op} attempt {attempt}/{retries} failed: {e}; retrying in {interval}s")
                    sleep_n_sec(interval)
                else:
                    self._inc("failures", op if op in self._metrics["failures"] else "unknown", 1)
                    self._set_failure(op=op, exc=e, details=f"api call failed after {retries} attempts", ctx=ctx, api_err=api_err)
                    raise

    # ----------------------------
    # Helpers
    # ----------------------------
    def _pick_client(self, i: int) -> str:
        return self.client_machines[i % len(self.client_machines)]

    def _active_inventory_count(self):
        """Count items not queued/in-progress for deletion."""
        with self._lock:
            lvols = sum(1 for lm in self._lvol_registry.values() if lm["delete_state"] == "not_queued")
            snaps = sum(1 for sm in self._snap_registry.values() if sm["delete_state"] == "not_queued")
            clones = sum(1 for cm in self._clone_registry.values() if cm["delete_state"] == "not_queued")
            return lvols + snaps + clones

    def _wait_lvol_id(self, lvol_name: str, timeout=300, interval=10) -> str:
        sleep_n_sec(3)  # brief initial delay — lvol rarely visible immediately
        start = time.time()
        while time.time() - start < timeout:
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            if lvol_id:
                return lvol_id
            sleep_n_sec(interval)
        raise TimeoutError(f"LVOL id not visible for {lvol_name} after {timeout}s")

    def _wait_snapshot_id(self, snap_name: str, timeout=300, interval=10) -> str:
        sleep_n_sec(3)  # brief initial delay — snapshot rarely visible immediately
        start = time.time()
        while time.time() - start < timeout:
            snap_id = self.sbcli_utils.get_snapshot_id(snap_name=snap_name)
            if snap_id:
                return snap_id
            sleep_n_sec(interval)
        raise TimeoutError(f"Snapshot id not visible for {snap_name} after {timeout}s")

    # ----------------------------
    # IO sanity
    # ----------------------------
    def _connect_format_mount_sanity(self, client: str, lvol_name: str, lvol_id: str, tag: str) -> str:
        self._inc("attempts", "connect_mount_sanity", 1)

        connect_cmds = self.sbcli_utils.get_lvol_connect_str(lvol_name)
        if not connect_cmds:
            raise Exception(f"No connect strings returned for {lvol_name}")

        for cmd in connect_cmds:
            out, err = self.ssh_obj.exec_command(node=client, command=cmd)
            if err:
                raise Exception(f"NVMe connect failed for {lvol_name} on {client}. err={err} out={out}")

        device = None
        for _ in range(25):
            device = self.ssh_obj.get_lvol_vs_device(node=client, lvol_id=lvol_id)
            if device:
                break
            sleep_n_sec(2)
        if not device:
            raise Exception(f"Unable to resolve NVMe device for lvol_id={lvol_id} ({lvol_name}) on {client}")

        mount_path = f"{self.MOUNT_BASE}/{tag}_{lvol_name}"

        self.ssh_obj.format_disk(node=client, device=device, fs_type="ext4")
        self.ssh_obj.mount_path(node=client, device=device, mount_path=mount_path)

        sanity_file = f"{mount_path}/sanity.bin"
        self.ssh_obj.exec_command(node=client, command=f"sudo dd if=/dev/zero of={sanity_file} bs=1M count=8 status=none")
        self.ssh_obj.exec_command(node=client, command="sync")
        out, err = self.ssh_obj.exec_command(node=client, command=f"md5sum {sanity_file} | awk '{{print $1}}'")
        if err:
            raise Exception(f"md5sum failed on {client} for {sanity_file}: {err}")
        self.ssh_obj.exec_command(node=client, command=f"sudo rm -f {sanity_file}")

        self._inc("success", "connect_mount_sanity", 1)
        return mount_path

    def _unmount_and_disconnect(self, client: str, mount_path: str, lvol_name: str, lvol_id_hint: str = None):
        self._inc("attempts", "unmount_disconnect", 1)

        if mount_path:
            self.ssh_obj.unmount_path(node=client, device=mount_path)

        lvol_id = lvol_id_hint or self.sbcli_utils.get_lvol_id(lvol_name)
        if not lvol_id:
            raise Exception(f"Could not resolve lvol_id for disconnect: {lvol_name}")

        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        nqn = lvol_details[0]["nqn"]
        self.ssh_obj.disconnect_nvme(node=client, nqn_grep=nqn)

        self._inc("success", "unmount_disconnect", 1)

    # ----------------------------
    # Create tasks
    # ----------------------------
    def _task_create_lvol(self, idx: int, lvol_name: str):
        self._inc("attempts", "create_lvol", 1)

        GENERAL_RETRY_MAX = 10
        GENERAL_RETRY_INTERVAL = 5
        BDEV_RETRY_MAX = 7
        SYNC_DELETION_RETRY_MAX = 10  # 10 × 15s = 2.5 min
        bdev_retries = 0
        sync_retries = 0
        general_retries = 0
        ctx = {"lvol_name": lvol_name, "idx": idx, "client": self._pick_client(idx)}

        while True:
            self.logger.info(f"[create_lvol] ctx={ctx}")
            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name,
                    pool_name=self.pool_name,
                    size=self.LVOL_SIZE,
                    distr_ndcs=self.ndcs,
                    distr_npcs=self.npcs,
                    distr_bs=self.bs,
                    distr_chunk_bs=self.chunk_bs,
                    retry=1,
                )
                break  # success — exit retry loop

            except Exception as e:
                api_err = self._extract_api_error(e)

                if self._is_max_lvols_error(api_err):
                    self._inc("failures", "create_lvol", 1)
                    self.logger.warning(f"[max_lvols] op=create_lvol ctx={ctx}: cluster hit max-lvol limit; triggering forced deletes and continuing")
                    self._force_enqueue_deletes()
                    raise

                if self._is_lvol_sync_deletion_error(api_err) and sync_retries < SYNC_DELETION_RETRY_MAX:
                    sync_retries += 1
                    self.logger.warning(f"[sync_deletion_retry] create_lvol sync_retry {sync_retries}/{SYNC_DELETION_RETRY_MAX} for {lvol_name}, waiting 15s")
                    sleep_n_sec(15)
                    continue

                if self._is_bdev_error(api_err) and bdev_retries < BDEV_RETRY_MAX - 1:
                    bdev_retries += 1
                    lvol_name = f"lvl{generate_random_sequence(15)}_{idx}_{int(time.time())}"
                    ctx["lvol_name"] = lvol_name
                    self.logger.warning(f"[bdev_retry] create_lvol bdev_retry {bdev_retries}/{BDEV_RETRY_MAX}: retrying with new name {lvol_name}")
                    sleep_n_sec(2)
                    continue

                if self._is_lvol_sync_deletion_error(api_err):
                    self._inc("failures", "create_lvol", 1)
                    self.logger.warning(
                        f"[sync_deletion] create_lvol sync_retry exhausted "
                        f"({sync_retries}/{SYNC_DELETION_RETRY_MAX}) for {lvol_name}; "
                        f"triggering forced deletes and continuing"
                    )
                    self._force_enqueue_deletes()
                    raise

                # General retry for unrecognised errors
                if general_retries < GENERAL_RETRY_MAX - 1:
                    general_retries += 1
                    self.logger.warning(f"[retry] create_lvol attempt {general_retries}/{GENERAL_RETRY_MAX} failed: {e}; retrying in {GENERAL_RETRY_INTERVAL}s")
                    sleep_n_sec(GENERAL_RETRY_INTERVAL)
                    continue

                self._inc("failures", "create_lvol", 1)
                if self._is_bdev_error(api_err):
                    details = f"bdev creation failed after {bdev_retries + 1} attempts"
                else:
                    details = f"api call failed after {GENERAL_RETRY_MAX} attempts"
                self._set_failure(op="create_lvol", exc=e, details=details, ctx=ctx, api_err=api_err)
                raise

        lvol_id = self._wait_lvol_id(lvol_name)
        client = self._pick_client(idx)

        try:
            mount_path = self._connect_format_mount_sanity(client, lvol_name, lvol_id, tag="lvol")
        except Exception:
            # Lvol exists on the cluster but connect/mount failed — register it
            # with mount_path=None and immediately queue for deletion so it gets
            # cleaned up instead of becoming an orphan.
            self.logger.warning(f"[create_lvol] connect/mount failed for {lvol_name}; registering orphan and queuing delete")
            with self._lock:
                self._lvol_registry[lvol_name] = {
                    "id": lvol_id,
                    "client": client,
                    "mount_path": None,
                    "snap_state": "done",
                    "delete_state": "queued",
                    "snapshots": set(),
                }
                self._metrics["counts"]["lvols_created"] += 1
                self._lvol_delete_tree_q.append(lvol_name)
            self._inc("failures", "connect_mount_sanity", 1)
            raise

        with self._lock:
            self._lvol_registry[lvol_name] = {
                "id": lvol_id,
                "client": client,
                "mount_path": mount_path,
                "snap_state": "pending",
                "delete_state": "not_queued",
                "snapshots": set(),
            }
            self._metrics["counts"]["lvols_created"] += 1

        self._inc("success", "create_lvol", 1)
        return lvol_name, lvol_id

    def _task_create_snapshot(self, src_lvol_name: str, src_lvol_id: str, snap_name: str):
        # Guard: re-check that parent lvol is still alive (may have been queued for
        # deletion between submit time and execution time)
        with self._lock:
            lm = self._lvol_registry.get(src_lvol_name)
            if not lm or lm["delete_state"] != "not_queued":
                self.logger.info(f"[create_snapshot] Skipping — source lvol {src_lvol_name} is queued/in-progress for deletion")
                if lm:
                    lm["snap_state"] = "done"  # unblock delete tree wait
                return None, None

        self._inc("attempts", "create_snapshot", 1)

        GENERAL_RETRY_MAX = 10
        GENERAL_RETRY_INTERVAL = 5
        SYNC_DELETION_RETRY_MAX = 10  # 10 × 15s = 2.5 min
        sync_retries = 0
        general_retries = 0
        ctx = {"src_lvol_name": src_lvol_name, "src_lvol_id": src_lvol_id, "snap_name": snap_name}

        try:
            while True:
                self.logger.info(f"[create_snapshot] ctx={ctx}")
                try:
                    self.sbcli_utils.add_snapshot(
                        lvol_id=src_lvol_id,
                        snapshot_name=snap_name,
                        retry=1,
                    )
                    break  # success — exit retry loop

                except Exception as e:
                    api_err = self._extract_api_error(e)

                    if self._is_max_lvols_error(api_err):
                        self._inc("failures", "create_snapshot", 1)
                        self.logger.warning(f"[max_lvols] op=create_snapshot ctx={ctx}: cluster hit max-lvol limit; triggering forced deletes and continuing")
                        self._force_enqueue_deletes()
                        raise

                    if self._is_lvol_sync_deletion_error(api_err) and sync_retries < SYNC_DELETION_RETRY_MAX:
                        sync_retries += 1
                        self.logger.warning(f"[sync_deletion_retry] create_snapshot sync_retry {sync_retries}/{SYNC_DELETION_RETRY_MAX} for {snap_name}, waiting 15s")
                        sleep_n_sec(15)
                        continue

                    if self._is_lvol_sync_deletion_error(api_err):
                        self._inc("failures", "create_snapshot", 1)
                        self.logger.warning(
                            f"[sync_deletion] create_snapshot sync_retry exhausted "
                            f"({sync_retries}/{SYNC_DELETION_RETRY_MAX}) for {snap_name}; "
                            f"triggering forced deletes and continuing"
                        )
                        self._force_enqueue_deletes()
                        raise

                    # General retry for unrecognised errors
                    if general_retries < GENERAL_RETRY_MAX - 1:
                        general_retries += 1
                        self.logger.warning(f"[retry] create_snapshot attempt {general_retries}/{GENERAL_RETRY_MAX} failed: {e}; retrying in {GENERAL_RETRY_INTERVAL}s")
                        sleep_n_sec(GENERAL_RETRY_INTERVAL)
                        continue

                    self._inc("failures", "create_snapshot", 1)
                    self._set_failure(op="create_snapshot", exc=e, details=f"api call failed after {GENERAL_RETRY_MAX} attempts", ctx=ctx, api_err=api_err)
                    raise

            snap_id = self._wait_snapshot_id(snap_name)

            with self._lock:
                self._snap_registry[snap_name] = {
                    "snap_id": snap_id,
                    "src_lvol_name": src_lvol_name,
                    "clone_state": "pending",
                    "delete_state": "not_queued",
                    "clones": set(),
                }
                self._metrics["counts"]["snapshots_created"] += 1

                lm = self._lvol_registry.get(src_lvol_name)
                if lm:
                    lm["snapshots"].add(snap_name)
                    lm["snap_state"] = "done"

            self._inc("success", "create_snapshot", 1)
            return snap_name, snap_id

        except Exception:
            # Reset snap_state so the lvol can be re-snapshotted or deleted
            # without a 60s stall in _task_delete_lvol_tree
            with self._lock:
                lm = self._lvol_registry.get(src_lvol_name)
                if lm and lm["snap_state"] == "in_progress":
                    lm["snap_state"] = "pending"
            raise

    def _task_create_clone(self, snap_name: str, snap_id: str, idx: int, clone_name: str):
        # Guard: re-check that snapshot and parent lvol are still alive
        with self._lock:
            sm = self._snap_registry.get(snap_name)
            if not sm or sm["delete_state"] != "not_queued":
                self.logger.info(f"[create_clone] Skipping — snapshot {snap_name} is queued/in-progress for deletion")
                if sm:
                    sm["clone_state"] = "done"  # unblock delete tree wait
                return None, None
            src_lvol = sm["src_lvol_name"]
            lm = self._lvol_registry.get(src_lvol)
            if lm and lm["delete_state"] != "not_queued":
                self.logger.info(f"[create_clone] Skipping — parent lvol {src_lvol} is queued/in-progress for deletion")
                sm["clone_state"] = "done"
                return None, None

        self._inc("attempts", "create_clone", 1)

        GENERAL_RETRY_MAX = 10
        GENERAL_RETRY_INTERVAL = 5
        BDEV_RETRY_MAX = 7
        SYNC_DELETION_RETRY_MAX = 10  # 10 × 15s = 2.5 min
        bdev_retries = 0
        sync_retries = 0
        general_retries = 0
        ctx = {"snap_name": snap_name, "snapshot_id": snap_id, "clone_name": clone_name, "client": self._pick_client(idx)}

        try:
            while True:
                self.logger.info(f"[create_clone] ctx={ctx}")
                try:
                    self.sbcli_utils.add_clone(
                        snapshot_id=snap_id,
                        clone_name=clone_name,
                        retry=1,
                    )
                    break  # success — exit retry loop

                except Exception as e:
                    api_err = self._extract_api_error(e)

                    if self._is_max_lvols_error(api_err):
                        self._inc("failures", "create_clone", 1)
                        self.logger.warning(f"[max_lvols] op=create_clone ctx={ctx}: cluster hit max-lvol limit; triggering forced deletes and continuing")
                        self._force_enqueue_deletes()
                        raise

                    if self._is_lvol_sync_deletion_error(api_err) and sync_retries < SYNC_DELETION_RETRY_MAX:
                        sync_retries += 1
                        self.logger.warning(f"[sync_deletion_retry] create_clone sync_retry {sync_retries}/{SYNC_DELETION_RETRY_MAX} for {clone_name}, waiting 15s")
                        sleep_n_sec(15)
                        continue

                    if self._is_bdev_error(api_err) and bdev_retries < BDEV_RETRY_MAX - 1:
                        bdev_retries += 1
                        clone_name = f"cln{generate_random_sequence(15)}_{idx}_{int(time.time())}"
                        ctx["clone_name"] = clone_name
                        self.logger.warning(f"[bdev_retry] create_clone bdev_retry {bdev_retries}/{BDEV_RETRY_MAX}: retrying with new name {clone_name}")
                        sleep_n_sec(2)
                        continue

                    if self._is_lvol_sync_deletion_error(api_err):
                        self._inc("failures", "create_clone", 1)
                        self.logger.warning(
                            f"[sync_deletion] create_clone sync_retry exhausted "
                            f"({sync_retries}/{SYNC_DELETION_RETRY_MAX}) for {clone_name}; "
                            f"triggering forced deletes and continuing"
                        )
                        self._force_enqueue_deletes()
                        raise

                    # General retry for unrecognised errors
                    if general_retries < GENERAL_RETRY_MAX - 1:
                        general_retries += 1
                        self.logger.warning(f"[retry] create_clone attempt {general_retries}/{GENERAL_RETRY_MAX} failed: {e}; retrying in {GENERAL_RETRY_INTERVAL}s")
                        sleep_n_sec(GENERAL_RETRY_INTERVAL)
                        continue

                    self._inc("failures", "create_clone", 1)
                    if self._is_bdev_error(api_err):
                        details = f"bdev creation failed after {bdev_retries + 1} attempts"
                    else:
                        details = f"api call failed after {GENERAL_RETRY_MAX} attempts"
                    self._set_failure(op="create_clone", exc=e, details=details, ctx=ctx, api_err=api_err)
                    raise

            clone_lvol_id = self._wait_lvol_id(clone_name)
            client = self._pick_client(idx)

            try:
                mount_path = self._connect_format_mount_sanity(client, clone_name, clone_lvol_id, tag="clone")
            except Exception:
                # Clone exists on the cluster but connect/mount failed — register it
                # with mount_path=None and queue parent snapshot tree for deletion.
                self.logger.warning(f"[create_clone] connect/mount failed for {clone_name}; registering orphan and queuing snap tree delete")
                with self._lock:
                    self._metrics["counts"]["clones_created"] += 1
                    sm = self._snap_registry.get(snap_name)
                    if sm:
                        sm["clone_state"] = "done"
                        sm["clones"].add(clone_name)
                    self._clone_registry[clone_name] = {
                        "id": clone_lvol_id,
                        "client": client,
                        "mount_path": None,
                        "snap_name": snap_name,
                        "delete_state": "not_queued",
                    }
                self._inc("failures", "connect_mount_sanity", 1)
                raise

            with self._lock:
                self._metrics["counts"]["clones_created"] += 1

                sm = self._snap_registry.get(snap_name)
                if sm:
                    sm["clone_state"] = "done"
                    sm["clones"].add(clone_name)

                self._clone_registry[clone_name] = {
                    "id": clone_lvol_id,
                    "client": client,
                    "mount_path": mount_path,
                    "snap_name": snap_name,
                    "delete_state": "not_queued",
                }

            self._inc("success", "create_clone", 1)
            return clone_name, clone_lvol_id

        except Exception:
            # Reset clone_state so the snapshot can be re-cloned or deleted
            # without a 60s stall in _task_delete_snapshot_tree
            with self._lock:
                sm = self._snap_registry.get(snap_name)
                if sm and sm["clone_state"] == "in_progress":
                    sm["clone_state"] = "pending"
            raise

    # ----------------------------
    # Delete primitives
    # ----------------------------
    def _delete_clone_lvol(self, clone_name: str):
        with self._lock:
            meta = self._clone_registry.get(clone_name)
        if not meta:
            return

        client = meta["client"]
        mount_path = meta["mount_path"]
        lvol_id = meta["id"]

        self._unmount_and_disconnect(client=client, mount_path=mount_path, lvol_name=clone_name, lvol_id_hint=lvol_id)

        ctx = {"clone_name": clone_name, "lvol_id": lvol_id, "client": client}
        self._inc("attempts", "delete_lvol", 1)
        self._api("delete_lvol", ctx, lambda: self.sbcli_utils.delete_lvol(lvol_name=clone_name, skip_error=False))

        with self._lock:
            self._metrics["counts"]["lvols_deleted"] += 1
            self._metrics["counts"]["clones_deleted"] += 1
            self._clone_registry.pop(clone_name, None)

        self._inc("success", "delete_lvol", 1)

    def _delete_snapshot_only(self, snap_name: str, snap_id: str):
        ctx = {"snap_name": snap_name, "snap_id": snap_id}
        self._inc("attempts", "delete_snapshot", 1)

        RETRY_MAX = 10
        RETRY_INTERVAL = 5
        for attempt in range(RETRY_MAX):
            try:
                self.sbcli_utils.delete_snapshot(snap_id=snap_id, snap_name=snap_name, skip_error=False)
                break  # success

            except Exception as e:
                if attempt == 0:
                    # Snapshot may have been soft-deleted by the cluster because clones still exist.
                    # Find clones referencing this snapshot in our registry, delete them, then retry.
                    with self._lock:
                        orphan_clones = [cn for cn, cm in self._clone_registry.items() if cm["snap_name"] == snap_name]

                    if orphan_clones:
                        self.logger.warning(
                            f"[delete_snapshot] {snap_name} delete failed; "
                            f"found {len(orphan_clones)} orphan clone(s): {orphan_clones} — deleting and retrying"
                        )
                        for cn in orphan_clones:
                            self._delete_clone_lvol(cn)
                            with self._lock:
                                m = self._snap_registry.get(snap_name)
                                if m:
                                    m["clones"].discard(cn)
                        continue  # retry snapshot delete

                if attempt < RETRY_MAX - 1:
                    self.logger.warning(f"[retry] delete_snapshot attempt {attempt + 1}/{RETRY_MAX} failed: {e}; retrying in {RETRY_INTERVAL}s")
                    sleep_n_sec(RETRY_INTERVAL)
                    continue

                # Final attempt failed — propagate as test failure
                api_err = self._extract_api_error(e)
                self._inc("failures", "delete_snapshot", 1)
                self._set_failure(op="delete_snapshot", exc=e, details=f"snapshot delete failed after {RETRY_MAX} attempts", ctx=ctx, api_err=api_err)
                raise

        with self._lock:
            self._metrics["counts"]["snapshots_deleted"] += 1
            self._snap_registry.pop(snap_name, None)

        self._inc("success", "delete_snapshot", 1)

    def _delete_lvol_only(self, lvol_name: str):
        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
        if not meta:
            return

        client = meta["client"]
        mount_path = meta["mount_path"]
        lvol_id = meta["id"]

        self._unmount_and_disconnect(client=client, mount_path=mount_path, lvol_name=lvol_name, lvol_id_hint=lvol_id)

        ctx = {"lvol_name": lvol_name, "lvol_id": lvol_id, "client": client}
        self._inc("attempts", "delete_lvol", 1)

        RETRY_MAX = 10
        RETRY_INTERVAL = 5
        for attempt in range(RETRY_MAX):
            try:
                self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=False)
                break  # success

            except Exception as e:
                if attempt == 0:
                    # Lvol may have stalled in deletion because snapshots still exist on the cluster.
                    # Find snapshots referencing this lvol in our registry, delete their trees, then retry.
                    with self._lock:
                        orphan_snaps = [sn for sn, sm in self._snap_registry.items() if sm["src_lvol_name"] == lvol_name]

                    if orphan_snaps:
                        self.logger.warning(
                            f"[delete_lvol] {lvol_name} delete failed; "
                            f"found {len(orphan_snaps)} orphan snapshot(s): {orphan_snaps} — deleting and retrying"
                        )
                        for sn in orphan_snaps:
                            self._task_delete_snapshot_tree(sn)
                            with self._lock:
                                m = self._lvol_registry.get(lvol_name)
                                if m:
                                    m["snapshots"].discard(sn)
                        continue  # retry lvol delete

                if attempt < RETRY_MAX - 1:
                    self.logger.warning(f"[retry] delete_lvol attempt {attempt + 1}/{RETRY_MAX} failed: {e}; retrying in {RETRY_INTERVAL}s")
                    sleep_n_sec(RETRY_INTERVAL)
                    continue

                # Final attempt failed — propagate as test failure
                api_err = self._extract_api_error(e)
                self._inc("failures", "delete_lvol", 1)
                self._set_failure(op="delete_lvol", exc=e, details=f"lvol delete failed after {RETRY_MAX} attempts", ctx=ctx, api_err=api_err)
                raise

        with self._lock:
            self._metrics["counts"]["lvols_deleted"] += 1
            self._lvol_registry.pop(lvol_name, None)

        self._inc("success", "delete_lvol", 1)

    # ----------------------------
    # Delete Trees (order-safe)
    # ----------------------------
    def _task_delete_snapshot_tree(self, snap_name: str):
        """
        Snapshot delete tree:
          - wait for any in-flight clone creation to register
          - delete all clones for that snapshot (from both registry and clone_registry scan)
          - delete snapshot
        """
        self._inc("attempts", "delete_snapshot_tree", 1)

        with self._lock:
            meta = self._snap_registry.get(snap_name)
            if not meta:
                self._inc("success", "delete_snapshot_tree", 1)
                return True
            meta["delete_state"] = "in_progress"
            snap_id = meta["snap_id"]
            src_lvol = meta["src_lvol_name"]

        # Wait for any in-flight clone creation (add_clone done but not yet registered)
        for _ in range(60):
            with self._lock:
                m = self._snap_registry.get(snap_name)
                if not m or m["clone_state"] != "in_progress":
                    break
            self.logger.info(f"[delete_snap_tree] Waiting for in-flight clone creation for snap={snap_name}")
            sleep_n_sec(1)

        # Collect clones from both the snapshot's tracked set and a full registry scan
        # (the scan catches clones that finished add_clone but haven't registered yet)
        with self._lock:
            m = self._snap_registry.get(snap_name)
            tracked = set(m["clones"]) if m else set()
            extra = {cn for cn, cm in self._clone_registry.items() if cm["snap_name"] == snap_name and cn not in tracked}
            clones = list(tracked | extra)
            if extra:
                self.logger.warning(f"[delete_snap_tree] Found {len(extra)} untracked clones for snap={snap_name}: {extra}")

        for cn in clones:
            self._delete_clone_lvol(cn)
            with self._lock:
                m = self._snap_registry.get(snap_name)
                if m:
                    m["clones"].discard(cn)

        with self._lock:
            m = self._snap_registry.get(snap_name)
            remaining = len(m["clones"]) if m else 0
        if remaining != 0:
            raise Exception(f"Snapshot tree delete invariant violated: clones still exist for snap={snap_name} remaining={remaining}")

        self._delete_snapshot_only(snap_name, snap_id)

        # unlink from LVOL snapshots set
        with self._lock:
            lm = self._lvol_registry.get(src_lvol)
            if lm:
                lm["snapshots"].discard(snap_name)

        self._inc("success", "delete_snapshot_tree", 1)
        return True

    def _task_delete_lvol_tree(self, lvol_name: str):
        """
        LVOL delete tree:
          - wait for any in-flight snapshot creation to register
          - delete all snapshots (each snapshot-tree deletes clones then snapshot)
          - delete lvol
        """
        self._inc("attempts", "delete_lvol_tree", 1)

        with self._lock:
            meta = self._lvol_registry.get(lvol_name)
            if not meta:
                self._inc("success", "delete_lvol_tree", 1)
                return True
            meta["delete_state"] = "in_progress"

        # Wait for any in-flight snapshot creation to register in meta["snapshots"]
        for _ in range(60):
            with self._lock:
                m = self._lvol_registry.get(lvol_name)
                if not m or m["snap_state"] != "in_progress":
                    break
            self.logger.info(f"[delete_lvol_tree] Waiting for in-flight snapshot creation for lvol={lvol_name}")
            sleep_n_sec(1)

        # Collect snapshots from both the lvol's tracked set and a full registry scan
        with self._lock:
            m = self._lvol_registry.get(lvol_name)
            tracked = set(m["snapshots"]) if m else set()
            extra = {sn for sn, sm in self._snap_registry.items() if sm["src_lvol_name"] == lvol_name and sn not in tracked}
            snap_names = list(tracked | extra)
            if extra:
                self.logger.warning(f"[delete_lvol_tree] Found {len(extra)} untracked snapshots for lvol={lvol_name}: {extra}")

        for sn in snap_names:
            self._task_delete_snapshot_tree(sn)

        with self._lock:
            meta2 = self._lvol_registry.get(lvol_name)
            remaining_snaps = len(meta2["snapshots"]) if meta2 else 0
        if remaining_snaps != 0:
            raise Exception(f"LVOL tree delete invariant violated: snapshots still exist for lvol={lvol_name} remaining={remaining_snaps}")

        self._delete_lvol_only(lvol_name)

        self._inc("success", "delete_lvol_tree", 1)
        return True

    # ----------------------------
    # Inventory-based delete enqueue
    # ----------------------------
    def _maybe_enqueue_deletes(self):
        """
        Keep total inventory (lvols + snapshots + clones) <= TOTAL_INVENTORY_MAX.

        Strategy:
          - If total > TOTAL_DELETE_THRESHOLD: enqueue LVOL tree deletes (removes lvol+snap+clone in one go).
          - Never reduce live (not-queued) lvols below MIN_LIVE_LVOLS.
          - Prefer lvols that have already been snapshotted (snap_state==done) so we exercise the full tree.
          - Fall back to any not-queued lvol if none with snapshots are available.
          - Also clean up orphan snapshots (whose parent lvol was already deleted).
        """
        with self._lock:
            total = len(self._lvol_registry) + len(self._snap_registry) + len(self._clone_registry)
            if total <= self.TOTAL_DELETE_THRESHOLD:
                return

            # Count how many lvols are still alive (not queued for deletion)
            live_lvols = sum(1 for lm in self._lvol_registry.values() if lm["delete_state"] == "not_queued")

            added = 0
            # Prefer lvols that have completed at least one snapshot cycle (fuller trees)
            for ln, lm in list(self._lvol_registry.items()):
                if live_lvols - added <= self.MIN_LIVE_LVOLS:
                    break  # preserve minimum live lvols
                if lm["delete_state"] == "not_queued" and lm["snap_state"] == "done":
                    lm["delete_state"] = "queued"
                    self._lvol_delete_tree_q.append(ln)
                    added += 1
                    if added >= self.LVOL_DELETE_TREE_INFLIGHT:
                        break

            # Fall back to any not-queued lvol (e.g., snapshot still pending)
            if added < self.LVOL_DELETE_TREE_INFLIGHT:
                for ln, lm in list(self._lvol_registry.items()):
                    if live_lvols - added <= self.MIN_LIVE_LVOLS:
                        break  # preserve minimum live lvols
                    if lm["delete_state"] == "not_queued":
                        lm["delete_state"] = "queued"
                        self._lvol_delete_tree_q.append(ln)
                        added += 1
                        if added >= self.LVOL_DELETE_TREE_INFLIGHT:
                            break

            # Clean up orphan snapshots (parent lvol already gone but snap still in registry)
            for sn, sm in list(self._snap_registry.items()):
                if sm["delete_state"] == "not_queued" and sm["src_lvol_name"] not in self._lvol_registry:
                    sm["delete_state"] = "queued"
                    self._snapshot_delete_tree_q.append(sn)

    # ----------------------------
    # Scheduler submitters
    # ----------------------------
    def _submit_creates(self, ex, create_f: dict, idx_counter: dict):
        while (not self._stop_event.is_set()) and (len(create_f) < self.CREATE_INFLIGHT):
            active = self._active_inventory_count()
            if active >= self.TOTAL_INVENTORY_MAX:
                return  # at capacity; wait for deletes to free space
            idx = idx_counter["idx"]
            idx_counter["idx"] += 1
            lvol_name = f"lvl{generate_random_sequence(15)}_{idx}_{int(time.time())}"
            f = ex.submit(lambda i=idx, n=lvol_name: self._task_create_lvol(i, n))
            create_f[f] = time.time()

    def _submit_snapshots(self, ex, snap_f: dict):
        while (not self._stop_event.is_set()) and (len(snap_f) < self.SNAPSHOT_INFLIGHT):
            candidate = None
            with self._lock:
                for lvol_name, lm in self._lvol_registry.items():
                    if lm["delete_state"] == "not_queued" and lm["snap_state"] == "pending":
                        # Skip if any snapshot of this lvol has a clone creation in flight
                        has_busy_clone = any(
                            sm["clone_state"] == "in_progress"
                            for sm in self._snap_registry.values()
                            if sm["src_lvol_name"] == lvol_name
                        )
                        if has_busy_clone:
                            continue  # clone in flight on this lvol — would cause sync contention
                        lm["snap_state"] = "in_progress"
                        candidate = (lvol_name, lm["id"])
                        break
            if not candidate:
                return

            lvol_name, lvol_id = candidate
            snap_name = f"snap{generate_random_sequence(15)}_{int(time.time())}"
            f = ex.submit(lambda ln=lvol_name, lid=lvol_id, sn=snap_name: self._task_create_snapshot(ln, lid, sn))
            snap_f[f] = time.time()

    def _submit_clones(self, ex, clone_f: dict):
        while (not self._stop_event.is_set()) and (len(clone_f) < self.CLONE_INFLIGHT):
            candidate = None
            with self._lock:
                for sn, sm in self._snap_registry.items():
                    if sm["clone_state"] != "pending":
                        continue
                    if sm["delete_state"] != "not_queued":
                        continue  # snapshot is scheduled/in-progress for deletion
                    lm = self._lvol_registry.get(sm["src_lvol_name"])
                    if lm and lm["delete_state"] != "not_queued":
                        continue  # parent lvol is scheduled/in-progress for deletion
                    if lm and lm["snap_state"] == "in_progress":
                        continue  # parent lvol has a snapshot creation in flight — would cause sync contention
                    sm["clone_state"] = "in_progress"
                    candidate = (sn, sm["snap_id"])
                    break
            if not candidate:
                return

            snap_name, snap_id = candidate
            idx = int(time.time())
            clone_name = f"cln{generate_random_sequence(15)}_{idx}_{int(time.time())}"
            f = ex.submit(lambda s=snap_name, sid=snap_id, i=idx, cn=clone_name: self._task_create_clone(s, sid, i, cn))
            clone_f[f] = time.time()

    def _submit_snapshot_delete_trees(self, ex, snap_del_f: dict):
        while (not self._stop_event.is_set()) and (len(snap_del_f) < self.SNAPSHOT_DELETE_TREE_INFLIGHT):
            with self._lock:
                if not self._snapshot_delete_tree_q:
                    return
                sn = self._snapshot_delete_tree_q.popleft()
            f = ex.submit(lambda sn=sn: self._task_delete_snapshot_tree(sn))
            snap_del_f[f] = time.time()

    def _submit_lvol_delete_trees(self, ex, lvol_del_f: dict):
        while (not self._stop_event.is_set()) and (len(lvol_del_f) < self.LVOL_DELETE_TREE_INFLIGHT):
            with self._lock:
                if not self._lvol_delete_tree_q:
                    return
                ln = self._lvol_delete_tree_q.popleft()
            f = ex.submit(lambda ln=ln: self._task_delete_lvol_tree(ln))
            lvol_del_f[f] = time.time()

    def _update_peaks(self, create_f, snap_f, clone_f, snap_del_f, lvol_del_f):
        with self._lock:
            self._metrics["peak_inflight"]["create"] = max(self._metrics["peak_inflight"]["create"], len(create_f))
            self._metrics["peak_inflight"]["snapshot"] = max(self._metrics["peak_inflight"]["snapshot"], len(snap_f))
            self._metrics["peak_inflight"]["clone"] = max(self._metrics["peak_inflight"]["clone"], len(clone_f))
            self._metrics["peak_inflight"]["snapshot_delete_tree"] = max(
                self._metrics["peak_inflight"]["snapshot_delete_tree"], len(snap_del_f)
            )
            self._metrics["peak_inflight"]["lvol_delete_tree"] = max(
                self._metrics["peak_inflight"]["lvol_delete_tree"], len(lvol_del_f)
            )

    def _harvest_fail_fast(self, fut_dict: dict):
        now = time.time()
        done = [f for f in fut_dict if f.done()]
        for f in done:
            del fut_dict[f]
            try:
                f.result()
            except Exception as exc:
                self.logger.warning(f"[harvest] Future failed: {type(exc).__name__}: {exc}")
                return

        # Cancel futures that have been running longer than TASK_TIMEOUT
        stale = [f for f, ts in fut_dict.items() if (now - ts) > self.TASK_TIMEOUT and not f.done()]
        for f in stale:
            f.cancel()
            elapsed = now - fut_dict.pop(f)
            self.logger.warning(f"[harvest] Cancelled stale future after {elapsed:.0f}s (timeout={self.TASK_TIMEOUT}s)")

    # ----------------------------
    # Summary
    # ----------------------------
    def _print_summary(self):
        with self._lock:
            self._metrics["end_ts"] = time.time()
            dur = self._metrics["end_ts"] - self._metrics["start_ts"] if self._metrics["start_ts"] else None

            self.logger.info("======== TEST SUMMARY (parallel continuous steady) ========")
            self.logger.info(f"Duration (sec): {dur:.1f}" if dur else "Duration (sec): n/a")
            self.logger.info(f"Loops: {self._metrics['loops']}")
            self.logger.info(f"Max workers: {self._metrics['max_workers']}")
            self.logger.info(f"Targets: {self._metrics['targets']}")
            self.logger.info(f"Peak inflight: {self._metrics['peak_inflight']}")
            self.logger.info(f"Counts: {self._metrics['counts']}")
            self.logger.info(f"Attempts: {self._metrics['attempts']}")
            self.logger.info(f"Success: {self._metrics['success']}")
            self.logger.info(f"Failures: {self._metrics['failures']}")
            self.logger.info(f"Failure info: {self._metrics['failure_info']}")

            # Live inventory breakdown
            live_lvols = len(self._lvol_registry)
            live_snaps = len(self._snap_registry)
            live_clones = len(self._clone_registry)
            self.logger.info(
                f"Live inventory now: lvols={live_lvols} snaps={live_snaps} clones={live_clones} "
                f"total={live_lvols + live_snaps + live_clones} (max={self.TOTAL_INVENTORY_MAX})"
            )

            # Delete queue sizes
            lvol_del_queued = sum(1 for lm in self._lvol_registry.values() if lm["delete_state"] in ("queued", "in_progress"))
            snap_del_queued = sum(1 for sm in self._snap_registry.values() if sm["delete_state"] in ("queued", "in_progress"))
            clone_del_queued = sum(1 for cm in self._clone_registry.values() if cm["delete_state"] in ("queued", "in_progress"))
            self.logger.info(
                f"Pending deletes: lvols={lvol_del_queued} snaps={snap_del_queued} clones={clone_del_queued}"
            )

            # Validate counts vs registry
            # Note: lvols_deleted includes clone deletions (each clone delete
            # increments both lvols_deleted and clones_deleted), so subtract
            # clones_deleted to get pure lvol deletions.
            c = self._metrics["counts"]
            expected_lvols = c["lvols_created"] - (c["lvols_deleted"] - c["clones_deleted"])
            expected_snaps = c["snapshots_created"] - c["snapshots_deleted"]
            expected_clones = c["clones_created"] - c["clones_deleted"]

            if expected_lvols != live_lvols:
                self.logger.warning(
                    f"[summary] MISMATCH: expected_live_lvols={expected_lvols} but registry has {live_lvols} "
                    f"(created={c['lvols_created']} deleted={c['lvols_deleted']})"
                )
            if expected_snaps != live_snaps:
                self.logger.warning(
                    f"[summary] MISMATCH: expected_live_snaps={expected_snaps} but registry has {live_snaps} "
                    f"(created={c['snapshots_created']} deleted={c['snapshots_deleted']})"
                )
            if expected_clones != live_clones:
                self.logger.warning(
                    f"[summary] MISMATCH: expected_live_clones={expected_clones} but registry has {live_clones} "
                    f"(created={c['clones_created']} deleted={c['clones_deleted']})"
                )

            self.logger.info("===========================================================")

    # ----------------------------
    # Main
    # ----------------------------
    def run(self):
        self.logger.info("=== Starting TestParallelLvolSnapshotCloneAPI (steady snapshots/clones) ===")

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        sleep_n_sec(2)

        max_workers = self.MAX_TOTAL_INFLIGHT + 5

        with self._lock:
            self._metrics["start_ts"] = time.time()
            self._metrics["max_workers"] = max_workers

        create_f = {}   # Future -> submit_timestamp
        snap_f = {}
        clone_f = {}
        snap_del_f = {}
        lvol_del_f = {}

        idx_counter = {"idx": 0}

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                # seed initial creates
                self._submit_creates(ex, create_f, idx_counter)

                while not self._stop_event.is_set():
                    if os.path.exists(self.STOP_FILE):
                        self.logger.info(f"Stop file found: {self.STOP_FILE}. Stopping gracefully.")
                        break
                    if self.MAX_RUNTIME_SEC and (time.time() - self._metrics["start_ts"]) > self.MAX_RUNTIME_SEC:
                        self.logger.info("MAX_RUNTIME_SEC reached. Stopping gracefully.")
                        break

                    with self._lock:
                        self._metrics["loops"] += 1

                    # Decide delete enqueue based on inventory (keeps snapshots/clones alive)
                    self._maybe_enqueue_deletes()

                    # Global concurrency cap: skip submitting if already at limit
                    total_inflight = len(create_f) + len(snap_f) + len(clone_f) + len(snap_del_f) + len(lvol_del_f)
                    if total_inflight < self.MAX_TOTAL_INFLIGHT:
                        # Submit work to maintain in-flight
                        self._submit_creates(ex, create_f, idx_counter)
                        self._submit_snapshots(ex, snap_f)
                        self._submit_clones(ex, clone_f)

                        # Submit deletes (trees)
                        self._submit_snapshot_delete_trees(ex, snap_del_f)
                        self._submit_lvol_delete_trees(ex, lvol_del_f)

                    # Update peaks and harvest
                    self._update_peaks(create_f, snap_f, clone_f, snap_del_f, lvol_del_f)
                    self._harvest_fail_fast(create_f)
                    self._harvest_fail_fast(snap_f)
                    self._harvest_fail_fast(clone_f)
                    self._harvest_fail_fast(snap_del_f)
                    self._harvest_fail_fast(lvol_del_f)

                    sleep_n_sec(1)

                # Cancel all pending futures so ThreadPoolExecutor.shutdown doesn't hang
                self.logger.info("Cancelling remaining futures before shutdown...")
                cancelled = 0
                for f_dict in [create_f, snap_f, clone_f, snap_del_f, lvol_del_f]:
                    for f in list(f_dict.keys()):
                        if f.cancel():
                            cancelled += 1
                        f_dict.pop(f, None)
                self.logger.info(f"Cancelled {cancelled} pending futures")

        finally:
            self._print_summary()

        with self._lock:
            failure_info = self._metrics["failure_info"]

        if failure_info:
            raise Exception(f"Test stopped due to failure: {failure_info}")

        raise Exception("Test stopped without failure (graceful stop).")
