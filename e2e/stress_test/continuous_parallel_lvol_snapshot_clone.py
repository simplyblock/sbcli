o# import os
# import time
# import threading
# from collections import deque
# from concurrent.futures import ThreadPoolExecutor

# from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
# from utils.common_utils import sleep_n_sec

# try:
#     import requests
# except Exception:
#     requests = None


# class TestParallelLvolSnapshotCloneAPI(TestClusterBase):
#     """
#     Continuous parallel stress until failure.

#     Adds traceability WITHOUT changing sbcli_utils:
#       - Every API call is wrapped to capture HTTP status/text when available (HTTPError.response).
#       - Every task carries ctx: lvol_name/snap_name/snapshot_id/clone_name/client.
#       - Summary prints ctx + api_error payload.
#     """

#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "parallel_lvol_snapshot_clone_api_continuous"

#         self.CREATE_INFLIGHT = 10
#         self.DELETE_INFLIGHT = 15
#         self.SNAPSHOT_INFLIGHT = 10
#         self.CLONE_INFLIGHT = 10
#         self.SNAPSHOT_DELETE_INFLIGHT = 10

#         self.STOP_FILE = "/tmp/stop_api_stress"
#         self.MAX_RUNTIME_SEC = None

#         self.LVOL_SIZE = "10G"
#         self.MOUNT_BASE = "/mnt/test_location"

#         self._lock = threading.Lock()
#         self._dq_lock = threading.Lock()
#         self._sdq_lock = threading.Lock()
#         self._stop_event = threading.Event()

#         self._delete_queue = deque()       # {"name","id","client","mount_path","kind"}
#         self._snap_delete_queue = deque()  # {"snap_name","snap_id"}

#         # State protection
#         # LVOL: snap_state pending|in_progress|done ; delete_state not_queued|queued|in_progress|done
#         self._lvol_registry = {}
#         # SNAP: clone_state pending|in_progress|done ; delete_state not_queued|queued|in_progress|done
#         self._snap_registry = {}

#         self._metrics = {
#             "start_ts": None,
#             "end_ts": None,
#             "loops": 0,
#             "max_workers": 0,
#             "targets": {
#                 "create_inflight": self.CREATE_INFLIGHT,
#                 "delete_inflight": self.DELETE_INFLIGHT,
#                 "snapshot_inflight": self.SNAPSHOT_INFLIGHT,
#                 "clone_inflight": self.CLONE_INFLIGHT,
#                 "snapshot_delete_inflight": self.SNAPSHOT_DELETE_INFLIGHT,
#             },
#             "attempts": {
#                 "create_lvol": 0,
#                 "delete_lvol": 0,
#                 "create_snapshot": 0,
#                 "delete_snapshot": 0,
#                 "create_clone": 0,
#                 "connect_mount_sanity": 0,
#                 "unmount_disconnect": 0,
#             },
#             "success": {
#                 "create_lvol": 0,
#                 "delete_lvol": 0,
#                 "create_snapshot": 0,
#                 "delete_snapshot": 0,
#                 "create_clone": 0,
#                 "connect_mount_sanity": 0,
#                 "unmount_disconnect": 0,
#             },
#             "failures": {
#                 "create_lvol": 0,
#                 "delete_lvol": 0,
#                 "create_snapshot": 0,
#                 "delete_snapshot": 0,
#                 "create_clone": 0,
#                 "connect_mount_sanity": 0,
#                 "unmount_disconnect": 0,
#                 "unknown": 0,
#             },
#             "counts": {
#                 "lvols_created": 0,
#                 "lvols_deleted": 0,
#                 "snapshots_created": 0,
#                 "snapshots_deleted": 0,
#                 "clones_created": 0,
#                 "clones_deleted": 0,
#             },
#             "peak_inflight": {"create": 0, "delete": 0, "snapshot": 0, "clone": 0, "snapshot_delete": 0},
#             "failure_info": None,
#         }

#     # ----------------------------
#     # Metrics
#     # ----------------------------
#     def _inc(self, bucket: str, key: str, n: int = 1):
#         with self._lock:
#             self._metrics[bucket][key] += n

#     def _set_failure(self, op: str, exc: Exception, details: str = "", ctx: dict = None, api_err: dict = None):
#         with self._lock:
#             if self._metrics["failure_info"] is None:
#                 self._metrics["failure_info"] = {
#                     "op": op,
#                     "exc": repr(exc),
#                     "when": time.strftime("%Y-%m-%d %H:%M:%S"),
#                     "details": details,
#                     "ctx": ctx or {},
#                     "api_error": api_err or {},
#                 }
#         self._stop_event.set()

#     # ----------------------------
#     # Extract API response WITHOUT sbcli changes
#     # ----------------------------
#     def _extract_api_error(self, e: Exception) -> dict:
#         """
#         Best-effort extraction of HTTP response info from exceptions.

#         Works if sbcli_utils ultimately raises:
#           - requests.exceptions.HTTPError with .response
#           - custom exception containing a .response
#           - or embeds response text in string
#         """
#         info = {"type": type(e).__name__, "msg": str(e)}

#         # requests HTTPError
#         if requests is not None:
#             try:
#                 if isinstance(e, requests.exceptions.HTTPError):
#                     resp = getattr(e, "response", None)
#                     if resp is not None:
#                         info["status_code"] = getattr(resp, "status_code", None)
#                         try:
#                             info["text"] = resp.text
#                         except Exception:
#                             info["text"] = "<no-text>"
#                         try:
#                             info["json"] = resp.json()
#                         except Exception:
#                             pass
#                         return info
#             except Exception:
#                 pass

#         # generic .response on exception
#         resp = getattr(e, "response", None)
#         if resp is not None:
#             info["status_code"] = getattr(resp, "status_code", None)
#             try:
#                 info["text"] = resp.text
#             except Exception:
#                 info["text"] = "<no-text>"
#             try:
#                 info["json"] = resp.json()
#             except Exception:
#                 pass
#             return info

#         return info

#     def _pick_client(self, i: int) -> str:
#         return self.client_machines[i % len(self.client_machines)]

#     def _wait_lvol_id(self, lvol_name: str, timeout=300, interval=5) -> str:
#         start = time.time()
#         while time.time() - start < timeout:
#             lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
#             if lvol_id:
#                 return lvol_id
#             sleep_n_sec(interval)
#         raise TimeoutError(f"LVOL id not visible for {lvol_name} after {timeout}s")

#     def _wait_snapshot_id(self, snap_name: str, timeout=300, interval=5) -> str:
#         start = time.time()
#         while time.time() - start < timeout:
#             snap_id = self.sbcli_utils.get_snapshot_id(snap_name=snap_name)
#             if snap_id:
#                 return snap_id
#             sleep_n_sec(interval)
#         raise TimeoutError(f"Snapshot id not visible for {snap_name} after {timeout}s")

#     # ----------------------------
#     # API wrappers (NO sbcli changes)
#     # ----------------------------
#     def _api(self, op: str, ctx: dict, fn):
#         """
#         Wrap sbcli_utils calls:
#           - On exception: capture best-effort response info and stop test.
#         """
#         try:
#             return fn()
#         except Exception as e:
#             api_err = self._extract_api_error(e)
#             self._inc("failures", op if op in self._metrics["failures"] else "unknown", 1)
#             self._set_failure(op=op, exc=e, details="api call failed", ctx=ctx, api_err=api_err)
#             raise

#     # ----------------------------
#     # IO sanity
#     # ----------------------------
#     def _connect_format_mount_sanity(self, client: str, lvol_name: str, lvol_id: str, tag: str) -> str:
#         self._inc("attempts", "connect_mount_sanity", 1)

#         connect_cmds = self.sbcli_utils.get_lvol_connect_str(lvol_name)
#         if not connect_cmds:
#             raise Exception(f"No connect strings returned for {lvol_name}")

#         for cmd in connect_cmds:
#             out, err = self.ssh_obj.exec_command(node=client, command=cmd)
#             if err:
#                 raise Exception(f"NVMe connect failed for {lvol_name} on {client}. err={err} out={out}")

#         device = None
#         for _ in range(25):
#             device = self.ssh_obj.get_lvol_vs_device(node=client, lvol_id=lvol_id)
#             if device:
#                 break
#             sleep_n_sec(2)
#         if not device:
#             raise Exception(f"Unable to resolve NVMe device for lvol_id={lvol_id} ({lvol_name}) on {client}")

#         mount_path = f"{self.MOUNT_BASE}/{tag}_{lvol_name}"

#         self.ssh_obj.format_disk(node=client, device=device, fs_type="ext4")
#         self.ssh_obj.mount_path(node=client, device=device, mount_path=mount_path)

#         sanity_file = f"{mount_path}/sanity.bin"
#         self.ssh_obj.exec_command(node=client, command=f"sudo dd if=/dev/zero of={sanity_file} bs=1M count=8 status=none")
#         self.ssh_obj.exec_command(node=client, command="sync")
#         out, err = self.ssh_obj.exec_command(node=client, command=f"md5sum {sanity_file} | awk '{{print $1}}'")
#         if err:
#             raise Exception(f"md5sum failed on {client} for {sanity_file}: {err}")
#         self.ssh_obj.exec_command(node=client, command=f"sudo rm -f {sanity_file}")

#         self._inc("success", "connect_mount_sanity", 1)
#         return mount_path

#     def _enqueue_delete(self, name: str, lvol_id: str, client: str, mount_path: str, kind: str):
#         with self._dq_lock:
#             self._delete_queue.append({"name": name, "id": lvol_id, "client": client, "mount_path": mount_path, "kind": kind})

#     def _enqueue_snapshot_delete(self, snap_name: str, snap_id: str):
#         with self._sdq_lock:
#             self._snap_delete_queue.append({"snap_name": snap_name, "snap_id": snap_id})

#     def _unmount_and_disconnect(self, client: str, mount_path: str, lvol_name: str, lvol_id_hint: str = None):
#         self._inc("attempts", "unmount_disconnect", 1)

#         if mount_path:
#             self.ssh_obj.unmount_path(node=client, device=mount_path)

#         lvol_id = lvol_id_hint or self.sbcli_utils.get_lvol_id(lvol_name)
#         if not lvol_id:
#             raise Exception(f"Could not resolve lvol_id for disconnect: {lvol_name}")

#         lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#         nqn = lvol_details[0]["nqn"]
#         self.ssh_obj.disconnect_nvme(node=client, nqn_grep=nqn)

#         self._inc("success", "unmount_disconnect", 1)

#     # ----------------------------
#     # Tasks (each carries ctx and wraps API calls)
#     # ----------------------------
#     def _task_create_lvol(self, idx: int, lvol_name: str):
#         self._inc("attempts", "create_lvol", 1)
#         ctx = {"lvol_name": lvol_name, "idx": idx, "client": self._pick_client(idx)}
#         self.logger.info(f"[create_lvol] {ctx}")

#         self._api("create_lvol", ctx, lambda: self.sbcli_utils.add_lvol(
#             lvol_name=lvol_name,
#             pool_name=self.pool_name,
#             size=self.LVOL_SIZE,
#             distr_ndcs=self.ndcs,
#             distr_npcs=self.npcs,
#             distr_bs=self.bs,
#             distr_chunk_bs=self.chunk_bs,
#         ))

#         lvol_id = self._wait_lvol_id(lvol_name)
#         client = self._pick_client(idx)
#         mount_path = self._connect_format_mount_sanity(client, lvol_name, lvol_id, tag="lvol")

#         with self._lock:
#             self._lvol_registry[lvol_name] = {
#                 "name": lvol_name,
#                 "id": lvol_id,
#                 "client": client,
#                 "mount_path": mount_path,
#                 "snap_state": "pending",
#                 "delete_state": "not_queued",
#             }
#             self._metrics["counts"]["lvols_created"] += 1

#         self._enqueue_delete(name=lvol_name, lvol_id=lvol_id, client=client, mount_path=mount_path, kind="lvol")

#         self._inc("success", "create_lvol", 1)
#         return lvol_name, lvol_id

#     def _task_create_snapshot(self, src_lvol_name: str, src_lvol_id: str, snap_name: str):
#         self._inc("attempts", "create_snapshot", 1)
#         ctx = {"src_lvol_name": src_lvol_name, "src_lvol_id": src_lvol_id, "snap_name": snap_name}
#         self.logger.info(f"[create_snapshot] {ctx}")

#         self._api("create_snapshot", ctx, lambda: self.sbcli_utils.add_snapshot(
#             lvol_id=src_lvol_id,
#             snapshot_name=snap_name,
#         ))

#         snap_id = self._wait_snapshot_id(snap_name)

#         with self._lock:
#             self._snap_registry[snap_name] = {
#                 "snap_name": snap_name,
#                 "snap_id": snap_id,
#                 "src_lvol_name": src_lvol_name,
#                 "clone_state": "pending",
#                 "delete_state": "not_queued",
#             }
#             if src_lvol_name in self._lvol_registry:
#                 self._lvol_registry[src_lvol_name]["snap_state"] = "done"
#             self._metrics["counts"]["snapshots_created"] += 1

#         self._inc("success", "create_snapshot", 1)
#         return snap_name, snap_id

#     def _task_create_clone(self, snap_name: str, snap_id: str, idx: int, clone_name: str):
#         self._inc("attempts", "create_clone", 1)
#         ctx = {"snap_name": snap_name, "snapshot_id": snap_id, "clone_name": clone_name, "client": self._pick_client(idx)}
#         self.logger.info(f"[create_clone] {ctx}")

#         self._api("create_clone", ctx, lambda: self.sbcli_utils.add_clone(
#             snapshot_id=snap_id,
#             clone_name=clone_name,
#         ))

#         clone_lvol_id = self._wait_lvol_id(clone_name)
#         client = self._pick_client(idx)
#         mount_path = self._connect_format_mount_sanity(client, clone_name, clone_lvol_id, tag="clone")

#         with self._lock:
#             self._metrics["counts"]["clones_created"] += 1
#             if snap_name in self._snap_registry:
#                 self._snap_registry[snap_name]["clone_state"] = "done"

#         self._enqueue_delete(name=clone_name, lvol_id=clone_lvol_id, client=client, mount_path=mount_path, kind="clone")

#         # enqueue snapshot delete after clone success
#         with self._lock:
#             meta = self._snap_registry.get(snap_name)
#             if meta and meta["delete_state"] == "not_queued":
#                 meta["delete_state"] = "queued"
#                 self._enqueue_snapshot_delete(meta["snap_name"], meta["snap_id"])

#         self._inc("success", "create_clone", 1)
#         return clone_name, clone_lvol_id

#     def _task_delete_volume(self, item: dict):
#         self._inc("attempts", "delete_lvol", 1)
#         ctx = {"name": item["name"], "id": item.get("id"), "kind": item.get("kind"), "client": item.get("client")}
#         self.logger.info(f"[delete_lvol] {ctx}")

#         name = item["name"]
#         client = item["client"]
#         mount_path = item.get("mount_path")
#         kind = item.get("kind", "lvol")
#         lvol_id = item.get("id")

#         with self._lock:
#             if kind == "lvol" and name in self._lvol_registry:
#                 self._lvol_registry[name]["delete_state"] = "in_progress"

#         self._unmount_and_disconnect(client=client, mount_path=mount_path, lvol_name=name, lvol_id_hint=lvol_id)
#         self._api("delete_lvol", ctx, lambda: self.sbcli_utils.delete_lvol(lvol_name=name, skip_error=False))

#         with self._lock:
#             self._metrics["counts"]["lvols_deleted"] += 1
#             if kind == "clone":
#                 self._metrics["counts"]["clones_deleted"] += 1

#         self._inc("success", "delete_lvol", 1)
#         return name

#     def _task_delete_snapshot(self, snap_name: str, snap_id: str):
#         self._inc("attempts", "delete_snapshot", 1)
#         ctx = {"snap_name": snap_name, "snap_id": snap_id}
#         self.logger.info(f"[delete_snapshot] {ctx}")

#         self._api("delete_snapshot", ctx, lambda: self.sbcli_utils.delete_snapshot(
#             snap_id=snap_id,
#             snap_name=snap_name,
#             skip_error=False,
#         ))

#         with self._lock:
#             self._metrics["counts"]["snapshots_deleted"] += 1
#             if snap_name in self._snap_registry:
#                 self._snap_registry[snap_name]["delete_state"] = "done"

#         self._inc("success", "delete_snapshot", 1)
#         return snap_name

#     # ----------------------------
#     # Scheduling
#     # ----------------------------
#     def _update_peaks(self, create_f, delete_f, snap_f, clone_f, snap_del_f):
#         with self._lock:
#             self._metrics["peak_inflight"]["create"] = max(self._metrics["peak_inflight"]["create"], len(create_f))
#             self._metrics["peak_inflight"]["delete"] = max(self._metrics["peak_inflight"]["delete"], len(delete_f))
#             self._metrics["peak_inflight"]["snapshot"] = max(self._metrics["peak_inflight"]["snapshot"], len(snap_f))
#             self._metrics["peak_inflight"]["clone"] = max(self._metrics["peak_inflight"]["clone"], len(clone_f))
#             self._metrics["peak_inflight"]["snapshot_delete"] = max(self._metrics["peak_inflight"]["snapshot_delete"], len(snap_del_f))

#     def _harvest_fail_fast(self, fut_set: set):
#         done = [f for f in fut_set if f.done()]
#         for f in done:
#             fut_set.remove(f)
#             try:
#                 f.result()
#             except Exception:
#                 return

#     def _submit_creates(self, ex, create_f: set, idx_counter: dict):
#         while (not self._stop_event.is_set()) and (len(create_f) < self.CREATE_INFLIGHT):
#             idx = idx_counter["idx"]
#             idx_counter["idx"] += 1
#             lvol_name = f"lvl{generate_random_sequence(15)}_{idx}_{int(time.time())}"
#             create_f.add(ex.submit(lambda i=idx, n=lvol_name: self._task_create_lvol(i, n)))

#     def _submit_snapshots(self, ex, snap_f: set):
#         while (not self._stop_event.is_set()) and (len(snap_f) < self.SNAPSHOT_INFLIGHT):
#             candidate = None
#             with self._lock:
#                 for nm, meta in self._lvol_registry.items():
#                     if meta["snap_state"] == "pending" and meta["delete_state"] == "not_queued":
#                         meta["snap_state"] = "in_progress"
#                         candidate = (meta["name"], meta["id"])
#                         break
#             if not candidate:
#                 return
#             lvol_name, lvol_id = candidate
#             snap_name = f"snap{generate_random_sequence(15)}_{int(time.time())}"
#             snap_f.add(ex.submit(lambda ln=lvol_name, lid=lvol_id, sn=snap_name: self._task_create_snapshot(ln, lid, sn)))

#     def _submit_clones(self, ex, clone_f: set):
#         while (not self._stop_event.is_set()) and (len(clone_f) < self.CLONE_INFLIGHT):
#             candidate = None
#             with self._lock:
#                 for sn, meta in self._snap_registry.items():
#                     if meta["clone_state"] == "pending":
#                         meta["clone_state"] = "in_progress"
#                         candidate = (meta["snap_name"], meta["snap_id"])
#                         break
#             if not candidate:
#                 return
#             snap_name, snap_id = candidate
#             idx = int(time.time())
#             clone_name = f"cln{generate_random_sequence(15)}_{idx}_{int(time.time())}"
#             clone_f.add(ex.submit(lambda s=snap_name, sid=snap_id, i=idx, cn=clone_name: self._task_create_clone(s, sid, i, cn)))

#     def _submit_deletes(self, ex, del_f: set):
#         while (not self._stop_event.is_set()) and (len(del_f) < self.DELETE_INFLIGHT):
#             with self._dq_lock:
#                 if not self._delete_queue:
#                     return
#                 item = self._delete_queue.popleft()
#             del_f.add(ex.submit(lambda it=item: self._task_delete_volume(it)))

#     def _submit_snapshot_deletes(self, ex, sdel_f: set):
#         while (not self._stop_event.is_set()) and (len(sdel_f) < self.SNAPSHOT_DELETE_INFLIGHT):
#             with self._sdq_lock:
#                 if not self._snap_delete_queue:
#                     return
#                 item = self._snap_delete_queue.popleft()
#             sdel_f.add(ex.submit(lambda sn=item["snap_name"], sid=item["snap_id"]: self._task_delete_snapshot(sn, sid)))

#     # ----------------------------
#     # Summary
#     # ----------------------------
#     def _print_summary(self):
#         with self._lock:
#             self._metrics["end_ts"] = time.time()
#             dur = self._metrics["end_ts"] - self._metrics["start_ts"] if self._metrics["start_ts"] else None

#             self.logger.info("======== TEST SUMMARY (parallel continuous) ========")
#             self.logger.info(f"Duration (sec): {dur:.1f}" if dur else "Duration (sec): n/a")
#             self.logger.info(f"Loops: {self._metrics['loops']}")
#             self.logger.info(f"Max workers: {self._metrics['max_workers']}")
#             self.logger.info(f"Targets: {self._metrics['targets']}")
#             self.logger.info(f"Peak inflight: {self._metrics['peak_inflight']}")
#             self.logger.info(f"Counts: {self._metrics['counts']}")
#             self.logger.info(f"Attempts: {self._metrics['attempts']}")
#             self.logger.info(f"Success: {self._metrics['success']}")
#             self.logger.info(f"Failures: {self._metrics['failures']}")
#             self.logger.info(f"Failure info: {self._metrics['failure_info']}")
#             self.logger.info("====================================================")

#     # ----------------------------
#     # Main
#     # ----------------------------
#     def run(self):
#         self.logger.info("=== Starting TestParallelLvolSnapshotCloneAPI (continuous until failure) ===")

#         self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
#         sleep_n_sec(2)

#         max_workers = (
#             self.CREATE_INFLIGHT
#             + self.DELETE_INFLIGHT
#             + self.SNAPSHOT_INFLIGHT
#             + self.CLONE_INFLIGHT
#             + self.SNAPSHOT_DELETE_INFLIGHT
#             + 10
#         )

#         with self._lock:
#             self._metrics["start_ts"] = time.time()
#             self._metrics["max_workers"] = max_workers

#         create_f = set()
#         delete_f = set()
#         snap_f = set()
#         clone_f = set()
#         sdel_f = set()

#         idx_counter = {"idx": 0}

#         try:
#             with ThreadPoolExecutor(max_workers=max_workers) as ex:
#                 self._submit_creates(ex, create_f, idx_counter)

#                 while not self._stop_event.is_set():
#                     if os.path.exists(self.STOP_FILE):
#                         self.logger.info(f"Stop file found: {self.STOP_FILE}. Stopping gracefully.")
#                         break
#                     if self.MAX_RUNTIME_SEC and (time.time() - self._metrics["start_ts"]) > self.MAX_RUNTIME_SEC:
#                         self.logger.info("MAX_RUNTIME_SEC reached. Stopping gracefully.")
#                         break

#                     with self._lock:
#                         self._metrics["loops"] += 1

#                     self._submit_creates(ex, create_f, idx_counter)
#                     self._submit_snapshots(ex, snap_f)
#                     self._submit_clones(ex, clone_f)
#                     self._submit_deletes(ex, delete_f)
#                     self._submit_snapshot_deletes(ex, sdel_f)

#                     self._update_peaks(create_f, delete_f, snap_f, clone_f, sdel_f)

#                     self._harvest_fail_fast(create_f)
#                     self._harvest_fail_fast(snap_f)
#                     self._harvest_fail_fast(clone_f)
#                     self._harvest_fail_fast(delete_f)
#                     self._harvest_fail_fast(sdel_f)

#                     sleep_n_sec(1)

#         finally:
#             self._print_summary()

#         with self._lock:
#             failure_info = self._metrics["failure_info"]

#         if failure_info:
#             raise Exception(f"Test stopped due to failure: {failure_info}")

#         raise Exception("Test stopped without failure (graceful stop).")


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
        self.CREATE_INFLIGHT = 10
        self.SNAPSHOT_INFLIGHT = 10
        self.CLONE_INFLIGHT = 10
        self.SNAPSHOT_DELETE_TREE_INFLIGHT = 5
        self.LVOL_DELETE_TREE_INFLIGHT = 5

        # Inventory targets (high-water marks) to ensure snapshots/clones exist
        self.TARGET_SNAP_INVENTORY_MIN = 50   # try to keep at least this many snapshots around
        self.TARGET_SNAP_INVENTORY_MAX = 60   # if above, snapshot deletes may kick in
        self.TARGET_CLONE_INVENTORY_MIN = 10  # keep at least this many clones around
        self.TARGET_CLONE_INVENTORY_MAX = 20  # if above, clone deletions will happen via snapshot/LVOL delete trees

        # LVOL sizing
        self.LVOL_SIZE = "10G"

        # Mount base
        self.MOUNT_BASE = "/mnt/test_location"

        # Optional stop controls
        self.STOP_FILE = "/tmp/stop_api_stress"
        self.MAX_RUNTIME_SEC = None

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
                "snap_inventory_min": self.TARGET_SNAP_INVENTORY_MIN,
                "snap_inventory_max": self.TARGET_SNAP_INVENTORY_MAX,
                "clone_inventory_min": self.TARGET_CLONE_INVENTORY_MIN,
                "clone_inventory_max": self.TARGET_CLONE_INVENTORY_MAX,
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

    def _api(self, op: str, ctx: dict, fn):
        try:
            return fn()
        except Exception as e:
            api_err = self._extract_api_error(e)
            self._inc("failures", op if op in self._metrics["failures"] else "unknown", 1)
            self._set_failure(op=op, exc=e, details="api call failed", ctx=ctx, api_err=api_err)
            raise

    # ----------------------------
    # Helpers
    # ----------------------------
    def _pick_client(self, i: int) -> str:
        return self.client_machines[i % len(self.client_machines)]

    def _wait_lvol_id(self, lvol_name: str, timeout=300, interval=5) -> str:
        start = time.time()
        while time.time() - start < timeout:
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            if lvol_id:
                return lvol_id
            sleep_n_sec(interval)
        raise TimeoutError(f"LVOL id not visible for {lvol_name} after {timeout}s")

    def _wait_snapshot_id(self, snap_name: str, timeout=300, interval=5) -> str:
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
        ctx = {"lvol_name": lvol_name, "idx": idx, "client": self._pick_client(idx)}
        self.logger.info(f"[create_lvol] ctx={ctx}")

        self._api("create_lvol", ctx, lambda: self.sbcli_utils.add_lvol(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size=self.LVOL_SIZE,
            distr_ndcs=self.ndcs,
            distr_npcs=self.npcs,
            distr_bs=self.bs,
            distr_chunk_bs=self.chunk_bs,
        ))

        lvol_id = self._wait_lvol_id(lvol_name)
        client = self._pick_client(idx)
        mount_path = self._connect_format_mount_sanity(client, lvol_name, lvol_id, tag="lvol")

        with self._lock:
            self._lvol_registry[lvol_name] = {
                "id": lvol_id,
                "client": client,
                "mount_path": mount_path,
                "snap_state": "pending",
                "delete_state": "not_queued",   # IMPORTANT: do NOT queue delete immediately
                "snapshots": set(),
            }
            self._metrics["counts"]["lvols_created"] += 1

        self._inc("success", "create_lvol", 1)
        return lvol_name, lvol_id

    def _task_create_snapshot(self, src_lvol_name: str, src_lvol_id: str, snap_name: str):
        self._inc("attempts", "create_snapshot", 1)
        ctx = {"src_lvol_name": src_lvol_name, "src_lvol_id": src_lvol_id, "snap_name": snap_name}
        self.logger.info(f"[create_snapshot] ctx={ctx}")

        self._api("create_snapshot", ctx, lambda: self.sbcli_utils.add_snapshot(
            lvol_id=src_lvol_id,
            snapshot_name=snap_name,
        ))

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

    def _task_create_clone(self, snap_name: str, snap_id: str, idx: int, clone_name: str):
        self._inc("attempts", "create_clone", 1)
        ctx = {"snap_name": snap_name, "snapshot_id": snap_id, "clone_name": clone_name, "client": self._pick_client(idx)}
        self.logger.info(f"[create_clone] ctx={ctx}")

        self._api("create_clone", ctx, lambda: self.sbcli_utils.add_clone(
            snapshot_id=snap_id,
            clone_name=clone_name,
        ))

        clone_lvol_id = self._wait_lvol_id(clone_name)
        client = self._pick_client(idx)
        mount_path = self._connect_format_mount_sanity(client, clone_name, clone_lvol_id, tag="clone")

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
        self._api("delete_snapshot", ctx, lambda: self.sbcli_utils.delete_snapshot(
            snap_id=snap_id, snap_name=snap_name, skip_error=False
        ))

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
        self._api("delete_lvol", ctx, lambda: self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=False))

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
          - delete all clones for that snapshot
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
            clones = list(meta["clones"])
            src_lvol = meta["src_lvol_name"]

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
            snap_names = list(meta["snapshots"])

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
        Keep snapshots ~50-60 and clones ~10-20.

        Strategy:
          - If clones > CLONE_MAX: start deleting snapshot trees for snapshots that have clones.
          - If snapshots > SNAP_MAX: start deleting snapshot trees for snapshots with 0 clones first, else with clones.
          - If LVOL count grows too large, start deleting LVOL trees for oldest-ish lvols.
        """
        with self._lock:
            snap_count = len(self._snap_registry)
            clone_count = len(self._clone_registry)
            lvol_count = len(self._lvol_registry)

            # Pick snapshot delete candidates
            if clone_count > self.TARGET_CLONE_INVENTORY_MAX:
                # delete snapshots that have clones (will delete clones too)
                for sn, sm in self._snap_registry.items():
                    if sm["delete_state"] == "not_queued" and len(sm["clones"]) > 0:
                        sm["delete_state"] = "queued"
                        self._snapshot_delete_tree_q.append(sn)
                        if len(self._snapshot_delete_tree_q) >= 10:
                            break

            if snap_count > self.TARGET_SNAP_INVENTORY_MAX:
                # prefer snapshots with no clones first (cheaper), else any
                added = 0
                for sn, sm in self._snap_registry.items():
                    if sm["delete_state"] == "not_queued" and len(sm["clones"]) == 0:
                        sm["delete_state"] = "queued"
                        self._snapshot_delete_tree_q.append(sn)
                        added += 1
                        if added >= 10:
                            break
                if added == 0:
                    for sn, sm in self._snap_registry.items():
                        if sm["delete_state"] == "not_queued":
                            sm["delete_state"] = "queued"
                            self._snapshot_delete_tree_q.append(sn)
                            added += 1
                            if added >= 10:
                                break

            # Backpressure for LVOL inventory: if it grows too much, delete LVOL trees
            # (this is what prevents "stopping at 300 volumes")
            # You can tune this; it basically keeps LVOLs bounded even if snapshot creation is fast.
            LVOL_HIGH_WATER = max(200, self.TARGET_SNAP_INVENTORY_MAX + 100)
            if lvol_count > LVOL_HIGH_WATER:
                added = 0
                for ln, lm in self._lvol_registry.items():
                    if lm["delete_state"] == "not_queued":
                        # only delete lvols that already have at least 1 snapshot or are old-ish
                        lm["delete_state"] = "queued"
                        self._lvol_delete_tree_q.append(ln)
                        added += 1
                        if added >= 10:
                            break

    # ----------------------------
    # Scheduler submitters
    # ----------------------------
    def _submit_creates(self, ex, create_f: set, idx_counter: dict):
        while (not self._stop_event.is_set()) and (len(create_f) < self.CREATE_INFLIGHT):
            idx = idx_counter["idx"]
            idx_counter["idx"] += 1
            lvol_name = f"lvl{generate_random_sequence(15)}_{idx}_{int(time.time())}"
            create_f.add(ex.submit(lambda i=idx, n=lvol_name: self._task_create_lvol(i, n)))

    def _submit_snapshots(self, ex, snap_f: set):
        # Keep snapshot creation rolling until we have >= SNAP_MIN
        with self._lock:
            snap_count = len(self._snap_registry)

        if snap_count >= self.TARGET_SNAP_INVENTORY_MIN:
            # Still keep inflight steady, but no need to over-push if inventory is healthy
            pass

        while (not self._stop_event.is_set()) and (len(snap_f) < self.SNAPSHOT_INFLIGHT):
            candidate = None
            with self._lock:
                for lvol_name, lm in self._lvol_registry.items():
                    if lm["delete_state"] == "not_queued" and lm["snap_state"] == "pending":
                        lm["snap_state"] = "in_progress"
                        candidate = (lvol_name, lm["id"])
                        break
            if not candidate:
                return

            lvol_name, lvol_id = candidate
            snap_name = f"snap{generate_random_sequence(15)}_{int(time.time())}"
            snap_f.add(ex.submit(lambda ln=lvol_name, lid=lvol_id, sn=snap_name: self._task_create_snapshot(ln, lid, sn)))

    def _submit_clones(self, ex, clone_f: set):
        # keep at least CLONE_MIN around
        with self._lock:
            clone_count = len(self._clone_registry)

        if clone_count < self.TARGET_CLONE_INVENTORY_MIN:
            # push clones harder (still limited by inflight)
            pass

        while (not self._stop_event.is_set()) and (len(clone_f) < self.CLONE_INFLIGHT):
            candidate = None
            with self._lock:
                for sn, sm in self._snap_registry.items():
                    if sm["clone_state"] == "pending":
                        sm["clone_state"] = "in_progress"
                        candidate = (sn, sm["snap_id"])
                        break
            if not candidate:
                return

            snap_name, snap_id = candidate
            idx = int(time.time())
            clone_name = f"cln{generate_random_sequence(15)}_{idx}_{int(time.time())}"
            clone_f.add(ex.submit(lambda s=snap_name, sid=snap_id, i=idx, cn=clone_name: self._task_create_clone(s, sid, i, cn)))

    def _submit_snapshot_delete_trees(self, ex, snap_del_f: set):
        while (not self._stop_event.is_set()) and (len(snap_del_f) < self.SNAPSHOT_DELETE_TREE_INFLIGHT):
            with self._lock:
                if not self._snapshot_delete_tree_q:
                    return
                sn = self._snapshot_delete_tree_q.popleft()
            snap_del_f.add(ex.submit(lambda sn=sn: self._task_delete_snapshot_tree(sn)))

    def _submit_lvol_delete_trees(self, ex, lvol_del_f: set):
        while (not self._stop_event.is_set()) and (len(lvol_del_f) < self.LVOL_DELETE_TREE_INFLIGHT):
            with self._lock:
                if not self._lvol_delete_tree_q:
                    return
                ln = self._lvol_delete_tree_q.popleft()
            lvol_del_f.add(ex.submit(lambda ln=ln: self._task_delete_lvol_tree(ln)))

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

    def _harvest_fail_fast(self, fut_set: set):
        done = [f for f in fut_set if f.done()]
        for f in done:
            fut_set.remove(f)
            try:
                f.result()
            except Exception:
                return

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
            self.logger.info(
                f"Live inventory now: lvols={len(self._lvol_registry)} snaps={len(self._snap_registry)} clones={len(self._clone_registry)}"
            )
            self.logger.info("===========================================================")

    # ----------------------------
    # Main
    # ----------------------------
    def run(self):
        self.logger.info("=== Starting TestParallelLvolSnapshotCloneAPI (steady snapshots/clones) ===")

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        sleep_n_sec(2)

        max_workers = (
            self.CREATE_INFLIGHT
            + self.SNAPSHOT_INFLIGHT
            + self.CLONE_INFLIGHT
            + self.SNAPSHOT_DELETE_TREE_INFLIGHT
            + self.LVOL_DELETE_TREE_INFLIGHT
            + 10
        )

        with self._lock:
            self._metrics["start_ts"] = time.time()
            self._metrics["max_workers"] = max_workers

        create_f = set()
        snap_f = set()
        clone_f = set()
        snap_del_f = set()
        lvol_del_f = set()

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

        finally:
            self._print_summary()

        with self._lock:
            failure_info = self._metrics["failure_info"]

        if failure_info:
            raise Exception(f"Test stopped due to failure: {failure_info}")

        raise Exception("Test stopped without failure (graceful stop).")

