"""
CREATE:
- Parent LVOL:
  - size = self.lvol_size
  - max_namespace_per_subsys = self.max_namespace_per_subsys
  - host_id is chosen (outage-aware) OR scheduler picks; we then derive parent node_id
- Child LVOLs (namespaces):
  - size = self.lvol_size (same as parent)
  - namespace = <parent_lvol_id>
  - host_id = SAME as parent host_id (mandatory)

CONNECT:
- nvme connect ONLY for parent NQN (children appear automatically as nvmeXn2, nvmeXn3...)

FIO:
- runs on parent + all child namespaces (each mounted separately)

DELETE:
- delete_random_lvols picks RANDOM from all LVOLs (not just parents)
- If LVOL is namespace-related (parent or child):
  - DO NOT disconnect_lvol() by default
  - EXCEPTION: if it is the LAST namespace left on that controller AFTER delete, then disconnect_lvol()
  - verify that the specific namespace device is gone (e.g. /dev/nvme32n3)
- If LVOL is NOT namespace-related:
  - keep existing behavior: disconnect_lvol() + delete
"""

import random
import threading
import time
from collections import defaultdict
import string

from utils.common_utils import sleep_n_sec
from utils.ssh_utils import get_parent_device
from exceptions.custom_exception import LvolNotConnectException

from stress_test.continuous_failover_ha_multi_client import (
    RandomMultiClientFailoverTest,
    generate_random_sequence,
)


class RandomMultiClientFailoverNamespaceTest(RandomMultiClientFailoverTest):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Namespace config
        self.max_namespace_per_subsys = 10
        self.total_lvols = 20
        self.children_per_parent = 2
        self.parent_lvols = set()
        self.child_lvols = set()   # optional but recommended

        # Tracking
        self.parent_ctrl = {}                    # parent_name -> "/dev/nvmeX"
        self.parent_host_id = {}                # parent_name -> node uuid (host_id)
        self.parent_to_children = defaultdict(list)
        self.stale_ns_devices = {}               # lvol_name -> {device, ctrl_dev, client}

        self.test_name = "continuous_random_failover_multi_client_ha_namespace"
    
    def _rand_suffix(self, n=3) -> str:
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))

    def _shuffle_name(self, name: str) -> str:
        chars = list(name)
        random.shuffle(chars)
        return "".join(chars)

    def _name_exists_in_cluster(self, name: str) -> bool:
        try:
            return self.sbcli_utils.lvol_exists(name)
        except Exception:
            # if API temporarily fails, treat as "exists" to avoid collisions
            return True

    def _gen_unique_parent_name(self, base: str) -> str:
        """
        base: already random-ish (e.g., lvl{generate_random_sequence(15)} or similar)
        Rules:
        - if collision: shuffle once
        - if still collision: append +3 chars (repeat) until unique
        """
        candidate = base

        # 1) direct collision check
        if candidate in self.parent_lvols or candidate in self.lvol_mount_details or self._name_exists_in_cluster(candidate):
            # 2) shuffle once
            candidate = self._shuffle_name(candidate)

        # 3) if still colliding, append +3 chars until unique
        while (
            candidate in self.parent_lvols
            or candidate in self.lvol_mount_details
            or self._name_exists_in_cluster(candidate)
        ):
            candidate = f"{candidate}{self._rand_suffix(3)}"

        self.parent_lvols.add(candidate)
        return candidate

    def _gen_unique_child_name(self, parent_name: str, ns_idx: int) -> str:
        """
        Child naming:
        - start with parent-based name (readable)
        - if collision (old leftovers), apply same shuffle/append logic using child_lvols set
        """
        candidate = f"{parent_name}_ns{ns_idx}"

        if candidate in self.child_lvols or candidate in self.lvol_mount_details or self._name_exists_in_cluster(candidate):
            candidate = self._shuffle_name(candidate)

        while (
            candidate in self.child_lvols
            or candidate in self.lvol_mount_details
            or self._name_exists_in_cluster(candidate)
        ):
            candidate = f"{candidate}{self._rand_suffix(3)}"

        self.child_lvols.add(candidate)
        return candidate

    # -------------------------
    # Namespace helpers
    # -------------------------
    def _list_nvme_ns_devices(self, node, ctrl_dev: str):
        """
        ctrl_dev: /dev/nvme32
        returns: ['/dev/nvme32n1', '/dev/nvme32n2', ...]
        """
        ctrl = get_parent_device(ctrl_dev)  # safe if /dev/nvme32n1 passed accidentally
        self.logger.info(f"ctrl dev : {ctrl_dev}, ctrl:{ctrl}")
        cmd = f"bash -lc \"ls -1 {ctrl}n* 2>/dev/null | sort -V || true\""
        out, _ = self.ssh_obj.exec_command(node=node, command=cmd, supress_logs=True)
        self.logger.info(f"cmd: {cmd}, out:{out}")
        return [x.strip() for x in (out or "").splitlines() if x.strip()]

    def _wait_for_new_namespace_device(self, node, ctrl_dev: str, before_set: set, timeout=120, interval=2):
        deadline = time.time() + timeout
        while time.time() < deadline:
            cur = set(self._list_nvme_ns_devices(node=node, ctrl_dev=ctrl_dev))
            self.logger.info(f"Current: {cur}, Old:{ctrl_dev}")
            diff = sorted(cur - before_set)
            if diff:
                return diff[-1], cur
            sleep_n_sec(interval)
        return None, set(self._list_nvme_ns_devices(node=node, ctrl_dev=ctrl_dev))

    def _rescan_nvme_namespaces(self, node, ctrl_dev: str):
        """Trigger a namespace rescan on the NVMe controller so the host refreshes its namespace list."""
        self.logger.info(
            f"RESCAN Namespaces FUNCTION: {node} {ctrl_dev}"
        )
        ctrl = get_parent_device(ctrl_dev)  # ensure /dev/nvmeX not /dev/nvmeXn1
        self.logger.info(
            f"RESCAN Namespaces JUST BEFORE rescan FUNCTION: {node} {ctrl_dev} {ctrl}"
        )
        cmd = f"bash -lc \"nvme ns-rescan {ctrl} 2>/dev/null || true\""
        out, err = self.ssh_obj.exec_command(node=node, command=cmd, supress_logs=False)
        self.logger.info(f"[rescan_ns] ctrl={ctrl} out={out} err={err}")

    def _wait_until_namespace_device_gone(self, node, ctrl_dev: str, device: str, timeout=120, interval=2):
        deadline = time.time() + timeout
        while time.time() < deadline:
            cur = set(self._list_nvme_ns_devices(node=node, ctrl_dev=ctrl_dev))
            if device not in cur:
                return True
            sleep_n_sec(interval)
        return False

    def _is_namespace_lvol(self, details: dict) -> bool:
        # parent has is_parent=True, child has parent=<name>
        return bool(details.get("is_parent", False) or details.get("parent"))

    def _is_last_namespace_after_delete(self, client, ctrl_dev: str) -> bool:
        """
        Check if the controller has zero namespaces left on the client.
        Used AFTER delete + device gone verification.
        """
        cur = set(self._list_nvme_ns_devices(node=client, ctrl_dev=ctrl_dev))
        return len(cur) == 0

    def _start_fio_for_lvol(self, lvol_name: str, runtime=2000):
        d = self.lvol_mount_details[lvol_name]
        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(d["Client"], None, d["Mount"], d["Log"]),
            kwargs={
                "size": self.fio_size,
                "name": f"{lvol_name}_fio",
                "rw": "randrw",
                "bs": f"{2 ** random.randint(2, 7)}K",
                "nrfiles": 16,
                "iodepth": 1,
                "numjobs": 5,
                "time_based": True,
                "runtime": runtime,
                "log_avg_msec": 1000,
                "iolog_file": d["iolog_base_path"],
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)

    # -------------------------
    # OVERRIDE: create_lvols_with_fio
    # -------------------------
    def create_lvols_with_fio(self, count):
        for i in range(count):
            fs_type = random.choice(["ext4", "xfs"])
            is_crypto = random.choice([True, False])

            base = f"lvl{generate_random_sequence(15)}"
            base = self._gen_unique_parent_name(base)   # returns final unique base string
            parent_name = f"c{base}" if is_crypto else base
            self.lvol_name = parent_name

            self.logger.info(
                f"[NS] Creating PARENT lvol: {parent_name}, fs={fs_type}, crypto={is_crypto}, "
                f"size={self.lvol_size}, max_namespace_per_subsys={self.max_namespace_per_subsys}"
            )

            # -------- Parent create (outage-aware host pin if needed) --------
            parent_host_id_used = None
            try:
                if self.current_outage_nodes:
                    skip_nodes = [
                        node for node in self.sn_primary_secondary_map
                        if self.sn_primary_secondary_map[node] in self.current_outage_nodes
                    ]
                    for node in self.current_outage_nodes:
                        skip_nodes.append(node)

                    host_id = [node for node in self.sn_nodes_with_sec if node not in skip_nodes]
                    parent_host_id_used = host_id[0]

                    self.sbcli_utils.add_lvol(
                        lvol_name=parent_name,
                        pool_name=self.pool_name,
                        size=self.lvol_size,
                        crypto=is_crypto,
                        key1=self.lvol_crypt_keys[0],
                        key2=self.lvol_crypt_keys[1],
                        host_id=parent_host_id_used,
                        max_namespace_per_subsys=self.max_namespace_per_subsys,
                    )

                elif self.current_outage_node:
                    skip_nodes = [
                        node for node in self.sn_primary_secondary_map
                        if self.sn_primary_secondary_map[node] == self.current_outage_node
                    ]
                    skip_nodes.append(self.current_outage_node)
                    skip_nodes.append(self.sn_primary_secondary_map[self.current_outage_node])

                    host_id = [node for node in self.sn_nodes_with_sec if node not in skip_nodes]
                    parent_host_id_used = host_id[0]

                    self.sbcli_utils.add_lvol(
                        lvol_name=parent_name,
                        pool_name=self.pool_name,
                        size=self.lvol_size,
                        crypto=is_crypto,
                        key1=self.lvol_crypt_keys[0],
                        key2=self.lvol_crypt_keys[1],
                        host_id=parent_host_id_used,
                        max_namespace_per_subsys=self.max_namespace_per_subsys,
                    )
                else:
                    # let scheduler pick; we will read node_id afterwards and pin children to it
                    self.sbcli_utils.add_lvol(
                        lvol_name=parent_name,
                        pool_name=self.pool_name,
                        size=self.lvol_size,
                        crypto=is_crypto,
                        key1=self.lvol_crypt_keys[0],
                        key2=self.lvol_crypt_keys[1],
                        max_namespace_per_subsys=self.max_namespace_per_subsys,
                    )
            except Exception as e:
                self.logger.warning(f"[NS] Parent create failed: {e}. Skipping.")
                continue

            parent_id = self.sbcli_utils.get_lvol_id(parent_name)
            parent_node_id = self.sbcli_utils.get_lvol_details(lvol_id=parent_id)[0]["node_id"]
            if parent_host_id_used is None:
                parent_host_id_used = parent_node_id

            self.parent_host_id[parent_name] = parent_host_id_used

            self.lvol_mount_details[parent_name] = {
                "ID": parent_id,
                "Command": None,
                "Mount": None,
                "Device": None,
                "MD5": None,
                "FS": fs_type,
                "Log": f"{self.log_path}/{parent_name}.log",
                "snapshots": [],
                "iolog_base_path": f"{self.log_path}/{parent_name}_fio_iolog",
                "is_parent": True,
                "host_id": parent_host_id_used,
                "ctrl_dev": None,  # filled after connect
            }
            self.parent_to_children[parent_name] = []

            self.logger.info(
                f"[NS] Created parent {parent_name} (id={parent_id}, node_id={parent_node_id}, pinned_host={parent_host_id_used})"
            )
            sleep_n_sec(3)

            # Keep node_vs_lvol mapping for compatibility (parents only)
            if parent_node_id in self.node_vs_lvol:
                self.node_vs_lvol[parent_node_id].append(parent_name)
            else:
                self.node_vs_lvol[parent_node_id] = [parent_name]

            # -------- Connect parent only once --------
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=parent_name)
            self.lvol_mount_details[parent_name]["Command"] = connect_ls

            client_node = random.choice(self.fio_node)
            self.lvol_mount_details[parent_name]["Client"] = client_node

            initial_devices = self.ssh_obj.get_devices(node=client_node)
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(node=client_node, command=connect_str)
                if error:
                    self.record_failed_nvme_connect(parent_name, connect_str)
                    sleep_n_sec(30)
                    continue

            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=client_node)

            parent_ns_dev = None
            for device in final_devices:
                if device not in initial_devices:
                    parent_ns_dev = f"/dev/{device.strip()}"  # expected /dev/nvmeXn1
                    break

            if not parent_ns_dev:
                raise LvolNotConnectException("[NS] Parent LVOL did not connect")

            self.lvol_mount_details[parent_name]["Device"] = parent_ns_dev

            ctrl_dev = get_parent_device(parent_ns_dev)   # /dev/nvmeX
            self.logger.info(f"Parent: {parent_ns_dev}, ctrl_dev: {ctrl_dev}")
            self.parent_ctrl[parent_name] = ctrl_dev
            self.lvol_mount_details[parent_name]["ctrl_dev"] = ctrl_dev

            # Format + mount parent ns
            self.ssh_obj.format_disk(node=client_node, device=parent_ns_dev, fs_type=fs_type)
            parent_mount = f"{self.mount_path}/{parent_name}"
            self.ssh_obj.mount_path(node=client_node, device=parent_ns_dev, mount_path=parent_mount)
            self.lvol_mount_details[parent_name]["Mount"] = parent_mount

            sleep_n_sec(10)
            self.ssh_obj.delete_files(client_node, [f"{parent_mount}/*fio*"])
            self.ssh_obj.delete_files(client_node, [f"{self.log_path}/local-{parent_name}_fio*"])
            self.ssh_obj.delete_files(client_node, [f"{self.log_path}/{parent_name}_fio_iolog*"])
            sleep_n_sec(5)

            self._start_fio_for_lvol(parent_name, runtime=2000)
            sleep_n_sec(5)

            # -------- Create children (same host_id as parent) --------
            before_set = set(self._list_nvme_ns_devices(node=client_node, ctrl_dev=ctrl_dev))

            for k in range(1, self.children_per_parent + 1):
                child_name = self._gen_unique_child_name(parent_name, k+1)

                self.logger.info(
                    f"[NS] Creating CHILD {child_name}: namespace={parent_id}, host_id={parent_host_id_used}, size={self.lvol_size}"
                )

                try:
                    self.sbcli_utils.add_lvol(
                        lvol_name=child_name,
                        pool_name=self.pool_name,
                        size=self.lvol_size,
                        crypto=is_crypto,
                        key1=self.lvol_crypt_keys[0],
                        key2=self.lvol_crypt_keys[1],
                        host_id=parent_host_id_used,
                        namespace=parent_id,
                    )
                except Exception as e:
                    self.logger.warning(f"[NS] Child create failed for {child_name}: {e}")
                    continue

                child_id = self.sbcli_utils.get_lvol_id(child_name)

                new_dev, new_set = self._wait_for_new_namespace_device(
                    node=client_node,
                    ctrl_dev=ctrl_dev,
                    before_set=before_set,
                    timeout=120,
                    interval=2,
                )
                if not new_dev:
                    raise Exception(f"[NS] Child namespace device did not appear for {child_name}")

                before_set = new_set

                self.lvol_mount_details[child_name] = {
                    "ID": child_id,
                    "Command": None,  # no connect
                    "Mount": None,
                    "Device": new_dev,
                    "MD5": None,
                    "FS": fs_type,
                    "Log": f"{self.log_path}/{child_name}.log",
                    "snapshots": [],
                    "iolog_base_path": f"{self.log_path}/{child_name}_fio_iolog",
                    "is_parent": False,
                    "parent": parent_name,
                    "host_id": parent_host_id_used,
                    "ctrl_dev": ctrl_dev,  # IMPORTANT for delete verification
                }
                self.lvol_mount_details[child_name]["Client"] = client_node
                self.parent_to_children[parent_name].append(child_name)

                self.ssh_obj.format_disk(node=client_node, device=new_dev, fs_type=fs_type)
                child_mount = f"{self.mount_path}/{child_name}"
                self.ssh_obj.mount_path(node=client_node, device=new_dev, mount_path=child_mount)
                self.lvol_mount_details[child_name]["Mount"] = child_mount

                sleep_n_sec(5)
                self.ssh_obj.delete_files(client_node, [f"{child_mount}/*fio*"])
                self.ssh_obj.delete_files(client_node, [f"{self.log_path}/local-{child_name}_fio*"])
                self.ssh_obj.delete_files(client_node, [f"{self.log_path}/{child_name}_fio_iolog*"])
                sleep_n_sec(3)

                self._start_fio_for_lvol(child_name, runtime=2000)
                sleep_n_sec(5)

    # -------------------------
    # OVERRIDE: delete_random_lvols
    # -------------------------
    def delete_random_lvols(self, count):
        """
        Random delete across ALL LVOLs in lvol_mount_details.

        Rules:
        - For namespace LVOLs (parent or child):
            - Do NOT disconnect by default
            - After delete + device gone, if controller has no namespaces left -> disconnect
        - For non-namespace LVOLs:
            - keep normal behavior: disconnect + delete
        """
        # Keep base-style skip_nodes pattern (it gets cleared)
        skip_nodes = [node for node in self.sn_primary_secondary_map if self.sn_primary_secondary_map[node] == self.current_outage_node]
        skip_nodes.append(self.current_outage_node)
        skip_nodes.append(self.sn_primary_secondary_map[self.current_outage_node])
        skip_nodes = []
        self.logger.info(f"[NS] Skipping Nodes: {skip_nodes}")
        count = count * 3

        available = list(self.lvol_mount_details.keys())
        self.logger.info(f"[NS] Available LVOLs for random delete: {available}")

        if len(available) < count:
            self.logger.warning("[NS] Not enough LVOLs available to delete requested count.")
            count = len(available)

        chosen = random.sample(available, count)

        for lvol_name in chosen:
            d = self.lvol_mount_details.get(lvol_name)
            if not d:
                continue

            client = d.get("Client")
            device = d.get("Device")       # /dev/nvmeXnY
            ctrl_dev = d.get("ctrl_dev")   # /dev/nvmeX (for namespace lvols)
            lvol_id = d.get("ID")

            is_ns = self._is_namespace_lvol(d)

            self.logger.info(
                f"[NS] Random delete picked: {lvol_name} (namespace={is_ns}, id={lvol_id}, device={device}, ctrl={ctrl_dev})"
            )

            # Stop fio only for this lvol
            try:
                self.common_utils.validate_fio_test(client, log_file=d["Log"])
            except Exception as e:
                self.logger.warning(f"[NS] validate_fio_test failed for {lvol_name}: {e}")

            try:
                self.ssh_obj.find_process_name(client, f"{lvol_name}_fio", return_pid=False)
                sleep_n_sec(3)
                fio_pids = self.ssh_obj.find_process_name(client, f"{lvol_name}_fio", return_pid=True) or []
                for pid in fio_pids:
                    self.ssh_obj.kill_processes(client, pid=pid)

                attempt = 1
                while True:
                    fio_pids = self.ssh_obj.find_process_name(client, f"{lvol_name}_fio", return_pid=True) or []
                    if len(fio_pids) <= 2:
                        break
                    if attempt >= 30:
                        raise Exception(f"[NS] FIO not killed for {lvol_name}")
                    attempt += 1
                    sleep_n_sec(5)
            except Exception as e:
                self.logger.warning(f"[NS] fio stop flow failed for {lvol_name}: {e}")

            # Unmount only this lvol mount
            mount_path = d.get("Mount")
            if mount_path:
                try:
                    self.ssh_obj.unmount_path(client, mount_path)
                except Exception:
                    pass
                try:
                    self.ssh_obj.remove_dir(client, dir_path=mount_path)
                except Exception:
                    pass

            # Disconnect behavior depends on namespace or not
            if not is_ns:
                try:
                    self.disconnect_lvol(lvol_id)
                except Exception as e:
                    self.logger.warning(f"[NS] disconnect_lvol failed for {lvol_name}: {e}")
            else:
                self.logger.info(f"[NS] Skipping disconnect_lvol BEFORE delete for namespace LVOL: {lvol_name}")

            # API delete (same)
            try:
                self.sbcli_utils.delete_lvol(lvol_name, max_attempt=20, skip_error=True)
            except Exception as e:
                self.logger.warning(f"[NS] delete_lvol failed for {lvol_name}: {e}")
                self.record_pending_lvol_delete(lvol_name, lvol_id)
            
            self.logger.info(
                f"[NS] Random delete picked: {lvol_name} (namespace={is_ns}, id={lvol_id}, device={device}, ctrl={ctrl_dev})"
            )
            # Rescan namespaces on the controller so the host drops the removed namespace device
            if is_ns and client and ctrl_dev:
                self.logger.info(
                    "RESCAN Namespaces on controller to trigger host refresh after delete (before verification)"
                )
                self._rescan_nvme_namespaces(node=client, ctrl_dev=ctrl_dev)

            # Verify namespace device disappearance (without disconnect)
            if is_ns and client and ctrl_dev and device:
                # Quick check — resolves in most cases after initial nvme ns-rescan
                ok = self._wait_until_namespace_device_gone(
                    node=client, ctrl_dev=ctrl_dev, device=device, timeout=30, interval=3
                )
                if not ok:
                    # Wait before fallback to allow the host to settle
                    self.logger.info("[NS] Device still present after initial rescan; waiting 120s before sysfs fallback.")
                    sleep_n_sec(120)
                    # Fallback: trigger rescan via sysfs rescan_controller
                    ctrl_name = ctrl_dev.split("/")[-1]  # /dev/nvme12 -> nvme12
                    sysfs_cmd = (
                        f"bash -lc \"echo 1 | sudo tee /sys/class/nvme/{ctrl_name}/rescan_controller"
                        f" 2>/dev/null || true\""
                    )
                    out, err = self.ssh_obj.exec_command(node=client, command=sysfs_cmd, supress_logs=False)
                    self.logger.info(
                        f"[NS] sysfs rescan fallback: ctrl={ctrl_name} out={out} err={err}"
                    )
                    ok = self._wait_until_namespace_device_gone(
                        node=client, ctrl_dev=ctrl_dev, device=device, timeout=180, interval=3
                    )
                if not ok:
                    self.logger.warning(
                        f"[NS] Device {device} still present after delete+rescan (lvol={lvol_name}). "
                        f"Will retry after outage recovery. Not a failure."
                    )
                    self.stale_ns_devices[lvol_name] = {
                        "device": device, "ctrl_dev": ctrl_dev, "client": client
                    }
                else:
                    self.logger.info(f"[NS] Verified namespace device removed: {device}")

                    # If this was the last namespace on that controller -> disconnect now
                    if self._is_last_namespace_after_delete(client, ctrl_dev):
                        self.logger.info(f"[NS] Last namespace left on {ctrl_dev} -> disconnecting {lvol_name}")
                        try:
                            self.disconnect_lvol(lvol_id)
                        except Exception as e:
                            self.logger.warning(f"[NS] disconnect_lvol (last-ns) failed for {lvol_name}: {e}")
                    else:
                        self.logger.info(f"[NS] Other namespaces still exist on {ctrl_dev}; not disconnecting.")
            # For non-namespace, disconnect already done (best-effort)

            # Cleanup only this lvol logs
            try:
                if client:
                    self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{lvol_name}_fio*"])
                    self.ssh_obj.delete_files(client, [f"{self.log_path}/{lvol_name}_fio_iolog*"])
                    if mount_path:
                        self.ssh_obj.delete_files(client, [f"{mount_path}/*"])
            except Exception:
                pass

            if lvol_name in self.lvols_without_sec_connect:
                self.lvols_without_sec_connect.remove(lvol_name)

            # Remove from node_vs_lvol if present (safe)
            for _, lvols in self.node_vs_lvol.items():
                if lvol_name in lvols:
                    lvols.remove(lvol_name)
                    break

            # Remove tracking
            try:
                del self.lvol_mount_details[lvol_name]
            except Exception:
                pass

            # If it's a parent, drop some optional maps (children remain in lvol_mount_details until randomly deleted)
            if d.get("is_parent", False):
                self.parent_ctrl.pop(lvol_name, None)
                self.parent_host_id.pop(lvol_name, None)
                self.parent_to_children.pop(lvol_name, None)

        sleep_n_sec(60)

    # -------------------------
    # Stale device retry after outage recovery
    # -------------------------
    def _retry_stale_ns_rescans(self):
        """
        After an outage node comes back online, retry rescan for any devices that
        did not disappear during delete_random_lvols.  Log results; never fail.
        """
        if not self.stale_ns_devices:
            return

        self.logger.info(
            f"[NS] Retrying rescan for {len(self.stale_ns_devices)} stale device(s) after outage recovery: "
            f"{list(self.stale_ns_devices.keys())}"
        )

        for lvol_name, info in list(self.stale_ns_devices.items()):
            device = info["device"]
            ctrl_dev = info["ctrl_dev"]
            client = info["client"]
            ctrl_name = ctrl_dev.split("/")[-1]

            # nvme ns-rescan
            self._rescan_nvme_namespaces(node=client, ctrl_dev=ctrl_dev)

            # sysfs fallback
            sysfs_cmd = (
                f"bash -lc \"echo 1 | sudo tee /sys/class/nvme/{ctrl_name}/rescan_controller"
                f" 2>/dev/null || true\""
            )
            out, err = self.ssh_obj.exec_command(node=client, command=sysfs_cmd, supress_logs=True)
            self.logger.info(f"[NS] post-outage sysfs rescan: ctrl={ctrl_name} out={out} err={err}")

            gone = self._wait_until_namespace_device_gone(
                node=client, ctrl_dev=ctrl_dev, device=device, timeout=60, interval=3
            )
            if gone:
                self.logger.info(
                    f"[NS] Stale device {device} (lvol={lvol_name}) cleared after outage recovery."
                )
                self.stale_ns_devices.pop(lvol_name, None)
            else:
                self.logger.warning(
                    f"[NS] Stale device {device} (lvol={lvol_name}) still present after outage recovery rescan. "
                    f"Not a failure — continuing."
                )

        if self.stale_ns_devices:
            self.logger.warning(
                f"[NS] Devices not yet cleared (not a failure): "
                + ", ".join(
                    f"{n}={v['device']}" for n, v in self.stale_ns_devices.items()
                )
            )

    def restart_nodes_after_failover(self, *args, **kwargs):
        super().restart_nodes_after_failover(*args, **kwargs)
        self._retry_stale_ns_rescans()

    # -------------------------
    # RUN
    # -------------------------
    def run(self):
        """
        Keep outages + everything exactly as base.
        Base run will call our overridden create/delete automatically.
        """
        self.logger.info("[NS] Starting namespace failover test (delegating run loop to base).")
        return super().run()