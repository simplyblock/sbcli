# import os
# import threading
# from e2e_tests.cluster_test_base import TestClusterBase
# from utils.common_utils import sleep_n_sec
# from logger_config import setup_logger
# from pathlib import Path



# class TestMajorUpgrade(TestClusterBase):
#     """
#     Steps:
#     1. Check base version in input matches sbcli version on all the nodes
#     2. Create storage pool
#     3. Create LVOL
#     4. Connect LVOL
#     5. Mount Device
#     6. Start FIO runs and wait for it to complete
#     7. Take snapshots and clones. Take md5 of lvols and clones
#     8. Upgrade to target version
#     9. Check target version once upgrade completes.
#     10. Check current lvols and clones md5sum, should match
#     11. Try creating new snapshot and clones from older lvols and clones and their md5 matches or not
#     12. Create new lvols, run fio on them and let that complete.
#     13. Create snapshot and clones as well.
#     """
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.logger = setup_logger(__name__)
#         self.base_version = kwargs.get("base_version")
#         self.target_version = kwargs.get("target_version")
#         self.snapshot_name = "upgrade_snap"
#         self.clone_name = "upgrade_clone"
#         self.test_name = "major_upgrade_test"
#         self.mount_path = f"{Path.home()}/upgrade_test_fio"
#         self.log_path = f"{os.path.dirname(self.mount_path)}/upgrade_fio_log.log"
#         self.logger.info(f"Running upgrade test from {self.base_version} to {self.target_version}")

#     def run(self):
#         self.logger.info("Step 1: Verify base version on all nodes")
#         prev_versions = self.common_utils.get_all_node_versions()
#         for node_ip, version in prev_versions.items():
#             assert self.base_version in version, f"Base version mismatch on {node_ip}: {version}"
        
#         self.logger.info("Getting Containers on all the nodes before upgrade!!")
#         pre_upgrade_containers = {}
#         mgmt, storage = self.sbcli_utils.get_all_nodes_ip()
#         all_nodes = mgmt + storage
#         for node in all_nodes:
#             pre_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)

#         self.logger.info("Step 2: Recreate storage pool and add LVOL")
#         self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
#         self.sbcli_utils.add_lvol(lvol_name=self.lvol_name, pool_name=self.pool_name, size="5G")

#         self.logger.info("Step 3-5: Connect LVOL, format, and mount")
#         initial_devices = self.ssh_obj.get_devices(self.mgmt_nodes[0])
#         connect_cmds = self.sbcli_utils.get_lvol_connect_str(self.lvol_name)
#         for cmd in connect_cmds:
#             self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

#         final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
#         disk_use = None
#         self.logger.info("Initial vs final disk:")
#         self.logger.info(f"Initial: {initial_devices}")
#         self.logger.info(f"Final: {final_devices}")
#         for device in final_devices:
#             if device not in initial_devices:
#                 self.logger.info(f"Using disk: /dev/{device.strip()}")
#                 disk_use = f"/dev/{device.strip()}"
#                 break

#         self.ssh_obj.format_disk(self.mgmt_nodes[0], disk_use)
#         self.ssh_obj.mount_path(self.mgmt_nodes[0], disk_use, self.mount_path)

#         self.logger.info("Step 6: Start FIO and wait")
#         fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
#                                       args=(self.mgmt_nodes[0], None, self.mount_path, self.log_path),
#                                       kwargs={"name": "fio_run_pre_upgrade", "runtime": 120, "debug": self.fio_debug})
#         fio_thread.start()
#         self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
#                                              threads=[fio_thread],
#                                              timeout=300)

#         self.logger.info("Step 7: Snapshot and Clone + MD5 of LVOL")
#         self.ssh_obj.add_snapshot(self.mgmt_nodes[0], self.sbcli_utils.get_lvol_id(self.lvol_name), f"{self.snapshot_name}_pre")
#         snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], f"{self.snapshot_name}_pre")
#         self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, f"{self.clone_name}_pre")

#         files = self.ssh_obj.find_files(self.mgmt_nodes[0], self.mount_path)
#         pre_upgrade_lvol_md5 = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], files)

#         initial_devices = self.ssh_obj.get_devices(self.mgmt_nodes[0])
#         connect_cmds = self.sbcli_utils.get_lvol_connect_str(f"{self.clone_name}_pre")
#         for cmd in connect_cmds:
#             self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

#         final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
#         disk_use = None
#         self.logger.info("Initial vs final disk:")
#         self.logger.info(f"Initial: {initial_devices}")
#         self.logger.info(f"Final: {final_devices}")
#         for device in final_devices:
#             if device not in initial_devices:
#                 self.logger.info(f"Using disk: /dev/{device.strip()}")
#                 disk_use = f"/dev/{device.strip()}"
#                 break

#         self.ssh_obj.mount_path(self.mgmt_nodes[0], disk_use, f"{self.mount_path}_clone_pre")

#         files = self.ssh_obj.find_files(self.mgmt_nodes[0], f"{self.mount_path}_clone_pre")
#         pre_upgrade_clone_md5 = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], files)

#         original_checksum = set(pre_upgrade_lvol_md5.values())
#         final_checksum = set(pre_upgrade_clone_md5.values())

#         self.logger.info(f"Set Original checksum: {original_checksum}")
#         self.logger.info(f"Set Final checksum: {final_checksum}")

#         assert original_checksum == final_checksum, "Checksum mismatch between lvol and clone before upgrade!!"

#         self.logger.info("Step 8: Perform Upgrade")

#         package_name = f"{self.base_cmd}=={self.target_version}" if self.target_version != "latest" else self.base_cmd

#         self.ssh_obj.exec_command(self.mgmt_nodes[0], f"pip install {package_name} --upgrade")
#         sleep_n_sec(10)

#         self.logger.info("Step: Override Docker config to enable remote API and restart Docker")

#         for node in self.mgmt_nodes:
#             docker_override_cmds = [
#                 "sudo mkdir -p /etc/systemd/system/docker.service.d/",
#                 f"echo -e '[Service]\\nExecStart=\\nExecStart=-/usr/bin/dockerd --containerd=/run/containerd/containerd.sock "
#                 f"-H tcp://{node}:2375 -H unix:///var/run/docker.sock -H fd://' | "
#                 "sudo tee /etc/systemd/system/docker.service.d/override.conf",
#                 "sudo systemctl daemon-reload",
#                 "sudo systemctl restart docker"
#             ]

#             for cmd in docker_override_cmds:
#                 self.ssh_obj.exec_command(node, cmd)

#             self.logger.info(f"Docker override configuration applied and Docker restarted on {node}")

#             # Health check: ensure Docker is running
#             self.logger.info(f"Checking Docker status on {node}...")
#             max_attempts = 50
#             attempt = 0
#             while attempt < max_attempts:
#                 output, _ = self.ssh_obj.exec_command(node, "sudo systemctl is-active docker")
#                 if output.strip() == "active":
#                     self.logger.info(f"Docker is active on {node}")
#                     break
#                 attempt += 1
#                 self.logger.info(f"Docker not active yet on {node}, retrying in 3s (attempt {attempt}/{max_attempts})...")
#                 sleep_n_sec(3)
#             else:
#                 raise RuntimeError(f"Docker failed to become active on {node} after {max_attempts} attempts!")
        
#         sleep_n_sec(30)
#         cmd = f"{self.base_cmd} --dev -d cluster graceful-shutdown {self.cluster_id}"
#         self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

#         node_sample = self.sbcli_utils.get_storage_nodes()["results"][0]
#         max_lvol = node_sample["max_lvol"]
#         max_prov = int(node_sample["max_prov"] / (1024**3))  # Convert bytes to GB
        
#         for snode in self.storage_nodes:
#             cmd = f"pip install {package_name} --upgrade"
#             self.ssh_obj.exec_command(snode, cmd)
#             sleep_n_sec(10)
#             self.ssh_obj.deploy_storage_node(
#                 node=snode,
#                 max_lvol=max_lvol,
#                 max_prov_gb=max_prov
#             )
#             sleep_n_sec(10)
        
#         upgrade_cmd = f"{self.base_cmd} -d cluster update {self.cluster_id} --cp-only true"
#         self.ssh_obj.exec_command(self.mgmt_nodes[0], upgrade_cmd)
#         sleep_n_sec(180)

#         self.logger.info("Step 9: Validate upgraded version")
#         post_upgrade_containers = {}
#         for node in all_nodes:
#             post_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)
        
#         self.common_utils.assert_upgrade_docker_image(pre_upgrade_containers, post_upgrade_containers)

#         self.logger.info("Step 10: Verify pre-upgrade LVOL checksum")
#         post_files = self.ssh_obj.find_files(self.mgmt_nodes[0], self.mount_path)
#         post_md5_lvol = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], post_files)

#         original_checksum = set(pre_upgrade_lvol_md5.values())
#         final_checksum = set(post_md5_lvol.values())

#         self.logger.info(f"Set Original checksum: {original_checksum}")
#         self.logger.info(f"Set Final checksum: {final_checksum}")

#         assert original_checksum == final_checksum, "Checksum mismatch after upgrade!!"

#         self.logger.info("Step 11: Clone from old snapshot and verify MD5")
#         files = self.ssh_obj.find_files(self.mgmt_nodes[0], f"{self.mount_path}_clone_pre")
#         post_upgrade_clone_md5 = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], files)

#         original_checksum = set(pre_upgrade_clone_md5.values())
#         final_checksum = set(post_upgrade_clone_md5.values())

#         self.logger.info(f"Set Original checksum: {original_checksum}")
#         self.logger.info(f"Set Final checksum: {final_checksum}")

#         assert original_checksum == final_checksum, "Post-upgrade clone checksum mismatch!!"

#         self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, f"{self.clone_name}_pre_post")
#         initial_devices = self.ssh_obj.get_devices(self.mgmt_nodes[0])
#         connect_cmds = self.sbcli_utils.get_lvol_connect_str(f"{self.clone_name}_pre_post")
#         for cmd in connect_cmds:
#             self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

#         final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
#         disk_use = None
#         self.logger.info("Initial vs final disk:")
#         self.logger.info(f"Initial: {initial_devices}")
#         self.logger.info(f"Final: {final_devices}")
#         for device in final_devices:
#             if device not in initial_devices:
#                 self.logger.info(f"Using disk: /dev/{device.strip()}")
#                 disk_use = f"/dev/{device.strip()}"
#                 break

#         self.ssh_obj.mount_path(self.mgmt_nodes[0], disk_use, f"{self.mount_path}_clone_pre_post")

#         files = self.ssh_obj.find_files(self.mgmt_nodes[0], f"{self.mount_path}_clone_pre_post")
#         pre_post_upgrade_clone_md5 = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], files)

#         original_checksum = set(pre_upgrade_clone_md5.values())
#         final_checksum = set(pre_post_upgrade_clone_md5.values())

#         self.logger.info(f"Set Original checksum: {original_checksum}")
#         self.logger.info(f"Set Final checksum: {final_checksum}")

#         assert original_checksum == final_checksum, "Post-upgrade clone create and older clone checksum mismatch!!"

#         self.logger.info("Step 12-13: Create new LVOL, run fio, snapshot + clone")
#         new_lvol = f"{self.lvol_name}_new"
#         self.sbcli_utils.add_lvol(lvol_name=new_lvol, pool_name=self.pool_name, size="5G")


#         initial_devices = self.ssh_obj.get_devices(self.mgmt_nodes[0])
#         connect_cmds = self.sbcli_utils.get_lvol_connect_str(new_lvol)
#         for cmd in connect_cmds:
#             self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

#         final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
#         disk_use = None
#         self.logger.info("Initial vs final disk:")
#         self.logger.info(f"Initial: {initial_devices}")
#         self.logger.info(f"Final: {final_devices}")
#         for device in final_devices:
#             if device not in initial_devices:
#                 self.logger.info(f"Using disk: /dev/{device.strip()}")
#                 disk_use = f"/dev/{device.strip()}"
#                 break

#         self.ssh_obj.format_disk(self.mgmt_nodes[0], disk_use)
#         new_mount = f"{self.mount_path}_{new_lvol}"
#         self.ssh_obj.mount_path(self.mgmt_nodes[0], disk_use, new_mount)
        
#         fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
#                                       args=(self.mgmt_nodes[0], None, new_mount, self.log_path + "_new"),
#                                       kwargs={"name": "fio_run_post_upgrade", "runtime": 120,"debug": self.fio_debug})
#         fio_thread.start()
#         self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
#                                              threads=[fio_thread],
#                                              timeout=300)

#         self.ssh_obj.add_snapshot(self.mgmt_nodes[0], self.sbcli_utils.get_lvol_id(new_lvol), f"{self.snapshot_name}_post")
#         self.ssh_obj.add_clone(self.mgmt_nodes[0], self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], f"{self.snapshot_name}_post"),
#                                f"{self.clone_name}_post")

#         self.logger.info("TEST CASE PASSED !!!")
        

import os
import threading
import time
import re
from pathlib import Path

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestMajorUpgrade(TestClusterBase):
    """
    Upgrade test (rolling), aligned with manual steps:

    Pre-upgrade per client-node:
      - Create 1 LVOL
      - Connect + mount
      - Run fio (short) and complete
      - Snapshot + Clone
      - Mount clone and verify md5 == base

    During upgrade:
      - Start fio (long) on each client-node and keep running
      - Upgrade flow:
          pip install ... on all nodes
          sbctl cluster update --cp-only true
          for each storage node:
              sbctl sn suspend
              sbctl sn shutdown
              (on that node) sbctl sn deploy --ifname eth0
              sbctl sn restart --spdk-image <tag>

    Post-upgrade:
      - Wait for fio to finish
      - Verify fio logs have no errors
      - Verify pre-upgrade clone md5 still matches
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)

        self.base_version = kwargs.get("base_version")
        self.target_version = kwargs.get("target_version")  # e.g. "R25.10-Hotfix"
        self.ifname = kwargs.get("ifname", "eth0")

        # Explicit commands
        self.sbctl_cmd = kwargs.get("sbctl_cmd", os.environ.get("SBCTL_CMD", "sbctl"))

        # SPDK image tag as per manual
        # Example: simplyblock/spdk:R25.10-Hotfix-latest
        self.spdk_image = kwargs.get(
            "spdk_image",
            f"simplyblock/spdk:{self.target_version}-latest" if self.target_version else "simplyblock/spdk:latest"
        )

        # naming
        self.snapshot_name = "upgrade_snap"
        self.clone_name = "upgrade_clone"
        self.test_name = "major_upgrade_test"

        self.base_mount_root = f"/mnt/test_location/"
        self.base_log_root = f"{self.docker_logs_path}/upgrade_fio_logs"
        self.fio_debug = getattr(self, "fio_debug", False)
        self.test_name = "test_major_upgrade"

        self.logger.info(f"Running upgrade test from {self.base_version} to {self.target_version}")

    def _detect_new_device(self, node: str, before: list, after: list) -> str:
        for dev in after:
            if dev not in before:
                return f"/dev/{dev.strip()}"
        raise RuntimeError(f"[{node}] Could not detect newly attached device. before={before} after={after}")

    def _pip_install_target(self, node: str):
        """
        Manual equivalent:
          pip install git+https://github.com/simplyblock-io/sbcli.git@R25.10-Hotfix --upgrade --force
        Pip equivalent uses --force-reinstall.
        """
        if not self.target_version:
            raise ValueError("target_version is required (e.g., R25.10-Hotfix)")

        pkg = f"git+https://github.com/simplyblock-io/sbcli.git@{self.target_version}"
        cmd = f"pip install '{pkg}' --upgrade --force-reinstall"
        self.logger.info(f"[{node}] Installing sbcli from {pkg}")
        self.ssh_obj.exec_command(node, cmd)

    def _start_fio_tmux(self, node: str, mount_path: str, log_file: str, name: str, runtime: int):
        """
        Uses your SshUtils.run_fio_test which starts fio inside tmux and returns once running.
        """
        self.ssh_obj.make_directory(node, os.path.dirname(log_file))
        self.ssh_obj.run_fio_test(
            node,
            device=None,
            directory=mount_path,
            log_file=log_file,
            name=name,
            runtime=runtime,
            debug=self.fio_debug
        )
        return f"fio_{name}"  # tmux session name is "fio_{name}" in ssh_utils

    def _wait_tmux_gone(self, node: str, session: str, timeout: int = 3600):
        start = time.time()
        while time.time() - start < timeout:
            out, _ = self.ssh_obj.exec_command(node, f"sudo tmux has-session -t {session} 2>/dev/null && echo RUNNING || echo DONE", supress_logs=True)
            if out.strip() == "DONE":
                return
            sleep_n_sec(5)
        raise RuntimeError(f"[{node}] Timed out waiting for tmux session to finish: {session}")

    def _assert_fio_log_clean(self, node: str, log_file: str):
        """
        Very pragmatic fio log check:
        - any 'error' (case-insensitive)
        - verify failures
        - 'err=' non-zero patterns
        """
        # grep returns exit code 1 if no match, so add '|| true'
        cmd = (
            f"sudo bash -lc \""
            f"test -f '{log_file}' || (echo 'MISSING_LOG'; exit 0); "
            f"grep -iE 'verify failed|corrupt|io error|input/output error|fatal|err=[1-9]|error' '{log_file}' || true"
            f"\""
        )
        out, _ = self.ssh_obj.exec_command(node, cmd, supress_logs=True)
        out = out.strip()
        if out and "MISSING_LOG" not in out:
            raise AssertionError(f"[{node}] FIO log has errors in {log_file}:\n{out}")

    # ----------------------------
    # Main run
    # ----------------------------
    def run(self):
        # Choose nodes that will mount volumes + run fio
        # If your storage nodes also act as initiators, you can extend this list.

        self.logger.info("Step 1: Verify base version on all nodes")
        prev_versions = self.common_utils.get_all_node_versions()
        for node_ip, version in prev_versions.items():
            assert self.base_version in version, f"Base version mismatch on {node_ip}: {version}"

        self.logger.info("Collect containers/images on all nodes (pre-upgrade)")
        pre_upgrade_containers = {}
        mgmt, storage = self.sbcli_utils.get_all_nodes_ip()
        all_nodes = mgmt + storage
        for node in all_nodes:
            pre_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)

        # ------------------------------------------------------------
        # Step 2: Create pool once, then create per-node LVOLs
        # ------------------------------------------------------------
        self.logger.info("Step 2: Create storage pool (once)")
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        node_ctx = {}  # per client node context

        self.logger.info("Step 3-7: Pre-upgrade per-node: LVOL -> fio -> snapshot+clone -> md5 check")
        for snode in storage:
            tag = "upg"
            lvol_name = f"{self.lvol_name}_{tag}"
            snap_name = f"{self.snapshot_name}_{tag}_pre"
            clone_name = f"{self.clone_name}_{tag}_pre"

            mount_path = f"{self.base_mount_root}_{tag}"
            clone_mount = f"{self.base_mount_root}_{tag}_clone_pre"
            pre_log = f"{self.base_log_root}/fio_pre_{tag}.log"

            client_node = random.choice(self.fio_node)

            self.logger.info(f"[{node}] Creating LVOL: {lvol_name}")
            self.sbcli_utils.add_lvol(lvol_name=lvol_name, pool_name=self.pool_name, size="5G")

            self.logger.info(f"[{node}] Connect LVOL, format, mount")
            before = self.ssh_obj.get_devices(client_node)
            for cmd in self.sbcli_utils.get_lvol_connect_str(lvol_name):
                self.ssh_obj.exec_command(client_node, cmd)
            after = self.ssh_obj.get_devices(client_node)
            disk = self._detect_new_device(client_node, before, after)

            self.ssh_obj.format_disk(client_node, disk)
            self.ssh_obj.mount_path(client_node, disk, mount_path)

            # Pre-upgrade fio (short) must COMPLETE before snapshot
            fio_name_pre = f"fio_pre_{tag}"
            fio_session_pre = self._start_fio_tmux(client_node, mount_path, pre_log, fio_name_pre, runtime=120)
            self._wait_tmux_gone(client_node, fio_session_pre, timeout=600)
            self._assert_fio_log_clean(client_node, pre_log)

            # Snapshot + clone
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_id, snap_name)
            snap_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snap_name)
            self.ssh_obj.add_clone(self.mgmt_nodes[0], snap_id, clone_name)

            # Base md5
            base_files = self.ssh_obj.find_files(client_node, mount_path)
            base_md5 = self.ssh_obj.generate_checksums(client_node, base_files)

            # Connect clone, mount without formatting
            before2 = self.ssh_obj.get_devices(client_node)
            for cmd in self.sbcli_utils.get_lvol_connect_str(clone_name):
                self.ssh_obj.exec_command(client_node, cmd)
            after2 = self.ssh_obj.get_devices(client_node)
            clone_disk = self._detect_new_device(client_node, before2, after2)

            self.ssh_obj.mount_path(client_node, clone_disk, clone_mount)

            clone_files = self.ssh_obj.find_files(client_node, clone_mount)
            clone_md5 = self.ssh_obj.generate_checksums(client_node, clone_files)

            assert set(base_md5.values()) == set(clone_md5.values()), f"[{client_node}] Pre-upgrade md5 mismatch (lvol vs clone)"

            node_ctx[snode] = {
                "tag": tag,
                "lvol_name": lvol_name,
                "mount_path": mount_path,
                "snapshot_name": snap_name,
                "snapshot_id": snap_id,
                "clone_name": clone_name,
                "clone_mount": clone_mount,
                "base_md5": base_md5,
                "clone_md5": clone_md5,
            }

        # ------------------------------------------------------------
        # Step 8: Start fio DURING upgrade on each node (long)
        # ------------------------------------------------------------
        self.logger.info("Step 8: Start fio (long) on all client nodes and keep it running during upgrade")
        for snode in self.storage_nodes:
            client_node = random.choice(self.fio_node)
            tag = node_ctx[node]["tag"]
            mount_path = node_ctx[node]["mount_path"]
            long_log = f"{self.base_log_root}/fio_during_upgrade_{tag}.log"
            fio_name = f"fio_upgrade_{tag}"
            session = self._start_fio_tmux(client_node, mount_path, long_log, fio_name, runtime=3600)  # 30 min
            node_ctx[node]["fio_upgrade_session"] = session
            node_ctx[node]["fio_upgrade_log"] = long_log

        # ------------------------------------------------------------
        # Step 9: Upgrade flow (MATCH MANUAL)
        # ------------------------------------------------------------
        self.logger.info("Step 9: Upgrade - pip install on ALL nodes")
        for node in all_nodes:
            self._pip_install_target(node)
            sleep_n_sec(3)

        self.logger.info("Step 10: Cluster update (cp-only true)")
        self.ssh_obj.exec_command(self.mgmt_nodes[0], f"{self.sbctl_cmd} -d cluster update {self.cluster_id} --cp-only true")
        sleep_n_sec(60)

        # Rolling upgrade for storage nodes (suspend -> shutdown -> deploy(on node) -> restart(with spdk image))
        self.logger.info("Step 11: Rolling upgrade storage nodes")
        sn_results = self.sbcli_utils.get_storage_nodes().get("results", [])
        # Build map ip -> node_id robustly
        ip_to_id = {}
        for r in sn_results:
            nid = r.get("id") or r.get("uuid") or r.get("node_id")
            ip = r.get("ip") or r.get("mgmt_ip") or r.get("management_ip")
            if nid and ip:
                ip_to_id[ip] = nid

        for snode in self.storage_nodes:
            node_id = ip_to_id.get(snode)
            if not node_id:
                raise RuntimeError(f"Could not resolve node_id for storage node {snode} from get_storage_nodes()")

            self.logger.info(f"[SN {snode}] suspend -> shutdown")
            self.ssh_obj.exec_command(self.mgmt_nodes[0], f"{self.sbctl_cmd} -d sn suspend {node_id}")
            self.sbcli_utils.wait_for_storage_node_status(node_id, "suspended", timeout=1000)
            sleep_n_sec(60)
            self.ssh_obj.exec_command(self.mgmt_nodes[0], f"{self.sbctl_cmd} -d sn shutdown {node_id}")
            self.sbcli_utils.wait_for_storage_node_status(node_id, "offline", timeout=1000)
            sleep_n_sec(60)

            self.logger.info(f"[SN {snode}] deploy on node (ifname={self.ifname})")
            self.ssh_obj.exec_command(snode, f"{self.sbctl_cmd} -d sn deploy --ifname {self.ifname}")
            sleep_n_sec(60)

            self.logger.info(f"[SN {snode}] restart with spdk-image={self.spdk_image}")
            self.ssh_obj.exec_command(self.mgmt_nodes[0], f"{self.sbctl_cmd} --dev -d sn restart {node_id} --spdk-image {self.spdk_image}")
            try:
                self.sbcli_utils.wait_for_storage_node_status(node_id, "online", timeout=1000)
            except Exception as e:
                self.logger.info(f"[SN {snode}] Restart failed!!")
            finally:
                if not self.k8s_test:
                    for node in self.storage_nodes:
                        self.ssh_obj.restart_docker_logging(
                            node_ip=snode,
                            containers=self.container_nodes[node],
                            log_dir=os.path.join(self.docker_logs_path, snode),
                            test_name=self.test_name
                        )
                else:
                    self.runner_k8s_log.restart_logging()
                sleep_n_sec(120)

        # ------------------------------------------------------------
        # Step 12: Validate images/containers upgraded
        # ------------------------------------------------------------
        self.logger.info("Step 12: Validate upgraded docker images/containers")
        post_upgrade_containers = {}
        for node in all_nodes:
            post_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)

        self.common_utils.assert_upgrade_docker_image(pre_upgrade_containers, post_upgrade_containers)

        # ------------------------------------------------------------
        # Step 13: Wait fio to finish and check logs
        # ------------------------------------------------------------
        self.logger.info("Step 13: Wait for fio during upgrade to finish + verify logs are clean")
        for node in self.fio_node:
            session = node_ctx[node]["fio_upgrade_session"]
            logf = node_ctx[node]["fio_upgrade_log"]
            self._wait_tmux_gone(node, session, timeout=3600)
            self._assert_fio_log_clean(node, logf)

        # ------------------------------------------------------------
        # Step 14: Post-upgrade md5 check on PRE-upgrade clone mount
        # (clone was derived from pre-upgrade snapshot; it should remain stable)
        # ------------------------------------------------------------
        self.logger.info("Step 14: Post-upgrade md5 check on pre-upgrade clones")
        for node in self.fio_node:
            clone_mount = node_ctx[node]["clone_mount"]
            pre_clone_md5 = node_ctx[node]["clone_md5"]

            files = self.ssh_obj.find_files(node, clone_mount)
            post_md5 = self.ssh_obj.generate_checksums(node, files)

            assert set(pre_clone_md5.values()) == set(post_md5.values()), f"[{node}] Post-upgrade clone md5 mismatch!"

        self.logger.info("TEST CASE PASSED !!!")
