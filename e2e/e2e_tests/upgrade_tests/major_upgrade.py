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
import time
import random

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger

LVOLS_PER_NODE = 3


class TestMajorUpgrade(TestClusterBase):
    """
    Upgrade test (rolling), aligned with manual steps:

    Pre-upgrade per storage-node (LVOLS_PER_NODE lvols each):
      - Create LVOL, connect + mount on a client node
      - Run short fio and wait for it to complete
      - Snapshot + Clone, mount clone, verify md5 == base

    During upgrade:
      - Start long fio (3600s) on every lvol and keep running
      - Upgrade flow:
          pip install git+...@<target> --upgrade --force-reinstall  (all mgmt+storage nodes)
          sbctl -d cluster update --cp-only true
          for each storage node:
              sbctl -d sn suspend
              sbctl -d sn shutdown
              (on storage node) update env file with target docker/spdk images if given
              sbctl -d sn deploy --ifname eth0
              sbctl --dev -d sn restart --spdk-image <tag>
              wait for node online
              wait for migration tasks to complete
      - After upgrade: assert fio still running, then wait for all fio to finish
      - Verify fio logs have no errors
      - Verify pre-upgrade clone md5 still matches

    Sleep of 30 seconds between each major step.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)

        self.base_version = kwargs.get("base_version")
        self.target_version = kwargs.get("target_version")
        self.ifname = kwargs.get("ifname", "eth0")
        self.step_sleep = 30

        self.sbctl_cmd = kwargs.get("sbctl_cmd", os.environ.get("SBCTL_CMD", "sbctl"))

        # Target SPDK image (used for sn restart --spdk-image)
        self.spdk_image = (
            kwargs.get("target_spdk_image")
            or kwargs.get("spdk_image")
            or (f"simplyblock/spdk:{self.target_version}-latest" if self.target_version else "simplyblock/spdk:latest")
        )

        # Target Docker image (used to update env file on storage node before deploy)
        self.target_docker_image = kwargs.get("target_docker_image", "")

        self.snapshot_name = "upgrade_snap"
        self.clone_name = "upgrade_clone"
        self.base_mount_root = "/mnt/test_location"
        self.base_log_root = f"{self.docker_logs_path}/upgrade_fio_logs"
        self.fio_debug = getattr(self, "fio_debug", False)
        self.test_name = "test_major_upgrade"
        self.fio_during_upgrade = True  # set False in subclass to skip FIO during upgrade

        self.logger.info(f"Running upgrade test from {self.base_version} to {self.target_version}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _detect_new_device(self, node: str, before: list, after: list) -> str:
        for dev in after:
            if dev not in before:
                return f"/dev/{dev.strip()}"
        raise RuntimeError(
            f"[{node}] Could not detect newly attached device. before={before} after={after}"
        )

    def _pip_install_target(self, node: str):
        """
        pip install git+https://github.com/simplyblock-io/sbcli.git@<target> --upgrade --force-reinstall
        """
        if not self.target_version:
            raise ValueError("target_version is required (e.g., R25.10-Hotfix)")
        pkg = f"git+https://github.com/simplyblock-io/sbcli.git@{self.target_version}"
        cmd = f"pip install '{pkg}' --upgrade --force-reinstall"
        self.logger.info(f"[{node}] Installing sbcli: {cmd}")
        self.ssh_obj.exec_command(node, cmd)

    def _start_fio_tmux(self, node: str, mount_path: str, log_file: str, name: str, runtime: int):
        self.ssh_obj.make_directory(node, os.path.dirname(log_file))
        self.ssh_obj.run_fio_test(
            node,
            device=None,
            directory=mount_path,
            log_file=log_file,
            name=name,
            runtime=runtime,
            debug=self.fio_debug,
        )
        return f"fio_{name}"

    def _wait_tmux_gone(self, node: str, session: str, timeout: int = 3600):
        start = time.time()
        while time.time() - start < timeout:
            out, _ = self.ssh_obj.exec_command(
                node,
                f"sudo tmux has-session -t {session} 2>/dev/null && echo RUNNING || echo DONE",
                supress_logs=True,
            )
            if out.strip() == "DONE":
                return
            sleep_n_sec(5)
        raise RuntimeError(f"[{node}] Timed out waiting for tmux session: {session}")

    def _is_tmux_running(self, node: str, session: str) -> bool:
        out, _ = self.ssh_obj.exec_command(
            node,
            f"sudo tmux has-session -t {session} 2>/dev/null && echo RUNNING || echo DONE",
            supress_logs=True,
        )
        return out.strip() == "RUNNING"

    def _assert_fio_log_clean(self, node: str, log_file: str):
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

    def _update_node_env(self, node: str):
        """
        Before running sn deploy on a storage node, update the env file with target
        docker image and/or target SPDK ultra image if they were provided.
        The env file path is auto-detected under /etc/simplyblock/.
        """
        if not self.target_docker_image and not self.spdk_image:
            self.logger.info(f"[{node}] No image overrides to apply to env file")
            return

        out, _ = self.ssh_obj.exec_command(
            node,
            "find /etc/simplyblock -maxdepth 2 -name 'env' -type f 2>/dev/null | head -1",
            supress_logs=True,
        )
        env_path = out.strip() if out.strip() else "/etc/simplyblock/env"
        self.logger.info(f"[{node}] Updating env file at: {env_path}")

        if self.target_docker_image:
            self.ssh_obj.exec_command(
                node,
                f"grep -q 'SIMPLY_BLOCK_DOCKER_IMAGE' {env_path} 2>/dev/null "
                f"&& sed -i 's|^SIMPLY_BLOCK_DOCKER_IMAGE=.*|SIMPLY_BLOCK_DOCKER_IMAGE={self.target_docker_image}|' {env_path} "
                f"|| echo 'SIMPLY_BLOCK_DOCKER_IMAGE={self.target_docker_image}' >> {env_path}",
            )
            self.logger.info(f"[{node}] Set SIMPLY_BLOCK_DOCKER_IMAGE={self.target_docker_image}")

        if self.spdk_image:
            self.ssh_obj.exec_command(
                node,
                f"grep -q 'SIMPLY_BLOCK_SPDK_ULTRA_IMAGE' {env_path} 2>/dev/null "
                f"&& sed -i 's|^SIMPLY_BLOCK_SPDK_ULTRA_IMAGE=.*|SIMPLY_BLOCK_SPDK_ULTRA_IMAGE={self.spdk_image}|' {env_path} "
                f"|| echo 'SIMPLY_BLOCK_SPDK_ULTRA_IMAGE={self.spdk_image}' >> {env_path}",
            )
            self.logger.info(f"[{node}] Set SIMPLY_BLOCK_SPDK_ULTRA_IMAGE={self.spdk_image}")

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    def run(self):
        # ----------------------------------------------------------------
        # Step 1: Verify base version
        # ----------------------------------------------------------------
        self.logger.info("Step 1: Verify base version on all nodes")
        prev_versions = self.common_utils.get_all_node_versions()
        for node_ip, version in prev_versions.items():
            assert self.base_version in version, (
                f"Base version mismatch on {node_ip}: {version}"
            )
        sleep_n_sec(self.step_sleep)

        self.logger.info("Collect containers/images on all nodes (pre-upgrade)")
        pre_upgrade_containers = {}
        mgmt, storage = self.sbcli_utils.get_all_nodes_ip()
        all_nodes = mgmt + storage
        for node in all_nodes:
            pre_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)

        # ----------------------------------------------------------------
        # Step 2: Create pool
        # ----------------------------------------------------------------
        self.logger.info("Step 2: Create storage pool")
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        sleep_n_sec(self.step_sleep)

        # ----------------------------------------------------------------
        # Step 3-7: Pre-upgrade per storage node — LVOLS_PER_NODE lvols each
        #           short fio -> snapshot+clone -> md5 check
        # ----------------------------------------------------------------
        node_ctx = {}  # snode_ip -> {"lvols": [...]}

        self.logger.info(
            f"Step 3-7: Pre-upgrade: create {LVOLS_PER_NODE} LVOLs per storage node, "
            "run short fio, snapshot+clone, md5 verify"
        )
        for snode_idx, snode in enumerate(storage):
            snode_lvols = []
            for lvol_idx in range(LVOLS_PER_NODE):
                tag = f"upg_{snode_idx}_{lvol_idx}"
                lvol_name = f"{self.lvol_name}_{tag}"
                snap_name = f"{self.snapshot_name}_{tag}_pre"
                clone_name = f"{self.clone_name}_{tag}_pre"
                mount_path = f"{self.base_mount_root}_{tag}"
                clone_mount = f"{self.base_mount_root}_{tag}_clone_pre"
                pre_log = f"{self.base_log_root}/fio_pre_{tag}.log"

                client_node = random.choice(self.fio_node)

                self.logger.info(f"[{snode}] Creating LVOL {lvol_idx+1}/{LVOLS_PER_NODE}: {lvol_name}")
                self.sbcli_utils.add_lvol(lvol_name=lvol_name, pool_name=self.pool_name, size="5G")
                sleep_n_sec(self.step_sleep)

                self.logger.info(f"[{snode}] Connect + format + mount: {lvol_name}")
                before = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(lvol_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                after = self.ssh_obj.get_devices(client_node)
                disk = self._detect_new_device(client_node, before, after)

                self.ssh_obj.format_disk(client_node, disk)
                self.ssh_obj.mount_path(client_node, disk, mount_path)
                sleep_n_sec(self.step_sleep)

                # Short pre-upgrade fio — must complete before snapshot
                fio_name_pre = f"fio_pre_{tag}"
                self.logger.info(f"[{snode}] Starting pre-upgrade fio: {fio_name_pre}")
                fio_session_pre = self._start_fio_tmux(
                    client_node, mount_path, pre_log, fio_name_pre, runtime=120
                )
                self._wait_tmux_gone(client_node, fio_session_pre, timeout=600)
                self._assert_fio_log_clean(client_node, pre_log)
                sleep_n_sec(self.step_sleep)

                # Snapshot + clone
                lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
                self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_id, snap_name)
                snap_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snap_name)
                self.ssh_obj.add_clone(self.mgmt_nodes[0], snap_id, clone_name)
                sleep_n_sec(self.step_sleep)

                # Base md5
                base_files = self.ssh_obj.find_files(client_node, mount_path)
                base_md5 = self.ssh_obj.generate_checksums(client_node, base_files)

                # Connect + mount clone (no format)
                before2 = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(clone_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                after2 = self.ssh_obj.get_devices(client_node)
                clone_disk = self._detect_new_device(client_node, before2, after2)
                self.ssh_obj.mount_path(client_node, clone_disk, clone_mount)

                clone_files = self.ssh_obj.find_files(client_node, clone_mount)
                clone_md5 = self.ssh_obj.generate_checksums(client_node, clone_files)

                assert set(base_md5.values()) == set(clone_md5.values()), (
                    f"[{client_node}] Pre-upgrade md5 mismatch (lvol vs clone) for {lvol_name}"
                )
                sleep_n_sec(self.step_sleep)

                snode_lvols.append({
                    "tag": tag,
                    "client_node": client_node,
                    "lvol_name": lvol_name,
                    "mount_path": mount_path,
                    "snapshot_name": snap_name,
                    "snapshot_id": snap_id,
                    "clone_name": clone_name,
                    "clone_mount": clone_mount,
                    "base_md5": base_md5,
                    "clone_md5": clone_md5,
                    "fio_upgrade_session": None,
                    "fio_upgrade_log": None,
                    "fio_client_node": None,
                })

            node_ctx[snode] = {"lvols": snode_lvols}

        # ----------------------------------------------------------------
        # Step 8: Start long fio on ALL lvols (keep running during upgrade)
        # ----------------------------------------------------------------
        if self.fio_during_upgrade:
            self.logger.info(
                f"Step 8: Start long fio (3600s) on all {LVOLS_PER_NODE} lvols "
                "per storage node — keep running during upgrade"
            )
            for snode in self.storage_nodes:
                for lvol_ctx in node_ctx[snode]["lvols"]:
                    tag = lvol_ctx["tag"]
                    client_node = lvol_ctx["client_node"]
                    mount_path = lvol_ctx["mount_path"]
                    long_log = f"{self.base_log_root}/fio_during_upgrade_{tag}.log"
                    fio_name = f"fio_upgrade_{tag}"
                    session = self._start_fio_tmux(
                        client_node, mount_path, long_log, fio_name, runtime=3600
                    )
                    lvol_ctx["fio_upgrade_session"] = session
                    lvol_ctx["fio_upgrade_log"] = long_log
                    lvol_ctx["fio_client_node"] = client_node
                    self.logger.info(f"  [{client_node}] fio session started: {session}")
                    sleep_n_sec(self.step_sleep)
        else:
            self.logger.info("Step 8: Skipping FIO during upgrade (single-node / non-HA mode)")

        # ----------------------------------------------------------------
        # Step 9: pip install target sbcli on ALL mgmt+storage nodes
        # ----------------------------------------------------------------
        self.logger.info("Step 9: pip install target sbcli on ALL nodes")
        for node in all_nodes:
            self._pip_install_target(node)
            sleep_n_sec(self.step_sleep)

        # ----------------------------------------------------------------
        # Step 10: Cluster update cp-only
        # ----------------------------------------------------------------
        self.logger.info("Step 10: sbctl -d cluster update --cp-only true")
        self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.sbctl_cmd} -d cluster update {self.cluster_id} --cp-only true",
        )
        sleep_n_sec(60)

        # ----------------------------------------------------------------
        # Step 11: Rolling upgrade — suspend -> shutdown -> env update ->
        #          deploy -> restart -> wait online -> wait migration
        # ----------------------------------------------------------------
        self.logger.info("Step 11: Rolling upgrade of storage nodes")
        sn_results = self.sbcli_utils.get_storage_nodes().get("results", [])
        ip_to_id = {}
        for r in sn_results:
            nid = r.get("id") or r.get("uuid") or r.get("node_id")
            ip = r.get("ip") or r.get("mgmt_ip") or r.get("management_ip")
            if nid and ip:
                ip_to_id[ip] = nid

        for snode in self.storage_nodes:
            node_id = ip_to_id.get(snode)
            if not node_id:
                raise RuntimeError(
                    f"Could not resolve node_id for storage node {snode} from get_storage_nodes()"
                )
            node_ctx[snode]["node_id"] = node_id

            # Verify fio still running before touching this node
            if self.fio_during_upgrade:
                self.logger.info(f"[SN {snode}] Verifying fio is still running on all lvols")
                for lvol_ctx in node_ctx[snode]["lvols"]:
                    session = lvol_ctx["fio_upgrade_session"]
                    cn = lvol_ctx["fio_client_node"]
                    assert self._is_tmux_running(cn, session), (
                        f"FIO session {session} on {cn} is not running before upgrade of {snode}!"
                    )

            # Suspend
            self.logger.info(f"[SN {snode}] Suspending node {node_id}")
            self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.sbctl_cmd} -d sn suspend {node_id}"
            )
            self.sbcli_utils.wait_for_storage_node_status(node_id, "suspended", timeout=1000)
            sleep_n_sec(self.step_sleep)

            # Shutdown
            self.logger.info(f"[SN {snode}] Shutting down node {node_id}")
            self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.sbctl_cmd} -d sn shutdown {node_id}"
            )
            self.sbcli_utils.wait_for_storage_node_status(node_id, "offline", timeout=1000)
            sleep_n_sec(self.step_sleep)

            # Update env file on storage node with target images (only if provided)
            self.logger.info(f"[SN {snode}] Updating env file with target images")
            self._update_node_env(snode)
            sleep_n_sec(self.step_sleep)

            # Deploy on storage node
            self.logger.info(f"[SN {snode}] Running sn deploy (ifname={self.ifname})")
            self.ssh_obj.exec_command(
                snode, f"{self.sbctl_cmd} -d sn deploy --ifname {self.ifname}"
            )
            sleep_n_sec(self.step_sleep)

            # Restart with target spdk image
            self.logger.info(f"[SN {snode}] Restarting with spdk-image={self.spdk_image}")
            self.ssh_obj.exec_command(
                self.mgmt_nodes[0],
                f"{self.sbctl_cmd} --dev -d sn restart {node_id} --spdk-image {self.spdk_image}",
            )
            try:
                self.sbcli_utils.wait_for_storage_node_status(node_id, "online", timeout=1000)
            except Exception:
                self.logger.warning(f"[SN {snode}] Restart status check failed — continuing")
            finally:
                if not self.k8s_test:
                    for node in self.storage_nodes:
                        self.ssh_obj.restart_docker_logging(
                            node_ip=snode,
                            containers=self.container_nodes[node],
                            log_dir=os.path.join(self.docker_logs_path, snode),
                            test_name=self.test_name,
                        )
                else:
                    self.runner_k8s_log.restart_logging()
            sleep_n_sec(self.step_sleep)

            # Wait for migration tasks to complete before moving to next node
            self.logger.info(f"[SN {snode}] Waiting for migration tasks to complete")
            migration_ts = int(time.time()) - 120  # look back 2 min to catch tasks
            self.validate_migration_for_node(
                timestamp=migration_ts,
                timeout=1800,
                node_id=node_id,
                check_interval=30,
                no_task_ok=(not self.fio_during_upgrade),
            )
            sleep_n_sec(self.step_sleep)

        # ----------------------------------------------------------------
        # Step 12: Validate docker images upgraded
        # ----------------------------------------------------------------
        self.logger.info("Step 12: Validate upgraded docker images/containers")
        post_upgrade_containers = {}
        for node in all_nodes:
            post_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)
        self.common_utils.assert_upgrade_docker_image(pre_upgrade_containers, post_upgrade_containers)
        sleep_n_sec(self.step_sleep)

        # ----------------------------------------------------------------
        # Step 13: Verify fio still running, then wait to finish + check logs
        # ----------------------------------------------------------------
        if self.fio_during_upgrade:
            self.logger.info("Step 13: Verify fio still running post-upgrade, then wait for completion")
            for snode in self.storage_nodes:
                for lvol_ctx in node_ctx[snode]["lvols"]:
                    session = lvol_ctx["fio_upgrade_session"]
                    cn = lvol_ctx["fio_client_node"]
                    if self._is_tmux_running(cn, session):
                        self.logger.info(f"  [{cn}] fio {session}: still running (good)")
                    else:
                        self.logger.warning(f"  [{cn}] fio {session}: already finished — will check log")

            self.logger.info("Step 13: Waiting for all fio sessions to complete")
            for snode in self.storage_nodes:
                for lvol_ctx in node_ctx[snode]["lvols"]:
                    session = lvol_ctx["fio_upgrade_session"]
                    logf = lvol_ctx["fio_upgrade_log"]
                    cn = lvol_ctx["fio_client_node"]
                    self._wait_tmux_gone(cn, session, timeout=3600)
                    self._assert_fio_log_clean(cn, logf)
        else:
            self.logger.info("Step 13: Skipping FIO wait (single-node / non-HA mode)")

        # ----------------------------------------------------------------
        # Step 14: Post-upgrade md5 check on pre-upgrade clone mounts
        # ----------------------------------------------------------------
        self.logger.info("Step 14: Post-upgrade md5 check on pre-upgrade clones")
        for snode in self.storage_nodes:
            for lvol_ctx in node_ctx[snode]["lvols"]:
                clone_mount = lvol_ctx["clone_mount"]
                pre_clone_md5 = lvol_ctx["clone_md5"]
                client_node = lvol_ctx["client_node"]

                files = self.ssh_obj.find_files(client_node, clone_mount)
                post_md5 = self.ssh_obj.generate_checksums(client_node, files)

                assert set(pre_clone_md5.values()) == set(post_md5.values()), (
                    f"[{snode}/{lvol_ctx['lvol_name']}] Post-upgrade clone md5 mismatch!"
                )

        self.logger.info("TEST CASE PASSED !!!")


class TestMajorUpgradeSingleNode(TestMajorUpgrade):
    """
    Single-node upgrade variant: identical to TestMajorUpgrade but skips continuous
    FIO during the upgrade window (single-node has no HA, so the device goes offline
    during node restart and FIO would error out).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fio_during_upgrade = False
        self.test_name = "test_major_upgrade_single"
        self.logger.info("Single-node upgrade mode: FIO will NOT run during the upgrade window")
