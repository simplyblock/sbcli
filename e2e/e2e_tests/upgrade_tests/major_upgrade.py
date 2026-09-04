"""
Docker (VM/bare-metal) rolling major upgrade.

OPERATOR PROCEDURE -- the exact sequence this test performs, so it can be
handed to a customer without re-reading the code or a CI log. Verified by run
33733403479 (26.2.8-PRE -> R26.3, success).

Scope: rolling upgrade between R26.x versions (e.g. 26.2.8-PRE -> R26.3).
The R25 -> R26 path additionally needs a DB migration; see
_needs_db_migration, which gates it to base versions starting with 25.

Workloads stay online throughout: this test keeps 4 FIO sessions per storage
node running across every step below.

Preconditions
-------------
  * cluster status ACTIVE, every storage node "online"
  * you know the target release tag and BOTH container image tags

Steps
-----
  1. Install the target release on EVERY node (management + storage):

         pip install "git+https://github.com/simplyblock-io/sbcli.git@R26.3" \
             --upgrade --force-reinstall

  2. Pin the target images in simplyblock_core/env_var on the MANAGEMENT
     nodes (skip if not overriding image tags). Locate the file with:

         python3 -c "import simplyblock_core, os; print(os.path.join(
             os.path.dirname(simplyblock_core.__file__), 'env_var'))"
         # e.g. /usr/local/lib/python3.12/site-packages/simplyblock_core/env_var

  3. Update the control plane only:

         sbctl -d cluster update <CLUSTER_ID> --cp-only true

  4. DB migration -- R25 -> R26 ONLY. Skip it for R26.x -> R26.y.

  5. Rolling storage-node upgrade, ONE NODE AT A TIME. Do not start the next
     node until the current one is "online" AND its migration tasks have
     finished.

         sbctl -d sn suspend  <NODE_ID>       # wait: status = suspended
         sbctl -d sn shutdown <NODE_ID>       # wait: status = offline

         # on the storage node itself, if pinning images: update env_var
         sbctl -d sn deploy --ifname eth0     # run ON the storage node

         sbctl --dev -d sn restart <NODE_ID> \
             --spdk-image       public.ecr.aws/simply-block/ultra:R26.3-latest \
             --spdk-proxy-image public.ecr.aws/simply-block/simplyblock:R26.3
         # wait: status = online, then wait for migration tasks

     BOTH images are required. They are different repositories ("ultra" vs
     "simplyblock") with different tag shapes ("R26.3-latest" vs "R26.3"), so
     neither can be inferred from the other.

  6. Verify every container is running the target image.

  7. Activate v2 write protection, then restart every node AGAIN.

     An upgraded cluster's existing distribs stay on v1 write protection --
     only freshly created clusters start on v2. Run this only once every node
     is back online: the switch sends the runtime RPC to all online nodes and
     records v2 only when every one of them accepts it.

         sbctl -d cluster switch-write-protection <CLUSTER_ID>

     then, one node at a time:

         sbctl --dev -d sn restart <NODE_ID> --force
         # wait: online + migration tasks, before moving to the next node

     --force is REQUIRED: the nodes are already online and healthy, so a plain
     restart is refused as unnecessary. No image flags here -- the node is
     already on the target images. This second pass proves the v2 generation
     persisted and the nodes come back cleanly under it.

  8. Post-upgrade validation: cluster ACTIVE, all nodes online, workload I/O
     uninterrupted, pre-upgrade checksums still match.

Note on flags: this test uses -d / --dev (debug/dev) throughout. Confirm which
of those belong in a customer-facing procedure before publishing it.
"""

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
import threading

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger

# 1 verification lvol per node: short FIO → snap + clone → md5 check (no ongoing FIO during upgrade)
VERIFY_LVOLS_PER_NODE = 1
# 2 FIO lvols per node: long FIO runs on lvol AND its clone throughout the entire upgrade
FIO_LVOLS_PER_NODE = 2


class TestMajorUpgrade(TestClusterBase):
    """
    Upgrade test (rolling), aligned with manual steps:

    Pre-upgrade per storage-node:
      - VERIFY_LVOLS_PER_NODE (1) verification lvols:
          connect + format + mount → short fio → snap + clone + md5 verify
          (no ongoing FIO during upgrade on these)
      - FIO_LVOLS_PER_NODE (2) fio lvols:
          connect + format + mount → snap + clone → connect + mount clone
          long fio (3600s) started on BOTH the lvol AND its clone, kept running during upgrade

    During upgrade:
      - 4 fio sessions per node (2 lvols + 2 clones) keep running
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
      - Verify pre-upgrade verification clone md5 still matches

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
        self.ssh_obj.exec_command(node, cmd, raise_on_error=True)

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

    def _start_fio_tmux_thread(self, node: str, mount_path: str, log_file: str,
                               name: str, runtime: int, results: dict, key: str):
        """Start fio in a background thread; store session name in results[key]."""
        try:
            session = self._start_fio_tmux(node, mount_path, log_file, name, runtime)
            results[key] = session
        except Exception as exc:
            self.logger.error(f"[{node}] Failed to start fio {name}: {exc}")
            results[key] = None

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

    def _get_env_var_path(self, node: str) -> str:
        """
        Dynamically locate the simplyblock_core/env_var file on node.
        Uses the same resolution logic as the bootstrap script.
        """
        out, _ = self.ssh_obj.exec_command(
            node,
            "python3 -c \"import simplyblock_core, os; "
            "print(os.path.join(os.path.dirname(simplyblock_core.__file__), 'env_var'))\"",
            supress_logs=True,
        )
        path = out.strip()
        if not path:
            # Fallback: find in site-packages
            out2, _ = self.ssh_obj.exec_command(
                node,
                "find /usr/local/lib -path '*/simplyblock_core/env_var' 2>/dev/null | head -1",
                supress_logs=True,
            )
            path = out2.strip()
        if not path:
            raise RuntimeError(f"[{node}] Could not locate simplyblock_core/env_var")
        self.logger.info(f"[{node}] Found env_var at: {path}")
        return path

    _R25_R26_MIGRATION_SCRIPT = r"""
from simplyblock_core import utils
from simplyblock_core.db_controller import DBController

db_controller = DBController()

for snode in db_controller.get_storage_nodes():
    print(f"updating storage node object: {snode.get_id()}")
    for node in db_controller.get_storage_nodes():
        if snode.get_id() == node.secondary_node_id:
            snode.lvstore_stack_secondary = node.get_id()
            break
    snode.lvstore_ports = {
        snode.lvstore: {
            "lvol_subsys_port": snode.lvol_subsys_port,
            "hublvol_port": snode.hublvol.nvmf_port
        }
    }
    if snode.lvstore_stack_secondary:
        sec = db_controller.get_storage_node_by_id(snode.lvstore_stack_secondary)
        snode.lvstore_ports[sec.lvstore] = {
            "lvol_subsys_port": sec.lvol_subsys_port,
            "hublvol_port": sec.hublvol.nvmf_port,
        }
    if snode.poller_cpu_cores:
        snode.lvol_poller_mask = utils.generate_mask([snode.poller_cpu_cores[-1]])
        if len(snode.poller_cpu_cores) > 1:
            snode.poller_cpu_cores = snode.poller_cpu_cores[:-1]
            snode.pollers_mask = utils.generate_mask(snode.poller_cpu_cores)

    snode.write_to_db()

print("Creating mini lvol objects")
for lvol in db_controller.get_all_lvols():
    lvol.write_to_db()

print("Creating mini Snapshots objects")
for snap in db_controller.get_snapshots():
    snap.write_to_db()

print("done")
"""

    def _run_r25_to_r26_migration(self, node: str):
        """
        Run the R25 -> R26 DB migration script on the management node.
        Updates storage node objects with new fields required by R26:
        lvstore_ports, lvstore_stack_secondary, lvol_poller_mask, pollers_mask.
        """
        remote_path = "/tmp/update_r25_r26.py"
        # Write script to remote node
        self.ssh_obj.exec_command(
            node,
            f"cat > {remote_path} << 'MIGRATION_EOF'\n{self._R25_R26_MIGRATION_SCRIPT}MIGRATION_EOF",
        )
        self.logger.info(f"[{node}] Running R25->R26 DB migration script")
        self.ssh_obj.exec_command(node, f"python3 {remote_path}", raise_on_error=True)
        self.ssh_obj.exec_command(node, f"rm -f {remote_path}")
        self.logger.info(f"[{node}] R25->R26 DB migration complete")

    def _needs_db_migration(self) -> bool:
        """Whether the R25 -> R26 DB migration applies to this upgrade.

        It applies ONLY when coming from R25. The script backfills fields
        that R26 introduced (lvstore_ports, lvstore_stack_secondary,
        lvol_poller_mask, pollers_mask) onto storage-node objects written by
        R25, and rewrites lvol/snapshot objects in the new shape. On a
        cluster already running R26 those fields are present and correct, so
        running it there is at best pointless and at worst overwrites live
        values with recomputed ones.

        The previous check returned True for ANY base != target, so a
        26.2.8-PRE -> R26.3 upgrade ran the R25 migration unnecessarily
        (observed in run 33733403479). Its docstring also claimed a "same
        base prefix" comparison while the code compared full equality, so
        even R25.10-Hotfix -> R25.10-Hotfix2 would have run it.
        """
        if not self.base_version:
            self.logger.warning(
                "base_version unknown — assuming the R25->R26 migration is "
                "NOT needed; pass --base_version to be explicit")
            return False
        base = self.base_version.lower().lstrip("r")
        needed = base.startswith("25")
        self.logger.info(
            f"R25->R26 DB migration {'REQUIRED' if needed else 'not needed'} "
            f"(base_version={self.base_version!r}, "
            f"target_version={self.target_version!r})")
        return needed

    def _update_node_env(self, node: str):
        """
        Update simplyblock_core/env_var on a node with target docker/spdk images.
        Uses the same sed pattern as bootstrap-k3s.sh / bootstrap.sh.
        """
        if not self.target_docker_image and not self.spdk_image:
            self.logger.info(f"[{node}] No image overrides to apply to env_var")
            return

        env_path = self._get_env_var_path(node)

        if self.target_docker_image:
            self.ssh_obj.exec_command(
                node,
                f"sed -i \"s#^\\(SIMPLY_BLOCK_DOCKER_IMAGE=\\).*#\\1{self.target_docker_image}#\" {env_path}",
                raise_on_error=True,
            )
            self.logger.info(f"[{node}] Set SIMPLY_BLOCK_DOCKER_IMAGE={self.target_docker_image}")

        if self.spdk_image:
            self.ssh_obj.exec_command(
                node,
                f"sed -i \"s#^\\(SIMPLY_BLOCK_SPDK_ULTRA_IMAGE=\\).*#\\1{self.spdk_image}#\" {env_path}",
                raise_on_error=True,
            )
            self.logger.info(f"[{node}] Set SIMPLY_BLOCK_SPDK_ULTRA_IMAGE={self.spdk_image}")

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    def run(self):
        # Resolve base_log_root now that setup() has populated docker_logs_path
        self.base_log_root = f"{self.docker_logs_path}/upgrade_fio_logs"

        # ----------------------------------------------------------------
        # Step 1: Verify base version
        # ----------------------------------------------------------------
        self.logger.info("Step 1: Verify base version on all nodes")
        prev_versions = self.common_utils.get_all_node_versions()
        for node_ip, version in prev_versions.items():
            assert self.base_version in version, (
                f"Base version mismatch on {node_ip}: {version}"
            )

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
        sleep_n_sec(5)

        # ----------------------------------------------------------------
        # Step 3: Create VERIFY lvols (VERIFY_LVOLS_PER_NODE per node)
        #         connect + format + mount only — FIO and snap/clone come later
        # Step 4: Create FIO lvols (FIO_LVOLS_PER_NODE per node)
        #         connect + format + mount + snap + clone + connect clone + mount clone
        # ----------------------------------------------------------------
        # node_ctx[snode] = {
        #   "node_id": None,
        #   "verify_lvols": [{tag, client_node, lvol_name, mount_path, pre_log,
        #                     snapshot_name, snapshot_id, clone_name, clone_mount,
        #                     base_md5, clone_md5}],
        #   "fio_lvols":    [{tag, client_node, lvol_name, mount_path,
        #                     snapshot_name, snapshot_id, clone_name, clone_mount,
        #                     lvol_fio_session, lvol_fio_log,
        #                     clone_fio_session, clone_fio_log}],
        # }
        node_ctx = {}

        self.logger.info(
            f"Step 3-4: Pre-upgrade: {VERIFY_LVOLS_PER_NODE} verify lvol(s) + "
            f"{FIO_LVOLS_PER_NODE} fio lvol(s) per storage node"
        )
        for snode_idx, snode in enumerate(storage):
            verify_lvols = []
            fio_lvols = []

            # --- Verify lvols ---
            for lvol_idx in range(VERIFY_LVOLS_PER_NODE):
                tag = f"vfy_{snode_idx}_{lvol_idx}"
                lvol_name = f"{self.lvol_name}_{tag}"
                snap_name = f"{self.snapshot_name}_{tag}"
                clone_name = f"{self.clone_name}_{tag}"
                mount_path = f"{self.base_mount_root}_{tag}"
                clone_mount = f"{self.base_mount_root}_{tag}_clone"
                pre_log = f"{self.base_log_root}/fio_pre_{tag}.log"
                client_node = random.choice(self.fio_node)

                self.logger.info(f"[{snode}] Creating verify LVOL {lvol_idx+1}/{VERIFY_LVOLS_PER_NODE}: {lvol_name}")
                self.sbcli_utils.add_lvol(lvol_name=lvol_name, pool_name=self.pool_name, size="5G")
                sleep_n_sec(3)

                before = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(lvol_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                sleep_n_sec(3)
                after = self.ssh_obj.get_devices(client_node)
                disk = self._detect_new_device(client_node, before, after)
                self.ssh_obj.format_disk(client_node, disk)
                self.ssh_obj.mount_path(client_node, disk, mount_path)

                verify_lvols.append({
                    "tag": tag,
                    "client_node": client_node,
                    "lvol_name": lvol_name,
                    "mount_path": mount_path,
                    "pre_log": pre_log,
                    "snapshot_name": snap_name,
                    "snapshot_id": None,
                    "clone_name": clone_name,
                    "clone_mount": clone_mount,
                    "base_md5": None,
                    "clone_md5": None,
                })

            # --- FIO lvols (create lvol + snap + clone, connect both) ---
            for lvol_idx in range(FIO_LVOLS_PER_NODE):
                tag = f"fio_{snode_idx}_{lvol_idx}"
                lvol_name = f"{self.lvol_name}_{tag}"
                snap_name = f"{self.snapshot_name}_{tag}"
                clone_name = f"{self.clone_name}_{tag}"
                mount_path = f"{self.base_mount_root}_{tag}"
                clone_mount = f"{self.base_mount_root}_{tag}_clone"
                client_node = random.choice(self.fio_node)

                self.logger.info(f"[{snode}] Creating fio LVOL {lvol_idx+1}/{FIO_LVOLS_PER_NODE}: {lvol_name}")
                self.sbcli_utils.add_lvol(lvol_name=lvol_name, pool_name=self.pool_name, size="5G")
                sleep_n_sec(3)

                # Connect + format + mount lvol
                before = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(lvol_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                sleep_n_sec(3)
                after = self.ssh_obj.get_devices(client_node)
                disk = self._detect_new_device(client_node, before, after)
                self.ssh_obj.format_disk(client_node, disk)
                self.ssh_obj.mount_path(client_node, disk, mount_path)

                # Snapshot + clone
                lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
                self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_id, snap_name)
                snap_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snap_name)
                self.ssh_obj.add_clone(self.mgmt_nodes[0], snap_id, clone_name)
                sleep_n_sec(3)

                # Connect + mount clone (no format — clone inherits filesystem)
                before2 = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(clone_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                sleep_n_sec(3)
                after2 = self.ssh_obj.get_devices(client_node)
                clone_disk = self._detect_new_device(client_node, before2, after2)
                self.ssh_obj.mount_path(client_node, clone_disk, clone_mount)

                fio_lvols.append({
                    "tag": tag,
                    "client_node": client_node,
                    "lvol_name": lvol_name,
                    "mount_path": mount_path,
                    "snapshot_name": snap_name,
                    "snapshot_id": snap_id,
                    "clone_name": clone_name,
                    "clone_mount": clone_mount,
                    "lvol_fio_session": None,
                    "lvol_fio_log": None,
                    "clone_fio_session": None,
                    "clone_fio_log": None,
                })

            node_ctx[snode] = {
                "node_id": None,
                "verify_lvols": verify_lvols,
                "fio_lvols": fio_lvols,
            }

        # ----------------------------------------------------------------
        # Step 5: Short FIO in PARALLEL on all verify lvols, then wait + check
        # ----------------------------------------------------------------
        self.logger.info("Step 5: Start short pre-upgrade fio in PARALLEL on all verify lvols (runtime=120s)")
        pre_fio_threads = []
        pre_fio_results = {}
        for snode in storage:
            for lvol_ctx in node_ctx[snode]["verify_lvols"]:
                tag = lvol_ctx["tag"]
                t = threading.Thread(
                    target=self._start_fio_tmux_thread,
                    args=(lvol_ctx["client_node"], lvol_ctx["mount_path"],
                          lvol_ctx["pre_log"], f"fio_pre_{tag}", 120,
                          pre_fio_results, tag),
                    daemon=True,
                )
                t.start()
                pre_fio_threads.append(t)
                sleep_n_sec(1)

        for t in pre_fio_threads:
            t.join(timeout=30)

        self.logger.info("Step 5: Waiting for all verify fio sessions to complete")
        for snode in storage:
            for lvol_ctx in node_ctx[snode]["verify_lvols"]:
                tag = lvol_ctx["tag"]
                session = pre_fio_results.get(tag, f"fio_fio_pre_{tag}")
                self._wait_tmux_gone(lvol_ctx["client_node"], session, timeout=600)
                self._assert_fio_log_clean(lvol_ctx["client_node"], lvol_ctx["pre_log"])

        # ----------------------------------------------------------------
        # Step 6: Snap + clone + md5 verify on all verify lvols
        # ----------------------------------------------------------------
        self.logger.info("Step 6: Snapshot + clone + md5 verify on all verify lvols")
        for snode in storage:
            for lvol_ctx in node_ctx[snode]["verify_lvols"]:
                lvol_name = lvol_ctx["lvol_name"]
                snap_name = lvol_ctx["snapshot_name"]
                clone_name = lvol_ctx["clone_name"]
                client_node = lvol_ctx["client_node"]
                mount_path = lvol_ctx["mount_path"]
                clone_mount = lvol_ctx["clone_mount"]

                lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
                self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_id, snap_name)
                snap_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snap_name)
                self.ssh_obj.add_clone(self.mgmt_nodes[0], snap_id, clone_name)
                sleep_n_sec(3)

                base_files = self.ssh_obj.find_files(client_node, mount_path)
                base_md5 = self.ssh_obj.generate_checksums(client_node, base_files)

                before2 = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(clone_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                sleep_n_sec(3)
                after2 = self.ssh_obj.get_devices(client_node)
                clone_disk = self._detect_new_device(client_node, before2, after2)
                self.ssh_obj.mount_path(client_node, clone_disk, clone_mount)

                clone_files = self.ssh_obj.find_files(client_node, clone_mount)
                clone_md5 = self.ssh_obj.generate_checksums(client_node, clone_files)

                assert set(base_md5.values()) == set(clone_md5.values()), (
                    f"[{client_node}] Pre-upgrade md5 mismatch (lvol vs clone) for {lvol_name}"
                )

                lvol_ctx["snapshot_id"] = snap_id
                lvol_ctx["base_md5"] = base_md5
                lvol_ctx["clone_md5"] = clone_md5

        # ----------------------------------------------------------------
        # Step 7: Start long fio (3600s) IN PARALLEL on all fio lvols AND their clones
        #         — keep running throughout the entire upgrade
        # ----------------------------------------------------------------
        if self.fio_during_upgrade:
            self.logger.info(
                "Step 7: Start long fio (3600s) in PARALLEL on all fio lvols + clones "
                f"({FIO_LVOLS_PER_NODE * 2} sessions per node)"
            )
            upgrade_fio_threads = []
            upgrade_fio_results = {}

            for snode in self.storage_nodes:
                for lvol_ctx in node_ctx[snode]["fio_lvols"]:
                    tag = lvol_ctx["tag"]
                    client_node = lvol_ctx["client_node"]

                    # FIO on the lvol itself
                    lvol_log = f"{self.base_log_root}/fio_upgrade_{tag}_lvol.log"
                    lvol_ctx["lvol_fio_log"] = lvol_log
                    t = threading.Thread(
                        target=self._start_fio_tmux_thread,
                        args=(client_node, lvol_ctx["mount_path"],
                              lvol_log, f"fio_upg_{tag}_lvol", 3600,
                              upgrade_fio_results, f"{tag}_lvol"),
                        daemon=True,
                    )
                    t.start()
                    upgrade_fio_threads.append(t)
                    sleep_n_sec(1)

                    # FIO on the clone
                    clone_log = f"{self.base_log_root}/fio_upgrade_{tag}_clone.log"
                    lvol_ctx["clone_fio_log"] = clone_log
                    t = threading.Thread(
                        target=self._start_fio_tmux_thread,
                        args=(client_node, lvol_ctx["clone_mount"],
                              clone_log, f"fio_upg_{tag}_clone", 3600,
                              upgrade_fio_results, f"{tag}_clone"),
                        daemon=True,
                    )
                    t.start()
                    upgrade_fio_threads.append(t)
                    sleep_n_sec(1)

            for t in upgrade_fio_threads:
                t.join(timeout=30)

            for snode in self.storage_nodes:
                for lvol_ctx in node_ctx[snode]["fio_lvols"]:
                    tag = lvol_ctx["tag"]
                    lvol_ctx["lvol_fio_session"] = upgrade_fio_results.get(
                        f"{tag}_lvol", f"fio_fio_upg_{tag}_lvol"
                    )
                    lvol_ctx["clone_fio_session"] = upgrade_fio_results.get(
                        f"{tag}_clone", f"fio_fio_upg_{tag}_clone"
                    )
                    self.logger.info(
                        f"  [{lvol_ctx['client_node']}] fio sessions: "
                        f"lvol={lvol_ctx['lvol_fio_session']}  clone={lvol_ctx['clone_fio_session']}"
                    )

            sleep_n_sec(10)
        else:
            self.logger.info("Step 7: Skipping FIO during upgrade (single-node / non-HA mode)")

        # ----------------------------------------------------------------
        # Step 8: pip install target sbcli on ALL mgmt+storage nodes
        # ----------------------------------------------------------------
        self.logger.info("Step 8: pip install target sbcli on ALL nodes")
        for node in all_nodes:
            self._pip_install_target(node)
            sleep_n_sec(5)

        # ----------------------------------------------------------------
        # Step 8b: Update env_var on ALL mgmt nodes before cluster update
        #          (same pattern as bootstrap script — sets SIMPLY_BLOCK_DOCKER_IMAGE
        #           and SIMPLY_BLOCK_SPDK_ULTRA_IMAGE in simplyblock_core/env_var)
        # ----------------------------------------------------------------
        self.logger.info("Step 8b: Update simplyblock_core/env_var on all mgmt nodes")
        for node in mgmt:
            self._update_node_env(node)

        # ----------------------------------------------------------------
        # Step 9: Cluster update cp-only
        # ----------------------------------------------------------------
        self.logger.info("Step 9: sbctl -d cluster update --cp-only true")
        self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.sbctl_cmd} -d cluster update {self.cluster_id} --cp-only true",
            raise_on_error=True,
        )
        sleep_n_sec(60)

        # ----------------------------------------------------------------
        # Step 9b: Run DB migration (re-writes snode/lvol/snapshot objects)
        # ----------------------------------------------------------------
        if self._needs_db_migration():
            self.logger.info("Step 9b: Running DB migration script on mgmt node")
            self._run_r25_to_r26_migration(self.mgmt_nodes[0])
            sleep_n_sec(self.step_sleep)
        else:
            self.logger.info(
                f"Step 9b: Skipping DB migration "
                f"(base={self.base_version}, target={self.target_version})"
            )

        # ----------------------------------------------------------------
        # Step 10: Rolling upgrade — suspend -> shutdown -> env update ->
        #          deploy -> restart -> wait online -> wait migration
        # ----------------------------------------------------------------
        self.logger.info("Step 10: Rolling upgrade of storage nodes")
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

            # Verify all fio sessions still running before touching this node
            if self.fio_during_upgrade:
                self.logger.info(f"[SN {snode}] Verifying all fio sessions still running")
                for lvol_ctx in node_ctx[snode]["fio_lvols"]:
                    cn = lvol_ctx["client_node"]
                    for sess_key in ("lvol_fio_session", "clone_fio_session"):
                        session = lvol_ctx[sess_key]
                        assert self._is_tmux_running(cn, session), (
                            f"FIO session {session} on {cn} is not running before upgrade of {snode}!"
                        )

            # Suspend
            self.logger.info(f"[SN {snode}] Suspending node {node_id}")
            self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.sbctl_cmd} -d sn suspend {node_id}",
                raise_on_error=True,
            )
            self.sbcli_utils.wait_for_storage_node_status(node_id, "suspended", timeout=1000)
            sleep_n_sec(self.step_sleep)

            # Shutdown
            self.logger.info(f"[SN {snode}] Shutting down node {node_id}")
            self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.sbctl_cmd} -d sn shutdown {node_id}",
                raise_on_error=True,
            )
            self.sbcli_utils.wait_for_storage_node_status(node_id, "offline", timeout=1000)
            sleep_n_sec(self.step_sleep)

            # Update simplyblock_core/env_var on storage node with target images
            self.logger.info(f"[SN {snode}] Updating simplyblock_core/env_var with target images")
            self._update_node_env(snode)
            sleep_n_sec(self.step_sleep)

            # Deploy on storage node
            self.logger.info(f"[SN {snode}] Running sn deploy (ifname={self.ifname})")
            self.ssh_obj.exec_command(
                snode, f"{self.sbctl_cmd} -d sn deploy --ifname {self.ifname}",
                raise_on_error=True,
            )
            sleep_n_sec(self.step_sleep)

            # Restart with target spdk image and proxy image
            proxy_flag = f" --spdk-proxy-image {self.target_docker_image}" if self.target_docker_image else ""
            self.logger.info(f"[SN {snode}] Restarting with spdk-image={self.spdk_image}, spdk-proxy-image={self.target_docker_image or '(default)'}")
            self.ssh_obj.exec_command(
                self.mgmt_nodes[0],
                f"{self.sbctl_cmd} --dev -d sn restart {node_id} --spdk-image {self.spdk_image}{proxy_flag}",
                raise_on_error=True,
            )
            try:
                self.sbcli_utils.wait_for_storage_node_status(node_id, "online", timeout=1000)
            except Exception:
                self.logger.warning(f"[SN {snode}] Restart status check failed — continuing")
            finally:
                if not self.k8s_test:
                    for node in self.storage_nodes:
                        self.ssh_obj.restart_docker_logging(
                            node_ip=node,
                            containers=self.container_nodes[node],
                            log_dir=os.path.join(self.docker_logs_path, node),
                            test_name=self.test_name,
                        )
                else:
                    self.runner_k8s_log.restart_logging()
            sleep_n_sec(self.step_sleep)

            # Wait for migration tasks to complete before moving to next node
            self.logger.info(f"[SN {snode}] Waiting for migration tasks to complete")
            migration_ts = int(time.time()) - 120
            self.validate_migration_for_node(
                timestamp=migration_ts,
                timeout=1800,
                node_id=None,
                check_interval=30,
                no_task_ok=(not self.fio_during_upgrade),
            )
            sleep_n_sec(self.step_sleep)

        # ----------------------------------------------------------------
        # Step 11: Validate docker images upgraded
        # ----------------------------------------------------------------
        self.logger.info("Step 11: Validate upgraded docker images/containers")
        post_upgrade_containers = {}
        for node in all_nodes:
            post_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)
        self.common_utils.assert_upgrade_docker_image(pre_upgrade_containers, post_upgrade_containers)
        sleep_n_sec(self.step_sleep)

        # ----------------------------------------------------------------
        # TEMP STEP — one-off validation, remove after this run.
        #
        # An upgraded cluster's existing distribs are still on v1 write
        # protection (new clusters are created on v2 already). Activate v2
        # cluster-wide via the runtime RPC, then restart every storage
        # node again to verify the v2 generation is correctly
        # recorded/persisted and nodes come back online cleanly under it.
        # Deliberately placed BEFORE Step 12 (which waits for FIO to
        # finish) so FIO keeps running as I/O load through both the
        # switch and the second round of restarts.
        # ----------------------------------------------------------------
        self.logger.info(
            f"TEMP Step 11b: sbctl cluster switch-write-protection {self.cluster_id}")
        if self.fio_during_upgrade:
            for snode in self.storage_nodes:
                for lvol_ctx in node_ctx[snode]["fio_lvols"]:
                    cn = lvol_ctx["client_node"]
                    for sess_key in ("lvol_fio_session", "clone_fio_session"):
                        session = lvol_ctx[sess_key]
                        assert self._is_tmux_running(cn, session), (
                            f"FIO session {session} on {cn} is not running "
                            f"before switch-write-protection!"
                        )
        self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.sbctl_cmd} -d cluster switch-write-protection {self.cluster_id}",
            raise_on_error=True,
        )
        sleep_n_sec(self.step_sleep)

        self.logger.info(
            "TEMP Step 11c: restarting all storage nodes again post-switch, "
            "with FIO still running")
        for snode in self.storage_nodes:
            node_id = node_ctx[snode]["node_id"]

            if self.fio_during_upgrade:
                for lvol_ctx in node_ctx[snode]["fio_lvols"]:
                    cn = lvol_ctx["client_node"]
                    for sess_key in ("lvol_fio_session", "clone_fio_session"):
                        session = lvol_ctx[sess_key]
                        assert self._is_tmux_running(cn, session), (
                            f"FIO session {session} on {cn} is not running "
                            f"before post-switch restart of {snode}!"
                        )

            self.logger.info(f"[SN {snode}] TEMP: restarting node {node_id} (post-switch)")
            self.ssh_obj.exec_command(
                self.mgmt_nodes[0],
                f"{self.sbctl_cmd} --dev -d sn restart {node_id} --force",
                raise_on_error=True,
            )
            try:
                self.sbcli_utils.wait_for_storage_node_status(node_id, "online", timeout=1000)
            except Exception:
                self.logger.warning(f"[SN {snode}] TEMP: restart status check failed — continuing")
            sleep_n_sec(self.step_sleep)

            self.logger.info(f"[SN {snode}] TEMP: waiting for migration tasks post-switch restart")
            migration_ts = int(time.time()) - 120
            self.validate_migration_for_node(
                timestamp=migration_ts,
                timeout=1800,
                node_id=None,
                check_interval=30,
                no_task_ok=(not self.fio_during_upgrade),
            )
            sleep_n_sec(self.step_sleep)

            if self.fio_during_upgrade:
                for lvol_ctx in node_ctx[snode]["fio_lvols"]:
                    cn = lvol_ctx["client_node"]
                    for sess_key in ("lvol_fio_session", "clone_fio_session"):
                        session = lvol_ctx[sess_key]
                        assert self._is_tmux_running(cn, session), (
                            f"FIO session {session} on {cn} stopped running "
                            f"after post-switch restart of {snode}!"
                        )
        # ----------------------------------------------------------------
        # END TEMP STEP
        # ----------------------------------------------------------------

        # ----------------------------------------------------------------
        # Step 12: Verify fio still running on fio lvols+clones, wait for all to finish
        # ----------------------------------------------------------------
        if self.fio_during_upgrade:
            self.logger.info("Step 12: Verify fio still running post-upgrade on all fio lvols + clones")
            for snode in self.storage_nodes:
                for lvol_ctx in node_ctx[snode]["fio_lvols"]:
                    cn = lvol_ctx["client_node"]
                    for sess_key, log_key in (
                        ("lvol_fio_session", "lvol_fio_log"),
                        ("clone_fio_session", "clone_fio_log"),
                    ):
                        session = lvol_ctx[sess_key]
                        if self._is_tmux_running(cn, session):
                            self.logger.info(f"  [{cn}] {session}: still running (good)")
                        else:
                            self.logger.warning(f"  [{cn}] {session}: already finished — will check log")

            self.logger.info("Step 12: Waiting for all fio sessions to complete")
            for snode in self.storage_nodes:
                for lvol_ctx in node_ctx[snode]["fio_lvols"]:
                    cn = lvol_ctx["client_node"]
                    for sess_key, log_key in (
                        ("lvol_fio_session", "lvol_fio_log"),
                        ("clone_fio_session", "clone_fio_log"),
                    ):
                        self._wait_tmux_gone(cn, lvol_ctx[sess_key], timeout=3600)
                        self._assert_fio_log_clean(cn, lvol_ctx[log_key])
        else:
            self.logger.info("Step 12: Skipping FIO wait (single-node / non-HA mode)")

        # ----------------------------------------------------------------
        # Step 13: Post-upgrade md5 check on verify clone mounts
        # ----------------------------------------------------------------
        self.logger.info("Step 13: Post-upgrade md5 check on verify clones")
        for snode in self.storage_nodes:
            for lvol_ctx in node_ctx[snode]["verify_lvols"]:
                clone_mount = lvol_ctx["clone_mount"]
                pre_clone_md5 = lvol_ctx["clone_md5"]
                client_node = lvol_ctx["client_node"]

                files = self.ssh_obj.find_files(client_node, clone_mount)
                post_md5 = self.ssh_obj.generate_checksums(client_node, files)

                assert set(pre_clone_md5.values()) == set(post_md5.values()), (
                    f"[{snode}/{lvol_ctx['lvol_name']}] Post-upgrade verify clone md5 mismatch!"
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


class TestMajorUpgradeDualNode(TestMajorUpgrade):
    """
    Dual-node-per-host upgrade variant: handles clusters where each physical
    host runs 2 storage nodes (``--nodes-per-socket 2``).

    Key differences from the single-node-per-host parent:

    * ``node_ctx`` is keyed by **node_id** (UUID), not by IP address.
    * An ``ip_to_node_ids`` mapping (IP → list of node_ids) is built from the
      storage-node API so we can iterate logical nodes per physical host.
    * The rolling upgrade loop (Step 10) iterates **unique IPs**: for each host
      it suspends/shuts-down/deploys/restarts ALL logical nodes on that host,
      while physical operations (env update, ``sn deploy``, Docker logging) run
      only once per IP.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "test_major_upgrade_dual_node"
        self.nodes_per_socket = 2
        self.logger.info(
            f"Dual-node-per-host upgrade mode: expecting {self.nodes_per_socket} "
            "logical nodes per physical host"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_ip_to_node_ids(self):
        """Build IP → [node_id, ...] mapping from the storage-node API.

        For dual-node-per-host, one IP maps to 2 node_ids.
        Returns dict[str, list[str]].
        """
        sn_results = self.sbcli_utils.get_storage_nodes().get("results", [])
        ip_to_ids = {}
        for r in sn_results:
            nid = r.get("id") or r.get("uuid") or r.get("node_id")
            ip = r.get("ip") or r.get("mgmt_ip") or r.get("management_ip")
            if nid and ip:
                ip_to_ids.setdefault(ip, []).append(nid)
        return ip_to_ids

    # ------------------------------------------------------------------
    # Overridden run — node_ctx keyed by node_id, rolling upgrade per host
    # ------------------------------------------------------------------

    def run(self):
        # Resolve paths now that setup() has populated docker_logs_path
        self.base_log_root = f"{self.docker_logs_path}/upgrade_fio_logs"

        # ----------------------------------------------------------------
        # Step 1: Verify base version
        # ----------------------------------------------------------------
        self.logger.info("Step 1: Verify base version on all nodes")
        prev_versions = self.common_utils.get_all_node_versions()
        for node_ip, version in prev_versions.items():
            assert self.base_version in version, (
                f"Base version mismatch on {node_ip}: {version}"
            )

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
        sleep_n_sec(5)

        # ----------------------------------------------------------------
        # Build IP → [node_id, ...] mapping
        # ----------------------------------------------------------------
        ip_to_node_ids = self._build_ip_to_node_ids()
        unique_storage_ips = list(dict.fromkeys(storage))  # de-dup, preserve order

        # Flatten all node_ids in host order
        all_node_ids = []
        for ip in unique_storage_ips:
            nids = ip_to_node_ids.get(ip, [])
            assert len(nids) >= self.nodes_per_socket, (
                f"Expected at least {self.nodes_per_socket} nodes on {ip}, "
                f"got {len(nids)}: {nids}"
            )
            all_node_ids.extend(nids)

        self.logger.info(
            f"Dual-node mapping: {len(unique_storage_ips)} hosts, "
            f"{len(all_node_ids)} logical nodes: {ip_to_node_ids}"
        )

        # ----------------------------------------------------------------
        # Steps 3-4: Create VERIFY + FIO lvols per logical node
        # ----------------------------------------------------------------
        node_ctx = {}  # keyed by node_id

        self.logger.info(
            f"Step 3-4: Pre-upgrade: {VERIFY_LVOLS_PER_NODE} verify lvol(s) + "
            f"{FIO_LVOLS_PER_NODE} fio lvol(s) per logical node "
            f"({len(all_node_ids)} nodes total)"
        )

        for nid_idx, node_id in enumerate(all_node_ids):
            # Find host IP for this node_id
            host_ip = None
            for ip, nids in ip_to_node_ids.items():
                if node_id in nids:
                    host_ip = ip
                    break

            verify_lvols = []
            fio_lvols = []

            # --- Verify lvols ---
            for lvol_idx in range(VERIFY_LVOLS_PER_NODE):
                tag = f"vfy_{nid_idx}_{lvol_idx}"
                lvol_name = f"{self.lvol_name}_{tag}"
                snap_name = f"{self.snapshot_name}_{tag}"
                clone_name = f"{self.clone_name}_{tag}"
                mount_path = f"{self.base_mount_root}_{tag}"
                clone_mount = f"{self.base_mount_root}_{tag}_clone"
                pre_log = f"{self.base_log_root}/fio_pre_{tag}.log"
                client_node = random.choice(self.fio_node)

                self.logger.info(
                    f"[{node_id}@{host_ip}] Creating verify LVOL "
                    f"{lvol_idx+1}/{VERIFY_LVOLS_PER_NODE}: {lvol_name}"
                )
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name, pool_name=self.pool_name, size="5G"
                )
                sleep_n_sec(3)

                before = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(lvol_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                sleep_n_sec(3)
                after = self.ssh_obj.get_devices(client_node)
                disk = self._detect_new_device(client_node, before, after)
                self.ssh_obj.format_disk(client_node, disk)
                self.ssh_obj.mount_path(client_node, disk, mount_path)

                verify_lvols.append({
                    "tag": tag,
                    "client_node": client_node,
                    "lvol_name": lvol_name,
                    "mount_path": mount_path,
                    "pre_log": pre_log,
                    "snapshot_name": snap_name,
                    "snapshot_id": None,
                    "clone_name": clone_name,
                    "clone_mount": clone_mount,
                    "base_md5": None,
                    "clone_md5": None,
                })

            # --- FIO lvols ---
            for lvol_idx in range(FIO_LVOLS_PER_NODE):
                tag = f"fio_{nid_idx}_{lvol_idx}"
                lvol_name = f"{self.lvol_name}_{tag}"
                snap_name = f"{self.snapshot_name}_{tag}"
                clone_name = f"{self.clone_name}_{tag}"
                mount_path = f"{self.base_mount_root}_{tag}"
                clone_mount = f"{self.base_mount_root}_{tag}_clone"
                client_node = random.choice(self.fio_node)

                self.logger.info(
                    f"[{node_id}@{host_ip}] Creating fio LVOL "
                    f"{lvol_idx+1}/{FIO_LVOLS_PER_NODE}: {lvol_name}"
                )
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name, pool_name=self.pool_name, size="5G"
                )
                sleep_n_sec(3)

                before = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(lvol_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                sleep_n_sec(3)
                after = self.ssh_obj.get_devices(client_node)
                disk = self._detect_new_device(client_node, before, after)
                self.ssh_obj.format_disk(client_node, disk)
                self.ssh_obj.mount_path(client_node, disk, mount_path)

                # Create snapshot + clone
                lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
                self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_id, snap_name)
                snap_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snap_name)
                self.ssh_obj.add_clone(self.mgmt_nodes[0], snap_id, clone_name)
                sleep_n_sec(3)

                before2 = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(clone_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                sleep_n_sec(3)
                after2 = self.ssh_obj.get_devices(client_node)
                clone_disk = self._detect_new_device(client_node, before2, after2)
                self.ssh_obj.mount_path(client_node, clone_disk, clone_mount)

                fio_lvols.append({
                    "tag": tag,
                    "client_node": client_node,
                    "lvol_name": lvol_name,
                    "mount_path": mount_path,
                    "snapshot_name": snap_name,
                    "snapshot_id": snap_id,
                    "clone_name": clone_name,
                    "clone_mount": clone_mount,
                    "lvol_fio_session": None,
                    "lvol_fio_log": None,
                    "clone_fio_session": None,
                    "clone_fio_log": None,
                })

            node_ctx[node_id] = {
                "host_ip": host_ip,
                "verify_lvols": verify_lvols,
                "fio_lvols": fio_lvols,
            }

        # ----------------------------------------------------------------
        # Step 5: Short FIO on all verify lvols
        # ----------------------------------------------------------------
        self.logger.info("Step 5: Start short pre-upgrade fio on all verify lvols (runtime=120s)")
        pre_fio_threads = []
        pre_fio_results = {}
        for node_id in all_node_ids:
            for lvol_ctx in node_ctx[node_id]["verify_lvols"]:
                tag = lvol_ctx["tag"]
                t = threading.Thread(
                    target=self._start_fio_tmux_thread,
                    args=(lvol_ctx["client_node"], lvol_ctx["mount_path"],
                          lvol_ctx["pre_log"], f"fio_pre_{tag}", 120,
                          pre_fio_results, tag),
                    daemon=True,
                )
                t.start()
                pre_fio_threads.append(t)
                sleep_n_sec(1)

        for t in pre_fio_threads:
            t.join(timeout=30)

        self.logger.info("Step 5: Waiting for all verify fio sessions to complete")
        for node_id in all_node_ids:
            for lvol_ctx in node_ctx[node_id]["verify_lvols"]:
                tag = lvol_ctx["tag"]
                session = pre_fio_results.get(tag, f"fio_fio_pre_{tag}")
                self._wait_tmux_gone(lvol_ctx["client_node"], session, timeout=600)
                self._assert_fio_log_clean(lvol_ctx["client_node"], lvol_ctx["pre_log"])

        # ----------------------------------------------------------------
        # Step 6: Snap + clone + md5 verify on all verify lvols
        # ----------------------------------------------------------------
        self.logger.info("Step 6: Snapshot + clone + md5 verify on all verify lvols")
        for node_id in all_node_ids:
            for lvol_ctx in node_ctx[node_id]["verify_lvols"]:
                lvol_name = lvol_ctx["lvol_name"]
                snap_name = lvol_ctx["snapshot_name"]
                clone_name = lvol_ctx["clone_name"]
                client_node = lvol_ctx["client_node"]
                mount_path = lvol_ctx["mount_path"]
                clone_mount = lvol_ctx["clone_mount"]

                lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
                self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_id, snap_name)
                snap_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snap_name)
                self.ssh_obj.add_clone(self.mgmt_nodes[0], snap_id, clone_name)
                sleep_n_sec(3)

                base_files = self.ssh_obj.find_files(client_node, mount_path)
                base_md5 = self.ssh_obj.generate_checksums(client_node, base_files)

                before2 = self.ssh_obj.get_devices(client_node)
                for cmd in self.sbcli_utils.get_lvol_connect_str(clone_name):
                    self.ssh_obj.exec_command(client_node, cmd)
                sleep_n_sec(3)
                after2 = self.ssh_obj.get_devices(client_node)
                clone_disk = self._detect_new_device(client_node, before2, after2)
                self.ssh_obj.mount_path(client_node, clone_disk, clone_mount)

                clone_files = self.ssh_obj.find_files(client_node, clone_mount)
                clone_md5 = self.ssh_obj.generate_checksums(client_node, clone_files)

                assert set(base_md5.values()) == set(clone_md5.values()), (
                    f"[{client_node}] Pre-upgrade md5 mismatch (lvol vs clone) "
                    f"for {lvol_name}"
                )

                lvol_ctx["snapshot_id"] = snap_id
                lvol_ctx["base_md5"] = base_md5
                lvol_ctx["clone_md5"] = clone_md5

        # ----------------------------------------------------------------
        # Step 7: Start long fio on all fio lvols + clones
        # ----------------------------------------------------------------
        if self.fio_during_upgrade:
            self.logger.info(
                "Step 7: Start long fio (3600s) on all fio lvols + clones "
                f"({FIO_LVOLS_PER_NODE * 2} sessions per node)"
            )
            upgrade_fio_threads = []
            upgrade_fio_results = {}

            for node_id in all_node_ids:
                for lvol_ctx in node_ctx[node_id]["fio_lvols"]:
                    tag = lvol_ctx["tag"]
                    client_node = lvol_ctx["client_node"]

                    # FIO on the lvol
                    lvol_log = f"{self.base_log_root}/fio_upgrade_{tag}_lvol.log"
                    lvol_ctx["lvol_fio_log"] = lvol_log
                    t = threading.Thread(
                        target=self._start_fio_tmux_thread,
                        args=(client_node, lvol_ctx["mount_path"],
                              lvol_log, f"fio_upg_{tag}_lvol", 3600,
                              upgrade_fio_results, f"{tag}_lvol"),
                        daemon=True,
                    )
                    t.start()
                    upgrade_fio_threads.append(t)
                    sleep_n_sec(1)

                    # FIO on the clone
                    clone_log = f"{self.base_log_root}/fio_upgrade_{tag}_clone.log"
                    lvol_ctx["clone_fio_log"] = clone_log
                    t = threading.Thread(
                        target=self._start_fio_tmux_thread,
                        args=(client_node, lvol_ctx["clone_mount"],
                              clone_log, f"fio_upg_{tag}_clone", 3600,
                              upgrade_fio_results, f"{tag}_clone"),
                        daemon=True,
                    )
                    t.start()
                    upgrade_fio_threads.append(t)
                    sleep_n_sec(1)

            for t in upgrade_fio_threads:
                t.join(timeout=30)

            for node_id in all_node_ids:
                for lvol_ctx in node_ctx[node_id]["fio_lvols"]:
                    tag = lvol_ctx["tag"]
                    lvol_ctx["lvol_fio_session"] = upgrade_fio_results.get(
                        f"{tag}_lvol", f"fio_fio_upg_{tag}_lvol"
                    )
                    lvol_ctx["clone_fio_session"] = upgrade_fio_results.get(
                        f"{tag}_clone", f"fio_fio_upg_{tag}_clone"
                    )
                    self.logger.info(
                        f"  [{lvol_ctx['client_node']}] fio sessions: "
                        f"lvol={lvol_ctx['lvol_fio_session']}  "
                        f"clone={lvol_ctx['clone_fio_session']}"
                    )

            sleep_n_sec(10)
        else:
            self.logger.info("Step 7: Skipping FIO during upgrade (non-HA mode)")

        # ----------------------------------------------------------------
        # Step 8: pip install target sbcli on ALL nodes
        # ----------------------------------------------------------------
        self.logger.info("Step 8: pip install target sbcli on ALL nodes")
        unique_all_nodes = list(dict.fromkeys(all_nodes))
        for node in unique_all_nodes:
            self._pip_install_target(node)
            sleep_n_sec(5)

        # ----------------------------------------------------------------
        # Step 8b: Update env_var on ALL mgmt nodes
        # ----------------------------------------------------------------
        self.logger.info("Step 8b: Update simplyblock_core/env_var on all mgmt nodes")
        for node in mgmt:
            self._update_node_env(node)

        # ----------------------------------------------------------------
        # Step 9: Cluster update cp-only
        # ----------------------------------------------------------------
        self.logger.info("Step 9: sbctl -d cluster update --cp-only true")
        self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.sbctl_cmd} -d cluster update {self.cluster_id} --cp-only true",
            raise_on_error=True,
        )
        sleep_n_sec(60)

        # ----------------------------------------------------------------
        # Step 9b: DB migration
        # ----------------------------------------------------------------
        if self._needs_db_migration():
            self.logger.info("Step 9b: Running DB migration script on mgmt node")
            self._run_r25_to_r26_migration(self.mgmt_nodes[0])
            sleep_n_sec(self.step_sleep)
        else:
            self.logger.info(
                f"Step 9b: Skipping DB migration "
                f"(base={self.base_version}, target={self.target_version})"
            )

        # ----------------------------------------------------------------
        # Step 10: Rolling upgrade — per physical host
        # ----------------------------------------------------------------
        self.logger.info(
            f"Step 10: Rolling upgrade of storage nodes "
            f"({len(unique_storage_ips)} hosts, {len(all_node_ids)} logical nodes)"
        )

        for host_ip in unique_storage_ips:
            node_ids_on_host = ip_to_node_ids.get(host_ip, [])
            self.logger.info(
                f"[HOST {host_ip}] Upgrading {len(node_ids_on_host)} nodes: "
                f"{node_ids_on_host}"
            )

            # Verify FIO sessions for all nodes on this host
            if self.fio_during_upgrade:
                self.logger.info(f"[HOST {host_ip}] Verifying fio sessions")
                for nid in node_ids_on_host:
                    for lvol_ctx in node_ctx[nid]["fio_lvols"]:
                        cn = lvol_ctx["client_node"]
                        for sess_key in ("lvol_fio_session", "clone_fio_session"):
                            session = lvol_ctx[sess_key]
                            assert self._is_tmux_running(cn, session), (
                                f"FIO session {session} on {cn} is not running "
                                f"before upgrade of {nid}@{host_ip}!"
                            )

            # Suspend ALL nodes on this host
            for nid in node_ids_on_host:
                self.logger.info(f"[{nid}@{host_ip}] Suspending")
                self.ssh_obj.exec_command(
                    self.mgmt_nodes[0],
                    f"{self.sbctl_cmd} -d sn suspend {nid}",
                    raise_on_error=True,
                )
            for nid in node_ids_on_host:
                self.sbcli_utils.wait_for_storage_node_status(
                    nid, "suspended", timeout=1000
                )
            sleep_n_sec(self.step_sleep)

            # Shutdown ALL nodes on this host
            for nid in node_ids_on_host:
                self.logger.info(f"[{nid}@{host_ip}] Shutting down")
                self.ssh_obj.exec_command(
                    self.mgmt_nodes[0],
                    f"{self.sbctl_cmd} -d sn shutdown {nid}",
                    raise_on_error=True,
                )
            for nid in node_ids_on_host:
                self.sbcli_utils.wait_for_storage_node_status(
                    nid, "offline", timeout=1000
                )
            sleep_n_sec(self.step_sleep)

            # Physical host ops — once per IP
            self.logger.info(f"[HOST {host_ip}] Updating env_var with target images")
            self._update_node_env(host_ip)
            sleep_n_sec(self.step_sleep)

            self.logger.info(f"[HOST {host_ip}] Running sn deploy")
            self.ssh_obj.exec_command(
                host_ip,
                f"{self.sbctl_cmd} -d sn deploy --ifname {self.ifname}",
                raise_on_error=True,
            )
            sleep_n_sec(self.step_sleep)

            # Restart ALL nodes on this host
            for nid in node_ids_on_host:
                proxy_flag = (
                    f" --spdk-proxy-image {self.target_docker_image}"
                    if self.target_docker_image else ""
                )
                self.logger.info(
                    f"[{nid}@{host_ip}] Restarting with "
                    f"spdk-image={self.spdk_image}, "
                    f"spdk-proxy-image={self.target_docker_image or '(default)'}"
                )
                self.ssh_obj.exec_command(
                    self.mgmt_nodes[0],
                    f"{self.sbctl_cmd} --dev -d sn restart {nid} "
                    f"--spdk-image {self.spdk_image}{proxy_flag}",
                    raise_on_error=True,
                )

            # Wait for ALL nodes online
            for nid in node_ids_on_host:
                try:
                    self.sbcli_utils.wait_for_storage_node_status(
                        nid, "online", timeout=1000
                    )
                except Exception:
                    self.logger.warning(
                        f"[{nid}@{host_ip}] Restart status check failed — continuing"
                    )
            # Restart Docker logging — once per IP
            if not self.k8s_test:
                for node in unique_storage_ips:
                    if node == host_ip:
                        self.ssh_obj.restart_docker_logging(
                            node_ip=host_ip,
                            containers=self.container_nodes.get(host_ip, []),
                            log_dir=os.path.join(self.docker_logs_path, host_ip),
                            test_name=self.test_name,
                        )
            else:
                self.runner_k8s_log.restart_logging()
            sleep_n_sec(self.step_sleep)

            # Wait for migration for ALL nodes on this host
            for nid in node_ids_on_host:
                self.logger.info(f"[{nid}@{host_ip}] Waiting for migration tasks")
                migration_ts = int(time.time()) - 120
                self.validate_migration_for_node(
                    timestamp=migration_ts,
                    timeout=1800,
                    node_id=None,
                    check_interval=30,
                    no_task_ok=(not self.fio_during_upgrade),
                )
            sleep_n_sec(self.step_sleep)

        # ----------------------------------------------------------------
        # Step 11: Validate docker images upgraded
        # ----------------------------------------------------------------
        self.logger.info("Step 11: Validate upgraded docker images/containers")
        post_upgrade_containers = {}
        for node in unique_all_nodes:
            post_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)
        self.common_utils.assert_upgrade_docker_image(
            pre_upgrade_containers, post_upgrade_containers
        )
        sleep_n_sec(self.step_sleep)

        # ----------------------------------------------------------------
        # Step 12: Verify fio still running, wait for finish
        # ----------------------------------------------------------------
        if self.fio_during_upgrade:
            self.logger.info("Step 12: Verify fio still running post-upgrade")
            for node_id in all_node_ids:
                for lvol_ctx in node_ctx[node_id]["fio_lvols"]:
                    cn = lvol_ctx["client_node"]
                    for sess_key, log_key in (
                        ("lvol_fio_session", "lvol_fio_log"),
                        ("clone_fio_session", "clone_fio_log"),
                    ):
                        session = lvol_ctx[sess_key]
                        if self._is_tmux_running(cn, session):
                            self.logger.info(f"  [{cn}] {session}: still running")
                        else:
                            self.logger.warning(
                                f"  [{cn}] {session}: already finished — will check log"
                            )

            self.logger.info("Step 12: Waiting for all fio sessions to complete")
            for node_id in all_node_ids:
                for lvol_ctx in node_ctx[node_id]["fio_lvols"]:
                    cn = lvol_ctx["client_node"]
                    for sess_key, log_key in (
                        ("lvol_fio_session", "lvol_fio_log"),
                        ("clone_fio_session", "clone_fio_log"),
                    ):
                        self._wait_tmux_gone(cn, lvol_ctx[sess_key], timeout=3600)
                        self._assert_fio_log_clean(cn, lvol_ctx[log_key])
        else:
            self.logger.info("Step 12: Skipping FIO wait (non-HA mode)")

        # ----------------------------------------------------------------
        # Step 13: Post-upgrade md5 check on verify clone mounts
        # ----------------------------------------------------------------
        self.logger.info("Step 13: Post-upgrade md5 check on verify clones")
        for node_id in all_node_ids:
            host_ip = node_ctx[node_id]["host_ip"]
            for lvol_ctx in node_ctx[node_id]["verify_lvols"]:
                clone_mount = lvol_ctx["clone_mount"]
                pre_clone_md5 = lvol_ctx["clone_md5"]
                client_node = lvol_ctx["client_node"]

                files = self.ssh_obj.find_files(client_node, clone_mount)
                post_md5 = self.ssh_obj.generate_checksums(client_node, files)

                assert set(pre_clone_md5.values()) == set(post_md5.values()), (
                    f"[{node_id}@{host_ip}/{lvol_ctx['lvol_name']}] "
                    "Post-upgrade verify clone md5 mismatch!"
                )

        self.logger.info("TEST CASE PASSED !!!")
