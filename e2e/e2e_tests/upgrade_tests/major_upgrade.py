import os
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from pathlib import Path



class TestMajorUpgrade(TestClusterBase):
    """
    Steps:
    1. Check base version in input matches sbcli version on all the nodes
    2. Create storage pool
    3. Create LVOL
    4. Connect LVOL
    5. Mount Device
    6. Start FIO runs and wait for it to complete
    7. Take snapshots and clones. Take md5 of lvols and clones
    8. Upgrade to target version
    9. Check target version once upgrade completes.
    10. Check current lvols and clones md5sum, should match
    11. Try creating new snapshot and clones from older lvols and clones and their md5 matches or not
    12. Create new lvols, run fio on them and let that complete.
    13. Create snapshot and clones as well.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.base_version = kwargs.get("base_version")
        self.target_version = kwargs.get("target_version")
        self.snapshot_name = "upgrade_snap"
        self.clone_name = "upgrade_clone"
        self.test_name = "major_upgrade_test"
        self.mount_path = f"{Path.home()}/upgrade_test_fio"
        self.log_path = f"{os.path.dirname(self.mount_path)}/upgrade_fio_log.log"
        self.logger.info(f"Running upgrade test from {self.base_version} to {self.target_version}")

    def run(self):
        self.logger.info("Step 1: Verify base version on all nodes")
        prev_versions = self.common_utils.get_all_node_versions()
        for node_ip, version in prev_versions.items():
            assert self.base_version in version, f"Base version mismatch on {node_ip}: {version}"
        
        self.logger.info("Getting Containers on all the nodes before upgrade!!")
        pre_upgrade_containers = {}
        mgmt, storage = self.sbcli_utils.get_all_nodes_ip()
        all_nodes = mgmt + storage
        for node in all_nodes:
            pre_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)

        self.logger.info("Step 2: Recreate storage pool and add LVOL")
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.sbcli_utils.add_lvol(lvol_name=self.lvol_name, pool_name=self.pool_name, size="5G")

        self.logger.info("Step 3-5: Connect LVOL, format, and mount")
        initial_devices = self.ssh_obj.get_devices(self.mgmt_nodes[0])
        connect_cmds = self.sbcli_utils.get_lvol_connect_str(self.lvol_name)
        for cmd in connect_cmds:
            self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

        final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        disk_use = None
        self.logger.info("Initial vs final disk:")
        self.logger.info(f"Initial: {initial_devices}")
        self.logger.info(f"Final: {final_devices}")
        for device in final_devices:
            if device not in initial_devices:
                self.logger.info(f"Using disk: /dev/{device.strip()}")
                disk_use = f"/dev/{device.strip()}"
                break

        self.ssh_obj.format_disk(self.mgmt_nodes[0], disk_use)
        self.ssh_obj.mount_path(self.mgmt_nodes[0], disk_use, self.mount_path)

        self.logger.info("Step 6: Start FIO and wait")
        fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
                                      args=(self.mgmt_nodes[0], None, self.mount_path, self.log_path),
                                      kwargs={"name": "fio_run_pre_upgrade", "runtime": 120, "debug": self.fio_debug})
        fio_thread.start()
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                             threads=[fio_thread],
                                             timeout=300)

        self.logger.info("Step 7: Snapshot and Clone + MD5 of LVOL")
        self.ssh_obj.add_snapshot(self.mgmt_nodes[0], self.sbcli_utils.get_lvol_id(self.lvol_name), f"{self.snapshot_name}_pre")
        snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], f"{self.snapshot_name}_pre")
        self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, f"{self.clone_name}_pre")

        files = self.ssh_obj.find_files(self.mgmt_nodes[0], self.mount_path)
        pre_upgrade_lvol_md5 = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], files)

        initial_devices = self.ssh_obj.get_devices(self.mgmt_nodes[0])
        connect_cmds = self.sbcli_utils.get_lvol_connect_str(f"{self.clone_name}_pre")
        for cmd in connect_cmds:
            self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

        final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        disk_use = None
        self.logger.info("Initial vs final disk:")
        self.logger.info(f"Initial: {initial_devices}")
        self.logger.info(f"Final: {final_devices}")
        for device in final_devices:
            if device not in initial_devices:
                self.logger.info(f"Using disk: /dev/{device.strip()}")
                disk_use = f"/dev/{device.strip()}"
                break

        self.ssh_obj.mount_path(self.mgmt_nodes[0], disk_use, f"{self.mount_path}_clone_pre")

        files = self.ssh_obj.find_files(self.mgmt_nodes[0], f"{self.mount_path}_clone_pre")
        pre_upgrade_clone_md5 = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], files)

        original_checksum = set(pre_upgrade_lvol_md5.values())
        final_checksum = set(pre_upgrade_clone_md5.values())

        self.logger.info(f"Set Original checksum: {original_checksum}")
        self.logger.info(f"Set Final checksum: {final_checksum}")

        assert original_checksum == final_checksum, "Checksum mismatch between lvol and clone before upgrade!!"

        self.logger.info("Step 8: Perform Upgrade")

        package_name = f"{self.base_cmd}=={self.target_version}" if self.target_version != "latest" else self.base_cmd

        self.ssh_obj.exec_command(self.mgmt_nodes[0], f"pip install {package_name} --upgrade")
        sleep_n_sec(10)

        cmd = f"{self.base_cmd} cluster graceful-shutdown {self.cluster_id}"
        self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

        node_sample = self.sbcli_utils.get_storage_nodes()["results"][0]
        max_lvol = node_sample["max_lvol"]
        max_prov = int(node_sample["max_prov"] / (1024**3))  # Convert bytes to GB
        
        for snode in self.storage_nodes:
            cmd = f"pip install {package_name} --upgrade"
            self.ssh_obj.exec_command(snode, cmd)
            sleep_n_sec(10)
            self.ssh_obj.deploy_storage_node(
                node=snode,
                max_lvol=max_lvol,
                max_prov_gb=max_prov
            )
            sleep_n_sec(10)
        
        upgrade_cmd = f"{self.base_cmd} -d cluster update {self.cluster_id} --restart true --spdk-image simplyblock/spdk:main-latest"
        self.ssh_obj.exec_command(self.mgmt_nodes[0], upgrade_cmd)
        sleep_n_sec(180)

        self.logger.info("Step 9: Validate upgraded version")
        post_upgrade_containers = {}
        for node in all_nodes:
            post_upgrade_containers[node] = self.ssh_obj.get_image_dict(node=node)
        
        self.common_utils.assert_upgrade_docker_image(pre_upgrade_containers, post_upgrade_containers)

        self.logger.info("Step 10: Verify pre-upgrade LVOL checksum")
        post_files = self.ssh_obj.find_files(self.mgmt_nodes[0], self.mount_path)
        post_md5_lvol = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], post_files)

        original_checksum = set(pre_upgrade_lvol_md5.values())
        final_checksum = set(post_md5_lvol.values())

        self.logger.info(f"Set Original checksum: {original_checksum}")
        self.logger.info(f"Set Final checksum: {final_checksum}")

        assert original_checksum == final_checksum, "Checksum mismatch after upgrade!!"

        self.logger.info("Step 11: Clone from old snapshot and verify MD5")
        files = self.ssh_obj.find_files(self.mgmt_nodes[0], f"{self.mount_path}_clone_pre")
        post_upgrade_clone_md5 = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], files)

        original_checksum = set(pre_upgrade_clone_md5.values())
        final_checksum = set(post_upgrade_clone_md5.values())

        self.logger.info(f"Set Original checksum: {original_checksum}")
        self.logger.info(f"Set Final checksum: {final_checksum}")

        assert original_checksum == final_checksum, "Post-upgrade clone checksum mismatch!!"

        self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, f"{self.clone_name}_pre_post")
        initial_devices = self.ssh_obj.get_devices(self.mgmt_nodes[0])
        connect_cmds = self.sbcli_utils.get_lvol_connect_str(f"{self.clone_name}_pre_post")
        for cmd in connect_cmds:
            self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

        final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        disk_use = None
        self.logger.info("Initial vs final disk:")
        self.logger.info(f"Initial: {initial_devices}")
        self.logger.info(f"Final: {final_devices}")
        for device in final_devices:
            if device not in initial_devices:
                self.logger.info(f"Using disk: /dev/{device.strip()}")
                disk_use = f"/dev/{device.strip()}"
                break

        self.ssh_obj.mount_path(self.mgmt_nodes[0], disk_use, f"{self.mount_path}_clone_pre_post")

        files = self.ssh_obj.find_files(self.mgmt_nodes[0], f"{self.mount_path}_clone_pre_post")
        pre_post_upgrade_clone_md5 = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], files)

        original_checksum = set(pre_upgrade_clone_md5.values())
        final_checksum = set(pre_post_upgrade_clone_md5.values())

        self.logger.info(f"Set Original checksum: {original_checksum}")
        self.logger.info(f"Set Final checksum: {final_checksum}")

        assert original_checksum == final_checksum, "Post-upgrade clone create and older clone checksum mismatch!!"

        self.logger.info("Step 12-13: Create new LVOL, run fio, snapshot + clone")
        new_lvol = f"{self.lvol_name}_new"
        self.sbcli_utils.add_lvol(lvol_name=new_lvol, pool_name=self.pool_name, size="5G")


        initial_devices = self.ssh_obj.get_devices(self.mgmt_nodes[0])
        connect_cmds = self.sbcli_utils.get_lvol_connect_str(new_lvol)
        for cmd in connect_cmds:
            self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

        final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        disk_use = None
        self.logger.info("Initial vs final disk:")
        self.logger.info(f"Initial: {initial_devices}")
        self.logger.info(f"Final: {final_devices}")
        for device in final_devices:
            if device not in initial_devices:
                self.logger.info(f"Using disk: /dev/{device.strip()}")
                disk_use = f"/dev/{device.strip()}"
                break

        self.ssh_obj.format_disk(self.mgmt_nodes[0], disk_use)
        new_mount = f"{self.mount_path}_{new_lvol}"
        self.ssh_obj.mount_path(self.mgmt_nodes[0], disk_use, new_mount)
        
        fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
                                      args=(self.mgmt_nodes[0], None, new_mount, self.log_path + "_new"),
                                      kwargs={"name": "fio_run_post_upgrade", "runtime": 120,"debug": self.fio_debug})
        fio_thread.start()
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                             threads=[fio_thread],
                                             timeout=300)

        self.ssh_obj.add_snapshot(self.mgmt_nodes[0], self.sbcli_utils.get_lvol_id(new_lvol), f"{self.snapshot_name}_post")
        self.ssh_obj.add_clone(self.mgmt_nodes[0], self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], f"{self.snapshot_name}_post"),
                               f"{self.clone_name}_post")

        self.logger.info("TEST CASE PASSED !!!")
        