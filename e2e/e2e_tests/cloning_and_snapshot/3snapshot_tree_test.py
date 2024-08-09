import json
import os
import threading
import time
from utils.common_utils import sleep_n_sec
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from logger_config import setup_logger

class TestNestedCloneTrees:
    def __init__(self, **kwargs):
        self.ssh_obj = SshUtils(bastion_server=os.getenv("BASTION_SERVER"))
        self.logger = setup_logger(__name__)
        self.sbcli_utils = SbcliUtils(
            cluster_api_url=os.getenv("API_BASE_URL"),
            cluster_id=os.getenv("CLUSTER_ID"),
            cluster_secret=os.getenv("CLUSTER_SECRET")
        )
        self.common_utils = CommonUtils(self.sbcli_utils, self.ssh_obj)
        self.pool_name = "test_pool"
        self.lvol_name = "lvol_2_1"
        self.mount_dir = "/mnt"
        self.fs_type = "ext4"
        self.lvol_device = {}

    def setup(self):
        self.logger.info("Inside setup function")
        self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
        for node in self.mgmt_nodes + self.storage_nodes:
            self.logger.info(f"**Connecting to node** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=os.getenv("BASTION_SERVER")
            )
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=self.mount_dir)
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()

    def create_lvol(self):
        self.sbcli_utils.add_lvol(
            lvol_name=self.lvol_name,
            pool_name=self.pool_name,
            size="250G",
            distr_ndcs=2,
            distr_npcs=1
        )
        device = self.ssh_obj.get_device_from_lvol_name(node=self.mgmt_nodes[0], lvol_name=self.lvol_name)
        self.lvol_device = {"Device": device, "Path": f"{self.mount_dir}/{self.lvol_name}", "Log": f"{self.mount_dir}/{self.lvol_name}/log.json"}

    def format_fs(self, device):
        self.ssh_obj.format_disk(device=device, fs_type=self.fs_type)

    def run_fio_workload(self, mount_point):
        self.ssh_obj.run_fio_test(
            node=self.mgmt_nodes[0],
            directory=mount_point,
            readwrite="randrw",
            bs="4K-128K",
            size="1G",
            name="test",
            numjobs=5
        )

    def create_snapshot_and_clone(self, lvol_name, snapshot_prefix, clone_prefix, depth=1):
        snapshot_name = f"{snapshot_prefix}_{depth}"
        self.ssh_obj.create_snapshot(node=self.mgmt_nodes[0], lvol_name=lvol_name, snapshot_name=snapshot_name)
        clone_name = f"{clone_prefix}_{depth}"
        self.ssh_obj.create_clone(node=self.mgmt_nodes[0], snapshot_name=snapshot_name, clone_name=clone_name)
        return clone_name

    def run(self):
        self.logger.info("Inside run function")
        self.create_lvol()
        device = self.lvol_device["Device"]
        self.format_fs(device)
        mount_point = self.lvol_device["Path"]
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=device, mount_path=mount_point)
        
        self.run_fio_workload(mount_point)
        
        for i in range(40):
            clone_name = self.create_snapshot_and_clone(self.lvol_name, "snapshot", "clone", depth=i+1)
            clone_device = self.ssh_obj.get_device_from_lvol_name(node=self.mgmt_nodes[0], lvol_name=clone_name)
            clone_mount_point = f"{self.mount_dir}/{clone_name}"
            self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=clone_device, mount_path=clone_mount_point)
            self.run_fio_workload(clone_mount_point)
            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=clone_mount_point)

    def teardown(self):
        self.logger.info("Inside teardown function")
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=self.lvol_device["Device"])
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()
        for node, ssh in self.ssh_obj.ssh_connections.items():
            self.logger.info(f"Closing node ssh connection for {node}")
            ssh.close()

if __name__ == "__main__":
    test = TestNestedCloneTrees()
    test.setup()
    try:
        test.run()
    finally:
        test.teardown()
