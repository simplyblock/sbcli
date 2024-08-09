import json
import os
import threading
import time
from utils.common_utils import sleep_n_sec
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from logger_config import setup_logger

class TestMixedWorkloadWithSnapshots:
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
        self.workload_size = "10G"
        self.snapshot_interval = 2
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
            size="160G",
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
            size=self.workload_size,
            runtime=360,
            name="test",
            time_based=True
        )

    def create_snapshots(self):
        while True:
            snapshot_name = f"{self.lvol_name}_snapshot_{int(time.time())}"
            self.ssh_obj.create_snapshot(node=self.mgmt_nodes[0], lvol_name=self.lvol_name, snapshot_name=snapshot_name)
            time.sleep(self.snapshot_interval)

    def create_lvols_from_snapshots(self):
        snapshots = self.ssh_obj.list_snapshots(node=self.mgmt_nodes[0])
        snapshot_list = [s for s in snapshots if s.startswith(f"{self.lvol_name}_snapshot_")]
        random.shuffle(snapshot_list)
        for snapshot in snapshot_list:
            clone_name = f"{snapshot}_clone"
            self.ssh_obj.create_clone(node=self.mgmt_nodes[0], snapshot_name=snapshot, clone_name=clone_name)
            device = self.ssh_obj.get_device_from_lvol_name(node=self.mgmt_nodes[0], lvol_name=clone_name)
            mount_point = f"{self.mount_dir}/{clone_name}"
            self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=device, mount_path=mount_point)
            self.format_fs(device)

    def run(self):
        self.logger.info("Inside run function")
        self.create_lvol()
        device = self.lvol_device["Device"]
        self.format_fs(device)
        mount_point = self.lvol_device["Path"]
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=device, mount_path=mount_point)
        
        fio_thread = threading.Thread(target=self.run_fio_workload, args=(mount_point,))
        fio_thread.start()

        snapshot_thread = threading.Thread(target=self.create_snapshots)
        snapshot_thread.start()

        fio_thread.join()
        snapshot_thread.join(timeout=360)

        self.create_lvols_from_snapshots()

    def teardown(self):
        self.logger.info("Inside teardown function")
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=self.lvol_name)
        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        nqn = lvol_details[0]["nqn"]
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=self.lvol_device["Device"])
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=f"sudo nvme disconnect -n {nqn}")
        for node, ssh in self.ssh_obj.ssh_connections.items():
            self.logger.info(f"Closing node ssh connection for {node}")
            ssh.close()

if __name__ == "__main__":
    test = TestMixedWorkloadWithSnapshots()
    test.setup()
    try:
        test.run()
    finally:
        test.teardown()
