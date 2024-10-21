import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
import random


class TestMultiFioSnapshotDowntime(TestClusterBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lvol_size = "100G"
        self.fio_size = "40G"
        self.numjobs = 16
        self.iodepth = 1
        self.fio_runtime = 500  # seconds
        self.node_with_lvols = []  # Track nodes with LVOLs
        self.node_id_ip = {}

    def run(self):
        """Performs the steps of the test case"""
        self.logger.info("Starting test case: Running `fio` with downtime and snapshots/clones")

        # Step 1: Create storage pool
        self.logger.info("Creating storage pool")
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        lvol_vs_node = {}  # Track which LVOL is on which node
        lvol_fio_info = {}  # Track device and mount info for each LVOL

        # Step 2: Create the first 2 LVOLs on nodes without LVOLs
        for i in range(2):
            node_uuid = self.sbcli_utils.get_node_without_lvols()  # Get node without LVOLs
            node_details = self.sbcli_utils.get_storage_node_details(node_uuid)
            node_ip = node_details[0]["mgmt_ip"]
            self.node_with_lvols.append(node_uuid)
            self.node_id_ip[node_uuid] = node_ip

            lvol_name = f"test_lvol_{i + 1}"
            self.logger.info(f"Creating LVOL {lvol_name} on node {node_ip}")
            self.sbcli_utils.add_lvol(lvol_name=lvol_name, pool_name=self.pool_name, size=self.lvol_size, host_id=node_uuid)
            lvol_vs_node[lvol_name] = node_uuid

            # Get devices and mount them for non-trimwrite workloads
            initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            self.logger.info(f"Initial devices on node {self.mgmt_nodes[0]}: {initial_devices}")
            
            # Step 3: Check for new device after connecting the LVOL
            sleep_n_sec(2)

            connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            new_device = [dev for dev in final_devices if dev not in initial_devices][0]
            self.logger.info(f"Final devices after LVOL connection: {final_devices}")
            lvol_fio_info[lvol_name] = {"device": f"/dev/{new_device[0]}" if new_device else None}

        # Step 4: Create 3 more LVOLs on the same two nodes with LVOLs (to leave 1 node without LVOLs)
        for i in range(2, 5):
            node_uuid = self.node_with_lvols[i % 2]  # Distribute the remaining 3 LVOLs across the two nodes
            lvol_name = f"test_lvol_{i + 1}"
            self.logger.info(f"Creating LVOL {lvol_name} on node {node_uuid}")
            self.sbcli_utils.add_lvol(lvol_name=lvol_name, pool_name=self.pool_name, size=self.lvol_size, host_id=node_uuid)
            
            lvol_vs_node[lvol_name] = self.node_id_ip[node_uuid]

            # Get devices and mount them for non-trimwrite workloads
            initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            self.logger.info(f"Initial devices on node {self.mgmt_nodes[0]}: {initial_devices}")
            
            sleep_n_sec(2)

            connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

            sleep_n_sec(3)

            final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            new_device = [dev for dev in final_devices if dev not in initial_devices][0]
            self.logger.info(f"Final devices after LVOL connection: {final_devices}")
            lvol_fio_info[lvol_name] = {"device": f"/dev/{new_device[0]}" if new_device else None}

        # Step 5: Identify the node without LVOLs
        node_without_lvols = self.sbcli_utils.get_node_without_lvols()
        node_details = self.sbcli_utils.get_storage_node_details(node_uuid)
        node_without_lvols_node_ip = node_details[0]["mgmt_ip"]
        self.node_id_ip[node_without_lvols] = node_without_lvols_node_ip
        self.logger.info(f"Node without LVOLs: {node_without_lvols}")

        # Step 6: Run different fio workloads in parallel on the LVOLs (2 nodes)
        self.logger.info("Starting fio workloads on LVOLs")
        fio_threads = []
        fio_workloads = [("randrw", "4K"), ("read", "32K"), ("write", "64K"), ("trimwrite", "16K")]

        device_count = 1
        for lvol_name, _ in lvol_vs_node.items():
            fio_workload = fio_workloads[int(lvol_name.split('_')[-1]) % len(fio_workloads)]
            
            # For "trimwrite", don't format or mount the LVOL
            if fio_workload[0] == "trimwrite":
                fio_thread = threading.Thread(
                    target=self.ssh_obj.run_fio_test,
                    args=(self.mgmt_nodes[0], lvol_fio_info[lvol_name]["device"], None),
                    kwargs={
                        "name": f"fio_{lvol_name}",
                        "rw": fio_workload[0],
                        "bs": fio_workload[1],
                        "size": self.fio_size,
                        "numjobs": self.numjobs,
                        "iodepth": self.iodepth,
                        "runtime": self.fio_runtime
                    }
                )
            else:
                # Format and mount the LVOL for other workloads
                self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=lvol_fio_info[lvol_name]["device"])
                fs_type = random.choice(["xfs", "ext4"])
                self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=lvol_fio_info[lvol_name]["device"], fs_type=fs_type)
                mount_path = f"/mnt/device_{device_count}_{fs_type}"
                self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=lvol_fio_info[lvol_name]["device"], mount_path=mount_path)
                lvol_fio_info[lvol_name]["mount_path"] = mount_path
                device_count += 1

                fio_thread = threading.Thread(
                    target=self.ssh_obj.run_fio_test,
                    args=(self.mgmt_nodes[0], None, mount_path),
                    kwargs={
                        "name": f"fio_{lvol_name}",
                        "rw": fio_workload[0],
                        "bs": fio_workload[1],
                        "size": self.fio_size,
                        "numjobs": self.numjobs,
                        "iodepth": self.iodepth,
                        "verify": "md5",
                        "runtime": self.fio_runtime
                    }
                )
            
            fio_threads.append(fio_thread)
            fio_thread.start()

        # Step 8: Stop the SPDK process on the node without LVOLs
        self.logger.info("Stopping SPDK process on node without LVOLs")
        self.ssh_obj.stop_spdk_process(node=node_without_lvols_node_ip)

        # Step 9: Delete one LVOL while node is down
        self.logger.info("Deleting LVOL while node is down")
        lvol_to_delete = list(lvol_vs_node.keys())[0]  # Select the first LVOL for deletion
        self.sbcli_utils.delete_lvol(lvol_name=lvol_to_delete)

        del lvol_vs_node[lvol_to_delete]
        del lvol_fio_info[lvol_to_delete]

        # Step 7: Create snapshot and clone for the current LVOL
        for lvol_name, _ in lvol_vs_node.items():
            snapshot_name = f"{lvol_name}_snapshot"
            self.logger.info(f"Taking snapshot of LVOL {lvol_name}")
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            self.ssh_obj.add_snapshot(node=self.mgmt_nodes[0], lvol_id=lvol_id, snapshot_name=snapshot_name)

            clone_name = f"{snapshot_name}_clone"
            snapshot_id = self.ssh_obj.get_snapshot_id(node=self.mgmt_nodes[0], snapshot_name=snapshot_name)
            self.logger.info(f"Cloning snapshot {snapshot_name}")
            self.ssh_obj.add_clone(node=self.mgmt_nodes[0], snapshot_id=snapshot_id, clone_name=clone_name)

        # Step 10: Wait for node restart validate its status
    
        node_wait_thread = threading.Thread(
            target=self.sbcli_utils.wait_for_storage_node_status,
            args=(node_without_lvols, "online", 500)
            )

        # Step 11: Wait for fio threads to complete and validate
        self.logger.info("Waiting for fio workloads to finish")
        self.common_utils.manage_fio_threads(
            node=self.mgmt_nodes[0],
            threads=fio_threads,
            timeout=1000
        )
        for fio_thread in fio_threads:
            fio_thread.join()

        node_wait_thread.join()

        self.logger.info("Test case completed successfully")
