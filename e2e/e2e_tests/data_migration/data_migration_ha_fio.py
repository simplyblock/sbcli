from pathlib import Path
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec, convert_bytes_to_gb_tb
from logger_config import setup_logger
from datetime import datetime
import random



class FioWorkloadTest(TestClusterBase):
    """
    This test automates:
    1. Create lvols on each node, connect lvols, check devices, and mount them.
    2. Run fio workloads.
    3. Shutdown and restart nodes, remount and check fio processes.
    4. Validate migration tasks for a specific node, ensuring fio continues running on unaffected nodes.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mount_path = "/mnt"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("Starting test case: FIO Workloads with lvol connections and migrations.")

        # Step 1: Create 4 lvols on each node and connect them
        lvol_fio_path = {}
        sn_lvol_data = {}

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )

        for i in range(0, len(self.storage_nodes)):
            node_uuid = self.sbcli_utils.get_node_without_lvols()
            sn_lvol_data[node_uuid] = []
            self.logger.info(f"Creating 2 lvols on node {node_uuid}.")
            for j in range(2):
                lvol_name = f"test_lvol_{i+1}_{j+1}"
                self.sbcli_utils.add_lvol(lvol_name=lvol_name, pool_name=self.pool_name, size="1G", host_id=node_uuid)
                sn_lvol_data[node_uuid].append(lvol_name)
                lvol_fio_path[lvol_name] = {"lvol_id": self.sbcli_utils.get_lvol_id(lvol_name=lvol_name),
                                            "mount_path": None,
                                            "disk": None}
        
        trim_node = random.choice(list(sn_lvol_data.keys()))
        fs = "/mnt"
        while fs:
            fs = self.ssh_obj.get_mount_points(self.mgmt_nodes[0],
                                               "/mnt")
            self.logger.info(f"FS mounts: {fs}")
            for device in fs:
                self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=device)

        device_count = 1
        for node, lvol_list in sn_lvol_data.items():
            # node_ip = self.get_node_ip(node)
            # Step 2: Connect lvol to the node
            for lvol in lvol_list:
                initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            
                connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol)
                for connect_str in connect_ls:
                    self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)
                sleep_n_sec(5)

                # Step 3: Check for new device after connecting the lvol
                final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
                self.logger.info(f"Initial vs final disk on node {node}:")
                self.logger.info(f"Initial: {initial_devices}")
                self.logger.info(f"Final: {final_devices}")

                # Step 4: Identify the new device and mount it (if applicable)
                for device in final_devices:
                    if device not in initial_devices:
                        disk_use = f"/dev/{device.strip()}"
                        lvol_fio_path[lvol]["disk"] = disk_use
                        break
                if trim_node == node and lvol[-1] == "2":
                    continue
                fs_type = "xfs" if lvol[-1] == "1" else "ext4"
                # Unmount, format, and mount the device
                self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use)
                sleep_n_sec(2)
                self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=disk_use, fs_type=fs_type)
                sleep_n_sec(2)
                mount_path = f"/mnt/device_{device_count}_{fs_type}"
                device_count += 1
                self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=disk_use, mount_path=mount_path)
                lvol_fio_path[lvol]["mount_path"] = mount_path
                sleep_n_sec(2)

        self.logger.info(f"SN List: {sn_lvol_data}")
        self.logger.info(f"LVOL Mounts: {lvol_fio_path}")

        # Step 5: Run fio workloads with different configurations
        fio_threads = self.run_fio(lvol_fio_path)

        # SCE-1: Graceful Shutdown

        # Step 6: Continue with node shutdown, restart, and migration task validation
        affected_node = list(sn_lvol_data.keys())[0]
        self.logger.info(f"Shutting down node {affected_node}.")

        fio_process_terminated = ["fio_test_lvol_1_1", "fio_test_lvol_1_2"]

        timestamp = int(datetime.now().timestamp())

        self.shutdown_node_and_verify(affected_node, process_name=fio_process_terminated)

        sleep_n_sec(300)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        self.logger.info(f"Validating migration tasks for node {affected_node}.")
        self.validate_migration_for_node(timestamp, 1000, None)

        sleep_n_sec(30)

        fio_process = self.ssh_obj.find_process_name(self.mgmt_nodes[0], 'fio')
        self.logger.info(f"FIO PROCESS: {fio_process}")
        if not fio_process:
            raise RuntimeError("FIO process was interrupted on unaffected nodes.")
        for fio in fio_process_terminated:
            for running_fio in fio_process:
                assert fio not in running_fio, "FIO Process running on restarted node"

        lvol_list = sn_lvol_data[affected_node]
        affected_fio = {}
        for lvol in lvol_list:
            affected_fio[lvol] = {}
            affected_fio[lvol]["mount_path"] = lvol_fio_path[lvol]["mount_path"]
            lvol_fio_path[lvol]["disk"] = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0],
                                                                         lvol_id=lvol_fio_path[lvol]["lvol_id"])
            affected_fio[lvol]["disk"] = lvol_fio_path[lvol]["disk"]
            fs_type = "xfs" if lvol[-1] == "1" else "ext4"
            if lvol_fio_path[lvol]["mount_path"]:
                fs = lvol_fio_path[lvol]["mount_path"]
                while fs:
                    fs = self.ssh_obj.get_mount_points(self.mgmt_nodes[0],
                                                       lvol_fio_path[lvol]["mount_path"])
                    self.logger.info(f"FS mounts: {fs}")
                    for device in fs:
                        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=device)
                self.ssh_obj.mount_path(self.mgmt_nodes[0],
                                        device=lvol_fio_path[lvol]["disk"],
                                        mount_path=lvol_fio_path[lvol]["mount_path"])
        fio_threads.extend(self.run_fio(affected_fio))

        sleep_n_sec(120)

        # SCE-2: Node crash
        # # Step 7: Stop container on another node
        affected_node = list(sn_lvol_data.keys())[1]
        timestamp = int(datetime.now().timestamp())
        self.logger.info(f"Stopping docker container on node {affected_node}.")

        self.stop_container_verify(affected_node)

        sleep_n_sec(300)

        self.logger.info(f"Validating migration tasks for node {affected_node}.")
        self.validate_migration_for_node(timestamp, 1000, None)

        sleep_n_sec(30)

        output, _ = self.ssh_obj.exec_command(
            self.mgmt_nodes[0], command=f"{self.base_cmd} sn list"
        )
        self.logger.info(f"Output for sn list: {output}")

        lvol_list = sn_lvol_data[affected_node]
        affected_fio = {}
        
        for lvol in lvol_list:
            affected_fio[lvol] = {}
            affected_fio[lvol]["mount_path"] = lvol_fio_path[lvol]["mount_path"]
            lvol_fio_path[lvol]["disk"] = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0],
                                                                         lvol_id=lvol_fio_path[lvol]["lvol_id"])
            affected_fio[lvol]["disk"] = lvol_fio_path[lvol]["disk"]
            fs_type = "xfs" if lvol[-1] == "1" else "ext4"
            if lvol_fio_path[lvol]["mount_path"]:
                fs = self.ssh_obj.get_mount_points(self.mgmt_nodes[0],
                                                   lvol_fio_path[lvol]["mount_path"])
                for device in fs:
                    self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=device)
                self.ssh_obj.mount_path(self.mgmt_nodes[0],
                                        device=lvol_fio_path[lvol]["disk"],
                                        mount_path=lvol_fio_path[lvol]["mount_path"])
        for lvol in list(affected_fio.keys()):
            self.ssh_obj.kill_processes(node=self.mgmt_nodes[0],
                                        process_name=lvol)
        sleep_n_sec(100)

        fio_threads.extend(self.run_fio(affected_fio))

        output, _ = self.ssh_obj.exec_command(
            self.mgmt_nodes[0], command=f"{self.base_cmd} sn list"
        )
        self.logger.info(f"Output for sn list: {output}")

        
        # SCE-3: Instance terminate and new add
        # # Step 8: Stop instance
        timestamp = int(datetime.now().timestamp())
        affected_node = list(sn_lvol_data.keys())[2]
        affected_node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=affected_node)
        instance_id = affected_node_details[0]["cloud_instance_id"]

        self.logger.info("Creating New instance")
        new_node_instance_id, new_node_ip = \
            self.common_utils.create_instance_from_existing(ec2_resource=self.ec2_resource, 
                                                            instance_id=instance_id,
                                                            instance_name="e2e-new-instance")
        
        sleep_n_sec(120)
        
        fio_process = self.ssh_obj.find_process_name(self.mgmt_nodes[0], 'fio')
        self.logger.info(f"FIO PROCESS: {fio_process}")
        if not fio_process:
            raise RuntimeError("FIO process was interrupted on unaffected nodes.")
        self.logger.info("FIO process is running uninterrupted.")

        self.ssh_obj.connect(
            address=new_node_ip,
            bastion_server_address=self.bastion_server,
        )
        
        # Step 9: Add node
        # self.sbcli_utils.add_storage_node(
        #     cluster_id=self.cluster_id,
        #     node_ip=new_node_ip,
        #     ifname="eth0",
        #     max_lvol=affected_node_details[0]["max_lvol"],
        #     max_snap=affected_node_details[0]["max_snap"],
        #     max_prov=affected_node_details[0]["max_prov"],
        #     number_of_distribs=affected_node_details[0]["number_of_distribs"],
        #     number_of_devices=affected_node_details[0]["number_of_devices"],
        #     partitions=affected_node_details[0]["num_partitions_per_dev"],
        #     jm_percent=affected_node_details[0]["jm_percent"],
        #     disable_ha_jm=not affected_node_details[0]["enable_ha_jm"],
        #     enable_test_device=affected_node_details[0]["enable_test_device"],
        #     namespace=affected_node_details[0]["namespace"],
        #     iobuf_small_pool_count=affected_node_details[0]["iobuf_small_pool_count"],
        #     iobuf_large_pool_count=affected_node_details[0]["iobuf_large_pool_count"],
        #     spdk_debug=affected_node_details[0]["spdk_debug"],
        #     spdk_image=affected_node_details[0]["spdk_image"],
        #     spdk_cpu_mask=affected_node_details[0]["spdk_cpu_mask"]
        # )

        self.ssh_obj.deploy_storage_node(node=new_node_ip,
                                         max_lvol=affected_node_details[0]["max_lvol"],
                                         max_prov_gb=convert_bytes_to_gb_tb(affected_node_details[0]["max_prov"]),

)

        self.ssh_obj.add_storage_node(
            node=self.mgmt_nodes[0],
            cluster_id=self.cluster_id,
            node_ip=new_node_ip,
            ifname="eth0",
            partitions=affected_node_details[0]["num_partitions_per_dev"],
            disable_ha_jm=not affected_node_details[0]["enable_ha_jm"],
            enable_test_device=affected_node_details[0]["enable_test_device"],
            # iobuf_small_pool_count=affected_node_details[0]["iobuf_small_pool_count"],
            # iobuf_large_pool_count=affected_node_details[0]["iobuf_large_pool_count"],
            spdk_debug=affected_node_details[0]["spdk_debug"],
            spdk_image=affected_node_details[0]["spdk_image"],
            spdk_cpu_mask=affected_node_details[0]["spdk_cpu_mask"]
        )
        sleep_n_sec(200)
        new_node = self.sbcli_utils.get_node_without_lvols()
        self.sbcli_utils.wait_for_storage_node_status(node_id=new_node, status="online", timeout=500)

        # self.logger.info(f"Validating migration tasks for node {new_node}.")
        # self.validate_migration_for_node(timestamp, 5000, None)

        timestamp = int(datetime.now().timestamp())

        self.common_utils.stop_ec2_instance(self.ec2_resource,
                                            instance_id=instance_id)
        
        sleep_n_sec(100)

        # self.validate_migration_for_node(timestamp, 5000, None)

        # Step 10: Remove stopped instance
        sn_remove = f"{self.base_cmd} storage-node remove {affected_node} --force-remove"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=sn_remove)
        sleep_n_sec(10)
        sn_delete = f"{self.base_cmd} storage-node delete {affected_node}"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=sn_delete)
        
        sleep_n_sec(200)

        del sn_lvol_data[affected_node]
        
        affected_node_ip = affected_node_details[0]["mgmt_ip"]
        self.logger.info(f"Affected node ip: {affected_node_ip}")
        del self.ssh_obj.ssh_connections[affected_node_ip]

        sn_lvol_data[new_node] = {}

        output, _ = self.ssh_obj.exec_command(
            self.mgmt_nodes[0], command=f"{self.base_cmd} sn list"
        )
        self.logger.info(f"Output for sn list: {output}")

        # self.common_utils.terminate_instance(self.ec2_resource, instance_id)

        storage_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        
        for node in storage_nodes:
            assert node["id"] != affected_node, "Deleted node still present in storage node list"

        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                             threads=fio_threads,
                                             timeout=2000)

        # Wait for all fio threads to finish
        for thread in fio_threads:
            thread.join()

        output, _ = self.ssh_obj.exec_command(
            self.mgmt_nodes[0], command=f"{self.base_cmd} sn list"
        )
        self.logger.info(f"Output for sn list: {output}")

        # self.common_utils.terminate_instance(self.ec2_resource, new_node_instance_id)

        self.logger.info("Test completed successfully.")

    def get_node_ip(self, node_id):
        return self.sbcli_utils.get_storage_node_details(node_id)[0]["mgmt_ip"]

    def run_fio(self, lvol_fio_path):
        self.logger.info("Starting fio workloads on the logical volumes with different configurations.")
        fio_threads = []
        fio_configs = [("randrw", "4K"), ("read", "32K"), ("write", "64K")]
        for lvol, data in lvol_fio_path.items():
            fio_run = random.choice(fio_configs)
            if data["mount_path"]:
                thread = threading.Thread(
                                target=self.ssh_obj.run_fio_test,
                                args=(self.mgmt_nodes[0], None, data["mount_path"], None),
                                kwargs={
                                    "name": f"fio_{lvol}",
                                    "rw": fio_run[0],
                                    "ioengine": "libaio",
                                    "iodepth": 1,
                                    "bs": fio_run[1],
                                    "size": "300M",
                                    "time_based": True,
                                    "runtime": 2000,
                                    "output_file": f"{Path.home()}/{lvol}.log",
                                    "numjobs": 2,
                                    "debug": self.fio_debug
                                }
                            )
            else:
                thread = threading.Thread(
                                target=self.ssh_obj.run_fio_test,
                                args=(self.mgmt_nodes[0], data["disk"], None, None),
                                kwargs={
                                    "name": f"fio_{lvol}",
                                    "rw": "trimwrite",
                                    "ioengine": "libaio",
                                    "iodepth": 1,
                                    "bs": "16K",
                                    "size": "300M",
                                    "time_based": True,
                                    "runtime": 2000,
                                    "output_file": f"{Path.home()}/{lvol}.log",
                                    "nrfiles": 2,
                                    "debug": self.fio_debug
                                }
                            )
            fio_threads.append(thread)
            thread.start()
        return fio_threads

    def shutdown_node_and_verify(self, node_id, process_name):
        """Shutdown the node and ensure fio is uninterrupted."""
        fio_process = self.ssh_obj.find_process_name(self.mgmt_nodes[0], 'fio')
        self.logger.info(f"FIO PROCESS: {fio_process}")

        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        output = output[0].strip().split('\n')
        self.logger.info(f"Mount paths before suspend: {output}")
        self.sbcli_utils.suspend_node(node_id)
        self.logger.info(f"Node {node_id} suspended successfully.")

        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        output = output[0].strip().split('\n')
        self.logger.info(f"Mount paths after suspend: {output}")

        sleep_n_sec(30)

        fio_process = self.ssh_obj.find_process_name(self.mgmt_nodes[0], 'fio')
        self.logger.info(f"FIO PROCESS: {fio_process}")
        if not fio_process:
            raise RuntimeError("FIO process was interrupted on unaffected nodes.")
        for fio in process_name:
            for running_fio in fio_process: 
                assert fio not in running_fio, "FIO Process running on suspended node"
        self.logger.info("FIO process is running uninterrupted.")

        self.sbcli_utils.shutdown_node(node_id)
        self.logger.info(f"Node {node_id} shut down successfully.")

        self.sbcli_utils.wait_for_storage_node_status(node_id=node_id, status="offline",
                                                      timeout=800)

        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        output = output[0].strip().split('\n')
        self.logger.info(f"Mount paths after shutdown: {output}")
        
        # Validate fio is running on other nodes
        fio_process = self.ssh_obj.find_process_name(self.mgmt_nodes[0], 'fio')
        if not fio_process:
            raise RuntimeError("FIO process was interrupted on unaffected nodes.")
        for fio in process_name:
            for running_fio in fio_process: 
                assert fio not in running_fio, "FIO Process running on offline node"
        self.logger.info("FIO process is running uninterrupted.")

        sleep_n_sec(30)

        # Restart node
        self.sbcli_utils.restart_node(node_id)
        self.logger.info(f"Node {node_id} restarted successfully.")
        
        self.sbcli_utils.wait_for_storage_node_status(node_id=node_id, status="online",
                                                      timeout=800)

        self.sbcli_utils.wait_for_health_status(node_id=node_id, status=True,
                                                timeout=800)
        
        storage_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        for node in storage_nodes:
            self.sbcli_utils.wait_for_storage_node_status(node_id=node['id'], status="online",
                                                          timeout=800)

            self.sbcli_utils.wait_for_health_status(node_id=node['id'], status=True,
                                                    timeout=800)

        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        output = output[0].strip().split('\n')
        self.logger.info(f"Mount paths after restart: {output}")

        fio_process = self.ssh_obj.find_process_name(self.mgmt_nodes[0], 'fio')
        if not fio_process:
            raise RuntimeError("FIO process was interrupted on unaffected nodes.")
        for fio in process_name:
            for running_fio in fio_process: 
                assert fio not in running_fio, "FIO Process running on restarted node"
        self.logger.info("FIO process is running uninterrupted.")

    def stop_container_verify(self, node_id):
        """Shutdown the node and ensure fio is uninterrupted."""
        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        output = output[0].strip().split('\n')
        self.logger.info(f"Mount paths before shutdown: {output}")
        
        node_details = self.sbcli_utils.get_storage_node_details(node_id)
        node_ip = node_details[0]["mgmt_ip"]
        self.ssh_obj.stop_spdk_process(node_ip, node_details[0]["rpc_port"])

        self.logger.info(f"Docker container on node {node_id} stopped successfully.")

        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        output = output[0].strip().split('\n')
        self.logger.info(f"Mount paths after stop: {output}")

        self.logger.info(f"Waiting for node {node_id} to be restarted automatically.")

        self.sbcli_utils.wait_for_storage_node_status(node_id=node_id, status="online",
                                                      timeout=800)
        self.sbcli_utils.wait_for_health_status(node_id=node_id, status=True,
                                                timeout=800)
        
        storage_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        for node in storage_nodes:
            self.sbcli_utils.wait_for_storage_node_status(node_id=node['id'], status="online",
                                                          timeout=800)

            self.sbcli_utils.wait_for_health_status(node_id=node['id'], status=True,
                                                    timeout=800)
        
        sleep_n_sec(300)
        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        output = output[0].strip().split('\n')
        self.logger.info(f"Mount paths after restart: {output}")

        fio_process = self.ssh_obj.find_process_name(self.mgmt_nodes[0], 'fio')
        if not fio_process:
            raise RuntimeError("FIO process was interrupted on unaffected nodes.")
        # NOTE: FIO Process can exit or hang when node is crashed. Hence commenting this validation
        # for fio in process_name:
        #     for running_fio in fio_process: 
        #         assert fio not in running_fio, "FIO Process running on crashed node"
        self.logger.info("FIO process is running uninterrupted.")
