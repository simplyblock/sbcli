from datetime import datetime
from pathlib import Path
import threading
from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
from utils.common_utils import sleep_n_sec
from utils import proxmox

class TestRestartNodeOnAnotherHost(TestClusterBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.new_node_ip = kwargs.get("new_nodes")
        self.test_name = "restart_node_on_another_host"
        self.mount_base = "/mnt/"
        self.log_base = f"{Path.home()}/"
        assert self.new_node_ip, "Missing required input: new_node_ip"

        if isinstance(self.new_node_ip, list):
            self.new_node_ip = self.new_node_ip[0]

    def run(self):
        fio_threads = []
        self.logger.info("Starting test to restart node on another host")

        # Step 1: Create lvols, snapshots, clones on all nodes
        self.sbcli_utils.add_storage_pool(self.pool_name)
        lvol_details = {}
        restart_target = {}

        for i, _ in enumerate(self.storage_nodes):
            node_uuid = self.sbcli_utils.get_node_without_lvols()
            lvol_name = f"lvl_{generate_random_sequence(4)}_{i}"
            self.sbcli_utils.add_lvol(lvol_name, self.pool_name, size="5G", host_id=node_uuid)

            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for cmd in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

            device = self.ssh_obj.get_lvol_vs_device(self.mgmt_nodes[0], lvol_id)
            mount_path = f"{self.mount_base}/{lvol_name}"
            log_path = f"{self.log_base}/{lvol_name}.log"
            self.ssh_obj.format_disk(self.mgmt_nodes[0], device)
            self.ssh_obj.mount_path(self.mgmt_nodes[0], device, mount_path)

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.mgmt_nodes[0], None, mount_path, log_path),
                kwargs={
                    "size": "500M",
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": 600,
                },
            )
            fio_thread.start()
            fio_threads.append(fio_thread)

            sleep_n_sec(5)

            lvol_details[lvol_name] = {
                "ID": self.sbcli_utils.get_lvol_id(lvol_name),
                "Mount": mount_path,
                "Log": log_path,
                "Clone": {
                    "ID": None,
                    "Snapshot": None,
                    "Log": None,
                    "Mount": None,
                }
            }

            snapshot_name = f"snap_{lvol_name}"
            self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_id, snapshot_name)
            snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name)
            clone_name = f"clone_{lvol_name}"
            self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)

            clone_id = self.sbcli_utils.get_lvol_id(clone_name)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            for cmd in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

            device = self.ssh_obj.get_lvol_vs_device(self.mgmt_nodes[0], clone_id)
            cl_mount = f"{self.mount_base}/{clone_name}"
            cl_log = f"{self.log_base}/{clone_name}.log"
            self.ssh_obj.format_disk(self.mgmt_nodes[0], device)
            self.ssh_obj.mount_path(self.mgmt_nodes[0], device, cl_mount)

            lvol_details[lvol_name]["Clone"]["ID"] = clone_id
            lvol_details[lvol_name]["Clone"]["Snapshot"] = snapshot_name
            lvol_details[lvol_name]["Clone"]["Log"] = cl_log
            lvol_details[lvol_name]["Clone"]["Mount"] = cl_mount

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.mgmt_nodes[0], None, cl_mount, cl_log),
                kwargs={
                    "size": "500M",
                    "name": f"{clone_name}_fio",
                    "rw": "randrw",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": 600,
                },
            )
            fio_thread.start()
            fio_threads.append(fio_thread)

            if i == 0:
                restart_target = {
                    "node_uuid": node_uuid,
                    "lvol_id": lvol_id,
                    "lvol_name": lvol_name,
                    "clone_name": clone_name,
                    "clone_id": clone_id
                }

        # Step 2: Shutdown original node via Proxmox
        node_details = self.sbcli_utils.get_storage_node_details(restart_target["node_uuid"])[0]
        old_ip = node_details["mgmt_ip"]
        proxmox_id, vm_id = proxmox.get_proxmox(old_ip)
        self.logger.info(f"Stopping VM {vm_id} on proxmox {proxmox_id}")
        try:
            proxmox.stop_vm(proxmox_id, vm_id)

            # Step 3: Wait for schedulable state
            self.sbcli_utils.wait_for_storage_node_status(restart_target["node_uuid"],
                                                        status="schedulable",
                                                        timeout=600)

            # Step 4: Deploy node config on new IP
            node_sample = self.sbcli_utils.get_storage_nodes()["results"][0]
            max_lvol = node_sample["max_lvol"]
            max_prov = int(node_sample["max_prov"] / (1024**3))
            self.ssh_obj.deploy_storage_node(self.new_node_ip, max_lvol, max_prov)

            timestamp = int(datetime.now().timestamp())

            # Step 5: Restart node with new IP
            restart_cmd = f"{self.base_cmd} storage-node restart {restart_target['node_uuid']} --node-ip {self.new_node_ip}:5000"
            self.ssh_obj.exec_command(self.mgmt_nodes[0], restart_cmd)

            self.sbcli_utils.wait_for_storage_node_status(restart_target["node_uuid"],
                                                        status="online",
                                                        timeout=600)
            
            self.storage_nodes.append(self.new_node_ip)
            containers = self.ssh_obj.get_running_containers(node_ip=self.new_node_ip)
            self.container_nodes[self.new_node_ip] = containers
            
            for node in self.storage_nodes:
                self.ssh_obj.restart_docker_logging(
                    node_ip=node,
                    containers=self.container_nodes[node],
                    log_dir=self.docker_logs_path,
                    test_name=self.test_name
                )

            # Step 6: Disconnect old NVMe devices
            devices = self.ssh_obj.get_nvme_device_subsystems(self.mgmt_nodes[0])
            for dev in devices:
                if dev["traddr"] == old_ip:
                    self.ssh_obj.disconnect_lvol_node_device(self.mgmt_nodes[0], dev["device"])

            # Step 7: Reconnect using new IP only

            node_details = self.sbcli_utils.get_storage_node_details(restart_target["node_uuid"])[0]
            new_ip = node_details["data_nics"]["ip4_address"]
            
            
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=restart_target["lvol_name"])
            self.logger.info(f"Output: {connect_ls}")
            for cmd in connect_ls:
                self.logger.info(f"Output: {cmd}")
                if new_ip in cmd:
                    for _ in range(10):
                        _, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
                        if not err:
                            break
                        sleep_n_sec(5)

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=restart_target["clone_name"])
            self.logger.info(f"Output: {connect_ls}")
            for cmd in connect_ls:
                self.logger.info(f"Output: {cmd}")
                if new_ip in cmd:
                    for _ in range(10):
                        _, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
                        if not err:
                            break
                        sleep_n_sec(5)

            # Step 8: Now mark node as primary
            make_primary_cmd = f"{self.base_cmd} --dev storage-node make-primary {restart_target['node_uuid']}"
            self.ssh_obj.exec_command(self.mgmt_nodes[0], make_primary_cmd)

            # Step 9: Wait for fio and node online
            self.common_utils.manage_fio_threads(self.mgmt_nodes[0], fio_threads, timeout=700)

            for lvol_name, lvol_detail in lvol_details.items():
                self.logger.info(f"Checking fio log for lvol and clone for {lvol_name}")
                self.common_utils.validate_fio_test(node=self.mgmt_nodes[0], log_file=lvol_detail["Log"])
                self.common_utils.validate_fio_test(node=self.mgmt_nodes[0], log_file=lvol_detail["Clone"]["Log"])
            self.logger.info(f"Testing migration jobs after timestamp")
            self.validate_migration_for_node(timestamp, 2000, None, 60, no_task_ok=False)
            
            for node in self.sbcli_utils.get_storage_nodes()["results"]:
                assert node["status"] == "online", f"{node['id']} is not online"
                assert node["health_check"], f"{node['id']} health check failed"
        except Exception as e:
            raise e
        finally:
            proxmox.start_vm(proxmox_id, vm_id, 600)

        self.logger.info("TEST CASE PASSED !!!")
