from datetime import datetime
from pathlib import Path
import threading
from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
from utils.common_utils import sleep_n_sec


class TestAddNodesDuringFioRun(TestClusterBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.new_nodes = kwargs.get("new_nodes")
        self.test_name = "add_nodes_during_fio"
        self.mount_base = "/mnt/"
        self.log_base = f"{Path.home()}/"
        self.logger.info(f"New Nodes to Add: {self.new_nodes}")

    def run(self):
        self.logger.info("Starting Test: Add Nodes During FIO Run")

        # Step 1: Create lvol on existing nodes
        fio_threads = []
        lvol_details = {}
        self.sbcli_utils.add_storage_pool(self.pool_name)
        sleep_n_sec(10)
        for i, _ in enumerate(self.storage_nodes):
            lvol_name = f"lvl_{generate_random_sequence(4)}{i}"
            mount_path = f"{self.mount_base}/{lvol_name}"
            log_path = f"{self.log_base}/{lvol_name}.log"

            node_uuid = self.sbcli_utils.get_node_without_lvols()

            self.sbcli_utils.add_lvol(lvol_name, self.pool_name, size="10G",
                                      host_id=node_uuid)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], connect_str)

            device = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0], lvol_id=self.sbcli_utils.get_lvol_id(lvol_name))
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

            sleep_n_sec(10)
            
            snapshot_name = f"snap_{lvol_name}"
            self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_details[lvol_name]["ID"], snapshot_name)

            snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name=snapshot_name)

            sleep_n_sec(10)

            clone_name = f"clone_{lvol_name}"

            self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)

            clone_id = self.sbcli_utils.get_lvol_id(clone_name)

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], connect_str)

            cl_mount_path = f"{self.mount_base}/{clone_name}"
            cl_log_path = f"{self.log_base}/{clone_name}.log"

            lvol_details[lvol_name]["Clone"]["ID"] = clone_id
            lvol_details[lvol_name]["Clone"]["Snapshot"] = snapshot_name
            lvol_details[lvol_name]["Clone"]["Log"] = cl_mount_path
            lvol_details[lvol_name]["Clone"]["Mount"] = cl_log_path

            device = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0], lvol_id=clone_id)
            self.ssh_obj.format_disk(self.mgmt_nodes[0], device)
            self.ssh_obj.mount_path(self.mgmt_nodes[0], device, cl_mount_path)

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.mgmt_nodes[0], None, cl_mount_path, cl_log_path),
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

            sleep_n_sec(30)


        sleep_n_sec(30)

        # Step 2: Suspend cluster
        # self.logger.info("Suspending the Cluster")
        # self.ssh_obj.suspend_cluster(self.mgmt_nodes[0], self.cluster_id)

        # Step 3: Add new nodes
        self.logger.info("Adding new nodes")

        node_sample = self.sbcli_utils.get_storage_nodes()["results"][0]
        max_lvol = node_sample["max_lvol"]
        max_prov = int(node_sample["max_prov"] / (1024**3))  # Convert bytes to GB


        new_nodes_id = []
        timestamp = int(datetime.now().timestamp())
        cluster_details = None
        for ip in self.new_nodes:
            self.logger.info(f"Configuring and deploying storage node: {ip}")
            self.ssh_obj.deploy_storage_node(ip, max_lvol, max_prov)
            self.ssh_obj.add_storage_node(self.mgmt_nodes[0], self.cluster_id, ip,
                                          spdk_image=node_sample["spdk_image"],
                                          partitions=node_sample["num_partitions_per_dev"],
                                          disable_ha_jm= not node_sample["enable_ha_jm"],
                                          enable_test_device=node_sample["enable_test_device"],
                                          spdk_debug=node_sample["spdk_debug"])
            sleep_n_sec(60)
            new_nodes_ids_temp = self.sbcli_utils.get_all_node_without_lvols()
            for node_id in new_nodes_ids_temp:
                if node_id not in new_nodes_id:
                    new_nodes_id.append(node_id)
            self.storage_nodes.append(ip)
            containers = self.ssh_obj.get_running_containers(node_ip=ip)
            self.container_nodes[ip] = containers

            cluster_details = self.sbcli_utils.wait_for_cluster_status(
                cluster_id=self.cluster_id,
                status="in_expansion",
                timeout=300
                )

        for node in self.storage_nodes:
            self.ssh_obj.restart_docker_logging(
                node_ip=node,
                containers=self.container_nodes[node],
                log_dir=self.docker_logs_path,
                test_name=self.test_name
            )

        # Step 4: Resume cluster
        sleep_n_sec(60)
        self.logger.info("Expanding the cluster")
        self.ssh_obj.expand_cluster(self.mgmt_nodes[0], cluster_id=self.cluster_id)

        for node in new_nodes_id:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node,
                status="online",
                timeout=300
            )

        sleep_n_sec(120)

        self.validate_migration_for_node(timestamp, 2000, None, 60, no_task_ok=False)
        sleep_n_sec(30)

        cluster_details = self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id,
            status="active",
            timeout=300
            )
        self.logger.info(f"Completed cluster expansion for cluster id: {self.cluster_id} and Cluster status is {cluster_details['status']}")

        # Step 5: Create lvols on new nodes and validate
        for node in new_nodes_id:
            lvol_name = f"lvl_{generate_random_sequence(4)}_nn"
            mount_path = f"{self.mount_base}/{lvol_name}"
            log_path = f"{self.log_base}/{lvol_name}.log"

            self.sbcli_utils.add_lvol(lvol_name, self.pool_name, size="10G",
                                      host_id=node)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], connect_str)

            device = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0], lvol_id=self.sbcli_utils.get_lvol_id(lvol_name))
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

            sleep_n_sec(10)
            
            snapshot_name = f"snap_{lvol_name}"
            self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_details[lvol_name]["ID"], snapshot_name)

            snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name=snapshot_name)

            sleep_n_sec(10)

            clone_name = f"clone_{lvol_name}"

            self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)

            clone_id = self.sbcli_utils.get_lvol_id(clone_name)

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], connect_str)

            cl_mount_path = f"{self.mount_base}/{clone_name}"
            cl_log_path = f"{self.log_base}/{clone_name}.log"

            lvol_details[lvol_name]["Clone"]["ID"] = clone_id
            lvol_details[lvol_name]["Clone"]["Snapshot"] = snapshot_name
            lvol_details[lvol_name]["Clone"]["Log"] = cl_mount_path
            lvol_details[lvol_name]["Clone"]["Mount"] = cl_log_path

            device = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0], lvol_id=clone_id)
            self.ssh_obj.format_disk(self.mgmt_nodes[0], device)
            self.ssh_obj.mount_path(self.mgmt_nodes[0], device, cl_mount_path)

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.mgmt_nodes[0], None, cl_mount_path, cl_log_path),
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

        self.common_utils.manage_fio_threads(
            node=self.mgmt_nodes[0],
            threads=fio_threads,
            timeout=2000
        )
        sleep_n_sec(60)


        for lvol_name, lvol_detail in lvol_details.items():
            self.logger.info(f"Checking fio log for lvol and clone for {lvol_name}")
            self.common_utils.validate_fio_test(node=self.mgmt_nodes[0], log_file=lvol_detail["Log"])
            self.common_utils.validate_fio_test(node=self.mgmt_nodes[0], log_file=lvol_detail["Clone"]["Log"])

        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            assert node["status"] == "online", f"{node['id']} is not online"
            assert node["health_check"], f"{node['id']} health check failed"

        self.logger.info("TEST CASE PASSED !!!")


class TestAddK8sNodesDuringFioRun(TestClusterBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.new_nodes = kwargs.get("new_nodes")  # List of new worker node IPs
        self.k3s_mnode = kwargs.get("k3s_mnode")
        self.storage_pool_name = self.pool_name # Taking from base class
        self.mount_base = "/mnt/"
        self.log_base = f"{Path.home()}/"
        self.namespace = kwargs.get("namespace", None)
        self.test_name = "add_nodes_during_fio_k8s"
        self.logger.info(f"New Nodes to Add: {self.new_nodes}")

    def get_namespace(self):
        """Retrieves the namespace using the specified logic."""
        namespace = None
        node_ip = self.storage_nodes[0]  # Use the first storage node

        # 1. Namespace provided as input
        if self.namespace and len(self.namespace) > 0:
            self.logger.info(f"Namespace provided as input: {namespace}")

        # 2. Check /etc/simplyblock/namespace on an existing node
        command = "cat /var/simplyblock/namespace 2>/dev/null"  # Suppress errors if file doesn't exist
        try:
            namespace = self.ssh_obj.exec_command(node_ip, command).strip()
            if namespace:
                self.logger.info(f"Namespace found in /var/simplyblock/namespace: {namespace}")
                return namespace
        except Exception as e:
            self.logger.debug(f"Error reading namespace from file: {e}")

        # 3. Default to 'simplyblk' with a warning
        self.logger.warning("Namespace not found in file or input flag. Defaulting to 'simplyblk'")
        return "simplyblk"

    def _prepare_worker_node(self, node_ip):
        """Prepares a worker node by installing necessary packages and configuring kernel parameters."""
        commands = [
            "sudo rm -f /usr/local/bin/kubectl || true",
            "sudo rm -f /usr/bin/kubectl || true",
            "sudo yum remove -y kubectl || true",
            "sudo yum install -y fio nvme-cli bc",
            "sudo modprobe nvme-tcp",
            "sudo modprobe nbd",
            "total_memory_kb=$(grep MemTotal /proc/meminfo | awk '{print $2}')",
            "total_memory_mb=$((total_memory_kb / 1024))",
            "hugepages=$(echo \"$total_memory_mb * 0.3 / 1\" | bc)",
            "sudo sysctl -w vm.nr_hugepages=$hugepages",
            "sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1",
            "sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1",
            "sudo systemctl disable nm-cloud-setup.service nm-cloud-setup.timer",
            "sudo /usr/local/bin/k3s kubectl get node",
            "sudo yum install -y pciutils",
            "lspci",
            "sudo yum install -y make golang",
            "echo 'nvme-tcp' | sudo tee /etc/modules-load.d/nvme-tcp.conf",
            "echo 'nbd' | sudo tee /etc/modules-load.d/nbd.conf",
            "echo \"vm.nr_hugepages=$hugepages\" | sudo tee /etc/sysctl.d/hugepages.conf",
            "sudo sysctl --system"
        ]
        for command in commands:
            self.logger.info(f"Executing command on {node_ip}: {command}")
            self.ssh_obj.exec_command(node_ip, command)

    def _add_node_to_cluster(self, node_ip):
        """Adds a worker node to the k3s cluster."""
        # 1. Get the token from the k3s master node
        token, _ = self.ssh_obj.exec_command(self.k3s_mnode, "sudo cat /var/lib/rancher/k3s/server/node-token")

        token = token.strip()

        # 2. Install k3s and join the cluster
        k3s_install_cmd = f"curl -sfL https://get.k3s.io | K3S_URL=https://{self.k3s_mnode}:6443 K3S_TOKEN={token} bash -"
        self.logger.info(f"Installing k3s on {node_ip} and joining cluster")
        self.ssh_obj.exec_command(node_ip, k3s_install_cmd)

        self.logger.info(f"Waiting for kubectl to be ready on {node_ip}")
        sleep_n_sec(30)

        node_name_cmd = "kubectl get nodes -o wide | grep -w %s | awk '{print $1}'" % node_ip
        self.logger.info(f"Getting node name to label {node_ip}.")
        name, _ = self.ssh_obj.exec_command(self.k3s_mnode, node_name_cmd)

        name = name.strip()
        
        # 3. Add label to the node
        kubectl_label_cmd = f"kubectl label node {name} type=simplyblock-storage-plane"
        self.logger.info(f"Adding label to node {node_ip}, name: {name}")
        self.ssh_obj.exec_command(self.k3s_mnode, kubectl_label_cmd)

    def _add_node_sbcli(self, node_ip):
          """Adds the node to the SimplyBlock cluster using sbcli."""
          node_sample = self.sbcli_utils.get_storage_nodes()["results"][0]
          self.ssh_obj.add_storage_node(self.mgmt_nodes[0], self.cluster_id, node_ip,
                                        spdk_image=node_sample["spdk_image"],
                                        partitions=node_sample["num_partitions_per_dev"],
                                        disable_ha_jm= not node_sample["enable_ha_jm"],
                                        enable_test_device=node_sample["enable_test_device"],
                                        spdk_debug=node_sample["spdk_debug"],
                                        namespace=self.namespace,
                                        data_nic=None)
          sleep_n_sec(180)

    def run(self):
        self.logger.info("Starting Test: Add Nodes During FIO Run (Kubernetes)")
        self.namespace = self.get_namespace() # Getting namespace
        self.mgmt_node = self.mgmt_nodes[0]

        # Step 1: Create lvol on existing nodes
        fio_threads = []
        lvol_details = {}
        self.sbcli_utils.add_storage_pool(self.storage_pool_name)
        sleep_n_sec(10)
        for i, _ in enumerate(self.storage_nodes):
            lvol_name = f"lvl_{generate_random_sequence(4)}{i}"
            mount_path = f"{self.mount_base}/{lvol_name}"
            log_path = f"{self.log_base}/{lvol_name}.log"

            node_uuid = self.sbcli_utils.get_node_without_lvols()

            self.sbcli_utils.add_lvol(lvol_name, self.storage_pool_name, size="10G",
                                      host_id=node_uuid)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], connect_str)

            device = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0], lvol_id=self.sbcli_utils.get_lvol_id(lvol_name))
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

            sleep_n_sec(10)
            
            snapshot_name = f"snap_{lvol_name}"
            self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_details[lvol_name]["ID"], snapshot_name)

            snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name=snapshot_name)

            sleep_n_sec(10)

            clone_name = f"clone_{lvol_name}"

            self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)

            clone_id = self.sbcli_utils.get_lvol_id(clone_name)

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], connect_str)

            cl_mount_path = f"{self.mount_base}/{clone_name}"
            cl_log_path = f"{self.log_base}/{clone_name}.log"

            lvol_details[lvol_name]["Clone"]["ID"] = clone_id
            lvol_details[lvol_name]["Clone"]["Snapshot"] = snapshot_name
            lvol_details[lvol_name]["Clone"]["Log"] = cl_mount_path
            lvol_details[lvol_name]["Clone"]["Mount"] = cl_log_path

            device = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0], lvol_id=clone_id)
            self.ssh_obj.format_disk(self.mgmt_nodes[0], device)
            self.ssh_obj.mount_path(self.mgmt_nodes[0], device, cl_mount_path)

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.mgmt_nodes[0], None, cl_mount_path, cl_log_path),
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

            sleep_n_sec(30)


        sleep_n_sec(30)

        # Step 2: Add new nodes
        self.logger.info("Adding new nodes")
        new_nodes_id = []
        timestamp = int(datetime.now().timestamp())
        cluster_details = None

        for ip in self.new_nodes:
            self.logger.info(f"Preparing worker node: {ip}")
            self._prepare_worker_node(ip)
            self.logger.info(f"Adding node {ip} to k3s cluster")
            self._add_node_to_cluster(ip)
            sleep_n_sec(30)
            self.logger.info(f"Adding node {ip} to SimplyBlock cluster")
            # self._add_node_sbcli(ip)
            sleep_n_sec(180)
            new_nodes_ids_temp = self.sbcli_utils.get_all_node_without_lvols()
            for node_id in new_nodes_ids_temp:
                if node_id not in new_nodes_id:
                    new_nodes_id.append(node_id)
            self.storage_nodes.append(ip)

            cluster_details = self.sbcli_utils.wait_for_cluster_status(
                cluster_id=self.cluster_id,
                status="in_expansion",
                timeout=300
                )

        self.runner_k8s_log.restart_logging()

        # Step 3: Resume cluster
        sleep_n_sec(300)
        for node in new_nodes_id:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node,
                status="online",
                timeout=300
            )
        
        self.logger.info("Expanding the cluster")
        self.ssh_obj.expand_cluster(self.mgmt_nodes[0], cluster_id=self.cluster_id)

        for node in new_nodes_id:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node,
                status="online",
                timeout=300
            )

        sleep_n_sec(120)

        self.validate_migration_for_node(timestamp, 2000, None, 60, no_task_ok=False)
        sleep_n_sec(30)

        cluster_details = self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id,
            status="active",
            timeout=300
            )
        self.logger.info(f"Completed cluster expansion for cluster id: {self.cluster_id} and Cluster status is {cluster_details['status']}")

        # Step 4: Create lvols on new nodes and validate
        for node in new_nodes_id:
            lvol_name = f"lvl_{generate_random_sequence(4)}_nn"
            mount_path = f"{self.mount_base}/{lvol_name}"
            log_path = f"{self.log_base}/{lvol_name}.log"

            self.sbcli_utils.add_lvol(lvol_name, self.pool_name, size="10G",
                                      host_id=node)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], connect_str)

            device = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0], lvol_id=self.sbcli_utils.get_lvol_id(lvol_name))
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

            sleep_n_sec(10)
            
            snapshot_name = f"snap_{lvol_name}"
            self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_details[lvol_name]["ID"], snapshot_name)

            snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name=snapshot_name)

            sleep_n_sec(10)

            clone_name = f"clone_{lvol_name}"

            self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)

            clone_id = self.sbcli_utils.get_lvol_id(clone_name)

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(self.mgmt_nodes[0], connect_str)

            cl_mount_path = f"{self.mount_base}/{clone_name}"
            cl_log_path = f"{self.log_base}/{clone_name}.log"

            lvol_details[lvol_name]["Clone"]["ID"] = clone_id
            lvol_details[lvol_name]["Clone"]["Snapshot"] = snapshot_name
            lvol_details[lvol_name]["Clone"]["Log"] = cl_mount_path
            lvol_details[lvol_name]["Clone"]["Mount"] = cl_log_path

            device = self.ssh_obj.get_lvol_vs_device(node=self.mgmt_nodes[0], lvol_id=clone_id)
            self.ssh_obj.format_disk(self.mgmt_nodes[0], device)
            self.ssh_obj.mount_path(self.mgmt_nodes[0], device, cl_mount_path)

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.mgmt_nodes[0], None, cl_mount_path, cl_log_path),
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

        self.common_utils.manage_fio_threads(
            node=self.mgmt_nodes[0],
            threads=fio_threads,
            timeout=2000
        )
        sleep_n_sec(60)


        for lvol_name, lvol_detail in lvol_details.items():
            self.logger.info(f"Checking fio log for lvol and clone for {lvol_name}")
            self.common_utils.validate_fio_test(node=self.mgmt_nodes[0], log_file=lvol_detail["Log"])
            self.common_utils.validate_fio_test(node=self.mgmt_nodes[0], log_file=lvol_detail["Clone"]["Log"])

        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            assert node["status"] == "online", f"{node['id']} is not online"
            assert node["health_check"], f"{node['id']} health check failed"

        self.logger.info("TEST CASE PASSED !!!")