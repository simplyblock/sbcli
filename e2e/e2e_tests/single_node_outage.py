### simplyblock e2e tests
import os
import time
import threading
from utils.common_utils import sleep_n_sec
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from utils.common_utils import CommonUtils
from logger_config import setup_logger


cluster_secret = os.environ.get("CLUSTER_SECRET")
cluster_id = os.environ.get("CLUSTER_ID")

api_base_url = os.environ.get("API_BASE_URL")
headers = {
    "Content-Type": "application/json",
    "Authorization": f"{cluster_id} {cluster_secret}"
}
bastion_server = os.environ.get("BASTION_SERVER")


class TestSingleNodeOutage:
    """
    Steps:
    1. Create Storage Pool and Delete Storage pool
    2. Create storage pool
    3. Create LVOL
    4. Connect LVOL
    5. Mount Device
    6. Start FIO tests
    7. While FIO is running, validate this scenario:
        a. In a cluster with three nodes, select one node, which does not
           have any lvol attached.
        b. Suspend the Node via API or CLI while the fio test is running.
        c. Shutdown the Node via API or CLI while the fio test is running.
        d. Check status of objects during outage:
            - the node is in status “offline”
            - the devices of the node are in status “unavailable”
            - lvols remain in “online” state
            - the event log contains the records indicating the object status
              changes; the event log also contains records indicating read and
              write IO errors.
            - select a cluster map from any of the two lvols (lvol get-cluster-map)
              and verify that the status changes of the node and devices are reflected in
              the other cluster map. Other two nodes and 4 devices remain online.
            - health-check status of all nodes and devices is “true”
        e. check that fio remains running without interruption.

    8. Restart the node again.
        a. check the status again:
            - the status of all nodes is “online”
            - all devices in the cluster are in status “online”
            - the event log contains the records indicating the object status changes
            - select a cluster map from any of the two lvols (lvol get-cluster-map)
              and verify that all nodes and all devices appear online
        b. check that fio remains running without interruption.
    """

    def __init__(self, **kwargs):
        self.ssh_obj = SshUtils(bastion_server=bastion_server)
        self.logger = setup_logger(__name__)
        self.sbcli_utils = SbcliUtils(
            cluster_api_url=api_base_url,
            cluster_id=cluster_id,
            cluster_secret=cluster_secret
        )
        self.common_utils = CommonUtils(self.sbcli_utils, self.ssh_obj)
        self.mgmt_nodes = None
        self.storage_nodes = None
        self.pool_name = "test_pool"
        self.lvol_name = "test_lvol"
        self.mount_path = "/home/ec2-user/test_location"
        self.log_path = f"{os.path.dirname(self.mount_path)}/log_file.log"
        self.base_cmd = None
        self.fio_debug = kwargs.get("fio_debug", False)

    def setup(self):
        """Contains setup required to run the test case
        """
        self.logger.info("Inside setup function")
        self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
        for node in self.mgmt_nodes:
            self.logger.info(f"**Connecting to management nodes** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=bastion_server,
            )
        for node in self.storage_nodes:
            self.logger.info(f"**Connecting to storage nodes** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=bastion_server,
            )
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=self.mount_path)
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()
        expected_base = ["sbcli", "sbcli-dev", "sbcli-release"]
        for base in expected_base:
            output, error = self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                                      command=base)
            if len(output.strip()):
                self.base_cmd = base
                self.logger.info(f"Using base command as {self.base_cmd}")

    def run(self):
        """ Performs each step of the testcase
        """
        self.logger.info("Inside run function")
        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name,
            cluster_id=cluster_id
        )
        pools = self.sbcli_utils.list_storage_pools()
        assert self.pool_name in list(pools.keys()), \
            f"Pool {self.pool_name} not present in list of pools: {pools}"

        self.sbcli_utils.delete_storage_pool(
            pool_name=self.pool_name
        )
        pools = self.sbcli_utils.list_storage_pools()
        assert self.pool_name not in list(pools.keys()), \
            f"Pool {self.pool_name} present in list of pools post delete: {pools}"

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name,
            cluster_id=cluster_id
        )

        self.sbcli_utils.add_lvol(
            lvol_name=self.lvol_name,
            pool_name=self.pool_name,
            size="800M"
        )
        lvols = self.sbcli_utils.list_lvols()
        assert self.lvol_name in list(lvols.keys()), \
            f"Lvol {self.lvol_name} present in list of lvols post add: {lvols}"

        connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=self.lvol_name)

        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=connect_str)

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
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=disk_use)
        self.ssh_obj.format_disk(node=self.mgmt_nodes[0],
                                 device=disk_use)
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                device=disk_use,
                                mount_path=self.mount_path)

        fio_thread1 = threading.Thread(target=self.ssh_obj.run_fio_test, args=(self.mgmt_nodes[0], None, self.mount_path, self.log_path,),
                                       kwargs={"name": "fio_run_1",
                                               "runtime": 150,
                                               "debug": self.fio_debug})
        fio_thread1.start()

        no_lvol_node_uuid = self.sbcli_utils.get_node_without_lvols()

        self.logger.info("Getting lvol status before shutdown")
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=self.lvol_name)
        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        for lvol in lvol_details:
            self.logger.info(f"LVOL STATUS: {lvol['status']}")
            assert lvol["status"] == "online", \
                f"Lvol {lvol['id']} is not in online state. {lvol['status']}"

        self.validations(node_uuid=no_lvol_node_uuid,
                         node_status="online",
                         device_status="online",
                         lvol_status="online",
                         health_check_status=True
                         )

        self.sbcli_utils.suspend_node(node_uuid=no_lvol_node_uuid)
        self.sbcli_utils.shutdown_node(node_uuid=no_lvol_node_uuid)

        self.logger.info("Sleeping for 10 seconds")
        sleep_n_sec(10)

        self.validations(node_uuid=no_lvol_node_uuid,
                         node_status="offline",
                         device_status="unavailable",
                         lvol_status="online",
                         health_check_status=True
                         )

        self.sbcli_utils.restart_node(node_uuid=no_lvol_node_uuid)

        self.logger.info("Sleeping for 10 seconds")
        sleep_n_sec(10)

        self.validations(node_uuid=no_lvol_node_uuid,
                         node_status="online",
                         device_status="online",
                         lvol_status="online",
                         health_check_status=True
                         )

        # Write steps in order
        steps = {
            "Storage Node": ["suspended", "shutdown", "restart"],
            "Device": {"restart"}
        }
        self.common_utils.validate_event_logs(cluster_id=cluster_id,
                                              operations=steps)
        
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                             threads=[fio_thread1],
                                             timeout=300)

        self.common_utils.validate_fio_test(node=self.mgmt_nodes[0],
                                            log_file=self.log_path)

        self.logger.info("TEST CASE PASSED !!!")

    def validations(self, node_uuid, node_status, device_status, lvol_status,
                    health_check_status):
        """Validates node, devices, lvol status with expected status

        Args:
            node_uuid (str): UUID of node to validate
            node_status (str): Expected node status
            device_status (str): Expected device status
            lvol_status (str): Expected lvol status
            health_check_status (bool): Expected health check status
        """
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=node_uuid)
        device_details = self.sbcli_utils.get_device_details(storage_node_id=node_uuid)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=self.lvol_name)
        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        command = f"{self.base_cmd} lvol get-cluster-map {lvol_id}"
        lvol_cluster_map_details, _ = self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                                                    command=command)
        self.logger.info(f"LVOL Cluster map: {lvol_cluster_map_details}")
        cluster_map_nodes, cluster_map_devices = self.common_utils.parse_lvol_cluster_map_output(lvol_cluster_map_details)
        offline_device = None

        assert node_details[0]["status"] == node_status, \
            f"Node {node_uuid} is not in {node_status} state. {node_details[0]['status']}"
        for device in device_details:
            if "jm" in device["jm_bdev"]:
                assert device["status"] == "JM_DEV", \
                    f"JM Device {device['id']} is not in JM_DEV state. {device['status']}"
            else:
                assert device["status"] == device_status, \
                    f"Device {device['id']} is not in {device_status} state. {device['status']}"
                offline_device = device['id']

        for lvol in lvol_details:
            assert lvol["status"] == lvol_status, \
                f"Lvol {lvol['id']} is not in {lvol_status} state. {lvol['status']}"

        storage_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        for node in storage_nodes:
            assert node["health_check"] == health_check_status, \
                f"Node {node['id']} health-check is not {health_check_status}. {node['health_check']}"
            device_details = self.sbcli_utils.get_device_details(storage_node_id=node["id"])
            for device in device_details:
                assert device["health_check"] == health_check_status, \
                    f"Device {device['id']} health-check is not {health_check_status}. {device['health_check']}"

        for node_id, node in cluster_map_nodes.items():
            if node_id == node_uuid:
                assert node["Reported Status"] == node_status, \
                    f"Node {node_id} is not in {node_status} state. {node['Reported Status']}"
                assert node["Actual Status"] == node_status, \
                    f"Node {node_id} is not in {node_status} state. {node['Actual Status']}"
            else:
                assert node["Reported Status"] == "online", \
                    f"Node {node_uuid} is not in online state. {node['Reported Status']}"
                assert node["Actual Status"] == "online", \
                    f"Node {node_uuid} is not in online state. {node['Actual Status']}"

        for device_id, device in cluster_map_devices.items():
            if device_id == offline_device:
                assert device["Reported Status"] == device_status, \
                    f"Device {device_id} is not in {device_status} state. {device['Reported Status']}"
                assert device["Actual Status"] == device_status, \
                    f"Device {device_id} is not in {device_status} state. {device['Actual Status']}"
            else:
                assert device["Reported Status"] == "online", \
                    f"Device {device_id} is not in online state. {device['Reported Status']}"
                assert device["Actual Status"] == "online", \
                    f"Device {device_id} is not in online state. {device['Actual Status']}"

    def teardown(self):
        """Contains teradown required post test case execution
        """
        self.logger.info("Inside teardown function")
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=self.lvol_name)
        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        nqn = lvol_details[0]["nqn"]
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=self.mount_path)
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=f"sudo nvme disconnect -n {nqn}")
        for node, ssh in self.ssh_obj.ssh_connections.items():
            self.logger.info(f"Closing node ssh connection for {node}")
            ssh.close()
