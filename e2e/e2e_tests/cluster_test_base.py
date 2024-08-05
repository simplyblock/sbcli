import os
import boto3
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from utils.common_utils import CommonUtils
from logger_config import setup_logger


class TestClusterBase:
    def __init__(self, **kwargs):
        self.cluster_secret = os.environ.get("CLUSTER_SECRET")
        self.cluster_id = os.environ.get("CLUSTER_ID")

        self.api_base_url = os.environ.get("API_BASE_URL")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"{self.cluster_id} {self.cluster_secret}"
        }
        self.bastion_server = os.environ.get("BASTION_SERVER")

        self.ssh_obj = SshUtils(bastion_server=self.bastion_server)
        self.logger = setup_logger(__name__)
        self.sbcli_utils = SbcliUtils(
            cluster_api_url=self.api_base_url,
            cluster_id=self.cluster_id,
            cluster_secret=self.cluster_secret
        )
        self.common_utils = CommonUtils(self.sbcli_utils, self.ssh_obj)
        self.mgmt_nodes = None
        self.storage_nodes = None
        self.pool_name = "test_pool"
        self.lvol_name = "test_lvol"
        self.mount_path = "/home/ec2-user/test_location"
        self.log_path = f"{os.path.dirname(self.mount_path)}/log_file.log"
        self.base_cmd = os.environ.get("SBCLI_CMD", "sbcli-dev")
        self.fio_debug = kwargs.get("fio_debug", False)
        self.ec2_client = None

    def setup(self):
        """Contains setup required to run the test case
        """
        self.logger.info("Inside setup function")
        self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
        for node in self.mgmt_nodes:
            self.logger.info(f"**Connecting to management nodes** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=self.bastion_server,
            )
        for node in self.storage_nodes:
            self.logger.info(f"**Connecting to storage nodes** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=self.bastion_server,
            )
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=self.mount_path)
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()
        session = boto3.Session(
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_REGION")
        )
        self.ec2_client = session.client('ec2')

    def teardown(self):
        """Contains teradown required post test case execution
        """
        self.logger.info("Inside teardown function")
        self.ssh_obj.kill_processes(node=self.mgmt_nodes[0],
                                    process_name="fio")
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=self.lvol_name)
        if lvol_id is not None:
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
        offline_device = []

        if isinstance(node_status, list):
            assert node_details[0]["status"] in node_status, \
                f"Node {node_uuid} is not in {node_status} state. {node_details[0]['status']}"
        else:
            assert node_details[0]["status"] == node_status, \
                f"Node {node_uuid} is not in {node_status} state. {node_details[0]['status']}"
        for device in device_details:
            # if "jm" in device["jm_bdev"]:
            #     assert device["status"] == "JM_DEV", \
            #         f"JM Device {device['id']} is not in JM_DEV state. {device['status']}"
            # else:
            assert device["status"] == device_status, \
                f"Device {device['id']} is not in {device_status} state. {device['status']}"
            offline_device.append(device['id'])

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
                if isinstance(node_status, list):
                    assert node["Reported Status"] in node_status, \
                    f"Node {node_id} is not in {node_status} reported state. {node['Reported Status']}"
                    assert node["Actual Status"] in node_status, \
                        f"Node {node_id} is not in {node_status} state. {node['Actual Status']}"
                else:
                    assert node["Reported Status"] == node_status, \
                    f"Node {node_id} is not in {node_status} reported state. {node['Reported Status']}"
                    assert node["Actual Status"] == node_status, \
                        f"Node {node_id} is not in {node_status} state. {node['Actual Status']}"
                    
            else:
                assert node["Reported Status"] == "online", \
                    f"Node {node_uuid} is not in online state. {node['Reported Status']}"
                assert node["Actual Status"] == "online", \
                    f"Node {node_uuid} is not in online state. {node['Actual Status']}"

        if device_status is not None:
            for device_id, device in cluster_map_devices.items():
                if device_id in offline_device:
                    assert device["Reported Status"] == device_status, \
                        f"Device {device_id} is not in {device_status} state. {device['Reported Status']}"
                    assert device["Actual Status"] == device_status, \
                        f"Device {device_id} is not in {device_status} state. {device['Actual Status']}"
                else:
                    assert device["Reported Status"] == "online", \
                        f"Device {device_id} is not in online state. {device['Reported Status']}"
                    assert device["Actual Status"] == "online", \
                        f"Device {device_id} is not in online state. {device['Actual Status']}"
