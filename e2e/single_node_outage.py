### simplyblock e2e tests
import os
import requests
import time
from utils import SbcliUtils
from ssh_utils import SshUtils

# selected the node that doesn't have lvol attached

cluster_secret = "ajNau5afZ6z6sSAioQlP"
cluster_id = "f8b32ee7-2bd1-4c63-b287-9a4b42b880e3"
cluster_ip = "10.0.3.151"

url = f"http://{cluster_ip}"
api_base_url = "https://ur6wajztxe.execute-api.us-east-2.amazonaws.com/"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"{cluster_id} {cluster_secret}"
}
bastion_server = "18.218.46.26"

class TestSingleNodeOutage:
    """
    Steps:
    1. Start FIO tests
    2. While FIO is running, validate this scenario:
        a. In a cluster with three nodes, select one node, which does not 
           have any lvol attached.
        b. Suspend the Node via API or CLI while the fio test is running. 
        c. Shutdown the Node via API or CLI while the fio test is running. 
        d. Check status of objects during outage: 
            - the node is in status “offline”
            - the cluster is in status “degraded”
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

    3. Restart the node again.
        a. check the status again: 
            - the status of all nodes is “online”
            - the cluster is in status “active”
            - all devices in the cluster are in status “online”
            - the event log contains the records indicating the object status changes
            - select a cluster map from any of the two lvols (lvol get-cluster-map)
              and verify that all nodes and all devices appear online

        b. check that fio remains running without interruption.
    """

    def __init__(self):
        self.ssh_obj = SshUtils(bastion_server=bastion_server)
        self.sbcli_utils = SbcliUtils(
            cluster_ip=cluster_ip,
            url=url,
            cluster_api_url=api_base_url,
            cluster_id=cluster_id,
            cluster_secret=cluster_secret
        )
        self.mgmt_nodes = None
        self.storage_nodes = None
        self.pool_name = "test_pool"
        self.lvol_name = "test_lvol"
        self.mount_path = "/home/ec2-user/test_location"
        self.log_path = f"{os.path.dirname(self.mount_path)}/log_file.log"

    def setup(self):
        """Contains setup required to run the test case
        """
        print("Inside setup function")
        self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
        for node in self.mgmt_nodes:
            print(f"**Connecting to management nodes** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=bastion_server,
            )
        for node in self.storage_nodes:
            print(f"**Connecting to storage nodes** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=bastion_server,
            )
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=self.mount_path)
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()

    def run(self):
        """ Performs each step of the testcase
        """
        print("Inside run function")
        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
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
            pool_name=self.pool_name
        )
        
        lvol = self.sbcli_utils.list_lvols()
        self.sbcli_utils.add_lvol(
            lvol_name=self.lvol_name,
            pool_name=self.pool_name
        )
        lvols = self.sbcli_utils.list_lvols()
        assert self.lvol_name in list(lvols.keys()), \
            f"Lvol {self.lvol_name} present in list of lvols post add: {lvols}"

        lvol_details = self.sbcli_utils.get_lvol_by_id(lvol_id=lvols[self.lvol_name])
        lvol_hostname = lvol_details["results"][0]["hostname"]
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        storage_hostnames = []
        for node in storage_nodes["results"]:
            storage_hostnames.append(node["hostname"])

        print(f"Storage hostname: {storage_hostnames}")
        print(f"LVOL hostname: {lvol_hostname}")
        
        connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=self.lvol_name)

        self.ssh_obj.exec_command(node=self.mgmt_nodes[0], 
                                  command=connect_str)

        final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        disk_use = None
        print("Initial vs final disk:")
        print(f"Initial: {initial_devices}")
        print(f"Final: {final_devices}")
        for device in final_devices:
            if device not in initial_devices:
                print(f"Using disk: /dev/{device.strip()}")
                disk_use = f"/dev/{device.strip()}"
                break
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=disk_use)
        self.ssh_obj.format_disk(node=self.mgmt_nodes[0],
                                 device=disk_use)
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                device=disk_use,
                                mount_path=self.mount_path)
        
        self.ssh_obj.run_fio_test(node=self.mgmt_nodes[0],
                                  directory=self.mount_path,
                                  log_file=self.log_path)
        print("Test case passed!!")

    def teardown(self):
        """Contains teradown required post test case execution
        """
        print("Inside teardown function")
        # self.sbcli_utils.delete_all_lvols()
        # self.sbcli_utils.delete_all_storage_pools()
        for node, ssh in self.ssh_obj.ssh_connections.items():
            print(f"Closing node ssh connection for {node}")
            ssh.close()
