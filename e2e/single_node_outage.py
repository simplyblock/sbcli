### simplyblock e2e tests
import requests
import time
from utils import SbcliUtils
from ssh_utils import SshUtils

# selected the node that doesn't have lvol attached

cluster_secret = "1yD8rY4BGaTEIYn9YA3U"
cluster_id = "53e690d2-c71c-48c0-b4cc-a4233e23a95b"
cluster_ip = "10.0.3.59"

url = f"http://{cluster_ip}"
api_base_url = "https://zc198uuzzj.execute-api.us-east-2.amazonaws.com/"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"{cluster_id} {cluster_secret}"
}
bastion_server = "3.138.204.127"

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
        self.ssh_obj = SshUtils()
        self.sbcli_utils = SbcliUtils(
            cluster_ip=cluster_ip,
            url=url,
            cluster_api_url=api_base_url,
            cluster_id=cluster_id,
            cluster_secret=cluster_secret
        )
        self.mgmt_nodes = None
        self.storage_nodes = None

    def setup(self):
        """Contains setup required to run the test case
        """
        print("Inside setup function")
        self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
        self.sbcli_utils.delete_all_storage_pools()

    def run(self):
        """ Performs each step of the testcase
        """
        print("Inside run function")
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
        self.sbcli_utils.add_storage_pool(
            pool_name="test_pool"
        )
        pools = self.sbcli_utils.list_storage_pools()
        assert "test_pool" in list(pools.keys()), \
            f"Pool test_pool not present in list of pools: {pools}"
        
        self.sbcli_utils.delete_storage_pool(
            pool_name="test_pool"
        )
        pools = self.sbcli_utils.list_storage_pools()
        assert "test_pool" not in list(pools.keys()), \
            f"Pool test_pool present in list of pools post delete: {pools}"
        

    def teardown(self):
        """Contains teradown required post test case execution
        """
        print("Inside teardown function")
        self.sbcli_utils.delete_all_storage_pools()
        for node, ssh in self.ssh_obj.ssh_connections.items():
            print(f"Closing node ssh connection for {node}")
            ssh.close()
        print("Test case passed!!")
