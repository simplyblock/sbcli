### simplyblock e2e tests
import requests
import time
from utils import SbcliUtils
from ssh_utils import SshUtils

# selected the node that doesn't have lvol attached

cluster_secret = "YFtzNUJ2VF9FOk74uSLe"
cluster_uuid = "35c2443e-c317-4492-a2bc-608a66ab7de2"
cluster_ip = "10.0.3.233"

url = f"http://{cluster_ip}"
api_base_url = "https://61uxjimgrj.execute-api.us-east-2.amazonaws.com/"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"{cluster_uuid} {cluster_secret}"
}

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
    def test_single_node_outage(self):
        self.setup()
        self.run()
        self.teardown()

    def setup(self):
        """Contains setup required to run the test case
        """
        print("Inside setup function")
        self.ssh_obj = SshUtils()
        self.sbcli_utils = SbcliUtils(
            cluster_ip=cluster_ip,
            url=url,
            cluster_api_url=api_base_url,
            cluster_id=cluster_uuid,
            cluster_secret=cluster_secret
        )
        # mgmt_nodes, storage_nodes = self.get_all_nodes_ip(cluster_id=)

    def run(self):
        """ Performs each step of the testcase
        """
        print("Inside run function")
        self.ssh_obj.connect(
            address="10.0.3.233",
            bastion_server_address="3.147.205.106",
        )

    def teardown(self):
        """Contains teradown required post test case execution
        """
        print("Inside teardown function")
        for node, ssh in self.ssh_obj.ssh_connections.items():
            print(f"Closing node ssh connection for {node}")
            ssh.close()

if __name__ == "__main__":
    test = TestSingleNodeOutage()
    test.setup()
    test.run()
    test.teardown()