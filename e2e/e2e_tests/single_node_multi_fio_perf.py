### simplyblock e2e tests
import json
import os
import re
import threading
import time
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


class TestSingleNodeMultipleFioPerfValidation:
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
        self.lvol_name1 = "test_lvol1"
        self.lvol_name2 = "test_lvol2"
        self.mount_path1 = "/home/ec2-user/test_location1"
        self.mount_path2 = "/home/ec2-user/test_location2"
        self.log_path1 = f"{os.path.dirname(self.mount_path1)}/log_file1.json"
        self.log_path2 = f"{os.path.dirname(self.mount_path2)}/log_file2.json"
        self.base_cmd = None
        self.lvol_devices = {self.lvol_name1: {"Device": None, 
                                               "Path": self.mount_path1, 
                                               "Log": self.log_path1},
                             self.lvol_name2: {"Device": None, 
                                               "Path": self.mount_path2, 
                                               "Log": self.log_path2}}
        self.fio_debug = kwargs.get("fio_debug", False)
        self.base_cmd = os.environ.get("SBCLI_CMD", "sbcli-dev")

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
                                  device=self.mount_path1)
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=self.mount_path2)
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()
        
    def run(self):
        """ Performs each step of the testcase
        """
        self.logger.info("Inside run function")
        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        pools = self.sbcli_utils.list_storage_pools()
        assert self.pool_name not in list(pools.keys()), \
            f"Pool {self.pool_name} present in list of pools post delete: {pools}"
        
        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name,
            cluster_id=cluster_id,
            max_rw_iops=30000,
            max_r_mbytes=100,
            max_w_mbytes=100
        )

        pools = self.sbcli_utils.list_storage_pools()
        assert self.pool_name in list(pools.keys()), \
            f"Pool {self.pool_name} not present in list of pools post add: {pools}"
        
        self.sbcli_utils.add_lvol(
            lvol_name=self.lvol_name1,
            pool_name=self.pool_name,
            size="10G",
            # distr_ndcs=1,
            # distr_npcs=1,
            # distr_bs=4096,
            # distr_chunk_bs=4096,
            max_rw_iops=600,
            max_r_mbytes=5,
            max_w_mbytes=5
        )

        self.sbcli_utils.add_lvol(
            lvol_name=self.lvol_name2,
            pool_name=self.pool_name,
            size="10G",
            # distr_ndcs=1,
            # distr_npcs=1,
            # distr_bs=4096,
            # distr_chunk_bs=4096,
            max_rw_iops=600,
            max_r_mbytes=5,
            max_w_mbytes=5
        )

        lvols = self.sbcli_utils.list_lvols()
        assert self.lvol_name1 in list(lvols.keys()), \
            f"Lvol {self.lvol_name1} present in list of lvols post add: {lvols}"

        assert self.lvol_name2 in list(lvols.keys()), \
            f"Lvol {self.lvol_name2} present in list of lvols post add: {lvols}"
        
        connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=self.lvol_name1)
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
        self.lvol_devices[self.lvol_name1]["Device"] = disk_use

        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=self.lvol_name2)
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
        self.lvol_devices[self.lvol_name2]["Device"] = disk_use

        sleep_n_sec(5)
        for lvol, data in self.lvol_devices.items():
            self.logger.info(f"Setting device and Running FIO for lvol: {lvol}, Data: {data}")
            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                      device=data["Device"])
            sleep_n_sec(2)
            self.ssh_obj.format_disk(node=self.mgmt_nodes[0],
                                     device=data["Device"])
            sleep_n_sec(2)
            self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                    device=data["Device"],
                                    mount_path=data["Path"])
            sleep_n_sec(2)
        
        fio_thread1 = threading.Thread(target=self.ssh_obj.run_fio_test, args=(self.mgmt_nodes[0], None, self.lvol_devices[self.lvol_name1]["Path"], None,),
                                   kwargs={"name": "fio_run_1",
                                           "readwrite": "randrw",
                                           "ioengine": "libaio",
                                           "iodepth": 64,
                                           "bs": 4096,
                                           "rwmixread": 55,
                                           "size": "2G",
                                           "time_based": True,
                                           "runtime": 300,
                                           "output_format": "json",
                                           "output_file": self.lvol_devices[self.lvol_name1]["Log"],
                                           "debug": self.fio_debug})

        fio_thread2 = threading.Thread(target=self.ssh_obj.run_fio_test, args=(self.mgmt_nodes[0], None, self.lvol_devices[self.lvol_name2]["Path"], None,),
                                    kwargs={"name": "fio_run_2",
                                            "readwrite": "randrw",
                                            "ioengine": "libaio",
                                            "iodepth": 64,
                                            "bs": 4096,
                                            "rwmixread": 55,
                                            "size": "2G",
                                            "time_based": True,
                                            "runtime": 300,
                                            "output_format": "json",
                                            "output_file": self.lvol_devices[self.lvol_name2]["Log"],
                                            "debug": self.fio_debug})
            
                                      
        fio_thread1.start()
        fio_thread2.start()
        
        sleep_n_sec(5)

        process_list_before = self.ssh_obj.find_process_name(node=self.mgmt_nodes[0],
                                                             process_name="fio")
        self.logger.info(f"Process List: {process_list_before}")
        sleep_n_sec(60)

        process_list_after = self.ssh_obj.find_process_name(node=self.mgmt_nodes[0],
                                                            process_name="fio")
        self.logger.info(f"Process List: {process_list_after}")

        process_list_before = process_list_before[0:len(process_list_before)-2]
        process_list_after = process_list_before[0:len(process_list_after)-2]
        
        assert process_list_after == process_list_before, \
            f"FIO process list changed - Before Sleep: {process_list_before}, After Sleep: {process_list_after}"
        
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                             threads=[fio_thread1, fio_thread2],
                                             timeout=900)

        out1 = self.ssh_obj.read_file(node=self.mgmt_nodes[0],
                                      file_name=self.log_path1)
        out2 = self.ssh_obj.read_file(node=self.mgmt_nodes[0],
                                      file_name=self.log_path2)
        out1 = json.loads(out1)
        out2 = json.loads(out2)
        
        self.logger.info(f"Log file 1 {self.log_path1}: \n {out1}")
        self.logger.info(f"Log file 2 {self.log_path2}: \n {out2}")
        
        self.common_utils.validate_fio_json_output(out1)
        self.common_utils.validate_fio_json_output(out2)

        self.logger.info("TEST CASE PASSED !!!")

    def teardown(self):
        """Contains teradown required post test case execution
        """
        nqn_devices = []
        self.logger.info("Inside teardown function")
        for lvol in list(self.lvol_devices.keys()):
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol)
            lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
            nqn_devices.append(lvol_details[0]["nqn"])
            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                      device=self.lvol_devices[lvol]["Device"])
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()
        for nqn in nqn_devices:
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                      command=f"sudo nvme disconnect -n {nqn}")
        for node, ssh in self.ssh_obj.ssh_connections.items():
            self.logger.info(f"Closing node ssh connection for {node}")
            ssh.close()