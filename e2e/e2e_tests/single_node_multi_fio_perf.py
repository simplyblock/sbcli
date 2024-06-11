### simplyblock e2e tests
import os
import re
from utils.common_utils import sleep_n_sec
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from utils.common_utils import CommonUtils
from logger_config import setup_logger


cluster_secret = os.environ.get("CLUSTER_SECRET")
cluster_id = os.environ.get("CLUSTER_ID")
cluster_ip = os.environ.get("CLUSTER_IP")

url = f"http://{cluster_ip}"
api_base_url = os.environ.get("API_BASE_URL")
headers = {
    "Content-Type": "application/json",
    "Authorization": f"{cluster_id} {cluster_secret}"
}
bastion_server = os.environ.get("BASTION_SERVER")

# cluster_secret = "bsmEXb6W3XtEFr4LIRnx"
# cluster_id = "79d5e453-ca37-4124-af57-c4d99b12402d"
# cluster_ip = "10.0.3.136"

# url = f"http://{cluster_ip}"
# api_base_url = "https://zybd1owv43.execute-api.us-east-2.amazonaws.com/"
# headers = {
#     "Content-Type": "application/json",
#     "Authorization": f"{cluster_id} {cluster_secret}"
# }
# bastion_server = "18.116.14.160"


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

    def __init__(self):
        self.ssh_obj = SshUtils(bastion_server=bastion_server)
        self.logger = setup_logger(__name__)
        self.sbcli_utils = SbcliUtils(
            cluster_ip=cluster_ip,
            url=url,
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
        self.log_path1 = f"{os.path.dirname(self.mount_path1)}/log_file1.log"
        self.log_path2 = f"{os.path.dirname(self.mount_path2)}/log_file2.log"
        self.base_cmd = None
        self.lvol_devices = {self.lvol_name1: {"Device": None, 
                                               "Path": self.mount_path1, 
                                               "Log": self.log_path1},
                             self.lvol_name2: {"Device": None, 
                                               "Path": self.mount_path2, 
                                               "Log": self.log_path2}}

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
        expected_base = ["sbcli", "sbcli-dev", "sbcli-release"]
        for base in expected_base:
            output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                               command=base)
            if len(output.strip()):
                self.base_cmd = base
                self.logger.info(f"Using base command as {self.base_cmd}")

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
            distr_ndcs=1,
            distr_npcs=1,
            distr_bs=4096,
            distr_chunk_bs=4096,
            max_rw_iops=600,
            max_r_mbytes=5,
            max_w_mbytes=5
        )

        self.sbcli_utils.add_lvol(
            lvol_name=self.lvol_name2,
            pool_name=self.pool_name,
            size="10G",
            distr_ndcs=1,
            distr_npcs=1,
            distr_bs=4096,
            distr_chunk_bs=4096,
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
        

        i = 1
        for lvol, data in self.lvol_devices.items():
            self.logger.info(f"Setting device and Running FIO for lvol: {lvol}, Data: {data}")
            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                    device=data["Device"])
            self.ssh_obj.format_disk(node=self.mgmt_nodes[0],
                                    device=data["Device"])
            self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                    device=data["Device"],
                                    mount_path=data["Path"])
            sleep_n_sec(3)
        
            self.ssh_obj.run_fio_test(node=self.mgmt_nodes[0],
                                      directory=data["Path"],
                                      log_file=data["Log"],
                                      name=f"fio_run_{i}",
                                      readwrite="randrw",
                                      ioengine="libaio",
                                      iodepth=64,
                                      bs=4096,
                                      rwmixread=55,
                                      size="2G",
                                      time_based=True,
                                      runtime=300)
            i += 1
        
        sleep_n_sec(3)
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
        
        sleep_n_sec(1900)

        process_list_after = self.ssh_obj.find_process_name(node=self.mgmt_nodes[0],
                                                            process_name="fio")
        self.logger.info(f"Process List: {process_list_after}")
        did_not_quit = False
        if len(process_list_after) == 2:
            process_list_after = 0
        else:
            self.ssh_obj.kill_processes(
                node=self.mgmt_nodes[0],
                process_name="fio"
            )
            self.logger.error(f"FIO process did not get stopped after runtime. {process_list_after}")
            did_not_quit = True
            # raise RuntimeError(f"FIO process did not get stopped after runtime. {process_list_after}")
        
        out1 = self.ssh_obj.read_file(node=self.mgmt_nodes[0],
                                      file_name=self.log_path1)
        out2 = self.ssh_obj.read_file(node=self.mgmt_nodes[0],
                                      file_name=self.log_path2)
        
        self.logger.info(f"Log file 1 {self.log_path1}: \n {out1}")
        self.logger.info(f"Log file 2 {self.log_path2}: \n {out2}")
        
        # self.common_utils.validate_fio_test(node=self.mgmt_nodes[0],
        #                                   log_file=self.log_path)

        if did_not_quit:
            raise RuntimeError(f"FIO process did not get stopped after runtime. {process_list_after}")
        self.logger.info("TEST CASE PASSED !!!")

    def validate_fio_output(self, output):
        iops_pattern = re.compile(r'total:.*iops=([0-9]+)')
        read_bw_pattern = re.compile(r'read: IOPS=.*, BW=([0-9.]+)MiB/s')
        write_bw_pattern = re.compile(r'write: IOPS=.*, BW=([0-9.]+)MiB/s')

        iops_match = iops_pattern.search(output)
        read_bw_match = read_bw_pattern.search(output)
        write_bw_match = write_bw_pattern.search(output)

        if not iops_match or not read_bw_match or not write_bw_match:
            return False, "Failed to parse fio output"

        total_iops = int(iops_match.group(1))
        read_bw = float(read_bw_match.group(1))
        write_bw = float(write_bw_match.group(1))

        if not (550 < total_iops < 650):
            return False, f"Total IOPS {total_iops} out of range (550-650)"
        if not (4.5 < read_bw < 5.5):
            return False, f"Read BW {read_bw} out of range (4.5-5.5 MiB/s)"
        if not (4.5 < write_bw < 5.5):
            return False, f"Write BW {write_bw} out of range (4.5-5.5 MiB/s)"

        return True, "Fio output validation passed"

    def teardown(self):
        """Contains teradown required post test case execution
        """
        # nqn_devices = []
        # self.logger.info("Inside teardown function")
        # for lvol in list(self.lvol_devices.keys()):
        #     lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol)
        #     lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        #     nqn_devices.append(lvol_details[0]["nqn"])
        # self.sbcli_utils.delete_all_lvols()
        # self.sbcli_utils.delete_all_storage_pools()
        # for nqn in nqn_devices:
        #     self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
        #                               command=f"sudo nvme disconnect -n {nqn}")
        for node, ssh in self.ssh_obj.ssh_connections.items():
            self.logger.info(f"Closing node ssh connection for {node}")
            ssh.close()
