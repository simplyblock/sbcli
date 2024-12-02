### simplyblock e2e tests
import json
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
import traceback
from requests.exceptions import HTTPError

class TestSingleNodeFailureHA(TestClusterBase):
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
        b. Stop the spdk docker container of that node
        c. Check status of objects during outage:
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
        d. check that fio remains running without interruption.

    8. Wait for node spdk to automatically restart.
        a. check the status again:
            - the status of all nodes is “online”
            - all devices in the cluster are in status “online”
            - the event log contains the records indicating the object status changes
            - select a cluster map from any of the two lvols (lvol get-cluster-map)
              and verify that all nodes and all devices appear online
        b. check that fio remains running without interruption.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fio_runtime = 10*60
        self.logger = setup_logger(__name__)

    def run(self):
        """ Performs each step of the testcase
        """
        self.logger.info(f"Inside run function. Base command: {self.base_cmd}")
        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )

        self.sbcli_utils.add_lvol(
            lvol_name=self.lvol_name,
            pool_name=self.pool_name,
            size="10G",
            # distr_ndcs=2,
            # distr_npcs=1
        )
        lvols = self.sbcli_utils.list_lvols()
        assert self.lvol_name in list(lvols.keys()), \
            f"Lvol {self.lvol_name} not present in list of lvols post add: {lvols}"

        no_lvol_node_uuid = self.sbcli_utils.get_lvol_by_id(lvols[self.lvol_name])['results'][0]['node_id']

        connect_ls = self.sbcli_utils.get_lvol_connect_str_list(lvol_name=self.lvol_name)

        for connect_str in connect_ls:
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

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
                                               "runtime": self.fio_runtime,
                                               "debug": self.fio_debug,
                                               "time_based": True,
                                               "size": "8GiB"})
        fio_thread1.start()

        no_lvol_node = self.sbcli_utils.get_storage_node_details(storage_node_id=no_lvol_node_uuid)
        node_ip = no_lvol_node[0]["mgmt_ip"]

        self.validations(node_uuid=no_lvol_node_uuid,
                         node_status="online",
                         device_status="online",
                         lvol_status="online",
                         health_check_status=True
                         )

        for i in range(5):

            sleep_n_sec(30)
            self.ssh_obj.stop_spdk_process(node=node_ip)

            try:
                self.logger.info(f"Waiting for node to become offline/unreachable, {no_lvol_node_uuid}")
                self.sbcli_utils.wait_for_storage_node_status(no_lvol_node_uuid,
                                                              ["unreachable", "offline"],
                                                              timeout=500)
            except Exception as exp:
                self.logger.debug(exp)
                # self.sbcli_utils.restart_node(node_uuid=no_lvol_node_uuid)
                self.logger.info(f"Waiting for node to become online, {no_lvol_node_uuid}")
                self.sbcli_utils.wait_for_storage_node_status(no_lvol_node_uuid,
                                                              "online",
                                                              timeout=300)
                raise exp

            # self.sbcli_utils.restart_node(node_uuid=no_lvol_node_uuid)
            self.logger.info(f"Waiting for node to become online, {no_lvol_node_uuid}")
            self.sbcli_utils.wait_for_storage_node_status(no_lvol_node_uuid, "online", timeout=300)
            sleep_n_sec(30)
            self.validations(node_uuid=no_lvol_node_uuid,
                             node_status="online",
                             device_status="online",
                             lvol_status="online",
                             health_check_status=True
                             )


        # Write steps in order
        steps = {
            "Storage Node": ["shutdown", "restart"],
        }
        self.common_utils.validate_event_logs(cluster_id=self.cluster_id,
                                              operations=steps)
        
        end_time = self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                                        threads=[fio_thread1],
                                                        timeout=1000)

        self.common_utils.validate_fio_test(node=self.mgmt_nodes[0],
                                            log_file=self.log_path)
        
        total_fio_runtime = end_time - self.ssh_obj.fio_runtime["fio_run_1"]
        self.logger.info(f"FIO Run Time: {total_fio_runtime}")
        
        assert  total_fio_runtime >= self.fio_runtime, \
            f'FIO Run Time Interrupted before given runtime. Actual: {self.ssh_obj.fio_runtime["fio_run_1"]}'

        self.logger.info("TEST CASE PASSED !!!")
