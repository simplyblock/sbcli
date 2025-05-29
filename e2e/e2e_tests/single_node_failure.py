### simplyblock e2e tests
import json
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
import traceback
from requests.exceptions import HTTPError

class TestSingleNodeFailure(TestClusterBase):
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
        self.snapshot_name = "snapshot"
        self.logger = setup_logger(__name__)
        self.test_name = "single_node_failure"

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

        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=self.lvol_name)
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
                                               "runtime": 500,
                                               "debug": self.fio_debug,
                                               "time_based": False,
                                               "size": "8GiB"})
        fio_thread1.start()

        no_lvol_node_uuid = self.sbcli_utils.get_node_without_lvols()
        no_lvol_node = self.sbcli_utils.get_storage_node_details(storage_node_id=no_lvol_node_uuid)
        node_ip = no_lvol_node[0]["mgmt_ip"]
        
        self.logger.info("Taking snapshot")
        self.ssh_obj.add_snapshot(node=self.mgmt_nodes[0],
                                  lvol_id=self.sbcli_utils.get_lvol_id(self.lvol_name),
                                  snapshot_name=f"{self.snapshot_name}_1")
        snapshot_id_1 = self.ssh_obj.get_snapshot_id(node=self.mgmt_nodes[0],
                                                     snapshot_name=f"{self.snapshot_name}_1")
        
        self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(self.lvol_name),
                                     new_size="20G")

        self.validations(node_uuid=no_lvol_node_uuid,
                         node_status="online",
                         device_status="online",
                         lvol_status="online",
                         health_check_status=True,
                         device_health_check=None
                         )
        
        sleep_n_sec(30)
        timestamp = int(datetime.now().timestamp())
        self.ssh_obj.stop_spdk_process(node=node_ip, rpc_port=no_lvol_node[0]["rpc_port"])

        try:
            self.logger.info(f"Waiting for node to become offline/unreachable, {no_lvol_node_uuid}")
            self.sbcli_utils.wait_for_storage_node_status(no_lvol_node_uuid,
                                                          ["unreachable", "offline"],
                                                          timeout=500)
            # sleep_n_sec(30)
            # self.validations(node_uuid=no_lvol_node_uuid,
            #                 node_status=["offline", "in_shutdown", "in_restart"],
            #                 # The status changes between them very quickly hence
            #                 # needed multiple checks
            #                 device_status="unavailable",
            #                 lvol_status="online",
            #                 health_check_status=False
            #                 )
            try:
                # expected_error_regex = r"Failed to create BDev: distr_\d+_test_lvol_fail"
                self.sbcli_utils.add_lvol(
                    lvol_name=f"{self.lvol_name}_fail",
                    pool_name=self.pool_name,
                    size="10G",
                    # distr_ndcs=2,
                    # distr_npcs=1,
                    host_id=no_lvol_node_uuid,
                    retry=2
                )
            except HTTPError as e:
                error = json.loads(e.response.text)
                self.logger.info(f"Lvol addition failed for node {no_lvol_node_uuid}. Error:{error}")
                assert "Storage node is not online" in error["error"], f"Unexpected error: {error['error']}"
                lvols = self.sbcli_utils.list_lvols()
                assert f"{self.lvol_name}_fail" not in list(lvols.keys()), \
                    (f"Lvol {self.lvol_name}_fail present in list of lvols post add: {lvols}. "
                     "Expected: Lvol is not added")
            
            sleep_n_sec(10)
            self.sbcli_utils.add_lvol(
                    lvol_name=f"{self.lvol_name}_2",
                    pool_name=self.pool_name,
                    size="10G",
                    # distr_ndcs=2,
                    # distr_npcs=1,
                )
            lvols = self.sbcli_utils.list_lvols()
            assert f"{self.lvol_name}_2" in list(lvols.keys()), \
                (f"Lvol {self.lvol_name}_2 not present in list of lvols post add: {lvols}. "
                 "Expected: Lvol is added")

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
                         health_check_status=True,
                         device_health_check=None
                         )
        

        self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(self.lvol_name),
                                     new_size="25G")
        if not self.k8s_test:
            for node in self.storage_nodes:
                self.ssh_obj.restart_docker_logging(
                    node_ip=node,
                    containers=self.container_nodes[node],
                    log_dir=self.docker_logs_path,
                    test_name=self.test_name
                )
        else:
            self.runner_k8s_log.restart_logging()
        sleep_n_sec(120)
        self.validate_migration_for_node(
            timestamp=timestamp,
            timeout=1000,
            node_id=None
        )

        # Write steps in order
        steps = {
            "Storage Node": ["shutdown", "restart"],
            "Device": {"restart"}
        }
        self.common_utils.validate_event_logs(cluster_id=self.cluster_id,
                                              operations=steps)
        
        end_time = self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                                        threads=[fio_thread1],
                                                        timeout=1000)
        
        self.ssh_obj.add_snapshot(node=self.mgmt_nodes[0],
                                  lvol_id=self.sbcli_utils.get_lvol_id(self.lvol_name),
                                  snapshot_name=f"{self.snapshot_name}_2")
        snapshot_id_2 = self.ssh_obj.get_snapshot_id(node=self.mgmt_nodes[0],
                                                     snapshot_name=f"{self.snapshot_name}_2")
        
        lvol_files = self.ssh_obj.find_files(self.mgmt_nodes[0], directory=self.mount_path)
        original_checksum = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], lvol_files)

        clone_mount_file = f"{self.mount_path}_cl"

        self.ssh_obj.add_clone(node=self.mgmt_nodes[0],
                               snapshot_id=snapshot_id_1,
                               clone_name=f"{self.lvol_name}_cl_1")
        
        self.ssh_obj.add_clone(node=self.mgmt_nodes[0],
                               snapshot_id=snapshot_id_2,
                               clone_name=f"{self.lvol_name}_cl_2")
        
        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=f"{self.lvol_name}_cl_1")
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
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                device=disk_use,
                                mount_path=f"{clone_mount_file}_1")
        
        initial_devices = final_devices
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=f"{self.lvol_name}_cl_2")
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
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                device=disk_use,
                                mount_path=f"{clone_mount_file}_2")

        self.common_utils.validate_fio_test(node=self.mgmt_nodes[0],
                                            log_file=self.log_path)
        
        self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(f"{self.lvol_name}_cl_1"),
                                     new_size="30G")
        sleep_n_sec(10)
        self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(f"{self.lvol_name}_cl_2"),
                                     new_size="30G")
        
        clone_files = self.ssh_obj.find_files(self.mgmt_nodes[0], directory=f"{clone_mount_file}_2")
        final_checksum = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], clone_files)

        self.logger.info(f"Original checksum: {original_checksum}")
        self.logger.info(f"Final checksum: {final_checksum}")
        original_checksum = set(original_checksum.values())
        final_checksum = set(final_checksum.values())

        self.logger.info(f"Set Original checksum: {original_checksum}")
        self.logger.info(f"Set Final checksum: {final_checksum}")

        assert original_checksum == final_checksum, "Checksum mismatch for lvol and clone"
        self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(self.lvol_name),
                                     new_size="30G")

        lvol_files = self.ssh_obj.find_files(self.mgmt_nodes[0], directory=self.mount_path)
        final_lvl_checksum = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], lvol_files)
        final_lvl_checksum = set(final_lvl_checksum.values())

        assert original_checksum == final_lvl_checksum, "Checksum mismatch for lvol before and after clone"
        
        # total_fio_runtime = end_time - self.ssh_obj.fio_runtime["fio_run_1"]
        # self.logger.info(f"FIO Run Time: {total_fio_runtime}")
        
        # assert  total_fio_runtime >= 500, \
        #     f'FIO Run Time Interrupted before given runtime. Actual: {self.ssh_obj.fio_runtime["fio_run_1"]}'

        self.logger.info("TEST CASE PASSED !!!")


class TestHASingleNodeFailure(TestClusterBase):
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
        self.fio_runtime = 5*60
        self.logger = setup_logger(__name__)
        self.fio_threads = []
        self.test_name = "single_node_failure_ha"

    def run(self):
        """ Performs each step of the testcase
        """
        self.logger.info(f"Inside run function. Base command: {self.base_cmd}")

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )

        for i in range(3):
            lvol_name = f"LVOL_{i}"
            lvol_id = self.add_lvol_and_run_fio(lvol_name)

        # no_lvol_node_uuid = self.sbcli_utils.get_lvol_by_id(lvol_id)['results'][0]['node_id']

        no_lvol_node = None
        for node in self.sbcli_utils.get_storage_nodes()['results'][::-1]:
            if node['lvols'] > 0 and node['is_secondary_node'] is False:
                no_lvol_node = node
                break

        no_lvol_node_uuid = no_lvol_node['uuid']
        node_ip = no_lvol_node["mgmt_ip"]

        self.validations(node_uuid=no_lvol_node_uuid,
                         node_status="online",
                         device_status="online",
                         lvol_status="online",
                         health_check_status=True,
                         device_health_check=None
                         )
        
        self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(self.lvol_name),
                                     new_size="20G")

        for i in range(2):

            sleep_n_sec(30)
            timestamp = int(datetime.now().timestamp())
            self.ssh_obj.stop_spdk_process(node=node_ip, rpc_port=no_lvol_node["rpc_port"])

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
                             health_check_status=True,
                             device_health_check=None
                             )
            if not self.k8s_test:
                for node in self.storage_nodes:
                    self.ssh_obj.restart_docker_logging(
                        node_ip=node,
                        containers=self.container_nodes[node],
                        log_dir=self.docker_logs_path,
                        test_name=self.test_name
                    )
            else:
                self.runner_k8s_log.restart_logging()
            sleep_n_sec(120)
            self.validate_migration_for_node(
                timestamp=timestamp,
                timeout=1000,
                node_id=None
            )

        self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(self.lvol_name),
                                     new_size="25G")
        # Write steps in order
        steps = {
            "Storage Node": ["shutdown", "restart"],
        }
        self.common_utils.validate_event_logs(cluster_id=self.cluster_id,
                                              operations=steps)

        end_time = self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                                        threads=self.fio_threads,
                                                        timeout=1000)

        for i in range(3):
            lvol_name = f"LVOL_{i}"

            self.common_utils.validate_fio_test(node=self.mgmt_nodes[0],
                                                log_file=self.log_path+f"_{lvol_name}")

            total_fio_runtime = end_time - self.ssh_obj.fio_runtime[f"fio_run_{lvol_name}"]
            self.logger.info(f"FIO Run Time: {total_fio_runtime}")

            assert  total_fio_runtime >= self.fio_runtime, \
                f'FIO Run Time Interrupted before given runtime. Actual: {self.ssh_obj.fio_runtime[f"fio_run_{lvol_name}"]}'

        self.logger.info("TEST CASE PASSED !!!")


    def add_lvol_and_run_fio(self, lvol_name):
        self.lvol_name = lvol_name
        mount_path = self.mount_path+f"_{lvol_name}"
        log_path = self.log_path+f"_{lvol_name}"

        host_id = self.sbcli_utils.get_node_without_lvols()

        self.sbcli_utils.add_lvol(
            lvol_name=self.lvol_name,
            pool_name=self.pool_name,
            size="10G",
            host_id=host_id
        )

        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        lvols = self.sbcli_utils.list_lvols()
        assert self.lvol_name in list(lvols.keys()), \
            f"Lvol {self.lvol_name} not present in list of lvols post add: {lvols}"

        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=self.lvol_name)

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
                                 device=disk_use, fs_type='xfs')
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                device=disk_use,
                                mount_path=mount_path)

        fio_thread1 = threading.Thread(target=self.ssh_obj.run_fio_test,
                                       args=(self.mgmt_nodes[0], None, mount_path, log_path,),
                                       kwargs={"name": f"fio_run_{lvol_name}",
                                               "runtime": self.fio_runtime,
                                               "debug": self.fio_debug})
        fio_thread1.start()
        self.fio_threads.append(fio_thread1)
        return lvols[self.lvol_name]