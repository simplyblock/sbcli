### simplyblock e2e tests
import os
import json
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
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
            - the node is in status "offline"
            - the devices of the node are in status "unavailable"
            - lvols remain in "online" state
            - the event log contains the records indicating the object status
              changes; the event log also contains records indicating read and
              write IO errors.
            - select a cluster map from any of the two lvols (lvol get-cluster-map)
              and verify that the status changes of the node and devices are reflected in
              the other cluster map. Other two nodes and 4 devices remain online.
            - health-check status of all nodes and devices is "true"
        d. check that fio remains running without interruption.

    8. Wait for node spdk to automatically restart.
        a. check the status again:
            - the status of all nodes is "online"
            - all devices in the cluster are in status "online"
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

        self._add_pool_dual(pool_name=self.pool_name)

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        self._create_lvol_dual(
            lvol_name=self.lvol_name,
            pool_name=self.pool_name,
            size="10G",
        )
        lvols = self.sbcli_utils.list_lvols()
        assert self.lvol_name in list(lvols.keys()), \
            f"Lvol {self.lvol_name} not present in list of lvols post add: {lvols}"

        device, mount = self._connect_and_mount_dual(
            self.lvol_name, mount_path=self.mount_path
        )

        fio_handle = self._run_fio_dual(
            lvol_name=self.lvol_name,
            mount_path=mount if not self.k8s_test else None,
            name="fio_run_1",
            runtime=500,
            time_based=False,
            size="8GiB",
        )

        no_lvol_node_uuid = self.sbcli_utils.get_node_without_lvols()
        no_lvol_node = self.sbcli_utils.get_storage_node_details(storage_node_id=no_lvol_node_uuid)
        node_ip = no_lvol_node[0]["mgmt_ip"]

        self.logger.info("Taking snapshot")
        snapshot_id_1 = self._create_snapshot_dual(
            self.lvol_name, f"{self.snapshot_name}_1"
        )

        self._resize_lvol_dual(self.lvol_name, "20G")

        self.validations(node_uuid=no_lvol_node_uuid,
                         node_status="online",
                         device_status="online",
                         lvol_status="online",
                         health_check_status=True,
                         device_health_check=None
                         )

        sleep_n_sec(30)
        timestamp = int(datetime.now().timestamp())
        if self.k8s_test:
            k8s_obj = getattr(self.sbcli_utils, 'k8s', None)
            if k8s_obj:
                k8s_obj.restart_spdk_pod(node_ip)
        else:
            self.ssh_obj.stop_spdk_process(node=node_ip, rpc_port=no_lvol_node[0]["rpc_port"], cluster_id=self.cluster_id)

        try:
            self.logger.info(f"Waiting for node to become offline/unreachable, {no_lvol_node_uuid}")
            self.sbcli_utils.wait_for_storage_node_status(no_lvol_node_uuid,
                                                          ["unreachable", "offline"],
                                                          timeout=500)
            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=f"{self.lvol_name}_fail",
                    pool_name=self.pool_name,
                    size="10G",
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
                )
            lvols = self.sbcli_utils.list_lvols()
            assert f"{self.lvol_name}_2" in list(lvols.keys()), \
                (f"Lvol {self.lvol_name}_2 not present in list of lvols post add: {lvols}. "
                 "Expected: Lvol is added")

        except Exception as exp:
            self.logger.debug(exp)
            self.logger.info(f"Waiting for node to become online, {no_lvol_node_uuid}")
            self.sbcli_utils.wait_for_storage_node_status(no_lvol_node_uuid,
                                                          "online",
                                                          timeout=300)
            raise exp

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

        self._resize_lvol_dual(self.lvol_name, "25G")

        if not self.k8s_test:
            for node in self.storage_nodes:
                self.ssh_obj.restart_docker_logging(
                    node_ip=node,
                    containers=self.container_nodes[node],
                    log_dir=os.path.join(self.docker_logs_path, node),
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

        self._wait_fio_dual([fio_handle], timeout=1000)
        if not self.k8s_test:
            if isinstance(fio_handle, threading.Thread):
                fio_handle.join()

        self.logger.info("Taking snapshot 2")
        snapshot_id_2 = self._create_snapshot_dual(
            self.lvol_name, f"{self.snapshot_name}_2"
        )

        original_checksum = self._generate_checksums_dual(
            self.lvol_name, directory=self.mount_path if not self.k8s_test else None
        )

        clone_mount_file = f"{self.mount_path}_cl"

        _, cl1_mount = self._create_clone_dual(
            snapshot_id=snapshot_id_1,
            clone_name=f"{self.lvol_name}_cl_1",
            mount_path=f"{clone_mount_file}_1" if not self.k8s_test else None,
            format_disk=False,
        )
        _, cl2_mount = self._create_clone_dual(
            snapshot_id=snapshot_id_2,
            clone_name=f"{self.lvol_name}_cl_2",
            mount_path=f"{clone_mount_file}_2" if not self.k8s_test else None,
            format_disk=False,
        )

        self._validate_fio_dual(fio_handle, log_path=self.log_path)

        self._resize_lvol_dual(f"{self.lvol_name}_cl_1", "30G")
        sleep_n_sec(10)
        self._resize_lvol_dual(f"{self.lvol_name}_cl_2", "30G")

        final_checksum = self._generate_checksums_dual(
            f"{self.lvol_name}_cl_2",
            directory=f"{clone_mount_file}_2" if not self.k8s_test else None,
        )

        self.logger.info(f"Original checksum: {original_checksum}")
        self.logger.info(f"Final checksum: {final_checksum}")
        original_checksum = set(original_checksum.values())
        final_checksum = set(final_checksum.values())

        self.logger.info(f"Set Original checksum: {original_checksum}")
        self.logger.info(f"Set Final checksum: {final_checksum}")

        assert original_checksum == final_checksum, "Checksum mismatch for lvol and clone"
        self._resize_lvol_dual(self.lvol_name, "30G")

        final_lvl_checksum = self._generate_checksums_dual(
            self.lvol_name,
            directory=self.mount_path if not self.k8s_test else None,
        )
        final_lvl_checksum = set(final_lvl_checksum.values())

        assert original_checksum == final_lvl_checksum, "Checksum mismatch for lvol before and after clone"

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
            - the node is in status "offline"
            - the devices of the node are in status "unavailable"
            - lvols remain in "online" state
            - the event log contains the records indicating the object status
              changes; the event log also contains records indicating read and
              write IO errors.
            - select a cluster map from any of the two lvols (lvol get-cluster-map)
              and verify that the status changes of the node and devices are reflected in
              the other cluster map. Other two nodes and 4 devices remain online.
            - health-check status of all nodes and devices is "true"
        d. check that fio remains running without interruption.

    8. Wait for node spdk to automatically restart.
        a. check the status again:
            - the status of all nodes is "online"
            - all devices in the cluster are in status "online"
            - the event log contains the records indicating the object status changes
            - select a cluster map from any of the two lvols (lvol get-cluster-map)
              and verify that all nodes and all devices appear online
        b. check that fio remains running without interruption.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fio_runtime = 5*60
        self.logger = setup_logger(__name__)
        self.fio_handles = []
        self.test_name = "single_node_failure_ha"

    def run(self):
        """ Performs each step of the testcase
        """
        self.logger.info(f"Inside run function. Base command: {self.base_cmd}")

        self._add_pool_dual(pool_name=self.pool_name)

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        for i in range(3):
            lvol_name = f"LVOL_{i}"
            self.add_lvol_and_run_fio(lvol_name)

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

        self._resize_lvol_dual(self.lvol_name, "20G")

        for i in range(2):

            sleep_n_sec(30)
            timestamp = int(datetime.now().timestamp())
            if self.k8s_test:
                k8s_obj = getattr(self.sbcli_utils, 'k8s', None)
                if k8s_obj:
                    k8s_obj.restart_spdk_pod(node_ip)
            else:
                self.ssh_obj.stop_spdk_process(node=node_ip, rpc_port=no_lvol_node["rpc_port"], cluster_id=self.cluster_id)

            try:
                self.logger.info(f"Waiting for node to become offline/unreachable, {no_lvol_node_uuid}")
                self.sbcli_utils.wait_for_storage_node_status(no_lvol_node_uuid,
                                                              ["unreachable", "offline"],
                                                              timeout=500)
            except Exception as exp:
                self.logger.debug(exp)
                self.logger.info(f"Waiting for node to become online, {no_lvol_node_uuid}")
                self.sbcli_utils.wait_for_storage_node_status(no_lvol_node_uuid,
                                                              "online",
                                                              timeout=300)
                raise exp

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
                        log_dir=os.path.join(self.docker_logs_path, node),
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

        self._resize_lvol_dual(self.lvol_name, "25G")

        # Write steps in order
        steps = {
            "Storage Node": ["shutdown", "restart"],
        }
        self.common_utils.validate_event_logs(cluster_id=self.cluster_id,
                                              operations=steps)

        self._wait_fio_dual(self.fio_handles, timeout=1000)
        if not self.k8s_test:
            for h in self.fio_handles:
                if isinstance(h, threading.Thread):
                    h.join()

        for i in range(3):
            lvol_name = f"LVOL_{i}"
            handle = self.fio_handles[i] if i < len(self.fio_handles) else None
            if handle:
                self._validate_fio_dual(handle,
                                        log_path=self.log_path + f"_{lvol_name}")

        self.logger.info("TEST CASE PASSED !!!")


    def add_lvol_and_run_fio(self, lvol_name):
        self.lvol_name = lvol_name
        mount_path = self.mount_path + f"_{lvol_name}"
        log_path = self.log_path + f"_{lvol_name}.log"

        host_id = self.sbcli_utils.get_node_without_lvols()

        self._create_lvol_dual(
            lvol_name=self.lvol_name,
            pool_name=self.pool_name,
            size="10G",
            host_id=host_id,
        )

        lvols = self.sbcli_utils.list_lvols()
        assert self.lvol_name in list(lvols.keys()), \
            f"Lvol {self.lvol_name} not present in list of lvols post add: {lvols}"

        device, mount = self._connect_and_mount_dual(
            self.lvol_name, mount_path=mount_path, fs_type='xfs'
        )

        fio_handle = self._run_fio_dual(
            lvol_name=self.lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=log_path if not self.k8s_test else None,
            name=f"fio_run_{lvol_name}",
            runtime=self.fio_runtime,
            time_based=True,
        )
        self.fio_handles.append(fio_handle)
        return lvols[self.lvol_name]
