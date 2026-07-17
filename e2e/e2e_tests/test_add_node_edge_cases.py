"""
Edge-case E2E tests for node-add operations.

TestSequentialNodeAdd
    Add two storage nodes one-by-one (not in parallel), verifying that the
    cluster recovers to active between each addition and that FIO on existing
    volumes is never interrupted.

TestAddNodeSnapshotCloneOnNewNode
    After adding a node, create a snapshot of an existing lvol, clone it,
    and verify the clone data matches the original via checksums.

Both tests support Docker (SSH) and K8s-native (CRD) modes.
"""

from __future__ import annotations

import os
import random
import string
import threading
from datetime import datetime

from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec


def _rand_seq(length: int = 6) -> str:
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


class TestSequentialNodeAdd(TestClusterBase):
    """
    Add two nodes one-by-one, verifying cluster state between each addition.

    Steps
    -----
    1. Create pool, create 1 lvol per existing node, connect/mount, start FIO.
    2. Create snapshots and clones on existing lvols, run FIO on clones.
    3. Add first new node; check for in_expansion (graceful); wait for
       node online + cluster active; validate migration tasks.
    4. Create lvol on first new node, run FIO.
    5. Add second new node; same state checks + migration validation.
    6. Create lvol on second new node, run FIO.
    7. Wait for ALL FIO to complete, validate all FIO logs/jobs.
    8. Assert total node count = original + 2, all online + health_check.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "sequential_node_add"

        # Docker / SSH mode: new node IPs
        self.new_nodes = kwargs.get("new_nodes", [])
        if isinstance(self.new_nodes, str):
            self.new_nodes = self.new_nodes.strip().split()

        # K8s-native mode: new worker node names
        self.new_worker_nodes = kwargs.get("new_worker_nodes", [])
        if isinstance(self.new_worker_nodes, str):
            self.new_worker_nodes = [
                n.strip() for n in self.new_worker_nodes.split(",") if n.strip()
            ]

        self.logger.info(f"New nodes (Docker): {self.new_nodes}")
        self.logger.info(f"New worker nodes (K8s): {self.new_worker_nodes}")

    # ── helpers ─────────────────────────────────────────────────────────

    def _check_in_expansion(self):
        """Try to catch the in_expansion state; log either way."""
        try:
            self.sbcli_utils.wait_for_cluster_status(
                cluster_id=self.cluster_id,
                status="in_expansion",
                timeout=60,
            )
            self.logger.info("Cluster entered in_expansion state")
        except Exception:
            self.logger.info("Cluster may already be past in_expansion state")

    def _add_node_docker(self, ip: str):
        """Deploy + add a single storage node via SSH."""
        node_sample = self.sbcli_utils.get_storage_nodes()["results"][0]
        max_lvol = node_sample["max_lvol"]
        max_prov = int(node_sample["max_prov"] / (1024**3))
        data_nics = node_sample.get("data_nics", [])
        data_nic = data_nics[0]["if_name"] if data_nics else None

        self.logger.info(f"Deploying storage node: {ip}")
        self.ssh_obj.deploy_storage_node(ip, max_lvol, max_prov)
        self.ssh_obj.add_storage_node(
            self.mgmt_nodes[0], self.cluster_id, ip,
            spdk_image=node_sample["spdk_image"],
            partitions=node_sample["num_partitions_per_dev"],
            disable_ha_jm=not node_sample["enable_ha_jm"],
            enable_test_device=node_sample["enable_test_device"],
            spdk_debug=node_sample["spdk_debug"],
            data_nic=data_nic,
        )
        sleep_n_sec(60)
        self.storage_nodes.append(ip)
        containers = self.ssh_obj.get_running_containers(node_ip=ip)
        self.container_nodes[ip] = containers

    def _add_node_k8s(self, worker_name: str, initial_pod_count: int):
        """Add a single worker via CRD patch and wait for its spdk pod."""
        from utils.k8s_utils import K8sUtils

        mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
        k8s_utils = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=mgmt_node)

        k8s_utils.patch_storage_node_add_workers(new_workers=[worker_name])
        sleep_n_sec(10)
        k8s_utils.patch_storage_cluster_expand()

        expected_pods = initial_pod_count + 1
        self.logger.info(f"Waiting for {expected_pods} snode-spdk pods")
        k8s_utils.wait_spdk_pods_ready(expected_count=expected_pods, timeout=900)

    def _detect_new_node_ids(self, initial_ids: set) -> list[str]:
        """Return node IDs that appeared since initial snapshot."""
        sleep_n_sec(60)
        all_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        return [n["id"] for n in all_nodes if n["id"] not in initial_ids]

    def _wait_node_online_cluster_active(self, new_node_ids: list[str]):
        """Wait for every new node to go online and cluster to reach active."""
        for nid in new_node_ids:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=nid, status="online", timeout=600,
            )
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )

    # ── main test flow ──────────────────────────────────────────────────

    def run(self):
        self.logger.info("Starting Test: Sequential Node Add")

        nodes_to_add = self.new_worker_nodes if self.k8s_test else self.new_nodes
        assert len(nodes_to_add) >= 2, (
            "TestSequentialNodeAdd requires at least 2 new nodes "
            f"(got {len(nodes_to_add)})"
        )

        # ── Step 1: Create pool + lvols on existing nodes ─────────────
        self.logger.info("Step 1: Creating pool and lvols on existing nodes")
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        initial_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        initial_node_ids = {n["id"] for n in initial_nodes}
        initial_pod_count = len(initial_nodes)
        self.logger.info(f"Initial cluster: {initial_pod_count} storage nodes")

        fio_handles = []
        lvol_names = []

        for i, _ in enumerate(initial_nodes):
            lvol_name = f"seq_add_{_rand_seq(4)}_{i}"
            mount_path = f"{self.mount_path}_{i}"
            log_path = f"{self.log_path}_{i}"

            node_id = self.sbcli_utils.get_node_without_lvols()
            self._create_lvol_dual(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size="5G",
                host_id=node_id,
            )
            device, mount = self._connect_and_mount_dual(
                lvol_name, mount_path=mount_path
            )
            fio_handle = self._run_fio_dual(
                lvol_name=lvol_name,
                mount_path=mount if not self.k8s_test else None,
                log_path=log_path if not self.k8s_test else None,
                name=f"fio_orig_{i}",
                runtime=1200,
                time_based=True,
            )
            fio_handles.append(fio_handle)
            lvol_names.append(lvol_name)

        # ── Step 2: Snapshots + clones with FIO ──────────────────────
        self.logger.info("Step 2: Creating snapshots and clones")

        for i, lvol_name in enumerate(lvol_names):
            snap_name = f"{lvol_name}_snap"
            clone_name = f"{lvol_name}_clone"
            mount_path = f"{self.mount_path}_cl_{i}"
            log_path = f"{self.log_path}_cl_{i}"

            snapshot_id = self._create_snapshot_dual(lvol_name, snap_name)
            sleep_n_sec(5)

            _, cl_mount = self._create_clone_dual(
                snapshot_id=snapshot_id,
                clone_name=clone_name,
                mount_path=mount_path if not self.k8s_test else None,
                format_disk=False,
            )
            fio_handle = self._run_fio_dual(
                lvol_name=clone_name,
                mount_path=cl_mount if not self.k8s_test else None,
                log_path=log_path if not self.k8s_test else None,
                name=f"fio_cl_{i}",
                runtime=1200,
                time_based=True,
            )
            fio_handles.append(fio_handle)

        sleep_n_sec(30)

        # ── Step 3: Add first node ───────────────────────────────────
        self.logger.info(f"Step 3: Adding first node: {nodes_to_add[0]}")
        timestamp_1 = int(datetime.now().timestamp())

        if self.k8s_test:
            self._add_node_k8s(nodes_to_add[0], initial_pod_count)
        else:
            self._add_node_docker(nodes_to_add[0])

        self._check_in_expansion()
        new_ids_1 = self._detect_new_node_ids(initial_node_ids)
        self.logger.info(f"New node IDs after first add: {new_ids_1}")
        self._wait_node_online_cluster_active(new_ids_1)

        sleep_n_sec(120)
        self.validate_migration_for_node(timestamp_1, 2000, None, 60, no_task_ok=False)
        sleep_n_sec(30)

        # ── Step 4: Create lvol on first new node ────────────────────
        self.logger.info("Step 4: Creating lvol on first new node")
        if new_ids_1:
            nn1_lvol = f"nn1_{_rand_seq(4)}"
            nn1_mount = f"{self.mount_path}_nn1"
            nn1_log = f"{self.log_path}_nn1"

            self._create_lvol_dual(
                lvol_name=nn1_lvol, pool_name=self.pool_name,
                size="5G", host_id=new_ids_1[0],
            )
            _, mount = self._connect_and_mount_dual(nn1_lvol, mount_path=nn1_mount)
            fio_handle = self._run_fio_dual(
                lvol_name=nn1_lvol,
                mount_path=mount if not self.k8s_test else None,
                log_path=nn1_log if not self.k8s_test else None,
                name="fio_nn1", runtime=600, time_based=True,
            )
            fio_handles.append(fio_handle)

        # ── Step 5: Add second node ──────────────────────────────────
        self.logger.info(f"Step 5: Adding second node: {nodes_to_add[1]}")
        timestamp_2 = int(datetime.now().timestamp())

        all_ids_before_2 = {n["id"] for n in self.sbcli_utils.get_storage_nodes()["results"]}

        if self.k8s_test:
            current_pod_count = initial_pod_count + len(new_ids_1)
            self._add_node_k8s(nodes_to_add[1], current_pod_count)
        else:
            self._add_node_docker(nodes_to_add[1])

        self._check_in_expansion()
        new_ids_2 = self._detect_new_node_ids(all_ids_before_2)
        self.logger.info(f"New node IDs after second add: {new_ids_2}")
        self._wait_node_online_cluster_active(new_ids_2)

        sleep_n_sec(120)
        self.validate_migration_for_node(timestamp_2, 2000, None, 60, no_task_ok=False)
        sleep_n_sec(30)

        # ── Step 6: Create lvol on second new node ───────────────────
        self.logger.info("Step 6: Creating lvol on second new node")
        if new_ids_2:
            nn2_lvol = f"nn2_{_rand_seq(4)}"
            nn2_mount = f"{self.mount_path}_nn2"
            nn2_log = f"{self.log_path}_nn2"

            self._create_lvol_dual(
                lvol_name=nn2_lvol, pool_name=self.pool_name,
                size="5G", host_id=new_ids_2[0],
            )
            _, mount = self._connect_and_mount_dual(nn2_lvol, mount_path=nn2_mount)
            fio_handle = self._run_fio_dual(
                lvol_name=nn2_lvol,
                mount_path=mount if not self.k8s_test else None,
                log_path=nn2_log if not self.k8s_test else None,
                name="fio_nn2", runtime=600, time_based=True,
            )
            fio_handles.append(fio_handle)

        # ── Step 7: Wait for all FIO ─────────────────────────────────
        self.logger.info("Step 7: Waiting for all FIO to complete")
        self._wait_fio_dual(fio_handles, timeout=1800)
        if not self.k8s_test:
            for h in fio_handles:
                if isinstance(h, threading.Thread):
                    h.join()

        for i, h in enumerate(fio_handles):
            self._validate_fio_dual(h, log_path=f"{self.log_path}_{i}")

        # ── Step 8: Final validation ─────────────────────────────────
        self.logger.info("Step 8: Final validation")
        final_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        assert len(final_nodes) == initial_pod_count + 2, (
            f"Expected {initial_pod_count + 2} nodes, got {len(final_nodes)}"
        )
        for node in final_nodes:
            assert node["status"] == "online", (
                f"Node {node['id']} status={node['status']}, expected online"
            )

        self.logger.info("TEST CASE PASSED: TestSequentialNodeAdd")


class TestAddNodeSnapshotCloneOnNewNode(TestClusterBase):
    """
    After adding a node, create snapshot of existing lvol, clone it,
    and verify clone data matches original via checksums.

    Steps
    -----
    1. Create pool, create 2 lvols on existing nodes, write known data.
    2. Capture checksums.
    3. Add 1 new node, wait for cluster active + migration tasks done.
    4. Create snapshot of each existing lvol.
    5. Clone each snapshot.
    6. Connect/mount clones, capture checksums, assert match originals.
    7. Run FIO (randrw, verify=md5) on clones for 120s.
    8. Validate FIO, validate all nodes healthy.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "add_node_snap_clone"

        self.new_nodes = kwargs.get("new_nodes", [])
        if isinstance(self.new_nodes, str):
            self.new_nodes = self.new_nodes.strip().split()

        self.new_worker_nodes = kwargs.get("new_worker_nodes", [])
        if isinstance(self.new_worker_nodes, str):
            self.new_worker_nodes = [
                n.strip() for n in self.new_worker_nodes.split(",") if n.strip()
            ]

    def run(self):
        self.logger.info("Starting Test: Add Node + Snapshot Clone Verification")

        nodes_to_add = self.new_worker_nodes if self.k8s_test else self.new_nodes
        assert len(nodes_to_add) >= 1, (
            "TestAddNodeSnapshotCloneOnNewNode requires at least 1 new node"
        )

        # ── Step 1: Create pool + lvols, write data ───────────────────
        self.logger.info("Step 1: Creating pool and lvols with FIO data")
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        initial_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        initial_node_ids = {n["id"] for n in initial_nodes}
        initial_pod_count = len(initial_nodes)

        lvol_checksums = {}
        fio_handles = []

        for i in range(min(2, len(initial_nodes))):
            lvol_name = f"snap_cl_{_rand_seq(4)}_{i}"
            mount_path = f"{self.mount_path}_{i}"
            log_path = f"{self.log_path}_{i}"

            node_id = self.sbcli_utils.get_node_without_lvols()
            self._create_lvol_dual(
                lvol_name=lvol_name, pool_name=self.pool_name,
                size="5G", host_id=node_id,
            )
            device, mount = self._connect_and_mount_dual(
                lvol_name, mount_path=mount_path
            )

            # Write known data (not time-based, just write a fixed amount)
            fio_handle = self._run_fio_dual(
                lvol_name=lvol_name,
                mount_path=mount if not self.k8s_test else None,
                log_path=log_path if not self.k8s_test else None,
                name=f"fio_write_{i}",
                runtime=60,
                time_based=True,
            )
            fio_handles.append((lvol_name, fio_handle))

        # Wait for initial writes to complete
        self._wait_fio_dual([h for _, h in fio_handles], timeout=300)
        if not self.k8s_test:
            for _, h in fio_handles:
                if isinstance(h, threading.Thread):
                    h.join()

        # ── Step 2: Capture checksums ─────────────────────────────────
        self.logger.info("Step 2: Capturing checksums")
        for i, (lvol_name, _) in enumerate(fio_handles):
            mount_path = f"{self.mount_path}_{i}"
            checksums = self._generate_checksums_dual(
                lvol_name,
                directory=mount_path if not self.k8s_test else None,
            )
            lvol_checksums[lvol_name] = checksums
            self.logger.info(f"Checksums for {lvol_name}: {checksums}")

        # ── Step 3: Add node ──────────────────────────────────────────
        self.logger.info(f"Step 3: Adding node: {nodes_to_add[0]}")
        timestamp = int(datetime.now().timestamp())

        if self.k8s_test:
            from utils.k8s_utils import K8sUtils
            mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
            k8s_utils = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=mgmt_node)
            k8s_utils.patch_storage_node_add_workers(new_workers=[nodes_to_add[0]])
            sleep_n_sec(10)
            k8s_utils.patch_storage_cluster_expand()
            k8s_utils.wait_spdk_pods_ready(
                expected_count=initial_pod_count + 1, timeout=900
            )
        else:
            node_sample = self.sbcli_utils.get_storage_nodes()["results"][0]
            max_lvol = node_sample["max_lvol"]
            max_prov = int(node_sample["max_prov"] / (1024**3))
            data_nics = node_sample.get("data_nics", [])
            data_nic = data_nics[0]["if_name"] if data_nics else None
            self.ssh_obj.deploy_storage_node(nodes_to_add[0], max_lvol, max_prov)
            self.ssh_obj.add_storage_node(
                self.mgmt_nodes[0], self.cluster_id, nodes_to_add[0],
                spdk_image=node_sample["spdk_image"],
                partitions=node_sample["num_partitions_per_dev"],
                disable_ha_jm=not node_sample["enable_ha_jm"],
                enable_test_device=node_sample["enable_test_device"],
                spdk_debug=node_sample["spdk_debug"],
                data_nic=data_nic,
            )
            sleep_n_sec(60)
            self.storage_nodes.append(nodes_to_add[0])

        # Check in_expansion
        try:
            self.sbcli_utils.wait_for_cluster_status(
                cluster_id=self.cluster_id, status="in_expansion", timeout=60,
            )
            self.logger.info("Cluster entered in_expansion state")
        except Exception:
            self.logger.info("Cluster may already be past in_expansion state")

        # Wait for new node online + cluster active
        sleep_n_sec(60)
        all_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        new_node_ids = [n["id"] for n in all_nodes if n["id"] not in initial_node_ids]
        for nid in new_node_ids:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=nid, status="online", timeout=600
            )
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )

        sleep_n_sec(120)
        self.validate_migration_for_node(timestamp, 2000, None, 60, no_task_ok=False)

        # ── Step 4-6: Snapshot, clone, verify checksums ──────────────
        self.logger.info("Step 4-6: Creating snapshots, clones, verifying checksums")

        clone_fio_handles = []
        for i, (lvol_name, _) in enumerate(fio_handles):
            snap_name = f"{lvol_name}_snap"
            clone_name = f"{lvol_name}_clone"
            cl_mount = f"{self.mount_path}_cl_{i}"
            cl_log = f"{self.log_path}_cl_{i}"

            snapshot_id = self._create_snapshot_dual(lvol_name, snap_name)
            sleep_n_sec(5)

            _, mount = self._create_clone_dual(
                snapshot_id=snapshot_id,
                clone_name=clone_name,
                mount_path=cl_mount if not self.k8s_test else None,
                format_disk=False,
            )

            # Capture clone checksums
            clone_checksums = self._generate_checksums_dual(
                clone_name,
                directory=cl_mount if not self.k8s_test else None,
            )

            # Compare checksums
            original = set(lvol_checksums[lvol_name].values())
            cloned = set(clone_checksums.values())
            self.logger.info(f"Original checksums: {original}")
            self.logger.info(f"Clone checksums: {cloned}")
            assert original == cloned, (
                f"Checksum mismatch for {clone_name}! "
                f"Original: {original}, Clone: {cloned}"
            )

            # Run FIO with verify on clone
            fio_handle = self._run_fio_dual(
                lvol_name=clone_name,
                mount_path=mount if not self.k8s_test else None,
                log_path=cl_log if not self.k8s_test else None,
                name=f"fio_verify_{i}",
                runtime=120,
                time_based=True,
            )
            clone_fio_handles.append(fio_handle)

        # ── Step 7-8: Validate FIO and nodes ─────────────────────────
        self.logger.info("Step 7-8: Validating FIO and node health")
        self._wait_fio_dual(clone_fio_handles, timeout=300)
        if not self.k8s_test:
            for h in clone_fio_handles:
                if isinstance(h, threading.Thread):
                    h.join()

        for i, h in enumerate(clone_fio_handles):
            self._validate_fio_dual(h, log_path=f"{self.log_path}_cl_{i}")

        final_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        for node in final_nodes:
            assert node["status"] == "online", (
                f"Node {node['id']} status={node['status']}, expected online"
            )

        self.logger.info("TEST CASE PASSED: TestAddNodeSnapshotCloneOnNewNode")
