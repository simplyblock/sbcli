# stress_test/continuous_failover_ha_multi_client_quick_outage.py
# Fast outages with long-running FIO, no churn beyond initial setup.
# - Create lvols, snapshots, clones ONCE at the beginning
# - Start 30min FIO on all mounts (lvols + clones)
# - Run fast outages (as soon as node is ONLINE again)
# - Every 5 outages: wait for all FIO to complete, validate, then (optionally) wait for migration window
# - Graceful shutdown: suspend -> wait SUSPENDED -> shutdown -> wait OFFLINE -> keep offline 5 min -> restart
# - After any restart: 15–30s idle then immediately next outage

import os
import random
import string
import threading
import time
from datetime import datetime
from utils.common_utils import sleep_n_sec
from exceptions.custom_exception import LvolNotConnectException
from stress_test.lvol_ha_stress_fio import TestLvolHACluster


def _rand_id(n=15, first_alpha=True):
    letters = string.ascii_uppercase
    digits = string.digits
    allc = letters + digits
    if first_alpha:
        return random.choice(letters) + ''.join(random.choices(allc, k=n-1))
    return ''.join(random.choices(allc, k=n))


class RandomRapidFailoverNoGap(TestLvolHACluster):
    """
    - Minimal churn (only bootstrap creates)
    - Long FIO (30 mins) on every lvol/clone
    - Outage pacing: next outage right after ONLINE; add 15–30s buffer post-restart
    - Validate FIO and pause for migration every 5 outages
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Base knobs
        self.total_lvols = 20
        self.lvol_size = "40G"
        self.fio_size = "15G"

        # Validation cadence & FIO runtime
        self.validate_every = 5
        self._iter = 0
        self._per_wave_fio_runtime = 900      # 60 minutes
        self._fio_wait_timeout = 1800         # wait for all to finish

        # Internal state
        self.fio_threads = []
        self.lvol_mount_details = {}
        self.clone_mount_details = {}
        self.sn_nodes = []
        self.sn_nodes_with_sec = []
        self.sn_primary_secondary_map = {}
        self.node_vs_lvol = {}
        self.snapshot_names = []
        self.snap_vs_node = {}
        self.current_outage_node = None
        self.outage_start_time = None
        self.outage_end_time = None
        self.first_outage_ts = None            # track the first outage for migration window
        self.test_name = "longfio_nochurn_rapid_outages"
        # Maps node_uuid -> (node_ip, local_tmp_log_dir) for ongoing network outages
        self._local_outage_log_dirs = {}

        self.outage_types = [
            # "graceful_shutdown",
            "container_stop",
            # "interface_full_network_interrupt",
        ]
        self.available_fabrics = ["tcp"]   # overwritten in run() via fabric detection
        self.k8s_utils = None              # initialised in run() when k8s_test=True

        # Names
        self.lvol_base = f"lvl{_rand_id(12)}"
        self.clone_base = f"cln{_rand_id(12)}"
        self.snap_base = f"snap{_rand_id(12)}"

        # Logging file for outages
        self.outage_log_file = os.path.join("logs", f"outage_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        self._init_outage_log()

    # ---------- small utilities ----------

    def _init_outage_log(self):
        os.makedirs(os.path.dirname(self.outage_log_file), exist_ok=True)
        with open(self.outage_log_file, "w") as f:
            f.write("Timestamp,Node,Outage_Type,Event\n")

    def _log_outage_event(self, node, outage_type, event):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.outage_log_file, "a") as f:
            f.write(f"{ts},{node},{outage_type},{event}\n")

    def _short_bs(self):
        # return f"{2 ** random.randint(2, 7)}K"  # 4K–128K
        return f"{2 ** 6}K"

    def _pick_outage(self):
        random.shuffle(self.outage_types)
        return self.outage_types[0]

    # ---------- cluster bootstrap ----------

    def _wait_cluster_active(self, timeout=900, poll=5):
        """
        Poll `sbctl cluster list` until status ACTIVE.
        Avoids 400 in_activation when creating lvol/snap/clone during bring-up.
        """
        end = datetime.now().timestamp() + timeout
        while datetime.now().timestamp() < end:
            try:
                info = self.ssh_obj.cluster_list(self.mgmt_nodes[0], self.cluster_id)  # must wrap "sbctl cluster list"
                self.logger.info(info)
                # Expect a single row with Status
                status = str(info).upper()
                if "ACTIVE" in status:
                    return
            except Exception as e:
                self.logger.info(f"ERROR: {e}")
            sleep_n_sec(poll)
        raise RuntimeError("Cluster did not become ACTIVE within timeout")

    def _bootstrap_cluster(self):
        # Ensure Cluster is ACTIVE
        self._wait_cluster_active()

        # create pool
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # discover storage nodes
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for res in storage_nodes['results']:
            self.sn_nodes.append(res["uuid"])
            self.sn_nodes_with_sec.append(res["uuid"])
            self.sn_primary_secondary_map[res["uuid"]] = res["secondary_node_id"]
        
        self.logger.info(f"[LFNG] SN sec map: {self.sn_primary_secondary_map}")

        # initial lvols + mount + then later clone from snapshots
        self._create_lvols(count=self.total_lvols)  # start_fio=False → we launch after clones
        self._seed_snapshots_and_clones()           # also mounts clones

        # Start 30 min FIO on all (lvols + clones)
        self._kick_fio_for_all(runtime=self._per_wave_fio_runtime)

        # start container logs
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

    # ---------- lvol / fio helpers ----------

    def _create_lvols(self, count=1):
        for _ in range(count):
            fs_type = random.choice(["ext4", "xfs"])
            is_crypto = random.choice([True, False])
            fabric = random.choice(self.available_fabrics)
            name_core = f"{self.lvol_base}_{_rand_id(6, first_alpha=False)}"
            lvol_name = name_core if not is_crypto else f"c{name_core}"

            kwargs = dict(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                crypto=is_crypto,
                key1=self.lvol_crypt_keys[0],
                key2=self.lvol_crypt_keys[1],
                fabric=fabric,
            )

            # Avoid outage node & partner during initial placement
            if self.current_outage_node:
                skip_nodes = [self.current_outage_node, self.sn_primary_secondary_map.get(self.current_outage_node)]
                skip_nodes += [p for p, s in self.sn_primary_secondary_map.items() if s == self.current_outage_node]
                host_id = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
                if host_id:
                    kwargs["host_id"] = host_id[0]

            # Ensure cluster ACTIVE before creating
            self._wait_cluster_active()

            try:
                self.sbcli_utils.add_lvol(**kwargs)
            except Exception as e:
                self.logger.warning(f"[LFNG] lvol create failed ({lvol_name}) → {e}; retry once after ACTIVE gate")
                self._wait_cluster_active()
                self.sbcli_utils.add_lvol(**kwargs)

            # record
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            self.lvol_mount_details[lvol_name] = {
                "ID": lvol_id,
                "Command": None,
                "Mount": None,
                "Device": None,
                "MD5": None,
                "FS": fs_type,
                "Log": f"{self.log_path}/{lvol_name}.log",
                "snapshots": [],
                "iolog_base_path": f"{self.log_path}/{lvol_name}_fio_iolog",
            }

            # refresh list
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=f"{self.base_cmd} lvol list", supress_logs=True)

            # track node placement
            lvol_node_id = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]["node_id"]
            self.node_vs_lvol.setdefault(lvol_node_id, []).append(lvol_name)

            # connect
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            self.lvol_mount_details[lvol_name]["Command"] = connect_ls

            client_node = random.choice(self.fio_node)
            self.lvol_mount_details[lvol_name]["Client"] = client_node

            initial = self.ssh_obj.get_devices(node=client_node)
            for c in connect_ls:
                _, err = self.ssh_obj.exec_command(node=client_node, command=c)
                if err:
                    nqn = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]["nqn"]
                    self.ssh_obj.disconnect_nvme(node=client_node, nqn_grep=nqn)
                    self.logger.info(f"[LFNG] connect error → clean lvol {lvol_name}")
                    self.sbcli_utils.delete_lvol(lvol_name=lvol_name, max_attempt=20, skip_error=True)
                    sleep_n_sec(3)
                    del self.lvol_mount_details[lvol_name]
                    self.node_vs_lvol[lvol_node_id].remove(lvol_name)
                    break

            final = self.ssh_obj.get_devices(node=client_node)
            new_dev = None
            for d in final:
                if d not in initial:
                    new_dev = f"/dev/{d.strip()}"
                    break
            if not new_dev:
                raise LvolNotConnectException("LVOL did not connect")

            self.lvol_mount_details[lvol_name]["Device"] = new_dev
            self.ssh_obj.format_disk(node=client_node, device=new_dev, fs_type=fs_type)

            mnt = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=client_node, device=new_dev, mount_path=mnt)
            self.lvol_mount_details[lvol_name]["Mount"] = mnt

            # clean old logs
            self.ssh_obj.delete_files(client_node, [
                f"{mnt}/*fio*",
                f"{self.log_path}/local-{lvol_name}_fio*",
                f"{self.log_path}/{lvol_name}_fio_iolog*"
            ])

    def _seed_snapshots_and_clones(self):
        """Create one snapshot and one clone per lvol (best effort). Mount clones on same client."""
        for lvol, det in list(self.lvol_mount_details.items()):
            # Ensure ACTIVE
            self._wait_cluster_active()

            snap_name = f"{self.snap_base}_{_rand_id(8, first_alpha=False)}"
            out, err = self.ssh_obj.add_snapshot(self.mgmt_nodes[0], det["ID"], snap_name)
            if "(False," in str(out) or "(False," in str(err):
                self.logger.warning(f"[LFNG] snapshot create failed for {lvol} → skip clone")
                continue

            self.snapshot_names.append(snap_name)
            node_id = self.sbcli_utils.get_lvol_details(lvol_id=det["ID"])[0]["node_id"]
            self.snap_vs_node[snap_name] = node_id
            det["snapshots"].append(snap_name)

            snap_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snap_name)
            clone_name = f"{self.clone_base}_{_rand_id(8, first_alpha=False)}"
            try:
                self.ssh_obj.add_clone(self.mgmt_nodes[0], snap_id, clone_name)
            except Exception as e:
                self.logger.warning(f"[LFNG] clone create failed for {lvol} → {e}")
                continue

            # connect clone
            fs_type = det["FS"]
            client = det["Client"]

            self.clone_mount_details[clone_name] = {
                "ID": self.sbcli_utils.get_lvol_id(clone_name),
                "Command": None,
                "Mount": None,
                "Device": None,
                "MD5": None,
                "FS": fs_type,
                "Log": f"{self.log_path}/{clone_name}.log",
                "snapshot": snap_name,
                "Client": client,
                "iolog_base_path": f"{self.log_path}/{clone_name}_fio_iolog",
            }

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            self.clone_mount_details[clone_name]["Command"] = connect_ls

            initial = self.ssh_obj.get_devices(node=client)
            for c in connect_ls:
                _, err = self.ssh_obj.exec_command(node=client, command=c)
                if err:
                    nqn = self.sbcli_utils.get_lvol_details(lvol_id=self.clone_mount_details[clone_name]["ID"])[0]["nqn"]
                    self.ssh_obj.disconnect_nvme(node=client, nqn_grep=nqn)
                    self.logger.info("[LFNG] connect clone error → cleanup")
                    self.sbcli_utils.delete_lvol(lvol_name=clone_name, max_attempt=20, skip_error=True)
                    sleep_n_sec(3)
                    del self.clone_mount_details[clone_name]
                    continue

            final = self.ssh_obj.get_devices(node=client)
            new_dev = None
            for d in final:
                if d not in initial:
                    new_dev = f"/dev/{d.strip()}"
                    break
            if not new_dev:
                raise LvolNotConnectException("Clone did not connect")

            self.clone_mount_details[clone_name]["Device"] = new_dev
            if fs_type == "xfs":
                self.ssh_obj.clone_mount_gen_uuid(client, new_dev)
            mnt = f"{self.mount_path}/{clone_name}"
            self.ssh_obj.mount_path(node=client, device=new_dev, mount_path=mnt)
            self.clone_mount_details[clone_name]["Mount"] = mnt

            # purge old logs
            self.ssh_obj.delete_files(client, [
                f"{self.log_path}/local-{clone_name}_fio*",
                f"{self.log_path}/{clone_name}_fio_iolog*",
                f"{mnt}/*fio*"
            ])

    def _kick_fio_for_all(self, runtime=None):
        """Start verified fio (PID-checked; auto-rerun) for all lvols + clones."""
        # small stagger to avoid SSH bursts
        def _launch(name, det):
            self.ssh_obj.run_fio_test(
                det["Client"], None, det["Mount"], det["Log"],
                size=self.fio_size, name=f"{name}_fio", rw="randrw",
                bs=self._short_bs(), nrfiles=8, iodepth=1, numjobs=2,
                time_based=True, runtime=runtime, log_avg_msec=1000,
                iolog_file=det["iolog_base_path"], max_latency="30s",
                verify="md5", verify_dump=1, verify_fatal=1, retries=6,
                use_latency=False
            )

        for lvol, det in self.lvol_mount_details.items():
            self.ssh_obj.delete_files(det["Client"], [f"/mnt/{lvol}/*"])
            t = threading.Thread(target=_launch, args=(lvol, det))
            t.start()
            self.fio_threads.append(t)
            sleep_n_sec(0.2)

        for cname, det in self.clone_mount_details.items():
            self.ssh_obj.delete_files(det["Client"], [f"/mnt/{cname}/*"])
            t = threading.Thread(target=_launch, args=(cname, det))
            t.start()
            self.fio_threads.append(t)
            sleep_n_sec(0.2)

    # ---------- outage flow ----------

    def _perform_outage(self):
        random.shuffle(self.sn_nodes)
        self.current_outage_node = self.sn_nodes[0]
        outage_type = self._pick_outage()

        if self.first_outage_ts is None:
            self.first_outage_ts = int(datetime.now().timestamp())

        cur_node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        cur_node_ip = cur_node_details[0]["mgmt_ip"]
        self.ssh_obj.fetch_distrib_logs(
            storage_node_ip=cur_node_ip,
            storage_node_id=self.current_outage_node,
            logs_path=self.docker_logs_path,
            validate_async=True,
            error_sink=self.dump_validation_errors
        )

        threading.Thread(
            target=self.ssh_obj.dump_lvstore,
            kwargs={"node_ip": self.mgmt_nodes[0],
                    "storage_node_id": self.current_outage_node},
            daemon=True,
        ).start()

        self.outage_start_time = int(datetime.now().timestamp())
        self._log_outage_event(self.current_outage_node, outage_type, "Outage started")
        self.logger.info(f"[LFNG] Outage={outage_type} node={self.current_outage_node}")

        node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        node_ip = node_details[0]["mgmt_ip"]
        node_rpc_port = node_details[0]["rpc_port"]

        if outage_type == "graceful_shutdown":
            self.logger.info(f"Issuing graceful shutdown (no --force) for node {self.current_outage_node}.")
            deadline = time.time() + 300  # 5 minutes
            while True:
                try:
                    self.sbcli_utils.shutdown_node(node_uuid=self.current_outage_node, force=False)
                except Exception as e:
                    self.logger.warning(f"shutdown_node raised (may already be shutting down): {e}")
                sleep_n_sec(20)
                node_detail = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
                if node_detail[0]["status"] == "offline":
                    self.logger.info(f"Node {self.current_outage_node} is offline.")
                    break
                if time.time() >= deadline:
                    raise RuntimeError(
                        f"Node {self.current_outage_node} did not go offline within 5 minutes of graceful shutdown."
                    )
                self.logger.info(f"Node {self.current_outage_node} not yet offline; retrying shutdown...")

            for node in self.sn_nodes_with_sec:
                if node != self.current_outage_node:
                    cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                    cur_node_ip = cur_node_details[0]["mgmt_ip"]
                    self.ssh_obj.fetch_distrib_logs(
                        storage_node_ip=cur_node_ip,
                        storage_node_id=node,
                        logs_path=self.docker_logs_path,
                        validate_async=True,
                        error_sink=self.dump_validation_errors
                    )
                    threading.Thread(
                        target=self.ssh_obj.dump_lvstore,
                        kwargs={"node_ip": self.mgmt_nodes[0], "storage_node_id": node},
                        daemon=True,
                    ).start()
            # Keep node strictly offline for 5 minutes
            # sleep_n_sec(60)

        elif outage_type == "container_stop":
            if self.k8s_test and self.k8s_utils:
                self.k8s_utils.stop_spdk_pod(node_ip)
            else:
                self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
            self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, ["offline", "unreachable"], timeout=900)
            for node in self.sn_nodes_with_sec:
                if node != self.current_outage_node:
                    cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                    cur_node_ip = cur_node_details[0]["mgmt_ip"]
                    self.ssh_obj.fetch_distrib_logs(
                        storage_node_ip=cur_node_ip,
                        storage_node_id=node,
                        logs_path=self.docker_logs_path,
                        validate_async=True,
                        error_sink=self.dump_validation_errors
                    )
                    threading.Thread(
                        target=self.ssh_obj.dump_lvstore,
                        kwargs={"node_ip": self.mgmt_nodes[0], "storage_node_id": node},
                        daemon=True,
                    ).start()

        elif outage_type == "interface_full_network_interrupt":
            # Before cutting the network: start local-tmp logging alongside NFS logging
            # so logs are preserved even when the NFS mount becomes unreachable.
            if not self.k8s_test and node_ip in self.container_nodes:
                ts = int(datetime.now().timestamp())
                local_log_dir = f"/tmp/outage_logs/{self.current_outage_node}_{ts}"
                self.ssh_obj.start_local_docker_logging(
                    node_ip,
                    self.container_nodes[node_ip],
                    local_log_dir,
                    self.test_name,
                )
                self._local_outage_log_dirs[self.current_outage_node] = (node_ip, local_log_dir)
                self.logger.info(
                    f"[LFNG] Local logging started alongside NFS on {node_ip} → {local_log_dir}"
                )

            # Down all active data interfaces for ~300s (5 minutes) with ping verification
            active = self.ssh_obj.get_active_interfaces(node_ip)
            self.ssh_obj.disconnect_all_active_interfaces(node_ip, active, 300)
            for node in self.sn_nodes_with_sec:
                if node != self.current_outage_node:
                    cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                    cur_node_ip = cur_node_details[0]["mgmt_ip"]
                    self.ssh_obj.fetch_distrib_logs(
                        storage_node_ip=cur_node_ip,
                        storage_node_id=node,
                        logs_path=self.docker_logs_path,
                        validate_async=True,
                        error_sink=self.dump_validation_errors
                    )
            sleep_n_sec(200)
            try:
                self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, ["offline", "down"], timeout=130)
            except Exception as _:
                self.logger.info("Node might have not aborted! Checking if online.")

        return outage_type

    def restart_nodes_after_failover(self, outage_type):

        self.logger.info(f"[LFNG] Recover outage={outage_type} node={self.current_outage_node}")

        cur_node_details = self.sbcli_utils.get_storage_node_details(self.sn_primary_secondary_map[self.current_outage_node])
        cur_node_ip = cur_node_details[0]["mgmt_ip"]
        self.ssh_obj.fetch_distrib_logs(
            storage_node_ip=cur_node_ip,
            storage_node_id=self.sn_primary_secondary_map[self.current_outage_node],
            logs_path=self.docker_logs_path,
            validate_async=True,
            error_sink=self.dump_validation_errors
        )
        threading.Thread(
            target=self.ssh_obj.dump_lvstore,
            kwargs={"node_ip": self.mgmt_nodes[0],
                    "storage_node_id": self.sn_primary_secondary_map[self.current_outage_node]},
            daemon=True,
        ).start()

        # Only wait for ONLINE (skip deep health)
        if outage_type == 'graceful_shutdown':
            try:
                if self.k8s_test:
                    self.sbcli_utils.restart_node(node_uuid=self.current_outage_node, force=True)
                else:
                    self.ssh_obj.restart_node(self.mgmt_nodes[0], node_id=self.current_outage_node, force=True)
            except Exception:
                pass
            self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "online", timeout=900)
        elif outage_type == 'container_stop':
            self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "online", timeout=900)
        elif "network_interrupt" in outage_type:
            self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "online", timeout=900)

        self._log_outage_event(self.current_outage_node, outage_type, "Node online")
        self.outage_end_time = int(datetime.now().timestamp())

        # If we started local logging before a network outage, copy it to NFS now
        if self.current_outage_node in self._local_outage_log_dirs:
            _nip, _local_dir = self._local_outage_log_dirs.pop(self.current_outage_node)
            nfs_target = os.path.join(self.docker_logs_path, _nip, "local_logs")
            self.ssh_obj.flush_local_logs_to_nfs(_nip, _local_dir, nfs_target)
            self.logger.info(f"[LFNG] Flushed local outage logs to NFS: {nfs_target}")

        cur_node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        cur_node_ip = cur_node_details[0]["mgmt_ip"]
        self.ssh_obj.fetch_distrib_logs(
            storage_node_ip=cur_node_ip,
            storage_node_id=self.current_outage_node,
            logs_path=self.docker_logs_path,
            validate_async=True,
            error_sink=self.dump_validation_errors
        )
        threading.Thread(
            target=self.ssh_obj.dump_lvstore,
            kwargs={"node_ip": self.mgmt_nodes[0],
                    "storage_node_id": self.current_outage_node},
            daemon=True,
        ).start()

        # keep container log streaming going
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

        # small cool-down before next outage to reduce SSH churn
        # sleep_n_sec(random.randint(30, 60))

    # ---------- main ----------

    def run(self):
        self.logger.info("[LFNG] Starting RandomRapidFailoverNoGap")

        # Fabric detection
        cluster_details = self.sbcli_utils.get_cluster_details()
        fabric_rdma = cluster_details.get("fabric_rdma", False)
        fabric_tcp = cluster_details.get("fabric_tcp", True)
        if fabric_rdma and fabric_tcp:
            self.available_fabrics = ["tcp", "rdma"]
        elif fabric_rdma:
            self.available_fabrics = ["rdma"]
        else:
            self.available_fabrics = ["tcp"]

        # K8s: initialise pod-management utils + restrict outage types
        if self.k8s_test:
            from utils.k8s_utils import K8sUtils
            self.k8s_utils = K8sUtils(
                ssh_obj=self.ssh_obj,
                mgmt_node=self.mgmt_nodes[0],
            )
            self.outage_types = [t for t in self.outage_types if "network_interrupt" not in t]
            self.logger.info(f"[LFNG] K8s mode — outage types: {self.outage_types}")

        self.logger.info(f"[LFNG] fabrics={self.available_fabrics}")

        self._bootstrap_cluster()
        sleep_n_sec(5)

        iteration = 1
        while True:
            if self.dump_validation_errors:
                raise RuntimeError(
                    f"Placement dump validation failed: {self.dump_validation_errors}"
                )
            outage_type = self._perform_outage()
            self.restart_nodes_after_failover(outage_type)

            self._iter += 1
            if self._iter % self.validate_every == 0:
                self.logger.info(f"[LFNG] {self._iter} outages → wait & validate all FIO")
                # Join launch threads so we know all jobs issued
                for t in self.fio_threads:
                    t.join(timeout=10)
                self.fio_threads = []

                # Wait for all fio jobs to end (they’re 30min jobs)
                self.common_utils.manage_fio_threads(self.fio_node, [], timeout=self._fio_wait_timeout)

                for node in self.sn_nodes_with_sec:
                    cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                    cur_node_ip = cur_node_details[0]["mgmt_ip"]
                    self.ssh_obj.fetch_distrib_logs(
                        storage_node_ip=cur_node_ip,
                        storage_node_id=node,
                        logs_path=self.docker_logs_path,
                        validate_async=True,
                        error_sink=self.dump_validation_errors
                    )

                    self.ssh_obj.dump_lvstore(node_ip=self.mgmt_nodes[0],
                                              storage_node_id=node)

                # Validate logs
                for lvol, det in self.lvol_mount_details.items():
                    self.common_utils.validate_fio_test(det["Client"], log_file=det["Log"])
                for cname, det in self.clone_mount_details.items():
                    self.common_utils.validate_fio_test(det["Client"], log_file=det["Log"])

                # Optional: wait for migration window after FIO completes
                # (replace with your actual migration-check, if any)
                self.logger.info("[LFNG] FIO validated; pausing briefly for migration window")
                sleep_n_sec(10)

                # Re-kick next 30min wave
                self._kick_fio_for_all(runtime=self._per_wave_fio_runtime)
                self.logger.info("[LFNG] Next FIO wave started")

            self.logger.info(f"[LFNG] Iter {iteration} complete → starting next outage ASAP")
            iteration += 1


class RandomRapidFailoverNoGapV2(RandomRapidFailoverNoGap):
    """
    Improved successor to RandomRapidFailoverNoGap.  All V1 infrastructure
    (bootstrap, FIO management, snapshot/clone, outage logging) is reused.

    New in V2
    ---------
    - Fabric auto-detection: reads cluster fabric_rdma / fabric_tcp flags and
      assigns fabric ("tcp" | "rdma") randomly per lvol.
    - ndcs / npcs taken from self.ndcs / self.npcs (CLI kwargs, same for
      all lvols in the run).  Custom per-lvol geometry is a future addition.
    - ft-aware outage node selection:
        ft >= 2   → any node (cluster can survive two simultaneous failures)
        ft <  2 + self.npcs >= 2 → avoid the secondary partner of the last
                    outaged node so consecutive outages don’t threaten the
                    same replica group back-to-back.
    - Restart with 4-attempt retry; final attempt uses force=True.
    - Outage pacing: 30–60 s gap between outage-end and next-outage-start
      (V1 used 18–30 s).
    - K8s support:
        • container_stop → kubectl delete SPDK pod (via K8sUtils)
        • interface_full_network_interrupt removed from outage pool
        • graceful_shutdown restart uses sbcli_utils.restart_node
        • logging restarts use runner_k8s_log.restart_logging()
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "longfio_nochurn_rapid_outages_v2"
        self.available_fabrics = ["tcp"]   # overwritten in run()
        self.max_fault_tolerance = 1       # overwritten in run()
        self._last_outage_node = None      # for non-related node selection
        self.k8s_utils = None              # initialised in run() when k8s_test=True

    # ── helper: ft-aware single-node selection ───────────────────────────────

    def _pick_outage_node_v2(self):
        """Return one node UUID to outage, respecting fault-tolerance rules.

        - ft >= 2    : any node is safe
        - ft <  2 + self.npcs >= 2 : avoid the secondary partner of the last
          outaged node so the same replica group isn’t hit back-to-back.
        """
        all_nodes = self.sn_nodes[:]
        random.shuffle(all_nodes)

        if self.max_fault_tolerance >= 2:
            return all_nodes[0]

        if self.npcs >= 2 and self._last_outage_node:
            partner = self.sn_primary_secondary_map.get(self._last_outage_node)
            avoid = {self._last_outage_node}
            if partner:
                avoid.add(partner)
            candidates = [n for n in all_nodes if n not in avoid]
            if candidates:
                return candidates[0]

        return all_nodes[0]

    # ── Override 1: lvol creation — fabric + ndcs/npcs ───────────────────────

    def _create_lvols(self, count=1):
        for _ in range(count):
            fs_type = random.choice(["ext4", "xfs"])
            is_crypto = random.choice([True, False])
            fabric = random.choice(self.available_fabrics)
            ndcs, npcs_geom = self.ndcs, self.npcs

            name_core = f"{self.lvol_base}_{_rand_id(6, first_alpha=False)}"
            lvol_name = name_core if not is_crypto else f"c{name_core}"

            self.logger.info(
                f"[V2] Creating lvol {lvol_name!r}: fs={fs_type}, crypto={is_crypto}, "
                f"fabric={fabric}, ndcs={ndcs}, npcs={npcs_geom}"
            )

            add_kwargs = dict(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                crypto=is_crypto,
                key1=self.lvol_crypt_keys[0],
                key2=self.lvol_crypt_keys[1],
                fabric=fabric,
                distr_ndcs=ndcs,
                distr_npcs=npcs_geom,
            )

            # Avoid outage node & partner during initial placement
            if self.current_outage_node:
                skip_nodes = [
                    self.current_outage_node,
                    self.sn_primary_secondary_map.get(self.current_outage_node),
                ]
                skip_nodes += [
                    p for p, s in self.sn_primary_secondary_map.items()
                    if s == self.current_outage_node
                ]
                host_id = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
                if host_id:
                    add_kwargs["host_id"] = host_id[0]

            self._wait_cluster_active()
            try:
                self.sbcli_utils.add_lvol(**add_kwargs)
            except Exception as e:
                self.logger.warning(
                    f"[V2] lvol create failed ({lvol_name}, ndcs={ndcs}, npcs={npcs_geom}): {e}; "
                    f"retrying with cluster default geometry"
                )
                add_kwargs["distr_ndcs"] = 0
                add_kwargs["distr_npcs"] = 0
                self._wait_cluster_active()
                try:
                    self.sbcli_utils.add_lvol(**add_kwargs)
                except Exception as e2:
                    self.logger.warning(f"[V2] Retry lvol create failed ({lvol_name}): {e2}; skipping")
                    continue

            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            self.lvol_mount_details[lvol_name] = {
                "ID": lvol_id,
                "Command": None,
                "Mount": None,
                "Device": None,
                "MD5": None,
                "FS": fs_type,
                "Log": f"{self.log_path}/{lvol_name}.log",
                "snapshots": [],
                "iolog_base_path": f"{self.log_path}/{lvol_name}_fio_iolog",
            }

            self.ssh_obj.exec_command(
                node=self.mgmt_nodes[0],
                command=f"{self.base_cmd} lvol list",
                supress_logs=True,
            )

            lvol_node_id = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]["node_id"]
            self.node_vs_lvol.setdefault(lvol_node_id, []).append(lvol_name)

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            self.lvol_mount_details[lvol_name]["Command"] = connect_ls

            client_node = random.choice(self.fio_node)
            self.lvol_mount_details[lvol_name]["Client"] = client_node

            initial = self.ssh_obj.get_devices(node=client_node)
            for c in connect_ls:
                _, err = self.ssh_obj.exec_command(node=client_node, command=c)
                if err:
                    nqn = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]["nqn"]
                    self.ssh_obj.disconnect_nvme(node=client_node, nqn_grep=nqn)
                    self.logger.info(f"[V2] connect error → cleaning lvol {lvol_name}")
                    self.sbcli_utils.delete_lvol(lvol_name=lvol_name, max_attempt=20, skip_error=True)
                    sleep_n_sec(3)
                    del self.lvol_mount_details[lvol_name]
                    self.node_vs_lvol[lvol_node_id].remove(lvol_name)
                    break

            if lvol_name not in self.lvol_mount_details:
                continue

            final = self.ssh_obj.get_devices(node=client_node)
            new_dev = None
            for d in final:
                if d not in initial:
                    new_dev = f"/dev/{d.strip()}"
                    break
            if not new_dev:
                raise LvolNotConnectException(f"[V2] LVOL {lvol_name!r} did not connect")

            self.lvol_mount_details[lvol_name]["Device"] = new_dev
            self.ssh_obj.format_disk(node=client_node, device=new_dev, fs_type=fs_type)

            mnt = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=client_node, device=new_dev, mount_path=mnt)
            self.lvol_mount_details[lvol_name]["Mount"] = mnt

            self.ssh_obj.delete_files(client_node, [
                f"{mnt}/*fio*",
                f"{self.log_path}/local-{lvol_name}_fio*",
                f"{self.log_path}/{lvol_name}_fio_iolog*",
            ])

    # ── Override 2: outage — ft-aware node selection + K8s ───────────────────

    def _perform_outage(self):
        self.current_outage_node = self._pick_outage_node_v2()

        # K8s: drop network-interrupt from the eligible pool
        available_types = list(self.outage_types)
        if self.k8s_test:
            available_types = [t for t in available_types if "network_interrupt" not in t]
            if not available_types:
                available_types = ["graceful_shutdown"]
        random.shuffle(available_types)
        outage_type = available_types[0]

        if self.first_outage_ts is None:
            self.first_outage_ts = int(datetime.now().timestamp())

        cur_node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        cur_node_ip = cur_node_details[0]["mgmt_ip"]
        self.ssh_obj.fetch_distrib_logs(
            storage_node_ip=cur_node_ip,
            storage_node_id=self.current_outage_node,
            logs_path=self.docker_logs_path,
            validate_async=True,
            error_sink=self.dump_validation_errors,
        )
        threading.Thread(
            target=self.ssh_obj.dump_lvstore,
            kwargs={"node_ip": self.mgmt_nodes[0],
                    "storage_node_id": self.current_outage_node},
            daemon=True,
        ).start()

        self.outage_start_time = int(datetime.now().timestamp())
        self._log_outage_event(self.current_outage_node, outage_type, "Outage started")
        self.logger.info(
            f"[V2] Outage={outage_type} node={self.current_outage_node} "
            f"ft={self.max_fault_tolerance}"
        )

        node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        node_ip = node_details[0]["mgmt_ip"]
        node_rpc_port = node_details[0]["rpc_port"]

        def _fetch_others():
            for node in self.sn_nodes_with_sec:
                if node == self.current_outage_node:
                    continue
                nd = self.sbcli_utils.get_storage_node_details(node)
                self.ssh_obj.fetch_distrib_logs(
                    storage_node_ip=nd[0]["mgmt_ip"],
                    storage_node_id=node,
                    logs_path=self.docker_logs_path,
                    validate_async=True,
                    error_sink=self.dump_validation_errors,
                )
                threading.Thread(
                    target=self.ssh_obj.dump_lvstore,
                    kwargs={"node_ip": self.mgmt_nodes[0], "storage_node_id": node},
                    daemon=True,
                ).start()

        if outage_type == "graceful_shutdown":
            deadline = time.time() + 300
            while True:
                try:
                    self.sbcli_utils.shutdown_node(node_uuid=self.current_outage_node, force=False)
                except Exception as e:
                    self.logger.warning(f"[V2] shutdown_node raised: {e}")
                sleep_n_sec(30)
                nd = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
                if nd[0]["status"] == "offline":
                    self.logger.info(f"[V2] Node {self.current_outage_node} offline.")
                    break
                if time.time() >= deadline:
                    raise RuntimeError(
                        f"[V2] Node {self.current_outage_node} did not go offline within 5 min."
                    )
                self.logger.info(f"[V2] Node {self.current_outage_node} not yet offline; retrying...")
            _fetch_others()
            # sleep_n_sec(60)

        elif outage_type == "container_stop":
            if self.k8s_test and self.k8s_utils:
                self.k8s_utils.stop_spdk_pod(node_ip)
            else:
                self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
            self.sbcli_utils.wait_for_storage_node_status(
                self.current_outage_node, ["offline", "unreachable"], timeout=900
            )
            _fetch_others()

        elif outage_type == "interface_full_network_interrupt":
            if not self.k8s_test and node_ip in self.container_nodes:
                ts = int(datetime.now().timestamp())
                local_log_dir = f"/tmp/outage_logs/{self.current_outage_node}_{ts}"
                self.ssh_obj.start_local_docker_logging(
                    node_ip,
                    self.container_nodes[node_ip],
                    local_log_dir,
                    self.test_name,
                )
                self._local_outage_log_dirs[self.current_outage_node] = (node_ip, local_log_dir)

            # 30 s  → network blip; SPDK stays running, tests reconnect path
            # 300 s → triggers SPDK abort, tests full restart/recovery
            # 600 s → same as 300 s but longer outage window
            outage_dur = random.choice([30, 300, 600])
            outage_type = f"interface_full_network_interrupt_{outage_dur}sec"
            self.logger.info(f"[V2] Network outage duration: {outage_dur}s")

            active = self.ssh_obj.get_active_interfaces(node_ip)
            self.ssh_obj.disconnect_all_active_interfaces(node_ip, active, outage_dur)
            # disconnect_all_active_interfaces blocks for outage_dur seconds then reconnects

            _fetch_others()

            if outage_dur >= 300:
                # Long outage: SPDK should have aborted; wait for offline detection
                sleep_n_sec(200)
                try:
                    self.sbcli_utils.wait_for_storage_node_status(
                        self.current_outage_node, ["offline", "down"], timeout=130
                    )
                except Exception:
                    self.logger.info("[V2] Node may not have aborted; will check in recovery.")
            else:
                # Short blip: SPDK likely stayed alive; brief stabilisation wait
                sleep_n_sec(30)

        return outage_type

    # ── Override 3: restart — 4-retry + 30–60 s gap + K8s ───────────────────

    def restart_nodes_after_failover(self, outage_type):
        self.logger.info(f"[V2] Recovering outage={outage_type} node={self.current_outage_node}")

        partner = self.sn_primary_secondary_map.get(self.current_outage_node)
        if partner:
            nd = self.sbcli_utils.get_storage_node_details(partner)
            self.ssh_obj.fetch_distrib_logs(
                storage_node_ip=nd[0]["mgmt_ip"],
                storage_node_id=partner,
                logs_path=self.docker_logs_path,
                validate_async=True,
                error_sink=self.dump_validation_errors,
            )
            threading.Thread(
                target=self.ssh_obj.dump_lvstore,
                kwargs={"node_ip": self.mgmt_nodes[0], "storage_node_id": partner},
                daemon=True,
            ).start()

        if outage_type == "graceful_shutdown":
            max_retries = 4
            retry_delay = 10
            for attempt in range(max_retries):
                # Restart container logging before each restart attempt
                if not self.k8s_test:
                    for node in self.storage_nodes:
                        self.ssh_obj.restart_docker_logging(
                            node_ip=node,
                            containers=self.container_nodes[node],
                            log_dir=os.path.join(self.docker_logs_path, node),
                            test_name=self.test_name,
                        )
                else:
                    self.runner_k8s_log.restart_logging()
                try:
                    force = (attempt == max_retries - 1)
                    if force:
                        self.logger.info("[V2] Restart node with force=True (last attempt).")
                    else:
                        self.logger.info(f"[V2] Restart node attempt {attempt + 1}/{max_retries}.")
                    if self.k8s_test:
                        self.sbcli_utils.restart_node(
                            node_uuid=self.current_outage_node, force=force
                        )
                    else:
                        self.ssh_obj.restart_node(
                            node=self.mgmt_nodes[0],
                            node_id=self.current_outage_node,
                            force=force,
                        )
                    if not self.k8s_test:
                        for node in self.storage_nodes:
                            self.ssh_obj.restart_docker_logging(
                                node_ip=node,
                                containers=self.container_nodes[node],
                                log_dir=os.path.join(self.docker_logs_path, node),
                                test_name=self.test_name,
                            )
                    else:
                        self.runner_k8s_log.restart_logging()
                    self.sbcli_utils.wait_for_storage_node_status(
                        self.current_outage_node, "online", timeout=300
                    )
                    break
                except Exception:
                    if attempt < max_retries - 1:
                        self.logger.info(
                            f"[V2] Restart attempt {attempt + 1} failed; retrying in {retry_delay}s…"
                        )
                        sleep_n_sec(retry_delay)
                    else:
                        self.logger.info("[V2] All restart attempts exhausted.")
                        raise
            # Safety-net wait
            self.sbcli_utils.wait_for_storage_node_status(
                self.current_outage_node, "online", timeout=300
            )

        elif outage_type == "container_stop":
            self.sbcli_utils.wait_for_storage_node_status(
                self.current_outage_node, "online", timeout=900
            )

        elif "network_interrupt" in outage_type:
            self.sbcli_utils.wait_for_storage_node_status(
                self.current_outage_node, "online", timeout=900
            )

        self._log_outage_event(self.current_outage_node, outage_type, "Node online")
        self.outage_end_time = int(datetime.now().timestamp())
        self._last_outage_node = self.current_outage_node

        # Flush local network-outage logs to NFS
        if self.current_outage_node in self._local_outage_log_dirs:
            _nip, _local_dir = self._local_outage_log_dirs.pop(self.current_outage_node)
            nfs_target = os.path.join(self.docker_logs_path, _nip, "local_logs")
            self.ssh_obj.flush_local_logs_to_nfs(_nip, _local_dir, nfs_target)
            self.logger.info(f"[V2] Flushed local outage logs to NFS: {nfs_target}")

        nd = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        self.ssh_obj.fetch_distrib_logs(
            storage_node_ip=nd[0]["mgmt_ip"],
            storage_node_id=self.current_outage_node,
            logs_path=self.docker_logs_path,
            validate_async=True,
            error_sink=self.dump_validation_errors,
        )
        threading.Thread(
            target=self.ssh_obj.dump_lvstore,
            kwargs={"node_ip": self.mgmt_nodes[0],
                    "storage_node_id": self.current_outage_node},
            daemon=True,
        ).start()

        # Restart container log streaming
        if not self.k8s_test:
            for node in self.storage_nodes:
                self.ssh_obj.restart_docker_logging(
                    node_ip=node,
                    containers=self.container_nodes[node],
                    log_dir=os.path.join(self.docker_logs_path, node),
                    test_name=self.test_name,
                )
        else:
            self.runner_k8s_log.restart_logging()

        # 30–60 s gap: more than V1’s 18–30 s, less than 1 minute
        # gap = random.randint(30, 60)
        # self.logger.info(f"[V2] Outage resolved; waiting {gap}s before next outage.")
        # sleep_n_sec(gap)

    # ── Override 4: dual (simultaneous) outage support ───────────────────────

    def _pick_non_related_pair(self):
        """Return (node_a, node_b) where neither is the other's secondary partner,
        or None if no such pair exists.

        Used when npcs >= 2 but ft < 2: each replica group must keep at least
        one node alive, so the two chosen nodes must be from different groups.
        """
        all_nodes = self.sn_nodes[:]
        random.shuffle(all_nodes)
        for i, node_a in enumerate(all_nodes):
            sec_a = self.sn_primary_secondary_map.get(node_a)
            related = {node_a}
            if sec_a:
                related.add(sec_a)
            # Also nodes whose secondary IS node_a
            for p, s in self.sn_primary_secondary_map.items():
                if s == node_a:
                    related.add(p)
            for node_b in all_nodes[i + 1:]:
                if node_b not in related:
                    return node_a, node_b
        return None

    def _pick_outage_pair(self):
        """Return (node_a, node_b) for a simultaneous dual outage, or None for single outage.

        Decision tree:
          npcs < 2              → single outage only (only 1 secondary per primary)
          npcs >= 2, ft >= 2    → any two nodes (cluster survives any 2 simultaneous failures)
          npcs >= 2, ft < 2     → non-related nodes only (different replica groups)
        """
        if self.npcs < 2:
            return None

        all_nodes = self.sn_nodes[:]
        random.shuffle(all_nodes)

        if self.max_fault_tolerance >= 2:
            # Any two nodes are safe
            if len(all_nodes) >= 2:
                return all_nodes[0], all_nodes[1]
            return None

        # ft < 2: must pick nodes from different replica groups
        return self._pick_non_related_pair()

    def _execute_outage_for_node(self, node_uuid, result_dict, errors_list):
        """Thread worker: perform one outage on node_uuid.
        Stores the final outage_type string in result_dict[node_uuid].
        """
        try:
            nd = self.sbcli_utils.get_storage_node_details(node_uuid)
            node_ip = nd[0]["mgmt_ip"]
            node_rpc_port = nd[0]["rpc_port"]

            available_types = list(self.outage_types)
            if self.k8s_test:
                available_types = [t for t in available_types if "network_interrupt" not in t]
            random.shuffle(available_types)
            outage_type = available_types[0]

            self._log_outage_event(node_uuid, outage_type, "Outage started")
            self.logger.info(f"[V2-dual] Outage={outage_type} node={node_uuid}")

            self.ssh_obj.fetch_distrib_logs(
                storage_node_ip=node_ip,
                storage_node_id=node_uuid,
                logs_path=self.docker_logs_path,
                validate_async=True,
                error_sink=self.dump_validation_errors,
            )

            if outage_type == "graceful_shutdown":
                deadline = time.time() + 300
                while True:
                    try:
                        self.sbcli_utils.shutdown_node(node_uuid=node_uuid, force=False)
                    except Exception as e:
                        self.logger.warning(f"[V2-dual] shutdown_node raised for {node_uuid}: {e}")
                    sleep_n_sec(30)
                    node_status = self.sbcli_utils.get_storage_node_details(node_uuid)
                    if node_status[0]["status"] == "offline":
                        self.logger.info(f"[V2-dual] Node {node_uuid} offline.")
                        break
                    if time.time() >= deadline:
                        raise RuntimeError(f"[V2-dual] {node_uuid} did not go offline within 5 min.")
                    self.logger.info(f"[V2-dual] {node_uuid} not offline yet; retrying...")
                sleep_n_sec(60)

            elif outage_type == "container_stop":
                if self.k8s_test and self.k8s_utils:
                    self.k8s_utils.stop_spdk_pod(node_ip)
                else:
                    self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
                self.sbcli_utils.wait_for_storage_node_status(
                    node_uuid, ["offline", "unreachable"], timeout=900
                )

            elif outage_type == "interface_full_network_interrupt":
                if not self.k8s_test and node_ip in self.container_nodes:
                    ts = int(datetime.now().timestamp())
                    local_log_dir = f"/tmp/outage_logs/{node_uuid}_{ts}"
                    self.ssh_obj.start_local_docker_logging(
                        node_ip, self.container_nodes[node_ip], local_log_dir, self.test_name
                    )
                    self._local_outage_log_dirs[node_uuid] = (node_ip, local_log_dir)

                outage_dur = random.choice([30, 300, 600])
                outage_type = f"interface_full_network_interrupt_{outage_dur}sec"
                self.logger.info(f"[V2-dual] NW outage {outage_dur}s on {node_uuid}")

                active = self.ssh_obj.get_active_interfaces(node_ip)
                self.ssh_obj.disconnect_all_active_interfaces(node_ip, active, outage_dur)

                if outage_dur >= 300:
                    sleep_n_sec(200)
                    try:
                        self.sbcli_utils.wait_for_storage_node_status(
                            node_uuid, ["offline", "down"], timeout=130
                        )
                    except Exception:
                        self.logger.info(f"[V2-dual] {node_uuid} may not have aborted.")
                else:
                    sleep_n_sec(30)

            result_dict[node_uuid] = outage_type

        except Exception as e:
            self.logger.error(f"[V2-dual] Outage error for {node_uuid}: {e}")
            errors_list.append((node_uuid, e))

    def _recover_node_after_failover(self, node_uuid, outage_type):
        """Thread worker: wait for node_uuid to come back online after an outage."""
        self.logger.info(f"[V2-dual] Recovering node={node_uuid} outage_type={outage_type}")

        partner = self.sn_primary_secondary_map.get(node_uuid)
        if partner:
            nd = self.sbcli_utils.get_storage_node_details(partner)
            self.ssh_obj.fetch_distrib_logs(
                storage_node_ip=nd[0]["mgmt_ip"],
                storage_node_id=partner,
                logs_path=self.docker_logs_path,
                validate_async=True,
                error_sink=self.dump_validation_errors,
            )

        if outage_type == "graceful_shutdown":
            max_retries = 4
            for attempt in range(max_retries):
                try:
                    force = (attempt == max_retries - 1)
                    if force:
                        self.logger.info(f"[V2-dual] Restart {node_uuid} with force=True (last attempt).")
                    if self.k8s_test:
                        self.sbcli_utils.restart_node(node_uuid=node_uuid, force=force)
                    else:
                        self.ssh_obj.restart_node(
                            node=self.mgmt_nodes[0], node_id=node_uuid, force=force
                        )
                    self.sbcli_utils.wait_for_storage_node_status(node_uuid, "online", timeout=300)
                    break
                except Exception:
                    if attempt < max_retries - 1:
                        sleep_n_sec(10)
                    else:
                        raise
            # Safety-net
            self.sbcli_utils.wait_for_storage_node_status(node_uuid, "online", timeout=300)

        elif outage_type == "container_stop":
            self.sbcli_utils.wait_for_storage_node_status(node_uuid, "online", timeout=900)

        elif "network_interrupt" in outage_type:
            self.sbcli_utils.wait_for_storage_node_status(node_uuid, "online", timeout=900)

        self._log_outage_event(node_uuid, outage_type, "Node online")

        # Flush local outage logs if any
        if node_uuid in self._local_outage_log_dirs:
            _nip, _local_dir = self._local_outage_log_dirs.pop(node_uuid)
            nfs_target = os.path.join(self.docker_logs_path, _nip, "local_logs")
            self.ssh_obj.flush_local_logs_to_nfs(_nip, _local_dir, nfs_target)
            self.logger.info(f"[V2-dual] Flushed local logs for {node_uuid}")

        nd = self.sbcli_utils.get_storage_node_details(node_uuid)
        self.ssh_obj.fetch_distrib_logs(
            storage_node_ip=nd[0]["mgmt_ip"],
            storage_node_id=node_uuid,
            logs_path=self.docker_logs_path,
            validate_async=True,
            error_sink=self.dump_validation_errors,
        )

    def _dual_outage_cycle(self, node_a, node_b):
        """Perform simultaneous outages on two non-related nodes then recover both in parallel."""
        self.logger.info(f"[V2-dual] Starting dual outage: {node_a} + {node_b}")
        if self.first_outage_ts is None:
            self.first_outage_ts = int(datetime.now().timestamp())
        self.outage_start_time = int(datetime.now().timestamp())

        result_dict = {}
        errors = []

        t_a = threading.Thread(
            target=self._execute_outage_for_node, args=(node_a, result_dict, errors)
        )
        t_b = threading.Thread(
            target=self._execute_outage_for_node, args=(node_b, result_dict, errors)
        )
        t_a.start()
        t_b.start()
        t_a.join()
        t_b.join()

        if errors:
            raise RuntimeError(f"[V2-dual] Outage thread errors: {errors}")

        # Recover both nodes in parallel
        rec_errors = []

        def _do_recover(node_uuid, ot):
            try:
                self._recover_node_after_failover(node_uuid, ot)
            except Exception as e:
                self.logger.error(f"[V2-dual] Recovery error for {node_uuid}: {e}")
                rec_errors.append((node_uuid, e))

        rec_threads = [
            threading.Thread(target=_do_recover, args=(nu, ot))
            for nu, ot in result_dict.items()
        ]
        for t in rec_threads:
            t.start()
        for t in rec_threads:
            t.join()

        if rec_errors:
            raise RuntimeError(f"[V2-dual] Recovery errors: {rec_errors}")

        self.outage_end_time = int(datetime.now().timestamp())
        self._last_outage_node = node_b

        # Restart container/k8s logging after all nodes are recovered
        if not self.k8s_test:
            for node in self.storage_nodes:
                self.ssh_obj.restart_docker_logging(
                    node_ip=node,
                    containers=self.container_nodes[node],
                    log_dir=os.path.join(self.docker_logs_path, node),
                    test_name=self.test_name,
                )
        else:
            self.runner_k8s_log.restart_logging()

        gap = random.randint(30, 60)
        self.logger.info(f"[V2-dual] Both nodes online; waiting {gap}s before next outage.")
        sleep_n_sec(gap)

        return result_dict

    # ── Override 5: run — read cluster config then delegate ──────────────────

    def run(self):
        self.logger.info("[V2] Starting RandomRapidFailoverNoGapV2")

        cluster_details = self.sbcli_utils.get_cluster_details()

        # Fabric detection
        fabric_rdma = cluster_details.get("fabric_rdma", False)
        fabric_tcp = cluster_details.get("fabric_tcp", True)
        if fabric_rdma and fabric_tcp:
            self.available_fabrics = ["tcp", "rdma"]
        elif fabric_rdma:
            self.available_fabrics = ["rdma"]
        else:
            self.available_fabrics = ["tcp"]

        # Fault tolerance
        self.max_fault_tolerance = cluster_details.get("max_fault_tolerance", 1)

        self.logger.info(
            f"[V2] fabrics={self.available_fabrics}, "
            f"ft={self.max_fault_tolerance}, npcs_cli={self.npcs}"
        )

        # K8s: initialise pod-management utils + restrict outage types
        if self.k8s_test:
            from utils.k8s_utils import K8sUtils
            self.k8s_utils = K8sUtils(
                ssh_obj=self.ssh_obj,
                mgmt_node=self.mgmt_nodes[0],
            )
            self.outage_types = [
                t for t in self.outage_types if "network_interrupt" not in t
            ]
            self.logger.info(f"[V2] K8s mode — outage types: {self.outage_types}")

        # Delegate to parent bootstrap + outage loop.
        # All overridden methods (_create_lvols, _perform_outage,
        # restart_nodes_after_failover) are dispatched through self.
        self._bootstrap_cluster()
        sleep_n_sec(5)

        iteration = 1
        while True:
            if self.dump_validation_errors:
                raise RuntimeError(
                    f"[V2] Placement dump validation failed: {self.dump_validation_errors}"
                )
            # Pick outage strategy based on npcs and ft:
            #   npcs < 2              → single (only 1 secondary, can't afford 2 simultaneous)
            #   npcs >= 2, ft >= 2    → dual any nodes
            #   npcs >= 2, ft < 2     → dual non-related nodes only
            pair = self._pick_outage_pair()
            if pair:
                self.logger.info(
                    f"[V2] Dual outage (npcs={self.npcs}, ft={self.max_fault_tolerance}): "
                    f"{pair[0]} + {pair[1]}"
                )
                self._dual_outage_cycle(*pair)
            else:
                self.logger.info(
                    f"[V2] Single outage (npcs={self.npcs}, ft={self.max_fault_tolerance})"
                )
                outage_type = self._perform_outage()
                self.restart_nodes_after_failover(outage_type)

            self._iter += 1
            if self._iter % self.validate_every == 0:
                self.logger.info(f"[V2] {self._iter} outages → wait & validate all FIO")
                for t in self.fio_threads:
                    t.join(timeout=10)
                self.fio_threads = []

                self.common_utils.manage_fio_threads(
                    self.fio_node, [], timeout=self._fio_wait_timeout
                )

                for node in self.sn_nodes_with_sec:
                    nd = self.sbcli_utils.get_storage_node_details(node)
                    self.ssh_obj.fetch_distrib_logs(
                        storage_node_ip=nd[0]["mgmt_ip"],
                        storage_node_id=node,
                        logs_path=self.docker_logs_path,
                        validate_async=True,
                        error_sink=self.dump_validation_errors,
                    )
                    self.ssh_obj.dump_lvstore(
                        node_ip=self.mgmt_nodes[0], storage_node_id=node
                    )

                for lvol, det in self.lvol_mount_details.items():
                    self.common_utils.validate_fio_test(det["Client"], log_file=det["Log"])
                for cname, det in self.clone_mount_details.items():
                    self.common_utils.validate_fio_test(det["Client"], log_file=det["Log"])

                self.logger.info("[V2] FIO validated; pausing briefly for migration window")
                sleep_n_sec(10)

                self._kick_fio_for_all(runtime=self._per_wave_fio_runtime)
                self.logger.info("[V2] Next FIO wave started")

            self.logger.info(f"[V2] Iter {iteration} complete → starting next outage")
            iteration += 1