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
        self._per_wave_fio_runtime = 3600      # 60 minutes
        self._fio_wait_timeout = 5000          # wait for all to finish

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

        self.outage_types = [
            "graceful_shutdown",
            "container_stop",
            # "interface_full_network_interrupt",
        ]

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
            name_core = f"{self.lvol_base}_{_rand_id(6, first_alpha=False)}"
            lvol_name = name_core if not is_crypto else f"c{name_core}"

            kwargs = dict(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                crypto=is_crypto,
                key1=self.lvol_crypt_keys[0],
                key2=self.lvol_crypt_keys[1],
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
            logs_path=self.docker_logs_path
        )
        
        # self.ssh_obj.dump_lvstore(node_ip=self.mgmt_nodes[0],
        #                           storage_node_id=self.current_outage_node)

        self.outage_start_time = int(datetime.now().timestamp())
        self._log_outage_event(self.current_outage_node, outage_type, "Outage started")
        self.logger.info(f"[LFNG] Outage={outage_type} node={self.current_outage_node}")

        node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        node_ip = node_details[0]["mgmt_ip"]
        node_rpc_port = node_details[0]["rpc_port"]

        if outage_type == "graceful_shutdown":
            # suspend -> wait SUSPENDED -> shutdown -> wait OFFLINE
            try:
                self.logger.info(f"[LFNG] Suspending node via: sbcli-dev sn suspend {self.current_outage_node}")
                self.sbcli_utils.suspend_node(node_uuid=self.current_outage_node, expected_error_code=[503])
                self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "suspended", timeout=600)
            except Exception:
                self.logger.warning("[LFNG] Suspend failed from API; ignoring if already suspended")

            try:
                self.sbcli_utils.shutdown_node(node_uuid=self.current_outage_node, force=True, expected_error_code=[503])
            except Exception:
                self.ssh_obj.shutdown_node(node=self.mgmt_nodes[0], node_id=self.current_outage_node, force=True)
            self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "offline", timeout=900)

            for node in self.sn_nodes_with_sec:
                if node != self.current_outage_node:
                    cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                    cur_node_ip = cur_node_details[0]["mgmt_ip"]
                    self.ssh_obj.fetch_distrib_logs(
                        storage_node_ip=cur_node_ip,
                        storage_node_id=node,
                        logs_path=self.docker_logs_path
                    )
            # Keep node strictly offline for 5 minutes
            sleep_n_sec(500)

        elif outage_type == "container_stop":
            self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port)

        elif outage_type == "interface_full_network_interrupt":
            # Down all active data interfaces for ~300s (5 minutes) with ping verification
            active = self.ssh_obj.get_active_interfaces(node_ip)
            self.ssh_obj.disconnect_all_active_interfaces(node_ip, active, 300)
            sleep_n_sec(280)

        return outage_type

    def restart_nodes_after_failover(self, outage_type):

        self.logger.info(f"[LFNG] Recover outage={outage_type} node={self.current_outage_node}")

        cur_node_details = self.sbcli_utils.get_storage_node_details(self.sn_primary_secondary_map[self.current_outage_node])
        cur_node_ip = cur_node_details[0]["mgmt_ip"]
        self.ssh_obj.fetch_distrib_logs(
            storage_node_ip=cur_node_ip,
            storage_node_id=self.sn_primary_secondary_map[self.current_outage_node],
            logs_path=self.docker_logs_path
        )

        # Only wait for ONLINE (skip deep health)
        if outage_type == 'graceful_shutdown':
            try:
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

        cur_node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        cur_node_ip = cur_node_details[0]["mgmt_ip"]
        self.ssh_obj.fetch_distrib_logs(
            storage_node_ip=cur_node_ip,
            storage_node_id=self.current_outage_node,
            logs_path=self.docker_logs_path
        )

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
        # sleep_n_sec(random.randint(1, 5))

    # ---------- main ----------

    def run(self):
        self.logger.info("[LFNG] Starting RandomRapidFailoverNoGap")
        self._bootstrap_cluster()
        sleep_n_sec(5)

        iteration = 1
        while True:
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
                        logs_path=self.docker_logs_path
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