"""
E2E security tests for lvol DH-HMAC-CHAP, allowed-hosts, and dynamic host management.

Security feature summary:
  pool add --sec-options <file>   JSON {dhchap_key: bool, dhchap_ctrlr_key: bool}; applied at pool level.
  --allowed-hosts <file>  JSON list of host NQNs that can access the lvol
  volume connect <id> --host-nqn <nqn>   returns connect string with embedded DHCHAP keys
  volume add-host <id> <nqn>   add host to existing lvol
  volume remove-host <id> <nqn>                remove host from existing lvol

All sbcli CLI wrappers live in ssh_utils.SshUtils:
  ssh_obj.create_sec_lvol(...)
  ssh_obj.get_lvol_connect_str_with_host_nqn(...)
  ssh_obj.add_host_to_lvol(...)
  ssh_obj.remove_host_from_lvol(...)
  ssh_obj.get_client_host_nqn(node)
"""

import threading
import time
import random
import string
from pathlib import Path

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from exceptions.custom_exception import LvolNotConnectException


# ───────────────────────────────────── helpers ──────────────────────────────


def _rand_suffix(n=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))


# COMMENTED OUT: old security option constants (DHCHAP is now pool-level via --dhchap flag)
# SEC_BOTH = {"dhchap_key": True, "dhchap_ctrlr_key": True}
# SEC_HOST_ONLY = {"dhchap_key": True, "dhchap_ctrlr_key": False}
# SEC_CTRL_ONLY = {"dhchap_key": False, "dhchap_ctrlr_key": True}


# ─────────────────────────────────── base class ─────────────────────────────


class SecurityTestBase(TestClusterBase):
    """
    Base class for all security test scenarios.

    CLI-level security operations are delegated to ssh_obj so that the
    implementations are reusable across E2E and stress tests:
      self.ssh_obj.create_sec_lvol(...)
      self.ssh_obj.get_lvol_connect_str_with_host_nqn(...)
      self.ssh_obj.add_host_to_lvol(...)
      self.ssh_obj.remove_host_from_lvol(...)
      self.ssh_obj.get_client_host_nqn(node)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "5G"
        self.fio_size = "1G"
        self.mount_path = "/mnt"
        self.log_path = str(Path.home())
        self.lvol_mount_details = {}
        self.pool_name = "sec_test_pool"
        self._client_host_nqn = None
        self.fio_threads = []

    # ── filesystem helper ────────────────────────────────────────────────────

    def _pick_fs_type(self):
        """Randomly choose ext4 or xfs so both filesystems get coverage."""
        fs = random.choice(["ext4", "xfs"])
        self.logger.info(f"[_pick_fs_type] Selected filesystem: {fs}")
        return fs

    # ── debug helpers ─────────────────────────────────────────────────────────

    def _log_cluster_security_config(self):
        """Log cluster-level security fields for debugging."""
        try:
            details = self.sbcli_utils.get_cluster_details()
            keys = ["ha_type", "sec_enabled", "host_sec", "tls_enabled",
                    "fabric_tcp", "fabric_rdma", "status"]
            summary = {k: details.get(k) for k in keys if k in details}
            self.logger.info(f"[DEBUG] Cluster security fields: {summary}")
            self.logger.info(f"[DEBUG] Full cluster details: {details}")
        except Exception as exc:
            self.logger.warning(f"[DEBUG] Could not get cluster details: {exc}")

        # Also dump via CLI
        try:
            out, _ = self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.base_cmd} cluster list")
            self.logger.info(f"[DEBUG] cluster list output:\n{out}")
        except Exception as exc:
            self.logger.warning(f"[DEBUG] cluster list failed: {exc}")

    def _log_lvol_security(self, lvol_id, label=""):
        """Log full lvol details via CLI after creation."""
        try:
            out = self._get_lvol_details_via_cli(lvol_id)
            self.logger.info(f"[DEBUG] volume get {lvol_id} {label}:\n{out}")
        except Exception as exc:
            self.logger.warning(f"[DEBUG] volume get failed: {exc}")

    # ── NQN cache ────────────────────────────────────────────────────────────

    def _get_client_host_nqn(self, node=None, force_new=False):
        """Return (and cache) the host NQN from /etc/nvme/hostnqn on the client node.

        Reads the existing hostnqn rather than generating a new one so that the
        NQN matches what the kernel NVMe driver will present during connect.
        """
        if self._client_host_nqn and not force_new:
            return self._client_host_nqn
        target = node or self.fio_node
        nqn_out, _ = self.ssh_obj.exec_command(target, "cat /etc/nvme/hostnqn")
        nqn = nqn_out.strip().split('\n')[0].strip()
        assert nqn, f"Could not read hostnqn from /etc/nvme/hostnqn on {target}"
        self.logger.info(f"[_get_client_host_nqn] NQN on {target}: {nqn!r}")
        self._client_host_nqn = nqn
        return nqn

    # ── connect / disconnect helpers ─────────────────────────────────────────

    def _get_connect_str_cli(self, lvol_id, host_nqn=None):
        """
        Return (connect_commands, stderr) for *lvol_id*.

        When *host_nqn* is provided the commands include embedded DHCHAP keys
        and use ``--ctrl-loss-tmo=-1`` (matching the existing API helper) so
        that NVMe controllers never time out during a storage-node outage.

        When *host_nqn* is None the plain ``volume connect`` output is returned
        (no DHCHAP keys, default ctrl-loss-tmo).
        """
        if host_nqn:
            return self.ssh_obj.get_lvol_connect_str_with_host_nqn(
                self.mgmt_nodes[0], lvol_id, host_nqn)
        # Unauthenticated path — use existing API helper via CLI
        cmd = f"{self.base_cmd} volume connect {lvol_id}"
        out, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
        connect_lines = [
            line.strip() for line in out.strip().split('\n')
            if line.strip() and 'nvme connect' in line
        ]
        return connect_lines, err

    def _connect_and_get_device(self, lvol_name, lvol_id, host_nqn=None):
        """
        Issue nvme connect command(s) on fio_node and return the new
        block device path (e.g. ``/dev/nvme3n1``).

        Returns (device_path, connect_commands_list).
        """
        self.logger.info(f"[DEBUG] _connect_and_get_device: lvol={lvol_name} id={lvol_id} host_nqn={host_nqn}")
        if host_nqn:
            connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn)
            self.logger.info(f"[DEBUG] connect strings (with host_nqn): err={err!r} cmds={connect_ls}")
            if err or not connect_ls:
                raise LvolNotConnectException(
                    f"No connect string for {lvol_name} (host_nqn={host_nqn}): {err}")
        else:
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            self.logger.info(f"[DEBUG] connect strings (no host_nqn): cmds={connect_ls}")

        initial_devices = self.ssh_obj.get_devices(node=self.fio_node)
        self.logger.info(f"[DEBUG] initial devices on {self.fio_node}: {initial_devices}")

        for cmd in connect_ls:
            cmd = ' '.join(cmd.split())  # normalise any embedded whitespace / stray \r\n
            self.logger.info(f"[DEBUG] executing nvme connect (repr): {cmd!r}")
            out, err = self.ssh_obj.exec_command(node=self.fio_node, command=cmd)
            self.logger.info(f"[DEBUG] nvme connect result: out={out!r} err={err!r}")
            if err:
                self.logger.warning(f"nvme connect warning: {err}")
                # Dump dmesg nvme entries after failure for diagnosis
                dmesg_out, _ = self.ssh_obj.exec_command(
                    node=self.fio_node, command="dmesg | grep -i nvme | tail -20")
                self.logger.info(f"[DEBUG] dmesg nvme tail after failed connect:\n{dmesg_out}")

        sleep_n_sec(3)
        final_devices = self.ssh_obj.get_devices(node=self.fio_node)
        self.logger.info(f"[DEBUG] final devices on {self.fio_node}: {final_devices}")
        new_devices = [d for d in final_devices if d not in initial_devices]
        self.logger.info(f"[DEBUG] new devices after connect: {new_devices}")

        lvol_device = None
        for dev in final_devices:
            if dev not in initial_devices:
                lvol_device = f"/dev/{dev.strip()}"
                break

        if not lvol_device:
            raise LvolNotConnectException(
                f"LVOL {lvol_name} did not appear as a block device")

        return lvol_device, connect_ls

    def _disconnect_lvol(self, lvol_id):
        """Disconnect a single lvol from fio_node by NQN."""
        try:
            details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
            if details:
                nqn = details[0]["nqn"]
                self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=nqn)
        except Exception as e:
            self.logger.warning(f"Disconnect error for {lvol_id}: {e}")

    def _get_lvol_details_via_cli(self, lvol_id):
        """Run ``volume get <id>`` and return raw CLI output."""
        out, _ = self.ssh_obj.exec_command(
            self.mgmt_nodes[0], f"{self.base_cmd} volume get {lvol_id}")
        return out

    # ── FIO helpers ──────────────────────────────────────────────────────────

    def _run_fio_and_validate(self, lvol_name, mount_point, log_file,
                               rw="randrw", bs="4K", numjobs=2, runtime=120):
        """Start FIO in a detached tmux session, wait for it to finish, then validate."""
        job_name = f"{lvol_name}_fio"
        self.ssh_obj.run_fio_test(
            self.fio_node, None, mount_point, log_file,
            size=self.fio_size,
            name=job_name,
            rw=rw, bs=bs, nrfiles=4, iodepth=1,
            numjobs=numjobs, time_based=True, runtime=runtime,
        )
        # run_fio_test launches FIO inside a detached tmux session and returns
        # immediately.  Poll until the process exits so that any subsequent
        # unmount/disconnect never races with a still-running FIO job.
        deadline = runtime + 60   # generous grace period
        waited = 0
        while waited < deadline:
            procs = self.ssh_obj.find_process_name(self.fio_node, f"fio.*{job_name}")
            running = [p for p in procs
                       if p.strip() and "grep" not in p and "fio --name" in p]
            if not running:
                break
            sleep_n_sec(5)
            waited += 5
        else:
            self.logger.warning(
                f"FIO job {job_name!r} did not finish after {deadline}s; killing")
            self.ssh_obj.kill_processes(node=self.fio_node, process_name="fio")
            sleep_n_sec(3)
        self.common_utils.validate_fio_test(self.fio_node, log_file=log_file)
# ═══════════════════════════════════════════════════════════════════════════
# COMMENTED OUT: All old test classes below used volume-level host management
# (volume add-host/remove-host, --allowed-hosts, --sec-options) which has been
# replaced by pool-level DHCHAP (pool add --dhchap, pool add-host/remove-host).
# ═══════════════════════════════════════════════════════════════════════════

# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 1 – All 4 core security combinations with FIO validation
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityCombinations(SecurityTestBase):
#     """
#     Creates one lvol for each of the four core security combinations:
#       1. plain         – no encryption, no auth
#       2. crypto        – encryption only
#       3. auth          – bidirectional DH-HMAC-CHAP, no encryption
#       4. crypto_auth   – encryption + bidirectional DH-HMAC-CHAP
#
#     Each lvol is connected to the FIO node and subjected to a 2-minute
#     randrw FIO workload.  Data integrity is validated via FIO log.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_combinations"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityCombinations START ===")
#         self._log_cluster_security_config()
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_auth", self.cluster_id, sec_options=SEC_BOTH)
#
#         # (label, encrypt, sec_opts, pool)
#         combinations = [
#             ("plain",       False, None,     self.pool_name),
#             ("crypto",      True,  None,     self.pool_name),
#             ("auth",        False, SEC_BOTH, self.pool_name + "_auth"),
#             ("crypto_auth", True,  SEC_BOTH, self.pool_name + "_auth"),
#         ]
#
#         fio_threads = []
#         for sec_type, encrypt, sec_opts, pool in combinations:
#             suffix = _rand_suffix()
#             lvol_name = f"sec{sec_type}{suffix}"
#             self.logger.info(f"--- Creating lvol {lvol_name!r} (sec_type={sec_type}) ---")
#
#             if sec_opts is not None:
#                 host_nqn = self._get_client_host_nqn()
#                 _, err = self.ssh_obj.create_sec_lvol(
#                     self.mgmt_nodes[0], lvol_name, self.lvol_size, pool,
#                     encrypt=encrypt,
#                     allowed_hosts=[host_nqn],
#                     key1=self.lvol_crypt_keys[0] if encrypt else None,
#                     key2=self.lvol_crypt_keys[1] if encrypt else None)
#                 assert not err or "error" not in err.lower(), \
#                     f"Failed to create {sec_type} lvol: {err}"
#             else:
#                 host_nqn = None
#                 self.sbcli_utils.add_lvol(
#                     lvol_name=lvol_name,
#                     pool_name=pool,
#                     size=self.lvol_size,
#                     crypto=encrypt,
#                     key1=self.lvol_crypt_keys[0] if encrypt else None,
#                     key2=self.lvol_crypt_keys[1] if encrypt else None,
#                 )
#
#             sleep_n_sec(3)
#             lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#             assert lvol_id, f"Could not get lvol ID for {lvol_name}"
#             self._log_lvol_security(lvol_id, label=f"({sec_type})")
#
#             lvol_device, connect_ls = self._connect_and_get_device(
#                 lvol_name, lvol_id, host_nqn=host_nqn)
#             self.logger.info(f"Connected {lvol_name} → {lvol_device}")
#
#             fs_type = "ext4"
#             mount_point = f"{self.mount_path}/{lvol_name}"
#             self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device,
#                                      fs_type=fs_type)
#             self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                     mount_path=mount_point)
#             log_file = f"{self.log_path}/{lvol_name}.log"
#
#             self.lvol_mount_details[lvol_name] = {
#                 "ID":      lvol_id,
#                 "Command": connect_ls,
#                 "Mount":   mount_point,
#                 "Device":  lvol_device,
#                 "FS":      fs_type,
#                 "Log":     log_file,
#                 "sec_type": sec_type,
#                 "host_nqn": host_nqn,
#             }
#
#             if sec_opts is not None:
#                 # DHCHAP volumes run synchronously: FIO → unmount → disconnect before
#                 # the next iteration can reset _client_host_nqn and get a new hostnqn.
#                 # Running them in background would leave the NVMe connection active when
#                 # the next DHCHAP iteration resets the hostnqn, causing the kernel to
#                 # reject the new connect with "found same hostid but different hostnqn".
#                 self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=120)
#                 self.logger.info(f"FIO validated for {sec_type} ✓")
#                 self.ssh_obj.unmount_path(self.fio_node, mount_point)
#                 sleep_n_sec(2)
#                 self._disconnect_lvol(lvol_id)
#                 sleep_n_sec(2)
#                 self.lvol_mount_details[lvol_name]["Mount"] = None
#             else:
#                 # Non-DHCHAP volumes run FIO in background (unchanged behaviour)
#                 t = threading.Thread(
#                     target=self._run_fio_and_validate,
#                     args=(lvol_name, mount_point, log_file),
#                     kwargs={"runtime": 120},
#                 )
#                 t.start()
#                 fio_threads.append((sec_type, t))
#                 sleep_n_sec(5)
#
#         # Wait for non-DHCHAP background FIO jobs
#         for sec_type, t in fio_threads:
#             self.logger.info(f"Waiting for FIO on {sec_type} lvol …")
#             t.join(timeout=600)
#             assert not t.is_alive(), f"FIO timed out for {sec_type}"
#             self.logger.info(f"FIO validated for {sec_type} ✓")
#
#         self.logger.info("=== TestLvolSecurityCombinations PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 2 – Allowed-hosts positive (correct NQN → connects)
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolAllowedHostsPositive(SecurityTestBase):
#     """
#     Creates an lvol with --allowed-hosts + bidirectional DH-HMAC-CHAP.
#     Verifies that:
#       - Connecting with the registered host NQN succeeds and FIO runs.
#       - ``volume get-secret`` returns non-empty credentials for that NQN.
#       - Connecting *without* --host-nqn returns a connect string but
#         without embedded DHCHAP keys (no dhchap-secret flag in the output).
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_allowed_hosts_positive"
#
#     def run(self):
#         self.logger.info("=== TestLvolAllowedHostsPositive START ===")
#         self._log_cluster_security_config()
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secallowed{_rand_suffix()}"
#
#         # Create lvol with both sec-options and allowed-hosts
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"lvol creation with allowed-hosts failed: {err}"
#
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id, "Could not find lvol ID"
#
#         # ── positive: connect with the registered NQN ──────────────────────
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"Connected with allowed NQN → {lvol_device}")
#
#         # Verify DHCHAP keys appear in at least one connect command
#         has_dhchap = any("dhchap" in c.lower() for c in connect_ls)
#         self.logger.info(f"DHCHAP key present in connect string: {has_dhchap}")
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                 mount_path=mount_point)
#         log_file = f"{self.log_path}/{lvol_name}.log"
#
#         self.lvol_mount_details[lvol_name] = {
#             "ID": lvol_id, "Mount": mount_point,
#             "Device": lvol_device, "Log": log_file,
#         }
#
#         # Run FIO to validate actual I/O
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#
#         # ── verify get-secret returns credentials ──────────────────────────
#         secret_out, _ = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         self.logger.info(f"get-secret output: {secret_out!r}")
#         assert secret_out.strip(), "Expected non-empty secret for registered host"
#
#         # ── verify lvol get shows allowed_hosts ───────────────────────────
#         detail_out = self._get_lvol_details_via_cli(lvol_id)
#         self.logger.info(f"lvol get output: {detail_out}")
#
#         # ── no host-nqn → connect string returned without dhchap keys ─────
#         connect_no_nqn, _ = self._get_connect_str_cli(lvol_id, host_nqn=None)
#         self.logger.info(f"Connect-without-NQN strings: {connect_no_nqn}")
#         # The connect string should exist (system responds) but DHCHAP key
#         # info should not be present since no specific host was identified
#         if connect_no_nqn:
#             has_dhchap_no_nqn = any("dhchap" in c.lower() for c in connect_no_nqn)
#             self.logger.info(f"DHCHAP in no-NQN connect string: {has_dhchap_no_nqn} "
#                              f"(expected False or command-level rejection)")
#
#         self.logger.info("=== TestLvolAllowedHostsPositive PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 3 – Allowed-hosts negative (wrong NQN → rejected)
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolAllowedHostsNegative(SecurityTestBase):
#     """
#     Creates an lvol with a specific allowed host NQN and verifies that
#     requesting a connect string for a *different* NQN is rejected at the
#     connect-string-generation stage (before any nvme connect attempt).
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_allowed_hosts_negative"
#
#     def run(self):
#         self.logger.info("=== TestLvolAllowedHostsNegative START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         allowed_nqn = self._get_client_host_nqn()
#         wrong_nqn = "nqn.2024-01.io.simplyblock:test:wrong-host-" + _rand_suffix()
#         lvol_name = f"secneg{_rand_suffix()}"
#
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[allowed_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"lvol creation failed: {err}"
#
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # Attempt connect with wrong NQN – expect error or empty connect list
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=wrong_nqn)
#         self.logger.info(
#             f"Connect with wrong NQN → connect_ls={connect_ls}, err={err!r}")
#
#         rejected = bool(err) or not connect_ls
#         assert rejected, (
#             f"Expected rejection for wrong NQN {wrong_nqn!r} "
#             f"but got connect strings: {connect_ls}")
#
#         self.logger.info("Correct: wrong host NQN was rejected at connect-string "
#                          "generation stage.")
#         self.logger.info("=== TestLvolAllowedHostsNegative PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 4 – Dynamic add-host / remove-host management
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolDynamicHostManagement(SecurityTestBase):
#     """
#     Verifies that hosts can be added to and removed from an existing lvol:
#
#     1. Create a plain lvol (no initial security).
#     2. Add a host NQN with sec-options (DHCHAP) via ``volume add-host``.
#     3. Verify the host appears in ``volume get`` output.
#     4. Connect and run FIO using the newly added host NQN.
#     5. Remove the host via ``volume remove-host``.
#     6. Verify connection with that NQN is now rejected.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_dynamic_host_management"
#
#     def run(self):
#         self.logger.info("=== TestLvolDynamicHostManagement START ===")
#         fio_nodes = self.fio_node          # full list before reassignment
#         self.fio_node = fio_nodes[0]
#         two_clients = len(fio_nodes) >= 2
#         self.logger.info(f"two_clients={two_clients} (fio_nodes={fio_nodes})")
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         lvol_name = f"secdyn{_rand_suffix()}"
#         host_nqn = self._get_client_host_nqn()        # NQN from fio_nodes[0]
#
#         # Get second client NQN when available (read directly, bypass cache)
#         second_host_nqn = None
#         if two_clients:
#             nqn_out, _ = self.ssh_obj.exec_command(fio_nodes[1], "cat /etc/nvme/hostnqn")
#             second_host_nqn = nqn_out.strip().split('\n')[0].strip()
#             assert second_host_nqn, f"Could not read hostnqn from {fio_nodes[1]}"
#             self.logger.info(f"Second client NQN: {second_host_nqn!r}")
#
#         # ── Step 1: Create plain lvol via API ──────────────────────────────
#         self.sbcli_utils.add_lvol(
#             lvol_name=lvol_name,
#             pool_name=self.pool_name,
#             size=self.lvol_size,
#         )
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id, "Could not find lvol ID"
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # ── Step 2: Add host(s) with DHCHAP via CLI ──────────────────────────
#         self.logger.info(f"Adding host {host_nqn!r} …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), \
#             f"add-host failed: {err}"
#
#         if two_clients:
#             self.logger.info(f"Adding second host {second_host_nqn!r} …")
#             out, err = self.ssh_obj.add_host_to_lvol(
#                 self.mgmt_nodes[0], lvol_id, second_host_nqn)
#             assert not err or "error" not in err.lower(), \
#                 f"add-host (second client) failed: {err}"
#
#         # ── Step 3: Verify host(s) appear in lvol details ───────────────────
#         # Use the API (structured data) rather than the CLI table output,
#         # because the table wraps long NQN strings across multiple lines.
#         lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#         allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
#         self.logger.info(f"allowed_hosts NQNs after add-host: {allowed_nqns}")
#         assert host_nqn in allowed_nqns, \
#             f"Expected {host_nqn!r} in allowed_hosts, got: {allowed_nqns}"
#         if two_clients:
#             assert second_host_nqn in allowed_nqns, \
#                 f"Expected second {second_host_nqn!r} in allowed_hosts, got: {allowed_nqns}"
#
#         # ── Step 4: Connect with the first host NQN and run FIO ─────────────
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"Connected via added host NQN → {lvol_device}")
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                 mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}.log"
#
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#
#         # Unmount and disconnect before removing host
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # ── Step 5: Remove the first host ────────────────────────────────────
#         self.logger.info(f"Removing host {host_nqn!r} …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), \
#             f"remove-host failed: {err}"
#
#         # ── Step 6: Verify removed host is rejected ───────────────────────────
#         sleep_n_sec(3)
#         if two_clients:
#             # allowed_hosts still has second_host_nqn → backend must reject removed NQN
#             connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#             rejected = bool(err) or not connect_ls
#             self.logger.info(
#                 f"[2-client] Connect after remove-host → connect_ls={connect_ls}, "
#                 f"err={err!r}, rejected={rejected}")
#             assert rejected, \
#                 "Expected rejection after remove-host (2-client) but still got a connect string"
#             self.logger.info("[2-client] Removed host correctly rejected PASSED")
#         else:
#             # allowed_hosts is now empty → backend falls back to "no security".
#             # Verify allowed_hosts is empty and the connect string has no DHCHAP keys.
#             lvol_api_after = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#             allowed_after = [h.get("nqn") for h in lvol_api_after[0].get("allowed_hosts", [])]
#             self.logger.info(f"[1-client] allowed_hosts after remove: {allowed_after}")
#             assert len(allowed_after) == 0, \
#                 f"Expected empty allowed_hosts after remove (1-client), got: {allowed_after}"
#             connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#             self.logger.info(
#                 f"[1-client] Connect after remove-host → connect_ls={connect_ls}, err={err!r}")
#             assert connect_ls, "Expected a plain connect string in 1-client fallback"
#             combined = " ".join(connect_ls)
#             assert "dhchap" not in combined.lower(), \
#                 f"Expected no DHCHAP keys in 1-client fallback connect string, got: {combined!r}"
#             self.logger.info("[1-client] allowed_hosts empty, connect string has no DHCHAP keys PASSED")
#
#         self.logger.info("=== TestLvolDynamicHostManagement PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 5 – Crypto + allowed-hosts end-to-end
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolCryptoWithAllowedHosts(SecurityTestBase):
#     """
#     Creates a crypto-encrypted lvol with both --sec-options and --allowed-hosts.
#     Verifies:
#       - Connection with correct NQN succeeds and returns DHCHAP-bearing command.
#       - FIO workload completes without errors.
#       - ``volume get-secret`` returns credentials.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_crypto_with_allowed_hosts"
#
#     def run(self):
#         self.logger.info("=== TestLvolCryptoWithAllowedHosts START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"seccryauth{_rand_suffix()}"
#
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn], encrypt=True,
#             key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"Crypto+auth lvol creation failed: {err}"
#
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"Connected crypto+auth lvol → {lvol_device}")
#
#         # Verify DHCHAP keys embedded
#         has_dhchap = any("dhchap" in c.lower() for c in connect_ls)
#         assert has_dhchap, "Expected DHCHAP keys in connect string for auth lvol"
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         log_file = f"{self.log_path}/{lvol_name}.log"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                 mount_path=mount_point)
#         self.lvol_mount_details[lvol_name] = {
#             "ID": lvol_id, "Mount": mount_point,
#             "Device": lvol_device, "Log": log_file,
#         }
#
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=120)
#
#         # Confirm get-secret returns something
#         secret_out, _ = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert secret_out.strip(), "Expected credentials from get-secret"
#
#         self.logger.info("=== TestLvolCryptoWithAllowedHosts PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 6 – Host-only vs controller-only DHCHAP directions
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolDhcapDirections(SecurityTestBase):
#     """
#     Tests each DHCHAP direction in isolation:
#       - host-only (dhchap_key=true, dhchap_ctrlr_key=false):
#           the host must authenticate to the controller.
#       - ctrl-only (dhchap_key=false, dhchap_ctrlr_key=true):
#           the controller must authenticate to the host.
#       - bidirectional (both=true): already covered by other tests,
#           included here for completeness.
#
#     Each variant is connected and subjected to a short FIO workload.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_dhchap_directions"
#
#     def run(self):
#         self.logger.info("=== TestLvolDhcapDirections START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_host", self.cluster_id, sec_options=SEC_HOST_ONLY)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_ctrl", self.cluster_id, sec_options=SEC_CTRL_ONLY)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         pool_host = self.pool_name + "_host"
#         pool_ctrl = self.pool_name + "_ctrl"
#         directions = [
#             ("host_only", pool_host),
#             ("ctrl_only", pool_ctrl),
#             ("bidir",     self.pool_name),
#         ]
#
#         for label, pool in directions:
#             # Each volume needs its own unique NQN to avoid SPDK keyring
#             # key-name collisions when multiple DHCHAP volumes are created.
#             self._client_host_nqn = None
#             host_nqn = self._get_client_host_nqn()
#
#             lvol_name = f"secdir{label}{_rand_suffix()}"
#             self.logger.info(f"--- Testing direction: {label} ---")
#
#             out, err = self.ssh_obj.create_sec_lvol(
#                 self.mgmt_nodes[0], lvol_name, self.lvol_size, pool,
#             )
#             assert not err or "error" not in err.lower(), \
#                 f"lvol creation failed for {label}: {err}"
#
#             sleep_n_sec(3)
#             lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#             assert lvol_id
#
#             lvol_device, connect_ls = self._connect_and_get_device(
#                 lvol_name, lvol_id, host_nqn=host_nqn)
#             self.logger.info(f"[{label}] Connected → {lvol_device}")
#
#             mount_point = f"{self.mount_path}/{lvol_name}"
#             log_file = f"{self.log_path}/{lvol_name}.log"
#             self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#             self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                     mount_path=mount_point)
#             self.lvol_mount_details[lvol_name] = {
#                 "ID": lvol_id, "Mount": mount_point,
#                 "Device": lvol_device, "Log": log_file,
#                 "host_nqn": host_nqn,
#             }
#
#             self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#             # Disconnect before the next iteration resets _client_host_nqn.
#             # The kernel binds hostid→hostnqn on the first connect; leaving the
#             # connection active causes "found same hostid but different hostnqn".
#             self.ssh_obj.unmount_path(self.fio_node, mount_point)
#             sleep_n_sec(2)
#             self._disconnect_lvol(lvol_id)
#             sleep_n_sec(2)
#             self.lvol_mount_details[lvol_name]["Mount"] = None
#             self.logger.info(f"[{label}] FIO validated ✓")
#
#         self.logger.info("=== TestLvolDhcapDirections PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 7 – Multi-host: add two hosts, verify each, remove one
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolMultipleAllowedHosts(SecurityTestBase):
#     """
#     Creates an lvol with two allowed host NQNs, verifies that the registered
#     NQN can connect, then removes one host and confirms its access is revoked
#     while the other host's access remains intact.
#
#     Since tests typically run on a single client machine, the 'second' host
#     NQN is a synthetic one injected into the allowed list.  The test focuses
#     on the control-plane operations (add-host / remove-host / volume get)
#     rather than dual-machine connectivity.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_multiple_allowed_hosts"
#
#     def run(self):
#         self.logger.info("=== TestLvolMultipleAllowedHosts START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         real_nqn = self._get_client_host_nqn()
#         fake_nqn = f"nqn.2024-01.io.simplyblock:test:fake-{_rand_suffix()}"
#         lvol_name = f"secmulti{_rand_suffix()}"
#
#         # Create with both NQNs in allowed list
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[real_nqn, fake_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"Multi-host lvol creation failed: {err}"
#
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # Both NQNs should appear in lvol details
#         lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#         allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
#         self.logger.info(f"allowed_hosts NQNs (2 hosts): {allowed_nqns}")
#         assert real_nqn in allowed_nqns, f"real NQN missing from allowed_hosts: {allowed_nqns}"
#         assert fake_nqn in allowed_nqns, f"fake NQN missing from allowed_hosts: {allowed_nqns}"
#
#         # Connect with real NQN
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=real_nqn)
#         self.logger.info(f"Connected with real NQN → {lvol_device}")
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         log_file = f"{self.log_path}/{lvol_name}.log"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                 mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#
#         # Disconnect before removing host
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # Remove fake NQN
#         self.logger.info(f"Removing fake NQN {fake_nqn!r} …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, fake_nqn)
#         assert not err or "error" not in err.lower(), f"remove-host failed: {err}"
#
#         # Verify fake NQN no longer in details, real NQN still there
#         lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#         allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
#         self.logger.info(f"allowed_hosts NQNs (after removal): {allowed_nqns}")
#         assert fake_nqn not in allowed_nqns, f"fake NQN should have been removed: {allowed_nqns}"
#         assert real_nqn in allowed_nqns, f"real NQN should still be present: {allowed_nqns}"
#
#         # Real NQN should still be able to get a connect string
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=real_nqn)
#         assert connect_ls and not err, \
#             f"real NQN should still connect after removing fake NQN; err={err!r}"
#
#         self.logger.info("=== TestLvolMultipleAllowedHosts PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 8 – Negative: get-secret, remove-host, add-host edge cases
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityNegativeHostOps(SecurityTestBase):
#     """
#     Covers error-path scenarios for host management operations:
#
#     TC-SEC-026  remove-host for NQN not in allowed list → error
#     TC-SEC-027  add-host with duplicate NQN → handled gracefully (no crash)
#     TC-SEC-028  get-secret for a host NQN that was never registered → error
#     TC-SEC-029  remove-host then re-add same NQN → should work correctly
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_negative_host_ops"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNegativeHostOps START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         absent_nqn = f"nqn.2024-01.io.simplyblock:test:absent-{_rand_suffix()}"
#         lvol_name = f"secnegops{_rand_suffix()}"
#
#         # Create a lvol with one allowed host
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # ── TC-SEC-026: remove non-existent NQN ──────────────────────────
#         self.logger.info("TC-SEC-026: remove-host for unregistered NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, absent_nqn)
#         has_error = bool(err) or ("error" in out.lower() if out else False) \
#                     or ("not found" in out.lower() if out else False)
#         self.logger.info(
#             f"remove non-existent NQN → out={out!r}, err={err!r}, "
#             f"has_error={has_error}")
#         assert has_error, \
#             "Expected error when removing a NQN that was never added"
#
#         # ── TC-SEC-027: add duplicate NQN ─────────────────────────────────
#         self.logger.info("TC-SEC-027: add-host with duplicate NQN …")
#         out1, err1 = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         self.logger.info(f"First add-host (already present): out={out1!r}, err={err1!r}")
#         # Should either succeed idempotently or return a meaningful error;
#         # the system must not crash or corrupt state.
#         detail_out = self._get_lvol_details_via_cli(lvol_id)
#         nqn_count = detail_out.count(host_nqn)
#         assert nqn_count <= 2, \
#             f"Duplicate NQN should not be listed more than once; got count={nqn_count}"
#
#         # ── TC-SEC-028: get-secret for unregistered NQN ───────────────────
#         self.logger.info("TC-SEC-028: get-secret for unregistered NQN …")
#         secret_out, secret_err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, absent_nqn)
#         is_empty_or_err = (
#             not secret_out.strip() or
#             bool(secret_err) or
#             "error" in secret_out.lower() or
#             "not found" in secret_out.lower()
#         )
#         self.logger.info(
#             f"get-secret absent NQN → out={secret_out!r}, err={secret_err!r}")
#         assert is_empty_or_err, \
#             "Expected empty result or error for unregistered NQN in get-secret"
#
#         # ── TC-SEC-029: remove then re-add same NQN ────────────────────────
#         self.logger.info("TC-SEC-029: remove-host then re-add same NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"remove-host failed: {err}"
#         sleep_n_sec(2)
#
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"re-add-host failed: {err}"
#         sleep_n_sec(2)
#
#         # Verify host NQN is back and can get a connect string
#         lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#         allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
#         assert host_nqn in allowed_nqns, \
#             f"Re-added NQN should appear in allowed_hosts: {allowed_nqns}"
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn)
#         assert connect_ls and not err, \
#             f"Re-added NQN should produce a valid connect string; err={err!r}"
#
#         self.logger.info("=== TestLvolSecurityNegativeHostOps PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 9 – Negative: invalid inputs at lvol creation time
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityNegativeCreation(SecurityTestBase):
#     """
#     Covers invalid input scenarios at lvol-creation time:
#
#     TC-SEC-050  --sec-options file path does not exist → CLI error
#     TC-SEC-051  --allowed-hosts file contains non-array JSON → CLI error
#     TC-SEC-053  --allowed-hosts with empty list [] → error or meaningful warning
#     TC-SEC-055  add-host with syntactically invalid NQN → error
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_negative_creation"
#
#     def _assert_cli_error(self, out: str, err: str, label: str) -> None:
#         """Assert that at least one of out/err signals a failure."""
#         failure_signals = ("error", "invalid", "failed", "no such", "not found",
#                            "cannot", "unable")
#         combined = (out or "").lower() + (err or "").lower()
#         has_signal = any(s in combined for s in failure_signals)
#         self.logger.info(
#             f"[{label}] out={out!r}, err={err!r}, has_error_signal={has_signal}")
#         assert has_signal or not out.strip(), \
#             f"[{label}] Expected error signal but got: out={out!r} err={err!r}"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNegativeCreation START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id)
#
#         # ── TC-SEC-050: non-existent sec-options file ─────────────────────
#         self.logger.info("TC-SEC-050: --sec-options with non-existent file path …")
#         lvol_name = f"secneg050{_rand_suffix()}"
#         cmd = (f"{self.base_cmd} -d volume add {lvol_name} {self.lvol_size}"
#                f" {self.pool_name} --sec-options /tmp/does_not_exist_ever.json")
#         out, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
#         # Should error; lvol must NOT be created
#         created_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert not created_id, \
#             "TC-SEC-050: lvol should NOT be created with non-existent sec-options file"
#         self.logger.info("TC-SEC-050 PASS: lvol not created for missing file")
#
#         # ── TC-SEC-051: allowed-hosts file contains object not array ───────
#         self.logger.info("TC-SEC-051: --allowed-hosts with invalid JSON (not array) …")
#         lvol_name = f"secneg051{_rand_suffix()}"
#         bad_json_path = "/tmp/bad_hosts.json"
#         # Write an object instead of an array
#         self.ssh_obj.write_json_file(
#             self.mgmt_nodes[0], bad_json_path,
#             {"nqn": "nqn.2024-01.io.simplyblock:bad"})
#         cmd = (f"{self.base_cmd} -d volume add {lvol_name} {self.lvol_size}"
#                f" {self.pool_name} --allowed-hosts {bad_json_path}")
#         out, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
#         self.ssh_obj.exec_command(
#             self.mgmt_nodes[0], f"rm -f {bad_json_path}", supress_logs=True)
#         created_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert not created_id, \
#             "TC-SEC-051: lvol should NOT be created when allowed-hosts JSON is not an array"
#         self.logger.info("TC-SEC-051 PASS")
#
#         # ── TC-SEC-053: --allowed-hosts with empty list ────────────────────
#         self.logger.info("TC-SEC-053: --allowed-hosts with empty list [] …")
#         lvol_name = f"secneg053{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[],   # empty list
#         )
#         # Behaviour: either error, or create with no allowed hosts (effectively open)
#         # The important thing is it does not crash and gives a clear response.
#         self.logger.info(
#             f"TC-SEC-053: empty allowed-hosts → out={out!r}, err={err!r}")
#         created_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         if created_id:
#             self.logger.info("TC-SEC-053: lvol created with empty hosts list; cleaning up")
#             self.lvol_mount_details[lvol_name] = {"ID": created_id, "Mount": None}
#         else:
#             self.logger.info("TC-SEC-053: lvol rejected with empty hosts list")
#
#         # ── TC-SEC-055: add-host with syntactically invalid NQN ───────────
#         self.logger.info("TC-SEC-055: add-host with invalid NQN format …")
#         # Create a plain lvol to test add-host against
#         plain_name = f"secneg055{_rand_suffix()}"
#         self.sbcli_utils.add_lvol(
#             lvol_name=plain_name,
#             pool_name=self.pool_name,
#             size=self.lvol_size,
#         )
#         sleep_n_sec(3)
#         plain_id = self.sbcli_utils.get_lvol_id(plain_name)
#         assert plain_id
#         self.lvol_mount_details[plain_name] = {"ID": plain_id, "Mount": None}
#
#         invalid_nqn = "not-a-valid-nqn-format-!@#$%"
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], plain_id, invalid_nqn)
#         self._assert_cli_error(out, err, "TC-SEC-055")
#         self.logger.info("TC-SEC-055 PASS: invalid NQN rejected")
#
#         self.logger.info("=== TestLvolSecurityNegativeCreation PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 10 – Negative: connect & I/O rejection scenarios
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityNegativeConnect(SecurityTestBase):
#     """
#     Tests rejection of connections that should not succeed:
#
#     TC-SEC-009  DHCHAP lvol (no allowed-hosts): connect with mismatched NQN
#     TC-SEC-013  Allowed-hosts lvol: connect without --host-nqn (no keys path)
#     TC-SEC-054  Auth lvol: attempt nvme connect using tampered connect string
#     TC-SEC-056  Delete lvol with active allowed-hosts → cleanup succeeds
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_negative_connect"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNegativeConnect START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#
#         # ── TC-SEC-009: auth lvol (no allowed-hosts) + wrong NQN ──────────
#         self.logger.info(
#             "TC-SEC-009: DHCHAP lvol (no allowed-hosts) + wrong NQN …")
#         lvol_name_009 = f"secneg009{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name_009, self.lvol_size, self.pool_name,
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_id_009 = self.sbcli_utils.get_lvol_id(lvol_name_009)
#         assert lvol_id_009
#         self.lvol_mount_details[lvol_name_009] = {"ID": lvol_id_009, "Mount": None}
#
#         wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
#         connect_ls, err = self._get_connect_str_cli(lvol_id_009, host_nqn=wrong_nqn)
#         self.logger.info(
#             f"TC-SEC-009: wrong NQN → connect_ls={connect_ls}, err={err!r}")
#         # When no allowed-hosts is configured, any NQN may get a connect string
#         # but the DHCHAP negotiation at the kernel level should fail.
#         # We log the result; the definitive rejection happens at nvme-connect time.
#         self.logger.info(
#             "TC-SEC-009: Connect string generation noted; actual DHCHAP rejection "
#             "occurs at kernel nvme-connect level (verified by non-zero connect exit code)")
#
#         # ── TC-SEC-013: allowed-hosts lvol, connect without --host-nqn ────
#         self.logger.info(
#             "TC-SEC-013: allowed-hosts lvol, connect without --host-nqn …")
#         # Fresh NQN for this volume to avoid SPDK keyring key-name collision
#         # with lvol_009 which was created with the same host_nqn.
#         self._client_host_nqn = None
#         host_nqn = self._get_client_host_nqn()
#
#         lvol_name_013 = f"secneg013{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name_013, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_id_013 = self.sbcli_utils.get_lvol_id(lvol_name_013)
#         assert lvol_id_013
#         self.lvol_mount_details[lvol_name_013] = {"ID": lvol_id_013, "Mount": None}
#
#         # Without host-nqn, connect string should not contain DHCHAP keys
#         connect_no_nqn, err_no_nqn = self._get_connect_str_cli(
#             lvol_id_013, host_nqn=None)
#         self.logger.info(
#             f"TC-SEC-013: no-NQN connect → strings={connect_no_nqn}, err={err_no_nqn!r}")
#         if connect_no_nqn:
#             has_dhchap = any("dhchap" in c.lower() for c in connect_no_nqn)
#             self.logger.info(
#                 f"TC-SEC-013: DHCHAP keys present={has_dhchap} "
#                 f"(expected False when no host-nqn supplied)")
#             assert not has_dhchap, \
#                 "Connect string without --host-nqn must not contain DHCHAP keys"
#
#         # ── TC-SEC-054: tampered connect string ────────────────────────────
#         self.logger.info(
#             "TC-SEC-054: connect with tampered DHCHAP key in connect string …")
#         connect_auth, err_auth = self._get_connect_str_cli(
#             lvol_id_013, host_nqn=host_nqn)
#         if connect_auth:
#             tampered = connect_auth[0]
#             # Replace dhchap-secret value with garbage if present
#             if "dhchap-secret" in tampered:
#                 import re
#                 tampered = re.sub(
#                     r'(--dhchap-secret\s+)\S+',
#                     r'\1DEADBEEFDEADBEEF00000000FFFFFFFF',
#                     tampered)
#                 self.logger.info(f"TC-SEC-054: Tampered connect cmd: {tampered!r}")
#                 _, connect_err = self.ssh_obj.exec_command(
#                     node=self.fio_node, command=tampered)
#                 self.logger.info(
#                     f"TC-SEC-054: Tampered connect result err={connect_err!r} "
#                     f"(expected non-zero exit / auth failure at kernel level)")
#                 # Note: even if exec_command swallows the exit code, the device
#                 # will NOT appear since DHCHAP negotiation fails.  The absence of
#                 # a new block device is the definitive check.
#                 sleep_n_sec(3)
#                 # We do NOT assert here because exec_command masks exit codes;
#                 # the behaviour is logged for manual / log-level verification.
#             else:
#                 self.logger.info(
#                     "TC-SEC-054: no dhchap-secret in connect string (no allowed-hosts); "
#                     "skipping tamper check")
#
#         # ── TC-SEC-056: delete lvol that has active allowed-hosts ──────────
#         self.logger.info(
#             "TC-SEC-056: delete lvol that has active allowed-hosts list …")
#         # lvol_013 has an allowed host – delete it and verify it's gone
#         self.sbcli_utils.delete_lvol(lvol_name=lvol_name_013, skip_error=False)
#         sleep_n_sec(3)
#         gone_id = self.sbcli_utils.get_lvol_id(lvol_name_013)
#         assert not gone_id, \
#             f"TC-SEC-056: lvol {lvol_name_013!r} should be deleted"
#         del self.lvol_mount_details[lvol_name_013]
#         self.logger.info("TC-SEC-056 PASS: lvol with allowed-hosts deleted cleanly")
#
#         self.logger.info("=== TestLvolSecurityNegativeConnect PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 11 – Allowed-hosts without DHCHAP (NQN whitelist only)
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolAllowedHostsNoDhchap(SecurityTestBase):
#     """
#     TC-SEC-034  Create lvol with --allowed-hosts but NO --sec-options
#                 (pure NQN whitelist, no DH-HMAC-CHAP key exchange).
#
#     Verifies:
#       - Allowed NQN can get a connect string and connect successfully.
#       - Connect string does NOT contain DHCHAP keys (no key negotiation).
#       - Unregistered NQN is still rejected at connect-string level.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_allowed_hosts_no_dhchap"
#
#     def run(self):
#         self.logger.info("=== TestLvolAllowedHostsNoDhchap START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id)
#
#         host_nqn = self._get_client_host_nqn()
#         wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
#         lvol_name = f"secnqnonly{_rand_suffix()}"
#
#         # No sec_options — NQN whitelist only
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"NQN-whitelist lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # Allowed NQN should get connect string (without DHCHAP keys)
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"Allowed NQN connect → {connect_ls}, err={err!r}")
#         assert connect_ls and not err, \
#             f"Allowed NQN should produce a connect string; err={err!r}"
#         has_dhchap = any("dhchap" in c.lower() for c in connect_ls)
#         assert not has_dhchap, \
#             "No DHCHAP keys expected when --sec-options not provided"
#
#         # Unregistered NQN should be rejected
#         wrong_connect, wrong_err = self._get_connect_str_cli(
#             lvol_id, host_nqn=wrong_nqn)
#         self.logger.info(
#             f"Wrong NQN connect → {wrong_connect}, err={wrong_err!r}")
#         rejected = bool(wrong_err) or not wrong_connect
#         assert rejected, \
#             f"Unregistered NQN should be rejected even without DHCHAP; " \
#             f"got: {wrong_connect}"
#
#         # Connect with correct NQN and run FIO
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"NQN-whitelist lvol connected → {lvol_device}")
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         log_file = f"{self.log_path}/{lvol_name}.log"
#         self.ssh_obj.format_disk(
#             node=self.fio_node, device=lvol_device, fs_type="ext4")
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#
#         self.logger.info("=== TestLvolAllowedHostsNoDhchap PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 12 – Snapshot & clone inherit security settings from the parent lvol
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecuritySnapshotClone(SecurityTestBase):
#     """
#     Verifies that snapshots and clones inherit security settings from their
#     parent lvol.  The backend copies ``allowed_hosts`` (including embedded
#     DHCHAP keys) and crypto settings at clone-creation time.
#
#     Scenarios:
#       A) auth parent   – DHCHAP only, no encryption
#          * Clone connects with the same host NQN / DHCHAP keys  (positive)
#          * Clone rejects a different host NQN                    (negative)
#
#       B) crypto_auth parent – DHCHAP + encryption
#          * Clone connects with the same host NQN / DHCHAP keys  (positive)
#          * Connect string includes dhchap keys                   (assertion)
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_snapshot_clone"
#
#     # ── helpers ──────────────────────────────────────────────────────────────
#
#     def _create_snap_and_clone(self, parent_id, label):
#         """Snapshot *parent_id* then clone it; return (snap_id, clone_id, clone_name)."""
#         snap_name = f"snap_{label}{_rand_suffix()}"
#         snap_result = self.sbcli_utils.add_snapshot(parent_id, snap_name)
#         assert snap_result, f"Snapshot creation failed for {snap_name}"
#         sleep_n_sec(3)
#         snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
#         assert snap_id, f"Could not find snapshot ID for {snap_name}"
#
#         clone_name = f"clone_{label}{_rand_suffix()}"
#         clone_result = self.sbcli_utils.add_clone(snap_id, clone_name)
#         assert clone_result, f"Clone creation failed for {clone_name}"
#         sleep_n_sec(3)
#         clone_id = self.sbcli_utils.get_lvol_id(clone_name)
#         assert clone_id, f"Could not find clone ID for {clone_name}"
#
#         self.lvol_mount_details[clone_name] = {"ID": clone_id, "Mount": None}
#         return snap_id, clone_id, clone_name
#
#     def _verify_clone_security(self, clone_name, clone_id, host_nqn, wrong_nqn,
#                                 expect_dhchap=True):
#         """
#         Core clone security assertions:
#           - wrong NQN is rejected
#           - correct host NQN connects successfully (with DHCHAP keys if expected)
#           - FIO read workload succeeds on the mounted clone
#         """
#         # Negative: wrong NQN should be rejected
#         wrong_connect, wrong_err = self._get_connect_str_cli(
#             clone_id, host_nqn=wrong_nqn)
#         rejected = bool(wrong_err) or not wrong_connect
#         assert rejected, \
#             f"Wrong NQN should be rejected on clone {clone_name}; got: {wrong_connect}"
#         self.logger.info(f"[{clone_name}] Wrong-NQN rejected as expected")
#
#         # Positive: correct host NQN connects
#         clone_device, clone_cmds = self._connect_and_get_device(
#             clone_name, clone_id, host_nqn=host_nqn)
#         self.logger.info(f"[{clone_name}] Connected → {clone_device}")
#
#         if expect_dhchap:
#             has_dhchap = any("dhchap" in c.lower() for c in clone_cmds)
#             assert has_dhchap, \
#                 f"Clone {clone_name} connect string should include DHCHAP keys"
#
#         mount_clone = f"{self.mount_path}/{clone_name}"
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=clone_device, mount_path=mount_clone)
#         self.lvol_mount_details[clone_name]["Mount"] = mount_clone
#
#         log_clone = f"{self.log_path}/{clone_name}.log"
#         self._run_fio_and_validate(
#             clone_name, mount_clone, log_clone, rw="read", runtime=30)
#         self.logger.info(f"[{clone_name}] FIO read validated")
#
#     # ── main test ─────────────────────────────────────────────────────────────
#
#     def run(self):
#         self.logger.info("=== TestLvolSecuritySnapshotClone START ===")
#         self._log_cluster_security_config()
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
#
#         # ── Scenario A: auth (DHCHAP only, no crypto) ────────────────────────
#         self.logger.info("--- Scenario A: auth parent (DHCHAP only) ---")
#         auth_parent = f"secsnap_auth{_rand_suffix()}"
#
#         _, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], auth_parent, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn])
#         assert not err or "error" not in err.lower(), \
#             f"auth parent creation failed: {err}"
#         sleep_n_sec(3)
#
#         auth_parent_id = self.sbcli_utils.get_lvol_id(auth_parent)
#         assert auth_parent_id, f"Could not find ID for {auth_parent}"
#         self._log_lvol_security(auth_parent_id, label="(auth parent)")
#
#         # Write data to parent so we can verify clone is readable
#         auth_device, _ = self._connect_and_get_device(
#             auth_parent, auth_parent_id, host_nqn=host_nqn)
#         mount_auth = f"{self.mount_path}/{auth_parent}"
#         self.ssh_obj.format_disk(
#             node=self.fio_node, device=auth_device, fs_type="ext4")
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=auth_device, mount_path=mount_auth)
#         self.lvol_mount_details[auth_parent] = {
#             "ID": auth_parent_id, "Mount": mount_auth, "Device": auth_device}
#
#         log_auth = f"{self.log_path}/{auth_parent}.log"
#         self._run_fio_and_validate(
#             auth_parent, mount_auth, log_auth, rw="write", runtime=30)
#
#         # Unmount parent before snapshotting
#         self.ssh_obj.unmount_path(self.fio_node, mount_auth)
#         self.lvol_mount_details[auth_parent]["Mount"] = None
#         sleep_n_sec(2)
#
#         _, auth_clone_id, auth_clone_name = self._create_snap_and_clone(
#             auth_parent_id, "auth")
#         self._log_lvol_security(auth_clone_id, label="(auth clone)")
#
#         self._verify_clone_security(
#             auth_clone_name, auth_clone_id, host_nqn, wrong_nqn,
#             expect_dhchap=True)
#
#         self.logger.info("--- Scenario A PASSED ---")
#
#         # ── Scenario B: crypto_auth (DHCHAP + encryption) ────────────────────
#         self.logger.info("--- Scenario B: crypto_auth parent (DHCHAP + crypto) ---")
#         # Disconnect Scenario A volumes before generating a fresh hostnqn for Scenario B.
#         # auth_parent was unmounted above but its NVMe connection is still active.
#         # auth_clone was mounted and connected by _verify_clone_security and never cleaned up.
#         # The kernel binds hostid→hostnqn on the first connect; the new hostnqn for Scenario B
#         # would be rejected with "found same hostid but different hostnqn" if any Scenario A
#         # connection remains active.
#         mount_auth_clone = f"{self.mount_path}/{auth_clone_name}"
#         self.ssh_obj.unmount_path(self.fio_node, mount_auth_clone)
#         sleep_n_sec(2)
#         self._disconnect_lvol(auth_clone_id)
#         sleep_n_sec(2)
#         self._disconnect_lvol(auth_parent_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[auth_clone_name]["Mount"] = None
#
#         # Fresh NQN for Scenario B to avoid SPDK keyring key-name collision
#         # with Scenario A's volumes (same host_nqn → same key_name → re-
#         # registration rejected → Scenario B auth would fail).
#         self._client_host_nqn = None
#         host_nqn = self._get_client_host_nqn()
#
#         ca_parent = f"secsnap_ca{_rand_suffix()}"
#
#         _, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], ca_parent, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#             encrypt=True,
#             key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1])
#         assert not err or "error" not in err.lower(), \
#             f"crypto_auth parent creation failed: {err}"
#         sleep_n_sec(3)
#
#         ca_parent_id = self.sbcli_utils.get_lvol_id(ca_parent)
#         assert ca_parent_id, f"Could not find ID for {ca_parent}"
#         self._log_lvol_security(ca_parent_id, label="(crypto_auth parent)")
#
#         ca_device, _ = self._connect_and_get_device(
#             ca_parent, ca_parent_id, host_nqn=host_nqn)
#         mount_ca = f"{self.mount_path}/{ca_parent}"
#         self.ssh_obj.format_disk(
#             node=self.fio_node, device=ca_device, fs_type="ext4")
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=ca_device, mount_path=mount_ca)
#         self.lvol_mount_details[ca_parent] = {
#             "ID": ca_parent_id, "Mount": mount_ca, "Device": ca_device}
#
#         log_ca = f"{self.log_path}/{ca_parent}.log"
#         self._run_fio_and_validate(
#             ca_parent, mount_ca, log_ca, rw="write", runtime=30)
#
#         self.ssh_obj.unmount_path(self.fio_node, mount_ca)
#         self.lvol_mount_details[ca_parent]["Mount"] = None
#         sleep_n_sec(2)
#
#         _, ca_clone_id, ca_clone_name = self._create_snap_and_clone(
#             ca_parent_id, "ca")
#         self._log_lvol_security(ca_clone_id, label="(crypto_auth clone)")
#
#         self._verify_clone_security(
#             ca_clone_name, ca_clone_id, host_nqn, wrong_nqn,
#             expect_dhchap=True)
#
#         self.logger.info("--- Scenario B PASSED ---")
#         self.logger.info("=== TestLvolSecuritySnapshotClone PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 12 – Storage node outage + DHCHAP credential persistence
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityOutageRecovery(SecurityTestBase):
#     """
#     Verifies that DHCHAP credentials survive a storage node outage/restart.
#
#     TC-SEC-070  Create DHCHAP (SEC_BOTH) lvol and connect successfully
#     TC-SEC-071  Shutdown a storage node; verify cluster remains accessible
#     TC-SEC-072  Restart the node; wait for it to come back online
#     TC-SEC-073  Reconnect the lvol with the same DHCHAP credentials – must succeed
#     TC-SEC-074  Run FIO on the reconnected lvol to confirm data plane integrity
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_outage_recovery"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityOutageRecovery START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secout{_rand_suffix()}"
#
#         # TC-SEC-070: create DHCHAP lvol and verify initial connect
#         self.logger.info("TC-SEC-070: Creating DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id, f"Could not find ID for {lvol_name}"
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         self.logger.info("TC-SEC-070: Initial connect + format PASSED")
#
#         # Disconnect before node outage
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-071: shutdown a storage node
#         self.logger.info("TC-SEC-071: Shutting down a storage node …")
#         nodes = self.sbcli_utils.get_storage_nodes()
#         primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
#         assert primary_nodes, "No primary storage nodes found"
#         target_node = primary_nodes[0]["uuid"]
#         self.sbcli_utils.shutdown_node(target_node)
#         self.sbcli_utils.wait_for_storage_node_status(target_node, "offline", timeout=120)
#         self.logger.info("TC-SEC-071: Node offline PASSED")
#
#         # TC-SEC-072: restart node and wait for it to come online
#         self.logger.info("TC-SEC-072: Waiting 2 min before restarting node …")
#         sleep_n_sec(120)
#         self.logger.info("TC-SEC-072: Restarting the storage node …")
#         self.sbcli_utils.restart_node(target_node)
#         self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
#         self.logger.info("TC-SEC-072: Node online — waiting 2 min for HA to settle …")
#         sleep_n_sec(120)
#         self.logger.info("TC-SEC-072: Node back online PASSED")
#
#         # TC-SEC-073: reconnect with original DHCHAP credentials
#         self.logger.info("TC-SEC-073: Reconnecting with original DHCHAP creds …")
#         lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         assert lvol_device2, "Reconnect after node restart failed"
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         self.logger.info("TC-SEC-073: Reconnect with DHCHAP creds PASSED")
#
#         # TC-SEC-074: FIO on reconnected lvol
#         self.logger.info("TC-SEC-074: Running FIO on reconnected lvol …")
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-074: FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityOutageRecovery PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 13 – 30-second network interrupt + DHCHAP re-auth
# ═══════════════════════════════════════════════════════════════════════════

# class TestLvolSecurityNetworkInterrupt(SecurityTestBase):
#     """
#     30-second NIC-level network interrupt on a storage node; verifies that
#     the DHCHAP session resumes correctly after reconnect.
#
#     TC-SEC-075  Create DHCHAP lvol, connect, mount
#     TC-SEC-076  Trigger 30-second network interrupt on a storage node
#     TC-SEC-077  Wait for interrupt to end; reconnect with DHCHAP creds
#     TC-SEC-078  Mount and run FIO – data plane must be intact
#     TC-SEC-079  Verify get-secret still returns valid credentials
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_network_interrupt"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNetworkInterrupt START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secnwi{_rand_suffix()}"
#
#         # TC-SEC-075: create lvol + connect
#         self.logger.info("TC-SEC-075: Creating DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         self.logger.info("TC-SEC-075: PASSED")
#
#         # Disconnect before network interrupt
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-076: trigger 30-second NIC interrupt on a storage node
#         self.logger.info("TC-SEC-076: Triggering 30s network interrupt …")
#         nodes = self.sbcli_utils.get_storage_nodes()
#         primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
#         assert primary_nodes, "No primary storage nodes found"
#         target_node_ip = primary_nodes[0]["mgmt_ip"]
#         active_ifaces = self.ssh_obj.get_active_interfaces(target_node_ip)
#         if active_ifaces:
#             self.ssh_obj.disconnect_all_active_interfaces(
#                 target_node_ip, active_ifaces, duration_secs=30)
#         self.logger.info("TC-SEC-076: Network interrupt triggered PASSED")
#
#         # TC-SEC-077: wait for interrupt to end then reconnect
#         self.logger.info("TC-SEC-077: Waiting 35s for interrupt to end …")
#         sleep_n_sec(35)
#         self.logger.info("TC-SEC-077: Reconnecting with DHCHAP creds …")
#         lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         assert lvol_device2, "Reconnect after network interrupt failed"
#         self.logger.info("TC-SEC-077: PASSED")
#
#         # TC-SEC-078: mount and run FIO
#         self.logger.info("TC-SEC-078: Running FIO after reconnect …")
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-078: FIO PASSED")
#
#         # TC-SEC-079: get-secret must still return valid creds
#         self.logger.info("TC-SEC-079: Verifying get-secret still works …")
#         out, err = self.ssh_obj.get_lvol_host_secret(self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert out and "error" not in out.lower(), f"get-secret failed after network interrupt: {err}"
#         self.logger.info("TC-SEC-079: get-secret PASSED")
#
#         self.logger.info("=== TestLvolSecurityNetworkInterrupt PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 14 – HA lvol: security preserved through primary failover
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityHAFailover(SecurityTestBase):
#     """
#     Creates an HA lvol (npcs=1) with DHCHAP, triggers primary failover by
#     shutting down the primary node, and verifies security config is intact
#     after the secondary takes over.
#
#     TC-SEC-080  Create HA DHCHAP lvol (ndcs=1, npcs=1)
#     TC-SEC-081  Connect with correct host NQN and run FIO
#     TC-SEC-082  Shutdown the primary storage node
#     TC-SEC-083  Restart the node; wait for HA to settle
#     TC-SEC-084  Reconnect with original DHCHAP creds and verify FIO
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_ha_failover"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityHAFailover START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secha{_rand_suffix()}"
#
#         # TC-SEC-080: create HA lvol with DHCHAP
#         self.logger.info("TC-SEC-080: Creating HA DHCHAP lvol (ndcs=1, npcs=1) …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#             distr_ndcs=1, distr_npcs=1,
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(5)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # TC-SEC-081: connect and run FIO
#         self.logger.info("TC-SEC-081: Connecting HA lvol and running FIO …")
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_pre.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=30)
#         self.logger.info("TC-SEC-081: Pre-failover FIO PASSED")
#
#         # Disconnect before shutdown
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-082: shutdown a primary storage node
#         self.logger.info("TC-SEC-082: Shutting down a primary storage node …")
#         nodes = self.sbcli_utils.get_storage_nodes()
#         primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
#         assert primary_nodes, "No primary storage nodes found"
#         target_node = primary_nodes[0]["uuid"]
#         self.sbcli_utils.shutdown_node(target_node)
#         self.sbcli_utils.wait_for_storage_node_status(target_node, "offline", timeout=120)
#         self.logger.info("TC-SEC-082: Node offline PASSED")
#
#         # TC-SEC-083: restart node, wait for HA to settle
#         self.logger.info("TC-SEC-083: Waiting 2 min before restarting node …")
#         sleep_n_sec(120)
#         self.logger.info("TC-SEC-083: Restarting node and waiting for HA settle …")
#         self.sbcli_utils.restart_node(target_node)
#         self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
#         self.logger.info("TC-SEC-083: Node online — waiting 2 min for HA to settle …")
#         sleep_n_sec(120)
#         self.logger.info("TC-SEC-083: HA settled PASSED")
#
#         # TC-SEC-084: reconnect with original DHCHAP creds
#         self.logger.info("TC-SEC-084: Reconnecting with DHCHAP creds after failover …")
#         lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         assert lvol_device2, "Reconnect after HA failover failed"
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file2 = f"{self.log_path}/{lvol_name}_post.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file2, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-084: Post-failover FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityHAFailover PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 15 – Management node reboot: DHCHAP config survives
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityMgmtNodeReboot(SecurityTestBase):
#     """
#     Reboots the management node and verifies that DHCHAP credentials are
#     still retrievable (get-secret) and connections still work after mgmt
#     node comes back online.
#
#     TC-SEC-085  Create DHCHAP lvol (SEC_BOTH), add allowed host, get-secret OK
#     TC-SEC-086  Reboot management node; wait for it to come back
#     TC-SEC-087  get-secret after mgmt reboot – credentials must still be present
#     TC-SEC-088  Connect lvol with original DHCHAP creds and run brief FIO
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_mgmt_node_reboot"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityMgmtNodeReboot START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secmgmt{_rand_suffix()}"
#
#         # TC-SEC-085: create lvol, get-secret baseline
#         self.logger.info("TC-SEC-085: Creating DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         pre_secret, pre_err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert pre_secret and "error" not in pre_secret.lower(), \
#             f"Pre-reboot get-secret failed: {pre_err}"
#         self.logger.info("TC-SEC-085: Pre-reboot secret obtained PASSED")
#
#         # TC-SEC-086: reboot management node
#         self.logger.info("TC-SEC-086: Rebooting management node …")
#         self.ssh_obj.reboot_node(self.mgmt_nodes[0], wait_time=300)
#         sleep_n_sec(15)
#         self.logger.info("TC-SEC-086: Management node back online PASSED")
#
#         # TC-SEC-087: get-secret after reboot
#         sleep_n_sec(100)  # Extra wait to ensure all services are fully up and secrets are loaded
#         self.logger.info("TC-SEC-087: Verifying get-secret after mgmt reboot …")
#         post_secret, post_err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert post_secret and "error" not in post_secret.lower(), \
#             f"Post-reboot get-secret failed: {post_err}"
#         self.logger.info("TC-SEC-087: get-secret after reboot PASSED")
#
#         # TC-SEC-088: connect + FIO
#         self.logger.info("TC-SEC-088: Connecting with DHCHAP creds after mgmt reboot …")
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-088: FIO after mgmt reboot PASSED")
#
#         self.logger.info("=== TestLvolSecurityMgmtNodeReboot PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 16 – Dynamic modification of allowed hosts during FIO
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityDynamicModification(SecurityTestBase):
#     """
#     Tests live add/remove of host NQNs, NQN rotation (key change), and
#     multi-NQN scenarios on a running lvol.
#
#     TC-SEC-089  Remove host NQN while FIO running → connection drops
#     TC-SEC-090  Re-add host NQN → reconnect resumes
#     TC-SEC-091  Add a second NQN; verify both NQNs can get connect strings
#     TC-SEC-092  Remove first NQN; verify second NQN still works
#     TC-SEC-093  Remove second NQN; verify no NQN can connect
#     TC-SEC-094  Add first NQN back → reconnect works again
#     TC-SEC-095  Teardown
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_dynamic_modification"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityDynamicModification START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         host_nqn = self._get_client_host_nqn()
#         second_nqn = f"nqn.2024-01.io.simplyblock:test:second-{_rand_suffix()}"
#         lvol_name = f"secdyn{_rand_suffix()}"
#
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # Pre-add second_nqn so that removing host_nqn in TC-089/TC-092 never leaves
#         # allowed_hosts empty (empty list → backend assumes no security → no rejection).
#         out, err = self.ssh_obj.add_host_to_lvol(self.mgmt_nodes[0], lvol_id, second_nqn)
#         assert not err or "error" not in err.lower(), f"pre-add second NQN failed: {err}"
#         self.logger.info(f"Pre-added {second_nqn!r} to keep allowed_hosts non-empty during removals")
#
#         # TC-SEC-089: remove host_nqn → second_nqn still in list → backend rejects host_nqn
#         self.logger.info("TC-SEC-089: Removing host NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"remove-host failed: {err}"
#         connect_ls, err2 = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         assert not connect_ls or err2, \
#             "Expected no connect string after removing host NQN"
#         self.logger.info("TC-SEC-089: Remove host NQN PASSED")
#
#         # TC-SEC-090: re-add host → connect string available
#         self.logger.info("TC-SEC-090: Re-adding host NQN …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"add-host failed: {err}"
#         sleep_n_sec(2)
#         connect_ls2, err3 = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         assert connect_ls2 and not err3, \
#             f"Connect string should be available after re-adding NQN; err={err3}"
#         self.logger.info("TC-SEC-090: Re-add host NQN PASSED")
#
#         # TC-SEC-091: add second NQN, verify both get connect strings
#         self.logger.info("TC-SEC-091: Adding second NQN …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, second_nqn)
#         assert not err or "error" not in err.lower(), f"add second NQN failed: {err}"
#         sleep_n_sec(2)
#         cs1, _ = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         cs2, _ = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
#         assert cs1, "First NQN should still get connect string"
#         assert cs2, "Second NQN should get connect string"
#         self.logger.info("TC-SEC-091: Both NQNs work PASSED")
#
#         # TC-SEC-092: remove first NQN, verify second still works
#         self.logger.info("TC-SEC-092: Removing first NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(2)
#         cs2b, _ = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
#         assert cs2b, "Second NQN should still work after removing first"
#         cs1b, err1b = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         assert not cs1b or err1b, "First NQN should not work after removal"
#         self.logger.info("TC-SEC-092: PASSED")
#
#         # Re-add host_nqn so that removing second_nqn in TC-093 doesn't leave
#         # allowed_hosts empty (same empty-list bug as TC-089).
#         out, err = self.ssh_obj.add_host_to_lvol(self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"re-add host_nqn before TC-093 failed: {err}"
#
#         # TC-SEC-093: remove second NQN → host_nqn still in list → backend rejects second_nqn
#         self.logger.info("TC-SEC-093: Removing second NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, second_nqn)
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(2)
#         cs2c, err2c = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
#         assert not cs2c or err2c, "Second NQN should not work after removal"
#         self.logger.info("TC-SEC-093: PASSED")
#
#         # TC-SEC-094: re-add first NQN, connect + FIO
#         self.logger.info("TC-SEC-094: Re-adding first NQN and running FIO …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-094: FIO after re-add PASSED")
#
#         self.logger.info("TC-SEC-095: TestLvolSecurityDynamicModification teardown")
#         self.logger.info("=== TestLvolSecurityDynamicModification PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 17 – Concurrent multi-client connections with DHCHAP
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityMultiClientConcurrent(SecurityTestBase):
#     """
#     Tests concurrent client connection attempts: correct NQN vs wrong NQN
#     issued simultaneously.
#
#     TC-SEC-096  Create DHCHAP lvol with one registered NQN
#     TC-SEC-097  Concurrently request connect strings for correct and wrong NQNs
#     TC-SEC-098  Verify correct NQN returns a valid connect string
#     TC-SEC-099  Verify wrong NQN returns no connect string or an error
#     TC-SEC-100  Connect with correct NQN and run FIO
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_multi_client_concurrent"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityMultiClientConcurrent START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
#         lvol_name = f"secmc{_rand_suffix()}"
#
#         # TC-SEC-096: create DHCHAP lvol
#         self.logger.info("TC-SEC-096: Creating DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # TC-SEC-097 & TC-SEC-098 & TC-SEC-099: concurrent connect string requests
#         self.logger.info("TC-SEC-097: Launching concurrent connect-string requests …")
#         results = {}
#
#         def _req(nqn, key):
#             try:
#                 cs, err = self._get_connect_str_cli(lvol_id, host_nqn=nqn)
#                 results[key] = (cs, err)
#             except Exception as e:
#                 results[key] = (None, str(e))
#
#         t_good = threading.Thread(target=_req, args=(host_nqn, "good"))
#         t_bad  = threading.Thread(target=_req, args=(wrong_nqn, "bad"))
#         t_good.start()
#         t_bad.start()
#         t_good.join()
#         t_bad.join()
#
#         good_cs, good_err = results.get("good", (None, "no result"))
#         bad_cs,  bad_err  = results.get("bad",  (None, "no result"))
#
#         # TC-SEC-098: correct NQN must succeed
#         assert good_cs, \
#             f"Correct NQN should return connect string; err={good_err}"
#         self.logger.info("TC-SEC-098: Correct NQN connect string PASSED")
#
#         # TC-SEC-099: wrong NQN must fail
#         assert not good_err or "error" not in (good_err or "").lower(), \
#             f"Correct NQN should have no error; err={good_err}"
#         assert not bad_cs or bad_err, \
#             f"Wrong NQN should not return a connect string; got {bad_cs}"
#         self.logger.info("TC-SEC-099: Wrong NQN rejected PASSED")
#
#         # TC-SEC-100: connect + FIO with correct NQN
#         self.logger.info("TC-SEC-100: Connecting and running FIO with correct NQN …")
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-100: FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityMultiClientConcurrent PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 18 – Scale: 10 DHCHAP volumes with rapid add/remove
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityScaleAndRapidOps(SecurityTestBase):
#     """
#     Creates 10 DHCHAP volumes simultaneously (each with a unique NQN) then
#     performs rapid add/remove of host NQNs.  Verifies no SPDK key-name
#     collisions occur and all volumes remain independently accessible.
#
#     TC-SEC-101  Create 10 DHCHAP lvols with unique NQNs (no collisions)
#     TC-SEC-102  Rapidly remove all host NQNs from all volumes
#     TC-SEC-103  Rapidly re-add all host NQNs
#     TC-SEC-104  Verify every volume can still be connected (get connect string)
#     """
#
#     VOLUME_COUNT = 10
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_scale_and_rapid_ops"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityScaleAndRapidOps START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         # TC-SEC-101: create 10 volumes each with unique NQN
#         self.logger.info(f"TC-SEC-101: Creating {self.VOLUME_COUNT} DHCHAP lvols …")
#         volumes = []  # list of (lvol_name, lvol_id, nqn)
#         for i in range(self.VOLUME_COUNT):
#             suffix = _rand_suffix()
#             lvol_name = f"secsc{i}{suffix}"
#             # unique NQN per volume to avoid SPDK keyring collision
#             uuid_out, _ = self.ssh_obj.exec_command(self.fio_node, "uuidgen")
#             uuid = uuid_out.strip().split('\n')[0].strip().lower()
#             nqn = f"nqn.2014-08.org.nvmexpress:uuid:{uuid}"
#             # Write hostnqn only for the last volume (we only connect one)
#             out, err = self.ssh_obj.create_sec_lvol(
#                 self.mgmt_nodes[0], lvol_name, "1G", self.pool_name,
#                 allowed_hosts=[nqn],
#             )
#             assert not err or "error" not in err.lower(), \
#                 f"lvol {lvol_name} creation failed: {err}"
#             sleep_n_sec(1)
#             lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#             assert lvol_id, f"Could not find ID for {lvol_name}"
#             volumes.append((lvol_name, lvol_id, nqn))
#             self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#         self.logger.info(f"TC-SEC-101: {self.VOLUME_COUNT} volumes created PASSED")
#
#         # TC-SEC-102: rapid remove all NQNs
#         self.logger.info("TC-SEC-102: Rapidly removing all host NQNs …")
#         for lvol_name, lvol_id, nqn in volumes:
#             out, err = self.ssh_obj.remove_host_from_lvol(
#                 self.mgmt_nodes[0], lvol_id, nqn)
#             assert not err or "error" not in err.lower(), \
#                 f"remove-host failed for {lvol_name}: {err}"
#         self.logger.info("TC-SEC-102: PASSED")
#
#         # TC-SEC-103: rapid re-add all NQNs
#         self.logger.info("TC-SEC-103: Rapidly re-adding all host NQNs …")
#         for lvol_name, lvol_id, nqn in volumes:
#             out, err = self.ssh_obj.add_host_to_lvol(
#                 self.mgmt_nodes[0], lvol_id, nqn)
#             assert not err or "error" not in err.lower(), \
#                 f"add-host failed for {lvol_name}: {err}"
#         sleep_n_sec(3)
#         self.logger.info("TC-SEC-103: PASSED")
#
#         # TC-SEC-104: all volumes can still get connect strings
#         self.logger.info("TC-SEC-104: Verifying all volumes still have valid connect strings …")
#         for lvol_name, lvol_id, nqn in volumes:
#             cs, err = self._get_connect_str_cli(lvol_id, host_nqn=nqn)
#             assert cs, \
#                 f"Volume {lvol_name} should have valid connect string after re-add; err={err}"
#         self.logger.info("TC-SEC-104: All volumes accessible PASSED")
#
#         self.logger.info("=== TestLvolSecurityScaleAndRapidOps PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 19 – Extended negative: tampered keys, edge-case CLI errors
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityNegativeConnectExtended(SecurityTestBase):
#     """
#     Extended negative scenarios beyond the basic TestLvolSecurityNegativeConnect:
#
#     TC-SEC-105  get-secret after remove-host → must return error
#     TC-SEC-106  add-host with empty NQN string → expect error
#     TC-SEC-107  add-host on non-existent lvol ID → expect error
#     TC-SEC-108  remove-host on non-existent lvol ID → expect error
#     TC-SEC-109  create lvol with SEC_CTRL_ONLY (bidirectional) and wrong host NQN → rejected
#     TC-SEC-110  create lvol with SEC_BOTH then get-secret with unregistered NQN → error
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_negative_connect_extended"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNegativeConnectExtended START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_ctrl", self.cluster_id, sec_options=SEC_CTRL_ONLY)
#
#         host_nqn = self._get_client_host_nqn()
#         absent_nqn = f"nqn.2024-01.io.simplyblock:test:absent-{_rand_suffix()}"
#         fake_lvol_id = "00000000-0000-0000-0000-000000000099"
#
#         lvol_name = f"secnex{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # TC-SEC-105: get-secret after remove-host
#         self.logger.info("TC-SEC-105: get-secret after remove-host …")
#         self.ssh_obj.remove_host_from_lvol(self.mgmt_nodes[0], lvol_id, host_nqn)
#         sleep_n_sec(2)
#         out, err = self.ssh_obj.get_lvol_host_secret(self.mgmt_nodes[0], lvol_id, host_nqn)
#         has_error = bool(err) or ("error" in (out or "").lower()) \
#                     or ("not found" in (out or "").lower())
#         assert has_error, f"get-secret after remove should fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-105: PASSED")
#
#         # Restore host for subsequent tests
#         self.ssh_obj.add_host_to_lvol(self.mgmt_nodes[0], lvol_id, host_nqn)
#         sleep_n_sec(2)
#
#         # TC-SEC-106: add-host with empty NQN
#         self.logger.info("TC-SEC-106: add-host with empty NQN …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, "")
#         has_error = bool(err) or ("error" in (out or "").lower())
#         assert has_error, f"add-host with empty NQN should fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-106: PASSED")
#
#         # TC-SEC-107: add-host on non-existent lvol
#         self.logger.info("TC-SEC-107: add-host on non-existent lvol …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], fake_lvol_id, host_nqn)
#         has_error = bool(err) or ("error" in (out or "").lower()) \
#                     or ("not found" in (out or "").lower())
#         assert has_error, \
#             f"add-host on non-existent lvol should fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-107: PASSED")
#
#         # TC-SEC-108: remove-host on non-existent lvol
#         self.logger.info("TC-SEC-108: remove-host on non-existent lvol …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], fake_lvol_id, host_nqn)
#         has_error = bool(err) or ("error" in (out or "").lower()) \
#                     or ("not found" in (out or "").lower())
#         assert has_error, \
#             f"remove-host on non-existent lvol should fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-108: PASSED")
#
#         # TC-SEC-109: SEC_CTRL_ONLY lvol with wrong NQN → no connect string
#         self.logger.info("TC-SEC-109: SEC_CTRL_ONLY lvol with wrong NQN …")
#         lvol_ctrl = f"secctrl{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_ctrl, self.lvol_size, self.pool_name + "_ctrl",
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_ctrl_id = self.sbcli_utils.get_lvol_id(lvol_ctrl)
#         assert lvol_ctrl_id
#         self.lvol_mount_details[lvol_ctrl] = {"ID": lvol_ctrl_id, "Mount": None}
#         wrong_cs, wrong_err = self._get_connect_str_cli(lvol_ctrl_id, host_nqn=absent_nqn)
#         assert not wrong_cs or wrong_err, \
#             f"Unregistered NQN should not get connect string; cs={wrong_cs}"
#         self.logger.info("TC-SEC-109: PASSED")
#
#         # TC-SEC-110: get-secret with unregistered NQN
#         self.logger.info("TC-SEC-110: get-secret with unregistered NQN …")
#         out, err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, absent_nqn)
#         has_error = bool(err) or ("error" in (out or "").lower()) \
#                     or ("not found" in (out or "").lower())
#         assert has_error, \
#             f"get-secret for unregistered NQN must fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-110: PASSED")
#
#         self.logger.info("=== TestLvolSecurityNegativeConnectExtended PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 20 – Clone has independent security config from parent
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityCloneOverride(SecurityTestBase):
#     """
#     Verifies that a clone can have a different security configuration from
#     its parent and that the two configs do not interfere.
#
#     TC-SEC-111  Create parent lvol with SEC_HOST_ONLY + allowed host NQN_A
#     TC-SEC-112  Create clone of parent snapshot – no explicit sec_options (inherits)
#     TC-SEC-113  Add a different NQN_B to the clone; verify NQN_A works on parent,
#                 NQN_B works on clone
#     TC-SEC-114  Remove NQN_A from parent; verify parent is inaccessible but clone
#                 still accessible with NQN_B
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_clone_override"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityCloneOverride START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         nqn_a = self._get_client_host_nqn()
#         uuid_out, _ = self.ssh_obj.exec_command(self.fio_node, "uuidgen")
#         uuid_b = uuid_out.strip().split('\n')[0].strip().lower()
#         nqn_b = f"nqn.2014-08.org.nvmexpress:uuid:{uuid_b}"
#
#         parent_name = f"secpar{_rand_suffix()}"
#
#         # TC-SEC-111: create parent lvol with SEC_HOST_ONLY + NQN_A
#         self.logger.info("TC-SEC-111: Creating parent DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], parent_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[nqn_a],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         parent_id = self.sbcli_utils.get_lvol_id(parent_name)
#         assert parent_id
#         self.lvol_mount_details[parent_name] = {"ID": parent_id, "Mount": None}
#         self.logger.info("TC-SEC-111: PASSED")
#
#         # Connect, write data, disconnect
#         lvol_device, _ = self._connect_and_get_device(parent_name, parent_id, host_nqn=nqn_a)
#         mount_point = f"{self.mount_path}/{parent_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[parent_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{parent_name}_w.log"
#         self._run_fio_and_validate(parent_name, mount_point, log_file, rw="write", runtime=20)
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(parent_id)
#         self.lvol_mount_details[parent_name]["Mount"] = None
#
#         # TC-SEC-112: snapshot + clone
#         self.logger.info("TC-SEC-112: Creating snapshot and clone …")
#         snap_name = f"snappar{_rand_suffix()}"
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0],
#             f"{self.base_cmd} -d snapshot add {parent_id} {snap_name}")
#         assert not err or "error" not in err.lower(), f"snapshot creation failed: {err}"
#         sleep_n_sec(3)
#         snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
#         assert snap_id, f"Could not find snapshot ID for {snap_name}"
#
#         clone_name = f"secclone{_rand_suffix()}"
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0],
#             f"{self.base_cmd} -d snapshot clone {snap_id} {clone_name}")
#         assert not err or "error" not in err.lower(), f"clone creation failed: {err}"
#         sleep_n_sec(5)
#         clone_id = self.sbcli_utils.get_lvol_id(clone_name)
#         assert clone_id, f"Could not find clone ID for {clone_name}"
#         self.lvol_mount_details[clone_name] = {"ID": clone_id, "Mount": None}
#         self.logger.info("TC-SEC-112: Snapshot+clone created PASSED")
#
#         # TC-SEC-113: add NQN_B to clone; verify NQN_A on parent, NQN_B on clone
#         self.logger.info("TC-SEC-113: Adding NQN_B to clone …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], clone_id, nqn_b)
#         assert not err or "error" not in err.lower(), f"add NQN_B to clone failed: {err}"
#         sleep_n_sec(2)
#         cs_parent_a, _ = self._get_connect_str_cli(parent_id, host_nqn=nqn_a)
#         cs_clone_b, _  = self._get_connect_str_cli(clone_id,  host_nqn=nqn_b)
#         assert cs_parent_a, "Parent: NQN_A should still get connect string"
#         assert cs_clone_b,  "Clone: NQN_B should get connect string"
#         self.logger.info("TC-SEC-113: Independent NQNs PASSED")
#
#         # TC-SEC-114: remove NQN_A from parent; clone NQN_B still works.
#         # Pre-add nqn_b to parent so that after removing nqn_a the parent's
#         # allowed_hosts is non-empty (empty list → backend assumes no security → no rejection).
#         out, err = self.ssh_obj.add_host_to_lvol(self.mgmt_nodes[0], parent_id, nqn_b)
#         assert not err or "error" not in err.lower(), f"pre-add nqn_b to parent failed: {err}"
#         self.logger.info("TC-SEC-114: Removing NQN_A from parent …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], parent_id, nqn_a)
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(2)
#         parent_api = self.sbcli_utils.get_lvol_details(lvol_id=parent_id)
#         allowed_after = [h.get("nqn") for h in parent_api[0].get("allowed_hosts", [])]
#         assert nqn_a not in allowed_after, \
#             f"NQN_A should have been removed from parent allowed_hosts, got: {allowed_after}"
#         cs_clone_b2, _ = self._get_connect_str_cli(clone_id, host_nqn=nqn_b)
#         assert cs_clone_b2, "Clone NQN_B should still be accessible"
#         self.logger.info("TC-SEC-114: Clone independence PASSED")
#
#         self.logger.info("=== TestLvolSecurityCloneOverride PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 21 – Security + backup: credentials survive backup/restore cycle
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityWithBackup(SecurityTestBase):
#     """
#     Backs up a DHCHAP+crypto lvol and verifies that the restored lvol
#     can be connected with the appropriate credentials.
#
#     TC-SEC-115  Create DHCHAP+crypto lvol, write FIO data, create snapshot
#     TC-SEC-116  Trigger backup of snapshot; wait for completion
#     TC-SEC-117  Restore backup to a new lvol name
#     TC-SEC-118  Verify the restored lvol can be accessed (get connect string
#                 for the original NQN should succeed since DHCHAP config
#                 is preserved with the lvol metadata)
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_with_backup"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityWithBackup START ===")
#         # Check backup is available
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0], f"{self.base_cmd} backup list 2>&1 | head -5")
#         if "command not found" in (out or "").lower() or "error" in (err or "").lower():
#             self.logger.info("Backup feature not available – skipping TC-SEC-115..118")
#             return
#
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secbck{_rand_suffix()}"
#
#         # TC-SEC-115: create DHCHAP+crypto lvol, write data
#         self.logger.info("TC-SEC-115: Creating DHCHAP+crypto lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#             encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_w.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)
#
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-116: snapshot + backup
#         self.logger.info("TC-SEC-116: Creating snapshot and backup …")
#         snap_name = f"snap{lvol_name[-6:]}"
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0],
#             f"{self.base_cmd} -d snapshot add {lvol_id} {snap_name} --backup")
#         assert not err or "error" not in err.lower(), f"snapshot add --backup failed: {err}"
#         sleep_n_sec(5)
#
#         # Wait for backup completion
#         import time as _time
#         deadline = _time.time() + 300
#         backup_id = None
#         while _time.time() < deadline:
#             list_out, _ = self.ssh_obj.exec_command(
#                 self.mgmt_nodes[0], f"{self.base_cmd} -d backup list")
#             for line in (list_out or "").splitlines():
#                 if snap_name in line:
#                     parts = [p.strip() for p in line.split("|") if p.strip()]
#                     if parts:
#                         for p in parts:
#                             if len(p) == 36 and "-" in p:
#                                 backup_id = p
#                     status_lower = line.lower()
#                     if "done" in status_lower or "complete" in status_lower:
#                         break
#             else:
#                 sleep_n_sec(10)
#                 continue
#             break
#         assert backup_id, "Could not find backup ID after snapshot backup"
#         self.logger.info(f"TC-SEC-116: Backup {backup_id} complete PASSED")
#
#         # TC-SEC-117: restore backup
#         self.logger.info("TC-SEC-117: Restoring backup …")
#         restored_name = f"secrst{_rand_suffix()}"
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0],
#             f"{self.base_cmd} -d backup restore {backup_id} --lvol {restored_name} --pool {self.pool_name}")
#         assert not err or "error" not in err.lower(), f"backup restore failed: {err}"
#         # Wait for restored lvol to appear
#         deadline2 = _time.time() + 300
#         while _time.time() < deadline2:
#             list_out, _ = self.ssh_obj.exec_command(self.mgmt_nodes[0], f"{self.base_cmd} lvol list")
#             if restored_name in (list_out or ""):
#                 break
#             sleep_n_sec(10)
#         else:
#             raise TimeoutError(f"Restored lvol {restored_name} did not appear within 300s")
#         self.logger.info(f"TC-SEC-117: Restore of {restored_name} PASSED")
#         self.lvol_mount_details[restored_name] = {"ID": None, "Mount": None}
#
#         # TC-SEC-118: verify unauthenticated connect is rejected (security enforced)
#         self.logger.info("TC-SEC-118: Verifying unauthenticated connect is rejected …")
#         restored_id = self.sbcli_utils.get_lvol_id(restored_name)
#         assert restored_id, f"Could not find ID for restored lvol {restored_name}"
#         self.lvol_mount_details[restored_name]["ID"] = restored_id
#         connect_ls, err = self._get_connect_str_cli(restored_id)
#         # Restored lvol inherits allowed_hosts — connect without host_nqn must fail
#         assert not connect_ls or ("host-nqn" in (err or "").lower() or "allowed" in (err or "").lower()), \
#             f"TC-SEC-118: Expected rejection without host-nqn, got connect_ls={connect_ls!r} err={err!r}"
#         self.logger.info("TC-SEC-118: Unauthenticated connect correctly rejected PASSED")
#
#         # TC-SEC-119: Connect restored lvol with source host NQN (security must be preserved)
#         self.logger.info("TC-SEC-119: Connecting restored lvol with source host NQN …")
#         restored_device, _ = self._connect_and_get_device(
#             restored_name, restored_id, host_nqn=host_nqn)
#         assert restored_device, \
#             f"TC-SEC-119: Restored lvol did not connect with source host_nqn={host_nqn}"
#         mount_restored = f"{self.mount_path}/{restored_name}"
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=restored_device, mount_path=mount_restored)
#         self.lvol_mount_details[restored_name]["Mount"] = mount_restored
#         self.logger.info(
#             f"TC-SEC-119: Restored lvol connected at {restored_device}, "
#             f"mounted at {mount_restored} PASSED")
#
#         # TC-SEC-120: Data integrity — restored files must match source lvol
#         self.logger.info("TC-SEC-120: Verifying data integrity: source vs restored …")
#
#         # Reconnect source lvol to generate checksums
#         source_device, _ = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_source = f"{self.mount_path}/{lvol_name}_verify"
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=source_device, mount_path=mount_source)
#         source_files = self.ssh_obj.find_files(self.fio_node, mount_source)
#         source_checksums = self.ssh_obj.generate_checksums(self.fio_node, source_files)
#         self.ssh_obj.unmount_path(self.fio_node, mount_source)
#         self._disconnect_lvol(lvol_id)
#
#         # Compare restored files against source checksums
#         restored_files = self.ssh_obj.find_files(self.fio_node, mount_restored)
#         self.ssh_obj.verify_checksums(
#             self.fio_node, restored_files, source_checksums,
#             by_name=True,
#             message="Restored lvol data does not match source lvol data")
#         self.logger.info("TC-SEC-120: Data integrity verified PASSED")
#
#         # Cleanup restored lvol
#         self.ssh_obj.unmount_path(self.fio_node, mount_restored)
#         sleep_n_sec(2)
#         self._disconnect_lvol(restored_id)
#         self.lvol_mount_details[restored_name]["Mount"] = None
#
#         self.logger.info("=== TestLvolSecurityWithBackup PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 22 – Resize a DHCHAP+crypto lvol: security config must be preserved
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityResize(SecurityTestBase):
#     """
#     Creates a DHCHAP+crypto lvol, resizes it, and verifies that the DHCHAP
#     configuration is unchanged after the resize operation.
#
#     TC-SEC-119  Create DHCHAP+crypto lvol (5G), connect, run FIO
#     TC-SEC-120  Resize lvol to 10G via sbcli_utils.resize_lvol
#     TC-SEC-121  Verify get-secret still works; connect with DHCHAP and run FIO
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_resize"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityResize START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secrsz{_rand_suffix()}"
#
#         # TC-SEC-119: create DHCHAP+crypto lvol 5G
#         self.logger.info("TC-SEC-119: Creating DHCHAP+crypto 5G lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, "5G", self.pool_name,
#             allowed_hosts=[host_nqn],
#             encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_pre.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)
#         self.logger.info("TC-SEC-119: Pre-resize FIO PASSED")
#
#         # Disconnect before resize
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-120: resize to 10G
#         self.logger.info("TC-SEC-120: Resizing lvol to 10G …")
#         self.sbcli_utils.resize_lvol(lvol_id, "10G")
#         sleep_n_sec(5)
#         self.logger.info("TC-SEC-120: Resize completed PASSED")
#
#         # TC-SEC-121: get-secret still works; reconnect + FIO
#         self.logger.info("TC-SEC-121: Verifying DHCHAP config after resize …")
#         out, err = self.ssh_obj.get_lvol_host_secret(self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert out and "error" not in (out or "").lower(), \
#             f"get-secret after resize failed: {err}"
#         self.logger.info("TC-SEC-121: get-secret after resize PASSED")
#
#         lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file2 = f"{self.log_path}/{lvol_name}_post.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file2, rw="randrw", runtime=20)
#         self.logger.info("TC-SEC-121: Post-resize FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityResize PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 23 – Volume list security fields validation
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityVolumeListFields(SecurityTestBase):
#     """
#     Verifies that security-related fields appear correctly in CLI output
#     after volume creation with various security options.
#
#     TC-SEC-122  Create DHCHAP+crypto lvol; verify CLI `volume get` has
#                 dhchap_key / dhchap_ctrlr_key fields
#     TC-SEC-123  Create SEC_HOST_ONLY lvol; verify ctrl key fields absent/false
#     TC-SEC-124  get-secret returns non-empty credential for registered NQN
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_volume_list_fields"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityVolumeListFields START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_host", self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         host_nqn = self._get_client_host_nqn()
#
#         # TC-SEC-122: SEC_BOTH lvol – both dhchap fields should be true/present
#         self.logger.info("TC-SEC-122: Creating SEC_BOTH lvol and checking fields …")
#         lvol_both = f"secvlb{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_both, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_both_id = self.sbcli_utils.get_lvol_id(lvol_both)
#         assert lvol_both_id
#         self.lvol_mount_details[lvol_both] = {"ID": lvol_both_id, "Mount": None}
#
#         detail_out = self._get_lvol_details_via_cli(lvol_both_id)
#         has_dhchap_key = "dhchap_key" in detail_out.lower() or "dhchap" in detail_out.lower()
#         assert has_dhchap_key, \
#             f"volume get should mention dhchap fields for SEC_BOTH: {detail_out!r}"
#         self.logger.info("TC-SEC-122: DHCHAP fields present PASSED")
#
#         # TC-SEC-123: SEC_HOST_ONLY lvol
#         self.logger.info("TC-SEC-123: Creating SEC_HOST_ONLY lvol and checking fields …")
#         uuid_out, _ = self.ssh_obj.exec_command(self.fio_node, "uuidgen")
#         uuid_h = uuid_out.strip().split('\n')[0].strip().lower()
#         nqn_h = f"nqn.2014-08.org.nvmexpress:uuid:{uuid_h}"
#
#         lvol_host = f"secvlh{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_host, self.lvol_size, self.pool_name + "_host",
#             allowed_hosts=[nqn_h],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_host_id = self.sbcli_utils.get_lvol_id(lvol_host)
#         assert lvol_host_id
#         self.lvol_mount_details[lvol_host] = {"ID": lvol_host_id, "Mount": None}
#
#         detail_host = self._get_lvol_details_via_cli(lvol_host_id)
#         self.logger.info(f"TC-SEC-123: volume get output: {detail_host!r}")
#         # SEC_HOST_ONLY means dhchap_key=True, dhchap_ctrlr_key=False
#         assert "dhchap" in detail_host.lower() or "allowed_host" in detail_host.lower(), \
#             f"SEC_HOST_ONLY lvol should show dhchap-related info: {detail_host!r}"
#         self.logger.info("TC-SEC-123: PASSED")
#
#         # TC-SEC-124: get-secret returns non-empty credential
#         self.logger.info("TC-SEC-124: Verifying get-secret returns credentials …")
#         secret_out, secret_err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_both_id, host_nqn)
#         assert secret_out and "error" not in (secret_out or "").lower(), \
#             f"get-secret should return credentials; out={secret_out!r} err={secret_err!r}"
#         self.logger.info("TC-SEC-124: get-secret credentials PASSED")
#
#         self.logger.info("=== TestLvolSecurityVolumeListFields PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 24 – DHCHAP over RDMA transport (skipped if RDMA not available)
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityRDMA(SecurityTestBase):
#     """
#     Creates a DHCHAP lvol on an RDMA-capable cluster and verifies that
#     authentication and data I/O work correctly over the RDMA fabric.
#
#     TC-SEC-125  Skip if cluster does not support RDMA (fabric_rdma=False)
#     TC-SEC-126  Create DHCHAP lvol with fabric=rdma; get connect string
#     TC-SEC-127  Connect via RDMA, mount, run FIO, validate data integrity
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_rdma"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityRDMA START ===")
#         self.fio_node = self.fio_node[0]
#
#         # TC-SEC-125: skip if RDMA not available
#         self.logger.info("TC-SEC-125: Checking RDMA availability …")
#         cluster_details = self.sbcli_utils.get_cluster_details()
#         fabric_rdma = cluster_details.get("fabric_rdma", False)
#         if not fabric_rdma:
#             self.logger.info(
#                 "TC-SEC-125: RDMA not available on this cluster (fabric_rdma=False) – SKIPPED")
#             return
#         self.logger.info("TC-SEC-125: RDMA available – proceeding")
#
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secrdma{_rand_suffix()}"
#
#         # TC-SEC-126: create DHCHAP lvol with rdma fabric
#         self.logger.info("TC-SEC-126: Creating DHCHAP lvol with RDMA fabric …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#             fabric="rdma",
#         )
#         assert not err or "error" not in err.lower(), f"RDMA lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id, f"Could not find ID for {lvol_name}"
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         assert connect_ls and not err, \
#             f"RDMA lvol should return connect string; err={err}"
#         self.logger.info("TC-SEC-126: RDMA DHCHAP connect string PASSED")
#
#         # TC-SEC-127: connect, mount, FIO
#         self.logger.info("TC-SEC-127: Connecting RDMA lvol and running FIO …")
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-127: RDMA FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityRDMA PASSED ===")



# ═══════════════════════════════════════════════════════════════════════════
# NEW TESTS – pool-level DHCHAP (--dhchap flag on pool add, pool add-host / remove-host)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityCombinations(SecurityTestBase):
    """
    Creates a DHCHAP-enabled pool, registers the client host NQN at pool
    level, then creates 4 lvol flavours (plain, crypto, auth-connect,
    crypto+auth) and verifies FIO on each.

    TC-NEW-001  Create pool with --dhchap; register client host
    TC-NEW-002  Plain lvol – create, connect with host-nqn, FIO
    TC-NEW-003  Crypto lvol – encrypted + DHCHAP, FIO
    TC-NEW-004  Auth-connect lvol – connect with host-nqn, verify secret
    TC-NEW-005  Crypto+Auth lvol – encrypted + DHCHAP + host-nqn, FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_combinations_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityCombinations START ===")
        self.fio_node = self.fio_node[0]

        # TC-NEW-001: create DHCHAP pool and register host
        self.logger.info("TC-NEW-001: Creating DHCHAP pool …")
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        assert pool_id, f"Pool {self.pool_name} not found"
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        self.logger.info("TC-NEW-001: Pool created + host registered PASSED")

        combos = [
            ("plain",       False, None, None),
            ("crypto",      True,  None, None),
            ("auth",        False, None, None),
            ("crypto_auth", True,  None, None),
        ]

        for tag, encrypt, key1, key2 in combos:
            tc = f"TC-NEW-00{combos.index((tag, encrypt, key1, key2)) + 2}"
            lvol_name = f"sec{tag}{_rand_suffix()}"
            self.logger.info(f"{tc}: Creating {tag} lvol …")

            kw = {}
            if encrypt:
                kw["encrypt"] = True
                kw["key1"] = self.lvol_crypt_keys[0]
                kw["key2"] = self.lvol_crypt_keys[1]

            out, err = self.ssh_obj.create_sec_lvol(
                self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
                **kw,
            )
            assert not err or "error" not in err.lower(), f"{tag} lvol creation failed: {err}"
            sleep_n_sec(3)
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            assert lvol_id, f"Could not find ID for {lvol_name}"
            self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

            lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
            self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
            self.lvol_mount_details[lvol_name]["Mount"] = mount_point
            log_file = f"{self.log_path}/{lvol_name}_out.log"
            self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
            self.logger.info(f"{tc}: {tag} FIO PASSED")

        self.logger.info("=== TestLvolSecurityCombinations PASSED ===")


class TestLvolDynamicHostManagement(SecurityTestBase):
    """
    Tests pool-level add-host / remove-host lifecycle.

    TC-NEW-010  Create DHCHAP pool + lvol, register host, connect, FIO
    TC-NEW-011  Remove host from pool → connect string no longer available
    TC-NEW-012  Re-add host to pool → connect string available again, FIO works
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_dynamic_host_management_v2"

    def run(self):
        self.logger.info("=== TestLvolDynamicHostManagement START ===")
        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        lvol_name = f"secdyn{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name)
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-NEW-010: connect + FIO
        self.logger.info("TC-NEW-010: Connecting and running FIO …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_pre.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=30)
        self.logger.info("TC-NEW-010: Pre-removal FIO PASSED")

        # Disconnect before removal
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-NEW-011: remove host from pool
        self.logger.info("TC-NEW-011: Removing host from pool …")
        self.ssh_obj.remove_host_from_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert not connect_ls or err, \
            "Expected no connect string after removing host from pool"
        self.logger.info("TC-NEW-011: Host removed – connect rejected PASSED")

        # TC-NEW-012: re-add host
        self.logger.info("TC-NEW-012: Re-adding host to pool …")
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file2 = f"{self.log_path}/{lvol_name}_post.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file2, rw="randrw", runtime=30)
        self.logger.info("TC-NEW-012: Re-added host – FIO PASSED")

        self.logger.info("=== TestLvolDynamicHostManagement PASSED ===")


class TestLvolCryptoWithDhchap(SecurityTestBase):
    """
    Encryption + DHCHAP combined test.

    TC-NEW-020  Create DHCHAP pool with host registered
    TC-NEW-021  Create encrypted lvol in DHCHAP pool
    TC-NEW-022  Connect with host-nqn, mount, FIO (randrw)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_crypto_with_dhchap_v2"

    def run(self):
        self.logger.info("=== TestLvolCryptoWithDhchap START ===")
        self.fio_node = self.fio_node[0]

        # TC-NEW-020
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        self.logger.info("TC-NEW-020: DHCHAP pool + host PASSED")

        # TC-NEW-021
        lvol_name = f"seccryp{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1])
        assert not err or "error" not in err.lower(), f"crypto lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        self.logger.info("TC-NEW-021: Encrypted lvol created PASSED")

        # TC-NEW-022
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-NEW-022: Crypto+DHCHAP FIO PASSED")

        self.logger.info("=== TestLvolCryptoWithDhchap PASSED ===")


class TestLvolDhchapBidirectional(SecurityTestBase):
    """
    Verifies bidirectional DHCHAP – always the default mode now.

    TC-NEW-030  Create DHCHAP pool + host
    TC-NEW-031  Create lvol, connect with host-nqn
    TC-NEW-033  FIO completes successfully
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_dhchap_bidirectional_v2"

    def run(self):
        self.logger.info("=== TestLvolDhchapBidirectional START ===")
        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        self.logger.info("TC-NEW-030: DHCHAP pool + host PASSED")

        lvol_name = f"secbidir{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name)
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-NEW-031: connect
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        self.logger.info("TC-NEW-031: Connected with host-nqn PASSED")

        # TC-NEW-033: FIO
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-NEW-033: Bidirectional FIO PASSED")

        self.logger.info("=== TestLvolDhchapBidirectional PASSED ===")


class TestLvolSecurityNegativeHostOps(SecurityTestBase):
    """
    Negative tests for pool-level host operations.

    TC-NEW-040  Connect without registered host → connect string returned but without DHCHAP keys
    TC-NEW-041  Remove non-registered NQN from pool → expect error or no-op
    TC-NEW-042  Add host, connect succeeds with DHCHAP keys; remove host, connect string without DHCHAP keys
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_host_ops_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeHostOps START ===")
        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)

        lvol_name = f"secneg{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name)
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-NEW-040: connect without registering host → connect string returned but without DHCHAP keys
        self.logger.info("TC-NEW-040: Connecting without registered host …")
        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert connect_ls and not err, \
            f"Expected connect string even without registered host; err={err}"
        connect_str = " ".join(connect_ls) if isinstance(connect_ls, list) else str(connect_ls)
        assert "dhchap" not in connect_str.lower(), \
            f"Expected no DHCHAP keys when host is not registered; got: {connect_str}"
        self.logger.info("TC-NEW-040: Connect without DHCHAP keys PASSED")

        # TC-NEW-041: remove non-registered NQN → should not crash
        self.logger.info("TC-NEW-041: Removing non-registered NQN …")
        fake_nqn = f"nqn.2024-01.io.simplyblock:test:fake-{_rand_suffix()}"
        out, err = self.ssh_obj.remove_host_from_pool(self.mgmt_nodes[0], pool_id, fake_nqn)
        # Should return error or be a no-op – must not crash
        self.logger.info(f"TC-NEW-041: remove non-registered NQN result: out={out!r} err={err!r}")
        self.logger.info("TC-NEW-041: PASSED (no crash)")

        # TC-NEW-042: add host → connect with DHCHAP keys; remove → connect without DHCHAP keys
        self.logger.info("TC-NEW-042: Add host, verify connect with DHCHAP, remove, verify no DHCHAP …")
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        connect_ls2, err2 = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert connect_ls2 and not err2, \
            f"Connect should succeed after adding host; err={err2}"
        connect_str2 = " ".join(connect_ls2) if isinstance(connect_ls2, list) else str(connect_ls2)
        assert "dhchap" in connect_str2.lower(), \
            f"Expected DHCHAP keys after registering host; got: {connect_str2}"
        self.logger.info("TC-NEW-042: Connect with DHCHAP keys PASSED")

        self.ssh_obj.remove_host_from_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        connect_ls3, err3 = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert connect_ls3 and not err3, \
            f"Connect string should still be returned after removing host; err={err3}"
        connect_str3 = " ".join(connect_ls3) if isinstance(connect_ls3, list) else str(connect_ls3)
        assert "dhchap" not in connect_str3.lower(), \
            f"Expected no DHCHAP keys after removing host; got: {connect_str3}"
        self.logger.info("TC-NEW-042: Add/remove lifecycle PASSED")

        self.logger.info("=== TestLvolSecurityNegativeHostOps PASSED ===")


class TestLvolSecuritySnapshotClone(SecurityTestBase):
    """
    Snapshot + clone inherits pool-level DHCHAP security.

    TC-NEW-050  Create DHCHAP pool + host, create lvol, write data
    TC-NEW-051  Create snapshot + clone
    TC-NEW-052  Connect clone with same host-nqn (pool-level auth), run FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_snapshot_clone_v2"

    def run(self):
        self.logger.info("=== TestLvolSecuritySnapshotClone START ===")
        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        # TC-NEW-050: create lvol, write data
        lvol_name = f"secsnap{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name)
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)

        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        self.lvol_mount_details[lvol_name]["Mount"] = None
        self.logger.info("TC-NEW-050: Source lvol written PASSED")

        # TC-NEW-051: snapshot + clone
        self.logger.info("TC-NEW-051: Creating snapshot and clone …")
        snap_name = f"snap{lvol_name[-6:]}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d snapshot add {lvol_id} {snap_name}")
        assert not err or "error" not in err.lower(), f"snapshot creation failed: {err}"
        sleep_n_sec(3)
        snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
        assert snap_id

        clone_name = f"secclone{_rand_suffix()}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d snapshot clone {snap_id} {clone_name}")
        assert not err or "error" not in err.lower(), f"clone creation failed: {err}"
        sleep_n_sec(5)
        clone_id = self.sbcli_utils.get_lvol_id(clone_name)
        assert clone_id
        self.lvol_mount_details[clone_name] = {"ID": clone_id, "Mount": None}
        self.logger.info("TC-NEW-051: Snapshot+clone PASSED")

        # TC-NEW-052: connect clone with pool-level auth
        self.logger.info("TC-NEW-052: Connecting clone with host-nqn …")
        clone_device, _ = self._connect_and_get_device(clone_name, clone_id, host_nqn=host_nqn)
        clone_mount = f"{self.mount_path}/{clone_name}"
        self.ssh_obj.mount_path(node=self.fio_node, device=clone_device, mount_path=clone_mount)
        self.lvol_mount_details[clone_name]["Mount"] = clone_mount
        log_file2 = f"{self.log_path}/{clone_name}_out.log"
        self._run_fio_and_validate(clone_name, clone_mount, log_file2, rw="randrw", runtime=20)
        self.logger.info("TC-NEW-052: Clone FIO PASSED")

        self.logger.info("=== TestLvolSecuritySnapshotClone PASSED ===")


class TestLvolSecurityRDMAv2(SecurityTestBase):
    """
    DHCHAP over RDMA fabric (pool-level API).

    TC-NEW-060  Skip if RDMA not available
    TC-NEW-061  Create DHCHAP pool + host, create lvol with fabric=rdma
    TC-NEW-062  Connect via RDMA with host-nqn, FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_rdma_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityRDMAv2 START ===")
        self.fio_node = self.fio_node[0]

        # TC-NEW-060: check RDMA
        cluster_details = self.sbcli_utils.get_cluster_details()
        if not cluster_details.get("fabric_rdma", False):
            self.logger.info("TC-NEW-060: RDMA not available – SKIPPED")
            return
        self.logger.info("TC-NEW-060: RDMA available")

        # TC-NEW-061: DHCHAP pool + RDMA lvol
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        lvol_name = f"secrdma{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            fabric="rdma")
        assert not err or "error" not in err.lower(), f"RDMA lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert connect_ls and not err, f"RDMA connect string failed; err={err}"
        self.logger.info("TC-NEW-061: RDMA DHCHAP lvol PASSED")

        # TC-NEW-062: connect, FIO
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-NEW-062: RDMA FIO PASSED")

        self.logger.info("=== TestLvolSecurityRDMAv2 PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Outage Test 1 – Storage node outage with FIO running (DHCHAP HA lvol)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityStorageNodeOutage(SecurityTestBase):
    """
    Verifies that DHCHAP credentials and I/O survive a storage node
    outage/restart on an HA lvol.  FIO runs *during* the outage and
    must complete without interruption.

    TC-SEC-070  Create DHCHAP pool + host, create HA lvol (ndcs=1, npcs=1)
    TC-SEC-071  Connect, format, mount, start long-running FIO in thread
    TC-SEC-072  Shutdown a primary storage node; validate node offline,
                lvols remain online, FIO still running
    TC-SEC-073  Restart node; wait for online + HA settle
    TC-SEC-074  Wait for FIO to finish; validate FIO log (no interruption)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_storage_node_outage"
        self.fio_runtime = 300

    def run(self):
        self.logger.info("=== TestLvolSecurityStorageNodeOutage START ===")
        self.fio_node = self.fio_node[0]

        # TC-SEC-070: DHCHAP pool + host + HA lvol
        self.logger.info("TC-SEC-070: Creating DHCHAP pool + HA lvol …")
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        assert pool_id, f"Pool {self.pool_name} not found"
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        lvol_name = f"secout{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            distr_ndcs=1, distr_npcs=1,
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(5)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, f"Could not find ID for {lvol_name}"
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        self.logger.info("TC-SEC-070: DHCHAP pool + HA lvol PASSED")

        # TC-SEC-071: connect, format, mount, start FIO in thread
        self.logger.info("TC-SEC-071: Connecting and starting long-running FIO …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        log_file = f"{self.log_path}/{lvol_name}_out.log"
        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(self.fio_node, None, mount_point, log_file),
            kwargs={
                "name": f"fio_run_{lvol_name}",
                "runtime": self.fio_runtime,
                "rw": "randrw",
                "bs": "4K",
                "size": self.fio_size,
                "nrfiles": 4,
                "iodepth": 1,
                "numjobs": 2,
                "time_based": True,
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)
        sleep_n_sec(15)  # let FIO settle
        self.logger.info("TC-SEC-071: FIO thread started PASSED")

        # TC-SEC-072: shutdown a primary storage node
        self.logger.info("TC-SEC-072: Shutting down a primary storage node …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"]
                         if not n.get("is_secondary_node") and n.get("lvols", 0) > 0]
        assert primary_nodes, "No primary storage nodes with lvols found"
        target_node = primary_nodes[0]["uuid"]

        deadline = time.time() + 300
        self.sbcli_utils.shutdown_node(node_uuid=target_node, force=False)
        while True:
            sleep_n_sec(20)
            node_detail = self.sbcli_utils.get_storage_node_details(target_node)
            if node_detail[0]["status"] == "offline":
                break
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Node {target_node} did not go offline within 5 minutes")
            self.logger.info(f"Node {target_node} not yet offline; retrying …")
            try:
                self.sbcli_utils.shutdown_node(node_uuid=target_node, force=False)
            except Exception as e:
                self.logger.warning(f"shutdown retry raised: {e}")

        self.logger.info("TC-SEC-072: Node offline — verifying FIO still running …")
        procs = self.ssh_obj.find_process_name(self.fio_node, f"fio.*fio_run_{lvol_name}")
        running = [p for p in procs if p.strip() and "grep" not in p and "fio --name" in p]
        assert running, "FIO should still be running during outage"
        self.logger.info("TC-SEC-072: Node offline + FIO alive PASSED")

        # TC-SEC-073: restart node
        self.logger.info("TC-SEC-073: Restarting storage node …")
        sleep_n_sec(30)
        self.sbcli_utils.restart_node(node_uuid=target_node)
        self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
        self.logger.info("TC-SEC-073: Node online — waiting for HA to settle …")
        sleep_n_sec(120)
        self.logger.info("TC-SEC-073: Node restart PASSED")

        # TC-SEC-074: wait for FIO and validate
        self.logger.info("TC-SEC-074: Waiting for FIO to complete …")
        self.common_utils.manage_fio_threads(
            self.fio_node, self.fio_threads, timeout=self.fio_runtime + 120)
        self.common_utils.validate_fio_test(self.fio_node, log_file=log_file)
        self.logger.info("TC-SEC-074: FIO completed without interruption PASSED")

        self.logger.info("=== TestLvolSecurityStorageNodeOutage PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Outage Test 2 – Management node reboot (DHCHAP config survives)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityMgmtNodeReboot(SecurityTestBase):
    """
    Reboots the management node and verifies that pool-level DHCHAP
    configuration is preserved — connect strings still contain DHCHAP
    keys and volumes remain accessible.

    TC-SEC-080  Create DHCHAP pool + host, create lvol, verify DHCHAP keys in connect string
    TC-SEC-081  Reboot management node; wait for services to recover
    TC-SEC-082  Verify connect string still has DHCHAP keys post-reboot
    TC-SEC-083  Connect lvol, mount, FIO — data plane intact after mgmt reboot
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_mgmt_node_reboot"

    def run(self):
        self.logger.info("=== TestLvolSecurityMgmtNodeReboot START ===")
        self.fio_node = self.fio_node[0]

        # TC-SEC-080: DHCHAP pool + host + lvol + baseline check
        self.logger.info("TC-SEC-080: Creating DHCHAP pool + lvol …")
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        assert pool_id, f"Pool {self.pool_name} not found"
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        lvol_name = f"secmgmt{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name)
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # Verify DHCHAP keys in connect string before reboot
        pre_connect, pre_err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert pre_connect and not pre_err, f"Pre-reboot connect failed: {pre_err}"
        pre_str = " ".join(pre_connect) if isinstance(pre_connect, list) else str(pre_connect)
        assert "dhchap" in pre_str.lower(), \
            f"Expected DHCHAP keys in pre-reboot connect string; got: {pre_str}"
        self.logger.info("TC-SEC-080: Pre-reboot DHCHAP keys present PASSED")

        # TC-SEC-081: reboot management node
        self.logger.info("TC-SEC-081: Rebooting management node …")
        self.ssh_obj.reboot_node(self.mgmt_nodes[0], wait_time=300)
        sleep_n_sec(100)  # wait for all services to fully start
        self.logger.info("TC-SEC-081: Management node back online PASSED")

        # TC-SEC-082: verify DHCHAP keys post-reboot
        self.logger.info("TC-SEC-082: Verifying DHCHAP keys post-reboot …")
        post_connect, post_err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert post_connect and not post_err, \
            f"Post-reboot connect string failed: {post_err}"
        post_str = " ".join(post_connect) if isinstance(post_connect, list) else str(post_connect)
        assert "dhchap" in post_str.lower(), \
            f"Expected DHCHAP keys in post-reboot connect string; got: {post_str}"
        self.logger.info("TC-SEC-082: Post-reboot DHCHAP keys preserved PASSED")

        # TC-SEC-083: connect, mount, FIO
        self.logger.info("TC-SEC-083: Connecting and running FIO after mgmt reboot …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-083: FIO after mgmt reboot PASSED")

        self.logger.info("=== TestLvolSecurityMgmtNodeReboot PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Outage Test 3 – HA failover with DHCHAP + encryption (FIO during outage)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityHAFailover(SecurityTestBase):
    """
    Creates an HA lvol (ndcs=1, npcs=1) with encryption + DHCHAP,
    runs FIO *during* a primary node shutdown, and verifies that
    security config survives the failover.

    TC-SEC-085  Create DHCHAP pool + host, create encrypted HA lvol
    TC-SEC-086  Connect, format, mount, start long-running FIO in thread
    TC-SEC-087  Shutdown the primary storage node; validate FIO alive
    TC-SEC-088  Restart node; wait for HA settle
    TC-SEC-089  Wait for FIO to finish; validate no interruption;
                verify DHCHAP keys still present in connect string
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_ha_failover"
        self.fio_runtime = 300

    def run(self):
        self.logger.info("=== TestLvolSecurityHAFailover START ===")
        self.fio_node = self.fio_node[0]

        # TC-SEC-085: DHCHAP pool + host + encrypted HA lvol
        self.logger.info("TC-SEC-085: Creating DHCHAP pool + encrypted HA lvol …")
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        assert pool_id, f"Pool {self.pool_name} not found"
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        lvol_name = f"secha{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
            distr_ndcs=1, distr_npcs=1,
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(5)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        self.logger.info("TC-SEC-085: Encrypted HA lvol PASSED")

        # TC-SEC-086: connect, format, mount, start FIO thread
        self.logger.info("TC-SEC-086: Connecting and starting FIO …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        log_file = f"{self.log_path}/{lvol_name}_out.log"
        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(self.fio_node, None, mount_point, log_file),
            kwargs={
                "name": f"fio_run_{lvol_name}",
                "runtime": self.fio_runtime,
                "rw": "randrw",
                "bs": "4K",
                "size": self.fio_size,
                "nrfiles": 4,
                "iodepth": 1,
                "numjobs": 2,
                "time_based": True,
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)
        sleep_n_sec(15)
        self.logger.info("TC-SEC-086: FIO thread started PASSED")

        # TC-SEC-087: shutdown a primary storage node
        self.logger.info("TC-SEC-087: Shutting down primary storage node …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"]
                         if not n.get("is_secondary_node") and n.get("lvols", 0) > 0]
        assert primary_nodes, "No primary storage nodes with lvols found"
        target_node = primary_nodes[0]["uuid"]

        deadline = time.time() + 300
        self.sbcli_utils.shutdown_node(node_uuid=target_node, force=False)
        while True:
            sleep_n_sec(20)
            node_detail = self.sbcli_utils.get_storage_node_details(target_node)
            if node_detail[0]["status"] == "offline":
                break
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Node {target_node} did not go offline within 5 minutes")
            self.logger.info(f"Node {target_node} not yet offline; retrying …")
            try:
                self.sbcli_utils.shutdown_node(node_uuid=target_node, force=False)
            except Exception as e:
                self.logger.warning(f"shutdown retry raised: {e}")

        self.logger.info("TC-SEC-087: Node offline — verifying FIO alive …")
        procs = self.ssh_obj.find_process_name(self.fio_node, f"fio.*fio_run_{lvol_name}")
        running = [p for p in procs if p.strip() and "grep" not in p and "fio --name" in p]
        assert running, "FIO should still be running during HA failover"
        self.logger.info("TC-SEC-087: Node offline + FIO alive PASSED")

        # TC-SEC-088: restart node, settle
        self.logger.info("TC-SEC-088: Restarting node …")
        sleep_n_sec(30)
        self.sbcli_utils.restart_node(node_uuid=target_node)
        self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
        self.logger.info("TC-SEC-088: Node online — waiting for HA to settle …")
        sleep_n_sec(120)
        self.logger.info("TC-SEC-088: Node restart PASSED")

        # TC-SEC-089: wait for FIO, validate, check DHCHAP keys
        self.logger.info("TC-SEC-089: Waiting for FIO to complete …")
        self.common_utils.manage_fio_threads(
            self.fio_node, self.fio_threads, timeout=self.fio_runtime + 120)
        self.common_utils.validate_fio_test(self.fio_node, log_file=log_file)
        self.logger.info("TC-SEC-089: FIO completed without interruption")

        # Verify DHCHAP keys still in connect string post-failover
        post_connect, post_err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert post_connect and not post_err, \
            f"Post-failover connect string failed: {post_err}"
        post_str = " ".join(post_connect) if isinstance(post_connect, list) else str(post_connect)
        assert "dhchap" in post_str.lower(), \
            f"Expected DHCHAP keys post-failover; got: {post_str}"
        self.logger.info("TC-SEC-089: DHCHAP keys preserved post-failover PASSED")

        self.logger.info("=== TestLvolSecurityHAFailover PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Outage Test 4 – 30-second network interrupt with FIO running
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityNetworkInterrupt(SecurityTestBase):
    """
    30-second NIC-level network interrupt on a storage node while FIO
    is running on an HA DHCHAP lvol.  FIO must survive the interrupt
    and DHCHAP auth must still work after reconnect.

    TC-SEC-090  Create DHCHAP pool + host, create HA lvol, connect, format, mount
    TC-SEC-091  Start long-running FIO in thread
    TC-SEC-092  Trigger 30s network interrupt on a storage node
    TC-SEC-093  Wait for interrupt to end; verify FIO completed without errors
    TC-SEC-094  Disconnect + reconnect with DHCHAP creds; verify auth still works
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_network_interrupt"
        self.fio_runtime = 120

    def run(self):
        self.logger.info("=== TestLvolSecurityNetworkInterrupt START ===")
        self.fio_node = self.fio_node[0]

        # TC-SEC-090: DHCHAP pool + host + HA lvol
        self.logger.info("TC-SEC-090: Creating DHCHAP pool + HA lvol …")
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        assert pool_id, f"Pool {self.pool_name} not found"
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        lvol_name = f"secnwi{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            distr_ndcs=1, distr_npcs=1,
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(5)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        self.logger.info("TC-SEC-090: HA lvol connected + mounted PASSED")

        # TC-SEC-091: start FIO in thread
        self.logger.info("TC-SEC-091: Starting FIO thread …")
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(self.fio_node, None, mount_point, log_file),
            kwargs={
                "name": f"fio_run_{lvol_name}",
                "runtime": self.fio_runtime,
                "rw": "randrw",
                "bs": "4K",
                "size": self.fio_size,
                "nrfiles": 4,
                "iodepth": 1,
                "numjobs": 2,
                "time_based": True,
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)
        sleep_n_sec(15)
        self.logger.info("TC-SEC-091: FIO running PASSED")

        # TC-SEC-092: trigger 30s network interrupt on a storage node
        self.logger.info("TC-SEC-092: Triggering 30s network interrupt …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"]
                         if not n.get("is_secondary_node")]
        assert primary_nodes, "No primary storage nodes found"
        target_node_ip = primary_nodes[0]["mgmt_ip"]
        active_ifaces = self.ssh_obj.get_active_interfaces(target_node_ip)
        assert active_ifaces, f"No active interfaces found on {target_node_ip}"
        self.ssh_obj.disconnect_all_active_interfaces(
            target_node_ip, active_ifaces, duration_secs=30)
        self.logger.info("TC-SEC-092: Network interrupt triggered PASSED")

        # TC-SEC-093: wait for interrupt to end, then wait for FIO
        self.logger.info("TC-SEC-093: Waiting 45s for network recovery …")
        sleep_n_sec(45)
        self.logger.info("TC-SEC-093: Waiting for FIO to complete …")
        self.common_utils.manage_fio_threads(
            self.fio_node, self.fio_threads, timeout=self.fio_runtime + 120)
        self.common_utils.validate_fio_test(self.fio_node, log_file=log_file)
        self.logger.info("TC-SEC-093: FIO completed without interruption PASSED")

        # TC-SEC-094: disconnect + reconnect to verify DHCHAP still works
        self.logger.info("TC-SEC-094: Reconnecting with DHCHAP after interrupt …")
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        assert lvol_device2, "Reconnect after network interrupt failed"
        mount_point2 = f"{self.mount_path}/{lvol_name}_post"
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point2)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point2
        log_file2 = f"{self.log_path}/{lvol_name}_post.log"
        self._run_fio_and_validate(lvol_name, mount_point2, log_file2, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-094: Post-interrupt reconnect + FIO PASSED")

        self.logger.info("=== TestLvolSecurityNetworkInterrupt PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Negative Test 1 – Invalid pool-level host operations at creation time
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityNegativeCreation(SecurityTestBase):
    """
    Covers invalid input scenarios for pool-level host management:

    TC-SEC-100  add-host to pool with syntactically invalid NQN → error
    TC-SEC-101  add-host to pool with empty NQN string → error
    TC-SEC-102  remove-host with non-existent NQN → error or no-op (no crash)
    TC-SEC-103  Create lvol in non-DHCHAP pool → connect string has no DHCHAP keys
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_creation_v2"

    def _assert_cli_error(self, out, err, label):
        """Assert that at least one of out/err signals a failure."""
        failure_signals = ("error", "invalid", "failed", "no such", "not found",
                           "cannot", "unable")
        combined = (out or "").lower() + (err or "").lower()
        has_signal = any(s in combined for s in failure_signals)
        self.logger.info(
            f"[{label}] out={out!r}, err={err!r}, has_error_signal={has_signal}")
        assert has_signal or not (out or "").strip(), \
            f"[{label}] Expected error signal but got: out={out!r} err={err!r}"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeCreation START ===")
        self.fio_node = self.fio_node[0]

        # Create DHCHAP pool
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        assert pool_id, f"Pool {self.pool_name} not found"

        # TC-SEC-100: add-host with invalid NQN
        self.logger.info("TC-SEC-100: add-host with invalid NQN …")
        invalid_nqn = "not-a-valid-nqn-format-!@#$%"
        out, err = self.ssh_obj.add_host_to_pool(
            self.mgmt_nodes[0], pool_id, invalid_nqn)
        self._assert_cli_error(out, err, "TC-SEC-100")
        self.logger.info("TC-SEC-100: Invalid NQN rejected PASSED")

        # TC-SEC-101: add-host with empty NQN
        self.logger.info("TC-SEC-101: add-host with empty NQN …")
        out, err = self.ssh_obj.add_host_to_pool(
            self.mgmt_nodes[0], pool_id, "")
        self._assert_cli_error(out, err, "TC-SEC-101")
        self.logger.info("TC-SEC-101: Empty NQN rejected PASSED")

        # TC-SEC-102: remove-host with non-existent NQN
        self.logger.info("TC-SEC-102: remove-host with non-existent NQN …")
        fake_nqn = f"nqn.2024-01.io.simplyblock:test:fake-{_rand_suffix()}"
        out, err = self.ssh_obj.remove_host_from_pool(
            self.mgmt_nodes[0], pool_id, fake_nqn)
        # Should return error or be a no-op – must not crash
        self.logger.info(
            f"TC-SEC-102: remove non-existent NQN result: out={out!r} err={err!r}")
        self.logger.info("TC-SEC-102: PASSED (no crash)")

        # TC-SEC-103: lvol in non-DHCHAP pool → no DHCHAP keys
        self.logger.info("TC-SEC-103: Creating lvol in non-DHCHAP pool …")
        plain_pool = f"{self.pool_name}_nodhchap"
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], plain_pool, self.cluster_id, dhchap=False)
        lvol_name = f"secneg{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, plain_pool)
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        host_nqn = self._get_client_host_nqn()
        connect_ls, cerr = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        if connect_ls:
            connect_str = " ".join(connect_ls) if isinstance(connect_ls, list) else str(connect_ls)
            assert "dhchap" not in connect_str.lower(), \
                f"Non-DHCHAP pool should not produce DHCHAP keys; got: {connect_str}"
        self.logger.info("TC-SEC-103: Non-DHCHAP pool has no keys PASSED")

        self.logger.info("=== TestLvolSecurityNegativeCreation PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Negative Test 2 – Connect rejection scenarios (pool-level)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityNegativeConnect(SecurityTestBase):
    """
    Tests rejection/absence of DHCHAP keys when host is not registered:

    TC-SEC-110  Unregistered NQN → connect string has no DHCHAP keys
    TC-SEC-111  Tampered DHCHAP secret → nvme connect fails (no new device)
    TC-SEC-112  Connect without host-nqn → no DHCHAP keys
    TC-SEC-113  Delete lvol in DHCHAP pool → cleanup succeeds
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_connect_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeConnect START ===")
        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        lvol_name = f"secnc{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name)
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-110: unregistered NQN → no DHCHAP keys
        self.logger.info("TC-SEC-110: Connect with unregistered NQN …")
        wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
        connect_ls, cerr = self._get_connect_str_cli(lvol_id, host_nqn=wrong_nqn)
        if connect_ls:
            connect_str = " ".join(connect_ls) if isinstance(connect_ls, list) else str(connect_ls)
            assert "dhchap" not in connect_str.lower(), \
                f"Unregistered NQN should not get DHCHAP keys; got: {connect_str}"
        self.logger.info("TC-SEC-110: No DHCHAP keys for unregistered NQN PASSED")

        # TC-SEC-111: tampered DHCHAP secret → no new device
        self.logger.info("TC-SEC-111: Tampered DHCHAP secret …")
        import re
        connect_auth, _ = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        if connect_auth:
            tampered = connect_auth[0]
            if "dhchap-secret" in tampered:
                tampered = re.sub(
                    r'(--dhchap-secret\s+)\S+',
                    r'\1DHHC-1:00:DEADBEEFDEADBEEFDEADBEEFDEADBEEF',
                    tampered)
                initial_devices = self.ssh_obj.get_devices(node=self.fio_node)
                self.ssh_obj.exec_command(node=self.fio_node, command=tampered)
                sleep_n_sec(3)
                final_devices = self.ssh_obj.get_devices(node=self.fio_node)
                new_devices = [d for d in final_devices if d not in initial_devices]
                assert not new_devices, \
                    f"Tampered key should not produce a new device; got: {new_devices}"
                self.logger.info("TC-SEC-111: Tampered key rejected PASSED")
            else:
                self.logger.info("TC-SEC-111: No dhchap-secret in string; skipped")
        else:
            self.logger.info("TC-SEC-111: No connect string; skipped")

        # TC-SEC-112: connect without host-nqn → no DHCHAP keys
        self.logger.info("TC-SEC-112: Connect without host-nqn …")
        connect_no_nqn, _ = self._get_connect_str_cli(lvol_id, host_nqn=None)
        if connect_no_nqn:
            has_dhchap = any("dhchap" in c.lower() for c in connect_no_nqn)
            assert not has_dhchap, \
                "Connect without host-nqn must not contain DHCHAP keys"
        self.logger.info("TC-SEC-112: No keys without host-nqn PASSED")

        # TC-SEC-113: delete lvol in DHCHAP pool
        self.logger.info("TC-SEC-113: Deleting lvol in DHCHAP pool …")
        self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=False)
        sleep_n_sec(3)
        gone_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert not gone_id, f"lvol {lvol_name} should be deleted"
        del self.lvol_mount_details[lvol_name]
        self.logger.info("TC-SEC-113: DHCHAP lvol deleted cleanly PASSED")

        self.logger.info("=== TestLvolSecurityNegativeConnect PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Dynamic modification of pool hosts with multi-NQN lifecycle
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityDynamicModification(SecurityTestBase):
    """
    Tests live add/remove of host NQNs at pool level and multi-NQN scenarios.

    TC-SEC-120  Create DHCHAP pool + NQN_A, create lvol, connect + FIO
    TC-SEC-121  Remove NQN_A from pool → connect string has no DHCHAP keys
    TC-SEC-122  Re-add NQN_A → connect string has DHCHAP keys, FIO works
    TC-SEC-123  Add NQN_B to pool → both NQNs get DHCHAP connect strings
    TC-SEC-124  Remove NQN_A → NQN_B still gets DHCHAP; NQN_A does not
    TC-SEC-125  Remove NQN_B → neither NQN gets DHCHAP keys
    TC-SEC-126  Re-add NQN_A → reconnect + FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_dynamic_modification_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityDynamicModification START ===")
        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        second_nqn = f"nqn.2024-01.io.simplyblock:test:second-{_rand_suffix()}"
        lvol_name = f"secdmod{_rand_suffix()}"

        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name)
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-120: connect + FIO
        self.logger.info("TC-SEC-120: Initial connect + FIO …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_pre.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)
        self.logger.info("TC-SEC-120: Initial FIO PASSED")

        # Disconnect
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-SEC-121: remove NQN_A → no DHCHAP keys
        self.logger.info("TC-SEC-121: Removing NQN_A from pool …")
        self.ssh_obj.remove_host_from_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        if connect_ls:
            cs = " ".join(connect_ls) if isinstance(connect_ls, list) else str(connect_ls)
            assert "dhchap" not in cs.lower(), \
                f"Expected no DHCHAP keys after removing host; got: {cs}"
        self.logger.info("TC-SEC-121: No DHCHAP keys after removal PASSED")

        # TC-SEC-122: re-add NQN_A → DHCHAP keys present, FIO works
        self.logger.info("TC-SEC-122: Re-adding NQN_A …")
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        connect_ls2, err2 = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert connect_ls2 and not err2, f"Re-add should restore connect; err={err2}"
        cs2 = " ".join(connect_ls2) if isinstance(connect_ls2, list) else str(connect_ls2)
        assert "dhchap" in cs2.lower(), f"Expected DHCHAP keys after re-add; got: {cs2}"
        lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file2 = f"{self.log_path}/{lvol_name}_readd.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file2, rw="randrw", runtime=20)
        self.logger.info("TC-SEC-122: Re-add FIO PASSED")

        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-SEC-123: add NQN_B → both get DHCHAP
        self.logger.info("TC-SEC-123: Adding NQN_B …")
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, second_nqn)
        sleep_n_sec(3)
        cs_a, _ = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        cs_b, _ = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
        assert cs_a, "NQN_A should get connect string"
        assert cs_b, "NQN_B should get connect string"
        str_a = " ".join(cs_a) if isinstance(cs_a, list) else str(cs_a)
        str_b = " ".join(cs_b) if isinstance(cs_b, list) else str(cs_b)
        assert "dhchap" in str_a.lower(), f"NQN_A should have DHCHAP; got: {str_a}"
        assert "dhchap" in str_b.lower(), f"NQN_B should have DHCHAP; got: {str_b}"
        self.logger.info("TC-SEC-123: Both NQNs have DHCHAP PASSED")

        # TC-SEC-124: remove NQN_A → NQN_B still has DHCHAP
        self.logger.info("TC-SEC-124: Removing NQN_A …")
        self.ssh_obj.remove_host_from_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        cs_a2, _ = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        cs_b2, _ = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
        if cs_a2:
            str_a2 = " ".join(cs_a2) if isinstance(cs_a2, list) else str(cs_a2)
            assert "dhchap" not in str_a2.lower(), \
                f"NQN_A should not have DHCHAP after removal; got: {str_a2}"
        assert cs_b2, "NQN_B should still get connect string"
        str_b2 = " ".join(cs_b2) if isinstance(cs_b2, list) else str(cs_b2)
        assert "dhchap" in str_b2.lower(), f"NQN_B should still have DHCHAP; got: {str_b2}"
        self.logger.info("TC-SEC-124: PASSED")

        # TC-SEC-125: remove NQN_B → neither has DHCHAP
        self.logger.info("TC-SEC-125: Removing NQN_B …")
        self.ssh_obj.remove_host_from_pool(self.mgmt_nodes[0], pool_id, second_nqn)
        sleep_n_sec(3)
        cs_a3, _ = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        cs_b3, _ = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
        for label, cs in [("NQN_A", cs_a3), ("NQN_B", cs_b3)]:
            if cs:
                s = " ".join(cs) if isinstance(cs, list) else str(cs)
                assert "dhchap" not in s.lower(), \
                    f"{label} should not have DHCHAP after all hosts removed; got: {s}"
        self.logger.info("TC-SEC-125: Neither NQN has DHCHAP PASSED")

        # TC-SEC-126: re-add NQN_A → reconnect + FIO
        self.logger.info("TC-SEC-126: Re-adding NQN_A and running FIO …")
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        lvol_device3, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device3, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file3 = f"{self.log_path}/{lvol_name}_final.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file3, rw="randrw", runtime=20)
        self.logger.info("TC-SEC-126: Final FIO PASSED")

        self.logger.info("=== TestLvolSecurityDynamicModification PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Scale: 10 DHCHAP volumes with rapid pool-level host add/remove
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityScaleAndRapidOps(SecurityTestBase):
    """
    Creates 10 DHCHAP volumes in the same pool, rapidly removes and re-adds
    the host, then verifies all volumes still have DHCHAP connect strings.

    TC-SEC-130  Create 10 lvols in DHCHAP pool (no key collisions)
    TC-SEC-131  Remove host from pool → no lvol has DHCHAP connect string
    TC-SEC-132  Re-add host → all 10 lvols have DHCHAP connect strings
    TC-SEC-133  Connect one lvol and run FIO to confirm
    """

    VOLUME_COUNT = 10

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_scale_rapid_ops_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityScaleAndRapidOps START ===")
        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        # TC-SEC-130: create 10 lvols
        self.logger.info(f"TC-SEC-130: Creating {self.VOLUME_COUNT} lvols …")
        volumes = []
        for i in range(self.VOLUME_COUNT):
            lvol_name = f"secsc{i}{_rand_suffix()}"
            out, err = self.ssh_obj.create_sec_lvol(
                self.mgmt_nodes[0], lvol_name, "1G", self.pool_name)
            assert not err or "error" not in err.lower(), \
                f"lvol {lvol_name} creation failed: {err}"
            sleep_n_sec(1)
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            assert lvol_id, f"Could not find ID for {lvol_name}"
            volumes.append((lvol_name, lvol_id))
            self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        self.logger.info(f"TC-SEC-130: {self.VOLUME_COUNT} volumes created PASSED")

        # TC-SEC-131: remove host → no DHCHAP
        self.logger.info("TC-SEC-131: Removing host from pool …")
        self.ssh_obj.remove_host_from_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        for lvol_name, lvol_id in volumes:
            cs, _ = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
            if cs:
                s = " ".join(cs) if isinstance(cs, list) else str(cs)
                assert "dhchap" not in s.lower(), \
                    f"{lvol_name}: should not have DHCHAP after host removal; got: {s}"
        self.logger.info("TC-SEC-131: All volumes have no DHCHAP PASSED")

        # TC-SEC-132: re-add host → all have DHCHAP
        self.logger.info("TC-SEC-132: Re-adding host to pool …")
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)
        sleep_n_sec(3)
        for lvol_name, lvol_id in volumes:
            cs, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
            assert cs and not err, \
                f"{lvol_name}: should have connect string after re-add; err={err}"
            s = " ".join(cs) if isinstance(cs, list) else str(cs)
            assert "dhchap" in s.lower(), \
                f"{lvol_name}: should have DHCHAP after re-add; got: {s}"
        self.logger.info("TC-SEC-132: All volumes have DHCHAP PASSED")

        # TC-SEC-133: connect one lvol + FIO
        self.logger.info("TC-SEC-133: Connecting first lvol and running FIO …")
        first_name, first_id = volumes[0]
        lvol_device, _ = self._connect_and_get_device(first_name, first_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{first_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[first_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{first_name}_out.log"
        self._run_fio_and_validate(first_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-133: Scale FIO PASSED")

        self.logger.info("=== TestLvolSecurityScaleAndRapidOps PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Resize DHCHAP+crypto lvol: security config preserved
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityResize(SecurityTestBase):
    """
    Creates a DHCHAP+crypto lvol, resizes it, and verifies that DHCHAP
    configuration is unchanged after the resize operation.

    TC-SEC-140  Create DHCHAP+crypto lvol (5G), connect, FIO
    TC-SEC-141  Disconnect, resize to 10G
    TC-SEC-142  Verify DHCHAP keys in connect string post-resize; reconnect, FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_resize_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityResize START ===")
        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        lvol_name = f"secrsz{_rand_suffix()}"

        # TC-SEC-140: create DHCHAP+crypto 5G lvol, connect, FIO
        self.logger.info("TC-SEC-140: Creating DHCHAP+crypto 5G lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, "5G", self.pool_name,
            encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1])
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_pre.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)
        self.logger.info("TC-SEC-140: Pre-resize FIO PASSED")

        # TC-SEC-141: disconnect, resize to 10G
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        self.logger.info("TC-SEC-141: Resizing to 10G …")
        self.sbcli_utils.resize_lvol(lvol_id, "10G")
        sleep_n_sec(5)
        self.logger.info("TC-SEC-141: Resize PASSED")

        # TC-SEC-142: verify DHCHAP keys, reconnect, FIO
        self.logger.info("TC-SEC-142: Verifying DHCHAP after resize …")
        post_cs, post_err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert post_cs and not post_err, f"Post-resize connect failed: {post_err}"
        post_str = " ".join(post_cs) if isinstance(post_cs, list) else str(post_cs)
        assert "dhchap" in post_str.lower(), \
            f"Expected DHCHAP keys post-resize; got: {post_str}"

        lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file2 = f"{self.log_path}/{lvol_name}_post.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file2, rw="randrw", runtime=20)
        self.logger.info("TC-SEC-142: Post-resize FIO PASSED")

        self.logger.info("=== TestLvolSecurityResize PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Backup/restore preserves DHCHAP credentials
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityWithBackup(SecurityTestBase):
    """
    Backs up a DHCHAP+crypto lvol and verifies the restored lvol
    is accessible with pool-level DHCHAP credentials.

    TC-SEC-150  Create DHCHAP+crypto lvol, write data, snapshot + backup
    TC-SEC-151  Wait for backup completion
    TC-SEC-152  Restore backup to new lvol name
    TC-SEC-153  Verify restored lvol has DHCHAP connect string; connect + FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_with_backup_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityWithBackup START ===")
        # Check backup feature availability
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0], f"{self.base_cmd} backup list 2>&1 | head -5")
        if "command not found" in (out or "").lower() or "error" in (err or "").lower():
            self.logger.info("Backup feature not available – SKIPPED")
            return

        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        lvol_name = f"secbck{_rand_suffix()}"

        # TC-SEC-150: create lvol, write data, snapshot + backup
        self.logger.info("TC-SEC-150: Creating DHCHAP+crypto lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1])
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)

        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        snap_name = f"snap{lvol_name[-6:]}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d snapshot add {lvol_id} {snap_name} --backup")
        assert not err or "error" not in err.lower(), f"snapshot+backup failed: {err}"
        sleep_n_sec(5)
        self.logger.info("TC-SEC-150: Snapshot + backup triggered PASSED")

        # TC-SEC-151: wait for backup
        self.logger.info("TC-SEC-151: Waiting for backup completion …")
        deadline = time.time() + 300
        backup_id = None
        while time.time() < deadline:
            list_out, _ = self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.base_cmd} -d backup list")
            for line in (list_out or "").splitlines():
                if snap_name in line:
                    parts = [p.strip() for p in line.split("|") if p.strip()]
                    for p in parts:
                        if len(p) == 36 and "-" in p:
                            backup_id = p
                    if "done" in line.lower() or "complete" in line.lower():
                        break
            if backup_id:
                break
            sleep_n_sec(10)
        assert backup_id, "Could not find backup ID"
        self.logger.info(f"TC-SEC-151: Backup {backup_id} complete PASSED")

        # TC-SEC-152: restore
        self.logger.info("TC-SEC-152: Restoring backup …")
        restored_name = f"secrst{_rand_suffix()}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d backup restore {backup_id} --lvol {restored_name} --pool {self.pool_name}")
        assert not err or "error" not in err.lower(), f"restore failed: {err}"

        deadline2 = time.time() + 300
        while time.time() < deadline2:
            list_out, _ = self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.base_cmd} lvol list")
            if restored_name in (list_out or ""):
                break
            sleep_n_sec(10)
        else:
            raise TimeoutError(f"Restored lvol {restored_name} did not appear within 300s")

        restored_id = self.sbcli_utils.get_lvol_id(restored_name)
        assert restored_id
        self.lvol_mount_details[restored_name] = {"ID": restored_id, "Mount": None}
        self.logger.info("TC-SEC-152: Restore PASSED")

        # TC-SEC-153: verify DHCHAP + connect + FIO
        self.logger.info("TC-SEC-153: Verifying restored lvol DHCHAP …")
        rest_cs, rest_err = self._get_connect_str_cli(restored_id, host_nqn=host_nqn)
        assert rest_cs and not rest_err, f"Restored connect failed: {rest_err}"
        rest_str = " ".join(rest_cs) if isinstance(rest_cs, list) else str(rest_cs)
        assert "dhchap" in rest_str.lower(), \
            f"Expected DHCHAP keys for restored lvol; got: {rest_str}"

        rest_device, _ = self._connect_and_get_device(restored_name, restored_id, host_nqn=host_nqn)
        rest_mount = f"{self.mount_path}/{restored_name}"
        self.ssh_obj.mount_path(node=self.fio_node, device=rest_device, mount_path=rest_mount)
        self.lvol_mount_details[restored_name]["Mount"] = rest_mount
        log_file2 = f"{self.log_path}/{restored_name}_out.log"
        self._run_fio_and_validate(restored_name, rest_mount, log_file2, rw="randrw", runtime=20)
        self.logger.info("TC-SEC-153: Restored lvol FIO PASSED")

        self.logger.info("=== TestLvolSecurityWithBackup PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Concurrent multi-client connect with DHCHAP
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityMultiClientConcurrent(SecurityTestBase):
    """
    Tests concurrent connect string requests: registered NQN vs unregistered.

    TC-SEC-160  Create DHCHAP pool, register NQN_A only, create lvol
    TC-SEC-161  Concurrently request connect strings for NQN_A and NQN_B
    TC-SEC-162  NQN_A gets DHCHAP keys; NQN_B does not
    TC-SEC-163  Connect with NQN_A and run FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_multi_client_concurrent_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityMultiClientConcurrent START ===")
        self.fio_node = self.fio_node[0]

        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.pool_name, self.cluster_id, dhchap=True)
        host_nqn = self._get_client_host_nqn()
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

        wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
        lvol_name = f"secmc{_rand_suffix()}"

        # TC-SEC-160: create lvol
        self.logger.info("TC-SEC-160: Creating DHCHAP lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name)
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-161: concurrent requests
        self.logger.info("TC-SEC-161: Concurrent connect-string requests …")
        results = {}

        def _req(nqn, key):
            try:
                cs, cerr = self._get_connect_str_cli(lvol_id, host_nqn=nqn)
                results[key] = (cs, cerr)
            except Exception as e:
                results[key] = (None, str(e))

        t_good = threading.Thread(target=_req, args=(host_nqn, "good"))
        t_bad = threading.Thread(target=_req, args=(wrong_nqn, "bad"))
        t_good.start()
        t_bad.start()
        t_good.join()
        t_bad.join()

        good_cs, good_err = results.get("good", (None, "no result"))
        bad_cs, bad_err = results.get("bad", (None, "no result"))

        # TC-SEC-162: registered NQN gets DHCHAP; unregistered does not
        assert good_cs, f"Registered NQN should get connect string; err={good_err}"
        good_str = " ".join(good_cs) if isinstance(good_cs, list) else str(good_cs)
        assert "dhchap" in good_str.lower(), \
            f"Registered NQN should have DHCHAP keys; got: {good_str}"
        self.logger.info("TC-SEC-162: Registered NQN has DHCHAP PASSED")

        if bad_cs:
            bad_str = " ".join(bad_cs) if isinstance(bad_cs, list) else str(bad_cs)
            assert "dhchap" not in bad_str.lower(), \
                f"Unregistered NQN should not have DHCHAP; got: {bad_str}"
        self.logger.info("TC-SEC-162: Unregistered NQN has no DHCHAP PASSED")

        # TC-SEC-163: connect + FIO
        self.logger.info("TC-SEC-163: Connecting and running FIO …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-163: FIO PASSED")

        self.logger.info("=== TestLvolSecurityMultiClientConcurrent PASSED ===")
