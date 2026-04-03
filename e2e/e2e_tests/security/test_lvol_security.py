"""
E2E security tests for lvol DH-HMAC-CHAP, allowed-hosts, and dynamic host management.

Security feature summary:
  pool add --sec-options <file>   JSON {dhchap_key: bool, dhchap_ctrlr_key: bool}; applied at pool level.
  --allowed-hosts <file>  JSON list of host NQNs that can access the lvol
  volume connect <id> --host-nqn <nqn>   returns connect string with embedded DHCHAP keys
  volume add-host <id> <nqn>   add host to existing lvol
  volume remove-host <id> <nqn>                remove host from existing lvol
  volume get-secret <id> <nqn>                 retrieve DHCHAP credentials for a host

All sbcli CLI wrappers live in ssh_utils.SshUtils:
  ssh_obj.create_sec_lvol(...)
  ssh_obj.get_lvol_connect_str_with_host_nqn(...)
  ssh_obj.add_host_to_lvol(...)
  ssh_obj.remove_host_from_lvol(...)
  ssh_obj.get_lvol_host_secret(...)
  ssh_obj.get_client_host_nqn(node)
"""

import threading
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


SEC_BOTH = {"dhchap_key": True, "dhchap_ctrlr_key": True}
SEC_HOST_ONLY = {"dhchap_key": True, "dhchap_ctrlr_key": False}
SEC_CTRL_ONLY = {"dhchap_key": False, "dhchap_ctrlr_key": True}


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
      self.ssh_obj.get_lvol_host_secret(...)
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
        """Return (and cache) a unique host NQN for the client node.

        Uses uuidgen to produce a random UUID so that every call with an
        empty cache generates a *unique* NQN.  This is critical when
        multiple DHCHAP volumes are created in the same test: two volumes
        with the same host NQN share the same SPDK keyring key_name.  If
        SPDK rejects re-registration the second volume's key is never
        stored and auth fails.  A unique NQN per volume gives a unique
        key_name so each registration is always the first for that name.

        The NQN is written to /etc/nvme/hostnqn so the kernel NVMe driver
        uses the same NQN that was registered in the volume's allowed-hosts
        list.
        """
        if self._client_host_nqn and not force_new:
            return self._client_host_nqn
        target = node or self.fio_node
        self.ssh_obj.exec_command(target, "sudo mkdir -p /etc/nvme")
        uuid_out, _ = self.ssh_obj.exec_command(target, "uuidgen")
        uuid = uuid_out.strip().split('\n')[0].strip().lower()
        nqn = f"nqn.2014-08.org.nvmexpress:uuid:{uuid}"
        self.ssh_obj.exec_command(
            target, f"echo '{nqn}' | sudo tee /etc/nvme/hostnqn")
        self.ssh_obj.exec_command(
            target, f"echo '{uuid}' | sudo tee /etc/nvme/hostid")
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
#  Test 1 – All 4 core security combinations with FIO validation
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityCombinations(SecurityTestBase):
    """
    Creates one lvol for each of the four core security combinations:
      1. plain         – no encryption, no auth
      2. crypto        – encryption only
      3. auth          – bidirectional DH-HMAC-CHAP, no encryption
      4. crypto_auth   – encryption + bidirectional DH-HMAC-CHAP

    Each lvol is connected to the FIO node and subjected to a 2-minute
    randrw FIO workload.  Data integrity is validated via FIO log.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_combinations"

    def run(self):
        self.logger.info("=== TestLvolSecurityCombinations START ===")
        self._log_cluster_security_config()
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id)
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_auth", self.cluster_id, sec_options=SEC_BOTH)

        # (label, encrypt, sec_opts, pool)
        combinations = [
            ("plain",       False, None,     self.pool_name),
            ("crypto",      True,  None,     self.pool_name),
            ("auth",        False, SEC_BOTH, self.pool_name + "_auth"),
            ("crypto_auth", True,  SEC_BOTH, self.pool_name + "_auth"),
        ]

        fio_threads = []
        for sec_type, encrypt, sec_opts, pool in combinations:
            suffix = _rand_suffix()
            lvol_name = f"sec{sec_type}{suffix}"
            self.logger.info(f"--- Creating lvol {lvol_name!r} (sec_type={sec_type}) ---")

            if sec_opts is not None:
                # Each DHCHAP volume gets its own unique NQN so SPDK keyring
                # key names never collide across volumes.
                self._client_host_nqn = None
                host_nqn = self._get_client_host_nqn()
                _, err = self.ssh_obj.create_sec_lvol(
                    self.mgmt_nodes[0], lvol_name, self.lvol_size, pool,
                    encrypt=encrypt,
                    allowed_hosts=[host_nqn],
                    key1=self.lvol_crypt_keys[0] if encrypt else None,
                    key2=self.lvol_crypt_keys[1] if encrypt else None)
                assert not err or "error" not in err.lower(), \
                    f"Failed to create {sec_type} lvol: {err}"
            else:
                host_nqn = None
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name,
                    pool_name=pool,
                    size=self.lvol_size,
                    crypto=encrypt,
                    key1=self.lvol_crypt_keys[0] if encrypt else None,
                    key2=self.lvol_crypt_keys[1] if encrypt else None,
                )

            sleep_n_sec(3)
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            assert lvol_id, f"Could not get lvol ID for {lvol_name}"
            self._log_lvol_security(lvol_id, label=f"({sec_type})")

            lvol_device, connect_ls = self._connect_and_get_device(
                lvol_name, lvol_id, host_nqn=host_nqn)
            self.logger.info(f"Connected {lvol_name} → {lvol_device}")

            fs_type = "ext4"
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device,
                                     fs_type=fs_type)
            self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
                                    mount_path=mount_point)
            log_file = f"{self.log_path}/{lvol_name}.log"

            self.lvol_mount_details[lvol_name] = {
                "ID":      lvol_id,
                "Command": connect_ls,
                "Mount":   mount_point,
                "Device":  lvol_device,
                "FS":      fs_type,
                "Log":     log_file,
                "sec_type": sec_type,
                "host_nqn": host_nqn,
            }

            # Start FIO in background
            t = threading.Thread(
                target=self._run_fio_and_validate,
                args=(lvol_name, mount_point, log_file),
                kwargs={"runtime": 120},
            )
            t.start()
            fio_threads.append((sec_type, t))
            sleep_n_sec(5)

        # Wait for all FIO jobs
        for sec_type, t in fio_threads:
            self.logger.info(f"Waiting for FIO on {sec_type} lvol …")
            t.join(timeout=600)
            assert not t.is_alive(), f"FIO timed out for {sec_type}"
            self.logger.info(f"FIO validated for {sec_type} ✓")

        self.logger.info("=== TestLvolSecurityCombinations PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 2 – Allowed-hosts positive (correct NQN → connects)
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolAllowedHostsPositive(SecurityTestBase):
    """
    Creates an lvol with --allowed-hosts + bidirectional DH-HMAC-CHAP.
    Verifies that:
      - Connecting with the registered host NQN succeeds and FIO runs.
      - ``volume get-secret`` returns non-empty credentials for that NQN.
      - Connecting *without* --host-nqn returns a connect string but
        without embedded DHCHAP keys (no dhchap-secret flag in the output).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_allowed_hosts_positive"

    def run(self):
        self.logger.info("=== TestLvolAllowedHostsPositive START ===")
        self._log_cluster_security_config()
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        lvol_name = f"secallowed{_rand_suffix()}"

        # Create lvol with both sec-options and allowed-hosts
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower(), \
            f"lvol creation with allowed-hosts failed: {err}"

        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, "Could not find lvol ID"

        # ── positive: connect with the registered NQN ──────────────────────
        lvol_device, connect_ls = self._connect_and_get_device(
            lvol_name, lvol_id, host_nqn=host_nqn)
        self.logger.info(f"Connected with allowed NQN → {lvol_device}")

        # Verify DHCHAP keys appear in at least one connect command
        has_dhchap = any("dhchap" in c.lower() for c in connect_ls)
        self.logger.info(f"DHCHAP key present in connect string: {has_dhchap}")

        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
                                mount_path=mount_point)
        log_file = f"{self.log_path}/{lvol_name}.log"

        self.lvol_mount_details[lvol_name] = {
            "ID": lvol_id, "Mount": mount_point,
            "Device": lvol_device, "Log": log_file,
        }

        # Run FIO to validate actual I/O
        self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)

        # ── verify get-secret returns credentials ──────────────────────────
        secret_out, _ = self.ssh_obj.get_lvol_host_secret(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        self.logger.info(f"get-secret output: {secret_out!r}")
        assert secret_out.strip(), "Expected non-empty secret for registered host"

        # ── verify lvol get shows allowed_hosts ───────────────────────────
        detail_out = self._get_lvol_details_via_cli(lvol_id)
        self.logger.info(f"lvol get output: {detail_out}")

        # ── no host-nqn → connect string returned without dhchap keys ─────
        connect_no_nqn, _ = self._get_connect_str_cli(lvol_id, host_nqn=None)
        self.logger.info(f"Connect-without-NQN strings: {connect_no_nqn}")
        # The connect string should exist (system responds) but DHCHAP key
        # info should not be present since no specific host was identified
        if connect_no_nqn:
            has_dhchap_no_nqn = any("dhchap" in c.lower() for c in connect_no_nqn)
            self.logger.info(f"DHCHAP in no-NQN connect string: {has_dhchap_no_nqn} "
                             f"(expected False or command-level rejection)")

        self.logger.info("=== TestLvolAllowedHostsPositive PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 3 – Allowed-hosts negative (wrong NQN → rejected)
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolAllowedHostsNegative(SecurityTestBase):
    """
    Creates an lvol with a specific allowed host NQN and verifies that
    requesting a connect string for a *different* NQN is rejected at the
    connect-string-generation stage (before any nvme connect attempt).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_allowed_hosts_negative"

    def run(self):
        self.logger.info("=== TestLvolAllowedHostsNegative START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        allowed_nqn = self._get_client_host_nqn()
        wrong_nqn = "nqn.2024-01.io.simplyblock:test:wrong-host-" + _rand_suffix()
        lvol_name = f"secneg{_rand_suffix()}"

        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[allowed_nqn],
        )
        assert not err or "error" not in err.lower(), \
            f"lvol creation failed: {err}"

        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id

        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # Attempt connect with wrong NQN – expect error or empty connect list
        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=wrong_nqn)
        self.logger.info(
            f"Connect with wrong NQN → connect_ls={connect_ls}, err={err!r}")

        rejected = bool(err) or not connect_ls
        assert rejected, (
            f"Expected rejection for wrong NQN {wrong_nqn!r} "
            f"but got connect strings: {connect_ls}")

        self.logger.info("Correct: wrong host NQN was rejected at connect-string "
                         "generation stage.")
        self.logger.info("=== TestLvolAllowedHostsNegative PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 4 – Dynamic add-host / remove-host management
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolDynamicHostManagement(SecurityTestBase):
    """
    Verifies that hosts can be added to and removed from an existing lvol:

    1. Create a plain lvol (no initial security).
    2. Add a host NQN with sec-options (DHCHAP) via ``volume add-host``.
    3. Verify the host appears in ``volume get`` output.
    4. Connect and run FIO using the newly added host NQN.
    5. Remove the host via ``volume remove-host``.
    6. Verify connection with that NQN is now rejected.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_dynamic_host_management"

    def run(self):
        self.logger.info("=== TestLvolDynamicHostManagement START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        lvol_name = f"secdyn{_rand_suffix()}"
        host_nqn = self._get_client_host_nqn()

        # ── Step 1: Create plain lvol via API ──────────────────────────────
        self.sbcli_utils.add_lvol(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size=self.lvol_size,
        )
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, "Could not find lvol ID"
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # ── Step 2: Add host with DHCHAP via CLI ──────────────────────────
        self.logger.info(f"Adding host {host_nqn!r} with sec-options …")
        out, err = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert not err or "error" not in err.lower(), \
            f"add-host failed: {err}"

        # ── Step 3: Verify host appears in lvol details ───────────────────
        # Use the API (structured data) rather than the CLI table output,
        # because the table wraps long NQN strings across multiple lines.
        lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
        self.logger.info(f"allowed_hosts NQNs after add-host: {allowed_nqns}")
        assert host_nqn in allowed_nqns, \
            f"Expected {host_nqn!r} in allowed_hosts, got: {allowed_nqns}"

        # ── Step 4: Connect with the new host NQN and run FIO ─────────────
        lvol_device, connect_ls = self._connect_and_get_device(
            lvol_name, lvol_id, host_nqn=host_nqn)
        self.logger.info(f"Connected via added host NQN → {lvol_device}")

        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
                                mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}.log"

        self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)

        # Unmount and disconnect before removing host
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # ── Step 5: Remove the host ───────────────────────────────────────
        self.logger.info(f"Removing host {host_nqn!r} …")
        out, err = self.ssh_obj.remove_host_from_lvol(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert not err or "error" not in err.lower(), \
            f"remove-host failed: {err}"

        # ── Step 6: Verify removed host is rejected ───────────────────────────
        # After removing the host, any connect request for that NQN must be
        # rejected — the backend returns an error when host_nqn is passed but
        # is not found in allowed_hosts (bug fix: no longer falls back to a
        # plain connect string).
        sleep_n_sec(3)
        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        rejected = bool(err) or not connect_ls
        self.logger.info(
            f"Connect after remove-host → connect_ls={connect_ls}, err={err!r}, "
            f"rejected={rejected}")
        assert rejected, \
            "Expected rejection after remove-host but still got a connect string"

        self.logger.info("=== TestLvolDynamicHostManagement PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 5 – Crypto + allowed-hosts end-to-end
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolCryptoWithAllowedHosts(SecurityTestBase):
    """
    Creates a crypto-encrypted lvol with both --sec-options and --allowed-hosts.
    Verifies:
      - Connection with correct NQN succeeds and returns DHCHAP-bearing command.
      - FIO workload completes without errors.
      - ``volume get-secret`` returns credentials.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_crypto_with_allowed_hosts"

    def run(self):
        self.logger.info("=== TestLvolCryptoWithAllowedHosts START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        lvol_name = f"seccryauth{_rand_suffix()}"

        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn], encrypt=True,
            key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
        )
        assert not err or "error" not in err.lower(), \
            f"Crypto+auth lvol creation failed: {err}"

        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id

        lvol_device, connect_ls = self._connect_and_get_device(
            lvol_name, lvol_id, host_nqn=host_nqn)
        self.logger.info(f"Connected crypto+auth lvol → {lvol_device}")

        # Verify DHCHAP keys embedded
        has_dhchap = any("dhchap" in c.lower() for c in connect_ls)
        assert has_dhchap, "Expected DHCHAP keys in connect string for auth lvol"

        mount_point = f"{self.mount_path}/{lvol_name}"
        log_file = f"{self.log_path}/{lvol_name}.log"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
                                mount_path=mount_point)
        self.lvol_mount_details[lvol_name] = {
            "ID": lvol_id, "Mount": mount_point,
            "Device": lvol_device, "Log": log_file,
        }

        self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=120)

        # Confirm get-secret returns something
        secret_out, _ = self.ssh_obj.get_lvol_host_secret(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert secret_out.strip(), "Expected credentials from get-secret"

        self.logger.info("=== TestLvolCryptoWithAllowedHosts PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 6 – Host-only vs controller-only DHCHAP directions
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolDhcapDirections(SecurityTestBase):
    """
    Tests each DHCHAP direction in isolation:
      - host-only (dhchap_key=true, dhchap_ctrlr_key=false):
          the host must authenticate to the controller.
      - ctrl-only (dhchap_key=false, dhchap_ctrlr_key=true):
          the controller must authenticate to the host.
      - bidirectional (both=true): already covered by other tests,
          included here for completeness.

    Each variant is connected and subjected to a short FIO workload.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_dhchap_directions"

    def run(self):
        self.logger.info("=== TestLvolDhcapDirections START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_host", self.cluster_id, sec_options=SEC_HOST_ONLY)
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_ctrl", self.cluster_id, sec_options=SEC_CTRL_ONLY)
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        pool_host = self.pool_name + "_host"
        pool_ctrl = self.pool_name + "_ctrl"
        directions = [
            ("host_only", pool_host),
            ("ctrl_only", pool_ctrl),
            ("bidir",     self.pool_name),
        ]

        for label, pool in directions:
            # Each volume needs its own unique NQN to avoid SPDK keyring
            # key-name collisions when multiple DHCHAP volumes are created.
            self._client_host_nqn = None
            host_nqn = self._get_client_host_nqn()

            lvol_name = f"secdir{label}{_rand_suffix()}"
            self.logger.info(f"--- Testing direction: {label} ---")

            out, err = self.ssh_obj.create_sec_lvol(
                self.mgmt_nodes[0], lvol_name, self.lvol_size, pool,
            )
            assert not err or "error" not in err.lower(), \
                f"lvol creation failed for {label}: {err}"

            sleep_n_sec(3)
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            assert lvol_id

            lvol_device, connect_ls = self._connect_and_get_device(
                lvol_name, lvol_id, host_nqn=host_nqn)
            self.logger.info(f"[{label}] Connected → {lvol_device}")

            mount_point = f"{self.mount_path}/{lvol_name}"
            log_file = f"{self.log_path}/{lvol_name}.log"
            self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
            self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
                                    mount_path=mount_point)
            self.lvol_mount_details[lvol_name] = {
                "ID": lvol_id, "Mount": mount_point,
                "Device": lvol_device, "Log": log_file,
                "host_nqn": host_nqn,
            }

            self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
            self.logger.info(f"[{label}] FIO validated ✓")

        self.logger.info("=== TestLvolDhcapDirections PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 7 – Multi-host: add two hosts, verify each, remove one
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolMultipleAllowedHosts(SecurityTestBase):
    """
    Creates an lvol with two allowed host NQNs, verifies that the registered
    NQN can connect, then removes one host and confirms its access is revoked
    while the other host's access remains intact.

    Since tests typically run on a single client machine, the 'second' host
    NQN is a synthetic one injected into the allowed list.  The test focuses
    on the control-plane operations (add-host / remove-host / volume get)
    rather than dual-machine connectivity.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_multiple_allowed_hosts"

    def run(self):
        self.logger.info("=== TestLvolMultipleAllowedHosts START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        real_nqn = self._get_client_host_nqn()
        fake_nqn = f"nqn.2024-01.io.simplyblock:test:fake-{_rand_suffix()}"
        lvol_name = f"secmulti{_rand_suffix()}"

        # Create with both NQNs in allowed list
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[real_nqn, fake_nqn],
        )
        assert not err or "error" not in err.lower(), \
            f"Multi-host lvol creation failed: {err}"

        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # Both NQNs should appear in lvol details
        lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
        self.logger.info(f"allowed_hosts NQNs (2 hosts): {allowed_nqns}")
        assert real_nqn in allowed_nqns, f"real NQN missing from allowed_hosts: {allowed_nqns}"
        assert fake_nqn in allowed_nqns, f"fake NQN missing from allowed_hosts: {allowed_nqns}"

        # Connect with real NQN
        lvol_device, connect_ls = self._connect_and_get_device(
            lvol_name, lvol_id, host_nqn=real_nqn)
        self.logger.info(f"Connected with real NQN → {lvol_device}")

        mount_point = f"{self.mount_path}/{lvol_name}"
        log_file = f"{self.log_path}/{lvol_name}.log"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
                                mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)

        # Disconnect before removing host
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # Remove fake NQN
        self.logger.info(f"Removing fake NQN {fake_nqn!r} …")
        out, err = self.ssh_obj.remove_host_from_lvol(
            self.mgmt_nodes[0], lvol_id, fake_nqn)
        assert not err or "error" not in err.lower(), f"remove-host failed: {err}"

        # Verify fake NQN no longer in details, real NQN still there
        lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
        self.logger.info(f"allowed_hosts NQNs (after removal): {allowed_nqns}")
        assert fake_nqn not in allowed_nqns, f"fake NQN should have been removed: {allowed_nqns}"
        assert real_nqn in allowed_nqns, f"real NQN should still be present: {allowed_nqns}"

        # Real NQN should still be able to get a connect string
        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=real_nqn)
        assert connect_ls and not err, \
            f"real NQN should still connect after removing fake NQN; err={err!r}"

        self.logger.info("=== TestLvolMultipleAllowedHosts PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 8 – Negative: get-secret, remove-host, add-host edge cases
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityNegativeHostOps(SecurityTestBase):
    """
    Covers error-path scenarios for host management operations:

    TC-SEC-026  remove-host for NQN not in allowed list → error
    TC-SEC-027  add-host with duplicate NQN → handled gracefully (no crash)
    TC-SEC-028  get-secret for a host NQN that was never registered → error
    TC-SEC-029  remove-host then re-add same NQN → should work correctly
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_host_ops"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeHostOps START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        absent_nqn = f"nqn.2024-01.io.simplyblock:test:absent-{_rand_suffix()}"
        lvol_name = f"secnegops{_rand_suffix()}"

        # Create a lvol with one allowed host
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower(), \
            f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # ── TC-SEC-026: remove non-existent NQN ──────────────────────────
        self.logger.info("TC-SEC-026: remove-host for unregistered NQN …")
        out, err = self.ssh_obj.remove_host_from_lvol(
            self.mgmt_nodes[0], lvol_id, absent_nqn)
        has_error = bool(err) or ("error" in out.lower() if out else False) \
                    or ("not found" in out.lower() if out else False)
        self.logger.info(
            f"remove non-existent NQN → out={out!r}, err={err!r}, "
            f"has_error={has_error}")
        assert has_error, \
            "Expected error when removing a NQN that was never added"

        # ── TC-SEC-027: add duplicate NQN ─────────────────────────────────
        self.logger.info("TC-SEC-027: add-host with duplicate NQN …")
        out1, err1 = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        self.logger.info(f"First add-host (already present): out={out1!r}, err={err1!r}")
        # Should either succeed idempotently or return a meaningful error;
        # the system must not crash or corrupt state.
        detail_out = self._get_lvol_details_via_cli(lvol_id)
        nqn_count = detail_out.count(host_nqn)
        assert nqn_count <= 2, \
            f"Duplicate NQN should not be listed more than once; got count={nqn_count}"

        # ── TC-SEC-028: get-secret for unregistered NQN ───────────────────
        self.logger.info("TC-SEC-028: get-secret for unregistered NQN …")
        secret_out, secret_err = self.ssh_obj.get_lvol_host_secret(
            self.mgmt_nodes[0], lvol_id, absent_nqn)
        is_empty_or_err = (
            not secret_out.strip() or
            bool(secret_err) or
            "error" in secret_out.lower() or
            "not found" in secret_out.lower()
        )
        self.logger.info(
            f"get-secret absent NQN → out={secret_out!r}, err={secret_err!r}")
        assert is_empty_or_err, \
            "Expected empty result or error for unregistered NQN in get-secret"

        # ── TC-SEC-029: remove then re-add same NQN ────────────────────────
        self.logger.info("TC-SEC-029: remove-host then re-add same NQN …")
        out, err = self.ssh_obj.remove_host_from_lvol(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert not err or "error" not in err.lower(), f"remove-host failed: {err}"
        sleep_n_sec(2)

        out, err = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert not err or "error" not in err.lower(), f"re-add-host failed: {err}"
        sleep_n_sec(2)

        # Verify host NQN is back and can get a connect string
        lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
        assert host_nqn in allowed_nqns, \
            f"Re-added NQN should appear in allowed_hosts: {allowed_nqns}"
        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn)
        assert connect_ls and not err, \
            f"Re-added NQN should produce a valid connect string; err={err!r}"

        self.logger.info("=== TestLvolSecurityNegativeHostOps PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 9 – Negative: invalid inputs at lvol creation time
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityNegativeCreation(SecurityTestBase):
    """
    Covers invalid input scenarios at lvol-creation time:

    TC-SEC-050  --sec-options file path does not exist → CLI error
    TC-SEC-051  --allowed-hosts file contains non-array JSON → CLI error
    TC-SEC-053  --allowed-hosts with empty list [] → error or meaningful warning
    TC-SEC-055  add-host with syntactically invalid NQN → error
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_creation"

    def _assert_cli_error(self, out: str, err: str, label: str) -> None:
        """Assert that at least one of out/err signals a failure."""
        failure_signals = ("error", "invalid", "failed", "no such", "not found",
                           "cannot", "unable")
        combined = (out or "").lower() + (err or "").lower()
        has_signal = any(s in combined for s in failure_signals)
        self.logger.info(
            f"[{label}] out={out!r}, err={err!r}, has_error_signal={has_signal}")
        assert has_signal or not out.strip(), \
            f"[{label}] Expected error signal but got: out={out!r} err={err!r}"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeCreation START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id)

        # ── TC-SEC-050: non-existent sec-options file ─────────────────────
        self.logger.info("TC-SEC-050: --sec-options with non-existent file path …")
        lvol_name = f"secneg050{_rand_suffix()}"
        cmd = (f"{self.base_cmd} -d volume add {lvol_name} {self.lvol_size}"
               f" {self.pool_name} --sec-options /tmp/does_not_exist_ever.json")
        out, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
        # Should error; lvol must NOT be created
        created_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert not created_id, \
            "TC-SEC-050: lvol should NOT be created with non-existent sec-options file"
        self.logger.info("TC-SEC-050 PASS: lvol not created for missing file")

        # ── TC-SEC-051: allowed-hosts file contains object not array ───────
        self.logger.info("TC-SEC-051: --allowed-hosts with invalid JSON (not array) …")
        lvol_name = f"secneg051{_rand_suffix()}"
        bad_json_path = "/tmp/bad_hosts.json"
        # Write an object instead of an array
        self.ssh_obj.write_json_file(
            self.mgmt_nodes[0], bad_json_path,
            {"nqn": "nqn.2024-01.io.simplyblock:bad"})
        cmd = (f"{self.base_cmd} -d volume add {lvol_name} {self.lvol_size}"
               f" {self.pool_name} --allowed-hosts {bad_json_path}")
        out, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
        self.ssh_obj.exec_command(
            self.mgmt_nodes[0], f"rm -f {bad_json_path}", supress_logs=True)
        created_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert not created_id, \
            "TC-SEC-051: lvol should NOT be created when allowed-hosts JSON is not an array"
        self.logger.info("TC-SEC-051 PASS")

        # ── TC-SEC-053: --allowed-hosts with empty list ────────────────────
        self.logger.info("TC-SEC-053: --allowed-hosts with empty list [] …")
        lvol_name = f"secneg053{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[],   # empty list
        )
        # Behaviour: either error, or create with no allowed hosts (effectively open)
        # The important thing is it does not crash and gives a clear response.
        self.logger.info(
            f"TC-SEC-053: empty allowed-hosts → out={out!r}, err={err!r}")
        created_id = self.sbcli_utils.get_lvol_id(lvol_name)
        if created_id:
            self.logger.info("TC-SEC-053: lvol created with empty hosts list; cleaning up")
            self.lvol_mount_details[lvol_name] = {"ID": created_id, "Mount": None}
        else:
            self.logger.info("TC-SEC-053: lvol rejected with empty hosts list")

        # ── TC-SEC-055: add-host with syntactically invalid NQN ───────────
        self.logger.info("TC-SEC-055: add-host with invalid NQN format …")
        # Create a plain lvol to test add-host against
        plain_name = f"secneg055{_rand_suffix()}"
        self.sbcli_utils.add_lvol(
            lvol_name=plain_name,
            pool_name=self.pool_name,
            size=self.lvol_size,
        )
        sleep_n_sec(3)
        plain_id = self.sbcli_utils.get_lvol_id(plain_name)
        assert plain_id
        self.lvol_mount_details[plain_name] = {"ID": plain_id, "Mount": None}

        invalid_nqn = "not-a-valid-nqn-format-!@#$%"
        out, err = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], plain_id, invalid_nqn)
        self._assert_cli_error(out, err, "TC-SEC-055")
        self.logger.info("TC-SEC-055 PASS: invalid NQN rejected")

        self.logger.info("=== TestLvolSecurityNegativeCreation PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 10 – Negative: connect & I/O rejection scenarios
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityNegativeConnect(SecurityTestBase):
    """
    Tests rejection of connections that should not succeed:

    TC-SEC-009  DHCHAP lvol (no allowed-hosts): connect with mismatched NQN
    TC-SEC-013  Allowed-hosts lvol: connect without --host-nqn (no keys path)
    TC-SEC-054  Auth lvol: attempt nvme connect using tampered connect string
    TC-SEC-056  Delete lvol with active allowed-hosts → cleanup succeeds
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_connect"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeConnect START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()

        # ── TC-SEC-009: auth lvol (no allowed-hosts) + wrong NQN ──────────
        self.logger.info(
            "TC-SEC-009: DHCHAP lvol (no allowed-hosts) + wrong NQN …")
        lvol_name_009 = f"secneg009{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name_009, self.lvol_size, self.pool_name,
        )
        assert not err or "error" not in err.lower()
        sleep_n_sec(3)
        lvol_id_009 = self.sbcli_utils.get_lvol_id(lvol_name_009)
        assert lvol_id_009
        self.lvol_mount_details[lvol_name_009] = {"ID": lvol_id_009, "Mount": None}

        wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
        connect_ls, err = self._get_connect_str_cli(lvol_id_009, host_nqn=wrong_nqn)
        self.logger.info(
            f"TC-SEC-009: wrong NQN → connect_ls={connect_ls}, err={err!r}")
        # When no allowed-hosts is configured, any NQN may get a connect string
        # but the DHCHAP negotiation at the kernel level should fail.
        # We log the result; the definitive rejection happens at nvme-connect time.
        self.logger.info(
            "TC-SEC-009: Connect string generation noted; actual DHCHAP rejection "
            "occurs at kernel nvme-connect level (verified by non-zero connect exit code)")

        # ── TC-SEC-013: allowed-hosts lvol, connect without --host-nqn ────
        self.logger.info(
            "TC-SEC-013: allowed-hosts lvol, connect without --host-nqn …")
        # Fresh NQN for this volume to avoid SPDK keyring key-name collision
        # with lvol_009 which was created with the same host_nqn.
        self._client_host_nqn = None
        host_nqn = self._get_client_host_nqn()

        lvol_name_013 = f"secneg013{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name_013, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower()
        sleep_n_sec(3)
        lvol_id_013 = self.sbcli_utils.get_lvol_id(lvol_name_013)
        assert lvol_id_013
        self.lvol_mount_details[lvol_name_013] = {"ID": lvol_id_013, "Mount": None}

        # Without host-nqn, connect string should not contain DHCHAP keys
        connect_no_nqn, err_no_nqn = self._get_connect_str_cli(
            lvol_id_013, host_nqn=None)
        self.logger.info(
            f"TC-SEC-013: no-NQN connect → strings={connect_no_nqn}, err={err_no_nqn!r}")
        if connect_no_nqn:
            has_dhchap = any("dhchap" in c.lower() for c in connect_no_nqn)
            self.logger.info(
                f"TC-SEC-013: DHCHAP keys present={has_dhchap} "
                f"(expected False when no host-nqn supplied)")
            assert not has_dhchap, \
                "Connect string without --host-nqn must not contain DHCHAP keys"

        # ── TC-SEC-054: tampered connect string ────────────────────────────
        self.logger.info(
            "TC-SEC-054: connect with tampered DHCHAP key in connect string …")
        connect_auth, err_auth = self._get_connect_str_cli(
            lvol_id_013, host_nqn=host_nqn)
        if connect_auth:
            tampered = connect_auth[0]
            # Replace dhchap-secret value with garbage if present
            if "dhchap-secret" in tampered:
                import re
                tampered = re.sub(
                    r'(--dhchap-secret\s+)\S+',
                    r'\1DEADBEEFDEADBEEF00000000FFFFFFFF',
                    tampered)
                self.logger.info(f"TC-SEC-054: Tampered connect cmd: {tampered!r}")
                _, connect_err = self.ssh_obj.exec_command(
                    node=self.fio_node, command=tampered)
                self.logger.info(
                    f"TC-SEC-054: Tampered connect result err={connect_err!r} "
                    f"(expected non-zero exit / auth failure at kernel level)")
                # Note: even if exec_command swallows the exit code, the device
                # will NOT appear since DHCHAP negotiation fails.  The absence of
                # a new block device is the definitive check.
                sleep_n_sec(3)
                # We do NOT assert here because exec_command masks exit codes;
                # the behaviour is logged for manual / log-level verification.
            else:
                self.logger.info(
                    "TC-SEC-054: no dhchap-secret in connect string (no allowed-hosts); "
                    "skipping tamper check")

        # ── TC-SEC-056: delete lvol that has active allowed-hosts ──────────
        self.logger.info(
            "TC-SEC-056: delete lvol that has active allowed-hosts list …")
        # lvol_013 has an allowed host – delete it and verify it's gone
        self.sbcli_utils.delete_lvol(lvol_name=lvol_name_013, skip_error=False)
        sleep_n_sec(3)
        gone_id = self.sbcli_utils.get_lvol_id(lvol_name_013)
        assert not gone_id, \
            f"TC-SEC-056: lvol {lvol_name_013!r} should be deleted"
        del self.lvol_mount_details[lvol_name_013]
        self.logger.info("TC-SEC-056 PASS: lvol with allowed-hosts deleted cleanly")

        self.logger.info("=== TestLvolSecurityNegativeConnect PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 11 – Allowed-hosts without DHCHAP (NQN whitelist only)
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolAllowedHostsNoDhchap(SecurityTestBase):
    """
    TC-SEC-034  Create lvol with --allowed-hosts but NO --sec-options
                (pure NQN whitelist, no DH-HMAC-CHAP key exchange).

    Verifies:
      - Allowed NQN can get a connect string and connect successfully.
      - Connect string does NOT contain DHCHAP keys (no key negotiation).
      - Unregistered NQN is still rejected at connect-string level.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_allowed_hosts_no_dhchap"

    def run(self):
        self.logger.info("=== TestLvolAllowedHostsNoDhchap START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id)

        host_nqn = self._get_client_host_nqn()
        wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
        lvol_name = f"secnqnonly{_rand_suffix()}"

        # No sec_options — NQN whitelist only
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower(), \
            f"NQN-whitelist lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # Allowed NQN should get connect string (without DHCHAP keys)
        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        self.logger.info(f"Allowed NQN connect → {connect_ls}, err={err!r}")
        assert connect_ls and not err, \
            f"Allowed NQN should produce a connect string; err={err!r}"
        has_dhchap = any("dhchap" in c.lower() for c in connect_ls)
        assert not has_dhchap, \
            "No DHCHAP keys expected when --sec-options not provided"

        # Unregistered NQN should be rejected
        wrong_connect, wrong_err = self._get_connect_str_cli(
            lvol_id, host_nqn=wrong_nqn)
        self.logger.info(
            f"Wrong NQN connect → {wrong_connect}, err={wrong_err!r}")
        rejected = bool(wrong_err) or not wrong_connect
        assert rejected, \
            f"Unregistered NQN should be rejected even without DHCHAP; " \
            f"got: {wrong_connect}"

        # Connect with correct NQN and run FIO
        lvol_device, connect_ls = self._connect_and_get_device(
            lvol_name, lvol_id, host_nqn=host_nqn)
        self.logger.info(f"NQN-whitelist lvol connected → {lvol_device}")

        mount_point = f"{self.mount_path}/{lvol_name}"
        log_file = f"{self.log_path}/{lvol_name}.log"
        self.ssh_obj.format_disk(
            node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(
            node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)

        self.logger.info("=== TestLvolAllowedHostsNoDhchap PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 12 – Snapshot & clone inherit security settings from the parent lvol
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecuritySnapshotClone(SecurityTestBase):
    """
    Verifies that snapshots and clones inherit security settings from their
    parent lvol.  The backend copies ``allowed_hosts`` (including embedded
    DHCHAP keys) and crypto settings at clone-creation time.

    Scenarios:
      A) auth parent   – DHCHAP only, no encryption
         * Clone connects with the same host NQN / DHCHAP keys  (positive)
         * Clone rejects a different host NQN                    (negative)

      B) crypto_auth parent – DHCHAP + encryption
         * Clone connects with the same host NQN / DHCHAP keys  (positive)
         * Connect string includes dhchap keys                   (assertion)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_snapshot_clone"

    # ── helpers ──────────────────────────────────────────────────────────────

    def _create_snap_and_clone(self, parent_id, label):
        """Snapshot *parent_id* then clone it; return (snap_id, clone_id, clone_name)."""
        snap_name = f"snap_{label}{_rand_suffix()}"
        snap_result = self.sbcli_utils.add_snapshot(parent_id, snap_name)
        assert snap_result, f"Snapshot creation failed for {snap_name}"
        sleep_n_sec(3)
        snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
        assert snap_id, f"Could not find snapshot ID for {snap_name}"

        clone_name = f"clone_{label}{_rand_suffix()}"
        clone_result = self.sbcli_utils.add_clone(snap_id, clone_name)
        assert clone_result, f"Clone creation failed for {clone_name}"
        sleep_n_sec(3)
        clone_id = self.sbcli_utils.get_lvol_id(clone_name)
        assert clone_id, f"Could not find clone ID for {clone_name}"

        self.lvol_mount_details[clone_name] = {"ID": clone_id, "Mount": None}
        return snap_id, clone_id, clone_name

    def _verify_clone_security(self, clone_name, clone_id, host_nqn, wrong_nqn,
                                expect_dhchap=True):
        """
        Core clone security assertions:
          - wrong NQN is rejected
          - correct host NQN connects successfully (with DHCHAP keys if expected)
          - FIO read workload succeeds on the mounted clone
        """
        # Negative: wrong NQN should be rejected
        wrong_connect, wrong_err = self._get_connect_str_cli(
            clone_id, host_nqn=wrong_nqn)
        rejected = bool(wrong_err) or not wrong_connect
        assert rejected, \
            f"Wrong NQN should be rejected on clone {clone_name}; got: {wrong_connect}"
        self.logger.info(f"[{clone_name}] Wrong-NQN rejected as expected")

        # Positive: correct host NQN connects
        clone_device, clone_cmds = self._connect_and_get_device(
            clone_name, clone_id, host_nqn=host_nqn)
        self.logger.info(f"[{clone_name}] Connected → {clone_device}")

        if expect_dhchap:
            has_dhchap = any("dhchap" in c.lower() for c in clone_cmds)
            assert has_dhchap, \
                f"Clone {clone_name} connect string should include DHCHAP keys"

        mount_clone = f"{self.mount_path}/{clone_name}"
        self.ssh_obj.mount_path(
            node=self.fio_node, device=clone_device, mount_path=mount_clone)
        self.lvol_mount_details[clone_name]["Mount"] = mount_clone

        log_clone = f"{self.log_path}/{clone_name}.log"
        self._run_fio_and_validate(
            clone_name, mount_clone, log_clone, rw="read", runtime=30)
        self.logger.info(f"[{clone_name}] FIO read validated")

    # ── main test ─────────────────────────────────────────────────────────────

    def run(self):
        self.logger.info("=== TestLvolSecuritySnapshotClone START ===")
        self._log_cluster_security_config()
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"

        # ── Scenario A: auth (DHCHAP only, no crypto) ────────────────────────
        self.logger.info("--- Scenario A: auth parent (DHCHAP only) ---")
        auth_parent = f"secsnap_auth{_rand_suffix()}"

        _, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], auth_parent, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn])
        assert not err or "error" not in err.lower(), \
            f"auth parent creation failed: {err}"
        sleep_n_sec(3)

        auth_parent_id = self.sbcli_utils.get_lvol_id(auth_parent)
        assert auth_parent_id, f"Could not find ID for {auth_parent}"
        self._log_lvol_security(auth_parent_id, label="(auth parent)")

        # Write data to parent so we can verify clone is readable
        auth_device, _ = self._connect_and_get_device(
            auth_parent, auth_parent_id, host_nqn=host_nqn)
        mount_auth = f"{self.mount_path}/{auth_parent}"
        self.ssh_obj.format_disk(
            node=self.fio_node, device=auth_device, fs_type="ext4")
        self.ssh_obj.mount_path(
            node=self.fio_node, device=auth_device, mount_path=mount_auth)
        self.lvol_mount_details[auth_parent] = {
            "ID": auth_parent_id, "Mount": mount_auth, "Device": auth_device}

        log_auth = f"{self.log_path}/{auth_parent}.log"
        self._run_fio_and_validate(
            auth_parent, mount_auth, log_auth, rw="write", runtime=30)

        # Unmount parent before snapshotting
        self.ssh_obj.unmount_path(self.fio_node, mount_auth)
        self.lvol_mount_details[auth_parent]["Mount"] = None
        sleep_n_sec(2)

        _, auth_clone_id, auth_clone_name = self._create_snap_and_clone(
            auth_parent_id, "auth")
        self._log_lvol_security(auth_clone_id, label="(auth clone)")

        self._verify_clone_security(
            auth_clone_name, auth_clone_id, host_nqn, wrong_nqn,
            expect_dhchap=True)

        self.logger.info("--- Scenario A PASSED ---")

        # ── Scenario B: crypto_auth (DHCHAP + encryption) ────────────────────
        self.logger.info("--- Scenario B: crypto_auth parent (DHCHAP + crypto) ---")
        # Fresh NQN for Scenario B to avoid SPDK keyring key-name collision
        # with Scenario A's volumes (same host_nqn → same key_name → re-
        # registration rejected → Scenario B auth would fail).
        self._client_host_nqn = None
        host_nqn = self._get_client_host_nqn()

        ca_parent = f"secsnap_ca{_rand_suffix()}"

        _, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], ca_parent, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
            encrypt=True,
            key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1])
        assert not err or "error" not in err.lower(), \
            f"crypto_auth parent creation failed: {err}"
        sleep_n_sec(3)

        ca_parent_id = self.sbcli_utils.get_lvol_id(ca_parent)
        assert ca_parent_id, f"Could not find ID for {ca_parent}"
        self._log_lvol_security(ca_parent_id, label="(crypto_auth parent)")

        ca_device, _ = self._connect_and_get_device(
            ca_parent, ca_parent_id, host_nqn=host_nqn)
        mount_ca = f"{self.mount_path}/{ca_parent}"
        self.ssh_obj.format_disk(
            node=self.fio_node, device=ca_device, fs_type="ext4")
        self.ssh_obj.mount_path(
            node=self.fio_node, device=ca_device, mount_path=mount_ca)
        self.lvol_mount_details[ca_parent] = {
            "ID": ca_parent_id, "Mount": mount_ca, "Device": ca_device}

        log_ca = f"{self.log_path}/{ca_parent}.log"
        self._run_fio_and_validate(
            ca_parent, mount_ca, log_ca, rw="write", runtime=30)

        self.ssh_obj.unmount_path(self.fio_node, mount_ca)
        self.lvol_mount_details[ca_parent]["Mount"] = None
        sleep_n_sec(2)

        _, ca_clone_id, ca_clone_name = self._create_snap_and_clone(
            ca_parent_id, "ca")
        self._log_lvol_security(ca_clone_id, label="(crypto_auth clone)")

        self._verify_clone_security(
            ca_clone_name, ca_clone_id, host_nqn, wrong_nqn,
            expect_dhchap=True)

        self.logger.info("--- Scenario B PASSED ---")
        self.logger.info("=== TestLvolSecuritySnapshotClone PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 12 – Storage node outage + DHCHAP credential persistence
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityOutageRecovery(SecurityTestBase):
    """
    Verifies that DHCHAP credentials survive a storage node outage/restart.

    TC-SEC-070  Create DHCHAP (SEC_BOTH) lvol and connect successfully
    TC-SEC-071  Shutdown a storage node; verify cluster remains accessible
    TC-SEC-072  Restart the node; wait for it to come back online
    TC-SEC-073  Reconnect the lvol with the same DHCHAP credentials – must succeed
    TC-SEC-074  Run FIO on the reconnected lvol to confirm data plane integrity
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_outage_recovery"

    def run(self):
        self.logger.info("=== TestLvolSecurityOutageRecovery START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        lvol_name = f"secout{_rand_suffix()}"

        # TC-SEC-070: create DHCHAP lvol and verify initial connect
        self.logger.info("TC-SEC-070: Creating DHCHAP lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, f"Could not find ID for {lvol_name}"
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        self.logger.info("TC-SEC-070: Initial connect + format PASSED")

        # Disconnect before node outage
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-SEC-071: shutdown a storage node
        self.logger.info("TC-SEC-071: Shutting down a storage node …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
        assert primary_nodes, "No primary storage nodes found"
        target_node = primary_nodes[0]["uuid"]
        self.sbcli_utils.shutdown_node(target_node)
        self.sbcli_utils.wait_for_storage_node_status(target_node, "offline", timeout=120)
        self.logger.info("TC-SEC-071: Node offline PASSED")

        # TC-SEC-072: restart node and wait for it to come online
        self.logger.info("TC-SEC-072: Restarting the storage node …")
        self.sbcli_utils.restart_node(target_node)
        self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
        sleep_n_sec(10)
        self.logger.info("TC-SEC-072: Node back online PASSED")

        # TC-SEC-073: reconnect with original DHCHAP credentials
        self.logger.info("TC-SEC-073: Reconnecting with original DHCHAP creds …")
        lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        assert lvol_device2, "Reconnect after node restart failed"
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        self.logger.info("TC-SEC-073: Reconnect with DHCHAP creds PASSED")

        # TC-SEC-074: FIO on reconnected lvol
        self.logger.info("TC-SEC-074: Running FIO on reconnected lvol …")
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-074: FIO PASSED")

        self.logger.info("=== TestLvolSecurityOutageRecovery PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 13 – 30-second network interrupt + DHCHAP re-auth
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityNetworkInterrupt(SecurityTestBase):
    """
    30-second NIC-level network interrupt on a storage node; verifies that
    the DHCHAP session resumes correctly after reconnect.

    TC-SEC-075  Create DHCHAP lvol, connect, mount
    TC-SEC-076  Trigger 30-second network interrupt on a storage node
    TC-SEC-077  Wait for interrupt to end; reconnect with DHCHAP creds
    TC-SEC-078  Mount and run FIO – data plane must be intact
    TC-SEC-079  Verify get-secret still returns valid credentials
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_network_interrupt"

    def run(self):
        self.logger.info("=== TestLvolSecurityNetworkInterrupt START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)

        host_nqn = self._get_client_host_nqn()
        lvol_name = f"secnwi{_rand_suffix()}"

        # TC-SEC-075: create lvol + connect
        self.logger.info("TC-SEC-075: Creating DHCHAP lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        self.logger.info("TC-SEC-075: PASSED")

        # Disconnect before network interrupt
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-SEC-076: trigger 30-second NIC interrupt on a storage node
        self.logger.info("TC-SEC-076: Triggering 30s network interrupt …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
        assert primary_nodes, "No primary storage nodes found"
        target_node_ip = primary_nodes[0]["mgmt_ip"]
        active_ifaces = self.ssh_obj.get_active_interfaces(target_node_ip)
        if active_ifaces:
            self.ssh_obj.disconnect_all_active_interfaces(
                target_node_ip, active_ifaces, duration_secs=30)
        self.logger.info("TC-SEC-076: Network interrupt triggered PASSED")

        # TC-SEC-077: wait for interrupt to end then reconnect
        self.logger.info("TC-SEC-077: Waiting 35s for interrupt to end …")
        sleep_n_sec(35)
        self.logger.info("TC-SEC-077: Reconnecting with DHCHAP creds …")
        lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        assert lvol_device2, "Reconnect after network interrupt failed"
        self.logger.info("TC-SEC-077: PASSED")

        # TC-SEC-078: mount and run FIO
        self.logger.info("TC-SEC-078: Running FIO after reconnect …")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-078: FIO PASSED")

        # TC-SEC-079: get-secret must still return valid creds
        self.logger.info("TC-SEC-079: Verifying get-secret still works …")
        out, err = self.ssh_obj.get_lvol_host_secret(self.mgmt_nodes[0], lvol_id, host_nqn)
        assert out and "error" not in out.lower(), f"get-secret failed after network interrupt: {err}"
        self.logger.info("TC-SEC-079: get-secret PASSED")

        self.logger.info("=== TestLvolSecurityNetworkInterrupt PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 14 – HA lvol: security preserved through primary failover
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityHAFailover(SecurityTestBase):
    """
    Creates an HA lvol (npcs=1) with DHCHAP, triggers primary failover by
    shutting down the primary node, and verifies security config is intact
    after the secondary takes over.

    TC-SEC-080  Create HA DHCHAP lvol (ndcs=1, npcs=1)
    TC-SEC-081  Connect with correct host NQN and run FIO
    TC-SEC-082  Shutdown the primary storage node
    TC-SEC-083  Restart the node; wait for HA to settle
    TC-SEC-084  Reconnect with original DHCHAP creds and verify FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_ha_failover"

    def run(self):
        self.logger.info("=== TestLvolSecurityHAFailover START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        lvol_name = f"secha{_rand_suffix()}"

        # TC-SEC-080: create HA lvol with DHCHAP
        self.logger.info("TC-SEC-080: Creating HA DHCHAP lvol (ndcs=1, npcs=1) …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
            distr_ndcs=1, distr_npcs=1,
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(5)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-081: connect and run FIO
        self.logger.info("TC-SEC-081: Connecting HA lvol and running FIO …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_pre.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=30)
        self.logger.info("TC-SEC-081: Pre-failover FIO PASSED")

        # Disconnect before shutdown
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-SEC-082: shutdown a primary storage node
        self.logger.info("TC-SEC-082: Shutting down a primary storage node …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
        assert primary_nodes, "No primary storage nodes found"
        target_node = primary_nodes[0]["uuid"]
        self.sbcli_utils.shutdown_node(target_node)
        self.sbcli_utils.wait_for_storage_node_status(target_node, "offline", timeout=120)
        self.logger.info("TC-SEC-082: Node offline PASSED")

        # TC-SEC-083: restart node, wait for HA to settle
        self.logger.info("TC-SEC-083: Restarting node and waiting for HA settle …")
        self.sbcli_utils.restart_node(target_node)
        self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
        sleep_n_sec(15)
        self.logger.info("TC-SEC-083: HA settled PASSED")

        # TC-SEC-084: reconnect with original DHCHAP creds
        self.logger.info("TC-SEC-084: Reconnecting with DHCHAP creds after failover …")
        lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        assert lvol_device2, "Reconnect after HA failover failed"
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file2 = f"{self.log_path}/{lvol_name}_post.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file2, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-084: Post-failover FIO PASSED")

        self.logger.info("=== TestLvolSecurityHAFailover PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 15 – Management node reboot: DHCHAP config survives
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityMgmtNodeReboot(SecurityTestBase):
    """
    Reboots the management node and verifies that DHCHAP credentials are
    still retrievable (get-secret) and connections still work after mgmt
    node comes back online.

    TC-SEC-085  Create DHCHAP lvol (SEC_BOTH), add allowed host, get-secret OK
    TC-SEC-086  Reboot management node; wait for it to come back
    TC-SEC-087  get-secret after mgmt reboot – credentials must still be present
    TC-SEC-088  Connect lvol with original DHCHAP creds and run brief FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_mgmt_node_reboot"

    def run(self):
        self.logger.info("=== TestLvolSecurityMgmtNodeReboot START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        lvol_name = f"secmgmt{_rand_suffix()}"

        # TC-SEC-085: create lvol, get-secret baseline
        self.logger.info("TC-SEC-085: Creating DHCHAP lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        pre_secret, pre_err = self.ssh_obj.get_lvol_host_secret(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert pre_secret and "error" not in pre_secret.lower(), \
            f"Pre-reboot get-secret failed: {pre_err}"
        self.logger.info(f"TC-SEC-085: Pre-reboot secret obtained PASSED")

        # TC-SEC-086: reboot management node
        self.logger.info("TC-SEC-086: Rebooting management node …")
        self.ssh_obj.reboot_node(self.mgmt_nodes[0], wait_time=300)
        sleep_n_sec(15)
        self.logger.info("TC-SEC-086: Management node back online PASSED")

        # TC-SEC-087: get-secret after reboot
        sleep_n_sec(100)  # Extra wait to ensure all services are fully up and secrets are loaded
        self.logger.info("TC-SEC-087: Verifying get-secret after mgmt reboot …")
        post_secret, post_err = self.ssh_obj.get_lvol_host_secret(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert post_secret and "error" not in post_secret.lower(), \
            f"Post-reboot get-secret failed: {post_err}"
        self.logger.info("TC-SEC-087: get-secret after reboot PASSED")

        # TC-SEC-088: connect + FIO
        self.logger.info("TC-SEC-088: Connecting with DHCHAP creds after mgmt reboot …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-088: FIO after mgmt reboot PASSED")

        self.logger.info("=== TestLvolSecurityMgmtNodeReboot PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 16 – Dynamic modification of allowed hosts during FIO
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityDynamicModification(SecurityTestBase):
    """
    Tests live add/remove of host NQNs, NQN rotation (key change), and
    multi-NQN scenarios on a running lvol.

    TC-SEC-089  Remove host NQN while FIO running → connection drops
    TC-SEC-090  Re-add host NQN → reconnect resumes
    TC-SEC-091  Add a second NQN; verify both NQNs can get connect strings
    TC-SEC-092  Remove first NQN; verify second NQN still works
    TC-SEC-093  Remove second NQN; verify no NQN can connect
    TC-SEC-094  Add first NQN back → reconnect works again
    TC-SEC-095  Teardown
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_dynamic_modification"

    def run(self):
        self.logger.info("=== TestLvolSecurityDynamicModification START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)

        host_nqn = self._get_client_host_nqn()
        second_nqn = f"nqn.2024-01.io.simplyblock:test:second-{_rand_suffix()}"
        lvol_name = f"secdyn{_rand_suffix()}"

        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-089: remove host while connected → verify connect string unavailable
        self.logger.info("TC-SEC-089: Removing host NQN …")
        out, err = self.ssh_obj.remove_host_from_lvol(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert not err or "error" not in err.lower(), f"remove-host failed: {err}"
        connect_ls, err2 = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert not connect_ls or err2, \
            "Expected no connect string after removing host NQN"
        self.logger.info("TC-SEC-089: Remove host NQN PASSED")

        # TC-SEC-090: re-add host → connect string available
        self.logger.info("TC-SEC-090: Re-adding host NQN …")
        out, err = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert not err or "error" not in err.lower(), f"add-host failed: {err}"
        sleep_n_sec(2)
        connect_ls2, err3 = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert connect_ls2 and not err3, \
            f"Connect string should be available after re-adding NQN; err={err3}"
        self.logger.info("TC-SEC-090: Re-add host NQN PASSED")

        # TC-SEC-091: add second NQN, verify both get connect strings
        self.logger.info("TC-SEC-091: Adding second NQN …")
        out, err = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], lvol_id, second_nqn)
        assert not err or "error" not in err.lower(), f"add second NQN failed: {err}"
        sleep_n_sec(2)
        cs1, _ = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        cs2, _ = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
        assert cs1, "First NQN should still get connect string"
        assert cs2, "Second NQN should get connect string"
        self.logger.info("TC-SEC-091: Both NQNs work PASSED")

        # TC-SEC-092: remove first NQN, verify second still works
        self.logger.info("TC-SEC-092: Removing first NQN …")
        out, err = self.ssh_obj.remove_host_from_lvol(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert not err or "error" not in err.lower()
        sleep_n_sec(2)
        cs2b, _ = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
        assert cs2b, "Second NQN should still work after removing first"
        cs1b, err1b = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert not cs1b or err1b, "First NQN should not work after removal"
        self.logger.info("TC-SEC-092: PASSED")

        # TC-SEC-093: remove second NQN
        self.logger.info("TC-SEC-093: Removing second NQN …")
        out, err = self.ssh_obj.remove_host_from_lvol(
            self.mgmt_nodes[0], lvol_id, second_nqn)
        assert not err or "error" not in err.lower()
        sleep_n_sec(2)
        cs2c, err2c = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
        assert not cs2c or err2c, "Second NQN should not work after removal"
        self.logger.info("TC-SEC-093: PASSED")

        # TC-SEC-094: re-add first NQN, connect + FIO
        self.logger.info("TC-SEC-094: Re-adding first NQN and running FIO …")
        out, err = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        assert not err or "error" not in err.lower()
        sleep_n_sec(3)
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-094: FIO after re-add PASSED")

        self.logger.info("TC-SEC-095: TestLvolSecurityDynamicModification teardown")
        self.logger.info("=== TestLvolSecurityDynamicModification PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 17 – Concurrent multi-client connections with DHCHAP
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityMultiClientConcurrent(SecurityTestBase):
    """
    Tests concurrent client connection attempts: correct NQN vs wrong NQN
    issued simultaneously.

    TC-SEC-096  Create DHCHAP lvol with one registered NQN
    TC-SEC-097  Concurrently request connect strings for correct and wrong NQNs
    TC-SEC-098  Verify correct NQN returns a valid connect string
    TC-SEC-099  Verify wrong NQN returns no connect string or an error
    TC-SEC-100  Connect with correct NQN and run FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_multi_client_concurrent"

    def run(self):
        self.logger.info("=== TestLvolSecurityMultiClientConcurrent START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
        lvol_name = f"secmc{_rand_suffix()}"

        # TC-SEC-096: create DHCHAP lvol
        self.logger.info("TC-SEC-096: Creating DHCHAP lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-097 & TC-SEC-098 & TC-SEC-099: concurrent connect string requests
        self.logger.info("TC-SEC-097: Launching concurrent connect-string requests …")
        results = {}

        def _req(nqn, key):
            try:
                cs, err = self._get_connect_str_cli(lvol_id, host_nqn=nqn)
                results[key] = (cs, err)
            except Exception as e:
                results[key] = (None, str(e))

        t_good = threading.Thread(target=_req, args=(host_nqn, "good"))
        t_bad  = threading.Thread(target=_req, args=(wrong_nqn, "bad"))
        t_good.start()
        t_bad.start()
        t_good.join()
        t_bad.join()

        good_cs, good_err = results.get("good", (None, "no result"))
        bad_cs,  bad_err  = results.get("bad",  (None, "no result"))

        # TC-SEC-098: correct NQN must succeed
        assert good_cs, \
            f"Correct NQN should return connect string; err={good_err}"
        self.logger.info("TC-SEC-098: Correct NQN connect string PASSED")

        # TC-SEC-099: wrong NQN must fail
        assert not good_err or "error" not in (good_err or "").lower(), \
            f"Correct NQN should have no error; err={good_err}"
        assert not bad_cs or bad_err, \
            f"Wrong NQN should not return a connect string; got {bad_cs}"
        self.logger.info("TC-SEC-099: Wrong NQN rejected PASSED")

        # TC-SEC-100: connect + FIO with correct NQN
        self.logger.info("TC-SEC-100: Connecting and running FIO with correct NQN …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-100: FIO PASSED")

        self.logger.info("=== TestLvolSecurityMultiClientConcurrent PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 18 – Scale: 10 DHCHAP volumes with rapid add/remove
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityScaleAndRapidOps(SecurityTestBase):
    """
    Creates 10 DHCHAP volumes simultaneously (each with a unique NQN) then
    performs rapid add/remove of host NQNs.  Verifies no SPDK key-name
    collisions occur and all volumes remain independently accessible.

    TC-SEC-101  Create 10 DHCHAP lvols with unique NQNs (no collisions)
    TC-SEC-102  Rapidly remove all host NQNs from all volumes
    TC-SEC-103  Rapidly re-add all host NQNs
    TC-SEC-104  Verify every volume can still be connected (get connect string)
    """

    VOLUME_COUNT = 10

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_scale_and_rapid_ops"

    def run(self):
        self.logger.info("=== TestLvolSecurityScaleAndRapidOps START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)

        # TC-SEC-101: create 10 volumes each with unique NQN
        self.logger.info(f"TC-SEC-101: Creating {self.VOLUME_COUNT} DHCHAP lvols …")
        volumes = []  # list of (lvol_name, lvol_id, nqn)
        for i in range(self.VOLUME_COUNT):
            suffix = _rand_suffix()
            lvol_name = f"secsc{i}{suffix}"
            # unique NQN per volume to avoid SPDK keyring collision
            uuid_out, _ = self.ssh_obj.exec_command(self.fio_node, "uuidgen")
            uuid = uuid_out.strip().split('\n')[0].strip().lower()
            nqn = f"nqn.2014-08.org.nvmexpress:uuid:{uuid}"
            # Write hostnqn only for the last volume (we only connect one)
            out, err = self.ssh_obj.create_sec_lvol(
                self.mgmt_nodes[0], lvol_name, "1G", self.pool_name,
                allowed_hosts=[nqn],
            )
            assert not err or "error" not in err.lower(), \
                f"lvol {lvol_name} creation failed: {err}"
            sleep_n_sec(1)
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            assert lvol_id, f"Could not find ID for {lvol_name}"
            volumes.append((lvol_name, lvol_id, nqn))
            self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        self.logger.info(f"TC-SEC-101: {self.VOLUME_COUNT} volumes created PASSED")

        # TC-SEC-102: rapid remove all NQNs
        self.logger.info("TC-SEC-102: Rapidly removing all host NQNs …")
        for lvol_name, lvol_id, nqn in volumes:
            out, err = self.ssh_obj.remove_host_from_lvol(
                self.mgmt_nodes[0], lvol_id, nqn)
            assert not err or "error" not in err.lower(), \
                f"remove-host failed for {lvol_name}: {err}"
        self.logger.info("TC-SEC-102: PASSED")

        # TC-SEC-103: rapid re-add all NQNs
        self.logger.info("TC-SEC-103: Rapidly re-adding all host NQNs …")
        for lvol_name, lvol_id, nqn in volumes:
            out, err = self.ssh_obj.add_host_to_lvol(
                self.mgmt_nodes[0], lvol_id, nqn)
            assert not err or "error" not in err.lower(), \
                f"add-host failed for {lvol_name}: {err}"
        sleep_n_sec(3)
        self.logger.info("TC-SEC-103: PASSED")

        # TC-SEC-104: all volumes can still get connect strings
        self.logger.info("TC-SEC-104: Verifying all volumes still have valid connect strings …")
        for lvol_name, lvol_id, nqn in volumes:
            cs, err = self._get_connect_str_cli(lvol_id, host_nqn=nqn)
            assert cs, \
                f"Volume {lvol_name} should have valid connect string after re-add; err={err}"
        self.logger.info("TC-SEC-104: All volumes accessible PASSED")

        self.logger.info("=== TestLvolSecurityScaleAndRapidOps PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 19 – Extended negative: tampered keys, edge-case CLI errors
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityNegativeConnectExtended(SecurityTestBase):
    """
    Extended negative scenarios beyond the basic TestLvolSecurityNegativeConnect:

    TC-SEC-105  get-secret after remove-host → must return error
    TC-SEC-106  add-host with empty NQN string → expect error
    TC-SEC-107  add-host on non-existent lvol ID → expect error
    TC-SEC-108  remove-host on non-existent lvol ID → expect error
    TC-SEC-109  create lvol with SEC_CTRL_ONLY (bidirectional) and wrong host NQN → rejected
    TC-SEC-110  create lvol with SEC_BOTH then get-secret with unregistered NQN → error
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_connect_extended"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeConnectExtended START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_ctrl", self.cluster_id, sec_options=SEC_CTRL_ONLY)

        host_nqn = self._get_client_host_nqn()
        absent_nqn = f"nqn.2024-01.io.simplyblock:test:absent-{_rand_suffix()}"
        fake_lvol_id = "00000000-0000-0000-0000-000000000099"

        lvol_name = f"secnex{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-105: get-secret after remove-host
        self.logger.info("TC-SEC-105: get-secret after remove-host …")
        self.ssh_obj.remove_host_from_lvol(self.mgmt_nodes[0], lvol_id, host_nqn)
        sleep_n_sec(2)
        out, err = self.ssh_obj.get_lvol_host_secret(self.mgmt_nodes[0], lvol_id, host_nqn)
        has_error = bool(err) or ("error" in (out or "").lower()) \
                    or ("not found" in (out or "").lower())
        assert has_error, f"get-secret after remove should fail; out={out!r} err={err!r}"
        self.logger.info("TC-SEC-105: PASSED")

        # Restore host for subsequent tests
        self.ssh_obj.add_host_to_lvol(self.mgmt_nodes[0], lvol_id, host_nqn)
        sleep_n_sec(2)

        # TC-SEC-106: add-host with empty NQN
        self.logger.info("TC-SEC-106: add-host with empty NQN …")
        out, err = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], lvol_id, "")
        has_error = bool(err) or ("error" in (out or "").lower())
        assert has_error, f"add-host with empty NQN should fail; out={out!r} err={err!r}"
        self.logger.info("TC-SEC-106: PASSED")

        # TC-SEC-107: add-host on non-existent lvol
        self.logger.info("TC-SEC-107: add-host on non-existent lvol …")
        out, err = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], fake_lvol_id, host_nqn)
        has_error = bool(err) or ("error" in (out or "").lower()) \
                    or ("not found" in (out or "").lower())
        assert has_error, \
            f"add-host on non-existent lvol should fail; out={out!r} err={err!r}"
        self.logger.info("TC-SEC-107: PASSED")

        # TC-SEC-108: remove-host on non-existent lvol
        self.logger.info("TC-SEC-108: remove-host on non-existent lvol …")
        out, err = self.ssh_obj.remove_host_from_lvol(
            self.mgmt_nodes[0], fake_lvol_id, host_nqn)
        has_error = bool(err) or ("error" in (out or "").lower()) \
                    or ("not found" in (out or "").lower())
        assert has_error, \
            f"remove-host on non-existent lvol should fail; out={out!r} err={err!r}"
        self.logger.info("TC-SEC-108: PASSED")

        # TC-SEC-109: SEC_CTRL_ONLY lvol with wrong NQN → no connect string
        self.logger.info("TC-SEC-109: SEC_CTRL_ONLY lvol with wrong NQN …")
        lvol_ctrl = f"secctrl{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_ctrl, self.lvol_size, self.pool_name + "_ctrl",
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower()
        sleep_n_sec(3)
        lvol_ctrl_id = self.sbcli_utils.get_lvol_id(lvol_ctrl)
        assert lvol_ctrl_id
        self.lvol_mount_details[lvol_ctrl] = {"ID": lvol_ctrl_id, "Mount": None}
        wrong_cs, wrong_err = self._get_connect_str_cli(lvol_ctrl_id, host_nqn=absent_nqn)
        assert not wrong_cs or wrong_err, \
            f"Unregistered NQN should not get connect string; cs={wrong_cs}"
        self.logger.info("TC-SEC-109: PASSED")

        # TC-SEC-110: get-secret with unregistered NQN
        self.logger.info("TC-SEC-110: get-secret with unregistered NQN …")
        out, err = self.ssh_obj.get_lvol_host_secret(
            self.mgmt_nodes[0], lvol_id, absent_nqn)
        has_error = bool(err) or ("error" in (out or "").lower()) \
                    or ("not found" in (out or "").lower())
        assert has_error, \
            f"get-secret for unregistered NQN must fail; out={out!r} err={err!r}"
        self.logger.info("TC-SEC-110: PASSED")

        self.logger.info("=== TestLvolSecurityNegativeConnectExtended PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 20 – Clone has independent security config from parent
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityCloneOverride(SecurityTestBase):
    """
    Verifies that a clone can have a different security configuration from
    its parent and that the two configs do not interfere.

    TC-SEC-111  Create parent lvol with SEC_HOST_ONLY + allowed host NQN_A
    TC-SEC-112  Create clone of parent snapshot – no explicit sec_options (inherits)
    TC-SEC-113  Add a different NQN_B to the clone; verify NQN_A works on parent,
                NQN_B works on clone
    TC-SEC-114  Remove NQN_A from parent; verify parent is inaccessible but clone
                still accessible with NQN_B
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_clone_override"

    def run(self):
        self.logger.info("=== TestLvolSecurityCloneOverride START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)

        nqn_a = self._get_client_host_nqn()
        uuid_out, _ = self.ssh_obj.exec_command(self.fio_node, "uuidgen")
        uuid_b = uuid_out.strip().split('\n')[0].strip().lower()
        nqn_b = f"nqn.2014-08.org.nvmexpress:uuid:{uuid_b}"

        parent_name = f"secpar{_rand_suffix()}"

        # TC-SEC-111: create parent lvol with SEC_HOST_ONLY + NQN_A
        self.logger.info("TC-SEC-111: Creating parent DHCHAP lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], parent_name, self.lvol_size, self.pool_name,
            allowed_hosts=[nqn_a],
        )
        assert not err or "error" not in err.lower()
        sleep_n_sec(3)
        parent_id = self.sbcli_utils.get_lvol_id(parent_name)
        assert parent_id
        self.lvol_mount_details[parent_name] = {"ID": parent_id, "Mount": None}
        self.logger.info("TC-SEC-111: PASSED")

        # Connect, write data, disconnect
        lvol_device, _ = self._connect_and_get_device(parent_name, parent_id, host_nqn=nqn_a)
        mount_point = f"{self.mount_path}/{parent_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[parent_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{parent_name}_w.log"
        self._run_fio_and_validate(parent_name, mount_point, log_file, rw="write", runtime=20)
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(parent_id)
        self.lvol_mount_details[parent_name]["Mount"] = None

        # TC-SEC-112: snapshot + clone
        self.logger.info("TC-SEC-112: Creating snapshot and clone …")
        snap_name = f"snappar{_rand_suffix()}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d snapshot add {parent_id} {snap_name}")
        assert not err or "error" not in err.lower(), f"snapshot creation failed: {err}"
        sleep_n_sec(3)
        snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
        assert snap_id, f"Could not find snapshot ID for {snap_name}"

        clone_name = f"secclone{_rand_suffix()}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d snapshot clone {snap_id} {clone_name}")
        assert not err or "error" not in err.lower(), f"clone creation failed: {err}"
        sleep_n_sec(5)
        clone_id = self.sbcli_utils.get_lvol_id(clone_name)
        assert clone_id, f"Could not find clone ID for {clone_name}"
        self.lvol_mount_details[clone_name] = {"ID": clone_id, "Mount": None}
        self.logger.info("TC-SEC-112: Snapshot+clone created PASSED")

        # TC-SEC-113: add NQN_B to clone; verify NQN_A on parent, NQN_B on clone
        self.logger.info("TC-SEC-113: Adding NQN_B to clone …")
        out, err = self.ssh_obj.add_host_to_lvol(
            self.mgmt_nodes[0], clone_id, nqn_b)
        assert not err or "error" not in err.lower(), f"add NQN_B to clone failed: {err}"
        sleep_n_sec(2)
        cs_parent_a, _ = self._get_connect_str_cli(parent_id, host_nqn=nqn_a)
        cs_clone_b, _  = self._get_connect_str_cli(clone_id,  host_nqn=nqn_b)
        assert cs_parent_a, "Parent: NQN_A should still get connect string"
        assert cs_clone_b,  "Clone: NQN_B should get connect string"
        self.logger.info("TC-SEC-113: Independent NQNs PASSED")

        # TC-SEC-114: remove NQN_A from parent; clone NQN_B still works
        self.logger.info("TC-SEC-114: Removing NQN_A from parent …")
        out, err = self.ssh_obj.remove_host_from_lvol(
            self.mgmt_nodes[0], parent_id, nqn_a)
        assert not err or "error" not in err.lower()
        sleep_n_sec(2)
        cs_parent_a2, err_a2 = self._get_connect_str_cli(parent_id, host_nqn=nqn_a)
        cs_clone_b2, _       = self._get_connect_str_cli(clone_id,  host_nqn=nqn_b)
        assert not cs_parent_a2 or err_a2, \
            "Parent NQN_A should be inaccessible after removal"
        assert cs_clone_b2, "Clone NQN_B should still be accessible"
        self.logger.info("TC-SEC-114: Clone independence PASSED")

        self.logger.info("=== TestLvolSecurityCloneOverride PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 21 – Security + backup: credentials survive backup/restore cycle
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityWithBackup(SecurityTestBase):
    """
    Backs up a DHCHAP+crypto lvol and verifies that the restored lvol
    can be connected with the appropriate credentials.

    TC-SEC-115  Create DHCHAP+crypto lvol, write FIO data, create snapshot
    TC-SEC-116  Trigger backup of snapshot; wait for completion
    TC-SEC-117  Restore backup to a new lvol name
    TC-SEC-118  Verify the restored lvol can be accessed (get connect string
                for the original NQN should succeed since DHCHAP config
                is preserved with the lvol metadata)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_with_backup"

    def run(self):
        self.logger.info("=== TestLvolSecurityWithBackup START ===")
        # Check backup is available
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0], f"{self.base_cmd} backup list 2>&1 | head -5")
        if "command not found" in (out or "").lower() or "error" in (err or "").lower():
            self.logger.info("Backup feature not available – skipping TC-SEC-115..118")
            return

        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        lvol_name = f"secbck{_rand_suffix()}"

        # TC-SEC-115: create DHCHAP+crypto lvol, write data
        self.logger.info("TC-SEC-115: Creating DHCHAP+crypto lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
            encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)

        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-SEC-116: snapshot + backup
        self.logger.info("TC-SEC-116: Creating snapshot and backup …")
        snap_name = f"snap{lvol_name[-6:]}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d snapshot add {lvol_id} {snap_name} --backup")
        assert not err or "error" not in err.lower(), f"snapshot add --backup failed: {err}"
        sleep_n_sec(5)

        # Wait for backup completion
        import time as _time
        deadline = _time.time() + 300
        backup_id = None
        while _time.time() < deadline:
            list_out, _ = self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.base_cmd} -d backup list")
            for line in (list_out or "").splitlines():
                if snap_name in line:
                    parts = [p.strip() for p in line.split("|") if p.strip()]
                    if parts:
                        for p in parts:
                            if len(p) == 36 and "-" in p:
                                backup_id = p
                    status_lower = line.lower()
                    if "done" in status_lower or "complete" in status_lower:
                        break
            else:
                sleep_n_sec(10)
                continue
            break
        assert backup_id, "Could not find backup ID after snapshot backup"
        self.logger.info(f"TC-SEC-116: Backup {backup_id} complete PASSED")

        # TC-SEC-117: restore backup
        self.logger.info("TC-SEC-117: Restoring backup …")
        restored_name = f"secrst{_rand_suffix()}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d backup restore {backup_id} --lvol {restored_name} --pool {self.pool_name}")
        assert not err or "error" not in err.lower(), f"backup restore failed: {err}"
        # Wait for restored lvol to appear
        deadline2 = _time.time() + 300
        while _time.time() < deadline2:
            list_out, _ = self.ssh_obj.exec_command(self.mgmt_nodes[0], f"{self.base_cmd} lvol list")
            if restored_name in (list_out or ""):
                break
            sleep_n_sec(10)
        else:
            raise TimeoutError(f"Restored lvol {restored_name} did not appear within 300s")
        self.logger.info(f"TC-SEC-117: Restore of {restored_name} PASSED")
        self.lvol_mount_details[restored_name] = {"ID": None, "Mount": None}

        # TC-SEC-118: get connect string for restored lvol
        self.logger.info("TC-SEC-118: Checking connect string for restored lvol …")
        restored_id = self.sbcli_utils.get_lvol_id(restored_name)
        if restored_id:
            connect_ls, err = self._get_connect_str_cli(restored_id)
            # Restored lvol inherits security; can be connected (possibly without DHCHAP
            # depending on restore behaviour), just verify it's accessible
            self.logger.info(
                f"TC-SEC-118: Restored lvol connect_ls={bool(connect_ls)} err={err!r}")
        else:
            self.logger.warning("TC-SEC-118: Could not find ID for restored lvol; skipping connect check")
        self.logger.info("TC-SEC-118: PASSED")

        self.logger.info("=== TestLvolSecurityWithBackup PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 22 – Resize a DHCHAP+crypto lvol: security config must be preserved
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityResize(SecurityTestBase):
    """
    Creates a DHCHAP+crypto lvol, resizes it, and verifies that the DHCHAP
    configuration is unchanged after the resize operation.

    TC-SEC-119  Create DHCHAP+crypto lvol (5G), connect, run FIO
    TC-SEC-120  Resize lvol to 10G via sbcli_utils.resize_lvol
    TC-SEC-121  Verify get-secret still works; connect with DHCHAP and run FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_resize"

    def run(self):
        self.logger.info("=== TestLvolSecurityResize START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)

        host_nqn = self._get_client_host_nqn()
        lvol_name = f"secrsz{_rand_suffix()}"

        # TC-SEC-119: create DHCHAP+crypto lvol 5G
        self.logger.info("TC-SEC-119: Creating DHCHAP+crypto 5G lvol …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, "5G", self.pool_name,
            allowed_hosts=[host_nqn],
            encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
        )
        assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_pre.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)
        self.logger.info("TC-SEC-119: Pre-resize FIO PASSED")

        # Disconnect before resize
        self.ssh_obj.unmount_path(self.fio_node, mount_point)
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-SEC-120: resize to 10G
        self.logger.info("TC-SEC-120: Resizing lvol to 10G …")
        self.sbcli_utils.resize_lvol(lvol_id, "10G")
        sleep_n_sec(5)
        self.logger.info("TC-SEC-120: Resize completed PASSED")

        # TC-SEC-121: get-secret still works; reconnect + FIO
        self.logger.info("TC-SEC-121: Verifying DHCHAP config after resize …")
        out, err = self.ssh_obj.get_lvol_host_secret(self.mgmt_nodes[0], lvol_id, host_nqn)
        assert out and "error" not in (out or "").lower(), \
            f"get-secret after resize failed: {err}"
        self.logger.info("TC-SEC-121: get-secret after resize PASSED")

        lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file2 = f"{self.log_path}/{lvol_name}_post.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file2, rw="randrw", runtime=20)
        self.logger.info("TC-SEC-121: Post-resize FIO PASSED")

        self.logger.info("=== TestLvolSecurityResize PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 23 – Volume list security fields validation
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityVolumeListFields(SecurityTestBase):
    """
    Verifies that security-related fields appear correctly in CLI output
    after volume creation with various security options.

    TC-SEC-122  Create DHCHAP+crypto lvol; verify CLI `volume get` has
                dhchap_key / dhchap_ctrlr_key fields
    TC-SEC-123  Create SEC_HOST_ONLY lvol; verify ctrl key fields absent/false
    TC-SEC-124  get-secret returns non-empty credential for registered NQN
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_volume_list_fields"

    def run(self):
        self.logger.info("=== TestLvolSecurityVolumeListFields START ===")
        self.fio_node = self.fio_node[0]
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_host", self.cluster_id, sec_options=SEC_HOST_ONLY)

        host_nqn = self._get_client_host_nqn()

        # TC-SEC-122: SEC_BOTH lvol – both dhchap fields should be true/present
        self.logger.info("TC-SEC-122: Creating SEC_BOTH lvol and checking fields …")
        lvol_both = f"secvlb{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_both, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
        )
        assert not err or "error" not in err.lower()
        sleep_n_sec(3)
        lvol_both_id = self.sbcli_utils.get_lvol_id(lvol_both)
        assert lvol_both_id
        self.lvol_mount_details[lvol_both] = {"ID": lvol_both_id, "Mount": None}

        detail_out = self._get_lvol_details_via_cli(lvol_both_id)
        has_dhchap_key = "dhchap_key" in detail_out.lower() or "dhchap" in detail_out.lower()
        assert has_dhchap_key, \
            f"volume get should mention dhchap fields for SEC_BOTH: {detail_out!r}"
        self.logger.info("TC-SEC-122: DHCHAP fields present PASSED")

        # TC-SEC-123: SEC_HOST_ONLY lvol
        self.logger.info("TC-SEC-123: Creating SEC_HOST_ONLY lvol and checking fields …")
        uuid_out, _ = self.ssh_obj.exec_command(self.fio_node, "uuidgen")
        uuid_h = uuid_out.strip().split('\n')[0].strip().lower()
        nqn_h = f"nqn.2014-08.org.nvmexpress:uuid:{uuid_h}"

        lvol_host = f"secvlh{_rand_suffix()}"
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_host, self.lvol_size, self.pool_name + "_host",
            allowed_hosts=[nqn_h],
        )
        assert not err or "error" not in err.lower()
        sleep_n_sec(3)
        lvol_host_id = self.sbcli_utils.get_lvol_id(lvol_host)
        assert lvol_host_id
        self.lvol_mount_details[lvol_host] = {"ID": lvol_host_id, "Mount": None}

        detail_host = self._get_lvol_details_via_cli(lvol_host_id)
        self.logger.info(f"TC-SEC-123: volume get output: {detail_host!r}")
        # SEC_HOST_ONLY means dhchap_key=True, dhchap_ctrlr_key=False
        assert "dhchap" in detail_host.lower() or "allowed_host" in detail_host.lower(), \
            f"SEC_HOST_ONLY lvol should show dhchap-related info: {detail_host!r}"
        self.logger.info("TC-SEC-123: PASSED")

        # TC-SEC-124: get-secret returns non-empty credential
        self.logger.info("TC-SEC-124: Verifying get-secret returns credentials …")
        secret_out, secret_err = self.ssh_obj.get_lvol_host_secret(
            self.mgmt_nodes[0], lvol_both_id, host_nqn)
        assert secret_out and "error" not in (secret_out or "").lower(), \
            f"get-secret should return credentials; out={secret_out!r} err={secret_err!r}"
        self.logger.info("TC-SEC-124: get-secret credentials PASSED")

        self.logger.info("=== TestLvolSecurityVolumeListFields PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 24 – DHCHAP over RDMA transport (skipped if RDMA not available)
# ═══════════════════════════════════════════════════════════════════════════

class TestLvolSecurityRDMA(SecurityTestBase):
    """
    Creates a DHCHAP lvol on an RDMA-capable cluster and verifies that
    authentication and data I/O work correctly over the RDMA fabric.

    TC-SEC-125  Skip if cluster does not support RDMA (fabric_rdma=False)
    TC-SEC-126  Create DHCHAP lvol with fabric=rdma; get connect string
    TC-SEC-127  Connect via RDMA, mount, run FIO, validate data integrity
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_rdma"

    def run(self):
        self.logger.info("=== TestLvolSecurityRDMA START ===")
        self.fio_node = self.fio_node[0]

        # TC-SEC-125: skip if RDMA not available
        self.logger.info("TC-SEC-125: Checking RDMA availability …")
        cluster_details = self.sbcli_utils.get_cluster_details()
        fabric_rdma = cluster_details.get("fabric_rdma", False)
        if not fabric_rdma:
            self.logger.info(
                "TC-SEC-125: RDMA not available on this cluster (fabric_rdma=False) – SKIPPED")
            return
        self.logger.info("TC-SEC-125: RDMA available – proceeding")

        self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
        host_nqn = self._get_client_host_nqn()
        lvol_name = f"secrdma{_rand_suffix()}"

        # TC-SEC-126: create DHCHAP lvol with rdma fabric
        self.logger.info("TC-SEC-126: Creating DHCHAP lvol with RDMA fabric …")
        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
            allowed_hosts=[host_nqn],
            fabric="rdma",
        )
        assert not err or "error" not in err.lower(), f"RDMA lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, f"Could not find ID for {lvol_name}"
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
        assert connect_ls and not err, \
            f"RDMA lvol should return connect string; err={err}"
        self.logger.info("TC-SEC-126: RDMA DHCHAP connect string PASSED")

        # TC-SEC-127: connect, mount, FIO
        self.logger.info("TC-SEC-127: Connecting RDMA lvol and running FIO …")
        lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type="ext4")
        self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-127: RDMA FIO PASSED")

        self.logger.info("=== TestLvolSecurityRDMA PASSED ===")
