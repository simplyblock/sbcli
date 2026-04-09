"""
Continuous failover stress tests with security combinations.

Creates a mix of lvols covering all four core security types:
  plain        – no encryption, no authentication
  crypto       – AES encryption only
  auth         – bidirectional DH-HMAC-CHAP (no encryption)
  crypto_auth  – AES encryption + bidirectional DH-HMAC-CHAP

And two additional "allowed-hosts" variants:
  auth_allowed        – DHCHAP + NQN whitelist, no encryption
  crypto_auth_allowed – AES encryption + DHCHAP + NQN whitelist

The test runs the same continuous random-failover loop as
RandomFailoverTest (graceful shutdown, container stop, partial/full
network interrupt) so we can verify that authenticated volumes survive
every outage type with full data integrity.

All sbcli CLI wrappers are delegated to ssh_obj:
  ssh_obj.create_sec_lvol(...)
  ssh_obj.get_lvol_connect_str_with_host_nqn(...)
  ssh_obj.get_client_host_nqn(node)
"""

from __future__ import annotations

import itertools
import random
import string
import threading

from exceptions.custom_exception import LvolNotConnectException
from logger_config import setup_logger
from stress_test.continuous_failover_ha import (
    RandomFailoverTest,
    generate_random_sequence,
)
from stress_test.continuous_failover_ha_rdma_multi_outage import (
    RandomRDMAMultiFailoverTest,
)
from utils.common_utils import sleep_n_sec

_NDCS_NPCS_CHOICES = [(1, 1), (1, 2), (2, 1)]


# ─── helpers ─────────────────────────────────────────────────────────────────

SEC_BOTH = {"dhchap_key": True, "dhchap_ctrlr_key": True}


def _rand_suffix(n: int = 8) -> str:
    letters = string.ascii_uppercase
    all_chars = letters + string.digits
    return random.choice(letters) + "".join(random.choices(all_chars, k=n - 1))


# Security type constants
_SEC_TYPES_CORE = ["plain", "crypto", "auth", "crypto_auth"]

# Full set including allowed-hosts variants
_SEC_TYPES_ALL = [
    "plain", "crypto", "auth", "crypto_auth",
    "auth_allowed", "crypto_auth_allowed",
]


# ═══════════════════════════════════════════════════════════════════════════
#  Core stress test — 4 security types
# ═══════════════════════════════════════════════════════════════════════════


class RandomSecurityFailoverTest(RandomFailoverTest):
    """
    Continuous random failover stress test with all four core security types.

    Each new lvol is assigned one of:
      plain        – created via API, no auth
      crypto       – created via API, AES encryption
      auth         – created via CLI, DHCHAP both directions
      crypto_auth  – created via CLI, AES + DHCHAP

    All other test mechanics (FIO workload, outage types, migration
    validation, snapshot / clone creation) are inherited from
    RandomFailoverTest unchanged.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "continuous_random_security_failover_ha"
        self.total_lvols = 20
        self.lvol_size = "10G"
        self.fio_size = "1G"
        self._sec_cycle = itertools.cycle(_SEC_TYPES_CORE)
        self._cached_host_nqn: str | None = None
        self.available_fabrics = ["tcp"]  # overwritten in run() from cluster info

    # ── host NQN (cached) ─────────────────────────────────────────────────────

    def _get_client_host_nqn(self) -> str:
        if self._cached_host_nqn:
            return self._cached_host_nqn
        nqn = self.ssh_obj.get_client_host_nqn(self.fio_node)
        self.logger.info(f"Client host NQN: {nqn}")
        self._cached_host_nqn = nqn
        return nqn

    # ── override lvol creation ────────────────────────────────────────────────

    def create_lvols_with_fio(self, count: int) -> None:
        """Create *count* lvols cycling through security types."""
        for i in range(count):
            sec_type = next(self._sec_cycle)
            self._create_one_lvol_with_fio(i, sec_type)

    def _create_one_lvol_with_fio(self, index: int, sec_type: str) -> None:
        """Create a single lvol of *sec_type* and start FIO on it."""
        fs_type = random.choice(["ext4", "xfs"])
        ndcs, npcs = random.choice(_NDCS_NPCS_CHOICES)
        fabric = random.choice(self.available_fabrics)
        prefix = {"plain": "pl", "crypto": "cr",
                   "auth": "au", "crypto_auth": "ca"}.get(sec_type, "xx")
        lvol_name = f"{prefix}{self.lvol_name}_{index}"

        # Ensure unique name
        attempt = 0
        while lvol_name in self.lvol_mount_details:
            self.lvol_name = f"lvl{generate_random_sequence(15)}"
            lvol_name = f"{prefix}{self.lvol_name}_{index}"
            attempt += 1
            if attempt > 20:
                break

        encrypt = sec_type in ("crypto", "crypto_auth")
        has_auth = sec_type in ("auth", "crypto_auth")

        self.logger.info(
            f"Creating lvol {lvol_name!r} "
            f"(sec_type={sec_type}, encrypt={encrypt}, auth={has_auth}, "
            f"ndcs={ndcs}, npcs={npcs}, fabric={fabric})")

        try:
            if has_auth:
                _, err = self.ssh_obj.create_sec_lvol(
                    self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
                    sec_options=SEC_BOTH, encrypt=encrypt,
                    key1=self.lvol_crypt_keys[0] if encrypt else None,
                    key2=self.lvol_crypt_keys[1] if encrypt else None,
                    distr_ndcs=ndcs, distr_npcs=npcs, fabric=fabric,
                )
                if err and "error" in err.lower():
                    self.logger.warning(
                        f"CLI lvol creation error for {lvol_name}: {err}")
                    return
            else:
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name,
                    pool_name=self.pool_name,
                    size=self.lvol_size,
                    crypto=encrypt,
                    key1=self.lvol_crypt_keys[0] if encrypt else None,
                    key2=self.lvol_crypt_keys[1] if encrypt else None,
                    distr_ndcs=ndcs,
                    distr_npcs=npcs,
                    fabric=fabric,
                )
        except Exception as exc:
            self.logger.warning(
                f"lvol creation failed for {lvol_name}: {exc}. Retrying …")
            self.lvol_name = f"lvl{generate_random_sequence(15)}"
            lvol_name = f"{prefix}{self.lvol_name}_{index}"
            try:
                if has_auth:
                    self.ssh_obj.create_sec_lvol(
                        self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
                        sec_options=SEC_BOTH, encrypt=encrypt,
                        key1=self.lvol_crypt_keys[0] if encrypt else None,
                        key2=self.lvol_crypt_keys[1] if encrypt else None,
                        distr_ndcs=ndcs, distr_npcs=npcs, fabric=fabric,
                    )
                else:
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name,
                        pool_name=self.pool_name,
                        size=self.lvol_size,
                        crypto=encrypt,
                        key1=self.lvol_crypt_keys[0] if encrypt else None,
                        key2=self.lvol_crypt_keys[1] if encrypt else None,
                        distr_ndcs=ndcs,
                        distr_npcs=npcs,
                        fabric=fabric,
                    )
            except Exception as exc2:
                self.logger.warning(
                    f"Retry lvol creation also failed: {exc2}")
                return

        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        if not lvol_id:
            self.logger.warning(f"Could not find lvol ID for {lvol_name}, skipping")
            return

        # Track node placement
        try:
            lvol_node_id = self.sbcli_utils.get_lvol_details(
                lvol_id=lvol_id)[0]["node_id"]
        except Exception:
            lvol_node_id = None
        if lvol_node_id:
            self.node_vs_lvol.setdefault(lvol_node_id, []).append(lvol_name)

        # Determine host_nqn for auth lvols
        host_nqn = self._get_client_host_nqn() if has_auth else None

        # Get connect string
        try:
            if host_nqn:
                connect_ls, err = self.ssh_obj.get_lvol_connect_str_with_host_nqn(
                    self.mgmt_nodes[0], lvol_id, host_nqn)
                if err or not connect_ls:
                    self.logger.warning(
                        f"No connect string for auth lvol {lvol_name}: {err}")
                    self.sbcli_utils.delete_lvol(lvol_name=lvol_name,
                                                 skip_error=True)
                    return
            else:
                connect_ls = self.sbcli_utils.get_lvol_connect_str(
                    lvol_name=lvol_name)
        except Exception as exc:
            self.logger.warning(f"get_connect_str failed for {lvol_name}: {exc}")
            return

        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=f"{self.base_cmd} lvol list")

        # Register in tracking dict before connecting
        log_file = f"{self.log_path}/{lvol_name}.log"
        self.lvol_mount_details[lvol_name] = {
            "ID":       lvol_id,
            "Command":  connect_ls,
            "Mount":    None,
            "Device":   None,
            "MD5":      None,
            "FS":       fs_type,
            "Log":      log_file,
            "snapshots": [],
            "sec_type": sec_type,
            "host_nqn": host_nqn,
        }

        # Connect NVMe on fio_node
        initial_devices = self.ssh_obj.get_devices(node=self.fio_node)
        for cmd in connect_ls:
            _, err = self.ssh_obj.exec_command(node=self.fio_node, command=cmd)
            if err:
                self.logger.warning(
                    f"nvme connect error for {lvol_name}: {err}")
                try:
                    details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
                    nqn = details[0]["nqn"]
                    self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=nqn)
                except Exception:
                    pass
                self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=True)
                del self.lvol_mount_details[lvol_name]
                if lvol_node_id and lvol_name in self.node_vs_lvol.get(
                        lvol_node_id, []):
                    self.node_vs_lvol[lvol_node_id].remove(lvol_name)
                return

        sleep_n_sec(3)
        final_devices = self.ssh_obj.get_devices(node=self.fio_node)
        lvol_device = None
        for dev in final_devices:
            if dev not in initial_devices:
                lvol_device = f"/dev/{dev.strip()}"
                break

        if not lvol_device:
            raise LvolNotConnectException(
                f"LVOL {lvol_name} (sec={sec_type}) did not connect")

        self.lvol_mount_details[lvol_name]["Device"] = lvol_device
        self.ssh_obj.format_disk(node=self.fio_node,
                                 device=lvol_device, fs_type=fs_type)

        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.mount_path(node=self.fio_node,
                                device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        sleep_n_sec(10)
        self.ssh_obj.delete_files(self.fio_node, [f"{mount_point}/*fio*"])
        self.ssh_obj.delete_files(
            self.fio_node, [f"{self.log_path}/local-{lvol_name}_fio*"])
        sleep_n_sec(5)

        # Start FIO
        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(self.fio_node, None, mount_point, log_file),
            kwargs={
                "size":       self.fio_size,
                "name":       f"{lvol_name}_fio",
                "rw":         "randrw",
                "bs":         f"{2 ** random.randint(2, 7)}K",
                "nrfiles":    16,
                "iodepth":    1,
                "numjobs":    5,
                "time_based": True,
                "runtime":    2000,
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)
        sleep_n_sec(10)

    # ── reconnect helper (used after outage recovery if needed) ──────────────

    def _get_reconnect_commands(self, lvol_name: str) -> list[str]:
        """
        Return up-to-date connect commands for *lvol_name*.
        For auth lvols the commands contain fresh DHCHAP keys obtained via CLI,
        including ``--ctrl-loss-tmo -1`` for outage resilience.
        """
        details = self.lvol_mount_details.get(lvol_name)
        if not details:
            return []
        lvol_id = details["ID"]
        host_nqn = details.get("host_nqn")
        if host_nqn:
            connect_ls, err = self.ssh_obj.get_lvol_connect_str_with_host_nqn(
                self.mgmt_nodes[0], lvol_id, host_nqn)
            if err or not connect_ls:
                self.logger.warning(
                    f"Could not get auth connect str for {lvol_name}: {err}")
                return details.get("Command") or []
            return connect_ls
        return self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name) or []

    def run(self):
        """Read cluster fabric config, then delegate to parent run()."""
        self.logger.info("Reading cluster config for security failover test.")
        cluster_details = self.sbcli_utils.get_cluster_details()
        fabric_rdma = cluster_details.get("fabric_rdma", False)
        fabric_tcp = cluster_details.get("fabric_tcp", True)
        if fabric_rdma and fabric_tcp:
            self.available_fabrics = ["tcp", "rdma"]
        elif fabric_rdma:
            self.available_fabrics = ["rdma"]
        else:
            self.available_fabrics = ["tcp"]
        self.logger.info(f"Available fabrics: {self.available_fabrics}")
        super().run()


# ═══════════════════════════════════════════════════════════════════════════
#  Extended stress test — all 6 security types (includes allowed-hosts)
# ═══════════════════════════════════════════════════════════════════════════

class RandomAllSecurityFailoverTest(RandomSecurityFailoverTest):
    """
    Extends RandomSecurityFailoverTest to also include the two
    allowed-hosts variants:
      auth_allowed        – DHCHAP + NQN whitelist, no encryption
      crypto_auth_allowed – AES encryption + DHCHAP + NQN whitelist

    The client machine's NQN is registered as the sole allowed host so
    that FIO I/O can proceed normally.  The test additionally validates
    that requesting a connect string for an *unregistered* NQN is rejected.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "continuous_random_all_security_failover_ha"
        self._sec_cycle = itertools.cycle(_SEC_TYPES_ALL)

    def _create_one_lvol_with_fio(self, index: int, sec_type: str) -> None:
        """Handle the two extra allowed-hosts types; delegate the rest."""
        if sec_type not in ("auth_allowed", "crypto_auth_allowed"):
            super()._create_one_lvol_with_fio(index, sec_type)
            return

        # ── allowed-hosts variant ─────────────────────────────────────────
        fs_type = random.choice(["ext4", "xfs"])
        ndcs, npcs = random.choice(_NDCS_NPCS_CHOICES)
        fabric = random.choice(self.available_fabrics)
        encrypt = (sec_type == "crypto_auth_allowed")
        prefix = "ca2" if encrypt else "aa"
        lvol_name = f"{prefix}{self.lvol_name}_{index}"

        attempt = 0
        while lvol_name in self.lvol_mount_details:
            self.lvol_name = f"lvl{generate_random_sequence(15)}"
            lvol_name = f"{prefix}{self.lvol_name}_{index}"
            attempt += 1
            if attempt > 20:
                break

        host_nqn = self._get_client_host_nqn()

        self.logger.info(
            f"Creating {sec_type} lvol {lvol_name!r} "
            f"(allowed_hosts=[{host_nqn}], encrypt={encrypt}, "
            f"ndcs={ndcs}, npcs={npcs}, fabric={fabric})")

        try:
            _, err = self.ssh_obj.create_sec_lvol(
                self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
                sec_options=SEC_BOTH, allowed_hosts=[host_nqn], encrypt=encrypt,
                key1=self.lvol_crypt_keys[0] if encrypt else None,
                key2=self.lvol_crypt_keys[1] if encrypt else None,
                distr_ndcs=ndcs, distr_npcs=npcs, fabric=fabric,
            )
            if err and "error" in err.lower():
                self.logger.warning(
                    f"CLI error creating {lvol_name}: {err}")
                return
        except Exception as exc:
            self.logger.warning(f"lvol creation failed: {exc}")
            return

        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        if not lvol_id:
            self.logger.warning(f"Could not find ID for {lvol_name}")
            return

        try:
            lvol_node_id = self.sbcli_utils.get_lvol_details(
                lvol_id=lvol_id)[0]["node_id"]
        except Exception:
            lvol_node_id = None
        if lvol_node_id:
            self.node_vs_lvol.setdefault(lvol_node_id, []).append(lvol_name)

        # Connect string requires host_nqn (with --ctrl-loss-tmo -1)
        connect_ls, err = self.ssh_obj.get_lvol_connect_str_with_host_nqn(
            self.mgmt_nodes[0], lvol_id, host_nqn)
        if err or not connect_ls:
            self.logger.warning(
                f"No connect string for {lvol_name}: {err}")
            self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=True)
            return

        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=f"{self.base_cmd} lvol list")

        log_file = f"{self.log_path}/{lvol_name}.log"
        self.lvol_mount_details[lvol_name] = {
            "ID":       lvol_id,
            "Command":  connect_ls,
            "Mount":    None,
            "Device":   None,
            "MD5":      None,
            "FS":       fs_type,
            "Log":      log_file,
            "snapshots": [],
            "sec_type": sec_type,
            "host_nqn": host_nqn,
        }

        # Connect NVMe
        initial_devices = self.ssh_obj.get_devices(node=self.fio_node)
        for cmd in connect_ls:
            _, err = self.ssh_obj.exec_command(node=self.fio_node, command=cmd)
            if err:
                self.logger.warning(
                    f"nvme connect error for {lvol_name}: {err}")
                try:
                    dets = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
                    self.ssh_obj.disconnect_nvme(
                        node=self.fio_node, nqn_grep=dets[0]["nqn"])
                except Exception:
                    pass
                self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=True)
                del self.lvol_mount_details[lvol_name]
                if lvol_node_id:
                    self.node_vs_lvol.get(lvol_node_id, []).remove(lvol_name) \
                        if lvol_name in self.node_vs_lvol.get(lvol_node_id, []) \
                        else None
                return

        sleep_n_sec(3)
        final_devices = self.ssh_obj.get_devices(node=self.fio_node)
        lvol_device = None
        for dev in final_devices:
            if dev not in initial_devices:
                lvol_device = f"/dev/{dev.strip()}"
                break
        if not lvol_device:
            raise LvolNotConnectException(
                f"LVOL {lvol_name} ({sec_type}) did not connect")

        self.lvol_mount_details[lvol_name]["Device"] = lvol_device
        self.ssh_obj.format_disk(node=self.fio_node,
                                 device=lvol_device, fs_type=fs_type)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.mount_path(node=self.fio_node,
                                device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        sleep_n_sec(10)
        self.ssh_obj.delete_files(self.fio_node, [f"{mount_point}/*fio*"])
        self.ssh_obj.delete_files(
            self.fio_node, [f"{self.log_path}/local-{lvol_name}_fio*"])
        sleep_n_sec(5)

        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(self.fio_node, None, mount_point, log_file),
            kwargs={
                "size":       self.fio_size,
                "name":       f"{lvol_name}_fio",
                "rw":         "randrw",
                "bs":         f"{2 ** random.randint(2, 7)}K",
                "nrfiles":    16,
                "iodepth":    1,
                "numjobs":    5,
                "time_based": True,
                "runtime":    2000,
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)
        sleep_n_sec(10)

        # ── Negative check: wrong NQN should be rejected ──────────────────
        wrong_nqn = f"nqn.2024-01.io.simplyblock:stress:wrong-{_rand_suffix()}"
        wrong_connect, wrong_err = self.ssh_obj.get_lvol_connect_str_with_host_nqn(
            self.mgmt_nodes[0], lvol_id, wrong_nqn)
        if not wrong_err and wrong_connect:
            self.logger.warning(
                f"WARNING: wrong NQN {wrong_nqn!r} was NOT rejected for "
                f"lvol {lvol_name!r}. connect_ls={wrong_connect}")
        else:
            self.logger.info(
                f"Correct: wrong NQN rejected for allowed-hosts lvol "
                f"{lvol_name!r} (err={wrong_err!r})")


# ═══════════════════════════════════════════════════════════════════════════
#  Combined: all 6 security types + N+K simultaneous outages + TCP/RDMA
# ═══════════════════════════════════════════════════════════════════════════

class RandomAllSecurityMultiFailoverTest(RandomRDMAMultiFailoverTest):
    """
    Combines all 6 security types with N+K simultaneous outages and
    auto-detected fabric (TCP, RDMA, or both).

    Security types cycled per lvol:
      plain, crypto, auth, crypto_auth, auth_allowed, crypto_auth_allowed

    Outage count (K) and available fabrics are derived from cluster config
    in run() (inherited from RandomRDMAMultiFailoverTest).

    fio_node is a list (multi-client), so each lvol is assigned a random
    client node. lvol_mount_details includes "Client" and "iolog_base_path"
    keys to match the multi-outage parent's expectations.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "n_plus_k_all_security_failover_ha"
        self._sec_cycle = itertools.cycle(_SEC_TYPES_ALL)
        self._cached_host_nqn: str | None = None

    # ── host NQN (cached) ────────────────────────────────────────────────

    def _get_client_host_nqn(self) -> str:
        if self._cached_host_nqn:
            return self._cached_host_nqn
        # fio_node is a list in multi-client context
        node = self.fio_node[0] if isinstance(self.fio_node, list) else self.fio_node
        nqn = self.ssh_obj.get_client_host_nqn(node)
        self.logger.info(f"Client host NQN: {nqn}")
        self._cached_host_nqn = nqn
        return nqn

    # ── lvol creation ────────────────────────────────────────────────────

    def create_lvols_with_fio(self, count):
        """Create *count* lvols cycling through all 6 security types."""
        for i in range(count):
            sec_type = next(self._sec_cycle)
            self._create_one_sec_lvol_multi(i, sec_type)

    def _create_one_sec_lvol_multi(self, index: int, sec_type: str) -> None:
        """Create a single security lvol and start FIO (multi-client aware)."""
        fs_type = random.choice(["ext4", "xfs"])
        ndcs, npcs = random.choice(_NDCS_NPCS_CHOICES)
        fabric = random.choice(self.available_fabrics)
        client_node = random.choice(self.fio_node) if isinstance(self.fio_node, list) else self.fio_node

        encrypt = sec_type in ("crypto", "crypto_auth", "crypto_auth_allowed")
        has_auth = sec_type in ("auth", "crypto_auth", "auth_allowed", "crypto_auth_allowed")
        is_allowed = sec_type in ("auth_allowed", "crypto_auth_allowed")

        prefix_map = {
            "plain": "pl", "crypto": "cr", "auth": "au", "crypto_auth": "ca",
            "auth_allowed": "aa", "crypto_auth_allowed": "ca2",
        }
        prefix = prefix_map.get(sec_type, "xx")
        lvol_name = f"{prefix}{self.lvol_name}_{index}"

        attempt = 0
        while lvol_name in self.lvol_mount_details:
            self.lvol_name = f"lvl{generate_random_sequence(15)}"
            lvol_name = f"{prefix}{self.lvol_name}_{index}"
            attempt += 1
            if attempt > 20:
                break

        # Build skip_nodes for mid-outage creation
        host_id = None
        if self.current_outage_nodes:
            skip_nodes = [
                n for n in self.sn_primary_secondary_map
                if self.sn_primary_secondary_map[n] in self.current_outage_nodes
            ]
            for n in self.current_outage_nodes:
                skip_nodes.append(n)
            candidates = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
            host_id = candidates[0] if candidates else None
        elif self.current_outage_node:
            skip_nodes = [
                n for n in self.sn_primary_secondary_map
                if self.sn_primary_secondary_map[n] == self.current_outage_node
            ]
            skip_nodes.append(self.current_outage_node)
            skip_nodes.append(self.sn_primary_secondary_map.get(self.current_outage_node))
            candidates = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
            host_id = candidates[0] if candidates else None

        host_nqn = self._get_client_host_nqn() if has_auth else None

        self.logger.info(
            f"Creating {sec_type} lvol {lvol_name!r} "
            f"(encrypt={encrypt}, auth={has_auth}, allowed={is_allowed}, "
            f"ndcs={ndcs}, npcs={npcs}, fabric={fabric}, client={client_node})")

        try:
            if has_auth:
                _, err = self.ssh_obj.create_sec_lvol(
                    self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
                    sec_options=SEC_BOTH,
                    allowed_hosts=[host_nqn] if is_allowed else None,
                    encrypt=encrypt,
                    key1=self.lvol_crypt_keys[0] if encrypt else None,
                    key2=self.lvol_crypt_keys[1] if encrypt else None,
                    distr_ndcs=ndcs, distr_npcs=npcs, fabric=fabric,
                )
                if err and "error" in err.lower():
                    self.logger.warning(f"CLI lvol creation error for {lvol_name}: {err}")
                    return
            else:
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name, pool_name=self.pool_name, size=self.lvol_size,
                    crypto=encrypt,
                    key1=self.lvol_crypt_keys[0] if encrypt else None,
                    key2=self.lvol_crypt_keys[1] if encrypt else None,
                    host_id=host_id, distr_ndcs=ndcs, distr_npcs=npcs, fabric=fabric,
                )
        except Exception as exc:
            self.logger.warning(f"lvol creation failed for {lvol_name}: {exc}. Retrying …")
            self.lvol_name = f"lvl{generate_random_sequence(15)}"
            lvol_name = f"{prefix}{self.lvol_name}_{index}"
            try:
                if has_auth:
                    self.ssh_obj.create_sec_lvol(
                        self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
                        sec_options=SEC_BOTH,
                        allowed_hosts=[host_nqn] if is_allowed else None,
                        encrypt=encrypt,
                        key1=self.lvol_crypt_keys[0] if encrypt else None,
                        key2=self.lvol_crypt_keys[1] if encrypt else None,
                        distr_ndcs=ndcs, distr_npcs=npcs, fabric=fabric,
                    )
                else:
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name, pool_name=self.pool_name, size=self.lvol_size,
                        crypto=encrypt,
                        key1=self.lvol_crypt_keys[0] if encrypt else None,
                        key2=self.lvol_crypt_keys[1] if encrypt else None,
                        host_id=host_id, distr_ndcs=ndcs, distr_npcs=npcs, fabric=fabric,
                    )
            except Exception as exc2:
                self.logger.warning(f"Retry lvol creation also failed: {exc2}")
                return

        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        if not lvol_id:
            self.logger.warning(f"Could not find lvol ID for {lvol_name}, skipping")
            return

        try:
            lvol_node_id = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]["node_id"]
        except Exception:
            lvol_node_id = None
        if lvol_node_id:
            self.node_vs_lvol.setdefault(lvol_node_id, []).append(lvol_name)

        # Connect string
        try:
            if host_nqn:
                connect_ls, err = self.ssh_obj.get_lvol_connect_str_with_host_nqn(
                    self.mgmt_nodes[0], lvol_id, host_nqn)
                if err or not connect_ls:
                    self.logger.warning(f"No connect string for auth lvol {lvol_name}: {err}")
                    self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=True)
                    return
            else:
                connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        except Exception as exc:
            self.logger.warning(f"get_connect_str failed for {lvol_name}: {exc}")
            return

        self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=f"{self.base_cmd} lvol list")

        log_file = f"{self.log_path}/{lvol_name}.log"
        iolog_base = f"{self.log_path}/{lvol_name}_fio_iolog"
        self.lvol_mount_details[lvol_name] = {
            "ID": lvol_id, "Command": connect_ls, "Mount": None, "Device": None,
            "MD5": None, "FS": fs_type, "Log": log_file, "snapshots": [],
            "sec_type": sec_type, "host_nqn": host_nqn,
            "Client": client_node, "iolog_base_path": iolog_base,
        }

        # NVMe connect
        initial_devices = self.ssh_obj.get_devices(node=client_node)
        for cmd in connect_ls:
            _, err = self.ssh_obj.exec_command(node=client_node, command=cmd)
            if err:
                self.logger.warning(f"nvme connect error for {lvol_name}: {err}")
                try:
                    nqn = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]["nqn"]
                    self.ssh_obj.disconnect_nvme(node=client_node, nqn_grep=nqn)
                except Exception:
                    pass
                self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=True)
                del self.lvol_mount_details[lvol_name]
                if lvol_node_id and lvol_name in self.node_vs_lvol.get(lvol_node_id, []):
                    self.node_vs_lvol[lvol_node_id].remove(lvol_name)
                return

        sleep_n_sec(3)
        final_devices = self.ssh_obj.get_devices(node=client_node)
        lvol_device = next(
            (f"/dev/{d.strip()}" for d in final_devices if d not in initial_devices), None)
        if not lvol_device:
            raise LvolNotConnectException(f"LVOL {lvol_name} ({sec_type}) did not connect")

        self.lvol_mount_details[lvol_name]["Device"] = lvol_device
        self.ssh_obj.format_disk(node=client_node, device=lvol_device, fs_type=fs_type)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.mount_path(node=client_node, device=lvol_device, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        sleep_n_sec(10)
        self.ssh_obj.delete_files(client_node, [f"{mount_point}/*fio*"])
        self.ssh_obj.delete_files(client_node, [f"{self.log_path}/local-{lvol_name}_fio*"])
        self.ssh_obj.delete_files(client_node, [f"{iolog_base}*"])
        sleep_n_sec(5)

        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(client_node, None, mount_point, log_file),
            kwargs={
                "size": self.fio_size, "name": f"{lvol_name}_fio", "rw": "randrw",
                "bs": f"{2 ** random.randint(2, 7)}K", "nrfiles": 16,
                "iodepth": 1, "numjobs": 5, "time_based": True, "runtime": 2000,
                "log_avg_msec": 1000, "iolog_file": iolog_base,
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)
        sleep_n_sec(10)

        # Negative check for allowed-hosts lvols
        if is_allowed:
            wrong_nqn = f"nqn.2024-01.io.simplyblock:stress:wrong-{_rand_suffix()}"
            wrong_connect, wrong_err = self.ssh_obj.get_lvol_connect_str_with_host_nqn(
                self.mgmt_nodes[0], lvol_id, wrong_nqn)
            if not wrong_err and wrong_connect:
                self.logger.warning(
                    f"WARNING: wrong NQN {wrong_nqn!r} was NOT rejected for {lvol_name!r}")
            else:
                self.logger.info(
                    f"Correct: wrong NQN rejected for {lvol_name!r} (err={wrong_err!r})")
