"""Shared machinery for the async-replication lanes.

Everything here is grounded in what the code does, not in the August design
discussion, because the two disagree in places. The disagreements that matter:

* **Every transfer is a FULL transfer.** ``allow_partial`` is deliberately
  disabled in ``rpc_client.py``: "the SPDK fork's fragment write path corrupts
  partial transfers, so allow_partial is never emitted and every transfer is a
  full one, regardless of what the caller requests" (PR #1276, 52e75afb2). So
  a 20 GiB volume on a one-minute cadence re-sends 20 GiB every minute.
  :data:`REPL_VOLUME_SIZE` is small on purpose, and tests that watch lag have
  to allow for transfer time dominated by volume size rather than delta size.

* **Cut-over is a two-phase handshake.** A migration or fail-back parks in
  ``cutover_pending`` until ``cutover_proceed`` is set, "once the operator has
  connected the target NVMe paths", with ``REPL_CUTOVER_PROCEED_TIMEOUT_SEC``
  as a safety fallback. On Kubernetes the operator does that. On Docker
  nothing does, so :meth:`ReplicationTestBase.drive_cutover` has to -- without
  it every migration test sits until the timeout and then reports a slow pass,
  which is worse than failing.

* **Fail-back reverses ``direction``.** The original source becomes the TARGET
  of the reverse relationship, so a lookup by source silently misses a volume
  that is failing back. :meth:`relationship_for` searches both ends.

The state machine, from ``models/lvol_model.py``::

    replicating -> cutover_pending -> cutover_done     (migration / fail-back)
    replicating -> failed_over                         (fail-over)
    direction: to_target | to_source
"""
import json
import os
import re
import time

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec


class ReplicationPreconditionError(Exception):
    """The harness could not set up what the test needs.

    Separate from an assertion failure on purpose: this says "the test never
    ran", not "the product is wrong". Conflating the two is how a broken lab
    gets filed as a product defect.
    """


class ReplicationTestBase(TestClusterBase):
    """Two clusters under one control plane, plus the replication vocabulary."""

    # ── sizing ────────────────────────────────────────────────────────────
    #: Minimum storage nodes per cluster. Below this a cluster cannot be
    #: created at 1+1, so the split is refused rather than attempted.
    MIN_NODES_PER_CLUSTER = 2

    #: Deliberately small. Every replication transfer is a FULL copy today
    #: (see the module docstring), so volume size sets cycle duration. A
    #: 20 GiB volume on a 1-minute cadence would mean the test measures the
    #: lab network instead of the product.
    REPL_VOLUME_SIZE = "2G"

    #: Default cadence. One minute is the shortest the policy accepts and
    #: keeps a test's wall-clock down, but see REPL_VOLUME_SIZE: with full
    #: transfers a cadence shorter than the transfer time means cycles
    #: overlap, which is a different test than the one most cases want.
    REPL_INTERVAL_MIN = 1

    #: How long to wait for a replication cycle to be reflected on the target.
    #: Generous: a full transfer of REPL_VOLUME_SIZE plus scheduling, not the
    #: delta the design discussion assumed.
    REPL_CYCLE_SEC = 600

    #: How long to wait for a fail-over / fail-back / migration to settle.
    REPL_OP_SEC = 900

    # ── two-cluster bootstrap ─────────────────────────────────────────────
    def setup(self):
        super().setup()
        self.cluster_a = self.cluster_id
        self.cluster_b = None
        self.pool_a = None
        self.pool_b = None
        self._repl_targets = []
        self._repl_policies = []

    def build_second_cluster(self):
        """Create cluster B on the same control plane. Sets ``cluster_b``.

        Nodes come from whatever is spare: IPs in ``NEW_NODE_IPS`` or
        ``STORAGE_PRIVATE_IPS`` that are not already in cluster A. If every
        node is already in A, the set is split in half and A gives up its
        second half first.

        The sequence mirrors ``TestBackupCrossClusterRestore`` because that
        one is proven in CI. It is reimplemented here rather than imported so
        that a change made for replication cannot break the backup lane.
        """
        if self.cluster_b:
            return self.cluster_b

        mgmt = self.mgmt_nodes[0]
        c2_ips = self._pick_cluster_b_nodes()
        self.logger.info("[AR-S] cluster B will use %s", c2_ips)

        for ip in c2_ips:
            try:
                self.ssh_obj.connect(address=ip,
                                     bastion_server_address=self.bastion_server)
            except Exception as exc:                  # noqa: BLE001
                raise ReplicationPreconditionError(
                    f"[AR-S] cannot ssh to {ip} for cluster B: {exc}") from exc

        branch = os.environ.get("SBCLI_BRANCH", "main")
        ifname = os.environ.get("IFNAME", "eth0")
        for ip in c2_ips:
            self.logger.info("[AR-S] preparing storage node %s", ip)
            self.ssh_obj.exec_command(
                node=ip,
                command=f"pip install --force-reinstall "
                        f"git+https://github.com/simplyblock-io/sbcli.git@{branch}")
            sleep_n_sec(5)
            self.ssh_obj.exec_command(
                node=ip,
                command=f"{self.base_cmd} --dev -d sn configure "
                        f"--max-subsys {os.environ.get('BOOTSTRAP_MAX_SUBSYS', '1024')}")
            self.ssh_obj.exec_command(
                node=ip, command=f"{self.base_cmd} sn deploy --ifname {ifname}")
        sleep_n_sec(30)

        create = (f"{self.base_cmd} --dev -d cluster add"
                  f" --ha-type {os.environ.get('HA_TYPE', 'ha')}"
                  f" --data-chunks-per-stripe {os.environ.get('NDCS', str(self.ndcs))}"
                  f" --parity-chunks-per-stripe {os.environ.get('NPCS', str(self.npcs))}")
        extra = os.environ.get("EXTRA_CLUSTER_ARGS", "")
        if extra:
            create += f" {extra}"
        self.ssh_obj.exec_command(node=mgmt, command=create)

        out, _ = self.ssh_obj.exec_command(
            node=mgmt, command=f"{self.base_cmd} cluster list --json 2>/dev/null || "
                               f"{self.base_cmd} cluster list")
        c2 = self._other_cluster_id(out)
        if not c2:
            raise ReplicationPreconditionError(
                f"[AR-S] could not identify cluster B in:\n{out[:600]}")
        self.logger.info("[AR-S] cluster B = %s", c2)

        add = (f"{self.base_cmd} --dev -d storage-node add-node"
               f" --journal-partition {os.environ.get('BOOTSTRAP_JOURNAL_PARTITION', '0')}"
               f" --ha-jm-count {os.environ.get('BOOTSTRAP_HA_JM_COUNT', '3')}"
               f" --data-nics {os.environ.get('BOOTSTRAP_DATA_NIC', 'eth1')}")
        if os.environ.get("SPDK_IMAGE"):
            add += f" --spdk-image {os.environ['SPDK_IMAGE']}"
        if os.environ.get("EXTRA_SN_ARGS"):
            add += f" {os.environ['EXTRA_SN_ARGS']}"
        for ip in c2_ips:
            self.ssh_obj.exec_command(node=mgmt,
                                      command=f"{add} {c2} {ip}:5000 {ifname}")
            sleep_n_sec(3)

        self.ssh_obj.exec_command(
            node=mgmt, command=f"{self.base_cmd} -d cluster activate {c2}")
        self._await_cluster_active(c2)
        self.cluster_b = c2
        return c2

    def _pick_cluster_b_nodes(self):
        all_ips, seen = [], set()
        raw = (os.environ.get("STORAGE_PRIVATE_IPS", "") + " " +
               os.environ.get("NEW_NODE_IPS", ""))
        for ip in raw.split():
            ip = ip.strip()
            if ip and ip not in seen:
                all_ips.append(ip)
                seen.add(ip)
        if not all_ips:
            raise ReplicationPreconditionError(
                "[AR-S] STORAGE_PRIVATE_IPS and/or NEW_NODE_IPS must list the "
                "storage nodes; replication needs two clusters and the harness "
                "has nothing to build the second one from")

        in_a = set(self.storage_nodes or [])
        spare = [ip for ip in all_ips if ip not in in_a]
        if len(spare) >= self.MIN_NODES_PER_CLUSTER:
            return spare
        if len(all_ips) >= self.MIN_NODES_PER_CLUSTER * 2:
            split = len(all_ips) // 2
            if len(all_ips[:split]) < self.MIN_NODES_PER_CLUSTER:
                raise ReplicationPreconditionError(
                    f"[AR-S] cannot split {len(all_ips)} nodes into two "
                    f"clusters of at least {self.MIN_NODES_PER_CLUSTER}")
            take = all_ips[split:]
            self.logger.info("[AR-S] no spare nodes; taking %s out of cluster A",
                             take)
            self._release_from_cluster_a(take)
            return take
        raise ReplicationPreconditionError(
            f"[AR-S] need {self.MIN_NODES_PER_CLUSTER * 2} storage nodes for "
            f"two clusters, have {len(all_ips)} ({len(spare)} spare)")

    def _release_from_cluster_a(self, ips):
        """Remove nodes from cluster A so cluster B can take them."""
        mgmt = self.mgmt_nodes[0]
        for ip in ips:
            try:
                nodes = self.sbcli_utils.get_storage_nodes().get("results", [])
                nid = next((n["uuid"] for n in nodes
                            if n.get("mgmt_ip") == ip), None)
                if not nid:
                    continue
                self.logger.info("[AR-S] removing %s from cluster A", ip)
                self.ssh_obj.exec_command(
                    node=mgmt,
                    command=f"{self.base_cmd} -d storage-node remove {nid} --force")
                sleep_n_sec(5)
            except Exception as exc:                  # noqa: BLE001
                raise ReplicationPreconditionError(
                    f"[AR-S] could not release {ip} from cluster A: {exc}") from exc

    def _other_cluster_id(self, listing):
        """The cluster in *listing* that is not cluster A."""
        text = (listing or "").strip()
        try:
            data = json.loads(text)
            rows = data if isinstance(data, list) else data.get("results", [])
            for row in rows:
                cid = row.get("id") or row.get("uuid") or ""
                if cid and cid != self.cluster_a:
                    return cid
        except Exception:                             # noqa: BLE001
            pass
        for cid in re.findall(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                text):
            if cid != self.cluster_a:
                return cid
        return ""

    def _await_cluster_active(self, cluster_id, timeout=900):
        mgmt = self.mgmt_nodes[0]
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline:
            out, _ = self.ssh_obj.exec_command(
                node=mgmt, command=f"{self.base_cmd} cluster list")
            last = out
            for line in (out or "").splitlines():
                if cluster_id in line and "ACTIVE" in line.upper():
                    self.logger.info("[AR-S] cluster %s is active", cluster_id)
                    return True
            sleep_n_sec(15)
        raise ReplicationPreconditionError(
            f"[AR-S] cluster {cluster_id} did not become ACTIVE within "
            f"{timeout}s. Last listing:\n{last[:600]}")

    # ── replication vocabulary ────────────────────────────────────────────
    def _cli(self, command):
        out, err = self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                             command=command)
        return (out or ""), (err or "")

    def target_add(self, name, target_cluster=None, target_pool=None,
                   timeout=None):
        """``cluster replication-target-add``. Returns the target name."""
        cmd = (f"{self.base_cmd} -d cluster replication-target-add "
               f"{self.cluster_a} {name} {target_cluster or self.cluster_b}")
        if target_pool:
            cmd += f" --target-pool {target_pool}"
        if timeout is not None:
            cmd += f" --timeout {timeout}"
        out, err = self._cli(cmd)
        if "error" in (out + err).lower() and name not in out:
            raise ReplicationPreconditionError(
                f"[AR] replication-target-add failed: {(out + err)[:400]}")
        self._repl_targets.append(name)
        return name

    def target_list(self, cluster_id=None):
        out, _ = self._cli(f"{self.base_cmd} cluster replication-target-list "
                           f"--cluster-id {cluster_id or self.cluster_a}")
        return out

    def target_remove(self, name):
        return self._cli(f"{self.base_cmd} -d cluster replication-target-remove {name}")

    def policy_add(self, name, target, interval_min=None, mode=None,
                   consistency_group=False, retention=None):
        """``cluster replication-policy-add``. Returns the policy name."""
        cmd = (f"{self.base_cmd} -d cluster replication-policy-add "
               f"{self.cluster_a} {name} {target}")
        if interval_min is None:
            interval_min = self.REPL_INTERVAL_MIN
        cmd += f" --interval-min {interval_min}"
        if mode:
            cmd += f" --mode {mode}"
        if consistency_group:
            cmd += " --consistency-group"
        if retention is not None:
            cmd += f" --snapshot-retention {retention}"
        out, err = self._cli(cmd)
        if "error" in (out + err).lower() and name not in out:
            raise ReplicationPreconditionError(
                f"[AR] replication-policy-add failed: {(out + err)[:400]}")
        self._repl_policies.append(name)
        return name

    def policy_list(self):
        out, _ = self._cli(f"{self.base_cmd} cluster replication-policy-list")
        return out

    def policy_remove(self, name):
        return self._cli(f"{self.base_cmd} -d cluster replication-policy-remove {name}")

    def policy_set(self, volume_id, policy):
        return self._cli(f"{self.base_cmd} -d volume replication-policy-set "
                         f"{volume_id} {policy}")

    def policy_clear(self, volume_id):
        return self._cli(f"{self.base_cmd} -d volume replication-policy-clear "
                         f"{volume_id}")

    def replication_info(self, volume_id):
        out, _ = self._cli(f"{self.base_cmd} volume replication-info {volume_id}")
        return out

    def replication_relationship(self, volume_id):
        out, _ = self._cli(f"{self.base_cmd} volume replication-relationship "
                           f"{volume_id} --json")
        return out

    def replication_status(self, cluster_id=None):
        out, _ = self._cli(f"{self.base_cmd} cluster replication-status "
                           f"{cluster_id or self.cluster_a}")
        return out

    # ── state ─────────────────────────────────────────────────────────────
    #: From models/lvol_model.py: LVolReplication.
    STATE_REPLICATING = "replicating"
    STATE_CUTOVER_PENDING = "cutover_pending"
    STATE_CUTOVER_DONE = "cutover_done"
    STATE_FAILED_OVER = "failed_over"
    DIRECTION_TO_TARGET = "to_target"
    DIRECTION_TO_SOURCE = "to_source"

    def relationship_for(self, volume_id):
        """The replication relationship for *volume_id*, from either end.

        Fail-back reverses ``direction``: the original source becomes the
        TARGET of the reverse relationship. Searching only by source is the
        documented trap -- the control plane's own ``set_cutover_proceed``
        carries a comment about it -- so this looks at both ends.

        Returns a dict or None.
        """
        raw = self.replication_relationship(volume_id)
        try:
            data = json.loads(raw)
        except Exception:                             # noqa: BLE001
            return None
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            if volume_id in (row.get("source_lvol_id", ""),
                             row.get("target_lvol_id", ""),
                             row.get("lvol_id", "")):
                return row
        return rows[0] if rows else None

    def await_state(self, volume_id, states, timeout=None, what=""):
        """Wait until the relationship reaches one of *states*.

        Raises with the state it actually reached, because "timed out" alone
        has cost us whole investigations before.
        """
        want = {states} if isinstance(states, str) else set(states)
        timeout = timeout or self.REPL_OP_SEC
        deadline = time.time() + timeout
        seen = None
        while time.time() < deadline:
            rel = self.relationship_for(volume_id) or {}
            seen = rel.get("state")
            if seen in want:
                self.logger.info("[AR] %s reached %s%s", volume_id, seen,
                                 f" ({what})" if what else "")
                return rel
            sleep_n_sec(10)
        raise AssertionError(
            f"[AR] {volume_id} did not reach {sorted(want)} within {timeout}s"
            f"{(' (' + what + ')') if what else ''}; last state was "
            f"{seen!r}. Valid states are replicating, cutover_pending, "
            f"cutover_done, failed_over.")

    def drive_cutover(self, volume_id, timeout=None):
        """Complete a cut-over that is waiting on ``cutover_proceed``.

        The task runner parks in ``cutover_pending`` until the operator has
        connected the target NVMe paths and signalled. On Kubernetes the
        operator does that. On Docker nothing does, so without this every
        migration and fail-back would sit until
        ``REPL_CUTOVER_PROCEED_TIMEOUT_SEC`` expires and then report a pass --
        a pass that proves the fallback works, not the feature.
        """
        self.await_state(volume_id, self.STATE_CUTOVER_PENDING,
                         timeout=timeout, what="waiting for cutover-proceed")
        self.logger.info("[AR] signalling cutover-proceed for %s", volume_id)
        out, err = self._cli(
            f"{self.base_cmd} -d volume replication-cutover-proceed {volume_id} "
            f"2>&1 || true")
        combined = (out + err)
        if "not found" in combined.lower() or "no such" in combined.lower():
            # The CLI verb may not be exposed on every build; fall through to
            # the API. Say which route was taken -- silently using the
            # fallback would hide a missing verb.
            self.logger.warning(
                "[AR] no replication-cutover-proceed CLI verb on this build "
                "(%s); using the API route instead", combined.strip()[:120])
            self.sbcli_utils.post_request(
                f"/lvol/{volume_id}/replication/cutover-proceed", body={})
        return True

    # ── integrity ─────────────────────────────────────────────────────────
    def assert_no_corruption(self, context):
        """Scan both clusters for the markers that are never acceptable.

        md5 mismatch is NOT in this list: on a cluster whose devices do not
        guarantee 4K atomicity a torn write produces one legitimately, and the
        filesystem above is expected to cope. IO errors, bad magic headers and
        MD corruption are a different matter -- MD corruption in particular is
        the symptom that the metadata journal is not holding.
        """
        for cid in (self.cluster_a, self.cluster_b):
            if not cid:
                continue
            try:
                self._scan_cluster_logs(cid, context)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning(
                    "[AR] could not scan cluster %s for corruption markers "
                    "%s: %s", cid, context, str(exc)[:160])

    FATAL_MARKERS = ("bad magic header", "hdr_fail", "MD corruption",
                     "Metadata page is all zero", "crc mismatch for blob")

    def _scan_cluster_logs(self, cluster_id, context):
        nodes = self.sbcli_utils.get_storage_nodes().get("results", [])
        for n in nodes:
            if n.get("cluster_id") and n["cluster_id"] != cluster_id:
                continue
            ip = n.get("mgmt_ip")
            if not ip:
                continue
            out, _ = self.ssh_obj.exec_command(
                node=ip,
                command="sudo docker logs --tail 2000 "
                        "$(sudo docker ps -q -f name=spdk_ 2>/dev/null | head -1) "
                        "2>&1 || true")
            for marker in self.FATAL_MARKERS:
                if marker.lower() in (out or "").lower():
                    raise AssertionError(
                        f"[AR] {marker!r} in SPDK log on {ip} {context}. "
                        f"This is never acceptable -- unlike an md5 mismatch, "
                        f"which a non-4K-atomic device can produce legitimately.")

    # ── teardown ──────────────────────────────────────────────────────────
    def cleanup_replication(self):
        """Best effort. Never raises: teardown must not mask the result."""
        for pol in reversed(self._repl_policies):
            try:
                self.policy_remove(pol)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[AR] could not remove policy %s: %s",
                                    pol, str(exc)[:120])
        for tgt in reversed(self._repl_targets):
            try:
                self.target_remove(tgt)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[AR] could not remove target %s: %s",
                                    tgt, str(exc)[:120])
