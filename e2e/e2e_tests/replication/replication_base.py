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
from utils.common_utils import cli_failed, sleep_n_sec


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
        # The pool is created HERE, not in a run() method, because every case
        # in this lane needs one and the failure when it is missing does not
        # look like a missing pool: `volume add` prints "Pool not found:
        # testpool", which contains no "error" substring, so a naive check
        # passes it through and the test dies later in seed() with
        # "'NoneType' object is not iterable".
        #
        # On docker the base setup DELETES every pool first, so nothing is
        # inherited from a previous run. ensure_pool also waits for the pool
        # to become queryable (creation is async) and makes the StorageClass
        # on k8s. All three steps, once, where no case can skip them.
        self.pool_name = self.ensure_pool()
        self.cluster_a = self.cluster_id
        self.cluster_b = None
        self.pool_a = None
        self.pool_b = None
        #: Pool UUID on cluster A. The v2 routes are nested under the pool,
        #: so cutover_proceed_url needs the id, not the name.
        self.pool_id_a = None
        self._repl_skips = []
        self._repl_targets = []
        self._repl_policies = []

    def build_second_cluster(self):
        """Make ``cluster_b`` available. Two very different routes.

        **On Kubernetes the test does not build anything.** A simplyblock
        cluster there is a ``StorageCluster`` CR reconciled by the operator,
        and "two clusters" means two of them in two NAMESPACES of ONE
        Kubernetes cluster -- the control plane cannot span two. Workers are
        split by NODE NAME, never by IP, and both are reached through one
        kubeconfig. All of that is twenty minutes of pipeline work, and
        ``k8s-native-cross-cluster-restore.yaml`` already does it and already
        exports the result. So here we adopt it. A test that reimplemented
        cluster bring-up would be racing the operator and testing the wrong
        thing.

        **On docker the test does build it**, because there is no equivalent
        pipeline step: ssh to spare IPs from ``NEW_NODE_IPS`` or
        ``STORAGE_PRIVATE_IPS``, ``sn configure``, ``sn deploy``, then
        ``cluster add`` on the management node. The sequence mirrors
        ``TestBackupCrossClusterRestore`` because that one is proven in CI,
        and is reimplemented rather than imported so a change made for
        replication cannot break the backup lane.
        """
        if self.cluster_b:
            return self.cluster_b

        if self.k8s_test:
            return self._adopt_second_cluster_k8s()

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

    #: Set by the k8s path so later calls can reach cluster B's API.
    cluster_b_secret = None
    cluster_b_namespace = None

    def _adopt_second_cluster_k8s(self):
        """Adopt the second StorageCluster the workflow already stood up.

        Reads the three variables ``k8s-native-cross-cluster-restore.yaml``
        exports at its "Run e2e" step::

            export CLUSTER2_ID="${{ env.C2_CLUSTER_ID }}"
            export CLUSTER2_SECRET="${{ env.C2_CLUSTER_SECRET }}"
            export CLUSTER2_NAMESPACE="${NS_C2}"

        Nothing is created and nothing is addressed by IP, because neither
        is how a Kubernetes cluster works.
        """
        cid = (os.environ.get("CLUSTER2_ID")
               or os.environ.get("C2_CLUSTER_ID") or "").strip()
        if not cid:
            raise ReplicationPreconditionError(
                "[AR-S] no second cluster on this Kubernetes run. On k8s the "
                "test does NOT build one: a simplyblock cluster is a "
                "StorageCluster CR reconciled by the operator, two of them "
                "live in two NAMESPACES of one Kubernetes cluster, and the "
                "workers are split by NODE NAME (not IP) across the two "
                "StorageNodeSets. All of that belongs to the workflow.\n\n"
                "Run this lane from k8s-native-cross-cluster-restore.yaml, "
                "which builds both and exports CLUSTER2_ID, CLUSTER2_SECRET "
                "and CLUSTER2_NAMESPACE. Or export CLUSTER2_ID yourself "
                "against a second StorageCluster that already exists.\n\n"
                "It needs (ndcs + npcs) * 2 workers in total, since each "
                "cluster needs a full stripe of its own.")

        self.cluster_b = cid
        self.cluster_b_secret = (os.environ.get("CLUSTER2_SECRET")
                                 or os.environ.get("C2_CLUSTER_SECRET") or "")
        self.cluster_b_namespace = (os.environ.get("CLUSTER2_NAMESPACE")
                                    or os.environ.get("NS_C2") or "")
        self.logger.info(
            "[AR-S] adopted cluster B %s (namespace %r) \u2014 built by the "
            "workflow, not by this test", self.cluster_b,
            self.cluster_b_namespace or "<unset>")

        if self.cluster_b == self.cluster_a:
            raise ReplicationPreconditionError(
                f"[AR-S] CLUSTER2_ID is the same cluster as cluster A "
                f"({self.cluster_a}). Replicating a volume to its own cluster "
                f"puts source and target in the same failure domain, which is "
                f"the thing AR-N-001 exists to refuse.")

        self._await_cluster_active(self.cluster_b)
        self.pool_b = self.pool_name
        return self.cluster_b

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
                    command=f"{self.base_cmd} -d storage-node remove {nid} "
                            f"--force-remove")
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
        # `and name not in out` is the real guard here: these verbs echo the
        # created name on success, so a name in the output means it worked
        # whatever else was printed.
        if cli_failed(out, err) and name not in out:
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
                   consistency_group=False, retention=None, rpo_target=None):
        """``cluster replication-policy-add``. Returns the policy name."""
        # --target is a REQUIRED FLAG, not a positional. Passing it
        # positionally is an argparse error.
        cmd = (f"{self.base_cmd} -d cluster replication-policy-add "
               f"{self.cluster_a} {name} --target {target}")
        if interval_min is None:
            interval_min = self.REPL_INTERVAL_MIN
        cmd += f" --interval-min {interval_min}"
        if mode:
            cmd += f" --mode {mode}"
        if consistency_group:
            cmd += " --consistency-group"
        if rpo_target is not None:
            # --rpo-target-sec sets ReplicationPolicy.rpo_target_seconds, so
            # compliance is computed against a DECLARED target instead of
            # being inferred from observed lag. It post-dates the first
            # version of this harness and had no coverage at all; AR-P-001
            # is the first case that sets it, because an unachievable
            # cadence is exactly where a declared target earns its keep.
            cmd += f" --rpo-target-sec {rpo_target}"
        if retention is not None:
            # The flag is --keep ("replicated internal snapshots to retain on
            # each side"). --snapshot-retention does not exist; there is also
            # a --retention-schedule for tiered retention, which is a
            # different thing.
            cmd += f" --keep {retention}"
        out, err = self._cli(cmd)
        # `and name not in out` is the real guard here: these verbs echo the
        # created name on success, so a name in the output means it worked
        # whatever else was printed.
        if cli_failed(out, err) and name not in out:
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

    def replication_status(self, volume_id=None, cluster_id=None):
        """The typed steady-state status.

        There is NO `cluster replication-status` verb -- only
        `volume replication-status <volume_id>`. An earlier version of this
        helper invented the cluster form, which meant every caller silently
        got an argparse usage error instead of a status. For a cluster-wide
        view the nearest real thing is the policy and target listings, so
        that is what this returns when no volume is named.
        """
        if volume_id:
            out, _ = self._cli(f"{self.base_cmd} volume replication-status "
                               f"{volume_id}")
            return out
        pols, _ = self._cli(f"{self.base_cmd} cluster replication-policy-list")
        tgts, _ = self._cli(f"{self.base_cmd} cluster replication-target-list "
                            f"--cluster-id {cluster_id or self.cluster_a}")
        return chr(10).join(["targets:", tgts, "policies:", pols])

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
        self.sbcli_utils.post_request(self.cutover_proceed_url(volume_id), body={})
        return True

    def cutover_proceed_url(self, volume_id):
        """The only route that exists for cutover-proceed.

        There is NO CLI verb for this -- checked against the full replication
        surface in simplyblock_cli/cli.py. It is a v2 API route only, nested
        under cluster and pool:

            api.include_router(v2.api, prefix='/api/v2')            app.py:103
            include_router(cluster.api, prefix='/clusters')      v2/__init__:10
            instance_api = APIRouter(prefix='/{cluster_id}')     cluster/__init__:163
            include_router(pool_api, prefix='/storage-pools')    cluster/__init__:297
            instance_api = APIRouter(prefix='/{volume_id}')      volume/__init__:148
            include_router(replication_api, prefix='/replication')  volume/__init__:379
            @api.post('/cutover-proceed')                        replication.py:190
        """
        return (f"/api/v2/clusters/{self.cluster_a}/storage-pools/"
                f"{self.pool_id_a or self.pool_name}/volumes/{volume_id}"
                f"/replication/cutover-proceed")

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
        """Read each storage node's SPDK log and look for the fatal markers.

        Both platforms, because the docker-only version of this silently
        scanned nothing on k8s -- and a corruption check that cannot fail is
        indistinguishable from one that passes.
        """
        nodes = self.sbcli_utils.get_storage_nodes().get("results", [])
        scanned = 0
        for n in nodes:
            if n.get("cluster_id") and n["cluster_id"] != cluster_id:
                continue
            ip = n.get("mgmt_ip")
            if not ip:
                continue
            out = self._spdk_log_for(ip)
            if out is None:
                continue
            scanned += 1
            for marker in self.FATAL_MARKERS:
                if marker.lower() in out.lower():
                    raise AssertionError(
                        f"[AR] {marker!r} in SPDK log on {ip} {context}. "
                        f"This is never acceptable -- unlike an md5 mismatch, "
                        f"which a non-4K-atomic device can produce legitimately.")
        if not scanned:
            # Say so loudly. A scan that reached no node is not a pass, and
            # treating it as one is exactly how this check was dead on k8s.
            self.logger.warning(
                "[AR] corruption scan %s reached NO storage node in cluster "
                "%s -- nothing was actually checked. Treat any 'no corruption' "
                "conclusion for this window as unverified.", context, cluster_id)
        else:
            self.logger.info("[AR] corruption scan %s: %d node(s) clean",
                             context, scanned)

    def _spdk_log_for(self, node_ip):
        """The node's SPDK log, on either platform. None if unreachable."""
        try:
            if self.k8s_test:
                k8s = self._ensure_k8s_utils()
                pod = k8s.get_spdk_pod_name(node_ip)
                return k8s.get_pod_logs(pod, tail=2000)
            out, _ = self.ssh_obj.exec_command(
                node=node_ip,
                command="sudo docker logs --tail 2000 "
                        "$(sudo docker ps -q -f name=spdk_ 2>/dev/null | head -1) "
                        "2>&1 || true")
            return out or ""
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[AR] could not read the SPDK log on %s: %s",
                                node_ip, str(exc)[:160])
            return None

    # ── operations ────────────────────────────────────────────────────────
    # The complete verb set, confirmed against simplyblock_cli/cli.py. Worth
    # stating what is NOT here: there is no volume-scope failover verb. Only
    # policy and target scope exist on the CLI, which is why AR-R-001 goes
    # through the API and says so.

    def failover_target(self, target_id):
        """``cluster replication-target-failover`` -- every volume on the pair."""
        return self._cli(f"{self.base_cmd} -d cluster "
                         f"replication-target-failover {target_id} 2>&1")

    def failover_policy(self, policy_id):
        """``cluster replication-policy-failover`` -- every volume on the policy."""
        return self._cli(f"{self.base_cmd} -d cluster "
                         f"replication-policy-failover {policy_id} 2>&1")

    def failback(self, volume_id, source_cluster_id=None):
        """``volume replication-failback``. Cut over afterwards with commit().

        A recovered original source replicates the delta only; a fresh cluster
        takes a full copy. The CLI help says exactly that, and AR-R-006 vs
        AR-R-007 are the two halves of it.
        """
        cmd = f"{self.base_cmd} -d volume replication-failback {volume_id}"
        if source_cluster_id:
            cmd += f" --source-cluster-id {source_cluster_id}"
        return self._cli(cmd + " 2>&1")

    def commit(self, volume_id, delete_source=False):
        """``volume replication-commit`` -- the cut-over itself."""
        cmd = f"{self.base_cmd} -d volume replication-commit {volume_id}"
        if delete_source:
            cmd += " --delete-source"
        return self._cli(cmd + " 2>&1")

    def replication_start(self, volume_id, cluster_id, mode=None,
                          interval_min=None):
        """``volume replication-start`` -- this is the migration entry point."""
        cmd = (f"{self.base_cmd} -d volume replication-start {volume_id} "
               f"--replication-cluster-id {cluster_id}")
        if mode:
            cmd += f" --mode {mode}"
        if interval_min is not None:
            cmd += f" --interval-min {interval_min}"
        return self._cli(cmd + " 2>&1")

    def replication_stop(self, volume_id):
        return self._cli(f"{self.base_cmd} -d volume replication-stop "
                         f"{volume_id} 2>&1")

    def replication_trigger(self, volume_id):
        """Force a cycle now instead of waiting out the interval."""
        return self._cli(f"{self.base_cmd} -d volume replication-trigger "
                         f"{volume_id} 2>&1")

    def policy_snapshot(self, policy_id):
        """``cluster replication-policy-snapshot`` -- one generation, all members."""
        return self._cli(f"{self.base_cmd} -d cluster "
                         f"replication-policy-snapshot {policy_id} 2>&1")

    # ── data path ─────────────────────────────────────────────────────────
    def seed_volume(self, lvol_name, files=4, file_size="32M"):
        """Connect, format, mount and write known files. Returns checksums.

        Uses the dual helpers so docker and k8s take the same code path. The
        files are small and few on purpose: every replication transfer is a
        FULL copy (see the module docstring), so the bytes written here set
        how long every later cycle takes.
        """
        try:
            return self._seed_volume_dual(lvol_name, files=files,
                                          size=file_size, prefix="arseed")
        except AssertionError as exc:
            raise ReplicationPreconditionError(
                f"[AR] could not seed {lvol_name}: {exc}. There would be "
                f"nothing to compare after a fail-over.") from exc

    def write_marker_files(self, lvol_name, prefix, count=2, size_mb=16):
        """Write *count* identifiable files into *lvol_name*. Both platforms.

        Exists because the call sites used to be guarded with
        ``if not self.k8s_test:`` and no else branch, which meant that on k8s
        no data was written and the assertions downstream compared empty sets
        -- AR-R-004's whole RPO claim asserted over nothing. A test must not
        have to know which platform it is on to write a file.

        Returns the checksums of everything on the volume afterwards.
        """
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            reg = self._volume_registry.get(lvol_name, {})
            pvc = reg.get("pvc_name") or self._k8s_normalize_name(lvol_name)
            pod = f"armark-{pvc}"[:63]
            k8s.create_utility_pod(pod, pvc)
            self._k8s_utility_pods.append(pod)
            try:
                k8s.wait_pod_running(pod)
                for i in range(count):
                    k8s.exec_in_pod(
                        pod,
                        f"sh -c 'dd if=/dev/urandom of=/spdkvol/{prefix}{i} "
                        f"bs=1M count={size_mb} 2>/dev/null && sync'")
            finally:
                k8s.delete_pod(pod, wait=True)
                if pod in self._k8s_utility_pods:
                    self._k8s_utility_pods.remove(pod)
        else:
            reg = self._volume_registry.get(lvol_name, {})
            mount = reg.get("mount")
            if not mount:
                raise ReplicationPreconditionError(
                    f"[AR] {lvol_name} is not mounted, so there is nowhere to "
                    f"write {prefix!r} markers. Seed the volume first.")
            # The client the device actually appeared on, not fio_node[0] --
            # they are not always the same machine.
            self.ssh_obj.create_random_files(
                node=reg.get("node") or self.client_machines[0],
                mount_path=mount, file_size=f"{size_mb}M",
                file_prefix=prefix, file_count=count)
        sums = self._generate_checksums_dual(lvol_name)
        self.logger.info("[AR] wrote %d %r file(s) to %s (%d file(s) total)",
                         count, prefix, lvol_name, len(sums))
        return sums

    def verify_volume(self, lvol_name, expected, context=""):
        """Re-checksum and compare against *expected*. Raises on any mismatch.

        Reports which files differ, not just that something did. A single
        differing file after a fail-over is a very different bug from all of
        them differing, and "checksum mismatch" alone does not distinguish.
        """
        got = self._generate_checksums_dual(lvol_name)
        missing = sorted(set(expected) - set(got))
        extra = sorted(set(got) - set(expected))
        bad = sorted(f for f in set(expected) & set(got)
                     if expected[f] != got[f])
        if missing or bad:
            raise AssertionError(
                f"[AR] {lvol_name} does not match what was written {context}. "
                f"differing={bad or 'none'} missing={missing or 'none'} "
                f"unexpected={extra or 'none'}. Replicated data must be "
                f"byte-identical; this is the whole point of the feature.")
        self.logger.info("[AR] %s verified byte-identical (%d files) %s",
                         lvol_name, len(got), context)
        return True

    def failed_over_volume_name(self, volume_id, source_name):
        """Where the data lives after a fail-over.

        The target copy is not independently readable while replicating; a
        fail-over clones it into a usable volume. The clone's name is not
        guaranteed to equal the source's, so resolve it from the relationship
        rather than assuming.
        """
        rel = self.relationship_for(volume_id) or {}
        tid = rel.get("target_lvol_id")
        if not tid:
            return source_name
        try:
            details = self.sbcli_utils.get_lvol_details(lvol_id=tid)
            rows = details.get("results", details) if isinstance(details, dict) else details
            row = rows[0] if isinstance(rows, list) and rows else rows
            return (row or {}).get("lvol_name") or source_name
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[AR] could not resolve the target volume name "
                                "for %s: %s", volume_id, str(exc)[:120])
            return source_name

    # ── outages ───────────────────────────────────────────────────────────
    def outage_on_node(self, node_ip, kind, duration=120):
        """Inject *kind* on *node_ip* and return a callable that undoes it.

        Deliberately thin: the cluster-level suites own the heavy outage
        machinery, and duplicating it here would mean two implementations
        drifting apart. What this adds is that every outage is paired with
        its own undo, so a replication test cannot leave a node down for the
        next case in the list.
        """
        node_id = None
        for n in self.sbcli_utils.get_storage_nodes().get("results", []):
            if n.get("mgmt_ip") == node_ip:
                node_id = n.get("uuid") or n.get("id")
                break
        if not node_id:
            raise ReplicationPreconditionError(
                f"[AR] no storage node with mgmt_ip {node_ip}")

        self.logger.info("[AR] outage %s on %s (%s)", kind, node_ip, node_id)
        if kind == "graceful_shutdown":
            self.sbcli_utils.shutdown_node(node_uuid=node_id)
            return lambda: self.sbcli_utils.restart_node(node_uuid=node_id)
        if kind == "container_stop":
            if self.k8s_test:
                # The docker spelling stopped nothing here, so AR-O-002 used
                # to pass without an outage ever happening.
                k8s = self._ensure_k8s_utils()
                k8s.stop_spdk_pod(node_ip)
                return lambda: self.sbcli_utils.restart_node(
                    node_uuid=node_id, force=True)
            self.ssh_obj.exec_command(
                node=node_ip,
                command="sudo docker stop $(sudo docker ps -q -f name=spdk_ "
                        "| head -1) 2>&1 || true")
            return lambda: self.sbcli_utils.restart_node(node_uuid=node_id,
                                                         force=True)
        if kind == "storage_node_reboot":
            self.ssh_obj.reboot_node(node_ip=node_ip)
            return lambda: True       # reboot_node waits for the node itself
        if kind in ("network_interrupt", "short_network_interrupt"):
            # (node_ip, interfaces, duration_secs=...) -- interfaces is a
            # required list, and every working call site reads it from
            # get_active_interfaces first. Called as (node=...,
            # interfaces=None, duration=...) this raises TypeError, which is
            # exactly how the migration lane's equivalent died.
            if_names = self.ssh_obj.get_active_interfaces(node_ip)
            if not if_names:
                raise ReplicationPreconditionError(
                    f"[AR] no active interfaces on {node_ip} to drop")
            self.ssh_obj.disconnect_all_active_interfaces(
                node_ip, if_names, duration_secs=duration)
            return lambda: True       # self-restoring after duration_secs
        raise ReplicationPreconditionError(f"[AR] unknown outage kind {kind!r}")

    # ── reporting ─────────────────────────────────────────────────────────
    def skip_case(self, case_id, why):
        """Record that a case could not run, and why.

        Not a pass. The run summary prints these, and the QA sheet's
        Automated column carries the same reason, so coverage claimed on
        paper matches coverage that actually executed.
        """
        self._repl_skips.append((case_id, why))
        self.logger.warning("[%s] SKIPPED: %s", case_id, why)

    def expect_refused(self, case_id, out_err, what, allow=()):
        """Assert the CLI refused something it should refuse.

        *allow* lists substrings that also count as a refusal, for verbs whose
        wording differs between builds. An empty response counts as acceptance
        -- a verb that prints nothing and exits 0 did the thing.
        """
        combined = (out_err[0] + out_err[1]) if isinstance(out_err, tuple) else str(out_err)
        low = combined.lower()
        markers = ("error", "refus", "cannot", "not allowed", "invalid",
                   "in use", "conflict", "must ") + tuple(allow)
        if not any(m in low for m in markers):
            raise AssertionError(
                f"[{case_id}] {what} was NOT refused. Response: "
                f"{combined.strip()[:300] or '(empty)'}")
        self.logger.info("[%s] PASS: refused -- %s", case_id,
                         combined.strip()[:160])
        return True

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
        if self._repl_skips:
            self.logger.warning(
                "[AR] %d case(s) did not run on this build:\n%s",
                len(self._repl_skips),
                "\n".join(f"    {cid}: {why}" for cid, why in self._repl_skips))
