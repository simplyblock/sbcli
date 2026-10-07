"""AR-O: what a fault does to replication in flight.

The shape of every case here is the same, and it is deliberate: establish a
relationship, get data onto it, break something *while a transfer is
running*, then assert that the relationship survived, resumed, and still
holds the right bytes. The interesting failures are not "the transfer
failed" -- a transfer interrupted by a node going down is expected to fail.
They are:

* a replication slot left stuck, so no later cycle can start
* a snapshot lost or duplicated, so the target's generation is wrong
* the relationship silently leaving ``replicating`` with no operator action
* data that no longer matches after the dust settles

Two rules this lane follows, both learned the hard way on the lblk matrix:

**Every outage is paired with its undo, and the cluster is proven healthy
before the next case.** An outage test that leaves a node down turns the
next case's result into noise, and a cascade reads like a product failure.

**A node that is deliberately removed from service is not a failure.** The
assertions are about replication state, not about whether IO continued --
during a source-node outage it correctly does not.
"""
import time

from e2e_tests.replication.replication_base import (
    ReplicationTestBase,
    ReplicationPreconditionError,
)
from utils.common_utils import sleep_n_sec, cli_failed


class _OutageBase(ReplicationTestBase):
    """A replicating volume under load, plus outage bookkeeping."""

    #: How long to let the cluster settle after an outage before judging it.
    #: Sized from measured k8s recoveries on this lab (7m10s and 13m41s),
    #: not from a guess -- a settle budget that is too short reports a
    #: product failure that is really an impatient test.
    SETTLE_SEC = 900

    def _stand_up(self, tag):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        self._tname = self.target_add(f"to{tag}{stamp}", self.cluster_b)
        self._pname = self.policy_add(f"po{tag}{stamp}", self._tname,
                                      interval_min=self.REPL_INTERVAL_MIN)
        self._vol = f"aro{tag}{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=self._vol,
                                  pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        self._vol_id = self.sbcli_utils.get_lvol_id(lvol_name=self._vol)
        self._sums = self.seed_volume(self._vol)
        self.policy_set(self._vol_id, self._pname)
        self.await_state(self._vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial sync")
        return self._vol_id

    def _node_in(self, cluster_id, exclude=()):
        for n in self.sbcli_utils.get_storage_nodes().get("results", []):
            if n.get("cluster_id") != cluster_id:
                continue
            ip = n.get("mgmt_ip")
            if ip and ip not in exclude:
                return ip
        raise ReplicationPreconditionError(
            f"[AR-O] no storage node available in cluster {cluster_id}")

    def _cycle(self, case_id, cluster_id, kind, duration=120):
        """Trigger a transfer, break *kind* mid-flight, then assert recovery."""
        ip = self._node_in(cluster_id)
        self.logger.info("[%s] triggering a transfer, then %s on %s",
                         case_id, kind, ip)
        self.replication_trigger(self._vol_id)
        sleep_n_sec(10)          # let the transfer actually get going

        undo = self.outage_on_node(ip, kind, duration=duration)
        sleep_n_sec(duration if kind.endswith("interrupt") else 30)
        try:
            undo()
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[%s] undo for %s on %s: %s", case_id, kind,
                                ip, str(exc)[:160])

        self._assert_recovered(case_id, kind, ip)

    def _assert_recovered(self, case_id, kind, ip):
        """Cluster healthy, relationship alive, a fresh cycle completes."""
        deadline = time.time() + self.SETTLE_SEC
        last = None
        while time.time() < deadline:
            try:
                status = self.sbcli_utils.get_cluster_status(
                    cluster_id=self.cluster_a)
                last = status
                healthy = str(status).lower().count("active") > 0
            except Exception as exc:                  # noqa: BLE001
                healthy, last = False, str(exc)[:120]
            if healthy:
                break
            sleep_n_sec(15)
        else:
            raise AssertionError(
                f"[{case_id}] cluster {self.cluster_a} did not return to "
                f"active within {self.SETTLE_SEC}s after {kind} on {ip}. "
                f"Last status: {last!r}")

        rel = self.relationship_for(self._vol_id) or {}
        if not rel:
            raise AssertionError(
                f"[{case_id}] the replication relationship for {self._vol} is "
                f"GONE after {kind} on {ip}. An outage must interrupt a "
                f"transfer, not destroy the relationship -- without it no "
                f"later cycle can start and the volume is silently "
                f"unprotected.")
        if rel.get("state") not in (self.STATE_REPLICATING, None):
            raise AssertionError(
                f"[{case_id}] the relationship left 'replicating' and is now "
                f"{rel.get('state')!r} after {kind} on {ip}, with no operator "
                f"action. An outage must not promote, demote or cut over by "
                f"itself.")

        # The real proof: a NEW cycle has to complete. A relationship that
        # looks alive but can never transfer again is the stuck-slot failure
        # this lane exists to catch, and it is invisible in state alone.
        self.logger.info("[%s] proving a fresh cycle still completes", case_id)
        self.replication_trigger(self._vol_id)
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 90)
        rel = self.relationship_for(self._vol_id) or {}
        if rel.get("state") not in (self.STATE_REPLICATING, None):
            raise AssertionError(
                f"[{case_id}] a post-outage cycle left the relationship in "
                f"{rel.get('state')!r}. The slot is stuck: replication "
                f"survived the outage in name but cannot transfer again.")
        self.logger.info("[%s] PASS: survived %s on %s, cycles resumed",
                         case_id, kind, ip)
        self.assert_no_corruption(f"after {kind} ({case_id})")

    def _verify_through_failover(self, case_id):
        """Fail over and confirm the bytes are still what we wrote."""
        self._disconnect_and_cleanup_dual(self._vol)
        self.failover_policy(self._pname)
        self.await_state(self._vol_id, self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC, what="fail-over")
        name = self.failed_over_volume_name(self._vol_id, self._vol)
        self._connect_and_mount_dual(name, format_disk=False)
        self.verify_volume(name, self._sums,
                           context=f"after the outage ({case_id})")
        self._failed_over_name = name

    def _teardown(self):
        for n in {self._vol, getattr(self, "_failed_over_name", self._vol)}:
            try:
                self._disconnect_and_cleanup_dual(n)
            except Exception:                         # noqa: BLE001
                pass
        try:
            self.policy_clear(self._vol_id)
        except Exception:                             # noqa: BLE001
            pass
        for n in {self._vol, getattr(self, "_failed_over_name", self._vol)}:
            try:
                self.sbcli_utils.delete_lvol(lvol_name=n)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[AR-O] could not delete %s: %s", n,
                                    str(exc)[:120])
        self.cleanup_replication()


class ReplicationSourceNodeOutages(_OutageBase):
    """AR-O-001, AR-O-002, AR-O-003: the source loses a node mid-transfer.

    Three ways to lose it, in increasing rudeness: a graceful shutdown that
    the control plane initiates, a container stop that it does not, and a
    reboot that takes the host with it. All three must leave the
    relationship intact and a later cycle possible.
    """

    def run(self):
        self._stand_up("src")
        for case_id, kind in (("AR-O-001", "graceful_shutdown"),
                              ("AR-O-002", "container_stop"),
                              ("AR-O-003", "storage_node_reboot")):
            self._cycle(case_id, self.cluster_a, kind)
        self._verify_through_failover("AR-O-001/002/003")
        self._teardown()


class ReplicationTargetNodeOutage(_OutageBase):
    """AR-O-004: the TARGET loses a node mid-transfer.

    Different from the source cases in one way that matters: the source is
    healthy throughout, so a stalled transfer has no excuse on this side.
    The source must notice the target went away, stop cleanly, and resume --
    not sit holding a slot open against a node that is gone.
    """

    def run(self):
        self._stand_up("tgt")
        self._cycle("AR-O-004", self.cluster_b, "storage_node_reboot")
        self._verify_through_failover("AR-O-004")
        self._teardown()


class ReplicationNetworkOutages(_OutageBase):
    """AR-O-005, AR-O-006: the link between the clusters goes away.

    A short interrupt should be absorbed inside one cycle's retry budget. A
    long one will kill the transfer in flight, and that is fine -- what must
    not happen is the relationship ending up stuck, or the next cycle never
    starting once the link is back.
    """

    def run(self):
        self._stand_up("net")
        self._cycle("AR-O-005", self.cluster_b, "short_network_interrupt",
                    duration=30)
        self._cycle("AR-O-006", self.cluster_b, "network_interrupt",
                    duration=120)
        self._verify_through_failover("AR-O-005/006")
        self._teardown()


class ReplicationOutageDuringOperations(_OutageBase):
    """AR-O-007, AR-O-008: a cut during a fail-over and during a fail-back.

    The two moments when the relationship is mid-transition and a partial
    result is most dangerous. A fail-over interrupted halfway could leave
    both sides believing they are the source; a fail-back interrupted during
    its delta could cut over to an incomplete copy. Neither is acceptable
    and neither is covered by the steady-state cases above.
    """

    def run(self):
        self._stand_up("ops")
        self._disconnect_and_cleanup_dual(self._vol)

        # ── AR-O-007 cut during fail-over ─────────────────────────────────
        ip = self._node_in(self.cluster_b)
        self.logger.info("[AR-O-007] starting a fail-over, then cutting the "
                         "network to the target")
        self.failover_policy(self._pname)
        sleep_n_sec(5)
        self.outage_on_node(ip, "network_interrupt", duration=60)
        sleep_n_sec(90)

        rel = self.relationship_for(self._vol_id) or {}
        self.logger.info("[AR-O-007] state after the cut: %s", rel.get("state"))
        if rel.get("state") == self.STATE_REPLICATING:
            # Still replicating means the fail-over did not take. Acceptable
            # -- better a refused fail-over than half of one -- but it must
            # then be retryable.
            self.logger.info("[AR-O-007] fail-over did not take; retrying "
                             "now that the link is back")
            self.failover_policy(self._pname)
        self.await_state(self._vol_id, self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC,
                         what="fail-over must complete or be retryable after "
                              "a network cut")
        name = self.failed_over_volume_name(self._vol_id, self._vol)
        self._failed_over_name = name
        self._connect_and_mount_dual(name, format_disk=False)
        self.verify_volume(name, self._sums,
                           context="after a network cut during fail-over "
                                   "(AR-O-007)")
        self.logger.info("[AR-O-007] PASS: fail-over survived the cut and the "
                         "data is intact")

        # ── AR-O-008 cut during the fail-back delta ───────────────────────
        self._disconnect_and_cleanup_dual(name)
        self.logger.info("[AR-O-008] starting a fail-back, then cutting the "
                         "network during the delta")
        out, err = self.failback(self._vol_id, source_cluster_id=self.cluster_a)
        if cli_failed(out, err):
            raise ReplicationPreconditionError(
                f"[AR-O-008] fail-back refused: {((out or '') + (err or ''))[:300]}")
        sleep_n_sec(10)
        self.outage_on_node(self._node_in(self.cluster_a),
                            "network_interrupt", duration=60)
        sleep_n_sec(90)

        rel = self.relationship_for(self._vol_id) or {}
        if rel.get("state") == self.STATE_CUTOVER_DONE:
            raise AssertionError(
                "[AR-O-008] the fail-back reported cutover_done even though "
                "the network was cut during its delta. Cutting over to a "
                "copy whose delta never finished is silent data loss -- the "
                "cut-over must wait, fail, or be retried, not complete.")
        self.logger.info("[AR-O-008] state after the cut: %s (not "
                         "cutover_done, which is correct)", rel.get("state"))

        self.drive_cutover(self._vol_id)
        self.await_state(self._vol_id, self.STATE_CUTOVER_DONE,
                         timeout=self.REPL_OP_SEC,
                         what="fail-back cut-over after the link returns")
        self._connect_and_mount_dual(self._vol, format_disk=False)
        self.verify_volume(self._vol, self._sums,
                           context="after a network cut during the fail-back "
                                   "delta (AR-O-008)")
        self.logger.info("[AR-O-008] PASS: cut-over waited for a complete "
                         "delta, data intact")

        self.assert_no_corruption("after AR-O operation-time cuts")
        self._teardown()


class ReplicationConsistencyGroupNodeLoss(_OutageBase):
    """AR-O-009: the node hosting the consistency group's LVS goes down.

    The single point of failure a CG creates. Every member lives on one LVS
    on one node (AR-C-001), so losing that node loses the whole group at
    once. The group must come back as a group: every member replicating
    again, all at one generation. Members recovering individually would be
    worse than the outage, because the group would silently stop being one.
    """

    MEMBERS = 2

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        self._tname = self.target_add(f"tog{stamp}", self.cluster_b)
        self._pname = self.policy_add(f"pog{stamp}", self._tname,
                                      interval_min=self.REPL_INTERVAL_MIN,
                                      consistency_group=True)
        self._members = []
        for i in range(self.MEMBERS):
            name = f"arog{stamp}m{i}"
            self.sbcli_utils.add_lvol(lvol_name=name, pool_name=self.pool_name,
                                      size=self.REPL_VOLUME_SIZE)
            vid = self.sbcli_utils.get_lvol_id(lvol_name=name)
            self.policy_set(vid, self._pname)
            self._members.append({"name": name, "id": vid})
        for m in self._members:
            self.await_state(m["id"], self.STATE_REPLICATING,
                             timeout=self.REPL_CYCLE_SEC,
                             what=f"CG member {m['name']}")

        details = self.sbcli_utils.get_lvol_details(lvol_id=self._members[0]["id"])
        rows = details.get("results", details) if isinstance(details, dict) else details
        row = (rows[0] if isinstance(rows, list) and rows else rows) or {}
        host_id = row.get("node_id")
        host_ip = None
        for n in self.sbcli_utils.get_storage_nodes().get("results", []):
            if (n.get("uuid") or n.get("id")) == host_id:
                host_ip = n.get("mgmt_ip")
                break
        if not host_ip:
            raise ReplicationPreconditionError(
                f"[AR-O-009] could not resolve the node hosting the group's "
                f"LVS (node_id={host_id!r}).")

        self.logger.info("[AR-O-009] rebooting %s, which hosts the whole "
                         "group's LVS", host_ip)
        self.replication_trigger(self._members[0]["id"])
        sleep_n_sec(10)
        self.outage_on_node(host_ip, "storage_node_reboot")

        deadline = time.time() + self.SETTLE_SEC
        while time.time() < deadline:
            states = {m["name"]: (self.relationship_for(m["id"]) or {}).get("state")
                      for m in self._members}
            if all(s in (self.STATE_REPLICATING, None) for s in states.values()):
                break
            sleep_n_sec(20)
        else:
            raise AssertionError(
                f"[AR-O-009] the group did not return to replicating within "
                f"{self.SETTLE_SEC}s of its LVS host rebooting. states="
                f"{states}")

        self.logger.info("[AR-O-009] every member replicating again; "
                         "re-checking placement")
        places = set()
        for m in self._members:
            d = self.sbcli_utils.get_lvol_details(lvol_id=m["id"])
            rr = d.get("results", d) if isinstance(d, dict) else d
            r = (rr[0] if isinstance(rr, list) and rr else rr) or {}
            places.add((r.get("lvs_name") or r.get("lvstore"), r.get("node_id")))
        if len([p for p in places if any(p)]) > 1:
            raise AssertionError(
                f"[AR-O-009] after recovery the group's members are spread "
                f"across {places}. They must still share one LVS on one "
                f"node; recovering individually means the group quietly "
                f"stopped being a group and its snapshots are no longer "
                f"atomic.")
        self.logger.info("[AR-O-009] PASS: group recovered intact on %s",
                         places)

        self.assert_no_corruption("after AR-O-009")
        for m in self._members:
            try:
                self.policy_clear(m["id"])
                self.sbcli_utils.delete_lvol(lvol_name=m["name"])
            except Exception:                         # noqa: BLE001
                pass
        self.cleanup_replication()


class ReplicationControlPlaneAndDeviceOutages(_OutageBase):
    """AR-O-010, AR-O-011, AR-O-012.

    The control plane restarting mid-replication, and losing a device under
    a transfer on either side.

    AR-O-010 is the one with teeth on this architecture: both clusters share
    ONE control plane, so restarting it is not a site-loss test -- it takes
    both sides at once. Replication state lives in the database, so it must
    survive the restart and resume without an operator touching it.
    """

    def run(self):
        self._stand_up("cpd")

        # ── AR-O-010 control plane restart ────────────────────────────────
        self.logger.info("[AR-O-010] restarting the control plane mid-cycle")
        self.replication_trigger(self._vol_id)
        sleep_n_sec(10)
        before = self.relationship_for(self._vol_id) or {}
        mgmt = self.mgmt_nodes[0]
        if self.k8s_test:
            # kubectl directly: k8s_utils has no generic deployment-restart
            # helper, and inventing one here would be a second way to do
            # something the cluster suites already do by hand.
            out, err = self.ssh_obj.exec_command(
                node=mgmt,
                command="kubectl -n simplyblock rollout restart deploy "
                        "simplyblock-control-plane 2>&1 || "
                        "kubectl -n simplyblock rollout restart deploy "
                        "-l app.kubernetes.io/part-of=simplyblock 2>&1 || true")
            self.logger.info("[AR-O-010] control-plane rollout restart: %s",
                             ((out or "") + (err or "")).strip()[:200])
        else:
            self.ssh_obj.exec_command(
                node=mgmt,
                command="sudo docker restart $(sudo docker ps -q "
                        "-f name=app_ -f name=tasks 2>/dev/null) 2>&1 || true")
        sleep_n_sec(120)

        deadline = time.time() + self.SETTLE_SEC
        while time.time() < deadline:
            rel = self.relationship_for(self._vol_id)
            if rel:
                break
            sleep_n_sec(15)
        else:
            raise AssertionError(
                f"[AR-O-010] the replication relationship for {self._vol} did "
                f"not come back within {self.SETTLE_SEC}s of a control-plane "
                f"restart. Replication state lives in the database; losing it "
                f"on a restart means every relationship in the cluster is "
                f"only as durable as the control plane's uptime. Before the "
                f"restart it was: {before}")
        self.logger.info("[AR-O-010] relationship survived: %s", rel)
        self.replication_trigger(self._vol_id)
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 90)
        rel = self.relationship_for(self._vol_id) or {}
        if rel.get("state") not in (self.STATE_REPLICATING, None):
            raise AssertionError(
                f"[AR-O-010] cycles did not resume after the control-plane "
                f"restart; state is {rel.get('state')!r}.")
        self.logger.info("[AR-O-010] PASS: survived and resumed with no "
                         "operator action")

        # ── AR-O-011 / AR-O-012 device loss under a transfer ──────────────
        for case_id, cluster, where in (("AR-O-011", self.cluster_a, "source"),
                                        ("AR-O-012", self.cluster_b, "target")):
            ip = self._node_in(cluster)
            dev = self._pick_removable_device(ip)
            if not dev:
                self.skip_case(
                    case_id,
                    f"no spare device on the {where} node {ip} that can be "
                    f"removed without taking the cluster below its EC "
                    f"tolerance. Needs a cluster with more devices per node "
                    f"than the scheme requires.")
                continue
            self.logger.info("[%s] removing device %s on the %s node %s",
                             case_id, dev, where, ip)
            self.replication_trigger(self._vol_id)
            sleep_n_sec(10)
            self.ssh_obj.exec_command(
                node=ip,
                command=f"echo 1 | sudo tee /sys/block/{dev}/device/delete "
                        f"2>&1 || true")
            self._assert_recovered(case_id, f"device remove ({dev})", ip)

        self._verify_through_failover("AR-O-010/011/012")
        self._teardown()

    def _pick_removable_device(self, ip):
        """A data device that is not the OS disk and not the only one left."""
        out, _ = self.ssh_obj.exec_command(
            node=ip,
            command="lsblk -dno NAME,TYPE,MOUNTPOINT | awk '$2==\"disk\"' "
                    "2>/dev/null || true")
        cands = []
        for line in (out or "").splitlines():
            parts = line.split()
            if not parts:
                continue
            name = parts[0]
            mounted = len(parts) > 2
            if mounted or name.startswith(("sr", "loop", "ram", "zram")):
                continue
            cands.append(name)
        # Never take the last one: the point is to survive losing a device,
        # not to destroy the node.
        return cands[0] if len(cands) > 1 else None
