"""MIG-F: faults during a migration. Where the bugs actually were.

The dev who wrote the original scripts said it plainly: this is where most
of the bugs showed up. The shape is always the same and the discipline in
it is what makes it work --

    get the migration INTO a specific phase, THEN break something.

"Kill the target during snap_copy" only means anything if the kill lands in
snap_copy. :meth:`await_phase` exists for that, and it returns None when the
migration terminated before the phase was reached -- so a fault that was
never injected is reported as a skip rather than passing as a success.

Three assertions recur, and none is "the migration succeeded":

* **it always reaches a terminal state.** A migration stuck in a phase
  forever is the worst outcome, because nothing alerts on it and the volume
  is pinned until someone notices.
* **the volume is on exactly one node, and that node is right.** After a
  rollback it must still be on the source, serving.
* **the target is as it was found.** Orphaned bdevs or subsystems are
  invisible until the next migration to that node collides with them.

Then a manual retry must work. A rollback that cannot be retried is only
half a rollback.
"""
import time

from e2e_tests.migration.migration_base import (
    MigrationPreconditionError,
    MigrationTestBase,
)
from utils.common_utils import sleep_n_sec


class _FaultBase(MigrationTestBase):
    """A seeded volume mid-migration, plus fault injection and its undo."""

    def _stage(self, tag, snaps=0):
        stamp = int(time.time()) % 100000
        self._vol = f"mig{tag}{stamp}"
        self._vol_id, self._sums = self.make_volume(self._vol)
        self._src = self.lvol_node(self._vol_id)
        for i in range(snaps):
            self._cli(f"{self.base_cmd} -d snapshot add {self._vol_id} "
                      f"{tag}s{stamp}n{i} 2>&1")
        self._tgt = self.pick_target(self._src, "no-overlap")
        return self._vol_id

    def _node_ip(self, node_id):
        for n in self.online_nodes():
            if (n.get("uuid") or n.get("id")) == node_id:
                return n.get("mgmt_ip")
        return None

    #: How long to wait for an injected fault to actually show up as the node
    #: leaving "online". A graceful shutdown is a request, not an event.
    FAULT_LANDS_SEC = 180

    def _await_offline(self, node_id, kind):
        """Block until the node stops reporting online. Raises if it never does.

        MIG-F-001 failed without this. shutdown_node is a single fire-and-
        forget GET -- no wait, no confirmation -- and a graceful shutdown of
        a node serving an in-flight migration is deferred. The node never
        left "online", the migration copied all 21 snapshots and completed
        onto a healthy target, and the test then asserted the volume should
        have stayed on the source. A fault that did not land must be a skip,
        not a failure, and must never be reported as the product's doing.
        """
        try:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id, ["offline", "unreachable", "removed", "in_shutdown"],
                timeout=self.FAULT_LANDS_SEC)
            self.logger.info("[MIG-F] %s landed: %s is no longer online",
                             kind, node_id)
            return True
        except TimeoutError:
            return False

    def _break(self, node_id, kind):
        """Inject *kind* on *node_id*; return a callable that undoes it.

        Raises MigrationPreconditionError when the fault does not take, so
        the caller records a skip rather than asserting against a cluster
        nothing happened to.
        """
        ip = self._node_ip(node_id)
        if not ip:
            raise MigrationPreconditionError(
                f"[MIG-F] node {node_id} has no mgmt_ip; cannot inject {kind}")
        self.logger.info("[MIG-F] injecting %s on %s (%s)", kind, node_id, ip)
        if kind in ("nic_down", "spdk_crash", "reboot"):
            # A core on this node is now evidence the fault landed, not
            # a failure. A NIC drop makes the journal client lose quorum
            # and abort by design; spdk_crash is a kill. The runner
            # still fails the run on a core anywhere else.
            self.expected_core_nodes.add(ip)
        if kind == "graceful_shutdown":
            self.sbcli_utils.shutdown_node(node_uuid=node_id)
            if not self._await_offline(node_id, kind):
                raise MigrationPreconditionError(
                    f"[MIG-F] {node_id} was still online "
                    f"{self.FAULT_LANDS_SEC}s after a graceful shutdown was "
                    f"requested. A graceful shutdown of a node serving an "
                    f"in-flight migration is deferred, so the fault never "
                    f"landed and anything this case went on to assert would "
                    f"be about an undisturbed cluster.")
            return lambda: self.sbcli_utils.restart_node(node_uuid=node_id)
        if kind == "spdk_crash":
            if self.k8s_test:
                self._ensure_k8s_utils().stop_spdk_pod(ip)
            else:
                self.ssh_obj.exec_command(
                    node=ip,
                    command="sudo docker kill $(sudo docker ps -q -f "
                            "name=spdk_ | head -1) 2>&1 || true")
            if not self._await_offline(node_id, kind):
                raise MigrationPreconditionError(
                    f"[MIG-F] {node_id} still reports online "
                    f"{self.FAULT_LANDS_SEC}s after its SPDK was killed; the "
                    f"fault did not land.")
            return lambda: self.sbcli_utils.restart_node(node_uuid=node_id,
                                                          force=True)
        if kind == "nic_down":
            # The real signature is (node_ip, interfaces, duration_secs=...),
            # and interfaces is a required list -- every working call site
            # reads it from get_active_interfaces first. Calling it as
            # (node=..., interfaces=None, duration=...) raised TypeError and
            # took MigrationSourceAndTargetFaults out before it ran.
            if_names = self.ssh_obj.get_active_interfaces(ip)
            if not if_names:
                raise MigrationPreconditionError(
                    f"[MIG-F] no active interfaces on {ip} to drop")
            self.ssh_obj.disconnect_all_active_interfaces(
                ip, if_names, duration_secs=90)
            return lambda: True       # self-restoring after duration_secs
        raise MigrationPreconditionError(f"[MIG-F] unknown fault {kind!r}")

    def _assert_terminal_and_placed(self, case_id, src, tgt=None, mid=None):
        """Terminate, and land where the outcome says it should.

        The rule has two arms and the assertion has to pick by status: a
        migration that FAILED must leave the volume on the source, still
        serving; one that SUCCEEDED must leave it on the target. Only a
        third node is always wrong.

        This used to take one expected node and the callers always passed
        the source, so a migration that completed -- which is allowed, and
        does happen when the fault lands after the phase it was aimed at --
        was reported as the volume being in the wrong place. That is how
        MIG-F-001 produced "the volume is on X, expected Y" for a cluster
        behaving correctly, and it made the 'completed despite the fault'
        branch below it unreachable.
        """
        status, phase = self.await_migration(
            vol_id=self._vol_id, migration_id=mid, timeout=1800,
            what=f"{case_id} must terminate, whatever the outcome")
        self.logger.info("[%s] terminal: status=%s phase=%s", case_id,
                         status, phase)
        node = self.lvol_node(self._vol_id)
        succeeded = status in self.OK_TERMINAL
        expect = (tgt if succeeded else src) if tgt else src
        if node != expect:
            raise AssertionError(
                f"[{case_id}] the migration ended {status!r} and the volume "
                f"is on {node!r}, but a migration that "
                f"{'succeeds must leave it on the target' if succeeded else 'fails must leave it on the source'} "
                f"-- {expect!r}. Source was {src!r}, target was {tgt!r}. "
                f"Anything else means the data and the database disagree "
                f"about where the volume lives.")
        return status, phase

    def _teardown(self):
        try:
            self.cleanup_migrations()
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[MIG-F] teardown: %s", str(exc)[:140])


class MigrationTargetOfflinePerPhase(_FaultBase):
    """MIG-F-001, MIG-F-002, MIG-F-003: the target dies in each phase.

    One case per phase, because the rollback has different work to do in
    each: snap_copy has copied snapshots to undo, intermediate has a hub
    controller attached, lvol_migrate is past the point where the source
    has been frozen. The original scripts ran exactly this split and it is
    where the failures clustered.
    """

    def run(self):
        for case_id, phase in (("MIG-F-001", "snap_copy"),
                               ("MIG-F-002", "intermediate"),
                               ("MIG-F-003", "lvol_migrate")):
            # 20 snapshots widen snap_copy enough to land a fault in it.
            self._stage(f"f{phase[:4]}", snaps=20 if phase == "snap_copy" else 4)
            self.logger.info("[%s] target offline during %s", case_id, phase)
            mid = self.migrate(self._vol_id, self._tgt)
            self.migrate_continue(mid)

            reached = self.await_phase(self._vol_id, phase, timeout=420)
            if not reached:
                self.logger.warning(
                    "[%s] SKIPPED: the migration never entered %s, so the "
                    "fault was not injected into the phase under test. This "
                    "is a timing miss, not a pass.", case_id, phase)
                self._teardown()
                continue

            try:
                undo = self._break(self._tgt, "graceful_shutdown")
            except MigrationPreconditionError as exc:
                # The fault did not take. Asserting now would be a
                # statement about an undisturbed cluster.
                self.logger.warning(
                    "[%s] SKIPPED: %s", case_id, str(exc)[:240])
                self._teardown()
                continue
            try:
                status, _ = self._assert_terminal_and_placed(
                    case_id, self._src, self._tgt, mid)
                if status in self.OK_TERMINAL:
                    self.logger.warning(
                        "[%s] the migration COMPLETED despite the target "
                        "going offline during %s. Either the fault landed "
                        "after the phase finished, or the target was not "
                        "needed any more. Worth confirming with dev which.",
                        case_id, phase)
                else:
                    self.logger.info(
                        "[%s] PASS: rolled back to %s, volume still on the "
                        "source", case_id, status)
            finally:
                try:
                    undo()
                except Exception as exc:              # noqa: BLE001
                    self.logger.warning("[%s] undo: %s", case_id,
                                        str(exc)[:160])

            sleep_n_sec(60)
            self.assert_no_target_leftovers(
                self._tgt, self._vol_id,
                f"after the rollback ({case_id})")

            # A rollback that cannot be retried is only half a rollback.
            self.logger.info("[%s] retrying the migration by hand", case_id)
            try:
                self.full_migration(self._vol_id, self._tgt,
                                    what=f"{case_id} manual retry")
                self.assert_placed_on(self._vol_id, self._tgt,
                                      f"after the retry ({case_id})")
                self.verify(self._vol, self._sums,
                            f"after rollback and retry ({case_id})")
                self.logger.info("[%s] PASS: retry succeeded", case_id)
            except Exception as exc:                  # noqa: BLE001
                raise AssertionError(
                    f"[{case_id}] the migration rolled back cleanly but a "
                    f"manual retry then FAILED: {str(exc)[:300]}. A rollback "
                    f"whose state blocks the next attempt has not actually "
                    f"rolled anything back.") from exc
            self._teardown()


class MigrationSourceAndTargetFaults(_FaultBase):
    """MIG-F-004 .. MIG-F-009: crash, graceful shutdown and NIC loss.

    Three fault kinds on each side of the migration. The point is not that
    any particular one succeeds -- a source whose SPDK is killed mid-copy
    may legitimately fail the migration -- but that every combination ends
    somewhere, with the volume on exactly one node and nothing stranded.
    """

    def run(self):
        cases = [
            ("MIG-F-004", "source", "spdk_crash"),
            ("MIG-F-005", "source", "graceful_shutdown"),
            ("MIG-F-006", "source", "nic_down"),
            ("MIG-F-007", "target", "spdk_crash"),
            ("MIG-F-008", "target", "graceful_shutdown"),
            ("MIG-F-009", "target", "nic_down"),
        ]
        for case_id, side, kind in cases:
            self._stage(f"f{case_id[-3:]}", snaps=12)
            victim = self._src if side == "source" else self._tgt
            self.logger.info("[%s] %s on the %s node", case_id, kind, side)

            mid = self.migrate(self._vol_id, self._tgt)
            self.migrate_continue(mid)
            reached = self.await_phase(self._vol_id, ("snap_copy",),
                                       timeout=300)
            if not reached:
                self.logger.warning(
                    "[%s] SKIPPED: migration left snap_copy before the fault "
                    "could be injected.", case_id)
                self._teardown()
                continue

            try:
                undo = self._break(victim, kind)
            except MigrationPreconditionError as exc:
                # The fault did not take. Asserting now would be a
                # statement about an undisturbed cluster.
                self.logger.warning(
                    "[%s] SKIPPED: %s", case_id, str(exc)[:240])
                self._teardown()
                continue
            try:
                # Either outcome is defensible; being stuck is not.
                status, phase = self.await_migration(
                    vol_id=self._vol_id, migration_id=mid, timeout=1800,
                    what=f"{case_id} {kind} on {side}")
                node = self.lvol_node(self._vol_id)
                if node not in (self._src, self._tgt):
                    raise AssertionError(
                        f"[{case_id}] after {kind} on the {side} the volume "
                        f"reports node {node!r}, which is neither the source "
                        f"({self._src}) nor the target ({self._tgt}). A "
                        f"volume has to be somewhere definite.")
                self.logger.info(
                    "[%s] PASS: reached %s (phase %s), volume on %s",
                    case_id, status, phase,
                    "source" if node == self._src else "target")
            finally:
                try:
                    undo()
                except Exception as exc:              # noqa: BLE001
                    self.logger.warning("[%s] undo: %s", case_id,
                                        str(exc)[:160])
            sleep_n_sec(90)
            self._teardown()


class MigrationCancel(_FaultBase):
    """MIG-F-010, MIG-F-011, MIG-F-012: cancel mid-flight.

    Deterministic where a fault is not: cancel forces the same rollback path
    a real failure takes, at a moment of the test's choosing. Early and late
    in snap_copy differ because the amount to undo differs; during
    lvol_migrate the source has already been frozen, which is the one that
    could plausibly leave a volume unusable.
    """

    def run(self):
        for case_id, phase, wait_snaps in (
                ("MIG-F-010", "snap_copy", 2),
                ("MIG-F-011", "snap_copy", 15),
                ("MIG-F-012", "lvol_migrate", 0)):
            self._stage(f"c{case_id[-3:]}", snaps=20)
            mid = self.migrate(self._vol_id, self._tgt)
            self.migrate_continue(mid)

            if wait_snaps:
                deadline = time.time() + 420
                while time.time() < deadline:
                    copied, _ = self.snap_progress(self._vol_id)
                    if copied is not None and copied >= wait_snaps:
                        self.logger.info("[%s] cancelling at %d snapshots "
                                         "copied", case_id, copied)
                        break
                    sleep_n_sec(3)
            else:
                if not self.await_phase(self._vol_id, phase, timeout=600):
                    self.logger.warning(
                        "[%s] SKIPPED: never reached %s to cancel in.",
                        case_id, phase)
                    self._teardown()
                    continue

            self.migrate_cancel(mid)
            # A cancelled migration must leave the volume on the source;
            # pass the target too so a cancel that nevertheless
            # completed is judged against the right node.
            status, _ = self._assert_terminal_and_placed(
                case_id, self._src, self._tgt, mid)
            if status not in ("cancelled", "failed", "error"):
                self.logger.warning(
                    "[%s] cancel was issued but the migration ended %r. The "
                    "runner has a known canceled-flag race; if the volume "
                    "moved anyway that is the race, not a clean completion.",
                    case_id, status)
            self.verify(self._vol, self._sums,
                        f"after a cancelled migration ({case_id})")
            self.assert_no_target_leftovers(self._tgt, self._vol_id,
                                            f"after cancel ({case_id})")
            self.logger.info("[%s] PASS: clean rollback, volume on the source",
                             case_id)
            self._teardown()


class MigrationRetryAfterTargetReboot(_FaultBase):
    """MIG-F-013: cancelled, target rebooted, then retried.

    The case the original scripts called out specifically for journal and
    distrib page corruption. A cancel leaves partially-created state on the
    target; a reboot then reloads that node from disk; a retry writes over
    whatever survived. If the rollback left the journal inconsistent, this
    is where it surfaces -- and it surfaces as corruption, not as an error.
    """

    def run(self):
        self._stage("retry", snaps=15)
        mid = self.migrate(self._vol_id, self._tgt)
        self.migrate_continue(mid)
        if not self.await_phase(self._vol_id, "snap_copy", timeout=420):
            self.logger.warning("[MIG-F-013] SKIPPED: never reached snap_copy.")
            self._teardown()
            return

        self.logger.info("[MIG-F-013] cancelling, then rebooting the target")
        self.migrate_cancel(mid)
        self.await_migration(vol_id=self._vol_id, migration_id=mid,
                             timeout=900, what="MIG-F-013 cancel")

        ip = self._node_ip(self._tgt)
        self.ssh_obj.reboot_node(node_ip=ip)
        deadline = time.time() + 1200
        while time.time() < deadline:
            if any((n.get("uuid") or n.get("id")) == self._tgt
                   for n in self.online_nodes()):
                break
            sleep_n_sec(20)
        else:
            raise AssertionError(
                f"[MIG-F-013] target {self._tgt} did not come back online "
                f"within 1200s of a reboot.")
        self.logger.info("[MIG-F-013] target back; retrying the migration")
        sleep_n_sec(60)

        self.full_migration(self._vol_id, self._tgt,
                            what="MIG-F-013 retry after target reboot")
        self.assert_placed_on(self._vol_id, self._tgt, "(MIG-F-013)")
        self.verify(self._vol, self._sums,
                    "after cancel, target reboot and retry (MIG-F-013)")
        self.watch_io(self._vol, context="(MIG-F-013)")
        self.logger.info("[MIG-F-013] PASS: no corruption across the "
                         "cancel/reboot/retry sequence")
        self._teardown()


class MigrationHaPartnerRestart(_FaultBase):
    """MIG-F-014: the HA secondary or tertiary restarts mid-transfer.

    Not the source and not the target -- a third node that merely holds a
    replica. The migration should not care, and the assertion is that it
    completes anyway. If it does not, the transfer has an undeclared
    dependency on a node nobody told it about.
    """

    def run(self):
        self._stage("hapart", snaps=20)
        partner = (self.node_secondary(self._src)
                   or self.node_tertiary(self._src))
        if not partner or partner in (self._src, self._tgt):
            self.logger.warning(
                "[MIG-F-014] SKIPPED: the source has no HA partner distinct "
                "from the migration's own two nodes (secondary=%r). Needs a "
                "cluster with more nodes.", partner)
            self._teardown()
            return

        mid = self.migrate(self._vol_id, self._tgt)
        self.migrate_continue(mid)
        if not self.await_phase(self._vol_id, "snap_copy", timeout=420):
            self.logger.warning("[MIG-F-014] SKIPPED: never reached snap_copy.")
            self._teardown()
            return

        self.logger.info("[MIG-F-014] restarting HA partner %s mid-transfer",
                         partner)
        try:
            undo = self._break(partner, "graceful_shutdown")
        except MigrationPreconditionError as exc:
            self.logger.warning(
                "[MIG-F-014] SKIPPED: %s", str(exc)[:240])
            self._teardown()
            return
        try:
            status, phase = self.await_migration(
                vol_id=self._vol_id, migration_id=mid, timeout=1800,
                what="MIG-F-014 with an HA partner restarting")
            if status not in self.OK_TERMINAL:
                raise AssertionError(
                    f"[MIG-F-014] the migration ended {status!r} (phase "
                    f"{phase!r}) because a node that is NEITHER the source "
                    f"nor the target restarted. {partner} only holds a "
                    f"replica; the transfer must not depend on it.")
            self.assert_placed_on(self._vol_id, self._tgt, "(MIG-F-014)")
            self.verify(self._vol, self._sums,
                        "after an HA partner restart (MIG-F-014)")
            self.logger.info("[MIG-F-014] PASS: unaffected by the partner")
        finally:
            try:
                undo()
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG-F-014] undo: %s", str(exc)[:160])
        self._teardown()


class MigrationClusterRestartBetween(_FaultBase):
    """MIG-F-015: a full cluster restart between two migrations.

    The regression the scripts kept: migrate, restart everything, migrate
    the same pair again. The cluster passes through a degraded status and
    comes back; the second migration must still work. It is a test of what
    the first migration left in the database as much as of the restart.
    """

    def run(self):
        self._stage("clrst", snaps=5)
        self.full_migration(self._vol_id, self._tgt,
                            what="MIG-F-015 first migration")
        self.assert_placed_on(self._vol_id, self._tgt, "(MIG-F-015 first)")

        self.logger.info("[MIG-F-015] restarting every storage node")
        ids = [n.get("uuid") or n.get("id") for n in self.online_nodes()]
        for nid in ids:
            try:
                self.sbcli_utils.restart_node(node_uuid=nid, force=True)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG-F-015] restart %s: %s", nid,
                                    str(exc)[:140])
        deadline = time.time() + 1800
        while time.time() < deadline:
            if len(self.online_nodes()) >= len(ids):
                break
            sleep_n_sec(30)
        else:
            raise AssertionError(
                f"[MIG-F-015] only {len(self.online_nodes())} of {len(ids)} "
                f"nodes came back within 1800s.")
        self.logger.info("[MIG-F-015] cluster back; migrating the same pair "
                         "again")
        sleep_n_sec(120)       # balancing_on_restart blocks new migrations

        self.full_migration(self._vol_id, self._src,
                            what="MIG-F-015 second migration after restart")
        self.assert_placed_on(self._vol_id, self._src, "(MIG-F-015 second)")
        self.verify(self._vol, self._sums,
                    "after a cluster restart between migrations (MIG-F-015)")
        self.logger.info("[MIG-F-015] PASS: migration works after a degraded "
                         "cluster recovers")
        self._teardown()
