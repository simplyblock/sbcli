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
        if snaps:
            self.seed_snapshots(self._vol, self._vol_id,
                                f"{tag}s{stamp}n", snaps)
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
            if self.k8s_test:
                # isolate_node, not an interface drop. Dropping every
                # interface on a k8s worker takes the kubelet with it, so
                # the operator loses the node as well as the cluster does
                # and the recovery under test is a different one. isolate
                # cuts the storage paths and leaves the control plane able
                # to watch what happens, which is the fault this case
                # means. It restores itself, like the docker arm.
                self._ensure_k8s_utils().isolate_node(ip, 90)
                return lambda: True
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
        cases = [("MIG-F-001", "snap_copy"),
                 ("MIG-F-002", "intermediate"),
                 ("MIG-F-003", "lvol_migrate")]
        # Drawn order. Each case stages and tears down its own volume, so
        # the sequence means nothing -- and a fault whose recovery only
        # works when it follows a clean run is exactly what a fixed order
        # hides.
        self.rng.shuffle(cases)
        self.logger.info("[MIG-F] phases this run, in order: %s",
                         [p for _, p in cases])
        for case_id, phase in cases:
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
        # Drawn order, same reasoning: six independent faults, each on
        # its own staged volume. Running spdk_crash first every time means
        # nic_down is only ever tested on a cluster that has already
        # recovered from a crash.
        self.rng.shuffle(cases)
        self.logger.info("[MIG-F] fault order this run: %s",
                         [(s, k) for _, s, k in cases])
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
    """MIG-F-016, MIG-F-010, MIG-F-011, MIG-F-012, MIG-F-017: cancel at
    four points, and migrate again after each.

    Deterministic where a fault is not: cancel forces the same rollback path
    a real failure takes, at a moment of the test's choosing. Early and late
    in snap_copy differ because the amount to undo differs; during
    lvol_migrate the source has already been frozen, which is the one that
    could plausibly leave a volume unusable.

    **MIG-F-016 is the one that must succeed, and it was missing.** All
    three original cases cancel after ``migrate-continue``, and dev says a
    cancel is only honoured before that:

        "It should abort while still in preparing. Whenever the actual
        migration started, it cannot be canceled and has to finish first."

    So the lane had three cases on the side where cancel is refused -- and
    was logging the resulting completion as a suspected "canceled-flag
    race" -- and none on the side where it works. MIG-F-016 cancels
    between the pre-create and the continue, and REQUIRES the rollback.

    The other three now accept either outcome and record which they saw,
    because dev's answer and the operator's design doc disagree:
    ``design-persistentvolumeops.md`` section 5 says an abort is
    expressible from ``Validating`` *and* ``Migrating``, which would make a
    mid-copy cancel legal. Only one can be right, and a test should not
    pick the loser. What both readings agree on is asserted either way:
    terminal state, the volume on exactly one node, data intact, target
    clean.
    """

    def run(self):
        # ("case", phase-to-cancel-in, snapshots-to-wait-for)
        # phase None means CANCEL BEFORE THE COPY STARTS -- between the
        # pre-create and the continue. That is the one point dev says a
        # cancel actually takes:
        #
        #     "It should abort while still in preparing. Whenever the
        #     actual migration started, it cannot be canceled and has to
        #     finish first."
        #
        # Which is worth stating plainly, because every case this class
        # had cancelled AFTER the continue -- so the lane tested only the
        # side where cancel is refused, called the resulting completion a
        # "known canceled-flag race", and had no coverage at all of the
        # side where it works. MIG-F-016 is that coverage.
        #
        # Note this is also where dev and the operator's own design doc
        # disagree. design-persistentvolumeops.md section 5 says "an abort
        # is expressible from Validating and Migrating and not from
        # Verifying", which would make a cancel during the copy legal;
        # dev says the copy cannot be cancelled once started. The cases
        # below therefore ACCEPT EITHER on the post-continue points and
        # assert only what both readings agree on -- it ends terminal, the
        # volume is on exactly one node, the data verifies, and the target
        # is left clean. Whichever answer is right, those hold.
        cancels = [("MIG-F-016", None, 0),
                   ("MIG-F-010", "snap_copy", 2),
                   ("MIG-F-011", "snap_copy", 15),
                   ("MIG-F-012", "lvol_migrate", 0)]
        self.rng.shuffle(cancels)
        self.logger.info("[MIG-F] cancel points this run: %s",
                         [(c, p or "preparing") for c, p, _ in cancels])
        for case_id, phase, wait_snaps in cancels:
            self._stage(f"c{case_id[-3:]}", snaps=20)
            mid = self.migrate(self._vol_id, self._tgt)

            if phase is None:
                # ── MIG-F-016 cancel while still preparing ───────────────
                # No continue has been issued, so there is no copy to
                # interrupt: the migration exists, its target paths are
                # built, and nothing has moved. This cancel MUST take, and
                # the volume must stay on the source.
                self.logger.info(
                    "[%s] cancelling before migrate-continue -- the copy "
                    "has not started, so this is the one cancel that is "
                    "documented to work", case_id)
                self.migrate_cancel(mid)
                status, _ = self._assert_terminal_and_placed(
                    case_id, self._src, self._tgt, mid)
                if status in self.OK_TERMINAL:
                    raise AssertionError(
                        f"[{case_id}] a migration cancelled BEFORE "
                        f"migrate-continue still ended {status!r} and the "
                        f"copy ran. Cancelling while preparing is the one "
                        f"point a cancel is meant to be honoured -- nothing "
                        f"has moved yet and there is a target to tear down. "
                        f"If this cannot be cancelled either, then no "
                        f"migration can be cancelled at any point, and "
                        f"migrate-cancel does nothing.")
                self.verify(self._vol, self._sums,
                            f"after cancelling while preparing ({case_id})")
                self.assert_no_target_leftovers(
                    self._tgt, self._vol_id,
                    f"after cancelling while preparing ({case_id})")
                self.logger.info(
                    "[%s] PASS: cancelled while preparing, ended %r, volume "
                    "still on the source", case_id, status)
                self._teardown()
                continue

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
            # Either outcome is acceptable here and the case says which it
            # saw rather than judging it. Pass the target too, so a cancel
            # that did not take is judged against the node the volume
            # actually ended on.
            status, _ = self._assert_terminal_and_placed(
                case_id, self._src, self._tgt, mid)
            if status in self.OK_TERMINAL:
                self.logger.info(
                    "[%s] the cancel did NOT stop it: the migration "
                    "completed and the volume is on the target. Per dev "
                    "that is the contract once the copy has started ('it "
                    "cannot be canceled and has to finish first'), so this "
                    "is recorded rather than failed -- but it is the "
                    "opposite of what design-persistentvolumeops.md "
                    "section 5 says, and only one of the two can be right.",
                    case_id)
            else:
                self.logger.info(
                    "[%s] the cancel took: ended %r and the volume stayed "
                    "on the source, which is a rollback mid-copy.",
                    case_id, status)
            self.verify(self._vol, self._sums,
                        f"after cancelling in {phase} ({case_id})")
            self.assert_no_target_leftovers(self._tgt, self._vol_id,
                                            f"after cancel ({case_id})")

            # -- MIG-F-017 the volume is still migratable afterwards -----
            # The question none of the four cancel points asked. Every
            # assertion above is about the state the rollback left on this
            # node right now; none of them establishes that the volume can
            # still be moved, which is the thing a rollback exists to
            # preserve. Run 20261013-221421 is why it is asked: two
            # migrations failed and BOTH reported that the rollback failed
            # as well ("taking the migration back also failed: cancel
            # migration ..."), and nothing in the lane would have noticed
            # the volume was left unmovable. MIG-F-013 retries after a
            # cancel but reboots the target first, so a rollback that is
            # only repairable by a reboot still passes there.
            here = self.lvol_node(self._vol_id)
            elsewhere = self.pick_target(here, "no-overlap")
            self.logger.info(
                "[MIG-F-017] after the %s cancel, migrating again %s -> %s",
                case_id, here, elsewhere)
            try:
                self.full_migration(
                    self._vol_id, elsewhere,
                    what=f"MIG-F-017 re-migration after the {case_id} cancel")
            except Exception as exc:                      # noqa: BLE001
                raise AssertionError(
                    f"[MIG-F-017] the volume could not be migrated again "
                    f"after the {case_id} cancel: {str(exc)[:300]}. The "
                    f"rollback reported terminal, left the data intact and "
                    f"left no leftovers the previous checks could see, and "
                    f"the volume is still stuck -- so the state that blocks "
                    f"it is state none of those checks looks at."
                ) from exc
            self.assert_placed_on(self._vol_id, elsewhere,
                                  f"(MIG-F-017, after the {case_id} cancel)")
            self.verify(self._vol, self._sums,
                        "after re-migrating post-cancel (MIG-F-017)")
            self.assert_migration_invariants(
                self._vol_id, here, elsewhere,
                f"(MIG-F-017, after the {case_id} cancel)")
            self.logger.info(
                "[%s] PASS: terminal, on exactly one node, data intact, "
                "target clean, and the volume still migrates", case_id)
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
        if self.k8s_test:
            # drain first. An undrained reboot evicts the fio pods with
            # the node and the Jobs behind them count as failed, which
            # loses the IO this case is watching for corruption -- the
            # reboot is meant to reload the TARGET's lvstore from disk,
            # not to end the workload.
            #
            # And UNCORDON afterwards, which is not optional: draining
            # cordons the node and reboot_node says so in as many words,
            # "the caller uncordons once the node is Ready again". Without
            # it the node comes back but nothing can be scheduled onto it,
            # so its storage pod never returns and the StorageNode sits
            # unreachable forever. That is how run 20261013-143016 failed
            # this case at its full 1200s and then took the two cases
            # after it down with "Pre-test health check FAILED -- 1
            # storage node(s) not healthy": one missing uncordon, three
            # red classes.
            k8s = self._ensure_k8s_utils()
            # Registered BEFORE the reboot, not after. If the reboot
            # itself raises, the node may already be cordoned -- drain
            # cordons first -- and teardown has to know about it even
            # then. Registering afterwards would miss exactly the case
            # that matters.
            self._cordoned.add(ip)
            k8s.reboot_node(ip, drain=True)
            try:
                k8s.wait_node_condition(ip, ready=True, timeout=900)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning(
                    "[MIG-F-013] %s did not report Ready after the reboot "
                    "(%s); uncordoning anyway, because leaving it cordoned "
                    "is worse than uncordoning a node that is still "
                    "settling", ip, str(exc)[:140])
            k8s.cordon_node(ip, cordon=False)
            self._cordoned.discard(ip)
            self.logger.info("[MIG-F-013] uncordoned %s", ip)
        else:
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
