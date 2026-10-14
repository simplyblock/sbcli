"""MIG-N: migrations that must be refused, and one that must not be.

The gaps in the original scripts. They were thorough about faults *during*
a migration and said almost nothing about migrations that should never have
started -- which is the cheaper class of bug to find and the easier one to
fix.

Two rules this file follows in its own logic:

* **An empty response is acceptance.** A verb that prints nothing and exits
  zero did the thing. Silence is treated as a failure here.
* **"It errored" is not "it refused for the right reason."** A typo errors
  too. Where it matters, the case checks what the cluster looks like
  afterwards rather than trusting the message.

MIG-N-006 is the one that must NOT be refused: a volume under a replication
policy being migrated between nodes. Two features that both move data,
never tested together, and the interesting outcome is not a refusal but
whether replication survives its volume moving underneath it.
"""
import os
import time

from e2e_tests.migration.migration_base import (
    MigrationTestBase,
)
from exceptions.custom_exception import SkippedTestsException
from utils.common_utils import cli_failed, sleep_n_sec


class MigrationNegativeTargets(MigrationTestBase):
    """MIG-N-001 .. MIG-N-004: targets that make no sense."""

    def _refused(self, case_id, out_err, what, allow=()):
        # One implementation, in the base: this lane now has four classes
        # that need it and they were drifting apart on which words count
        # as a refusal.
        return self.assert_refused(case_id, out_err, what, allow=allow)

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"mign{stamp}"
        vol_id, _ = self.make_volume(vol, seed=False)
        src = self.lvol_node(vol_id)

        # ── MIG-N-001 migrate to the node it is already on ───────────────
        self.logger.info("[MIG-N-001] migrating %s to its own node", vol)
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {src} 2>&1")
        self._refused("MIG-N-001", out + err,
                      "a migration whose target is the source node",
                      allow=("already",))

        # ── MIG-N-002 a target that does not exist ───────────────────────
        bogus = "00000000-0000-0000-0000-00000000dead"
        self.logger.info("[MIG-N-002] migrating to a non-existent node")
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {bogus} 2>&1")
        self._refused("MIG-N-002", out + err,
                      f"a migration to the non-existent node {bogus}")

        # ── MIG-N-003 a target that is offline ───────────────────────────
        tgt = self.pick_target(src, "no-overlap")
        next((n.get("mgmt_ip") for n in self.online_nodes()
                   if (n.get("uuid") or n.get("id")) == tgt), None)
        self.logger.info("[MIG-N-003] shutting %s down, then migrating to it",
                         tgt)
        self.sbcli_utils.shutdown_node(node_uuid=tgt)
        sleep_n_sec(45)
        try:
            out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                                 f"{vol_id} {tgt} 2>&1")
            combined = out + err
            if self.MIG_ID_RE.search(combined):
                mid = self.MIG_ID_RE.search(combined).group(1)
                self._migrations.append(mid)
                raise AssertionError(
                    f"[MIG-N-003] pre-create succeeded against an OFFLINE "
                    f"target and returned migration {mid}. The target "
                    f"subsystem cannot have been created, so this migration "
                    f"is already broken and will fail later, further from "
                    f"the cause.")
            self._refused("MIG-N-003", combined,
                          "a migration to an offline target")
        finally:
            try:
                self.sbcli_utils.restart_node(node_uuid=tgt)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG-N-003] could not restart %s: %s",
                                    tgt, str(exc)[:160])
            deadline = time.time() + 900
            while time.time() < deadline:
                if any((n.get("uuid") or n.get("id")) == tgt
                       for n in self.online_nodes()):
                    break
                sleep_n_sec(20)

        # ── MIG-N-004 two migrations for one volume ──────────────────────
        sleep_n_sec(90)       # balancing_on_restart blocks new migrations
        self.logger.info("[MIG-N-004] starting a second migration for a "
                         "volume that already has one")
        mid = self.migrate(vol_id, tgt)
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {src} 2>&1")
        combined = out + err
        second = self.MIG_ID_RE.search(combined)
        if second and second.group(1) != mid:
            self._migrations.append(second.group(1))
            raise AssertionError(
                f"[MIG-N-004] a volume now has TWO migrations in flight "
                f"({mid} and {second.group(1)}), heading for different "
                f"nodes. Whichever finishes last decides where the volume "
                f"lives, and the other's cleanup runs against state it no "
                f"longer owns.")
        self.logger.info("[MIG-N-004] PASS: second migration refused -- %s",
                         combined.strip()[:200])
        self.migrate_cancel(mid)
        self.cleanup_migrations()


class MigrationNegativeCapacity(MigrationTestBase):
    """MIG-N-005: a target without room for the volume.

    Attempted honestly: the lab usually has plenty of space, so this fills
    the target first and says so clearly if it cannot. A case that quietly
    passes because the precondition was never created is worse than one
    that skips.
    """

    def _free_bytes(self):
        """Cluster size_free in bytes, or None when it cannot be read.

        The capacity endpoint returns a list of samples; the newest is the
        one that matters.
        """
        try:
            data = self.sbcli_utils.get_cluster_capacity()
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[MIG-N-005] capacity read failed: %s",
                                str(exc)[:140])
            return None
        rows = data.get("results", data) if isinstance(data, dict) else data
        if isinstance(rows, list) and rows:
            rows = sorted(rows, key=lambda r: r.get("date", 0))
            return rows[-1].get("size_free")
        if isinstance(rows, dict):
            return rows.get("size_free")
        return None

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"migcap{stamp}"
        vol_id, _ = self.make_volume(vol, seed=False)
        src = self.lvol_node(vol_id)
        tgt = self.pick_target(src, "no-overlap")

        cap = None
        try:
            data = self.sbcli_utils.get_cluster_capacity()
            cap = data
        except Exception:                             # noqa: BLE001
            pass
        self.logger.info("[MIG-N-005] cluster capacity: %s",
                         str(cap)[:200] if cap else "unknown")

        # Try to fill the target by provisioning a volume pinned to it that
        # is larger than what is left.
        #
        # Provisioning alone does NOT fill anything: volumes are thin, so a
        # 10T request is accepted on a 6.4T cluster and size_free does not
        # move. This case used to take that acceptance as proof the target
        # was full, migrate into a target with plenty of room, and then fail
        # the product for succeeding. The precondition is measured now, and
        # when it cannot be created the result is a skip, which is honest.
        free_before = self._free_bytes()
        filler = f"migfill{stamp}"
        huge = os.environ.get("MIG_FILLER_SIZE", "10T")
        # A PVC on k8s, not `volume add`. A volume this lane creates has
        # to be the kind a Kubernetes user gets -- the whole point of the
        # k8s arm -- and a CLI-created lvol has no PersistentVolume, so it
        # is invisible to everything the operator does and would not be
        # cleaned up with the claims either.
        refused = ""
        try:
            self.make_volume(filler, size=huge, host_id=tgt, seed=False)
        except Exception as exc:                      # noqa: BLE001
            refused = str(exc)[:200]
        if refused:
            self.logger.warning(
                "[MIG-N-005] SKIPPED: the cluster refused the %s filler "
                "(%s), so the out-of-space condition could not be created.",
                huge, refused)
            self.cleanup_migrations()
            raise SkippedTestsException(
                "MIG-N-005: the cluster refused the capacity filler, so the target was never actually full")
        sleep_n_sec(20)

        free_after = self._free_bytes()
        self.logger.info("[MIG-N-005] size_free before=%s after=%s",
                         free_before, free_after)
        if free_after is None:
            self.logger.warning(
                "[MIG-N-005] SKIPPED: cluster capacity could not be read, so "
                "there is no way to confirm the target is actually full.")
            self.cleanup_migrations()
            raise SkippedTestsException(
                "MIG-N-005: cluster capacity could not be read, so a full target cannot be confirmed")
        if free_after > 0:
            self.logger.warning(
                "[MIG-N-005] SKIPPED: the %s filler was accepted but "
                "size_free is still %s bytes. Volumes are thin, so "
                "provisioning a large one consumes nothing and the target "
                "still has room -- a migration into it succeeding would be "
                "CORRECT. Genuinely filling a target means writing data to "
                "it, which is a different and much longer test than this.",
                huge, free_after)
            self.cleanup_migrations()
            raise SkippedTestsException(
                "MIG-N-005: the filler was accepted but free space did not fall, so the target is thin-provisioned")
        self.logger.info("[MIG-N-005] target is genuinely out of space "
                         "(size_free=0); the migration below must not land "
                         "there")

        self.logger.info("[MIG-N-005] migrating to a target with no room")
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {tgt} 2>&1")
        combined = out + err
        if self.MIG_ID_RE.search(combined):
            mid = self.MIG_ID_RE.search(combined).group(1)
            self._migrations.append(mid)
            self.logger.warning(
                "[MIG-N-005] pre-create was accepted against a full target "
                "(%s). Checking whether the migration then fails cleanly "
                "rather than part-way.", mid)
            self.migrate_continue(mid)
            status, phase = self.await_migration(
                vol_id=vol_id, migration_id=mid, timeout=1800,
                what="MIG-N-005 migration to a full target")
            node = self.lvol_node(vol_id)
            if node != src:
                raise AssertionError(
                    f"[MIG-N-005] a migration to a target with no room ended "
                    f"{status!r} and the volume is now on {node!r}. If the "
                    f"target cannot hold it, the volume must stay on the "
                    f"source.")
            self.logger.info(
                "[MIG-N-005] PASS: accepted at pre-create but failed cleanly "
                "(%s/%s) with the volume still on the source. Worth asking "
                "dev whether capacity should be checked at pre-create "
                "instead -- failing later is correct but costs a round trip.",
                status, phase)
        else:
            self.logger.info("[MIG-N-005] PASS: refused at pre-create -- %s",
                             combined.strip()[:200])
        self.cleanup_migrations()


class MigrationUnderReplication(MigrationTestBase):
    """MIG-N-006, MIG-N-007: migrating a volume that is being replicated.

    Not in anyone's plan, and the most interesting case in this file. Two
    features that both move data: replication ships the volume to another
    CLUSTER on a schedule, migration moves it to another NODE in this one.
    They have never been run together.

    The question is not whether migration is refused -- it may well be
    allowed, and arguably should be. It is whether the replication
    relationship survives its volume moving underneath it, and whether the
    next cycle still works. A relationship that silently stops replicating
    after a migration leaves a volume that everyone believes is protected.
    """

    def run(self):
        data = self._cli_json(f"{self.base_cmd} cluster replication-target-list "
                              f"--cluster-id {self.cluster_id}")
        rows = (data.get("results", data) if isinstance(data, dict)
                else data) or []
        if not rows:
            self.logger.warning(
                "[MIG-N-006] SKIPPED: no replication target on this cluster, "
                "so there is no replicating volume to migrate. This case "
                "needs the two-cluster lab the replication lane builds "
                "(e2e/scripts/replication_lab_setup.sh), then a policy "
                "attached before it runs.")
            raise SkippedTestsException(
                "MIG-N-006: no replication target on this cluster -- needs the two-cluster lab")

        target = self._get(rows[0], "name", "id", "uuid")
        stamp = int(time.time()) % 100000
        pol = f"migpol{stamp}"
        out, err = self._cli(f"{self.base_cmd} -d cluster "
                             f"replication-policy-add {self.cluster_id} "
                             f"{pol} {target} --interval-min 1 2>&1")
        if cli_failed(out, err):
            self.logger.warning(
                "[MIG-N-006] SKIPPED: could not create a replication policy "
                "on the existing target: %s", (out + err).strip()[:220])
            return

        vol = f"migrepl{stamp}"
        vol_id, sums = self.make_volume(vol)
        self._cli(f"{self.base_cmd} -d volume replication-policy-set "
                  f"{vol_id} {pol} 2>&1")
        sleep_n_sec(90)       # let a cycle start

        before = self._cli_json(f"{self.base_cmd} volume "
                                f"replication-relationship {vol_id}")
        self.logger.info("[MIG-N-006] relationship before the migration: %s",
                         str(before)[:240])
        if not before:
            self.logger.warning(
                "[MIG-N-006] SKIPPED: the volume never started replicating, "
                "so migrating it proves nothing about the interaction.")
            self._cli(f"{self.base_cmd} -d cluster replication-policy-remove "
                      f"{pol} 2>&1")
            self.cleanup_migrations()
            return

        src = self.lvol_node(vol_id)
        tgt = self.pick_target(src, "no-overlap")
        self.logger.info("[MIG-N-006] migrating a REPLICATING volume "
                         "%s -> %s", src, tgt)
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {tgt} 2>&1")
        combined = out + err
        if not self.MIG_ID_RE.search(combined):
            self.logger.info(
                "[MIG-N-006] PASS (by refusal): migrating a replicating "
                "volume is not allowed -- %s. That is a defensible design; "
                "worth confirming with dev it is deliberate rather than a "
                "side effect.", combined.strip()[:220])
            self._cli(f"{self.base_cmd} -d volume replication-policy-clear "
                      f"{vol_id} 2>&1")
            self._cli(f"{self.base_cmd} -d cluster replication-policy-remove "
                      f"{pol} 2>&1")
            self.cleanup_migrations()
            return

        mid = self.MIG_ID_RE.search(combined).group(1)
        self._migrations.append(mid)
        self._connect_target_paths(combined)
        self.migrate_continue(mid)
        status, phase = self.await_migration(
            vol_id=vol_id, migration_id=mid, timeout=1800,
            what="MIG-N-006 migrating a replicating volume")
        self.logger.info("[MIG-N-006] migration ended %s (%s)", status, phase)

        # ── MIG-N-007 does replication survive? ──────────────────────────
        sleep_n_sec(120)
        after = self._cli_json(f"{self.base_cmd} volume "
                               f"replication-relationship {vol_id}")
        self.logger.info("[MIG-N-007] relationship after: %s",
                         str(after)[:240])
        if not after:
            raise AssertionError(
                f"[MIG-N-007] the replication relationship for {vol} is GONE "
                f"after the volume migrated between nodes. The volume is now "
                f"unprotected and nothing says so -- an operator who set a "
                f"policy still believes it is replicating. Before the "
                f"migration it was: {str(before)[:200]}")

        self._cli(f"{self.base_cmd} -d volume replication-trigger "
                  f"{vol_id} 2>&1")
        sleep_n_sec(150)
        rel = self._cli_json(f"{self.base_cmd} volume "
                             f"replication-relationship {vol_id}") or {}
        state = str(self._get(rel if isinstance(rel, dict) else {},
                              "state") or "").lower()
        if state and state not in ("replicating", "in_sync"):
            raise AssertionError(
                f"[MIG-N-007] after the migration the relationship is in "
                f"state {state!r} and a forced cycle did not restore it. "
                f"Replication survived in name but cannot transfer.")
        self.logger.info("[MIG-N-007] PASS: replication survived the "
                         "migration and still cycles (state=%r)",
                         state or "unreported")
        self.verify(vol, sums, "after migrating a replicating volume "
                               "(MIG-N-007)")

        self._cli(f"{self.base_cmd} -d volume replication-policy-clear "
                  f"{vol_id} 2>&1")
        self._cli(f"{self.base_cmd} -d cluster replication-policy-remove "
                  f"{pol} 2>&1")
        self.cleanup_migrations()


class MigrationProtectionGuards(MigrationTestBase):
    """MIG-N-008, MIG-N-009, MIG-N-010: what must be refused DURING a move.

    From the product's own description of the feature
    (``docs/architecture/concepts/volume-migration.md``):

        "**Protection guards:** Volumes undergoing migration are protected
        from deletion, resizing, and snapshot deletion until the migration
        completes or is canceled."

    Three documented guards, and we had a case for none of them. They are
    worth more than they look: a migration transfers the volume's whole
    lineage, so deleting a snapshot halfway through removes a link the
    transfer is still walking, and resizing changes the geometry the target
    side was built for. Both are the kind of defect that corrupts rather
    than errors.

    The case also checks the guards are RELEASED afterwards, which is the
    half a naive test skips: a guard that never lifts is as broken as one
    that never applies, and it surfaces later as a volume nobody can
    manage.

    Asked through the CLI on both backends, deliberately. These are control
    plane guards rather than Kubernetes admission, and the CLI is how to
    address the control plane directly on either. Deleting a *claim* on k8s
    is a documented and different thing -- it drives the operation to
    ``Aborted`` and removes it, rather than being refused -- so that path
    is not exercised here.
    """

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"mignguard{stamp}"
        vol_id, sums = self.make_volume(vol)
        src = self.lvol_node(vol_id)

        # A snapshot to try to delete mid-flight, and enough of them that
        # snap_copy has real work to do -- the guards only mean anything
        # while the migration is actually in a phase.
        snaps = []
        for i in range(6):
            name = f"{vol}s{i}"
            try:
                self.make_snapshot(vol, name)
                snaps.append(name)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG-N-008] could not snapshot: %s",
                                    str(exc)[:160])
                break
        if not snaps:
            self.logger.warning(
                "[MIG-N-008] SKIPPED: no snapshot could be taken, so the "
                "snapshot-deletion guard cannot be tested and the volume "
                "has no lineage to make the migration slow enough to "
                "interrupt.")
            self.cleanup_migrations()
            raise SkippedTestsException(
                "MIG-N-008: no snapshot could be taken, so the guards have no lineage to protect")

        tgt = self.pick_target(src, "no-overlap")
        self.logger.info("[MIG-N-008] migrating %s (%d snapshots) %s -> %s, "
                         "then trying the three guarded operations",
                         vol, len(snaps), src, tgt)
        mid = self.migrate(vol_id, tgt)
        self.migrate_continue(mid)

        # The guards only apply while it is running. If it finishes before
        # we can ask, the case proves nothing and must say so rather than
        # pass: an operation refused because it is already done is not the
        # guard working.
        reached = self.await_phase(vol_id, ("snap_copy", "intermediate"),
                                   timeout=420)
        rec = self.migration_record(vol_id=vol_id, migration_id=mid) or {}
        status = str(self._get(rec, "status") or "").lower()
        if not reached or status in self.TERMINAL:
            self.logger.warning(
                "[MIG-N-008] SKIPPED: the migration reached %r before the "
                "guarded operations could be attempted (phase seen: %r). A "
                "refusal now would be 'already finished' rather than the "
                "guard, so this is not reported as a pass. Retry with a "
                "longer lineage or a larger volume.",
                status or "unknown", reached)
            self.await_migration(vol_id=vol_id, migration_id=mid,
                                 what="MIG-N-008 settle")
            self.cleanup_migrations()
            raise SkippedTestsException(
                "MIG-N-008: the migration finished before the guarded operations could be attempted")

        # -- MIG-N-008 delete, mid-migration -----------------------------
        out, err = self._cli(f"{self.base_cmd} -d volume delete {vol_id} "
                             f"--force 2>&1")
        self.assert_refused(
            "MIG-N-008", out + err,
            "deleting a volume while it is migrating", reject_id=False)
        if not self.lvol_node(vol_id):
            raise AssertionError(
                f"[MIG-N-008] the refusal was printed but {vol} is GONE from "
                f"the cluster. The guard reported itself and then deleted "
                f"the volume anyway, which is worse than no guard: the "
                f"migration is still running against storage that no longer "
                f"has an owner.")

        # -- MIG-N-009 resize, mid-migration -----------------------------
        out, err = self._cli(f"{self.base_cmd} -d volume resize {vol_id} "
                             f"4G 2>&1")
        self.assert_refused(
            "MIG-N-009", out + err,
            "resizing a volume while it is migrating", reject_id=False)

        # -- MIG-N-010 snapshot delete, mid-migration --------------------
        # Judged on STATE, not on wording. Whether the product refuses
        # with a sentence, an error code or silence does not matter -- the
        # guard either kept the snapshot or it did not, and that is the
        # only thing this case is about. Matching refusal words got it
        # wrong in both directions: MIG-N-003 "passed" on a connection
        # error because "Connection refused" contains "refus", and this
        # case failed on output that was neither a refusal nor a quiet
        # success but the CLI narrating itself --
        #   "INFO: Removing snapshot: 4c44671f-..."
        #   "DEBUG: Requesting method: bdev_lvol_get_lvstores"
        # which is the product going ahead and deleting it.
        # No --force, and that is the whole correction. The run that made
        # this case look like a product gap asked for a refusal while
        # passing the one flag that waives it: snapshot_controller.py
        # guards with `if active_mig and not force_delete`, so --force is
        # a documented bypass and the product was right to delete.
        #
        # The sibling guards are NOT waivable -- lvol delete and resize
        # both use a bare `if active_mig:` (lvol_controller.py:2725, 3827)
        # and refuse even under --force, which is why MIG-N-008 passes
        # while passing --force too. Three guards documented in one
        # sentence, two absolute and one bypassable; that difference is
        # worth a question to dev, but it is not this case's business.
        sid = self.snap_id(snaps[0])
        if sid:
            out, err = self._cli(f"{self.base_cmd} -d snapshot delete {sid} "
                                 f"2>&1")
            sleep_n_sec(5)
            if self.snap_id(snaps[0]):
                self.logger.info(
                    "[MIG-N-010] PASS: %s is still there, so the guard held. "
                    "Response was: %s", snaps[0],
                    (out + err).strip()[:200] or "(silent)")
            else:
                raise AssertionError(
                    f"[MIG-N-010] snapshot {snaps[0]} was DELETED while its "
                    f"volume was migrating. The other two guards held on the "
                    f"same volume moments earlier -- 'Cannot delete lvol ...: "
                    f"active migration' and 'Cannot resize lvol ...: active "
                    f"migration' -- so deletion and resize are enforced and "
                    f"snapshot deletion is not, though all three are "
                    f"documented together (and this delete passed no "
                    f"--force, so the documented bypass is not the "
                    f"explanation): 'Volumes undergoing migration are "
                    f"protected from deletion, resizing, and snapshot "
                    f"deletion until the migration completes or is "
                    f"canceled.' A migration transfers the ancestry chain "
                    f"oldest first, so removing a link while it is being "
                    f"walked is how a transfer ends up pointing at nothing. "
                    f"Response was: {(out + err).strip()[:200] or '(silent)'}")
        else:
            self.logger.warning("[MIG-N-010] SKIPPED: could not resolve the "
                                "id of %s", snaps[0])

        # -- and the guards must LIFT ------------------------------------
        status, phase = self.await_migration(
            vol_id=vol_id, migration_id=mid, what="MIG-N-008 completion")
        if status not in self.OK_TERMINAL:
            raise AssertionError(
                f"[MIG-N-008] the migration ended {status!r} after three "
                f"refused operations. Being asked to do something it "
                f"correctly refused must not disturb the migration itself.")
        self.assert_placed_on(vol_id, tgt, "(MIG-N-008 after the guards)")
        self.verify(vol, sums, "after three guarded operations were refused")

        out, err = self._cli(f"{self.base_cmd} -d volume resize {vol_id} "
                             f"4G 2>&1")
        if cli_failed(out, err):
            raise AssertionError(
                f"[MIG-N-009] the volume still cannot be resized AFTER the "
                f"migration completed: {(out + err)[:300]}. The guard is "
                f"documented to hold 'until the migration completes or is "
                f"canceled'; one that never lifts leaves a volume nobody "
                f"can manage.")
        self.logger.info("[MIG-N-009] PASS: the guard lifted once the "
                         "migration completed")
        self.cleanup_migrations()
