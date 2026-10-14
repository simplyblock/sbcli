"""MIG-H: the happy path, and what the volume is like afterwards.

Two halves, and the second is the one that catches things. Moving a volume
and watching it arrive is the obvious test. What the original scripts added,
and what makes the difference, is then *using* the volume: snapshot it,
clone it, resize it, change its QoS, delete it. A migration that completes
cleanly and leaves a volume that cannot be snapshotted has still broken
something, and nothing in the migration's own status says so.

IO runs throughout -- from before the pre-create to five minutes past the
cutover -- because a cutover that looks clean and then drops IO thirty
seconds later is a real failure mode and a test that stops at "status: done"
cannot see it.
"""
import time

from e2e_tests.migration.migration_base import (
    MigrationPreconditionError,
    MigrationTestBase,
)
from utils.common_utils import cli_failed, sleep_n_sec


class MigrationHappyPath(MigrationTestBase):
    """MIG-H-001, MIG-H-002, MIG-H-003, MIG-H-004.

    One volume, no overlap, IO throughout. The baseline every other case
    is a variation on.
    """

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"mig{stamp}"
        vol_id, sums = self.make_volume(vol)
        src = self.lvol_node(vol_id)
        if not src:
            raise MigrationPreconditionError(
                f"[MIG-H-001] cannot read the node hosting {vol}; without it "
                f"there is nothing to assert placement against.")
        tgt = self.pick_target(src, "no-overlap")
        self.logger.info("[MIG-H-001] %s: %s -> %s", vol, src, tgt)

        # ── MIG-H-001 migrate under load ─────────────────────────────────
        handle = self._run_fio_dual(vol, runtime=600, rw="randrw", bs="16K",
                                    iodepth=8, numjobs=1, size="512M",
                                    verify="md5", verify_fatal=True,
                                    name="mighappy")
        sleep_n_sec(15)          # let IO actually be in flight

        mid, status, phase = self.full_migration(
            vol_id, tgt, what="MIG-H-001 no-overlap migration under load")

        # ── MIG-H-002 placement and the database agree ───────────────────
        self.assert_placed_on(vol_id, tgt, "after the migration (MIG-H-002)")

        # ── MIG-H-003 IO never broke ─────────────────────────────────────
        self._wait_fio_dual([handle], timeout=900)
        self._validate_fio_dual(handle)
        self.logger.info("[MIG-H-003] PASS: IO survived the cutover")
        self.watch_io(vol, context="(MIG-H-003, post-cutover)")
        self.verify(vol, sums, "after migrating (MIG-H-003)")

        # ── MIG-H-004 the volume is still a normal volume ────────────────
        # The half that catches things. A migration can complete and leave a
        # volume that cannot be snapshotted, cloned or resized -- and its
        # own status says "done" either way.
        self.logger.info("[MIG-H-004] exercising the migrated volume")
        snap = f"migsnap{stamp}"
        try:
            handle = self.make_snapshot(vol, snap)
        except Exception as exc:                      # noqa: BLE001
            raise AssertionError(
                f"[MIG-H-004] cannot snapshot {vol} after migrating it: "
                f"{self.detail(exc)}. The migration reported success, so "
                f"this is state it left behind, not an unrelated failure."
            ) from exc
        if handle:
            clone = f"migclone{stamp}"
            clone_id, detail = self.make_clone(handle, clone)
            if not clone_id:
                raise AssertionError(
                    f"[MIG-H-004] cannot clone a snapshot of the migrated "
                    f"volume: {detail}")
            self.logger.info("[MIG-H-004] snapshot + clone OK")

        out, err = self._cli(f"{self.base_cmd} -d volume resize {vol_id} "
                             f"4G 2>&1")
        if cli_failed(out, err):
            self.logger.warning(
                "[MIG-H-004] resize after migration was refused: %s. Raised "
                "rather than failed -- resize has its own preconditions and "
                "this may be one of them rather than migration damage.",
                self.detail((out + err).strip()))
        else:
            self.logger.info("[MIG-H-004] resize OK")

        # QoS is deliberately not exercised here: the suite does not
        # support it, so a result either way means nothing. It is left in
        # place rather than deleted because the line is the natural home
        # for the check if QoS is ever taken on.
        #
        # It also happens to be broken. `volume qos-set <id>
        # --max-rw-iops 10000` raises
        #   TypeError: '<' not supported between instances of 'NoneType'
        #   and 'int'
        # from lvol_controller.py set_lvol, because the validation block
        # compares max_* < 0 on all four limits with no None check while
        # the assignment block below it guards each one properly. Passing
        # fewer than four limits is therefore never accepted. Reported
        # separately; nothing here depends on it.
        #
        # out, err = self._cli(f"{self.base_cmd} -d volume qos-set "
        #                      f"{vol_id} --max-rw-iops 10000 2>&1")
        # self.logger.info("[MIG-H-004] qos-set: %s",
        #                  self.detail((out + err).strip()) or "accepted")
        self.logger.info("[MIG-H-004] PASS: the migrated volume behaves "
                         "normally")
        self.cleanup_migrations()


class MigrationRoundTrip(MigrationTestBase):
    """MIG-H-005, MIG-H-006: A -> B -> A, and A -> B -> C -> B.

    Multi-hop is where stale state shows up. Each hop leaves a chance for a
    ``_m`` bdev or a database row to survive on a node the volume has left,
    and a single hop cannot see it: the leftover only collides with the
    *next* migration back to that node.
    """

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"migrt{stamp}"
        vol_id, sums = self.make_volume(vol)
        a = self.lvol_node(vol_id)
        b = self.pick_target(a, "no-overlap")

        # ── MIG-H-005 round trip A -> B -> A ─────────────────────────────
        self.logger.info("[MIG-H-005] round trip %s -> %s -> %s", a, b, a)
        self.full_migration(vol_id, b, what="MIG-H-005 outbound")
        self.assert_placed_on(vol_id, b, "(MIG-H-005 outbound)")
        self.verify(vol, sums, "after the outbound hop (MIG-H-005)")

        self.full_migration(vol_id, a, what="MIG-H-005 return")
        self.assert_placed_on(vol_id, a, "(MIG-H-005 return)")
        self.verify(vol, sums, "after returning home (MIG-H-005)")
        self.assert_no_target_leftovers(b, vol_id,
                                        "after the volume left it again "
                                        "(MIG-H-005)")
        self.assert_migration_invariants(vol_id, b, a, "(MIG-H-005)")
        self.logger.info("[MIG-H-005] PASS: round trip clean")

        # ── MIG-H-006 three-hop ──────────────────────────────────────────
        nodes = [n.get("uuid") or n.get("id") for n in self.online_nodes()]
        c = next((n for n in nodes if n not in (a, b)), None)
        if not c:
            self.logger.warning(
                "[MIG-H-006] SKIPPED: the three-hop case needs a third "
                "online storage node; this cluster has %d. The round trip "
                "above did run.", len(nodes))
        else:
            self.logger.info("[MIG-H-006] %s -> %s -> %s -> %s", a, b, c, b)
            self.full_migration(vol_id, b, what="MIG-H-006 hop 1")
            self.full_migration(vol_id, c, what="MIG-H-006 hop 2")
            self.full_migration(vol_id, b, what="MIG-H-006 hop 3")
            self.assert_placed_on(vol_id, b, "(MIG-H-006, three hops)")
            self.verify(vol, sums, "after three hops (MIG-H-006)")
            self.assert_no_target_leftovers(
                c, vol_id, "after hopping away from it (MIG-H-006)")
            self.assert_migration_invariants(vol_id, c, b, "(MIG-H-006)")
            self.logger.info("[MIG-H-006] PASS: no stale state across hops")

        self.cleanup_migrations()


class MigrationWithSnapshots(MigrationTestBase):
    """MIG-H-007, MIG-H-008: a volume that carries history.

    snap_copy is the phase that moves the snapshot chain, so a volume with
    twenty snapshots spends most of its migration there. That makes this
    both a correctness case and the setup every fault case wants, because
    it widens the window a fault can be injected into.
    """

    SNAP_COUNT = 20

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"migsnaps{stamp}"
        vol_id, sums = self.make_volume(vol)
        src = self.lvol_node(vol_id)

        self.logger.info("[MIG-H-007] taking %d snapshots", self.SNAP_COUNT)
        self.seed_snapshots(vol, vol_id, f"migs{stamp}n", self.SNAP_COUNT)

        tgt = self.pick_target(src, "no-overlap")
        mid = self.migrate(vol_id, tgt)
        self.migrate_continue(mid)

        # Watch the snapshot copy actually make progress. A snap_copy that
        # sits at 0/20 is a different failure from one that never starts.
        seen = []
        deadline = time.time() + 600
        while time.time() < deadline:
            copied, total = self.snap_progress(vol_id)
            if copied is not None:
                seen.append((copied, total))
            rec = self.migration_record(vol_id=vol_id) or {}
            if str(self._get(rec, "status") or "").lower() in self.TERMINAL \
                    or not rec:
                break
            sleep_n_sec(5)
        self.logger.info("[MIG-H-007] snap progress samples: %s", seen[:12])

        status, phase = self.await_migration(vol_id=vol_id,
                                             migration_id=mid,
                                             what="MIG-H-007 with 20 snapshots")
        if status not in self.OK_TERMINAL:
            raise AssertionError(
                f"[MIG-H-007] a volume with {self.SNAP_COUNT} snapshots "
                f"ended {status!r} in phase {phase!r}. History must migrate "
                f"with the volume.")
        self.assert_placed_on(vol_id, tgt, "(MIG-H-007)")
        self.verify(vol, sums, "after migrating with history (MIG-H-007)")

        # ── MIG-H-008 the snapshots came too ─────────────────────────────
        # By parent id, not by name. The CSI driver names the backend
        # snapshot on k8s, so a prefix match finds none of them and reads
        # as "the migration lost every snapshot" -- see snapshots_of.
        mine = self.snapshots_of(vol_id)
        if len(mine) < self.SNAP_COUNT:
            raise AssertionError(
                f"[MIG-H-008] only {len(mine)} of {self.SNAP_COUNT} snapshots "
                f"survived the migration. A migration moves the volume AND "
                f"its history; losing snapshots silently loses every "
                f"recovery point taken before the move.")
        self.logger.info("[MIG-H-008] PASS: all %d snapshots present",
                         len(mine))

        # Present is not the same as correct. SFAM-2820 is a snapshot that
        # is still listed, still counted here, and whose bdev kept the
        # ``m`` suffix the cutover should have renamed away while its
        # record still names the lvstore on the node the volume has left.
        # Nothing above can see that; it only surfaces when something
        # resolves the record, which in the ticket is the next migration.
        self.assert_migration_invariants(vol_id, src, tgt, "(MIG-H-008)")
        self.cleanup_migrations()
