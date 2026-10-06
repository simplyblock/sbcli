"""MIG-T: HA overlap, snapshot/clone trees, and concurrency.

Three shapes of "the data is not a single simple volume", each of which the
original scripts found bugs in.

**The HA overlap matrix** is the one that needs explaining. A migration has
a source and a target, and both have HA partners. When the target happens
to BE one of the source's partners, the resulting path layout is different
from the no-overlap case, and so are the ANA states the client sees. Four
shapes beyond no-overlap:

    a   target's PRIMARY is the source's SECONDARY
    b   source's PRIMARY is the target's SECONDARY
    c   target's PRIMARY is the source's TERTIARY
    d   source's PRIMARY is the target's TERTIARY

None is exotic -- a scheduler picking a target by free capacity will land
on them regularly. They are separate cases because the code paths differ,
not because the data does.

**Trees** are snapshots and clones hanging off the volume being moved. The
failure they expose is stale state: a ``_m`` bdev or database row left on a
node the volume has left. A single migration cannot see it; the leftover
only collides with the next one. One constraint from the scripts worth
keeping: the clones must not share a subsystem, or the test is exercising
the batch path by accident.

**Concurrency** is N volumes moving between the same pair at once.
"""
import time

from e2e_tests.migration.migration_base import (
    MigrationTestBase,
    MigrationPreconditionError,
)
from utils.common_utils import sleep_n_sec


class MigrationHaOverlapMatrix(MigrationTestBase):
    """MIG-T-001 .. MIG-T-005: every overlap shape.

    Each shape is attempted and, if the cluster cannot provide it, recorded
    as a skip naming what was missing. That matters: 'a' needs the source to
    have a secondary, 'c' needs a tertiary, and 'b'/'d' need a node whose
    partner is the source. A three-node 1+1 cluster can offer some and not
    others, and silently substituting no-overlap would turn four distinct
    cases into one rerun four times.
    """

    SHAPES = (("MIG-T-001", "no-overlap"),
              ("MIG-T-002", "a"),
              ("MIG-T-003", "b"),
              ("MIG-T-004", "c"),
              ("MIG-T-005", "d"))

    def run(self):
        for case_id, shape in self.SHAPES:
            stamp = int(time.time()) % 100000
            vol = f"migov{shape.replace('-', '')}{stamp}"
            vol_id, sums = self.make_volume(vol)
            src = self.lvol_node(vol_id)
            try:
                tgt = self.pick_target(src, shape)
            except MigrationPreconditionError as exc:
                self.logger.warning("[%s] SKIPPED (%s): %s", case_id, shape,
                                    str(exc)[:220])
                self.cleanup_migrations()
                continue

            self.logger.info("[%s] overlap %r: %s -> %s  (src sec=%s ter=%s)",
                             case_id, shape, src, tgt,
                             self.node_secondary(src), self.node_tertiary(src))
            before = self._path_count(vol)
            self.full_migration(vol_id, tgt,
                                what=f"{case_id} overlap {shape}")
            self.assert_placed_on(vol_id, tgt, f"({case_id}, overlap {shape})")
            self.verify(vol, sums, f"after an overlap-{shape} migration "
                                   f"({case_id})")
            after = self._path_count(vol)
            self.logger.info("[%s] client paths %s -> %s", case_id, before,
                             after)
            if after == 0:
                raise AssertionError(
                    f"[{case_id}] the client has NO paths to {vol} after an "
                    f"overlap-{shape} migration. The cutover is supposed to "
                    f"be an ANA flip between paths that are both already "
                    f"connected, not a disconnect.")
            self.logger.info("[%s] PASS", case_id)
            self.cleanup_migrations()

    def _path_count(self, name):
        """How many NVMe paths the client has for this volume."""
        if self.k8s_test:
            return "n/a (k8s)"
        node = self.fio_node[0] if self.fio_node else self.mgmt_nodes[0]
        out, _ = self.ssh_obj.exec_command(
            node=node, command="sudo nvme list-subsys 2>/dev/null | "
                               "grep -c live || true")
        try:
            return int((out or "0").strip().splitlines()[0])
        except (ValueError, IndexError):
            return 0


class MigrationSnapshotCloneTrees(MigrationTestBase):
    """MIG-T-006 .. MIG-T-009: migrating things with descendants.

    Four orders, because which one you move first changes what has to be
    rewritten: the parent before its clones, a clone before its parent, a
    deep chain, and the round trip that brings a whole tree home.
    """

    def run(self):
        stamp = int(time.time()) % 100000

        # ── build a tree: volume -> snapshot -> two clones ───────────────
        vol = f"migtree{stamp}"
        vol_id, sums = self.make_volume(vol)
        src = self.lvol_node(vol_id)
        snap = f"migtsnap{stamp}"
        self._cli(f"{self.base_cmd} -d snapshot add {vol_id} {snap} 2>&1")
        snap_id = self._snap_id(snap)
        if not snap_id:
            raise MigrationPreconditionError(
                f"[MIG-T-006] could not resolve the snapshot {snap}; there is "
                f"no tree to migrate.")
        clones = []
        for i in range(2):
            cn = f"migtclone{stamp}n{i}"
            # Distinct subsystems on purpose: clones sharing one would make
            # this the batch path, which MIG-B covers separately.
            out, err = self._cli(f"{self.base_cmd} -d snapshot clone "
                                 f"{snap_id} {cn} 2>&1")
            if "error" in (out + err).lower():
                raise MigrationPreconditionError(
                    f"[MIG-T-006] could not clone {snap}: {(out + err)[:240]}")
            self._mig_vols.append(cn)
            clones.append((cn, self.sbcli_utils.get_lvol_id(lvol_name=cn)))
        tgt = self.pick_target(src, "no-overlap")

        # ── MIG-T-006 migrate the parent while clones exist ──────────────
        self.logger.info("[MIG-T-006] migrating the parent with %d clones "
                         "attached", len(clones))
        self.full_migration(vol_id, tgt, what="MIG-T-006 parent with clones")
        self.assert_placed_on(vol_id, tgt, "(MIG-T-006)")
        self.verify(vol, sums, "after migrating the parent (MIG-T-006)")
        for cn, cid in clones:
            node = self.lvol_node(cid)
            self.logger.info("[MIG-T-006] clone %s is on %s", cn, node)
            if node is None:
                raise AssertionError(
                    f"[MIG-T-006] clone {cn} has no node after its parent "
                    f"migrated. Moving a parent must not orphan its "
                    f"descendants.")
        self.logger.info("[MIG-T-006] PASS: clones survived the parent's move")

        # ── MIG-T-007 migrate a clone away from its parent ───────────────
        cn, cid = clones[0]
        cl_src = self.lvol_node(cid)
        cl_tgt = self.pick_target(cl_src, "no-overlap")
        self.logger.info("[MIG-T-007] migrating clone %s away from its parent",
                         cn)
        self.full_migration(cid, cl_tgt, what="MIG-T-007 clone alone")
        self.assert_placed_on(cid, cl_tgt, "(MIG-T-007)")
        if self.lvol_node(vol_id) != tgt:
            raise AssertionError(
                "[MIG-T-007] migrating a clone moved its PARENT as well. The "
                "two are separate volumes; only the one named should move.")
        self.logger.info("[MIG-T-007] PASS: clone moved alone")

        # ── MIG-T-008 the tree comes home ────────────────────────────────
        self.logger.info("[MIG-T-008] bringing the parent back to %s", src)
        self.full_migration(vol_id, src, what="MIG-T-008 tree round trip")
        self.assert_placed_on(vol_id, src, "(MIG-T-008)")
        self.verify(vol, sums, "after the tree round trip (MIG-T-008)")
        self.assert_no_target_leftovers(
            tgt, vol_id, "after the tree left it again (MIG-T-008)")
        self.logger.info("[MIG-T-008] PASS: no stale tree state left behind")

        # ── MIG-T-009 snapshots still usable after all that ──────────────
        newclone = f"migtpost{stamp}"
        out, err = self._cli(f"{self.base_cmd} -d snapshot clone {snap_id} "
                             f"{newclone} 2>&1")
        if "error" in (out + err).lower():
            raise AssertionError(
                f"[MIG-T-009] the snapshot cannot be cloned after its volume "
                f"was migrated twice: {(out + err)[:300]}. The chain has been "
                f"left pointing at something that moved.")
        self._mig_vols.append(newclone)
        self.logger.info("[MIG-T-009] PASS: the chain is still usable")
        self.cleanup_migrations()

    def _snap_id(self, name):
        data = self._cli_json(f"{self.base_cmd} snapshot list")
        for s in (data.get("results", data) if isinstance(data, dict)
                  else data) or []:
            if self._get(s, "snap_name", "name") == name:
                return self._get(s, "uuid", "id")
        return None


class MigrationConcurrent(MigrationTestBase):
    """MIG-T-010, MIG-T-011: several volumes between one pair at once.

    Sequential first, as the control, then concurrent. If sequential passes
    and concurrent does not, the problem is contention rather than
    migration -- a distinction the failure message should make, because the
    two have very different fixes.
    """

    COUNT = 4

    def run(self):
        stamp = int(time.time()) % 100000
        vols = []
        for i in range(self.COUNT):
            name = f"migcon{stamp}n{i}"
            vid, sums = self.make_volume(name, seed=(i == 0))
            vols.append({"name": name, "id": vid, "sums": sums})
        src = self.lvol_node(vols[0]["id"])
        tgt = self.pick_target(src, "no-overlap")

        # ── MIG-T-010 sequential, as the control ─────────────────────────
        self.logger.info("[MIG-T-010] %d migrations one after another",
                         self.COUNT)
        t0 = time.time()
        for v in vols:
            self.full_migration(v["id"], tgt,
                                what=f"MIG-T-010 sequential {v['name']}")
            self.assert_placed_on(v["id"], tgt, "(MIG-T-010)")
        seq = time.time() - t0
        self.logger.info("[MIG-T-010] PASS: %d sequential in %.0fs",
                         self.COUNT, seq)

        # ── MIG-T-011 concurrent, back the other way ─────────────────────
        self.logger.info("[MIG-T-011] %d migrations started together",
                         self.COUNT)
        t0 = time.time()
        mids = []
        for v in vols:
            try:
                mid = self.migrate(v["id"], src)
                self.migrate_continue(mid)
                mids.append((v, mid))
            except Exception as exc:                  # noqa: BLE001
                raise AssertionError(
                    f"[MIG-T-011] could not even START a concurrent "
                    f"migration for {v['name']}: {str(exc)[:240]}. The "
                    f"sequential run above passed, so this is contention in "
                    f"the pre-create path, not migration being broken.") from exc

        failures = []
        for v, mid in mids:
            try:
                status, phase = self.await_migration(
                    vol_id=v["id"], migration_id=mid, timeout=1800,
                    what=f"MIG-T-011 concurrent {v['name']}")
                if status not in self.OK_TERMINAL:
                    failures.append((v["name"], status, phase))
            except AssertionError as exc:
                failures.append((v["name"], "stuck", str(exc)[:120]))
        con = time.time() - t0

        if failures:
            raise AssertionError(
                f"[MIG-T-011] {len(failures)} of {self.COUNT} concurrent "
                f"migrations did not complete: {failures}. The same "
                f"{self.COUNT} volumes migrated cleanly one at a time "
                f"({seq:.0f}s), so this is contention between concurrent "
                f"migrations on one source/target pair.")
        for v, _ in mids:
            self.assert_placed_on(v["id"], src, "(MIG-T-011)")
        self.verify(vols[0]["name"], vols[0]["sums"],
                    "after concurrent migrations (MIG-T-011)")
        self.logger.info("[MIG-T-011] PASS: %d concurrent in %.0fs "
                         "(sequential was %.0fs)", self.COUNT, con, seq)
        self.cleanup_migrations()
