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
    MigrationPreconditionError,
    MigrationTestBase,
)
from utils.common_utils import cli_failed


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
            # These land in the PARENT's subsystem, not their own:
            # --namespaced defaults to true on snapshot clone and
            # cannot be set false from the CLI (SFAM-2819). The cases
            # below detect that rather than assume either way.
            out, err = self._cli(f"{self.base_cmd} -d snapshot clone "
                                 f"{snap_id} {cn} 2>&1")
            if cli_failed(out, err):
                raise MigrationPreconditionError(
                    f"[MIG-T-006] could not clone {snap}: {(out + err)[:240]}")
            self._mig_vols.append(cn)
            clones.append((cn, self.sbcli_utils.get_lvol_id(lvol_name=cn)))
        tgt = self.pick_target(src, "no-overlap")

        # ── MIG-T-006 migrate the parent while clones exist ────────────
        # Whether the clones share the parent's subsystem is the product's
        # choice, not ours: snapshot clone --namespaced defaults to true and
        # cannot be turned off from the CLI (SFAM-2819). So ask the cluster,
        # and assert whichever contract actually applies.
        shared = any(self.shares_subsystem(vol_id, cid) for _, cid in clones)
        self.logger.info("[MIG-T-006] parent nqn=%s; clones %s the subsystem",
                         self.lvol_nqn(vol_id),
                         "SHARE" if shared else "do not share")

        if shared:
            # Moving one member of a shared subsystem must be refused, and
            # the refusal must say how to do it properly. Accepting it would
            # split one subsystem across two nodes.
            out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                                 f"{vol_id} {tgt} 2>&1")
            combined = out + err
            if self.MIG_ID_RE.search(combined):
                raise AssertionError(
                    f"[MIG-T-006] a single-volume migrate was ACCEPTED for "
                    f"{vol}, which shares a subsystem with its clones. That "
                    f"moves one namespace and leaves the others behind, "
                    f"splitting one subsystem across two nodes. It should be "
                    f"refused with a pointer to --batch. Output: "
                    f"{combined[:300]}")
            if "--batch" not in combined:
                raise AssertionError(
                    f"[MIG-T-006] the single-volume migrate was refused, "
                    f"which is right, but the message does not tell the "
                    f"operator to use --batch: {combined[:300]}")
            self.logger.info("[MIG-T-006] single-volume migrate correctly "
                             "refused, pointing at --batch")
            self.full_migration(vol_id, tgt, batch=True,
                                what="MIG-T-006 parent and clones as a group")
            for cn, cid in clones:
                self.assert_placed_on(cid, tgt, f"({cn}, MIG-T-006 batch)")
        else:
            self.full_migration(vol_id, tgt,
                                what="MIG-T-006 parent with clones")
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
        self.logger.info("[MIG-T-006] PASS")

        # ── MIG-T-007 a clone on its own ───────────────────────────────
        cn, cid = clones[0]
        if shared:
            # The same rule from the other side. There is no "move one clone"
            # while the subsystem is shared, and that refusal is the thing
            # worth pinning down rather than working around.
            cl_tgt = self.pick_target(self.lvol_node(cid), "no-overlap")
            out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                                 f"{cid} {cl_tgt} 2>&1")
            if self.MIG_ID_RE.search(out + err):
                raise AssertionError(
                    f"[MIG-T-007] migrating clone {cn} alone was ACCEPTED "
                    f"while it shares a subsystem with its parent; that "
                    f"splits the subsystem across nodes.")
            self.logger.info("[MIG-T-007] PASS: a lone clone is refused while "
                             "the subsystem is shared")
        else:
            cl_src = self.lvol_node(cid)
            cl_tgt = self.pick_target(cl_src, "no-overlap")
            self.logger.info("[MIG-T-007] migrating clone %s away from its "
                             "parent", cn)
            self.full_migration(cid, cl_tgt, what="MIG-T-007 clone alone")
            self.assert_placed_on(cid, cl_tgt, "(MIG-T-007)")
            if self.lvol_node(vol_id) != tgt:
                raise AssertionError(
                    "[MIG-T-007] migrating a clone moved its PARENT as well. "
                    "The two are separate volumes; only the one named should "
                    "move.")
            self.logger.info("[MIG-T-007] PASS: clone moved alone")

        # ── MIG-T-008 the tree comes home ──────────────────────────────
        self.logger.info("[MIG-T-008] bringing the parent back to %s", src)
        self.full_migration(vol_id, src, batch=shared,
                            what="MIG-T-008 tree round trip")
        self.assert_placed_on(vol_id, src, "(MIG-T-008)")
        self.verify(vol, sums, "after the tree round trip (MIG-T-008)")
        self.assert_no_target_leftovers(
            tgt, vol_id, "after the tree left it again (MIG-T-008)")
        self.logger.info("[MIG-T-008] PASS: no stale tree state left behind")

        # ── MIG-T-009 snapshots still usable after all that ────────────
        newclone = f"migtpost{stamp}"
        out, err = self._cli(f"{self.base_cmd} -d snapshot clone {snap_id} "
                             f"{newclone} 2>&1")
        if cli_failed(out, err):
            raise AssertionError(
                f"[MIG-T-009] the snapshot cannot be cloned after its volume "
                f"was migrated twice: {(out + err)[:300]}. The chain has "
                f"been left pointing at something that moved.")
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
        # The scheduler spreads these four across nodes; they do NOT all land
        # together. Taking the target from vols[0] alone and then moving all
        # four to it meant one of them was already there, and the product
        # correctly refused: "LVol ... is already on node ...; cannot migrate
        # to the same node". Record where each one actually is, and skip any
        # that is already on the target rather than asking for a no-op.
        for v in vols:
            v["start"] = self.lvol_node(v["id"])
        src = vols[0]["start"]
        tgt = self.pick_target(src, "no-overlap")
        self.logger.info("[MIG-T] starting placement: %s; target %s",
                         {v["name"]: v["start"] for v in vols}, tgt)

        # ── MIG-T-010 sequential, as the control ─────────────────────────
        movable = [v for v in vols if v["start"] != tgt]
        if len(movable) < len(vols):
            self.logger.info(
                "[MIG-T-010] %d of %d volume(s) were already on the target "
                "and are not migrated: moving a volume to the node it is "
                "already on is refused, correctly.",
                len(vols) - len(movable), len(vols))
        self.logger.info("[MIG-T-010] %d migrations one after another",
                         len(movable))
        t0 = time.time()
        for v in movable:
            self.full_migration(v["id"], tgt,
                                what=f"MIG-T-010 sequential {v['name']}")
            self.assert_placed_on(v["id"], tgt, "(MIG-T-010)")
        seq = time.time() - t0
        self.logger.info("[MIG-T-010] PASS: %d sequential in %.0fs",
                         len(movable), seq)

        # ── MIG-T-011 concurrent, back the other way ─────────────────────
        # Same rule in reverse: only volumes not already on the source.
        # Placement is read again rather than inferred from the loop
        # above, so a volume the product moved for its own reasons is
        # still handled correctly.
        back = [v for v in vols if self.lvol_node(v["id"]) != src]
        if not back:
            self.logger.warning(
                "[MIG-T-011] SKIPPED: every volume is already on %s, so "
                "there is no concurrent migration to start.", src)
            self.cleanup_migrations()
            return
        self.logger.info("[MIG-T-011] %d migrations started together",
                         len(back))
        t0 = time.time()
        mids = []
        for v in back:
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
                f"[MIG-T-011] {len(failures)} of {len(back)} concurrent "
                f"migrations did not complete: {failures}. The same "
                f"{len(movable)} volume(s) migrated cleanly one at a time "
                f"({seq:.0f}s), so this is contention between concurrent "
                f"migrations on one source/target pair.")
        for v, _ in mids:
            self.assert_placed_on(v["id"], src, "(MIG-T-011)")
        self.verify(vols[0]["name"], vols[0]["sums"],
                    "after concurrent migrations (MIG-T-011)")
        self.logger.info("[MIG-T-011] PASS: %d concurrent in %.0fs "
                         "(sequential was %.0fs)", len(back), con, seq)
        self.cleanup_migrations()
