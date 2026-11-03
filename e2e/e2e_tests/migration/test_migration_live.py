"""MIG-L: live migration -- the volume moves while clients are writing to it.

The rest of the lane migrates volumes that are idle at the moment of the
cutover. MIG-H-001 is the one exception and it loads a single volume; nothing
loads a *group*, and nothing measures how long IO actually paused.

That gap matters because a live migration is the only kind anyone performs. A
volume with no incoming IO is a different operation with a different risk
profile -- as the 2026-10-21 failure showed, it is not even the same code path
(an idle volume's data has all shifted into the intermediate snapshot, so the
head has nothing left to transfer). Passing on idle volumes says very little
about the feature as it is used.

Three cases, cheapest first:

    MIG-L-001  one volume, loaded across the cutover, stall measured
    MIG-L-002  LIVE_VOLS_PER_NODE volumes loaded on the source AND on the
               target, one of them migrated -- so a bystander on either end
               being disturbed is a finding, not invisible
    MIG-L-003  a shared-namespace GROUP, every member seeded and loaded,
               migrated as a batch

What these assert that the existing cases do not:

* fio ran to completion. ``_wait_fio_dual`` only warns when a k8s Job did not
  succeed and ``_validate_fio_dual`` is a no-op on docker unless the caller
  passes ``log_path``, which the lane's callers did not -- so a run where fio
  failed outright could still log "IO survived the cutover".
* the data is right, via md5 with ``verify_fatal``. Without ``verify`` a k8s
  fio job checks nothing at all; it only moves IO.
* how long the longest single IO took, which IS the client-visible cutover
  gap: an IO issued before the cutover and completed after it carries the
  whole pause. The number is logged whether or not it passes, because
  "IO paused for N ms" is what belongs in a bug report.

The machinery lives in MigrationTestBase.load_across / assert_io_unbroken so
the other classes in this lane can adopt it without copying anything.
"""
import time

from e2e_tests.migration.migration_base import (
    MigrationPreconditionError,
    MigrationTestBase,
)
from logger_config import setup_logger


class MigrationLiveSingle(MigrationTestBase):
    """MIG-L-001: one volume, migrated while fio writes to it."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"migl1{stamp}"
        vol_id, sums = self.make_volume(vol)
        src = self.lvol_node(vol_id)
        if not src:
            raise MigrationPreconditionError(
                f"[MIG-L-001] cannot read the node hosting {vol}; without it "
                f"there is nothing to assert placement against.")
        tgt = self.pick_target(src, "no-overlap")
        self.logger.info("[MIG-L-001] %s: %s -> %s, under load", vol, src, tgt)

        with self.load_across([vol], context="(MIG-L-001)"):
            _, status, phase = self.full_migration(
                vol_id, tgt, what="MIG-L-001 live migration")
            if status not in self.OK_TERMINAL:
                raise AssertionError(
                    f"[MIG-L-001] the migration ended {status!r} in phase "
                    f"{phase!r} with IO in flight and no fault injected. "
                    f"Whether it also broke IO is reported separately below.")

        self.assert_placed_on(vol_id, tgt, "after a live migration (MIG-L-001)")
        self.verify(vol, sums, "after a live migration (MIG-L-001)")
        self.watch_io(vol, context="(MIG-L-001, post-cutover)")
        self.logger.info("[MIG-L-001] PASS: moved under load, IO unbroken")


class MigrationLiveBystanders(MigrationTestBase):
    """MIG-L-002: the migrated volume is not the only one taking IO.

    How the feature is used, and how it is exercised by hand: every node
    carries several active volumes, so a migration has to leave the ones it
    is not moving alone. A cutover that freezes a bystander on the same
    lvstore, or on the target, is a real outage for that volume and no case
    in this lane would see it -- they all migrate the only loaded volume on
    the cluster.

    Volumes are placed on BOTH ends on purpose. A target-side bystander is
    the one most likely to be disturbed, because the target is where the
    transfer lands and where the subsystem is built.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)

    def run(self):
        stamp = int(time.time()) % 100000
        per_node = max(1, self.LIVE_VOLS_PER_NODE)

        subject = f"migl2{stamp}s"
        subject_id, subject_sums = self.make_volume(subject)
        src = self.lvol_node(subject_id)
        if not src:
            raise MigrationPreconditionError(
                f"[MIG-L-002] cannot read the node hosting {subject}.")
        tgt = self.pick_target(src, "no-overlap")

        # Bystanders, pinned: unpinned they land wherever the control plane
        # likes and the case stops being about the source and the target.
        bystanders = []
        for end, node in (("src", src), ("tgt", tgt)):
            for i in range(per_node - 1 if end == "src" else per_node):
                name = f"migl2{stamp}{end}{i}"
                try:
                    vid, sums = self.make_volume(name, host_id=node)
                except Exception as exc:               # noqa: BLE001
                    self.logger.warning(
                        "[MIG-L-002] could not place a bystander on %s (%s); "
                        "continuing with %d", node, exc, len(bystanders))
                    continue
                bystanders.append({"name": name, "id": vid, "sums": sums,
                                   "node": node, "end": end})

        if not bystanders:
            raise MigrationPreconditionError(
                "[MIG-L-002] no bystander volume could be placed, so there is "
                "nothing for this case to observe. It needs at least one "
                "volume on the source or the target besides the subject.")

        loaded = [subject] + [b["name"] for b in bystanders]
        self.logger.info(
            "[MIG-L-002] migrating %s (%s -> %s) with %d volume(s) under "
            "load: %s", subject, src, tgt, len(loaded), ", ".join(loaded))

        with self.load_across(loaded, context="(MIG-L-002)"):
            _, status, phase = self.full_migration(
                subject_id, tgt, what="MIG-L-002 live migration, bystanders")
            if status not in self.OK_TERMINAL:
                raise AssertionError(
                    f"[MIG-L-002] the migration of {subject} ended {status!r} "
                    f"in phase {phase!r} while {len(loaded)} volumes were "
                    f"taking IO.")

        self.assert_placed_on(subject_id, tgt, "(MIG-L-002 subject)")
        self.verify(subject, subject_sums, "the migrated volume (MIG-L-002)")

        # The bystanders must be untouched -- same data, same node.
        moved = []
        for b in bystanders:
            self.verify(b["name"], b["sums"],
                        f"a bystander on the {b['end']} (MIG-L-002)")
            now = self.lvol_node(b["id"])
            if now and b["node"] and now != b["node"]:
                moved.append(f"{b['name']} {b['node']} -> {now}")
        if moved:
            raise AssertionError(
                f"[MIG-L-002] volumes nobody asked to move have moved: "
                f"{', '.join(moved)}. A migration must touch only its "
                f"subject.")
        self.logger.info(
            "[MIG-L-002] PASS: subject moved, %d bystander(s) undisturbed",
            len(bystanders))


class MigrationLiveGroup(MigrationTestBase):
    """MIG-L-003: a shared-namespace group migrated with every member loaded.

    MIG-B proves a group moves as one unit; it does so with one member seeded
    and none of them taking IO. A batch cutover switches a whole subsystem at
    once, so if any part of it is going to interrupt clients this is where it
    shows, and it is the case the batch lane cannot see.

    Members are built the way MIG-B builds them -- ``--namespaced`` into one
    subsystem on one node -- but every member is seeded here, because an
    unseeded member cannot be mounted and so cannot take IO.
    """

    MEMBERS = 3

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._group = []

    def run(self):
        stamp = int(time.time()) % 100000
        group, host = self._make_loaded_group(f"migl3{stamp}")
        lead = group[0]
        src = host or self.lvol_node(lead["id"])
        if not src:
            raise MigrationPreconditionError(
                "[MIG-L-003] cannot read the node the group landed on.")
        tgt = self.pick_target(src, "no-overlap")

        names = [m["name"] for m in group]
        self.logger.info(
            "[MIG-L-003] migrating a group of %d as one unit, %s -> %s, all "
            "under load: %s", len(group), src, tgt, ", ".join(names))

        with self.load_across(names, context="(MIG-L-003)"):
            mid = self.migrate(lead["id"], tgt, batch=True)
            self.migrate_continue(mid, batch=True)
            status, phase = self.await_batch(
                mid, timeout=1800, what="MIG-L-003 live group")
            if status not in self.OK_TERMINAL:
                raise AssertionError(
                    f"[MIG-L-003] the group migration ended {status!r} in "
                    f"phase {phase!r} with every member taking IO. A batch is "
                    f"all-or-nothing, so a partial move leaves the subsystem "
                    f"addressable from neither node.")

        stranded = [m["name"] for m in group
                    if self.lvol_node(m["id"]) not in (tgt, None)]
        if stranded:
            raise AssertionError(
                f"[MIG-L-003] the group reported success but these members "
                f"are not on {tgt}: {', '.join(stranded)}")
        for m in group:
            self.verify(m["name"], m["sums"], "a group member (MIG-L-003)")
        self.logger.info("[MIG-L-003] PASS: %d-member group moved under load",
                         len(group))

    def _make_loaded_group(self, tag):
        """N volumes in one subsystem, every one seeded so it can take IO.

        Not _BatchBase._make_group: that seeds only the lead, by design, and
        an unseeded volume has no mount for fio to write to.
        """
        if self.k8s_test:
            return self._make_loaded_group_k8s(tag)
        group, host = [], None
        for i in range(self.MEMBERS):
            name = f"{tag}n{i}"
            extra = (f" --namespaced true "
                     f"--max-namespace-per-subsys {self.MEMBERS + 2}"
                     if i == 0 else " --namespaced true")
            if host:
                extra += f" --host-id {host}"
            out, err = self._cli(
                f"{self.base_cmd} -d volume add {name} {self.VOL_SIZE} "
                f"{self.pool_name}{extra} 2>&1")
            from utils.common_utils import cli_failed
            if cli_failed(out, err):
                raise MigrationPreconditionError(
                    f"[MIG-L-003] could not add {name} as a namespace on a "
                    f"shared subsystem: {self.detail(out + err)}")
            vid = self.sbcli_utils.get_lvol_id(lvol_name=name)
            self._mig_vols.append(name)
            group.append({"name": name, "id": vid, "sums": self.seed(name)})
            if host is None:
                host = self.lvol_node(vid)
        self._group = group
        return group, host

    def _make_loaded_group_k8s(self, tag):
        """The k8s shape: one StorageClass whose max_namespace_per_subsys
        leaves room, every PVC from it, every one seeded."""
        k8s = self._ensure_k8s_utils()
        sc = f"migl3-{tag}"[:63]
        k8s.create_storage_class(
            name=sc, cluster_id=self.cluster_id, pool_name=self.pool_name,
            ndcs=self.ndcs, npcs=self.npcs,
            max_namespace_per_subsys=self.MEMBERS + 2)
        self._extra_scs.append(sc)      # initialised and torn down by the base

        prev = getattr(self, "_k8s_storage_class_name", None)
        self._k8s_storage_class_name = sc
        try:
            group, host = [], None
            for i in range(self.MEMBERS):
                name = f"{tag}n{i}"
                vid, sums = self.make_volume(name, host_id=host)
                group.append({"name": name, "id": vid, "sums": sums})
                if host is None:
                    host = self.lvol_node(vid)
        finally:
            self._k8s_storage_class_name = prev
        self._group = group
        return group, host
