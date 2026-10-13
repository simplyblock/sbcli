"""MIG-P: migration at scale and over time. The SEPARATE stress lane.

Run with ``--testname migration-stress``. Not part of ``--testname
migration`` for the same reason the replication lanes are split: a soak
measured in hours must never stand between a correctness run and its
answer.

What these add over the correctness lane, which the original scripts had
only one of (a rebalance soak):

* **MIG-P-001/002 large volumes.** Migration is documented to move only
  front storage, so its duration should be "independent of the size of the
  volume". This case disproves that or confirms it, against a small
  control migrated in the same run -- and it times the ``lvol_migrate``
  freeze, which is the window where the source has stopped serving and
  nothing in the product reports how long it was.
* **MIG-P-003/004 many volumes.** Not four, as the correctness lane does,
  but as many as the lab will carry -- which is where a scheduler that
  serialises quietly, or a per-node migration cap nobody documented, shows
  up.
* **MIG-P-005/006 the soak.** Migrate round and round for hours. Finds the
  leaks a single migration cannot: snapshot accumulation, ``_m`` bdevs that
  are not cleaned, database rows that pile up, a cluster that is slower at
  hour six than hour one.

Every scale number is environment-overridable, because the right value is
a property of the lab rather than of the test.
"""
import os
import time

from e2e_tests.migration.migration_base import (
    MigrationPreconditionError,
    MigrationTestBase,
)
from utils.common_utils import cli_failed, sleep_n_sec


def _env(name, default):
    try:
        return type(default)(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


class MigrationLargeVolume(MigrationTestBase):
    """MIG-P-001, MIG-P-002: a big volume must migrate as fast as a small one.

    This case used to assume the opposite. It provisioned 100G, allowed two
    hours, logged how long the move took and asserted nothing about it --
    "size changes duration, not correctness". The product documents the
    reverse (``docs/architecture/concepts/volume-migration.md``):

        "No back storage is copied during a volume migration. The data of
        the volume and its snapshots remains in place [...] and only the
        front storage moves to the target node. A migration therefore
        completes instantly or within a very short time, independent of the
        size of the volume."

    So duration-by-size is the thing to DISPROVE, and a two-hour budget was
    hiding a hundredfold regression inside a pass. The axis that should
    cost time is the snapshot chain, not the bytes -- MIG-H-007 covers
    that.

    The bound is RELATIVE, against a small control volume migrated in the
    same run on the same cluster, because an absolute "very short" is not
    ours to invent and would be wrong on the next hardware. A control also
    absorbs whatever the lab is doing that day.
    """

    SIZE = os.environ.get("MIG_LARGE_SIZE", "100G")
    BUDGET = _env("MIG_LARGE_BUDGET_SEC", 7200)
    #: How many times the control's duration the large volume may take.
    #: Generous on purpose: the claim is independence, so even 4x is a
    #: clear signal while leaving room for scheduling noise on a volume
    #: that is 50x the size.
    SIZE_RATIO = _env("MIG_LARGE_RATIO", 4.0)
    #: Absolute slack below which a ratio means nothing. Two migrations
    #: that take 3s and 9s are not a 3x regression, they are two fast
    #: migrations and a scheduler.
    SIZE_SLACK_SEC = _env("MIG_LARGE_SLACK_SEC", 60.0)
    #: How long IO runs across the move. NOT the whole BUDGET: the budget
    #: is the deadline for a migration that has gone wrong, and tying fio
    #: to it made a healthy run of this case cost two hours of writing to
    #: prove a move that takes seconds. Long enough to span the migration
    #: with margin; if the move outlasts it, the duration assertion is
    #: what reports that, not a starved fio.
    FIO_SEC = _env("MIG_LARGE_FIO_SEC", 900)
    #: The IO footprint BOTH migrations in this case carry. One constant
    #: rather than a number at each call site, because the comparison is
    #: only valid while the two workloads are identical -- and "control
    #: writes 1G, subject writes 4G" is a workload difference, which dev
    #: says costs migration time through extra snapshot round-trips. It
    #: would reappear as a fake size regression.
    FIO_FOOTPRINT = os.environ.get("MIG_LARGE_FIO_SIZE", "1G")

    def _timed_migration(self, vol, vol_id, tag, under_load=True):
        """Migrate to a drawn target and return (seconds, target).

        Under the SAME load as the case's other migration by default, and
        that is the whole validity of the comparison -- see :meth:`run`.
        """
        handle = None
        if under_load:
            handle = self._run_fio_dual(
                vol, runtime=self.FIO_SEC, rw="randwrite", bs="64K",
                iodepth=8, numjobs=2, size=self.FIO_FOOTPRINT,
                name=f"ctl{self._k8s_normalize_name(vol)[:12]}",
                verify="md5", verify_fatal=True)
            sleep_n_sec(15)       # let IO actually be in flight
        src = self.lvol_node(vol_id)
        # any_target, not pick_target: pick_target's second argument is an
        # OVERLAP SHAPE, and passing a log label there raised "unknown
        # overlap" -- this case died on its first migration. It also wants
        # every shape in the draw, per dev, not just no-overlap.
        tgt = self.any_target(src, f"{tag} target")
        t0 = time.time()
        try:
            self.full_migration(vol_id, tgt, deadline=self.BUDGET,
                                what=f"{tag} {vol}")
            secs = time.time() - t0
        finally:
            if handle is not None:
                self._wait_fio_dual([handle], timeout=self.FIO_SEC + 600)
                self._validate_fio_dual(handle)
        return secs, tgt

    def run(self):
        stamp = int(time.time()) % 100000

        # ── the control, under the SAME load as the large volume ──────────
        #
        # Both volumes carry IO, and that is not incidental. Dev, asked
        # whether migration time should track volume size:
        #
        #     "It should be size-independent but not workload-independent.
        #     Traffic against a migrated volume will force additional
        #     snapshot round-trips, hence, increasing the time for
        #     migration."
        #
        # So an idle control against a loaded 100G volume would compare
        # two different things and fail a correct product: the large one
        # would be slower because it is BUSY, not because it is big, and
        # the case would report a size regression that is not there. Same
        # workload shape on both -- same rw, bs, iodepth, numjobs -- so
        # size is the only variable left, which is the one claim being
        # tested. (fio `size=1G` on both, deliberately: the IO footprint
        # is part of the workload, not of the volume.)
        ctl = f"miglgctl{stamp}"
        ctl_id, ctl_sums = self.make_volume(ctl)
        ctl_secs, _ = self._timed_migration(ctl, ctl_id, "MIG-P-001 control")
        self.verify(ctl, ctl_sums, "after the control migration (MIG-P-001)")
        self.logger.info(
            "[MIG-P-001] control: %s migrated in %.1fs under load. The "
            "large volume carries the same workload, so any difference is "
            "size and nothing else.", self.VOL_SIZE, ctl_secs)

        vol = f"miglg{stamp}"
        self.logger.info("[MIG-P-001] creating a %s volume", self.SIZE)
        try:
            vol_id, sums = self.make_volume(vol, size=self.SIZE)
        except MigrationPreconditionError as exc:
            self.logger.warning(
                "[MIG-P-001] SKIPPED: could not create a %s volume (%s). "
                "This lane needs a cluster that can hold one; set "
                "MIG_LARGE_SIZE to something the lab can carry.",
                self.SIZE, str(exc)[:200])
            return

        src = self.lvol_node(vol_id)
        tgt = self.pick_target(src, "no-overlap")

        # Keep writing throughout, so the final delta is a real delta.
        # verify, and fatally. This is the case most likely to catch the
        # failure the feature's own design doc calls its whole risk -- "a
        # migration that reports Succeeded and lost writes is
        # indistinguishable from a correct one at every level above the
        # bytes", with field evidence of a cutover that froze the source
        # and lost writes silently. Without verify= the k8s arm does no
        # data checking whatever (_run_fio_dual defaults it off), so this
        # wrote 4G across a cutover and checked none of it.
        # Identical to the control's workload in every parameter. See
        # FIO_FOOTPRINT: the only intended difference between the two
        # migrations in this case is the volume's size.
        handle = self._run_fio_dual(vol, runtime=self.FIO_SEC, rw="randwrite",
                                    bs="64K", iodepth=8, numjobs=2,
                                    size=self.FIO_FOOTPRINT, name="miglarge",
                                    verify="md5", verify_fatal=True)

        t0 = time.time()
        mid = self.migrate(vol_id, tgt)
        self.migrate_continue(mid, deadline=self.BUDGET)

        # Time the phases separately. snap_copy scales with history,
        # lvol_migrate is the freeze, and conflating them hides which one
        # is the problem at size.
        phase_at = {}
        last_phase = None
        deadline = time.time() + self.BUDGET
        while time.time() < deadline:
            rec = self.migration_record(vol_id=vol_id, migration_id=mid)
            if not rec:
                break
            status = str(self._get(rec, "status") or "").lower()
            phase = str(self._get(rec, "phase") or "").lower()
            if phase and phase != last_phase:
                phase_at[phase] = time.time()
                self.logger.info("[MIG-P-001] entered %s at +%.0fs", phase,
                                 time.time() - t0)
                last_phase = phase
            if status in self.TERMINAL:
                break
            sleep_n_sec(5)
        total = time.time() - t0

        self.logger.info("[MIG-P-001] RESULT: migrating %s took %.0fs "
                         "(%.1f min)", self.SIZE, total, total / 60)
        if "lvol_migrate" in phase_at:
            freeze_start = phase_at["lvol_migrate"]
            freeze_end = phase_at.get("cleanup_source", t0 + total)
            freeze = freeze_end - freeze_start
            self.logger.info(
                "[MIG-P-002] RESULT: the lvol_migrate FREEZE lasted %.0fs "
                "(%.1f min) on a %s volume. This is the window where the "
                "source has stopped serving and the final delta is "
                "shipping. The design calls it a short freeze; this is the "
                "number to put against that word.", freeze, freeze / 60,
                self.SIZE)
        else:
            self.logger.warning(
                "[MIG-P-002] never observed the lvol_migrate phase, so the "
                "freeze could not be timed. Polling every 5s can miss a "
                "phase that is genuinely short -- which would itself be the "
                "answer.")

        self._wait_fio_dual([handle], timeout=self.FIO_SEC + 600)
        self._validate_fio_dual(handle)
        self.assert_placed_on(vol_id, tgt, "(MIG-P-001)")
        self.verify(vol, sums, f"after migrating {self.SIZE} (MIG-P-002)")

        # ── MIG-P-002 the duration must not scale with the size ──────────
        allowed = ctl_secs * self.SIZE_RATIO + self.SIZE_SLACK_SEC
        self.logger.info(
            "[MIG-P-002] %s took %.1fs; %s control took %.1fs; allowed "
            "%.1fs (%.1fx + %.0fs slack)", self.SIZE, total, self.VOL_SIZE,
            ctl_secs, allowed, self.SIZE_RATIO, self.SIZE_SLACK_SEC)
        if total > allowed:
            raise AssertionError(
                f"[MIG-P-002] migration time scales with volume SIZE: "
                f"{self.SIZE} took {total:.1f}s against {ctl_secs:.1f}s for "
                f"{self.VOL_SIZE} on the same cluster in the same run "
                f"({total / max(ctl_secs, 0.001):.1f}x, allowed "
                f"{self.SIZE_RATIO:.1f}x + {self.SIZE_SLACK_SEC:.0f}s). "
                f"Migration is documented to move only front storage -- "
                f"'completes instantly or within a very short time, "
                f"independent of the size of the volume' -- so a duration "
                f"that tracks the bytes means back storage is being copied, "
                f"or the final delta is not a delta. Neither is visible in "
                f"the migration's own status, which reports Succeeded "
                f"either way. Phase timings: "
                f"{ {k: round(v - t0, 1) for k, v in phase_at.items()} }")
        self.logger.info("[MIG-P-002] PASS: %s migrated in %.1fs, within "
                         "%.1fx of the %s control -- duration does not "
                         "track size", self.SIZE, total, self.SIZE_RATIO,
                         self.VOL_SIZE)
        self.cleanup_migrations()


class MigrationManyVolumes(MigrationTestBase):
    """MIG-P-003, MIG-P-004: as many at once as the lab will carry.

    The correctness lane does four. This does as many as configured, which
    is where the interesting answers are: does the scheduler serialise
    quietly, is there an undocumented per-node cap, and does the Nth
    migration get starved while the first N-1 proceed.
    """

    COUNT = _env("MIG_MANY_COUNT", 20)
    SETTLE = _env("MIG_MANY_SETTLE_SEC", 3600)

    def run(self):
        stamp = int(time.time()) % 100000
        vols = []
        for i in range(self.COUNT):
            name = f"migmany{stamp}n{i}"
            try:
                vid, sums = self.make_volume(name, size="1G",
                                             seed=(i == 0))
            except MigrationPreconditionError as exc:
                raise MigrationPreconditionError(
                    f"[MIG-P-003] the lab ran out at volume {i + 1} of "
                    f"{self.COUNT}: {str(exc)[:200]}. Lower MIG_MANY_COUNT.")
            vols.append({"name": name, "id": vid, "sums": sums})

        src = self.lvol_node(vols[0]["id"])
        tgt = self.pick_target(src, "no-overlap")
        self.logger.info("[MIG-P-003] starting %d migrations %s -> %s",
                         self.COUNT, src, tgt)

        t0 = time.time()
        started, refused = [], []
        for v in vols:
            try:
                mid = self.migrate(v["id"], tgt, retries=2, retry_interval=5)
                self.migrate_continue(mid)
                started.append((v, mid))
            except Exception as exc:                  # noqa: BLE001
                refused.append((v["name"], str(exc)[:160]))
        self.logger.info("[MIG-P-003] %d started, %d refused in %.0fs",
                         len(started), len(refused), time.time() - t0)
        if refused:
            self.logger.warning(
                "[MIG-P-003] %d migration(s) could not be started while %d "
                "were in flight: %s. If this is a deliberate cap it should "
                "be documented and the error should say so; if it is "
                "contention it is a bug.", len(refused), len(started),
                refused[:4])

        done, failed = [], []
        for v, mid in started:
            try:
                status, phase = self.await_migration(
                    vol_id=v["id"], migration_id=mid, timeout=self.SETTLE,
                    what=f"MIG-P-004 {v['name']}")
                (done if status in self.OK_TERMINAL else failed).append(
                    (v["name"], status, phase))
            except AssertionError as exc:
                failed.append((v["name"], "stuck", str(exc)[:100]))
        elapsed = time.time() - t0

        self.logger.info("[MIG-P-004] %d/%d completed in %.0fs (%.1f min)",
                         len(done), len(started), elapsed, elapsed / 60)
        if failed:
            raise AssertionError(
                f"[MIG-P-004] {len(failed)} of {len(started)} concurrent "
                f"migrations did not complete: {failed[:6]}. The correctness "
                f"lane migrates four of these cleanly, so the variable here "
                f"is concurrency, not migration.")
        off = [v["name"] for v, _ in started
               if self.lvol_node(v["id"]) != tgt]
        if off:
            raise AssertionError(
                f"[MIG-P-004] these report success but are not on the "
                f"target: {off}")
        self.verify(vols[0]["name"], vols[0]["sums"],
                    f"after {len(started)} concurrent migrations (MIG-P-004)")
        self.logger.info("[MIG-P-004] PASS")
        self.cleanup_migrations()


class MigrationSoak(MigrationTestBase):
    """MIG-P-005, MIG-P-006: migrate at random for hours.

    The leak detector. A single migration cannot show you that each one
    leaves a snapshot behind, or a ``_m`` bdev, or a database row -- those
    only become visible after fifty. The original scripts had a rebalance
    soak; this is the per-volume equivalent, and it measures whether the
    hundredth migration is as fast as the first.

    RANDOMISED, and that is the point of it. This case used to move one
    volume back and forth between the same two nodes --
    ``next((n for n in nodes if n != here))`` picks the first node that is
    not the current one, so a six-node cluster ping-ponged between two of
    them for six hours. Dev's objection to exactly that, about their own
    migration script:

        "They are in random order because it's the only meaningful thing
        to find bugs. [...] If they run in a specific order over and over
        again, it just means that this specific order works."

    So each hop now draws the volume, draws a target from every node the
    volume is not on, and takes a snapshot first with probability
    MIG_SNAPSHOT_CHANCE -- which is dev's ``--snapshot-chance``, default
    and all. The seed is logged by the base class and replaying a run is
    ``MIG_SEED=<n>``.

    A snapshot taken here is also checked to still exist afterwards, on
    dev's rule that "a migration that drops its snapshots is a failure".
    """

    HOURS = _env("MIG_SOAK_HOURS", 6.0)
    #: How many volumes take part -- a RANGE, drawn per run from the seed.
    #:
    #: Dev, asked whether a fixed 3 was enough:
    #:
    #:     "I ran many different runs, with many different numbers of
    #:     volumes. I had up to 60 volumes for Sadegh. Ideally, the number
    #:     is a randomized amount of volumes. Some sharing a subsystem,
    #:     some not. The seeded randomizer is important (also for volume
    #:     numbers and volume assignments). 3 volumes are barely a lot for
    #:     a subsystem, no talking about the fully loaded test."
    #:
    #: So the count is itself drawn, not configured: a fixed number means
    #: every run of this case rehearses one fleet size, and the subsystem
    #: packing that goes with it. ``MIG_SOAK_VOLUMES`` takes "LOW-HIGH" to
    #: set the range, or a bare integer to pin it when reproducing.
    #: Default 6-18: above the "barely a lot" line for a subsystem, and
    #: still enough hops per volume to find an accumulation bug inside one
    #: time budget. Raise it to 60 for a fully loaded run.
    VOLUMES = os.environ.get("MIG_SOAK_VOLUMES", "6-18")
    #: Fraction of the volumes created into a SHARED subsystem, so one run
    #: carries both kinds. Also dev's: "Some sharing a subsystem, some
    #: not." A run of all-shared or all-private volumes tests one of the
    #: two code paths and silently skips the interaction between them --
    #: and it is the interaction that moves a whole group when one member
    #: is named.
    SHARED_FRACTION = _env("MIG_SOAK_SHARED_FRACTION", 0.5)
    #: Probability of snapshotting a volume before migrating it. Dev's
    #: SNAPSHOT_CHANCE, same default: it makes snap_copy do real work on
    #: some hops and not others, which is the mix a cluster actually sees.
    SNAPSHOT_CHANCE = _env("MIG_SNAPSHOT_CHANCE", 0.15)

    def _volume_count(self):
        """How many volumes this run uses. Drawn, unless pinned."""
        raw = str(self.VOLUMES).strip()
        if "-" in raw:
            lo, _, hi = raw.partition("-")
            try:
                lo, hi = int(lo), int(hi)
            except ValueError:
                raise MigrationPreconditionError(
                    f"[MIG-P-005] MIG_SOAK_VOLUMES={raw!r} is not a range "
                    f"'LOW-HIGH' or an integer") from None
            if lo > hi:
                lo, hi = hi, lo
            n = self.rng.randint(max(1, lo), max(1, hi))
            self.logger.info(
                "[MIG-P-005] drew %d volume(s) from the range %d-%d; pin "
                "this run with %s=%s", n, lo, hi, self.SEED_ENV, self.rng_seed)
            return n
        try:
            return max(1, int(raw))
        except ValueError:
            raise MigrationPreconditionError(
                f"[MIG-P-005] MIG_SOAK_VOLUMES={raw!r} is neither a range "
                f"nor an integer") from None

    def _make_shared_group(self, stamp, n):
        """n volumes on ONE subsystem, on either backend.

        The two backends share a subsystem by different means and neither
        is a flag on the volume:

        * docker -- ``volume add --namespaced true``, with
          ``--max-namespace-per-subsys`` raised on the first member so the
          rest have slots to land in.
        * k8s -- a StorageClass whose ``max_namespace_per_subsys`` leaves
          room, which is how the operator's own migration test builds its
          namespaced volumes. The volumes are PVCs because a k8s move is
          addressed by PV name.

        Raises rather than returning a partial group: a "shared" set whose
        members each got their own subsystem would be indistinguishable
        from the private volumes beside it, and the run would report
        covering the group path while covering nothing.
        """
        names = [f"migsoak{stamp}s{i}" for i in range(n)]
        out = []

        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            sc = f"migsoak-shared-{stamp}"[:63]
            k8s.create_storage_class(
                name=sc, cluster_id=self.cluster_id,
                pool_name=self.pool_name, ndcs=self.ndcs, npcs=self.npcs,
                max_namespace_per_subsys=n + 2)
            self._extra_scs.append(sc)
            prev = self._k8s_storage_class_name
            self._k8s_storage_class_name = sc
            try:
                host = None
                for name in names:
                    # Pinned after the first: a subsystem belongs to a
                    # node, so max_namespace_per_subsys cannot pack
                    # volumes the control plane put on different ones.
                    _, vid = self._create_lvol_dual(name, self.VOL_SIZE,
                                                    host_id=host)
                    self._mig_vols.append(name)
                    out.append({"name": name, "id": vid,
                                "sums": self.seed(name)})
                    if host is None:
                        host = self.lvol_node(vid)
            finally:
                self._k8s_storage_class_name = prev
        else:
            host = None
            for i, name in enumerate(names):
                extra = (f" --namespaced true --max-namespace-per-subsys {n + 2}"
                         if i == 0 else " --namespaced true")
                if host:
                    extra += f" --host-id {host}"
                cmd = (f"{self.base_cmd} -d volume add {name} "
                       f"{self.VOL_SIZE} {self.pool_name}{extra} 2>&1")
                o, e = self._cli(cmd)
                if cli_failed(o, e):
                    raise MigrationPreconditionError(
                        f"[MIG-P-005] could not add {name} to a shared "
                        f"subsystem: {(o + e)[:240]}")
                vid = self.sbcli_utils.get_lvol_id(lvol_name=name)
                self._mig_vols.append(name)
                out.append({"name": name, "id": vid,
                            "sums": self.seed(name)})
                if host is None:
                    host = self.lvol_node(vid)

        # Unpin before anything tries to move them: the pin is what put
        # them on one node, and a pinned volume cannot be migrated.
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            for v in out:
                pvc = (self._volume_registry.get(v["name"]) or {}).get(
                    "pvc_name")
                if pvc:
                    k8s.unpin_pvc(pvc)

        nqns = {self.lvol_nqn(v["id"]) for v in out}
        if len(nqns) != 1 or None in nqns:
            raise MigrationPreconditionError(
                f"[MIG-P-005] the {n} volumes meant to share a subsystem "
                f"landed on {len(nqns)}: {nqns}. max_namespace_per_subsys "
                f"raises a ceiling, it does not pull a volume into a "
                f"subsystem another pool member already filled -- so on a "
                f"pool with other volumes this can spread. The shared half "
                f"of this run would otherwise be indistinguishable from the "
                f"private half.")
        self.logger.info("[MIG-P-005] %d volume(s) share subsystem %s",
                         n, next(iter(nqns)))
        return out

    def run(self):
        stamp = int(time.time()) % 100000
        nodes = [n.get("uuid") or n.get("id") for n in self.online_nodes()]
        if len(nodes) < 2:
            self.logger.warning("[MIG-P-005] SKIPPED: needs two online nodes.")
            return

        # A drawn count, and a MIX of shared and private subsystems in
        # the same run -- both on dev's instruction (see VOLUMES). The
        # shared ones go through the batch group builder, which is what
        # puts several volumes on one subsystem; the rest are ordinary
        # single-namespace volumes. One run therefore exercises the
        # whole-group move and the single move, and the interaction
        # between them, which is where naming one member of a group and
        # watching the others follow actually gets tested.
        count = self._volume_count()
        n_shared = round(count * max(0.0, min(1.0, self.SHARED_FRACTION)))
        n_shared = min(n_shared, count)
        vols = []
        if n_shared >= 2:
            try:
                group = self._make_shared_group(stamp, n_shared)
                vols.extend(group)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning(
                    "[MIG-P-005] could not build a shared subsystem of %d "
                    "(%s); falling back to all-private volumes, so this run "
                    "does NOT cover the group path", n_shared, str(exc)[:200])
                n_shared = 0
        elif n_shared == 1:
            # One volume cannot share a subsystem with itself.
            n_shared = 0
        for i in range(count - n_shared):
            name = f"migsoak{stamp}p{i}"
            vid, sums = self.make_volume(name)
            vols.append({"name": name, "id": vid, "sums": sums})

        by_nqn = {}
        for v in vols:
            by_nqn.setdefault(self.lvol_nqn(v["id"]), []).append(v["name"])
        self.logger.info(
            "[MIG-P-005] %d volume(s) across %d subsystem(s): %s",
            len(vols), len(by_nqn),
            {k: len(v) for k, v in by_nqn.items()})

        total = int(self.HOURS * 3600)
        started_at = time.time()
        deadline = started_at + total
        hops, durations, snap_counts = 0, [], []
        #: Snapshots this case took, so the growth assertion below can tell
        #: them from snapshots a migration left behind. Without this the
        #: snapshot-leak check fires on our own snapshots.
        taken = []

        self.logger.info(
            "[MIG-P-005] soaking for %.1fh: %d volume(s) over %d nodes, "
            "random target each hop, %.0f%% snapshot chance",
            self.HOURS, len(vols), len(nodes), self.SNAPSHOT_CHANCE * 100)
        while time.time() < deadline:
            v = self.pick(vols, "soak volume")
            here = self.lvol_node(v["id"])
            elsewhere = [n for n in nodes if n != here]
            if not elsewhere:
                break
            # Every other node is a candidate, and the HA relationship is
            # named in the log. pick_target's "no-overlap" would exclude
            # the source's secondary and tertiary, which dev says are
            # exactly what a random run has to hit.
            there = self.pick(elsewhere, f"soak target for {v['name']}")
            self.logger.info("[MIG-P-005] hop %d: %s %s -> %s (%s)",
                             hops + 1, v["name"], here, there,
                             self.ha_shape(here, there))

            # Snapshot first, sometimes. Drawn from the same stream, so a
            # replayed seed takes them on the same hops.
            snap_name = None
            if self.rng.random() < self.SNAPSHOT_CHANCE:
                snap_name = f"{v['name']}s{hops}"
                try:
                    self.make_snapshot(v["name"], snap_name)
                    taken.append(snap_name)
                    self.logger.info(
                        "[MIG-P-005] hop %d: snapshotted %s first, so this "
                        "migration has to carry it", hops + 1, v["name"])
                except Exception as exc:              # noqa: BLE001
                    self.logger.warning(
                        "[MIG-P-005] could not snapshot %s before hop %d: "
                        "%s; migrating without one", v["name"], hops + 1,
                        str(exc)[:160])
                    snap_name = None

            t0 = time.time()
            try:
                self.full_migration(
                    v["id"], there,
                    what=f"MIG-P-005 hop {hops + 1} {v['name']} -> {there}")
            except AssertionError as exc:
                raise AssertionError(
                    f"[MIG-P-005] hop {hops + 1} ({v['name']} {here} -> "
                    f"{there}) failed after "
                    f"{(time.time() - started_at) / 3600:.1f}h of soaking: "
                    f"{str(exc)[:240]}. Earlier hops succeeded, so something "
                    f"accumulated. Replay this run with "
                    f"{self.SEED_ENV}={self.rng_seed}.") from exc
            durations.append(time.time() - t0)
            hops += 1

            # Dev's rule: "a migration that drops its snapshots is a
            # failure." Checked by name rather than by count, because a
            # count can be right while the wrong snapshot survived.
            if snap_name and not self.snap_id(snap_name):
                raise AssertionError(
                    f"[MIG-P-005] the snapshot {snap_name} taken just before "
                    f"hop {hops} is gone after the migration. A migration "
                    f"carries the volume's whole lineage; dropping a user "
                    f"snapshot silently loses the only copy of that point in "
                    f"time. Replay with {self.SEED_ENV}={self.rng_seed}.")

            data = self._cli_json(f"{self.base_cmd} snapshot list")
            rows = (data.get("results", data) if isinstance(data, dict)
                    else data) or []
            snap_counts.append(len(rows))

            if hops % 5 == 0:
                self.logger.info(
                    "[MIG-P-005] %d hops, %.1fh elapsed, last 5 durations "
                    "%s, snapshots %s (%d taken by this case)", hops,
                    (time.time() - started_at) / 3600,
                    [round(d) for d in durations[-5:]], snap_counts[-5:],
                    len(taken))
                for w in vols:
                    self.verify(w["name"], w["sums"],
                                f"at hop {hops} (MIG-P-005)")

        self.logger.info("[MIG-P-005] soak finished: %d hops over %.1fh "
                         "(seed %s)", hops, self.HOURS, self.rng_seed)
        if hops < 2:
            self.logger.warning(
                "[MIG-P-005] only %d hop(s) completed in %.1fh -- each "
                "migration is slower than expected, which is itself worth "
                "reporting.", hops, self.HOURS)
            self.cleanup_migrations()
            return

        # ── MIG-P-006 nothing accumulated ────────────────────────────────
        if len(durations) >= 6:
            first = sum(durations[:3]) / 3
            last = sum(durations[-3:]) / 3
            self.logger.info("[MIG-P-006] mean duration: first 3 hops %.0fs, "
                             "last 3 hops %.0fs", first, last)
            if last > first * 2.5 and last - first > 60:
                raise AssertionError(
                    f"[MIG-P-006] migrations got materially slower over the "
                    f"soak: {first:.0f}s -> {last:.0f}s across {hops} hops "
                    f"with an unchanged volume. Something each migration "
                    f"leaves behind is being walked by the next one. "
                    f"durations={[round(d) for d in durations]}")
        # Allow for the snapshots this case took on purpose. Without that
        # term the check fires on our own MIG_SNAPSHOT_CHANCE snapshots and
        # reports a product leak that is really the test's doing.
        allowed = snap_counts[0] + len(taken)
        if len(snap_counts) >= 4 and snap_counts[-1] > allowed:
            raise AssertionError(
                f"[MIG-P-006] snapshot count grew from {snap_counts[0]} to "
                f"{snap_counts[-1]} across {hops} migrations, and this case "
                f"only took {len(taken)} snapshot(s) ({taken}). The "
                f"{snap_counts[-1] - allowed} extra are intermediate "
                f"snapshots migrations left behind; at this rate the cluster "
                f"fills up on migration traffic alone. counts={snap_counts}. "
                f"Replay with {self.SEED_ENV}={self.rng_seed}.")

        for w in vols:
            self.verify(w["name"], w["sums"],
                        f"after {hops} migrations (MIG-P-006)")
        self.logger.info("[MIG-P-006] PASS: %d hops over %d volume(s), no "
                         "drift, data intact (seed %s)", hops, len(vols),
                         self.rng_seed)
        self.cleanup_migrations()
