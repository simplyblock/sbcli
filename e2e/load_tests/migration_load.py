"""MIG-L: how long does a migration take, as the volume grows.

A load test, not a stress test. The stress lane asks "does a 100G volume
migrate correctly" and answers yes or no. This asks "how long is the source
frozen, at each size" and answers with a curve -- which is the question an
operator planning a maintenance window actually has.

**The number that matters is the freeze.** A migration is four phases and
only one of them stops the volume serving:

    snap_copy       source still serving, ships the snapshot chain
    intermediate    source still serving
    lvol_migrate    THE FREEZE. Source stops, final delta ships, ANA flips
    cleanup_source  target already serving

The design calls ``lvol_migrate`` a short freeze. On the 2G volumes the
correctness lane uses, it is. Nobody has published what it is at the sizes
customers run, and the product does not report it. This produces that
number per size, writes it to CSV, and plots it.

Two measurements per step, because conflating them hides which one scales:

    total_sec    pre-create to terminal
    freeze_sec   time inside lvol_migrate -- the service interruption

Resumable. A sweep to 500G takes hours and often gets killed; every size
already measured stays in the CSV and is skipped on the next run.
"""
import os
import time

from e2e_tests.migration.migration_base import (
    MigrationTestBase,
)
from load_tests._load_base import LoadSweepMixin
from utils.common_utils import sleep_n_sec


def _sizes():
    """Sizes to sweep, as GiB integers. MIG_LOAD_SIZES overrides."""
    raw = os.environ.get("MIG_LOAD_SIZES", "2,10,50,100")
    out = []
    for part in raw.split(","):
        part = part.strip().rstrip("Gg")
        if part:
            try:
                out.append(int(part))
            except ValueError:
                pass
    return out or [2, 10, 50, 100]


class MigrationFreezeByVolumeSize(LoadSweepMixin, MigrationTestBase):
    """MIG-L-001: freeze duration vs volume size.

    One migration per size, under write load so the final delta is real.
    An idle volume has nothing to ship at cutover and would flatter the
    number into uselessness.
    """

    COLUMNS = ("size_gb", "total_sec", "freeze_sec", "snap_copy_sec", "error")
    DEFAULT_OUTPUT = "migration_freeze_by_size.csv"
    PLOT_TITLE = "lvol migration: duration vs volume size"
    PLOT_X = "volume size (GiB)"
    PLOT_Y = "seconds"

    #: Per-step ceiling. A size that exceeds it is recorded as a failure and
    #: the sweep moves on, which is more useful than hanging.
    STEP_BUDGET = int(os.environ.get("MIG_LOAD_BUDGET_SEC", 5400))

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.init_sweep(**kwargs)
        self.test_name = "migration_freeze_by_size"

    def run(self):
        sizes = _sizes()
        self.logger.info("[MIG-L-001] sweeping volume sizes: %s GiB", sizes)
        self.sweep(sizes)

    def measure_step(self, size_gb):
        stamp = int(time.time()) % 100000
        vol = f"migl{size_gb}g{stamp}"
        vol_id, _ = self.make_volume(vol, size=f"{size_gb}G", seed=False)
        self._connect_and_mount_dual(vol, format_disk=True)

        src = self.lvol_node(vol_id)
        tgt = self.pick_target(src, "no-overlap")

        # Write throughout, so lvol_migrate has a genuine delta to ship.
        handle = self._run_fio_dual(vol, runtime=self.STEP_BUDGET,
                                    rw="randwrite", bs="64K", iodepth=8,
                                    numjobs=2, size="2G",
                                    name=f"migload{size_gb}")
        sleep_n_sec(20)

        t0 = time.time()
        mid = self.migrate(vol_id, tgt)
        self.migrate_continue(mid, deadline=self.STEP_BUDGET)

        entered = {}
        last = None
        deadline = time.time() + self.STEP_BUDGET
        while time.time() < deadline:
            rec = self.migration_record(vol_id=vol_id, migration_id=mid)
            if not rec:
                break
            status = str(self._get(rec, "status") or "").lower()
            phase = str(self._get(rec, "phase") or "").lower()
            if phase and phase != last:
                entered[phase] = time.time()
                self.logger.info("[MIG-L-001] %dG: %s at +%.0fs", size_gb,
                                 phase, time.time() - t0)
                last = phase
            if status in self.TERMINAL:
                break
            # Poll fast: the freeze is the measurement, and a 5s poll on a
            # 3s freeze reports zero.
            sleep_n_sec(1)
        end = time.time()
        total = end - t0

        def span(start_phase, *next_phases):
            if start_phase not in entered:
                return ""
            nxt = min((entered[p] for p in next_phases if p in entered),
                      default=end)
            return round(nxt - entered[start_phase], 1)

        freeze = span("lvol_migrate", "cleanup_source")
        snapcopy = span("snap_copy", "intermediate", "lvol_migrate")

        try:
            self._wait_fio_dual([handle], timeout=300)
            self._validate_fio_dual(handle)
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[MIG-L-001] %dG: fio check: %s", size_gb,
                                str(exc)[:200])

        node = self.lvol_node(vol_id)
        err = ""
        if node != tgt:
            err = f"ended on {node}, expected {tgt}"
            self.logger.warning("[MIG-L-001] %dG: %s", size_gb, err)
        if freeze == "":
            self.logger.warning(
                "[MIG-L-001] %dG: never observed lvol_migrate even at a 1s "
                "poll. Either the phase is genuinely sub-second at this "
                "size -- which is the answer -- or the record does not "
                "report it.", size_gb)

        self.logger.info(
            "[MIG-L-001] RESULT %dG: total %.0fs, snap_copy %ss, FREEZE %ss",
            size_gb, total, snapcopy, freeze)

        self.cleanup_migrations()
        return {"size_gb": size_gb, "total_sec": round(total, 1),
                "freeze_sec": freeze, "snap_copy_sec": snapcopy, "error": err}


class MigrationTimeBySnapshotCount(LoadSweepMixin, MigrationTestBase):
    """MIG-L-002: migration duration vs how much history the volume carries.

    A different axis from size, and the one that drives ``snap_copy``. A
    volume with two hundred snapshots is not two hundred times a volume
    with one, and where that curve bends is worth knowing before a customer
    finds it with a retention policy.
    """

    COLUMNS = ("snapshots", "total_sec", "snap_copy_sec", "freeze_sec", "error")
    DEFAULT_OUTPUT = "migration_time_by_snapshots.csv"
    PLOT_TITLE = "lvol migration: duration vs snapshot count"
    PLOT_X = "snapshots on the volume"
    PLOT_Y = "seconds"

    STEP_BUDGET = int(os.environ.get("MIG_LOAD_BUDGET_SEC", 5400))

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.init_sweep(**kwargs)
        self.test_name = "migration_time_by_snapshots"

    def run(self):
        raw = os.environ.get("MIG_LOAD_SNAPS", "0,10,50,100")
        counts = [int(p) for p in raw.split(",") if p.strip().isdigit()]
        self.logger.info("[MIG-L-002] sweeping snapshot counts: %s", counts)
        self.sweep(counts or [0, 10, 50, 100])

    def measure_step(self, count):
        stamp = int(time.time()) % 100000
        vol = f"migls{count}n{stamp}"
        vol_id, _ = self.make_volume(vol, seed=False)

        t_snap = time.time()
        for i in range(count):
            self._cli(f"{self.base_cmd} -d snapshot add {vol_id} "
                      f"migls{stamp}s{i} 2>&1")
        if count:
            self.logger.info("[MIG-L-002] took %d snapshots in %.0fs", count,
                             time.time() - t_snap)

        src = self.lvol_node(vol_id)
        tgt = self.pick_target(src, "no-overlap")

        t0 = time.time()
        mid = self.migrate(vol_id, tgt)
        self.migrate_continue(mid, deadline=self.STEP_BUDGET)

        entered, last = {}, None
        deadline = time.time() + self.STEP_BUDGET
        while time.time() < deadline:
            rec = self.migration_record(vol_id=vol_id, migration_id=mid)
            if not rec:
                break
            status = str(self._get(rec, "status") or "").lower()
            phase = str(self._get(rec, "phase") or "").lower()
            if phase and phase != last:
                entered[phase] = time.time()
                last = phase
            if status in self.TERMINAL:
                break
            sleep_n_sec(1)
        end = time.time()

        def span(p, *nxt):
            if p not in entered:
                return ""
            n = min((entered[q] for q in nxt if q in entered), default=end)
            return round(n - entered[p], 1)

        row = {"snapshots": count,
               "total_sec": round(end - t0, 1),
               "snap_copy_sec": span("snap_copy", "intermediate",
                                     "lvol_migrate"),
               "freeze_sec": span("lvol_migrate", "cleanup_source"),
               "error": "" if self.lvol_node(vol_id) == tgt
                        else f"ended on {self.lvol_node(vol_id)}"}
        self.logger.info("[MIG-L-002] RESULT %d snapshots: total %ss, "
                         "snap_copy %ss, freeze %ss", count,
                         row["total_sec"], row["snap_copy_sec"],
                         row["freeze_sec"])
        self.cleanup_migrations()
        return row


class MigrationConcurrencyCurve(LoadSweepMixin, MigrationTestBase):
    """MIG-L-003: wall-clock for N concurrent migrations, as N grows.

    The stress lane asks whether N concurrent migrations all finish. This
    asks what N costs. A flat line means they genuinely overlap; a line
    that rises with slope one means the scheduler is serialising them and
    "concurrent" is a word rather than a behaviour.
    """

    COLUMNS = ("concurrency", "wall_sec", "per_volume_sec", "completed",
               "error")
    DEFAULT_OUTPUT = "migration_concurrency_curve.csv"
    PLOT_TITLE = "lvol migration: wall-clock vs concurrency"
    PLOT_X = "migrations started together"
    PLOT_Y = "seconds"

    STEP_BUDGET = int(os.environ.get("MIG_LOAD_BUDGET_SEC", 5400))

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.init_sweep(**kwargs)
        self.test_name = "migration_concurrency_curve"

    def run(self):
        raw = os.environ.get("MIG_LOAD_CONCURRENCY", "1,2,4,8")
        ns = [int(p) for p in raw.split(",") if p.strip().isdigit()]
        self.logger.info("[MIG-L-003] sweeping concurrency: %s", ns)
        self.sweep(ns or [1, 2, 4, 8])

    def measure_step(self, n):
        stamp = int(time.time()) % 100000
        vols = []
        for i in range(n):
            name = f"miglc{n}x{stamp}n{i}"
            vid, _ = self.make_volume(name, size="1G", seed=False)
            vols.append((name, vid))
        src = self.lvol_node(vols[0][1])
        tgt = self.pick_target(src, "no-overlap")

        t0 = time.time()
        started = []
        for name, vid in vols:
            try:
                mid = self.migrate(vid, tgt, retries=2, retry_interval=5)
                self.migrate_continue(mid, deadline=self.STEP_BUDGET)
                started.append((name, vid, mid))
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG-L-003] n=%d: could not start %s: "
                                    "%s", n, name, str(exc)[:160])

        completed = 0
        for name, vid, mid in started:
            try:
                status, _ = self.await_migration(
                    vol_id=vid, migration_id=mid, timeout=self.STEP_BUDGET,
                    what=f"MIG-L-003 n={n} {name}")
                if status in self.OK_TERMINAL:
                    completed += 1
            except AssertionError:
                pass
        wall = time.time() - t0

        row = {"concurrency": n, "wall_sec": round(wall, 1),
               "per_volume_sec": round(wall / max(completed, 1), 1),
               "completed": f"{completed}/{n}",
               "error": "" if completed == n
                        else f"only {completed} of {n} completed"}
        self.logger.info(
            "[MIG-L-003] RESULT n=%d: %.0fs wall, %.0fs per volume, %d/%d "
            "completed. A flat per-volume figure as n grows means real "
            "overlap; one that tracks wall-clock means serialisation.",
            n, wall, row["per_volume_sec"], completed, n)
        self.cleanup_migrations()
        return row
