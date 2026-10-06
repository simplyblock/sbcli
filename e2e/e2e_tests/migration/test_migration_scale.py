"""MIG-P: migration at scale and over time. The SEPARATE stress lane.

Run with ``--testname migration-stress``. Not part of ``--testname
migration`` for the same reason the replication lanes are split: a soak
measured in hours must never stand between a correctness run and its
answer.

What these add over the correctness lane, which the original scripts had
only one of (a rebalance soak):

* **MIG-P-001/002 large volumes.** ``lvol_migrate`` freezes the source for
  the final delta. How long is that freeze on a volume the size customers
  actually use? Nobody has a number, and "short freeze" is doing a lot of
  work in the design description. This case produces the number.
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
    MigrationTestBase,
    MigrationPreconditionError,
)
from utils.common_utils import sleep_n_sec


def _env(name, default):
    try:
        return type(default)(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


class MigrationLargeVolume(MigrationTestBase):
    """MIG-P-001, MIG-P-002: how long is the "short freeze", really.

    The design calls ``lvol_migrate`` a short freeze. On a 2 GiB test volume
    it is. The question this answers is what happens at the size a customer
    runs, because the freeze is when the source stops serving and the final
    delta ships -- and nothing in the product reports how long that was.
    """

    SIZE = os.environ.get("MIG_LARGE_SIZE", "100G")
    BUDGET = _env("MIG_LARGE_BUDGET_SEC", 7200)

    def run(self):
        stamp = int(time.time()) % 100000
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
        handle = self._run_fio_dual(vol, runtime=self.BUDGET, rw="randwrite",
                                    bs="64K", iodepth=8, numjobs=2,
                                    size="4G", name="miglarge")

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

        self._wait_fio_dual([handle], timeout=self.BUDGET + 600)
        self._validate_fio_dual(handle)
        self.assert_placed_on(vol_id, tgt, "(MIG-P-001)")
        self.verify(vol, sums, f"after migrating {self.SIZE} (MIG-P-002)")
        self.logger.info("[MIG-P-002] PASS: size changes duration, not "
                         "correctness")
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
    """MIG-P-005, MIG-P-006: migrate round and round for hours.

    The leak detector. A single migration cannot show you that each one
    leaves a snapshot behind, or a ``_m`` bdev, or a database row -- those
    only become visible after fifty. The original scripts had a rebalance
    soak; this is the per-volume equivalent, and it measures whether the
    hundredth migration is as fast as the first.
    """

    HOURS = _env("MIG_SOAK_HOURS", 6.0)

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"migsoak{stamp}"
        vol_id, sums = self.make_volume(vol)
        nodes = [n.get("uuid") or n.get("id") for n in self.online_nodes()]
        if len(nodes) < 2:
            self.logger.warning("[MIG-P-005] SKIPPED: needs two online nodes.")
            return

        total = int(self.HOURS * 3600)
        deadline = time.time() + total
        hops, durations, snap_counts = 0, [], []
        here = self.lvol_node(vol_id)

        self.logger.info("[MIG-P-005] soaking for %.1f hours, hopping between "
                         "%d nodes", self.HOURS, len(nodes))
        while time.time() < deadline:
            there = next((n for n in nodes if n != here), None)
            if not there:
                break
            t0 = time.time()
            try:
                self.full_migration(vol_id, there,
                                    what=f"MIG-P-005 hop {hops + 1}")
            except AssertionError as exc:
                raise AssertionError(
                    f"[MIG-P-005] hop {hops + 1} failed after "
                    f"{(time.time() - (deadline - total)) / 3600:.1f}h of "
                    f"soaking: {str(exc)[:240]}. Earlier hops succeeded, so "
                    f"something accumulated.") from exc
            durations.append(time.time() - t0)
            hops += 1
            here = there

            data = self._cli_json(f"{self.base_cmd} snapshot list")
            rows = (data.get("results", data) if isinstance(data, dict)
                    else data) or []
            snap_counts.append(len(rows))

            if hops % 5 == 0:
                self.logger.info(
                    "[MIG-P-005] %d hops, %.1fh elapsed, last 5 durations "
                    "%s, snapshots %s", hops,
                    (time.time() - (deadline - total)) / 3600,
                    [round(d) for d in durations[-5:]], snap_counts[-5:])
                self.verify(vol, sums, f"at hop {hops} (MIG-P-005)")

        self.logger.info("[MIG-P-005] soak finished: %d hops over %.1fh",
                         hops, self.HOURS)
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
        if len(snap_counts) >= 4 and snap_counts[-1] > snap_counts[0] + hops:
            raise AssertionError(
                f"[MIG-P-006] snapshot count grew from {snap_counts[0]} to "
                f"{snap_counts[-1]} across {hops} migrations of ONE volume "
                f"with no snapshots taken by the test. Each migration is "
                f"leaving its internal snapshots behind; at this rate the "
                f"cluster fills up on migration traffic alone. "
                f"counts={snap_counts}")

        self.verify(vol, sums, f"after {hops} migrations (MIG-P-006)")
        self.logger.info("[MIG-P-006] PASS: %d hops, no drift, data intact",
                         hops)
        self.cleanup_migrations()
