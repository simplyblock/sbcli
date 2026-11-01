"""AR-L: replication timings that only mean anything as a curve.

The two numbers nobody has, and the reason neither belongs in a pass/fail
lane: there is no threshold to assert against. The deliverable is the
curve, and the decision it supports is "how big a volume can we honestly
offer this on".

**AR-L-001: cycle time vs volume size.** Every transfer is a FULL copy --
``allow_partial`` is disabled because the SPDK fork corrupts partial
transfers (PR #1276, ``52e75afb2``). So cycle time tracks volume size, not
how much changed, and the shortest interval you can honestly offer at a
given size is whatever this measures. A policy set shorter than that can
never be met, and today nothing refuses it.

**AR-L-002: planned-relocation downtime vs volume size.** ``demote_lvol``
fences the source FIRST and then ships ("fence BEFORE triggering the final
snapshot, never after"). Combined with full transfers, the volume is
unavailable for the duration of a whole-volume copy -- during the operation
we describe to customers as the graceful one. This puts a number on it per
size.

Both are resumable: a sweep to 500G runs for hours and often gets killed,
and the sizes already measured stay in the CSV.
"""
import os
import time

from e2e_tests.replication.replication_base import (
    ReplicationTestBase,
)
from load_tests._load_base import LoadSweepMixin
from utils.common_utils import cli_failed, sleep_n_sec


def _sizes(env, default):
    raw = os.environ.get(env, default)
    out = []
    for p in raw.split(","):
        p = p.strip().rstrip("Gg")
        if p:
            try:
                out.append(int(p))
            except ValueError:
                pass
    return out


class ReplicationCycleTimeBySize(LoadSweepMixin, ReplicationTestBase):
    """AR-L-001: how long one full cycle takes, per volume size.

    The output is the honest minimum interval at each size. With full
    transfers that figure does not improve when little changes, which is
    the part operators get wrong.
    """

    COLUMNS = ("size_gb", "initial_sync_sec", "steady_cycle_sec",
               "min_honest_interval_min", "error")
    DEFAULT_OUTPUT = "replication_cycle_by_size.csv"
    PLOT_TITLE = "async replication: cycle time vs volume size"
    PLOT_X = "volume size (GiB)"
    PLOT_Y = "seconds"

    STEP_BUDGET = int(os.environ.get("AR_LOAD_BUDGET_SEC", 7200))

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.init_sweep(**kwargs)
        self.test_name = "replication_cycle_by_size"

    def run(self):
        self.build_second_cluster()
        sizes = _sizes("AR_LOAD_SIZES", "2,10,50,100")
        self.logger.info("[AR-L-001] sweeping volume sizes: %s GiB", sizes)
        self.sweep(sizes)

    def measure_step(self, size_gb):
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tl{size_gb}g{stamp}", self.cluster_b)
        # A long interval on purpose: this measures how long a cycle TAKES,
        # so cycles must not overlap or the number is meaningless.
        pname = self.policy_add(f"pl{size_gb}g{stamp}", tname,
                                interval_min=120)

        vol = f"arl{size_gb}g{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=f"{size_gb}G")
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        self.seed_volume(vol, files=2, file_size="128M")

        # ── initial full sync ────────────────────────────────────────────
        t0 = time.time()
        self.policy_set(vol_id, pname)
        err = ""
        try:
            self.await_state(vol_id, self.STATE_REPLICATING,
                             timeout=self.STEP_BUDGET,
                             what=f"AR-L-001 initial sync of {size_gb}G")
        except AssertionError as exc:
            err = f"initial sync did not complete: {str(exc)[:120]}"
        initial = time.time() - t0

        # ── a steady-state cycle, with the volume otherwise idle ─────────
        # Idle on purpose: this isolates transfer time from write rate.
        # AR-P-001 covers what happens when the two compete.
        t1 = time.time()
        self.replication_trigger(vol_id)
        steady = ""
        deadline = time.time() + self.STEP_BUDGET
        seen_start = False
        while time.time() < deadline:
            rel = self.relationship_for(vol_id) or {}
            outstanding = (rel.get("outstanding_count")
                           or rel.get("outstanding") or 0)
            try:
                outstanding = int(outstanding)
            except (TypeError, ValueError):
                outstanding = 0
            if outstanding > 0:
                seen_start = True
            elif seen_start:
                steady = round(time.time() - t1, 1)
                break
            sleep_n_sec(5)
        if steady == "":
            # No outstanding-count to watch; fall back to the interval the
            # relationship reports having last completed in.
            steady = round(time.time() - t1, 1)
            self.logger.warning(
                "[AR-L-001] %dG: could not observe a cycle start and finish "
                "from outstanding_count; the steady figure is elapsed time "
                "to quiescence and may overstate the transfer.", size_gb)

        honest = round((max(initial, float(steady or 0)) / 60) + 1, 1)
        self.logger.info(
            "[AR-L-001] RESULT %dG: initial sync %.0fs, steady cycle %ss. "
            "Shortest HONEST interval at this size is about %s minutes -- "
            "and with allow_partial disabled it does not improve when "
            "little changes.", size_gb, initial, steady, honest)

        try:
            self.policy_clear(vol_id)
            self._disconnect_and_cleanup_dual(vol)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[AR-L-001] cleanup: %s", str(exc)[:140])
        self.cleanup_replication()

        return {"size_gb": size_gb, "initial_sync_sec": round(initial, 1),
                "steady_cycle_sec": steady,
                "min_honest_interval_min": honest, "error": err}


class ReplicationDemoteDowntimeBySize(LoadSweepMixin, ReplicationTestBase):
    """AR-L-002: how long a PLANNED relocation takes the volume offline.

    The number behind open question 5. ``demote_lvol`` fences first and
    then ships, and the ship is a full volume copy, so the "graceful"
    operation's downtime scales with size. Measured end to end: from the
    demote request to the volume being usable on the other side.
    """

    COLUMNS = ("size_gb", "demote_sec", "cutover_sec", "total_downtime_sec",
               "error")
    DEFAULT_OUTPUT = "replication_demote_downtime.csv"
    PLOT_TITLE = "async replication: planned-relocation downtime vs size"
    PLOT_X = "volume size (GiB)"
    PLOT_Y = "seconds"

    STEP_BUDGET = int(os.environ.get("AR_LOAD_BUDGET_SEC", 7200))

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.init_sweep(**kwargs)
        self.test_name = "replication_demote_downtime"

    def run(self):
        self.build_second_cluster()
        sizes = _sizes("AR_LOAD_SIZES", "2,10,50,100")
        self.logger.info("[AR-L-002] sweeping volume sizes: %s GiB", sizes)
        self.sweep(sizes)

    def measure_step(self, size_gb):
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"td{size_gb}g{stamp}", self.cluster_b)
        pname = self.policy_add(f"pd{size_gb}g{stamp}", tname,
                                interval_min=120)

        vol = f"ard{size_gb}g{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=f"{size_gb}G")
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        sums = self.seed_volume(vol, files=2, file_size="128M")

        self.policy_set(vol_id, pname)
        err = ""
        try:
            self.await_state(vol_id, self.STATE_REPLICATING,
                             timeout=self.STEP_BUDGET,
                             what=f"AR-L-002 initial sync {size_gb}G")
        except AssertionError as exc:
            err = f"never synced: {str(exc)[:120]}"
            self.cleanup_replication()
            return {"size_gb": size_gb, "error": err}

        # Everything from here is downtime: the source is unmounted, the
        # demote fences it, and it is not usable again until the far side
        # is serving.
        self._disconnect_and_cleanup_dual(vol)
        t0 = time.time()

        demote_sec = ""
        try:
            self.sbcli_utils.post_request(
                f"/api/v2/clusters/{self.cluster_a}/storage-pools/"
                f"{self.pool_id_a or self.pool_name}/volumes/{vol_id}"
                f"/replication/demote", body={})
            demote_sec = round(time.time() - t0, 1)
            self.logger.info("[AR-L-002] %dG: demote returned in %ss",
                             size_gb, demote_sec)
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning(
                "[AR-L-002] %dG: demote route not reachable (%s); timing the "
                "failback+commit path instead, which is the CLI equivalent.",
                size_gb, str(exc)[:160])

        t1 = time.time()
        out, cerr = self.failback(vol_id, source_cluster_id=self.cluster_a)
        if cli_failed(out, cerr):
            err = f"failback refused: {((out or '') + (cerr or ''))[:140]}"
        else:
            try:
                self.drive_cutover(vol_id, timeout=self.STEP_BUDGET)
                self.await_state(vol_id, self.STATE_CUTOVER_DONE,
                                 timeout=self.STEP_BUDGET,
                                 what=f"AR-L-002 cutover {size_gb}G")
            except AssertionError as exc:
                err = f"cutover did not finish: {str(exc)[:120]}"
        cutover_sec = round(time.time() - t1, 1)
        total = round(time.time() - t0, 1)

        self.logger.info(
            "[AR-L-002] RESULT %dG: demote %ss, cutover %ss, TOTAL DOWNTIME "
            "%ss (%.1f min). The source is fenced for most of that, during "
            "the operation we call graceful.", size_gb, demote_sec,
            cutover_sec, total, total / 60)

        if not err:
            try:
                self._connect_and_mount_dual(vol, format_disk=False)
                self.verify_volume(vol, sums,
                                   f"after a planned relocation at {size_gb}G "
                                   f"(AR-L-002)")
            except Exception as exc:                  # noqa: BLE001
                err = f"data check failed: {str(exc)[:140]}"

        try:
            self.policy_clear(vol_id)
            self._disconnect_and_cleanup_dual(vol)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception:                             # noqa: BLE001
            pass
        self.cleanup_replication()

        return {"size_gb": size_gb, "demote_sec": demote_sec,
                "cutover_sec": cutover_sec, "total_downtime_sec": total,
                "error": err}
