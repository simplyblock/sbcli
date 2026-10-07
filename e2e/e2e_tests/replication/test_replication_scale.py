"""AR-P: scale and endurance. A SEPARATE lane from the correctness suite.

Run with ``--testname replication-stress``. Deliberately not part of
``--testname replication``: a soak measured in hours must never stand between
a correctness run and its answer, and a scale case that needs a terabyte of
lab must not fail a suite that otherwise only needs a few gigabytes.

**Everything here exists because transfers are full, not incremental.**
``allow_partial`` is disabled (PR #1276, ``52e75afb2``) -- the SPDK fork's
fragment write path corrupts partial transfers, so every cycle re-ships the
whole volume no matter how little changed. That single fact sets:

    cycle time      = volume size / link, NOT delta size / link
    useful interval > cycle time, or cycles overlap
    demote downtime = a full transfer, because demote fences first and then
                      ships (lvol_controller.demote_lvol)

The correctness lane keeps volumes small on purpose so it measures the
product rather than the lab network. This lane does the opposite: it makes
size and count the variable, because that is where the feature's limits
live and nothing currently looks for them.

Four questions, one class each:

    AR-P-001..003  what happens when a cycle cannot finish inside its interval
    AR-P-004..006  many relationships at once -- the scheduler, not the bytes
    AR-P-007..009  one large volume -- cycle time, demote time, RPO honesty
    AR-P-010..012  endurance: does anything accumulate over hours

The scale numbers are all overridable from the environment, because the
right value is a property of the lab and not of the test.
"""
import os
import time

from e2e_tests.replication.replication_base import (
    ReplicationTestBase,
    ReplicationPreconditionError,
)
from utils.common_utils import sleep_n_sec, cli_failed


def _env(name, default):
    try:
        return type(default)(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


class ReplicationOverlappingIntervals(ReplicationTestBase):
    """AR-P-001, AR-P-002, AR-P-003: when a cycle cannot finish in time.

    The predictable failure, and the one nothing currently looks for. Ask
    for a cadence shorter than a full transfer can possibly complete in and
    see what the scheduler does. Three outcomes are defensible and they are
    very different to operate:

        queue   cycles run back to back, lag grows but bounded by one cycle
        skip    a cycle is dropped when the previous is still running
        stack   concurrent transfers for one volume

    Only the third is a bug on its own. But ALL THREE must be reported
    honestly, because an operator setting a 1-minute RPO on a volume that
    takes 4 minutes to ship needs to be told, not quietly given a 4-minute
    RPO that the dashboard still calls 1 minute.
    """

    #: Big enough that a full transfer cannot finish inside the interval.
    OVERLAP_VOLUME_SIZE = os.environ.get("AR_OVERLAP_SIZE", "20G")
    OBSERVE_SEC = _env("AR_OVERLAP_OBSERVE_SEC", 900)

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tov{stamp}", self.cluster_b)
        # The shortest cadence the policy accepts, against a volume that
        # cannot possibly ship that fast.
        pname = self.policy_add(f"pov{stamp}", tname, interval_min=1,
                                rpo_target=60)

        vol = f"arov{stamp}"
        self.logger.info("[AR-P-001] %s at %s on a 1-minute cadence -- a full "
                         "transfer cannot finish in time, by construction",
                         vol, self.OVERLAP_VOLUME_SIZE)
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.OVERLAP_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        self._connect_and_mount_dual(vol, format_disk=True)
        self.policy_set(vol_id, pname)
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC * 3,
                         what="initial sync of a deliberately oversized volume")

        # Keep writing, so the source never goes quiet and lets the backlog
        # drain by accident.
        handle = self._run_fio_dual(vol, runtime=self.OBSERVE_SEC,
                                    rw="randwrite", bs="64K", iodepth=8,
                                    numjobs=2, size="2G", name="aroverlap")

        samples = []
        deadline = time.time() + self.OBSERVE_SEC
        while time.time() < deadline:
            rel = self.relationship_for(vol_id) or {}
            info = self.replication_info(vol_id)
            samples.append({
                "t": round(time.time() - (deadline - self.OBSERVE_SEC)),
                "state": rel.get("state"),
                "lag": rel.get("lag") or rel.get("lag_seconds"),
                "inflight": info.count("in_progress") + info.count("running"),
            })
            # AR-P-003: whatever the scheduler does, it must not change state.
            if rel.get("state") not in (self.STATE_REPLICATING, None):
                raise AssertionError(
                    f"[AR-P-003] the relationship left 'replicating' and went "
                    f"to {rel.get('state')!r} purely from cadence pressure -- "
                    f"no outage, no operator action, just an interval shorter "
                    f"than a cycle. Back-pressure must never promote, demote "
                    f"or cut over. samples={samples[-5:]}")
            sleep_n_sec(30)

        self._wait_fio_dual([handle], timeout=self.OBSERVE_SEC + 300)
        self._validate_fio_dual(handle)

        # AR-P-002: concurrent transfers for ONE volume would be the real bug.
        stacked = [s for s in samples if (s["inflight"] or 0) > 1]
        if stacked:
            raise AssertionError(
                f"[AR-P-002] more than one transfer appears to be in flight "
                f"for {vol} at the same time: {stacked[:4]}. A replication "
                f"slot is 1:1 with the volume; two concurrent transfers into "
                f"one target race each other and the target's generation "
                f"becomes whichever finished last.")

        lags = [s["lag"] for s in samples if isinstance(s["lag"], (int, float))]
        self.logger.info("[AR-P-001] lag over %ds: %s", self.OBSERVE_SEC,
                         lags or "not reported")
        if len(lags) >= 4:
            grew = lags[-1] - lags[0]
            self.logger.info(
                "[AR-P-001] lag moved %+.1f over the window (start %.1f, end "
                "%.1f)", grew, lags[0], lags[-1])
            if grew > 0 and lags == sorted(lags):
                self.logger.warning(
                    "[AR-P-001] lag rose monotonically for the whole window. "
                    "With full transfers this is what an unachievable cadence "
                    "looks like: the volume can never catch up, and the "
                    "effective RPO is unbounded while the policy still "
                    "advertises 1 minute. Raised for dev -- the question is "
                    "whether the control plane should REFUSE an interval it "
                    "cannot meet, or report non-compliance against "
                    "rpo_target_seconds.")
        else:
            self.skip_case(
                "AR-P-001",
                "the relationship does not report a numeric lag, so cadence "
                "pressure cannot be quantified. Lag is the only health "
                "signal the feature exposes; without it an operator cannot "
                "tell a slow target from a stopped one.")

        self.logger.info("[AR-P-002/003] PASS: no stacked transfers, no "
                         "unrequested state change under cadence pressure")
        self.assert_no_corruption("after AR-P overlapping intervals")
        self._teardown(vol, vol_id)

    def _teardown(self, vol, vol_id):
        try:
            self.policy_clear(vol_id)
            self._disconnect_and_cleanup_dual(vol)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[AR-P] teardown: %s", str(exc)[:140])
        self.cleanup_replication()


class ReplicationManyVolumes(ReplicationTestBase):
    """AR-P-004, AR-P-005, AR-P-006: many relationships, not many bytes.

    A different axis entirely from volume size. Here each volume is tiny and
    there are a lot of them, so what is under test is the scheduler, the
    task runner and the control plane's bookkeeping -- not the network.

    Finds the things that only appear in aggregate: a per-cluster ceiling
    nobody documented, task-runner saturation where the Nth volume simply
    never gets a cycle, and on Kubernetes whether N VolumeReplication
    objects actually reconcile or whether the controller falls behind.
    """

    COUNT = _env("AR_MANY_COUNT", 25)
    SIZE = os.environ.get("AR_MANY_SIZE", "1G")
    SETTLE_SEC = _env("AR_MANY_SETTLE_SEC", 1800)

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tmv{stamp}", self.cluster_b)
        pname = self.policy_add(f"pmv{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)
        self._vols = []

        self.logger.info("[AR-P-004] creating %d volumes of %s under one "
                         "policy", self.COUNT, self.SIZE)
        t0 = time.time()
        for i in range(self.COUNT):
            name = f"armv{stamp}n{i}"
            try:
                self.sbcli_utils.add_lvol(lvol_name=name,
                                          pool_name=self.pool_name,
                                          size=self.SIZE)
            except Exception as exc:                  # noqa: BLE001
                raise ReplicationPreconditionError(
                    f"[AR-P-004] could not create volume {i + 1} of "
                    f"{self.COUNT}: {str(exc)[:200]}. The lab ran out before "
                    f"the test could exercise scale; lower AR_MANY_COUNT.")
            vid = self.sbcli_utils.get_lvol_id(lvol_name=name)
            self.policy_set(vid, pname)
            self._vols.append({"name": name, "id": vid})
        self.logger.info("[AR-P-004] %d volumes attached in %.0fs",
                         self.COUNT, time.time() - t0)

        # AR-P-005: EVERY one must reach replicating. The interesting failure
        # is not "it was slow" but "volumes 1-20 are fine and 21-25 never
        # started", which is what a saturated task runner looks like and
        # which no per-volume test can see.
        deadline = time.time() + self.SETTLE_SEC
        reached, laggards = set(), {}
        while time.time() < deadline and len(reached) < len(self._vols):
            for v in self._vols:
                if v["name"] in reached:
                    continue
                rel = self.relationship_for(v["id"]) or {}
                if rel.get("state") == self.STATE_REPLICATING:
                    reached.add(v["name"])
                else:
                    laggards[v["name"]] = rel.get("state")
            if len(reached) < len(self._vols):
                self.logger.info("[AR-P-005] %d/%d replicating",
                                 len(reached), len(self._vols))
                sleep_n_sec(30)

        if len(reached) < len(self._vols):
            missing = {k: v for k, v in laggards.items() if k not in reached}
            raise AssertionError(
                f"[AR-P-005] only {len(reached)} of {len(self._vols)} volumes "
                f"reached 'replicating' within {self.SETTLE_SEC}s. Never "
                f"started: {missing}. A per-volume test cannot see this -- it "
                f"is the shape of a saturated scheduler, and it means a "
                f"customer protecting N volumes silently protects fewer.")
        self.logger.info("[AR-P-005] PASS: all %d replicating in %.0fs",
                         len(self._vols), time.time() - t0)

        # AR-P-006: and they must all keep cycling, not just start once.
        self.logger.info("[AR-P-006] forcing a cycle on every volume")
        for v in self._vols:
            self.replication_trigger(v["id"])
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 180)
        stalled = []
        for v in self._vols:
            rel = self.relationship_for(v["id"]) or {}
            if rel.get("state") not in (self.STATE_REPLICATING, None):
                stalled.append((v["name"], rel.get("state")))
        if stalled:
            raise AssertionError(
                f"[AR-P-006] {len(stalled)} volume(s) left 'replicating' "
                f"after a simultaneous cycle across {len(self._vols)}: "
                f"{stalled[:6]}. Concurrent cycles must not knock a "
                f"relationship out of state.")
        self.logger.info("[AR-P-006] PASS: %d volumes all still cycling",
                         len(self._vols))

        self.assert_no_corruption("after AR-P many-volumes")
        for v in self._vols:
            try:
                self.policy_clear(v["id"])
                self.sbcli_utils.delete_lvol(lvol_name=v["name"])
            except Exception:                         # noqa: BLE001
                pass
        self.cleanup_replication()


class ReplicationLargeVolume(ReplicationTestBase):
    """AR-P-007, AR-P-008, AR-P-009: one large volume, measured.

    This is the case that answers the scaling question the feature cannot
    currently answer about itself, and it answers it with a number rather
    than an opinion.

    AR-P-008 is the one to read. A planned relocation fences the source and
    THEN ships (``demote_lvol``: "fence BEFORE triggering the final
    snapshot, never after"), and with full transfers that ship is the whole
    volume. So planned-relocation downtime scales with volume SIZE, in the
    operation we describe to customers as the graceful one. Nobody has put
    a number on it.
    """

    SIZE = os.environ.get("AR_LARGE_SIZE", "100G")
    CYCLE_BUDGET_SEC = _env("AR_LARGE_CYCLE_SEC", 7200)

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tlg{stamp}", self.cluster_b)
        pname = self.policy_add(f"plg{stamp}", tname, interval_min=60)

        vol = f"arlg{stamp}"
        self.logger.info("[AR-P-007] creating a %s volume", self.SIZE)
        try:
            self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                      size=self.SIZE)
        except Exception as exc:                      # noqa: BLE001
            self.skip_case(
                "AR-P-007",
                f"could not create a {self.SIZE} volume: {str(exc)[:160]}. "
                f"This lane needs a cluster with the capacity to hold one; "
                f"set AR_LARGE_SIZE to something the lab can carry.")
            self.cleanup_replication()
            return
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        self.seed_volume(vol, files=2, file_size="256M")

        # ── AR-P-007 how long does one full cycle actually take ───────────
        t0 = time.time()
        self.policy_set(vol_id, pname)
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.CYCLE_BUDGET_SEC,
                         what=f"initial full sync of {self.SIZE}")
        sync = time.time() - t0
        self.logger.info("[AR-P-007] RESULT: initial full sync of %s took "
                         "%.0fs (%.1f min)", self.SIZE, sync, sync / 60)
        self.logger.info(
            "[AR-P-007] Therefore the shortest HONEST interval for a %s "
            "volume on this lab is about %.0f minutes. A policy set shorter "
            "than that cannot be met, and with allow_partial disabled this "
            "does not improve when little changes -- every cycle re-ships "
            "the whole volume.", self.SIZE, (sync / 60) + 1)

        # ── AR-P-008 planned-relocation downtime ──────────────────────────
        self.logger.info("[AR-P-008] measuring the fenced window of a planned "
                         "relocation")
        self._disconnect_and_cleanup_dual(vol)
        t1 = time.time()
        out, err = self._cli(
            f"{self.base_cmd} -d volume replication-failback {vol_id} "
            f"--source-cluster-id {self.cluster_a} 2>&1")
        if cli_failed(out, err):
            self.skip_case(
                "AR-P-008",
                f"could not drive a planned relocation on the large volume: "
                f"{((out or '') + (err or ''))[:200]}")
        else:
            try:
                self.drive_cutover(vol_id, timeout=self.CYCLE_BUDGET_SEC)
                self.await_state(vol_id, self.STATE_CUTOVER_DONE,
                                 timeout=self.CYCLE_BUDGET_SEC,
                                 what="large-volume cut-over")
                fenced = time.time() - t1
                self.logger.info(
                    "[AR-P-008] RESULT: planned relocation of %s took %.0fs "
                    "(%.1f min) end to end. demote fences the source FIRST "
                    "and then ships, so most of that is hard downtime -- "
                    "during the operation we call the graceful one. This is "
                    "the number to put in front of dev for question 5.",
                    self.SIZE, fenced, fenced / 60)
            except AssertionError as exc:
                self.logger.warning(
                    "[AR-P-008] the large-volume relocation did not complete "
                    "within %ds: %s. That is itself the finding: a planned "
                    "relocation whose fenced window exceeds any reasonable "
                    "maintenance slot is not usable at this size.",
                    self.CYCLE_BUDGET_SEC, str(exc)[:200])

        # ── AR-P-009 the data still has to be right ───────────────────────
        self.logger.info("[AR-P-009] verifying the large volume end to end")
        try:
            self._connect_and_mount_dual(vol, format_disk=False)
            sums = self._generate_checksums_dual(vol)
            self.logger.info("[AR-P-009] PASS: %d file(s) readable after a "
                             "large-volume relocation", len(sums))
        except Exception as exc:                      # noqa: BLE001
            raise AssertionError(
                f"[AR-P-009] the large volume is not readable after its "
                f"relocation: {str(exc)[:200]}. Size must not change "
                f"correctness, only duration.")

        self.assert_no_corruption("after AR-P large volume")
        try:
            self.policy_clear(vol_id)
            self._disconnect_and_cleanup_dual(vol)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception:                             # noqa: BLE001
            pass
        self.cleanup_replication()


class ReplicationSoak(ReplicationTestBase):
    """AR-P-010, AR-P-011, AR-P-012: does anything accumulate over hours.

    The correctness lane's longest case runs seven minutes, which is long
    enough to prove a cycle works and far too short to find anything that
    grows. Everything here is a slow leak, invisible per-cycle and obvious
    after a hundred:

        AR-P-010  snapshots accumulating -- retention not actually pruning,
                  so the target fills up days after it was configured
        AR-P-011  lag drifting upward cycle over cycle, the signature of a
                  cadence that is very slightly unachievable
        AR-P-012  the relationship still intact and still correct at the end

    Default six hours. Set AR_SOAK_HOURS for a weekend run; the lblk lane
    found its real bugs at hour 20, not hour 2.
    """

    HOURS = _env("AR_SOAK_HOURS", 6.0)
    RETENTION = _env("AR_SOAK_RETENTION", 3)

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tsk{stamp}", self.cluster_b)
        pname = self.policy_add(f"psk{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN,
                                retention=self.RETENTION)

        vol = f"arsk{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        sums = self.seed_volume(vol)
        self.policy_set(vol_id, pname)
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial sync")

        total = int(self.HOURS * 3600)
        self.logger.info("[AR-P-010] soaking for %.1f hours at a %d-minute "
                         "cadence with retention %d", self.HOURS,
                         self.REPL_INTERVAL_MIN, self.RETENTION)
        started = time.time()
        deadline = started + total
        snap_counts, lags, cycles = [], [], 0

        while time.time() < deadline:
            handle = self._run_fio_dual(vol, runtime=240, rw="randwrite",
                                        bs="16K", numjobs=1, size="256M",
                                        name=f"arsoak{cycles}")
            self._wait_fio_dual([handle], timeout=600)
            self.replication_trigger(vol_id)
            sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 60)
            cycles += 1

            rel = self.relationship_for(vol_id) or {}
            if rel.get("state") not in (self.STATE_REPLICATING, None):
                raise AssertionError(
                    f"[AR-P-012] after {cycles} cycles and "
                    f"{(time.time() - started) / 3600:.1f}h the relationship "
                    f"is {rel.get('state')!r}. Nothing was injected -- this "
                    f"is drift, and it is exactly what a short test cannot "
                    f"find.")
            lag = rel.get("lag") or rel.get("lag_seconds")
            if isinstance(lag, (int, float)):
                lags.append(lag)

            try:
                out, _ = self._cli(f"{self.base_cmd} snapshot list 2>&1")
                n = sum(1 for ln in out.splitlines() if vol in ln)
                snap_counts.append(n)
            except Exception:                         # noqa: BLE001
                pass

            if cycles % 5 == 0:
                self.logger.info(
                    "[AR-P-010] %.1fh elapsed, %d cycles, snapshots=%s "
                    "lag=%s", (time.time() - started) / 3600, cycles,
                    snap_counts[-5:] or "n/a", lags[-5:] or "n/a")
                self.assert_no_corruption(f"soak checkpoint, cycle {cycles}")

        self.logger.info("[AR-P] soak finished: %d cycles over %.1fh",
                         cycles, (time.time() - started) / 3600)

        # ── AR-P-010 retention must actually prune ────────────────────────
        if snap_counts:
            peak = max(snap_counts)
            self.logger.info("[AR-P-010] snapshot count over the soak: "
                             "min=%d peak=%d final=%d (retention=%d)",
                             min(snap_counts), peak, snap_counts[-1],
                             self.RETENTION)
            # Generous: internal replication snapshots are not the same as
            # user snapshots, so the bar is "bounded", not "exactly N".
            if peak > self.RETENTION * 4 and snap_counts[-1] >= peak:
                raise AssertionError(
                    f"[AR-P-010] snapshots grew to {peak} over {cycles} "
                    f"cycles against a retention of {self.RETENTION}, and "
                    f"never came back down (final {snap_counts[-1]}). "
                    f"Retention is not pruning: the target fills up days "
                    f"after the policy was configured, long after anyone is "
                    f"still watching. counts={snap_counts}")
            self.logger.info("[AR-P-010] PASS: snapshot count stayed bounded")
        else:
            self.skip_case("AR-P-010",
                           "snapshot listing produced nothing attributable to "
                           "the volume, so retention could not be observed "
                           "over the soak.")

        # ── AR-P-011 lag must not drift ───────────────────────────────────
        if len(lags) >= 6:
            first, last = sum(lags[:3]) / 3, sum(lags[-3:]) / 3
            self.logger.info("[AR-P-011] mean lag: first 3 cycles %.1f, last "
                             "3 cycles %.1f", first, last)
            if last > first * 3 and last - first > 60:
                raise AssertionError(
                    f"[AR-P-011] lag drifted from a mean of {first:.1f}s to "
                    f"{last:.1f}s over {cycles} cycles with an unchanged "
                    f"workload. A steady workload must reach a steady lag; "
                    f"upward drift means the cadence is very slightly "
                    f"unachievable and the real RPO is degrading silently. "
                    f"lags={lags}")
            self.logger.info("[AR-P-011] PASS: lag stable across the soak")

        # ── AR-P-012 and the data is still right ──────────────────────────
        self._disconnect_and_cleanup_dual(vol)
        self.failover_policy(pname)
        self.await_state(vol_id, self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC, what="post-soak fail-over")
        name = self.failed_over_volume_name(vol_id, vol)
        self._connect_and_mount_dual(name, format_disk=False)
        self.verify_volume(name, sums,
                           context=f"after {cycles} cycles over "
                                   f"{self.HOURS:.1f}h (AR-P-012)")
        self.logger.info("[AR-P-012] PASS: byte-identical after a full soak")

        self.assert_no_corruption("after AR-P soak")
        for n in {vol, name}:
            try:
                self._disconnect_and_cleanup_dual(n)
                self.sbcli_utils.delete_lvol(lvol_name=n)
            except Exception:                         # noqa: BLE001
                pass
        self.cleanup_replication()
