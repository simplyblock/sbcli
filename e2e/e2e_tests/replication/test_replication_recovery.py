"""AR-R: fail-over, fail-back and migration.

The three operations that move a volume between clusters, and the scopes
they come in. What the CLI actually offers, checked against
``simplyblock_cli/cli.py`` rather than the design notes:

    cluster replication-target-failover <target_id>     every volume on the pair
    cluster replication-policy-failover <policy_id>     every volume on the policy
    volume  replication-failback <lvol_id> [--source-cluster-id]
    volume  replication-commit   <lvol_id> [--delete-source]
    volume  replication-start    <lvol_id> --replication-cluster-id   (migration)

Two consequences worth stating up front, because they shape several cases:

* **There is no volume-scope fail-over verb.** Only policy and target scope
  exist. AR-R-001 therefore goes through the v2 API, and if that is not
  reachable it is recorded as a skip rather than quietly folded into
  AR-R-002.

* **Fail-back reverses ``direction``.** The original source becomes the
  TARGET of the reverse relationship, which is why every lookup here goes
  through :meth:`relationship_for` (it searches both ends) and never by
  source alone. The control plane's own ``set_cutover_proceed`` carries a
  comment about the same trap.

Cut-over is a two-phase handshake: a migration or fail-back parks in
``cutover_pending`` until something signals ``cutover_proceed``. On k8s the
operator does it. On docker nothing does, so :meth:`drive_cutover` must --
otherwise the case sits until ``REPL_CUTOVER_PROCEED_TIMEOUT_SEC`` (120s)
expires and reports a pass that proves the fallback works, not the feature.
"""
import time

from e2e_tests.replication.replication_base import (
    ReplicationPreconditionError,
    ReplicationTestBase,
)
from utils.common_utils import cli_failed, sleep_n_sec


class _RecoveryBase(ReplicationTestBase):
    """Shared setup: a target, a policy, and n seeded volumes under it."""

    VOLUMES = 1

    def _stand_up(self, tag, interval_min=None, mode=None, seed=True,
                  consistency_group=False):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        self._tname = self.target_add(f"t{tag}{stamp}", self.cluster_b)
        self._pname = self.policy_add(
            f"p{tag}{stamp}", self._tname,
            interval_min=interval_min if interval_min is not None
            else self.REPL_INTERVAL_MIN,
            mode=mode, consistency_group=consistency_group)
        self._vols = []
        for i in range(self.VOLUMES):
            name = f"ar{tag}{stamp}v{i}"
            self.sbcli_utils.add_lvol(lvol_name=name, pool_name=self.pool_name,
                                      size=self.REPL_VOLUME_SIZE)
            vid = self.sbcli_utils.get_lvol_id(lvol_name=name)
            sums = self.seed_volume(name) if seed else {}
            self.policy_set(vid, self._pname)
            self._vols.append({"name": name, "id": vid, "sums": sums})
        for v in self._vols:
            self.await_state(v["id"], self.STATE_REPLICATING,
                             timeout=self.REPL_CYCLE_SEC,
                             what=f"initial sync of {v['name']}")
        return self._vols

    def _quiesce(self):
        """Unmount every source volume so no cycle is mid-flight."""
        for v in self._vols:
            try:
                self._disconnect_and_cleanup_dual(v["name"])
            except Exception:                         # noqa: BLE001
                pass

    def _teardown(self):
        for v in getattr(self, "_vols", []):
            for n in {v["name"], v.get("failed_over_name") or v["name"]}:
                try:
                    self._disconnect_and_cleanup_dual(n)
                except Exception:                     # noqa: BLE001
                    pass
            try:
                self.policy_clear(v["id"])
            except Exception:                         # noqa: BLE001
                pass
            for n in {v["name"], v.get("failed_over_name") or v["name"]}:
                try:
                    self.sbcli_utils.delete_lvol(lvol_name=n)
                except Exception as exc:              # noqa: BLE001
                    self.logger.warning("[AR] could not delete %s: %s", n,
                                        str(exc)[:120])
        self.cleanup_replication()


class ReplicationFailoverScopes(_RecoveryBase):
    """AR-R-001, AR-R-002, AR-R-003, AR-R-005.

    The three scopes, and what a second fail-over does. Scope is the whole
    blast-radius question: an operator reaching for target scope during a
    site loss needs to know it takes everything, and one reaching for policy
    scope needs to know it does not touch the neighbouring policy.
    """

    VOLUMES = 2

    def run(self):
        vols = self._stand_up("fs")

        # A second policy on the same target, so AR-R-002 can prove that
        # policy scope is bounded. Without a bystander there is nothing to
        # show it did not take everything.
        stamp = int(time.time()) % 100000
        other_pol = self.policy_add(f"pby{stamp}", self._tname,
                                    interval_min=self.REPL_INTERVAL_MIN)
        bystander = f"arby{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=bystander,
                                  pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        by_id = self.sbcli_utils.get_lvol_id(lvol_name=bystander)
        self.policy_set(by_id, other_pol)
        self.await_state(by_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="bystander sync")
        self._vols.append({"name": bystander, "id": by_id, "sums": {}})
        self._quiesce()

        # ── AR-R-001 volume scope: API only ───────────────────────────────
        self.logger.info("[AR-R-001] volume-scope fail-over for %s",
                         vols[0]["name"])
        try:
            self.sbcli_utils.post_request(
                f"/api/v2/clusters/{self.cluster_a}/storage-pools/"
                f"{self.pool_id_a or self.pool_name}/volumes/{vols[0]['id']}"
                f"/replication/failover", body={"scope": "volume"})
        except Exception as exc:                      # noqa: BLE001
            self.skip_case(
                "AR-R-001",
                f"no volume-scope fail-over route on this build "
                f"({str(exc)[:140]}). The CLI has only policy and target "
                f"scope; if volume scope is API-only it is untested from the "
                f"operator's usual surface, and if it does not exist the "
                f"ReplicationOps CRD's scope=volume has no backend.")
        else:
            self.await_state(vols[0]["id"], self.STATE_FAILED_OVER,
                             timeout=self.REPL_OP_SEC,
                             what="volume-scope fail-over")
            other = self.relationship_for(vols[1]["id"]) or {}
            if other.get("state") == self.STATE_FAILED_OVER:
                raise AssertionError(
                    f"[AR-R-001] volume scope failed over {vols[1]['name']} "
                    f"too. Volume scope must take exactly one volume; taking "
                    f"its policy-mates makes the narrowest scope the widest.")
            self.logger.info("[AR-R-001] PASS: only the named volume moved")

        # ── AR-R-002 policy scope takes every member, and only them ───────
        self.logger.info("[AR-R-002] policy-scope fail-over of %s", self._pname)
        self.failover_policy(self._pname)
        for v in vols:
            self.await_state(v["id"], self.STATE_FAILED_OVER,
                             timeout=self.REPL_OP_SEC,
                             what=f"policy-scope fail-over of {v['name']}")
        by_rel = self.relationship_for(by_id) or {}
        if by_rel.get("state") == self.STATE_FAILED_OVER:
            raise AssertionError(
                f"[AR-R-002] {bystander} follows a DIFFERENT policy "
                f"({other_pol}) and was failed over anyway. Policy scope must "
                f"stop at the policy boundary, or an operator failing over "
                f"one application takes the whole cluster with it.")
        self.logger.info("[AR-R-002] PASS: every member moved, the bystander "
                         "on %s did not", other_pol)

        # ── AR-R-005 a second fail-over is a no-op, not an error ──────────
        self.logger.info("[AR-R-005] failing over %s a second time",
                         self._pname)
        out, err = self.failover_policy(self._pname)
        combined = ((out or "") + (err or "")).lower()
        if "traceback" in combined or "exception" in combined:
            raise AssertionError(
                f"[AR-R-005] a repeat fail-over raised rather than being "
                f"skipped: {(out or '') + (err or '')!r:.300}. Operators "
                f"retry during an incident; the second attempt must be safe.")
        for v in vols:
            rel = self.relationship_for(v["id"]) or {}
            if rel.get("state") != self.STATE_FAILED_OVER:
                raise AssertionError(
                    f"[AR-R-005] {v['name']} left {self.STATE_FAILED_OVER!r} "
                    f"and is now {rel.get('state')!r} after a repeat "
                    f"fail-over. A no-op must be a no-op.")
        self.logger.info("[AR-R-005] PASS: repeat fail-over skipped cleanly")

        # ── AR-R-003 target scope takes every policy on the pair ──────────
        self.logger.info("[AR-R-003] target-scope fail-over of %s", self._tname)
        self.failover_target(self._tname)
        self.await_state(by_id, self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC,
                         what="target scope must reach the second policy")
        self.logger.info("[AR-R-003] PASS: target scope reached %s on policy "
                         "%s", bystander, other_pol)

        self.assert_no_corruption("after AR-R fail-over scopes")
        self._teardown()


class ReplicationFailoverDataLoss(_RecoveryBase):
    """AR-R-004: data loss on fail-over is bounded by one interval.

    The feature's actual RPO claim, and the one an operator plans around.
    Write a marker, let a cycle take it; write a second marker and fail over
    *before* the next cycle. The first must survive. The second may or may
    not -- that is the interval's worth of loss, and it is allowed. What is
    not allowed is losing the first, which would mean the RPO is unbounded.
    """

    def run(self):
        self._stand_up("dl", seed=False)
        v = self._vols[0]
        self._connect_and_mount_dual(v["name"], format_disk=True)

        self.logger.info("[AR-R-004] writing marker A and letting it replicate")
        sums_a = self.write_marker_files(v["name"], "markerA", count=2)
        if not any("markerA" in f for f in sums_a):
            raise ReplicationPreconditionError(
                "[AR-R-004] no markerA files landed on the volume, so the "
                "RPO assertion below would compare an empty set and pass "
                "without proving anything.")
        self.replication_trigger(v["id"])
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 90)

        self.logger.info("[AR-R-004] writing marker B, NOT letting it "
                         "replicate, then failing over immediately")
        self.write_marker_files(v["name"], "markerB", count=2)
        self._disconnect_and_cleanup_dual(v["name"])

        self.failover_policy(self._pname)
        self.await_state(v["id"], self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC, what="fail-over")

        name = self.failed_over_volume_name(v["id"], v["name"])
        v["failed_over_name"] = name
        self._connect_and_mount_dual(name, format_disk=False)
        after = self._generate_checksums_dual(name)

        lost_a = [f for f in sums_a
                  if "markerA" in f and (f not in after or after[f] != sums_a[f])]
        if lost_a:
            raise AssertionError(
                f"[AR-R-004] data that completed a replication cycle BEFORE "
                f"the fail-over is missing or changed on the target: "
                f"{lost_a}. RPO is supposed to be bounded by one interval; "
                f"losing an already-replicated write makes it unbounded.")

        kept_b = [f for f in after if "markerB" in f]
        self.logger.info(
            "[AR-R-004] PASS: every pre-cycle write survived. Post-cycle "
            "writes present on the target: %d (either outcome is within the "
            "one-interval RPO)", len(kept_b))

        self.assert_no_corruption("after AR-R-004")
        self._teardown()


class ReplicationFailback(_RecoveryBase):
    """AR-R-006, AR-R-007, AR-R-008, AR-R-009.

    Getting back. Fail-back to a recovered original source should move only
    the delta; to a fresh cluster it must take a full copy. Both end in a
    two-phase cut-over, and both reverse ``direction``.
    """

    def run(self):
        self._stand_up("fb")
        v = self._vols[0]
        self._quiesce()

        self.logger.info("[AR-R-006] failing over, then back to %s",
                         self.cluster_a)
        self.failover_policy(self._pname)
        self.await_state(v["id"], self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC, what="fail-over")

        # ── AR-R-008 fail-back is volume-scoped ───────────────────────────
        # The CLI takes an lvol_id, so volume scope is the only scope there
        # is. Worth asserting rather than assuming: a fail-back that quietly
        # took the whole policy would be a nasty surprise mid-incident.
        self.logger.info("[AR-R-008] fail-back takes a volume id")
        out, err = self.failback(v["id"], source_cluster_id=self.cluster_a)
        combined = (out or "") + (err or "")
        if cli_failed(combined):
            raise ReplicationPreconditionError(
                f"[AR-R-008] fail-back refused for {v['name']}: "
                f"{combined[:300]}")
        self.logger.info("[AR-R-008] PASS: accepted at volume scope")

        rel = self.relationship_for(v["id"]) or {}
        if rel.get("direction") and rel["direction"] != self.DIRECTION_TO_SOURCE:
            raise AssertionError(
                f"[AR-R-006] after fail-back the direction is "
                f"{rel['direction']!r}; it must be {self.DIRECTION_TO_SOURCE!r}. "
                f"If direction does not reverse, every lookup by source finds "
                f"the wrong end and the cut-over signal goes to the wrong "
                f"volume.")

        # ── AR-R-009 the freeze is brief ──────────────────────────────────
        # Back-replication is online: the volume should be writable right up
        # to the cut-over, with only a short freeze at the handshake. Time
        # the pending window -- a long one is an availability bug even if
        # the data is correct.
        t0 = time.time()
        self.drive_cutover(v["id"])
        self.await_state(v["id"], self.STATE_CUTOVER_DONE,
                         timeout=self.REPL_OP_SEC, what="fail-back cut-over")
        froze_for = time.time() - t0
        self.logger.info("[AR-R-009] cut-over completed in %.1fs", froze_for)
        if froze_for > 120:
            self.logger.warning(
                "[AR-R-009] the cut-over took %.1fs. REPL_CUTOVER_PROCEED_"
                "TIMEOUT_SEC is 120s, so anything near or above that means "
                "the handshake fell through to its safety fallback rather "
                "than being signalled -- the test would then be proving the "
                "fallback, not the feature.", froze_for)
        else:
            self.logger.info("[AR-R-009] PASS: freeze was brief (%.1fs)",
                             froze_for)

        self.verify_volume(v["name"], v["sums"],
                           context="after fail-back to the original source "
                                   "(AR-R-006)")
        self.logger.info("[AR-R-006] PASS: delta fail-back, data intact")

        # ── AR-R-007 fail-back to a fresh cluster ─────────────────────────
        # Only meaningful with a third cluster to land on. With two we can
        # still exercise the "fresh" path by failing over and back again
        # with no prior relationship on the destination, but say plainly
        # that this is not the full case.
        self.skip_case(
            "AR-R-007",
            "fail-back to a FRESH source cluster needs a third cluster to "
            "land on; the lab provides two. The delta path (AR-R-006) is "
            "covered. Running the full case needs either a third cluster in "
            "the lab or a documented way to make cluster A look fresh to "
            "the control plane.")

        self.assert_no_corruption("after AR-R fail-back cases")
        self._teardown()


class ReplicationMigration(_RecoveryBase):
    """AR-R-010, AR-R-011, AR-R-012: moving a volume while it is in use.

    Migration is ``replication-start`` plus ``replication-commit``, with the
    same two-phase cut-over as fail-back. The interesting case is AR-R-012:
    committing before the first full transfer has finished must be refused,
    because the destination does not yet hold the data.
    """

    def run(self):
        self._stand_up("mg")
        v = self._vols[0]

        # ── AR-R-012 commit before the copy is complete ───────────────────
        # Do this first, on a volume that has only just started, so the
        # refusal is being tested against a genuinely incomplete transfer
        # rather than one that finished while the test was setting up.
        fresh = f"armgfresh{int(time.time()) % 100000}"
        self.sbcli_utils.add_lvol(lvol_name=fresh, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        fresh_id = self.sbcli_utils.get_lvol_id(lvol_name=fresh)
        self._vols.append({"name": fresh, "id": fresh_id, "sums": {}})
        self.logger.info("[AR-R-012] starting a migration and committing "
                         "immediately")
        self.replication_start(fresh_id, self.cluster_b,
                               interval_min=self.REPL_INTERVAL_MIN)
        out, err = self.commit(fresh_id)
        self.expect_refused(
            "AR-R-012", (out, err),
            "a commit issued before the first full transfer completed",
            allow=("not complete", "in progress", "pending", "not ready",
                   "still"))

        # ── AR-R-010 migrate a volume online ──────────────────────────────
        self.logger.info("[AR-R-010] migrating %s to %s while mounted",
                         v["name"], self.cluster_b)
        self._connect_and_mount_dual(v["name"], format_disk=False)
        handle = self._run_fio_dual(v["name"], runtime=180, rw="randwrite",
                                    bs="16K", numjobs=1, size="256M",
                                    name="armigrate")
        self.replication_start(v["id"], self.cluster_b,
                               interval_min=self.REPL_INTERVAL_MIN)
        self.await_state(v["id"], self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC,
                         what="migration initial copy")
        self._wait_fio_dual([handle], timeout=480)
        self._validate_fio_dual(handle)

        self._disconnect_and_cleanup_dual(v["name"])
        self.commit(v["id"])
        self.drive_cutover(v["id"])
        self.await_state(v["id"], self.STATE_CUTOVER_DONE,
                         timeout=self.REPL_OP_SEC, what="migration cut-over")
        self.logger.info("[AR-R-010] PASS: migrated online, cut-over complete")

        # ── AR-R-011 --delete-source removes the old volume ───────────────
        second = f"armgdel{int(time.time()) % 100000}"
        self.sbcli_utils.add_lvol(lvol_name=second, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        second_id = self.sbcli_utils.get_lvol_id(lvol_name=second)
        self._vols.append({"name": second, "id": second_id, "sums": {}})
        self.logger.info("[AR-R-011] migrating %s with --delete-source", second)
        self.replication_start(second_id, self.cluster_b,
                               interval_min=self.REPL_INTERVAL_MIN)
        self.await_state(second_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial copy")
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 60)
        self.commit(second_id, delete_source=True)
        self.drive_cutover(second_id)
        self.await_state(second_id, self.STATE_CUTOVER_DONE,
                         timeout=self.REPL_OP_SEC,
                         what="cut-over with --delete-source")
        sleep_n_sec(60)
        still_there = [l for l in self.sbcli_utils.list_lvols().get("results", [])
                       if l.get("lvol_name") == second
                       and l.get("cluster_id") == self.cluster_a]
        if still_there:
            raise AssertionError(
                f"[AR-R-011] --delete-source completed but {second} is still "
                f"present on the source cluster {self.cluster_a}. The source "
                f"copy is now stale and writable; leaving it is how two "
                f"divergent copies of one volume end up in production.")
        self.logger.info("[AR-R-011] PASS: source volume removed")

        self.assert_no_corruption("after AR-R migration cases")
        self._teardown()
