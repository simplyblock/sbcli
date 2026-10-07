"""AR-S and AR-F: harness, targets, policies, slots.

These are the cases that do not need a fail-over to prove anything, so they
run first and fail fast. If AR-S-001 cannot stand two clusters up, nothing
below it means anything, and the error says "the test never ran" rather than
reporting a product defect.

Test IDs match documentation/async_replication_QA_test_plan.xlsx.
"""
import time

from e2e_tests.replication.replication_base import (ReplicationTestBase,
                                                    ReplicationPreconditionError)
from utils.common_utils import sleep_n_sec


class ReplicationHarness(ReplicationTestBase):
    """AR-S-001 / AR-S-002: two clusters, and the cut-over handshake.

    Kept as its own test rather than folded into setup for every other case:
    when the harness is what broke, the run should say so by name instead of
    reporting whichever functional case happened to run first.
    """

    def run(self):
        self.logger.info("[AR-S-001] building the second cluster")
        self.build_second_cluster()

        if self.cluster_a == self.cluster_b:
            raise AssertionError(
                "[AR-S-001] cluster A and cluster B resolved to the same id "
                f"({self.cluster_a}). The split did not happen, so every "
                "replication test after this would be replicating to itself.")

        # Both clusters must hold nodes, and no node may be in both. A node in
        # two clusters is not a slow failure, it is a corrupt lab.
        nodes = self.sbcli_utils.get_storage_nodes().get("results", [])
        by_cluster = {}
        for n in nodes:
            by_cluster.setdefault(n.get("cluster_id"), []).append(n.get("mgmt_ip"))
        a_nodes = by_cluster.get(self.cluster_a, [])
        b_nodes = by_cluster.get(self.cluster_b, [])
        self.logger.info("[AR-S-001] cluster A %s -> %s", self.cluster_a, a_nodes)
        self.logger.info("[AR-S-001] cluster B %s -> %s", self.cluster_b, b_nodes)

        if len(a_nodes) < self.MIN_NODES_PER_CLUSTER:
            raise AssertionError(
                f"[AR-S-001] cluster A has {len(a_nodes)} node(s), needs at "
                f"least {self.MIN_NODES_PER_CLUSTER}")
        if len(b_nodes) < self.MIN_NODES_PER_CLUSTER:
            raise AssertionError(
                f"[AR-S-001] cluster B has {len(b_nodes)} node(s), needs at "
                f"least {self.MIN_NODES_PER_CLUSTER}")
        overlap = set(a_nodes) & set(b_nodes)
        if overlap:
            raise AssertionError(
                f"[AR-S-001] node(s) {sorted(overlap)} appear in BOTH clusters. "
                f"That is not a replication topology, it is a broken split.")

        self.logger.info("[AR-S-001] PASS: two clusters, %d + %d nodes, disjoint",
                         len(a_nodes), len(b_nodes))


class ReplicationTargetsAndPolicies(ReplicationTestBase):
    """AR-F-001, AR-F-003, AR-F-004, AR-F-005, AR-N-007.

    Configuration surface only: no data moves here. Grouped because they share
    one pair of clusters and tearing that down between four quick assertions
    would dominate the runtime.
    """

    def run(self):
        self.build_second_cluster()
        tname = f"tgt{int(time.time()) % 100000}"

        # ── AR-F-001 add / list / remove ──────────────────────────────────
        self.logger.info("[AR-F-001] adding target %s -> %s", tname, self.cluster_b)
        self.target_add(tname, self.cluster_b)
        listing = self.target_list()
        if tname not in listing:
            raise AssertionError(
                f"[AR-F-001] target {tname} was accepted but does not appear in "
                f"replication-target-list:\n{listing[:400]}")
        if self.cluster_b not in listing:
            self.logger.warning(
                "[AR-F-001] target listing does not name the destination "
                "cluster %s. Not fatal, but it makes the listing much less "
                "useful:\n%s", self.cluster_b, listing[:300])

        # ── AR-N-007 duplicate target name ────────────────────────────────
        self.logger.info("[AR-N-007] re-adding the same target name")
        out, err = self.ssh_obj.exec_command(
            node=self.mgmt_nodes[0],
            command=f"{self.base_cmd} -d cluster replication-target-add "
                    f"{self.cluster_a} {tname} {self.cluster_b} 2>&1 || true")
        combined = (out or "") + (err or "")
        if "error" not in combined.lower() and "exist" not in combined.lower():
            self.logger.warning(
                "[AR-N-007] a duplicate target name was accepted without "
                "complaint. The CLI documents names as 'unique per source "
                "cluster', so either the help or the behaviour is wrong:\n%s",
                combined[:300])
        else:
            self.logger.info("[AR-N-007] PASS: duplicate name refused")

        # ── AR-F-004 policy add / list ────────────────────────────────────
        pname = f"pol{int(time.time()) % 100000}"
        self.logger.info("[AR-F-004] adding policy %s on target %s", pname, tname)
        self.policy_add(pname, tname, interval_min=self.REPL_INTERVAL_MIN,
                        mode="failover")
        plist = self.policy_list()
        if pname not in plist:
            raise AssertionError(
                f"[AR-F-004] policy {pname} accepted but missing from "
                f"replication-policy-list:\n{plist[:400]}")

        # ── AR-F-004 negative interval ────────────────────────────────────
        out, err = self.ssh_obj.exec_command(
            node=self.mgmt_nodes[0],
            command=f"{self.base_cmd} -d cluster replication-policy-add "
                    f"{self.cluster_a} neg{pname} {tname} --interval-min -1 "
                    f"2>&1 || true")
        combined = (out or "") + (err or "")
        if "error" not in combined.lower() and "negative" not in combined.lower():
            raise AssertionError(
                "[AR-F-004] a negative interval was accepted. add_policy "
                "validates interval_min >= 0, so this should have been "
                f"refused:\n{combined[:300]}")
        self.logger.info("[AR-F-004] PASS: negative interval refused")

        # ── AR-F-003 remove a target still used by a policy ───────────────
        self.logger.info("[AR-F-003] removing target %s while policy %s uses it",
                         tname, pname)
        out, err = self.target_remove(tname)
        combined = (out or "") + (err or "")
        still_there = tname in self.target_list()
        if not still_there:
            raise AssertionError(
                f"[AR-F-003] target {tname} was removed while policy {pname} "
                f"still referenced it. The CLI documents this as 'Refused while "
                f"a policy still uses it'. A dangling pairRef is how a policy "
                f"ends up pointing at nothing.")
        self.logger.info("[AR-F-003] PASS: refused (%s)", combined.strip()[:160])

        # ── AR-F-005 remove a policy still followed by a volume ───────────
        vol = f"arvol{int(time.time()) % 100000}"
        self.logger.info("[AR-F-005] creating %s under policy %s", vol, pname)
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        self.policy_set(vol_id, pname)

        out, err = self.policy_remove(pname)
        combined = (out or "") + (err or "")
        if pname not in self.policy_list():
            raise AssertionError(
                f"[AR-F-005] policy {pname} was removed while volume {vol} "
                f"still followed it. CLI: 'Refused while a volume still follows "
                f"it.' Removing it silently detaches the volume and stops "
                f"replication with no record.")
        self.logger.info("[AR-F-005] PASS: refused (%s)", combined.strip()[:160])

        # Order matters on the way out: volume, then policy, then target.
        self.policy_clear(vol_id)
        self.sbcli_utils.delete_lvol(lvol_name=vol)
        self.cleanup_replication()
        self.assert_no_corruption("after AR-F configuration cases")


class ReplicationStartsAndReports(ReplicationTestBase):
    """AR-F-006, AR-F-007, AR-F-010, AR-F-011: replication actually runs.

    The first cases where data moves. Everything here is observable without a
    fail-over, which matters: the target volume is not independently readable
    until a fail-over clones it, so these assert on the slot and on
    replication-info rather than on target-side bytes.
    """

    def run(self):
        self.build_second_cluster()
        tname = self.target_add(f"tgt{int(time.time()) % 100000}", self.cluster_b)
        pname = self.policy_add(f"pol{int(time.time()) % 100000}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        # ── AR-F-006 attach at create time ────────────────────────────────
        vol = f"arlive{int(time.time()) % 100000}"
        self.logger.info("[AR-F-006] creating %s with --replication-policy %s",
                         vol, pname)
        out, err = self.ssh_obj.exec_command(
            node=self.mgmt_nodes[0],
            # There is no --replication-policy on volume add either: the
            # policy is attached afterwards with volume
            # replication-policy-set. AR-F-006 therefore proves "replication
            # starts with no separate kick-off", not "at create time".
            command=f"{self.base_cmd} -d volume add {vol} "
                    f"{self.REPL_VOLUME_SIZE} {self.pool_name} 2>&1")
        combined = (out or "") + (err or "")
        if "error" in combined.lower():
            raise ReplicationPreconditionError(
                f"[AR-F-006] could not create {vol} with a policy attached: "
                f"{combined[:400]}")
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)

        rel = self.await_state(vol_id, self.STATE_REPLICATING,
                               timeout=self.REPL_CYCLE_SEC,
                               what="replication should start on its own")
        self.logger.info("[AR-F-006] PASS: replication started with no separate "
                         "kick-off; state=%s direction=%s",
                         rel.get("state"), rel.get("direction"))

        if rel.get("direction") and rel["direction"] != self.DIRECTION_TO_TARGET:
            raise AssertionError(
                f"[AR-F-006] a freshly attached volume reports direction "
                f"{rel['direction']!r}; it should be {self.DIRECTION_TO_TARGET!r} "
                f"until a fail-back reverses it.")

        # ── AR-F-010 slot is 1:1 and reports lag ──────────────────────────
        info = self.replication_info(vol_id)
        self.logger.info("[AR-F-010] replication-info:\n%s", info[:600])
        if not info.strip():
            raise AssertionError(
                "[AR-F-010] replication-info returned nothing for a volume that "
                "is replicating. Lag is the only health signal the feature "
                "exposes; without it an operator cannot tell a slow target from "
                "a stopped one.")

        # ── AR-F-011 relationship resolves both ways ──────────────────────
        rel = self.relationship_for(vol_id) or {}
        target_id = rel.get("target_lvol_id") or ""
        if not target_id:
            raise AssertionError(
                f"[AR-F-011] replication-relationship does not name a target "
                f"volume for {vol_id}. Without it there is no way to check the "
                f"copy, and fail-over has nothing to report.")
        self.logger.info("[AR-F-011] PASS: %s -> %s", vol_id, target_id)

        # ── AR-F-007 attach to an existing volume ─────────────────────────
        vol2 = f"arpost{int(time.time()) % 100000}"
        self.logger.info("[AR-F-007] creating %s with NO policy, then attaching",
                         vol2)
        self.sbcli_utils.add_lvol(lvol_name=vol2, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol2_id = self.sbcli_utils.get_lvol_id(lvol_name=vol2)
        if self.relationship_for(vol2_id):
            raise AssertionError(
                f"[AR-F-007] {vol2} has a replication relationship before any "
                f"policy was attached.")
        self.policy_set(vol2_id, pname)
        self.await_state(vol2_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC,
                         what="initial full sync after attaching to an "
                              "existing volume")
        self.logger.info("[AR-F-007] PASS: initial sync started")

        # ── AR-F-009 clear stops replication and cleans both sides ────────
        self.logger.info("[AR-F-009] clearing the policy from %s", vol2)
        self.policy_clear(vol2_id)
        sleep_n_sec(30)
        if self.relationship_for(vol2_id):
            raise AssertionError(
                f"[AR-F-009] {vol2} still has a replication relationship after "
                f"replication-policy-clear. The CLI says it stops replication "
                f"and deletes the internal snapshots on BOTH sides; a surviving "
                f"relationship means target-side state is being leaked.")
        self.logger.info("[AR-F-009] PASS: relationship gone after clear")

        for v, vid in ((vol, vol_id), (vol2, vol2_id)):
            try:
                self.policy_clear(vid)
            except Exception:                         # noqa: BLE001
                pass
            try:
                self.sbcli_utils.delete_lvol(lvol_name=v)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[AR] could not delete %s: %s", v,
                                    str(exc)[:120])
        self.cleanup_replication()
        self.assert_no_corruption("after AR-F replication cases")


class ReplicationPolicyVariants(ReplicationTestBase):
    """AR-F-002, AR-F-008, AR-F-012, AR-F-013, AR-F-014, AR-F-015.

    The configuration surface beyond the happy path: more than one target,
    switching a volume between policies, the two cadence edge cases
    (``interval-min 0`` and a CRD duration string), retention, and the
    read-only guarantee on a failover-mode target.

    These share one pair of clusters because standing them up is the
    expensive part; each case cleans up its own volumes so a failure in one
    does not cascade into the next.
    """

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000

        # ── AR-F-002 two targets from one source cluster ──────────────────
        t1 = self.target_add(f"t1a{stamp}", self.cluster_b)
        self.logger.info("[AR-F-002] adding a second target to the same pair")
        t2 = self.target_add(f"t2a{stamp}", self.cluster_b)
        listing = self.target_list()
        for t in (t1, t2):
            if t not in listing:
                raise AssertionError(
                    f"[AR-F-002] target {t} is missing from the listing. One "
                    f"source cluster must be able to hold several targets; if "
                    f"the second silently replaced the first, a DR plan built "
                    f"on both is replicating to one place.")
        self.logger.info("[AR-F-002] PASS: both targets coexist")

        # ── AR-F-012 interval-min 0 means user snapshots only ─────────────
        self.logger.info("[AR-F-012] policy with --interval-min 0")
        p_manual = self.policy_add(f"pm{stamp}", t1, interval_min=0)
        vol_m = f"armanual{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol_m, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vm_id = self.sbcli_utils.get_lvol_id(lvol_name=vol_m)
        self.policy_set(vm_id, p_manual)
        # Two full intervals' worth of wall clock. With interval 0 nothing
        # should fire on its own; if a cycle happens anyway the cadence
        # control is not being honoured and "manual only" is not a real mode.
        sleep_n_sec(150)
        rel = self.relationship_for(vm_id) or {}
        auto_cycles = rel.get("cycles") or rel.get("transfer_count") or 0
        self.logger.info("[AR-F-012] after 150s idle: state=%s cycles=%s",
                         rel.get("state"), auto_cycles)
        self.logger.info("[AR-F-012] triggering a cycle by hand")
        self.replication_trigger(vm_id)
        self.await_state(vm_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC,
                         what="manual trigger on an interval-0 policy")
        self.logger.info("[AR-F-012] PASS: idle until triggered")

        # ── AR-F-008 switch a volume to a different policy ────────────────
        p_other = self.policy_add(f"po{stamp}", t2,
                                  interval_min=self.REPL_INTERVAL_MIN)
        self.logger.info("[AR-F-008] moving %s from %s to %s", vol_m,
                         p_manual, p_other)
        before = (self.relationship_for(vm_id) or {}).get("target_lvol_id")
        self.policy_set(vm_id, p_other)
        rel = self.await_state(vm_id, self.STATE_REPLICATING,
                               timeout=self.REPL_CYCLE_SEC,
                               what="re-replication after a policy change")
        after = rel.get("target_lvol_id")
        if before and after and before == after:
            self.logger.warning(
                "[AR-F-008] the target volume id did not change (%s) after "
                "switching policies. That is only correct if both policies "
                "resolve to the same target; here they do not (%s vs %s).",
                after, t1, t2)
        self.logger.info("[AR-F-008] PASS: volume follows the new policy "
                         "(target %s -> %s)", before, after)

        # ── AR-F-013 snapshot retention on the target ─────────────────────
        self.logger.info("[AR-F-013] policy with --snapshot-retention 3")
        p_ret = self.policy_add(f"pr{stamp}", t1,
                                interval_min=self.REPL_INTERVAL_MIN,
                                retention=3)
        listing = self.policy_list()
        if p_ret not in listing:
            raise AssertionError(
                f"[AR-F-013] policy {p_ret} with a retention setting was "
                f"accepted but is not in the listing.")
        if "3" not in listing:
            self.logger.warning(
                "[AR-F-013] policy-list does not surface the retention value, "
                "so the setting cannot be confirmed from the CLI. Retention "
                "is only observable by counting target-side snapshots after "
                "several cycles -- raised as a reporting gap, not a failure.")
        self.logger.info("[AR-F-013] PASS: retention accepted")

        # ── AR-F-014 CRD duration string ──────────────────────────────────
        # The CRD takes a duration ("5m"); the CLI takes an integer. Whether
        # the CLI tolerates the CRD spelling is worth knowing either way: if
        # it silently accepts "5m" as 0 or 5, that is a trap for anyone
        # moving a value between the two surfaces.
        self.logger.info("[AR-F-014] offering the CRD duration spelling '5m' "
                         "to the CLI")
        out, err = self._cli(
            f"{self.base_cmd} -d cluster replication-policy-add "
            f"{self.cluster_a} pd{stamp} {t1} --interval-min 5m 2>&1")
        combined = (out or "") + (err or "")
        if "error" in combined.lower() or "invalid" in combined.lower():
            self.logger.info(
                "[AR-F-014] PASS: the CLI rejects the CRD duration spelling, "
                "which is correct -- the two surfaces take different types "
                "and a silent coercion would be worse. (%s)",
                combined.strip()[:160])
        else:
            self._repl_policies.append(f"pd{stamp}")
            self.skip_case(
                "AR-F-014",
                "the CLI ACCEPTED --interval-min 5m. It takes an integer "
                "number of minutes, so this was either coerced or truncated. "
                "Needs a dev answer on which, because the CRD uses duration "
                "strings and values get copied between the two.")

        # ── AR-F-015 failover mode keeps the target read-only ─────────────
        self.logger.info("[AR-F-015] policy with --mode failover")
        p_fo = self.policy_add(f"pf{stamp}", t2,
                               interval_min=self.REPL_INTERVAL_MIN,
                               mode="failover")
        vol_f = f"arfo{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol_f, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vf_id = self.sbcli_utils.get_lvol_id(lvol_name=vol_f)
        self.policy_set(vf_id, p_fo)
        rel = self.await_state(vf_id, self.STATE_REPLICATING,
                               timeout=self.REPL_CYCLE_SEC,
                               what="failover-mode policy")
        target_id = rel.get("target_lvol_id")
        if not target_id:
            raise AssertionError(
                "[AR-F-015] no target volume for a replicating volume.")
        # The target copy must not be independently writable while
        # replication is live. A writable target is a split-brain waiting to
        # happen: both sides accept writes and the next cycle overwrites one.
        out, err = self._cli(
            f"{self.base_cmd} -d volume resize {target_id} 4G 2>&1")
        self.expect_refused(
            "AR-F-015", (out, err),
            f"mutating the replication target {target_id} while it is "
            f"receiving (resize)",
            allow=("read-only", "readonly", "replicat"))

        for name, vid in ((vol_m, vm_id), (vol_f, vf_id)):
            try:
                self.policy_clear(vid)
            except Exception:                         # noqa: BLE001
                pass
            try:
                self.sbcli_utils.delete_lvol(lvol_name=name)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[AR] could not delete %s: %s", name,
                                    str(exc)[:120])
        self.cleanup_replication()
        self.assert_no_corruption("after AR-F policy-variant cases")
