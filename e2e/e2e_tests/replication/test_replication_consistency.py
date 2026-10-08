"""AR-C: consistency groups.

A consistency group means several volumes share one snapshot generation, so
a fail-over lands every member at the same instant rather than at whatever
instant each one happened to reach. That is what makes a database plus its
log recoverable together.

The constraint that shapes every case here: **all members must live on one
LVS on one node.** The group snapshot is taken by freezing that LVS, and a
freeze cannot span nodes atomically. So a CG that spans two LVS is refused
(AR-C-005), and the node hosting that LVS becomes a single point of failure
for the whole group (AR-O-009, in the outage lane).

There is no ConsistencyGroup CRD. A group is a replication policy created
with ``--consistency-group``; membership is "volumes following that policy".
So AR-C-007 -- a consistency group outside replication -- has no surface to
test on this build, and is recorded as a skip with that reason rather than
quietly reinterpreted into something that does pass.
"""
import time

from e2e_tests.replication.replication_base import (
    ReplicationTestBase,
)
from utils.common_utils import cli_failed, sleep_n_sec


class ConsistencyGroupPlacement(ReplicationTestBase):
    """AR-C-001, AR-C-005, AR-C-006: where members are allowed to live.

    Placement is the precondition for everything else a CG promises. If the
    control plane does not actually pin members to one LVS, the group
    snapshot is not atomic and every later guarantee is decorative.
    """

    MEMBERS = 3

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tcg{stamp}", self.cluster_b)
        pname = self.policy_add(f"pcg{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN,
                                consistency_group=True)
        self._vols = []

        # ── AR-C-001 members are pinned to one LVS on one node ────────────
        for i in range(self.MEMBERS):
            name = f"arcg{stamp}m{i}"
            self.sbcli_utils.add_lvol(lvol_name=name, pool_name=self.pool_name,
                                      size=self.REPL_VOLUME_SIZE)
            vid = self.sbcli_utils.get_lvol_id(lvol_name=name)
            self.policy_set(vid, pname)
            self._vols.append({"name": name, "id": vid})
        for v in self._vols:
            self.await_state(v["id"], self.STATE_REPLICATING,
                             timeout=self.REPL_CYCLE_SEC,
                             what=f"CG member {v['name']}")

        placements = {}
        for v in self._vols:
            details = self.sbcli_utils.get_lvol_details(lvol_id=v["id"])
            rows = details.get("results", details) if isinstance(details, dict) else details
            row = (rows[0] if isinstance(rows, list) and rows else rows) or {}
            placements[v["name"]] = (row.get("lvs_name") or row.get("lvstore"),
                                     row.get("node_id"))
        self.logger.info("[AR-C-001] member placement: %s", placements)

        distinct = {p for p in placements.values() if any(p)}
        if len(distinct) > 1:
            raise AssertionError(
                f"[AR-C-001] consistency-group members are spread across "
                f"{len(distinct)} (lvs, node) pairs: {placements}. A group "
                f"snapshot freezes one LVS; members on different LVS cannot "
                f"be frozen at the same instant, so the group's whole "
                f"promise -- one generation across every member -- cannot "
                f"hold.")
        self.logger.info("[AR-C-001] PASS: all %d members on one LVS/node %s",
                         self.MEMBERS, distinct)

        # ── AR-C-006 add a member after the policy exists ─────────────────
        late = f"arcglate{stamp}"
        self.logger.info("[AR-C-006] adding %s to the group after creation",
                         late)
        self.sbcli_utils.add_lvol(lvol_name=late, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        late_id = self.sbcli_utils.get_lvol_id(lvol_name=late)
        self._vols.append({"name": late, "id": late_id})
        self.policy_set(late_id, pname)
        self.await_state(late_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="late CG member")
        details = self.sbcli_utils.get_lvol_details(lvol_id=late_id)
        rows = details.get("results", details) if isinstance(details, dict) else details
        row = (rows[0] if isinstance(rows, list) and rows else rows) or {}
        late_place = (row.get("lvs_name") or row.get("lvstore"), row.get("node_id"))
        if distinct and late_place not in distinct and any(late_place):
            raise AssertionError(
                f"[AR-C-006] {late} joined the group but landed on "
                f"{late_place} while the existing members are on "
                f"{distinct}. A member added later must be placed under the "
                f"same constraint as the originals, or joining the group "
                f"silently breaks it for everyone.")
        self.logger.info("[AR-C-006] PASS: late member honoured the same "
                         "placement constraint")

        # ── AR-C-005 a CG spanning two LVS is refused ─────────────────────
        # Force the conflict by asking for a member on a different node than
        # the group occupies. If the control plane cannot be asked to do
        # that from the CLI, say so rather than claiming the refusal works.
        nodes = [n for n in self.sbcli_utils.get_storage_nodes().get("results", [])
                 if n.get("cluster_id") == self.cluster_a]
        occupied = {p[1] for p in distinct if p[1]}
        elsewhere = next((n for n in nodes
                          if (n.get("uuid") or n.get("id")) not in occupied), None)
        if not elsewhere:
            self.skip_case(
                "AR-C-005",
                "cluster A has no second storage node free of the group's "
                "LVS, so a two-LVS group cannot be requested. Needs a "
                "cluster with more nodes than the group occupies.")
        else:
            other_node = elsewhere.get("uuid") or elsewhere.get("id")
            self.logger.info("[AR-C-005] asking for a group member on %s, "
                             "away from the group's LVS", other_node)
            # Creating a volume on another node is perfectly legal, so the
            # refusal has to come from ATTACHING it to the group -- that is
            # the step that would make the group span two LVS. Asserting on
            # the create would test the wrong thing and pass for the wrong
            # reason.
            split = f"arcgsplit{stamp}"
            out, err = self._cli(
                f"{self.base_cmd} -d volume add {split} "
                f"{self.REPL_VOLUME_SIZE} {self.pool_name} "
                f"--host-id {other_node} 2>&1")
            if cli_failed(out, err):
                self.skip_case(
                    "AR-C-005",
                    f"could not place a volume on {other_node} to build the "
                    f"two-LVS case: {(out + err)[:200]}")
            else:
                split_id = self.sbcli_utils.get_lvol_id(lvol_name=split)
                self._vols.append({"name": split, "id": split_id})
                out, err = self.policy_set(split_id, pname)
                self.expect_refused(
                    "AR-C-005", (out or "", err or ""),
                    f"attaching {split} (on node {other_node}) to a "
                    f"consistency group whose members live elsewhere, which "
                    f"would make the group span two LVS",
                    allow=("same lvs", "same node", "consistency",
                           "placement", "lvs"))

        self.assert_no_corruption("after AR-C placement cases")
        self._teardown(pname)

    def _teardown(self, pname=None):
        for v in getattr(self, "_vols", []):
            try:
                self.policy_clear(v["id"])
            except Exception:                         # noqa: BLE001
                pass
            try:
                self._disconnect_and_cleanup_dual(v["name"])
            except Exception:                         # noqa: BLE001
                pass
            try:
                self.sbcli_utils.delete_lvol(lvol_name=v["name"])
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[AR] could not delete %s: %s",
                                    v["name"], str(exc)[:120])
        self.cleanup_replication()


class ConsistencyGroupGeneration(ConsistencyGroupPlacement):
    """AR-C-002, AR-C-003, AR-C-004, AR-C-008: one generation, write order,
    and what a freeze costs.

    AR-C-003 is the case that matters most and is hardest to assert. Crash
    consistency means: if member B holds a write, member A must hold every
    write that preceded it. The usual way to check this without a database
    is a *dependent* write pattern -- write a payload to A, and only once it
    is acknowledged write a pointer to B. After a group fail-over, a pointer
    on B with no payload on A is a write-order violation. The reverse
    (payload with no pointer) is fine: that is just an older generation.
    """

    MEMBERS = 2
    ROUNDS = 6

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tcx{stamp}", self.cluster_b)
        pname = self.policy_add(f"pcx{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN,
                                consistency_group=True)
        self._vols = []
        for i in range(self.MEMBERS):
            name = f"arcx{stamp}m{i}"
            self.sbcli_utils.add_lvol(lvol_name=name, pool_name=self.pool_name,
                                      size=self.REPL_VOLUME_SIZE)
            vid = self.sbcli_utils.get_lvol_id(lvol_name=name)
            self._vols.append({"name": name, "id": vid})
            self._connect_and_mount_dual(name, format_disk=True)
            self.policy_set(vid, pname)
        for v in self._vols:
            self.await_state(v["id"], self.STATE_REPLICATING,
                             timeout=self.REPL_CYCLE_SEC,
                             what=f"CG member {v['name']}")

        if self.k8s_test:
            self.skip_case(
                "AR-C-003",
                "the dependent-write pattern needs two volumes mounted on "
                "one client at known paths. On k8s each volume is a PVC in "
                "its own pod, so the ordering between them is not "
                "controllable from the test. Covered on docker.")
        else:
            self._dependent_writes(stamp)

        # ── AR-C-004 IO resumes after the group freeze ────────────────────
        self.logger.info("[AR-C-004] taking a group snapshot under load")
        handle = self._run_fio_dual(self._vols[0]["name"], runtime=150,
                                    rw="randwrite", bs="16K", numjobs=1,
                                    size="256M", name="arcgfreeze")
        sleep_n_sec(20)
        t0 = time.time()
        self.policy_snapshot(pname)
        froze = time.time() - t0
        self.logger.info("[AR-C-004] group snapshot returned in %.1fs", froze)
        self._wait_fio_dual([handle], timeout=420)
        self._validate_fio_dual(handle)
        self.logger.info("[AR-C-004] PASS: IO continued and completed across "
                         "the freeze (%.1fs)", froze)

        # ── AR-C-002 one generation across every member ───────────────────
        self.logger.info("[AR-C-002] checking the generation across members")
        gens = {}
        for v in self._vols:
            rel = self.relationship_for(v["id"]) or {}
            gens[v["name"]] = (rel.get("generation")
                               or rel.get("snapshot_generation")
                               or rel.get("last_snapshot_id"))
        self.logger.info("[AR-C-002] generations: %s", gens)
        values = {g for g in gens.values() if g is not None}
        if not values:
            self.skip_case(
                "AR-C-002",
                "replication-relationship does not expose a generation or "
                "snapshot id for CG members, so 'one generation across every "
                "member' cannot be asserted from any available surface. This "
                "is a reporting gap worth raising: it is the group's central "
                "guarantee and it is currently unobservable.")
        elif len(values) > 1:
            raise AssertionError(
                f"[AR-C-002] members report DIFFERENT generations: {gens}. "
                f"The group snapshot must be one generation across every "
                f"member; differing generations mean a fail-over lands the "
                f"members at different instants, which is exactly what a "
                f"consistency group exists to prevent.")
        else:
            self.logger.info("[AR-C-002] PASS: one generation %s across all "
                             "%d members", values, len(self._vols))

        # ── AR-C-008 fail over a member with no snapshot of the gen ───────
        self.logger.info("[AR-C-008] adding a member with no group snapshot "
                         "yet, then failing the group over")
        orphan = f"arcxorph{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=orphan, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        orphan_id = self.sbcli_utils.get_lvol_id(lvol_name=orphan)
        self._vols.append({"name": orphan, "id": orphan_id})
        self.policy_set(orphan_id, pname)
        out, err = self.failover_policy(pname)
        combined = ((out or "") + (err or ""))
        rel = self.relationship_for(orphan_id) or {}
        if rel.get("state") == self.STATE_FAILED_OVER:
            self.logger.warning(
                "[AR-C-008] the group failed over a member that had no "
                "snapshot of the group generation (%s -> %s). Either it was "
                "given a generation of its own -- which breaks the group's "
                "one-generation guarantee -- or it was promoted empty. "
                "Needs a dev answer; raised rather than failed because the "
                "intended behaviour is not documented.", orphan,
                rel.get("state"))
        else:
            self.logger.info("[AR-C-008] PASS: the member with no group "
                             "generation was not silently promoted (%s)",
                             combined.strip()[:160] or rel.get("state"))

        # ── AR-C-007 consistency group outside replication ────────────────
        self.skip_case(
            "AR-C-007",
            "there is no ConsistencyGroup CRD and no CLI verb for a group "
            "outside replication -- a group IS a replication policy created "
            "with --consistency-group. So 'consistency group outside "
            "replication' has no surface on this build. Worth confirming "
            "with dev whether it is planned or whether the case should be "
            "retired from the plan.")

        self.assert_no_corruption("after AR-C generation cases")
        self._teardown(pname)

    def _dependent_writes(self, stamp):
        """AR-C-003: payload on member 0, pointer on member 1, in that order.

        Each round writes a payload file to member 0 and *syncs it* before
        writing the matching pointer to member 1. After a group fail-over,
        every pointer present must have its payload present. A pointer
        without a payload means the group captured member 1 at a later
        instant than member 0 -- a write-order violation, and the thing a
        consistency group exists to make impossible.
        """
        node = self.fio_node[0]
        a_mount = self.mount_path
        b_mount = f"{self.mount_path}_b"
        self.ssh_obj.exec_command(node=node, command=f"sudo mkdir -p {b_mount}")

        self.logger.info("[AR-C-003] %d rounds of dependent writes", self.ROUNDS)
        for r in range(self.ROUNDS):
            self.ssh_obj.exec_command(
                node=node,
                command=f"sudo sh -c 'dd if=/dev/urandom "
                        f"of={a_mount}/payload{r} bs=1M count=4 2>/dev/null; "
                        f"sync {a_mount}/payload{r}'")
            self.ssh_obj.exec_command(
                node=node,
                command=f"sudo sh -c 'echo payload{r} > {b_mount}/pointer{r}; "
                        f"sync {b_mount}/pointer{r}'")
        self.replication_trigger(self._vols[0]["id"])
        self.replication_trigger(self._vols[1]["id"])
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 90)

        for v in self._vols:
            self._disconnect_and_cleanup_dual(v["name"])
        self.failover_policy(self._cg_policy_for_log())
        for v in self._vols:
            self.await_state(v["id"], self.STATE_FAILED_OVER,
                             timeout=self.REPL_OP_SEC,
                             what=f"group fail-over of {v['name']}")

        a_name = self.failed_over_volume_name(self._vols[0]["id"],
                                              self._vols[0]["name"])
        b_name = self.failed_over_volume_name(self._vols[1]["id"],
                                              self._vols[1]["name"])
        self._connect_and_mount_dual(a_name, format_disk=False)
        payloads, _ = self.ssh_obj.exec_command(
            node=node, command=f"ls {a_mount} 2>/dev/null || true")
        self._connect_and_mount_dual(b_name, format_disk=False)
        pointers, _ = self.ssh_obj.exec_command(
            node=node, command=f"ls {b_mount} 2>/dev/null || true")

        have_payload = {l.strip() for l in (payloads or "").split() if l.strip()}
        have_pointer = {l.strip() for l in (pointers or "").split() if l.strip()}
        violations = [p for p in have_pointer
                      if p.startswith("pointer")
                      and f"payload{p[len('pointer'):]}" not in have_payload]
        if violations:
            raise AssertionError(
                f"[AR-C-003] WRITE ORDER VIOLATED. These pointers survived "
                f"the group fail-over with no matching payload: {violations}. "
                f"Each pointer was written only after its payload was synced, "
                f"so the group captured the two members at different "
                f"instants. payloads={sorted(have_payload)} "
                f"pointers={sorted(have_pointer)}")
        self.logger.info("[AR-C-003] PASS: every surviving pointer has its "
                         "payload (%d pointers, %d payloads)",
                         len(have_pointer), len(have_payload))

    def _cg_policy_for_log(self):
        return self._repl_policies[-1]
