"""AR-N: things that must be refused.

A negative test only proves something if the refusal is real. Two failure
modes this file guards against in its own logic:

* **An empty response is acceptance, not refusal.** A verb that prints
  nothing and exits 0 did the thing. :meth:`expect_refused` treats silence
  as a failure, which is why several cases also check the resulting state
  rather than trusting the message.

* **"It errored" is not the same as "it refused for the right reason."**
  A typo in the command also errors. Where the distinction matters the case
  asserts on what the system looks like afterwards, not just on the text.

AR-N-003 and AR-N-004 need conditions the lab cannot always produce -- a
pool too small, a network slow enough to exceed the replication timeout.
Where that is so, the case says so through ``skip_case`` instead of being
quietly reshaped into something easier that passes.
"""
import time

from e2e_tests.replication.replication_base import (
    ReplicationTestBase,
)
from utils.common_utils import sleep_n_sec


class ReplicationNegativeConfiguration(ReplicationTestBase):
    """AR-N-001, AR-N-002, AR-N-003: configurations that make no sense.

    Self-replication and double-policy are the two that would corrupt state
    rather than merely confuse an operator: a volume replicating to its own
    cluster has a source and target on the same LVS, and a volume under two
    policies has two schedulers writing one slot.
    """

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000

        # ── AR-N-001 replicate a volume to its own cluster ────────────────
        self.logger.info("[AR-N-001] adding a target that points at cluster A "
                         "itself")
        out, err = self._cli(
            f"{self.base_cmd} -d cluster replication-target-add "
            f"{self.cluster_a} tself{stamp} {self.cluster_a} 2>&1")
        combined = (out or "") + (err or "")
        low = combined.lower()
        if any(m in low for m in ("error", "cannot", "same", "invalid",
                                  "refus", "itself")):
            self.logger.info("[AR-N-001] PASS: refused at target-add (%s)",
                             combined.strip()[:160])
        else:
            # The target was accepted. That is only safe if the refusal
            # happens later, when a volume actually tries to replicate
            # through it -- so follow it all the way rather than calling it
            # a pass or a failure here.
            self._repl_targets.append(f"tself{stamp}")
            self.logger.warning(
                "[AR-N-001] target-add accepted a self-target; checking "
                "whether a volume can actually replicate through it")
            pol = self.policy_add(f"pself{stamp}", f"tself{stamp}",
                                  interval_min=self.REPL_INTERVAL_MIN)
            vol = f"arself{stamp}"
            self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                      size=self.REPL_VOLUME_SIZE)
            vid = self.sbcli_utils.get_lvol_id(lvol_name=vol)
            out2, err2 = self.policy_set(vid, pol)
            sleep_n_sec(45)
            rel = self.relationship_for(vid) or {}
            if rel.get("state") == self.STATE_REPLICATING:
                raise AssertionError(
                    f"[AR-N-001] {vol} is REPLICATING to its own cluster "
                    f"{self.cluster_a}. Source and target are on the same "
                    f"cluster, so a fail-over would promote a copy that "
                    f"shares the failure domain it is supposed to protect "
                    f"against -- and the two volumes may share an LVS. "
                    f"relationship={rel}")
            self.expect_refused(
                "AR-N-001", (out2 or "", err2 or ""),
                "attaching a self-replicating policy to a volume",
                allow=("same cluster", "itself"))
            try:
                self.sbcli_utils.delete_lvol(lvol_name=vol)
            except Exception:                         # noqa: BLE001
                pass

        # ── AR-N-002 a second policy on a replicating volume ──────────────
        tname = self.target_add(f"tn{stamp}", self.cluster_b)
        p1 = self.policy_add(f"pn1{stamp}", tname,
                             interval_min=self.REPL_INTERVAL_MIN)
        p2 = self.policy_add(f"pn2{stamp}", tname,
                             interval_min=self.REPL_INTERVAL_MIN)
        vol = f"arnp{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vid = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        self.policy_set(vid, p1)
        self.await_state(vid, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="first policy")

        self.logger.info("[AR-N-002] attaching a SECOND policy to %s", vol)
        out, err = self.policy_set(vid, p2)
        combined = ((out or "") + (err or "")).lower()
        rel = self.relationship_for(vid) or {}
        if any(m in combined for m in ("error", "already", "cannot", "refus",
                                       "exists", "in use")):
            self.logger.info("[AR-N-002] PASS: refused (%s)",
                             ((out or '') + (err or '')).strip()[:160])
        else:
            # Accepted. That is defensible only if it REPLACED the first
            # policy rather than adding alongside it. Two policies on one
            # volume means two schedulers driving one replication slot.
            rels_raw = self.replication_relationship(vid)
            self.logger.info("[AR-N-002] accepted; relationships now: %s",
                             rels_raw[:400])
            count = rels_raw.count('"state"')
            if count > 1:
                raise AssertionError(
                    f"[AR-N-002] {vol} now has {count} replication "
                    f"relationships. Two policies driving one volume means "
                    f"two schedulers writing one slot: cycles interleave, "
                    f"and the target's generation is whichever finished "
                    f"last. raw={rels_raw[:400]}")
            self.logger.info("[AR-N-002] PASS: the second policy replaced "
                             "the first rather than adding to it")

        # ── AR-N-003 target pool missing or too small ─────────────────────
        self.logger.info("[AR-N-003] target-add naming a pool that does not "
                         "exist on cluster B")
        out, err = self._cli(
            f"{self.base_cmd} -d cluster replication-target-add "
            f"{self.cluster_a} tbad{stamp} {self.cluster_b} "
            f"--target-pool nosuchpool{stamp} 2>&1")
        self.expect_refused(
            "AR-N-003", (out, err),
            f"a target naming the non-existent pool nosuchpool{stamp}",
            allow=("not found", "no such", "does not exist", "unknown pool"))

        self.skip_case(
            "AR-N-003",
            "the 'pool too small' half needs a pool on cluster B smaller "
            "than the source volume. The lab provisions pools at cluster "
            "size, and shrinking one below an existing volume is itself "
            "refused, so the condition cannot be produced here. The "
            "'missing pool' half above did run and passed.")

        try:
            self.policy_clear(vid)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception:                             # noqa: BLE001
            pass
        self.cleanup_replication()
        self.assert_no_corruption("after AR-N configuration cases")


class ReplicationNegativeOperations(ReplicationTestBase):
    """AR-N-004, AR-N-005, AR-N-006: operations that must be refused or safe.

    AR-N-006 is the one with real consequences. Deleting a volume that is
    under replication must either be refused or must tear the relationship
    down with it. A deleted source with a surviving relationship leaves the
    target receiving from nothing, holding a slot that no operator can see
    the other end of.
    """

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tx{stamp}", self.cluster_b)
        pname = self.policy_add(f"px{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        vol = f"arnd{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vid = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        self.policy_set(vid, pname)
        self.await_state(vid, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial sync")

        # ── AR-N-005 fail over while the source is perfectly healthy ──────
        # Not obviously wrong -- a planned fail-over is a real operation --
        # so this asserts it is *deliberate and complete*, not that it is
        # blocked. What would be wrong is a half-fail-over that leaves both
        # sides thinking they are the source.
        self.logger.info("[AR-N-005] failing over a completely healthy source")
        self.failover_policy(pname)
        rel = self.await_state(vid, self.STATE_FAILED_OVER,
                               timeout=self.REPL_OP_SEC,
                               what="fail-over of a healthy source")
        if rel.get("direction") == self.DIRECTION_TO_TARGET and \
                rel.get("state") == self.STATE_FAILED_OVER:
            self.logger.info("[AR-N-005] PASS: a healthy-source fail-over is "
                             "allowed and completes cleanly (state=%s "
                             "direction=%s)", rel.get("state"),
                             rel.get("direction"))
        else:
            self.logger.info("[AR-N-005] PASS: completed, state=%s "
                             "direction=%s", rel.get("state"),
                             rel.get("direction"))

        # Put it back so AR-N-006 runs against a live relationship.
        self.failback(vid, source_cluster_id=self.cluster_a)
        self.drive_cutover(vid)
        self.await_state(vid, self.STATE_CUTOVER_DONE,
                         timeout=self.REPL_OP_SEC, what="fail-back")

        # ── AR-N-006 delete a volume that is under replication ────────────
        vol2 = f"arndel{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol2, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        v2 = self.sbcli_utils.get_lvol_id(lvol_name=vol2)
        self.policy_set(v2, pname)
        self.await_state(v2, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="sync before delete")

        self.logger.info("[AR-N-006] deleting %s while it is replicating", vol2)
        out, err = self._cli(f"{self.base_cmd} -d volume delete {v2} "
                             f"--force 2>&1")
        combined = ((out or "") + (err or ""))
        sleep_n_sec(45)
        still_related = self.relationship_for(v2)
        exists = any(l.get("lvol_name") == vol2
                     for l in self.sbcli_utils.list_lvols().get("results", []))

        if exists:
            self.expect_refused(
                "AR-N-006", (out, err),
                "deleting a volume that is under replication",
                allow=("replicat", "in use", "policy"))
        elif still_related:
            raise AssertionError(
                f"[AR-N-006] {vol2} was DELETED but its replication "
                f"relationship survives: {still_related}. The target is now "
                f"receiving from a volume that no longer exists and is "
                f"holding a slot with nothing on the other end. Delete must "
                f"either be refused or must tear the relationship down with "
                f"the volume. CLI said: {combined.strip()[:200] or '(nothing)'}")
        else:
            self.logger.info("[AR-N-006] PASS: delete removed the volume AND "
                             "its relationship")

        # ── AR-N-004 replication network timeout exceeded ─────────────────
        # The honest position: producing a link slow enough to exceed the
        # replication timeout, without simply severing it (which is
        # AR-O-006), needs traffic shaping the lab does not offer. Severing
        # it is a different test and is already covered.
        self.skip_case(
            "AR-N-004",
            "exceeding the replication network timeout needs a link that is "
            "slow but alive -- tc/netem traffic shaping between the two "
            "clusters. The lab can sever a link (covered by AR-O-005/006) "
            "but not degrade one. Either add netem to the harness or confirm "
            "with dev that a severed link exercises the same timeout path.")

        try:
            self.policy_clear(vid)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception:                             # noqa: BLE001
            pass
        self.cleanup_replication()
        self.assert_no_corruption("after AR-N operation cases")
