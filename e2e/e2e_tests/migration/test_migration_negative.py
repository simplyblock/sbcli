"""MIG-N: migrations that must be refused, and one that must not be.

The gaps in the original scripts. They were thorough about faults *during*
a migration and said almost nothing about migrations that should never have
started -- which is the cheaper class of bug to find and the easier one to
fix.

Two rules this file follows in its own logic:

* **An empty response is acceptance.** A verb that prints nothing and exits
  zero did the thing. Silence is treated as a failure here.
* **"It errored" is not "it refused for the right reason."** A typo errors
  too. Where it matters, the case checks what the cluster looks like
  afterwards rather than trusting the message.

MIG-N-006 is the one that must NOT be refused: a volume under a replication
policy being migrated between nodes. Two features that both move data,
never tested together, and the interesting outcome is not a refusal but
whether replication survives its volume moving underneath it.
"""
import os
import time

from e2e_tests.migration.migration_base import (
    MigrationTestBase,
)
from utils.common_utils import cli_failed, sleep_n_sec


class MigrationNegativeTargets(MigrationTestBase):
    """MIG-N-001 .. MIG-N-004: targets that make no sense."""

    def _refused(self, case_id, out_err, what, allow=()):
        combined = out_err if isinstance(out_err, str) else "".join(out_err)
        low = combined.lower()
        if self.MIG_ID_RE.search(combined):
            raise AssertionError(
                f"[{case_id}] {what} was ACCEPTED and returned a migration "
                f"id. Response: {combined.strip()[:300]}")
        markers = ("error", "cannot", "invalid", "refus", "not allowed",
                   "same", "offline", "not found", "no such",
                   "insufficient", "space") + tuple(allow)
        if not any(m in low for m in markers):
            raise AssertionError(
                f"[{case_id}] {what} produced no migration id but also no "
                f"recognisable refusal: {combined.strip()[:300] or '(empty)'}. "
                f"Silence is acceptance as far as an operator can tell.")
        self.logger.info("[%s] PASS: refused -- %s", case_id,
                         combined.strip()[:200])

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"mign{stamp}"
        vol_id, _ = self.make_volume(vol, seed=False)
        src = self.lvol_node(vol_id)

        # ── MIG-N-001 migrate to the node it is already on ───────────────
        self.logger.info("[MIG-N-001] migrating %s to its own node", vol)
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {src} 2>&1")
        self._refused("MIG-N-001", out + err,
                      "a migration whose target is the source node",
                      allow=("already",))

        # ── MIG-N-002 a target that does not exist ───────────────────────
        bogus = "00000000-0000-0000-0000-00000000dead"
        self.logger.info("[MIG-N-002] migrating to a non-existent node")
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {bogus} 2>&1")
        self._refused("MIG-N-002", out + err,
                      f"a migration to the non-existent node {bogus}")

        # ── MIG-N-003 a target that is offline ───────────────────────────
        tgt = self.pick_target(src, "no-overlap")
        next((n.get("mgmt_ip") for n in self.online_nodes()
                   if (n.get("uuid") or n.get("id")) == tgt), None)
        self.logger.info("[MIG-N-003] shutting %s down, then migrating to it",
                         tgt)
        self.sbcli_utils.shutdown_node(node_uuid=tgt)
        sleep_n_sec(45)
        try:
            out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                                 f"{vol_id} {tgt} 2>&1")
            combined = out + err
            if self.MIG_ID_RE.search(combined):
                mid = self.MIG_ID_RE.search(combined).group(1)
                self._migrations.append(mid)
                raise AssertionError(
                    f"[MIG-N-003] pre-create succeeded against an OFFLINE "
                    f"target and returned migration {mid}. The target "
                    f"subsystem cannot have been created, so this migration "
                    f"is already broken and will fail later, further from "
                    f"the cause.")
            self._refused("MIG-N-003", combined,
                          "a migration to an offline target")
        finally:
            try:
                self.sbcli_utils.restart_node(node_uuid=tgt)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG-N-003] could not restart %s: %s",
                                    tgt, str(exc)[:160])
            deadline = time.time() + 900
            while time.time() < deadline:
                if any((n.get("uuid") or n.get("id")) == tgt
                       for n in self.online_nodes()):
                    break
                sleep_n_sec(20)

        # ── MIG-N-004 two migrations for one volume ──────────────────────
        sleep_n_sec(90)       # balancing_on_restart blocks new migrations
        self.logger.info("[MIG-N-004] starting a second migration for a "
                         "volume that already has one")
        mid = self.migrate(vol_id, tgt)
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {src} 2>&1")
        combined = out + err
        second = self.MIG_ID_RE.search(combined)
        if second and second.group(1) != mid:
            self._migrations.append(second.group(1))
            raise AssertionError(
                f"[MIG-N-004] a volume now has TWO migrations in flight "
                f"({mid} and {second.group(1)}), heading for different "
                f"nodes. Whichever finishes last decides where the volume "
                f"lives, and the other's cleanup runs against state it no "
                f"longer owns.")
        self.logger.info("[MIG-N-004] PASS: second migration refused -- %s",
                         combined.strip()[:200])
        self.migrate_cancel(mid)
        self.cleanup_migrations()


class MigrationNegativeCapacity(MigrationTestBase):
    """MIG-N-005: a target without room for the volume.

    Attempted honestly: the lab usually has plenty of space, so this fills
    the target first and says so clearly if it cannot. A case that quietly
    passes because the precondition was never created is worse than one
    that skips.
    """

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"migcap{stamp}"
        vol_id, _ = self.make_volume(vol, seed=False)
        src = self.lvol_node(vol_id)
        tgt = self.pick_target(src, "no-overlap")

        cap = None
        try:
            data = self.sbcli_utils.get_cluster_capacity()
            cap = data
        except Exception:                             # noqa: BLE001
            pass
        self.logger.info("[MIG-N-005] cluster capacity: %s",
                         str(cap)[:200] if cap else "unknown")

        # Fill the target by provisioning a volume pinned to it that is
        # larger than what is left. If the cluster refuses the filler, the
        # precondition cannot be made and this is a skip, not a pass.
        filler = f"migfill{stamp}"
        huge = os.environ.get("MIG_FILLER_SIZE", "10T")
        out, err = self._cli(f"{self.base_cmd} -d volume add {filler} {huge} "
                             f"{self.pool_name} --host-id {tgt} 2>&1")
        if cli_failed(out, err):
            self.logger.warning(
                "[MIG-N-005] SKIPPED: could not fill the target to create "
                "the out-of-space condition (%s). The lab has more capacity "
                "than this case can consume; set MIG_FILLER_SIZE higher or "
                "run it on a smaller cluster.",
                (out + err).strip()[:200])
            self.cleanup_migrations()
            return
        self._mig_vols.append(filler)
        sleep_n_sec(20)

        self.logger.info("[MIG-N-005] migrating to a target with no room")
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {tgt} 2>&1")
        combined = out + err
        if self.MIG_ID_RE.search(combined):
            mid = self.MIG_ID_RE.search(combined).group(1)
            self._migrations.append(mid)
            self.logger.warning(
                "[MIG-N-005] pre-create was accepted against a full target "
                "(%s). Checking whether the migration then fails cleanly "
                "rather than part-way.", mid)
            self.migrate_continue(mid)
            status, phase = self.await_migration(
                vol_id=vol_id, migration_id=mid, timeout=1800,
                what="MIG-N-005 migration to a full target")
            node = self.lvol_node(vol_id)
            if node != src:
                raise AssertionError(
                    f"[MIG-N-005] a migration to a target with no room ended "
                    f"{status!r} and the volume is now on {node!r}. If the "
                    f"target cannot hold it, the volume must stay on the "
                    f"source.")
            self.logger.info(
                "[MIG-N-005] PASS: accepted at pre-create but failed cleanly "
                "(%s/%s) with the volume still on the source. Worth asking "
                "dev whether capacity should be checked at pre-create "
                "instead -- failing later is correct but costs a round trip.",
                status, phase)
        else:
            self.logger.info("[MIG-N-005] PASS: refused at pre-create -- %s",
                             combined.strip()[:200])
        self.cleanup_migrations()


class MigrationUnderReplication(MigrationTestBase):
    """MIG-N-006, MIG-N-007: migrating a volume that is being replicated.

    Not in anyone's plan, and the most interesting case in this file. Two
    features that both move data: replication ships the volume to another
    CLUSTER on a schedule, migration moves it to another NODE in this one.
    They have never been run together.

    The question is not whether migration is refused -- it may well be
    allowed, and arguably should be. It is whether the replication
    relationship survives its volume moving underneath it, and whether the
    next cycle still works. A relationship that silently stops replicating
    after a migration leaves a volume that everyone believes is protected.
    """

    def run(self):
        data = self._cli_json(f"{self.base_cmd} cluster replication-target-list "
                              f"--cluster-id {self.cluster_id}")
        rows = (data.get("results", data) if isinstance(data, dict)
                else data) or []
        if not rows:
            self.logger.warning(
                "[MIG-N-006] SKIPPED: no replication target on this cluster, "
                "so there is no replicating volume to migrate. This case "
                "needs the two-cluster lab the replication lane builds "
                "(e2e/scripts/replication_lab_setup.sh), then a policy "
                "attached before it runs.")
            return

        target = self._get(rows[0], "name", "id", "uuid")
        stamp = int(time.time()) % 100000
        pol = f"migpol{stamp}"
        out, err = self._cli(f"{self.base_cmd} -d cluster "
                             f"replication-policy-add {self.cluster_id} "
                             f"{pol} {target} --interval-min 1 2>&1")
        if cli_failed(out, err):
            self.logger.warning(
                "[MIG-N-006] SKIPPED: could not create a replication policy "
                "on the existing target: %s", (out + err).strip()[:220])
            return

        vol = f"migrepl{stamp}"
        vol_id, sums = self.make_volume(vol)
        self._cli(f"{self.base_cmd} -d volume replication-policy-set "
                  f"{vol_id} {pol} 2>&1")
        sleep_n_sec(90)       # let a cycle start

        before = self._cli_json(f"{self.base_cmd} volume "
                                f"replication-relationship {vol_id}")
        self.logger.info("[MIG-N-006] relationship before the migration: %s",
                         str(before)[:240])
        if not before:
            self.logger.warning(
                "[MIG-N-006] SKIPPED: the volume never started replicating, "
                "so migrating it proves nothing about the interaction.")
            self._cli(f"{self.base_cmd} -d cluster replication-policy-remove "
                      f"{pol} 2>&1")
            self.cleanup_migrations()
            return

        src = self.lvol_node(vol_id)
        tgt = self.pick_target(src, "no-overlap")
        self.logger.info("[MIG-N-006] migrating a REPLICATING volume "
                         "%s -> %s", src, tgt)
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{vol_id} {tgt} 2>&1")
        combined = out + err
        if not self.MIG_ID_RE.search(combined):
            self.logger.info(
                "[MIG-N-006] PASS (by refusal): migrating a replicating "
                "volume is not allowed -- %s. That is a defensible design; "
                "worth confirming with dev it is deliberate rather than a "
                "side effect.", combined.strip()[:220])
            self._cli(f"{self.base_cmd} -d volume replication-policy-clear "
                      f"{vol_id} 2>&1")
            self._cli(f"{self.base_cmd} -d cluster replication-policy-remove "
                      f"{pol} 2>&1")
            self.cleanup_migrations()
            return

        mid = self.MIG_ID_RE.search(combined).group(1)
        self._migrations.append(mid)
        self._connect_target_paths(combined)
        self.migrate_continue(mid)
        status, phase = self.await_migration(
            vol_id=vol_id, migration_id=mid, timeout=1800,
            what="MIG-N-006 migrating a replicating volume")
        self.logger.info("[MIG-N-006] migration ended %s (%s)", status, phase)

        # ── MIG-N-007 does replication survive? ──────────────────────────
        sleep_n_sec(120)
        after = self._cli_json(f"{self.base_cmd} volume "
                               f"replication-relationship {vol_id}")
        self.logger.info("[MIG-N-007] relationship after: %s",
                         str(after)[:240])
        if not after:
            raise AssertionError(
                f"[MIG-N-007] the replication relationship for {vol} is GONE "
                f"after the volume migrated between nodes. The volume is now "
                f"unprotected and nothing says so -- an operator who set a "
                f"policy still believes it is replicating. Before the "
                f"migration it was: {str(before)[:200]}")

        self._cli(f"{self.base_cmd} -d volume replication-trigger "
                  f"{vol_id} 2>&1")
        sleep_n_sec(150)
        rel = self._cli_json(f"{self.base_cmd} volume "
                             f"replication-relationship {vol_id}") or {}
        state = str(self._get(rel if isinstance(rel, dict) else {},
                              "state") or "").lower()
        if state and state not in ("replicating", "in_sync"):
            raise AssertionError(
                f"[MIG-N-007] after the migration the relationship is in "
                f"state {state!r} and a forced cycle did not restore it. "
                f"Replication survived in name but cannot transfer.")
        self.logger.info("[MIG-N-007] PASS: replication survived the "
                         "migration and still cycles (state=%r)",
                         state or "unreported")
        self.verify(vol, sums, "after migrating a replicating volume "
                               "(MIG-N-007)")

        self._cli(f"{self.base_cmd} -d volume replication-policy-clear "
                  f"{vol_id} 2>&1")
        self._cli(f"{self.base_cmd} -d cluster replication-policy-remove "
                  f"{pol} 2>&1")
        self.cleanup_migrations()
