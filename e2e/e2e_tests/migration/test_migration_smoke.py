"""MIG-S: the one test to run first. Does any of this work at all?

Deliberately the smallest thing that still exercises every link in the
chain, so that a green result means the other 55 cases are worth launching
and a red one points at exactly which link broke.

It exists because the first full run of this lane burned nine hours and
proved nothing: all 19 classes failed in their first second on a CLI flag
that does not exist, and each failure was followed by ten minutes of
teardown. A ten-minute smoke test would have found it immediately.

Six links, in order, each logged by name so a failure says which one:

    1  volume add          the CLI surface -- the one that broke last time
    2  sn list / topology  can we read node ids, secondaries, tertiaries
    3  volume migrate      pre-create, AND the nvme connect strings it
                           returns. The riskiest link: the connect-string
                           format was inferred from the original scripts,
                           never observed.
    4  migrate-continue    does the task runner start, and can we read the
                           record's status and phase back
    5  placement           does the volume actually end up on the target
    6  data                is it byte-identical afterwards

What it deliberately does NOT do: inject a fault, run load, exercise
post-migration operations, or touch more than one volume. Those are the
other lanes. If this passes, run `--testname migration`.
"""
import time

from e2e_tests.migration.migration_base import (
    MigrationTestBase,
    MigrationPreconditionError,
)
from utils.common_utils import sleep_n_sec


class MigrationSmoke(MigrationTestBase):
    """MIG-S-001: one volume, one hop, no faults. ~10 minutes."""

    #: No post-cutover IO watch -- that is MIG-H's job and it costs 5 minutes.
    POST_OP_WATCH_SEC = 0

    def run(self):
        stamp = int(time.time()) % 100000
        vol = f"migsmoke{stamp}"
        step = 0

        def nxt(what):
            nonlocal step
            step += 1
            self.logger.info("")
            self.logger.info("=" * 64)
            self.logger.info("[MIG-S-001] LINK %d/7: %s", step, what)
            self.logger.info("=" * 64)

        # ── 1. the CLI surface ───────────────────────────────────────────
        # Creating and seeding are separate links on purpose. They used to
        # be one, and when a seed failed the report said "This is the CLI
        # surface" and sent everyone to look at an invocation that was fine.
        nxt("volume add -- does the CLI accept how we call it")
        try:
            vol_id, _ = self.make_volume(vol, seed=False)
        except MigrationPreconditionError as exc:
            raise AssertionError(
                f"[MIG-S-001] LINK 1 FAILED: could not create a volume. "
                f"{str(exc)[:400]}\n\n"
                f"This is the CLI surface, not migration. Check the invocation "
                f"against simplyblock_cli/cli.py -- "
                f"`python e2e/scripts/check_cli_usage.py` does it statically "
                f"in about a second, and would have caught the last "
                f"occurrence of this (volume add was called with --pool, "
                f"which does not exist; the pool is positional).") from exc
        self.logger.info("[MIG-S-001] LINK 1 OK: %s = %s", vol, vol_id)

        # ── 2. the data path ───────────────────────────────────────────
        nxt("connect, mount, write -- can the client use the volume at all")
        try:
            sums = self.seed(vol)
        except MigrationPreconditionError as exc:
            raise AssertionError(
                f"[MIG-S-001] LINK 2 FAILED: {vol} exists but could not be "
                f"seeded. {str(exc)[:400]}\n\n"
                f"None of this is migration -- it is nvme connect, mkfs, "
                f"mount and a write on the client, in that order. Check it "
                f"in that order too: did a new block device appear after "
                f"connecting (if not, suspect the connect strings or the "
                f"client's network), did the mount succeed, and is there "
                f"room on the volume for the files.") from exc
        self.logger.info("[MIG-S-001] LINK 2 OK: %d seeded file(s)", len(sums))

        # ── 3. topology ──────────────────────────────────────────────────
        nxt("topology -- can we read nodes, and find a target")
        nodes = self.online_nodes()
        self.logger.info("[MIG-S-001] %d online storage node(s)", len(nodes))
        for n in nodes:
            nid = n.get("uuid") or n.get("id")
            self.logger.info("    %s  %s  sec=%s  ter=%s", nid,
                             n.get("mgmt_ip"), self.node_secondary(nid, nodes),
                             self.node_tertiary(nid, nodes))
        src = self.lvol_node(vol_id)
        if not src:
            raise AssertionError(
                f"[MIG-S-001] LINK 3 FAILED: {vol} exists but reports no "
                f"node_id, so there is nothing to migrate FROM and no way to "
                f"assert where it ends up. Check what "
                f"`volume get {vol_id}` returns.")
        try:
            tgt = self.pick_target(src, "no-overlap")
        except MigrationPreconditionError as exc:
            raise AssertionError(
                f"[MIG-S-001] LINK 3 FAILED: no target node. {str(exc)[:300]}\n"
                f"At least two online storage nodes are needed, and at "
                f"NDCS+NPCS=1+1 so that one is spare. A 1+2 cluster on four "
                f"nodes leaves nothing to migrate to.") from exc
        self.logger.info("[MIG-S-001] LINK 3 OK: %s -> %s", src, tgt)

        # ── 4. pre-create, and the connect strings ───────────────────────
        nxt("volume migrate -- pre-create and the nvme connect strings")
        try:
            mid = self.migrate(vol_id, tgt)
        except MigrationPreconditionError as exc:
            raise AssertionError(
                f"[MIG-S-001] LINK 4 FAILED: pre-create did not return a "
                f"migration id. {str(exc)[:400]}\n\n"
                f"Either the verb is wrong, or the output does not contain "
                f"'Migration ID: <uuid>' in the shape MIG_ID_RE expects.") from exc
        self.logger.info("[MIG-S-001] LINK 4 OK: migration %s", mid)

        # The client MUST be attached to both ends before the cutover, or the
        # flip becomes a disconnect. migrate() does the connecting; this just
        # reports whether it found anything to connect, because a silent
        # zero here is the difference between testing the feature and
        # testing a disconnect.
        if not self.k8s_test:
            node = self.fio_node[0] if self.fio_node else self.mgmt_nodes[0]
            out, _ = self.ssh_obj.exec_command(
                node=node,
                command="sudo nvme list-subsys 2>/dev/null | grep -c live || true")
            self.logger.info("[MIG-S-001] client now has %s live path(s)",
                             (out or "?").strip().splitlines()[0:1])

        # ── 5. continue, and read the record back ────────────────────────
        nxt("migrate-continue -- does the runner start and report phases")
        self.migrate_continue(mid)
        rec = None
        deadline = time.time() + 120
        while time.time() < deadline:
            rec = self.migration_record(vol_id=vol_id, migration_id=mid)
            if rec:
                break
            sleep_n_sec(5)
        if not rec:
            raise AssertionError(
                f"[MIG-S-001] LINK 5 FAILED: no migration record for {vol_id} "
                f"within 120s of migrate-continue. Either migrate-list does "
                f"not list it, or the field this harness matches on "
                f"(lvol_id / volume_id) is spelled differently in the "
                f"output. Run `{self.base_cmd} volume migrate-list "
                f"--cluster-id {self.cluster_id} --json` by hand and compare.")
        self.logger.info("[MIG-S-001] LINK 5 OK: record reads status=%s "
                         "phase=%s", self._get(rec, "status"),
                         self._get(rec, "phase"))

        status, phase = self.await_migration(
            vol_id=vol_id, migration_id=mid,
            what="MIG-S-001 the migration itself")
        if status not in self.OK_TERMINAL:
            raise AssertionError(
                f"[MIG-S-001] the migration ended {status!r} in phase "
                f"{phase!r} with no fault injected. Everything up to here "
                f"worked, so this is the product or the cluster, not the "
                f"harness -- which is exactly what this test exists to tell "
                f"you apart.")

        # ── 6. placement ─────────────────────────────────────────────────
        nxt("placement -- did it actually move")
        self.assert_placed_on(vol_id, tgt, "(MIG-S-001)")
        self.logger.info("[MIG-S-001] LINK 6 OK")

        # ── 7. data ──────────────────────────────────────────────────────
        nxt("data -- is it byte-identical")
        self.verify(vol, sums, "after a plain migration (MIG-S-001)")
        self.logger.info("[MIG-S-001] LINK 7 OK")

        self.logger.info("")
        self.logger.info("=" * 64)
        self.logger.info("[MIG-S-001] PASS -- all seven links work. The rest of "
                         "the lane is variations on this; run")
        self.logger.info("           python e2e/e2e.py --testname migration")
        self.logger.info("=" * 64)
        self.cleanup_migrations()
