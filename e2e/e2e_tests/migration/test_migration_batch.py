"""MIG-B: shared-namespace groups -- many volumes in one subsystem.

A subsystem that allows more than one namespace holds several volumes at
once, and they migrate as a unit. This is the ``--batch`` path, and it has
its own record type (``migrate-group-list``), its own terminal states, and
its own rollback.

The CLI/API difference matters here more than anywhere else:

* the **CLI** takes a volume id and needs ``--batch`` spelled out. Forget it
  and you ask for a single-volume migration of something that is part of a
  group.
* the **API** is keyed by the subsystem NQN and decides for itself: if the
  subsystem allows more than one namespace, the whole group moves.

So the same intent expressed two ways can produce two different operations,
and MIG-B-005 is the case that checks the CLI refuses the ambiguous one
rather than quietly doing half of it.

``ns_id`` gaps are the other thing worth knowing. Delete a member from the
middle of a group and the remaining namespace ids keep their numbering --
the gap stays. A migration must preserve that, because clients address
namespaces by id and renumbering them silently repoints every one of them.
"""
import time

from e2e_tests.migration.migration_base import (
    MigrationPreconditionError,
    MigrationTestBase,
)
from utils.common_utils import cli_failed, sleep_n_sec


class _BatchBase(MigrationTestBase):
    """A shared-namespace group, built the way the product builds one."""

    MEMBERS = 4

    def _make_group(self, tag, members=None, seed_first=True):
        """Create N volumes in ONE subsystem. Returns [{name,id,ns_id}]."""
        members = members or self.MEMBERS
        stamp = int(time.time()) % 100000
        self._subsys = f"migsub{tag}{stamp}"
        group = []
        for i in range(members):
            name = f"mig{tag}{stamp}n{i}"
            # --namespaced is what shares a subsystem: it adds the lvol as
            # a namespace on an existing subsystem of the same pool on the
            # node, opening a new one only when none has a free slot. There
            # is no --subsystem flag to name one, so the group is "every
            # member created with --namespaced in this pool".
            extra = (f" --namespaced true --max-namespace-per-subsys {members + 2}"
                     if i == 0 else " --namespaced true")
            out, err = self._cli(
                f"{self.base_cmd} -d volume add {name} {self.VOL_SIZE} "
                f"{self.pool_name}{extra} 2>&1")
            if cli_failed(out, err):
                raise MigrationPreconditionError(
                    f"[MIG-B] could not add {name} as a namespace on a "
                    f"shared subsystem: {(out + err)[:300]}. The group is "
                    f"built with --namespaced, which fills an existing "
                    f"subsystem of this pool on the node before opening a "
                    f"new one; if that is refused here the batch lane needs "
                    f"its group built another way.")
            vid = self.sbcli_utils.get_lvol_id(lvol_name=name)
            self._mig_vols.append(name)
            group.append({"name": name, "id": vid,
                          "ns_id": self._ns_id(vid)})
        self._group = group
        self._sums = self.seed(group[0]["name"]) if seed_first else {}
        return group

    def _ns_id(self, vol_id):
        try:
            d = self.sbcli_utils.get_lvol_details(lvol_id=vol_id)
            rows = d.get("results", d) if isinstance(d, dict) else d
            row = (rows[0] if isinstance(rows, list) and rows else rows) or {}
            return row.get("ns_id") or row.get("nsid")
        except Exception:                             # noqa: BLE001
            return None

    def _teardown(self):
        try:
            self.cleanup_migrations()
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[MIG-B] teardown: %s", str(exc)[:140])


class MigrationBatchFlat(_BatchBase):
    """MIG-B-001, MIG-B-002, MIG-B-003: a flat group moves as one unit."""

    def run(self):
        group = self._make_group("flat")
        src = self.lvol_node(group[0]["id"])
        tgt = self.pick_target(src, "no-overlap")
        self.logger.info("[MIG-B-001] migrating %d members as a group: "
                         "%s -> %s", len(group), src, tgt)

        mid = self.migrate(group[0]["id"], tgt, batch=True)
        self.migrate_continue(mid, batch=True)
        status, phase = self.await_batch(mid, timeout=1800,
                                         what="MIG-B-001 flat group")
        if status not in self.OK_TERMINAL:
            raise AssertionError(
                f"[MIG-B-001] the group migration ended {status!r} in phase "
                f"{phase!r}. A batch is all-or-nothing: a partially moved "
                f"subsystem has members on two nodes and no client can "
                f"address it coherently.")

        # ── MIG-B-002 EVERY member moved, not most of them ───────────────
        stragglers = [(m["name"], self.lvol_node(m["id"])) for m in group
                      if self.lvol_node(m["id"]) != tgt]
        if stragglers:
            raise AssertionError(
                f"[MIG-B-002] the group reported success but these members "
                f"are not on {tgt}: {stragglers}. That is exactly the split "
                f"state a batch exists to prevent.")
        self.logger.info("[MIG-B-002] PASS: all %d members on the target",
                         len(group))

        # ── MIG-B-003 namespace ids preserved ────────────────────────────
        changed = [(m["name"], m["ns_id"], self._ns_id(m["id"])) for m in group
                   if m["ns_id"] is not None
                   and self._ns_id(m["id"]) != m["ns_id"]]
        if changed:
            raise AssertionError(
                f"[MIG-B-003] namespace ids changed across the migration: "
                f"{changed} (name, before, after). Clients address "
                f"namespaces by id; renumbering them silently repoints every "
                f"one of them at different data.")
        self.logger.info("[MIG-B-003] PASS: ns_ids unchanged")
        self.verify(group[0]["name"], self._sums,
                    "after a group migration (MIG-B-003)")
        self._teardown()


class MigrationBatchNsGaps(_BatchBase):
    """MIG-B-004: a gap in the namespace numbering survives the move.

    Delete a member from the middle and the remaining ids keep their
    numbering -- 1,2,4,5 rather than being compacted to 1,2,3,4. A
    migration that renumbers to close the gap silently repoints two
    clients at each other's data, and nothing reports an error.
    """

    MEMBERS = 5

    def run(self):
        group = self._make_group("gaps")
        victim = group[2]
        self.logger.info("[MIG-B-004] deleting the middle member %s (ns_id "
                         "%s) to make a gap", victim["name"], victim["ns_id"])
        self.sbcli_utils.delete_lvol(lvol_name=victim["name"])
        if victim["name"] in self._mig_vols:
            self._mig_vols.remove(victim["name"])
        sleep_n_sec(20)

        remaining = [m for m in group if m is not victim]
        before = {m["name"]: self._ns_id(m["id"]) for m in remaining}
        self.logger.info("[MIG-B-004] ns_ids before: %s", before)

        src = self.lvol_node(remaining[0]["id"])
        tgt = self.pick_target(src, "no-overlap")
        mid = self.migrate(remaining[0]["id"], tgt, batch=True)
        self.migrate_continue(mid, batch=True)
        status, _ = self.await_batch(mid, timeout=1800,
                                     what="MIG-B-004 group with a gap")
        if status not in self.OK_TERMINAL:
            raise AssertionError(
                f"[MIG-B-004] a group with a namespace gap failed to migrate "
                f"({status}). A gap is normal -- it is what deleting a "
                f"member leaves behind.")

        after = {m["name"]: self._ns_id(m["id"]) for m in remaining}
        self.logger.info("[MIG-B-004] ns_ids after:  %s", after)
        if after != before:
            raise AssertionError(
                f"[MIG-B-004] namespace ids were renumbered across the "
                f"migration.\n  before: {before}\n  after:  {after}\n"
                f"The gap left by the deleted member must be preserved; "
                f"compacting it repoints every client past the gap at "
                f"different data, with no error anywhere.")
        self.logger.info("[MIG-B-004] PASS: the gap survived")
        self._teardown()


class MigrationBatchNegative(_BatchBase):
    """MIG-B-005, MIG-B-006: asking for the wrong thing.

    MIG-B-005 is the CLI/API asymmetry made testable. The CLI takes a
    volume id and needs --batch; the API takes the subsystem NQN and
    decides batch itself. So a CLI call WITHOUT --batch against a member of
    a shared subsystem is ambiguous, and the interesting question is
    whether it refuses or quietly migrates one namespace out of a group.
    """

    def run(self):
        group = self._make_group("neg")
        src = self.lvol_node(group[0]["id"])
        tgt = self.pick_target(src, "no-overlap")

        # ── MIG-B-005 a group member, migrated without --batch ───────────
        self.logger.info("[MIG-B-005] migrating a group member WITHOUT "
                         "--batch")
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{group[0]['id']} {tgt} 2>&1")
        combined = (out + err)
        mid = self.MIG_ID_RE.search(combined)
        if not mid:
            self.logger.info("[MIG-B-005] PASS: refused -- %s",
                             combined.strip()[:220])
        else:
            self._migrations.append(mid.group(1))
            self.logger.warning(
                "[MIG-B-005] the CLI ACCEPTED a non-batch migration of a "
                "shared-subsystem member (migration %s). If it moves one "
                "namespace out of the group the subsystem ends up split "
                "across two nodes. Checking what it actually does.",
                mid.group(1))
            self.migrate_continue(mid.group(1))
            self.await_migration(vol_id=group[0]["id"],
                                 migration_id=mid.group(1), timeout=1800,
                                 what="MIG-B-005 non-batch on a group member")
            nodes = {m["name"]: self.lvol_node(m["id"]) for m in group}
            if len(set(nodes.values())) > 1:
                raise AssertionError(
                    f"[MIG-B-005] the group is now SPLIT across nodes: "
                    f"{nodes}. One subsystem's namespaces must all live "
                    f"together; a client addressing the subsystem cannot "
                    f"reach half of it.")
            self.logger.info("[MIG-B-005] it migrated the whole group anyway "
                             "-- effectively treating it as batch: %s", nodes)

        # ── MIG-B-006 --batch on a volume that is not in a group ─────────
        solo = f"migsolo{int(time.time()) % 100000}"
        solo_id, _ = self.make_volume(solo, seed=False)
        self.logger.info("[MIG-B-006] --batch on a solo volume")
        out, err = self._cli(f"{self.base_cmd} --dev volume migrate "
                             f"{solo_id} {tgt} --batch 2>&1")
        combined = (out + err)
        if self.MIG_ID_RE.search(combined):
            self._migrations.append(
                self.MIG_ID_RE.search(combined).group(1))
            self.logger.info(
                "[MIG-B-006] --batch on a single-member subsystem was "
                "accepted and behaves as a group of one. Defensible; noted "
                "rather than failed.")
        else:
            self.logger.info("[MIG-B-006] PASS: refused -- %s",
                             combined.strip()[:200])
        self._teardown()


class MigrationBatchWithTrees(_BatchBase):
    """MIG-B-007, MIG-B-008: group members that have their own history.

    The combination the scripts pushed hardest: a shared-namespace group
    where members carry snapshot chains, and a clone tree with fifteen
    members. Each member's chain has to move with it, as part of an
    all-or-nothing group operation -- two mechanisms that are individually
    tested and interact only here.
    """

    MEMBERS = 3
    SNAPS_EACH = 5

    def run(self):
        group = self._make_group("tree")
        for m in group:
            for i in range(self.SNAPS_EACH):
                self._cli(f"{self.base_cmd} -d snapshot add {m['id']} "
                          f"{m['name']}s{i} 2>&1")
        self.logger.info("[MIG-B-007] %d members, %d snapshots each",
                         len(group), self.SNAPS_EACH)

        src = self.lvol_node(group[0]["id"])
        tgt = self.pick_target(src, "no-overlap")
        mid = self.migrate(group[0]["id"], tgt, batch=True)
        self.migrate_continue(mid, batch=True)
        status, phase = self.await_batch(
            mid, timeout=2400, what="MIG-B-007 group with per-member chains")
        if status not in self.OK_TERMINAL:
            raise AssertionError(
                f"[MIG-B-007] a group whose members carry snapshot chains "
                f"ended {status!r} (phase {phase!r}). Each member's history "
                f"has to move with it; this is where the group path and the "
                f"snapshot path meet.")

        off = [(m["name"], self.lvol_node(m["id"])) for m in group
               if self.lvol_node(m["id"]) != tgt]
        if off:
            raise AssertionError(
                f"[MIG-B-007] members not on the target: {off}")

        data = self._cli_json(f"{self.base_cmd} snapshot list")
        rows = (data.get("results", data) if isinstance(data, dict)
                else data) or []
        for m in group:
            mine = [s for s in rows
                    if str(self._get(s, "snap_name", "name") or "")
                    .startswith(m["name"])]
            if len(mine) < self.SNAPS_EACH:
                raise AssertionError(
                    f"[MIG-B-008] member {m['name']} has {len(mine)} of "
                    f"{self.SNAPS_EACH} snapshots after the group migrated. "
                    f"Every member's history moves, or none of it does.")
        self.logger.info("[MIG-B-008] PASS: every member kept its chain")
        self.verify(group[0]["name"], self._sums,
                    "after a group-with-trees migration (MIG-B-008)")
        self._teardown()
