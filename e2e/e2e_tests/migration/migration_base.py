"""Shared machinery for lvol migration -- moving one volume between nodes.

Ported from the scripts on ``origin/lvol-migration-test-scripts``
(``test-logs/migration_test_lib.py``, 1872 lines), which found most of the
bugs this feature has. The scenarios survive; four things had to change
before any of it could run in CI, and they are worth knowing because they
are the difference between those scripts and this module:

1. **``sbctl`` was a local subprocess.** ``subprocess.run(["sbctl", ...])``
   assumes the test is running ON the management node, which is how they
   were used -- by hand, on a cluster someone had already deployed. Our
   suite runs from a runner and reaches the cluster over ssh, so every call
   goes through :meth:`_cli`.

2. **The CLI noun changed.** The scripts call ``lvol migrate``; current
   ``origin/main`` has ``volume migrate`` and five siblings. Porting the old
   spelling verbatim would have produced "unknown command" on every call.

3. **A hardcoded SSH password** sat in the library
   (``SSH_PASS = "3tango11"``). It is not reproduced here; auth comes from
   the suite's own key handling like every other lane.

4. **Docker only.** No Kubernetes path existed. There is one now, and
   because the two backends do not merely plumb the same request
   differently -- they use different mechanisms -- this module branches
   once per verb rather than making each test know which it is on.

THE FEATURE, because it is easy to confuse with three others we already
test. This is **lvol migration**: one volume moves between two nodes of ONE
cluster, through a deliberate two-phase handshake. It is not device-failure
rebalancing, not node migration, and not cross-cluster replication.

DOCKER -- the test drives both halves over the CLI::

    volume migrate <vol> <target_node>      pre-create: builds the target
                                            subsystem, returns a migration id
                                            and connect strings. The new
                                            paths come up ANA INACCESSIBLE,
                                            so the source keeps serving IO.
      -> THE TEST connects the client to those paths <-
    volume migrate-continue <migration_id>  snap_copy -> intermediate ->
                                            lvol_migrate (short freeze, final
                                            delta, ANA flip) -> cleanup_source
                                            on failure: cleanup_target -> failed

The gap between the two commands is the whole point: the client is attached
to both ends before anything moves, so the cutover is an ANA flip rather
than a disconnect. A test that calls them back to back without connecting
in between is testing a different thing.

KUBERNETES -- nothing calls the CLI, and the test drives NEITHER half. A
move is a cluster-scoped ``PersistentVolumeOps`` naming a PV and a target
``StorageNode``, and the operator walks a three-step graph::

    Validating   POST the migration, then start one Job PER CONSUMING NODE
                 which connects and verifies the target paths
    Migrating    continue the migration, which is what starts the copy
    Verifying    delete those Jobs and clear the controllers they left

So on k8s the test authors the CR and watches ``status.step``; it must not
connect anything, because the operator's Jobs own that step and racing
them for the same paths is the defect that created ``Verifying``. The spec
is the operator's
``docs/designs/crd-redesign/design-persistentvolumeops.md``.

The registered ``VolumeMigration`` kind is deliberately not used: the
operator's ``cmd/main.go`` defaults ``--legacy-volume-migration`` to false,
so a CR of that kind is reconciled by nobody and the move silently never
happens.

What does NOT carry across, and is therefore docker-only:

* ``--batch``. A k8s move is addressed by SUBSYSTEM -- naming one member
  moves the whole group -- so there is no flag to set and no refusal to
  provoke.
* the four control-plane phases. The k8s graph is coarser (three of them
  are the one ``Migrating`` step), so ``await_phase`` translates and says
  so; a k8s pass does not prove a fault landed in a named phase.
* target == source. The CLI refuses it; k8s ends ``Succeeded`` with
  ``TargetNodeIsSource`` and creates no migration, which is the ordinary
  outcome for a sibling of an already-moved volume.

CLI vs API, which matters for what a test can address:

* the CLI takes a VOLUME id, and needs ``--batch`` spelled out for a
  shared-namespace group
* the API is keyed by the subsystem NQN and decides batch by itself -- if
  the subsystem allows more than one namespace it migrates the whole group
"""
import json
import os
import random
import re
import time
from typing import ClassVar

from e2e_tests.cluster_test_base import TestClusterBase
from utils.ana_monitor import AnaWatcher
from utils.common_utils import cli_failed, sleep_n_sec


class MigrationPreconditionError(Exception):
    """The harness could not set up what the test needs.

    Separate from an assertion failure on purpose: this says "the test never
    ran", not "the product is wrong". Conflating the two is how a short-
    staffed lab gets filed as a product defect.
    """


class MigrationTestBase(TestClusterBase):
    """Two nodes, one volume, and the vocabulary to move it between them."""

    # ── states, from the migration record ────────────────────────────────
    TERMINAL = ("done", "completed", "failed", "error", "cancelled")
    OK_TERMINAL = ("done", "completed")
    #: Phases in order. cleanup_target is the rollback path, not a step.
    PHASES = ("snap_copy", "intermediate", "lvol_migrate", "cleanup_source")
    ROLLBACK_PHASE = "cleanup_target"

    MIGRATION_TIMEOUT = 900
    MIGRATION_POLL = 5

    #: The operator's own step budgets, from
    #: operator/internal/controllers/volume/graphs.go. A k8s wait MUST be
    #: at least their sum, or the test calls a healthy migration stuck
    #: before the controller has even reached its own deadline -- and the
    #: docker number is five times too small for that: Migrating alone is
    #: 30 minutes plus ten per member, with no ceiling, deliberately
    #: ("capping the bound would fail it for being big rather than for
    #: being stuck"). At one member that is 65 minutes against our 900s.
    K8S_VALIDATING_SEC = 15 * 60
    K8S_COPY_BASE_SEC = 30 * 60
    K8S_COPY_PER_MEMBER_SEC = 10 * 60
    K8S_VERIFYING_SEC = 10 * 60

    #: How long after a migration to keep verifying IO. The scripts watched
    #: for five minutes past the operation because that is where the late
    #: failures showed up -- a cutover that looks clean and then drops IO
    #: thirty seconds later is the bug this catches.
    POST_OP_WATCH_SEC = int(os.environ.get("MIG_POST_OP_SEC", 300))

    VOL_SIZE = os.environ.get("MIG_VOL_SIZE", "2G")

    #: How often the cutover window is sampled. The pause budget is
    #: derived from this (design + interval + slack), so lowering it
    #: tightens the bound and costs more ssh/exec round trips.
    ANA_INTERVAL_S = float(os.environ.get("MIG_ANA_INTERVAL", 2.0))

    #: Whether this lane's NEW checks fail a case or only report.
    #:
    #: "warn" by default, and the reason is about reading a run rather
    #: than about confidence. Split-brain and NQN-preservation are checks
    #: this lane has never run before, on a product nobody has measured
    #: them against. Turning three new gates on at once in a lane that was
    #: passing 15/20 means a red run where a product regression and a
    #: too-strict new assertion look identical -- and the first instinct
    #: is then to loosen the assertion, which is how a real finding gets
    #: explained away on its first sighting.
    #:
    #: So: one run with them reporting, look at what they actually see,
    #: then MIG_NEW_GATES=fail. The freeze COUNT is excluded and stays
    #: fatal regardless -- it is not ours, and the feature team has
    #: evidence that it predicted silent data loss exactly.
    NEW_GATES = os.environ.get("MIG_NEW_GATES", "warn").strip().lower()

    def _gate(self, case_id, message):
        """Fail on a new check, or report it, per MIG_NEW_GATES."""
        if self.NEW_GATES in ("fail", "strict", "true", "1"):
            raise AssertionError(message)
        self.logger.warning(
            "[%s] NEW-CHECK (reporting only, MIG_NEW_GATES=%s): %s",
            case_id, self.NEW_GATES, message)
        return False

    # ── randomisation ────────────────────────────────────────────────────
    #
    # Dev's position on their own migration script, and it is correct:
    # "They are in random order because it's the only meaningful thing to
    # find bugs. If they run in a specific order over and over again, it
    # just means that this specific order works."
    #
    # So where a choice does not change what a case PROVES -- which of
    # several equally valid targets, which volume of a set, whether to take
    # a snapshot first -- it is drawn rather than fixed. What the case
    # asserts stays deterministic; only the path to it varies.
    #
    # The seed is the other half, and the half a fuzzer usually forgets. A
    # random order that finds a bug is worth nothing if it cannot be run
    # again, so the seed is logged at the top of every class and replaying
    # it is MIG_SEED=<n>. Per-class streams, derived from the seed and the
    # class name, mean adding or removing a case does not shift what every
    # other case draws -- without that, a seed only reproduces a whole-lane
    # run of exactly the same code.
    SEED_ENV = "MIG_SEED"

    def _init_rng(self):
        base = os.environ.get(self.SEED_ENV)
        if base is None:
            base = random.SystemRandom().randrange(1, 2 ** 31)
            how = "generated"
        else:
            try:
                base = int(base)
            except ValueError:
                raise MigrationPreconditionError(
                    f"[MIG] {self.SEED_ENV}={base!r} is not an integer. It is "
                    f"the replay handle for a randomised run; a seed that "
                    f"cannot be parsed would silently become a different "
                    f"run.") from None
            how = "from " + self.SEED_ENV
        self.rng_seed = base
        cls = type(self).__name__
        self.rng = random.Random(f"{base}:{cls}")
        self.logger.info(
            "[MIG] RANDOM SEED %s (%s); this class draws from stream "
            "%s:%s. Replay this exact run with %s=%s",
            base, how, base, cls, self.SEED_ENV, base)
        return self.rng

    def pick(self, seq, what="choice"):
        """One of *seq*, drawn from this class's stream, and logged.

        Logged because an unlogged random choice makes a failure report
        unreadable: "migrated to node X" has to say that X was a draw and
        not the only option, or the next person assumes the case is
        pinned to X.
        """
        items = list(seq)
        if not items:
            raise MigrationPreconditionError(
                f"[MIG] nothing to pick from for {what}")
        chosen = self.rng.choice(items)
        if len(items) > 1:
            self.logger.info("[MIG] %s: drew %s from %d candidate(s)",
                             what, chosen, len(items))
        return chosen

    def migration_timeout(self, members=1):
        """How long to wait for a terminal state on this backend."""
        if not self.k8s_test:
            return self.MIGRATION_TIMEOUT
        members = max(1, int(members or 1))
        return (self.K8S_VALIDATING_SEC
                + self.K8S_COPY_BASE_SEC
                + members * self.K8S_COPY_PER_MEMBER_SEC
                + self.K8S_VERIFYING_SEC
                + 300)          # slack for scheduling and image pulls

    def setup(self):
        super().setup()
        # The pool is created HERE, not in a run() method, because every case
        # in this lane needs one and the failure when it is missing does not
        # look like a missing pool: `volume add` prints "Pool not found:
        # testpool", which contains no "error" substring, so a naive check
        # passes it through and the test dies later in seed() with
        # "'NoneType' object is not iterable".
        #
        # On docker the base setup DELETES every pool first, so nothing is
        # inherited from a previous run. ensure_pool also waits for the pool
        # to become queryable (creation is async) and makes the StorageClass
        # on k8s. All three steps, once, where no case can skip them.
        self._init_rng()
        self.warn_if_autoplacement_on()
        self.pool_name = self.ensure_pool()
        self._migrations = []
        self._mig_vols = []
        #: PersistentVolumeOps raised by this test, for teardown. Separate
        #: from _migrations because those are ids on docker and CR names
        #: here, and only the latter are something to delete.
        self._k8s_pvos = []
        #: StorageClasses a case made beyond the lane's own. The base
        #: teardown deletes only _k8s_storage_class_name, so anything
        #: created with different parameters -- a namespaced class for a
        #: shared-subsystem group -- leaks a cluster-scoped object into
        #: every later run unless it is tracked here.
        self._extra_scs = []
        #: Worker nodes this lane cordoned. A cordon outlives the case
        #: that made it and makes the node unschedulable for every case
        #: after it, so it is undone in teardown, not inline.
        self._cordoned = set()

    # ── CLI ──────────────────────────────────────────────────────────────
    def _cli(self, command):
        """Run a CLI command wherever the CLI lives on this backend.

        Docker reaches it over ssh on the management node. Kubernetes has
        no management node to ssh to -- the control plane is a deployment
        -- so the command runs inside the admin pod. ``exec_sbcli`` takes
        the command verbatim (``bash -c``), which is why ``base_cmd`` and
        any ``2>&1`` are passed straight through.

        The READ-ONLY verbs are what this is for on k8s: ``volume list``,
        ``migrate-list``, ``cluster list`` all answer identically there.
        The move itself does not come through here on k8s -- it is a CR.
        See :meth:`migrate`.
        """
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            out, err = k8s.exec_sbcli(command)
            return (out or ""), (err or "")
        out, err = self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                             command=command)
        return (out or ""), (err or "")

    def _cli_json(self, command):
        out, _ = self._cli(command + " --json")
        for i, ch in enumerate(out):
            if ch in "{[":
                try:
                    return json.loads(out[i:])
                except json.JSONDecodeError:
                    pass
        return None

    @staticmethod
    def _get(obj, *keys):
        """Read a field under any of its spellings.

        The CLI's table output and its --json output disagree on case and
        wording for the same field ('Lvol ID' vs 'lvol_id' vs 'volume_id'),
        and which you get depends on the verb. The scripts hit this
        repeatedly; tolerating all of them is cheaper than tracking which
        verb spells it which way.

        Both sides are reduced to alphanumerics-only lowercase rather than
        guessed at with a list of transformations. The guesses were wrong:
        migrate-list returns "Volume ID" and "Migration ID"
        (migration_controller.list_migrations), and 'volume_id'.title()
        is "Volume Id" -- so every id lookup here silently returned None
        and migration_record never found a record at all.
        """
        if not isinstance(obj, dict):
            return None
        norm = {}
        for key, val in obj.items():
            norm.setdefault(
                "".join(ch for ch in str(key).lower() if ch.isalnum()), val)
        for k in keys:
            flat = "".join(ch for ch in k.lower() if ch.isalnum())
            if norm.get(flat) not in (None, ""):
                return norm[flat]
        return None

    # ── topology ─────────────────────────────────────────────────────────
    def online_nodes(self):
        rows = self.sbcli_utils.get_storage_nodes().get("results", [])
        return [n for n in rows
                if str(n.get("status", "")).lower() in ("online", "active")]

    def node_secondary(self, node_id, nodes=None):
        for n in (nodes or self.online_nodes()):
            if (n.get("uuid") or n.get("id")) == node_id:
                return (n.get("secondary_node_id")
                        or n.get("secondary_node")
                        or self._get(n, "secondary"))
        return None

    def node_tertiary(self, node_id, nodes=None):
        for n in (nodes or self.online_nodes()):
            if (n.get("uuid") or n.get("id")) == node_id:
                return (n.get("tertiary_node_id")
                        or n.get("tertiary_node")
                        or self._get(n, "tertiary"))
        return None

    def any_target(self, source_id, what="target"):
        """Draw a target from EVERY other online node, and name the shape.

        Not :meth:`pick_target`, which draws only from outside the
        source's HA set. Dev, on what a randomised run has to cover:

            "The random targets are also important to ensure you also hit
            secondaries, tertiary and non-related members."

        A target that is the source's secondary puts the volume on a node
        that already holds its replica; a tertiary is a third shape again;
        and the overlap matrix proves each of those works ONCE, in a quiet
        cluster, on a deterministic pick. A long random run is where they
        get hit repeatedly, in combination, under load -- but only if the
        draw includes them, which "no-overlap" by construction does not.

        The relationship is logged rather than merely drawn, because
        "migrated to node X" in a failure report does not say whether X
        was the source's secondary, and that is usually the first thing
        worth knowing.
        """
        nodes = self.online_nodes()
        cands = [n.get("uuid") or n.get("id") for n in nodes
                 if (n.get("uuid") or n.get("id")) != source_id]
        if not cands:
            raise MigrationPreconditionError(
                f"[MIG] no node to migrate to besides {source_id}")
        tgt = self.pick(cands, what)
        self.logger.info("[MIG] %s: %s -> %s (%s)", what, source_id, tgt,
                         self.ha_shape(source_id, tgt, nodes))
        return tgt

    def ha_shape(self, source_id, target_id, nodes=None):
        """How the target is related to the source, in words.

        The same five shapes the overlap matrix names, so a random run's
        log and the matrix's cases speak one vocabulary.
        """
        nodes = nodes or self.online_nodes()
        if self.node_secondary(source_id, nodes) == target_id:
            return "overlap a: target is the source's SECONDARY"
        if self.node_tertiary(source_id, nodes) == target_id:
            return "overlap c: target is the source's TERTIARY"
        if self.node_secondary(target_id, nodes) == source_id:
            return "overlap b: the source is the target's SECONDARY"
        if self.node_tertiary(target_id, nodes) == source_id:
            return "overlap d: the source is the target's TERTIARY"
        return "no-overlap: unrelated to the source's HA set"

    def pick_target(self, source_id, overlap="no-overlap"):
        """Pick a target node for a given HA overlap shape.

        The overlap matrix is the heart of the topology lane, and the four
        shapes are not interchangeable -- each puts the target in a
        different relationship to the source's HA partners, and the paths
        and ANA states that result differ:

            no-overlap  target shares no HA role with the source
            a           target's PRIMARY is the source's SECONDARY
            b           source's PRIMARY is the target's SECONDARY
            c           target's PRIMARY is the source's TERTIARY
            d           source's PRIMARY is the target's TERTIARY

        Raises rather than silently falling back for b/c/d: a test asked
        for a specific topology, and quietly giving it a different one
        produces a pass that means nothing.
        """
        nodes = self.online_nodes()
        cands = [n for n in nodes
                 if (n.get("uuid") or n.get("id")) != source_id]
        if not cands:
            raise MigrationPreconditionError(
                "[MIG] no candidate target node: the cluster has one online "
                "storage node, so there is nowhere to migrate to.")
        sec = self.node_secondary(source_id, nodes)
        ter = self.node_tertiary(source_id, nodes)

        def nid(n):
            return n.get("uuid") or n.get("id")

        if overlap == "no-overlap":
            ha = {sec, ter} - {None}
            free = [n for n in cands if nid(n) not in ha]
            # Drawn, not free[0]. Every node outside the source's HA set
            # is an equally valid no-overlap target, and taking the first
            # the API lists means a six-node cluster only ever tests one
            # of the four available shapes of this case.
            chosen = (self.pick([nid(n) for n in free], "no-overlap target")
                      if free else self.pick([nid(n) for n in cands],
                                             "fallback target"))
            if not free:
                self.logger.warning(
                    "[MIG] no node outside the source's HA set %s; using %s, "
                    "so this run is NOT really no-overlap", ha, chosen)
            return chosen
        if overlap == "a":
            if sec and any(nid(n) == sec for n in cands):
                return sec
            raise MigrationPreconditionError(
                f"[MIG] overlap 'a' needs the source's secondary ({sec}) to "
                f"be an online, non-source node. It is not.")
        if overlap == "b":
            for n in cands:
                if self.node_secondary(nid(n), nodes) == source_id:
                    return nid(n)
            raise MigrationPreconditionError(
                f"[MIG] overlap 'b' needs a node whose SECONDARY is the "
                f"source ({source_id}). No such node in this cluster.")
        if overlap == "c":
            if ter and any(nid(n) == ter for n in cands):
                return ter
            raise MigrationPreconditionError(
                f"[MIG] overlap 'c' needs the source's tertiary ({ter}); "
                f"this cluster has none configured or it is not online.")
        if overlap == "d":
            for n in cands:
                if self.node_tertiary(nid(n), nodes) == source_id:
                    return nid(n)
            raise MigrationPreconditionError(
                f"[MIG] overlap 'd' needs a node whose TERTIARY is the "
                f"source ({source_id}). No such node in this cluster.")
        raise MigrationPreconditionError(f"[MIG] unknown overlap {overlap!r}")

    def lvol_nqn(self, vol_id):
        """The NVMe-oF subsystem NQN this volume is served under."""
        try:
            d = self.sbcli_utils.get_lvol_details(lvol_id=vol_id)
            rows = d.get("results", d) if isinstance(d, dict) else d
            row = (rows[0] if isinstance(rows, list) and rows else rows) or {}
            return row.get("nqn")
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[MIG] could not read the nqn of %s: %s",
                                vol_id, str(exc)[:140])
            return None

    def shares_subsystem(self, vol_id, other_id):
        """True when two volumes are namespaces of ONE subsystem.

        Worth asking rather than assuming: `snapshot clone --namespaced`
        defaults to true and cannot be turned off from the CLI (SFAM-2819),
        so clones land in their parent's subsystem and the single-volume
        migrate verb refuses them, pointing at --batch.
        """
        a, b = self.lvol_nqn(vol_id), self.lvol_nqn(other_id)
        return bool(a) and a == b

    def lvol_node(self, vol_id):
        """Which node currently hosts the volume. The placement assertion."""
        try:
            d = self.sbcli_utils.get_lvol_details(lvol_id=vol_id)
            rows = d.get("results", d) if isinstance(d, dict) else d
            row = (rows[0] if isinstance(rows, list) and rows else rows) or {}
            return row.get("node_id")
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[MIG] could not read the node of %s: %s",
                                vol_id, str(exc)[:140])
            return None

    # ── the two-phase protocol ───────────────────────────────────────────
    #: `volume migrate` prints "Migration ID: <uuid>"; `volume migrate
    #: --batch` prints "Migration Group ID: <uuid>" instead (clibase.py:978
    #: vs :992). MIG_ID_RE does not match the group line -- "Migration ID:"
    #: is not a substring of "Migration Group ID:" -- so every batch
    #: pre-create looked like it had returned nothing, retried five times
    #: against a product that idempotently reused the group it had already
    #: built, and then failed. Three of the four batch cases died that way
    #: on a pre-create that had in fact succeeded.
    MIG_ID_RE = re.compile(r"(?<!Group )Migration ID:\s*([0-9a-f-]{36})",
                           re.IGNORECASE)
    GROUP_ID_RE = re.compile(r"Migration Group ID:\s*([0-9a-f-]{36})",
                             re.IGNORECASE)

    # ── Kubernetes: the same move, raised as a CR ────────────────────────
    #
    # k8s does not use the CLI for this at all. A move is a cluster-scoped
    # PersistentVolumeOps naming a PV and a target StorageNode, and the
    # OPERATOR performs both halves of the handshake itself: ``Validating``
    # POSTs the migration and starts one Job per consuming node to connect
    # and verify the target paths, ``Migrating`` continues it, and
    # ``Verifying`` tears those Jobs and the controllers they left down
    # (operator docs/designs/crd-redesign/design-persistentvolumeops.md §5).
    #
    # So on k8s there is no pre-create/continue pair for a test to drive,
    # and nothing for it to connect. What a case does instead is author the
    # CR and watch the step graph -- which is the assertion worth making
    # there, because it is the one a real k8s user's move actually makes.
    #
    # The registered VolumeMigration kind is deliberately NOT used: the
    # operator's main.go defaults --legacy-volume-migration to false, so a
    # CR of that kind is reconciled by nobody and the move silently never
    # happens.

    #: ``status.phase`` -> the status words the docker migration records
    #: use, so :meth:`await_migration` and every placement assertion in
    #: this lane read ONE vocabulary on both backends.
    PVO_STATUS: ClassVar = {
        "succeeded": "done",
        "failed": "failed",
        "aborted": "cancelled",
        "running": "running",
        "pending": "pending",
    }
    #: The k8s step graph. NOT the same vocabulary as the docker
    #: :attr:`PHASES` and deliberately not mapped onto it -- these are the
    #: operator's steps, those are the control plane's phases, and a case
    #: asserting on one cannot assert on the other. A shared case asks
    #: :meth:`phases_for_backend` instead of naming either.
    K8S_STEPS = ("Validating", "Migrating", "Verifying")

    def phases_for_backend(self):
        """The ordered step names this backend actually reports."""
        return self.K8S_STEPS if self.k8s_test else self.PHASES

    #: Which k8s step a control-plane phase happens inside. The k8s graph
    #: is COARSER -- three of the four control-plane phases are the one
    #: Migrating step, because Migrating's side effect is "continue the
    #: migration, which is what starts the data copy" and the copy's
    #: internal phases are the control plane's business, not the
    #: operator's. So "inject the fault during snap_copy" becomes "inject
    #: it during the copy" on k8s. The INTENT survives; the precision
    #: does not, and _k8s_steps_for says so out loud rather than letting a
    #: k8s pass be read as proof the fault landed in a specific phase.
    PHASE_TO_K8S_STEP: ClassVar = {
        "snap_copy": "migrating",
        "intermediate": "migrating",
        "lvol_migrate": "migrating",
        "cleanup_source": "verifying",
        "cleanup_target": "verifying",
    }

    def _k8s_steps_for(self, phases):
        """Translate control-plane phase names to k8s step names."""
        out, unknown = [], []
        for p in phases:
            step = self.PHASE_TO_K8S_STEP.get(str(p).lower())
            if step:
                out.append(step)
            else:
                # Already a step name, or a phase nothing maps. Pass it
                # through lowered so a case that asks for "Migrating"
                # directly still works.
                out.append(str(p).lower())
                unknown.append(p)
        if unknown:
            self.logger.info(
                "[MIG] phase(s) %s have no k8s mapping and are matched "
                "as-is", unknown)
        uniq = tuple(dict.fromkeys(out))
        self.logger.info(
            "[MIG] waiting on k8s step(s) %s for phase(s) %s -- the k8s "
            "graph is coarser, so this does NOT prove the fault landed in "
            "one specific control-plane phase", list(uniq), list(phases))
        return uniq

    def _pvo_name(self, vol_id):
        """A DNS-1123 name for the operation, unique per attempt.

        Counted because several cases migrate a volume more than once
        (there-and-back), and ``spec.action`` is immutable -- reusing a
        name would patch a finished operation instead of raising a new
        one, and the test would assert on the previous move's status.
        """
        self._pvo_seq = getattr(self, "_pvo_seq", 0) + 1
        short = str(vol_id).replace("-", "")[:12]
        return f"e2e-mig-{short}-{self._pvo_seq}"

    def _pv_for_vol(self, vol_id):
        """The PersistentVolume name backing this lvol.

        The CR names a PV, not an lvol -- the operator resolves the volume
        from it. ``_create_lvol_dual`` already records the PVC it made and
        the lvol uuid the CSI handle resolved to, so this is a lookup and
        not a search of the cluster.
        """
        k8s = self._ensure_k8s_utils()
        for name, reg in self._volume_registry.items():
            if reg.get("lvol_id") != vol_id:
                continue
            if reg.get("pv_name"):
                return reg["pv_name"]
            pvc = reg.get("pvc_name")
            if not pvc:
                continue
            pv = k8s.get_pvc_pv_name(pvc)
            if not pv:
                raise MigrationPreconditionError(
                    f"[MIG] PVC {pvc} (volume {name}) is bound to no PV, so "
                    f"there is nothing a PersistentVolumeOps could name.")
            reg["pv_name"] = pv
            return pv
        raise MigrationPreconditionError(
            f"[MIG] nothing in this test's volume registry resolves to lvol "
            f"{vol_id}. A k8s move is addressed by PV name, so a volume not "
            f"created through _create_lvol_dual cannot be migrated here.")

    def _k8s_migrate(self, vol_id, target_node_id, batch=False):
        """Raise the move as a PersistentVolumeOps. Returns its name."""
        k8s = self._ensure_k8s_utils()
        pv = self._pv_for_vol(vol_id)
        # pick_target deals in backend node uuids; the CR wants a
        # StorageNode CR name. They are two identifiers for one node and
        # status.uuid is what ties them together.
        cr = k8s.resolve_storage_node_cr_name(target_node_id)
        name = self._pvo_name(vol_id)
        if batch:
            # Not a flag here, and not a gap. A migration is addressed by
            # SUBSYSTEM: "every volume on that subsystem moves at once and
            # every host consuming one of them is affected at the same
            # instant" (§5). Naming one member moves the group, so there
            # is nothing for --batch to turn on. The corollary is that the
            # CLI's refusal to move a shared volume singly has no k8s
            # counterpart, and cases built on that refusal are docker-only.
            self.logger.info(
                "[MIG] batch is implicit on k8s: the operation names one "
                "PV and the whole subsystem moves with it.")
        k8s.create_persistent_volume_ops(name, pv, cr)
        self._migrations.append(name)
        self._k8s_pvos.append(name)
        self.logger.info(
            "[MIG] raised PersistentVolumeOps %s: %s (lvol %s) -> "
            "StorageNode %s (node %s)", name, pv, vol_id, cr, target_node_id)
        return name

    def _k8s_migration_record(self, migration_id=None, vol_id=None):
        """One PersistentVolumeOps, flattened into a docker-shaped record.

        Returning the keys both backends' assertions already read is what
        lets a case be written once. The mapping is narrow on purpose --
        status, step, error and the ids. What the operator reports and the
        control plane does not (``validationJobs``, ``memberCount``,
        ``connections``) is passed through under its own name for the
        cases that want it, and is simply absent on docker.
        """
        k8s = self._ensure_k8s_utils()
        res = {}
        if migration_id:
            res = k8s.get_persistent_volume_ops(migration_id)
        elif vol_id:
            try:
                pv = self._pv_for_vol(vol_id)
            except MigrationPreconditionError:
                pv = None
            if pv:
                for item in k8s.list_persistent_volume_ops():
                    if (item.get("spec") or {}).get(
                            "persistentVolumeName") == pv:
                        res = item
        if not res:
            return None
        st = res.get("status") or {}
        mig = st.get("migration") or {}
        step = st.get("step") or {}
        phase = str(st.get("phase") or "").lower()
        return {
            "uuid": (res.get("metadata") or {}).get("name"),
            "status": self.PVO_STATUS.get(phase, phase or "unknown"),
            # The operator's step, under the key the docker record calls
            # phase. Different words, same role: where in the graph it is.
            # status.step is an object, like StorageNodeOps' -- the name
            # is under .state, and reading step itself gives a dict that
            # str()s into something no assertion will ever match.
            "phase": str(step.get("state") or ""),
            "error_message": st.get("message") or "",
            "lvol_id": mig.get("volumeUUID") or "",
            "migration_id": mig.get("migrationUUID") or "",
            "k8s_phase": st.get("phase") or "",
            "step_deadline": step.get("deadline") or "",
            "member_count": mig.get("memberCount"),
            "subsystem_nqn": mig.get("subsystemNQN"),
            "source_node_uuid": mig.get("sourceNodeUUID"),
            "target_node_uuid": mig.get("targetNodeUUID"),
            "validation_jobs": mig.get("validationJobs"),
            "connections": mig.get("connections"),
        }

    def migrate(self, vol_id, target_node_id, batch=False, ctrl_loss_tmo=3600,
                host_nqn=None, retries=5, retry_interval=10,
                rebalancing_timeout=300):
        """Phase one: pre-create. Returns the migration id.

        Also connects the client to the returned paths, because that is the
        step the protocol is built around: the new paths come up ANA
        inaccessible and the source keeps serving, so the later cutover is
        a flip rather than a disconnect. Skipping it turns the whole
        exercise into a different, easier test.

        Retries on two distinct conditions the scripts learned the hard way:
        a node reporting online only means its status flipped in the DB, not
        that its lvstore and RPC layer are back, so a pre-create right after
        a restart can transiently fail; and a cluster that is rebalancing
        refuses new migrations for a minute or two, which deserves a longer,
        separate budget rather than burning the ordinary retries.
        """
        if self.k8s_test:
            # One call does the whole thing on k8s. ctrl_loss_tmo is not
            # ours to pass: the operator sets it from the CSI driver's
            # value on purpose, "because a target path becomes the
            # volume's data path at cutover and one volume's paths must
            # not sit on two different timeouts depending on which of them
            # last moved" (jobs.go). host_nqn likewise belongs to the Jobs.
            if host_nqn:
                self.logger.info(
                    "[MIG] host_nqn is ignored on k8s: the operator starts "
                    "one validation Job per CONSUMING NODE and each uses "
                    "its own host nqn.")
            return self._k8s_migrate(vol_id, target_node_id, batch=batch)

        cmd = (f"{self.base_cmd} --dev volume migrate {vol_id} "
               f"{target_node_id} --ctrl-loss-tmo {ctrl_loss_tmo}")
        if batch:
            cmd += " --batch"
        if host_nqn:
            cmd += f" --host-nqn {host_nqn}"

        reb_deadline, attempt, last = None, 0, ""
        while True:
            attempt += 1
            out, err = self._cli(cmd + " 2>&1")
            combined = out + err
            # --batch returns a GROUP id under a different label. Accept the
            # group line first when we asked for a batch, and fall back to
            # the single-volume line, so a product that stops distinguishing
            # them does not break this.
            m = (self.GROUP_ID_RE.search(combined) if batch else None) \
                or self.MIG_ID_RE.search(combined)
            if m:
                mid = m.group(1)
                self._migrations.append(mid)
                self._connect_target_paths(
                    combined, node=self._client_for_vol(vol_id))
                self.logger.info("[MIG] pre-created %s -> %s (%s %s)",
                                 vol_id, target_node_id,
                                 "group" if batch else "migration", mid)
                return mid
            last = combined
            if "rebalancing" in combined.lower():
                if reb_deadline is None:
                    reb_deadline = time.time() + rebalancing_timeout
                left = reb_deadline - time.time()
                if left <= 0:
                    break
                self.logger.warning(
                    "[MIG] cluster is rebalancing; a restart kicks off a "
                    "balancing task that blocks new migrations. Retrying "
                    "(%.0fs of budget left)", left)
                sleep_n_sec(min(30, left))
                continue
            if attempt >= retries:
                break
            self.logger.warning(
                "[MIG] pre-create %d/%d returned no migration id; a node can "
                "report online before its lvstore is back. Retrying in %ds",
                attempt, retries, retry_interval)
            sleep_n_sec(retry_interval)

        raise MigrationPreconditionError(
            f"[MIG] no Migration ID after {attempt} attempt(s) for {vol_id}. "
            f"Last output: {last[:300]!r}")

    def _client_for_vol(self, vol_id):
        """The client machine this volume is actually attached to.

        Not fio_node[0]. _connect_and_mount_dual searches every client and
        records the one the device turned up on, and with two clients
        configured those are regularly different machines. Attaching the
        target paths to the wrong one leaves the client that holds the
        volume connected to the source only, which turns the cutover into
        the disconnect this whole protocol exists to avoid.
        """
        for name in reversed(self._mig_vols):
            reg = self._volume_registry.get(name) or {}
            node = reg.get("node")
            if not node:
                continue
            try:
                if self.sbcli_utils.get_lvol_id(lvol_name=name) == vol_id:
                    return node
            except Exception:                         # noqa: BLE001
                continue
        return self.fio_node[0] if self.fio_node else self.mgmt_nodes[0]

    #: `sudo nvme connect ...` -- simplyblock_core/utils/nvme.py:118 builds
    #: the string with the sudo already on it. Matching bare "nvme connect"
    #: found nothing, so every pre-create logged "returned no nvme connect
    #: strings" and the cutover ran as a disconnect; prefixing another sudo
    #: onto what we did match would have produced `sudo sudo nvme connect`.
    CONNECT_RE = re.compile(r"^\s*(?:sudo\s+)?(nvme\s+connect(?=\s|$).*)$")

    def _connect_target_paths(self, output, node=None):
        """Connect the client to the target's inaccessible-ANA paths."""
        if self.k8s_test:
            # Never on k8s, and not because it is unimplemented. The
            # operator starts a Job per consuming node which connects the
            # paths itself -- "it reads the node's own NVMe fabric and
            # connects paths on it, and neither is visible from the
            # operator's pod" (jobs.go). Racing it for the same paths is
            # how the defect that created the Verifying step happened:
            # paths a checker left behind "poisoned the data path, and
            # blocked every later migration on that volume".
            self.logger.info(
                "[MIG] not connecting target paths: on k8s the operator's "
                "validation Jobs own that step.")
            return 0
        cmds = []
        for line in output.splitlines():
            m = self.CONNECT_RE.match(line)
            if m:
                cmds.append(m.group(1).strip())
        if not cmds:
            self.logger.warning(
                "[MIG] pre-create returned no nvme connect strings. The "
                "cutover needs the client attached to BOTH ends first; "
                "without it this becomes a disconnect, not an ANA flip.")
            return 0
        node = node or (self.fio_node[0] if self.fio_node
                        else self.mgmt_nodes[0])
        for c in cmds:
            self.ssh_obj.exec_command(node=node,
                                      command=f"sudo {c} 2>&1 || true")
        sleep_n_sec(3)
        self.logger.info("[MIG] connected %d target path(s) on %s",
                         len(cmds), node)
        return len(cmds)

    def migrate_continue(self, migration_id, batch=False, max_retries=10,
                         deadline=14400, retry_on_failure=False):
        """Phase two: start the copy and the task runner."""
        if self.k8s_test:
            # Nothing to call. The operator continues the migration on
            # entry to its Migrating step, which is "what starts the data
            # copy" (§5). Returning rather than raising keeps a shared
            # case readable: it still says migrate() then
            # migrate_continue() then await, and on k8s the middle line is
            # simply the operator's job rather than the test's.
            self.logger.info(
                "[MIG] migrate-continue is implicit on k8s: the operator "
                "continues the migration when it enters Migrating.")
            return "", ""
        cmd = (f"{self.base_cmd} --dev volume migrate-continue {migration_id} "
               f"--max-retries {max_retries} --deadline {deadline}")
        if batch:
            cmd += " --batch"
        if retry_on_failure:
            cmd += " --retry-on-failure"
        out, err = self._cli(cmd + " 2>&1")
        self.logger.info("[MIG] migrate-continue %s: %s", migration_id,
                         (out + err).strip()[:200])
        return out, err

    def migrate_cancel(self, migration_id, batch=False):
        """Force an in-flight migration down the rollback path.

        Deterministic, unlike racing a short --deadline: it goes straight to
        cleanup_target and ends cancelled, which is the same path a real
        failure takes.
        """
        if self.k8s_test:
            # spec.abort, which the graph allows from Validating and
            # Migrating only. Arriving in Verifying it is an
            # IllegalTransitionError the controller reports, because by
            # then the volume has moved and there is nothing to undo --
            # so a late abort is not a harness failure and is not retried.
            out, err = self._ensure_k8s_utils().abort_persistent_volume_ops(
                migration_id)
            combined = (out or "") + (err or "")
            self.logger.info("[MIG] abort requested on %s: %s", migration_id,
                             combined.strip()[:200])
            return combined
        cmd = f"{self.base_cmd} --dev volume migrate-cancel {migration_id}"
        if batch:
            cmd += " --batch"
        out, err = self._cli(cmd + " 2>&1")
        combined = (out + err)
        if "cancel" not in combined.lower():
            self.logger.warning(
                "[MIG] migrate-cancel for %s did not report success: %r. The "
                "runner has a known canceled-flag race, so the migration may "
                "still be running.", migration_id, combined.strip()[:200])
        return combined

    def migrate_cleanup(self, migration_id):
        """Idempotently remove whatever the migration left on the target."""
        if self.k8s_test:
            # Deleting the operation is the k8s equivalent, and it is the
            # supported one at every step that admits it: a non-terminal
            # object's finalizer cancels the backend migration, deletes
            # the Jobs, leaves no path connected and releases the
            # volume's lock before it clears (§5.1). Two things a caller
            # should expect rather than treat as failures -- a delete is
            # REFUSED during Verifying, because that step declares no
            # abort edge and is itself the cleanup; and a finalizer that
            # cannot finish deliberately BLOCKS the delete and leaves the
            # object with the reason in status.message, which is a stuck
            # cleanup to report, not to retry around.
            out, err = self._ensure_k8s_utils().delete_persistent_volume_ops(
                migration_id)
            return (out or "") + (err or "")
        out, _ = self._cli(f"{self.base_cmd} --dev volume migrate-cleanup "
                           f"{migration_id} 2>&1")
        return out

    def migration_record(self, vol_id=None, migration_id=None):
        if self.k8s_test:
            return self._k8s_migration_record(migration_id=migration_id,
                                              vol_id=vol_id)
        data = self._cli_json(f"{self.base_cmd} volume migrate-list "
                              f"--cluster-id {self.cluster_id}")
        rows = data.get("results", data) if isinstance(data, dict) else data
        for m in (rows or []):
            if migration_id and self._get(m, "uuid", "id", "migration_id") == migration_id:
                return m
            if vol_id and self._get(m, "lvol_id", "volume_id") == vol_id:
                return m
        return None

    def batch_record(self, group_id):
        if self.k8s_test:
            # There is no group object to list. One operation moves the
            # whole subsystem, so the operation IS the group -- and
            # memberCount on it is the member count a docker group record
            # would have carried.
            return self._k8s_migration_record(migration_id=group_id)
        data = self._cli_json(f"{self.base_cmd} volume migrate-group-list "
                              f"--cluster-id {self.cluster_id}")
        rows = data.get("results", data) if isinstance(data, dict) else data
        for g in (rows or []):
            if self._get(g, "uuid", "id", "group_id") == group_id:
                return g
        return None

    # ── waiting ──────────────────────────────────────────────────────────
    def await_migration(self, vol_id=None, migration_id=None,
                        terminal_only=True, timeout=None, what="",
                        watcher=None):
        """Poll until terminal (or cutover, if terminal_only is False).

        Logs the record's own error and retry fields every poll. Without
        them a stuck migration is an opaque wall of 'status=suspended' with
        no hint why -- the scripts added this after exactly that.
        """
        timeout = timeout or self.migration_timeout()
        deadline = time.time() + timeout
        last = {}
        seen = False
        while time.time() < deadline:
            m = (self.migration_record(vol_id=vol_id, migration_id=migration_id)
                 or {})
            if not m:
                # A record that EXISTED and then vanished has been cleaned up,
                # which is a completed migration. One that was never there is
                # a record we cannot read -- and returning "done" for that is
                # a false pass. It is not hypothetical: _get could not match
                # migrate-list's "Volume ID"/"Migration ID" spellings at all,
                # so every lane calling this directly would have been handed
                # "done" without a single migration having been observed.
                if seen:
                    self.logger.info("[MIG] record gone -- treating as done%s",
                                     f" ({what})" if what else "")
                    return "done", ""
                self.logger.info(
                    "[MIG] no migration record yet%s; still waiting",
                    f" ({what})" if what else "")
                sleep_n_sec(self.MIGRATION_POLL)
                continue
            seen = True
            last = m
            status = str(self._get(m, "status") or "unknown").lower()
            phase = str(self._get(m, "phase") or "").lower()
            err = self._get(m, "error_message", "error")
            # Stamp the sampler with the step, so an ANA transition reads
            # as "during Migrating" rather than as a bare timestamp. The
            # operator's own sampler does this and the reasoning holds for
            # ours: for split-brain especially, both ends serving during
            # the cutover and during Validating are not the same finding.
            if watcher is not None and phase:
                try:
                    watcher.set_phase(phase)
                except Exception:                     # noqa: BLE001
                    pass
            line = (f"[MIG] status={status} phase={phase} "
                    f"snaps={self._get(m, 'snaps') or ''} "
                    f"retries={self._get(m, 'retries') or ''}")
            terminal = status in self.TERMINAL
            if err:
                # Truncated while polling, because the same error repeats
                # on every poll and would bury the log. NOT truncated once
                # the migration is terminal: run 20261013-221421 cut two
                # failures off at exactly the interesting clause --
                #
                #   the migration failed: delete LVS_31/SNAP_187m: ...
                #   (taking the migration back also failed: cancel
                #    migration 58205fd2-...: cancel mig
                #
                # -- so why the ROLLBACK failed, which is the more serious
                # half of both failures, was the part that got cut, and
                # the state it describes is gone with the cluster.
                self.logger.warning(
                    "%s error=%r", line,
                    str(err) if terminal else str(err)[:200])
            else:
                self.logger.info(line)
            if terminal:
                return status, phase
            if status == "cutover" and not terminal_only:
                return "cutover", phase
            sleep_n_sec(self.MIGRATION_POLL)
        if not seen:
            how = (f"`kubectl get persistentvolumeops.storage.simplyblock.io "
                   f"{migration_id or ''} -o yaml`"
                   if self.k8s_test else
                   f"`{self.base_cmd} volume migrate-list --cluster-id "
                   f"{self.cluster_id} --json`")
            raise AssertionError(
                f"[MIG] no migration record was EVER readable within "
                f"{timeout}s{(' (' + what + ')') if what else ''} for "
                f"vol_id={vol_id!r} migration_id={migration_id!r}. Either "
                f"it is not listed at all, or the field names this harness "
                f"matches on differ from what it returns. Run {how} by hand "
                f"and compare the keys.")
        raise AssertionError(
            f"[MIG] migration did not reach a terminal state within "
            f"{timeout}s{(' (' + what + ')') if what else ''}. Last record: "
            f"{last}. A migration that never terminates is itself the bug -- "
            f"it must always end somewhere, even if that somewhere is failed.")

    def await_batch(self, group_id, timeout=None, what=""):
        # A batch is where the per-member term bites: the operator gives
        # the copy 30 minutes plus ten per member, so a 4-member group has
        # 70 minutes for that step alone and a fixed wait would call it
        # stuck at 15.
        timeout = timeout or self.migration_timeout(
            members=getattr(self, "MEMBERS", 4))
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            g = self.batch_record(group_id)
            if not g:
                return "done", ""
            last = g
            status = str(self._get(g, "status") or "unknown").lower()
            phase = str(self._get(g, "phase") or "").lower()
            self.logger.info("[MIG] batch status=%s phase=%s members=%s",
                             status, phase, self._get(g, "member_count"))
            if status in self.TERMINAL:
                return status, phase
            sleep_n_sec(self.MIGRATION_POLL)
        raise AssertionError(
            f"[MIG] batch {group_id} did not terminate within {timeout}s"
            f"{(' (' + what + ')') if what else ''}. Last: {last}")

    def await_phase(self, vol_id, phase_keywords, timeout=300):
        """Wait until the migration is in one of *phase_keywords*.

        The fault lane needs this: "kill the target during snap_copy" only
        means something if the kill lands in snap_copy. Returns the phase
        seen, or None if the migration terminated before reaching it -- the
        caller then knows its fault was never injected, which is a skip and
        not a pass.
        """
        want = ((phase_keywords,) if isinstance(phase_keywords, str)
                else tuple(phase_keywords))
        if self.k8s_test:
            want = self._k8s_steps_for(want)
        deadline = time.time() + timeout
        while time.time() < deadline:
            m = self.migration_record(vol_id=vol_id) or {}
            if not m:
                return None
            status = str(self._get(m, "status") or "").lower()
            phase = str(self._get(m, "phase") or "").lower()
            if any(k in phase for k in want):
                self.logger.info("[MIG] reached phase %r", phase)
                return phase
            if status in self.TERMINAL:
                self.logger.warning(
                    "[MIG] migration terminated (%s) before reaching %s -- "
                    "the fault was never injected into that phase",
                    status, list(want))
                return None
            sleep_n_sec(2)
        return None

    def snap_progress(self, vol_id):
        """(copied, total) from the record's Snaps field, or (None, None)."""
        m = self.migration_record(vol_id=vol_id) or {}
        raw = str(self._get(m, "snaps") or "")
        mm = re.match(r"\s*(\d+)\s*/\s*(\d+)", raw)
        return (int(mm.group(1)), int(mm.group(2))) if mm else (None, None)

    # ── convenience ──────────────────────────────────────────────────────
    def full_migration(self, vol_id, target_node_id, batch=False,
                       deadline=14400, expect="done", what=""):
        """Pre-create, connect, continue, wait. The happy path in one call.

        Also watches the cutover window and asserts the NQN survived, so
        every case that migrates through here gets both for free. Both are
        cheap and both catch things no other assertion in the lane can:
        a retried cutover, and a move that silently re-identified the
        volume.
        """
        # Read the identity BEFORE the move. "A new volume is created on
        # the target node with the same identity (NQN) as the source"
        # (docs/architecture/concepts/volume-migration.md) -- that sameness
        # is the whole reason a client does not have to reconnect, and
        # nothing else in the lane checks it.
        nqn_before = self.lvol_nqn(vol_id)

        watcher = self.watch_cutover(vol_id, what=what,
                                     source=self.lvol_node(vol_id),
                                     target=target_node_id)
        try:
            mid = self.migrate(vol_id, target_node_id, batch=batch)
            self.migrate_continue(mid, batch=batch, deadline=deadline)
            if batch:
                status, phase = self.await_batch(mid, what=what)
            else:
                status, phase = self.await_migration(
                    migration_id=mid, vol_id=vol_id, what=what,
                    watcher=watcher)
        except BaseException:
            # Stop the sampler on any exit, or its thread outlives a failed
            # case and keeps ssh-ing to a node the next case is using.
            if watcher is not None:
                try:
                    watcher.stop()
                except Exception:                     # noqa: BLE001
                    pass
            raise

        if expect and status not in (
                self.OK_TERMINAL if expect == "done" else (expect,)):
            if watcher is not None:
                watcher.stop()
            raise AssertionError(
                f"[MIG] migration of {vol_id} ended {status!r} (phase "
                f"{phase!r}), expected {expect!r}{(' -- ' + what) if what else ''}")

        # Only judge the window on a migration that was SUPPOSED to cut
        # over. A cancelled or failed move never flips ANA, so its paths
        # going inaccessible is the rollback working, not a freeze.
        if status in self.OK_TERMINAL:
            self.assert_cutover_clean(watcher, "MIG", what=what)
            nqn_after = self.lvol_nqn(vol_id)
            if nqn_before and nqn_after and nqn_before != nqn_after:
                self._gate("MIG", 
                    f"the subsystem NQN of {vol_id} CHANGED across the "
                    f"migration{(' (' + what + ')') if what else ''}: "
                    f"{nqn_before} -> {nqn_after}. The target is documented "
                    f"to take the same identity as the source, which is what "
                    f"lets a client keep its connection; a new NQN means "
                    f"every consumer has to reconnect and anything holding "
                    f"the old one is now pointed at nothing.")
        elif watcher is not None:
            watcher.stop()
        return mid, status, phase

    def make_volume(self, name, size=None, pool=None, host_id=None,
                    seed=True):
        """Create a volume, optionally on a named node, and seed it."""
        if self.k8s_test:
            # A PVC, not `volume add`. This is not a style preference: a
            # k8s move is addressed by PV NAME, and a volume created
            # through the CLI on a k8s cluster has no PersistentVolume at
            # all -- so there would be nothing for a PersistentVolumeOps
            # to name and every case in this lane would die in
            # _pv_for_vol. It is also what a k8s user actually has.
            _, vol_id = self._create_lvol_dual(
                name, size or self.VOL_SIZE, pool_name=pool or self.pool_name,
                host_id=host_id)
            self._mig_vols.append(name)
            return vol_id, (self.seed(name) if seed else {})

        # `volume add <name> <size> <pool>` -- the pool is POSITIONAL.
        # There is no --pool flag; passing one is an argparse error, which is
        # how every case in this lane failed on its first line.
        cmd = (f"{self.base_cmd} -d volume add {name} "
               f"{size or self.VOL_SIZE} {pool or self.pool_name}")
        if host_id:
            cmd += f" --host-id {host_id}"
        out, err = self._cli(cmd + " 2>&1")
        if cli_failed(out, err):
            raise MigrationPreconditionError(
                f"[MIG] could not create {name}: {(out + err)[:300]}")
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=name)
        self._mig_vols.append(name)
        sums = self.seed(name) if seed else {}
        return vol_id, sums

    def warn_if_autoplacement_on(self):
        """Say so if the cluster will migrate volumes behind our back.

        ``StorageCluster.spec.enableVolumeAutoPlacement`` makes the
        operator move volumes off nodes it judges hot, creating the very
        same ``PersistentVolumeOps`` objects this lane creates. It is off
        by default, which is why this is a warning and not a precondition
        failure -- but on a cluster where someone turned it on, a volume
        this lane did not move arrives somewhere else mid-case, and the
        obvious reading is that the product migrated something it should
        not have. Knowing it is on turns a day of misdiagnosis into a line
        in the log.

        The marked-experimental rebalancer also deletes its own benchmark
        volumes (``^sb-fio-baseline-.*``), so a stray volume appearing and
        vanishing is explained by the same setting.
        """
        if not self.k8s_test:
            return False
        try:
            k8s = self._ensure_k8s_utils()
            name = k8s._storage_cluster_cr_name()
            if not name:
                return False
            cr = k8s.get_resource_json(
                "storageclusters.storage.simplyblock.io", name)
            spec = (cr or {}).get("spec") or {}
            on = bool(spec.get("enableVolumeAutoPlacement"))
            bench = bool((spec.get("volumeAutoPlacement") or {})
                          .get("enableLatencyBenchmark"))
            if on or bench:
                self.logger.warning(
                    "[MIG] this cluster has enableVolumeAutoPlacement=%s "
                    "and enableLatencyBenchmark=%s. The operator may "
                    "migrate volumes on its own while this lane runs, "
                    "which looks exactly like a spurious migration in the "
                    "results. Set both false for a correctness run, or "
                    "read any unexplained placement change as the "
                    "rebalancer rather than a defect.", on, bench)
            else:
                self.logger.info(
                    "[MIG] auto volume placement is off, so every migration "
                    "in this run is one the test asked for")
            return on or bench
        except Exception as exc:                      # noqa: BLE001
            self.logger.info("[MIG] could not read the cluster's auto "
                             "placement settings: %s", str(exc)[:160])
            return False

    # ── the cutover window ───────────────────────────────────────────────

    def _ana_probe(self, vol_id):
        """A callable that runs the ANA probe on the consuming host.

        None when there is nowhere to run it, which makes the check
        unavailable rather than failing: a diagnostic must never be what
        ends a run.
        """
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            # The node the volume is consumed ON, which is where its paths
            # live -- not the storage node it is served from.
            node = None
            for name, reg in self._volume_registry.items():
                if reg.get("lvol_id") != vol_id:
                    continue
                for job in reversed(getattr(self, "_k8s_fio_jobs", []) or []):
                    if self._k8s_normalize_name(name) in job:
                        node = k8s.job_pod_node(job)
                        break
                break
            if not node:
                self.logger.info(
                    "[ANA] no consuming pod found for %s, so its host's "
                    "fabric cannot be read; cutover window not measured",
                    vol_id)
                return None
            pod, container = k8s.csi_node_pod(node)
            if not pod:
                return None
            self.logger.info("[ANA] reading %s's fabric through %s/%s",
                             node, pod, container)
            return lambda cmd: k8s.exec_in_pod_container(pod, container, cmd)

        node = self._client_for_vol(vol_id)
        if not node:
            return None

        def _run(cmd):
            # Short timeout, no retries, deliberately. The default is 360s
            # with three attempts, and a probe that blocks for that long
            # stops sampling during the exact window this exists to
            # measure -- the gap would then read as "no freeze observed".
            # A missed sample is visible in the report (readable count);
            # a stalled sampler is not.
            out, _ = self.ssh_obj.exec_command(
                node=node, command=cmd, supress_logs=True,
                timeout=max(5, int(self.ANA_INTERVAL_S * 3)), max_retries=1)
            return out or ""
        return _run

    def node_data_addrs(self, node_id):
        """Every data-NIC address of a node, as the fabric names it.

        The ANA probe reports a path by its traddr, which is a DATA
        address -- not the mgmt_ip the rest of this lane resolves nodes
        by. Without this mapping the split-brain check cannot tell the
        source's paths from the target's and skips itself.
        """
        out = set()
        try:
            for n in self.online_nodes():
                if (n.get("uuid") or n.get("id")) != node_id:
                    continue
                for nic in (n.get("data_nics") or []):
                    ip = (nic.get("ip4_address") if isinstance(nic, dict)
                          else None)
                    if ip:
                        out.add(str(ip).strip())
                # Some records carry the single serving ip directly.
                for key in ("nvmf_ip", "ip"):
                    if n.get(key):
                        out.add(str(n[key]).strip())
                break
        except Exception as exc:                      # noqa: BLE001
            self.logger.info("[ANA] could not resolve data addresses of "
                             "%s: %s", node_id, str(exc)[:140])
        return out

    def watch_cutover(self, vol_id, what="", source=None, target=None):
        """Sample the subsystem's ANA state across a migration.

        Returns an AnaWatcher already started, or None when the fabric
        cannot be read. Pair it with :meth:`assert_cutover_clean`.

        This is the only check in the lane that can see a cutover that was
        RETRIED. An fio verify catches the resulting lost write only once
        the read pattern comes back around to that block -- the feature
        team measured a 3-34s lag -- and attributes it to nothing. The
        freeze count sees it as it happens.
        """
        nqn = self.lvol_nqn(vol_id)
        if not nqn:
            self.logger.info("[ANA] no nqn for %s; not watching", vol_id)
            return None
        probe = self._ana_probe(vol_id)
        if probe is None:
            return None
        src_addrs = self.node_data_addrs(source) if source else set()
        tgt_addrs = self.node_data_addrs(target) if target else set()
        if not (src_addrs and tgt_addrs):
            self.logger.info(
                "[ANA] source/target data addresses unresolved (src=%d, "
                "tgt=%d); freeze count and pause are still measured, "
                "split-brain is not", len(src_addrs), len(tgt_addrs))
        w = AnaWatcher(nqn, probe, interval=self.ANA_INTERVAL_S,
                       logger=self.logger, source_addrs=src_addrs,
                       target_addrs=tgt_addrs)
        self.logger.info("[ANA] watching the cutover of %s %s", vol_id, what)
        return w.start()

    def assert_cutover_clean(self, watcher, case_id, what=""):
        """Stop *watcher* and fail on a long or repeated freeze."""
        if watcher is None:
            return None
        report = watcher.stop()
        if report.ok is None:
            self.logger.warning(
                "[%s] the cutover window was NOT measured (%s). This case "
                "still proves what it asserts elsewhere, but it did not "
                "check for a retried cutover.", case_id, report.summary())
            return report
        if report.over_budget and not report.duration_fatal:
            # Reported, not failed. The duration budget derives from the
            # BATCH path's `time.sleep(2)` -- which is itself labelled
            # "(diagnostic)" in the product, next to "Re-enable the
            # narrower pre-freeze once this test is done" -- and the
            # single-volume path has no fixed sleep at all. So a long
            # window is worth knowing and is not something to fail a run
            # against a number we cannot defend.
            self.logger.warning(
                "[%s] the cutover pause ran LONG: %.1fs against a %.1fs "
                "budget%s. Not failed: that budget comes from the batch "
                "path's diagnostic 2s sleep and does not govern a "
                "single-volume move, whose window is however long SPDK's "
                "drain takes. The freeze COUNT is the check that failed or "
                "passed here.", case_id, report.stall_s, report.budget,
                (" " + what) if what else "")
        if report.split_brain and not report.freezes > report.freezes_allowed:
            # Only the split-brain arm tripped. New check, so it reports
            # unless MIG_NEW_GATES says otherwise.
            self._gate(case_id,
                       f"the cutover was not clean{(' ' + what) if what else ''}: "
                       + "; ".join(report.violations()) + f". {report.summary()}")
            return report
        if not report.ok:
            raise AssertionError(
                f"[{case_id}] the cutover was not clean{(' ' + what) if what else ''}: "
                + "; ".join(report.violations())
                + f". {report.summary()}. This is the signal that has "
                f"accompanied silent write loss: reads keep succeeding and "
                f"roughly one block per affected member is lost per event, "
                f"so an fio verify may not notice for another 3-34s.")
        self.logger.info("[%s] cutover clean: %s", case_id, report.summary())
        return report

    #: Words a refusal may be phrased with. Deliberately broad: the test
    #: is "did it refuse", and insisting on exact wording makes a reworded
    #: error message a test failure. Where the REASON matters, a case
    #: checks cluster state afterwards instead of trusting the words.
    REFUSAL_MARKERS = ("error", "cannot", "can not", "invalid", "refus",
                       "not allowed", "same", "offline", "not found",
                       "no such", "insufficient", "space", "in progress",
                       "migrating", "migration", "busy", "locked",
                       "protected", "forbidden", "denied")

    def assert_refused(self, case_id, out_err, what, allow=(),
                       reject_id=True):
        """Fail unless the product refused *what*.

        Two failure modes, both real and both easy to write a test that
        misses: the operation was ACCEPTED (an id came back), and the
        operation produced neither an id nor anything recognisable as a
        refusal -- which from an operator's seat is indistinguishable from
        success, so it is treated as one here.
        """
        combined = out_err if isinstance(out_err, str) else "".join(out_err)
        low = combined.lower()
        if reject_id and self.MIG_ID_RE.search(combined):
            raise AssertionError(
                f"[{case_id}] {what} was ACCEPTED and returned a migration "
                f"id. Response: {combined.strip()[:300]}")
        if not any(m in low for m in self.REFUSAL_MARKERS + tuple(allow)):
            raise AssertionError(
                f"[{case_id}] {what} produced no recognisable refusal: "
                f"{combined.strip()[:300] or '(empty)'}. Silence is "
                f"acceptance as far as an operator can tell.")
        self.logger.info("[%s] PASS: refused -- %s", case_id,
                         combined.strip()[:200])

    def snapshots_of(self, vol_id):
        """Every snapshot the control plane holds FOR THIS VOLUME.

        By parent volume id, not by name prefix, and that is the whole
        point. The lane used to count snapshots whose name began with
        the prefix it had asked for, which works on docker and cannot
        work on k8s: there a snapshot is a VolumeSnapshot and the CSI
        driver names the backend object, so `snapshot list` comes back
        holding rows like

            "Name": "snapshot-<uuid>", "BDev": "LVS_19/SNAP_69",
            "LVol ID": "8f001596-..."

        and the logical name appears nowhere. MIG-H-008 and MIG-B-008
        both read that as "0 of 20 snapshots survived the migration" and
        accused the product of silently dropping every recovery point,
        when all twenty were sitting right there under names we had not
        looked for.

        The parent id survives a migration -- a volume keeps its uuid and
        only changes node -- so this is also the thing the case actually
        means: the history moved with the volume.
        """
        data = self._cli_json(f"{self.base_cmd} snapshot list")
        rows = (data.get("results", data) if isinstance(data, dict)
                else data) or []
        out = []
        for row in rows:
            owner = self._get(row, "lvol_id", "lvol", "volume_id")
            if owner and str(owner) == str(vol_id):
                out.append(row)
        return out

    def snap_id(self, name):
        """The backend snapshot uuid for a snapshot NAME, or None.

        NOT the same lookup on both backends, though it reads like it
        should be. On docker the control plane holds the snapshot under
        the name it was asked for, so `snapshot list` finds it. On k8s
        the snapshot is a VolumeSnapshot and the CSI driver names the
        backend object itself -- the logical name appears nowhere in
        `snapshot list`, so matching on it returns None for a snapshot
        that exists perfectly well.

        That is not hypothetical: it took MigrationSnapshotCloneTrees out
        of run 20261013-092206 before it built its tree, with
        "could not resolve the snapshot migtsnap28074
        (handle='migtsnap28074', id=None)". The id was there the whole
        time, in the VolumeSnapshot's snapshotHandle, which
        _get_snapshot_id_dual reads.
        """
        if self.k8s_test:
            try:
                return self._get_snapshot_id_dual(name)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning(
                    "[MIG] could not resolve the backend id of snapshot "
                    "%s from its VolumeSnapshot: %s", name, str(exc)[:160])
                return None
        data = self._cli_json(f"{self.base_cmd} snapshot list")
        for s in (data.get("results", data) if isinstance(data, dict)
                  else data) or []:
            if self._get(s, "snap_name", "name") == name:
                return self._get(s, "uuid", "id")
        return None

    def make_snapshot(self, vol_name, snap_name):
        """Snapshot a volume on either backend. Returns a CLONE HANDLE.

        The handle is what this backend's clone step wants, and it is not
        the same kind of thing on both: docker gives back the backend
        snapshot uuid, k8s gives back the VolumeSnapshot NAME, because a
        clone PVC's ``dataSource`` references the snapshot by name. Pass
        it to :meth:`make_clone` unread. When the backend uuid itself is
        what a case needs -- to assert on ``snapshot list``, say -- ask
        ``_get_snapshot_id_dual`` for it instead of parsing this.
        """
        return self._create_snapshot_dual(vol_name, snap_name)

    def seed_snapshots(self, vol_name, vol_id, prefix, count):
        """Give a volume a snapshot chain for snap_copy to carry.

        These exist only to make the copy do real work -- they are never
        cloned, mounted, or named in an assertion -- which was the excuse
        for taking them with `snapshot add` even on k8s. That excuse does
        not survive contact with the point of the k8s arm: a volume there
        is a PVC and a snapshot is a VolumeSnapshot, and a snapshot the
        CSI layer did not make is not the thing a user has. It is also
        not tracked for teardown, so it outlives the claim it belongs to.

        Returns how many were actually taken. A snapshot that is refused
        is logged and skipped rather than raising: the caller wants a
        chain of about this length, and a chain of eighteen instead of
        twenty tests the same thing.
        """
        taken = 0
        for i in range(count):
            name = f"{prefix}{i}"
            try:
                if self.k8s_test:
                    self.make_snapshot(vol_name, name)
                else:
                    self._cli(f"{self.base_cmd} -d snapshot add {vol_id} "
                              f"{name} 2>&1")
                taken += 1
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning(
                    "[MIG] snapshot %d/%d (%s) was refused: %s; carrying on "
                    "with the %d taken so far", i + 1, count, name,
                    str(exc)[:140], taken)
        if taken:
            self.logger.info("[MIG] %s now carries %d snapshot(s)",
                             vol_name, taken)
        return taken

    def make_clone(self, snap_handle, clone_name, size=None):
        """Clone a snapshot on either backend. Returns (vol_id, detail).

        ``vol_id`` is None when the clone was refused, with the product's
        own words in ``detail``. Reporting rather than raising, because
        each caller in this lane has its own thing to say about a refusal
        and some of them expect one.

        On k8s the clone is a PVC whose dataSource is the VolumeSnapshot.
        That is what gives it a PersistentVolume, and therefore what makes
        it migratable at all -- a clone made with ``snapshot clone`` on a
        k8s cluster has no PV for a PersistentVolumeOps to name, so the
        tree cases would set up fine and then fail in _pv_for_vol.
        """
        if self.k8s_test:
            try:
                self._create_clone_dual(snap_handle, clone_name,
                                        size=size or self.VOL_SIZE)
            except Exception as exc:                  # noqa: BLE001
                return None, str(exc)[:300]
            self._mig_vols.append(clone_name)
            vol_id = (self._volume_registry.get(clone_name)
                      or {}).get("lvol_id")
            if not vol_id:
                return None, (f"clone PVC for {clone_name} bound but no "
                              f"lvol id was resolved from its volumeHandle")
            return vol_id, f"clone PVC {clone_name} bound as {vol_id}"
        out, err = self._cli(f"{self.base_cmd} -d snapshot clone "
                             f"{snap_handle} {clone_name} 2>&1")
        if cli_failed(out, err):
            return None, (out + err)[:300]
        self._mig_vols.append(clone_name)
        return self.sbcli_utils.get_lvol_id(lvol_name=clone_name), (out + err)

    def seed(self, name, files=3, file_size="32M"):
        """Write known content to a fresh volume. Returns {path: md5}.

        The mount is per volume, which this lane needs more than most:
        MIG-T-010 migrates four at once, and a shared mount point would
        have them overwrite each other.
        """
        try:
            return self._seed_volume_dual(name, files=files, size=file_size,
                                          prefix="migseed")
        except AssertionError as exc:
            raise MigrationPreconditionError(
                f"[MIG] could not seed {name}: {exc}") from exc

    def verify(self, name, expected, context=""):
        got = self._generate_checksums_dual(name)
        bad = sorted(f for f in set(expected) & set(got)
                     if expected[f] != got[f])
        missing = sorted(set(expected) - set(got))
        if bad or missing:
            raise AssertionError(
                f"[MIG] {name} does not match what was written {context}. "
                f"differing={bad or 'none'} missing={missing or 'none'}. A "
                f"migration must move the data unchanged; this is the whole "
                f"point of the operation.")
        self.logger.info("[MIG] %s verified (%d files) %s", name, len(got),
                         context)
        return True

    def assert_placed_on(self, vol_id, node_id, context=""):
        got = self.lvol_node(vol_id)
        if got != node_id:
            raise AssertionError(
                f"[MIG] {vol_id} is on node {got!r}, expected {node_id!r} "
                f"{context}. The database and the data have to agree about "
                f"where a volume lives, or the next operation targets the "
                f"wrong node.")
        self.logger.info("[MIG] placement confirmed: %s on %s %s", vol_id,
                         node_id, context)

    def assert_no_target_leftovers(self, node_id, vol_id, context=""):
        """No orphaned bdevs or subsystems after a rolled-back migration.

        The rollback case the scripts cared most about: a migration that
        failed must leave the target as it found it. Leftovers are invisible
        until the next migration to the same node collides with them.
        """
        ip = None
        for n in self.online_nodes():
            if (n.get("uuid") or n.get("id")) == node_id:
                ip = n.get("mgmt_ip")
                break
        if not ip:
            self.logger.warning("[MIG] cannot reach node %s to check for "
                                "leftovers", node_id)
            return
        short = str(vol_id)[:8]
        # Ask the node's own SPDK what bdevs it holds. nvme list-subsys on the
        # client would show the client's view, which is not the question --
        # the leftover we care about is on the TARGET, and it survives the
        # client disconnecting.
        rpc = "bdev_get_bdevs"          # same RPC on both platforms
        try:
            if self.k8s_test:
                k8s = self._ensure_k8s_utils()
                out, _ = k8s.exec_in_spdk_container(
                    ip, f"/root/spdk/scripts/rpc.py {rpc} 2>/dev/null | grep -i {short} || true")
            else:
                out, _ = self.ssh_obj.exec_command(
                    node=ip,
                    command=f"sudo docker exec $(sudo docker ps -q -f "
                            f"name=spdk_ | head -1) "
                            f"/root/spdk/scripts/rpc.py {rpc} 2>/dev/null "
                            f"| grep -i {short} || true")
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[MIG] leftover check on %s: %s", ip,
                                str(exc)[:140])
            return
        if (out or "").strip():
            raise AssertionError(
                f"[MIG] target {node_id} still has subsystem state for "
                f"{vol_id} {context}:\n{out.strip()[:400]}\nA rolled-back "
                f"migration must leave the target as it found it; leftovers "
                f"are invisible until the next migration to this node "
                f"collides with them.")
        self.logger.info("[MIG] no leftovers on target %s %s", node_id, context)

    # -- post-migration invariants ---------------------------------------
    #: A bdev the migration machinery creates on the target and renames away
    #: at cutover: ``LVS_31/SNAP_187m``, ``LVS_7/LVOL_12m``. Its presence
    #: after a migration reported success is the state SFAM-2820 describes --
    #: the artefact survives, and the next migration of the same object then
    #: fails looking it up. Catching it here names the defect at the moment
    #: it is created, instead of waiting for a second migration to trip over
    #: it and reporting a confusing "No such device".
    ARTEFACT_RE = re.compile(r"(?:LVS_\d+/)?(?:SNAP|LVOL)_\d+m(?![0-9A-Za-z])")

    def _spdk_rpc(self, node_ip, args, grep=None):
        """Run an SPDK RPC on a node's own SPDK, whichever backend we are on.

        The node's view, not the client's: the artefacts these checks look
        for live on the storage node and survive the client disconnecting,
        so ``nvme list-subsys`` on the client cannot see them.
        """
        tail = f" | grep -i {grep}" if grep else ""
        cmd = f"/root/spdk/scripts/rpc.py {args} 2>/dev/null{tail} || true"
        try:
            if self.k8s_test:
                out, _ = self._ensure_k8s_utils().exec_in_spdk_container(
                    node_ip, cmd)
            else:
                out, _ = self.ssh_obj.exec_command(
                    node=node_ip,
                    command=f"sudo docker exec $(sudo docker ps -q -f "
                            f"name=spdk_ | head -1) {cmd}")
            return out or ""
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[MIG] rpc %s on %s: %s", args, node_ip,
                                str(exc)[:140])
            return None

    def _node_ip(self, node_id):
        for n in self.online_nodes():
            if (n.get("uuid") or n.get("id")) == node_id:
                return n.get("mgmt_ip")
        return None

    def assert_no_migration_artefacts(self, node_ids, context=""):
        """No ``*m`` bdev survives a migration that reported success.

        Backend-neutral by construction: it asks SPDK what it is holding,
        which is the same question and the same answer on docker and on
        k8s. Both the source and the target are worth asking -- the
        artefact is created on the target and the source's copy is dropped
        at cleanup_source, so either one surviving is a leak.
        """
        if isinstance(node_ids, str):
            node_ids = [node_ids]
        found = {}
        for node_id in [n for n in node_ids if n]:
            ip = self._node_ip(node_id)
            if not ip:
                self.logger.warning(
                    "[MIG] cannot reach node %s for the artefact check",
                    node_id)
                continue
            out = self._spdk_rpc(ip, "bdev_get_bdevs")
            if out is None:
                continue
            hits = sorted(set(self.ARTEFACT_RE.findall(out)))
            if hits:
                found[node_id] = hits
        if found:
            raise AssertionError(
                f"[MIG] a migration reported success and left migration "
                f"artefacts behind {context}: {found}. These are the "
                f"intermediate bdevs the copy creates and the cutover is "
                f"meant to rename away. One surviving is not cosmetic: the "
                f"next migration of the same object looks the name up, does "
                f"not find it, and fails -- which is why this is asserted "
                f"here, where the leak happens, rather than two migrations "
                f"later where it surfaces.")
        self.logger.info("[MIG] no migration artefacts on %s %s",
                         node_ids, context)

    def assert_snapshots_on_current_lvstore(self, vol_id, node_id,
                                            context=""):
        """Every snapshot record points at an lvstore on the volume's node.

        The other half of the same defect. A snapshot whose record still
        names the lvstore it was taken on is readable right up until
        something resolves it, and nothing in the migration's own status
        says the record was left behind.
        """
        ip = self._node_ip(node_id)
        if not ip:
            self.logger.warning("[MIG] cannot reach node %s to read its "
                                "lvstores", node_id)
            return
        out = self._spdk_rpc(ip, "bdev_lvol_get_lvstores")
        if out is None:
            return
        try:
            stores = {s.get("name") for s in json.loads(out) if s.get("name")}
        except (ValueError, TypeError, AttributeError):
            self.logger.warning("[MIG] could not parse lvstores on %s: %s",
                                node_id, (out or "").strip()[:200])
            return
        if not stores:
            self.logger.warning("[MIG] node %s reported no lvstores; "
                                "skipping the snapshot-record check", node_id)
            return
        stale = []
        for row in self.snapshots_of(vol_id):
            bdev = str(self._get(row, "bdev", "BDev") or "")
            lvs = bdev.split("/", 1)[0] if "/" in bdev else ""
            if lvs and lvs not in stores:
                stale.append((self._get(row, "uuid", "id") or "?", bdev))
        if stale:
            raise AssertionError(
                f"[MIG] {len(stale)} snapshot record(s) of {vol_id} still "
                f"name an lvstore that is not on node {node_id} {context}: "
                f"{stale[:6]}. The node now holds {sorted(stores)}. The "
                f"volume moved and its history did not follow in the "
                f"records, so these snapshots resolve to a store on the "
                f"node the volume has left.")
        self.logger.info("[MIG] all snapshot records resolve on %s %s",
                         node_id, context)

    def assert_migration_invariants(self, vol_id, src, tgt, context=""):
        """Both post-migration invariants, for callers that want the pair."""
        self.assert_no_migration_artefacts([src, tgt], context)
        self.assert_snapshots_on_current_lvstore(vol_id, tgt, context)

    def watch_io(self, name, seconds=None, context=""):
        """Keep IO running past the operation and confirm it stayed clean.

        The scripts ran fio from before the migration to five minutes after,
        because a cutover that looks clean and then drops IO thirty seconds
        later is a real failure mode and a test that stops at the cutover
        cannot see it.
        """
        seconds = seconds or self.POST_OP_WATCH_SEC
        self.logger.info("[MIG] watching IO for %ds %s", seconds, context)
        handle = self._run_fio_dual(name, runtime=seconds, rw="randrw",
                                    bs="16K", iodepth=8, numjobs=1,
                                    size="512M", verify="md5",
                                    verify_fatal=True, name="migwatch")
        self._wait_fio_dual([handle], timeout=seconds + 300)
        self._validate_fio_dual(handle)
        self.logger.info("[MIG] IO clean across %ds %s", seconds, context)

    # ── teardown ─────────────────────────────────────────────────────────
    def teardown(self, *args, **kwargs):
        """Drop this lane's cluster-scoped leftovers, pass or fail.

        The StorageClasses a batch case creates are deleted by
        cleanup_migrations, which runs at the END of run() -- so a case
        that raises never reaches it. Cleanup that only happens on the
        happy path is not cleanup, and here it does not merely leak: a
        StoragePool cannot be deleted while a StorageClass is assigned to
        it, so one failed batch case blocks the pool for every case after
        it. Run 38248680886 showed exactly that, four classes deep:

            StorageClassStillAssigned: this pool cannot be deleted while 4
            storage classes are assigned to it; delete migbatch-flat-...,
            migbatch-gaps-..., migbatch-neg-..., migbatch-tree-... and the
            pool goes on its own

        The framework calls teardown() after every test whatever the
        outcome, which makes it the only place this belongs. Best effort
        and never raising: a teardown that throws replaces the test's own
        verdict with its own.
        """
        # UNCORDON FIRST, and from teardown rather than from the case.
        #
        # reboot_node(drain=True) cordons, and says so: "the caller
        # uncordons once the node is Ready again". Doing that inline at
        # the end of the case is not enough -- anything that raises
        # between the reboot and that line leaves the node cordoned, its
        # storage pod unschedulable, and every later case failing its
        # pre-test health check on a node nothing is wrong with. That is
        # how run 20261013-143016 lost three classes to one reboot, and
        # an undo at the end of run() would have covered only the case
        # where it was never needed.
        for ip in sorted(getattr(self, "_cordoned", set()) or set()):
            try:
                self._ensure_k8s_utils().cordon_node(ip, cordon=False)
                self.logger.info("[MIG] teardown uncordoned %s", ip)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning(
                    "[MIG] could NOT uncordon %s: %s. Until someone does, "
                    "nothing schedules there, its storage node stays down, "
                    "and every later case fails its health check on a node "
                    "with nothing wrong with it.", ip, str(exc)[:160])
        self._cordoned = set()

        for sc in reversed(getattr(self, "_extra_scs", []) or []):
            try:
                self._ensure_k8s_utils().delete_storage_class(sc)
                self.logger.info("[MIG] teardown removed StorageClass %s", sc)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning(
                    "[MIG] could NOT remove StorageClass %s: %s. The pool it "
                    "is assigned to cannot be deleted until it goes, so the "
                    "next case starts against a pool that will not clear.",
                    sc, str(exc)[:160])
        self._extra_scs = []
        return super().teardown(*args, **kwargs)

    def cleanup_migrations(self):
        """Best effort. Never raises: teardown must not mask the result."""
        for mid in reversed(self._migrations):
            try:
                rec = self.migration_record(migration_id=mid)
                if rec and str(self._get(rec, "status") or "").lower() \
                        not in self.TERMINAL:
                    self.migrate_cancel(mid)
                self.migrate_cleanup(mid)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG] cleanup of %s: %s", mid,
                                    str(exc)[:140])
        # Snapshots first, because a volume with any cannot be deleted and
        # the failure is reported against the VOLUME, which reads like a
        # stuck migration rather than the ordinary dependency it is.
        # Through the CLI on both backends on purpose: the bulk snapshots
        # these cases take exist only to give snap_copy work -- they are
        # never cloned, mounted or named in an assertion -- so they are
        # taken with `snapshot add` rather than as 100+ VolumeSnapshot CRs
        # with a readiness wait each, and this is what then removes them.
        for name in reversed(self._mig_vols):
            try:
                data = self._cli_json(f"{self.base_cmd} snapshot list")
                rows = (data.get("results", data)
                        if isinstance(data, dict) else data) or []
                for s in rows:
                    sname = str(self._get(s, "snap_name", "name") or "")
                    if not sname.startswith(name):
                        continue
                    sid = self._get(s, "uuid", "id")
                    if sid:
                        self._cli(f"{self.base_cmd} -d snapshot delete {sid} "
                                  f"--force 2>&1")
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG] snapshot sweep for %s: %s", name,
                                    str(exc)[:140])
        for name in reversed(self._mig_vols):
            try:
                self._disconnect_and_cleanup_dual(name)
            except Exception:                         # noqa: BLE001
                pass
            try:
                # The dual helper, not sbcli_utils.delete_lvol: on k8s the
                # volume IS a PVC, and deleting the lvol under a live PVC
                # leaves the claim bound to a PV whose backing volume is
                # gone -- which the next run inherits as an unbindable
                # claim and a leaked PV.
                self._delete_lvol_dual(name)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG] could not delete %s: %s", name,
                                    str(exc)[:140])
        # After the volumes, never before: a StorageClass still named by a
        # bound PVC cannot be removed, and deleting it first would leave
        # the PVCs un-provisionable mid-teardown.
        for sc in reversed(self._extra_scs):
            try:
                self._ensure_k8s_utils().delete_storage_class(sc)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG] could not delete SC %s: %s", sc,
                                    str(exc)[:140])
        self._extra_scs = []
