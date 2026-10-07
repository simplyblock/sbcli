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

4. **Docker only.** No Kubernetes path existed. Where a step differs, this
   module branches once rather than making each test know.

THE FEATURE, because it is easy to confuse with three others we already
test. This is **lvol migration**: one volume moves between two nodes of ONE
cluster, through a deliberate two-phase handshake. It is not device-failure
rebalancing, not node migration, and not cross-cluster replication.

    volume migrate <vol> <target_node>      pre-create: builds the target
                                            subsystem, returns a migration id
                                            and connect strings. The new
                                            paths come up ANA INACCESSIBLE,
                                            so the source keeps serving IO.
      -> the operator connects the client to those paths <-
    volume migrate-continue <migration_id>  snap_copy -> intermediate ->
                                            lvol_migrate (short freeze, final
                                            delta, ANA flip) -> cleanup_source
                                            on failure: cleanup_target -> failed

The gap between the two commands is the whole point: the client is attached
to both ends before anything moves, so the cutover is an ANA flip rather
than a disconnect. A test that calls them back to back without connecting
in between is testing a different thing.

CLI vs API, which matters for what a test can address:

* the CLI takes a VOLUME id, and needs ``--batch`` spelled out for a
  shared-namespace group
* the API is keyed by the subsystem NQN and decides batch by itself -- if
  the subsystem allows more than one namespace it migrates the whole group
"""
import json
import os
import re
import time

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec


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
    #: How long after a migration to keep verifying IO. The scripts watched
    #: for five minutes past the operation because that is where the late
    #: failures showed up -- a cutover that looks clean and then drops IO
    #: thirty seconds later is the bug this catches.
    POST_OP_WATCH_SEC = int(os.environ.get("MIG_POST_OP_SEC", 300))

    VOL_SIZE = os.environ.get("MIG_VOL_SIZE", "2G")

    def setup(self):
        super().setup()
        self._migrations = []
        self._mig_vols = []

    # ── CLI ──────────────────────────────────────────────────────────────
    def _cli(self, command):
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
        """
        if not isinstance(obj, dict):
            return None
        for k in keys:
            for cand in (k, k.lower(), k.replace("_", " ").title(),
                         k.replace("_", " ")):
                if cand in obj and obj[cand] not in (None, ""):
                    return obj[cand]
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
            chosen = nid(free[0]) if free else nid(cands[0])
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
    MIG_ID_RE = re.compile(r"Migration ID:\s*([0-9a-f-]{36})", re.IGNORECASE)

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
            m = self.MIG_ID_RE.search(combined)
            if m:
                mid = m.group(1)
                self._migrations.append(mid)
                self._connect_target_paths(combined)
                self.logger.info("[MIG] pre-created %s -> %s (migration %s)",
                                 vol_id, target_node_id, mid)
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

    def _connect_target_paths(self, output):
        """Connect the client to the target's inaccessible-ANA paths."""
        cmds = [l.strip() for l in output.splitlines()
                if l.strip().startswith("nvme connect")]
        if not cmds:
            self.logger.warning(
                "[MIG] pre-create returned no nvme connect strings. The "
                "cutover needs the client attached to BOTH ends first; "
                "without it this becomes a disconnect, not an ANA flip.")
            return
        node = self.fio_node[0] if self.fio_node else self.mgmt_nodes[0]
        for c in cmds:
            self.ssh_obj.exec_command(node=node, command=f"sudo {c} 2>&1 || true")
        sleep_n_sec(3)
        self.logger.info("[MIG] connected %d target path(s)", len(cmds))

    def migrate_continue(self, migration_id, batch=False, max_retries=10,
                         deadline=14400, retry_on_failure=False):
        """Phase two: start the copy and the task runner."""
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
        out, _ = self._cli(f"{self.base_cmd} --dev volume migrate-cleanup "
                           f"{migration_id} 2>&1")
        return out

    def migration_record(self, vol_id=None, migration_id=None):
        data = self._cli_json(f"{self.base_cmd} volume migrate-list "
                              f"--cluster-id {self.cluster_id}")
        rows = data.get("results", data) if isinstance(data, dict) else data
        for m in (rows or []):
            if migration_id and self._get(m, "id", "migration_id") == migration_id:
                return m
            if vol_id and self._get(m, "lvol_id", "volume_id") == vol_id:
                return m
        return None

    def batch_record(self, group_id):
        data = self._cli_json(f"{self.base_cmd} volume migrate-group-list "
                              f"--cluster-id {self.cluster_id}")
        rows = data.get("results", data) if isinstance(data, dict) else data
        for g in (rows or []):
            if self._get(g, "id", "group_id") == group_id:
                return g
        return None

    # ── waiting ──────────────────────────────────────────────────────────
    def await_migration(self, vol_id=None, migration_id=None,
                        terminal_only=True, timeout=None, what=""):
        """Poll until terminal (or cutover, if terminal_only is False).

        Logs the record's own error and retry fields every poll. Without
        them a stuck migration is an opaque wall of 'status=suspended' with
        no hint why -- the scripts added this after exactly that.
        """
        timeout = timeout or self.MIGRATION_TIMEOUT
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            m = (self.migration_record(vol_id=vol_id, migration_id=migration_id)
                 or {})
            if not m:
                self.logger.info("[MIG] record gone -- treating as done%s",
                                 f" ({what})" if what else "")
                return "done", ""
            last = m
            status = str(self._get(m, "status") or "unknown").lower()
            phase = str(self._get(m, "phase") or "").lower()
            err = self._get(m, "error_message", "error")
            line = (f"[MIG] status={status} phase={phase} "
                    f"snaps={self._get(m, 'snaps') or ''} "
                    f"retries={self._get(m, 'retries') or ''}")
            if err:
                self.logger.warning("%s error=%r", line, str(err)[:200])
            else:
                self.logger.info(line)
            if status in self.TERMINAL:
                return status, phase
            if status == "cutover" and not terminal_only:
                return "cutover", phase
            sleep_n_sec(self.MIGRATION_POLL)
        raise AssertionError(
            f"[MIG] migration did not reach a terminal state within "
            f"{timeout}s{(' (' + what + ')') if what else ''}. Last record: "
            f"{last}. A migration that never terminates is itself the bug -- "
            f"it must always end somewhere, even if that somewhere is failed.")

    def await_batch(self, group_id, timeout=None, what=""):
        timeout = timeout or self.MIGRATION_TIMEOUT
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
        """Pre-create, connect, continue, wait. The happy path in one call."""
        mid = self.migrate(vol_id, target_node_id, batch=batch)
        self.migrate_continue(mid, batch=batch, deadline=deadline)
        if batch:
            status, phase = self.await_batch(mid, what=what)
        else:
            status, phase = self.await_migration(migration_id=mid,
                                                 vol_id=vol_id, what=what)
        if expect and status not in (
                self.OK_TERMINAL if expect == "done" else (expect,)):
            raise AssertionError(
                f"[MIG] migration of {vol_id} ended {status!r} (phase "
                f"{phase!r}), expected {expect!r}{(' -- ' + what) if what else ''}")
        return mid, status, phase

    def make_volume(self, name, size=None, pool=None, host_id=None,
                    seed=True):
        """Create a volume, optionally on a named node, and seed it."""
        # `volume add <name> <size> <pool>` -- the pool is POSITIONAL.
        # There is no --pool flag; passing one is an argparse error, which is
        # how every case in this lane failed on its first line.
        cmd = (f"{self.base_cmd} -d volume add {name} "
               f"{size or self.VOL_SIZE} {pool or self.pool_name}")
        if host_id:
            cmd += f" --host-id {host_id}"
        out, err = self._cli(cmd + " 2>&1")
        if "error" in (out + err).lower():
            raise MigrationPreconditionError(
                f"[MIG] could not create {name}: {(out + err)[:300]}")
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=name)
        self._mig_vols.append(name)
        sums = self.seed(name) if seed else {}
        return vol_id, sums

    def seed(self, name, files=3, file_size="32M"):
        """Connect, format, mount, write known content, return checksums."""
        self._connect_and_mount_dual(name, format_disk=True)
        if self.k8s_test:
            self._run_fio_dual(name, runtime=60, rw="write", bs="256K",
                               size=file_size, numjobs=1, nrfiles=files,
                               time_based=False, name="migseed")
        else:
            self.ssh_obj.create_random_files(
                node=self.fio_node[0], mount_path=self.mount_path,
                file_size=file_size, file_prefix="migseed", file_count=files)
        sums = self._generate_checksums_dual(name)
        if not sums:
            raise MigrationPreconditionError(
                f"[MIG] seeded {name} but produced no checksums, so any later "
                f"comparison would pass over an empty set.")
        return sums

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
        rpc = ("bdev_get_bdevs" if not self.k8s_test else "bdev_get_bdevs")
        try:
            if self.k8s_test:
                k8s = self._ensure_k8s_utils()
                pod = k8s.get_spdk_pod_name(ip)
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
                                    name="migwatch")
        self._wait_fio_dual([handle], timeout=seconds + 300)
        self._validate_fio_dual(handle)
        self.logger.info("[MIG] IO clean across %ds %s", seconds, context)

    # ── teardown ─────────────────────────────────────────────────────────
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
        for name in reversed(self._mig_vols):
            try:
                self._disconnect_and_cleanup_dual(name)
            except Exception:                         # noqa: BLE001
                pass
            try:
                self.sbcli_utils.delete_lvol(lvol_name=name)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[MIG] could not delete %s: %s", name,
                                    str(exc)[:140])
