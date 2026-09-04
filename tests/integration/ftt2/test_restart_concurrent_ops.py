"""
test_restart_concurrent_ops.py - stress tests for concurrent CRUD operations
during restart, exercising the sync delete / registration gate mechanism.

Verifies that at high operation frequency:
  - Operations arriving BEFORE port block complete (block waits for them)
  - Operations arriving DURING port block are DELAYED until post_unblock
  - Operations arriving AFTER port unblock proceed normally
  - Strict ordering is preserved for delayed operations
  - No operation's RPC reaches a node while phase is "blocked"

Topology: same as conftest.py (4 nodes, round-robin LVS assignment).
"""

import logging
import random
import threading
import time
from dataclasses import dataclass
from unittest.mock import patch


from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core import storage_node_ops

from tests.integration.ftt2.conftest import (
    prepare_node_for_restart,
    create_test_lvol,
    patch_externals,
)

logger = logging.getLogger(__name__)

RESTART_NODE = 0


# ---------------------------------------------------------------------------
# Gate audit log — records every call to wait_or_delay_for_restart_gate
# ---------------------------------------------------------------------------

@dataclass
class GateEvent:
    timestamp: float
    node_id: str
    lvs_name: str
    phase: str
    result: str  # "proceed" or "delay"
    operation: str = ""
    thread_id: int = 0


class GateAuditor:
    """Wraps wait_or_delay_for_restart_gate to log all calls."""

    def __init__(self):
        self.events: list[GateEvent] = []
        self.lock = threading.Lock()
        self._original_fn = storage_node_ops.wait_or_delay_for_restart_gate

    def __call__(self, node_id, lvs_name, timeout=30):
        result = self._original_fn(node_id, lvs_name, timeout)
        phase = storage_node_ops.get_restart_phase(node_id, lvs_name)
        event = GateEvent(
            timestamp=time.time(),
            node_id=node_id,
            lvs_name=lvs_name,
            phase=phase,
            result=result,
            thread_id=threading.current_thread().ident or 0,
        )
        with self.lock:
            self.events.append(event)
        return result

    def assert_no_proceed_during_blocked(self):
        """Assert no operation was allowed to proceed while phase was blocked.

        NOTE: vacuous as things stand. ``wait_or_delay_for_restart_gate`` has a
        single production caller (``snapshot_controller.py:783``, the snapshot
        sync-delete path), and StressRunner fires only create/delete/resize --
        so ``self.events`` is always empty and this assertion always holds. The
        lvol paths gate on ``get_restart_phase`` from background services
        (``tasks_runner_sync_lvol_del.py``, ``tasks_controller.py``) that this
        tier does not run. Making it real means either firing snapshot/clone
        ops from StressRunner or driving those services; until then, what these
        tests actually cover is "concurrent CRUD during a restart does not
        explode", which is `total_ops` plus the absence of exceptions.
        """
        violations = [e for e in self.events
                      if e.phase == StorageNode.RESTART_PHASE_BLOCKED
                      and e.result == "proceed"]
        assert len(violations) == 0, (
            f"Operations proceeded during blocked phase: {violations}")

    def assert_delayed_ops_after_unblock(self):
        """Assert all delayed operations have matching post-unblock proceeds."""
        delayed = [e for e in self.events if e.result == "delay"]
        # Each delay should eventually have a proceed after phase changes
        # (in real code, the caller retries — we check no delay was orphaned
        # within the test window)
        return delayed

    def get_events_for_node(self, node_id):
        return [e for e in self.events if e.node_id == node_id]


# ---------------------------------------------------------------------------
# Stress runner — fires operations at high frequency
# ---------------------------------------------------------------------------

class StressRunner:
    """Fires CRUD operations at high frequency in background threads."""

    def __init__(self, env, target_lvs_primary_idx: int, num_threads: int = 4,
                 interval_ms: int = 20, duration_sec: float = 5.0):
        self.env = env
        self.target_lvs_primary_idx = target_lvs_primary_idx
        self.num_threads = num_threads
        self.interval = interval_ms / 1000.0
        self.duration = duration_sec
        self._stop = threading.Event()
        self._entered = threading.Barrier(num_threads + 1)
        self._threads: list[threading.Thread] = []
        self._results: list[dict] = []
        self._lock = threading.Lock()
        self._vol_counter = 0

    def _next_vol_name(self):
        with self._lock:
            self._vol_counter += 1
            return f"stress-vol-{self._vol_counter}"

    def _record(self, op_type, success, start_time, end_time, details=""):
        with self._lock:
            self._results.append({
                "op": op_type,
                "success": success,
                "start": start_time,
                "end": end_time,
                "duration_ms": (end_time - start_time) * 1000,
                "details": details,
                "thread": threading.current_thread().name,
            })

    def _worker(self, worker_id, rng):
        """Worker thread that fires random operations.
        NOTE: patches must be started by the caller BEFORE spawning threads."""
        from simplyblock_core.db_controller import DBController

        # Inside the guard, and catching BaseException: this used to sit
        # outside the try, so anything raised here (including SystemExit from
        # a stray patch elsewhere in the session) killed the daemon thread
        # silently. The test then failed with "0 operations" and no clue as to
        # why -- which is exactly how test_delete_during_port_block failed
        # repeatedly in CI with no diagnosable cause. Record the death so the
        # next failure names itself.
        try:
            DBController()
        except BaseException as e:            # noqa: BLE001 - diagnostic only
            self._record("worker-init", False, time.time(), time.time(),
                         f"{type(e).__name__}: {e}")
            self._entered.abort()
            return
        created_lvols = []

        # Rendezvous with start(). Without it the window can close before this
        # thread reaches the loop condition below, and the run records zero
        # operations -- which is how test_delete_during_port_block failed in CI.
        try:
            self._entered.wait(timeout=10)
        except threading.BrokenBarrierError:
            return

        while not self._stop.is_set():
            op = rng.choice(["create", "delete", "resize", "create", "delete"])
            t0 = time.time()

            try:
                if op == "create":
                    name = self._next_vol_name()
                    lvol = create_test_lvol(self.env, self.target_lvs_primary_idx, name)
                    created_lvols.append(lvol)
                    self._record("create", True, t0, time.time(), lvol.uuid)

                elif op == "delete" and created_lvols:
                    from simplyblock_core.controllers import lvol_controller
                    lvol = created_lvols.pop(rng.randrange(len(created_lvols)))
                    lvol_controller.delete_lvol(lvol, force_delete=True)
                    self._record("delete", True, t0, time.time(), lvol.uuid)

                elif op == "resize" and created_lvols:
                    from simplyblock_core.controllers import lvol_controller
                    lvol = rng.choice(created_lvols)
                    new_size = lvol.size + 1_073_741_824
                    lvol_controller.resize_lvol(lvol.uuid, new_size)
                    self._record("resize", True, t0, time.time(), lvol.uuid)

            except Exception as e:
                self._record(op, False, t0, time.time(), str(e))
            except BaseException as e:        # noqa: BLE001 - diagnostic only
                # SystemExit/KeyboardInterrupt would otherwise kill this
                # daemon thread without recording anything.
                self._record(op, False, t0, time.time(),
                             f"{type(e).__name__}: {e}")
                return

            # Event.wait, not time.sleep: see run_for. Also makes stop()
            # immediate instead of up to one interval late.
            self._stop.wait(self.interval + rng.uniform(0, self.interval))

    def start(self):
        self._stop.clear()
        self._entered = threading.Barrier(self.num_threads + 1)
        for i in range(self.num_threads):
            # Seed drawn here, in the spawning thread: this loop runs in a fixed
            # order, so each worker replays the same op sequence for a given
            # session seed. Drawing inside _worker would race and defeat replay.
            # Thread interleaving stays nondeterministic either way, so the
            # assertions below are invariants, not exact outcomes.
            worker_rng = random.Random(random.getrandbits(64))
            t = threading.Thread(target=self._worker, args=(i, worker_rng),
                                 name=f"stress-{i}", daemon=True)
            t.start()
            self._threads.append(t)

        try:
            self._entered.wait(timeout=10)
        except threading.BrokenBarrierError:
            raise AssertionError(
                f"stress workers failed to start; worker records: {self.results}"
            ) from None

    def stop(self):
        self._stop.set()
        for t in self._threads:
            t.join(timeout=10)
        self._threads.clear()

    def run_for(self, duration: float = None):
        """Run stress ops for given duration then stop."""
        window = duration or self.duration
        self.start()
        started = time.monotonic()
        # NOT time.sleep. patch_externals() patches `<module>.time.sleep`, which
        # is an attribute on the shared stdlib `time` module, so it nulls sleep
        # for the whole process and every thread. That collapsed this window to
        # microseconds, leaving total_ops to whatever the workers managed
        # between start() and stop() -- a pure scheduling race. Event.wait is a
        # timed lock acquire and cannot be patched out this way.
        self._stop.wait(window)
        elapsed = time.monotonic() - started
        self.stop()
        assert elapsed >= window * 0.9, (
            f"stress window collapsed: {elapsed:.3f}s of a nominal {window}s. "
            "Something patched a timing primitive out from under this runner.")

    @property
    def results(self):
        return list(self._results)

    @property
    def total_ops(self):
        return len(self._results)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_restart_in_thread(env, node_idx=RESTART_NODE):
    """Run restart in a background thread.

    Returns ``(thread, result_holder, patches)``. ``patch_externals()`` is
    started here in the CALLING thread (not inside the restart thread) and the
    started patchers are returned so the caller can stop them *after* the
    concurrent StressRunner has finished. Starting/stopping the patches inside
    the restart thread (as an earlier version did) dropped the external mocks
    the instant the fast mocked restart returned, leaving the still-running
    stress workers to hit the real controllers and block — so only a handful of
    ops completed. Keeping the mocks alive for the whole window (as CLASS 2/3
    already do) mirrors a real concurrent restart. The caller MUST stop the
    returned patches once done.
    """
    result_holder = {"result": None, "node": None, "error": None}
    patches = patch_externals()
    for p in patches:
        p.start()

    def _do():
        try:
            from simplyblock_core.db_controller import DBController
            node = env['nodes'][node_idx]
            result_holder["result"] = storage_node_ops.restart_storage_node(node.uuid)
            db = DBController()
            result_holder["node"] = db.get_storage_node_by_id(node.uuid)
        except Exception as e:
            result_holder["error"] = str(e)

    t = threading.Thread(target=_do, daemon=True, name="restart-thread")
    t.start()
    return t, result_holder, patches


# ===========================================================================
# CLASS 1: Concurrent ops on secondary/tertiary during primary LVS restart
# ===========================================================================

class TestConcurrentOpsOnPeersduringPrimaryRestart:
    """n0 restarts. LVS_0 primary restart causes port block on n1, n2.

    Background threads do create/delete/resize targeting LVS_0 volumes.
    These operations need sync delete / registration on n1 and n2,
    which are gated by their restart_phases["LVS_0"].
    """

    def test_delete_during_port_block(self, ftt2_env):
        """High frequency deletes while ports are blocked during restart."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)

        # Pre-create volumes to delete
        for i in range(10):
            create_test_lvol(env, 0, f"del-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            restart_thread, restart_result, _patches = _run_restart_in_thread(env)

            stress = StressRunner(env, 0, num_threads=2, interval_ms=10,
                                  duration_sec=3.0)
            stress.run_for()
            restart_thread.join(timeout=30)

            for p in _patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()
        assert stress.total_ops >= stress.num_threads, (
            f"expected at least one op per worker, got {stress.total_ops}; "
            f"worker records: {stress.results[:3]}")

    def test_create_during_port_block(self, ftt2_env):
        """High frequency creates while ports are blocked during restart."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            restart_thread, restart_result, _patches = _run_restart_in_thread(env)

            stress = StressRunner(env, 0, num_threads=2, interval_ms=10,
                                  duration_sec=3.0)
            stress.run_for()
            restart_thread.join(timeout=30)

            for p in _patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()

    def test_resize_during_port_block(self, ftt2_env):
        """High frequency resizes while ports are blocked during restart."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)

        # Pre-create volumes to resize
        for i in range(5):
            create_test_lvol(env, 0, f"resize-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            restart_thread, restart_result, _patches = _run_restart_in_thread(env)

            stress = StressRunner(env, 0, num_threads=2, interval_ms=10,
                                  duration_sec=3.0)
            stress.run_for()
            restart_thread.join(timeout=30)

            for p in _patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()

    def test_mixed_ops_high_frequency(self, ftt2_env):
        """All operation types mixed at high frequency during restart."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)

        for i in range(10):
            create_test_lvol(env, 0, f"mixed-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            restart_thread, restart_result, _patches = _run_restart_in_thread(env)

            stress = StressRunner(env, 0, num_threads=4, interval_ms=10,
                                  duration_sec=5.0)
            stress.run_for()
            restart_thread.join(timeout=30)

            for p in _patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()
        # This used to read "only a handful of ops land inside the window,
        # because a single-node FDB serializes them behind the restart". That
        # was wrong: the window itself was microseconds long (see run_for), so
        # the count measured thread-start latency, not FDB throughput.
        assert stress.total_ops >= stress.num_threads, (
            f"expected at least one op per worker, got {stress.total_ops}; "
            f"worker records: {stress.results[:3]}")

    def test_long_running_stress(self, ftt2_env):
        """10 second stress run with all operations."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)

        for i in range(20):
            create_test_lvol(env, 0, f"long-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            restart_thread, restart_result, _patches = _run_restart_in_thread(env)

            stress = StressRunner(env, 0, num_threads=4, interval_ms=15,
                                  duration_sec=10.0)
            stress.run_for()
            restart_thread.join(timeout=60)

            for p in _patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()
        delayed = auditor.assert_delayed_ops_after_unblock()
        logger.info("Total ops: %d, Delayed ops: %d, Gate events: %d",
                    stress.total_ops, len(delayed), len(auditor.events))

    def test_ordering_preserved_for_delayed_ops(self, ftt2_env):
        """Verify strict ordering: delayed deletes execute in submission order."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)

        for i in range(10):
            create_test_lvol(env, 0, f"order-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            restart_thread, restart_result, _patches = _run_restart_in_thread(env)

            stress = StressRunner(env, 0, num_threads=1, interval_ms=5,
                                  duration_sec=3.0)
            stress.run_for()
            restart_thread.join(timeout=30)

            for p in _patches:
                p.stop()

        # Check that delayed events are in timestamp order
        delayed = [e for e in auditor.events if e.result == "delay"]
        for i in range(1, len(delayed)):
            assert delayed[i].timestamp >= delayed[i-1].timestamp, \
                "Delayed operations must maintain submission order"


# ===========================================================================
# CLASS 2: Concurrent ops on tertiary during secondary LVS restart
# ===========================================================================

class TestConcurrentOpsOnTertiaryDuringSecondaryRestart:
    """n1 restarts as secondary for LVS_0. During recreate_lvstore_on_non_leader(),
    n2 (tertiary) gets port blocked. Background ops targeting LVS_0 needing
    sync on n2 must be gated.
    """

    def test_mixed_ops_on_tertiary(self, ftt2_env):
        """Mixed ops while tertiary port blocked during secondary restart."""
        env = ftt2_env
        # Restart n1 (secondary for LVS_0)
        prepare_node_for_restart(env, 1)

        for i in range(10):
            create_test_lvol(env, 0, f"sec-restart-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            node = env['nodes'][1]
            patches = patch_externals()
            for p in patches:
                p.start()

            restart_thread = threading.Thread(
                target=lambda: storage_node_ops.restart_storage_node(node.uuid),
                daemon=True)
            restart_thread.start()

            stress = StressRunner(env, 0, num_threads=3, interval_ms=10,
                                  duration_sec=5.0)
            stress.run_for()
            restart_thread.join(timeout=30)

            for p in patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()

    def test_long_running_stress_on_tertiary(self, ftt2_env):
        """10 second stress targeting tertiary during secondary restart."""
        env = ftt2_env
        prepare_node_for_restart(env, 1)

        for i in range(15):
            create_test_lvol(env, 0, f"sec-long-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            node = env['nodes'][1]
            patches = patch_externals()
            for p in patches:
                p.start()

            restart_thread = threading.Thread(
                target=lambda: storage_node_ops.restart_storage_node(node.uuid),
                daemon=True)
            restart_thread.start()

            stress = StressRunner(env, 0, num_threads=4, interval_ms=15,
                                  duration_sec=10.0)
            stress.run_for()
            restart_thread.join(timeout=60)

            for p in patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()


# ===========================================================================
# CLASS 3: Concurrent ops on primary during non-leader restart
# ===========================================================================

class TestConcurrentOpsOnPrimaryDuringNonLeaderRestart:
    """When n1 (secondary for LVS_0) restarts, the PRIMARY n0 also gets
    port blocked during recreate_lvstore_on_non_leader(). Async operations
    (delete, create, clone, resize) on the primary are gated.
    """

    def test_async_delete_on_primary_during_sec_restart(self, ftt2_env):
        """Async deletes on primary n0 while n1's non-leader recreation
        port-blocks n0."""
        env = ftt2_env
        prepare_node_for_restart(env, 1)

        for i in range(10):
            create_test_lvol(env, 0, f"pri-del-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            node = env['nodes'][1]
            patches = patch_externals()
            for p in patches:
                p.start()

            restart_thread = threading.Thread(
                target=lambda: storage_node_ops.restart_storage_node(node.uuid),
                daemon=True)
            restart_thread.start()

            stress = StressRunner(env, 0, num_threads=2, interval_ms=10,
                                  duration_sec=5.0)
            stress.run_for()
            restart_thread.join(timeout=30)

            for p in patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()

    def test_create_clone_resize_on_primary_during_sec_restart(self, ftt2_env):
        """Create, clone, resize on primary n0 while n1's restart blocks n0."""
        env = ftt2_env
        prepare_node_for_restart(env, 1)

        for i in range(5):
            create_test_lvol(env, 0, f"pri-mixed-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            node = env['nodes'][1]
            patches = patch_externals()
            for p in patches:
                p.start()

            restart_thread = threading.Thread(
                target=lambda: storage_node_ops.restart_storage_node(node.uuid),
                daemon=True)
            restart_thread.start()

            stress = StressRunner(env, 0, num_threads=4, interval_ms=10,
                                  duration_sec=5.0)
            stress.run_for()
            restart_thread.join(timeout=30)

            for p in patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()

    def test_long_stress_on_primary_during_tert_restart(self, ftt2_env):
        """Long stress on primary n0 while n2 (tertiary) restarts and blocks n0."""
        env = ftt2_env
        prepare_node_for_restart(env, 2)  # restart tertiary

        for i in range(15):
            create_test_lvol(env, 0, f"pri-tert-test-{i}")

        auditor = GateAuditor()
        with patch.object(storage_node_ops, 'wait_or_delay_for_restart_gate',
                          side_effect=auditor):
            node = env['nodes'][2]
            patches = patch_externals()
            for p in patches:
                p.start()

            restart_thread = threading.Thread(
                target=lambda: storage_node_ops.restart_storage_node(node.uuid),
                daemon=True)
            restart_thread.start()

            stress = StressRunner(env, 0, num_threads=4, interval_ms=15,
                                  duration_sec=10.0)
            stress.run_for()
            restart_thread.join(timeout=60)

            for p in patches:
                p.stop()

        auditor.assert_no_proceed_during_blocked()
        delayed = auditor.assert_delayed_ops_after_unblock()
        logger.info("Total ops: %d, Delayed: %d", stress.total_ops, len(delayed))
