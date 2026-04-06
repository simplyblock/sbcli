# coding=utf-8
"""
operation_runner.py – external test service that runs real control plane
operations (create/delete/resize volumes, snapshots, clones) concurrently
with restart, as they would run in a fully deployed system.

Architecture:
  - PhaseGate: synchronization primitive injected into the mock RPC server.
    The mock pauses at a specific RPC (e.g. bdev_examine) and signals the
    test.  The test then triggers an operation and releases the gate.

  - OperationRunner: runs a control plane operation in a separate thread
    using the real lvol_controller / snapshot_controller code.

  - Test flow:
      1. Install a PhaseGate on the mock (e.g. "pause at bdev_examine")
      2. Start restart in thread A
      3. Wait for gate to signal "paused" → restart is at Phase 5
      4. Run an operation in thread B via OperationRunner
      5. Release the gate
      6. Both threads complete
      7. Assert outcomes
"""

import threading
import time
import logging
from typing import Callable, Optional, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase gate — synchronization between restart and concurrent operations
# ---------------------------------------------------------------------------

class PhaseGate:
    """Blocks a specific RPC call until released, enabling precise timing
    control for concurrent operation tests.

    Usage:
        gate = PhaseGate("bdev_examine")
        mock_server.install_gate(gate)

        # restart runs in thread, hits bdev_examine, pauses
        gate.wait_until_paused(timeout=10)

        # now restart is at Phase 5 — run concurrent operation
        do_something()

        # release the gate so restart continues
        gate.release()
    """

    def __init__(self, rpc_method: str):
        self.rpc_method = rpc_method
        self._paused = threading.Event()
        self._released = threading.Event()
        self._hit_count = 0
        self.lock = threading.Lock()

    def should_pause(self, method: str) -> bool:
        """Called by mock RPC handler. Returns True if this RPC should block."""
        return method == self.rpc_method and not self._released.is_set()

    def pause(self):
        """Called by mock RPC handler when it hits the gated RPC."""
        with self.lock:
            self._hit_count += 1
        self._paused.set()
        # Block until released
        self._released.wait(timeout=30)

    def wait_until_paused(self, timeout: float = 10) -> bool:
        """Wait until the restart thread hits the gated RPC."""
        return self._paused.wait(timeout=timeout)

    def release(self):
        """Release the gate so the restart thread continues."""
        self._released.set()

    @property
    def was_hit(self) -> bool:
        return self._hit_count > 0

    def reset(self):
        self._paused.clear()
        self._released.clear()
        self._hit_count = 0


# ---------------------------------------------------------------------------
# Operation definitions
# ---------------------------------------------------------------------------

@dataclass
class OperationResult:
    """Result of a control plane operation."""
    success: bool = False
    error: Optional[str] = None
    result_id: Optional[str] = None  # UUID of created object


class OperationRunner:
    """Runs real control plane operations in a separate thread.

    The operations go through the full code path (lvol_controller,
    snapshot_controller, etc.) with all RPCs hitting the mock servers.
    """

    def __init__(self, cluster_id: str, patches: list):
        self.cluster_id = cluster_id
        self._patches = patches
        self._thread: Optional[threading.Thread] = None
        self._result = OperationResult()
        self._done = threading.Event()

    def _run_with_patches(self, fn: Callable):
        """Execute fn with all external patches applied."""
        for p in self._patches:
            p.start()
        try:
            fn()
        except Exception as e:
            self._result.error = str(e)
            logger.exception("OperationRunner failed: %s", e)
        finally:
            for p in self._patches:
                p.stop()
            self._done.set()

    def start(self, fn: Callable):
        """Start the operation in a background thread."""
        self._done.clear()
        self._result = OperationResult()
        self._thread = threading.Thread(
            target=self._run_with_patches, args=(fn,), daemon=True)
        self._thread.start()

    def wait(self, timeout: float = 30) -> OperationResult:
        """Wait for the operation to complete."""
        self._done.wait(timeout=timeout)
        if self._thread:
            self._thread.join(timeout=5)
        return self._result

    # --- Pre-built operations ---

    def create_volume(self, pool_name: str, vol_name: str, size: str = "1G",
                      encrypted: bool = False, qos_iops: int = 0,
                      dhchap: bool = False):
        """Create a volume via lvol_controller.add_lvol_ha()."""
        def _do():
            from simplyblock_core.controllers import lvol_controller

            crypto_key1 = "a" * 64 if encrypted else ""
            crypto_key2 = "b" * 64 if encrypted else ""

            allowed_hosts = []
            if dhchap:
                allowed_hosts = ["nqn.2014-08.org.nvmexpress:uuid:test-host"]

            result = lvol_controller.add_lvol_ha(
                cluster_id=self.cluster_id,
                pool_id_or_name=pool_name,
                name=vol_name,
                size=size,
                crypto_key1=crypto_key1,
                crypto_key2=crypto_key2,
                max_rw_iops=qos_iops,
                allowed_hosts=allowed_hosts,
            )
            if result:
                self._result.success = True
                self._result.result_id = result
            else:
                self._result.error = "add_lvol_ha returned None/False"

        self.start(_do)
        return self

    def delete_volume(self, lvol_id: str):
        """Delete a volume via lvol_controller.delete_lvol()."""
        def _do():
            from simplyblock_core.controllers import lvol_controller
            result = lvol_controller.delete_lvol(lvol_id)
            self._result.success = bool(result)

        self.start(_do)
        return self

    def create_snapshot(self, lvol_id: str, snap_name: str):
        """Create a snapshot via snapshot_controller.add()."""
        def _do():
            from simplyblock_core.controllers import snapshot_controller
            result = snapshot_controller.add(lvol_id, snap_name)
            if result:
                self._result.success = True
                self._result.result_id = result

        self.start(_do)
        return self

    def delete_snapshot(self, snap_id: str):
        """Delete a snapshot via snapshot_controller.delete()."""
        def _do():
            from simplyblock_core.controllers import snapshot_controller
            result = snapshot_controller.delete(snap_id)
            self._result.success = bool(result)

        self.start(_do)
        return self

    def clone_from_snapshot(self, snap_id: str, clone_name: str,
                            size: str = "1G"):
        """Clone from snapshot via snapshot_controller.clone()."""
        def _do():
            from simplyblock_core.controllers import snapshot_controller
            result = snapshot_controller.clone(snap_id, clone_name, size)
            if result:
                self._result.success = True
                self._result.result_id = result

        self.start(_do)
        return self

    def resize_volume(self, lvol_id: str, new_size: str):
        """Resize a volume via lvol_controller.resize_lvol()."""
        def _do():
            from simplyblock_core.controllers import lvol_controller
            result = lvol_controller.resize_lvol(lvol_id, new_size)
            self._result.success = bool(result)

        self.start(_do)
        return self

    def modify_volume_qos(self, lvol_id: str, max_rw_iops: int):
        """Modify volume QoS via lvol_controller.set_lvol()."""
        def _do():
            from simplyblock_core.controllers import lvol_controller
            result = lvol_controller.set_lvol(
                lvol_id, max_rw_iops=max_rw_iops)
            self._result.success = bool(result)

        self.start(_do)
        return self


# ---------------------------------------------------------------------------
# Concurrent restart + operation helper
# ---------------------------------------------------------------------------

def run_restart_with_concurrent_op(
    env,
    node_idx: int,
    gate_rpc: str,
    operation_fn: Callable[[OperationRunner], None],
    patches: list,
) -> tuple:
    """Run restart_storage_node() with a concurrent operation injected
    at a specific phase.

    Args:
        env: ftt2_env fixture dict
        node_idx: index of node to restart
        gate_rpc: RPC method name where restart should pause
        operation_fn: callable that receives an OperationRunner and starts an op
        patches: list of patch context managers

    Returns:
        (restart_result, node_after, op_result)
    """
    from simplyblock_core.db_controller import DBController
    from tests.ftt2.mock_cluster import FTT2MockRpcServer

    node = env['nodes'][node_idx]
    srv: FTT2MockRpcServer = env['servers'][node_idx]

    # Install phase gate
    gate = PhaseGate(gate_rpc)
    srv.state._phase_gate = gate

    restart_result = [None]
    restart_error = [None]

    def _restart_thread():
        for p in patches:
            p.start()
        try:
            restart_result[0] = storage_node_ops.restart_storage_node(node.uuid)
        except Exception as e:
            restart_error[0] = e
        finally:
            for p in patches:
                p.stop()

    # Start restart in background
    t = threading.Thread(target=_restart_thread, daemon=True)
    t.start()

    # Wait for restart to reach the gate
    if gate.wait_until_paused(timeout=15):
        # Restart is paused at the gate — run the concurrent operation
        runner = OperationRunner(env['cluster'].uuid, patches)
        operation_fn(runner)
        op_result = runner.wait(timeout=15)

        # Release the gate so restart continues
        gate.release()
    else:
        op_result = OperationResult(error="Gate was never hit")
        gate.release()

    # Wait for restart to complete
    t.join(timeout=30)

    # Clean up gate
    srv.state._phase_gate = None

    db = DBController()
    updated_node = db.get_storage_node_by_id(node.uuid)

    return restart_result[0], updated_node, op_result
