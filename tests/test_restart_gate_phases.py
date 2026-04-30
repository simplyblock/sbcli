# coding=utf-8
"""Pin the widened restart-gate semantics for lvol/snapshot/resize fan-out.

Prior to this fix, ``check_non_leader_for_operation`` returned ``"skip"``
for ``pre_block`` and ``"proceed"`` for ``post_unblock``. Both decisions
could lose a mgmt-side create/delete/resize on a restarting peer:

- ``pre_block``: primary-side op ran over mgmt RPC (port 8085), which is
  not blocked by the LVS port block (port 4436). Examine then read the
  primary's blobstore at an arbitrary point relative to the op; a lvol
  created just before examine's read was silently missing on the
  rebuilt peer.
- ``post_unblock``: the per-lvol subsystem re-registration loop on the
  restarting node was still in flight. A concurrent mgmt-side
  ``nvmf_subsystem_add_ns`` would race the restart's own
  ``subsystem_create`` and fail.

The fix:

1. All three non-empty phases (``pre_block`` / ``blocked`` / ``post_unblock``)
   now gate the operation: ``check_non_leader_for_operation`` returns
   ``"queue"`` and ``wait_or_delay_for_restart_gate`` returns ``"delay"``.
2. ``_set_restart_phase`` drains the queue on BOTH
   ``BLOCKED → POST_UNBLOCK`` and ``POST_UNBLOCK → ""`` transitions.

These tests pin that contract by driving the public gate/queue API
directly — no FDB, no SPDK, no restart task — and checking:

- queue/delay is returned for every non-empty phase.
- queued ops execute in FIFO order.
- ops enqueued after the first drain (during post_unblock) are applied
  by the second drain (on phase clear).
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core import storage_node_ops


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_node(node_id="node-1", restart_phases=None):
    node = MagicMock(spec=StorageNode)
    node.get_id.return_value = node_id
    node.uuid = node_id
    node.cluster_id = "cluster-1"
    node.secondary_node_id = ""
    node.tertiary_node_id = ""
    node.mgmt_ip = "10.0.0.1"
    node.rpc_port = 8080
    node.rpc_username = "u"
    node.rpc_password = "p"
    node.status = StorageNode.STATUS_ONLINE
    node.restart_phases = restart_phases or {}
    return node


def _with_phase(node, lvs, phase):
    """Return a new node stub with ``restart_phases[lvs] = phase``."""
    n = _fake_node(node.get_id())
    n.restart_phases = dict(node.restart_phases or {})
    if phase:
        n.restart_phases[lvs] = phase
    else:
        n.restart_phases.pop(lvs, None)
    return n


def _install_db_mock(node):
    """Return a contextmanager that makes DBController.get_storage_node_by_id
    return ``node`` so storage_node_ops' helpers can resolve it."""
    def _get(nid):
        if nid == node.get_id():
            return node
        raise KeyError(nid)
    db = MagicMock()
    db.get_storage_node_by_id.side_effect = _get
    return patch.object(storage_node_ops, "DBController", return_value=db)


# ---------------------------------------------------------------------------
# wait_or_delay_for_restart_gate
# ---------------------------------------------------------------------------

class WaitOrDelayGate(unittest.TestCase):
    """``wait_or_delay_for_restart_gate`` must delay any non-empty phase."""

    def test_empty_phase_proceeds(self):
        node = _fake_node()
        with _install_db_mock(node):
            self.assertEqual(
                storage_node_ops.wait_or_delay_for_restart_gate(node.get_id(), "LVS_1"),
                "proceed",
            )

    def test_pre_block_delays(self):
        node = _fake_node(restart_phases={"LVS_1": StorageNode.RESTART_PHASE_PRE_BLOCK})
        with _install_db_mock(node):
            self.assertEqual(
                storage_node_ops.wait_or_delay_for_restart_gate(node.get_id(), "LVS_1"),
                "delay",
                "pre_block must delay: restarting node's SPDK state is about to "
                "be torn down, a create/delete applied now would be lost",
            )

    def test_blocked_delays(self):
        node = _fake_node(restart_phases={"LVS_1": StorageNode.RESTART_PHASE_BLOCKED})
        with _install_db_mock(node):
            self.assertEqual(
                storage_node_ops.wait_or_delay_for_restart_gate(node.get_id(), "LVS_1"),
                "delay",
            )

    def test_post_unblock_delays(self):
        node = _fake_node(restart_phases={"LVS_1": StorageNode.RESTART_PHASE_POST_UNBLOCK})
        with _install_db_mock(node):
            self.assertEqual(
                storage_node_ops.wait_or_delay_for_restart_gate(node.get_id(), "LVS_1"),
                "delay",
                "post_unblock must delay: subsystem re-registration loop is "
                "still running, add_ns would race subsystem_create",
            )

    def test_other_lvs_phase_does_not_gate(self):
        """Phase on a different LVS must not gate ours."""
        node = _fake_node(restart_phases={"LVS_OTHER": StorageNode.RESTART_PHASE_BLOCKED})
        with _install_db_mock(node):
            self.assertEqual(
                storage_node_ops.wait_or_delay_for_restart_gate(node.get_id(), "LVS_1"),
                "proceed",
            )


# ---------------------------------------------------------------------------
# check_non_leader_for_operation
# ---------------------------------------------------------------------------

class CheckNonLeaderPhases(unittest.TestCase):
    """The create/clone/delete/resize fan-out must queue on every non-empty
    phase."""

    def _call(self, node, lvs="LVS_1"):
        with _install_db_mock(node), \
             patch.object(storage_node_ops, "_check_peer_disconnected",
                          return_value=False), \
             patch.object(storage_node_ops, "_is_node_rpc_responsive",
                          return_value=True):
            return storage_node_ops.check_non_leader_for_operation(
                node.get_id(), lvs, operation_type="create",
                leader_op_completed=False, all_nodes=[node])

    def test_empty_phase_proceeds(self):
        node = _fake_node()
        self.assertEqual(self._call(node), "proceed")

    def test_pre_block_queues(self):
        """Pre-fix this returned "skip" — create-on-primary would run
        unthrottled while examine read a racing blobstore snapshot."""
        node = _fake_node(restart_phases={"LVS_1": StorageNode.RESTART_PHASE_PRE_BLOCK})
        self.assertEqual(self._call(node), "queue")

    def test_blocked_queues(self):
        node = _fake_node(restart_phases={"LVS_1": StorageNode.RESTART_PHASE_BLOCKED})
        self.assertEqual(self._call(node), "queue")

    def test_post_unblock_queues(self):
        """Pre-fix this fell through to the RPC-responsive check and returned
        "proceed" — but subsystem re-registration on the restarting node
        was not complete yet, so add_ns would race subsystem_create."""
        node = _fake_node(restart_phases={"LVS_1": StorageNode.RESTART_PHASE_POST_UNBLOCK})
        self.assertEqual(self._call(node), "queue")


# ---------------------------------------------------------------------------
# Queue + drain — FIFO across both phase transitions
# ---------------------------------------------------------------------------

class QueueDrainAcrossPhaseTransitions(unittest.TestCase):
    """The FIFO queue must drain on BOTH BLOCKED → POST_UNBLOCK and
    POST_UNBLOCK → "" transitions, so ops enqueued during post_unblock
    still land on the rebuilt node."""

    def setUp(self):
        # Clear any residual queue state from other tests.
        storage_node_ops._restart_op_queues.clear()
        self.node_id = "node-q"
        self.lvs = "LVS_Q"

    def _make_db_backed_node(self):
        """Build a lightweight node + db_controller that the restart-phase
        setter operates on. restart_phases is backed by a plain dict so
        we can drive transitions from the test."""
        node = _fake_node(self.node_id)
        node.write_to_db = MagicMock()
        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        return node, db

    def _set_phase(self, node, db, phase):
        storage_node_ops._set_restart_phase(node, self.lvs, phase, db)

    def test_fifo_drain_on_blocked_to_post_unblock(self):
        node, db = self._make_db_backed_node()
        calls = []
        storage_node_ops.queue_for_restart_drain(
            self.node_id, self.lvs, lambda: calls.append("a"), "a")
        storage_node_ops.queue_for_restart_drain(
            self.node_id, self.lvs, lambda: calls.append("b"), "b")

        self._set_phase(node, db, StorageNode.RESTART_PHASE_BLOCKED)
        self.assertEqual(calls, [], "no drain while still in blocked")

        self._set_phase(node, db, StorageNode.RESTART_PHASE_POST_UNBLOCK)
        self.assertEqual(calls, ["a", "b"], "drain must run in FIFO order")

    def test_drain_on_post_unblock_to_cleared(self):
        """Ops enqueued after the BLOCKED→POST_UNBLOCK drain (i.e. during
        post_unblock) must be applied when the phase is cleared."""
        node, db = self._make_db_backed_node()

        # Phase 1: blocked → post_unblock (drains anything enqueued so far).
        self._set_phase(node, db, StorageNode.RESTART_PHASE_BLOCKED)
        self._set_phase(node, db, StorageNode.RESTART_PHASE_POST_UNBLOCK)

        # A fresh op arrives DURING post_unblock (gate returned "delay").
        calls = []
        storage_node_ops.queue_for_restart_drain(
            self.node_id, self.lvs, lambda: calls.append("late"), "late")
        self.assertEqual(calls, [], "no drain yet, still in post_unblock")

        # Phase 2: post_unblock → cleared triggers the second drain.
        self._set_phase(node, db, "")
        self.assertEqual(
            calls, ["late"],
            "POST_UNBLOCK → '' must drain the queue a second time so ops "
            "enqueued during post_unblock land on the fully-rebuilt node",
        )

    def test_second_drain_on_empty_queue_is_noop(self):
        """Both drain points firing on an empty queue must be safe."""
        node, db = self._make_db_backed_node()
        # No ops queued, walk through every transition.
        self._set_phase(node, db, StorageNode.RESTART_PHASE_PRE_BLOCK)
        self._set_phase(node, db, StorageNode.RESTART_PHASE_BLOCKED)
        self._set_phase(node, db, StorageNode.RESTART_PHASE_POST_UNBLOCK)
        self._set_phase(node, db, "")
        # If we got here without raising, both drain() calls tolerated
        # an empty queue.


if __name__ == "__main__":
    unittest.main()
