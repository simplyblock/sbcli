"""The fence RPC budget is relaxed only with no fence held AND a removal in flight.

2026-10-14 run 63: a node removal left a survivor's SPDK aborted, the cluster
went suspended and every auto-restart cycle ended with

    Aborting recreate_lvstore on c2547485 for LVS_9: bdev_lvol_set_leader failed
    inside the port-fence window (budget 0.50s, fence 0.000s): Did not get a
    response from remote within timeout

while the node's SPDK proxy logged the very same call completing with 200 in
669ms (the other survivors: 74ms and 153ms). No peer port was fenced ("fence
0.000s"), the node was holding the departing node's orphaned replica, and the
0.5s budget only turned a slow but correct answer into an endless restart
loop, eight cycles of draining the cluster to all-offline and back.

The fence window itself is tuned and stays as it is, and a plain restart
keeps the bound even when unfenced: a healthy node answers far inside it.
These cover the decision ``_fenced()`` makes per call.
"""
import pytest

from simplyblock_core import constants
from simplyblock_core.storage_node_ops import _fence_rpc_bounds

BUDGET = constants.FENCE_RPC_TIMEOUT_SEC
RETRY = constants.FENCE_RPC_RETRY
EXAMINE = constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC
SET_LEADER = constants.FENCE_SET_LEADER_TIMEOUT_SEC


class TestFenceHeldIsNeverRelaxed:
    @pytest.mark.parametrize("removal", [False, True])
    def test_default_budget_early_in_the_window(self, removal):
        assert _fence_rpc_bounds(True, 0.1, removal_in_progress=removal) == (BUDGET, RETRY)

    @pytest.mark.parametrize("removal", [False, True])
    @pytest.mark.parametrize("budget", [EXAMINE, SET_LEADER])
    def test_explicit_budget_is_honoured(self, removal, budget):
        assert _fence_rpc_bounds(True, 0.1, budget=budget,
                                 removal_in_progress=removal) == (budget, RETRY)

    def test_budget_is_clamped_to_the_time_left(self):
        """A 6s call started 5s into a 7.5s fence may only run 2.5s."""
        elapsed = constants.FENCE_DEADLINE_SEC - 2.5
        timeout, _ = _fence_rpc_bounds(True, elapsed, budget=EXAMINE, removal_in_progress=True)
        assert timeout == pytest.approx(2.5)

    def test_past_the_deadline_yields_a_non_positive_timeout(self):
        """_fenced() turns a non-positive timeout into the deadline abort
        before building a client, so the value must not be clamped upwards."""
        timeout, _ = _fence_rpc_bounds(True, constants.FENCE_DEADLINE_SEC + 0.2)
        assert timeout is not None and timeout <= 0


class TestNoFenceHeld:
    def test_plain_restart_keeps_the_bound(self):
        """Not fenced, no removal: the bound stays, as the fence work tuned it."""
        assert _fence_rpc_bounds(False, 0.0) == (BUDGET, RETRY)
        assert _fence_rpc_bounds(False, 0.0, budget=EXAMINE) == (EXAMINE, RETRY)

    def test_removal_gets_the_ordinary_client(self):
        """Not fenced, removal in flight: ``(None, None)`` tells _fenced to
        build the default client rather than the 0.5s/retry=1 one."""
        assert _fence_rpc_bounds(False, 0.0, removal_in_progress=True) == (None, None)

    def test_an_explicit_budget_does_not_apply_during_a_removal_either(self):
        assert _fence_rpc_bounds(False, 0.0, budget=EXAMINE,
                                 removal_in_progress=True) == (None, None)

    def test_elapsed_is_ignored_without_a_fence(self):
        """_fence_elapsed() reports 0.0 with no fence; any value must still
        give the unfenced answer, never a clamp against the deadline."""
        assert _fence_rpc_bounds(False, 3.0, removal_in_progress=True) == (None, None)
        assert _fence_rpc_bounds(False, 3.0) == (BUDGET, RETRY)


class TestRemovalInFlight:
    """The relaxation keys off removals still being worked, never off the
    terminal states: a REMOVED node stays in the node list for good, so counting
    it would relax every restart after the first removal ever made."""

    def test_in_flight_is_the_removal_walk_without_its_terminal_state(self):
        from simplyblock_core.models.storage_node import StorageNode
        from simplyblock_core.storage_node_ops import REMOVAL_IN_FLIGHT_STATUSES
        assert set(REMOVAL_IN_FLIGHT_STATUSES) == {
            StorageNode.STATUS_PENDING_REMOVAL, StorageNode.STATUS_MIGRATING_DEVICES,
            StorageNode.STATUS_MIGRATING_LVOLS, StorageNode.STATUS_IN_REMOVAL}
        assert StorageNode.STATUS_REMOVED not in REMOVAL_IN_FLIGHT_STATUSES
        assert StorageNode.STATUS_REMOVED_FAILED not in REMOVAL_IN_FLIGHT_STATUSES
        assert set(REMOVAL_IN_FLIGHT_STATUSES) < set(StorageNode.REMOVAL_STATUS_ORDER)


def test_run63_set_leader_fails_fenced_and_in_a_plain_restart_but_passes_during_a_removal():
    """The incident numbers: 669ms against the 0.5s budget."""
    observed = 0.669
    for bounds in (_fence_rpc_bounds(True, 0.0), _fence_rpc_bounds(False, 0.0)):
        timeout, _ = bounds
        assert timeout is not None and observed > timeout
    assert _fence_rpc_bounds(False, 0.0, removal_in_progress=True) == (None, None)
