"""The fence RPC budget applies while a peer port is fenced, and only then.

2026-10-14 run 63: a node removal left a survivor's SPDK aborted, the cluster
went suspended and every auto-restart cycle ended with

    Aborting recreate_lvstore on c2547485 for LVS_9: bdev_lvol_set_leader failed
    inside the port-fence window (budget 0.50s, fence 0.000s): Did not get a
    response from remote within timeout

while the node's SPDK proxy logged the very same call completing with 200 in
669ms. No peer port was fenced ("fence 0.000s": every peer had been skipped as
disconnected), so the 0.5s budget protected nothing -- SPDK's reject conversion
only threatens a port we are holding blocked -- and merely turned a slow but
correct answer into an endless restart loop, seven cycles of draining the
cluster to all-offline and back.

These cover the decision ``_fenced()`` makes per call: bounded inside a fence,
ordinary client outside it.
"""
import pytest

from simplyblock_core import constants
from simplyblock_core.storage_node_ops import _fence_rpc_bounds


class TestNoFenceHeld:
    def test_unfenced_call_gets_the_ordinary_client(self):
        """No fence, no budget: ``(None, None)`` tells _fenced to build the
        default client rather than the 0.5s/retry=1 one."""
        assert _fence_rpc_bounds(False, 0.0) == (None, None)

    def test_an_explicit_budget_does_not_apply_either(self):
        """bdev_wait_for_examine asks for 6s inside the window; outside it
        the request is moot and the default client is used as well."""
        assert _fence_rpc_bounds(
            False, 0.0, budget=constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC) == (None, None)

    def test_elapsed_is_ignored_without_a_fence(self):
        """_fence_elapsed() reports 0.0 with no fence; any value must still
        yield the ordinary client, never a clamp against the deadline."""
        assert _fence_rpc_bounds(False, 3.0) == (None, None)


class TestFenceHeld:
    def test_default_budget_early_in_the_window(self):
        timeout, retry = _fence_rpc_bounds(True, 0.1)
        assert timeout == constants.FENCE_RPC_TIMEOUT_SEC
        assert retry == constants.FENCE_RPC_RETRY

    def test_explicit_budget_is_honoured(self):
        timeout, retry = _fence_rpc_bounds(
            True, 0.1, budget=constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC)
        assert timeout == constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC
        assert retry == constants.FENCE_RPC_RETRY

    def test_budget_is_clamped_to_the_time_left(self):
        """A 6s call started 5s into a 7.5s fence may only run 2.5s."""
        elapsed = constants.FENCE_DEADLINE_SEC - 2.5
        timeout, _ = _fence_rpc_bounds(
            True, elapsed, budget=constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC)
        assert timeout == pytest.approx(2.5)

    def test_past_the_deadline_yields_a_non_positive_timeout(self):
        """_fenced() turns a non-positive timeout into the deadline abort
        before building a client, so the value must not be clamped upwards."""
        timeout, _ = _fence_rpc_bounds(True, constants.FENCE_DEADLINE_SEC + 0.2)
        assert timeout is not None and timeout <= 0

    def test_run63_set_leader_would_have_passed_unfenced_and_failed_fenced(self):
        """The incident numbers: 669ms against the 0.5s budget."""
        observed = 0.669
        timeout, _ = _fence_rpc_bounds(True, 0.0)
        assert timeout is not None and observed > timeout
        assert _fence_rpc_bounds(False, 0.0) == (None, None)
