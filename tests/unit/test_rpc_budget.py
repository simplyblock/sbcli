"""The fence budget must bind everything under the fence -- and nothing else.

Threading timeout=/retry= through call sites did not work for this window: the
fenced region calls model methods that build their own rpc_client (
recreate_hublvol, connect_to_hublvol, create_transfer_hublvol), with
expose_bdev a level below them. A 2026-09-01 audit found 14 bounded calls
against 45 unbounded ones reached that way, after two earlier passes had each
missed sites.

The risk of an ambient budget is the mirror image: leaking 0.5s onto ordinary
work once the fence is gone. These tests pin both directions.
"""
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants
from simplyblock_core.utils import rpc_budget


@pytest.fixture(autouse=True)
def _clean():
    rpc_budget.clear_budget()
    yield
    rpc_budget.clear_budget()


class TestBudgetState:
    def test_absent_by_default(self):
        assert rpc_budget.current_budget() is None

    def test_set_and_clear(self):
        rpc_budget.set_budget(0.5, 1)
        assert rpc_budget.current_budget() == (0.5, 1)
        rpc_budget.clear_budget()
        assert rpc_budget.current_budget() is None

    def test_clear_is_safe_when_unset(self):
        rpc_budget.clear_budget()
        assert rpc_budget.current_budget() is None

    def test_context_manager_restores_absence(self):
        with rpc_budget.fence_budget(0.5, 1):
            assert rpc_budget.current_budget() == (0.5, 1)
        assert rpc_budget.current_budget() is None

    def test_context_manager_restores_on_exception(self):
        with pytest.raises(RuntimeError):
            with rpc_budget.fence_budget(0.5, 1):
                raise RuntimeError("boom")
        assert rpc_budget.current_budget() is None

    def test_nesting_restores_the_outer_budget(self):
        with rpc_budget.fence_budget(0.5, 1):
            with rpc_budget.fence_budget(6, 0):
                assert rpc_budget.current_budget() == (6, 0)
            assert rpc_budget.current_budget() == (0.5, 1)

    def test_budget_does_not_leak_to_another_thread(self):
        import threading
        seen = {}
        rpc_budget.set_budget(0.5, 1)

        def worker():
            seen["other"] = rpc_budget.current_budget()

        t = threading.Thread(target=worker)
        t.start()
        t.join()
        assert seen["other"] is None          # thread-local, as intended
        assert rpc_budget.current_budget() == (0.5, 1)


class TestRpcClientHonoursBudget:
    """StorageNode.rpc_client() is where the nested helpers get bounded."""

    @staticmethod
    def _node():
        from simplyblock_core.models.storage_node import StorageNode
        n = StorageNode()
        n.mgmt_ip = "10.0.0.1"
        n.rpc_port = 4420
        n.rpc_username = "u"
        n.rpc_password = "p"
        return n

    def _capture(self, node, **kwargs):
        with patch("simplyblock_core.models.storage_node.RPCClient") as rc:
            rc.return_value = MagicMock()
            node.rpc_client(**kwargs)
            return rc.call_args.kwargs

    def test_no_budget_leaves_defaults_untouched(self):
        """Outside the fence, callers must keep the 180s default."""
        got = self._capture(self._node())
        assert "timeout" not in got and "retry" not in got

    def test_budget_bounds_a_bare_client(self):
        rpc_budget.set_budget(constants.FENCE_RPC_TIMEOUT_SEC,
                              constants.FENCE_RPC_RETRY)
        got = self._capture(self._node())
        assert got["timeout"] == constants.FENCE_RPC_TIMEOUT_SEC
        assert got["retry"] == constants.FENCE_RPC_RETRY

    def test_explicit_timeout_wins_over_the_budget(self):
        """bdev_wait_for_examine legitimately asks for longer."""
        rpc_budget.set_budget(0.5, 1)
        got = self._capture(self._node(), timeout=6, retry=0)
        assert got["timeout"] == 6
        assert got["retry"] == 0

    def test_budget_stops_applying_once_cleared(self):
        node = self._node()
        rpc_budget.set_budget(0.5, 1)
        assert self._capture(node)["timeout"] == 0.5
        rpc_budget.clear_budget()
        assert "timeout" not in self._capture(node)
