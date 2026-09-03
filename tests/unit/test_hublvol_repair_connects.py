"""Repairing a hublvol must rejoin the lvstore, not just re-attach transport.

2026-09-01, LVS_10. Node ...4424 (secondary) took a hublvol remove event at
16:24:50. The health cycle re-attached the NVMe controller at 16:25:35 and
never sent the connect RPC, because it called
HublvolReconnectCoordinator.reconcile() -- attach only, per its own docstring:
"The caller can return immediately and proceed to bdev_lvol_connect_hublvol /
port-unblock." Nothing did.

The node was left with paths up but not connected as secondary, so when IO
reached it at 16:28:31 it could not redirect and triggered a leadership switch
instead. The control plane then unblocked the old primary at 16:28:37, the
client returned to a node that was no longer leader, and its IO came back as a
generic INTERNAL DEVICE ERROR -- which nvme-multipath does not retry on
another path. Client EIO, fio rc=4.
"""
import inspect

from simplyblock_core.controllers import health_controller


class TestRepairCompletesTheConnect:
    def test_reconcile_is_still_used_for_the_paths(self):
        """reconcile() is the only thing that adds a MISSING path through the
        coordinator's cooldown / cntlid-duplicate protection. Replacing it
        with connect_to_hublvol broke exactly that: connect_to_hublvol skips
        the attach when the remote bdev already exists, so a partially
        attached controller never got its missing path."""
        src = inspect.getsource(health_controller)
        assert "coordinator.reconcile(" in src

    def test_reconcile_is_followed_by_the_rejoin(self):
        """Both steps, in order: paths first, then rejoin the lvstore."""
        src = inspect.getsource(health_controller)
        assert src.index("coordinator.reconcile(") < src.index(
            "connected = node.connect_to_hublvol(")

    def test_repair_calls_connect_to_hublvol(self):
        src = inspect.getsource(health_controller)
        assert "connect_to_hublvol(" in src

    def test_role_is_derived_from_topology_not_defaulted(self):
        """A wrong role stamps two nodes as the same role for one LVS."""
        src = inspect.getsource(health_controller)
        assert 'role = "tertiary" if is_sec2 else "secondary"' in src
        # and that same value is what both steps receive
        assert "role=role" in src

    def test_connect_is_bounded_so_one_peer_cannot_stall_the_cycle(self):
        assert health_controller.HUBLVOL_REPAIR_RPC_TIMEOUT_SEC > 0
        assert health_controller.HUBLVOL_REPAIR_RPC_TIMEOUT_SEC <= 5
        src = inspect.getsource(health_controller)
        assert "rpc_timeout=HUBLVOL_REPAIR_RPC_TIMEOUT_SEC" in src

    def test_failure_marks_the_node_unhealthy(self):
        """A repair that did not complete must not report a healthy node."""
        src = inspect.getsource(health_controller)
        i = src.index("connected = node.connect_to_hublvol(")
        window = src[i:i + 900]
        assert "passed = False" in window


class TestConnectContract:
    """connect_to_hublvol is the complete operation the repair needs."""

    def test_it_requires_an_explicit_role(self):
        sig = inspect.signature(
            __import__("simplyblock_core.models.storage_node", fromlist=["StorageNode"])
            .StorageNode.connect_to_hublvol)
        assert sig.parameters["role"].kind == inspect.Parameter.KEYWORD_ONLY
        assert sig.parameters["role"].default is inspect.Parameter.empty

    def test_it_accepts_a_failover_peer_for_a_tertiary(self):
        sig = inspect.signature(
            __import__("simplyblock_core.models.storage_node", fromlist=["StorageNode"])
            .StorageNode.connect_to_hublvol)
        assert "failover_node" in sig.parameters

    def test_it_accepts_a_bounded_rpc_timeout(self):
        sig = inspect.signature(
            __import__("simplyblock_core.models.storage_node", fromlist=["StorageNode"])
            .StorageNode.connect_to_hublvol)
        assert "rpc_timeout" in sig.parameters


class TestEmptyControllerNameIsNotPruned:
    """An empty controller name must never reach the prune.

    bdev_nvme_controller_list("") returns EVERY controller on the node, so
    duplicate_attached_paths() then aggregates unrelated controllers and
    reports the same target IP as "duplicated" across them. Acting on that
    detaches live paths.

    Observed on the 2026-09-02 cluster at 12:16 during activation: 10 false
    duplicates reported on one node and the detach ran, because the prior
    commit made the (previously log-only) detection act.
    """

    def test_primitive_refuses_an_empty_name(self):
        from unittest.mock import MagicMock
        from simplyblock_core import storage_node_ops
        rpc = MagicMock()
        dup = [{"ctrlrs": [
            {"state": "enabled", "trid": {"traddr": "10.0.0.1"}},
            {"state": "enabled", "trid": {"traddr": "10.0.0.1"}}]}]
        assert storage_node_ops.prune_duplicate_paths(rpc, "", dup, 4420, "TCP") is False
        rpc.bdev_nvme_detach_controller.assert_not_called()

    def test_primitive_still_prunes_a_named_controller(self):
        from unittest.mock import MagicMock
        from simplyblock_core import storage_node_ops
        rpc = MagicMock()
        dup = [{"ctrlrs": [
            {"state": "enabled", "trid": {"traddr": "10.0.0.1"}},
            {"state": "enabled", "trid": {"traddr": "10.0.0.1"}},
            {"state": "enabled", "trid": {"traddr": "10.0.0.2"}}]}]
        assert storage_node_ops.prune_duplicate_paths(
            rpc, "LVS_1/hublvol", dup, 4420, "TCP") is True
        rpc.bdev_nvme_detach_controller.assert_called_once()

    def test_health_check_gates_on_a_named_hublvol(self):
        src = inspect.getsource(health_controller)
        assert "if hub_bdev else set()" in src
