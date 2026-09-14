"""Health-check remote-device sweep gate (run 20260725).

sync_remote_devices_from_spdk pays one SPDK inventory RPC per node per pass;
it fired every ~40-50s per SPDK instance around the clock. The sweep only
changes its answer when peer topology changes, so it is now gated on a
topology epoch with a forced floor.
"""
import types

from simplyblock_core import constants
from simplyblock_core.services import health_check_service
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice


def _node(node_id, status=StorageNode.STATUS_ONLINE, devs=()):
    return types.SimpleNamespace(
        get_id=lambda: node_id, status=status,
        nvme_devices=[types.SimpleNamespace(
            get_id=lambda d=d: d, status=NVMeDevice.STATUS_ONLINE,
            alceml_bdev=f"alc_{d}") for d in devs])


class TestRemoteSweepGate:
    def setup_method(self, _method):
        health_check_service._remote_sweep_memo.clear()

    def test_first_pass_runs(self):
        snode = _node("n1")
        peers = [snode, _node("n2", devs=["d1"])]
        assert health_check_service._remote_sweep_due(snode, peers) is True

    def test_unchanged_topology_skips(self):
        snode = _node("n1")
        peers = [snode, _node("n2", devs=["d1"])]
        assert health_check_service._remote_sweep_due(snode, peers) is True
        assert health_check_service._remote_sweep_due(snode, peers) is False
        assert health_check_service._remote_sweep_due(snode, peers) is False

    def test_topology_change_reruns(self):
        snode = _node("n1")
        peers = [snode, _node("n2", devs=["d1"])]
        assert health_check_service._remote_sweep_due(snode, peers) is True
        changed = [snode, _node("n2", devs=["d1", "d2"])]
        assert health_check_service._remote_sweep_due(snode, changed) is True

    def test_peer_status_change_reruns(self):
        snode = _node("n1")
        peers = [snode, _node("n2", devs=["d1"])]
        assert health_check_service._remote_sweep_due(snode, peers) is True
        flipped = [snode, _node("n2", status=StorageNode.STATUS_DOWN,
                                devs=["d1"])]
        assert health_check_service._remote_sweep_due(snode, flipped) is True

    def test_forced_floor_reruns(self, monkeypatch):
        snode = _node("n1")
        peers = [snode, _node("n2", devs=["d1"])]
        assert health_check_service._remote_sweep_due(snode, peers) is True
        # Age the memo past the floor.
        epoch, ts = health_check_service._remote_sweep_memo["n1"]
        health_check_service._remote_sweep_memo["n1"] = (
            epoch, ts - constants.HEALTH_CHECK_REMOTE_SWEEP_FORCE_SEC - 1)
        assert health_check_service._remote_sweep_due(snode, peers) is True

    def test_gate_is_per_node(self):
        n1, n2 = _node("n1"), _node("n2")
        peers = [n1, n2, _node("n3", devs=["d1"])]
        assert health_check_service._remote_sweep_due(n1, peers) is True
        assert health_check_service._remote_sweep_due(n2, peers) is True
        assert health_check_service._remote_sweep_due(n1, peers) is False
