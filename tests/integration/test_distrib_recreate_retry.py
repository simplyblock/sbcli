# coding=utf-8
"""
test_distrib_recreate_retry.py

Regression test for the restart-time distrib (re)creation race:

If a peer node goes offline at the exact moment a restarting node rebuilds its
lvstore stack, the cluster map is briefly stale (the departed node's devices
still flagged online) and bdev_distrib_create / the cluster-map push fails.

Old behavior: _create_bdev_stack._create_distr swallowed the failure (logged
only) and the loop marked the distrib branch ret=True unconditionally, so the
restart "completed" on a broken distrib.

New behavior:
  - the failed distrib is retried exactly once (with a freshly built map);
  - if the retry succeeds, the stack build succeeds;
  - if the retry also fails, _create_bdev_stack returns (False, err) and rolls
    back, so recreate_lvstore aborts the restart -> standard auto-restart.

All external dependencies (RPC, DBController, cluster-map push, sleep) mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode


def _stack():
    return [
        {"type": "bdev_distr", "name": "distrib_1", "params": {"vuid": 1}},
        {"type": "bdev_raid", "name": "raid_1", "params": {"strip_size_kb": 64},
         "distribs_list": ["distrib_1"]},
    ]


def _snode():
    n = StorageNode()
    n.uuid = "snode-1"
    n.cluster_id = "cluster-1"
    n.enable_ha_jm = False
    n.ha_jm_count = 4
    n.jm_device = MagicMock(jm_bdev="jm0")
    n.distrib_cpu_cores = []
    n.lvstore_stack = _stack()
    return n


@patch("simplyblock_core.storage_node_ops.time.sleep", return_value=None)
@patch("simplyblock_core.storage_node_ops.distr_controller")
@patch("simplyblock_core.storage_node_ops.DBController")
class TestDistribRecreateRetry(unittest.TestCase):

    def _wire(self, MockDB, mock_distr, *, create_effects, map_effects=None):
        import simplyblock_core.storage_node_ops as ops
        self.ops = ops
        cluster = MagicMock(full_page_unmap=False)
        MockDB.return_value.get_cluster_by_id.return_value = cluster
        rpc = MagicMock()
        rpc.get_bdevs.return_value = []
        rpc.bdev_distrib_create.side_effect = create_effects
        rpc.bdev_raid_create.return_value = True
        snode = _snode()
        snode.rpc_client = MagicMock(return_value=rpc)
        mock_distr.send_cluster_map_to_distr.side_effect = (
            map_effects if map_effects is not None else lambda *a, **k: True)
        return ops, snode, rpc

    def test_retry_succeeds_after_stale_map_failure(self, MockDB, mock_distr, _sleep):
        # First create raises (stale map), second succeeds.
        ops, snode, rpc = self._wire(
            MockDB, mock_distr, create_effects=[Exception("stale map"), None])
        ok, err = ops._create_bdev_stack(snode)
        assert ok is True and err is None, (ok, err)
        assert rpc.bdev_distrib_create.call_count == 2      # initial + retry
        rpc.bdev_raid_create.assert_called_once()           # raid built on good distrib

    def test_map_push_failure_then_retry_succeeds(self, MockDB, mock_distr, _sleep):
        # Creation succeeds but the first cluster-map push fails, second works.
        ops, snode, rpc = self._wire(
            MockDB, mock_distr, create_effects=[None, None],
            map_effects=[False, True])
        ok, err = ops._create_bdev_stack(snode)
        assert ok is True and err is None, (ok, err)
        assert rpc.bdev_distrib_create.call_count == 2
        rpc.bdev_raid_create.assert_called_once()

    def test_abort_when_retry_also_fails(self, MockDB, mock_distr, _sleep):
        # Both attempts raise -> propagate failure, no raid, rollback.
        ops, snode, rpc = self._wire(
            MockDB, mock_distr,
            create_effects=[Exception("stale map"), Exception("still stale")])
        ok, err = ops._create_bdev_stack(snode)
        assert ok is False
        assert "distrib" in err.lower(), err
        rpc.bdev_raid_create.assert_not_called()            # never built on broken distrib
        assert rpc.bdev_distrib_create.call_count == 2      # tried exactly once more
        rpc.bdev_distrib_delete.assert_called()             # cleanup (retry and/or rollback)


if __name__ == "__main__":
    unittest.main()
