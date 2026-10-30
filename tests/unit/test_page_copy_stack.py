"""
Page copy (snapshot copy-on-write in the distribution layer) is a cluster-wide
setting fixed at creation: create_lvstore persists it on every distrib stack
entry, and bdev_distrib_create sends it to the data plane.
"""

import unittest
from unittest import mock
from unittest.mock import MagicMock

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.rpc_client import RPCClient


def _lvstore_stack(monkeypatch, page_copy, shared_placement=True):
    cluster = MagicMock()
    cluster.page_copy = page_copy
    cluster.shared_placement = shared_placement
    cluster.write_protection_v2 = True
    db = MagicMock()
    db.get_cluster_by_id.return_value = cluster
    monkeypatch.setattr(storage_node_ops, "DBController", lambda: db)
    monkeypatch.setattr(storage_node_ops.utils, "get_next_lvstore_ports", lambda cluster_id: (4430, 4431))
    # Publishing the node's ports (pod network) is outside this test; main's
    # create_lvstore does it before the stack is built.
    monkeypatch.setattr(storage_node_ops, "storage_network", MagicMock(), raising=False)
    vuids = iter(range(100, 200))
    monkeypatch.setattr(storage_node_ops.utils, "get_random_vuid", lambda: next(vuids))
    captured = {}

    def create_bdev_stack(snode, stack, primary_node=None):
        captured["stack"] = stack
        return None, "stop here"

    monkeypatch.setattr(storage_node_ops, "_create_bdev_stack", create_bdev_stack)
    snode = MagicMock()
    snode.enable_ha_jm = False
    snode.number_of_distribs = 2
    assert storage_node_ops.create_lvstore(snode, 2, 1, 4096, 4096, 2 * 1024 * 1024, 0) is False
    return captured["stack"]


@pytest.mark.parametrize("page_copy", [True, False])
def test_create_lvstore_persists_page_copy_per_distrib(monkeypatch, page_copy):
    stack = _lvstore_stack(monkeypatch, page_copy)
    distribs = [b for b in stack if b["type"] == "bdev_distr"]
    assert len(distribs) == 2
    for b in distribs:
        assert b["params"].get("page_copy", False) is page_copy
        assert b["params"]["shared_placement"] is True


def test_create_lvstore_cluster_is_one_distrib_page_per_data_chunk(monkeypatch):
    # A COPY is one whole distrib page: the lvstore cluster must be ndcs pages.
    stack = _lvstore_stack(monkeypatch, True)
    lvs = next(b for b in stack if b["type"] == "bdev_lvstore")
    assert lvs["params"]["cluster_sz"] == 2 * 2 * 1024 * 1024


@pytest.mark.parametrize("page_copy", [True, False])
def test_bdev_distrib_create_sends_page_copy(page_copy):
    rpc = RPCClient.__new__(RPCClient)
    rpc.bdev_get = MagicMock(return_value=None)
    rpc._request = MagicMock(return_value=True)
    rpc.bdev_distrib_create("distrib_1", 1, 2, 1, 1000, 4096, ["jm_1"], 4096, page_copy=page_copy)
    params = rpc._request.call_args.kwargs
    assert params.get("page_copy", False) is page_copy


def test_cluster_page_copy_defaults_off():
    assert Cluster().page_copy is False


class TestKeepPlacedBalancing(unittest.TestCase):
    """Page copy clusters: restart balancing keeps columns that page copies placed acceptably."""

    def _client(self):
        from simplyblock_core import rpc_client as rpc_mod
        client = rpc_mod.RPCClient.__new__(rpc_mod.RPCClient)
        client._request = mock.Mock(return_value=True)
        return client

    def test_expansion_start_sends_keep_placed_only_when_set(self):
        c = self._client()
        c.distr_migration_expansion_start("d", keep_placed=True)
        self.assertIs(c._request.call_args.kwargs["keep_placed"], True)
        c.distr_migration_expansion_start("d")
        self.assertNotIn("keep_placed", c._request.call_args.kwargs)

    def test_only_restart_balancing_passes_keep_placed(self):
        import inspect

        from simplyblock_core.services import (
            tasks_runner_migration,
            tasks_runner_new_dev_migration,
        )
        self.assertIn('keep_placed=bool(getattr(db.get_cluster_by_id(snode.cluster_id), "page_copy", False))',
                      inspect.getsource(tasks_runner_migration))
        # expansion onto new devices keeps moving data to the primary location
        self.assertNotIn("keep_placed", inspect.getsource(tasks_runner_new_dev_migration))
