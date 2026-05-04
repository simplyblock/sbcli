# coding=utf-8
"""
test_connect_hublvol_lvs_node.py — pins the ``lvs_node`` parameter on
``StorageNode.connect_to_hublvol`` and verifies it routes LVS metadata
from the configured primary, not from the (possibly peer) hublvol target.

Background — incident 2026-05-02 (k8s_native_failover_ha-20260502-101452,
worker1 at 15:53:42):

  - worker1 is configured *secondary* of LVS_4729 (and *tertiary* of
    LVS_6207).
  - During worker1's restart of its LVS_6207 lvstore, the configured
    primary (worker2) was in a planned outage; worker5 had taken over
    leadership of LVS_6207. The recreate path on worker1 picked
    worker5 as ``sync_target`` (acting leader) and called
    ``connect_to_hublvol(worker5, role='tertiary')``.
  - The OLD code used ``primary_node.lvstore`` / ``primary_node.jm_vuid``
    / ``primary_node.hublvol.bdev_name`` for everything. Since
    worker5's OWN primary lvstore is LVS_4729 (not LVS_6207), the
    ``bdev_lvol_set_lvs_opts`` RPC fired with ``groupid=4729`` and
    ``port=4434`` while we were trying to wire up LVS_6207 — and the
    remote bdev name became ``LVS_4729/hublvoln1`` rather than the
    expected ``LVS_6207/hublvoln1``.

The fix: ``connect_to_hublvol`` accepts a separate ``lvs_node``
parameter (defaulting to ``primary_node`` for backward-compat) that
sources the LVS-side metadata. ``primary_node`` is now strictly the
nvme-of attach target.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode


class FakeHubLvol:
    def __init__(self, bdev_name, nqn, port):
        self.bdev_name = bdev_name
        self.nqn = nqn
        self.nvmf_port = port
        self.uuid = "fake-uuid"
        self.nguid = "fake-nguid"
        self.model_number = "fake-model"


def _make_node(uuid, lvstore, jm_vuid, hub_bdev_name=None, hub_nqn=None,
               hub_port=4435, lvol_subsys_port=4434):
    n = StorageNode()
    n.uuid = uuid
    n.lvstore = lvstore
    n.jm_vuid = jm_vuid
    n.cluster_id = "c1"
    n.hublvol = FakeHubLvol(
        bdev_name=hub_bdev_name or f"{lvstore}/hublvol",
        nqn=hub_nqn or f"nqn.test:hublvol:{lvstore}",
        port=hub_port,
    )
    n.get_lvol_subsys_port = MagicMock(return_value=lvol_subsys_port)
    n.write_to_db = MagicMock(return_value=True)
    n.rpc_client = MagicMock()
    return n


class TestConnectToHublvolLvsNode(unittest.TestCase):
    """When lvs_node differs from primary_node, the LVS-side RPCs use
    lvs_node's metadata, not primary_node's."""

    def setUp(self):
        # The recovering node (snode) — the one calling connect_to_hublvol
        self.snode = _make_node("snode", "LVS_640", jm_vuid=640)

        # primary_node = the *peer* that took over leadership for
        # an LVS not its own (e.g. worker5 acting as leader of LVS_6207
        # despite owning LVS_4729 as its own primary).
        self.peer = _make_node(
            "peer-uuid", lvstore="LVS_4729", jm_vuid=4729,
            hub_bdev_name="LVS_4729/hublvol",
            hub_nqn="nqn.test:hublvol:LVS_4729",
            hub_port=4435,
            lvol_subsys_port=4434,
        )

        # lvs_node = the configured primary of the LVS we're connecting
        # for (e.g. worker2 for LVS_6207). The node may be offline; what
        # matters is its DB record carries the right LVS metadata.
        self.lvs_node = _make_node(
            "lvs-node-uuid", lvstore="LVS_6207", jm_vuid=6207,
            hub_bdev_name="LVS_6207/hublvol",
            hub_nqn="nqn.test:hublvol:LVS_6207",
            hub_port=4433,
            lvol_subsys_port=4432,
        )

        # snode.rpc_client() returns a MagicMock that records calls
        self.rpc = MagicMock()
        self.rpc.get_bdevs.return_value = [{"name": "LVS_6207/hublvoln1"}]
        self.rpc.bdev_lvol_set_lvs_opts.return_value = True
        self.rpc.bdev_lvol_connect_hublvol.return_value = True
        self.snode.rpc_client = MagicMock(return_value=self.rpc)

    def test_lvs_node_routes_set_lvs_opts(self):
        """bdev_lvol_set_lvs_opts must use lvs_node's lvstore + jm_vuid + port."""
        ok = self.snode.connect_to_hublvol(
            self.peer, failover_node=None, role="tertiary",
            lvs_node=self.lvs_node,
        )
        self.assertTrue(ok)
        # The set_lvs_opts call must use lvs_node's lvstore / jm_vuid / port,
        # NOT the peer's.
        self.rpc.bdev_lvol_set_lvs_opts.assert_called_once()
        args, kwargs = self.rpc.bdev_lvol_set_lvs_opts.call_args
        self.assertEqual(args[0], "LVS_6207",
                         "lvstore positional arg should be lvs_node.lvstore "
                         "(LVS_6207), not peer.lvstore (LVS_4729)")
        self.assertEqual(kwargs.get("groupid"), 6207,
                         "groupid should be lvs_node.jm_vuid (6207)")
        self.assertEqual(kwargs.get("subsystem_port"), 4432,
                         "subsystem_port should be lvs_node's port (4432)")
        self.assertEqual(kwargs.get("role"), "tertiary")

    def test_lvs_node_routes_connect_hublvol(self):
        """bdev_lvol_connect_hublvol must use lvs_node.lvstore for both args."""
        ok = self.snode.connect_to_hublvol(
            self.peer, failover_node=None, role="tertiary",
            lvs_node=self.lvs_node,
        )
        self.assertTrue(ok)
        self.rpc.bdev_lvol_connect_hublvol.assert_called_once()
        args, kwargs = self.rpc.bdev_lvol_connect_hublvol.call_args
        self.assertEqual(args[0], "LVS_6207",
                         "first arg (lvstore) should be lvs_node.lvstore")
        # remote_bdev should be derived from lvs_node, not peer
        self.assertEqual(args[1], "LVS_6207/hublvoln1",
                         "remote bdev name should encode lvs_node's "
                         "lvstore (LVS_6207), not peer's (LVS_4729)")

    def test_lvs_node_routes_remote_bdev_lookup(self):
        """The pre-attach bdev existence check must look up by lvs_node's bdev name."""
        # Force the get_bdevs branch to confirm what name is checked.
        self.rpc.get_bdevs.return_value = []  # bdev not present → triggers coordinator
        with patch(
            "simplyblock_core.utils.hublvol_reconnect.HublvolReconnectCoordinator"
        ) as mock_coord_cls:
            mock_coord = MagicMock()
            mock_coord.reconcile.return_value = True
            mock_coord_cls.return_value = mock_coord
            self.snode.connect_to_hublvol(
                self.peer, failover_node=None, role="tertiary",
                lvs_node=self.lvs_node,
            )
        # The first get_bdevs call was the existence check on remote_bdev
        first_call = self.rpc.get_bdevs.call_args_list[0]
        args, _kw = first_call
        self.assertEqual(
            args[0], "LVS_6207/hublvoln1",
            "remote bdev existence check should use lvs_node's lvstore",
        )
        # The coordinator should be invoked with lvs_node, not peer,
        # so it derives ctrl_name / nqn / port from lvs_node.hublvol.
        mock_coord.reconcile.assert_called_once()
        c_args, _c_kw = mock_coord.reconcile.call_args
        # Signature: (node, primary_node_or_lvs_node, peers, role=...)
        self.assertIs(c_args[1], self.lvs_node,
                      "coordinator must receive lvs_node so its hublvol "
                      "metadata (NQN/bdev_name/port) matches the LVS we're "
                      "connecting for, not the peer's own primary LVS")
        self.assertIn(self.peer, c_args[2],
                      "peer must still appear in peer_nodes — it remains "
                      "the actual NVMe-oF attach target (its IPs)")


class TestBackwardCompat(unittest.TestCase):
    """Without lvs_node, behavior is identical to the prior
    primary_node-only contract."""

    def test_lvs_node_defaults_to_primary_node(self):
        node = _make_node("self", "LVS_640", jm_vuid=640)
        primary = _make_node("primary", "LVS_3261", jm_vuid=3261,
                             hub_bdev_name="LVS_3261/hublvol",
                             hub_nqn="nqn.test:hublvol:LVS_3261",
                             hub_port=4427,
                             lvol_subsys_port=4420)
        rpc = MagicMock()
        rpc.get_bdevs.return_value = [{"name": "LVS_3261/hublvoln1"}]
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_connect_hublvol.return_value = True
        node.rpc_client = MagicMock(return_value=rpc)

        ok = node.connect_to_hublvol(primary, failover_node=None, role="secondary")
        self.assertTrue(ok)
        # Without lvs_node, all metadata comes from primary_node — backward compat.
        args, kwargs = rpc.bdev_lvol_set_lvs_opts.call_args
        self.assertEqual(args[0], "LVS_3261")
        self.assertEqual(kwargs.get("groupid"), 3261)
        self.assertEqual(kwargs.get("subsystem_port"), 4420)


class TestSourceCallSites(unittest.TestCase):
    """Pin the call sites in recreate_lvstore_on_non_leader use lvs_node."""

    @classmethod
    def setUpClass(cls):
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..",
            "simplyblock_core", "storage_node_ops.py",
        )
        with open(path, "r") as f:
            cls.src = f.read()

    def test_recreate_on_non_leader_passes_lvs_node_for_tertiary_branch(self):
        # The tertiary branch must pass lvs_node=primary_node when the
        # sync_target may be a peer (acting leader for an LVS that
        # primary_node owns but is offline for).
        start = self.src.index("def recreate_lvstore_on_non_leader(")
        end = self.src.index("\ndef ", start + 1)
        body = self.src[start:end]
        # Find the tertiary branch (sync_target is not None)
        tertiary_idx = body.index("if sync_target is not None:")
        tertiary_window = body[tertiary_idx:tertiary_idx + 1500]
        self.assertIn(
            "lvs_node=primary_node",
            tertiary_window,
            "tertiary branch in recreate_lvstore_on_non_leader must pass "
            "lvs_node=primary_node so connect_to_hublvol uses the right "
            "LVS metadata when sync_target is a peer",
        )

    def test_recreate_on_non_leader_passes_lvs_node_for_secondary_branch(self):
        start = self.src.index("def recreate_lvstore_on_non_leader(")
        end = self.src.index("\ndef ", start + 1)
        body = self.src[start:end]
        # Secondary branch: snode.connect_to_hublvol(leader_node, ...)
        sec_idx = body.index("snode.connect_to_hublvol(leader_node")
        sec_window = body[sec_idx:sec_idx + 800]
        self.assertIn(
            "lvs_node=primary_node",
            sec_window,
            "secondary branch must also pass lvs_node=primary_node "
            "(leader_node may be a peer)",
        )


if __name__ == "__main__":
    unittest.main()
