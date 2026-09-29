"""Pure rules of the LVS stack on a sync-replication remote triplet: the
triplet a node belongs to, the leader candidates of a non-leader rebuild, the
hublvol site rule, the pending marks kept with the refs, and the probes that
tell a torn-down or not-yet-built distrib from a failure. The flows against
the real database are in tests/integration/test_sync_replication_lvs_stack.py.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from simplyblock_core import distr_controller
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException


def _owner(**fields):
    node = StorageNode()
    node.uuid = "p"
    node.site = "site-a"
    node.secondary_node_id = "s"
    node.tertiary_node_id = "t"
    node.remote_primary_node_id = "rp"
    node.remote_secondary_node_id = "rs"
    node.remote_tertiary_node_id = "rt"
    for key, value in fields.items():
        setattr(node, key, value)
    return node


class TestLvsTripletOf:

    def test_home_members_get_the_home_triplet(self):
        owner = _owner()
        for node_id in ("p", "s", "t"):
            assert ops.lvs_triplet_of(owner, node_id) == ("p", "s", "t")

    def test_remote_members_get_the_remote_triplet(self):
        owner = _owner()
        for node_id in ("rp", "rs", "rt"):
            assert ops.lvs_triplet_of(owner, node_id) == ("rp", "rs", "rt")

    def test_without_refs_everything_is_home(self):
        owner = _owner(remote_primary_node_id="", remote_secondary_node_id="",
                       remote_tertiary_node_id="")
        assert ops.lvs_triplet_of(owner, "x") == ("p", "s", "t")
        assert ops.lvs_triplet_of(owner, "") == ("p", "s", "t")

    def test_ftt1_home_triplet_has_no_tertiary(self):
        owner = _owner(tertiary_node_id="")
        assert ops.lvs_triplet_of(owner, "s") == ("p", "s", "")


class TestRebuildCandidates:

    def test_home_triplet_without_the_rebuilding_node(self):
        owner = _owner()
        assert ops.non_leader_rebuild_candidates(owner, "rs") == ["p", "s", "t"]
        assert ops.non_leader_rebuild_candidates(owner, "s") == ["p", "t"]

    def test_lost_home_site_leads_from_the_remote_triplet(self):
        owner = _owner()
        assert ops.non_leader_rebuild_candidates(owner, "s", "site-a") == ["rp", "rs", "rt"]
        assert ops.non_leader_rebuild_candidates(owner, "p", "site-a") == ["rp", "rs", "rt"]
        assert ops.non_leader_rebuild_candidates(owner, "rs", "site-a") == ["rp", "rt"]

    def test_losing_the_other_site_keeps_the_home_triplet(self):
        owner = _owner()
        assert ops.non_leader_rebuild_candidates(owner, "rs", "site-b") == ["p", "s", "t"]

    def test_empty_slots_are_skipped(self):
        owner = _owner(tertiary_node_id="")
        assert ops.non_leader_rebuild_candidates(owner, "rp") == ["p", "s"]


class TestHublvolSiteRule:

    def test_sync_cluster_only_within_a_site(self):
        cluster = SimpleNamespace(sync_replication=True)
        a1, a2, b1 = (SimpleNamespace(site=s) for s in ("site-a", "site-a", "site-b"))
        assert ops._hublvol_same_site(cluster, a1, a2) is True
        assert ops._hublvol_same_site(cluster, a1, b1) is False

    def test_non_sync_cluster_is_never_restricted(self):
        cluster = SimpleNamespace(sync_replication=False)
        assert ops._hublvol_same_site(cluster, SimpleNamespace(site=""), SimpleNamespace(site="x")) is True


class TestPendingMarks:

    def test_new_members_become_pending(self):
        node = StorageNode()
        ops._set_remote_triplet_refs(node, ("b1", "b2", "b3"))
        assert ops.remote_triplet_refs(node) == ("b1", "b2", "b3")
        assert node.remote_instances_pending == ["b1", "b2", "b3"]

    def test_an_unchanged_member_is_not_marked_again(self):
        node = StorageNode()
        node.remote_primary_node_id, node.remote_secondary_node_id, node.remote_tertiary_node_id = (
            "b1", "b2", "b3")
        ops._set_remote_triplet_refs(node, ("b1", "b4", "b3"))
        assert node.remote_instances_pending == ["b4"]

    def test_a_member_leaving_the_triplet_leaves_the_pending_list(self):
        node = StorageNode()
        ops._set_remote_triplet_refs(node, ("b1", "b2", "b3"))
        node.remote_instances_pending = ["b2"]
        ops._set_remote_triplet_refs(node, ("b1", "b4", "b3"))
        assert node.remote_instances_pending == ["b4"]

    def test_clearing_the_refs_clears_the_marks(self):
        node = StorageNode()
        ops._set_remote_triplet_refs(node, ("b1", "b2", "b3"))
        ops._set_remote_triplet_refs(node, ("", "", ""))
        assert node.remote_instances_pending == []


def _stack():
    return [{"type": "bdev_distr", "name": "d1"}, {"type": "bdev_distr", "name": "d2"},
            {"type": "bdev_raid", "name": "raid0_1"}, {"type": "bdev_lvstore", "name": "LVS_1"}]


class TestRemoteInstanceLeftovers:

    def _peer(self, bdev_get):
        peer = MagicMock()
        peer.get_id.return_value = "b1"
        peer.rpc_client.return_value.bdev_get.side_effect = bdev_get
        return peer

    def test_all_gone(self):
        owner = SimpleNamespace(lvstore_stack=_stack())
        assert ops._remote_instance_leftovers(self._peer(lambda name: None), owner) == []

    def test_a_surviving_bdev_is_reported(self):
        owner = SimpleNamespace(lvstore_stack=_stack())
        peer = self._peer(lambda name: {"name": name} if name == "d2" else None)
        assert ops._remote_instance_leftovers(peer, owner) == ["d2"]

    def test_a_failed_probe_counts_as_present(self):
        def _probe(name):
            if name == "raid0_1":
                raise RPCException("connection error")
        owner = SimpleNamespace(lvstore_stack=_stack())
        assert ops._remote_instance_leftovers(self._peer(_probe), owner) == ["raid0_1"]

    def test_the_lvstore_entry_is_not_probed(self):
        owner = SimpleNamespace(lvstore_stack=_stack())
        peer = self._peer(lambda name: None)
        ops._remote_instance_leftovers(peer, owner)
        probed = [c.args[0] for c in peer.rpc_client.return_value.bdev_get.call_args_list]
        assert probed == ["d1", "d2", "raid0_1"]


class TestDistribNotBuilt:

    def _node(self):
        node = MagicMock()
        node.get_id.return_value = "b1"
        return node

    def test_no_such_device_means_not_built(self):
        rpc = MagicMock()
        rpc.bdev_get.return_value = None
        assert distr_controller._distrib_not_built(rpc, self._node(), "d1") is True

    def test_an_existing_distrib_keeps_the_failure(self):
        rpc = MagicMock()
        rpc.bdev_get.return_value = {"name": "d1"}
        assert distr_controller._distrib_not_built(rpc, self._node(), "d1") is False

    def test_an_unreachable_node_keeps_the_failure(self):
        rpc = MagicMock()
        rpc.bdev_get.side_effect = RPCException("connection error")
        assert distr_controller._distrib_not_built(rpc, self._node(), "d1") is False
