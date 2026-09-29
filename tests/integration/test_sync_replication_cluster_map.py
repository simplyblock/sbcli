"""Per-LVS cluster maps of a sync-replication cluster against the real
FoundationDB: replica flags by the home site of each distrib, the node
affinity per zone, and the senders that address every distrib by name.
Storage nodes (RPC) are mocked; the database never is. The owner enumeration
is covered as pure logic in tests/unit/test_sync_replication_cluster_map.py.
"""
import uuid
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import distr_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

_order = iter(range(1, 100000))


@pytest.fixture()
def db():
    return DBController()


@pytest.fixture()
def rpcs():
    rpcs: dict = {}

    def _rpc(self, **kwargs):
        return rpcs.setdefault(self.get_id(), MagicMock(name=f"rpc-{self.get_id()}"))

    with patch.object(StorageNode, "rpc_client", autospec=True, side_effect=_rpc):
        yield rpcs


def _seed_cluster(db, *, sync=True, affinity=False):
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.ha_type = "ha"
    cluster.sync_replication = sync
    cluster.enable_node_affinity = affinity
    cluster.write_to_db(db.kv_store)
    return cluster


def _device(node_id):
    dev = NVMeDevice()
    dev.uuid = str(uuid.uuid4())
    dev.node_id = node_id
    dev.status = NVMeDevice.STATUS_ONLINE
    dev.size = 10 * 1024 ** 3
    dev.cluster_device_order = next(_order)
    dev.alceml_bdev = f"alceml_{dev.uuid}"
    return dev


def _seed_node(db, cluster, name, site, *, distribs=(), **fields):
    node = StorageNode()
    node.uuid = f"{name}-{uuid.uuid4().hex[:8]}"
    node.cluster_id = cluster.get_id()
    node.site = site
    node.status = StorageNode.STATUS_ONLINE
    node.nvme_devices = [_device(node.uuid)]
    node.lvstore_stack = [{"type": "bdev_distr", "name": d, "params": {}} for d in distribs]
    for key, value in fields.items():
        setattr(node, key, value)
    node.write_to_db(db.kv_store)
    return node


def _layout(db, cluster, **host_fields):
    """a1 owns LVS d_a (site-a), b1 owns LVS d_b (site-b). ``host`` (site-a)
    runs d_a as a1's secondary and d_b as b1's remote primary."""
    a1 = _seed_node(db, cluster, "a1", "site-a", distribs=["d_a"])
    host = _seed_node(db, cluster, "host", "site-a", lvstore_stack_secondary=a1.get_id(), **host_fields)
    a3 = _seed_node(db, cluster, "a3", "site-a")
    b1 = _seed_node(db, cluster, "b1", "site-b", distribs=["d_b"], remote_primary_node_id=host.get_id())
    b2 = _seed_node(db, cluster, "b2", "site-b")
    b3 = _seed_node(db, cluster, "b3", "site-b")
    return a1, host, a3, b1, b2, b3


def _replica_nodes(cl_map):
    return {node_id for node_id, entry in cl_map["map_cluster"].items() if entry.get("replica")}


def _assert_buckets_agree(cl_map):
    entries = list(cl_map["map_cluster"].values())
    assert len(entries) == len(cl_map["map_prob"])
    for entry, bucket in zip(entries, cl_map["map_prob"]):
        assert entry.get("replica") == bucket.get("replica")
        assert "replica" not in entry or entry["replica"] is True


# ---------------------------------------------------------------------------
# get_distr_cluster_map
# ---------------------------------------------------------------------------

class TestReplicaFlags:

    def test_other_site_nodes_are_the_replica_zone(self, db):
        cluster = _seed_cluster(db)
        nodes = _layout(db, cluster)
        a1, host, a3, b1, b2, b3 = nodes
        cl_map = distr_controller.get_distr_cluster_map(list(nodes), host, "d_a", home_site="site-a")
        assert _replica_nodes(cl_map) == {b1.get_id(), b2.get_id(), b3.get_id()}
        _assert_buckets_agree(cl_map)

    def test_two_lvs_with_opposite_home_sites_on_one_node_get_inverted_flags(self, db):
        cluster = _seed_cluster(db)
        nodes = _layout(db, cluster)
        host = nodes[1]
        map_a = distr_controller.get_distr_cluster_map(list(nodes), host, "d_a", home_site="site-a")
        map_b = distr_controller.get_distr_cluster_map(list(nodes), host, "d_b", home_site="site-b")
        every = set(map_a["map_cluster"])
        assert _replica_nodes(map_b) == every - _replica_nodes(map_a)
        _assert_buckets_agree(map_b)

    def test_a_sync_map_without_a_home_site_is_refused(self, db):
        cluster = _seed_cluster(db)
        nodes = _layout(db, cluster)
        with pytest.raises(distr_controller.DistribHomeSiteError):
            distr_controller.get_distr_cluster_map(list(nodes), nodes[1], "d_a")

    def test_non_sync_map_has_no_zone(self, db):
        cluster = _seed_cluster(db, sync=False)
        nodes = [_seed_node(db, cluster, f"n{i}", "") for i in range(3)]
        cl_map = distr_controller.get_distr_cluster_map(nodes, nodes[0])
        assert _replica_nodes(cl_map) == set()
        assert all("replica" not in bucket for bucket in cl_map["map_prob"])


class TestAffinityPerZone:

    def test_replica_zone_target_is_the_replica_preference(self, db):
        cluster = _seed_cluster(db, affinity=True)
        nodes = _layout(db, cluster)
        host = nodes[1]
        cl_map = distr_controller.get_distr_cluster_map(list(nodes), host, "d_b", home_site="site-b")
        assert "ppln1" not in cl_map
        assert list(cl_map["map_cluster"])[cl_map["replica_ppln1"]] == host.get_id()

    def test_primary_zone_target_is_the_primary_preference(self, db):
        cluster = _seed_cluster(db, affinity=True)
        nodes = _layout(db, cluster)
        host = nodes[1]
        cl_map = distr_controller.get_distr_cluster_map(list(nodes), host, "d_a", home_site="site-a")
        assert "replica_ppln1" not in cl_map
        assert list(cl_map["map_cluster"])[cl_map["ppln1"]] == host.get_id()

    def test_index_counts_emitted_nodes_only(self, db):
        cluster = _seed_cluster(db, affinity=True)
        dedicated = _seed_node(db, cluster, "sec", "site-a", is_secondary_node=True)
        nodes = [dedicated, *_layout(db, cluster)]
        host = nodes[2]
        cl_map = distr_controller.get_distr_cluster_map(nodes, host, "d_a", home_site="site-a")
        assert dedicated.get_id() not in cl_map["map_cluster"]
        assert list(cl_map["map_cluster"])[cl_map["ppln1"]] == host.get_id()

    def test_a_target_outside_the_map_gets_no_preference(self, db):
        cluster = _seed_cluster(db, affinity=True)
        dedicated = _seed_node(db, cluster, "sec", "site-a", is_secondary_node=True)
        nodes = [dedicated, *_layout(db, cluster)]
        cl_map = distr_controller.get_distr_cluster_map(nodes, dedicated, "d_a", home_site="site-a")
        assert "ppln1" not in cl_map and "replica_ppln1" not in cl_map


# ---------------------------------------------------------------------------
# senders
# ---------------------------------------------------------------------------

class TestSendersPerDistrib:

    def test_full_map_goes_to_every_distrib_with_its_own_zones(self, db, rpcs):
        cluster = _seed_cluster(db)
        a1, host, a3, b1, b2, b3 = _layout(db, cluster)
        assert distr_controller.send_cluster_map_to_node(host) is True
        sent = {c.args[0]["name"]: c.args[0] for c in rpcs[host.get_id()].distr_send_cluster_map.call_args_list}
        assert set(sent) == {"d_a", "d_b"}
        assert _replica_nodes(sent["d_a"]) == {b1.get_id(), b2.get_id(), b3.get_id()}
        assert _replica_nodes(sent["d_b"]) == {a1.get_id(), host.get_id(), a3.get_id()}

    def test_a_failed_distrib_does_not_stop_the_others(self, db, rpcs):
        cluster = _seed_cluster(db)
        host = _layout(db, cluster)[1]
        rpc = rpcs.setdefault(host.get_id(), MagicMock())
        rpc.distr_send_cluster_map.side_effect = [RPCException("first fails"), True]
        assert distr_controller.send_cluster_map_to_node(host) is False
        assert rpc.distr_send_cluster_map.call_count == 2

    def test_a_rejected_distrib_map_counts_as_failed(self, db, rpcs):
        # the RPC client hands a JSON-RPC error back as None, not as an exception
        cluster = _seed_cluster(db)
        host = _layout(db, cluster)[1]
        rpc = rpcs.setdefault(host.get_id(), MagicMock())
        rpc.distr_send_cluster_map.side_effect = [None, True]
        assert distr_controller.send_cluster_map_to_node(host) is False
        assert rpc.distr_send_cluster_map.call_count == 2
        rpc.distr_send_cluster_map.side_effect = None
        rpc.distr_send_cluster_map.return_value = None
        assert distr_controller.send_cluster_map_to_distr(host, "d_a") is False

    def test_one_distrib_map_resolves_its_home_site(self, db, rpcs):
        cluster = _seed_cluster(db)
        a1, host, a3, b1, b2, b3 = _layout(db, cluster)
        assert distr_controller.send_cluster_map_to_distr(host, "d_b") is True
        cl_map = rpcs[host.get_id()].distr_send_cluster_map.call_args.args[0]
        assert cl_map["name"] == "d_b"
        assert _replica_nodes(cl_map) == {a1.get_id(), host.get_id(), a3.get_id()}

    def test_added_node_is_sent_per_distrib_with_its_flag(self, db, rpcs):
        cluster = _seed_cluster(db)
        host = _layout(db, cluster)[1]
        new = _seed_node(db, cluster, "b4", "site-b")
        assert distr_controller.send_cluster_map_add_node(new, host) is True
        calls = {c.kwargs["name"]: c.args[0] for c in rpcs[host.get_id()].distr_add_nodes.call_args_list}
        assert set(calls) == {"d_a", "d_b"}
        assert calls["d_a"]["map_cluster"][new.get_id()]["replica"] is True
        assert calls["d_a"]["map_prob"][0]["replica"] is True
        assert "replica" not in calls["d_b"]["map_cluster"][new.get_id()]
        assert "replica" not in calls["d_b"]["map_prob"][0]

    def test_added_node_failure_on_one_distrib(self, db, rpcs):
        cluster = _seed_cluster(db)
        host = _layout(db, cluster)[1]
        new = _seed_node(db, cluster, "b4", "site-b")
        rpc = rpcs.setdefault(host.get_id(), MagicMock())
        rpc.distr_add_nodes.side_effect = [RPCException("first fails"), True]
        assert distr_controller.send_cluster_map_add_node(new, host) is False
        assert rpc.distr_add_nodes.call_count == 2

    def test_a_rejected_add_counts_as_failed(self, db, rpcs):
        cluster = _seed_cluster(db)
        host = _layout(db, cluster)[1]
        new = _seed_node(db, cluster, "b4", "site-b")
        rpc = rpcs.setdefault(host.get_id(), MagicMock())
        rpc.distr_add_nodes.side_effect = [None, True]
        rpc.distr_add_devices.side_effect = [True, None]
        assert distr_controller.send_cluster_map_add_node(new, host) is False
        assert distr_controller.send_cluster_map_add_device(new.nvme_devices[0], host) is False
        assert rpc.distr_add_nodes.call_count == 2 and rpc.distr_add_devices.call_count == 2

    def test_added_device_is_sent_per_distrib(self, db, rpcs):
        cluster = _seed_cluster(db)
        a1, host, a3, b1, b2, b3 = _layout(db, cluster)
        device = b2.nvme_devices[0]
        assert distr_controller.send_cluster_map_add_device(device, host) is True
        calls = rpcs[host.get_id()].distr_add_devices.call_args_list
        assert sorted(c.kwargs["name"] for c in calls) == ["d_a", "d_b"]
        assert calls[0].args[0] == calls[1].args[0]
        assert calls[0].args[0]["UUID_node"] == b2.get_id()


class TestNonSyncSendersUnchanged:

    def test_one_broadcast_without_a_name(self, db, rpcs):
        cluster = _seed_cluster(db, sync=False)
        nodes = [_seed_node(db, cluster, f"n{i}", "", distribs=[f"d_{i}"]) for i in range(3)]
        target, new = nodes[0], nodes[1]
        assert distr_controller.send_cluster_map_to_node(target) is True
        rpc = rpcs[target.get_id()]
        rpc.distr_send_cluster_map.assert_called_once()
        assert rpc.distr_send_cluster_map.call_args.args[0]["name"] == ""
        assert distr_controller.send_cluster_map_add_node(new, target) is True
        rpc.distr_add_nodes.assert_called_once()
        assert rpc.distr_add_nodes.call_args.kwargs == {}
        assert distr_controller.send_cluster_map_add_device(new.nvme_devices[0], target) is True
        rpc.distr_add_devices.assert_called_once()
        assert rpc.distr_add_devices.call_args.kwargs == {}
