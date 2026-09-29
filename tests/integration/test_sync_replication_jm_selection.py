"""Sync-replication journals against the real FoundationDB: JM copies per
site, their names on every node that runs an LVS instance, the distrib
creation parameters, the remote JM name lookups of the liveness probes and the
JM replacement on node removal. Storage nodes (RPC) are mocked; the database
never is. The pure rules are in tests/unit/test_sync_replication_jm_selection.py.
"""
import uuid
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import health_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import JMDevice, RemoteJMDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import storage_node_monitor


@pytest.fixture()
def db():
    return DBController()


class _Rpcs(dict):
    """One RPC mock per node id, created on first use."""

    def __missing__(self, node_id):
        rpc = MagicMock(name=f"rpc-{node_id}")
        rpc.get_bdevs.side_effect = lambda name=None: [{"name": name}] if name else []
        self[node_id] = rpc
        return rpc


@pytest.fixture()
def rpcs():
    rpcs = _Rpcs()
    with patch.object(StorageNode, "rpc_client", autospec=True,
                      side_effect=lambda self, **kwargs: rpcs[self.get_id()]):
        yield rpcs


def _seed_cluster(db, *, sync=True, fd=False):
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.ha_type = "ha"
    cluster.sync_replication = sync
    cluster.enable_failure_domain = fd
    cluster.max_fault_tolerance = 1
    cluster.distr_ndcs = 1
    cluster.distr_npcs = 1
    cluster.write_to_db(db.kv_store)
    return cluster


def _seed_node(db, cluster, name, site, *, fd=-1, ha_jm_count=6, status=StorageNode.STATUS_ONLINE,
               **fields):
    """A node with an online JM; ``name`` makes ids, hosts and JM names readable."""
    node = StorageNode()
    node.uuid = f"{name}-{uuid.uuid4().hex[:8]}"
    node.cluster_id = cluster.get_id()
    node.site = site
    node.mgmt_ip = f"10.0.{abs(hash(name)) % 250}.{abs(hash(node.uuid)) % 250 + 1}"
    node.failure_domain = fd
    node.status = status
    node.enable_ha_jm = True
    node.ha_jm_count = ha_jm_count
    dev = JMDevice()
    dev.uuid = f"jm-{node.uuid}"
    dev.node_id = node.uuid
    dev.jm_bdev = f"jm_{node.uuid}"
    dev.status = JMDevice.STATUS_ONLINE
    node.jm_device = dev
    for key, value in fields.items():
        setattr(node, key, value)
    node.write_to_db(db.kv_store)
    return node


def _jm(node):
    return node.jm_device.get_id()


def _two_sites(db, cluster, per_site=3, **kwargs):
    a = [_seed_node(db, cluster, f"a{i}", "site-a", **kwargs) for i in range(1, per_site + 1)]
    b = [_seed_node(db, cluster, f"b{i}", "site-b", **kwargs) for i in range(1, per_site + 1)]
    return a, b


# ---------------------------------------------------------------------------
# get_sorted_ha_jms
# ---------------------------------------------------------------------------

class TestJournalCopiesPerSite:

    def test_three_plus_three(self, db):
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        picked = ops.get_sorted_ha_jms(a[0])
        assert set(picked[:2]) == {_jm(a[1]), _jm(a[2])}
        assert set(picked[2:]) == {_jm(n) for n in b}

    def test_four_plus_four_balances_domains_inside_each_site(self, db):
        cluster = _seed_cluster(db, fd=True)
        a = [_seed_node(db, cluster, f"a{i}", "site-a", fd=fd, ha_jm_count=8)
             for i, fd in enumerate([0, 0, 1, 1, 1], start=1)]
        b = [_seed_node(db, cluster, f"b{i}", "site-b", fd=fd, ha_jm_count=8)
             for i, fd in enumerate([0, 0, 0, 1, 1], start=1)]
        picked = ops.get_sorted_ha_jms(a[0])
        fd_of = {_jm(n): (n.site, n.failure_domain) for n in a + b}
        own, other = picked[:3], picked[3:]
        assert len(own) == 3 and len(other) == 4
        # own site: the local JM (fd 0) plus these three split 2-2
        own_fds = [fd_of[j][1] for j in own] + [0]
        assert {fd_of[j][0] for j in own} == {"site-a"}
        assert own_fds.count(0) == 2 and own_fds.count(1) == 2
        other_fds = [fd_of[j][1] for j in other]
        assert {fd_of[j][0] for j in other} == {"site-b"}
        assert other_fds.count(0) == 2 and other_fds.count(1) == 2

    def test_a_site_without_enough_jms_is_a_specific_error(self, db):
        cluster = _seed_cluster(db)
        a = [_seed_node(db, cluster, f"a{i}", "site-a") for i in range(1, 4)]
        [_seed_node(db, cluster, f"b{i}", "site-b") for i in range(1, 3)]
        with pytest.raises(ops.SiteJournalPlacementError, match="site 'site-b' 2"):
            ops.get_sorted_ha_jms(a[0])

    def test_an_offline_owner_with_a_stale_online_jm_is_no_copy(self, db):
        # a forced shutdown takes a node offline without marking its JM
        cluster = _seed_cluster(db)
        a = [_seed_node(db, cluster, f"a{i}", "site-a") for i in range(1, 4)]
        b = [_seed_node(db, cluster, f"b{i}", "site-b") for i in range(1, 3)]
        offline = _seed_node(db, cluster, "b3", "site-b", status=StorageNode.STATUS_OFFLINE)
        with pytest.raises(ops.SiteJournalPlacementError, match="site 'site-b' 2"):
            ops.get_sorted_ha_jms(a[0])
        spare = _seed_node(db, cluster, "b4", "site-b")
        picked = ops.get_sorted_ha_jms(a[0])
        assert set(picked[2:]) == {_jm(b[0]), _jm(b[1]), _jm(spare)}
        assert _jm(offline) not in picked

    def test_non_sync_selection_is_unchanged(self, db):
        cluster = _seed_cluster(db, sync=False)
        nodes = [_seed_node(db, cluster, f"n{i}", "", ha_jm_count=3) for i in range(1, 5)]
        picked = ops.get_sorted_ha_jms(nodes[0])
        assert len(picked) == 2 and _jm(nodes[0]) not in picked


# ---------------------------------------------------------------------------
# get_node_jm_names
# ---------------------------------------------------------------------------

def _owner_with_journal(db, cluster, a, b, **fields):
    owner = a[0]
    owner.jm_ids = ops.get_sorted_ha_jms(owner)
    for key, value in fields.items():
        setattr(owner, key, value)
    owner.write_to_db(db.kv_store)
    return owner


class TestJournalNames:

    def test_home_primary_names_sort_local_first(self, db):
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        owner = _owner_with_journal(db, cluster, a, b)
        jm = ops.get_node_jm_names(owner)
        assert jm.n_local == 3
        assert jm.names[0] == owner.jm_device.jm_bdev
        ordered = sorted(jm.names)
        assert set(ordered[:3]) == {owner.jm_device.jm_bdev} | {f"remote_jm_{n.get_id()}n1" for n in a[1:]}
        assert set(ordered[3:]) == {f"remote_xs_jm_{n.get_id()}n1" for n in b}

    def test_the_list_is_never_cut_to_ha_jm_count(self, db):
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        owner = _owner_with_journal(db, cluster, a, b, ha_jm_count=4)
        assert len(ops.get_node_jm_names(owner).names) == 6

    def test_remote_site_host_holding_a_copy(self, db):
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        owner = _owner_with_journal(db, cluster, a, b)
        host = b[0]
        jm = ops.get_node_jm_names(owner, remote_node=host)
        assert jm.n_local == 3
        ordered = sorted(jm.names)
        assert set(ordered[:3]) == {host.jm_device.jm_bdev} | {f"remote_jm_{n.get_id()}n1" for n in b[1:]}
        assert set(ordered[3:]) == {f"remote_xs_jm_{n.get_id()}n1" for n in a}

    def test_remote_site_host_without_a_copy(self, db):
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        owner = _owner_with_journal(db, cluster, a, b)
        host = _seed_node(db, cluster, "b4", "site-b")
        jm = ops.get_node_jm_names(owner, remote_node=host)
        assert jm.n_local == 3 and len(jm.names) == 6
        ordered = sorted(jm.names)
        assert set(ordered[:3]) == {f"remote_jm_{n.get_id()}n1" for n in b}
        assert all(name.startswith("remote_xs_jm_") for name in ordered[3:])

    def test_non_sync_names_are_unchanged(self, db):
        cluster = _seed_cluster(db, sync=False)
        nodes = [_seed_node(db, cluster, f"n{i}", "", ha_jm_count=3) for i in range(1, 5)]
        owner = nodes[0]
        owner.jm_ids = [_jm(nodes[1]), _jm(nodes[2]), _jm(nodes[3])]
        owner.write_to_db(db.kv_store)
        jm = ops.get_node_jm_names(owner)
        assert jm.n_local is None
        assert jm.names == [owner.jm_device.jm_bdev, f"remote_jm_{nodes[1].get_id()}n1",
                            f"remote_jm_{nodes[2].get_id()}n1"]


# ---------------------------------------------------------------------------
# remote JM connections and name lookups
# ---------------------------------------------------------------------------

class TestRemoteJmNames:

    def test_connections_name_other_site_jms_cross_site(self, db, rpcs):
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        owner = _owner_with_journal(db, cluster, a, b)
        with patch.object(ops, "connect_device", side_effect=lambda name, *a, **k: f"{name}n1") as connect:
            records = ops._connect_to_remote_jm_devs(owner, owner.jm_ids)
        controllers = {call.args[0] for call in connect.call_args_list}
        assert controllers == ({f"remote_jm_{n.get_id()}" for n in a[1:]}
                               | {f"remote_xs_jm_{n.get_id()}" for n in b})
        assert {r.remote_bdev for r in records} == {f"{c}n1" for c in controllers}

    def test_remote_triplet_host_connects_its_owners_journal(self, db, rpcs):
        # b4 holds no copy and has no journal of its own yet: it reaches the
        # owner's JMs only because it is in the owner's remote triplet.
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        host = _seed_node(db, cluster, "b4", "site-b")
        _owner_with_journal(db, cluster, a, b, remote_primary_node_id=host.get_id())
        with patch.object(ops, "connect_device", side_effect=lambda name, *a, **k: f"{name}n1") as connect:
            records = ops._connect_to_remote_jm_devs(host)
        controllers = {call.args[0] for call in connect.call_args_list}
        assert controllers == ({f"remote_xs_jm_{n.get_id()}" for n in a}
                               | {f"remote_jm_{n.get_id()}" for n in b})
        assert {r.remote_bdev for r in records} == {f"{c}n1" for c in controllers}

    def test_quorum_probe_asks_each_peer_for_its_own_name(self, db, rpcs):
        cluster = _seed_cluster(db)
        target = _seed_node(db, cluster, "t", "site-a")
        this = _seed_node(db, cluster, "x", "site-a")
        same = _seed_node(db, cluster, "p1", "site-a", jm_vuid=1)
        cross = _seed_node(db, cluster, "p2", "site-b", jm_vuid=2)
        rpcs[same.get_id()].jc_get_jm_status.return_value = {f"remote_jm_{target.get_id()}n1": False}
        rpcs[cross.get_id()].jc_get_jm_status.return_value = {f"remote_xs_jm_{target.get_id()}n1": True}
        assert ops._peer_reachable_via_jm_quorum(target.get_id(), this) is True
        # the same-site spelling from a cross-site peer is not an answer
        rpcs[cross.get_id()].jc_get_jm_status.return_value = {f"remote_jm_{target.get_id()}n1": True}
        assert ops._peer_reachable_via_jm_quorum(target.get_id(), this) is False

    def test_data_plane_vote_asks_each_peer_for_its_own_name(self, db, rpcs):
        cluster = _seed_cluster(db)
        node = _seed_node(db, cluster, "t", "site-a")
        same = _seed_node(db, cluster, "p1", "site-a", jm_vuid=1)
        cross = _seed_node(db, cluster, "p2", "site-b", jm_vuid=2)
        for peer in (same, cross):
            rpcs[peer.get_id()].bdev_nvme_controller_list.return_value = [{"ctrlrs": [{"state": "enabled"}]}]
        with patch.object(health_controller, "repairs_allowed", return_value=True):
            assert storage_node_monitor._count_data_plane_votes_uncached(node) == (0, 2)
        rpcs[same.get_id()].get_bdevs.assert_called_with(f"remote_jm_{node.get_id()}n1")
        rpcs[same.get_id()].bdev_nvme_controller_list.assert_called_with(f"remote_jm_{node.get_id()}")
        rpcs[cross.get_id()].get_bdevs.assert_called_with(f"remote_xs_jm_{node.get_id()}n1")
        rpcs[cross.get_id()].bdev_nvme_controller_list.assert_called_with(f"remote_xs_jm_{node.get_id()}")

    def test_health_check_uses_the_recorded_cross_site_controller(self, db, rpcs):
        cluster = _seed_cluster(db)
        owner = _seed_node(db, cluster, "a1", "site-a")
        record = RemoteJMDevice()
        record.uuid = _jm(owner)
        record.node_id = owner.get_id()
        record.jm_bdev = owner.jm_device.jm_bdev
        record.remote_bdev = f"remote_xs_{owner.jm_device.jm_bdev}n1"
        node = _seed_node(db, cluster, "b1", "site-b", jm_ids=[_jm(owner)], remote_jm_devices=[record])
        rpc = rpcs[node.get_id()]
        rpc.get_bdevs.side_effect = lambda name=None: [{"name": name, "driver_specific": {"mp_policy": "x"}}]
        rpc.bdev_nvme_controller_list.return_value = [
            {"ctrlrs": [{"trid": {"traddr": "10.0.0.1", "trsvcid": "4420"}}]}]
        with patch.object(health_controller, "_check_node_ping", return_value=True), \
                patch.object(health_controller, "_check_node_api", return_value=True), \
                patch.object(health_controller, "check_node_rpc", return_value=(True, True)), \
                patch.object(health_controller, "check_ports_on_node", return_value={}), \
                patch.object(health_controller, "check_jm_device", return_value=True):
            health_controller.check_node(node.get_id())
        rpc.bdev_nvme_controller_list.assert_any_call(f"remote_xs_{owner.jm_device.jm_bdev}")


# ---------------------------------------------------------------------------
# distrib creation
# ---------------------------------------------------------------------------

def _distr_stack():
    return [{"type": "bdev_distr", "name": "distrib_1",
             "params": {"name": "distrib_1", "vuid": 11, "jm_vuid": 7}}]


class TestDistribCreation:

    def _create(self, rpcs, snode, primary_node=None):
        rpcs[snode.get_id()].get_bdevs.side_effect = lambda name=None: []   # nothing exists yet
        with patch.object(ops.distr_controller, "send_cluster_map_to_distr", return_value=True) as push:
            ok, err = ops._create_bdev_stack(snode, _distr_stack(), primary_node=primary_node)
        return ok, err, push

    def test_home_primary_creates_a_site_aware_sync_distrib(self, db, rpcs):
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        owner = _owner_with_journal(db, cluster, a, b)
        ok, err, push = self._create(rpcs, owner)
        assert ok is True, err
        params = rpcs[owner.get_id()].bdev_distrib_create.call_args.kwargs
        assert params["synchronous_replication_mode"] == 1
        assert params["jm_n_local"] == 3
        assert params["jm_names"] == ops.get_node_jm_names(owner).names
        push.assert_called_once_with(owner, "distrib_1", home_site="site-a")

    def test_remote_site_instance_counts_its_own_site_as_local(self, db, rpcs):
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        owner = _owner_with_journal(db, cluster, a, b)
        host = b[1]
        ok, err, push = self._create(rpcs, host, primary_node=owner)
        assert ok is True, err
        params = rpcs[host.get_id()].bdev_distrib_create.call_args.kwargs
        assert params["jm_n_local"] == 3
        assert host.jm_device.jm_bdev in params["jm_names"]
        push.assert_called_once_with(host, "distrib_1", home_site="site-a")

    def test_a_rejected_map_push_is_retried(self, db, rpcs):
        # the RPC client hands a JSON-RPC error back as None, not as an exception
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        owner = _owner_with_journal(db, cluster, a, b)
        rpc = rpcs[owner.get_id()]
        rpc.get_bdevs.side_effect = lambda name=None: []
        rpc.distr_send_cluster_map.side_effect = [None, True]
        with patch.object(ops.time, "sleep"):
            ok, err = ops._create_bdev_stack(owner, _distr_stack())
        assert ok is True, err
        assert rpc.bdev_distrib_create.call_count == 2
        assert [c.args[0]["name"] for c in rpc.distr_send_cluster_map.call_args_list] == ["distrib_1"] * 2

    def test_a_journal_without_other_site_copies_is_refused(self, db, rpcs):
        cluster = _seed_cluster(db)
        a, b = _two_sites(db, cluster)
        owner = _owner_with_journal(db, cluster, a, b, enable_ha_jm=False)
        ok, err, push = self._create(rpcs, owner)
        assert ok is False and "not site-aware" in err
        rpcs[owner.get_id()].bdev_distrib_create.assert_not_called()
        push.assert_not_called()

    def test_non_sync_distrib_gets_no_sync_parameters(self, db, rpcs):
        cluster = _seed_cluster(db, sync=False)
        nodes = [_seed_node(db, cluster, f"n{i}", "", ha_jm_count=3) for i in range(1, 4)]
        owner = nodes[0]
        owner.jm_ids = [_jm(nodes[1]), _jm(nodes[2])]
        owner.write_to_db(db.kv_store)
        ok, err, _push = self._create(rpcs, owner)
        assert ok is True, err
        params = rpcs[owner.get_id()].bdev_distrib_create.call_args.kwargs
        assert "synchronous_replication_mode" not in params and "jm_n_local" not in params


# ---------------------------------------------------------------------------
# JM replacement on node removal
# ---------------------------------------------------------------------------

def _remote_record(owner, bdev):
    record = RemoteJMDevice()
    record.uuid = _jm(owner)
    record.node_id = owner.get_id()
    record.jm_bdev = owner.jm_device.jm_bdev
    record.remote_bdev = bdev
    return record


class TestReplacementOnRemoval:
    """Owner a1 journals on a2, a3 | b1, b2, b3 and runs on a2, a3 (local
    secondary / tertiary) and b2, b3, b4 (remote triplet). b2's own journal
    also holds b1. b1 is removed: b4 is the only free site-b JM, a4 a free
    site-a one that must never be picked."""

    def _setup(self, db):
        cluster = _seed_cluster(db)
        a = [_seed_node(db, cluster, f"a{i}", "site-a", jm_vuid=100 + i) for i in range(1, 5)]
        b = [_seed_node(db, cluster, f"b{i}", "site-b", jm_vuid=200 + i) for i in range(1, 5)]
        a1, a2, a3, a4 = a
        b1, b2, b3, b4 = b
        a1.jm_ids = [_jm(a2), _jm(a3), _jm(b1), _jm(b2), _jm(b3)]
        a1.remote_primary_node_id, a1.remote_secondary_node_id, a1.remote_tertiary_node_id = (
            b2.get_id(), b3.get_id(), b4.get_id())
        b2.jm_ids = [_jm(b1), _jm(b3), _jm(a2), _jm(a3), _jm(a4)]
        a2.lvstore_stack_secondary = a1.get_id()
        a3.lvstore_stack_tertiary = a1.get_id()
        b1_jm = b1.jm_device.jm_bdev
        for node in (a1, a2, a3):
            node.remote_jm_devices = [_remote_record(b1, f"remote_xs_{b1_jm}n1")]
        for node in (b2, b3, b4):
            node.remote_jm_devices = [_remote_record(b1, f"remote_{b1_jm}n1")]
        b1.status = StorageNode.STATUS_IN_REMOVAL
        for node in a + b:
            node.write_to_db(db.kv_store)
        return a, b

    def _strict_replace(self, rpcs, expected):
        """jc_replace_jm must cover exactly the node's affected jm_vuids (the
        data plane rejects a partial set with -17)."""
        for node_id, vuids in expected.items():
            def _replace(name_old, replacements, vuids=vuids):
                if {r["jm_vuid"] for r in replacements} != vuids:
                    raise RPCException("-17: replacements do not cover every jm_vuid")
                return True
            rpcs[node_id].jc_replace_jm.side_effect = _replace

    def test_replacement_from_the_same_site_on_all_six_instances(self, db, rpcs):
        (a1, a2, a3, a4), (b1, b2, b3, b4) = self._setup(db)
        expected = {n.get_id(): {a1.jm_vuid} for n in (a1, a2, a3, b3, b4)}
        expected[b2.get_id()] = {a1.jm_vuid, b2.jm_vuid}
        self._strict_replace(rpcs, expected)
        with patch.object(ops.device_controller, "remove_jm_device"), \
                patch.object(ops, "connect_device", side_effect=lambda name, *a, **k: f"{name}n1"):
            ops._decommission_node_jm(b1)

        b4_jm = b4.jm_device.jm_bdev
        new_name = {a1.get_id(): f"remote_xs_{b4_jm}n1", a2.get_id(): f"remote_xs_{b4_jm}n1",
                    a3.get_id(): f"remote_xs_{b4_jm}n1", b2.get_id(): f"remote_{b4_jm}n1",
                    b3.get_id(): f"remote_{b4_jm}n1", b4.get_id(): b4_jm}
        old_name = {n: (f"remote_xs_{b1.jm_device.jm_bdev}n1" if n in (a1.get_id(), a2.get_id(), a3.get_id())
                        else f"remote_{b1.jm_device.jm_bdev}n1") for n in new_name}
        for node_id, vuids in expected.items():
            call = rpcs[node_id].jc_replace_jm
            call.assert_called_once()
            assert call.call_args.kwargs["name_old"] == old_name[node_id]
            assert sorted((r["jm_vuid"], r["name_new"]) for r in call.call_args.kwargs["replacements"]) == \
                sorted((v, new_name[node_id]) for v in vuids)
        rpcs[a4.get_id()].jc_replace_jm.assert_not_called()

        stored_a1 = db.get_storage_node_by_id(a1.get_id())
        stored_b2 = db.get_storage_node_by_id(b2.get_id())
        assert _jm(b4) in stored_a1.jm_ids and _jm(b1) not in stored_a1.jm_ids
        assert _jm(b4) in stored_b2.jm_ids and _jm(b1) not in stored_b2.jm_ids
        assert _jm(a4) not in stored_a1.jm_ids

    def test_no_free_jm_on_the_removed_site_leaves_the_slot_short(self, db, rpcs):
        (a1, a2, a3, a4), (b1, b2, b3, b4) = self._setup(db)
        b4.jm_device.status = JMDevice.STATUS_UNAVAILABLE   # site b has no free JM left
        b4.write_to_db(db.kv_store)
        with patch.object(ops.device_controller, "remove_jm_device"), \
                patch.object(ops, "connect_device", side_effect=lambda name, *a, **k: f"{name}n1"):
            ops._decommission_node_jm(b1)
        for node in (a1, a2, a3, b2, b3):
            rpcs[node.get_id()].jc_replace_jm.assert_not_called()
        assert _jm(a4) not in db.get_storage_node_by_id(a1.get_id()).jm_ids
