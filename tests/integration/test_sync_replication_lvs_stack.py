"""The LVS stack on the remote triplet of a sync-replication cluster, against
the real FoundationDB: creation of the six (FTT1: five) instances, the
non-leader rebuild of a remote-triplet member and of a lost site's nodes on
restart, the hublvol site rule, the senders facing a distrib that is not built
yet, and node removal (phase 3a teardown, phase 3d build). Storage nodes (RPC)
and the hublvol / port-block side effects are mocked; the database never is.
The pure rules are in tests/unit/test_sync_replication_lvs_stack.py.
"""
import logging
import threading
import time
import uuid
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import distr_controller
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

SITE_A = "site-a"
SITE_B = "site-b"
_order = iter(range(1, 1000000))


@pytest.fixture()
def db():
    return DBController()


class _Rpcs(dict):
    """One RPC mock per node id: nothing built yet, no leadership anywhere."""

    def __missing__(self, node_id):
        rpc = MagicMock(name=f"rpc-{node_id}")
        rpc.get_bdevs.side_effect = lambda name=None, **kwargs: []
        rpc.bdev_get.return_value = None
        rpc.jc_suspend_compression.return_value = (True, None)
        rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self[node_id] = rpc
        return rpc

    def lead(self, node):
        self[node.get_id()].bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]


@pytest.fixture()
def rpcs():
    rpcs = _Rpcs()
    with patch.object(StorageNode, "rpc_client", autospec=True,
                      side_effect=lambda self, *args, **kwargs: rpcs[self.get_id()]):
        yield rpcs


@pytest.fixture()
def hub():
    """The hublvol side effects of every node, recorded (self is the node)."""
    names = ("connect_to_hublvol", "create_hublvol", "create_transfer_hublvol",
             "create_secondary_hublvol", "add_hublvol_failover_path", "prestage_hublvol_subsystem",
             "recreate_hublvol")
    with ExitStack() as stack:
        yield {name: stack.enter_context(patch.object(StorageNode, name, autospec=True, return_value=True))
               for name in names}


class _Env:
    def __init__(self):
        self.down: set = set()
        self.set_port = MagicMock(name="set_port")
        self.set_node_status = MagicMock(name="set_node_status")

    def blocked(self):
        """Node ids port-blocked, in order."""
        return [c.args[0].get_id() for c in self.set_port.call_args_list if c.kwargs.get("block")]

    def unblocked(self):
        return [c.args[0].get_id() for c in self.set_port.call_args_list if not c.kwargs.get("block")]


@pytest.fixture()
def env():
    e = _Env()
    with patch.object(ops, "_connect_to_remote_devs", return_value=[]), \
            patch.object(ops, "_connect_to_remote_jm_devs", return_value=[]), \
            patch.object(ops.distr_controller, "send_cluster_map_to_distr", return_value=True), \
            patch.object(ops, "_check_peer_disconnected",
                         side_effect=lambda node, lvs_peer_ids=None: node.get_id() in e.down), \
            patch.object(ops.port_block, "set_port", e.set_port), \
            patch.object(ops, "tcp_ports_events"), \
            patch.object(ops, "storage_events"), \
            patch.object(ops, "set_node_status", e.set_node_status), \
            patch.object(ops.constants, "NON_LEADER_BLOCK_QUIESCE_SEC", 0), \
            patch.object(StorageNode, "client", autospec=True), \
            patch("simplyblock_core.utils.hublvol_reconnect.HublvolReconnectCoordinator"):
        yield e


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------

def _seed_cluster(db, *, ftt=2, lost_site=""):
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.nqn = f"nqn.2023-02.io.simplyblock:{cluster.uuid}"
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.ha_type = "ha"
    cluster.sync_replication = True
    cluster.max_fault_tolerance = ftt
    cluster.distr_ndcs = 1
    cluster.distr_npcs = 1
    cluster.lost_site = lost_site
    cluster.write_to_db(db.kv_store)
    return cluster


def _seed_node(db, cluster, name, site, *, status=StorageNode.STATUS_ONLINE, **fields):
    node = StorageNode()
    node.uuid = f"{name}-{uuid.uuid4().hex[:8]}"
    node.cluster_id = cluster.get_id()
    node.site = site
    node.mgmt_ip = f"10.{3 if site == SITE_A else 4}.{next(_order) % 250}.{next(_order) % 250 + 1}"
    node.status = status
    node.enable_ha_jm = True
    node.ha_jm_count = 6
    node.number_of_distribs = 2
    jm = JMDevice()
    jm.uuid = f"jm-{node.uuid}"
    jm.node_id = node.uuid
    jm.jm_bdev = f"jm_{node.uuid}"
    jm.status = JMDevice.STATUS_ONLINE
    node.jm_device = jm
    dev = NVMeDevice()
    dev.uuid = str(uuid.uuid4())
    dev.node_id = node.uuid
    dev.status = NVMeDevice.STATUS_ONLINE
    dev.size = 10 * 1024 ** 3
    dev.cluster_device_order = next(_order)
    dev.alceml_bdev = f"alceml_{dev.uuid}"
    node.nvme_devices = [dev]
    for key, value in fields.items():
        setattr(node, key, value)
    node.write_to_db(db.kv_store)
    return node


def _update(db, node, **fields):
    fresh = db.get_storage_node_by_id(node.get_id())
    for key, value in fields.items():
        setattr(fresh, key, value)
    fresh.write_to_db(db.kv_store)
    return fresh


def _stack(lvs_id):
    return [
        {"type": "bdev_distr", "name": f"distrib_{lvs_id}1",
         "params": {"name": f"distrib_{lvs_id}1", "jm_vuid": lvs_id, "vuid": lvs_id * 10 + 1}},
        {"type": "bdev_distr", "name": f"distrib_{lvs_id}2",
         "params": {"name": f"distrib_{lvs_id}2", "jm_vuid": lvs_id, "vuid": lvs_id * 10 + 2}},
        {"type": "bdev_raid", "name": f"raid0_{lvs_id}",
         "params": {"name": f"raid0_{lvs_id}", "strip_size_kb": 64},
         "distribs_list": [f"distrib_{lvs_id}1", f"distrib_{lvs_id}2"]},
        {"type": "bdev_lvstore", "name": f"LVS_{lvs_id}", "params": {"name": f"LVS_{lvs_id}"}},
    ]


def _make_owner(db, owner, home, remote, *, lvs_id, ftt=2, built=True):
    """``owner`` (in ``home``) owns an LVS: local roles on its site, the
    remote triplet ``remote`` on the other one, a site-aware journal. With
    ``built`` the LVS exists (stack, ports, lvstore_status ready)."""
    local = [n for n in home if n.get_id() != owner.get_id()]
    fields = {
        "secondary_node_id": local[0].get_id(),
        "tertiary_node_id": local[1].get_id() if ftt >= 2 else "",
        "remote_primary_node_id": remote[0].get_id(),
        "remote_secondary_node_id": remote[1].get_id(),
        "remote_tertiary_node_id": remote[2].get_id(),
        "jm_ids": [n.jm_device.get_id() for n in (local[0], local[1], *remote)],
    }
    if built:
        port = 4400 + lvs_id * 2
        fields.update(lvstore=f"LVS_{lvs_id}", raid=f"raid0_{lvs_id}", jm_vuid=lvs_id,
                      lvstore_stack=_stack(lvs_id), lvstore_status="ready", lvol_subsys_port=port,
                      lvstore_ports={f"LVS_{lvs_id}": {"lvol_subsys_port": port, "hublvol_port": port + 1}},
                      hublvol=HubLVol({"nvmf_port": port + 1, "uuid": f"hub-{lvs_id}",
                                       "nqn": f"nqn.hub.{lvs_id}", "bdev_name": f"LVS_{lvs_id}/hublvol",
                                       "model_number": "m", "nguid": "0" * 32}))
    owner = _update(db, owner, **fields)
    _update(db, local[0], lvstore_stack_secondary=owner.get_id())
    if ftt >= 2:
        _update(db, local[1], lvstore_stack_tertiary=owner.get_id())
    return owner


def _sites(db, cluster, per_site=3, **kwargs):
    a = [_seed_node(db, cluster, f"a{i}", SITE_A, **kwargs) for i in range(per_site)]
    b = [_seed_node(db, cluster, f"b{i}", SITE_B, **kwargs) for i in range(per_site)]
    return a, b


def _fresh(db, node):
    return db.get_storage_node_by_id(node.get_id())


def _roles(rpc):
    return [c.kwargs["role"] for c in rpc.bdev_lvol_set_lvs_opts.call_args_list]


def _hub_callers(hub, name):
    return [c.args[0].get_id() for c in hub[name].call_args_list]


def _took_leadership(rpc):
    return any(c.kwargs.get("leader") for c in rpc.bdev_lvol_set_leader.call_args_list)


# ---------------------------------------------------------------------------
# creation
# ---------------------------------------------------------------------------

def _create(db, rpcs, owner):
    owner = _fresh(db, owner)
    with patch.object(ops, "get_sorted_ha_jms", return_value=list(owner.jm_ids)):
        return ops.create_lvstore(owner, 1, 1, 4096, 4096, 2, 10 ** 12)


class TestCreation:

    @pytest.mark.parametrize("ftt, instances", [(2, 6), (1, 5)])
    def test_every_instance_is_built_with_its_role_and_journal_view(
            self, db, rpcs, hub, env, ftt, instances):
        cluster = _seed_cluster(db, ftt=ftt)
        a, b = _sites(db, cluster)
        owner = _make_owner(db, a[0], a, b, lvs_id=1, ftt=ftt, built=False)
        owner = _update(db, owner, remote_instances_pending=[n.get_id() for n in b])

        assert _create(db, rpcs, owner) is True

        owner = _fresh(db, owner)
        builders = {nid for nid, rpc in rpcs.items() if rpc.bdev_distrib_create.called}
        expected = {owner.get_id(), owner.secondary_node_id, *[n.get_id() for n in b]}
        if ftt >= 2:
            expected.add(owner.tertiary_node_id)
        assert builders == expected and len(builders) == instances
        for node_id in builders:
            for c in rpcs[node_id].bdev_distrib_create.call_args_list:
                params = c.kwargs
                assert params["jm_vuid"] == owner.jm_vuid
                assert params["synchronous_replication_mode"] == 1
                site = SITE_A if node_id.startswith("a") else SITE_B
                own_site = [x for x in (*a, *b) if x.site == site]
                local_names = {x.jm_device.jm_bdev for x in own_site} | {
                    f"remote_{x.jm_device.jm_bdev}n1" for x in own_site}
                n_local = params["jm_n_local"]
                assert n_local == 3
                assert set(sorted(params["jm_names"])[:n_local]) <= local_names
        assert _took_leadership(rpcs[owner.get_id()])
        for node_id in builders - {owner.get_id()}:
            assert not _took_leadership(rpcs[node_id])
        assert _roles(rpcs[b[0].get_id()]) == ["secondary"]
        assert _roles(rpcs[b[1].get_id()]) == ["secondary"]
        assert _roles(rpcs[b[2].get_id()]) == ["tertiary"]
        for member in b:
            rpcs[member.get_id()].bdev_examine.assert_called_once_with(owner.raid)
            rpcs[member.get_id()].subsystem_create.assert_not_called()
        remote_ids = {n.get_id() for n in b}
        for name in ("connect_to_hublvol", "create_secondary_hublvol", "add_hublvol_failover_path"):
            assert not remote_ids & set(_hub_callers(hub, name)), name
        local_roles = {c.args[0].get_id(): c.kwargs["role"] for c in hub["connect_to_hublvol"].call_args_list}
        assert local_roles[owner.secondary_node_id] == "secondary"
        if ftt >= 2:
            assert local_roles[owner.tertiary_node_id] == "tertiary"
        assert owner.remote_instances_pending == []
        assert all(owner.lvstore in _fresh(db, n).lvstore_ports for n in b)

    def test_an_activation_mode_local_member_left_as_primary_fails(self, db, rpcs, hub, env):
        # Its hublvol connect runs before the examine: the post-examine stamp
        # is the only one that sticks.
        _, a, b, owner_a = _one_owner_layout(db)
        rpcs[a[1].get_id()].bdev_lvol_set_lvs_opts.return_value = False
        assert ops.recreate_lvstore_on_non_leader(
            _fresh(db, a[1]), _fresh(db, owner_a), _fresh(db, owner_a), activation_mode=True) is False
        assert _fresh(db, a[1]).restart_phases.get(owner_a.lvstore, "") == ""
        rpcs[a[1].get_id()].bdev_lvol_set_lvs_opts.return_value = True
        assert ops.recreate_lvstore_on_non_leader(
            _fresh(db, a[1]), _fresh(db, owner_a), _fresh(db, owner_a), activation_mode=True) is True

    def test_an_offline_member_is_left_pending(self, db, rpcs, hub, env):
        cluster = _seed_cluster(db)
        a, b = _sites(db, cluster)
        _update(db, b[2], status=StorageNode.STATUS_OFFLINE)
        owner = _make_owner(db, a[0], a, b, lvs_id=1, built=False)
        owner = _update(db, owner, remote_instances_pending=[n.get_id() for n in b])
        assert _create(db, rpcs, owner) is True
        assert not rpcs[b[2].get_id()].method_calls
        assert _fresh(db, owner).remote_instances_pending == [b[2].get_id()]

    @pytest.mark.parametrize("failure", [False, RPCException("stamp failed")])
    def test_a_remote_member_left_as_primary_fails_the_create(self, db, rpcs, hub, env, failure):
        cluster = _seed_cluster(db)
        a, b = _sites(db, cluster)
        owner = _make_owner(db, a[0], a, b, lvs_id=1, built=False)
        owner = _update(db, owner, remote_instances_pending=[n.get_id() for n in b])
        stamp = rpcs[b[1].get_id()].bdev_lvol_set_lvs_opts
        if isinstance(failure, Exception):
            stamp.side_effect = failure
        else:
            stamp.return_value = failure
        assert _create(db, rpcs, owner) is False
        assert b[1].get_id() in _fresh(db, owner).remote_instances_pending
        assert _fresh(db, b[1]).restart_phases.get(_fresh(db, owner).lvstore, "") == ""


# ---------------------------------------------------------------------------
# restart
# ---------------------------------------------------------------------------

def _one_owner_layout(db, *, lost_site=""):
    """a0 owns LVS_1, its remote triplet b0 b1 b2 hosts nothing else."""
    cluster = _seed_cluster(db, lost_site=lost_site)
    a, b = _sites(db, cluster)
    return cluster, a, b, _make_owner(db, a[0], a, b, lvs_id=1)


def _two_owner_layout(db, *, ftt=2, lost_site=""):
    """a0 owns LVS_1 (remote triplet b0 b1 b2), b0 owns LVS_2 (remote a0 a1 a2).
    With ``lost_site`` A, LVS_1 has been moved to its remote triplet (the
    state after a disaster promote: led from site B)."""
    cluster = _seed_cluster(db, ftt=ftt, lost_site=lost_site)
    a, b = _sites(db, cluster)
    owner_a = _make_owner(db, a[0], a, b, lvs_id=1, ftt=ftt)
    if lost_site == SITE_A:
        owner_a = _update(db, owner_a, lvs_active_site=SITE_B)
    owner_b = _make_owner(db, b[0], b, a, lvs_id=2, ftt=ftt)
    return cluster, a, b, owner_a, owner_b


def _restart(db, node):
    """Restart dispatch of ``node``; its own LVS (Step 1) is mocked."""
    with patch.object(ops, "recreate_lvstore", return_value=True) as own:
        ret = ops.recreate_all_lvstores(_fresh(db, node))
    assert all("lvs_primary" not in c.kwargs for c in own.call_args_list), "took over an LVS"
    return ret


class TestRemoteMemberRestart:

    def test_rebuilt_as_non_leader_behind_a_quiesced_home_leader(self, db, rpcs, hub, env):
        _, a, b, owner_a = _one_owner_layout(db)
        rpcs.lead(owner_a)
        member = b[1]
        assert _restart(db, member) is True
        rpc = rpcs[member.get_id()]
        assert {c.kwargs["name"] for c in rpc.bdev_distrib_create.call_args_list} == {
            "distrib_11", "distrib_12"}
        rpc.bdev_examine.assert_called_once_with("raid0_1")
        assert not _took_leadership(rpc)
        assert _roles(rpc) == ["secondary"]
        rpc.subsystem_create.assert_not_called()
        assert env.blocked() == [owner_a.get_id()]
        assert owner_a.get_id() in env.unblocked()
        for name, mock in hub.items():
            assert not mock.called, name
        assert _fresh(db, owner_a).lvstore_status == "ready"

    def test_the_leading_secondary_is_quiesced_not_the_idle_owner(self, db, rpcs, hub, env):
        _, a, b, owner_a = _one_owner_layout(db)
        rpcs.lead(a[1])
        assert _restart(db, b[2]) is True
        assert env.blocked() == [a[1].get_id()]

    def test_no_leader_among_reachable_members_builds_nothing(self, db, rpcs, hub, env):
        _, a, b, owner_a = _one_owner_layout(db)
        assert _restart(db, b[2]) is False
        rpcs[b[2].get_id()].bdev_distrib_create.assert_not_called()
        assert env.blocked() == []
        assert _fresh(db, owner_a).lvstore_status == "ready"

    def test_home_triplet_all_down_rebuilds_without_quiesce_and_never_takes_over(
            self, db, rpcs, hub, env):
        _, a, b, owner_a = _one_owner_layout(db)
        env.down.update(n.get_id() for n in a)
        assert _restart(db, b[2]) is True
        rpc = rpcs[b[2].get_id()]
        assert {c.kwargs["name"] for c in rpc.bdev_distrib_create.call_args_list} == {
            "distrib_11", "distrib_12"}
        assert not _took_leadership(rpc)
        assert _roles(rpc) == ["tertiary"]
        assert env.blocked() == []

    def test_a_failed_stamp_fails_the_restart_and_keeps_the_owner_healthy(self, db, rpcs, hub, env):
        _, a, b, owner_a = _one_owner_layout(db)
        rpcs.lead(owner_a)
        _update(db, owner_a, remote_instances_pending=[b[0].get_id()])
        rpcs[b[0].get_id()].bdev_lvol_set_lvs_opts.return_value = False
        assert _restart(db, b[0]) is False
        assert owner_a.get_id() in env.blocked() and owner_a.get_id() in env.unblocked()
        env.set_node_status.assert_any_call(b[0].get_id(), StorageNode.STATUS_OFFLINE,
                                            caused_by="restart_cleanup")
        assert _fresh(db, owner_a).lvstore_status == "ready"
        assert _fresh(db, owner_a).remote_instances_pending == [b[0].get_id()]

    def test_a_successful_rebuild_clears_the_pending_mark(self, db, rpcs, hub, env):
        _, a, b, owner_a = _one_owner_layout(db)
        rpcs.lead(owner_a)
        _update(db, owner_a, remote_instances_pending=[b[1].get_id(), b[2].get_id()])
        assert _restart(db, b[1]) is True
        assert _fresh(db, owner_a).remote_instances_pending == [b[2].get_id()]

    def test_two_members_rebuilding_at_once_restore_the_owner_status(self, db, rpcs, env):
        _, a, b, owner_a = _one_owner_layout(db)
        seen = []

        def _impl(member, leader, owner, activation_mode=False, force=False):
            seen.append(_fresh(db, owner).lvstore_status)
            time.sleep(0.2)
            return True

        with patch.object(ops, "_recreate_lvstore_on_non_leader_impl", side_effect=_impl), \
                patch.object(ops, "_non_leader_rebuild_leader", return_value=owner_a):
            threads = [threading.Thread(target=ops._rebuild_remote_instance, args=(_fresh(db, m), owner_a))
                       for m in (b[1], b[2])]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        assert seen == ["in_creation", "in_creation"]
        assert _fresh(db, owner_a).lvstore_status == "ready"

    @pytest.mark.parametrize("slot", ["secondary", "tertiary"])
    def test_a_local_restart_never_writes_the_owner_record_whole(self, db, rpcs, env, caplog, slot):
        # A stale full-object write of the owner would bring back a pending
        # mark a concurrent remote-triplet build just cleared on it.
        _, a, b, owner_a = _one_owner_layout(db)
        rpcs.lead(owner_a)
        host = a[1] if slot == "secondary" else a[2]
        caplog.set_level(logging.INFO)
        with patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True) as nl:
            assert _restart(db, host) is True
        assert [c.args[2].get_id() for c in nl.call_args_list] == [owner_a.get_id()]
        assert _fresh(db, owner_a).lvstore_status == "in_creation"
        stale = [r.getMessage() for r in caplog.records
                 if f"full-object write of {owner_a.get_id()}" in r.getMessage()
                 and "_recreate_all_lvstores_serial" in r.getMessage()]
        assert stale == []

    def test_recreate_on_sec_covers_remote_instances(self, db, rpcs, env):
        _, a, b, owner_a, owner_b = _two_owner_layout(db)
        rpcs.lead(owner_a)
        rpcs.lead(owner_b)
        with patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True) as nl:
            assert ops.recreate_lvstore_on_sec(_fresh(db, b[1])) is True
        pairs = [(c.args[0].get_id(), c.kwargs["leader_node"].get_id(), c.kwargs["primary_node"].get_id())
                 for c in nl.call_args_list]
        assert (b[1].get_id(), owner_a.get_id(), owner_a.get_id()) in pairs
        assert (b[1].get_id(), owner_b.get_id(), owner_b.get_id()) in pairs


class TestSameSiteRemoteLeader:

    def test_a_remote_leader_on_the_members_site_wires_hublvol_within_the_site(self, db, rpcs, hub, env):
        # LVS_1's home site A is lost: its remote triplet on B leads.
        _, a, b, owner_a, _ = _two_owner_layout(db, lost_site=SITE_A)
        env.down.update(n.get_id() for n in a)
        rpcs.lead(b[0])
        member = _fresh(db, b[1])
        leader = ops._non_leader_rebuild_leader(_fresh(db, owner_a), member, db)
        assert leader.get_id() == b[0].get_id()
        assert ops.recreate_lvstore_on_non_leader(member, leader, _fresh(db, owner_a)) is True
        site_b = {n.get_id() for n in b}
        # Every path stays on site B and carries LVS_1's metadata, never the
        # leader's own LVS (b0 owns LVS_2).
        attach = [c for c in hub["connect_to_hublvol"].call_args_list if c.args[0].get_id() == member.get_id()]
        assert attach
        for c in attach:
            assert c.args[1].get_id() == b[0].get_id() and c.kwargs["lvs_node"].lvstore == "LVS_1"
            assert c.kwargs["role"] == "secondary"
        (secondary_hub,) = hub["create_secondary_hublvol"].call_args_list
        assert secondary_hub.args[0].get_id() == member.get_id() and secondary_hub.args[1].lvstore == "LVS_1"
        failover = hub["add_hublvol_failover_path"].call_args_list
        assert failover
        for c in failover:
            assert {c.args[0].get_id(), c.args[1].get_id(), c.args[2].get_id()} <= site_b
            assert c.kwargs["lvs_node"].lvstore == "LVS_1"
        assert env.blocked()[0] == b[0].get_id()

    def test_a_failover_path_takes_its_metadata_from_the_lvs_node(self, db):
        cluster = _seed_cluster(db)
        tert, leader, peer, owner = (_seed_node(db, cluster, name, SITE_B) for name in ("t", "l", "p", "o"))
        with patch("simplyblock_core.utils.hublvol_reconnect.HublvolReconnectCoordinator") as coordinator:
            tert.add_hublvol_failover_path(leader, peer, lvs_node=owner)
            tert.add_hublvol_failover_path(leader, peer)
        calls = coordinator.return_value.reconcile.call_args_list
        assert calls[0].args == (tert, owner, [leader, peer])
        assert calls[1].args == (tert, leader, [leader, peer])


# ---------------------------------------------------------------------------
# lost site
# ---------------------------------------------------------------------------

class TestLostSiteRestart:

    def test_every_lvs_comes_back_non_leader(self, db, rpcs, env):
        _, a, b, owner_a, owner_b = _two_owner_layout(db, lost_site=SITE_A)
        # a1 owns LVS_3 on the lost site and is a0's secondary.
        owner_a1 = _make_owner(db, a[1], [a[1], a[2], a[0]], b, lvs_id=3)
        owner_a1 = _update(db, owner_a1, lvs_active_site=SITE_B)
        rpcs.lead(b[0])       # b0 leads LVS_1 (a0's) and LVS_3 (a1's) from site B
        with patch.object(ops, "recreate_lvstore") as takeover, \
                patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True) as nl, \
                patch.object(ops, "_rebuild_remote_instance", return_value=True) as remote:
            assert ops.recreate_all_lvstores(_fresh(db, owner_a1)) is True
        takeover.assert_not_called()
        calls = {c.args[2].get_id(): c.args[1].get_id() for c in nl.call_args_list}
        assert calls == {owner_a1.get_id(): b[0].get_id(), owner_a.get_id(): b[0].get_id()}
        assert [c.args[1].get_id() for c in remote.call_args_list] == [owner_b.get_id()]

    @pytest.mark.parametrize("stamp", [True, False])
    def test_a_local_secondary_is_stamped_non_leader_behind_the_remote_leader(
            self, db, rpcs, hub, env, stamp):
        # Real rebuild: no hublvol exists to stamp the role again, so the one
        # stamp must hold or the rebuild aborts.
        _, a, b, owner_a, _ = _two_owner_layout(db, lost_site=SITE_A)
        env.down.update(n.get_id() for n in a if n.get_id() != a[1].get_id())
        rpcs.lead(b[0])
        rpcs[a[1].get_id()].bdev_lvol_set_lvs_opts.return_value = stamp
        with patch.object(ops, "recreate_lvstore") as takeover, \
                patch.object(ops, "_rebuild_remote_instance", return_value=True):
            ret = ops.recreate_all_lvstores(_fresh(db, a[1]))
        takeover.assert_not_called()
        rpc = rpcs[a[1].get_id()]
        assert {c.kwargs["name"] for c in rpc.bdev_distrib_create.call_args_list} == {
            "distrib_11", "distrib_12"}
        assert _roles(rpc) == ["secondary"]
        assert not _took_leadership(rpc)
        for name, mock in hub.items():
            assert not mock.called, name
        assert env.blocked() == [b[0].get_id()] and b[0].get_id() in env.unblocked()
        assert ret is stamp
        if not stamp:
            env.set_node_status.assert_any_call(a[1].get_id(), StorageNode.STATUS_OFFLINE,
                                                caused_by="restart_cleanup")

    def test_a_raw_failure_reopens_the_surviving_leader(self, db, rpcs, env):
        _, a, b, owner_a, _ = _two_owner_layout(db, lost_site=SITE_A)
        rpcs.lead(b[0])
        with patch.object(ops, "recreate_lvstore_on_non_leader", side_effect=RPCException("spdk gone")):
            with pytest.raises(RPCException):
                ops.recreate_all_lvstores(_fresh(db, owner_a))
        assert b[0].get_id() in env.unblocked()


# ---------------------------------------------------------------------------
# lvstore ports, activation owner set
# ---------------------------------------------------------------------------

class TestHostedOwners:

    def test_ports_keep_every_owner_hosted_on_a_multi_owner_member(self, db, rpcs):
        cluster = _seed_cluster(db)
        a, b = _sites(db, cluster, per_site=4)
        owner_1 = _make_owner(db, a[0], a[:3], b[:3], lvs_id=1)
        owner_2 = _make_owner(db, a[3], [a[3], a[1], a[2]], [b[1], b[0], b[3]], lvs_id=2)
        member = _fresh(db, b[1])
        ports = ops._derive_lvstore_ports(member, owner_1, db)
        assert {owner_1.lvstore, owner_2.lvstore} <= set(ports)

    def test_hosted_owners_list_local_then_remote(self, db, rpcs):
        _, a, b, owner_a, owner_b = _two_owner_layout(db)
        hosted = ops.hosted_lvs_owners(_fresh(db, b[1]), db)
        assert [o.get_id() for o in hosted] == [owner_b.get_id(), owner_a.get_id()]
        assert [o.get_id() for o in ops.remote_instance_owners(_fresh(db, b[1]), db)] == [owner_a.get_id()]

    def test_activation_pass_2_rebuild_clears_the_pending_mark(self, db, rpcs, hub, env):
        _, a, b, owner_a, _ = _two_owner_layout(db)
        _update(db, owner_a, remote_instances_pending=[b[0].get_id()])
        assert ops.recreate_lvstore_on_non_leader(
            _fresh(db, b[0]), _fresh(db, owner_a), _fresh(db, owner_a), activation_mode=True) is True
        assert _fresh(db, owner_a).remote_instances_pending == []
        assert _fresh(db, owner_a).lvstore_status == "ready"

    def test_assignment_marks_new_members_pending(self, db):
        cluster = _seed_cluster(db)
        a, b = _sites(db, cluster)
        for node in a:
            _update(db, node, lvstore=f"LVS_{node.get_id()[:4]}")
        _update(db, a[0], secondary_node_id=a[1].get_id(), tertiary_node_id=a[2].get_id())
        written = ops.assign_remote_triplets(cluster, [a[0].get_id()], db_controller=db)
        owner = _fresh(db, a[0])
        assert owner.remote_instances_pending == list(written[a[0].get_id()])
        assert ops.assign_remote_triplets(cluster, [a[0].get_id()], db_controller=db) == {}
        assert _fresh(db, owner).remote_instances_pending == list(written[a[0].get_id()])


# ---------------------------------------------------------------------------
# senders
# ---------------------------------------------------------------------------

class TestSendersBeforeTheBuild:

    def _layout(self, db):
        cluster = _seed_cluster(db)
        a, b = _sites(db, cluster)
        owner = _make_owner(db, a[0], a, b, lvs_id=1)
        return owner, _fresh(db, b[0])

    def test_a_distrib_not_built_yet_is_skipped(self, db, rpcs):
        owner, member = self._layout(db)
        rpc = rpcs[member.get_id()]
        rpc.distr_send_cluster_map.return_value = None
        rpc.distr_add_nodes.return_value = None
        rpc.distr_add_devices.return_value = None
        assert distr_controller.send_cluster_map_to_node(member) is True
        assert distr_controller.send_cluster_map_add_node(_fresh(db, owner), member) is True
        assert distr_controller.send_cluster_map_add_device(owner.nvme_devices[0], member) is True
        assert {c.args[0] for c in rpc.bdev_get.call_args_list} == {"distrib_11", "distrib_12"}

    def test_once_built_the_distrib_is_pushed(self, db, rpcs):
        owner, member = self._layout(db)
        rpc = rpcs[member.get_id()]
        rpc.distr_send_cluster_map.return_value = True
        assert distr_controller.send_cluster_map_to_node(member) is True
        assert sorted(c.args[0]["name"] for c in rpc.distr_send_cluster_map.call_args_list) == [
            "distrib_11", "distrib_12"]
        rpc.bdev_get.assert_not_called()

    @pytest.mark.parametrize("probe", [{"name": "distrib_11"}, RPCException("unreachable")])
    def test_a_built_or_unknown_distrib_keeps_the_failure(self, db, rpcs, probe):
        owner, member = self._layout(db)
        rpc = rpcs[member.get_id()]
        rpc.distr_send_cluster_map.return_value = None
        if isinstance(probe, Exception):
            rpc.bdev_get.side_effect = probe
        else:
            rpc.bdev_get.return_value = probe
        assert distr_controller.send_cluster_map_to_node(member) is False


# ---------------------------------------------------------------------------
# node removal
# ---------------------------------------------------------------------------

class TestRemovalTeardown:

    def test_remote_instances_are_torn_down_and_the_refs_cleared(self, db, rpcs):
        _, a, b, owner_a, _ = _two_owner_layout(db)
        owner_a = _update(db, owner_a, status=StorageNode.STATUS_IN_REMOVAL,
                          remote_instances_pending=[b[2].get_id()])
        for member in b:
            _update(db, member, lvstore_ports={owner_a.lvstore: {"lvol_subsys_port": 1, "hublvol_port": 2}})
        assert ops._teardown_replicas_of_primary(owner_a) is True
        owner = _fresh(db, owner_a)
        assert ops.remote_triplet_refs(owner) == ("", "", "")
        assert owner.remote_instances_pending == []
        assert owner.secondary_node_id == "" and owner.tertiary_node_id == ""
        for member in b:
            rpc = rpcs[member.get_id()]
            assert {c.args[0] for c in rpc.bdev_distrib_delete.call_args_list} == {"distrib_11", "distrib_12"}
            rpc.bdev_lvol_delete_lvstore.assert_not_called()
            assert owner_a.lvstore not in _fresh(db, member).lvstore_ports

    @pytest.mark.parametrize("probe", [{"name": "distrib_12"}, RPCException("unreachable")])
    def test_a_surviving_remote_instance_fails_the_phase_before_any_pointer_moves(self, db, rpcs, probe):
        _, a, b, owner_a, _ = _two_owner_layout(db)
        owner_a = _update(db, owner_a, status=StorageNode.STATUS_IN_REMOVAL)
        rpc = rpcs[b[1].get_id()]
        rpc.bdev_distrib_delete.side_effect = RPCException("delete failed")
        if isinstance(probe, Exception):
            rpc.bdev_get.side_effect = probe
        else:
            rpc.bdev_get.side_effect = lambda name: probe if name == "distrib_12" else None
        assert ops._teardown_replicas_of_primary(owner_a) is False
        owner = _fresh(db, owner_a)
        assert ops.remote_triplet_refs(owner) == tuple(n.get_id() for n in b)
        assert owner.secondary_node_id == a[1].get_id() and owner.tertiary_node_id == a[2].get_id()
        # the retry completes once the peer lets go
        rpc.bdev_get.side_effect = None
        rpc.bdev_get.return_value = None
        assert ops._teardown_replicas_of_primary(owner) is True
        assert ops.remote_triplet_refs(_fresh(db, owner_a)) == ("", "", "")

    def test_an_offline_remote_peer_only_loses_its_ref(self, db, rpcs):
        _, a, b, owner_a, _ = _two_owner_layout(db)
        owner_a = _update(db, owner_a, status=StorageNode.STATUS_IN_REMOVAL)
        _update(db, b[2], status=StorageNode.STATUS_OFFLINE)
        assert ops._teardown_replicas_of_primary(owner_a) is True
        assert not rpcs[b[2].get_id()].method_calls
        assert ops.remote_triplet_refs(_fresh(db, owner_a)) == ("", "", "")

    def test_phase_2_learns_the_remote_carriers(self, db, rpcs):
        _, a, b, owner_a, _ = _two_owner_layout(db)
        with patch.object(ops, "shutdown_storage_node", return_value=True), \
                patch.object(ops, "set_node_status"), \
                patch.object(ops.cluster_ops, "set_cluster_status"), \
                patch.object(ops, "_check_replica_relocation_feasible", return_value=(True, "")), \
                patch.object(ops, "_teardown_replicas_of_primary", return_value=True), \
                patch.object(ops, "_decommission_node_jm") as decommission, \
                patch.object(ops, "_relocate_replicas_hosted_on", return_value=True), \
                patch.object(ops, "_verify_replica_stacks"), \
                patch.object(ops, "_reselect_remote_roles_held_by", return_value=True), \
                patch.object(ops, "_finalize_node_removal"), \
                patch.object(ops, "_decommission_node_devices", return_value=True):
            assert ops.node_removal_orchestrate(owner_a.get_id()) is True
        peers = decommission.call_args.kwargs["replica_peer_ids"]
        assert set(peers) == {a[1].get_id(), a[2].get_id(), *[n.get_id() for n in b]}


class TestRemovalReselection:

    def _layout(self, db):
        cluster = _seed_cluster(db)
        a, b = _sites(db, cluster, per_site=4)
        owner = _make_owner(db, a[0], a[:3], b[:3], lvs_id=1)
        for node in (*a, *b):
            if not _fresh(db, node).lvstore:
                _update(db, node, lvstore=f"LVS_x{node.get_id()[:3]}")
        return cluster, a, b, owner

    def test_the_new_member_gets_the_instance_from_the_fresh_owner(self, db, rpcs):
        cluster, a, b, owner = self._layout(db)
        removed = _update(db, b[1], status=StorageNode.STATUS_IN_REMOVAL)
        with patch.object(ops, "_rebuild_remote_instance", return_value=True) as build:
            assert ops._reselect_remote_roles_held_by(removed) is True
        refs = ops.remote_triplet_refs(_fresh(db, owner))
        assert removed.get_id() not in refs and b[3].get_id() in refs
        (member, passed_owner), = [c.args for c in build.call_args_list]
        assert member.get_id() == b[3].get_id()
        assert b[3].get_id() in ops.remote_triplet_refs(passed_owner)

    def test_a_retry_after_a_crash_finishes_the_build(self, db, rpcs):
        cluster, a, b, owner = self._layout(db)
        removed = _update(db, b[1], status=StorageNode.STATUS_IN_REMOVAL)
        # the crash: refs and marks written, no build
        ops.assign_remote_triplets(cluster, [owner.get_id()], exclude_ids=[removed.get_id()],
                                   db_controller=db)
        assert ops.owners_with_remote_role_on(removed.get_id(), db.get_storage_nodes_by_cluster_id(
            cluster.get_id())) == []
        with patch.object(ops, "_rebuild_remote_instance", return_value=True) as build:
            assert ops._reselect_remote_roles_held_by(removed) is True
        assert [c.args[0].get_id() for c in build.call_args_list] == [b[3].get_id()]

    def test_a_failed_build_keeps_the_mark_and_fails_the_phase(self, db, rpcs):
        cluster, a, b, owner = self._layout(db)
        removed = _update(db, b[1], status=StorageNode.STATUS_IN_REMOVAL)
        with patch.object(ops, "_rebuild_remote_instance", return_value=False):
            assert ops._reselect_remote_roles_held_by(removed) is False
        assert b[3].get_id() in _fresh(db, owner).remote_instances_pending
        with patch.object(ops, "_rebuild_remote_instance", side_effect=ops.LVSLeaderUnknownError("x")):
            assert ops._reselect_remote_roles_held_by(removed) is False

    def test_an_offline_pending_member_is_left_for_its_restart(self, db, rpcs):
        cluster, a, b, owner = self._layout(db)
        removed = _update(db, b[1], status=StorageNode.STATUS_IN_REMOVAL)
        ops.assign_remote_triplets(cluster, [owner.get_id()], exclude_ids=[removed.get_id()],
                                   db_controller=db)
        _update(db, b[3], status=StorageNode.STATUS_OFFLINE)
        with patch.object(ops, "_rebuild_remote_instance") as build:
            assert ops._reselect_remote_roles_held_by(removed) is True
        build.assert_not_called()
        assert _fresh(db, owner).remote_instances_pending == [b[3].get_id()]

    def test_owners_on_the_removed_site_are_not_built(self, db, rpcs):
        cluster, a, b, owner = self._layout(db)
        # b0 owns an LVS with a pending member on site A: not this removal's business
        _make_owner(db, b[0], b[:3], a[:3], lvs_id=2)
        _update(db, b[0], remote_instances_pending=[a[1].get_id()])
        removed = _update(db, b[3], status=StorageNode.STATUS_IN_REMOVAL)
        with patch.object(ops, "_rebuild_remote_instance") as build:
            assert ops._reselect_remote_roles_held_by(removed) is True
        build.assert_not_called()


# ---------------------------------------------------------------------------
# activation dispatch
# ---------------------------------------------------------------------------

def _activation_patches(stack):
    from simplyblock_core import cluster_ops
    stack.enter_context(patch.object(cluster_ops, "_wait_for_full_device_connectivity"))
    stack.enter_context(patch.object(cluster_ops.port_block, "set_port"))
    stack.enter_context(patch.object(cluster_ops, "tcp_ports_events"))
    stack.enter_context(patch.object(cluster_ops.time, "sleep"))
    stack.enter_context(patch.object(cluster_ops.utils, "set_storage_mcp_max_unavailable"))
    stack.enter_context(patch.object(ops, "verify_jm_mesh_coverage", return_value=[]))
    return cluster_ops


class TestActivationDispatch:

    def test_fresh_creates_sharing_a_remote_member_never_overlap(self, db, rpcs, hub):
        cluster = _seed_cluster(db, ftt=1)
        cluster.status = Cluster.STATUS_UNREADY
        cluster.write_to_db(db.kv_store)
        _sites(db, cluster)
        guard = threading.Lock()
        active: dict = {}
        overlaps = []

        def _create(snode, *args):
            touched = {snode.get_id(), snode.secondary_node_id, snode.tertiary_node_id,
                       *ops.remote_triplet_refs(snode)} - {""}
            with guard:
                overlaps.extend(touched & set(active))
                active.update(dict.fromkeys(touched, True))
            threading.Event().wait(0.05)   # time.sleep is patched out below
            with guard:
                for node_id in touched:
                    active.pop(node_id, None)
            _update(db, snode, lvstore=f"LVS_{snode.get_id()[:6]}")
            return True

        with ExitStack() as stack:
            cluster_ops = _activation_patches(stack)
            stack.enter_context(patch.object(ops, "create_lvstore", side_effect=_create))
            non_leader = stack.enter_context(
                patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True))
            cluster_ops._cluster_activate(cluster.get_id())

        assert overlaps == []
        nodes = {n.get_id(): n for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())}
        built = {(c.args[0].get_id(), c.args[2].get_id()) for c in non_leader.call_args_list}
        for owner in nodes.values():
            for member_id in ops.remote_triplet_refs(owner):
                assert (member_id, owner.get_id()) in built
            assert (owner.secondary_node_id, owner.get_id()) in built

    def test_reactivation_rebuilds_every_remote_instance_and_clears_its_mark(
            self, db, rpcs, hub, env):
        cluster = _seed_cluster(db, ftt=1)
        a, b = _sites(db, cluster)
        owners = []
        for i in range(3):
            owners.append(_make_owner(db, a[i], a[i:] + a[:i], b[i:] + b[:i], lvs_id=i + 1, ftt=1))
            owners.append(_make_owner(db, b[i], b[i:] + b[:i], a[i:] + a[:i], lvs_id=i + 4, ftt=1))
        for owner in owners:
            _update(db, owner, remote_instances_pending=list(ops.remote_triplet_refs(owner)))
        cluster = db.get_cluster_by_id(cluster.get_id())
        cluster.status = Cluster.STATUS_SUSPENDED
        cluster.activated_node_ids = [n.get_id() for n in a + b]
        cluster.write_to_db(db.kv_store)

        with ExitStack() as stack:
            cluster_ops = _activation_patches(stack)
            stack.enter_context(patch.object(ops, "recreate_lvstore", return_value=True))
            cluster_ops._cluster_activate(cluster.get_id())

        for owner in owners:
            owner = _fresh(db, owner)
            assert owner.remote_instances_pending == [], owner.get_id()
            for member_id in ops.remote_triplet_refs(owner):
                rpc = rpcs[member_id]
                rpc.bdev_examine.assert_any_call(owner.raid)
                assert not _took_leadership(rpc)
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_ACTIVE
