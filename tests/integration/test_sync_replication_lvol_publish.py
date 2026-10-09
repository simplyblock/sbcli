"""Volume publication on a sync-replication cluster, against the real
FoundationDB: six paths (five on FTT1) at stable positions on create, clone
and after a remote-ref change; the site ANA rule on every path that publishes
a listener or sets a volume's ANA state (create, the concurrent join of a
shared subsystem, restart of every member, repair, in-site failover /
failback, the deferred promotion, activation Pass 4); connect per site; and
the operations refused on a sync cluster. Storage nodes are mocked - a
stateful fake of the nvmf target where the ANA groups matter, the RPC
mocks of test_sync_replication_lvs_stack.py elsewhere; the database never is. The pure rules are in
tests/unit/test_sync_replication_lvol_publish.py.
"""
import copy
import sys
import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from flask import Flask

from simplyblock_cli import cli as cli_module
from simplyblock_core import cluster_ops
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import (
    health_controller, lvol_controller, migration_controller, snapshot_controller, tasks_controller,
)
from simplyblock_core.exceptions import SyncReplicationSiteError, SyncReplicationUnsupportedError
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import tasks_runner_batch_migration, tasks_runner_lvol_migration
from simplyblock_core.utils import ttl_cache
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
_seed_cluster, _sites, _make_owner = stack._seed_cluster, stack._sites, stack._make_owner
_update, _fresh = stack._update, stack._fresh

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
env = stack.env


@pytest.fixture(autouse=True)
def _clean_leader_caches():
    ttl_cache.leader_cache.invalidate()
    ttl_cache.no_leader_cache.invalidate()
    yield
    ttl_cache.leader_cache.invalidate()
    ttl_cache.no_leader_cache.invalidate()


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------

def _nic(ip):
    nic = IFace()
    nic.ip4_address = ip
    nic.trtype = "TCP"
    return nic


def _layout(db, *, ftt=2, active_site="", lost_site="", nics=1):
    """a0 owns LVS_1: home triplet a0 a1 a2 (FTT1: a0 a1), remote triplet
    b0 b1 b2. Every member serves the LVS on its port (listeners)."""
    cluster = _seed_cluster(db, ftt=ftt, lost_site=lost_site)
    a, b = _sites(db, cluster)
    owner = _make_owner(db, a[0], a, b, lvs_id=1, ftt=ftt)
    ports = dict(owner.lvstore_ports)
    for node in (*a, *b):
        fields = {"data_nics": [_nic(f"{node.mgmt_ip}{'' if i == 0 else f'.{i}'}")
                                for i in range(nics)],
                  "max_lvol": 100}
        if node.get_id() != owner.get_id():
            fields["lvstore_ports"] = ports
        _update(db, node, **fields)
    if active_site:
        _update(db, owner, lvs_active_site=active_site)
    return cluster, [_fresh(db, n) for n in a], [_fresh(db, n) for n in b], _fresh(db, owner)


def _pool(db, cluster):
    pool = Pool()
    pool.uuid = str(uuid.uuid4())
    pool.pool_name = f"pool-{pool.uuid[:8]}"
    pool.cluster_id = cluster.get_id()
    pool.status = Pool.STATUS_ACTIVE
    pool.write_to_db(db.kv_store)
    return pool


def _volume(db, cluster, owner, pool, *, nqn=None, namespaced=False, ns_id=0,
            active_site=SITE_A, demoted=(), nodes=None, status=LVol.STATUS_ONLINE):
    lv = LVol()
    lv.uuid = str(uuid.uuid4())
    lv.lvol_name = f"vol-{lv.uuid[:8]}"
    lv.place_in_pool(pool)
    lv.node_id = owner.get_id()
    lv.hostname = owner.hostname
    lv.lvs_name = owner.lvstore
    lv.lvol_bdev = f"LVOL_{lv.uuid[:8]}"
    lv.top_bdev = f"{owner.lvstore}/{lv.lvol_bdev}"
    lv.base_bdev = lv.top_bdev
    lv.lvol_uuid = lv.uuid
    lv.guid = "0" * 16
    lv.ha_type = "ha"
    lv.fabric = "tcp"
    lv.size = 1024 ** 3
    lv.nqn = nqn or f"{cluster.nqn}:lvol:{lv.uuid}"
    lv.namespace = "shared" if namespaced else ""
    lv.max_namespace_per_subsys = 32 if namespaced else 1
    lv.ns_id = ns_id
    lv.status = status
    lv.nodes = nodes if nodes is not None else [owner.get_id()] + lvol_controller.role_secondary_ids(owner)
    lv.sync_active_site = active_site
    lv.sync_demoted_sites = list(demoted)
    lv.write_to_db(db.kv_store)
    return lv


# ---------------------------------------------------------------------------
# a stateful nvmf target
# ---------------------------------------------------------------------------

class FakeSpdk:
    """The nvmf state of one node: subsystems, namespaces, listeners (created
    with a listener-wide ANA state) and the per-group ANA state set later.
    Every other RPC is a MagicMock."""

    def __init__(self, node_id):
        self.node_id = node_id
        self.subsystems: dict = {}
        self.default: dict = {}   # (nqn, ip) -> state the listener was created with
        self.groups: dict = {}    # (nqn, ip, nsid) -> state set per group
        self.fail_probe = False
        self.others: dict = {}

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return self.others.setdefault(name, MagicMock(name=f"{self.node_id}.{name}"))

    # subsystems / namespaces
    def subsystem_get(self, nqn):
        if self.fail_probe:
            raise RuntimeError("probe failed")
        sub = self.subsystems.get(nqn)
        return copy.deepcopy(sub) if sub else None

    def subsystem_create(self, nqn, serial_number, model_number, min_cntlid=1, max_namespaces=32,
                         allow_any_host=True):
        self.subsystems.setdefault(nqn, {"nqn": nqn, "min_cntlid": min_cntlid, "namespaces": [],
                                         "listen_addresses": []})
        return True

    def nvmf_subsystem_add_ns2(self, nqn, dev_name, uuid=None, nguid=None, nsid=None, **kwargs):
        sub = self.subsystems.get(nqn)
        if sub is None:
            return None, {"code": -32602, "message": "no subsystem"}
        nsid = nsid or max((n["nsid"] for n in sub["namespaces"]), default=0) + 1
        sub["namespaces"].append({"nsid": nsid, "uuid": uuid, "bdev_name": dev_name})
        return nsid, None

    def nvmf_subsystem_add_ns(self, nqn, dev_name, uuid=None, nguid=None, nsid=None, **kwargs):
        sub = self.subsystems.get(nqn)
        if sub is not None and nsid and any(n["nsid"] == nsid for n in sub["namespaces"]):
            return nsid     # idempotent re-add
        return self.nvmf_subsystem_add_ns2(nqn, dev_name, uuid, nguid, nsid)[0]

    def nvmf_subsystem_remove_ns(self, nqn, nsid):
        sub = self.subsystems.get(nqn)
        if sub:
            sub["namespaces"] = [n for n in sub["namespaces"] if n["nsid"] != nsid]
        return True

    # listeners
    def _add_listener(self, nqn, trtype, ip, port, state):
        sub = self.subsystems.get(nqn)
        if sub is None:
            return False
        if any(la["traddr"] == ip and la["trsvcid"] == str(port) for la in sub["listen_addresses"]):
            return False
        sub["listen_addresses"].append({"trtype": trtype, "traddr": ip, "trsvcid": str(port)})
        self.default[(nqn, ip)] = state or "optimized"
        return True

    def nvmf_subsystem_add_listener(self, nqn, trtype, traddr, trsvcid, ana_state=None):
        if self._add_listener(nqn, trtype, traddr, trsvcid, ana_state):
            return True, None
        return False, {"code": -32602, "message": "listener exists"}

    def listeners_create(self, nqn, trtype, traddr, trsvcid, ana_state=None):
        return self._add_listener(nqn, trtype, traddr, trsvcid, ana_state) or None

    def listeners_del(self, nqn, trtype, traddr, trsvcid):
        sub = self.subsystems.get(nqn)
        if sub:
            sub["listen_addresses"] = [la for la in sub["listen_addresses"] if la["traddr"] != traddr]
        return True

    def nvmf_subsystem_listener_set_ana_state(self, nqn, ip, port, trtype="TCP", is_optimized=True,
                                              ana=None, anagrpid=None):
        sub = self.subsystems.get(nqn)
        if not sub or not any(la["traddr"] == ip for la in sub["listen_addresses"]):
            return False
        self.groups[(nqn, ip, anagrpid)] = ana
        return True

    def get_bdevs(self, name=None):
        return [{"uuid": "bdev-uuid", "driver_specific": {"lvol": {"blobid": 7}}}]

    def listeners_list(self, nqn):
        """nvmf_subsystem_get_listeners: every listener with the state of
        each ANA group (group id = nsid), as SPDK dumps it."""
        sub = self.subsystems.get(nqn)
        if sub is None:
            return None
        top = max((n["nsid"] for n in sub["namespaces"]), default=0)
        return [{"address": dict(la),
                 "ana_states": [{"ana_group": g, "ana_state": self.state(nqn, la["traddr"], g)}
                                for g in range(1, top + 1)]}
                for la in sub["listen_addresses"]]

    # reading back
    def state(self, nqn, ip, nsid):
        """What a host sees for namespace ``nsid`` through listener ``ip``."""
        if (nqn, ip, nsid) in self.groups:
            return self.groups[(nqn, ip, nsid)]
        return self.default.get((nqn, ip))


class _Targets(dict):
    def __missing__(self, node_id):
        self[node_id] = FakeSpdk(node_id)
        return self[node_id]


@pytest.fixture()
def spdk():
    targets = _Targets()
    with patch.object(StorageNode, "rpc_client", autospec=True,
                      side_effect=lambda self, *args, **kwargs: targets[self.get_id()]), \
            patch.object(lvol_controller, "_create_bdev_stack", return_value=(True, None)):
        yield targets


def _ip(node, i=0):
    return node.data_nics[i].ip4_address


def _seen(spdk, node, lvol, nsid=None, i=0):
    return spdk[node.get_id()].state(lvol.nqn, _ip(node, i), nsid or lvol.ns_id)


def _register_everywhere(db, lvol, leader, members):
    """The create sequence of add_lvol_ha: the leader, then every other path."""
    bdev, err = lvol_controller.add_lvol_on_node(lvol, leader)
    assert err is None, err
    for member in members:
        if member.get_id() == leader.get_id():
            continue
        _, err = lvol_controller.add_lvol_on_node(
            lvol, member, is_primary=False,
            secondary_index=lvol_controller._lvol_secondary_index(lvol, member))
        assert err is None, err
    return bdev


# ---------------------------------------------------------------------------
# member list
# ---------------------------------------------------------------------------

def _add_lvol_ha(db, pool, owner):
    registered = []

    def _add(lvol, node, is_primary=True, secondary_index=0, **kwargs):
        registered.append((node.get_id(), is_primary, secondary_index))
        return {"uuid": "bdev-uuid", "driver_specific": {"lvol": {"blobid": 7}}}, None

    with patch.object(lvol_controller, "add_lvol_on_node", side_effect=_add), \
            patch.object(lvol_controller, "set_lvol"), \
            patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                  return_value="proceed"):
        vol_id, err = lvol_controller.add_lvol_ha(
            "vol-1", 2 * 1024 ** 3, owner.get_id(), "ha", pool.get_id())
    assert err is None, err
    return db.get_lvol_by_id(vol_id), registered


class TestMembers:

    @pytest.mark.parametrize("ftt", [2, 1])
    def test_a_volume_lists_every_instance_home_then_remote(self, db, rpcs, env, ftt):
        cluster, a, b, owner = _layout(db, ftt=ftt)
        rpcs.lead(owner)
        vol, registered = _add_lvol_ha(db, _pool(db, cluster), owner)
        home = [n.get_id() for n in a][:3 if ftt == 2 else 2]
        assert vol.nodes == home + [n.get_id() for n in b]
        assert registered[0] == (owner.get_id(), True, 0)
        # each non-leader in the window of its position
        assert registered[1:] == [(nid, False, i) for i, nid in enumerate(vol.nodes[1:])]
        assert vol.sync_active_site == SITE_A

    @pytest.mark.parametrize("marker, site", [(SITE_B, SITE_B), ("moving:" + SITE_B, SITE_B)])
    def test_a_volume_created_on_a_remote_led_lvs_is_active_there(self, db, rpcs, env, marker, site):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        vol, registered = _add_lvol_ha(db, _pool(db, cluster), owner)
        assert vol.sync_active_site == SITE_B
        assert registered[0][:2] == (b[0].get_id(), True)
        assert vol.nodes[:3] == [n.get_id() for n in a]

    def test_a_clone_lists_every_instance(self, db, rpcs, env):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        pool = _pool(db, cluster)
        src = _volume(db, cluster, owner, pool, active_site=SITE_B)
        snap = SnapShot()
        snap.uuid = str(uuid.uuid4())
        snap.cluster_id = cluster.get_id()
        snap.pool_uuid = pool.get_id()
        snap.lvol = src
        snap.status = SnapShot.STATUS_ONLINE
        snap.size = src.size
        snap.snap_bdev = f"{owner.lvstore}/SNAP_1"
        snap.fabric = "tcp"
        snap.write_to_db(db.kv_store)
        rpcs.lead(b[0])
        registered = []

        def _add(lvol, node, is_primary=True, secondary_index=0, **kwargs):
            registered.append((node.get_id(), is_primary))
            return {"uuid": "bdev-uuid", "driver_specific": {"lvol": {"blobid": 7}}}, None

        with patch.object(lvol_controller, "add_lvol_on_node", side_effect=_add), \
                patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                      return_value="proceed"), \
                patch.object(snapshot_controller, "snapshot_events"):
            clone_id, err = snapshot_controller.clone(snap.get_id(), "clone-1", lock=False)
        assert err is False, err
        clone = db.get_lvol_by_id(clone_id)
        assert clone.nodes == [n.get_id() for n in (*a, *b)]
        assert clone.sync_active_site == SITE_B
        assert registered[0] == (b[0].get_id(), True)
        assert sorted(nid for nid, primary in registered if not primary) == sorted(
            n.get_id() for n in (*a, b[1], b[2]))


class TestRemoteRefChange:

    def _layout_with_spare(self, db):
        cluster, a, b, owner = _layout(db)
        spare = stack._seed_node(db, cluster, "b3", SITE_B)
        return cluster, a, b, owner, spare

    def test_a_reselected_member_takes_the_same_position(self, db):
        cluster, a, b, owner, spare = self._layout_with_spare(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster))
        written = ops.assign_remote_triplets(cluster, [owner.get_id()], exclude_ids=(b[1].get_id(),))
        triplet = written[owner.get_id()]
        assert triplet[0] == b[0].get_id() and triplet[2] == b[2].get_id()
        assert db.get_lvol_by_id(vol.get_id()).nodes == [n.get_id() for n in a] + list(triplet)

    def test_a_crash_between_the_ref_and_the_volume_writes_is_repaired_by_the_retry(self, db):
        cluster, a, b, owner, spare = self._layout_with_spare(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster))
        real = ops.reconcile_lvol_remote_members
        with patch.object(ops, "reconcile_lvol_remote_members", side_effect=RuntimeError("crash")):
            with pytest.raises(RuntimeError):
                ops.assign_remote_triplets(cluster, [owner.get_id()], exclude_ids=(b[1].get_id(),))
        triplet = ops.remote_triplet_refs(_fresh(db, owner))
        assert spare.get_id() in triplet
        assert db.get_lvol_by_id(vol.get_id()).nodes[3:] == [n.get_id() for n in b]
        with patch.object(ops, "reconcile_lvol_remote_members", side_effect=real) as rec:
            assert ops.assign_remote_triplets(cluster, [owner.get_id()],
                                              exclude_ids=(b[1].get_id(),)) == {}
        rec.assert_called_once()
        assert db.get_lvol_by_id(vol.get_id()).nodes == [n.get_id() for n in a] + list(triplet)


# ---------------------------------------------------------------------------
# create: listeners and ANA groups
# ---------------------------------------------------------------------------

class TestCreatePublication:

    def test_home_led(self, db, spdk):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), status=LVol.STATUS_IN_CREATION)
        _register_everywhere(db, vol, owner, [*a, *b])
        expected = [(a[0], "optimized"), (a[1], "non_optimized"), (a[2], "non_optimized"),
                    (b[0], "inaccessible"), (b[1], "inaccessible"), (b[2], "inaccessible")]
        for index, (node, state) in enumerate(expected):
            target = spdk[node.get_id()]
            assert target.subsystems[vol.nqn]["min_cntlid"] == lvol_controller.lvol_min_cntlid(index)
            assert target.default[(vol.nqn, _ip(node))] == "inaccessible"
            assert _seen(spdk, node, vol) == state, node.get_id()

    def test_remote_led_leader_in_its_own_window(self, db, spdk):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        vol = _volume(db, cluster, owner, _pool(db, cluster), active_site=SITE_B,
                      status=LVol.STATUS_IN_CREATION)
        _register_everywhere(db, vol, b[0], [*a, *b])
        assert spdk[b[0].get_id()].subsystems[vol.nqn]["min_cntlid"] == 3000
        assert spdk[a[0].get_id()].subsystems[vol.nqn]["min_cntlid"] == 1
        assert [_seen(spdk, n, vol) for n in (*a, *b)] == [
            "inaccessible"] * 3 + ["optimized", "non_optimized", "non_optimized"]

    def test_a_leading_secondary_is_the_optimized_path(self, db, spdk):
        """Home primary offline, its secondary leads: the create runs there."""
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), status=LVol.STATUS_IN_CREATION)
        _register_everywhere(db, vol, a[1], [a[1], a[2], *b])
        assert spdk[a[1].get_id()].subsystems[vol.nqn]["min_cntlid"] == 1000
        assert _seen(spdk, a[1], vol) == "optimized"
        assert _seen(spdk, a[2], vol) == "non_optimized"

    def test_demoted_home_site_stays_fenced(self, db, spdk):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), demoted=[SITE_A],
                      status=LVol.STATUS_IN_CREATION)
        _register_everywhere(db, vol, owner, [*a, *b])
        assert {_seen(spdk, n, vol) for n in (*a, *b)} == {"inaccessible"}

    def test_the_group_is_the_namespace_spdk_assigned(self, db, spdk):
        cluster, a, b, owner = _layout(db)
        pool = _pool(db, cluster)
        nqn = f"{cluster.nqn}:lvol:shared"
        target = spdk[owner.get_id()]
        target.subsystem_create(nqn, "ha", "x")
        target.subsystems[nqn]["namespaces"] = [{"nsid": 1, "uuid": "o1", "bdev_name": "x1"},
                                                {"nsid": 2, "uuid": "o2", "bdev_name": "x2"}]
        vol = _volume(db, cluster, owner, pool, nqn=nqn, namespaced=True,
                      status=LVol.STATUS_IN_CREATION)
        target.listeners_create(nqn, "TCP", _ip(owner), 4402, ana_state="inaccessible")
        lvol_controller.add_lvol_on_node(vol, owner)
        assert vol.ns_id == 3
        assert target.groups[(nqn, _ip(owner), 3)] == "optimized"
        # the foreign namespaces of the subsystem were not touched
        assert (nqn, _ip(owner), 1) not in target.groups


class TestSharedSubsystem:
    """Two volumes of one NQN with different site states (one demoted on A)."""

    def _pair(self, db, nics=1):
        cluster, a, b, owner = _layout(db, nics=nics)
        pool = _pool(db, cluster)
        nqn = f"{cluster.nqn}:lvol:shared"
        first = _volume(db, cluster, owner, pool, nqn=nqn, namespaced=True,
                        status=LVol.STATUS_IN_CREATION)
        second = _volume(db, cluster, owner, pool, nqn=nqn, namespaced=True, demoted=[SITE_A],
                         status=LVol.STATUS_IN_CREATION)
        return cluster, a, b, owner, first, second

    def test_a_join_after_the_listener_sets_its_own_group(self, db, spdk):
        _, a, b, owner, first, second = self._pair(db)
        # the creator makes the subsystem, then the joiner attaches
        first.namespace = ""
        lvol_controller.add_lvol_on_node(first, owner)
        lvol_controller.add_lvol_on_node(second, owner)
        assert _seen(spdk, owner, first) == "optimized"
        assert _seen(spdk, owner, second) == "inaccessible"
        assert (second.nqn, _ip(owner), second.ns_id) in spdk[owner.get_id()].groups

    def test_a_join_before_the_listener_is_set_by_the_creators_publication(self, db, spdk):
        _, a, b, owner, first, second = self._pair(db)
        first.namespace = ""
        # creator: subsystem + namespace, publication not yet done
        lvol_controller.add_lvol_on_node(first, owner, defer_listeners=True)
        ns_first = first.ns_id
        # the demoted joiner attaches, finds no listener, skips
        lvol_controller.add_lvol_on_node(second, owner)
        assert not spdk[owner.get_id()].subsystems[first.nqn]["listen_addresses"]
        # now the creator publishes: every namespace gets its own state
        ok, err = lvol_controller.publish_lvol_listeners(first, owner, is_primary=True, ns_id=ns_first)
        assert (ok, err) == (True, None)
        assert _seen(spdk, owner, first) == "optimized"
        assert _seen(spdk, owner, second) == "inaccessible"
        # the joiner's group was set explicitly, not left to the listener default
        assert spdk[owner.get_id()].groups[(second.nqn, _ip(owner), second.ns_id)] == "inaccessible"

    def test_an_open_joiner_before_the_listener_is_opened_by_the_sweep(self, db, spdk):
        _, a, b, owner, first, second = self._pair(db)
        first.namespace = ""
        # the site rule reads the stored record (under the site-rule lock)
        first.sync_demoted_sites = [SITE_A]
        first.write_to_db(db.kv_store)
        second.sync_demoted_sites = []
        second.write_to_db(db.kv_store)
        lvol_controller.add_lvol_on_node(first, owner, defer_listeners=True)
        lvol_controller.add_lvol_on_node(second, owner)
        lvol_controller.publish_lvol_listeners(first, owner, is_primary=True, ns_id=first.ns_id)
        assert _seen(spdk, owner, first) == "inaccessible"
        assert _seen(spdk, owner, second) == "optimized"

    def test_two_nics_a_join_between_the_creators_listeners(self, db, spdk):
        _, a, b, owner, first, second = self._pair(db, nics=2)
        first.namespace = ""
        lvol_controller.add_lvol_on_node(first, owner, defer_listeners=True)
        target = spdk[owner.get_id()]
        # the creator has published its first listener only
        target.listeners_create(first.nqn, "TCP", _ip(owner, 0), 4402, ana_state="inaccessible")
        _, err = lvol_controller.add_lvol_on_node(second, owner)
        assert err is None
        assert (second.nqn, _ip(owner, 0), second.ns_id) in target.groups
        assert (second.nqn, _ip(owner, 1), second.ns_id) not in target.groups
        lvol_controller.publish_lvol_listeners(first, owner, is_primary=True, ns_id=first.ns_id)
        for i in (0, 1):
            assert _seen(spdk, owner, second, i=i) == "inaccessible"
            assert _seen(spdk, owner, first, i=i) == "optimized"

    def test_a_join_that_cannot_probe_the_listeners_fails_and_rolls_back(self, db, spdk):
        _, a, b, owner, first, second = self._pair(db)
        first.namespace = ""
        lvol_controller.add_lvol_on_node(first, owner)
        target = spdk[owner.get_id()]
        real_add = target.nvmf_subsystem_add_ns2

        def _add_then_break(*args, **kwargs):
            ret = real_add(*args, **kwargs)
            target.fail_probe = True
            return ret

        with patch.object(target, "nvmf_subsystem_add_ns2", side_effect=_add_then_break):
            _, err = lvol_controller.add_lvol_on_node(second, owner)
        target.fail_probe = False
        assert err and "Cannot tell which listeners" in err
        assert [n["nsid"] for n in target.subsystems[first.nqn]["namespaces"]] == [first.ns_id]

    def test_a_repair_recreating_the_listener_sets_every_namespace(self, db, spdk):
        cluster, a, b, owner, first, second = self._pair(db)
        target = spdk[a[1].get_id()]
        target.subsystem_create(first.nqn, "ha", "x")
        for ns_id, vol in ((1, first), (2, second)):
            vol.ns_id = ns_id
            vol.status = LVol.STATUS_ONLINE
            vol.write_to_db(db.kv_store)
            target.subsystems[first.nqn]["namespaces"].append(
                {"nsid": ns_id, "uuid": vol.get_ns_uuid(), "bdev_name": vol.top_bdev})
        ok, err = ops._publish_lvol_listener(first, _fresh(db, a[1]), target, "non_optimized")
        assert (ok, err) == (True, None)
        assert _seen(spdk, a[1], first) == "non_optimized"
        assert _seen(spdk, a[1], second) == "inaccessible"


class TestFailedGroup:

    def test_a_group_that_failed_is_retried_durably_until_set(self, db, spdk):
        """Listener and namespace are there, so the lvol monitor's presence
        check calls the path healthy: the failed group state must be retried
        by the sync-op registration instead."""
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        node = _fresh(db, a[1])
        target = spdk[node.get_id()]
        target.subsystem_create(vol.nqn, "ha", "x", min_cntlid=1000)
        target.subsystems[vol.nqn]["namespaces"].append(
            {"nsid": 1, "uuid": vol.get_ns_uuid(), "bdev_name": vol.top_bdev})
        with patch.object(target, "nvmf_subsystem_listener_set_ana_state", return_value=False):
            ok, err = ops._publish_lvol_listener(vol, node, target, "non_optimized")
        assert not ok and "ns [1]" in err
        assert health_controller.check_subsystem(vol.nqn, rpc_client=target, ns_uuid=vol.get_ns_uuid())
        assert _seen(spdk, node, vol) == "inaccessible"
        (task,) = [t for t in db.get_job_tasks(cluster.get_id())
                   if t.function_name == JobSchedule.FN_LVOL_SYNC_OP]
        assert (task.node_id, task.function_params["lvol_id"], task.function_params["op"]) == (
            node.get_id(), vol.get_id(), "register")
        tasks_controller.run_lvol_sync_op_task(task)
        (done,) = [t for t in db.get_job_tasks(cluster.get_id())
                   if t.function_name == JobSchedule.FN_LVOL_SYNC_OP]
        assert done.status == JobSchedule.STATUS_DONE, done.function_result
        assert _seen(spdk, node, vol) == "non_optimized"

    def test_another_members_failed_group_is_queued_for_that_member(self, db, spdk):
        cluster, a, b, owner = _layout(db)
        pool = _pool(db, cluster)
        nqn = f"{cluster.nqn}:lvol:shared"
        first = _volume(db, cluster, owner, pool, nqn=nqn, namespaced=True, ns_id=1)
        second = _volume(db, cluster, owner, pool, nqn=nqn, namespaced=True, ns_id=2,
                         demoted=[SITE_A])
        node = _fresh(db, a[1])
        target = spdk[node.get_id()]
        target.subsystem_create(nqn, "ha", "x", min_cntlid=1000)
        for vol in (first, second):
            target.subsystems[nqn]["namespaces"].append(
                {"nsid": vol.ns_id, "uuid": vol.get_ns_uuid(), "bdev_name": vol.top_bdev})
        real = target.nvmf_subsystem_listener_set_ana_state
        broken = {"once": True}

        def _second_fails_once(nqn_, ip, port, anagrpid=None, **kwargs):
            if anagrpid == 2 and broken.pop("once", False):
                return False
            return real(nqn_, ip, port, anagrpid=anagrpid, **kwargs)

        with patch.object(target, "nvmf_subsystem_listener_set_ana_state",
                          side_effect=_second_fails_once):
            # first creates the listener; its own group is right, the sweep's
            # second group is not
            ok, err = ops._publish_lvol_listener(first, node, target, "non_optimized")
        assert (ok, err) == (True, None)
        assert _seen(spdk, node, first) == "non_optimized"
        assert _seen(spdk, node, second) == "inaccessible"
        (task,) = [t for t in db.get_job_tasks(cluster.get_id())
                   if t.function_name == JobSchedule.FN_LVOL_SYNC_OP]
        assert task.function_params["lvol_id"] == second.get_id()
        # the demoted second volume stays fenced whichever way: prove the
        # retry SETS it by opening its site before the retry runs
        second.sync_demoted_sites = []
        second.write_to_db(db.kv_store)
        tasks_controller.run_lvol_sync_op_task(task)
        assert _seen(spdk, node, second) == "non_optimized"
        assert _seen(spdk, node, first) == "non_optimized"

    def _shared(self, db, spdk, nics=1):
        """Two online volumes of one NQN on a1 (the second demoted on A),
        both namespaces attached, no listener yet."""
        cluster, a, b, owner = _layout(db, nics=nics)
        pool = _pool(db, cluster)
        nqn = f"{cluster.nqn}:lvol:shared"
        first = _volume(db, cluster, owner, pool, nqn=nqn, namespaced=True, ns_id=1)
        second = _volume(db, cluster, owner, pool, nqn=nqn, namespaced=True, ns_id=2,
                         demoted=[SITE_A])
        node = _fresh(db, a[1])
        target = spdk[node.get_id()]
        target.subsystem_create(nqn, "ha", "x", min_cntlid=1000)
        for vol in (first, second):
            target.subsystems[nqn]["namespaces"].append(
                {"nsid": vol.ns_id, "uuid": vol.get_ns_uuid(), "bdev_name": vol.top_bdev})
        return cluster, node, target, first, second

    def _retry_all(self, db, cluster):
        tasks = [t for t in db.get_job_tasks(cluster.get_id())
                 if t.function_name == JobSchedule.FN_LVOL_SYNC_OP]
        for task in tasks:
            tasks_controller.run_lvol_sync_op_task(task)
        return sorted(t.function_params["lvol_id"] for t in tasks)

    def _right(self, spdk, node, first, second, nics=1):
        for i in range(nics):
            assert _seen(spdk, node, first, i=i) == "non_optimized"
            assert _seen(spdk, node, second, i=i) == "inaccessible"
            assert (second.nqn, _ip(node, i), second.ns_id) in spdk[node.get_id()].groups

    def test_a_raised_group_rpc_is_queued_like_a_failed_one(self, db, spdk):
        cluster, node, target, first, second = self._shared(db, spdk)
        real = target.nvmf_subsystem_listener_set_ana_state
        broken = {"once": True}

        def _raise_once(nqn_, ip, port, anagrpid=None, **kwargs):
            if anagrpid == 2 and broken.pop("once", False):
                raise RPCException("connection error")
            return real(nqn_, ip, port, anagrpid=anagrpid, **kwargs)

        with patch.object(target, "nvmf_subsystem_listener_set_ana_state", side_effect=_raise_once):
            assert ops._publish_lvol_listener(first, node, target, "non_optimized") == (True, None)
        assert self._retry_all(db, cluster) == [second.get_id()]
        self._right(spdk, node, first, second)

    def test_a_raised_namespace_read_queues_every_member(self, db, spdk):
        cluster, node, target, first, second = self._shared(db, spdk)
        real = FakeSpdk.subsystem_get
        calls = {"n": 0}

        def _second_read_raises(nqn_):
            calls["n"] += 1
            if calls["n"] == 2:     # the sweep, after the listener was created
                raise RPCException("connection error")
            return real(target, nqn_)

        with patch.object(target, "subsystem_get", side_effect=_second_read_raises):
            ok, err = ops._publish_lvol_listener(first, node, target, "non_optimized")
        assert not ok and "queued" in err
        assert _seen(spdk, node, second) == "inaccessible"
        assert self._retry_all(db, cluster) == sorted([first.get_id(), second.get_id()])
        self._right(spdk, node, first, second)

    def test_a_listener_raised_on_the_second_nic_queues_every_member(self, db, spdk):
        cluster, node, target, first, second = self._shared(db, spdk, nics=2)
        real = target.listeners_create

        def _second_nic_raises(nqn_, trtype, ip, port, ana_state=None):
            if ip == _ip(node, 1):
                raise RPCException("connection error")
            return real(nqn_, trtype, ip, port, ana_state=ana_state)

        with patch.object(target, "listeners_create", side_effect=_second_nic_raises):
            with pytest.raises(RPCException):
                ops._publish_lvol_listener(first, node, target, "non_optimized")
        assert self._retry_all(db, cluster) == sorted([first.get_id(), second.get_id()])
        self._right(spdk, node, first, second, nics=2)

    def test_a_listener_raised_on_recreate_queues_every_member(self, db, spdk):
        cluster, node, target, first, second = self._shared(db, spdk, nics=2)
        real = target.listeners_create

        def _second_nic_raises(nqn_, trtype, ip, port, ana_state=None):
            if ip == _ip(node, 1):
                raise RPCException("connection error")
            return real(nqn_, trtype, ip, port, ana_state=ana_state)

        with patch.object(target, "listeners_create", side_effect=_second_nic_raises):
            with pytest.raises(RPCException):
                lvol_controller.recreate_lvol_on_node(first, node)
        assert self._retry_all(db, cluster) == sorted([first.get_id(), second.get_id()])
        self._right(spdk, node, first, second, nics=2)

    def test_a_creator_whose_listener_raises_hands_the_joiners_to_the_retry(self, db, spdk):
        cluster, a, b, owner = _layout(db, nics=2)
        pool = _pool(db, cluster)
        nqn = f"{cluster.nqn}:lvol:shared"
        creator = _volume(db, cluster, owner, pool, nqn=nqn, status=LVol.STATUS_IN_CREATION)
        joiner = _volume(db, cluster, owner, pool, nqn=nqn, namespaced=True)
        target = spdk[owner.get_id()]
        lvol_controller.add_lvol_on_node(creator, owner, defer_listeners=True)
        lvol_controller.add_lvol_on_node(joiner, owner)       # no listener yet: skips
        joiner.write_to_db(db.kv_store)                        # its ns_id, as the create persists it
        real = target.nvmf_subsystem_add_listener

        def _second_nic_raises(nqn_, trtype, ip, port, ana_state=None):
            if ip == _ip(owner, 1):
                raise RPCException("connection error")
            return real(nqn_, trtype, ip, port, ana_state=ana_state)

        with patch.object(target, "nvmf_subsystem_add_listener", side_effect=_second_nic_raises):
            ok, err = lvol_controller.publish_lvol_listeners(creator, owner, is_primary=True,
                                                             ns_id=creator.ns_id)
        assert not ok and "connection error" in err
        assert not target.subsystems[nqn]["listen_addresses"]        # rolled back
        assert self._retry_all(db, cluster) == [joiner.get_id()]
        for i in (0, 1):
            assert _seen(spdk, owner, joiner, i=i) == "optimized"

    def test_a_failed_group_on_recreate_is_queued(self, db, spdk):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        node = _fresh(db, a[1])
        target = spdk[node.get_id()]
        with patch.object(target, "nvmf_subsystem_listener_set_ana_state", return_value=False):
            ok, err = lvol_controller.recreate_lvol_on_node(vol, node)
        assert not ok and "queued" in err
        assert _seen(spdk, node, vol) == "inaccessible"
        (task,) = [t for t in db.get_job_tasks(cluster.get_id())
                   if t.function_name == JobSchedule.FN_LVOL_SYNC_OP]
        tasks_controller.run_lvol_sync_op_task(task)
        assert _seen(spdk, node, vol) == "non_optimized"
        assert target.subsystems[vol.nqn]["min_cntlid"] == 1000


class TestMonitorSelfHeal:
    """The lvol monitor compares every path's ANA group with the site rule and
    repairs a mismatch: a listener present with its group left at
    the inaccessible default is not a healthy path."""

    def _registered(self, db, spdk, **layout):
        cluster, a, b, owner = _layout(db, **layout)
        vol = _volume(db, cluster, owner, _pool(db, cluster),
                      active_site=layout.get("active_site") or SITE_A)
        leader = owner if not layout.get("active_site") else b[0]
        _register_everywhere(db, vol, leader, [*a, *b])
        vol.write_to_db(db.kv_store)
        spdk[owner.get_id()]      # the owner's target exists (primary check)
        return cluster, a, b, _fresh(db, owner), db.get_lvol_by_id(vol.get_id())

    def _cycle(self, cluster, owner, vol):
        from simplyblock_core.services import lvol_monitor
        repairs = []
        real = lvol_monitor.try_repair_lvol_on_non_leader

        def _spy(lvol, node, index):
            repairs.append(node.get_id())
            return real(lvol, node, index)

        with patch.object(lvol_monitor, "try_repair_lvol_on_non_leader", side_effect=_spy):
            lvol_monitor.check_node(cluster, owner, [vol], subsys_check=True)
        return repairs

    def test_a_listener_whose_creation_answer_was_lost_is_repaired_on_the_next_cycle(self, db, spdk):
        cluster, a, b, owner, vol = self._registered(db, spdk)
        node = a[1]
        target = spdk[node.get_id()]
        target.listeners_del(vol.nqn, "TCP", _ip(node), 4402)
        target.groups.clear()
        real = target.listeners_create

        def _applied_then_lost(nqn_, trtype, ip, port, ana_state=None):
            real(nqn_, trtype, ip, port, ana_state=ana_state)
            raise RPCException("connection error")

        with patch.object(target, "listeners_create", side_effect=_applied_then_lost):
            with pytest.raises(RPCException):
                ops._publish_lvol_listener(vol, node, target, "non_optimized")
        # present, so the presence check alone calls it healthy - but closed
        assert health_controller.check_subsystem(vol.nqn, rpc_client=target, ns_uuid=vol.get_ns_uuid())
        assert _seen(spdk, node, vol) == "inaccessible"
        assert self._cycle(cluster, owner, vol) == [node.get_id()]
        assert _seen(spdk, node, vol) == "non_optimized"
        assert db.get_lvol_by_id(vol.get_id()).health_check is False
        # the next cycle finds it right and repairs nothing
        assert self._cycle(cluster, owner, db.get_lvol_by_id(vol.get_id())) == []
        assert db.get_lvol_by_id(vol.get_id()).health_check is True

    def test_the_owner_path_recreated_with_its_answer_lost_is_repaired(self, db, spdk):
        """recreate_lvol_on_node on the owner: the first listener is created,
        then the answer is lost - the owner's group stays inaccessible."""
        cluster, a, b, owner, vol = self._registered(db, spdk)
        target = spdk[owner.get_id()]
        del target.subsystems[vol.nqn]
        target.groups.clear()
        real = target.listeners_create

        def _applied_then_lost(nqn_, trtype, ip, port, ana_state=None):
            real(nqn_, trtype, ip, port, ana_state=ana_state)
            raise RPCException("connection error")

        with patch.object(target, "listeners_create", side_effect=_applied_then_lost):
            with pytest.raises(RPCException):
                lvol_controller.recreate_lvol_on_node(vol, owner)
        assert health_controller.check_subsystem(vol.nqn, rpc_client=target, ns_uuid=vol.get_ns_uuid())
        assert _seen(spdk, owner, vol) == "inaccessible"
        assert self._cycle(cluster, owner, vol) == [owner.get_id()]
        assert _seen(spdk, owner, vol) == "optimized"
        assert target.subsystems[vol.nqn]["min_cntlid"] == 1
        assert self._cycle(cluster, owner, db.get_lvol_by_id(vol.get_id())) == []

    def test_a_remote_led_owner_found_open_is_fenced_again(self, db, spdk):
        cluster, a, b, owner, vol = self._registered(db, spdk, active_site=SITE_B)
        spdk[owner.get_id()].nvmf_subsystem_listener_set_ana_state(
            vol.nqn, _ip(owner), 4402, ana="optimized", anagrpid=vol.ns_id)
        assert self._cycle(cluster, owner, vol) == [owner.get_id()]
        assert _seen(spdk, owner, vol) == "inaccessible"

    def test_a_healthy_sync_volume_is_left_alone(self, db, spdk):
        cluster, a, b, owner, vol = self._registered(db, spdk)
        assert self._cycle(cluster, owner, vol) == []
        assert db.get_lvol_by_id(vol.get_id()).health_check is True
        assert [_seen(spdk, n, vol) for n in (*a, *b)] == [
            "optimized", "non_optimized", "non_optimized"] + ["inaccessible"] * 3

    def test_a_closed_site_path_found_open_is_fenced_again(self, db, spdk):
        cluster, a, b, owner, vol = self._registered(db, spdk)
        spdk[b[1].get_id()].nvmf_subsystem_listener_set_ana_state(
            vol.nqn, _ip(b[1]), 4402, ana="optimized", anagrpid=vol.ns_id)
        assert self._cycle(cluster, owner, vol) == [b[1].get_id()]
        assert _seen(spdk, b[1], vol) == "inaccessible"

    def test_a_failed_over_secondary_is_not_drift(self, db, spdk):
        """optimized vs non_optimized on the open site is the in-site
        failover's business: a promoted secondary is left as it is."""
        cluster, a, b, owner, vol = self._registered(db, spdk)
        spdk[a[1].get_id()].nvmf_subsystem_listener_set_ana_state(
            vol.nqn, _ip(a[1]), 4402, ana="optimized", anagrpid=vol.ns_id)
        assert self._cycle(cluster, owner, vol) == []
        assert _seen(spdk, a[1], vol) == "optimized"

    def test_a_remote_led_volume(self, db, spdk):
        cluster, a, b, owner, vol = self._registered(db, spdk, active_site=SITE_B)
        spdk[b[1].get_id()].nvmf_subsystem_listener_set_ana_state(
            vol.nqn, _ip(b[1]), 4402, ana="inaccessible", anagrpid=vol.ns_id)
        assert self._cycle(cluster, owner, vol) == [b[1].get_id()]
        assert _seen(spdk, b[1], vol) == "non_optimized"
        assert {_seen(spdk, n, vol) for n in a} == {"inaccessible"}


# ---------------------------------------------------------------------------
# restart and repair
# ---------------------------------------------------------------------------

def _ana_calls(rpc, nqn):
    """(anagrpid, ana) of every group state set for ``nqn``."""
    return [(c.kwargs.get("anagrpid"), c.kwargs.get("ana"))
            for c in rpc.nvmf_subsystem_listener_set_ana_state.call_args_list if c.args[0] == nqn]


def _min_cntlid(rpc, nqn):
    return [c.args[3] for c in rpc.subsystem_create.call_args_list if c.args[0] == nqn]


def _absent_until_created(rpc, nqn):
    """``nqn`` does not exist on the node until it is created there (then it
    has no listener and no namespace the probe could see)."""
    created = {"yes": False}
    default = rpc.subsystem_get.return_value

    def _get(name, *args, **kwargs):
        if name != nqn:
            return default
        return {"nqn": nqn} if created["yes"] else None

    def _create(name, *args, **kwargs):
        if name == nqn:
            created["yes"] = True
        return True

    rpc.subsystem_get.side_effect = _get
    rpc.subsystem_create.side_effect = _create


def _examines(rpc, *vols):
    """The examine of the LVS on the node brings ``vols``' bdevs back."""
    names = {n for vol in vols for n in (vol.lvol_uuid, f"{vol.lvs_name}/{vol.lvol_bdev}")}
    rpc.get_bdevs.side_effect = lambda name=None, **kwargs: [{"name": name}] if name in names else []


def _leads(rpcs, node):
    """``node`` leads, with no JC compression to wait for."""
    rpcs.lead(node)
    rpcs[node.get_id()].jc_compression_get_status.return_value = False


@pytest.fixture()
def attached():
    """Namespaces attach (add_lvol_thread); listener publication stays real."""
    with patch.object(ops, "add_lvol_thread", return_value=(True, None)) as add:
        yield add


class TestRestart:

    def test_a_remote_member_publishes_in_its_window_and_stays_fenced(
            self, db, rpcs, hub, env, attached):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        rpcs.lead(owner)
        _absent_until_created(rpcs[b[1].get_id()], vol.nqn)
        _examines(rpcs[b[1].get_id()], vol)
        assert stack._restart(db, b[1]) is True
        rpc = rpcs[b[1].get_id()]
        assert _min_cntlid(rpc, vol.nqn) == [4000]
        created = [c for c in rpc.listeners_create.call_args_list if c.args[0] == vol.nqn]
        assert created and all(c.kwargs["ana_state"] == "inaccessible" for c in created)
        assert _ana_calls(rpc, vol.nqn) == [(1, "inaccessible")]

    @pytest.mark.parametrize("member, window, state", [
        (1, 4000, "non_optimized"), (0, 3000, "optimized")])
    def test_after_a_switchover_the_active_remote_members(
            self, db, rpcs, hub, env, attached, member, window, state):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, active_site=SITE_B,
                      demoted=[SITE_A])
        rpcs.lead(b[1 - member])
        _absent_until_created(rpcs[b[member].get_id()], vol.nqn)
        _examines(rpcs[b[member].get_id()], vol)
        assert stack._restart(db, b[member]) is True
        rpc = rpcs[b[member].get_id()]
        assert _min_cntlid(rpc, vol.nqn) == [window]
        assert _ana_calls(rpc, vol.nqn) == [(1, state)]

    def test_after_a_switchover_the_home_primary_comes_back_fenced_in_window_0(
            self, db, rpcs, hub, env, attached):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, active_site=SITE_B,
                      demoted=[SITE_A])
        rpcs.lead(b[0])
        _absent_until_created(rpcs[owner.get_id()], vol.nqn)
        _examines(rpcs[owner.get_id()], vol)
        assert ops._recreate_all_lvstores_serial(_fresh(db, owner)) is True
        rpc = rpcs[owner.get_id()]
        assert _min_cntlid(rpc, vol.nqn) == [1]
        assert _ana_calls(rpc, vol.nqn) == [(1, "inaccessible")]

    def test_a_home_primary_leader_restart_on_a_demoted_site_stays_fenced(
            self, db, rpcs, hub, env, attached):
        """Between demote and promote the LVS is still led from A: the home
        primary rebuilds as the leader (_recreate_lvstore_impl), its volumes
        stay inaccessible, and so do its peers' (step 11)."""
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, demoted=[SITE_A])
        _leads(rpcs, a[1])
        _absent_until_created(rpcs[owner.get_id()], vol.nqn)
        _examines(rpcs[owner.get_id()], vol)
        with patch.object(ops, "transfer_lvs_leadership", return_value=[]):
            assert ops.recreate_lvstore(_fresh(db, owner)) is True
        rpc = rpcs[owner.get_id()]
        assert _min_cntlid(rpc, vol.nqn) == [1]
        assert _ana_calls(rpc, vol.nqn) == [(1, "inaccessible")]
        # the old leader keeps its fence (no raw non_optimized listener re-add)
        peer = rpcs[a[1].get_id()]
        assert (1, "inaccessible") in _ana_calls(peer, vol.nqn)
        assert not [c for c in peer.listeners_create.call_args_list if c.args[0] == vol.nqn]

    def test_a_home_primary_leader_restart_opens_its_site(self, db, rpcs, hub, env, attached):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        _leads(rpcs, a[1])
        _absent_until_created(rpcs[owner.get_id()], vol.nqn)
        _examines(rpcs[owner.get_id()], vol)
        with patch.object(ops, "transfer_lvs_leadership", return_value=[]):
            assert ops.recreate_lvstore(_fresh(db, owner)) is True
        assert _ana_calls(rpcs[owner.get_id()], vol.nqn) == [(1, "optimized")]
        assert (1, "non_optimized") in _ana_calls(rpcs[a[1].get_id()], vol.nqn)


class TestRepair:

    @pytest.mark.parametrize("active_site, member, window, state", [
        ("", 4, 4000, "inaccessible"),
        (SITE_B, 4, 4000, "non_optimized"),
        (SITE_B, 0, 1, "inaccessible"),
    ])
    def test_repair_follows_the_rule_and_the_window(self, db, rpcs, active_site, member, window, state):
        cluster, a, b, owner = _layout(db, active_site=active_site)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1,
                      active_site=active_site or SITE_A)
        node = _fresh(db, (*a, *b)[member])
        rpc = rpcs[node.get_id()]
        _absent_until_created(rpc, vol.nqn)
        with patch.object(ops, "add_lvol_thread", side_effect=lambda lvol, sn, lvol_ana_state:
                          ops._publish_lvol_listener(lvol, sn, rpc, lvol_ana_state)):
            # the sync-op task hands in the clamped index for the home owner (0)
            ok, err = ops.repair_lvol_registration_on_non_leader(vol, node, max(member - 1, 0))
        assert (ok, err) == (True, None)
        assert _min_cntlid(rpc, vol.nqn) == [window]
        assert _ana_calls(rpc, vol.nqn) == [(1, state)]


# ---------------------------------------------------------------------------
# failover, failback, deferred promotion, Pass 4
# ---------------------------------------------------------------------------

def _states_by_node(rpcs, nodes, nqn):
    return {n.get_id(): _ana_calls(rpcs[n.get_id()], nqn) for n in nodes}


class TestFailover:

    def test_an_active_remote_primary_lost_promotes_its_secondary(self, db, rpcs):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, active_site=SITE_B,
                      demoted=[SITE_A])
        ops.trigger_ana_failover_for_node(_fresh(db, b[0]))
        calls = _states_by_node(rpcs, (*a, *b), vol.nqn)
        assert calls[b[1].get_id()] == [(1, "optimized")]
        assert all(not v for k, v in calls.items() if k != b[1].get_id())

    def test_a_home_primary_lost_on_a_remote_led_lvs_changes_nothing(self, db, rpcs):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, active_site=SITE_B)
        ops.trigger_ana_failover_for_node(_fresh(db, owner))
        assert all(not v for v in _states_by_node(rpcs, (*a, *b), vol.nqn).values())

    def test_a_home_primary_lost_on_a_home_led_lvs_promotes_its_secondary(self, db, rpcs):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        ops.trigger_ana_failover_for_node(_fresh(db, owner))
        calls = _states_by_node(rpcs, (*a, *b), vol.nqn)
        assert calls[a[1].get_id()] == [(1, "optimized")]
        assert all(not v for k, v in calls.items() if k != a[1].get_id())

    def test_a_demoted_site_is_not_reopened_by_a_failover(self, db, rpcs):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, demoted=[SITE_A])
        ops.trigger_ana_failover_for_node(_fresh(db, owner))
        assert _ana_calls(rpcs[a[1].get_id()], vol.nqn) == [(1, "inaccessible")]

    def test_nothing_moves_while_the_leadership_moves(self, db, rpcs):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        ops.trigger_ana_failover_for_node(_fresh(db, owner))
        assert all(not v for v in _states_by_node(rpcs, (*a, *b), vol.nqn).values())

    def test_failback_demotes_the_active_secondary(self, db, rpcs):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, active_site=SITE_B)
        ops.trigger_ana_failback_for_node(_fresh(db, b[0]))
        calls = _states_by_node(rpcs, (*a, *b), vol.nqn)
        assert calls[b[1].get_id()] == [(1, "non_optimized")]
        assert all(not v for k, v in calls.items() if k != b[1].get_id())

    def test_deferred_promotion_of_a_recovered_active_secondary(self, db, rpcs):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, active_site=SITE_B)
        _update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        ops.promote_active_secondary_ana(_fresh(db, b[1]))
        assert _ana_calls(rpcs[b[1].get_id()], vol.nqn) == [(1, "optimized")]
        # an online active primary: nothing to promote
        _update(db, b[0], status=StorageNode.STATUS_ONLINE)
        rpcs[b[1].get_id()].reset_mock()
        ops.promote_active_secondary_ana(_fresh(db, b[1]))
        assert _ana_calls(rpcs[b[1].get_id()], vol.nqn) == []


class TestFanOut:

    def test_qos_reaches_every_instance(self, db, rpcs):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        assert lvol_controller.set_lvol(vol.get_id(), 1000, 0, 0, 0) is True
        for node in (*a, *b):
            rpcs[node.get_id()].bdev_set_qos_limit.assert_called_once_with(vol.top_bdev, 1000, 0, 0, 0)


class TestActivationPass4:

    def test_the_site_rule_on_every_member(self, db, rpcs):
        cluster, a, b, owner = _layout(db)
        pool = _pool(db, cluster)
        open_vol = _volume(db, cluster, owner, pool, ns_id=1)
        demoted = _volume(db, cluster, owner, pool, ns_id=1, demoted=[SITE_A])
        cluster_ops._activation_open_node_ana(owner.get_id())
        for vol, home in ((open_vol, ["optimized", "non_optimized", "non_optimized"]),
                          (demoted, ["inaccessible"] * 3)):
            calls = _states_by_node(rpcs, (*a, *b), vol.nqn)
            assert [calls[n.get_id()] for n in a] == [[(1, s)] for s in home]
            assert [calls[n.get_id()] for n in b] == [[(1, "inaccessible")]] * 3

    def test_a_volume_active_on_the_other_site_is_fenced_at_home(self, db, rpcs):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, active_site=SITE_B)
        cluster_ops._activation_open_node_ana(owner.get_id())
        calls = _states_by_node(rpcs, (*a, *b), vol.nqn)
        assert {s for v in calls.values() for _, s in v} == {"inaccessible"}


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------

def _connect_ips(db, vol_id, **kwargs):
    entries, err = lvol_controller.connect_lvol(vol_id, **kwargs)
    assert err is None, err
    return [e.ip for e in entries]


class TestConnect:

    @pytest.mark.parametrize("ftt", [2, 1])
    def test_the_paths_of_the_requested_site(self, db, ftt):
        cluster, a, b, owner = _layout(db, ftt=ftt)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        home = a[:3 if ftt == 2 else 2]
        assert _connect_ips(db, vol.get_id(), site=SITE_A) == [_ip(n) for n in home]
        assert _connect_ips(db, vol.get_id(), site=SITE_B) == [_ip(n) for n in b]

    def test_without_a_site_uses_the_led_site(self, db):
        """Regression: 2026-10-09-connect-active-site - connect required the
        caller to name the site and returned that site's paths, so a node that
        guessed its own zone could be handed the paths of a site the volume is
        not led from (ANA-inaccessible). With no site it returns the paths of
        the site the LVS is led from (lvs_active_site)."""
        cluster, a, b, owner = _layout(db)          # led from home => SITE_A
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        assert _connect_ips(db, vol.get_id()) == [_ip(n) for n in a[:3]]

    def test_without_a_site_follows_a_promote_to_the_other_site(self, db):
        """After a promote to SITE_B (lvs_active_site=SITE_B), connect with no
        site returns SITE_B's paths, not the home site's."""
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, active_site=SITE_B)
        assert _connect_ips(db, vol.get_id()) == [_ip(n) for n in b]

    def test_an_unknown_site_raises(self, db):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        with pytest.raises(SyncReplicationSiteError, match="not a site"):
            lvol_controller.connect_lvol(vol.get_id(), site="site-c")

    def test_a_site_on_a_cluster_without_sites_raises(self, db):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
        with pytest.raises(SyncReplicationSiteError, match="not a sync-replication"):
            lvol_controller.connect_lvol(vol.get_id(), site=SITE_A)

    def test_non_sync_unchanged_including_an_async_cutover(self, db):
        """Non-sync: no site, every path; a cutover_pending relationship hands
        out both ends, nothing filtered."""
        cluster = _seed_cluster(db)
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
        nodes = [stack._seed_node(db, cluster, f"n{i}", "") for i in range(4)]
        for node in nodes:
            _update(db, node, data_nics=[_nic(node.mgmt_ip)])
        nodes = [_fresh(db, n) for n in nodes]
        pool = _pool(db, cluster)
        src = _volume(db, cluster, nodes[0], pool, ns_id=1, active_site="",
                      nodes=[nodes[0].get_id(), nodes[1].get_id()])
        tgt = _volume(db, cluster, nodes[2], pool, ns_id=1, active_site="",
                      nodes=[nodes[2].get_id(), nodes[3].get_id()])
        rep = LVolReplication()
        rep.uuid = str(uuid.uuid4())
        rep.cluster_id = cluster.get_id()
        rep.source_lvol = src
        rep.target_lvol = tgt
        rep.state = LVolReplication.STATE_CUTOVER_PENDING
        rep.write_to_db(db.kv_store)
        assert sorted(_connect_ips(db, src.get_id())) == sorted(_ip(n) for n in nodes)

    def test_v1_connect_is_refused_on_a_sync_cluster(self, db):
        from simplyblock_web.api.v1 import lvol as v1_lvol
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        with Flask(__name__).test_request_context(f"/lvol/connect/{vol.get_id()}"):
            response, code = v1_lvol.connect_lvol(vol.get_id())
            assert code == 400
            assert "sync-replication" in response.get_json()["error"]


# ---------------------------------------------------------------------------
# refused on a sync cluster
# ---------------------------------------------------------------------------

@pytest.fixture()
def no_rpc():
    with patch.object(StorageNode, "rpc_client", autospec=True,
                      side_effect=AssertionError("no RPC may run")):
        yield


@pytest.fixture()
def web_client():
    from simplyblock_web.app import app
    from simplyblock_web.api.v2._auth import verify_api_token
    app.dependency_overrides[verify_api_token] = lambda: None
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.pop(verify_api_token, None)


class TestRefused:

    @pytest.mark.parametrize("create", ["create_migration", "create_batch_migration"])
    def test_migration_create_creates_nothing(self, db, no_rpc, create):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, namespaced=True)
        with pytest.raises(SyncReplicationUnsupportedError):
            getattr(migration_controller, create)(vol.get_id(), a[1].get_id())
        assert db.get_migrations(cluster.get_id()) == []
        assert db.get_migration_groups(cluster.get_id()) == []

    @pytest.mark.parametrize("namespaced", [False, True])
    def test_the_v2_subsystem_migration_route(self, db, no_rpc, web_client, namespaced):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, namespaced=namespaced)
        target = _uuid_node(db, cluster)
        response = web_client.post(
            f"/api/v2/clusters/{cluster.get_id()}/subsystems/{vol.nqn}/migrations/",
            json={"target_node_id": target.get_id()})
        assert response.status_code == 400, response.text
        assert "sync-replication" in response.text
        assert db.get_migrations(cluster.get_id()) == []
        assert db.get_migration_groups(cluster.get_id()) == []

    def test_a_batch_migration_task_is_ended_without_touching_anything(self, db, no_rpc):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1, namespaced=True)
        group = LVolMigrationGroup()
        group.uuid = str(uuid.uuid4())
        group.cluster_id = cluster.get_id()
        group.source_node_id = owner.get_id()
        group.target_node_id = a[1].get_id()
        group.target_nqn = vol.nqn
        group.status = LVolMigrationGroup.STATUS_RUNNING
        group.phase = LVolMigrationGroup.PHASE_SNAP_COPY
        group.write_to_db(db.kv_store)
        task = JobSchedule()
        task.uuid = str(uuid.uuid4())
        task.cluster_id = cluster.get_id()
        task.node_id = owner.get_id()
        task.function_name = JobSchedule.FN_LVOL_BATCH_MIG
        task.function_params = {"group_id": group.get_id()}
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)
        assert tasks_runner_batch_migration.task_runner(task) is True
        (done,) = db.get_job_tasks(cluster.get_id())
        assert done.status == JobSchedule.STATUS_DONE and "sync-replication" in done.function_result
        failed = db.get_migration_group_by_id(group.get_id())
        assert failed.status == LVolMigrationGroup.STATUS_FAILED
        assert "sync-replication" in failed.error_message

    def test_a_migration_task_is_ended_without_touching_anything(self, db, no_rpc):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        migration = LVolMigration()
        migration.uuid = str(uuid.uuid4())
        migration.cluster_id = cluster.get_id()
        migration.lvol_id = vol.get_id()
        migration.source_node_id = owner.get_id()
        migration.target_node_id = a[1].get_id()
        migration.status = LVolMigration.STATUS_RUNNING
        migration.write_to_db(db.kv_store)
        task = JobSchedule()
        task.uuid = str(uuid.uuid4())
        task.cluster_id = cluster.get_id()
        task.node_id = owner.get_id()
        task.function_name = JobSchedule.FN_LVOL_MIG
        task.function_params = {"migration_id": migration.get_id()}
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)
        assert tasks_runner_lvol_migration.task_runner(task) is True
        (done,) = db.get_job_tasks(cluster.get_id())
        assert done.status == JobSchedule.STATUS_DONE and "sync-replication" in done.function_result
        assert db.get_migration_by_id(migration.get_id()).status == LVolMigration.STATUS_FAILED

    @pytest.mark.parametrize("operation", ["suspend_lvol", "resume_lvol"])
    def test_suspend_and_resume(self, db, no_rpc, operation):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        with pytest.raises(SyncReplicationUnsupportedError):
            getattr(lvol_controller, operation)(vol.get_id())

    @pytest.mark.parametrize("verb", ["suspend", "resume"])
    def test_suspend_and_resume_through_the_cli(self, db, no_rpc, capsys, verb):
        cluster, a, b, owner = _layout(db)
        vol = _volume(db, cluster, owner, _pool(db, cluster), ns_id=1)
        with patch.object(sys, "argv", ["sbctl", "volume", verb, vol.get_id()]):
            with pytest.raises(SystemExit) as exc:
                cli_module.CLIWrapper().run()
        assert exc.value.code == 1
        assert "sync-replication" in capsys.readouterr().out

    @pytest.mark.parametrize("verb", ["suspend", "resume"])
    def test_suspend_and_resume_through_v2(self, db, no_rpc, web_client, verb):
        cluster, a, b, owner = _layout(db)
        pool = _pool(db, cluster)
        vol = _volume(db, cluster, owner, pool, ns_id=1)
        response = web_client.post(
            f"/api/v2/clusters/{cluster.get_id()}/storage-pools/{pool.get_id()}"
            f"/volumes/{vol.get_id()}/{verb}")
        assert response.status_code == 400, response.text
        assert "sync-replication" in response.text

    @pytest.mark.parametrize("verb", ["suspend", "resume"])
    @pytest.mark.parametrize("method", ["get", "post"])
    def test_v2_suspend_and_resume_work_on_a_non_sync_cluster(self, db, rpcs, web_client, verb, method):
        cluster, a, b, owner = _layout(db)
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
        pool = _pool(db, cluster)
        vol = _volume(db, cluster, owner, pool, ns_id=1)
        response = getattr(web_client, method)(
            f"/api/v2/clusters/{cluster.get_id()}/storage-pools/{pool.get_id()}"
            f"/volumes/{vol.get_id()}/{verb}")
        assert response.status_code == 200, response.text
        assert response.json() is True
        assert rpcs[owner.get_id()].nvmf_subsystem_listener_set_ana_state.called

    def test_v2_connect_without_a_site_is_a_client_error(self, db, web_client):
        cluster, a, b, owner = _layout(db)
        pool = _pool(db, cluster)
        vol = _volume(db, cluster, owner, pool, ns_id=1)
        response = web_client.get(
            f"/api/v2/clusters/{cluster.get_id()}/storage-pools/{pool.get_id()}"
            f"/volumes/{vol.get_id()}/connect")
        assert response.status_code == 400, response.text
        assert "site is required" in response.text


def _uuid_node(db, cluster):
    """A node of site A whose id is a UUID (the v2 API validates node ids)."""
    node = StorageNode()
    node.uuid = str(uuid.uuid4())
    node.cluster_id = cluster.get_id()
    node.site = SITE_A
    node.status = StorageNode.STATUS_ONLINE
    node.lvstore = "LVS_9"
    node.write_to_db(db.kv_store)
    return node
