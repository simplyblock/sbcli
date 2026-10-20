"""The background services facing an ONLINE node of a site a sync-replication
cluster records lost, against the real FoundationDB. Such a node came back
through the restart without lvstores and holds no instance until the site
activation builds them; the other site serves meanwhile (cluster DEGRADED).

What reads it: the storage-node monitor's port check and the lvol monitor's
owner-side check skip a node whose lvstore is not ready; the health check
service looks at its lvstore chain and repairs nothing on it; the device
migration task of its distribs waits behind the leadership probe, as every
migration task does on a SUSPENDED cluster on main. The lvol monitor's
non-leader self-heal and the deferred lvol registration (FN_LVOL_SYNC_OP) treat
it as absent - the answer an OFFLINE member gets - instead of creating the
volume's subsystem on it.

Storage nodes are mocked at the RPC boundary (the RPC mocks of
test_sync_replication_lvs_stack, the stateful nvmf target of
test_sync_replication_lvol_publish, the healthy fake SPDK of
test_sync_replication_leaderless_health); the database, the task queue and the
status writes never are.
"""
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core import utils
from simplyblock_core.controllers import (
    health_controller,
    lvol_controller,
    tasks_controller,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import (
    health_check_service,
    lvol_monitor,
    storage_node_monitor,
    tasks_runner_migration,
    tasks_runner_sync_lvol_del,
)
from simplyblock_core.utils import ttl_cache
from tests.integration import test_sync_replication_leaderless_health as healthy
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack
from tests.integration import test_sync_replication_site_restart as site
from tests.integration.task_driver import drive

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
DRAINING, DOWN = Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN
FENCING, DONE = Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DONE
ONLINE = StorageNode.STATUS_ONLINE
RECORDED = [DOWN, FENCING, DONE]

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
links = site.links
checks = healthy.checks
_fresh, _update = stack._fresh, stack._update
_layout, _empty = site._layout, site._empty


@pytest.fixture(autouse=True)
def _clean_caches():
    ttl_cache.invalidate_all()
    yield
    ttl_cache.invalidate_all()


def _serving(db, state=DOWN, **kwargs):
    """site._layout (a0 owns LVS_1 led from A, b0 owns LVS_2 led from A, b1
    owns LVS_3 led on B; A ONLINE without lvstores) with what a volume needs on
    every node: a data NIC, room for volumes, the ports of every LVS it hosts."""
    cluster, a, b = _layout(db, state, **kwargs)
    for node in (*a, *b):
        _update(db, node, data_nics=[publish._nic(node.mgmt_ip)], max_lvol=100)
    for node in (*a, *b):
        _update(db, node, lvstore_ports=ops._derive_lvstore_ports(_fresh(db, node), None, db))
    return (db.get_cluster_by_id(cluster.get_id()),
            [_fresh(db, n) for n in a], [_fresh(db, n) for n in b])


def _record(db, cluster, state):
    fresh = db.get_cluster_by_id(cluster.get_id())
    fresh.lost_site, fresh.lost_site_state = (SITE_A, state) if state else ("", "")
    fresh.write_to_db(db.kv_store)
    return fresh


# ---------------------------------------------------------------------------
# the storage-node monitor's port check
# ---------------------------------------------------------------------------

class TestPortCheck:

    @pytest.mark.parametrize("state", RECORDED)
    @pytest.mark.parametrize("index", [0, 1, 2])
    def test_the_port_check_skips_an_empty_node(self, db, rpcs, state, index):
        """No LVS port of an empty node is checked and its data NICs are not
        pinged (storage_node_monitor.node_port_check_fun reads only a node
        whose lvstore is ready): the check passes, the node stays ONLINE."""
        cluster, a, b = _layout(db, state)
        with patch.object(StorageNode, "client", autospec=True) as client:
            assert storage_node_monitor.node_port_check_fun(_fresh(db, a[index])) is True
        client.assert_not_called()
        rpcs[a[index].get_id()].nvmf_get_blocked_ports.assert_not_called()
        assert _fresh(db, a[index]).status == ONLINE


# ---------------------------------------------------------------------------
# the lvol monitor
# ---------------------------------------------------------------------------

def _volume_on(db, cluster, owner, active_site):
    vol = publish._volume(db, cluster, owner, publish._pool(db, cluster), ns_id=1,
                          active_site=active_site)
    return db.get_lvol_by_id(vol.get_id())


def _published_on(db, spdk, cluster, owner, members, active_site=SITE_B):
    """A volume of ``owner``'s LVS registered on ``members`` (the serving
    site): the leader first, then every other path, as add_lvol_ha does."""
    vol = _volume_on(db, cluster, owner, active_site)
    publish._register_everywhere(db, vol, owner, members)
    return db.get_lvol_by_id(vol.get_id())


def _monitor_pass(db, cluster, owner, vol):
    lvol_monitor.check_node(db.get_cluster_by_id(cluster.get_id()), _fresh(db, owner), [vol],
                            subsys_check=True)


class TestLvolMonitor:

    def test_the_ana_repair_skips_an_empty_owner(self, db, publish_spdk):
        """A volume of LVS_1, owned by a0 of the recorded site: the owner's
        lvstore is not ready, so the pass checks no path of it and repairs
        nothing (lvol_monitor.check_node: the owner-side skip)."""
        cluster, a, b = _serving(db, DOWN)
        vol = _volume_on(db, cluster, a[0], SITE_A)
        with patch.object(health_controller, "check_subsystem",
                          side_effect=health_controller.check_subsystem) as probe:
            _monitor_pass(db, cluster, a[0], vol)
        probe.assert_not_called()
        for node in (*a, *b):
            assert publish_spdk[node.get_id()].subsystems == {}
        assert db.get_lvol_by_id(vol.get_id()).health_check == vol.health_check

    @pytest.mark.parametrize("state", RECORDED)
    @pytest.mark.parametrize("owner_index", [1, 0], ids=["led-on-B", "led-from-A"])
    def test_no_subsystem_is_created_on_an_empty_member(self, db, publish_spdk, state, owner_index):
        """A volume of an LVS owned on B (LVS_3 led on B, LVS_2 led from A) has
        paths on every instance, A's remote triplet included. The pass checks
        and repairs B's paths - a follower on B that lost its registration gets
        it back; an empty member of A counts as absent - like an OFFLINE one -
        so no subsystem, namespace or listener is made on it."""
        cluster, a, b = _serving(db, state)
        owner = b[owner_index]
        vol = _published_on(db, publish_spdk, cluster, owner, b)
        assert set(vol.nodes) == {n.get_id() for n in (*a, *b)}
        follower = next(n for n in b if n.get_id() != owner.get_id())
        del publish_spdk[follower.get_id()].subsystems[vol.nqn]

        _monitor_pass(db, cluster, owner, vol)

        for node in a:
            assert publish_spdk[node.get_id()].subsystems == {}
        target = publish_spdk[follower.get_id()].subsystems[vol.nqn]
        assert [ns["nsid"] for ns in target["namespaces"]] == [vol.ns_id]
        assert _fresh(db, a[0]).status == ONLINE

    @pytest.mark.parametrize("case", ["no record", "draining", "no sync replication"])
    def test_a_member_outside_a_record_is_repaired(self, db, publish_spdk, case):
        """The control of the absent-member skip: the same ONLINE nodes of A
        without their subsystem are repaired when nothing records them lost
        down / fencing / done - no record, a site still draining (its nodes
        not shut down yet hold their instances), a cluster without sync
        replication carrying the same fields."""
        state = {"no record": "", "draining": DRAINING}.get(case, DOWN)
        cluster, a, b = _serving(db, state, sync=case != "no sync replication")
        vol = _published_on(db, publish_spdk, cluster, b[1], b)

        _monitor_pass(db, cluster, b[1], vol)

        for node in a:
            assert vol.nqn in publish_spdk[node.get_id()].subsystems


@pytest.fixture()
def publish_spdk():
    """test_sync_replication_lvol_publish's stateful nvmf target per node."""
    targets = publish._Targets()
    with patch.object(StorageNode, "rpc_client", autospec=True,
                      side_effect=lambda self, *args, **kwargs: targets[self.get_id()]), \
            patch.object(lvol_controller, "_create_bdev_stack", return_value=(True, None)):
        yield targets


# ---------------------------------------------------------------------------
# the deferred lvol registration (FN_LVOL_SYNC_OP)
# ---------------------------------------------------------------------------

def _sync_op(db, cluster, node, vol, op="register"):
    task_id = tasks_controller.add_lvol_sync_op_task(
        cluster.get_id(), node.get_id(), vol.get_id(), op,
        secondary_index=lvol_controller._lvol_secondary_index(vol, node))
    assert task_id
    return task_id


def _run(db, task_id, passes=3):
    """``passes`` driver passes of the sync-op runner over the task."""
    for _ in range(passes):
        drive(tasks_runner_sync_lvol_del.SPEC, db.get_task_by_id(task_id))
    return db.get_task_by_id(task_id)


@pytest.fixture()
def alerts():
    """The repeated-failure alerts of the sync-op runner, as a callable."""
    with patch.object(tasks_runner_sync_lvol_del.events_controller, "log_event_cluster") as log, \
            patch.dict(tasks_runner_sync_lvol_del._last_alerted_failure, clear=True):
        yield lambda: [c for c in log.call_args_list
                       if c.kwargs.get("event") == "LVOL_TASK_FAILING_REPEATEDLY"]


class TestDeferredRegistration:

    @pytest.mark.parametrize("state", RECORDED)
    def test_a_registration_waits_on_an_empty_member(self, db, publish_spdk, alerts, state):
        """A register task for a1 (a remote-triplet member of LVS_3, led on B):
        while A is recorded the task waits like one for an OFFLINE node - no
        subsystem, namespace or listener on a1, never RUNNING, not counted as a
        failure (retry unchanged, no repeated-failure alert) however often the
        runner passes, and a1 stays ONLINE."""
        cluster, a, b = _serving(db, state)
        vol = _published_on(db, publish_spdk, cluster, b[1], b)
        task_id = _sync_op(db, cluster, a[1], vol)

        task = _run(db, task_id)

        assert task.status == JobSchedule.STATUS_SUSPENDED
        assert task.retry == 0
        assert "recorded site" in task.function_result
        assert publish_spdk[a[1].get_id()].subsystems == {}
        assert alerts() == []
        assert _fresh(db, a[1]).status == ONLINE

    def test_a_waiting_registration_runs_once_returned(self, db, publish_spdk, alerts):
        """A task queued before the site was recorded waits while the record
        stands, then registers the volume on a1 once the record is cleared."""
        cluster, a, b = _serving(db, "")
        vol = _published_on(db, publish_spdk, cluster, b[1], b)
        task_id = _sync_op(db, cluster, a[1], vol)
        _record(db, cluster, DOWN)

        assert _run(db, task_id).status == JobSchedule.STATUS_SUSPENDED
        assert publish_spdk[a[1].get_id()].subsystems == {}

        assert db.clear_lost_site(cluster.get_id(), SITE_A, (DOWN,), [n.get_id() for n in a],
                                  lambda cl, nodes: []) == []
        task = _run(db, task_id, passes=1)

        assert task.status == JobSchedule.STATUS_DONE
        target = publish_spdk[a[1].get_id()].subsystems[vol.nqn]
        assert [ns["nsid"] for ns in target["namespaces"]] == [vol.ns_id]
        assert alerts() == []

    def test_a_resize_waits_on_an_empty_member(self, db, publish_spdk, alerts):
        cluster, a, b = _serving(db, DOWN)
        vol = _published_on(db, publish_spdk, cluster, b[1], b)
        task_id = _sync_op(db, cluster, a[1], vol, op="resize")

        task = _run(db, task_id)

        assert (task.status, task.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        assert "bdev_lvol_resize" not in publish_spdk[a[1].get_id()].others
        assert alerts() == []

    def test_without_sync_replication_it_registers(self, db, publish_spdk):
        cluster, a, b = _serving(db, DOWN, sync=False)
        vol = _published_on(db, publish_spdk, cluster, b[1], b)
        task_id = _sync_op(db, cluster, a[1], vol)

        assert _run(db, task_id, passes=1).status == JobSchedule.STATUS_DONE
        assert vol.nqn in publish_spdk[a[1].get_id()].subsystems


class TestCreateQueuesTheEmptyMembers:

    def test_a_create_queues_each_empty_member(self, db, rpcs, links):
        """The producer of those tasks: a volume created on LVS_3 (led on B)
        while A is recorded registers on B at once; each member of A is "skip"
        (an absent member) and gets a durable register task instead."""
        cluster, a, b = _serving(db, DOWN)
        _empty(rpcs, a)
        rpcs.lead(b[1])
        registered = []

        def _add(lvol, node, is_primary=True, secondary_index=0, **kwargs):
            registered.append(node.get_id())
            return {"uuid": "bdev-uuid", "driver_specific": {"lvol": {"blobid": 7}}}, None

        with patch.object(lvol_controller, "add_lvol_on_node", side_effect=_add), \
                patch.object(lvol_controller, "set_lvol"):
            vol_id, err = lvol_controller.add_lvol_ha(
                "vol-1", 2 * 1024 ** 3, b[1].get_id(), "ha", publish._pool(db, cluster).get_id())

        assert err is None, err
        assert sorted(registered) == sorted(n.get_id() for n in b)
        queued = [(t.node_id, t.function_params["op"], t.function_params["lvol_id"])
                  for t in db.get_job_tasks(cluster.get_id())
                  if t.function_name == JobSchedule.FN_LVOL_SYNC_OP]
        assert sorted(queued) == sorted((n.get_id(), "register", vol_id) for n in a)


# ---------------------------------------------------------------------------
# the health check service
# ---------------------------------------------------------------------------

#: The bdev / controller names of an LVS: its distribs, raid, lvstore and hublvol.
_LVS_OBJECTS = ("distrib_", "raid0_", "LVS_")

#: The RPCs that build an LVS object or a path on a node.
_BUILDING_RPCS = ("bdev_distrib_create", "bdev_raid_create", "create_lvstore",
                  "bdev_lvol_set_leader", "bdev_lvol_create_hublvol", "subsystem_create",
                  "nvmf_subsystem_add_ns", "nvmf_subsystem_add_ns2", "listeners_create",
                  "bdev_nvme_attach_controller", "nvmf_port_unblock", "nvmf_port_block")


@pytest.fixture()
def health_spdk(db):
    """test_sync_replication_leaderless_health's healthy fake SPDK."""
    spdk = healthy._Spdk(db)
    api = MagicMock(name="node-api")
    api.is_live.return_value = (True, None)
    api.spdk_process_is_up.return_value = (True, None)
    api.ping_ip.return_value = (True, None)
    with patch.object(StorageNode, "rpc_client", autospec=True,
                      side_effect=lambda self, *args, **kwargs: spdk[self.get_id()]), \
            patch.object(StorageNode, "client", autospec=True, return_value=api), \
            patch.object(utils, "ping_host", return_value=True), \
            patch.object(ops, "ensure_nvme_options", return_value=(True, {})):
        yield spdk


def _emptied(spdk, node):
    """``node`` has its devices, its JM and its links, and no LVS object: no
    distrib, raid, lvstore or hublvol bdev, no hublvol subsystem, no controller
    to a hublvol, no lvstore to report."""
    rpc = spdk[node.get_id()]
    devices = rpc.bdev_get.side_effect
    controllers = rpc.bdev_nvme_controller_list.side_effect
    subsystems = rpc.subsystem_get.side_effect
    rpc.bdev_get.side_effect = lambda name: None if name.startswith(_LVS_OBJECTS) else devices(name)
    rpc.get_lvstore.side_effect = lambda name: None
    rpc.bdev_nvme_controller_list.side_effect = lambda name=None: (
        [] if name and name.startswith(_LVS_OBJECTS) else controllers(name))
    rpc.subsystem_get.side_effect = lambda nqn: None if nqn.startswith("nqn.hub.") else subsystems(nqn)


@pytest.fixture()
def lvstore_checks():
    """(node id, id of the node whose stack it is checked against) of every
    distrib stack check, the real one called through."""
    seen = []
    real = health_controller._check_node_lvstore

    def _spy(lvstore_stack, node, auto_fix=False, node_bdev_namesss=None, stack_src_node=None):
        seen.append((node.get_id(), (stack_src_node or node).get_id()))
        return real(lvstore_stack, node, auto_fix=auto_fix, node_bdev_namesss=node_bdev_namesss,
                    stack_src_node=stack_src_node)

    with patch.object(health_controller, "_check_node_lvstore", side_effect=_spy):
        yield seen


class TestHealthPass:

    @pytest.mark.parametrize("state", RECORDED)
    def test_the_health_pass_builds_nothing_on_it(
            self, db, health_spdk, hub, checks, lvstore_checks, state):
        """A health pass over every node: the empty nodes of A are checked -
        a0's own stack and hublvol, a1 / a2 as a0's secondaries - and every
        repair it may run stays off them: no LVS object, path or port fence is
        made on a node of A, no hublvol is attached to or from one, no task is
        queued, no node changes status."""
        cluster, a, b = _layout(db, state)
        nodes = [_fresh(db, n) for n in (*a, *b)]
        for node in nodes:
            _update(db, node, lvstore_ports=ops._derive_lvstore_ports(_fresh(db, node), None, db))
        healthy._mesh(db, [_fresh(db, n) for n in nodes])
        for node in nodes:
            health_spdk.add(node)
        for node in a:
            _emptied(health_spdk, node)

        for node in nodes:
            health_check_service.check_node(_fresh(db, node))

        a_ids = {n.get_id() for n in a}
        assert (a[0].get_id(), a[0].get_id()) in lvstore_checks
        assert {(a[1].get_id(), a[0].get_id()), (a[2].get_id(), a[0].get_id())} <= set(lvstore_checks)
        assert (a[0].get_id(), False) in checks.own
        assert {(a[1].get_id(), a[0].get_id(), False),
                (a[2].get_id(), a[0].get_id(), False)} <= set(checks.sec)
        for node in a:
            rpc = health_spdk[node.get_id()]
            for method in _BUILDING_RPCS:
                getattr(rpc, method).assert_not_called()
        for name, mock in hub.items():
            for c in mock.call_args_list:
                involved = {x.get_id() for x in (*c.args, *c.kwargs.values()) if isinstance(x, StorageNode)}
                assert not a_ids & involved, name
        health_spdk.set_port.assert_not_called()
        assert db.get_job_tasks(cluster.get_id()) == []
        for node in nodes:
            assert _fresh(db, node).status == ONLINE
        for node in a:
            assert _fresh(db, node).lvstore_status == ""

    def test_a_failed_check_keeps_the_node_online(self, db, health_spdk, hub):
        """The verdict on an empty node is its health_check only: False (its
        lvstore chain is missing) - the status stays ONLINE."""
        cluster, a, b = _layout(db, DOWN)
        nodes = [_fresh(db, n) for n in (*a, *b)]
        healthy._mesh(db, nodes)
        for node in nodes:
            health_spdk.add(node)
        for node in a:
            _emptied(health_spdk, node)

        health_check_service.check_node(_fresh(db, a[0]))

        fresh = _fresh(db, a[0])
        assert (fresh.health_check, fresh.status, fresh.lvstore_status) == (False, ONLINE, "")


# ---------------------------------------------------------------------------
# the device migration tasks the restart queues
# ---------------------------------------------------------------------------

def _settled(db, nodes):
    """Every node ONLINE for ten minutes (past the runner's one-minute wait)."""
    since = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    for node in nodes:
        _update(db, node, online_since=since)


def _migration_tasks(db, cluster, node):
    return [t for t in db.get_job_tasks(cluster.get_id())
            if t.function_name == JobSchedule.FN_DEV_MIG and t.node_id == node.get_id()]


_MIGRATION_RPCS = ("distr_migration_expansion_start", "distr_migration_status")


class TestDeviceMigration:

    @pytest.mark.parametrize("case", [*RECORDED, "suspended"])
    def test_migration_on_an_empty_node_waits(self, db, rpcs, case):
        """The restart of a0 queues a migration task per distrib of every node
        (tasks_controller.add_device_mig_task_for_node). a0's own tasks wait on
        an empty a0 - the leadership probe finds no lvstore - pass after pass,
        as every task waits on main's SUSPENDED cluster ("suspended": no
        record, no sync replication): no migration RPC, never done, retried
        without limit, a0 ONLINE."""
        if case == "suspended":
            cluster, a, b = _layout(db, "", status=Cluster.STATUS_SUSPENDED, sync=False)
        else:
            cluster, a, b = _layout(db, case)
        _settled(db, (*a, *b))
        _empty(rpcs, a)
        rpcs.lead(b[1])
        assert tasks_controller.add_device_mig_task_for_node(a[0].get_id())

        own = _migration_tasks(db, cluster, a[0])
        assert len(own) == 2
        for _ in range(3):
            for task in own:
                assert drive(tasks_runner_migration.SPEC, task).status == JobSchedule.STATUS_SUSPENDED

        for task in _migration_tasks(db, cluster, a[0]):
            assert task.status == JobSchedule.STATUS_SUSPENDED
            assert task.max_retry == -1
        for method in _MIGRATION_RPCS:
            getattr(rpcs[a[0].get_id()], method).assert_not_called()
        assert _fresh(db, a[0]).status == ONLINE

    def test_the_serving_leader_migrates(self, db, rpcs):
        """The control of the wait: b1 leads LVS_3 on the serving site, its
        task of the same restart starts the migration."""
        cluster, a, b = _layout(db, DOWN)
        _settled(db, (*a, *b))
        _empty(rpcs, a)
        rpcs.lead(b[1])
        assert tasks_controller.add_device_mig_task_for_node(a[0].get_id())

        task = _migration_tasks(db, cluster, b[1])[0]
        drive(tasks_runner_migration.SPEC, task)

        rpcs[b[1].get_id()].distr_migration_expansion_start.assert_called_once()
