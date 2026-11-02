"""The passive triplet of a sync-replication LVS neither exposes nor connects
a hublvol, because a hublvol never connects nodes on two sites. These tests check, against
the real FoundationDB, that the health checks, the monitor's stale port-block
remediation and port-allow (its peer gate and its own outbound and inbound
repairs) neither expect nor build a hublvol on the passive triplet.

LVS_1 of owner a0 (home a0 a1 a2 on site A) is led from site B, and site A has
no hublvol bdev, subsystem or controller for it. The controls - the same LVS
led from home, and the same layout on a cluster without sync replication -
check and repair the hublvol on site A as on any active triplet. Storage
nodes (RPC, node API), ping and the hublvol side effects are mocked; the
database never is.
"""
import time
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from simplyblock_core import distr_controller, storage_node_ops
from simplyblock_core.controllers import health_controller, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import (
    health_check_service,
    storage_node_monitor,
    tasks_runner_port_allow,
)
from simplyblock_core.services.task_runner_base import TaskDefer
from simplyblock_core.storage_node_ops import LVS_MOVING_PREFIX
from tests.integration import test_sync_replication_leaderless_health as leaderless
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
_update, _fresh, _hub_callers = stack._update, stack._fresh, stack._hub_callers

db = stack.db
hub = stack.hub
spdk = leaderless.spdk

_HUB_BDEV = "LVS_1/hublvol"
_HUB_NQN = "nqn.hub.1"


def _no_hublvol(spdk, nodes):
    """``nodes`` hold no hublvol of LVS_1: no bdev, no subsystem, no controller."""
    for node in nodes:
        rpc = spdk[node.get_id()]
        bdev, subsystem, controllers = (
            rpc.bdev_get.side_effect, rpc.subsystem_get.side_effect, rpc.bdev_nvme_controller_list.side_effect)
        rpc.bdev_get.side_effect = lambda name, real=bdev: None if name.startswith(_HUB_BDEV) else real(name)
        rpc.subsystem_get.side_effect = lambda nqn, real=subsystem: None if nqn == _HUB_NQN else real(nqn)
        rpc.bdev_nvme_controller_list.side_effect = (
            lambda name=None, real=controllers: [] if name and name.startswith(_HUB_BDEV) else real(name))


def _layout(db, spdk, site):
    """LVS_1 open on ``site`` and led there; site A holds no hublvol of it."""
    layout = leaderless._Layout(db, spdk, site, leading=True)
    _no_hublvol(spdk, layout.a)
    return layout


def _non_sync(db, layout):
    """The same layout on a cluster without sync replication: no sites, no
    remote triplets, nothing led from anywhere but home."""
    cluster = db.get_cluster_by_id(layout.cluster.get_id())
    cluster.sync_replication = False
    cluster.write_to_db(db.kv_store)
    for node in layout.nodes:
        _update(db, node, site="")
    for owner in (layout.a[0], layout.b[0]):
        _update(db, owner, lvs_active_site="", remote_primary_node_id="",
                remote_secondary_node_id="", remote_tertiary_node_id="")


#: The two layouts where a hublvol of LVS_1 is expected on site A.
_controls = pytest.mark.parametrize("non_sync", [
    pytest.param(False, id="home-led"),
    pytest.param(True, id="non-sync"),
])


def _control(db, spdk, non_sync):
    layout = _layout(db, spdk, SITE_A)
    if non_sync:
        _non_sync(db, layout)
    return layout


def _hublvol_rpcs(spdk, node):
    """The RPCs ``node`` was sent about LVS_1's hublvol."""
    rpc = spdk[node.get_id()]
    calls = [*rpc.bdev_get.call_args_list, *rpc.bdev_nvme_controller_list.call_args_list,
             *rpc.subsystem_get.call_args_list]
    return [c for c in calls if c.args and str(c.args[0]).startswith((_HUB_BDEV, _HUB_NQN))]


def _without_metadata(db, layout, metadata):
    """If ``metadata`` is False, clear the hublvol metadata from the owner's
    record too."""
    if not metadata:
        _update(db, layout.a[0], hublvol=None)


_metadata = pytest.mark.parametrize("metadata", [
    pytest.param(True, id="hublvol-recorded"),
    pytest.param(False, id="no-hublvol-record"),
])


# ---------------------------------------------------------------------------
# the two hublvol checks
# ---------------------------------------------------------------------------

class TestOwnerCheck:

    @_metadata
    def test_a_passive_owner_passes_without_an_rpc(self, db, spdk, metadata):
        layout = _layout(db, spdk, SITE_B)
        _without_metadata(db, layout, metadata)
        owner = layout.a[0]
        spdk[owner.get_id()].reset_mock()

        assert health_controller._check_node_hublvol(_fresh(db, owner)) is True

        assert spdk[owner.get_id()].mock_calls == []

    @pytest.mark.parametrize("target", [SITE_A, SITE_B])
    def test_during_a_move_the_missing_hublvol_fails(self, db, spdk, target):
        layout = _layout(db, spdk, SITE_B)
        owner = _update(db, layout.a[0], lvs_active_site=LVS_MOVING_PREFIX + target)

        assert health_controller._check_node_hublvol(owner) is False

    @_controls
    def test_control_the_missing_hublvol_fails(self, db, spdk, non_sync):
        layout = _control(db, spdk, non_sync)

        assert health_controller._check_node_hublvol(_fresh(db, layout.a[0])) is False


class TestPeerCheck:

    @_metadata
    @pytest.mark.parametrize("mode", [
        pytest.param({"auto_fix": True}, id="auto-fix"),
        pytest.param({"repair_paths": True}, id="repair-paths"),
    ])
    @pytest.mark.parametrize("slot", [1, 2])
    def test_a_passive_peer_passes_and_is_not_repaired(self, db, spdk, hub, metadata, mode, slot):
        layout = _layout(db, spdk, SITE_B)
        _without_metadata(db, layout, metadata)
        owner, peer = layout.a[0], layout.a[slot]

        assert health_controller._check_sec_node_hublvol(
            _fresh(db, peer), primary_node_id=owner.get_id(), **mode) is True

        rpc = spdk[peer.get_id()]
        rpc.bdev_nvme_controller_list.assert_not_called()
        rpc.bdev_nvme_attach_controller.assert_not_called()
        for name in ("connect_to_hublvol", "add_hublvol_failover_path"):
            assert _hub_callers(hub, name) == [], name

    @_controls
    @pytest.mark.parametrize("slot", [1, 2])
    def test_control_the_missing_controller_is_reconnected(self, db, spdk, hub, non_sync, slot):
        layout = _control(db, spdk, non_sync)
        owner, peer = layout.a[0], layout.a[slot]

        assert health_controller._check_sec_node_hublvol(
            _fresh(db, peer), auto_fix=True, primary_node_id=owner.get_id()) is False

        assert _hub_callers(hub, "connect_to_hublvol") == [peer.get_id()]


# ---------------------------------------------------------------------------
# the sn check and the health service
# ---------------------------------------------------------------------------

@contextmanager
def _node_probes():
    """Ping and the node API answer; the RPCs are the fake SPDK's."""
    with patch.object(health_controller, "_check_node_ping", return_value=True), \
            patch.object(health_controller, "_check_node_api", return_value=True):
        yield


class TestCheckNode:

    def test_a_passive_owner_is_healthy_and_its_sec_1_gets_no_hublvol(self, db, spdk, hub):
        layout = _layout(db, spdk, SITE_B)
        owner = layout.a[0]

        with _node_probes():
            assert health_controller.check_node(owner.get_id()) is True

        assert _hub_callers(hub, "create_secondary_hublvol") == []
        assert _hub_callers(hub, "connect_to_hublvol") == []
        assert _hublvol_rpcs(spdk, owner) == []

    @_controls
    def test_control_sec_1_gets_its_secondary_hublvol(self, db, spdk, hub, non_sync):
        layout = _control(db, spdk, non_sync)
        owner = layout.a[0]

        with _node_probes():
            assert health_controller.check_node(owner.get_id()) is False

        assert _hub_callers(hub, "create_secondary_hublvol") == [layout.a[1].get_id()]


class TestHealthService:

    def test_a_passive_owner_is_stored_healthy(self, db, spdk, hub):
        layout = _layout(db, spdk, SITE_B)
        owner = layout.a[0]

        health_check_service.check_node(_fresh(db, owner))

        assert _fresh(db, owner).health_check is True
        assert _hublvol_rpcs(spdk, owner) == []
        # its passive peers get no hublvol check and no hublvol connect
        for peer in layout.a[1:]:
            assert _hublvol_rpcs(spdk, peer) == []
        assert _hub_callers(hub, "connect_to_hublvol") == []

    @_controls
    def test_control_the_missing_hublvol_is_unhealthy(self, db, spdk, hub, non_sync):
        layout = _control(db, spdk, non_sync)
        owner = layout.a[0]

        health_check_service.check_node(_fresh(db, owner))

        assert _fresh(db, owner).health_check is False


# ---------------------------------------------------------------------------
# the monitor's stale port-block remediation
# ---------------------------------------------------------------------------

@pytest.fixture()
def blocked_since():
    storage_node_monitor._blocked_port_since.clear()
    yield storage_node_monitor._blocked_port_since
    storage_node_monitor._blocked_port_since.clear()


def _remediate(db, blocked_since, node, owner):
    """Run one remediation pass on ``node`` for the owner's LVS port, blocked
    for longer than STALE_PORT_BLOCK_SEC. Returns the port."""
    port = owner.get_lvol_subsys_port(owner.lvstore)
    blocked_since[(node.get_id(), port)] = time.monotonic() - 10 * storage_node_monitor.STALE_PORT_BLOCK_SEC
    storage_node_monitor._remediate_stale_port_blocks(
        db, _fresh(db, node), {port: False}, {port: owner.get_id()})
    return port


def _unblocked(spdk):
    return [(c.args[0].get_id(), c.args[1]) for c in spdk.set_port.call_args_list if not c.kwargs["block"]]


class TestStalePortBlock:

    @pytest.mark.parametrize("slot", [0, 1, 2])
    def test_a_passive_node_is_unblocked(self, db, spdk, blocked_since, slot):
        layout = _layout(db, spdk, SITE_B)
        owner, node = layout.a[0], layout.a[slot]

        port = _remediate(db, blocked_since, node, owner)

        assert _unblocked(spdk) == [(node.get_id(), port)]

    @pytest.mark.parametrize("target", [SITE_A, SITE_B])
    @pytest.mark.parametrize("slot", [0, 1])
    def test_during_a_move_the_block_stays(self, db, spdk, blocked_since, slot, target):
        layout = _layout(db, spdk, SITE_B)
        owner = _update(db, layout.a[0], lvs_active_site=LVS_MOVING_PREFIX + target)

        _remediate(db, blocked_since, layout.a[slot], owner)

        assert _unblocked(spdk) == []

    @_controls
    @pytest.mark.parametrize("slot", [0, 1])
    def test_control_the_block_stays(self, db, spdk, blocked_since, non_sync, slot):
        layout = _control(db, spdk, non_sync)

        _remediate(db, blocked_since, layout.a[slot], _fresh(db, layout.a[0]))

        assert _unblocked(spdk) == []


# ---------------------------------------------------------------------------
# port-allow's peer gate
# ---------------------------------------------------------------------------

@pytest.fixture()
def peer_gate():
    """Mock the peer gate's strict verification, forced reconnect and retry
    delays; verification and reconnect fail."""
    with patch.object(tasks_runner_port_allow, "_hublvol_verified_open", return_value=False) as verify, \
            patch.object(tasks_runner_port_allow, "_reconnect_peer_hublvol_once", return_value=False) as reconnect, \
            patch.object(tasks_runner_port_allow.time, "sleep") as sleep:
        yield verify, reconnect, sleep


class TestPeerGate:

    @_metadata
    @pytest.mark.parametrize("slot", [1, 2])
    def test_a_passive_peer_passes_on_the_first_attempt(self, db, spdk, hub, peer_gate, metadata, slot):
        layout = _layout(db, spdk, SITE_B)
        _without_metadata(db, layout, metadata)
        verify, reconnect, sleep = peer_gate

        assert tasks_runner_port_allow._verify_or_reconnect_peer_hublvol(
            _fresh(db, layout.a[slot]), _fresh(db, layout.a[0])) is True

        verify.assert_not_called()
        reconnect.assert_not_called()
        sleep.assert_not_called()
        assert _hub_callers(hub, "connect_to_hublvol") == []

    @_controls
    def test_control_the_gate_drives_its_reconnects(self, db, spdk, hub, peer_gate, non_sync):
        layout = _control(db, spdk, non_sync)
        _, reconnect, sleep = peer_gate

        assert tasks_runner_port_allow._verify_or_reconnect_peer_hublvol(
            _fresh(db, layout.a[1]), _fresh(db, layout.a[0])) is False

        assert reconnect.call_count == tasks_runner_port_allow._HUBLVOL_MAX_ATTEMPTS
        assert sleep.call_count == tasks_runner_port_allow._HUBLVOL_MAX_ATTEMPTS - 1


# ---------------------------------------------------------------------------
# port-allow's own outbound and inbound repairs, and the whole task
# ---------------------------------------------------------------------------

@pytest.fixture()
def verified():
    """Mock a successful strict verification, so a control's reconnect
    succeeds."""
    with patch.object(tasks_runner_port_allow, "_hublvol_verified_open", return_value=True) as verify:
        yield verify


def _no_hublvol_repair(hub):
    """No node built, exposed or connected a hublvol."""
    for name in ("connect_to_hublvol", "create_secondary_hublvol", "add_hublvol_failover_path",
                 "create_hublvol", "recreate_hublvol"):
        assert _hub_callers(hub, name) == [], name


_primary_status = pytest.mark.parametrize("primary_status", [
    StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE])


class TestOwnOutboundHublvols:

    @_primary_status
    @pytest.mark.parametrize("slot", [1, 2])
    def test_a_passive_follower_connects_to_no_hublvol(self, db, spdk, hub, peer_gate, slot, primary_status):
        layout = _layout(db, spdk, SITE_B)
        _update(db, layout.a[0], status=primary_status)
        verify, reconnect, sleep = peer_gate

        assert tasks_runner_port_allow._reconnect_own_sec_tert_hublvols(
            _fresh(db, layout.a[slot])) == (True, "")

        _no_hublvol_repair(hub)
        verify.assert_not_called()
        reconnect.assert_not_called()
        sleep.assert_not_called()

    @_controls
    def test_control_the_tertiary_connects_to_the_acting_leader(self, db, spdk, hub, non_sync):
        layout = _control(db, spdk, non_sync)
        owner = _update(db, layout.a[0], status=StorageNode.STATUS_OFFLINE)
        sec, tert = layout.a[1], layout.a[2]

        assert tasks_runner_port_allow._reconnect_own_sec_tert_hublvols(_fresh(db, tert)) == (True, "")

        [call] = hub["connect_to_hublvol"].call_args_list
        assert [node.get_id() for node in call.args] == [tert.get_id(), sec.get_id()]
        assert call.kwargs["role"] == "tertiary"
        assert call.kwargs["lvs_node"].get_id() == owner.get_id()

    @_controls
    def test_control_the_secondary_reconnects_to_the_primary(self, db, spdk, hub, verified, non_sync):
        layout = _control(db, spdk, non_sync)
        sec = layout.a[1]

        assert tasks_runner_port_allow._reconnect_own_sec_tert_hublvols(_fresh(db, sec)) == (True, "")

        callers = _hub_callers(hub, "connect_to_hublvol")
        assert callers and set(callers) == {sec.get_id()}


class TestOwnInboundHublvols:

    def test_a_passive_secondary_exposes_nothing(self, db, spdk, hub):
        layout = _layout(db, spdk, SITE_B)
        sec = layout.a[1]

        assert tasks_runner_port_allow._reconnect_inbound_hublvols(_fresh(db, sec)) == (True, "")

        _no_hublvol_repair(hub)
        assert _hublvol_rpcs(spdk, sec) == []

    @_controls
    def test_control_the_secondary_hublvol_and_the_tertiary_path_are_repaired(self, db, spdk, hub, non_sync):
        layout = _control(db, spdk, non_sync)
        sec, tert = layout.a[1], layout.a[2]

        assert tasks_runner_port_allow._reconnect_inbound_hublvols(_fresh(db, sec)) == (True, "")

        assert _hub_callers(hub, "create_secondary_hublvol") == [sec.get_id()]
        assert _hub_callers(hub, "add_hublvol_failover_path") == [tert.get_id()]


def _lvs_ports(layout):
    """The client ports of LVS_1 and LVS_2, which every node of the layout serves."""
    return sorted([layout.a[0].lvol_subsys_port, layout.b[0].lvol_subsys_port])


def _after_outage(db, spdk, layout, node):
    """``node``'s port-allow task after a network outage, which blocked every
    LVS port the node serves. Returns the task and the port it names."""
    for port in _lvs_ports(layout):
        spdk.block(node, port)
    port = layout.a[0].lvol_subsys_port
    task_id = tasks_controller.add_port_allow_task(layout.cluster.get_id(), node.get_id(), port)
    return db.get_task_by_id(task_id), port


@contextmanager
def _recovering(db):
    """Mock the recovery steps that run before the hublvol repairs: the node
    pings, its remote devices and journals reconnect (the ones the database
    records), and every distrib applies the device-status events."""
    def _devices(node, reattach=False):
        return list(_fresh(db, node).remote_devices)

    def _journals(node, jm_ids=None, only_node_id=None):
        return list(_fresh(db, node).remote_jm_devices)

    with _node_probes(), \
            patch.object(storage_node_ops, "_connect_to_remote_devs", side_effect=_devices), \
            patch.object(storage_node_ops, "_connect_to_remote_jm_devs", side_effect=_journals), \
            patch.object(distr_controller, "send_dev_status_event", return_value=True):
        yield


class TestPortAllowTask:

    @pytest.mark.parametrize("slot", [0, 1])
    def test_a_passive_node_gets_its_ports_and_no_hublvol(self, db, spdk, hub, peer_gate, slot):
        layout = _layout(db, spdk, SITE_B)
        node = layout.a[slot]
        task, port = _after_outage(db, spdk, layout, node)
        verify, reconnect, _ = peer_gate

        with _recovering(db):
            tasks_runner_port_allow.exec_port_allow_task(task)

        assert sorted(_unblocked(spdk)) == [(node.get_id(), p) for p in _lvs_ports(layout)]
        assert db.get_task_by_id(task.uuid).function_result == f"Port {port} allowed on node"
        fresh = _fresh(db, node)
        assert fresh.status == StorageNode.STATUS_ONLINE
        fresh.client().spdk_process_kill.assert_not_called()
        _no_hublvol_repair(hub)
        verify.assert_not_called()
        reconnect.assert_not_called()

    @_controls
    def test_control_the_secondary_repairs_its_hublvols_first(self, db, spdk, hub, verified, non_sync):
        layout = _control(db, spdk, non_sync)
        sec, tert = layout.a[1], layout.a[2]
        task, _ = _after_outage(db, spdk, layout, sec)

        with _recovering(db):
            tasks_runner_port_allow.exec_port_allow_task(task)

        callers = _hub_callers(hub, "connect_to_hublvol")
        assert callers and set(callers) == {sec.get_id()}
        assert _hub_callers(hub, "create_secondary_hublvol") == [sec.get_id()]
        assert _hub_callers(hub, "add_hublvol_failover_path") == [tert.get_id()]
        assert sorted(_unblocked(spdk)) == [(sec.get_id(), p) for p in _lvs_ports(layout)]

    @_controls
    def test_control_the_owner_waits_for_its_hublvol(self, db, spdk, hub, non_sync):
        layout = _control(db, spdk, non_sync)
        task, _ = _after_outage(db, spdk, layout, layout.a[0])

        with _recovering(db), pytest.raises(TaskDefer, match="Node hublvol check fail"):
            tasks_runner_port_allow.exec_port_allow_task(task)

        assert _unblocked(spdk) == []


def test_the_non_sync_control_has_no_sync_left(db, spdk):
    layout = _control(db, spdk, non_sync=True)
    cluster = db.get_cluster_by_id(layout.cluster.get_id())
    assert cluster.sync_replication is False and cluster.status == Cluster.STATUS_ACTIVE
    for node in layout.nodes:
        fresh = _fresh(db, node)
        assert (fresh.site, fresh.lvs_active_site, fresh.remote_primary_node_id) == ("", "", "")
