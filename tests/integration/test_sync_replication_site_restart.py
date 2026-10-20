"""A node of a site a sync-replication cluster records lost, against the real
FoundationDB: its restart brings it back ONLINE without lvstores (main's
restart of a cluster that serves nothing), the triplet readers count it as
absent while the record stands, and no node restarts while a site activation
runs.

The restart runs the real ``_restart_storage_node_impl`` with its boundary
mocked: the node API, the RPC of every node (``test_sync_replication_lvs_stack``
rpcs), the device / JM connects and the broadcasts; the node and cluster
records, the restart claim and the status writes are real. The triplet
readers run with the real ``_check_peer_disconnected`` / ``_rebuild_anchor``;
the data-plane vote is answered at its boundary (``_count_data_plane_votes_uncached``).
The pure flag predicate is in tests/unit/test_activation_gate_rules.py.
"""
import sys
import uuid
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import health_controller, tasks_controller
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCConnectionError
from simplyblock_core.services import storage_node_monitor as monitor
from simplyblock_core.services import tasks_runner_restart
from simplyblock_core.services import tasks_runner_sync_promote as runner
from tests.integration import test_sync_replication_lvs_stack as stack
from tests.integration.task_driver import handler_reached

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
DRAINING, DOWN = Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN
FENCING, DONE = Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DONE
ONLINE, OFFLINE = StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
_fresh, _update = stack._fresh, stack._update

#: The RPCs that build or ask an LVS instance: the stack of _create_bdev_stack /
#: recreate_lvstore* and the leadership probe.
LVS_RPCS = ("bdev_distrib_create", "bdev_raid_create", "bdev_examine", "bdev_lvol_set_lvs_opts",
            "bdev_lvol_set_leader", "create_lvstore", "get_lvstore")


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------

def _cluster(db, state=DOWN, *, status=Cluster.STATUS_DEGRADED, sync=True):
    """A cluster stored ``status`` with site A recorded ``state`` ("" = no record)."""
    cluster = stack._seed_cluster(db, lost_site=SITE_A if state else "")
    cluster.lost_site_state = state
    cluster.status = status
    cluster.sync_replication = sync
    cluster.cluster_vip = "10.0.0.1"
    cluster.write_to_db(db.kv_store)
    return cluster


def _layout(db, state=DOWN, *, a_status=ONLINE, **kwargs):
    """3 + 3 nodes: a0 owns LVS_1 (led from A, its remote triplet b0 b1 b2
    passive), b0 owns LVS_2 (remote triplet a0 a1 a2, led from A), b1 owns
    LVS_3 (home b1 b2 b0, remote a1 a2 a0, led on B). The nodes of A are in
    ``a_status``, without lvstores - the state their restart leaves them in."""
    cluster = _cluster(db, state, **kwargs)
    a, b = stack._sites(db, cluster)
    stack._make_owner(db, a[0], a, b, lvs_id=1)
    stack._make_owner(db, b[0], b, a, lvs_id=2)
    _update(db, b[0], lvs_active_site=SITE_A)
    stack._make_owner(db, b[1], [b[1], b[2], b[0]], [a[1], a[2], a[0]], lvs_id=3)
    for node in a:
        _update(db, node, status=a_status, lvstore_status="")
    return cluster, [_fresh(db, n) for n in a], [_fresh(db, n) for n in b]


def _empty(rpcs, nodes):
    """``nodes`` hold no instance: the leadership probe gets no lvstore."""
    for node in nodes:
        rpcs[node.get_id()].get_lvstore.return_value = None


def _restartable(db, node, status=OFFLINE):
    """``node`` as a restart finds it: in ``status``, with a CPU mask and a
    serial number per device (what the device discovery matches on)."""
    fresh = _fresh(db, node)
    fresh.status = status
    fresh.spdk_cpu_mask = "0x3"
    for i, dev in enumerate(fresh.nvme_devices):
        dev.serial_number = f"SN-{fresh.get_id()}-{i}"
    fresh.write_to_db(db.kv_store)
    return fresh


# ---------------------------------------------------------------------------
# the restart boundary
# ---------------------------------------------------------------------------

class _Restart:
    """The boundary of _restart_storage_node_impl, each mock by name. The LVS
    path is a tripwire: recreate_all_lvstores answers False (the restart then
    aborts), so a test that expects the LVS path asserts it was taken."""

    def __init__(self):
        self.api = MagicMock(name="node-api")
        self.api.info.return_value = ({
            "memory_details": {"total": 64 << 30, "free": 32 << 30, "huge_total": 8 << 30},
            "spdk_pcie_list": []}, None)
        self.api.spdk_process_start.return_value = (True, None)
        self.api.read_allowed_list.return_value = ([], None)

    @staticmethod
    def _found(rpc, snode, ssds):
        """The device discovery finds the node's devices again (by serial)."""
        out = []
        for db_dev in snode.nvme_devices:
            dev = NVMeDevice()
            dev.uuid = str(uuid.uuid4())
            dev.serial_number = db_dev.serial_number
            dev.nvme_bdev = f"{db_dev.serial_number}n1"
            out.append(dev)
        return out

    def patches(self):
        return {
            "check_api": patch.object(health_controller, "_check_node_api", return_value=True),
            "client": patch.object(StorageNode, "client", autospec=True,
                                   side_effect=lambda node, *args, **kwargs: self.api),
            "stop": patch.object(ops, "ensure_spdk_stopped"),
            "sleep": patch.object(ops.time, "sleep"),
            "discover": patch.object(ops.utils, "addNvmeDevices", side_effect=self._found),
            "prepare": patch.object(ops, "_prepare_cluster_devices_on_restart", return_value=True),
            "connect": patch.object(ops, "_connect_to_remote_devs", return_value=[]),
            "connect_jm": patch.object(ops, "_connect_to_remote_jm_devs", return_value=[]),
            "dev_events": patch.object(ops.distr_controller, "send_dev_status_event"),
            "node_events": patch.object(ops.distr_controller, "send_node_status_event"),
            "maps": patch.object(ops.distr_controller, "send_cluster_map_to_distr", return_value=True),
            "jm_online": patch.object(ops.device_controller, "set_jm_device_state"),
            "device_events": patch.object(ops, "device_events"),
            "storage_events": patch.object(ops, "storage_events"),
            "ana": patch.object(ops, "trigger_ana_failback_for_node"),
            "refresh": patch.object(ops, "_refresh_cluster_maps_after_node_recovery"),
            "migration": patch.object(ops.tasks_controller, "add_device_mig_task_for_node"),
            "reconnect": patch.object(ops, "_reconnect_peers_to_restarted_node", return_value=(set(), set())),
            "coverage": patch.object(ops, "_verify_online_device_coverage", return_value=[]),
            "recreate": patch.object(ops, "recreate_all_lvstores", return_value=False),
            "kill": patch.object(ops, "_kill_spdk_until_dead"),
        }


@pytest.fixture()
def restart():
    r = _Restart()
    with ExitStack() as es:
        for name, p in r.patches().items():
            setattr(r, name, es.enter_context(p))
        yield r


def _took_the_lvs_path(r):
    return r.reconnect.called and r.recreate.called


# ---------------------------------------------------------------------------
# the restart of a node of the recorded site
# ---------------------------------------------------------------------------

class TestRestartOfARecordedSite:

    @pytest.mark.parametrize("state", [DOWN, FENCING, DONE, DRAINING])
    @pytest.mark.parametrize("index", [0, 1])
    def test_a_node_comes_back_online_without_lvstores(self, db, rpcs, hub, restart, state, index):
        """A site-aware journal counts only its local JMs (ultra ae6d5791,
        DISTR_v2 alg_journal.cpp:2316 -> alg_journal_site_rules.hpp:123
        jc_site_outage_detect) against c_jc_ha_min_required_jms = 2
        (alg_journal.hpp:108). The first node of a recorded site to come back
        sees one local JM of three: a restart that builds its LVS stack makes
        the JC abort the node ("JC aborts the node due to network outage").
        So it comes back as main's nodes do under a cluster that serves
        nothing: devices and its own JM up, its links made, the peers
        connected back to it, ONLINE - and no LVS instance. ``draining``: the restarts of the site are paused, a direct
        one takes the same branch."""
        cluster, a, b = _layout(db, state, a_status=OFFLINE)
        node = _restartable(db, a[index])
        assert ops._restart_storage_node_impl(node.get_id()) is True
        node = _fresh(db, node)
        assert (node.status, node.lvstore_status) == (ONLINE, "")
        restart.recreate.assert_not_called()
        restart.reconnect.assert_not_called()
        back = {c.args[0].get_id() for c in restart.connect.call_args_list
                if c.kwargs.get("only_node_id") == node.get_id()}
        assert back == {n.get_id() for n in b}
        assert {c.args[0].get_id() for c in restart.dev_events.call_args_list} == {
            d.get_id() for d in node.nvme_devices}
        restart.jm_online.assert_called_once_with(node.jm_device.get_id(), JMDevice.STATUS_ONLINE)
        rpc = rpcs[node.get_id()]
        for name in LVS_RPCS:
            assert not getattr(rpc, name).called, name
        for name, mock in hub.items():
            assert not mock.called, name

    def test_serving_site_node_rebuilds_its_lvstores(self, db, rpcs, restart):
        cluster, a, b = _layout(db, DOWN, a_status=OFFLINE)
        node = _restartable(db, b[1])
        assert ops._restart_storage_node_impl(node.get_id()) is False
        assert _took_the_lvs_path(restart)

    def test_node_without_record_rebuilds_lvstores(self, db, rpcs, restart):
        cluster, a, b = _layout(db, "", a_status=OFFLINE)
        node = _restartable(db, a[1])
        assert ops._restart_storage_node_impl(node.get_id()) is False
        assert _took_the_lvs_path(restart)


class TestRestartWithoutSyncReplication:
    """A restart on a cluster without sync replication follows the stored
    cluster status alone."""

    def _nodes(self, db, status):
        cluster = _cluster(db, "", status=status, sync=False)
        nodes = [stack._seed_node(db, cluster, f"n{i}", "") for i in range(3)]
        return cluster, nodes

    @pytest.mark.parametrize("status", [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED])
    def test_a_serving_cluster_rebuilds_the_lvstores(self, db, rpcs, restart, status):
        cluster, nodes = self._nodes(db, status)
        node = _restartable(db, nodes[0])
        assert ops._restart_storage_node_impl(node.get_id()) is False
        assert _took_the_lvs_path(restart)

    def test_a_suspended_cluster_restarts_without_lvstores(self, db, rpcs, restart):
        cluster, nodes = self._nodes(db, Cluster.STATUS_SUSPENDED)
        node = _restartable(db, nodes[0])
        assert ops._restart_storage_node_impl(node.get_id()) is True
        assert (_fresh(db, node).status, _fresh(db, node).lvstore_status) == (ONLINE, "")
        restart.recreate.assert_not_called()


# ---------------------------------------------------------------------------
# the nodes of the recorded site count as absent
# ---------------------------------------------------------------------------

@pytest.fixture()
def links(request):
    """stack.env with the real connectivity classification: the data-plane
    vote, its boundary, reports every node connected. ``_check_peer_disconnected``
    imports storage_node_monitor at call time, so the module patched here is
    pinned in ``sys.modules`` for the test - whatever another test left there."""
    e = stack._Env()
    e.route_fences(request)
    e.votes = MagicMock(name="votes", side_effect=lambda node: (0, 3))
    with patch.dict(sys.modules, {monitor.__name__: monitor}), \
            patch.object(ops, "_connect_to_remote_devs", return_value=[]), \
            patch.object(ops, "_connect_to_remote_jm_devs", return_value=[]), \
            patch.object(ops.distr_controller, "send_cluster_map_to_distr", return_value=True), \
            patch.object(monitor, "_count_data_plane_votes_uncached", e.votes), \
            patch.object(ops, "tcp_ports_events"), \
            patch.object(ops, "storage_events"), \
            patch.object(ops, "set_node_status", e.set_node_status), \
            patch.object(ops.constants, "NON_LEADER_BLOCK_QUIESCE_SEC", 0), \
            patch.object(StorageNode, "client", autospec=True), \
            patch("simplyblock_core.utils.hublvol_reconnect.HublvolReconnectCoordinator"):
        yield e


def _dispatch(db, node):
    """Restart dispatch of ``node`` with every non-leader rebuild real; returns
    the outcome, the leader rebuilds (``(node id, lvs_primary id or None)``)
    and the node each non-leader rebuild stood behind, per LVS."""
    impl = ops._recreate_lvstore_on_non_leader_impl
    with patch.object(ops, "recreate_lvstore", return_value=True) as leader, \
            patch.object(ops, "_recreate_lvstore_on_non_leader_impl", side_effect=impl) as nl:
        ret = ops.recreate_all_lvstores(_fresh(db, node))
    leaders = [(c.args[0].get_id(), c.kwargs.get("lvs_primary") and c.kwargs["lvs_primary"].get_id())
               for c in leader.call_args_list]
    anchors = {c.args[2].lvstore: c.args[1].get_id() for c in nl.call_args_list}
    return ret, leaders, anchors


class TestAbsentMembers:

    @pytest.mark.parametrize("a_status", [ONLINE, OFFLINE])
    @pytest.mark.parametrize("state", [DOWN, DONE])
    def test_a_serving_node_restarts_behind_the_empty_site(
            self, db, rpcs, hub, links, state, a_status):
        """b0 owns LVS_2, led from A; b1 is its secondary. With A ONLINE and
        empty, each restart stands behind A's primary of LVS_2 (rule 4 of
        _rebuild_anchor: no candidate connected) - the answer it gets with A
        OFFLINE - asks no node of A, quiesces none and wires no hublvol for an
        LVS led from A; its own LVS and its local roles on B rebuild as usual,
        its passive remote instance of LVS_1 behind a0."""
        cluster, a, b = _layout(db, state, a_status=a_status)
        _empty(rpcs, a)
        rpcs.lead(b[1])

        ret, leaders, anchors = _dispatch(db, b[0])
        assert ret is True
        assert leaders == []
        assert anchors == {"LVS_2": a[0].get_id(), "LVS_3": b[1].get_id(), "LVS_1": a[0].get_id()}

        ret, leaders, anchors = _dispatch(db, b[1])
        assert ret is True
        assert leaders == [(b[1].get_id(), None)]
        assert anchors == {"LVS_2": a[0].get_id(), "LVS_1": a[0].get_id()}

        a_ids = {n.get_id() for n in a}
        for node in a:
            rpcs[node.get_id()].get_lvstore.assert_not_called()
        assert not a_ids & set(links.blocked())
        # Hublvol work only for LVS_3, led on B: none for an LVS led from A.
        for name, mock in hub.items():
            for c in mock.call_args_list:
                nodes = [x for x in (*c.args, *c.kwargs.values()) if isinstance(x, StorageNode)]
                assert not a_ids & {n.get_id() for n in nodes}, name
                lvs_node = c.kwargs.get("lvs_node")
                assert lvs_node is None or lvs_node.lvstore == "LVS_3", name
        attach = hub["connect_to_hublvol"].call_args_list
        assert attach and {c.args[1].get_id() for c in attach} == {b[1].get_id()}

    @pytest.mark.parametrize("leader", [None, 1], ids=["nobody-leads", "b1-leads"])
    @pytest.mark.parametrize("target", [SITE_A, SITE_B])
    @pytest.mark.parametrize("index", [0, 1])
    def test_moving_lvs_anchor_is_on_the_serving_site(self, db, rpcs, links, index, target, leader):
        """a0's LVS_1 is being moved (``moving:``) while A is recorded, ONLINE
        and empty: the non-leader rebuild of its instance on a0 (its own) or
        a1 stands behind the member of B that leads it or, nobody leading,
        the first connected candidate - b0 (the source triplet comes first,
        A's members are disconnected); no node of A is asked."""
        cluster, a, b = _layout(db, DOWN)
        _empty(rpcs, a)
        owner = _update(db, a[0], lvs_active_site=ops.lvs_moving_value(target))
        if leader is not None:
            rpcs.lead(b[leader])
        anchor = ops._rebuild_anchor(owner, _fresh(db, a[index]), db)
        assert anchor.get_id() == b[leader or 0].get_id()
        for node in a:
            rpcs[node.get_id()].get_lvstore.assert_not_called()

    @pytest.mark.parametrize("state", [DOWN, FENCING, DONE])
    def test_an_online_node_of_the_site_is_disconnected(self, db, links, state):
        cluster, a, b = _layout(db, state)
        assert ops._check_peer_disconnected(a[1]) is True
        links.votes.assert_not_called()

    @pytest.mark.parametrize("case", ["draining", "no record", "serving site", "no sync replication"])
    def test_the_vote_decides_for_any_other_node(self, db, links, case):
        state = {"draining": DRAINING, "no record": ""}.get(case, DOWN)
        cluster, a, b = _layout(db, state, sync=case != "no sync replication")
        if case == "no sync replication":
            _update_cluster(db, cluster, lost_site=SITE_A, lost_site_state=DOWN)
        node = b[1] if case == "serving site" else a[1]
        assert ops._check_peer_disconnected(node) is False
        assert links.votes.called

    def test_an_empty_member_is_skipped_by_an_operation(self, db, rpcs, links):
        cluster, a, b = _layout(db, DOWN)
        _empty(rpcs, a)
        members = [_fresh(db, n) for n in (*b, *a)]
        assert ops.check_non_leader_for_operation(
            a[1].get_id(), "LVS_2", leader_op_completed=True, all_nodes=members) == "skip"

    def test_an_empty_node_votes_on_its_links(self, db, rpcs, links):
        """The data-plane vote is a physical link observation, not an LVS
        answer: an empty node of the recorded site whose controller to b1's JM
        is enabled is counted (the restart connected it)."""
        cluster, a, b = _layout(db, DOWN)
        for node in (a[0], a[2]):
            _update(db, node, status=OFFLINE)
        _update(db, a[1], jm_vuid=7)
        voter = rpcs[a[1].get_id()]
        voter.bdev_get.side_effect = lambda name: {"name": name}
        voter.bdev_nvme_controller_list.return_value = [{"ctrlrs": [{"state": "enabled"}]}]
        assert _REAL_VOTES(_fresh(db, b[1])) == (0, 1)


#: The vote at its boundary, unpatched (the ``links`` fixture answers it).
_REAL_VOTES = monitor._count_data_plane_votes_uncached


def _update_cluster(db, cluster, **fields):
    fresh = db.get_cluster_by_id(cluster.get_id())
    for key, value in fields.items():
        setattr(fresh, key, value)
    fresh.write_to_db(db.kv_store)
    return fresh


#: The in-site ANA flips of a sync-replication cluster (_sync_flip_active_peer).
_ANA_TRIGGERS = {"failback": ops.trigger_ana_failback_for_node,
                 "failover": ops.trigger_ana_failover_for_node}


class TestAnaFlip:
    """The ANA failover / failback of a node flips the next member of every
    triplet the node is the active primary of: a0 of LVS_1 and LVS_2 (next
    member a1), b1 of LVS_3 (next member b2). Each volume's state is captured
    at _set_lvol_ana_on_node."""

    @pytest.fixture()
    def flips(self):
        with patch.object(ops, "_served_lvols", side_effect=lambda db_ctrl, owner_id: [f"vol-{owner_id}"]), \
                patch.object(ops, "_set_lvol_ana_on_node") as flip:
            yield flip

    @pytest.mark.parametrize("trigger", list(_ANA_TRIGGERS))
    @pytest.mark.parametrize("state", [DOWN, FENCING, DONE])
    def test_an_empty_peer_is_not_flipped(self, db, flips, state, trigger):
        """a1, ONLINE without lvstores on the recorded site, holds no
        instance to flip: as if it were OFFLINE."""
        cluster, a, b = _layout(db, state)
        ret = _ANA_TRIGGERS[trigger](_fresh(db, a[0]))
        flips.assert_not_called()
        if trigger == "failover":
            # skipped, not attempted: the monitor retries it
            assert ret is False

    @pytest.mark.parametrize("trigger", list(_ANA_TRIGGERS))
    @pytest.mark.parametrize("case", ["no record", "draining", "serving site"])
    def test_a_peer_holding_its_instance_is_flipped(self, db, flips, case, trigger):
        """No record, a ``draining`` site (its nodes not yet shut down hold
        their instances) and the serving site while A is recorded ``down``."""
        cluster, a, b = _layout(db, {"no record": "", "draining": DRAINING}.get(case, DOWN))
        if case == "serving site":
            node, expected = b[1], {(f"vol-{b[1].get_id()}", b[2].get_id())}
        else:
            node, expected = a[0], {(f"vol-{a[0].get_id()}", a[1].get_id()),
                                    (f"vol-{b[0].get_id()}", a[1].get_id())}
        ret = _ANA_TRIGGERS[trigger](_fresh(db, node))
        assert {(c.args[0], c.args[1].get_id()) for c in flips.call_args_list} == expected
        if trigger == "failover":
            assert ret is True


class TestSyncStatus:

    def _answer(self, names):
        return [{"name": name, "status": src.STATUS_SYNCED, "sync_replication_mode": src.MODE_FULL}
                for name in names]

    def test_an_empty_node_is_not_asked(self, db, rpcs):
        cluster, a, b = _layout(db, DOWN)
        names = [f"distrib_{lvs}{i}" for lvs in (1, 2, 3) for i in (1, 2)]
        for node in b:
            rpcs[node.get_id()].distr_sync_replication_status.return_value = self._answer(names)
        for node in a:
            rpcs[node.get_id()].distr_sync_replication_status.return_value = []
        statuses = {s.lvs_name: s for s in src._live_lvs_statuses(db, db.get_cluster_by_id(cluster.get_id()))}
        assert statuses["LVS_2"].distribs == {"distrib_21": src.STATUS_SYNCED, "distrib_22": src.STATUS_SYNCED}
        assert set(statuses["LVS_2"].answering) == {n.get_id() for n in b}
        for node in a:
            rpcs[node.get_id()].distr_sync_replication_status.assert_not_called()


class TestPromoteHandOff:
    """_lost_members_not_leading: right before a disaster hand-off of LVS_1
    (led from A), no member on A may lead it."""

    @pytest.fixture()
    def spdk_up(self):
        api = MagicMock(name="node-api")
        api.spdk_process_is_up.return_value = (True, None)
        with patch.object(StorageNode, "client", autospec=True, side_effect=lambda node, *a, **k: api):
            yield api

    @pytest.mark.parametrize("answer", [None, RPCConnectionError("no answer")])
    def test_an_empty_member_does_not_stop_it(self, db, rpcs, spdk_up, answer):
        cluster, a, b = _layout(db, DONE)
        for node in a:
            stack._goes_silent(rpcs, node, answer)
        runner._lost_members_not_leading("LVS_1", a[0].get_id(), SITE_A)
        spdk_up.spdk_process_is_up.assert_not_called()

    def test_a_member_that_leads_stops_it(self, db, rpcs, spdk_up):
        cluster, a, b = _layout(db, DONE)
        _empty(rpcs, a)
        rpcs.lead(a[1])
        with pytest.raises(runner._PromoteFailed, match="reports the leadership"):
            runner._lost_members_not_leading("LVS_1", a[0].get_id(), SITE_A)

    def test_a_draining_site_needs_the_proof(self, db, rpcs, spdk_up):
        cluster, a, b = _layout(db, DRAINING)
        _empty(rpcs, a)
        with pytest.raises(runner._PromoteFailed, match="neither answering"):
            runner._lost_members_not_leading("LVS_1", a[0].get_id(), SITE_A)


# ---------------------------------------------------------------------------
# no restart while a site activation runs
# ---------------------------------------------------------------------------

class TestRestartDuringASiteActivation:

    def _activating(self, db):
        cluster, a, b = _layout(db, DOWN, a_status=OFFLINE)
        token = src.begin_site_activation(cluster.get_id(), SITE_A)
        assert token
        return cluster, a, b, token

    @pytest.mark.parametrize("site", [SITE_A, SITE_B])
    def test_a_restart_of_either_site_is_refused(self, db, rpcs, restart, site):
        cluster, a, b, token = self._activating(db)
        node = _restartable(db, a[1] if site == SITE_A else b[1])
        assert ops._restart_storage_node_impl(node.get_id()) is False
        assert _fresh(db, node).status == OFFLINE
        restart.check_api.assert_not_called()
        src.end_site_activation(cluster.get_id(), token)
        restart.check_api.return_value = False      # the next step of the restart, then stop
        assert ops._restart_storage_node_impl(node.get_id()) is False
        restart.check_api.assert_called_once()

    def test_auto_restart_is_paused_for_every_node(self, db):
        cluster, a, b, token = self._activating(db)
        node_b = _update(db, b[1], status=OFFLINE)
        fresh = db.get_cluster_by_id(cluster.get_id())
        for node in (a[1], node_b, None):
            assert tasks_controller.is_auto_restart_paused(fresh, node) is True
        assert not tasks_controller.add_node_to_auto_restart(node_b)
        assert not tasks_controller.get_active_node_restart_task(cluster.get_id(), node_b.get_id())
        task = JobSchedule()
        task.uuid, task.cluster_id, task.node_id = str(uuid.uuid4()), cluster.get_id(), node_b.get_id()
        task.function_name, task.status, task.max_retry = JobSchedule.FN_NODE_RESTART, JobSchedule.STATUS_NEW, 10
        task.write_to_db(db.kv_store)
        assert handler_reached(tasks_runner_restart.SPEC, task) is False

        src.end_site_activation(cluster.get_id(), token)
        fresh = db.get_cluster_by_id(cluster.get_id())
        for node in (a[1], node_b, None):
            assert tasks_controller.is_auto_restart_paused(fresh, node) is False
        assert handler_reached(tasks_runner_restart.SPEC, task) is True
        assert tasks_controller.add_node_to_auto_restart(node_b)
