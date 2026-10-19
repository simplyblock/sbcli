"""Disaster fail-over on a sync-replication cluster, against the real
FoundationDB: the forced promote of a volume whose LVS is led from a lost site
(the API row, the disaster gate scoped to the LVS it moves, the runner's site
steps and their ordering, retries from ``fencing``, the check of the lost
site's members before each hand-off), the grant guards that keep a lost site
from leading, the re-activation of a cluster with an LVS led from its remote
triplet, the cluster verdict with a site out, the one writer of the lost-site
record, the record step of a site return (the volumes' sites it leaves alone)
and the monitor pass that writes the record and clears it under a suspension.

Storage nodes are the stateful nvmf fake of
test_sync_replication_lvol_publish.py (or the plain RPC mocks of
test_sync_replication_lvs_stack.py for the rebuild paths); the liveness probes of the
lost site and the fenced hand-off are mocked at their boundary. The pure
rules are in tests/unit/test_sync_replication_disaster.py.
"""
import threading
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.exceptions import (
    SyncGateError,
    SyncPromoteFailedError,
    SyncPromoteRefusedError,
    SyncSiteOfflineError,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException, RPCHTTPError
from simplyblock_core.services import storage_node_monitor as monitor
from simplyblock_core.services import tasks_runner_sync_lvol_del
from simplyblock_core.services import tasks_runner_sync_promote as runner
from simplyblock_core.snode_client import SNodeClientException
from tests.integration import test_sync_replication_demote_promote as dp
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack
from tests.integration.task_driver import drive

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
MOVING_B = "moving:" + SITE_B

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
env = stack.env
spdk = publish.spdk
moves = dp.moves
gate = dp.gate
_clean_leader_caches = publish._clean_leader_caches
_fresh, _update = stack._fresh, stack._update


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

class _Evidence:
    """The liveness probes of the lost site at their boundaries: the node API
    of each node and the vote of the surviving peers about its JM link. Every
    node is down - its node API refuses, each of the three peers reports its
    JM controller failed - unless named. The node's own RPC, the third
    boundary, is the ``spdk`` fake: _Lost makes the lost nodes answer nothing."""

    PEERS = 3

    def __init__(self):
        self.alive: set = set()        # the node API reports the SPDK process up
        self.connected: set = set()    # one surviving peer still reports the JM link enabled
        self.unseen: set = set()       # no surviving peer holds the JM bdev: nobody votes
        self.calls: list = []

    def node_api(self, node, **kwargs):
        def _is_up(rpc_port, cluster_id):
            self.calls.append(("spdk", node.get_id()))
            if node.get_id() not in self.alive:
                raise SNodeClientException("connection refused")
            return True, None

        api = MagicMock(name=f"api-{node.get_id()}")
        api.spdk_process_is_up.side_effect = _is_up
        return api

    def votes(self, node):
        """``(disconnected, total)``, as the monitor counts the peers' votes."""
        self.calls.append(("dp", node.get_id()))
        if node.get_id() in self.unseen:
            return 0, 0
        return (self.PEERS - 1 if node.get_id() in self.connected else self.PEERS), self.PEERS


@pytest.fixture()
def evidence():
    """The liveness probes, the planned gate refused (a disaster never uses
    it) and the best-effort device broadcast of device_set_state silenced, so
    every ``distr_status_events_update`` a node records is the strict fence."""
    e = _Evidence()
    with patch.object(StorageNode, "client", autospec=True, side_effect=e.node_api), \
            patch.object(monitor, "_count_data_plane_votes_uncached", side_effect=e.votes), \
            patch("simplyblock_core.distr_controller.send_dev_status_event"), \
            patch.object(src, "check_gate", side_effect=AssertionError("planned gate used")):
        yield e


def _dead_spdk():
    """What the live RPC proxy of a dead SPDK answers: HTTP 500."""
    return RPCHTTPError("HTTP 500 from remote", status_code=500, response_text="")


def _answers(rpc, answer):
    """``rpc`` (one RPC of a node's fake) answers ``answer`` from now on - an
    exception is raised. A live RPC proxy in front of a dead SPDK answers
    HTTP 500 (_DEAD_SPDK)."""
    if isinstance(answer, Exception):
        rpc.side_effect = answer
    else:
        rpc.side_effect, rpc.return_value = None, answer


class _Lost:
    """Site A lost: a0 owns LVS_1 (home a0 a1 a2, remote b0 b1 b2), a1 owns
    LVS_2 (home a1 a0 a2, remote b1 b2 b0); two volumes on each, served on A
    and registered on all six paths; the nodes of A in ``t_status``."""

    def __init__(self, db, spdk, t_status=StorageNode.STATUS_OFFLINE):
        self.cluster, a, b, owner1 = publish._layout(db)
        owner2 = stack._make_owner(db, a[1], [a[1], a[0], a[2]], [b[1], b[2], b[0]], lvs_id=2)
        ports = {**_fresh(db, owner1).lvstore_ports, **owner2.lvstore_ports}
        for node in (*a, *b):
            _update(db, node, lvstore_ports=ports)
        self.owner1, self.owner2 = _fresh(db, owner1), _fresh(db, owner2)
        pool = publish._pool(db, self.cluster)
        self.vols = {}
        for owner, names in ((self.owner1, ("v1a", "v1b")), (self.owner2, ("v2a", "v2b"))):
            for name in names:
                vol = publish._volume(db, self.cluster, owner, pool)
                publish._register_everywhere(db, vol, owner, [_fresh(db, n) for n in (*a, *b)])
                vol.write_to_db(db.kv_store)
                self.vols[name] = vol.get_id()
        for node in (*a, *b):
            fresh = _fresh(db, node)
            for dev in fresh.nvme_devices:
                dev.cluster_id = self.cluster.get_id()
            fresh.write_to_db(db.kv_store)
        for node in a:
            _update(db, node, status=t_status)
            # a lost node answers nothing
            for rpc in (spdk[node.get_id()].get_lvstore, spdk[node.get_id()].get_version):
                _answers(rpc, RPCException("connection refused"))
        self.db = db
        self.a = [_fresh(db, n) for n in a]
        self.b = [_fresh(db, n) for n in b]

    def vol(self, name):
        return self.db.get_lvol_by_id(self.vols[name])

    def promote(self, name, force=True):
        return src.sync_promote_lvol(self.vols[name], SITE_B, force=force)

    def run(self, task_id):
        return runner.task_runner(self.db.get_task_by_id(task_id))

    def task(self, task_id):
        return self.db.get_task_by_id(task_id)

    def cluster_now(self):
        return self.db.get_cluster_by_id(self.cluster.get_id())

    def record(self, state):
        """The storage-node monitor's record of A (draining / down), through the one writer."""
        assert src.set_lost_site_state(self.cluster.get_id(), SITE_A, state, ("",))

    def led_from_b(self):
        """LVS_2 led from its remote triplet on B (b1 b2 b0) - moved there before the loss."""
        self.owner2 = _update(self.db, self.owner2, lvs_active_site=SITE_B)

    def site_devices(self, site_nodes):
        return [d for n in site_nodes for d in _fresh(self.db, n).nvme_devices]

    def events_to(self, spdk, node):
        """The device events ``node`` was sent, as ``{storage_ID: status}``."""
        out = {}
        for call in spdk[node.get_id()].distr_status_events_update.call_args_list:
            for event in call.args[0]["events"]:
                if event["event_type"] == "device_status":
                    out[event["storage_ID"]] = event["status"]
        return out


def _desync(node, vuid, ts="2026-09-30T10:00:00Z"):
    return src.record_sync_event(node.get_id(), {
        "timestamp": ts, "event_type": "sync_replication_status",
        "status": "secondary_zone_unavailable", "vuid": vuid})


# ---------------------------------------------------------------------------
# the API row
# ---------------------------------------------------------------------------

class TestApi:

    def test_a_forced_promote_queues_a_disaster_task(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        with pytest.raises(SyncSiteOfflineError):
            lost.promote("v1a", force=False)
        result = lost.promote("v1a")
        task = lost.task(result.task_id)
        assert result.in_progress
        assert task.function_params["lost_site"] == SITE_A
        assert task.function_params["fence"] is True
        assert task.function_params["owners"] == {"LVS_1": lost.owner1.get_id()}
        assert lost.cluster_now().lost_site == ""        # nothing marked on the API side

    @pytest.mark.parametrize("status", [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN,
                                        StorageNode.STATUS_RESTARTING])
    def test_a_node_of_the_site_that_may_run_refuses(self, db, spdk, evidence, status):
        lost = _Lost(db, spdk)
        _update(db, lost.a[2], status=status)
        with pytest.raises(SyncPromoteRefusedError, match="online|may still run"):
            lost.promote("v1a")
        assert db.get_sync_promote_task(lost.cluster.get_id(), "LVS_1") is None

    def test_an_lvs_unsynced_before_the_loss_is_refused(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        _desync(lost.a[1], vuid=11)
        with pytest.raises(SyncGateError):
            lost.promote("v1a")
        assert db.get_sync_promote_task(lost.cluster.get_id(), "LVS_1") is None
        assert lost.cluster_now().lost_site == ""

    def test_another_unsynced_lvs_blocks_only_its_own_promote(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        _desync(lost.a[2], vuid=21)                     # LVS_2 unsynced before the loss
        # the surviving site's instances report the loss itself: never counts
        _desync(lost.b[0], vuid=11, ts="2026-09-30T11:00:00Z")
        result = lost.promote("v1a")
        assert lost.run(result.task_id)
        assert lost.vol("v1a").sync_active_site == SITE_B
        with pytest.raises(SyncGateError):
            lost.promote("v2a")

    @pytest.mark.parametrize("force", [False, True], ids=["unforced", "forced"])
    @pytest.mark.parametrize("state", [Cluster.LOST_SITE_DOWN, Cluster.LOST_SITE_DRAINING])
    def test_opening_a_survivor_led_volume_fences_nothing(
            self, db, spdk, evidence, moves, state, force):
        # The monitor recorded A; no fail-over ran. The ANA-only row of a
        # volume whose LVS is led from B (forced or not: the row comes before
        # the force checks) is judged by the disaster gate and opens it on B -
        # the record, A's devices and nodes and every other volume stay as
        # they were.
        lost = _Lost(db, spdk)
        lost.led_from_b()
        lost.record(state)
        result = lost.promote("v2a", force=force)
        task = lost.task(result.task_id)
        assert (task.function_params["lost_site"], task.function_params["fence"]) == (SITE_A, False)
        assert lost.run(result.task_id)
        assert lost.task(result.task_id).function_result.startswith("promoted")
        v2a = lost.vol("v2a")
        assert (v2a.sync_active_site, v2a.sync_demoted_sites) == (SITE_B, [])
        assert dp._states(spdk, [lost.b[1], lost.b[2], lost.b[0]], v2a) == [
            "optimized", "non_optimized", "non_optimized"]
        assert _record(db, lost.cluster.get_id()) == (SITE_A, state)
        assert evidence.calls == []                                     # no site-loss proof
        assert all(not lost.events_to(spdk, n) for n in lost.b)         # no fence sent to B
        assert {d.status for d in lost.site_devices(lost.a)} == {NVMeDevice.STATUS_ONLINE}
        assert {_fresh(db, n).status for n in lost.a} == {StorageNode.STATUS_OFFLINE}
        assert all(SITE_A not in lost.vol(n).sync_demoted_sites for n in lost.vols)
        assert moves.calls == []

    def test_a_queued_promote_never_opens_a_later_lost_site(self, db, spdk, evidence, moves):
        # Queued to B while A was recorded; before the pass A returned and the
        # monitor recorded B. The task, judged against A, must not open B.
        lost = _Lost(db, spdk)
        lost.led_from_b()
        lost.record(Cluster.LOST_SITE_DOWN)
        result = lost.promote("v2a", force=False)
        _seed_record(db, lost.cluster, SITE_B, Cluster.LOST_SITE_DRAINING)
        ana = {n.get_id(): dp._ana_calls(spdk, n) for n in lost.b}
        lost.run(result.task_id)
        task = lost.task(result.task_id)
        assert task.status == JobSchedule.STATUS_DONE
        assert f"site {SITE_B} is recorded as lost (draining)" in task.function_result
        assert {n.get_id(): dp._ana_calls(spdk, n) for n in lost.b} == ana     # nothing opened on B
        v2a = lost.vol("v2a")
        assert (v2a.sync_active_site, v2a.sync_demoted_sites) == (SITE_A, [])
        assert _record(db, lost.cluster.get_id()) == (SITE_B, Cluster.LOST_SITE_DRAINING)

    @pytest.mark.parametrize("state", [Cluster.LOST_SITE_DOWN, Cluster.LOST_SITE_DRAINING])
    def test_a_recorded_site_refuses_a_planned_promote(self, db, spdk, evidence, state):
        # online_sites / promote_decision: even with every node of A back
        # ONLINE, a recorded A is no site to promote from or to.
        lost = _Lost(db, spdk)
        lost.led_from_b()
        lost.record(state)
        for node in lost.a:
            _update(db, node, status=StorageNode.STATUS_ONLINE)
        with pytest.raises(SyncSiteOfflineError):
            lost.promote("v1a", force=False)                            # led from A, to B: 412
        with pytest.raises(SyncPromoteRefusedError, match="no online node"):
            src.sync_promote_lvol(lost.vols["v2a"], SITE_A)             # led from B, to A
        assert db.get_sync_promote_task(lost.cluster.get_id(), "LVS_1") is None
        assert db.get_sync_promote_task(lost.cluster.get_id(), "LVS_2") is None


# ---------------------------------------------------------------------------
# the site steps
# ---------------------------------------------------------------------------

class TestSiteSteps:

    def test_the_site_is_fenced_before_the_lvs_moves(
            self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        seen = {}

        def _move(owner_id, moving_value, final_site, *, current_leader_id=None, taker_id):
            seen["demoted"] = {n: lost.vol(n).sync_demoted_sites for n in lost.vols}
            seen["cluster"] = (lost.cluster_now().lost_site, lost.cluster_now().lost_site_state)
            return dp._Moves.move(moves, owner_id, moving_value, final_site,
                                  current_leader_id=current_leader_id, taker_id=taker_id)

        with patch.object(ops, "move_lvs_leadership", side_effect=_move):
            result = lost.promote("v1a")
            assert lost.run(result.task_id)
        task = lost.task(result.task_id)
        assert task.status == JobSchedule.STATUS_DONE and task.function_result.startswith("promoted")
        assert seen["cluster"] == (SITE_A, Cluster.LOST_SITE_DONE)
        assert seen["demoted"] == {"v1a": [], "v1b": [SITE_A], "v2a": [SITE_A], "v2b": [SITE_A]}
        assert moves.calls == [("LVS_1", None, lost.b[0].get_id(), MOVING_B, SITE_B)]
        # every device of A unavailable, in the DB and in every distrib of B
        devices = lost.site_devices(lost.a)
        assert {d.status for d in devices} == {NVMeDevice.STATUS_UNAVAILABLE}
        for node in lost.b:
            events = lost.events_to(spdk, node)
            assert {events[d.cluster_device_order] for d in devices} == {NVMeDevice.STATUS_UNAVAILABLE}
        assert {_fresh(db, n).status for n in lost.a} == {StorageNode.STATUS_OFFLINE}
        v1a = lost.vol("v1a")
        assert (v1a.sync_active_site, v1a.sync_demoted_sites) == (SITE_B, [])
        assert dp._states(spdk, lost.b, v1a) == ["optimized", "non_optimized", "non_optimized"]

    def test_a_failed_device_is_fenced_as_failed(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        node = _fresh(db, lost.a[2])
        node.nvme_devices[0].status = NVMeDevice.STATUS_FAILED
        node.write_to_db(db.kv_store)
        failed = node.nvme_devices[0]
        # a surviving node that missed the failure still holds it online
        peer = _fresh(db, lost.b[1])
        assert all(d.get_id() != failed.get_id() or d.status == NVMeDevice.STATUS_ONLINE
                   for d in peer.remote_devices)
        assert lost.run(lost.promote("v1a").task_id)
        assert _fresh(db, lost.a[2]).nvme_devices[0].status == NVMeDevice.STATUS_FAILED
        for peer in lost.b:
            assert lost.events_to(spdk, peer)[failed.cluster_device_order] == NVMeDevice.STATUS_FAILED

    def test_a_refused_gate_marks_nothing(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        result = lost.promote("v1a")
        _desync(lost.a[0], vuid=12)                     # recorded between enqueue and run
        lost.run(result.task_id)
        assert "gate failed before the site steps" in lost.task(result.task_id).function_result
        cluster = lost.cluster_now()
        assert (cluster.lost_site, cluster.lost_site_state) == ("", "")
        assert evidence.calls == []
        assert all(not lost.events_to(spdk, n) for n in lost.b)
        assert {d.status for d in lost.site_devices(lost.a)} == {NVMeDevice.STATUS_ONLINE}
        assert all(lost.vol(n).sync_demoted_sites == [] for n in lost.vols)
        assert moves.calls == []

    def test_the_gate_runs_before_anything_is_marked(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        states = []
        real = src.check_disaster_gate

        def _gate(cluster_id, site, lvs_names=None):
            states.append(lost.cluster_now().lost_site)
            return real(cluster_id, site, lvs_names)

        result = lost.promote("v1a")
        with patch.object(src, "check_disaster_gate", side_effect=_gate):
            assert lost.run(result.task_id)
        assert states[0] == ""

    @pytest.mark.parametrize("probe, answered", [
        ("node-api", "the node API reports the SPDK process up"),
        ("rpc", "its RPC answers")])
    def test_an_unreachable_node_with_live_spdk_is_not_lost(
            self, db, spdk, evidence, moves, probe, answered):
        lost = _Lost(db, spdk)
        node = lost.a[2]
        _update(db, node, status=StorageNode.STATUS_UNREACHABLE)
        result = lost.promote("v1a")
        if probe == "node-api":
            evidence.alive.add(node.get_id())
        else:
            _answers(spdk[node.get_id()].get_version, {"version": "SPDK v25.05"})
        lost.run(result.task_id)
        assert f"node {node.get_id()}: {answered}" in lost.task(result.task_id).function_result
        assert lost.cluster_now().lost_site == ""
        assert moves.calls == []

    def test_a_disaster_promote_hands_off_through_the_real_move(self, db, spdk, evidence):
        """The promote driven through its runner with the real hand-off: the
        lost members are probed (none leads), the members of B answer as
        non-leaders before and after the hand-off, and LVS_1 ends led from B."""
        lost = _Lost(db, spdk)
        for node in lost.b:
            _answers(spdk[node.get_id()].get_lvstore, {"name": "LVS_1", "lvs leadership": False})
        result = lost.promote("v1a")
        with patch.object(ops.port_block, "set_port"):
            assert lost.run(result.task_id)
        task = lost.task(result.task_id)
        assert task.status == JobSchedule.STATUS_DONE and task.function_result.startswith("promoted"), \
            task.function_result
        assert _fresh(db, lost.owner1).lvs_active_site == SITE_B
        assert lost.vol("v1a").sync_active_site == SITE_B
        for node in (*lost.a, *lost.b):
            spdk[node.get_id()].get_lvstore.assert_any_call("LVS_1")

    def test_a_live_rpc_proxy_in_front_of_a_dead_spdk_is_lost(self, db, spdk, evidence, moves):
        # The RPC proxy of every lost node outlived its SPDK and answers each
        # call with no result.
        lost = _Lost(db, spdk)
        for node in lost.a:
            _answers(spdk[node.get_id()].get_version, _dead_spdk())
            _answers(spdk[node.get_id()].get_lvstore, _dead_spdk())
        result = lost.promote("v1a")
        lost.run(result.task_id)
        assert lost.task(result.task_id).function_result.startswith("promoted")
        cluster = lost.cluster_now()
        assert (cluster.lost_site, cluster.lost_site_state) == (SITE_A, Cluster.LOST_SITE_DONE)
        assert lost.vol("v1a").sync_active_site == SITE_B

    def test_a_site_nobody_holds_a_jm_link_to_passes_the_proof(self, db, spdk, evidence, moves):
        # 0/0: no surviving peer holds a JM bdev of the lost nodes, so nobody
        # votes about them - silence is no veto.
        lost = _Lost(db, spdk)
        evidence.unseen.update(n.get_id() for n in lost.a)
        # one member's RPC proxy outlived its SPDK: no leadership answer either
        _answers(spdk[lost.a[1].get_id()].get_lvstore, _dead_spdk())
        result = lost.promote("v1a")
        lost.run(result.task_id)
        assert lost.task(result.task_id).function_result.startswith("promoted")
        assert {("dp", n.get_id()) for n in lost.a} <= set(evidence.calls)      # the vote was taken
        cluster = lost.cluster_now()
        assert (cluster.lost_site, cluster.lost_site_state) == (SITE_A, Cluster.LOST_SITE_DONE)
        assert {d.status for d in lost.site_devices(lost.a)} == {NVMeDevice.STATUS_UNAVAILABLE}
        assert {_fresh(db, n).status for n in lost.a} == {StorageNode.STATUS_OFFLINE}
        assert lost.vol("v1a").sync_active_site == SITE_B
        assert moves.calls == [("LVS_1", None, lost.b[0].get_id(), MOVING_B, SITE_B)]

    def test_a_node_the_data_plane_still_reaches_is_not_lost(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        node = lost.a[1]
        evidence.connected.add(node.get_id())
        # a cached vote that calls the node gone is not what the proof reads
        monitor._status_probe_cache.put(("dp_votes", node.get_id()), (3, 3))
        result = lost.promote("v1a")
        lost.run(result.task_id)
        assert (f"node {node.get_id()}: 1 of 3 answering peers still report its JM link enabled"
                in lost.task(result.task_id).function_result)
        assert lost.cluster_now().lost_site == ""
        assert moves.calls == []

    def test_a_late_connected_vote_leaves_the_fence_open(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        real = evidence.votes
        asked = {"n": 0}

        def _votes(node):
            if node.get_id() == lost.a[0].get_id():
                asked["n"] += 1
                if asked["n"] > 1:
                    return 2, 3
            return real(node)

        result = lost.promote("v1a")
        with patch.object(monitor, "_count_data_plane_votes_uncached", side_effect=_votes):
            lost.run(result.task_id)
        assert "before the fence is recorded done" in lost.task(result.task_id).function_result
        cluster = lost.cluster_now()
        assert (cluster.lost_site, cluster.lost_site_state) == (SITE_A, Cluster.LOST_SITE_FENCING)
        assert moves.calls == []

    def test_a_retry_completes_a_fence_missing_an_ack(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        spdk[lost.b[2].get_id()].distr_status_events_update.side_effect = [RPCException("gone"), True, True]
        first = lost.promote("v1a")
        lost.run(first.task_id)
        assert "could not be marked unavailable" in lost.task(first.task_id).function_result
        assert lost.cluster_now().lost_site_state == Cluster.LOST_SITE_FENCING
        # the next call reports the failure once ...
        with pytest.raises(SyncPromoteFailedError) as exc:
            lost.promote("v1a")
        assert exc.value.task_id == first.task_id
        # ... the retry (a new promote call) redoes the site steps from ``fencing``
        second = lost.promote("v1a")
        assert second.task_id != first.task_id
        assert lost.run(second.task_id)
        assert lost.cluster_now().lost_site_state == Cluster.LOST_SITE_DONE
        assert lost.vol("v1a").sync_active_site == SITE_B

    def test_a_down_or_unreachable_survivor_must_ack(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        _update(db, lost.b[1], status=StorageNode.STATUS_DOWN)
        _update(db, lost.b[2], status=StorageNode.STATUS_UNREACHABLE)
        spdk[lost.b[2].get_id()].distr_status_events_update.side_effect = RPCException("mgmt down")
        result = lost.promote("v1a")
        lost.run(result.task_id)
        assert lost.b[2].get_id() in lost.task(result.task_id).function_result
        assert lost.events_to(spdk, lost.b[1])          # the DOWN node got them
        assert lost.cluster_now().lost_site_state == Cluster.LOST_SITE_FENCING

    def test_a_restarting_survivor_gets_the_fence_before_done(
            self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        _update(db, lost.b[2], status=StorageNode.STATUS_RESTARTING)
        real = src.demote_site_volumes

        def _demote(*args, **kwargs):
            assert not lost.events_to(spdk, lost.b[2])  # skipped by the first pass
            _update(db, lost.b[2], status=StorageNode.STATUS_ONLINE)
            return real(*args, **kwargs)

        result = lost.promote("v1a")
        with patch.object(src, "demote_site_volumes", side_effect=_demote):
            assert lost.run(result.task_id)
        assert set(lost.events_to(spdk, lost.b[2]).values()) == {NVMeDevice.STATUS_UNAVAILABLE}

    def test_a_second_promote_after_done_skips_the_site_steps(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        assert lost.run(lost.promote("v1a").task_id)
        sent = {n.get_id(): spdk[n.get_id()].distr_status_events_update.call_count for n in lost.b}
        result = lost.promote("v2a")
        with patch.object(runner, "_site_loss_problems", wraps=runner._site_loss_problems) as proof:
            assert lost.run(result.task_id)
        proof.assert_not_called()                                 # no site-loss proof again
        assert {n.get_id(): spdk[n.get_id()].distr_status_events_update.call_count for n in lost.b} == sent
        assert lost.vol("v2a").sync_active_site == SITE_B
        assert lost.vol("v2b").sync_demoted_sites == [SITE_A]     # still fenced on A
        assert lost.vol("v2b").sync_active_site == SITE_A
        assert moves.calls[-1] == ("LVS_2", None, lost.b[1].get_id(), MOVING_B, SITE_B)

    def test_a_lost_member_still_leading_aborts_the_hand_off(
            self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        assert lost.run(lost.promote("v1a").task_id)
        _answers(spdk[lost.a[1].get_id()].get_lvstore, {"name": "LVS_2", "lvs leadership": True})
        result = lost.promote("v2a")
        lost.run(result.task_id)
        assert "reports the leadership" in lost.task(result.task_id).function_result
        assert _fresh(db, lost.owner2).lvs_active_site == ""       # the marker undone
        assert [c[0] for c in moves.calls] == ["LVS_1"]

    def test_a_non_leader_lost_member_allows_the_hand_off(
            self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        assert lost.run(lost.promote("v1a").task_id)
        member = lost.a[1]
        _answers(spdk[member.get_id()].get_lvstore, {"name": "LVS_2", "lvs leadership": False})
        evidence.alive.add(member.get_id())             # rebuilt under the lost-site rule: it runs
        result = lost.promote("v2a")
        lost.run(result.task_id)
        assert lost.task(result.task_id).function_result.startswith("promoted")
        assert lost.vol("v2a").sync_active_site == SITE_B

    @pytest.mark.parametrize("silence", [None, RPCException("connection refused")],
                             ids=["no-lvs", "rpc-error"])
    def test_a_silent_but_seen_member_aborts_the_hand_off(
            self, db, spdk, evidence, moves, silence):
        lost = _Lost(db, spdk)
        assert lost.run(lost.promote("v1a").task_id)
        member = lost.a[2]
        _answers(spdk[member.get_id()].get_lvstore, silence)
        evidence.connected.add(member.get_id())
        # a cached vote that calls the member gone is not what the check reads
        monitor._status_probe_cache.put(("dp_votes", member.get_id()), (3, 3))
        result = lost.promote("v2a")
        lost.run(result.task_id)
        assert (f"{member.get_id()} is neither answering as a non-leader nor proven down "
                "(1 of 3 answering peers still report its JM link enabled)"
                in lost.task(result.task_id).function_result)
        assert _fresh(db, lost.owner2).lvs_active_site == ""       # the marker undone
        assert [c[0] for c in moves.calls] == ["LVS_1"]

    def test_a_silent_but_running_member_aborts_the_hand_off(
            self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        assert lost.run(lost.promote("v1a").task_id)
        member = lost.a[2]
        # no lvstore is no answer about the leadership, whatever the reason
        _answers(spdk[member.get_id()].get_lvstore, None)
        evidence.alive.add(member.get_id())
        result = lost.promote("v2a")
        lost.run(result.task_id)
        assert (f"{member.get_id()} is neither answering as a non-leader nor proven down "
                "(the node API reports the SPDK process up)"
                in lost.task(result.task_id).function_result)
        assert _fresh(db, lost.owner2).lvs_active_site == ""       # the marker undone
        assert [c[0] for c in moves.calls] == ["LVS_1"]

    def test_a_forced_promote_fails_a_site_that_is_down_over(self, db, spdk, evidence, moves):
        # The monitor recorded A down, no fail-over ran: the forced promote
        # takes the record down -> fencing -> done and fails A over.
        lost = _Lost(db, spdk)
        lost.record(Cluster.LOST_SITE_DOWN)
        written = []
        real = src.set_lost_site_state

        def _write(cluster_id, site, state, expect_states):
            before = _record(db, cluster_id)
            ok = real(cluster_id, site, state, expect_states)
            written.append((before, _record(db, cluster_id), ok))
            return ok

        with patch.object(src, "set_lost_site_state", side_effect=_write):
            result = lost.promote("v1a")
            assert lost.task(result.task_id).function_params["fence"] is True
            assert lost.run(result.task_id)
        assert lost.task(result.task_id).function_result.startswith("promoted")
        assert written == [((SITE_A, Cluster.LOST_SITE_DOWN), (SITE_A, Cluster.LOST_SITE_FENCING), True),
                           ((SITE_A, Cluster.LOST_SITE_FENCING), (SITE_A, Cluster.LOST_SITE_DONE), True)]
        devices = lost.site_devices(lost.a)
        assert {d.status for d in devices} == {NVMeDevice.STATUS_UNAVAILABLE}
        for node in lost.b:
            events = lost.events_to(spdk, node)
            assert {events[d.cluster_device_order] for d in devices} == {NVMeDevice.STATUS_UNAVAILABLE}
        assert {_fresh(db, n).status for n in lost.a} == {StorageNode.STATUS_OFFLINE}
        assert {n: lost.vol(n).sync_demoted_sites for n in lost.vols} == {
            "v1a": [], "v1b": [SITE_A], "v2a": [SITE_A], "v2b": [SITE_A]}
        assert lost.vol("v1a").sync_active_site == SITE_B
        assert moves.calls == [("LVS_1", None, lost.b[0].get_id(), MOVING_B, SITE_B)]

    def test_a_forced_promote_during_the_drain_changes_nothing(
            self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        lost.record(Cluster.LOST_SITE_DRAINING)
        result = lost.promote("v1a")
        assert lost.task(result.task_id).function_params["fence"] is True
        lost.run(result.task_id)
        task = lost.task(result.task_id)
        assert task.status == JobSchedule.STATUS_DONE
        assert f"site {SITE_A} is being shut down by the monitor; retry" in task.function_result
        assert _record(db, lost.cluster.get_id()) == (SITE_A, Cluster.LOST_SITE_DRAINING)
        assert evidence.calls == []                                     # not even probed
        assert all(not lost.events_to(spdk, n) for n in lost.b)
        assert {d.status for d in lost.site_devices(lost.a)} == {NVMeDevice.STATUS_ONLINE}
        assert all(lost.vol(n).sync_demoted_sites == [] for n in lost.vols)
        assert _fresh(db, lost.owner1).lvs_active_site == ""
        assert moves.calls == []

    def test_a_down_site_with_a_node_online_fails_the_proof(self, db, spdk, evidence, moves):
        # A recorded site is no "online" site for the API, so the forced
        # promote is queued; the runner's proof refuses it.
        lost = _Lost(db, spdk)
        lost.record(Cluster.LOST_SITE_DOWN)
        back = _update(db, lost.a[2], status=StorageNode.STATUS_ONLINE)
        result = lost.promote("v1a")
        lost.run(result.task_id)
        assert f"node {back.get_id()} is online" in lost.task(result.task_id).function_result
        assert _record(db, lost.cluster.get_id()) == (SITE_A, Cluster.LOST_SITE_DOWN)
        assert all(not lost.events_to(spdk, n) for n in lost.b)
        assert moves.calls == []

    def test_a_task_without_the_fence_flag_never_fences(self, db, spdk, evidence, moves):
        # A task that carries lost_site without the fence flag runs no site
        # steps; its move is refused while the fence is not done.
        lost = _Lost(db, spdk)
        lost.record(Cluster.LOST_SITE_DOWN)
        task_id, _ = tasks_controller.add_sync_promote_task(
            lost.cluster.get_id(), lost.owner1.get_id(), site=SITE_B, lvol_ids=[lost.vols["v1a"]],
            owners={"LVS_1": lost.owner1.get_id()}, lost_site=SITE_A)

        def _without_fence(t):
            t.function_params.pop("fence", None)

        db.atomic_update(lost.task(task_id), _without_fence)
        assert "fence" not in lost.task(task_id).function_params
        lost.run(task_id)
        task = lost.task(task_id)
        assert task.status == JobSchedule.STATUS_DONE
        assert f"the fence of site {SITE_A} is not complete" in task.function_result
        assert evidence.calls == []
        assert _record(db, lost.cluster.get_id()) == (SITE_A, Cluster.LOST_SITE_DOWN)
        assert all(not lost.events_to(spdk, n) for n in lost.b)
        assert {d.status for d in lost.site_devices(lost.a)} == {NVMeDevice.STATUS_ONLINE}
        assert all(lost.vol(n).sync_demoted_sites == [] for n in lost.vols)
        assert _fresh(db, lost.owner1).lvs_active_site == ""
        assert moves.calls == []

    @pytest.mark.parametrize("state", [Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN])
    def test_a_planned_move_is_refused_once_a_site_is_recorded(self, db, spdk, gate, moves, state):
        # promote_move_problems in the marker transaction: a planned promote
        # queued before the monitor recorded A.
        cluster, a, b, owner, pool, vol, task_id = dp._queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        assert src.set_lost_site_state(cluster.get_id(), SITE_A, state, ("",))
        runner.task_runner(db.get_task_by_id(task_id))
        task = db.get_task_by_id(task_id)
        assert task.status == JobSchedule.STATUS_DONE
        assert f"site {SITE_A} is lost; a planned promote needs both sites" in task.function_result
        assert task.function_params["moves"] == {}
        assert _fresh(db, owner).lvs_active_site == ""
        assert moves.calls == []
        assert _record(db, cluster.get_id()) == (SITE_A, state)

    def test_a_sibling_opens_on_s_after_the_fail_over(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        assert lost.run(lost.promote("v1a").task_id)
        _desync(lost.a[2], vuid=21)             # LVS_2 unsynced: does not concern LVS_1
        result = lost.promote("v1b", force=False)
        assert lost.run(result.task_id)
        v1b = lost.vol("v1b")
        assert (v1b.sync_active_site, v1b.sync_demoted_sites) == (SITE_B, [SITE_A])
        assert dp._states(spdk, lost.b, v1b) == ["optimized", "non_optimized", "non_optimized"]

    @pytest.mark.parametrize("state", [Cluster.LOST_SITE_FENCING, "moving", Cluster.LOST_SITE_DRAINING,
                                       Cluster.LOST_SITE_DOWN])
    def test_a_returning_node_keeps_every_path_closed(self, db, spdk, evidence, state):
        lost = _Lost(db, spdk)
        if state == "moving":
            src.set_lost_site_state(lost.cluster.get_id(), SITE_A, Cluster.LOST_SITE_DONE, ("",))
            _update(db, lost.owner1, lvs_active_site=MOVING_B)
        else:
            src.set_lost_site_state(lost.cluster.get_id(), SITE_A, state, ("",))
        for node in lost.a:
            node = _update(db, node, status=StorageNode.STATUS_ONLINE)
            for name in ("v1a", "v1b"):
                assert ops.lvol_ana_state(lost.vol(name), node, "optimized") == "inaccessible"


# ---------------------------------------------------------------------------
# no leadership on a lost site
# ---------------------------------------------------------------------------

class TestLostSiteGrantGuards:

    @pytest.mark.parametrize("state", [Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DONE,
                                       Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN])
    def test_no_grant_and_no_leader_rebuild_on_the_lost_site(self, db, rpcs, env, state):
        cluster, a, b, owner = stack._one_owner_layout(db, lost_site=SITE_A)
        cluster.lost_site_state = state
        cluster.write_to_db(db.kv_store)
        with ops._sync_grant_guard(cluster.get_id(), "LVS_1", owner.get_id(), a[0].get_id()) as ok:
            assert ok is False
        with pytest.raises(ops.LVSLeadershipElsewhereError, match="lost site"):
            ops.recreate_lvstore(_fresh(db, owner))

    @pytest.mark.parametrize("state", [Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DRAINING,
                                       Cluster.LOST_SITE_DOWN])
    def test_an_abandoned_move_to_the_lost_site_is_refused(self, db, rpcs, env, state):
        cluster, a, b, owner = stack._one_owner_layout(db, lost_site=SITE_A)
        cluster.lost_site_state = state
        cluster.write_to_db(db.kv_store)
        _update(db, owner, lvs_active_site="moving:" + SITE_A)
        with patch.object(ops, "_taker_jm_quorum_ok", return_value=True), \
                patch.object(ops, "_move_lvs_leadership_locked") as move:
            assert ops.reconcile_lvs_move(owner.get_id()) is None
        move.assert_not_called()
        with pytest.raises(ops.LVSMoveChangedError, match="lost site"):
            ops.move_lvs_leadership(owner.get_id(), "moving:" + SITE_A, SITE_A, taker_id=a[0].get_id())

    @pytest.mark.parametrize("state, refused", [(Cluster.LOST_SITE_DRAINING, True),
                                                (Cluster.LOST_SITE_DOWN, True),
                                                (Cluster.LOST_SITE_DONE, False)])
    def test_a_silent_unfenced_member_blocks_the_hand_off(
            self, db, rpcs, env, state, refused):
        # Only a fenced site (done) is skipped by the hand-off's probes: a
        # silent member of a site the monitor recorded may still hold the
        # PRIMARY role.
        cluster, a, b, owner = stack._one_owner_layout(db, lost_site=SITE_A)
        _seed_record(db, cluster, SITE_A, state)
        _update(db, owner, lvs_active_site=MOVING_B)
        rpcs[a[1].get_id()].get_lvstore.side_effect = RPCException("connection refused")
        with patch.object(ops, "transfer_lvs_leadership") as transfer:
            if refused:
                with pytest.raises(ops.LVSMoveChangedError, match="does not answer"):
                    ops.move_lvs_leadership(owner.get_id(), MOVING_B, SITE_B, taker_id=b[0].get_id())
            else:
                ops.move_lvs_leadership(owner.get_id(), MOVING_B, SITE_B, taker_id=b[0].get_id())
        assert transfer.called is not refused
        assert _fresh(db, owner).lvs_active_site == (MOVING_B if refused else SITE_B)


# ---------------------------------------------------------------------------
# re-activation with an LVS led from its remote triplet
# ---------------------------------------------------------------------------

@pytest.fixture()
def adopt():
    with patch.object(StorageNode, "adopt_hublvol", autospec=True, return_value=True) as a:
        yield a


def _suspended(db, cluster, nodes):
    cluster = db.get_cluster_by_id(cluster.get_id())
    cluster.status = cluster.STATUS_SUSPENDED
    cluster.activated_node_ids = [n.get_id() for n in nodes]
    cluster.write_to_db(db.kv_store)


class TestActivation:

    def test_the_leader_is_built_on_the_active_remote_primary(self, db, rpcs, hub, env, adopt):
        cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db)
        owner_a = _update(db, owner_a, lvs_active_site=SITE_B)      # LVS_1 led from b0
        _suspended(db, cluster, [*a, *b])
        with ExitStack() as es:
            cluster_ops = stack._activation_patches(es)
            es.enter_context(patch.object(ops, "create_lvstore", return_value=True))
            leader = es.enter_context(patch.object(ops, "recreate_lvstore", return_value=True))
            non_leader = es.enter_context(
                patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True))
            cluster_ops._cluster_activate(cluster.get_id())
        built = sorted((c.args[0].get_id(), c.kwargs.get("lvs_primary").get_id()
                        if c.kwargs.get("lvs_primary") else "") for c in leader.call_args_list)
        assert built == sorted([(b[0].get_id(), owner_a.get_id()), (b[0].get_id(), "")])
        rebuilt = {(c.args[0].get_id(), c.args[2].get_id()): c.args[1].get_id()
                   for c in non_leader.call_args_list}
        # the owner comes back non-leader behind b0, the remote primary is not rebuilt
        assert rebuilt[(owner_a.get_id(), owner_a.get_id())] == b[0].get_id()
        assert (b[0].get_id(), owner_a.get_id()) not in rebuilt
        assert rebuilt[(a[1].get_id(), owner_a.get_id())] == b[0].get_id()
        assert [c.args[0].get_id() for c in adopt.call_args_list] == [b[0].get_id()]
        connected = {(c.args[0].get_id(), c.args[1].get_id(), c.kwargs["role"])
                     for c in hub["connect_to_hublvol"].call_args_list
                     if c.kwargs.get("lvs_node") is not None and c.kwargs["lvs_node"].get_id() == owner_a.get_id()}
        assert connected == {(b[1].get_id(), b[0].get_id(), "secondary"),
                             (b[2].get_id(), b[0].get_id(), "tertiary")}

    @pytest.mark.parametrize("failure", ["adopt", "connect"])
    def test_an_unwired_remote_follower_fails_the_activation(
            self, db, rpcs, hub, env, adopt, failure):
        # Nothing repairs a remote follower's redirect later, and the site rule
        # would open its paths (Pass 4, the lvol monitor's ANA self-heal).
        cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db)
        owner_a = _update(db, owner_a, lvs_active_site=SITE_B)
        publish._volume(db, cluster, owner_a, publish._pool(db, cluster), active_site=SITE_B)
        _suspended(db, cluster, [*a, *b])
        if failure == "adopt":
            adopt.side_effect = RuntimeError("adopt failed")
        else:
            def _connect(node, primary, **kwargs):
                lvs_node = kwargs.get("lvs_node")
                return not (node.get_id() == b[2].get_id() and lvs_node is not None
                            and lvs_node.get_id() == owner_a.get_id())
            hub["connect_to_hublvol"].side_effect = _connect
        with ExitStack() as es:
            cluster_ops = stack._activation_patches(es)
            es.enter_context(patch.object(ops, "create_lvstore", return_value=True))
            es.enter_context(patch.object(ops, "recreate_lvstore", return_value=True))
            es.enter_context(patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True))
            ana = es.enter_context(patch.object(ops, "_set_lvol_ana_on_node"))
            with pytest.raises(ValueError, match="LVS_1"):
                cluster_ops._cluster_activate(cluster.get_id())
        ana.assert_not_called()                                     # Pass 4 never ran
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_SUSPENDED

    def test_the_leader_rebuilds_on_one_node_never_overlap(self, db, rpcs, hub, env, adopt):
        cluster = stack._seed_cluster(db)
        a, b = stack._sites(db, cluster)
        owners = [stack._make_owner(db, a[0], a, b, lvs_id=1),
                  stack._make_owner(db, a[1], [a[1], a[2], a[0]], b, lvs_id=3),
                  stack._make_owner(db, b[0], b, a, lvs_id=2)]
        for owner in owners[:2]:
            _update(db, owner, lvs_active_site=SITE_B)             # both led from b0
        _suspended(db, cluster, [*a, *b])
        guard = threading.Lock()
        running: dict = {}
        overlaps = []

        def _rebuild(snode, *args, lvs_primary=None, **kwargs):
            with guard:
                if running.get(snode.get_id()):
                    overlaps.append(snode.get_id())
                running[snode.get_id()] = True
            threading.Event().wait(0.05)
            with guard:
                running[snode.get_id()] = False
            return True

        with ExitStack() as es:
            cluster_ops = stack._activation_patches(es)
            es.enter_context(patch.object(ops, "create_lvstore", return_value=True))
            leader = es.enter_context(patch.object(ops, "recreate_lvstore", side_effect=_rebuild))
            es.enter_context(patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True))
            cluster_ops._cluster_activate(cluster.get_id())
        on_b0 = sorted(c.kwargs["lvs_primary"].get_id() if c.kwargs.get("lvs_primary") else ""
                       for c in leader.call_args_list if c.args[0].get_id() == b[0].get_id())
        assert on_b0 == sorted(["", owners[0].get_id(), owners[1].get_id()])
        assert overlaps == []

    def test_an_lvs_moving_still_refuses_the_activation(self, db, rpcs, hub, env):
        cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db)
        _update(db, owner_a, lvs_active_site=MOVING_B)
        _suspended(db, cluster, [*a, *b])
        with ExitStack() as es:
            cluster_ops = stack._activation_patches(es)
            es.enter_context(patch.object(ops, "create_lvstore", return_value=True))
            es.enter_context(patch.object(ops, "_recreate_lvstore_impl", return_value=True))
            es.enter_context(patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True))
            with pytest.raises(ValueError):
                cluster_ops._cluster_activate(cluster.get_id())

    def test_the_remote_primary_rebuilds_the_lvs_as_its_leader(self, db, rpcs, hub, env, adopt):
        # The real rebuild (RPCs mocked): the LVS of a0 led from b0.
        _, a, b, owner_a, _ = stack._two_owner_layout(db)
        owner_a = _update(db, owner_a, lvs_active_site=SITE_B)

        def _set_leader(*args, leader=False, **kwargs):
            if leader:
                rpcs.lead(b[0])
            return True
        rpcs[b[0].get_id()].bdev_lvol_set_leader.side_effect = _set_leader
        with patch.object(ops, "_connect_to_remote_jm_devs", return_value=[]) as jms:
            assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=owner_a, activation_mode=True) is True
        jms.assert_called_once()                              # the other site's JMs connected
        rpc = rpcs[b[0].get_id()]
        creates = [c.kwargs for c in rpc.bdev_distrib_create.call_args_list]
        assert {c["name"] for c in creates} == {"distrib_11", "distrib_12"}
        expected = ops.get_node_jm_names(_fresh(db, owner_a), remote_node=_fresh(db, b[0])).names
        for params in creates:
            assert params["jm_vuid"] == owner_a.jm_vuid
            assert params["jm_names"] == expected
        rpc.bdev_examine.assert_any_call(owner_a.raid)
        assert stack._took_leadership(rpc)
        assert stack._roles(rpc)[-1] == "primary"
        for node in (*a, b[1], b[2]):
            assert not stack._took_leadership(rpcs[node.get_id()])
        assert _fresh(db, owner_a).lvstore_status == "ready"

    def test_the_leader_rebuild_clears_its_pending_mark(self, db, rpcs, hub, env, adopt):
        # b0 leads LVS_1 from now on: the mark's readers must not take it for absent.
        _, a, b, owner_a, _ = stack._two_owner_layout(db)
        owner_a = _update(db, owner_a, lvs_active_site=SITE_B,
                          remote_instances_pending=[b[0].get_id(), b[1].get_id()])

        def _set_leader(*args, leader=False, **kwargs):
            if leader:
                rpcs.lead(b[0])
            return True
        rpcs[b[0].get_id()].bdev_lvol_set_leader.side_effect = _set_leader
        assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=owner_a, activation_mode=True) is True
        assert _fresh(db, owner_a).remote_instances_pending == [b[1].get_id()]

    def test_only_the_active_remote_primary_may_build_it(self, db, rpcs, env):
        _, a, b, owner_a, _ = stack._two_owner_layout(db)
        owner_a = _update(db, owner_a, lvs_active_site=SITE_B)
        with patch.object(ops, "_recreate_lvstore_impl", return_value=True) as impl:
            with pytest.raises(ops.LVSLeadershipElsewhereError):
                ops.recreate_lvstore(_fresh(db, b[1]), lvs_primary=owner_a, activation_mode=True)
            assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=owner_a, activation_mode=True)
            assert impl.call_count == 1
            # outside activation - its restart - the active primary builds it too
            assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=owner_a, activation_mode=False)
            assert impl.call_count == 2
            # led from home, nobody of the remote triplet builds it, in either mode
            _update(db, owner_a, lvs_active_site="")
            for activation in (True, False):
                with pytest.raises(ops.LVSLeadershipElsewhereError):
                    ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=owner_a, activation_mode=activation)
        assert impl.call_count == 2



# ---------------------------------------------------------------------------
# the cluster status with a site lost
# ---------------------------------------------------------------------------

def _verdict(db, cluster, dp_gone=(), seen_gone=None):
    """The status verdict; ``dp_gone``: nodes a peer majority reports gone
    from the data plane (main's dict, the probe of a transient state);
    ``seen_gone``: nodes no answering peer reports enabled (the veto dict),
    by default the same nodes."""
    seen_gone = dp_gone if seen_gone is None else seen_gone
    probes = ({}, {node.get_id(): True for node in dp_gone})
    with patch.object(monitor, "_collect_status_probes", return_value=probes), \
            patch.object(monitor, "_collect_site_gone", return_value={node.get_id(): True for node in seen_gone}):
        return monitor._next_cluster_verdict(cluster.get_id())


def _status(db, cluster, dp_gone=()):
    return _verdict(db, cluster, dp_gone).status


def _proposed(verdict):
    return verdict.status, verdict.site, verdict.state


def _fail_device(db, node, status=NVMeDevice.STATUS_FAILED):
    node = _fresh(db, node)
    node.nvme_devices[0].status = status
    node.write_to_db(db.kv_store)


DRAINING, DOWN = Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN
FENCING, DONE = Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DONE


class TestClusterStatus:
    """3 + 3 nodes, FTT1 per site (ndcs 1, npcs 1)."""

    def test_healthy(self, db):
        cluster = stack._seed_cluster(db)
        stack._sites(db, cluster)
        assert _status(db, cluster) == Cluster.STATUS_ACTIVE

    def test_losing_every_node_of_one_site_does_not_suspend(self, db):
        cluster = stack._seed_cluster(db)
        a, b = stack._sites(db, cluster)
        for node in a:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        assert _proposed(_verdict(db, cluster)) == (Cluster.STATUS_DEGRADED, SITE_A, DOWN)
        _update(db, b[0], status=StorageNode.STATUS_OFFLINE)       # the survivor at its FTT
        assert _status(db, cluster) == Cluster.STATUS_DEGRADED
        _update(db, b[1], status=StorageNode.STATUS_OFFLINE)       # beyond it
        assert _status(db, cluster) == Cluster.STATUS_SUSPENDED

    def test_live_nodes_with_failed_devices_drain_the_site(self, db):
        # beyond its FTT, so it is out and drained - but not down: its nodes run
        cluster = stack._seed_cluster(db)
        a, b = stack._sites(db, cluster)
        for node in a:
            _fail_device(db, node)
        assert _proposed(_verdict(db, cluster)) == (Cluster.STATUS_DEGRADED, SITE_A, DRAINING)

    def test_unreachable_nodes_are_lost_by_data_plane_only(self, db):
        cluster = stack._seed_cluster(db)
        a, b = stack._sites(db, cluster)
        for node in a:
            _update(db, node, status=StorageNode.STATUS_UNREACHABLE)
            _fail_device(db, node)
        # SPDK still serving (no data-plane verdict): the failed devices put the site beyond its FTT
        assert _proposed(_verdict(db, cluster)) == (Cluster.STATUS_DEGRADED, SITE_A, DRAINING)
        assert _proposed(_verdict(db, cluster, dp_gone=a[:2])) == (Cluster.STATUS_DEGRADED, SITE_A, DRAINING)
        assert _proposed(_verdict(db, cluster, dp_gone=a)) == (Cluster.STATUS_DEGRADED, SITE_A, DOWN)

    def test_both_sites_lost_suspends(self, db):
        cluster = stack._seed_cluster(db)
        a, b = stack._sites(db, cluster)
        for node in (*a, *b):
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        assert _status(db, cluster) == Cluster.STATUS_SUSPENDED

    def test_non_sync_site_offline_suspends(self, db):
        cluster = stack._seed_cluster(db)
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
        a, b = stack._sites(db, cluster)
        for node in a:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        assert _status(db, cluster) == Cluster.STATUS_SUSPENDED


# ---------------------------------------------------------------------------
# the lost-site record: its one writer
# ---------------------------------------------------------------------------

def _record(db, cluster_id):
    cluster = db.get_cluster_by_id(cluster_id)
    return cluster.lost_site, cluster.lost_site_state


def _seed_record(db, cluster, site, state, **fields):
    fresh = db.get_cluster_by_id(cluster.get_id())
    fresh.lost_site, fresh.lost_site_state = site, state
    for key, value in fields.items():
        setattr(fresh, key, value)
    fresh.write_to_db(db.kv_store)
    return fresh


class TestLostSiteRecord:

    @pytest.mark.parametrize("before, state, expect", [
        (("", ""), DRAINING, ("",)),                                   # the monitor
        (("", ""), DOWN, ("", DRAINING)),
        ((SITE_A, DRAINING), DOWN, ("", DRAINING)),
        (("", ""), FENCING, ("", FENCING)),                            # the promote runner
        ((SITE_A, DOWN), FENCING, ("", DOWN, FENCING)),
        ((SITE_A, FENCING), FENCING, ("", FENCING)),                   # a retry: already there
        ((SITE_A, FENCING), DONE, (FENCING,)),
    ])
    def test_a_transition(self, db, before, state, expect):
        cluster = _seed_record(db, stack._seed_cluster(db), *before)
        assert src.set_lost_site_state(cluster.get_id(), SITE_A, state, expect) is True
        assert _record(db, cluster.get_id()) == (SITE_A, state)

    @pytest.mark.parametrize("before, state, expect", [
        ((SITE_A, DRAINING), FENCING, ("", FENCING)),                  # not an expected state
        ((SITE_A, DONE), DONE, (FENCING,)),                            # even with the pair in place
        ((SITE_B, DOWN), DOWN, ("", DRAINING, DOWN)),                  # another site recorded
        ((SITE_B, DOWN), DRAINING, ("",)),
    ])
    def test_a_refused_transition_writes_nothing(self, db, before, state, expect):
        cluster = _seed_record(db, stack._seed_cluster(db), *before)
        assert src.set_lost_site_state(cluster.get_id(), SITE_A, state, expect) is False
        assert _record(db, cluster.get_id()) == before

    @pytest.mark.parametrize("state", [DRAINING, DOWN])
    def test_the_monitor_clears_its_own_record(self, db, state):
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, state)
        assert src.clear_monitor_lost_site(cluster.get_id()) is True
        assert _record(db, cluster.get_id()) == ("", "")

    @pytest.mark.parametrize("before", [(SITE_A, FENCING), (SITE_A, DONE), ("", "")])
    def test_the_monitor_does_not_clear_a_fail_over_record(self, db, before):
        cluster = _seed_record(db, stack._seed_cluster(db), *before)
        assert src.clear_monitor_lost_site(cluster.get_id()) is False
        assert _record(db, cluster.get_id()) == before

    @pytest.mark.parametrize("write, mutation, expected", [
        (lambda cid: src.set_lost_site_state(cid, SITE_A, DOWN, ("", DRAINING)),
         "_set_lost_site_record", (SITE_A, DOWN)),
        (lambda cid: src.clear_monitor_lost_site(cid), "_clear_monitor_record", ("", "")),
    ])
    def test_a_field_written_concurrently_survives(self, db, write, mutation, expected):
        # Another flow commits a change of the cluster between the writer's
        # transactional read and its commit (from the writer's mutation, the
        # first time it runs): the writer's transaction conflicts and replays
        # its mutation on the new record, so the other flow's field survives.
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DRAINING)
        real, seen = getattr(src, mutation), []

        def _concurrent(c, *args):
            assert c.get_id() == cluster.get_id()
            seen.append(c.is_re_balancing)
            if len(seen) == 1:
                other = db.get_cluster_by_id(c.get_id())
                other.is_re_balancing = True
                other.write_to_db(db.kv_store)
            return real(c, *args)

        with patch.object(src, mutation, side_effect=_concurrent):
            assert write(cluster.get_id()) is True
        assert len(seen) >= 2 and seen[0] is False and seen[-1] is True     # injected, then replayed
        fresh = db.get_cluster_by_id(cluster.get_id())
        assert fresh.is_re_balancing is True
        assert (fresh.lost_site, fresh.lost_site_state) == expected


class TestRecordSiteReturn:

    @pytest.mark.parametrize("state, active, demoted", [
        (DOWN, SITE_A, ()),             # the monitor's record: nothing was fenced
        (DONE, SITE_B, (SITE_A,)),      # a fail-over: the volume stays fenced on A
    ])
    def test_a_return_leaves_the_volume_sites(self, db, state, active, demoted):
        """The site activation's record step clears the record and leaves every
        volume's active site and fence as they are: after ``down`` the volume
        is still active on A with no fence, after a fail-over it stays fenced
        there until it is promoted back."""
        cluster, a, b, owner_a, _ = stack._two_owner_layout(db)
        _seed_record(db, cluster, SITE_A, state)
        vol = publish._volume(db, cluster, _fresh(db, owner_a), publish._pool(db, cluster),
                              active_site=active, demoted=demoted)
        token = src.begin_site_activation(cluster.get_id(), SITE_A)
        assert token
        assert src.record_site_return(cluster.get_id(), SITE_A, token, {n.get_id() for n in a}) == []
        assert _record(db, cluster.get_id()) == ("", "")
        assert db.get_cluster_by_id(cluster.get_id()).site_activation_since == token
        fresh = db.get_lvol_by_id(vol.get_id())
        assert (fresh.sync_active_site, fresh.sync_demoted_sites) == (active, list(demoted))


# ---------------------------------------------------------------------------
# the monitor pass and the record
# ---------------------------------------------------------------------------

class _Pass:
    """One monitor pass over the real database: the two probes of the
    verdict answer from ``majority`` / ``veto`` (the node ids a peer majority
    reports disconnected / no answering peer reports enabled), every status
    write is recorded with the lost-site record at that moment."""

    def __init__(self, db):
        self.db = db
        self.majority: set = set()
        self.veto: set = set()
        self.status_writes: list = []

    def probes(self, snodes):
        return {}, {node_id: True for node_id in self.majority}

    def site_gone(self, snodes):
        return {node_id: True for node_id in self.veto}

    def run(self, cluster):
        monitor.update_cluster_status(cluster.get_id())
        return self.db.get_cluster_by_id(cluster.get_id())


@pytest.fixture()
def monitor_pass(db, rpcs):
    """The pass with its RPC-backed and threaded side effects patched at their
    boundaries: the probes, the drain's shutdown thread, the activation, the
    ANA fail-over and the distr broadcasts of a status change."""
    from simplyblock_core import cluster_ops
    p = _Pass(db)
    set_status = cluster_ops.set_cluster_status

    def _set_status(cluster_id, status):
        p.status_writes.append((status, _record(db, cluster_id)))
        return set_status(cluster_id, status)

    with patch.object(monitor, "_collect_status_probes", side_effect=p.probes), \
            patch.object(monitor, "_collect_site_gone", side_effect=p.site_gone), \
            patch.object(monitor, "_spawn_recovery_shutdown") as shutdown, \
            patch.object(cluster_ops, "set_cluster_status", side_effect=_set_status), \
            patch.object(cluster_ops, "cluster_activate") as activate, \
            patch.object(monitor, "_maybe_enable_shared_placement"), \
            patch.object(monitor, "_maybe_switch_write_protection"), \
            patch.object(ops, "trigger_ana_failover_for_node"), \
            patch("simplyblock_core.distr_controller.send_node_status_event"), \
            patch("simplyblock_core.distr_controller.send_dev_status_event"):
        p.shutdown, p.activate = shutdown, activate
        yield p
        join_site_activations()


def join_site_activations(timeout=10.0):
    """Wait for every site activation thread a pass started, while the patches
    it runs against are still in place; fails when one does not end."""
    for thread in threading.enumerate():
        if thread.name.startswith("site-activation-"):
            thread.join(timeout)
            assert not thread.is_alive(), f"{thread.name} did not end"


def _site(db, cluster, a_status=StorageNode.STATUS_ONLINE, b_status=StorageNode.STATUS_ONLINE):
    a, b = stack._sites(db, cluster)
    for nodes, status in ((a, a_status), (b, b_status)):
        for node in nodes:
            _update(db, node, status=status)
    return [_fresh(db, n) for n in a], [_fresh(db, n) for n in b]


def _statuses(db, nodes):
    return [(_fresh(db, n).status, _fresh(db, n).nvme_devices[0].status) for n in nodes]


ONLINE_DEV = (StorageNode.STATUS_ONLINE, NVMeDevice.STATUS_ONLINE)


class TestMonitorSettlesAnAbandonedMove:
    """An LVS left ``moving:`` by a promote that ended without settling it:
    the monitor pass probes every member (get_lvstore) and settles the marker
    on the actual leader, and the pass goes on to its site record."""

    def test_the_marker_follows_the_member_that_leads(self, db, rpcs, monitor_pass):
        cluster, a, b, owner = stack._one_owner_layout(db)
        owner = _update(db, owner, lvs_active_site=MOVING_B)
        rpcs.lead(a[0])             # the hand-off never happened: the source primary leads
        with patch.object(monitor, "_apply_site_record", wraps=monitor._apply_site_record) as record:
            monitor_pass.run(cluster)
        assert _fresh(db, owner).lvs_active_site == SITE_A
        record.assert_called_once()
        for node in (*a, *b):
            rpcs[node.get_id()].get_lvstore.assert_any_call("LVS_1")


class TestMonitorRecord:
    """When the storage-node monitor writes, keeps or clears its record of a
    site, by the cluster's stored status."""

    def test_a_working_cluster_records_a_site_that_is_gone(self, db, monitor_pass):
        cluster = stack._seed_cluster(db)
        _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_DEGRADED)

    def test_a_read_only_cluster_records_and_stays_read_only(self, db, monitor_pass):
        cluster = _seed_record(db, stack._seed_cluster(db), "", "", status=Cluster.STATUS_READONLY)
        _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_READONLY)

    @pytest.mark.parametrize("status", [StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_SCHEDULABLE])
    def test_after_down_the_gone_nodes_of_the_site_are_offline(self, db, monitor_pass, status):
        cluster = stack._seed_cluster(db)
        a, b = _site(db, cluster, a_status=status)
        monitor_pass.majority = monitor_pass.veto = {n.get_id() for n in a}
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state) == (SITE_A, DOWN)
        assert _statuses(db, a) == [(StorageNode.STATUS_OFFLINE, NVMeDevice.STATUS_UNAVAILABLE)] * 3
        assert _statuses(db, b) == [ONLINE_DEV] * 3

    def test_a_suspended_cluster_coming_back_records_nothing(self, db, monitor_pass):
        # a whole-cluster recovery in flight: B back, A half up
        cluster = _seed_record(db, stack._seed_cluster(db), "", "", status=Cluster.STATUS_SUSPENDED,
                               suspend_drain_complete=True)
        a, b = _site(db, cluster)
        for node in a[:2]:
            _update(db, node, status=StorageNode.STATUS_RESTARTING)
            _fail_device(db, node, NVMeDevice.STATUS_UNAVAILABLE)
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == ("", "", Cluster.STATUS_SUSPENDED)
        monitor_pass.activate.assert_not_called()

    def test_an_activation_in_flight_records_nothing(self, db, monitor_pass):
        now = datetime.now(UTC).isoformat()
        cluster = _seed_record(db, stack._seed_cluster(db), "", "", status=Cluster.STATUS_IN_ACTIVATION,
                               in_activation_since=now, activation_heartbeat=now)
        _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == ("", "", Cluster.STATUS_IN_ACTIVATION)

    @pytest.mark.parametrize("stored", [Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY])
    def test_both_sites_out_clear_the_record_then_suspend(self, db, monitor_pass, stored):
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DOWN, status=stored)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        for node in b[:2]:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == ("", "", Cluster.STATUS_SUSPENDED)
        assert (Cluster.STATUS_SUSPENDED, ("", "")) in monitor_pass.status_writes

    @pytest.mark.parametrize("state", [DRAINING, DOWN])
    def test_a_suspension_by_cluster_restart_clears_the_record(self, db, monitor_pass, state):
        from simplyblock_core import cluster_ops
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, state, status=Cluster.STATUS_DEGRADED)
        _site(db, cluster)
        cluster_ops.cluster_restart(cluster.get_id())
        assert _record(db, cluster.get_id()) == (SITE_A, state)
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == ("", "", Cluster.STATUS_SUSPENDED)

    def test_a_fail_over_record_stays_under_a_suspension(self, db, monitor_pass):
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DONE, status=Cluster.STATUS_SUSPENDED,
                               suspend_drain_complete=True)
        _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state) == (SITE_A, DONE)

    # the watchdogs write SUSPENDED too

    @pytest.mark.parametrize("state, heartbeat", [(DRAINING, "stale"), (DOWN, "")])
    def test_a_wedged_activation_clears_the_record(
            self, db, monitor_pass, state, heartbeat):
        since = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, state, status=Cluster.STATUS_IN_ACTIVATION,
                               in_activation_since=since, activation_heartbeat=since if heartbeat else "")
        _site(db, cluster)
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == ("", "", Cluster.STATUS_SUSPENDED)
        assert monitor_pass.status_writes[0] == (Cluster.STATUS_SUSPENDED, ("", ""))

    def test_a_wedged_activation_keeps_a_fail_over_record(self, db, monitor_pass):
        since = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DONE, status=Cluster.STATUS_IN_ACTIVATION,
                               in_activation_since=since, activation_heartbeat=since)
        _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)      # not back: no site return
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DONE, Cluster.STATUS_SUSPENDED)

    def test_an_abandoned_shrink_clears_the_record(self, db, monitor_pass):
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DOWN, status=Cluster.STATUS_IN_SHRINK)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        for node in b[:2]:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        after = monitor_pass.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == ("", "", Cluster.STATUS_SUSPENDED)
        assert monitor_pass.status_writes[0] == (Cluster.STATUS_SUSPENDED, ("", ""))

    # the down sweep is a level rule

    @pytest.mark.parametrize("status", [StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_SCHEDULABLE])
    def test_a_gone_node_of_a_site_already_down_is_set_offline(self, db, monitor_pass, status):
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DOWN, status=Cluster.STATUS_DEGRADED)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=status)
        monitor_pass.majority = monitor_pass.veto = {a[0].get_id()}
        monitor_pass.run(cluster)
        assert _statuses(db, a[:1]) == [(StorageNode.STATUS_OFFLINE, NVMeDevice.STATUS_UNAVAILABLE)]
        assert _statuses(db, b) == [ONLINE_DEV] * 3

    @pytest.mark.parametrize("status", [StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_SCHEDULABLE])
    def test_a_vetoed_node_of_a_site_down_is_left_alone(self, db, monitor_pass, status):
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DOWN, status=Cluster.STATUS_DEGRADED)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=status)
        monitor_pass.majority = {a[0].get_id()}       # a majority lost it, one peer still sees it
        monitor_pass.run(cluster)
        assert _statuses(db, a[:1]) == [(status, NVMeDevice.STATUS_ONLINE)]
        assert _statuses(db, b) == [ONLINE_DEV] * 3

    def test_without_a_witness_no_node_is_set_offline(self, db, monitor_pass):
        # the management node lost its network: every vote 0/0, so the
        # majority dict (nobody gone) is in force
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DOWN, status=Cluster.STATUS_DEGRADED)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE, b_status=StorageNode.STATUS_UNREACHABLE)
        _update(db, a[0], status=StorageNode.STATUS_UNREACHABLE)
        monitor_pass.veto = {n.get_id() for n in (a[0], *b)}
        after = monitor_pass.run(cluster)
        assert _statuses(db, a[:1]) == [(StorageNode.STATUS_UNREACHABLE, NVMeDevice.STATUS_ONLINE)]
        assert _statuses(db, b) == [(StorageNode.STATUS_UNREACHABLE, NVMeDevice.STATUS_ONLINE)] * 3
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_DEGRADED)

    @pytest.mark.parametrize("status", [StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_SCHEDULABLE])
    @pytest.mark.parametrize("gone_by_both", [False, True])
    def test_with_no_healthy_site_offline_needs_both_dicts(
            self, db, monitor_pass, status, gone_by_both):
        # A coming back (two nodes ONLINE, the voters); B's b0, b1 UNREACHABLE
        # and abstained on (gone by the veto only): B is out by the veto rule
        # and live by the majority, which decides the status - a0, reported
        # disconnected by a majority, goes OFFLINE only when no answering
        # peer still reports it enabled
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DOWN, status=Cluster.STATUS_DEGRADED)
        a, b = _site(db, cluster)
        _update(db, a[0], status=status)
        for node in b[:2]:
            _update(db, node, status=StorageNode.STATUS_UNREACHABLE)
        monitor_pass.majority = {a[0].get_id()}
        monitor_pass.veto = {n.get_id() for n in b[:2]} | ({a[0].get_id()} if gone_by_both else set())
        after = monitor_pass.run(cluster)
        expected = ((StorageNode.STATUS_OFFLINE, NVMeDevice.STATUS_UNAVAILABLE) if gone_by_both
                    else (status, NVMeDevice.STATUS_ONLINE))
        assert _statuses(db, a) == [expected, ONLINE_DEV, ONLINE_DEV]
        assert _statuses(db, b[:2]) == [(StorageNode.STATUS_UNREACHABLE, NVMeDevice.STATUS_ONLINE)] * 2
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_DEGRADED)


# ---------------------------------------------------------------------------
# a follower opens only with its redirect to the leader
# ---------------------------------------------------------------------------

def _hub(spdk, node, lvs_name, *, connected, leader=False):
    """The node's instance of ``lvs_name`` reports its redirect as SPDK does
    (get_lvstore: ``connect_state`` / ``lvs_redirect``), tracking
    bdev_lvol_connect_hublvol."""
    target = spdk[node.get_id()]
    state = {"connected": connected}

    def _lvstore(name):
        if name != lvs_name:
            return None
        return {"name": lvs_name, "lvs leadership": leader, "lvs_secondary": True,
                "lvs_redirect": state["connected"], "connect_state": state["connected"]}

    def _connect(*args, **kwargs):
        state["connected"] = True
        return True

    target.get_lvstore.side_effect = _lvstore
    target.bdev_lvol_connect_hublvol.side_effect = _connect
    return state


def _retries(db, cluster, node, vol):
    return [t for t in db.get_job_tasks(cluster.get_id())
            if t.function_name == JobSchedule.FN_LVOL_SYNC_OP and t.node_id == node.get_id()
            and t.function_params.get("lvol_id") == vol.get_id() and t.status != JobSchedule.STATUS_DONE]


def _served(db, spdk, *, active_site=SITE_A):
    """One volume of LVS_1 registered on all six paths, led from ``active_site``."""
    cluster, a, b, owner = publish._layout(db, active_site="" if active_site == SITE_A else active_site)
    vol = publish._volume(db, cluster, owner, publish._pool(db, cluster), active_site=active_site)
    leader = owner if active_site == SITE_A else b[0]
    publish._register_everywhere(db, vol, leader, [*a, *b])
    vol.write_to_db(db.kv_store)
    spdk[owner.get_id()]
    return cluster, a, b, _fresh(db, owner), db.get_lvol_by_id(vol.get_id())


def _set(spdk, node, vol, state):
    spdk[node.get_id()].nvmf_subsystem_listener_set_ana_state(
        vol.nqn, publish._ip(node), node.get_lvol_subsys_port(vol.lvs_name), ana=state, anagrpid=vol.ns_id)


def _sweep(cluster, owner, vol):
    from simplyblock_core.services import lvol_monitor
    lvol_monitor.check_node(cluster, owner, [vol], subsys_check=True)


class TestFollowerRedirect:

    def test_an_unwired_follower_is_closed_and_retried(self, db, spdk):
        cluster, a, b, owner, vol = _served(db, spdk)
        _hub(spdk, a[1], "LVS_1", connected=False)
        _hub(spdk, a[2], "LVS_1", connected=True)
        for node in (a[1], a[2]):
            ops._set_lvol_ana_on_node(vol, node, "non_optimized")
        assert publish._seen(spdk, a[1], vol) == "inaccessible"
        assert publish._seen(spdk, a[2], vol) == "non_optimized"
        assert len(_retries(db, cluster, a[1], vol)) == 1 and _retries(db, cluster, a[2], vol) == []

    def test_the_group_writer_opens_a_follower_once_wired(self, db, spdk):
        cluster, a, b, owner, vol = _served(db, spdk)
        state = _hub(spdk, a[1], "LVS_1", connected=False)
        port = a[1].get_lvol_subsys_port("LVS_1")
        ok, err = ops.apply_sync_ana_groups(spdk[a[1].get_id()], vol, a[1], port, "non_optimized")
        assert ok and err is None
        assert publish._seen(spdk, a[1], vol) == "inaccessible"
        assert len(_retries(db, cluster, a[1], vol)) == 1
        state["connected"] = True
        ops.apply_sync_ana_groups(spdk[a[1].get_id()], vol, a[1], port, "non_optimized")
        assert publish._seen(spdk, a[1], vol) == "non_optimized"

    def test_a_leader_or_an_unknown_redirect_is_not_closed(self, db, spdk):
        cluster, a, b, owner, vol = _served(db, spdk)
        _hub(spdk, a[1], "LVS_1", connected=False, leader=True)     # a leader is not judged
        spdk[a[2].get_id()].get_lvstore.side_effect = RPCException("busy")
        for node in (a[1], a[2]):
            ops._set_lvol_ana_on_node(vol, node, "non_optimized")
        assert [publish._seen(spdk, n, vol) for n in (a[1], a[2])] == ["non_optimized"] * 2

    def test_the_monitor_never_opens_an_unwired_follower(self, db, spdk):
        # LVS_1 led from b0; Pass 3 wired b1 but not b2, the activation failed
        # before Pass 4: both followers still closed.
        cluster, a, b, owner, vol = _served(db, spdk, active_site=SITE_B)
        _hub(spdk, b[1], "LVS_1", connected=True)
        _hub(spdk, b[2], "LVS_1", connected=False)
        for node in (b[1], b[2]):
            _set(spdk, node, vol, "inaccessible")
        # restored to SUSPENDED: no ANA-drift repair at all
        cluster.status = Cluster.STATUS_SUSPENDED
        cluster.write_to_db(db.kv_store)
        _sweep(cluster, owner, vol)
        assert [publish._seen(spdk, n, vol) for n in (b[1], b[2])] == ["inaccessible"] * 2
        # restored to ACTIVE (a forced re-activation): the wired follower opens,
        # the unwired one stays closed and reads as no drift
        cluster.status = Cluster.STATUS_ACTIVE
        cluster.write_to_db(db.kv_store)
        _sweep(cluster, owner, vol)
        assert publish._seen(spdk, b[1], vol) == "non_optimized"
        assert publish._seen(spdk, b[2], vol) == "inaccessible"

    def test_an_open_follower_whose_redirect_is_gone_is_closed(self, db, spdk):
        from simplyblock_core.services import lvol_monitor
        cluster, a, b, owner, vol = _served(db, spdk, active_site=SITE_B)
        _hub(spdk, b[2], "LVS_1", connected=False)
        assert publish._seen(spdk, b[2], vol) == "non_optimized"
        with patch.object(lvol_monitor, "try_repair_lvol_on_non_leader",
                          wraps=lvol_monitor.try_repair_lvol_on_non_leader) as repair:
            _sweep(cluster, owner, vol)
        spdk[b[2].get_id()].get_lvstore.assert_any_call("LVS_1")
        assert b[2].get_id() in {c.args[1].get_id() for c in repair.call_args_list}
        assert db.get_lvol_by_id(vol.get_id()).health_check is False
        assert publish._seen(spdk, b[2], vol) == "inaccessible"
        assert publish._seen(spdk, b[0], vol) == "optimized"


class TestInSiteFailover:
    """The active triplet's secondary of an OFFLINE primary stays
    optimized for every later writer, before and after it takes the
    leadership, whatever its (gone) redirect reports."""

    def _failed_over(self, db, spdk):
        cluster, a, b, owner, vol = _served(db, spdk)
        owner = _update(db, owner, status=StorageNode.STATUS_OFFLINE)
        ops.trigger_ana_failover_for_node(owner)
        assert publish._seen(spdk, a[1], vol) == "optimized"
        return cluster, a, b, owner, vol

    @pytest.mark.parametrize("leader", [False, True])
    def test_the_monitor_keeps_the_secondary_optimized(self, db, spdk, leader):
        cluster, a, b, owner, vol = self._failed_over(db, spdk)
        _hub(spdk, a[1], "LVS_1", connected=False, leader=leader)
        _sweep(cluster, owner, vol)
        assert publish._seen(spdk, a[1], vol) == "optimized"
        assert publish._seen(spdk, a[2], vol) == "non_optimized"
        assert _retries(db, cluster, a[1], vol) == []

    def test_a_retried_registration_republishes_it_optimized(self, db, spdk):
        cluster, a, b, owner, vol = self._failed_over(db, spdk)
        _hub(spdk, a[1], "LVS_1", connected=False)
        _set(spdk, a[1], vol, "inaccessible")
        ops.queue_sync_ana_retry(a[1], [vol])
        (task,) = _retries(db, cluster, a[1], vol)
        drive(tasks_runner_sync_lvol_del.SPEC, task)
        assert publish._seen(spdk, a[1], vol) == "optimized"
        ops._set_lvol_ana_on_node(vol, a[1], "non_optimized")
        assert publish._seen(spdk, a[1], vol) == "optimized"

    def test_the_primary_restarting_hands_back_to_the_failback(self, db, spdk):
        cluster, a, b, owner, vol = self._failed_over(db, spdk)
        _update(db, owner, status=StorageNode.STATUS_RESTARTING)
        _hub(spdk, a[1], "LVS_1", connected=True)
        ops._set_lvol_ana_on_node(vol, a[1], "non_optimized")
        assert publish._seen(spdk, a[1], vol) == "non_optimized"
