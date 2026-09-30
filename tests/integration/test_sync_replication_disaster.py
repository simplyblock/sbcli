"""Disaster fail-over and site return on a sync-replication cluster, against
the real FoundationDB: the forced promote of a volume whose LVS is led from a
lost site (the API row, the disaster gate scoped to the LVS it moves, the
runner's site steps and their ordering, retries from ``fencing``, the
check of the lost site's members before each hand-off), the grant guards that
keep a lost site from leading, the leaderless rebuild of a lost site's nodes,
the site return (election of the LVS still led from the returned site,
readiness, catch-up, the transactional clear) and the re-activation of a
cluster with an LVS led from its remote triplet.

Storage nodes are the stateful nvmf fake of
test_sync_replication_lvol_publish.py (or the plain RPC mocks of
test_sync_replication_lvs_stack.py for the rebuild paths); the liveness probes of the
lost site and the fenced hand-off are mocked at their boundary. The pure
rules are in tests/unit/test_sync_replication_disaster.py.
"""
import threading
from contextlib import ExitStack
from unittest.mock import patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.exceptions import SyncGateError, SyncPromoteRefusedError, SyncSiteOfflineError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import tasks_runner_sync_promote as runner
from tests.integration import test_sync_replication_demote_promote as dp
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
MOVING_B = "moving:" + SITE_B

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
env = stack.env
spdk = publish.spdk
moves = dp.moves
_clean_leader_caches = publish._clean_leader_caches
_fresh, _update = stack._fresh, stack._update


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

class _Evidence:
    """The liveness probes of the lost site: every node down unless named."""

    def __init__(self):
        self.alive: set = set()        # SPDK seen by the management plane
        self.connected: set = set()    # still reached by the surviving data plane
        self.calls: list = []

    def spdk_answers(self, node):
        self.calls.append(("spdk", node.get_id()))
        return node.get_id() in self.alive

    def data_plane_gone(self, node):
        self.calls.append(("dp", node.get_id()))
        return node.get_id() not in self.connected


@pytest.fixture()
def evidence():
    """The liveness probes, the planned gate refused (a disaster never uses
    it) and the best-effort device broadcast of device_set_state silenced, so
    every ``distr_status_events_update`` a node records is the strict fence."""
    e = _Evidence()
    with patch.object(runner, "_spdk_answers", side_effect=e.spdk_answers), \
            patch("simplyblock_core.distr_controller.send_dev_status_event"), \
            patch.object(runner, "_data_plane_gone", side_effect=e.data_plane_gone), \
            patch.object(src, "check_gate", side_effect=AssertionError("planned gate used")):
        yield e


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
            spdk[node.get_id()].bdev_lvol_get_lvstores.side_effect = RPCException("connection refused")
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

    def test_a_forced_promote_of_a_lost_site_queues_a_disaster_task(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        with pytest.raises(SyncSiteOfflineError):
            lost.promote("v1a", force=False)
        result = lost.promote("v1a")
        task = lost.task(result.task_id)
        assert result.in_progress
        assert task.function_params["lost_site"] == SITE_A
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

    def test_the_lvs_unsynced_before_the_loss_is_refused_with_nothing_queued(self, db, spdk, evidence):
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


# ---------------------------------------------------------------------------
# the site steps
# ---------------------------------------------------------------------------

class TestSiteSteps:

    def test_the_site_is_fenced_before_the_move_and_the_volume_opens_on_s(
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
        assert seen["cluster"] == (SITE_A, src.LOST_SITE_DONE)
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

    def test_a_failed_device_of_the_site_keeps_its_state_and_is_delivered(self, db, spdk, evidence, moves):
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

    def test_an_unreachable_node_whose_spdk_answers_is_not_lost(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        _update(db, lost.a[2], status=StorageNode.STATUS_UNREACHABLE)
        result = lost.promote("v1a")
        evidence.alive.add(lost.a[2].get_id())
        lost.run(result.task_id)
        assert "SPDK answers" in lost.task(result.task_id).function_result
        assert lost.cluster_now().lost_site == ""
        assert moves.calls == []

    def test_a_node_the_data_plane_still_reaches_is_not_lost(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        evidence.connected.add(lost.a[1].get_id())
        result = lost.promote("v1a")
        lost.run(result.task_id)
        assert "data plane" in lost.task(result.task_id).function_result
        assert lost.cluster_now().lost_site == ""

    def test_a_vote_turning_connected_before_done_leaves_the_fence_open(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        real = evidence.data_plane_gone
        votes = {"n": 0}

        def _vote(node):
            if node.get_id() == lost.a[0].get_id():
                votes["n"] += 1
                if votes["n"] > 1:
                    return False
            return real(node)

        result = lost.promote("v1a")
        with patch.object(runner, "_data_plane_gone", side_effect=_vote):
            lost.run(result.task_id)
        assert "before the fence is recorded done" in lost.task(result.task_id).function_result
        cluster = lost.cluster_now()
        assert (cluster.lost_site, cluster.lost_site_state) == (SITE_A, src.LOST_SITE_FENCING)
        assert moves.calls == []

    def test_a_missing_ack_fails_the_phase_and_a_retry_completes_it(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        spdk[lost.b[2].get_id()].distr_status_events_update.side_effect = [RPCException("gone"), True, True]
        first = lost.promote("v1a")
        lost.run(first.task_id)
        assert "could not be marked unavailable" in lost.task(first.task_id).function_result
        assert lost.cluster_now().lost_site_state == src.LOST_SITE_FENCING
        # the retry (a new promote call) redoes the site steps from ``fencing``
        second = lost.promote("v1a")
        assert second.task_id != first.task_id
        assert lost.run(second.task_id)
        assert lost.cluster_now().lost_site_state == src.LOST_SITE_DONE
        assert lost.vol("v1a").sync_active_site == SITE_B

    def test_a_down_or_unreachable_surviving_node_must_acknowledge(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        _update(db, lost.b[1], status=StorageNode.STATUS_DOWN)
        _update(db, lost.b[2], status=StorageNode.STATUS_UNREACHABLE)
        spdk[lost.b[2].get_id()].distr_status_events_update.side_effect = RPCException("mgmt down")
        result = lost.promote("v1a")
        lost.run(result.task_id)
        assert lost.b[2].get_id() in lost.task(result.task_id).function_result
        assert lost.events_to(spdk, lost.b[1])          # the DOWN node got them
        assert lost.cluster_now().lost_site_state == src.LOST_SITE_FENCING

    def test_a_node_restarting_during_the_first_pass_gets_the_devices_before_done(
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

    def test_a_lost_site_member_reporting_the_leadership_aborts_its_hand_off(
            self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        assert lost.run(lost.promote("v1a").task_id)
        answer = spdk[lost.a[1].get_id()].bdev_lvol_get_lvstores
        answer.side_effect, answer.return_value = None, [{"lvs leadership": True}]
        result = lost.promote("v2a")
        lost.run(result.task_id)
        assert "reports the leadership" in lost.task(result.task_id).function_result
        assert _fresh(db, lost.owner2).lvs_active_site == ""       # the marker undone
        assert [c[0] for c in moves.calls] == ["LVS_1"]

    def test_a_lost_site_member_answering_as_non_leader_is_no_obstacle(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        assert lost.run(lost.promote("v1a").task_id)
        answer = spdk[lost.a[1].get_id()].bdev_lvol_get_lvstores
        answer.side_effect, answer.return_value = None, [{"lvs leadership": False}]
        assert lost.run(lost.promote("v2a").task_id)
        assert lost.vol("v2a").sync_active_site == SITE_B

    def test_a_sibling_opens_on_s_after_the_fail_over(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        assert lost.run(lost.promote("v1a").task_id)
        _desync(lost.a[2], vuid=21)             # LVS_2 unsynced: does not concern LVS_1
        result = lost.promote("v1b", force=False)
        assert lost.run(result.task_id)
        v1b = lost.vol("v1b")
        assert (v1b.sync_active_site, v1b.sync_demoted_sites) == (SITE_B, [SITE_A])
        assert dp._states(spdk, lost.b, v1b) == ["optimized", "non_optimized", "non_optimized"]

    @pytest.mark.parametrize("state", ["fencing", "moving"])
    def test_a_returning_node_of_the_site_keeps_every_path_closed(self, db, spdk, evidence, state):
        lost = _Lost(db, spdk)
        if state == "fencing":
            runner._set_lost_site_state(lost.cluster.get_id(), SITE_A, src.LOST_SITE_FENCING, ("",))
        else:
            runner._set_lost_site_state(lost.cluster.get_id(), SITE_A, src.LOST_SITE_DONE, ("",))
            _update(db, lost.owner1, lvs_active_site=MOVING_B)
        for node in lost.a:
            node = _update(db, node, status=StorageNode.STATUS_ONLINE)
            for name in ("v1a", "v1b"):
                assert ops.lvol_ana_state(lost.vol(name), node, "optimized") == "inaccessible"


# ---------------------------------------------------------------------------
# no leadership on a lost site
# ---------------------------------------------------------------------------

class TestLostSiteGrantGuards:

    @pytest.mark.parametrize("state", ["fencing", "done"])
    def test_no_recovery_grant_and_no_leader_rebuild_on_the_lost_site(self, db, rpcs, env, state):
        cluster, a, b, owner = stack._one_owner_layout(db, lost_site=SITE_A)
        cluster.lost_site_state = state
        cluster.write_to_db(db.kv_store)
        with ops._sync_grant_guard(cluster.get_id(), "LVS_1", owner.get_id(), a[0].get_id()) as ok:
            assert ok is False
        with pytest.raises(ops.LVSLeadershipElsewhereError, match="lost site"):
            ops.recreate_lvstore(_fresh(db, owner))

    def test_an_abandoned_move_to_the_lost_site_is_not_granted_there(self, db, rpcs, env):
        cluster, a, b, owner = stack._one_owner_layout(db, lost_site=SITE_A)
        cluster.lost_site_state = src.LOST_SITE_FENCING
        cluster.write_to_db(db.kv_store)
        _update(db, owner, lvs_active_site="moving:" + SITE_A)
        with patch.object(ops, "_taker_jm_quorum_ok", return_value=True), \
                patch.object(ops, "_move_lvs_leadership_locked") as move:
            assert ops.reconcile_lvs_move(owner.get_id()) is None
        move.assert_not_called()
        with pytest.raises(ops.LVSMoveChangedError, match="lost site"):
            ops.move_lvs_leadership(owner.get_id(), "moving:" + SITE_A, SITE_A, taker_id=a[0].get_id())


# ---------------------------------------------------------------------------
# the lost site's nodes come back
# ---------------------------------------------------------------------------

class TestLostSiteRestart:

    def test_an_lvs_still_led_from_the_lost_site_is_rebuilt_leaderless(self, db, rpcs, hub, env):
        # Real rebuild of a home secondary: no leader anywhere, the rest of A down.
        _, a, b, owner = stack._one_owner_layout(db, lost_site=SITE_A)
        env.down.update(n.get_id() for n in a if n.get_id() != a[1].get_id())
        with patch.object(ops, "recreate_lvstore") as leader_rebuild, \
                patch.object(ops, "_rebuild_remote_instance", return_value=True):
            assert ops.recreate_all_lvstores(_fresh(db, a[1])) is True
        leader_rebuild.assert_not_called()
        rpc = rpcs[a[1].get_id()]
        assert {c.kwargs["name"] for c in rpc.bdev_distrib_create.call_args_list} == {
            "distrib_11", "distrib_12"}
        assert stack._roles(rpc) == ["secondary"]
        assert not stack._took_leadership(rpc)
        for name, mock in hub.items():
            assert not mock.called, name
        assert env.blocked() == []

    def test_routing_per_lvs(self, db, rpcs, env):
        # LVS_1 (a0) moved to B; LVS_3 (a1's own) still led from A; LVS_2 (b0,
        # S-home) led from its remote triplet on A, whose primary is a1.
        cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db, lost_site=SITE_A)
        owner_a1 = stack._make_owner(db, a[1], [a[1], a[2], a[0]], b, lvs_id=3)
        _update(db, owner_b, remote_primary_node_id=a[1].get_id(), remote_secondary_node_id=a[0].get_id(),
                lvs_active_site=SITE_A)
        rpcs.lead(b[0])
        with patch.object(ops, "recreate_lvstore") as leader_rebuild, \
                patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True) as nl, \
                patch.object(ops, "_recreate_lvstore_on_non_leader_impl", return_value=True) as impl:
            assert ops.recreate_all_lvstores(_fresh(db, owner_a1)) is True
        leader_rebuild.assert_not_called()
        calls = {c.args[2].get_id(): (c.args[1].get_id(), c.kwargs) for c in nl.call_args_list}
        assert calls[owner_a1.get_id()] == (owner_a1.get_id(),
                                            {"activation_mode": True, "force": False, "skip_hublvol": True})
        assert calls[owner_a.get_id()] == (b[0].get_id(), {"force": False})
        (remote,) = impl.call_args_list
        assert (remote.args[0].get_id(), remote.args[1].get_id(), remote.args[2].get_id()) == (
            a[1].get_id(), owner_b.get_id(), owner_b.get_id())
        assert remote.kwargs == {"activation_mode": True, "force": False, "skip_hublvol": True}

    def test_a_whole_site_comes_back_one_node_after_the_other(self, db, rpcs, hub, env):
        # FD off: restarts are sequential. LVS_1 moved to B (behind b0), LVS_3
        # (a1) and LVS_2 (b0, S-home) still led from A: rebuilt leaderless.
        cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db, lost_site=SITE_A)
        owner_a1 = stack._make_owner(db, a[1], [a[1], a[2], a[0]], b, lvs_id=3)   # still led from A
        _update(db, owner_b, lvs_active_site=SITE_A)        # S-home, led from its triplet on A
        rpcs.lead(b[0])
        env.down.update(n.get_id() for n in a)
        for node in a:
            env.down.discard(node.get_id())
            assert ops.recreate_all_lvstores(_fresh(db, node)) is True, node.get_id()
            assert not stack._took_leadership(rpcs[node.get_id()])
        for name, mock in hub.items():
            assert [c for c in mock.call_args_list if c.args[0].site == SITE_A] == [], name
        assert _fresh(db, owner_a1).lvs_active_site == ""


# ---------------------------------------------------------------------------
# the election on the returned site
# ---------------------------------------------------------------------------

@pytest.fixture()
def transfer():
    with patch.object(ops, "transfer_lvs_leadership") as t, \
            patch.object(ops, "_taker_jm_quorum_ok", return_value=True) as quorum:
        t.quorum = quorum
        yield t


class TestElection:

    def _returned(self, db, rpcs):
        cluster, a, b, owner = stack._one_owner_layout(db, lost_site=SITE_A)
        cluster.lost_site_state = src.LOST_SITE_DONE
        cluster.write_to_db(db.kv_store)
        return cluster, a, b, owner

    def test_nobody_leads_the_active_primary_is_elected(self, db, rpcs, env, transfer):
        cluster, a, b, owner = self._returned(db, rpcs)
        assert ops.elect_lvs_leader_on_site(owner.get_id()) == a[0].get_id()
        (call,) = transfer.call_args_list
        assert call.args[0] is None and call.args[1].get_id() == a[0].get_id()
        assert {n.get_id() for n in call.args[2]} == {n.get_id() for n in (*a[1:], *b)}
        assert call.kwargs["lvs_node"].get_id() == owner.get_id() and call.kwargs["reload_metadata"]

    def test_the_remote_primary_of_an_s_home_lvs_led_from_the_lost_site(self, db, rpcs, env, transfer):
        cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db)
        cluster.lost_site, cluster.lost_site_state = SITE_A, src.LOST_SITE_DONE
        cluster.write_to_db(db.kv_store)
        _update(db, owner_b, lvs_active_site=SITE_A)
        assert ops.elect_lvs_leader_on_site(owner_b.get_id()) == owner_b.remote_primary_node_id

    def test_without_the_local_journal_quorum_nothing_is_granted(self, db, rpcs, env, transfer):
        _, a, b, owner = self._returned(db, rpcs)
        transfer.quorum.return_value = False
        assert ops.elect_lvs_leader_on_site(owner.get_id()) is None
        transfer.assert_not_called()

    def test_a_member_of_the_triplet_leading_already_needs_no_transfer(self, db, rpcs, env, transfer):
        _, a, b, owner = self._returned(db, rpcs)
        rpcs.lead(a[1])
        assert ops.elect_lvs_leader_on_site(owner.get_id()) == a[1].get_id()
        transfer.assert_not_called()

    def test_a_leader_on_the_other_site_or_two_leaders_stop_it(self, db, rpcs, env, transfer):
        _, a, b, owner = self._returned(db, rpcs)
        rpcs.lead(b[0])
        assert ops.elect_lvs_leader_on_site(owner.get_id()) is None
        rpcs.lead(a[0])
        assert ops.elect_lvs_leader_on_site(owner.get_id()) is None
        transfer.assert_not_called()

    def test_a_silent_member_stops_it(self, db, rpcs, env, transfer):
        _, a, b, owner = self._returned(db, rpcs)
        rpcs[b[2].get_id()].bdev_lvol_get_lvstores.side_effect = RPCException("down")
        assert ops.elect_lvs_leader_on_site(owner.get_id()) is None
        transfer.assert_not_called()

    def test_a_task_owning_the_leadership_stops_it(self, db, rpcs, env, transfer):
        _, a, b, owner = self._returned(db, rpcs)
        with patch.object(ops, "_leadership_moving_tasks_active", return_value=True):
            assert ops.elect_lvs_leader_on_site(owner.get_id()) is None
        transfer.assert_not_called()

    def test_a_failed_transfer_is_retried_by_the_next_call(self, db, rpcs, env, transfer):
        _, a, b, owner = self._returned(db, rpcs)
        transfer.side_effect = [ops.LeadershipTransferError("aborted"), None]
        assert ops.elect_lvs_leader_on_site(owner.get_id()) is None
        assert ops.elect_lvs_leader_on_site(owner.get_id()) == a[0].get_id()

    def test_an_lvs_moved_away_is_not_elected(self, db, rpcs, env, transfer):
        _, a, b, owner = self._returned(db, rpcs)
        _update(db, owner, lvs_active_site=SITE_B)
        assert ops.elect_lvs_leader_on_site(owner.get_id()) is None
        transfer.assert_not_called()


# ---------------------------------------------------------------------------
# the site return
# ---------------------------------------------------------------------------

class _Return:
    """Site A lost and back: LVS_1 (a0) moved to B, LVS_3 (a1) and LVS_4
    (a2, no volumes) still led from A, LVS_2 (b0, S-home) led from its remote
    triplet on A. Every node online, every device online."""

    def __init__(self, db, rpcs, state=src.LOST_SITE_DONE):
        self.cluster, self.a, self.b, self.owner_a, self.owner_b = stack._two_owner_layout(
            db, lost_site=SITE_A)
        self.cluster.lost_site_state = state
        self.cluster.write_to_db(db.kv_store)
        a, b = self.a, self.b
        self.owner_a1 = stack._make_owner(db, a[1], [a[1], a[2], a[0]], b, lvs_id=3)
        self.owner_a2 = stack._make_owner(db, a[2], [a[2], a[0], a[1]], b, lvs_id=4)
        self.owner_b = _update(db, self.owner_b, lvs_active_site=SITE_A)
        self.db, self.rpcs = db, rpcs
        self.elected: list = []
        self.answers: dict = {}
        leaders = {self.owner_a1.get_id(): a[1], self.owner_a2.get_id(): a[2],
                   self.owner_b.get_id(): _fresh(db, a[0])}
        self.leaders = {k: v.get_id() for k, v in leaders.items()}
        for node in (*a, *b):
            rpcs[node.get_id()].distr_sync_replication_status.side_effect = self._answer(node.get_id())

    def _answer(self, node_id):
        def answer(name=None):
            out = []
            for owner in (self.owner_a1, self.owner_a2, self.owner_b):
                if self.leaders[owner.get_id()] != node_id:
                    continue
                for bdev in owner.lvstore_stack:
                    if bdev["type"] == "bdev_distr":
                        elem = {"name": bdev["name"], "sync_replication_mode": "full",
                                "status": "replica_unsynced"}
                        elem.update(self.answers.get(bdev["name"], {}))
                        out.append(elem)
            return out
        return answer

    def elect(self, owner_id):
        self.elected.append(owner_id)
        return self.leaders.get(owner_id)

    def settle(self):
        with patch.object(ops, "elect_lvs_leader_on_site", side_effect=self.elect):
            return src.settle_site_return(self.cluster.get_id())

    def resync_lvs(self):
        return sorted(t.function_params["lvs_name"]
                      for t in self.db.get_active_sync_resync_tasks(self.cluster.get_id()))

    def cluster_now(self):
        return self.db.get_cluster_by_id(self.cluster.get_id())


class TestSiteReturn:

    def test_the_whole_site_back_elects_schedules_and_clears(self, db, rpcs):
        ret = _Return(db, rpcs)
        assert ret.settle() is True
        assert sorted(ret.elected) == sorted([ret.owner_a1.get_id(), ret.owner_a2.get_id(),
                                              ret.owner_b.get_id()])       # never the moved LVS_1
        assert ret.resync_lvs() == ["LVS_1", "LVS_2", "LVS_3", "LVS_4"]
        cluster = ret.cluster_now()
        assert (cluster.lost_site, cluster.lost_site_state) == ("", "")

    @pytest.mark.parametrize("status,blocks", [(NVMeDevice.STATUS_FAILED, True),
                                               (NVMeDevice.STATUS_UNAVAILABLE, True),
                                               (NVMeDevice.STATUS_FAILED_AND_MIGRATED, False),
                                               (NVMeDevice.STATUS_REMOVED, False)])
    def test_a_device_of_the_site_not_online(self, db, rpcs, status, blocks):
        ret = _Return(db, rpcs)
        node = _fresh(db, ret.a[1])
        node.nvme_devices[0].status = status
        node.write_to_db(db.kv_store)
        assert ret.settle() is (not blocks)
        assert (ret.cluster_now().lost_site == SITE_A) is blocks

    def test_a_suspended_node_is_not_back(self, db, rpcs):
        ret = _Return(db, rpcs)
        _update(db, ret.a[2], status=StorageNode.STATUS_SUSPENDED)
        assert ret.settle() is False
        assert ret.elected == [] and ret.resync_lvs() == []

    def test_an_aborted_fence_is_cleared_unless_a_disaster_task_runs(self, db, rpcs):
        ret = _Return(db, rpcs, state=src.LOST_SITE_FENCING)
        task_id, _ = tasks_controller.add_sync_promote_task(
            ret.cluster.get_id(), ret.owner_a1.get_id(), site=SITE_B, lvol_ids=["x"],
            owners={"LVS_3": ret.owner_a1.get_id()}, lost_site=SITE_A)
        assert ret.settle() is False and ret.elected == []
        assert tasks_controller.cancel_task(task_id)
        task = db.get_task_by_id(task_id)
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        assert ret.settle() is True
        assert ret.cluster_now().lost_site == ""

    def test_a_failed_election_schedules_nothing(self, db, rpcs):
        ret = _Return(db, rpcs)
        ret.leaders[ret.owner_a2.get_id()] = None
        assert ret.settle() is False
        assert ret.resync_lvs() == [] and ret.cluster_now().lost_site == SITE_A

    def test_a_volume_missing_on_the_elected_leader_holds_the_return(self, db, rpcs):
        ret = _Return(db, rpcs)
        pool = publish._pool(db, ret.cluster)
        vol = publish._volume(db, ret.cluster, _fresh(db, ret.owner_a1), pool)
        rpc = rpcs[ret.a[1].get_id()]
        rpc.get_bdevs.side_effect = lambda name=None, **kwargs: []
        with patch.object(src, "_held") as held:
            assert ret.settle() is False
        held.assert_called_once()
        assert held.call_args.args[2] == [vol.get_id()]
        assert ret.resync_lvs() == [] and ret.cluster_now().lost_site == SITE_A
        rpc.get_bdevs.side_effect = lambda name=None, **kwargs: [{"name": name}]
        assert ret.settle() is True

    @pytest.mark.parametrize("override", [{"status": "unknown"}, {"status": 7}, {"status": ""},
                                          {"sync_replication_mode": "disabled"}])
    def test_a_distrib_not_replayed_yet_holds_the_return(self, db, rpcs, override):
        ret = _Return(db, rpcs)
        ret.answers["distrib_32"] = override
        assert ret.settle() is False
        assert ret.resync_lvs() == [] and ret.cluster_now().lost_site == SITE_A
        ret.answers.clear()
        assert ret.settle() is True

    def test_a_device_failing_after_the_elections_keeps_the_site_lost(self, db, rpcs):
        ret = _Return(db, rpcs)
        real = ret.elect

        def _elect(owner_id):
            if owner_id == ret.owner_b.get_id():
                node = _fresh(db, ret.a[0])
                node.nvme_devices[0].status = NVMeDevice.STATUS_UNAVAILABLE
                node.write_to_db(db.kv_store)
            return real(owner_id)

        with patch.object(ops, "elect_lvs_leader_on_site", side_effect=_elect):
            assert src.settle_site_return(ret.cluster.get_id()) is False
        assert ret.cluster_now().lost_site == SITE_A


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
    def test_a_remote_led_lvs_left_without_its_redirect_fails_the_activation(
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

    def test_only_the_active_remote_primary_may_build_it(self, db, rpcs, env):
        _, a, b, owner_a, _ = stack._two_owner_layout(db)
        owner_a = _update(db, owner_a, lvs_active_site=SITE_B)
        with patch.object(ops, "_recreate_lvstore_impl", return_value=True) as impl:
            with pytest.raises(ops.LVSLeadershipElsewhereError):
                ops.recreate_lvstore(_fresh(db, b[1]), lvs_primary=owner_a, activation_mode=True)
            assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=owner_a, activation_mode=True)
        assert impl.call_count == 1
        with pytest.raises(ops.LVSLeadershipElsewhereError):
            ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=owner_a, activation_mode=False)



# ---------------------------------------------------------------------------
# the cluster status with a site lost
# ---------------------------------------------------------------------------

def _status(db, cluster, dp_gone=()):
    """The status verdict; ``dp_gone``: nodes a peer majority reports gone
    from the data plane (the probe of a transient state)."""
    from simplyblock_core.services import storage_node_monitor
    probes = ({}, {node.get_id(): True for node in dp_gone})
    with patch.object(storage_node_monitor, "_collect_status_probes", return_value=probes):
        return storage_node_monitor.get_next_cluster_status(cluster.get_id())


def _fail_device(db, node, status=NVMeDevice.STATUS_FAILED):
    node = _fresh(db, node)
    node.nvme_devices[0].status = status
    node.write_to_db(db.kv_store)


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
        assert _status(db, cluster) == Cluster.STATUS_DEGRADED
        _update(db, b[0], status=StorageNode.STATUS_OFFLINE)       # the survivor at its FTT
        assert _status(db, cluster) == Cluster.STATUS_DEGRADED
        _update(db, b[1], status=StorageNode.STATUS_OFFLINE)       # beyond it
        assert _status(db, cluster) == Cluster.STATUS_SUSPENDED

    def test_a_site_whose_nodes_are_up_with_failed_devices_is_not_lost(self, db):
        cluster = stack._seed_cluster(db)
        a, b = stack._sites(db, cluster)
        for node in a:
            _fail_device(db, node)
        assert _status(db, cluster) == Cluster.STATUS_SUSPENDED

    def test_unreachable_nodes_count_as_lost_only_on_the_data_plane_verdict(self, db):
        cluster = stack._seed_cluster(db)
        a, b = stack._sites(db, cluster)
        for node in a:
            _update(db, node, status=StorageNode.STATUS_UNREACHABLE)
            _fail_device(db, node)
        # SPDK still serving (no data-plane verdict): the failed devices suspend
        assert _status(db, cluster) == Cluster.STATUS_SUSPENDED
        assert _status(db, cluster, dp_gone=a[:2]) == Cluster.STATUS_SUSPENDED
        assert _status(db, cluster, dp_gone=a) == Cluster.STATUS_DEGRADED

    def test_both_sites_lost_suspends(self, db):
        cluster = stack._seed_cluster(db)
        a, b = stack._sites(db, cluster)
        for node in (*a, *b):
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        assert _status(db, cluster) == Cluster.STATUS_SUSPENDED

    def test_without_sync_replication_unchanged(self, db):
        cluster = stack._seed_cluster(db)
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
        a, b = stack._sites(db, cluster)
        for node in a:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        assert _status(db, cluster) == Cluster.STATUS_SUSPENDED


# ---------------------------------------------------------------------------
# a follower opens only with its redirect to the leader
# ---------------------------------------------------------------------------

def _hub(spdk, node, lvs_name, *, connected, leader=False):
    """The node's instance of ``lvs_name`` reports its redirect as SPDK does
    (bdev_lvol_get_lvstores: ``connect_state`` / ``lvs_redirect``), tracking
    bdev_lvol_connect_hublvol."""
    target = spdk[node.get_id()]
    state = {"connected": connected}

    def _lvstores(name=None, *args, **kwargs):
        if name != lvs_name:
            return []
        return [{"name": lvs_name, "lvs leadership": leader, "lvs_secondary": True,
                 "lvs_redirect": state["connected"], "connect_state": state["connected"]}]

    def _connect(*args, **kwargs):
        state["connected"] = True
        return True

    target.bdev_lvol_get_lvstores.side_effect = _lvstores
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

    def test_a_follower_without_its_redirect_is_published_closed_and_retried(self, db, spdk):
        cluster, a, b, owner, vol = _served(db, spdk)
        _hub(spdk, a[1], "LVS_1", connected=False)
        _hub(spdk, a[2], "LVS_1", connected=True)
        for node in (a[1], a[2]):
            ops._set_lvol_ana_on_node(vol, node, "non_optimized")
        assert publish._seen(spdk, a[1], vol) == "inaccessible"
        assert publish._seen(spdk, a[2], vol) == "non_optimized"
        assert len(_retries(db, cluster, a[1], vol)) == 1 and _retries(db, cluster, a[2], vol) == []

    def test_the_group_writer_closes_an_unwired_follower_and_opens_a_wired_one(self, db, spdk):
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

    def test_the_leader_and_an_unknown_redirect_are_left_as_the_rule_says(self, db, spdk):
        cluster, a, b, owner, vol = _served(db, spdk)
        _hub(spdk, a[1], "LVS_1", connected=False, leader=True)     # a leader is not judged
        spdk[a[2].get_id()].bdev_lvol_get_lvstores.side_effect = RPCException("busy")
        for node in (a[1], a[2]):
            ops._set_lvol_ana_on_node(vol, node, "non_optimized")
        assert [publish._seen(spdk, n, vol) for n in (a[1], a[2])] == ["non_optimized"] * 2

    def test_after_a_failed_reactivation_the_monitor_opens_no_unwired_follower(self, db, spdk):
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
        cluster, a, b, owner, vol = _served(db, spdk, active_site=SITE_B)
        _hub(spdk, b[2], "LVS_1", connected=False)
        assert publish._seen(spdk, b[2], vol) == "non_optimized"
        _sweep(cluster, owner, vol)
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
    def test_the_monitor_keeps_the_promoted_secondary_optimized(self, db, spdk, leader):
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
        tasks_controller.run_lvol_sync_op_task(task)
        assert publish._seen(spdk, a[1], vol) == "optimized"
        ops._set_lvol_ana_on_node(vol, a[1], "non_optimized")
        assert publish._seen(spdk, a[1], vol) == "optimized"

    def test_the_primary_restarting_hands_back_to_the_failback(self, db, spdk):
        cluster, a, b, owner, vol = self._failed_over(db, spdk)
        _update(db, owner, status=StorageNode.STATUS_RESTARTING)
        _hub(spdk, a[1], "LVS_1", connected=True)
        ops._set_lvol_ana_on_node(vol, a[1], "non_optimized")
        assert publish._seen(spdk, a[1], vol) == "non_optimized"
