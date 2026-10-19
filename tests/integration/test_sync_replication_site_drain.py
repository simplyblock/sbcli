"""The site drain of a sync-replication cluster, against the real
FoundationDB: a site recorded ``draining`` by the monitor has what is left of
it shut down by main's drain loop narrowed to that site (a vetoed node left
alone), reaches ``down`` once nothing of it is pending, and has its restarts
paused meanwhile; a site that is ``down`` is restarted although more nodes are
offline than the flat past-FTT guard allows. Under a cluster-wide suspension
the suspend drain covers every node, vetoed ones included.

The monitor pass is the one of test_sync_replication_disaster.py: its probes
answer from ``majority`` / ``veto`` and ``_spawn_recovery_shutdown`` is
patched at its boundary - here it writes the node OFFLINE, as a finished
shutdown does. ``monitor_pass.run`` is the public update_cluster_status: a
real set_node_offline inside it re-enters it, so one call may run two passes.
"""
import uuid
from unittest.mock import patch

import pytest

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor as monitor
from simplyblock_core.services import tasks_runner_restart as restart_runner
from tests.integration import test_sync_replication_disaster as disaster
from tests.integration import test_sync_replication_lvs_stack as stack
from tests.integration.task_driver import handler_reached

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
DRAINING, DOWN = Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN

db = stack.db
rpcs = stack.rpcs
monitor_pass = disaster.monitor_pass
_fresh, _update = stack._fresh, stack._update
_record, _seed_record, _site, _statuses = disaster._record, disaster._seed_record, disaster._site, disaster._statuses

OFFLINE_DEV = (StorageNode.STATUS_OFFLINE, NVMeDevice.STATUS_UNAVAILABLE)


@pytest.fixture()
def drain(db, monitor_pass):
    """The monitor pass whose shutdown boundary finishes the shutdown (the
    node OFFLINE) and records, per node shut down, the lost-site record and
    whether its restart was paused at that moment."""
    seen = {}

    def _shutdown(node):
        cluster = db.get_cluster_by_id(node.cluster_id)
        seen[node.get_id()] = (_record(db, node.cluster_id),
                               tasks_controller.is_auto_restart_paused(cluster, _fresh(db, node)))
        _update(db, node, status=StorageNode.STATUS_OFFLINE)

    monitor_pass.shutdown.side_effect = _shutdown
    monitor_pass.seen = seen
    return monitor_pass


def _shut(drain):
    return sorted(drain.seen)


def _ids(nodes):
    return sorted(n.get_id() for n in nodes)


def _draining(db, stored=Cluster.STATUS_DEGRADED, **fields):
    return _seed_record(db, stack._seed_cluster(db), SITE_A, DRAINING, status=stored, **fields)


def _paused(db, cluster, node):
    return tasks_controller.is_auto_restart_paused(db.get_cluster_by_id(cluster.get_id()), _fresh(db, node))


def _restart_task(cluster, node):
    return tasks_controller.get_active_node_restart_task(cluster.get_id(), node.get_id())


# ---------------------------------------------------------------------------
# the site drain
# ---------------------------------------------------------------------------

class TestSiteDrain:
    """3 + 3 nodes, npcs 1; record ``(A, draining)`` and a stored DEGRADED
    unless the test says otherwise."""

    def test_one_pass_records_and_drains_a_site_beyond_ftt(self, db, drain):
        cluster = _seed_record(db, stack._seed_cluster(db), "", "", status=Cluster.STATUS_ACTIVE)
        a, b = _site(db, cluster)
        for node in a[1:]:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        after = drain.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DRAINING, Cluster.STATUS_DEGRADED)
        # the record and the pause were committed before the shutdown
        assert drain.seen == {a[0].get_id(): ((SITE_A, DRAINING), True)}
        after = drain.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_DEGRADED)
        assert Cluster.STATUS_SUSPENDED not in [status for status, _ in drain.status_writes]

    def test_only_the_live_nodes_of_the_site_are_shut_down(self, db, drain):
        cluster = _draining(db)
        a, b = _site(db, cluster)
        _update(db, a[1], status=StorageNode.STATUS_DOWN)
        _update(db, a[2], status=StorageNode.STATUS_OFFLINE)
        after = drain.run(cluster)
        assert _shut(drain) == _ids(a[:2])
        assert (after.lost_site, after.lost_site_state) == (SITE_A, DRAINING)     # judged on the pass's snapshot
        after = drain.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_DEGRADED)
        assert _shut(drain) == _ids(a[:2])
        assert _statuses(db, b) == [disaster.ONLINE_DEV] * 3

    def test_an_operator_stopped_node_does_not_hold_the_drain(self, db, drain):
        cluster = _draining(db)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=StorageNode.STATUS_ONLINE, auto_restart_disabled=True)
        after = drain.run(cluster)
        drain.shutdown.assert_not_called()
        assert (after.lost_site, after.lost_site_state) == (SITE_A, DOWN)
        assert _fresh(db, a[0]).status == StorageNode.STATUS_ONLINE

    def test_a_gone_node_goes_offline_and_the_site_down(self, db, drain):
        cluster = _draining(db)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=StorageNode.STATUS_UNREACHABLE)
        drain.majority = drain.veto = {a[0].get_id()}
        after = drain.run(cluster)        # set_node_offline re-enters the update: a second pass
        assert _statuses(db, a[:1]) == [OFFLINE_DEV]
        assert (after.lost_site, after.lost_site_state) == (SITE_A, DOWN)
        assert _statuses(db, b) == [disaster.ONLINE_DEV] * 3

    def test_a_vetoed_unreachable_node_holds_the_drain(self, db, drain):
        cluster = _draining(db)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=StorageNode.STATUS_UNREACHABLE)
        drain.majority = {a[0].get_id()}           # a majority lost it, one peer still sees it enabled
        for _ in range(2):
            after = drain.run(cluster)
            assert _statuses(db, a[:1]) == [(StorageNode.STATUS_UNREACHABLE, NVMeDevice.STATUS_ONLINE)]
            assert (after.lost_site, after.lost_site_state) == (SITE_A, DRAINING)

    @pytest.mark.parametrize("status", [StorageNode.STATUS_RESTARTING, StorageNode.STATUS_SCHEDULABLE])
    def test_a_node_coming_back_or_sick_holds_the_drain(self, db, drain, status):
        # Main's drain waits for both and acts on neither; a SCHEDULABLE node
        # whose RPC recovers stays so (an inherited limitation - final report).
        cluster = _draining(db)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=status)
        after = drain.run(cluster)
        drain.shutdown.assert_not_called()
        assert _fresh(db, a[0]).status == status
        assert (after.lost_site, after.lost_site_state) == (SITE_A, DRAINING)

    @pytest.mark.parametrize("status", [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED])
    def test_a_site_with_nothing_pending_is_down(self, db, drain, status):
        cluster = _draining(db)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=status)
        after = drain.run(cluster)
        drain.shutdown.assert_not_called()
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_DEGRADED)

    def test_without_a_witness_no_node_is_set_offline(self, db, drain):
        # the management node lost its network: every vote 0/0, so the
        # majority dict (nobody gone) is in force
        cluster = _draining(db)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE, b_status=StorageNode.STATUS_UNREACHABLE)
        _update(db, a[0], status=StorageNode.STATUS_UNREACHABLE)
        drain.veto = {n.get_id() for n in (a[0], *b)}
        after = drain.run(cluster)
        assert _statuses(db, a[:1]) == [(StorageNode.STATUS_UNREACHABLE, NVMeDevice.STATUS_ONLINE)]
        assert _statuses(db, b) == [(StorageNode.STATUS_UNREACHABLE, NVMeDevice.STATUS_ONLINE)] * 3
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DRAINING, Cluster.STATUS_DEGRADED)

    @pytest.mark.parametrize("gone_by_both", [False, True])
    def test_with_no_healthy_site_escalation_needs_both_dicts(
            self, db, drain, gone_by_both):
        # B's b0, b1 UNREACHABLE and abstained on by the voters (gone by the
        # veto only): B is out by the veto rule and live by the majority, so
        # the majority dict decides the status - and a0, which a majority of
        # A's ONLINE peers reports disconnected, is escalated only when no
        # answering peer still reports it enabled
        cluster = _draining(db)
        a, b = _site(db, cluster)
        _update(db, a[0], status=StorageNode.STATUS_UNREACHABLE)
        for node in b[:2]:
            _update(db, node, status=StorageNode.STATUS_UNREACHABLE)
        drain.majority = {a[0].get_id()}
        drain.veto = {n.get_id() for n in b[:2]} | ({a[0].get_id()} if gone_by_both else set())
        after = drain.run(cluster)
        assert _shut(drain) == _ids(a[1:])
        if gone_by_both:
            assert _statuses(db, a[:1]) == [OFFLINE_DEV]
            after = drain.run(cluster)
            assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_DEGRADED)
        else:
            assert _statuses(db, a[:1]) == [(StorageNode.STATUS_UNREACHABLE, NVMeDevice.STATUS_ONLINE)]
            after = drain.run(cluster)
            assert _statuses(db, a[:1]) == [(StorageNode.STATUS_UNREACHABLE, NVMeDevice.STATUS_ONLINE)]
            assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DRAINING, Cluster.STATUS_DEGRADED)
        assert _statuses(db, b[:2]) == [(StorageNode.STATUS_UNREACHABLE, NVMeDevice.STATUS_ONLINE)] * 2

    @pytest.mark.parametrize("flag", [True, False])
    def test_the_site_drain_ignores_the_cluster_drain_flag(self, db, drain, flag):
        cluster = _draining(db, suspend_drain_complete=flag)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=StorageNode.STATUS_ONLINE)
        drain.run(cluster)
        assert _shut(drain) == _ids(a[:1])
        after = drain.run(cluster)
        assert (after.lost_site_state, after.status, after.suspend_drain_complete) == (
            DOWN, Cluster.STATUS_DEGRADED, flag)

    def test_a_read_only_cluster_drains_and_resumes_restarts(self, db, drain):
        cluster = _draining(db, stored=Cluster.STATUS_READONLY)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=StorageNode.STATUS_ONLINE)
        drain.run(cluster)
        assert _shut(drain) == _ids(a[:1])
        after = drain.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_READONLY)
        assert [_paused(db, cluster, n) for n in a] == [False] * 3

    def test_a_suspended_cluster_clears_the_drain_record(self, db, drain):
        cluster = _draining(db, stored=Cluster.STATUS_SUSPENDED, suspend_drain_complete=True)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=StorageNode.STATUS_ONLINE)
        assert _paused(db, cluster, a[1]) is True
        after = drain.run(cluster)
        drain.shutdown.assert_not_called()
        assert (after.lost_site, after.lost_site_state) == ("", "")
        assert [_paused(db, cluster, n) for n in a] == [False] * 3

    def test_both_sites_out_clear_the_record_then_suspend(self, db, drain):
        cluster = _draining(db)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], status=StorageNode.STATUS_ONLINE)
        for node in b[:2]:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        after = drain.run(cluster)
        assert (after.lost_site, after.lost_site_state, after.status) == ("", "", Cluster.STATUS_SUSPENDED)
        assert (Cluster.STATUS_SUSPENDED, ("", "")) in drain.status_writes

    def test_a_suspension_drains_vetoed_nodes_too(self, db, monitor_pass):
        # no healthy site: main's suspend drain - it escalates every
        # UNREACHABLE node, whatever a peer reports (the plain pass, whose
        # finished shutdown asks nothing of the per-site restart pause)
        monitor_pass.shutdown.side_effect = lambda node: _update(db, node, status=StorageNode.STATUS_OFFLINE)
        cluster = _seed_record(db, stack._seed_cluster(db), "", "", status=Cluster.STATUS_SUSPENDED,
                               suspend_drain_complete=False)
        a, b = _site(db, cluster)
        _update(db, a[1], status=StorageNode.STATUS_UNREACHABLE)
        _update(db, a[2], status=StorageNode.STATUS_DOWN)
        _update(db, b[1], status=StorageNode.STATUS_UNREACHABLE)
        _update(db, b[2], status=StorageNode.STATUS_OFFLINE)
        after = monitor_pass.run(cluster)
        assert sorted(c.args[0].get_id() for c in monitor_pass.shutdown.call_args_list) == _ids([a[0], a[2], b[0]])
        assert _statuses(db, [a[1], b[1]]) == [OFFLINE_DEV] * 2
        assert (after.lost_site, after.lost_site_state, after.status) == ("", "", Cluster.STATUS_SUSPENDED)
        assert after.suspend_drain_complete is True


# ---------------------------------------------------------------------------
# the restart pause
# ---------------------------------------------------------------------------

def _task(db, cluster, node_id):
    """A queued restart task of ``node_id``, as stored."""
    task = JobSchedule()
    task.uuid = str(uuid.uuid4())
    task.cluster_id = cluster.get_id()
    task.function_name = JobSchedule.FN_NODE_RESTART
    task.node_id = node_id
    task.status = JobSchedule.STATUS_NEW
    task.max_retry = 10
    task.write_to_db(db.kv_store)
    return task


def _deferred(task):
    """The restart runner, driven through its spec, leaves ``task`` waiting."""
    return not handler_reached(restart_runner.SPEC, task)


class TestRestartPause:

    @pytest.mark.parametrize("state, paused_a", [(DRAINING, True), (DOWN, False)])
    def test_only_the_draining_site_is_paused(self, db, state, paused_a):
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, state, status=Cluster.STATUS_DEGRADED)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        assert _paused(db, cluster, a[0]) is paused_a
        assert _paused(db, cluster, b[0]) is False
        assert tasks_controller.is_auto_restart_paused(db.get_cluster_by_id(cluster.get_id())) is False

    def test_a_record_without_sync_replication_pauses_nothing(self, db):
        cluster = _draining(db, sync_replication=False)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        assert _paused(db, cluster, a[0]) is False

    def test_the_queue_refuses_only_the_draining_site(self, db):
        cluster = _draining(db)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        assert tasks_controller.add_node_to_auto_restart(_fresh(db, a[0])) is False
        assert _restart_task(cluster, a[0]) is False
        assert tasks_controller.add_node_to_auto_restart(_fresh(db, b[0]))
        assert _restart_task(cluster, b[0])

    def test_the_requeue_scan_skips_the_draining_site(self, db):
        cluster = _draining(db)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        _update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        with patch.object(tasks_controller, "add_node_to_auto_restart",
                          wraps=tasks_controller.add_node_to_auto_restart) as queue:
            monitor._requeue_stuck_auto_restarts(cluster.get_id())
        assert [c.args[0].get_id() for c in queue.call_args_list] == [b[0].get_id()]
        assert _restart_task(cluster, b[0])

    @pytest.mark.parametrize("state, deferred_a", [(DRAINING, True), (DOWN, False)])
    def test_the_runner_defers_only_the_draining_site(self, db, state, deferred_a):
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, state, status=Cluster.STATUS_DEGRADED)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        assert _deferred(_task(db, cluster, a[0].get_id())) is deferred_a
        assert _deferred(_task(db, cluster, b[0].get_id())) is False

    @pytest.mark.parametrize("stored, drained, record, deferred", [
        (Cluster.STATUS_SUSPENDED, False, ("", ""), True),            # the cluster-wide pause still holds
        (Cluster.STATUS_DEGRADED, False, (SITE_A, DRAINING), False),  # no node, no site to pause
    ])
    def test_the_runner_with_a_task_of_a_missing_node(self, db, stored, drained, record, deferred):
        cluster = _seed_record(db, stack._seed_cluster(db), *record, status=stored,
                               suspend_drain_complete=drained)
        _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        assert _deferred(_task(db, cluster, "no-such-node")) is deferred


# ---------------------------------------------------------------------------
# the restart of a site that is down
# ---------------------------------------------------------------------------

class TestSiteDownRestart:

    def test_every_node_of_a_site_down_is_restarted(self, db):
        # three nodes offline with npcs 1: the flat past-FTT guard would refuse
        cluster = _seed_record(db, stack._seed_cluster(db), SITE_A, DOWN, status=Cluster.STATUS_DEGRADED)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        assert tasks_controller.add_node_to_auto_restart(_fresh(db, a[0]))
        monitor._requeue_stuck_auto_restarts(cluster.get_id())
        assert all(_restart_task(cluster, n) for n in a)

    def test_a_non_sync_cluster_keeps_the_flat_guard(self, db):
        cluster = _seed_record(db, stack._seed_cluster(db), "", "", status=Cluster.STATUS_DEGRADED,
                               sync_replication=False)
        a, b = _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        assert tasks_controller.add_node_to_auto_restart(_fresh(db, a[0])) is False
        assert _restart_task(cluster, a[0]) is False
