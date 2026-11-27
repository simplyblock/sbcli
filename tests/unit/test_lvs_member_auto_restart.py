"""bf36 R6 (2026-11-25): a member's copy of LVS_19 failed every registration for
10 hours. A reload of the lvstore cannot repair that (the member's data layer
served stale data); a restart of the node can. When the registrations fail again
after a successful reload, the control plane now restarts the node itself --
through the existing unattended path (graceful shutdown with auto-restart kept,
then the queued FN_NODE_RESTART task), behind the gates tested here, at most once
per node per 6 h (persisted on the node).
"""
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import lvs_member_repair as rep
from simplyblock_core.controllers import storage_events
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode

ONLINE = StorageNode.STATUS_ONLINE
NOW = 1_000_000.0
SIX_H = constants.LVS_MEMBER_AUTO_RESTART_INTERVAL_SEC


def _n(nid, **kw):
    d = dict(uuid=nid, status=ONLINE, cluster_id="C", lvstore="", secondary_node_id="",
             tertiary_node_id="", lvstore_stack_secondary="", lvstore_stack_tertiary="",
             auto_restart_disabled=False, lvs_member_auto_restart_at=0.0)
    d.update(kw)
    n = SimpleNamespace(**d)
    n.get_id = lambda: n.uuid
    return n


class _DB:
    """Four nodes, each primary of its lvstore with the next one as secondary
    and the one after as tertiary. T (the member) is tertiary of LVS_19 (P)."""

    def __init__(self, cluster_kw=None, tasks=None):
        ids = ["P", "S", "T", "X"]
        self.nodes = {}
        for i, nid in enumerate(ids):
            self.nodes[nid] = _n(nid, lvstore=f"LVS_{nid}" if nid != "P" else "LVS_19",
                                 secondary_node_id=ids[(i + 1) % 4],
                                 tertiary_node_id=ids[(i + 2) % 4])
        for nid, n in self.nodes.items():
            for p in self.nodes.values():
                if p.secondary_node_id == nid:
                    n.lvstore_stack_secondary = p.uuid
                if p.tertiary_node_id == nid:
                    n.lvstore_stack_tertiary = p.uuid
        c = dict(status=Cluster.STATUS_ACTIVE, sync_replication=False, two_node_arbitration=False)
        c.update(cluster_kw or {})
        self.cluster = SimpleNamespace(**c)
        self.cluster.get_id = lambda: "C"
        self.tasks = list(tasks or [])
        self.race_value = None

    def get_storage_node_by_id(self, nid):
        return self.nodes[nid]

    def get_cluster_by_id(self, _cid):
        return self.cluster

    def get_storage_nodes_by_cluster_id(self, _cid):
        return list(self.nodes.values())

    def get_job_tasks(self, _cid):
        return self.tasks

    def atomic_update(self, obj, fn):
        fresh = self.nodes[obj.get_id()]
        if self.race_value is not None:
            fresh.lvs_member_auto_restart_at = self.race_value
        if fn(fresh) is False:
            return fresh
        return fresh


@pytest.fixture
def env():
    with patch.object(rep.storage_events, "snode_lvs_member_auto_restart") as ev, \
            patch("simplyblock_core.controllers.activation_gate.site_activation_blocks", return_value=""), \
            patch("simplyblock_core.controllers.tasks_controller.get_active_node_restart_task",
                  return_value=False) as active:
        yield SimpleNamespace(ev=ev, active=active)


def _restart(db, nid="T", now=NOW):
    deferred = []
    out = rep.auto_restart_member(db.nodes[nid], "LVS_19", db=db, now=now, run=deferred.append)
    return out, deferred


def _stages(ev):
    return [c.args[2] for c in ev.call_args_list]


# ---------------- all gates pass ----------------

def test_restarts_through_the_existing_auto_restart_path(env):
    db = _DB()
    (outcome, _), deferred = _restart(db)
    assert outcome == rep.RESTART_STARTED
    assert _stages(env.ev) == ["restarting"]
    # persisted before anything is stopped
    assert db.nodes["T"].lvs_member_auto_restart_at == NOW
    assert len(deferred) == 1
    with patch.object(rep.storage_node_ops, "shutdown_storage_node", return_value=True) as sd, \
            patch("simplyblock_core.controllers.tasks_controller.add_node_to_auto_restart",
                  return_value="task-1") as queue:
        assert deferred[0]() is True
    sd.assert_called_once_with("T", keep_auto_restart=True)
    queue.assert_called_once()
    assert _stages(env.ev)[-1] == "queued" and "task-1" in env.ev.call_args.args[3]


def test_a_failed_shutdown_is_reported_and_nothing_is_queued(env):
    db = _DB()
    _, deferred = _restart(db)
    with patch.object(rep.storage_node_ops, "shutdown_storage_node", return_value=False), \
            patch("simplyblock_core.controllers.tasks_controller.add_node_to_auto_restart") as queue:
        assert deferred[0]() is False
    queue.assert_not_called()
    assert _stages(env.ev)[-1] == "failed"


def test_a_task_that_cannot_be_queued_is_left_to_the_monitor(env):
    db = _DB()
    _, deferred = _restart(db)
    with patch.object(rep.storage_node_ops, "shutdown_storage_node", return_value=True), \
            patch("simplyblock_core.controllers.tasks_controller.add_node_to_auto_restart",
                  return_value=False):
        assert deferred[0]() is True
    assert _stages(env.ev)[-1] == "queued" and "re-queues" in env.ev.call_args.args[3]


# ---------------- gates: skipped, tried again at the next alert ----------------

def _skipped(env, db, word):
    (outcome, why), deferred = _restart(db)
    assert outcome == rep.RESTART_SKIPPED, why
    assert word in why
    assert deferred == []
    assert db.nodes["T"].lvs_member_auto_restart_at == 0.0     # nothing consumed
    assert _stages(env.ev) == ["skipped"]


def test_not_when_the_node_is_not_online(env):
    db = _DB()
    db.nodes["T"].status = StorageNode.STATUS_DOWN
    _skipped(env, db, "the node is down")


@pytest.mark.parametrize("status", [Cluster.STATUS_DEGRADED, Cluster.STATUS_IN_ACTIVATION,
                                    Cluster.STATUS_IN_EXPANSION, Cluster.STATUS_SUSPENDED])
def test_only_on_an_active_cluster(env, status):
    _skipped(env, _DB(cluster_kw={"status": status}), "not active")


def test_not_during_a_site_activation(env):
    with patch("simplyblock_core.controllers.activation_gate.site_activation_blocks",
               return_value="site b is being activated; retry"):
        _skipped(env, _DB(), "site activation")


@pytest.mark.parametrize("status", [StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_SHUTDOWN,
                                    StorageNode.STATUS_OFFLINE, StorageNode.STATUS_UNREACHABLE,
                                    StorageNode.STATUS_DOWN, StorageNode.STATUS_SCHEDULABLE])
def test_not_while_another_node_is_not_online(env, status):
    db = _DB()
    db.nodes["X"].status = status
    _skipped(env, db, "other nodes are not online")


def test_not_when_a_restart_task_is_already_queued(env):
    env.active.return_value = "t-restart"
    _skipped(env, _DB(), "already queued")


@pytest.mark.parametrize("fn", [JobSchedule.FN_REPLICATION_FINAL, JobSchedule.FN_LVOL_MIG,
                                JobSchedule.FN_LVOL_BATCH_MIG, JobSchedule.FN_CLUSTER_EXPAND])
def test_not_during_a_cutover_migration_or_expansion(env, fn):
    task = SimpleNamespace(function_name=fn, status=JobSchedule.STATUS_RUNNING, canceled=False)
    _skipped(env, _DB(tasks=[task]), fn)


def test_a_finished_or_canceled_task_does_not_block(env):
    tasks = [SimpleNamespace(function_name=JobSchedule.FN_REPLICATION_FINAL,
                             status=JobSchedule.STATUS_DONE, canceled=False),
             SimpleNamespace(function_name=JobSchedule.FN_LVOL_MIG,
                             status=JobSchedule.STATUS_NEW, canceled=True)]
    (outcome, _), _ = _restart(_DB(tasks=tasks))
    assert outcome == rep.RESTART_STARTED


def test_not_when_its_own_lvstore_has_no_secondary_to_take_over(env):
    db = _DB()
    db.nodes["T"].secondary_node_id = ""
    _skipped(env, db, "no online secondary")


def test_not_when_it_is_the_only_online_member_of_an_lvstore(env):
    # defence in depth: a member record that reads not-online when looked up
    db = _DB()
    gone = _n("P", status=StorageNode.STATUS_OFFLINE, lvstore="LVS_19",
              secondary_node_id="S", tertiary_node_id="T")
    sgone = _n("S", status=StorageNode.STATUS_OFFLINE, lvstore="LVS_S",
               secondary_node_id="T", tertiary_node_id="X")
    real_get = db.get_storage_node_by_id
    db.get_storage_node_by_id = lambda nid: {"P": gone, "S": sgone}.get(nid) or real_get(nid)
    _skipped(env, db, "only online member of LVS_19")


def test_the_member_lvstores_are_its_own_and_those_it_serves():
    db = _DB()
    with patch.object(rep.replica_chain, "lvstore_replicas",
                      side_effect=lambda p, db=None: [("secondary", db.nodes[p.secondary_node_id]),
                                                      ("tertiary", db.nodes[p.tertiary_node_id])]):
        got = {lvs: sorted(o.get_id() for o in others) for lvs, others in rep._member_lvstores(db.nodes["T"], db)}
    # T is primary of LVS_T, secondary of LVS_S, tertiary of LVS_19 (P)
    assert got == {"LVS_T": ["P", "X"], "LVS_S": ["S", "X"], "LVS_19": ["P", "S"]}


# ---------------- never automated: event only, manual action ----------------

def _manual(env, db, word):
    (outcome, why), deferred = _restart(db)
    assert outcome == rep.RESTART_MANUAL, why
    assert word in why and deferred == []
    assert _stages(env.ev) == ["manual"]


def test_sync_replication_clusters_are_never_auto_restarted_for_this(env):
    _manual(env, _DB(cluster_kw={"sync_replication": True}), "sync-replication")


def test_two_node_clusters_are_never_auto_restarted_for_this(env):
    _manual(env, _DB(cluster_kw={"two_node_arbitration": True}), "two-node")


def test_a_two_member_cluster_counts_as_two_node(env):
    db = _DB()
    for nid in ("S", "X"):
        db.nodes[nid].status = StorageNode.STATUS_REMOVED
    _manual(env, db, "two-node")


def test_a_deliberately_stopped_node_is_left_alone(env):
    db = _DB()
    db.nodes["T"].auto_restart_disabled = True
    _manual(env, db, "deliberately")


# ---------------- rate limit, persisted ----------------

def test_at_most_one_automatic_restart_per_six_hours(env):
    db = _DB()
    db.nodes["T"].lvs_member_auto_restart_at = NOW - SIX_H + 60
    _manual(env, db, "at most one automatic restart per 6 h")


def test_after_six_hours_it_may_restart_again(env):
    db = _DB()
    db.nodes["T"].lvs_member_auto_restart_at = NOW - SIX_H - 1
    (outcome, _), _ = _restart(db)
    assert outcome == rep.RESTART_STARTED
    assert db.nodes["T"].lvs_member_auto_restart_at == NOW


def test_the_limit_is_read_from_the_stored_node_not_from_memory(env):
    # a restarted monitor or task runner has no memory: only the node record counts
    db = _DB()
    stale = _n("T", lvs_member_auto_restart_at=0.0)               # caller's old copy
    db.nodes["T"].lvs_member_auto_restart_at = NOW - 60
    outcome, _ = rep.auto_restart_member(stale, "LVS_19", db=db, now=NOW, run=lambda f: None)
    assert outcome == rep.RESTART_MANUAL


def test_a_concurrent_decision_does_not_restart_twice(env):
    db = _DB()
    db.race_value = NOW - 5                                      # written between read and claim
    (outcome, why), deferred = _restart(db)
    assert outcome == rep.RESTART_MANUAL and "meanwhile" in why
    assert deferred == []


def test_the_field_exists_on_the_model_and_defaults_to_never():
    assert StorageNode().lvs_member_auto_restart_at == 0.0


# ---------------- event wording ----------------

@pytest.mark.parametrize("stage, level, words", [
    ("restarting", "Warning", ["Restarting this node", "a reload cannot repair", "Graceful shutdown"]),
    ("queued", "Info", ["automatic restart is queued"]),
    ("skipped", "Warning", ["Not restarting this node", "Tried again at the next alert"]),
    ("manual", "Error", ["will not restart it automatically", "Manual action is needed"]),
    ("failed", "Error", ["failed", "Manual action is needed"]),
])
def test_event_wording(stage, level, words):
    node = SimpleNamespace(cluster_id="C", get_id=lambda: "T")
    with patch.object(storage_events.ec, "log_event_cluster") as log:
        storage_events.snode_lvs_member_auto_restart(node, "LVS_19", stage, "why")
    kw = log.call_args.kwargs
    assert kw["event"] == "LVS_MEMBER_AUTO_RESTART" and kw["event_level"] == level
    assert "LVS_19" in kw["message"] and "why" in kw["message"]
    for w in words:
        assert w in kw["message"]


def test_no_new_restart_path():
    import inspect
    src = inspect.getsource(rep._shutdown_and_queue)
    assert "shutdown_storage_node(node_id, keep_auto_restart=True)" in src
    assert "add_node_to_auto_restart(" in src
    assert "restart_storage_node(" not in src
