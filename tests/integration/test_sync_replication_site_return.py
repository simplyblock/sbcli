"""The return of a recorded site of a sync-replication cluster, driven by the
storage-node monitor against the real FoundationDB: the site gate
(``storage_node_monitor._site_return_problem`` - main's activation gate split
by scope, the journal floor, no promote and no running site activation), the
background thread that runs ``cluster_activate(force=True, site=S)`` while the
cluster-status pass goes on, the per-process registry of those threads judged
by liveness, and a suspension written while one runs.

3 + 3 nodes (``test_sync_replication_lvs_stack._sites``), site A recorded, the
cluster DEGRADED. The gate is driven with ``cluster_ops.cluster_activate``
patched; where the activation itself is the point it is the real one over the
four LVS shapes of ``test_sync_replication_site_activation``, its storage-node
primitives captured there and the first Pass 2 rebuild held.
"""
import threading
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import call, patch

import pytest

from simplyblock_core import cluster_ops
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor as monitor
from simplyblock_core.services import tasks_runner_restart as restart_runner
from tests.integration import test_sync_replication_disaster as disaster
from tests.integration import test_sync_replication_lvs_stack as stack
from tests.integration import test_sync_replication_site_activation as site_activation

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
DRAINING, DOWN = Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN
FENCING, DONE = Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DONE
ONLINE, OFFLINE = StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE
TRANSITIONS = (StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_SHUTDOWN, StorageNode.STATUS_IN_CREATION)

#: The activation wrapper itself, for the paths that run it while the module attribute is patched.
REAL_ACTIVATE = cluster_ops.cluster_activate

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
adopt = stack.adopt
monitor_pass = disaster.monitor_pass
_record, _seed_record = disaster._record, disaster._seed_record
_fresh, _update = stack._fresh, stack._update
join_site_activations = disaster.join_site_activations


def _ago(seconds):
    return (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()


def _returned(db, state=DOWN, *, ftt=1, status=Cluster.STATUS_DEGRADED, **fields):
    """Site A recorded ``state``, every node ONLINE for a minute."""
    cluster = _seed_record(db, stack._seed_cluster(db, ftt=ftt), SITE_A, state, status=status, **fields)
    a, b = stack._sites(db, cluster)
    for node in (*a, *b):
        _update(db, node, online_since=_ago(60))
    return SimpleNamespace(cluster=cluster, cid=cluster.get_id(), a=[_fresh(db, n) for n in a],
                           b=[_fresh(db, n) for n in b])


def _flag(db, cluster_id):
    c = db.get_cluster_by_id(cluster_id)
    return c.site_activation_site, c.site_activation_since


def _task(db, s, function_name, node_id, params=None, **fields):
    task = JobSchedule()
    task.uuid = f"task-{len(db.get_job_tasks(s.cid))}-{s.cid[:8]}"
    task.cluster_id = s.cid
    task.node_id = node_id
    task.date = int(datetime.now().timestamp())
    task.function_name = function_name
    task.function_params = params or {}
    task.status = JobSchedule.STATUS_NEW
    for key, value in fields.items():
        setattr(task, key, value)
    task.write_to_db(db.kv_store)
    return task


def _restart_task(db, s, node):
    return _task(db, s, JobSchedule.FN_NODE_RESTART, node.get_id())


def _promote(db, s, lost_site, **fields):
    fields.setdefault("status", JobSchedule.STATUS_RUNNING)
    return _task(db, s, JobSchedule.FN_SYNC_PROMOTE, s.b[0].get_id(),
                 {"lost_site": lost_site, "lvs_names": ["LVS_x"]}, **fields)


def _live_threads():
    return [t for t in threading.enumerate() if t.name.startswith("site-activation-") and t.is_alive()]


@pytest.fixture()
def registry(monkeypatch):
    """This process's registry of running site activations, empty."""
    threads: dict = {}
    monkeypatch.setattr(monitor, "_site_activation_threads", threads)
    return threads


class _Gate:
    """``_drive_site_return`` over the real database with the activation
    patched (``activate``); ``block()`` holds the activation until
    ``release`` and sets ``entered`` once it runs."""

    def __init__(self, db, activate):
        self.db, self.activate = db, activate
        self.release = threading.Event()
        self.entered = threading.Event()

    def block(self, before=None):
        def _held(cluster_id, **kwargs):
            if before is not None:
                before(cluster_id, **kwargs)
            self.entered.set()
            assert self.release.wait(10)
        self.activate.side_effect = _held

    def drive(self, s):
        monitor._drive_site_return(self.db.get_cluster_by_id(s.cid))


@pytest.fixture()
def gate(db, registry):
    with patch.object(cluster_ops, "cluster_activate") as activate:
        g = _Gate(db, activate)
        try:
            yield g
        finally:
            g.release.set()
            join_site_activations()


def _started(gate, s):
    """Exactly one activation of site A, started with main's arguments."""
    join_site_activations()
    gate.activate.assert_called_once_with(s.cid, force=True, site=SITE_A)


def _not_started(gate, registry):
    assert registry == {}
    assert _live_threads() == []
    gate.activate.assert_not_called()


# ---------------------------------------------------------------------------
# the site gate
# ---------------------------------------------------------------------------

def _set_node(site, index, **fields):
    return lambda db, s: _update(db, getattr(s, site)[index], **fields)


def _just_online(site, index):
    """The node ONLINE for 5 s, stamped when the test runs."""
    return lambda db, s: _update(db, getattr(s, site)[index], online_since=_ago(5))


def _with_restart_task(site, index):
    return lambda db, s: _restart_task(db, s, getattr(s, site)[index])


def _with_flag(db, s):
    now = _ago(0)
    _seed_record(db, s.cluster, SITE_A, DOWN, site_activation_site=SITE_A, site_activation_since=now,
                 site_activation_heartbeat=now)


def _below_floor(db, s):
    # FTT 2: two deliberately stopped nodes of A are within the fault
    # tolerance, and leave one local JM (ultra alg_journal.hpp:108 needs 2).
    for node in s.a[:2]:
        _update(db, node, status=OFFLINE, auto_restart_disabled=True)


CLOSED = [
    pytest.param({"state": DRAINING}, None, id="draining"),
    pytest.param({"status": Cluster.STATUS_SUSPENDED}, None, id="suspended"),
    pytest.param({"status": Cluster.STATUS_IN_ACTIVATION}, None, id="in-activation"),
    pytest.param({"sync_replication": False}, None, id="no-sync-replication"),
    pytest.param({}, _set_node("a", 0, status=StorageNode.STATUS_UNREACHABLE), id="a-unreachable"),
    pytest.param({}, _set_node("a", 0, status=OFFLINE), id="a-offline-not-deliberate"),
    pytest.param({}, _just_online("a", 1), id="a-online-under-30s"),
    *[pytest.param({}, _set_node(site, 1, status=status), id=f"{site}-{status}")
      for site in ("a", "b") for status in TRANSITIONS],
    pytest.param({}, _with_restart_task("a", 2), id="a-online-restart-task"),
    pytest.param({}, _with_restart_task("b", 2), id="b-online-restart-task"),
    pytest.param({"state": FENCING}, lambda db, s: _promote(db, s, SITE_A), id="fencing-promote-running"),
    pytest.param({"state": DONE}, lambda db, s: _promote(db, s, SITE_B), id="done-promote-of-other-site"),
    pytest.param({"state": DONE}, lambda db, s: _promote(db, s, SITE_A, canceled=True,
                                                         status=JobSchedule.STATUS_NEW),
                 id="done-promote-canceled-not-done"),
    pytest.param({}, _with_flag, id="flag-set"),
    pytest.param({"ftt": 2}, _below_floor, id="below-jc-floor"),
]

OPEN = [
    pytest.param({"state": DOWN}, None, id="down"),
    pytest.param({"state": DONE}, None, id="done"),
    pytest.param({"state": FENCING}, None, id="stale-fencing"),
    pytest.param({"state": FENCING}, lambda db, s: _promote(db, s, SITE_A, status=JobSchedule.STATUS_DONE),
                 id="fencing-promote-done"),
    pytest.param({}, _set_node("a", 2, status=OFFLINE, auto_restart_disabled=True), id="a-stopped-within-ftt"),
    pytest.param({}, _set_node("b", 1, status=StorageNode.STATUS_UNREACHABLE), id="b-unreachable"),
    pytest.param({}, _just_online("b", 1), id="b-online-under-30s"),
]


class TestSiteGate:

    @pytest.mark.parametrize("seed, setup", OPEN)
    def test_a_returned_site_starts_its_activation(self, db, gate, registry, seed, setup):
        s = _returned(db, **seed)
        if setup is not None:
            setup(db, s)
        gate.block()
        gate.drive(s)
        # the drive has returned while the activation still runs
        assert gate.entered.wait(10)
        assert monitor._site_activation_running(s.cid)
        assert [t.name for t in _live_threads()] == [f"site-activation-{s.cid[:8]}"]
        gate.release.set()
        _started(gate, s)
        assert registry == {}

    @pytest.mark.parametrize("seed, setup", CLOSED)
    def test_a_site_not_ready_starts_nothing(self, db, gate, registry, seed, setup):
        s = _returned(db, **seed)
        if setup is not None:
            setup(db, s)
        gate.drive(s)
        _not_started(gate, registry)

    def test_an_offline_serving_node_does_not_hold_it(self, db, gate, registry):
        # An OFFLINE node of B always has a queued restart task; it does not
        # close the gate, and while the site activates that task waits.
        s = _returned(db)
        _update(db, s.b[2], status=OFFLINE)
        task = _restart_task(db, s, s.b[2])
        gate.block(before=lambda cluster_id, site, **kwargs: src.begin_site_activation(cluster_id, site))
        gate.drive(s)
        assert gate.entered.wait(10)
        assert restart_runner._deferred_by_drain(db.get_cluster_by_id(s.cid), task) is True
        src.clear_site_activation(s.cid)
        gate.release.set()
        _started(gate, s)
        assert restart_runner._deferred_by_drain(db.get_cluster_by_id(s.cid), task) is False

    def test_the_gate_reason_is_logged(self, db, gate, registry):
        s = _returned(db)
        _update(db, s.b[1], status=StorageNode.STATUS_RESTARTING)
        with patch.object(monitor, "logger") as logger:
            gate.drive(s)
        logger.warning.assert_called_once_with(
            "can not activate site %s: %s", SITE_A,
            f"node {s.b[1].get_id()} is still transitioning ({StorageNode.STATUS_RESTARTING})")
        _not_started(gate, registry)


# ---------------------------------------------------------------------------
# the registry
# ---------------------------------------------------------------------------

class TestRegistry:

    def test_a_second_drive_starts_nothing(self, db, gate, registry):
        # The flag is not set (the activation is patched): the live thread is
        # what keeps the second drive from starting another.
        s = _returned(db)
        gate.block()
        gate.drive(s)
        assert gate.entered.wait(10)
        gate.drive(s)
        assert len(_live_threads()) == 1
        gate.release.set()
        _started(gate, s)

    def test_concurrent_starters_start_one(self, db, gate, registry):
        s = _returned(db)
        gate.block()
        barrier = threading.Barrier(2)
        results: list = []

        def _start():
            barrier.wait(10)
            results.append(monitor._start_site_activation(s.cid, SITE_A))
        starters = [threading.Thread(target=_start) for _ in range(2)]
        for starter in starters:
            starter.start()
        for starter in starters:
            starter.join(10)
        assert sorted(results) == [False, True]
        gate.release.set()
        _started(gate, s)

    def test_a_dead_entry_does_not_hold_the_gate(self, db, gate, registry):
        s = _returned(db)
        dead = threading.Thread(target=lambda: None)
        dead.start()
        dead.join()
        registry[s.cid] = dead
        gate.drive(s)
        _started(gate, s)
        assert registry == {}

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    @pytest.mark.parametrize("ending", ["cancelled", "failed", "uncaught", "refused"])
    def test_an_ended_activation_frees_the_gate(self, db, gate, registry, ending):
        s = _returned(db)
        endings = {"cancelled": src.SiteActivationCancelled("the token is gone"),
                   "failed": ValueError("Failed to activate site A"),
                   "uncaught": KeyError("a record is gone"),
                   "refused": REAL_ACTIVATE}
        gate.activate.side_effect = endings[ending]
        # "refused": the real wrapper, whose begin_site_activation refuses
        # right after the thread started - it raises before any work.
        with patch.object(src, "begin_site_activation", return_value="") as begin:
            gate.drive(s)
            join_site_activations()
        assert begin.call_count == (1 if ending == "refused" else 0)
        assert registry == {}
        gate.activate.side_effect = None
        gate.drive(s)
        join_site_activations()
        assert gate.activate.call_args_list == [call(s.cid, force=True, site=SITE_A)] * 2


class TestMonitorPass:

    def test_the_pass_starts_it_and_does_not_wait(self, db, monitor_pass, registry):
        s = _returned(db)
        release, entered = threading.Event(), threading.Event()

        def _held(cluster_id, **kwargs):
            entered.set()
            assert release.wait(10)
        monitor_pass.activate.side_effect = _held
        try:
            after = monitor_pass.run(s.cluster)
            assert entered.wait(10)
            assert monitor._site_activation_running(s.cid)
            assert (after.lost_site, after.lost_site_state) == (SITE_A, DOWN)
            monitor_pass.activate.assert_called_once_with(s.cid, force=True, site=SITE_A)
        finally:
            release.set()


# ---------------------------------------------------------------------------
# the real site activation, held inside Pass 2
# ---------------------------------------------------------------------------

class _Live:
    """Monitor passes over the four LVS shapes with the real site activation:
    the pass's probes, drain shutdown thread and ANA / distr side effects
    patched as in ``test_sync_replication_disaster.monitor_pass``; a site
    activation runs the real ``cluster_activate`` (``activate`` records every
    call), a full one is recorded only; the site activation's primitives are
    captured (``bnd``) and its first Pass 2 rebuild held until ``release``."""

    def __init__(self, db, es):
        self.db = db
        self.pass_ = disaster._Pass(db)
        self.s = site_activation._shapes(db)
        self.release, self.entered = threading.Event(), threading.Event()
        set_status = cluster_ops.set_cluster_status

        def _set_status(cluster_id, status):
            self.pass_.status_writes.append((status, _record(db, cluster_id)))
            return set_status(cluster_id, status)

        es.enter_context(patch.object(monitor, "_collect_status_probes", side_effect=self.pass_.probes))
        es.enter_context(patch.object(monitor, "_collect_site_gone", side_effect=self.pass_.site_gone))
        self.shutdown = es.enter_context(patch.object(monitor, "_spawn_recovery_shutdown"))
        es.enter_context(patch.object(cluster_ops, "set_cluster_status", side_effect=_set_status))
        self.activate = es.enter_context(patch.object(
            cluster_ops, "cluster_activate",
            side_effect=lambda cluster_id, **kwargs: REAL_ACTIVATE(cluster_id, **kwargs) if kwargs.get("site") else None))
        es.enter_context(patch.object(monitor, "_maybe_enable_shared_placement"))
        es.enter_context(patch.object(monitor, "_maybe_switch_write_protection"))
        es.enter_context(patch.object(ops, "trigger_ana_failover_for_node"))
        es.enter_context(patch("simplyblock_core.distr_controller.send_node_status_event"))
        es.enter_context(patch("simplyblock_core.distr_controller.send_dev_status_event"))
        es.enter_context(patch.object(cluster_ops.constants, "CLUSTER_ACTIVATION_MAX_PARALLEL_NODES", 1))
        self.bnd = site_activation._Boundary(db, es, self.s)
        self.bnd.hook = self._hook

    def _hook(self, route, node_id, owner_id):
        if route != "p1" and not self.entered.is_set():
            self.entered.set()
            assert self.release.wait(10)

    def run(self):
        return self.pass_.run(self.db.get_cluster_by_id(self.s.cid))

    def rebuilds(self):
        return self.bnd.calls("act", *site_activation.RESTART_ROUTES)


@pytest.fixture()
def live(db, rpcs, hub, adopt, registry):
    with ExitStack() as es:
        lv = _Live(db, es)
        try:
            yield lv
        finally:
            lv.release.set()
            join_site_activations()


class TestTheStatusPassGoesOn:

    def test_a_serving_node_goes_offline_meanwhile(self, db, live):
        s = live.s
        live.run()
        assert live.entered.wait(10)
        writes = len(live.pass_.status_writes)
        # set_node_offline drives the status pass itself: held behind the
        # activation, it would only mark the cluster pending and write nothing.
        monitor.set_node_offline(s.b[2])
        assert _fresh(db, s.b[2]).status == OFFLINE
        assert live.pass_.status_writes[writes:] == [(Cluster.STATUS_DEGRADED, (SITE_A, DOWN))]
        assert monitor._site_activation_running(s.cid)
        assert len(live.rebuilds()) == 1
        live.release.set()
        join_site_activations()


class TestSuspensionDuringIt:

    def test_a_cluster_restart_waits_for_it(self, db, live):
        s = live.s
        live.run()
        assert live.entered.wait(10)
        flag = _flag(db, s.cid)
        assert flag[0] == SITE_A
        cluster_ops.cluster_restart(s.cid)

        after = live.run()
        assert after.status == Cluster.STATUS_SUSPENDED
        assert _flag(db, s.cid) == flag
        live.shutdown.assert_not_called()
        live.activate.assert_called_once_with(s.cid, force=True, site=SITE_A)
        # an operator's activation, from any process, refuses on the flag
        with pytest.raises(ValueError, match=f"site {SITE_A} is being activated"):
            REAL_ACTIVATE(s.cid, force=True)

        live.release.set()
        join_site_activations()
        # stopped at its next checkpoint: no further rebuild, no return
        assert len(live.rebuilds()) == 1
        assert live.bnd.calls("ana", "event") == []
        assert _flag(db, s.cid) == ("", "")
        assert _record(db, s.cid) == ("", "")      # the monitor's record, cleared under the suspension

        live.run()
        assert live.shutdown.call_count == 6
