"""The per-node monitor threads and the loop that supervises them.

2026-09-30, node vm201_4420: an FDB transaction timeout (1031) killed three of
the four per-node threads in StorageNodeMonitor and, one second later, the main
loop that replaces them. The workers were non-daemon, so the one survivor held
the interpreter open for 2h37m -- the process stayed up, monitoring a single
node, and the orchestrator never saw it fail. The node under test had its SPDK
killed two minutes later, nothing marked it offline, no restart was queued, and
the cluster reported it `online` with a dead data plane for 2h23m.

The contract these tests pin:

- a worker that fails dies, and the next main-loop tick replaces it;
- a main loop that fails ends the process, because the workers are daemons;
- replacing is bounded, so a wedged FDB client (which a replacement thread
  inherits, `db` being process-global) escalates to that process exit instead
  of churning threads forever.
"""
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.services import health_check_service as hcs
from simplyblock_core.services import main_distr_event_collector as mdec
from simplyblock_core.services import storage_node_monitor as snm


class _LoopDone(Exception):
    """Breaks a service's `while True` out of its sleep."""


class _DBBoom(Exception):
    """Stands in for an FDB error out of a db_controller read."""


class _FakeThread:
    """Records construction instead of running anything."""

    def __init__(self, target=None, args=(), daemon=False, name=None):
        self.target = target
        self.args = args
        self.daemon = daemon
        self.alive = True

    def start(self):
        pass

    def is_alive(self):
        return self.alive


@pytest.fixture
def spawned():
    """Collects every _FakeThread the module under test constructs."""
    made: list[_FakeThread] = []

    def factory(*args, **kwargs):
        t = _FakeThread(*args, **kwargs)
        made.append(t)
        return t

    return made, factory


@pytest.fixture(autouse=True)
def _clear_respawn_history():
    snm._thread_respawns.clear()
    mdec.threads_maps.clear()
    yield
    snm._thread_respawns.clear()
    mdec.threads_maps.clear()


def _node(node_id="node-a"):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.status = "online"
    return n


def _cluster(cluster_id="cluster-1"):
    c = MagicMock()
    c.get_id.return_value = cluster_id
    c.status = "active"
    return c


def _stop_after(ticks):
    """A time.sleep side effect that ends the loop after `ticks` iterations."""
    state = {"n": 0}

    def side_effect(_seconds):
        state["n"] += 1
        if state["n"] >= ticks:
            raise _LoopDone

    return side_effect


def _run_monitor(spawned, ticks, nodes=None):
    """Drive snm.main() for `ticks` iterations. Returns the threads it made."""
    made, factory = spawned
    db = MagicMock()
    db.get_clusters.return_value = [_cluster()]
    db.get_storage_nodes_by_cluster_id.return_value = nodes or [_node()]
    with (
        patch.object(snm, "db", db),
        patch.object(snm, "update_cluster_status"),
        patch.object(snm, "_run_periodic_housekeeping"),
        patch.object(snm.threading, "Thread", factory),
        patch.object(snm.time, "sleep", side_effect=_stop_after(ticks)),
    ):
        with pytest.raises(_LoopDone):
            snm.main()
    return made


class TestWorkerFailure:
    def test_loop_for_node_does_not_swallow(self):
        """A failing check_node must kill the thread, so main() can see it."""
        with patch.object(snm, "check_node", side_effect=_DBBoom):
            with pytest.raises(_DBBoom):
                snm.loop_for_node(_node())

    def test_dead_thread_is_replaced_on_the_next_tick(self, spawned):
        made, factory = spawned
        db = MagicMock()
        db.get_clusters.return_value = [_cluster()]
        db.get_storage_nodes_by_cluster_id.return_value = [_node()]

        def kill_then_tick(_seconds):
            made[-1].alive = False
            if len(made) >= 2:
                raise _LoopDone

        with (
            patch.object(snm, "db", db),
            patch.object(snm, "update_cluster_status"),
            patch.object(snm, "_run_periodic_housekeeping"),
            patch.object(snm.threading, "Thread", factory),
            patch.object(snm.time, "sleep", side_effect=kill_then_tick),
        ):
            with pytest.raises(_LoopDone):
                snm.main()

        assert len(made) == 2
        assert made[1].target is snm.loop_for_node

    def test_a_live_thread_is_not_replaced(self, spawned):
        made = _run_monitor(spawned, ticks=4)
        assert len(made) == 1


class TestDaemonThreads:
    """A dead main thread must end the process, not leave a partial monitor."""

    def test_monitor_workers_are_daemons(self, spawned):
        made = _run_monitor(spawned, ticks=1)
        assert made and all(t.daemon for t in made)

    def test_health_check_workers_are_daemons(self, spawned):
        made, factory = spawned
        db = MagicMock()
        db.get_clusters.return_value = [_cluster()]
        db.get_storage_nodes_by_cluster_id.return_value = [_node()]
        with (
            patch.object(hcs, "db", db),
            patch.dict(hcs.threads_maps, clear=True),
            patch.object(hcs.threading, "Thread", factory),
            patch.object(hcs.time, "sleep", side_effect=_stop_after(1)),
        ):
            with pytest.raises(_LoopDone):
                hcs.main()
        assert made and all(t.daemon for t in made)

    def test_event_collector_workers_are_daemons(self, spawned):
        made, factory = spawned
        with patch.object(mdec.threading, "Thread", factory):
            mdec.ensure_collectors([_node()])
        assert made and all(t.daemon for t in made)


class TestRespawnCeiling:
    def test_repeated_death_exits_instead_of_churning(self, spawned):
        """Past the ceiling the failure must leave main(), so the orchestrator
        restarts the service with a fresh FDB client."""
        made, factory = spawned
        db = MagicMock()
        db.get_clusters.return_value = [_cluster()]
        db.get_storage_nodes_by_cluster_id.return_value = [_node("node-a")]

        with (
            patch.object(snm, "db", db),
            patch.object(snm, "update_cluster_status"),
            patch.object(snm, "_run_periodic_housekeeping"),
            patch.object(snm.threading, "Thread", factory),
            patch.object(snm.time, "sleep", side_effect=lambda _s: made[-1].__setattr__("alive", False)),
        ):
            with pytest.raises(RuntimeError, match="node-a"):
                snm.main()

        # The first spawn is not a respawn, so the ceiling is reached one tick
        # later than its value.
        assert len(made) == snm.THREAD_RESPAWN_CEILING + 1

    def test_deaths_outside_the_window_do_not_accumulate(self):
        with patch.object(snm.time, "time", side_effect=[0.0, 1.0]):
            assert snm._record_thread_respawn("node-a") == 1
            assert snm._record_thread_respawn("node-a") == 2

        far = snm.THREAD_RESPAWN_WINDOW_SEC + 1
        with patch.object(snm.time, "time", return_value=far):
            assert snm._record_thread_respawn("node-a") == 1

    def test_the_ceiling_is_per_node(self):
        with patch.object(snm.time, "time", return_value=0.0):
            for _ in range(snm.THREAD_RESPAWN_CEILING):
                snm._record_thread_respawn("node-a")
            assert snm._record_thread_respawn("node-b") == 1


class TestMainLoopFailure:
    def test_main_does_not_swallow_db_errors(self, spawned):
        """The loop must not retry past a DB failure: it ends the process and
        the orchestrator restarts it with a fresh client."""
        _made, factory = spawned
        db = MagicMock()
        db.get_clusters.side_effect = _DBBoom
        with (
            patch.object(snm, "db", db),
            patch.object(snm.threading, "Thread", factory),
            patch.object(snm.time, "sleep"),
        ):
            with pytest.raises(_DBBoom):
                snm.main()

    def test_main_does_not_swallow_a_node_read_failure(self, spawned):
        """get_storage_nodes_by_cluster_id is the call that was fatal on
        2026-09-30, and it sat outside the loop's only try."""
        _made, factory = spawned
        db = MagicMock()
        db.get_clusters.return_value = [_cluster()]
        db.get_storage_nodes_by_cluster_id.side_effect = _DBBoom
        with (
            patch.object(snm, "db", db),
            patch.object(snm.threading, "Thread", factory),
            patch.object(snm.time, "sleep"),
        ):
            with pytest.raises(_DBBoom):
                snm.main()
