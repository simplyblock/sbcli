# coding=utf-8
"""Unit tests for simplyblock_lib.monitors.supervisor.PerItemSupervisor."""
import threading

from simplyblock_lib.monitors.supervisor import PerItemSupervisor


def _make(items, worker, **kwargs):
    kwargs.setdefault("interval_sec", 0)
    kwargs.setdefault("sleep", lambda _s: None)
    return PerItemSupervisor(lambda: list(items), worker, **kwargs)


def test_spawns_one_worker_per_item():
    started = []
    release = threading.Event()

    def worker(item):
        started.append(item)
        release.wait(timeout=5)

    sup = _make([("a", "item-a"), ("b", "item-b")], worker)
    sup.run_once()
    for thread in sup.threads.values():
        assert thread.is_alive()
    release.set()
    for thread in sup.threads.values():
        thread.join(timeout=5)
    assert sorted(started) == ["item-a", "item-b"]


def test_live_worker_not_respawned():
    starts = []
    release = threading.Event()

    def worker(item):
        starts.append(item)
        release.wait(timeout=5)

    sup = _make([("a", "item-a")], worker)
    sup.run_once()
    sup.run_once()
    sup.run_once()
    assert starts == ["item-a"]
    release.set()


def test_dead_worker_respawned():
    starts = []

    def worker(item):
        starts.append(item)  # returns immediately → thread dies

    sup = _make([("a", "item-a")], worker)
    sup.run_once()
    sup.threads["a"].join(timeout=5)
    sup.run_once()
    sup.threads["a"].join(timeout=5)
    assert starts == ["item-a", "item-a"]


def test_crashing_worker_is_contained_and_respawned():
    starts = []

    def worker(item):
        starts.append(item)
        raise RuntimeError("worker crash")

    sup = _make([("a", "item-a")], worker)
    sup.run_once()
    sup.threads["a"].join(timeout=5)
    sup.run_once()
    sup.threads["a"].join(timeout=5)
    assert starts == ["item-a", "item-a"]


def test_discovery_failure_uses_error_cadence():
    sleeps = []

    def discover():
        raise RuntimeError("db down")

    sup = PerItemSupervisor(discover, lambda item: None,
                            interval_sec=30, error_interval_sec=3,
                            sleep=sleeps.append)
    sup.run_once()
    assert sleeps == [3]
    assert sup.threads == {}


def test_on_cycle_runs_each_cycle_and_is_isolated():
    calls = []

    def on_cycle():
        calls.append(1)
        raise RuntimeError("cycle hook crash")

    sup = _make([], lambda item: None, on_cycle=on_cycle)
    sup.run_once()
    sup.run_once()
    assert len(calls) == 2
