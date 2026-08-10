# coding=utf-8
"""Unit tests for simplyblock_lib.monitors.polling.PollingService."""
import pytest

from simplyblock_lib.monitors.polling import PollingService


class Recorder(PollingService):
    def __init__(self, outcomes, **kwargs):
        self.sleeps = []
        kwargs.setdefault("sleep", self.sleeps.append)
        super().__init__("recorder", **kwargs)
        self.outcomes = list(outcomes)
        self.ticks = 0

    def tick(self):
        self.ticks += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_normal_tick_sleeps_full_interval():
    svc = Recorder([None], interval_sec=30)
    svc.run_once()
    assert svc.ticks == 1
    assert svc.sleeps == [30]


def test_fast_interval_on_pending_work():
    svc = Recorder([True, False], interval_sec=30, fast_interval_sec=2)
    svc.run_once()
    svc.run_once()
    assert svc.sleeps == [2, 30]


def test_true_without_fast_interval_uses_normal():
    svc = Recorder([True], interval_sec=30)
    svc.run_once()
    assert svc.sleeps == [30]


def test_tick_failure_uses_error_cadence():
    svc = Recorder([RuntimeError("db down"), None], interval_sec=30, error_interval_sec=3)
    svc.run_once()
    svc.run_once()
    assert svc.sleeps == [3, 30]


def test_failure_threshold_exits():
    svc = Recorder([RuntimeError("x")] * 3, interval_sec=30, failure_threshold=3)
    svc.run_once()
    svc.run_once()
    with pytest.raises(SystemExit):
        svc.run_once()


def test_success_resets_failure_counter():
    svc = Recorder([RuntimeError("x"), None, RuntimeError("x"), RuntimeError("x")],
                   interval_sec=30, failure_threshold=2)
    svc.run_once()  # failure 1
    svc.run_once()  # success resets
    svc.run_once()  # failure 1 again
    with pytest.raises(SystemExit):
        svc.run_once()  # failure 2


def test_no_threshold_never_exits():
    svc = Recorder([RuntimeError("x")] * 100, interval_sec=30)
    for _ in range(100):
        svc.run_once()
    assert svc.ticks == 100
