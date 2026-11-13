"""Virtual time for the two-node simulator.

Every clock the code under test reads -- the arbiter's injected ``clock``,
``time.time`` / ``time.monotonic`` in the helpers it calls, task dates written
by ``tasks_controller`` -- is this one, so a scenario of minutes runs without
waiting and identically on every run.

Blocking is modelled, not slept. A node RPC "takes" virtual time (its latency,
or its whole timeout against a frozen SPDK):

* on the main thread (the arbiter's decision and lease loops run there,
  inline) the time is charged to the current *call context*; the loop's next
  run starts only after it -- exactly how a blocking RPC on the decision thread
  delays a verdict (campaign c7, F16);
* in a pool call or a recovery thread (``CoopTask``) the call parks its real
  thread until the virtual clock reaches the call's end, so its side effects
  (a node marked unreachable, a status applied) happen when the call returns,
  as with real threads. Only one thread runs at any moment: the scheduler
  hands control to a task and waits until it parks or finishes, so runs are
  deterministic.

``time.sleep`` behaves the same way; on the main thread outside any context it
advances the clock.
"""

from __future__ import annotations

import heapq
import itertools
import threading
import time as _real_time
from collections.abc import Callable
from contextlib import contextmanager

_local = threading.local()


def current_task():
    return getattr(_local, "task", None)


class CallContext:
    __slots__ = ("elapsed", "name")

    def __init__(self, name: str):
        self.elapsed = 0.0
        self.name = name


class CoopTask:
    """A real thread that runs only while the scheduler hands it control."""

    def __init__(self, vt: VirtualTime, fn: Callable, name: str, on_done=None):
        self.vt, self.fn, self.name, self.on_done = vt, fn, name, on_done
        self.go, self.parked = threading.Event(), threading.Event()
        self.done, self.result, self.exc = False, None, None
        self.thread = threading.Thread(target=self._body, daemon=True, name="sim-" + name)

    def _body(self):
        self.go.wait()
        self.go.clear()
        _local.task = self
        try:
            self.result = self.fn()
        except BaseException as e:                  # noqa: BLE001 - handed to on_done
            self.exc = e
        finally:
            self.done = True
            self.parked.set()

    def start(self):
        self.thread.start()
        self.resume()

    def resume(self):
        """Hand control to the task until it parks again or finishes."""
        self.parked.clear()
        self.go.set()
        self.parked.wait()
        if self.done and self.on_done is not None:
            cb, self.on_done = self.on_done, None
            cb(self)

    def block(self, seconds: float):
        """In the task's own thread: park until ``seconds`` have passed."""
        if seconds <= 0:
            return
        self.vt.at(self.vt.now + seconds, self.resume)
        self.parked.set()
        self.go.wait()
        self.go.clear()


class VirtualTime:
    def __init__(self, start_s: float | None = None):
        self.now = float(start_s if start_s is not None else _real_time.time())
        self._ctx: list[CallContext] = []
        self._heap: list = []
        self._seq = itertools.count()

    # ---- the clocks the code under test reads -------------------------------

    def time(self) -> float:
        if current_task() is not None:
            return self.now
        return self.now + (self._ctx[-1].elapsed if self._ctx else 0.0)

    def time_ns(self) -> int:
        return int(self.time() * 1e9)

    def monotonic(self) -> float:
        return self.time()

    def sleep(self, seconds: float) -> None:
        seconds = max(0.0, float(seconds))
        task = current_task()
        if task is not None:
            task.block(seconds)
        elif self._ctx:
            self._ctx[-1].elapsed += seconds
        else:
            self.now += seconds

    def ms(self) -> int:
        return int(self.time() * 1000)

    # ---- call contexts and blocking ---------------------------------------------

    @contextmanager
    def context(self, name: str):
        ctx = CallContext(name)
        self._ctx.append(ctx)
        try:
            yield ctx
        finally:
            self._ctx.pop()

    def charge(self, seconds: float) -> None:
        """An RPC took ``seconds``: a task parks for it; on the main thread the
        current context is charged (outside one the call is instantaneous)."""
        task = current_task()
        if task is not None:
            task.block(seconds)
        elif self._ctx:
            self._ctx[-1].elapsed += max(0.0, seconds)

    def spawn(self, fn: Callable, name: str, on_done=None) -> CoopTask:
        task = CoopTask(self, fn, name, on_done)
        task.start()
        return task

    def fold_context(self) -> None:
        """The main thread starts to wait: its context's elapsed time becomes
        clock time, so the events of that window can run meanwhile."""
        if self._ctx and current_task() is None:
            self.now += self._ctx[-1].elapsed
            self._ctx[-1].elapsed = 0.0

    # ---- scheduler ------------------------------------------------------------

    def at(self, t: float, fn: Callable[[], None]) -> None:
        heapq.heappush(self._heap, (t, next(self._seq), fn))

    def after(self, dt: float, fn: Callable[[], None]) -> None:
        self.at(self.now + dt, fn)

    def run_while(self, pending: Callable[[], bool], t_end: float) -> None:
        """Run events up to ``t_end`` while ``pending()`` holds (nested in a wait)."""
        while pending() and self._heap and self._heap[0][0] <= t_end:
            t, _, fn = heapq.heappop(self._heap)
            if t > self.now:
                self.now = t
            fn()
        if pending() and t_end > self.now:
            self.now = t_end

    def run_until(self, t_end: float) -> None:
        while self._heap and self._heap[0][0] <= t_end:
            t, _, fn = heapq.heappop(self._heap)
            if t > self.now:
                self.now = t
            fn()
        if t_end > self.now:
            self.now = t_end


def install_virtual_time(monkeypatch, vt: VirtualTime) -> None:
    """Point the ``time`` module at the virtual clock for the scenario."""
    monkeypatch.setattr(_real_time, "time", vt.time)
    monkeypatch.setattr(_real_time, "time_ns", vt.time_ns)
    monkeypatch.setattr(_real_time, "monotonic", vt.monotonic)
    monkeypatch.setattr(_real_time, "sleep", vt.sleep)
