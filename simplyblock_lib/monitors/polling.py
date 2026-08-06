# coding=utf-8
"""Flat sweep-loop skeleton for monitor services.

The "pattern A" monitor (device monitor, capacity monitor, lvol monitor, …)
is a ``while True`` loop that sweeps state, isolates per-item failures, and
sleeps a fixed — or adaptive — interval. This base class owns the loop; the
subclass owns the sweep.

Cross-cutting behaviors provided here:

- **Error cadence**: an exception escaping ``tick()`` is logged and the loop
  re-runs after the short ``error_interval_sec`` instead of the full interval
  (a transient DB read failure shouldn't stall monitoring for a full cycle).
- **DB-wedge self-restart** (opt-in via ``failure_threshold``): after that many
  *consecutive* failing ticks the process exits(1) so the orchestrator restarts
  it with a clean DB connection. A long-lived process whose FDB client wedges
  never recovers by retrying the same handle (incident
  mass_create_delete_docker-20260629).
- **Adaptive interval**: ``tick()`` returning True selects
  ``fast_interval_sec`` for the next sleep (work pending / recovery in
  progress); any other return uses ``interval_sec``.
"""

import logging
import sys
import time


class PollingService:
    """Base class for a sweep-loop monitor service."""

    def __init__(self, name=None, *, interval_sec, fast_interval_sec=None,
                 error_interval_sec=3, failure_threshold=None,
                 logger=None, sleep=time.sleep):
        self.name = name or type(self).__name__
        self.interval_sec = interval_sec
        self.fast_interval_sec = fast_interval_sec
        self.error_interval_sec = error_interval_sec
        self.failure_threshold = failure_threshold
        self._logger = logger or logging.getLogger(self.name)
        self._sleep = sleep
        self._consecutive_failures = 0

    def tick(self):
        """One sweep. Return True to poll again at ``fast_interval_sec``.
        Per-item failures should be isolated *inside* the sweep (one bad node
        must not abort the rest); an exception escaping this method counts
        toward the wedge threshold."""
        raise NotImplementedError

    def run_forever(self):
        self._logger.info(f"Starting {self.name}...")
        while True:
            self.run_once()

    def run_once(self):
        """One tick + the matching sleep (extracted for tests)."""
        try:
            fast = self.tick() is True
        except Exception as e:
            self._consecutive_failures += 1
            self._logger.error(f"{self.name} tick failed ({self._consecutive_failures}): {e}")
            if (self.failure_threshold is not None
                    and self._consecutive_failures >= self.failure_threshold):
                self._logger.error(
                    f"{self.name}: DB unreadable for too long (client likely wedged); "
                    "exiting for a clean restart")
                sys.exit(1)
            self._sleep(self.error_interval_sec)
            return
        self._consecutive_failures = 0
        if fast and self.fast_interval_sec is not None:
            self._sleep(self.fast_interval_sec)
        else:
            self._sleep(self.interval_sec)
