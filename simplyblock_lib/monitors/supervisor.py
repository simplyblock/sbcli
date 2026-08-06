# coding=utf-8
"""Thread-per-item supervisor skeleton for monitor services.

The "pattern B" monitor (storage-node monitor, health-check service) keeps one
long-lived worker thread per item (node), respawning any thread that died, and
re-discovers the item set every cycle. This base class owns discovery-loop +
respawn; the caller provides ``discover`` and ``worker``.

- ``discover()`` yields ``(key, item)`` pairs (e.g. ``(node_id, node)``).
  A failure inside discovery is logged and retried after ``error_interval_sec``
  — it must not kill the supervisor.
- ``worker(item)`` runs in a daemon thread and normally loops forever with its
  own cadence. If it returns (e.g. the item was deleted) or crashes, the next
  discovery cycle that still yields the key respawns it.
- ``on_cycle()`` (optional) runs once per discovery cycle after respawning —
  the storage-node monitor uses this slot for the cluster-status update.
"""

import logging
import threading
import time


class PerItemSupervisor:
    """Discovery loop that maintains one worker thread per discovered item."""

    def __init__(self, discover, worker, *, interval_sec, name=None,
                 on_cycle=None, error_interval_sec=3, logger=None,
                 sleep=time.sleep):
        self.name = name or type(self).__name__
        self._discover = discover
        self._worker = worker
        self._on_cycle = on_cycle
        self.interval_sec = interval_sec
        self.error_interval_sec = error_interval_sec
        self._logger = logger or logging.getLogger(self.name)
        self._sleep = sleep
        self.threads: dict = {}  # key -> threading.Thread

    def run_forever(self):
        self._logger.info(f"Starting {self.name}...")
        while True:
            self.run_once()

    def run_once(self):
        """One discovery cycle + the matching sleep (extracted for tests)."""
        try:
            items = list(self._discover())
        except Exception as e:
            self._logger.error(f"{self.name} discovery failed: {e}")
            self._sleep(self.error_interval_sec)
            return

        for key, item in items:
            thread = self.threads.get(key)
            if thread is None or not thread.is_alive():
                self._logger.info(f"{self.name}: starting worker for {key}")
                thread = threading.Thread(
                    target=self._run_worker, args=(key, item), daemon=True)
                thread.start()
                self.threads[key] = thread

        if self._on_cycle is not None:
            try:
                self._on_cycle()
            except Exception as e:
                self._logger.error(f"{self.name} on_cycle failed: {e}")

        self._sleep(self.interval_sec)

    def _run_worker(self, key, item):
        try:
            self._worker(item)
        except Exception as e:
            self._logger.error(f"{self.name} worker for {key} crashed: {e}")
            self._logger.exception(e)
