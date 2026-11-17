"""
fence_release.py — cross-process ledger of client-port fence releases.

Three processes release a client LVS port that SPDK fenced: the storage-node
monitor's stale-fence remediation, the health-check service (which schedules a
port_allow task when it sees the port blocked) and the port_allow task runner.
Each of them is right to release a fence that leaked once. None of them can
tell, on its own, that it is the third process this minute to reopen a port
that SPDK closes again on the very next IO.

That loop is what turned one unreadable stripe into a nine-minute outage on a
production cluster (2026-11-01, LVS_4033, 2x2): a stripe with an unallocated
column could not be served while one of its nodes was rebooting, so every
retried read made the leader fence its port, a peer took leadership on the
next retry and fenced too, and the control plane reopened each port within
15 s -- 60 leadership switches, 9 port_allow tasks, 144 failed reads, and the
leadership churn drove a peer's SPDK into a self-abort.

This record is the shared memory those processes lack: per (cluster,
lvstore), the wall-clock times each node's port for that lvstore was
released, kept for FENCE_RELEASE_WINDOW_SEC. The key is the lvstore, not the
(node, port), because the loop moves: each re-fence hands leadership and the
fence to a peer, so a per-node budget would give every path its own three
releases before anything stopped. Once FENCE_RELEASE_BUDGET releases of any
of the lvstore's paths fall inside the window, the remaining paths stay
fenced, an error event names the LVS, and the budget re-opens by itself when
the window has rolled past -- so a fence that really is stale by then is
still lifted, just not every 15 s.

Two things the ledger deliberately does not do, both decided in review:

* It never fences the lvstore's last path. The controller exempts the
  preferred path (the primary, or the first online follower) from the gate,
  so volumes on the lvstore keep a route in and only the leadership
  ping-pong between peers is stopped.
* It forgets a node's releases when that node's SPDK is restarted
  (``forget``). A restart reopens the ports by design; counting those
  reopens would leave a healthy node fenced after three restarts in ten
  minutes.

Two writers racing on the very first record of a key (plain write, no
compare-and-set yet) can lose one stamp, which only makes the budget one
release more lenient once; every later update goes through
``DBController.atomic_update``.
"""

from simplyblock_core import constants
from simplyblock_core.models.base_model import BaseModel, default_factory


class FenceReleaseLedger(BaseModel):

    cluster_id: str = ""
    lvstore: str = ""

    #: node_id -> ``time.time()`` stamps (wall clock, NOT ``time.monotonic()``:
    #: these are compared across processes, possibly on different hosts) of
    #: the releases of that node's port for this lvstore still inside the
    #: window, oldest first.
    releases: dict = default_factory(dict)

    #: Wall-clock time the fence-loop event was last emitted for this key, or
    #: 0 if never. One event per window, not one per refused tick.
    loop_reported_at: float = 0.0

    def get_id(self):
        return f"{self.cluster_id}/{self.lvstore}"

    # ---- pure logic; the controller only loads, calls, stores ----

    def prune(self, now: float) -> None:
        """Drop the stamps that have fallen out of the window."""
        window = constants.FENCE_RELEASE_WINDOW_SEC
        kept = {}
        for node_id, stamps in self.releases.items():
            inside = [t for t in stamps if now - t < window]
            if inside:
                kept[node_id] = inside
        self.releases = kept

    def record(self, node_id: str, now: float) -> None:
        self.prune(now)
        self.releases.setdefault(node_id, []).append(now)

    def forget(self, node_id: str) -> None:
        """A restart of ``node_id`` reopens its ports by design: those
        releases are not evidence of a loop."""
        self.releases.pop(node_id, None)

    def count(self, now: float) -> int:
        """Releases of any path of this lvstore inside the window."""
        self.prune(now)
        return sum(len(stamps) for stamps in self.releases.values())

    def exhausted(self, now: float) -> bool:
        """True once FENCE_RELEASE_BUDGET releases fall inside the window."""
        return self.count(now) >= constants.FENCE_RELEASE_BUDGET

    def reopens_in(self, now: float) -> float:
        """Seconds until the oldest counted release leaves the window."""
        self.prune(now)
        stamps = [t for node_stamps in self.releases.values() for t in node_stamps]
        if not stamps:
            return 0.0
        return max(0.0, constants.FENCE_RELEASE_WINDOW_SEC - (now - min(stamps)))

    def should_report(self, now: float) -> bool:
        """Whether the loop event is due: never sent, or sent a window ago."""
        return now - self.loop_reported_at >= constants.FENCE_RELEASE_WINDOW_SEC
