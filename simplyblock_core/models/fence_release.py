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

This record is the shared memory those processes lack: the wall-clock times
one (cluster, node, port) was released, kept for FENCE_RELEASE_WINDOW_SEC.
Once FENCE_RELEASE_BUDGET releases fall inside the window the port stays
fenced, an error event names the LVS, and the budget re-opens by itself when
the window has rolled past -- so a fence that really is stale by then is
still lifted, just not every 15 s.

Two writers racing on the very first record of a key (plain write, no
compare-and-set yet) can lose one stamp, which only makes the budget one
release more lenient once; every later update goes through
``DBController.atomic_update``.
"""

from simplyblock_core import constants
from simplyblock_core.models.base_model import BaseModel, default_factory


class FenceReleaseLedger(BaseModel):

    cluster_id: str = ""
    node_id: str = ""
    port: int = 0

    #: ``time.time()`` stamps (wall clock, NOT ``time.monotonic()``: these are
    #: compared across processes, possibly on different hosts) of the releases
    #: still inside the window, oldest first.
    releases: list[float] = default_factory(list)

    #: Wall-clock time the fence-loop event was last emitted for this key, or
    #: 0 if never. One event per window, not one per refused tick.
    loop_reported_at: float = 0.0

    def get_id(self):
        return f"{self.cluster_id}/{self.node_id}/{self.port}"

    # ---- pure logic; the controller only loads, calls, stores ----

    def prune(self, now: float) -> None:
        """Drop the stamps that have fallen out of the window."""
        window = constants.FENCE_RELEASE_WINDOW_SEC
        self.releases = [t for t in self.releases if now - t < window]

    def record(self, now: float) -> None:
        self.prune(now)
        self.releases.append(now)

    def exhausted(self, now: float) -> bool:
        """True once FENCE_RELEASE_BUDGET releases fall inside the window."""
        self.prune(now)
        return len(self.releases) >= constants.FENCE_RELEASE_BUDGET

    def reopens_in(self, now: float) -> float:
        """Seconds until the oldest counted release leaves the window."""
        self.prune(now)
        if not self.releases:
            return 0.0
        return max(0.0, constants.FENCE_RELEASE_WINDOW_SEC - (now - self.releases[0]))

    def should_report(self, now: float) -> bool:
        """Whether the loop event is due: never sent, or sent a window ago."""
        return now - self.loop_reported_at >= constants.FENCE_RELEASE_WINDOW_SEC
