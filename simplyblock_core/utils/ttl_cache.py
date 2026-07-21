"""Small thread-safe TTL cache for expensive, staleness-tolerant reads.

Used on the hot create paths (lvol / snapshot / clone) to avoid re-deriving
cluster state that changes rarely but is re-verified on every operation:
full-DB capacity scans, lvstore leadership, and per-node data-plane quorum
verdicts. Staleness is bounded by the per-entry TTL; callers must only cache
values whose consumers tolerate that window (advisory capacity checks,
optimistically-verified leadership, connectivity verdicts that the operation
re-validates by simply failing and retrying).

Per-process only — each service / API worker keeps its own view. Writers that
know they changed the underlying state (e.g. a forced leadership failover)
should call ``invalidate`` so their own process re-reads immediately.
"""

import threading
import time
from typing import Any, Callable

_NOT_SET = object()


class TTLCache:
    # How long a follower waits for the in-flight leader compute before
    # giving up and computing on its own (leader crashed / hung on FDB).
    _INFLIGHT_WAIT_SEC = 60.0

    def __init__(self):
        self._data: dict[Any, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._inflight: dict[Any, threading.Event] = {}

    def get(self, key, ttl: float):
        """Return the cached value for ``key`` if younger than ``ttl`` seconds,
        else ``None``."""
        now = time.monotonic()
        with self._lock:
            entry = self._data.get(key)
            if entry is not None and (now - entry[0]) < ttl:
                return entry[1]
        return None

    def put(self, key, value) -> None:
        with self._lock:
            self._data[key] = (time.monotonic(), value)

    def invalidate(self, key=_NOT_SET) -> None:
        """Drop one key, or the whole cache when called without arguments."""
        with self._lock:
            if key is _NOT_SET:
                self._data.clear()
            else:
                self._data.pop(key, None)

    def get_or_compute(self, key, ttl: float, compute: Callable[[], Any],
                       cache_none: bool = False):
        """Return the cached value or run ``compute()`` and cache its result.

        Single-flight: at most ONE thread per process runs ``compute()`` for a
        given key at a time. Without this, every request arriving after the
        TTL lapsed fired its own full-DB scan; once the scan grew slower than
        the TTL (mass-snapshot run 2026-07-21: 15s scan vs 10s TTL) the cache
        was permanently stale and the scans stampeded FDB into 1031 timeouts.
        While a refresh is in flight, other threads are served the stale value
        (staleness-tolerant by contract of this cache); threads with no stale
        value wait for the leader and fall back to computing themselves only
        if the leader fails.

        ``None`` results are not cached by default (a failed probe should not
        suppress retries for a full TTL); pass ``cache_none=True`` where
        ``None`` is a meaningful, stable answer.
        """
        now = time.monotonic()
        stale = None
        with self._lock:
            entry = self._data.get(key)
            if entry is not None and (now - entry[0]) < ttl:
                return entry[1]
            event = self._inflight.get(key)
            if event is not None:
                # Someone else is already refreshing this key.
                if entry is not None:
                    return entry[1]  # serve stale while it refreshes
                leader = False
            else:
                event = threading.Event()
                self._inflight[key] = event
                leader = True

        if not leader:
            event.wait(self._INFLIGHT_WAIT_SEC)
            with self._lock:
                entry = self._data.get(key)
            if entry is not None:
                return entry[1]
            # Leader failed (exception) or timed out without caching — fall
            # through and compute uncoordinated; correctness over dedup.
            value = compute()
            if value is not None or cache_none:
                self.put(key, value)
            return value

        try:
            value = compute()
            if value is not None or cache_none:
                self.put(key, value)
            return value
        finally:
            with self._lock:
                self._inflight.pop(key, None)
            event.set()


# Shared instances, one per concern, so unrelated keys never collide and
# targeted invalidation stays simple.
capacity_scan_cache = TTLCache()   # "mini_lvols" / "mini_snapshots" -> list
leader_cache = TTLCache()          # (cluster_id, lvs_name) -> leader node id
no_leader_cache = TTLCache()       # (cluster_id, lvs_name) -> True (LVS confirmed leaderless)
quorum_verdict_cache = TTLCache()  # (node_id, lvs_peer_ids) -> bool (disconnected)

CAPACITY_SCAN_TTL_SEC = 10
LEADER_TTL_SEC = 8
# Negative verdict: a full find_leader_with_failover pass (scan + recovery)
# found NO confirmable leader. Object create/clone/snapshot requests against
# the LVS fail fast inside this window instead of re-running the probe/recovery
# machinery per request — a leaderless LVS under a mass-create workload
# otherwise probes every member several times per second for hours (run
# 20260712-231123: 61k bdev_lvol_get_lvstores per member). The TTL bounds how
# long a restored leader can go unnoticed, so keep it short-ish.
NO_LEADER_TTL_SEC = 15
QUORUM_VERDICT_TTL_SEC = 8


def cached_mini_lvols(db_controller, ttl: float = CAPACITY_SCAN_TTL_SEC):
    """TTL-cached ``get_mini_lvols()`` for advisory capacity math and vuid
    dedup on the create paths. Name uniqueness does NOT go through here — it
    uses the O(1) per-pool name index."""
    return capacity_scan_cache.get_or_compute(
        "mini_lvols", ttl, db_controller.get_mini_lvols)


def cached_mini_snapshots(db_controller, ttl: float = CAPACITY_SCAN_TTL_SEC):
    """TTL-cached ``get_mini_snapshots()`` — same tolerance as above.

    Minis, never full SnapShot records: a full record embeds the complete
    72-field LVol dict, so scanning the full table cost ~10ms/object in
    deserialization alone and reached 15s per refresh at 10k snapshots
    (mass-snapshot run 2026-07-21) — on every create request. The mini table
    is maintained transactionally on every SnapShot write and carries
    everything the advisory consumers need (lvol.node_id / lvol.pool_uuid,
    status, size, used_size, vuid)."""
    return capacity_scan_cache.get_or_compute(
        "mini_snapshots", ttl, db_controller.get_mini_snapshots)
