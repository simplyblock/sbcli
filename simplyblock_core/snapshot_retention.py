"""Tiered retention for replication snapshots.

Replication used to keep a flat count of internal snapshots (the newest
``keep_replicated``), which gives no history: everything older than a couple
of cadence ticks is gone, so a fail-over can only ever land on "a minute
ago". A retention SCHEDULE keeps a thinning history instead -- dense for the
recent past, sparse further back -- so an operator can fail over to a chosen
point in time (yesterday, six hours ago) after a logical corruption that a
one-minute-old copy would have faithfully replicated.

Schedule syntax (compact, order-independent, parsed left to right):

    "15m:2h,1h:11h,1d:7d"

reads as "one snapshot every 15 minutes covering the last 2 hours, then one
every hour covering the next 11 hours, then one per day covering the next 7
days". Each tier is ``<every>:<for>``; both accept an integer with a unit
suffix s/m/h/d. A snapshot older than the sum of all tier spans is not
covered by the schedule and is pruned.

The selection is a pure function of (snapshot times, schedule, now), which
is what makes it testable without a cluster: see test_snapshot_retention.py.
"""
from __future__ import annotations

import re
from typing import NamedTuple
from collections.abc import Iterable, Sequence

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_TOKEN = re.compile(r"^(\d+)([smhd])$")


class RetentionTier(NamedTuple):
    """Keep one snapshot per ``every_sec`` bucket, covering ``span_sec``."""

    every_sec: int
    span_sec: int


class RetentionScheduleError(ValueError):
    """The schedule string could not be parsed."""


def _duration(token: str) -> int:
    m = _TOKEN.match(token.strip().lower())
    if not m:
        raise RetentionScheduleError(
            f"bad duration {token!r}: expected <int><s|m|h|d>, e.g. 15m or 7d")
    value, unit = int(m.group(1)), m.group(2)
    if value <= 0:
        raise RetentionScheduleError(f"duration must be positive: {token!r}")
    return value * _UNITS[unit]


def parse_schedule(spec: str) -> list[RetentionTier]:
    """Parse ``"15m:2h,1h:11h,1d:7d"`` into tiers. Empty string -> no tiers."""
    if not spec or not spec.strip():
        return []
    tiers: list[RetentionTier] = []
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if chunk.count(":") != 1:
            raise RetentionScheduleError(
                f"bad tier {chunk!r}: expected <every>:<for>, e.g. 15m:2h")
        every_s, span_s = chunk.split(":")
        every, span = _duration(every_s), _duration(span_s)
        if every > span:
            raise RetentionScheduleError(
                f"tier {chunk!r} keeps one snapshot every {every_s} but only "
                f"covers {span_s} -- the interval cannot exceed the span")
        tiers.append(RetentionTier(every, span))
    # Coarser tiers must come after finer ones; sorting makes the spec
    # order-independent rather than silently producing a nonsense ladder.
    tiers.sort(key=lambda t: t.every_sec)
    return tiers


def horizon_sec(tiers: Sequence[RetentionTier]) -> int:
    """Total age covered by the schedule; older snapshots are not retained."""
    return sum(t.span_sec for t in tiers)


def select_retained(created_ats: Iterable[float], tiers: Sequence[RetentionTier],
                    now: float, always_keep_newest: int = 0) -> set[float]:
    """Return the subset of ``created_ats`` the schedule retains.

    One snapshot per bucket per tier: the NEWEST in each bucket, so the
    retained point-in-time is as close as possible to the bucket boundary the
    operator asked for. Tiers apply to successive age ranges, finest first.

    ``always_keep_newest`` protects the N most recent regardless of the
    schedule. Replication needs that: deleting a snapshot swap-merges its
    segments into the successor chained to it, so the newest pair must
    survive or an arriving delta has nothing to chain onto.
    """
    times = sorted({float(t) for t in created_ats if t}, reverse=True)
    if not times:
        return set()

    keep: set[float] = set(times[:max(0, always_keep_newest)])
    if not tiers:
        return keep

    # Walk age ranges: tier i covers [range_start, range_start + span).
    range_start = 0.0
    for tier in tiers:
        range_end = range_start + tier.span_sec
        # Bucket by absolute age so bucket edges do not drift between calls.
        best_per_bucket: dict[int, float] = {}
        for t in times:
            age = now - t
            if age < range_start or age >= range_end:
                continue
            bucket = int(age // tier.every_sec)
            # times is newest-first, so the first hit in a bucket is newest.
            best_per_bucket.setdefault(bucket, t)
        keep.update(best_per_bucket.values())
        range_start = range_end
    return keep


def describe(tiers: Sequence[RetentionTier]) -> str:
    """Render tiers back to the compact spec (for display / round-trip)."""
    def fmt(seconds: int) -> str:
        for unit in ("d", "h", "m", "s"):
            size = _UNITS[unit]
            if seconds % size == 0:
                return f"{seconds // size}{unit}"
        return f"{seconds}s"
    return ",".join(f"{fmt(t.every_sec)}:{fmt(t.span_sec)}" for t in tiers)
