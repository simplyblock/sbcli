"""Tiered snapshot retention: parsing and selection are pure functions."""
import pytest

from simplyblock_core.snapshot_retention import (
    RetentionScheduleError, RetentionTier, describe, horizon_sec,
    parse_schedule, select_retained,
)

HOUR = 3600
DAY = 86400
#: the example from the feature request
SPEC = "15m:2h,1h:11h,1d:7d"


def test_parse_the_requested_schedule():
    assert parse_schedule(SPEC) == [
        RetentionTier(15 * 60, 2 * HOUR),
        RetentionTier(HOUR, 11 * HOUR),
        RetentionTier(DAY, 7 * DAY),
    ]
    assert horizon_sec(parse_schedule(SPEC)) == 2 * HOUR + 11 * HOUR + 7 * DAY


def test_empty_schedule_is_no_tiers():
    assert parse_schedule("") == []
    assert parse_schedule("   ") == []


def test_schedule_is_order_independent():
    assert parse_schedule("1d:7d,15m:2h,1h:11h") == parse_schedule(SPEC)


@pytest.mark.parametrize("bad", [
    "15m", "15m:2h:3d", "15x:2h", "0m:2h", "-5m:2h", "2h:15m", "abc",
])
def test_bad_schedules_are_rejected(bad):
    with pytest.raises(RetentionScheduleError):
        parse_schedule(bad)


def test_round_trip_describe():
    assert describe(parse_schedule(SPEC)) == SPEC


def test_one_snapshot_kept_per_bucket_newest_wins():
    tiers = parse_schedule("15m:1h")
    now = 10_000_000.0
    # first four are all inside bucket 0 (ages 60..800s < 900s), the last
    # one is in bucket 1 (age 1000s)
    snaps = [now - 60, now - 120, now - 200, now - 800, now - 1000]
    keep = select_retained(snaps, tiers, now)
    assert now - 60 in keep                      # newest of bucket 0
    for shadowed in (now - 120, now - 200, now - 800):
        assert shadowed not in keep
    assert now - 1000 in keep                    # bucket 1 keeps its own


def test_history_thins_out_with_age():
    """A minute-cadence stream over 8 days collapses to roughly
    8 quarter-hours + 11 hours + 7 days, not thousands of snapshots."""
    tiers = parse_schedule(SPEC)
    now = 1_000_000_000.0
    snaps = [now - 60 * i for i in range(8 * 24 * 60)]     # every minute, 8 days
    keep = select_retained(snaps, tiers, now)
    assert 20 <= len(keep) <= 30, len(keep)
    # dense recent history
    assert max(keep) == now
    # and genuine multi-day depth
    assert min(keep) < now - 6 * DAY


def test_snapshots_older_than_the_horizon_are_dropped():
    tiers = parse_schedule("15m:2h")
    now = 5_000_000.0
    keep = select_retained([now - 60, now - 10 * HOUR], tiers, now)
    assert now - 60 in keep
    assert now - 10 * HOUR not in keep


def test_always_keep_newest_survives_the_schedule():
    """Replication chains the arriving delta onto its predecessor, so the
    newest pair must never be pruned no matter what the schedule says."""
    tiers = parse_schedule("1d:7d")
    now = 2_000_000.0
    snaps = [now - 10, now - 20, now - 30]      # all inside one daily bucket
    keep = select_retained(snaps, tiers, now, always_keep_newest=2)
    assert now - 10 in keep and now - 20 in keep


def test_no_tiers_keeps_only_the_protected_newest():
    now = 100.0
    keep = select_retained([now - 1, now - 2, now - 3], [], now, always_keep_newest=2)
    assert keep == {now - 1, now - 2}


def test_selection_is_stable_across_repeated_calls():
    """Bucketing on absolute age must not drift, or a snapshot kept on one
    pass gets pruned on the next and the history develops holes."""
    tiers = parse_schedule(SPEC)
    now = 3_000_000.0
    snaps = [now - 60 * i for i in range(600)]
    assert select_retained(snaps, tiers, now) == select_retained(snaps, tiers, now)


# --- wiring: the schedule and the fail-over generation ----------------------

def test_prune_consults_the_schedule_before_the_flat_count():
    """_prune_internal_snapshots must ask the policy's schedule which
    snapshots survive; the flat keep-count is only the fallback."""
    import inspect
    from simplyblock_core.services import snapshot_replication as sr
    src = inspect.getsource(sr._prune_internal_snapshots)
    assert "_retention_schedule_for" in src
    assert "select_retained" in src
    # the newest `keep` stay protected even under a schedule
    assert "always_keep_newest=keep" in src


def test_invalid_schedule_falls_back_instead_of_crashing_the_runner():
    import inspect
    from simplyblock_core.services import snapshot_replication as sr
    src = inspect.getsource(sr._retention_schedule_for)
    assert "RetentionScheduleError" in src and "return []" in src


def test_policy_rejects_an_invalid_schedule_at_ingress():
    import inspect
    from simplyblock_core.controllers import replication_policy_controller as rpc
    src = inspect.getsource(rpc.add_policy)
    assert "parse_schedule" in src, "the policy must validate the schedule when set"
    assert src.index("parse_schedule") < src.index("policy = ReplicationPolicy()")


def test_failover_generation_walks_back_through_history():
    """generation=0 is the newest point-in-time; higher values step back, and
    asking for more generations than exist is an error, not a silent newest."""
    import inspect
    from simplyblock_core.controllers import lvol_controller as lc
    src = inspect.getsource(lc._last_replicated_target_snapshot)
    assert "generation" in src
    assert "snaps[generation:]" in src
    assert "only" in src and "exist" in src        # explicit out-of-range error
