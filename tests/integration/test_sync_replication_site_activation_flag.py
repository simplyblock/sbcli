"""The site activation flag of a sync-replication cluster, against the real
FoundationDB: its writers (a token begun only for a recorded site that is not
draining, checked, stamped and ended by its holder, cleared by the watchdog),
the suspension, which never touches the flag and whose handling by the
monitor waits while the flag is set, and the watchdog that reclaims the flag
of an activation whose driver is gone.

3 + 3 nodes (``test_sync_replication_lvs_stack._sites``); the monitor pass is
the one of ``test_sync_replication_disaster.py``, its probes, shutdown thread
and ``cluster_activate`` patched at their boundaries.
"""
import threading
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor as monitor
from tests.integration import test_sync_replication_disaster as disaster
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
DRAINING, DOWN = Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN
FENCING, DONE = Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DONE

db = stack.db
rpcs = stack.rpcs
monitor_pass = disaster.monitor_pass
_record, _seed_record, _site = disaster._record, disaster._seed_record, disaster._site


def _ago(seconds):
    return (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()


def _cluster(db, state=DOWN, **fields):
    """A cluster with site A recorded ``state`` ("" = no record), stored DEGRADED unless ``fields`` say otherwise."""
    fields.setdefault("status", Cluster.STATUS_DEGRADED)
    return _seed_record(db, stack._seed_cluster(db), SITE_A if state else "", state, **fields)


def _with_flag(db, cluster, since, heartbeat=None, site=SITE_A):
    """``cluster`` with a site activation of ``site`` begun at ``since``, its heartbeat at ``heartbeat``."""
    return _seed_record(db, cluster, cluster.lost_site, cluster.lost_site_state, site_activation_site=site,
                        site_activation_since=since,
                        site_activation_heartbeat=since if heartbeat is None else heartbeat)


def _flag(db, cluster_id):
    c = db.get_cluster_by_id(cluster_id)
    return c.site_activation_site, c.site_activation_since, c.site_activation_heartbeat


NO_FLAG = ("", "", "")


@pytest.fixture()
def registry(monkeypatch):
    """This process's registry of running site activations, empty."""
    threads: dict = {}
    monkeypatch.setattr(monitor, "_site_activation_threads", threads)
    return threads


# ---------------------------------------------------------------------------
# the writers
# ---------------------------------------------------------------------------

class TestBeginSiteActivation:

    @pytest.mark.parametrize("state", [DOWN, FENCING, DONE])
    def test_begin_sets_the_flag_of_the_recorded_site(self, db, state):
        cluster = _cluster(db, state)
        token = src.begin_site_activation(cluster.get_id(), SITE_A)
        assert token
        datetime.fromisoformat(token)
        assert _flag(db, cluster.get_id()) == (SITE_A, token, token)
        assert _record(db, cluster.get_id()) == (SITE_A, state)

    @pytest.mark.parametrize("state, site", [(DRAINING, SITE_A), (DOWN, SITE_B), ("", SITE_A)])
    def test_begin_refuses_a_site_not_ready_to_return(self, db, state, site):
        cluster = _cluster(db, state)
        assert src.begin_site_activation(cluster.get_id(), site) == ""
        assert _flag(db, cluster.get_id()) == NO_FLAG

    def test_begin_refuses_while_a_flag_is_set(self, db):
        cluster = _cluster(db, DONE)
        first = src.begin_site_activation(cluster.get_id(), SITE_A)
        assert src.begin_site_activation(cluster.get_id(), SITE_A) == ""
        assert _flag(db, cluster.get_id()) == (SITE_A, first, first)

    def test_begin_refuses_without_sync_replication(self, db):
        cluster = _cluster(db, DOWN, sync_replication=False)
        assert src.begin_site_activation(cluster.get_id(), SITE_A) == ""
        assert _flag(db, cluster.get_id()) == NO_FLAG

    def test_a_concurrent_begin_wins_and_the_loser_refuses(self, db):
        # Another process begins between this begin's transactional read and
        # its commit (from the mutation, the first time it runs): the
        # transaction conflicts and replays on the winner's record, so this
        # begin returns "" and the winner's token and another flow's field stay.
        cluster = _cluster(db, DONE)
        real, seen = src._begin_site_activation_flag, []

        def _concurrent(c, *args):
            seen.append(c.site_activation_since)
            if len(seen) == 1:
                other = db.get_cluster_by_id(c.get_id())
                other.site_activation_site = SITE_A
                other.site_activation_since = other.site_activation_heartbeat = "winner"
                other.is_re_balancing = True
                other.write_to_db(db.kv_store)
            return real(c, *args)

        with patch.object(src, "_begin_site_activation_flag", side_effect=_concurrent):
            assert src.begin_site_activation(cluster.get_id(), SITE_A) == ""
        assert len(seen) >= 2 and seen[0] == "" and seen[-1] == "winner"
        assert _flag(db, cluster.get_id()) == (SITE_A, "winner", "winner")
        assert db.get_cluster_by_id(cluster.get_id()).is_re_balancing is True


    @pytest.mark.parametrize("write, mutation", [
        (lambda cid: src.begin_site_activation(cid, SITE_A), "_begin_site_activation_flag"),
        (lambda cid: src.clear_site_activation(cid), "_drop_site_activation_flag"),
    ])
    def test_cluster_removed_mid_write_reports_nothing(self, db, write, mutation):
        # The cluster record is removed between the writer's transactional
        # read and its commit: the retry finds no record and runs no mutation,
        # so the first run's result is never reported.
        cluster = _cluster(db, DONE)
        if mutation == "_drop_site_activation_flag":
            src.begin_site_activation(cluster.get_id(), SITE_A)
        real, calls = getattr(src, mutation), []

        def _removed(c, *args):
            calls.append(c.get_id())
            if len(calls) == 1:
                db.get_cluster_by_id(c.get_id()).remove(db.kv_store)
            return real(c, *args)

        with patch.object(src, mutation, side_effect=_removed):
            assert write(cluster.get_id()) == ""
        assert len(calls) == 1
        with pytest.raises(KeyError):
            db.get_cluster_by_id(cluster.get_id())


class TestSiteActivationOwned:

    @pytest.mark.parametrize("status", [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY])
    def test_the_holder_under_a_serving_status_passes(self, db, status):
        cluster = _cluster(db, DOWN, status=status)
        token = src.begin_site_activation(cluster.get_id(), SITE_A)
        src.site_activation_owned(cluster.get_id(), token)

    @pytest.mark.parametrize("drop", [
        lambda cid, token: src.end_site_activation(cid, token),
        lambda cid, token: src.clear_site_activation(cid),
    ])
    def test_a_token_gone_is_cancelled(self, db, drop):
        cluster = _cluster(db, DOWN)
        token = src.begin_site_activation(cluster.get_id(), SITE_A)
        drop(cluster.get_id(), token)
        with pytest.raises(src.SiteActivationCancelled):
            src.site_activation_owned(cluster.get_id(), token)

    @pytest.mark.parametrize("token", ["", "another"])
    def test_another_token_is_cancelled(self, db, token):
        cluster = _cluster(db, DOWN)
        src.begin_site_activation(cluster.get_id(), SITE_A)
        with pytest.raises(src.SiteActivationCancelled):
            src.site_activation_owned(cluster.get_id(), token)

    @pytest.mark.parametrize("status", [Cluster.STATUS_SUSPENDED, Cluster.STATUS_IN_ACTIVATION])
    def test_a_cluster_not_serving_is_cancelled(self, db, status):
        cluster = _cluster(db, DONE)
        token = src.begin_site_activation(cluster.get_id(), SITE_A)
        _seed_record(db, cluster, SITE_A, DONE, status=status)
        with pytest.raises(src.SiteActivationCancelled):
            src.site_activation_owned(cluster.get_id(), token)
        assert _flag(db, cluster.get_id()) == (SITE_A, token, token)


class TestEndClearStamp:

    @pytest.mark.parametrize("token", ["", "another"])
    def test_end_leaves_another_token(self, db, token):
        cluster = _cluster(db, DOWN)
        own = src.begin_site_activation(cluster.get_id(), SITE_A)
        src.end_site_activation(cluster.get_id(), token)
        assert _flag(db, cluster.get_id()) == (SITE_A, own, own)

    def test_end_clears_its_own_token(self, db):
        cluster = _cluster(db, DOWN)
        src.end_site_activation(cluster.get_id(), src.begin_site_activation(cluster.get_id(), SITE_A))
        assert _flag(db, cluster.get_id()) == NO_FLAG
        assert _record(db, cluster.get_id()) == (SITE_A, DOWN)

    def test_clear_clears_any_token(self, db):
        cluster = _cluster(db, DONE)
        src.begin_site_activation(cluster.get_id(), SITE_A)
        assert src.clear_site_activation(cluster.get_id()) == SITE_A
        assert _flag(db, cluster.get_id()) == NO_FLAG
        assert _record(db, cluster.get_id()) == (SITE_A, DONE)

    def test_clear_without_a_flag_names_no_site(self, db):
        cluster = _cluster(db, DOWN)
        assert src.clear_site_activation(cluster.get_id()) == ""
        assert _flag(db, cluster.get_id()) == NO_FLAG

    def test_stamp_advances_its_own_heartbeat(self, db):
        cluster = _with_flag(db, _cluster(db, DOWN), since=_ago(120))
        token = cluster.site_activation_since
        src.stamp_site_activation(cluster.get_id(), token)
        site, since, heartbeat = _flag(db, cluster.get_id())
        assert (site, since) == (SITE_A, token)
        assert datetime.fromisoformat(heartbeat) > datetime.fromisoformat(token)

    @pytest.mark.parametrize("token", ["", "another"])
    def test_stamp_leaves_another_token(self, db, token):
        cluster = _with_flag(db, _cluster(db, DOWN), since=_ago(120))
        before = _flag(db, cluster.get_id())
        src.stamp_site_activation(cluster.get_id(), token)
        assert _flag(db, cluster.get_id()) == before

    def test_stamp_does_not_resurrect_a_cleared_flag(self, db):
        cluster = _cluster(db, DOWN)
        token = src.begin_site_activation(cluster.get_id(), SITE_A)
        src.clear_site_activation(cluster.get_id())
        src.stamp_site_activation(cluster.get_id(), token)
        assert _flag(db, cluster.get_id()) == NO_FLAG

    @pytest.mark.parametrize("write, mutation, flag_after", [
        (lambda cid, token: src.end_site_activation(cid, token), "_drop_site_activation_flag", False),
        (lambda cid, token: src.clear_site_activation(cid), "_drop_site_activation_flag", False),
        (lambda cid, token: src.stamp_site_activation(cid, token), "_stamp_site_activation_flag", True),
    ])
    def test_a_field_written_concurrently_survives(self, db, write, mutation, flag_after):
        # Another flow commits a change of the cluster between the writer's
        # transactional read and its commit: the writer's mutation replays on
        # the new record, so the other flow's field survives.
        cluster = _cluster(db, DONE)
        token = src.begin_site_activation(cluster.get_id(), SITE_A)
        real, seen = getattr(src, mutation), []

        def _concurrent(c, *args):
            seen.append(c.is_re_balancing)
            if len(seen) == 1:
                other = db.get_cluster_by_id(c.get_id())
                other.is_re_balancing = True
                other.write_to_db(db.kv_store)
            return real(c, *args)

        with patch.object(src, mutation, side_effect=_concurrent):
            write(cluster.get_id(), token)
        assert len(seen) >= 2 and seen[0] is False and seen[-1] is True
        assert db.get_cluster_by_id(cluster.get_id()).is_re_balancing is True
        assert (_flag(db, cluster.get_id())[1] == token) is flag_after

    def test_the_record_clear_leaves_the_flag(self, db):
        cluster = _cluster(db, DONE)
        token = src.begin_site_activation(cluster.get_id(), SITE_A)
        assert db.clear_lost_site(cluster.get_id(), SITE_A, (DONE,), [], lambda _c, _nodes: []) == []
        assert _record(db, cluster.get_id()) == ("", "")
        assert _flag(db, cluster.get_id()) == (SITE_A, token, token)


# ---------------------------------------------------------------------------
# the suspension
# ---------------------------------------------------------------------------

class TestSuspensionKeepsTheFlag:

    @pytest.mark.parametrize("state, record_after", [(DOWN, ("", "")), (FENCING, (SITE_A, FENCING)),
                                                     (DONE, (SITE_A, DONE))])
    def test_the_record_clear_under_suspension_leaves_it(self, db, state, record_after):
        cluster = _with_flag(db, _cluster(db, state, status=Cluster.STATUS_SUSPENDED), since=_ago(10))
        before = _flag(db, cluster.get_id())
        monitor._clear_site_record_under_suspension(cluster.get_id())
        assert _record(db, cluster.get_id()) == record_after
        assert _flag(db, cluster.get_id()) == before

    @pytest.mark.parametrize("state", [DOWN, FENCING, DONE])
    def test_a_monitor_suspension_leaves_it(self, db, state):
        cluster = _with_flag(db, _cluster(db, state), since=_ago(10))
        before = _flag(db, cluster.get_id())
        monitor._set_cluster_status(db.get_cluster_by_id(cluster.get_id()), Cluster.STATUS_SUSPENDED)
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_SUSPENDED
        assert _flag(db, cluster.get_id()) == before


def _suspended_with_flag(db, **fields):
    """Stored SUSPENDED, not yet drained, every node ONLINE, record ``down`` (the
    pass clears it first) and a live site activation of A."""
    now = _ago(0)
    cluster = _cluster(db, DOWN, status=Cluster.STATUS_SUSPENDED, suspend_drain_complete=False,
                       site_activation_site=SITE_A, site_activation_since=now, site_activation_heartbeat=now,
                       **fields)
    _site(db, cluster)
    return cluster


class TestSuspensionWaitsForTheFlag:

    def test_suspension_waits_for_site_activation(self, db, monitor_pass):
        cluster = _suspended_with_flag(db)
        flag = _flag(db, cluster.get_id())
        after = monitor_pass.run(cluster)
        monitor_pass.shutdown.assert_not_called()
        monitor_pass.activate.assert_not_called()
        assert monitor_pass.status_writes == []
        assert after.status == Cluster.STATUS_SUSPENDED
        assert _flag(db, cluster.get_id()) == flag

    def test_suspension_handled_once_flag_is_gone(self, db, monitor_pass):
        cluster = _suspended_with_flag(db)
        monitor_pass.run(cluster)
        src.clear_site_activation(cluster.get_id())
        monitor_pass.run(cluster)
        assert monitor_pass.shutdown.call_count == 6

    def test_non_sync_suspension_is_handled(self, db, monitor_pass):
        cluster = _suspended_with_flag(db, sync_replication=False)
        monitor_pass.run(cluster)
        assert monitor_pass.shutdown.call_count == 6
        monitor_pass.activate.assert_called_once_with(cluster.get_id(), force=True)


# ---------------------------------------------------------------------------
# the watchdog
# ---------------------------------------------------------------------------

STALE = monitor.ACTIVATION_HEARTBEAT_STALE_SEC
PAST_BUDGET = 2 * 24 * 3600


def _watch(db, cluster):
    monitor._watchdog_stuck_site_activation(db.get_cluster_by_id(cluster.get_id()))


class TestSiteActivationWatchdog:

    def test_a_stale_heartbeat_clears_the_flag(self, db, registry):
        cluster = _with_flag(db, _cluster(db, DONE), since=_ago(STALE + 60), heartbeat=_ago(STALE + 5))
        _site(db, cluster)
        _watch(db, cluster)
        assert _flag(db, cluster.get_id()) == NO_FLAG
        assert _record(db, cluster.get_id()) == (SITE_A, DONE)

    def test_a_fresh_heartbeat_within_budget_keeps_it(self, db, registry):
        cluster = _with_flag(db, _cluster(db, DOWN), since=_ago(STALE + 60), heartbeat=_ago(30))
        _site(db, cluster)
        before = _flag(db, cluster.get_id())
        _watch(db, cluster)
        assert _flag(db, cluster.get_id()) == before

    def test_past_budget_with_no_thread_clears_it(self, db, registry):
        cluster = _with_flag(db, _cluster(db, DOWN), since=_ago(PAST_BUDGET), heartbeat=_ago(30))
        _site(db, cluster)
        _watch(db, cluster)
        assert _flag(db, cluster.get_id()) == NO_FLAG
        assert _record(db, cluster.get_id()) == (SITE_A, DOWN)

    def test_past_budget_with_a_dead_thread_clears_it(self, db, registry):
        cluster = _with_flag(db, _cluster(db, DOWN), since=_ago(PAST_BUDGET), heartbeat=_ago(30))
        _site(db, cluster)
        dead = threading.Thread(target=lambda: None)
        dead.start()
        dead.join()
        registry[cluster.get_id()] = dead
        _watch(db, cluster)
        assert _flag(db, cluster.get_id()) == NO_FLAG
        assert cluster.get_id() not in registry

    def test_past_budget_with_a_live_thread_keeps_it(self, db, registry):
        cluster = _with_flag(db, _cluster(db, DOWN), since=_ago(PAST_BUDGET), heartbeat=_ago(30))
        _site(db, cluster)
        before = _flag(db, cluster.get_id())
        release = threading.Event()
        live = threading.Thread(target=release.wait, daemon=True)
        live.start()
        registry[cluster.get_id()] = live
        try:
            _watch(db, cluster)
            assert _flag(db, cluster.get_id()) == before
            assert registry[cluster.get_id()] is live
        finally:
            release.set()
            live.join()

    def test_non_sync_cluster_is_untouched(self, db, registry):
        cluster = _with_flag(db, _cluster(db, "", sync_replication=False), since=_ago(PAST_BUDGET),
                             heartbeat=_ago(PAST_BUDGET))
        before = _flag(db, cluster.get_id())
        _watch(db, cluster)
        assert _flag(db, cluster.get_id()) == before

    def test_the_monitor_pass_runs_the_watchdog(self, db, monitor_pass, registry):
        cluster = _with_flag(db, _cluster(db, DOWN), since=_ago(STALE + 60), heartbeat=_ago(STALE + 5))
        _site(db, cluster, a_status=StorageNode.STATUS_OFFLINE)
        after = monitor_pass.run(cluster)
        assert _flag(db, cluster.get_id()) == NO_FLAG
        assert (after.lost_site, after.lost_site_state, after.status) == (SITE_A, DOWN, Cluster.STATUS_DEGRADED)
