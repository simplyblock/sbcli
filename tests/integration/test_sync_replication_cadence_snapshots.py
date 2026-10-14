"""The timer of the scheduled snapshots (snapshot_monitor.take_due_internal_snapshots:
the interval snapshot of a replicated volume, the group snapshot of a
consistency group) on a sync-replication cluster, against the real
FoundationDB. An LVS has no leader between a promote and the first client IO,
and the timer must not make one: a due volume or group on such an LVS is
deferred - nothing is granted, nothing is snapshotted - and taken on the first
tick after a member of the active triplet reports the leadership. The verdict
"nobody leads" is reused within a tick for the same LVS and marker only, a
leader found never is. A led LVS, and every LVS of a cluster without sync
replication, is snapshotted on its due tick.
The snapshot itself is the real one (snapshot_controller.add,
consistency_group_controller.create_group_snapshot_for_group), so a tick that
is not deferred reaches the leaderless recovery those run. Storage nodes (RPC)
and the port-block / hublvol side effects are mocked; the database, its locks
and the task queue never are.
"""
import itertools
import logging
import time
import uuid
from unittest.mock import ANY, patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import consistency_group_controller, snapshot_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_monitor
from tests.integration import test_sync_replication_active_triplet as triplet
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
_update, _fresh = stack._update, stack._fresh

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
env = stack.env
no_sleep = triplet.no_sleep

#: What makes a leader: the reload and the grant of the leaderless recovery, the forced hand-over.
_GRANT_RPCS = ("bdev_lvol_update_lvstore", "bdev_lvol_set_leader", "bdev_lvol_set_lvs_signal")
#: What creates a snapshot on a leader (one volume, a group).
_CREATE_RPCS = ("lvol_create_snapshot2", "lvol_create_snapshot", "bdev_lvol_snapshot_group")

_blobids = itertools.count(1000)


@pytest.fixture(autouse=True)
def _clean_warn_memo():
    snapshot_monitor._leaderless_warn_memo.clear()
    yield
    snapshot_monitor._leaderless_warn_memo.clear()


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------

def _layout(db, *, active_site=""):
    """a0 owns LVS_1: home triplet a0 a1 a2, remote triplet b0 b1 b2. One pool."""
    cluster, a, b, owner = triplet._layout(db, active_site=active_site)
    return cluster, a, b, owner, publish._pool(db, cluster)


def _due_volume(db, cluster, owner, pool):
    """A volume on ``owner``'s LVS that replicates on a one-minute interval
    and has no internal snapshot yet: due on the next tick."""
    lvol = publish._volume(db, cluster, owner, pool, active_site=owner.site)
    lvol.do_replicate = True
    lvol.replication_interval_min = 1
    lvol.write_to_db(db.kv_store)
    return lvol


def _group(db, cluster, owner, members):
    """A consistency group pinned to ``owner``'s LVS with ``members``."""
    group = ConsistencyGroup()
    group.uuid = str(uuid.uuid4())
    group.cluster_id = cluster.get_id()
    group.group_name = f"cg-{group.uuid[:8]}"
    group.node_id = owner.get_id()
    group.lvs_name = owner.lvstore
    group.members = {lvol.get_id(): {"joined_seq": 1, "removed_seq": 0} for lvol in members}
    group.write_to_db(db.kv_store)
    for lvol in members:
        lvol.group_id = group.get_id()
        lvol.write_to_db(db.kv_store)
    return group


def _read_back(name=None, **kwargs):
    """get_bdevs of a node: a snapshot it was asked for exists, nothing else."""
    if not name or "/SNAP_" not in name:
        return []
    return [{"uuid": str(uuid.uuid4()),
             "driver_specific": {"lvol": {"blobid": next(_blobids), "num_allocated_clusters": 1}}}]


def _can_snapshot(rpcs, nodes):
    """Every node of ``nodes`` takes the snapshot it is asked for - whoever
    the tick ends up asking."""
    for node in nodes:
        rpc = rpcs[node.get_id()]
        rpc.lvol_create_snapshot2.return_value = (True, None)
        rpc.get_bdevs.side_effect = _read_back


def _leaderless(db, rpcs):
    """LVS_1 is led from site B and nobody leads it. The recovery WOULD make
    b0 its leader: its journal is ready and a grant on it takes."""
    cluster, a, b, owner, pool = _layout(db, active_site=SITE_B)
    triplet._journal_ready(rpcs, b[0])
    stack._grant_takes(rpcs, b[0])
    _can_snapshot(rpcs, [*a, *b])
    return cluster, a, b, owner, pool


def _tick(cluster):
    snapshot_monitor.take_due_internal_snapshots(cluster.get_id(), int(time.time()))


# ---------------------------------------------------------------------------
# what a tick did
# ---------------------------------------------------------------------------

def _not_sent(rpcs, nodes, methods):
    for method in methods:
        for node in nodes:
            getattr(rpcs[node.get_id()], method).assert_not_called()


def _nothing_made(db, rpcs, cluster, nodes):
    """The tick made neither a leader nor a snapshot: no grant and no snapshot
    RPC on any node, no record, no task, and the leaderless recovery did not
    even start (it takes this lock first and never releases it)."""
    _not_sent(rpcs, nodes, (*_GRANT_RPCS, *_CREATE_RPCS, "bdev_lvol_snapshot_register"))
    assert db.get_snapshots(cluster.get_id()) == []
    assert db.get_job_tasks(cluster.get_id()) == []
    assert db.acquire_lvstore_lock(cluster.get_id(), "takeleader/LVS_1", "test") == (True, None)


def _takers(rpcs, nodes):
    """Node ids that created a snapshot, one entry per RPC."""
    return [node.get_id() for node in nodes for method in _CREATE_RPCS
            for _ in getattr(rpcs[node.get_id()], method).call_args_list]


def _deferrals(caplog, lvs_name="LVS_1"):
    """The timer's own "deferred for a leaderless LVS" lines. The lookup
    behind it (storage_node_ops) logs a line of its own per pass."""
    return [r.getMessage() for r in caplog.records
            if r.name == snapshot_monitor.logger.name
            and "scheduled snapshots are deferred" in r.getMessage() and lvs_name in r.getMessage()]


def _snapshots_of(db, cluster, lvol):
    return [s for s in db.get_snapshots(cluster.get_id()) if s.lvol.get_id() == lvol.get_id()]


# ---------------------------------------------------------------------------
# a due volume on an LVS nobody leads
# ---------------------------------------------------------------------------

class TestLeaderlessVolume:

    def test_a_due_volume_is_deferred_and_nothing_is_granted(self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner, pool = _leaderless(db, rpcs)
        _due_volume(db, cluster, owner, pool)

        _tick(cluster)

        _nothing_made(db, rpcs, cluster, [*a, *b])
        # it asked the active triplet, and only it
        for node in b:
            rpcs[node.get_id()].bdev_lvol_get_lvstores.assert_called_with("LVS_1")
        for node in a:
            rpcs[node.get_id()].bdev_lvol_get_lvstores.assert_not_called()

    def test_deferrals_log_once_and_look_up_once_per_tick(
            self, db, rpcs, hub, env, no_sleep, caplog):
        cluster, a, b, owner, pool = _leaderless(db, rpcs)
        _due_volume(db, cluster, owner, pool)
        _due_volume(db, cluster, owner, pool)
        caplog.set_level(logging.INFO)
        probes = rpcs[b[1].get_id()].bdev_lvol_get_lvstores   # asked once per lookup

        _tick(cluster)
        # the second volume reused "nobody leads" ...
        assert probes.call_count == 1
        _tick(cluster)
        # ... which the next tick did not inherit
        assert probes.call_count == 2

        _nothing_made(db, rpcs, cluster, [*a, *b])
        # four deferred snapshots, one line (rate-limited per LVS)
        assert len(_deferrals(caplog)) == 1

    def test_the_snapshot_is_taken_once_the_primary_leads(
            self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner, pool = _leaderless(db, rpcs)
        lvol = _due_volume(db, cluster, owner, pool)
        _tick(cluster)
        assert db.get_snapshots(cluster.get_id()) == []

        rpcs.lead(b[0])                                   # the first client IO arrived
        _tick(cluster)

        [snap] = db.get_snapshots(cluster.get_id())
        assert snap.lvol.get_id() == lvol.get_id()
        assert snap.snap_type == SnapShot.TYPE_INTERNAL
        assert _takers(rpcs, [*a, *b]) == [b[0].get_id()]
        rpcs[b[0].get_id()].lvol_create_snapshot2.assert_called_once_with(f"LVS_1/{lvol.lvol_bdev}", ANY)
        # its creation queued the replication, as on any tick
        assert [(t.function_name, t.function_params["snapshot_id"])
                for t in db.get_job_tasks(cluster.get_id())] == [
            (JobSchedule.FN_SNAPSHOT_REPLICATION, snap.get_id())]
        _not_sent(rpcs, [*a, *b], _GRANT_RPCS)


# ---------------------------------------------------------------------------
# a due consistency group whose pinned LVS nobody leads
# ---------------------------------------------------------------------------

class TestLeaderlessGroup:

    def _grouped(self, db, rpcs):
        cluster, a, b, owner, pool = _leaderless(db, rpcs)
        members = [_due_volume(db, cluster, owner, pool) for _ in range(2)]
        return cluster, a, b, members, _group(db, cluster, owner, members)

    def test_the_group_is_deferred_as_a_whole(self, db, rpcs, hub, env, no_sleep, caplog):
        cluster, a, b, _, group = self._grouped(db, rpcs)
        caplog.set_level(logging.INFO)

        _tick(cluster)

        _nothing_made(db, rpcs, cluster, [*a, *b])
        assert db.get_consistency_group_by_id(group.get_id()).last_group_seq == 0
        assert len(_deferrals(caplog)) == 1

    def test_the_generation_is_taken_once_the_primary_leads(
            self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, members, group = self._grouped(db, rpcs)
        _tick(cluster)
        assert db.get_snapshots(cluster.get_id()) == []

        rpcs.lead(b[0])
        _tick(cluster)

        # ONE group RPC on the leader carrying both members, never one per volume
        assert _takers(rpcs, [*a, *b]) == [b[0].get_id()]
        [taken] = rpcs[b[0].get_id()].bdev_lvol_snapshot_group.call_args_list
        assert taken.args[0] == "LVS_1"
        assert {entry["lvol_name"] for entry in taken.args[1]} == {
            f"LVS_1/{lvol.lvol_bdev}" for lvol in members}
        snaps = db.get_snapshots(cluster.get_id())
        assert sorted(s.lvol.get_id() for s in snaps) == sorted(lvol.get_id() for lvol in members)
        assert {(s.group_id, s.group_seq, s.snap_type) for s in snaps} == {
            (group.get_id(), 1, SnapShot.TYPE_INTERNAL)}
        assert db.get_consistency_group_by_id(group.get_id()).last_group_seq == 1
        _not_sent(rpcs, [*a, *b], _GRANT_RPCS)


# ---------------------------------------------------------------------------
# the active site changes between two volumes of one tick
# ---------------------------------------------------------------------------

class TestMidTick:

    def test_a_mid_tick_promote_defers_the_second_volume(
            self, db, rpcs, hub, env, no_sleep):
        # A leader found for one volume says nothing about the next one.
        cluster, a, b, owner, pool = _layout(db)
        rpcs.lead(a[0])
        triplet._journal_ready(rpcs, b[0])
        stack._grant_takes(rpcs, b[0])
        _can_snapshot(rpcs, [*a, *b])
        lvols = [_due_volume(db, cluster, owner, pool) for _ in range(2)]

        def _snapshot_then_promote(*args, **kwargs):
            # the promote ends right behind the first snapshot: a0 is demoted,
            # the LVS is open on site B and waits for its first IO there
            rpcs[a[0].get_id()].bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
            _update(db, owner, lvs_active_site=SITE_B)
            return True, None
        rpcs[a[0].get_id()].lvol_create_snapshot2.side_effect = _snapshot_then_promote

        _tick(cluster)

        _not_sent(rpcs, [*a, *b], _GRANT_RPCS)
        assert _takers(rpcs, [*a, *b]) == [a[0].get_id()]
        assert sorted(len(_snapshots_of(db, cluster, lvol)) for lvol in lvols) == [0, 1]
        assert db.acquire_lvstore_lock(cluster.get_id(), "takeleader/LVS_1", "test") == (True, None)

    def test_a_mid_tick_move_back_snapshots_the_second_volume(
            self, db, rpcs, hub, env, no_sleep):
        # "Nobody leads" is a verdict about one marker: it does not outlive it.
        cluster, a, b, owner, pool = _leaderless(db, rpcs)
        lvols = [_due_volume(db, cluster, owner, pool) for _ in range(2)]
        moved = []

        def _probed_then_moved_back(*args, **kwargs):
            # b2 is the last member the first lookup asks: while it answers,
            # the LVS goes back to site A and a0 leads it
            if not moved:
                moved.append(True)
                _update(db, owner, lvs_active_site=SITE_A)
                rpcs.lead(a[0])
            return [{"lvs leadership": False}]
        rpcs[b[2].get_id()].bdev_lvol_get_lvstores.side_effect = _probed_then_moved_back

        _tick(cluster)

        # the first volume was looked up on site B and deferred, the second on site A
        assert _takers(rpcs, [*a, *b]) == [a[0].get_id()]
        assert sorted(len(_snapshots_of(db, cluster, lvol)) for lvol in lvols) == [0, 1]
        _not_sent(rpcs, [*a, *b], _GRANT_RPCS)


# ---------------------------------------------------------------------------
# guards: what the timer must not change
# ---------------------------------------------------------------------------

def _plain_layout(db):
    """A cluster WITHOUT sync replication: n0 owns LVS_1, n1 / n2 are its
    secondary and tertiary, no node has a site. Nobody reports the leadership."""
    cluster = stack._seed_cluster(db)
    cluster.sync_replication = False
    cluster.write_to_db(db.kv_store)
    nodes = [stack._seed_node(db, cluster, f"n{i}", "") for i in range(3)]
    owner = _update(db, nodes[0], secondary_node_id=nodes[1].get_id(), tertiary_node_id=nodes[2].get_id(),
                    lvstore="LVS_1", lvstore_status="ready")
    return cluster, owner, publish._pool(db, cluster)


class TestGuards:

    @pytest.mark.parametrize("active_site, site, slot", [
        pytest.param("", "a", 0, id="home-led"),
        pytest.param(SITE_B, "b", 1, id="remote-led-by-its-secondary"),
    ])
    def test_led_sync_lvs_snapshots_without_a_grant(self, db, rpcs, hub, env, caplog, active_site, site, slot):
        cluster, a, b, owner, pool = _layout(db, active_site=active_site)
        leader = {"a": a, "b": b}[site][slot]
        rpcs.lead(leader)
        _can_snapshot(rpcs, [*a, *b])
        lvol = _due_volume(db, cluster, owner, pool)
        caplog.set_level(logging.INFO)

        _tick(cluster)

        [snap] = db.get_snapshots(cluster.get_id())
        assert (snap.lvol.get_id(), snap.snap_type) == (lvol.get_id(), SnapShot.TYPE_INTERNAL)
        assert _takers(rpcs, [*a, *b]) == [leader.get_id()]
        _not_sent(rpcs, [*a, *b], _GRANT_RPCS)
        assert _deferrals(caplog) == []

    def test_a_leaderless_lvs_does_not_hold_back_another_lvs(self, db, rpcs, hub, env, no_sleep):
        # LVS_1 (owner a0) is open on site B with nobody leading; LVS_2 (owner
        # b0) is led by b0. Both are probed on the same three nodes.
        cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db)
        owner_a = _update(db, owner_a, lvs_active_site=SITE_B)
        rpcs[b[0].get_id()].bdev_lvol_get_lvstores.side_effect = \
            lambda name=None, **kwargs: [{"lvs leadership": name == "LVS_2"}]
        triplet._journal_ready(rpcs, b[0])
        _can_snapshot(rpcs, [*a, *b])
        pool = publish._pool(db, cluster)
        waiting = _due_volume(db, cluster, owner_a, pool)
        led = _due_volume(db, cluster, owner_b, pool)

        _tick(cluster)

        assert _snapshots_of(db, cluster, waiting) == []
        assert len(_snapshots_of(db, cluster, led)) == 1
        assert _takers(rpcs, [*a, *b]) == [b[0].get_id()]
        rpcs[b[0].get_id()].lvol_create_snapshot2.assert_called_once_with(f"LVS_2/{led.lvol_bdev}", ANY)
        _not_sent(rpcs, [*a, *b], _GRANT_RPCS)

    def test_a_lookup_that_fails_skips_that_volume_only(self, db, rpcs, hub, env, caplog):
        cluster, a, b, owner, pool = _layout(db)
        rpcs.lead(a[0])
        _can_snapshot(rpcs, [*a, *b])
        lvols = [_due_volume(db, cluster, owner, pool) for _ in range(2)]
        caplog.set_level(logging.INFO)
        real, lookups = ops.find_leader_with_failover, []

        def _first_lookup_fails(all_nodes, lvs_name, **kwargs):
            lookups.append(kwargs)
            if len(lookups) == 1:
                raise RuntimeError("leader lookup broke")
            return real(all_nodes, lvs_name, **kwargs)

        with patch.object(ops, "find_leader_with_failover", side_effect=_first_lookup_fails):
            _tick(cluster)

        # the timer's two lookups, both without recovery; the snapshot's own found a0 at once
        assert lookups == [{"recover": False}, {"recover": False}]
        [skipped] = [lvol for lvol in lvols if not _snapshots_of(db, cluster, lvol)]
        assert len(db.get_snapshots(cluster.get_id())) == 1
        errors = [r.getMessage() for r in caplog.records
                  if r.name == snapshot_monitor.logger.name and r.levelno == logging.ERROR]
        assert errors == [f"Internal snapshot scheduling failed for {skipped.get_id()}: leader lookup broke"]
        # a lookup that failed is not "nobody leads"
        assert _deferrals(caplog) == []
        _not_sent(rpcs, [*a, *b], _GRANT_RPCS)

    def test_a_non_sync_volume_skips_the_leader_lookup(self, db, rpcs, caplog):
        cluster, owner, pool = _plain_layout(db)
        lvol = _due_volume(db, cluster, owner, pool)
        publish._volume(db, cluster, owner, pool, active_site="")   # does not replicate
        caplog.set_level(logging.INFO)

        with patch.object(snapshot_controller, "add", return_value=("snap-1", None)) as add:
            _tick(cluster)

        # today's path: straight into the snapshot, which does its own lookup
        add.assert_called_once_with(lvol.get_id(), ANY, snap_type=SnapShot.TYPE_INTERNAL)
        assert add.call_args.args[1].startswith(f"repl_internal_{lvol.get_id()[:8]}_")
        assert rpcs == {}                                 # the timer asked no node anything
        assert _deferrals(caplog) == []

    def test_a_non_sync_group_skips_the_leader_lookup(self, db, rpcs, caplog):
        cluster, owner, pool = _plain_layout(db)
        group = _group(db, cluster, owner, [_due_volume(db, cluster, owner, pool) for _ in range(2)])
        caplog.set_level(logging.INFO)

        with patch.object(consistency_group_controller, "create_group_snapshot_for_group",
                          return_value=(["snap-1", "snap-2"], None)) as take, \
                patch.object(snapshot_controller, "add") as add:
            _tick(cluster)

        assert [c.args[0].get_id() for c in take.call_args_list] == [group.get_id()]
        add.assert_not_called()                           # members never snapshot one by one
        assert rpcs == {}
        assert _deferrals(caplog) == []
