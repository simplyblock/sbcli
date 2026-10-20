"""The active triplet of a sync-replication LVS against the real
FoundationDB: every leader lookup (volume / snapshot / group snapshot /
snapshot delete / resize) probes and grants only inside the triplet that
leads the LVS, a leaderless one is recovered there, a restarting home member
never takes a leadership led from the other site, no grant runs while the
leadership is being moved, the nodes of the active triplet read back by
slot, the fenced transfer to the other site's triplet (RPC order, roles,
hublvol, failures), the leader rebuild of a member of the active triplet on
either site (its own triplet only, who may run it), the restart of every
member by its position in the active triplet (leader, takeover, non-leader
behind its anchor; nobody leading blocks none of them), the reconciler of
a move nobody owns any more and the promote runner's own leader lookup.
Storage nodes (RPC) and the port-block / hublvol side effects are mocked; the
database and its locks never are. The pure rules are in
tests/unit/test_sync_replication_active_triplet.py and
tests/unit/test_lvs_active_position.py.
"""
import datetime
import logging
import sys
import threading
import types
import uuid
from contextlib import ExitStack, contextmanager
from unittest.mock import MagicMock, call, patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import lvol_controller, snapshot_controller
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import RemoteJMDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException, RPCRemoteError
from simplyblock_core.services import snapshot_monitor, storage_node_monitor
from simplyblock_core.services import tasks_runner_sync_promote as promote_runner
from simplyblock_core.utils import ttl_cache
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
_seed_cluster, _sites, _make_owner = stack._seed_cluster, stack._sites, stack._make_owner
_update, _fresh, _took_leadership = stack._update, stack._fresh, stack._took_leadership
_grant_takes, _transfer_ready, _goes_silent = stack._grant_takes, stack._transfer_ready, stack._goes_silent

# The fixtures of test_sync_replication_lvs_stack: real DB, one RPC mock per node, hublvol side effects
# and port-block / monitor side effects recorded.
db = stack.db
rpcs = stack.rpcs
hub = stack.hub
env = stack.env
adopt = stack.adopt
attached = publish.attached


@pytest.fixture(autouse=True)
def _clean_leader_caches():
    ttl_cache.leader_cache.invalidate()
    ttl_cache.no_leader_cache.invalidate()
    yield
    ttl_cache.leader_cache.invalidate()
    ttl_cache.no_leader_cache.invalidate()


@pytest.fixture()
def no_sleep():
    with patch.object(ops.time, "sleep"):
        yield


def _layout(db, *, active_site="", lost_site="", lost_site_state=""):
    """a0 owns LVS_1: home triplet a0 a1 a2, remote triplet b0 b1 b2."""
    cluster = _seed_cluster(db, lost_site=lost_site)
    if lost_site_state:
        cluster.lost_site_state = lost_site_state
        cluster.write_to_db(db.kv_store)
    a, b = _sites(db, cluster)
    owner = _make_owner(db, a[0], a, b, lvs_id=1)
    if active_site:
        owner = _update(db, owner, lvs_active_site=active_site)
    return cluster, a, b, owner


def _home(db, a):
    return [_fresh(db, n) for n in a]


def _grants(rpcs, nodes):
    """Node ids that were granted the leadership."""
    return [n.get_id() for n in nodes if _took_leadership(rpcs[n.get_id()])]


#: What an RPC grant sends to the taker: the reload, the grant and its confirmation poll.
_GRANT_RPCS = {"bdev_lvol_update_lvstore", "bdev_lvol_set_leader", "get_lvstore"}


def _rpc_names(rpcs, node):
    """The RPC methods ``node`` received, in order."""
    return [c[0] for c in rpcs[node.get_id()].method_calls]


def _journal_ready(rpcs, node, *, local=True):
    """``node`` reports its journal copies: two ready same-site ones
    (``local``) or only other-site ones."""
    status = ({"jm_x": True, "remote_jm_yn1": True, "remote_xs_jm_zn1": False} if local
              else {"jm_x": False, "remote_jm_yn1": False, "remote_xs_jm_zn1": True,
                    "remote_xs_jm_wn1": True})
    rpcs[node.get_id()].jc_get_jm_status.return_value = status


def _demote_takes(rpcs, node):
    """A set_leader(False) on ``node`` makes it stop reporting the leadership."""
    rpc = rpcs[node.get_id()]

    def _set_leader(lvs_name, leader=False, **kwargs):
        if not leader:
            rpc.get_lvstore.return_value = {"lvs leadership": False}
        return True
    rpc.bdev_lvol_set_leader.side_effect = _set_leader


def _nothing_granted(rpcs, nodes):
    """No node of ``nodes`` got a grant: neither a set_leader(True) nor the
    metadata reload that precedes one."""
    assert _grants(rpcs, nodes) == []
    for node in nodes:
        rpcs[node.get_id()].bdev_lvol_update_lvstore.assert_not_called()


def _during_hand_off(rpcs, taker, change):
    """Run ``change()`` when ``taker`` gets its role stamp: inside the fenced
    window of a hand-off, after the move's probe before it and before the one
    after it."""
    def _stamp(*args, **kwargs):
        change()
        return True
    rpcs[taker.get_id()].bdev_lvol_set_lvs_opts.side_effect = _stamp


def _promote_task(db, cluster, owner, *, status=JobSchedule.STATUS_RUNNING, stale=False, **fields):
    task = JobSchedule()
    task.uuid = str(uuid.uuid4())
    task.cluster_id = cluster.get_id()
    task.node_id = owner.get_id()
    task.date = int(datetime.datetime.now().timestamp())
    task.function_name = JobSchedule.FN_SYNC_PROMOTE
    task.function_params = {"lvs_names": [owner.lvstore]}
    task.status = status
    for key, value in fields.items():
        setattr(task, key, value)
    task.write_to_db(db.kv_store)
    if stale:
        old = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
            seconds=ops.constants.TASK_LEASE_TTL_SEC + 60)
        db.atomic_update(task, lambda t: setattr(t, "updated_at", str(old)))
    return task


# ---------------------------------------------------------------------------
# lookups on an LVS led from the other site
# ---------------------------------------------------------------------------

class TestRemoteLedLookups:

    def test_volume_lookup_finds_the_remote_leader(self, db, rpcs, env):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[1])
        home = _home(db, a)
        leader, non_leaders = ops.find_leader_with_failover(home, "LVS_1")
        assert leader.get_id() == b[1].get_id()
        # the caller's own members come back as the non-leaders
        assert [n.get_id() for n in non_leaders] == [n.get_id() for n in home]
        for node in a:
            rpcs[node.get_id()].get_lvstore.assert_not_called()

    def test_create_on_leader_runs_on_the_remote_leader(self, db, rpcs, env):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        ran_on = []
        ok, leader, _ = ops.execute_on_leader_with_failover(
            _home(db, a), "LVS_1", lambda node: ran_on.append(node.get_id()) or True)
        assert ok and ran_on == [b[0].get_id()] and leader.get_id() == b[0].get_id()

    def test_snapshot_lookup_finds_the_remote_leader(self, db, rpcs, env):
        cluster, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[2])
        # snapshot create / clone / consistency-group snapshot all resolve
        # their leader through _find_lvs_leader with the home member list
        leader = snapshot_controller._find_lvs_leader(cluster.get_id(), "LVS_1", _home(db, a))
        assert leader.get_id() == b[2].get_id()

    def test_snapshot_delete_runs_on_the_remote_leader(self, db, rpcs, env):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        snap = types.SimpleNamespace(
            get_id=lambda: "snap-1", snap_bdev="LVS_1/SNAP_1", deletion_status="",
            instances=[], lvol=types.SimpleNamespace(lvs_name="LVS_1"))
        with patch.object(snapshot_monitor.snapshot_controller, "delete_bdev_absent_ok",
                          return_value=False) as delete:
            snapshot_monitor.process_snap_delete(snap, _fresh(db, a[0]), all_mini_lvols=[])
        assert delete.call_args.args[0].get_id() == b[0].get_id()

    def test_snapshot_delete_skips_a_raising_member_and_caches_by_site(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b0, b1 = b[0].get_id(), b[1].get_id()
        rpcs[b0].get_lvstore.side_effect = RPCRemoteError(message="busy", code=-16)
        rpcs.lead(b[1])
        snap = types.SimpleNamespace(
            get_id=lambda: "snap-1", snap_bdev="LVS_1/SNAP_1", deletion_status="",
            instances=[], lvol=types.SimpleNamespace(lvs_name="LVS_1"))
        cycle_cache: dict = {}
        with patch.object(snapshot_monitor.snapshot_controller, "delete_bdev_absent_ok",
                          return_value=False) as delete:
            snapshot_monitor.process_snap_delete(snap, _fresh(db, owner), all_mini_lvols=[],
                                                 leader_cache=cycle_cache)
            probes = {n.get_id(): rpcs[n.get_id()].get_lvstore.call_count for n in (*a, *b)}
            snapshot_monitor.process_snap_delete(snap, _fresh(db, owner), all_mini_lvols=[],
                                                 leader_cache=cycle_cache)
        # the raising b0 was asked and skipped, b1 leads and is reused from the cache
        rpcs[b0].get_lvstore.assert_called_once_with("LVS_1")
        assert [c.args[0].get_id() for c in delete.call_args_list] == [b1, b1]
        assert list(cycle_cache) == [(owner.get_id(), SITE_B)]
        assert cycle_cache[(owner.get_id(), SITE_B)].get_id() == b1
        assert {n.get_id(): rpcs[n.get_id()].get_lvstore.call_count for n in (*a, *b)} == probes

    def test_snapshot_monitor_cache_follows_a_site_switch(self, db, rpcs, env):
        _, a, b, owner = _layout(db)
        rpcs.lead(a[0])
        snap = types.SimpleNamespace(
            get_id=lambda: "snap-1", snap_bdev="LVS_1/SNAP_1", deletion_status="",
            instances=[], lvol=types.SimpleNamespace(lvs_name="LVS_1"))
        cycle_cache: dict = {}
        with patch.object(snapshot_monitor.snapshot_controller, "delete_bdev_absent_ok",
                          return_value=False) as delete:
            snapshot_monitor.process_snap_delete(snap, _fresh(db, owner), all_mini_lvols=[],
                                                 leader_cache=cycle_cache)
            # a promote moves LVS_1 to site B within the same monitor cycle
            rpcs[a[0].get_id()].get_lvstore.return_value = {"lvs leadership": False}
            rpcs.lead(b[0])
            _update(db, owner, lvs_active_site=SITE_B)
            snapshot_monitor.process_snap_delete(snap, _fresh(db, owner), all_mini_lvols=[],
                                                 leader_cache=cycle_cache)
        assert [c.args[0].get_id() for c in delete.call_args_list] == [a[0].get_id(), b[0].get_id()]

    def test_home_lookup_skips_the_remote_triplet(self, db, rpcs, env):
        _, a, b, _ = _layout(db)
        rpcs.lead(a[0])
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader.get_id() == a[0].get_id()
        for node in b:
            rpcs[node.get_id()].get_lvstore.assert_not_called()

    def test_resize_without_a_leader_resizes_nowhere(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        lvol = types.SimpleNamespace(
            ha_type="ha", nodes=[n.get_id() for n in a], lvs_name="LVS_1", lvol_bdev="LVOL_1",
            get_id=lambda: "lvol-1")
        with pytest.raises(RuntimeError, match="no leader"):
            lvol_controller._resize_lvol_on_all_nodes(lvol, _fresh(db, owner), 2048, lock=False)
        for node in (*a, *b):
            rpcs[node.get_id()].bdev_lvol_resize.assert_not_called()

    def test_resize_runs_on_the_remote_leader_first(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        lvol = types.SimpleNamespace(
            ha_type="ha", nodes=[n.get_id() for n in a], lvs_name="LVS_1", lvol_bdev="LVOL_1",
            get_id=lambda: "lvol-1")
        with patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                   return_value="skip"):
            lvol_controller._resize_lvol_on_all_nodes(lvol, _fresh(db, owner), 2048, lock=False)
        rpcs[b[0].get_id()].bdev_lvol_resize.assert_called_once_with("LVS_1/LVOL_1", 2048)
        for node in a:
            rpcs[node.get_id()].bdev_lvol_resize.assert_not_called()


# ---------------------------------------------------------------------------
# leaderless recovery
# ---------------------------------------------------------------------------

class TestLeaderlessRecovery:

    def test_recovery_grants_the_remote_primary(
            self, db, rpcs, hub, env, no_sleep):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        _journal_ready(rpcs, b[0])
        _grant_takes(rpcs, b[0])
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader.get_id() == b[0].get_id()
        names = [c[0] for c in rpcs[b[0].get_id()].method_calls]
        assert names.index("bdev_lvol_update_lvstore") < names.index("bdev_lvol_set_leader")
        assert _grants(rpcs, [*a, *b]) == [b[0].get_id()]
        # no hublvol repair wires the remote primary's own LVS
        hub["connect_to_hublvol"].assert_not_called()

    def test_a_taker_without_local_journal_quorum_is_refused(self, db, rpcs, hub, env, no_sleep):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        _journal_ready(rpcs, b[0], local=False)
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader is None
        assert _grants(rpcs, [*a, *b]) == []

    def test_recovery_during_a_move_grants_nothing(self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _journal_ready(rpcs, b[0])
        nodes = [_fresh(db, n) for n in (*a, *b)]
        # no promote task: only the grant guard stops it
        assert ops._recover_leaderless_lvs(
            cluster.get_id(), nodes, "LVS_1", _fresh(db, b[0]), owner_id=owner.get_id()) is None
        assert _grants(rpcs, nodes) == []
        for node in nodes:
            rpcs[node.get_id()].bdev_lvol_update_lvstore.assert_not_called()


# ---------------------------------------------------------------------------
# lookup without recovery: it reports who leads and never makes a leader
# ---------------------------------------------------------------------------

class TestLookupWithoutRecovery:

    def _leaderless(self, db, rpcs):
        """Led from site B, nobody leads, and the recovery WOULD grant on b0:
        its journal is ready and a grant on it takes."""
        cluster, a, b, _ = _layout(db, active_site=SITE_B)
        _journal_ready(rpcs, b[0])
        _grant_takes(rpcs, b[0])
        return cluster, a, b

    def test_a_leaderless_lvs_has_no_leader_and_gets_none(self, db, rpcs, hub, env, no_sleep):
        cluster, a, b = self._leaderless(db, rpcs)
        assert ops.find_leader_with_failover(_home(db, a), "LVS_1", recover=False) == (None, [])
        for node in (*a, *b):
            rpc = rpcs[node.get_id()]
            rpc.bdev_lvol_update_lvstore.assert_not_called()
            rpc.bdev_lvol_set_leader.assert_not_called()
            rpc.bdev_lvol_set_lvs_signal.assert_not_called()
        # the recovery takes this lock first and never releases it: free
        # means it did not start
        assert db.acquire_lvstore_lock(cluster.get_id(), "takeleader/LVS_1", "test") == (True, None)

    def test_the_answer_does_not_hold_off_a_later_recovery(self, db, rpcs, hub, env, no_sleep):
        _, a, b = self._leaderless(db, rpcs)
        assert ops.find_leader_with_failover(_home(db, a), "LVS_1", recover=False) == (None, [])
        # not a "confirmed leaderless" verdict: the default lookup still recovers
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader.get_id() == b[0].get_id()
        assert _grants(rpcs, [*a, *b]) == [b[0].get_id()]

    def test_a_leader_that_reports_itself_is_returned(self, db, rpcs, env):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[1])
        home = _home(db, a)
        leader, non_leaders = ops.find_leader_with_failover(home, "LVS_1", recover=False)
        assert leader.get_id() == b[1].get_id()
        assert [n.get_id() for n in non_leaders] == [n.get_id() for n in home]
        _nothing_granted(rpcs, [*a, *b])

    def test_a_cached_no_leader_verdict_is_honoured(self, db, rpcs, env):
        cluster, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        ttl_cache.no_leader_cache.put((cluster.get_id(), "LVS_1"), True)
        assert ops.find_leader_with_failover(_home(db, a), "LVS_1", recover=False) == (None, [])
        for node in (*a, *b):
            rpcs[node.get_id()].get_lvstore.assert_not_called()

    def test_an_unreachable_member_forces_no_leadership_change(self, db, rpcs, hub, env, no_sleep):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        # b0 does not answer on mgmt while its fabric is healthy
        _goes_silent(rpcs, b[0], RPCException("timeout"))
        assert ops.find_leader_with_failover(_home(db, a), "LVS_1", recover=False) == (None, [])
        for node in (*a, *b):
            rpcs[node.get_id()].bdev_lvol_set_lvs_signal.assert_not_called()
        # in this very state the default lookup forces the change through b1
        ops.find_leader_with_failover(_home(db, a), "LVS_1")
        rpcs[b[1].get_id()].bdev_lvol_set_lvs_signal.assert_called_once_with("LVS_1")


# ---------------------------------------------------------------------------
# a move in flight, owned by a live promote task
# ---------------------------------------------------------------------------

class TestMovingOwnedByPromote:

    @pytest.mark.parametrize("leader_index", [0, 4])
    def test_lookup_finds_a_leader_on_either_triplet(
            self, db, rpcs, env, leader_index):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _promote_task(db, cluster, owner)
        members = [*a, *b]
        rpcs.lead(members[leader_index])
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader.get_id() == members[leader_index].get_id()
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_no_leader_means_no_grant_and_no_forced_change(self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _promote_task(db, cluster, owner)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        leader, non_leaders = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert (leader, non_leaders) == (None, [])
        for node in (*a, *b):
            rpc = rpcs[node.get_id()]
            assert not _took_leadership(rpc)
            rpc.bdev_lvol_update_lvstore.assert_not_called()
            rpc.bdev_lvol_set_lvs_signal.assert_not_called()
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_a_promote_task_blocks_recovery_by_node_or_lvs(self, db, env):
        cluster, a, b, owner = _layout(db)
        other = _promote_task(db, cluster, owner, node_id="elsewhere")
        assert ops._leadership_moving_tasks_active(cluster.get_id(), [], "LVS_1")
        assert not ops._leadership_moving_tasks_active(cluster.get_id(), [], "LVS_2")
        db.atomic_update(other, lambda t: setattr(t, "function_params", {}))
        assert ops._leadership_moving_tasks_active(cluster.get_id(), ["elsewhere"], "LVS_2")
        assert not ops._leadership_moving_tasks_active(cluster.get_id(), [a[0].get_id()], "LVS_2")

    def test_a_finished_or_dead_promote_task_blocks_nothing(self, db, env):
        cluster, _, _, owner = _layout(db)
        _promote_task(db, cluster, owner, status=JobSchedule.STATUS_DONE)
        _promote_task(db, cluster, owner, stale=True)
        assert not ops._leadership_moving_tasks_active(cluster.get_id(), [owner.get_id()], "LVS_1")


# ---------------------------------------------------------------------------
# the grant lock serialises grants against the move's marker
# ---------------------------------------------------------------------------

def _begin_move(db, cluster, owner, target, *, expect=""):
    """The promote's marker transaction (DBController.begin_sync_promote_moves)
    for ``owner``'s LVS, by an active promote task that does NOT own this
    LVS's move (other node, no lvs_names), so only the grant lock and the
    marker decide - not _leadership_moving_tasks_active. Returns the
    problems."""
    task = _promote_task(db, cluster, owner, node_id="elsewhere", function_params={"lvs_names": []})

    def check(cluster, owner, volumes, exp):
        return src.promote_move_problems(cluster, owner, volumes, exp, target)
    return db.begin_sync_promote_moves(task, {owner.lvstore: (owner.get_id(), expect)}, target, check)


class TestGrantLockAgainstBeginMove:

    @pytest.fixture(autouse=True)
    def _no_hublvol_repair(self):
        with patch.object(ops.health_controller, "_check_sec_node_hublvol"):
            yield

    def test_a_grant_in_progress_holds_the_move_back(
            self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner = _layout(db)
        _journal_ready(rpcs, a[0])
        inside, release = threading.Event(), threading.Event()

        def _reload(lvs_name):
            inside.set()
            release.wait(10)
            return True
        rpcs[a[0].get_id()].bdev_lvol_update_lvstore.side_effect = _reload
        nodes = _home(db, a)
        worker = threading.Thread(target=ops._recover_leaderless_lvs, args=(
            cluster.get_id(), nodes, "LVS_1", nodes[0]), kwargs={"owner_id": owner.get_id()})
        worker.start()
        assert inside.wait(10)
        problems = _begin_move(db, cluster, owner, SITE_B)
        assert len(problems) == 1 and "a leadership grant is in progress" in problems[0]
        assert _fresh(db, owner).lvs_active_site == ""
        release.set()
        worker.join(10)
        assert _grants(rpcs, nodes) == [a[0].get_id()]
        assert _begin_move(db, cluster, owner, SITE_B) == []
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_recovery_after_a_committed_move_grants_nothing(self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner = _layout(db)
        _journal_ready(rpcs, a[0])
        assert _begin_move(db, cluster, owner, SITE_B) == []
        nodes = _home(db, a)
        assert ops._recover_leaderless_lvs(
            cluster.get_id(), nodes, "LVS_1", nodes[0], owner_id=owner.get_id()) is None
        assert _grants(rpcs, [*a, *b]) == []

    def test_recovery_during_an_owned_move_grants_nothing(
            self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner = _layout(db)
        _journal_ready(rpcs, a[0])
        task = _promote_task(db, cluster, owner)

        def check(cluster, owner, volumes, exp):
            return src.promote_move_problems(cluster, owner, volumes, exp, SITE_B)
        assert db.begin_sync_promote_moves(task, {"LVS_1": (owner.get_id(), "")}, SITE_B, check) == []
        assert db.get_task_by_id(task.uuid).function_params["moves"] == {"LVS_1": ""}
        nodes = _home(db, a)
        assert ops._recover_leaderless_lvs(
            cluster.get_id(), nodes, "LVS_1", nodes[0], owner_id=owner.get_id()) is None
        assert _grants(rpcs, [*a, *b]) == []
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_a_home_leader_rebuild_holds_the_move_back(self, db, env):
        cluster, a, _, owner = _layout(db)
        inside, release = threading.Event(), threading.Event()

        def _impl(*args, **kwargs):
            inside.set()
            release.wait(10)
            return True
        with patch.object(ops, "_recreate_lvstore_impl", side_effect=_impl):
            worker = threading.Thread(target=ops.recreate_lvstore, args=(_fresh(db, owner),))
            worker.start()
            assert inside.wait(10)
            assert _begin_move(db, cluster, owner, SITE_B)
            assert _fresh(db, owner).lvs_active_site == ""
            release.set()
            worker.join(10)
        assert _begin_move(db, cluster, owner, SITE_B) == []

    @pytest.mark.parametrize("marker", ["moving:" + SITE_B, SITE_B])
    def test_a_home_leader_rebuild_after_the_move_is_refused(self, db, env, marker):
        _, _, _, owner = _layout(db, active_site=marker)
        with patch.object(ops, "_recreate_lvstore_impl") as impl:
            with pytest.raises(ops.LVSLeadershipElsewhereError):
                ops.recreate_lvstore(_fresh(db, owner))
            with pytest.raises(ops.LVSLeadershipElsewhereError):
                ops.recreate_lvstore(_fresh(db, owner), activation_mode=True)
        impl.assert_not_called()

    def test_begin_move_needs_the_expected_marker(self, db, env):
        cluster, _, _, owner = _layout(db, active_site=SITE_B)
        problems = _begin_move(db, cluster, owner, SITE_A)
        assert len(problems) == 1 and "expected ''" in problems[0]
        assert _fresh(db, owner).lvs_active_site == SITE_B
        assert _begin_move(db, cluster, owner, SITE_A, expect=SITE_B) == []
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_A

    def test_marker_compare_and_set_detects_a_change(self, db, env):
        _, _, _, owner = _layout(db, active_site="moving:" + SITE_B)
        assert ops.set_lvs_active_site(owner.get_id(), SITE_A, expect="moving:" + SITE_A) is False
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B
        assert ops.set_lvs_active_site(owner.get_id(), SITE_B, expect="moving:" + SITE_B) is True


# ---------------------------------------------------------------------------
# the nodes of the active triplet
# ---------------------------------------------------------------------------

def _ids(nodes):
    return tuple(n and n.get_id() for n in nodes)


class TestActiveTripletNodes:

    def test_home_led(self, db):
        _, a, _, owner = _layout(db)
        assert _ids(ops.active_triplet_nodes(owner, db)) == _ids(a)

    def test_led_from_the_other_site(self, db):
        _, _, b, owner = _layout(db, active_site=SITE_B)
        assert _ids(ops.active_triplet_nodes(owner, db)) == _ids(b)

    def test_ftt1_leaves_the_tertiary_slot_empty(self, db):
        cluster = _seed_cluster(db, ftt=1)
        a, b = _sites(db, cluster)
        owner = _make_owner(db, a[0], a, b, lvs_id=1, ftt=1)
        assert _ids(ops.active_triplet_nodes(owner, db)) == (a[0].get_id(), a[1].get_id(), None)

        # _make_owner fills the remote tertiary whatever the FTT: empty it, so
        # the remote-led answer has to keep the slot too.
        owner = _update(db, owner, remote_tertiary_node_id="", lvs_active_site=SITE_B)
        assert _ids(ops.active_triplet_nodes(owner, db)) == (b[0].get_id(), b[1].get_id(), None)

    def test_the_home_primary_is_read_from_the_database_too(self, db):
        _, a, _, owner = _layout(db)
        _update(db, a[0], status=StorageNode.STATUS_OFFLINE)
        primary, _, _ = ops.active_triplet_nodes(owner, db)
        # Fetched from the database, never the caller's (stale) copy handed back.
        assert owner.status == StorageNode.STATUS_ONLINE
        assert primary.get_id() == owner.get_id() and primary.status == StorageNode.STATUS_OFFLINE

    @pytest.mark.parametrize("target", [SITE_A, SITE_B])
    def test_a_move_in_flight_has_no_active_triplet(self, db, target):
        _, _, _, owner = _layout(db, active_site="moving:" + target)
        with pytest.raises(ValueError, match="no active triplet"):
            ops.active_triplet_nodes(owner, db)

    def test_a_slot_naming_a_missing_node_is_not_an_empty_slot(self, db):
        _, _, _, owner = _layout(db, active_site=SITE_B)
        owner = _update(db, owner, remote_secondary_node_id="gone")
        with pytest.raises(KeyError, match="gone"):
            ops.active_triplet_nodes(owner, db)


# ---------------------------------------------------------------------------
# restart
# ---------------------------------------------------------------------------

class TestRestart:

    @pytest.mark.parametrize("marker", [SITE_B, "moving:" + SITE_B])
    def test_home_primary_restarts_behind_the_remote_leader(
            self, db, rpcs, env, marker):
        _, a, b, owner = _layout(db, active_site=marker)
        rpcs.lead(b[0])
        with patch.object(ops, "recreate_lvstore") as leader_rebuild, \
                patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True) as rebuild:
            assert ops._recreate_all_lvstores_serial(_fresh(db, owner)) is True
        leader_rebuild.assert_not_called()
        snode, leader, primary = rebuild.call_args.args
        assert (snode.get_id(), leader.get_id(), primary.get_id()) == (
            owner.get_id(), b[0].get_id(), owner.get_id())

    def test_home_secondary_never_takes_over_a_remote_led_lvs(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        env.down.add(owner.get_id())
        rpcs.lead(b[0])
        with patch.object(ops, "recreate_lvstore", return_value=True) as leader_rebuild, \
                patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True) as rebuild:
            assert ops._recreate_all_lvstores_serial(_fresh(db, a[1])) is True
        # only its own (empty) primary step, never a takeover of LVS_1
        assert all(c.kwargs.get("lvs_primary") is None for c in leader_rebuild.call_args_list)
        snode, leader, primary = rebuild.call_args.args
        assert (snode.get_id(), leader.get_id(), primary.get_id()) == (
            a[1].get_id(), b[0].get_id(), owner.get_id())

    def test_home_tertiary_quiesces_the_remote_leader(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        rpcs.lead(b[1])
        with patch.object(ops, "recreate_lvstore", return_value=True), \
                patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True) as rebuild:
            assert ops._recreate_all_lvstores_serial(_fresh(db, a[2])) is True
        assert rebuild.call_args.args[1].get_id() == b[1].get_id()

    def test_recreate_on_sec_quiesces_the_remote_leader(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        with patch.object(ops, "recreate_lvstore_on_non_leader", return_value=True) as rebuild:
            assert ops.recreate_lvstore_on_sec(_fresh(db, a[1])) is True
        assert rebuild.call_args.kwargs["leader_node"].get_id() == b[0].get_id()

    def test_home_led_lvs_rebuilt_by_owner_as_leader(self, db, rpcs, env):
        _, a, b, owner = _layout(db)
        with patch.object(ops, "recreate_lvstore", return_value=True) as leader_rebuild, \
                patch.object(ops, "recreate_lvstore_on_non_leader") as rebuild:
            assert ops._recreate_all_lvstores_serial(_fresh(db, owner)) is True
        leader_rebuild.assert_called_once()
        rebuild.assert_not_called()

    @pytest.mark.parametrize("marker, restarting, down, as_leader", [
        (SITE_B, 0, (), True),                  # the active primary
        (SITE_B, 1, (), False),                 # the active secondary behind a connected primary
        (SITE_B, 1, (0,), True),                # ... takes over from a disconnected one
        (SITE_B, 2, (), False),                 # the active tertiary never leads
        (SITE_B, 2, (0,), False),
        (SITE_B, 2, (0, 1), False),             # ... and is never skipped
        ("", 0, (), False),                     # the passive triplet
        ("", 1, (0,), False),
        ("moving:" + SITE_B, 0, (), False),     # a move in flight
        ("moving:" + SITE_A, 1, (0,), False),
    ])
    def test_a_remote_member_leads_by_its_position(
            self, db, rpcs, env, marker, restarting, down, as_leader):
        _, a, b, owner = _layout(db, active_site=marker)
        env.down.update(b[i].get_id() for i in down)
        me = b[restarting].get_id()
        with patch.object(ops, "recreate_lvstore", return_value=True) as leader_rebuild, \
                patch.object(ops, "_rebuild_remote_instance", return_value=True) as non_leader_rebuild:
            assert ops.recreate_all_lvstores(_fresh(db, b[restarting])) is True
        as_leader_of = [(c.args[0].get_id(), c.kwargs["lvs_primary"].get_id())
                        for c in leader_rebuild.call_args_list if c.kwargs.get("lvs_primary") is not None]
        as_non_leader_of = [(c.args[0].get_id(), c.args[1].get_id()) for c in non_leader_rebuild.call_args_list]
        assert as_leader_of == ([(me, owner.get_id())] if as_leader else [])
        assert as_non_leader_of == ([] if as_leader else [(me, owner.get_id())])

    def test_an_empty_primary_slot_counts_as_disconnected(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        _update(db, owner, remote_primary_node_id="")
        with patch.object(ops, "recreate_lvstore", return_value=True) as leader_rebuild, \
                patch.object(ops, "_rebuild_remote_instance", return_value=True) as non_leader_rebuild:
            assert ops.recreate_all_lvstores(_fresh(db, b[1])) is True
        assert [(c.args[0].get_id(), c.kwargs["lvs_primary"].get_id()) for c in leader_rebuild.call_args_list
                if c.kwargs.get("lvs_primary") is not None] == [(b[1].get_id(), owner.get_id())]
        non_leader_rebuild.assert_not_called()

    @pytest.mark.parametrize("failure", [False, RPCException("spdk gone")])
    def test_a_failed_leader_rebuild_fails_the_restart(self, db, rpcs, env, failure):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        seen = []

        def _rebuild(snode, force=False, lvs_primary=None, activation_mode=False):
            if lvs_primary is None:
                return True
            seen.append(_fresh(db, owner).lvstore_status)
            if isinstance(failure, Exception):
                raise failure
            return failure

        with patch.object(ops, "recreate_lvstore", side_effect=_rebuild):
            assert ops.recreate_all_lvstores(_fresh(db, b[0])) is False
        # the owner carries the window marker for the rebuild and gets rid of it
        assert seen == ["in_creation"]
        assert _fresh(db, owner).lvstore_status == "ready"
        # an exception may have left a fence behind: every other member is released
        assert env.blocked() == []
        assert set(env.unblocked()) == (
            {n.get_id() for n in (*a, b[1], b[2])} if isinstance(failure, Exception) else set())

    def test_reactivation_is_refused_for_a_remote_led_lvs(self, db, rpcs, hub, env):
        cluster = _seed_cluster(db, ftt=1)
        a, b = _sites(db, cluster)
        led_elsewhere = _make_owner(db, a[0], a, b, lvs_id=1, ftt=1)
        _update(db, led_elsewhere, lvs_active_site=SITE_B)
        _make_owner(db, b[0], b, a, lvs_id=2, ftt=1)
        cluster = db.get_cluster_by_id(cluster.get_id())
        cluster.status = Cluster.STATUS_SUSPENDED
        cluster.activated_node_ids = [n.get_id() for n in a + b]
        cluster.write_to_db(db.kv_store)

        with ExitStack() as es:
            cluster_ops = stack._activation_patches(es)
            impl = es.enter_context(patch.object(ops, "_recreate_lvstore_impl", return_value=True))
            with pytest.raises(ValueError, match="Failed to activate cluster"):
                cluster_ops._cluster_activate(cluster.get_id())
        assert led_elsewhere.get_id() not in {c.args[0].get_id() for c in impl.call_args_list}
        assert not _took_leadership(rpcs[led_elsewhere.get_id()])


# ---------------------------------------------------------------------------
# the fenced transfer
# ---------------------------------------------------------------------------

class TestTransfer:

    def _run(self, db, rpcs, a, b, owner, *, reload=True):
        leader, taker = _fresh(db, a[0]), _fresh(db, b[0])
        _transfer_ready(rpcs, leader)
        _grant_takes(rpcs, taker)
        peers = [_fresh(db, n) for n in (*a, b[1], b[2])]
        ops.transfer_lvs_leadership(leader, taker, peers, lvs_node=_fresh(db, owner),
                                    reload_metadata=reload)
        return leader, taker

    def test_to_the_remote_triplet(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db)
        _update(db, owner, lvstore_status="ready")
        leader, taker = self._run(db, rpcs, a, b, owner)

        # every connected peer fenced, the leader last, all released again
        blocked = env.blocked()
        assert blocked[-1] == leader.get_id()
        assert set(blocked) == {n.get_id() for n in (*a, b[1], b[2])}
        assert sorted(env.unblocked()) == sorted(blocked)
        # the old leader: replication suspended, drained, demoted, re-stamped non-leader
        old = [c[0] for c in rpcs[leader.get_id()].method_calls]
        assert old.index("jc_disable_replication") < old.index("bdev_distrib_check_inflight_io") \
            < old.index("bdev_lvol_set_leader") < old.index("bdev_distrib_force_to_non_leader") \
            < old.index("bdev_lvol_set_lvs_opts")
        rpcs[leader.get_id()].bdev_lvol_set_leader.assert_called_once_with(
            "LVS_1", leader=False, bs_nonleadership=True)
        # every other-site instance stamped non-leader (no hublvol re-stamps it)
        assert stack._roles(rpcs[leader.get_id()]) == ["secondary"]
        assert stack._roles(rpcs[a[1].get_id()]) == ["secondary"]
        assert stack._roles(rpcs[a[2].get_id()]) == ["tertiary"]
        # the taker: metadata reloaded, role primary, granted
        new = [c[0] for c in rpcs[taker.get_id()].method_calls]
        assert new.index("bdev_lvol_update_lvstore") < new.index("bdev_lvol_set_lvs_opts") \
            < new.index("bdev_lvol_set_leader")
        assert stack._roles(rpcs[taker.get_id()]) == ["primary"]
        assert _grants(rpcs, [*a, *b]) == [taker.get_id()]
        # hublvol only within the taker's site
        assert adopt.call_args.args[0].get_id() == taker.get_id()
        assert adopt.call_args.args[1].get_id() == owner.get_id()
        assert stack._hub_callers(hub, "create_secondary_hublvol") == [b[1].get_id()]
        connects = {c.args[0].get_id(): c.kwargs["role"] for c in hub["connect_to_hublvol"].call_args_list
                    if not c.kwargs.get("attach_only")}
        assert connects == {b[1].get_id(): "secondary", b[2].get_id(): "tertiary"}
        assert not {n.get_id() for n in a} & set(stack._hub_callers(hub, "connect_to_hublvol"))
        failover = hub["add_hublvol_failover_path"].call_args
        assert failover.args[0].get_id() == b[2].get_id()
        assert failover.kwargs["lvs_node"].get_id() == owner.get_id()
        # nothing killed, the old leader's status restored
        env.set_node_status.assert_not_called()
        assert _fresh(db, owner).lvstore_status == "ready"

    @pytest.mark.parametrize("reload_result", [False, RPCException("reload failed")])
    def test_a_failed_metadata_reload_grants_nothing(
            self, db, rpcs, hub, env, adopt, reload_result):
        _, a, b, owner = _layout(db)
        _update(db, owner, lvstore_status="ready")
        reload = rpcs[b[0].get_id()].bdev_lvol_update_lvstore
        if isinstance(reload_result, Exception):
            reload.side_effect = reload_result
        else:
            reload.return_value = reload_result
        with pytest.raises(ops.LeadershipTransferError) as err:
            self._run(db, rpcs, a, b, owner)
        assert err.value.granted is False
        assert not _took_leadership(rpcs[b[0].get_id()])
        assert sorted(env.unblocked()) == sorted(env.blocked())
        env.set_node_status.assert_not_called()
        assert _fresh(db, owner).lvstore_status == "ready"

    def test_a_raw_error_releases_every_blocked_peer(self, db, rpcs, hub, env, adopt):
        """An error that is not a fence abort leaves the window through the
        transfer's cleanup, which releases every port the fence blocked."""
        _, a, b, owner = _layout(db)
        _update(db, owner, lvstore_status="ready")
        leader, taker = _fresh(db, a[0]), _fresh(db, b[0])
        _transfer_ready(rpcs, leader)
        _grant_takes(rpcs, taker)

        def _fails():
            raise RuntimeError("before the grant")

        with pytest.raises(RuntimeError, match="before the grant"):
            ops.transfer_lvs_leadership(leader, taker, [_fresh(db, n) for n in (*a, b[1], b[2])],
                                        lvs_node=_fresh(db, owner), before_grant=_fails)
        assert len(env.blocked()) >= 2
        assert sorted(env.unblocked()) == sorted(env.blocked())
        assert _fresh(db, owner).lvstore_status == "ready"

    def test_a_grant_that_does_not_take_aborts(self, db, rpcs, hub, env, adopt, no_sleep):
        _, a, b, owner = _layout(db)
        leader, taker = _fresh(db, a[0]), _fresh(db, b[0])
        _transfer_ready(rpcs, leader)       # the taker never reports the leadership
        with pytest.raises(ops.LeadershipTransferError, match="Failed to restore leadership"):
            ops.transfer_lvs_leadership(leader, taker, [_fresh(db, n) for n in (*a, b[1], b[2])],
                                        lvs_node=_fresh(db, owner))
        assert sorted(env.unblocked()) == sorted(env.blocked())
        assert not _took_leadership(rpcs[a[0].get_id()])

    def test_a_failed_old_leader_restamp_aborts_the_grant(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db)
        rpcs[a[0].get_id()].bdev_lvol_set_lvs_opts.return_value = False
        with pytest.raises(ops.LeadershipTransferError) as err:
            self._run(db, rpcs, a, b, owner)
        assert err.value.granted is False
        assert _grants(rpcs, [*a, *b]) == []
        rpcs[b[0].get_id()].bdev_lvol_set_lvs_opts.assert_not_called()
        assert sorted(env.unblocked()) == sorted(env.blocked())

    def test_a_leader_marked_disconnected_is_still_fenced(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db)
        env.down.add(a[0].get_id())          # stale / cached verdict: "gone"
        leader, _ = self._run(db, rpcs, a, b, owner)
        assert env.blocked()[-1] == leader.get_id()
        rpcs[leader.get_id()].bdev_lvol_set_leader.assert_called_once_with(
            "LVS_1", leader=False, bs_nonleadership=True)

    def test_a_former_leader_is_stamped_non_leader_first(self, db, rpcs, hub, env, adopt):
        # Crash after the demote, before the grant: nobody leads, the former
        # leader may still carry the PRIMARY role.
        _, a, b, owner = _layout(db)
        taker = _fresh(db, b[0])
        _grant_takes(rpcs, taker)
        events = []
        rpcs[a[0].get_id()].bdev_lvol_set_lvs_opts.side_effect = \
            lambda *args, **kwargs: events.append(("stamp", kwargs["role"])) or True
        env.set_port.side_effect = lambda node, port, block=False, **kwargs: events.append(
            ("block" if block else "release", node.get_id()))
        grant = rpcs[taker.get_id()].bdev_lvol_set_leader.side_effect
        rpcs[taker.get_id()].bdev_lvol_set_leader.side_effect = \
            lambda *args, **kwargs: events.append(("grant", taker.get_id())) or grant(*args, **kwargs)
        ops.transfer_lvs_leadership(None, taker, [_fresh(db, n) for n in (*a, b[1], b[2])],
                                    lvs_node=_fresh(db, owner))
        own = [e for e in events if e[0] in ("stamp", "grant") or e[1] == a[0].get_id()]
        assert own == [("block", a[0].get_id()), ("stamp", "secondary"), ("grant", taker.get_id()),
                       ("release", a[0].get_id())]

    def test_without_a_current_leader_nothing_is_demoted(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db)
        taker = _fresh(db, b[0])
        _grant_takes(rpcs, taker)
        ops.transfer_lvs_leadership(None, taker, [_fresh(db, n) for n in (*a, b[1], b[2])],
                                    lvs_node=_fresh(db, owner))
        for node in a:
            rpcs[node.get_id()].bdev_lvol_set_leader.assert_not_called()
        assert _grants(rpcs, [*a, *b]) == [taker.get_id()]

    # grant=False: the taker gets its role and its triplet's hublvol, never the
    # leadership - it takes that itself on the first client IO

    def test_without_a_grant_the_taker_is_stamped_and_wired(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db)
        _update(db, owner, lvstore_status="ready")
        leader, taker = _fresh(db, a[0]), _fresh(db, b[0])
        _transfer_ready(rpcs, leader)
        ops.transfer_lvs_leadership(leader, taker, [_fresh(db, n) for n in (*a, b[1], b[2])],
                                    lvs_node=_fresh(db, owner), grant=False)

        # the old leader: replication suspended, drained, demoted, re-stamped non-leader
        old = _rpc_names(rpcs, leader)
        assert old.index("jc_disable_replication") < old.index("bdev_distrib_check_inflight_io") \
            < old.index("bdev_lvol_set_leader") < old.index("bdev_distrib_force_to_non_leader") \
            < old.index("bdev_lvol_set_lvs_opts")
        rpcs[leader.get_id()].bdev_lvol_set_leader.assert_called_once_with(
            "LVS_1", leader=False, bs_nonleadership=True)
        assert stack._roles(rpcs[leader.get_id()]) == ["secondary"]
        assert stack._roles(rpcs[a[1].get_id()]) == ["secondary"]
        assert stack._roles(rpcs[a[2].get_id()]) == ["tertiary"]
        # the taker: role primary and nothing of a grant - no reload, no set_leader, no poll
        new = _rpc_names(rpcs, taker)
        assert "bdev_lvol_set_lvs_opts" in new and not set(new) & _GRANT_RPCS
        assert stack._roles(rpcs[taker.get_id()]) == ["primary"]
        assert _grants(rpcs, [*a, *b]) == []
        # hublvol only within the taker's site, as with a grant
        assert adopt.call_args.args[0].get_id() == taker.get_id()
        assert adopt.call_args.args[1].get_id() == owner.get_id()
        assert stack._hub_callers(hub, "create_secondary_hublvol") == [b[1].get_id()]
        connects = {c.args[0].get_id(): c.kwargs["role"] for c in hub["connect_to_hublvol"].call_args_list
                    if not c.kwargs.get("attach_only")}
        assert connects == {b[1].get_id(): "secondary", b[2].get_id(): "tertiary"}
        assert not {n.get_id() for n in a} & set(stack._hub_callers(hub, "connect_to_hublvol"))
        failover = hub["add_hublvol_failover_path"].call_args
        assert failover.args[0].get_id() == b[2].get_id()
        assert failover.kwargs["lvs_node"].get_id() == owner.get_id()
        # every connected peer fenced, the leader last, all released again
        blocked = env.blocked()
        assert blocked[-1] == leader.get_id()
        assert set(blocked) == {n.get_id() for n in (*a, b[1], b[2])}
        assert sorted(env.unblocked()) == sorted(blocked)
        env.set_node_status.assert_not_called()
        assert _fresh(db, owner).lvstore_status == "ready"

    def test_without_a_grant_the_hand_off_is_repeatable(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db)

        def hand_off():
            ops.transfer_lvs_leadership(None, _fresh(db, b[0]), [_fresh(db, n) for n in (*a, b[1], b[2])],
                                        lvs_node=_fresh(db, owner), grant=False)
            return ({n.get_id(): list(rpcs[n.get_id()].method_calls) for n in (*a, *b)},
                    {name: stack._hub_callers(hub, name) for name in hub}, env.blocked(), env.unblocked())

        first_rpcs, first_hub, blocked, unblocked = hand_off()
        # nobody is demoted, the other site is stamped non-leader, the taker primary
        for node in (*a, *b):
            rpcs[node.get_id()].bdev_lvol_set_leader.assert_not_called()
        assert [stack._roles(rpcs[n.get_id()]) for n in a] == [["secondary"], ["secondary"], ["tertiary"]]
        assert stack._roles(rpcs[b[0].get_id()]) == ["primary"]
        assert not set(_rpc_names(rpcs, b[0])) & _GRANT_RPCS
        assert set(blocked) == {n.get_id() for n in (*a, b[1], b[2])}
        assert sorted(unblocked) == sorted(blocked)

        # a second hand-off to the same taker: the same RPCs, the same wiring
        second_rpcs, second_hub, blocked_twice, unblocked_twice = hand_off()
        assert second_rpcs == {node_id: calls * 2 for node_id, calls in first_rpcs.items()}
        assert second_hub == {name: callers * 2 for name, callers in first_hub.items()}
        assert adopt.call_count == 2
        assert blocked_twice == blocked * 2 and sorted(unblocked_twice) == sorted(blocked_twice)

    def test_without_a_grant_through_the_callers_fence(self, db, rpcs, hub, env, adopt):
        # the branch a restart uses: the caller owns the fence and the steps after the window
        _, a, b, owner = _layout(db)
        leader, taker = _fresh(db, a[0]), _fresh(db, b[0])
        _transfer_ready(rpcs, leader)
        fence = ops._LvsFence(_fresh(db, owner), taker, db)
        try:
            deferred = ops.transfer_lvs_leadership(
                leader, taker, [_fresh(db, n) for n in (*a, b[1], b[2])], lvs_node=fence.lvs_node,
                fence=fence, grant=False)
        finally:
            fence.release_hub_locks()
            fence.close()
        assert fence.granted is False
        assert stack._roles(rpcs[taker.get_id()]) == ["primary"]
        assert not set(_rpc_names(rpcs, taker)) & _GRANT_RPCS
        assert _grants(rpcs, [*a, *b]) == []
        # the tertiary's second path is handed back for the caller to add after the window
        assert [[n.get_id() for n in path] for path in deferred] == [
            [b[2].get_id(), taker.get_id(), b[1].get_id()]]
        hub["add_hublvol_failover_path"].assert_not_called()
        assert not fence.blocked_peers and sorted(env.unblocked()) == sorted(env.blocked())

    @pytest.mark.parametrize("stamp_result", [False, RPCException("stamp failed")])
    def test_without_a_grant_a_failed_taker_stamp_aborts(
            self, db, rpcs, hub, env, adopt, stamp_result):
        _, a, b, owner = _layout(db)
        _update(db, owner, lvstore_status="ready")
        leader, taker = _fresh(db, a[0]), _fresh(db, b[0])
        _transfer_ready(rpcs, leader)
        stamp = rpcs[taker.get_id()].bdev_lvol_set_lvs_opts
        if isinstance(stamp_result, Exception):
            stamp.side_effect = stamp_result
        else:
            stamp.return_value = stamp_result
        with pytest.raises(ops.LeadershipTransferError) as err:
            ops.transfer_lvs_leadership(leader, taker, [_fresh(db, n) for n in (*a, b[1], b[2])],
                                        lvs_node=_fresh(db, owner), grant=False)
        assert err.value.granted is False
        # the abort is at the taker's stamp: tried once, nothing wired, nothing granted
        stamp.assert_called_once()
        adopt.assert_not_called()
        assert _grants(rpcs, [*a, *b]) == []
        assert not set(_rpc_names(rpcs, taker)) & _GRANT_RPCS
        assert sorted(env.unblocked()) == sorted(env.blocked())
        env.set_node_status.assert_not_called()
        assert _fresh(db, owner).lvstore_status == "ready"

    def test_without_a_grant_a_failed_adopt_reports_no_grant(
            self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db)
        leader, taker = _fresh(db, a[0]), _fresh(db, b[0])
        _transfer_ready(rpcs, leader)
        adopt.side_effect = RPCException("adopt failed")
        with pytest.raises(ops.LeadershipTransferError, match="adopt_hublvol") as err:
            ops.transfer_lvs_leadership(leader, taker, [_fresh(db, n) for n in (*a, b[1], b[2])],
                                        lvs_node=_fresh(db, owner), grant=False)
        assert err.value.granted is False
        assert stack._roles(rpcs[taker.get_id()]) == ["primary"]
        assert sorted(env.unblocked()) == sorted(env.blocked())


# ---------------------------------------------------------------------------
# the leader rebuild of a member of the active triplet
# ---------------------------------------------------------------------------

def _ids_of(nodes):
    return {n.get_id() for n in nodes}


#: The hublvol side effects that connect their node to other nodes.
_HUB_CONNECTS = ("connect_to_hublvol", "add_hublvol_failover_path")


def _hub_nodes(hub, adopt):
    """Every node id a hublvol side effect ran on or connected its node to.
    The owner handed over as the source of the LVS metadata (``lvs_node``, the
    argument of an adopt and of a secondary hublvol) is not one."""
    seen = set()
    for name, mock in (*hub.items(), ("adopt_hublvol", adopt)):
        for c in mock.call_args_list:
            nodes = [c.args[0]]
            if name in _HUB_CONNECTS:
                nodes += [*c.args[1:], c.kwargs.get("failover_node")]
            seen |= {n.get_id() for n in nodes if isinstance(n, StorageNode)}
    return seen


def _untouched(rpcs, hub, adopt, env, nodes):
    """No node of ``nodes`` got an RPC or a port block or release, ran a
    hublvol side effect or was connected to by one."""
    for node in nodes:
        assert rpcs[node.get_id()].method_calls == [], node.get_id()
    assert not _ids_of(nodes) & {*env.blocked(), *env.unblocked(), *_hub_nodes(hub, adopt)}


def _wired(hub):
    """node id -> (role, target id) of every in-window hublvol connect."""
    return {c.args[0].get_id(): (c.kwargs["role"], c.args[1].get_id())
            for c in hub["connect_to_hublvol"].call_args_list if not c.kwargs.get("attach_only")}


def _examined_then_granted(rpcs, node):
    """``node`` examined the LVS and was then granted the leadership, once,
    with no metadata reload: the grant of a restart."""
    rpc = rpcs[node.get_id()]
    names = _rpc_names(rpcs, node)
    assert names.index("bdev_examine") < names.index("bdev_lvol_set_leader")
    rpc.bdev_examine.assert_called_once_with("raid0_1")
    rpc.bdev_lvol_set_leader.assert_called_once_with("LVS_1", leader=True)
    rpc.bdev_lvol_update_lvstore.assert_not_called()


def _whole_writes(caplog):
    """Node ids the [NODE-WRITE] tripwire logged a full-object write of."""
    marker = "[NODE-WRITE] full-object write of "
    return [r.getMessage().split(marker, 1)[1].split()[0]
            for r in caplog.records if marker in r.getMessage()]


class TestLeaderRebuild:
    """ops.recreate_lvstore on a member of the ACTIVE triplet, the real rebuild
    (nothing of it patched): it works on that member's own triplet, whichever
    site it is on, and never touches the passive one."""

    def test_the_remote_primary_examines_and_is_granted(self, db, rpcs, hub, env, adopt, attached):
        cluster, a, b, owner = publish._layout(db, active_site=SITE_B)
        vol = publish._volume(db, cluster, owner, publish._pool(db, cluster), ns_id=1,
                              active_site=SITE_B, demoted=[SITE_A])
        b0, b1, b2 = (n.get_id() for n in b)
        _grant_takes(rpcs, b[0])
        publish._absent_until_created(rpcs[b0], vol.nqn)
        publish._examines(rpcs[b0], vol)
        events = []
        env.set_port.side_effect = lambda node, port, block=False, **kwargs: events.append(
            ("block" if block else "release", node.get_id()))
        rpcs[b1].nvmf_subsystem_listener_set_ana_state.side_effect = \
            lambda *args, **kwargs: events.append(("ana", kwargs["ana"])) or True

        assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=_fresh(db, owner)) is True

        _examined_then_granted(rpcs, b[0])
        assert stack._roles(rpcs[b0]) == ["primary"]
        assert _grants(rpcs, [*a, *b]) == [b0]
        # its own triplet is fenced and wired to its hublvol, every port released again
        assert sorted(env.blocked()) == sorted([b1, b2]) == sorted(env.unblocked())
        assert adopt.call_args.args[0].get_id() == b0 and adopt.call_args.args[1].get_id() == owner.get_id()
        assert stack._hub_callers(hub, "create_secondary_hublvol") == [b1]
        assert _wired(hub) == {b1: ("secondary", b0), b2: ("tertiary", b0)}
        # the tertiary's second path carries the LVS of the owner, not b0's own
        (failover,) = hub["add_hublvol_failover_path"].call_args_list
        assert [n.get_id() for n in failover.args] == [b2, b0, b1]
        assert failover.kwargs["lvs_node"].lvstore == "LVS_1"
        # the passive triplet is not probed, fenced, stamped or wired
        _untouched(rpcs, hub, adopt, env, a)
        assert _fresh(db, owner).lvstore_status == "ready"
        # ANA: the returning active primary is the optimized path again and its
        # secondary is flipped back before the window (the failback), then with
        # the tertiary once the hand-off is over
        assert publish._ana_calls(rpcs[b0], vol.nqn) == [(1, "optimized")]
        assert events.index(("ana", "non_optimized")) < events.index(("block", b1))
        assert publish._ana_calls(rpcs[b1], vol.nqn) == [(1, "non_optimized")] * 2
        assert publish._ana_calls(rpcs[b2], vol.nqn) == [(1, "non_optimized")]

    def test_a_failing_failback_does_not_abort_the_rebuild(self, db, rpcs, hub, env, adopt, attached):
        """The failback of the returning remote primary's secondary is
        housekeeping, like the home primary's: an error in it is logged and
        the rebuild goes on to the grant."""
        cluster, a, b, owner = publish._layout(db, active_site=SITE_B)
        vol = publish._volume(db, cluster, owner, publish._pool(db, cluster), ns_id=1,
                              active_site=SITE_B, demoted=[SITE_A])
        b0 = b[0].get_id()
        _grant_takes(rpcs, b[0])
        publish._absent_until_created(rpcs[b0], vol.nqn)
        publish._examines(rpcs[b0], vol)

        with patch.object(ops, "_sync_flip_active_peer", side_effect=RuntimeError("peer gone")) as flip:
            assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=_fresh(db, owner)) is True

        assert [c.args[0].get_id() for c in flip.call_args_list] == [b0]
        _examined_then_granted(rpcs, b[0])
        assert _grants(rpcs, [*a, *b]) == [b0]
        assert _fresh(db, owner).lvstore_status == "ready"
        assert publish._ana_calls(rpcs[b0], vol.nqn) == [(1, "optimized")]

    def test_an_acting_leader_is_demoted_before_the_grant(
            self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b0, b1, b2 = (n.get_id() for n in b)
        _transfer_ready(rpcs, b[1])
        rpcs[b1].jc_compression_get_status.return_value = False
        _grant_takes(rpcs, b[0])
        events = []
        grant = rpcs[b0].bdev_lvol_set_leader.side_effect
        rpcs[b0].bdev_lvol_set_leader.side_effect = \
            lambda *args, **kwargs: events.append(("grant", kwargs)) or grant(*args, **kwargs)
        rpcs[b1].bdev_lvol_set_leader.side_effect = \
            lambda *args, **kwargs: events.append(("demote", kwargs)) or True

        assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=_fresh(db, owner)) is True

        assert events == [("demote", {"leader": False, "bs_nonleadership": True}), ("grant", {"leader": True})]
        # fenced last, replication suspended, drained, demoted
        assert env.blocked() == [b2, b1] and sorted(env.unblocked()) == sorted([b1, b2])
        old = _rpc_names(rpcs, b[1])
        assert old.index("jc_disable_replication") < old.index("bdev_distrib_check_inflight_io") \
            < old.index("bdev_lvol_set_leader") < old.index("bdev_distrib_force_to_non_leader")
        _examined_then_granted(rpcs, b[0])
        assert _wired(hub) == {b1: ("secondary", b0), b2: ("tertiary", b0)}
        _untouched(rpcs, hub, adopt, env, a)

    def test_the_remote_secondary_takes_over(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b0, b1, b2 = (n.get_id() for n in b)
        env.down.add(b0)
        _grant_takes(rpcs, b[1])

        assert ops.recreate_lvstore(_fresh(db, b[1]), lvs_primary=_fresh(db, owner)) is True

        _examined_then_granted(rpcs, b[1])
        # an acting leader keeps the role of its slot
        assert stack._roles(rpcs[b1]) == ["secondary"]
        assert _grants(rpcs, [*a, *b]) == [b1]
        assert env.blocked() == [b2] == env.unblocked()
        assert adopt.call_args.args[0].get_id() == b1
        assert _wired(hub) == {b2: ("tertiary", b1)}
        hub["create_secondary_hublvol"].assert_not_called()
        hub["add_hublvol_failover_path"].assert_not_called()
        _untouched(rpcs, hub, adopt, env, [*a, b[0]])
        assert _fresh(db, owner).lvstore_status == "ready"

    def test_an_unanswered_leader_probe_aborts_the_rebuild(
            self, db, rpcs, hub, env, adopt):
        cluster, a, b, owner = publish._layout(db, active_site=SITE_B)
        vol = publish._volume(db, cluster, owner, publish._pool(db, cluster), ns_id=1,
                              active_site=SITE_B, demoted=[SITE_A])
        b0, b1, b2 = (n.get_id() for n in b)
        _update(db, owner, remote_instances_pending=[b0, b2])
        rpcs[b1].get_lvstore.side_effect = RPCException("no answer")

        with pytest.raises(Exception, match=f"leader detection RPC to peer {b1} failed"):
            ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=_fresh(db, owner))

        _nothing_granted(rpcs, [*a, *b])
        rpcs[b0].bdev_examine.assert_not_called()
        assert env.blocked() == [] and env.unblocked() == []
        # the acting leader, if b1 is one, keeps its paths: nothing was flipped
        assert publish._ana_calls(rpcs[b1], vol.nqn) == []
        # the owner is a live node of the passive site: its marker is given back
        assert _fresh(db, owner).lvstore_status == "ready"
        assert _fresh(db, owner).remote_instances_pending == [b0, b2]
        _untouched(rpcs, hub, adopt, env, a)

    def test_a_rebuild_clears_the_members_pending_mark(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        _update(db, owner, remote_instances_pending=[b[0].get_id(), b[2].get_id()])
        _grant_takes(rpcs, b[0])
        assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=_fresh(db, owner)) is True
        assert _fresh(db, owner).remote_instances_pending == [b[2].get_id()]

    def test_the_home_primary_works_on_the_home_triplet_only(self, db, rpcs, hub, env, adopt):
        # the same rule from the other side: a home-led LVS, the remote triplet is the passive one
        _, a, b, owner = _layout(db)
        a0, a1, a2 = (n.get_id() for n in a)
        _grant_takes(rpcs, a[0])

        assert ops.recreate_lvstore(_fresh(db, owner)) is True

        _examined_then_granted(rpcs, a[0])
        assert stack._roles(rpcs[a0]) == ["primary"]
        assert sorted(env.blocked()) == sorted([a1, a2]) == sorted(env.unblocked())
        adopt.assert_not_called()
        assert stack._hub_callers(hub, "recreate_hublvol") == [a0]
        assert stack._hub_callers(hub, "create_secondary_hublvol") == [a1]
        assert _wired(hub) == {a1: ("secondary", a0), a2: ("tertiary", a0)}
        (failover,) = hub["add_hublvol_failover_path"].call_args_list
        assert [n.get_id() for n in failover.args] == [a2, a0, a1] and failover.kwargs == {}
        _untouched(rpcs, hub, adopt, env, b)
        assert _fresh(db, owner).lvstore_status == "ready"

    def test_the_owner_record_is_never_written_whole(self, db, rpcs, hub, env, adopt, caplog):
        # The owner is a live node of the passive site: a full-object write of
        # the caller's copy would bring back what another flow changed on it.
        _, a, b, owner = _layout(db, active_site=SITE_B)
        callers_copy = _fresh(db, owner)
        _update(db, owner, remote_instances_pending=[b[2].get_id()])
        _grant_takes(rpcs, b[0])
        caplog.set_level(logging.INFO)
        caplog.clear()                                  # the seeding wrote every node whole
        # the tripwire is live: a full-object write of a node record is logged
        _fresh(db, a[2]).write_to_db(db.kv_store)
        assert _whole_writes(caplog) == [a[2].get_id()]
        caplog.clear()
        assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=callers_copy) is True
        assert _fresh(db, owner).remote_instances_pending == [b[2].get_id()]
        assert _fresh(db, owner).lvstore_status == "ready"
        # neither the owner nor the member that rebuilds it: both are live nodes
        # whose records other flows change under other locks
        assert not {owner.get_id(), b[0].get_id()} & set(_whole_writes(caplog))

    def test_the_rebuilding_member_is_never_written_whole(
            self, db, rpcs, hub, env, adopt, caplog):
        # b0 connects the journal of the other site (RPC round-trips) under the
        # locks of LVS_1 only. Its own record is changed by flows that hold
        # none of them - a move of the LVS it owns, a pending mark cleared on
        # it: what they write meanwhile must survive, field by field.
        _, a, b, owner = _layout(db, active_site=SITE_B)
        _grant_takes(rpcs, b[0])
        jm = RemoteJMDevice()
        jm.uuid = str(uuid.uuid4())
        jm.jm_bdev = "jm_a0"

        def _connect(node, *args, **kwargs):
            def _concurrent(fresh):
                fresh.lvs_active_site = SITE_A
                fresh.remote_instances_pending = [a[1].get_id()]
            db.atomic_update(_fresh(db, node), _concurrent)
            return [jm]

        caplog.set_level(logging.INFO)
        caplog.clear()                                  # the seeding wrote every node whole
        with patch.object(ops, "_connect_to_remote_jm_devs", side_effect=_connect) as connect:
            assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=_fresh(db, owner)) is True
        assert [c.args[0].get_id() for c in connect.call_args_list] == [b[0].get_id()]
        rebuilt = _fresh(db, b[0])
        assert [d.uuid for d in rebuilt.remote_jm_devices] == [jm.uuid]
        assert (rebuilt.lvs_active_site, rebuilt.remote_instances_pending) == (SITE_A, [a[1].get_id()])
        assert b[0].get_id() not in _whole_writes(caplog)

    def test_the_rebuild_uses_the_triplet_on_record(self, db, rpcs, hub, env, adopt):
        # The remote refs change under no lock of the rebuild (assign_remote_triplets):
        # the rebuild works on the owner it validated, never on a member that left.
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        b3 = stack._seed_node(db, cluster, "b3", SITE_B)
        b0, b1, b2 = (n.get_id() for n in b)
        callers_copy = _fresh(db, owner)
        _update(db, owner, remote_secondary_node_id=b2, remote_tertiary_node_id=b3.get_id())
        _grant_takes(rpcs, b[0])
        assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=callers_copy) is True
        assert sorted(env.blocked()) == sorted([b2, b3.get_id()])
        assert _wired(hub) == {b2: ("secondary", b0), b3.get_id(): ("tertiary", b0)}
        _untouched(rpcs, hub, adopt, env, [*a, b[1]])


def _verdicts_asked():
    """The disconnected verdicts asked of the ``env`` double, as ``(peer id,
    lvs_peer_ids, fresh)``."""
    return [(c.args[0].get_id(), c.kwargs.get("lvs_peer_ids"), c.kwargs.get("fresh", False))
            for c in ops._check_peer_disconnected.call_args_list]


class TestLeaderRebuildGuard:
    """Who may rebuild a sync LVS as its leader (recreate_lvstore): a member
    of its ACTIVE triplet, with no move in flight and not on the lost site; in
    a cluster activation only the primary of that triplet."""

    @pytest.fixture()
    def impl(self):
        with patch.object(ops, "_recreate_lvstore_impl", return_value=True) as impl:
            yield impl

    def _rebuild(self, db, node, owner, **kwargs):
        owner = _fresh(db, owner)
        if node.get_id() == owner.get_id():
            return ops.recreate_lvstore(owner, **kwargs)
        return ops.recreate_lvstore(_fresh(db, node), lvs_primary=owner, **kwargs)

    def _refused(self, db, node, owner, match=None, **kwargs):
        with pytest.raises(ops.LVSLeadershipElsewhereError, match=match):
            self._rebuild(db, node, owner, **kwargs)

    def test_led_from_home_only_the_home_triplet_may(self, db, env, impl):
        _, a, b, owner = _layout(db)
        for node in b:
            self._refused(db, node, owner, match="not a member of its active triplet")
        impl.assert_not_called()
        assert self._rebuild(db, a[0], owner) is True
        assert self._rebuild(db, a[1], owner) is True       # a takeover by the home secondary
        assert [(c.args[0].get_id(), c.kwargs["lvs_primary"] and c.kwargs["lvs_primary"].get_id())
                for c in impl.call_args_list] == [(a[0].get_id(), None), (a[1].get_id(), owner.get_id())]

    def test_led_remotely_only_the_remote_triplet_may(self, db, env, impl):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        for node in a:
            self._refused(db, node, owner, match="not a member of its active triplet")
        impl.assert_not_called()
        assert self._rebuild(db, b[0], owner) is True
        env.down.add(b[0].get_id())
        assert self._rebuild(db, b[1], owner) is True       # a takeover by the remote secondary
        assert [c.args[0].get_id() for c in impl.call_args_list] == [b[0].get_id(), b[1].get_id()]
        assert all(c.kwargs["lvs_primary"].get_id() == owner.get_id() for c in impl.call_args_list)

    @pytest.mark.parametrize("node, down, allowed", [
        (0, (), True),              # the primary, whoever else is connected
        (0, (1, 2), True),
        (1, (), False),             # the secondary next to a connected primary
        (1, (2,), False),
        (1, (0,), True),            # ... takes over from a disconnected one
        (2, (), False),             # the tertiary never
        (2, (0,), False),
        (2, (0, 1), False),
    ])
    def test_a_remote_member_is_held_to_its_position(
            self, db, env, impl, node, down, allowed):
        # restart Step 4 decides this before the rebuild waits for its locks;
        # the guard under them does not take its word for it
        _, a, b, owner = _layout(db, active_site=SITE_B)
        env.down.update(b[i].get_id() for i in down)
        if allowed:
            assert self._rebuild(db, b[node], owner) is True
            assert impl.call_args.args[0].get_id() == b[node].get_id()
        else:
            self._refused(db, b[node], owner, match="no takeover is due")
            impl.assert_not_called()

    def test_an_empty_primary_slot_allows_a_takeover(self, db, env, impl):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        _update(db, owner, remote_primary_node_id="")
        assert self._rebuild(db, b[1], owner) is True
        self._refused(db, b[2], owner, match="no takeover is due")
        assert _verdicts_asked() == []

    def test_the_takeover_verdict_is_taken_under_the_locks(self, db, env, impl):
        # about the primary, by the dispatch's own voters, and never the
        # verdict the dispatch cached: fresh
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b0, b1, b2 = (n.get_id() for n in b)
        held = []
        real_lock = ops._lvs_grant_lock

        @contextmanager
        def _lock(*args, **kwargs):
            with real_lock(*args, **kwargs):
                held.append(len(_verdicts_asked()))
                yield
                held.append(len(_verdicts_asked()))

        env.down.add(b0)
        with patch.object(ops, "_lvs_grant_lock", _lock):
            assert self._rebuild(db, b[1], owner) is True
        # none asked before the grant lock was held, one while it was
        assert held == [0, 1]
        assert _verdicts_asked() == [(b0, [b1, b2], True)]
        # the primary and the tertiary need no verdict
        ops._check_peer_disconnected.reset_mock()
        assert self._rebuild(db, b[0], owner) is True
        self._refused(db, b[2], owner, match="no takeover is due")
        assert _verdicts_asked() == []

    def test_the_home_triplet_keeps_the_callers_precondition(self, db, env, impl):
        # main's contract (restart Step 2 decides, nothing asks again): its
        # secondary and tertiary pass next to a connected primary, and the
        # guard asks no peer anything
        _, a, b, owner = _layout(db)
        assert self._rebuild(db, a[1], owner) is True
        assert self._rebuild(db, a[2], owner) is True
        assert [c.args[0].get_id() for c in impl.call_args_list] == [a[1].get_id(), a[2].get_id()]
        assert _verdicts_asked() == []

    @pytest.mark.parametrize("active_site, builder", [("", 0), (SITE_B, 3)])
    def test_an_activation_asks_for_no_verdict(self, db, env, impl, active_site, builder):
        _, a, b, owner = _layout(db, active_site=active_site)
        assert self._rebuild(db, [*a, *b][builder], owner, activation_mode=True) is True
        assert _verdicts_asked() == []

    @pytest.mark.parametrize("marker", ["moving:" + SITE_A, "moving:" + SITE_B])
    @pytest.mark.parametrize("activation", [False, True])
    def test_a_move_in_flight_refuses_everybody(self, db, env, impl, marker, activation):
        _, a, b, owner = _layout(db, active_site=marker)
        for node in (a[0], a[1], b[0], b[1]):
            self._refused(db, node, owner, match="being moved", activation_mode=activation)
        impl.assert_not_called()

    @pytest.mark.parametrize("activation", [False, True])
    def test_a_member_on_the_lost_site_is_refused(self, db, env, impl, activation):
        _, a, b, owner = _layout(db, active_site=SITE_B, lost_site=SITE_B)
        self._refused(db, b[0], owner, match="lost site", activation_mode=activation)
        impl.assert_not_called()

    def test_a_node_that_holds_no_instance_is_refused(self, db, env, impl):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        stranger = stack._seed_node(db, cluster, "b3", SITE_B)
        self._refused(db, stranger, owner, match="not a member")
        impl.assert_not_called()

    @pytest.mark.parametrize("active_site, builder, secondary", [("", 0, 1), (SITE_B, 3, 4)])
    def test_activation_builds_on_the_active_primary_only(self, db, env, impl, active_site, builder, secondary):
        _, a, b, owner = _layout(db, active_site=active_site)
        members = [*a, *b]
        self._refused(db, members[secondary], owner, match="activation", activation_mode=True)
        impl.assert_not_called()
        assert self._rebuild(db, members[builder], owner, activation_mode=True) is True
        assert impl.call_args.args[0].get_id() == members[builder].get_id()
        assert impl.call_args.kwargs["activation_mode"] is True

    def test_the_rebuild_gets_the_owner_as_validated(self, db, env, impl):
        # not the caller's copy: its triplet may have changed since it was read
        _, a, b, owner = _layout(db, active_site=SITE_B)
        callers_copy = _fresh(db, owner)
        _update(db, owner, remote_secondary_node_id=b[2].get_id(), remote_tertiary_node_id="")
        assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=callers_copy) is True
        passed = impl.call_args.kwargs["lvs_primary"]
        assert passed is not callers_copy
        assert ops.remote_triplet_refs(passed) == (b[0].get_id(), b[2].get_id(), "")
        # a member that left the triplet since the caller's read is no member any more
        self._refused(db, b[1], owner, match="not a member")

    @pytest.mark.parametrize("failure", [False, RPCException("spdk gone")])
    def test_a_failed_rebuild_gives_the_owner_its_status_back(self, db, env, impl, failure):
        _, a, b, owner = _layout(db, active_site=SITE_B)

        def _fails(snode, **kwargs):
            ops._set_lvstore_status_atomic(owner.get_id(), "in_creation", db)
            if isinstance(failure, Exception):
                raise failure
            return failure
        impl.side_effect = _fails

        def _rebuild_fails():
            if isinstance(failure, Exception):
                with pytest.raises(RPCException):
                    self._rebuild(db, b[0], owner)
            else:
                assert self._rebuild(db, b[0], owner) is False

        _rebuild_fails()
        assert _fresh(db, owner).lvstore_status == "ready"
        # a caller that holds the marker itself (restart Steps 2-3) keeps it
        ops._set_lvstore_status_atomic(owner.get_id(), "in_creation", db)
        _rebuild_fails()
        assert _fresh(db, owner).lvstore_status == "in_creation"


class TestTakeoverVerdictUnderTheLocks:
    """The guard of a remote secondary's takeover with the REAL
    _check_peer_disconnected (no ``env``, which replaces it): the verdict the
    dispatch cached - in the verdict cache and in the vote cache under it - is
    not the one the guard decides on."""

    @pytest.fixture()
    def impl(self):
        with patch.object(ops, "_recreate_lvstore_impl", return_value=True) as impl:
            yield impl

    @pytest.fixture()
    def votes(self, monkeypatch):
        """What the online peers say about a node's journal, ``(disconnected,
        total)``: the function below both caches.

        _check_peer_disconnected imports the monitor by name at call time, so
        the name is bound here to the module the patch is on: an earlier test
        may have left another object under it in sys.modules (see
        ``replaced_monitor``). The previous entry is back after the test."""
        monkeypatch.setitem(sys.modules, storage_node_monitor.__name__, storage_node_monitor)
        with patch.object(storage_node_monitor, "_count_data_plane_votes_uncached",
                          return_value=(0, 2)) as votes:
            yield votes

    @pytest.fixture()
    def replaced_monitor(self, monkeypatch):
        """What an earlier test can leave under the monitor's name in
        sys.modules: tests/integration/test_secondary_promotion.py puts a
        MagicMock there and never takes it out. Any attribute of it is truthy:
        "disconnected", without a vote."""
        stand_in = MagicMock(name="storage_node_monitor left by an earlier test")
        monkeypatch.setitem(sys.modules, storage_node_monitor.__name__, stand_in)
        return stand_in

    @staticmethod
    def _dispatch_says(db, owner, node):
        """Restart Step 4's question, as Step 4 asks it."""
        return ops._remote_member_leads_at_restart(_fresh(db, owner), _fresh(db, node), db)

    def test_a_primary_back_within_the_ttl_stops_takeover(self, db, impl, votes):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        votes.return_value = (2, 2)             # b0 is ONLINE on record, its journal gone for its peers
        assert self._dispatch_says(db, owner, b[1]) is True
        votes.return_value = (0, 2)             # ... and back
        # the control: the dispatch's question is answered from the cache
        assert self._dispatch_says(db, owner, b[1]) is True
        assert votes.call_count == 1

        with pytest.raises(ops.LVSLeadershipElsewhereError, match="no takeover is due"):
            ops.recreate_lvstore(_fresh(db, b[1]), lvs_primary=_fresh(db, owner))

        impl.assert_not_called()
        assert votes.call_count == 2
        assert votes.call_args.args[0].get_id() == b[0].get_id()
        # the fresh verdict replaced the cached one: the retry's dispatch gets it without a vote
        assert self._dispatch_says(db, owner, b[1]) is False
        assert votes.call_count == 2

    def test_a_primary_gone_within_the_ttl_allows_takeover(self, db, impl, votes):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        assert self._dispatch_says(db, owner, b[1]) is False        # cached: connected
        votes.return_value = (2, 2)

        assert ops.recreate_lvstore(_fresh(db, b[1]), lvs_primary=_fresh(db, owner)) is True

        assert impl.call_args.args[0].get_id() == b[1].get_id()
        assert votes.call_count == 2

    def test_a_primary_offline_on_record_needs_no_vote(self, db, impl, votes):
        # the mgmt short-circuit comes first and is never cached
        _, a, b, owner = _layout(db, active_site=SITE_B)
        _update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        peer_ids = [b[1].get_id(), b[2].get_id()]
        assert ops._check_peer_disconnected(_fresh(db, b[0]), lvs_peer_ids=peer_ids, fresh=True) is True

        assert ops.recreate_lvstore(_fresh(db, b[1]), lvs_primary=_fresh(db, owner)) is True

        votes.assert_not_called()

    def test_the_primary_itself_needs_no_vote(self, db, impl, votes):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        assert ops.recreate_lvstore(_fresh(db, b[0]), lvs_primary=_fresh(db, owner)) is True
        votes.assert_not_called()

    # replaced_monitor before votes: pytest sets fixtures of one scope up in the order they are requested
    def test_a_replaced_monitor_module_is_not_asked(self, db, impl, replaced_monitor, votes):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        assert self._dispatch_says(db, owner, b[1]) is False
        assert votes.call_count == 1
        replaced_monitor.is_node_data_plane_disconnected_quorum.assert_not_called()


# ---------------------------------------------------------------------------
# the restart of a member, by its position in the active triplet
# ---------------------------------------------------------------------------

@contextmanager
def _back_once_the_grant_lock_is_held(env, node):
    """``node`` is connected again by the time a leader rebuild holds its
    grant lock: it came back while the rebuild waited for its locks."""
    real_lock = ops._lvs_grant_lock

    @contextmanager
    def _lock(*args, **kwargs):
        with real_lock(*args, **kwargs):
            env.down.discard(node.get_id())
            yield

    with patch.object(ops, "_lvs_grant_lock", _lock):
        yield


def _built(rpcs, node):
    """``node`` built its instance of LVS_1 and examined it."""
    rpc = rpcs[node.get_id()]
    assert {c.kwargs["name"] for c in rpc.bdev_distrib_create.call_args_list} == {
        "distrib_11", "distrib_12"}
    rpc.bdev_examine.assert_called_once_with("raid0_1")


def _quiesced(env, nodes):
    """Exactly ``nodes`` were port-blocked, in that order, and released."""
    ids = [n.get_id() for n in nodes]
    assert env.blocked() == ids
    assert set(env.unblocked()) == set(ids)


class TestRestartByPosition:
    """ops.recreate_all_lvstores with the real rebuilds (stack._restart_for_real),
    LVS_1 led from site B: its triplet there restarts as a triplet on main
    does - the primary as the leader, the secondary taking over from a
    disconnected primary, everybody else a non-leader behind the node
    _rebuild_anchor names - and "nobody leads" blocks nothing."""

    def test_the_active_primary_rebuilds_the_lvs_as_its_leader(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b0, b1, b2 = (n.get_id() for n in b)
        _grant_takes(rpcs, b[0])

        ret, leader_rebuilds = stack._restart_for_real(db, b[0])

        assert ret is True and leader_rebuilds == [(b0, owner.get_id())]
        _examined_then_granted(rpcs, b[0])
        assert stack._roles(rpcs[b0]) == ["primary"]
        assert _grants(rpcs, [*a, *b]) == [b0]
        assert sorted(env.blocked()) == sorted([b1, b2]) == sorted(env.unblocked())
        assert _wired(hub) == {b1: ("secondary", b0), b2: ("tertiary", b0)}
        _untouched(rpcs, hub, adopt, env, a)
        assert _fresh(db, owner).lvstore_status == "ready"

    @pytest.mark.parametrize("primary_leads", [False, True])
    def test_the_active_secondary_stands_behind_its_primary(
            self, db, rpcs, hub, env, adopt, primary_leads):
        """Its primary is connected, whether it leads or not."""
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b0, b1, b2 = (n.get_id() for n in b)
        if primary_leads:
            rpcs.lead(b[0])

        ret, leader_rebuilds = stack._restart_for_real(db, b[1])

        assert ret is True and leader_rebuilds == []
        _built(rpcs, b[1])
        assert stack._roles(rpcs[b1]) == ["secondary"]
        # b0 is quiesced for the examine whether it leads or not
        _quiesced(env, [b[0]])
        assert _wired(hub) == {b1: ("secondary", b0)}
        _nothing_granted(rpcs, [*a, *b])
        adopt.assert_not_called()
        assert _fresh(db, owner).lvstore_status == "ready"

    def test_the_active_secondary_leads_without_its_primary(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b0, b1, b2 = (n.get_id() for n in b)
        env.down.add(b0)
        _grant_takes(rpcs, b[1])

        ret, leader_rebuilds = stack._restart_for_real(db, b[1])

        assert ret is True and leader_rebuilds == [(b1, owner.get_id())]
        _examined_then_granted(rpcs, b[1])
        assert stack._roles(rpcs[b1]) == ["secondary"]
        assert _grants(rpcs, [*a, *b]) == [b1]
        _quiesced(env, [b[2]])
        assert _wired(hub) == {b2: ("tertiary", b1)}
        _untouched(rpcs, hub, adopt, env, [*a, b[0]])
        assert _fresh(db, owner).lvstore_status == "ready"

    def test_a_returning_primary_stops_the_takeover(self, db, rpcs, hub, env, adopt):
        # Step 4 chooses the takeover before the leader rebuild waits for its
        # locks; a primary that is connected again once they are held is no
        # peer of that rebuild and would get a second leader next to it
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b0, b1, b2 = (n.get_id() for n in b)
        env.down.add(b0)
        _grant_takes(rpcs, b[1])

        with _back_once_the_grant_lock_is_held(env, b[0]):
            ret, leader_rebuilds = stack._restart_for_real(db, b[1])

        assert ret is False and leader_rebuilds == [(b1, owner.get_id())]
        _nothing_granted(rpcs, [*a, *b])
        # refused before anything was built, examined, fenced or wired
        rpcs[b1].bdev_distrib_create.assert_not_called()
        rpcs[b1].bdev_examine.assert_not_called()
        assert stack._roles(rpcs[b1]) == []
        assert env.blocked() == []
        for name, mock in hub.items():
            assert not mock.called, name
        adopt.assert_not_called()
        assert _fresh(db, owner).lvstore_status == "ready"

        # the restart task's retry decides afresh: a non-leader behind b0
        ret, leader_rebuilds = stack._restart_for_real(db, b[1])

        assert ret is True and leader_rebuilds == []
        _built(rpcs, b[1])
        assert stack._roles(rpcs[b1]) == ["secondary"]
        assert env.blocked() == [b0]
        assert _wired(hub) == {b1: ("secondary", b0)}
        _nothing_granted(rpcs, [*a, *b])
        assert _fresh(db, owner).lvstore_status == "ready"

    @pytest.mark.parametrize("down, behind", [((), 0), ((0,), 1), ((0, 1), None)])
    def test_the_active_tertiary_is_always_built_non_leader(
            self, db, rpcs, hub, env, adopt, down, behind):
        # behind the primary, behind the secondary when the primary is
        # disconnected, and with both disconnected it is BUILT all the same,
        # without a port block: a node that comes online without its instance
        # makes the later leader rebuild of its primary abort.
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b2 = b[2].get_id()
        env.down.update(b[i].get_id() for i in down)

        ret, leader_rebuilds = stack._restart_for_real(db, b[2])

        assert ret is True and leader_rebuilds == []
        _built(rpcs, b[2])
        assert stack._roles(rpcs[b2]) == ["tertiary"]
        _quiesced(env, [] if behind is None else [b[behind]])
        assert _wired(hub) == ({} if behind is None else {b2: ("tertiary", b[behind].get_id())})
        _nothing_granted(rpcs, [*a, *b])
        adopt.assert_not_called()

    @pytest.mark.parametrize("restarting, role", [(0, "secondary"), (1, "secondary"), (2, "tertiary")])
    @pytest.mark.parametrize("site_b_down", [False, True])
    def test_a_passive_member_stands_behind_the_active_primary(
            self, db, rpcs, hub, env, adopt, restarting, role, site_b_down):
        # restart Steps 1-3 of a home member (owner, secondary, tertiary):
        # nobody reports the leadership, and with the whole active triplet
        # disconnected nothing is quiesced
        _, a, b, owner = _layout(db, active_site=SITE_B)
        if site_b_down:
            env.down.update(n.get_id() for n in b)

        ret, leader_rebuilds = stack._restart_for_real(db, a[restarting])

        assert ret is True and leader_rebuilds == []
        _built(rpcs, a[restarting])
        assert stack._roles(rpcs[a[restarting].get_id()]) == [role]
        _quiesced(env, [] if site_b_down else [b[0]])
        # no hublvol across the sites
        for name, mock in hub.items():
            assert not mock.called, name
        adopt.assert_not_called()
        _nothing_granted(rpcs, [*a, *b])
        assert _fresh(db, owner).lvstore_status == "ready"

    @pytest.mark.parametrize("restarting, role", [(1, "secondary"), (2, "tertiary")])
    def test_a_leaderless_home_led_lvs_blocks_no_restart(
            self, db, rpcs, hub, env, adopt, restarting, role):
        # the state after a fail-back promote, before the first IO: Steps 2-3
        # as on main, behind the primary although it does not lead
        _, a, b, owner = _layout(db)

        ret, leader_rebuilds = stack._restart_for_real(db, a[restarting])

        assert ret is True and leader_rebuilds == []
        _built(rpcs, a[restarting])
        assert stack._roles(rpcs[a[restarting].get_id()]) == [role]
        _quiesced(env, [a[0]])
        assert _wired(hub)[a[restarting].get_id()] == (role, a[0].get_id())
        _nothing_granted(rpcs, [*a, *b])
        _untouched(rpcs, hub, adopt, env, b)
        assert _fresh(db, owner).lvstore_status == "ready"

    # --- a whole-site reboot: the tertiary first, then the leader

    def _hublvol_needs_an_instance(self, rpcs, hub):
        """connect_to_hublvol fails on a node that never examined the LVS, as
        the real one does (its pre-block controller attach needs no lvstore)."""
        def _connect(node, *args, **kwargs):
            if not kwargs.get("attach_only") and not rpcs[node.get_id()].bdev_examine.called:
                raise RPCException(f"{node.get_id()} holds no lvstore")
            return True
        hub["connect_to_hublvol"].side_effect = _connect

    @pytest.mark.parametrize("taker", [0, 1])
    def test_a_site_reboot_ends_with_the_tertiary_wired(
            self, db, rpcs, hub, env, adopt, taker):
        # b2 restarts with b0 and b1 disconnected, then b0 comes back (or b1,
        # which takes over from b0)
        _, a, b, owner = _layout(db, active_site=SITE_B)
        b2, leader = b[2].get_id(), b[taker].get_id()
        self._hublvol_needs_an_instance(rpcs, hub)
        env.down.update({b[0].get_id(), b[1].get_id()})

        ret, leader_rebuilds = stack._restart_for_real(db, b[2])
        assert ret is True and leader_rebuilds == []
        _built(rpcs, b[2])
        assert env.blocked() == [] and _wired(hub) == {}

        env.down.discard(leader)
        _grant_takes(rpcs, b[taker])
        ret, leader_rebuilds = stack._restart_for_real(db, b[taker])

        assert ret is True and leader_rebuilds == [(leader, owner.get_id())]
        _examined_then_granted(rpcs, b[taker])
        assert _grants(rpcs, [*a, *b]) == [leader]
        assert _wired(hub) == {b2: ("tertiary", leader)}
        assert _fresh(db, owner).lvstore_status == "ready"

    def test_an_unbuilt_tertiary_fails_the_leader_rebuild(
            self, db, rpcs, hub, env, adopt, no_sleep):
        """The tertiary is online but holds no instance of LVS_1."""
        # the reason the tertiary is never skipped (no_sleep: the abort of the
        # hand-off paces its SPDK kill with real sleeps)
        _, a, b, owner = _layout(db, active_site=SITE_B)
        self._hublvol_needs_an_instance(rpcs, hub)
        env.down.add(b[1].get_id())
        _grant_takes(rpcs, b[0])

        ret, leader_rebuilds = stack._restart_for_real(db, b[0])

        assert ret is False and leader_rebuilds == [(b[0].get_id(), owner.get_id())]
        assert _fresh(db, owner).lvstore_status == "ready"

    # --- a move in flight: nobody is rebuilt as a leader

    @pytest.mark.parametrize("restarting", [1, 3])          # a1: Step 2, b0: Step 4
    @pytest.mark.parametrize("answer", [RPCException("no answer"), None])
    def test_while_moving_a_silent_member_refuses_a_hosted_lvs(
            self, db, rpcs, hub, env, adopt, restarting, answer):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        members = [*a, *b]
        _goes_silent(rpcs, b[2], answer)

        ret, leader_rebuilds = stack._restart_for_real(db, members[restarting])

        assert ret is False and leader_rebuilds == []
        rpcs[members[restarting].get_id()].bdev_distrib_create.assert_not_called()
        assert env.blocked() == []
        _nothing_granted(rpcs, members)
        assert _fresh(db, owner).lvstore_status == "ready"

    @pytest.mark.parametrize("answer", [RPCException("no answer"), None])
    def test_while_moving_a_silent_member_refuses_the_owner(
            self, db, rpcs, hub, env, adopt, answer):
        # Step 1 re-raises, after it released the port of every other member
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _goes_silent(rpcs, b[2], answer)

        with pytest.raises(ops.LVSLeaderUnknownError, match="did not answer"):
            stack._restart_for_real(db, owner)

        rpcs[owner.get_id()].bdev_distrib_create.assert_not_called()
        assert env.blocked() == []
        assert set(env.unblocked()) == {n.get_id() for n in (a[1], a[2], *b)}
        _nothing_granted(rpcs, [*a, *b])

    @pytest.mark.parametrize("target, restarting, behind, role", [
        (SITE_B, 1, 0, "secondary"),        # a1 (Step 2) behind the source primary a0
        (SITE_B, 3, 0, "secondary"),        # b0 (Step 4): the target primary is no leader yet
        (SITE_B, 0, 1, "secondary"),        # a0 (Step 1) behind the next of its triplet
        (SITE_A, 3, 4, "secondary"),        # moving back: the source is the remote triplet
        (SITE_A, 2, 3, "tertiary"),
    ])
    def test_while_moving_a_member_restarts_behind_the_source(
            self, db, rpcs, hub, env, adopt, target, restarting, behind, role):
        """Nobody reports the leadership."""
        _, a, b, owner = _layout(db, active_site="moving:" + target)
        members = [*a, *b]

        ret, leader_rebuilds = stack._restart_for_real(db, members[restarting])

        assert ret is True and leader_rebuilds == []
        _built(rpcs, members[restarting])
        assert stack._roles(rpcs[members[restarting].get_id()]) == [role]
        _quiesced(env, [members[behind]])
        _nothing_granted(rpcs, members)
        adopt.assert_not_called()
        assert _fresh(db, owner).lvs_active_site == "moving:" + target
        assert _fresh(db, owner).lvstore_status == "ready"

    @pytest.mark.parametrize("restarting", [0, 1, 3])       # Steps 1, 2 and 4
    def test_while_moving_a_member_restarts_behind_the_leader(
            self, db, rpcs, hub, env, adopt, restarting):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        members = [*a, *b]
        rpcs.lead(b[1])

        ret, leader_rebuilds = stack._restart_for_real(db, members[restarting])

        assert ret is True and leader_rebuilds == []
        _built(rpcs, members[restarting])
        _quiesced(env, [b[1]])
        assert _grants(rpcs, members) == []


# ---------------------------------------------------------------------------
# the reconciler of a move nobody owns
# ---------------------------------------------------------------------------

@pytest.fixture()
def short_lock_wait():
    # a nested acquire of the grant lock would deadlock: fail fast instead
    with patch.object(ops.constants, "LVSTORE_MUTATION_LOCK_WAIT_SEC", 3):
        yield


class TestPromoteRunnerLookup:
    """The leader lookup of the promote runner's transfer step
    (tasks_runner_sync_promote._transfer) with the real lookup and the real
    hand-off. The lookup reconciles a move its task no longer owns and can
    settle the marker itself; it must not make a leader on the settled one."""

    @pytest.mark.parametrize("lost_by, transferring", [
        pytest.param("cancel", [], id="cancel"),
        pytest.param("cancel", ["LVS_1"], id="cancel, hand-off recorded"),
        pytest.param("stale lease", [], id="stale lease"),
    ])
    def test_a_task_that_lost_its_move_makes_no_leader(
            self, db, rpcs, hub, env, adopt, no_sleep, short_lock_wait, lost_by, transferring):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        task = _promote_task(
            db, cluster, owner, canceled=lost_by == "cancel", stale=lost_by == "stale lease",
            function_params={"lvs_names": ["LVS_1"], "owners": {"LVS_1": owner.get_id()},
                             "transferring": transferring})
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        _grant_takes(rpcs, b[0])            # a recovery grant on the taker WOULD take
        # A canceled task stops before the hand-off; a stale one tries it on
        # the settled marker and fails.
        raised = ((promote_runner._PromoteCanceled, "canceled") if lost_by == "cancel"
                  else (promote_runner._PromoteFailed, "leadership transfer to .* failed"))
        with patch.object(ops, "move_lvs_leadership", wraps=ops.move_lvs_leadership) as move, \
                pytest.raises(raised[0], match=raised[1]):
            promote_runner._transfer(task, "LVS_1", SITE_B)
        assert move.called == (lost_by != "cancel")
        # the lookup's reconcile finished the move by the hand-off: b0 has its
        # role and leads from its first IO ...
        assert _fresh(db, owner).lvs_active_site == SITE_B
        assert stack._roles(rpcs[b[0].get_id()]) == ["primary"]
        # ... and the lookup, now on a settled marker, granted nothing
        _nothing_granted(rpcs, [*a, *b])
        for node in (*a, *b):
            rpcs[node.get_id()].bdev_lvol_set_leader.assert_not_called()
            rpcs[node.get_id()].bdev_lvol_set_lvs_signal.assert_not_called()
        assert db.acquire_lvstore_lock(cluster.get_id(), "takeleader/LVS_1", "test") == (True, None)

    def test_a_task_that_owns_its_move_hands_off_from_nobody(
            self, db, rpcs, hub, env, adopt, no_sleep, short_lock_wait):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        task = _promote_task(
            db, cluster, owner,
            function_params={"lvs_names": ["LVS_1"], "owners": {"LVS_1": owner.get_id()}})
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        _grant_takes(rpcs, b[0])
        promote_runner._transfer(task, "LVS_1", SITE_B)
        assert _fresh(db, owner).lvs_active_site == SITE_B
        assert stack._roles(rpcs[b[0].get_id()]) == ["primary"]
        _nothing_granted(rpcs, [*a, *b])
        assert db.get_task_by_id(task.uuid).function_params["transferring"] == ["LVS_1"]


class TestReconcile:

    @pytest.mark.parametrize("leader_index, site", [(3, SITE_B), (1, SITE_A)])
    def test_an_abandoned_move_settles_on_the_actual_leader(
            self, db, rpcs, env, leader_index, site):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _promote_task(db, cluster, owner, stale=True)
        members = [*a, *b]
        rpcs.lead(members[leader_index])
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader.get_id() == members[leader_index].get_id()
        assert _fresh(db, owner).lvs_active_site == site
        assert _grants(rpcs, members) == []

    def test_reconciled_even_behind_a_cached_no_leader_verdict(self, db, rpcs, env):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        ttl_cache.no_leader_cache.put((cluster.get_id(), "LVS_1"), True)
        rpcs.lead(b[0])
        ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert _fresh(db, owner).lvs_active_site == SITE_B
        ttl_cache.no_leader_cache.put((cluster.get_id(), "LVS_1"), True)
        _update(db, owner, lvs_active_site="moving:" + SITE_A)
        assert snapshot_controller._find_lvs_leader(cluster.get_id(), "LVS_1", _home(db, a)) is not None
        assert _fresh(db, owner).lvs_active_site == SITE_B

    @pytest.mark.parametrize("leader_index, unknown_index", [(0, 5), (4, 2)])
    def test_an_unanswered_member_leaves_the_marker(self, db, rpcs, env, leader_index, unknown_index):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        members = [*a, *b]
        rpcs.lead(members[leader_index])
        rpcs[members[unknown_index].get_id()].get_lvstore.side_effect = RPCException("timeout")
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader.get_id() == members[leader_index].get_id()
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_a_failed_probe_is_never_read_as_no_leader(self, db, rpcs, hub, env, no_sleep):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        rpcs[b[2].get_id()].get_lvstore.side_effect = RPCException("timeout")
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _grants(rpcs, [*a, *b]) == []
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    @pytest.mark.parametrize("leader_index", [4, 5])
    def test_a_non_primary_target_leader_is_not_followed(
            self, db, rpcs, env, leader_index):
        # On the target site only the primary of its triplet - the move's taker
        # - may lead. Settling on b1 / b2 would open b0, alive and not
        # redirected to them, as the optimized path of every volume promoted
        # there. A non-primary leading on the SOURCE site is the state before
        # the move and is followed (test_an_abandoned_move_settles_on_the_actual_leader).
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        members = [*a, *b]
        rpcs.lead(members[leader_index])
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B
        _nothing_granted(rpcs, members)
        for node in members:
            rpcs[node.get_id()].bdev_lvol_set_lvs_opts.assert_not_called()
            rpcs[node.get_id()].bdev_lvol_set_leader.assert_not_called()
        assert env.blocked() == []

    def test_two_leaders_settle_nothing(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        rpcs.lead(a[0])
        rpcs.lead(b[0])
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_a_cached_disconnected_verdict_hides_no_leader(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_A)
        env.down.add(b[0].get_id())          # _check_peer_disconnected says gone
        rpcs.lead(b[0])                      # but it answers: it leads
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B
        assert _grants(rpcs, [*a, *b]) == []

    def test_a_leaderless_move_finishes_on_the_target_primary(
            self, db, rpcs, hub, env, adopt, short_lock_wait):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _promote_task(db, cluster, owner, status=JobSchedule.STATUS_DONE)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B
        assert _fresh(db, owner).lvs_active_site == SITE_B
        # handed off, not granted: the target primary gets its role and takes
        # the leadership itself on the first IO; the source site is stamped non-leader
        assert stack._roles(rpcs[b[0].get_id()]) == ["primary"]
        assert [stack._roles(rpcs[n.get_id()]) for n in a] == [["secondary"], ["secondary"], ["tertiary"]]
        _nothing_granted(rpcs, [*a, *b])
        for node in (*a, *b):
            rpcs[node.get_id()].bdev_lvol_set_leader.assert_not_called()
        assert env.blocked() and sorted(env.unblocked()) == sorted(env.blocked())

    def test_a_lost_marker_write_is_settled_again(
            self, db, rpcs, hub, env, adopt, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B
        # the hand-off completed, its marker write did not: nobody leads yet
        _update(db, owner, lvs_active_site="moving:" + SITE_B)
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B
        assert _fresh(db, owner).lvs_active_site == SITE_B
        # the same hand-off once more, to the same taker
        assert stack._roles(rpcs[b[0].get_id()]) == ["primary", "primary"]
        assert adopt.call_count == 2
        _nothing_granted(rpcs, [*a, *b])
        assert sorted(env.unblocked()) == sorted(env.blocked())

    def test_a_target_without_quorum_keeps_the_marker(
            self, db, rpcs, hub, env, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        _journal_ready(rpcs, b[0], local=False)
        # neither the target secondary nor the source takes it: retried later
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _grants(rpcs, [*a, *b]) == []
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_a_move_taker_must_be_a_triplet_primary(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        with pytest.raises(ops.LVSMoveChangedError, match="not the primary"):
            ops.move_lvs_leadership(owner.get_id(), "moving:" + SITE_B, SITE_B, taker_id=b[1].get_id())
        assert not rpcs[b[1].get_id()].method_calls

    def test_a_fenced_lost_target_hands_back_to_the_source(
            self, db, rpcs, hub, env, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B, lost_site=SITE_B,
                                 lost_site_state="done")
        env.down.update(n.get_id() for n in b)
        for node in b:
            rpcs[node.get_id()].get_lvstore.side_effect = RPCException("site lost")
        for node in a:
            _journal_ready(rpcs, node)
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_A
        assert _fresh(db, owner).lvs_active_site == SITE_A
        assert stack._roles(rpcs[a[0].get_id()]) == ["primary"]
        _nothing_granted(rpcs, [*a, *b])

    def test_a_stale_disconnected_verdict_skips_no_member(
            self, db, rpcs, hub, env, adopt, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        env.down.add(a[0].get_id())          # cached "gone", but it answers the probe
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B
        assert a[0].get_id() in env.blocked()
        assert stack._roles(rpcs[a[0].get_id()]) == ["secondary"]
        _nothing_granted(rpcs, [*a, *b])

    def test_a_silent_member_on_the_old_site_blocks_the_move(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        rpcs[a[2].get_id()].get_lvstore.side_effect = RPCException("timeout")
        with pytest.raises(ops.LVSMoveChangedError, match="does not answer"):
            ops.move_lvs_leadership(owner.get_id(), "moving:" + SITE_B, SITE_B, taker_id=b[0].get_id())
        assert _grants(rpcs, [*a, *b]) == []
        assert env.blocked() == []

    def test_a_leader_after_the_scan_stops_the_hand_off(
            self, db, rpcs, hub, env, adopt, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        # a1 answers "not leading" to the reconciler's scan, then self-promotes
        rpcs[a[1].get_id()].get_lvstore.side_effect = [
            {"lvs leadership": False}, {"lvs leadership": True}]
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _grants(rpcs, [*a, *b]) == []
        assert env.blocked() == []
        rpcs[b[0].get_id()].bdev_lvol_set_lvs_opts.assert_not_called()
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_a_failed_hand_off_leaves_the_marker(self, db, rpcs, hub, env, adopt, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        # the taker's role stamp does not land: it would never take the leadership
        stamp = rpcs[b[0].get_id()].bdev_lvol_set_lvs_opts
        stamp.return_value = False
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B
        stamp.assert_called_once()
        adopt.assert_not_called()
        _nothing_granted(rpcs, [*a, *b])
        rpcs[b[0].get_id()].bdev_lvol_set_leader.assert_not_called()
        assert env.blocked() and sorted(env.unblocked()) == sorted(env.blocked())

    def test_a_live_promote_task_keeps_its_move(self, db, rpcs, env):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        task = _promote_task(db, cluster, owner)
        rpcs.lead(b[0])
        # wrong order: the runner reconciles before finishing its task -> no-op
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B
        db.atomic_update(task, lambda t: setattr(t, "status", JobSchedule.STATUS_DONE))
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B

    def test_the_fenced_lost_site_counts_as_not_leading(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B, lost_site=SITE_A,
                                 lost_site_state="done")
        for node in a:
            rpcs[node.get_id()].get_lvstore.side_effect = RPCException("site lost")
        rpcs.lead(b[0])
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B

    def test_a_lost_site_still_fencing_is_unknown(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B, lost_site=SITE_A,
                                 lost_site_state="fencing")
        for node in a:
            rpcs[node.get_id()].get_lvstore.side_effect = RPCException("site lost")
        rpcs.lead(b[0])
        assert ops.reconcile_lvs_move(owner.get_id()) is None

    def test_a_move_whose_marker_changed_touches_nothing(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        with pytest.raises(ops.LVSMoveChangedError):
            ops.move_lvs_leadership(owner.get_id(), "moving:" + SITE_B, SITE_B, taker_id=b[0].get_id())
        assert not rpcs[b[0].get_id()].method_calls

    def test_a_move_from_a_stale_leader_touches_nothing(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        with pytest.raises(ops.LVSMoveChangedError, match="expected"):
            ops.move_lvs_leadership(owner.get_id(), "moving:" + SITE_B, SITE_B,
                                    current_leader_id=a[0].get_id(), taker_id=b[0].get_id())
        # probed only: nothing reloaded, stamped or granted on the taker, nothing fenced
        assert {c[0] for c in rpcs[b[0].get_id()].method_calls} == {"get_lvstore"}
        assert env.blocked() == []
        rpcs[a[0].get_id()].bdev_lvol_set_leader.assert_not_called()

    # the move itself (ops.move_lvs_leadership): a hand-off without a grant,
    # then one more leadership probe before the marker settles

    def _move(self, owner, target, taker, *, leader=None):
        ops.move_lvs_leadership(owner.get_id(), "moving:" + target, target,
                                current_leader_id=leader.get_id() if leader is not None else None,
                                taker_id=taker.get_id())

    def test_a_move_from_a_live_leader_grants_nothing(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _transfer_ready(rpcs, a[0])
        _demote_takes(rpcs, a[0])
        self._move(owner, SITE_B, b[0], leader=a[0])
        assert _fresh(db, owner).lvs_active_site == SITE_B
        # the old leader demoted once; no other set_leader anywhere, no reload
        demote = call("LVS_1", leader=False, bs_nonleadership=True)
        for node in (*a, *b):
            assert rpcs[node.get_id()].bdev_lvol_set_leader.call_args_list == (
                [demote] if node.get_id() == a[0].get_id() else [])
        _nothing_granted(rpcs, [*a, *b])
        # the taker has its role, the old site is stamped non-leader
        assert stack._roles(rpcs[b[0].get_id()]) == ["primary"]
        assert [stack._roles(rpcs[n.get_id()]) for n in a] == [["secondary"], ["secondary"], ["tertiary"]]
        assert adopt.call_args.args[0].get_id() == b[0].get_id()
        assert env.blocked()[-1] == a[0].get_id()
        assert sorted(env.unblocked()) == sorted(env.blocked())

    def test_the_unblock_guard_reads_the_flag_the_move_remembered(
            self, db, rpcs, hub, env, adopt, monkeypatch):
        """Every port the hand-off releases asks the two-node unblock guard;
        it answers from the cluster the move read before the fence, never with
        a database read inside the port-block window."""
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _transfer_ready(rpcs, a[0])
        _demote_takes(rpcs, a[0])
        monkeypatch.setattr(ops, "_ARBITRATION_FLAG", {})
        real, cached = ops._cluster_two_node_arbitrated, []

        def _guard_flag(cluster_id, cluster=None, db=None):
            if cluster is None:
                cached.append(cluster_id in ops._ARBITRATION_FLAG)
            return real(cluster_id, cluster=cluster, db=db)

        monkeypatch.setattr(ops, "_cluster_two_node_arbitrated", _guard_flag)
        self._move(owner, SITE_B, b[0], leader=a[0])
        assert len(env.unblocked()) >= 2
        assert cached and all(cached), cached

    def test_an_old_leader_that_still_leads_fails_the_move(
            self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _transfer_ready(rpcs, a[0])          # it reports the leadership whatever it is sent
        with pytest.raises(ops.LVSMoveChangedError, match="after the hand-off"):
            self._move(owner, SITE_B, b[0], leader=a[0])
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B
        # refused after the hand-off, not before it
        assert stack._roles(rpcs[b[0].get_id()]) == ["primary"]
        _nothing_granted(rpcs, [*a, *b])
        assert env.blocked() and sorted(env.unblocked()) == sorted(env.blocked())

    @pytest.mark.parametrize("promoted_index, settled_site", [(3, SITE_B), (1, None)])
    def test_a_self_promotion_mid_hand_off_fails_the_move(
            self, db, rpcs, hub, env, adopt, promoted_index, settled_site):
        # B -> A from nobody: the previous taker (b0) armed its promotion before
        # the hand-off, which sends it no demote; a member of the taker's own
        # site (a1) that turns up leading is refused the same way.
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_A)
        promoted = [*a, *b][promoted_index]
        _during_hand_off(rpcs, a[0], lambda: rpcs.lead(promoted))
        with pytest.raises(ops.LVSMoveChangedError, match="after the hand-off"):
            self._move(owner, SITE_A, a[0])
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_A
        assert stack._roles(rpcs[a[0].get_id()]) == ["primary"]
        _nothing_granted(rpcs, [*a, *b])
        assert env.blocked() and sorted(env.unblocked()) == sorted(env.blocked())
        # the reconciler then follows a leader on the other site (the taker
        # stays behind, stamped primary, on a site ANA keeps closed); one next
        # to the taker on its own site is not followed - the marker stays
        assert ops.reconcile_lvs_move(owner.get_id()) == settled_site
        assert _fresh(db, owner).lvs_active_site == (settled_site or "moving:" + SITE_A)
        _nothing_granted(rpcs, [*a, *b])

    def test_a_leader_beside_the_taker_keeps_ana_closed(
            self, db, rpcs, hub, env, adopt, short_lock_wait):
        # B -> A from nobody, a1 turns up leading during the hand-off: a0 is
        # stamped primary and is the hub, with no redirect to a1 - its first IO
        # would make it a second leader. The marker must not settle on A while
        # a1 leads: the retry of the promote would open a0 as the optimized path.
        cluster, a, b, owner = publish._layout(db, active_site="moving:" + SITE_A)
        vol = publish._volume(db, cluster, owner, publish._pool(db, cluster), ns_id=1,
                              active_site=SITE_B, demoted=[SITE_B])
        a0, a1 = a[0].get_id(), a[1].get_id()
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        _during_hand_off(rpcs, a[0], lambda: rpcs.lead(a[1]))
        with pytest.raises(ops.LVSMoveChangedError, match="after the hand-off"):
            self._move(owner, SITE_A, a[0])
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_A
        # the promote's retry: still a move in flight, and the open step refuses
        assert src.promote_decision(
            vol, _fresh(db, owner), [vol], SITE_A, online_sites={SITE_A, SITE_B},
            online_nodes={n.get_id() for n in (*a, *b)}, force=False).kind == src.PROMOTE_IN_PROGRESS
        with pytest.raises(promote_runner._PromoteFailed, match="is not led from"):
            promote_runner._open(cluster.get_id(), vol.get_id(), SITE_A)
        for node in (*a, *b):
            assert publish._ana_calls(rpcs[node.get_id()], vol.nqn) == []
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_B
        _nothing_granted(rpcs, [*a, *b])

        # The way out: a1 is restarted - a non-leader rebuild while the LVS is
        # moving - and no longer leads. The reconciler then finishes the move
        # on a0 by the hand-off, and only now does the open reach a0.
        rpcs[a1].get_lvstore.return_value = {"lvs leadership": False}
        stamp = rpcs[a0].bdev_lvol_set_lvs_opts
        stamp.side_effect, stamp.return_value = None, True
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_A
        assert stack._roles(rpcs[a0]) == ["primary", "primary"]
        promote_runner._open(cluster.get_id(), vol.get_id(), SITE_A)
        assert publish._ana_calls(rpcs[a0], vol.nqn) == [(1, "optimized")]
        assert publish._ana_calls(rpcs[a1], vol.nqn) == [(1, "non_optimized")]
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_A
        _nothing_granted(rpcs, [*a, *b])

    @pytest.mark.parametrize("answer", [RPCException("timeout"), None,
                                        RPCRemoteError(message="busy", code=-16)])
    def test_a_silent_member_on_the_other_site_fails_the_move(
            self, db, rpcs, hub, env, adopt, answer):
        # it answered "not leading" before the hand-off; no answer after it is
        # never read as "does not lead"
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_A)
        _during_hand_off(rpcs, a[0], lambda: _goes_silent(rpcs, b[2], answer))
        with pytest.raises(ops.LVSMoveChangedError, match="does not answer after the hand-off"):
            self._move(owner, SITE_A, a[0])
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_A
        assert stack._roles(rpcs[a[0].get_id()]) == ["primary"]
        _nothing_granted(rpcs, [*a, *b])
        assert env.blocked() and sorted(env.unblocked()) == sorted(env.blocked())

    @pytest.mark.parametrize("answer", [RPCException("timeout"), None,
                                        RPCRemoteError(message="busy", code=-16)])
    def test_a_silent_member_beside_the_taker_is_ignored(
            self, db, rpcs, hub, env, adopt, answer):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_A)
        _during_hand_off(rpcs, a[0], lambda: _goes_silent(rpcs, a[2], answer))
        self._move(owner, SITE_A, a[0])
        assert _fresh(db, owner).lvs_active_site == SITE_A
        _nothing_granted(rpcs, [*a, *b])

    def test_the_taker_is_not_probed_again_after_the_hand_off(self, db, rpcs, hub, env, adopt):
        # whether the taker leads yet does not decide the move: a taker that
        # took the leadership during the hand-off is where it belongs
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_A)
        _during_hand_off(rpcs, a[0], lambda: rpcs.lead(a[0]))
        self._move(owner, SITE_A, a[0])
        assert _fresh(db, owner).lvs_active_site == SITE_A
        # asked once, before the hand-off: no probe after it, no confirmation poll
        assert rpcs[a[0].get_id()].get_lvstore.call_count == 1
        _nothing_granted(rpcs, [*a, *b])
