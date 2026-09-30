"""The active triplet of a sync-replication LVS against the real
FoundationDB: every leader lookup (volume / snapshot / group snapshot /
snapshot delete / resize) probes and grants only inside the triplet that
leads the LVS, a leaderless one is recovered there, a restarting home member
never takes a leadership led from the other site, no grant runs while the
leadership is being moved, the fenced transfer to the other site's triplet
(RPC order, roles, hublvol, failures) and the reconciler of a move nobody
owns any more. Storage nodes (RPC) and the port-block / hublvol side effects
are mocked; the database and its locks never are. The pure rules are in
tests/unit/test_sync_replication_active_triplet.py.
"""
import datetime
import threading
import types
import uuid
from contextlib import ExitStack
from unittest.mock import patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import lvol_controller, snapshot_controller
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import snapshot_monitor
from simplyblock_core.utils import ttl_cache
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
_seed_cluster, _sites, _make_owner = stack._seed_cluster, stack._sites, stack._make_owner
_update, _fresh, _took_leadership = stack._update, stack._fresh, stack._took_leadership

# The fixtures of test_sync_replication_lvs_stack: real DB, one RPC mock per node, hublvol side effects
# and port-block / monitor side effects recorded.
db = stack.db
rpcs = stack.rpcs
hub = stack.hub
env = stack.env


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


def _journal_ready(rpcs, node, *, local=True):
    """``node`` reports its journal copies: two ready same-site ones
    (``local``) or only other-site ones."""
    status = ({"jm_x": True, "remote_jm_yn1": True, "remote_xs_jm_zn1": False} if local
              else {"jm_x": False, "remote_jm_yn1": False, "remote_xs_jm_zn1": True,
                    "remote_xs_jm_wn1": True})
    rpcs[node.get_id()].jc_get_jm_status.return_value = status


def _grant_takes(rpcs, node):
    """A set_leader(True) on ``node`` makes it report the leadership."""
    rpc = rpcs[node.get_id()]

    def _set_leader(lvs_name, leader=False, **kwargs):
        if leader:
            rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        return True
    rpc.bdev_lvol_set_leader.side_effect = _set_leader


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

    def test_volume_lookup_finds_the_remote_leader_from_the_home_list(self, db, rpcs, env):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[1])
        home = _home(db, a)
        leader, non_leaders = ops.find_leader_with_failover(home, "LVS_1")
        assert leader.get_id() == b[1].get_id()
        # the caller's own members come back as the non-leaders
        assert [n.get_id() for n in non_leaders] == [n.get_id() for n in home]
        for node in a:
            rpcs[node.get_id()].bdev_lvol_get_lvstores.assert_not_called()

    def test_create_on_leader_runs_on_the_remote_leader(self, db, rpcs, env):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        ran_on = []
        ok, leader, _ = ops.execute_on_leader_with_failover(
            _home(db, a), "LVS_1", lambda node: ran_on.append(node.get_id()) or True)
        assert ok and ran_on == [b[0].get_id()] and leader.get_id() == b[0].get_id()

    def test_snapshot_and_group_snapshot_lookup_find_the_remote_leader(self, db, rpcs, env):
        cluster, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[2])
        # snapshot create / clone / consistency-group snapshot all resolve
        # their leader through _find_lvs_leader with the home member list
        leader = snapshot_controller._find_lvs_leader(cluster.get_id(), "LVS_1", _home(db, a))
        assert leader.get_id() == b[2].get_id()

    def test_snapshot_delete_runs_phase_one_on_the_remote_leader(self, db, rpcs, env):
        _, a, b, _ = _layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        snap = types.SimpleNamespace(
            get_id=lambda: "snap-1", snap_bdev="LVS_1/SNAP_1", deletion_status="",
            instances=[], lvol=types.SimpleNamespace(lvs_name="LVS_1"))
        with patch.object(snapshot_monitor.snapshot_controller, "delete_bdev_absent_ok",
                          return_value=False) as delete:
            snapshot_monitor.process_snap_delete(snap, _fresh(db, a[0]), all_mini_lvols=[])
        assert delete.call_args.args[0].get_id() == b[0].get_id()

    def test_the_snapshot_monitor_cache_follows_a_site_switch_in_one_cycle(self, db, rpcs, env):
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
            rpcs[a[0].get_id()].bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
            rpcs.lead(b[0])
            _update(db, owner, lvs_active_site=SITE_B)
            snapshot_monitor.process_snap_delete(snap, _fresh(db, owner), all_mini_lvols=[],
                                                 leader_cache=cycle_cache)
        assert [c.args[0].get_id() for c in delete.call_args_list] == [a[0].get_id(), b[0].get_id()]

    def test_home_lookup_is_unchanged_on_a_home_led_lvs(self, db, rpcs, env):
        _, a, b, _ = _layout(db)
        rpcs.lead(a[0])
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader.get_id() == a[0].get_id()
        for node in b:
            rpcs[node.get_id()].bdev_lvol_get_lvstores.assert_not_called()

    def test_resize_without_a_leader_never_resizes_on_the_home_primary(self, db, rpcs, env):
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

    def test_a_leaderless_remote_led_lvs_is_granted_on_the_remote_primary(
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

    def test_a_remote_taker_without_local_journal_quorum_is_refused(self, db, rpcs, hub, env, no_sleep):
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
# a move in flight, owned by a live promote task
# ---------------------------------------------------------------------------

class TestMovingOwnedByPromote:

    @pytest.mark.parametrize("leader_index", [0, 4])
    def test_lookup_finds_the_leader_on_either_triplet_and_leaves_the_marker(
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

    def test_the_promote_task_blocks_recovery_by_node_and_by_lvs(self, db, env):
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

    def test_a_grant_in_progress_refuses_the_move_then_the_move_goes_through(
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

    def test_a_committed_move_makes_the_next_recovery_grant_nothing(self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner = _layout(db)
        _journal_ready(rpcs, a[0])
        assert _begin_move(db, cluster, owner, SITE_B) == []
        nodes = _home(db, a)
        assert ops._recover_leaderless_lvs(
            cluster.get_id(), nodes, "LVS_1", nodes[0], owner_id=owner.get_id()) is None
        assert _grants(rpcs, [*a, *b]) == []

    def test_a_move_owned_by_a_live_promote_task_makes_the_recovery_grant_nothing(
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

    def test_a_home_leader_rebuild_holds_the_lock_against_the_move(self, db, env):
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

    def test_the_marker_compare_and_set_reports_a_changed_value(self, db, env):
        _, _, _, owner = _layout(db, active_site="moving:" + SITE_B)
        assert ops.set_lvs_active_site(owner.get_id(), SITE_A, expect="moving:" + SITE_A) is False
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B
        assert ops.set_lvs_active_site(owner.get_id(), SITE_B, expect="moving:" + SITE_B) is True


# ---------------------------------------------------------------------------
# restart
# ---------------------------------------------------------------------------

class TestRestart:

    @pytest.mark.parametrize("marker", [SITE_B, "moving:" + SITE_B])
    def test_a_home_primary_comes_back_non_leader_behind_the_remote_leader(
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

    def test_a_home_secondary_does_not_take_over_a_remote_led_lvs(self, db, rpcs, env):
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

    def test_a_home_tertiary_rebuild_quiesces_the_remote_leader(self, db, rpcs, env):
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

    def test_a_home_led_lvs_restarts_as_before(self, db, rpcs, env):
        _, a, b, owner = _layout(db)
        with patch.object(ops, "recreate_lvstore", return_value=True) as leader_rebuild, \
                patch.object(ops, "recreate_lvstore_on_non_leader") as rebuild:
            assert ops._recreate_all_lvstores_serial(_fresh(db, owner)) is True
        leader_rebuild.assert_called_once()
        rebuild.assert_not_called()

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

def _transfer_ready(rpcs, leader):
    rpcs.lead(leader)
    rpc = rpcs[leader.get_id()]
    rpc.jc_get_jm_status.return_value = {"jm_x": True}
    rpc.jc_disable_replication.return_value = True
    rpc.bdev_distrib_check_inflight_io.return_value = False


@pytest.fixture()
def adopt():
    with patch.object(StorageNode, "adopt_hublvol", autospec=True, return_value="nqn") as m:
        yield m


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
    def test_a_failed_metadata_reload_grants_nothing_and_releases_every_port(
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

    def test_a_grant_that_does_not_take_aborts(self, db, rpcs, hub, env, adopt, no_sleep):
        _, a, b, owner = _layout(db)
        leader, taker = _fresh(db, a[0]), _fresh(db, b[0])
        _transfer_ready(rpcs, leader)       # the taker never reports the leadership
        with pytest.raises(ops.LeadershipTransferError, match="Failed to restore leadership"):
            ops.transfer_lvs_leadership(leader, taker, [_fresh(db, n) for n in (*a, b[1], b[2])],
                                        lvs_node=_fresh(db, owner))
        assert sorted(env.unblocked()) == sorted(env.blocked())
        assert not _took_leadership(rpcs[a[0].get_id()])

    def test_a_failed_restamp_of_the_old_leader_aborts_before_the_grant(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db)
        rpcs[a[0].get_id()].bdev_lvol_set_lvs_opts.return_value = False
        with pytest.raises(ops.LeadershipTransferError) as err:
            self._run(db, rpcs, a, b, owner)
        assert err.value.granted is False
        assert _grants(rpcs, [*a, *b]) == []
        rpcs[b[0].get_id()].bdev_lvol_set_lvs_opts.assert_not_called()
        assert sorted(env.unblocked()) == sorted(env.blocked())

    def test_a_confirmed_leader_is_fenced_despite_a_disconnected_verdict(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db)
        env.down.add(a[0].get_id())          # stale / cached verdict: "gone"
        leader, _ = self._run(db, rpcs, a, b, owner)
        assert env.blocked()[-1] == leader.get_id()
        rpcs[leader.get_id()].bdev_lvol_set_leader.assert_called_once_with(
            "LVS_1", leader=False, bs_nonleadership=True)

    def test_a_former_leader_is_stamped_non_leader_before_its_release(self, db, rpcs, hub, env, adopt):
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


# ---------------------------------------------------------------------------
# the reconciler of a move nobody owns
# ---------------------------------------------------------------------------

@pytest.fixture()
def short_lock_wait():
    # a nested acquire of the grant lock would deadlock: fail fast instead
    with patch.object(ops.constants, "LVSTORE_MUTATION_LOCK_WAIT_SEC", 3):
        yield


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
        rpcs[members[unknown_index].get_id()].bdev_lvol_get_lvstores.side_effect = RPCException("timeout")
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader.get_id() == members[leader_index].get_id()
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_a_failed_probe_is_never_read_as_no_leader(self, db, rpcs, hub, env, no_sleep):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        rpcs[b[2].get_id()].bdev_lvol_get_lvstores.side_effect = RPCException("timeout")
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _grants(rpcs, [*a, *b]) == []
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_two_leaders_settle_nothing(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        rpcs.lead(a[0])
        rpcs.lead(b[0])
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_a_cached_disconnected_verdict_does_not_hide_a_leader(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_A)
        env.down.add(b[0].get_id())          # _check_peer_disconnected says gone
        rpcs.lead(b[0])                      # but it answers: it leads
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B
        assert _grants(rpcs, [*a, *b]) == []

    def test_nobody_leads_the_move_is_finished_on_the_target_primary(
            self, db, rpcs, hub, env, adopt, short_lock_wait):
        cluster, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        _promote_task(db, cluster, owner, status=JobSchedule.STATUS_DONE)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        _grant_takes(rpcs, b[0])
        leader, _ = ops.find_leader_with_failover(_home(db, a), "LVS_1")
        assert leader.get_id() == b[0].get_id()
        assert _fresh(db, owner).lvs_active_site == SITE_B
        assert _grants(rpcs, [*a, *b]) == [b[0].get_id()]
        names = [c[0] for c in rpcs[b[0].get_id()].method_calls]
        assert names.index("bdev_lvol_update_lvstore") < names.index("bdev_lvol_set_leader")
        for node in a:
            rpcs[node.get_id()].bdev_lvol_set_leader.assert_not_called()

    def test_a_reachable_target_without_quorum_keeps_the_marker(
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

    def test_a_fenced_lost_target_site_grants_back_on_the_source(
            self, db, rpcs, hub, env, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B, lost_site=SITE_B,
                                 lost_site_state="done")
        env.down.update(n.get_id() for n in b)
        for node in b:
            rpcs[node.get_id()].bdev_lvol_get_lvstores.side_effect = RPCException("site lost")
        for node in a:
            _journal_ready(rpcs, node)
        _grant_takes(rpcs, a[0])
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_A
        assert _grants(rpcs, [*a, *b]) == [a[0].get_id()]
        assert _fresh(db, owner).lvs_active_site == SITE_A

    def test_a_stale_disconnected_verdict_does_not_skip_a_source_member(
            self, db, rpcs, hub, env, adopt, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        env.down.add(a[0].get_id())          # cached "gone", but it answers the probe
        _grant_takes(rpcs, b[0])
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B
        assert a[0].get_id() in env.blocked()
        assert stack._roles(rpcs[a[0].get_id()]) == ["secondary"]

    def test_a_silent_member_on_the_old_site_blocks_the_move(self, db, rpcs, hub, env, adopt):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        rpcs[a[2].get_id()].bdev_lvol_get_lvstores.side_effect = RPCException("timeout")
        with pytest.raises(ops.LVSMoveChangedError, match="does not answer"):
            ops.move_lvs_leadership(owner.get_id(), "moving:" + SITE_B, SITE_B, taker_id=b[0].get_id())
        assert _grants(rpcs, [*a, *b]) == []
        assert env.blocked() == []

    def test_a_leader_appearing_after_the_leaderless_verdict_gets_no_second_grant(
            self, db, rpcs, hub, env, adopt, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        # a1 answers "not leading" to the reconciler's scan, then self-promotes
        rpcs[a[1].get_id()].bdev_lvol_get_lvstores.side_effect = [
            [{"lvs leadership": False}], [{"lvs leadership": True}]]
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _grants(rpcs, [*a, *b]) == []
        assert env.blocked() == []
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

    def test_a_failed_grant_leaves_the_marker(self, db, rpcs, hub, env, adopt, short_lock_wait):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        for node in (*a, *b):
            _journal_ready(rpcs, node)
        rpcs[b[0].get_id()].bdev_lvol_update_lvstore.return_value = False
        assert ops.reconcile_lvs_move(owner.get_id()) is None
        assert _grants(rpcs, [*a, *b]) == []
        assert _fresh(db, owner).lvs_active_site == "moving:" + SITE_B

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
            rpcs[node.get_id()].bdev_lvol_get_lvstores.side_effect = RPCException("site lost")
        rpcs.lead(b[0])
        assert ops.reconcile_lvs_move(owner.get_id()) == SITE_B

    def test_a_lost_site_still_fencing_is_unknown(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B, lost_site=SITE_A,
                                 lost_site_state="fencing")
        for node in a:
            rpcs[node.get_id()].bdev_lvol_get_lvstores.side_effect = RPCException("site lost")
        rpcs.lead(b[0])
        assert ops.reconcile_lvs_move(owner.get_id()) is None

    def test_a_move_whose_marker_changed_touches_nothing(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site=SITE_B)
        with pytest.raises(ops.LVSMoveChangedError):
            ops.move_lvs_leadership(owner.get_id(), "moving:" + SITE_B, SITE_B, taker_id=b[0].get_id())
        assert not rpcs[b[0].get_id()].method_calls

    def test_a_move_with_a_stale_current_leader_touches_nothing(self, db, rpcs, env):
        _, a, b, owner = _layout(db, active_site="moving:" + SITE_B)
        with pytest.raises(ops.LVSMoveChangedError, match="expected"):
            ops.move_lvs_leadership(owner.get_id(), "moving:" + SITE_B, SITE_B,
                                    current_leader_id=a[0].get_id(), taker_id=b[0].get_id())
        # probed only: nothing reloaded, stamped or granted on the taker, nothing fenced
        assert {c[0] for c in rpcs[b[0].get_id()].method_calls} == {"bdev_lvol_get_lvstores"}
        assert env.blocked() == []
        rpcs[a[0].get_id()].bdev_lvol_set_leader.assert_not_called()
