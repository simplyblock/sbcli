"""Demote and planned promote on a sync-replication cluster, against the real
FoundationDB: the strict fence of a site's triplet and its record, the promote
decision table as the API answers it, the FN_SYNC_PROMOTE runner (gates, the
marker transaction, the hand-off, the opening on the promoted site, the
settling of its markers on every failure and cancel, resuming after a crash),
the volume create / clone claim against a ``moving:`` marker (interleaved
transactions), and the site-rule lock that serializes all of it with the ANA
writers of the other flows (lvol monitor repair, restart, failover).

Storage nodes are the stateful nvmf fake of
test_sync_replication_lvol_publish.py; the live gate
(``check_gate``, tested in test_sync_replication_status.py) and the fenced
hand-off (``move_lvs_leadership``, tested in
test_sync_replication_active_triplet.py) are mocked at their boundary. The
pure rules are in tests/unit/test_sync_replication_demote_promote.py.
"""
import threading
import time
import uuid
from unittest.mock import MagicMock, patch

import fdb
import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import lvol_controller, snapshot_controller, tasks_controller
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import (
    SyncAnaError, SyncGateError, SyncGroupMemberError, SyncLeadershipMovingError, SyncPromoteRefusedError,
    SyncReplicationUnsupportedError, SyncSiteOfflineError,
)
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import tasks_runner_sync_promote as runner
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
MOVING_B = "moving:" + SITE_B

db = stack.db
rpcs = stack.rpcs
env = stack.env
spdk = publish.spdk
_clean_leader_caches = publish._clean_leader_caches
_layout, _pool, _volume = publish._layout, publish._pool, publish._volume
_fresh, _update, _ip = stack._fresh, stack._update, publish._ip


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def gate():
    """The live planned gate: passes unless told otherwise."""
    with patch.object(src, "check_gate") as check:
        yield check


def _gate_fails(*_args, **_kwargs):
    raise SyncGateError("sync-replication planned", ["LVS_1: distrib_11 replica_unsynced"])


class _Moves:
    """The hand-off boundary: find_leader_with_failover answers ``leader``,
    move_lvs_leadership records its call and settles the marker as the real
    one does (compare-and-set) unless ``fail`` is set."""

    def __init__(self):
        self.leader = {}
        self.calls = []
        self.fail: dict = {}

    def find(self, nodes, lvs_name):
        return self.leader.get(lvs_name), []

    def move(self, owner_id, moving_value, final_site, *, current_leader_id=None, taker_id):
        owner = DBController().get_storage_node_by_id(owner_id)
        self.calls.append((owner.lvstore, current_leader_id, taker_id, moving_value, final_site))
        if owner.lvstore in self.fail:
            raise self.fail[owner.lvstore]
        assert ops.set_lvs_active_site(owner_id, final_site, expect=moving_value)


@pytest.fixture()
def moves():
    m = _Moves()
    with patch.object(ops, "find_leader_with_failover", side_effect=m.find), \
            patch.object(ops, "move_lvs_leadership", side_effect=m.move):
        yield m


def _registered(db, spdk, **layout):
    """a0 owns LVS_1 (home a0 a1 a2, remote b0 b1 b2); one volume served on
    site A, registered on all six paths of the nvmf fake."""
    cluster, a, b, owner = _layout(db, **layout)
    pool = _pool(db, cluster)
    vol = _volume(db, cluster, owner, pool)
    publish._register_everywhere(db, vol, owner, [*a, *b])
    vol.write_to_db(db.kv_store)
    return cluster, a, b, _fresh(db, owner), pool, db.get_lvol_by_id(vol.get_id())


def _another(db, spdk, cluster, owner, pool, a, b, **fields):
    vol = _volume(db, cluster, owner, pool, **fields)
    publish._register_everywhere(db, vol, owner, [*a, *b])
    vol.write_to_db(db.kv_store)
    return db.get_lvol_by_id(vol.get_id())


def _state(spdk, node, vol):
    return publish._seen(spdk, node, vol)


def _states(spdk, nodes, vol):
    return [_state(spdk, n, vol) for n in nodes]


def _ana_calls(spdk, node):
    """Every per-group ANA call a node received, as (nqn, nsid, state)."""
    return [(nqn, nsid, state) for (nqn, _ip, nsid), state in spdk[node.get_id()].groups.items()]


def _task(db, task_id):
    return db.get_task_by_id(task_id)


def _run(db, task_id):
    return runner.task_runner(_task(db, task_id))


# ---------------------------------------------------------------------------
# demote
# ---------------------------------------------------------------------------

class TestDemote:

    def test_fences_exactly_the_site_triplet_of_this_lvs_then_records(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        before_b = _states(spdk, b, vol)
        assert src.sync_demote_lvol(vol.get_id(), SITE_A) == [vol.get_id()]
        assert _states(spdk, a, vol) == ["inaccessible"] * 3
        assert _states(spdk, b, vol) == before_b
        gate.assert_called_once_with(cluster.get_id())
        assert db.get_lvol_by_id(vol.get_id()).sync_demoted_sites == [SITE_A]

    def test_a_volume_not_served_on_the_site_is_a_no_op(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        calls = {n.get_id(): dict(spdk[n.get_id()].groups) for n in (*a, *b)}
        assert src.sync_demote_lvol(vol.get_id(), SITE_B) == []
        gate.assert_not_called()
        assert {n.get_id(): spdk[n.get_id()].groups for n in (*a, *b)} == calls
        assert src.sync_demote_lvol(vol.get_id(), SITE_A) == [vol.get_id()]
        assert src.sync_demote_lvol(vol.get_id(), SITE_A) == []     # idempotent

    def test_a_failed_gate_fences_and_records_nothing(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        gate.side_effect = _gate_fails
        with pytest.raises(SyncGateError):
            src.sync_demote_lvol(vol.get_id(), SITE_A)
        assert _state(spdk, a[0], vol) == "optimized"
        assert db.get_lvol_by_id(vol.get_id()).sync_demoted_sites == []

    @pytest.mark.parametrize("failure", ["false", "raise"])
    def test_one_failed_ana_rpc_leaves_the_demote_unrecorded(self, db, spdk, gate, failure):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        target = spdk[a[2].get_id()]
        effect = (MagicMock(return_value=False) if failure == "false"
                  else MagicMock(side_effect=RPCException("timeout")))
        with patch.object(target, "nvmf_subsystem_listener_set_ana_state", effect):
            with pytest.raises(SyncAnaError):
                src.sync_demote_lvol(vol.get_id(), SITE_A)
        assert db.get_lvol_by_id(vol.get_id()).sync_demoted_sites == []

    def test_an_offline_member_is_skipped_an_unreachable_one_must_answer(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        _update(db, a[1], status=StorageNode.STATUS_OFFLINE)
        _update(db, a[2], status=StorageNode.STATUS_UNREACHABLE)
        with patch.object(spdk[a[2].get_id()], "nvmf_subsystem_listener_set_ana_state",
                          MagicMock(side_effect=RPCException("unreachable"))):
            with pytest.raises(SyncAnaError, match=a[2].get_id()):
                src.sync_demote_lvol(vol.get_id(), SITE_A)
        assert src.sync_demote_lvol(vol.get_id(), SITE_A) == [vol.get_id()]
        assert _state(spdk, a[1], vol) == "non_optimized"      # SPDK down: its restart closes it
        assert _state(spdk, a[2], vol) == "inaccessible"

    def test_a_restart_after_the_demote_keeps_the_path_closed(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        src.sync_demote_lvol(vol.get_id(), SITE_A)
        ops._set_lvol_ana_on_node(vol, a[1], "non_optimized")      # stale caller copy of the volume
        assert _state(spdk, a[1], vol) == "inaccessible"

    def test_group_demote_fences_every_member(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        vol2 = _another(db, spdk, cluster, owner, pool, a, b)
        group = _group(db, cluster, [vol, vol2])
        assert src.sync_demote_group(group.get_id(), SITE_A) == [vol.get_id(), vol2.get_id()]
        gate.assert_called_once()
        for v in (vol, vol2):
            assert _states(spdk, a, v) == ["inaccessible"] * 3
            assert db.get_lvol_by_id(v.get_id()).sync_demoted_sites == [SITE_A]

    def test_group_with_an_unresolvable_member_touches_nothing(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        group = _group(db, cluster, [vol], extra_ids=["gone-" + uuid.uuid4().hex[:6]])
        with pytest.raises(SyncGroupMemberError) as exc:
            src.sync_demote_group(group.get_id(), SITE_A)
        assert exc.value.volumes[0].startswith("gone-")
        gate.assert_not_called()
        assert _state(spdk, a[0], vol) == "optimized"


def _group(db, cluster, members, extra_ids=()):
    group = ConsistencyGroup()
    group.uuid = str(uuid.uuid4())
    group.cluster_id = cluster.get_id()
    group.group_name = f"cg-{group.uuid[:6]}"
    group.members = {m.get_id(): {"joined_seq": 1, "removed_seq": 0} for m in members}
    group.members.update({i: {"joined_seq": 1, "removed_seq": 0} for i in extra_ids})
    group.write_to_db(db.kv_store)
    return group


# ---------------------------------------------------------------------------
# promote: the API answers
# ---------------------------------------------------------------------------

class TestPromoteApi:

    def test_a_volume_served_on_the_site_answers_its_paths_there(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        result = src.sync_promote_lvol(vol.get_id(), SITE_A)
        assert not result.in_progress
        assert [e.ip for e in result.connection_strings[vol.get_id()]] == [_ip(n) for n in a]
        gate.assert_not_called()

    def test_not_demoted_on_t_is_refused(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        with pytest.raises(SyncPromoteRefusedError) as exc:
            src.sync_promote_lvol(vol.get_id(), SITE_B)
        assert exc.value.volumes == [vol.get_id()]
        assert db.get_sync_promote_task(cluster.get_id(), "LVS_1") is None

    def test_other_volumes_of_the_lvs_still_on_t_are_listed(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        other = _another(db, spdk, cluster, owner, pool, a, b)
        src.sync_demote_lvol(vol.get_id(), SITE_A)
        with pytest.raises(SyncPromoteRefusedError) as exc:
            src.sync_promote_lvol(vol.get_id(), SITE_B)
        assert exc.value.volumes == [other.get_id()]

    def test_t_offline_is_412_and_forced_is_not_available_yet(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        for node in a:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        with pytest.raises(SyncSiteOfflineError):
            src.sync_promote_lvol(vol.get_id(), SITE_B)
        with pytest.raises(SyncReplicationUnsupportedError, match="disaster"):
            src.sync_promote_lvol(vol.get_id(), SITE_B, force=True)

    def test_forced_while_t_is_online_is_refused(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        src.sync_demote_lvol(vol.get_id(), SITE_A)
        with pytest.raises(SyncPromoteRefusedError, match="online"):
            src.sync_promote_lvol(vol.get_id(), SITE_B, force=True)

    def test_a_failed_gate_is_409_and_queues_nothing(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        src.sync_demote_lvol(vol.get_id(), SITE_A)
        gate.side_effect = _gate_fails
        with pytest.raises(SyncGateError):
            src.sync_promote_lvol(vol.get_id(), SITE_B)
        assert db.get_sync_promote_task(cluster.get_id(), "LVS_1") is None

    def test_in_progress_while_the_task_runs_done_after(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        src.sync_demote_lvol(vol.get_id(), SITE_A)
        first = src.sync_promote_lvol(vol.get_id(), SITE_B)
        assert first.in_progress and first.task_id
        assert src.sync_promote_lvol(vol.get_id(), SITE_B) == first     # no second task
        assert len(db.get_active_sync_promote_tasks(cluster.get_id())) == 1
        moves.leader["LVS_1"] = owner
        assert _run(db, first.task_id)
        done = src.sync_promote_lvol(vol.get_id(), SITE_B)
        assert not done.in_progress
        assert [e.ip for e in done.connection_strings[vol.get_id()]] == [_ip(n) for n in b]

    def test_a_canceled_task_is_not_in_progress(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        src.sync_demote_lvol(vol.get_id(), SITE_A)
        first = src.sync_promote_lvol(vol.get_id(), SITE_B)
        assert tasks_controller.cancel_task(first.task_id)
        second = src.sync_promote_lvol(vol.get_id(), SITE_B)
        assert second.in_progress and second.task_id != first.task_id
        # the runner still sees the canceled one, to settle it
        assert {t.uuid for t in db.get_active_sync_promote_tasks(cluster.get_id())} == {
            first.task_id, second.task_id}

    def test_a_move_in_flight_without_a_task_is_in_progress(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        src.sync_demote_lvol(vol.get_id(), SITE_A)
        _update(db, owner, lvs_active_site=MOVING_B)
        assert src.sync_promote_lvol(vol.get_id(), SITE_B) == src.SyncPromoteResult(in_progress=True)

    def test_group_lvs_rule_covers_volumes_outside_the_group(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        outsider = _another(db, spdk, cluster, owner, pool, a, b)
        member2 = _another(db, spdk, cluster, owner, pool, a, b)
        group = _group(db, cluster, [vol, member2])
        src.sync_demote_group(group.get_id(), SITE_A)
        with pytest.raises(SyncPromoteRefusedError) as exc:
            src.sync_promote_group(group.get_id(), SITE_B)
        assert exc.value.volumes == [outsider.get_id()]
        src.sync_demote_lvol(outsider.get_id(), SITE_A)
        result = src.sync_promote_group(group.get_id(), SITE_B)
        task = _task(db, result.task_id)
        assert sorted(task.function_params["lvol_ids"]) == sorted([vol.get_id(), member2.get_id()])
        assert task.function_params["owners"] == {"LVS_1": owner.get_id()}


# ---------------------------------------------------------------------------
# promote: the runner
# ---------------------------------------------------------------------------

def _queued(db, spdk, gate, **layout):
    """A registered volume demoted on A and a promote to B queued."""
    cluster, a, b, owner, pool, vol = _registered(db, spdk, **layout)
    src.sync_demote_lvol(vol.get_id(), SITE_A)
    result = src.sync_promote_lvol(vol.get_id(), SITE_B)
    assert result.in_progress
    gate.reset_mock()
    return cluster, a, b, owner, pool, vol, result.task_id


class TestPromoteRunner:

    def test_planned_promote_moves_the_lvs_and_opens_the_volume_on_s(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        assert _run(db, task_id)
        task = _task(db, task_id)
        assert task.status == JobSchedule.STATUS_DONE and task.function_result.startswith("promoted")
        assert task.function_params["moves"] == {"LVS_1": ""}
        assert moves.calls == [("LVS_1", owner.get_id(), b[0].get_id(), MOVING_B, SITE_B)]
        assert gate.call_count == 3          # before the marker, the hand-off, the opening
        assert _fresh(db, owner).lvs_active_site == SITE_B
        fresh = db.get_lvol_by_id(vol.get_id())
        assert (fresh.sync_active_site, fresh.sync_demoted_sites) == (SITE_B, [SITE_A])
        assert _states(spdk, b, vol) == ["optimized", "non_optimized", "non_optimized"]
        assert _states(spdk, a, vol) == ["inaccessible"] * 3

    def test_the_gate_failing_before_the_marker_changes_nothing(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        gate.side_effect = _gate_fails
        _run(db, task_id)
        task = _task(db, task_id)
        assert task.status == JobSchedule.STATUS_DONE and "gate failed" in task.function_result
        assert task.function_params["moves"] == {}
        assert _fresh(db, owner).lvs_active_site == ""
        assert moves.calls == []

    def test_a_volume_created_after_the_enqueue_aborts_the_move(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        late = _volume(db, cluster, owner, pool)            # active on A, not demoted
        _run(db, task_id)
        task = _task(db, task_id)
        assert late.get_id() in task.function_result
        assert _fresh(db, owner).lvs_active_site == ""
        assert moves.calls == []

    def test_the_gate_failing_after_the_marker_restores_the_previous_site(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        gate.side_effect = [None, SyncGateError("sync-replication planned", ["late unsynced"])]
        _run(db, task_id)
        assert "late unsynced" in _task(db, task_id).function_result
        assert _fresh(db, owner).lvs_active_site == ""
        assert moves.calls == []
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_A

    def test_no_rpc_runs_inside_the_transaction(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        inside = threading.Event()
        real_tx = type(db)._begin_sync_promote_moves_tx

        def _tx(self, tr, *args, **kwargs):      # fdb.transactional finds ``tr`` by name
            inside.set()
            try:
                return real_tx(self, tr, *args, **kwargs)
            finally:
                inside.clear()

        def _no_rpc(node, *args, **kwargs):
            assert not inside.is_set(), "an RPC client was used inside the promote transaction"
            return spdk[node.get_id()]

        gate.side_effect = lambda *a, **k: _no_rpc(owner)
        with patch.object(type(db), "_begin_sync_promote_moves_tx", _tx), \
                patch.object(StorageNode, "rpc_client", new=_no_rpc):
            assert _run(db, task_id)
        assert _fresh(db, owner).lvs_active_site == SITE_B

    def test_leader_already_on_s_is_only_the_ana_step(self, db, spdk, gate, moves):
        """Another volume of the LVS was promoted before: only this one's
        paths open, no hand-off."""
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        _run(db, task_id)
        sibling = _another(db, spdk, cluster, owner, pool, a, b, active_site=SITE_A, demoted=[SITE_A])
        result = src.sync_promote_lvol(sibling.get_id(), SITE_B)
        moves.calls.clear()
        assert _run(db, result.task_id)
        assert moves.calls == []
        assert db.get_lvol_by_id(sibling.get_id()).sync_active_site == SITE_B
        assert _states(spdk, b, sibling) == ["optimized", "non_optimized", "non_optimized"]

    def test_an_ana_only_task_whose_gate_turned_false_opens_nothing(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        _run(db, task_id)
        sibling = _another(db, spdk, cluster, owner, pool, a, b, active_site=SITE_A, demoted=[SITE_A])
        result = src.sync_promote_lvol(sibling.get_id(), SITE_B)
        gate.side_effect = _gate_fails
        _run(db, result.task_id)
        assert "gate failed before opening" in _task(db, result.task_id).function_result
        assert _states(spdk, b, sibling) == ["inaccessible"] * 3
        assert db.get_lvol_by_id(sibling.get_id()).sync_active_site == SITE_A

    @pytest.mark.parametrize("status", [LVol.STATUS_IN_DELETION, LVol.STATUS_IN_CREATION])
    def test_a_volume_not_servable_at_the_pass_is_not_opened(self, db, spdk, gate, moves, status):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        db.atomic_update(vol, lambda v: setattr(v, "status", status))
        _run(db, task_id)
        assert status in _task(db, task_id).function_result
        assert _states(spdk, b, vol) == ["inaccessible"] * 3
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_A

    def test_a_delete_starting_during_the_open_closes_the_paths_again(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        target = spdk[b[2].get_id()]
        real = target.nvmf_subsystem_listener_set_ana_state

        def _delete_starts(nqn, ip, port, trtype="TCP", is_optimized=True, ana=None, anagrpid=None):
            db.atomic_update(vol, lambda v: setattr(v, "status", LVol.STATUS_IN_DELETION))
            return real(nqn, ip, port, trtype=trtype, ana=ana, anagrpid=anagrpid)

        with patch.object(target, "nvmf_subsystem_listener_set_ana_state", side_effect=_delete_starts):
            _run(db, task_id)
        assert "status changed during the open" in _task(db, task_id).function_result
        assert _states(spdk, b, vol) == ["inaccessible"] * 3
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_A

    def test_a_failed_close_after_a_lockless_status_change_is_reported(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        target = spdk[b[2].get_id()]
        real = target.nvmf_subsystem_listener_set_ana_state

        def _delete_starts_then_fails(nqn, ip, port, trtype="TCP", is_optimized=True, ana=None,
                                      anagrpid=None):
            if ana == "inaccessible":
                raise RPCException("node gone")
            db.atomic_update(vol, lambda v: setattr(v, "status", LVol.STATUS_IN_DELETION))
            return real(nqn, ip, port, trtype=trtype, ana=ana, anagrpid=anagrpid)

        with patch.object(target, "nvmf_subsystem_listener_set_ana_state",
                          side_effect=_delete_starts_then_fails):
            _run(db, task_id)
        assert "left servable" in _task(db, task_id).function_result
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_A

    def test_the_delete_transition_waits_for_an_open_and_keeps_its_record(self, db, spdk, gate, moves):
        """delete_lvol's in_deletion write takes the site-rule lock and is
        field-scoped: it lands after the open and its record, never between,
        and never reverts the promoted site."""
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        stale = db.get_lvol_by_id(vol.get_id())
        pause = _Pause(spdk, b[0], only_state="optimized")
        with patch.object(pause.target, "nvmf_subsystem_listener_set_ana_state", pause):
            promote = _thread(_run, db, task_id)
            assert pause.entered.wait(30)
            delete = _thread(lvol_controller._mark_in_deletion, db, stale, owner)
            time.sleep(1.0)
            assert db.get_lvol_by_id(vol.get_id()).status == LVol.STATUS_ONLINE
            pause.release.set()
            promote.join(60)
            delete.join(30)
        assert not promote.errors and not delete.errors
        stored = db.get_lvol_by_id(vol.get_id())
        assert (stored.status, stored.sync_active_site) == (LVol.STATUS_IN_DELETION, SITE_B)
        assert _task(db, task_id).function_result.startswith("promoted")

    def test_a_writer_keeps_a_volume_in_deletion_closed(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        db.atomic_update(vol, lambda v: setattr(v, "status", LVol.STATUS_IN_DELETION))
        ops._set_lvol_ana_on_node(vol, a[0], "optimized")
        assert _state(spdk, a[0], vol) == "inaccessible"

    def test_a_failed_open_records_nothing_and_the_next_call_retries_it(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        with patch.object(spdk[b[1].get_id()], "nvmf_subsystem_listener_set_ana_state",
                          MagicMock(return_value=False)):
            _run(db, task_id)
        assert "failed" in _task(db, task_id).function_result
        assert _fresh(db, owner).lvs_active_site == SITE_B      # the move itself stands
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_A
        again = src.sync_promote_lvol(vol.get_id(), SITE_B)
        assert _run(db, again.task_id)
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_B


class TestResume:
    """A pass that restarts after a crash continues from the DB state."""

    def _marked(self, db, gate, cluster, owner, task_id):
        task = _task(db, task_id)
        problems = db.begin_sync_promote_moves(
            task, {"LVS_1": (owner.get_id(), "")}, SITE_B,
            lambda c, o, v, e: src.promote_move_problems(c, o, v, e, SITE_B))
        assert problems == []
        assert _fresh(db, owner).lvs_active_site == MOVING_B

    def test_after_the_marker_transaction(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        self._marked(db, gate, cluster, owner, task_id)
        moves.leader["LVS_1"] = owner
        assert _run(db, task_id)
        assert gate.call_count == 2          # the resumed move right before its hand-off, the opening
        assert moves.calls[0][:3] == ("LVS_1", owner.get_id(), b[0].get_id())
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_B

    def test_after_the_marker_with_the_gate_now_failing(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        self._marked(db, gate, cluster, owner, task_id)
        gate.side_effect = _gate_fails
        _run(db, task_id)
        assert moves.calls == []
        assert _fresh(db, owner).lvs_active_site == ""

    def test_after_the_grant_before_the_marker_settled(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        self._marked(db, gate, cluster, owner, task_id)
        runner._save(_task(db, task_id), lambda t: runner._add_transferring(t, "LVS_1"))
        moves.leader["LVS_1"] = b[0]        # the grant happened
        assert _run(db, task_id)
        assert moves.calls == []
        assert _fresh(db, owner).lvs_active_site == SITE_B
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_B

    def test_after_the_move_before_the_open(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        self._marked(db, gate, cluster, owner, task_id)
        ops.set_lvs_active_site(owner.get_id(), SITE_B, expect=MOVING_B)
        assert _run(db, task_id)
        assert moves.calls == [] and gate.call_count == 1      # the opening only
        assert _states(spdk, b, vol) == ["optimized", "non_optimized", "non_optimized"]


# ---------------------------------------------------------------------------
# two LVS: every marker the task owns is settled
# ---------------------------------------------------------------------------

def _two_lvs(db, spdk, gate):
    """LVS_1 owned by a0 (remote b0 b1 b2), LVS_2 owned by a1 (home a1 a0 a2,
    remote b1 b2 b0); one volume on each, demoted on A, one group."""
    cluster, a, b, owner1 = _layout(db)
    owner2 = stack._make_owner(db, a[1], [a[1], a[0], a[2]], [b[1], b[2], b[0]], lvs_id=2)
    ports = {**_fresh(db, owner1).lvstore_ports, **owner2.lvstore_ports}
    for node in (*a, *b):
        _update(db, node, lvstore_ports=ports)
    owner1, owner2 = _fresh(db, owner1), _fresh(db, owner2)
    pool = _pool(db, cluster)
    vols = []
    for owner in (owner1, owner2):
        vol = _volume(db, cluster, owner, pool)
        publish._register_everywhere(db, vol, owner, [_fresh(db, n) for n in (*a, *b)])
        vol.write_to_db(db.kv_store)
        vols.append(db.get_lvol_by_id(vol.get_id()))
    group = _group(db, cluster, vols)
    src.sync_demote_group(group.get_id(), SITE_A)
    result = src.sync_promote_group(group.get_id(), SITE_B)
    gate.reset_mock()
    return cluster, a, b, (owner1, owner2), vols, result.task_id


class TestTwoLvs:

    def test_both_move_and_open(self, db, spdk, gate, moves):
        cluster, a, b, owners, vols, task_id = _two_lvs(db, spdk, gate)
        moves.leader.update(LVS_1=owners[0], LVS_2=owners[1])
        assert _run(db, task_id)
        assert [c[:3] for c in moves.calls] == [("LVS_1", owners[0].get_id(), b[0].get_id()),
                                                ("LVS_2", owners[1].get_id(), b[1].get_id())]
        assert gate.call_count == 4
        assert [db.get_lvol_by_id(v.get_id()).sync_active_site for v in vols] == [SITE_B, SITE_B]

    def test_the_gate_turning_false_between_the_transfers(self, db, spdk, gate, moves):
        cluster, a, b, owners, vols, task_id = _two_lvs(db, spdk, gate)
        moves.leader.update(LVS_1=owners[0], LVS_2=owners[1])
        gate.side_effect = [None, None, SyncGateError("sync-replication planned", ["LVS_2 unsynced"])]
        _run(db, task_id)
        assert [c[0] for c in moves.calls] == ["LVS_1"]
        assert _fresh(db, owners[0]).lvs_active_site == SITE_B     # moved stays moved
        assert _fresh(db, owners[1]).lvs_active_site == ""         # restored, never handed off
        assert [db.get_lvol_by_id(v.get_id()).sync_active_site for v in vols] == [SITE_A, SITE_A]

    def test_a_failed_second_hand_off_is_reconciled_after_done(self, db, spdk, gate, moves):
        cluster, a, b, owners, vols, task_id = _two_lvs(db, spdk, gate)
        moves.leader.update(LVS_1=owners[0], LVS_2=owners[1])
        moves.fail["LVS_2"] = ops.LeadershipTransferError("grant refused")
        reconciled = []

        def _reconcile(owner_id):
            assert _task(db, task_id).status == JobSchedule.STATUS_DONE   # DONE first
            reconciled.append(owner_id)

        with patch.object(ops, "reconcile_lvs_move", side_effect=_reconcile), \
                patch.object(ops, "set_lvs_active_site", wraps=ops.set_lvs_active_site) as cas:
            _run(db, task_id)
        assert reconciled == [owners[1].get_id()]
        assert all(c.args[1] != "" for c in cas.call_args_list)        # no blind CAS back
        assert _fresh(db, owners[0]).lvs_active_site == SITE_B
        assert _fresh(db, owners[1]).lvs_active_site == MOVING_B        # left to the reconciler


# ---------------------------------------------------------------------------
# cancellation
# ---------------------------------------------------------------------------

class TestCancel:

    def test_canceled_before_the_pass_touches_nothing(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        tasks_controller.cancel_task(task_id)
        with patch.object(ops, "reconcile_lvs_move") as reconcile:
            assert _run(db, task_id)
        task = _task(db, task_id)
        assert (task.status, task.function_result) == (JobSchedule.STATUS_DONE, "canceled")
        reconcile.assert_not_called()
        gate.assert_not_called()
        assert _fresh(db, owner).lvs_active_site == ""

    def test_a_cancel_racing_the_marker_keeps_the_journal_and_the_marker_is_undone(
            self, db, spdk, gate, moves):
        """cancel_task reads the task, the marker transaction commits, the
        cancel writes: the journal (moves) survives the cancel, and the next
        pass - DONE first - writes the previous site back."""
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        stale = _task(db, task_id)
        TestResume()._marked(db, gate, cluster, owner, task_id)
        with patch.object(tasks_controller.db, "get_task_by_id", return_value=stale):
            assert tasks_controller.cancel_task(task_id)
        task = _task(db, task_id)
        assert task.canceled and task.function_params["moves"] == {"LVS_1": ""}

        def _cas(owner_id, value, *, expect=None):
            assert _task(db, task_id).status == JobSchedule.STATUS_DONE
            return real(owner_id, value, expect=expect)

        real = ops.set_lvs_active_site
        with patch.object(ops, "set_lvs_active_site", side_effect=_cas):
            assert _run(db, task_id)
        assert _fresh(db, owner).lvs_active_site == ""
        assert moves.calls == []

    def test_a_cancel_racing_the_transfer_record_reconciles(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        TestResume()._marked(db, gate, cluster, owner, task_id)
        stale = _task(db, task_id)
        runner._save(_task(db, task_id), lambda t: runner._add_transferring(t, "LVS_1"))
        with patch.object(tasks_controller.db, "get_task_by_id", return_value=stale):
            tasks_controller.cancel_task(task_id)
        assert _task(db, task_id).function_params["transferring"] == ["LVS_1"]
        with patch.object(ops, "reconcile_lvs_move") as reconcile:
            _run(db, task_id)
        reconcile.assert_called_once_with(owner.get_id())

    def test_a_cancel_seen_after_the_marker_undoes_it(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        real = db.begin_sync_promote_moves

        def _then_cancel(*args, **kwargs):
            problems = real(*args, **kwargs)
            tasks_controller.cancel_task(task_id)
            return problems

        with patch.object(runner.db, "begin_sync_promote_moves", side_effect=_then_cancel):
            _run(db, task_id)
        assert _task(db, task_id).function_result == "canceled"
        assert moves.calls == []
        assert _fresh(db, owner).lvs_active_site == ""

    def test_a_cancel_before_the_second_transfer(self, db, spdk, gate, moves):
        cluster, a, b, owners, vols, task_id = _two_lvs(db, spdk, gate)
        moves.leader.update(LVS_1=owners[0], LVS_2=owners[1])
        real_move = moves.move

        def _move_then_cancel(*args, **kwargs):
            real_move(*args, **kwargs)
            tasks_controller.cancel_task(task_id)

        with patch.object(ops, "move_lvs_leadership", side_effect=_move_then_cancel):
            _run(db, task_id)
        assert [c[0] for c in moves.calls] == ["LVS_1"]
        assert _fresh(db, owners[0]).lvs_active_site == SITE_B
        assert _fresh(db, owners[1]).lvs_active_site == ""

    def test_a_cancel_before_the_open_leaves_the_volume_for_the_next_call(self, db, spdk, gate, moves):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        real_move = moves.move

        def _move_then_cancel(*args, **kwargs):
            real_move(*args, **kwargs)
            tasks_controller.cancel_task(task_id)

        with patch.object(ops, "move_lvs_leadership", side_effect=_move_then_cancel):
            _run(db, task_id)
        assert _fresh(db, owner).lvs_active_site == SITE_B
        assert db.get_lvol_by_id(vol.get_id()).sync_active_site == SITE_A
        again = src.sync_promote_lvol(vol.get_id(), SITE_B)
        assert again.in_progress and again.task_id != task_id


# ---------------------------------------------------------------------------
# create / clone against the moving: marker
# ---------------------------------------------------------------------------

def _new_volume(cluster, owner, pool, *, site=""):
    lvol = LVol()
    lvol.uuid = str(uuid.uuid4())
    lvol.lvol_name = f"new-{lvol.uuid[:8]}"
    lvol.place_in_pool(pool)
    lvol.node_id = owner.get_id()
    lvol.lvs_name = owner.lvstore
    lvol.ha_type = "ha"
    lvol.status = LVol.STATUS_IN_CREATION
    lvol.max_namespace_per_subsys = 1
    lvol.sync_active_site = site
    return lvol


def _claim_args(cluster, lvol):
    return (False, f"{cluster.nqn}:lvol:{lvol.uuid}", "", 1, None, None)


def _check(site=SITE_B):
    return lambda c, o, v, e: src.promote_move_problems(c, o, v, e, site)


class TestCreateAgainstTheMove:

    def test_a_claim_read_before_the_marker_commit_conflicts_and_is_refused(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        lvol = _new_volume(cluster, owner, pool)
        tr = db.kv_store.create_transaction()
        db._claim_lvol_ns_slot_tx(tr, lvol, owner, *_claim_args(cluster, lvol))
        assert lvol.sync_active_site == SITE_A
        assert db.begin_sync_promote_moves(_task(db, task_id), {"LVS_1": (owner.get_id(), "")},
                                           SITE_B, _check()) == []
        with pytest.raises(fdb.FDBError) as exc:
            tr.commit().wait()
        assert exc.value.code == 1020        # not_committed: the owner record changed
        with pytest.raises(SyncLeadershipMovingError):
            db.claim_lvol_ns_slot(lvol, owner, False, standalone_nqn=f"{cluster.nqn}:lvol:{lvol.uuid}")
        with pytest.raises(KeyError):
            db.get_lvol_by_id(lvol.get_id())

    def test_a_create_committed_under_the_promote_transaction_makes_it_retry_and_abort(
            self, db, spdk, gate):
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        task = _task(db, task_id)
        tr = db.kv_store.create_transaction()
        problems = db._begin_sync_promote_moves_tx(
            tr, task.get_db_id().encode(), cluster.get_id(), {"LVS_1": (owner.get_id(), "")}, SITE_B,
            _check(), StorageNode.active_indexes(db.kv_store), JobSchedule.active_indexes(db.kv_store),
            int(time.time()))
        assert problems == []
        lvol = _new_volume(cluster, owner, pool)
        db.claim_lvol_ns_slot(lvol, owner, False, standalone_nqn=f"{cluster.nqn}:lvol:{lvol.uuid}")
        with pytest.raises(fdb.FDBError):
            tr.commit().wait()
        assert _fresh(db, owner).lvs_active_site == ""
        problems = db.begin_sync_promote_moves(task, {"LVS_1": (owner.get_id(), "")}, SITE_B, _check())
        assert lvol.get_id() in problems[0]
        assert _fresh(db, owner).lvs_active_site == ""

    def test_a_site_computed_before_a_whole_promote_is_replaced_in_the_claim(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        lvol = _new_volume(cluster, owner, pool, site=SITE_A)       # computed earlier
        _update(db, owner, lvs_active_site=SITE_B)                   # a whole promote ran since
        db.claim_lvol_ns_slot(lvol, owner, False, standalone_nqn=f"{cluster.nqn}:lvol:{lvol.uuid}")
        assert db.get_lvol_by_id(lvol.get_id()).sync_active_site == SITE_B

    def test_volume_create_is_refused_while_moving(self, db, rpcs, env):
        cluster, a, b, owner = _layout(db, active_site=MOVING_B)
        rpcs.lead(owner)
        pool = _pool(db, cluster)
        with patch.object(lvol_controller, "add_lvol_on_node") as add, \
                patch.object(lvol_controller, "set_lvol"), \
                patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                      return_value="proceed"):
            vol_id, err = lvol_controller.add_lvol_ha(
                "vol-new", 2 * 1024 ** 3, owner.get_id(), "ha", pool.get_id())
        assert vol_id is False and "being moved" in err
        add.assert_not_called()
        assert db.get_lvols_by_node_id(owner.get_id()) == []

    def test_clone_is_refused_while_moving(self, db, rpcs, env):
        cluster, a, b, owner = _layout(db)
        rpcs.lead(owner)
        pool = _pool(db, cluster)
        vol = _volume(db, cluster, owner, pool)
        snap = SnapShot()
        snap.uuid = str(uuid.uuid4())
        snap.cluster_id = cluster.get_id()
        snap.pool_uuid = pool.get_id()
        snap.lvol = vol
        snap.status = SnapShot.STATUS_ONLINE
        snap.size = vol.size
        snap.snap_bdev = f"{owner.lvstore}/SNAP_1"
        snap.fabric = "tcp"
        snap.write_to_db(db.kv_store)
        _update(db, owner, lvs_active_site=MOVING_B)
        with patch.object(lvol_controller, "add_lvol_on_node") as add, \
                patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                      return_value="proceed"), \
                patch.object(snapshot_controller, "snapshot_events"):
            clone_id, err = snapshot_controller.clone(snap.get_id(), "clone-1", lock=False)
        assert clone_id is False and "being moved" in err
        add.assert_not_called()
        assert [v.get_id() for v in db.get_lvols_by_node_id(owner.get_id())] == [vol.get_id()]


# ---------------------------------------------------------------------------
# the site-rule lock against concurrent ANA writers
# ---------------------------------------------------------------------------

class _Pause:
    """Makes one node's ANA RPC stop inside the call until released."""

    def __init__(self, spdk, node, only_state=None):
        self.target = spdk[node.get_id()]
        self.real = self.target.nvmf_subsystem_listener_set_ana_state
        self.entered, self.release = threading.Event(), threading.Event()
        self.only_state = only_state
        self.sent: list = []

    def __call__(self, nqn, ip, port, trtype="TCP", is_optimized=True, ana=None, anagrpid=None):
        self.sent.append(ana)
        if not self.entered.is_set() and self.only_state in (None, ana):
            self.entered.set()
            assert self.release.wait(30)
        return self.real(nqn, ip, port, trtype=trtype, ana=ana, anagrpid=anagrpid)


def _thread(fn, *args):
    errors = []

    def _target():
        try:
            fn(*args)
        except BaseException as e:     # surfaced by the test
            errors.append(e)

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.errors = errors
    return t


class TestSiteRuleLock:

    def test_a_demote_waits_for_a_writer_that_computed_the_open_state(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        pause = _Pause(spdk, a[1])
        with patch.object(pause.target, "nvmf_subsystem_listener_set_ana_state", pause):
            writer = _thread(ops._set_lvol_ana_on_node, vol, a[1], "non_optimized")
            assert pause.entered.wait(30)
            demote = _thread(src.sync_demote_lvol, vol.get_id(), SITE_A)
            time.sleep(1.0)
            assert _state(spdk, a[0], vol) == "optimized"       # the fence has not started
            assert db.get_lvol_by_id(vol.get_id()).sync_demoted_sites == []
            pause.release.set()
            writer.join(30)
            demote.join(30)
        assert not writer.errors and not demote.errors
        assert pause.sent == ["non_optimized", "inaccessible"]   # the writer's, then the fence's
        assert _states(spdk, a, vol) == ["inaccessible"] * 3
        assert db.get_lvol_by_id(vol.get_id()).sync_demoted_sites == [SITE_A]

    def test_a_writer_waiting_on_a_demote_sends_the_post_demote_state(self, db, spdk, gate):
        cluster, a, b, owner, pool, vol = _registered(db, spdk)
        pause = _Pause(spdk, a[2], only_state="inaccessible")
        with patch.object(pause.target, "nvmf_subsystem_listener_set_ana_state", pause):
            demote = _thread(src.sync_demote_lvol, vol.get_id(), SITE_A)
            assert pause.entered.wait(30)                          # the fence holds the lock
            writer = _thread(ops._set_lvol_ana_on_node, vol, a[0], "optimized")
            time.sleep(1.0)
            pause.release.set()
            demote.join(30)
            writer.join(30)
        assert not writer.errors and not demote.errors
        assert _state(spdk, a[0], vol) == "inaccessible"

    def test_a_stale_close_never_lands_after_the_promote_opened(self, db, spdk, gate, moves):
        """A writer paused on the S primary after computing its pre-promote
        state (closed) holds the lock: the promote's marker and opening wait
        for it, so the path ends open."""
        cluster, a, b, owner, pool, vol, task_id = _queued(db, spdk, gate)
        moves.leader["LVS_1"] = owner
        pause = _Pause(spdk, b[0])
        with patch.object(pause.target, "nvmf_subsystem_listener_set_ana_state", pause):
            writer = _thread(ops._set_lvol_ana_on_node, vol, b[0], "optimized")
            assert pause.entered.wait(30)
            promote = _thread(_run, db, task_id)
            time.sleep(1.0)
            assert _fresh(db, owner).lvs_active_site == ""        # the marker waits
            pause.release.set()
            writer.join(30)
            promote.join(60)
        assert not writer.errors and not promote.errors
        assert pause.sent == ["inaccessible", "optimized"]
        assert _state(spdk, b[0], vol) == "optimized"
        ops._set_lvol_ana_on_node(vol, b[0], "non_optimized")     # a writer after: fresh rule
        assert _state(spdk, b[0], vol) == "optimized"
