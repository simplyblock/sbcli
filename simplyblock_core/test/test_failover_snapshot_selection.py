"""Which snapshot a cross-cluster fail-over is allowed to clone from.

Lab run 2026-08-10: all 5 failed-over volumes read as ALL ZEROS (no filesystem,
md5 mismatch). Every `cloned_from` parent was missing from the snapshot table
while dozens of replicated snapshots sat in `in_deletion`, and 5 replication
tasks were still `running`. The selector accepted any snapshot whose
`target_replicated_snap_uuid` was merely SET, so an unfinished or
being-deleted target copy could become the fail-over point.
"""
from simplyblock_core.controllers.lvol_controller import last_replicated_target_snapshot
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot

LVOL_ID = "LV1"
CLUSTER_ID = "CL_src"


def _lvol(uuid=LVOL_ID):
    lvol = LVol()
    lvol.uuid = uuid
    return lvol


def _snap(uuid, created_at, target_uuid):
    s = SnapShot()
    s.uuid = uuid
    s.created_at = created_at
    s.target_replicated_snap_uuid = target_uuid
    s.lvol = _lvol()
    return s


def _target(uuid, status=SnapShot.STATUS_ONLINE):
    s = SnapShot()
    s.uuid = uuid
    s.status = status
    return s


def _task(snapshot_id, status=JobSchedule.STATUS_DONE):
    t = JobSchedule()
    t.uuid = f"task_{snapshot_id}"
    t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    t.status = status
    t.function_params = {"snapshot_id": snapshot_id}
    return t


class _FakeDB:
    def __init__(self, tasks, snaps):
        self._tasks = tasks
        self._snaps = {s.get_id(): s for s in snaps}

    def get_job_tasks(self, cluster_id):
        return self._tasks

    def get_snapshot_by_id(self, sid):
        if sid not in self._snaps:
            raise KeyError(f"Snapshot {sid} not found")
        return self._snaps[sid]


def _pick(tasks, snaps):
    return last_replicated_target_snapshot(_FakeDB(tasks, snaps), LVOL_ID, CLUSTER_ID)


def test_picks_newest_completed():
    old = _snap("S_old", 100, "T_old")
    new = _snap("S_new", 200, "T_new")
    got = _pick([_task("S_old"), _task("S_new")],
                [old, new, _target("T_old"), _target("T_new")])
    assert got.get_id() == "T_new"


def test_skips_snapshot_whose_task_is_not_done():
    """The newest snapshot's transfer is still running -> use the older one."""
    old = _snap("S_old", 100, "T_old")
    running = _snap("S_running", 200, "T_running")
    got = _pick([_task("S_old"), _task("S_running", JobSchedule.STATUS_RUNNING)],
                [old, running, _target("T_old"), _target("T_running")])
    assert got.get_id() == "T_old", "an in-flight transfer is not a valid fail-over point"


def test_falls_back_when_newest_target_copy_was_deleted():
    """Retention removed the newest target copy -> fall back, do not orphan."""
    old = _snap("S_old", 100, "T_old")
    new = _snap("S_new", 200, "T_gone")
    got = _pick([_task("S_old"), _task("S_new")],
                [old, new, _target("T_old")])          # T_gone absent
    assert got.get_id() == "T_old"


def test_falls_back_when_newest_target_copy_is_in_deletion():
    old = _snap("S_old", 100, "T_old")
    new = _snap("S_new", 200, "T_dying")
    got = _pick([_task("S_old"), _task("S_new")],
                [old, new, _target("T_old"),
                 _target("T_dying", SnapShot.STATUS_IN_DELETION)])
    assert got.get_id() == "T_old"


def test_none_when_no_completed_replication():
    running = _snap("S_running", 200, "T_running")
    got = _pick([_task("S_running", JobSchedule.STATUS_RUNNING)],
                [running, _target("T_running")])
    assert got is None, "better to refuse fail-over than to hand back empty data"


def test_none_when_all_target_copies_gone():
    old = _snap("S_old", 100, "T_old")
    got = _pick([_task("S_old")], [old])
    assert got is None


def test_ignores_other_lvols_and_other_task_types():
    mine = _snap("S_mine", 100, "T_mine")
    theirs = _snap("S_theirs", 300, "T_theirs")
    theirs.lvol = _lvol("OTHER_LVOL")
    unrelated = _task("S_mine")
    unrelated.function_name = "some_other_function"
    got = _pick([_task("S_mine"), _task("S_theirs"), unrelated],
                [mine, theirs, _target("T_mine"), _target("T_theirs")])
    assert got.get_id() == "T_mine"


# --- Split control planes: the copies are recorded on the peer's ---------------

from pydantic import SecretStr  # noqa: E402

from simplyblock_core.controllers.lvol_controller import _policy_peer  # noqa: E402
from simplyblock_core.models.replication import ReplicationPolicy, ReplicationTarget  # noqa: E402
from simplyblock_core.peer.client import HttpPeerClient  # noqa: E402
from simplyblock_core.peer.protocol import DescribeResponse, SnapshotState  # noqa: E402

PEER_CLUSTER = "CL_peer"


class _FakePeer:
    """Answers describe from a dict of copy id -> SnapshotState; counts calls."""

    cluster_id = PEER_CLUSTER

    def __init__(self, states):
        self._states = states
        self.calls = []

    def describe(self, request):
        self.calls.append(list(request.snapshot_ids))
        return DescribeResponse(snapshots=[
            self._states.get(sid, SnapshotState(snapshot_id=sid, exists=False))
            for sid in request.snapshot_ids])


def _copy(sid, **kw):
    return SnapshotState(snapshot_id=sid, exists=True, **kw)


def _pick_peer(tasks, snaps, peer):
    # Only the source half is in this database: no target records at all.
    return last_replicated_target_snapshot(_FakeDB(tasks, snaps), LVOL_ID, CLUSTER_ID, peer=peer)


def test_peer_newest_copy_carries_what_a_clone_needs():
    old = _snap("S_old", 100, "T_old")
    new = _snap("S_new", 200, "T_new")
    peer = _FakePeer({
        "T_old": _copy("T_old"),
        "T_new": _copy("T_new", pool_id="P_peer", lvol_id="LV_peer", size=10, used_size=4,
                       created_at=201, group_id="G", group_seq=7),
    })
    got = _pick_peer([_task("S_old"), _task("S_new")], [old, new], peer)
    assert got.get_id() == "T_new"
    assert (got.cluster_id, got.pool_uuid, got.lvol.get_id()) == (PEER_CLUSTER, "P_peer", "LV_peer")
    assert (got.size, got.used_size, got.created_at, got.group_id, got.group_seq) == (10, 4, 201, "G", 7)
    assert peer.calls == [["T_new", "T_old"]], "one describe for every candidate, newest first"


def test_peer_skips_gone_and_dying_copies():
    a = _snap("S_a", 100, "T_a")
    b = _snap("S_b", 200, "T_b")
    c = _snap("S_c", 300, "T_c")
    peer = _FakePeer({"T_a": _copy("T_a"), "T_b": _copy("T_b", in_deletion=True)})  # T_c is gone
    got = _pick_peer([_task("S_a"), _task("S_b"), _task("S_c")], [a, b, c], peer)
    assert got.get_id() == "T_a"


def test_peer_none_when_nothing_replicated_asks_nothing():
    running = _snap("S_running", 200, "T_running")
    peer = _FakePeer({})
    got = _pick_peer([_task("S_running", JobSchedule.STATUS_RUNNING)], [running], peer)
    assert got is None
    assert peer.calls == []


class _PolicyDB:
    def __init__(self, target):
        self._policy = ReplicationPolicy()
        self._policy.uuid = "POL"
        self._policy.target_id = target.uuid
        self._target = target

    def get_replication_policy_by_id(self, pid):
        if pid != "POL":
            raise KeyError(pid)
        return self._policy

    def get_replication_target_by_id(self, tid):
        return self._target


def _target_rec(peer_endpoint=""):
    t = ReplicationTarget()
    t.uuid = "TGT"
    t.target_cluster_id = PEER_CLUSTER
    t.peer_endpoint = peer_endpoint
    t.peer_secret = SecretStr("s3cret" if peer_endpoint else "")
    return t


def test_policy_peer_only_for_a_target_on_a_peer_control_plane():
    lvol = _lvol()
    lvol.replication_policy_id = "POL"
    peer = _policy_peer(_PolicyDB(_target_rec("http://cp-b:15000")), lvol)
    assert isinstance(peer, HttpPeerClient) and peer.cluster_id == PEER_CLUSTER
    assert _policy_peer(_PolicyDB(_target_rec()), lvol) is None, "a target in this control plane"
    assert _policy_peer(_PolicyDB(_target_rec("http://cp-b:15000")), _lvol()) is None, "no policy"
    lvol.replication_policy_id = "GONE"
    assert _policy_peer(_PolicyDB(_target_rec("http://cp-b:15000")), lvol) is None, "policy deleted"
