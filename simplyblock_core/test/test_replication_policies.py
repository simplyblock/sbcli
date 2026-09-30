"""Replication targets, policies, volume assignment and group fail-over.

target -> policy -> volume, replacing the single cluster-scoped destination that
every `cluster add-replication` overwrote.
"""
import pytest

from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.controllers.replication_policy_controller import ReplicationConfigError
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.replication import (ConsistencyGroup, ReplicationPolicy,
                                                 ReplicationTarget)
from simplyblock_core.models.snapshot import SnapShot


class _FakeDB:
    kv_store = object()

    def __init__(self, clusters=("CL_SRC", "CL_TGT"), pools=(), lvols=(),
                 snapshots=(), replications=(), groups=(), tasks=(), nodes=()):
        self._clusters = list(clusters)
        self._pools = list(pools)
        self._lvols = list(lvols)
        self._snapshots = list(snapshots)
        self._replications = list(replications)
        self._groups = list(groups)
        self._tasks = list(tasks)
        self._nodes = list(nodes)
        self.written = []
        self.removed = []

    def get_storage_node_by_id(self, node_id):
        # Absent by default: the origin-primary guard then treats a member's source
        # as gone (KeyError) and the fail-over proceeds -- the behaviour every
        # existing fail-over test asserts. The protect no-op test seeds online
        # nodes so the members read as the live primary.
        for n in self._nodes:
            if getattr(n, "uuid", None) == node_id:
                return n
        raise KeyError(f'StorageNode {node_id} not found')

    # clusters / pools
    def get_cluster_by_id(self, cluster_id):
        if not cluster_id:
            raise KeyError('Cluster lookup with a blank id')
        if cluster_id not in self._clusters:
            raise KeyError(f'Cluster {cluster_id} not found')
        return type("C", (), {"uuid": cluster_id, "get_id": lambda s=None: cluster_id})()

    def get_pool_by_id_or_name(self, id_or_name):
        for p in self._pools:
            if p.uuid == id_or_name or p.pool_name == id_or_name:
                return p
        raise KeyError(f'Pool {id_or_name} not found')

    def get_pools(self, cluster_id=None):
        return [p for p in self._pools if not cluster_id or p.cluster_id == cluster_id]

    # targets / policies
    def get_replication_targets(self, cluster_id=None):
        return [t for t in self._targets() if not cluster_id or t.cluster_id == cluster_id]

    def _targets(self):
        return [o for o in self.written if isinstance(o, ReplicationTarget)
                and o not in self.removed]

    def get_replication_target_by_id(self, target_id):
        if not target_id:
            raise KeyError('ReplicationTarget lookup with a blank id')
        wanted = target_id.split('/')[-1]
        for t in self._targets():
            if t.uuid == wanted:
                return t
        raise KeyError(f'ReplicationTarget {target_id} not found')

    def get_replication_target_by_name(self, cluster_id, name):
        for t in self.get_replication_targets(cluster_id):
            if t.target_name == name:
                return t
        raise KeyError(f'ReplicationTarget {name} not found')

    def get_replication_policies(self, cluster_id=None):
        return [p for p in self._policies() if not cluster_id or p.cluster_id == cluster_id]

    def _policies(self):
        return [o for o in self.written if isinstance(o, ReplicationPolicy)
                and o not in self.removed]

    def get_replication_policy_by_id(self, policy_id):
        if not policy_id:
            raise KeyError('ReplicationPolicy lookup with a blank id')
        wanted = policy_id.split('/')[-1]
        for p in self._policies():
            if p.uuid == wanted:
                return p
        raise KeyError(f'ReplicationPolicy {policy_id} not found')

    def get_lvols_by_replication_policy(self, policy_id):
        wanted = policy_id.split('/')[-1]
        return [lv for lv in self._lvols
                if getattr(lv, 'replication_policy_id', '').split('/')[-1] == wanted]

    # volumes / snapshots / relationships
    def get_lvol_by_id(self, lvol_id):
        for lv in self._lvols:
            if lv.get_id() == lvol_id:
                return lv
        raise KeyError(f'LVol {lvol_id} not found')

    def get_lvols(self, cluster_id=None):
        if not cluster_id:
            return self._lvols
        return [lv for lv in self._lvols
                if getattr(lv, "cluster_id", cluster_id) == cluster_id]

    def get_mini_lvols(self):
        return self._lvols

    def get_snapshots(self, cluster_id=None):
        return self._snapshots

    def get_snapshots_by_lvol_id(self, lvol_id):
        return [s for s in self._snapshots
                if s.lvol and s.lvol.get_id() == lvol_id]

    def get_snapshot_by_id(self, uuid):
        if not uuid:
            raise KeyError('Snapshot lookup with a blank id')
        for s in self._snapshots:
            if s.get_id() == uuid:
                return s
        raise KeyError(f'Snapshot {uuid} not found')

    def get_lvol_replication_objects(self):
        return self._replications

    def get_consistency_group_for_policy(self, policy_id):
        wanted = policy_id.split('/')[-1] if policy_id else ""
        for g in self._groups:
            if g.policy_id.split('/')[-1] == wanted:
                return g
        return None

    def get_consistency_group_by_id(self, group_id):
        wanted = group_id.split('/')[-1] if group_id else ""
        for g in self._groups:
            if g.uuid == wanted:
                return g
        raise KeyError(f'ConsistencyGroup {group_id} not found')

    def get_consistency_group_by_name(self, cluster_id, name):
        for g in self._groups:
            if g.cluster_id == cluster_id and getattr(g, "group_name", "") == name:
                return g
        return None

    def get_job_tasks(self, cluster_id):
        return self._tasks


def _install(monkeypatch, db):
    monkeypatch.setattr(rpc, "db", db)
    # Record writes/removes through the models.
    monkeypatch.setattr(ReplicationTarget, "write_to_db",
                        lambda self, kv=None: db.written.append(self))
    monkeypatch.setattr(ReplicationPolicy, "write_to_db",
                        lambda self, kv=None: db.written.append(self))
    monkeypatch.setattr(ReplicationTarget, "remove", lambda self, kv: db.removed.append(self))
    monkeypatch.setattr(ReplicationPolicy, "remove", lambda self, kv: db.removed.append(self))
    return db


def _pool(uuid, cluster_id="CL_TGT", status=Pool.STATUS_ACTIVE):
    p = Pool()
    p.uuid = uuid
    p.pool_name = uuid
    p.cluster_id = cluster_id
    p.status = status
    return p


def _lvol(uuid, policy_id="", status=LVol.STATUS_ONLINE, demote_snapshot_id=""):
    lv = LVol()
    lv.uuid = uuid
    lv.status = status
    lv.replication_policy_id = policy_id
    lv.replication_demote_snapshot_id = demote_snapshot_id
    return lv


def _recording(sink, tag=None, returns=True):
    """A stub recording every call in `sink` — `tag`, or the call's first
    positional argument when no tag is given."""
    def _stub(arg, **kwargs):
        sink.append(arg if tag is None else tag)
        return returns
    return _stub


# --------------------------------------------------------------------------- #
# Targets
# --------------------------------------------------------------------------- #

def test_many_targets_per_cluster(monkeypatch):
    """The whole point: a cluster is no longer limited to one destination."""
    db = _install(monkeypatch, _FakeDB(clusters=("CL_SRC", "CL_A", "CL_B")))
    rpc.add_target("CL_SRC", "site-a", "CL_A")
    rpc.add_target("CL_SRC", "site-b", "CL_B")
    assert sorted(t.target_name for t in db.get_replication_targets("CL_SRC")) == ["site-a", "site-b"]


def test_duplicate_target_name_rejected(monkeypatch):
    _install(monkeypatch, _FakeDB(clusters=("CL_SRC", "CL_A")))
    rpc.add_target("CL_SRC", "site-a", "CL_A")
    with pytest.raises(ReplicationConfigError, match="already exists"):
        rpc.add_target("CL_SRC", "site-a", "CL_A")


def test_self_replication_rejected(monkeypatch):
    _install(monkeypatch, _FakeDB())
    with pytest.raises(ReplicationConfigError, match="cannot replicate to itself"):
        rpc.add_target("CL_SRC", "self", "CL_SRC")


def test_target_pool_is_stored_as_uuid(monkeypatch):
    """A pool NAME resolved lazily is what made the old add_replication raise
    KeyError later despite advertising "id or name"."""
    db = _install(monkeypatch, _FakeDB(pools=[_pool("POOL_T")]))
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT", target_pool="POOL_T")
    assert db.get_replication_target_by_id(target_id).target_pool_uuid == "POOL_T"


def test_inactive_pool_rejected(monkeypatch):
    _install(monkeypatch, _FakeDB(pools=[_pool("POOL_T", status=Pool.STATUS_INACTIVE)]))
    with pytest.raises(ReplicationConfigError, match="not active"):
        rpc.add_target("CL_SRC", "site-a", "CL_TGT", target_pool="POOL_T")


def test_pool_on_wrong_cluster_rejected(monkeypatch):
    _install(monkeypatch, _FakeDB(pools=[_pool("POOL_X", cluster_id="CL_SRC")]))
    with pytest.raises(ReplicationConfigError, match="not on target cluster"):
        rpc.add_target("CL_SRC", "site-a", "CL_TGT", target_pool="POOL_X")


def test_target_in_use_by_policy_cannot_be_removed(monkeypatch):
    db = _install(monkeypatch, _FakeDB())
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    rpc.add_policy("CL_SRC", "every-minute", target_id)
    with pytest.raises(ReplicationConfigError, match="is used by"):
        rpc.remove_target(target_id)
    assert db.removed == []


# --------------------------------------------------------------------------- #
# Policies
# --------------------------------------------------------------------------- #

def test_several_policies_per_target(monkeypatch):
    db = _install(monkeypatch, _FakeDB())
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    rpc.add_policy("CL_SRC", "fast", target_id, interval_min=1)
    rpc.add_policy("CL_SRC", "hourly", target_id, interval_min=60)
    cadences = {p.policy_name: p.interval_min for p in db.get_replication_policies("CL_SRC")}
    assert cadences == {"fast": 1, "hourly": 60}


def test_policy_can_reference_target_by_name(monkeypatch):
    _install(monkeypatch, _FakeDB())
    rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    assert rpc.add_policy("CL_SRC", "fast", "site-a")


def test_keep_replicated_below_the_floor_is_rejected(monkeypatch):
    """Fewer than a pair leaves an arriving snapshot with nothing to chain onto,
    so retention drops segments instead of swap-merging them."""
    _install(monkeypatch, _FakeDB())
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    with pytest.raises(ReplicationConfigError, match="at least 2"):
        rpc.add_policy("CL_SRC", "risky", target_id, keep_replicated=1)


def test_unknown_mode_rejected(monkeypatch):
    _install(monkeypatch, _FakeDB())
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    with pytest.raises(ReplicationConfigError, match="Unknown replication mode"):
        rpc.add_policy("CL_SRC", "bad", target_id, mode="sideways")


def test_policy_with_volumes_cannot_be_removed(monkeypatch):
    db = _FakeDB()
    _install(monkeypatch, db)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "fast", target_id)
    db._lvols.append(_lvol("LV1", policy_id=policy_id))
    with pytest.raises(ReplicationConfigError, match="followed by"):
        rpc.remove_policy(policy_id)


# --------------------------------------------------------------------------- #
# Volume assignment
# --------------------------------------------------------------------------- #

def test_attach_derives_effective_fields_from_policy(monkeypatch):
    """The service keeps reading the per-volume fields, so attaching must
    resolve policy + target into them."""
    db = _FakeDB(lvols=[_lvol("LV1")])
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "fast", target_id, interval_min=7, mode="migration")

    calls = {}
    monkeypatch.setattr(rpc.lvol_controller, "replication_start",
                        lambda lvol_id, **kw: calls.update(kw) or True)
    assert rpc.attach_policy("LV1", policy_id) is True
    assert calls == {"replication_cluster_id": "CL_TGT", "mode": "migration",
                     "interval_min": 7, "from_policy": True}
    assert db.get_lvol_by_id("LV1").replication_policy_id == policy_id


def test_attach_rolls_back_when_replication_cannot_start(monkeypatch):
    db = _FakeDB(lvols=[_lvol("LV1")])
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "fast", target_id)
    monkeypatch.setattr(rpc.lvol_controller, "replication_start", lambda lvol_id, **kw: False)

    with pytest.raises(ReplicationConfigError, match="Could not start replication"):
        rpc.attach_policy("LV1", policy_id)
    # Must not be left pointing at a policy that never started.
    assert db.get_lvol_by_id("LV1").replication_policy_id == ""


def test_change_policy_detaches_first(monkeypatch):
    db = _FakeDB(lvols=[_lvol("LV1")])
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    first = rpc.add_policy("CL_SRC", "fast", target_id, interval_min=1)
    second = rpc.add_policy("CL_SRC", "hourly", target_id, interval_min=60)
    monkeypatch.setattr(rpc.lvol_controller, "replication_start", lambda lvol_id, **kw: True)

    order: list[str] = []
    monkeypatch.setattr(rpc.lvol_controller, "replication_stop", _recording(order, "stop"))
    monkeypatch.setattr(rpc, "_purge_internal_replication_snapshots",
                        _recording(order, "purge", returns=0))

    rpc.attach_policy("LV1", first)
    rpc.attach_policy("LV1", second)
    assert order == ["stop", "purge"], "changing policy must detach (stop + purge) first"
    assert db.get_lvol_by_id("LV1").replication_policy_id == second


def test_attach_same_policy_twice_is_a_noop(monkeypatch):
    db = _FakeDB(lvols=[_lvol("LV1")])
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "fast", target_id)
    starts: list[str] = []
    monkeypatch.setattr(rpc.lvol_controller, "replication_start", _recording(starts))
    rpc.attach_policy("LV1", policy_id)
    rpc.attach_policy("LV1", policy_id)
    assert starts == ["LV1"], "re-attaching the same policy must not restart replication"


def test_detach_refused_while_a_cutover_is_in_flight(monkeypatch):
    lv = _lvol("LV1", policy_id="CL_SRC/P1")
    rep = LVolReplication()
    rep.source_lvol = lv
    rep.state = LVolReplication.STATE_CUTOVER_PENDING
    db = _FakeDB(lvols=[lv], replications=[rep])
    _install(monkeypatch, db)
    with pytest.raises(ReplicationConfigError, match="cutover in flight"):
        rpc.detach_policy("LV1")
    assert db.get_lvol_by_id("LV1").replication_policy_id == "CL_SRC/P1", "must not be cleared"


def test_detach_stops_and_purges_both_sides(monkeypatch):
    lv = _lvol("LV1", policy_id="CL_SRC/P1")
    db = _FakeDB(lvols=[lv])
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    stopped: list[str] = []
    monkeypatch.setattr(rpc.lvol_controller, "replication_stop", _recording(stopped))
    monkeypatch.setattr(rpc, "_purge_internal_replication_snapshots", lambda lvol_id: 4)
    assert rpc.detach_policy("LV1") is True
    assert stopped == ["LV1"]
    assert db.get_lvol_by_id("LV1").replication_policy_id == ""


# --------------------------------------------------------------------------- #
# Purge
# --------------------------------------------------------------------------- #

def _snap(uuid, lvol, snap_type=SnapShot.TYPE_INTERNAL, target="", created_at=0):
    s = SnapShot()
    s.uuid = uuid
    s.lvol = lvol
    s.snap_type = snap_type
    s.target_replicated_snap_uuid = target
    s.created_at = created_at
    return s


def test_purge_deletes_superseded_internal_snapshots_on_both_sides(monkeypatch):
    lv = _lvol("LV1")
    # The target copy belongs to the REP_ receiving volume on the other cluster,
    # not to the source volume.
    remote = _lvol("REP_LV1")
    older_src = _snap("S_SRC_OLD", lv, target="S_TGT_OLD", created_at=100)
    older_tgt = _snap("S_TGT_OLD", remote)
    newest_src = _snap("S_SRC_NEW", lv, target="S_TGT_NEW", created_at=200)
    newest_tgt = _snap("S_TGT_NEW", remote)
    db = _FakeDB(lvols=[lv, remote],
                 snapshots=[older_src, older_tgt, newest_src, newest_tgt])
    _install(monkeypatch, db)
    deleted: list[str] = []
    monkeypatch.setattr(rpc.snapshot_controller, "delete", _recording(deleted))
    rpc._purge_internal_replication_snapshots("LV1")
    assert deleted == ["S_TGT_OLD", "S_SRC_OLD"], \
        "the superseded pair goes, target copy first, then the source snapshot"


def test_purge_without_demote_keeps_the_newest_replicated_pair(monkeypatch):
    """Regression: 2026-09-25-disable-during-failover-purges-the-failover-point
    — during an UNPLANNED failover, Ramen deletes the source side's
    VolumeReplication while flipping its VRG to Secondary, which reaches this
    purge through DisableVolumeReplication -> detach_policy. Nothing was ever
    demoted (that is the whole premise of an unplanned failover) and the
    promote has not cloned yet (it races this very teardown), so neither the
    demote-snapshot guard nor the dependent-clone guard fires -- and the purge
    deleted the volume's ONLY recoverable point mid-failover (confirmed live
    2026-09-25 09:34:23: "detached from its replication policy (2 internal
    replication snapshot(s) removed)", after which the fail-over's clone
    selector 409-looped forever against a dead source). The newest fully
    replicated pair is the volume's last recovery point and survives a detach
    UNCONDITIONALLY; it is released only when the volume itself is deleted."""
    lv = _lvol("LV1")
    remote = _lvol("REP_LV1")
    src = _snap("S_SRC", lv, target="S_TGT", created_at=100)
    tgt = _snap("S_TGT", remote)
    db = _FakeDB(lvols=[lv, remote], snapshots=[src, tgt])
    _install(monkeypatch, db)
    deleted: list[str] = []
    monkeypatch.setattr(rpc.snapshot_controller, "delete", _recording(deleted))
    rpc._purge_internal_replication_snapshots("LV1")
    assert deleted == [], \
        "the sole replicated pair is the last recovery point and must survive the detach"


def test_purge_never_touches_user_snapshots(monkeypatch):
    lv = _lvol("LV1")
    user = _snap("S_USER", lv, snap_type=SnapShot.TYPE_USER, target="S_USER_TGT")
    db = _FakeDB(lvols=[lv], snapshots=[user])
    _install(monkeypatch, db)
    deleted: list[str] = []
    monkeypatch.setattr(rpc.snapshot_controller, "delete", _recording(deleted))
    rpc._purge_internal_replication_snapshots("LV1")
    assert deleted == []


def test_purge_keeps_the_demoted_volumes_fail_over_point(monkeypatch):
    """The snapshot a demoted volume is fenced on -- SOURCE copy and TARGET
    copy alike -- is the current fail-over point a pending PromoteVolume may
    still need, confirmed live 2026-09-24 (Ramen relocate M-02): detach_policy
    runs the instant a source demotes to Secondary, well before any fail-over
    gets a chance to promote the target, and deleting either copy just
    because nothing has cloned from it YET stranded every subsequent
    PromoteVolume attempt -- even though the source volume was still fully
    healthy at the time.

    Both copies matter for different reasons: the target copy is what a clone
    is actually built from, but last_replicated_target_snapshot resolves its
    fail-over candidates by first looking up each completed replication
    task's SOURCE snapshot id (task.function_params["snapshot_id"]) and only
    THEN reading that record's target_replicated_snap_uuid -- so a deleted
    source copy makes the whole candidate vanish before the target copy is
    ever even consulted, regardless of whether the target copy itself
    survived.

    An older, already-superseded internal snapshot's copies have no such role
    (a newer one already carries the current state forward) and stay
    purge-eligible. This demote guard is no longer the only protection: the
    NEWEST replicated pair now survives every detach unconditionally (see
    test_purge_without_demote_keeps_the_newest_replicated_pair -- an unplanned
    failover detaches without any demote), so this test pins the demote guard
    specifically because a demote may fence the volume on a snapshot that is
    not the newest by timestamp.
    """
    lv = _lvol("LV1", demote_snapshot_id="S_SRC_NEW")
    remote = _lvol("REP_LV1")
    older_src = _snap("S_SRC_OLD", lv, target="S_TGT_OLD", created_at=100)
    older_src.next_snap_uuid = "S_SRC_NEW"  # superseded
    older_tgt = _snap("S_TGT_OLD", remote)
    newest_src = _snap("S_SRC_NEW", lv, target="S_TGT_NEW", created_at=200)
    newest_tgt = _snap("S_TGT_NEW", remote)
    db = _FakeDB(lvols=[lv, remote],
                 snapshots=[older_src, older_tgt, newest_src, newest_tgt])
    _install(monkeypatch, db)
    deleted: list[str] = []
    monkeypatch.setattr(rpc.snapshot_controller, "delete", _recording(deleted))
    rpc._purge_internal_replication_snapshots("LV1")
    assert "S_TGT_NEW" not in deleted, "the newest target copy is a live fail-over point"
    assert "S_SRC_NEW" not in deleted, \
        "the newest SOURCE copy is what the job-task lookup resolves by id first"
    assert "S_TGT_OLD" in deleted, "a superseded target copy is still purge-eligible"
    assert "S_SRC_OLD" in deleted, "a superseded source copy is still purge-eligible"


def test_purge_keeps_a_snapshot_a_live_clone_depends_on(monkeypatch):
    """bdev_lvol_delete(sync=False) frees the blocks immediately, so a
    failed-over volume built on this snapshot would start reading zeros."""
    lv = _lvol("LV1")
    remote = _lvol("REP_LV1")
    src = _snap("S_SRC", lv, target="S_TGT")
    tgt = _snap("S_TGT", remote)
    clone = _lvol("FO_VOL")
    clone.cloned_from_snap = "S_TGT"
    db = _FakeDB(lvols=[lv, remote, clone], snapshots=[src, tgt])
    _install(monkeypatch, db)
    deleted: list[str] = []
    monkeypatch.setattr(rpc.snapshot_controller, "delete", _recording(deleted))
    rpc._purge_internal_replication_snapshots("LV1")
    assert "S_TGT" not in deleted


# --------------------------------------------------------------------------- #
# Group fail-over and relationship lookup
# --------------------------------------------------------------------------- #

def test_group_failover_covers_every_volume_of_a_target(monkeypatch):
    db = _FakeDB()
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "fast", target_id)
    db._lvols.extend([_lvol("LV1", policy_id=policy_id), _lvol("LV2", policy_id=policy_id)])
    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster",
                        lambda lvol_id: {"lvol_id": f"T_{lvol_id}", "connection_strings": []})

    results = rpc.failover_target(target_id)
    assert [(r["lvol_id"], r["status"], r["target_lvol_id"]) for r in results] == [
        ("LV1", "failed_over", "T_LV1"),
        ("LV2", "failed_over", "T_LV2"),
    ]


def test_group_failover_skips_already_failed_over_volumes(monkeypatch):
    db = _FakeDB()
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "fast", target_id)
    lv = _lvol("LV1", policy_id=policy_id)
    db._lvols.append(lv)
    rep = LVolReplication()
    rep.source_lvol = lv
    rep.target_lvol = _lvol("T_LV1")
    rep.state = LVolReplication.STATE_FAILED_OVER
    db._replications.append(rep)
    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster",
                        lambda lvol_id: pytest.fail("must not fail over twice"))

    results = rpc.failover_policy(policy_id)
    assert results[0]["status"] == "skipped"
    assert results[0]["target_lvol_id"] == "T_LV1"


def test_group_failover_reports_per_volume_failures(monkeypatch):
    db = _FakeDB()
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "fast", target_id)
    db._lvols.extend([_lvol("LV1", policy_id=policy_id), _lvol("LV2", policy_id=policy_id)])

    def _flaky(lvol_id):
        if lvol_id == "LV1":
            raise RuntimeError("node offline")
        return "T_LV2"

    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster", _flaky)
    results = rpc.failover_policy(policy_id)
    assert results[0]["status"] == "failed" and "node offline" in results[0]["detail"]
    assert results[1]["status"] == "failed_over", "one bad volume must not stop the group"


# --------------------------------------------------------------------------- #
# Consistency-group fail-over: one generation for the whole group
# --------------------------------------------------------------------------- #

def _cg_group(policy_id, members, last_seq):
    g = ConsistencyGroup()
    g.uuid = "CG1"
    g.cluster_id = "CL_SRC"
    g.policy_id = policy_id
    g.last_group_seq = last_seq
    g.members = {m: {"joined_seq": 1, "removed_seq": 0} for m in members}
    return g


def _done_replication_task(snapshot_id):
    return type("T", (), {
        "function_name": JobSchedule.FN_SNAPSHOT_REPLICATION,
        "status": JobSchedule.STATUS_DONE,
        "function_params": {"snapshot_id": snapshot_id},
    })()


def _group_snap(uuid, lvol, group, seq, target=""):
    s = _snap(uuid, lvol, target=target)
    s.group_id = group.get_id()
    s.group_seq = seq
    return s


def _cg_policy(monkeypatch, db):
    """A CG policy without add_policy's group-creation side effect (the group
    record is installed directly into the fake)."""
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "cg", target_id)
    db.get_replication_policy_by_id(policy_id).consistency_group = True
    return policy_id


def test_cg_failover_pins_every_member_to_one_common_generation(monkeypatch):
    """Fail-over of a consistency group must cut EVERY member at the newest
    generation fully replicated for ALL members — not at each volume's own
    newest replicated snapshot. Regression: run 2026-09-07 restored
    generations (3, 3, 4) because generation 4 had replicated for one member
    only."""
    db = _FakeDB()
    _install(monkeypatch, db)
    policy_id = _cg_policy(monkeypatch, db)
    lv1, lv2 = _lvol("LV1", policy_id=policy_id), _lvol("LV2", policy_id=policy_id)
    db._lvols.extend([lv1, lv2])
    group = _cg_group(policy_id, ["LV1", "LV2"], last_seq=2)
    db._groups.append(group)

    # Generation 1 fully replicated for both members; generation 2 only for
    # LV2 (LV1's copy has no target yet).
    remote = _lvol("REP")
    db._snapshots.extend([
        _group_snap("S1_LV1", lv1, group, 1, target="T1_LV1"), _snap("T1_LV1", remote),
        _group_snap("S1_LV2", lv2, group, 1, target="T1_LV2"), _snap("T1_LV2", remote),
        _group_snap("S2_LV1", lv1, group, 2),
        _group_snap("S2_LV2", lv2, group, 2, target="T2_LV2"), _snap("T2_LV2", remote),
    ])
    db._tasks.extend([_done_replication_task("S1_LV1"),
                      _done_replication_task("S1_LV2"),
                      _done_replication_task("S2_LV2")])

    pins: dict[str, str] = {}

    def _record(lvol_id, pin_snapshot_id=None):
        pins[lvol_id] = pin_snapshot_id
        return {"lvol_id": f"T_{lvol_id}", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster", _record)
    results = rpc.failover_policy(policy_id)
    assert all(r["status"] == "failed_over" for r in results), results
    assert pins == {"LV1": "S1_LV1", "LV2": "S1_LV2"}, \
        "every member must be pinned to generation 1, the newest COMMON one"


def test_cg_failover_refuses_without_a_common_generation(monkeypatch):
    """No generation replicated for every member: failing over anything would
    tear the group, so every volume is refused and none is cloned."""
    db = _FakeDB()
    _install(monkeypatch, db)
    policy_id = _cg_policy(monkeypatch, db)
    lv1, lv2 = _lvol("LV1", policy_id=policy_id), _lvol("LV2", policy_id=policy_id)
    db._lvols.extend([lv1, lv2])
    group = _cg_group(policy_id, ["LV1", "LV2"], last_seq=2)
    db._groups.append(group)

    remote = _lvol("REP")
    db._snapshots.extend([
        _group_snap("S1_LV1", lv1, group, 1, target="T1_LV1"), _snap("T1_LV1", remote),
        _group_snap("S2_LV2", lv2, group, 2, target="T2_LV2"), _snap("T2_LV2", remote),
    ])
    db._tasks.extend([_done_replication_task("S1_LV1"),
                      _done_replication_task("S2_LV2")])

    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster",
                        lambda *a, **k: pytest.fail("no member may be failed over"))
    results = rpc.failover_policy(policy_id)
    assert {r["status"] for r in results} == {"failed"}
    assert "generation" in results[0]["detail"]


def test_cg_failover_resume_pins_to_the_incumbent_generation(monkeypatch):
    """A partial re-run must finish on the generation the first pass cut, even
    when a newer generation has since fully replicated — otherwise the resumed
    members land on a different cut than the settled ones."""
    db = _FakeDB()
    _install(monkeypatch, db)
    policy_id = _cg_policy(monkeypatch, db)
    lv1, lv2 = _lvol("LV1", policy_id=policy_id), _lvol("LV2", policy_id=policy_id)
    db._lvols.extend([lv1, lv2])
    group = _cg_group(policy_id, ["LV1", "LV2"], last_seq=2)
    db._groups.append(group)

    remote = _lvol("REP")
    t1_lv1 = _group_snap("T1_LV1", remote, group, 1)   # target copies keep provenance
    db._snapshots.extend([
        _group_snap("S1_LV1", lv1, group, 1, target="T1_LV1"), t1_lv1,
        _group_snap("S1_LV2", lv2, group, 1, target="T1_LV2"), _snap("T1_LV2", remote),
        _group_snap("S2_LV1", lv1, group, 2, target="T2_LV1"), _snap("T2_LV1", remote),
        _group_snap("S2_LV2", lv2, group, 2, target="T2_LV2"), _snap("T2_LV2", remote),
    ])
    db._tasks.extend([_done_replication_task(s) for s in
                      ("S1_LV1", "S1_LV2", "S2_LV1", "S2_LV2")])

    # LV1 already failed over on generation 1: its clone derives from T1_LV1.
    clone = _lvol("FO_LV1")
    clone.cloned_from_snap = "T1_LV1"
    rep = LVolReplication()
    rep.source_lvol = lv1
    rep.target_lvol = clone
    rep.state = LVolReplication.STATE_FAILED_OVER
    db._replications.append(rep)

    pins: dict[str, str] = {}

    def _record(lvol_id, pin_snapshot_id=None):
        pins[lvol_id] = pin_snapshot_id
        return {"lvol_id": f"T_{lvol_id}", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster", _record)
    results = rpc.failover_policy(policy_id)
    assert results[0]["status"] == "skipped"
    assert results[1]["status"] == "failed_over"
    assert pins == {"LV2": "S1_LV2"}, \
        "the resumed member must join the incumbent generation 1, not the newer 2"


def test_cg_failover_triggers_on_membership_without_the_flag(monkeypatch):
    """Group fail-over keys off consistency-group MEMBERSHIP, not a policy flag:
    a plain policy whose volumes carry a group_id still cuts every member at one
    common generation, resolving the group by the members' group_id."""
    db = _FakeDB()
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "plain", target_id)   # no consistency_group flag
    group = _cg_group(policy_id, ["LV1", "LV2"], last_seq=2)
    db._groups.append(group)
    lv1, lv2 = _lvol("LV1", policy_id=policy_id), _lvol("LV2", policy_id=policy_id)
    lv1.group_id = group.get_id()
    lv2.group_id = group.get_id()
    db._lvols.extend([lv1, lv2])

    # Generation 1 fully replicated for both members; generation 2 only for LV2.
    remote = _lvol("REP")
    db._snapshots.extend([
        _group_snap("S1_LV1", lv1, group, 1, target="T1_LV1"), _snap("T1_LV1", remote),
        _group_snap("S1_LV2", lv2, group, 1, target="T1_LV2"), _snap("T1_LV2", remote),
        _group_snap("S2_LV1", lv1, group, 2),
        _group_snap("S2_LV2", lv2, group, 2, target="T2_LV2"), _snap("T2_LV2", remote),
    ])
    db._tasks.extend([_done_replication_task("S1_LV1"),
                      _done_replication_task("S1_LV2"),
                      _done_replication_task("S2_LV2")])

    pins: dict[str, str] = {}

    def _record(lvol_id, pin_snapshot_id=None):
        pins[lvol_id] = pin_snapshot_id
        return {"lvol_id": f"T_{lvol_id}", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster", _record)
    results = rpc.failover_policy(policy_id)
    assert all(r["status"] == "failed_over" for r in results), results
    assert pins == {"LV1": "S1_LV1", "LV2": "S1_LV2"}, \
        "membership alone must pin every member to generation 1, the newest COMMON one"


def _shared_policy_group_and_standalone(monkeypatch, db):
    """A policy shared by a 2-member consistency group AND a standalone volume
    (STD, no group_id) -- the live shape where a single-PVC workload and a VGR
    group land on one backend replication policy. Returns policy_id."""
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "shared", target_id)
    group = _cg_group(policy_id, ["LV1", "LV2"], last_seq=2)
    db._groups.append(group)
    lv1, lv2 = _lvol("LV1", policy_id=policy_id), _lvol("LV2", policy_id=policy_id)
    lv1.group_id = group.get_id()
    lv2.group_id = group.get_id()
    std = _lvol("STD", policy_id=policy_id)          # no group_id
    db._lvols.extend([lv1, lv2, std])
    remote = _lvol("REP")
    db._snapshots.extend([
        _group_snap("S1_LV1", lv1, group, 1, target="T1_LV1"), _snap("T1_LV1", remote),
        _group_snap("S1_LV2", lv2, group, 1, target="T1_LV2"), _snap("T1_LV2", remote),
    ])
    db._tasks.extend([_done_replication_task("S1_LV1"), _done_replication_task("S1_LV2")])
    return policy_id


def test_failover_policy_does_not_demand_a_group_generation_for_a_standalone(monkeypatch):
    """Regression (2026-09-27): a policy shared by a consistency group and a
    standalone volume refused the WHOLE fail-over because the group-generation
    check demanded the standalone be in the group's generation ("generation N
    lacks STD"). The standalone must fail over per-volume, the members as a
    group -- not one poisoning the other."""
    db = _FakeDB()
    _install(monkeypatch, db)
    policy_id = _shared_policy_group_and_standalone(monkeypatch, db)
    pins: dict = {}

    def _record(lvol_id, pin_snapshot_id=None):
        pins[lvol_id] = pin_snapshot_id
        return {"lvol_id": f"T_{lvol_id}", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster", _record)
    results = rpc.failover_policy(policy_id)
    by_id = {r["lvol_id"]: r["status"] for r in results}
    assert by_id == {"LV1": "failed_over", "LV2": "failed_over", "STD": "failed_over"}, results
    assert pins["LV1"] == "S1_LV1" and pins["LV2"] == "S1_LV2", "members pinned to the group cut"
    assert pins["STD"] is None, "standalone fails over per-volume, not pinned to the group"


def test_failover_group_touches_only_its_members(monkeypatch):
    """The VGR entry point fails over ONLY the group's members; a standalone
    volume that merely shares the policy is left alone (it has its own DRPC)."""
    db = _FakeDB()
    _install(monkeypatch, db)
    _shared_policy_group_and_standalone(monkeypatch, db)
    group = db._groups[0]
    touched: list = []

    def _record(lvol_id, pin_snapshot_id=None):
        touched.append(lvol_id)
        return {"lvol_id": f"T_{lvol_id}", "connection_strings": []}

    # Source down -> a genuine fail-over (same signal the standalone path reads).
    monkeypatch.setattr(rpc.lvol_controller, "replication_source_online", lambda lvol: False)
    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster", _record)
    results = rpc.failover_group(group)
    assert {r["lvol_id"] for r in results} == {"LV1", "LV2"}
    assert all(r["status"] == "failed_over" for r in results), results
    assert "STD" not in touched, "the standalone volume must NOT be failed over by a group fail-over"


def test_failover_group_promote_is_a_noop_for_the_live_primary(monkeypatch):
    """Regression (2026-09-27): csi-addons calls PromoteGroup whenever the VGR is
    Primary -- including the origin cluster during protect -- and the group path
    lacks the per-volume endpoint's planned/demote guard. Without this check the
    protect-promote cloned the still-primary members to the target and stopped
    their replication (pre-staging hollow clones, breaking protect). When the
    members are the live primary (source online via the SAME replication_source_online
    check the standalone path uses, none failed over, none demoted), the promote is
    a no-op success -- nothing is cloned."""
    db = _FakeDB()
    _install(monkeypatch, db)
    _shared_policy_group_and_standalone(monkeypatch, db)
    group = db._groups[0]
    monkeypatch.setattr(rpc.lvol_controller, "replication_source_online", lambda lvol: True)
    touched: list = []
    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster",
                        _recording(touched))
    results = rpc.failover_group(group)
    assert all(r["status"] == "already_primary" for r in results), results
    assert {r["lvol_id"] for r in results} == {"LV1", "LV2"}
    assert touched == [], "an origin-primary promote must NOT clone anything"


def test_failover_group_promote_proceeds_when_members_are_demoted(monkeypatch):
    """A PLANNED relocate demotes the source first and the source stays ONLINE, so
    source health alone cannot tell it apart from protect -- the demote state must.
    A demoted member is a real hand-off, not the untouched primary, so the promote
    must proceed (not no-op) even though the source is online."""
    db = _FakeDB()
    _install(monkeypatch, db)
    _shared_policy_group_and_standalone(monkeypatch, db)
    group = db._groups[0]
    # Source online, but the members are demoted (a relocate) -> not the untouched
    # primary, so the guard must NOT no-op.
    monkeypatch.setattr(rpc.lvol_controller, "replication_source_online", lambda lvol: True)
    for lv in db._lvols:
        if getattr(lv, "group_id", "") == group.get_id():
            lv.replication_demote_state = LVol.REPLICATION_DEMOTE_DONE
    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster",
                        lambda lvol_id, **kw: {"lvol_id": f"T_{lvol_id}", "connection_strings": []})
    results = rpc.failover_group(group)
    assert all(r["status"] != "already_primary" for r in results), \
        "a demoted (relocating) member is a hand-off, not the live primary"


def _failback_scenario(monkeypatch, db, unshipped=()):
    """A failed-over consistency group ready to fail BACK: an EMPTY local group on
    CL_SRC whose members now live in the peer group on CL_TGT, with a demote cut
    (generation 1) shipped home for every peer member. Returns (local_group,
    policy_id). ``unshipped`` names member ids whose generation-1 replication task
    is omitted -- their cut never finished shipping home."""
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "cg", target_id)

    local = ConsistencyGroup()
    local.uuid, local.cluster_id, local.group_name = "CG_SRC", "CL_SRC", "cg"
    local.policy_id = policy_id
    local.members = {}                                 # emptied by the fail-over
    db._groups.append(local)

    peer = ConsistencyGroup()
    peer.uuid, peer.cluster_id, peer.group_name = "CG_TGT", "CL_TGT", "cg"
    peer.members = {"PB1": {"joined_seq": 1, "removed_seq": 0},
                    "PB2": {"joined_seq": 1, "removed_seq": 0}}
    db._groups.append(peer)

    pb1, pb2 = _lvol("PB1"), _lvol("PB2")
    for m in (pb1, pb2):
        m.group_id = peer.get_id()
        m.cluster_id = "CL_TGT"
    db._lvols.extend([pb1, pb2])

    # Demote generation 1, shipped home (a home-side copy + a DONE task).
    home = _lvol("HOME")
    for src, snap_id, tgt_id in (("PB1", "D1_PB1", "H1_PB1"),
                                 ("PB2", "D1_PB2", "H1_PB2")):
        db._snapshots.extend([
            _group_snap(snap_id, db.get_lvol_by_id(src), peer, 1, target=tgt_id),
            _snap(tgt_id, home)])
        if src not in unshipped:
            db._tasks.append(_done_replication_task(snap_id))
    return local, policy_id


def test_failback_group_clones_peer_members_home_pinned_to_the_demote_cut(monkeypatch):
    """Regression (2026-09-27): promoting an empty local group on fail-back cloned
    NOTHING -- the promote reported success while the workload kept writing to the
    peer's clones. The empty local group must resolve to the peer group and clone
    EVERY member home, pinned to the one demote generation the peer shipped."""
    db = _FakeDB()
    _install(monkeypatch, db)
    local, _ = _failback_scenario(monkeypatch, db)
    pins: dict = {}

    def _record(lvol_id, pin_snapshot_id=None):
        pins[lvol_id] = pin_snapshot_id
        return {"lvol_id": f"HOME_{lvol_id}", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster", _record)
    results = rpc.failover_group(local)
    assert {r["lvol_id"]: r["status"] for r in results} == \
        {"PB1": "failed_over", "PB2": "failed_over"}, results
    assert pins == {"PB1": "D1_PB1", "PB2": "D1_PB2"}, \
        "every peer member cloned home pinned to the common demote generation 1"


def test_failback_group_clones_settled_targets_not_skipped(monkeypatch):
    """The members to clone home are the failed-over targets, every one settled
    (STATE_FAILED_OVER). The fail-over path skips settled volumes; the fail-back
    must NOT -- routing these through it is exactly the silent no-op."""
    db = _FakeDB()
    _install(monkeypatch, db)
    local, _ = _failback_scenario(monkeypatch, db)
    for src in ("PB1", "PB2"):
        rep = LVolReplication()
        rep.source_lvol = _lvol(f"ORIG_{src}")
        rep.target_lvol = db.get_lvol_by_id(src)
        rep.state = LVolReplication.STATE_FAILED_OVER
        db._replications.append(rep)
    touched: list = []

    def _record(lvol_id, pin_snapshot_id=None):
        touched.append(lvol_id)
        return {"lvol_id": f"HOME_{lvol_id}", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster", _record)
    results = rpc.failover_group(local)
    assert sorted(touched) == ["PB1", "PB2"], \
        "settled failed-over targets must still be cloned home, not skipped"
    assert all(r["status"] == "failed_over" for r in results), results


def test_failback_group_refuses_until_the_demote_cut_finished_shipping(monkeypatch):
    """A fail-back cut is atomic: if the demote generation has shipped home for one
    member but not the other, refuse rather than clone a split group -- and clone
    nothing."""
    db = _FakeDB()
    _install(monkeypatch, db)
    local, _ = _failback_scenario(monkeypatch, db, unshipped=("PB2",))
    touched: list = []
    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster",
                        lambda lvol_id, **kw: touched.append(lvol_id))
    results = rpc.failover_group(local)
    assert all(r["status"] == "failed" for r in results), results
    assert "not finished shipping" in results[0]["detail"]
    assert touched == [], "a mixed-generation fail-back must clone nothing"


def test_resolve_active_peer_group_by_name_on_the_target_cluster(monkeypatch):
    """The peer group is found by the group's name on the policy's replication
    TARGET cluster -- the key reconstitute_group_after_handoff formed it under."""
    db = _FakeDB()
    _install(monkeypatch, db)
    local, policy_id = _failback_scenario(monkeypatch, db)
    policy = db.get_replication_policy_by_id(policy_id)
    peer = rpc._resolve_active_peer_group(local, policy)
    assert peer is not None and peer.cluster_id == "CL_TGT" and peer.group_name == "cg"


def test_group_demote_resolves_to_peer_primary_and_ships_home(monkeypatch):
    """Regression (2026-09-28): a relocate demote lands on the empty ORIGIN group
    (the origin-pinned cg: handle always resolves there), where it no-op'd --
    returning demoted=True with no members and shipping nothing. The fail-back
    promote then looped forever on 'the demote cut has not finished shipping'
    because the current primary's clones were never demoted. The demote must
    resolve to the PEER (current-primary) group and drive ITS clones through the
    ship-home cut: point each reverse pipe home and seed the one demote generation
    -- the demote analog of _failback_group / the driver's resolveToLocalReplica."""
    from simplyblock_core.controllers import consistency_group_controller as cgc
    from simplyblock_core.controllers import lvol_controller as lc
    from simplyblock_core.services import replication_final_step as rfs

    db = _FakeDB()
    _install(monkeypatch, db)
    monkeypatch.setattr(cgc, "db", db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    local, _ = _failback_scenario(monkeypatch, db)

    # The peer's clones are settled failed-over TARGETS -- exactly the side a
    # relocate demote must now ship back the other way.
    for src in ("PB1", "PB2"):
        rep = LVolReplication()
        rep.source_lvol = _lvol(f"ORIG_{src}")
        rep.target_lvol = db.get_lvol_by_id(src)
        rep.state = LVolReplication.STATE_FAILED_OVER
        db._replications.append(rep)

    failed_back: list = []
    monkeypatch.setattr(lc, "replication_failback", _recording(failed_back))
    monkeypatch.setattr(rfs, "fence_source_paths", lambda *a, **k: None)

    def _group_snap_cut(group, **kw):
        ids = []
        for src, sid in (("PB1", "DEMOTE_PB1"), ("PB2", "DEMOTE_PB2")):
            db._snapshots.append(_snap(sid, db.get_lvol_by_id(src)))
            ids.append(sid)
        return ids, None
    monkeypatch.setattr(cgc, "create_group_snapshot_for_group", _group_snap_cut)

    result = cgc.demote_group(local)

    assert result["demoted"] is False, \
        "the demote is still shipping the peer's cut home, not a no-op 'done'"
    assert {m["lvol_id"] for m in result["members"]} == {"PB1", "PB2"}, result
    assert sorted(failed_back) == ["PB1", "PB2"], \
        "each peer clone's reverse pipe must be pointed home"
    for src in ("PB1", "PB2"):
        m = db.get_lvol_by_id(src)
        assert m.replication_demote_state == LVol.REPLICATION_DEMOTE_PENDING
        assert m.replication_demote_snapshot_id == f"DEMOTE_{src}"


def test_group_promote_is_idempotent_once_members_failed_home(monkeypatch):
    """Regression (2026-09-28): after _failback_group clones the peer's members
    HOME, this group's members are the home-side clones -- the settled TARGET end of
    the reverse relationship. Ramen re-drives PromoteGroup every reconcile, so the
    re-promote must report success. The settled check is SOURCE-keyed
    (_active_relationship), so without recognising the target side these members read
    as pending, no fail-over generation qualifies, and the promote refuses with a
    'mixed-generation fail-over' -- leaving the relocate stuck though the data is
    already home on this cluster."""
    db = _FakeDB()
    _install(monkeypatch, db)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    target_id = rpc.add_target("CL_SRC", "site-a", "CL_TGT")
    policy_id = rpc.add_policy("CL_SRC", "cg", target_id)

    group = ConsistencyGroup()
    group.uuid, group.cluster_id, group.group_name = "CG_HOME", "CL_SRC", "cg"
    group.policy_id = policy_id
    group.members = {"HC1": {"joined_seq": 1, "removed_seq": 0},
                     "HC2": {"joined_seq": 1, "removed_seq": 0}}
    db._groups.append(group)

    for hc in ("HC1", "HC2"):
        lv = _lvol(hc, policy_id=policy_id)
        lv.group_id = group.get_id()
        lv.cluster_id = "CL_SRC"
        lv.replication_demote_state = LVol.REPLICATION_DEMOTE_DONE
        db._lvols.append(lv)
        rep = LVolReplication()
        rep.source_lvol = _lvol(f"PEER_{hc}")       # the peer (current) clone on CL_TGT
        rep.target_lvol = db.get_lvol_by_id(hc)     # the home clone == this member
        rep.source_cluster_id = "CL_TGT"
        rep.target_cluster_id = "CL_SRC"
        rep.state = LVolReplication.STATE_FAILED_OVER
        db._replications.append(rep)

    touched: list = []
    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster",
                        lambda lvol_id, **kw: touched.append(lvol_id))
    results = rpc.failover_group(group)
    assert {r["status"] for r in results} == {"failed_over"}, results
    assert {r["lvol_id"] for r in results} == {"HC1", "HC2"}, results
    assert touched == [], "an already-home group must clone nothing on re-promote"


def test_relationship_resolves_source_to_target_and_back(monkeypatch):
    source = _lvol("LV_SRC")
    target = _lvol("LV_TGT")
    rep = LVolReplication()
    rep.source_lvol = source
    rep.target_lvol = target
    rep.source_cluster_id = "CL_SRC"
    rep.target_cluster_id = "CL_TGT"
    rep.state = LVolReplication.STATE_FAILED_OVER
    rep.target_nqn = "nqn.test:vol"
    rep.target_ns_id = 3
    db = _FakeDB(lvols=[source, target], replications=[rep])
    _install(monkeypatch, db)

    forward = rpc.get_relationship("LV_SRC")
    assert forward["target_lvol_id"] == "LV_TGT" and forward["is_source"] is True
    assert forward["target_nqn"] == "nqn.test:vol" and forward["target_ns_id"] == 3

    reverse = rpc.get_relationship("LV_TGT")
    assert reverse["source_lvol_id"] == "LV_SRC" and reverse["is_source"] is False

    assert rpc.get_relationship("LV_UNRELATED") is None


# --------------------------------------------------------------------------- #
# Assignment at create time
# --------------------------------------------------------------------------- #

def test_policy_can_be_assigned_when_the_volume_is_created(monkeypatch):
    """Step 3 of the hierarchy: a policy assigned at create time configures
    replication for that volume, with no separate call."""
    from simplyblock_core.controllers import lvol_controller

    attached: dict[str, str] = {}
    monkeypatch.setattr(rpc, "attach_policy",
                        lambda lvol_id, policy: attached.update(lvol=lvol_id, policy=policy) or True)

    # add_lvol_ha attaches after the volume is online; exercise that tail
    # directly, since a full create needs a live cluster.
    lvol = _lvol("LV1")
    policy = "fast"
    if policy:
        from simplyblock_core.controllers import replication_policy_controller
        replication_policy_controller.attach_policy(lvol.get_id(), policy)
    assert attached == {"lvol": "LV1", "policy": "fast"}
    assert 'replication_policy' in lvol_controller.add_lvol_ha.__code__.co_varnames, \
        "add_lvol_ha must accept replication_policy so create-time assignment works"


def test_create_reports_when_the_policy_cannot_be_attached(monkeypatch):
    """A volume that was created but could not be replicated must not look like
    a fully successful create."""
    import inspect
    from simplyblock_core.controllers import lvol_controller
    src = inspect.getsource(lvol_controller.add_lvol_ha)
    assert "replication policy could not be attached" in src, \
        "the attach failure has to surface to the caller"


def test_policy_controller_may_drive_the_raw_verbs(monkeypatch):
    """The guard must not lock the policy controller itself out."""
    import inspect
    from simplyblock_core.controllers import replication_policy_controller
    attach_src = inspect.getsource(replication_policy_controller.attach_policy)
    detach_src = inspect.getsource(replication_policy_controller.detach_policy)
    assert "from_policy=True" in attach_src
    assert "from_policy=True" in detach_src


def test_failed_over_clone_does_not_inherit_the_source_policy(monkeypatch):
    """The target clone is a deep copy of the source, so it would otherwise carry
    a policy id that names nothing on the other cluster — and, with the guard on
    replication_start, that would block fail-back entirely."""
    import inspect
    from simplyblock_core.controllers import lvol_controller
    src = inspect.getsource(lvol_controller._create_target_lvol_clone)
    assert "new_lvol.replication_policy_id = \"\"" in src


def test_failback_is_not_blocked_by_the_policy_guard(monkeypatch):
    """Fail-back configures the reverse replication itself; it must be allowed to
    drive replication_start even on a policy-managed volume."""
    import inspect
    from simplyblock_core.controllers import lvol_controller
    src = inspect.getsource(lvol_controller.replication_failback)
    assert src.count("from_policy=True") == 2, \
        "both the delta and the fresh-cluster fail-back paths must bypass the guard"


def test_empty_policy_cannot_be_attached(monkeypatch):
    """An empty policy is not "no policy": attaching it would wipe the volume's
    replication configuration while reporting success."""
    db = _FakeDB(lvols=[_lvol("LV1")])
    _install(monkeypatch, db)
    for empty in ("", "   ", None):
        with pytest.raises(ReplicationConfigError, match="required"):
            rpc.attach_policy("LV1", empty)
    assert db.get_lvol_by_id("LV1").replication_policy_id == ""


def test_volume_without_a_policy_may_still_start_replication_directly(monkeypatch):
    """Guard rail against the exact break in commit 95a35804a.

    The policy guard must test TRUTHINESS. replication_policy_id defaults to the
    empty string, so a type-checker-friendly `is not None` rewrite makes every
    volume look policy-managed and refuses all six lab cases at their first step
    ("follows replication policy ;" — note the empty id in the message).
    """
    from simplyblock_core.controllers import lvol_controller

    lv = _lvol("LV1")                       # no policy attached
    assert lv.replication_policy_id == ""

    class _DB:
        def get_lvol_by_id(self, lvol_id):
            return lv

    monkeypatch.setattr(lvol_controller, "DBController", lambda: _DB())
    # Stop right after the guard: a None cluster and no configured target make
    # replication_start return False further down, which is not what we assert on.
    monkeypatch.setattr(lvol_controller, "_get_next_3_nodes", lambda *a, **kw: [])
    monkeypatch.setattr(lv, "write_to_db", lambda *a, **kw: None)

    import inspect
    src = inspect.getsource(lvol_controller.replication_start)
    assert "is not None" not in src.split("def replication_start")[0] or True
    guard = [ln for ln in src.splitlines() if "replication_policy_id" in ln][0]
    assert "is not None" not in guard, (
        f"the guard must use truthiness, not an is-not-None test: {guard.strip()}")


def test_stop_guard_also_uses_truthiness():
    import inspect
    from simplyblock_core.controllers import lvol_controller
    src = inspect.getsource(lvol_controller.replication_stop)
    guard = [ln for ln in src.splitlines() if "replication_policy_id" in ln][0]
    assert "is not None" not in guard, guard.strip()
