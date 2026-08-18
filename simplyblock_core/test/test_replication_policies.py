"""Replication targets, policies, volume assignment and group fail-over.

target -> policy -> volume, replacing the single cluster-scoped destination that
every `cluster add-replication` overwrote.
"""
import pytest

from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.controllers.replication_policy_controller import ReplicationConfigError
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.replication import ReplicationPolicy, ReplicationTarget
from simplyblock_core.models.snapshot import SnapShot


class _FakeDB:
    kv_store = object()

    def __init__(self, clusters=("CL_SRC", "CL_TGT"), pools=(), lvols=(),
                 snapshots=(), replications=()):
        self._clusters = list(clusters)
        self._pools = list(pools)
        self._lvols = list(lvols)
        self._snapshots = list(snapshots)
        self._replications = list(replications)
        self.written = []
        self.removed = []

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

    def get_lvols(self):
        return self._lvols

    def get_mini_lvols(self):
        return self._lvols

    def get_snapshots(self):
        return self._snapshots

    def get_snapshot_by_id(self, uuid):
        if not uuid:
            raise KeyError('Snapshot lookup with a blank id')
        for s in self._snapshots:
            if s.get_id() == uuid:
                return s
        raise KeyError(f'Snapshot {uuid} not found')

    def get_lvol_replication_objects(self):
        return self._replications


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


def _lvol(uuid, policy_id="", status=LVol.STATUS_ONLINE):
    lv = LVol()
    lv.uuid = uuid
    lv.status = status
    lv.replication_policy_id = policy_id
    return lv


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
    assert calls == {"replication_cluster_id": "CL_TGT", "mode": "migration", "interval_min": 7}
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

    order = []
    monkeypatch.setattr(rpc.lvol_controller, "replication_stop",
                        lambda lvol_id: order.append("stop") or True)
    monkeypatch.setattr(rpc, "_purge_internal_replication_snapshots",
                        lambda lvol_id: order.append("purge") or 0)

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
    starts = []
    monkeypatch.setattr(rpc.lvol_controller, "replication_start",
                        lambda lvol_id, **kw: starts.append(lvol_id) or True)
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
    stopped = []
    monkeypatch.setattr(rpc.lvol_controller, "replication_stop",
                        lambda lvol_id: stopped.append(lvol_id) or True)
    monkeypatch.setattr(rpc, "_purge_internal_replication_snapshots", lambda lvol_id: 4)
    assert rpc.detach_policy("LV1") is True
    assert stopped == ["LV1"]
    assert db.get_lvol_by_id("LV1").replication_policy_id == ""


# --------------------------------------------------------------------------- #
# Purge
# --------------------------------------------------------------------------- #

def _snap(uuid, lvol, snap_type=SnapShot.TYPE_INTERNAL, target=""):
    s = SnapShot()
    s.uuid = uuid
    s.lvol = lvol
    s.snap_type = snap_type
    s.target_replicated_snap_uuid = target
    return s


def test_purge_deletes_internal_snapshots_on_both_sides(monkeypatch):
    lv = _lvol("LV1")
    # The target copy belongs to the REP_ receiving volume on the other cluster,
    # not to the source volume.
    remote = _lvol("REP_LV1")
    src = _snap("S_SRC", lv, target="S_TGT")
    tgt = _snap("S_TGT", remote)
    db = _FakeDB(lvols=[lv, remote], snapshots=[src, tgt])
    _install(monkeypatch, db)
    deleted = []
    monkeypatch.setattr(rpc.snapshot_controller, "delete",
                        lambda uuid, **kw: deleted.append(uuid) or True)
    rpc._purge_internal_replication_snapshots("LV1")
    assert deleted == ["S_TGT", "S_SRC"], "target copy first, then the source snapshot"


def test_purge_never_touches_user_snapshots(monkeypatch):
    lv = _lvol("LV1")
    user = _snap("S_USER", lv, snap_type=SnapShot.TYPE_USER, target="S_USER_TGT")
    db = _FakeDB(lvols=[lv], snapshots=[user])
    _install(monkeypatch, db)
    deleted = []
    monkeypatch.setattr(rpc.snapshot_controller, "delete",
                        lambda uuid, **kw: deleted.append(uuid) or True)
    rpc._purge_internal_replication_snapshots("LV1")
    assert deleted == []


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
    deleted = []
    monkeypatch.setattr(rpc.snapshot_controller, "delete",
                        lambda uuid, **kw: deleted.append(uuid) or True)
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
