"""A group fail-over on the target cluster with the source cluster cut off
(control-plane split, step 3): the target's database holds only the copies,
and the metadata each copy carries (source_volume, source_group) stands in for
the source volumes, snapshots and group record it cannot read."""
import contextlib

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.controllers import replication_recovery_points as rrp
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.test.test_cg_recovery_point import _DB, _gsnap, _vol
from simplyblock_core.test.test_replication_policies import _install

SRC, TGT = "CL_A", "CL_B"
GROUP = f"{SRC}/G"


def _source_volume(uuid):
    v = LVol()
    v.uuid = uuid
    v.lvol_name = f"pvc-{uuid.lower()}"
    v.nqn = f"nqn.2023-02.io.simplyblock:{uuid.lower()}"
    v.size = 1 << 30
    v.group_id = GROUP
    return v.to_dict()


def _copy(db, landing, member, seq, created, members):
    """The copy on TGT of *member*'s snapshot at generation *seq*, as finish
    records it: no origin on this cluster, the metadata on the copy."""
    c = _gsnap(f"T{seq}_{member}", landing, TGT, GROUP, seq, created,
               partner_src=f"S{seq}_{member}")
    c.source_volume = _source_volume(member)
    c.source_group = {"group_name": "gitea", "policy_id": f"{SRC}/P", "members": members}
    db._snapshots.append(c)
    return c


def _site_b(monkeypatch):
    db = _DB()
    _install(monkeypatch, db)
    monkeypatch.setattr(cgc, "db", db)
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db, raising=False)
    landing = _vol("LAND", TGT, node="NB")
    db._lvols.append(landing)
    return db, landing


# ------------------------------------------------------------------ completeness

def test_a_generation_missing_a_members_copy_is_not_complete(monkeypatch):
    db, landing = _site_b(monkeypatch)
    for m in ("M1", "M2"):
        _copy(db, landing, m, 15, 1000, ["M1", "M2"])
    _copy(db, landing, "M1", 16, 1300, ["M1", "M2"])          # M2's copy of 16 not here yet
    seq, origins, copies = rrp.newest_group_generation(GROUP, db=db)
    assert seq == 15
    assert {c.get_id() for c in copies} == {"T15_M1", "T15_M2"}


def test_a_generation_with_every_members_copy_is_complete(monkeypatch):
    db, landing = _site_b(monkeypatch)
    for m in ("M1", "M2"):
        _copy(db, landing, m, 15, 1000, ["M1", "M2"])
        _copy(db, landing, m, 16, 1300, ["M1", "M2"])
    assert rrp.newest_group_generation(GROUP, db=db)[0] == 16


# ------------------------------------------------------------------ fail-over

def _record_clones(monkeypatch):
    calls = []

    def _clone(template, copy, source_cluster_id):
        calls.append((template.get_id(), template.lvol_name, template.nqn, copy.get_id(),
                      source_cluster_id))
        return {"lvol_id": f"CLONE_{template.get_id()}", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "failover_from_replicated_copy", _clone)
    return calls


def test_the_group_fails_over_from_its_copies_with_no_source_records(monkeypatch):
    db, landing = _site_b(monkeypatch)
    for m in ("M1", "M2"):
        _copy(db, landing, m, 15, 1000, ["M1", "M2"])
    calls = _record_clones(monkeypatch)
    results = rpc.failover_replicated_group(TGT, GROUP)
    assert sorted(calls) == [
        ("M1", "pvc-m1", "nqn.2023-02.io.simplyblock:m1", "T15_M1", SRC),
        ("M2", "pvc-m2", "nqn.2023-02.io.simplyblock:m2", "T15_M2", SRC),
    ], "each clone keeps its source volume's identity, from the copy's record"
    assert sorted(r["status"] for r in results) == ["failed_over", "failed_over"]


def test_a_group_with_no_complete_generation_here_fails_over_nothing(monkeypatch):
    db, landing = _site_b(monkeypatch)
    _copy(db, landing, "M1", 15, 1000, ["M1", "M2"])
    calls = _record_clones(monkeypatch)
    assert rpc.failover_replicated_group(TGT, GROUP) == []
    assert calls == []


def test_a_copy_without_its_source_volume_is_refused(monkeypatch):
    """A copy recorded before the source sent its volume and group: cloning it as the
    landing volume would give the clone the wrong name and NQN."""
    db, landing = _site_b(monkeypatch)
    c = _copy(db, landing, "M1", 15, 1000, ["M1"])
    c.source_volume, c.source_group = {}, {}
    calls = _record_clones(monkeypatch)
    results = rpc.failover_replicated_group(TGT, GROUP)
    assert calls == []
    assert results[0]["status"] == "failed"
    assert "source volume" in results[0]["detail"]


def test_a_second_promote_reports_the_clone_it_made(monkeypatch):
    db, landing = _site_b(monkeypatch)
    _copy(db, landing, "M1", 15, 1000, ["M1"])
    clone = _vol("CLONE", TGT, node="NB")
    clone.cloned_from_snap = "T15_M1"
    db._lvols.append(clone)
    monkeypatch.setattr(rpc.lvol_controller, "failover_from_replicated_copy",
                        lambda *a: (_ for _ in ()).throw(AssertionError("cloned twice")))
    results = rpc.failover_replicated_group(TGT, GROUP)
    assert results[0]["status"] == "failed_over" and results[0]["target_lvol_id"] == "CLONE"


# ------------------------------------------------------------------ the group on this side

def test_the_clone_rejoins_a_group_of_the_copys_group_name(monkeypatch):
    """No source group record here: the group is named from the copy."""
    db, landing = _site_b(monkeypatch)
    node = StorageNode()
    node.uuid, node.status, node.cluster_id = "NB", StorageNode.STATUS_ONLINE, TGT
    db._nodes.append(node)
    copy = _copy(db, landing, "M1", 15, 1000, ["M1"])
    template = LVol().from_dict(copy.source_volume)
    clone = _vol("CLONE", TGT, node="NB")
    rejoined = []
    monkeypatch.setattr(lvol_controller.snapshot_controller, "object_mutation_lock",
                        lambda *a: contextlib.nullcontext())
    monkeypatch.setattr(lvol_controller, "_create_target_lvol_clone",
                        lambda dbc, template, tnode, pool, snap: (clone, None))
    monkeypatch.setattr(lvol_controller, "_persist_clone_reclaiming_ghost_unique", lambda *a: None)
    monkeypatch.setattr(LVolReplication, "write_to_db", lambda self, kv=None: None)
    monkeypatch.setattr(lvol_controller, "connect_lvol", lambda lid: ([], None))
    def _ensure_group(cluster, name):
        rejoined.append((cluster, name))
        g = ConsistencyGroup()
        g.uuid, g.cluster_id, g.group_name = "GB", cluster, name
        return g

    monkeypatch.setattr(cgc, "ensure_group", _ensure_group)
    monkeypatch.setattr(cgc, "add_member_to_group", lambda group, lvol: None)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    lvol_controller.failover_from_replicated_copy(template, copy, SRC)
    assert rejoined == [(TGT, "gitea")]
    assert clone.group_id == f"{TGT}/GB"
