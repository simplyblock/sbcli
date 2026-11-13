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


def test_a_failed_over_group_replicates_back_toward_its_source(monkeypatch):
    """The central fail-over attaches the clones' group to this cluster's policy
    toward the source (_attach_promoted_peer); csi-addons calls no Enable after a
    promote. Without it the group ran unprotected after the heal (bed run
    20261112-1712-ebs-splitdr, M3)."""
    db, landing = _site_b(monkeypatch)
    for m in ("M1", "M2"):
        _copy(db, landing, m, 15, 1000, ["M1", "M2"])
    group_here = ConsistencyGroup()
    group_here.uuid, group_here.cluster_id, group_here.group_name = "GB", TGT, "gitea"
    db._groups.append(group_here)
    for m in ("M1", "M2"):
        db._lvols.append(_vol(f"CLONE_{m}", TGT, node="NB", group=f"{TGT}/GB"))
    _record_clones(monkeypatch)
    attached = []
    monkeypatch.setattr(rpc, "_attach_reverse_replication",
                        lambda group, toward, former: attached.append((group.get_id(), toward)))

    rpc.failover_replicated_group(TGT, GROUP)

    assert attached == [(f"{TGT}/GB", SRC)]


def test_a_fail_over_that_cloned_nothing_attaches_nothing(monkeypatch):
    db, landing = _site_b(monkeypatch)
    _copy(db, landing, "M1", 15, 1000, ["M1", "M2"])          # no complete generation
    _record_clones(monkeypatch)
    attached = []
    monkeypatch.setattr(rpc, "_attach_reverse_replication",
                        lambda group, toward, former: attached.append((group.get_id(), toward)))

    rpc.failover_replicated_group(TGT, GROUP)

    assert attached == []


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


# ------------------------------------------------------------------ resolution

def _failed_over(monkeypatch):
    """Site B after the fail-over of M1: its clone in group gitea here, and the
    relationship the fail-over recorded, from the source volume the copy named."""
    db, landing = _site_b(monkeypatch)
    _copy(db, landing, "M1", 15, 1000, ["M1"])
    node = StorageNode()
    node.uuid, node.status, node.cluster_id = "NB", StorageNode.STATUS_ONLINE, TGT
    db._nodes.append(node)
    group = ConsistencyGroup()
    group.uuid, group.cluster_id, group.group_name = "GB", TGT, "gitea"
    db._groups.append(group)
    clone = _vol("CLONE", TGT, node="NB", group=group.get_id())
    db._lvols.append(clone)
    source = LVol().from_dict(dict(_source_volume("M1"), pool_uuid="POOL_A"))
    rel = LVolReplication()
    rel.uuid, rel.source_lvol, rel.target_lvol = "R1", source, clone
    rel.source_cluster_id, rel.target_cluster_id = SRC, TGT
    rel.state = LVolReplication.STATE_FAILED_OVER
    db._replications.append(rel)
    return db


def test_a_failed_over_group_resolves_to_its_group_here(monkeypatch):
    """The CSI driver asks this site where the cut-off source's group lives now:
    the status of the VolumeGroupReplication, which keeps the source's group
    handle, comes from the group the fail-over made here."""
    _failed_over(monkeypatch)
    res = rpc.resolve_replicated_group(GROUP)
    assert (res["active_cluster_id"], res["active_group_id"]) == (TGT, "GB")
    assert res["members"] == [{"origin_handle": f"{SRC}:POOL_A:M1",
                               "active_handle": f"{TGT}:POOL_{TGT}:CLONE"}]


def test_a_group_not_failed_over_here_resolves_to_nothing(monkeypatch):
    """Copies but no fail-over yet: the source's control plane still serves the
    group, and the caller asks it."""
    db, landing = _site_b(monkeypatch)
    _copy(db, landing, "M1", 15, 1000, ["M1"])
    res = rpc.resolve_replicated_group(GROUP)
    assert (res["active_cluster_id"], res["active_group_id"], res["members"]) == ("", "", [])
