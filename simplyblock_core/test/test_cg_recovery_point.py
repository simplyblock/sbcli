"""A consistency group's newest replicated generation: never deleted, and the
point a promote on the peer -- or a relocate back -- restores the group from
when its source volumes are gone (2026-10-04, WordPress relocate A -> B: the
demoted source volume was deleted, as a relocate requires, the group was
detached and emptied, and the promote on site B was refused with "not attached
to a replication policy")."""
from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.controllers import replication_recovery_points as rrp
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr
from simplyblock_core.test.test_replication_policies import (
    _done_replication_task,
    _FakeDB,
    _install,
    _lvol,
)

SRC, TGT = "CL_A", "CL_B"


class _DB(_FakeDB):
    def get_consistency_groups(self, cluster_id=None):
        return [g for g in self._groups if not cluster_id or g.cluster_id == cluster_id]

    def get_snapshots_by_node_id(self, node_id):
        return [s for s in self._snapshots if s.lvol and s.lvol.node_id == node_id]



def _recorder(deleted):
    """A stand-in for snapshot_controller.delete that records the ids it was given."""
    def delete(sid):
        deleted.append(sid)
        return True
    return delete

def _group(uuid, cluster, name="wp", policy_id="", members=None, last_seq=0):
    g = ConsistencyGroup()
    g.uuid, g.cluster_id, g.group_name, g.policy_id = uuid, cluster, name, policy_id
    g.members = members or {}
    g.last_group_seq = last_seq
    return g


def _vol(uuid, cluster, node="N", group="", demoted=""):
    v = _lvol(uuid)
    v.cluster_id = cluster
    v.node_id = node
    v.pool_uuid = f"POOL_{cluster}"
    v.group_id = group
    v.replication_demote_state = demoted
    return v


def _gsnap(uuid, lvol, cluster, group, seq, created, partner_tgt="", partner_src=""):
    s = SnapShot()
    s.uuid, s.lvol, s.cluster_id = uuid, lvol, cluster
    s.snap_type = SnapShot.TYPE_INTERNAL
    s.status = SnapShot.STATUS_ONLINE
    s.group_id, s.group_seq, s.created_at = group, seq, created
    s.target_replicated_snap_uuid = partner_tgt
    s.source_replicated_snap_uuid = partner_src
    return s


def _generation(db, group, seq, created, members, landing, with_copy=None):
    """Origin snapshots of *members* at generation *seq* on SRC, with a copy on
    TGT for each member in *with_copy* (default: all)."""
    with_copy = members if with_copy is None else with_copy
    for m in members:
        origin_id, copy_id = f"S{seq}_{m.uuid}", f"T{seq}_{m.uuid}"
        has_copy = m in with_copy
        db._snapshots.append(_gsnap(origin_id, m, SRC, group.get_id(), seq, created,
                                    partner_tgt=copy_id if has_copy else ""))
        if has_copy:
            db._snapshots.append(_gsnap(copy_id, landing, TGT, group.get_id(), seq, created + 2,
                                        partner_src=origin_id))


# ------------------------------------------------------------------ selection

def test_newest_complete_generation_skips_a_generation_not_every_member_shipped():
    db = _DB()
    g = _group("G", SRC)
    db._groups.append(g)
    m1, m2 = _vol("M1", SRC, group="G"), _vol("M2", SRC, group="G")
    landing = _vol("LAND", TGT, node="NB")
    _generation(db, g, 15, 1000, [m1, m2], landing)
    _generation(db, g, 16, 1300, [m1, m2], landing, with_copy=[m1])   # M2 not shipped yet
    seq, origins, copies = rrp.newest_group_generation("G", db=db)
    assert seq == 15
    assert {s.get_id() for s in copies} == {"T15_M1", "T15_M2"}


def test_generations_are_ordered_by_time_not_by_their_number():
    """A group emptied before the counter was kept monotonic restarted at 1: the
    later generation 1 is the newest, not the departed cycle's generation 15."""
    db = _DB()
    g = _group("G", SRC)
    db._groups.append(g)
    m = _vol("M1", SRC, group="G")
    landing = _vol("LAND", TGT, node="NB")
    _generation(db, g, 15, 1000, [m], landing)
    _generation(db, g, 1, 5000, [m], landing)
    assert rrp.newest_group_generation("G", db=db)[0] == 1


def test_copies_stand_for_a_generation_whose_origin_snapshots_are_gone():
    db = _DB()
    g = _group("G", SRC)
    db._groups.append(g)
    landing = _vol("LAND", TGT, node="NB")
    db._snapshots.append(_gsnap("T15_M1", landing, TGT, "G", 15, 1002, partner_src="S15_M1"))
    seq, origins, copies = rrp.newest_group_generation("G", db=db)
    assert (seq, origins, [c.get_id() for c in copies]) == (15, [], ["T15_M1"])


def test_generation_reset_keeps_the_counter():
    g = _group("G", SRC, last_seq=15)
    members = {"M1": {"joined_seq": 1, "removed_seq": 15}}
    assert cgc._reset_generation_if_emptied(g, members) == {}
    assert g.last_group_seq == 15, "the next cycle must number after the surviving generation"


# ------------------------------------------------------------------ promote on the peer

def _incident(monkeypatch):
    """WordPress after the relocate's demote: the source volume is deleted, the
    group on SRC is detached and empty, generation 15 survives on both sides."""
    db = _DB()
    _install(monkeypatch, db)
    monkeypatch.setattr(cgc, "db", db)
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db, raising=False)
    g = _group("G", SRC, policy_id="")
    db._groups.append(g)
    deleted_source = _vol("SRC_VOL", SRC, group="G")            # embedded in its snapshot only
    landing = _vol("LAND", TGT, node="NB")
    db._lvols.append(landing)
    _generation(db, g, 15, 1000, [deleted_source], landing)
    return db, g, deleted_source


def test_a_detached_empty_group_is_failed_over_from_its_replicated_generation(monkeypatch):
    db, g, src = _incident(monkeypatch)
    calls = []

    def _clone(template, copy, source_cluster_id):
        calls.append((template.get_id(), copy.get_id(), source_cluster_id))
        return {"lvol_id": "CLONE", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "failover_from_replicated_copy", _clone)
    results = rpc.failover_group(g)
    assert calls == [("SRC_VOL", "T15_SRC_VOL", SRC)], \
        "clone from the peer's copy of the newest generation, the source record as template"
    assert results == [{"lvol_id": "SRC_VOL", "status": "failed_over", "target_lvol_id": "CLONE",
                        "connection_strings": [], "warnings": []}]


def test_the_promote_is_idempotent_on_a_copy_that_already_has_a_clone(monkeypatch):
    db, g, src = _incident(monkeypatch)
    clone = _vol("CLONE", TGT, node="NB")
    clone.cloned_from_snap = "T15_SRC_VOL"
    db._lvols.append(clone)
    monkeypatch.setattr(rpc.lvol_controller, "failover_from_replicated_copy",
                        lambda *a: (_ for _ in ()).throw(AssertionError("cloned twice")))
    results = rpc.failover_group(g)
    assert results[0]["status"] == "failed_over" and results[0]["target_lvol_id"] == "CLONE"


def test_a_re_promote_after_the_move_is_a_no_op(monkeypatch):
    """Ramen re-drives the promote every reconcile. Once the peer group holds the
    serving clone (not demoted, its node online), nothing is cloned -- in
    particular not 'home' to the old site."""
    db, g, src = _incident(monkeypatch)
    peer = _group("G_B", TGT, members={"CLONE": {"joined_seq": 16, "removed_seq": 0}})
    db._groups.append(peer)
    clone = _vol("CLONE", TGT, node="NB", group=peer.get_id())
    db._lvols.append(clone)
    node = StorageNode()
    node.uuid, node.status = "NB", StorageNode.STATUS_ONLINE
    db._nodes.append(node)
    monkeypatch.setattr(rpc.lvol_controller, "replication_source_online", lambda m: True)
    for name in ("failover_from_replicated_copy", "replicate_lvol_on_target_cluster"):
        monkeypatch.setattr(rpc.lvol_controller, name,
                            lambda *a, **k: (_ for _ in ()).throw(AssertionError("cloned")))
    results = rpc.failover_group(g)
    assert results == [{"lvol_id": "CLONE", "status": "failed_over", "target_lvol_id": "CLONE"}]


def test_a_relocate_back_clones_the_group_home_without_the_old_source_or_a_policy(monkeypatch):
    """After the promote on B, relocate B -> A: B's members are demoted, their
    demote generation shipped to A, the old source volume on A is long gone and
    the group on A is detached. The promote on A must clone B's members home,
    pinned to that generation."""
    db, g, _ = _incident(monkeypatch)
    peer = _group("G_B", TGT, members={"CLONE": {"joined_seq": 16, "removed_seq": 0}})
    db._groups.append(peer)
    clone = _vol("CLONE", TGT, node="NB", group=peer.get_id(), demoted=LVol.REPLICATION_DEMOTE_DONE)
    db._lvols.append(clone)
    home_landing = _vol("HOME_LAND", SRC)
    db._snapshots.extend([
        _gsnap("D1_CLONE", clone, TGT, peer.get_id(), 1, 2000, partner_tgt="H1_CLONE"),
        _gsnap("H1_CLONE", home_landing, SRC, peer.get_id(), 1, 2002, partner_src="D1_CLONE")])
    db._tasks.append(_done_replication_task("D1_CLONE"))
    pins = {}

    def _home(lvol_id, pin_snapshot_id=None):
        pins[lvol_id] = pin_snapshot_id
        return {"lvol_id": f"HOME_{lvol_id}", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster", _home)
    results = rpc.failover_group(g)
    assert pins == {"CLONE": "D1_CLONE"}
    assert results[0]["status"] == "failed_over"


# ------------------------------------------------------------------ never deleted

def test_detach_purge_keeps_the_groups_newest_complete_generation(monkeypatch):
    """M1 shipped generation 16, M2 did not: the group's restore point is 15. The
    purge keeps M1's own newest (16) AND its generation-15 pair."""
    db = _DB()
    _install(monkeypatch, db)
    g = _group("G", SRC)
    db._groups.append(g)
    m1, m2 = _vol("M1", SRC, group="G"), _vol("M2", SRC, group="G")
    db._lvols.extend([m1, m2])
    landing = _vol("LAND", TGT, node="NB")
    _generation(db, g, 14, 700, [m1, m2], landing)
    _generation(db, g, 15, 1000, [m1, m2], landing)
    _generation(db, g, 16, 1300, [m1, m2], landing, with_copy=[m1])
    deleted: list[str] = []
    monkeypatch.setattr(rpc.snapshot_controller, "delete", _recorder(deleted))
    monkeypatch.setattr(rpc, "_has_dependent_clone", lambda sid: False)
    rpc._purge_internal_replication_snapshots("M1")
    for kept in ("S15_M1", "T15_M1", "S16_M1", "T16_M1"):
        assert kept not in deleted, f"{kept} deleted: {deleted}"
    assert "S14_M1" in deleted and "T14_M1" in deleted, "older generations still go"


def test_retention_keeps_the_groups_newest_complete_generation(monkeypatch):
    db = _DB()
    g = _group("G", SRC)
    db._groups.append(g)
    m1, m2 = _vol("M1", SRC, group="G"), _vol("M2", SRC, group="G")
    landing = _vol("LAND", TGT, node="NB")
    _generation(db, g, 15, 1000, [m1, m2], landing)
    for seq, t in ((16, 1300), (17, 1600), (18, 1900)):
        _generation(db, g, seq, t, [m1, m2], landing, with_copy=[m1])   # M2 lags
    monkeypatch.setattr(sr, "db", db)
    monkeypatch.setattr(sr, "_keep_replicated_for", lambda lv: 2)
    monkeypatch.setattr(sr, "_retention_schedule_for", lambda lv: [])
    monkeypatch.setattr(sr, "_successor_is_chained_to", lambda a, b: True)
    monkeypatch.setattr(sr, "_has_dependent_clone", lambda sid: False)
    deleted: list[str] = []
    monkeypatch.setattr(sr.snapshot_controller, "delete", _recorder(deleted))
    sr._prune_internal_snapshots(m1)
    assert "S15_M1" not in deleted and "T15_M1" not in deleted, deleted
    assert "S16_M1" in deleted and "T16_M1" in deleted, "an ordinary superseded pair still goes"


def test_retiring_a_clone_keeps_its_base_when_it_is_the_groups_restore_point(monkeypatch):
    db = _DB()
    g = _group("G", SRC)
    db._groups.append(g)
    m = _vol("M1", SRC, group="G")
    landing = _vol("LAND", TGT, node="NB")
    _generation(db, g, 15, 1000, [m], landing)
    _generation(db, g, 14, 700, [m], landing)
    deleted: list[str] = []
    monkeypatch.setattr(lvol_controller.snapshot_controller, "delete",
                        _recorder(deleted))
    lvol_controller._delete_base_unless_recovery_point(db, "T15_M1")
    lvol_controller._delete_base_unless_recovery_point(db, "T14_M1")
    assert deleted == ["T14_M1"]


def test_failover_from_a_copy_records_the_relationship_and_rejoins_the_group(monkeypatch):
    """The PV still names the deleted source's handle: the relationship
    template -> clone (FAILED_OVER) is what resolves it to the clone, and the
    clone rejoins the group on the peer so a relocate back finds it."""
    import contextlib
    db, g, src = _incident(monkeypatch)
    node = StorageNode()
    node.uuid, node.status, node.cluster_id = "NB", StorageNode.STATUS_ONLINE, TGT
    db._nodes.append(node)
    copy = db.get_snapshot_by_id("T15_SRC_VOL")
    copy.lvol.node_id = "NB"
    clone = _vol("CLONE", TGT, node="NB")
    written, rejoined = [], []
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)
    monkeypatch.setattr(lvol_controller.snapshot_controller, "object_mutation_lock",
                        lambda *a: contextlib.nullcontext())
    monkeypatch.setattr(lvol_controller, "_create_target_lvol_clone",
                        lambda dbc, template, tnode, pool, snap: (clone, None))
    monkeypatch.setattr(lvol_controller, "_persist_clone_reclaiming_ghost_unique", lambda *a: None)
    monkeypatch.setattr(LVolReplication, "write_to_db", lambda self, kv=None: written.append(self))
    monkeypatch.setattr(lvol_controller, "connect_lvol", lambda lid: ([], None))
    monkeypatch.setattr(cgc, "reconstitute_group_after_handoff",
                        lambda s, d, c: rejoined.append((s.get_id(), d.get_id(), c)))
    ret = lvol_controller.failover_from_replicated_copy(src, copy, SRC)
    assert ret["lvol_id"] == "CLONE"
    rep = written[0]
    assert (rep.source_lvol.get_id(), rep.target_lvol.get_id(), rep.state) == \
        ("SRC_VOL", "CLONE", LVolReplication.STATE_FAILED_OVER)
    assert (rep.source_cluster_id, rep.target_cluster_id) == (SRC, TGT)
    assert rejoined == [("SRC_VOL", "CLONE", TGT)]


def test_deleting_the_newest_replicated_generation_is_refused(monkeypatch):
    db = _DB()
    g = _group("G", SRC)
    db._groups.append(g)
    m = _vol("M1", SRC, group="G")
    landing = _vol("LAND", TGT, node="NB")
    _generation(db, g, 14, 700, [m], landing)
    _generation(db, g, 15, 1000, [m], landing)
    monkeypatch.setattr(cgc, "db", db)
    monkeypatch.setattr(cgc, "_group_snapshots",
                        lambda grp: [s for s in db._snapshots if s.group_id == grp.get_id()])
    deleted: list[str] = []
    from simplyblock_core.controllers import snapshot_controller
    monkeypatch.setattr(snapshot_controller, "delete", _recorder(deleted))
    ids, err = cgc.delete_generation(g, 15)
    assert ids is None and "newest replicated generation" in err and deleted == []
    ids, err = cgc.delete_generation(g, 14)
    assert err is None and sorted(ids) == ["S14_M1", "T14_M1"]


# ------------------------------------------------------------------ replicating back

from simplyblock_core.models.replication import (  # noqa: E402
    ReplicationPolicy,
    ReplicationTarget,
)


def _reverse_policy(db, uuid, cluster, toward, name, status=ReplicationPolicy.STATUS_ACTIVE):
    """A policy on *cluster* replicating toward *toward*, with its target."""
    t = ReplicationTarget()
    t.uuid, t.cluster_id, t.target_cluster_id = f"T_{uuid}", cluster, toward
    t.target_name = f"simplyblock-repl-{toward}"
    p = ReplicationPolicy()
    p.uuid, p.cluster_id, p.policy_name, p.target_id = uuid, cluster, name, t.get_id()
    p.status = status
    db.written.extend([t, p])
    return p


def _clone_into(db, peer):
    """A stand-in for failover_from_replicated_copy: the clone appears on the
    peer and joins the peer group, as the real hand-off does."""
    def clone(template, copy, source_cluster_id):
        db._lvols.append(_vol("CLONE", TGT, node="NB", group=peer.get_id()))
        return {"lvol_id": "CLONE", "connection_strings": []}
    return clone


def _record_attach(monkeypatch):
    attached: list[tuple[str, str]] = []

    def attach(group, policy_id):
        attached.append((group.get_id(), policy_id))
        return group
    monkeypatch.setattr(cgc, "attach_group_policy", attach)
    return attached


def test_a_group_failed_over_from_its_copy_replicates_back_toward_the_old_source(monkeypatch):
    """The incident: after the promote on B, the group holding the clone followed
    no policy, nothing replicated back to A and Ramen waited for a first sync
    forever. It must be attached to B's policy toward A."""
    db, g, _ = _incident(monkeypatch)
    peer = _group("G_B", TGT)
    db._groups.append(peer)
    toward_a = _reverse_policy(db, "P_B", TGT, SRC, "sb-dr-realbed-primary-to-site-a")
    _reverse_policy(db, "P_A", SRC, TGT, "sb-dr-realbed-primary-to-site-b")
    attached = _record_attach(monkeypatch)
    monkeypatch.setattr(rpc.lvol_controller, "failover_from_replicated_copy", _clone_into(db, peer))
    rpc.failover_group(g)
    assert attached == [(peer.get_id(), toward_a.get_id())]


def test_a_re_promote_heals_a_promoted_group_that_replicates_nowhere(monkeypatch):
    """Ramen re-drives the promote; the no-op path attaches a peer group still
    without a policy (the bed's state before this fix) and leaves an attached
    one alone."""
    db, g, _ = _incident(monkeypatch)
    peer = _group("G_B", TGT, members={"CLONE": {"joined_seq": 16, "removed_seq": 0}})
    db._groups.append(peer)
    db._lvols.append(_vol("CLONE", TGT, node="NB", group=peer.get_id()))
    node = StorageNode()
    node.uuid, node.status = "NB", StorageNode.STATUS_ONLINE
    db._nodes.append(node)
    monkeypatch.setattr(rpc.lvol_controller, "replication_source_online", lambda m: True)
    toward_a = _reverse_policy(db, "P_B", TGT, SRC, "sb-dr-realbed-primary-to-site-a")
    attached = _record_attach(monkeypatch)
    rpc.failover_group(g)
    assert attached == [(peer.get_id(), toward_a.get_id())]
    peer.policy_id = toward_a.get_id()
    rpc.failover_group(g)
    assert len(attached) == 1, "a group already following a policy is not re-attached"


def test_a_relocate_back_replicates_from_home_toward_the_peer(monkeypatch):
    db, g, _ = _incident(monkeypatch)
    peer = _group("G_B", TGT, members={"CLONE": {"joined_seq": 16, "removed_seq": 0}})
    db._groups.append(peer)
    clone = _vol("CLONE", TGT, node="NB", group=peer.get_id(), demoted=LVol.REPLICATION_DEMOTE_DONE)
    db._lvols.append(clone)
    home_landing = _vol("HOME_LAND", SRC)
    db._snapshots.extend([
        _gsnap("D1_CLONE", clone, TGT, peer.get_id(), 1, 2000, partner_tgt="H1_CLONE"),
        _gsnap("H1_CLONE", home_landing, SRC, peer.get_id(), 1, 2002, partner_src="D1_CLONE")])
    db._tasks.append(_done_replication_task("D1_CLONE"))
    toward_b = _reverse_policy(db, "P_A", SRC, TGT, "sb-dr-realbed-primary-to-site-b")
    attached = _record_attach(monkeypatch)
    monkeypatch.setattr(rpc.lvol_controller, "replicate_lvol_on_target_cluster",
                        lambda lvol_id, pin_snapshot_id=None: {"lvol_id": f"HOME_{lvol_id}",
                                                               "connection_strings": []})
    rpc.failover_group(g)
    assert attached == [(g.get_id(), toward_b.get_id())]


def test_the_reverse_policy_is_the_active_one_of_the_same_family(monkeypatch):
    db, g, _ = _incident(monkeypatch)
    _reverse_policy(db, "P_OFF", TGT, SRC, "sb-dr-realbed-primary-to-site-a",
                    status=ReplicationPolicy.STATUS_INACTIVE)
    _reverse_policy(db, "P_OTHER", TGT, SRC, "aaa-other-plan-to-site-a")
    same = _reverse_policy(db, "P_SAME", TGT, SRC, "sb-dr-realbed-primary-to-site-a")
    _reverse_policy(db, "P_ELSEWHERE", TGT, "CL_C", "sb-dr-realbed-primary-to-site-c")
    pol = cgc.reverse_replication_policy(TGT, SRC, "sb-dr-realbed-primary-to-site-b")
    assert pol is not None and pol.get_id() == same.get_id()
    assert cgc.reverse_replication_policy(TGT, "CL_NONE") is None


def test_no_reverse_policy_leaves_the_promote_standing(monkeypatch):
    db, g, _ = _incident(monkeypatch)
    peer = _group("G_B", TGT)
    db._groups.append(peer)
    attached = _record_attach(monkeypatch)
    monkeypatch.setattr(rpc.lvol_controller, "failover_from_replicated_copy", _clone_into(db, peer))
    results = rpc.failover_group(g)
    assert attached == []
    assert results[0]["status"] == "failed_over"
