"""Where a consistency group's data lives now, keyed by the handles its PVs keep
(replication_policy_controller.resolve_group).

After a relocate the source group a VGR names is empty -- its demoted members
are deleted so a relocate back is possible -- and the data lives in the peer
group of the same name, as clones whose relationships lead back to the original
volumes. The CSI driver resolves the original group handle through this
(2026-10-04: WordPress's VRG on site B waited for destination info for ever,
because the driver listed the members of the emptied source group)."""
from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.test.test_replication_policies import _FakeDB, _install, _lvol

SRC, TGT = "CL_A", "CL_B"


class _DB(_FakeDB):
    def get_consistency_groups(self, cluster_id=None):
        return [g for g in self._groups if not cluster_id or g.cluster_id == cluster_id]


def _node(uuid, cluster):
    n = StorageNode()
    n.uuid, n.cluster_id, n.status = uuid, cluster, StorageNode.STATUS_ONLINE
    return n


def _group(uuid, cluster, name="wordpress"):
    g = ConsistencyGroup()
    g.uuid, g.cluster_id, g.group_name, g.policy_id = uuid, cluster, name, ""
    g.members = {}
    return g


def _vol(uuid, cluster, group=""):
    v = _lvol(uuid)
    v.cluster_id = cluster
    v.node_id = f"N_{cluster}"
    v.pool_uuid = f"POOL_{cluster}"
    v.group_id = group
    v.status = LVol.STATUS_ONLINE
    return v


def _rep(source, target, state=LVolReplication.STATE_FAILED_OVER):
    r = LVolReplication()
    r.uuid = f"R_{source.uuid}_{target.uuid}"
    r.source_lvol, r.target_lvol = source, target
    r.source_cluster_id, r.target_cluster_id = source.cluster_id, target.cluster_id
    r.state = state
    return r


def _db(monkeypatch):
    db = _DB(nodes=[_node(f"N_{SRC}", SRC), _node(f"N_{TGT}", TGT)])
    _install(monkeypatch, db)
    return db


def _h(v):
    return f"{v.cluster_id}:{v.pool_uuid}:{v.uuid}"


def test_a_group_emptied_by_a_relocate_resolves_to_its_peer_and_keeps_the_pv_handles(monkeypatch):
    """The incident: the source volume was deleted after the demote (it is only
    in the relationship record), the source group is empty, the clone lives in
    the peer group of the same name on the target."""
    db = _db(monkeypatch)
    src_group, peer = _group("G_A", SRC), _group("G_B", TGT)
    db._groups += [src_group, peer]
    original = _vol("ORIG", SRC, group="CL_A/G_A")            # deleted: not in db._lvols
    clone = _vol("CLONE", TGT, group="CL_B/G_B")
    db._lvols.append(clone)
    db._replications.append(_rep(original, clone))

    res = rpc.resolve_group(src_group)

    assert (res["active_cluster_id"], res["active_group_id"]) == (TGT, "G_B")
    assert res["members"] == [{"origin_handle": _h(original), "active_handle": _h(clone)}]


def test_a_group_still_live_at_its_source_resolves_to_itself(monkeypatch):
    db = _db(monkeypatch)
    g = _group("G_A", SRC)
    db._groups.append(g)
    m1, m2 = _vol("M1", SRC, group="CL_A/G_A"), _vol("M2", SRC, group="CL_A/G_A")
    db._lvols += [m1, m2]

    res = rpc.resolve_group(g)

    assert (res["active_cluster_id"], res["active_group_id"]) == (SRC, "G_A")
    assert res["members"] == [{"origin_handle": _h(m1), "active_handle": _h(m1)},
                              {"origin_handle": _h(m2), "active_handle": _h(m2)}]


def test_after_a_fail_back_the_original_handles_resolve_to_the_home_clones(monkeypatch):
    """Relocate A -> B (ORIG -> CLONE), then back B -> A (CLONE -> HOME): the PV
    still carries ORIG's handle; it resolves to HOME, which lives in the source
    group again."""
    db = _db(monkeypatch)
    src_group, peer = _group("G_A", SRC), _group("G_B", TGT)
    db._groups += [src_group, peer]
    original = _vol("ORIG", SRC, group="CL_A/G_A")
    clone = _vol("CLONE", TGT, group="CL_B/G_B")              # demoted and deleted after the way back
    home = _vol("HOME", SRC, group="CL_A/G_A")
    db._lvols.append(home)
    db._replications += [_rep(original, clone), _rep(clone, home)]

    res = rpc.resolve_group(src_group)

    assert (res["active_cluster_id"], res["active_group_id"]) == (SRC, "G_A")
    assert res["members"] == [{"origin_handle": _h(original), "active_handle": _h(home)}]


def test_the_peer_group_resolves_the_same_lineages(monkeypatch):
    """A VGR created on the target after the move names the peer group; it maps
    the same PV handles to the same live volumes (re-protection B -> A)."""
    db = _db(monkeypatch)
    src_group, peer = _group("G_A", SRC), _group("G_B", TGT)
    db._groups += [src_group, peer]
    original = _vol("ORIG", SRC, group="CL_A/G_A")
    clone = _vol("CLONE", TGT, group="CL_B/G_B")
    db._lvols.append(clone)
    db._replications.append(_rep(original, clone))

    res = rpc.resolve_group(peer)

    assert (res["active_cluster_id"], res["active_group_id"]) == (TGT, "G_B")
    assert res["members"] == [{"origin_handle": _h(original), "active_handle": _h(clone)}]


def test_a_lineage_whose_volumes_are_all_gone_is_left_out(monkeypatch):
    db = _db(monkeypatch)
    g = _group("G_A", SRC)
    db._groups.append(g)
    gone_src, gone_tgt = _vol("X", SRC, group="CL_A/G_A"), _vol("Y", TGT)
    db._replications.append(_rep(gone_src, gone_tgt))

    res = rpc.resolve_group(g)

    assert res == {"active_cluster_id": "", "active_group_id": "", "members": []}
