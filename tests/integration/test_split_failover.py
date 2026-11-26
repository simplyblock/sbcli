"""A group fail-over on the target cluster with the source cluster cut off
(control-plane split, step 3): the target's database holds only the copies,
and the metadata each copy carries (source_volume, source_group) stands in for
the source volumes, snapshots and group record it cannot read.

Real FDB: site B's cluster, node, pool, landing volume, copies, groups and
relationships are seeded and read back. The clone of a copy (node RPCs) is
mocked where a test is about what comes before or after it.
"""
import uuid

import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.controllers import replication_recovery_points as rrp
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.snapshot import SnapShot
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SRC = "split-failover-site-a"          # cut off: nothing of it is in site B's database
GROUP = f"{SRC}/G"
SOURCE_POOL = "split-failover-pool-a"

db = stack.db


class _SiteB:
    def __init__(self, db):
        self.db = db
        self.cluster = stack._seed_cluster(db)
        self.cluster.sync_replication = False
        self.cluster.write_to_db(db.kv_store)
        self.node = stack._seed_node(db, self.cluster, "b1", "")
        self.pool = publish._pool(db, self.cluster)
        self.landing = self.volume()

    @property
    def id(self):
        return self.cluster.get_id()

    def volume(self, **fields):
        vol = publish._volume(self.db, self.cluster, self.node, self.pool, active_site="",
                              nodes=[self.node.get_id()])
        for key, value in fields.items():
            setattr(vol, key, value)
        vol.write_to_db(self.db.kv_store)
        return vol

    def group(self, name="gitea"):
        group = ConsistencyGroup()
        group.uuid = str(uuid.uuid4())
        group.cluster_id = self.id
        group.group_name = name
        group.node_id = self.node.get_id()
        group.lvs_name = self.node.lvstore
        group.write_to_db(self.db.kv_store)
        return group

    def copy(self, member, seq, created, members, *, metadata=True):
        """The copy here of *member*'s snapshot at generation *seq*, as finish
        records it: no origin on this cluster, the metadata on the copy."""
        c = SnapShot()
        c.uuid = f"T{seq}_{member}-{uuid.uuid4().hex[:6]}"
        c.snap_name = f"REP_{c.uuid}"
        c.snap_bdev = f"{self.node.lvstore}/SNAP_{c.uuid}"
        c.lvol = self.landing
        c.cluster_id = self.id
        c.pool_uuid = self.pool.get_id()
        c.size = 1 << 30
        c.snap_type = SnapShot.TYPE_INTERNAL
        c.status = SnapShot.STATUS_ONLINE
        c.group_id, c.group_seq, c.created_at = GROUP, seq, created
        c.source_replicated_snap_uuid = f"S{seq}_{member}"
        if metadata:
            c.source_volume = _source_volume(member)
            c.source_group = {"group_name": "gitea", "policy_id": f"{SRC}/P", "members": members}
        c.write_to_db(self.db.kv_store)
        return c


def _source_volume(member):
    v = LVol()
    v.uuid = member
    v.lvol_name = f"pvc-{member.lower()}"
    v.nqn = f"nqn.2023-02.io.simplyblock:{member.lower()}"
    v.size = 1 << 30
    v.group_id = GROUP
    return v.to_dict()


@pytest.fixture()
def site_b(db):
    return _SiteB(db)


# ------------------------------------------------------------------ completeness

def test_a_generation_missing_a_members_copy_is_not_complete(db, site_b):
    first = [site_b.copy(m, 15, 1000, ["M1", "M2"]) for m in ("M1", "M2")]
    site_b.copy("M1", 16, 1300, ["M1", "M2"])          # M2's copy of 16 not here yet

    seq, origins, copies = rrp.newest_group_generation(GROUP, db=db)

    assert seq == 15
    assert {c.get_id() for c in copies} == {c.get_id() for c in first}


def test_a_generation_with_every_members_copy_is_complete(db, site_b):
    for m in ("M1", "M2"):
        site_b.copy(m, 15, 1000, ["M1", "M2"])
        site_b.copy(m, 16, 1300, ["M1", "M2"])

    assert rrp.newest_group_generation(GROUP, db=db)[0] == 16


# ------------------------------------------------------------------ fail-over

@pytest.fixture()
def clones(monkeypatch):
    """failover_from_replicated_copy (node RPCs) records each clone it is asked for."""
    calls = []

    def _clone(template, copy, source_cluster_id):
        calls.append((template.get_id(), template.lvol_name, template.nqn, copy.get_id(),
                      source_cluster_id))
        return {"lvol_id": f"CLONE_{template.get_id()}", "connection_strings": []}

    monkeypatch.setattr(rpc.lvol_controller, "failover_from_replicated_copy", _clone)
    return calls


@pytest.fixture()
def attached(monkeypatch):
    calls = []
    monkeypatch.setattr(rpc, "_attach_reverse_replication",
                        lambda group, toward, former: calls.append((group.get_id(), toward)))
    return calls


def test_the_group_fails_over_from_its_copies_with_no_source_records(db, site_b, clones, attached):
    copies = {m: site_b.copy(m, 15, 1000, ["M1", "M2"]) for m in ("M1", "M2")}

    results = rpc.failover_replicated_group(site_b.id, GROUP)

    assert sorted(clones) == [
        ("M1", "pvc-m1", "nqn.2023-02.io.simplyblock:m1", copies["M1"].get_id(), SRC),
        ("M2", "pvc-m2", "nqn.2023-02.io.simplyblock:m2", copies["M2"].get_id(), SRC),
    ], "each clone keeps its source volume's identity, from the copy's record"
    assert sorted(r["status"] for r in results) == ["failed_over", "failed_over"]


def test_a_group_with_no_complete_generation_here_fails_over_nothing(db, site_b, clones, attached):
    site_b.copy("M1", 15, 1000, ["M1", "M2"])

    assert rpc.failover_replicated_group(site_b.id, GROUP) == []
    assert clones == []


def test_a_copy_without_its_source_volume_is_refused(db, site_b, clones, attached):
    """A copy recorded before the source sent its volume and group: cloning it as the
    landing volume would give the clone the wrong name and NQN."""
    site_b.copy("M1", 15, 1000, ["M1"], metadata=False)

    results = rpc.failover_replicated_group(site_b.id, GROUP)

    assert clones == []
    assert results[0]["status"] == "failed"
    assert "source volume" in results[0]["detail"]


def test_a_second_promote_reports_the_clone_it_made(db, site_b, monkeypatch, attached):
    copy = site_b.copy("M1", 15, 1000, ["M1"])
    clone = site_b.volume(cloned_from_snap=copy.get_id())
    monkeypatch.setattr(rpc.lvol_controller, "failover_from_replicated_copy",
                        lambda *a: (_ for _ in ()).throw(AssertionError("cloned twice")))

    results = rpc.failover_replicated_group(site_b.id, GROUP)

    assert results[0]["status"] == "failed_over"
    assert results[0]["target_lvol_id"] == clone.get_id()


def test_a_failed_over_group_replicates_back_toward_its_source(db, site_b, monkeypatch, attached):
    """The central fail-over attaches the clones' group to this cluster's policy
    toward the source (_attach_promoted_peer); csi-addons calls no Enable after a
    promote. Without it the group ran unprotected after the heal (bed run
    20261112-1712-ebs-splitdr, M3)."""
    for m in ("M1", "M2"):
        site_b.copy(m, 15, 1000, ["M1", "M2"])
    group_here = site_b.group()
    made = {m: site_b.volume(group_id=group_here.get_id()) for m in ("M1", "M2")}
    monkeypatch.setattr(rpc.lvol_controller, "failover_from_replicated_copy",
                        lambda template, copy, source: {"lvol_id": made[template.get_id()].get_id(),
                                                        "connection_strings": []})

    rpc.failover_replicated_group(site_b.id, GROUP)

    assert attached == [(group_here.get_id(), SRC)]


def test_a_fail_over_that_cloned_nothing_attaches_nothing(db, site_b, clones, attached):
    site_b.copy("M1", 15, 1000, ["M1", "M2"])          # no complete generation

    rpc.failover_replicated_group(site_b.id, GROUP)

    assert attached == []


# ------------------------------------------------------------------ the group on this side

def test_the_clone_rejoins_a_group_of_the_copys_group_name(db, site_b, monkeypatch):
    """No source group record here: the group is named from the copy."""
    copy = site_b.copy("M1", 15, 1000, ["M1"])
    template = LVol().from_dict(copy.source_volume)
    clone = LVol().from_dict(site_b.landing.to_dict())
    clone.uuid = str(uuid.uuid4())
    clone.lvol_name = template.lvol_name
    clone.nqn = template.nqn
    clone.cloned_from_snap = copy.get_id()
    # The clone itself is made on the node (bdev_lvol_clone and the subsystem).
    monkeypatch.setattr(lvol_controller, "_create_target_lvol_clone",
                        lambda dbc, template, tnode, pool, snap: (clone, None))
    monkeypatch.setattr(lvol_controller, "connect_lvol", lambda lid: ([], None))

    lvol_controller.failover_from_replicated_copy(template, copy, SRC)

    group = db.get_consistency_group_by_name(site_b.id, "gitea")
    assert group is not None, "the clone's group is made here, named from the copy"
    assert db.get_lvol_by_id(clone.get_id()).group_id == group.get_id()


# ------------------------------------------------------------------ resolution

def _failed_over(db, site_b):
    """Site B after the fail-over of M1: its clone in group gitea here, and the
    relationship the fail-over recorded, from the source volume the copy named."""
    site_b.copy("M1", 15, 1000, ["M1"])
    group = site_b.group()
    clone = site_b.volume(group_id=group.get_id())
    source = LVol().from_dict(dict(_source_volume("M1"), pool_uuid=SOURCE_POOL))
    rel = LVolReplication()
    rel.uuid = str(uuid.uuid4())
    rel.source_lvol, rel.target_lvol = source, clone
    rel.source_cluster_id, rel.target_cluster_id = SRC, site_b.id
    rel.state = LVolReplication.STATE_FAILED_OVER
    rel.write_to_db(db.kv_store)
    return group, clone


def test_a_failed_over_group_resolves_to_its_group_here(db, site_b):
    """The CSI driver asks this site where the cut-off source's group lives now:
    the status of the VolumeGroupReplication, which keeps the source's group
    handle, comes from the group the fail-over made here."""
    group, clone = _failed_over(db, site_b)

    res = rpc.resolve_replicated_group(GROUP)

    assert (res["active_cluster_id"], res["active_group_id"]) == (site_b.id, group.uuid)
    assert res["members"] == [{
        "origin_handle": f"{SRC}:{SOURCE_POOL}:M1",
        "active_handle": f"{site_b.id}:{site_b.pool.get_id()}:{clone.get_id()}"}]


def test_a_group_not_failed_over_here_resolves_to_nothing(db, site_b):
    """Copies but no fail-over yet: the source's control plane still serves the
    group, and the caller asks it."""
    site_b.copy("M1", 15, 1000, ["M1"])

    res = rpc.resolve_replicated_group(GROUP)

    assert (res["active_cluster_id"], res["active_group_id"], res["members"]) == ("", "", [])
