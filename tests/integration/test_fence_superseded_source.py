"""The fence order for a source that was failed over elsewhere (control-plane
split, M3).

After a manual failover with site B cut off, site A holds B's group as clones.
When B returns, its control plane does not find out on its own that it was
superseded: the layer above (the DR hub, a person, any other component that
detects the failover) orders it, through the operator, as a demote with
``superseded=True``. That demote stops each member's replication and takes no
demote cut: B's data is stale, and shipping it would land as one more
generation of B's group on A. It leaves the paths up: the order arrives while
B's copy of the application may still run, and a guest whose disk goes dark
hangs on shutdown, so B is never cleaned up and Ramen never makes it secondary
(bed run 20261117-0616). Ramen's own DemoteVolume, which comes only once the
workload is unmounted, cuts the paths.

The order and Ramen's own DemoteVolume can reach B in either order, and B's old
replication tasks can resume before either. So the new primary also guards its
own records: a landing for a volume it failed over is refused.

Real FDB: clusters, nodes, volumes, groups, snapshots, tasks and relationships
are seeded and read back. The fence (node RPCs) and the peer control plane are
mocked.
"""
import uuid

import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.peer import service
from simplyblock_core.peer.protocol import LandingRequest
from simplyblock_core.services import replication_final_step
from simplyblock_core.services import snapshot_replication as sr
from tests.integration import test_deferred_peer_placement as deferred
from tests.integration import test_sync_replication_lvs_stack as stack

db = stack.db
peer = deferred.peer
source_side = deferred.source_side


@pytest.fixture()
def fences(monkeypatch):
    """Every member fenced, by (nqn, ns_id)."""
    calls = []
    monkeypatch.setattr(replication_final_step, "fence_source_paths",
                        lambda node, lvstore, nqn, ns_id: calls.append((nqn, ns_id)))
    return calls


@pytest.fixture()
def no_snapshots(monkeypatch):
    """A demote cut is a snapshot; none may be taken."""
    taken = []

    def _add(*args, **kwargs):
        taken.append(args)
        return "demote-cut", None

    monkeypatch.setattr(lvol_controller.snapshot_controller, "add", _add)
    return taken


def _replicating(db, members=1):
    """Site B's volumes, still replicating toward site A as before the partition."""
    return deferred._unplaced(db, members)


def _group(db, cluster, vols):
    group = ConsistencyGroup()
    group.uuid = str(uuid.uuid4())
    group.cluster_id = cluster.get_id()
    group.group_name = "gitea"
    group.node_id = vols[0].node_id
    group.lvs_name = vols[0].lvs_name
    group.last_group_seq = 4
    group.members = {v.get_id(): {"joined_seq": 1, "removed_seq": 0} for v in vols}
    group.write_to_db(db.kv_store)
    for v in vols:
        deferred._with(db, v, group_id=group.get_id())
    return group


# ------------------------------------------------------------------ one volume

def test_a_superseded_demote_stops_replication_and_leaves_the_paths_up(
        db, fences, no_snapshots):
    cluster, (vol,) = _replicating(db)
    task, _ = deferred._task(db, cluster, vol)

    assert lvol_controller.demote_lvol(vol.get_id(), superseded=True) == {"demoted": True}

    stored = db.get_lvol_by_id(vol.get_id())
    assert fences == [], "the paths stay up: the old site's workload may still run"
    assert no_snapshots == [], "no demote cut: the data is stale"
    assert stored.replication_demote_state == LVol.REPLICATION_DEMOTE_DONE
    assert stored.do_replicate is False, "nothing ships toward the new primary"
    assert db.get_task_by_id(task.uuid).canceled, "a queued transfer of the volume is ended"


def test_a_superseded_demote_overrides_a_demote_cut_already_started(db, fences, no_snapshots):
    """Ramen's DemoteVolume came first and started a normal demote: the cut is
    waiting for a transfer the new primary refuses. The order ends it."""
    cluster, (vol,) = _replicating(db)
    cut_task, cut = deferred._task(db, cluster, vol)
    deferred._with(db, vol, replication_demote_state=LVol.REPLICATION_DEMOTE_PENDING,
                   replication_demote_snapshot_id=cut.get_id())

    assert lvol_controller.demote_lvol(vol.get_id(), superseded=True) == {"demoted": True}

    stored = db.get_lvol_by_id(vol.get_id())
    assert stored.replication_demote_state == LVol.REPLICATION_DEMOTE_DONE
    assert stored.do_replicate is False
    assert db.get_task_by_id(cut_task.uuid).canceled
    assert fences == [], "the order cuts no paths of its own"


def test_a_repeated_order_and_a_later_ramen_demote_answer_done(db, fences, no_snapshots):
    _, (vol,) = _replicating(db)

    lvol_controller.demote_lvol(vol.get_id(), superseded=True)
    assert lvol_controller.demote_lvol(vol.get_id(), superseded=True) == {"demoted": True}
    assert lvol_controller.demote_lvol(vol.get_id()) == {"demoted": True}

    assert no_snapshots == []
    assert db.get_lvol_by_id(vol.get_id()).do_replicate is False


def test_a_plain_demote_still_takes_a_cut(db, fences, no_snapshots):
    """Without the order nothing changes: the planned relocate depends on the cut."""
    _, (vol,) = _replicating(db)

    assert lvol_controller.demote_lvol(vol.get_id()) == {"demoted": False}

    assert len(no_snapshots) == 1
    stored = db.get_lvol_by_id(vol.get_id())
    assert stored.replication_demote_state == LVol.REPLICATION_DEMOTE_PENDING
    assert stored.do_replicate is True


# ------------------------------------------------------------------ a group

def test_a_superseded_group_demote_stops_every_member_without_a_group_cut(
        db, fences, no_snapshots, monkeypatch):
    cluster, vols = _replicating(db, members=2)
    group = _group(db, cluster, vols)
    cuts = []
    monkeypatch.setattr(cgc, "create_group_snapshot_for_group",
                        lambda g: cuts.append(g.get_id()) or (5, None))

    result = cgc.demote_group(group, superseded=True)

    assert result["demoted"] is True
    assert {m["lvol_id"] for m in result["members"]} == {v.get_id() for v in vols}
    assert cuts == [] and no_snapshots == []
    assert fences == [], "no member's paths are cut"
    for v in vols:
        stored = db.get_lvol_by_id(v.get_id())
        assert (stored.replication_demote_state, stored.do_replicate) == \
            (LVol.REPLICATION_DEMOTE_DONE, False)
    assert db.get_consistency_group_by_id(group.get_id()).last_group_seq == 4


def test_a_superseded_order_for_an_empty_group_is_done(db, fences):
    cluster, vols = _replicating(db)
    group = _group(db, cluster, vols)
    group.members = {}
    group.write_to_db(db.kv_store)

    assert cgc.demote_group(group, superseded=True)["demoted"] is True
    assert fences == []


# ------------------------------------------------------------------ the new primary

SOURCE_VOLUME = "site-b-gitea-data"


def _site_a_holding_a_failover(db, source_volume=SOURCE_VOLUME):
    """Site A after failing B's volume over: the relationship, keyed by B's
    volume, is all A knows of it."""
    cluster = stack._seed_cluster(db)
    source = LVol()
    source.uuid = source_volume
    clone = LVol()
    clone.uuid = str(uuid.uuid4())
    rep = LVolReplication()
    rep.uuid = str(uuid.uuid4())
    rep.source_lvol, rep.target_lvol = source, clone
    rep.source_cluster_id = "site-b"
    rep.target_cluster_id = cluster.get_id()
    rep.state = LVolReplication.STATE_FAILED_OVER
    rep.write_to_db(db.kv_store)
    return cluster


def _landing(source_lvol_id):
    return LandingRequest(source_cluster_id="site-b", snapshot_id="site-b-snap",
                          landing_name="REP_site-b-snap", size=1 << 30, ha_type="single",
                          source_lvol_id=source_lvol_id)


class _PastTheGuard(Exception):
    pass


@pytest.fixture()
def past_the_guard(monkeypatch):
    """Where a landing goes once it is accepted (the member choice) raises, so a
    test sees whether the request got that far."""
    def _node(*args, **kwargs):
        raise _PastTheGuard
    monkeypatch.setattr(service, "_landing_node", _node)


def test_the_new_primary_refuses_a_landing_of_a_volume_it_failed_over(db, past_the_guard):
    cluster = _site_a_holding_a_failover(db)

    with pytest.raises(service.SupersededSourceError, match=SOURCE_VOLUME):
        service.prepare_landing(cluster.get_id(), _landing(SOURCE_VOLUME))


def test_the_refusal_reaches_the_source_as_a_refusal(db, past_the_guard):
    from simplyblock_core.peer.client import LocalPeerClient, PeerRefusedError
    cluster = _site_a_holding_a_failover(db)

    with pytest.raises(PeerRefusedError):
        LocalPeerClient(cluster.get_id()).prepare_landing(_landing(SOURCE_VOLUME))


@pytest.mark.parametrize("source_lvol_id", ["another-volume", ""])
def test_a_landing_of_any_other_volume_is_not_refused(db, past_the_guard, source_lvol_id):
    cluster = _site_a_holding_a_failover(db)

    with pytest.raises(_PastTheGuard):
        service.prepare_landing(cluster.get_id(), _landing(source_lvol_id))


def test_a_failover_onto_another_cluster_does_not_refuse_here(db, past_the_guard):
    cluster = _site_a_holding_a_failover(db)
    rep = db.get_lvol_replication_objects()[0]
    rep.target_cluster_id = "site-c"
    rep.write_to_db(db.kv_store)

    with pytest.raises(_PastTheGuard):
        service.prepare_landing(cluster.get_id(), _landing(SOURCE_VOLUME))


def test_the_reverse_pipe_of_the_failed_over_clone_is_not_refused(db, past_the_guard):
    """The fail-back direction: site A's clone replicating home is the TARGET
    end of the relationship, never its source."""
    cluster = _site_a_holding_a_failover(db)
    clone_id = db.get_lvol_replication_objects()[0].target_lvol.get_id()

    with pytest.raises(_PastTheGuard):
        service.prepare_landing(cluster.get_id(), _landing(clone_id))


# ------------------------------------------------------------------ the source side

def test_a_landing_request_names_its_source_volume(db, peer, source_side):
    peer.nodes = ["B1"]
    cluster, (vol,) = deferred._unplaced(db)
    task, snap = deferred._task(db, cluster, vol)

    sr.process_snap_replicate_start(task, snap)

    assert [r.source_lvol_id for r in peer.landings] == [vol.get_id()]

