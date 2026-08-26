"""Where a replicated copy lands must follow the volume's policy, not a
cluster-level field that describes the other direction.

Two defects are pinned here, both observed in the 2026-08-19 replication lab:

  * ``snapshot_replication._destination_pool_uuid`` placed an INCOMING copy
    using the DESTINATION cluster's own OUTGOING config
    (``snapshot_replication_target_pool`` = "the pool I replicate into on my
    target"). Any cluster that is itself a source therefore handed out a pool
    belonging to a third cluster the moment data came back the other way: the
    fail-back into the src cluster created its ``REP_*`` volumes in the tgt
    cluster's pool, and 13 of them ended up stuck ``in_deletion``.

  * ``replicate_lvol_on_target_cluster`` / ``_prepare_cutover`` resolved the
    destination CLUSTER from ``source_cluster.snapshot_replication_target_cluster``.
    ``add_target()``/``add_policy()`` never write that field, so a policy-driven
    volume could not fail over or commit a cutover at all — and on a fail-back
    the field points at the wrong cluster even when it is set.
"""
import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr


SRC, TGT = "CL_SRC", "CL_TGT"
POOL_SRC, POOL_TGT = "POOL_SRC", "POOL_TGT"


def _node(cluster_id, uuid="N1"):
    node = StorageNode()
    node.uuid = uuid
    node.cluster_id = cluster_id
    return node


def _pool(uuid, cluster_id, status=Pool.STATUS_ACTIVE):
    pool = Pool()
    pool.uuid = uuid
    pool.pool_name = uuid
    pool.cluster_id = cluster_id
    pool.status = status
    return pool


def _cluster(uuid, target_cluster="", target_pool=""):
    return type("C", (), {
        "uuid": uuid,
        "get_id": lambda self=None, _u=uuid: _u,
        "snapshot_replication_target_cluster": target_cluster,
        "snapshot_replication_target_pool": target_pool,
    })()


def _lvol(policy_id=""):
    return type("LV", (), {"uuid": "LV1", "replication_policy_id": policy_id,
                           "get_id": lambda self: "LV1"})()


class _FakeDB:
    """Just enough DB for the two resolvers."""

    def __init__(self, clusters, pools, policy=None, target=None):
        self._clusters = clusters
        self._pools = pools
        self._policy = policy
        self._target = target

    def get_cluster_by_id(self, cluster_id):
        if not cluster_id:
            raise KeyError("Cluster lookup with a blank id")
        return self._clusters[cluster_id]

    def get_pools(self, cluster_id=None):
        return [p for p in self._pools if cluster_id in (None, p.cluster_id)]

    def get_replication_policy_for_lvol(self, lvol):
        return self._policy if getattr(lvol, "replication_policy_id", "") else None

    def get_replication_target_by_id(self, target_id):
        if self._target is None or self._target.get_id() != target_id:
            raise KeyError(target_id)
        return self._target


def _policy(target_id="T1"):
    return type("P", (), {"uuid": "P1", "target_id": target_id,
                          "get_id": lambda self: "P1"})()


def _target(target_cluster, target_pool, uuid="T1"):
    return type("T", (), {
        "uuid": uuid,
        "get_id": lambda self=None, _u=uuid: _u,
        "target_cluster_id": target_cluster,
        "target_pool_uuid": target_pool,
    })()


# --- snapshot_replication._destination_pool_uuid -------------------------


def _sr_db(src_target_cluster="", src_target_pool="", tgt_target_pool="",
           policy=None, target=None):
    return _FakeDB(
        clusters={
            SRC: _cluster(SRC, src_target_cluster, src_target_pool),
            TGT: _cluster(TGT, "", tgt_target_pool),
        },
        pools=[_pool(POOL_SRC, SRC), _pool(POOL_TGT, TGT)],
        policy=policy, target=target)


def test_incoming_copy_ignores_the_destinations_outgoing_pool(monkeypatch):
    """The regression: failing BACK into src must not use src's outgoing pool.

    src is configured to replicate into POOL_TGT on the tgt cluster. Data coming
    the other way belongs in a pool of src, never in POOL_TGT.
    """
    monkeypatch.setattr(sr, "db", _sr_db(src_target_cluster=TGT,
                                         src_target_pool=POOL_TGT))
    assert sr._destination_pool_uuid(_node(SRC), lvol=_lvol(),
                                     source_cluster_id=TGT) == POOL_SRC


def test_policy_target_pool_wins(monkeypatch):
    monkeypatch.setattr(sr, "db", _sr_db(
        policy=_policy(), target=_target(TGT, "POOL_FROM_TARGET")))
    assert sr._destination_pool_uuid(_node(TGT), lvol=_lvol("P1"),
                                     source_cluster_id=SRC) == "POOL_FROM_TARGET"


def test_policy_target_for_another_cluster_is_not_used(monkeypatch):
    """A target naming a third cluster's pool must not be applied here."""
    monkeypatch.setattr(sr, "db", _sr_db(
        policy=_policy(), target=_target("CL_OTHER", "POOL_ELSEWHERE")))
    assert sr._destination_pool_uuid(_node(TGT), lvol=_lvol("P1"),
                                     source_cluster_id=SRC) == POOL_TGT


def test_legacy_field_still_used_for_the_direction_it_describes(monkeypatch):
    """Forward replication with no policy: the source's config is authoritative."""
    monkeypatch.setattr(sr, "db", _sr_db(src_target_cluster=TGT,
                                         src_target_pool="POOL_CONFIGURED"))
    assert sr._destination_pool_uuid(_node(TGT), lvol=_lvol(),
                                     source_cluster_id=SRC) == "POOL_CONFIGURED"


def test_falls_back_to_first_active_pool_on_the_destination(monkeypatch):
    db = _FakeDB(clusters={SRC: _cluster(SRC), TGT: _cluster(TGT)},
                 pools=[_pool("POOL_DEAD", TGT, status=Pool.STATUS_INACTIVE),
                        _pool(POOL_TGT, TGT)])
    monkeypatch.setattr(sr, "db", db)
    assert sr._destination_pool_uuid(_node(TGT), lvol=_lvol(),
                                     source_cluster_id=SRC) == POOL_TGT


def test_no_pool_on_the_destination_returns_none(monkeypatch):
    monkeypatch.setattr(sr, "db", _FakeDB(
        clusters={SRC: _cluster(SRC), TGT: _cluster(TGT)}, pools=[]))
    assert sr._destination_pool_uuid(_node(TGT), lvol=_lvol(),
                                     source_cluster_id=SRC) is None


# --- lvol_controller.resolve_replication_destination ---------------------


def test_destination_cluster_comes_from_the_target_node():
    """No policy, no cluster config: the node the volume replicates to decides."""
    db = _FakeDB(clusters={SRC: _cluster(SRC), TGT: _cluster(TGT)},
                 pools=[_pool(POOL_TGT, TGT)])
    cluster, pool = lvol_controller.resolve_replication_destination(
        db, _lvol(), _node(TGT, "TGT_N"), _node(SRC, "SRC_N"))
    assert cluster.get_id() == TGT
    assert pool == POOL_TGT


def test_failback_does_not_follow_the_old_forward_config():
    """Volume now lives on tgt and fails back to src.

    tgt still carries the forward config it was given as a source (-> a third
    cluster). Resolution must ignore it and land on the src cluster's own pool.
    """
    db = _FakeDB(
        clusters={SRC: _cluster(SRC),
                  TGT: _cluster(TGT, target_cluster="CL_OTHER",
                                target_pool="POOL_ELSEWHERE")},
        pools=[_pool(POOL_SRC, SRC), _pool(POOL_TGT, TGT)])
    cluster, pool = lvol_controller.resolve_replication_destination(
        db, _lvol(), _node(SRC, "SRC_N"), _node(TGT, "TGT_N"))
    assert cluster.get_id() == SRC
    assert pool == POOL_SRC


def test_policy_target_pool_wins_for_the_cutover_clone():
    db = _FakeDB(clusters={SRC: _cluster(SRC), TGT: _cluster(TGT)},
                 pools=[_pool(POOL_TGT, TGT)],
                 policy=_policy(), target=_target(TGT, "POOL_FROM_TARGET"))
    _cluster_obj, pool = lvol_controller.resolve_replication_destination(
        db, _lvol("P1"), _node(TGT, "TGT_N"), _node(SRC, "SRC_N"))
    assert pool == "POOL_FROM_TARGET"


def test_blank_target_cluster_field_is_never_looked_up():
    """A policy-driven volume has no snapshot_replication_target_cluster.

    The old code passed that empty string to get_cluster_by_id and died before
    the fail-over could start.
    """
    db = _FakeDB(clusters={SRC: _cluster(SRC), TGT: _cluster(TGT)},
                 pools=[_pool(POOL_TGT, TGT)])
    with pytest.raises(KeyError):
        db.get_cluster_by_id("")                 # what the old path did
    cluster, _pool_uuid = lvol_controller.resolve_replication_destination(
        db, _lvol("P1"), _node(TGT, "TGT_N"), _node(SRC, "SRC_N"))
    assert cluster.get_id() == TGT
