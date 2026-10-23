"""What a replication start may name as its destination, and the pool the
copies land in when the caller names one.

A supercluster moves a volume between member clusters on one control plane,
and each member holds one pool per supercluster pool: the copy belongs in the
pool matching the source's, which the caller names, rather than in the
destination's first active pool.
"""
from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr

SRC, TGT, OTHER = "CL_SRC", "CL_TGT", "CL_OTHER"


def _node(uuid, cluster_id):
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


def _lvol(crypto="", replication_node_id="", pool_uuid="", policy=""):
    lvol = LVol()
    lvol.replication_policy_id = policy
    lvol.uuid = "LV1"
    lvol.node_id = "N_SRC"
    lvol.crypto_bdev = crypto
    lvol.replication_node_id = replication_node_id
    lvol.replication_pool_uuid = pool_uuid
    return lvol


class _FakeDB:

    def __init__(self, lvol):
        self._lvol = lvol
        self._nodes = {"N_SRC": _node("N_SRC", SRC), "N_TGT": _node("N_TGT", TGT),
                       "N_OTHER": _node("N_OTHER", OTHER)}
        self._clusters = {c: SimpleNamespace(snapshot_replication_target_cluster="",
                                             snapshot_replication_target_pool="")
                          for c in (SRC, TGT, OTHER)}
        self._pools = {p.uuid: p for p in (
            _pool("P_SRC", SRC), _pool("P_TGT_A", TGT), _pool("P_TGT_B", TGT),
            _pool("P_TGT_OFF", TGT, status=Pool.STATUS_INACTIVE), _pool("P_OTHER", OTHER))}

    def get_lvol_by_id(self, _):
        return self._lvol

    def get_storage_node_by_id(self, node_id):
        return self._nodes[node_id]

    def get_cluster_by_id(self, cluster_id):
        return self._clusters[cluster_id]

    def get_pool_by_id(self, pool_id):
        return self._pools[pool_id]

    def get_pools(self, cluster_id=None):
        return [p for p in self._pools.values() if cluster_id in (None, p.cluster_id)]

    def get_replication_policy_for_lvol(self, _):
        return None


@pytest.fixture
def db(monkeypatch):
    def install(lvol):
        fake = _FakeDB(lvol)
        monkeypatch.setattr(lvol_controller, "DBController", lambda: fake)
        monkeypatch.setattr(sr, "db", fake)
        return fake
    return install


@pytest.mark.parametrize("cluster, pool, mode, lvol, expected", [
    (SRC, None, "migration", _lvol(), "own cluster"),
    ("CL_MISSING", None, "migration", _lvol(), "does not exist"),
    (TGT, "P_OTHER", "migration", _lvol(), f"belongs to cluster {OTHER}"),
    (TGT, "P_MISSING", "migration", _lvol(), "does not exist"),
    (TGT, "P_TGT_OFF", "migration", _lvol(), "not active"),
    (None, "P_TGT_A", "migration", _lvol(), "replication_cluster_id"),
    (TGT, None, "migration", _lvol(crypto="crypto_LV1"), "encrypted"),
    (TGT, None, "migration", _lvol(replication_node_id="N_OTHER"), f"already replicates to cluster {OTHER}"),
])
def test_a_destination_the_move_cannot_use_is_refused(db, cluster, pool, mode, lvol, expected):
    db(lvol)

    error = lvol_controller.replication_start_error(
        "LV1", replication_cluster_id=cluster, replication_pool_id=pool, mode=mode)

    assert error is not None and expected in error


def test_a_destination_in_another_cluster_is_accepted(db):
    db(_lvol())

    assert lvol_controller.replication_start_error(
        "LV1", replication_cluster_id=TGT, replication_pool_id="P_TGT_B", mode="migration") is None


def test_an_encrypted_volume_may_still_replicate_for_failover(db):
    """Only a move deletes the source, and with it the only key; fail-over
    replication keeps the source and is not refused here."""
    db(_lvol(crypto="crypto_LV1"))

    assert lvol_controller.replication_start_error(
        "LV1", replication_cluster_id=TGT, mode="failover") is None


def test_the_requested_pool_receives_the_copies(db):
    lvol = _lvol(pool_uuid="P_TGT_B")
    db(lvol)

    assert sr._destination_pool_uuid(_node("N_TGT", TGT), lvol=lvol, source_cluster_id=SRC) == "P_TGT_B"
    fake = lvol_controller.DBController()
    _, pool = lvol_controller.resolve_replication_destination(
        fake, lvol, _node("N_TGT", TGT), _node("N_SRC", SRC))
    assert pool == "P_TGT_B"


def test_a_requested_pool_of_another_cluster_is_not_used(db):
    """A replication that later lands elsewhere (a fail-back) does not carry
    the forward move's pool into a cluster it does not belong to."""
    lvol = _lvol(pool_uuid="P_TGT_B")
    db(lvol)

    assert sr._destination_pool_uuid(_node("N_OTHER", OTHER), lvol=lvol, source_cluster_id=SRC) == "P_OTHER"


def test_a_policy_overrides_a_pool_named_before_it(db):
    """Started by hand into one pool, stopped, then attached to a policy: the
    policy's pool wins, and the earlier pool is not consulted."""
    lvol = _lvol(pool_uuid="P_TGT_B", policy="POLICY-1")
    db(lvol)

    assert lvol_controller.requested_destination_pool(lvol_controller.DBController(), lvol, TGT) == ""
