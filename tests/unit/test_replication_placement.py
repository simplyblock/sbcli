"""Destination-node placement for replication (docs/online-migration.md, G5).

The fallback pick was a weighted-random choice by subsystem count: it ignored
the volume's size and every replica already headed to a node, so starting
many volumes at once (a migration plan) piled them onto whichever nodes the
dice favoured. The pick is now the least PLANNED load -- resident volumes
plus replicas already assigned -- deterministic, and reservable across a batch.
"""
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode

GIB = 1024 ** 3


def _node(uuid, status=StorageNode.STATUS_ONLINE, max_lvol=100, secondary=False):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = "C2"
    n.status = status
    n.max_lvol = max_lvol
    n.is_secondary_node = secondary
    return n


def _lvol(uuid, node_id="", size=GIB, repl_node="", do_replicate=False,
          status=LVol.STATUS_ONLINE):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node_id
    lv.size = size
    lv.nqn = f"nqn:{uuid}"
    lv.replication_node_id = repl_node
    lv.do_replicate = do_replicate
    lv.status = status
    return lv


class _DB:
    def __init__(self, nodes, lvols):
        self._nodes = nodes
        self._lvols = lvols

    def get_storage_nodes_by_cluster_id(self, cid):
        return [n for n in self._nodes if n.cluster_id == cid]

    def get_lvols(self, cluster_id=None):
        return list(self._lvols)


def _pick(db, size=GIB, exclude=(), load=None):
    node = lvol_controller._pick_replication_node(db, "C2", size, exclude=exclude, load=load)
    return node.get_id() if node else None


def test_least_resident_load_wins():
    db = _DB([_node("A"), _node("B")],
             [_lvol("x", "A", 10 * GIB), _lvol("y", "B", 2 * GIB)])
    assert _pick(db) == "B"


def test_replicas_already_assigned_count_as_load():
    """B is empty, but 20 GiB are already replicating INTO it from another
    cluster: those land there at cutover."""
    db = _DB([_node("A"), _node("B")],
             [_lvol("x", "A", 5 * GIB),
              _lvol("src", "S1", 20 * GIB, repl_node="B", do_replicate=True)])
    assert _pick(db) == "A"


def test_assignments_that_stopped_replicating_do_not_count():
    db = _DB([_node("A"), _node("B")],
             [_lvol("x", "A", 5 * GIB),
              _lvol("src", "S1", 20 * GIB, repl_node="B", do_replicate=False)])
    assert _pick(db) == "B"


def test_deterministic_tie_break_by_node_id():
    db = _DB([_node("B"), _node("A")], [])
    assert [_pick(db) for _ in range(5)] == ["A"] * 5


def test_offline_secondary_excluded_and_full_nodes_are_skipped():
    db = _DB([_node("A", status=StorageNode.STATUS_OFFLINE),
              _node("B", secondary=True),
              _node("C", max_lvol=1),
              _node("D")],
             [_lvol("c1", "C", GIB), _lvol("d1", "D", 50 * GIB)])
    assert _pick(db) == "D"
    assert _pick(db, exclude={"D"}) is None


def test_a_batch_spreads_when_load_is_reserved():
    db = _DB([_node("A"), _node("B"), _node("C")], [])
    load = lvol_controller._replication_node_load(db, "C2")
    picks = [_pick(db, size=10 * GIB, load=load) for _ in range(6)]
    assert sorted(picks) == ["A", "A", "B", "B", "C", "C"]


def test_reservation_counts_the_subsystem_cap():
    db = _DB([_node("A", max_lvol=1), _node("B", max_lvol=5)], [])
    load = lvol_controller._replication_node_load(db, "C2")
    picks = [_pick(db, load=load) for _ in range(3)]
    assert picks == ["A", "B", "B"]
