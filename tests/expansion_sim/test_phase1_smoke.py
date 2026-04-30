# coding=utf-8
"""Phase 1 smoke tests — incremental setup checks. Each test exercises a
slice of the fixtures so we can pinpoint where a regression hides."""

import logging
import os
import sys

logger = logging.getLogger(__name__)

# Force unbuffered prints so we see progress on a hang.
os.environ.setdefault("PYTHONUNBUFFERED", "1")


def _say(msg):
    print(f"[smoke] {msg}", flush=True)
    sys.stdout.flush()


def test_a_fdb_alive(fdb_db):
    """FDB connection works — read/write a probe key."""
    _say("starting test_a_fdb_alive")
    fdb_db[b"probe-key"] = b"hello"
    assert bytes(fdb_db[b"probe-key"]) == b"hello"
    _say("done test_a_fdb_alive")


def test_b_fdb_clean(fdb_clean):
    """fdb_clean wipes the keyspace."""
    _say("start test_b_fdb_clean")
    # Should be empty after fixture
    rng = list(fdb_clean.get_range(b"\x00", b"\xff", limit=5))
    _say(f"got {len(rng)} keys after clean")
    assert len(rng) == 0
    _say("done test_b_fdb_clean")


def test_c_db_controller(db_controller):
    """DBController binds to the same FDB."""
    _say("start test_c_db_controller")
    assert db_controller.kv_store is not None
    _say("done test_c_db_controller")


def test_d_router_active(patched_rpc_router):
    """RpcRouter is patched into the canonical rpc_client module."""
    _say("start test_d_router_active")
    from tests.expansion_sim._rpc_sim import RpcRouter
    from simplyblock_core import rpc_client
    assert rpc_client.RPCClient is RpcRouter, (
        f"RPCClient is {rpc_client.RPCClient}, expected RpcRouter")
    _say("done test_d_router_active")


def test_e_baseline_scenario(fdb_clean, db_controller_singleton_reset,
                              patched_rpc_router):
    """Build the 4-node FTT1 baseline; assert FDB has the records and
    simulators are registered."""
    _say("start test_e_baseline_scenario")
    from tests.expansion_sim._scenarios import build_4_node_ftt1_baseline
    from simplyblock_core.db_controller import DBController

    db = DBController()
    cluster_id, existing_ids, new_node_id = build_4_node_ftt1_baseline(
        patched_rpc_router, db.kv_store)
    _say(f"baseline built: cluster={cluster_id} existing={existing_ids} new={new_node_id}")

    cluster = db.get_cluster_by_id(cluster_id)
    assert cluster.uuid == cluster_id
    assert cluster.max_fault_tolerance == 1

    snodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    assert len(snodes) == 5

    for nid in existing_ids:
        n = db.get_storage_node_by_id(nid)
        assert n.lvstore == f"LVS_{nid}", n.lvstore
        sim = patched_rpc_router.get_by_node_id(nid)
        assert sim is not None

    _say("done test_e_baseline_scenario")


def test_f_planner_against_db(fdb_clean, db_controller_singleton_reset,
                               patched_rpc_router):
    """compute_role_diff against the FDB-backed scenario produces the right
    move count for 4 -> 5 FTT1 (3 moves: 1 re-home + 2 newcomer creates)."""
    _say("start test_f_planner_against_db")
    from tests.expansion_sim._scenarios import build_4_node_ftt1_baseline
    from simplyblock_core.cluster_expand_planner import compute_role_diff
    from simplyblock_core.db_controller import DBController

    db = DBController()
    cluster_id, existing_ids, new_node_id = build_4_node_ftt1_baseline(
        patched_rpc_router, db.kv_store)

    moves = compute_role_diff(existing_ids, new_node_id, ftt=1)
    _say(f"planned {len(moves)} moves: {[(m.role, m.from_node_id, m.to_node_id) for m in moves]}")
    assert len(moves) == 3
    _say("done test_f_planner_against_db")
